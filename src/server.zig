const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();
const testing = std.testing;
const Cache = @import("Cache.zig");
const parser = @import("parser.zig");
const ServerState = @import("ServerState.zig");
const network = @import("network.zig");
const rdb = @import("rdb.zig");

const Error = error{};

pub fn runServer(state: *ServerState) !void {
    switch (state.getInfoSectionsThreadSafe().replication.role) {
        .master => {
            try runMasterServer(state);
        },
        .slave => {
            try runSlaveServer(state);
        },
    }
}

fn handleRequestAndRespond(allocator: std.mem.Allocator, raw_message: []const u8, state: *ServerState, connection: anytype) !void {
    // Parse the raw_message into a Request (a command from the client).
    var request = try parser.parseRequest(allocator, raw_message, connection.address);
    defer request.deinit();

    // Handle the Request (update state) and generate a Response as a string.
    const response_str = try parser.handleRequest(allocator, request, state);
    defer allocator.free(response_str);

    // Send the Response back to the client.
    try connection.stream.writeAll(response_str);
}

const MockConnection = struct {
    address: std.net.Address,
    stream: std.io.FixedBufferStream([]u8).Writer,
};
test "handleRequestAndRespond" {
    var state = try ServerState.initFromCliArgs(testing.allocator, &[_][]const u8{ "--port", "123" });
    defer state.deinit();

    const unused_address = try std.net.Address.parseIp4("127.0.0.1", 1234);
    {
        var buffer: [64]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buffer);
        try handleRequestAndRespond(
            testing.allocator,
            "*2\r\n$4\r\nECHO\r\n$13\r\nHello, world!\r\n",
            &state,
            MockConnection{
                .address = unused_address,
                .stream = fbs.writer(),
            },
        );
        try testing.expectEqualSlices(u8, "$13\r\nHello, world!\r\n", fbs.getWritten());
    }
    {
        var buffer: [64]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buffer);
        try handleRequestAndRespond(
            testing.allocator,
            "*2\r\n$3\r\ngEt\r\n$13\r\nHello, world!\r\n",
            &state,
            MockConnection{
                .address = unused_address,
                .stream = fbs.writer(),
            },
        );
        try testing.expectEqualSlices(u8, "$-1\r\n", fbs.getWritten());
    }
    {
        var buffer: [64]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buffer);
        try handleRequestAndRespond(
            testing.allocator,
            "*3\r\n$3\r\nSEt\r\n$13\r\nHello, world!\r\n$4\r\nbye!\r\n",
            &state,
            MockConnection{
                .address = unused_address,
                .stream = fbs.writer(),
            },
        );
        try testing.expectEqualSlices(u8, "+OK\r\n", fbs.getWritten());
    }
    {
        var buffer: [64]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buffer);
        try handleRequestAndRespond(
            testing.allocator,
            "*2\r\n$3\r\ngEt\r\n$13\r\nHello, world!\r\n",
            &state,
            MockConnection{
                .address = unused_address,
                .stream = fbs.writer(),
            },
        );
        try testing.expectEqualSlices(u8, "$4\r\nbye!\r\n", fbs.getWritten());
    }
    {
        var buffer: [128]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buffer);
        try handleRequestAndRespond(
            testing.allocator,
            "*2\r\n$4\r\nINfo\r\n$11\r\nrePLicAtion\r\n",
            &state,
            MockConnection{
                .address = unused_address,
                .stream = fbs.writer(),
            },
        );
        var iter = std.mem.splitScalar(u8, fbs.getWritten(), '\n');
        {
            const line = iter.next();
            try testing.expect(line != null);
            try testing.expectEqualStrings("$88\r", line.?);
        }
        {
            const line = iter.next();
            try testing.expect(line != null);
            try testing.expectEqualStrings("role:master", line.?);
        }
        {
            const line = iter.next();
            try testing.expect(line != null);
            try testing.expectStringStartsWith(line.?, "master_replid:");
            try testing.expectEqual("master_replid:".len + 40, line.?.len);
        }
        {
            const line = iter.next();
            try testing.expect(line != null);
            try testing.expectEqualStrings("master_repl_offset:0", line.?);
        }
        {
            const line = iter.next();
            try testing.expect(line != null);
            try testing.expectEqualStrings("\r", line.?);
        }
        {
            const line = iter.next();
            try testing.expect(line != null);
            try testing.expectEqualStrings("", line.?);
        }
        {
            const line = iter.next();
            try testing.expect(line == null);
        }
    }
}

fn handleClient(client_connection: net.Server.Connection, state: *ServerState) !void {
    defer client_connection.stream.close();

    // We don't expect each client to use up too much memory, so we use an arena allocator for speed and blow away all the memory at once when we're done.
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var raw_message = std.ArrayList(u8).init(allocator);
    defer raw_message.deinit();

    while (true) {
        const num_read_bytes = try network.readFromStream(client_connection.stream, &raw_message);
        if (num_read_bytes == 0) return;

        try handleRequestAndRespond(allocator, raw_message.items, state, client_connection);

        // Clear the message since the client might send more, but don't actually deallocate so we can reuse this for the next message.
        raw_message.clearRetainingCapacity();

        // TODO just temporarily, send an RDB file if the current client is waiting to receive. Need to find a better place to put this + relaying writes to connected replicas.
        if (state.replicaof == null) {
            if (state.replicaStatesGetThreadSafe(client_connection.address)) |replica_state| {
                switch (replica_state) {
                    .receiving_sync => {
                        const rdb_file = try rdb.getEmptyRdb(allocator);
                        defer allocator.free(rdb_file);
                        const msg = try std.fmt.allocPrint(allocator, "${d}\r\n{s}", .{ rdb_file.len, rdb_file });
                        defer allocator.free(msg);
                        try client_connection.stream.writeAll(msg);
                    },
                    else => {},
                }
            }
        }
    }
}
fn sendReplicaUpdates(allocator: std.mem.Allocator, state: *ServerState) !void {
    // For now, just take care of sending the RDB file for all replicas waiting for it.
    const replicas = try state.getReplicaStatesByTypeThreadSafe(allocator, ServerState.ReplicaStateType.receiving_sync);
    defer allocator.free(replicas);
    const rdb_data = try rdb.getEmptyRdb(allocator);
    defer allocator.free(rdb_data);
    // TODO write to each replica

    // TODO later, relay all writes to replicas (maybe not here, actually).
}

fn loadRdbFile(rdb_file: parser.RdbFile, state: *ServerState) !void {
    // TODO take the parsed RDB that master sent and use it to update our state.
    _ = rdb_file;
    _ = state;
}
fn syncWithMaster(allocator: std.mem.Allocator, state: *ServerState) !void {
    // Send full sync handshake to master
    const rdb_file = try parser.sendSyncHandshakeToMaster(allocator, state);

    // Take the parsed RDB that master sent and use it to update our state.
    try loadRdbFile(rdb_file, state);
}
fn listenForClientsAndHandleRequests(address: net.Address, state: *ServerState) !void {
    // Keep listening to new client connections, and once one comes in, set it up to be handled be a new thread and go back to listening.
    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();
    try stdout.print("Started listening at address: {}\n", .{address.in});
    while (true) {

        // TODO we delegate the responsibility of closing the connection to the spawned thread, but technically we could have an error before that happens.
        const connection = try listener.accept();

        try stdout.print("accepted new connection from client {}\n", .{connection.address.in.sa.port});
        // TODO join on these threads for correctness.
        // TODO handle errors coming back from these threads.
        const t = try std.Thread.spawn(.{}, handleClient, .{ connection, state });
        t.detach();
    }
}
pub fn runMasterServer(state: *ServerState) !void {
    const our_address = try net.Address.resolveIp("127.0.0.1", state.port);
    try listenForClientsAndHandleRequests(our_address, state);
}
pub fn runSlaveServer(state: *ServerState) !void {
    const our_address = try net.Address.resolveIp("127.0.0.1", state.port);

    // Set up an arena allocator backed by a page allocator. We only need it for syncWithMaster.
    {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const allocator = arena.allocator();
        try syncWithMaster(allocator, state);
    }

    try listenForClientsAndHandleRequests(our_address, state);
}
