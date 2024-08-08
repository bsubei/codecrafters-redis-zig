const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();
const testing = std.testing;
const Cache = @import("Cache.zig");
const parser = @import("parser.zig");
const ServerState = @import("server_state.zig").ServerState;
const network = @import("network.zig");
const rdb = @import("rdb.zig");
const xev = @import("xev");
const posix = std.posix;
const Connection = @import("connection.zig").Connection;

const Error = error{};

pub fn runServer(state: *ServerState) !void {
    switch (state.info_sections.replication.role) {
        .master => {
            try runMasterServer(state);
        },
        .slave => {
            try runSlaveServer(state);
        },
    }
}

fn handleRequestAndRespond(allocator: std.mem.Allocator, raw_message: []const u8, state: *ServerState, connection_fd: posix.socket_t) !void {
    // Parse the raw_message into a Request (a command from the client).
    var request = try parser.parseRequest(allocator, raw_message);
    defer request.deinit();

    // Handle the Request (update state) and generate a Response as a string.
    const response_str = try parser.handleRequest(allocator, request, state);
    defer allocator.free(response_str);

    // Send the Response back to the client.
    std.debug.print("Writing this to socket: {s}\n", .{response_str});
    const written_bytes = try network.writeToSocket(connection_fd, response_str);
    _ = written_bytes;
    // try connection.stream.writeAll(response_str);
}

// TODO Fix these tests by wrapping the connection fd in a reader and a writer and passing these into the function. That way we can test it.
// const MockConnection = struct {
//     address: std.net.Address,
//     stream: std.io.FixedBufferStream([]u8).Writer,
// };
// test "handleRequestAndRespond" {
//     var state = try ServerState.initFromCliArgs(testing.allocator, &[_][]const u8{ "--port", "123" });
//     defer state.deinit();

//     const unused_address = try std.net.Address.parseIp4("127.0.0.1", 1234);
//     {
//         var buffer: [64]u8 = undefined;
//         var fbs = std.io.fixedBufferStream(&buffer);
//         try handleRequestAndRespond(
//             testing.allocator,
//             "*2\r\n$4\r\nECHO\r\n$13\r\nHello, world!\r\n",
//             &state,
//             MockConnection{
//                 .address = unused_address,
//                 .stream = fbs.writer(),
//             },
//         );
//         try testing.expectEqualSlices(u8, "$13\r\nHello, world!\r\n", fbs.getWritten());
//     }
//     {
//         var buffer: [64]u8 = undefined;
//         var fbs = std.io.fixedBufferStream(&buffer);
//         try handleRequestAndRespond(
//             testing.allocator,
//             "*2\r\n$3\r\ngEt\r\n$13\r\nHello, world!\r\n",
//             &state,
//             MockConnection{
//                 .address = unused_address,
//                 .stream = fbs.writer(),
//             },
//         );
//         try testing.expectEqualSlices(u8, "$-1\r\n", fbs.getWritten());
//     }
//     {
//         var buffer: [64]u8 = undefined;
//         var fbs = std.io.fixedBufferStream(&buffer);
//         try handleRequestAndRespond(
//             testing.allocator,
//             "*3\r\n$3\r\nSEt\r\n$13\r\nHello, world!\r\n$4\r\nbye!\r\n",
//             &state,
//             MockConnection{
//                 .address = unused_address,
//                 .stream = fbs.writer(),
//             },
//         );
//         try testing.expectEqualSlices(u8, "+OK\r\n", fbs.getWritten());
//     }
//     {
//         var buffer: [64]u8 = undefined;
//         var fbs = std.io.fixedBufferStream(&buffer);
//         try handleRequestAndRespond(
//             testing.allocator,
//             "*2\r\n$3\r\ngEt\r\n$13\r\nHello, world!\r\n",
//             &state,
//             MockConnection{
//                 .address = unused_address,
//                 .stream = fbs.writer(),
//             },
//         );
//         try testing.expectEqualSlices(u8, "$4\r\nbye!\r\n", fbs.getWritten());
//     }
//     {
//         var buffer: [128]u8 = undefined;
//         var fbs = std.io.fixedBufferStream(&buffer);
//         try handleRequestAndRespond(
//             testing.allocator,
//             "*2\r\n$4\r\nINfo\r\n$11\r\nrePLicAtion\r\n",
//             &state,
//             MockConnection{
//                 .address = unused_address,
//                 .stream = fbs.writer(),
//             },
//         );
//         var iter = std.mem.splitScalar(u8, fbs.getWritten(), '\n');
//         {
//             const line = iter.next();
//             try testing.expect(line != null);
//             try testing.expectEqualStrings("$88\r", line.?);
//         }
//         {
//             const line = iter.next();
//             try testing.expect(line != null);
//             try testing.expectEqualStrings("role:master", line.?);
//         }
//         {
//             const line = iter.next();
//             try testing.expect(line != null);
//             try testing.expectStringStartsWith(line.?, "master_replid:");
//             try testing.expectEqual("master_replid:".len + 40, line.?.len);
//         }
//         {
//             const line = iter.next();
//             try testing.expect(line != null);
//             try testing.expectEqualStrings("master_repl_offset:0", line.?);
//         }
//         {
//             const line = iter.next();
//             try testing.expect(line != null);
//             try testing.expectEqualStrings("\r", line.?);
//         }
//         {
//             const line = iter.next();
//             try testing.expect(line != null);
//             try testing.expectEqualStrings("", line.?);
//         }
//         {
//             const line = iter.next();
//             try testing.expect(line == null);
//         }
//     }
// }

/// Accepts incoming connections and sets up a recv event for each.
fn acceptCallback(
    ud: ?*anyopaque,
    loop: *xev.Loop,
    _: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    const connection_fd: posix.socket_t = result.accept catch unreachable;
    const state = @as(*ServerState, @ptrCast(@alignCast(ud.?)));
    // TODO need to keep track of these created connections in the ServerState so we deinit them when the connection closes.
    const connection = Connection.init(state, connection_fd) catch return .rearm;

    // Create recv event with this accepted socket and trigger the recvCallback.
    connection.recv(loop, recvCallback);
    return .rearm;
}

fn recvCallback(
    ud: ?*anyopaque,
    _: *xev.Loop,
    completion: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    const recv = completion.op.recv;
    const connection = @as(*Connection, @ptrCast(@alignCast(ud.?)));

    std.debug.print("INSIDE recvCallback\n", .{});

    const read_len = result.recv catch {
        // conn.close(loop);
        return .disarm;
    };
    std.debug.print("INSIDE recvCallback, read_len: {d}\n", .{read_len});
    const raw_message = recv.buffer.slice[0..read_len];
    std.debug.print("INSIDE recvCallback, raw message: {s}\n", .{raw_message});
    handleRequestAndRespond(connection.server_state.allocator, raw_message, connection.server_state, recv.fd) catch return .disarm;
    return .disarm;
}
fn sendReplicaUpdates(allocator: std.mem.Allocator, state: *ServerState) !void {
    // For now, just take care of sending the RDB file for all replicas waiting for it.
    const replicas = try state.getReplicaStatesByType(allocator, ServerState.ReplicaStateType.receiving_sync);
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
    _ = allocator;
    _ = state;
    // Send full sync handshake to master
    // const rdb_file = try parser.sendSyncHandshakeToMaster(allocator, state);

    // Take the parsed RDB that master sent and use it to update our state.
    // try loadRdbFile(rdb_file, state);
}
/// Set up a listener at this address, and arm the accept event to handle incoming connections.
fn listenForConnections(address: net.Address, state: *ServerState) !void {
    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();
    try stdout.print("Started listening at address: {}\n", .{address.in});

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    // Create accept event on the listener socket and trigger the acceptCallback.
    var c_accept: xev.Completion = .{
        .op = .{ .accept = .{ .socket = listener.stream.handle } },
        .userdata = state,
        .callback = acceptCallback,
    };
    loop.add(&c_accept);
    try loop.run(.until_done);
}
pub fn runMasterServer(state: *ServerState) !void {
    const our_address = try net.Address.resolveIp("127.0.0.1", state.port);
    try listenForConnections(our_address, state);
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

    try listenForConnections(our_address, state);
}
