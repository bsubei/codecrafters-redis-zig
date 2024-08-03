const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();
const testing = std.testing;
const Cache = @import("RwLockHashMap.zig");
const parser = @import("parser.zig");
const cli = @import("cli.zig");
const server_config = @import("config.zig");
const network = @import("network.zig");
const ServerConfig = server_config.ServerConfig;

const Error = error{
    badConfiguration,
};

fn handleRequestAndRespond(allocator: std.mem.Allocator, raw_message: []const u8, cache: *Cache, config: *const ServerConfig, client_stream: anytype) !void {
    // Parse the raw_message into a Request (a command from the client).
    var request = try parser.parseRequest(allocator, raw_message);
    defer request.deinit();

    // Handle the Request (update state).
    try parser.handleRequest(request, cache);

    // Generate a Response to the Request as a string.
    const response_str = try parser.getResponse(allocator, request, cache, config);
    defer allocator.free(response_str);

    // Send the Response back to the client.
    try client_stream.writeAll(response_str);
}

test "handleRequestAndRespond" {
    var cache = Cache.init(testing.allocator);
    defer cache.deinit();
    const master_args = cli.Args{ .port = 123, .replicaof = null, .allocator = testing.allocator };
    const master_config = try server_config.createConfig(master_args);
    {
        var buffer: [64]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buffer);
        try handleRequestAndRespond(testing.allocator, "*2\r\n$4\r\nECHO\r\n$13\r\nHello, world!\r\n", &cache, &master_config, fbs.writer());
        try testing.expectEqualSlices(u8, "$13\r\nHello, world!\r\n", fbs.getWritten());
    }
    {
        var buffer: [64]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buffer);
        try handleRequestAndRespond(testing.allocator, "*2\r\n$3\r\ngEt\r\n$13\r\nHello, world!\r\n", &cache, &master_config, fbs.writer());
        try testing.expectEqualSlices(u8, "$-1\r\n", fbs.getWritten());
    }
    {
        var buffer: [64]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buffer);
        try handleRequestAndRespond(testing.allocator, "*3\r\n$3\r\nSEt\r\n$13\r\nHello, world!\r\n$4\r\nbye!\r\n", &cache, &master_config, fbs.writer());
        try testing.expectEqualSlices(u8, "+OK\r\n", fbs.getWritten());
    }
    {
        var buffer: [64]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buffer);
        try handleRequestAndRespond(testing.allocator, "*2\r\n$3\r\ngEt\r\n$13\r\nHello, world!\r\n", &cache, &master_config, fbs.writer());
        try testing.expectEqualSlices(u8, "$4\r\nbye!\r\n", fbs.getWritten());
    }
    {
        var buffer: [128]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buffer);
        try handleRequestAndRespond(testing.allocator, "*2\r\n$4\r\nINfo\r\n$11\r\nrePLicAtion\r\n", &cache, &master_config, fbs.writer());
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

fn handleClient(client_stream: net.Stream, config: *const ServerConfig, cache: *Cache) !void {
    defer client_stream.close();

    // We don't expect each client to use up too much memory, so we use an arena allocator for speed and blow away all the memory at once when we're done.
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var raw_message = std.ArrayList(u8).init(allocator);
    defer raw_message.deinit();

    while (true) {
        const num_read_bytes = try network.readFromStream(client_stream, &raw_message);
        if (num_read_bytes == 0) return;

        try handleRequestAndRespond(allocator, raw_message.items, cache, config, client_stream);

        // Clear the message since the client might send more, but don't actually deallocate so we can reuse this for the next message.
        raw_message.clearRetainingCapacity();
    }
}

fn loadRdbFile(rdb: parser.RdbFile, cache: *Cache) !void {
    // TODO take the parsed RDB that master sent and use it to update our state.
    _ = rdb;
    _ = cache;
}
fn syncWithMaster(allocator: std.mem.Allocator, master_stream: net.Stream, cache: *Cache) !void {
    // Send full sync handshake to master
    const rdb = try parser.sendSyncHandshakeToMaster(allocator, master_stream);

    // Take the parsed RDB that master sent and use it to update our state.
    try loadRdbFile(rdb, cache);
}
fn listenForClientsAndHandleRequests(address: net.Address, config: *const ServerConfig, cache: *Cache) !void {
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
        const t = try std.Thread.spawn(.{}, handleClient, .{ connection.stream, config, cache });
        t.detach();
    }
}
pub fn runMasterServer(args: *const cli.Args, config: *const ServerConfig, cache: *Cache) !void {
    const our_address = try net.Address.resolveIp("127.0.0.1", args.port);
    try listenForClientsAndHandleRequests(our_address, config, cache);
}
pub fn runSlaveServer(args: *const cli.Args, config: *const ServerConfig, cache: *Cache) !void {
    const our_address = try net.Address.resolveIp("127.0.0.1", args.port);

    // Set up an arena allocator backed by a page allocator. We only need it for syncWithMaster.
    {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        // Connect to master.
        if (args.replicaof == null) return Error.badConfiguration;
        var master_stream = try std.net.tcpConnectToHost(allocator, args.replicaof.?.master_host, args.replicaof.?.master_port);
        defer master_stream.close();

        try syncWithMaster(allocator, master_stream, cache);
    }

    try listenForClientsAndHandleRequests(our_address, config, cache);
}
