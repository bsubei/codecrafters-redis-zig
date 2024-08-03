const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();
const Cache = @import("RwLockHashMap.zig");
const parser = @import("parser.zig");
const cli = @import("cli.zig");
const server_config = @import("config.zig");
const Config = server_config.ServerConfig;
const testing = std.testing;

fn handleRequestAndRespond(allocator: std.mem.Allocator, raw_message: []const u8, cache: *Cache, config: *const Config, client_stream: anytype) !void {
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
            try testing.expectEqualStrings("$91\r", line.?);
        }
        {
            const line = iter.next();
            try testing.expect(line != null);
            try testing.expectEqualStrings("", line.?);
        }
        {
            const line = iter.next();
            try testing.expect(line != null);
            try testing.expectEqualStrings("role:master", line.?);
        }
        {
            const line = iter.next();
            try testing.expect(line != null);
            try testing.expectEqualStrings("", line.?);
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
            try testing.expectEqualStrings("", line.?);
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

fn handleClient(client_connection: net.Server.Connection, cache: *Cache, config: *const Config) !void {
    defer client_connection.stream.close();

    // We don't expect each client to use up too much memory, so we use an arena allocator for speed and blow away all the memory at once when we're done.
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // Because the client will send data and wait for our reply before closing the socket connection, we can't just "read all bytes" from the stream
    // then parse them at our leisure, since we would block forever waiting for end of stream which will never come.
    // The clean alternative would be to read until seeing a delimiter ('\n' for example) or eof, but misbehaving clients could just not send either and block us forever.
    // Since I don't know how to make those calls use timeouts, I'll just call read() one chunk at a time (nonblocking) and concatenate them into the final message.
    var raw_message = std.ArrayList(u8).init(allocator);
    // No need to dealloc since we're using an arena allocator.

    while (true) {
        // Read one chunk and append it to the raw_message.
        const num_read_bytes = try parser.readChunk(client_connection, &raw_message);

        // Connection closed, leave if there's no pending raw_message to send.
        if (num_read_bytes == 0 and raw_message.items.len == 0) {
            return;
        }
        // There's possibly more to read for this raw_message! Go back and read another chunk.
        if (num_read_bytes == parser.CLIENT_READER_CHUNK_SIZE) {
            continue;
        }

        try handleRequestAndRespond(allocator, raw_message.items, cache, config, client_connection.stream);

        // Clear the message since the client might send more, but don't actually deallocate so we can reuse this for the next message.
        raw_message.clearRetainingCapacity();
    }
}

fn runMasterServer(allocator: std.mem.Allocator, cache: Cache, config: Config) !void {
    _ = allocator;
    _ = cache;
    _ = config;
}
fn runServer(allocator: std.mem.Allocator, args: cli.Args) !void {
    var config = try server_config.createConfig(args);
    // if (config.replication.role

    var cache = Cache.init(allocator);
    defer cache.deinit();

    const address = try net.Address.resolveIp("127.0.0.1", args.port);

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();
    try stdout.print("Started listening at address: {}\n", .{address.in});

    // Keep listening to new client connections, and once one comes in, set it up to be handled be a new thread and go back to listening.
    while (true) {
        // TODO we delegate the responsibility of closing the connection to the spawned thread, but technically we could have an error before that happens.
        const connection = try listener.accept();

        try stdout.print("accepted new connection from client {}\n", .{connection.address.in.sa.port});
        // TODO join on these threads for correctness.
        // TODO handle errors coming back from these threads.
        const t = try std.Thread.spawn(.{}, handleClient, .{ connection, &cache, &config });
        t.detach();
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var args = try cli.parseArgs(allocator);
    try runServer(allocator, args);
    defer args.deinit();
}
