const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();
const Cache = @import("RwLockHashMap.zig").RwLockHashMap;

// Bytes coming in from the client socket are first parsed as a Message, which is then further interpreted as a
// Request. The Request is handled and a Response is produced. Finally, the Response is converted into a Message,
// which is then sent back to the client over the socket as a sequence of bytes.
const SimpleString = []const u8;
const BulkString = []const u8;
const Array = struct { length: u32, elements: std.ArrayList([]const u8) };
const Message = union(enum) {
    simple_string: SimpleString,
    bulk_string: BulkString,
    array: Array,
};

// Messages from the client are parsed as one of these Requests, which are then processed to produce a Response.
const PingCommand = struct { index: u32, arg: ?[]const u8 };
const EchoCommand = struct { index: u32, arg: ?[]const u8 };
// Even though the set command takes two args, we only need to store one arg because we're done as soon as we see the second arg.
const SetCommand = struct { index: u32, arg: ?[]const u8 };
const Request = union(enum) {
    ping: PingCommand,
    echo: EchoCommand,
    set: SetCommand,
};

// The server produces and sends a Response back to the client.
const Response = []const u8;
// TODO implement proper parsing. For now, we're just assuming the structure of incoming messages look like this:
// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
fn get_response(request: []const u8, cache: *Cache) !Response {
    var it = std.mem.tokenizeSequence(u8, request, "\r\n");
    var i: u32 = 0;

    var command: ?Request = null;

    while (it.next()) |word| : (i += 1) {
        if (command) |cmd| {
            switch (cmd) {
                .echo => |echo| {
                    if (i == echo.index + 2) {
                        var buf: [1024]u8 = undefined;
                        return try std.fmt.bufPrint(&buf, "+{s}\r\n", .{word});
                    }
                },
                .set => |set| {
                    if (i == set.index + 2) {
                        // Record the requested KEY
                        // TODO do we need a memcpy here?
                        command.?.set.arg = word;
                    } else if (i == set.index + 4) {
                        // Store the key-value into the cache.
                        // TODO this is wrong, we need to make copies. Right now it's just reusing the memory locations of these vars on the stack.
                        try cache.put(set.arg.?, word);
                        return "+OK\r\n";
                    }
                },
                else => {},
            }
        }
        if (std.ascii.eqlIgnoreCase(word, "echo")) {
            // We know it's an echo command, but we don't know the arg yet.
            command = .{ .echo = EchoCommand{ .index = i, .arg = null } };
            continue;
        }
        if (std.ascii.eqlIgnoreCase(word, "ping")) {
            // TODO Ping could have an attached message, handle that (can't return early always).
            return "+PONG\r\n";
        }
        if (std.ascii.eqlIgnoreCase(word, "set")) {
            // We know it's a set command, but we don't know the args yet.
            command = .{ .set = SetCommand{ .index = i, .arg = null } };
            continue;
        }
    }
    // TODO reply with OK if we don't understand. This is necessary for now because "redis-cli" sometimes sends the COMMANDS command which we don't understand.
    return "+OK\r\n";
}

fn handleClient(client_connection: net.Server.Connection, cache: *Cache) !void {
    defer client_connection.stream.close();

    // TODO handle more than 1024 bytes at a time.
    var buf: [1024]u8 = undefined;
    while (true) {
        const num_read_bytes = client_connection.stream.read(&buf) catch break;
        try stdout.print("Read {d} bytes: {s}\n", .{ num_read_bytes, buf[0..num_read_bytes] });
        if (num_read_bytes == 0) {
            break;
        }
        const response = try get_response(&buf, cache);
        try client_connection.stream.writeAll(response);
        try stdout.print("Done sending response: {s} to client {}\n", .{ response, client_connection.address.in });
        try cache.print();
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    var cache = Cache.init(allocator);
    defer cache.deinit();

    const address = try net.Address.resolveIp("127.0.0.1", 6379);

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
        const t = try std.Thread.spawn(.{}, handleClient, .{ connection, &cache });
        t.detach();
    }
}
