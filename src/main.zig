const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();
const Cache = @import("RwLockHashMap.zig").RwLockHashMap;

// Bytes coming in from the client socket are first parsed as a Message, which is then further interpreted as a
// Request. The Request is handled and a Response is produced. Finally, the Response is converted into a Message,
// which is then sent back to the client over the socket as a sequence of bytes.
// NOTE: we have to make these into wrapper classes because otherwise these String types would be aliases to []const u8, and then we can't distinguish between the two in the tagged union.
const SimpleString = struct { value: []const u8 };
const BulkString = struct { value: []const u8 };
const Array = struct { length: u32, elements: std.ArrayList([]const u8) };
const Message = union(enum) {
    simple_string: SimpleString,
    bulk_string: BulkString,
    array: Array,
};

// Messages from the client are parsed as one of these Requests, which are then processed to produce a Response.
const PingCommand = struct { index: u32 };
const EchoCommand = struct { index: u32 };
// Even though the set command takes two args, we only need to store one arg because we're done as soon as we see the second arg.
const SetCommand = struct { index: u32, key: ?[]const u8, value: ?[]const u8, expiry: ?[]const u8 };
const GetCommand = struct { index: u32 };
const Request = union(enum) {
    ping: PingCommand,
    echo: EchoCommand,
    set: SetCommand,
    get: GetCommand,
};

const Error = error{
    MissingExpiryArgument,
};

// The server produces and sends a Response back to the client.
const Response = []const u8;
// TODO implement proper parsing. For now, we're just assuming the structure of incoming messages look like this:
// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
fn get_response(allocator: std.mem.Allocator, request: []const u8, cache: *Cache) !Response {
    var it = std.mem.tokenizeSequence(u8, request, "\r\n");
    var i: u32 = 0;

    var command: ?Request = null;

    while (it.next()) |word| : (i += 1) {
        if (command) |cmd| {
            switch (cmd) {
                .echo => |echo| {
                    if (i == echo.index + 2) {
                        const buf = try allocator.alloc(u8, word.len + 3);
                        return try std.fmt.bufPrint(buf, "+{s}\r\n", .{word});
                    }
                },
                .set => |set| {
                    if (i == set.index + 2) {
                        // Record the requested KEY
                        // NOTE: this is ok since word is a slice originating from the "request" slice. Its lifetime should be the lifetime of this function.
                        command.?.set.key = word;
                        continue;
                    } else if (i == set.index + 4) {
                        // Record the requested VALUE
                        command.?.set.value = word;
                        continue;
                    } else if (i == set.index + 6) {
                        // Check for an expiry argument.
                        if (std.ascii.eqlIgnoreCase(word, "px")) {
                            command.?.set.expiry = word;
                            continue;
                        }
                    } else if (i == set.index + 8) {
                        // Store the key-value with an expiry.
                        if (set.expiry) |_| {
                            const expiry_ms = try std.fmt.parseInt(i64, word, 10);
                            const now_ms = std.time.milliTimestamp();
                            const expiry_timestamp = now_ms + expiry_ms;
                            try cache.putWithExpiry(set.key.?, set.value.?, @as(?i64, expiry_timestamp));
                        } else {
                            return Error.MissingExpiryArgument;
                        }
                        // TODO create a make_response function to make this less ugly.
                        const buf = try allocator.alloc(u8, 5);
                        std.mem.copyForwards(u8, buf, "+OK\r\n");
                        return buf;
                    }
                },
                .get => |get| {
                    if (i == get.index + 2) {
                        const value = cache.get(word);
                        if (value) |val| {
                            var buf: [1024]u8 = undefined;
                            const filled = try std.fmt.bufPrint(&buf, "${d}\r\n{s}\r\n", .{ val.len, val });
                            const response = try allocator.alloc(u8, filled.len);
                            std.mem.copyForwards(u8, response, filled);
                            return response;
                        }
                        const buf = try allocator.alloc(u8, 5);
                        std.mem.copyForwards(u8, buf, "$-1\r\n");
                        return buf;
                    }
                },
                else => {},
            }
        }
        if (std.ascii.eqlIgnoreCase(word, "echo")) {
            // We know it's an echo command, but we don't know the arg yet.
            command = .{ .echo = EchoCommand{ .index = i } };
            continue;
        }
        if (std.ascii.eqlIgnoreCase(word, "ping")) {
            // TODO Ping could have an attached message, handle that (can't return early always).
            const buf = try allocator.alloc(u8, 7);
            std.mem.copyForwards(u8, buf, "+PONG\r\n");
            return buf;
        }
        if (std.ascii.eqlIgnoreCase(word, "set")) {
            // We know it's a set command, but we don't know the args yet.
            command = .{ .set = SetCommand{ .index = i, .key = null, .value = null, .expiry = null } };
            continue;
        }
        if (std.ascii.eqlIgnoreCase(word, "get")) {
            // We know it's a get command, but we don't know the arg yet.
            command = .{ .get = GetCommand{ .index = i } };
            continue;
        }
    }
    // Leftover, was looking for px for SET command but found none.
    // Store the key-value without an expiry.
    if (command) |cmd| {
        switch (cmd) {
            .set => |set| {
                try cache.put(set.key.?, set.value.?);
                const buf = try allocator.alloc(u8, 5);
                std.mem.copyForwards(u8, buf, "+OK\r\n");
                return buf;
            },
            else => {},
        }
    }

    // TODO reply with OK if we don't understand. This is necessary for now because "redis-cli" sometimes sends the COMMANDS command which we don't understand.
    const buf = try allocator.alloc(u8, 5);
    std.mem.copyForwards(u8, buf, "+OK\r\n");
    return buf;
}

fn handleClient(client_connection: net.Server.Connection, cache: *Cache) !void {
    defer client_connection.stream.close();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // TODO handle more than 1024 bytes at a time.
    var buf: [1024]u8 = undefined;
    while (true) {
        const num_read_bytes = client_connection.stream.read(&buf) catch break;
        try stdout.print("Read {d} bytes: {s}\n", .{ num_read_bytes, buf[0..num_read_bytes] });
        if (num_read_bytes == 0) {
            break;
        }
        const response = try get_response(allocator, &buf, cache);
        defer allocator.free(response);

        try client_connection.stream.writeAll(response);
        try stdout.print("Done sending response: {s} to client {} at timestamp {d}\n", .{ response, client_connection.address.in, std.time.milliTimestamp() });
    }
}

pub fn main() !void {
    // TODO do smarter allocation because it's expensive to do all this alloc inside get_response
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
