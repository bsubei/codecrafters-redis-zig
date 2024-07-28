const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();
const Cache = @import("rw_lock_hashmap.zig").RwLockHashMap;

pub const CLIENT_READER_CHUNK_SIZE = 1 << 10;

const Error = error{
    MissingExpiryArgument,
};

// Bytes coming in from the client socket are first parsed as a Message, which is then further interpreted as a
// Request. The Request is handled and a Response is produced. Finally, the Response is converted into a Message,
// which is then sent back to the client over the socket as a sequence of bytes.
// NOTE: we have to make these into wrapper classes because otherwise these String types would be aliases to []const u8, and then we can't distinguish between the two in the tagged union.
const SimpleString = struct { value: []const u8 };
const BulkString = struct { value: []const u8 };
// TODO update Array to actually contain other Message elements.
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

fn strToMessage(raw_message: []const u8) !Message {
    _ = raw_message;

    // The first byte tells you what kind of message this is.

    // Depending on the contents of the string, parse it. Examples:
    // +OK\r\n --> this is a SimpleString containing "OK".
    // $5\r\nhello\r\n --> this is a BulkString containing "hello".
    // *2\r\n$2\r\nhi\r\n$3\r\nbye\r\n --> this is an Array with two BulkStrings: first one contains "hi" and the second contains "bye".

    return error.UnimplementedError;
}

fn messageToRequest(message: Message) !Request {
    _ = message;
    return error.UnimplementedError;
}

pub fn parseRequest(raw_message: []const u8) !Request {
    // Parse the raw_message into a Message.
    // TODO I think we can get away without having to allocate anything for Message or Request. We can just have slices into the raw_message string.
    const message = try strToMessage(raw_message);
    // Parse the Message into a Request.
    return messageToRequest(message);
}

pub fn handleRequest(request: Request, cache: *Cache) !void {
    _ = request;
    _ = cache;
}

// Caller owns returned Message.
fn getResponseMessage(allocator: std.mem.Allocator, request: Request, cache: *Cache) !*Message {
    _ = allocator;
    _ = request;
    _ = cache;
    return error.UnimplementedError;
}

// Caller owns returned string.
fn messageToStr(allocator: std.mem.Allocator, message: *Message) ![]const u8 {
    _ = allocator;
    _ = message;
    return error.UnimplementedError;
}

// Caller owns returned string.
pub fn getResponse(allocator: std.mem.Allocator, request: Request, cache: *Cache) ![]const u8 {
    const response_message = try getResponseMessage(allocator, request, cache);
    defer allocator.destroy(response_message);
    return messageToStr(allocator, response_message);
}

pub fn readChunk(client_connection: net.Server.Connection, message_ptr: *std.ArrayList(u8)) !usize {
    // Read one chunk.
    var buf: [CLIENT_READER_CHUNK_SIZE]u8 = undefined;
    const num_read_bytes = client_connection.stream.read(&buf) catch |err| {
        // Handle retry, otherwise bubble up any errors.
        if (err == error.WouldBlock) return readChunk(client_connection, message_ptr);
        return err;
    };
    try stdout.print("Read {d} bytes: {s}\n", .{ num_read_bytes, buf[0..num_read_bytes] });

    // Save this chunk to the message.
    try message_ptr.appendSlice(buf[0..num_read_bytes]);

    return num_read_bytes;
}

// The server produces and sends a Response back to the client.
const Response = []const u8;
// TODO implement proper parsing. For now, we're just assuming the structure of incoming messages look like this:
// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
// fn getResponseOld(allocator: std.mem.Allocator, request: []const u8, cache: *Cache) !Response {
//     var it = std.mem.tokenizeSequence(u8, request, "\r\n");
//     var i: u32 = 0;
//
//     var command: ?Request = null;
//
//     while (it.next()) |word| : (i += 1) {
//         if (command) |cmd| {
//             switch (cmd) {
//                 .echo => |echo| {
//                     if (i == echo.index + 2) {
//                         // TODO create a make_response function to make this less ugly.
//                         const buf = try allocator.alloc(u8, word.len + 3);
//                         return try std.fmt.bufPrint(buf, "+{s}\r\n", .{word});
//                     }
//                 },
//                 .set => |set| {
//                     if (i == set.index + 2) {
//                         // Record the requested KEY
//                         // NOTE: this is ok since word is a slice originating from the "request" slice. Its lifetime should be the lifetime of this function.
//                         command.?.set.key = word;
//                         continue;
//                     } else if (i == set.index + 4) {
//                         // Record the requested VALUE
//                         command.?.set.value = word;
//                         continue;
//                     } else if (i == set.index + 6) {
//                         // Check for an expiry argument.
//                         if (std.ascii.eqlIgnoreCase(word, "px")) {
//                             command.?.set.expiry = word;
//                             continue;
//                         }
//                     } else if (i == set.index + 8) {
//                         // Store the key-value with an expiry.
//                         if (set.expiry) |_| {
//                             const expiry_ms = try std.fmt.parseInt(i64, word, 10);
//                             const now_ms = std.time.milliTimestamp();
//                             const expiry_timestamp = now_ms + expiry_ms;
//                             try cache.putWithExpiry(set.key.?, set.value.?, @as(?i64, expiry_timestamp));
//                         } else {
//                             return Error.MissingExpiryArgument;
//                         }
//                         return "+OK\r\n";
//                     }
//                 },
//                 .get => |get| {
//                     if (i == get.index + 2) {
//                         const value = cache.get(word);
//                         if (value) |val| {
//                             var buf: [1024]u8 = undefined;
//                             const filled = try std.fmt.bufPrint(&buf, "${d}\r\n{s}\r\n", .{ val.len, val });
//                             const response = try allocator.alloc(u8, filled.len);
//                             std.mem.copyForwards(u8, response, filled);
//                             return response;
//                         }
//                         return "$-1\r\n";
//                     }
//                 },
//                 else => {},
//             }
//         }
//         if (std.ascii.eqlIgnoreCase(word, "echo")) {
//             // We know it's an echo command, but we don't know the arg yet.
//             command = .{ .echo = EchoCommand{ .index = i } };
//             continue;
//         }
//         if (std.ascii.eqlIgnoreCase(word, "ping")) {
//             // TODO Ping could have an attached message, handle that (can't return early always).
//             return "+PONG\r\n";
//         }
//         if (std.ascii.eqlIgnoreCase(word, "set")) {
//             // We know it's a set command, but we don't know the args yet.
//             command = .{ .set = SetCommand{ .index = i, .key = null, .value = null, .expiry = null } };
//             continue;
//         }
//         if (std.ascii.eqlIgnoreCase(word, "get")) {
//             // We know it's a get command, but we don't know the arg yet.
//             command = .{ .get = GetCommand{ .index = i } };
//             continue;
//         }
//     }
//     // Leftover, was looking for px for SET command but found none.
//     // Store the key-value without an expiry.
//     if (command) |cmd| {
//         switch (cmd) {
//             .set => |set| {
//                 try cache.put(set.key.?, set.value.?);
//                 return "+OK\r\n";
//             },
//             else => {},
//         }
//     }
//
//     // TODO reply with OK if we don't understand. This is necessary for now because "redis-cli" sometimes sends the COMMANDS command which we don't understand.
//     return "+OK\r\n";
// }
//
