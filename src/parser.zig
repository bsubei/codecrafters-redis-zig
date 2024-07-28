const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();
const Cache = @import("rw_lock_hashmap.zig").RwLockHashMap;
const testing = std.testing;

pub const CLIENT_READER_CHUNK_SIZE = 1 << 10;
const CRLF_DELIMITER = "\r\n";

const Error = error{
    MissingExpiryArgument,
    UnknownMessageType,
    MissingDelimiter,
    BulkStringBadLengthHeader,
};

// Bytes coming in from the client socket are first parsed as a Message, which is then further interpreted as a
// Request (a command from the client). The server then handles the Request (state updates are applied) and a response Message is produced.
// Finally, the response Message is sent back to the client as a sequence of bytes.
// NOTE: we have to make these into wrapper classes because otherwise these String types would be aliases to []const u8, and then we can't distinguish between the two in the tagged union.
const SimpleString = struct {
    value: []const u8,

    fn parse(raw_message: []const u8) !@This() {
        const skip_first = raw_message[1..];
        const index = std.mem.indexOf(u8, skip_first, CRLF_DELIMITER);
        if (index) |idx| {
            return .{ .value = skip_first[0..idx] };
        }
        return Error.MissingDelimiter;
    }
};

const BulkString = struct {
    value: []const u8,

    fn parse(raw_message: []const u8) !@This() {
        const skip_first = raw_message[1..];
        const index_crlf = std.mem.indexOf(u8, skip_first, CRLF_DELIMITER);
        if (index_crlf) |idx_crlf| {
            const length_header = skip_first[0..idx_crlf];
            if (length_header.len == 0) {
                return Error.BulkStringBadLengthHeader;
            }
            // Special case, handle null bulk string.
            // TODO we can't distinguish between an actual null BulkString from an empty BulkString but maybe that's fine.
            if (std.mem.eql(u8, length_header, "-1") or length_header[0] == '0') {
                return .{ .value = "" };
            }
            const num_chars = std.fmt.parseInt(u32, length_header, 10) catch return Error.BulkStringBadLengthHeader;
            const text_idx_start = idx_crlf + CRLF_DELIMITER.len;
            const text_idx_end = text_idx_start + num_chars;
            return .{ .value = skip_first[text_idx_start..text_idx_end] };
        }
        return Error.MissingDelimiter;
    }
};
const Array = struct {
    elements: std.ArrayList(Message),

    fn parse(raw_message: []const u8) !@This() {
        // TODO
        _ = raw_message;
        return .{ .elements = undefined };
    }
};

const MessageType = enum {
    simple_string,
    bulk_string,
    array,
    const Self = @This();
    fn fromByte(byte: u8) !@This() {
        return switch (byte) {
            '+' => Self.simple_string,
            '$' => Self.bulk_string,
            '*' => Self.array,
            else => Error.UnknownMessageType,
        };
    }
};
const Message = union(MessageType) {
    simple_string: SimpleString,
    bulk_string: BulkString,
    array: Array,

    const Self = @This();
    fn parse(raw_message: []const u8, message_type: MessageType) !Self {
        // TODO less boilerplate, probably using builtins
        return switch (message_type) {
            .simple_string => .{ .simple_string = try SimpleString.parse(raw_message) },
            .bulk_string => .{ .bulk_string = try BulkString.parse(raw_message) },
            .array => .{ .array = try Array.parse(raw_message) },
        };
    }
    // TODO I think we have to allocate the Message here because of the Array being recursive.
    fn fromStr(raw_message: []const u8) !Self {
        // The first byte tells you what kind of message this is.
        const message_type = try MessageType.fromByte(raw_message[0]);

        // Depending on the contents of the string, parse it. Examples:
        // +OK\r\n --> this is a SimpleString containing "OK".
        // $5\r\nhello\r\n --> this is a BulkString containing "hello".
        // *2\r\n$2\r\nhi\r\n$3\r\nbye\r\n --> this is an Array with two BulkStrings: first one contains "hi" and the second contains "bye".
        return parse(raw_message, message_type);
    }
    // Caller owns returned string.
    fn toStr(self: *Self, allocator: std.mem.Allocator) ![]const u8 {
        _ = self;
        _ = allocator;
        return error.UnimplementedError;
    }
};

test "parse Message Unknown type" {
    try testing.expectError(Error.UnknownMessageType, Message.fromStr(".cannot parse this\r\n"));
}
test "parse Message missing delimiter" {
    try testing.expectError(Error.MissingDelimiter, Message.fromStr("+I will ramble on forever with no end on and on and on and"));
}
test "parse Message SimpleString" {
    try testing.expectEqualSlices(u8, (try Message.fromStr("+\r\n")).simple_string.value, "");
    try testing.expectEqualSlices(u8, (try Message.fromStr("+Hi there, my name is Zog!\r\n")).simple_string.value, "Hi there, my name is Zog!");
}
test "parse Message BulkString" {
    try testing.expectEqualSlices(u8, (try Message.fromStr("$1\r\nx\r\n")).bulk_string.value, "x");
    try testing.expectEqualSlices(u8, "1234567890", (try Message.fromStr("$10\r\n1234567890\r\n")).bulk_string.value);
    try testing.expectEqualSlices(u8, "", (try Message.fromStr("$-1\r\n")).bulk_string.value);
    try testing.expectEqualSlices(u8, "", (try Message.fromStr("$0\r\n\r\n")).bulk_string.value);
}
test "parse Message BulkString bad length header" {
    try testing.expectError(Error.BulkStringBadLengthHeader, Message.fromStr("$\r\n\r\n"));
    try testing.expectError(Error.BulkStringBadLengthHeader, Message.fromStr("$not_an_int\r\nhibye\r\n"));
    try testing.expectError(Error.BulkStringBadLengthHeader, Message.fromStr("$5.0\r\nhibye\r\n"));
}

// Messages from the client are parsed as one of these Requests, which are then processed to produce a Response.
const PingCommand = struct { contents: ?[]const u8 };
const EchoCommand = struct { contents: []const u8 };
const SetCommand = struct { key: []const u8, value: []const u8, expiry: Cache.ExpiryTimestampMs };
const GetCommand = struct { key: []const u8 };
const Request = union(enum) {
    ping: PingCommand,
    echo: EchoCommand,
    set: SetCommand,
    get: GetCommand,
};

fn messageToRequest(message: Message) !Request {
    _ = message;
    return error.UnimplementedError;
}

pub fn parseRequest(raw_message: []const u8) !Request {
    // Parse the raw_message into a Message.
    // TODO I think we can get away without having to allocate anything for Message or Request. We can just have slices into the raw_message string.
    const message = try Message.fromStr(raw_message);
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
pub fn getResponse(allocator: std.mem.Allocator, request: Request, cache: *Cache) ![]const u8 {
    const response_message = try getResponseMessage(allocator, request, cache);
    defer allocator.destroy(response_message);
    return response_message.toStr(allocator);
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
