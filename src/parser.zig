//! Bytes coming in from the client socket are first parsed as a Message, which is then further interpreted as a
//! Request (a command from the client). The server then handles the Request (state updates are applied) and a response Message is produced.
//! Finally, the response Message is sent back to the client as a sequence of bytes.

const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();
const Cache = @import("RwLockHashMap.zig");
const ServerConfig = @import("config.zig").ServerConfig;
const testing = std.testing;
const string_utils = @import("string_utils.zig");

const CRLF_DELIMITER = "\r\n";

const Error = error{
    MissingExpiryArgument,
    UnknownMessageType,
    MissingDelimiter,
    BulkStringBadLengthHeader,
    ArrayBadLengthHeader,
    ArrayInvalidNestedMessage,
    OutOfMemory,
    InvalidRequest,
    InvalidRequestEmptyMessage,
    InvalidRequestNumberOfArgs,
    UnimplementedNonArrayRequest,
};

// NOTE: we have to make these into wrapper classes because otherwise these String types would be aliases to []const u8, and then we can't distinguish between the two in the tagged union.
const SimpleString = struct {
    value: []const u8,
    allocator: ?std.mem.Allocator = null,

    const Self = @This();

    fn fromStr(raw_message: []const u8) Error!Self {
        const rest_of_text = raw_message[1..];
        const index = std.mem.indexOf(u8, rest_of_text, CRLF_DELIMITER);
        if (index) |idx| {
            return .{ .value = rest_of_text[0..idx] };
        }
        return Error.MissingDelimiter;
    }
    fn fromStrAlloc(allocator: std.mem.Allocator, raw_message: []const u8) Error!Self {
        const str = try fromStr(raw_message);
        return .{ .allocator = allocator, .value = try allocator.dupe(u8, str.value) };
    }
    fn toStr(self: *const Self, allocator: std.mem.Allocator) Error![]const u8 {
        return std.fmt.allocPrint(allocator, "+{s}{s}", .{ self.value, CRLF_DELIMITER });
    }
};

const BulkString = struct {
    value: []const u8,
    allocator: ?std.mem.Allocator = null,

    const Self = @This();
    fn fromStr(raw_message: []const u8) Error!Self {
        const rest_of_text = raw_message[1..];
        const index_crlf = std.mem.indexOf(u8, rest_of_text, CRLF_DELIMITER);
        if (index_crlf) |idx_crlf| {
            const length_header = rest_of_text[0..idx_crlf];
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
            return .{ .value = rest_of_text[text_idx_start..text_idx_end] };
        }
        return Error.MissingDelimiter;
    }
    fn fromStrAlloc(allocator: std.mem.Allocator, raw_message: []const u8) Error!Self {
        const str = try fromStr(raw_message);
        return .{ .allocator = allocator, .value = try allocator.dupe(u8, str.value) };
    }
    fn toStr(self: *const Self, allocator: std.mem.Allocator) Error![]const u8 {
        // Special case, handle null bulk string.
        if (self.value.len == 0) {
            return std.fmt.allocPrint(allocator, "$-1{s}", .{CRLF_DELIMITER});
        }
        return std.fmt.allocPrint(allocator, "${d}{s}{s}{s}", .{ self.value.len, CRLF_DELIMITER, self.value, CRLF_DELIMITER });
    }
};
const Array = struct {
    elements: []const Message,
    allocator: std.mem.Allocator,

    const Self = @This();
    fn fromStr(allocator: std.mem.Allocator, raw_message: []const u8) Error!Self {
        var rest_of_text = raw_message[1..];
        const index_crlf = std.mem.indexOf(u8, rest_of_text, CRLF_DELIMITER);
        if (index_crlf) |idx_crlf| {
            const num_elements_header = rest_of_text[0..idx_crlf];
            if (num_elements_header.len == 0) {
                return Error.ArrayBadLengthHeader;
            }
            const num_elements = std.fmt.parseInt(u32, num_elements_header, 10) catch return Error.ArrayBadLengthHeader;
            const elements = try allocator.alloc(Message, num_elements);
            errdefer allocator.free(elements);

            // Chomp off (remove) everything we've read so far.
            rest_of_text = rest_of_text[idx_crlf + CRLF_DELIMITER.len ..];
            // For each element, parse one Message from the remainder of the text
            for (0..elements.len) |i| {
                var message = try Message.fromStr(allocator, rest_of_text);
                errdefer message.deinit();
                switch (message) {
                    // Disallow nested messages that are themselves arrays.
                    .array => return Error.ArrayInvalidNestedMessage,
                    .simple_string => {
                        // Save the message we just parsed.
                        elements[i] = message;

                        // Skip over one CRLF to prepare us for parsing the next message.
                        const nested_index_crlf = std.mem.indexOf(u8, rest_of_text, CRLF_DELIMITER);
                        if (nested_index_crlf) |nested_idx_crlf| {
                            rest_of_text = rest_of_text[nested_idx_crlf + CRLF_DELIMITER.len ..];
                        } else {
                            return Error.MissingDelimiter;
                        }
                    },
                    .bulk_string => {
                        // Save the message we just parsed.
                        elements[i] = message;

                        // Skip over two CRLF to prepare us for parsing the next message.
                        const nested_index_crlf = std.mem.indexOf(u8, rest_of_text, CRLF_DELIMITER);
                        if (nested_index_crlf) |nested_idx_crlf| {
                            rest_of_text = rest_of_text[nested_idx_crlf + CRLF_DELIMITER.len ..];
                            const again_index = std.mem.indexOf(u8, rest_of_text, CRLF_DELIMITER);
                            if (again_index) |again_idx| {
                                rest_of_text = rest_of_text[again_idx + CRLF_DELIMITER.len ..];
                            }
                        } else {
                            return Error.MissingDelimiter;
                        }
                    },
                }
            }

            return .{ .elements = elements, .allocator = allocator };
        }

        return .{ .elements = try allocator.alloc(Message, 0), .allocator = allocator };
    }
    fn toStr(self: *const Self, allocator: std.mem.Allocator) Error![]const u8 {
        // Start out with the header.
        var final_string = try std.fmt.allocPrint(allocator, "*{d}{s}", .{
            self.elements.len,
            CRLF_DELIMITER,
        });

        // Keep appending to the final_string as we see each new message.
        for (self.elements) |message| {
            // This message string is temporary and not needed once we concatenate it using allocPrint.
            const msg_str = try message.toStr(allocator);
            defer allocator.free(msg_str);

            // Append this message's string contents to the final string.
            const tmp = try std.fmt.allocPrint(allocator, "{s}{s}", .{ final_string, msg_str });
            // Get rid of the old "final string" and set the new one.
            allocator.free(final_string);
            final_string = tmp;
        }
        return final_string;
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
/// A Message may or may not own the underlying string data, depending on whether its allocator field is set or not.
const Message = union(MessageType) {
    simple_string: SimpleString,
    bulk_string: BulkString,
    array: Array,

    const Self = @This();

    /// Frees only the Message slice when this Message is an Array. Does not free the contents (strings), since those are not owned by the Messages.
    pub fn deinit(self: *Self) void {
        switch (self.*) {
            .simple_string => |s| {
                if (s.allocator) |alloc| alloc.free(s.value);
            },
            .bulk_string => |b| {
                if (b.allocator) |alloc| alloc.free(b.value);
            },
            .array => |arr| {
                // NOTE: we don't need to recursively free because our elements are guaranteed never to be Arrays themselves.
                arr.allocator.free(arr.elements);
            },
        }
    }
    /// Caller is responsible for calling deinit() on returned Message, which is only strictly necessary when the returned Message is an Array.
    /// Depending on the contents of the string, parse it. Examples:
    /// +OK\r\n --> this is a SimpleString containing "OK".
    /// $5\r\nhello\r\n --> this is a BulkString containing "hello".
    /// *2\r\n$2\r\nhi\r\n$3\r\nbye\r\n --> this is an Array with two BulkStrings: first one contains "hi" and the second contains "bye".
    fn fromStr(allocator: std.mem.Allocator, raw_message: []const u8) !Self {
        // The first byte tells you what kind of message this is.
        const message_type = try MessageType.fromByte(raw_message[0]);

        // TODO less boilerplate, probably using builtins
        return switch (message_type) {
            .simple_string => return .{ .simple_string = try SimpleString.fromStr(raw_message) },
            .bulk_string => return .{ .bulk_string = try BulkString.fromStr(raw_message) },
            .array => return .{ .array = try Array.fromStr(allocator, raw_message) },
        };
    }
    /// Caller owns returned string.
    fn toStr(self: *const Self, allocator: std.mem.Allocator) Error![]const u8 {
        switch (self.*) {
            .simple_string => {
                return self.simple_string.toStr(allocator);
            },
            .bulk_string => {
                return self.bulk_string.toStr(allocator);
            },
            .array => {
                return self.array.toStr(allocator);
            },
        }
    }

    /// Just gets a view of the underlying string. Array messages are not supported.
    fn get_contents(self: *const Self) Error![]const u8 {
        return switch (self.*) {
            .simple_string => |s| s.value,
            .bulk_string => |b| b.value,
            else => return Error.ArrayInvalidNestedMessage,
        };
    }
};

test "parse Message fromStr Unknown type" {
    try testing.expectError(Error.UnknownMessageType, Message.fromStr(testing.allocator, ".cannot parse this\r\n"));
}
test "parse Message fromStr missing delimiter" {
    try testing.expectError(Error.MissingDelimiter, Message.fromStr(testing.allocator, "+I will ramble on forever with no end on and on and on and"));
}
test "parse Message fromStr SimpleString" {
    try testing.expectEqualSlices(u8, "", (try Message.fromStr(testing.allocator, "+\r\n")).simple_string.value);
    try testing.expectEqualSlices(u8, "Hi there, my name is Zog!", (try Message.fromStr(testing.allocator, "+Hi there, my name is Zog!\r\n")).simple_string.value);
}
test "parse Message fromStr BulkString" {
    try testing.expectEqualSlices(u8, "x", (try Message.fromStr(testing.allocator, "$1\r\nx\r\n")).bulk_string.value);
    try testing.expectEqualSlices(u8, "1234567890", (try Message.fromStr(testing.allocator, "$10\r\n1234567890\r\n")).bulk_string.value);
    try testing.expectEqualSlices(u8, "", (try Message.fromStr(testing.allocator, "$-1\r\n")).bulk_string.value);
    try testing.expectEqualSlices(u8, "", (try Message.fromStr(testing.allocator, "$0\r\n\r\n")).bulk_string.value);
}
test "parse Message fromStr BulkString bad length header" {
    try testing.expectError(Error.BulkStringBadLengthHeader, Message.fromStr(testing.allocator, "$\r\n\r\n"));
    try testing.expectError(Error.BulkStringBadLengthHeader, Message.fromStr(testing.allocator, "$not_an_int\r\nhibye\r\n"));
    try testing.expectError(Error.BulkStringBadLengthHeader, Message.fromStr(testing.allocator, "$5.0\r\nhibye\r\n"));
}
test "parse Message fromStr Array bad length header" {
    try testing.expectError(Error.ArrayBadLengthHeader, Message.fromStr(testing.allocator, "*\r\n+NOK\r\n"));
    try testing.expectError(Error.ArrayBadLengthHeader, Message.fromStr(testing.allocator, "*this isn't a number\r\n+OK\r\n"));
    try testing.expectError(Error.ArrayBadLengthHeader, Message.fromStr(testing.allocator, "*-1\r\n+OK\r\n"));
    try testing.expectError(Error.ArrayBadLengthHeader, Message.fromStr(testing.allocator, "*1.0\r\n+OK\r\n"));
}
test "parse Message fromStr Array invalid nested message" {
    try testing.expectError(Error.ArrayInvalidNestedMessage, Message.fromStr(testing.allocator, "*1\r\n*1\r\n+OK\r\n"));
    try testing.expectError(Error.ArrayInvalidNestedMessage, Message.fromStr(testing.allocator, "*2\r\n$2\r\nhi\r\n*1\r\n+OK\r\n"));
}
test "parse Message fromStr Array" {
    {
        var message = try Message.fromStr(testing.allocator, "*0\r\n");
        defer message.deinit();
        try testing.expectEqual(0, message.array.elements.len);
    }
    {
        var message = try Message.fromStr(testing.allocator, "*1\r\n+OK\r\n");
        defer message.deinit();
        try testing.expectEqual(1, message.array.elements.len);
        try testing.expectEqualSlices(u8, "OK", message.array.elements[0].simple_string.value);
    }
    {
        var message = try Message.fromStr(testing.allocator, "*1\r\n$4\r\nhiya\r\n");
        defer message.deinit();
        try testing.expectEqual(1, message.array.elements.len);
        try testing.expectEqualSlices(u8, "hiya", message.array.elements[0].bulk_string.value);
    }
    {
        var message = try Message.fromStr(testing.allocator, "*2\r\n$4\r\nnope\r\n+bye\r\n");
        defer message.deinit();
        try testing.expectEqual(2, message.array.elements.len);
        try testing.expectEqualSlices(u8, "nope", message.array.elements[0].bulk_string.value);
        try testing.expectEqualSlices(u8, "bye", message.array.elements[1].simple_string.value);
    }
}
test "Message toStr SimpleString" {
    {
        const message = Message{ .simple_string = .{ .value = "hello" } };
        const msg_str = try message.toStr(testing.allocator);
        defer testing.allocator.free(msg_str);
        try testing.expectEqualSlices(u8, "+hello\r\n", msg_str);
    }
}
test "Message toStr BulkString" {
    {
        const message = Message{ .bulk_string = .{ .value = "hello" } };
        const msg_str = try message.toStr(testing.allocator);
        defer testing.allocator.free(msg_str);
        try testing.expectEqualSlices(u8, "$5\r\nhello\r\n", msg_str);
    }
    {
        const message = Message{ .bulk_string = .{ .value = "" } };
        const msg_str = try message.toStr(testing.allocator);
        defer testing.allocator.free(msg_str);
        try testing.expectEqualSlices(u8, "$-1\r\n", msg_str);
    }
}
test "Message toStr Array" {
    {
        const first = .{ .bulk_string = .{ .value = "first" } };
        const last = .{ .simple_string = .{ .value = "last" } };
        const message = Message{ .array = .{ .elements = &[_]Message{ first, last }, .allocator = testing.allocator } };
        const msg_str = try message.toStr(testing.allocator);
        defer testing.allocator.free(msg_str);
        try testing.expectEqualSlices(u8, "*2\r\n$5\r\nfirst\r\n+last\r\n", msg_str);
    }
}
test "Message roundtrip SimpleString" {
    const msg = Message{ .simple_string = .{ .value = "last" } };
    const msg_str = try msg.toStr(testing.allocator);
    defer testing.allocator.free(msg_str);
    const msg_again = try Message.fromStr(testing.allocator, msg_str);
    try testing.expectEqualSlices(u8, msg.simple_string.value, msg_again.simple_string.value);
}
test "Message roundtrip BulkString" {
    const msg = Message{ .bulk_string = .{ .value = "arbitrary" } };
    const msg_str = try msg.toStr(testing.allocator);
    defer testing.allocator.free(msg_str);
    const msg_again = try Message.fromStr(testing.allocator, msg_str);
    try testing.expectEqualSlices(u8, msg.bulk_string.value, msg_again.bulk_string.value);
}
test "Message roundtrip Array" {
    const first = .{ .bulk_string = .{ .value = "first" } };
    const last = .{ .simple_string = .{ .value = "last" } };
    const msg = Message{ .array = .{ .elements = &[_]Message{ first, last }, .allocator = testing.allocator } };
    const msg_str = try msg.toStr(testing.allocator);
    defer testing.allocator.free(msg_str);
    var msg_again = try Message.fromStr(testing.allocator, msg_str);
    defer msg_again.deinit();
    try testing.expectEqual(msg.array.elements.len, msg_again.array.elements.len);
    try testing.expectEqualSlices(u8, msg.array.elements[0].bulk_string.value, msg_again.array.elements[0].bulk_string.value);
    try testing.expectEqualSlices(u8, msg.array.elements[1].simple_string.value, msg_again.array.elements[1].simple_string.value);
}
// TODO add tests for case when SimpleString and BulkString have allocators and use them to free.

const PingCommand = struct { contents: ?[]const u8 };
const EchoCommand = struct { contents: []const u8 };
const SetCommand = struct { key: []const u8, value: []const u8, expiry: Cache.ExpiryTimestampMs };
const GetCommand = struct { key: []const u8 };
const InfoCommand = struct { section_keys: [][]const u8 };
const CommandType = enum {
    ping,
    echo,
    set,
    get,
    info,
};
const Command = union(CommandType) {
    ping: PingCommand,
    echo: EchoCommand,
    set: SetCommand,
    get: GetCommand,
    info: InfoCommand,
};

/// Messages from the client are parsed as one of these Requests, which are then processed to produce a Response.
const Request = struct {
    allocator: std.mem.Allocator,
    command: Command,

    const Self = @This();
    pub fn deinit(self: *Self) void {
        switch (self.command) {
            .ping => |p| {
                if (p.contents != null) self.allocator.free(p.contents.?);
            },
            .echo => |e| {
                self.allocator.free(e.contents);
            },
            .set => |s| {
                self.allocator.free(s.key);
                self.allocator.free(s.value);
            },
            .get => |g| {
                self.allocator.free(g.key);
            },
            .info => |i| {
                for (i.section_keys) |key| {
                    self.allocator.free(key);
                }
                self.allocator.free(i.section_keys);
            },
        }
    }
};

fn messageToRequest(allocator: std.mem.Allocator, message: Message) !Request {
    switch (message) {
        .array => {
            const messages = message.array.elements;
            if (messages.len == 0) return Error.InvalidRequestEmptyMessage;
            // The first word is the command. Assume the message elements are not Arrays.
            const first_word = try messages[0].get_contents();

            // TODO surely I can get a comptime func to match against the first_word here? Or at least just get the name of the enum field
            if (std.ascii.eqlIgnoreCase(first_word, "ping")) {
                switch (messages.len) {
                    1 => return Request{ .command = Command{ .ping = .{ .contents = null } }, .allocator = undefined },
                    2 => return Request{ .command = Command{ .ping = .{ .contents = @as(?[]const u8, try allocator.dupe(u8, try messages[1].get_contents())) } }, .allocator = allocator },
                    else => {
                        return Error.InvalidRequestNumberOfArgs;
                    },
                }
            }
            if (std.ascii.eqlIgnoreCase(first_word, "echo")) {
                switch (messages.len) {
                    2 => return Request{ .command = Command{ .echo = .{ .contents = try allocator.dupe(u8, try messages[1].get_contents()) } }, .allocator = allocator },
                    else => {
                        return Error.InvalidRequestNumberOfArgs;
                    },
                }
            }
            if (std.ascii.eqlIgnoreCase(first_word, "get")) {
                switch (messages.len) {
                    2 => return Request{ .command = Command{ .get = .{ .key = try allocator.dupe(u8, try messages[1].get_contents()) } }, .allocator = allocator },
                    else => {
                        return Error.InvalidRequestNumberOfArgs;
                    },
                }
            }
            if (std.ascii.eqlIgnoreCase(first_word, "set")) {
                switch (messages.len) {
                    3, 5 => {
                        const key = try allocator.dupe(u8, try messages[1].get_contents());
                        errdefer allocator.free(key);
                        const value = try allocator.dupe(u8, try messages[2].get_contents());
                        errdefer allocator.free(value);

                        // If PX and timestamp are provided, also set the expiry field.
                        const expiry = if (messages.len == 5 and std.ascii.eqlIgnoreCase(try messages[3].get_contents(), "px"))
                            try std.fmt.parseInt(i64, try messages[4].get_contents(), 10) + std.time.milliTimestamp()
                        else
                            null;

                        return Request{ .command = Command{ .set = .{ .key = key, .value = value, .expiry = expiry } }, .allocator = allocator };
                    },
                    else => {
                        return Error.InvalidRequestNumberOfArgs;
                    },
                }
            }
            if (std.ascii.eqlIgnoreCase(first_word, "info")) {
                // TODO we should dedupe any given section keys.
                // This seems like a bunch of unnecessary allocating, but it's ok since we're using an arena allocator backed by a page allocator.
                const section_messages = messages[1..];
                var temp_list = std.ArrayList([]const u8).init(allocator);
                errdefer temp_list.deinit();
                for (section_messages) |msg| {
                    try temp_list.append(try allocator.dupe(u8, try msg.get_contents()));
                }
                // We transfer ownership of the section_keys within the Request to the caller.
                return Request{ .command = Command{ .info = .{ .section_keys = try temp_list.toOwnedSlice() } }, .allocator = allocator };
            }
        },
        else => {
            // TODO think about implementing simple_string or bulk_string as a request.
            return Error.UnimplementedNonArrayRequest;
        },
    }
    return Error.InvalidRequest;
}

/// Parse and interpret the given raw_message as a Request from a client. The caller owns the Request and is responsible for calling deinit().
pub fn parseRequest(allocator: std.mem.Allocator, raw_message: []const u8) !Request {
    // Parse the raw_message into a Message. Make sure to free the message contents when we're done using it.
    var message = try Message.fromStr(allocator, raw_message);
    defer message.deinit();

    // Parse the Message into a Request.
    return messageToRequest(allocator, message);
}
test "parseRequest PingCommand" {
    {
        var request = try parseRequest(testing.allocator, "*1\r\n$4\r\nPING\r\n");
        defer request.deinit();
        try testing.expect(request.command.ping.contents == null);
    }
    {
        var request = try parseRequest(testing.allocator, "*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n");
        defer request.deinit();
        try testing.expectEqualSlices(u8, "hello", request.command.ping.contents.?);
    }
}
test "parseRequest EchoCommand" {
    {
        var request = try parseRequest(testing.allocator, "*2\r\n$4\r\nEChO\r\n$3\r\nbye\r\n");
        defer request.deinit();
        try testing.expectEqualSlices(u8, "bye", request.command.echo.contents);
    }
}
test "parseRequest GetCommand" {
    {
        var request = try parseRequest(testing.allocator, "*2\r\n$3\r\nGet\r\n$3\r\nbye\r\n");
        defer request.deinit();
        try testing.expectEqualSlices(u8, "bye", request.command.get.key);
    }
}
test "parseRequest SetCommand" {
    {
        var request = try parseRequest(testing.allocator, "*3\r\n$3\r\nsEt\r\n$4\r\nfour\r\n$1\r\n4\r\n");
        defer request.deinit();
        try testing.expectEqualSlices(u8, "four", request.command.set.key);
        try testing.expectEqualSlices(u8, "4", request.command.set.value);
        try testing.expect(request.command.set.expiry == null);
    }
    {
        // We can't predict exactly what expiry timestamp the cache will record (because time will elapse from this moment until we actually put the entry
        // in the hashmap and), but it must at least be 1234 milliseconds after this moment.
        const time_before_request = std.time.milliTimestamp();
        var request = try parseRequest(testing.allocator, "*5\r\n$3\r\nsEt\r\n$4\r\nfour\r\n$4\r\nFOUR\r\n$2\r\nPx\r\n$4\r\n1234\r\n");
        defer request.deinit();
        try testing.expectEqualSlices(u8, "four", request.command.set.key);
        try testing.expectEqualSlices(u8, "FOUR", request.command.set.value);
        try testing.expect(request.command.set.expiry.? >= time_before_request + 1234);
    }
}
// TODO test errors for parseRequest
// TODO test parseRequest for InfoCommand

/// Given a client request, update any server state if needed.
pub fn handleRequest(request: Request, cache: *Cache) !void {
    // We only need to update state for SET commands. Everything else is ignored here.
    switch (request.command) {
        .set => |s| {
            try cache.putWithExpiry(s.key, s.value, s.expiry);
        },
        else => {},
    }
}
test "handleRequest no effect" {
    var cache = Cache.init(testing.allocator);
    try testing.expectEqual(0, cache.count());
    {
        const request = Request{ .allocator = undefined, .command = Command{ .echo = EchoCommand{ .contents = "can you hear me?" } } };
        try handleRequest(request, &cache);
        try testing.expectEqual(0, cache.count());
    }
    {
        const request = Request{ .allocator = undefined, .command = Command{ .get = GetCommand{ .key = "does not exist" } } };
        try handleRequest(request, &cache);
        try testing.expectEqual(0, cache.count());
    }
}
test "handleRequest SetCommand" {
    var cache = Cache.init(testing.allocator);
    defer cache.deinit();

    try testing.expectEqual(0, cache.count());
    {
        const request = Request{ .allocator = undefined, .command = Command{ .set = SetCommand{ .key = "key1", .value = "value1", .expiry = null } } };
        try handleRequest(request, &cache);
        try testing.expectEqual(1, cache.count());
        try testing.expectEqualSlices(u8, "value1", cache.get("key1").?);
    }
    {
        // NOTE: even though it's usually a bad idea to write unit tests that depend on timing, this should be fine since we're putting in
        // a big negative expiry. It's unlikely that the system clock will jump back by more than 100 days in the past in between calling cache.putWithExpiry() and cache.get().
        const request = Request{ .allocator = undefined, .command = Command{ .set = SetCommand{ .key = "key1", .value = "value1000", .expiry = -100 * std.time.ms_per_day } } };
        try handleRequest(request, &cache);
        // Replacing the same key with a new value should result in the same count.
        try testing.expectEqual(1, cache.count());
        // The value should be expired.
        try testing.expectEqual(null, cache.get("key1"));
    }
}

fn getResponseMessage(allocator: std.mem.Allocator, request: Request, cache: *Cache, config: *const ServerConfig) !Message {
    switch (request.command) {
        .ping => |p| {
            if (p.contents) |text| {
                return .{ .bulk_string = .{ .value = text } };
            }
            return .{ .simple_string = .{ .value = "PONG" } };
        },
        .echo => |e| {
            return .{ .bulk_string = .{ .value = e.contents } };
        },
        .get => |g| {
            const value = cache.get(g.key);
            if (value) |v| {
                return .{ .bulk_string = .{ .value = v } };
            }
            return .{ .bulk_string = .{ .value = "" } };
        },
        .set => {
            return .{ .simple_string = .{ .value = "OK" } };
        },
        .info => |i| {
            // A ServerConfig consists of multiple sections, e.g. ReplicationConfig is one section.
            // Each section consists of multiple fields.
            // The INFO command will list section keys, and we should print all sections that match those keys.
            // Printing a section means we go over every field in that section and print the field name and field value (name:value).

            var concatenated: []const u8 = try allocator.alloc(u8, 0);

            // For every section key we are given in the INFO command,
            for (i.section_keys) |section_key| {
                // TODO for now we just ignore unknown section keys.
                // Find the section in ServerConfig that matches the section_key (e.g. "replication").
                inline for (@typeInfo(ServerConfig).Struct.fields) |section| {
                    if (std.ascii.eqlIgnoreCase(section.name, section_key)) {
                        // Now, take that config section (e.g. ReplicationConfig) and concatenate all its fields as name:value strings.
                        const config_section = @field(config, section.name);
                        inline for (@typeInfo(@TypeOf(config_section)).Struct.fields) |section_field| {
                            const field_value = @field(config_section, section_field.name);
                            concatenated = try string_utils.appendNameValue(allocator, section_field.type, section_field.name, field_value, concatenated);
                        }
                    }
                }
            }
            // NOTE: we set the allocator for this Message because we want deinit() to free up the value string.
            return .{ .bulk_string = .{ .value = concatenated, .allocator = allocator } };
        },
    }
    return error.UnimplementedError;
}

/// Given a Request from a client, return a string message containing the response to send to the client. Caller owns returned string.
pub fn getResponse(allocator: std.mem.Allocator, request: Request, cache: *Cache, config: *const ServerConfig) ![]const u8 {
    var response_message = try getResponseMessage(allocator, request, cache, config);
    defer response_message.deinit();

    return response_message.toStr(allocator);
}

// TODO test getResponse

pub const RdbFile = struct {};

// TODO send full sync handshake to master
pub fn sendSyncHandshakeToMaster() !RdbFile {
    // TODO send a ping to master, expect a PONG back
    const ping = .{ .simple_string = .{ .value = "PING" } };
    _ = ping;

    // TODO send a REPLCONF to master twice, expecting OK back

    // TODO send a PSYNC to master, expecting FULLRESYNC back

    // TODO expect master to send us an RDB file
    return .{};
}
