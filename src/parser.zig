//! Bytes coming in from the client socket are first parsed as a Message, which is then further interpreted as a
//! Request (a command from the client). The server then handles the Request (state updates are applied) and a response Message is produced.
//! Finally, the response Message is sent back to the client as a sequence of bytes.

const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();
const Cache = @import("Cache.zig");
const ServerState = @import("server_state.zig").ServerState;
const testing = std.testing;
const string_utils = @import("string_utils.zig");
const network = @import("network.zig");
const Connection = @import("connection.zig").Connection;
const replica_state = @import("replica_state.zig");
const ReplconfCapability = replica_state.ReplconfCapability;

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
    FailedSyncHandshake,
    BadConfiguration,
    UnimplementedRequest,
    InvalidReplicaState,
    InvalidPsyncHandshakeArgs,
    InvalidRequestForReplicaToReceive,
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
    allocator: ?std.mem.Allocator = null,

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
                // NOTE: we don't need to recursively free the elements because they are guaranteed never to be Arrays themselves.
                if (arr.allocator) |alloc| alloc.free(arr.elements);
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
const InfoCommand = struct { arguments: [][]const u8 };
const ReplconfCommand = struct { arguments: [][]const u8 };
const PsyncCommand = struct { replicationid: []const u8, offset: i64 };
const UnknownCommand = struct {};
const CommandType = enum {
    ping,
    echo,
    set,
    get,
    info,
    replconf,
    psync,
    unknown,
};
// TODO I'm starting to think I need to either rename this or something to account for the fact that these can be sent by a server to respond to requests.
const Command = union(CommandType) {
    ping: PingCommand,
    echo: EchoCommand,
    set: SetCommand,
    get: GetCommand,
    info: InfoCommand,
    replconf: ReplconfCommand,
    psync: PsyncCommand,
    unknown: UnknownCommand,
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
                for (i.arguments) |arg| {
                    self.allocator.free(arg);
                }
                self.allocator.free(i.arguments);
            },
            .replconf => |r| {
                for (r.arguments) |arg| {
                    self.allocator.free(arg);
                }
                self.allocator.free(r.arguments);
            },
            .psync => |p| {
                self.allocator.free(p.replicationid);
            },
            .unknown => {},
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
                    1 => return .{
                        .command = .{ .ping = .{ .contents = null } },
                        .allocator = undefined,
                    },
                    2 => return .{
                        .command = .{ .ping = .{ .contents = @as(?[]const u8, try allocator.dupe(u8, try messages[1].get_contents())) } },
                        .allocator = allocator,
                    },
                    else => {
                        return Error.InvalidRequestNumberOfArgs;
                    },
                }
            }
            if (std.ascii.eqlIgnoreCase(first_word, "echo")) {
                switch (messages.len) {
                    2 => return .{
                        .command = .{ .echo = .{ .contents = try allocator.dupe(u8, try messages[1].get_contents()) } },
                        .allocator = allocator,
                    },
                    else => {
                        return Error.InvalidRequestNumberOfArgs;
                    },
                }
            }
            if (std.ascii.eqlIgnoreCase(first_word, "get")) {
                switch (messages.len) {
                    2 => return .{
                        .command = .{ .get = .{ .key = try allocator.dupe(u8, try messages[1].get_contents()) } },
                        .allocator = allocator,
                    },
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

                        return .{ .command = .{ .set = .{ .key = key, .value = value, .expiry = expiry } }, .allocator = allocator };
                    },
                    else => {
                        return Error.InvalidRequestNumberOfArgs;
                    },
                }
            }
            if (std.ascii.eqlIgnoreCase(first_word, "info")) {
                // TODO we should dedupe any given section keys.
                // Just copy over all the remaining message contents to the arguments, and pass on ownership to the caller.
                var list_str = try messagesToArrayList(allocator, messages[1..]);
                errdefer list_str.deinit();
                return .{ .command = .{ .info = .{ .arguments = try list_str.toOwnedSlice() } }, .allocator = allocator };
            }
            if (std.ascii.eqlIgnoreCase(first_word, "replconf")) {
                switch (messages.len) {
                    1 => return Error.InvalidRequestNumberOfArgs,
                    else => {
                        // Just copy over all the remaining message contents to the arguments, and pass on ownership to the caller.
                        var list_str = try messagesToArrayList(allocator, messages[1..]);
                        errdefer list_str.deinit();
                        return .{ .command = .{ .replconf = .{ .arguments = try list_str.toOwnedSlice() } }, .allocator = allocator };
                    },
                }
            }
            if (std.ascii.eqlIgnoreCase(first_word, "psync")) {
                switch (messages.len) {
                    3 => {
                        const replicationid = try messages[1].get_contents();
                        const replicationidCopy = try allocator.alloc(u8, replicationid.len);
                        errdefer allocator.free(replicationidCopy);
                        std.mem.copyForwards(u8, replicationidCopy, replicationid);
                        const offset = try std.fmt.parseInt(i64, try messages[2].get_contents(), 10);
                        return .{ .command = .{ .psync = .{ .replicationid = replicationidCopy, .offset = offset } }, .allocator = allocator };
                    },
                    else => return Error.InvalidRequestNumberOfArgs,
                }
            }
        },
        else => {
            // TODO think about implementing simple_string or bulk_string as a request.
            return Error.UnimplementedNonArrayRequest;
        },
    }
    return .{ .command = .{ .unknown = .{} }, .allocator = allocator };
}
/// Allocates and fills an ArrayList containing a slice/view of the given Messages' contents.
/// The caller is responsible for calling deinit() on the ArrayList.
fn messagesToArrayList(allocator: std.mem.Allocator, messages: []const Message) !std.ArrayList([]const u8) {
    // This seems like a bunch of unnecessary allocating, but it's ok since we're using an arena allocator backed by a page allocator.
    var temp_list = std.ArrayList([]const u8).init(allocator);
    errdefer temp_list.deinit();
    for (messages) |msg| {
        try temp_list.append(try allocator.dupe(u8, try msg.get_contents()));
    }
    return temp_list;
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
// TODO test parseRequest including errors

fn handleRequestAndGenerateResponseMessage(allocator: std.mem.Allocator, request: Request, connection: *Connection) !Message {
    const state = connection.server_state;

    switch (request.command) {
        .ping => |p| {
            if (p.contents) |text| {
                // Bulk string PINGs are not part of a handshake.
                return .{ .bulk_string = .{ .value = text } };
            }
            // Record the requester address as a possible replica. They're in the initial Ping state.
            if (state.info_sections.replication.role == .master) {
                connection.replica_state = .{ .initial_ping = .{} };
            }
            return .{ .simple_string = .{ .value = "PONG" } };
        },
        .echo => |e| {
            return .{ .bulk_string = .{ .value = e.contents } };
        },
        .get => |g| {
            const value = state.cache.get(g.key);
            if (value) |v| {
                return .{ .bulk_string = .{ .value = v } };
            }
            return .{ .bulk_string = .{ .value = "" } };
        },
        .set => |s| {
            try state.cache.putWithExpiry(s.key, s.value, s.expiry);
            return .{ .simple_string = .{ .value = "OK" } };
        },
        .info => |i| {
            // A InfoSections consists of multiple sections, e.g. ReplicationConfig is one section.
            // Each section consists of multiple fields.
            // The INFO command will list section keys, and we should print all sections that match those keys.
            // Printing a section means we go over every field in that section and print the field name and field value (name:value).

            var concatenated: []const u8 = try allocator.alloc(u8, 0);

            // For every section key we are given in the INFO command,
            for (i.arguments) |section_key| {
                // TODO for now we just ignore unknown section keys.
                // Find the section in InfoSections that matches the section_key (e.g. "replication").
                inline for (@typeInfo(@TypeOf(state.info_sections)).Struct.fields) |section| {
                    if (std.ascii.eqlIgnoreCase(section.name, section_key)) {
                        // Now, take that config section (e.g. ReplicationConfig) and concatenate all its fields as name:value strings.
                        const config_section = @field(state.info_sections, section.name);
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
        .replconf => |r| {
            if (state.replicaof != null) {
                return Error.InvalidRequestForReplicaToReceive;
            }
            switch (r.arguments.len) {
                2 => {
                    // Check that we're in the correct state for this replica and that we should advance to the next. Then return OK.
                    // TODO this code is a bit too gnarly. Hide all this away in a getNextReplicaState function.
                    if (connection.replica_state) |r_state| {
                        switch (r_state) {
                            // Advance from initial_ping to first_replconf, and record the provided port.
                            .initial_ping => {
                                if (std.ascii.eqlIgnoreCase(r.arguments[0], "listening-port")) {
                                    connection.replica_state = .{ .first_replconf = .{ .listening_port = try std.fmt.parseInt(u16, r.arguments[1], 10) } };
                                } else {
                                    return Error.InvalidPsyncHandshakeArgs;
                                }
                            },
                            // Advance from first_replconf to second_replconf if the args are correct. Don't forget to keep the port from the old state.
                            .first_replconf => |first| {
                                const capability = std.meta.stringToEnum(ReplconfCapability, r.arguments[1]);
                                if (std.ascii.eqlIgnoreCase(r.arguments[0], "capa") and capability == ReplconfCapability.psync2) {
                                    connection.replica_state = .{ .second_replconf = .{ .capa = capability.?, .listening_port = first.listening_port } };
                                } else {
                                    return Error.InvalidPsyncHandshakeArgs;
                                }
                            },
                            else => return Error.InvalidReplicaState,
                        }
                    } else {
                        return Error.InvalidReplicaState;
                    }
                    // If we didn't error out from any previous check, just respond with OK.
                    return .{ .simple_string = .{ .value = "OK" } };
                },
                else => {},
            }
        },
        .psync => |p| {
            if (state.replicaof != null) {
                return Error.InvalidRequestForReplicaToReceive;
            }
            if (connection.replica_state) |r_state| {
                switch (r_state) {
                    .second_replconf => |second| {
                        // The replica is announcing that it's ready for a full resync. Update its state to .receiving_sync and reply with FULLRESYNC and our replid.
                        if (std.ascii.eqlIgnoreCase(p.replicationid, "?") and p.offset == -1) {
                            const reply = try std.fmt.allocPrint(allocator, "+FULLRESYNC {s} 0", .{state.info_sections.replication.master_replid.?});
                            connection.replica_state = .{ .receiving_sync = .{ .listening_port = second.listening_port, .capa = second.capa } };
                            return .{ .simple_string = .{ .value = reply, .allocator = allocator } };
                        } else {
                            return Error.InvalidPsyncHandshakeArgs;
                        }
                    },
                    else => return Error.InvalidReplicaState,
                }
            } else {
                return Error.InvalidReplicaState;
            }
        },
        .unknown => {
            // Return OK for now.
            return .{ .simple_string = .{ .value = "OK" } };
        },
    }
    return Error.UnimplementedRequest;
}

/// Given a Request from a client, return a string message containing the response to send to the client. Caller owns returned string.
pub fn handleRequest(allocator: std.mem.Allocator, request: Request, connection: *Connection) ![]const u8 {
    var response_message = try handleRequestAndGenerateResponseMessage(allocator, request, connection);
    defer response_message.deinit();
    return response_message.toStr(allocator);
}

// TODO test handleRequest

pub const RdbFile = struct {};

// TODO send full sync handshake to master
pub fn sendSyncHandshakeToMaster(allocator: std.mem.Allocator, state: *ServerState) !RdbFile {
    // Connect to master and ask it to sync. This blocks (does not listen to incoming connections) until finished.
    if (state.info_sections.replication.role == .master) return Error.BadConfiguration;
    var master_stream = (try std.net.tcpConnectToHost(allocator, state.replicaof.?.master_host, state.replicaof.?.master_port));
    defer master_stream.close();
    const master_socket = master_stream.handle;

    // Send a ping to master, expect a PONG back
    {
        const ping = Message{ .array = .{ .allocator = allocator, .elements = &[_]Message{.{ .bulk_string = .{ .value = "PING" } }} } };
        const write_buffer = try ping.toStr(allocator);
        defer allocator.free(write_buffer);
        try network.writeToSocket(master_socket, write_buffer);
        var response_buffer = std.ArrayList(u8).init(allocator);
        defer response_buffer.deinit();
        const num_read_bytes = try network.readFromSocket(master_socket, &response_buffer);
        if (num_read_bytes == 0) return Error.FailedSyncHandshake;
        var response = try Message.fromStr(allocator, response_buffer.items);
        defer response.deinit();
        switch (response) {
            .simple_string => |s| if (!std.ascii.eqlIgnoreCase(s.value, "pong")) return Error.FailedSyncHandshake,
            else => return Error.FailedSyncHandshake,
        }
    }

    // Send a REPLCONF to master twice, expecting OK back
    {
        // NOTE: it's fine to put this on the stack since its lifetime is as long as this function.
        var port_buf: [8]u8 = undefined;
        const len = std.fmt.formatIntBuf(&port_buf, state.port, 10, .lower, .{});
        const port = port_buf[0..len];

        {
            // Send first REPLCONF: REPLCONF listening-port <port>
            const msgs = [_]Message{ .{ .bulk_string = .{ .value = "REPLCONF" } }, .{ .bulk_string = .{ .value = "listening-port" } }, .{ .bulk_string = .{ .value = port } } };
            const first_replconf = Message{ .array = .{ .allocator = allocator, .elements = &msgs } };
            const write_buffer = try first_replconf.toStr(allocator);
            defer allocator.free(write_buffer);
            try network.writeToSocket(master_socket, write_buffer);
            // Expect OK.
            var response_buffer = std.ArrayList(u8).init(allocator);
            defer response_buffer.deinit();
            const num_read_bytes = try network.readFromSocket(master_socket, &response_buffer);
            if (num_read_bytes == 0) return Error.FailedSyncHandshake;
            var response = try Message.fromStr(allocator, response_buffer.items);
            defer response.deinit();
            switch (response) {
                .simple_string => |s| if (!std.ascii.eqlIgnoreCase(s.value, "OK")) return Error.FailedSyncHandshake,
                else => return Error.FailedSyncHandshake,
            }
        }
        {
            // Send second REPLCONF: REPLCONF capa psync2
            const msgs = [_]Message{ .{ .bulk_string = .{ .value = "REPLCONF" } }, .{ .bulk_string = .{ .value = "capa" } }, .{ .bulk_string = .{ .value = "psync2" } } };
            const second_replconf = Message{ .array = .{ .elements = &msgs } };
            const write_buffer = try second_replconf.toStr(allocator);
            defer allocator.free(write_buffer);
            try network.writeToSocket(master_socket, write_buffer);
            // Expect OK.
            var response_buffer = std.ArrayList(u8).init(allocator);
            defer response_buffer.deinit();
            const num_read_bytes = try network.readFromSocket(master_socket, &response_buffer);
            if (num_read_bytes == 0) return Error.FailedSyncHandshake;
            var response = try Message.fromStr(allocator, response_buffer.items);
            defer response.deinit();
            switch (response) {
                .simple_string => |s| if (!std.ascii.eqlIgnoreCase(s.value, "OK")) return Error.FailedSyncHandshake,
                else => return Error.FailedSyncHandshake,
            }
        }
        {
            // Send PSYNC: PSYNC ? -1, expecting FULLRESYNC back
            const msgs = [_]Message{ .{ .bulk_string = .{ .value = "PSYNC" } }, .{ .bulk_string = .{ .value = "?" } }, .{ .bulk_string = .{ .value = "-1" } } };
            const psync = Message{ .array = .{ .elements = &msgs } };
            const write_buffer = try psync.toStr(allocator);
            defer allocator.free(write_buffer);
            try network.writeToSocket(master_socket, write_buffer);
            // Expect OK.
            var response_buffer = std.ArrayList(u8).init(allocator);
            defer response_buffer.deinit();
            const num_read_bytes = try network.readFromSocket(master_socket, &response_buffer);
            if (num_read_bytes == 0) return Error.FailedSyncHandshake;
            // TODO ignore the response for now, handle later when we can actually read/write RDB files. Right now, assume empty RDB.
        }
    }

    // TODO expect master to send us an RDB file
    return .{};
}
