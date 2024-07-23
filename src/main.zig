const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();

// The server produces and sends a Response back to the client.
const Response = []const u8;

// TODO implement proper parsing. For now, we're just assuming the structure of incoming messages look like this:
// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
fn get_response(request: []const u8) !Response {
    var it = std.mem.tokenizeSequence(u8, request, "\r\n");
    var i: usize = 0;
    var command_index: ?usize = undefined;
    while (it.next()) |word| : (i += 1) {
        if (command_index) |index| if (i == index + 2) {
            var buf: [1024]u8 = undefined;
            return try std.fmt.bufPrint(&buf, "+{s}\r\n", .{word});
        };
        if (std.ascii.eqlIgnoreCase(word, "echo")) {
            command_index = i;
        }
        if (std.ascii.eqlIgnoreCase(word, "ping")) {
            return "+PONG\r\n";
        }
    }
    // TODO reply with OK if we don't understand. This is necessary for now because "redis-cli" sometimes sends the COMMANDS command which we don't understand.
    return "+OK\r\n";
}

fn handleClient(client_connection: net.Server.Connection) !void {
    defer client_connection.stream.close();

    // TODO handle more than 1024 bytes at a time.
    var buf: [1024]u8 = undefined;
    while (true) {
        const num_read_bytes = client_connection.stream.read(&buf) catch break;
        try stdout.print("Read {d} bytes: {s}\n", .{ num_read_bytes, buf[0..num_read_bytes] });
        if (num_read_bytes == 0) {
            break;
        }
        const response = try get_response(&buf);
        try client_connection.stream.writeAll(response);
        try stdout.print("Done sending response: {s} to client {}\n", .{ response, client_connection.address.in });
    }
}

pub fn main() !void {
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
        const t = try std.Thread.spawn(.{}, handleClient, .{connection});
        t.detach();
    }
}
