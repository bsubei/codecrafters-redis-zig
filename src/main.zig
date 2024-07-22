const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();

fn handleClient(client_connection: net.Server.Connection) !void {
    defer client_connection.stream.close();

    var buf: [1024]u8 = undefined;
    while (true) {
        const num_read_bytes = client_connection.stream.read(&buf) catch break;
        try stdout.print("Read {d} bytes: {s}\n", .{ num_read_bytes, buf[0..num_read_bytes] });
        if (num_read_bytes == 0) {
            break;
        }
        const response = "+PONG\r\n";
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
