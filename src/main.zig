const std = @import("std");
const net = std.net;

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    // You can use print statements as follows for debugging, they'll be visible when running tests.
    try stdout.print("Logs from your program will appear here!\n", .{});

    const address = try net.Address.resolveIp("127.0.0.1", 6379);

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    const connection = try listener.accept();
    try stdout.print("accepted new connection from client {}\n", .{connection.address.in});
    defer connection.stream.close();

    var buf: [1024]u8 = undefined;
    while ((connection.stream.read(&buf) catch 0) > 0) {
        try connection.stream.writeAll("+PONG\r\n");
        try stdout.print("Done sending to client {}\n", .{connection.address.in});
    }
}
