const std = @import("std");
const net = std.net;
const posix = std.posix;
const stdout = std.io.getStdOut().writer();

const CLIENT_READER_CHUNK_SIZE = 1 << 10;

/// Because the client will send data and wait for our reply before closing the socket connection, we can't just "read all bytes" from the stream
/// then parse them at our leisure, since we would block forever waiting for end of stream which will never come.
/// The clean alternative would be to read until seeing a delimiter ('\n' for example) or eof, but misbehaving clients could just not send either and block us forever.
/// Since I don't know how to make those calls use timeouts, I'll just call read() one chunk at a time (nonblocking) and concatenate them into the final message.
pub fn readFromSocket(socket: posix.socket_t, buffer: *std.ArrayList(u8)) !u64 {
    // Read one chunk and append it to the raw_message.
    var num_read_bytes = try readChunk(socket, buffer);

    // Connection closed, leave if there's no pending raw_message to send.
    if (num_read_bytes == 0 and buffer.items.len == 0) {
        return num_read_bytes;
    }
    // There's possibly more to read for this raw_message! Go back and read another chunk.
    if (num_read_bytes == CLIENT_READER_CHUNK_SIZE) {
        num_read_bytes += try readFromSocket(socket, buffer);
    }

    return num_read_bytes;
}

pub fn readChunk(socket: posix.socket_t, message_ptr: *std.ArrayList(u8)) !usize {
    // Read one chunk.
    var buf: [CLIENT_READER_CHUNK_SIZE]u8 = undefined;
    const num_read_bytes = posix.read(socket, &buf) catch |err| {
        // Handle retry, otherwise bubble up any errors.
        if (err == error.WouldBlock) return readChunk(socket, message_ptr);
        return err;
    };
    try stdout.print("Read {d} bytes: {s}\n", .{ num_read_bytes, buf[0..num_read_bytes] });

    // Save this chunk to the message.
    try message_ptr.appendSlice(buf[0..num_read_bytes]);

    return num_read_bytes;
}

pub fn writeToSocket(socket: posix.socket_t, data: []const u8) !usize {
    var total_written: usize = 0;
    while (total_written < data.len) {
        const written = try posix.write(socket, data[total_written..]);
        if (written == 0) return error.WriteZero;
        total_written += written;
    }
    return total_written;
}
