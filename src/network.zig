const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();

const CLIENT_READER_CHUNK_SIZE = 1 << 10;

pub fn readFromClient(client_stream: net.Stream, buffer: *std.ArrayList(u8)) !u64 {
    // Read one chunk and append it to the raw_message.
    var num_read_bytes = try readChunk(client_stream, buffer);

    // Connection closed, leave if there's no pending raw_message to send.
    if (num_read_bytes == 0 and buffer.items.len == 0) {
        return num_read_bytes;
    }
    // There's possibly more to read for this raw_message! Go back and read another chunk.
    if (num_read_bytes == CLIENT_READER_CHUNK_SIZE) {
        num_read_bytes += try readFromClient(client_stream, buffer);
    }

    return num_read_bytes;
}

pub fn readChunk(client_stream: net.Stream, message_ptr: *std.ArrayList(u8)) !usize {
    // Read one chunk.
    var buf: [CLIENT_READER_CHUNK_SIZE]u8 = undefined;
    const num_read_bytes = client_stream.read(&buf) catch |err| {
        // Handle retry, otherwise bubble up any errors.
        if (err == error.WouldBlock) return readChunk(client_stream, message_ptr);
        return err;
    };
    try stdout.print("Read {d} bytes: {s}\n", .{ num_read_bytes, buf[0..num_read_bytes] });

    // Save this chunk to the message.
    try message_ptr.appendSlice(buf[0..num_read_bytes]);

    return num_read_bytes;
}
