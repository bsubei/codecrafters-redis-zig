const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();
const testing = std.testing;
const Cache = @import("Cache.zig");
const parser = @import("parser.zig");
const ServerState = @import("server_state.zig").ServerState;
const network = @import("network.zig");
const rdb = @import("rdb.zig");
const xev = @import("xev");
const posix = std.posix;
const Connection = @import("connection.zig").Connection;

const Error = error{};

pub fn runServer(state: *ServerState) !void {
    switch (state.info_sections.replication.role) {
        .master => {
            try runMasterServer(state);
        },
        .slave => {
            try runSlaveServer(state);
        },
    }
}

// TODO Fix these tests by wrapping the connection fd in a reader and a writer and passing these into the function. That way we can test it.
// const MockConnection = struct {
//     address: std.net.Address,
//     stream: std.io.FixedBufferStream([]u8).Writer,
// };
// test "handleRequestAndRespond" {
//     var state = try ServerState.initFromCliArgs(testing.allocator, &[_][]const u8{ "--port", "123" });
//     defer state.deinit();

//     const unused_address = try std.net.Address.parseIp4("127.0.0.1", 1234);
//     {
//         var buffer: [64]u8 = undefined;
//         var fbs = std.io.fixedBufferStream(&buffer);
//         try handleRequestAndRespond(
//             testing.allocator,
//             "*2\r\n$4\r\nECHO\r\n$13\r\nHello, world!\r\n",
//             &state,
//             MockConnection{
//                 .address = unused_address,
//                 .stream = fbs.writer(),
//             },
//         );
//         try testing.expectEqualSlices(u8, "$13\r\nHello, world!\r\n", fbs.getWritten());
//     }
//     {
//         var buffer: [64]u8 = undefined;
//         var fbs = std.io.fixedBufferStream(&buffer);
//         try handleRequestAndRespond(
//             testing.allocator,
//             "*2\r\n$3\r\ngEt\r\n$13\r\nHello, world!\r\n",
//             &state,
//             MockConnection{
//                 .address = unused_address,
//                 .stream = fbs.writer(),
//             },
//         );
//         try testing.expectEqualSlices(u8, "$-1\r\n", fbs.getWritten());
//     }
//     {
//         var buffer: [64]u8 = undefined;
//         var fbs = std.io.fixedBufferStream(&buffer);
//         try handleRequestAndRespond(
//             testing.allocator,
//             "*3\r\n$3\r\nSEt\r\n$13\r\nHello, world!\r\n$4\r\nbye!\r\n",
//             &state,
//             MockConnection{
//                 .address = unused_address,
//                 .stream = fbs.writer(),
//             },
//         );
//         try testing.expectEqualSlices(u8, "+OK\r\n", fbs.getWritten());
//     }
//     {
//         var buffer: [64]u8 = undefined;
//         var fbs = std.io.fixedBufferStream(&buffer);
//         try handleRequestAndRespond(
//             testing.allocator,
//             "*2\r\n$3\r\ngEt\r\n$13\r\nHello, world!\r\n",
//             &state,
//             MockConnection{
//                 .address = unused_address,
//                 .stream = fbs.writer(),
//             },
//         );
//         try testing.expectEqualSlices(u8, "$4\r\nbye!\r\n", fbs.getWritten());
//     }
//     {
//         var buffer: [128]u8 = undefined;
//         var fbs = std.io.fixedBufferStream(&buffer);
//         try handleRequestAndRespond(
//             testing.allocator,
//             "*2\r\n$4\r\nINfo\r\n$11\r\nrePLicAtion\r\n",
//             &state,
//             MockConnection{
//                 .address = unused_address,
//                 .stream = fbs.writer(),
//             },
//         );
//         var iter = std.mem.splitScalar(u8, fbs.getWritten(), '\n');
//         {
//             const line = iter.next();
//             try testing.expect(line != null);
//             try testing.expectEqualStrings("$88\r", line.?);
//         }
//         {
//             const line = iter.next();
//             try testing.expect(line != null);
//             try testing.expectEqualStrings("role:master", line.?);
//         }
//         {
//             const line = iter.next();
//             try testing.expect(line != null);
//             try testing.expectStringStartsWith(line.?, "master_replid:");
//             try testing.expectEqual("master_replid:".len + 40, line.?.len);
//         }
//         {
//             const line = iter.next();
//             try testing.expect(line != null);
//             try testing.expectEqualStrings("master_repl_offset:0", line.?);
//         }
//         {
//             const line = iter.next();
//             try testing.expect(line != null);
//             try testing.expectEqualStrings("\r", line.?);
//         }
//         {
//             const line = iter.next();
//             try testing.expect(line != null);
//             try testing.expectEqualStrings("", line.?);
//         }
//         {
//             const line = iter.next();
//             try testing.expect(line == null);
//         }
//     }
// }

/// Accepts incoming connections and sets up a recv event for each.
fn acceptCallback(
    ud: ?*anyopaque,
    loop: *xev.Loop,
    _: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    std.debug.print("acceptCallback\n", .{});
    const connection_fd: posix.socket_t = result.accept catch unreachable;
    const state = @as(*ServerState, @ptrCast(@alignCast(ud.?)));
    // TODO need to keep track of these created connections in the ServerState so we deinit them when the connection closes.
    const connection = Connection.init(state, connection_fd) catch return .rearm;

    // Start reading from this connection forever.
    connection.recv(loop, recvCallback);

    return .rearm;
}

// TODO figure out why the redis-tester is bailing too quickly when trying to get back the response
fn recvCallback(
    ud: ?*anyopaque,
    loop: *xev.Loop,
    completion: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    std.debug.print("recvCallback\n", .{});
    const recv = completion.op.recv;
    const connection = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    const allocator = connection.server_state.allocator;

    const read_len = result.recv catch |err| {
        switch (err) {
            error.EOF => std.debug.print("socket closed from client side: {}\n", .{connection.socket_fd}),
            else => std.debug.print("ERROR reading from socket: {s}\n", .{@errorName(err)}),
        }
        connection.close(loop, closeCallback);
        return .disarm;
    };
    const raw_message = recv.buffer.slice[0..read_len];
    std.debug.print(
        "Read from socket {} ({} bytes): {s}\n",
        .{ recv.fd, read_len, raw_message },
    );

    // Parse the raw_message into a Request (a command from the client).
    var request = parser.parseRequest(allocator, raw_message) catch |err| {
        std.debug.print("ERROR parsing request: {s}\n", .{@errorName(err)});
        connection.close(loop, closeCallback);
        return .disarm;
    };
    defer request.deinit();

    // Handle the Request (update state) and generate a Response as a string.
    const response_str = parser.handleRequest(allocator, request, connection) catch |err| {
        std.debug.print("ERROR handling request: {s}\n", .{@errorName(err)});
        connection.close(loop, closeCallback);
        return .disarm;
    };
    std.debug.print("request -> response:\n{s},{s}\n", .{ raw_message, response_str });
    defer allocator.free(response_str);

    // TODO if we're a replica, don't set up a send if the sender is master.

    // TODO if we're master, set up a send event for replicas (if this is a write command)

    // Send the Response back to the client by setting up a send event.
    connection.send(loop, sendCallback, response_str) catch |err| {
        std.debug.print("ERROR creating a send event: {s}\n", .{@errorName(err)});
        connection.close(loop, closeCallback);
        return .disarm;
    };

    // Don't read anymore for now. Once we process the send event, we set up this recvCallback again.
    return .disarm;
}
fn sendCallback(
    ud: ?*anyopaque,
    loop: *xev.Loop,
    completion: *xev.Completion,
    result: xev.Result,
) xev.CallbackAction {
    std.debug.print("sendCallback\n", .{});
    const send = completion.op.send;
    const connection = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    const send_len = result.send catch |err| {
        std.debug.print("ERROR sending: {s}\n", .{@errorName(err)});
        connection.close(loop, closeCallback);
        return .disarm;
    };

    std.debug.print("send_len: {d}, buffer len: {d}\n", .{ send_len, send.buffer.slice.len });
    std.debug.print(
        "Send to socket {} ({} bytes): {s}\n",
        .{ send.fd, send_len, send.buffer.slice[0..send_len] },
    );

    // If we're master, send the RDB file to conclude the full synchronization if the connection is ready.
    const server_state = connection.server_state;
    const allocator = server_state.allocator;
    if (connection.server_state.replicaof == null) {
        if (server_state.replicaStatesByAddress.get(connection.socket_fd)) |replica_state| {
            switch (replica_state) {
                .receiving_sync => |r_state| {
                    const rdb_file = rdb.getEmptyRdb(allocator) catch |err| {
                        std.debug.print("ERROR getting RDB file to send to replica {d}: {s}\n", .{ connection.socket_fd, @errorName(err) });
                        return .disarm;
                    };
                    defer allocator.free(rdb_file);
                    const msg = std.fmt.allocPrint(allocator, "${d}\r\n{s}", .{ rdb_file.len, rdb_file }) catch |err| {
                        std.debug.print("ERROR formatting RDB output for socket {d}: {s}\n", .{ connection.socket_fd, @errorName(err) });
                        return .disarm;
                    };
                    defer allocator.free(msg);
                    server_state.replicaStatesByAddress.put(connection.socket_fd, .{ .connected_replica = .{ .capa = r_state.capa, .listening_port = r_state.listening_port } }) catch |err| {
                        std.debug.print("ERROR putting replica state for socket {d}: {s}\n", .{ connection.socket_fd, @errorName(err) });
                        return .disarm;
                    };
                    connection.send(loop, sendCallback, msg) catch |err| {
                        std.debug.print("ERROR creating send event for socket {d}: {s}\n", .{ connection.socket_fd, @errorName(err) });
                        return .disarm;
                    };
                    return .disarm;
                },
                else => {},
            }
        }
    }

    // We're done with sending. However, let's set up recv again in case there's more messages.
    connection.recv(loop, recvCallback);
    return .disarm;
}
fn closeCallback(
    ud: ?*anyopaque,
    _: *xev.Loop,
    _: *xev.Completion,
    _: xev.Result,
) xev.CallbackAction {
    std.debug.print("closeCallback\n", .{});
    const connection = @as(*Connection, @ptrCast(@alignCast(ud.?)));
    connection.deinit();
    return .disarm;
}

fn loadRdbFile(rdb_file: parser.RdbFile, state: *ServerState) !void {
    // TODO take the parsed RDB that master sent and use it to update our state.
    _ = rdb_file;
    _ = state;
}
fn syncWithMaster(allocator: std.mem.Allocator, state: *ServerState) !void {
    // Send full sync handshake to master
    const rdb_file = try parser.sendSyncHandshakeToMaster(allocator, state);

    // Take the parsed RDB that master sent and use it to update our state.
    try loadRdbFile(rdb_file, state);
}
/// Set up a listener at this address, and arm the accept event to handle incoming connections.
fn listenForConnections(address: net.Address, state: *ServerState) !void {
    var listener = try address.listen(.{
        .reuse_address = true,
        .kernel_backlog = 128,
    });
    defer listener.deinit();
    try stdout.print("Started listening at address: {}\n", .{address.in});

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    // Create accept event on the listener socket and trigger the acceptCallback.
    state.accept_completion = try state.allocator.create(xev.Completion);
    state.accept_completion.?.* = .{
        .op = .{ .accept = .{ .socket = listener.stream.handle } },
        .userdata = state,
        .callback = acceptCallback,
    };
    loop.add(state.accept_completion.?);
    try loop.run(.until_done);
}
pub fn runMasterServer(state: *ServerState) !void {
    const our_address = try net.Address.resolveIp("127.0.0.1", state.port);
    try listenForConnections(our_address, state);
}
pub fn runSlaveServer(state: *ServerState) !void {
    const our_address = try net.Address.resolveIp("127.0.0.1", state.port);

    // Set up an arena allocator backed by a page allocator. We only need it for syncWithMaster.
    {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const allocator = arena.allocator();
        try syncWithMaster(allocator, state);
    }

    try listenForConnections(our_address, state);
}
