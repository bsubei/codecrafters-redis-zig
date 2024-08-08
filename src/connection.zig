const std = @import("std");
const posix = std.posix;
const socket_t = posix.socket_t;
const ServerState = @import("server_state.zig").ServerState;
const xev = @import("xev");

pub const Connection = struct {
    /// The server state holds the allocator that will be used to allocate everything inside this connection, including the connection itself.
    server_state: *ServerState,

    read_buffer: [512]u8,
    write_buffer: [512]u8,

    /// The underlying socket that this Connection object is meant to wrap.
    socket_fd: socket_t,
    completion: xev.Completion,

    const Self = @This();
    pub fn init(server_state: *ServerState, socket_fd: socket_t) !*Self {
        const connection = try server_state.allocator.create(Self);
        connection.* = .{
            .server_state = server_state,
            // TODO make sure these undefined fields are not misused.
            .read_buffer = undefined,
            .write_buffer = undefined,
            .socket_fd = socket_fd,
            .completion = undefined,
        };
        return connection;
    }

    /// Deallocates everything that this connection owns, then deallocates the connection itself.
    pub fn deinit(self: *Self) void {
        const allocator = self.server_state.allocator;
        allocator.destroy(self);
    }

    /// Sets up a recv event using this connection and returns. Does not block.
    pub fn recv(self: *Self, loop: *xev.Loop, callback: anytype) void {
        self.completion = .{
            .op = .{
                .recv = .{
                    .fd = self.socket_fd,
                    .buffer = .{ .slice = &self.read_buffer },
                },
            },
            .userdata = self,
            .callback = callback,
        };
        loop.add(&self.completion);
    }
    /// Sets up a send event using this connection and returns. Does not block.
    pub fn send(self: *Self, loop: *xev.Loop, callback: anytype) void {
        self.completion = .{
            .op = .{
                .send = .{
                    .fd = self.socket_fd,
                    .buffer = .{ .slice = &self.write_buffer },
                },
            },
            .userdata = self,
            .callback = callback,
        };
        loop.add(&self.completion);
    }
    /// Sets up a close event using this connection and returns. Does not block.
    pub fn close(self: *Self, loop: *xev.Loop, callback: anytype) void {
        self.comp = .{
            .op = .{ .close = .{ .fd = self.socket_fd } },
            .userdata = self,
            .callback = callback,
        };
        loop.add(&self.completion);
    }
};
