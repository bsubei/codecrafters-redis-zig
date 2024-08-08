const std = @import("std");
const server = @import("server.zig");
const ServerState = @import("server_state.zig").ServerState;

pub fn main() !void {
    // This allocator is used for the ServerState, which is the data that stays around for the entire duration of the program.
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var state = try ServerState.initFromCliArgs(allocator, args);
    defer state.deinit();
    try server.runServer(&state);
}
