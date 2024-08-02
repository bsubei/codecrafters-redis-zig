const std = @import("std");
const cli = @import("cli.zig");
const Args = cli.Args;

const ReplicationConfig = struct {
    role: []const u8,
};
pub const Config = struct {
    replication: ReplicationConfig,
};

pub fn createConfig(allocator: std.mem.Allocator, args: Args) !Config {
    _ = args;
    _ = allocator;
    const replication = .{ .role = "master" };
    return Config{ .replication = replication };
}
