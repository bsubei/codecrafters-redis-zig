const std = @import("std");

const DEFAULT_PORT = 6379;
const Error = error{
    BadCLIArgument,
};
const ReplicaOf = struct {
    master_host: []const u8,
    master_port: u16,
};
pub const Args = struct {
    port: u16,
    replicaof: ?ReplicaOf,
    allocator: std.mem.Allocator,

    const Self = @This();
    pub fn deinit(self: *Self) void {
        if (self.replicaof) |replicaof| {
            self.allocator.free(replicaof.master_host);
        }
    }
};
// TODO refactor this to make it testable and write tests.
pub fn parseArgs(allocator: std.mem.Allocator) !Args {
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var port: ?u16 = null;
    var replicaof: ?ReplicaOf = null;

    for (args, 0..) |arg, idx| {
        if (std.mem.eql(u8, arg, "--port")) {
            if (idx + 1 >= args.len) {
                return Error.BadCLIArgument;
            }
            port = try std.fmt.parseInt(u16, args[idx + 1], 10);
        }
        if (std.mem.eql(u8, arg, "--replicaof")) {
            if (idx + 1 >= args.len) {
                return Error.BadCLIArgument;
            }
            var iter = std.mem.splitScalar(u8, args[idx + 1], ' ');
            const first = iter.next();
            const second = iter.next();
            if (first != null and second != null) {
                const master_host = try allocator.dupe(u8, first.?);
                errdefer allocator.free(master_host);
                const master_port = try std.fmt.parseInt(u16, second.?, 10);
                replicaof = ReplicaOf{ .master_host = master_host, .master_port = master_port };
            } else {
                return Error.BadCLIArgument;
            }
        }
    }

    return Args{ .port = if (port) |p| p else DEFAULT_PORT, .replicaof = replicaof, .allocator = allocator };
}
