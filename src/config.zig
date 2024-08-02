const std = @import("std");
const cli = @import("cli.zig");
const Args = cli.Args;

const ReplicationConfig = struct {
    role: []const u8,
    master_replid: ?[40]u8,
    master_repl_offset: u64,
};
pub const Config = struct {
    replication: ReplicationConfig,
};

fn generateId() ![40]u8 {
    // This doesn't need to be cryptographically secure, we just need fast pseudo-random numbers.
    // Grab 20 random bytes, and format them as hex digits (because each byte is 2 hex digits, that gives 40 hex digits).
    var bytes: [20]u8 = undefined;
    var rand = std.Random.DefaultPrng.init(@intCast(std.time.timestamp()));
    rand.fill(&bytes);
    var buf: [40]u8 = undefined;
    _ = try std.fmt.bufPrint(&buf, "{s}", .{std.fmt.fmtSliceHexLower(&bytes)});
    return buf;
}

pub fn createConfig(args: Args) !Config {
    var replication: ReplicationConfig = undefined;
    if (args.replicaof) |replicaof| {
        _ = replicaof;
        replication = .{ .role = "slave", .master_replid = null, .master_repl_offset = 0 };
    } else {
        replication = .{ .role = "master", .master_replid = try generateId(), .master_repl_offset = 0 };
    }
    return Config{ .replication = replication };
}

// TODO test createConfig
