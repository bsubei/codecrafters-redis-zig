//! This struct holds all the server state, including the config, information about replicas/master, and the cache date (key-value store with expiry).
//! Most of its members should only be accessed when `rwLock` is held, to allow for thread-safe reads and writes. Prefer using thread-safe getters and setters.

const std = @import("std");
const Cache = @import("Cache.zig");
const RwLock = std.Thread.RwLock;

allocator: std.mem.Allocator,
/// Make sure to hold this lock whenever accessing any other writeable fields. Prefer using thread-safe getters and setters.
rwLock: RwLock,
/// This field must only be accessed when `rwLock` is held.
/// This field should contain all the data needed when replying to INFO commands.
info_sections: InfoSections,
/// This field must only be accessed when `rwLock` is held.
cache: Cache,
// TODO figure out how to make these const.
/// This field is safe to access without holding `rwLock`, since it's not supposed to mutate.
port: u16,
/// This field is safe to access without holding `rwLock`, since it's not supposed to mutate.
replicaof: ?ReplicaOf,

const Self = @This();
const DEFAULT_PORT = 6379;
const Error = error{
    BadCLIArgument,
};

/// Even though the returned value contains slices of bytes, because the bytes are const, making copies
/// of the slices is enough to make this thread-safe.
pub fn getInfoSectionsThreadSafe(self: *Self) InfoSections {
    self.rwLock.lockShared();
    defer self.rwLock.unlockShared();
    return self.info_sections;
}
pub fn cacheGetThreadSafe(self: *Self, key: Cache.K) ?Cache.V {
    self.rwLock.lockShared();
    defer self.rwLock.unlockShared();
    return self.cache.get(key);
}
pub fn cachePutThreadSafe(self: *Self, key: Cache.K, value: Cache.V) !void {
    self.rwLock.lock();
    defer self.rwLock.unlock();
    return self.cache.put(key, value);
}
pub fn cachePutWithExpiryThreadSafe(self: *Self, key: Cache.K, value: Cache.V, expiry: Cache.ExpiryTimestampMs) !void {
    self.rwLock.lock();
    defer self.rwLock.unlock();
    return self.cache.putWithExpiry(key, value, expiry);
}

pub fn deinit(self: *Self) void {
    if (self.replicaof) |replicaof| {
        self.allocator.free(replicaof.master_host);
    }
    self.cache.deinit();
}

const ReplicaOf = struct {
    master_host: []const u8,
    master_port: u16,
};

const ServerRole = enum {
    master,
    slave,
};
const ReplicationInfoSection = struct {
    role: ServerRole,
    master_replid: ?[40]u8,
    master_repl_offset: u64,
};
const InfoSections = struct {
    replication: ReplicationInfoSection,
};

// TODO refactor this to make it testable and write tests.
pub fn initFromCliArgs(allocator: std.mem.Allocator, args: []const []const u8) !Self {
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

    const infos = try createInfoSections(replicaof);
    const cache = Cache.init(allocator);
    return Self{
        .allocator = allocator,
        .rwLock = RwLock{},
        .info_sections = infos,
        .port = if (port) |p| p else DEFAULT_PORT,
        .replicaof = replicaof,
        .cache = cache,
    };
}

/// This doesn't need to be cryptographically secure, we just need fast pseudo-random numbers.
/// Grab 20 random bytes, and format them as hex digits (because each byte is 2 hex digits, that gives 40 hex digits).
fn generateId() ![40]u8 {
    var bytes: [20]u8 = undefined;
    var rand = std.Random.DefaultPrng.init(@intCast(std.time.timestamp()));
    rand.fill(&bytes);
    var buf: [40]u8 = undefined;
    _ = try std.fmt.bufPrint(&buf, "{s}", .{std.fmt.fmtSliceHexLower(&bytes)});
    return buf;
}

fn createInfoSections(replicaof: ?ReplicaOf) !InfoSections {
    var replication: ReplicationInfoSection = undefined;
    if (replicaof != null) {
        replication = .{ .role = .slave, .master_replid = null, .master_repl_offset = 0 };
    } else {
        replication = .{ .role = .master, .master_replid = try generateId(), .master_repl_offset = 0 };
    }
    return .{ .replication = replication };
}

// TODO test createConfig
