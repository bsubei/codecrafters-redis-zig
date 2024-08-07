//! This struct holds all the server state, including the config, information about replicas/master, and the cache date (key-value store with expiry).
//! Most of its members should only be accessed when `rwLock` is held, to allow for thread-safe reads and writes. Prefer using thread-safe getters and setters.

const std = @import("std");
const net = std.net;
const Cache = @import("Cache.zig");
const RwLock = std.Thread.RwLock;
const posix = std.posix;

const Self = @This();
const PortType = u16;
const ReplicaMap = std.HashMap(net.Address, ReplicaState, AddressContext, std.hash_map.default_max_load_percentage);
const DEFAULT_PORT = 6379;

allocator: std.mem.Allocator,
/// Make sure to hold this lock whenever accessing any other writeable fields. Prefer using thread-safe getters and setters.
rwLock: RwLock,
/// This field must only be accessed when `rwLock` is held.
/// This field should contain all the data needed when replying to INFO commands.
info_sections: InfoSections,
/// This field must only be accessed when `rwLock` is held.
cache: Cache,
/// This field must only be accessed when `rwLock` is held.
/// This hashmap contains all of our replicas keyed by their address.
replicaStatesByAddress: ReplicaMap,
// TODO figure out how to make these const.
/// This field is safe to access without holding `rwLock`, since it's not supposed to mutate.
port: PortType,
/// This field is safe to access without holding `rwLock`, since it's not supposed to mutate.
replicaof: ?ReplicaOf,

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
pub fn replicaStatesGetThreadSafe(self: *Self, key: net.Address) ?ReplicaState {
    self.rwLock.lockShared();
    defer self.rwLock.unlockShared();
    return self.replicaStatesByAddress.get(key);
}
pub fn replicaStatesPutThreadSafe(self: *Self, key: net.Address, value: ReplicaState) !void {
    self.rwLock.lock();
    defer self.rwLock.unlock();
    return self.replicaStatesByAddress.put(key, value);
}
/// Returns the ReplicaStates that match the given type.
pub fn getReplicaStatesByTypeThreadSafe(self: *Self, allocator: std.mem.Allocator, replica_type: ReplicaStateType) ![]const ReplicaState {
    self.rwLock.lockShared();
    defer self.rwLock.unlockShared();
    var buf = std.ArrayList(ReplicaState).init(allocator);
    errdefer buf.deinit();

    var it = self.replicaStatesByAddress.iterator();
    while (it.next()) |entry| {
        if (@as(ReplicaStateType, entry.value_ptr.*) == replica_type) {
            buf.append(entry.value_ptr.*);
        }
    }

    return buf.toOwnedSlice();
}

pub fn deinit(self: *Self) void {
    if (self.replicaof) |replicaof| {
        self.allocator.free(replicaof.master_host);
    }
    self.cache.deinit();
    self.replicaStatesByAddress.deinit();
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
    master_repl_offset: i64,
};
const InfoSections = struct {
    replication: ReplicationInfoSection,
};

pub const ReplconfCapability = enum {
    psync2,
};

const InitialPing = struct {};
const FirstReplconf = struct { listening_port: u16 };
const SecondReplconf = struct { listening_port: u16, capa: ReplconfCapability };
const ReceivingSync = struct { listening_port: u16, capa: ReplconfCapability };
const ConnectedReplica = struct { listening_port: u16, capa: ReplconfCapability };
pub const ReplicaStateType = enum {
    initial_ping,
    first_replconf,
    second_replconf,
    receiving_sync,
    connected_replica,
};
pub fn isReplicaReadyToReceive(replica_state: ReplicaState) bool {
    switch (replica_state) {
        .receiving_sync, .connected_replica => true,
        else => false,
    }
}
/// A replica performs a handshake with the master server by going through these states in this order (no skipping!):
/// InitialPing <-- after a replica sends a PING to the master.
/// FirstReplconf <-- after a replica sends the first "REPLCONF listening-port <port>" command to the master.
/// SecondReplconf <-- after a replica sends "REPLCONF capa psync2" or a similar command to the master.
/// ReceivingSync <-- after a replica sends "PSYNC ? -1" to the master, the master replies with "+FULLRESYNC <replid> 0". The
///     master should start sending the RDB file now.
/// ConnectedReplica <-- Once the RDB file is sent over, the replica is now fully synchronized and connected. The master will relay
///     write commands to it.
pub const ReplicaState = union(ReplicaStateType) {
    initial_ping: InitialPing,
    first_replconf: FirstReplconf,
    second_replconf: SecondReplconf,
    receiving_sync: ReceivingSync,
    connected_replica: ConnectedReplica,
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
        .replicaStatesByAddress = ReplicaMap.init(allocator),
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

pub const AddressContext = struct {
    pub fn hash(_: @This(), address: net.Address) u64 {
        switch (address.any.family) {
            posix.AF.INET => {
                const ip4 = address.in;
                return @as(u64, ip4.sa.port) << 32 | ip4.sa.addr;
            },
            posix.AF.INET6 => {
                const ip6 = address.in6;
                var hasher = std.hash.Wyhash.init(0);
                std.hash.autoHash(&hasher, ip6.sa.addr);
                std.hash.autoHash(&hasher, ip6.sa.port);
                std.hash.autoHash(&hasher, ip6.sa.flowinfo);
                std.hash.autoHash(&hasher, ip6.sa.scope_id);
                return hasher.final();
            },
            posix.AF.UNIX => {
                if (!@hasField(std.net.Address, "un")) {
                    @compileError("Unix sockets are not supported on this platform");
                }
                var hasher = std.hash.Wyhash.init(0);
                std.hash.autoHash(&hasher, address.un.path);
                return hasher.final();
            },
            else => @panic("Unsupported address family"),
        }
    }
    pub fn eql(_: @This(), a: std.net.Address, b: std.net.Address) bool {
        if (a.any.family != b.any.family) return false;

        switch (a.any.family) {
            posix.AF.INET => {
                return a.in.sa.port == b.in.sa.port and a.in.sa.addr == b.in.sa.addr;
            },
            posix.AF.INET6 => {
                return a.in6.sa.port == b.in6.sa.port and
                    std.mem.eql(u8, &a.in6.sa.addr, &b.in6.sa.addr) and
                    a.in6.sa.flowinfo == b.in6.sa.flowinfo and
                    a.in6.sa.scope_id == b.in6.sa.scope_id;
            },
            posix.AF.UNIX => {
                if (!@hasField(std.net.Address, "un")) {
                    @compileError("Unix sockets are not supported on this platform");
                }
                return std.mem.eql(u8, &a.un.path, &b.un.path);
            },
            else => @panic("Unsupported address family"),
        }
    }
};
