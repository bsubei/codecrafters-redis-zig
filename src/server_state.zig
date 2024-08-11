//! TODO UPDATE THESE DOCS
//! This struct holds all the server state, including the config, information about replicas/master, and the cache date (key-value store with expiry).

const std = @import("std");
const net = std.net;
const Cache = @import("Cache.zig");
const RwLock = std.Thread.RwLock;
const posix = std.posix;
const socket_t = posix.socket_t;
const xev = @import("xev");
const Connection = @import("connection.zig").Connection;
const replica_state = @import("replica_state.zig");
const ReplicaState = replica_state.ReplicaState;
const ReplicaStateType = replica_state.ReplicaStateType;

pub const ServerState = struct {
    const Self = @This();
    const PortType = u16;
    const ConnectionsMap = std.AutoHashMap(socket_t, *Connection);
    const DEFAULT_PORT = 6379;

    allocator: std.mem.Allocator,

    /// This field should contain all the data needed when replying to INFO commands.
    info_sections: InfoSections,
    /// This hashmap contains the data store for this Redis server.
    cache: Cache,
    /// This hashmap contains pointers to all of our connections keyed by their socket.
    connectionsMap: ConnectionsMap,

    accept_completion: ?*xev.Completion = null,

    port: PortType,
    /// TODO need to store the connection to master so we treat them differently (e.g. don't reply to their write commands)
    /// Will be set if this server is a replica. Will be null if the server is a master.
    replicaof: ?ReplicaOf,

    const Error = error{
        BadCLIArgument,
    };

    pub fn deinit(self: *Self) void {
        if (self.replicaof) |replicaof| {
            self.allocator.free(replicaof.master_host);
        }
        self.cache.deinit();
        self.connectionsMap.deinit();
        if (self.accept_completion) |comp| {
            self.allocator.destroy(comp);
        }
    }

    pub fn removeConnection(self: *Self, socket_fd: socket_t) void {
        if (self.connectionsMap.fetchRemove(socket_fd)) |entry| {
            entry.value.deinit();
        }
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
            .info_sections = infos,
            .port = if (port) |p| p else DEFAULT_PORT,
            .replicaof = replicaof,
            .cache = cache,
            .connectionsMap = ConnectionsMap.init(allocator),
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

    pub fn getReplicaConnectionsByType(self: *Self, allocator: std.mem.Allocator, replica_type: ReplicaStateType) ![]const *Connection {
        var buf = std.ArrayList(*Connection).init(allocator);
        errdefer buf.deinit();

        var it = self.connectionsMap.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.*.replica_state) |r_state| if (@as(ReplicaStateType, r_state) == replica_type) {
                try buf.append(entry.value_ptr.*);
            };
        }

        return buf.toOwnedSlice();
    }
};
