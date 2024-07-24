const std = @import("std");
const RwLock = std.Thread.RwLock;
const Allocator = std.mem.Allocator;
const stdout = std.io.getStdOut().writer();

// TODO the map currently doesn't own its data. Change it to own its data, which means it has to free the keys and values.
pub const RwLockHashMap = struct {
    const Self = @This();
    const K = []const u8;
    const V = []const u8;

    map: std.StringHashMap(V),
    rwLock: RwLock,

    pub fn init(allocator: Allocator) Self {
        return .{ .map = std.StringHashMap(V).init(allocator), .rwLock = RwLock{} };
    }
    pub fn deinit(self: *Self) void {
        self.map.deinit();
    }

    pub fn put(self: *Self, key: K, value: V) !void {
        self.rwLock.lock();
        defer self.rwLock.unlock();

        try self.map.put(key, value);
    }
    pub fn get(self: *Self, key: K) ?V {
        self.rwLock.lockShared();
        defer self.rwLock.unlockShared();

        return self.map.get(key);
    }
    pub fn print(self: *Self) !void {
        self.rwLock.lockShared();
        defer self.rwLock.unlockShared();

        try stdout.print("Cache size: {d}\nContents:\n", .{self.map.count()});
        var it = self.map.iterator();
        while (it.next()) |entry| {
            try stdout.print("{s}: {s}\n", .{ entry.key_ptr.*, entry.value_ptr.* });
        }
    }
};

// TODO write some simple tests.
