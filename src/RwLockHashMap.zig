const std = @import("std");
const RwLock = std.Thread.RwLock;
const Allocator = std.mem.Allocator;
const stdout = std.io.getStdOut().writer();
const testing = std.testing;

pub const RwLockHashMap = struct {
    const Self = @This();
    const K = []const u8;
    const V = []const u8;
    const ValueTimestampPair = struct { value: V, expiry_timestamp_ms: ?i64 };

    map: std.StringHashMap(ValueTimestampPair),
    rwLock: RwLock,

    pub fn init(allocator: Allocator) Self {
        return .{ .map = std.StringHashMap(ValueTimestampPair).init(allocator), .rwLock = RwLock{} };
    }

    pub fn deinit(self: *Self) void {
        // Free the contents of the map.
        var iter = self.map.iterator();
        while (iter.next()) |entry| {
            self.map.allocator.free(entry.key_ptr.*);
            self.map.allocator.free(entry.value_ptr.*.value);
            self.map.allocator.destroy(entry.value_ptr);
        }
        // Free the map itself.
        self.map.deinit();
    }

    // Given strings or u8 slices (key-value pair), insert them into the hash map by copying them.
    pub fn put(self: *Self, key: K, value: V) !void {
        try self.putWithExpiry(key, value, null);
    }

    pub fn putWithExpiry(self: *Self, key: K, value: V, expiry: ?i64) !void {
        self.rwLock.lock();
        defer self.rwLock.unlock();

        // We definitely need to copy the string value and own it because the old one is going away.
        const owned_value = try self.map.allocator.dupe(u8, value);
        errdefer self.map.allocator.free(owned_value);

        // Remove the old value, place the new one in its stead.
        if (self.map.getPtr(key)) |old_value_ptr| {
            // We need to free the old string because no one looks at it anymore.
            self.map.allocator.free(old_value_ptr.*.value);
            old_value_ptr.* = .{ .value = owned_value, .expiry_timestamp_ms = expiry };
        } else {
            // Create the key and value.
            const owned_key = try self.map.allocator.dupe(u8, key);
            errdefer self.map.allocator.free(owned_key);
            const new_entry = try self.map.allocator.create(ValueTimestampPair);
            new_entry.* = .{ .value = owned_value, .expiry_timestamp_ms = expiry };
            // Insert the copied key and value.
            try self.map.put(owned_key, new_entry.*);
        }
    }

    pub fn get(self: *Self, key: K) ?V {
        self.rwLock.lockShared();
        defer self.rwLock.unlockShared();
        const pair = self.map.get(key);
        if (pair) |p| {
            if (p.expiry_timestamp_ms) |ts| {
                const now_ms = std.time.timestamp() * std.time.ms_per_s;
                if (ts > now_ms) {
                    return p.value;
                }
            } else {
                return p.value;
            }
        }
        return null;
    }

    pub fn print(self: *Self) !void {
        self.rwLock.lockShared();
        defer self.rwLock.unlockShared();

        try stdout.print("Cache size: {d}\nContents:\n", .{self.count()});
        var it = self.map.iterator();
        while (it.next()) |entry| {
            try stdout.print("{s}: {s} {?}\n", .{ entry.key_ptr.*, entry.value_ptr.value, entry.value_ptr.expiry_timestamp_ms });
        }
    }

    pub fn count(self: *Self) @TypeOf(self.map).Size {
        return self.map.count();
    }
};

test "RwLockHashMap basic access patterns" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    var map = RwLockHashMap.init(allocator);
    defer map.deinit();

    try testing.expectEqual(map.count(), 0);

    try map.put("hi", "bye");
    try testing.expectEqual(map.count(), 1);
    const result = map.get("hi");
    try testing.expect(result != null);
    try testing.expectEqualSlices(u8, result.?, "bye");

    // TODO finish tests
}

test "RwLockHashMap concurrent reads do not lock" {
    try testing.expect(false);
}

test "RwLockHashMap concurrent reads/writes do lock" {
    try testing.expect(false);
}

test "RwLockHashMap concurrent read/writes are correct" {
    try testing.expect(false);
}
