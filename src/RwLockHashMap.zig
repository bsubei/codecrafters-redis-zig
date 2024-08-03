//! Thread-safe hashmap that makes copies of and owns the string keys and string values.
//! Supports including expiry timestamps, which will filter out any expired values when get() is called.
const std = @import("std");
const RwLock = std.Thread.RwLock;
const Allocator = std.mem.Allocator;
const stdout = std.io.getStdOut().writer();
const testing = std.testing;

const Self = @This();
const K = []const u8;
const V = []const u8;
pub const ExpiryTimestampMs = ?i64;
const ValueTimestampPair = struct { value: V, expiry_timestamp_ms: ExpiryTimestampMs };

map: std.StringHashMap(ValueTimestampPair),
rwLock: RwLock,

pub fn init(allocator: Allocator) Self {
    return .{ .map = std.StringHashMap(ValueTimestampPair).init(allocator), .rwLock = RwLock{} };
}

pub fn deinit(self: *Self) void {
    // Free the owned strings that we allocated for the keys and values in the hashmap. We don't free the map itself yet.
    var iter = self.map.iterator();
    while (iter.next()) |entry| {
        self.map.allocator.free(entry.key_ptr.*);
        self.map.allocator.free(entry.value_ptr.*.value);
    }
    // Free the map itself (this is the header + array of entries it uses to store the key-value slices/pointers).
    self.map.deinit();
    self.* = undefined;
}

/// Given strings or u8 slices (key-value pair), insert them into the hash map by copying them.
pub fn put(self: *Self, key: K, value: V) !void {
    try self.putWithExpiry(key, value, null);
}

/// The provided expiry timestamp is relative, i.e. 100 means the given key-value will expire 100 milliseconds in the future from now.
pub fn putWithExpiry(self: *Self, key: K, value: V, expiry: ExpiryTimestampMs) !void {
    self.rwLock.lock();
    defer self.rwLock.unlock();

    // We always need to copy the string value and own it because the old one is going away.
    const owned_value = try self.map.allocator.dupe(u8, value);
    errdefer self.map.allocator.free(owned_value);

    // Remove the old value, place the new one in its stead. The key stays the same.
    if (self.map.getPtr(key)) |old_value_ptr| {
        // We need to free the old value string because no one looks at it anymore. We also update the expiry.
        self.map.allocator.free(old_value_ptr.*.value);
        old_value_ptr.* = .{ .value = owned_value, .expiry_timestamp_ms = expiry };
    } else {
        // Make a copy of the key, and insert the kv into the map.
        const owned_key = try self.map.allocator.dupe(u8, key);
        errdefer self.map.allocator.free(owned_key);
        const new_entry = .{ .value = owned_value, .expiry_timestamp_ms = expiry };
        try self.map.putNoClobber(owned_key, new_entry);
    }
}

/// Get the value behind the given key if it exists AND if it's not expired.
pub fn get(self: *Self, key: K) ?V {
    self.rwLock.lockShared();
    defer self.rwLock.unlockShared();
    const pair = self.map.get(key);
    if (pair) |p| {
        if (p.expiry_timestamp_ms) |ts| {
            const now_ms = std.time.milliTimestamp();
            if (ts > now_ms) {
                return p.value;
            }
        } else {
            return p.value;
        }
    }
    return null;
}

/// Print out the contents of the hash map.
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
    self.rwLock.lockShared();
    defer self.rwLock.unlockShared();

    return self.map.count();
}

// TODO finish tests
test "RwLockHashMap basic access patterns" {
    const allocator = std.testing.allocator;
    var map = Self.init(allocator);
    defer map.deinit();

    try testing.expectEqual(map.count(), 0);

    try map.put("hi", "bye");
    try testing.expectEqual(map.count(), 1);
    const result = map.get("hi");
    try testing.expect(result != null);
    try testing.expectEqualSlices(u8, result.?, "bye");
}

// test "RwLockHashMap concurrent reads do not lock" {}

// test "RwLockHashMap concurrent reads/writes do lock" {}

// test "RwLockHashMap concurrent read/writes are correct" {}
