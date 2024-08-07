const std = @import("std");
const testing = std.testing;

pub fn getEmptyRdb(allocator: std.mem.Allocator) ![]const u8 {
    const rdb_in_base64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

    const decoder = std.base64.standard.Decoder;
    const decoded_size = try decoder.calcSizeForSlice(rdb_in_base64);

    const buf = try allocator.alloc(u8, decoded_size);
    errdefer allocator.free(buf);
    try decoder.decode(buf, rdb_in_base64);
    return buf;
}

test "getEmptyRdb decodes to exactly the hex we expect" {
    const rdb = try getEmptyRdb(testing.allocator);
    defer testing.allocator.free(rdb);

    const hex = try std.fmt.allocPrint(testing.allocator, "{x}", .{std.fmt.fmtSliceHexLower(rdb)});
    defer testing.allocator.free(hex);
    const expected = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
    try testing.expectEqualSlices(u8, expected, hex);
}
