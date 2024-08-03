const std = @import("std");
const testing = std.testing;

/// This function appends the given field's name and value (in the format "name:value\n") to the given input_string and returns the result.
/// The function always takes ownership of (consumes) the input_string. The caller is responsible for the returned string.
///
/// String-like values (slices, arrays, and many-pointers) are printed as strings.
/// Numbers are printed in decimal representation as strings.
/// Activated optionals (either string-like or numbers) are printed as strings. Other optional types not supported yet.
/// Unactivated optionals are simply skipped, and we return the same input_string.
/// Enums values are converted to their @tagName (string representation of the value).
/// TODO consider using std.fmt.formatType or something else to reduce the amount of code here. Also to support optional enums and arbitrary types.
pub fn appendNameValue(allocator: std.mem.Allocator, comptime field_type: type, field_name: []const u8, field_value: anytype, input_string: []const u8) ![]const u8 {
    // Skip any optional fields that are not activated.
    if (@typeInfo(field_type) != .Optional or field_value != null) {
        defer allocator.free(input_string);

        // The formatter string depends on the field_type, i.e. {s} for strings and {d} for decimals etc.
        const line = try allocPrintNameValue(allocator, field_type, field_name, field_value);
        defer allocator.free(line);

        return try std.fmt.allocPrint(allocator, "{s}\n{s}", .{ input_string, line });
    }
    return input_string;
}

/// Given a field's type, name, and value, call allocPrint with the appropriate string formatter, i.e. "{}:{}\n" for name:value. Can
/// handle Enums, Optionals, Numbers, and Strings (as slices, pointers, or arrays).
fn allocPrintNameValue(allocator: std.mem.Allocator, comptime field_type: type, field_name: []const u8, field_value: anytype) ![]const u8 {
    switch (@typeInfo(field_type)) {
        .Enum => {
            return try std.fmt.allocPrint(allocator, "{s}:{s}\n", .{ field_name, @tagName(field_value) });
        },
        else => {
            const formatter_string = comptime getFormatterString(field_type);
            return try std.fmt.allocPrint(allocator, formatter_string, .{ field_name, field_value });
        },
    }
}

fn isStringType(comptime field: type) bool {
    switch (@typeInfo(field)) {
        .Array => |info| {
            if (info.child == u8) {
                return true;
            }
        },
        .Pointer => |info| {
            if ((info.size == .Slice or info.size == .Many) and info.child == u8) {
                return true;
            }
        },
        else => return false,
    }
}

fn getFormatterString(comptime T: type) []const u8 {
    const is_optional = @typeInfo(T) == .Optional;
    const underlying_type = if (is_optional) @typeInfo(T).Optional.child else T;

    if (isStringType(underlying_type)) {
        return if (is_optional) "{s}:{?s}\n" else "{s}:{s}\n";
    } else {
        return if (is_optional) "{s}:{?d}\n" else "{s}:{d}\n";
    }
}
test "getFormatterString" {
    try testing.expectEqualSlices(u8, "{s}:{s}\n", getFormatterString([]const u8));
    try testing.expectEqualSlices(u8, "{s}:{?s}\n", getFormatterString(?[]const u8));
    try testing.expectEqualSlices(u8, "{s}:{s}\n", getFormatterString([100]u8));
    try testing.expectEqualSlices(u8, "{s}:{?s}\n", getFormatterString(?[100]u8));
    try testing.expectEqualSlices(u8, "{s}:{d}\n", getFormatterString(u64));
    try testing.expectEqualSlices(u8, "{s}:{?d}\n", getFormatterString(?u64));
}
