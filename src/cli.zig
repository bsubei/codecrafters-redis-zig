const std = @import("std");

const DEFAULT_PORT = 6379;
const Error = error{
    BadCLIArgument,
};
pub const Args = struct {
    port: u16,
};
pub fn parseArgs(allocator: std.mem.Allocator) !Args {
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var port: ?u16 = null;

    for (args, 0..) |arg, idx| {
        if (std.mem.eql(u8, arg, "--port")) {
            if (idx + 1 >= args.len) {
                return Error.BadCLIArgument;
            }
            port = try std.fmt.parseInt(u16, args[idx + 1], 10);
        }
    }

    return Args{ .port = if (port) |p| p else DEFAULT_PORT };
}
