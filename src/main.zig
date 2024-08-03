const std = @import("std");
const Cache = @import("RwLockHashMap.zig");
const cli = @import("cli.zig");
const server_config = @import("config.zig");
const server = @import("server.zig");

fn runServer(allocator: std.mem.Allocator, args: *const cli.Args) !void {
    const config = try server_config.createConfig(args.*);

    var cache = Cache.init(allocator);
    defer cache.deinit();

    switch (config.replication.role) {
        .master => {
            try server.runMasterServer(args, &config, &cache);
        },
        .slave => {
            try server.runSlaveServer(args, &config, &cache);
        },
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var args = try cli.parseArgs(allocator);
    defer args.deinit();
    try runServer(allocator, &args);
}
