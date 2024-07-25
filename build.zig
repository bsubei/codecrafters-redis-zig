const std = @import("std");

// Learn more about this file here: https://ziglang.org/learn/build-system
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exe = b.addExecutable(.{
        .name = "zig",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    // This declares intent for the executable to be installed into the
    // standard location when the user invokes the "install" step (the default
    // step when running `zig build`).
    b.installArtifact(exe);

    // This *creates* a Run step in the build graph, to be executed when another
    // step is evaluated that depends on it. The next line below will establish
    // such a dependency.
    const run_cmd = b.addRunArtifact(exe);

    // This creates a build step. It will be visible in the `zig build --help` menu,
    // and can be selected like this: `zig build run`
    // This will evaluate the `run` step rather than the default, which is "install".
    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    // This allows the user to pass arguments to the application in the build
    // command itself, like this: `zig build run -- arg1 arg2 etc`
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    // Add test step.
    const test_step = b.step("test", "Run the tests");
    // TODO figure out how to properly run all tests defined in all files. We're hardcoding the file names for now.
    const test_runner = b.addTest(.{ .root_source_file = b.path("src/RwLockHashMap.zig"), .target = target, .optimize = optimize });
    //    // Create a test runner.
    //    const test_runner = b.addTest(.{
    //        .root_source_file = b.path("src/tests.zig"),
    //        .target = target,
    //        .optimize = optimize,
    //    });
    //    // Add all the .zig files in the src/ directory to the test runner.
    //    const src_dir = std.fs.cwd().openDir("src", .{ .iterate = true }) catch unreachable;
    //    var walker = src_dir.walk(b.allocator) catch unreachable;
    //    defer walker.deinit();
    //
    //    while (walker.next() catch unreachable) |entry| {
    //        if (entry.kind == .file and std.mem.endsWith(u8, entry.path, ".zig")) {
    //            test_runner.addIncludePath(b.path(b.fmt("src/{s}", .{entry.path})));
    //        }
    //    }

    const run_tests = b.addRunArtifact(test_runner);
    test_step.dependOn(&run_tests.step);
}
