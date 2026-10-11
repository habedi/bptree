const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // --- Common C compilation settings ---

    const c_flags: []const []const u8 = &.{
        "-std=c11",
        "-Wall",
        "-Wextra",
        "-pedantic",
    };

    // --- Library header installation ---
    // `zig build` (default step) installs the header to `zig-out/include/`.

    const install_header = b.addInstallFileWithDir(
        b.path("include/bptree.h"),
        .header,
        "bptree.h",
    );
    b.getInstallStep().dependOn(&install_header.step);

    // --- Executables ---

    const test_exe = addCExe(b, "test_bptree", "test/test_bptree.c", target, optimize, c_flags);
    const bench_exe = addCExe(b, "bench_bptree", "test/bench_bptree.c", target, .ReleaseFast, c_flags);
    const example_exe = addCExe(b, "example", "test/example.c", target, optimize, c_flags);
    const safety_exe = addCExe(b, "test_safety", "test/extras/test_safety.c", target, optimize, c_flags);
    const safety_string_exe = addCExe(b, "test_safety_string", "test/extras/test_safety.c", target, optimize, c_flags);
    safety_string_exe.root_module.addCMacro("BPTREE_KEY_TYPE_STRING", "1");
    safety_string_exe.root_module.addCMacro("BPTREE_KEY_SIZE", "5");
    const safety_wide_exe = addCExe(b, "test_safety_wide", "test/extras/test_safety.c", target, optimize, c_flags);
    safety_wide_exe.root_module.addCMacro("BPTREE_TEST_WIDE_VALUE", "1");

    // Install all executables so `zig build` puts them in zig-out/bin/.
    b.installArtifact(test_exe);
    b.installArtifact(bench_exe);
    b.installArtifact(example_exe);

    // --- Top-level steps ---

    // `zig build test` build and run the unit tests
    const test_step = b.step("test", "Build and run unit tests");
    const run_tests = b.addRunArtifact(test_exe);
    run_tests.step.dependOn(b.getInstallStep());
    test_step.dependOn(&run_tests.step);
    for ([_]*std.Build.Step.Compile{ safety_exe, safety_string_exe, safety_wide_exe }) |exe| {
        test_step.dependOn(&b.addRunArtifact(exe).step);
    }

    // `zig build bench` build and run the benchmarks
    const bench_step = b.step("bench", "Build and run benchmarks");
    const run_bench = b.addRunArtifact(bench_exe);
    run_bench.step.dependOn(b.getInstallStep());
    bench_step.dependOn(&run_bench.step);

    // `zig build example` build and run the example program
    const example_step = b.step("example", "Build and run the example program");
    const run_example = b.addRunArtifact(example_exe);
    run_example.step.dependOn(b.getInstallStep());
    example_step.dependOn(&run_example.step);
}

/// Helper: create a C executable from a single source file with the project's
/// standard include path and compiler flags.
fn addCExe(
    b: *std.Build,
    name: []const u8,
    source: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    c_flags: []const []const u8,
) *std.Build.Step.Compile {
    const mod = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    mod.addCSourceFile(.{
        .file = b.path(source),
        .flags = c_flags,
    });
    mod.addIncludePath(b.path("include"));

    const exe = b.addExecutable(.{
        .name = name,
        .root_module = mod,
    });
    return exe;
}
