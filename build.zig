const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exchange_core = b.addModule("exchange_core", .{
        .root_source_file = b.path("src/zig/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const module_tests = b.addTest(.{
        .root_module = exchange_core,
    });

    const run_module_tests = b.addRunArtifact(module_tests);

    const test_step = b.step("test", "Run exchange-core Zig unit tests");
    test_step.dependOn(&run_module_tests.step);

    const release_safe_module = b.createModule(.{
        .root_source_file = b.path("src/zig/root.zig"),
        .target = target,
        .optimize = .ReleaseSafe,
    });
    const release_safe_tests = b.addTest(.{
        .root_module = release_safe_module,
    });
    const run_release_safe_tests = b.addRunArtifact(release_safe_tests);
    const test_release_safe_step = b.step("test-release-safe", "Run exchange-core Zig unit tests in ReleaseSafe mode");
    test_release_safe_step.dependOn(&run_release_safe_tests.step);

    const bench_module = b.createModule(.{
        .root_source_file = b.path("src/zig/bench/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    bench_module.addImport("exchange_core", exchange_core);
    const bench_exe = b.addExecutable(.{
        .name = "exchange-core-bench",
        .root_module = bench_module,
    });
    const run_bench = b.addRunArtifact(bench_exe);
    if (b.args) |args| {
        run_bench.addArgs(args);
    }

    const bench_step = b.step("bench", "Run exchange-core Zig benchmark harness");
    bench_step.dependOn(&run_bench.step);

    const parity_diff_module = b.createModule(.{
        .root_source_file = b.path("src/zig/tools/fixture_diff.zig"),
        .target = target,
        .optimize = optimize,
    });
    parity_diff_module.addImport("exchange_core", exchange_core);
    const parity_diff_exe = b.addExecutable(.{
        .name = "exchange-core-parity-diff",
        .root_module = parity_diff_module,
    });
    const run_parity_diff = b.addRunArtifact(parity_diff_exe);
    if (b.args) |args| {
        run_parity_diff.addArgs(args);
    }

    const parity_diff_step = b.step("parity-diff", "Run Zig vs fixture parity diff");
    parity_diff_step.dependOn(&run_parity_diff.step);

    const api_surface_check_module = b.createModule(.{
        .root_source_file = b.path("src/zig/tools/api_surface_check.zig"),
        .target = target,
        .optimize = optimize,
    });
    const api_surface_check_exe = b.addExecutable(.{
        .name = "exchange-core-api-surface-check",
        .root_module = api_surface_check_module,
    });
    const run_api_surface_check = b.addRunArtifact(api_surface_check_exe);
    const api_surface_check_step = b.step(
        "api-surface-check",
        "Ensure ExchangeApi exposes every public ExchangeCore method",
    );
    api_surface_check_step.dependOn(&run_api_surface_check.step);

    const java_api_surface_diff_module = b.createModule(.{
        .root_source_file = b.path("src/zig/tools/java_exchange_api_surface_diff.zig"),
        .target = target,
        .optimize = optimize,
    });
    const java_api_surface_diff_exe = b.addExecutable(.{
        .name = "exchange-core-java-api-surface-diff",
        .root_module = java_api_surface_diff_module,
    });
    const run_java_api_surface_diff = b.addRunArtifact(java_api_surface_diff_exe);
    const java_api_surface_diff_step = b.step(
        "java-api-surface-diff",
        "Generate Java-vs-Zig ExchangeApi surface diff artifacts",
    );
    java_api_surface_diff_step.dependOn(&run_java_api_surface_diff.step);

    const java_api_parity_audit_module = b.createModule(.{
        .root_source_file = b.path("src/zig/tools/java_exchange_api_parity_audit.zig"),
        .target = target,
        .optimize = optimize,
    });
    const java_api_parity_audit_exe = b.addExecutable(.{
        .name = "exchange-core-java-api-parity-audit",
        .root_module = java_api_parity_audit_module,
    });
    const run_java_api_parity_audit = b.addRunArtifact(java_api_parity_audit_exe);
    const java_api_parity_audit_step = b.step(
        "java-api-parity-audit",
        "Generate class-by-class Java-vs-Zig ExchangeApi parity audit artifacts and fail on missing same-name coverage",
    );
    java_api_parity_audit_step.dependOn(&run_java_api_parity_audit.step);

    const gate_step = b.step(
        "gate",
        "Run full Zig migration gate (test, test-release-safe, parity-diff, api-surface-check, java-api-parity-audit)",
    );
    gate_step.dependOn(&run_module_tests.step);
    gate_step.dependOn(&run_release_safe_tests.step);
    gate_step.dependOn(&run_parity_diff.step);
    gate_step.dependOn(&run_api_surface_check.step);
    gate_step.dependOn(&run_java_api_parity_audit.step);
}
