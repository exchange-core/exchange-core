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

    const orchestrator_ab_module = b.createModule(.{
        .root_source_file = b.path("src/zig/bench/orchestrator_ab.zig"),
        .target = target,
        .optimize = optimize,
    });
    orchestrator_ab_module.addImport("exchange_core", exchange_core);
    const orchestrator_ab_exe = b.addExecutable(.{
        .name = "exchange-core-orchestrator-ab",
        .root_module = orchestrator_ab_module,
    });
    const run_orchestrator_ab = b.addRunArtifact(orchestrator_ab_exe);
    if (b.args) |args| {
        run_orchestrator_ab.addArgs(args);
    }

    const orchestrator_ab_step = b.step(
        "bench-orchestrator-ab",
        "Run orchestrator immediate vs persistent-grouped A/B benchmark",
    );
    orchestrator_ab_step.dependOn(&run_orchestrator_ab.step);

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
    const parity_diff_tests = b.addTest(.{
        .root_module = parity_diff_module,
    });
    const run_parity_diff_tests = b.addRunArtifact(parity_diff_tests);

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
    const api_surface_check_tests = b.addTest(.{
        .root_module = api_surface_check_module,
    });
    const run_api_surface_check_tests = b.addRunArtifact(api_surface_check_tests);
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
    const java_api_surface_diff_tests = b.addTest(.{
        .root_module = java_api_surface_diff_module,
    });
    const run_java_api_surface_diff_tests = b.addRunArtifact(java_api_surface_diff_tests);
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
    const java_api_parity_audit_tests = b.addTest(.{
        .root_module = java_api_parity_audit_module,
    });
    const run_java_api_parity_audit_tests = b.addRunArtifact(java_api_parity_audit_tests);
    const java_api_parity_audit_step = b.step(
        "java-api-parity-audit",
        "Generate class-by-class Java-vs-Zig ExchangeApi parity audit artifacts and fail on missing same-name coverage",
    );
    java_api_parity_audit_step.dependOn(&run_java_api_parity_audit.step);

    const java_class_map_check_module = b.createModule(.{
        .root_source_file = b.path("src/zig/tools/java_production_class_map_check.zig"),
        .target = target,
        .optimize = optimize,
    });
    const java_class_map_check_exe = b.addExecutable(.{
        .name = "exchange-core-java-production-class-map-check",
        .root_module = java_class_map_check_module,
    });
    const run_java_class_map_check = b.addRunArtifact(java_class_map_check_exe);
    const java_class_map_check_tests = b.addTest(.{
        .root_module = java_class_map_check_module,
    });
    const run_java_class_map_check_tests = b.addRunArtifact(java_class_map_check_tests);
    const java_class_map_check_step = b.step(
        "java-class-map-check",
        "Ensure every Java production class has exactly one Zig responsibility-map row",
    );
    java_class_map_check_step.dependOn(&run_java_class_map_check.step);

    const tool_tests_step = b.step(
        "tool-tests",
        "Run migration evidence tool unit tests",
    );
    tool_tests_step.dependOn(&run_parity_diff_tests.step);
    tool_tests_step.dependOn(&run_api_surface_check_tests.step);
    tool_tests_step.dependOn(&run_java_api_surface_diff_tests.step);
    tool_tests_step.dependOn(&run_java_api_parity_audit_tests.step);
    tool_tests_step.dependOn(&run_java_class_map_check_tests.step);

    const gate_step = b.step(
        "gate",
        "Run full Zig migration gate (test, test-release-safe, parity-diff, API surface checks and audits, Java production class map)",
    );
    // Some persistence tests intentionally exercise stable Java-compatible
    // artifact names. Keep the two optimization-mode runners serial inside
    // the aggregate gate so they cannot race each other, while preserving the
    // standalone `test-release-safe` step as a ReleaseSafe-only command.
    const run_gate_release_safe_tests = b.addRunArtifact(release_safe_tests);
    run_gate_release_safe_tests.step.dependOn(&run_module_tests.step);
    gate_step.dependOn(&run_gate_release_safe_tests.step);
    // The standalone parity diff accepts an optional fixture path through
    // `b.args`. The aggregate gate must always exercise its default fixture
    // set, regardless of arguments supplied to another requested build step.
    const run_gate_parity_diff = b.addRunArtifact(parity_diff_exe);
    gate_step.dependOn(&run_gate_parity_diff.step);
    gate_step.dependOn(&run_api_surface_check.step);
    // Keep the two ExchangeApi evidence generators serial within the gate.
    // Gate-specific runners preserve independent standalone commands while
    // ensuring checked-in evidence files are regenerated deterministically.
    const run_gate_java_api_parity_audit = b.addRunArtifact(java_api_parity_audit_exe);
    const run_gate_java_api_surface_diff = b.addRunArtifact(java_api_surface_diff_exe);
    run_gate_java_api_surface_diff.step.dependOn(&run_gate_java_api_parity_audit.step);
    gate_step.dependOn(&run_gate_java_api_surface_diff.step);
    gate_step.dependOn(tool_tests_step);
    gate_step.dependOn(&run_java_class_map_check.step);
}
