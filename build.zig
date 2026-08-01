const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exchange_core = b.addModule("exchange_core", .{
        .root_source_file = b.path("src/zig/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const exchange_protocol = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/protocol.zig"),
        .target = target,
        .optimize = optimize,
    });

    const server_module = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/server.zig"),
        .target = target,
        .optimize = optimize,
    });
    server_module.addImport("exchange_core", exchange_core);
    server_module.addImport("exchange_protocol", exchange_protocol);
    const server_exe = b.addExecutable(.{
        .name = "exchange-core-server",
        .root_module = server_module,
    });
    const install_server = b.addInstallArtifact(server_exe, .{});
    b.getInstallStep().dependOn(&install_server.step);
    const server_step = b.step("server", "Build and install the standalone TCP server");
    server_step.dependOn(&install_server.step);

    const run_server = b.addRunArtifact(server_exe);
    if (b.args) |args| {
        run_server.addArgs(args);
    }
    const run_server_step = b.step("run-server", "Run the standalone TCP server");
    run_server_step.dependOn(&run_server.step);

    const client_module = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/client.zig"),
        .target = target,
        .optimize = optimize,
    });
    client_module.addImport("exchange_protocol", exchange_protocol);
    const client_exe = b.addExecutable(.{
        .name = "exchange-core-client",
        .root_module = client_module,
    });
    const install_client = b.addInstallArtifact(client_exe, .{});
    b.getInstallStep().dependOn(&install_client.step);
    const client_step = b.step("client", "Build and install the standalone TCP client");
    client_step.dependOn(&install_client.step);

    const run_client = b.addRunArtifact(client_exe);
    if (b.args) |args| {
        run_client.addArgs(args);
    }
    const run_client_step = b.step("run-client", "Run the standalone TCP client");
    run_client_step.dependOn(&run_client.step);

    const module_tests = b.addTest(.{
        .root_module = exchange_core,
    });

    const run_module_tests = b.addRunArtifact(module_tests);

    const debug_exchange_core = b.createModule(.{
        .root_source_file = b.path("src/zig/root.zig"),
        .target = target,
        .optimize = .Debug,
    });
    const debug_exchange_protocol = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/protocol.zig"),
        .target = target,
        .optimize = .Debug,
    });
    const debug_protocol_tests = b.addTest(.{
        .root_module = debug_exchange_protocol,
    });
    const run_debug_protocol_tests = b.addRunArtifact(debug_protocol_tests);
    run_debug_protocol_tests.step.dependOn(&run_module_tests.step);

    const debug_parix_transport_test_module = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/parix_http_transport.zig"),
        .target = target,
        .optimize = .Debug,
    });
    debug_parix_transport_test_module.addImport("exchange_core", debug_exchange_core);
    const debug_parix_transport_tests = b.addTest(.{
        .root_module = debug_parix_transport_test_module,
    });
    const run_debug_parix_transport_tests = b.addRunArtifact(debug_parix_transport_tests);
    run_debug_parix_transport_tests.step.dependOn(&run_debug_protocol_tests.step);

    const debug_parix_bootstrap_test_module = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/parix_account_bootstrap.zig"),
        .target = target,
        .optimize = .Debug,
    });
    debug_parix_bootstrap_test_module.addImport("exchange_core", debug_exchange_core);
    const debug_parix_bootstrap_tests = b.addTest(.{
        .root_module = debug_parix_bootstrap_test_module,
    });
    const run_debug_parix_bootstrap_tests = b.addRunArtifact(debug_parix_bootstrap_tests);
    run_debug_parix_bootstrap_tests.step.dependOn(&run_debug_parix_transport_tests.step);

    const debug_parix_runtime_test_module = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/parix_settlement_runtime.zig"),
        .target = target,
        .optimize = .Debug,
    });
    debug_parix_runtime_test_module.addImport("exchange_core", debug_exchange_core);
    const debug_parix_runtime_tests = b.addTest(.{
        .root_module = debug_parix_runtime_test_module,
    });
    const run_debug_parix_runtime_tests = b.addRunArtifact(debug_parix_runtime_tests);
    run_debug_parix_runtime_tests.step.dependOn(&run_debug_parix_bootstrap_tests.step);

    const debug_parix_funding_saga_test_module = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/parix_funding_saga.zig"),
        .target = target,
        .optimize = .Debug,
    });
    debug_parix_funding_saga_test_module.addImport("exchange_core", debug_exchange_core);
    const debug_parix_funding_saga_tests = b.addTest(.{
        .root_module = debug_parix_funding_saga_test_module,
    });
    const run_debug_parix_funding_saga_tests = b.addRunArtifact(debug_parix_funding_saga_tests);
    run_debug_parix_funding_saga_tests.step.dependOn(&run_debug_parix_runtime_tests.step);

    const debug_parix_funding_store_test_module = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/parix_funding_store.zig"),
        .target = target,
        .optimize = .Debug,
    });
    debug_parix_funding_store_test_module.addImport("exchange_core", debug_exchange_core);
    const debug_parix_funding_store_tests = b.addTest(.{
        .root_module = debug_parix_funding_store_test_module,
    });
    const run_debug_parix_funding_store_tests = b.addRunArtifact(debug_parix_funding_store_tests);
    run_debug_parix_funding_store_tests.step.dependOn(&run_debug_parix_funding_saga_tests.step);

    const debug_parix_funding_runtime_test_module = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/parix_funding_runtime.zig"),
        .target = target,
        .optimize = .Debug,
    });
    debug_parix_funding_runtime_test_module.addImport("exchange_core", debug_exchange_core);
    const debug_parix_funding_runtime_tests = b.addTest(.{
        .root_module = debug_parix_funding_runtime_test_module,
    });
    const run_debug_parix_funding_runtime_tests = b.addRunArtifact(debug_parix_funding_runtime_tests);
    run_debug_parix_funding_runtime_tests.step.dependOn(&run_debug_parix_funding_store_tests.step);

    const debug_server_test_module = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/server.zig"),
        .target = target,
        .optimize = .Debug,
    });
    debug_server_test_module.addImport("exchange_core", debug_exchange_core);
    debug_server_test_module.addImport("exchange_protocol", debug_exchange_protocol);
    const debug_server_tests = b.addTest(.{
        .root_module = debug_server_test_module,
    });
    const run_debug_server_tests = b.addRunArtifact(debug_server_tests);
    run_debug_server_tests.step.dependOn(&run_debug_parix_funding_runtime_tests.step);
    const test_parix_apps_step = b.step(
        "test-parix-apps",
        "Run native Parix transport, bootstrap, settlement, funding, and server tests in Debug mode",
    );
    test_parix_apps_step.dependOn(&run_debug_server_tests.step);

    const debug_client_test_module = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/client.zig"),
        .target = target,
        .optimize = .Debug,
    });
    debug_client_test_module.addImport("exchange_protocol", debug_exchange_protocol);
    const debug_client_tests = b.addTest(.{
        .root_module = debug_client_test_module,
    });
    const run_debug_client_tests = b.addRunArtifact(debug_client_tests);
    run_debug_client_tests.step.dependOn(&run_debug_server_tests.step);

    const debug_exchange_kernel = b.createModule(.{
        .root_source_file = b.path("src/zig/core/exchange_kernel.zig"),
        .target = target,
        .optimize = .Debug,
    });
    const debug_browser_test_module = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/browser/wasm.zig"),
        .target = target,
        .optimize = .Debug,
    });
    debug_browser_test_module.addImport("exchange_kernel", debug_exchange_kernel);
    const debug_browser_tests = b.addTest(.{
        .root_module = debug_browser_test_module,
    });
    const run_debug_browser_tests = b.addRunArtifact(debug_browser_tests);
    run_debug_browser_tests.step.dependOn(&run_debug_client_tests.step);

    const debug_simd_bench_module = b.createModule(.{
        .root_source_file = b.path("src/zig/bench/simd.zig"),
        .target = target,
        .optimize = .Debug,
    });
    debug_simd_bench_module.addImport("exchange_core", debug_exchange_core);
    const debug_simd_bench_tests = b.addTest(.{
        .root_module = debug_simd_bench_module,
    });
    const run_debug_simd_bench_tests = b.addRunArtifact(debug_simd_bench_tests);
    run_debug_simd_bench_tests.step.dependOn(&run_debug_browser_tests.step);

    const test_step = b.step(
        "test",
        "Run exchange-core, standalone app, browser adapter, and SIMD benchmark Zig unit tests",
    );
    test_step.dependOn(&run_debug_simd_bench_tests.step);

    const release_safe_module = b.createModule(.{
        .root_source_file = b.path("src/zig/root.zig"),
        .target = target,
        .optimize = .ReleaseSafe,
    });
    const release_safe_tests = b.addTest(.{
        .root_module = release_safe_module,
    });
    const run_release_safe_tests = b.addRunArtifact(release_safe_tests);

    const release_safe_protocol = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/protocol.zig"),
        .target = target,
        .optimize = .ReleaseSafe,
    });
    const release_safe_protocol_tests = b.addTest(.{
        .root_module = release_safe_protocol,
    });
    const run_release_safe_protocol_tests = b.addRunArtifact(release_safe_protocol_tests);
    run_release_safe_protocol_tests.step.dependOn(&run_release_safe_tests.step);

    const release_safe_parix_transport_test_module = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/parix_http_transport.zig"),
        .target = target,
        .optimize = .ReleaseSafe,
    });
    release_safe_parix_transport_test_module.addImport("exchange_core", release_safe_module);
    const release_safe_parix_transport_tests = b.addTest(.{
        .root_module = release_safe_parix_transport_test_module,
    });
    const run_release_safe_parix_transport_tests = b.addRunArtifact(release_safe_parix_transport_tests);
    run_release_safe_parix_transport_tests.step.dependOn(&run_release_safe_protocol_tests.step);

    const release_safe_parix_bootstrap_test_module = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/parix_account_bootstrap.zig"),
        .target = target,
        .optimize = .ReleaseSafe,
    });
    release_safe_parix_bootstrap_test_module.addImport("exchange_core", release_safe_module);
    const release_safe_parix_bootstrap_tests = b.addTest(.{
        .root_module = release_safe_parix_bootstrap_test_module,
    });
    const run_release_safe_parix_bootstrap_tests = b.addRunArtifact(release_safe_parix_bootstrap_tests);
    run_release_safe_parix_bootstrap_tests.step.dependOn(&run_release_safe_parix_transport_tests.step);

    const release_safe_parix_runtime_test_module = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/parix_settlement_runtime.zig"),
        .target = target,
        .optimize = .ReleaseSafe,
    });
    release_safe_parix_runtime_test_module.addImport("exchange_core", release_safe_module);
    const release_safe_parix_runtime_tests = b.addTest(.{
        .root_module = release_safe_parix_runtime_test_module,
    });
    const run_release_safe_parix_runtime_tests = b.addRunArtifact(release_safe_parix_runtime_tests);
    run_release_safe_parix_runtime_tests.step.dependOn(&run_release_safe_parix_bootstrap_tests.step);

    const release_safe_parix_funding_saga_test_module = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/parix_funding_saga.zig"),
        .target = target,
        .optimize = .ReleaseSafe,
    });
    release_safe_parix_funding_saga_test_module.addImport("exchange_core", release_safe_module);
    const release_safe_parix_funding_saga_tests = b.addTest(.{
        .root_module = release_safe_parix_funding_saga_test_module,
    });
    const run_release_safe_parix_funding_saga_tests = b.addRunArtifact(release_safe_parix_funding_saga_tests);
    run_release_safe_parix_funding_saga_tests.step.dependOn(&run_release_safe_parix_runtime_tests.step);

    const release_safe_parix_funding_store_test_module = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/parix_funding_store.zig"),
        .target = target,
        .optimize = .ReleaseSafe,
    });
    release_safe_parix_funding_store_test_module.addImport("exchange_core", release_safe_module);
    const release_safe_parix_funding_store_tests = b.addTest(.{
        .root_module = release_safe_parix_funding_store_test_module,
    });
    const run_release_safe_parix_funding_store_tests = b.addRunArtifact(release_safe_parix_funding_store_tests);
    run_release_safe_parix_funding_store_tests.step.dependOn(&run_release_safe_parix_funding_saga_tests.step);

    const release_safe_parix_funding_runtime_test_module = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/parix_funding_runtime.zig"),
        .target = target,
        .optimize = .ReleaseSafe,
    });
    release_safe_parix_funding_runtime_test_module.addImport("exchange_core", release_safe_module);
    const release_safe_parix_funding_runtime_tests = b.addTest(.{
        .root_module = release_safe_parix_funding_runtime_test_module,
    });
    const run_release_safe_parix_funding_runtime_tests = b.addRunArtifact(release_safe_parix_funding_runtime_tests);
    run_release_safe_parix_funding_runtime_tests.step.dependOn(&run_release_safe_parix_funding_store_tests.step);

    const release_safe_server_test_module = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/server.zig"),
        .target = target,
        .optimize = .ReleaseSafe,
    });
    release_safe_server_test_module.addImport("exchange_core", release_safe_module);
    release_safe_server_test_module.addImport("exchange_protocol", release_safe_protocol);
    const release_safe_server_tests = b.addTest(.{
        .root_module = release_safe_server_test_module,
    });
    const run_release_safe_server_tests = b.addRunArtifact(release_safe_server_tests);
    run_release_safe_server_tests.step.dependOn(&run_release_safe_parix_funding_runtime_tests.step);
    const test_release_safe_parix_apps_step = b.step(
        "test-parix-apps-release-safe",
        "Run native Parix transport, bootstrap, settlement, funding, and server tests in ReleaseSafe mode",
    );
    test_release_safe_parix_apps_step.dependOn(&run_release_safe_server_tests.step);

    const release_safe_client_test_module = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/client.zig"),
        .target = target,
        .optimize = .ReleaseSafe,
    });
    release_safe_client_test_module.addImport("exchange_protocol", release_safe_protocol);
    const release_safe_client_tests = b.addTest(.{
        .root_module = release_safe_client_test_module,
    });
    const run_release_safe_client_tests = b.addRunArtifact(release_safe_client_tests);
    run_release_safe_client_tests.step.dependOn(&run_release_safe_server_tests.step);

    const release_safe_exchange_kernel = b.createModule(.{
        .root_source_file = b.path("src/zig/core/exchange_kernel.zig"),
        .target = target,
        .optimize = .ReleaseSafe,
    });
    const release_safe_browser_test_module = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/browser/wasm.zig"),
        .target = target,
        .optimize = .ReleaseSafe,
    });
    release_safe_browser_test_module.addImport("exchange_kernel", release_safe_exchange_kernel);
    const release_safe_browser_tests = b.addTest(.{
        .root_module = release_safe_browser_test_module,
    });
    const run_release_safe_browser_tests = b.addRunArtifact(release_safe_browser_tests);
    run_release_safe_browser_tests.step.dependOn(&run_release_safe_client_tests.step);

    const release_safe_simd_bench_module = b.createModule(.{
        .root_source_file = b.path("src/zig/bench/simd.zig"),
        .target = target,
        .optimize = .ReleaseSafe,
    });
    release_safe_simd_bench_module.addImport("exchange_core", release_safe_module);
    const release_safe_simd_bench_tests = b.addTest(.{
        .root_module = release_safe_simd_bench_module,
    });
    const run_release_safe_simd_bench_tests = b.addRunArtifact(release_safe_simd_bench_tests);
    run_release_safe_simd_bench_tests.step.dependOn(&run_release_safe_browser_tests.step);

    const test_release_safe_step = b.step(
        "test-release-safe",
        "Run exchange-core, standalone app, browser adapter, and SIMD benchmark tests in ReleaseSafe mode",
    );
    test_release_safe_step.dependOn(&run_release_safe_simd_bench_tests.step);

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

    const simd_bench_exchange_core = b.createModule(.{
        .root_source_file = b.path("src/zig/root.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const simd_bench_module = b.createModule(.{
        .root_source_file = b.path("src/zig/bench/simd.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    simd_bench_module.addImport("exchange_core", simd_bench_exchange_core);
    const simd_bench_exe = b.addExecutable(.{
        .name = "exchange-core-simd-bench",
        .root_module = simd_bench_module,
    });
    const run_simd_bench = b.addRunArtifact(simd_bench_exe);
    if (b.args) |args| {
        run_simd_bench.addArgs(args);
    }

    const simd_bench_step = b.step(
        "bench-simd",
        "Run the fixed-ReleaseFast three-way binary-carrier SIMD benchmark",
    );
    simd_bench_step.dependOn(&run_simd_bench.step);

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
        "Run full Zig migration gate (core/app/browser tests, browser WASM compile, ReleaseSafe, parity diff, API surface checks and audits, Java production class map)",
    );
    // Some persistence tests intentionally exercise stable Java-compatible
    // artifact names. Keep the two optimization-mode runners serial inside
    // the aggregate gate so they cannot race each other, while preserving the
    // standalone `test-release-safe` step as a ReleaseSafe-only command.
    const run_gate_release_safe_tests = b.addRunArtifact(release_safe_tests);
    run_gate_release_safe_tests.step.dependOn(&run_debug_simd_bench_tests.step);
    const run_gate_release_safe_protocol_tests = b.addRunArtifact(release_safe_protocol_tests);
    run_gate_release_safe_protocol_tests.step.dependOn(&run_gate_release_safe_tests.step);
    const run_gate_release_safe_parix_transport_tests = b.addRunArtifact(release_safe_parix_transport_tests);
    run_gate_release_safe_parix_transport_tests.step.dependOn(&run_gate_release_safe_protocol_tests.step);
    const run_gate_release_safe_parix_bootstrap_tests = b.addRunArtifact(release_safe_parix_bootstrap_tests);
    run_gate_release_safe_parix_bootstrap_tests.step.dependOn(&run_gate_release_safe_parix_transport_tests.step);
    const run_gate_release_safe_parix_runtime_tests = b.addRunArtifact(release_safe_parix_runtime_tests);
    run_gate_release_safe_parix_runtime_tests.step.dependOn(&run_gate_release_safe_parix_bootstrap_tests.step);
    const run_gate_release_safe_parix_funding_saga_tests = b.addRunArtifact(release_safe_parix_funding_saga_tests);
    run_gate_release_safe_parix_funding_saga_tests.step.dependOn(&run_gate_release_safe_parix_runtime_tests.step);
    const run_gate_release_safe_parix_funding_store_tests = b.addRunArtifact(release_safe_parix_funding_store_tests);
    run_gate_release_safe_parix_funding_store_tests.step.dependOn(&run_gate_release_safe_parix_funding_saga_tests.step);
    const run_gate_release_safe_parix_funding_runtime_tests = b.addRunArtifact(release_safe_parix_funding_runtime_tests);
    run_gate_release_safe_parix_funding_runtime_tests.step.dependOn(&run_gate_release_safe_parix_funding_store_tests.step);
    const run_gate_release_safe_server_tests = b.addRunArtifact(release_safe_server_tests);
    run_gate_release_safe_server_tests.step.dependOn(&run_gate_release_safe_parix_funding_runtime_tests.step);
    const run_gate_release_safe_client_tests = b.addRunArtifact(release_safe_client_tests);
    run_gate_release_safe_client_tests.step.dependOn(&run_gate_release_safe_server_tests.step);
    const run_gate_release_safe_browser_tests = b.addRunArtifact(release_safe_browser_tests);
    run_gate_release_safe_browser_tests.step.dependOn(&run_gate_release_safe_client_tests.step);
    const run_gate_release_safe_simd_bench_tests = b.addRunArtifact(release_safe_simd_bench_tests);
    run_gate_release_safe_simd_bench_tests.step.dependOn(&run_gate_release_safe_browser_tests.step);
    gate_step.dependOn(&run_gate_release_safe_simd_bench_tests.step);
    // Compile, but do not execute, the fixed-ReleaseFast benchmark as part of
    // the gate. The benchmark's functional tests run above in both safety modes.
    gate_step.dependOn(&simd_bench_exe.step);
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

    const wasm_target = b.resolveTargetQuery(.{
        .cpu_arch = .wasm32,
        .os_tag = .freestanding,
    });
    const wasm_settlement_check_module = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/browser/settlement_check.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSmall,
        .single_threaded = true,
    });
    const wasm_settlement_core = b.createModule(.{
        .root_source_file = b.path("src/zig/core/root.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSmall,
        .single_threaded = true,
    });
    wasm_settlement_check_module.addImport("exchange_core", wasm_settlement_core);
    const wasm_settlement_check = b.addExecutable(.{
        .name = "exchange-core-settlement-check",
        .root_module = wasm_settlement_check_module,
    });
    wasm_settlement_check.entry = .disabled;
    const wasm_settlement_check_step = b.step(
        "wasm-settlement-check",
        "Compile the pure TigerBeetle plugin, exchange integration, and Parix adapter for browser WASM",
    );
    wasm_settlement_check_step.dependOn(&wasm_settlement_check.step);

    const wasm_exchange_kernel = b.createModule(.{
        .root_source_file = b.path("src/zig/core/exchange_kernel.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSmall,
        .single_threaded = true,
    });
    const wasm_module = b.createModule(.{
        .root_source_file = b.path("src/zig/apps/browser/wasm.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSmall,
        .single_threaded = true,
    });
    wasm_module.export_symbol_names = &.{
        "exchange_abi_version",
        "exchange_create",
        "exchange_destroy",
        "exchange_reset",
        "exchange_add_symbol",
        "exchange_add_user",
        "exchange_adjust_balance",
        "exchange_place_order",
        "exchange_move_order",
        "exchange_cancel_order",
        "exchange_reduce_order",
        "exchange_query_order_book",
        "exchange_last_event_count",
        "exchange_order_book_ptr",
        "exchange_order_book_len",
    };
    wasm_module.addImport("exchange_kernel", wasm_exchange_kernel);
    const wasm_exe = b.addExecutable(.{
        .name = "exchange-core",
        .root_module = wasm_module,
    });
    wasm_exe.entry = .disabled;
    wasm_exe.export_memory = true;

    const install_wasm = b.addInstallArtifact(wasm_exe, .{
        .dest_dir = .{ .override = .prefix },
        .dest_sub_path = "browser/exchange-core.wasm",
    });
    const install_wasm_loader = b.addInstallFile(
        b.path("src/zig/apps/browser/exchange-core.js"),
        "browser/exchange-core.js",
    );
    const install_wasm_page = b.addInstallFile(
        b.path("src/zig/apps/browser/index.html"),
        "browser/index.html",
    );
    const install_wasm_app = b.addInstallFile(
        b.path("src/zig/apps/browser/app.js"),
        "browser/app.js",
    );
    const install_wasm_scenarios = b.addInstallFile(
        b.path("src/zig/apps/browser/scenarios.js"),
        "browser/scenarios.js",
    );
    const install_wasm_scenarios_page = b.addInstallFile(
        b.path("src/zig/apps/browser/scenarios.html"),
        "browser/scenarios.html",
    );
    const install_wasm_manual_app = b.addInstallFile(
        b.path("src/zig/apps/browser/manual-app.js"),
        "browser/manual-app.js",
    );
    const install_wasm_manual_session = b.addInstallFile(
        b.path("src/zig/apps/browser/manual-session.js"),
        "browser/manual-session.js",
    );

    const wasm_step = b.step("wasm", "Build the browser exchange and stage zig-out/browser");
    wasm_step.dependOn(&install_wasm.step);
    wasm_step.dependOn(&install_wasm_loader.step);
    wasm_step.dependOn(&install_wasm_page.step);
    wasm_step.dependOn(&install_wasm_app.step);
    wasm_step.dependOn(&install_wasm_scenarios.step);
    wasm_step.dependOn(&install_wasm_scenarios_page.step);
    wasm_step.dependOn(&install_wasm_manual_app.step);
    wasm_step.dependOn(&install_wasm_manual_session.step);
    wasm_step.dependOn(&wasm_settlement_check.step);

    const run_wasm_smoke = b.addSystemCommand(&.{ "node", "src/zig/apps/browser/smoke.mjs" });
    run_wasm_smoke.addFileArg(wasm_exe.getEmittedBin());
    const wasm_smoke_step = b.step(
        "wasm-smoke",
        "Build and smoke-test the browser exchange in Node",
    );
    wasm_smoke_step.dependOn(&run_wasm_smoke.step);

    // Keep Node out of the main migration gate while still proving that the
    // browser target compiles with the exact artifact used by `wasm-smoke`.
    gate_step.dependOn(&wasm_exe.step);
    gate_step.dependOn(&wasm_settlement_check.step);
}
