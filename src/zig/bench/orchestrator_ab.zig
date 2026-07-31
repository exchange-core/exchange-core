const std = @import("std");
const builtin = @import("builtin");
const core = @import("exchange_core").core;

const common = core.common;
const api_command_mod = core.api_command;
const cmd = core.cmd;
const config_mod = core.config;
const kernel_mod = core.exchange_kernel;
const orchestrator_mod = core.exchange_orchestrator;

const publication_mode: orchestrator_mod.ExecutionMode = .routed;
const symbol_ids = [_]i32{ 240, 241, 242, 243 };
const user_ids = [_]i64{ 50_001, 50_002, 50_003, 50_004 };
const base_currency: i32 = 11;
const quote_currency: i32 = 15;
const funding_amount: i64 = 1_000_000_000_000;
const throughput_order_id_base: i64 = 1_000_000;
const warmup_order_id_base: i64 = 2_000_000_000_000;
const detached_order_id_base: i64 = 3_000_000_000_000;
const terminal_order_id_base: i64 = 4_000_000_000_000;
const java_default_ring_buffer_size: usize = 16 * 1024;

const BenchError = error{
    InvalidArguments,
    MissingArgumentValue,
    MissingAsyncResult,
    UnexpectedResultVariant,
    UnexpectedResultCode,
    UnexpectedBackend,
    UnexpectedShardCount,
    UnexpectedDrainCount,
    UnexpectedFatalState,
    UnexpectedShutdownOutcome,
    UnexpectedShutdownState,
    UnexpectedReportVariant,
    UnexpectedUserStatus,
    UnexpectedUserPositions,
    InvalidOrderSequence,
    SemanticFingerprintMismatch,
    MemoryProbeLeak,
    MemoryProbeNotReleased,
    CapacityThresholdNotMet,
};

const PipelineMode = enum {
    immediate,
    persistent_grouped,

    fn configValue(self: PipelineMode) config_mod.ShardedPipelineMode {
        return switch (self) {
            .immediate => .immediate,
            .persistent_grouped => .persistent_grouped,
        };
    }

    fn label(self: PipelineMode) []const u8 {
        return @tagName(self);
    }
};

const ModeSelection = enum {
    both,
    immediate,
    persistent_grouped,

    fn includes(self: ModeSelection, mode: PipelineMode) bool {
        return switch (self) {
            .both => true,
            .immediate => mode == .immediate,
            .persistent_grouped => mode == .persistent_grouped,
        };
    }
};

const Topology = enum {
    one_by_one,
    four_by_two,

    fn matchingShards(self: Topology) usize {
        return switch (self) {
            .one_by_one => 1,
            .four_by_two => 4,
        };
    }

    fn riskShards(self: Topology) usize {
        return switch (self) {
            .one_by_one => 1,
            .four_by_two => 2,
        };
    }

    fn label(self: Topology) []const u8 {
        return switch (self) {
            .one_by_one => "1x1",
            .four_by_two => "4x2",
        };
    }
};

const TopologySelection = enum {
    both,
    one_by_one,
    four_by_two,

    fn includes(self: TopologySelection, topology: Topology) bool {
        return switch (self) {
            .both => true,
            .one_by_one => topology == .one_by_one,
            .four_by_two => topology == .four_by_two,
        };
    }
};

fn selectedScenarioCount(options: Options) usize {
    const topology_count: usize = switch (options.topology) {
        .both => 2,
        .one_by_one, .four_by_two => 1,
    };
    const mode_count: usize = switch (options.mode) {
        .both => 2,
        .immediate, .persistent_grouped => 1,
    };
    return topology_count * mode_count;
}

const Options = struct {
    topology: TopologySelection = .both,
    mode: ModeSelection = .both,
    trials: usize = 4,
    throughput_commands: usize = 100_000,
    detached_commands: usize = 100_000,
    latency_samples: usize = 1_000,
    warmup_commands: usize = 256,
    window: usize = 256,
    memory_probe: bool = true,
    memory_probe_commands: usize = 512,
    enforce_capacity_thresholds: bool = true,

    fn smoke(self: *Options) void {
        self.trials = 1;
        self.throughput_commands = 32;
        self.detached_commands = 32;
        self.latency_samples = 8;
        self.warmup_commands = 8;
        self.window = 8;
        self.memory_probe_commands = 16;
        self.enforce_capacity_thresholds = false;
    }

    fn validate(self: Options) BenchError!void {
        if (self.trials == 0 or
            self.throughput_commands == 0 or
            (self.throughput_commands & 1) != 0 or
            self.throughput_commands > 1_000_000_000 or
            self.detached_commands == 0 or
            (self.detached_commands & 1) != 0 or
            self.detached_commands > 1_000_000_000 or
            self.latency_samples == 0 or
            (self.warmup_commands & 1) != 0 or
            self.warmup_commands > 1_000_000_000 or
            self.window == 0 or
            self.window > java_default_ring_buffer_size / 2 or
            (self.memory_probe and self.memory_probe_commands == 0) or
            (self.memory_probe_commands & 1) != 0)
        {
            return error.InvalidArguments;
        }
    }
};

const ParseOutcome = union(enum) {
    help,
    options: Options,
};

const WorkerExpectation = struct {
    driver: usize,
    matching: usize,
    risk: usize,

    fn total(self: WorkerExpectation) usize {
        return self.driver + self.matching + self.risk;
    }
};

const TimingResult = struct {
    trial: usize,
    run_order: usize,
    topology: Topology,
    mode: PipelineMode,
    backend: orchestrator_mod.BackendKind,
    matching_shards: usize,
    risk_shards: usize,
    workers: WorkerExpectation,
    init_ns: u64,
    startup_ns: u64,
    terminal_drain_p50_ns: u64,
    terminal_drain_p95_ns: u64,
    terminal_drain_p99_ns: u64,
    terminal_drain_max_ns: u64,
    total_currency_balance_report_p50_ns: u64,
    total_currency_balance_report_p95_ns: u64,
    total_currency_balance_report_p99_ns: u64,
    total_currency_balance_report_max_ns: u64,
    throughput_commands: usize,
    throughput_elapsed_ns: u64,
    throughput_commands_per_second: f64,
    detached_commands: usize,
    detached_elapsed_ns: u64,
    detached_commands_per_second: f64,
    shutdown_ns: u64,
    fingerprint: u64,
};

const MemoryEvidence = struct {
    topology: Topology,
    mode: PipelineMode,
    backend: orchestrator_mod.BackendKind,
    workers: WorkerExpectation,
    probe_commands: usize,
    live_requested_after_init: usize,
    live_requested_after_startup: usize,
    live_requested_after_setup: usize,
    live_requested_after_settlement: usize,
    live_requested_after_shutdown: usize,
    live_requested_after_deinit: usize,
};

const CapacityThresholds = struct {
    minimum_throughput: f64,
    minimum_detached_throughput: f64,
    maximum_terminal_p50_ns: u64,
    maximum_report_p50_ns: u64,
};

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.smp_allocator;
    const parsed = parseOptions(init.minimal.args, allocator) catch |err| {
        try printArgumentError(init.io, err);
        return err;
    };

    var stdout_buffer: [16 * 1024]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;

    switch (parsed) {
        .help => {
            try writeUsage(out);
            try out.flush();
            return;
        },
        .options => |options| {
            try printHeader(out, options);
            var expected_fingerprint: ?u64 = null;
            var fingerprint_comparisons: usize = 0;
            var capacity_rows_checked: usize = 0;
            const topologies = [_]Topology{ .one_by_one, .four_by_two };
            const even_order = [_]PipelineMode{ .immediate, .persistent_grouped };
            const odd_order = [_]PipelineMode{ .persistent_grouped, .immediate };

            for (topologies) |topology| {
                if (!options.topology.includes(topology)) continue;
                for (0..options.trials) |trial| {
                    const mode_order = if ((trial & 1) == 0) even_order else odd_order;
                    var run_order: usize = 0;
                    for (mode_order) |mode| {
                        if (!options.mode.includes(mode)) continue;
                        const result = try runTimingTrial(
                            init.io,
                            allocator,
                            options,
                            topology,
                            mode,
                            trial,
                            run_order,
                        );
                        try printTimingResult(out, result);
                        if (options.enforce_capacity_thresholds and
                            builtin.mode == .ReleaseFast and
                            mode == .persistent_grouped)
                        {
                            try gateCapacity(result);
                            capacity_rows_checked += 1;
                        }
                        try gateFingerprint(
                            &expected_fingerprint,
                            &fingerprint_comparisons,
                            result.fingerprint,
                        );
                        run_order += 1;
                    }
                }
            }

            if (options.memory_probe) {
                try out.writeAll("\nmemory_evidence_csv\n");
                try out.writeAll(
                    "topology,config_mode,backend,expected_owned_workers,probe_commands," ++
                        "live_requested_after_init,live_requested_after_startup," ++
                        "live_requested_after_setup,live_requested_after_settlement," ++
                        "live_requested_after_shutdown,live_requested_after_deinit\n",
                );
                for (topologies) |topology| {
                    if (!options.topology.includes(topology)) continue;
                    for (even_order) |mode| {
                        if (!options.mode.includes(mode)) continue;
                        const evidence = try runMemoryProbe(
                            init.io,
                            options,
                            topology,
                            mode,
                        );
                        try printMemoryEvidence(out, evidence);
                    }
                }
            }

            const semantic_gate = if (selectedScenarioCount(options) > 1)
                "PASS"
            else
                "NOT_APPLICABLE";
            const mode_equivalence_gate = if (options.mode == .both)
                "PASS"
            else
                "NOT_APPLICABLE";
            const topology_equivalence_gate = if (options.topology == .both)
                "PASS"
            else
                "NOT_APPLICABLE";
            const capacity_gate = if (!options.enforce_capacity_thresholds or
                builtin.mode != .ReleaseFast or
                !options.mode.includes(.persistent_grouped))
                "NOT_APPLICABLE"
            else
                "PASS";
            try out.print(
                "\nsemantic_gate={s} canonical_fingerprint={x:0>16} " ++
                    "fingerprint_comparisons={d} mode_equivalence_gate={s} " ++
                    "topology_equivalence_gate={s} capacity_gate={s} " ++
                    "capacity_rows_checked={d}\n",
                .{
                    semantic_gate,
                    expected_fingerprint.?,
                    fingerprint_comparisons,
                    mode_equivalence_gate,
                    topology_equivalence_gate,
                    capacity_gate,
                    capacity_rows_checked,
                },
            );
            try out.flush();
        },
    }
}

fn parseOptions(
    process_args: std.process.Args,
    allocator: std.mem.Allocator,
) !ParseOutcome {
    var args = try std.process.Args.Iterator.initAllocator(process_args, allocator);
    defer args.deinit();
    _ = args.next();

    var options: Options = .{};
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            return .help;
        } else if (std.mem.eql(u8, arg, "--smoke")) {
            options.smoke();
        } else if (std.mem.eql(u8, arg, "--no-memory-probe")) {
            options.memory_probe = false;
        } else if (std.mem.eql(u8, arg, "--topology")) {
            options.topology = try parseTopology(args.next() orelse
                return error.MissingArgumentValue);
        } else if (std.mem.eql(u8, arg, "--mode")) {
            options.mode = try parseMode(args.next() orelse
                return error.MissingArgumentValue);
        } else if (std.mem.eql(u8, arg, "--trials")) {
            options.trials = try parseUsize(args.next() orelse
                return error.MissingArgumentValue);
        } else if (std.mem.eql(u8, arg, "--commands")) {
            options.throughput_commands = try parseUsize(args.next() orelse
                return error.MissingArgumentValue);
        } else if (std.mem.eql(u8, arg, "--detached-commands")) {
            options.detached_commands = try parseUsize(args.next() orelse
                return error.MissingArgumentValue);
        } else if (std.mem.eql(u8, arg, "--latency-samples")) {
            options.latency_samples = try parseUsize(args.next() orelse
                return error.MissingArgumentValue);
        } else if (std.mem.eql(u8, arg, "--warmup")) {
            options.warmup_commands = try parseUsize(args.next() orelse
                return error.MissingArgumentValue);
        } else if (std.mem.eql(u8, arg, "--window")) {
            options.window = try parseUsize(args.next() orelse
                return error.MissingArgumentValue);
        } else if (std.mem.eql(u8, arg, "--memory-probe-commands")) {
            options.memory_probe_commands = try parseUsize(args.next() orelse
                return error.MissingArgumentValue);
        } else {
            return error.InvalidArguments;
        }
    }
    try options.validate();
    return .{ .options = options };
}

fn parseTopology(raw: []const u8) BenchError!TopologySelection {
    if (std.mem.eql(u8, raw, "both")) return .both;
    if (std.mem.eql(u8, raw, "1x1")) return .one_by_one;
    if (std.mem.eql(u8, raw, "4x2")) return .four_by_two;
    return error.InvalidArguments;
}

fn parseMode(raw: []const u8) BenchError!ModeSelection {
    if (std.mem.eql(u8, raw, "both")) return .both;
    if (std.mem.eql(u8, raw, "immediate")) return .immediate;
    if (std.mem.eql(u8, raw, "persistent_grouped")) return .persistent_grouped;
    return error.InvalidArguments;
}

fn parseUsize(raw: []const u8) BenchError!usize {
    return std.fmt.parseInt(usize, raw, 10) catch error.InvalidArguments;
}

fn printArgumentError(io: std.Io, err: anyerror) !void {
    var buffer: [4096]u8 = undefined;
    var writer = std.Io.File.stderr().writer(io, &buffer);
    try writer.interface.print("argument error: {s}\n\n", .{@errorName(err)});
    try writeUsage(&writer.interface);
    try writer.interface.flush();
}

fn writeUsage(writer: anytype) !void {
    try writer.writeAll(
        \\Usage:
        \\  zig build -Doptimize=ReleaseFast bench-orchestrator-ab -- [options]
        \\
        \\Options:
        \\  --topology both|1x1|4x2          Topology selection (default: both)
        \\  --mode both|immediate|persistent_grouped
        \\                                      Pipeline mode selection (default: both)
        \\  --trials N                         Trials per topology (default: 4)
        \\  --commands N                       Even poll-window trade count (default: 100000)
        \\  --detached-commands N              Even detached trade count (default: 100000)
        \\  --latency-samples N                Terminal-drain/report samples (default: 1000)
        \\  --warmup N                         Even trade warmup count; 0 disables (default: 256)
        \\  --window N                         Max in-flight poll commands (default: 256)
        \\  --memory-probe-commands N          Even probe command count (default: 512)
        \\  --no-memory-probe                  Skip separate requested-byte probe
        \\  --smoke                            Tiny 1-trial configuration
        \\  -h, --help                         Show this help
        \\
        \\All runs start from Java defaults, override only topology and the
        \\explicit sharded pipeline mode, and use routed publication.
        \\
    );
}

fn printHeader(writer: anytype, options: Options) !void {
    const java_defaults = config_mod.PerformanceConfig.javaDefault();
    try writer.print(
        "exchange-core orchestrator A/B benchmark\n" ++
            "output_schema_version=2 build_mode={s} timing_allocator=smp_allocator " ++
            "memory_allocator=separate_debug_allocator_requested_bytes\n" ++
            "config_base=JavaExchangeConfig.javaDefault publication_mode=routed " ++
            "trials={d} commands={d} detached_commands={d} latency_samples={d} " ++
            "warmup={d} window={d}\n" ++
            "java_default_ring_buffer={d} java_default_msgs_in_group={d} " ++
            "java_default_max_group_duration_ns={d} java_default_wait_strategy={s} " ++
            "java_default_orderbook={s}\n" ++
            "terminal_drain_latency_command=nop " ++
            "terminal_drain_boundary=async_submit_drain_take_includes_grouped_r2 " ++
            "total_currency_balance_report_latency_boundary=separate_post_drain_query " ++
            "warmup_workload=paired_gtc_trades poll_window_workload=paired_gtc_trades " ++
            "detached_workload=paired_gtc_trades_then_terminal_drain " ++
            "memory_scope=allocator_live_requested_heap_excludes_rss_and_thread_stacks\n" ++
            "worker_count_basis=configured_expected_owned_threads_including_driver " ++
            "public_aggregate_worker_count=unavailable\n" ++
            "capacity_gate_reference=fixed_ReleaseFast_local_floor " ++
            "capacity_gate_1x1=throughput>=300000_detached>=450000_" ++
            "terminal_p50<=100000ns_report_p50<=25000ns " ++
            "capacity_gate_4x2=throughput>=150000_detached>=180000_" ++
            "terminal_p50<=150000ns_report_p50<=25000ns\n\n" ++
            "timing_results_csv\n" ++
            "trial,run_order,topology,config_mode,backend,matching_shards,risk_shards," ++
            "expected_driver_workers,expected_matching_workers,expected_risk_workers," ++
            "expected_owned_workers,init_ns,startup_ns,terminal_drain_p50_ns," ++
            "terminal_drain_p95_ns,terminal_drain_p99_ns," ++
            "terminal_drain_max_ns,total_currency_balance_report_p50_ns," ++
            "total_currency_balance_report_p95_ns," ++
            "total_currency_balance_report_p99_ns," ++
            "total_currency_balance_report_max_ns," ++
            "throughput_commands,throughput_settled_elapsed_ns," ++
            "throughput_settled_commands_per_second,shutdown_ns,fingerprint," ++
            "detached_commands,detached_terminal_elapsed_ns," ++
            "detached_terminal_commands_per_second\n",
        .{
            @tagName(builtin.mode),
            options.trials,
            options.throughput_commands,
            options.detached_commands,
            options.latency_samples,
            options.warmup_commands,
            options.window,
            java_defaults.ring_buffer_size,
            java_defaults.msgs_in_group_limit,
            java_defaults.max_group_duration_ns,
            @tagName(java_defaults.wait_strategy),
            @tagName(java_defaults.orderbook_kind),
        },
    );
}

fn gateCapacity(result: TimingResult) BenchError!void {
    std.debug.assert(result.mode == .persistent_grouped);
    const thresholds: CapacityThresholds = switch (result.topology) {
        .one_by_one => .{
            .minimum_throughput = 300_000.0,
            .minimum_detached_throughput = 450_000.0,
            .maximum_terminal_p50_ns = 100_000,
            .maximum_report_p50_ns = 25_000,
        },
        .four_by_two => .{
            .minimum_throughput = 150_000.0,
            .minimum_detached_throughput = 180_000.0,
            .maximum_terminal_p50_ns = 150_000,
            .maximum_report_p50_ns = 25_000,
        },
    };
    if (result.throughput_commands_per_second < thresholds.minimum_throughput or
        result.detached_commands_per_second < thresholds.minimum_detached_throughput or
        result.terminal_drain_p50_ns > thresholds.maximum_terminal_p50_ns or
        result.total_currency_balance_report_p50_ns > thresholds.maximum_report_p50_ns)
    {
        return error.CapacityThresholdNotMet;
    }
}

fn runTimingTrial(
    io: std.Io,
    allocator: std.mem.Allocator,
    options: Options,
    topology: Topology,
    mode: PipelineMode,
    trial: usize,
    run_order: usize,
) !TimingResult {
    const java_config = makeJavaConfig(topology, mode);

    const init_started = std.Io.Clock.awake.now(io);
    var orchestrator = try orchestrator_mod.ExchangeOrchestrator.initFromJavaConfig(
        allocator,
        java_config,
    );
    defer orchestrator.deinit();
    const init_ns = elapsedNs(init_started, std.Io.Clock.awake.now(io));

    const backend = orchestrator.backendKind();
    try validateTopology(&orchestrator, topology, mode);
    const workers = expectedWorkers(topology, mode, backend);

    const startup_started = std.Io.Clock.awake.now(io);
    try orchestrator.startup();
    const startup_ns = elapsedNs(startup_started, std.Io.Clock.awake.now(io));

    try setupBenchmark(allocator, &orchestrator);
    if (options.warmup_commands != 0) {
        _ = try runTradeWindowed(
            io,
            allocator,
            &orchestrator,
            options.warmup_commands,
            options.window,
            warmup_order_id_base,
        );
    }
    try terminalSettlementBarrier(allocator, &orchestrator);

    const terminal_drain_latencies = try allocator.alloc(u64, options.latency_samples);
    defer allocator.free(terminal_drain_latencies);
    const total_currency_balance_report_latencies = try allocator.alloc(
        u64,
        options.latency_samples,
    );
    defer allocator.free(total_currency_balance_report_latencies);
    for (
        terminal_drain_latencies,
        total_currency_balance_report_latencies,
    ) |*terminal_drain_sample, *total_currency_balance_report_sample| {
        const started = std.Io.Clock.awake.now(io);
        const handle = try orchestrator.submitApiCommandAsync(publication_mode, .nop);
        const drained = try orchestrator.drainPending();
        if (drained != 1) return error.UnexpectedDrainCount;
        try takeSuccessfulResult(allocator, &orchestrator, handle);
        terminal_drain_sample.* = elapsedNs(started, std.Io.Clock.awake.now(io));

        const report_started = std.Io.Clock.awake.now(io);
        try terminalSettlementBarrier(allocator, &orchestrator);
        total_currency_balance_report_sample.* = elapsedNs(
            report_started,
            std.Io.Clock.awake.now(io),
        );
    }
    std.mem.sort(u64, terminal_drain_latencies, {}, std.sort.asc(u64));
    std.mem.sort(
        u64,
        total_currency_balance_report_latencies,
        {},
        std.sort.asc(u64),
    );

    const throughput = try runTradeWindowed(
        io,
        allocator,
        &orchestrator,
        options.throughput_commands,
        options.window,
        throughput_order_id_base,
    );
    const detached = try runTradeDetached(
        io,
        &orchestrator,
        options.detached_commands,
        detached_order_id_base,
    );
    try installTerminalOrders(allocator, &orchestrator);
    try terminalSettlementBarrier(allocator, &orchestrator);
    if (orchestrator.fatalInfo() != null) return error.UnexpectedFatalState;
    const fingerprint = try semanticFingerprint(allocator, &orchestrator);

    const shutdown_started = std.Io.Clock.awake.now(io);
    const shutdown_outcome = try orchestrator.shutdownTimeout(.none);
    const shutdown_ns = elapsedNs(shutdown_started, std.Io.Clock.awake.now(io));
    if (shutdown_outcome != .complete) return error.UnexpectedShutdownOutcome;
    if (orchestrator.shutdownState() != .complete) return error.UnexpectedShutdownState;

    return .{
        .trial = trial,
        .run_order = run_order,
        .topology = topology,
        .mode = mode,
        .backend = backend,
        .matching_shards = orchestrator.matchingShardCount(),
        .risk_shards = orchestrator.riskShardCount(),
        .workers = workers,
        .init_ns = init_ns,
        .startup_ns = startup_ns,
        .terminal_drain_p50_ns = percentile(terminal_drain_latencies, 50),
        .terminal_drain_p95_ns = percentile(terminal_drain_latencies, 95),
        .terminal_drain_p99_ns = percentile(terminal_drain_latencies, 99),
        .terminal_drain_max_ns = terminal_drain_latencies[terminal_drain_latencies.len - 1],
        .total_currency_balance_report_p50_ns = percentile(
            total_currency_balance_report_latencies,
            50,
        ),
        .total_currency_balance_report_p95_ns = percentile(
            total_currency_balance_report_latencies,
            95,
        ),
        .total_currency_balance_report_p99_ns = percentile(
            total_currency_balance_report_latencies,
            99,
        ),
        .total_currency_balance_report_max_ns = total_currency_balance_report_latencies[
            total_currency_balance_report_latencies.len - 1
        ],
        .throughput_commands = options.throughput_commands,
        .throughput_elapsed_ns = throughput.elapsed_ns,
        .throughput_commands_per_second = throughput.commands_per_second,
        .detached_commands = options.detached_commands,
        .detached_elapsed_ns = detached.elapsed_ns,
        .detached_commands_per_second = detached.commands_per_second,
        .shutdown_ns = shutdown_ns,
        .fingerprint = fingerprint,
    };
}

const ThroughputResult = struct {
    elapsed_ns: u64,
    commands_per_second: f64,
};

fn runTradeWindowed(
    io: std.Io,
    allocator: std.mem.Allocator,
    orchestrator: *orchestrator_mod.ExchangeOrchestrator,
    command_count: usize,
    window: usize,
    order_id_base: i64,
) !ThroughputResult {
    std.debug.assert(command_count > 0 and (command_count & 1) == 0);
    const handles = try allocator.alloc(api_command_mod.ApiCommandAsyncHandle, window);
    defer allocator.free(handles);

    const started = std.Io.Clock.awake.now(io);
    var published: usize = 0;
    while (published < command_count) {
        const batch_len = @min(window, command_count - published);
        for (0..batch_len) |offset| {
            handles[offset] = try orchestrator.submitApiCommandAsync(
                publication_mode,
                try makeTradeCommand(published + offset, order_id_base),
            );
        }
        const drained = try orchestrator.drainPending();
        if (drained != batch_len) return error.UnexpectedDrainCount;
        for (handles[0..batch_len]) |handle| {
            try takeSuccessfulResult(allocator, orchestrator, handle);
        }
        published += batch_len;
    }
    try terminalSettlementBarrier(allocator, orchestrator);
    const elapsed_ns = elapsedNs(started, std.Io.Clock.awake.now(io));
    const seconds = @max(
        @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0,
        0.000_000_001,
    );
    return .{
        .elapsed_ns = elapsed_ns,
        .commands_per_second = @as(f64, @floatFromInt(command_count)) / seconds,
    };
}

fn runTradeDetached(
    io: std.Io,
    orchestrator: *orchestrator_mod.ExchangeOrchestrator,
    command_count: usize,
    order_id_base: i64,
) !ThroughputResult {
    std.debug.assert(command_count > 0 and (command_count & 1) == 0);

    const started = std.Io.Clock.awake.now(io);
    for (0..command_count) |index| {
        try orchestrator.submitApiCommand(
            publication_mode,
            try makeTradeCommand(index, order_id_base),
        );
    }
    // Detached commands do not contribute to the returned async count, but
    // drainPending still waits for the captured accepted prefix through R2.
    const drained = try orchestrator.drainPending();
    if (drained != 0) return error.UnexpectedDrainCount;
    const elapsed_ns = elapsedNs(started, std.Io.Clock.awake.now(io));
    const seconds = @max(
        @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0,
        0.000_000_001,
    );
    return .{
        .elapsed_ns = elapsed_ns,
        .commands_per_second = @as(f64, @floatFromInt(command_count)) / seconds,
    };
}

fn terminalSettlementBarrier(
    allocator: std.mem.Allocator,
    orchestrator: *orchestrator_mod.ExchangeOrchestrator,
) !void {
    var response = try orchestrator.queryReport(.total_currency_balance);
    defer response.deinit(allocator);
    switch (response) {
        .total_currency_balance => {},
        else => return error.UnexpectedReportVariant,
    }
}

fn makeJavaConfig(topology: Topology, mode: PipelineMode) config_mod.JavaExchangeConfig {
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.matching_engines_num = topology.matchingShards();
    java_config.performance_cfg.risk_engines_num = topology.riskShards();
    java_config.sharded_pipeline_mode = mode.configValue();
    return java_config;
}

fn validateTopology(
    orchestrator: *const orchestrator_mod.ExchangeOrchestrator,
    topology: Topology,
    mode: PipelineMode,
) BenchError!void {
    const expected_backend: orchestrator_mod.BackendKind = if (topology == .one_by_one and
        mode == .immediate)
        .single
    else
        .sharded;
    if (orchestrator.backendKind() != expected_backend) return error.UnexpectedBackend;
    if (orchestrator.matchingShardCount() != topology.matchingShards() or
        orchestrator.riskShardCount() != topology.riskShards())
    {
        return error.UnexpectedShardCount;
    }
}

fn expectedWorkers(
    topology: Topology,
    mode: PipelineMode,
    backend: orchestrator_mod.BackendKind,
) WorkerExpectation {
    if (backend == .single or mode == .immediate) {
        return .{ .driver = 1, .matching = 0, .risk = 0 };
    }
    return .{
        .driver = 1,
        .matching = topology.matchingShards(),
        .risk = topology.riskShards(),
    };
}

fn setupBenchmark(
    allocator: std.mem.Allocator,
    orchestrator: *orchestrator_mod.ExchangeOrchestrator,
) !void {
    var specs: [symbol_ids.len]common.CoreSymbolSpecification = undefined;
    for (symbol_ids, 0..) |symbol, index| {
        specs[index] = try makeExchangeSpec(symbol);
    }
    var symbols_result = try orchestrator.submitApiCommandSync(publication_mode, .{
        .binary_add_symbols = .{
            .transfer_id = 70_001,
            .timestamp = 1,
            .symbols = &specs,
        },
    });
    try consumeSuccessfulResult(allocator, &symbols_result);

    for (user_ids) |uid| {
        var add_result = try orchestrator.submitApiCommandSync(
            publication_mode,
            .{ .add_user = uid },
        );
        try consumeSuccessfulResult(allocator, &add_result);

        inline for ([_]i32{ base_currency, quote_currency }, 0..) |currency, index| {
            var balance_result = try orchestrator.submitApiCommandSync(publication_mode, .{
                .adjust_user_balance = .{
                    .uid = uid,
                    .transaction_id = uid * 10 + @as(i64, @intCast(index + 1)),
                    .currency = currency,
                    .amount = funding_amount,
                    .adjustment_type = .adjustment,
                    .timestamp = 2,
                },
            });
            try consumeSuccessfulResult(allocator, &balance_result);
        }
    }
}

fn makeExchangeSpec(symbol: i32) !common.CoreSymbolSpecification {
    return common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol,
        .symbol_type = .currency_exchange_pair,
        .base_currency = base_currency,
        .quote_currency = quote_currency,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
    });
}

fn makeTradeCommand(
    index: usize,
    order_id_base: i64,
) BenchError!api_command_mod.ApiCommand {
    const pair_index = index / 2;
    const pair_i64 = std.math.cast(i64, pair_index) orelse
        return error.InvalidOrderSequence;
    const symbol = symbol_ids[pair_index % symbol_ids.len];
    const price = 100 + @as(i64, @intCast(pair_index % 8));
    const size = 1 + @as(i64, @intCast(pair_index % 3));
    const bid = (index & 1) == 0;
    return .{ .place_order = .{
        .order_id = order_id_base + @as(i64, @intCast(index)),
        .uid = if (bid)
            user_ids[pair_index % user_ids.len]
        else
            user_ids[(pair_index + 1) % user_ids.len],
        .symbol = symbol,
        .price = price,
        .reserve_bid_price = if (bid) price else 0,
        .size = size,
        .action = if (bid) .bid else .ask,
        .order_type = .gtc,
        .user_cookie = @as(i32, @intCast(pair_index % std.math.maxInt(i32))),
        .timestamp = pair_i64 + 10,
    } };
}

fn installTerminalOrders(
    allocator: std.mem.Allocator,
    orchestrator: *orchestrator_mod.ExchangeOrchestrator,
) !void {
    for (symbol_ids, 0..) |symbol, index| {
        const bid_order_id = terminal_order_id_base + @as(i64, @intCast(index * 2));
        const ask_order_id = bid_order_id + 1;
        var bid = try orchestrator.submitApiCommandSync(publication_mode, .{
            .place_order = .{
                .order_id = bid_order_id,
                .uid = user_ids[index % user_ids.len],
                .symbol = symbol,
                .price = 90,
                .reserve_bid_price = 90,
                .size = 2,
                .action = .bid,
                .order_type = .gtc,
                .user_cookie = 0,
                .timestamp = 9_000_000 + @as(i64, @intCast(index)),
            },
        });
        try consumeSuccessfulResult(allocator, &bid);

        var ask = try orchestrator.submitApiCommandSync(publication_mode, .{
            .place_order = .{
                .order_id = ask_order_id,
                .uid = user_ids[(index + 1) % user_ids.len],
                .symbol = symbol,
                .price = 110,
                .reserve_bid_price = 0,
                .size = 3,
                .action = .ask,
                .order_type = .gtc,
                .user_cookie = 0,
                .timestamp = 9_000_100 + @as(i64, @intCast(index)),
            },
        });
        try consumeSuccessfulResult(allocator, &ask);
    }
}

fn consumeSuccessfulResult(
    allocator: std.mem.Allocator,
    result: *api_command_mod.ApiCommandResult,
) !void {
    defer result.deinit(allocator);
    switch (result.*) {
        .response => |response| {
            if (response.result_code != .success) return error.UnexpectedResultCode;
        },
        .persist_state => return error.UnexpectedResultVariant,
    }
}

fn takeSuccessfulResult(
    allocator: std.mem.Allocator,
    orchestrator: *orchestrator_mod.ExchangeOrchestrator,
    handle: api_command_mod.ApiCommandAsyncHandle,
) !void {
    var result = orchestrator.takeApiCommandAsyncResult(handle) orelse
        return error.MissingAsyncResult;
    try consumeSuccessfulResult(allocator, &result);
}

const CanonicalHash = struct {
    value: u64 = 0xcbf29ce484222325,

    fn bytes(self: *CanonicalHash, slice: []const u8) void {
        for (slice) |byte| {
            self.value ^= byte;
            self.value *%= 0x100000001b3;
        }
    }

    fn u64Value(self: *CanonicalHash, value: u64) void {
        var remaining = value;
        for (0..8) |_| {
            self.bytes(&.{@truncate(remaining)});
            remaining >>= 8;
        }
    }

    fn i64Value(self: *CanonicalHash, value: i64) void {
        self.u64Value(@bitCast(value));
    }

    fn i32Value(self: *CanonicalHash, value: i32) void {
        self.u64Value(@as(u32, @bitCast(value)));
    }

    fn usizeValue(self: *CanonicalHash, value: usize) void {
        self.u64Value(value);
    }
};

fn semanticFingerprint(
    allocator: std.mem.Allocator,
    orchestrator: *orchestrator_mod.ExchangeOrchestrator,
) !u64 {
    var hash: CanonicalHash = .{};
    hash.bytes("exchange-core-orchestrator-ab-semantic-v1");

    for (symbol_ids) |symbol| {
        var response = try orchestrator.queryReport(.{
            .order_book = .{ .symbol = symbol, .depth = 32 },
        });
        defer response.deinit(allocator);
        hash.bytes("order_book");
        hash.i32Value(symbol);
        switch (response) {
            .order_book => |book| {
                hash.usizeValue(book.asks.items.len);
                for (book.asks.items) |level| {
                    hash.i64Value(level.price);
                    hash.i64Value(level.volume);
                    hash.u64Value(level.orders);
                }
                hash.usizeValue(book.bids.items.len);
                for (book.bids.items) |level| {
                    hash.i64Value(level.price);
                    hash.i64Value(level.volume);
                    hash.u64Value(level.orders);
                }
            },
            else => return error.UnexpectedReportVariant,
        }
    }

    for (user_ids) |uid| {
        var response = try orchestrator.queryReport(.{ .single_user = uid });
        defer response.deinit(allocator);
        hash.bytes("single_user");
        hash.i64Value(uid);
        switch (response) {
            .single_user => |user| {
                if (user.query_execution_status != .ok) return error.UnexpectedUserStatus;
                if (user.positions.count() != 0) return error.UnexpectedUserPositions;
                if (user.user_status) |status| {
                    hash.u64Value(1);
                    hash.i64Value(@intFromEnum(status));
                } else {
                    hash.u64Value(0);
                }
                try hashBalanceMap(&hash, allocator, "accounts", &user.accounts);
                hash.usizeValue(user.orders.items.len);
                for (user.orders.items) |order| {
                    hash.i32Value(order.symbol);
                    hash.i64Value(order.order_id);
                    hash.i64Value(order.price);
                    hash.i64Value(order.size);
                    hash.i64Value(order.filled);
                    hash.i64Value(order.reserve_bid_price);
                    hash.i64Value(@intFromEnum(order.action));
                    hash.i64Value(order.uid);
                    hash.i64Value(order.timestamp);
                }
            },
            else => return error.UnexpectedReportVariant,
        }
    }

    var totals_response = try orchestrator.queryReport(.total_currency_balance);
    defer totals_response.deinit(allocator);
    switch (totals_response) {
        .total_currency_balance => |*totals| {
            try hashBalanceMap(&hash, allocator, "account_balances", &totals.account_balances);
            try hashBalanceMap(&hash, allocator, "orders_balances", &totals.orders_balances);
            try hashBalanceMap(&hash, allocator, "fees", &totals.fees);
            try hashBalanceMap(&hash, allocator, "adjustments", &totals.adjustments);
            try hashBalanceMap(&hash, allocator, "suspends", &totals.suspends);
            try hashBalanceMap(&hash, allocator, "open_interest_long", &totals.open_interest_long);
            try hashBalanceMap(&hash, allocator, "open_interest_short", &totals.open_interest_short);
        },
        else => return error.UnexpectedReportVariant,
    }
    return hash.value;
}

fn hashBalanceMap(
    hash: *CanonicalHash,
    allocator: std.mem.Allocator,
    label: []const u8,
    map: *const std.AutoHashMapUnmanaged(i32, i64),
) !void {
    var keys: std.ArrayListUnmanaged(i32) = .empty;
    defer keys.deinit(allocator);
    var iterator = map.keyIterator();
    while (iterator.next()) |key| try keys.append(allocator, key.*);
    std.mem.sort(i32, keys.items, {}, std.sort.asc(i32));

    hash.bytes(label);
    hash.usizeValue(keys.items.len);
    for (keys.items) |key| {
        hash.i32Value(key);
        hash.i64Value(map.get(key).?);
    }
}

fn gateFingerprint(
    expected: *?u64,
    comparison_count: *usize,
    actual: u64,
) BenchError!void {
    if (expected.*) |fingerprint| {
        comparison_count.* += 1;
        if (fingerprint != actual) return error.SemanticFingerprintMismatch;
    } else {
        expected.* = actual;
    }
}

fn runMemoryProbe(
    io: std.Io,
    options: Options,
    topology: Topology,
    mode: PipelineMode,
) !MemoryEvidence {
    var debug_allocator: std.heap.DebugAllocator(.{
        .enable_memory_limit = true,
    }) = .init;
    var debug_allocator_live = true;
    errdefer {
        if (debug_allocator_live) _ = debug_allocator.deinit();
    }
    const allocator = debug_allocator.allocator();

    const java_config = makeJavaConfig(topology, mode);
    var orchestrator = try orchestrator_mod.ExchangeOrchestrator.initFromJavaConfig(
        allocator,
        java_config,
    );
    var orchestrator_live = true;
    defer if (orchestrator_live) orchestrator.deinit();
    const backend = orchestrator.backendKind();
    try validateTopology(&orchestrator, topology, mode);
    const workers = expectedWorkers(topology, mode, backend);
    const after_init = debug_allocator.total_requested_bytes;

    try orchestrator.startup();
    try terminalSettlementBarrier(allocator, &orchestrator);
    const after_startup = debug_allocator.total_requested_bytes;
    try setupBenchmark(allocator, &orchestrator);
    try terminalSettlementBarrier(allocator, &orchestrator);
    const after_setup = debug_allocator.total_requested_bytes;
    const probe_commands = @min(
        options.memory_probe_commands,
        options.throughput_commands,
    );
    _ = try runTradeWindowed(
        io,
        allocator,
        &orchestrator,
        probe_commands,
        options.window,
        throughput_order_id_base,
    );
    try installTerminalOrders(allocator, &orchestrator);
    try terminalSettlementBarrier(allocator, &orchestrator);
    if (orchestrator.fatalInfo() != null) return error.UnexpectedFatalState;
    const after_settlement = debug_allocator.total_requested_bytes;
    const shutdown_outcome = try orchestrator.shutdownTimeout(.none);
    if (shutdown_outcome != .complete) return error.UnexpectedShutdownOutcome;
    const after_shutdown = debug_allocator.total_requested_bytes;

    orchestrator.deinit();
    orchestrator_live = false;
    const after_deinit = debug_allocator.total_requested_bytes;
    if (after_deinit != 0) return error.MemoryProbeNotReleased;
    const debug_status = debug_allocator.deinit();
    debug_allocator_live = false;
    if (debug_status != .ok) return error.MemoryProbeLeak;

    return .{
        .topology = topology,
        .mode = mode,
        .backend = backend,
        .workers = workers,
        .probe_commands = probe_commands,
        .live_requested_after_init = after_init,
        .live_requested_after_startup = after_startup,
        .live_requested_after_setup = after_setup,
        .live_requested_after_settlement = after_settlement,
        .live_requested_after_shutdown = after_shutdown,
        .live_requested_after_deinit = after_deinit,
    };
}

fn percentile(sorted: []const u64, percent: usize) u64 {
    const rank = (sorted.len * percent + 99) / 100;
    return sorted[@max(rank, 1) - 1];
}

fn elapsedNs(started: std.Io.Timestamp, finished: std.Io.Timestamp) u64 {
    const raw = started.durationTo(finished).nanoseconds;
    if (raw <= 0) return 0;
    return std.math.cast(u64, raw) orelse std.math.maxInt(u64);
}

fn printTimingResult(writer: anytype, result: TimingResult) !void {
    try writer.print(
        "{d},{d},{s},{s},{s}," ++
            "{d},{d},{d},{d},{d},{d}," ++
            "{d},{d}," ++
            "{d},{d},{d},{d}," ++
            "{d},{d},{d},{d}," ++
            "{d},{d},{d:.2}," ++
            "{d},{x:0>16},{d},{d},{d:.2}\n",
        .{
            result.trial,
            result.run_order,
            result.topology.label(),
            result.mode.label(),
            @tagName(result.backend),
            result.matching_shards,
            result.risk_shards,
            result.workers.driver,
            result.workers.matching,
            result.workers.risk,
            result.workers.total(),
            result.init_ns,
            result.startup_ns,
            result.terminal_drain_p50_ns,
            result.terminal_drain_p95_ns,
            result.terminal_drain_p99_ns,
            result.terminal_drain_max_ns,
            result.total_currency_balance_report_p50_ns,
            result.total_currency_balance_report_p95_ns,
            result.total_currency_balance_report_p99_ns,
            result.total_currency_balance_report_max_ns,
            result.throughput_commands,
            result.throughput_elapsed_ns,
            result.throughput_commands_per_second,
            result.shutdown_ns,
            result.fingerprint,
            result.detached_commands,
            result.detached_elapsed_ns,
            result.detached_commands_per_second,
        },
    );
}

fn printMemoryEvidence(writer: anytype, evidence: MemoryEvidence) !void {
    try writer.print(
        "{s},{s},{s},{d},{d},{d},{d},{d},{d},{d},{d}\n",
        .{
            evidence.topology.label(),
            evidence.mode.label(),
            @tagName(evidence.backend),
            evidence.workers.total(),
            evidence.probe_commands,
            evidence.live_requested_after_init,
            evidence.live_requested_after_startup,
            evidence.live_requested_after_setup,
            evidence.live_requested_after_settlement,
            evidence.live_requested_after_shutdown,
            evidence.live_requested_after_deinit,
        },
    );
}
