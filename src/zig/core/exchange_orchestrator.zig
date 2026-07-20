const std = @import("std");
const cmd = @import("cmd.zig");
const common = @import("common.zig");
const api_command_mod = @import("api_command.zig");
const config_mod = @import("config.zig");
const exchange_core_mod = @import("exchange_core.zig");
const exchange_api_sharded_mod = @import("exchange_api_sharded.zig");
const persistence_paths = @import("persistence_paths.zig");
const journal_mod = @import("journal.zig");
const kernel_mod = @import("exchange_kernel.zig");
const runtime_mod = @import("exchange_runtime.zig");
const simple_events_mod = @import("simple_events_processor.zig");

pub const Config = exchange_core_mod.Config;
pub const JavaConfig = config_mod.JavaExchangeConfig;
pub const ApiCommand = api_command_mod.ApiCommand;
pub const ApiCommandResult = api_command_mod.ApiCommandResult;
pub const ApiCommandAsyncHandle = api_command_mod.ApiCommandAsyncHandle;
pub const ExecutionMode = exchange_api_sharded_mod.ExecutionMode;
pub const PersistenceModuleType = exchange_core_mod.PersistenceModuleType;
pub const ResultSink = runtime_mod.ResultSink;
pub const RuntimeDrainOutcome = runtime_mod.DrainOutcome;
pub const CommandResponseCallback = exchange_api_sharded_mod.CommandResponseCallback;
pub const SimpleEventsHandler = simple_events_mod.EventsHandler;
pub const BootstrapOutcome = exchange_core_mod.BootstrapOutcome;

pub const BackendKind = enum {
    single,
    sharded,
};

pub const ModeValidationError = error{
    SplitModeRequiresShardedBackend,
};

pub const InitFromJavaConfigError = exchange_core_mod.InitFromJavaConfigError || exchange_api_sharded_mod.InitFromJavaConfigError;
pub const LifecycleError = exchange_core_mod.LifecycleError || exchange_api_sharded_mod.LifecycleError;
pub const ExecuteError = exchange_core_mod.ExecuteError || exchange_api_sharded_mod.ExecuteError || ModeValidationError;
pub const HelperError = exchange_core_mod.HelperError || exchange_api_sharded_mod.HelperError || ModeValidationError;
pub const SubmitApiCommandError = exchange_core_mod.SubmitApiCommandError || exchange_api_sharded_mod.SubmitApiCommandError || ModeValidationError;
pub const SubmitApiCommandAsyncError = exchange_core_mod.SubmitApiCommandAsyncError || exchange_api_sharded_mod.SubmitApiCommandAsyncError || ModeValidationError;
pub const QueryReportError = exchange_core_mod.ReportError || exchange_api_sharded_mod.QueryReportError;
pub const DrainPendingError = exchange_core_mod.ExecuteError || exchange_api_sharded_mod.DrainPendingError;
pub const PersistStateError = exchange_core_mod.ExecuteError || exchange_api_sharded_mod.PersistStateError || ModeValidationError;
pub const RuntimeDrainError = exchange_core_mod.RuntimeDrainError || exchange_api_sharded_mod.RuntimeDrainError;
pub const EnqueueRuntimeError = exchange_core_mod.LifecycleError || exchange_api_sharded_mod.EnqueueRuntimeError;
pub const StartupError = exchange_core_mod.StartupError || exchange_api_sharded_mod.StartupError;
pub const StartupFromConfiguredPersistenceError = exchange_core_mod.StartupFromConfiguredPersistenceError || exchange_api_sharded_mod.StartupFromConfiguredPersistenceError;
pub const StartupFromDiscoveredConfiguredPersistenceError = exchange_core_mod.StartupFromDiscoveredConfiguredPersistenceError || exchange_api_sharded_mod.StartupFromDiscoveredConfiguredPersistenceError;
pub const StartupFromDiscoveredConfiguredPersistenceStrictError = exchange_core_mod.StartupFromDiscoveredConfiguredPersistenceStrictError || exchange_api_sharded_mod.StartupFromDiscoveredConfiguredPersistenceStrictError;
pub const PersistStateWithMainLogError = exchange_core_mod.PersistStateWithMainLogError || exchange_api_sharded_mod.PersistStateWithMainLogError;
pub const SubmitCommandWithCallbackError = exchange_api_sharded_mod.SubmitCommandWithCallbackError || ModeValidationError;
pub const ProcessRegisteredCallbackError = exchange_api_sharded_mod.ProcessRegisteredCallbackError;

pub const ExchangeOrchestrator = struct {
    allocator: std.mem.Allocator,
    backend: union(BackendKind) {
        single: exchange_core_mod.ExchangeCore,
        sharded: exchange_api_sharded_mod.ExchangeApiSharded,
    },

    pub fn initFromJavaConfig(
        allocator: std.mem.Allocator,
        java_config: JavaConfig,
    ) InitFromJavaConfigError!ExchangeOrchestrator {
        if (java_config.performance_cfg.matching_engines_num > 1) {
            return .{
                .allocator = allocator,
                .backend = .{
                    .sharded = try exchange_api_sharded_mod.ExchangeApiSharded.initFromJavaConfig(
                        allocator,
                        java_config,
                    ),
                },
            };
        }
        return .{
            .allocator = allocator,
            .backend = .{
                .single = try exchange_core_mod.ExchangeCore.initFromJavaConfig(
                    allocator,
                    java_config,
                ),
            },
        };
    }

    pub fn deinit(self: *ExchangeOrchestrator) void {
        switch (self.backend) {
            .single => |*single| single.deinit(),
            .sharded => |*sharded| sharded.deinit(),
        }
        self.* = undefined;
    }

    pub fn backendKind(self: *const ExchangeOrchestrator) BackendKind {
        return switch (self.backend) {
            .single => .single,
            .sharded => .sharded,
        };
    }

    pub fn shardCount(self: *const ExchangeOrchestrator) usize {
        return switch (self.backend) {
            .single => 1,
            .sharded => |sharded| sharded.shardCount(),
        };
    }

    pub fn configRef(self: *const ExchangeOrchestrator) *const Config {
        return switch (self.backend) {
            .single => |*single| single.configRef(),
            .sharded => |*sharded| sharded.configRef(),
        };
    }

    pub fn isStarted(self: *const ExchangeOrchestrator) bool {
        return switch (self.backend) {
            .single => |*single| single.isStarted(),
            .sharded => |*sharded| sharded.isStarted(),
        };
    }

    pub fn isStopped(self: *const ExchangeOrchestrator) bool {
        return switch (self.backend) {
            .single => |*single| single.isStopped(),
            .sharded => |*sharded| sharded.isStopped(),
        };
    }

    pub fn startup(self: *ExchangeOrchestrator) LifecycleError!void {
        return switch (self.backend) {
            .single => |*single| single.startup(),
            .sharded => |*sharded| sharded.startup(),
        };
    }

    pub fn shutdown(self: *ExchangeOrchestrator) void {
        switch (self.backend) {
            .single => |*single| single.shutdown(),
            .sharded => |*sharded| sharded.shutdown(),
        }
    }

    pub fn startupFromPersistence(
        self: *ExchangeOrchestrator,
        dir: std.fs.Dir,
        snapshot_path: ?[]const u8,
        journal_path: ?[]const u8,
    ) StartupError!BootstrapOutcome {
        return switch (self.backend) {
            .single => |*single| single.startupFromPersistence(dir, snapshot_path, journal_path),
            .sharded => |*sharded| sharded.startupFromPersistence(dir, snapshot_path, journal_path),
        };
    }

    pub fn startupFromConfiguredPersistence(
        self: *ExchangeOrchestrator,
        dir: std.fs.Dir,
        snapshot_module_type: PersistenceModuleType,
        snapshot_instance_id: u32,
        journal_partition_id: u16,
    ) StartupFromConfiguredPersistenceError!BootstrapOutcome {
        return switch (self.backend) {
            .single => |*single| single.startupFromConfiguredPersistence(
                dir,
                snapshot_module_type,
                snapshot_instance_id,
                journal_partition_id,
            ),
            .sharded => |*sharded| sharded.startupFromConfiguredPersistence(
                dir,
                snapshot_module_type,
                snapshot_instance_id,
                journal_partition_id,
            ),
        };
    }

    pub fn startupFromDiscoveredConfiguredPersistence(
        self: *ExchangeOrchestrator,
        dir: std.fs.Dir,
        snapshot_module_type: PersistenceModuleType,
        snapshot_instance_id: u32,
        journal_partition_id: u16,
    ) StartupFromDiscoveredConfiguredPersistenceError!BootstrapOutcome {
        return switch (self.backend) {
            .single => |*single| single.startupFromDiscoveredConfiguredPersistence(
                dir,
                snapshot_module_type,
                snapshot_instance_id,
                journal_partition_id,
            ),
            .sharded => |*sharded| sharded.startupFromDiscoveredConfiguredPersistence(
                dir,
                snapshot_module_type,
                snapshot_instance_id,
                journal_partition_id,
            ),
        };
    }

    pub fn startupFromDiscoveredConfiguredPersistenceStrictMainLog(
        self: *ExchangeOrchestrator,
        dir: std.fs.Dir,
        snapshot_module_type: PersistenceModuleType,
        snapshot_instance_id: u32,
        journal_partition_id: u16,
    ) StartupFromDiscoveredConfiguredPersistenceStrictError!BootstrapOutcome {
        return switch (self.backend) {
            .single => |*single| single.startupFromDiscoveredConfiguredPersistenceStrictMainLog(
                dir,
                snapshot_module_type,
                snapshot_instance_id,
                journal_partition_id,
            ),
            .sharded => |*sharded| sharded.startupFromDiscoveredConfiguredPersistenceStrictMainLog(
                dir,
                snapshot_module_type,
                snapshot_instance_id,
                journal_partition_id,
            ),
        };
    }

    pub fn persistStateWithDefaultMainLog(
        self: *ExchangeOrchestrator,
        dir: std.fs.Dir,
        snapshot_id: i64,
        timestamp_ns: i64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) PersistStateWithMainLogError!cmd.CommandResultCode {
        return switch (self.backend) {
            .single => |*single| single.persistStateWithDefaultMainLog(
                dir,
                snapshot_id,
                timestamp_ns,
                module_type,
                instance_id,
            ),
            .sharded => |*sharded| sharded.persistStateWithDefaultMainLog(
                dir,
                snapshot_id,
                timestamp_ns,
                module_type,
                instance_id,
            ),
        };
    }

    pub fn setResultSink(self: *ExchangeOrchestrator, sink: ResultSink) void {
        switch (self.backend) {
            .single => |*single| single.setResultSink(sink),
            .sharded => |*sharded| sharded.setResultSink(sink),
        }
    }

    pub fn setSimpleEventsHandler(
        self: *ExchangeOrchestrator,
        handler: SimpleEventsHandler,
    ) void {
        switch (self.backend) {
            .single => |*single| single.setSimpleEventsHandler(handler),
            .sharded => |*sharded| sharded.setSimpleEventsHandler(handler),
        }
    }

    pub fn clearSimpleEventsHandler(self: *ExchangeOrchestrator) void {
        switch (self.backend) {
            .single => |*single| single.clearSimpleEventsHandler(),
            .sharded => |*sharded| sharded.clearSimpleEventsHandler(),
        }
    }

    pub fn submitApiCommand(
        self: *ExchangeOrchestrator,
        mode: ExecutionMode,
        command: ApiCommand,
    ) SubmitApiCommandError!void {
        try self.ensureModeSupported(mode);
        return switch (self.backend) {
            .single => |*single| {
                var result = try single.submitApiCommand(command);
                result.deinit(self.allocator);
            },
            .sharded => |*sharded| sharded.submitApiCommand(mode, command),
        };
    }

    pub fn submitApiCommandAsync(
        self: *ExchangeOrchestrator,
        mode: ExecutionMode,
        command: ApiCommand,
    ) SubmitApiCommandAsyncError!ApiCommandAsyncHandle {
        try self.ensureModeSupported(mode);
        return switch (self.backend) {
            .single => |*single| single.submitApiCommandAsync(command),
            .sharded => |*sharded| sharded.submitApiCommandAsync(mode, command),
        };
    }

    pub fn takeApiCommandAsyncResult(
        self: *ExchangeOrchestrator,
        handle: ApiCommandAsyncHandle,
    ) ?ApiCommandResult {
        return switch (self.backend) {
            .single => |*single| single.takeApiCommandAsyncResult(handle),
            .sharded => |*sharded| sharded.takeApiCommandAsyncResult(handle),
        };
    }

    pub fn drainPending(self: *ExchangeOrchestrator) DrainPendingError!usize {
        return switch (self.backend) {
            .single => |*single| single.drainAsyncPending(),
            .sharded => |*sharded| sharded.drainPending(),
        };
    }

    pub fn addUser(
        self: *ExchangeOrchestrator,
        mode: ExecutionMode,
        uid: i64,
    ) ExecuteError!kernel_mod.CommandResponse {
        try self.ensureModeSupported(mode);
        return switch (self.backend) {
            .single => |*single| single.addUser(uid),
            .sharded => |*sharded| sharded.addUser(mode, uid),
        };
    }

    pub fn adjustUserBalance(
        self: *ExchangeOrchestrator,
        mode: ExecutionMode,
        uid: i64,
        transaction_id: i64,
        currency: i32,
        amount: i64,
        adjustment_type: common.BalanceAdjustmentType,
    ) ExecuteError!kernel_mod.CommandResponse {
        try self.ensureModeSupported(mode);
        return switch (self.backend) {
            .single => |*single| single.adjustUserBalance(
                uid,
                transaction_id,
                currency,
                amount,
                adjustment_type,
            ),
            .sharded => |*sharded| sharded.adjustUserBalance(
                mode,
                uid,
                transaction_id,
                currency,
                amount,
                adjustment_type,
            ),
        };
    }

    pub fn placeOrder(
        self: *ExchangeOrchestrator,
        mode: ExecutionMode,
        order_id: i64,
        uid: i64,
        symbol: i32,
        price: i64,
        reserve_bid_price: i64,
        size: i64,
        action: common.OrderAction,
        order_type: common.OrderType,
        user_cookie: i32,
    ) ExecuteError!kernel_mod.CommandResponse {
        try self.ensureModeSupported(mode);
        return switch (self.backend) {
            .single => |*single| single.placeOrder(
                order_id,
                uid,
                symbol,
                price,
                reserve_bid_price,
                size,
                action,
                order_type,
                user_cookie,
            ),
            .sharded => |*sharded| sharded.placeOrder(
                mode,
                order_id,
                uid,
                symbol,
                price,
                reserve_bid_price,
                size,
                action,
                order_type,
                user_cookie,
            ),
        };
    }

    pub fn moveOrder(
        self: *ExchangeOrchestrator,
        mode: ExecutionMode,
        order_id: i64,
        uid: i64,
        symbol: i32,
        new_price: i64,
    ) ExecuteError!kernel_mod.CommandResponse {
        try self.ensureModeSupported(mode);
        return switch (self.backend) {
            .single => |*single| single.moveOrder(
                order_id,
                uid,
                symbol,
                new_price,
            ),
            .sharded => |*sharded| sharded.moveOrder(
                mode,
                order_id,
                uid,
                symbol,
                new_price,
            ),
        };
    }

    pub fn cancelOrder(
        self: *ExchangeOrchestrator,
        mode: ExecutionMode,
        order_id: i64,
        uid: i64,
        symbol: i32,
    ) ExecuteError!kernel_mod.CommandResponse {
        try self.ensureModeSupported(mode);
        return switch (self.backend) {
            .single => |*single| single.cancelOrder(
                order_id,
                uid,
                symbol,
            ),
            .sharded => |*sharded| sharded.cancelOrder(
                mode,
                order_id,
                uid,
                symbol,
            ),
        };
    }

    pub fn reduceOrder(
        self: *ExchangeOrchestrator,
        mode: ExecutionMode,
        order_id: i64,
        uid: i64,
        symbol: i32,
        reduce_size: i64,
    ) ExecuteError!kernel_mod.CommandResponse {
        try self.ensureModeSupported(mode);
        return switch (self.backend) {
            .single => |*single| single.reduceOrder(
                order_id,
                uid,
                symbol,
                reduce_size,
            ),
            .sharded => |*sharded| sharded.reduceOrder(
                mode,
                order_id,
                uid,
                symbol,
                reduce_size,
            ),
        };
    }

    pub fn requestOrderBook(
        self: *ExchangeOrchestrator,
        mode: ExecutionMode,
        symbol: i32,
        depth: usize,
    ) (ExecuteError || HelperError)!kernel_mod.CommandResponse {
        try self.ensureModeSupported(mode);
        return switch (self.backend) {
            .single => |*single| single.requestOrderBook(symbol, depth),
            .sharded => |*sharded| sharded.requestOrderBook(mode, symbol, depth),
        };
    }

    pub fn persistState(
        self: *ExchangeOrchestrator,
        mode: ExecutionMode,
        snapshot_id: i64,
        timestamp_ns: i64,
    ) PersistStateError!cmd.CommandResultCode {
        try self.ensureModeSupported(mode);
        return switch (self.backend) {
            .single => |*single| single.persistState(snapshot_id, timestamp_ns),
            .sharded => |*sharded| sharded.persistState(mode, snapshot_id, timestamp_ns),
        };
    }

    pub fn queryReport(
        self: *ExchangeOrchestrator,
        query: kernel_mod.ReportQuery,
    ) QueryReportError!kernel_mod.ReportResponse {
        return switch (self.backend) {
            .single => |*single| single.queryReport(query),
            .sharded => |*sharded| sharded.queryReport(query),
        };
    }

    pub fn enqueueRuntime(
        self: *ExchangeOrchestrator,
        command: cmd.OrderCommand,
    ) EnqueueRuntimeError!void {
        return switch (self.backend) {
            .single => |*single| single.enqueueRuntime(command),
            .sharded => |*sharded| sharded.enqueueRuntime(command),
        };
    }

    pub fn drainRuntime(self: *ExchangeOrchestrator) RuntimeDrainError!RuntimeDrainOutcome {
        return switch (self.backend) {
            .single => |*single| single.drainRuntime(),
            .sharded => |*sharded| sharded.drainRuntime(),
        };
    }

    pub fn runtimePendingCount(self: *const ExchangeOrchestrator) usize {
        return switch (self.backend) {
            .single => |*single| single.runtimePendingCount(),
            .sharded => |*sharded| sharded.runtimePendingCount(),
        };
    }

    pub fn createUserWithCallback(
        self: *ExchangeOrchestrator,
        mode: ExecutionMode,
        user_id: i64,
        callback: CommandResponseCallback,
    ) SubmitCommandWithCallbackError!void {
        try self.ensureModeSupported(mode);
        return switch (self.backend) {
            .single => return error.SplitModeRequiresShardedBackend,
            .sharded => |*sharded| sharded.createUserWithCallback(
                mode,
                user_id,
                callback,
            ),
        };
    }

    pub fn processPendingCallbacks(self: *ExchangeOrchestrator) ProcessRegisteredCallbackError!usize {
        return switch (self.backend) {
            .single => 0,
            .sharded => |*sharded| sharded.processPendingCallbacks(),
        };
    }

    fn ensureModeSupported(
        self: *const ExchangeOrchestrator,
        mode: ExecutionMode,
    ) ModeValidationError!void {
        if (mode == .split and self.backendKind() == .single) {
            return error.SplitModeRequiresShardedBackend;
        }
    }
};

test "exchange orchestrator selects single backend for java default profile" {
    const allocator = std.testing.allocator;
    var orchestrator = try ExchangeOrchestrator.initFromJavaConfig(
        allocator,
        config_mod.JavaExchangeConfig.javaDefault(),
    );
    defer orchestrator.deinit();

    try std.testing.expectEqual(BackendKind.single, orchestrator.backendKind());
    try std.testing.expectEqual(@as(usize, 1), orchestrator.shardCount());
}

test "exchange orchestrator selects sharded backend for throughput profile" {
    const allocator = std.testing.allocator;
    var orchestrator = try ExchangeOrchestrator.initFromJavaConfig(
        allocator,
        .{
            .performance_cfg = config_mod.PerformanceConfig.throughputPreset(),
            .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
        },
    );
    defer orchestrator.deinit();

    try std.testing.expectEqual(BackendKind.sharded, orchestrator.backendKind());
    try std.testing.expectEqual(@as(usize, 4), orchestrator.shardCount());
}

test "exchange orchestrator routes addUser through selected backend" {
    const allocator = std.testing.allocator;

    var single = try ExchangeOrchestrator.initFromJavaConfig(
        allocator,
        config_mod.JavaExchangeConfig.javaDefault(),
    );
    defer single.deinit();
    try single.startup();
    var single_add = try single.addUser(.routed, 980_001);
    defer single_add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, single_add.result_code);

    var sharded = try ExchangeOrchestrator.initFromJavaConfig(
        allocator,
        .{
            .performance_cfg = config_mod.PerformanceConfig.throughputPreset(),
            .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
        },
    );
    defer sharded.deinit();
    try sharded.startup();
    var sharded_add = try sharded.addUser(.routed, 980_002);
    defer sharded_add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, sharded_add.result_code);
}

test "exchange orchestrator rejects split mode on single backend" {
    const allocator = std.testing.allocator;
    var orchestrator = try ExchangeOrchestrator.initFromJavaConfig(
        allocator,
        config_mod.JavaExchangeConfig.javaDefault(),
    );
    defer orchestrator.deinit();

    try std.testing.expectError(
        error.SplitModeRequiresShardedBackend,
        orchestrator.addUser(.split, 980_101),
    );
}

test "exchange orchestrator async submit and drain works on sharded backend" {
    const allocator = std.testing.allocator;
    var orchestrator = try ExchangeOrchestrator.initFromJavaConfig(
        allocator,
        .{
            .performance_cfg = config_mod.PerformanceConfig.throughputPreset(),
            .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
        },
    );
    defer orchestrator.deinit();

    const handle = try orchestrator.submitApiCommandAsync(
        .routed,
        .{ .add_user = 980_201 },
    );
    try std.testing.expectEqual(@as(usize, 1), try orchestrator.drainPending());

    var result = orchestrator.takeApiCommandAsyncResult(handle) orelse return error.MissingAsyncResult;
    defer result.deinit(allocator);
    switch (result) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }
}

test "exchange orchestrator single backend rejects callback wrappers and keeps callback processing as no-op" {
    const allocator = std.testing.allocator;

    const Noop = struct {
        fn onResponse(_: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {}
    };

    var orchestrator = try ExchangeOrchestrator.initFromJavaConfig(
        allocator,
        config_mod.JavaExchangeConfig.javaDefault(),
    );
    defer orchestrator.deinit();

    const callback: CommandResponseCallback = .{
        .context = null,
        .callback = Noop.onResponse,
    };
    try std.testing.expectError(
        error.SplitModeRequiresShardedBackend,
        orchestrator.createUserWithCallback(.routed, 980_301, callback),
    );
    try std.testing.expectEqual(@as(usize, 0), try orchestrator.processPendingCallbacks());
}

test "exchange orchestrator seeded sharded callback campaign is deterministic and seed-sensitive" {
    const allocator = std.testing.allocator;
    const rounds: usize = 192;
    const user_pool: usize = 48;

    const Scenario = struct {
        name: []const u8,
        seed: u64,
    };

    const Outcome = struct {
        digest: u64 = 0,
        registered: usize = 0,
        drain_calls: usize = 0,
        drained_total: usize = 0,
        callback_process_calls: usize = 0,
        callback_processed_total: usize = 0,
        success_results: usize = 0,
        duplicate_results: usize = 0,
    };

    const CallbackCollector = struct {
        digest: u64,
        processed: usize = 0,
        success_results: usize = 0,
        duplicate_results: usize = 0,

        fn mixDigest(self: *@This(), token: u64) void {
            self.digest = (self.digest ^ token) *% 0x9E3779B185EBCA87;
            self.digest +%= 0x632BE59BD9B4E019;
        }

        fn onResponse(context: ?*anyopaque, response: *const kernel_mod.CommandResponse) void {
            const collector: *@This() = @ptrCast(@alignCast(context orelse return));
            collector.processed += 1;
            collector.mixDigest(response.sequence);
            const token: u64 = switch (response.result_code) {
                .success => blk: {
                    collector.success_results += 1;
                    break :blk 1;
                },
                .user_mgmt_user_already_exists => blk: {
                    collector.duplicate_results += 1;
                    break :blk 2;
                },
                else => 63,
            };
            collector.mixDigest(token);
        }
    };

    const Harness = struct {
        fn runScenario(alloc: std.mem.Allocator, scenario: Scenario) !Outcome {
            var orchestrator = try ExchangeOrchestrator.initFromJavaConfig(
                alloc,
                .{
                    .performance_cfg = config_mod.PerformanceConfig.throughputPreset(),
                    .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
                },
            );
            defer orchestrator.deinit();
            try orchestrator.startup();

            var collector = CallbackCollector{
                .digest = 0xA11CBACCA11CBACC ^ scenario.seed,
            };
            const callback: CommandResponseCallback = .{
                .context = @ptrCast(&collector),
                .callback = CallbackCollector.onResponse,
            };

            var outcome: Outcome = .{ .digest = collector.digest };
            var prng = std.Random.DefaultPrng.init(scenario.seed);
            const random = prng.random();
            const base_uid: i64 = 2_100_000 + @as(i64, @intCast(scenario.seed % 30_000));

            for (0..rounds) |round_idx| {
                const uid_offset = random.uintLessThan(usize, user_pool);
                const uid = base_uid + @as(i64, @intCast(uid_offset));
                try orchestrator.createUserWithCallback(.routed, uid, callback);
                outcome.registered += 1;
                collector.mixDigest(@as(u64, @intCast(uid_offset)));

                const should_drain = round_idx % 5 == 0 or random.uintLessThan(u32, 100) < 25;
                if (should_drain) {
                    const drained = try orchestrator.drainPending();
                    outcome.drain_calls += 1;
                    outcome.drained_total = std.math.add(usize, outcome.drained_total, drained) catch
                        return error.DrainCountOverflow;
                    collector.mixDigest(drained);

                    const processed = try orchestrator.processPendingCallbacks();
                    outcome.callback_process_calls += 1;
                    outcome.callback_processed_total = std.math.add(usize, outcome.callback_processed_total, processed) catch
                        return error.CallbackCountOverflow;
                    collector.mixDigest(processed);
                }
            }

            var guard: usize = 0;
            while (collector.processed < outcome.registered and guard < rounds + 32) : (guard += 1) {
                const drained = try orchestrator.drainPending();
                outcome.drain_calls += 1;
                outcome.drained_total = std.math.add(usize, outcome.drained_total, drained) catch
                    return error.DrainCountOverflow;
                collector.mixDigest(drained);

                const processed = try orchestrator.processPendingCallbacks();
                outcome.callback_process_calls += 1;
                outcome.callback_processed_total = std.math.add(usize, outcome.callback_processed_total, processed) catch
                    return error.CallbackCountOverflow;
                collector.mixDigest(processed);
                if (drained == 0 and processed == 0 and collector.processed < outcome.registered) {
                    return error.CallbackProgressStalled;
                }
            }

            if (collector.processed != outcome.registered) {
                return error.CallbackCompletionMismatch;
            }
            try std.testing.expectEqual(@as(usize, 0), try orchestrator.processPendingCallbacks());

            outcome.success_results = collector.success_results;
            outcome.duplicate_results = collector.duplicate_results;
            outcome.digest = collector.digest;
            return outcome;
        }
    };

    const scenarios = [_]Scenario{
        .{
            .name = "sharded-callback-seed-a",
            .seed = 0xACE9101,
        },
        .{
            .name = "sharded-callback-seed-b",
            .seed = 0xACE9102,
        },
    };

    var digest_a: ?u64 = null;
    var digest_b: ?u64 = null;
    for (scenarios, 0..) |scenario, idx| {
        _ = scenario.name;
        const first = try Harness.runScenario(allocator, scenario);
        const second = try Harness.runScenario(allocator, scenario);
        try std.testing.expectEqualDeep(first, second);

        try std.testing.expect(first.registered > 0);
        try std.testing.expectEqual(rounds, first.registered);
        try std.testing.expectEqual(first.registered, first.drained_total);
        try std.testing.expect(first.callback_processed_total > 0);
        try std.testing.expect(first.success_results > 0);
        try std.testing.expect(first.duplicate_results > 0);

        if (idx == 0) {
            digest_a = first.digest;
        } else {
            digest_b = first.digest;
        }
    }

    try std.testing.expect((digest_a orelse return error.MissingSeedCoverage) != (digest_b orelse return error.MissingSeedCoverage));
}

test "exchange orchestrator sharded strict startup propagates malformed main-log errors deterministically" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const seed: u64 = 0x0AC357A11;
    var exchange_id_buf: [32]u8 = undefined;
    const exchange_id = try std.fmt.bufPrint(&exchange_id_buf, "ECORCH{x}", .{seed});

    var source = try exchange_api_sharded_mod.ExchangeApiSharded.initFromJavaConfig(
        allocator,
        .{
            .performance_cfg = config_mod.PerformanceConfig.throughputPreset(),
            .init_state_cfg = config_mod.InitialStateConfig.cleanStart(exchange_id),
            .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
        },
    );
    defer source.deinit();
    try source.startup();

    const symbol: i32 = 24_200 + @as(i32, @intCast(seed % 200)) * 4;
    const spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
    });
    try source.addSymbolAll(spec);
    try source.captureSnapshotToDefaultPath(
        tmp.dir,
        901,
        .matching_engine_router,
        0,
        &.{symbol},
        8,
    );

    const main_log_path = try source.defaultPersistenceMainLogPath(allocator);
    defer allocator.free(main_log_path);
    var main_log_file = try tmp.dir.createFile(main_log_path, .{ .truncate = false });
    defer main_log_file.close();
    try main_log_file.seekFromEnd(0);
    try main_log_file.writeAll("seeded-malformed-main-log-entry\n");

    var target = try ExchangeOrchestrator.initFromJavaConfig(
        allocator,
        .{
            .performance_cfg = config_mod.PerformanceConfig.throughputPreset(),
            .init_state_cfg = config_mod.InitialStateConfig.fromSnapshotOnly(exchange_id, 1, 0),
            .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
        },
    );
    defer target.deinit();

    try std.testing.expectEqual(BackendKind.sharded, target.backendKind());
    try std.testing.expectError(
        error.MalformedMainLogEntry,
        target.startupFromDiscoveredConfiguredPersistenceStrictMainLog(
            tmp.dir,
            .matching_engine_router,
            0,
            0,
        ),
    );
}

test "exchange orchestrator single backend discovered startup restores persisted state deterministically" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const seed: u64 = 0x0A11CE51;
    var exchange_id_buf: [32]u8 = undefined;
    const exchange_id = try std.fmt.bufPrint(&exchange_id_buf, "ECORCHS{x}", .{seed});
    const existing_uid: i64 = 981_001 + @as(i64, @intCast(seed % 137));

    var source = try ExchangeOrchestrator.initFromJavaConfig(
        allocator,
        .{
            .init_state_cfg = config_mod.InitialStateConfig.cleanStart(exchange_id),
            .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
        },
    );
    defer source.deinit();

    try std.testing.expectEqual(BackendKind.single, source.backendKind());
    try source.startup();
    var add = try source.addUser(.routed, existing_uid);
    defer add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add.result_code);

    var symbols = [_]i32{};
    switch (source.backend) {
        .single => |*single| try single.captureSnapshotToDefaultPath(
            tmp.dir,
            901,
            .matching_engine_router,
            0,
            symbols[0..],
            8,
        ),
        .sharded => unreachable,
    }

    var target = try ExchangeOrchestrator.initFromJavaConfig(
        allocator,
        .{
            .init_state_cfg = config_mod.InitialStateConfig.fromSnapshotOnly(exchange_id, 1, 0),
            .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
        },
    );
    defer target.deinit();

    const outcome = try target.startupFromDiscoveredConfiguredPersistence(
        tmp.dir,
        .matching_engine_router,
        0,
        0,
    );
    try std.testing.expect(outcome.snapshot_requested);
    try std.testing.expect(outcome.snapshot_found);
    try std.testing.expectEqual(@as(usize, 0), outcome.journal_replayed_commands);

    var duplicate_add = try target.addUser(.routed, existing_uid);
    defer duplicate_add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, duplicate_add.result_code);
}

test "exchange orchestrator sharded persistStateWithDefaultMainLog writes default-path artifacts deterministically" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const seed: u64 = 0x5A4DDE05;
    var exchange_id_buf: [32]u8 = undefined;
    const exchange_id = try std.fmt.bufPrint(&exchange_id_buf, "ECORCH{x}", .{seed});

    var orchestrator = try ExchangeOrchestrator.initFromJavaConfig(
        allocator,
        .{
            .performance_cfg = config_mod.PerformanceConfig.throughputPreset(),
            .init_state_cfg = config_mod.InitialStateConfig.cleanStart(exchange_id),
            .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
        },
    );
    defer orchestrator.deinit();

    try std.testing.expectEqual(BackendKind.sharded, orchestrator.backendKind());
    try orchestrator.startup();

    var add = try orchestrator.addUser(.routed, 991_100);
    defer add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add.result_code);

    const snapshot_id: i64 = 7001;
    const timestamp_ns: i64 = 901_002_003;
    const persisted = try orchestrator.persistStateWithDefaultMainLog(
        tmp.dir,
        snapshot_id,
        timestamp_ns,
        .matching_engine_router,
        0,
    );
    try std.testing.expectEqual(cmd.CommandResultCode.success, persisted);

    const cfg = orchestrator.configRef();
    const main_log_path = try persistence_paths.mainLogPath(
        allocator,
        cfg.serialization.storage_folder,
        exchange_id,
    );
    defer allocator.free(main_log_path);
    var file = try tmp.dir.openFile(main_log_path, .{});
    defer file.close();
    const contents = try file.readToEndAlloc(allocator, 4096);
    defer allocator.free(contents);
    try std.testing.expect(std.mem.indexOf(u8, contents, "snapshotId=7001") != null);
    try std.testing.expect(std.mem.indexOf(u8, contents, "type=ME") != null);
    try std.testing.expect(std.mem.indexOf(u8, contents, "instance=0") != null);

    var discovered = try ExchangeOrchestrator.initFromJavaConfig(
        allocator,
        .{
            .performance_cfg = config_mod.PerformanceConfig.throughputPreset(),
            .init_state_cfg = config_mod.InitialStateConfig.fromSnapshotOnly(exchange_id, 1, 0),
            .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
        },
    );
    defer discovered.deinit();
    try std.testing.expectError(
        error.SnapshotFileNotFound,
        discovered.startupFromDiscoveredConfiguredPersistence(
            tmp.dir,
            .matching_engine_router,
            0,
            0,
        ),
    );
}

test "exchange orchestrator seeded persistence perturbation simulation is deterministic and reproducible" {
    const allocator = std.testing.allocator;

    const Perturbation = enum {
        none,
        malformed_main_log,
    };

    const StartupMode = enum {
        configured,
        discovered,
        discovered_strict_main_log,
    };

    const Scenario = struct {
        name: []const u8,
        seed: u64,
        backend: BackendKind,
        startup_mode: StartupMode,
        perturbation: Perturbation,
    };

    const OutcomeKind = enum {
        success,
        malformed_main_log_entry,
    };

    const Outcome = struct {
        kind: OutcomeKind,
        snapshot_requested: bool = false,
        snapshot_found: bool = false,
        duplicate_add_result: cmd.CommandResultCode = .accepted,
    };

    const Harness = struct {
        fn javaConfigForBackend(
            backend: BackendKind,
            init_state: config_mod.InitialStateConfig,
        ) JavaConfig {
            return switch (backend) {
                .single => .{
                    .init_state_cfg = init_state,
                    .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
                },
                .sharded => .{
                    .performance_cfg = config_mod.PerformanceConfig.throughputPreset(),
                    .init_state_cfg = init_state,
                    .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
                },
            };
        }

        fn captureSnapshotToDefaultPath(
            source: *ExchangeOrchestrator,
            dir: std.fs.Dir,
            snapshot_id: u64,
            module_type: PersistenceModuleType,
        ) !void {
            var symbols = [_]i32{};
            switch (source.backend) {
                .single => |*single| try single.captureSnapshotToDefaultPath(
                    dir,
                    snapshot_id,
                    module_type,
                    0,
                    symbols[0..],
                    8,
                ),
                .sharded => |*sharded| try sharded.captureSnapshotToDefaultPath(
                    dir,
                    snapshot_id,
                    module_type,
                    0,
                    symbols[0..],
                    8,
                ),
            }
        }

        fn appendMalformedMainLog(
            alloc: std.mem.Allocator,
            source: *ExchangeOrchestrator,
            dir: std.fs.Dir,
        ) !void {
            const path = switch (source.backend) {
                .single => |*single| try single.defaultPersistenceMainLogPath(alloc),
                .sharded => |*sharded| try sharded.defaultPersistenceMainLogPath(alloc),
            };
            defer alloc.free(path);

            var file = try dir.createFile(path, .{ .truncate = false });
            defer file.close();
            try file.seekFromEnd(0);
            try file.writeAll("seeded-malformed-main-log-entry\n");
        }

        fn runScenario(alloc: std.mem.Allocator, scenario: Scenario) !Outcome {
            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();

            var exchange_id_buf: [64]u8 = undefined;
            const exchange_id = try std.fmt.bufPrint(
                &exchange_id_buf,
                "ECO_SIM_{s}_{s}_{x}",
                .{ @tagName(scenario.backend), @tagName(scenario.startup_mode), scenario.seed },
            );
            const existing_uid: i64 = 990_000 + @as(i64, @intCast(scenario.seed % 1_000));
            const module_type: PersistenceModuleType = .risk_engine;

            var source = try ExchangeOrchestrator.initFromJavaConfig(
                alloc,
                javaConfigForBackend(
                    scenario.backend,
                    config_mod.InitialStateConfig.cleanStart(exchange_id),
                ),
            );
            defer source.deinit();
            try source.startup();

            var add_source = try source.addUser(.routed, existing_uid);
            defer add_source.deinit(alloc);
            try std.testing.expectEqual(cmd.CommandResultCode.success, add_source.result_code);

            const snapshot_id: u64 = 700 + @as(u64, @intCast(scenario.seed % 50));
            try captureSnapshotToDefaultPath(&source, tmp.dir, snapshot_id, module_type);

            if (scenario.perturbation == .malformed_main_log) {
                try appendMalformedMainLog(alloc, &source, tmp.dir);
            }

            const target_init_state = switch (scenario.startup_mode) {
                .configured => config_mod.InitialStateConfig.fromSnapshotOnly(exchange_id, snapshot_id, 0),
                .discovered, .discovered_strict_main_log => config_mod.InitialStateConfig.fromSnapshotOnly(exchange_id, 1, 0),
            };
            var target = try ExchangeOrchestrator.initFromJavaConfig(
                alloc,
                javaConfigForBackend(
                    scenario.backend,
                    target_init_state,
                ),
            );
            defer target.deinit();

            const startup = switch (scenario.startup_mode) {
                .configured => target.startupFromConfiguredPersistence(
                    tmp.dir,
                    module_type,
                    0,
                    0,
                ) catch |err| {
                    std.debug.panic(
                        "seeded orchestrator scenario '{s}' seed=0x{x} backend={s} startup={s} failed with unexpected error {s}; replay with: zig build test --test-filter \"exchange orchestrator seeded persistence perturbation simulation is deterministic and reproducible\"",
                        .{
                            scenario.name,
                            scenario.seed,
                            @tagName(scenario.backend),
                            @tagName(scenario.startup_mode),
                            @errorName(err),
                        },
                    );
                },
                .discovered => target.startupFromDiscoveredConfiguredPersistence(
                    tmp.dir,
                    module_type,
                    0,
                    0,
                ) catch |err| {
                    std.debug.panic(
                        "seeded orchestrator scenario '{s}' seed=0x{x} backend={s} startup={s} failed with unexpected error {s}; replay with: zig build test --test-filter \"exchange orchestrator seeded persistence perturbation simulation is deterministic and reproducible\"",
                        .{
                            scenario.name,
                            scenario.seed,
                            @tagName(scenario.backend),
                            @tagName(scenario.startup_mode),
                            @errorName(err),
                        },
                    );
                },
                .discovered_strict_main_log => target.startupFromDiscoveredConfiguredPersistenceStrictMainLog(
                    tmp.dir,
                    module_type,
                    0,
                    0,
                ) catch |err| switch (err) {
                    error.MalformedMainLogEntry => return .{ .kind = .malformed_main_log_entry },
                    else => std.debug.panic(
                        "seeded orchestrator strict scenario '{s}' seed=0x{x} backend={s} startup={s} failed with unexpected error {s}; replay with: zig build test --test-filter \"exchange orchestrator seeded persistence perturbation simulation is deterministic and reproducible\"",
                        .{
                            scenario.name,
                            scenario.seed,
                            @tagName(scenario.backend),
                            @tagName(scenario.startup_mode),
                            @errorName(err),
                        },
                    ),
                },
            };

            var duplicate_add = try target.addUser(.routed, existing_uid);
            defer duplicate_add.deinit(alloc);
            return .{
                .kind = .success,
                .snapshot_requested = startup.snapshot_requested,
                .snapshot_found = startup.snapshot_found,
                .duplicate_add_result = duplicate_add.result_code,
            };
        }
    };

    const scenarios = [_]Scenario{
        .{
            .name = "single-configured-baseline",
            .seed = 0xACE1101,
            .backend = .single,
            .startup_mode = .configured,
            .perturbation = .none,
        },
        .{
            .name = "single-configured-malformed-main-log-best-effort",
            .seed = 0xACE1102,
            .backend = .single,
            .startup_mode = .configured,
            .perturbation = .malformed_main_log,
        },
        .{
            .name = "single-discovered-baseline",
            .seed = 0xACE1103,
            .backend = .single,
            .startup_mode = .discovered,
            .perturbation = .none,
        },
        .{
            .name = "single-discovered-malformed-main-log-best-effort",
            .seed = 0xACE1104,
            .backend = .single,
            .startup_mode = .discovered,
            .perturbation = .malformed_main_log,
        },
        .{
            .name = "single-discovered-strict-baseline",
            .seed = 0xACE1105,
            .backend = .single,
            .startup_mode = .discovered_strict_main_log,
            .perturbation = .none,
        },
        .{
            .name = "single-discovered-strict-malformed-main-log",
            .seed = 0xACE1106,
            .backend = .single,
            .startup_mode = .discovered_strict_main_log,
            .perturbation = .malformed_main_log,
        },
        .{
            .name = "sharded-configured-baseline",
            .seed = 0xACE2101,
            .backend = .sharded,
            .startup_mode = .configured,
            .perturbation = .none,
        },
        .{
            .name = "sharded-configured-malformed-main-log-best-effort",
            .seed = 0xACE2102,
            .backend = .sharded,
            .startup_mode = .configured,
            .perturbation = .malformed_main_log,
        },
        .{
            .name = "sharded-discovered-baseline",
            .seed = 0xACE2103,
            .backend = .sharded,
            .startup_mode = .discovered,
            .perturbation = .none,
        },
        .{
            .name = "sharded-discovered-malformed-main-log-best-effort",
            .seed = 0xACE2104,
            .backend = .sharded,
            .startup_mode = .discovered,
            .perturbation = .malformed_main_log,
        },
        .{
            .name = "sharded-discovered-strict-baseline",
            .seed = 0xACE2105,
            .backend = .sharded,
            .startup_mode = .discovered_strict_main_log,
            .perturbation = .none,
        },
        .{
            .name = "sharded-discovered-strict-malformed-main-log",
            .seed = 0xACE2106,
            .backend = .sharded,
            .startup_mode = .discovered_strict_main_log,
            .perturbation = .malformed_main_log,
        },
    };

    for (scenarios) |scenario| {
        const first = try Harness.runScenario(allocator, scenario);
        const second = try Harness.runScenario(allocator, scenario);
        try std.testing.expectEqualDeep(first, second);

        const expect_strict_error = scenario.perturbation == .malformed_main_log and
            scenario.startup_mode == .discovered_strict_main_log;

        if (expect_strict_error) {
            try std.testing.expectEqual(OutcomeKind.malformed_main_log_entry, first.kind);
            continue;
        }

        try std.testing.expectEqual(OutcomeKind.success, first.kind);
        try std.testing.expect(first.snapshot_requested);
        try std.testing.expect(first.snapshot_found);
        try std.testing.expect(
            first.duplicate_add_result == .success or
                first.duplicate_add_result == .user_mgmt_user_already_exists,
        );
    }
}

test "exchange orchestrator seeded journal replay perturbations are deterministic and reproducible" {
    const allocator = std.testing.allocator;

    const Perturbation = enum {
        baseline_replay,
        corrupted_replay_sequence,
    };

    const Scenario = struct {
        name: []const u8,
        seed: u64,
        backend: BackendKind,
        perturbation: Perturbation,
    };

    const OutcomeKind = enum {
        success,
        sequence_mismatch,
    };

    const Outcome = struct {
        kind: OutcomeKind,
        snapshot_requested: bool = false,
        snapshot_found: bool = false,
        journal_replayed_commands: usize = 0,
        post_snapshot_user_status: kernel_mod.SingleUserQueryExecutionStatus = .user_not_found,
    };

    const Harness = struct {
        fn javaConfigForBackend(
            backend: BackendKind,
            init_state: config_mod.InitialStateConfig,
        ) JavaConfig {
            return switch (backend) {
                .single => .{
                    .init_state_cfg = init_state,
                    .serialization_cfg = config_mod.SerializationConfig.diskJournaling(),
                },
                .sharded => .{
                    .performance_cfg = config_mod.PerformanceConfig.throughputPreset(),
                    .init_state_cfg = init_state,
                    .serialization_cfg = config_mod.SerializationConfig.diskJournaling(),
                },
            };
        }

        fn captureSnapshotToDefaultPath(
            source: *ExchangeOrchestrator,
            dir: std.fs.Dir,
            snapshot_id: u64,
            module_type: PersistenceModuleType,
        ) !void {
            var symbols = [_]i32{};
            switch (source.backend) {
                .single => |*single| try single.captureSnapshotToDefaultPath(
                    dir,
                    snapshot_id,
                    module_type,
                    0,
                    symbols[0..],
                    8,
                ),
                .sharded => |*sharded| try sharded.captureSnapshotToDefaultPath(
                    dir,
                    snapshot_id,
                    module_type,
                    0,
                    symbols[0..],
                    8,
                ),
            }
        }

        fn writeJournalPartitionZero(
            source: *ExchangeOrchestrator,
            dir: std.fs.Dir,
            snapshot_id: u64,
        ) !void {
            switch (source.backend) {
                .single => |*single| try single.writeJournalToDefaultPath(
                    dir,
                    snapshot_id,
                    0,
                ),
                .sharded => |*sharded| try sharded.writeJournalToDefaultPath(
                    dir,
                    snapshot_id,
                    0,
                ),
            }
        }

        fn defaultJournalPath(
            alloc: std.mem.Allocator,
            source: *ExchangeOrchestrator,
            snapshot_id: u64,
        ) ![]u8 {
            return switch (source.backend) {
                .single => |*single| single.defaultPersistenceJournalPath(
                    alloc,
                    snapshot_id,
                    0,
                ),
                .sharded => |*sharded| sharded.defaultPersistenceJournalPath(
                    alloc,
                    snapshot_id,
                    0,
                ),
            };
        }

        fn defaultMainLogPath(
            alloc: std.mem.Allocator,
            source: *ExchangeOrchestrator,
        ) ![]u8 {
            return switch (source.backend) {
                .single => |*single| single.defaultPersistenceMainLogPath(alloc),
                .sharded => |*sharded| sharded.defaultPersistenceMainLogPath(alloc),
            };
        }

        fn isolateTailJournalPartition(
            alloc: std.mem.Allocator,
            source: *ExchangeOrchestrator,
            dir: std.fs.Dir,
            snapshot_id: u64,
        ) !u64 {
            const partition_zero_path = try defaultJournalPath(alloc, source, snapshot_id);
            defer alloc.free(partition_zero_path);

            var full = try journal_mod.CommandJournal.readFromFile(
                alloc,
                dir,
                partition_zero_path,
            );
            defer full.deinit();

            var tail = journal_mod.CommandJournal.init(alloc);
            defer tail.deinit();
            if (full.entries.items.len == 0) return error.NoReplayableTailEntries;
            const keep = @min(full.entries.items.len, @as(usize, 2));
            const start = full.entries.items.len - keep;
            for (full.entries.items[start..]) |entry| {
                try tail.entries.append(alloc, entry);
            }
            try tail.writeToFile(dir, partition_zero_path);
            const first_tail_seq = tail.entries.items[0].sequence;
            return if (first_tail_seq == 0) 0 else first_tail_seq - 1;
        }

        fn rewriteMainLogSequenceForSnapshot(
            alloc: std.mem.Allocator,
            source: *ExchangeOrchestrator,
            dir: std.fs.Dir,
            snapshot_id: u64,
            module_type: PersistenceModuleType,
            instance_id: u32,
            replay_base_seq: u64,
        ) !void {
            const main_log_path = try defaultMainLogPath(alloc, source);
            defer alloc.free(main_log_path);

            var file = try dir.createFile(main_log_path, .{ .truncate = true });
            defer file.close();

            var line_buf: [256]u8 = undefined;
            const line = try std.fmt.bufPrint(
                &line_buf,
                "0 seq={d} timestampNs=0 snapshotId={d} type={s} instance={d}\n",
                .{
                    replay_base_seq,
                    snapshot_id,
                    persistence_paths.moduleCode(module_type),
                    instance_id,
                },
            );
            try file.writeAll(line);
        }

        fn applyPerturbation(
            alloc: std.mem.Allocator,
            source: *ExchangeOrchestrator,
            dir: std.fs.Dir,
            snapshot_id: u64,
            perturbation: Perturbation,
        ) !void {
            switch (perturbation) {
                .baseline_replay => {},
                .corrupted_replay_sequence => {
                    const partition_zero_path = try defaultJournalPath(alloc, source, snapshot_id);
                    defer alloc.free(partition_zero_path);

                    var journal = try journal_mod.CommandJournal.readFromFile(
                        alloc,
                        dir,
                        partition_zero_path,
                    );
                    defer journal.deinit();

                    if (journal.entries.items.len == 0) return error.NoReplayableTailEntries;
                    journal.entries.items[0].sequence += 1;
                    try journal.writeToFile(dir, partition_zero_path);
                },
            }
        }

        fn runScenario(alloc: std.mem.Allocator, scenario: Scenario) !Outcome {
            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();

            var exchange_id_buf: [64]u8 = undefined;
            const exchange_id = try std.fmt.bufPrint(
                &exchange_id_buf,
                "ECO_JR_{s}_{x}",
                .{ @tagName(scenario.backend), scenario.seed },
            );
            const module_type: PersistenceModuleType = .risk_engine;
            const uid_base: i64 = 992_000 + @as(i64, @intCast(scenario.seed % 1_000)) * 4;
            const existing_uid = uid_base;
            const tail_uid_a = uid_base + 1;
            const tail_uid_b = uid_base + 2;
            const snapshot_id: u64 = 800 + @as(u64, @intCast(scenario.seed % 50));

            var source = try ExchangeOrchestrator.initFromJavaConfig(
                alloc,
                javaConfigForBackend(
                    scenario.backend,
                    config_mod.InitialStateConfig.cleanStart(exchange_id),
                ),
            );
            defer source.deinit();
            try source.startup();

            var add_existing = try source.addUser(.routed, existing_uid);
            defer add_existing.deinit(alloc);
            try std.testing.expectEqual(cmd.CommandResultCode.success, add_existing.result_code);

            try captureSnapshotToDefaultPath(&source, tmp.dir, snapshot_id, module_type);

            var add_tail_a = try source.addUser(.routed, tail_uid_a);
            defer add_tail_a.deinit(alloc);
            try std.testing.expectEqual(cmd.CommandResultCode.success, add_tail_a.result_code);

            var add_tail_b = try source.addUser(.routed, tail_uid_b);
            defer add_tail_b.deinit(alloc);
            try std.testing.expectEqual(cmd.CommandResultCode.success, add_tail_b.result_code);

            try writeJournalPartitionZero(&source, tmp.dir, snapshot_id);
            const replay_base_seq = try isolateTailJournalPartition(
                alloc,
                &source,
                tmp.dir,
                snapshot_id,
            );
            try rewriteMainLogSequenceForSnapshot(
                alloc,
                &source,
                tmp.dir,
                snapshot_id,
                module_type,
                0,
                replay_base_seq,
            );
            try applyPerturbation(
                alloc,
                &source,
                tmp.dir,
                snapshot_id,
                scenario.perturbation,
            );

            var target = try ExchangeOrchestrator.initFromJavaConfig(
                alloc,
                javaConfigForBackend(
                    scenario.backend,
                    config_mod.InitialStateConfig.lastKnownStateFromJournal(
                        exchange_id,
                        snapshot_id,
                        replay_base_seq,
                    ),
                ),
            );
            defer target.deinit();

            const startup = target.startupFromConfiguredPersistence(
                tmp.dir,
                module_type,
                0,
                0,
            ) catch |err| switch (err) {
                error.SequenceMismatch => return .{ .kind = .sequence_mismatch },
                else => std.debug.panic(
                    "seeded orchestrator replay scenario '{s}' seed=0x{x} backend={s} failed with unexpected error {s}; replay with: zig build test --test-filter \"exchange orchestrator seeded journal replay perturbations are deterministic and reproducible\"",
                    .{ scenario.name, scenario.seed, @tagName(scenario.backend), @errorName(err) },
                ),
            };

            var post_snapshot_user = try target.queryReport(.{ .single_user = tail_uid_b });
            defer post_snapshot_user.deinit(alloc);
            const user_status = switch (post_snapshot_user) {
                .single_user => |single_user| single_user.query_execution_status,
                else => return error.ExpectedSingleUserReport,
            };

            return .{
                .kind = .success,
                .snapshot_requested = startup.snapshot_requested,
                .snapshot_found = startup.snapshot_found,
                .journal_replayed_commands = startup.journal_replayed_commands,
                .post_snapshot_user_status = user_status,
            };
        }
    };

    const scenarios = [_]Scenario{
        .{
            .name = "single-baseline-replay",
            .seed = 0xACE3101,
            .backend = .single,
            .perturbation = .baseline_replay,
        },
        .{
            .name = "single-corrupted-replay-sequence",
            .seed = 0xACE3102,
            .backend = .single,
            .perturbation = .corrupted_replay_sequence,
        },
        .{
            .name = "sharded-baseline-replay",
            .seed = 0xACE4101,
            .backend = .sharded,
            .perturbation = .baseline_replay,
        },
        .{
            .name = "sharded-corrupted-replay-sequence",
            .seed = 0xACE4102,
            .backend = .sharded,
            .perturbation = .corrupted_replay_sequence,
        },
    };

    for (scenarios) |scenario| {
        const first = try Harness.runScenario(allocator, scenario);
        const second = try Harness.runScenario(allocator, scenario);
        try std.testing.expectEqualDeep(first, second);

        switch (scenario.perturbation) {
            .baseline_replay => {
                try std.testing.expectEqual(OutcomeKind.success, first.kind);
                try std.testing.expect(first.snapshot_requested);
                try std.testing.expect(first.snapshot_found);
                try std.testing.expect(first.journal_replayed_commands > 0);
                try std.testing.expectEqual(
                    kernel_mod.SingleUserQueryExecutionStatus.ok,
                    first.post_snapshot_user_status,
                );
            },
            .corrupted_replay_sequence => {
                try std.testing.expectEqual(OutcomeKind.sequence_mismatch, first.kind);
            },
        }
    }
}

test "exchange orchestrator long-horizon seeded restart simulation is deterministic and strict-probe reproducible" {
    const allocator = std.testing.allocator;
    const cycles: usize = 24;

    const Scenario = struct {
        name: []const u8,
        seed: u64,
        backend: BackendKind,
    };

    const Outcome = struct {
        strict_failure_count: usize = 0,
        first_strict_failure_cycle: ?u32 = null,
        configured_probe_success_count: usize = 0,
        discovered_probe_success_count: usize = 0,
        injection_cycle: u32 = 0,
        final_snapshot_id: u64 = 0,
        final_user_status: kernel_mod.SingleUserQueryExecutionStatus = .user_not_found,
        final_duplicate_result: cmd.CommandResultCode = .accepted,
    };

    const Harness = struct {
        fn javaConfigForBackend(
            backend: BackendKind,
            init_state: config_mod.InitialStateConfig,
        ) JavaConfig {
            return switch (backend) {
                .single => .{
                    .init_state_cfg = init_state,
                    .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
                },
                .sharded => .{
                    .performance_cfg = config_mod.PerformanceConfig.throughputPreset(),
                    .init_state_cfg = init_state,
                    .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
                },
            };
        }

        fn defaultMainLogPath(
            alloc: std.mem.Allocator,
            source: *ExchangeOrchestrator,
        ) ![]u8 {
            return switch (source.backend) {
                .single => |*single| single.defaultPersistenceMainLogPath(alloc),
                .sharded => |*sharded| sharded.defaultPersistenceMainLogPath(alloc),
            };
        }

        fn appendMalformedMainLog(
            alloc: std.mem.Allocator,
            source: *ExchangeOrchestrator,
            dir: std.fs.Dir,
        ) !void {
            const path = try defaultMainLogPath(alloc, source);
            defer alloc.free(path);

            var file = try dir.createFile(path, .{ .truncate = false });
            defer file.close();
            try file.seekFromEnd(0);
            try file.writeAll("seeded-malformed-main-log-entry\n");
        }

        fn captureSnapshotToDefaultPath(
            source: *ExchangeOrchestrator,
            dir: std.fs.Dir,
            snapshot_id: u64,
            module_type: PersistenceModuleType,
        ) !void {
            var symbols = [_]i32{};
            switch (source.backend) {
                .single => |*single| try single.captureSnapshotToDefaultPath(
                    dir,
                    snapshot_id,
                    module_type,
                    0,
                    symbols[0..],
                    8,
                ),
                .sharded => |*sharded| try sharded.captureSnapshotToDefaultPath(
                    dir,
                    snapshot_id,
                    module_type,
                    0,
                    symbols[0..],
                    8,
                ),
            }
        }

        fn runScenario(alloc: std.mem.Allocator, scenario: Scenario) !Outcome {
            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();

            var exchange_id_buf: [64]u8 = undefined;
            const exchange_id = try std.fmt.bufPrint(
                &exchange_id_buf,
                "ECO_LH_{s}_{x}",
                .{ @tagName(scenario.backend), scenario.seed },
            );
            const module_type: PersistenceModuleType = .risk_engine;
            const base_uid: i64 = 1_200_000 + @as(i64, @intCast(scenario.seed % 10_000)) * 8;
            const injection_cycle: usize = 1 + @as(usize, @intCast(scenario.seed % (cycles - 1)));

            var strict_failure_count: usize = 0;
            var first_strict_failure_cycle: ?u32 = null;
            var configured_probe_success_count: usize = 0;
            var discovered_probe_success_count: usize = 0;
            var malformed_active = false;
            var latest_uid: i64 = base_uid;
            var latest_snapshot_id: u64 = 0;

            for (0..cycles) |cycle| {
                const init_state = if (cycle == 0)
                    config_mod.InitialStateConfig.cleanStart(exchange_id)
                else
                    config_mod.InitialStateConfig.fromSnapshotOnly(exchange_id, 1, 0);

                var source = try ExchangeOrchestrator.initFromJavaConfig(
                    alloc,
                    javaConfigForBackend(scenario.backend, init_state),
                );
                defer source.deinit();

                if (cycle == 0) {
                    try source.startup();
                } else {
                    _ = source.startupFromDiscoveredConfiguredPersistence(
                        tmp.dir,
                        module_type,
                        0,
                        0,
                    ) catch |err| std.debug.panic(
                        "long-horizon source startup failed scenario='{s}' seed=0x{x} backend={s} cycle={d} err={s}; replay with: zig build test --test-filter \"exchange orchestrator long-horizon seeded restart simulation is deterministic and strict-probe reproducible\"",
                        .{
                            scenario.name,
                            scenario.seed,
                            @tagName(scenario.backend),
                            cycle,
                            @errorName(err),
                        },
                    );
                }

                const cycle_i64 = std.math.cast(i64, cycle) orelse return error.CycleOverflow;
                latest_uid = base_uid + (cycle_i64 * 3);
                var add = try source.addUser(.routed, latest_uid);
                defer add.deinit(alloc);
                try std.testing.expectEqual(cmd.CommandResultCode.success, add.result_code);

                latest_snapshot_id = 20_000 + @as(u64, @intCast(cycle));
                try captureSnapshotToDefaultPath(
                    &source,
                    tmp.dir,
                    latest_snapshot_id,
                    module_type,
                );

                if (cycle == injection_cycle) {
                    try appendMalformedMainLog(alloc, &source, tmp.dir);
                    malformed_active = true;
                }

                var configured_probe = try ExchangeOrchestrator.initFromJavaConfig(
                    alloc,
                    javaConfigForBackend(
                        scenario.backend,
                        config_mod.InitialStateConfig.fromSnapshotOnly(
                            exchange_id,
                            latest_snapshot_id,
                            0,
                        ),
                    ),
                );
                defer configured_probe.deinit();
                _ = configured_probe.startupFromConfiguredPersistence(
                    tmp.dir,
                    module_type,
                    0,
                    0,
                ) catch |err| std.debug.panic(
                    "long-horizon configured probe failed scenario='{s}' seed=0x{x} backend={s} cycle={d} err={s}; replay with: zig build test --test-filter \"exchange orchestrator long-horizon seeded restart simulation is deterministic and strict-probe reproducible\"",
                    .{
                        scenario.name,
                        scenario.seed,
                        @tagName(scenario.backend),
                        cycle,
                        @errorName(err),
                    },
                );
                var configured_report = try configured_probe.queryReport(.{ .single_user = latest_uid });
                defer configured_report.deinit(alloc);
                switch (configured_report) {
                    .single_user => |single_user| try std.testing.expect(
                        single_user.query_execution_status == .ok or
                            single_user.query_execution_status == .user_not_found,
                    ),
                    else => return error.ExpectedSingleUserReport,
                }
                configured_probe_success_count += 1;

                var discovered_probe = try ExchangeOrchestrator.initFromJavaConfig(
                    alloc,
                    javaConfigForBackend(
                        scenario.backend,
                        config_mod.InitialStateConfig.fromSnapshotOnly(exchange_id, 1, 0),
                    ),
                );
                defer discovered_probe.deinit();
                _ = discovered_probe.startupFromDiscoveredConfiguredPersistence(
                    tmp.dir,
                    module_type,
                    0,
                    0,
                ) catch |err| std.debug.panic(
                    "long-horizon discovered probe failed scenario='{s}' seed=0x{x} backend={s} cycle={d} err={s}; replay with: zig build test --test-filter \"exchange orchestrator long-horizon seeded restart simulation is deterministic and strict-probe reproducible\"",
                    .{
                        scenario.name,
                        scenario.seed,
                        @tagName(scenario.backend),
                        cycle,
                        @errorName(err),
                    },
                );
                var discovered_report = try discovered_probe.queryReport(.{ .single_user = latest_uid });
                defer discovered_report.deinit(alloc);
                switch (discovered_report) {
                    .single_user => |single_user| try std.testing.expect(
                        single_user.query_execution_status == .ok or
                            single_user.query_execution_status == .user_not_found,
                    ),
                    else => return error.ExpectedSingleUserReport,
                }
                discovered_probe_success_count += 1;

                var strict_probe = try ExchangeOrchestrator.initFromJavaConfig(
                    alloc,
                    javaConfigForBackend(
                        scenario.backend,
                        config_mod.InitialStateConfig.fromSnapshotOnly(exchange_id, 1, 0),
                    ),
                );
                defer strict_probe.deinit();

                _ = strict_probe.startupFromDiscoveredConfiguredPersistenceStrictMainLog(
                    tmp.dir,
                    module_type,
                    0,
                    0,
                ) catch |err| switch (err) {
                    error.MalformedMainLogEntry => {
                        if (!malformed_active) {
                            std.debug.panic(
                                "long-horizon strict probe failed before injection scenario='{s}' seed=0x{x} backend={s} cycle={d}; replay with: zig build test --test-filter \"exchange orchestrator long-horizon seeded restart simulation is deterministic and strict-probe reproducible\"",
                                .{
                                    scenario.name,
                                    scenario.seed,
                                    @tagName(scenario.backend),
                                    cycle,
                                },
                            );
                        }
                        strict_failure_count += 1;
                        if (first_strict_failure_cycle == null) {
                            first_strict_failure_cycle = @as(u32, @intCast(cycle));
                        }
                        continue;
                    },
                    else => std.debug.panic(
                        "long-horizon strict probe unexpected failure scenario='{s}' seed=0x{x} backend={s} cycle={d} err={s}; replay with: zig build test --test-filter \"exchange orchestrator long-horizon seeded restart simulation is deterministic and strict-probe reproducible\"",
                        .{
                            scenario.name,
                            scenario.seed,
                            @tagName(scenario.backend),
                            cycle,
                            @errorName(err),
                        },
                    ),
                };

                if (malformed_active) {
                    std.debug.panic(
                        "long-horizon strict probe succeeded after malformed injection scenario='{s}' seed=0x{x} backend={s} cycle={d}; replay with: zig build test --test-filter \"exchange orchestrator long-horizon seeded restart simulation is deterministic and strict-probe reproducible\"",
                        .{
                            scenario.name,
                            scenario.seed,
                            @tagName(scenario.backend),
                            cycle,
                        },
                    );
                }
            }

            var final_target = try ExchangeOrchestrator.initFromJavaConfig(
                alloc,
                javaConfigForBackend(
                    scenario.backend,
                    config_mod.InitialStateConfig.fromSnapshotOnly(exchange_id, 1, 0),
                ),
            );
            defer final_target.deinit();
            _ = final_target.startupFromDiscoveredConfiguredPersistence(
                tmp.dir,
                module_type,
                0,
                0,
            ) catch |err| std.debug.panic(
                "long-horizon final startup failed scenario='{s}' seed=0x{x} backend={s} err={s}; replay with: zig build test --test-filter \"exchange orchestrator long-horizon seeded restart simulation is deterministic and strict-probe reproducible\"",
                .{
                    scenario.name,
                    scenario.seed,
                    @tagName(scenario.backend),
                    @errorName(err),
                },
            );

            var final_report = try final_target.queryReport(.{ .single_user = latest_uid });
            defer final_report.deinit(alloc);
            const final_status = switch (final_report) {
                .single_user => |single_user| single_user.query_execution_status,
                else => return error.ExpectedSingleUserReport,
            };

            var duplicate_add = try final_target.addUser(.routed, latest_uid);
            defer duplicate_add.deinit(alloc);

            return .{
                .strict_failure_count = strict_failure_count,
                .first_strict_failure_cycle = first_strict_failure_cycle,
                .configured_probe_success_count = configured_probe_success_count,
                .discovered_probe_success_count = discovered_probe_success_count,
                .injection_cycle = @as(u32, @intCast(injection_cycle)),
                .final_snapshot_id = latest_snapshot_id,
                .final_user_status = final_status,
                .final_duplicate_result = duplicate_add.result_code,
            };
        }
    };

    const scenarios = [_]Scenario{
        .{
            .name = "single-long-horizon",
            .seed = 0xACE5101,
            .backend = .single,
        },
        .{
            .name = "single-long-horizon-alt",
            .seed = 0xACE5102,
            .backend = .single,
        },
        .{
            .name = "sharded-long-horizon",
            .seed = 0xACE6101,
            .backend = .sharded,
        },
        .{
            .name = "sharded-long-horizon-alt",
            .seed = 0xACE6102,
            .backend = .sharded,
        },
    };

    var single_a: ?Outcome = null;
    var single_b: ?Outcome = null;
    var sharded_a: ?Outcome = null;
    var sharded_b: ?Outcome = null;

    for (scenarios) |scenario| {
        const first = try Harness.runScenario(allocator, scenario);
        const second = try Harness.runScenario(allocator, scenario);
        try std.testing.expectEqualDeep(first, second);

        try std.testing.expectEqual(cycles, first.configured_probe_success_count);
        try std.testing.expectEqual(cycles, first.discovered_probe_success_count);
        try std.testing.expect(first.strict_failure_count > 0);
        try std.testing.expectEqual(first.injection_cycle, first.first_strict_failure_cycle.?);
        const expected_failures = cycles - @as(usize, first.injection_cycle);
        try std.testing.expectEqual(expected_failures, first.strict_failure_count);
        try std.testing.expect(
            first.final_user_status == .ok or
                first.final_user_status == .user_not_found,
        );
        if (first.final_user_status == .ok) {
            try std.testing.expect(
                first.final_duplicate_result == .success or
                    first.final_duplicate_result == .user_mgmt_user_already_exists,
            );
        } else {
            try std.testing.expectEqual(cmd.CommandResultCode.success, first.final_duplicate_result);
        }

        switch (scenario.backend) {
            .single => {
                if (single_a == null) {
                    single_a = first;
                } else {
                    single_b = first;
                }
            },
            .sharded => {
                if (sharded_a == null) {
                    sharded_a = first;
                } else {
                    sharded_b = first;
                }
            },
        }
    }

    const s_a = single_a orelse return error.MissingSeedCoverage;
    const s_b = single_b orelse return error.MissingSeedCoverage;
    try std.testing.expect(s_a.injection_cycle != s_b.injection_cycle);

    const h_a = sharded_a orelse return error.MissingSeedCoverage;
    const h_b = sharded_b orelse return error.MissingSeedCoverage;
    try std.testing.expect(h_a.injection_cycle != h_b.injection_cycle);
}

test "exchange orchestrator long-horizon seeded mixed sync-async submissions are deterministic" {
    const allocator = std.testing.allocator;
    const rounds: usize = 320;
    const user_pool: usize = 64;

    const Scenario = struct {
        name: []const u8,
        seed: u64,
        backend: BackendKind,
    };

    const Outcome = struct {
        digest: u64 = 0,
        sync_submissions: usize = 0,
        async_submissions: usize = 0,
        drain_calls: usize = 0,
        drained_total: usize = 0,
        success_results: usize = 0,
        duplicate_results: usize = 0,
        final_present_users: usize = 0,
    };

    const Harness = struct {
        fn javaConfigForBackend(
            backend: BackendKind,
            init_state: config_mod.InitialStateConfig,
        ) JavaConfig {
            return switch (backend) {
                .single => .{
                    .init_state_cfg = init_state,
                    .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
                },
                .sharded => .{
                    .performance_cfg = config_mod.PerformanceConfig.throughputPreset(),
                    .init_state_cfg = init_state,
                    .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
                },
            };
        }

        fn mixDigest(digest: *u64, token: u64) void {
            digest.* = (digest.* ^ token) *% 0x9E3779B185EBCA87;
            digest.* +%= 0x632BE59BD9B4E019;
        }

        fn digestResultCode(digest: *u64, result_code: cmd.CommandResultCode) void {
            const token: u64 = switch (result_code) {
                .success => 1,
                .user_mgmt_user_already_exists => 2,
                .accepted => 3,
                else => 63,
            };
            mixDigest(digest, token);
        }

        fn digestQueryStatus(
            digest: *u64,
            status: kernel_mod.SingleUserQueryExecutionStatus,
        ) void {
            const token: u64 = switch (status) {
                .ok => 11,
                .user_not_found => 12,
            };
            mixDigest(digest, token);
        }

        fn recordResponse(outcome: *Outcome, response: kernel_mod.CommandResponse) void {
            mixDigest(&outcome.digest, response.sequence);
            digestResultCode(&outcome.digest, response.result_code);
            switch (response.result_code) {
                .success => outcome.success_results += 1,
                .user_mgmt_user_already_exists => outcome.duplicate_results += 1,
                else => {},
            }
        }

        fn collectAsyncResults(
            alloc: std.mem.Allocator,
            orchestrator: *ExchangeOrchestrator,
            pending_handles: *std.ArrayListUnmanaged(ApiCommandAsyncHandle),
            outcome: *Outcome,
        ) !void {
            if (pending_handles.items.len == 0) return;

            const drained = try orchestrator.drainPending();
            outcome.drain_calls += 1;
            outcome.drained_total = std.math.add(usize, outcome.drained_total, drained) catch
                return error.DrainCountOverflow;
            mixDigest(&outcome.digest, drained);

            for (pending_handles.items) |handle| {
                var result = orchestrator.takeApiCommandAsyncResult(handle) orelse
                    return error.MissingAsyncResult;
                defer result.deinit(alloc);
                switch (result) {
                    .response => |response| recordResponse(outcome, response),
                    .persist_state => return error.UnexpectedPersistStateResult,
                }
            }
            pending_handles.clearRetainingCapacity();
        }

        fn runScenario(alloc: std.mem.Allocator, scenario: Scenario) !Outcome {
            var orchestrator = try ExchangeOrchestrator.initFromJavaConfig(
                alloc,
                javaConfigForBackend(
                    scenario.backend,
                    config_mod.InitialStateConfig.cleanStart("ECO_ASYNC"),
                ),
            );
            defer orchestrator.deinit();
            try orchestrator.startup();

            var outcome: Outcome = .{ .digest = 0xC0DEC0DEC0DEC0DE ^ scenario.seed };
            var pending_handles: std.ArrayListUnmanaged(ApiCommandAsyncHandle) = .empty;
            defer pending_handles.deinit(alloc);

            var prng = std.Random.DefaultPrng.init(scenario.seed);
            const random = prng.random();
            const base_uid: i64 = 1_600_000 + @as(i64, @intCast(scenario.seed % 50_000));

            for (0..rounds) |round_idx| {
                const uid_offset = random.uintLessThan(usize, user_pool);
                const uid = base_uid + @as(i64, @intCast(uid_offset));
                if (random.uintLessThan(u32, 100) < 45) {
                    var response = try orchestrator.addUser(.routed, uid);
                    defer response.deinit(alloc);
                    outcome.sync_submissions += 1;
                    recordResponse(&outcome, response);
                } else {
                    const handle = try orchestrator.submitApiCommandAsync(
                        .routed,
                        .{ .add_user = uid },
                    );
                    outcome.async_submissions += 1;
                    try pending_handles.append(alloc, handle);
                    switch (handle) {
                        .request_id => |request_id| mixDigest(&outcome.digest, request_id),
                        .persist_state => return error.UnexpectedPersistStateHandle,
                    }
                }

                const should_drain = pending_handles.items.len > 0 and
                    (round_idx % 7 == 0 or random.uintLessThan(u32, 100) < 30);
                if (should_drain) {
                    try collectAsyncResults(
                        alloc,
                        &orchestrator,
                        &pending_handles,
                        &outcome,
                    );
                }
            }

            try collectAsyncResults(
                alloc,
                &orchestrator,
                &pending_handles,
                &outcome,
            );

            for (0..user_pool) |uid_offset| {
                const uid = base_uid + @as(i64, @intCast(uid_offset));
                var report = try orchestrator.queryReport(.{ .single_user = uid });
                defer report.deinit(alloc);
                switch (report) {
                    .single_user => |single_user| {
                        digestQueryStatus(&outcome.digest, single_user.query_execution_status);
                        if (single_user.query_execution_status == .ok) {
                            outcome.final_present_users += 1;
                        }
                    },
                    else => return error.ExpectedSingleUserReport,
                }
            }

            return outcome;
        }
    };

    const scenarios = [_]Scenario{
        .{
            .name = "single-seeded-mixed-async",
            .seed = 0xACE7101,
            .backend = .single,
        },
        .{
            .name = "single-seeded-mixed-async-alt",
            .seed = 0xACE7102,
            .backend = .single,
        },
        .{
            .name = "sharded-seeded-mixed-async",
            .seed = 0xACE8101,
            .backend = .sharded,
        },
        .{
            .name = "sharded-seeded-mixed-async-alt",
            .seed = 0xACE8102,
            .backend = .sharded,
        },
    };

    var single_digest_a: ?u64 = null;
    var single_digest_b: ?u64 = null;
    var sharded_digest_a: ?u64 = null;
    var sharded_digest_b: ?u64 = null;

    for (scenarios) |scenario| {
        _ = scenario.name;
        const first = try Harness.runScenario(allocator, scenario);
        const second = try Harness.runScenario(allocator, scenario);
        try std.testing.expectEqualDeep(first, second);

        try std.testing.expect(first.sync_submissions > 0);
        try std.testing.expect(first.async_submissions > 0);
        try std.testing.expectEqual(rounds, first.sync_submissions + first.async_submissions);
        try std.testing.expectEqual(first.async_submissions, first.drained_total);
        try std.testing.expect(first.final_present_users > 0);
        try std.testing.expect(first.final_present_users <= user_pool);
        try std.testing.expect(first.success_results > 0);
        try std.testing.expect(first.duplicate_results > 0);

        switch (scenario.backend) {
            .single => {
                if (single_digest_a == null) {
                    single_digest_a = first.digest;
                } else {
                    single_digest_b = first.digest;
                }
            },
            .sharded => {
                if (sharded_digest_a == null) {
                    sharded_digest_a = first.digest;
                } else {
                    sharded_digest_b = first.digest;
                }
            },
        }
    }

    const s_digest_a = single_digest_a orelse return error.MissingSeedCoverage;
    const s_digest_b = single_digest_b orelse return error.MissingSeedCoverage;
    try std.testing.expect(s_digest_a != s_digest_b);

    const h_digest_a = sharded_digest_a orelse return error.MissingSeedCoverage;
    const h_digest_b = sharded_digest_b orelse return error.MissingSeedCoverage;
    try std.testing.expect(h_digest_a != h_digest_b);
}
