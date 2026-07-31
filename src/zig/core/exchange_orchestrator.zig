const std = @import("std");
const io_context = @import("io_context.zig");
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
pub const RawResultSink = runtime_mod.RawResultSink;
pub const RuntimeDrainOutcome = runtime_mod.DrainOutcome;
pub const ShardedPipelineMode = config_mod.ShardedPipelineMode;
pub const CommandResponseCallback = exchange_api_sharded_mod.CommandResponseCallback;
pub const SimpleEventsHandler = simple_events_mod.EventsHandler;
pub const BootstrapOutcome = exchange_core_mod.BootstrapOutcome;
pub const CoreFatalInfo = runtime_mod.FatalRecord;
pub const ShutdownState = runtime_mod.ShutdownState;
pub const ShutdownOutcome = runtime_mod.ShutdownOutcome;

pub const BackendKind = enum {
    single,
    sharded,
};

fn backendKindForPerformance(performance: config_mod.PerformanceConfig) BackendKind {
    return if (performance.matching_engines_num > 1 or
        performance.risk_engines_num > 1)
        .sharded
    else
        .single;
}

fn backendKindForJavaConfig(java_config: JavaConfig) BackendKind {
    if (java_config.sharded_pipeline_mode == .persistent_grouped) return .sharded;
    return backendKindForPerformance(java_config.performance_cfg);
}

fn ensureCompositeBlockingCallAllowed() error{CoreReentrantCall}!void {
    if (runtime_mod.isCallbackExecutionActive()) return error.CoreReentrantCall;
}

pub const ModeValidationError = error{
    SplitModeRequiresShardedBackend,
};

pub const InitFromJavaConfigError = exchange_core_mod.InitFromJavaConfigError || exchange_api_sharded_mod.InitFromJavaConfigError;
pub const LifecycleError = exchange_core_mod.LifecycleError || exchange_api_sharded_mod.LifecycleError;
pub const CallbackConfigError = exchange_core_mod.CallbackConfigError || exchange_api_sharded_mod.CallbackConfigError;
pub const StartError = exchange_core_mod.StartError || exchange_api_sharded_mod.StartError;
pub const ExecuteError = exchange_core_mod.ExecuteError || exchange_api_sharded_mod.ExecuteError || ModeValidationError;
pub const HelperError = exchange_core_mod.HelperError || exchange_api_sharded_mod.HelperError || ModeValidationError;
pub const SubmitApiCommandError = exchange_core_mod.SubmitApiCommandError || exchange_api_sharded_mod.SubmitApiCommandError || ModeValidationError;
pub const SubmitApiCommandAsyncError = exchange_core_mod.SubmitApiCommandAsyncError || exchange_api_sharded_mod.SubmitApiCommandAsyncError || ModeValidationError;
pub const SubmitCommandAsyncFullResponseError = exchange_core_mod.SubmitApiCommandAsyncError ||
    exchange_api_sharded_mod.SubmitCommandAsyncFullResponseError ||
    ModeValidationError;
pub const SubmitApiCommandBatchError = exchange_core_mod.SubmitApiCommandBatchError ||
    exchange_api_sharded_mod.SubmitApiCommandBatchError ||
    ModeValidationError;
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
pub const SubmitCommandWithCallbackError = exchange_core_mod.SubmitApiCommandWithCallbackError ||
    exchange_api_sharded_mod.SubmitCommandWithCallbackError ||
    ModeValidationError;
pub const ProcessRegisteredCallbackError = exchange_api_sharded_mod.ProcessRegisteredCallbackError;
pub const ShutdownError = exchange_core_mod.ShutdownError || exchange_api_sharded_mod.ShutdownError;

/// Owns its selected backend inline. After any startup entrypoint succeeds,
/// this value must remain at a stable address until shutdown is `.complete` or
/// `deinit` returns because either backend worker can retain its payload
/// address.
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
        if (backendKindForJavaConfig(java_config) == .sharded) {
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

    /// Selects the single or sharded backend from the Java performance
    /// configuration and gives that backend an independently owned handle to
    /// `persistence_root`. The supplied directory is borrowed only during
    /// initialization and may be closed immediately after this call returns.
    pub fn initFromJavaConfigWithPersistenceRoot(
        allocator: std.mem.Allocator,
        persistence_root: std.Io.Dir,
        java_config: JavaConfig,
    ) InitFromJavaConfigError!ExchangeOrchestrator {
        if (backendKindForJavaConfig(java_config) == .sharded) {
            return .{
                .allocator = allocator,
                .backend = .{
                    .sharded = try exchange_api_sharded_mod.ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
                        allocator,
                        persistence_root,
                        java_config,
                    ),
                },
            };
        }

        if (java_config.performance_cfg.matching_engines_num != 1) {
            return error.SingleCoreRequiresOneMatchingEngine;
        }
        if (java_config.performance_cfg.risk_engines_num != 1) {
            return error.SingleCoreRequiresOneRiskEngine;
        }
        return .{
            .allocator = allocator,
            .backend = .{
                .single = try exchange_core_mod.ExchangeCore.initWithPersistenceRoot(
                    allocator,
                    persistence_root,
                    try java_config.toExchangeCoreConfig(),
                ),
            },
        };
    }

    /// The caller must exclude concurrent public calls, including concurrent
    /// shutdown waiters. Sequential deinit after a timed-out close is safe:
    /// the selected backend's unbounded shutdown finishes or joins its
    /// retained worker first.
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
        return self.matchingShardCount();
    }

    pub fn matchingShardCount(self: *const ExchangeOrchestrator) usize {
        return switch (self.backend) {
            .single => 1,
            .sharded => |sharded| sharded.matchingShardCount(),
        };
    }

    pub fn riskShardCount(self: *const ExchangeOrchestrator) usize {
        return switch (self.backend) {
            .single => 1,
            .sharded => |sharded| sharded.riskShardCount(),
        };
    }

    pub fn matchingShardMask(self: *const ExchangeOrchestrator) u64 {
        return switch (self.backend) {
            .single => 0,
            .sharded => |sharded| sharded.matchingShardMask(),
        };
    }

    pub fn riskShardMask(self: *const ExchangeOrchestrator) u64 {
        return switch (self.backend) {
            .single => 0,
            .sharded => |sharded| sharded.riskShardMask(),
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

    /// On success, the selected worker may retain the address of its inline
    /// union payload. Do not move this orchestrator until shutdown is
    /// `.complete` or `deinit` returns.
    pub fn startup(self: *ExchangeOrchestrator) StartError!void {
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

    pub fn shutdownState(self: *const ExchangeOrchestrator) ShutdownState {
        return switch (self.backend) {
            .single => |*single| single.shutdownState(),
            .sharded => |*sharded| sharded.shutdownState(),
        };
    }

    /// A bounded or deferred close can retain the address of the inline union
    /// payload for either backend. Keep this orchestrator in place until
    /// shutdown completes or `deinit` returns; deinitialization must not race
    /// active public calls.
    pub fn shutdownTimeout(
        self: *ExchangeOrchestrator,
        timeout: std.Io.Timeout,
    ) ShutdownError!ShutdownOutcome {
        return switch (self.backend) {
            .single => |*single| single.shutdownTimeout(timeout),
            .sharded => |*sharded| sharded.shutdownTimeout(timeout),
        };
    }

    pub fn startupFromPersistence(
        self: *ExchangeOrchestrator,
        dir: std.Io.Dir,
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
        dir: std.Io.Dir,
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
        dir: std.Io.Dir,
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
        dir: std.Io.Dir,
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
        dir: std.Io.Dir,
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

    pub fn setRawResultSink(self: *ExchangeOrchestrator, sink: RawResultSink) void {
        self.setRawResultSinkChecked(sink) catch |err| @panic(@errorName(err));
    }

    pub fn setRawResultSinkChecked(
        self: *ExchangeOrchestrator,
        sink: RawResultSink,
    ) CallbackConfigError!void {
        return switch (self.backend) {
            .single => |*single| single.setRawResultSinkChecked(sink),
            .sharded => |*sharded| sharded.setRawResultSinkChecked(sink),
        };
    }

    pub fn clearRawResultSink(self: *ExchangeOrchestrator) void {
        self.clearRawResultSinkChecked() catch |err| @panic(@errorName(err));
    }

    pub fn clearRawResultSinkChecked(
        self: *ExchangeOrchestrator,
    ) CallbackConfigError!void {
        return switch (self.backend) {
            .single => |*single| single.clearRawResultSinkChecked(),
            .sharded => |*sharded| sharded.clearRawResultSinkChecked(),
        };
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
            .single => |*single| single.submitApiCommandDetached(command),
            .sharded => |*sharded| sharded.submitApiCommand(mode, command),
        };
    }

    pub fn submitCommand(
        self: *ExchangeOrchestrator,
        mode: ExecutionMode,
        command: ApiCommand,
    ) SubmitApiCommandError!void {
        return self.submitApiCommand(mode, command);
    }

    /// Explicit settlement-safe submission for callers that need an owned
    /// result instead of Java's detached `submitCommand` timing.
    pub fn submitApiCommandSync(
        self: *ExchangeOrchestrator,
        mode: ExecutionMode,
        command: ApiCommand,
    ) SubmitApiCommandError!ApiCommandResult {
        try self.ensureModeSupported(mode);
        return switch (self.backend) {
            .single => |*single| single.submitApiCommand(command),
            .sharded => |*sharded| sharded.submitApiCommandResult(mode, command),
        };
    }

    pub fn submitCommandSync(
        self: *ExchangeOrchestrator,
        mode: ExecutionMode,
        command: ApiCommand,
    ) SubmitApiCommandError!ApiCommandResult {
        return self.submitApiCommandSync(mode, command);
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

    /// Java-compatible full-response async submission. Only commands that
    /// translate to one ordinary `OrderCommand` are accepted; callers retain
    /// poll ownership through the returned handle.
    pub fn submitCommandAsyncFullResponse(
        self: *ExchangeOrchestrator,
        mode: ExecutionMode,
        command: ApiCommand,
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
        try self.ensureModeSupported(mode);
        if (!api_command_mod.supportsAsyncFullResponse(command)) {
            return error.UnsupportedAsyncFullResponseCommand;
        }
        return switch (self.backend) {
            .single => |*single| single.submitApiCommandAsync(command),
            .sharded => |*sharded| sharded.submitCommandAsyncFullResponse(mode, command),
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

    /// Settlement-safe batch submission whose final result remains owned by
    /// the caller. An empty batch is rejected consistently by both backends.
    pub fn submitApiCommandsSyncResult(
        self: *ExchangeOrchestrator,
        mode: ExecutionMode,
        commands: []const ApiCommand,
    ) SubmitApiCommandBatchError!ApiCommandResult {
        try self.ensureModeSupported(mode);
        if (commands.len == 0) return error.EmptyApiCommandBatch;
        try ensureCompositeBlockingCallAllowed();
        return switch (self.backend) {
            .single => |*single| single.submitApiCommandsSync(commands),
            .sharded => |*sharded| sharded.submitApiCommandsSyncResult(mode, commands),
        };
    }

    /// Java-style batch submission. The terminal result is used as the
    /// settlement barrier and released before this call returns.
    pub fn submitApiCommandsSync(
        self: *ExchangeOrchestrator,
        mode: ExecutionMode,
        commands: []const ApiCommand,
    ) SubmitApiCommandBatchError!void {
        try self.ensureModeSupported(mode);
        if (commands.len == 0) return;
        try ensureCompositeBlockingCallAllowed();
        var result = switch (self.backend) {
            .single => |*single| try single.submitApiCommandsSync(commands),
            .sharded => |*sharded| try sharded.submitApiCommandsSyncResult(mode, commands),
        };
        defer result.deinit(self.allocator);
    }

    pub fn submitCommandsSync(
        self: *ExchangeOrchestrator,
        mode: ExecutionMode,
        commands: []const ApiCommand,
    ) SubmitApiCommandBatchError!void {
        return self.submitApiCommandsSync(mode, commands);
    }

    /// Uses a NOP as an explicit FIFO settlement barrier, including for an
    /// empty input. The returned response is owned by the caller.
    pub fn submitApiCommandsSyncWithNop(
        self: *ExchangeOrchestrator,
        mode: ExecutionMode,
        commands: []const ApiCommand,
    ) SubmitApiCommandBatchError!kernel_mod.CommandResponse {
        try self.ensureModeSupported(mode);
        try ensureCompositeBlockingCallAllowed();
        return switch (self.backend) {
            .single => |*single| single.submitApiCommandsSyncWithNop(commands),
            .sharded => |*sharded| sharded.submitApiCommandsSyncWithNop(mode, commands),
        };
    }

    pub fn submitCommandsSyncWithNop(
        self: *ExchangeOrchestrator,
        mode: ExecutionMode,
        commands: []const ApiCommand,
    ) SubmitApiCommandBatchError!kernel_mod.CommandResponse {
        return self.submitApiCommandsSyncWithNop(mode, commands);
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

    pub fn fatalInfo(self: *const ExchangeOrchestrator) ?CoreFatalInfo {
        return switch (self.backend) {
            .single => |*single| single.fatalInfo(),
            .sharded => |*sharded| sharded.fatalInfo(),
        };
    }

    /// Atomically publishes one callback-owned ordinary command. The
    /// backend-neutral allowlist excludes raw/multi-command escape hatches and
    /// reset/NOP, matching Java's command-supported Consumer surface.
    pub fn submitApiCommandWithCallback(
        self: *ExchangeOrchestrator,
        mode: ExecutionMode,
        command: ApiCommand,
        callback: CommandResponseCallback,
    ) SubmitCommandWithCallbackError!void {
        try self.ensureModeSupported(mode);
        try ensureCallbackCommandSupported(command);
        return switch (self.backend) {
            .single => |*single| single.submitApiCommandWithCallback(command, callback),
            .sharded => |*sharded| sharded.submitCommandAsyncWithCallback(
                mode,
                command,
                callback,
            ),
        };
    }

    pub fn createUserWithCallback(
        self: *ExchangeOrchestrator,
        mode: ExecutionMode,
        user_id: i64,
        callback: CommandResponseCallback,
    ) SubmitCommandWithCallbackError!void {
        return self.submitApiCommandWithCallback(
            mode,
            .{ .add_user = user_id },
            callback,
        );
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

    fn ensureCallbackCommandSupported(
        command: ApiCommand,
    ) error{UnsupportedCallbackCommand}!void {
        switch (command) {
            .add_user,
            .add_user_with_timestamp,
            .adjust_user_balance,
            .suspend_user,
            .suspend_user_with_timestamp,
            .resume_user,
            .resume_user_with_timestamp,
            .place_order,
            .move_order,
            .cancel_order,
            .reduce_order,
            .request_order_book,
            => {},
            .raw_command,
            .persist_state,
            .grouping_control,
            .reset,
            .reset_with_timestamp,
            .nop,
            .nop_with_timestamp,
            .binary_add_symbols,
            .binary_add_accounts,
            .binary_report_query,
            => return error.UnsupportedCallbackCommand,
        }
    }
};

const OrchestratorRawResultProbe = struct {
    sequences: [8]u64 = undefined,
    command_images: [8]cmd.OrderCommand = undefined,
    result_codes: [8]cmd.CommandResultCode = undefined,
    len: usize = 0,

    fn onResult(
        context: ?*anyopaque,
        response: *const kernel_mod.CommandResponse,
    ) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        std.debug.assert(self.len < self.sequences.len);
        self.sequences[self.len] = response.sequence;
        self.command_images[self.len] = response.command;
        self.result_codes[self.len] = response.result_code;
        self.len += 1;
    }
};

fn expectSuccessfulOrchestratorResponse(
    response: *const kernel_mod.CommandResponse,
    expected_sequence: u64,
    expected_command: cmd.OrderCommandType,
) !void {
    try std.testing.expectEqual(expected_sequence, response.sequence);
    try std.testing.expectEqual(expected_command, response.command.command);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.command.result_code);
}

fn expectSuccessfulOrchestratorApiResult(
    result: *const ApiCommandResult,
    expected_sequence: u64,
    expected_command: cmd.OrderCommandType,
) !void {
    switch (result.*) {
        .response => |response| try expectSuccessfulOrchestratorResponse(
            &response,
            expected_sequence,
            expected_command,
        ),
        .persist_state => return error.UnexpectedPersistStateResult,
    }
}

fn runOrchestratorRawResultForwardingScenario(
    java_config: JavaConfig,
    expected_backend: BackendKind,
    first_uid: i64,
) !void {
    const allocator = std.testing.allocator;
    const typed_timestamp: i64 = 9_979_310_000;
    const grouping_off_timestamp: i64 = 9_979_310_001;
    const grouping_on_timestamp: i64 = 9_979_310_002;

    var orchestrator = try ExchangeOrchestrator.initFromJavaConfig(
        allocator,
        java_config,
    );
    defer orchestrator.deinit();
    try std.testing.expectEqual(expected_backend, orchestrator.backendKind());

    var probe: OrchestratorRawResultProbe = .{};
    try orchestrator.setRawResultSinkChecked(.{
        .context = &probe,
        .on_result = OrchestratorRawResultProbe.onResult,
    });
    try orchestrator.startup();

    var first = try orchestrator.addUser(.routed, first_uid);
    defer first.deinit(allocator);
    try expectSuccessfulOrchestratorResponse(&first, 1, .add_user);

    var typed = try orchestrator.submitApiCommandSync(.routed, .{
        .adjust_user_balance = .{
            .uid = first_uid,
            .transaction_id = 1,
            .currency = 15,
            .amount = 500,
            .adjustment_type = .adjustment,
            .timestamp = typed_timestamp,
        },
    });
    defer typed.deinit(allocator);
    try expectSuccessfulOrchestratorApiResult(&typed, 2, .balance_adjustment);
    switch (typed) {
        .response => |response| try std.testing.expectEqual(
            typed_timestamp,
            response.command.timestamp,
        ),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var grouping_off = try orchestrator.submitApiCommandSync(.routed, .{
        .grouping_control = .{
            .timestamp_ns = grouping_off_timestamp,
            .mode = 0,
        },
    });
    defer grouping_off.deinit(allocator);
    try expectSuccessfulOrchestratorApiResult(&grouping_off, 3, .grouping_control);

    var suppressed = try orchestrator.addUser(.routed, first_uid + 1);
    defer suppressed.deinit(allocator);
    try expectSuccessfulOrchestratorResponse(&suppressed, 4, .add_user);

    var grouping_on = try orchestrator.submitApiCommandSync(.routed, .{
        .grouping_control = .{
            .timestamp_ns = grouping_on_timestamp,
            .mode = 1,
        },
    });
    defer grouping_on.deinit(allocator);
    try expectSuccessfulOrchestratorApiResult(&grouping_on, 5, .grouping_control);

    var visible_again = try orchestrator.addUser(.routed, first_uid + 2);
    defer visible_again.deinit(allocator);
    try expectSuccessfulOrchestratorResponse(&visible_again, 6, .add_user);

    try std.testing.expectEqual(@as(usize, 4), probe.len);
    try std.testing.expectEqualSlices(
        u64,
        &.{ 1, 2, 5, 6 },
        probe.sequences[0..probe.len],
    );
    try std.testing.expectEqualSlices(
        cmd.OrderCommandType,
        &.{ .add_user, .balance_adjustment, .grouping_control, .add_user },
        &.{
            probe.command_images[0].command,
            probe.command_images[1].command,
            probe.command_images[2].command,
            probe.command_images[3].command,
        },
    );
    try std.testing.expectEqualSlices(
        cmd.CommandResultCode,
        &.{ .success, .success, .success, .success },
        probe.result_codes[0..probe.len],
    );
    for (probe.command_images[0..probe.len]) |command_image| {
        try std.testing.expectEqual(cmd.CommandResultCode.success, command_image.result_code);
    }
    try std.testing.expectEqual(first_uid, probe.command_images[0].uid);
    try std.testing.expectEqual(typed_timestamp, probe.command_images[1].timestamp);
    try std.testing.expectEqual(@as(i64, 1), probe.command_images[2].order_id);
    try std.testing.expectEqual(grouping_on_timestamp, probe.command_images[2].timestamp);
    try std.testing.expectEqual(first_uid + 2, probe.command_images[3].uid);

    try orchestrator.clearRawResultSinkChecked();
    const count_after_clear = probe.len;
    var after_clear = try orchestrator.addUser(.routed, first_uid + 3);
    defer after_clear.deinit(allocator);
    try expectSuccessfulOrchestratorResponse(&after_clear, 7, .add_user);
    try std.testing.expectEqual(count_after_clear, probe.len);
}

fn runOrchestratorScalarUnitTimestampScenario(
    java_config: JavaConfig,
    expected_backend: BackendKind,
    uid: i64,
) !void {
    const allocator = std.testing.allocator;
    const timestamps = [_]i64{
        9_979_320_001,
        9_979_320_002,
        9_979_320_003,
        9_979_320_004,
        9_979_320_005,
    };

    var orchestrator = try ExchangeOrchestrator.initFromJavaConfig(
        allocator,
        java_config,
    );
    defer orchestrator.deinit();
    try std.testing.expectEqual(expected_backend, orchestrator.backendKind());

    var probe: OrchestratorRawResultProbe = .{};
    try orchestrator.setRawResultSinkChecked(.{
        .context = &probe,
        .on_result = OrchestratorRawResultProbe.onResult,
    });
    try orchestrator.startup();

    var add = try orchestrator.submitApiCommandSync(.routed, .{
        .add_user_with_timestamp = .{
            .uid = uid,
            .timestamp = timestamps[0],
        },
    });
    defer add.deinit(allocator);
    try expectSuccessfulOrchestratorApiResult(&add, 1, .add_user);
    switch (add) {
        .response => |response| try std.testing.expectEqual(
            timestamps[0],
            response.command.timestamp,
        ),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    try orchestrator.submitApiCommand(.routed, .{
        .suspend_user_with_timestamp = .{
            .uid = uid,
            .timestamp = timestamps[1],
        },
    });
    const resume_handle = try orchestrator.submitApiCommandAsync(.routed, .{
        .resume_user_with_timestamp = .{
            .uid = uid,
            .timestamp = timestamps[2],
        },
    });
    _ = try orchestrator.drainPending();
    var resume_result = orchestrator.takeApiCommandAsyncResult(resume_handle) orelse
        return error.MissingAsyncResult;
    defer resume_result.deinit(allocator);
    try expectSuccessfulOrchestratorApiResult(&resume_result, 3, .resume_user);
    switch (resume_result) {
        .response => |response| try std.testing.expectEqual(
            timestamps[2],
            response.command.timestamp,
        ),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var reset_result = try orchestrator.submitApiCommandSync(.routed, .{
        .reset_with_timestamp = .{ .timestamp = timestamps[3] },
    });
    defer reset_result.deinit(allocator);
    try expectSuccessfulOrchestratorApiResult(&reset_result, 4, .reset);
    switch (reset_result) {
        .response => |response| try std.testing.expectEqual(
            timestamps[3],
            response.command.timestamp,
        ),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    const nop_handle = try orchestrator.submitApiCommandAsync(.routed, .{
        .nop_with_timestamp = .{ .timestamp = timestamps[4] },
    });
    _ = try orchestrator.drainPending();
    var nop_result = orchestrator.takeApiCommandAsyncResult(nop_handle) orelse
        return error.MissingAsyncResult;
    defer nop_result.deinit(allocator);
    try expectSuccessfulOrchestratorApiResult(&nop_result, 5, .nop);
    switch (nop_result) {
        .response => |response| try std.testing.expectEqual(
            timestamps[4],
            response.command.timestamp,
        ),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    const expected_types = [_]cmd.OrderCommandType{
        .add_user,
        .suspend_user,
        .resume_user,
        .reset,
        .nop,
    };
    try std.testing.expectEqual(expected_types.len, probe.len);
    for (probe.command_images[0..probe.len], expected_types, timestamps) |
        command_image,
        command_type,
        timestamp,
    | {
        try std.testing.expectEqual(command_type, command_image.command);
        try std.testing.expectEqual(timestamp, command_image.timestamp);
        try std.testing.expectEqual(cmd.CommandResultCode.success, command_image.result_code);
    }
}

const OrchestratorCommandCallbackProbe = struct {
    sequence: u64 = 0,
    command_image: cmd.OrderCommand = undefined,
    result_code: cmd.CommandResultCode = .new,
    calls: usize = 0,

    fn onResponse(
        context: ?*anyopaque,
        response: *const kernel_mod.CommandResponse,
    ) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.sequence = response.sequence;
        self.command_image = response.command;
        self.result_code = response.result_code;
        self.calls += 1;
    }
};

fn runOrchestratorOrdinaryFacadeScenario(
    java_config: JavaConfig,
    expected_backend: BackendKind,
    uid: i64,
) !void {
    const allocator = std.testing.allocator;
    const timestamps = [_]i64{
        9_979_330_001,
        9_979_330_002,
        9_979_330_003,
        9_979_330_004,
        9_979_330_005,
        9_979_330_006,
        9_979_330_007,
        9_979_330_008,
        9_979_330_009,
        9_979_330_010,
    };

    var orchestrator = try ExchangeOrchestrator.initFromJavaConfig(
        allocator,
        java_config,
    );
    defer orchestrator.deinit();
    try std.testing.expectEqual(expected_backend, orchestrator.backendKind());

    const result_batch = [_]ApiCommand{
        .{ .add_user_with_timestamp = .{
            .uid = uid,
            .timestamp = timestamps[0],
        } },
        .{ .suspend_user_with_timestamp = .{
            .uid = uid,
            .timestamp = timestamps[1],
        } },
        .{ .resume_user_with_timestamp = .{
            .uid = uid,
            .timestamp = timestamps[2],
        } },
    };
    var batch_result = try orchestrator.submitApiCommandsSyncResult(
        .routed,
        result_batch[0..],
    );
    defer batch_result.deinit(allocator);
    try expectSuccessfulOrchestratorApiResult(&batch_result, 3, .resume_user);
    switch (batch_result) {
        .response => |response| try std.testing.expectEqual(
            timestamps[2],
            response.command.timestamp,
        ),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    const empty: [0]ApiCommand = .{};
    try orchestrator.submitApiCommandsSync(.routed, empty[0..]);

    const void_batch = [_]ApiCommand{
        .{ .add_user_with_timestamp = .{
            .uid = uid + 1,
            .timestamp = timestamps[3],
        } },
        .{ .suspend_user_with_timestamp = .{
            .uid = uid + 1,
            .timestamp = timestamps[4],
        } },
    };
    try orchestrator.submitCommandsSync(.routed, void_batch[0..]);

    var flush = try orchestrator.submitCommandsSyncWithNop(.routed, empty[0..]);
    defer flush.deinit(allocator);
    try expectSuccessfulOrchestratorResponse(&flush, 6, .nop);

    const full_response_handle = try orchestrator.submitCommandAsyncFullResponse(
        .routed,
        .{ .add_user_with_timestamp = .{
            .uid = uid + 2,
            .timestamp = timestamps[5],
        } },
    );
    try std.testing.expectEqual(@as(usize, 1), try orchestrator.drainPending());
    var full_response_result = orchestrator.takeApiCommandAsyncResult(full_response_handle) orelse
        return error.MissingAsyncResult;
    defer full_response_result.deinit(allocator);
    try expectSuccessfulOrchestratorApiResult(&full_response_result, 7, .add_user);
    switch (full_response_result) {
        .response => |response| try std.testing.expectEqual(
            timestamps[5],
            response.command.timestamp,
        ),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    try std.testing.expectError(
        error.UnsupportedAsyncFullResponseCommand,
        orchestrator.submitCommandAsyncFullResponse(
            .routed,
            .{
                .persist_state = .{
                    .snapshot_id = 1,
                    .timestamp_ns = timestamps[6],
                },
            },
        ),
    );

    var callback_probe: OrchestratorCommandCallbackProbe = .{};
    const callback: CommandResponseCallback = .{
        .context = &callback_probe,
        .callback = OrchestratorCommandCallbackProbe.onResponse,
    };
    try orchestrator.submitApiCommandWithCallback(
        .routed,
        .{ .add_user_with_timestamp = .{
            .uid = uid + 3,
            .timestamp = timestamps[6],
        } },
        callback,
    );
    try std.testing.expectEqual(@as(usize, 0), callback_probe.calls);
    try std.testing.expectEqual(@as(usize, 1), try orchestrator.drainPending());
    try std.testing.expectEqual(@as(usize, 1), callback_probe.calls);
    try std.testing.expectEqual(@as(u64, 8), callback_probe.sequence);
    try std.testing.expectEqual(cmd.OrderCommandType.add_user, callback_probe.command_image.command);
    try std.testing.expectEqual(uid + 3, callback_probe.command_image.uid);
    try std.testing.expectEqual(timestamps[6], callback_probe.command_image.timestamp);
    try std.testing.expectEqual(@as(i64, -1), callback_probe.command_image.order_id);
    try std.testing.expectEqual(@as(i32, -1), callback_probe.command_image.symbol);
    try std.testing.expectEqual(cmd.CommandResultCode.success, callback_probe.result_code);
    try std.testing.expectEqual(@as(usize, 0), try orchestrator.processPendingCallbacks());

    try std.testing.expectError(
        error.UnsupportedCallbackCommand,
        orchestrator.submitApiCommandWithCallback(.routed, .reset, callback),
    );
    try std.testing.expectError(
        error.UnsupportedCallbackCommand,
        orchestrator.submitApiCommandWithCallback(
            .routed,
            .{ .reset_with_timestamp = .{ .timestamp = timestamps[0] } },
            callback,
        ),
    );
    try std.testing.expectError(
        error.UnsupportedCallbackCommand,
        orchestrator.submitApiCommandWithCallback(.routed, .nop, callback),
    );
    try std.testing.expectError(
        error.UnsupportedCallbackCommand,
        orchestrator.submitApiCommandWithCallback(
            .routed,
            .{ .nop_with_timestamp = .{ .timestamp = timestamps[1] } },
            callback,
        ),
    );

    const nop_handle = try orchestrator.submitCommandAsyncFullResponse(
        .routed,
        .{ .nop_with_timestamp = .{ .timestamp = timestamps[2] } },
    );
    try std.testing.expectEqual(@as(usize, 1), try orchestrator.drainPending());
    var nop_result = orchestrator.takeApiCommandAsyncResult(nop_handle) orelse
        return error.MissingAsyncResult;
    defer nop_result.deinit(allocator);
    try expectSuccessfulOrchestratorApiResult(&nop_result, 9, .nop);
    switch (nop_result) {
        .response => |response| try std.testing.expectEqual(
            timestamps[2],
            response.command.timestamp,
        ),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    if (expected_backend == .single) {
        try std.testing.expectError(
            error.SplitModeRequiresShardedBackend,
            orchestrator.submitApiCommandsSync(.split, empty[0..]),
        );
        try std.testing.expectError(
            error.SplitModeRequiresShardedBackend,
            orchestrator.submitCommandAsyncFullResponse(.split, .nop),
        );
        try std.testing.expectError(
            error.SplitModeRequiresShardedBackend,
            orchestrator.submitApiCommandWithCallback(
                .split,
                .{ .add_user = uid + 4 },
                callback,
            ),
        );
    } else {
        const split_uid = uid + 10;
        const split_batch = [_]ApiCommand{
            .{ .add_user_with_timestamp = .{
                .uid = split_uid,
                .timestamp = timestamps[7],
            } },
        };
        var split_batch_result = try orchestrator.submitApiCommandsSyncResult(
            .split,
            split_batch[0..],
        );
        defer split_batch_result.deinit(allocator);
        try expectSuccessfulOrchestratorApiResult(
            &split_batch_result,
            10,
            .add_user,
        );
        switch (split_batch_result) {
            .response => |response| try std.testing.expectEqual(
                timestamps[7],
                response.command.timestamp,
            ),
            .persist_state => return error.UnexpectedPersistStateResult,
        }

        const split_full_response_handle = try orchestrator.submitCommandAsyncFullResponse(
            .split,
            .{ .suspend_user_with_timestamp = .{
                .uid = split_uid,
                .timestamp = timestamps[8],
            } },
        );
        try std.testing.expectEqual(@as(usize, 1), try orchestrator.drainPending());
        var split_full_response_result =
            orchestrator.takeApiCommandAsyncResult(split_full_response_handle) orelse
            return error.MissingAsyncResult;
        defer split_full_response_result.deinit(allocator);
        try expectSuccessfulOrchestratorApiResult(
            &split_full_response_result,
            11,
            .suspend_user,
        );
        switch (split_full_response_result) {
            .response => |response| try std.testing.expectEqual(
                timestamps[8],
                response.command.timestamp,
            ),
            .persist_state => return error.UnexpectedPersistStateResult,
        }

        var split_callback_probe: OrchestratorCommandCallbackProbe = .{};
        const split_callback: CommandResponseCallback = .{
            .context = &split_callback_probe,
            .callback = OrchestratorCommandCallbackProbe.onResponse,
        };
        try orchestrator.submitApiCommandWithCallback(
            .split,
            .{ .resume_user_with_timestamp = .{
                .uid = split_uid,
                .timestamp = timestamps[9],
            } },
            split_callback,
        );
        try std.testing.expectEqual(@as(usize, 0), split_callback_probe.calls);
        try std.testing.expectEqual(@as(usize, 1), try orchestrator.drainPending());
        try std.testing.expectEqual(@as(usize, 1), split_callback_probe.calls);
        try std.testing.expectEqual(@as(u64, 12), split_callback_probe.sequence);
        try std.testing.expectEqual(
            cmd.OrderCommandType.resume_user,
            split_callback_probe.command_image.command,
        );
        try std.testing.expectEqual(split_uid, split_callback_probe.command_image.uid);
        try std.testing.expectEqual(
            timestamps[9],
            split_callback_probe.command_image.timestamp,
        );
        try std.testing.expectEqual(
            cmd.CommandResultCode.success,
            split_callback_probe.result_code,
        );
        try std.testing.expectEqual(
            @as(usize, 0),
            try orchestrator.processPendingCallbacks(),
        );
    }
}

fn runOrchestratorBatchCallbackPreflightScenario(
    java_config: JavaConfig,
    expected_backend: BackendKind,
    uid: i64,
) !void {
    const allocator = std.testing.allocator;
    var orchestrator = try ExchangeOrchestrator.initFromJavaConfig(
        allocator,
        java_config,
    );
    defer orchestrator.deinit();
    try std.testing.expectEqual(expected_backend, orchestrator.backendKind());

    const empty: [0]ApiCommand = .{};
    const one_command = [_]ApiCommand{.{ .add_user = uid }};
    {
        var callback_scope = runtime_mod.CallbackExecutionScope.enter();
        defer callback_scope.leave();

        try std.testing.expectError(
            error.EmptyApiCommandBatch,
            orchestrator.submitApiCommandsSyncResult(.routed, empty[0..]),
        );
        try orchestrator.submitApiCommandsSync(.routed, empty[0..]);

        try std.testing.expectError(
            error.CoreReentrantCall,
            orchestrator.submitApiCommandsSyncResult(.routed, one_command[0..]),
        );
        try std.testing.expectError(
            error.CoreReentrantCall,
            orchestrator.submitApiCommandsSync(.routed, one_command[0..]),
        );
        try std.testing.expectError(
            error.CoreReentrantCall,
            orchestrator.submitApiCommandsSyncWithNop(.routed, empty[0..]),
        );
        try std.testing.expectError(
            error.CoreReentrantCall,
            orchestrator.submitApiCommandsSyncWithNop(.routed, one_command[0..]),
        );
    }

    try std.testing.expectEqual(@as(usize, 0), try orchestrator.drainPending());
    var first_published = try orchestrator.submitApiCommandSync(.routed, .nop);
    defer first_published.deinit(allocator);
    try expectSuccessfulOrchestratorApiResult(
        &first_published,
        1,
        .nop,
    );
}

test "exchange orchestrator raw full-result observer forwarding covers java-default grouped backend" {
    try runOrchestratorRawResultForwardingScenario(
        config_mod.JavaExchangeConfig.javaDefault(),
        .sharded,
        980_401,
    );
}

test "exchange orchestrator raw full-result observer forwarding covers explicit-immediate single backend" {
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.sharded_pipeline_mode = .immediate;
    try runOrchestratorRawResultForwardingScenario(
        java_config,
        .single,
        980_501,
    );
}

test "exchange orchestrator scalar and unit timestamps cross java-default grouped backend" {
    try runOrchestratorScalarUnitTimestampScenario(
        config_mod.JavaExchangeConfig.javaDefault(),
        .sharded,
        980_601,
    );
}

test "exchange orchestrator scalar and unit timestamps cross explicit-immediate single backend" {
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.sharded_pipeline_mode = .immediate;
    try runOrchestratorScalarUnitTimestampScenario(
        java_config,
        .single,
        980_701,
    );
}

test "exchange orchestrator ordinary facade covers java-default grouped backend" {
    try runOrchestratorOrdinaryFacadeScenario(
        config_mod.JavaExchangeConfig.javaDefault(),
        .sharded,
        980_801,
    );
}

test "exchange orchestrator ordinary facade covers explicit-immediate single backend" {
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.sharded_pipeline_mode = .immediate;
    try runOrchestratorOrdinaryFacadeScenario(
        java_config,
        .single,
        980_901,
    );
}

test "exchange orchestrator batch callback preflight covers java-default grouped backend" {
    try runOrchestratorBatchCallbackPreflightScenario(
        config_mod.JavaExchangeConfig.javaDefault(),
        .sharded,
        981_001,
    );
}

test "exchange orchestrator batch callback preflight covers explicit-immediate single backend" {
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.sharded_pipeline_mode = .immediate;
    try runOrchestratorBatchCallbackPreflightScenario(
        java_config,
        .single,
        981_101,
    );
}

test "exchange orchestrator forwards shutdown state and completion for both backends" {
    const allocator = std.testing.allocator;
    {
        var java_config = config_mod.JavaExchangeConfig.javaDefault();
        java_config.sharded_pipeline_mode = .immediate;
        var orchestrator = try ExchangeOrchestrator.initFromJavaConfig(
            allocator,
            java_config,
        );
        defer orchestrator.deinit();
        try std.testing.expectEqual(BackendKind.single, orchestrator.backendKind());
        try std.testing.expectEqual(ShutdownState.open, orchestrator.shutdownState());
        try orchestrator.startup();
        try std.testing.expectEqual(
            ShutdownOutcome.complete,
            try orchestrator.shutdownTimeout(.none),
        );
        try std.testing.expectEqual(ShutdownState.complete, orchestrator.shutdownState());
    }

    {
        var java_config = config_mod.JavaExchangeConfig.javaDefault();
        java_config.performance_cfg.matching_engines_num = 2;
        java_config.performance_cfg.risk_engines_num = 2;
        var orchestrator = try ExchangeOrchestrator.initFromJavaConfig(
            allocator,
            java_config,
        );
        defer orchestrator.deinit();
        try std.testing.expectEqual(BackendKind.sharded, orchestrator.backendKind());
        try std.testing.expectEqual(ShutdownState.open, orchestrator.shutdownState());
        try orchestrator.startup();
        try std.testing.expectEqual(
            ShutdownOutcome.complete,
            try orchestrator.shutdownTimeout(.none),
        );
        try std.testing.expectEqual(ShutdownState.complete, orchestrator.shutdownState());
    }
}

test "exchange orchestrator selects grouped sharded backend for java default profile" {
    const allocator = std.testing.allocator;
    var orchestrator = try ExchangeOrchestrator.initFromJavaConfig(
        allocator,
        config_mod.JavaExchangeConfig.javaDefault(),
    );
    defer orchestrator.deinit();

    try std.testing.expectEqual(BackendKind.sharded, orchestrator.backendKind());
    try std.testing.expectEqual(@as(usize, 1), orchestrator.shardCount());
    try orchestrator.startup();
    switch (orchestrator.backend) {
        .single => unreachable,
        .sharded => |sharded| try std.testing.expect(
            sharded.persistent_grouped_runtime != null,
        ),
    }
}

test "exchange orchestrator explicit immediate mode selects 1x1 single backend" {
    const allocator = std.testing.allocator;
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.sharded_pipeline_mode = .immediate;
    var orchestrator = try ExchangeOrchestrator.initFromJavaConfig(
        allocator,
        java_config,
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
    try std.testing.expectEqual(@as(usize, 4), orchestrator.matchingShardCount());
    try std.testing.expectEqual(@as(usize, 2), orchestrator.riskShardCount());
    try std.testing.expectEqual(@as(u64, 3), orchestrator.matchingShardMask());
    try std.testing.expectEqual(@as(u64, 1), orchestrator.riskShardMask());
}

test "exchange orchestrator backend selection treats engine families independently" {
    try std.testing.expectEqual(
        BackendKind.single,
        backendKindForPerformance(.{
            .matching_engines_num = 1,
            .risk_engines_num = 1,
        }),
    );
    try std.testing.expectEqual(
        BackendKind.sharded,
        backendKindForPerformance(.{
            .matching_engines_num = 4,
            .risk_engines_num = 2,
        }),
    );
    try std.testing.expectEqual(
        BackendKind.sharded,
        backendKindForPerformance(.{
            .matching_engines_num = 1,
            .risk_engines_num = 4,
        }),
    );
}

test "exchange orchestrator routes addUser through selected backend" {
    const allocator = std.testing.allocator;

    var single = try ExchangeOrchestrator.initFromJavaConfig(allocator, .{
        .sharded_pipeline_mode = .immediate,
    });
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
    var orchestrator = try ExchangeOrchestrator.initFromJavaConfig(allocator, .{
        .sharded_pipeline_mode = .immediate,
    });
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

test "exchange orchestrator sharded backend progresses automatically and exposes lifecycle state" {
    const allocator = std.testing.allocator;
    var orchestrator = try ExchangeOrchestrator.initFromJavaConfig(
        allocator,
        .{
            .performance_cfg = config_mod.PerformanceConfig.throughputPreset(),
            .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
        },
    );
    defer orchestrator.deinit();
    try orchestrator.startup();

    const handle = try orchestrator.submitApiCommandAsync(
        .split,
        .{ .add_user = 980_251 },
    );
    var barrier = try orchestrator.queryReport(.{ .single_user = 980_251 });
    defer barrier.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        barrier.single_user.query_execution_status,
    );

    var result = orchestrator.takeApiCommandAsyncResult(handle) orelse return error.MissingAsyncResult;
    defer result.deinit(allocator);
    switch (result) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }
    try std.testing.expectEqual(@as(?CoreFatalInfo, null), orchestrator.fatalInfo());
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
        drained_total: usize = 0,
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
            var publication_digest: u64 = 0x51A7E5EED51A7E5E ^ scenario.seed;
            var prng = std.Random.DefaultPrng.init(scenario.seed);
            const random = prng.random();
            const base_uid: i64 = 2_100_000 + @as(i64, @intCast(scenario.seed % 30_000));

            for (0..rounds) |round_idx| {
                const uid_offset = random.uintLessThan(usize, user_pool);
                const uid = base_uid + @as(i64, @intCast(uid_offset));
                try orchestrator.createUserWithCallback(.routed, uid, callback);
                outcome.registered += 1;
                publication_digest = (publication_digest ^ @as(u64, @intCast(uid_offset))) *% 0x9E3779B185EBCA87;
                publication_digest +%= 0x632BE59BD9B4E019;
                _ = round_idx;
            }

            // The sharded lifecycle owns progress now. One terminal drain is
            // an execution barrier for the entire accepted callback prefix;
            // intermediate drain/process counts are scheduler observations,
            // so they must not participate in a deterministic digest.
            outcome.drained_total = try orchestrator.drainPending();

            if (collector.processed != outcome.registered) {
                return error.CallbackCompletionMismatch;
            }
            try std.testing.expectEqual(@as(usize, 0), try orchestrator.processPendingCallbacks());

            outcome.success_results = collector.success_results;
            outcome.duplicate_results = collector.duplicate_results;
            outcome.digest = collector.digest ^ std.math.rotl(u64, publication_digest, 23);
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

    var source = try exchange_api_sharded_mod.ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
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
    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try source.persistStateWithDefaultMainLog(
            tmp.dir,
            901,
            901_000_000,
            .matching_engine_router,
            0,
        ),
    );

    const main_log_path = try source.defaultPersistenceMainLogPath(allocator);
    defer allocator.free(main_log_path);
    var main_log_file = try tmp.dir.createFile(io_context.current(), main_log_path, .{ .truncate = false });
    defer main_log_file.close(io_context.current());
    try io_context.appendAll(main_log_file, "seeded-malformed-main-log-entry\n");

    var target = try ExchangeOrchestrator.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
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

    var source = try ExchangeOrchestrator.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        .{
            .sharded_pipeline_mode = .immediate,
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

    var target = try ExchangeOrchestrator.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        .{
            .sharded_pipeline_mode = .immediate,
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

test "exchange orchestrator grouped default fails closed on XCM1 while immediate restores it" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const exchange_id = "ECORCH_XCM1_PROMOTION";
    const existing_uid: i64 = 981_411;
    const snapshot_id: i64 = 941;

    var source = try ExchangeOrchestrator.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        .{
            .sharded_pipeline_mode = .immediate,
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
    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try source.persistStateWithDefaultMainLog(
            tmp.dir,
            snapshot_id,
            941_000_000,
            .matching_engine_router,
            0,
        ),
    );
    source.shutdown();

    const restore_state = config_mod.InitialStateConfig.fromSnapshotOnly(
        exchange_id,
        1,
        0,
    );
    var grouped = try ExchangeOrchestrator.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        .{
            .init_state_cfg = restore_state,
            .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
        },
    );
    defer grouped.deinit();
    try std.testing.expectEqual(BackendKind.sharded, grouped.backendKind());
    try std.testing.expectError(
        error.LegacyXcm1RequiresImmediateOrMigration,
        grouped.startupFromDiscoveredConfiguredPersistence(
            tmp.dir,
            .matching_engine_router,
            0,
            0,
        ),
    );

    var immediate = try ExchangeOrchestrator.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        .{
            .sharded_pipeline_mode = .immediate,
            .init_state_cfg = restore_state,
            .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
        },
    );
    defer immediate.deinit();
    try std.testing.expectEqual(BackendKind.single, immediate.backendKind());
    const outcome = try immediate.startupFromDiscoveredConfiguredPersistence(
        tmp.dir,
        .matching_engine_router,
        0,
        0,
    );
    try std.testing.expect(outcome.snapshot_requested);
    try std.testing.expect(outcome.snapshot_found);
    var duplicate_add = try immediate.addUser(.routed, existing_uid);
    defer duplicate_add.deinit(allocator);
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_user_already_exists,
        duplicate_add.result_code,
    );
}

test "exchange orchestrator sharded persistStateWithDefaultMainLog writes default-path artifacts deterministically" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const seed: u64 = 0x5A4DDE05;
    var exchange_id_buf: [32]u8 = undefined;
    const exchange_id = try std.fmt.bufPrint(&exchange_id_buf, "ECORCH{x}", .{seed});

    var orchestrator = try ExchangeOrchestrator.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
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
    var file = try tmp.dir.openFile(io_context.current(), main_log_path, .{});
    defer file.close(io_context.current());
    const contents = try io_context.readToEndAlloc(file, allocator, 4096);
    defer allocator.free(contents);
    try std.testing.expect(std.mem.indexOf(u8, contents, "snapshotId=7001") != null);
    try std.testing.expect(std.mem.indexOf(u8, contents, "type=ME") != null);
    try std.testing.expect(std.mem.indexOf(u8, contents, "instance=0") != null);

    var discovered = try ExchangeOrchestrator.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        .{
            .performance_cfg = config_mod.PerformanceConfig.throughputPreset(),
            .init_state_cfg = config_mod.InitialStateConfig.fromSnapshotOnly(exchange_id, 1, 0),
            .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
        },
    );
    defer discovered.deinit();
    const discovered_outcome = try discovered.startupFromDiscoveredConfiguredPersistence(
        tmp.dir,
        .matching_engine_router,
        0,
        0,
    );
    try std.testing.expect(discovered_outcome.snapshot_requested);
    try std.testing.expect(discovered_outcome.snapshot_found);
    var duplicate_add = try discovered.addUser(.routed, 991_100);
    defer duplicate_add.deinit(allocator);
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_user_already_exists,
        duplicate_add.result_code,
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
                    .sharded_pipeline_mode = .immediate,
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

        fn persistSnapshotToDefaultPath(
            source: *ExchangeOrchestrator,
            dir: std.Io.Dir,
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
                .sharded => {
                    const result = try source.persistStateWithDefaultMainLog(
                        dir,
                        @intCast(snapshot_id),
                        @intCast(snapshot_id),
                        module_type,
                        0,
                    );
                    try std.testing.expectEqual(cmd.CommandResultCode.success, result);
                },
            }
        }

        fn appendMalformedMainLog(
            alloc: std.mem.Allocator,
            source: *ExchangeOrchestrator,
            dir: std.Io.Dir,
        ) !void {
            const path = switch (source.backend) {
                .single => |*single| try single.defaultPersistenceMainLogPath(alloc),
                .sharded => |*sharded| try sharded.defaultPersistenceMainLogPath(alloc),
            };
            defer alloc.free(path);

            var file = try dir.createFile(io_context.current(), path, .{ .truncate = false });
            defer file.close(io_context.current());
            try io_context.appendAll(file, "seeded-malformed-main-log-entry\n");
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

            var source = try ExchangeOrchestrator.initFromJavaConfigWithPersistenceRoot(
                alloc,
                tmp.dir,
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
            try persistSnapshotToDefaultPath(&source, tmp.dir, snapshot_id, module_type);

            if (scenario.perturbation == .malformed_main_log) {
                try appendMalformedMainLog(alloc, &source, tmp.dir);
            }

            const target_init_state = switch (scenario.startup_mode) {
                .configured => config_mod.InitialStateConfig.fromSnapshotOnly(exchange_id, snapshot_id, 0),
                .discovered, .discovered_strict_main_log => config_mod.InitialStateConfig.fromSnapshotOnly(exchange_id, 1, 0),
            };
            var target = try ExchangeOrchestrator.initFromJavaConfigWithPersistenceRoot(
                alloc,
                tmp.dir,
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
                    .sharded_pipeline_mode = .immediate,
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

        fn persistSnapshotToDefaultPath(
            source: *ExchangeOrchestrator,
            dir: std.Io.Dir,
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
                .sharded => {
                    const result = try source.persistStateWithDefaultMainLog(
                        dir,
                        @intCast(snapshot_id),
                        @intCast(snapshot_id),
                        module_type,
                        0,
                    );
                    try std.testing.expectEqual(cmd.CommandResultCode.success, result);
                },
            }
        }

        fn writeJournalPartitionZero(
            source: *ExchangeOrchestrator,
            dir: std.Io.Dir,
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
            dir: std.Io.Dir,
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
            dir: std.Io.Dir,
            snapshot_id: u64,
            module_type: PersistenceModuleType,
            instance_id: u32,
            replay_base_seq: u64,
        ) !void {
            const main_log_path = try defaultMainLogPath(alloc, source);
            defer alloc.free(main_log_path);

            var file = try dir.createFile(io_context.current(), main_log_path, .{ .truncate = true });
            defer file.close(io_context.current());

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
            try file.writeStreamingAll(io_context.current(), line);
        }

        fn applyPerturbation(
            alloc: std.mem.Allocator,
            source: *ExchangeOrchestrator,
            dir: std.Io.Dir,
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

            var source = try ExchangeOrchestrator.initFromJavaConfigWithPersistenceRoot(
                alloc,
                tmp.dir,
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

            try persistSnapshotToDefaultPath(&source, tmp.dir, snapshot_id, module_type);

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

            var target = try ExchangeOrchestrator.initFromJavaConfigWithPersistenceRoot(
                alloc,
                tmp.dir,
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
                    .sharded_pipeline_mode = .immediate,
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
            dir: std.Io.Dir,
        ) !void {
            const path = try defaultMainLogPath(alloc, source);
            defer alloc.free(path);

            var file = try dir.createFile(io_context.current(), path, .{ .truncate = false });
            defer file.close(io_context.current());
            try io_context.appendAll(file, "seeded-malformed-main-log-entry\n");
        }

        fn persistSnapshotToDefaultPath(
            source: *ExchangeOrchestrator,
            dir: std.Io.Dir,
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
                .sharded => {
                    const result = try source.persistStateWithDefaultMainLog(
                        dir,
                        @intCast(snapshot_id),
                        @intCast(snapshot_id),
                        module_type,
                        0,
                    );
                    try std.testing.expectEqual(cmd.CommandResultCode.success, result);
                },
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

                var source = try ExchangeOrchestrator.initFromJavaConfigWithPersistenceRoot(
                    alloc,
                    tmp.dir,
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
                try persistSnapshotToDefaultPath(
                    &source,
                    tmp.dir,
                    latest_snapshot_id,
                    module_type,
                );

                if (cycle == injection_cycle) {
                    try appendMalformedMainLog(alloc, &source, tmp.dir);
                    malformed_active = true;
                }

                var configured_probe = try ExchangeOrchestrator.initFromJavaConfigWithPersistenceRoot(
                    alloc,
                    tmp.dir,
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

                var discovered_probe = try ExchangeOrchestrator.initFromJavaConfigWithPersistenceRoot(
                    alloc,
                    tmp.dir,
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

                var strict_probe = try ExchangeOrchestrator.initFromJavaConfigWithPersistenceRoot(
                    alloc,
                    tmp.dir,
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

            var final_target = try ExchangeOrchestrator.initFromJavaConfigWithPersistenceRoot(
                alloc,
                tmp.dir,
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
                    .sharded_pipeline_mode = .immediate,
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
