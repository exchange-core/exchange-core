const std = @import("std");
const io_context = @import("io_context.zig");
const cmd = @import("cmd.zig");
const common = @import("common.zig");
const config_mod = @import("config.zig");
const api_command_mod = @import("api_command.zig");
const binary_frames = @import("binary_frames.zig");
const binary_payload = @import("binary_payload.zig");
const exchange_core_mod = @import("exchange_core.zig");
const journal_mod = @import("journal.zig");
const kernel_mod = @import("exchange_kernel.zig");
const orderbook_naive = @import("orderbook_naive.zig");
const persistence_mod = @import("persistence_descriptors.zig");
const persistence_paths = @import("persistence_paths.zig");
const runtime_mod = @import("exchange_runtime.zig");
const sharded_core_mod = @import("exchange_core_sharded.zig");
const simple_events_mod = @import("simple_events_processor.zig");
const snapshot_mod = @import("snapshot.zig");
const risk = @import("risk.zig");
const log = std.log.scoped(.exchange_api_sharded);

pub const ExecutionMode = enum {
    routed,
    split,
};

const DescriptorInitError = persistence_mod.CreateSnapshotError || error{
    MatchingEngineCountOverflow,
};
const DescriptorTrackingError = persistence_mod.CreateSnapshotError || error{
    InvalidPersistSnapshotId,
    InvalidPersistTimestamp,
};
pub const InitError = sharded_core_mod.InitError || DescriptorInitError;
pub const InitFromJavaConfigError = sharded_core_mod.InitFromJavaConfigError || config_mod.PerformanceConfig.MappingError || DescriptorInitError;
pub const LifecycleError = sharded_core_mod.LifecycleError;
pub const ExecuteError = sharded_core_mod.ExecuteError || std.mem.Allocator.Error || DescriptorTrackingError;
pub const ReportError = sharded_core_mod.ReportError;
pub const AggregateError = sharded_core_mod.AggregateError;
pub const QueryReportError = sharded_core_mod.QueryReportError;
pub const Config = config_mod.ExchangeCoreConfig;
pub const BinaryReportQuery = binary_payload.DecodedReportQuery;
pub const BinaryCurrencyBalance = binary_payload.CurrencyBalance;
pub const BinaryUserAccountsBatch = binary_payload.UserAccountsBatch;
pub const PersistenceModuleType = persistence_paths.ModuleType;
pub const PersistenceFileNameError = persistence_paths.FileNameError;
pub const PersistencePathError = persistence_paths.PathError;
pub const SnapshotRef = exchange_core_mod.SnapshotRef;
pub const JournalRef = exchange_core_mod.JournalRef;
pub const SnapshotDescriptor = exchange_core_mod.SnapshotDescriptor;
pub const JournalDescriptor = exchange_core_mod.JournalDescriptor;
pub const BootstrapOutcome = exchange_core_mod.BootstrapOutcome;
pub const SimpleEventsHandler = simple_events_mod.EventsHandler;
pub const ApiCommand = api_command_mod.ApiCommand;
pub const ApiCommandResult = api_command_mod.ApiCommandResult;
pub const ApiCommandAsyncHandle = api_command_mod.ApiCommandAsyncHandle;
pub const BatchResponse = kernel_mod.BatchResponse;
pub const ResultSink = runtime_mod.ResultSink;
pub const RuntimeDrainOutcome = runtime_mod.DrainOutcome;
pub const UnsupportedShardedPersistenceError = error{
    UnsupportedShardedPersistence,
};
pub const BootstrapError = JournalReplayError || snapshot_mod.SnapshotIoError || snapshot_mod.SnapshotRestoreError || error{
    SnapshotPathRequired,
    SnapshotFileNotFound,
    JournalPathRequired,
};
pub const StartupError = LifecycleError || BootstrapError;
pub const StartupFromConfiguredPersistenceError = StartupError || PersistencePathError || error{
    JournalReplayCountOverflow,
};
const MainLogSequenceParseError = error{
    MalformedMainLogEntry,
};
pub const StartupFromDiscoveredConfiguredPersistenceError = StartupFromConfiguredPersistenceError || std.Io.Dir.OpenError || std.Io.Dir.Iterator.Error;
pub const StartupFromDiscoveredConfiguredPersistenceStrictError = StartupFromDiscoveredConfiguredPersistenceError || MainLogSequenceParseError;
pub const JournalApiError = error{
    JournalingDisabled,
} || journal_mod.JournalIoError;
pub const JournalReplayError = JournalApiError || ExecuteError || error{
    SequenceMismatch,
    ResultMismatch,
};
pub const SnapshotWriteError = snapshot_mod.SnapshotCaptureError || snapshot_mod.SnapshotIoError;
pub const SnapshotValidateError = snapshot_mod.SnapshotValidationError || snapshot_mod.SnapshotIoError;
const StorageFolderEnsureError = std.Io.Dir.CreateDirPathError;
const MainLogWriteError = std.Io.File.OpenError || io_context.AppendAllError || std.fmt.BufPrintError;
pub const DefaultJournalApiError = JournalApiError || PersistencePathError || StorageFolderEnsureError;
pub const DefaultJournalReplayError = JournalReplayError || PersistencePathError;
pub const DefaultJournalReplayRangeError = DefaultJournalReplayError || error{
    JournalReplayCountOverflow,
};
pub const DefaultSnapshotWriteError = SnapshotWriteError || PersistencePathError || StorageFolderEnsureError || MainLogWriteError;
pub const DefaultSnapshotValidateError = SnapshotValidateError || PersistencePathError;
pub const PersistStateWithMainLogError = PersistStateError || PersistencePathError || StorageFolderEnsureError || MainLogWriteError || error{
    PersistStateRejected,
    PersistStateSnapshotMismatch,
    PersistStateSnapshotIdOverflow,
    PersistStateTimestampOverflow,
};
pub const SnapshotExistsError = std.Io.Dir.StatFileError;
pub const DefaultSnapshotExistsError = SnapshotExistsError || PersistencePathError;
pub const QueryTypedReportError = QueryReportError || error{
    UnexpectedReportResponseType,
};
pub const HelperError = error{
    OrderBookDepthOverflow,
};
pub const AsyncSubmitError = LifecycleError || std.mem.Allocator.Error || error{
    RequestIdOverflow,
};
pub const DrainPendingError = ExecuteError || std.mem.Allocator.Error;
pub const ProcessCommandResultCodeError = AsyncSubmitError || DrainPendingError || error{
    MissingAsyncResult,
};
pub const ProcessCommandFullResponseError = SubmitApiCommandAsyncError || DrainPendingError || error{
    MissingAsyncResult,
    ExpectedResponseResult,
};
pub const PersistStateError = AsyncSubmitError || DrainPendingError || error{
    MissingAsyncResult,
};
pub const TakeOrderBookSnapshotError = error{
    MissingOrderBookSnapshot,
};
pub const TakeReportResponseError = error{
    MissingReportResponse,
};
pub const TakeTypedReportError = TakeReportResponseError || error{
    UnexpectedReportResponseType,
};
pub const ProcessReportError = BinaryReportQueryAsyncSubmitError || DrainPendingError || TakeReportResponseError || error{
    MissingAsyncResult,
};
pub const ProcessTypedReportError = BinaryReportQueryAsyncSubmitError || DrainPendingError || TakeTypedReportError || error{
    MissingAsyncResult,
};
pub const ProcessOrderBookError = (AsyncSubmitError || HelperError || DrainPendingError || TakeOrderBookSnapshotError || error{
    MissingAsyncResult,
});
pub const BinaryPayloadSubmitError = ExecuteError || error{
    SplitBinaryPayloadUnsupported,
    BinaryTransferShardMismatch,
    InvalidBinaryCommandType,
    EmptyBinaryPayload,
    BinaryPayloadSizeOverflow,
};
pub const BinaryPayloadAsyncSubmitError = AsyncSubmitError || error{
    SplitBinaryPayloadUnsupported,
    BinaryTransferShardMismatch,
    InvalidBinaryCommandType,
    EmptyBinaryPayload,
    BinaryPayloadSizeOverflow,
};
pub const BinaryPayloadTypedSubmitError = BinaryPayloadSubmitError || binary_payload.EncodeError;
pub const BinaryPayloadTypedAsyncSubmitError = BinaryPayloadAsyncSubmitError || binary_payload.EncodeError;
pub const BinaryReportQuerySubmitError = BinaryPayloadSubmitError || std.mem.Allocator.Error;
pub const BinaryReportQueryAsyncSubmitError = BinaryPayloadAsyncSubmitError || std.mem.Allocator.Error;
pub const SubmitBinaryPayloadAsyncError = BinaryPayloadAsyncSubmitError;
pub const SubmitApiCommandError = ExecuteError ||
    HelperError ||
    PersistStateError ||
    BinaryPayloadTypedSubmitError ||
    BinaryReportQuerySubmitError;
pub const SubmitApiCommandAsyncError = AsyncSubmitError ||
    HelperError ||
    BinaryPayloadTypedAsyncSubmitError ||
    BinaryReportQueryAsyncSubmitError;
pub const SubmitApiCommandBatchError = SubmitApiCommandAsyncError ||
    DrainPendingError ||
    std.mem.Allocator.Error ||
    error{
        EmptyApiCommandBatch,
        MissingBatchResult,
        UnexpectedFlushResultVariant,
    };
pub const EnqueueRuntimeError = LifecycleError || std.mem.Allocator.Error;
pub const RuntimeDrainError = ExecuteError || std.mem.Allocator.Error;
pub const PersistStateRequestPair = api_command_mod.PersistStateRequestPair;
pub const ProcessResultError = error{
    UnexpectedPersistStateResult,
};
pub const CallbackRegistrationError = error{
    UnsupportedPersistStateHandle,
    CallbackRegistryFull,
};
pub const SubmitCommandWithCallbackError = SubmitApiCommandAsyncError || CallbackRegistrationError;
pub const ProcessRegisteredCallbackError = ProcessResultError || CallbackRegistrationError;

pub const CommandResponseCallback = struct {
    context: ?*anyopaque = null,
    callback: *const fn (?*anyopaque, *const kernel_mod.CommandResponse) void,
};

const MaxCallbackRegistrations = 2048;
const CallbackRegistration = struct {
    request_id: u64,
    callback: CommandResponseCallback,
};

const PendingAsyncCommand = struct {
    request_id: u64,
    mode: ExecutionMode,
    command: cmd.OrderCommand,
};

const BinaryFrameLayout = struct {
    words_without_padding: usize,
    frame_count: usize,
};

pub const ExchangeApiSharded = struct {
    allocator: std.mem.Allocator,
    config: Config,
    core: sharded_core_mod.ShardedExchangeCore,
    descriptors: persistence_mod.DescriptorStore,
    current_snapshot_ref: SnapshotRef,
    journal: ?journal_mod.CommandJournal = null,
    next_request_id: u64 = 0,
    pending_queue: std.ArrayListUnmanaged(PendingAsyncCommand) = .empty,
    ready_results: std.AutoHashMapUnmanaged(u64, kernel_mod.CommandResponse) = .empty,
    runtime_pending: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty,
    runtime_sink: runtime_mod.ResultSink = .{},
    simple_events_processor: ?simple_events_mod.SimpleEventsProcessor = null,
    callback_registrations_len: usize = 0,
    callback_registrations: [MaxCallbackRegistrations]CallbackRegistration = undefined,

    pub fn init(
        allocator: std.mem.Allocator,
        shard_count: usize,
        orderbook_kind: kernel_mod.OrderBookKind,
    ) InitError!ExchangeApiSharded {
        var core_config = Config.javaDefault();
        core_config.matching.orderbook_kind = orderbook_kind;
        core_config.matching.num_shards = @intCast(shard_count);
        const matching_engine_count = std.math.cast(u32, shard_count) orelse
            return error.MatchingEngineCountOverflow;

        var descriptors = persistence_mod.DescriptorStore.init(allocator);
        errdefer descriptors.deinit();
        const root_snapshot_ref = try descriptors.createEmptySnapshot(
            matching_engine_count,
            1,
        );
        const journal = if (core_config.serialization.enable_journaling)
            journal_mod.CommandJournal.init(allocator)
        else
            null;

        return .{
            .allocator = allocator,
            .config = core_config,
            .core = try sharded_core_mod.ShardedExchangeCore.init(
                allocator,
                shard_count,
                orderbook_kind,
            ),
            .descriptors = descriptors,
            .current_snapshot_ref = root_snapshot_ref,
            .journal = journal,
        };
    }

    pub fn initFromJavaConfig(
        allocator: std.mem.Allocator,
        java_config: config_mod.JavaExchangeConfig,
    ) InitFromJavaConfigError!ExchangeApiSharded {
        const core_config = try java_config.toExchangeCoreConfig();
        const matching_engine_count = std.math.cast(u32, core_config.matching.num_shards) orelse
            return error.MatchingEngineCountOverflow;

        var descriptors = persistence_mod.DescriptorStore.init(allocator);
        errdefer descriptors.deinit();
        const root_snapshot_ref = try descriptors.createEmptySnapshot(
            matching_engine_count,
            1,
        );
        const journal = if (core_config.serialization.enable_journaling)
            journal_mod.CommandJournal.init(allocator)
        else
            null;

        return .{
            .allocator = allocator,
            .config = core_config,
            .core = try sharded_core_mod.ShardedExchangeCore.initFromJavaConfig(
                allocator,
                java_config,
            ),
            .descriptors = descriptors,
            .current_snapshot_ref = root_snapshot_ref,
            .journal = journal,
        };
    }

    pub fn deinit(self: *ExchangeApiSharded) void {
        self.pending_queue.deinit(self.allocator);
        self.runtime_pending.deinit(self.allocator);
        self.clearSimpleEventsHandler();

        var ready_it = self.ready_results.valueIterator();
        while (ready_it.next()) |response| {
            response.deinit(self.allocator);
        }
        self.ready_results.deinit(self.allocator);
        self.callback_registrations_len = 0;
        if (self.journal) |*journal| {
            journal.deinit();
            self.journal = null;
        }

        self.descriptors.deinit();
        self.core.deinit();
        self.* = undefined;
    }

    pub fn shardCount(self: *const ExchangeApiSharded) usize {
        return self.core.shardCount();
    }

    pub fn configRef(self: *const ExchangeApiSharded) *const Config {
        return &self.config;
    }

    pub fn journalingEnabled(self: *const ExchangeApiSharded) bool {
        return self.journal != null;
    }

    pub fn journalLen(self: *const ExchangeApiSharded) usize {
        if (self.journal) |journal| {
            return journal.len();
        }
        return 0;
    }

    pub fn kernelRef(self: *ExchangeApiSharded) *kernel_mod.ExchangeKernel {
        std.debug.assert(self.core.kernels.kernels.len > 0);
        return &self.core.kernels.kernels[0];
    }

    pub fn persistenceSnapshotCount(self: *const ExchangeApiSharded) usize {
        return self.descriptors.snapshotCount();
    }

    pub fn persistenceJournalCount(self: *const ExchangeApiSharded) usize {
        return self.descriptors.journalCount();
    }

    pub fn persistenceCurrentSnapshot(self: *const ExchangeApiSharded) *const SnapshotDescriptor {
        return self.descriptors.snapshotConst(self.current_snapshot_ref);
    }

    pub fn persistenceCurrentSnapshotRef(self: *const ExchangeApiSharded) SnapshotRef {
        return self.current_snapshot_ref;
    }

    pub fn persistenceJournalByRef(
        self: *const ExchangeApiSharded,
        journal_ref: JournalRef,
    ) *const JournalDescriptor {
        return self.descriptors.journalConst(journal_ref);
    }

    pub fn persistenceFindJournalForSequence(
        self: *const ExchangeApiSharded,
        sequence: u64,
    ) ?JournalRef {
        return self.descriptors.findJournalForSequence(
            self.current_snapshot_ref,
            sequence,
        );
    }

    pub fn persistenceFindSnapshotById(
        self: *const ExchangeApiSharded,
        snapshot_id: u64,
    ) ?SnapshotRef {
        return self.descriptors.findSnapshotById(snapshot_id);
    }

    pub fn persistenceSnapshotById(
        self: *const ExchangeApiSharded,
        snapshot_id: u64,
    ) ?*const SnapshotDescriptor {
        const snapshot_ref = self.persistenceFindSnapshotById(snapshot_id) orelse
            return null;
        return self.descriptors.snapshotConst(snapshot_ref);
    }

    pub fn defaultPersistenceSnapshotFileName(
        self: *const ExchangeApiSharded,
        allocator: std.mem.Allocator,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) PersistenceFileNameError![]u8 {
        return persistence_paths.snapshotFileName(
            allocator,
            self.config.initial_state.exchange_id,
            snapshot_id,
            module_type,
            instance_id,
        );
    }

    pub fn defaultPersistenceJournalFileName(
        self: *const ExchangeApiSharded,
        allocator: std.mem.Allocator,
        snapshot_id: u64,
        partition_id: u16,
    ) PersistenceFileNameError![]u8 {
        return persistence_paths.journalFileName(
            allocator,
            self.config.initial_state.exchange_id,
            snapshot_id,
            partition_id,
        );
    }

    pub fn defaultPersistenceMainLogFileName(
        self: *const ExchangeApiSharded,
        allocator: std.mem.Allocator,
    ) PersistenceFileNameError![]u8 {
        return persistence_paths.mainLogFileName(
            allocator,
            self.config.initial_state.exchange_id,
        );
    }

    pub fn defaultPersistenceSnapshotPath(
        self: *const ExchangeApiSharded,
        allocator: std.mem.Allocator,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) PersistencePathError![]u8 {
        return persistence_paths.snapshotPath(
            allocator,
            self.config.serialization.storage_folder,
            self.config.initial_state.exchange_id,
            snapshot_id,
            module_type,
            instance_id,
        );
    }

    pub fn defaultPersistenceJournalPath(
        self: *const ExchangeApiSharded,
        allocator: std.mem.Allocator,
        snapshot_id: u64,
        partition_id: u16,
    ) PersistencePathError![]u8 {
        return persistence_paths.journalPath(
            allocator,
            self.config.serialization.storage_folder,
            self.config.initial_state.exchange_id,
            snapshot_id,
            partition_id,
        );
    }

    pub fn defaultPersistenceMainLogPath(
        self: *const ExchangeApiSharded,
        allocator: std.mem.Allocator,
    ) PersistencePathError![]u8 {
        return persistence_paths.mainLogPath(
            allocator,
            self.config.serialization.storage_folder,
            self.config.initial_state.exchange_id,
        );
    }

    pub fn isStarted(self: *const ExchangeApiSharded) bool {
        return self.core.isStarted();
    }

    pub fn isStopped(self: *const ExchangeApiSharded) bool {
        return self.core.isStopped();
    }

    pub fn startup(self: *ExchangeApiSharded) LifecycleError!void {
        try self.core.startup();
    }

    pub fn shutdown(self: *ExchangeApiSharded) void {
        self.core.shutdown();
    }

    pub fn bootstrapFromPersistence(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_path: ?[]const u8,
        journal_path: ?[]const u8,
    ) BootstrapError!BootstrapOutcome {
        var outcome: BootstrapOutcome = .{};
        try self.bootstrapSnapshotPhase(dir, snapshot_path, &outcome);

        const init_state = self.config.initial_state;
        if (init_state.journal_timestamp_ns != 0) {
            const path = journal_path orelse return error.JournalPathRequired;
            outcome.journal_replayed_commands = try self.replayJournalFromFile(dir, path);
        }

        return outcome;
    }

    pub fn startupFromPersistence(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_path: ?[]const u8,
        journal_path: ?[]const u8,
    ) StartupError!BootstrapOutcome {
        try self.startup();
        return self.bootstrapFromPersistence(dir, snapshot_path, journal_path);
    }

    pub fn startupFromConfiguredPersistence(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_module_type: PersistenceModuleType,
        snapshot_instance_id: u32,
        journal_partition_id: u16,
    ) StartupFromConfiguredPersistenceError!BootstrapOutcome {
        var snapshot_path: ?[]u8 = null;
        defer if (snapshot_path) |path| self.allocator.free(path);

        var journal_path: ?[]u8 = null;
        defer if (journal_path) |path| self.allocator.free(path);

        const initial_state = self.config.initial_state;
        if (initial_state.fromSnapshot()) {
            snapshot_path = try self.defaultPersistenceSnapshotPath(
                self.allocator,
                initial_state.snapshot_id,
                snapshot_module_type,
                snapshot_instance_id,
            );
        }
        if (initial_state.journal_timestamp_ns != 0) {
            journal_path = try self.defaultPersistenceJournalPath(
                self.allocator,
                initial_state.snapshot_id,
                journal_partition_id,
            );
        }

        try self.startup();
        var outcome: BootstrapOutcome = .{};
        try self.bootstrapSnapshotPhase(dir, snapshot_path, &outcome);

        var first_partition_found = false;
        if (initial_state.journal_timestamp_ns != 0) {
            const path = journal_path orelse return error.JournalPathRequired;
            first_partition_found = true;
            outcome.journal_replayed_commands = self.replayJournalFromFile(dir, path) catch |err| switch (err) {
                error.FileNotFound => blk: {
                    first_partition_found = false;
                    break :blk 0;
                },
                else => return err,
            };
        }

        if (first_partition_found and journal_partition_id < std.math.maxInt(u16)) {
            const replayed_tail = try self.replayJournalFromDefaultPathRange(
                dir,
                initial_state.snapshot_id,
                journal_partition_id + 1,
            );
            outcome.journal_replayed_commands = std.math.add(
                usize,
                outcome.journal_replayed_commands,
                replayed_tail,
            ) catch return error.JournalReplayCountOverflow;
        }

        return outcome;
    }

    pub fn startupFromDiscoveredConfiguredPersistence(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_module_type: PersistenceModuleType,
        snapshot_instance_id: u32,
        journal_partition_id: u16,
    ) StartupFromDiscoveredConfiguredPersistenceError!BootstrapOutcome {
        const discovered_snapshot_id = try self.discoverLatestDefaultSnapshotId(
            dir,
            snapshot_module_type,
            snapshot_instance_id,
        );
        if (discovered_snapshot_id) |snapshot_id| {
            return self.startupFromConfiguredPersistenceWithSnapshotId(
                dir,
                snapshot_module_type,
                snapshot_instance_id,
                journal_partition_id,
                snapshot_id,
            );
        }

        const initial_state = self.config.initial_state;
        try self.startup();
        if (initial_state.fromSnapshot() and initial_state.throw_if_snapshot_not_found) {
            return error.SnapshotFileNotFound;
        }
        return .{
            .snapshot_requested = initial_state.fromSnapshot(),
            .snapshot_found = false,
            .journal_replayed_commands = 0,
        };
    }

    pub fn startupFromDiscoveredConfiguredPersistenceStrictMainLog(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_module_type: PersistenceModuleType,
        snapshot_instance_id: u32,
        journal_partition_id: u16,
    ) StartupFromDiscoveredConfiguredPersistenceStrictError!BootstrapOutcome {
        const discovered_snapshot_id = try self.discoverLatestDefaultSnapshotId(
            dir,
            snapshot_module_type,
            snapshot_instance_id,
        );
        if (discovered_snapshot_id) |snapshot_id| {
            return self.startupFromConfiguredPersistenceWithSnapshotIdStrictMainLog(
                dir,
                snapshot_module_type,
                snapshot_instance_id,
                journal_partition_id,
                snapshot_id,
            );
        }

        const initial_state = self.config.initial_state;
        try self.startup();
        if (initial_state.fromSnapshot() and initial_state.throw_if_snapshot_not_found) {
            return error.SnapshotFileNotFound;
        }
        return .{
            .snapshot_requested = initial_state.fromSnapshot(),
            .snapshot_found = false,
            .journal_replayed_commands = 0,
        };
    }

    pub fn writeJournalToFile(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        path: []const u8,
    ) JournalApiError!void {
        if (self.journal) |*journal| {
            try journal.writeToFile(dir, path);
            return;
        }
        return error.JournalingDisabled;
    }

    pub fn loadJournalFromFile(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        path: []const u8,
    ) JournalApiError!void {
        if (self.journal) |*journal| {
            const loaded = try journal_mod.CommandJournal.readFromFile(self.allocator, dir, path);
            journal.deinit();
            journal.* = loaded;
            return;
        }
        return error.JournalingDisabled;
    }

    pub fn replayJournalFromFile(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        path: []const u8,
    ) JournalReplayError!usize {
        if (self.journal) |*journal| {
            var loaded = try journal_mod.CommandJournal.readFromFile(self.allocator, dir, path);
            errdefer loaded.deinit();

            const replay_limit = self.config.initial_state.journal_timestamp_ns;
            const replay_limit_i64: i64 = if (replay_limit >= @as(u64, @intCast(std.math.maxInt(i64))))
                std.math.maxInt(i64)
            else
                @as(i64, @intCast(replay_limit));
            const sequence_exclusive = self.config.initial_state.snapshot_base_seq;

            var first_replayed_index: ?usize = null;
            var replayed: usize = 0;

            for (loaded.entries.items, 0..) |entry, idx| {
                if (entry.sequence <= sequence_exclusive) continue;
                if (replay_limit != 0 and entry.input.timestamp > replay_limit_i64) break;
                if (replay_limit == 0) break;
                if (first_replayed_index == null) first_replayed_index = idx;

                var replay_command = entry.input;
                var replay_response = try self.executeWithModeNoJournal(.routed, &replay_command);
                defer replay_response.deinit(self.allocator);
                try self.registerPersistDescriptorIfNeeded(
                    &replay_command,
                    replay_response.sequence,
                    replay_response.result_code,
                );

                if (replay_response.sequence != entry.sequence) {
                    return error.SequenceMismatch;
                }
                if (replay_response.result_code != entry.result_code) {
                    return error.ResultMismatch;
                }

                replayed += 1;
            }

            self.trimLoadedJournalAfterReplay(&loaded, first_replayed_index, replayed);

            journal.deinit();
            journal.* = loaded;
            return replayed;
        }
        return error.JournalingDisabled;
    }

    pub fn captureSnapshotToFile(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        path: []const u8,
        symbols: []const i32,
        depth: usize,
    ) SnapshotWriteError!void {
        var snapshot = try snapshot_mod.KernelSnapshot.captureFromKernel(
            self.allocator,
            self.kernelRef(),
            symbols,
            depth,
        );
        defer snapshot.deinit();
        try snapshot.writeToFile(dir, path);
    }

    pub fn validateSnapshotFile(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        path: []const u8,
    ) SnapshotValidateError!void {
        var snapshot = try snapshot_mod.KernelSnapshot.readFromFile(self.allocator, dir, path);
        defer snapshot.deinit();
        try snapshot.validateAgainstKernel(self.kernelRef());
    }

    pub fn writeJournalToDefaultPath(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_id: u64,
        partition_id: u16,
    ) DefaultJournalApiError!void {
        try self.ensureConfiguredStorageFolderExists(dir);
        const path = try self.defaultPersistenceJournalPath(
            self.allocator,
            snapshot_id,
            partition_id,
        );
        defer self.allocator.free(path);
        try self.writeJournalToFile(dir, path);
    }

    pub fn loadJournalFromDefaultPath(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_id: u64,
        partition_id: u16,
    ) DefaultJournalApiError!void {
        const path = try self.defaultPersistenceJournalPath(
            self.allocator,
            snapshot_id,
            partition_id,
        );
        defer self.allocator.free(path);
        try self.loadJournalFromFile(dir, path);
    }

    pub fn replayJournalFromDefaultPath(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_id: u64,
        partition_id: u16,
    ) DefaultJournalReplayError!usize {
        const path = try self.defaultPersistenceJournalPath(
            self.allocator,
            snapshot_id,
            partition_id,
        );
        defer self.allocator.free(path);
        return self.replayJournalFromFile(dir, path);
    }

    pub fn replayJournalFromDefaultPathRange(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_id: u64,
        partition_id: u16,
    ) DefaultJournalReplayRangeError!usize {
        var total_replayed: usize = 0;
        var current_partition = partition_id;
        while (true) {
            const replayed = self.replayJournalFromDefaultPath(
                dir,
                snapshot_id,
                current_partition,
            ) catch |err| switch (err) {
                error.FileNotFound => break,
                else => return err,
            };
            total_replayed = std.math.add(usize, total_replayed, replayed) catch
                return error.JournalReplayCountOverflow;
            if (current_partition == std.math.maxInt(u16)) break;
            current_partition += 1;
        }
        return total_replayed;
    }

    pub fn captureSnapshotToDefaultPath(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
        symbols: []const i32,
        depth: usize,
    ) DefaultSnapshotWriteError!void {
        try self.ensureConfiguredStorageFolderExists(dir);
        const path = try self.defaultPersistenceSnapshotPath(
            self.allocator,
            snapshot_id,
            module_type,
            instance_id,
        );
        defer self.allocator.free(path);
        try self.captureSnapshotToFile(
            dir,
            path,
            symbols,
            depth,
        );
        try self.appendDefaultPersistenceMainLogEntry(
            dir,
            self.kernelRef().next_seq,
            currentWallClockTimestampNs(),
            snapshot_id,
            module_type,
            instance_id,
        );
    }

    pub fn validateSnapshotDefaultPath(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) DefaultSnapshotValidateError!void {
        const path = try self.defaultPersistenceSnapshotPath(
            self.allocator,
            snapshot_id,
            module_type,
            instance_id,
        );
        defer self.allocator.free(path);
        try self.validateSnapshotFile(dir, path);
    }

    pub fn persistStateWithDefaultMainLog(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_id: i64,
        timestamp_ns: i64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) PersistStateWithMainLogError!cmd.CommandResultCode {
        const result = try self.persistState(.routed, snapshot_id, timestamp_ns);
        if (result != .success) return error.PersistStateRejected;

        const snapshot_id_u64 = std.math.cast(u64, snapshot_id) orelse return error.PersistStateSnapshotIdOverflow;
        const timestamp_ns_u64 = std.math.cast(u64, timestamp_ns) orelse return error.PersistStateTimestampOverflow;
        const persisted_snapshot = self.persistenceCurrentSnapshot();
        if (persisted_snapshot.snapshot_id != snapshot_id_u64 or
            persisted_snapshot.timestamp_ns != timestamp_ns_u64)
        {
            return error.PersistStateSnapshotMismatch;
        }

        try self.appendDefaultPersistenceMainLogEntry(
            dir,
            persisted_snapshot.seq,
            persisted_snapshot.timestamp_ns,
            persisted_snapshot.snapshot_id,
            module_type,
            instance_id,
        );
        return result;
    }

    pub fn snapshotExistsDefaultPath(
        self: *const ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) DefaultSnapshotExistsError!bool {
        const path = try self.defaultPersistenceSnapshotPath(
            self.allocator,
            snapshot_id,
            module_type,
            instance_id,
        );
        defer self.allocator.free(path);
        return self.snapshotExistsFile(dir, path);
    }

    pub fn snapshotExistsFile(
        self: *const ExchangeApiSharded,
        dir: std.Io.Dir,
        path: []const u8,
    ) SnapshotExistsError!bool {
        _ = self;
        _ = dir.statFile(io_context.current(), path, .{}) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
        return true;
    }

    pub fn addSymbolAll(
        self: *ExchangeApiSharded,
        spec: common.CoreSymbolSpecification,
    ) risk.SymbolSpecificationProvider.AddSymbolError!void {
        try self.core.addSymbolAll(spec);
    }

    pub fn addUser(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        uid: i64,
    ) ExecuteError!kernel_mod.CommandResponse {
        var command = makeAddUserCommand(uid);
        return self.executeWithMode(mode, &command);
    }

    pub fn suspendUser(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        uid: i64,
    ) ExecuteError!kernel_mod.CommandResponse {
        var command = makeSuspendUserCommand(uid);
        return self.executeWithMode(mode, &command);
    }

    pub fn resumeUser(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        uid: i64,
    ) ExecuteError!kernel_mod.CommandResponse {
        var command = makeResumeUserCommand(uid);
        return self.executeWithMode(mode, &command);
    }

    pub fn adjustUserBalance(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        uid: i64,
        transaction_id: i64,
        currency: i32,
        amount: i64,
        adjustment_type: common.BalanceAdjustmentType,
    ) ExecuteError!kernel_mod.CommandResponse {
        var command = makeBalanceAdjustmentCommand(
            uid,
            currency,
            amount,
            transaction_id,
            adjustment_type,
        );
        return self.executeWithMode(mode, &command);
    }

    pub fn placeOrder(
        self: *ExchangeApiSharded,
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
        var command = makePlaceOrderCommand(
            order_id,
            uid,
            symbol,
            price,
            reserve_bid_price,
            size,
            action,
            order_type,
            user_cookie,
        );
        return self.executeWithMode(mode, &command);
    }

    pub fn moveOrder(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        order_id: i64,
        uid: i64,
        symbol: i32,
        new_price: i64,
    ) ExecuteError!kernel_mod.CommandResponse {
        var command = makeMoveOrderCommand(order_id, uid, symbol, new_price);
        return self.executeWithMode(mode, &command);
    }

    pub fn cancelOrder(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        order_id: i64,
        uid: i64,
        symbol: i32,
    ) ExecuteError!kernel_mod.CommandResponse {
        var command = makeCancelOrderCommand(order_id, uid, symbol);
        return self.executeWithMode(mode, &command);
    }

    pub fn reduceOrder(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        order_id: i64,
        uid: i64,
        symbol: i32,
        reduce_size: i64,
    ) ExecuteError!kernel_mod.CommandResponse {
        var command = makeReduceOrderCommand(order_id, uid, symbol, reduce_size);
        return self.executeWithMode(mode, &command);
    }

    pub fn createUserWithCallback(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        user_id: i64,
        callback: CommandResponseCallback,
    ) SubmitCommandWithCallbackError!void {
        try self.submitCommandAsyncWithCallback(mode, .{ .add_user = user_id }, callback);
    }

    pub fn suspendUserWithCallback(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        user_id: i64,
        callback: CommandResponseCallback,
    ) SubmitCommandWithCallbackError!void {
        try self.submitCommandAsyncWithCallback(mode, .{ .suspend_user = user_id }, callback);
    }

    pub fn resumeUserWithCallback(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        user_id: i64,
        callback: CommandResponseCallback,
    ) SubmitCommandWithCallbackError!void {
        try self.submitCommandAsyncWithCallback(mode, .{ .resume_user = user_id }, callback);
    }

    pub fn balanceAdjustmentWithCallback(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        uid: i64,
        transaction_id: i64,
        currency: i32,
        amount: i64,
        adjustment_type: common.BalanceAdjustmentType,
        callback: CommandResponseCallback,
    ) SubmitCommandWithCallbackError!void {
        try self.submitCommandAsyncWithCallback(mode, .{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = transaction_id,
                .currency = currency,
                .amount = amount,
                .adjustment_type = adjustment_type,
            },
        }, callback);
    }

    pub fn placeNewOrderWithCallback(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        order_id: i64,
        user_cookie: i32,
        price: i64,
        reserve_bid_price: i64,
        size: i64,
        action: common.OrderAction,
        order_type: common.OrderType,
        symbol: i32,
        uid: i64,
        callback: CommandResponseCallback,
    ) SubmitCommandWithCallbackError!void {
        try self.submitCommandAsyncWithCallback(mode, .{
            .place_order = .{
                .order_id = order_id,
                .uid = uid,
                .symbol = symbol,
                .price = price,
                .reserve_bid_price = reserve_bid_price,
                .size = size,
                .action = action,
                .order_type = order_type,
                .user_cookie = user_cookie,
            },
        }, callback);
    }

    pub fn moveOrderWithCallback(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        price: i64,
        order_id: i64,
        symbol: i32,
        uid: i64,
        callback: CommandResponseCallback,
    ) SubmitCommandWithCallbackError!void {
        try self.submitCommandAsyncWithCallback(mode, .{
            .move_order = .{
                .order_id = order_id,
                .uid = uid,
                .symbol = symbol,
                .new_price = price,
            },
        }, callback);
    }

    pub fn cancelOrderWithCallback(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        order_id: i64,
        symbol: i32,
        uid: i64,
        callback: CommandResponseCallback,
    ) SubmitCommandWithCallbackError!void {
        try self.submitCommandAsyncWithCallback(mode, .{
            .cancel_order = .{
                .order_id = order_id,
                .uid = uid,
                .symbol = symbol,
            },
        }, callback);
    }

    pub fn reduceOrderWithCallback(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        reduce_size: i64,
        order_id: i64,
        symbol: i32,
        uid: i64,
        callback: CommandResponseCallback,
    ) SubmitCommandWithCallbackError!void {
        try self.submitCommandAsyncWithCallback(mode, .{
            .reduce_order = .{
                .order_id = order_id,
                .uid = uid,
                .symbol = symbol,
                .reduce_size = reduce_size,
            },
        }, callback);
    }

    pub fn groupingControl(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        timestamp_ns: i64,
        grouping_mode: i64,
    ) ExecuteError!kernel_mod.CommandResponse {
        var command = makeGroupingControlCommand(timestamp_ns, grouping_mode);
        return self.executeWithMode(mode, &command);
    }

    pub fn reset(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
    ) ExecuteError!kernel_mod.CommandResponse {
        var command = makeResetCommand();
        return self.executeWithMode(mode, &command);
    }

    pub fn nop(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
    ) ExecuteError!kernel_mod.CommandResponse {
        var command = makeNopCommand();
        return self.executeWithMode(mode, &command);
    }

    pub fn requestOrderBook(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        symbol: i32,
        depth: usize,
    ) (ExecuteError || HelperError)!kernel_mod.CommandResponse {
        const depth_i64 = std.math.cast(i64, depth) orelse return error.OrderBookDepthOverflow;
        var command = makeRequestOrderBookCommandI64(symbol, depth_i64);
        return self.executeWithMode(mode, &command);
    }

    pub fn submitCommand(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command: ApiCommand,
    ) SubmitApiCommandError!void {
        return self.submitApiCommand(mode, command);
    }

    pub fn createUser(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        service_flags: i32,
        events_group: i64,
        timestamp_ns: i64,
        user_id: i64,
    ) SubmitApiCommandError!void {
        try self.submitRawCommand(mode, .{
            .command = .add_user,
            .order_id = -1,
            .symbol = -1,
            .uid = user_id,
            .timestamp = timestamp_ns,
            .events_group = events_group,
            .service_flags = service_flags,
            .result_code = .new,
        });
    }

    pub fn balanceAdjustment(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        service_flags: i32,
        events_group: i64,
        timestamp_ns: i64,
        uid: i64,
        transaction_id: i64,
        currency: i32,
        amount: i64,
        adjustment_type: common.BalanceAdjustmentType,
    ) SubmitApiCommandError!void {
        try self.submitRawCommand(mode, .{
            .command = .balance_adjustment,
            .order_id = transaction_id,
            .symbol = currency,
            .uid = uid,
            .price = amount,
            .size = 0,
            .order_type = switch (adjustment_type) {
                .adjustment => .gtc,
                .@"suspend" => .ioc,
            },
            .timestamp = timestamp_ns,
            .events_group = events_group,
            .service_flags = service_flags,
            .result_code = .new,
        });
    }

    pub fn placeNewOrder(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        service_flags: i32,
        events_group: i64,
        timestamp_ns: i64,
        order_id: i64,
        user_cookie: i32,
        price: i64,
        reserve_bid_price: i64,
        size: i64,
        action: common.OrderAction,
        order_type: common.OrderType,
        symbol: i32,
        uid: i64,
    ) SubmitApiCommandError!void {
        try self.submitRawCommand(mode, .{
            .command = .place_order,
            .order_id = order_id,
            .user_cookie = user_cookie,
            .price = price,
            .reserve_bid_price = reserve_bid_price,
            .size = size,
            .action = action,
            .order_type = order_type,
            .symbol = symbol,
            .uid = uid,
            .timestamp = timestamp_ns,
            .events_group = events_group,
            .service_flags = service_flags,
            .result_code = .new,
        });
    }

    pub fn binaryData(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        service_flags: i32,
        events_group: i64,
        timestamp_ns: i64,
        last_flag: i32,
        word0: i64,
        word1: i64,
        word2: i64,
        word3: i64,
        word4: i64,
    ) SubmitApiCommandError!void {
        try self.submitRawCommand(mode, .{
            .command = .binary_data_command,
            .symbol = last_flag,
            .order_id = word0,
            .price = word1,
            .reserve_bid_price = word2,
            .size = word3,
            .uid = word4,
            .timestamp = timestamp_ns,
            .events_group = events_group,
            .service_flags = service_flags,
            .result_code = .new,
        });
    }

    pub fn orderBookRequest(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        symbol: i32,
        depth: usize,
    ) (ExecuteError || HelperError)!kernel_mod.CommandResponse {
        return self.requestOrderBook(mode, symbol, depth);
    }

    pub fn orderBookRequestAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        symbol: i32,
        depth: usize,
    ) (AsyncSubmitError || HelperError)!u64 {
        return self.requestOrderBookAsync(mode, symbol, depth);
    }

    pub fn submitBinaryPayload(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command_type: cmd.OrderCommandType,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
    ) BinaryPayloadSubmitError!kernel_mod.CommandResponse {
        try ensureBinaryRoutedMode(mode);
        switch (command_type) {
            .binary_data_command, .binary_data_query => {},
            else => return error.InvalidBinaryCommandType,
        }
        const frame_layout = try computeBinaryFrameLayout(payload.len);
        const frame_words = binary_frames.FrameWordsPerCommand;

        var last_response: ?kernel_mod.CommandResponse = null;
        errdefer if (last_response) |*response| response.deinit(self.allocator);

        for (0..frame_layout.frame_count) |frame_idx| {
            var frame_payload: [binary_frames.FrameWordsPerCommand]i64 = [_]i64{0} ** binary_frames.FrameWordsPerCommand;
            for (0..frame_words) |word_idx| {
                frame_payload[word_idx] = readBigEndianWordPadded(payload, frame_idx * frame_words + word_idx);
            }

            var frame_command = makeBinaryFrameCommand(
                command_type,
                transfer_id,
                if (frame_idx + 1 == frame_layout.frame_count) -1 else 0,
                timestamp,
                frame_payload,
            );
            const response = try self.executeWithMode(mode, &frame_command);
            if (last_response) |*previous| {
                previous.deinit(self.allocator);
            }
            last_response = response;
        }
        return last_response.?;
    }

    pub fn submitBinaryPayloadAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command_type: cmd.OrderCommandType,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
    ) BinaryPayloadAsyncSubmitError!u64 {
        try ensureBinaryRoutedMode(mode);
        switch (command_type) {
            .binary_data_command, .binary_data_query => {},
            else => return error.InvalidBinaryCommandType,
        }
        const frame_layout = try computeBinaryFrameLayout(payload.len);
        const frame_words = binary_frames.FrameWordsPerCommand;

        var last_request_id: ?u64 = null;
        for (0..frame_layout.frame_count) |frame_idx| {
            var frame_payload: [binary_frames.FrameWordsPerCommand]i64 = [_]i64{0} ** binary_frames.FrameWordsPerCommand;
            for (0..frame_words) |word_idx| {
                frame_payload[word_idx] = readBigEndianWordPadded(payload, frame_idx * frame_words + word_idx);
            }

            const frame_command = makeBinaryFrameCommand(
                command_type,
                transfer_id,
                if (frame_idx + 1 == frame_layout.frame_count) -1 else 0,
                timestamp,
                frame_payload,
            );
            last_request_id = try self.submitCommandAsync(mode, &frame_command);
        }

        return last_request_id.?;
    }

    pub fn submitBinaryDataPayload(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
    ) BinaryPayloadSubmitError!kernel_mod.CommandResponse {
        return self.submitBinaryPayload(
            mode,
            .binary_data_command,
            transfer_id,
            timestamp,
            payload,
        );
    }

    pub fn submitBinaryData(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
    ) BinaryPayloadSubmitError!kernel_mod.CommandResponse {
        return self.submitBinaryDataPayload(mode, transfer_id, timestamp, payload);
    }

    pub fn submitBinaryCommand(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
    ) BinaryPayloadSubmitError!kernel_mod.CommandResponse {
        return self.submitBinaryDataPayload(mode, transfer_id, timestamp, payload);
    }

    pub fn submitBinaryQueryPayload(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
    ) BinaryPayloadSubmitError!kernel_mod.CommandResponse {
        return self.submitBinaryPayload(
            mode,
            .binary_data_query,
            transfer_id,
            timestamp,
            payload,
        );
    }

    pub fn submitBinaryDataPayloadAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
    ) BinaryPayloadAsyncSubmitError!u64 {
        return self.submitBinaryPayloadAsync(
            mode,
            .binary_data_command,
            transfer_id,
            timestamp,
            payload,
        );
    }

    pub fn submitBinaryDataAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
    ) BinaryPayloadAsyncSubmitError!u64 {
        return self.submitBinaryDataPayloadAsync(mode, transfer_id, timestamp, payload);
    }

    pub fn submitBinaryCommandAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
    ) BinaryPayloadAsyncSubmitError!u64 {
        return self.submitBinaryDataPayloadAsync(mode, transfer_id, timestamp, payload);
    }

    pub fn submitBinaryQueryPayloadAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
    ) BinaryPayloadAsyncSubmitError!u64 {
        return self.submitBinaryPayloadAsync(
            mode,
            .binary_data_query,
            transfer_id,
            timestamp,
            payload,
        );
    }

    pub fn publishBinaryData(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
    ) BinaryPayloadAsyncSubmitError!u64 {
        return self.submitBinaryDataPayloadAsync(mode, transfer_id, timestamp, payload);
    }

    pub fn submitAddSymbolsBinary(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        symbols: []const common.CoreSymbolSpecification,
    ) BinaryPayloadTypedSubmitError!kernel_mod.CommandResponse {
        try self.ensureTransferMatchesSymbols(transfer_id, symbols);
        const payload = try binary_payload.encodeAddSymbolsToBytes(self.allocator, symbols);
        defer self.allocator.free(payload);
        return self.submitBinaryPayload(
            mode,
            .binary_data_command,
            transfer_id,
            timestamp,
            payload,
        );
    }

    pub fn submitAddSymbolsBinaryAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        symbols: []const common.CoreSymbolSpecification,
    ) BinaryPayloadTypedAsyncSubmitError!u64 {
        try self.ensureTransferMatchesSymbols(transfer_id, symbols);
        const payload = try binary_payload.encodeAddSymbolsToBytes(self.allocator, symbols);
        defer self.allocator.free(payload);
        return self.submitBinaryPayloadAsync(
            mode,
            .binary_data_command,
            transfer_id,
            timestamp,
            payload,
        );
    }

    pub fn submitAddAccountsBinary(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        users: []const BinaryUserAccountsBatch,
    ) BinaryPayloadTypedSubmitError!kernel_mod.CommandResponse {
        try self.ensureTransferMatchesUsers(transfer_id, users);
        const payload = try binary_payload.encodeAddAccountsToBytes(self.allocator, users);
        defer self.allocator.free(payload);
        return self.submitBinaryPayload(
            mode,
            .binary_data_command,
            transfer_id,
            timestamp,
            payload,
        );
    }

    pub fn submitAddAccountsBinaryAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        users: []const BinaryUserAccountsBatch,
    ) BinaryPayloadTypedAsyncSubmitError!u64 {
        try self.ensureTransferMatchesUsers(transfer_id, users);
        const payload = try binary_payload.encodeAddAccountsToBytes(self.allocator, users);
        defer self.allocator.free(payload);
        return self.submitBinaryPayloadAsync(
            mode,
            .binary_data_command,
            transfer_id,
            timestamp,
            payload,
        );
    }

    pub fn submitBinaryReportQuery(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    ) BinaryReportQuerySubmitError!kernel_mod.CommandResponse {
        try self.ensureTransferMatchesQuery(transfer_id, query);
        const payload = try binary_payload.encodeReportQueryToBytes(self.allocator, query);
        defer self.allocator.free(payload);
        return self.submitBinaryPayload(
            mode,
            .binary_data_query,
            transfer_id,
            timestamp,
            payload,
        );
    }

    pub fn submitBinaryReportQueryAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    ) BinaryReportQueryAsyncSubmitError!u64 {
        try self.ensureTransferMatchesQuery(transfer_id, query);
        const payload = try binary_payload.encodeReportQueryToBytes(self.allocator, query);
        defer self.allocator.free(payload);
        return self.submitBinaryPayloadAsync(
            mode,
            .binary_data_query,
            transfer_id,
            timestamp,
            payload,
        );
    }

    pub fn publishQuery(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    ) BinaryReportQueryAsyncSubmitError!u64 {
        return self.submitBinaryReportQueryAsync(mode, transfer_id, timestamp, query);
    }

    pub fn submitQueryAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    ) BinaryReportQueryAsyncSubmitError!u64 {
        return self.submitBinaryReportQueryAsync(mode, transfer_id, timestamp, query);
    }

    pub fn requestStateHashAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
    ) BinaryReportQueryAsyncSubmitError!u64 {
        return self.submitBinaryReportQueryAsync(mode, transfer_id, timestamp, .state_hash);
    }

    pub fn requestSingleUserReportAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        uid: i64,
    ) BinaryReportQueryAsyncSubmitError!u64 {
        return self.submitBinaryReportQueryAsync(mode, transfer_id, timestamp, .{ .single_user = uid });
    }

    pub fn requestTotalCurrencyBalanceAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
    ) BinaryReportQueryAsyncSubmitError!u64 {
        return self.submitBinaryReportQueryAsync(mode, transfer_id, timestamp, .total_currency_balance);
    }

    pub fn submitCommandAsyncResultCode(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command: ApiCommand,
    ) SubmitApiCommandAsyncError!ApiCommandAsyncHandle {
        return self.submitApiCommandAsync(mode, command);
    }

    pub fn submitCommandAsyncFullResponse(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command: ApiCommand,
    ) SubmitApiCommandAsyncError!ApiCommandAsyncHandle {
        return self.submitApiCommandAsync(mode, command);
    }

    pub fn processCommandFullResponse(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command: ApiCommand,
    ) ProcessCommandFullResponseError!kernel_mod.CommandResponse {
        const handle = try self.submitCommandAsyncFullResponse(mode, command);
        _ = try self.drainPending();
        var result = self.takeApiCommandAsyncResult(handle) orelse return error.MissingAsyncResult;
        errdefer result.deinit(self.allocator);
        return switch (result) {
            .response => |response| response,
            .persist_state => error.ExpectedResponseResult,
        };
    }

    pub fn processPersistStateResultCode(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        snapshot_id: i64,
        timestamp_ns: i64,
    ) ProcessCommandResultCodeError!cmd.CommandResultCode {
        return self.persistState(mode, snapshot_id, timestamp_ns);
    }

    pub fn submitCreateUserAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        user_id: i64,
    ) SubmitApiCommandAsyncError!ApiCommandAsyncHandle {
        return self.submitCommandAsyncFullResponse(mode, .{ .add_user = user_id });
    }

    pub fn submitSuspendUserAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        user_id: i64,
    ) SubmitApiCommandAsyncError!ApiCommandAsyncHandle {
        return self.submitCommandAsyncFullResponse(mode, .{ .suspend_user = user_id });
    }

    pub fn submitResumeUserAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        user_id: i64,
    ) SubmitApiCommandAsyncError!ApiCommandAsyncHandle {
        return self.submitCommandAsyncFullResponse(mode, .{ .resume_user = user_id });
    }

    pub fn submitBalanceAdjustmentAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        uid: i64,
        transaction_id: i64,
        currency: i32,
        amount: i64,
        adjustment_type: common.BalanceAdjustmentType,
    ) SubmitApiCommandAsyncError!ApiCommandAsyncHandle {
        return self.submitCommandAsyncFullResponse(mode, .{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = transaction_id,
                .currency = currency,
                .amount = amount,
                .adjustment_type = adjustment_type,
            },
        });
    }

    pub fn submitPlaceNewOrderAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        order_id: i64,
        user_cookie: i32,
        price: i64,
        reserve_bid_price: i64,
        size: i64,
        action: common.OrderAction,
        order_type: common.OrderType,
        symbol: i32,
        uid: i64,
    ) SubmitApiCommandAsyncError!ApiCommandAsyncHandle {
        return self.submitCommandAsyncFullResponse(mode, .{
            .place_order = .{
                .order_id = order_id,
                .uid = uid,
                .symbol = symbol,
                .price = price,
                .reserve_bid_price = reserve_bid_price,
                .size = size,
                .action = action,
                .order_type = order_type,
                .user_cookie = user_cookie,
            },
        });
    }

    pub fn submitMoveOrderAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        price: i64,
        order_id: i64,
        symbol: i32,
        uid: i64,
    ) SubmitApiCommandAsyncError!ApiCommandAsyncHandle {
        return self.submitCommandAsyncFullResponse(mode, .{
            .move_order = .{
                .order_id = order_id,
                .uid = uid,
                .symbol = symbol,
                .new_price = price,
            },
        });
    }

    pub fn submitCancelOrderAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        order_id: i64,
        symbol: i32,
        uid: i64,
    ) SubmitApiCommandAsyncError!ApiCommandAsyncHandle {
        return self.submitCommandAsyncFullResponse(mode, .{
            .cancel_order = .{
                .order_id = order_id,
                .uid = uid,
                .symbol = symbol,
            },
        });
    }

    pub fn submitReduceOrderAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        reduce_size: i64,
        order_id: i64,
        symbol: i32,
        uid: i64,
    ) SubmitApiCommandAsyncError!ApiCommandAsyncHandle {
        return self.submitCommandAsyncFullResponse(mode, .{
            .reduce_order = .{
                .order_id = order_id,
                .uid = uid,
                .symbol = symbol,
                .reduce_size = reduce_size,
            },
        });
    }

    pub fn submitPersistStateAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        snapshot_id: i64,
        timestamp_ns: i64,
    ) SubmitApiCommandAsyncError!ApiCommandAsyncHandle {
        return self.submitCommandAsyncResultCode(mode, .{
            .persist_state = .{
                .snapshot_id = snapshot_id,
                .timestamp_ns = timestamp_ns,
            },
        });
    }

    pub fn submitApiCommandResult(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        api_command: ApiCommand,
    ) SubmitApiCommandError!ApiCommandResult {
        return switch (api_command) {
            .raw_command => |command| blk: {
                var mutable = command;
                break :blk .{ .response = try self.executeWithMode(mode, &mutable) };
            },
            .add_user => |uid| .{ .response = try self.addUser(mode, uid) },
            .adjust_user_balance => |payload| .{ .response = try self.adjustUserBalance(
                mode,
                payload.uid,
                payload.transaction_id,
                payload.currency,
                payload.amount,
                payload.adjustment_type,
            ) },
            .suspend_user => |uid| .{ .response = try self.suspendUser(mode, uid) },
            .resume_user => |uid| .{ .response = try self.resumeUser(mode, uid) },
            .place_order => |payload| .{ .response = try self.placeOrder(
                mode,
                payload.order_id,
                payload.uid,
                payload.symbol,
                payload.price,
                payload.reserve_bid_price,
                payload.size,
                payload.action,
                payload.order_type,
                payload.user_cookie,
            ) },
            .move_order => |payload| .{ .response = try self.moveOrder(
                mode,
                payload.order_id,
                payload.uid,
                payload.symbol,
                payload.new_price,
            ) },
            .cancel_order => |payload| .{ .response = try self.cancelOrder(
                mode,
                payload.order_id,
                payload.uid,
                payload.symbol,
            ) },
            .reduce_order => |payload| .{ .response = try self.reduceOrder(
                mode,
                payload.order_id,
                payload.uid,
                payload.symbol,
                payload.reduce_size,
            ) },
            .request_order_book => |payload| .{ .response = try self.requestOrderBook(
                mode,
                payload.symbol,
                payload.depth,
            ) },
            .persist_state => |payload| .{ .persist_state = try self.persistState(
                mode,
                payload.snapshot_id,
                payload.timestamp_ns,
            ) },
            .grouping_control => |payload| .{ .response = try self.groupingControl(
                mode,
                payload.timestamp_ns,
                payload.mode,
            ) },
            .reset => .{ .response = try self.reset(mode) },
            .nop => .{ .response = try self.nop(mode) },
            .binary_add_symbols => |payload| .{ .response = try self.submitAddSymbolsBinary(
                mode,
                payload.transfer_id,
                payload.timestamp,
                payload.symbols,
            ) },
            .binary_add_accounts => |payload| .{ .response = try self.submitAddAccountsBinary(
                mode,
                payload.transfer_id,
                payload.timestamp,
                payload.users,
            ) },
            .binary_report_query => |payload| .{ .response = try self.submitBinaryReportQuery(
                mode,
                payload.transfer_id,
                payload.timestamp,
                payload.query,
            ) },
        };
    }

    pub fn submitApiCommand(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        api_command: ApiCommand,
    ) SubmitApiCommandError!void {
        var result = try self.submitApiCommandResult(mode, api_command);
        defer result.deinit(self.allocator);
    }

    pub fn submitApiCommandAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        api_command: ApiCommand,
    ) SubmitApiCommandAsyncError!ApiCommandAsyncHandle {
        return switch (api_command) {
            .raw_command => |command| .{ .request_id = try self.submitCommandAsync(mode, &command) },
            .add_user => |uid| .{ .request_id = try self.addUserAsync(mode, uid) },
            .adjust_user_balance => |payload| .{ .request_id = try self.adjustUserBalanceAsync(
                mode,
                payload.uid,
                payload.transaction_id,
                payload.currency,
                payload.amount,
                payload.adjustment_type,
            ) },
            .suspend_user => |uid| .{ .request_id = try self.suspendUserAsync(mode, uid) },
            .resume_user => |uid| .{ .request_id = try self.resumeUserAsync(mode, uid) },
            .place_order => |payload| .{ .request_id = try self.placeOrderAsync(
                mode,
                payload.order_id,
                payload.uid,
                payload.symbol,
                payload.price,
                payload.reserve_bid_price,
                payload.size,
                payload.action,
                payload.order_type,
                payload.user_cookie,
            ) },
            .move_order => |payload| .{ .request_id = try self.moveOrderAsync(
                mode,
                payload.order_id,
                payload.uid,
                payload.symbol,
                payload.new_price,
            ) },
            .cancel_order => |payload| .{ .request_id = try self.cancelOrderAsync(
                mode,
                payload.order_id,
                payload.uid,
                payload.symbol,
            ) },
            .reduce_order => |payload| .{ .request_id = try self.reduceOrderAsync(
                mode,
                payload.order_id,
                payload.uid,
                payload.symbol,
                payload.reduce_size,
            ) },
            .request_order_book => |payload| .{ .request_id = try self.requestOrderBookAsync(
                mode,
                payload.symbol,
                payload.depth,
            ) },
            .persist_state => |payload| .{ .persist_state = try self.persistStateAsync(
                mode,
                payload.snapshot_id,
                payload.timestamp_ns,
            ) },
            .grouping_control => |payload| .{ .request_id = try self.groupingControlAsync(
                mode,
                payload.timestamp_ns,
                payload.mode,
            ) },
            .reset => .{ .request_id = try self.resetAsync(mode) },
            .nop => .{ .request_id = try self.nopAsync(mode) },
            .binary_add_symbols => |payload| .{ .request_id = try self.submitAddSymbolsBinaryAsync(
                mode,
                payload.transfer_id,
                payload.timestamp,
                payload.symbols,
            ) },
            .binary_add_accounts => |payload| .{ .request_id = try self.submitAddAccountsBinaryAsync(
                mode,
                payload.transfer_id,
                payload.timestamp,
                payload.users,
            ) },
            .binary_report_query => |payload| .{ .request_id = try self.submitBinaryReportQueryAsync(
                mode,
                payload.transfer_id,
                payload.timestamp,
                payload.query,
            ) },
        };
    }

    pub fn isApiCommandAsyncResultReady(
        self: *const ExchangeApiSharded,
        handle: ApiCommandAsyncHandle,
    ) bool {
        return switch (handle) {
            .request_id => |request_id| self.isResultReady(request_id),
            .persist_state => |requests| self.isPersistStateResultReady(requests),
        };
    }

    pub fn takeApiCommandAsyncResult(
        self: *ExchangeApiSharded,
        handle: ApiCommandAsyncHandle,
    ) ?ApiCommandResult {
        return switch (handle) {
            .request_id => |request_id| if (self.takeResult(request_id)) |response|
                .{ .response = response }
            else
                null,
            .persist_state => |requests| if (self.takePersistStateResult(requests)) |result_code|
                .{ .persist_state = result_code }
            else
                null,
        };
    }

    pub fn registerResultCallback(
        self: *ExchangeApiSharded,
        handle: ApiCommandAsyncHandle,
        callback: CommandResponseCallback,
    ) CallbackRegistrationError!void {
        try self.registerCallbackForHandle(handle, callback);
    }

    pub fn pendingCallbackCount(self: *const ExchangeApiSharded) usize {
        return self.callback_registrations_len;
    }

    pub fn processResult(
        self: *ExchangeApiSharded,
        handle: ApiCommandAsyncHandle,
        callback: CommandResponseCallback,
    ) ProcessResultError!bool {
        var result = self.takeApiCommandAsyncResult(handle) orelse return false;
        defer result.deinit(self.allocator);
        switch (result) {
            .response => |*response| callback.callback(callback.context, response),
            .persist_state => return error.UnexpectedPersistStateResult,
        }
        return true;
    }

    pub fn processRegisteredCallback(
        self: *ExchangeApiSharded,
        handle: ApiCommandAsyncHandle,
    ) ProcessRegisteredCallbackError!bool {
        const request_id = try requestIdFromHandle(handle);
        const idx = self.callbackRegistrationIndex(request_id) orelse return false;
        const callback = self.callback_registrations[idx].callback;
        const consumed = try self.processResult(handle, callback);
        if (consumed) {
            self.removeCallbackRegistrationAt(idx);
        }
        return consumed;
    }

    pub fn processPendingCallbacks(self: *ExchangeApiSharded) ProcessRegisteredCallbackError!usize {
        var processed: usize = 0;
        var idx: usize = 0;
        while (idx < self.callback_registrations_len) {
            const registration = self.callback_registrations[idx];
            const consumed = try self.processResult(.{ .request_id = registration.request_id }, registration.callback);
            if (consumed) {
                self.removeCallbackRegistrationAt(idx);
                processed += 1;
            } else {
                idx += 1;
            }
        }
        return processed;
    }

    pub fn submitApiCommandsSyncResult(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        commands: []const ApiCommand,
    ) SubmitApiCommandBatchError!ApiCommandResult {
        if (commands.len == 0) return error.EmptyApiCommandBatch;

        var handles = try self.allocator.alloc(ApiCommandAsyncHandle, commands.len);
        defer self.allocator.free(handles);

        for (commands, 0..) |api_command, idx| {
            handles[idx] = try self.submitApiCommandAsync(mode, api_command);
        }

        _ = try self.drainPending();

        for (handles, 0..) |handle, idx| {
            var result = self.takeApiCommandAsyncResult(handle) orelse return error.MissingBatchResult;
            if (idx + 1 == handles.len) {
                return result;
            }
            result.deinit(self.allocator);
        }

        return error.MissingBatchResult;
    }

    pub fn submitApiCommandsSync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        commands: []const ApiCommand,
    ) SubmitApiCommandBatchError!void {
        var result = try self.submitApiCommandsSyncResult(mode, commands);
        defer result.deinit(self.allocator);
    }

    pub fn submitCommandsSync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        commands: []const ApiCommand,
    ) SubmitApiCommandBatchError!void {
        return self.submitApiCommandsSync(mode, commands);
    }

    pub fn submitApiCommandsSyncWithNop(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        commands: []const ApiCommand,
    ) SubmitApiCommandBatchError!kernel_mod.CommandResponse {
        var handles = try self.allocator.alloc(ApiCommandAsyncHandle, commands.len + 1);
        defer self.allocator.free(handles);

        for (commands, 0..) |api_command, idx| {
            handles[idx] = try self.submitApiCommandAsync(mode, api_command);
        }
        handles[commands.len] = try self.submitApiCommandAsync(mode, .nop);

        _ = try self.drainPending();

        for (handles[0..commands.len]) |handle| {
            var result = self.takeApiCommandAsyncResult(handle) orelse return error.MissingBatchResult;
            result.deinit(self.allocator);
        }

        var flush_result = self.takeApiCommandAsyncResult(handles[commands.len]) orelse return error.MissingBatchResult;
        errdefer flush_result.deinit(self.allocator);
        try expectFlushResultVariant(&flush_result);
        return switch (flush_result) {
            .response => |response| response,
            .persist_state => return error.UnexpectedFlushResultVariant,
        };
    }

    pub fn submitCommandsSyncWithNop(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        commands: []const ApiCommand,
    ) SubmitApiCommandBatchError!kernel_mod.CommandResponse {
        return self.submitApiCommandsSyncWithNop(mode, commands);
    }

    pub fn persistState(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        snapshot_id: i64,
        timestamp_ns: i64,
    ) PersistStateError!cmd.CommandResultCode {
        const requests = try self.persistStateAsync(mode, snapshot_id, timestamp_ns);
        _ = try self.drainPending();
        return self.takePersistStateResult(requests) orelse error.MissingAsyncResult;
    }

    pub fn submitCommandBatch(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        commands: []*cmd.OrderCommand,
    ) ExecuteError!BatchResponse {
        var batch: BatchResponse = .{};
        for (commands) |command| {
            var response = try self.executeWithMode(mode, command);
            defer response.deinit(self.allocator);

            if (batch.sequence_from == null) {
                batch.sequence_from = response.sequence;
            }
            batch.sequence_to = response.sequence;
            batch.result_code = cmd.CommandResultCode.mergeToFirstFailed(&.{
                batch.result_code,
                response.result_code,
            });
        }
        return batch;
    }

    pub fn submitCommandAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command: *const cmd.OrderCommand,
    ) AsyncSubmitError!u64 {
        if (self.core.isStopped()) return error.CoreStopped;
        const request_id = try self.nextRequestId();
        try self.pending_queue.append(self.allocator, .{
            .request_id = request_id,
            .mode = mode,
            .command = command.*,
        });
        return request_id;
    }

    pub fn addUserAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        uid: i64,
    ) AsyncSubmitError!u64 {
        const command = makeAddUserCommand(uid);
        return self.submitCommandAsync(mode, &command);
    }

    pub fn suspendUserAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        uid: i64,
    ) AsyncSubmitError!u64 {
        const command = makeSuspendUserCommand(uid);
        return self.submitCommandAsync(mode, &command);
    }

    pub fn resumeUserAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        uid: i64,
    ) AsyncSubmitError!u64 {
        const command = makeResumeUserCommand(uid);
        return self.submitCommandAsync(mode, &command);
    }

    pub fn adjustUserBalanceAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        uid: i64,
        transaction_id: i64,
        currency: i32,
        amount: i64,
        adjustment_type: common.BalanceAdjustmentType,
    ) AsyncSubmitError!u64 {
        const command = makeBalanceAdjustmentCommand(
            uid,
            currency,
            amount,
            transaction_id,
            adjustment_type,
        );
        return self.submitCommandAsync(mode, &command);
    }

    pub fn placeOrderAsync(
        self: *ExchangeApiSharded,
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
    ) AsyncSubmitError!u64 {
        const command = makePlaceOrderCommand(
            order_id,
            uid,
            symbol,
            price,
            reserve_bid_price,
            size,
            action,
            order_type,
            user_cookie,
        );
        return self.submitCommandAsync(mode, &command);
    }

    pub fn moveOrderAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        order_id: i64,
        uid: i64,
        symbol: i32,
        new_price: i64,
    ) AsyncSubmitError!u64 {
        const command = makeMoveOrderCommand(order_id, uid, symbol, new_price);
        return self.submitCommandAsync(mode, &command);
    }

    pub fn cancelOrderAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        order_id: i64,
        uid: i64,
        symbol: i32,
    ) AsyncSubmitError!u64 {
        const command = makeCancelOrderCommand(order_id, uid, symbol);
        return self.submitCommandAsync(mode, &command);
    }

    pub fn reduceOrderAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        order_id: i64,
        uid: i64,
        symbol: i32,
        reduce_size: i64,
    ) AsyncSubmitError!u64 {
        const command = makeReduceOrderCommand(order_id, uid, symbol, reduce_size);
        return self.submitCommandAsync(mode, &command);
    }

    pub fn groupingControlAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        timestamp_ns: i64,
        grouping_mode: i64,
    ) AsyncSubmitError!u64 {
        const command = makeGroupingControlCommand(timestamp_ns, grouping_mode);
        return self.submitCommandAsync(mode, &command);
    }

    pub fn resetAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
    ) AsyncSubmitError!u64 {
        const command = makeResetCommand();
        return self.submitCommandAsync(mode, &command);
    }

    pub fn nopAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
    ) AsyncSubmitError!u64 {
        const command = makeNopCommand();
        return self.submitCommandAsync(mode, &command);
    }

    pub fn requestOrderBookAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        symbol: i32,
        depth: usize,
    ) (AsyncSubmitError || HelperError)!u64 {
        const depth_i64 = std.math.cast(i64, depth) orelse return error.OrderBookDepthOverflow;
        const command = makeRequestOrderBookCommandI64(symbol, depth_i64);
        return self.submitCommandAsync(mode, &command);
    }

    pub fn persistStateAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        snapshot_id: i64,
        timestamp_ns: i64,
    ) AsyncSubmitError!PersistStateRequestPair {
        const matching = makePersistStateMatchingCommand(snapshot_id, timestamp_ns);
        const risk_cmd = makePersistStateRiskCommand(snapshot_id, timestamp_ns);
        return .{
            .matching_request_id = try self.submitCommandAsync(mode, &matching),
            .risk_request_id = try self.submitCommandAsync(mode, &risk_cmd),
        };
    }

    pub fn drainPending(self: *ExchangeApiSharded) DrainPendingError!usize {
        var processed: usize = 0;
        defer self.dropProcessedPending(processed);

        while (processed < self.pending_queue.items.len) {
            const entry = self.pending_queue.items[processed];
            var command = entry.command;
            var response = try self.executeWithMode(entry.mode, &command);
            errdefer response.deinit(self.allocator);
            try self.ready_results.put(self.allocator, entry.request_id, response);
            processed += 1;
        }
        return processed;
    }

    pub fn drainAsyncPending(self: *ExchangeApiSharded) DrainPendingError!usize {
        return self.drainPending();
    }

    pub fn takeResult(self: *ExchangeApiSharded, request_id: u64) ?kernel_mod.CommandResponse {
        const removed = self.ready_results.fetchRemove(request_id) orelse return null;
        return removed.value;
    }

    pub fn takeAsyncResult(self: *ExchangeApiSharded, request_id: u64) ?kernel_mod.CommandResponse {
        return self.takeResult(request_id);
    }

    pub fn takeResultCode(self: *ExchangeApiSharded, request_id: u64) ?cmd.CommandResultCode {
        var response = self.takeResult(request_id) orelse return null;
        defer response.deinit(self.allocator);
        return response.result_code;
    }

    pub fn takeFullResponse(
        self: *ExchangeApiSharded,
        request_id: u64,
    ) ?kernel_mod.CommandResponse {
        return self.takeResult(request_id);
    }

    pub fn takeOrderBookSnapshot(
        self: *ExchangeApiSharded,
        request_id: u64,
    ) TakeOrderBookSnapshotError!?orderbook_naive.L2MarketData {
        var response = self.takeFullResponse(request_id) orelse return null;
        errdefer response.deinit(self.allocator);

        const snapshot = response.l2_snapshot orelse return error.MissingOrderBookSnapshot;
        response.l2_snapshot = null;
        response.deinit(self.allocator);
        return snapshot;
    }

    pub fn takeReportResponse(
        self: *ExchangeApiSharded,
        request_id: u64,
    ) TakeReportResponseError!?kernel_mod.ReportResponse {
        var response = self.takeFullResponse(request_id) orelse return null;
        errdefer response.deinit(self.allocator);

        const report = response.report_response orelse return error.MissingReportResponse;
        response.report_response = null;
        response.deinit(self.allocator);
        return report;
    }

    pub fn takeStateHashDetailed(
        self: *ExchangeApiSharded,
        request_id: u64,
    ) TakeTypedReportError!?kernel_mod.DetailedStateHashReport {
        var report = (try self.takeReportResponse(request_id)) orelse return null;
        errdefer report.deinit(self.allocator);
        return switch (report) {
            .state_hash_detailed => |detailed| detailed,
            else => error.UnexpectedReportResponseType,
        };
    }

    pub fn takeSingleUserReport(
        self: *ExchangeApiSharded,
        request_id: u64,
    ) TakeTypedReportError!?kernel_mod.SingleUserReport {
        var report = (try self.takeReportResponse(request_id)) orelse return null;
        errdefer report.deinit(self.allocator);
        return switch (report) {
            .single_user => |single_user| single_user,
            else => error.UnexpectedReportResponseType,
        };
    }

    pub fn takeTotalCurrencyBalanceReport(
        self: *ExchangeApiSharded,
        request_id: u64,
    ) TakeTypedReportError!?kernel_mod.TotalCurrencyBalanceReport {
        var report = (try self.takeReportResponse(request_id)) orelse return null;
        errdefer report.deinit(self.allocator);
        return switch (report) {
            .total_currency_balance => |balance| balance,
            else => error.UnexpectedReportResponseType,
        };
    }

    pub fn processCommandResultCode(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command: *const cmd.OrderCommand,
    ) ProcessCommandResultCodeError!cmd.CommandResultCode {
        const handle = try self.submitCommandAsync(mode, command);
        _ = try self.drainPending();
        return self.takeResultCode(handle) orelse error.MissingAsyncResult;
    }

    pub fn processReport(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    ) ProcessReportError!kernel_mod.ReportResponse {
        const handle = try self.submitBinaryReportQueryAsync(mode, transfer_id, timestamp, query);
        _ = try self.drainPending();
        return (try self.takeReportResponse(handle)) orelse error.MissingAsyncResult;
    }

    pub fn processStateHashReport(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
    ) ProcessTypedReportError!kernel_mod.DetailedStateHashReport {
        const handle = try self.requestStateHashAsync(mode, transfer_id, timestamp);
        _ = try self.drainPending();
        return (try self.takeStateHashDetailed(handle)) orelse error.MissingAsyncResult;
    }

    pub fn processSingleUserReport(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        uid: i64,
    ) ProcessTypedReportError!kernel_mod.SingleUserReport {
        const handle = try self.requestSingleUserReportAsync(mode, transfer_id, timestamp, uid);
        _ = try self.drainPending();
        return (try self.takeSingleUserReport(handle)) orelse error.MissingAsyncResult;
    }

    pub fn processTotalCurrencyBalanceReport(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
    ) ProcessTypedReportError!kernel_mod.TotalCurrencyBalanceReport {
        const handle = try self.requestTotalCurrencyBalanceAsync(mode, transfer_id, timestamp);
        _ = try self.drainPending();
        return (try self.takeTotalCurrencyBalanceReport(handle)) orelse error.MissingAsyncResult;
    }

    pub fn processOrderBookRequest(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        symbol: i32,
        depth: usize,
    ) ProcessOrderBookError!orderbook_naive.L2MarketData {
        const handle = try self.orderBookRequestAsync(mode, symbol, depth);
        _ = try self.drainPending();
        return (try self.takeOrderBookSnapshot(handle)) orelse error.MissingAsyncResult;
    }

    pub fn isPersistStateResultReady(
        self: *const ExchangeApiSharded,
        requests: PersistStateRequestPair,
    ) bool {
        return self.isResultReady(requests.matching_request_id) and
            self.isResultReady(requests.risk_request_id);
    }

    pub fn takePersistStateResult(
        self: *ExchangeApiSharded,
        requests: PersistStateRequestPair,
    ) ?cmd.CommandResultCode {
        if (!self.isPersistStateResultReady(requests)) return null;

        var matching = self.takeResult(requests.matching_request_id) orelse return null;
        var risk_cmd = self.takeResult(requests.risk_request_id) orelse {
            matching.deinit(self.allocator);
            return null;
        };
        defer matching.deinit(self.allocator);
        defer risk_cmd.deinit(self.allocator);

        return cmd.CommandResultCode.mergeToFirstFailed(&.{
            matching.result_code,
            risk_cmd.result_code,
        });
    }

    pub fn isResultReady(self: *const ExchangeApiSharded, request_id: u64) bool {
        return self.ready_results.contains(request_id);
    }

    pub fn isAsyncResultReady(self: *const ExchangeApiSharded, request_id: u64) bool {
        return self.isResultReady(request_id);
    }

    pub fn pendingCount(self: *const ExchangeApiSharded) usize {
        return self.pending_queue.items.len;
    }

    pub fn asyncPendingCount(self: *const ExchangeApiSharded) usize {
        return self.pendingCount();
    }

    pub fn readyCount(self: *const ExchangeApiSharded) usize {
        return self.ready_results.count();
    }

    pub fn asyncReadyCount(self: *const ExchangeApiSharded) usize {
        return self.readyCount();
    }

    pub fn runtimePendingCount(self: *const ExchangeApiSharded) usize {
        return self.runtime_pending.items.len;
    }

    pub fn setResultSink(self: *ExchangeApiSharded, sink: runtime_mod.ResultSink) void {
        self.runtime_sink = sink;
    }

    pub fn setSimpleEventsHandler(self: *ExchangeApiSharded, handler: SimpleEventsHandler) void {
        self.clearSimpleEventsHandler();
        self.simple_events_processor = simple_events_mod.SimpleEventsProcessor.init(
            self.allocator,
            handler,
        );
    }

    pub fn clearSimpleEventsHandler(self: *ExchangeApiSharded) void {
        if (self.simple_events_processor) |*processor| {
            processor.deinit();
            self.simple_events_processor = null;
        }
    }

    pub fn enqueueRuntime(
        self: *ExchangeApiSharded,
        command: cmd.OrderCommand,
    ) EnqueueRuntimeError!void {
        if (self.core.isStopped()) return error.CoreStopped;
        try self.runtime_pending.append(self.allocator, command);
    }

    pub fn drainRuntime(self: *ExchangeApiSharded) RuntimeDrainError!runtime_mod.DrainOutcome {
        if (self.core.isStopped()) return error.CoreStopped;

        var outcome: runtime_mod.DrainOutcome = .{};
        var processed: usize = 0;
        while (processed < self.runtime_pending.items.len) {
            var command = self.runtime_pending.items[processed];
            var response = try self.executeWithMode(.routed, &command);
            defer response.deinit(self.allocator);

            outcome.groups_processed += 1;
            outcome.commands_processed += 1;
            outcome.last_merged_result = response.result_code;

            publishRuntimeResult(self.runtime_sink, .{
                .sequence = response.sequence,
                .command = command.command,
                .uid = command.uid,
                .symbol = command.symbol,
                .result_code = response.result_code,
            });

            processed += 1;
        }

        self.runtime_pending.clearRetainingCapacity();
        return outcome;
    }

    pub fn queryUserBalance(
        self: *ExchangeApiSharded,
        uid: i64,
        currency: i32,
    ) ReportError!?i64 {
        return self.core.queryUserBalance(uid, currency);
    }

    pub fn queryReport(
        self: *ExchangeApiSharded,
        query: kernel_mod.ReportQuery,
    ) QueryReportError!kernel_mod.ReportResponse {
        return self.core.queryReport(query);
    }

    pub fn queryTotalCurrencyBalanceReport(
        self: *ExchangeApiSharded,
    ) QueryReportError!kernel_mod.TotalCurrencyBalanceReport {
        return self.core.aggregateTotalCurrencyBalance();
    }

    pub fn queryUserBalanceReport(
        self: *ExchangeApiSharded,
        uid: i64,
        currency: i32,
    ) QueryReportError!?i64 {
        return self.core.queryUserBalance(uid, currency);
    }

    pub fn queryOrderBookReport(
        self: *ExchangeApiSharded,
        symbol: i32,
        depth: usize,
    ) QueryReportError!kernel_mod.ReportResponse {
        return self.queryReport(.{
            .order_book = .{
                .symbol = symbol,
                .depth = depth,
            },
        });
    }

    pub fn queryRiskStateHash(self: *ExchangeApiSharded) QueryTypedReportError!u64 {
        var report = try self.queryReport(.risk_state_hash);
        defer report.deinit(self.allocator);
        return switch (report) {
            .risk_state_hash => |value| value,
            else => error.UnexpectedReportResponseType,
        };
    }

    pub fn queryStateHashDetailedReport(
        self: *ExchangeApiSharded,
    ) QueryTypedReportError!kernel_mod.DetailedStateHashReport {
        var report = try self.queryReport(.state_hash_detailed);
        errdefer report.deinit(self.allocator);
        return switch (report) {
            .state_hash_detailed => |detailed| detailed,
            else => error.UnexpectedReportResponseType,
        };
    }

    pub fn queryFeeCollectedReport(
        self: *ExchangeApiSharded,
        currency: i32,
    ) QueryTypedReportError!i64 {
        var report = try self.queryReport(.{ .fee_collected = currency });
        defer report.deinit(self.allocator);
        return switch (report) {
            .fee_collected => |value| value,
            else => error.UnexpectedReportResponseType,
        };
    }

    pub fn queryFeeCollected(
        self: *ExchangeApiSharded,
        currency: i32,
    ) QueryTypedReportError!i64 {
        return self.queryFeeCollectedReport(currency);
    }

    pub fn querySingleUserReport(
        self: *ExchangeApiSharded,
        uid: i64,
    ) QueryTypedReportError!kernel_mod.SingleUserReport {
        var report = try self.queryReport(.{ .single_user = uid });
        errdefer report.deinit(self.allocator);
        return switch (report) {
            .single_user => |user| user,
            else => error.UnexpectedReportResponseType,
        };
    }

    pub fn queryOrderBook(
        self: *ExchangeApiSharded,
        symbol: i32,
        depth: usize,
    ) QueryTypedReportError!orderbook_naive.L2MarketData {
        var report = try self.queryOrderBookReport(symbol, depth);
        errdefer report.deinit(self.allocator);
        return switch (report) {
            .order_book => |snapshot| snapshot,
            else => error.UnexpectedReportResponseType,
        };
    }

    pub fn aggregateTotalCurrencyBalance(
        self: *ExchangeApiSharded,
    ) AggregateError!kernel_mod.TotalCurrencyBalanceReport {
        return self.core.aggregateTotalCurrencyBalance();
    }

    pub fn detailedHashFingerprint(self: *ExchangeApiSharded) ReportError!u64 {
        return self.core.detailedHashFingerprint();
    }

    fn ensureTransferMatchesSymbols(
        self: *const ExchangeApiSharded,
        transfer_id: i32,
        symbols: []const common.CoreSymbolSpecification,
    ) error{BinaryTransferShardMismatch}!void {
        const transfer_shard = self.core.shardIndexForSymbol(transfer_id);
        for (symbols) |symbol| {
            const symbol_shard = self.core.shardIndexForSymbol(symbol.symbol_id);
            if (symbol_shard != transfer_shard) {
                return error.BinaryTransferShardMismatch;
            }
        }
    }

    fn ensureTransferMatchesUsers(
        self: *const ExchangeApiSharded,
        transfer_id: i32,
        users: []const BinaryUserAccountsBatch,
    ) error{BinaryTransferShardMismatch}!void {
        const transfer_shard = self.core.shardIndexForSymbol(transfer_id);
        for (users) |user_batch| {
            const uid_shard = self.core.shardIndexForUid(user_batch.uid);
            if (uid_shard != transfer_shard) {
                return error.BinaryTransferShardMismatch;
            }
        }
    }

    fn ensureTransferMatchesQuery(
        self: *const ExchangeApiSharded,
        transfer_id: i32,
        query: BinaryReportQuery,
    ) error{BinaryTransferShardMismatch}!void {
        switch (query) {
            .single_user => |uid| {
                const transfer_shard = self.core.shardIndexForSymbol(transfer_id);
                const uid_shard = self.core.shardIndexForUid(uid);
                if (uid_shard != transfer_shard) {
                    return error.BinaryTransferShardMismatch;
                }
            },
            else => {},
        }
    }

    fn executeWithMode(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command: *cmd.OrderCommand,
    ) ExecuteError!kernel_mod.CommandResponse {
        const input = command.*;
        var response = try self.executeWithModeNoJournal(mode, command);
        try self.registerPersistDescriptorIfNeeded(command, response.sequence, response.result_code);
        if (self.journal) |*journal| {
            try journal.entries.append(self.allocator, .{
                .sequence = response.sequence,
                .input = input,
                .result_code = response.result_code,
            });
        }
        self.publishSimpleEvents(command, &response);
        return response;
    }

    fn executeWithModeNoJournal(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command: *cmd.OrderCommand,
    ) ExecuteError!kernel_mod.CommandResponse {
        return switch (mode) {
            .routed => self.core.executeRouted(command),
            .split => self.core.executeSplit(command),
        };
    }

    fn submitRawCommand(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command: cmd.OrderCommand,
    ) SubmitApiCommandError!void {
        try self.submitApiCommand(mode, .{
            .raw_command = command,
        });
    }

    fn registerCallbackForHandle(
        self: *ExchangeApiSharded,
        handle: ApiCommandAsyncHandle,
        callback: CommandResponseCallback,
    ) CallbackRegistrationError!void {
        const request_id = try requestIdFromHandle(handle);
        if (self.callbackRegistrationIndex(request_id)) |idx| {
            self.callback_registrations[idx].callback = callback;
            return;
        }
        if (self.callback_registrations_len >= MaxCallbackRegistrations) {
            return error.CallbackRegistryFull;
        }
        self.callback_registrations[self.callback_registrations_len] = .{
            .request_id = request_id,
            .callback = callback,
        };
        self.callback_registrations_len += 1;
    }

    fn callbackRegistrationIndex(
        self: *const ExchangeApiSharded,
        request_id: u64,
    ) ?usize {
        for (self.callback_registrations[0..self.callback_registrations_len], 0..) |registration, idx| {
            if (registration.request_id == request_id) {
                return idx;
            }
        }
        return null;
    }

    fn removeCallbackRegistrationAt(self: *ExchangeApiSharded, idx: usize) void {
        std.debug.assert(idx < self.callback_registrations_len);
        const len = self.callback_registrations_len;
        if (idx + 1 < len) {
            std.mem.copyForwards(
                CallbackRegistration,
                self.callback_registrations[idx .. len - 1],
                self.callback_registrations[idx + 1 .. len],
            );
        }
        self.callback_registrations_len = len - 1;
    }

    fn submitCommandAsyncWithCallback(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command: ApiCommand,
        callback: CommandResponseCallback,
    ) SubmitCommandWithCallbackError!void {
        const handle = try self.submitCommandAsyncFullResponse(mode, command);
        try self.registerResultCallback(handle, callback);
    }

    fn nextRequestId(self: *ExchangeApiSharded) error{RequestIdOverflow}!u64 {
        if (self.next_request_id == std.math.maxInt(u64)) {
            return error.RequestIdOverflow;
        }
        self.next_request_id += 1;
        return self.next_request_id;
    }

    fn dropProcessedPending(self: *ExchangeApiSharded, processed: usize) void {
        if (processed == 0) return;
        if (processed >= self.pending_queue.items.len) {
            self.pending_queue.clearRetainingCapacity();
            return;
        }

        const remaining = self.pending_queue.items.len - processed;
        std.mem.copyForwards(
            PendingAsyncCommand,
            self.pending_queue.items[0..remaining],
            self.pending_queue.items[processed..],
        );
        self.pending_queue.items.len = remaining;
    }

    fn trimLoadedJournalAfterReplay(
        self: *ExchangeApiSharded,
        loaded: *journal_mod.CommandJournal,
        first_replayed_index: ?usize,
        replayed: usize,
    ) void {
        _ = self;
        if (replayed == 0) {
            loaded.entries.clearRetainingCapacity();
            return;
        }

        const start = first_replayed_index orelse loaded.entries.items.len;
        std.debug.assert(start + replayed <= loaded.entries.items.len);
        if (start != 0) {
            const JournalEntry = std.meta.Child(@TypeOf(loaded.entries.items));
            std.mem.copyForwards(
                JournalEntry,
                loaded.entries.items[0..replayed],
                loaded.entries.items[start .. start + replayed],
            );
        }
        loaded.entries.items.len = replayed;
    }

    fn registerSnapshotDescriptor(
        self: *ExchangeApiSharded,
        snapshot_id: u64,
        sequence: u64,
        timestamp_ns: u64,
    ) persistence_mod.CreateSnapshotError!void {
        self.current_snapshot_ref = try self.descriptors.createNextSnapshot(
            self.current_snapshot_ref,
            snapshot_id,
            sequence,
            timestamp_ns,
        );
    }

    fn registerPersistDescriptorIfNeeded(
        self: *ExchangeApiSharded,
        command: *const cmd.OrderCommand,
        sequence: u64,
        result_code: cmd.CommandResultCode,
    ) DescriptorTrackingError!void {
        if (command.command != .persist_state_risk) return;
        if (result_code != .success) return;

        const snapshot_id = std.math.cast(u64, command.order_id) orelse return error.InvalidPersistSnapshotId;
        const timestamp_ns = std.math.cast(u64, command.timestamp) orelse return error.InvalidPersistTimestamp;
        try self.registerSnapshotDescriptor(snapshot_id, sequence, timestamp_ns);
    }

    fn bootstrapSnapshotPhase(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_path: ?[]const u8,
        outcome: *BootstrapOutcome,
    ) BootstrapError!void {
        const init_state = self.config.initial_state;
        if (!init_state.fromSnapshot()) return;
        const path = snapshot_path orelse {
            outcome.snapshot_requested = true;
            if (init_state.throw_if_snapshot_not_found) return error.SnapshotPathRequired;
            return;
        };
        try self.bootstrapSnapshotPhaseWithId(
            dir,
            path,
            init_state.snapshot_id,
            init_state.snapshot_base_seq,
            outcome,
        );
    }

    fn bootstrapSnapshotPhaseWithId(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_path: []const u8,
        snapshot_id: u64,
        snapshot_base_seq: u64,
        outcome: *BootstrapOutcome,
    ) BootstrapError!void {
        const init_state = self.config.initial_state;
        outcome.snapshot_requested = true;
        var snapshot = snapshot_mod.KernelSnapshot.readFromFile(self.allocator, dir, snapshot_path) catch |err| switch (err) {
            error.FileNotFound => {
                if (init_state.throw_if_snapshot_not_found) return error.SnapshotFileNotFound;
                return;
            },
            else => return err,
        };
        defer snapshot.deinit();

        outcome.snapshot_found = true;
        try snapshot.restoreIntoKernel(self.kernelRef());
        self.kernelRef().next_seq = snapshot_base_seq;
        self.core.kernels.next_seq = snapshot_base_seq;
        try self.registerSnapshotDescriptor(snapshot_id, snapshot_base_seq, 0);
    }

    fn startupFromConfiguredPersistenceWithSnapshotId(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_module_type: PersistenceModuleType,
        snapshot_instance_id: u32,
        journal_partition_id: u16,
        snapshot_id: u64,
    ) StartupFromConfiguredPersistenceError!BootstrapOutcome {
        const snapshot_path = try self.defaultPersistenceSnapshotPath(
            self.allocator,
            snapshot_id,
            snapshot_module_type,
            snapshot_instance_id,
        );
        defer self.allocator.free(snapshot_path);

        var journal_path: ?[]u8 = null;
        defer if (journal_path) |path| self.allocator.free(path);
        if (self.config.initial_state.journal_timestamp_ns != 0) {
            journal_path = try self.defaultPersistenceJournalPath(
                self.allocator,
                snapshot_id,
                journal_partition_id,
            );
        }

        const snapshot_base_seq = self.discoverSnapshotBaseSeqFromMainLog(
            dir,
            snapshot_id,
            snapshot_module_type,
            snapshot_instance_id,
        ) orelse self.config.initial_state.snapshot_base_seq;

        try self.startup();
        var outcome: BootstrapOutcome = .{};
        try self.bootstrapSnapshotPhaseWithId(
            dir,
            snapshot_path,
            snapshot_id,
            snapshot_base_seq,
            &outcome,
        );

        var first_partition_found = false;
        if (self.config.initial_state.journal_timestamp_ns != 0) {
            const path = journal_path orelse return error.JournalPathRequired;
            first_partition_found = true;
            outcome.journal_replayed_commands = self.replayJournalFromFile(dir, path) catch |err| switch (err) {
                error.FileNotFound => blk: {
                    first_partition_found = false;
                    break :blk 0;
                },
                else => return err,
            };
        }

        if (first_partition_found and journal_partition_id < std.math.maxInt(u16)) {
            const replayed_tail = try self.replayJournalFromDefaultPathRange(
                dir,
                snapshot_id,
                journal_partition_id + 1,
            );
            outcome.journal_replayed_commands = std.math.add(
                usize,
                outcome.journal_replayed_commands,
                replayed_tail,
            ) catch return error.JournalReplayCountOverflow;
        }

        return outcome;
    }

    fn startupFromConfiguredPersistenceWithSnapshotIdStrictMainLog(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_module_type: PersistenceModuleType,
        snapshot_instance_id: u32,
        journal_partition_id: u16,
        snapshot_id: u64,
    ) StartupFromDiscoveredConfiguredPersistenceStrictError!BootstrapOutcome {
        const snapshot_path = try self.defaultPersistenceSnapshotPath(
            self.allocator,
            snapshot_id,
            snapshot_module_type,
            snapshot_instance_id,
        );
        defer self.allocator.free(snapshot_path);

        var journal_path: ?[]u8 = null;
        defer if (journal_path) |path| self.allocator.free(path);
        if (self.config.initial_state.journal_timestamp_ns != 0) {
            journal_path = try self.defaultPersistenceJournalPath(
                self.allocator,
                snapshot_id,
                journal_partition_id,
            );
        }

        const discovered_seq = try self.discoverSnapshotBaseSeqFromMainLogStrict(
            dir,
            snapshot_id,
            snapshot_module_type,
            snapshot_instance_id,
        );
        const snapshot_base_seq = discovered_seq orelse self.config.initial_state.snapshot_base_seq;

        try self.startup();
        var outcome: BootstrapOutcome = .{};
        try self.bootstrapSnapshotPhaseWithId(
            dir,
            snapshot_path,
            snapshot_id,
            snapshot_base_seq,
            &outcome,
        );

        var first_partition_found = false;
        if (self.config.initial_state.journal_timestamp_ns != 0) {
            const path = journal_path orelse return error.JournalPathRequired;
            first_partition_found = true;
            outcome.journal_replayed_commands = self.replayJournalFromFile(dir, path) catch |err| switch (err) {
                error.FileNotFound => blk: {
                    first_partition_found = false;
                    break :blk 0;
                },
                else => return err,
            };
        }

        if (first_partition_found and journal_partition_id < std.math.maxInt(u16)) {
            const replayed_tail = try self.replayJournalFromDefaultPathRange(
                dir,
                snapshot_id,
                journal_partition_id + 1,
            );
            outcome.journal_replayed_commands = std.math.add(
                usize,
                outcome.journal_replayed_commands,
                replayed_tail,
            ) catch return error.JournalReplayCountOverflow;
        }

        return outcome;
    }

    fn discoverLatestDefaultSnapshotId(
        self: *const ExchangeApiSharded,
        dir: std.Io.Dir,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) (std.Io.Dir.OpenError || std.Io.Dir.Iterator.Error)!?u64 {
        const storage_folder = self.config.serialization.storage_folder;
        var storage_dir = dir.openDir(io_context.current(), storage_folder, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer storage_dir.close(io_context.current());

        var it = storage_dir.iterate();
        var latest_snapshot_id: ?u64 = null;
        while (try it.next(io_context.current())) |entry| {
            const snapshot_id = parseSnapshotIdFromDefaultFileName(
                entry.name,
                self.config.initial_state.exchange_id,
                module_type,
                instance_id,
            ) orelse continue;
            if (latest_snapshot_id == null or snapshot_id > latest_snapshot_id.?) {
                latest_snapshot_id = snapshot_id;
            }
        }
        return latest_snapshot_id;
    }

    fn discoverSnapshotBaseSeqFromMainLog(
        self: *const ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) ?u64 {
        const main_log_path = self.defaultPersistenceMainLogPath(self.allocator) catch return null;
        defer self.allocator.free(main_log_path);

        var main_log_file = dir.openFile(io_context.current(), main_log_path, .{}) catch return null;
        defer main_log_file.close(io_context.current());
        const main_log_contents = io_context.readToEndAlloc(main_log_file, self.allocator, 8 * 1024 * 1024) catch return null;
        defer self.allocator.free(main_log_contents);

        var best_seq: ?u64 = null;
        var lines = std.mem.splitScalar(u8, main_log_contents, '\n');
        while (lines.next()) |line| {
            const seq = parseMainLogSequenceForSnapshot(
                line,
                snapshot_id,
                module_type,
                instance_id,
            ) orelse continue;
            if (best_seq == null or seq > best_seq.?) {
                best_seq = seq;
            }
        }
        return best_seq;
    }

    fn discoverSnapshotBaseSeqFromMainLogStrict(
        self: *const ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) MainLogSequenceParseError!?u64 {
        const main_log_path = self.defaultPersistenceMainLogPath(self.allocator) catch return null;
        defer self.allocator.free(main_log_path);

        var main_log_file = dir.openFile(io_context.current(), main_log_path, .{}) catch return null;
        defer main_log_file.close(io_context.current());
        const main_log_contents = io_context.readToEndAlloc(main_log_file, self.allocator, 8 * 1024 * 1024) catch return null;
        defer self.allocator.free(main_log_contents);

        var best_seq: ?u64 = null;
        var lines = std.mem.splitScalar(u8, main_log_contents, '\n');
        while (lines.next()) |line| {
            const parsed = try parseMainLogEntryStrict(line) orelse continue;
            if (parsed.snapshot_id != snapshot_id) continue;
            if (!std.mem.eql(u8, parsed.type_code, persistence_paths.moduleCode(module_type))) continue;
            if (parsed.instance_id != instance_id) continue;
            if (best_seq == null or parsed.seq > best_seq.?) {
                best_seq = parsed.seq;
            }
        }
        return best_seq;
    }

    fn appendDefaultPersistenceMainLogEntry(
        self: *const ExchangeApiSharded,
        dir: std.Io.Dir,
        seq: u64,
        timestamp_ns: u64,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) (PersistencePathError || StorageFolderEnsureError || MainLogWriteError)!void {
        try self.ensureConfiguredStorageFolderExists(dir);
        const path = try self.defaultPersistenceMainLogPath(self.allocator);
        defer self.allocator.free(path);
        try appendMainLogEntryToFile(
            dir,
            path,
            seq,
            timestamp_ns,
            snapshot_id,
            module_type,
            instance_id,
        );
    }

    fn ensureConfiguredStorageFolderExists(self: *const ExchangeApiSharded, dir: std.Io.Dir) StorageFolderEnsureError!void {
        const storage_folder = self.config.serialization.storage_folder;
        try dir.createDirPath(io_context.current(), storage_folder);
    }

    fn publishSimpleEvents(
        self: *ExchangeApiSharded,
        command: *const cmd.OrderCommand,
        response: *const kernel_mod.CommandResponse,
    ) void {
        if (self.simple_events_processor) |*processor| {
            processor.accept(
                command,
                response.sequence,
                response,
                response.matcher_events_head,
            ) catch |err| {
                log.err("simple-events callback failed cmd={s} seq={d} err={}", .{
                    @tagName(command.command),
                    response.sequence,
                    err,
                });
            };
        }
    }
};

fn parseSnapshotIdFromDefaultFileName(
    file_name: []const u8,
    exchange_id: []const u8,
    module_type: PersistenceModuleType,
    instance_id: u32,
) ?u64 {
    if (!std.mem.startsWith(u8, file_name, exchange_id)) return null;

    const marker = "_snapshot_";
    if (file_name.len <= exchange_id.len + marker.len) return null;
    if (!std.mem.eql(u8, file_name[exchange_id.len .. exchange_id.len + marker.len], marker)) return null;

    const after_marker = file_name[exchange_id.len + marker.len ..];
    const id_sep = std.mem.indexOfScalar(u8, after_marker, '_') orelse return null;
    const snapshot_id_slice = after_marker[0..id_sep];
    if (snapshot_id_slice.len == 0) return null;
    const snapshot_id = std.fmt.parseInt(u64, snapshot_id_slice, 10) catch return null;

    const module_and_suffix = after_marker[id_sep + 1 ..];
    const module_code = persistence_paths.moduleCode(module_type);
    if (!std.mem.startsWith(u8, module_and_suffix, module_code)) return null;

    const ext = ".ecs";
    if (!std.mem.endsWith(u8, module_and_suffix, ext)) return null;

    const instance_slice = module_and_suffix[module_code.len .. module_and_suffix.len - ext.len];
    if (instance_slice.len == 0) return null;
    const parsed_instance = std.fmt.parseInt(u32, instance_slice, 10) catch return null;
    if (parsed_instance != instance_id) return null;

    return snapshot_id;
}

fn parseMainLogSequenceForSnapshot(
    line: []const u8,
    snapshot_id: u64,
    module_type: PersistenceModuleType,
    instance_id: u32,
) ?u64 {
    if (line.len == 0) return null;

    var tokens = std.mem.tokenizeScalar(u8, line, ' ');
    _ = tokens.next() orelse return null;
    const seq_token = tokens.next() orelse return null;
    _ = tokens.next() orelse return null;
    const snapshot_token = tokens.next() orelse return null;
    const type_token = tokens.next() orelse return null;
    const instance_token = tokens.next() orelse return null;

    const seq = parsePrefixedU64Token(seq_token, "seq=") orelse return null;
    const parsed_snapshot_id = parsePrefixedU64Token(snapshot_token, "snapshotId=") orelse return null;
    if (parsed_snapshot_id != snapshot_id) return null;

    if (!std.mem.startsWith(u8, type_token, "type=")) return null;
    const type_code = type_token["type=".len..];
    if (!std.mem.eql(u8, type_code, persistence_paths.moduleCode(module_type))) return null;

    const parsed_instance = parsePrefixedU32Token(instance_token, "instance=") orelse return null;
    if (parsed_instance != instance_id) return null;

    return seq;
}

const ParsedMainLogEntry = struct {
    seq: u64,
    snapshot_id: u64,
    type_code: []const u8,
    instance_id: u32,
};

fn parseMainLogEntryStrict(line: []const u8) MainLogSequenceParseError!?ParsedMainLogEntry {
    const trimmed = std.mem.trim(u8, line, " \t\r");
    if (trimmed.len == 0) return null;

    var tokens = std.mem.tokenizeScalar(u8, trimmed, ' ');
    _ = tokens.next() orelse return error.MalformedMainLogEntry;

    var seq: ?u64 = null;
    var snapshot_id: ?u64 = null;
    var type_code: ?[]const u8 = null;
    var instance_id: ?u32 = null;

    while (tokens.next()) |token| {
        if (std.mem.startsWith(u8, token, "seq=")) {
            seq = parsePrefixedU64Token(token, "seq=") orelse return error.MalformedMainLogEntry;
        } else if (std.mem.startsWith(u8, token, "snapshotId=")) {
            snapshot_id = parsePrefixedU64Token(token, "snapshotId=") orelse return error.MalformedMainLogEntry;
        } else if (std.mem.startsWith(u8, token, "type=")) {
            const value = token["type=".len..];
            if (value.len == 0) return error.MalformedMainLogEntry;
            type_code = value;
        } else if (std.mem.startsWith(u8, token, "instance=")) {
            instance_id = parsePrefixedU32Token(token, "instance=") orelse return error.MalformedMainLogEntry;
        }
    }

    return .{
        .seq = seq orelse return error.MalformedMainLogEntry,
        .snapshot_id = snapshot_id orelse return error.MalformedMainLogEntry,
        .type_code = type_code orelse return error.MalformedMainLogEntry,
        .instance_id = instance_id orelse return error.MalformedMainLogEntry,
    };
}

fn parsePrefixedU64Token(token: []const u8, prefix: []const u8) ?u64 {
    if (!std.mem.startsWith(u8, token, prefix)) return null;
    const value = token[prefix.len..];
    if (value.len == 0) return null;
    return std.fmt.parseInt(u64, value, 10) catch return null;
}

fn parsePrefixedU32Token(token: []const u8, prefix: []const u8) ?u32 {
    if (!std.mem.startsWith(u8, token, prefix)) return null;
    const value = token[prefix.len..];
    if (value.len == 0) return null;
    return std.fmt.parseInt(u32, value, 10) catch return null;
}

fn appendMainLogEntryToFile(
    dir: std.Io.Dir,
    path: []const u8,
    seq: u64,
    timestamp_ns: u64,
    snapshot_id: u64,
    module_type: PersistenceModuleType,
    instance_id: u32,
) MainLogWriteError!void {
    var file = try dir.createFile(io_context.current(), path, .{
        .truncate = false,
    });
    defer file.close(io_context.current());
    const wall_clock_ms = io_context.realTimestampMs();

    var line_buf: [256]u8 = undefined;
    const line = try std.fmt.bufPrint(
        &line_buf,
        "{d} seq={d} timestampNs={d} snapshotId={d} type={s} instance={d}\n",
        .{
            wall_clock_ms,
            seq,
            timestamp_ns,
            snapshot_id,
            persistence_paths.moduleCode(module_type),
            instance_id,
        },
    );
    try io_context.appendAll(file, line);
}

fn currentWallClockTimestampNs() u64 {
    return io_context.realTimestampNs();
}

fn requestIdFromHandle(handle: ApiCommandAsyncHandle) error{UnsupportedPersistStateHandle}!u64 {
    return switch (handle) {
        .request_id => |request_id| request_id,
        .persist_state => error.UnsupportedPersistStateHandle,
    };
}

fn publishRuntimeResult(sink: runtime_mod.ResultSink, event: runtime_mod.ResultEvent) void {
    const callback = sink.on_result orelse return;
    callback(sink.context, event);
}

fn makeBalanceAdjustmentCommand(
    uid: i64,
    currency: i32,
    amount: i64,
    transaction_id: i64,
    adjustment_type: common.BalanceAdjustmentType,
) cmd.OrderCommand {
    return .{
        .command = .balance_adjustment,
        .uid = uid,
        .symbol = currency,
        .price = amount,
        .order_id = transaction_id,
        .order_type = switch (adjustment_type) {
            .adjustment => .gtc,
            .@"suspend" => .ioc,
        },
        .result_code = .new,
    };
}

fn makeAddUserCommand(uid: i64) cmd.OrderCommand {
    return .{
        .command = .add_user,
        .uid = uid,
        .result_code = .new,
    };
}

fn makeSuspendUserCommand(uid: i64) cmd.OrderCommand {
    return .{
        .command = .suspend_user,
        .uid = uid,
        .result_code = .new,
    };
}

fn makeResumeUserCommand(uid: i64) cmd.OrderCommand {
    return .{
        .command = .resume_user,
        .uid = uid,
        .result_code = .new,
    };
}

fn makePlaceOrderCommand(
    order_id: i64,
    uid: i64,
    symbol: i32,
    price: i64,
    reserve_bid_price: i64,
    size: i64,
    action: common.OrderAction,
    order_type: common.OrderType,
    user_cookie: i32,
) cmd.OrderCommand {
    return .{
        .command = .place_order,
        .order_id = order_id,
        .uid = uid,
        .symbol = symbol,
        .price = price,
        .reserve_bid_price = reserve_bid_price,
        .size = size,
        .action = action,
        .order_type = order_type,
        .user_cookie = user_cookie,
        .result_code = .new,
    };
}

fn makeMoveOrderCommand(
    order_id: i64,
    uid: i64,
    symbol: i32,
    new_price: i64,
) cmd.OrderCommand {
    return .{
        .command = .move_order,
        .order_id = order_id,
        .uid = uid,
        .symbol = symbol,
        .price = new_price,
        .result_code = .new,
    };
}

fn makeCancelOrderCommand(order_id: i64, uid: i64, symbol: i32) cmd.OrderCommand {
    return .{
        .command = .cancel_order,
        .order_id = order_id,
        .uid = uid,
        .symbol = symbol,
        .result_code = .new,
    };
}

fn makeReduceOrderCommand(
    order_id: i64,
    uid: i64,
    symbol: i32,
    reduce_size: i64,
) cmd.OrderCommand {
    return .{
        .command = .reduce_order,
        .order_id = order_id,
        .uid = uid,
        .symbol = symbol,
        .size = reduce_size,
        .result_code = .new,
    };
}

fn makeResetCommand() cmd.OrderCommand {
    return .{
        .command = .reset,
        .result_code = .new,
    };
}

fn makeGroupingControlCommand(timestamp_ns: i64, grouping_mode: i64) cmd.OrderCommand {
    return .{
        .command = .grouping_control,
        .order_id = grouping_mode,
        .timestamp = timestamp_ns,
        .result_code = .new,
    };
}

fn makeNopCommand() cmd.OrderCommand {
    return .{
        .command = .nop,
        .result_code = .new,
    };
}

fn makePersistStateMatchingCommand(snapshot_id: i64, timestamp_ns: i64) cmd.OrderCommand {
    return .{
        .command = .persist_state_matching,
        .order_id = snapshot_id,
        .timestamp = timestamp_ns,
        .result_code = .new,
    };
}

fn makePersistStateRiskCommand(snapshot_id: i64, timestamp_ns: i64) cmd.OrderCommand {
    return .{
        .command = .persist_state_risk,
        .order_id = snapshot_id,
        .timestamp = timestamp_ns,
        .result_code = .new,
    };
}

fn makeRequestOrderBookCommandI64(symbol: i32, depth: i64) cmd.OrderCommand {
    return .{
        .command = .order_book_request,
        .symbol = symbol,
        .size = depth,
        .result_code = .new,
    };
}

fn ensureBinaryRoutedMode(mode: ExecutionMode) error{SplitBinaryPayloadUnsupported}!void {
    switch (mode) {
        .routed => {},
        .split => return error.SplitBinaryPayloadUnsupported,
    }
}

fn makeBinaryFrameCommand(
    command_type: cmd.OrderCommandType,
    transfer_id: i32,
    last_flag: i32,
    timestamp: i64,
    words: [binary_frames.FrameWordsPerCommand]i64,
) cmd.OrderCommand {
    return .{
        .command = command_type,
        .user_cookie = transfer_id,
        .symbol = last_flag,
        .timestamp = timestamp,
        .order_id = words[0],
        .price = words[1],
        .reserve_bid_price = words[2],
        .size = words[3],
        .uid = words[4],
        .result_code = .new,
    };
}

fn readBigEndianWordPadded(payload: []const u8, word_index: usize) i64 {
    const start = word_index * @sizeOf(i64);
    if (start >= payload.len) return 0;

    const end = @min(start + @sizeOf(i64), payload.len);
    var raw: [8]u8 = [_]u8{0} ** 8;
    @memcpy(raw[0 .. end - start], payload[start..end]);
    return std.mem.readInt(i64, &raw, .big);
}

fn computeBinaryFrameLayout(payload_len: usize) error{ EmptyBinaryPayload, BinaryPayloadSizeOverflow }!BinaryFrameLayout {
    if (payload_len == 0) return error.EmptyBinaryPayload;

    const words_without_padding = std.math.divCeil(usize, payload_len, @sizeOf(i64)) catch
        return error.BinaryPayloadSizeOverflow;
    const frame_count = std.math.divCeil(usize, words_without_padding, binary_frames.FrameWordsPerCommand) catch
        return error.BinaryPayloadSizeOverflow;
    if (frame_count == 0) return error.BinaryPayloadSizeOverflow;

    return .{
        .words_without_padding = words_without_padding,
        .frame_count = frame_count,
    };
}

fn expectFlushResultVariant(result: *const ApiCommandResult) error{UnexpectedFlushResultVariant}!void {
    return switch (result.*) {
        .response => {},
        .persist_state => error.UnexpectedFlushResultVariant,
    };
}

const FlowOutcome = struct {
    codes: [7]cmd.CommandResultCode,
    ask_levels: usize,
    bid_levels: usize,
};

const CallbackCollector = struct {
    count: usize = 0,
    last_result: ?cmd.CommandResultCode = null,
    last_snapshot_present: bool = false,
    last_ask_levels: usize = 0,
    last_bid_levels: usize = 0,

    fn onResponse(context: ?*anyopaque, response: *const kernel_mod.CommandResponse) void {
        const self: *CallbackCollector = @ptrCast(@alignCast(context.?));
        self.count += 1;
        self.last_result = response.result_code;
        if (response.l2_snapshot) |snapshot| {
            self.last_snapshot_present = true;
            self.last_ask_levels = snapshot.asks.items.len;
            self.last_bid_levels = snapshot.bids.items.len;
        } else {
            self.last_snapshot_present = false;
            self.last_ask_levels = 0;
            self.last_bid_levels = 0;
        }
    }
};

const RuntimeSinkCollector = struct {
    allocator: std.mem.Allocator,
    events: std.ArrayListUnmanaged(runtime_mod.ResultEvent) = .empty,

    fn init(allocator: std.mem.Allocator) RuntimeSinkCollector {
        return .{ .allocator = allocator };
    }

    fn deinit(self: *RuntimeSinkCollector) void {
        self.events.deinit(self.allocator);
        self.* = undefined;
    }

    fn onResult(context: ?*anyopaque, event: runtime_mod.ResultEvent) void {
        const self: *RuntimeSinkCollector = @ptrCast(@alignCast(context.?));
        self.events.append(self.allocator, event) catch unreachable;
    }
};

const SimpleEventsCollector = struct {
    add_user_count: usize = 0,
    order_book_count: usize = 0,
    other_count: usize = 0,

    fn onCommandResult(
        context: ?*anyopaque,
        event: simple_events_mod.ApiCommandResult,
    ) void {
        const self: *SimpleEventsCollector = @ptrCast(@alignCast(context.?));
        switch (event.command) {
            .add_user => self.add_user_count += 1,
            .order_book_request => self.order_book_count += 1,
            else => self.other_count += 1,
        }
    }
};

fn runBasicFlow(api: *ExchangeApiSharded, mode: ExecutionMode) !FlowOutcome {
    const symbol: i32 = 241;
    const base_currency: i32 = 11;
    const quote_currency: i32 = 15;
    const maker_uid: i64 = 11_001;
    const taker_uid: i64 = 11_002;

    var outcome: FlowOutcome = .{
        .codes = [_]cmd.CommandResultCode{.new} ** 7,
        .ask_levels = 0,
        .bid_levels = 0,
    };

    const spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol,
        .symbol_type = .currency_exchange_pair,
        .base_currency = base_currency,
        .quote_currency = quote_currency,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
    });
    try api.addSymbolAll(spec);

    var add_maker = try api.addUser(mode, maker_uid);
    defer add_maker.deinit(api.allocator);
    outcome.codes[0] = add_maker.result_code;

    var add_taker = try api.addUser(mode, taker_uid);
    defer add_taker.deinit(api.allocator);
    outcome.codes[1] = add_taker.result_code;

    var dep_maker = try api.adjustUserBalance(mode, maker_uid, 1, base_currency, 1_000, .adjustment);
    defer dep_maker.deinit(api.allocator);
    outcome.codes[2] = dep_maker.result_code;

    var dep_taker = try api.adjustUserBalance(mode, taker_uid, 1, quote_currency, 1_000, .adjustment);
    defer dep_taker.deinit(api.allocator);
    outcome.codes[3] = dep_taker.result_code;

    var maker_ask = try api.placeOrder(mode, 9_101, maker_uid, symbol, 100, 100, 5, .ask, .gtc, 0);
    defer maker_ask.deinit(api.allocator);
    outcome.codes[4] = maker_ask.result_code;

    var taker_bid = try api.placeOrder(mode, 9_201, taker_uid, symbol, 100, 100, 5, .bid, .gtc, 0);
    defer taker_bid.deinit(api.allocator);
    outcome.codes[5] = taker_bid.result_code;

    var book = try api.requestOrderBook(mode, symbol, 8);
    defer book.deinit(api.allocator);
    outcome.codes[6] = book.result_code;
    const snapshot = book.l2_snapshot orelse return error.MissingOrderBookSnapshot;
    outcome.ask_levels = snapshot.asks.items.len;
    outcome.bid_levels = snapshot.bids.items.len;

    return outcome;
}

fn runBasicFlowAsync(api: *ExchangeApiSharded, mode: ExecutionMode) !FlowOutcome {
    const symbol: i32 = 241;
    const base_currency: i32 = 11;
    const quote_currency: i32 = 15;
    const maker_uid: i64 = 11_001;
    const taker_uid: i64 = 11_002;

    var outcome: FlowOutcome = .{
        .codes = [_]cmd.CommandResultCode{.new} ** 7,
        .ask_levels = 0,
        .bid_levels = 0,
    };

    const spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol,
        .symbol_type = .currency_exchange_pair,
        .base_currency = base_currency,
        .quote_currency = quote_currency,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
    });
    try api.addSymbolAll(spec);

    const add_maker = try api.addUserAsync(mode, maker_uid);
    const add_taker = try api.addUserAsync(mode, taker_uid);
    const dep_maker = try api.adjustUserBalanceAsync(
        mode,
        maker_uid,
        1,
        base_currency,
        1_000,
        .adjustment,
    );
    const dep_taker = try api.adjustUserBalanceAsync(
        mode,
        taker_uid,
        1,
        quote_currency,
        1_000,
        .adjustment,
    );
    const maker_ask = try api.placeOrderAsync(mode, 9_101, maker_uid, symbol, 100, 100, 5, .ask, .gtc, 0);
    const taker_bid = try api.placeOrderAsync(mode, 9_201, taker_uid, symbol, 100, 100, 5, .bid, .gtc, 0);
    const book = try api.requestOrderBookAsync(mode, symbol, 8);

    try std.testing.expectEqual(@as(usize, 7), api.pendingCount());
    try std.testing.expectEqual(@as(usize, 0), api.readyCount());

    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 7), processed);
    try std.testing.expectEqual(@as(usize, 0), api.pendingCount());
    try std.testing.expectEqual(@as(usize, 7), api.readyCount());

    const handles = [_]u64{
        add_maker,
        add_taker,
        dep_maker,
        dep_taker,
        maker_ask,
        taker_bid,
        book,
    };
    for (handles, 0..) |handle, idx| {
        var response = api.takeResult(handle) orelse return error.MissingAsyncResult;
        defer response.deinit(api.allocator);
        outcome.codes[idx] = response.result_code;
        if (idx == handles.len - 1) {
            const snapshot = response.l2_snapshot orelse return error.MissingOrderBookSnapshot;
            outcome.ask_levels = snapshot.asks.items.len;
            outcome.bid_levels = snapshot.bids.items.len;
        }
    }
    try std.testing.expectEqual(@as(usize, 0), api.readyCount());

    return outcome;
}

fn runSuspendResumeAsyncCodes(
    api: *ExchangeApiSharded,
    mode: ExecutionMode,
) ![5]cmd.CommandResultCode {
    const uid: i64 = 12_901;
    const add = try api.addUserAsync(mode, uid);
    const suspend_handle = try api.suspendUserAsync(mode, uid);
    const suspend_again_handle = try api.suspendUserAsync(mode, uid);
    const resume_handle = try api.resumeUserAsync(mode, uid);
    const resume_again_handle = try api.resumeUserAsync(mode, uid);

    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 5), processed);

    const handles = [_]u64{
        add,
        suspend_handle,
        suspend_again_handle,
        resume_handle,
        resume_again_handle,
    };
    var codes = [_]cmd.CommandResultCode{.new} ** handles.len;
    for (handles, 0..) |handle, idx| {
        var response = api.takeResult(handle) orelse return error.MissingAsyncResult;
        defer response.deinit(api.allocator);
        codes[idx] = response.result_code;
    }
    return codes;
}

fn runResetNopAsyncCodes(
    api: *ExchangeApiSharded,
    mode: ExecutionMode,
) ![4]cmd.CommandResultCode {
    const uid: i64 = 14_901;
    const first_add = try api.addUserAsync(mode, uid);
    const reset_handle = try api.resetAsync(mode);
    const second_add = try api.addUserAsync(mode, uid);
    const nop_handle = try api.nopAsync(mode);

    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 4), processed);

    const handles = [_]u64{
        first_add,
        reset_handle,
        second_add,
        nop_handle,
    };
    var codes = [_]cmd.CommandResultCode{.new} ** handles.len;
    for (handles, 0..) |handle, idx| {
        var response = api.takeResult(handle) orelse return error.MissingAsyncResult;
        defer response.deinit(api.allocator);
        codes[idx] = response.result_code;
    }
    return codes;
}

fn runGroupingControlAsyncCode(
    api: *ExchangeApiSharded,
    mode: ExecutionMode,
    timestamp_ns: i64,
    grouping_mode: i64,
) !cmd.CommandResultCode {
    const handle = try api.groupingControlAsync(mode, timestamp_ns, grouping_mode);
    try std.testing.expectEqual(@as(usize, 1), api.pendingCount());
    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 1), processed);
    var response = api.takeResult(handle) orelse return error.MissingAsyncResult;
    defer response.deinit(api.allocator);
    return response.result_code;
}

fn runPersistStateAsyncResultCode(
    api: *ExchangeApiSharded,
    mode: ExecutionMode,
    snapshot_id: i64,
    timestamp_ns: i64,
) !cmd.CommandResultCode {
    const requests = try api.persistStateAsync(mode, snapshot_id, timestamp_ns);
    try std.testing.expect(!api.isPersistStateResultReady(requests));
    try std.testing.expectEqual(@as(usize, 2), api.pendingCount());

    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 2), processed);
    try std.testing.expect(api.isPersistStateResultReady(requests));

    const merged = api.takePersistStateResult(requests) orelse return error.MissingAsyncResult;
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, null), api.takePersistStateResult(requests));
    return merged;
}

const SeededStartupPerturbation = enum {
    baseline_replay,
    missing_first_journal_partition,
    corrupted_replay_sequence,
    malformed_main_log_strict,
};

const SeededStartupMode = enum {
    configured,
    discovered_strict_main_log,
};

const SeededStartupErrorKind = enum {
    none,
    sequence_mismatch,
    malformed_main_log_entry,
    unexpected,
};

const SeededStartupScenario = struct {
    name: []const u8,
    seed: u64,
    perturbation: SeededStartupPerturbation,
    mode: SeededStartupMode,
};

const SeededStartupOutcome = struct {
    error_kind: SeededStartupErrorKind = .none,
    snapshot_requested: bool = false,
    snapshot_found: bool = false,
    journal_replayed_commands: usize = 0,
    final_risk_hash: u64 = 0,
    post_replay_add_result: cmd.CommandResultCode = .new,
};

fn formatSeededExchangeId(seed: u64, buffer: *[32]u8) ![]const u8 {
    return std.fmt.bufPrint(buffer, "ECSEED{x}", .{seed});
}

fn mapSeededStartupError(err: anyerror) SeededStartupErrorKind {
    return switch (err) {
        error.SequenceMismatch => .sequence_mismatch,
        error.MalformedMainLogEntry => .malformed_main_log_entry,
        else => .unexpected,
    };
}

fn runSeededStartupCall(
    target: *ExchangeApiSharded,
    dir: std.Io.Dir,
    scenario: SeededStartupScenario,
) !BootstrapOutcome {
    return switch (scenario.mode) {
        .configured => target.startupFromConfiguredPersistence(
            dir,
            .matching_engine_router,
            0,
            0,
        ),
        .discovered_strict_main_log => target.startupFromDiscoveredConfiguredPersistenceStrictMainLog(
            dir,
            .matching_engine_router,
            0,
            0,
        ),
    };
}

fn applySeededStartupPerturbation(
    allocator: std.mem.Allocator,
    source: *ExchangeApiSharded,
    dir: std.Io.Dir,
    snapshot_id: u64,
    snapshot_base_seq: u64,
    perturbation: SeededStartupPerturbation,
) !void {
    const partition_zero_path = try source.defaultPersistenceJournalPath(
        allocator,
        snapshot_id,
        0,
    );
    defer allocator.free(partition_zero_path);

    switch (perturbation) {
        .baseline_replay => {},
        .missing_first_journal_partition => {
            const partition_one_path = try source.defaultPersistenceJournalPath(
                allocator,
                snapshot_id,
                1,
            );
            defer allocator.free(partition_one_path);

            var full = try journal_mod.CommandJournal.readFromFile(
                allocator,
                dir,
                partition_zero_path,
            );
            defer full.deinit();

            var tail = journal_mod.CommandJournal.init(allocator);
            defer tail.deinit();

            for (full.entries.items) |entry| {
                if (entry.sequence > snapshot_base_seq) {
                    try tail.entries.append(allocator, entry);
                }
            }
            if (tail.entries.items.len == 0) return error.NoReplayableTailEntries;
            try tail.writeToFile(dir, partition_one_path);
            try dir.deleteFile(io_context.current(), partition_zero_path);
        },
        .corrupted_replay_sequence => {
            var journal = try journal_mod.CommandJournal.readFromFile(
                allocator,
                dir,
                partition_zero_path,
            );
            defer journal.deinit();

            var mutated = false;
            for (journal.entries.items) |*entry| {
                if (entry.sequence > snapshot_base_seq) {
                    entry.sequence += 1;
                    mutated = true;
                    break;
                }
            }
            if (!mutated) return error.NoReplayableTailEntries;
            try journal.writeToFile(dir, partition_zero_path);
        },
        .malformed_main_log_strict => {
            const main_log_path = try source.defaultPersistenceMainLogPath(allocator);
            defer allocator.free(main_log_path);

            var file = try dir.createFile(io_context.current(), main_log_path, .{ .truncate = false });
            defer file.close(io_context.current());
            try io_context.appendAll(file, "seeded-malformed-main-log-entry\n");
        },
    }
}

fn runSeededStartupReplayScenario(
    allocator: std.mem.Allocator,
    scenario: SeededStartupScenario,
) !SeededStartupOutcome {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var exchange_id_buffer: [32]u8 = undefined;
    const exchange_id = try formatSeededExchangeId(scenario.seed, &exchange_id_buffer);

    const source_cfg = config_mod.JavaExchangeConfig{
        .performance_cfg = config_mod.PerformanceConfig.throughputPreset(),
        .init_state_cfg = config_mod.InitialStateConfig.cleanStart(exchange_id),
        .serialization_cfg = config_mod.SerializationConfig.diskJournaling(),
    };
    var source = try ExchangeApiSharded.initFromJavaConfig(allocator, source_cfg);
    defer source.deinit();
    try source.startup();

    const symbol_suffix: i32 = @as(i32, @intCast(scenario.seed % 300)) * 4;
    const symbol: i32 = 24_000 + symbol_suffix;
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

    const seed_offset = std.math.cast(i64, scenario.seed & 0x0FFF_FFFF) orelse
        return error.SeedOverflow;
    const uid_base: i64 = 400_000 + (seed_offset * 4);
    const maker_uid: i64 = uid_base;
    const taker_uid: i64 = uid_base + 4;
    const post_snapshot_uid: i64 = uid_base + 8;

    var add_maker = try source.addUser(.routed, maker_uid);
    defer add_maker.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_maker.result_code);

    var add_taker = try source.addUser(.routed, taker_uid);
    defer add_taker.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_taker.result_code);

    var dep_maker = try source.adjustUserBalance(
        .routed,
        maker_uid,
        70_000 + seed_offset,
        11,
        50_000,
        .adjustment,
    );
    defer dep_maker.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, dep_maker.result_code);

    var dep_taker = try source.adjustUserBalance(
        .routed,
        taker_uid,
        71_000 + seed_offset,
        15,
        50_000,
        .adjustment,
    );
    defer dep_taker.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, dep_taker.result_code);

    var maker_ask = try source.placeOrder(
        .routed,
        80_000 + seed_offset,
        maker_uid,
        symbol,
        100,
        100,
        7,
        .ask,
        .gtc,
        0,
    );
    defer maker_ask.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, maker_ask.result_code);

    var taker_bid = try source.placeOrder(
        .routed,
        81_000 + seed_offset,
        taker_uid,
        symbol,
        100,
        100,
        7,
        .bid,
        .ioc,
        0,
    );
    defer taker_bid.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, taker_bid.result_code);

    const snapshot_id: u64 = 60_000 + (scenario.seed & 0xFFFF);
    try source.captureSnapshotToDefaultPath(
        tmp.dir,
        snapshot_id,
        .matching_engine_router,
        0,
        &.{symbol},
        8,
    );
    const snapshot_base_seq = blk: {
        const journal = source.journal orelse break :blk source.kernelRef().next_seq;
        if (journal.entries.items.len == 0) break :blk source.kernelRef().next_seq;
        break :blk journal.entries.items[journal.entries.items.len - 1].sequence;
    };

    var add_post_snapshot = try source.addUser(.routed, post_snapshot_uid);
    defer add_post_snapshot.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_post_snapshot.result_code);

    var dep_post_snapshot = try source.adjustUserBalance(
        .routed,
        post_snapshot_uid,
        72_000 + seed_offset,
        11,
        1_234,
        .adjustment,
    );
    defer dep_post_snapshot.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, dep_post_snapshot.result_code);

    try source.writeJournalToDefaultPath(tmp.dir, snapshot_id, 0);
    try applySeededStartupPerturbation(
        allocator,
        &source,
        tmp.dir,
        snapshot_id,
        snapshot_base_seq,
        scenario.perturbation,
    );

    const target_cfg = config_mod.JavaExchangeConfig{
        .performance_cfg = config_mod.PerformanceConfig.throughputPreset(),
        .init_state_cfg = config_mod.InitialStateConfig.lastKnownStateFromJournal(
            exchange_id,
            snapshot_id,
            snapshot_base_seq,
        ),
        .serialization_cfg = config_mod.SerializationConfig.diskJournaling(),
    };
    var target = try ExchangeApiSharded.initFromJavaConfig(allocator, target_cfg);
    defer target.deinit();

    const startup_outcome = runSeededStartupCall(&target, tmp.dir, scenario) catch |err| {
        const kind = mapSeededStartupError(err);
        if (kind == .unexpected) {
            std.debug.panic(
                "seeded startup scenario '{s}' seed=0x{x} failed with unexpected error {s}; replay with: zig build test --test-filter \"sharded exchange api seeded startup/replay perturbations are deterministic and reproducible\"",
                .{ scenario.name, scenario.seed, @errorName(err) },
            );
        }
        return .{ .error_kind = kind };
    };

    const risk_hash = try target.queryRiskStateHash();
    var post_replay_add = try target.addUser(.routed, post_snapshot_uid);
    defer post_replay_add.deinit(allocator);

    return .{
        .error_kind = .none,
        .snapshot_requested = startup_outcome.snapshot_requested,
        .snapshot_found = startup_outcome.snapshot_found,
        .journal_replayed_commands = startup_outcome.journal_replayed_commands,
        .final_risk_hash = risk_hash,
        .post_replay_add_result = post_replay_add.result_code,
    };
}

fn expectSeededStartupDeterministic(
    scenario: SeededStartupScenario,
    first: SeededStartupOutcome,
    second: SeededStartupOutcome,
) !void {
    if (!std.meta.eql(first, second)) {
        std.debug.panic(
            "seeded startup scenario '{s}' diverged for seed=0x{x}; replay with: zig build test --test-filter \"sharded exchange api seeded startup/replay perturbations are deterministic and reproducible\"",
            .{ scenario.name, scenario.seed },
        );
    }
}

test "sharded exchange api basic typed flow keeps routed and split parity" {
    const allocator = std.testing.allocator;

    var routed_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer routed_api.deinit();
    var split_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer split_api.deinit();

    const routed = try runBasicFlow(&routed_api, .routed);
    const split = try runBasicFlow(&split_api, .split);

    for (routed.codes, split.codes) |lhs, rhs| {
        try std.testing.expectEqual(lhs, rhs);
    }
    try std.testing.expectEqual(cmd.CommandResultCode.success, routed.codes[6]);
    try std.testing.expectEqual(cmd.CommandResultCode.success, split.codes[6]);
    try std.testing.expect(routed.ask_levels <= 8 and routed.bid_levels <= 8);
    try std.testing.expect(split.ask_levels <= 8 and split.bid_levels <= 8);
}

test "sharded exchange api initFromJavaConfig supports throughput shard count" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, .{
        .performance_cfg = config_mod.PerformanceConfig.throughputPreset(),
    });
    defer api.deinit();
    try std.testing.expectEqual(@as(usize, 4), api.shardCount());
}

test "sharded exchange api java-style convenience wrappers execute routed lifecycle" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const symbol: i32 = 24_301;
    const maker_uid: i64 = 64_001;
    const taker_uid: i64 = 64_002;
    const base_currency: i32 = 11;
    const quote_currency: i32 = 15;
    const maker_order_id: i64 = 99_001;

    const spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol,
        .symbol_type = .currency_exchange_pair,
        .base_currency = base_currency,
        .quote_currency = quote_currency,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
    });
    try api.addSymbolAll(spec);

    try api.createUser(.routed, 0, 101, 9_940_001_001, maker_uid);
    try api.createUser(.routed, 0, 102, 9_940_001_002, taker_uid);
    try api.balanceAdjustment(
        .routed,
        0,
        103,
        9_940_001_003,
        maker_uid,
        801,
        base_currency,
        10_000,
        .adjustment,
    );
    try api.balanceAdjustment(
        .routed,
        0,
        104,
        9_940_001_004,
        taker_uid,
        802,
        quote_currency,
        10_000,
        .adjustment,
    );

    try std.testing.expectEqual(@as(?i64, 10_000), try api.queryUserBalanceReport(maker_uid, base_currency));
    try std.testing.expectEqual(@as(?i64, 10_000), try api.queryUserBalanceReport(taker_uid, quote_currency));

    try api.placeNewOrder(
        .routed,
        1,
        105,
        9_940_001_005,
        maker_order_id,
        77,
        100,
        100,
        5,
        .ask,
        .gtc,
        symbol,
        maker_uid,
    );

    var sync_ob = try api.orderBookRequest(.routed, symbol, 8);
    defer sync_ob.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, sync_ob.result_code);
    const sync_snapshot = sync_ob.l2_snapshot orelse return error.MissingL2Snapshot;
    try std.testing.expectEqual(@as(usize, 1), sync_snapshot.asks.items.len);
    try std.testing.expectEqual(@as(i64, 100), sync_snapshot.asks.items[0].price);
    try std.testing.expectEqual(@as(i64, 5), sync_snapshot.asks.items[0].volume);

    const async_ob_handle = try api.orderBookRequestAsync(.routed, symbol, 8);
    try std.testing.expectEqual(@as(usize, 1), api.pendingCount());
    _ = try api.drainPending();
    var async_ob = api.takeResult(async_ob_handle) orelse return error.MissingAsyncResult;
    defer async_ob.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, async_ob.result_code);
    const async_snapshot = async_ob.l2_snapshot orelse return error.MissingL2Snapshot;
    try std.testing.expectEqual(@as(usize, 1), async_snapshot.asks.items.len);
    try std.testing.expectEqual(@as(i64, 100), async_snapshot.asks.items[0].price);

    try api.submitCommand(.routed, .{
        .cancel_order = .{
            .order_id = maker_order_id,
            .uid = maker_uid,
            .symbol = symbol,
        },
    });

    var post_cancel_ob = try api.orderBookRequest(.routed, symbol, 8);
    defer post_cancel_ob.deinit(allocator);
    const post_cancel_snapshot = post_cancel_ob.l2_snapshot orelse return error.MissingL2Snapshot;
    try std.testing.expectEqual(@as(usize, 0), post_cancel_snapshot.asks.items.len);
    try std.testing.expectEqual(@as(usize, 0), post_cancel_snapshot.bids.items.len);
}

test "sharded exchange api report query wrappers support routed queries with deterministic state hashes" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    _ = try runBasicFlow(&api, .split);

    var total_report = try api.queryTotalCurrencyBalanceReport();
    defer total_report.deinit(allocator);
    try std.testing.expect(total_report.account_balances.count() > 0);

    const maker_quote = try api.queryUserBalanceReport(11_001, 15);
    try std.testing.expect(maker_quote != null);

    var single_user_report = try api.querySingleUserReport(11_001);
    defer single_user_report.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 11_001), single_user_report.uid);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        single_user_report.query_execution_status,
    );

    const fee_collected = try api.queryFeeCollectedReport(15);
    try std.testing.expectEqual(@as(i64, 0), fee_collected);

    var order_book_report = try api.queryOrderBookReport(241, 8);
    defer order_book_report.deinit(allocator);
    switch (order_book_report) {
        .order_book => |snapshot| {
            try std.testing.expect(snapshot.asks.items.len <= 8);
            try std.testing.expect(snapshot.bids.items.len <= 8);
        },
        else => unreachable,
    }

    var detailed_report_a = try api.queryReport(.state_hash_detailed);
    defer detailed_report_a.deinit(allocator);
    var detailed_report_b = try api.queryReport(.state_hash_detailed);
    defer detailed_report_b.deinit(allocator);
    switch (detailed_report_a) {
        .state_hash_detailed => |left| switch (detailed_report_b) {
            .state_hash_detailed => |right| {
                try std.testing.expect(left.submodules.items.len > 0);
                try std.testing.expectEqual(left.aggregate_hash, right.aggregate_hash);
            },
            else => unreachable,
        },
        else => unreachable,
    }

    const detailed_typed = try api.queryStateHashDetailedReport();
    defer {
        var owned = detailed_typed;
        owned.deinit(allocator);
    }
    try std.testing.expect(detailed_typed.submodules.items.len > 0);

    var risk_hash_a = try api.queryReport(.risk_state_hash);
    defer risk_hash_a.deinit(allocator);
    var risk_hash_b = try api.queryReport(.risk_state_hash);
    defer risk_hash_b.deinit(allocator);
    switch (risk_hash_a) {
        .risk_state_hash => |left| switch (risk_hash_b) {
            .risk_state_hash => |right| {
                try std.testing.expectEqual(left, right);
                try std.testing.expectEqual(left, try api.queryRiskStateHash());
            },
            else => unreachable,
        },
        else => unreachable,
    }
}

test "sharded exchange api typed take/process helpers return expected report variants" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    _ = try runBasicFlow(&api, .routed);

    const known_uid: i64 = 11_001;
    const transfer_for_uid: i32 = @intCast(known_uid);

    const state_handle = try api.requestStateHashAsync(.routed, 77_001, 9_960_001_001);
    _ = try api.drainPending();
    var state_report = (try api.takeStateHashDetailed(state_handle)) orelse return error.MissingAsyncResult;
    defer state_report.deinit(allocator);
    try std.testing.expect(state_report.submodules.items.len > 0);

    const user_handle = try api.requestSingleUserReportAsync(
        .routed,
        transfer_for_uid,
        9_960_001_002,
        known_uid,
    );
    _ = try api.drainPending();
    var user_report = (try api.takeSingleUserReport(user_handle)) orelse return error.MissingAsyncResult;
    defer user_report.deinit(allocator);
    try std.testing.expectEqual(known_uid, user_report.uid);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        user_report.query_execution_status,
    );

    const total_handle = try api.requestTotalCurrencyBalanceAsync(.routed, 77_002, 9_960_001_003);
    _ = try api.drainPending();
    var total_report = (try api.takeTotalCurrencyBalanceReport(total_handle)) orelse return error.MissingAsyncResult;
    defer total_report.deinit(allocator);
    try std.testing.expect(total_report.account_balances.count() > 0);

    const order_book_handle = try api.orderBookRequestAsync(.routed, 241, 8);
    _ = try api.drainPending();
    var order_book_snapshot = (try api.takeOrderBookSnapshot(order_book_handle)) orelse return error.MissingAsyncResult;
    defer order_book_snapshot.deinit(allocator);
    try std.testing.expect(order_book_snapshot.asks.items.len <= 8);
    try std.testing.expect(order_book_snapshot.bids.items.len <= 8);

    var one_shot_state = try api.processStateHashReport(.routed, 77_003, 9_960_001_004);
    defer one_shot_state.deinit(allocator);
    try std.testing.expect(one_shot_state.submodules.items.len > 0);

    var one_shot_user = try api.processSingleUserReport(
        .routed,
        transfer_for_uid,
        9_960_001_005,
        known_uid,
    );
    defer one_shot_user.deinit(allocator);
    try std.testing.expectEqual(known_uid, one_shot_user.uid);

    var one_shot_total = try api.processTotalCurrencyBalanceReport(.routed, 77_004, 9_960_001_006);
    defer one_shot_total.deinit(allocator);
    _ = one_shot_total.account_balances.get(11) orelse 0;
    _ = one_shot_total.account_balances.get(15) orelse 0;

    var generic_report = try api.processReport(.routed, 77_005, 9_960_001_007, .state_hash);
    defer generic_report.deinit(allocator);
    switch (generic_report) {
        .state_hash_detailed => |detailed| {
            try std.testing.expect(detailed.submodules.items.len > 0);
        },
        else => return error.UnexpectedReportResponseType,
    }

    var one_shot_order_book = try api.processOrderBookRequest(.routed, 241, 8);
    defer one_shot_order_book.deinit(allocator);
    try std.testing.expect(one_shot_order_book.asks.items.len <= 8);
    try std.testing.expect(one_shot_order_book.bids.items.len <= 8);

    try std.testing.expectEqual(@as(i64, 0), try api.queryFeeCollected(15));

    var queried_order_book = try api.queryOrderBook(241, 8);
    defer queried_order_book.deinit(allocator);
    try std.testing.expect(queried_order_book.asks.items.len <= 8);
    try std.testing.expect(queried_order_book.bids.items.len <= 8);
}

test "sharded exchange api async queue preserves fifo execution and request-id routing" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const req_first = try api.addUserAsync(.routed, 61_001);
    const req_second = try api.addUserAsync(.routed, 61_005);
    try std.testing.expectEqual(@as(usize, 2), api.pendingCount());
    try std.testing.expect(!api.isResultReady(req_first));
    try std.testing.expect(!api.isResultReady(req_second));

    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 2), processed);
    try std.testing.expectEqual(@as(usize, 0), api.pendingCount());
    try std.testing.expect(api.isResultReady(req_first));
    try std.testing.expect(api.isResultReady(req_second));
    try std.testing.expectEqual(@as(usize, 2), api.readyCount());

    var second = api.takeResult(req_second) orelse return error.MissingAsyncResult;
    defer second.deinit(allocator);
    var first = api.takeResult(req_first) orelse return error.MissingAsyncResult;
    defer first.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 1), first.sequence);
    try std.testing.expectEqual(@as(u64, 2), second.sequence);
    try std.testing.expectEqual(cmd.CommandResultCode.success, first.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, second.result_code);

    try std.testing.expectEqual(@as(?kernel_mod.CommandResponse, null), api.takeResult(req_first));
    try std.testing.expectEqual(@as(usize, 0), api.readyCount());
}

test "sharded exchange api async alias wrappers forward to queue primitives" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const handle = try api.addUserAsync(.routed, 61_101);
    try std.testing.expectEqual(@as(usize, 1), api.asyncPendingCount());
    try std.testing.expect(!api.isAsyncResultReady(handle));

    const processed = try api.drainAsyncPending();
    try std.testing.expectEqual(@as(usize, 1), processed);
    try std.testing.expectEqual(@as(usize, 0), api.asyncPendingCount());
    try std.testing.expect(api.isAsyncResultReady(handle));
    try std.testing.expectEqual(@as(usize, 1), api.asyncReadyCount());

    var response = api.takeAsyncResult(handle) orelse return error.MissingAsyncResult;
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);

    try std.testing.expectEqual(@as(?kernel_mod.CommandResponse, null), api.takeAsyncResult(handle));
    try std.testing.expectEqual(@as(usize, 0), api.asyncReadyCount());
}

test "sharded exchange api callback wrappers invoke callbacks for user lifecycle commands" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    var collector: CallbackCollector = .{};
    const callback: CommandResponseCallback = .{
        .context = &collector,
        .callback = CallbackCollector.onResponse,
    };

    try api.createUserWithCallback(.routed, 71_001, callback);
    try api.suspendUserWithCallback(.routed, 71_001, callback);
    try api.resumeUserWithCallback(.routed, 71_001, callback);

    try std.testing.expectEqual(@as(usize, 3), api.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, 0), collector.count);
    try std.testing.expectEqual(@as(usize, 0), try api.processPendingCallbacks());

    const drained = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 3), drained);
    try std.testing.expectEqual(@as(usize, 3), try api.processPendingCallbacks());

    try std.testing.expectEqual(@as(usize, 0), api.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, 3), collector.count);
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), collector.last_result);
}

test "sharded exchange api callback wrappers support order lifecycle commands" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const symbol: i32 = 24_402;
    const uid: i64 = 71_101;
    const order_id: i64 = 88_001;
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
    try api.addSymbolAll(spec);
    var add = try api.addUser(.routed, uid);
    defer add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add.result_code);

    var collector: CallbackCollector = .{};
    const callback: CommandResponseCallback = .{
        .context = &collector,
        .callback = CallbackCollector.onResponse,
    };

    try api.balanceAdjustmentWithCallback(.routed, uid, 8_101, 11, 10_000, .adjustment, callback);
    try api.placeNewOrderWithCallback(.routed, order_id, 9, 100, 100, 5, .ask, .gtc, symbol, uid, callback);
    try api.moveOrderWithCallback(.routed, 101, order_id, symbol, uid, callback);
    try api.reduceOrderWithCallback(.routed, 1, order_id, symbol, uid, callback);
    try api.cancelOrderWithCallback(.routed, order_id, symbol, uid, callback);

    try std.testing.expectEqual(@as(usize, 5), api.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, 0), collector.count);
    try std.testing.expectEqual(@as(usize, 0), try api.processPendingCallbacks());

    const drained = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 5), drained);
    try std.testing.expectEqual(@as(usize, 5), try api.processPendingCallbacks());
    try std.testing.expectEqual(@as(usize, 0), api.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, 5), collector.count);
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), collector.last_result);
}

test "sharded exchange api registered callback can be processed per handle after drain" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const handle = try api.submitCreateUserAsync(.routed, 71_201);
    var collector: CallbackCollector = .{};
    try api.registerResultCallback(handle, .{
        .context = &collector,
        .callback = CallbackCollector.onResponse,
    });

    try std.testing.expectEqual(@as(usize, 1), api.pendingCallbackCount());
    try std.testing.expect(!(try api.processRegisteredCallback(handle)));
    try std.testing.expectEqual(@as(usize, 0), collector.count);

    const drained = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 1), drained);
    try std.testing.expect(try api.processRegisteredCallback(handle));
    try std.testing.expectEqual(@as(usize, 0), api.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, 1), collector.count);
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), collector.last_result);
    try std.testing.expect(!(try api.processRegisteredCallback(handle)));
}

test "sharded exchange api persist-state async handles reject callback registration and preserve polling path" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const persist_poll_handle = try api.submitCommandAsyncResultCode(.routed, .{
        .persist_state = .{
            .snapshot_id = 99_101,
            .timestamp_ns = 9_981_111_222,
        },
    });
    const persist_process_handle = try api.submitCommandAsyncResultCode(.routed, .{
        .persist_state = .{
            .snapshot_id = 99_102,
            .timestamp_ns = 9_981_111_333,
        },
    });

    var collector: CallbackCollector = .{};
    const callback: CommandResponseCallback = .{
        .context = &collector,
        .callback = CallbackCollector.onResponse,
    };

    try std.testing.expectError(
        error.UnsupportedPersistStateHandle,
        api.registerResultCallback(persist_poll_handle, callback),
    );
    try std.testing.expectEqual(@as(usize, 0), api.pendingCallbackCount());
    try std.testing.expectError(
        error.UnsupportedPersistStateHandle,
        api.processRegisteredCallback(persist_poll_handle),
    );
    try std.testing.expect(!(try api.processResult(persist_process_handle, callback)));
    try std.testing.expectEqual(@as(usize, 0), api.pendingCallbackCount());

    const drained = try api.drainPending();
    try std.testing.expect(drained >= 2);
    try std.testing.expectError(
        error.UnexpectedPersistStateResult,
        api.processResult(persist_process_handle, callback),
    );
    try std.testing.expectEqual(@as(usize, 0), try api.processPendingCallbacks());
    try std.testing.expectEqual(@as(usize, 0), collector.count);

    var polled = api.takeApiCommandAsyncResult(persist_poll_handle) orelse return error.MissingAsyncResult;
    defer polled.deinit(allocator);
    switch (polled) {
        .persist_state => |code| try std.testing.expectEqual(cmd.CommandResultCode.success, code),
        .response => return error.ExpectedPersistStateResult,
    }
}

test "sharded exchange api async submit aliases cover response and persist-state variants" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const symbol: i32 = 24_401;
    const uid: i64 = 62_001;
    const order_id: i64 = 78_001;
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
    try api.addSymbolAll(spec);

    const add_handle = try api.submitCreateUserAsync(.routed, uid);
    const adjust_handle = try api.submitBalanceAdjustmentAsync(
        .routed,
        uid,
        8_001,
        11,
        10_000,
        .adjustment,
    );
    const place_handle = try api.submitPlaceNewOrderAsync(
        .routed,
        order_id,
        5,
        100,
        100,
        5,
        .ask,
        .gtc,
        symbol,
        uid,
    );
    const move_handle = try api.submitMoveOrderAsync(.routed, 101, order_id, symbol, uid);
    const reduce_handle = try api.submitReduceOrderAsync(.routed, 1, order_id, symbol, uid);
    const cancel_handle = try api.submitCancelOrderAsync(.routed, order_id, symbol, uid);
    const suspend_handle = try api.submitSuspendUserAsync(.routed, uid);
    const resume_handle = try api.submitResumeUserAsync(.routed, uid);
    const persist_handle = try api.submitPersistStateAsync(.routed, 93_001, 9_970_001_001);

    _ = try api.drainPending();

    const response_handles = [_]ApiCommandAsyncHandle{
        add_handle,
        adjust_handle,
        place_handle,
        move_handle,
        reduce_handle,
        cancel_handle,
        suspend_handle,
        resume_handle,
    };
    const expected_codes = [_]cmd.CommandResultCode{
        .success,
        .success,
        .success,
        .success,
        .success,
        .success,
        .user_mgmt_user_not_suspendable_non_empty_accounts,
        .user_mgmt_user_not_suspended,
    };
    for (response_handles, expected_codes) |handle, expected_code| {
        var result = api.takeApiCommandAsyncResult(handle) orelse return error.MissingAsyncResult;
        defer result.deinit(allocator);
        switch (result) {
            .response => |*response| try std.testing.expectEqual(expected_code, response.result_code),
            .persist_state => return error.ExpectedResponseResult,
        }
    }

    var persist_result = api.takeApiCommandAsyncResult(persist_handle) orelse return error.MissingAsyncResult;
    defer persist_result.deinit(allocator);
    switch (persist_result) {
        .persist_state => |code| try std.testing.expectEqual(cmd.CommandResultCode.success, code),
        .response => return error.ExpectedPersistStateResult,
    }

    var full_response = try api.processCommandFullResponse(.routed, .{ .add_user = 62_002 });
    defer full_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, full_response.result_code);

    try std.testing.expectError(
        error.ExpectedResponseResult,
        api.processCommandFullResponse(.routed, .{
            .persist_state = .{
                .snapshot_id = 93_002,
                .timestamp_ns = 9_970_001_002,
            },
        }),
    );
}

test "sharded exchange api submitCommandBatch returns merged result and sequence range" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const empty = try api.submitCommandBatch(.routed, &.{});
    try std.testing.expectEqual(@as(?u64, null), empty.sequence_from);
    try std.testing.expectEqual(@as(?u64, null), empty.sequence_to);
    try std.testing.expectEqual(cmd.CommandResultCode.accepted, empty.result_code);

    var c1: cmd.OrderCommand = .{
        .command = .add_user,
        .order_id = -1,
        .symbol = -1,
        .uid = 72_001,
        .result_code = .new,
    };
    var c2: cmd.OrderCommand = .{
        .command = .add_user,
        .order_id = -1,
        .symbol = -1,
        .uid = 72_001,
        .result_code = .new,
    };
    var batch_commands = [_]*cmd.OrderCommand{ &c1, &c2 };
    const batch = try api.submitCommandBatch(.routed, batch_commands[0..]);
    try std.testing.expect(batch.sequence_from != null);
    try std.testing.expect(batch.sequence_to != null);
    try std.testing.expect(batch.sequence_from.? <= batch.sequence_to.?);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, batch.result_code);
}

test "sharded exchange api processPersistStateResultCode matches manual async persist path" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const manual_handle = try api.submitCommandAsyncResultCode(.routed, .{
        .persist_state = .{
            .snapshot_id = 100_201,
            .timestamp_ns = 9_101_111_222,
        },
    });
    _ = try api.drainPending();
    var manual_result = api.takeApiCommandAsyncResult(manual_handle) orelse return error.MissingAsyncResult;
    defer manual_result.deinit(allocator);
    const manual_code = switch (manual_result) {
        .persist_state => |code| code,
        .response => return error.ExpectedPersistStateResult,
    };

    const typed_code = try api.processPersistStateResultCode(.routed, 100_202, 9_101_111_333);
    try std.testing.expectEqual(manual_code, typed_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, typed_code);
}

test "sharded exchange api async typed flow keeps routed and split parity" {
    const allocator = std.testing.allocator;

    var routed_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer routed_api.deinit();
    var split_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer split_api.deinit();

    const routed = try runBasicFlowAsync(&routed_api, .routed);
    const split = try runBasicFlowAsync(&split_api, .split);

    for (routed.codes, split.codes) |lhs, rhs| {
        try std.testing.expectEqual(lhs, rhs);
    }
    try std.testing.expectEqual(cmd.CommandResultCode.success, routed.codes[6]);
    try std.testing.expectEqual(cmd.CommandResultCode.success, split.codes[6]);
    try std.testing.expect(routed.ask_levels <= 8 and routed.bid_levels <= 8);
    try std.testing.expect(split.ask_levels <= 8 and split.bid_levels <= 8);
}

test "sharded exchange api suspend/resume wrappers keep routed and split parity" {
    const allocator = std.testing.allocator;

    var routed_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer routed_api.deinit();
    var split_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer split_api.deinit();

    var routed_add = try routed_api.addUser(.routed, 12_501);
    defer routed_add.deinit(allocator);
    var split_add = try split_api.addUser(.split, 12_501);
    defer split_add.deinit(allocator);

    var routed_suspend = try routed_api.suspendUser(.routed, 12_501);
    defer routed_suspend.deinit(allocator);
    var split_suspend = try split_api.suspendUser(.split, 12_501);
    defer split_suspend.deinit(allocator);

    var routed_resume = try routed_api.resumeUser(.routed, 12_501);
    defer routed_resume.deinit(allocator);
    var split_resume = try split_api.resumeUser(.split, 12_501);
    defer split_resume.deinit(allocator);

    var routed_resume_again = try routed_api.resumeUser(.routed, 12_501);
    defer routed_resume_again.deinit(allocator);
    var split_resume_again = try split_api.resumeUser(.split, 12_501);
    defer split_resume_again.deinit(allocator);

    try std.testing.expectEqual(routed_add.result_code, split_add.result_code);
    try std.testing.expectEqual(routed_suspend.result_code, split_suspend.result_code);
    try std.testing.expectEqual(routed_resume.result_code, split_resume.result_code);
    try std.testing.expectEqual(routed_resume_again.result_code, split_resume_again.result_code);

    try std.testing.expectEqual(cmd.CommandResultCode.success, routed_suspend.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, routed_resume.result_code);
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_user_not_suspended,
        routed_resume_again.result_code,
    );
}

test "sharded exchange api async suspend/resume wrappers keep routed and split parity" {
    const allocator = std.testing.allocator;

    var routed_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer routed_api.deinit();
    var split_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer split_api.deinit();

    const routed_codes = try runSuspendResumeAsyncCodes(&routed_api, .routed);
    const split_codes = try runSuspendResumeAsyncCodes(&split_api, .split);

    for (routed_codes, split_codes) |lhs, rhs| {
        try std.testing.expectEqual(lhs, rhs);
    }
    try std.testing.expectEqual(cmd.CommandResultCode.success, routed_codes[0]);
    try std.testing.expectEqual(cmd.CommandResultCode.success, routed_codes[1]);
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_user_not_found,
        routed_codes[2],
    );
    try std.testing.expectEqual(cmd.CommandResultCode.success, routed_codes[3]);
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_user_not_suspended,
        routed_codes[4],
    );
}

test "sharded exchange api reset and nop wrappers expose routed-vs-split reset scope" {
    const allocator = std.testing.allocator;

    var routed_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer routed_api.deinit();
    var split_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer split_api.deinit();

    var routed_first_add = try routed_api.addUser(.routed, 14_501);
    defer routed_first_add.deinit(allocator);
    var split_first_add = try split_api.addUser(.split, 14_501);
    defer split_first_add.deinit(allocator);

    var routed_reset = try routed_api.reset(.routed);
    defer routed_reset.deinit(allocator);
    var split_reset = try split_api.reset(.split);
    defer split_reset.deinit(allocator);

    var routed_second_add = try routed_api.addUser(.routed, 14_501);
    defer routed_second_add.deinit(allocator);
    var split_second_add = try split_api.addUser(.split, 14_501);
    defer split_second_add.deinit(allocator);

    var routed_nop = try routed_api.nop(.routed);
    defer routed_nop.deinit(allocator);
    var split_nop = try split_api.nop(.split);
    defer split_nop.deinit(allocator);

    try std.testing.expectEqual(cmd.CommandResultCode.success, routed_first_add.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, split_first_add.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, routed_reset.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, split_reset.result_code);
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_user_already_exists,
        routed_second_add.result_code,
    );
    try std.testing.expectEqual(cmd.CommandResultCode.success, split_second_add.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, routed_nop.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, split_nop.result_code);
}

test "sharded exchange api async reset and nop wrappers expose routed-vs-split reset scope" {
    const allocator = std.testing.allocator;

    var routed_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer routed_api.deinit();
    var split_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer split_api.deinit();

    const routed_codes = try runResetNopAsyncCodes(&routed_api, .routed);
    const split_codes = try runResetNopAsyncCodes(&split_api, .split);

    try std.testing.expectEqual(cmd.CommandResultCode.success, routed_codes[0]);
    try std.testing.expectEqual(cmd.CommandResultCode.success, split_codes[0]);
    try std.testing.expectEqual(cmd.CommandResultCode.success, routed_codes[1]);
    try std.testing.expectEqual(cmd.CommandResultCode.success, split_codes[1]);
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_user_already_exists,
        routed_codes[2],
    );
    try std.testing.expectEqual(cmd.CommandResultCode.success, split_codes[2]);
    try std.testing.expectEqual(cmd.CommandResultCode.success, routed_codes[3]);
    try std.testing.expectEqual(cmd.CommandResultCode.success, split_codes[3]);
}

test "sharded exchange api grouping control wrappers keep routed and split success semantics" {
    const allocator = std.testing.allocator;

    var routed_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer routed_api.deinit();
    var split_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer split_api.deinit();

    var routed = try routed_api.groupingControl(.routed, 9_700_100_001, 1);
    defer routed.deinit(allocator);
    var split = try split_api.groupingControl(.split, 9_700_100_001, 1);
    defer split.deinit(allocator);

    try std.testing.expectEqual(cmd.CommandResultCode.success, routed.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, split.result_code);
}

test "sharded exchange api async grouping control wrappers keep routed and split success semantics" {
    const allocator = std.testing.allocator;

    var routed_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer routed_api.deinit();
    var split_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer split_api.deinit();

    const routed_code = try runGroupingControlAsyncCode(&routed_api, .routed, 9_700_100_002, 1);
    const split_code = try runGroupingControlAsyncCode(&split_api, .split, 9_700_100_002, 1);

    try std.testing.expectEqual(cmd.CommandResultCode.success, routed_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, split_code);
}

test "sharded exchange api persist-state wrappers keep routed and split success semantics" {
    const allocator = std.testing.allocator;

    var routed_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer routed_api.deinit();
    var split_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer split_api.deinit();

    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try routed_api.persistState(.routed, 16_001, 9_800_100_001),
    );
    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try split_api.persistState(.split, 16_002, 9_800_100_002),
    );
}

test "sharded exchange api async persist-state wrappers expose readiness and single-consumption semantics" {
    const allocator = std.testing.allocator;

    var routed_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer routed_api.deinit();
    var split_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer split_api.deinit();

    const routed = try runPersistStateAsyncResultCode(&routed_api, .routed, 16_011, 9_800_100_011);
    const split = try runPersistStateAsyncResultCode(&split_api, .split, 16_012, 9_800_100_012);

    try std.testing.expectEqual(cmd.CommandResultCode.success, routed);
    try std.testing.expectEqual(cmd.CommandResultCode.success, split);
}

test "sharded exchange api async result-code helpers consume responses deterministically" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const first = try api.addUserAsync(.routed, 15_001);
    const duplicate = try api.addUserAsync(.routed, 15_001);

    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 2), processed);
    try std.testing.expectEqual(@as(usize, 2), api.readyCount());

    try std.testing.expectEqual(
        @as(?cmd.CommandResultCode, .success),
        api.takeResultCode(first),
    );
    try std.testing.expectEqual(
        @as(?cmd.CommandResultCode, .user_mgmt_user_already_exists),
        api.takeResultCode(duplicate),
    );
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, null), api.takeResultCode(first));
    try std.testing.expectEqual(@as(usize, 0), api.readyCount());

    const add_user = makeAddUserCommand(15_101);
    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try api.processCommandResultCode(.routed, &add_user),
    );
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_user_already_exists,
        try api.processCommandResultCode(.routed, &add_user),
    );
}

test "sharded exchange api routed raw binary payload wrappers support command and query flows" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = 19_301,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
    });
    const command_payload = try binary_payload.encodeAddSymbolsToBytes(allocator, &.{spec});
    defer allocator.free(command_payload);

    var command_response = try api.submitBinaryDataPayload(
        .routed,
        5_901,
        9_900_100_001,
        command_payload,
    );
    defer command_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, command_response.result_code);

    var order_book = try api.queryOrderBookReport(19_301, 8);
    defer order_book.deinit(allocator);
    switch (order_book) {
        .order_book => |snapshot| {
            try std.testing.expectEqual(@as(usize, 0), snapshot.asks.items.len);
            try std.testing.expectEqual(@as(usize, 0), snapshot.bids.items.len);
        },
        else => return error.UnexpectedReportResponseType,
    }

    const query_payload = try binary_payload.encodeReportQueryToBytes(allocator, .state_hash);
    defer allocator.free(query_payload);

    var query_response = try api.submitBinaryQueryPayload(
        .routed,
        5_902,
        9_900_100_002,
        query_payload,
    );
    defer query_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, query_response.result_code);
    switch (query_response.report_response orelse return error.MissingReportResponse) {
        .state_hash_detailed => |detailed| {
            try std.testing.expect(detailed.submodules.items.len > 0);
        },
        else => return error.UnexpectedReportResponseType,
    }
}

test "sharded exchange api routed typed and async binary wrappers apply deterministic state updates" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = 19_302,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
    });
    var add_symbols = try api.submitAddSymbolsBinary(.routed, 19_302, 9_900_100_003, &.{spec});
    defer add_symbols.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_symbols.result_code);

    const accounts = [_]BinaryCurrencyBalance{
        .{ .currency = 11, .balance = 7_000 },
        .{ .currency = 15, .balance = 4_000 },
    };
    const users = [_]BinaryUserAccountsBatch{
        .{
            .uid = 96_901,
            .accounts = accounts[0..],
        },
    };

    const aligned_transfer_id: i32 = 96_901;
    try std.testing.expectEqual(
        api.core.shardIndexForUid(96_901),
        api.core.shardIndexForSymbol(aligned_transfer_id),
    );
    const add_accounts_handle = try api.submitAddAccountsBinaryAsync(
        .routed,
        aligned_transfer_id,
        9_900_100_004,
        users[0..],
    );
    const hash_handle = try api.requestStateHashAsync(.routed, 5_905, 9_900_100_005);
    const processed = try api.drainPending();
    try std.testing.expect(processed >= 2);

    try std.testing.expectEqual(
        @as(?cmd.CommandResultCode, .success),
        api.takeResultCode(add_accounts_handle),
    );

    var hash_response = api.takeResult(hash_handle) orelse return error.MissingAsyncResult;
    defer hash_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, hash_response.result_code);
    switch (hash_response.report_response orelse return error.MissingReportResponse) {
        .state_hash_detailed => |detailed| {
            try std.testing.expect(detailed.submodules.items.len > 0);
        },
        else => return error.UnexpectedReportResponseType,
    }

    try std.testing.expectEqual(@as(?i64, 7_000), try api.queryUserBalanceReport(96_901, 11));
    try std.testing.expectEqual(@as(?i64, 4_000), try api.queryUserBalanceReport(96_901, 15));
}

test "sharded exchange api binary async aliases forward to wrapped command and query behavior" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const spec_a = try common.CoreSymbolSpecification.init(.{
        .symbol_id = 19_303,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
    });
    const spec_b = try common.CoreSymbolSpecification.init(.{
        .symbol_id = 19_304,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
    });
    const payload_a = try binary_payload.encodeAddSymbolsToBytes(allocator, &.{spec_a});
    defer allocator.free(payload_a);
    const payload_b = try binary_payload.encodeAddSymbolsToBytes(allocator, &.{spec_b});
    defer allocator.free(payload_b);

    const add_command = try api.submitBinaryCommandAsync(.routed, 5_906, 9_900_100_006, payload_a);
    const add_publish = try api.publishBinaryData(.routed, 5_907, 9_900_100_007, payload_b);
    const query_submit = try api.submitQueryAsync(.routed, 5_908, 9_900_100_008, .state_hash);
    const query_publish = try api.publishQuery(.routed, 5_909, 9_900_100_009, .state_hash);
    const processed = try api.drainPending();
    try std.testing.expect(processed >= 4);

    try std.testing.expectEqual(
        @as(?cmd.CommandResultCode, .success),
        api.takeResultCode(add_command),
    );
    try std.testing.expectEqual(
        @as(?cmd.CommandResultCode, .success),
        api.takeResultCode(add_publish),
    );

    var report_a = api.takeResult(query_submit) orelse return error.MissingAsyncResult;
    defer report_a.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, report_a.result_code);
    switch (report_a.report_response orelse return error.MissingReportResponse) {
        .state_hash_detailed => |detailed| {
            try std.testing.expect(detailed.submodules.items.len > 0);
        },
        else => return error.UnexpectedReportResponseType,
    }

    var report_b = api.takeResult(query_publish) orelse return error.MissingAsyncResult;
    defer report_b.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, report_b.result_code);
    switch (report_b.report_response orelse return error.MissingReportResponse) {
        .state_hash_detailed => |detailed| {
            try std.testing.expect(detailed.submodules.items.len > 0);
        },
        else => return error.UnexpectedReportResponseType,
    }
}

test "sharded exchange api binary wrappers reject invalid command, empty payload, and split mode" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const one_byte = [_]u8{0x7F};
    const empty: [0]u8 = .{};

    try std.testing.expectError(
        error.InvalidBinaryCommandType,
        api.submitBinaryPayload(.routed, .add_user, 5_910, 9_900_100_010, one_byte[0..]),
    );
    try std.testing.expectError(
        error.InvalidBinaryCommandType,
        api.submitBinaryPayloadAsync(.routed, .add_user, 5_911, 9_900_100_011, one_byte[0..]),
    );
    try std.testing.expectError(
        error.EmptyBinaryPayload,
        api.submitBinaryDataPayload(.routed, 5_912, 9_900_100_012, empty[0..]),
    );
    try std.testing.expectError(
        error.EmptyBinaryPayload,
        api.submitBinaryDataPayloadAsync(.routed, 5_913, 9_900_100_013, empty[0..]),
    );
    try std.testing.expectError(
        error.SplitBinaryPayloadUnsupported,
        api.submitBinaryDataPayload(.split, 5_914, 9_900_100_014, one_byte[0..]),
    );
    try std.testing.expectError(
        error.SplitBinaryPayloadUnsupported,
        api.submitBinaryDataPayloadAsync(.split, 5_915, 9_900_100_015, one_byte[0..]),
    );
    try std.testing.expectError(
        error.SplitBinaryPayloadUnsupported,
        api.submitBinaryReportQuery(.split, 5_916, 9_900_100_016, .state_hash),
    );
    try std.testing.expectError(
        error.SplitBinaryPayloadUnsupported,
        api.submitBinaryReportQueryAsync(.split, 5_917, 9_900_100_017, .state_hash),
    );
}

test "sharded exchange api ApiCommand sync dispatch keeps routed and split balance parity" {
    const allocator = std.testing.allocator;

    var routed_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer routed_api.deinit();
    var split_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer split_api.deinit();

    const uid: i64 = 98_001;
    try routed_api.submitApiCommand(.routed, .{ .add_user = uid });
    try split_api.submitApiCommand(.split, .{ .add_user = uid });
    try routed_api.submitApiCommand(.routed, .{
        .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 71,
            .currency = 15,
            .amount = 55_000,
            .adjustment_type = .adjustment,
        },
    });
    try split_api.submitApiCommand(.split, .{
        .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 71,
            .currency = 15,
            .amount = 55_000,
            .adjustment_type = .adjustment,
        },
    });

    try std.testing.expectEqual(@as(?i64, 55_000), try routed_api.queryUserBalanceReport(uid, 15));
    try std.testing.expectEqual(@as(?i64, 55_000), try split_api.queryUserBalanceReport(uid, 15));
}

test "sharded exchange api ApiCommand async handles resolve response and persist-state variants" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const add_handle = try api.submitApiCommandAsync(.routed, .{ .add_user = 98_101 });
    const persist_handle = try api.submitApiCommandAsync(.routed, .{
        .persist_state = .{
            .snapshot_id = 17_101,
            .timestamp_ns = 9_910_100_001,
        },
    });
    try std.testing.expect(!api.isApiCommandAsyncResultReady(add_handle));
    try std.testing.expect(!api.isApiCommandAsyncResultReady(persist_handle));

    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 3), processed);
    try std.testing.expect(api.isApiCommandAsyncResultReady(add_handle));
    try std.testing.expect(api.isApiCommandAsyncResultReady(persist_handle));

    var add_result = api.takeApiCommandAsyncResult(add_handle) orelse return error.MissingAsyncResult;
    defer add_result.deinit(allocator);
    switch (add_result) {
        .response => |*response| try std.testing.expectEqual(
            cmd.CommandResultCode.success,
            response.result_code,
        ),
        .persist_state => unreachable,
    }

    var persist_result = api.takeApiCommandAsyncResult(persist_handle) orelse return error.MissingAsyncResult;
    defer persist_result.deinit(allocator);
    switch (persist_result) {
        .persist_state => |code| try std.testing.expectEqual(cmd.CommandResultCode.success, code),
        .response => unreachable,
    }

    try std.testing.expectEqual(@as(?ApiCommandResult, null), api.takeApiCommandAsyncResult(add_handle));
    try std.testing.expectEqual(@as(?ApiCommandResult, null), api.takeApiCommandAsyncResult(persist_handle));
}

test "sharded exchange api ApiCommand binary dispatch follows routed wrappers and rejects split mode" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const symbol: i32 = 19_401;
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

    try std.testing.expectError(
        error.SplitBinaryPayloadUnsupported,
        api.submitApiCommand(.split, .{
            .binary_add_symbols = .{
                .transfer_id = symbol,
                .timestamp = 9_920_100_001,
                .symbols = &.{spec},
            },
        }),
    );

    var add_result = try api.submitApiCommandResult(.routed, .{
        .binary_add_symbols = .{
            .transfer_id = symbol,
            .timestamp = 9_920_100_002,
            .symbols = &.{spec},
        },
    });
    defer add_result.deinit(allocator);
    switch (add_result) {
        .response => |*response| {
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
        },
        .persist_state => unreachable,
    }

    var order_book = try api.queryOrderBookReport(symbol, 8);
    defer order_book.deinit(allocator);
    switch (order_book) {
        .order_book => |snapshot| {
            try std.testing.expectEqual(@as(usize, 0), snapshot.asks.items.len);
            try std.testing.expectEqual(@as(usize, 0), snapshot.bids.items.len);
        },
        else => return error.UnexpectedReportResponseType,
    }

    const query_handle = try api.submitApiCommandAsync(.routed, .{
        .binary_report_query = .{
            .transfer_id = 6_103,
            .timestamp = 9_920_100_003,
            .query = .state_hash,
        },
    });
    _ = try api.drainPending();

    var query_result = api.takeApiCommandAsyncResult(query_handle) orelse return error.MissingAsyncResult;
    defer query_result.deinit(allocator);
    switch (query_result) {
        .response => |*response| {
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
            switch (response.report_response orelse return error.MissingReportResponse) {
                .state_hash_detailed => |detailed| {
                    try std.testing.expect(detailed.submodules.items.len > 0);
                },
                else => return error.UnexpectedReportResponseType,
            }
        },
        .persist_state => unreachable,
    }
}

test "sharded exchange api ApiCommand batch sync returns final result and applies prior commands" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const uid: i64 = 99_001;
    const commands = [_]ApiCommand{
        .{ .add_user = uid },
        .{
            .adjust_user_balance = .{
                .uid = uid,
                .transaction_id = 81,
                .currency = 15,
                .amount = 77_000,
                .adjustment_type = .adjustment,
            },
        },
    };

    var result = try api.submitApiCommandsSyncResult(.routed, commands[0..]);
    defer result.deinit(allocator);
    switch (result) {
        .response => |*response| try std.testing.expectEqual(
            cmd.CommandResultCode.success,
            response.result_code,
        ),
        .persist_state => unreachable,
    }

    try std.testing.expectEqual(@as(?i64, 77_000), try api.queryUserBalanceReport(uid, 15));

    var persist_result = try api.submitApiCommandsSyncResult(.routed, &.{
        .{
            .persist_state = .{
                .snapshot_id = 18_001,
                .timestamp_ns = 9_930_100_001,
            },
        },
    });
    defer persist_result.deinit(allocator);
    switch (persist_result) {
        .persist_state => |code| try std.testing.expectEqual(cmd.CommandResultCode.success, code),
        .response => unreachable,
    }
}

test "sharded exchange api ApiCommand batch wrappers expose nop flush and empty-batch guard" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    try std.testing.expectError(
        error.EmptyApiCommandBatch,
        api.submitApiCommandsSyncResult(.routed, &.{}),
    );

    try api.submitApiCommandsSync(.routed, &.{
        .{ .add_user = 99_101 },
    });

    var flush = try api.submitApiCommandsSyncWithNop(.routed, &.{
        .{ .add_user = 99_102 },
    });
    defer flush.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, flush.result_code);
    try std.testing.expectEqual(@as(usize, 0), api.pendingCount());
    try std.testing.expectEqual(@as(usize, 0), api.readyCount());
}

test "sharded exchange api typed binary wrappers reject transfer shard mismatches" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const symbol: i32 = 19_501;
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
    var symbol_transfer = symbol;
    while (api.core.shardIndexForSymbol(symbol_transfer) == api.core.shardIndexForSymbol(symbol)) : (symbol_transfer += 1) {}

    try std.testing.expectError(
        error.BinaryTransferShardMismatch,
        api.submitAddSymbolsBinary(.routed, symbol_transfer, 9_940_100_001, &.{spec}),
    );

    const uid: i64 = 99_501;
    var uid_transfer: i32 = 1;
    while (api.core.shardIndexForSymbol(uid_transfer) == api.core.shardIndexForUid(uid)) : (uid_transfer += 1) {}

    const accounts = [_]BinaryCurrencyBalance{
        .{ .currency = 11, .balance = 1_000 },
    };
    const users = [_]BinaryUserAccountsBatch{
        .{
            .uid = uid,
            .accounts = accounts[0..],
        },
    };
    try std.testing.expectError(
        error.BinaryTransferShardMismatch,
        api.submitAddAccountsBinaryAsync(.routed, uid_transfer, 9_940_100_002, users[0..]),
    );
    try std.testing.expectError(
        error.BinaryTransferShardMismatch,
        api.submitBinaryReportQuery(.routed, uid_transfer, 9_940_100_003, .{ .single_user = uid }),
    );
}

test "sharded exchange api typed binary wrappers accept shard-aligned transfers" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const symbol: i32 = 19_502;
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

    var add_symbols = try api.submitAddSymbolsBinary(.routed, symbol, 9_940_100_011, &.{spec});
    defer add_symbols.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_symbols.result_code);

    const uid: i64 = 99_502;
    var uid_transfer: i32 = 1;
    while (api.core.shardIndexForSymbol(uid_transfer) != api.core.shardIndexForUid(uid)) : (uid_transfer += 1) {}

    const accounts = [_]BinaryCurrencyBalance{
        .{ .currency = 11, .balance = 7_000 },
        .{ .currency = 15, .balance = 4_000 },
    };
    const users = [_]BinaryUserAccountsBatch{
        .{
            .uid = uid,
            .accounts = accounts[0..],
        },
    };
    const add_accounts = try api.submitAddAccountsBinaryAsync(
        .routed,
        uid_transfer,
        9_940_100_012,
        users[0..],
    );
    _ = try api.drainPending();
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), api.takeResultCode(add_accounts));

    const single_user_handle = try api.requestSingleUserReportAsync(
        .routed,
        uid_transfer,
        9_940_100_013,
        uid,
    );
    _ = try api.drainPending();
    var single_user_result = api.takeResult(single_user_handle) orelse return error.MissingAsyncResult;
    defer single_user_result.deinit(allocator);
    switch (single_user_result.report_response orelse return error.MissingReportResponse) {
        .single_user => |user_report| {
            try std.testing.expectEqual(uid, user_report.uid);
            try std.testing.expectEqual(kernel_mod.SingleUserQueryExecutionStatus.ok, user_report.query_execution_status);
        },
        else => return error.UnexpectedReportResponseType,
    }
}

test "sharded exchange api lifecycle wrappers keep deterministic startup and shutdown flags" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    try std.testing.expect(!api.isStarted());
    try std.testing.expect(!api.isStopped());

    try api.startup();
    try std.testing.expect(api.isStarted());
    try std.testing.expect(!api.isStopped());

    try api.startup();
    try std.testing.expect(api.isStarted());
    try std.testing.expect(!api.isStopped());

    api.shutdown();
    try std.testing.expect(api.isStopped());
    try std.testing.expectError(error.CoreStopped, api.startup());
}

test "sharded exchange api blocks sync and async submissions after shutdown" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    api.shutdown();

    var routed_add = makeAddUserCommand(100_001);
    try std.testing.expectError(
        error.CoreStopped,
        api.submitApiCommand(.routed, .{ .add_user = routed_add.uid }),
    );
    try std.testing.expectError(error.CoreStopped, api.submitCommandAsync(.routed, &routed_add));

    try std.testing.expectError(
        error.CoreStopped,
        api.submitApiCommand(.split, .{ .add_user = 100_002 }),
    );
    try std.testing.expectError(
        error.CoreStopped,
        api.submitApiCommandAsync(.split, .{ .add_user = 100_003 }),
    );

    try std.testing.expectError(
        error.CoreStopped,
        api.persistStateAsync(.routed, 22_101, 9_950_100_001),
    );
}

test "sharded exchange api runtime queue drains in order and publishes sink events" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    var collector = RuntimeSinkCollector.init(allocator);
    defer collector.deinit();

    api.setResultSink(.{
        .context = @ptrCast(&collector),
        .on_result = RuntimeSinkCollector.onResult,
    });

    try api.enqueueRuntime(makeAddUserCommand(120_001));
    try api.enqueueRuntime(makeAddUserCommand(120_001));
    try std.testing.expectEqual(@as(usize, 2), api.runtimePendingCount());

    const outcome = try api.drainRuntime();
    try std.testing.expectEqual(@as(usize, 2), outcome.groups_processed);
    try std.testing.expectEqual(@as(usize, 2), outcome.commands_processed);
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_user_already_exists,
        outcome.last_merged_result,
    );
    try std.testing.expectEqual(@as(usize, 0), api.runtimePendingCount());

    try std.testing.expectEqual(@as(usize, 2), collector.events.items.len);
    try std.testing.expectEqual(cmd.CommandResultCode.success, collector.events.items[0].result_code);
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_user_already_exists,
        collector.events.items[1].result_code,
    );
    try std.testing.expect(collector.events.items[0].sequence < collector.events.items[1].sequence);
}

test "sharded exchange api runtime queue rejects enqueue and drain after shutdown" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    api.shutdown();

    try std.testing.expectError(error.CoreStopped, api.enqueueRuntime(makeAddUserCommand(120_101)));
    try std.testing.expectError(error.CoreStopped, api.drainRuntime());
    try std.testing.expectEqual(@as(usize, 0), api.runtimePendingCount());
}

test "sharded exchange api persistence compatibility methods execute real snapshot-discovery-mainlog paths" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const symbols = [_]i32{};

    const bootstrap = try api.bootstrapFromPersistence(tmp.dir, null, null);
    try std.testing.expectEqual(@as(bool, false), bootstrap.snapshot_requested);
    try std.testing.expectEqual(@as(bool, false), bootstrap.snapshot_found);
    try std.testing.expectEqual(@as(usize, 0), bootstrap.journal_replayed_commands);

    const startup = try api.startupFromPersistence(tmp.dir, null, null);
    try std.testing.expectEqual(@as(bool, false), startup.snapshot_requested);
    try std.testing.expectEqual(@as(usize, 0), startup.journal_replayed_commands);

    const configured = try api.startupFromConfiguredPersistence(
        tmp.dir,
        .matching_engine_router,
        0,
        0,
    );
    try std.testing.expectEqual(@as(bool, false), configured.snapshot_requested);
    try std.testing.expectEqual(@as(usize, 0), configured.journal_replayed_commands);

    const discovered = try api.startupFromDiscoveredConfiguredPersistence(
        tmp.dir,
        .matching_engine_router,
        0,
        0,
    );
    try std.testing.expectEqual(@as(bool, false), discovered.snapshot_requested);
    try std.testing.expectEqual(@as(bool, false), discovered.snapshot_found);
    try std.testing.expectEqual(@as(usize, 0), discovered.journal_replayed_commands);
    const discovered_strict = try api.startupFromDiscoveredConfiguredPersistenceStrictMainLog(
        tmp.dir,
        .matching_engine_router,
        0,
        0,
    );
    try std.testing.expectEqual(@as(bool, false), discovered_strict.snapshot_requested);
    try std.testing.expectEqual(@as(bool, false), discovered_strict.snapshot_found);
    try std.testing.expectEqual(@as(usize, 0), discovered_strict.journal_replayed_commands);

    try std.testing.expectError(error.JournalingDisabled, api.writeJournalToFile(tmp.dir, "journal.ecj"));
    try std.testing.expectError(error.JournalingDisabled, api.loadJournalFromFile(tmp.dir, "journal.ecj"));
    try std.testing.expectError(error.JournalingDisabled, api.replayJournalFromFile(tmp.dir, "journal.ecj"));
    try api.captureSnapshotToFile(tmp.dir, "snapshot.ecs", &symbols, 8);
    try api.validateSnapshotFile(tmp.dir, "snapshot.ecs");

    try std.testing.expectError(error.JournalingDisabled, api.writeJournalToDefaultPath(tmp.dir, 91, 0));
    try std.testing.expectError(error.JournalingDisabled, api.loadJournalFromDefaultPath(tmp.dir, 91, 0));
    try std.testing.expectError(error.JournalingDisabled, api.replayJournalFromDefaultPath(tmp.dir, 91, 0));
    try std.testing.expectError(error.JournalingDisabled, api.replayJournalFromDefaultPathRange(tmp.dir, 91, 0));
    try api.captureSnapshotToDefaultPath(
        tmp.dir,
        91,
        .matching_engine_router,
        0,
        &symbols,
        8,
    );
    try api.validateSnapshotDefaultPath(tmp.dir, 91, .matching_engine_router, 0);
    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try api.persistStateWithDefaultMainLog(tmp.dir, 91, 9_960_100_001, .matching_engine_router, 0),
    );
    try std.testing.expectEqual(
        @as(bool, true),
        try api.snapshotExistsDefaultPath(tmp.dir, 91, .matching_engine_router, 0),
    );
    try std.testing.expectEqual(@as(bool, true), try api.snapshotExistsFile(tmp.dir, "snapshot.ecs"));
}

test "sharded exchange api discovered startup picks latest snapshot and main-log base sequence" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const source_cfg = config_mod.JavaExchangeConfig{
        .init_state_cfg = config_mod.InitialStateConfig.cleanStart("ECDISC"),
        .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
    };
    var source = try ExchangeApiSharded.initFromJavaConfig(allocator, source_cfg);
    defer source.deinit();

    var add = try source.addUser(.routed, 301_001);
    defer add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add.result_code);

    try source.captureSnapshotToDefaultPath(
        tmp.dir,
        300,
        .matching_engine_router,
        0,
        &.{},
        8,
    );
    try source.captureSnapshotToDefaultPath(
        tmp.dir,
        301,
        .matching_engine_router,
        0,
        &.{},
        8,
    );

    const target_cfg = config_mod.JavaExchangeConfig{
        .init_state_cfg = config_mod.InitialStateConfig.fromSnapshotOnly("ECDISC", 1, 777),
        .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
    };
    var target = try ExchangeApiSharded.initFromJavaConfig(allocator, target_cfg);
    defer target.deinit();

    const outcome = try target.startupFromDiscoveredConfiguredPersistence(
        tmp.dir,
        .matching_engine_router,
        0,
        0,
    );
    try std.testing.expectEqual(@as(bool, true), outcome.snapshot_requested);
    try std.testing.expectEqual(@as(bool, true), outcome.snapshot_found);
    try std.testing.expectEqual(@as(usize, 0), outcome.journal_replayed_commands);
    try std.testing.expectEqual(@as(u64, 301), target.persistenceCurrentSnapshot().snapshot_id);

    var duplicate = try target.addUser(.routed, 301_001);
    defer duplicate.deinit(allocator);
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_user_already_exists,
        duplicate.result_code,
    );
    try std.testing.expectEqual(@as(u64, 2), duplicate.sequence);
}

test "sharded exchange api strict discovered startup rejects malformed main log entries" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const cfg = config_mod.JavaExchangeConfig{
        .init_state_cfg = config_mod.InitialStateConfig.cleanStart("ECSTRICT"),
        .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
    };
    var source = try ExchangeApiSharded.initFromJavaConfig(allocator, cfg);
    defer source.deinit();

    try source.captureSnapshotToDefaultPath(
        tmp.dir,
        401,
        .matching_engine_router,
        0,
        &.{},
        8,
    );

    const main_log_path = try source.defaultPersistenceMainLogPath(allocator);
    defer allocator.free(main_log_path);
    var main_log_file = try tmp.dir.createFile(io_context.current(), main_log_path, .{ .truncate = false });
    defer main_log_file.close(io_context.current());
    try io_context.appendAll(main_log_file, "malformed-entry\n");

    const target_cfg = config_mod.JavaExchangeConfig{
        .init_state_cfg = config_mod.InitialStateConfig.fromSnapshotOnly("ECSTRICT", 1, 0),
        .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
    };
    var strict_target = try ExchangeApiSharded.initFromJavaConfig(allocator, target_cfg);
    defer strict_target.deinit();

    try std.testing.expectError(
        error.MalformedMainLogEntry,
        strict_target.startupFromDiscoveredConfiguredPersistenceStrictMainLog(
            tmp.dir,
            .matching_engine_router,
            0,
            0,
        ),
    );
}

test "sharded exchange api seeded startup/replay perturbations are deterministic and reproducible" {
    const allocator = std.testing.allocator;
    const scenarios = [_]SeededStartupScenario{
        .{
            .name = "baseline-replay",
            .seed = 0xA11CE55D0001,
            .perturbation = .baseline_replay,
            .mode = .configured,
        },
        .{
            .name = "missing-first-journal-partition",
            .seed = 0xA11CE55D0002,
            .perturbation = .missing_first_journal_partition,
            .mode = .configured,
        },
        .{
            .name = "corrupted-replay-sequence",
            .seed = 0xA11CE55D0003,
            .perturbation = .corrupted_replay_sequence,
            .mode = .configured,
        },
        .{
            .name = "strict-main-log-malformed",
            .seed = 0xA11CE55D0004,
            .perturbation = .malformed_main_log_strict,
            .mode = .discovered_strict_main_log,
        },
    };

    for (scenarios) |scenario| {
        const first = try runSeededStartupReplayScenario(allocator, scenario);
        const second = try runSeededStartupReplayScenario(allocator, scenario);
        try expectSeededStartupDeterministic(scenario, first, second);

        switch (scenario.perturbation) {
            .baseline_replay => {
                try std.testing.expectEqual(SeededStartupErrorKind.none, first.error_kind);
                try std.testing.expect(first.snapshot_requested);
                try std.testing.expect(first.snapshot_found);
                try std.testing.expectEqual(@as(usize, 2), first.journal_replayed_commands);
                try std.testing.expect(first.final_risk_hash != 0);
                try std.testing.expectEqual(
                    cmd.CommandResultCode.user_mgmt_user_already_exists,
                    first.post_replay_add_result,
                );
            },
            .missing_first_journal_partition => {
                try std.testing.expectEqual(SeededStartupErrorKind.none, first.error_kind);
                try std.testing.expect(first.snapshot_requested);
                try std.testing.expect(first.snapshot_found);
                try std.testing.expectEqual(@as(usize, 0), first.journal_replayed_commands);
                try std.testing.expect(first.final_risk_hash != 0);
                try std.testing.expectEqual(
                    cmd.CommandResultCode.success,
                    first.post_replay_add_result,
                );
            },
            .corrupted_replay_sequence => {
                try std.testing.expectEqual(
                    SeededStartupErrorKind.sequence_mismatch,
                    first.error_kind,
                );
            },
            .malformed_main_log_strict => {
                try std.testing.expectEqual(
                    SeededStartupErrorKind.malformed_main_log_entry,
                    first.error_kind,
                );
            },
        }
    }
}

test "sharded exchange api persistStateWithDefaultMainLog updates descriptor and appends log entry" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_id: i64 = 7001;
    const timestamp_ns: i64 = 9_970_100_001;
    const result = try api.persistStateWithDefaultMainLog(
        tmp.dir,
        snapshot_id,
        timestamp_ns,
        .matching_engine_router,
        0,
    );
    try std.testing.expectEqual(cmd.CommandResultCode.success, result);

    const snapshot = api.persistenceCurrentSnapshot();
    try std.testing.expectEqual(@as(u64, @intCast(snapshot_id)), snapshot.snapshot_id);
    try std.testing.expectEqual(@as(u64, @intCast(timestamp_ns)), snapshot.timestamp_ns);

    const main_log_path = try api.defaultPersistenceMainLogPath(allocator);
    defer allocator.free(main_log_path);
    var main_log_file = try tmp.dir.openFile(io_context.current(), main_log_path, .{});
    defer main_log_file.close(io_context.current());
    const contents = try io_context.readToEndAlloc(main_log_file, allocator, 4096);
    defer allocator.free(contents);
    try std.testing.expect(std.mem.indexOf(u8, contents, "snapshotId=7001") != null);
}

test "sharded exchange api startup-persistence methods return CoreStopped after shutdown" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    api.shutdown();

    try std.testing.expectError(error.CoreStopped, api.startupFromPersistence(tmp.dir, null, null));
    try std.testing.expectError(
        error.CoreStopped,
        api.startupFromConfiguredPersistence(tmp.dir, .matching_engine_router, 0, 0),
    );
    try std.testing.expectError(
        error.CoreStopped,
        api.startupFromDiscoveredConfiguredPersistence(tmp.dir, .matching_engine_router, 0, 0),
    );
    try std.testing.expectError(
        error.CoreStopped,
        api.startupFromDiscoveredConfiguredPersistenceStrictMainLog(tmp.dir, .matching_engine_router, 0, 0),
    );
    try std.testing.expectError(
        error.CoreStopped,
        api.persistStateWithDefaultMainLog(tmp.dir, 91, 9_960_200_001, .matching_engine_router, 0),
    );
}

test "sharded exchange api journaling replay and default-path wrappers work when journaling enabled" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journaling_cfg = config_mod.JavaExchangeConfig{
        .init_state_cfg = .{
            .exchange_id = "ECJ",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = std.math.maxInt(u64),
            .throw_if_snapshot_not_found = false,
        },
        .serialization_cfg = config_mod.SerializationConfig.diskJournaling(),
    };

    var source = try ExchangeApiSharded.initFromJavaConfig(
        allocator,
        journaling_cfg,
    );
    defer source.deinit();

    try std.testing.expect(source.journalingEnabled());
    try std.testing.expectEqual(@as(usize, 0), source.journalLen());

    var add = try source.addUser(.routed, 201_001);
    defer add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add.result_code);
    try std.testing.expectEqual(@as(usize, 1), source.journalLen());

    try source.writeJournalToFile(tmp.dir, "journal.ecj");
    try source.writeJournalToDefaultPath(tmp.dir, 701, 0);

    var loaded = try ExchangeApiSharded.initFromJavaConfig(
        allocator,
        journaling_cfg,
    );
    defer loaded.deinit();
    try loaded.loadJournalFromFile(tmp.dir, "journal.ecj");
    try std.testing.expectEqual(@as(usize, 1), loaded.journalLen());
    try loaded.loadJournalFromDefaultPath(tmp.dir, 701, 0);
    try std.testing.expectEqual(@as(usize, 1), loaded.journalLen());

    var replayed = try ExchangeApiSharded.initFromJavaConfig(
        allocator,
        journaling_cfg,
    );
    defer replayed.deinit();
    try std.testing.expectEqual(@as(usize, 1), try replayed.replayJournalFromFile(tmp.dir, "journal.ecj"));
    try std.testing.expectEqual(@as(usize, 1), replayed.journalLen());

    var duplicate = try replayed.addUser(.routed, 201_001);
    defer duplicate.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, duplicate.result_code);

    var replayed_default = try ExchangeApiSharded.initFromJavaConfig(
        allocator,
        journaling_cfg,
    );
    defer replayed_default.deinit();
    try std.testing.expectEqual(@as(usize, 1), try replayed_default.replayJournalFromDefaultPath(tmp.dir, 701, 0));
}

test "sharded exchange api configRef and default persistence helpers are deterministic" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const cfg = api.configRef();
    try std.testing.expectEqual(@as(u64, 4), cfg.matching.num_shards);
    try std.testing.expectEqual(@as(kernel_mod.OrderBookKind, .direct), cfg.matching.orderbook_kind);
    try std.testing.expect(!api.journalingEnabled());
    try std.testing.expectEqual(@as(usize, 0), api.journalLen());
    try std.testing.expect(api.kernelRef() == &api.core.kernels.kernels[0]);

    try std.testing.expectEqual(@as(usize, 1), api.persistenceSnapshotCount());
    try std.testing.expectEqual(@as(usize, 0), api.persistenceJournalCount());
    const root_snapshot_ref = api.persistenceCurrentSnapshotRef();
    const root_snapshot = api.persistenceCurrentSnapshot();
    try std.testing.expectEqual(@as(u64, 0), root_snapshot.snapshot_id);
    try std.testing.expectEqual(@as(u64, 0), root_snapshot.seq);
    try std.testing.expectEqual(root_snapshot_ref, api.persistenceFindSnapshotById(0).?);
    try std.testing.expect(api.persistenceFindSnapshotById(9999) == null);
    try std.testing.expect(api.persistenceSnapshotById(0) != null);
    try std.testing.expect(api.persistenceFindJournalForSequence(1) == null);

    const journal_ref = try api.descriptors.createJournal(
        api.current_snapshot_ref,
        101_001,
        500,
    );
    try api.descriptors.finishJournal(journal_ref, 550);
    try std.testing.expectEqual(@as(usize, 1), api.persistenceJournalCount());
    try std.testing.expectEqual(journal_ref, api.persistenceFindJournalForSequence(500).?);
    const journal = api.persistenceJournalByRef(journal_ref);
    try std.testing.expectEqual(@as(u64, 500), journal.seq_first);
    try std.testing.expectEqual(@as(?u64, 550), journal.seq_last);

    const snapshot_id: u64 = 101;
    const instance_id: u32 = 3;
    const partition_id: u16 = 5;
    const module_type: PersistenceModuleType = .matching_engine_router;

    const expected_snapshot_file = try persistence_paths.snapshotFileName(
        allocator,
        cfg.initial_state.exchange_id,
        snapshot_id,
        module_type,
        instance_id,
    );
    defer allocator.free(expected_snapshot_file);
    const actual_snapshot_file = try api.defaultPersistenceSnapshotFileName(
        allocator,
        snapshot_id,
        module_type,
        instance_id,
    );
    defer allocator.free(actual_snapshot_file);
    try std.testing.expectEqualStrings(expected_snapshot_file, actual_snapshot_file);

    const expected_journal_file = try persistence_paths.journalFileName(
        allocator,
        cfg.initial_state.exchange_id,
        snapshot_id,
        partition_id,
    );
    defer allocator.free(expected_journal_file);
    const actual_journal_file = try api.defaultPersistenceJournalFileName(
        allocator,
        snapshot_id,
        partition_id,
    );
    defer allocator.free(actual_journal_file);
    try std.testing.expectEqualStrings(expected_journal_file, actual_journal_file);

    const expected_main_log_file = try persistence_paths.mainLogFileName(
        allocator,
        cfg.initial_state.exchange_id,
    );
    defer allocator.free(expected_main_log_file);
    const actual_main_log_file = try api.defaultPersistenceMainLogFileName(allocator);
    defer allocator.free(actual_main_log_file);
    try std.testing.expectEqualStrings(expected_main_log_file, actual_main_log_file);

    const expected_snapshot_path = try persistence_paths.snapshotPath(
        allocator,
        cfg.serialization.storage_folder,
        cfg.initial_state.exchange_id,
        snapshot_id,
        module_type,
        instance_id,
    );
    defer allocator.free(expected_snapshot_path);
    const actual_snapshot_path = try api.defaultPersistenceSnapshotPath(
        allocator,
        snapshot_id,
        module_type,
        instance_id,
    );
    defer allocator.free(actual_snapshot_path);
    try std.testing.expectEqualStrings(expected_snapshot_path, actual_snapshot_path);

    const expected_journal_path = try persistence_paths.journalPath(
        allocator,
        cfg.serialization.storage_folder,
        cfg.initial_state.exchange_id,
        snapshot_id,
        partition_id,
    );
    defer allocator.free(expected_journal_path);
    const actual_journal_path = try api.defaultPersistenceJournalPath(
        allocator,
        snapshot_id,
        partition_id,
    );
    defer allocator.free(actual_journal_path);
    try std.testing.expectEqualStrings(expected_journal_path, actual_journal_path);

    const expected_main_log_path = try persistence_paths.mainLogPath(
        allocator,
        cfg.serialization.storage_folder,
        cfg.initial_state.exchange_id,
    );
    defer allocator.free(expected_main_log_path);
    const actual_main_log_path = try api.defaultPersistenceMainLogPath(allocator);
    defer allocator.free(actual_main_log_path);
    try std.testing.expectEqualStrings(expected_main_log_path, actual_main_log_path);
}

test "sharded exchange api simple events handler tracks sync async and runtime command results" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const symbol: i32 = 241;
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
    try api.addSymbolAll(spec);

    var collector: SimpleEventsCollector = .{};
    api.setSimpleEventsHandler(.{
        .context = &collector,
        .on_command_result = SimpleEventsCollector.onCommandResult,
    });

    var sync_add = try api.addUser(.routed, 131_001);
    defer sync_add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, sync_add.result_code);

    const async_req = try api.addUserAsync(.routed, 131_002);
    _ = try api.drainPending();
    var async_response = api.takeResult(async_req) orelse return error.MissingAsyncResult;
    defer async_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, async_response.result_code);

    try api.enqueueRuntime(makeAddUserCommand(131_003));
    const runtime_outcome = try api.drainRuntime();
    try std.testing.expectEqual(@as(usize, 1), runtime_outcome.commands_processed);

    var order_book = try api.requestOrderBook(.routed, symbol, 8);
    defer order_book.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, order_book.result_code);

    try std.testing.expectEqual(@as(usize, 3), collector.add_user_count);
    try std.testing.expectEqual(@as(usize, 1), collector.order_book_count);
    try std.testing.expectEqual(@as(usize, 0), collector.other_count);

    api.clearSimpleEventsHandler();
    var post_clear_add = try api.addUser(.routed, 131_004);
    defer post_clear_add.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 3), collector.add_user_count);
    try std.testing.expectEqual(@as(usize, 1), collector.order_book_count);
    try std.testing.expectEqual(@as(usize, 0), collector.other_count);
}
