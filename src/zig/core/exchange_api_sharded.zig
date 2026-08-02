const std = @import("std");
const builtin = @import("builtin");
const io_context = @import("io_context.zig");
const cmd = @import("cmd.zig");
const common = @import("common.zig");
const config_mod = @import("config.zig");
const api_command_mod = @import("api_command.zig");
const binary_frames = @import("binary_frames.zig");
const binary_payload = @import("binary_payload.zig");
const custom_report_pipeline = @import("custom_report_pipeline.zig");
const java_binary_wire = @import("compat/java/binary_wire.zig");
const java_report_wire = @import("compat/java/report_wire.zig");
const exchange_core_mod = @import("exchange_core.zig");
const int_map_order = @import("compat/java/eclipse_collections/int_map_order.zig");
const grouping_processor = @import("grouping_processor.zig");
const journal_mod = @import("journal.zig");
const kernel_mod = @import("exchange_kernel.zig");
const locked_allocator_mod = @import("locked_allocator.zig");
const orderbook_naive = @import("orderbook_naive.zig");
const persistence_mod = @import("persistence_descriptors.zig");
const persistence_paths = @import("persistence_paths.zig");
const publication = @import("publication_sequence.zig");
const report_registry = @import("report_registry.zig");
const runtime_queue_mod = @import("runtime_queue.zig");
const runtime_mod = @import("exchange_runtime.zig");
const settlement_lifecycle = @import("settlement_lifecycle.zig");
const serialization_processor = @import("serialization_processor.zig");
const sharded_core_mod = @import("exchange_core_sharded.zig");
const sharded_pipeline = @import("sharded_pipeline.zig");
const sharded_snapshot_store = @import("sharded_snapshot_store.zig");
const simple_events_mod = @import("simple_events_processor.zig");
const snapshot_mod = @import("snapshot.zig");
const risk = @import("risk.zig");
const log = std.log.scoped(.exchange_api_sharded);

threadlocal var active_callback_api: ?*ExchangeApiSharded = null;
threadlocal var active_worker_api: ?*ExchangeApiSharded = null;

pub const ExecutionMode = journal_mod.ExecutionMode;

const DescriptorInitError = persistence_mod.CreateSnapshotError || error{
    MatchingEngineCountOverflow,
    RiskEngineCountOverflow,
};
const DescriptorTrackingError = persistence_mod.CreateSnapshotError || error{
    InvalidPersistSnapshotId,
    InvalidPersistTimestamp,
};
pub const LifecycleError = error{
    CoreStopped,
    CorePoisoned,
    CoreQueueFull,
    CoreNotStarted,
    CoreAlreadyStarted,
    CoreReentrantCall,
};
pub const CallbackConfigError = error{
    CallbackReconfigurationWouldBlock,
    CallbackRevisionOverflow,
};
pub const SettlementHooksConfigError = error{
    SettlementHooksRequireNativeDurableJournal,
    SettlementHooksRequireColdCore,
    SettlementHooksReconfigurationWouldBlock,
};
pub const StartError = LifecycleError ||
    std.Thread.SpawnError ||
    serialization_processor.StartupError ||
    sharded_pipeline.PersistentGroupedRuntime.CreateError ||
    sharded_pipeline.PersistentGroupedRuntime.StartError ||
    error{
        ConfiguredPersistenceBootstrapRequired,
        DurableJournalFrontierMismatch,
        InjectedDriverSpawnFailure,
    };
const DriverInitError = std.mem.Allocator.Error || error{
    InvalidCapacity,
    CompletionCapacityOverflow,
};
pub const InitError = sharded_core_mod.InitError || Config.ValidationError || DescriptorInitError || DriverInitError || grouping_processor.Config.ValidationError || report_registry.RegistryInitError || serialization_processor.CreateError || std.Io.Dir.OpenError || std.Io.Dir.CreateDirPathError || journal_mod.JournalIoError || PersistencePathError;
pub const InitFromJavaConfigError = InitError || sharded_core_mod.InitFromJavaConfigError || config_mod.PerformanceConfig.MappingError;
pub const ExecuteError = LifecycleError || sharded_core_mod.ExecuteError || std.mem.Allocator.Error || DescriptorTrackingError || publication.PrepareError || journal_mod.JournalIoError || serialization_processor.JournalError || error{
    RequestIdOverflow,
};
pub const ReportError = LifecycleError || sharded_core_mod.ReportError;
pub const AggregateError = LifecycleError || sharded_core_mod.AggregateError;
pub const QueryReportError = LifecycleError || sharded_core_mod.QueryReportError;
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
pub const RawResultSink = runtime_mod.RawResultSink;
pub const RuntimeDrainOutcome = runtime_mod.DrainOutcome;
pub const ShutdownState = runtime_mod.ShutdownState;
pub const ShutdownOutcome = runtime_mod.ShutdownOutcome;
pub const ShutdownError = std.Thread.SpawnError || std.Io.Event.WaitTimeoutError;
pub const UnsupportedShardedPersistenceError = error{
    UnsupportedShardedPersistence,
    LegacyXcm1RequiresImmediateOrMigration,
};
const ShardedSnapshotBootstrapError = error{
    OutOfMemory,
    SnapshotFileNotFound,
    ShardedSnapshotInvalid,
    ShardedSnapshotTopologyMismatch,
    ShardedSnapshotConfigMismatch,
    ShardedSnapshotExchangeMismatch,
    ShardedSnapshotSequenceMismatch,
};
pub const BootstrapError = JournalReplayError || snapshot_mod.SnapshotIoError || snapshot_mod.SnapshotRestoreError || error{
    SnapshotPathRequired,
    SnapshotFileNotFound,
    JournalPathRequired,
    DurableJournalFrontierMismatch,
} || UnsupportedShardedPersistenceError || ShardedSnapshotBootstrapError;
pub const StartupError = StartError || BootstrapError;
pub const StartupFromConfiguredPersistenceError = StartupError || PersistencePathError || error{
    JournalReplayCountOverflow,
};
const MainLogSequenceParseError = error{
    MalformedMainLogEntry,
};
pub const StartupFromDiscoveredConfiguredPersistenceError = StartupFromConfiguredPersistenceError || std.Io.Dir.OpenError || std.Io.Dir.Iterator.Error;
pub const StartupFromDiscoveredConfiguredPersistenceStrictError = StartupFromDiscoveredConfiguredPersistenceError || MainLogSequenceParseError;

fn selectDiscoveredXcm2(
    inventory: sharded_snapshot_store.CommittedSnapshotInventory,
) UnsupportedShardedPersistenceError!?u64 {
    if (inventory.latest_xcm1) |latest_xcm1| {
        if (inventory.latest_xcm2 == null or latest_xcm1 > inventory.latest_xcm2.?) {
            return error.LegacyXcm1RequiresImmediateOrMigration;
        }
    }
    return inventory.latest_xcm2;
}

pub const JournalApiError = LifecycleError || error{
    JournalingDisabled,
} || journal_mod.JournalIoError;
pub const JournalReplayError = JournalApiError || ExecuteError || error{
    SequenceMismatch,
    ResultMismatch,
    SnapshotCheckpointRequiresSnapshot,
    SnapshotCheckpointMismatch,
};
pub const SnapshotWriteError = LifecycleError || snapshot_mod.SnapshotCaptureError || snapshot_mod.SnapshotIoError;
pub const SnapshotValidateError = LifecycleError || snapshot_mod.SnapshotValidationError || snapshot_mod.SnapshotIoError;
const StorageFolderEnsureError = persistence_paths.DurableDirectoryError;
const MainLogWriteError = std.Io.File.OpenError || std.Io.File.SyncError || io_context.AppendAllError || std.fmt.BufPrintError;
pub const DefaultJournalApiError = JournalApiError || PersistencePathError || StorageFolderEnsureError;
pub const DefaultJournalReplayError = JournalReplayError || PersistencePathError;
pub const DefaultJournalReplayRangeError = DefaultJournalReplayError || error{
    JournalReplayCountOverflow,
};
pub const DefaultSnapshotWriteError = SnapshotWriteError || PersistencePathError || StorageFolderEnsureError || MainLogWriteError;
pub const DefaultSnapshotValidateError = SnapshotValidateError || PersistencePathError;
pub const PersistStateWithMainLogError = PersistStateError || PersistencePathError || StorageFolderEnsureError || MainLogWriteError || std.Io.Dir.RealPathFileError || error{
    ConfiguredMainLogRequiresDiskBackend,
    PersistStateRejected,
    PersistStateSnapshotMismatch,
    PersistStateSnapshotIdOverflow,
    PersistStateTimestampOverflow,
    MissingPersistSequence,
    PersistenceRootMismatch,
};
pub const SnapshotExistsError = std.Io.Dir.StatFileError;
pub const DefaultSnapshotExistsError = SnapshotExistsError || PersistencePathError;
pub const QueryTypedReportError = QueryReportError || error{
    UnexpectedReportResponseType,
};
pub const HelperError = error{
    OrderBookDepthOverflow,
};
pub const AsyncSubmitError = LifecycleError || std.mem.Allocator.Error || publication.PrepareError || error{
    RequestIdOverflow,
};
pub const DrainPendingError = ExecuteError || std.mem.Allocator.Error;
pub const ProcessCommandResultCodeError = AsyncSubmitError || DrainPendingError || error{
    MissingAsyncResult,
};
pub const ProcessCommandFullResponseError = SubmitCommandAsyncFullResponseError || DrainPendingError || error{
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
    InvalidBinaryCommandType,
    EmptyBinaryPayload,
    BinaryPayloadSizeOverflow,
};
pub const BinaryPayloadAsyncSubmitError = AsyncSubmitError || error{
    InvalidBinaryCommandType,
    EmptyBinaryPayload,
    BinaryPayloadSizeOverflow,
};
pub const BinaryPayloadTypedSubmitError = BinaryPayloadSubmitError || binary_payload.EncodeError || java_binary_wire.RequestEncodeError;
pub const BinaryPayloadTypedAsyncSubmitError = BinaryPayloadAsyncSubmitError || binary_payload.EncodeError || java_binary_wire.RequestEncodeError;
pub const BinaryReportQuerySubmitError = BinaryPayloadSubmitError || std.mem.Allocator.Error || java_binary_wire.RequestEncodeError;
pub const BinaryReportQueryAsyncSubmitError = BinaryPayloadAsyncSubmitError || std.mem.Allocator.Error || java_binary_wire.RequestEncodeError;
pub const CustomReportAsyncSubmitError = AsyncSubmitError ||
    custom_report_pipeline.StageRequestError;
pub const TakeCustomReportResultError = custom_report_pipeline.PendingReductionError ||
    error{
        CustomReportCommandFailed,
        CustomReportCompletionMismatch,
    };
pub const ProcessCustomReportError = CustomReportAsyncSubmitError ||
    DrainPendingError ||
    TakeCustomReportResultError ||
    error{MissingAsyncResult};
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
pub const SubmitCommandAsyncFullResponseError = SubmitApiCommandAsyncError || error{
    UnsupportedAsyncFullResponseCommand,
};
pub const SubmitApiCommandBatchError = SubmitApiCommandError ||
    SubmitApiCommandAsyncError ||
    DrainPendingError ||
    std.mem.Allocator.Error ||
    error{
        EmptyApiCommandBatch,
        MissingBatchResult,
        UnexpectedFlushResultVariant,
    };
pub const EnqueueRuntimeError = LifecycleError || std.mem.Allocator.Error || publication.PrepareError;
pub const RuntimeDrainError = ExecuteError || std.mem.Allocator.Error;
pub const PersistStateRequestPair = api_command_mod.PersistStateRequestPair;
pub const ProcessResultError = error{
    CoreReentrantCall,
    UnexpectedPersistStateResult,
};
pub const CallbackRegistrationError = error{
    CoreReentrantCall,
    UnsupportedPersistStateHandle,
    CallbackRegistryFull,
};
pub const SubmitCommandWithCallbackError = AsyncSubmitError || HelperError || error{
    UnsupportedCallbackCommand,
    CallbackTimestampOverflow,
    GeneratedOrderIdOverflow,
};
pub const ProcessRegisteredCallbackError = ProcessResultError || CallbackRegistrationError;

pub const CallbackFailureKind = exchange_core_mod.CallbackFailureKind;
pub const CallbackFailure = exchange_core_mod.CallbackFailure;
pub const CommandResponseCallback = exchange_core_mod.CommandResponseCallback;

const MaxCallbackRegistrations = 2048;
const CallbackRegistration = struct {
    request_id: u64,
    callback: CommandResponseCallback,
};

const WorkKind = enum {
    asynchronous,
    asynchronous_raw,
    synchronous,
    synchronous_raw,
    detached_api,
    detached_api_raw,
    runtime,
};

const CallbackCompletionOwner = struct {
    request_id: u64,
    callback: CommandResponseCallback,
};

const CustomReportCompletionOwner = struct {
    request_id: u64,
    pending: custom_report_pipeline.PendingRequest,
};

const CustomReportReady = struct {
    pending: custom_report_pipeline.PendingRequest,
    response: kernel_mod.CommandResponse,
};

/// Completion ownership is exclusive from publication onward: a result is
/// either detached, poll-owned, or callback-owned. This makes callback/poll
/// stealing and submit-then-register races unrepresentable.
const CompletionOwner = union(enum) {
    detached,
    poll: u64,
    callback: CallbackCompletionOwner,
    custom_report: CustomReportCompletionOwner,

    fn requestId(self: CompletionOwner) ?u64 {
        return switch (self) {
            .detached => null,
            .poll => |request_id| request_id,
            .callback => |owner| owner.request_id,
            .custom_report => |owner| owner.request_id,
        };
    }

    fn ownsPollSlot(self: CompletionOwner) bool {
        return switch (self) {
            .detached, .callback => false,
            .poll, .custom_report => true,
        };
    }
};

const CustomBinaryCompletion = struct {
    staged: *custom_report_pipeline.StagedRequest,
    pending_out: *?custom_report_pipeline.PendingRequest,
};

/// Completion belongs to the logical binary transfer. Intermediate carrier
/// frames are detached; only the terminal frame can publish a poll result.
const BinaryCompletionMode = union(enum) {
    synchronous,
    asynchronous,
    detached,
    custom_report: CustomBinaryCompletion,

    fn workKind(self: @This()) WorkKind {
        return switch (self) {
            .synchronous => .synchronous,
            .asynchronous, .custom_report => .asynchronous,
            .detached => .detached_api,
        };
    }

    fn ownsPollSlot(self: @This()) bool {
        return switch (self) {
            .synchronous, .asynchronous, .custom_report => true,
            .detached => false,
        };
    }

    fn hasRequestId(self: @This()) bool {
        return switch (self) {
            .synchronous, .asynchronous, .custom_report => true,
            .detached => false,
        };
    }

    fn countsAsAsync(self: @This()) bool {
        return switch (self) {
            .asynchronous, .custom_report => true,
            .synchronous, .detached => false,
        };
    }
};

const ShardedWorkItem = struct {
    acceptance_id: u64,
    sequence: publication.PublicationSequence,
    mode: ExecutionMode,
    command: cmd.OrderCommand,
    completion: CompletionOwner,
    kind: WorkKind,
};

const PreparedShardedWork = struct {
    item: ShardedWorkItem,
    journal_batch_position: serialization_processor.JournalBatchPosition,
};

const ShardedWorkQueue = runtime_queue_mod.RuntimeQueue(ShardedWorkItem);

const DeferredAcceptedSettlement = struct {
    first: u64,
    last: u64,
};

const RuntimeDriverState = struct {
    io: std.Io,
    queue: ShardedWorkQueue,
    /// Borrowed queue-head publications copied into the persistent runtime in
    /// one atomic admission. Storage is allocated once and reused exclusively
    /// by the sole driver consumer.
    grouped_submissions: []sharded_pipeline.Submission,
    grouped_batch_high_watermark: usize = 0,
    lifecycle: runtime_mod.RuntimeLifecycle = .cold,
    mutex: std.Io.Mutex = .init,
    work_available: std.Io.Condition = .init,
    space_available: std.Io.Condition = .init,
    completion_available: std.Io.Condition = .init,
    owner: ?*ExchangeApiSharded = null,
    worker: ?std.Thread = null,
    worker_id: ?std.Thread.Id = null,
    join_claimed: bool = false,
    cold_shutdown_drainer_active: bool = false,
    close_requested: std.atomic.Value(bool) = .init(false),
    worker_quiesced: std.Io.Event = .unset,
    join_complete: std.Io.Event = .unset,
    accepted_total: u64 = 0,
    completed_total: u64 = 0,
    /// Largest acceptance identity whose entire prefix has terminally settled.
    /// Unlike `completed_total`, this frontier cannot be advanced by a later
    /// unpublished binary suffix while an earlier callback is still running.
    settled_acceptance_frontier: u64 = 0,
    /// Fatal binary truncation is the only settlement path that can complete a
    /// later accepted range before the single consumer settles its prefix.
    /// Retaining that one range makes prefix advancement infallible and
    /// allocation-free even when the unpublished suffix exceeds queue capacity.
    deferred_accepted_settlement: ?DeferredAcceptedSettlement = null,
    async_accepted_total: u64 = 0,
    async_completed_total: u64 = 0,
    async_drained_total: u64 = 0,
    runtime_accepted_total: u64 = 0,
    runtime_completed_total: u64 = 0,
    runtime_drained_total: u64 = 0,
    completion_slots_used: usize = 0,
    callback_completions_pending: usize = 0,
    fatal_poll_settlements_pending: usize = 0,
    journal_slots_reserved: usize = 0,
    blocked_producers: usize = 0,
    accepted_drain_waiters: usize = 0,
    active_cold_calls: usize = 0,
    manual_drain_active: bool = false,
    bootstrap_owner: ?std.Thread.Id = null,
    last_runtime_result: cmd.CommandResultCode = .accepted,
    /// A running binary transfer reserves its entire sequence range before it
    /// exposes bounded chunks. Later publishers wait at this gate so no work
    /// can interleave with that reserved FIFO range.
    active_binary_stream: bool = false,
    publication_sequencer: publication.PublicationSequencer = .init(0),
    /// Number of already-grouped queue slots in the currently available Java
    /// sequence range. Producers may append behind this frontier.
    prepared_available_remaining: usize = 0,

    fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        capacity: usize,
        max_batch_size: usize,
    ) ShardedWorkQueue.InitError!RuntimeDriverState {
        var queue = try ShardedWorkQueue.init(allocator, capacity);
        errdefer queue.deinit();
        const grouped_submissions = try allocator.alloc(
            sharded_pipeline.Submission,
            max_batch_size,
        );
        return .{
            .io = io,
            .queue = queue,
            .grouped_submissions = grouped_submissions,
        };
    }

    fn deinit(self: *RuntimeDriverState) void {
        self.queue.allocator.free(self.grouped_submissions);
        self.queue.deinit();
        self.* = undefined;
    }
};

fn shutdownDeadlineExpired(io: std.Io, deadline: std.Io.Timeout) bool {
    return switch (deadline) {
        .none => false,
        .duration => unreachable,
        .deadline => |timestamp| timestamp.compare(
            .lte,
            .now(io, timestamp.clock),
        ),
    };
}

/// `Event.waitTimeout` reports a spurious wake as `error.Timeout`. Keep using
/// the one normalized absolute deadline until either the event is set or the
/// actual clock reaches it.
fn waitForShutdownEvent(
    event: *std.Io.Event,
    io: std.Io,
    deadline: std.Io.Timeout,
) std.Io.Event.WaitTimeoutError!void {
    if (deadline == .none) {
        event.waitUncancelable(io);
        return;
    }
    while (!event.isSet()) {
        event.waitTimeout(io, deadline) catch |err| switch (err) {
            error.Timeout => {
                if (event.isSet()) return;
                if (shutdownDeadlineExpired(io, deadline)) return error.Timeout;
            },
            else => return err,
        };
    }
}

fn lockShutdownDriver(
    mutex: *std.Io.Mutex,
    io: std.Io,
    deadline: std.Io.Timeout,
) std.Io.Event.WaitTimeoutError!void {
    if (deadline == .none) {
        mutex.lockUncancelable(io);
        return;
    }
    // Preserve the useful zero-timeout fast path when the lock is immediately
    // available, then enforce the normalized deadline around every retry.
    if (mutex.tryLock()) return;
    while (true) {
        if (shutdownDeadlineExpired(io, deadline)) return error.Timeout;
        std.Thread.yield() catch {};
        if (shutdownDeadlineExpired(io, deadline)) return error.Timeout;
        if (mutex.tryLock()) {
            // A caller may be descheduled between the preflight clock check
            // and the acquisition. Do not turn a post-deadline acquisition
            // into success.
            if (shutdownDeadlineExpired(io, deadline)) {
                mutex.unlock(io);
                return error.Timeout;
            }
            return;
        }
    }
}

const BinaryFrameLayout = struct {
    words_without_padding: usize,
    frame_count: usize,
};

pub const PersistenceFaultPoint = enum {
    before_manifest_publish,
};

const PendingPersistPair = struct {
    snapshot_id: u64,
    timestamp_ns: u64,
    matching_sequence: u64,
    mode: ExecutionMode,
    matching_store_succeeded: bool,
};

/// XCJ3 is deliberately isolated from Java/XCJ1/XCJ2 `.ecj` files. Keeping a
/// native suffix makes an incompatible artifact a typed startup refusal rather
/// than a candidate for truncation or replacement.
const NativeJournalPartitionId: u16 = 1;

const JournalCapacityTestGate = struct {
    reached: std.atomic.Value(bool) = .init(false),

    fn markReached(self: *@This()) void {
        self.reached.store(true, .release);
    }

    fn waitUntilReached(self: *@This()) void {
        while (!self.reached.load(.acquire)) std.Thread.yield() catch {};
    }
};

const AvailableBatchPauseTestGate = struct {
    entered: std.atomic.Value(bool) = .init(false),
    released: std.atomic.Value(bool) = .init(false),
    consumed: std.atomic.Value(bool) = .init(false),

    fn pauseOnce(self: *@This()) void {
        if (self.consumed.swap(true, .acq_rel)) return;
        self.entered.store(true, .release);
        while (!self.released.load(.acquire)) std.Thread.yield() catch {};
    }

    fn waitUntilEntered(self: *@This()) void {
        while (!self.entered.load(.acquire)) std.Thread.yield() catch {};
    }

    fn open(self: *@This()) void {
        self.released.store(true, .release);
    }
};

const GroupedPoisonReacquireTestGate = struct {
    reached: std.atomic.Value(bool) = .init(false),
    release: std.atomic.Value(bool) = .init(false),

    fn waitUntilReached(self: *@This()) void {
        while (!self.reached.load(.acquire)) std.Thread.yield() catch {};
    }

    fn pauseBeforeReacquire(self: *@This()) void {
        self.reached.store(true, .release);
        while (!self.release.load(.acquire)) std.Thread.yield() catch {};
    }

    fn open(self: *@This()) void {
        self.release.store(true, .release);
    }
};

const AcceptedPrefixBarrierTestGate = struct {
    entered: std.atomic.Value(bool) = .init(false),
    released: std.atomic.Value(bool) = .init(false),
    consumed: std.atomic.Value(bool) = .init(false),

    fn pauseOnce(self: *@This()) void {
        if (self.consumed.swap(true, .acq_rel)) return;
        self.entered.store(true, .release);
        while (!self.released.load(.acquire)) std.Thread.yield() catch {};
    }

    fn open(self: *@This()) void {
        self.released.store(true, .release);
    }
};

const PersistentCapacityTestGate = struct {
    reached: std.atomic.Value(bool) = .init(false),

    fn markReached(self: *@This()) void {
        self.reached.store(true, .release);
    }
};

fn nativeJournalPartitionPath(
    allocator: std.mem.Allocator,
    config: *const Config,
) PersistencePathError![]u8 {
    const file_name = try std.fmt.allocPrint(
        allocator,
        "{s}_journal_native_{X:0>4}.xcj3",
        .{ config.initial_state.exchange_id, NativeJournalPartitionId },
    );
    defer allocator.free(file_name);
    return persistence_paths.resolveInFolder(
        allocator,
        config.serialization.storage_folder,
        file_name,
    );
}

/// Java `DiskSerializationProcessor.writeToJournal` returns for every command
/// whose `OrderCommandType.isMutate()` flag is false. XCJ3 synchronizes every
/// appended record, so Java's preceding shutdown-only buffer flush has no
/// additional native action here.
fn shouldAppendToJavaDurableJournal(command_type: cmd.OrderCommandType) bool {
    return command_type.isMutating();
}

fn groupingConfig(runtime_config: runtime_mod.Config) grouping_processor.Config {
    return .{
        .max_messages_in_group = runtime_config.max_batch_size,
        .max_group_duration_ns = runtime_config.max_group_duration_ns,
        .l2_publish_interval_ns = runtime_config.l2_publish_interval_ns,
        .l2_flags_every_n_commands = runtime_config.l2_flags_every_n_commands,
    };
}

pub const ExchangeApiSharded = struct {
    /// The caller-owned allocator remains the lifetime boundary for response
    /// payloads returned by this API. Direct release through that allocator is
    /// valid after this API is quiesced/deinitialized, or while it is live
    /// when the backing allocator is itself thread-safe. While a
    /// non-thread-safe backing allocator is shared with live API workers,
    /// callers must use `releaseResponse` so the release crosses the same
    /// serialized facade as every runtime allocation.
    backing_allocator: std.mem.Allocator,
    /// Heap-stable because allocator handles retain its address.
    locked_allocator: *locked_allocator_mod.LockedAllocator,
    /// Runtime-owned allocations use the serialized facade. This includes the
    /// core, driver, registries, journals, and result storage reachable by
    /// worker and producer threads.
    allocator: std.mem.Allocator,
    config: Config,
    custom_serialization_processor: ?serialization_processor.Processor,
    custom_report_registry: *report_registry.Registry,
    custom_report_requests: *custom_report_pipeline.RequestStore,
    /// Independently opened handle for every automatic XCM2 operation. Disk
    /// configurations own one handle; dummy configurations own none.
    persistence_root: ?std.Io.Dir,
    core: sharded_core_mod.ShardedExchangeCore,
    descriptors: persistence_mod.DescriptorStore,
    current_snapshot_ref: SnapshotRef,
    journal: ?journal_mod.CommandJournal = null,
    durable_journal: ?journal_mod.DurableJournalAppender = null,
    settlement_hooks: settlement_lifecycle.Hooks = settlement_lifecycle.Hooks.disabled(),
    settlement_recovery_finished: bool = false,
    next_request_id: u64 = 0,
    ready_results: std.AutoHashMapUnmanaged(u64, kernel_mod.CommandResponse) = .empty,
    custom_ready_results: std.AutoHashMapUnmanaged(u64, CustomReportReady) = .empty,
    raw_result_sink: RawResultSink = .{},
    runtime_sink: runtime_mod.ResultSink = .{},
    simple_events_processor: ?simple_events_mod.SimpleEventsProcessor = null,
    simple_events_handler: ?SimpleEventsHandler = null,
    simple_events_revision: u64 = 0,
    /// Java ResultsHandler state. This is intentionally independent from the
    /// publication-side grouping metadata state above.
    results_processing_enabled: bool = true,
    callback_registrations_len: usize = 0,
    callback_registrations: [MaxCallbackRegistrations]CallbackRegistration = undefined,
    driver: *RuntimeDriverState,
    /// Created only by `startup`, after this API has reached its heap-stable
    /// production address. Cold/manual execution remains immediate.
    persistent_grouped_runtime: ?*sharded_pipeline.PersistentGroupedRuntime = null,
    persistent_grouped_activated: bool = false,
    persistent_grouped_spawn_failure_after: ?usize = null,
    persistent_grouped_driver_spawn_failure: bool = false,
    persistent_grouped_stage_gate: ?*sharded_pipeline.PersistentGroupedRuntime.StageGate = null,
    strict_snapshot_disappeared_after_validation_for_testing: bool = false,
    available_batch_test_gate: ?*AvailableBatchPauseTestGate = null,
    journal_capacity_test_gate: ?*JournalCapacityTestGate = null,
    grouped_poison_reacquire_test_gate: ?*GroupedPoisonReacquireTestGate = null,
    accepted_prefix_barrier_test_gate: ?*AcceptedPrefixBarrierTestGate = null,
    persistent_capacity_test_gate: ?*PersistentCapacityTestGate = null,
    /// Separates accepted-prefix capture from a newer atomic grouped admission.
    /// Batch admission only tries this mutex while holding `driver.mutex`; it
    /// never waits in that lock order. Once admitted, the sole consumer retains
    /// the fence through response observers, callbacks, and outer settlement.
    batch_barrier_fence: std.Io.Mutex = .init,
    execution_mutex: std.Io.Mutex = .init,
    callback_mutex: std.Io.Mutex = .init,
    callback_execution_mutex: std.Io.Mutex = .init,
    grouping: grouping_processor.State,
    grouping_clock: runtime_mod.MonotonicClock,
    pending_persist_pair: ?PendingPersistPair = null,
    persistence_fault: ?PersistenceFaultPoint = null,
    initial_state_bootstrapped: bool = false,

    pub fn init(
        allocator: std.mem.Allocator,
        shard_count: usize,
        orderbook_kind: kernel_mod.OrderBookKind,
    ) InitError!ExchangeApiSharded {
        var core_config = Config.javaDefault();
        core_config.matching.orderbook_kind = orderbook_kind;
        core_config.matching.num_shards = @intCast(shard_count);
        return initWithTopologyAndPersistenceRoot(
            allocator,
            std.Io.Dir.cwd(),
            core_config,
            shard_count,
            shard_count,
            false,
        );
    }

    /// Opens and owns an independent handle for `persistence_root` when the
    /// disk backend is enabled. The supplied handle is borrowed only for this
    /// call and may be closed immediately after return.
    pub fn initWithPersistenceRoot(
        allocator: std.mem.Allocator,
        persistence_root: std.Io.Dir,
        core_config: Config,
    ) InitError!ExchangeApiSharded {
        const shard_count = std.math.cast(usize, core_config.matching.num_shards) orelse
            return error.MatchingEngineCountOverflow;
        return initWithTopologyAndPersistenceRoot(
            allocator,
            persistence_root,
            core_config,
            shard_count,
            shard_count,
            true,
        );
    }

    pub fn initFromJavaConfig(
        allocator: std.mem.Allocator,
        java_config: config_mod.JavaExchangeConfig,
    ) InitFromJavaConfigError!ExchangeApiSharded {
        return initFromJavaConfigWithPersistenceRootInternal(
            allocator,
            std.Io.Dir.cwd(),
            java_config,
            false,
        );
    }

    /// Java-configured topology with an explicit, independently owned
    /// persistence root. Matching and risk engine counts remain independent.
    pub fn initFromJavaConfigWithPersistenceRoot(
        allocator: std.mem.Allocator,
        persistence_root: std.Io.Dir,
        java_config: config_mod.JavaExchangeConfig,
    ) InitFromJavaConfigError!ExchangeApiSharded {
        return initFromJavaConfigWithPersistenceRootInternal(
            allocator,
            persistence_root,
            java_config,
            true,
        );
    }

    fn initFromJavaConfigWithPersistenceRootInternal(
        allocator: std.mem.Allocator,
        persistence_root: std.Io.Dir,
        java_config: config_mod.JavaExchangeConfig,
        enable_durable_in_tests: bool,
    ) InitFromJavaConfigError!ExchangeApiSharded {
        var core_config = try java_config.toExchangeCoreConfig();
        core_config.matching.num_shards = java_config.performance_cfg.matching_engines_num;
        const matching_engine_count = std.math.cast(
            usize,
            java_config.performance_cfg.matching_engines_num,
        ) orelse
            return error.MatchingEngineCountOverflow;
        const risk_engine_count = std.math.cast(
            usize,
            java_config.performance_cfg.risk_engines_num,
        ) orelse
            return error.RiskEngineCountOverflow;

        return initWithTopologyAndPersistenceRoot(
            allocator,
            persistence_root,
            core_config,
            matching_engine_count,
            risk_engine_count,
            enable_durable_in_tests,
        );
    }

    fn initWithTopologyAndPersistenceRoot(
        backing_allocator: std.mem.Allocator,
        persistence_root: std.Io.Dir,
        core_config: Config,
        matching_count: usize,
        risk_count: usize,
        enable_durable_in_tests: bool,
    ) InitError!ExchangeApiSharded {
        try core_config.validate();
        const io = io_context.current();
        const locked_allocator = try backing_allocator.create(
            locked_allocator_mod.LockedAllocator,
        );
        errdefer backing_allocator.destroy(locked_allocator);
        locked_allocator.* = locked_allocator_mod.LockedAllocator.init(
            backing_allocator,
            io,
        );
        const allocator = locked_allocator.allocator();

        var custom_serialization_processor: ?serialization_processor.Processor =
            if (core_config.serialization.custom_processor_factory) |factory|
                try factory.createProcessor(allocator, .{
                    .exchange_id = core_config.initial_state.exchange_id,
                    .matching_engine_count = matching_count,
                    .risk_engine_count = risk_count,
                    .enable_journaling = core_config.serialization.enable_journaling,
                    .snapshot_id = core_config.initial_state.snapshot_id,
                    .snapshot_base_seq = core_config.initial_state.snapshot_base_seq,
                    .journal_timestamp_ns = core_config.initial_state.journal_timestamp_ns,
                    .throw_if_snapshot_not_found = core_config.initial_state.throw_if_snapshot_not_found,
                })
            else
                null;
        errdefer if (custom_serialization_processor) |*processor| {
            processor.deinit(allocator);
        };

        const custom_report_registry = try core_config.reports_queries.custom_report_registry.cloneOwned(
            allocator,
        );
        errdefer custom_report_registry.deinit();
        var owned_config = core_config;
        owned_config.reports_queries.custom_report_registry = custom_report_registry;

        const owned_persistence_root: ?std.Io.Dir = switch (owned_config.serialization.backend) {
            .dummy => null,
            .disk => try persistence_root.openDir(io, ".", .{}),
            .custom => null,
        };
        errdefer if (owned_persistence_root) |root_dir| root_dir.close(io);

        const runtime_config = owned_config.toRuntimeConfig();
        const grouping = try grouping_processor.State.init(groupingConfig(runtime_config));
        const matching_engine_count = std.math.cast(u32, matching_count) orelse
            return error.MatchingEngineCountOverflow;
        const risk_engine_count = std.math.cast(u32, risk_count) orelse
            return error.RiskEngineCountOverflow;

        var descriptors = persistence_mod.DescriptorStore.init(allocator);
        errdefer descriptors.deinit();
        const root_snapshot_ref = try descriptors.createEmptySnapshot(
            matching_engine_count,
            risk_engine_count,
        );
        const journal = if (owned_config.serialization.enable_journaling)
            journal_mod.CommandJournal.init(allocator)
        else
            null;

        var durable_journal: ?journal_mod.DurableJournalAppender = null;
        if (journal != null and
            owned_config.serialization.backend == .disk and
            (!builtin.is_test or enable_durable_in_tests))
        {
            const root_dir = owned_persistence_root orelse unreachable;
            try persistence_paths.ensureDirPathDurable(
                io,
                root_dir,
                owned_config.serialization.storage_folder,
            );
            const native_path = try nativeJournalPartitionPath(allocator, &owned_config);
            defer allocator.free(native_path);
            durable_journal = try journal_mod.DurableJournalAppender.openWithIo(
                allocator,
                io,
                root_dir,
                native_path,
            );
        }
        errdefer if (durable_journal) |*appender| appender.deinit();

        var core = try sharded_core_mod.ShardedExchangeCore.initWithTopologyConfig(
            allocator,
            matching_count,
            risk_count,
            owned_config.toKernelConfig(),
        );
        errdefer core.deinit();

        var ready_results: std.AutoHashMapUnmanaged(u64, kernel_mod.CommandResponse) = .empty;
        errdefer ready_results.deinit(allocator);
        var custom_ready_results: std.AutoHashMapUnmanaged(u64, CustomReportReady) = .empty;
        errdefer custom_ready_results.deinit(allocator);
        const completion_capacity = std.math.cast(
            std.AutoHashMapUnmanaged(u64, kernel_mod.CommandResponse).Size,
            owned_config.runtime.queue_capacity,
        ) orelse return error.CompletionCapacityOverflow;
        try ready_results.ensureTotalCapacity(allocator, completion_capacity);
        try custom_ready_results.ensureTotalCapacity(allocator, completion_capacity);

        const driver = try allocator.create(RuntimeDriverState);
        errdefer allocator.destroy(driver);
        driver.* = try RuntimeDriverState.init(
            allocator,
            io_context.current(),
            owned_config.runtime.queue_capacity,
            owned_config.runtime.max_batch_size,
        );
        errdefer driver.deinit();

        const custom_report_requests = try allocator.create(
            custom_report_pipeline.RequestStore,
        );
        errdefer allocator.destroy(custom_report_requests);
        custom_report_requests.* = try custom_report_pipeline.RequestStore.init(
            allocator,
            driver.io,
            custom_report_registry,
            @ptrCast(driver),
        );
        errdefer custom_report_requests.deinit();

        return .{
            .backing_allocator = backing_allocator,
            .locked_allocator = locked_allocator,
            .allocator = allocator,
            .config = owned_config,
            .custom_serialization_processor = custom_serialization_processor,
            .custom_report_registry = custom_report_registry,
            .custom_report_requests = custom_report_requests,
            .persistence_root = owned_persistence_root,
            .core = core,
            .descriptors = descriptors,
            .current_snapshot_ref = root_snapshot_ref,
            .journal = journal,
            .durable_journal = durable_journal,
            .ready_results = ready_results,
            .custom_ready_results = custom_ready_results,
            .driver = driver,
            .grouping = grouping,
            .grouping_clock = runtime_config.clock,
        };
    }

    /// The caller must exclude concurrent public calls, including concurrent
    /// shutdown waiters. Sequential deinit after a timed-out close is safe:
    /// its unbounded shutdown finishes or joins the retained worker first.
    pub fn deinit(self: *ExchangeApiSharded) void {
        self.assertWiring();
        if (runtime_mod.isCallbackExecutionActive() or self.isWorkerThread()) {
            @panic("ExchangeApiSharded.deinit cannot run from its worker callback");
        }
        self.shutdown();
        self.callback_execution_mutex.lockUncancelable(self.driver.io);
        self.callback_mutex.lockUncancelable(self.driver.io);
        self.raw_result_sink = .{};
        self.runtime_sink = .{};
        self.simple_events_handler = null;
        if (self.simple_events_processor) |*processor| processor.deinit();
        self.simple_events_processor = null;
        self.callback_mutex.unlock(self.driver.io);
        self.callback_execution_mutex.unlock(self.driver.io);

        var ready_it = self.ready_results.valueIterator();
        while (ready_it.next()) |response| {
            response.deinit(self.allocator);
        }
        self.ready_results.deinit(self.allocator);
        var custom_ready_it = self.custom_ready_results.valueIterator();
        while (custom_ready_it.next()) |ready| {
            ready.response.deinit(self.allocator);
        }
        self.custom_ready_results.deinit(self.allocator);
        self.custom_report_requests.deinit();
        self.allocator.destroy(self.custom_report_requests);
        self.callback_registrations_len = 0;
        std.debug.assert(self.persistent_grouped_runtime == null);
        if (self.durable_journal) |*appender| {
            appender.deinit();
            self.durable_journal = null;
        }
        if (self.journal) |*journal| {
            journal.deinit();
            self.journal = null;
        }

        self.descriptors.deinit();
        if (self.custom_serialization_processor) |*processor| {
            processor.deinit(self.allocator);
            self.custom_serialization_processor = null;
        }
        self.core.deinit();
        self.custom_report_registry.deinit();
        if (self.persistence_root) |root_dir| {
            root_dir.close(self.driver.io);
            self.persistence_root = null;
        }
        self.driver.deinit();
        self.allocator.destroy(self.driver);
        self.backing_allocator.destroy(self.locked_allocator);
        self.* = undefined;
    }

    pub fn shardCount(self: *const ExchangeApiSharded) usize {
        return self.matchingShardCount();
    }

    pub fn matchingShardCount(self: *const ExchangeApiSharded) usize {
        return self.core.matchingShardCount();
    }

    pub fn riskShardCount(self: *const ExchangeApiSharded) usize {
        return self.core.riskShardCount();
    }

    pub fn matchingShardMask(self: *const ExchangeApiSharded) u64 {
        return self.core.matchingShardMask();
    }

    pub fn riskShardMask(self: *const ExchangeApiSharded) u64 {
        return self.core.riskShardMask();
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

    /// Transitional persistence bridge only. Shard zero is not authoritative
    /// sharded state and is intentionally not exposed through the public API.
    fn kernelRef(self: *ExchangeApiSharded) *kernel_mod.ExchangeKernel {
        return self.core.kernels.matchingShardAt(0);
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
        self.assertWiringConst();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        return driver.lifecycle.current() != .cold;
    }

    pub fn isStopped(self: *const ExchangeApiSharded) bool {
        self.assertWiringConst();
        const driver = self.driver;
        if (driver.close_requested.load(.acquire)) return true;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        if (driver.close_requested.load(.acquire)) return true;
        return switch (driver.lifecycle.current()) {
            .closing, .stopped, .poisoned => true,
            .cold, .running => false,
        };
    }

    /// Installs the synchronous local settlement boundary before any restore,
    /// replay, or live publication. Enabled hooks require the native XCJ3
    /// appender: a custom serialization callback is not a durability proof.
    /// Hooks may inspect non-blocking state, but blocking exchange calls are
    /// rejected with `CoreReentrantCall` while any hook is executing.
    pub fn setSettlementHooks(
        self: *ExchangeApiSharded,
        hooks: settlement_lifecycle.Hooks,
    ) SettlementHooksConfigError!void {
        self.assertWiring();
        if (runtime_mod.isCallbackExecutionActive() or active_worker_api != null) {
            return error.SettlementHooksReconfigurationWouldBlock;
        }

        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        if (driver.lifecycle.current() != .cold or
            driver.close_requested.load(.acquire))
        {
            return error.SettlementHooksRequireColdCore;
        }
        if (driver.active_cold_calls != 0 or
            driver.manual_drain_active or
            driver.bootstrap_owner != null or
            driver.active_binary_stream)
        {
            return error.SettlementHooksReconfigurationWouldBlock;
        }

        if (self.initial_state_bootstrapped or
            self.settlement_recovery_finished or
            driver.publication_sequencer.highWater() != 0 or
            self.core.kernels.next_seq != 0)
        {
            return error.SettlementHooksRequireColdCore;
        }
        if (hooks.isEnabled() and self.durable_journal == null) {
            return error.SettlementHooksRequireNativeDurableJournal;
        }

        self.settlement_hooks = hooks;
        self.settlement_recovery_finished = false;
    }

    pub fn clearSettlementHooks(
        self: *ExchangeApiSharded,
    ) SettlementHooksConfigError!void {
        try self.setSettlementHooks(settlement_lifecycle.Hooks.disabled());
    }

    /// Starts the sole queue consumer. Once this succeeds, the API value must
    /// remain at this address until `shutdown`/`deinit` joins the worker.
    pub fn startup(self: *ExchangeApiSharded) StartError!void {
        self.assertWiring();
        try self.ensureBlockingCallAllowed();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);

        const current_thread = std.Thread.getCurrentId();
        while (driver.manual_drain_active or
            driver.active_cold_calls != 0 or
            (driver.bootstrap_owner != null and driver.bootstrap_owner.? != current_thread))
        {
            driver.completion_available.waitUncancelable(driver.io, &driver.mutex);
        }
        const lifecycle = driver.lifecycle.current();
        switch (lifecycle) {
            .poisoned => return error.CorePoisoned,
            .closing, .stopped => return error.CoreStopped,
            .cold, .running => {},
        }
        if (driver.close_requested.load(.acquire)) return error.CoreStopped;
        if (lifecycle == .running) return;
        if (self.custom_serialization_processor) |*processor| {
            try processor.startup(.{
                .exchange_id = self.config.initial_state.exchange_id,
                .snapshot_id = self.config.initial_state.snapshot_id,
                .snapshot_base_seq = self.config.initial_state.snapshot_base_seq,
                .journal_timestamp_ns = self.config.initial_state.journal_timestamp_ns,
                .throw_if_snapshot_not_found = self.config.initial_state.throw_if_snapshot_not_found,
                .enable_journaling = self.config.serialization.enable_journaling,
                .state = .{ .sharded = &self.core.kernels },
            });
            self.initial_state_bootstrapped = true;
        }
        if ((self.config.initial_state.fromSnapshot() or
            self.config.initial_state.journal_timestamp_ns != 0) and
            !self.initial_state_bootstrapped)
        {
            return error.ConfiguredPersistenceBootstrapRequired;
        }
        try self.validateDurableJournalFrontier();
        self.finishSettlementRecoveryLocked() catch return error.CorePoisoned;

        try self.core.startup();
        driver.owner = self;
        self.startPersistentGroupedRuntime() catch |err| {
            driver.owner = null;
            self.persistent_grouped_activated = false;
            self.core.rollbackStartup();
            return err;
        };
        _ = driver.lifecycle.start() catch unreachable;
        if (self.persistent_grouped_driver_spawn_failure) {
            driver.lifecycle = .cold;
            driver.owner = null;
            self.stopPersistentGroupedRuntime();
            self.persistent_grouped_activated = false;
            self.core.rollbackStartup();
            return error.InjectedDriverSpawnFailure;
        }
        driver.worker = std.Thread.spawn(.{}, workerMain, .{driver}) catch |err| {
            driver.lifecycle = .cold;
            driver.owner = null;
            self.stopPersistentGroupedRuntime();
            self.persistent_grouped_activated = false;
            self.core.rollbackStartup();
            return err;
        };
        self.persistent_grouped_activated =
            self.config.sharded_pipeline_mode == .persistent_grouped;
        driver.work_available.broadcast(driver.io);
    }

    fn startPersistentGroupedRuntime(
        self: *ExchangeApiSharded,
    ) (sharded_pipeline.PersistentGroupedRuntime.CreateError ||
        sharded_pipeline.PersistentGroupedRuntime.StartError)!void {
        if (self.config.sharded_pipeline_mode != .persistent_grouped) return;
        std.debug.assert(self.persistent_grouped_runtime == null);
        const runtime = try sharded_pipeline.PersistentGroupedRuntime.create(
            self.allocator,
            &self.core.kernels,
            .{
                .capacity = self.config.runtime.queue_capacity,
                // `self.allocator` and every kernel payload allocator are the
                // same heap-stable LockedAllocator facade.
                .payload_allocator_is_thread_safe = true,
                .spawn_failure_after = self.persistent_grouped_spawn_failure_after,
                .stage_gate = self.persistent_grouped_stage_gate,
                .settlement_hooks = self.scopedSettlementHooks(),
            },
        );
        errdefer runtime.destroy() catch unreachable;
        try runtime.start();
        self.persistent_grouped_runtime = runtime;
    }

    /// Requires exclusive API execution ownership whenever the driver worker
    /// may still be live.
    fn stopPersistentGroupedRuntime(self: *ExchangeApiSharded) void {
        const runtime = self.persistent_grouped_runtime orelse return;
        _ = runtime.shutdown() catch {};
        runtime.destroy() catch unreachable;
        self.persistent_grouped_runtime = null;
    }

    pub fn shutdownState(self: *const ExchangeApiSharded) ShutdownState {
        self.assertWiringConst();
        const driver = self.driver;
        if (driver.join_complete.isSet()) return .complete;
        if (driver.close_requested.load(.acquire)) return .closing;

        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        if (driver.join_complete.isSet()) return .complete;
        if (driver.close_requested.load(.acquire)) return .closing;
        return switch (driver.lifecycle.current()) {
            .cold, .running => .open,
            .closing, .stopped, .poisoned => .closing,
        };
    }

    /// Source-compatible unbounded shutdown. Cold external callers retain the
    /// historical inline drain; bounded and self-callback closes use the
    /// heap-stable driver worker.
    pub fn shutdown(self: *ExchangeApiSharded) void {
        while (true) {
            const outcome = self.shutdownTimeout(.none) catch |err| switch (err) {
                error.Canceled => continue,
                error.Timeout => unreachable,
                else => {
                    if (runtime_mod.isCallbackExecutionActive() or
                        self.isWorkerThread())
                    {
                        return;
                    }
                    continue;
                },
            };
            switch (outcome) {
                .complete => {
                    if (self.custom_serialization_processor) |*processor| {
                        processor.shutdown();
                    }
                    return;
                },
                .deferred => return,
            }
        }
    }

    /// Once a cold bounded or callback-initiated close starts, `self` must stay
    /// at this address until shutdown reaches `.complete` or `deinit` returns:
    /// the heap-stable driver intentionally retains its owning facade pointer.
    /// A timed-out call may be followed sequentially by another shutdown call
    /// or `deinit`; deinitialization must not race an active public call.
    /// A callback or worker that cannot acquire its one nonblocking driver-lock
    /// attempt returns `.deferred` after fencing admission only. Keep this
    /// object/address alive and use a later non-callback, non-worker shutdown
    /// retry (or `deinit`) to await `.complete`.
    ///
    /// Zig 0.16 has no timed Thread.join. The deadline bounds application and
    /// backend work, producer settlement, callbacks, and worker quiescence;
    /// after quiescence, the OS handle-reap tail is not a strict wall-clock
    /// bound.
    pub fn shutdownTimeout(
        self: *ExchangeApiSharded,
        timeout: std.Io.Timeout,
    ) ShutdownError!ShutdownOutcome {
        self.assertWiring();
        const driver = self.driver;
        const deadline = timeout.toDeadline(driver.io);
        const caller_must_defer = runtime_mod.isCallbackExecutionActive() or
            self.isWorkerThread();
        driver.close_requested.store(true, .release);
        driver.work_available.broadcast(driver.io);
        driver.space_available.broadcast(driver.io);
        driver.completion_available.broadcast(driver.io);
        if (driver.join_complete.isSet()) return .complete;
        const unbounded = switch (deadline) {
            .none => true,
            .duration, .deadline => false,
        };
        var drain_cold_inline = false;

        if (caller_must_defer) {
            if (!driver.mutex.tryLock()) return .deferred;
        } else {
            try lockShutdownDriver(&driver.mutex, driver.io, deadline);
        }
        if (driver.join_complete.isSet()) {
            driver.mutex.unlock(driver.io);
            return .complete;
        }

        const self_shutdown = caller_must_defer;
        switch (driver.lifecycle.current()) {
            .cold => {
                _ = driver.lifecycle.beginClose() catch unreachable;
                driver.owner = self;
            },
            .running => _ = driver.lifecycle.beginClose() catch unreachable,
            .closing, .stopped, .poisoned => {},
        }

        const needs_cold_drainer =
            !driver.worker_quiesced.isSet() and
            driver.worker == null and
            !driver.cold_shutdown_drainer_active and
            driver.lifecycle.current() != .stopped;
        if (needs_cold_drainer) {
            driver.owner = self;
            if (unbounded and !self_shutdown) {
                driver.cold_shutdown_drainer_active = true;
                drain_cold_inline = true;
            } else {
                driver.worker = std.Thread.spawn(.{}, workerMain, .{driver}) catch |err| {
                    driver.work_available.broadcast(driver.io);
                    driver.space_available.broadcast(driver.io);
                    driver.completion_available.broadcast(driver.io);
                    driver.mutex.unlock(driver.io);
                    return err;
                };
            }
        }

        driver.work_available.broadcast(driver.io);
        driver.space_available.broadcast(driver.io);
        driver.completion_available.broadcast(driver.io);

        if (self_shutdown) {
            driver.mutex.unlock(driver.io);
            return .deferred;
        }
        driver.mutex.unlock(driver.io);

        if (drain_cold_inline) {
            self.runColdShutdownInline();
            return .complete;
        }

        try waitForShutdownEvent(&driver.worker_quiesced, driver.io, deadline);
        return self.reapShutdownWorker(deadline);
    }

    fn runColdShutdownInline(self: *ExchangeApiSharded) void {
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        while (driver.active_cold_calls != 0 or
            driver.manual_drain_active or
            driver.bootstrap_owner != null)
        {
            driver.completion_available.waitUncancelable(
                driver.io,
                &driver.mutex,
            );
        }
        driver.mutex.unlock(driver.io);

        self.drainAcceptedOnCurrentThread();
        self.finalizeDriverExit();

        driver.mutex.lockUncancelable(driver.io);
        driver.owner = null;
        driver.completion_available.broadcast(driver.io);
        driver.space_available.broadcast(driver.io);
        driver.mutex.unlock(driver.io);
        driver.worker_quiesced.set(driver.io);
        // Keep completion publication as the final driver access.
        driver.join_complete.set(driver.io);
    }

    fn reapShutdownWorker(
        self: *ExchangeApiSharded,
        deadline: std.Io.Timeout,
    ) ShutdownError!ShutdownOutcome {
        const driver = self.driver;
        var worker_to_join: ?std.Thread = null;

        try lockShutdownDriver(&driver.mutex, driver.io, deadline);
        if (driver.join_complete.isSet()) {
            driver.mutex.unlock(driver.io);
            return .complete;
        }
        std.debug.assert(driver.worker_quiesced.isSet());
        if (!driver.join_claimed) {
            if (driver.worker) |worker| {
                driver.join_claimed = true;
                driver.worker = null;
                worker_to_join = worker;
            }
        }
        driver.mutex.unlock(driver.io);

        if (worker_to_join) |worker| {
            // Zig 0.16 has no timed join. Quiescence bounds all application and
            // backend work; this post-quiescence call only reaps the OS thread.
            worker.join();
            // Keep completion publication as the final driver access.
            driver.join_complete.set(driver.io);
            return .complete;
        }

        try waitForShutdownEvent(&driver.join_complete, driver.io, deadline);
        return .complete;
    }

    pub fn bootstrapFromPersistence(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_path: ?[]const u8,
        journal_path: ?[]const u8,
    ) BootstrapError!BootstrapOutcome {
        try self.beginBootstrap();
        defer self.finishBootstrap();
        const outcome = try self.bootstrapFromPersistenceColdOwned(dir, snapshot_path, journal_path);
        try self.validateDurableJournalFrontier();
        self.finishSettlementRecovery() catch return error.CorePoisoned;
        self.initial_state_bootstrapped = true;
        return outcome;
    }

    fn bootstrapFromPersistenceColdOwned(
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
            outcome.journal_replayed_commands = try self.replayJournalFromFileColdOwned(dir, path);
        }

        return outcome;
    }

    pub fn startupFromPersistence(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_path: ?[]const u8,
        journal_path: ?[]const u8,
    ) StartupError!BootstrapOutcome {
        try self.beginBootstrap();
        defer self.finishBootstrap();
        const outcome = try self.bootstrapFromPersistenceColdOwned(dir, snapshot_path, journal_path);
        try self.validateDurableJournalFrontier();
        self.finishSettlementRecovery() catch return error.CorePoisoned;
        self.initial_state_bootstrapped = true;
        try self.startup();
        return outcome;
    }

    pub fn startupFromConfiguredPersistence(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_module_type: PersistenceModuleType,
        snapshot_instance_id: u32,
        journal_partition_id: u16,
    ) StartupFromConfiguredPersistenceError!BootstrapOutcome {
        try self.beginBootstrap();
        defer self.finishBootstrap();
        _ = snapshot_module_type;
        _ = snapshot_instance_id;
        const persistence_root = self.persistence_root orelse dir;
        const initial_state = self.config.initial_state;
        var outcome: BootstrapOutcome = .{};
        var snapshot_frontier = initial_state.snapshot_base_seq;
        if (initial_state.fromSnapshot()) {
            if (try self.bootstrapXcm2SnapshotWithId(
                persistence_root,
                initial_state.snapshot_id,
                &outcome,
            )) |info| snapshot_frontier = info.publication_high_water;
        }

        var first_partition_found = false;
        if (initial_state.journal_timestamp_ns != 0 and
            self.settlement_hooks.isEnabled())
        {
            first_partition_found = true;
            outcome.journal_replayed_commands = try self.replayNativeSettlementJournalColdOwnedAfterSequence(
                snapshot_frontier,
            );
        } else if (initial_state.journal_timestamp_ns != 0) {
            const path = try self.defaultPersistenceJournalPath(
                self.allocator,
                initial_state.snapshot_id,
                journal_partition_id,
            );
            defer self.allocator.free(path);
            first_partition_found = true;
            outcome.journal_replayed_commands = self.replayJournalFromFileColdOwnedAfterSequence(
                persistence_root,
                path,
                snapshot_frontier,
            ) catch |err| switch (err) {
                error.FileNotFound => blk: {
                    first_partition_found = false;
                    break :blk 0;
                },
                else => return err,
            };
        }

        if (!self.settlement_hooks.isEnabled() and
            first_partition_found and
            journal_partition_id < std.math.maxInt(u16))
        {
            const replayed_tail = try self.replayJournalFromDefaultPathRangeColdOwnedAfterSequence(
                persistence_root,
                initial_state.snapshot_id,
                journal_partition_id + 1,
                snapshot_frontier,
            );
            outcome.journal_replayed_commands = std.math.add(
                usize,
                outcome.journal_replayed_commands,
                replayed_tail,
            ) catch return error.JournalReplayCountOverflow;
        }

        try self.validateDurableJournalFrontier();
        self.finishSettlementRecovery() catch return error.CorePoisoned;
        self.initial_state_bootstrapped = true;
        try self.startup();
        return outcome;
    }

    pub fn startupFromDiscoveredConfiguredPersistence(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_module_type: PersistenceModuleType,
        snapshot_instance_id: u32,
        journal_partition_id: u16,
    ) StartupFromDiscoveredConfiguredPersistenceError!BootstrapOutcome {
        try self.beginBootstrap();
        defer self.finishBootstrap();
        _ = snapshot_module_type;
        _ = snapshot_instance_id;
        const persistence_root = self.persistence_root orelse dir;
        const inventory = sharded_snapshot_store.discoverCommittedSnapshotInventory(
            self.allocator,
            persistence_root,
            self.config.serialization.storage_folder,
            self.config.initial_state.exchange_id,
            .{},
        ) catch |err| return mapShardedRestoreError(err);
        const discovered_snapshot_id = try selectDiscoveredXcm2(inventory);
        if (discovered_snapshot_id) |snapshot_id| {
            return self.startupFromConfiguredPersistenceWithSnapshotId(
                persistence_root,
                .matching_engine_router,
                0,
                journal_partition_id,
                snapshot_id,
            );
        }

        const initial_state = self.config.initial_state;
        if (initial_state.fromSnapshot() and initial_state.throw_if_snapshot_not_found) {
            return error.SnapshotFileNotFound;
        }
        const outcome: BootstrapOutcome = .{
            .snapshot_requested = initial_state.fromSnapshot(),
            .snapshot_found = false,
            .journal_replayed_commands = 0,
        };
        try self.validateDurableJournalFrontier();
        self.finishSettlementRecovery() catch return error.CorePoisoned;
        self.initial_state_bootstrapped = true;
        try self.startup();
        return outcome;
    }

    pub fn startupFromDiscoveredConfiguredPersistenceStrictMainLog(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_module_type: PersistenceModuleType,
        snapshot_instance_id: u32,
        journal_partition_id: u16,
    ) StartupFromDiscoveredConfiguredPersistenceStrictError!BootstrapOutcome {
        try self.beginBootstrap();
        defer self.finishBootstrap();
        const persistence_root = self.persistence_root orelse dir;
        const inventory = sharded_snapshot_store.discoverCommittedSnapshotInventory(
            self.allocator,
            persistence_root,
            self.config.serialization.storage_folder,
            self.config.initial_state.exchange_id,
            .{},
        ) catch |err| return mapShardedRestoreError(err);
        const discovered_snapshot_id = try selectDiscoveredXcm2(inventory);
        if (discovered_snapshot_id) |snapshot_id| {
            return self.startupFromConfiguredPersistenceWithSnapshotIdStrictMainLog(
                persistence_root,
                snapshot_module_type,
                snapshot_instance_id,
                journal_partition_id,
                snapshot_id,
            );
        }

        const initial_state = self.config.initial_state;
        if (initial_state.fromSnapshot() and initial_state.throw_if_snapshot_not_found) {
            return error.SnapshotFileNotFound;
        }
        const outcome: BootstrapOutcome = .{
            .snapshot_requested = initial_state.fromSnapshot(),
            .snapshot_found = false,
            .journal_replayed_commands = 0,
        };
        try self.validateDurableJournalFrontier();
        self.finishSettlementRecovery() catch return error.CorePoisoned;
        self.initial_state_bootstrapped = true;
        try self.startup();
        return outcome;
    }

    pub fn writeJournalToFile(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        path: []const u8,
    ) JournalApiError!void {
        const cold_claimed = try self.waitForAcceptedPrefixAndLockBatchFence();
        defer if (cold_claimed) self.finishColdDirectCall();
        defer self.batch_barrier_fence.unlock(self.driver.io);
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        try self.settlePersistentGroupedOwned();
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
        try self.ensureBlockingCallAllowed();
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
        if (self.journal == null) return error.JournalingDisabled;
        try self.beginBootstrap();
        defer self.finishBootstrap();
        return self.replayJournalFromFileColdOwned(dir, path);
    }

    fn replayJournalFromFileColdOwned(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        path: []const u8,
    ) JournalReplayError!usize {
        return self.replayJournalFromFileColdOwnedAfterSequence(
            dir,
            path,
            self.config.initial_state.snapshot_base_seq,
        );
    }

    fn replayJournalFromFileColdOwnedAfterSequence(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        path: []const u8,
        sequence_exclusive: u64,
    ) JournalReplayError!usize {
        _ = sequence_exclusive;
        if (self.journal) |*journal| {
            var loaded = try journal_mod.CommandJournal.readFromFile(self.allocator, dir, path);
            errdefer loaded.deinit();

            if (self.settlement_hooks.isEnabled()) {
                const replay_fingerprint = loaded.durablePrefixFingerprint() orelse
                    return error.JournalEntryMismatch;
                const appender = &(self.durable_journal orelse unreachable);
                const native_fingerprint = try appender.fingerprint(self.allocator);
                if (!replay_fingerprint.eql(native_fingerprint)) {
                    return error.JournalEntryMismatch;
                }
            }

            // The restored sharded kernel is authoritative. This matters when
            // an XCM2 manifest is newer than the configured snapshot base and
            // when a journal range advances across multiple partitions.
            const restored_frontier = self.core.kernels.next_seq;
            self.driver.publication_sequencer.restore(restored_frontier);

            const replay_limit = self.config.initial_state.journal_timestamp_ns;
            const replay_limit_i64: i64 = if (replay_limit >= @as(u64, @intCast(std.math.maxInt(i64))))
                std.math.maxInt(i64)
            else
                @as(i64, @intCast(replay_limit));
            const current_snapshot = self.persistenceCurrentSnapshot();
            const restored_snapshot: ?journal_mod.RestoredSnapshotFrontier = if (current_snapshot.seq == 0)
                null
            else
                .{
                    .snapshot_id = current_snapshot.snapshot_id,
                    .sequence = current_snapshot.seq,
                };
            const replay_frontier = try loaded.replayFrontier(
                restored_frontier,
                replay_limit,
                restored_snapshot,
            );
            var replayed: usize = 0;
            var replay_mutation_started = false;
            var replay_failure_sequence = publication.PublicationSequence.fromInt(0);
            var replay_failure_command = makeNopCommand();
            // Replaying directly into the restored kernels is intentionally
            // destructive. Once the publication frontier has advanced, any
            // later validation or execution error makes this instance unsafe
            // to retry: a retry would otherwise skip the already-applied
            // command and could bypass its recorded-result integrity check.
            errdefer |failure| if (replay_mutation_started) {
                self.poisonDirectExecution(
                    replay_failure_sequence,
                    &replay_failure_command,
                    failure,
                );
            };

            for (loaded.entries.items) |entry| {
                if (entry.sequence <= restored_frontier) continue;
                if (entry.sequence > replay_frontier) break;
                std.debug.assert(entry.input.timestamp <= replay_limit_i64);

                const assigned_sequence = publication.PublicationSequence.fromInt(entry.sequence);
                replay_failure_sequence = assigned_sequence;
                replay_failure_command = entry.input;
                if (entry.sequence <= self.driver.publication_sequencer.highWater()) {
                    return error.SequenceMismatch;
                }
                try self.driver.publication_sequencer.observe(assigned_sequence);
                replay_mutation_started = true;
                var replay_command = entry.input;
                var replay_response = self.executeWithModeNoJournalAtSequence(
                    entry.mode,
                    assigned_sequence,
                    &replay_command,
                    .replay,
                ) catch |failure| {
                    self.poisonDirectExecution(
                        replay_failure_sequence,
                        &replay_failure_command,
                        failure,
                    );
                    replay_mutation_started = false;
                    return switch (failure) {
                        error.SettlementPreparationRejected,
                        error.SettlementRiskR2MarkRejected,
                        error.SettlementJournalMarkRejected,
                        error.SettlementRecoveryRejected,
                        => error.CorePoisoned,
                        else => @errorCast(failure),
                    };
                };
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

            if (replay_frontier > self.core.kernels.next_seq) {
                replay_failure_sequence = publication.PublicationSequence.fromInt(replay_frontier);
                replay_failure_command = makeNopCommand();
                replay_mutation_started = true;
                self.core.restorePublicationSequence(replay_frontier);
                self.driver.publication_sequencer.restore(replay_frontier);
            }
            loaded.retainSequenceRange(restored_frontier, replay_frontier);

            journal.deinit();
            journal.* = loaded;
            return replayed;
        }
        return error.JournalingDisabled;
    }

    /// Configured recovery normally follows Java-numbered partitions. Enabled
    /// settlement instead replays the exact native XCJ3 file owned by the
    /// appender; its content fingerprint is checked before mutation below.
    fn replayNativeSettlementJournalColdOwnedAfterSequence(
        self: *ExchangeApiSharded,
        sequence_exclusive: u64,
    ) DefaultJournalReplayError!usize {
        std.debug.assert(self.settlement_hooks.isEnabled());
        const persistence_root = self.persistence_root orelse unreachable;
        const native_path = try nativeJournalPartitionPath(self.allocator, &self.config);
        defer self.allocator.free(native_path);
        return self.replayJournalFromFileColdOwnedAfterSequence(
            persistence_root,
            native_path,
            sequence_exclusive,
        );
    }

    pub fn captureSnapshotToFile(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        path: []const u8,
        symbols: []const i32,
        depth: usize,
    ) SnapshotWriteError!void {
        const cold_claimed = try self.waitForAcceptedPrefixAndLockBatchFence();
        defer if (cold_claimed) self.finishColdDirectCall();
        defer self.batch_barrier_fence.unlock(self.driver.io);
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        try self.settlePersistentGroupedOwned();
        if (self.custom_report_registry.len() != 0) {
            return error.LegacySnapshotCustomReportRegistryUnsupported;
        }
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
        const cold_claimed = try self.waitForAcceptedPrefixAndLockBatchFence();
        defer if (cold_claimed) self.finishColdDirectCall();
        defer self.batch_barrier_fence.unlock(self.driver.io);
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        try self.settlePersistentGroupedOwned();
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
        try self.ensureBlockingCallAllowed();
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
        try self.ensureBlockingCallAllowed();
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
        try self.ensureBlockingCallAllowed();
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
        if (self.journal == null) return error.JournalingDisabled;
        try self.beginBootstrap();
        defer self.finishBootstrap();
        return self.replayJournalFromDefaultPathRangeColdOwned(
            dir,
            snapshot_id,
            partition_id,
        );
    }

    fn replayJournalFromDefaultPathColdOwned(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_id: u64,
        partition_id: u16,
    ) DefaultJournalReplayError!usize {
        return self.replayJournalFromDefaultPathColdOwnedAfterSequence(
            dir,
            snapshot_id,
            partition_id,
            self.config.initial_state.snapshot_base_seq,
        );
    }

    fn replayJournalFromDefaultPathColdOwnedAfterSequence(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_id: u64,
        partition_id: u16,
        sequence_exclusive: u64,
    ) DefaultJournalReplayError!usize {
        const path = try self.defaultPersistenceJournalPath(
            self.allocator,
            snapshot_id,
            partition_id,
        );
        defer self.allocator.free(path);
        return self.replayJournalFromFileColdOwnedAfterSequence(
            dir,
            path,
            sequence_exclusive,
        );
    }

    fn replayJournalFromDefaultPathRangeColdOwned(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_id: u64,
        partition_id: u16,
    ) DefaultJournalReplayRangeError!usize {
        return self.replayJournalFromDefaultPathRangeColdOwnedAfterSequence(
            dir,
            snapshot_id,
            partition_id,
            self.config.initial_state.snapshot_base_seq,
        );
    }

    fn replayJournalFromDefaultPathRangeColdOwnedAfterSequence(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_id: u64,
        partition_id: u16,
        sequence_exclusive: u64,
    ) DefaultJournalReplayRangeError!usize {
        var total_replayed: usize = 0;
        var current_partition = partition_id;
        while (true) {
            const replayed = self.replayJournalFromDefaultPathColdOwnedAfterSequence(
                dir,
                snapshot_id,
                current_partition,
                sequence_exclusive,
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
        try self.ensureBlockingCallAllowed();
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
            self.driver.publication_sequencer.highWater(),
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
        try self.ensureBlockingCallAllowed();
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
        self.assertWiring();
        try self.ensureBlockingCallAllowed();
        // Prevent one root from receiving the committed bundle while another
        // receives the advertisement that makes it discoverable to operators.
        if (!try self.isConfiguredPersistenceRoot(dir)) {
            return error.PersistenceRootMismatch;
        }
        return self.persistStateWithConfiguredMainLog(
            snapshot_id,
            timestamp_ns,
            module_type,
            instance_id,
        );
    }

    /// Persists the XCM2 bundle and appends the Java-compatible main-log entry
    /// through the same independently owned root. The manifest and in-memory
    /// descriptor both exist before the main log can advertise the snapshot.
    pub fn persistStateWithConfiguredMainLog(
        self: *ExchangeApiSharded,
        snapshot_id: i64,
        timestamp_ns: i64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) PersistStateWithMainLogError!cmd.CommandResultCode {
        self.assertWiring();
        try self.ensureBlockingCallAllowed();
        if (self.config.serialization.backend == .custom) {
            return error.ConfiguredMainLogRequiresDiskBackend;
        }
        const batch = try self.persistStateBatch(.routed, snapshot_id, timestamp_ns);
        if (batch.result_code != .success) return error.PersistStateRejected;
        const persistence_root = self.persistence_root orelse
            return error.ConfiguredMainLogRequiresDiskBackend;

        const snapshot_id_u64 = std.math.cast(u64, snapshot_id) orelse return error.PersistStateSnapshotIdOverflow;
        const timestamp_ns_u64 = std.math.cast(u64, timestamp_ns) orelse return error.PersistStateTimestampOverflow;
        const sequence_from = batch.sequence_from orelse return error.MissingPersistSequence;
        const sequence_to = batch.sequence_to orelse return error.MissingPersistSequence;

        const cold_claimed = try self.waitForAcceptedPrefixAndLockBatchFence();
        defer if (cold_claimed) self.finishColdDirectCall();
        defer self.batch_barrier_fence.unlock(self.driver.io);
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);

        const snapshot_ref = self.descriptors.findSnapshotById(snapshot_id_u64) orelse
            return error.PersistStateSnapshotMismatch;
        const persisted_snapshot = self.descriptors.snapshotConst(snapshot_ref);
        if (persisted_snapshot.seq != sequence_to or
            persisted_snapshot.timestamp_ns != timestamp_ns_u64)
        {
            return error.PersistStateSnapshotMismatch;
        }

        try self.appendDefaultPersistenceMainLogEntry(
            persistence_root,
            switch (module_type) {
                .matching_engine_router => sequence_from,
                .risk_engine => sequence_to,
            },
            persisted_snapshot.timestamp_ns,
            persisted_snapshot.snapshot_id,
            module_type,
            instance_id,
        );
        return batch.result_code;
    }

    fn isConfiguredPersistenceRoot(
        self: *const ExchangeApiSharded,
        supplied_root: std.Io.Dir,
    ) std.Io.Dir.RealPathFileError!bool {
        const owned_root = self.persistence_root orelse return true;
        var owned_path_buf: [std.Io.Dir.max_path_bytes]u8 = undefined;
        var supplied_path_buf: [std.Io.Dir.max_path_bytes]u8 = undefined;
        const owned_path_len = try owned_root.realPathFile(self.driver.io, ".", &owned_path_buf);
        const supplied_path_len = try supplied_root.realPathFile(self.driver.io, ".", &supplied_path_buf);
        return std.mem.eql(
            u8,
            owned_path_buf[0..owned_path_len],
            supplied_path_buf[0..supplied_path_len],
        );
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
    ) (risk.SymbolSpecificationProvider.AddSymbolError || int_map_order.LayoutError)!void {
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

    /// Java-compatible convenience publication: the global publication
    /// sequence is also the generated order ID, while the internal request ID
    /// remains a separate callback-completion handle.
    pub fn placeNewOrderGeneratedIdWithCallback(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        user_cookie: i32,
        price: i64,
        reserve_bid_price: i64,
        size: i64,
        action: common.OrderAction,
        order_type: common.OrderType,
        symbol: i32,
        uid: i64,
        callback: CommandResponseCallback,
    ) SubmitCommandWithCallbackError!u64 {
        self.assertWiring();
        const driver = self.driver;
        try self.lockPublicationDriver();
        defer driver.mutex.unlock(driver.io);

        try self.waitForColdPublisherLocked();
        try self.waitForCapacityLocked(1, 0);
        const request_id = std.math.add(u64, self.next_request_id, 1) catch
            return error.RequestIdOverflow;
        try self.ensureJournalCapacityLocked(1);
        const sequences = try driver.publication_sequencer.prepare(1);
        const sequence = sequences.first.toInt();
        const order_id = std.math.cast(i64, sequence) orelse
            return error.GeneratedOrderIdOverflow;
        const timestamp = std.math.cast(i64, io_context.realTimestampMs()) orelse
            return error.CallbackTimestampOverflow;
        const acceptance_id = std.math.add(u64, driver.accepted_total, 1) catch
            return error.RequestIdOverflow;

        var reservation = driver.queue.reserve(1) catch unreachable;
        reservation.at(0).* = .{
            .acceptance_id = acceptance_id,
            .sequence = sequences.first,
            .mode = mode,
            .command = makePlaceOrderCommand(
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
            .completion = .{ .callback = .{
                .request_id = request_id,
                .callback = callback,
            } },
            .kind = .asynchronous,
        };
        reservation.at(0).command.timestamp = timestamp;

        driver.publication_sequencer.commit(sequences);
        driver.queue.commit(reservation);
        if (self.journal != null) driver.journal_slots_reserved += 1;
        self.next_request_id = request_id;
        driver.callback_completions_pending += 1;
        driver.accepted_total = acceptance_id;
        driver.async_accepted_total += 1;
        driver.work_available.broadcast(driver.io);
        return sequence;
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
        switch (command_type) {
            .binary_data_command, .binary_data_query => {},
            else => return error.InvalidBinaryCommandType,
        }
        const carrier_words = try self.rawPayloadCarrierWords(payload);
        defer self.allocator.free(carrier_words);
        return self.submitBinaryCarrierWords(
            mode,
            command_type,
            transfer_id,
            timestamp,
            carrier_words,
        );
    }

    pub fn submitBinaryPayloadAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command_type: cmd.OrderCommandType,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
    ) BinaryPayloadAsyncSubmitError!u64 {
        switch (command_type) {
            .binary_data_command, .binary_data_query => {},
            else => return error.InvalidBinaryCommandType,
        }
        const carrier_words = try self.rawPayloadCarrierWords(payload);
        defer self.allocator.free(carrier_words);
        return self.submitBinaryCarrierWordsAsync(
            mode,
            command_type,
            transfer_id,
            timestamp,
            carrier_words,
        );
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
        const body = try binary_payload.encodeJavaAddSymbolsToBytes(self.allocator, symbols);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        return self.submitBinaryCarrierWords(
            mode,
            .binary_data_command,
            transfer_id,
            timestamp,
            carrier_words,
        );
    }

    pub fn submitAddSymbolsBinaryAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        symbols: []const common.CoreSymbolSpecification,
    ) BinaryPayloadTypedAsyncSubmitError!u64 {
        const body = try binary_payload.encodeJavaAddSymbolsToBytes(self.allocator, symbols);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        return self.submitBinaryCarrierWordsAsync(
            mode,
            .binary_data_command,
            transfer_id,
            timestamp,
            carrier_words,
        );
    }

    pub fn submitAddAccountsBinary(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        users: []const BinaryUserAccountsBatch,
    ) BinaryPayloadTypedSubmitError!kernel_mod.CommandResponse {
        const body = try binary_payload.encodeJavaAddAccountsToBytes(self.allocator, users);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        return self.submitBinaryCarrierWords(
            mode,
            .binary_data_command,
            transfer_id,
            timestamp,
            carrier_words,
        );
    }

    pub fn submitAddAccountsBinaryAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        users: []const BinaryUserAccountsBatch,
    ) BinaryPayloadTypedAsyncSubmitError!u64 {
        const body = try binary_payload.encodeJavaAddAccountsToBytes(self.allocator, users);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        return self.submitBinaryCarrierWordsAsync(
            mode,
            .binary_data_command,
            transfer_id,
            timestamp,
            carrier_words,
        );
    }

    pub fn submitBinaryReportQuery(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    ) BinaryReportQuerySubmitError!kernel_mod.CommandResponse {
        const body = try binary_payload.encodeJavaReportQueryToBytes(self.allocator, query);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        return self.submitBinaryCarrierWords(
            mode,
            .binary_data_query,
            transfer_id,
            timestamp,
            carrier_words,
        );
    }

    pub fn submitBinaryReportQueryAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    ) BinaryReportQueryAsyncSubmitError!u64 {
        const body = try binary_payload.encodeJavaReportQueryToBytes(self.allocator, query);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        return self.submitBinaryCarrierWordsAsync(
            mode,
            .binary_data_query,
            transfer_id,
            timestamp,
            carrier_words,
        );
    }

    /// Publishes one registered custom report through Java-compatible binary
    /// query carriers. The returned token owns no allocations, and its response
    /// is published on a dedicated completion lane that ordinary raw result
    /// APIs cannot consume.
    pub fn submitCustomReportAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        query: *const report_registry.QueryHandle,
    ) CustomReportAsyncSubmitError!custom_report_pipeline.PendingRequest {
        var staged = try self.custom_report_requests.stageRequest(query, .{});
        defer staged.deinit();
        const carrier_words = staged.carrierWords() catch unreachable;
        const frame_commands = try self.binaryFrameCommandsFromCarrierWords(
            .binary_data_query,
            transfer_id,
            timestamp,
            carrier_words,
        );
        defer self.allocator.free(frame_commands);
        var pending: ?custom_report_pipeline.PendingRequest = null;

        _ = try self.enqueueBinaryFrames(
            frame_commands,
            mode,
            .{ .custom_report = .{
                .staged = &staged,
                .pending_out = &pending,
            } },
        );
        return pending.?;
    }

    /// Executes the exact Java report query and also opts the terminal carrier
    /// into the process-local Zig-native projection. The Java carrier words
    /// remain identical to an unmarked query.
    pub fn submitNativeReportQuery(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    ) BinaryReportQuerySubmitError!kernel_mod.CommandResponse {
        const body = try binary_payload.encodeJavaReportQueryToBytes(self.allocator, query);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        return self.submitBinaryCarrierWordsWithProjection(
            mode,
            .binary_data_query,
            transfer_id,
            timestamp,
            carrier_words,
            .exact_java_and_native,
        );
    }

    pub fn submitNativeReportQueryAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    ) BinaryReportQueryAsyncSubmitError!u64 {
        const body = try binary_payload.encodeJavaReportQueryToBytes(self.allocator, query);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        return self.submitBinaryCarrierWordsAsyncWithProjection(
            mode,
            .binary_data_query,
            transfer_id,
            timestamp,
            carrier_words,
            .exact_java_and_native,
        );
    }

    fn rawPayloadCarrierWords(
        self: *ExchangeApiSharded,
        payload: []const u8,
    ) (std.mem.Allocator.Error || error{ EmptyBinaryPayload, BinaryPayloadSizeOverflow })![]i64 {
        const frame_layout = try computeBinaryFrameLayout(payload.len);
        const word_count = std.math.mul(
            usize,
            frame_layout.frame_count,
            binary_frames.FrameWordsPerCommand,
        ) catch return error.BinaryPayloadSizeOverflow;
        const carrier_words = try self.allocator.alloc(i64, word_count);
        @memset(carrier_words, 0);
        for (0..frame_layout.words_without_padding) |word_index| {
            carrier_words[word_index] = readBigEndianWordPadded(payload, word_index);
        }
        return carrier_words;
    }

    fn binaryFrameCommandsFromCarrierWords(
        self: *ExchangeApiSharded,
        command_type: cmd.OrderCommandType,
        transfer_id: i32,
        timestamp: i64,
        carrier_words: []const i64,
    ) std.mem.Allocator.Error![]cmd.OrderCommand {
        return self.binaryFrameCommandsFromCarrierWordsWithProjection(
            command_type,
            transfer_id,
            timestamp,
            carrier_words,
            .exact_java_only,
        );
    }

    fn binaryFrameCommandsFromCarrierWordsWithProjection(
        self: *ExchangeApiSharded,
        command_type: cmd.OrderCommandType,
        transfer_id: i32,
        timestamp: i64,
        carrier_words: []const i64,
        projection: cmd.BinaryReportProjection,
    ) std.mem.Allocator.Error![]cmd.OrderCommand {
        comptime std.debug.assert(binary_frames.FrameWordsPerCommand == java_binary_wire.words_per_frame);
        std.debug.assert(carrier_words.len > 0);
        std.debug.assert(carrier_words.len % binary_frames.FrameWordsPerCommand == 0);
        const frame_count = carrier_words.len / binary_frames.FrameWordsPerCommand;
        const frame_commands = try self.allocator.alloc(cmd.OrderCommand, frame_count);
        for (frame_commands, 0..) |*frame_command, frame_index| {
            var frame_payload: [binary_frames.FrameWordsPerCommand]i64 = undefined;
            const word_start = frame_index * binary_frames.FrameWordsPerCommand;
            @memcpy(
                frame_payload[0..],
                carrier_words[word_start .. word_start + binary_frames.FrameWordsPerCommand],
            );
            frame_command.* = makeBinaryFrameCommand(
                command_type,
                transfer_id,
                if (frame_index + 1 == frame_count) -1 else 0,
                timestamp,
                frame_payload,
            );
            if (frame_index + 1 == frame_count) {
                frame_command.binary_report_projection = projection;
            }
        }
        return frame_commands;
    }

    fn submitBinaryCarrierWords(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command_type: cmd.OrderCommandType,
        transfer_id: i32,
        timestamp: i64,
        carrier_words: []const i64,
    ) BinaryPayloadSubmitError!kernel_mod.CommandResponse {
        return self.submitBinaryCarrierWordsWithProjection(
            mode,
            command_type,
            transfer_id,
            timestamp,
            carrier_words,
            .exact_java_only,
        );
    }

    fn submitBinaryCarrierWordsWithProjection(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command_type: cmd.OrderCommandType,
        transfer_id: i32,
        timestamp: i64,
        carrier_words: []const i64,
        projection: cmd.BinaryReportProjection,
    ) BinaryPayloadSubmitError!kernel_mod.CommandResponse {
        try self.ensureBlockingCallAllowed();
        const frame_commands = try self.binaryFrameCommandsFromCarrierWordsWithProjection(
            command_type,
            transfer_id,
            timestamp,
            carrier_words,
            projection,
        );
        defer self.allocator.free(frame_commands);

        if (try self.shouldUseWorkerForSynchronousCall()) {
            const request_id = (try self.enqueueBinaryFrames(
                frame_commands,
                mode,
                .synchronous,
            )).?;
            return self.waitTakeResponse(request_id);
        }
        defer self.finishColdDirectCall();

        const sequences = try self.prepareColdPublicationRange(frame_commands.len);
        self.stampDirectLiveBatch(frame_commands);
        var last_response: ?kernel_mod.CommandResponse = null;
        errdefer if (last_response) |*response| response.deinit(self.allocator);
        for (frame_commands, 0..) |*frame_command, index| {
            const response = try self.executePreparedDirect(
                mode,
                sequences.at(index),
                frame_command,
                if (index + 1 == frame_commands.len) .terminal else .continues,
            );
            if (last_response) |*previous| previous.deinit(self.allocator);
            last_response = response;
        }
        return last_response.?;
    }

    fn submitBinaryCarrierWordsAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command_type: cmd.OrderCommandType,
        transfer_id: i32,
        timestamp: i64,
        carrier_words: []const i64,
    ) BinaryPayloadAsyncSubmitError!u64 {
        return self.submitBinaryCarrierWordsAsyncWithProjection(
            mode,
            command_type,
            transfer_id,
            timestamp,
            carrier_words,
            .exact_java_only,
        );
    }

    fn submitBinaryCarrierWordsAsyncWithProjection(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command_type: cmd.OrderCommandType,
        transfer_id: i32,
        timestamp: i64,
        carrier_words: []const i64,
        projection: cmd.BinaryReportProjection,
    ) BinaryPayloadAsyncSubmitError!u64 {
        const frame_commands = try self.binaryFrameCommandsFromCarrierWordsWithProjection(
            command_type,
            transfer_id,
            timestamp,
            carrier_words,
            projection,
        );
        defer self.allocator.free(frame_commands);
        return (try self.enqueueBinaryFrames(
            frame_commands,
            mode,
            .asynchronous,
        )).?;
    }

    fn submitBinaryCarrierWordsDetached(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command_type: cmd.OrderCommandType,
        transfer_id: i32,
        timestamp: i64,
        carrier_words: []const i64,
    ) SubmitApiCommandError!void {
        const frame_commands = try self.binaryFrameCommandsFromCarrierWords(
            command_type,
            transfer_id,
            timestamp,
            carrier_words,
        );
        defer self.allocator.free(frame_commands);
        _ = try self.enqueueBinaryFrames(frame_commands, mode, .detached);
    }

    fn submitAddSymbolsBinaryDetached(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        symbols: []const common.CoreSymbolSpecification,
    ) SubmitApiCommandError!void {
        const body = try binary_payload.encodeJavaAddSymbolsToBytes(self.allocator, symbols);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        try self.submitBinaryCarrierWordsDetached(
            mode,
            .binary_data_command,
            transfer_id,
            timestamp,
            carrier_words,
        );
    }

    fn submitAddAccountsBinaryDetached(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        users: []const BinaryUserAccountsBatch,
    ) SubmitApiCommandError!void {
        const body = try binary_payload.encodeJavaAddAccountsToBytes(self.allocator, users);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        try self.submitBinaryCarrierWordsDetached(
            mode,
            .binary_data_command,
            transfer_id,
            timestamp,
            carrier_words,
        );
    }

    fn submitBinaryReportQueryDetached(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    ) SubmitApiCommandError!void {
        const body = try binary_payload.encodeJavaReportQueryToBytes(self.allocator, query);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        try self.submitBinaryCarrierWordsDetached(
            mode,
            .binary_data_query,
            transfer_id,
            timestamp,
            carrier_words,
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
        return self.submitNativeReportQueryAsync(mode, transfer_id, timestamp, .state_hash);
    }

    pub fn requestSingleUserReportAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        uid: i64,
    ) BinaryReportQueryAsyncSubmitError!u64 {
        return self.submitNativeReportQueryAsync(
            mode,
            transfer_id,
            timestamp,
            .{ .single_user = uid },
        );
    }

    pub fn requestTotalCurrencyBalanceAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
    ) BinaryReportQueryAsyncSubmitError!u64 {
        return self.submitNativeReportQueryAsync(
            mode,
            transfer_id,
            timestamp,
            .total_currency_balance,
        );
    }

    pub fn requestJavaStateHashAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
    ) BinaryReportQueryAsyncSubmitError!u64 {
        return self.submitBinaryReportQueryAsync(mode, transfer_id, timestamp, .state_hash);
    }

    pub fn requestJavaSingleUserReportAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        uid: i64,
    ) BinaryReportQueryAsyncSubmitError!u64 {
        return self.submitBinaryReportQueryAsync(
            mode,
            transfer_id,
            timestamp,
            .{ .single_user = uid },
        );
    }

    pub fn requestJavaTotalCurrencyBalanceAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
    ) BinaryReportQueryAsyncSubmitError!u64 {
        return self.submitBinaryReportQueryAsync(
            mode,
            transfer_id,
            timestamp,
            .total_currency_balance,
        );
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
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
        if (!api_command_mod.supportsAsyncFullResponse(command)) {
            return error.UnsupportedAsyncFullResponseCommand;
        }
        return self.submitApiCommandAsync(mode, command);
    }

    /// Returns detached command-response ownership. While the API is live,
    /// release it with `releaseResponse`.
    pub fn processCommandFullResponse(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command: ApiCommand,
    ) ProcessCommandFullResponseError!kernel_mod.CommandResponse {
        try self.ensureBlockingCallAllowed();
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
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
        return self.submitCommandAsyncFullResponse(mode, .{ .add_user = user_id });
    }

    pub fn submitSuspendUserAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        user_id: i64,
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
        return self.submitCommandAsyncFullResponse(mode, .{ .suspend_user = user_id });
    }

    pub fn submitResumeUserAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        user_id: i64,
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
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
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
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
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
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
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
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
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
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
    ) SubmitCommandAsyncFullResponseError!ApiCommandAsyncHandle {
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
        if (try api_command_mod.toOrdinaryOrderCommand(api_command)) |translated| {
            var command = translated;
            return .{ .response = try self.executeWithMode(mode, &command) };
        }

        return switch (api_command) {
            .raw_command => |command| blk: {
                var mutable = command;
                break :blk .{ .response = try self.executeRawApiCommandWithMode(mode, &mutable) };
            },
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
            .reset,
            .reset_with_timestamp,
            .nop,
            .nop_with_timestamp,
            => unreachable,
        };
    }

    pub fn submitApiCommand(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        api_command: ApiCommand,
    ) SubmitApiCommandError!void {
        try self.submitApiCommandDetached(mode, api_command);
    }

    /// Explicit settlement-safe counterpart to Java's detached
    /// `submitCommand`. The returned result is owned by the caller.
    pub fn submitCommandSync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        api_command: ApiCommand,
    ) SubmitApiCommandError!ApiCommandResult {
        return self.submitApiCommandResult(mode, api_command);
    }

    pub fn submitApiCommandSync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        api_command: ApiCommand,
    ) SubmitApiCommandError!ApiCommandResult {
        return self.submitCommandSync(mode, api_command);
    }

    /// Publishes every command slot atomically without owning a poll result.
    /// Binary carrier frames and the persistence pair stay contiguous.
    pub fn submitApiCommandDetached(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        api_command: ApiCommand,
    ) SubmitApiCommandError!void {
        if (try api_command_mod.toOrdinaryOrderCommand(api_command)) |command| {
            return self.enqueueDetachedBatch(&.{command}, mode, .detached_api);
        }

        return switch (api_command) {
            .raw_command => |command| self.enqueueDetachedBatch(
                &.{command},
                mode,
                .detached_api_raw,
            ),
            .persist_state => |payload| self.enqueueDetachedBatch(&.{
                makePersistStateMatchingCommand(payload.snapshot_id, payload.timestamp_ns),
                makePersistStateRiskCommand(payload.snapshot_id, payload.timestamp_ns),
            }, mode, .detached_api),
            .grouping_control => |payload| self.enqueueDetachedBatch(
                &.{makeGroupingControlCommand(payload.timestamp_ns, payload.mode)},
                mode,
                .detached_api,
            ),
            .binary_add_symbols => |payload| self.submitAddSymbolsBinaryDetached(
                mode,
                payload.transfer_id,
                payload.timestamp,
                payload.symbols,
            ),
            .binary_add_accounts => |payload| self.submitAddAccountsBinaryDetached(
                mode,
                payload.transfer_id,
                payload.timestamp,
                payload.users,
            ),
            .binary_report_query => |payload| self.submitBinaryReportQueryDetached(
                mode,
                payload.transfer_id,
                payload.timestamp,
                payload.query,
            ),
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
            .reset,
            .reset_with_timestamp,
            .nop,
            .nop_with_timestamp,
            => unreachable,
        };
    }

    fn submitApiCommandDetachedForBatch(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        api_command: ApiCommand,
    ) SubmitApiCommandBatchError!void {
        self.submitApiCommandDetached(mode, api_command) catch |err| switch (err) {
            error.CoreQueueFull => {
                _ = try self.drainPending();
                try self.submitApiCommandDetached(mode, api_command);
            },
            else => return err,
        };
    }

    fn submitApiCommandAsyncForBatch(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        api_command: ApiCommand,
    ) SubmitApiCommandBatchError!ApiCommandAsyncHandle {
        return self.submitApiCommandAsync(mode, api_command) catch |err| switch (err) {
            error.CoreQueueFull => blk: {
                _ = try self.drainPending();
                break :blk try self.submitApiCommandAsync(mode, api_command);
            },
            else => return err,
        };
    }

    pub fn submitApiCommandAsync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        api_command: ApiCommand,
    ) SubmitApiCommandAsyncError!ApiCommandAsyncHandle {
        if (try api_command_mod.toOrdinaryOrderCommand(api_command)) |command| {
            return .{ .request_id = try self.enqueuePoll(command, mode, .asynchronous) };
        }

        return switch (api_command) {
            .raw_command => |command| .{ .request_id = try self.enqueuePoll(
                command,
                mode,
                .asynchronous_raw,
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
            .reset,
            .reset_with_timestamp,
            .nop,
            .nop_with_timestamp,
            => unreachable,
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

    /// Takes detached async-result ownership. While the API is live, release
    /// it with `releaseApiCommandResult`.
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
        const request_id = try requestIdFromHandle(handle);
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        if (driver.lifecycle.fatalRecord()) |fatal_ptr| {
            const fatal = fatal_ptr.*;
            const ready = self.ready_results.get(request_id);
            const affected_ready = if (ready) |response|
                if (fatal.sequence) |fatal_sequence|
                    response.sequence >= fatal_sequence
                else
                    true
            else
                false;
            // A result strictly before the fatal frontier remains processable.
            // An affected ready result, or a handle with no ready result, is
            // terminally failed; late registration cannot recreate an orphan
            // or publish speculative matching success after failed R2.
            if (ready == null or affected_ready) {
                if (callback.failure_callback != null and
                    runtime_mod.isCallbackExecutionActive() and
                    !self.isCallbackThread())
                {
                    driver.mutex.unlock(driver.io);
                    return error.CoreReentrantCall;
                }
                const removed_response = if (affected_ready) blk: {
                    const removed = self.ready_results.fetchRemove(request_id) orelse
                        unreachable;
                    std.debug.assert(driver.completion_slots_used > 0);
                    driver.completion_slots_used -= 1;
                    driver.space_available.broadcast(driver.io);
                    break :blk removed.value;
                } else null;
                driver.callback_completions_pending += 1;
                driver.mutex.unlock(driver.io);
                var failed_sequence: ?u64 = null;
                if (removed_response) |value| {
                    var response = value;
                    failed_sequence = response.sequence;
                    response.deinit(self.allocator);
                }
                self.dispatchCommandFailure(.{
                    .request_id = request_id,
                    .callback = callback,
                }, .{
                    .request_id = request_id,
                    .kind = if (fatal.request_id == request_id or
                        (fatal.sequence != null and
                            fatal.sequence == failed_sequence))
                        .command_failed
                    else
                        .skipped_after_fatal,
                    .fatal = fatal,
                });
                driver.mutex.lockUncancelable(driver.io);
                std.debug.assert(driver.callback_completions_pending > 0);
                driver.callback_completions_pending -= 1;
                driver.completion_available.broadcast(driver.io);
                driver.mutex.unlock(driver.io);
                return;
            }
        }
        self.registerCallbackForHandle(handle, callback) catch |err| {
            driver.mutex.unlock(driver.io);
            return err;
        };
        driver.mutex.unlock(driver.io);
    }

    pub fn pendingCallbackCount(self: *const ExchangeApiSharded) usize {
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        return self.callback_registrations_len + driver.callback_completions_pending;
    }

    pub fn processResult(
        self: *ExchangeApiSharded,
        handle: ApiCommandAsyncHandle,
        callback: CommandResponseCallback,
    ) ProcessResultError!bool {
        if (runtime_mod.isCallbackExecutionActive() and !self.isCallbackThread()) {
            return error.CoreReentrantCall;
        }
        var result = self.takeApiCommandAsyncResult(handle) orelse return false;
        defer result.deinit(self.allocator);
        switch (result) {
            .response => |*response| self.dispatchCommandResponse(callback, response),
            .persist_state => return error.UnexpectedPersistStateResult,
        }
        return true;
    }

    pub fn processRegisteredCallback(
        self: *ExchangeApiSharded,
        handle: ApiCommandAsyncHandle,
    ) ProcessRegisteredCallbackError!bool {
        if (runtime_mod.isCallbackExecutionActive() and !self.isCallbackThread()) {
            return error.CoreReentrantCall;
        }
        const request_id = try requestIdFromHandle(handle);
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        const idx = self.callbackRegistrationIndex(request_id) orelse {
            driver.mutex.unlock(driver.io);
            return false;
        };
        const callback = self.callback_registrations[idx].callback;
        var response = self.takeReadyResultLocked(request_id) orelse {
            driver.mutex.unlock(driver.io);
            return false;
        };
        self.removeCallbackRegistrationAt(idx);
        driver.mutex.unlock(driver.io);
        defer response.deinit(self.allocator);
        self.dispatchCommandResponse(callback, &response);
        return true;
    }

    pub fn processPendingCallbacks(self: *ExchangeApiSharded) ProcessRegisteredCallbackError!usize {
        if (runtime_mod.isCallbackExecutionActive() and !self.isCallbackThread()) {
            return error.CoreReentrantCall;
        }
        var processed: usize = 0;
        var idx: usize = 0;
        const driver = self.driver;
        while (true) {
            driver.mutex.lockUncancelable(driver.io);
            if (idx >= self.callback_registrations_len) {
                driver.mutex.unlock(driver.io);
                break;
            }
            const registration = self.callback_registrations[idx];
            var response = self.takeReadyResultLocked(registration.request_id) orelse {
                idx += 1;
                driver.mutex.unlock(driver.io);
                continue;
            };
            self.removeCallbackRegistrationAt(idx);
            driver.mutex.unlock(driver.io);
            self.dispatchCommandResponse(registration.callback, &response);
            response.deinit(self.allocator);
            processed += 1;
        }
        return processed;
    }

    pub fn submitApiCommandsSyncResult(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        commands: []const ApiCommand,
    ) SubmitApiCommandBatchError!ApiCommandResult {
        if (commands.len == 0) return error.EmptyApiCommandBatch;
        try self.ensureBlockingCallAllowed();

        for (commands[0 .. commands.len - 1]) |api_command| {
            try self.submitApiCommandDetachedForBatch(mode, api_command);
        }

        const final_handle = try self.submitApiCommandAsyncForBatch(
            mode,
            commands[commands.len - 1],
        );
        // Zig deliberately resolves this barrier only after R2 settlement,
        // which is stronger than Java's earlier results-stage completion.
        _ = try self.drainPending();
        return self.takeApiCommandAsyncResult(final_handle) orelse
            error.MissingBatchResult;
    }

    pub fn submitApiCommandsSync(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        commands: []const ApiCommand,
    ) SubmitApiCommandBatchError!void {
        if (commands.len == 0) return;
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
        try self.ensureBlockingCallAllowed();
        for (commands) |api_command| {
            try self.submitApiCommandDetachedForBatch(mode, api_command);
        }

        const flush_handle = try self.submitApiCommandAsyncForBatch(mode, .nop);
        // NOP is the stream-equivalent FIFO barrier, including for an empty
        // input. Completion stays settlement-safe through R2.
        _ = try self.drainPending();
        var flush_result = self.takeApiCommandAsyncResult(flush_handle) orelse
            return error.MissingBatchResult;
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
        return (try self.persistStateBatch(mode, snapshot_id, timestamp_ns)).result_code;
    }

    fn persistStateBatch(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        snapshot_id: i64,
        timestamp_ns: i64,
    ) PersistStateError!BatchResponse {
        var matching = makePersistStateMatchingCommand(snapshot_id, timestamp_ns);
        var risk_command = makePersistStateRiskCommand(snapshot_id, timestamp_ns);
        var commands = [_]*cmd.OrderCommand{ &matching, &risk_command };
        return self.submitCommandBatch(mode, commands[0..]);
    }

    pub fn submitCommandBatch(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        commands: []*cmd.OrderCommand,
    ) ExecuteError!BatchResponse {
        if (commands.len == 0) return .{};
        if (try self.shouldUseWorkerForSynchronousCall()) {
            const copies = try self.allocator.alloc(cmd.OrderCommand, commands.len);
            defer self.allocator.free(copies);
            for (commands, 0..) |command, index| copies[index] = command.*;
            const request_ids = try self.allocator.alloc(u64, commands.len);
            defer self.allocator.free(request_ids);
            try self.enqueueResponseBatch(copies, mode, .synchronous, request_ids);

            var batch: BatchResponse = .{};
            for (commands, request_ids) |command, request_id| {
                var response = try self.waitTakeResponse(request_id);
                defer response.deinit(self.allocator);
                command.result_code = response.result_code;
                if (batch.sequence_from == null) batch.sequence_from = response.sequence;
                batch.sequence_to = response.sequence;
                batch.result_code = cmd.CommandResultCode.mergeToFirstFailed(&.{
                    batch.result_code,
                    response.result_code,
                });
            }
            return batch;
        }
        defer self.finishColdDirectCall();

        var direct_commands = try self.allocator.alloc(cmd.OrderCommand, commands.len);
        defer self.allocator.free(direct_commands);
        for (commands, 0..) |command, index| direct_commands[index] = command.*;
        const sequences = try self.prepareColdPublicationRange(commands.len);
        self.stampDirectLiveBatch(direct_commands);
        var batch: BatchResponse = .{};
        for (commands, direct_commands, 0..) |command, *direct_command, index| {
            var response = try self.executePreparedDirect(
                mode,
                sequences.at(index),
                direct_command,
                if (index + 1 == direct_commands.len) .terminal else .continues,
            );
            defer response.deinit(self.allocator);
            command.result_code = direct_command.result_code;

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
        return self.enqueuePoll(command.*, mode, .asynchronous);
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
        const commands = [_]cmd.OrderCommand{
            makePersistStateMatchingCommand(snapshot_id, timestamp_ns),
            makePersistStateRiskCommand(snapshot_id, timestamp_ns),
        };
        var request_ids: [2]u64 = undefined;
        try self.enqueueResponseBatch(commands[0..], mode, .asynchronous, request_ids[0..]);
        return .{
            .matching_request_id = request_ids[0],
            .risk_request_id = request_ids[1],
        };
    }

    /// Waits until the accepted prefix captured by this call is terminally
    /// settled. After poison, callback, detached, and poll owners in that
    /// prefix are settled before `CorePoisoned` is returned.
    pub fn drainPending(self: *ExchangeApiSharded) DrainPendingError!usize {
        self.assertWiring();
        if (runtime_mod.isCallbackExecutionActive() or self.isWorkerThread()) {
            return error.CoreReentrantCall;
        }

        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        if (driver.close_requested.load(.acquire) and
            driver.lifecycle.current() != .poisoned)
        {
            driver.mutex.unlock(driver.io);
            return error.CoreStopped;
        }
        switch (driver.lifecycle.current()) {
            .cold, .running, .poisoned => {},
            .closing, .stopped => {
                driver.mutex.unlock(driver.io);
                return error.CoreStopped;
            },
        }

        const settlement_target = driver.accepted_total;
        const target = driver.async_accepted_total;
        var drain_on_current_thread = false;
        while (true) {
            switch (driver.lifecycle.current()) {
                .cold => {
                    if (driver.bootstrap_owner != null) {
                        driver.mutex.unlock(driver.io);
                        return error.CoreNotStarted;
                    }
                    if (!driver.manual_drain_active) {
                        driver.manual_drain_active = true;
                        drain_on_current_thread = true;
                        break;
                    }
                },
                .running => break,
                .poisoned => {
                    if (driver.bootstrap_owner != null) {
                        driver.mutex.unlock(driver.io);
                        return error.CoreNotStarted;
                    }
                    if (driver.settled_acceptance_frontier >= settlement_target or
                        driver.manual_drain_active or
                        driver.worker != null or
                        driver.cold_shutdown_drainer_active)
                    {
                        break;
                    }
                    if (driver.active_cold_calls == 0 and
                        driver.bootstrap_owner == null)
                    {
                        driver.manual_drain_active = true;
                        drain_on_current_thread = true;
                        break;
                    }
                },
                .closing, .stopped => {
                    driver.mutex.unlock(driver.io);
                    return error.CoreStopped;
                },
            }
            if (driver.close_requested.load(.acquire) and
                driver.lifecycle.current() != .poisoned)
            {
                driver.mutex.unlock(driver.io);
                return error.CoreStopped;
            }
            driver.completion_available.waitUncancelable(driver.io, &driver.mutex);
        }
        driver.mutex.unlock(driver.io);

        if (drain_on_current_thread) {
            self.drainAcceptedOnCurrentThread();
            self.finishManualDrain();
        }

        driver.mutex.lockUncancelable(driver.io);
        while (driver.settled_acceptance_frontier < settlement_target) {
            switch (driver.lifecycle.current()) {
                .stopped => {
                    driver.mutex.unlock(driver.io);
                    return error.CoreStopped;
                },
                .cold, .running, .closing, .poisoned => {},
            }
            driver.accepted_drain_waiters += 1;
            driver.completion_available.broadcast(driver.io);
            driver.completion_available.waitUncancelable(driver.io, &driver.mutex);
            std.debug.assert(driver.accepted_drain_waiters > 0);
            driver.accepted_drain_waiters -= 1;
        }
        switch (driver.lifecycle.current()) {
            .poisoned => {
                driver.mutex.unlock(driver.io);
                self.settleReadyRegisteredCallbacksAfterFatal();
                self.waitForFatalCallbackDispatches();
                return error.CorePoisoned;
            },
            .closing, .stopped => {
                driver.mutex.unlock(driver.io);
                return error.CoreStopped;
            },
            .cold, .running => {},
        }
        driver.mutex.unlock(driver.io);

        // Matching-result delivery is intentionally earlier than persistent
        // grouped R2. A successful drain is stronger: it closes and reclaims
        // the grouped prefix before publishing any success count.
        self.execution_mutex.lockUncancelable(driver.io);
        self.settlePersistentGroupedOwned() catch |err| {
            self.execution_mutex.unlock(driver.io);
            if (err == error.CorePoisoned) {
                self.settleReadyRegisteredCallbacksAfterFatal();
                self.waitForFatalCallbackDispatches();
            }
            return err;
        };
        self.execution_mutex.unlock(driver.io);

        driver.mutex.lockUncancelable(driver.io);
        switch (driver.lifecycle.current()) {
            .poisoned => {
                driver.mutex.unlock(driver.io);
                self.settleReadyRegisteredCallbacksAfterFatal();
                self.waitForFatalCallbackDispatches();
                return error.CorePoisoned;
            },
            .closing, .stopped => {
                driver.mutex.unlock(driver.io);
                return error.CoreStopped;
            },
            .cold, .running => {},
        }
        std.debug.assert(driver.async_completed_total >= target);

        // A drain claims only the as-yet-unreported portion of its captured
        // target. This is monotonic under overlapping same/staggered drains and
        // prevents an older waiter from moving the frontier backwards.
        if (driver.async_drained_total >= target) {
            driver.mutex.unlock(driver.io);
            return 0;
        }
        const claimed = target - driver.async_drained_total;
        driver.async_drained_total = target;
        driver.mutex.unlock(driver.io);
        return std.math.cast(usize, claimed) orelse std.math.maxInt(usize);
    }

    pub fn drainAsyncPending(self: *ExchangeApiSharded) DrainPendingError!usize {
        return self.drainPending();
    }

    /// Takes detached command-response ownership. While the API is live,
    /// release it with `releaseResponse`.
    pub fn takeResult(self: *ExchangeApiSharded, request_id: u64) ?kernel_mod.CommandResponse {
        self.assertWiring();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        if (self.callbackRegistrationIndex(request_id) != null) return null;
        return self.takeReadyResultLocked(request_id);
    }

    /// Releases an owned response through this API's serialized allocator.
    ///
    /// Use this while a live API shares a non-thread-safe backing allocator
    /// with worker threads. The caller must coordinate against `deinit`; after
    /// API deinitialization, release any still-owned response directly with
    /// the original allocator supplied to init.
    pub fn releaseResponse(
        self: *ExchangeApiSharded,
        response: *kernel_mod.CommandResponse,
    ) void {
        self.assertWiring();
        response.deinit(self.allocator);
    }

    /// Releases a detached API-command result through the live serialized
    /// facade. Coordinate this call against `deinit`. After API
    /// quiescence/deinit, direct `result.deinit(backing_allocator)` remains
    /// valid; it is also valid while live for a thread-safe backing allocator.
    pub fn releaseApiCommandResult(
        self: *ExchangeApiSharded,
        result: *ApiCommandResult,
    ) void {
        self.assertWiring();
        result.deinit(self.allocator);
    }

    /// Releases a detached native report through the live serialized facade.
    /// Coordinate this call against `deinit`. After API quiescence/deinit,
    /// direct `report.deinit(backing_allocator)` remains valid; it is also
    /// valid while live when the backing allocator is itself thread-safe.
    pub fn releaseReportResponse(
        self: *ExchangeApiSharded,
        report: *kernel_mod.ReportResponse,
    ) void {
        self.assertWiring();
        report.deinit(self.allocator);
    }

    /// Releases a detached order-book snapshot through the live serialized
    /// facade. Coordinate this call against `deinit`. After API
    /// quiescence/deinit, direct `snapshot.deinit(backing_allocator)` remains
    /// valid; it is also valid while live for a thread-safe backing allocator.
    pub fn releaseOrderBookSnapshot(
        self: *ExchangeApiSharded,
        snapshot: *orderbook_naive.L2MarketData,
    ) void {
        self.assertWiring();
        snapshot.deinit(self.allocator);
    }

    /// Releases a detached detailed state-hash report through the live
    /// serialized facade. Coordinate this call against `deinit`. After API
    /// quiescence/deinit, direct `report.deinit(backing_allocator)` remains
    /// valid; it is also valid while live for a thread-safe backing allocator.
    pub fn releaseDetailedStateHashReport(
        self: *ExchangeApiSharded,
        report: *kernel_mod.DetailedStateHashReport,
    ) void {
        self.assertWiring();
        report.deinit(self.allocator);
    }

    /// Releases a detached single-user report through the live serialized
    /// facade. Coordinate this call against `deinit`. After API
    /// quiescence/deinit, direct `report.deinit(backing_allocator)` remains
    /// valid; it is also valid while live for a thread-safe backing allocator.
    pub fn releaseSingleUserReport(
        self: *ExchangeApiSharded,
        report: *kernel_mod.SingleUserReport,
    ) void {
        self.assertWiring();
        report.deinit(self.allocator);
    }

    /// Releases a detached total-currency-balance report through the live
    /// serialized facade. Coordinate this call against `deinit`. After API
    /// quiescence/deinit, direct `report.deinit(backing_allocator)` remains
    /// valid; it is also valid while live for a thread-safe backing allocator.
    pub fn releaseTotalCurrencyBalanceReport(
        self: *ExchangeApiSharded,
        report: *kernel_mod.TotalCurrencyBalanceReport,
    ) void {
        self.assertWiring();
        report.deinit(self.allocator);
    }

    /// Releases a custom result handle while its captured serialized allocator
    /// is live. The handle, including an empty handle left by typed
    /// `Adapter.takeResult`, must be released before API teardown and must
    /// never outlive this API.
    pub fn releaseCustomReportResult(
        self: *ExchangeApiSharded,
        result: *report_registry.ResultHandle,
    ) void {
        self.assertWiring();
        result.deinit();
    }

    /// Borrowed allocator for releasing a typed custom value moved out of a
    /// `ResultHandle`. Pass it only to `Adapter.deinitTakenResult`, do not
    /// retain it, and coordinate its use against API `deinit`. The emptied
    /// `ResultHandle` must separately be released with
    /// `releaseCustomReportResult` before teardown. A moved typed value
    /// retained until after API quiescence may instead be released with the
    /// original backing allocator.
    pub fn liveCustomResultAllocator(self: *ExchangeApiSharded) std.mem.Allocator {
        self.assertWiring();
        return self.allocator;
    }

    /// Takes and reduces one custom report exactly once. Token ownership is
    /// validated before touching ready_results, so a token from another API
    /// cannot consume this API's coincident request id. Reduction runs after
    /// releasing the driver mutex while the response still owns its event
    /// chain, allowing user reducer code to reenter unrelated API operations.
    ///
    /// A returned `ResultHandle` captures this API's allocator facade and must
    /// be released with `releaseCustomReportResult` before API teardown. If a
    /// typed adapter takes its value, release the emptied handle before
    /// teardown too, and release the moved value using
    /// `liveCustomResultAllocator` while live or the original backing
    /// allocator after quiescence.
    pub fn takeCustomReportResult(
        self: *ExchangeApiSharded,
        pending: custom_report_pipeline.PendingRequest,
    ) TakeCustomReportResultError!?*report_registry.ResultHandle {
        self.assertWiring();
        const driver = self.driver;
        const owner_identity: *const anyopaque = @ptrCast(driver);
        const request_id = self.custom_report_requests.pendingRequestId(
            pending,
            owner_identity,
        ) catch |err| return err;

        driver.mutex.lockUncancelable(driver.io);
        const stored = self.custom_ready_results.get(request_id) orelse {
            driver.mutex.unlock(driver.io);
            return null;
        };
        if (!std.meta.eql(stored.pending, pending)) {
            driver.mutex.unlock(driver.io);
            return error.CustomReportCompletionMismatch;
        }
        const removed = self.custom_ready_results.fetchRemove(request_id) orelse
            unreachable;
        std.debug.assert(driver.completion_slots_used > 0);
        driver.completion_slots_used -= 1;
        driver.space_available.broadcast(driver.io);
        driver.mutex.unlock(driver.io);

        var response = removed.value.response;
        defer response.deinit(self.allocator);

        if (response.result_code != .success) {
            try self.custom_report_requests.failPending(pending, owner_identity);
            return error.CustomReportCommandFailed;
        }
        return try self.custom_report_requests.reducePending(
            pending,
            owner_identity,
            self.allocator,
            response.matcher_events_head,
            .{},
        );
    }

    pub fn isCustomReportResultReady(
        self: *ExchangeApiSharded,
        pending: custom_report_pipeline.PendingRequest,
    ) custom_report_pipeline.RequestTokenError!bool {
        self.assertWiring();
        const driver = self.driver;
        const request_id = try self.custom_report_requests.pendingRequestId(
            pending,
            @ptrCast(driver),
        );
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        const stored = self.custom_ready_results.get(request_id) orelse return false;
        return std.meta.eql(stored.pending, pending);
    }

    /// Compatibility alias for `takeResult`; returned ownership follows
    /// `releaseResponse`.
    pub fn takeAsyncResult(self: *ExchangeApiSharded, request_id: u64) ?kernel_mod.CommandResponse {
        return self.takeResult(request_id);
    }

    pub fn takeResultCode(self: *ExchangeApiSharded, request_id: u64) ?cmd.CommandResultCode {
        var response = self.takeResult(request_id) orelse return null;
        defer response.deinit(self.allocator);
        return response.result_code;
    }

    /// Compatibility alias for `takeResult`; returned ownership follows
    /// `releaseResponse`.
    pub fn takeFullResponse(
        self: *ExchangeApiSharded,
        request_id: u64,
    ) ?kernel_mod.CommandResponse {
        return self.takeResult(request_id);
    }

    /// Takes detached snapshot ownership. While the API is live, release it
    /// with `releaseOrderBookSnapshot`.
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

    /// Takes detached report ownership. While the API is live, release it with
    /// `releaseReportResponse`.
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

    /// Takes detached detailed-report ownership. While the API is live,
    /// release it with `releaseDetailedStateHashReport`.
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

    /// Takes detached user-report ownership. While the API is live, release it
    /// with `releaseSingleUserReport`.
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

    /// Takes detached balance-report ownership. While the API is live, release
    /// it with `releaseTotalCurrencyBalanceReport`.
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
        try self.ensureBlockingCallAllowed();
        const handle = try self.submitCommandAsync(mode, command);
        _ = try self.drainPending();
        return self.takeResultCode(handle) orelse error.MissingAsyncResult;
    }

    /// Returns detached report ownership. While the API is live, release it
    /// with `releaseReportResponse`.
    pub fn processNativeReport(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    ) ProcessReportError!kernel_mod.ReportResponse {
        try self.ensureBlockingCallAllowed();
        const handle = try self.submitNativeReportQueryAsync(mode, transfer_id, timestamp, query);
        _ = try self.drainPending();
        return (try self.takeReportResponse(handle)) orelse error.MissingAsyncResult;
    }

    /// Compatibility alias for the historical Zig-native report surface.
    /// Exact Java reduction remains on the unmarked binary-query methods.
    /// The detached result follows `releaseReportResponse` ownership.
    pub fn processReport(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    ) ProcessReportError!kernel_mod.ReportResponse {
        return self.processNativeReport(mode, transfer_id, timestamp, query);
    }

    /// Returns detached ownership; use `releaseDetailedStateHashReport` while
    /// the API is live.
    pub fn processStateHashReport(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
    ) ProcessTypedReportError!kernel_mod.DetailedStateHashReport {
        try self.ensureBlockingCallAllowed();
        const handle = try self.requestStateHashAsync(mode, transfer_id, timestamp);
        _ = try self.drainPending();
        return (try self.takeStateHashDetailed(handle)) orelse error.MissingAsyncResult;
    }

    /// Returns detached ownership; use `releaseSingleUserReport` while the API
    /// is live.
    pub fn processSingleUserReport(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        uid: i64,
    ) ProcessTypedReportError!kernel_mod.SingleUserReport {
        try self.ensureBlockingCallAllowed();
        const handle = try self.requestSingleUserReportAsync(mode, transfer_id, timestamp, uid);
        _ = try self.drainPending();
        return (try self.takeSingleUserReport(handle)) orelse error.MissingAsyncResult;
    }

    /// Returns detached ownership; use `releaseTotalCurrencyBalanceReport`
    /// while the API is live.
    pub fn processTotalCurrencyBalanceReport(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
    ) ProcessTypedReportError!kernel_mod.TotalCurrencyBalanceReport {
        try self.ensureBlockingCallAllowed();
        const handle = try self.requestTotalCurrencyBalanceAsync(mode, transfer_id, timestamp);
        _ = try self.drainPending();
        return (try self.takeTotalCurrencyBalanceReport(handle)) orelse error.MissingAsyncResult;
    }

    /// Returns a custom handle that captures this API's allocator facade. It
    /// must be released with `releaseCustomReportResult` before API teardown;
    /// it must never outlive the API. Typed values moved from the handle follow
    /// the `liveCustomResultAllocator` contract.
    pub fn processCustomReport(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        transfer_id: i32,
        timestamp: i64,
        query: *const report_registry.QueryHandle,
    ) ProcessCustomReportError!*report_registry.ResultHandle {
        // This helper blocks in drainPending. Reject callback/worker callers
        // before admitting a request, otherwise the later reentrancy error
        // would leave a successfully published custom request without a
        // caller-owned token.
        try self.ensureBlockingCallAllowed();

        const pending = try self.submitCustomReportAsync(
            mode,
            transfer_id,
            timestamp,
            query,
        );
        _ = self.drainPending() catch |err| {
            // A concurrent close or fatal transition can win after admission.
            // Join/drain it to quiescence before touching the retained query,
            // then preserve the original drain error for the caller.
            self.shutdown();
            self.discardCustomReportRequest(pending);
            return err;
        };
        // Cleanup is installed only after a successful drain because fatal
        // settlement owns the token until that boundary.
        defer self.discardCustomReportRequest(pending);
        return (try self.takeCustomReportResult(pending)) orelse
            error.MissingAsyncResult;
    }

    /// Returns detached snapshot ownership. While the API is live, release it
    /// with `releaseOrderBookSnapshot`.
    pub fn processOrderBookRequest(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        symbol: i32,
        depth: usize,
    ) ProcessOrderBookError!orderbook_naive.L2MarketData {
        try self.ensureBlockingCallAllowed();
        const handle = try self.orderBookRequestAsync(mode, symbol, depth);
        _ = try self.drainPending();
        return (try self.takeOrderBookSnapshot(handle)) orelse error.MissingAsyncResult;
    }

    pub fn isPersistStateResultReady(
        self: *const ExchangeApiSharded,
        requests: PersistStateRequestPair,
    ) bool {
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        return self.ready_results.contains(requests.matching_request_id) and
            self.ready_results.contains(requests.risk_request_id);
    }

    pub fn takePersistStateResult(
        self: *ExchangeApiSharded,
        requests: PersistStateRequestPair,
    ) ?cmd.CommandResultCode {
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        if (!self.ready_results.contains(requests.matching_request_id) or
            !self.ready_results.contains(requests.risk_request_id)) return null;

        var matching = self.takeReadyResultLocked(requests.matching_request_id).?;
        var risk_cmd = self.takeReadyResultLocked(requests.risk_request_id).?;
        defer matching.deinit(self.allocator);
        defer risk_cmd.deinit(self.allocator);

        return cmd.CommandResultCode.mergeToFirstFailed(&.{
            matching.result_code,
            risk_cmd.result_code,
        });
    }

    pub fn isResultReady(self: *const ExchangeApiSharded, request_id: u64) bool {
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        return self.ready_results.contains(request_id);
    }

    pub fn isAsyncResultReady(self: *const ExchangeApiSharded, request_id: u64) bool {
        return self.isResultReady(request_id);
    }

    pub fn pendingCount(self: *const ExchangeApiSharded) usize {
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        return std.math.cast(usize, driver.async_accepted_total - driver.async_completed_total) orelse
            std.math.maxInt(usize);
    }

    pub fn asyncPendingCount(self: *const ExchangeApiSharded) usize {
        return self.pendingCount();
    }

    pub fn readyCount(self: *const ExchangeApiSharded) usize {
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        return self.ready_results.count();
    }

    pub fn asyncReadyCount(self: *const ExchangeApiSharded) usize {
        return self.readyCount();
    }

    pub fn runtimePendingCount(self: *const ExchangeApiSharded) usize {
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        return std.math.cast(usize, driver.runtime_accepted_total - driver.runtime_completed_total) orelse
            std.math.maxInt(usize);
    }

    pub fn setResultSink(self: *ExchangeApiSharded, sink: runtime_mod.ResultSink) void {
        self.setResultSinkChecked(sink) catch |err| @panic(@errorName(err));
    }

    pub fn setRawResultSink(self: *ExchangeApiSharded, sink: RawResultSink) void {
        self.setRawResultSinkChecked(sink) catch |err| @panic(@errorName(err));
    }

    pub fn setRawResultSinkChecked(
        self: *ExchangeApiSharded,
        sink: RawResultSink,
    ) CallbackConfigError!void {
        self.assertWiring();
        const owns_quiescence_barrier = try self.lockCallbackConfigBarrier();
        defer if (owns_quiescence_barrier) {
            self.callback_execution_mutex.unlock(self.driver.io);
        };
        self.callback_mutex.lockUncancelable(self.driver.io);
        defer self.callback_mutex.unlock(self.driver.io);
        self.raw_result_sink = sink;
    }

    pub fn clearRawResultSink(self: *ExchangeApiSharded) void {
        self.clearRawResultSinkChecked() catch |err| @panic(@errorName(err));
    }

    pub fn clearRawResultSinkChecked(
        self: *ExchangeApiSharded,
    ) CallbackConfigError!void {
        try self.setRawResultSinkChecked(.{});
    }

    pub fn setResultSinkChecked(
        self: *ExchangeApiSharded,
        sink: runtime_mod.ResultSink,
    ) CallbackConfigError!void {
        self.assertWiring();
        const owns_quiescence_barrier = try self.lockCallbackConfigBarrier();
        defer if (owns_quiescence_barrier) {
            self.callback_execution_mutex.unlock(self.driver.io);
        };
        self.callback_mutex.lockUncancelable(self.driver.io);
        defer self.callback_mutex.unlock(self.driver.io);
        self.runtime_sink = sink;
    }

    pub fn setSimpleEventsHandler(self: *ExchangeApiSharded, handler: SimpleEventsHandler) void {
        self.setSimpleEventsHandlerChecked(handler) catch |err| @panic(@errorName(err));
    }

    pub fn setSimpleEventsHandlerChecked(
        self: *ExchangeApiSharded,
        handler: SimpleEventsHandler,
    ) CallbackConfigError!void {
        self.assertWiring();
        const owns_quiescence_barrier = try self.lockCallbackConfigBarrier();
        defer if (owns_quiescence_barrier) {
            self.callback_execution_mutex.unlock(self.driver.io);
        };
        self.callback_mutex.lockUncancelable(self.driver.io);
        defer self.callback_mutex.unlock(self.driver.io);
        if (self.simple_events_processor == null) {
            self.simple_events_processor = simple_events_mod.SimpleEventsProcessor.init(
                self.allocator,
                .{},
            );
        }
        try self.advanceSimpleEventsRevisionLocked();
        self.simple_events_handler = handler;
    }

    pub fn clearSimpleEventsHandler(self: *ExchangeApiSharded) void {
        self.clearSimpleEventsHandlerChecked() catch |err| @panic(@errorName(err));
    }

    pub fn clearSimpleEventsHandlerChecked(
        self: *ExchangeApiSharded,
    ) CallbackConfigError!void {
        self.assertWiring();
        const owns_quiescence_barrier = try self.lockCallbackConfigBarrier();
        defer if (owns_quiescence_barrier) {
            self.callback_execution_mutex.unlock(self.driver.io);
        };
        self.callback_mutex.lockUncancelable(self.driver.io);
        defer self.callback_mutex.unlock(self.driver.io);
        try self.advanceSimpleEventsRevisionLocked();
        self.simple_events_handler = null;
    }

    pub fn enqueueRuntime(
        self: *ExchangeApiSharded,
        command: cmd.OrderCommand,
    ) EnqueueRuntimeError!void {
        try self.enqueueDetachedBatch(&.{command}, .routed, .runtime);
    }

    pub fn drainRuntime(self: *ExchangeApiSharded) RuntimeDrainError!runtime_mod.DrainOutcome {
        try self.ensureBlockingCallAllowed();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        self.ensureAcceptingLocked() catch |err| {
            driver.mutex.unlock(driver.io);
            return err;
        };
        if (driver.lifecycle.current() == .cold and driver.bootstrap_owner != null) {
            driver.mutex.unlock(driver.io);
            return error.CoreNotStarted;
        }

        const pre_start = driver.lifecycle.current() == .cold;
        if (pre_start) {
            while (driver.manual_drain_active) {
                driver.completion_available.waitUncancelable(driver.io, &driver.mutex);
                self.ensureAcceptingLocked() catch |err| {
                    driver.mutex.unlock(driver.io);
                    return err;
                };
                if (driver.lifecycle.current() != .cold) break;
            }
            if (driver.lifecycle.current() == .cold) driver.manual_drain_active = true;
        }
        const drain_on_current_thread = driver.lifecycle.current() == .cold;
        const target = driver.runtime_accepted_total;
        const start = driver.runtime_drained_total;
        driver.mutex.unlock(driver.io);

        if (drain_on_current_thread) {
            self.drainAcceptedOnCurrentThread();
            self.finishManualDrain();
        }

        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        while (driver.runtime_completed_total < target) {
            if (driver.lifecycle.current() == .poisoned) return error.CorePoisoned;
            if (driver.lifecycle.current() == .stopped) return error.CoreStopped;
            driver.completion_available.waitUncancelable(driver.io, &driver.mutex);
        }
        if (driver.lifecycle.current() == .poisoned) return error.CorePoisoned;
        driver.runtime_drained_total = target;
        const processed = std.math.cast(usize, target - start) orelse std.math.maxInt(usize);
        return .{
            .groups_processed = processed,
            .commands_processed = processed,
            .last_merged_result = driver.last_runtime_result,
        };
    }

    pub fn queryUserBalance(
        self: *ExchangeApiSharded,
        uid: i64,
        currency: i32,
    ) ReportError!?i64 {
        const cold_claimed = try self.waitForAcceptedPrefixAndLockBatchFence();
        defer if (cold_claimed) self.finishColdDirectCall();
        defer self.batch_barrier_fence.unlock(self.driver.io);
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        try self.settlePersistentGroupedOwned();
        return self.core.queryUserBalance(uid, currency);
    }

    /// Returns detached report ownership. While the API is live, release it
    /// with `releaseReportResponse`.
    pub fn queryReport(
        self: *ExchangeApiSharded,
        query: kernel_mod.ReportQuery,
    ) QueryReportError!kernel_mod.ReportResponse {
        const cold_claimed = try self.waitForAcceptedPrefixAndLockBatchFence();
        defer if (cold_claimed) self.finishColdDirectCall();
        defer self.batch_barrier_fence.unlock(self.driver.io);
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        try self.settlePersistentGroupedOwned();
        return self.core.queryReport(query);
    }

    /// Returns detached balance-report ownership. While the API is live,
    /// release it with `releaseTotalCurrencyBalanceReport`.
    pub fn queryTotalCurrencyBalanceReport(
        self: *ExchangeApiSharded,
    ) QueryReportError!kernel_mod.TotalCurrencyBalanceReport {
        return self.aggregateTotalCurrencyBalance();
    }

    pub fn queryUserBalanceReport(
        self: *ExchangeApiSharded,
        uid: i64,
        currency: i32,
    ) QueryReportError!?i64 {
        return self.queryUserBalance(uid, currency);
    }

    /// Returns detached report ownership. While the API is live, release it
    /// with `releaseReportResponse`.
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

    /// Returns detached detailed-report ownership. While the API is live,
    /// release it with `releaseDetailedStateHashReport`.
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

    /// Returns detached user-report ownership. While the API is live, release
    /// it with `releaseSingleUserReport`.
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

    /// Returns detached snapshot ownership. While the API is live, release it
    /// with `releaseOrderBookSnapshot`.
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

    /// Returns detached balance-report ownership. While the API is live,
    /// release it with `releaseTotalCurrencyBalanceReport`.
    pub fn aggregateTotalCurrencyBalance(
        self: *ExchangeApiSharded,
    ) AggregateError!kernel_mod.TotalCurrencyBalanceReport {
        const cold_claimed = try self.waitForAcceptedPrefixAndLockBatchFence();
        defer if (cold_claimed) self.finishColdDirectCall();
        defer self.batch_barrier_fence.unlock(self.driver.io);
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        try self.settlePersistentGroupedOwned();
        return self.core.aggregateTotalCurrencyBalance();
    }

    pub fn detailedHashFingerprint(self: *ExchangeApiSharded) ReportError!u64 {
        const cold_claimed = try self.waitForAcceptedPrefixAndLockBatchFence();
        defer if (cold_claimed) self.finishColdDirectCall();
        defer self.batch_barrier_fence.unlock(self.driver.io);
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        try self.settlePersistentGroupedOwned();
        return self.core.detailedHashFingerprint();
    }

    fn executeWithMode(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command: *cmd.OrderCommand,
    ) ExecuteError!kernel_mod.CommandResponse {
        self.assertWiring();
        if (!try self.shouldUseWorkerForSynchronousCall()) {
            defer self.finishColdDirectCall();
            const sequences = try self.prepareColdPublicationRange(1);
            self.stampDirectLiveBatch(command[0..1]);
            return self.executePreparedDirect(mode, sequences.first, command, .terminal);
        }

        const request_id = try self.enqueuePoll(command.*, mode, .synchronous);
        const response = try self.waitTakeResponse(request_id);
        command.result_code = response.result_code;
        return response;
    }

    /// Cold direct calls do not traverse the worker queue, so their currently
    /// available range is the logical slice supplied by the typed entrypoint.
    fn stampDirectLiveBatch(
        self: *ExchangeApiSharded,
        commands: []cmd.OrderCommand,
    ) void {
        std.debug.assert(commands.len > 0);
        for (commands) |*command| _ = self.grouping.stampLive(command);
        self.grouping.finishAvailableBatch(self.grouping_clock.nowNs());
    }

    fn executeRawApiCommandWithMode(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command: *cmd.OrderCommand,
    ) ExecuteError!kernel_mod.CommandResponse {
        self.assertWiring();
        if (!try self.shouldUseWorkerForSynchronousCall()) {
            defer self.finishColdDirectCall();
            return self.executeWithModeDirect(mode, command);
        }

        const request_id = try self.enqueuePoll(command.*, mode, .synchronous_raw);
        const response = try self.waitTakeResponse(request_id);
        command.result_code = response.result_code;
        return response;
    }

    fn executeWithModeDirect(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command: *cmd.OrderCommand,
    ) ExecuteError!kernel_mod.CommandResponse {
        const sequences = try self.prepareColdPublicationRange(1);
        return self.executePreparedDirect(mode, sequences.first, command, .terminal);
    }

    fn prepareColdPublicationRange(
        self: *ExchangeApiSharded,
        count: usize,
    ) ExecuteError!publication.PublicationRange {
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        if (self.journal) |*journal| {
            try journal.entries.ensureUnusedCapacity(self.allocator, count);
        }
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        const prepared = try driver.publication_sequencer.prepare(count);
        driver.publication_sequencer.commit(prepared);
        return prepared;
    }

    fn executePreparedDirect(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        sequence: publication.PublicationSequence,
        command: *cmd.OrderCommand,
        journal_batch_position: serialization_processor.JournalBatchPosition,
    ) ExecuteError!kernel_mod.CommandResponse {
        self.execution_mutex.lockUncancelable(self.driver.io);
        var response = self.executeWithModeDirectAtSequence(
            mode,
            sequence,
            command,
            journal_batch_position,
        ) catch |err| {
            self.execution_mutex.unlock(self.driver.io);
            self.poisonDirectExecution(sequence, command, err);
            return switch (err) {
                error.SettlementPreparationRejected,
                error.SettlementRiskR2MarkRejected,
                error.SettlementJournalMarkRejected,
                error.SettlementRecoveryRejected,
                => error.CorePoisoned,
                else => @errorCast(err),
            };
        };
        self.execution_mutex.unlock(self.driver.io);
        self.publishCompletionObservers(command, &response, null);
        return response;
    }

    fn executeWithModeDirectAtSequence(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        sequence: publication.PublicationSequence,
        command: *cmd.OrderCommand,
        journal_batch_position: serialization_processor.JournalBatchPosition,
    ) (sharded_core_mod.ExecuteWithSettlementError || DescriptorTrackingError || journal_mod.JournalIoError || serialization_processor.JournalError)!kernel_mod.CommandResponse {
        const input = command.*;
        var response = try self.executeWithModeNoJournalAtSequence(
            mode,
            sequence,
            command,
            .live,
        );
        errdefer response.deinit(self.allocator);
        self.coordinatePersistenceCommand(mode, command, &response);
        try self.appendJournalEntry(
            mode,
            response.sequence,
            input,
            response.result_code,
            journal_batch_position,
        );
        _ = try self.scopedSettlementHooks().markJournalCommitted(sequence);
        return response;
    }

    fn appendJournalEntry(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        sequence: u64,
        input: cmd.OrderCommand,
        result_code: cmd.CommandResultCode,
        journal_batch_position: serialization_processor.JournalBatchPosition,
    ) (journal_mod.JournalIoError || serialization_processor.JournalError)!void {
        const journal = if (self.journal) |*value| value else return;
        const entry: journal_mod.JournalEntry = .{
            .sequence = sequence,
            .mode = mode,
            .input = input,
            .result_code = result_code,
        };
        journal.entries.appendAssumeCapacity(entry);

        if (self.custom_serialization_processor) |*processor| {
            try processor.writeToJournal(.{
                .sequence = entry.sequence,
                .command = entry.input,
                .end_of_batch = journal_batch_position.endOfBatch(),
            });
        }

        if (self.durable_journal) |*appender| {
            if (shouldAppendToJavaDurableJournal(entry.input.command)) {
                try appender.append(entry);
            } else {
                try appender.appendSkippedSequence(entry.sequence, entry.input.timestamp);
            }
        }
    }

    fn settlePersistentGroupedOwned(
        self: *ExchangeApiSharded,
    ) LifecycleError!void {
        const runtime = self.persistent_grouped_runtime orelse return;
        _ = runtime.terminalQuiesce() catch |err| {
            self.poisonFromGroupedBarrierOwned(runtime, err);
            return error.CorePoisoned;
        };
    }

    /// Called with API execution ownership. Callback/tail settlement is left to
    /// the driver worker after this lock is released.
    fn poisonFromGroupedBarrierOwned(
        self: *ExchangeApiSharded,
        runtime: *sharded_pipeline.PersistentGroupedRuntime,
        failure: anyerror,
    ) void {
        const grouped_fatal = runtime.fatalInfo();
        const driver = self.driver;
        const fatal_stage = if (grouped_fatal) |inner|
            groupedFailureStage(inner.stage)
        else
            .runtime;
        const fatal_sequence = if (grouped_fatal) |inner|
            @as(?u64, inner.sequence)
        else
            null;
        const fatal_command = if (grouped_fatal) |inner| inner.command else null;
        const fatal_cause = if (grouped_fatal) |inner| inner.cause else failure;

        // Journal-enabled admission acquires driver -> execution while
        // reserving append capacity. Never invert that order here. The
        // runtime fatal is copied while execution ownership makes the runtime
        // pointer stable, then execution is yielded before acquiring driver.
        // Another poison owner may destroy the runtime during this handoff,
        // so nothing below dereferences the borrowed `runtime` pointer.
        self.execution_mutex.unlock(driver.io);
        driver.mutex.lockUncancelable(driver.io);
        const fatal: runtime_mod.FatalRecord = .{
            .stage = fatal_stage,
            .sequence = fatal_sequence,
            .request_id = if (fatal_sequence) |sequence|
                self.readyRequestIdForSequenceLocked(sequence)
            else
                null,
            .command = fatal_command,
            .cause = fatal_cause,
        };
        const first_poison = driver.lifecycle.poison(fatal);
        driver.prepared_available_remaining = 0;
        driver.work_available.broadcast(driver.io);
        driver.space_available.broadcast(driver.io);
        driver.completion_available.broadcast(driver.io);
        driver.mutex.unlock(driver.io);

        // Restore the caller's execution-lock invariant before teardown or
        // return. If another owner won first poison it also owns teardown.
        if (self.grouped_poison_reacquire_test_gate) |gate| {
            gate.pauseBeforeReacquire();
        }
        self.execution_mutex.lockUncancelable(driver.io);
        if (first_poison) {
            self.stopPersistentGroupedRuntime();
            self.core.shutdown();
        }
    }

    fn executePersistentGroupedAtSequence(
        self: *ExchangeApiSharded,
        runtime: *sharded_pipeline.PersistentGroupedRuntime,
        mode: ExecutionMode,
        sequence: publication.PublicationSequence,
        command: *cmd.OrderCommand,
        journal_batch_position: serialization_processor.JournalBatchPosition,
    ) anyerror!kernel_mod.CommandResponse {
        _ = runtime.submitAtSequence(sequence, command) catch |err| switch (err) {
            error.SlotNotReclaimed => blk: {
                if (self.persistent_capacity_test_gate) |gate| gate.markReached();
                // The sole driver consumer reaches this point only after every
                // earlier item has completed journal/result transfer, observers,
                // callbacks, and outer settlement. Close that healthy internal
                // capacity boundary under execution ownership, then retry the
                // unchanged atomic singleton admission.
                try self.settlePersistentGroupedOwned();
                break :blk try runtime.submitAtSequence(sequence, command);
            },
            else => return err,
        };
        return self.completePersistentGroupedAtSequence(
            runtime,
            mode,
            sequence,
            command,
            journal_batch_position,
        );
    }

    /// Completes a command whose publication was copied by atomic batch
    /// admission. Results remain strictly FIFO even though R1/matching work is
    /// allowed to overlap across the still-open events group.
    fn completePersistentGroupedAtSequence(
        self: *ExchangeApiSharded,
        runtime: *sharded_pipeline.PersistentGroupedRuntime,
        mode: ExecutionMode,
        sequence: publication.PublicationSequence,
        command: *cmd.OrderCommand,
        journal_batch_position: serialization_processor.JournalBatchPosition,
    ) anyerror!kernel_mod.CommandResponse {
        const input = command.*;
        try runtime.waitForMatching(sequence.toInt());
        const result_code = try runtime.matchedResultCode(sequence.toInt());
        try self.appendJournalEntry(
            mode,
            sequence.toInt(),
            input,
            result_code,
            journal_batch_position,
        );
        // Disabled compatibility still advances the logical frontier. Enabled
        // settlement reaches this combined boundary only after native sync, so
        // a rejected durable mark poisons before result transfer.
        try runtime.acknowledgeDurableJournal(sequence.toInt());
        var response = (try runtime.takeNextResult()) orelse
            return error.MissingGroupedResult;
        errdefer response.deinit(self.allocator);
        std.debug.assert(response.sequence == sequence.toInt());
        command.* = response.command;
        return response;
    }

    fn poisonDirectExecution(
        self: *ExchangeApiSharded,
        sequence: publication.PublicationSequence,
        command: *const cmd.OrderCommand,
        failure: anyerror,
    ) void {
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        const fatal: runtime_mod.FatalRecord = .{
            .stage = failureStage(failure),
            .sequence = sequence.toInt(),
            .request_id = null,
            .command = command.command,
            .cause = failure,
        };
        if (driver.lifecycle.poison(fatal)) {
            self.core.shutdown();
            driver.prepared_available_remaining = 0;
            driver.work_available.broadcast(driver.io);
            driver.space_available.broadcast(driver.io);
            driver.completion_available.broadcast(driver.io);
        }
        driver.mutex.unlock(driver.io);
    }

    fn executeWithModeNoJournal(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command: *cmd.OrderCommand,
    ) sharded_core_mod.ExecuteWithSettlementError!kernel_mod.CommandResponse {
        var response = switch (mode) {
            .routed => try self.core.executeRoutedWithSettlement(
                command,
                self.scopedSettlementHooks(),
                .live,
            ),
            .split => try self.core.executeSplitWithSettlement(
                command,
                self.scopedSettlementHooks(),
                .live,
            ),
        };
        errdefer response.deinit(self.allocator);
        _ = try self.scopedSettlementHooks().markRiskR2Committed(
            publication.PublicationSequence.fromInt(response.sequence),
        );
        return response;
    }

    fn executeWithModeNoJournalAtSequence(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        sequence: publication.PublicationSequence,
        command: *cmd.OrderCommand,
        settlement_mode: settlement_lifecycle.ExecutionMode,
    ) sharded_core_mod.ExecuteWithSettlementError!kernel_mod.CommandResponse {
        var response = switch (mode) {
            .routed => try self.core.executeRoutedAtSequenceWithSettlement(
                sequence,
                command,
                self.scopedSettlementHooks(),
                settlement_mode,
            ),
            .split => try self.core.executeSplitAtSequenceWithSettlement(
                sequence,
                command,
                self.scopedSettlementHooks(),
                settlement_mode,
            ),
        };
        errdefer response.deinit(self.allocator);
        if (settlement_mode == .live) {
            _ = try self.scopedSettlementHooks().markRiskR2Committed(sequence);
        }
        return response;
    }

    fn shouldUseWorkerForSynchronousCall(self: *ExchangeApiSharded) LifecycleError!bool {
        try self.ensureBlockingCallAllowed();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        while (driver.lifecycle.current() == .cold and driver.active_cold_calls != 0) {
            try self.ensureAcceptingLocked();
            driver.completion_available.waitUncancelable(driver.io, &driver.mutex);
        }
        try self.ensureAcceptingLocked();
        return switch (driver.lifecycle.current()) {
            .running => true,
            .cold => blk: {
                if (driver.bootstrap_owner != null or
                    !driver.queue.isEmpty() or
                    driver.manual_drain_active)
                {
                    return error.CoreNotStarted;
                }
                driver.active_cold_calls += 1;
                break :blk false;
            },
            .closing, .stopped, .poisoned => unreachable,
        };
    }

    fn finishColdDirectCall(self: *ExchangeApiSharded) void {
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        std.debug.assert(driver.active_cold_calls > 0);
        driver.active_cold_calls -= 1;
        driver.completion_available.broadcast(driver.io);
        driver.mutex.unlock(driver.io);
    }

    /// Callback and worker producers are no-wait publishers. A foreign
    /// callback must never block on this API's driver mutex because another
    /// callback can already be holding the inverse backend lock order.
    fn lockPublicationDriver(self: *ExchangeApiSharded) LifecycleError!void {
        const driver = self.driver;
        if (runtime_mod.isCallbackExecutionActive() or self.isWorkerThread()) {
            if (!driver.mutex.tryLock()) return error.CoreReentrantCall;
            return;
        }
        driver.mutex.lockUncancelable(driver.io);
    }

    fn enqueueResponseBatch(
        self: *ExchangeApiSharded,
        commands: []const cmd.OrderCommand,
        mode: ExecutionMode,
        kind: WorkKind,
        request_ids: []u64,
    ) AsyncSubmitError!void {
        self.assertWiring();
        std.debug.assert(kind == .asynchronous or kind == .asynchronous_raw or kind == .synchronous or kind == .synchronous_raw);
        std.debug.assert(commands.len == request_ids.len);
        if (commands.len == 0) return;

        const driver = self.driver;
        if (commands.len > driver.queue.capacity()) return error.CoreQueueFull;
        try self.lockPublicationDriver();
        defer driver.mutex.unlock(driver.io);
        try self.waitForColdPublisherLocked();
        try self.waitForCapacityLocked(commands.len, commands.len);

        const count_u64 = std.math.cast(u64, commands.len) orelse return error.RequestIdOverflow;
        const final_request_id = std.math.add(u64, self.next_request_id, count_u64) catch
            return error.RequestIdOverflow;
        const accepted_after = std.math.add(
            u64,
            driver.accepted_total,
            count_u64,
        ) catch return error.RequestIdOverflow;
        try self.ensureJournalCapacityLocked(commands.len);
        const sequences = try driver.publication_sequencer.prepare(commands.len);
        var reservation = driver.queue.reserve(commands.len) catch unreachable;
        for (commands, 0..) |command, index| {
            const index_u64 = std.math.cast(u64, index) orelse return error.RequestIdOverflow;
            const request_id = self.next_request_id + index_u64 + 1;
            request_ids[index] = request_id;
            reservation.at(index).* = .{
                .acceptance_id = driver.accepted_total + index_u64 + 1,
                .sequence = sequences.at(index),
                .mode = mode,
                .command = command,
                .completion = .{ .poll = request_id },
                .kind = kind,
            };
        }

        driver.publication_sequencer.commit(sequences);
        driver.queue.commit(reservation);
        if (self.journal != null) driver.journal_slots_reserved += commands.len;
        driver.completion_slots_used += commands.len;
        self.next_request_id = final_request_id;
        driver.accepted_total = accepted_after;
        if (kind == .asynchronous or kind == .asynchronous_raw) {
            driver.async_accepted_total += count_u64;
        }
        driver.work_available.broadcast(driver.io);
    }

    /// Admits one logical binary transfer, then exposes at most one quarter of
    /// the queue per publication step while the worker drains older frames.
    /// Every allocator, sequence, request-id, journal, and completion-capacity
    /// check is complete before the first frame becomes visible.
    fn enqueueBinaryFrames(
        self: *ExchangeApiSharded,
        commands: []const cmd.OrderCommand,
        mode: ExecutionMode,
        completion_mode: BinaryCompletionMode,
    ) AsyncSubmitError!?u64 {
        self.assertWiring();
        std.debug.assert(commands.len > 0);
        std.debug.assert(commands[commands.len - 1].symbol == -1);

        const driver = self.driver;
        const caller_must_not_wait =
            runtime_mod.isCallbackExecutionActive() or self.isWorkerThread();
        try self.lockPublicationDriver();
        var driver_locked = true;
        defer if (driver_locked) driver.mutex.unlock(driver.io);

        try self.waitForColdPublisherLocked();
        try self.waitForPublicationGateLocked();
        const running = driver.lifecycle.current() == .running;
        const chunk_capacity = driver.queue.capacity() / 4;
        std.debug.assert(chunk_capacity > 0);

        const poll_slots: usize = if (completion_mode.ownsPollSlot()) 1 else 0;
        if (running) {
            if (caller_must_not_wait) {
                // A running external publisher can stream transfers larger
                // than the ring. A callback or worker cannot: it would admit
                // a prefix and then wait for a later chunk while retaining a
                // callback barrier. Reject the whole transfer before any
                // publication identity or accounting mutation.
                if (commands.len > driver.queue.capacity()) {
                    return error.CoreReentrantCall;
                }
                try self.waitForCapacityLocked(commands.len, poll_slots);
            } else {
                // Reserve only the logical terminal completion here. Queue
                // space is acquired chunk-by-chunk after the stream gate is
                // installed.
                try self.waitForCapacityLocked(0, poll_slots);
            }
        } else {
            // Cold APIs require the entire logical transfer to fit because
            // there is no concurrent consumer.
            try self.waitForCapacityLocked(commands.len, poll_slots);
        }

        const count_u64 = std.math.cast(u64, commands.len) orelse return error.RequestIdOverflow;
        const request_id: ?u64 = if (completion_mode.hasRequestId())
            std.math.add(u64, self.next_request_id, count_u64) catch
                return error.RequestIdOverflow
        else
            null;
        const accepted_after = std.math.add(
            u64,
            driver.accepted_total,
            count_u64,
        ) catch return error.RequestIdOverflow;
        const first_acceptance_id = driver.accepted_total + 1;
        const async_accepted_after = if (completion_mode.countsAsAsync())
            std.math.add(u64, driver.async_accepted_total, count_u64) catch
                return error.RequestIdOverflow
        else
            driver.async_accepted_total;
        const journal_slots_after = if (self.journal != null)
            std.math.add(usize, driver.journal_slots_reserved, commands.len) catch
                return error.OutOfMemory
        else
            driver.journal_slots_reserved;

        try self.ensureJournalCapacityLocked(commands.len);
        const sequences = try driver.publication_sequencer.prepare(commands.len);
        const work_kind = completion_mode.workKind();
        const terminal_owner: CompletionOwner = switch (completion_mode) {
            .synchronous, .asynchronous => .{ .poll = request_id.? },
            .detached => .detached,
            .custom_report => |custom| blk: {
                const pending = custom.staged.commit(request_id.?) catch unreachable;
                custom.pending_out.* = pending;
                break :blk .{ .custom_report = .{
                    .request_id = request_id.?,
                    .pending = pending,
                } };
            },
        };

        if (!running) {
            var reservation = driver.queue.reserve(commands.len) catch unreachable;
            for (commands, 0..) |command, index| {
                const acceptance_offset = std.math.cast(u64, index) orelse unreachable;
                reservation.at(index).* = .{
                    .acceptance_id = first_acceptance_id + acceptance_offset,
                    .sequence = sequences.at(index),
                    .mode = mode,
                    .command = command,
                    .completion = if (index + 1 == commands.len)
                        terminal_owner
                    else
                        .detached,
                    .kind = work_kind,
                };
            }

            driver.publication_sequencer.commit(sequences);
            driver.queue.commit(reservation);
            driver.journal_slots_reserved = journal_slots_after;
            driver.completion_slots_used += poll_slots;
            if (request_id) |id| self.next_request_id = id;
            driver.accepted_total = accepted_after;
            driver.async_accepted_total = async_accepted_after;
            driver.work_available.broadcast(driver.io);
            return request_id;
        }

        // From here the transfer is admitted. Shutdown lets it finish; fatal
        // poison settles any suffix that never became visible to the worker.
        driver.publication_sequencer.commit(sequences);
        driver.journal_slots_reserved = journal_slots_after;
        driver.completion_slots_used += poll_slots;
        if (request_id) |id| self.next_request_id = id;
        driver.accepted_total = accepted_after;
        driver.async_accepted_total = async_accepted_after;
        driver.active_binary_stream = true;

        var published: usize = 0;
        while (published < commands.len) {
            const chunk_len = @min(chunk_capacity, commands.len - published);
            self.waitForBinaryChunkCapacityLocked(chunk_len) catch |err| {
                self.abortBinaryStreamLocked(
                    completion_mode,
                    first_acceptance_id + @as(u64, @intCast(published)),
                    commands.len - published,
                );
                const pending_to_fail = switch (terminal_owner) {
                    .custom_report => |owner| owner.pending,
                    .detached, .poll, .callback => null,
                };
                driver.mutex.unlock(driver.io);
                driver_locked = false;
                if (pending_to_fail) |pending| {
                    self.custom_report_requests.failPending(
                        pending,
                        @ptrCast(driver),
                    ) catch unreachable;
                }
                return err;
            };

            var reservation = driver.queue.reserve(chunk_len) catch unreachable;
            for (0..chunk_len) |chunk_index| {
                const command_index = published + chunk_index;
                const acceptance_offset = std.math.cast(u64, command_index) orelse unreachable;
                reservation.at(chunk_index).* = .{
                    .acceptance_id = first_acceptance_id + acceptance_offset,
                    .sequence = sequences.at(command_index),
                    .mode = mode,
                    .command = commands[command_index],
                    .completion = if (command_index + 1 == commands.len)
                        terminal_owner
                    else
                        .detached,
                    .kind = work_kind,
                };
            }
            driver.queue.commit(reservation);
            published += chunk_len;
            driver.work_available.broadcast(driver.io);
        }

        driver.active_binary_stream = false;
        driver.work_available.broadcast(driver.io);
        driver.space_available.broadcast(driver.io);
        driver.completion_available.broadcast(driver.io);
        return request_id;
    }

    /// Settles an admitted suffix that never reached the queue after the
    /// worker poisoned the API. Published frames are settled by fatal drain.
    fn abortBinaryStreamLocked(
        self: *ExchangeApiSharded,
        completion_mode: BinaryCompletionMode,
        first_unpublished_acceptance_id: u64,
        unpublished_count: usize,
    ) void {
        const driver = self.driver;
        std.debug.assert(driver.active_binary_stream);
        std.debug.assert(unpublished_count > 0);
        std.debug.assert(driver.lifecycle.current() == .poisoned);

        if (completion_mode.ownsPollSlot()) {
            std.debug.assert(driver.completion_slots_used > self.claimedCompletionCountLocked());
            driver.completion_slots_used -= 1;
        }
        if (self.journal != null) {
            std.debug.assert(driver.journal_slots_reserved >= unpublished_count);
            driver.journal_slots_reserved -= unpublished_count;
        }
        const unpublished_u64 = std.math.cast(u64, unpublished_count) orelse unreachable;
        self.settleAcceptedRangeLocked(
            first_unpublished_acceptance_id,
            unpublished_u64,
        );
        if (completion_mode.countsAsAsync()) {
            driver.async_completed_total += unpublished_u64;
        }
        driver.active_binary_stream = false;
        driver.work_available.broadcast(driver.io);
        driver.space_available.broadcast(driver.io);
        driver.completion_available.broadcast(driver.io);
    }

    fn enqueuePoll(
        self: *ExchangeApiSharded,
        command: cmd.OrderCommand,
        mode: ExecutionMode,
        kind: WorkKind,
    ) AsyncSubmitError!u64 {
        var request_ids: [1]u64 = undefined;
        try self.enqueueResponseBatch(&.{command}, mode, kind, request_ids[0..]);
        return request_ids[0];
    }

    fn enqueueCallback(
        self: *ExchangeApiSharded,
        command: cmd.OrderCommand,
        mode: ExecutionMode,
        callback: CommandResponseCallback,
    ) AsyncSubmitError!void {
        self.assertWiring();
        const driver = self.driver;
        try self.lockPublicationDriver();
        defer driver.mutex.unlock(driver.io);
        try self.waitForColdPublisherLocked();

        // Callback-owned requests consume queue capacity, but never occupy a
        // poll-result slot or enter `ready_results`.
        try self.waitForCapacityLocked(1, 0);
        const request_id = std.math.add(u64, self.next_request_id, 1) catch
            return error.RequestIdOverflow;
        const acceptance_id = std.math.add(u64, driver.accepted_total, 1) catch
            return error.RequestIdOverflow;
        try self.ensureJournalCapacityLocked(1);
        const sequences = try driver.publication_sequencer.prepare(1);
        var reservation = driver.queue.reserve(1) catch unreachable;
        reservation.at(0).* = .{
            .acceptance_id = acceptance_id,
            .sequence = sequences.first,
            .mode = mode,
            .command = command,
            .completion = .{ .callback = .{
                .request_id = request_id,
                .callback = callback,
            } },
            .kind = .asynchronous,
        };
        driver.publication_sequencer.commit(sequences);
        driver.queue.commit(reservation);
        if (self.journal != null) driver.journal_slots_reserved += 1;

        self.next_request_id = request_id;
        driver.callback_completions_pending += 1;
        driver.accepted_total = acceptance_id;
        driver.async_accepted_total += 1;
        driver.work_available.broadcast(driver.io);
    }

    fn enqueueDetachedBatch(
        self: *ExchangeApiSharded,
        commands: []const cmd.OrderCommand,
        mode: ExecutionMode,
        kind: WorkKind,
    ) EnqueueRuntimeError!void {
        self.assertWiring();
        std.debug.assert(kind == .detached_api or kind == .detached_api_raw or kind == .runtime);
        if (commands.len == 0) return;

        const driver = self.driver;
        if (commands.len > driver.queue.capacity()) return error.CoreQueueFull;
        try self.lockPublicationDriver();
        defer driver.mutex.unlock(driver.io);
        try self.waitForColdPublisherLocked();
        try self.waitForCapacityLocked(commands.len, 0);

        const count_u64 = std.math.cast(u64, commands.len) orelse return error.CoreQueueFull;
        const accepted_after = std.math.add(
            u64,
            driver.accepted_total,
            count_u64,
        ) catch return error.CoreQueueFull;
        try self.ensureJournalCapacityLocked(commands.len);
        const sequences = try driver.publication_sequencer.prepare(commands.len);
        var reservation = driver.queue.reserve(commands.len) catch unreachable;
        for (commands, 0..) |command, index| {
            const acceptance_offset = std.math.cast(u64, index) orelse unreachable;
            reservation.at(index).* = .{
                .acceptance_id = driver.accepted_total + acceptance_offset + 1,
                .sequence = sequences.at(index),
                .mode = mode,
                .command = command,
                .completion = .detached,
                .kind = kind,
            };
        }
        driver.publication_sequencer.commit(sequences);
        driver.queue.commit(reservation);
        if (self.journal != null) driver.journal_slots_reserved += commands.len;
        driver.accepted_total = accepted_after;
        if (kind == .runtime) driver.runtime_accepted_total += count_u64;
        driver.work_available.broadcast(driver.io);
    }

    /// Called with the driver mutex held. The queue cannot expose new work
    /// while journal storage and publication identities are being prepared.
    fn ensureJournalCapacityLocked(
        self: *ExchangeApiSharded,
        additional: usize,
    ) (LifecycleError || std.mem.Allocator.Error)!void {
        if (self.journal) |*journal| {
            if (self.journal_capacity_test_gate) |gate| gate.markReached();
            if (runtime_mod.isCallbackExecutionActive() or self.isWorkerThread()) {
                if (!self.execution_mutex.tryLock()) {
                    return error.CoreReentrantCall;
                }
            } else {
                self.execution_mutex.lockUncancelable(self.driver.io);
            }
            defer self.execution_mutex.unlock(self.driver.io);
            const required = std.math.add(
                usize,
                self.driver.journal_slots_reserved,
                additional,
            ) catch return error.OutOfMemory;
            try journal.entries.ensureUnusedCapacity(self.allocator, required);
        }
    }

    fn waitForCapacityLocked(
        self: *ExchangeApiSharded,
        queue_slots_needed: usize,
        completion_slots_needed: usize,
    ) LifecycleError!void {
        const driver = self.driver;
        const capacity = driver.queue.capacity();
        if (queue_slots_needed > capacity or completion_slots_needed > capacity) {
            return error.CoreQueueFull;
        }
        std.debug.assert(driver.completion_slots_used <= capacity);
        while (true) {
            try self.waitForPublicationGateLocked();
            if (driver.queue.remainingCapacity() >= queue_slots_needed and
                capacity - driver.completion_slots_used >= completion_slots_needed)
            {
                break;
            }
            try self.ensureAcceptingLocked();
            if (runtime_mod.isCallbackExecutionActive() or self.isWorkerThread()) {
                return error.CoreReentrantCall;
            }
            if (driver.lifecycle.current() == .cold) return error.CoreQueueFull;
            driver.blocked_producers += 1;
            driver.completion_available.broadcast(driver.io);
            switch (self.config.runtime.wait_strategy) {
                .blocking => driver.space_available.waitUncancelable(driver.io, &driver.mutex),
                .yielding => {
                    driver.mutex.unlock(driver.io);
                    std.Thread.yield() catch {};
                    driver.mutex.lockUncancelable(driver.io);
                },
                .busy_spin => {
                    driver.mutex.unlock(driver.io);
                    std.atomic.spinLoopHint();
                    driver.mutex.lockUncancelable(driver.io);
                },
                .second_step_no_wait => {
                    driver.blocked_producers -= 1;
                    return error.CoreQueueFull;
                },
            }
            driver.blocked_producers -= 1;
        }
        // A stream may win the mutex after the final capacity wake but before
        // this publisher reacquires it. Recheck the gate at commit frontier.
        try self.waitForPublicationGateLocked();
        try self.ensureAcceptingLocked();
    }

    /// Later publishers cannot reserve a sequence range or enter the FIFO
    /// between chunks of an already-admitted binary transfer.
    fn waitForPublicationGateLocked(self: *ExchangeApiSharded) LifecycleError!void {
        const driver = self.driver;
        while (driver.active_binary_stream) {
            try self.ensureAcceptingLocked();
            if (runtime_mod.isCallbackExecutionActive() or self.isWorkerThread()) {
                return error.CoreReentrantCall;
            }
            driver.blocked_producers += 1;
            driver.completion_available.broadcast(driver.io);
            switch (self.config.runtime.wait_strategy) {
                .blocking => driver.space_available.waitUncancelable(driver.io, &driver.mutex),
                .yielding => {
                    driver.mutex.unlock(driver.io);
                    std.Thread.yield() catch {};
                    driver.mutex.lockUncancelable(driver.io);
                },
                .busy_spin => {
                    driver.mutex.unlock(driver.io);
                    std.atomic.spinLoopHint();
                    driver.mutex.lockUncancelable(driver.io);
                },
                .second_step_no_wait => {
                    driver.blocked_producers -= 1;
                    return error.CoreQueueFull;
                },
            }
            driver.blocked_producers -= 1;
        }
        try self.ensureAcceptingLocked();
    }

    /// Closing does not cancel an admitted stream: shutdown waits for its
    /// terminal frame. Fatal poison is the only truncation path.
    fn waitForBinaryChunkCapacityLocked(
        self: *ExchangeApiSharded,
        chunk_len: usize,
    ) LifecycleError!void {
        const driver = self.driver;
        std.debug.assert(driver.active_binary_stream);
        std.debug.assert(chunk_len > 0 and chunk_len <= driver.queue.capacity() / 4);
        while (driver.queue.remainingCapacity() < chunk_len) {
            switch (driver.lifecycle.current()) {
                .running, .closing => {},
                .poisoned => return error.CorePoisoned,
                .stopped => return error.CoreStopped,
                .cold => unreachable,
            }
            if (runtime_mod.isCallbackExecutionActive() or self.isWorkerThread()) {
                return error.CoreReentrantCall;
            }
            driver.blocked_producers += 1;
            driver.completion_available.broadcast(driver.io);
            switch (self.config.runtime.wait_strategy) {
                .blocking => driver.space_available.waitUncancelable(driver.io, &driver.mutex),
                .yielding, .second_step_no_wait => {
                    driver.mutex.unlock(driver.io);
                    std.Thread.yield() catch {};
                    driver.mutex.lockUncancelable(driver.io);
                },
                .busy_spin => {
                    driver.mutex.unlock(driver.io);
                    std.atomic.spinLoopHint();
                    driver.mutex.lockUncancelable(driver.io);
                },
            }
            driver.blocked_producers -= 1;
        }
        switch (driver.lifecycle.current()) {
            .running, .closing => {},
            .poisoned => return error.CorePoisoned,
            .stopped => return error.CoreStopped,
            .cold => unreachable,
        }
    }

    fn waitForColdPublisherLocked(self: *ExchangeApiSharded) LifecycleError!void {
        const driver = self.driver;
        try self.ensureAcceptingLocked();
        if (driver.lifecycle.current() == .cold and
            driver.bootstrap_owner != null)
        {
            return error.CoreNotStarted;
        }
        while (driver.lifecycle.current() == .cold and driver.active_cold_calls != 0) {
            if (runtime_mod.isCallbackExecutionActive() or self.isWorkerThread()) {
                return error.CoreReentrantCall;
            }
            try self.ensureAcceptingLocked();
            driver.completion_available.waitUncancelable(driver.io, &driver.mutex);
        }
        try self.ensureAcceptingLocked();
        if (driver.lifecycle.current() == .cold and
            driver.bootstrap_owner != null)
        {
            return error.CoreNotStarted;
        }
    }

    fn ensureAcceptingLocked(self: *const ExchangeApiSharded) LifecycleError!void {
        return switch (self.driver.lifecycle.current()) {
            .cold, .running => if (self.driver.close_requested.load(.acquire))
                error.CoreStopped
            else {},
            .closing, .stopped => error.CoreStopped,
            .poisoned => error.CorePoisoned,
        };
    }

    fn beginBootstrap(self: *ExchangeApiSharded) LifecycleError!void {
        self.assertWiring();
        try self.ensureBlockingCallAllowed();
        const driver = self.driver;
        const current_thread = std.Thread.getCurrentId();
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);

        while (driver.bootstrap_owner != null or
            driver.manual_drain_active or
            driver.active_cold_calls != 0)
        {
            if (driver.bootstrap_owner == current_thread) return error.CoreReentrantCall;
            driver.completion_available.waitUncancelable(driver.io, &driver.mutex);
        }
        const lifecycle = driver.lifecycle.current();
        switch (lifecycle) {
            .running => return error.CoreAlreadyStarted,
            .poisoned => return error.CorePoisoned,
            .closing, .stopped => return error.CoreStopped,
            .cold => {},
        }
        if (self.settlement_hooks.isEnabled() and self.settlement_recovery_finished) {
            return error.CoreAlreadyStarted;
        }
        if (driver.close_requested.load(.acquire)) return error.CoreStopped;
        if (!driver.queue.isEmpty() or
            driver.active_binary_stream or
            driver.prepared_available_remaining != 0 or
            driver.accepted_total != driver.settled_acceptance_frontier)
        {
            return error.CoreNotStarted;
        }
        driver.bootstrap_owner = current_thread;
    }

    fn finishBootstrap(self: *ExchangeApiSharded) void {
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        std.debug.assert(driver.bootstrap_owner == std.Thread.getCurrentId());
        driver.bootstrap_owner = null;
        driver.completion_available.broadcast(driver.io);
        driver.mutex.unlock(driver.io);
    }

    fn takeReadyResultLocked(
        self: *ExchangeApiSharded,
        request_id: u64,
    ) ?kernel_mod.CommandResponse {
        const removed = self.ready_results.fetchRemove(request_id) orelse return null;
        std.debug.assert(self.driver.completion_slots_used > 0);
        self.driver.completion_slots_used -= 1;
        self.driver.space_available.broadcast(self.driver.io);
        return removed.value;
    }

    /// Best-effort cleanup for a synchronous custom-report wrapper that exits
    /// before normal reduction. It removes a ready response and the retained
    /// query independently, and never runs user reducer code.
    fn discardCustomReportRequest(
        self: *ExchangeApiSharded,
        pending: custom_report_pipeline.PendingRequest,
    ) void {
        const driver = self.driver;
        const owner_identity: *const anyopaque = @ptrCast(driver);
        driver.mutex.lockUncancelable(driver.io);
        const request_id = self.custom_report_requests.pendingRequestId(
            pending,
            owner_identity,
        ) catch {
            driver.mutex.unlock(driver.io);
            return;
        };
        const removed = self.custom_ready_results.fetchRemove(request_id);
        if (removed != null) {
            std.debug.assert(driver.completion_slots_used > 0);
            driver.completion_slots_used -= 1;
            driver.space_available.broadcast(driver.io);
        }
        driver.mutex.unlock(driver.io);

        if (removed) |entry| {
            var response = entry.value.response;
            response.deinit(self.allocator);
        }
        self.custom_report_requests.failPending(
            pending,
            owner_identity,
        ) catch {};
    }

    fn claimedCompletionCountLocked(self: *const ExchangeApiSharded) usize {
        return self.ready_results.count() + self.custom_ready_results.count();
    }

    /// Resolves the one retained poll owner for a publication sequence. The
    /// driver mutex must be held. Fatal publication copies this identity into
    /// the immutable fatal record before ready-result cleanup can erase it.
    fn readyRequestIdForSequenceLocked(
        self: *ExchangeApiSharded,
        sequence: u64,
    ) ?u64 {
        var iterator = self.ready_results.iterator();
        while (iterator.next()) |entry| {
            if (entry.value_ptr.sequence == sequence) return entry.key_ptr.*;
        }
        return null;
    }

    fn waitTakeResponse(
        self: *ExchangeApiSharded,
        request_id: u64,
    ) LifecycleError!kernel_mod.CommandResponse {
        try self.ensureBlockingCallAllowed();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        while (true) {
            if (self.takeReadyResultLocked(request_id)) |response| return response;
            switch (driver.lifecycle.current()) {
                .poisoned => return error.CorePoisoned,
                .stopped => return error.CoreStopped,
                .cold => return error.CoreNotStarted,
                .running, .closing => {},
            }
            driver.completion_available.waitUncancelable(driver.io, &driver.mutex);
        }
    }

    fn waitForAcceptedPrefix(self: *ExchangeApiSharded) LifecycleError!bool {
        self.assertWiring();
        try self.ensureBlockingCallAllowed();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        try self.ensureAcceptingLocked();
        while (driver.lifecycle.current() == .cold and driver.active_cold_calls != 0) {
            driver.completion_available.waitUncancelable(driver.io, &driver.mutex);
            try self.ensureAcceptingLocked();
        }
        if (driver.lifecycle.current() == .cold) {
            if (driver.bootstrap_owner != null or
                !driver.queue.isEmpty() or
                driver.manual_drain_active)
            {
                return error.CoreNotStarted;
            }
            driver.active_cold_calls += 1;
            return true;
        }

        const target = driver.accepted_total;
        while (driver.settled_acceptance_frontier < target) {
            if (driver.lifecycle.current() == .poisoned) return error.CorePoisoned;
            if (driver.lifecycle.current() == .stopped) return error.CoreStopped;
            driver.completion_available.waitUncancelable(driver.io, &driver.mutex);
        }
        if (driver.lifecycle.current() == .poisoned) return error.CorePoisoned;
        return false;
    }

    fn waitForAcceptedPrefixAndLockBatchFence(
        self: *ExchangeApiSharded,
    ) LifecycleError!bool {
        const cold_claimed = try self.waitForAcceptedPrefix();
        if (self.accepted_prefix_barrier_test_gate) |gate| gate.pauseOnce();
        // Never wait for this fence while holding `driver.mutex`: grouped
        // barrier poison retains the fence while it temporarily publishes its
        // fatal record under driver ownership.
        self.batch_barrier_fence.lockUncancelable(self.driver.io);
        return cold_claimed;
    }

    fn isRawFidelityKind(kind: WorkKind) bool {
        return switch (kind) {
            .asynchronous_raw, .synchronous_raw, .detached_api_raw => true,
            .asynchronous, .synchronous, .detached_api, .runtime => false,
        };
    }

    fn persistentGroupedBatchCandidateCount(
        queue: *const ShardedWorkQueue,
        prepared_count: usize,
        max_batch_size: usize,
    ) usize {
        if (prepared_count == 0 or max_batch_size < 2) return 0;
        const first = queue.peekConst() orelse return 0;
        if (!sharded_pipeline.isOrdinaryCommand(first.command.command)) return 0;
        const events_group = first.command.events_group;
        const limit = @min(prepared_count, max_batch_size);
        var count: usize = 0;
        while (count < limit) : (count += 1) {
            const item = queue.atConst(count) orelse unreachable;
            if (!sharded_pipeline.isOrdinaryCommand(item.command.command) or
                item.command.events_group != events_group)
            {
                break;
            }
        }
        return if (count >= 2) count else 0;
    }

    /// Captures the one currently available Java sequence range without
    /// extracting it from the queue. New publications append behind the
    /// prepared frontier and form the next range once this one is consumed.
    /// The driver mutex must be held.
    fn prepareAvailableGroupingBatchLocked(self: *ExchangeApiSharded) void {
        const driver = self.driver;
        std.debug.assert(driver.prepared_available_remaining == 0);
        const available_count = driver.queue.count();
        std.debug.assert(available_count > 0);

        var saw_live_command = false;
        for (0..available_count) |index| {
            const item = driver.queue.at(index).?;
            if (isRawFidelityKind(item.kind)) continue;
            _ = self.grouping.stampLive(&item.command);
            saw_live_command = true;
        }
        if (saw_live_command) {
            self.grouping.finishAvailableBatch(self.grouping_clock.nowNs());
        }
        driver.prepared_available_remaining = available_count;
    }

    /// Pops one slot from a grouping-prepared available range. The driver
    /// mutex must be held and the queue must be nonempty.
    fn popPreparedWorkLocked(self: *ExchangeApiSharded) PreparedShardedWork {
        const driver = self.driver;
        if (driver.prepared_available_remaining == 0) {
            self.prepareAvailableGroupingBatchLocked();
        }
        std.debug.assert(driver.prepared_available_remaining > 0);
        const item = driver.queue.pop().?;
        driver.prepared_available_remaining -= 1;
        return .{
            .item = item,
            .journal_batch_position = if (driver.prepared_available_remaining == 0)
                .terminal
            else
                .continues,
        };
    }

    /// Speculatively admits the already-stamped queue-head prefix without
    /// removing completion ownership from the driver FIFO. Restricting the
    /// batch to one events group prevents admission from exposing its closing
    /// boundary before every prefix callback/result has been delivered. A
    /// durable append can fail, so durable-journal execution deliberately
    /// retains singleton mutation-before-tail ordering.
    ///
    /// The driver mutex is held throughout this call. A return of zero means
    /// the caller must use the existing singleton path; batch preflight is
    /// mutation-free for all non-fatal admission failures.
    const PersistentBatchAdmission = union(enum) {
        none,
        capacity,
        admitted: usize,
    };

    fn admitPreparedPersistentBatchLocked(
        self: *ExchangeApiSharded,
    ) anyerror!PersistentBatchAdmission {
        const driver = self.driver;
        const runtime = self.persistent_grouped_runtime orelse return .none;
        if (self.durable_journal != null) return .none;
        if (driver.prepared_available_remaining == 0) {
            self.prepareAvailableGroupingBatchLocked();
        }

        const count = persistentGroupedBatchCandidateCount(
            &driver.queue,
            driver.prepared_available_remaining,
            driver.grouped_submissions.len,
        );
        if (count == 0) return .none;
        for (0..count) |index| {
            const item = driver.queue.atConst(index) orelse unreachable;
            driver.grouped_submissions[index] = .{
                .sequence = item.sequence,
                .command = &item.command,
            };
        }

        // Do not wait for the batch fence under driver ownership. An external
        // barrier may retain it while publishing poison under `driver.mutex`.
        if (!self.batch_barrier_fence.tryLock()) return .none;
        // Publication-side capacity reservation establishes the canonical
        // driver -> execution lock order used here.
        self.execution_mutex.lockUncancelable(driver.io);
        runtime.submitBatchAtSequence(driver.grouped_submissions[0..count]) catch |err| {
            self.execution_mutex.unlock(driver.io);
            self.batch_barrier_fence.unlock(driver.io);
            if (err == error.SlotNotReclaimed) {
                if (self.persistent_capacity_test_gate) |gate| gate.markReached();
                return .capacity;
            }
            return err;
        };
        self.execution_mutex.unlock(driver.io);
        driver.grouped_batch_high_watermark = @max(
            driver.grouped_batch_high_watermark,
            count,
        );
        // The caller owns `batch_barrier_fence` until every admitted item has
        // completed observer/callback delivery and outer driver settlement.
        return .{ .admitted = count };
    }

    /// Mirrors GroupingProcessor's no-event branch. Callers must hold the
    /// driver mutex and prove that no work is currently available.
    fn observeGroupingIdleLocked(self: *ExchangeApiSharded) void {
        std.debug.assert(self.driver.queue.isEmpty());
        std.debug.assert(self.driver.prepared_available_remaining == 0);
        self.grouping.observeIdle(self.grouping_clock.nowNs());
    }

    fn assertDriverWorkSettledLocked(driver: *const RuntimeDriverState) void {
        std.debug.assert(!driver.active_binary_stream);
        std.debug.assert(driver.queue.isEmpty());
        std.debug.assert(driver.prepared_available_remaining == 0);
        std.debug.assert(driver.accepted_total == driver.completed_total);
        std.debug.assert(
            driver.accepted_total == driver.settled_acceptance_frontier,
        );
        std.debug.assert(driver.deferred_accepted_settlement == null);
        std.debug.assert(
            driver.async_accepted_total == driver.async_completed_total,
        );
        std.debug.assert(
            driver.runtime_accepted_total == driver.runtime_completed_total,
        );
        std.debug.assert(driver.journal_slots_reserved == 0);
        std.debug.assert(driver.callback_completions_pending == 0);
    }

    fn workerMain(driver: *RuntimeDriverState) void {
        const worker_owner = driver.owner.?;
        const previous_worker_api = active_worker_api;
        active_worker_api = worker_owner;
        defer active_worker_api = previous_worker_api;

        driver.mutex.lockUncancelable(driver.io);
        driver.worker_id = std.Thread.getCurrentId();
        driver.mutex.unlock(driver.io);
        defer {
            worker_owner.finalizeDriverExit();

            driver.mutex.lockUncancelable(driver.io);
            driver.owner = null;
            driver.worker_id = null;
            driver.completion_available.broadcast(driver.io);
            driver.space_available.broadcast(driver.io);
            driver.mutex.unlock(driver.io);
            driver.worker_quiesced.set(driver.io);
        }

        driver.mutex.lockUncancelable(driver.io);
        while (driver.active_cold_calls != 0 or
            driver.manual_drain_active or
            driver.bootstrap_owner != null)
        {
            driver.completion_available.waitUncancelable(
                driver.io,
                &driver.mutex,
            );
        }
        driver.mutex.unlock(driver.io);

        while (true) {
            driver.mutex.lockUncancelable(driver.io);
            if (driver.close_requested.load(.acquire) and
                driver.lifecycle.current() == .running)
            {
                _ = driver.lifecycle.beginClose() catch unreachable;
                driver.work_available.broadcast(driver.io);
                driver.space_available.broadcast(driver.io);
                driver.completion_available.broadcast(driver.io);
            }
            if (driver.lifecycle.current() == .poisoned and
                !driver.queue.isEmpty())
            {
                const fatal = driver.lifecycle.fatalRecord().?.*;
                driver.prepared_available_remaining = 0;
                const skipped = driver.queue.pop().?;
                driver.space_available.broadcast(driver.io);
                driver.mutex.unlock(driver.io);
                worker_owner.settleFatalWorkItem(
                    skipped,
                    .skipped_after_fatal,
                    fatal,
                );
                continue;
            }
            while (driver.queue.isEmpty()) {
                if (driver.close_requested.load(.acquire) and
                    driver.lifecycle.current() == .running)
                {
                    _ = driver.lifecycle.beginClose() catch unreachable;
                    driver.work_available.broadcast(driver.io);
                    driver.space_available.broadcast(driver.io);
                    driver.completion_available.broadcast(driver.io);
                }
                switch (driver.lifecycle.current()) {
                    .closing => if (!driver.active_binary_stream) {
                        assertDriverWorkSettledLocked(driver);
                        driver.completion_available.broadcast(driver.io);
                        driver.mutex.unlock(driver.io);
                        return;
                    },
                    .poisoned => {
                        // An admitted binary publisher owns accounting for
                        // any reserved suffix that never reached the queue.
                        // Keep the worker/join frontier open until that owner
                        // observes poison and discharges the suffix.
                        if (driver.active_binary_stream) {
                            driver.completion_available.waitUncancelable(
                                driver.io,
                                &driver.mutex,
                            );
                            continue;
                        }
                        // A late fatal registration or concurrent poll sweep
                        // can own a callback after queue/accounting settlement.
                        // Keep the worker alive until that externally
                        // dispatched callback releases its ownership; exiting
                        // here would race finalization's quiescence invariant.
                        if (driver.callback_completions_pending != 0 or
                            driver.fatal_poll_settlements_pending != 0)
                        {
                            driver.completion_available.waitUncancelable(
                                driver.io,
                                &driver.mutex,
                            );
                            continue;
                        }
                        assertDriverWorkSettledLocked(driver);
                        driver.completion_available.broadcast(driver.io);
                        driver.mutex.unlock(driver.io);
                        return;
                    },
                    .stopped => {
                        assertDriverWorkSettledLocked(driver);
                        driver.completion_available.broadcast(driver.io);
                        driver.mutex.unlock(driver.io);
                        return;
                    },
                    .cold => unreachable,
                    .running => {},
                }
                switch (driver.owner.?.config.runtime.wait_strategy) {
                    .blocking => driver.work_available.waitUncancelable(driver.io, &driver.mutex),
                    .yielding, .second_step_no_wait => {
                        driver.owner.?.observeGroupingIdleLocked();
                        driver.mutex.unlock(driver.io);
                        std.Thread.yield() catch {};
                        driver.mutex.lockUncancelable(driver.io);
                    },
                    .busy_spin => {
                        driver.owner.?.observeGroupingIdleLocked();
                        driver.mutex.unlock(driver.io);
                        std.atomic.spinLoopHint();
                        driver.mutex.lockUncancelable(driver.io);
                    },
                }
            }

            const owner = driver.owner.?;
            const batch_admission = owner.admitPreparedPersistentBatchLocked() catch |err| {
                const failed = owner.popPreparedWorkLocked();
                driver.space_available.broadcast(driver.io);
                driver.mutex.unlock(driver.io);
                owner.poisonFromWork(failed.item, err);
                driver.mutex.lockUncancelable(driver.io);
                while (driver.active_binary_stream) {
                    driver.completion_available.waitUncancelable(
                        driver.io,
                        &driver.mutex,
                    );
                }
                driver.mutex.unlock(driver.io);
                return;
            };
            switch (batch_admission) {
                .none => {},
                .capacity => {
                    // The prior inner range has already completed journal/result
                    // transfer, observer/callback delivery, and outer settlement:
                    // this sole consumer has not popped the next item. Release
                    // driver ownership before waiting for terminal R2/reclaim so
                    // grouped poison can publish without lock inversion.
                    driver.mutex.unlock(driver.io);
                    owner.execution_mutex.lockUncancelable(driver.io);
                    owner.settlePersistentGroupedOwned() catch {};
                    owner.execution_mutex.unlock(driver.io);
                    continue;
                },
                .admitted => |grouped_batch_count| {
                    driver.mutex.unlock(driver.io);
                    var batch_index: usize = 0;
                    while (batch_index < grouped_batch_count) : (batch_index += 1) {
                        driver.mutex.lockUncancelable(driver.io);
                        const grouped_item = owner.popPreparedWorkLocked();
                        driver.space_available.broadcast(driver.io);
                        driver.mutex.unlock(driver.io);
                        owner.processAdmittedPersistentItem(grouped_item) catch |err| {
                            owner.poisonFromWork(grouped_item.item, err);
                            // Keep the fence through poison callback/tail
                            // settlement; the admitted batch is not externally
                            // quiescent before that ownership is discharged.
                            owner.batch_barrier_fence.unlock(driver.io);
                            driver.mutex.lockUncancelable(driver.io);
                            while (driver.active_binary_stream) {
                                driver.completion_available.waitUncancelable(
                                    driver.io,
                                    &driver.mutex,
                                );
                            }
                            driver.mutex.unlock(driver.io);
                            return;
                        };
                    }
                    owner.batch_barrier_fence.unlock(driver.io);
                    continue;
                },
            }
            const prepared = owner.popPreparedWorkLocked();
            driver.space_available.broadcast(driver.io);
            driver.mutex.unlock(driver.io);
            if (owner.available_batch_test_gate) |gate| gate.pauseOnce();

            owner.processQueuedItem(prepared) catch |err| {
                owner.poisonFromWork(prepared.item, err);
                // An admitted producer owns settlement of a suffix that never
                // reached the queue. Keep the worker/join frontier open until
                // that ownership has been discharged.
                driver.mutex.lockUncancelable(driver.io);
                while (driver.active_binary_stream) {
                    driver.completion_available.waitUncancelable(driver.io, &driver.mutex);
                }
                driver.mutex.unlock(driver.io);
                return;
            };
        }
    }

    /// The sole running consumer owns graceful finalization. This lets
    /// callback-initiated shutdown return from the callback without self-join
    /// while still guaranteeing that the same driver settles the terminal
    /// grouped R2 frontier, tears down the runtime/core, and publishes
    /// `.stopped` before it exits. External shutdown callers only join.
    fn finalizeDriverExit(self: *ExchangeApiSharded) void {
        const driver = self.driver;
        self.execution_mutex.lockUncancelable(driver.io);
        self.settlePersistentGroupedOwned() catch |err| {
            // A grouped stage can surface its first immutable fatal only when
            // finalization closes the last group. The poison record is the
            // authoritative outcome.
            if (err != error.CorePoisoned) {
                log.err("persistent grouped driver finalization failed err={}", .{err});
            }
        };
        self.stopPersistentGroupedRuntime();
        self.core.shutdown();
        self.execution_mutex.unlock(driver.io);
        self.settleReadyRegisteredCallbacksAfterFatal();
        self.waitForFatalCallbackDispatches();

        driver.mutex.lockUncancelable(driver.io);
        std.debug.assert(driver.fatal_poll_settlements_pending == 0);
        if (driver.lifecycle.current() != .poisoned) {
            _ = driver.lifecycle.markStopped() catch unreachable;
        }
        driver.completion_available.broadcast(driver.io);
        driver.space_available.broadcast(driver.io);
        driver.work_available.broadcast(driver.io);
        driver.mutex.unlock(driver.io);
    }

    fn processQueuedItem(
        self: *ExchangeApiSharded,
        prepared: PreparedShardedWork,
    ) anyerror!void {
        return self.processQueuedItemWithAdmission(prepared, false);
    }

    fn processAdmittedPersistentItem(
        self: *ExchangeApiSharded,
        prepared: PreparedShardedWork,
    ) anyerror!void {
        return self.processQueuedItemWithAdmission(prepared, true);
    }

    fn processQueuedItemWithAdmission(
        self: *ExchangeApiSharded,
        prepared: PreparedShardedWork,
        already_admitted: bool,
    ) anyerror!void {
        const item = prepared.item;
        var command = item.command;
        self.execution_mutex.lockUncancelable(self.driver.io);
        if (self.persistent_grouped_activated and
            self.persistent_grouped_runtime == null)
        {
            self.execution_mutex.unlock(self.driver.io);
            return error.CorePoisoned;
        }
        var response = blk: {
            if (self.persistent_grouped_runtime) |runtime| {
                if (sharded_pipeline.isOrdinaryCommand(command.command)) {
                    const grouped_response = if (already_admitted)
                        self.completePersistentGroupedAtSequence(
                            runtime,
                            item.mode,
                            item.sequence,
                            &command,
                            prepared.journal_batch_position,
                        )
                    else
                        self.executePersistentGroupedAtSequence(
                            runtime,
                            item.mode,
                            item.sequence,
                            &command,
                            prepared.journal_batch_position,
                        );
                    break :blk grouped_response catch |err| {
                        self.execution_mutex.unlock(self.driver.io);
                        return err;
                    };
                }
                std.debug.assert(!already_admitted);
                // Global/broadcast work executes exactly once only after every
                // prior grouped stage has terminally settled and reclaimed.
                self.settlePersistentGroupedOwned() catch |err| {
                    self.execution_mutex.unlock(self.driver.io);
                    return err;
                };
            }
            std.debug.assert(!already_admitted);
            break :blk self.executeWithModeDirectAtSequence(
                item.mode,
                item.sequence,
                &command,
                prepared.journal_batch_position,
            ) catch |err| {
                self.execution_mutex.unlock(self.driver.io);
                return err;
            };
        };
        if (response.sequence != item.sequence.toInt()) {
            self.execution_mutex.unlock(self.driver.io);
            response.deinit(self.allocator);
            return error.GroupedSequenceMismatch;
        }
        self.execution_mutex.unlock(self.driver.io);
        const result_code = response.result_code;
        const runtime_event: ?runtime_mod.ResultEvent = if (item.kind == .runtime)
            .{
                .sequence = response.sequence,
                .command = command.command,
                .uid = command.uid,
                .symbol = command.symbol,
                .result_code = result_code,
            }
        else
            null;
        self.publishCompletionObservers(&command, &response, runtime_event);

        switch (item.completion) {
            .poll => |request_id| {
                self.driver.mutex.lockUncancelable(self.driver.io);
                std.debug.assert(self.driver.completion_slots_used > self.claimedCompletionCountLocked());
                self.ready_results.putAssumeCapacityNoClobber(request_id, response);
                self.finishWorkLocked(item, result_code);
                self.driver.mutex.unlock(self.driver.io);
            },
            .custom_report => |owner| {
                self.driver.mutex.lockUncancelable(self.driver.io);
                std.debug.assert(self.driver.completion_slots_used > self.claimedCompletionCountLocked());
                self.custom_ready_results.putAssumeCapacityNoClobber(
                    owner.request_id,
                    .{ .pending = owner.pending, .response = response },
                );
                self.finishWorkLocked(item, result_code);
                self.driver.mutex.unlock(self.driver.io);
            },
            .detached => {
                response.deinit(self.allocator);
                self.driver.mutex.lockUncancelable(self.driver.io);
                self.finishWorkLocked(item, result_code);
                self.driver.mutex.unlock(self.driver.io);
            },
            .callback => |owner| {
                self.dispatchCommandResponse(owner.callback, &response);
                response.deinit(self.allocator);
                self.driver.mutex.lockUncancelable(self.driver.io);
                std.debug.assert(self.driver.callback_completions_pending > 0);
                self.driver.callback_completions_pending -= 1;
                self.finishWorkLocked(item, result_code);
                self.driver.mutex.unlock(self.driver.io);
            },
        }
    }

    fn finishWorkLocked(
        self: *ExchangeApiSharded,
        item: ShardedWorkItem,
        result_code: cmd.CommandResultCode,
    ) void {
        const driver = self.driver;
        if (self.journal != null) {
            std.debug.assert(driver.journal_slots_reserved > 0);
            driver.journal_slots_reserved -= 1;
        }
        self.settleAcceptedRangeLocked(item.acceptance_id, 1);
        switch (item.kind) {
            .asynchronous, .asynchronous_raw => driver.async_completed_total += 1,
            .runtime => {
                driver.runtime_completed_total += 1;
                driver.last_runtime_result = result_code;
            },
            .synchronous, .synchronous_raw, .detached_api, .detached_api_raw => {},
        }
        driver.completion_available.broadcast(driver.io);
    }

    /// Records terminal ownership settlement without allowing a later,
    /// out-of-order binary suffix to impersonate an earlier accepted prefix.
    /// The single consumer settles individual identities in FIFO order; fatal
    /// binary truncation can contribute one later contiguous range.
    fn settleAcceptedRangeLocked(
        self: *ExchangeApiSharded,
        first: u64,
        count: u64,
    ) void {
        const driver = self.driver;
        std.debug.assert(count > 0);
        const last = std.math.add(u64, first, count - 1) catch unreachable;
        std.debug.assert(last <= driver.accepted_total);
        driver.completed_total = std.math.add(
            u64,
            driver.completed_total,
            count,
        ) catch unreachable;

        const next = std.math.add(
            u64,
            driver.settled_acceptance_frontier,
            1,
        ) catch unreachable;
        if (first == next) {
            driver.settled_acceptance_frontier = last;
            if (driver.deferred_accepted_settlement) |deferred| {
                const after_prefix = std.math.add(
                    u64,
                    driver.settled_acceptance_frontier,
                    1,
                ) catch unreachable;
                std.debug.assert(deferred.first >= after_prefix);
                if (deferred.first == after_prefix) {
                    driver.settled_acceptance_frontier = deferred.last;
                    driver.deferred_accepted_settlement = null;
                }
            }
            driver.completion_available.broadcast(driver.io);
            return;
        }

        std.debug.assert(first > next);
        if (driver.deferred_accepted_settlement) |deferred| {
            // Poison fences admission, so two disjoint out-of-order ranges are
            // impossible. Accept adjacency only to keep this helper total if a
            // binary publisher reports its suffix in bounded pieces later.
            std.debug.assert(
                last >= deferred.first - 1 and
                    deferred.last >= first - 1,
            );
            driver.deferred_accepted_settlement = .{
                .first = @min(first, deferred.first),
                .last = @max(last, deferred.last),
            };
        } else {
            driver.deferred_accepted_settlement = .{
                .first = first,
                .last = last,
            };
        }
        driver.completion_available.broadcast(driver.io);
    }

    fn poisonFromWork(
        self: *ExchangeApiSharded,
        failed_item: ShardedWorkItem,
        failure: anyerror,
    ) void {
        // Runtime destruction is serialized by execution_mutex. Snapshot the
        // immutable grouped fatal under that ownership, but release it before
        // taking driver.mutex so journal admission keeps its driver ->
        // execution order and cannot form an ABBA cycle.
        self.execution_mutex.lockUncancelable(self.driver.io);
        const grouped_fatal = if (self.persistent_grouped_runtime) |runtime|
            runtime.fatalInfo()
        else
            null;
        self.execution_mutex.unlock(self.driver.io);
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        const failed_sequence = self.sequenceForWork(failed_item);
        const proposed_request_id = if (grouped_fatal) |inner|
            if (failed_sequence == inner.sequence)
                failed_item.completion.requestId()
            else
                self.readyRequestIdForSequenceLocked(inner.sequence)
        else
            failed_item.completion.requestId();
        const proposed_fatal: runtime_mod.FatalRecord = .{
            .stage = if (grouped_fatal) |inner|
                groupedFailureStage(inner.stage)
            else
                failureStage(failure),
            .sequence = if (grouped_fatal) |inner|
                inner.sequence
            else
                failed_sequence,
            .request_id = proposed_request_id,
            .command = if (grouped_fatal) |inner|
                inner.command
            else
                failed_item.command.command,
            .cause = if (grouped_fatal) |inner| inner.cause else failure,
        };
        const first_poison = driver.lifecycle.poison(proposed_fatal);
        const fatal = if (driver.lifecycle.fatalRecord()) |record|
            record.*
        else {
            driver.mutex.unlock(driver.io);
            return;
        };
        driver.prepared_available_remaining = 0;
        driver.work_available.broadcast(driver.io);
        driver.space_available.broadcast(driver.io);
        driver.completion_available.broadcast(driver.io);
        driver.mutex.unlock(driver.io);

        if (first_poison) {
            self.execution_mutex.lockUncancelable(driver.io);
            self.stopPersistentGroupedRuntime();
            self.core.shutdown();
            self.execution_mutex.unlock(driver.io);
        }

        const failed_request_id = failed_item.completion.requestId();
        const failure_kind: CallbackFailureKind =
            if (if (fatal.request_id) |fatal_request_id|
                failed_request_id == fatal_request_id
            else
                fatal.sequence != null and fatal.sequence == failed_sequence)
                .command_failed
            else
                .skipped_after_fatal;
        self.settleFatalWorkItem(failed_item, failure_kind, fatal);
        while (true) {
            driver.mutex.lockUncancelable(driver.io);
            const item = driver.queue.pop() orelse {
                driver.mutex.unlock(driver.io);
                break;
            };
            driver.space_available.broadcast(driver.io);
            driver.mutex.unlock(driver.io);
            self.settleFatalWorkItem(item, .skipped_after_fatal, fatal);
        }
        self.settleReadyRegisteredCallbacksAfterFatal();
    }

    fn settleFatalWorkItem(
        self: *ExchangeApiSharded,
        item: ShardedWorkItem,
        failure_kind: CallbackFailureKind,
        fatal: runtime_mod.FatalRecord,
    ) void {
        const driver = self.driver;
        switch (item.completion) {
            .detached => {},
            .poll => |request_id| {
                var registered_callback: ?CommandResponseCallback = null;
                driver.mutex.lockUncancelable(driver.io);
                std.debug.assert(driver.completion_slots_used > self.claimedCompletionCountLocked());
                driver.completion_slots_used -= 1;
                if (self.callbackRegistrationIndex(request_id)) |index| {
                    registered_callback = self.callback_registrations[index].callback;
                    self.removeCallbackRegistrationAt(index);
                    driver.callback_completions_pending += 1;
                }
                driver.space_available.broadcast(driver.io);
                driver.mutex.unlock(driver.io);
                if (registered_callback) |callback| {
                    self.dispatchCommandFailure(.{
                        .request_id = request_id,
                        .callback = callback,
                    }, .{
                        .request_id = request_id,
                        .kind = failure_kind,
                        .fatal = fatal,
                    });
                    driver.mutex.lockUncancelable(driver.io);
                    std.debug.assert(driver.callback_completions_pending > 0);
                    driver.callback_completions_pending -= 1;
                    driver.completion_available.broadcast(driver.io);
                    driver.mutex.unlock(driver.io);
                }
            },
            .custom_report => |owner| {
                self.custom_report_requests.failPending(
                    owner.pending,
                    @ptrCast(driver),
                ) catch unreachable;
                driver.mutex.lockUncancelable(driver.io);
                std.debug.assert(driver.completion_slots_used > self.claimedCompletionCountLocked());
                driver.completion_slots_used -= 1;
                driver.space_available.broadcast(driver.io);
                driver.mutex.unlock(driver.io);
            },
            .callback => |owner| {
                self.dispatchCommandFailure(owner, .{
                    .request_id = owner.request_id,
                    .kind = failure_kind,
                    .fatal = fatal,
                });
                driver.mutex.lockUncancelable(driver.io);
                std.debug.assert(driver.callback_completions_pending > 0);
                driver.callback_completions_pending -= 1;
                driver.mutex.unlock(driver.io);
            },
        }

        driver.mutex.lockUncancelable(driver.io);
        self.finishWorkLocked(item, .accepted);
        driver.space_available.broadcast(driver.io);
        driver.mutex.unlock(driver.io);
    }

    /// R2 can publish its first fatal after matching results are already ready.
    /// Poll owners have not consumed those responses yet, so discard the fatal
    /// sequence and any speculative suffix. Registered owners receive terminal
    /// failure callbacks; unregistered owners release their retained result
    /// slot so poison cannot strand capacity.
    fn settleReadyRegisteredCallbacksAfterFatal(
        self: *ExchangeApiSharded,
    ) void {
        const driver = self.driver;
        while (true) {
            driver.mutex.lockUncancelable(driver.io);
            const fatal = if (driver.lifecycle.fatalRecord()) |record|
                record.*
            else {
                driver.mutex.unlock(driver.io);
                return;
            };

            var selected_request_id: ?u64 = null;
            var ready_iterator = self.ready_results.iterator();
            while (ready_iterator.next()) |entry| {
                const response = entry.value_ptr.*;
                const affected = if (fatal.sequence) |fatal_sequence|
                    response.sequence >= fatal_sequence
                else
                    true;
                if (!affected) continue;
                selected_request_id = entry.key_ptr.*;
                break;
            }

            const request_id = selected_request_id orelse {
                driver.mutex.unlock(driver.io);
                return;
            };
            const removed = self.ready_results.fetchRemove(request_id) orelse
                unreachable;
            std.debug.assert(driver.completion_slots_used > 0);
            driver.completion_slots_used -= 1;
            driver.fatal_poll_settlements_pending += 1;
            const registered_callback = if (self.callbackRegistrationIndex(
                request_id,
            )) |index| blk: {
                const callback = self.callback_registrations[index].callback;
                self.removeCallbackRegistrationAt(index);
                driver.callback_completions_pending += 1;
                break :blk callback;
            } else null;
            driver.space_available.broadcast(driver.io);
            driver.mutex.unlock(driver.io);

            var response = removed.value;
            const failure_kind: CallbackFailureKind =
                if (fatal.request_id == request_id or
                (fatal.sequence != null and
                    fatal.sequence.? == response.sequence))
                    .command_failed
                else
                    .skipped_after_fatal;
            response.deinit(self.allocator);
            if (registered_callback) |callback| {
                self.dispatchCommandFailure(.{
                    .request_id = request_id,
                    .callback = callback,
                }, .{
                    .request_id = request_id,
                    .kind = failure_kind,
                    .fatal = fatal,
                });
            }
            driver.mutex.lockUncancelable(driver.io);
            std.debug.assert(driver.fatal_poll_settlements_pending > 0);
            driver.fatal_poll_settlements_pending -= 1;
            if (registered_callback != null) {
                std.debug.assert(driver.callback_completions_pending > 0);
                driver.callback_completions_pending -= 1;
            }
            driver.completion_available.broadcast(driver.io);
            driver.mutex.unlock(driver.io);
        }
    }

    fn waitForFatalCallbackDispatches(self: *ExchangeApiSharded) void {
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        while (driver.callback_completions_pending != 0 or
            driver.fatal_poll_settlements_pending != 0)
        {
            driver.completion_available.waitUncancelable(driver.io, &driver.mutex);
        }
        driver.mutex.unlock(driver.io);
    }

    fn sequenceForWork(self: *const ExchangeApiSharded, item: ShardedWorkItem) ?u64 {
        _ = self;
        return item.sequence.toInt();
    }

    fn drainAcceptedOnCurrentThread(self: *ExchangeApiSharded) void {
        while (true) {
            const driver = self.driver;
            driver.mutex.lockUncancelable(driver.io);
            if (driver.lifecycle.current() == .poisoned) {
                const fatal = driver.lifecycle.fatalRecord().?.*;
                driver.prepared_available_remaining = 0;
                const skipped = driver.queue.pop() orelse {
                    driver.mutex.unlock(driver.io);
                    return;
                };
                driver.space_available.broadcast(driver.io);
                driver.mutex.unlock(driver.io);
                self.settleFatalWorkItem(
                    skipped,
                    .skipped_after_fatal,
                    fatal,
                );
                continue;
            }
            if (driver.queue.isEmpty()) {
                std.debug.assert(driver.prepared_available_remaining == 0);
                self.observeGroupingIdleLocked();
                driver.mutex.unlock(driver.io);
                return;
            }
            const prepared = self.popPreparedWorkLocked();
            driver.space_available.broadcast(driver.io);
            driver.mutex.unlock(driver.io);
            self.processQueuedItem(prepared) catch |err| {
                self.poisonFromWork(prepared.item, err);
                return;
            };
        }
    }

    fn finishManualDrain(self: *ExchangeApiSharded) void {
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        std.debug.assert(driver.manual_drain_active);
        driver.manual_drain_active = false;
        driver.completion_available.broadcast(driver.io);
        driver.mutex.unlock(driver.io);
    }

    pub fn fatalInfo(self: *const ExchangeApiSharded) ?runtime_mod.FatalRecord {
        self.assertWiringConst();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        const record = driver.lifecycle.fatalRecord() orelse return null;
        return record.*;
    }

    fn assertWiring(self: *ExchangeApiSharded) void {
        std.debug.assert(self.allocator.ptr == @as(*anyopaque, @ptrCast(self.locked_allocator)));
        std.debug.assert(self.config.reports_queries.custom_report_registry == self.custom_report_registry);
        std.debug.assert((self.config.serialization.backend == .disk) == (self.persistence_root != null));
        std.debug.assert((self.config.serialization.backend == .custom) == (self.custom_serialization_processor != null));
        std.debug.assert(self.config.serialization.enable_journaling == (self.journal != null));
        std.debug.assert(self.durable_journal == null or
            (self.journal != null and self.config.serialization.backend == .disk));
        std.debug.assert(!self.settlement_hooks.isEnabled() or self.durable_journal != null);
    }

    fn assertWiringConst(self: *const ExchangeApiSharded) void {
        std.debug.assert(self.allocator.ptr == @as(*anyopaque, @ptrCast(self.locked_allocator)));
        std.debug.assert(self.config.reports_queries.custom_report_registry == self.custom_report_registry);
        std.debug.assert((self.config.serialization.backend == .disk) == (self.persistence_root != null));
        std.debug.assert((self.config.serialization.backend == .custom) == (self.custom_serialization_processor != null));
        std.debug.assert(self.config.serialization.enable_journaling == (self.journal != null));
        std.debug.assert(self.durable_journal == null or
            (self.journal != null and self.config.serialization.backend == .disk));
        std.debug.assert(!self.settlement_hooks.isEnabled() or self.durable_journal != null);
    }

    fn validateDurableJournalFrontier(
        self: *const ExchangeApiSharded,
    ) error{DurableJournalFrontierMismatch}!void {
        const appender = self.durable_journal orelse return;
        const durable_last = appender.lastSequence();
        const publication_high_water = self.driver.publication_sequencer.highWater();
        const last = durable_last orelse {
            if (publication_high_water == 0) return;
            return error.DurableJournalFrontierMismatch;
        };
        if (last != publication_high_water) return error.DurableJournalFrontierMismatch;
    }

    /// Publishes the recovered R2/journal frontier only after the complete
    /// snapshot plus journal suffix has passed native durable validation.
    /// Standalone replay deliberately never calls this boundary.
    fn finishSettlementRecovery(
        self: *ExchangeApiSharded,
    ) settlement_lifecycle.Error!void {
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        try self.finishSettlementRecoveryLocked();
    }

    /// Requires `driver.mutex`. The startup path already owns that lock, so a
    /// rejected hook must publish its fatal record directly instead of entering
    /// the ordinary poison helper and recursively acquiring the mutex.
    fn finishSettlementRecoveryLocked(
        self: *ExchangeApiSharded,
    ) settlement_lifecycle.Error!void {
        const driver = self.driver;
        if (!self.settlement_hooks.isEnabled() or self.settlement_recovery_finished) return;
        const sequence = publication.PublicationSequence.fromInt(
            driver.publication_sequencer.highWater(),
        );

        // Recovery hooks are application code. Keep the cold bootstrap fence
        // published, but release the driver mutex so read-only API inspection
        // cannot recursively acquire it. The callback execution scope makes
        // every blocking/reentrant public call fail with CoreReentrantCall.
        const current_thread = std.Thread.getCurrentId();
        const installed_bootstrap_fence = driver.bootstrap_owner == null;
        if (installed_bootstrap_fence) {
            driver.bootstrap_owner = current_thread;
        } else {
            std.debug.assert(driver.bootstrap_owner == current_thread);
        }
        driver.mutex.unlock(driver.io);
        const recovery = self.scopedSettlementHooks().finishRecovery(sequence);
        driver.mutex.lockUncancelable(driver.io);
        if (installed_bootstrap_fence) {
            std.debug.assert(driver.bootstrap_owner == current_thread);
            driver.bootstrap_owner = null;
            driver.completion_available.broadcast(driver.io);
        }

        _ = recovery catch |err| {
            const fatal: runtime_mod.FatalRecord = .{
                .stage = .startup,
                .sequence = sequence.toInt(),
                .request_id = null,
                .command = .nop,
                .cause = err,
            };
            if (driver.lifecycle.poison(fatal)) {
                self.core.shutdown();
                driver.prepared_available_remaining = 0;
                driver.work_available.broadcast(driver.io);
                driver.space_available.broadcast(driver.io);
                driver.completion_available.broadcast(driver.io);
            }
            return err;
        };
        self.settlement_recovery_finished = true;
    }

    /// Wraps every application-owned settlement callback in the same global
    /// callback scope used by result observers. The wrapper context is the
    /// heap-stable API address; the configured user hook remains immutable
    /// after startup/bootstrap begins.
    fn scopedSettlementHooks(self: *ExchangeApiSharded) settlement_lifecycle.Hooks {
        if (!self.settlement_hooks.isEnabled()) return .disabled();
        return .enabled(self, &.{
            .prepare = settlementPrepareInCallbackScope,
            .mark_risk_r2_committed = settlementMarkRiskInCallbackScope,
            .mark_journal_committed = settlementMarkJournalInCallbackScope,
            .finish_recovery = settlementFinishRecoveryInCallbackScope,
        });
    }

    fn settlementPrepareInCallbackScope(
        context: *anyopaque,
        batch: settlement_lifecycle.PreparedBatch,
    ) settlement_lifecycle.PrepareObservation {
        const self: *ExchangeApiSharded = @ptrCast(@alignCast(context));
        var callback_scope = runtime_mod.CallbackExecutionScope.enter();
        defer callback_scope.leave();
        _ = self.settlement_hooks.prepare(batch) catch return .rejected;
        return .prepared;
    }

    fn settlementMarkRiskInCallbackScope(
        context: *anyopaque,
        sequence: publication.PublicationSequence,
    ) settlement_lifecycle.MarkObservation {
        const self: *ExchangeApiSharded = @ptrCast(@alignCast(context));
        var callback_scope = runtime_mod.CallbackExecutionScope.enter();
        defer callback_scope.leave();
        _ = self.settlement_hooks.markRiskR2Committed(sequence) catch return .rejected;
        return .marked;
    }

    fn settlementMarkJournalInCallbackScope(
        context: *anyopaque,
        sequence: publication.PublicationSequence,
    ) settlement_lifecycle.MarkObservation {
        const self: *ExchangeApiSharded = @ptrCast(@alignCast(context));
        var callback_scope = runtime_mod.CallbackExecutionScope.enter();
        defer callback_scope.leave();
        _ = self.settlement_hooks.markJournalCommitted(sequence) catch return .rejected;
        return .marked;
    }

    fn settlementFinishRecoveryInCallbackScope(
        context: *anyopaque,
        sequence: publication.PublicationSequence,
    ) settlement_lifecycle.RecoveryObservation {
        const self: *ExchangeApiSharded = @ptrCast(@alignCast(context));
        var callback_scope = runtime_mod.CallbackExecutionScope.enter();
        defer callback_scope.leave();
        const disposition = self.settlement_hooks.finishRecovery(sequence) catch
            return .rejected;
        return switch (disposition) {
            .completed => .completed,
            .not_required => .not_required,
            .disabled => unreachable,
        };
    }

    fn adoptDurableSnapshotFrontier(
        self: *ExchangeApiSharded,
        snapshot_id: u64,
        sequence: u64,
        timestamp_ns: u64,
    ) journal_mod.JournalIoError!void {
        const appender = &(self.durable_journal orelse return);
        const timestamp: i64 = if (timestamp_ns > std.math.maxInt(i64))
            std.math.maxInt(i64)
        else
            @intCast(timestamp_ns);
        try appender.adoptSnapshotFrontier(snapshot_id, sequence, timestamp);
    }

    fn isWorkerThread(self: *const ExchangeApiSharded) bool {
        return active_worker_api == self;
    }

    /// Any operation that can wait for execution, publication, or settlement
    /// must fail before acquiring target-local locks while a callback is
    /// active on any exchange backend. This shared callback scope is what
    /// prevents cross-instance A -> B -> A lock cycles.
    fn ensureBlockingCallAllowed(self: *const ExchangeApiSharded) LifecycleError!void {
        if (runtime_mod.isCallbackExecutionActive() or self.isWorkerThread()) {
            return error.CoreReentrantCall;
        }
    }

    fn isCallbackThread(self: *const ExchangeApiSharded) bool {
        return active_callback_api == self;
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

    pub fn submitCommandAsyncWithCallback(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        api_command: ApiCommand,
        callback: CommandResponseCallback,
    ) SubmitCommandWithCallbackError!void {
        self.assertWiring();
        switch (api_command) {
            .add_user,
            .add_user_with_timestamp,
            .suspend_user,
            .suspend_user_with_timestamp,
            .resume_user,
            .resume_user_with_timestamp,
            .adjust_user_balance,
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
        var command = (try api_command_mod.toOrdinaryOrderCommand(api_command)).?;

        // Java's Consumer<OrderCommand> convenience overloads stamp wall-clock
        // milliseconds at publication time. An explicit translated timestamp
        // remains caller-owned; only the backwards-compatible zero default
        // receives the convenience fallback.
        if (command.timestamp == 0) {
            command.timestamp = std.math.cast(i64, io_context.realTimestampMs()) orelse
                return error.CallbackTimestampOverflow;
        }
        switch (api_command) {
            .add_user,
            .add_user_with_timestamp,
            .suspend_user,
            .suspend_user_with_timestamp,
            .resume_user,
            .resume_user_with_timestamp,
            => {
                command.order_id = -1;
                command.symbol = -1;
            },
            .request_order_book => {
                command.order_id = -1;
                command.uid = -1;
            },
            else => {},
        }
        try self.enqueueCallback(command, mode, callback);
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

    fn registerSnapshotDescriptorAssumeReserved(
        self: *ExchangeApiSharded,
        snapshot_id: u64,
        sequence: u64,
        timestamp_ns: u64,
    ) void {
        self.current_snapshot_ref = self.descriptors.createNextSnapshotAssumeReserved(
            self.current_snapshot_ref,
            snapshot_id,
            sequence,
            timestamp_ns,
        );
    }

    fn coordinatePersistenceCommand(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command: *cmd.OrderCommand,
        response: *kernel_mod.CommandResponse,
    ) void {
        const failure_code: cmd.CommandResultCode = switch (command.command) {
            .persist_state_matching => .state_persist_matching_engine_failed,
            .persist_state_risk => .state_persist_risk_engine_failed,
            else => return,
        };

        (switch (command.command) {
            .persist_state_matching => self.stageMatchingPersistence(mode, command, response.sequence),
            .persist_state_risk => self.commitRiskPersistence(mode, command, response.sequence),
            else => unreachable,
        }) catch |err| {
            command.result_code = failure_code;
            response.command.result_code = failure_code;
            response.result_code = failure_code;
            log.warn("persist command failed cmd={s} snapshot={d} seq={d} err={}", .{
                @tagName(command.command),
                command.order_id,
                response.sequence,
                err,
            });
        };
    }

    fn stageMatchingPersistence(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command: *const cmd.OrderCommand,
        sequence: u64,
    ) !void {
        // A new matching command always invalidates any incomplete older pair.
        self.pending_persist_pair = null;
        if (self.config.serialization.backend == .dummy) {
            return error.PersistenceBackendDisabled;
        }
        const snapshot_id = std.math.cast(u64, command.order_id) orelse
            return error.InvalidPersistSnapshotId;
        const timestamp_ns = std.math.cast(u64, command.timestamp) orelse
            return error.InvalidPersistTimestamp;
        const expected_risk_sequence = std.math.add(u64, sequence, 1) catch
            return error.PersistSequenceOverflow;

        try self.validatePersistenceFrontier(sequence);
        try self.descriptors.validateNextSnapshotSequence(
            self.current_snapshot_ref,
            expected_risk_sequence,
        );
        try self.descriptors.reserveNextSnapshot();

        if (self.custom_serialization_processor) |*processor| {
            const matching_instance_count = std.math.cast(
                u32,
                self.matchingShardCount(),
            ) orelse return error.EngineCountOverflow;
            var first_store_error: ?serialization_processor.StoreError = null;
            for (0..matching_instance_count) |instance| {
                processor.storeData(.{
                    .snapshot_id = snapshot_id,
                    .sequence = sequence,
                    .timestamp_ns = timestamp_ns,
                    .module_type = .matching_engine_router,
                    .instance_id = @intCast(instance),
                    .state = .{ .sharded = &self.core.kernels },
                }) catch |err| {
                    if (first_store_error == null) first_store_error = err;
                };
            }
            self.pending_persist_pair = .{
                .snapshot_id = snapshot_id,
                .timestamp_ns = timestamp_ns,
                .matching_sequence = sequence,
                .mode = mode,
                .matching_store_succeeded = first_store_error == null,
            };
            if (first_store_error) |err| {
                return err;
            }
            return;
        }

        const persistence_root = self.persistence_root orelse unreachable;
        try self.ensureConfiguredStorageFolderExists(persistence_root);
        const manifest_path = try persistence_paths.snapshotManifestPath(
            self.allocator,
            self.config.serialization.storage_folder,
            self.config.initial_state.exchange_id,
            snapshot_id,
        );
        defer self.allocator.free(manifest_path);
        if (try self.snapshotExistsFile(persistence_root, manifest_path)) {
            return error.SnapshotAlreadyCommitted;
        }

        self.pending_persist_pair = .{
            .snapshot_id = snapshot_id,
            .timestamp_ns = timestamp_ns,
            .matching_sequence = sequence,
            .mode = mode,
            .matching_store_succeeded = true,
        };
    }

    fn commitRiskPersistence(
        self: *ExchangeApiSharded,
        mode: ExecutionMode,
        command: *const cmd.OrderCommand,
        sequence: u64,
    ) !void {
        if (self.config.serialization.backend == .dummy) {
            return error.PersistenceBackendDisabled;
        }
        const pending = self.pending_persist_pair orelse return error.PersistPairMismatch;
        // No risk failure may leave a stale pair available to a later command.
        self.pending_persist_pair = null;

        const snapshot_id = std.math.cast(u64, command.order_id) orelse
            return error.InvalidPersistSnapshotId;
        const timestamp_ns = std.math.cast(u64, command.timestamp) orelse
            return error.InvalidPersistTimestamp;
        const expected_risk_sequence = std.math.add(u64, pending.matching_sequence, 1) catch
            return error.PersistSequenceOverflow;
        if (pending.snapshot_id != snapshot_id or
            pending.timestamp_ns != timestamp_ns or
            pending.mode != mode or
            sequence != expected_risk_sequence)
        {
            return error.PersistPairMismatch;
        }
        try self.validatePersistenceFrontier(sequence);

        if (self.custom_serialization_processor) |*processor| {
            const risk_instance_count = std.math.cast(
                u32,
                self.riskShardCount(),
            ) orelse return error.EngineCountOverflow;
            var first_store_error: ?serialization_processor.StoreError = null;
            for (0..risk_instance_count) |instance| {
                processor.storeData(.{
                    .snapshot_id = snapshot_id,
                    .sequence = sequence,
                    .timestamp_ns = timestamp_ns,
                    .module_type = .risk_engine,
                    .instance_id = @intCast(instance),
                    .state = .{ .sharded = &self.core.kernels },
                }) catch |err| {
                    if (first_store_error == null) first_store_error = err;
                };
            }
            if (first_store_error) |err| {
                return err;
            }
            if (pending.matching_store_succeeded) {
                self.registerSnapshotDescriptorAssumeReserved(
                    snapshot_id,
                    sequence,
                    timestamp_ns,
                );
            }
            return;
        }

        const persistence_root = self.persistence_root orelse unreachable;
        const matching_count = std.math.cast(u32, self.matchingShardCount()) orelse
            return error.EngineCountOverflow;
        const risk_count = std.math.cast(u32, self.riskShardCount()) orelse
            return error.EngineCountOverflow;
        const base_config = self.config.toKernelConfig();
        const fault = self.persistence_fault;
        self.persistence_fault = null;
        const summary = try sharded_snapshot_store.captureAndCommit(
            self.allocator,
            persistence_root,
            self.config.serialization.storage_folder,
            self.config.initial_state.exchange_id,
            &self.core.kernels,
            .{
                .snapshot_id = snapshot_id,
                .matching_command_seq = pending.matching_sequence,
                .risk_command_seq = sequence,
                .timestamp_ns = timestamp_ns,
                .config_sha256 = sharded_snapshot_store.kernelConfigSha256(
                    base_config,
                    matching_count,
                    risk_count,
                ),
            },
            .{
                .fault_point = if (fault != null) .before_manifest_publish else null,
            },
        );
        std.debug.assert(summary.publication_high_water == sequence);
        self.registerSnapshotDescriptorAssumeReserved(snapshot_id, sequence, timestamp_ns);
    }

    fn validatePersistenceFrontier(
        self: *const ExchangeApiSharded,
        expected: u64,
    ) error{PublicationFrontierMismatch}!void {
        if (self.core.kernels.next_seq != expected) return error.PublicationFrontierMismatch;
        for (0..self.matchingShardCount()) |shard_id| {
            if (self.core.kernels.matchingObservedSequence(shard_id) != expected) {
                return error.PublicationFrontierMismatch;
            }
        }
        for (0..self.riskShardCount()) |shard_id| {
            if (self.core.kernels.riskObservedSequence(shard_id) != expected) {
                return error.PublicationFrontierMismatch;
            }
        }
    }

    pub fn setPersistenceFaultForTesting(
        self: *ExchangeApiSharded,
        point: ?PersistenceFaultPoint,
    ) void {
        self.persistence_fault = point;
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
        // XCM1 contains one carrier kernel only. It remains an explicit
        // compatibility path solely for the topology it can represent.
        if (self.matchingShardCount() != 1 or self.riskShardCount() != 1) {
            return error.UnsupportedShardedPersistence;
        }
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

        if (self.custom_report_registry.len() != 0) {
            return error.LegacySnapshotCustomReportRegistryUnsupported;
        }
        outcome.snapshot_found = true;
        var restore_failure_command = makeNopCommand();
        var restore_started = true;
        errdefer |failure| if (restore_started) {
            self.poisonDirectExecution(
                publication.PublicationSequence.fromInt(self.core.kernels.next_seq),
                &restore_failure_command,
                failure,
            );
        };
        try snapshot.restoreIntoKernel(self.kernelRef());
        self.core.restorePublicationSequence(snapshot_base_seq);
        self.driver.publication_sequencer.restore(snapshot_base_seq);
        try self.adoptDurableSnapshotFrontier(snapshot_id, snapshot_base_seq, 0);
        try self.registerSnapshotDescriptor(snapshot_id, snapshot_base_seq, 0);
        restore_started = false;
    }

    fn bootstrapXcm2SnapshotWithId(
        self: *ExchangeApiSharded,
        persistence_root: std.Io.Dir,
        snapshot_id: u64,
        outcome: *BootstrapOutcome,
    ) BootstrapError!?sharded_snapshot_store.CommittedSnapshotInfo {
        outcome.snapshot_requested = true;
        const matching_count = std.math.cast(u32, self.matchingShardCount()) orelse
            return error.ShardedSnapshotTopologyMismatch;
        const risk_count = std.math.cast(u32, self.riskShardCount()) orelse
            return error.ShardedSnapshotTopologyMismatch;
        const info = sharded_snapshot_store.readCommittedSnapshotInfo(
            self.allocator,
            persistence_root,
            self.config.serialization.storage_folder,
            self.config.initial_state.exchange_id,
            snapshot_id,
            matching_count,
            risk_count,
            self.config.toKernelConfig(),
            .{},
        ) catch |err| switch (err) {
            error.FileNotFound => {
                if (self.config.initial_state.throw_if_snapshot_not_found) {
                    return error.SnapshotFileNotFound;
                }
                return null;
            },
            else => return mapShardedRestoreError(err),
        };

        const configured_base = self.config.initial_state.snapshot_base_seq;
        if (configured_base != 0 and configured_base != info.publication_high_water) {
            return error.ShardedSnapshotSequenceMismatch;
        }
        self.descriptors.validateNextSnapshotSequence(
            self.current_snapshot_ref,
            info.publication_high_water,
        ) catch return error.ShardedSnapshotSequenceMismatch;
        self.descriptors.reserveNextSnapshot() catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.ShardedSnapshotInvalid,
        };

        var staged = sharded_snapshot_store.restoreFresh(
            self.allocator,
            persistence_root,
            self.config.serialization.storage_folder,
            self.config.initial_state.exchange_id,
            .{
                .identity = info.identity,
                .matching_count = matching_count,
                .risk_count = risk_count,
                .base_config = self.config.toKernelConfig(),
            },
        ) catch |err| return mapShardedRestoreError(err);
        var staged_owned = true;
        defer if (staged_owned) staged.deinit();

        // A committed manifest is an authoritative publication checkpoint.
        // Adopt it before swapping the staged kernels so journal I/O failure
        // cannot leave the live core advanced beyond its durable frontier.
        try self.adoptDurableSnapshotFrontier(
            snapshot_id,
            info.publication_high_water,
            info.identity.timestamp_ns,
        );

        // This is the only mutation boundary. Everything above operates on a
        // fresh set; everything below is allocation-free and infallible.
        var previous = self.core.replaceKernelsCold(staged);
        staged_owned = false;
        self.driver.publication_sequencer.restore(info.publication_high_water);
        self.registerSnapshotDescriptorAssumeReserved(
            snapshot_id,
            info.publication_high_water,
            info.identity.timestamp_ns,
        );
        previous.deinit();
        outcome.snapshot_found = true;
        return info;
    }

    fn mapShardedRestoreError(
        err: anyerror,
    ) (ShardedSnapshotBootstrapError || UnsupportedShardedPersistenceError) {
        return switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            error.FileNotFound => error.SnapshotFileNotFound,
            error.LegacyXcm1Manifest => error.LegacyXcm1RequiresImmediateOrMigration,
            error.TopologyMismatch,
            error.InvalidMatchingCount,
            error.InvalidRiskCount,
            error.InvalidMatchingMask,
            error.InvalidRiskMask,
            error.InvalidShardCount,
            => error.ShardedSnapshotTopologyMismatch,
            error.ConfigMismatch => error.ShardedSnapshotConfigMismatch,
            error.ExchangeMismatch => error.ShardedSnapshotExchangeMismatch,
            error.InvalidSequence,
            error.SequenceMismatch,
            error.InvalidPublicationHighWater,
            error.InvalidObservedHighWater,
            error.InvalidEntrySequence,
            error.RestoredFrontierMismatch,
            => error.ShardedSnapshotSequenceMismatch,
            else => error.ShardedSnapshotInvalid,
        };
    }

    fn startupFromConfiguredPersistenceWithSnapshotId(
        self: *ExchangeApiSharded,
        dir: std.Io.Dir,
        snapshot_module_type: PersistenceModuleType,
        snapshot_instance_id: u32,
        journal_partition_id: u16,
        snapshot_id: u64,
    ) StartupFromConfiguredPersistenceError!BootstrapOutcome {
        _ = snapshot_module_type;
        _ = snapshot_instance_id;

        var journal_path: ?[]u8 = null;
        defer if (journal_path) |path| self.allocator.free(path);
        if (self.config.initial_state.journal_timestamp_ns != 0 and
            !self.settlement_hooks.isEnabled())
        {
            journal_path = try self.defaultPersistenceJournalPath(
                self.allocator,
                snapshot_id,
                journal_partition_id,
            );
        }

        var outcome: BootstrapOutcome = .{};
        const snapshot_info = try self.bootstrapXcm2SnapshotWithId(
            dir,
            snapshot_id,
            &outcome,
        );
        const snapshot_frontier = if (snapshot_info) |info|
            info.publication_high_water
        else
            self.config.initial_state.snapshot_base_seq;

        var first_partition_found = false;
        if (self.config.initial_state.journal_timestamp_ns != 0 and
            self.settlement_hooks.isEnabled())
        {
            first_partition_found = true;
            outcome.journal_replayed_commands = try self.replayNativeSettlementJournalColdOwnedAfterSequence(
                snapshot_frontier,
            );
        } else if (self.config.initial_state.journal_timestamp_ns != 0) {
            const path = journal_path orelse return error.JournalPathRequired;
            first_partition_found = true;
            outcome.journal_replayed_commands = self.replayJournalFromFileColdOwnedAfterSequence(
                dir,
                path,
                snapshot_frontier,
            ) catch |err| switch (err) {
                error.FileNotFound => blk: {
                    first_partition_found = false;
                    break :blk 0;
                },
                else => return err,
            };
        }

        if (!self.settlement_hooks.isEnabled() and
            first_partition_found and
            journal_partition_id < std.math.maxInt(u16))
        {
            const replayed_tail = try self.replayJournalFromDefaultPathRangeColdOwnedAfterSequence(
                dir,
                snapshot_id,
                journal_partition_id + 1,
                snapshot_frontier,
            );
            outcome.journal_replayed_commands = std.math.add(
                usize,
                outcome.journal_replayed_commands,
                replayed_tail,
            ) catch return error.JournalReplayCountOverflow;
        }

        try self.validateDurableJournalFrontier();
        self.finishSettlementRecovery() catch return error.CorePoisoned;
        self.initial_state_bootstrapped = true;
        try self.startup();
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
        var journal_path: ?[]u8 = null;
        defer if (journal_path) |path| self.allocator.free(path);
        if (self.config.initial_state.journal_timestamp_ns != 0 and
            !self.settlement_hooks.isEnabled())
        {
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
        if (discovered_seq) |sequence| {
            const matching_count = std.math.cast(u32, self.matchingShardCount()) orelse
                return error.ShardedSnapshotTopologyMismatch;
            const risk_count = std.math.cast(u32, self.riskShardCount()) orelse
                return error.ShardedSnapshotTopologyMismatch;
            const manifest_info = sharded_snapshot_store.readCommittedSnapshotInfo(
                self.allocator,
                dir,
                self.config.serialization.storage_folder,
                self.config.initial_state.exchange_id,
                snapshot_id,
                matching_count,
                risk_count,
                self.config.toKernelConfig(),
                .{},
            ) catch |err| return mapShardedRestoreError(err);
            const expected_main_log_sequence = switch (snapshot_module_type) {
                .matching_engine_router => manifest_info.identity.matching_command_seq,
                .risk_engine => manifest_info.identity.risk_command_seq,
            };
            if (sequence != expected_main_log_sequence) {
                return error.ShardedSnapshotSequenceMismatch;
            }
        }

        var outcome: BootstrapOutcome = .{};
        const snapshot_info = if (self.strict_snapshot_disappeared_after_validation_for_testing)
            null
        else
            try self.bootstrapXcm2SnapshotWithId(
                dir,
                snapshot_id,
                &outcome,
            );
        // Discovery and strict main-log validation selected this exact
        // committed snapshot. If it disappears before restore, success would
        // otherwise return a cold core without durable validation or recovery.
        const committed_snapshot = snapshot_info orelse
            return error.SnapshotFileNotFound;
        const snapshot_frontier = committed_snapshot.publication_high_water;

        var first_partition_found = false;
        if (self.config.initial_state.journal_timestamp_ns != 0 and
            self.settlement_hooks.isEnabled())
        {
            first_partition_found = true;
            outcome.journal_replayed_commands = try self.replayNativeSettlementJournalColdOwnedAfterSequence(
                snapshot_frontier,
            );
        } else if (self.config.initial_state.journal_timestamp_ns != 0) {
            const path = journal_path orelse return error.JournalPathRequired;
            first_partition_found = true;
            outcome.journal_replayed_commands = self.replayJournalFromFileColdOwnedAfterSequence(
                dir,
                path,
                snapshot_frontier,
            ) catch |err| switch (err) {
                error.FileNotFound => blk: {
                    first_partition_found = false;
                    break :blk 0;
                },
                else => return err,
            };
        }

        if (!self.settlement_hooks.isEnabled() and
            first_partition_found and
            journal_partition_id < std.math.maxInt(u16))
        {
            const replayed_tail = try self.replayJournalFromDefaultPathRangeColdOwnedAfterSequence(
                dir,
                snapshot_id,
                journal_partition_id + 1,
                snapshot_frontier,
            );
            outcome.journal_replayed_commands = std.math.add(
                usize,
                outcome.journal_replayed_commands,
                replayed_tail,
            ) catch return error.JournalReplayCountOverflow;
        }

        try self.validateDurableJournalFrontier();
        self.finishSettlementRecovery() catch return error.CorePoisoned;
        self.initial_state_bootstrapped = true;
        try self.startup();
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
        try persistence_paths.ensureDirPathDurable(
            io_context.current(),
            dir,
            storage_folder,
        );
    }

    /// Returns true when this call acquired the execution barrier and must
    /// release it. Same-API callbacks already own it; callbacks on any exchange
    /// backend may only acquire it without blocking to avoid ABBA waits.
    fn lockCallbackConfigBarrier(self: *ExchangeApiSharded) CallbackConfigError!bool {
        if (active_callback_api == self) return false;
        if (runtime_mod.isCallbackExecutionActive()) {
            if (!self.callback_execution_mutex.tryLock()) {
                return error.CallbackReconfigurationWouldBlock;
            }
            return true;
        }
        self.callback_execution_mutex.lockUncancelable(self.driver.io);
        return true;
    }

    fn advanceSimpleEventsRevisionLocked(self: *ExchangeApiSharded) CallbackConfigError!void {
        self.simple_events_revision = std.math.add(
            u64,
            self.simple_events_revision,
            1,
        ) catch return error.CallbackRevisionOverflow;
    }

    fn publishCompletionObservers(
        self: *ExchangeApiSharded,
        command: *const cmd.OrderCommand,
        response: *const kernel_mod.CommandResponse,
        runtime_event: ?runtime_mod.ResultEvent,
    ) void {
        self.callback_execution_mutex.lockUncancelable(self.driver.io);
        var callback_scope = runtime_mod.CallbackExecutionScope.enter();
        const previous_callback_api = active_callback_api;
        active_callback_api = self;
        defer {
            active_callback_api = previous_callback_api;
            callback_scope.leave();
            self.callback_execution_mutex.unlock(self.driver.io);
        }

        if (command.command == .grouping_control) {
            self.results_processing_enabled = command.order_id == 1;
        }
        if (!self.results_processing_enabled) return;

        self.callback_mutex.lockUncancelable(self.driver.io);
        const raw_sink = self.raw_result_sink;
        self.callback_mutex.unlock(self.driver.io);
        if (raw_sink.on_result) |callback| {
            callback(raw_sink.context, response);
        }

        if (runtime_event) |event| {
            self.callback_mutex.lockUncancelable(self.driver.io);
            const sink = self.runtime_sink;
            self.callback_mutex.unlock(self.driver.io);
            publishRuntimeResult(sink, event);
        }

        self.callback_mutex.lockUncancelable(self.driver.io);
        const handler = self.simple_events_handler;
        const revision = self.simple_events_revision;
        const processor = if (handler != null)
            if (self.simple_events_processor) |*value| value else null
        else
            null;
        self.callback_mutex.unlock(self.driver.io);

        if (processor) |events_processor| {
            events_processor.acceptWithHandler(
                handler.?,
                .{
                    .revision = &self.simple_events_revision,
                    .expected = revision,
                },
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

    fn dispatchCommandResponse(
        self: *ExchangeApiSharded,
        callback: CommandResponseCallback,
        response: *const kernel_mod.CommandResponse,
    ) void {
        const owns_quiescence_barrier = active_callback_api != self;
        if (owns_quiescence_barrier) {
            self.callback_execution_mutex.lockUncancelable(self.driver.io);
        }
        var callback_scope = runtime_mod.CallbackExecutionScope.enter();
        const previous_callback_api = active_callback_api;
        active_callback_api = self;
        defer {
            active_callback_api = previous_callback_api;
            callback_scope.leave();
            if (owns_quiescence_barrier) {
                self.callback_execution_mutex.unlock(self.driver.io);
            }
        }
        callback.callback(callback.context, response);
    }

    fn dispatchCommandFailure(
        self: *ExchangeApiSharded,
        owner: CallbackCompletionOwner,
        failure: CallbackFailure,
    ) void {
        const callback = owner.callback.failure_callback orelse return;
        const owns_quiescence_barrier = active_callback_api != self;
        if (owns_quiescence_barrier) {
            self.callback_execution_mutex.lockUncancelable(self.driver.io);
        }
        var callback_scope = runtime_mod.CallbackExecutionScope.enter();
        const previous_callback_api = active_callback_api;
        active_callback_api = self;
        defer {
            active_callback_api = previous_callback_api;
            callback_scope.leave();
            if (owns_quiescence_barrier) {
                self.callback_execution_mutex.unlock(self.driver.io);
            }
        }
        callback(owner.callback.context, failure);
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
    try file.sync(io_context.current());
    try persistence_paths.syncParentDirectory(io_context.current(), dir, path);
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

fn failureStage(failure: anyerror) runtime_mod.FailureStage {
    return switch (failure) {
        error.InjectedJournalIoFailure,
        error.AppenderPoisoned,
        error.SequenceGap,
        error.JournalEntryMismatch,
        error.SettlementJournalMarkRejected,
        => .journal,
        error.PositionRecordCorrupted,
        error.UserProfileCorrupted,
        error.SettlementPreparationRejected,
        error.SettlementRiskR2MarkRejected,
        => .r2,
        error.SettlementRecoveryRejected => .startup,
        error.OrderBookInvariantViolation => .matching,
        error.SequenceOverflow => .grouping,
        error.OutOfMemory => .store_result,
        else => .execute,
    };
}

fn groupedFailureStage(
    stage: sharded_pipeline.FailureStage,
) runtime_mod.FailureStage {
    return switch (stage) {
        .accept, .publication_ownership => .grouping,
        .risk_pre_process => .r1,
        .matching => .matching,
        .journal => .journal,
        .risk_release => .r2,
    };
}

const SettlementLifecycleTestEvent = enum {
    prepare,
    risk_r2,
    journal,
    recovery,
};

fn apiErrorSetContains(comptime ErrorSet: type, comptime error_name: []const u8) bool {
    inline for (std.meta.fields(ErrorSet)) |field| {
        if (std.mem.eql(u8, field.name, error_name)) return true;
    }
    return false;
}

const SettlementLifecycleTestRecorder = struct {
    api: ?*ExchangeApiSharded = null,
    reject_prepare: bool = false,
    reject_risk: bool = false,
    reject_journal: bool = false,
    reject_recovery: bool = false,
    prepare_calls: usize = 0,
    risk_calls: usize = 0,
    journal_calls: usize = 0,
    recovery_calls: usize = 0,
    last_prepare_mode: settlement_lifecycle.ExecutionMode = .live,
    last_prepare_trade_count: usize = 0,
    last_sequence: u64 = 0,
    journal_marks_followed_native_sync: bool = true,
    recovery_saw_validated_frontier: bool = true,
    events: [32]SettlementLifecycleTestEvent = undefined,
    events_len: usize = 0,

    fn hooks(self: *SettlementLifecycleTestRecorder) settlement_lifecycle.Hooks {
        return settlement_lifecycle.Hooks.enabled(self, &.{
            .prepare = prepare,
            .mark_risk_r2_committed = markRiskR2Committed,
            .mark_journal_committed = markJournalCommitted,
            .finish_recovery = finishRecovery,
        });
    }

    fn resetObservations(self: *SettlementLifecycleTestRecorder) void {
        self.prepare_calls = 0;
        self.risk_calls = 0;
        self.journal_calls = 0;
        self.recovery_calls = 0;
        self.last_prepare_mode = .live;
        self.last_prepare_trade_count = 0;
        self.last_sequence = 0;
        self.journal_marks_followed_native_sync = true;
        self.recovery_saw_validated_frontier = true;
        self.events_len = 0;
    }

    fn record(self: *SettlementLifecycleTestRecorder, event: SettlementLifecycleTestEvent) void {
        std.debug.assert(self.events_len < self.events.len);
        self.events[self.events_len] = event;
        self.events_len += 1;
    }

    fn prepare(
        context: *anyopaque,
        batch: settlement_lifecycle.PreparedBatch,
    ) settlement_lifecycle.PrepareObservation {
        const self: *SettlementLifecycleTestRecorder = @ptrCast(@alignCast(context));
        self.record(.prepare);
        self.prepare_calls += 1;
        self.last_prepare_mode = batch.mode;
        self.last_prepare_trade_count = batch.tradeCount();
        self.last_sequence = batch.sequence.toInt();
        return if (self.reject_prepare) .rejected else .prepared;
    }

    fn markRiskR2Committed(
        context: *anyopaque,
        sequence: publication.PublicationSequence,
    ) settlement_lifecycle.MarkObservation {
        const self: *SettlementLifecycleTestRecorder = @ptrCast(@alignCast(context));
        self.record(.risk_r2);
        self.risk_calls += 1;
        self.last_sequence = sequence.toInt();
        return if (self.reject_risk) .rejected else .marked;
    }

    fn markJournalCommitted(
        context: *anyopaque,
        sequence: publication.PublicationSequence,
    ) settlement_lifecycle.MarkObservation {
        const self: *SettlementLifecycleTestRecorder = @ptrCast(@alignCast(context));
        self.record(.journal);
        self.journal_calls += 1;
        self.last_sequence = sequence.toInt();
        const api = self.api orelse unreachable;
        const appender = &(api.durable_journal orelse unreachable);
        self.journal_marks_followed_native_sync =
            self.journal_marks_followed_native_sync and
            appender.lastSequence() == @as(?u64, sequence.toInt());
        return if (self.reject_journal) .rejected else .marked;
    }

    fn finishRecovery(
        context: *anyopaque,
        sequence: publication.PublicationSequence,
    ) settlement_lifecycle.RecoveryObservation {
        const self: *SettlementLifecycleTestRecorder = @ptrCast(@alignCast(context));
        self.record(.recovery);
        self.recovery_calls += 1;
        self.last_sequence = sequence.toInt();
        const api = self.api orelse unreachable;
        const appender = &(api.durable_journal orelse unreachable);
        self.recovery_saw_validated_frontier =
            self.recovery_saw_validated_frontier and
            appender.lastSequence() == @as(?u64, sequence.toInt()) and
            api.core.kernels.next_seq == sequence.toInt() and
            api.driver.publication_sequencer.highWater() == sequence.toInt();
        return if (self.reject_recovery) .rejected else .completed;
    }
};

const ConcurrentSettlementLifecycleTestRecorder = struct {
    api: *ExchangeApiSharded,
    prepare_calls: std.atomic.Value(usize) = .init(0),
    risk_calls: std.atomic.Value(usize) = .init(0),
    journal_calls: std.atomic.Value(usize) = .init(0),
    recovery_calls: std.atomic.Value(usize) = .init(0),
    last_sequence: std.atomic.Value(u64) = .init(0),
    last_prepare_mode: std.atomic.Value(u8) = .init(
        @intFromEnum(settlement_lifecycle.ExecutionMode.live),
    ),
    journal_marks_followed_native_sync: std.atomic.Value(bool) = .init(true),

    fn hooks(self: *ConcurrentSettlementLifecycleTestRecorder) settlement_lifecycle.Hooks {
        return settlement_lifecycle.Hooks.enabled(self, &.{
            .prepare = prepare,
            .mark_risk_r2_committed = markRiskR2Committed,
            .mark_journal_committed = markJournalCommitted,
            .finish_recovery = finishRecovery,
        });
    }

    fn prepare(
        context: *anyopaque,
        batch: settlement_lifecycle.PreparedBatch,
    ) settlement_lifecycle.PrepareObservation {
        const self: *ConcurrentSettlementLifecycleTestRecorder = @ptrCast(@alignCast(context));
        _ = self.prepare_calls.fetchAdd(1, .acq_rel);
        self.last_sequence.store(batch.sequence.toInt(), .release);
        self.last_prepare_mode.store(@intFromEnum(batch.mode), .release);
        return .prepared;
    }

    fn markRiskR2Committed(
        context: *anyopaque,
        sequence: publication.PublicationSequence,
    ) settlement_lifecycle.MarkObservation {
        const self: *ConcurrentSettlementLifecycleTestRecorder = @ptrCast(@alignCast(context));
        _ = self.risk_calls.fetchAdd(1, .acq_rel);
        self.last_sequence.store(sequence.toInt(), .release);
        return .marked;
    }

    fn markJournalCommitted(
        context: *anyopaque,
        sequence: publication.PublicationSequence,
    ) settlement_lifecycle.MarkObservation {
        const self: *ConcurrentSettlementLifecycleTestRecorder = @ptrCast(@alignCast(context));
        _ = self.journal_calls.fetchAdd(1, .acq_rel);
        self.last_sequence.store(sequence.toInt(), .release);
        const appender = &(self.api.durable_journal orelse unreachable);
        if (appender.lastSequence() != @as(?u64, sequence.toInt())) {
            self.journal_marks_followed_native_sync.store(false, .release);
        }
        return .marked;
    }

    fn finishRecovery(
        context: *anyopaque,
        sequence: publication.PublicationSequence,
    ) settlement_lifecycle.RecoveryObservation {
        const self: *ConcurrentSettlementLifecycleTestRecorder = @ptrCast(@alignCast(context));
        _ = self.recovery_calls.fetchAdd(1, .acq_rel);
        self.last_sequence.store(sequence.toInt(), .release);
        return .completed;
    }
};

const SettlementCallbackReentryProbe = struct {
    const Outcome = enum(u8) {
        not_called,
        core_reentrant,
        unexpected_error,
        unexpected_success,
    };

    api: *ExchangeApiSharded,
    probe_prepare: bool = false,
    probe_recovery: bool = false,
    prepare_outcome: std.atomic.Value(u8) = .init(@intFromEnum(Outcome.not_called)),
    recovery_outcome: std.atomic.Value(u8) = .init(@intFromEnum(Outcome.not_called)),
    recovery_saw_started: std.atomic.Value(bool) = .init(true),

    fn hooks(self: *SettlementCallbackReentryProbe) settlement_lifecycle.Hooks {
        return .enabled(self, &.{
            .prepare = prepare,
            .mark_risk_r2_committed = markRisk,
            .mark_journal_committed = markJournal,
            .finish_recovery = finishRecovery,
        });
    }

    fn attemptBlockingCall(
        self: *SettlementCallbackReentryProbe,
        outcome: *std.atomic.Value(u8),
    ) void {
        var response = self.api.nop(.routed) catch |err| {
            outcome.store(@intFromEnum(if (err == error.CoreReentrantCall)
                Outcome.core_reentrant
            else
                Outcome.unexpected_error), .release);
            return;
        };
        response.deinit(self.api.allocator);
        outcome.store(@intFromEnum(Outcome.unexpected_success), .release);
    }

    fn prepare(
        context: *anyopaque,
        _: settlement_lifecycle.PreparedBatch,
    ) settlement_lifecycle.PrepareObservation {
        const self: *SettlementCallbackReentryProbe = @ptrCast(@alignCast(context));
        if (self.probe_prepare) self.attemptBlockingCall(&self.prepare_outcome);
        return .prepared;
    }

    fn markRisk(
        _: *anyopaque,
        _: publication.PublicationSequence,
    ) settlement_lifecycle.MarkObservation {
        return .marked;
    }

    fn markJournal(
        _: *anyopaque,
        _: publication.PublicationSequence,
    ) settlement_lifecycle.MarkObservation {
        return .marked;
    }

    fn finishRecovery(
        context: *anyopaque,
        _: publication.PublicationSequence,
    ) settlement_lifecycle.RecoveryObservation {
        const self: *SettlementCallbackReentryProbe = @ptrCast(@alignCast(context));
        if (self.probe_recovery) {
            self.recovery_saw_started.store(self.api.isStarted(), .release);
            self.attemptBlockingCall(&self.recovery_outcome);
        }
        return .completed;
    }
};

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

fn testJavaIdentityHashContext() kernel_mod.JavaIdentityHashContext {
    return .{
        .order_action_ask = 0x101,
        .order_action_bid = 0x102,
        .user_status_active = 0x201,
        .user_status_suspended = 0x202,
        .risk_symbol_spec_provider = 0x301,
        .risk_user_profile_service = 0x302,
        .risk_binary_cmd_processor = 0x303,
        .risk_last_price_cache = 0x304,
        .risk_fees = 0x305,
        .risk_adjustments = 0x306,
        .risk_suspends = 0x307,
        .risk_shard_mask = 0x308,
        .matching_binary_cmd_processor = 0x309,
        .matching_order_books = 0x30a,
        .matching_shard_mask = 0x30b,
    };
}

fn installTestJavaIdentityHashContext(api: *ExchangeApiSharded) void {
    const identity = testJavaIdentityHashContext();
    for (api.core.kernels.matching_shards) |*kernel| {
        kernel.java_identity_hash_context = identity;
    }
}

fn expectJavaStateHashResponse(response: *const kernel_mod.CommandResponse) !void {
    try std.testing.expect(response.report_response == null);
    switch (response.java_report_response orelse return error.MissingReportResponse) {
        .state_hash => |state_hash| {
            try std.testing.expect(state_hash.report.entries.len > 0);
        },
        else => return error.UnexpectedReportResponseType,
    }
}

fn expectNativeAndJavaStateHashResponse(
    response: *const kernel_mod.CommandResponse,
) !void {
    switch (response.report_response orelse return error.MissingReportResponse) {
        .state_hash_detailed => |state_hash| {
            try std.testing.expect(state_hash.submodules.items.len > 0);
        },
        else => return error.UnexpectedReportResponseType,
    }
    switch (response.java_report_response orelse return error.MissingReportResponse) {
        .state_hash => |state_hash| {
            try std.testing.expect(state_hash.report.entries.len > 0);
        },
        else => return error.UnexpectedReportResponseType,
    }
}

fn javaMapValue(entries: ?[]const java_report_wire.IntLongEntry, key: i32) ?i64 {
    for (entries orelse return null) |entry| {
        if (entry.key == key) return entry.value;
    }
    return null;
}

fn padBinaryPayloadToFrameCount(
    allocator: std.mem.Allocator,
    body: []const u8,
    frame_count: usize,
) (std.mem.Allocator.Error || error{TestPayloadDoesNotFit})![]u8 {
    if (frame_count == 0) return error.TestPayloadDoesNotFit;
    const frame_bytes = binary_frames.FrameWordsPerCommand * @sizeOf(i64);
    const maximum_len = std.math.mul(usize, frame_count, frame_bytes) catch
        return error.TestPayloadDoesNotFit;
    const minimum_len = std.math.add(
        usize,
        std.math.mul(usize, frame_count - 1, frame_bytes) catch
            return error.TestPayloadDoesNotFit,
        1,
    ) catch return error.TestPayloadDoesNotFit;
    if (body.len > maximum_len) return error.TestPayloadDoesNotFit;

    const payload_len = @max(body.len, minimum_len);
    const payload = try allocator.alloc(u8, payload_len);
    @memset(payload, 0);
    @memcpy(payload[0..body.len], body);
    return payload;
}

fn encodeAddAccountsPayloadForFrameCount(
    allocator: std.mem.Allocator,
    frame_count: usize,
    base_uid: i64,
) (binary_payload.EncodeError || error{TestPayloadDoesNotFit})![]u8 {
    var user_count: usize = 1;
    while (true) : (user_count += 1) {
        const body_len = std.math.add(
            usize,
            8,
            std.math.mul(usize, user_count, 24) catch
                return error.TestPayloadDoesNotFit,
        ) catch return error.TestPayloadDoesNotFit;
        const layout = computeBinaryFrameLayout(body_len) catch
            return error.TestPayloadDoesNotFit;
        if (layout.frame_count == frame_count) break;
        if (layout.frame_count > frame_count) return error.TestPayloadDoesNotFit;
    }

    const accounts = try allocator.alloc(BinaryCurrencyBalance, user_count);
    defer allocator.free(accounts);
    const users = try allocator.alloc(BinaryUserAccountsBatch, user_count);
    defer allocator.free(users);
    for (users, accounts, 0..) |*user, *account, index| {
        account.* = .{
            .currency = 11,
            .balance = 10_000 + @as(i64, @intCast(index)),
        };
        user.* = .{
            .uid = base_uid + @as(i64, @intCast(index)),
            .accounts = accounts[index .. index + 1],
        };
    }
    return binary_payload.encodeAddAccountsToBytes(allocator, users);
}

fn expectJournalBinaryTransfer(
    journal: *const journal_mod.CommandJournal,
    start: usize,
    frame_count: usize,
    transfer_id: i32,
    command_type: cmd.OrderCommandType,
    mode: ExecutionMode,
) !void {
    try std.testing.expect(start + frame_count <= journal.entries.items.len);
    const entries = journal.entries.items[start .. start + frame_count];
    const first_sequence = entries[0].sequence;
    for (entries, 0..) |entry, index| {
        try std.testing.expectEqual(first_sequence + @as(u64, @intCast(index)), entry.sequence);
        try std.testing.expectEqual(mode, entry.mode);
        try std.testing.expectEqual(command_type, entry.input.command);
        try std.testing.expectEqual(transfer_id, entry.input.user_cookie);
        try std.testing.expectEqual(
            @as(i32, if (index + 1 == frame_count) -1 else 0),
            entry.input.symbol,
        );
    }
}

fn expectShardedBinaryFrontier(
    api: *const ExchangeApiSharded,
    expected_sequence: u64,
) !void {
    try std.testing.expectEqual(expected_sequence, api.core.kernels.next_seq);
    for (api.core.kernels.matching_shards) |*kernel| {
        try std.testing.expectEqual(expected_sequence, kernel.next_seq);
        try std.testing.expectEqual(@as(usize, 0), kernel.binary_frame_assembler.pendingTransfers());
    }
    for (api.core.kernels.risk_shards) |*risk_shard| {
        try std.testing.expectEqual(expected_sequence, risk_shard.next_seq);
        try std.testing.expectEqual(@as(usize, 0), risk_shard.binary_frame_assembler.pendingTransfers());
    }
}

const FakeGroupingClock = struct {
    now_ns: u64,
    reads: usize = 0,

    fn source(self: *FakeGroupingClock) runtime_mod.MonotonicClock {
        return .{
            .context = self,
            .read_ns = read,
        };
    }

    fn read(context: ?*anyopaque) u64 {
        const self: *FakeGroupingClock = @ptrCast(@alignCast(context.?));
        self.reads += 1;
        return self.now_ns;
    }
};

fn runGroupingMetadataFlow(mode: ExecutionMode) !void {
    const allocator = std.testing.allocator;
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 64;
    java_config.performance_cfg.msgs_in_group_limit = 16;
    java_config.performance_cfg.max_group_duration_ns = 10;
    java_config.serialization_cfg = config_mod.SerializationConfig.diskJournaling();

    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();
    var clock = FakeGroupingClock{ .now_ns = 100 };
    api.grouping_clock = clock.source();

    var typed_sync = try api.submitApiCommandSync(mode, .{ .add_user = 130_001 });
    typed_sync.deinit(allocator);
    var raw_sync = try api.submitApiCommandSync(mode, .{ .raw_command = .{
        .command = .add_user,
        .uid = 130_002,
        .events_group = 801,
        .service_flags = 11,
    } });
    raw_sync.deinit(allocator);

    const typed_async = try api.submitApiCommandAsync(mode, .{ .add_user = 130_003 });
    const raw_async = try api.submitApiCommandAsync(mode, .{ .raw_command = .{
        .command = .add_user,
        .uid = 130_004,
        .events_group = 802,
        .service_flags = 12,
    } });
    try api.submitApiCommand(mode, .{ .add_user = 130_005 });
    try api.submitApiCommand(mode, .{ .raw_command = .{
        .command = .add_user,
        .uid = 130_006,
        .events_group = 803,
        .service_flags = 13,
    } });
    const grouping_off = try api.groupingControlAsync(mode, 9_970_100_001, 0);
    var live_while_off = cmd.OrderCommand{
        .command = .add_user,
        .uid = 130_007,
        .events_group = 804,
        .service_flags = 14,
    };
    const live_while_off_request = try api.submitCommandAsync(mode, &live_while_off);
    const grouping_on = try api.groupingControlAsync(mode, 9_970_100_002, 1);

    try std.testing.expectEqual(@as(usize, 5), try api.drainPending());

    for ([_]ApiCommandAsyncHandle{ typed_async, raw_async }) |handle| {
        var result = api.takeApiCommandAsyncResult(handle) orelse return error.MissingAsyncResult;
        result.deinit(allocator);
    }
    for ([_]u64{ grouping_off, live_while_off_request, grouping_on }) |request_id| {
        var response = api.takeResult(request_id) orelse return error.MissingAsyncResult;
        response.deinit(allocator);
    }

    const entries = api.journal.?.entries.items;
    try std.testing.expectEqual(@as(usize, 9), entries.len);
    const expected_groups = [_]i64{ 0, 801, 0, 802, 0, 803, 0, 804, 1 };
    const expected_flags = [_]i32{ 0, 11, 0, 12, 0, 13, 0, 14, 0 };
    for (entries, expected_groups, expected_flags) |entry, expected_group, expected_flag| {
        try std.testing.expectEqual(expected_group, entry.input.events_group);
        try std.testing.expectEqual(expected_flag, entry.input.service_flags);
    }
    // One direct live range, one queued live range, and the terminal no-event
    // observation read the clock. Explicit raw commands never did.
    try std.testing.expectEqual(@as(usize, 3), clock.reads);
}

fn runGroupingDeadlineFlow(mode: ExecutionMode) !void {
    const allocator = std.testing.allocator;
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 8;
    java_config.performance_cfg.msgs_in_group_limit = 2;
    java_config.performance_cfg.max_group_duration_ns = 10;
    java_config.serialization_cfg = config_mod.SerializationConfig.diskJournaling();

    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();
    var clock = FakeGroupingClock{ .now_ns = 100 };
    api.grouping_clock = clock.source();

    const first_ids = [_]u64{
        try api.nopAsync(mode),
        try api.nopAsync(mode),
        try api.nopAsync(mode),
    };
    try std.testing.expectEqual(@as(usize, first_ids.len), try api.drainPending());
    for (first_ids) |request_id| {
        try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), api.takeResultCode(request_id));
    }

    var entries = api.journal.?.entries.items;
    try std.testing.expectEqual(@as(usize, 3), entries.len);
    try std.testing.expectEqual(@as(i64, 0), entries[0].input.events_group);
    try std.testing.expectEqual(@as(i64, 0), entries[1].input.events_group);
    try std.testing.expectEqual(@as(i64, 1), entries[2].input.events_group);

    // Java's deadline is strict: equality leaves the partial group open.
    clock.now_ns = 110;
    try std.testing.expectEqual(@as(usize, 0), try api.drainPending());
    try std.testing.expectEqual(@as(i64, 1), api.grouping.group_counter);
    clock.now_ns = 111;
    try std.testing.expectEqual(@as(usize, 0), try api.drainPending());
    try std.testing.expectEqual(@as(i64, 2), api.grouping.group_counter);

    const l2_request = try api.nopAsync(mode);
    const following_request = try api.nopAsync(mode);
    try std.testing.expectEqual(@as(usize, 2), try api.drainPending());
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), api.takeResultCode(l2_request));
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), api.takeResultCode(following_request));

    entries = api.journal.?.entries.items;
    try std.testing.expectEqual(@as(usize, 5), entries.len);
    try std.testing.expectEqual(@as(i64, 2), entries[3].input.events_group);
    try std.testing.expectEqual(@as(i32, 1), entries[3].input.service_flags);
    try std.testing.expectEqual(@as(i64, 2), entries[4].input.events_group);
    try std.testing.expectEqual(@as(i32, 0), entries[4].input.service_flags);
    try std.testing.expectEqual(@as(usize, 6), clock.reads);
}

fn runGroupingCompoundBoundaryFlow(mode: ExecutionMode) !void {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 64;
    java_config.performance_cfg.msgs_in_group_limit = 16;
    java_config.serialization_cfg = config_mod.SerializationConfig.diskJournaling();

    var api = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        java_config,
    );
    defer api.deinit();
    var clock = FakeGroupingClock{ .now_ns = 100 };
    api.grouping_clock = clock.source();

    const before = try api.nopAsync(mode);
    const persist = try api.persistStateAsync(mode, 130_100, 9_970_200_001);
    const spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = 25_100,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
    });
    const binary = try api.submitAddSymbolsBinaryAsync(
        mode,
        130_101,
        9_970_200_002,
        &.{spec},
    );
    const after = try api.nopAsync(mode);
    _ = try api.drainPending();

    for ([_]u64{ before, binary, after }) |request_id| {
        try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), api.takeResultCode(request_id));
    }
    try std.testing.expectEqual(
        @as(?cmd.CommandResultCode, .success),
        api.takePersistStateResult(persist),
    );

    const entries = api.journal.?.entries.items;
    try std.testing.expect(entries.len >= 6);
    try std.testing.expectEqual(cmd.OrderCommandType.nop, entries[0].input.command);
    try std.testing.expectEqual(@as(i64, 0), entries[0].input.events_group);
    try std.testing.expectEqual(cmd.OrderCommandType.persist_state_matching, entries[1].input.command);
    try std.testing.expectEqual(@as(i64, 1), entries[1].input.events_group);
    try std.testing.expectEqual(cmd.OrderCommandType.persist_state_risk, entries[2].input.command);
    try std.testing.expectEqual(@as(i64, 1), entries[2].input.events_group);

    const terminal_index = entries.len - 2;
    try std.testing.expectEqual(cmd.OrderCommandType.binary_data_command, entries[terminal_index].input.command);
    try std.testing.expectEqual(@as(i32, -1), entries[terminal_index].input.symbol);
    try std.testing.expectEqual(@as(i64, 2), entries[terminal_index].input.events_group);
    for (entries[3..terminal_index]) |entry| {
        try std.testing.expectEqual(cmd.OrderCommandType.binary_data_command, entry.input.command);
        try std.testing.expect(entry.input.symbol != -1);
        try std.testing.expectEqual(@as(i64, 1), entry.input.events_group);
    }
    try std.testing.expectEqual(cmd.OrderCommandType.nop, entries[entries.len - 1].input.command);
    try std.testing.expectEqual(@as(i64, 2), entries[entries.len - 1].input.events_group);
    try std.testing.expectEqual(@as(usize, 2), clock.reads);
}

fn runDefaultTimedL2RefreshFlow(mode: ExecutionMode) !void {
    const allocator = std.testing.allocator;
    const symbol: i32 = 25_200;
    const bidder: i64 = 131_001;
    const asker: i64 = 131_002;

    var api = try ExchangeApiSharded.init(allocator, 2, .direct);
    defer api.deinit();
    var clock = FakeGroupingClock{ .now_ns = 100 };
    api.grouping_clock = clock.source();
    try std.testing.expectEqual(
        grouping_processor.default_l2_publish_interval_ns,
        api.grouping.l2_publish_interval_ns,
    );

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

    var add_bidder = try api.addUser(mode, bidder);
    defer add_bidder.deinit(allocator);
    var add_asker = try api.addUser(mode, asker);
    defer add_asker.deinit(allocator);
    var fund_bidder = try api.adjustUserBalance(mode, bidder, 1, 15, 100_000, .adjustment);
    defer fund_bidder.deinit(allocator);
    var fund_asker = try api.adjustUserBalance(mode, asker, 1, 11, 100_000, .adjustment);
    defer fund_asker.deinit(allocator);
    var bid = try api.placeOrder(mode, 131_101, bidder, symbol, 91, 120, 3, .bid, .gtc, 0);
    defer bid.deinit(allocator);
    var ask = try api.placeOrder(mode, 131_102, asker, symbol, 105, 0, 4, .ask, .gtc, 0);
    defer ask.deinit(allocator);
    for ([_]cmd.CommandResultCode{
        add_bidder.result_code,
        add_asker.result_code,
        fund_bidder.result_code,
        fund_asker.result_code,
        bid.result_code,
        ask.result_code,
    }) |result_code| try std.testing.expectEqual(cmd.CommandResultCode.success, result_code);

    for (0..api.core.riskShardCount()) |index| {
        try std.testing.expect(api.core.kernels.riskEngineAt(index).getLastPrice(symbol) == null);
    }

    // A real no-event observation arms Java's default ten-millisecond L2
    // request. The next matching command consumes it exactly once.
    try std.testing.expectEqual(@as(usize, 0), try api.drainPending());
    var move = try api.moveOrder(mode, 131_101, bidder, symbol, 92);
    defer move.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, move.result_code);
    try std.testing.expectEqual(@as(i32, 1), move.command.service_flags);

    for (0..api.core.riskShardCount()) |index| {
        const last_price = api.core.kernels.riskEngineAt(index).getLastPrice(symbol) orelse
            return error.MissingLastPrice;
        try std.testing.expectEqual(@as(i64, 105), last_price.ask_price);
        try std.testing.expectEqual(@as(i64, 92), last_price.bid_price);
    }

    var order_book = try api.orderBookRequest(mode, symbol, 4);
    defer order_book.deinit(allocator);
    const snapshot = order_book.l2_snapshot orelse return error.MissingL2Snapshot;
    try std.testing.expectEqual(@as(usize, 1), snapshot.asks.items.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.bids.items.len);
    try std.testing.expectEqual(@as(i64, 105), snapshot.asks.items[0].price);
    try std.testing.expectEqual(@as(i64, 92), snapshot.bids.items[0].price);
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
    last_sequence: u64 = 0,
    last_result: ?cmd.CommandResultCode = null,
    last_command: ?cmd.OrderCommandType = null,
    last_uid: i64 = 0,
    last_order_id: i64 = 0,
    last_symbol: i32 = 0,
    last_timestamp: i64 = 0,
    last_snapshot_present: bool = false,
    last_ask_levels: usize = 0,
    last_bid_levels: usize = 0,

    fn onResponse(context: ?*anyopaque, response: *const kernel_mod.CommandResponse) void {
        const self: *CallbackCollector = @ptrCast(@alignCast(context.?));
        self.count += 1;
        self.last_sequence = response.sequence;
        self.last_result = response.result_code;
        self.last_command = response.command.command;
        self.last_uid = response.command.uid;
        self.last_order_id = response.command.order_id;
        self.last_symbol = response.command.symbol;
        self.last_timestamp = response.command.timestamp;
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

const FatalCallbackProbe = struct {
    response_count: usize = 0,
    failure_count: usize = 0,
    all_failure_callbacks_scoped: bool = true,
    request_ids: [4]u64 = undefined,
    kinds: [4]CallbackFailureKind = undefined,
    causes: [4]anyerror = undefined,
    fatal_request_ids: [4]?u64 = undefined,

    fn onResponse(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.response_count += 1;
    }

    fn onFailure(context: ?*anyopaque, failure: CallbackFailure) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.recordFailure(failure);
    }

    fn recordFailure(self: *@This(), failure: CallbackFailure) void {
        self.all_failure_callbacks_scoped =
            self.all_failure_callbacks_scoped and runtime_mod.isCallbackExecutionActive();
        std.debug.assert(self.failure_count < self.request_ids.len);
        const index = self.failure_count;
        self.request_ids[index] = failure.request_id;
        self.kinds[index] = failure.kind;
        self.causes[index] = failure.fatal.cause;
        self.fatal_request_ids[index] = failure.fatal.request_id;
        self.failure_count += 1;
    }
};

const FatalDrainBarrierProbe = struct {
    api: *ExchangeApiSharded,
    io: std.Io,
    records: FatalCallbackProbe = .{},
    entered: std.Io.Event = .unset,
    reentrant_finished: std.Io.Event = .unset,
    release: std.Io.Event = .unset,
    reentrant_result: ?(DrainPendingError!usize) = null,
    release_wait_failed: std.atomic.Value(bool) = .init(false),

    fn onResponse(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.records.response_count += 1;
    }

    fn onFailure(context: ?*anyopaque, failure: CallbackFailure) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.records.recordFailure(failure);
        if (self.records.failure_count != 1) return;

        self.entered.set(self.io);
        self.reentrant_result = self.api.drainPending();
        self.reentrant_finished.set(self.io);
        waitForShutdownEvent(
            &self.release,
            self.io,
            testWaitTimeout().toDeadline(self.io),
        ) catch {
            self.release_wait_failed.store(true, .release);
        };
    }
};

const BlockingCommandCallbackProbe = struct {
    io: std.Io,
    entered: std.Io.Event = .unset,
    release: std.Io.Event = .unset,
    response_count: usize = 0,
    release_wait_failed: std.atomic.Value(bool) = .init(false),

    fn onResponse(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.response_count += 1;
        self.entered.set(self.io);
        waitForShutdownEvent(
            &self.release,
            self.io,
            testWaitTimeout().toDeadline(self.io),
        ) catch {
            self.release_wait_failed.store(true, .release);
        };
    }
};

const CrossShardedBlockingProbe = struct {
    target: *ExchangeApiSharded,
    callback_count: usize = 0,
    sync_error: ?anyerror = null,
    sync_succeeded: bool = false,
    query_error: ?anyerror = null,
    query_succeeded: bool = false,
    process_error: ?anyerror = null,
    process_succeeded: bool = false,
    batch_error: ?anyerror = null,
    batch_succeeded: bool = false,
    composite_error: ?anyerror = null,
    composite_succeeded: bool = false,
    runtime_drain_error: ?anyerror = null,
    runtime_drain_succeeded: bool = false,
    callback_dispatch_error: ?anyerror = null,
    callback_dispatch_succeeded: bool = false,
    accepted_before_async: u64 = 0,
    next_request_before_async: u64 = 0,
    queued_before_async: usize = 0,
    immediate_ids: [4]u64 = [_]u64{0} ** 4,
    immediate_error: ?anyerror = null,
    saturated_error: ?anyerror = null,
    saturated_succeeded: bool = false,
    accepted_before_saturated: u64 = 0,
    accepted_after_saturated: u64 = 0,
    next_request_before_saturated: u64 = 0,
    next_request_after_saturated: u64 = 0,
    queued_before_saturated: usize = 0,
    queued_after_saturated: usize = 0,
    completion_before_saturated: usize = 0,
    completion_after_saturated: usize = 0,
    bootstrap_error: ?anyerror = null,
    bootstrap_succeeded: bool = false,
    startup_error: ?anyerror = null,
    startup_succeeded: bool = false,

    fn onResponse(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.callback_count += 1;
        self.trySynchronousSubmit();
        self.tryQuery();
        self.tryProcessWait();
        self.tryCommandBatch();
        self.tryComposite();
        self.tryRuntimeDrain();
        self.tryCallbackDispatch();

        const driver = self.target.driver;
        self.accepted_before_async = driver.accepted_total;
        self.next_request_before_async = self.target.next_request_id;
        self.queued_before_async = driver.queue.count();
        for (&self.immediate_ids, 0..) |*request_id, index| {
            request_id.* = self.target.addUserAsync(
                .routed,
                711_100 + @as(i64, @intCast(index)),
            ) catch |err| {
                self.immediate_error = err;
                break;
            };
        }

        self.accepted_before_saturated = driver.accepted_total;
        self.next_request_before_saturated = self.target.next_request_id;
        self.queued_before_saturated = driver.queue.count();
        self.completion_before_saturated = driver.completion_slots_used;
        _ = self.target.addUserAsync(.routed, 711_200) catch |err| {
            self.saturated_error = err;
            self.captureSaturatedAfter();
            self.tryBootstrap();
            self.tryStartup();
            return;
        };
        self.saturated_succeeded = true;
        self.captureSaturatedAfter();
        self.tryBootstrap();
        self.tryStartup();
    }

    fn trySynchronousSubmit(self: *@This()) void {
        var response = self.target.addUser(.routed, 711_001) catch |err| {
            self.sync_error = err;
            return;
        };
        self.sync_succeeded = true;
        response.deinit(self.target.allocator);
    }

    fn tryQuery(self: *@This()) void {
        _ = self.target.queryUserBalance(711_001, 11) catch |err| {
            self.query_error = err;
            return;
        };
        self.query_succeeded = true;
    }

    fn tryProcessWait(self: *@This()) void {
        const command = makeAddUserCommand(711_002);
        _ = self.target.processCommandResultCode(.routed, &command) catch |err| {
            self.process_error = err;
            return;
        };
        self.process_succeeded = true;
    }

    fn tryCommandBatch(self: *@This()) void {
        var command = makeAddUserCommand(711_003);
        var commands = [_]*cmd.OrderCommand{&command};
        _ = self.target.submitCommandBatch(.routed, commands[0..]) catch |err| {
            self.batch_error = err;
            return;
        };
        self.batch_succeeded = true;
    }

    fn tryComposite(self: *@This()) void {
        const commands = [_]ApiCommand{
            .{ .add_user = 711_004 },
            .nop,
        };
        var result = self.target.submitApiCommandsSyncResult(
            .routed,
            commands[0..],
        ) catch |err| {
            self.composite_error = err;
            return;
        };
        self.composite_succeeded = true;
        result.deinit(self.target.allocator);
    }

    fn tryRuntimeDrain(self: *@This()) void {
        _ = self.target.drainRuntime() catch |err| {
            self.runtime_drain_error = err;
            return;
        };
        self.runtime_drain_succeeded = true;
    }

    fn tryCallbackDispatch(self: *@This()) void {
        _ = self.target.processPendingCallbacks() catch |err| {
            self.callback_dispatch_error = err;
            return;
        };
        self.callback_dispatch_succeeded = true;
    }

    fn captureSaturatedAfter(self: *@This()) void {
        const driver = self.target.driver;
        self.accepted_after_saturated = driver.accepted_total;
        self.next_request_after_saturated = self.target.next_request_id;
        self.queued_after_saturated = driver.queue.count();
        self.completion_after_saturated = driver.completion_slots_used;
    }

    fn tryBootstrap(self: *@This()) void {
        self.target.beginBootstrap() catch |err| {
            self.bootstrap_error = err;
            return;
        };
        self.bootstrap_succeeded = true;
        self.target.finishBootstrap();
    }

    fn tryStartup(self: *@This()) void {
        self.target.startup() catch |err| {
            self.startup_error = err;
            return;
        };
        self.startup_succeeded = true;
    }
};

const CrossShardedAsyncProbe = struct {
    target: *ExchangeApiSharded,
    io: std.Io,
    uid: i64,
    finished: std.Io.Event = .unset,
    request_id: ?u64 = null,
    failure: ?anyerror = null,

    fn onResponse(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.request_id = self.target.addUserAsync(.routed, self.uid) catch |err| {
            self.failure = err;
            self.finished.set(self.io);
            return;
        };
        self.finished.set(self.io);
    }
};

const ShardedToCoreCycleProbe = struct {
    source: *ExchangeApiSharded,
    target: *exchange_core_mod.ExchangeCore,
    io: std.Io,
    outer_enqueued: std.Io.Event = .unset,
    inner_finished: std.Io.Event = .unset,
    outer_finished: std.Io.Event = .unset,
    outer_enqueue_error: ?anyerror = null,
    inner_sync_error: ?anyerror = null,
    inner_sync_succeeded: bool = false,
    outer_wait_failed: bool = false,

    fn outer(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.target.submitApiCommandWithCallback(.{ .add_user = 712_002 }, .{
            .context = self,
            .callback = inner,
        }) catch |err| {
            self.outer_enqueue_error = err;
            self.outer_enqueued.set(self.io);
            self.outer_finished.set(self.io);
            return;
        };
        self.outer_enqueued.set(self.io);
        waitForShutdownEvent(
            &self.inner_finished,
            self.io,
            (std.Io.Timeout{ .duration = .{
                .clock = .awake,
                .raw = .fromSeconds(1),
            } }).toDeadline(self.io),
        ) catch {
            self.outer_wait_failed = true;
        };
        self.outer_finished.set(self.io);
    }

    fn inner(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        var response = self.source.addUser(.routed, 712_003) catch |err| {
            self.inner_sync_error = err;
            self.inner_finished.set(self.io);
            return;
        };
        self.inner_sync_succeeded = true;
        response.deinit(self.source.allocator);
        self.inner_finished.set(self.io);
    }
};

const CoreToShardedCycleProbe = struct {
    source: *exchange_core_mod.ExchangeCore,
    target: *ExchangeApiSharded,
    io: std.Io,
    outer_enqueued: std.Io.Event = .unset,
    inner_finished: std.Io.Event = .unset,
    outer_finished: std.Io.Event = .unset,
    outer_enqueue_error: ?anyerror = null,
    inner_sync_error: ?anyerror = null,
    inner_sync_succeeded: bool = false,
    outer_wait_failed: bool = false,

    fn outer(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.target.submitCommandAsyncWithCallback(.routed, .{
            .add_user = 713_002,
        }, .{
            .context = self,
            .callback = inner,
        }) catch |err| {
            self.outer_enqueue_error = err;
            self.outer_enqueued.set(self.io);
            self.outer_finished.set(self.io);
            return;
        };
        self.outer_enqueued.set(self.io);
        waitForShutdownEvent(
            &self.inner_finished,
            self.io,
            (std.Io.Timeout{ .duration = .{
                .clock = .awake,
                .raw = .fromSeconds(1),
            } }).toDeadline(self.io),
        ) catch {
            self.outer_wait_failed = true;
        };
        self.outer_finished.set(self.io);
    }

    fn inner(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        var response = self.source.addUser(713_003) catch |err| {
            self.inner_sync_error = err;
            self.inner_finished.set(self.io);
            return;
        };
        self.inner_sync_succeeded = true;
        response.deinit(self.source.allocator);
        self.inner_finished.set(self.io);
    }
};

const ShardedAcceptedDrainRunner = struct {
    api: *ExchangeApiSharded,
    io: std.Io,
    finished: std.Io.Event = .unset,
    result: ?(DrainPendingError!usize) = null,

    fn run(self: *@This()) void {
        self.result = self.api.drainPending();
        const driver = self.api.driver;
        driver.mutex.lockUncancelable(self.io);
        self.finished.set(self.io);
        driver.completion_available.broadcast(self.io);
        driver.mutex.unlock(self.io);
    }
};

fn waitForAcceptedDrainWaiterCount(
    api: *ExchangeApiSharded,
    expected: usize,
) !void {
    const driver = api.driver;
    const deadline = testWaitTimeout().toDeadline(driver.io);
    while (true) {
        driver.mutex.lockUncancelable(driver.io);
        const waiter_count = driver.accepted_drain_waiters;
        driver.mutex.unlock(driver.io);
        if (waiter_count >= expected) return;
        if (shutdownDeadlineExpired(driver.io, deadline)) return error.Timeout;
        std.Thread.yield() catch {};
    }
}

fn waitForAcceptedSettlementFrontier(
    api: *ExchangeApiSharded,
    expected: u64,
) !void {
    const driver = api.driver;
    const deadline = testWaitTimeout().toDeadline(driver.io);
    while (true) {
        driver.mutex.lockUncancelable(driver.io);
        const frontier = driver.settled_acceptance_frontier;
        driver.mutex.unlock(driver.io);
        if (frontier >= expected) return;
        if (shutdownDeadlineExpired(driver.io, deadline)) return error.Timeout;
        std.Thread.yield() catch {};
    }
}

fn expectAcceptedDrainBlocked(
    api: *ExchangeApiSharded,
    runner: *const ShardedAcceptedDrainRunner,
) !void {
    const driver = api.driver;
    try waitForAcceptedDrainWaiterCount(api, 1);
    driver.mutex.lockUncancelable(driver.io);
    const finished = runner.finished.isSet();
    driver.mutex.unlock(driver.io);
    try std.testing.expect(!finished);
}

const ShardedBinaryAsyncPublisher = struct {
    api: *ExchangeApiSharded,
    mode: ExecutionMode,
    command_type: cmd.OrderCommandType = .binary_data_command,
    payload: []const u8,
    transfer_id: i32,
    result: ?(BinaryPayloadAsyncSubmitError!u64) = null,

    fn run(self: *@This()) void {
        self.result = self.api.submitBinaryPayloadAsync(
            self.mode,
            self.command_type,
            self.transfer_id,
            77,
            self.payload,
        );
    }
};

const ShardedOrdinaryPublisher = struct {
    api: *ExchangeApiSharded,
    mode: ExecutionMode,
    uid: i64,
    result: ?(AsyncSubmitError!u64) = null,

    fn run(self: *@This()) void {
        self.result = self.api.addUserAsync(self.mode, self.uid);
    }
};

const ShardedBackpressureGate = struct {
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    condition: std.Io.Condition = .init,
    entered: bool = false,
    released: bool = false,

    fn onResult(context: ?*anyopaque, _: runtime_mod.ResultEvent) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.mutex.lockUncancelable(self.io);
        self.entered = true;
        self.condition.broadcast(self.io);
        while (!self.released) self.condition.waitUncancelable(self.io, &self.mutex);
        self.mutex.unlock(self.io);
    }

    fn waitEntered(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        while (!self.entered) self.condition.waitUncancelable(self.io, &self.mutex);
    }

    fn release(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        self.released = true;
        self.condition.broadcast(self.io);
        self.mutex.unlock(self.io);
    }
};

const ShardedShutdownRunner = struct {
    api: *ExchangeApiSharded,
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    condition: std.Io.Condition = .init,
    entered: bool = false,
    done: bool = false,

    fn run(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        self.entered = true;
        self.condition.broadcast(self.io);
        self.mutex.unlock(self.io);
        self.api.shutdown();
        self.mutex.lockUncancelable(self.io);
        self.done = true;
        self.condition.broadcast(self.io);
        self.mutex.unlock(self.io);
    }

    fn waitEntered(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        while (!self.entered) self.condition.waitUncancelable(self.io, &self.mutex);
    }

    fn isDone(self: *@This()) bool {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.done;
    }
};

const ShardedDriverLockGate = struct {
    api: *ExchangeApiSharded,
    io: std.Io,
    entered: std.Io.Event = .unset,
    release: std.Io.Event = .unset,

    fn run(self: *@This()) void {
        self.api.driver.mutex.lockUncancelable(self.io);
        self.entered.set(self.io);
        self.release.waitUncancelable(self.io);
        self.api.driver.mutex.unlock(self.io);
    }
};

fn immediateShutdownTimeout() std.Io.Timeout {
    return .{
        .duration = .{
            .clock = .awake,
            .raw = .fromNanoseconds(0),
        },
    };
}

fn shortShutdownTimeout() std.Io.Timeout {
    return .{
        .duration = .{
            .clock = .awake,
            .raw = .fromMilliseconds(1),
        },
    };
}

fn testWaitTimeout() std.Io.Timeout {
    return .{
        .duration = .{
            .clock = .awake,
            .raw = .fromSeconds(5),
        },
    };
}

fn waitForTestEvent(event: *std.Io.Event, io: std.Io) !void {
    try waitForShutdownEvent(event, io, testWaitTimeout().toDeadline(io));
}

fn waitForTestAtomicTrue(
    flag: *const std.atomic.Value(bool),
    io: std.Io,
) !void {
    const deadline = testWaitTimeout().toDeadline(io);
    while (!flag.load(.acquire)) {
        if (shutdownDeadlineExpired(io, deadline)) return error.Timeout;
        std.Thread.yield() catch {};
    }
}

fn waitForTestResultsReady(
    api: *const ExchangeApiSharded,
    request_ids: []const u64,
) !void {
    const deadline = testWaitTimeout().toDeadline(api.driver.io);
    while (true) {
        var all_ready = true;
        for (request_ids) |request_id| {
            if (!api.isResultReady(request_id)) {
                all_ready = false;
                break;
            }
        }
        if (all_ready) return;
        if (shutdownDeadlineExpired(api.driver.io, deadline)) {
            return error.Timeout;
        }
        std.Thread.yield() catch {};
    }
}

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

const RawShapeProbe = struct {
    api: *ExchangeApiSharded,
    current_request_id: u64 = 0,
    callback_count: usize = 0,
    matcher_seen: bool = false,
    completed_command_metadata_seen: bool = false,
    l2_seen: bool = false,
    report_seen: bool = false,
    java_report_seen: bool = false,
    poll_was_ready: bool = false,

    fn onResult(context: ?*anyopaque, response: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.callback_count += 1;
        if (self.current_request_id != 0 and
            self.api.isResultReady(self.current_request_id))
        {
            self.poll_was_ready = true;
        }
        switch (response.command.command) {
            .cancel_order => {
                self.matcher_seen = response.matcher_events_head != null;
                self.completed_command_metadata_seen =
                    response.sequence != 0 and
                    response.result_code == .success and
                    response.command.result_code == response.result_code and
                    response.command.order_id == 131_001 and
                    response.command.uid == 11_001 and
                    response.command.symbol == 241;
            },
            .order_book_request => self.l2_seen = response.l2_snapshot != null,
            .binary_data_query => {
                if (response.report_response != null) self.report_seen = true;
                if (response.java_report_response != null) self.java_report_seen = true;
            },
            else => {},
        }
    }
};

const ObserverOrder = enum {
    raw,
    runtime,
    simple,
    request,
};

const ObserverOrderProbe = struct {
    events: [8]ObserverOrder = undefined,
    len: usize = 0,
    all_callbacks_scoped: bool = true,

    fn append(self: *@This(), event: ObserverOrder) void {
        std.debug.assert(self.len < self.events.len);
        self.all_callbacks_scoped =
            self.all_callbacks_scoped and runtime_mod.isCallbackExecutionActive();
        self.events[self.len] = event;
        self.len += 1;
    }

    fn clear(self: *@This()) void {
        self.len = 0;
    }

    fn raw(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.append(.raw);
    }

    fn runtime(context: ?*anyopaque, _: runtime_mod.ResultEvent) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.append(.runtime);
    }

    fn simple(
        context: ?*anyopaque,
        _: simple_events_mod.ApiCommandResult,
    ) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.append(.simple);
    }

    fn request(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.append(.request);
    }
};

const CrossBackendSetterOutcome = enum {
    pending,
    succeeded,
    would_block,
    unexpected_failure,
};

const CrossBackendCallbackGate = struct {
    io: std.Io,
    entered: std.Io.Event = .unset,
    setter_finished: std.Io.Event = .unset,
    callback_finished: std.Io.Event = .unset,
    bounded_wait_failed: std.atomic.Value(bool) = .init(false),

    fn holdRaw(
        context: ?*anyopaque,
        _: *const kernel_mod.CommandResponse,
    ) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.entered.set(self.io);
        defer self.callback_finished.set(self.io);
        self.setter_finished.waitTimeout(self.io, .{
            .duration = .{
                .clock = .awake,
                .raw = .fromSeconds(5),
            },
        }) catch {
            self.bounded_wait_failed.store(true, .release);
        };
    }

    fn waitEntered(self: *@This()) !void {
        try self.entered.waitTimeout(self.io, .{
            .duration = .{
                .clock = .awake,
                .raw = .fromSeconds(5),
            },
        });
    }

    fn waitFinished(self: *@This()) !void {
        try self.callback_finished.waitTimeout(self.io, .{
            .duration = .{
                .clock = .awake,
                .raw = .fromSeconds(5),
            },
        });
    }
};

const CoreToShardedSetterProbe = struct {
    target: *ExchangeApiSharded,
    gate: *CrossBackendCallbackGate,
    outcome: CrossBackendSetterOutcome = .pending,
    unexpected_failure: ?anyerror = null,

    fn onRaw(
        context: ?*anyopaque,
        _: *const kernel_mod.CommandResponse,
    ) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        defer self.gate.setter_finished.set(self.gate.io);
        self.target.setResultSinkChecked(.{}) catch |err| {
            switch (err) {
                error.CallbackReconfigurationWouldBlock => self.outcome = .would_block,
                else => {
                    self.outcome = .unexpected_failure;
                    self.unexpected_failure = err;
                },
            }
            return;
        };
        self.outcome = .succeeded;
    }
};

const ShardedToCoreSetterProbe = struct {
    target: *exchange_core_mod.ExchangeCore,
    gate: *CrossBackendCallbackGate,
    outcome: CrossBackendSetterOutcome = .pending,
    unexpected_failure: ?anyerror = null,

    fn onRaw(
        context: ?*anyopaque,
        _: *const kernel_mod.CommandResponse,
    ) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        defer self.gate.setter_finished.set(self.gate.io);
        self.target.setResultSinkChecked(.{}) catch |err| {
            switch (err) {
                error.CallbackReconfigurationWouldBlock => self.outcome = .would_block,
                else => {
                    self.outcome = .unexpected_failure;
                    self.unexpected_failure = err;
                },
            }
            return;
        };
        self.outcome = .succeeded;
    }
};

const GroupingObserverProbe = struct {
    raw_sequences: [8]u64 = undefined,
    raw_len: usize = 0,
    runtime_sequences: [8]u64 = undefined,
    runtime_len: usize = 0,
    simple_sequences: [8]u64 = undefined,
    simple_len: usize = 0,

    fn raw(context: ?*anyopaque, response: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.raw_sequences[self.raw_len] = response.sequence;
        self.raw_len += 1;
    }

    fn runtime(context: ?*anyopaque, event: runtime_mod.ResultEvent) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.runtime_sequences[self.runtime_len] = event.sequence;
        self.runtime_len += 1;
    }

    fn simple(
        context: ?*anyopaque,
        event: simple_events_mod.ApiCommandResult,
    ) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.simple_sequences[self.simple_len] = event.sequence;
        self.simple_len += 1;
    }
};

const RawJournalProbe = struct {
    api: *ExchangeApiSharded,
    count: usize = 0,
    durable_visible: bool = true,

    fn onResult(context: ?*anyopaque, response: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.count += 1;
        const journal = self.api.journal orelse {
            self.durable_visible = false;
            return;
        };
        if (journal.entries.items.len == 0 or
            journal.entries.items[journal.entries.items.len - 1].sequence != response.sequence or
            self.api.durable_journal == null or
            self.api.durable_journal.?.lastSequence() != response.sequence)
        {
            self.durable_visible = false;
        }
    }
};

const RawR2Probe = struct {
    count: usize = 0,
    sequence: u64 = 0,

    fn onResult(context: ?*anyopaque, response: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.count += 1;
        self.sequence = response.sequence;
    }
};

const TypedTimestampRawProbe = struct {
    sequences: [16]u64 = undefined,
    command_types: [16]cmd.OrderCommandType = undefined,
    timestamps: [16]i64 = undefined,
    result_codes: [16]cmd.CommandResultCode = undefined,
    len: usize = 0,

    fn onResult(context: ?*anyopaque, response: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        std.debug.assert(self.len < self.sequences.len);
        self.sequences[self.len] = response.sequence;
        self.command_types[self.len] = response.command.command;
        self.timestamps[self.len] = response.command.timestamp;
        self.result_codes[self.len] = response.result_code;
        self.len += 1;
    }
};

const TypedTimestampCallbackProbe = struct {
    timestamps: [4]i64 = undefined,
    len: usize = 0,

    fn onResponse(context: ?*anyopaque, response: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        std.debug.assert(self.len < self.timestamps.len);
        self.timestamps[self.len] = response.command.timestamp;
        self.len += 1;
    }
};

const ExactRawSequenceProbe = struct {
    api: *ExchangeApiSharded,
    pending: ?custom_report_pipeline.PendingRequest = null,
    expected_custom_transfer_id: i32 = 0,
    expected_custom_timestamp: i64 = 0,
    sequences: [64]u64 = undefined,
    len: usize = 0,
    custom_storage_was_ready: bool = false,
    custom_storage_check_failed: bool = false,
    custom_terminal_count: usize = 0,
    custom_terminal_metadata_seen: bool = false,
    custom_binary_event_chain_valid: bool = true,
    custom_binary_event_sections: [8]i32 = undefined,
    custom_binary_event_len: usize = 0,

    fn onResult(context: ?*anyopaque, response: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        std.debug.assert(self.len < self.sequences.len);
        self.sequences[self.len] = response.sequence;
        self.len += 1;
        if (response.command.command == .binary_data_query and
            response.command.symbol == -1)
        {
            self.custom_terminal_count += 1;
            self.custom_terminal_metadata_seen =
                response.result_code == .success and
                response.command.result_code == response.result_code and
                response.command.user_cookie == self.expected_custom_transfer_id and
                response.command.timestamp == self.expected_custom_timestamp;
            var node = response.matcher_events_head;
            while (node) |event| : (node = event.next_event) {
                if (event.event_type != .binary_event or
                    self.custom_binary_event_len == self.custom_binary_event_sections.len)
                {
                    self.custom_binary_event_chain_valid = false;
                    break;
                }
                self.custom_binary_event_sections[self.custom_binary_event_len] = event.section;
                self.custom_binary_event_len += 1;
            }
        }
        if (self.pending) |pending| {
            const ready = self.api.isCustomReportResultReady(pending) catch {
                self.custom_storage_check_failed = true;
                return;
            };
            if (ready) self.custom_storage_was_ready = true;
        }
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

const ColdCallbackShutdownProbe = struct {
    api: *ExchangeApiSharded,
    callback_count: usize = 0,
    startup_error: ?anyerror = null,
    shutdown_outcome: ?ShutdownOutcome = null,
    shutdown_error: ?anyerror = null,

    fn onCommandResult(
        context: ?*anyopaque,
        _: simple_events_mod.ApiCommandResult,
    ) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.callback_count += 1;
        self.api.startup() catch |err| {
            self.startup_error = err;
        };
        self.shutdown_outcome = self.api.shutdownTimeout(.none) catch |err| {
            self.shutdown_error = err;
            return;
        };
    }
};

const RunningCallbackShutdownProbe = struct {
    api: *ExchangeApiSharded,
    callback_count: std.atomic.Value(usize) = .init(0),
    shutdown_outcome: ?ShutdownOutcome = null,
    shutdown_error: ?anyerror = null,

    fn onCommandResult(
        context: ?*anyopaque,
        _: simple_events_mod.ApiCommandResult,
    ) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        _ = self.callback_count.fetchAdd(1, .acq_rel);
        self.shutdown_outcome = self.api.shutdownTimeout(.none) catch |err| {
            self.shutdown_error = err;
            return;
        };
    }
};

const SimpleEventsQuiescenceProbe = struct {
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    condition: std.Io.Condition = .init,
    old_entered: bool = false,
    release_old: bool = false,
    old_count: usize = 0,
    new_count: usize = 0,

    fn oldCommand(context: ?*anyopaque, _: simple_events_mod.ApiCommandResult) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.mutex.lockUncancelable(self.io);
        self.old_count += 1;
        self.old_entered = true;
        self.condition.broadcast(self.io);
        while (!self.release_old) self.condition.waitUncancelable(self.io, &self.mutex);
        self.mutex.unlock(self.io);
    }

    fn newCommand(context: ?*anyopaque, _: simple_events_mod.ApiCommandResult) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.mutex.lockUncancelable(self.io);
        self.new_count += 1;
        self.condition.broadcast(self.io);
        self.mutex.unlock(self.io);
    }

    fn waitOldEntered(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        while (!self.old_entered) self.condition.waitUncancelable(self.io, &self.mutex);
    }

    fn releaseOld(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        self.release_old = true;
        self.condition.broadcast(self.io);
        self.mutex.unlock(self.io);
    }

    fn counts(self: *@This()) [2]usize {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return .{ self.old_count, self.new_count };
    }
};

const SimpleEventsSetterRunner = struct {
    api: *ExchangeApiSharded,
    probe: *SimpleEventsQuiescenceProbe,
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    condition: std.Io.Condition = .init,
    entered: bool = false,
    done: bool = false,
    failure: ?anyerror = null,

    fn run(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        self.entered = true;
        self.condition.broadcast(self.io);
        self.mutex.unlock(self.io);
        self.api.setSimpleEventsHandlerChecked(.{
            .context = self.probe,
            .on_command_result = SimpleEventsQuiescenceProbe.newCommand,
        }) catch |err| {
            self.failure = err;
        };
        self.mutex.lockUncancelable(self.io);
        self.done = true;
        self.condition.broadcast(self.io);
        self.mutex.unlock(self.io);
    }

    fn waitEntered(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        while (!self.entered) self.condition.waitUncancelable(self.io, &self.mutex);
    }

    fn isDone(self: *@This()) bool {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.done;
    }
};

const ResultSinkQuiescenceProbe = struct {
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    condition: std.Io.Condition = .init,
    old_entered: bool = false,
    release_old: bool = false,
    old_count: usize = 0,
    new_count: usize = 0,

    fn oldResult(context: ?*anyopaque, _: runtime_mod.ResultEvent) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.mutex.lockUncancelable(self.io);
        self.old_count += 1;
        self.old_entered = true;
        self.condition.broadcast(self.io);
        while (!self.release_old) self.condition.waitUncancelable(self.io, &self.mutex);
        self.mutex.unlock(self.io);
    }

    fn newResult(context: ?*anyopaque, _: runtime_mod.ResultEvent) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.mutex.lockUncancelable(self.io);
        self.new_count += 1;
        self.condition.broadcast(self.io);
        self.mutex.unlock(self.io);
    }

    fn waitOldEntered(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        while (!self.old_entered) self.condition.waitUncancelable(self.io, &self.mutex);
    }

    fn releaseOld(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        self.release_old = true;
        self.condition.broadcast(self.io);
        self.mutex.unlock(self.io);
    }

    fn counts(self: *@This()) [2]usize {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return .{ self.old_count, self.new_count };
    }
};

const ResultSinkSetterRunner = struct {
    api: *ExchangeApiSharded,
    probe: *ResultSinkQuiescenceProbe,
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    condition: std.Io.Condition = .init,
    entered: bool = false,
    done: bool = false,
    failure: ?anyerror = null,

    fn run(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        self.entered = true;
        self.condition.broadcast(self.io);
        self.mutex.unlock(self.io);
        self.api.setResultSinkChecked(.{
            .context = self.probe,
            .on_result = ResultSinkQuiescenceProbe.newResult,
        }) catch |err| {
            self.failure = err;
        };
        self.mutex.lockUncancelable(self.io);
        self.done = true;
        self.condition.broadcast(self.io);
        self.mutex.unlock(self.io);
    }

    fn waitEntered(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        while (!self.entered) self.condition.waitUncancelable(self.io, &self.mutex);
    }

    fn isDone(self: *@This()) bool {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.done;
    }
};

const RawResultQuiescenceProbe = struct {
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    condition: std.Io.Condition = .init,
    old_entered: bool = false,
    release_old: bool = false,
    old_count: usize = 0,
    new_count: usize = 0,

    fn oldResult(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.mutex.lockUncancelable(self.io);
        self.old_count += 1;
        self.old_entered = true;
        self.condition.broadcast(self.io);
        while (!self.release_old) self.condition.waitUncancelable(self.io, &self.mutex);
        self.mutex.unlock(self.io);
    }

    fn newResult(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.mutex.lockUncancelable(self.io);
        self.new_count += 1;
        self.condition.broadcast(self.io);
        self.mutex.unlock(self.io);
    }

    fn waitOldEntered(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        while (!self.old_entered) self.condition.waitUncancelable(self.io, &self.mutex);
    }

    fn releaseOld(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        self.release_old = true;
        self.condition.broadcast(self.io);
        self.mutex.unlock(self.io);
    }

    fn counts(self: *@This()) [2]usize {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return .{ self.old_count, self.new_count };
    }
};

const RawResultSetterRunner = struct {
    api: *ExchangeApiSharded,
    probe: *RawResultQuiescenceProbe,
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    condition: std.Io.Condition = .init,
    entered: bool = false,
    done: bool = false,
    failure: ?anyerror = null,

    fn run(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        self.entered = true;
        self.condition.broadcast(self.io);
        self.mutex.unlock(self.io);
        self.api.setRawResultSinkChecked(.{
            .context = self.probe,
            .on_result = RawResultQuiescenceProbe.newResult,
        }) catch |err| {
            self.failure = err;
        };
        self.mutex.lockUncancelable(self.io);
        self.done = true;
        self.condition.broadcast(self.io);
        self.mutex.unlock(self.io);
    }

    fn waitEntered(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        while (!self.entered) self.condition.waitUncancelable(self.io, &self.mutex);
    }

    fn isDone(self: *@This()) bool {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.done;
    }
};

const RawReconfigureShutdownProbe = struct {
    api: *ExchangeApiSharded,
    first_count: usize = 0,
    replacement_count: usize = 0,
    request_id: ?u64 = null,
    reconfiguration_failure: ?anyerror = null,
    enqueue_failure: ?anyerror = null,
    synchronous_wait_failure: ?anyerror = null,

    fn first(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.first_count += 1;
        self.checkSynchronousWait();
        self.api.setRawResultSinkChecked(.{
            .context = self,
            .on_result = replacement,
        }) catch |err| {
            self.reconfiguration_failure = err;
            return;
        };
        self.request_id = self.api.addUserAsync(.routed, 131_132) catch |err| {
            self.enqueue_failure = err;
            return;
        };
        self.api.shutdown();
    }

    fn checkSynchronousWait(self: *@This()) void {
        var response = self.api.addUser(.routed, 131_133) catch |err| {
            self.synchronous_wait_failure = err;
            return;
        };
        response.deinit(self.api.allocator);
    }

    fn replacement(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.replacement_count += 1;
    }
};

const PersistPairTakeGate = struct {
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    condition: std.Io.Condition = .init,
    waiting: usize = 0,
    released: bool = false,

    fn arriveAndWait(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        self.waiting += 1;
        self.condition.broadcast(self.io);
        while (!self.released) self.condition.waitUncancelable(self.io, &self.mutex);
        self.mutex.unlock(self.io);
    }

    fn releaseWhenReady(self: *@This(), expected: usize) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        while (self.waiting != expected) self.condition.waitUncancelable(self.io, &self.mutex);
        self.released = true;
        self.condition.broadcast(self.io);
    }
};

const PersistPairTakeRunner = struct {
    api: *ExchangeApiSharded,
    requests: PersistStateRequestPair,
    gate: *PersistPairTakeGate,
    result: ?cmd.CommandResultCode = null,

    fn run(self: *@This()) void {
        self.gate.arriveAndWait();
        self.result = self.api.takePersistStateResult(self.requests);
    }
};

const CustomReportTakeOutcome = enum {
    result,
    not_ready,
    consumed,
    unexpected_error,
};

const CustomReportTakeRunner = struct {
    api: *ExchangeApiSharded,
    pending: custom_report_pipeline.PendingRequest,
    gate: *PersistPairTakeGate,
    outcome: ?CustomReportTakeOutcome = null,
    failure: ?anyerror = null,

    fn run(self: *@This()) void {
        self.gate.arriveAndWait();
        const maybe_result = self.api.takeCustomReportResult(self.pending) catch |err| {
            self.failure = err;
            self.outcome = if (err == error.RequestConsumedOrUnknown)
                .consumed
            else
                .unexpected_error;
            return;
        };
        if (maybe_result) |result| {
            result.deinit();
            self.outcome = .result;
        } else {
            self.outcome = .not_ready;
        }
    }
};

const CustomReportProcessRunner = struct {
    api: *ExchangeApiSharded,
    query: *const report_registry.QueryHandle,
    failure: ?anyerror = null,
    unexpectedly_succeeded: bool = false,

    fn run(self: *@This()) void {
        const result = self.api.processCustomReport(
            .routed,
            72_398,
            9_900_000_098,
            self.query,
        ) catch |err| {
            self.failure = err;
            return;
        };
        result.deinit();
        self.unexpectedly_succeeded = true;
    }
};

const CustomReportReentrantProbe = struct {
    api: *ExchangeApiSharded,
    query: *const report_registry.QueryHandle,
    failure: ?anyerror = null,
    unexpectedly_succeeded: bool = false,

    fn onResult(context: ?*anyopaque, _: runtime_mod.ResultEvent) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        const result = self.api.processCustomReport(
            .routed,
            72_399,
            9_900_000_099,
            self.query,
        ) catch |err| {
            self.failure = err;
            return;
        };
        result.deinit();
        self.unexpectedly_succeeded = true;
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
    // This fixture intentionally deletes/corrupts Java journal partitions to
    // exercise their discovery rules. Keep the independent native XCJ3 lane
    // out of scope so its stricter frontier cannot mask the intended fault.
    var source = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRootInternal(
        allocator,
        tmp.dir,
        source_cfg,
        false,
    );
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
    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try source.persistState(.routed, @intCast(snapshot_id), 9_975_000_000 + seed_offset),
    );
    const snapshot_base_seq = source.persistenceCurrentSnapshot().seq;

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
            0,
        ),
        .serialization_cfg = config_mod.SerializationConfig.diskJournaling(),
    };
    var target = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRootInternal(
        allocator,
        tmp.dir,
        target_cfg,
        false,
    );
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

const ShardedApiCustomQuery = struct {
    bias: i64,
    reentry_api: ?*ExchangeApiSharded = null,
};

const ShardedApiCustomSection = struct {
    value: i64,
};

const ShardedApiCustomResult = struct {
    value: i64,
    section_ids: []i32,
};

const ShardedApiCustomSpec = struct {
    pub const code: i32 = 72_301;
    pub const schema_id: [report_registry.schema_id_len]u8 =
        [_]u8{0xA7} ** report_registry.schema_id_len;
    pub const schema_version: u32 = 1;
    pub const Query = ShardedApiCustomQuery;
    pub const Section = ShardedApiCustomSection;
    pub const Result = ShardedApiCustomResult;
    pub const MatchingView = kernel_mod.MatchingReportView;
    pub const RiskView = kernel_mod.RiskReportView;
    var reduce_calls: std.atomic.Value(usize) = .init(0);

    pub fn cloneQuery(
        _: std.mem.Allocator,
        query: *const Query,
    ) report_registry.ResourceError!Query {
        return query.*;
    }

    pub fn deinitQuery(_: std.mem.Allocator, query: *Query) void {
        query.* = undefined;
    }

    pub fn encodeQuery(
        _: std.mem.Allocator,
        writer: *report_registry.EncodingWriter,
        query: *const Query,
    ) report_registry.EncodingError!void {
        try writer.writeInt(i64, query.bias, .little);
    }

    pub fn decodeQuery(
        _: std.mem.Allocator,
        body: []const u8,
    ) report_registry.QueryConstructionError!Query {
        if (body.len != @sizeOf(i64)) return error.InvalidQuery;
        return .{
            .bias = std.mem.readInt(
                i64,
                body[0..@sizeOf(i64)],
                .little,
            ),
            .reentry_api = null,
        };
    }

    pub fn processMatching(
        _: std.mem.Allocator,
        query: *const Query,
        context: report_registry.MatchingContext(MatchingView),
    ) report_registry.HandlerError!?Section {
        if (query.bias == std.math.minInt(i64)) return error.HandlerFailed;
        return .{
            .value = query.bias +% 1_000 +% @as(i64, context.shard_id),
        };
    }

    pub fn processRisk(
        _: std.mem.Allocator,
        query: *const Query,
        context: report_registry.RiskContext(RiskView),
    ) report_registry.HandlerError!?Section {
        if (query.bias == std.math.minInt(i64)) return error.HandlerFailed;
        return .{
            .value = query.bias +% 100 +% @as(i64, context.shard_id),
        };
    }

    pub fn encodeSection(
        _: std.mem.Allocator,
        writer: *report_registry.EncodingWriter,
        section: *const Section,
    ) report_registry.EncodingError!void {
        try writer.writeInt(i64, section.value, .little);
    }

    pub fn deinitSection(_: std.mem.Allocator, section: *Section) void {
        section.* = undefined;
    }

    pub fn reduce(
        allocator: std.mem.Allocator,
        query: *const Query,
        sections: []const report_registry.SectionBytes,
    ) report_registry.ReductionError!Result {
        _ = reduce_calls.fetchAdd(1, .monotonic);
        if (query.reentry_api) |api| {
            if (!api.driver.mutex.tryLock()) return error.ReducerFailed;
            api.driver.mutex.unlock(api.driver.io);
            _ = api.readyCount();
        }
        const section_ids = try allocator.alloc(i32, sections.len);
        errdefer allocator.free(section_ids);
        var value = query.bias;
        for (sections, 0..) |section, index| {
            if (section.padded_bytes.len < @sizeOf(i64)) {
                return error.InvalidSection;
            }
            section_ids[index] = section.section_id;
            value +%= std.mem.readInt(
                i64,
                section.padded_bytes[0..@sizeOf(i64)],
                .little,
            );
        }
        return .{ .value = value, .section_ids = section_ids };
    }

    pub fn deinitResult(allocator: std.mem.Allocator, result: *Result) void {
        allocator.free(result.section_ids);
        result.* = undefined;
    }
};

fn customReportConfig(
    registry: *const report_registry.Registry,
    shard_count: u32,
) Config {
    var config = Config.javaDefault();
    config.matching.num_shards = shard_count;
    config.reports_queries.custom_report_registry = registry;
    return config;
}

fn shardedLockedAllocatorInitFailureCampaign(allocator: std.mem.Allocator) !void {
    var api = try ExchangeApiSharded.init(allocator, 2, .direct);
    api.deinit();
}

const LockedAllocatorResultProducer = struct {
    api: *ExchangeApiSharded,
    published: []std.atomic.Value(u64),
    failure: ?anyerror = null,

    fn run(self: *@This()) void {
        for (self.published, 0..) |*slot, index| {
            const request_id = self.api.requestSingleUserReportAsync(
                .routed,
                @intCast(index + 1),
                @intCast(9_980_000_000 + index),
                880_001,
            ) catch |err| {
                self.failure = err;
                slot.store(std.math.maxInt(u64), .release);
                return;
            };
            slot.store(request_id + 1, .release);
        }
    }
};

test "sharded locked allocator cleans every init failure without leaks" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        shardedLockedAllocatorInitFailureCampaign,
        .{},
    );
}

test "sharded locked allocator cold deinit releases all wrapped ownership" {
    const backing_allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(backing_allocator, 2, .direct);
    try std.testing.expectEqual(
        @as(*anyopaque, @ptrCast(api.locked_allocator)),
        api.allocator.ptr,
    );
    api.deinit();
}

test "sharded locked allocator running deinit joins before releasing wrapped ownership" {
    const backing_allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(backing_allocator, 2, .direct);
    try api.startup();
    for (0..16) |index| {
        _ = try api.requestSingleUserReportAsync(
            .routed,
            @intCast(index + 1),
            @intCast(9_981_000_000 + index),
            881_001,
        );
    }
    api.deinit();
}

test "sharded transferred response outlives locked allocator wrapper" {
    const backing_allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(backing_allocator, 2, .direct);
    try api.startup();
    const request_id = try api.requestSingleUserReportAsync(
        .routed,
        1,
        9_981_100_000,
        881_101,
    );
    var response = while (true) {
        if (api.takeResult(request_id)) |ready| break ready;
        std.Thread.yield() catch {};
    };
    try std.testing.expect(response.report_response != null);

    api.deinit();
    response.deinit(backing_allocator);
}

test "sharded locked allocator supports concurrent producer and transferred result ownership" {
    const backing_allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(backing_allocator, 2, .direct);
    defer api.deinit();
    try api.startup();

    var published: [64]std.atomic.Value(u64) = undefined;
    for (&published) |*slot| slot.* = .init(0);
    var producer = LockedAllocatorResultProducer{
        .api = &api,
        .published = &published,
    };
    const producer_thread = try std.Thread.spawn(
        .{},
        LockedAllocatorResultProducer.run,
        .{&producer},
    );
    defer producer_thread.join();

    for (&published) |*slot| {
        var encoded_request_id = slot.load(.acquire);
        while (encoded_request_id == 0) {
            std.Thread.yield() catch {};
            encoded_request_id = slot.load(.acquire);
        }
        if (encoded_request_id == std.math.maxInt(u64)) {
            return producer.failure orelse error.UnexpectedProducerFailure;
        }
        const request_id = encoded_request_id - 1;
        var response = while (true) {
            if (api.takeResult(request_id)) |ready| break ready;
            std.Thread.yield() catch {};
        };
        try std.testing.expect(response.report_response != null);
        // The API remains live, so release through the shared facade. Direct
        // backing-allocator release is reserved for a thread-safe backing
        // allocator or for ownership that outlives API quiescence.
        api.releaseResponse(&response);
    }
    try std.testing.expectEqual(@as(?anyerror, null), producer.failure);
}

test "sharded detached typed results release through live facade during worker activity" {
    const backing_allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(backing_allocator, 2, .direct);
    defer api.deinit();
    _ = try runBasicFlow(&api, .routed);
    try api.startup();

    var published: [64]std.atomic.Value(u64) = undefined;
    for (&published) |*slot| slot.* = .init(0);
    var producer = LockedAllocatorResultProducer{
        .api = &api,
        .published = &published,
    };
    const producer_thread = try std.Thread.spawn(
        .{},
        LockedAllocatorResultProducer.run,
        .{&producer},
    );
    defer producer_thread.join();

    var generic = try api.queryReport(.state_hash_detailed);
    api.releaseReportResponse(&generic);
    var detailed = try api.queryStateHashDetailedReport();
    api.releaseDetailedStateHashReport(&detailed);
    var single_user = try api.querySingleUserReport(11_001);
    api.releaseSingleUserReport(&single_user);
    var total = try api.queryTotalCurrencyBalanceReport();
    api.releaseTotalCurrencyBalanceReport(&total);
    var order_book = try api.queryOrderBook(241, 8);
    api.releaseOrderBookSnapshot(&order_book);

    const generic_handle = try api.requestStateHashAsync(
        .routed,
        1_001,
        9_982_001_001,
    );
    const detailed_handle = try api.requestStateHashAsync(
        .routed,
        1_002,
        9_982_001_002,
    );
    const single_user_handle = try api.requestSingleUserReportAsync(
        .routed,
        1_003,
        9_982_001_003,
        11_001,
    );
    const total_handle = try api.requestTotalCurrencyBalanceAsync(
        .routed,
        1_004,
        9_982_001_004,
    );
    const order_book_handle = try api.requestOrderBookAsync(.routed, 241, 8);
    _ = try api.drainPending();

    var generic_async = (try api.takeReportResponse(generic_handle)) orelse
        return error.MissingAsyncResult;
    api.releaseReportResponse(&generic_async);
    var detailed_async = (try api.takeStateHashDetailed(detailed_handle)) orelse
        return error.MissingAsyncResult;
    api.releaseDetailedStateHashReport(&detailed_async);
    var single_user_async = (try api.takeSingleUserReport(single_user_handle)) orelse
        return error.MissingAsyncResult;
    api.releaseSingleUserReport(&single_user_async);
    var total_async = (try api.takeTotalCurrencyBalanceReport(total_handle)) orelse
        return error.MissingAsyncResult;
    api.releaseTotalCurrencyBalanceReport(&total_async);
    var order_book_async = (try api.takeOrderBookSnapshot(order_book_handle)) orelse
        return error.MissingAsyncResult;
    api.releaseOrderBookSnapshot(&order_book_async);

    for (&published) |*slot| {
        var encoded_request_id = slot.load(.acquire);
        while (encoded_request_id == 0) {
            std.Thread.yield() catch {};
            encoded_request_id = slot.load(.acquire);
        }
        if (encoded_request_id == std.math.maxInt(u64)) {
            return producer.failure orelse error.UnexpectedProducerFailure;
        }
        const request_id = encoded_request_id - 1;
        var response = while (true) {
            if (api.takeResult(request_id)) |ready| break ready;
            std.Thread.yield() catch {};
        };
        api.releaseResponse(&response);
    }
    try std.testing.expectEqual(@as(?anyerror, null), producer.failure);
}

test "sharded detached typed result can outlive wrapper via backing allocator" {
    const backing_allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(backing_allocator, 2, .direct);
    _ = try runBasicFlow(&api, .routed);
    var total = try api.queryTotalCurrencyBalanceReport();
    try std.testing.expect(total.account_balances.count() > 0);

    api.deinit();
    total.deinit(backing_allocator);
}

test "sharded custom reports survive source registry destruction and cold API move" {
    const allocator = std.testing.allocator;
    const Adapted = report_registry.Adapter(ShardedApiCustomSpec);
    const descriptors = [_]*const report_registry.Descriptor{
        Adapted.descriptorPtr(),
    };
    const source_registry = try report_registry.Registry.initValidated(
        allocator,
        &descriptors,
    );
    var api = try ExchangeApiSharded.initWithPersistenceRoot(
        allocator,
        std.Io.Dir.cwd(),
        customReportConfig(source_registry, 2),
    );
    source_registry.deinit();

    try std.testing.expectEqual(
        @as(usize, 1),
        api.configRef().reports_queries.custom_report_registry.len(),
    );
    const query_value: ShardedApiCustomQuery = .{ .bias = 10 };
    const query = try Adapted.cloneQuery(allocator, &query_value);
    defer query.deinit();
    const pending = try api.submitCustomReportAsync(
        .routed,
        72_301,
        9_900_000_001,
        query,
    );
    const pending_alias = pending;

    // Cold APIs are explicitly relocatable. RequestStore is heap-stable so a
    // token created before this value move remains valid afterward.
    var moved = api;
    api = undefined;
    defer moved.deinit();
    try std.testing.expectEqual(
        @as(?*report_registry.ResultHandle, null),
        try moved.takeCustomReportResult(pending),
    );
    _ = try moved.drainPending();

    const erased = (try moved.takeCustomReportResult(pending)) orelse
        return error.MissingAsyncResult;
    defer erased.deinit();
    const result = try Adapted.borrowResult(erased);
    try std.testing.expectEqual(@as(i64, 2_252), result.value);
    try std.testing.expectEqualSlices(
        i32,
        &.{ 0, 1, 1024, 1025 },
        result.section_ids,
    );
    try std.testing.expectError(
        error.RequestConsumedOrUnknown,
        moved.takeCustomReportResult(pending_alias),
    );
}

test "sharded custom report token cannot steal another API response" {
    const allocator = std.testing.allocator;
    const Adapted = report_registry.Adapter(ShardedApiCustomSpec);
    const registry = report_registry.Registry.fromSpecs(.{ShardedApiCustomSpec});
    const config = customReportConfig(registry, 1);
    var first = try ExchangeApiSharded.initWithPersistenceRoot(
        allocator,
        std.Io.Dir.cwd(),
        config,
    );
    defer first.deinit();
    var second = try ExchangeApiSharded.initWithPersistenceRoot(
        allocator,
        std.Io.Dir.cwd(),
        config,
    );
    defer second.deinit();
    const query_value: ShardedApiCustomQuery = .{ .bias = 3 };
    const query = try Adapted.cloneQuery(allocator, &query_value);
    defer query.deinit();

    const first_pending = try first.submitCustomReportAsync(
        .routed,
        72_302,
        9_900_000_002,
        query,
    );
    const second_pending = try second.submitCustomReportAsync(
        .routed,
        72_303,
        9_900_000_003,
        query,
    );
    _ = try first.drainPending();
    _ = try second.drainPending();
    try std.testing.expectEqual(@as(usize, 0), first.readyCount());
    try std.testing.expectEqual(@as(usize, 0), second.readyCount());
    try std.testing.expect(try first.isCustomReportResultReady(first_pending));
    try std.testing.expect(try second.isCustomReportResultReady(second_pending));

    const first_raw_id = try first.custom_report_requests.pendingRequestId(
        first_pending,
        @ptrCast(first.driver),
    );
    try std.testing.expectEqual(
        @as(?kernel_mod.CommandResponse, null),
        first.takeResult(first_raw_id),
    );
    try std.testing.expect(try first.isCustomReportResultReady(first_pending));

    try std.testing.expectError(
        error.WrongRequestStore,
        second.takeCustomReportResult(first_pending),
    );
    try std.testing.expect(try first.isCustomReportResultReady(first_pending));
    try std.testing.expect(try second.isCustomReportResultReady(second_pending));

    const first_result = (try first.takeCustomReportResult(first_pending)) orelse
        return error.MissingAsyncResult;
    defer first_result.deinit();
    const second_result = (try second.takeCustomReportResult(second_pending)) orelse
        return error.MissingAsyncResult;
    defer second_result.deinit();
    try std.testing.expectEqual(@as(usize, 0), first.readyCount());
    try std.testing.expectEqual(@as(usize, 0), second.readyCount());
}

test "sharded custom report command failure is terminal and sync helper recovers" {
    const allocator = std.testing.allocator;
    const Adapted = report_registry.Adapter(ShardedApiCustomSpec);
    const registry = report_registry.Registry.fromSpecs(.{ShardedApiCustomSpec});
    var api = try ExchangeApiSharded.initWithPersistenceRoot(
        allocator,
        std.Io.Dir.cwd(),
        customReportConfig(registry, 1),
    );
    defer api.deinit();

    const failing_value: ShardedApiCustomQuery = .{
        .bias = std.math.minInt(i64),
    };
    const failing_query = try Adapted.cloneQuery(allocator, &failing_value);
    defer failing_query.deinit();
    const pending = try api.submitCustomReportAsync(
        .routed,
        72_304,
        9_900_000_004,
        failing_query,
    );
    const pending_alias = pending;
    _ = try api.drainPending();
    try std.testing.expectError(
        error.CustomReportCommandFailed,
        api.takeCustomReportResult(pending),
    );
    try std.testing.expectError(
        error.RequestConsumedOrUnknown,
        api.takeCustomReportResult(pending_alias),
    );

    const query_value: ShardedApiCustomQuery = .{ .bias = 5 };
    const query = try Adapted.cloneQuery(allocator, &query_value);
    defer query.deinit();
    const erased = try api.processCustomReport(
        .routed,
        72_305,
        9_900_000_005,
        query,
    );
    defer erased.deinit();
    const result = try Adapted.borrowResult(erased);
    try std.testing.expectEqual(@as(i64, 1_115), result.value);
    try std.testing.expectEqualSlices(i32, &.{ 0, 1024 }, result.section_ids);
}

test "sharded copied custom report tokens reduce exactly once concurrently" {
    const allocator = std.testing.allocator;
    const Adapted = report_registry.Adapter(ShardedApiCustomSpec);
    const registry = report_registry.Registry.fromSpecs(.{ShardedApiCustomSpec});
    var api = try ExchangeApiSharded.initWithPersistenceRoot(
        allocator,
        std.Io.Dir.cwd(),
        customReportConfig(registry, 1),
    );
    defer api.deinit();

    ShardedApiCustomSpec.reduce_calls.store(0, .monotonic);
    const query_value: ShardedApiCustomQuery = .{ .bias = 7 };
    const query = try Adapted.cloneQuery(allocator, &query_value);
    defer query.deinit();
    const pending = try api.submitCustomReportAsync(
        .routed,
        72_306,
        9_900_000_006,
        query,
    );
    _ = try api.drainPending();
    try std.testing.expect(try api.isCustomReportResultReady(pending));

    var gate: PersistPairTakeGate = .{ .io = std.testing.io };
    var first: CustomReportTakeRunner = .{
        .api = &api,
        .pending = pending,
        .gate = &gate,
    };
    var second = first;
    const first_thread = try std.Thread.spawn(.{}, CustomReportTakeRunner.run, .{&first});
    const second_thread = try std.Thread.spawn(.{}, CustomReportTakeRunner.run, .{&second});
    gate.releaseWhenReady(2);
    first_thread.join();
    second_thread.join();

    const result_count = @as(usize, @intFromBool(first.outcome.? == .result)) +
        @as(usize, @intFromBool(second.outcome.? == .result));
    try std.testing.expectEqual(@as(usize, 1), result_count);
    for ([_]CustomReportTakeOutcome{ first.outcome.?, second.outcome.? }) |outcome| {
        try std.testing.expect(outcome == .result or
            outcome == .not_ready or
            outcome == .consumed);
    }
    try std.testing.expectEqual(
        @as(usize, 1),
        ShardedApiCustomSpec.reduce_calls.load(.monotonic),
    );
    try std.testing.expectEqual(@as(usize, 0), api.custom_ready_results.count());
    try std.testing.expectEqual(@as(usize, 0), api.driver.completion_slots_used);
}

test "sharded custom report reducer runs outside the driver mutex" {
    const allocator = std.testing.allocator;
    const Adapted = report_registry.Adapter(ShardedApiCustomSpec);
    const registry = report_registry.Registry.fromSpecs(.{ShardedApiCustomSpec});
    var api = try ExchangeApiSharded.initWithPersistenceRoot(
        allocator,
        std.Io.Dir.cwd(),
        customReportConfig(registry, 1),
    );
    defer api.deinit();

    ShardedApiCustomSpec.reduce_calls.store(0, .monotonic);
    const query_value: ShardedApiCustomQuery = .{
        .bias = 9,
        .reentry_api = &api,
    };
    const query = try Adapted.cloneQuery(allocator, &query_value);
    defer query.deinit();
    const result = try api.processCustomReport(
        .routed,
        72_307,
        9_900_000_007,
        query,
    );
    defer result.deinit();
    try std.testing.expectEqual(
        @as(usize, 1),
        ShardedApiCustomSpec.reduce_calls.load(.monotonic),
    );
}

test "sharded custom result handle and taken value release through live facade" {
    const backing_allocator = std.testing.allocator;
    const Adapted = report_registry.Adapter(ShardedApiCustomSpec);
    const registry = report_registry.Registry.fromSpecs(.{ShardedApiCustomSpec});
    var api = try ExchangeApiSharded.initWithPersistenceRoot(
        backing_allocator,
        std.Io.Dir.cwd(),
        customReportConfig(registry, 1),
    );
    defer api.deinit();
    try api.startup();

    const query_value: ShardedApiCustomQuery = .{ .bias = 17 };
    const query = try Adapted.cloneQuery(backing_allocator, &query_value);
    defer query.deinit();
    const erased = try api.processCustomReport(
        .routed,
        72_308,
        9_900_000_008,
        query,
    );
    var taken = try Adapted.takeResult(erased);
    try std.testing.expect(taken.section_ids.len > 0);

    const background = try api.addUserAsync(.routed, 72_308);
    api.releaseCustomReportResult(erased);
    Adapted.deinitTakenResult(api.liveCustomResultAllocator(), &taken);

    _ = try api.drainPending();
    var response = api.takeResult(background) orelse return error.MissingAsyncResult;
    api.releaseResponse(&response);
}

test "sharded synchronous custom report rejects a cold callback before admission" {
    const allocator = std.testing.allocator;
    const Adapted = report_registry.Adapter(ShardedApiCustomSpec);
    const registry = report_registry.Registry.fromSpecs(.{ShardedApiCustomSpec});
    var api = try ExchangeApiSharded.initWithPersistenceRoot(
        allocator,
        std.Io.Dir.cwd(),
        customReportConfig(registry, 1),
    );
    defer api.deinit();

    const query_value: ShardedApiCustomQuery = .{ .bias = 11 };
    const query = try Adapted.cloneQuery(allocator, &query_value);
    defer query.deinit();
    var probe: CustomReportReentrantProbe = .{
        .api = &api,
        .query = query,
    };
    api.setResultSink(.{
        .context = &probe,
        .on_result = CustomReportReentrantProbe.onResult,
    });
    try api.enqueueRuntime(makeNopCommand());
    _ = try api.drainRuntime();

    try std.testing.expect(!probe.unexpectedly_succeeded);
    try std.testing.expectEqual(
        @as(anyerror, error.CoreReentrantCall),
        probe.failure orelse return error.MissingCallbackFailure,
    );
    try std.testing.expectEqual(@as(u64, 0), api.driver.async_accepted_total);
    try std.testing.expectEqual(@as(usize, 0), api.custom_ready_results.count());
    try std.testing.expectEqual(@as(usize, 0), api.driver.completion_slots_used);
}

test "sharded synchronous custom report quiesces fatal settlement before cleanup" {
    const allocator = std.testing.allocator;
    const Adapted = report_registry.Adapter(ShardedApiCustomSpec);
    const registry = report_registry.Registry.fromSpecs(.{ShardedApiCustomSpec});
    var config = customReportConfig(registry, 1);
    config.runtime.queue_capacity = 64;
    config.runtime.max_batch_size = 16;
    config.runtime.wait_strategy = .blocking;
    var api = try ExchangeApiSharded.initWithPersistenceRoot(
        allocator,
        std.Io.Dir.cwd(),
        config,
    );
    defer api.deinit();

    var gate: ShardedBackpressureGate = .{ .io = std.testing.io };
    defer gate.release();
    api.setResultSink(.{
        .context = &gate,
        .on_result = ShardedBackpressureGate.onResult,
    });
    try api.startup();
    try api.enqueueRuntime(makeNopCommand());
    gate.waitEntered();

    const query_value: ShardedApiCustomQuery = .{ .bias = 13 };
    const query = try Adapted.cloneQuery(allocator, &query_value);
    defer query.deinit();
    var runner: CustomReportProcessRunner = .{
        .api = &api,
        .query = query,
    };
    const process_thread = try std.Thread.spawn(
        .{},
        CustomReportProcessRunner.run,
        .{&runner},
    );

    api.driver.mutex.lockUncancelable(api.driver.io);
    while (api.driver.async_accepted_total == 0) {
        api.driver.work_available.waitUncancelable(
            api.driver.io,
            &api.driver.mutex,
        );
    }
    api.driver.mutex.unlock(api.driver.io);

    api.execution_mutex.lockUncancelable(api.driver.io);
    api.core.kernels.next_seq = 100;
    api.execution_mutex.unlock(api.driver.io);
    gate.release();
    process_thread.join();

    try std.testing.expect(!runner.unexpectedly_succeeded);
    try std.testing.expectEqual(
        @as(anyerror, error.CorePoisoned),
        runner.failure orelse return error.MissingProcessFailure,
    );
    try std.testing.expect(api.fatalInfo() != null);
    api.driver.mutex.lockUncancelable(api.driver.io);
    const counters = .{
        api.driver.accepted_total,
        api.driver.completed_total,
        api.driver.async_accepted_total,
        api.driver.async_completed_total,
        api.driver.runtime_accepted_total,
        api.driver.runtime_completed_total,
        api.driver.completion_slots_used,
        api.driver.queue.count(),
    };
    api.driver.mutex.unlock(api.driver.io);
    try std.testing.expectEqual(counters[0], counters[1]);
    try std.testing.expectEqual(counters[2], counters[3]);
    try std.testing.expectEqual(counters[4], counters[5]);
    try std.testing.expectEqual(@as(usize, 0), counters[6]);
    try std.testing.expectEqual(@as(usize, 0), counters[7]);
    try std.testing.expectEqual(@as(usize, 0), api.custom_ready_results.count());
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

test "sharded exchange api simple init applies its Java-default risk config to every engine" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    try std.testing.expectEqual(
        config_mod.MarginTradingMode.margin_trading_enabled,
        api.configRef().orders_processing.margin_trading_mode,
    );
    for (0..api.riskShardCount()) |idx| {
        try std.testing.expect(api.core.kernels.riskEngineAt(idx).cfg_margin_trading_enabled);
    }
}

test "sharded exchange api exposes independent throughput engine counts" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, .{
        .performance_cfg = config_mod.PerformanceConfig.throughputPreset(),
    });
    defer api.deinit();
    try std.testing.expectEqual(@as(usize, 4), api.shardCount());
    try std.testing.expectEqual(@as(usize, 4), api.matchingShardCount());
    try std.testing.expectEqual(@as(usize, 2), api.riskShardCount());
    try std.testing.expectEqual(@as(u64, 3), api.matchingShardMask());
    try std.testing.expectEqual(@as(u64, 1), api.riskShardMask());
}

test "sharded exchange api detached submit and Java barriers preserve timing and counters" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const detached_uid: i64 = 63_901;
    const detached_shard = api.core.shardIndexForUid(detached_uid);
    const sequence_before = api.driver.publication_sequencer.highWater();
    try api.submitCommand(.routed, .{ .add_user = detached_uid });
    try std.testing.expectEqual(
        sequence_before + 1,
        api.driver.publication_sequencer.highWater(),
    );
    try std.testing.expect(
        api.core.kernels.kernels[detached_shard].risk_engine.getUserProfile(detached_uid) == null,
    );
    try std.testing.expectEqual(@as(usize, 0), api.pendingCount());
    try std.testing.expectEqual(@as(usize, 0), api.runtimePendingCount());
    try std.testing.expectEqual(@as(usize, 0), api.readyCount());
    try std.testing.expectEqual(@as(usize, 0), api.driver.completion_slots_used);

    const empty: [0]ApiCommand = .{};
    try api.submitCommandsSync(.routed, empty[0..]);
    try std.testing.expectEqual(
        sequence_before + 1,
        api.driver.publication_sequencer.highWater(),
    );
    try std.testing.expect(
        api.core.kernels.kernels[detached_shard].risk_engine.getUserProfile(detached_uid) == null,
    );

    const list_uid: i64 = 63_902;
    try api.submitCommandsSync(.routed, &.{
        .{ .add_user = list_uid },
        .{ .adjust_user_balance = .{
            .uid = list_uid,
            .transaction_id = 1,
            .currency = 15,
            .amount = 32_000,
            .adjustment_type = .adjustment,
        } },
    });
    try std.testing.expectEqual(@as(?i64, 32_000), try api.queryUserBalanceReport(list_uid, 15));
    try std.testing.expectEqual(@as(usize, 0), api.pendingCount());
    try std.testing.expectEqual(@as(usize, 0), api.readyCount());
    try std.testing.expectEqual(@as(usize, 0), api.driver.completion_slots_used);

    const stream_uid: i64 = 63_903;
    var stream_barrier = try api.submitCommandsSyncWithNop(.split, &.{
        .{ .add_user = stream_uid },
        .{ .adjust_user_balance = .{
            .uid = stream_uid,
            .transaction_id = 1,
            .currency = 11,
            .amount = 33_000,
            .adjustment_type = .adjustment,
        } },
    });
    stream_barrier.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 33_000), try api.queryUserBalanceReport(stream_uid, 11));

    const before_empty_stream = api.driver.publication_sequencer.highWater();
    var empty_stream_barrier = try api.submitCommandsSyncWithNop(.routed, empty[0..]);
    defer empty_stream_barrier.deinit(allocator);
    try std.testing.expectEqual(
        before_empty_stream + 1,
        api.driver.publication_sequencer.highWater(),
    );
    try std.testing.expectEqual(@as(usize, 0), api.driver.completion_slots_used);
}

test "sharded exchange api full-response submission enforces the exact Java allowlist" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const unsupported = [_]ApiCommand{
        .{ .raw_command = .{ .command = .nop } },
        .{ .grouping_control = .{ .timestamp_ns = 1, .mode = 0 } },
        .{ .persist_state = .{ .snapshot_id = 1, .timestamp_ns = 2 } },
        .{ .binary_add_symbols = .{ .transfer_id = 1, .timestamp = 2, .symbols = &.{} } },
        .{ .binary_add_accounts = .{ .transfer_id = 1, .timestamp = 2, .users = &.{} } },
        .{ .binary_report_query = .{ .transfer_id = 1, .timestamp = 2, .query = .state_hash } },
    };
    for (unsupported) |command| {
        try std.testing.expectError(
            error.UnsupportedAsyncFullResponseCommand,
            api.submitCommandAsyncFullResponse(.routed, command),
        );
    }
    try std.testing.expectEqual(@as(usize, 0), api.driver.completion_slots_used);
    try std.testing.expectEqual(@as(u64, 0), api.driver.publication_sequencer.highWater());
}

test "sharded exchange api detached persist and typed binary batches allocate no result slots" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    installTestJavaIdentityHashContext(&api);

    const symbol = try common.CoreSymbolSpecification.init(.{
        .symbol_id = 24_290,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
    });
    try api.submitCommand(.routed, .{ .binary_add_symbols = .{
        .transfer_id = 5_290,
        .timestamp = 9_929_000_001,
        .symbols = &.{symbol},
    } });
    const accounts = [_]BinaryCurrencyBalance{.{ .currency = 15, .balance = 34_000 }};
    const users = [_]BinaryUserAccountsBatch{.{
        .uid = 63_904,
        .accounts = accounts[0..],
    }};
    try api.submitCommand(.split, .{ .binary_add_accounts = .{
        .transfer_id = 5_291,
        .timestamp = 9_929_000_002,
        .users = users[0..],
    } });
    try api.submitCommand(.routed, .{ .binary_report_query = .{
        .transfer_id = 5_292,
        .timestamp = 9_929_000_003,
        .query = .state_hash,
    } });
    const before_persist = api.driver.publication_sequencer.highWater();
    try api.submitCommand(.routed, .{ .persist_state = .{
        .snapshot_id = 63_905,
        .timestamp_ns = 9_929_000_004,
    } });
    try std.testing.expectEqual(
        before_persist + 2,
        api.driver.publication_sequencer.highWater(),
    );
    try std.testing.expectEqual(@as(usize, 0), api.pendingCount());
    try std.testing.expectEqual(@as(usize, 0), api.runtimePendingCount());
    try std.testing.expectEqual(@as(usize, 0), api.readyCount());
    try std.testing.expectEqual(@as(usize, 0), api.driver.completion_slots_used);

    var barrier = try api.submitCommandsSyncWithNop(.routed, &.{});
    defer barrier.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 34_000), try api.queryUserBalanceReport(63_904, 15));
    var order_book = try api.queryOrderBookReport(symbol.symbol_id, 4);
    defer order_book.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), api.driver.completion_slots_used);
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

    var setup_barrier = try api.submitCommandsSyncWithNop(.routed, &.{});
    setup_barrier.deinit(allocator);

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

    var place_barrier = try api.submitCommandsSyncWithNop(.routed, &.{});
    place_barrier.deinit(allocator);

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

    var cancel_barrier = try api.submitCommandsSyncWithNop(.routed, &.{});
    cancel_barrier.deinit(allocator);

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

test "sharded exchange api binary report requests expose exact Java variants" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    installTestJavaIdentityHashContext(&api);

    _ = try runBasicFlow(&api, .routed);

    const known_uid: i64 = 11_001;
    const transfer_for_uid: i32 = @intCast(known_uid);

    const state_handle = try api.requestJavaStateHashAsync(.routed, 77_001, 9_960_001_001);
    _ = try api.drainPending();
    var state_response = api.takeResult(state_handle) orelse return error.MissingAsyncResult;
    defer state_response.deinit(allocator);
    try expectJavaStateHashResponse(&state_response);

    const user_handle = try api.requestJavaSingleUserReportAsync(
        .routed,
        transfer_for_uid,
        9_960_001_002,
        known_uid,
    );
    _ = try api.drainPending();
    var user_response = api.takeResult(user_handle) orelse return error.MissingAsyncResult;
    defer user_response.deinit(allocator);
    try std.testing.expect(user_response.report_response == null);
    switch (user_response.java_report_response orelse return error.MissingReportResponse) {
        .single_user => |user_report| {
            try std.testing.expectEqual(@as(i64, 0), user_report.uid);
            try std.testing.expectEqual(
                java_report_wire.QueryExecutionStatus.ok,
                user_report.query_execution_status,
            );
            try std.testing.expect(javaMapValue(user_report.accounts, 15) != null);
        },
        else => return error.UnexpectedReportResponseType,
    }

    const total_handle = try api.requestJavaTotalCurrencyBalanceAsync(
        .routed,
        77_002,
        9_960_001_003,
    );
    _ = try api.drainPending();
    var total_response = api.takeResult(total_handle) orelse return error.MissingAsyncResult;
    defer total_response.deinit(allocator);
    try std.testing.expect(total_response.report_response == null);
    switch (total_response.java_report_response orelse return error.MissingReportResponse) {
        .total_currency_balance => |total_report| {
            try std.testing.expect(total_report.account_balances != null);
        },
        else => return error.UnexpectedReportResponseType,
    }

    const order_book_handle = try api.orderBookRequestAsync(.routed, 241, 8);
    _ = try api.drainPending();
    var order_book_snapshot = (try api.takeOrderBookSnapshot(order_book_handle)) orelse return error.MissingAsyncResult;
    defer order_book_snapshot.deinit(allocator);
    try std.testing.expect(order_book_snapshot.asks.items.len <= 8);
    try std.testing.expect(order_book_snapshot.bids.items.len <= 8);

    var one_shot_state = try api.submitBinaryReportQuery(
        .routed,
        77_003,
        9_960_001_004,
        .state_hash,
    );
    defer one_shot_state.deinit(allocator);
    try expectJavaStateHashResponse(&one_shot_state);

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

fn expectShardedExactAndNativeReportOverflowIsolation(mode: ExecutionMode) !void {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    for ([_]i64{ 622_001, 622_002 }) |uid| {
        const risk_engine = api.core.kernels.riskEngineForUid(uid);
        try std.testing.expect(try risk_engine.user_profile_service.addEmptyUserProfile(uid));
        const profile = risk_engine.getUserProfile(uid) orelse unreachable;
        try profile.putAccount(allocator, 15, std.math.maxInt(i64));
    }

    var exact = try api.submitBinaryReportQuery(
        mode,
        62_201,
        9_622_000_001,
        .total_currency_balance,
    );
    defer exact.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, exact.result_code);
    try std.testing.expectEqual(
        cmd.BinaryReportProjection.exact_java_only,
        exact.command.binary_report_projection,
    );
    try std.testing.expect(exact.report_response == null);
    switch (exact.java_report_response orelse return error.MissingReportResponse) {
        .total_currency_balance => |report| {
            try std.testing.expectEqual(@as(?i64, -2), javaMapValue(report.account_balances, 15));
        },
        else => return error.UnexpectedReportResponseType,
    }

    var native = try api.submitNativeReportQuery(
        mode,
        62_202,
        9_622_000_002,
        .total_currency_balance,
    );
    defer native.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.binary_command_failed, native.result_code);
    try std.testing.expectEqual(
        cmd.BinaryReportProjection.exact_java_and_native,
        native.command.binary_report_projection,
    );
    try std.testing.expect(native.report_response == null);
    try std.testing.expect(native.java_report_response == null);
    try std.testing.expect(native.matcher_events_head == null);
}

test "sharded exact Java report is isolated from native overflow in routed and split modes" {
    try expectShardedExactAndNativeReportOverflowIsolation(.routed);
    try expectShardedExactAndNativeReportOverflowIsolation(.split);
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
    try std.testing.expectEqual(@as(usize, 0), try api.processPendingCallbacks());

    try std.testing.expectEqual(@as(usize, 0), api.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, 3), collector.count);
    try std.testing.expectEqual(@as(?cmd.CommandResultCode, .success), collector.last_result);
    try std.testing.expectEqual(@as(?cmd.OrderCommandType, .resume_user), collector.last_command);
    try std.testing.expectEqual(@as(i64, 71_001), collector.last_uid);
    try std.testing.expectEqual(@as(i64, -1), collector.last_order_id);
    try std.testing.expectEqual(@as(i32, -1), collector.last_symbol);
    try std.testing.expect(collector.last_timestamp > 0);
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
    try std.testing.expectEqual(@as(usize, 0), try api.processPendingCallbacks());
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

test "sharded drain rejects a globally active foreign callback scope" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const request_id = try api.addUserAsync(.routed, 71_211);
    {
        var callback_scope = runtime_mod.CallbackExecutionScope.enter();
        defer callback_scope.leave();
        try std.testing.expectError(error.CoreReentrantCall, api.drainPending());
    }

    try std.testing.expectEqual(@as(usize, 1), try api.drainPending());
    var response = api.takeResult(request_id) orelse
        return error.MissingAsyncResult;
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
}

test "sharded callback rejects foreign blocking roots and preserves immediate async admission" {
    const allocator = std.testing.allocator;
    var source = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer source.deinit();

    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 4;
    java_config.performance_cfg.msgs_in_group_limit = 1;
    java_config.performance_cfg.orderbook_kind = .direct;
    var target = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer target.deinit();

    var probe: CrossShardedBlockingProbe = .{ .target = &target };
    try source.createUserWithCallback(.routed, 711_000, .{
        .context = &probe,
        .callback = CrossShardedBlockingProbe.onResponse,
    });
    try std.testing.expectEqual(@as(usize, 1), try source.drainPending());

    try std.testing.expectEqual(@as(usize, 1), probe.callback_count);
    try std.testing.expect(!probe.sync_succeeded);
    try std.testing.expect(!probe.query_succeeded);
    try std.testing.expect(!probe.process_succeeded);
    try std.testing.expect(!probe.batch_succeeded);
    try std.testing.expect(!probe.composite_succeeded);
    try std.testing.expect(!probe.runtime_drain_succeeded);
    try std.testing.expect(!probe.callback_dispatch_succeeded);
    try std.testing.expect(!probe.bootstrap_succeeded);
    try std.testing.expect(!probe.startup_succeeded);
    inline for (.{
        probe.sync_error,
        probe.query_error,
        probe.process_error,
        probe.batch_error,
        probe.composite_error,
        probe.runtime_drain_error,
        probe.callback_dispatch_error,
        probe.bootstrap_error,
        probe.startup_error,
    }) |actual| {
        try std.testing.expectEqual(
            @as(?anyerror, error.CoreReentrantCall),
            actual,
        );
    }

    try std.testing.expectEqual(@as(u64, 0), probe.accepted_before_async);
    try std.testing.expectEqual(@as(u64, 0), probe.next_request_before_async);
    try std.testing.expectEqual(@as(usize, 0), probe.queued_before_async);
    try std.testing.expectEqual(@as(?anyerror, null), probe.immediate_error);
    try std.testing.expectEqual([4]u64{ 1, 2, 3, 4 }, probe.immediate_ids);
    try std.testing.expect(!probe.saturated_succeeded);
    try std.testing.expectEqual(
        @as(?anyerror, error.CoreReentrantCall),
        probe.saturated_error,
    );
    try std.testing.expectEqual(@as(u64, 4), probe.accepted_before_saturated);
    try std.testing.expectEqual(
        probe.accepted_before_saturated,
        probe.accepted_after_saturated,
    );
    try std.testing.expectEqual(@as(u64, 4), probe.next_request_before_saturated);
    try std.testing.expectEqual(
        probe.next_request_before_saturated,
        probe.next_request_after_saturated,
    );
    try std.testing.expectEqual(@as(usize, 4), probe.queued_before_saturated);
    try std.testing.expectEqual(
        probe.queued_before_saturated,
        probe.queued_after_saturated,
    );
    try std.testing.expectEqual(@as(usize, 4), probe.completion_before_saturated);
    try std.testing.expectEqual(
        probe.completion_before_saturated,
        probe.completion_after_saturated,
    );

    try target.startup();
    try std.testing.expectEqual(@as(usize, 4), try target.drainPending());
    for (probe.immediate_ids) |request_id| {
        var response = target.takeResult(request_id) orelse
            return error.MissingAsyncResult;
        response.deinit(target.allocator);
    }
    var sync_after = try target.addUser(.routed, 711_001);
    defer sync_after.deinit(target.allocator);
    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        sync_after.result_code,
    );
}

test "sharded callback reports reentrant before saturated running admission mutates" {
    const allocator = std.testing.allocator;
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 4;
    java_config.performance_cfg.msgs_in_group_limit = 1;
    java_config.performance_cfg.orderbook_kind = .direct;
    var target = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer target.deinit();
    try target.startup();

    var blocker: BlockingCommandCallbackProbe = .{ .io = target.driver.io };
    var source = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer source.deinit();
    defer {
        if (!blocker.release.isSet()) blocker.release.set(blocker.io);
        target.shutdown();
    }
    try source.startup();
    try target.createUserWithCallback(.routed, 711_300, .{
        .context = &blocker,
        .callback = BlockingCommandCallbackProbe.onResponse,
    });
    try waitForTestEvent(&blocker.entered, blocker.io);

    var immediate: CrossShardedAsyncProbe = .{
        .target = &target,
        .io = source.driver.io,
        .uid = 711_301,
    };
    try source.createUserWithCallback(.routed, 711_302, .{
        .context = &immediate,
        .callback = CrossShardedAsyncProbe.onResponse,
    });
    try waitForTestEvent(&immediate.finished, immediate.io);
    try std.testing.expectEqual(@as(?anyerror, null), immediate.failure);
    try std.testing.expectEqual(@as(?u64, 2), immediate.request_id);

    _ = try target.addUserAsync(.routed, 711_303);
    _ = try target.addUserAsync(.routed, 711_304);
    _ = try target.addUserAsync(.routed, 711_305);

    const driver = target.driver;
    driver.mutex.lockUncancelable(driver.io);
    const accepted_before = driver.accepted_total;
    const request_before = target.next_request_id;
    const queued_before = driver.queue.count();
    const completion_before = driver.completion_slots_used;
    driver.mutex.unlock(driver.io);

    var saturated: CrossShardedAsyncProbe = .{
        .target = &target,
        .io = source.driver.io,
        .uid = 711_306,
    };
    try source.createUserWithCallback(.routed, 711_307, .{
        .context = &saturated,
        .callback = CrossShardedAsyncProbe.onResponse,
    });
    try waitForTestEvent(&saturated.finished, saturated.io);
    try std.testing.expectEqual(
        @as(?anyerror, error.CoreReentrantCall),
        saturated.failure,
    );
    try std.testing.expectEqual(@as(?u64, null), saturated.request_id);

    driver.mutex.lockUncancelable(driver.io);
    defer driver.mutex.unlock(driver.io);
    try std.testing.expectEqual(accepted_before, driver.accepted_total);
    try std.testing.expectEqual(request_before, target.next_request_id);
    try std.testing.expectEqual(queued_before, driver.queue.count());
    try std.testing.expectEqual(completion_before, driver.completion_slots_used);
}

test "sharded callback rejects foreign publisher driver lock contention" {
    const allocator = std.testing.allocator;
    var target = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer target.deinit();
    var source = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer source.deinit();
    try source.startup();

    var gate: ShardedDriverLockGate = .{
        .api = &target,
        .io = target.driver.io,
    };
    const holder = try std.Thread.spawn(.{}, ShardedDriverLockGate.run, .{&gate});
    var holder_live = true;
    defer {
        if (!gate.release.isSet()) gate.release.set(gate.io);
        if (holder_live) holder.join();
    }
    try waitForTestEvent(&gate.entered, gate.io);

    var probe: CrossShardedAsyncProbe = .{
        .target = &target,
        .io = source.driver.io,
        .uid = 711_400,
    };
    try source.createUserWithCallback(.routed, 711_401, .{
        .context = &probe,
        .callback = CrossShardedAsyncProbe.onResponse,
    });
    try waitForTestEvent(&probe.finished, probe.io);
    try std.testing.expectEqual(
        @as(?anyerror, error.CoreReentrantCall),
        probe.failure,
    );
    try std.testing.expectEqual(@as(?u64, null), probe.request_id);

    gate.release.set(gate.io);
    holder.join();
    holder_live = false;
    try std.testing.expectEqual(@as(u64, 0), target.driver.accepted_total);
    try std.testing.expectEqual(@as(u64, 0), target.next_request_id);
    try std.testing.expectEqual(@as(usize, 0), target.driver.queue.count());
}

test "sharded foreign callback dispatch rejection preserves registered result ownership" {
    const allocator = std.testing.allocator;
    var target = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer target.deinit();
    const handle: ApiCommandAsyncHandle = .{
        .request_id = try target.addUserAsync(.routed, 711_500),
    };
    try std.testing.expectEqual(@as(usize, 1), try target.drainPending());
    var collector: CallbackCollector = .{};
    try target.registerResultCallback(handle, .{
        .context = &collector,
        .callback = CallbackCollector.onResponse,
    });

    const Probe = struct {
        target: *ExchangeApiSharded,
        handle: ApiCommandAsyncHandle,
        succeeded: bool = false,
        failure: ?anyerror = null,

        fn onResponse(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
            const self: *@This() = @ptrCast(@alignCast(context.?));
            self.succeeded = self.target.processRegisteredCallback(self.handle) catch |err| {
                self.failure = err;
                return;
            };
        }
    };
    var probe: Probe = .{
        .target = &target,
        .handle = handle,
    };
    var source = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer source.deinit();
    try source.createUserWithCallback(.routed, 711_501, .{
        .context = &probe,
        .callback = Probe.onResponse,
    });
    try std.testing.expectEqual(@as(usize, 1), try source.drainPending());

    try std.testing.expect(!probe.succeeded);
    try std.testing.expectEqual(
        @as(?anyerror, error.CoreReentrantCall),
        probe.failure,
    );
    try std.testing.expectEqual(@as(usize, 1), target.readyCount());
    try std.testing.expectEqual(@as(usize, 1), target.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, 0), collector.count);

    try std.testing.expect(try target.processRegisteredCallback(handle));
    try std.testing.expectEqual(@as(usize, 0), target.readyCount());
    try std.testing.expectEqual(@as(usize, 0), target.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, 1), collector.count);
}

test "mixed sharded callback through core rejects the sharded return call" {
    const allocator = std.testing.allocator;
    var source = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer source.deinit();
    try source.startup();
    var target = try exchange_core_mod.ExchangeCore.initWithIo(
        allocator,
        std.testing.io,
        .{},
    );
    defer target.deinit();

    var probe: ShardedToCoreCycleProbe = .{
        .source = &source,
        .target = &target,
        .io = source.driver.io,
    };
    try source.createUserWithCallback(.routed, 712_001, .{
        .context = &probe,
        .callback = ShardedToCoreCycleProbe.outer,
    });
    try waitForTestEvent(&probe.outer_enqueued, probe.io);
    try std.testing.expectEqual(@as(?anyerror, null), probe.outer_enqueue_error);
    try target.startup();
    try waitForTestEvent(&probe.inner_finished, probe.io);
    try waitForTestEvent(&probe.outer_finished, probe.io);

    try std.testing.expect(!probe.outer_wait_failed);
    try std.testing.expect(!probe.inner_sync_succeeded);
    try std.testing.expectEqual(
        @as(?anyerror, error.CoreReentrantCall),
        probe.inner_sync_error,
    );
    try std.testing.expectEqual(@as(u64, 1), source.driver.accepted_total);
    try std.testing.expectEqual(@as(u64, 1), target.driver.accepted_total);
}

test "mixed core callback through sharded rejects the core return call" {
    const allocator = std.testing.allocator;
    var source = try exchange_core_mod.ExchangeCore.initWithIo(
        allocator,
        std.testing.io,
        .{},
    );
    defer source.deinit();
    try source.startup();
    var target = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer target.deinit();

    var probe: CoreToShardedCycleProbe = .{
        .source = &source,
        .target = &target,
        .io = target.driver.io,
    };
    try source.submitApiCommandWithCallback(.{ .add_user = 713_001 }, .{
        .context = &probe,
        .callback = CoreToShardedCycleProbe.outer,
    });
    try waitForTestEvent(&probe.outer_enqueued, probe.io);
    try std.testing.expectEqual(@as(?anyerror, null), probe.outer_enqueue_error);
    try target.startup();
    try waitForTestEvent(&probe.inner_finished, probe.io);
    try waitForTestEvent(&probe.outer_finished, probe.io);

    try std.testing.expect(!probe.outer_wait_failed);
    try std.testing.expect(!probe.inner_sync_succeeded);
    try std.testing.expectEqual(
        @as(?anyerror, error.CoreReentrantCall),
        probe.inner_sync_error,
    );
    try std.testing.expectEqual(@as(u64, 1), source.driver.accepted_total);
    try std.testing.expectEqual(@as(u64, 1), target.driver.accepted_total);
}

test "sharded overlapping same-target drains claim completion once" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    try api.startup();

    var probe: BlockingCommandCallbackProbe = .{ .io = api.driver.io };
    defer if (!probe.release.isSet()) probe.release.set(probe.io);
    try api.createUserWithCallback(.routed, 71_221, .{
        .context = &probe,
        .callback = BlockingCommandCallbackProbe.onResponse,
    });
    try waitForTestEvent(&probe.entered, probe.io);

    var first: ShardedAcceptedDrainRunner = .{
        .api = &api,
        .io = api.driver.io,
    };
    const first_thread = try std.Thread.spawn(
        .{},
        ShardedAcceptedDrainRunner.run,
        .{&first},
    );
    defer first_thread.join();
    defer if (!probe.release.isSet()) probe.release.set(probe.io);
    var second: ShardedAcceptedDrainRunner = .{
        .api = &api,
        .io = api.driver.io,
    };
    const second_thread = try std.Thread.spawn(
        .{},
        ShardedAcceptedDrainRunner.run,
        .{&second},
    );
    defer second_thread.join();
    defer if (!probe.release.isSet()) probe.release.set(probe.io);

    try waitForAcceptedDrainWaiterCount(&api, 2);
    try std.testing.expect(!first.finished.isSet());
    try std.testing.expect(!second.finished.isSet());
    probe.release.set(probe.io);
    try waitForTestEvent(&first.finished, first.io);
    try waitForTestEvent(&second.finished, second.io);

    const first_result = first.result orelse return error.MissingDrainResult;
    const second_result = second.result orelse return error.MissingDrainResult;
    try std.testing.expectEqual(
        @as(usize, 1),
        (try first_result) + (try second_result),
    );
    try std.testing.expectEqual(@as(u64, 1), api.driver.async_drained_total);
    try std.testing.expectEqual(@as(usize, 1), probe.response_count);
    try std.testing.expect(!probe.release_wait_failed.load(.acquire));
}

test "sharded overlapping staggered drains keep the reported frontier monotonic" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    try api.startup();

    var probe: BlockingCommandCallbackProbe = .{ .io = api.driver.io };
    defer if (!probe.release.isSet()) probe.release.set(probe.io);
    try api.createUserWithCallback(.routed, 71_231, .{
        .context = &probe,
        .callback = BlockingCommandCallbackProbe.onResponse,
    });
    try waitForTestEvent(&probe.entered, probe.io);

    var first: ShardedAcceptedDrainRunner = .{
        .api = &api,
        .io = api.driver.io,
    };
    const first_thread = try std.Thread.spawn(
        .{},
        ShardedAcceptedDrainRunner.run,
        .{&first},
    );
    defer first_thread.join();
    defer if (!probe.release.isSet()) probe.release.set(probe.io);
    try waitForAcceptedDrainWaiterCount(&api, 1);

    const later_request_id = try api.addUserAsync(.split, 71_232);
    var second: ShardedAcceptedDrainRunner = .{
        .api = &api,
        .io = api.driver.io,
    };
    const second_thread = try std.Thread.spawn(
        .{},
        ShardedAcceptedDrainRunner.run,
        .{&second},
    );
    defer second_thread.join();
    defer if (!probe.release.isSet()) probe.release.set(probe.io);
    try waitForAcceptedDrainWaiterCount(&api, 2);

    probe.release.set(probe.io);
    try waitForTestEvent(&first.finished, first.io);
    try waitForTestEvent(&second.finished, second.io);
    const first_result = first.result orelse return error.MissingDrainResult;
    const second_result = second.result orelse return error.MissingDrainResult;
    try std.testing.expectEqual(
        @as(usize, 2),
        (try first_result) + (try second_result),
    );
    try std.testing.expectEqual(@as(u64, 2), api.driver.async_drained_total);
    try std.testing.expectEqual(@as(usize, 1), probe.response_count);
    try std.testing.expect(!probe.release_wait_failed.load(.acquire));

    var later_response = api.takeResult(later_request_id) orelse
        return error.MissingAsyncResult;
    defer later_response.deinit(allocator);
    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        later_response.result_code,
    );
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
        .persist_state => |code| try std.testing.expectEqual(
            cmd.CommandResultCode.state_persist_matching_engine_failed,
            code,
        ),
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
        .persist_state => |code| try std.testing.expectEqual(
            cmd.CommandResultCode.state_persist_matching_engine_failed,
            code,
        ),
        .response => return error.ExpectedPersistStateResult,
    }

    var full_response = try api.processCommandFullResponse(.routed, .{ .add_user = 62_002 });
    defer full_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, full_response.result_code);

    try std.testing.expectError(
        error.UnsupportedAsyncFullResponseCommand,
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
    try std.testing.expectEqual(cmd.CommandResultCode.state_persist_matching_engine_failed, typed_code);
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

test "sharded exchange api reset is global in routed and split modes" {
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
    try std.testing.expectEqual(cmd.CommandResultCode.success, routed_second_add.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, split_second_add.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, routed_nop.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, split_nop.result_code);
}

test "sharded exchange api async reset is global in routed and split modes" {
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
    try std.testing.expectEqual(cmd.CommandResultCode.success, routed_codes[2]);
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

test "sharded grouping stamps live sync async and detached commands while preserving raw metadata" {
    try runGroupingMetadataFlow(.routed);
    try runGroupingMetadataFlow(.split);
}

test "sharded grouping applies size and strict idle deadlines with one-shot L2 flags" {
    try runGroupingDeadlineFlow(.routed);
    try runGroupingDeadlineFlow(.split);
}

test "sharded grouping preserves persistence pairs and terminal binary boundaries" {
    try runGroupingCompoundBoundaryFlow(.routed);
    try runGroupingCompoundBoundaryFlow(.split);
}

test "sharded default timed L2 request refreshes matching and every risk last-price cache" {
    try runDefaultTimedL2RefreshFlow(.routed);
    try runDefaultTimedL2RefreshFlow(.split);
}

test "sharded exchange api persist-state wrappers surface dummy backend failures in routed and split modes" {
    const allocator = std.testing.allocator;

    var routed_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer routed_api.deinit();
    var split_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer split_api.deinit();

    try std.testing.expectEqual(
        cmd.CommandResultCode.state_persist_matching_engine_failed,
        try routed_api.persistState(.routed, 16_001, 9_800_100_001),
    );
    try std.testing.expectEqual(
        cmd.CommandResultCode.state_persist_matching_engine_failed,
        try split_api.persistState(.split, 16_002, 9_800_100_002),
    );
}

test "sharded exchange api async persist-state wrappers expose dummy failures and single-consumption semantics" {
    const allocator = std.testing.allocator;

    var routed_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer routed_api.deinit();
    var split_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer split_api.deinit();

    const routed = try runPersistStateAsyncResultCode(&routed_api, .routed, 16_011, 9_800_100_011);
    const split = try runPersistStateAsyncResultCode(&split_api, .split, 16_012, 9_800_100_012);

    try std.testing.expectEqual(cmd.CommandResultCode.state_persist_matching_engine_failed, routed);
    try std.testing.expectEqual(cmd.CommandResultCode.state_persist_matching_engine_failed, split);
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
    installTestJavaIdentityHashContext(&api);

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
    try expectJavaStateHashResponse(&query_response);
}

test "sharded exchange api routed typed and async binary wrappers apply deterministic state updates" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    installTestJavaIdentityHashContext(&api);

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
    try expectNativeAndJavaStateHashResponse(&hash_response);

    try std.testing.expectEqual(@as(?i64, 7_000), try api.queryUserBalanceReport(96_901, 11));
    try std.testing.expectEqual(@as(?i64, 4_000), try api.queryUserBalanceReport(96_901, 15));
}

test "sharded exchange api binary async aliases forward to wrapped command and query behavior" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    installTestJavaIdentityHashContext(&api);

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
    try expectJavaStateHashResponse(&report_a);

    var report_b = api.takeResult(query_publish) orelse return error.MissingAsyncResult;
    defer report_b.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, report_b.result_code);
    try expectJavaStateHashResponse(&report_b);
}

test "sharded exchange api binary wrappers reject invalid input and accept split mode" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    installTestJavaIdentityHashContext(&api);

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
    var malformed = try api.submitBinaryDataPayload(.split, 5_914, 9_900_100_014, one_byte[0..]);
    defer malformed.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.binary_command_failed, malformed.result_code);

    const malformed_handle = try api.submitBinaryDataPayloadAsync(
        .split,
        5_915,
        9_900_100_015,
        one_byte[0..],
    );
    _ = try api.drainPending();
    try std.testing.expectEqual(
        @as(?cmd.CommandResultCode, .binary_command_failed),
        api.takeResultCode(malformed_handle),
    );

    var query = try api.submitBinaryReportQuery(.split, 5_916, 9_900_100_016, .state_hash);
    defer query.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, query.result_code);
    try expectJavaStateHashResponse(&query);

    const query_handle = try api.submitBinaryReportQueryAsync(
        .split,
        5_917,
        9_900_100_017,
        .state_hash,
    );
    _ = try api.drainPending();
    var async_query = api.takeResult(query_handle) orelse return error.MissingAsyncResult;
    defer async_query.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, async_query.result_code);
    try expectJavaStateHashResponse(&async_query);
}

test "sharded exchange api ApiCommand sync dispatch keeps routed and split balance parity" {
    const allocator = std.testing.allocator;

    var routed_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer routed_api.deinit();
    var split_api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer split_api.deinit();

    const uid: i64 = 98_001;
    var routed_add = try routed_api.submitApiCommandSync(.routed, .{ .add_user = uid });
    routed_add.deinit(allocator);
    var split_add = try split_api.submitApiCommandSync(.split, .{ .add_user = uid });
    split_add.deinit(allocator);
    var routed_adjust = try routed_api.submitApiCommandSync(.routed, .{
        .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 71,
            .currency = 15,
            .amount = 55_000,
            .adjustment_type = .adjustment,
        },
    });
    routed_adjust.deinit(allocator);
    var split_adjust = try split_api.submitApiCommandSync(.split, .{
        .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 71,
            .currency = 15,
            .amount = 55_000,
            .adjustment_type = .adjustment,
        },
    });
    split_adjust.deinit(allocator);

    try std.testing.expectEqual(@as(?i64, 55_000), try routed_api.queryUserBalanceReport(uid, 15));
    try std.testing.expectEqual(@as(?i64, 55_000), try split_api.queryUserBalanceReport(uid, 15));
}

test "sharded typed ApiCommand timestamps survive sync detached async batch and callback publication" {
    const allocator = std.testing.allocator;
    const uid: i64 = 98_051;
    const currency: i32 = 15;
    const sync_timestamp: i64 = 9_901_001_001;
    const detached_timestamp: i64 = 9_901_001_002;
    const async_timestamp: i64 = 9_901_001_003;
    const batch_timestamp: i64 = 9_901_001_004;
    const callback_timestamp: i64 = 9_901_001_005;

    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    var setup = try api.submitApiCommandSync(.routed, .{ .add_user = uid });
    setup.deinit(allocator);

    var raw_probe: TypedTimestampRawProbe = .{};
    try api.setRawResultSinkChecked(.{
        .context = &raw_probe,
        .on_result = TypedTimestampRawProbe.onResult,
    });

    var sync_result = try api.submitApiCommandSync(.routed, .{
        .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 1,
            .currency = currency,
            .amount = 10,
            .adjustment_type = .adjustment,
            .timestamp = sync_timestamp,
        },
    });
    defer sync_result.deinit(allocator);
    switch (sync_result) {
        .response => |response| try std.testing.expectEqual(
            sync_timestamp,
            response.command.timestamp,
        ),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    try api.submitApiCommand(.split, .{
        .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 2,
            .currency = currency,
            .amount = 20,
            .adjustment_type = .adjustment,
            .timestamp = detached_timestamp,
        },
    });
    const async_handle = try api.submitApiCommandAsync(.routed, .{
        .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 3,
            .currency = currency,
            .amount = 30,
            .adjustment_type = .adjustment,
            .timestamp = async_timestamp,
        },
    });

    var batch_result = try api.submitApiCommandsSyncResult(.split, &.{
        .{ .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 4,
            .currency = currency,
            .amount = 40,
            .adjustment_type = .adjustment,
        } },
        .{ .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 5,
            .currency = currency,
            .amount = 50,
            .adjustment_type = .adjustment,
            .timestamp = batch_timestamp,
        } },
    });
    defer batch_result.deinit(allocator);
    switch (batch_result) {
        .response => |response| try std.testing.expectEqual(
            batch_timestamp,
            response.command.timestamp,
        ),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var async_result = api.takeApiCommandAsyncResult(async_handle) orelse
        return error.MissingAsyncResult;
    defer async_result.deinit(allocator);
    switch (async_result) {
        .response => |response| try std.testing.expectEqual(
            async_timestamp,
            response.command.timestamp,
        ),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var callback_probe: TypedTimestampCallbackProbe = .{};
    const callback: CommandResponseCallback = .{
        .context = &callback_probe,
        .callback = TypedTimestampCallbackProbe.onResponse,
    };
    try api.submitCommandAsyncWithCallback(.routed, .{
        .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 6,
            .currency = currency,
            .amount = 60,
            .adjustment_type = .adjustment,
            .timestamp = callback_timestamp,
        },
    }, callback);
    try api.submitCommandAsyncWithCallback(.split, .{
        .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 7,
            .currency = currency,
            .amount = 70,
            .adjustment_type = .adjustment,
        },
    }, callback);
    try std.testing.expectEqual(@as(usize, 2), try api.drainPending());

    try std.testing.expectEqual(@as(usize, 7), raw_probe.len);
    try std.testing.expectEqualSlices(
        u64,
        &.{ 2, 3, 4, 5, 6, 7, 8 },
        raw_probe.sequences[0..raw_probe.len],
    );
    try std.testing.expectEqualSlices(
        cmd.OrderCommandType,
        &.{
            .balance_adjustment,
            .balance_adjustment,
            .balance_adjustment,
            .balance_adjustment,
            .balance_adjustment,
            .balance_adjustment,
            .balance_adjustment,
        },
        raw_probe.command_types[0..raw_probe.len],
    );
    try std.testing.expectEqualSlices(
        i64,
        &.{
            sync_timestamp,
            detached_timestamp,
            async_timestamp,
            0,
            batch_timestamp,
            callback_timestamp,
            raw_probe.timestamps[6],
        },
        raw_probe.timestamps[0..raw_probe.len],
    );
    for (raw_probe.result_codes[0..raw_probe.len]) |result_code| {
        try std.testing.expectEqual(cmd.CommandResultCode.success, result_code);
    }

    try std.testing.expectEqual(@as(usize, 2), callback_probe.len);
    try std.testing.expectEqual(callback_timestamp, callback_probe.timestamps[0]);
    try std.testing.expect(callback_probe.timestamps[1] > 0);
    try std.testing.expectEqual(raw_probe.timestamps[6], callback_probe.timestamps[1]);
    try std.testing.expectEqual(
        @as(?i64, 280),
        try api.queryUserBalanceReport(uid, currency),
    );
}

test "sharded scalar and unit timestamp carriers preserve routing raw results and journal images" {
    const allocator = std.testing.allocator;
    const uid: i64 = 98_054;
    const timestamps = [_]i64{
        9_901_003_001,
        9_901_003_002,
        9_901_003_003,
        9_901_003_004,
        9_901_003_005,
        9_901_003_006,
    };

    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.msgs_in_group_limit = 2;
    java_config.performance_cfg.max_group_duration_ns = 10;
    java_config.serialization_cfg = config_mod.SerializationConfig.diskJournaling();
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();
    var grouping_clock = FakeGroupingClock{ .now_ns = 100 };
    api.grouping_clock = grouping_clock.source();

    var raw_probe: TypedTimestampRawProbe = .{};
    try api.setRawResultSinkChecked(.{
        .context = &raw_probe,
        .on_result = TypedTimestampRawProbe.onResult,
    });

    var add = try api.submitApiCommandSync(.routed, .{
        .add_user_with_timestamp = .{
            .uid = uid,
            .timestamp = timestamps[0],
        },
    });
    defer add.deinit(allocator);
    switch (add) {
        .response => |response| try std.testing.expectEqual(
            timestamps[0],
            response.command.timestamp,
        ),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    try api.submitApiCommand(.split, .{
        .suspend_user_with_timestamp = .{
            .uid = uid,
            .timestamp = timestamps[1],
        },
    });
    const resume_handle = try api.submitApiCommandAsync(.routed, .{
        .resume_user_with_timestamp = .{
            .uid = uid,
            .timestamp = timestamps[2],
        },
    });
    var batch = try api.submitApiCommandsSyncResult(.split, &.{
        .{ .reset_with_timestamp = .{ .timestamp = timestamps[3] } },
        .{ .nop_with_timestamp = .{ .timestamp = timestamps[4] } },
    });
    defer batch.deinit(allocator);
    switch (batch) {
        .response => |response| try std.testing.expectEqual(
            timestamps[4],
            response.command.timestamp,
        ),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var resume_result = api.takeApiCommandAsyncResult(resume_handle) orelse
        return error.MissingAsyncResult;
    defer resume_result.deinit(allocator);
    switch (resume_result) {
        .response => |response| try std.testing.expectEqual(
            timestamps[2],
            response.command.timestamp,
        ),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var callback_probe: TypedTimestampCallbackProbe = .{};
    const callback: CommandResponseCallback = .{
        .context = &callback_probe,
        .callback = TypedTimestampCallbackProbe.onResponse,
    };
    try api.submitCommandAsyncWithCallback(.routed, .{
        .add_user_with_timestamp = .{
            .uid = uid + 1,
            .timestamp = timestamps[5],
        },
    }, callback);
    try std.testing.expectEqual(@as(usize, 1), try api.drainPending());
    try std.testing.expectEqual(@as(usize, 1), callback_probe.len);
    try std.testing.expectEqual(timestamps[5], callback_probe.timestamps[0]);

    try std.testing.expect(api_command_mod.supportsAsyncFullResponse(.{
        .reset_with_timestamp = .{ .timestamp = timestamps[3] },
    }));
    try std.testing.expect(api_command_mod.supportsAsyncFullResponse(.{
        .nop_with_timestamp = .{ .timestamp = timestamps[4] },
    }));
    try std.testing.expectError(
        error.UnsupportedCallbackCommand,
        api.submitCommandAsyncWithCallback(.routed, .{
            .reset_with_timestamp = .{ .timestamp = timestamps[3] },
        }, callback),
    );
    try std.testing.expectError(
        error.UnsupportedCallbackCommand,
        api.submitCommandAsyncWithCallback(.routed, .{
            .nop_with_timestamp = .{ .timestamp = timestamps[4] },
        }, callback),
    );

    const expected_types = [_]cmd.OrderCommandType{
        .add_user,
        .suspend_user,
        .resume_user,
        .reset,
        .nop,
        .add_user,
    };
    const expected_modes = [_]ExecutionMode{
        .routed,
        .split,
        .routed,
        .split,
        .split,
        .routed,
    };
    // The two-command size limit advances ordinary groups, and the global
    // reset opens its own terminal barrier group before later work resumes.
    const expected_groups = [_]i64{ 0, 0, 1, 2, 2, 3 };
    try std.testing.expectEqual(expected_types.len, raw_probe.len);
    try std.testing.expectEqualSlices(
        cmd.OrderCommandType,
        &expected_types,
        raw_probe.command_types[0..raw_probe.len],
    );
    try std.testing.expectEqualSlices(
        i64,
        &timestamps,
        raw_probe.timestamps[0..raw_probe.len],
    );

    const journal = api.journal orelse return error.ExpectedJournal;
    try std.testing.expectEqual(expected_types.len, journal.entries.items.len);
    for (
        journal.entries.items,
        expected_types,
        timestamps,
        expected_modes,
        expected_groups,
    ) |entry, command_type, timestamp, mode, events_group| {
        try std.testing.expectEqual(command_type, entry.input.command);
        try std.testing.expectEqual(timestamp, entry.input.timestamp);
        try std.testing.expectEqual(mode, entry.mode);
        try std.testing.expectEqual(events_group, entry.input.events_group);
    }
}

test "sharded typed GTC placement timestamp survives move in native and exact Java reports" {
    const allocator = std.testing.allocator;
    const symbol: i32 = 19_351;
    const uid: i64 = 98_052;
    const order_id: i64 = 98_053;
    const placement_timestamp: i64 = 9_901_002_001;
    const move_timestamp: i64 = 9_901_002_002;
    const cancel_timestamp: i64 = 9_901_002_003;

    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    installTestJavaIdentityHashContext(&api);

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

    var add = try api.submitApiCommandSync(.routed, .{ .add_user = uid });
    add.deinit(allocator);
    var balance = try api.submitApiCommandSync(.routed, .{
        .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 1,
            .currency = 11,
            .amount = 1_000,
            .adjustment_type = .adjustment,
        },
    });
    balance.deinit(allocator);

    var placement = try api.submitApiCommandSync(.split, .{
        .place_order = .{
            .order_id = order_id,
            .uid = uid,
            .symbol = symbol,
            .price = 100,
            .reserve_bid_price = 100,
            .size = 5,
            .action = .ask,
            .order_type = .gtc,
            .user_cookie = 7,
            .timestamp = placement_timestamp,
        },
    });
    defer placement.deinit(allocator);
    switch (placement) {
        .response => |response| {
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
            try std.testing.expectEqual(placement_timestamp, response.command.timestamp);
        },
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var before_move = try api.querySingleUserReport(uid);
    defer before_move.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), before_move.orders.items.len);
    try std.testing.expectEqual(placement_timestamp, before_move.orders.items[0].timestamp);

    const move_handle = try api.submitApiCommandAsync(.routed, .{
        .move_order = .{
            .order_id = order_id,
            .uid = uid,
            .symbol = symbol,
            .new_price = 101,
            .timestamp = move_timestamp,
        },
    });
    try std.testing.expectEqual(@as(usize, 1), try api.drainPending());
    var move_result = api.takeApiCommandAsyncResult(move_handle) orelse
        return error.MissingAsyncResult;
    defer move_result.deinit(allocator);
    switch (move_result) {
        .response => |response| {
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
            try std.testing.expectEqual(move_timestamp, response.command.timestamp);
        },
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var native_report = try api.querySingleUserReport(uid);
    defer native_report.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), native_report.orders.items.len);
    try std.testing.expectEqual(@as(i64, 101), native_report.orders.items[0].price);
    try std.testing.expectEqual(placement_timestamp, native_report.orders.items[0].timestamp);

    var exact_response = try api.submitBinaryReportQuery(
        .split,
        symbol,
        9_901_002_004,
        .{ .single_user = uid },
    );
    defer exact_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, exact_response.result_code);
    try std.testing.expect(exact_response.report_response == null);
    switch (exact_response.java_report_response orelse return error.MissingReportResponse) {
        .single_user => |report| {
            const symbol_orders = report.orders orelse return error.MissingReportResponse;
            var found = false;
            for (symbol_orders) |group| {
                if (group.symbol != symbol) continue;
                try std.testing.expectEqual(@as(usize, 1), group.orders.len);
                try std.testing.expectEqual(order_id, group.orders[0].order_id);
                try std.testing.expectEqual(@as(i64, 101), group.orders[0].price);
                try std.testing.expectEqual(placement_timestamp, group.orders[0].timestamp);
                found = true;
            }
            try std.testing.expect(found);
        },
        else => return error.UnexpectedReportResponseType,
    }

    var cancel = try api.submitApiCommandSync(.split, .{
        .cancel_order = .{
            .order_id = order_id,
            .uid = uid,
            .symbol = symbol,
            .timestamp = cancel_timestamp,
        },
    });
    defer cancel.deinit(allocator);
    switch (cancel) {
        .response => |response| {
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
            try std.testing.expectEqual(cancel_timestamp, response.command.timestamp);
        },
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var after_cancel = try api.querySingleUserReport(uid);
    defer after_cancel.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), after_cancel.orders.items.len);
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
        .persist_state => |code| try std.testing.expectEqual(
            cmd.CommandResultCode.state_persist_matching_engine_failed,
            code,
        ),
        .response => unreachable,
    }

    try std.testing.expectEqual(@as(?ApiCommandResult, null), api.takeApiCommandAsyncResult(add_handle));
    try std.testing.expectEqual(@as(?ApiCommandResult, null), api.takeApiCommandAsyncResult(persist_handle));
}

test "sharded exchange api ApiCommand binary dispatch accepts split mode" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    installTestJavaIdentityHashContext(&api);

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

    var add_result = try api.submitApiCommandResult(.split, .{
        .binary_add_symbols = .{
            .transfer_id = symbol,
            .timestamp = 9_920_100_001,
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

    const query_handle = try api.submitApiCommandAsync(.split, .{
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
            try expectJavaStateHashResponse(response);
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
        .persist_state => |code| try std.testing.expectEqual(
            cmd.CommandResultCode.state_persist_matching_engine_failed,
            code,
        ),
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

test "sharded exchange api typed binary wrappers broadcast across transfer shard mismatches" {
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

    var add_symbols = try api.submitAddSymbolsBinary(
        .routed,
        symbol_transfer,
        9_940_100_001,
        &.{spec},
    );
    defer add_symbols.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_symbols.result_code);

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
    const add_accounts = try api.submitAddAccountsBinaryAsync(
        .split,
        uid_transfer,
        9_940_100_002,
        users[0..],
    );
    _ = try api.drainPending();
    try std.testing.expectEqual(
        @as(?cmd.CommandResultCode, .success),
        api.takeResultCode(add_accounts),
    );

    var single_user = try api.submitBinaryReportQuery(
        .routed,
        uid_transfer,
        9_940_100_003,
        .{ .single_user = uid },
    );
    defer single_user.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, single_user.result_code);
    try std.testing.expect(single_user.report_response == null);
    switch (single_user.java_report_response orelse return error.MissingReportResponse) {
        .single_user => |report| {
            try std.testing.expectEqual(@as(i64, 0), report.uid);
            try std.testing.expectEqual(@as(?i64, 1_000), javaMapValue(report.accounts, 11));
        },
        else => return error.UnexpectedReportResponseType,
    }
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
            try std.testing.expectEqual(
                kernel_mod.SingleUserQueryExecutionStatus.ok,
                user_report.query_execution_status,
            );
            try std.testing.expectEqual(@as(?i64, 7_000), user_report.accounts.get(11));
        },
        else => return error.UnexpectedReportResponseType,
    }
    switch (single_user_result.java_report_response orelse return error.MissingReportResponse) {
        .single_user => |user_report| {
            try std.testing.expectEqual(@as(i64, 0), user_report.uid);
            try std.testing.expectEqual(
                java_report_wire.QueryExecutionStatus.ok,
                user_report.query_execution_status,
            );
            try std.testing.expectEqual(@as(?i64, 7_000), javaMapValue(user_report.accounts, 11));
        },
        else => return error.UnexpectedReportResponseType,
    }
}

test "sharded typed Java wire routes across asymmetric ME and RE topologies" {
    const allocator = std.testing.allocator;
    const Topology = struct {
        matching_engines: u64,
        risk_engines: u64,
    };
    const topologies = [_]Topology{
        .{ .matching_engines = 4, .risk_engines = 2 },
        .{ .matching_engines = 1, .risk_engines = 4 },
    };
    const modes = [_]ExecutionMode{ .routed, .split };

    for (topologies, 0..) |topology, topology_index| {
        for (modes, 0..) |mode, mode_index| {
            var java_config = config_mod.JavaExchangeConfig.javaDefault();
            java_config.performance_cfg.matching_engines_num = topology.matching_engines;
            java_config.performance_cfg.risk_engines_num = topology.risk_engines;
            java_config.performance_cfg.orderbook_kind = .direct;
            var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
            defer api.deinit();

            // Split covers the live worker path while routed exercises cold
            // direct publication through the same carrier-word builder.
            if (mode == .split) try api.startup();

            const discriminator: i32 = @intCast(topology_index * modes.len + mode_index);
            const symbol: i32 = 19_700 + discriminator;
            const uid: i64 = 99_700 + discriminator;
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
            var add_symbols = try api.submitAddSymbolsBinary(
                mode,
                20_700 + discriminator,
                @as(i64, 9_960_100_000) + discriminator,
                &.{spec},
            );
            defer add_symbols.deinit(allocator);
            try std.testing.expectEqual(cmd.CommandResultCode.success, add_symbols.result_code);

            const balances = [_]BinaryCurrencyBalance{
                .{ .currency = 11, .balance = 10_000 + discriminator },
            };
            const users = [_]BinaryUserAccountsBatch{
                .{ .uid = uid, .accounts = &balances },
            };
            if (mode == .routed) {
                const request_id = try api.submitAddAccountsBinaryAsync(
                    mode,
                    21_700 + discriminator,
                    @as(i64, 9_960_200_000) + discriminator,
                    &users,
                );
                _ = try api.drainPending();
                try std.testing.expectEqual(
                    @as(?cmd.CommandResultCode, .success),
                    api.takeResultCode(request_id),
                );
            } else {
                var add_accounts = try api.submitAddAccountsBinary(
                    mode,
                    21_700 + discriminator,
                    @as(i64, 9_960_200_000) + discriminator,
                    &users,
                );
                defer add_accounts.deinit(allocator);
                try std.testing.expectEqual(cmd.CommandResultCode.success, add_accounts.result_code);
            }

            var single_user = try api.submitBinaryReportQuery(
                mode,
                22_700 + discriminator,
                @as(i64, 9_960_300_000) + discriminator,
                .{ .single_user = uid },
            );
            defer single_user.deinit(allocator);
            try std.testing.expectEqual(cmd.CommandResultCode.success, single_user.result_code);
            try std.testing.expect(single_user.report_response == null);
            switch (single_user.java_report_response orelse return error.MissingReportResponse) {
                .single_user => |report| {
                    try std.testing.expectEqual(@as(i64, 0), report.uid);
                    try std.testing.expectEqual(
                        @as(?i64, 10_000 + discriminator),
                        javaMapValue(report.accounts, 11),
                    );
                },
                else => return error.UnexpectedReportResponseType,
            }
        }
    }
}

test "sharded exchange api compound publications reject atomically with one free slot" {
    const allocator = std.testing.allocator;
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 4;
    java_config.performance_cfg.matching_engines_num = 4;
    java_config.performance_cfg.risk_engines_num = 4;
    java_config.performance_cfg.msgs_in_group_limit = 1;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();

    var accepted_ids: [3]u64 = undefined;
    for (&accepted_ids, 0..) |*request_id, index| {
        request_id.* = try api.addUserAsync(.split, 129_000 + @as(i64, @intCast(index)));
    }

    const next_request_before = api.next_request_id;
    const sequence_before = api.driver.publication_sequencer.highWater();
    try std.testing.expectError(
        error.CoreQueueFull,
        api.persistStateAsync(.routed, 129_100, 9_949_100_001),
    );
    const two_frame_payload = [_]u8{0} ** 41;
    try std.testing.expectError(
        error.CoreQueueFull,
        api.submitBinaryDataPayloadAsync(.routed, 129_101, 9_949_100_002, &two_frame_payload),
    );

    api.driver.mutex.lockUncancelable(api.driver.io);
    const observed = .{
        api.driver.queue.count(),
        api.driver.completion_slots_used,
        api.driver.accepted_total,
    };
    api.driver.mutex.unlock(api.driver.io);
    try std.testing.expectEqual(@as(usize, 3), observed[0]);
    try std.testing.expectEqual(@as(usize, 3), observed[1]);
    try std.testing.expectEqual(@as(u64, 3), observed[2]);
    try std.testing.expectEqual(next_request_before, api.next_request_id);
    try std.testing.expectEqual(sequence_before, api.driver.publication_sequencer.highWater());

    try api.startup();
    try std.testing.expectEqual(@as(usize, 3), try api.drainPending());
    for (accepted_ids) |request_id| {
        var response = api.takeResult(request_id) orelse return error.MissingAsyncResult;
        response.deinit(allocator);
    }
}

test "sharded exchange api binary async compound exposes only terminal completion" {
    const allocator = std.testing.allocator;
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 4;
    java_config.performance_cfg.matching_engines_num = 4;
    java_config.performance_cfg.risk_engines_num = 4;
    java_config.performance_cfg.msgs_in_group_limit = 1;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();

    const spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = 5_945,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
    });
    const payload = try binary_payload.encodeAddSymbolsToBytes(allocator, &.{spec});
    defer allocator.free(payload);
    const layout = try computeBinaryFrameLayout(payload.len);
    try std.testing.expectEqual(@as(usize, 2), layout.frame_count);

    const terminal_request_id = try api.submitBinaryDataPayloadAsync(
        .routed,
        spec.symbol_id,
        9_949_200_001,
        payload,
    );
    try std.testing.expectEqual(@as(u64, 2), terminal_request_id);
    api.driver.mutex.lockUncancelable(api.driver.io);
    const observed = .{
        api.driver.queue.count(),
        api.driver.completion_slots_used,
    };
    api.driver.mutex.unlock(api.driver.io);
    try std.testing.expectEqual(@as(usize, 2), observed[0]);
    try std.testing.expectEqual(@as(usize, 1), observed[1]);

    try api.startup();
    try std.testing.expectEqual(@as(usize, 2), try api.drainPending());
    try std.testing.expectEqual(@as(usize, 1), api.readyCount());
    try std.testing.expect(api.takeResult(1) == null);
    var response = api.takeResult(terminal_request_id) orelse return error.MissingAsyncResult;
    defer response.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 2), response.sequence);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    try std.testing.expectEqual(@as(usize, 0), api.readyCount());
    try std.testing.expectEqual(@as(usize, 0), api.driver.completion_slots_used);
}

test "sharded running binary streams cross bounded queues with terminal-only completion" {
    const allocator = std.testing.allocator;

    for ([_]usize{ 4, 8 }) |capacity| {
        for ([_]ExecutionMode{ .routed, .split }) |mode| {
            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();
            var java_config = config_mod.JavaExchangeConfig.javaDefault();
            java_config.performance_cfg.ring_buffer_size = capacity;
            java_config.performance_cfg.matching_engines_num = 4;
            java_config.performance_cfg.risk_engines_num = 2;
            java_config.performance_cfg.msgs_in_group_limit = capacity / 4;
            java_config.performance_cfg.wait_strategy = .blocking;
            java_config.serialization_cfg = config_mod.SerializationConfig.diskJournaling();
            var api = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
                allocator,
                tmp.dir,
                java_config,
            );
            defer api.deinit();
            installTestJavaIdentityHashContext(&api);
            try api.startup();

            // Move the physical head away from zero before every oversized
            // publication so reservations cross the ring boundary.
            const wrap_request = try api.nopAsync(mode);
            try std.testing.expectEqual(@as(usize, 1), try api.drainPending());
            var wrap_response = api.takeResult(wrap_request) orelse return error.MissingAsyncResult;
            wrap_response.deinit(allocator);
            api.driver.mutex.lockUncancelable(api.driver.io);
            const head_before_sync = api.driver.queue.head;
            api.driver.mutex.unlock(api.driver.io);

            const cap_plus_one = capacity + 1;
            const sync_payload = try encodeAddAccountsPayloadForFrameCount(
                allocator,
                cap_plus_one,
                400_000 + @as(i64, @intCast(capacity * 100 + @intFromEnum(mode) * 10)),
            );
            defer allocator.free(sync_payload);
            const sync_sequence_before = api.driver.publication_sequencer.highWater();
            const sync_request_before = api.next_request_id;
            const sync_journal_start = api.journal.?.len();
            var sync_response = try api.submitBinaryPayload(
                mode,
                .binary_data_command,
                @intCast(40_000 + capacity * 10 + @intFromEnum(mode)),
                1,
                sync_payload,
            );
            defer sync_response.deinit(allocator);
            try std.testing.expectEqual(cmd.CommandResultCode.success, sync_response.result_code);
            try std.testing.expectEqual(@as(i32, -1), sync_response.command.symbol);
            try std.testing.expectEqual(
                sync_sequence_before + @as(u64, @intCast(cap_plus_one)),
                sync_response.sequence,
            );
            try std.testing.expectEqual(
                sync_request_before + @as(u64, @intCast(cap_plus_one)),
                api.next_request_id,
            );
            try expectJournalBinaryTransfer(
                &api.journal.?,
                sync_journal_start,
                cap_plus_one,
                @intCast(40_000 + capacity * 10 + @intFromEnum(mode)),
                .binary_data_command,
                mode,
            );
            try std.testing.expectEqual(@as(usize, 0), api.readyCount());
            api.driver.mutex.lockUncancelable(api.driver.io);
            const head_after_sync = api.driver.queue.head;
            api.driver.mutex.unlock(api.driver.io);
            try std.testing.expectEqual(
                (head_before_sync + cap_plus_one) & (capacity - 1),
                head_after_sync,
            );

            const twice_capacity = capacity * 2;
            const async_payload = try encodeAddAccountsPayloadForFrameCount(
                allocator,
                twice_capacity,
                500_000 + @as(i64, @intCast(capacity * 100 + @intFromEnum(mode) * 10)),
            );
            defer allocator.free(async_payload);
            const async_sequence_before = api.driver.publication_sequencer.highWater();
            const async_request_before = api.next_request_id;
            const async_journal_start = api.journal.?.len();
            const async_transfer: i32 = @intCast(41_000 + capacity * 10 + @intFromEnum(mode));
            const async_request = try api.submitBinaryPayloadAsync(
                mode,
                .binary_data_command,
                async_transfer,
                2,
                async_payload,
            );
            try std.testing.expectEqual(
                async_request_before + @as(u64, @intCast(twice_capacity)),
                async_request,
            );
            try std.testing.expectEqual(twice_capacity, try api.drainPending());
            try std.testing.expectEqual(@as(usize, 1), api.readyCount());
            var async_response = api.takeResult(async_request) orelse return error.MissingAsyncResult;
            defer async_response.deinit(allocator);
            try std.testing.expectEqual(cmd.CommandResultCode.success, async_response.result_code);
            try std.testing.expectEqual(@as(i32, -1), async_response.command.symbol);
            try std.testing.expectEqual(
                async_sequence_before + @as(u64, @intCast(twice_capacity)),
                async_response.sequence,
            );
            try expectJournalBinaryTransfer(
                &api.journal.?,
                async_journal_start,
                twice_capacity,
                async_transfer,
                .binary_data_command,
                mode,
            );
            try std.testing.expectEqual(@as(usize, 0), api.readyCount());

            const detached_payload = try encodeAddAccountsPayloadForFrameCount(
                allocator,
                cap_plus_one,
                600_000 + @as(i64, @intCast(capacity * 100 + @intFromEnum(mode) * 10)),
            );
            defer allocator.free(detached_payload);
            const detached_words = try api.rawPayloadCarrierWords(detached_payload);
            defer allocator.free(detached_words);
            const detached_sequence_before = api.driver.publication_sequencer.highWater();
            const detached_request_before = api.next_request_id;
            const detached_journal_start = api.journal.?.len();
            const detached_transfer: i32 = @intCast(42_000 + capacity * 10 + @intFromEnum(mode));
            try api.submitBinaryCarrierWordsDetached(
                mode,
                .binary_data_command,
                detached_transfer,
                3,
                detached_words,
            );
            try std.testing.expectEqual(detached_request_before, api.next_request_id);
            var detached_barrier = try api.nop(mode);
            defer detached_barrier.deinit(allocator);
            try std.testing.expectEqual(
                detached_sequence_before + @as(u64, @intCast(cap_plus_one)) + 1,
                detached_barrier.sequence,
            );
            try expectJournalBinaryTransfer(
                &api.journal.?,
                detached_journal_start,
                cap_plus_one,
                detached_transfer,
                .binary_data_command,
                mode,
            );

            const expected_frontier = api.driver.publication_sequencer.highWater();
            try expectShardedBinaryFrontier(&api, expected_frontier);
            api.driver.mutex.lockUncancelable(api.driver.io);
            const balanced = .{
                api.driver.accepted_total,
                api.driver.completed_total,
                api.driver.journal_slots_reserved,
                api.driver.completion_slots_used,
                api.driver.callback_completions_pending,
                api.driver.blocked_producers,
                api.driver.prepared_available_remaining,
                api.driver.active_binary_stream,
                api.driver.queue.count(),
            };
            api.driver.mutex.unlock(api.driver.io);
            try std.testing.expectEqual(balanced[0], balanced[1]);
            try std.testing.expectEqual(@as(usize, 0), balanced[2]);
            try std.testing.expectEqual(@as(usize, 0), balanced[3]);
            try std.testing.expectEqual(@as(usize, 0), balanced[4]);
            try std.testing.expectEqual(@as(usize, 0), balanced[5]);
            try std.testing.expectEqual(@as(usize, 0), balanced[6]);
            try std.testing.expect(!balanced[7]);
            try std.testing.expectEqual(@as(usize, 0), balanced[8]);
        }
    }
}

test "sharded running callback admits fitting binary stream exactly once" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 4;
    java_config.performance_cfg.matching_engines_num = 4;
    java_config.performance_cfg.risk_engines_num = 2;
    java_config.performance_cfg.msgs_in_group_limit = 1;
    java_config.performance_cfg.wait_strategy = .blocking;
    java_config.serialization_cfg = config_mod.SerializationConfig.diskJournaling();
    var target = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        java_config,
    );
    defer target.deinit();
    installTestJavaIdentityHashContext(&target);
    try target.startup();

    const frame_count: usize = 2;
    const transfer_id: i32 = 44_801;
    const payload = try encodeAddAccountsPayloadForFrameCount(
        allocator,
        frame_count,
        689_000,
    );
    defer allocator.free(payload);

    const Probe = struct {
        target: *ExchangeApiSharded,
        payload: []const u8,
        transfer_id: i32,
        callback_count: usize = 0,
        request_id: ?u64 = null,
        failure: ?anyerror = null,

        fn onResponse(
            context: ?*anyopaque,
            _: *const kernel_mod.CommandResponse,
        ) void {
            const self: *@This() = @ptrCast(@alignCast(context.?));
            self.callback_count += 1;
            self.request_id = self.target.submitBinaryPayloadAsync(
                .routed,
                .binary_data_command,
                self.transfer_id,
                1,
                self.payload,
            ) catch |err| {
                self.failure = err;
                return;
            };
        }
    };

    const driver = target.driver;
    driver.mutex.lockUncancelable(driver.io);
    const journal_start = target.journal.?.len();
    try std.testing.expectEqual(@as(u64, 0), driver.accepted_total);
    try std.testing.expectEqual(@as(u64, 0), driver.completed_total);
    try std.testing.expectEqual(@as(usize, 0), driver.queue.count());
    driver.mutex.unlock(driver.io);

    var probe: Probe = .{
        .target = &target,
        .payload = payload,
        .transfer_id = transfer_id,
    };
    var source = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer source.deinit();
    try source.createUserWithCallback(.routed, 689_100, .{
        .context = &probe,
        .callback = Probe.onResponse,
    });
    try std.testing.expectEqual(@as(usize, 1), try source.drainPending());

    try std.testing.expectEqual(@as(usize, 1), probe.callback_count);
    try std.testing.expectEqual(@as(?anyerror, null), probe.failure);
    const request_id = probe.request_id orelse return error.MissingAsyncResult;
    try std.testing.expectEqual(@as(u64, frame_count), request_id);
    try std.testing.expectEqual(@as(usize, frame_count), try target.drainPending());
    try std.testing.expectEqual(@as(usize, 0), try target.drainPending());
    try std.testing.expectEqual(@as(usize, 1), target.readyCount());

    var response = target.takeResult(request_id) orelse
        return error.MissingAsyncResult;
    defer response.deinit(allocator);
    try std.testing.expectEqual(@as(u64, frame_count), response.sequence);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    try std.testing.expectEqual(@as(i32, -1), response.command.symbol);
    try std.testing.expect(target.takeResult(request_id) == null);
    try std.testing.expectEqual(@as(usize, 0), target.readyCount());

    try expectJournalBinaryTransfer(
        &target.journal.?,
        journal_start,
        frame_count,
        transfer_id,
        .binary_data_command,
        .routed,
    );
    driver.mutex.lockUncancelable(driver.io);
    defer driver.mutex.unlock(driver.io);
    try std.testing.expectEqual(@as(u64, frame_count), driver.accepted_total);
    try std.testing.expectEqual(driver.accepted_total, driver.completed_total);
    try std.testing.expectEqual(@as(u64, frame_count), driver.async_accepted_total);
    try std.testing.expectEqual(
        driver.async_accepted_total,
        driver.async_completed_total,
    );
    try std.testing.expectEqual(@as(u64, frame_count), target.next_request_id);
    try std.testing.expectEqual(
        @as(u64, frame_count),
        driver.publication_sequencer.highWater(),
    );
    try std.testing.expectEqual(@as(usize, 0), driver.queue.count());
    try std.testing.expectEqual(@as(usize, 0), driver.completion_slots_used);
    try std.testing.expectEqual(@as(usize, 0), driver.journal_slots_reserved);
    try std.testing.expectEqual(@as(usize, 0), driver.prepared_available_remaining);
    try std.testing.expect(!driver.active_binary_stream);
}

test "sharded running callback rejects oversized binary stream before admission" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 4;
    java_config.performance_cfg.matching_engines_num = 4;
    java_config.performance_cfg.risk_engines_num = 2;
    java_config.performance_cfg.msgs_in_group_limit = 1;
    java_config.performance_cfg.wait_strategy = .blocking;
    java_config.serialization_cfg = config_mod.SerializationConfig.diskJournaling();
    var target = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        java_config,
    );
    defer target.deinit();
    installTestJavaIdentityHashContext(&target);
    try target.startup();

    const payload = try encodeAddAccountsPayloadForFrameCount(
        allocator,
        5,
        690_000,
    );
    defer allocator.free(payload);

    const Probe = struct {
        target: *ExchangeApiSharded,
        payload: []const u8,
        callback_count: usize = 0,
        request_id: ?u64 = null,
        failure: ?anyerror = null,

        fn onResponse(
            context: ?*anyopaque,
            _: *const kernel_mod.CommandResponse,
        ) void {
            const self: *@This() = @ptrCast(@alignCast(context.?));
            self.callback_count += 1;
            self.request_id = self.target.submitBinaryPayloadAsync(
                .routed,
                .binary_data_command,
                44_901,
                1,
                self.payload,
            ) catch |err| {
                self.failure = err;
                return;
            };
        }
    };

    const driver = target.driver;
    driver.mutex.lockUncancelable(driver.io);
    const accepted_before = driver.accepted_total;
    const async_accepted_before = driver.async_accepted_total;
    const request_before = target.next_request_id;
    const sequence_before = driver.publication_sequencer.highWater();
    const queued_before = driver.queue.count();
    const completion_before = driver.completion_slots_used;
    const journal_slots_before = driver.journal_slots_reserved;
    const journal_entries_before = target.journal.?.len();
    const prepared_before = driver.prepared_available_remaining;
    const stream_before = driver.active_binary_stream;
    driver.mutex.unlock(driver.io);

    var probe: Probe = .{
        .target = &target,
        .payload = payload,
    };
    var source = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer source.deinit();
    try source.createUserWithCallback(.routed, 690_100, .{
        .context = &probe,
        .callback = Probe.onResponse,
    });
    try std.testing.expectEqual(@as(usize, 1), try source.drainPending());

    try std.testing.expectEqual(@as(usize, 1), probe.callback_count);
    try std.testing.expectEqual(@as(?u64, null), probe.request_id);
    try std.testing.expectEqual(
        @as(?anyerror, error.CoreReentrantCall),
        probe.failure,
    );

    driver.mutex.lockUncancelable(driver.io);
    defer driver.mutex.unlock(driver.io);
    try std.testing.expectEqual(accepted_before, driver.accepted_total);
    try std.testing.expectEqual(async_accepted_before, driver.async_accepted_total);
    try std.testing.expectEqual(request_before, target.next_request_id);
    try std.testing.expectEqual(sequence_before, driver.publication_sequencer.highWater());
    try std.testing.expectEqual(queued_before, driver.queue.count());
    try std.testing.expectEqual(completion_before, driver.completion_slots_used);
    try std.testing.expectEqual(journal_slots_before, driver.journal_slots_reserved);
    try std.testing.expectEqual(journal_entries_before, target.journal.?.len());
    try std.testing.expectEqual(prepared_before, driver.prepared_available_remaining);
    try std.testing.expectEqual(stream_before, driver.active_binary_stream);
}

test "sharded cold oversized binary async and detached publication reject atomically" {
    const allocator = std.testing.allocator;
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 4;
    java_config.performance_cfg.matching_engines_num = 4;
    java_config.performance_cfg.risk_engines_num = 2;
    java_config.performance_cfg.msgs_in_group_limit = 1;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();
    installTestJavaIdentityHashContext(&api);

    const payload = try encodeAddAccountsPayloadForFrameCount(allocator, 5, 700_000);
    defer allocator.free(payload);

    // Cold synchronous execution remains direct and is not ring-bounded.
    var sync_response = try api.submitBinaryPayload(
        .routed,
        .binary_data_command,
        45_001,
        1,
        payload,
    );
    defer sync_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, sync_response.result_code);

    const request_before = api.next_request_id;
    const sequence_before = api.driver.publication_sequencer.highWater();
    const accepted_before = api.driver.accepted_total;
    {
        var callback_scope = runtime_mod.CallbackExecutionScope.enter();
        defer callback_scope.leave();
        try std.testing.expectError(
            error.CoreQueueFull,
            api.submitBinaryPayloadAsync(.split, .binary_data_command, 45_002, 2, payload),
        );
    }
    const carrier_words = try api.rawPayloadCarrierWords(payload);
    defer allocator.free(carrier_words);
    try std.testing.expectError(
        error.CoreQueueFull,
        api.submitBinaryCarrierWordsDetached(
            .routed,
            .binary_data_command,
            45_003,
            3,
            carrier_words,
        ),
    );

    try std.testing.expectEqual(request_before, api.next_request_id);
    try std.testing.expectEqual(sequence_before, api.driver.publication_sequencer.highWater());
    try std.testing.expectEqual(accepted_before, api.driver.accepted_total);
    try std.testing.expectEqual(@as(usize, 0), api.driver.queue.count());
    try std.testing.expectEqual(@as(usize, 0), api.driver.completion_slots_used);
    try std.testing.expectEqual(@as(usize, 0), api.driver.journal_slots_reserved);
    try std.testing.expectEqual(@as(usize, 0), api.driver.prepared_available_remaining);
    try std.testing.expect(!api.driver.active_binary_stream);
    try expectShardedBinaryFrontier(&api, sequence_before);
}

test "sharded binary stream journal OOM fails before admission" {
    const allocator = std.testing.allocator;
    var failing = std.testing.FailingAllocator.init(allocator, .{});
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 4;
    java_config.performance_cfg.matching_engines_num = 4;
    java_config.performance_cfg.risk_engines_num = 2;
    java_config.performance_cfg.msgs_in_group_limit = 1;
    java_config.performance_cfg.wait_strategy = .blocking;
    java_config.serialization_cfg = config_mod.SerializationConfig.diskJournaling();
    var api = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
        failing.allocator(),
        tmp.dir,
        java_config,
    );
    defer api.deinit();
    try api.startup();

    var commands: [5]cmd.OrderCommand = undefined;
    for (&commands, 0..) |*command, index| {
        command.* = makeBinaryFrameCommand(
            .binary_data_query,
            45_101,
            if (index + 1 == commands.len) -1 else 0,
            5,
            [_]i64{0} ** binary_frames.FrameWordsPerCommand,
        );
    }

    const request_before = api.next_request_id;
    const sequence_before = api.driver.publication_sequencer.highWater();
    const accepted_before = api.driver.accepted_total;
    failing.fail_index = failing.alloc_index;
    defer failing.fail_index = std.math.maxInt(usize);
    try std.testing.expectError(
        error.OutOfMemory,
        api.enqueueBinaryFrames(&commands, .routed, .asynchronous),
    );
    failing.fail_index = std.math.maxInt(usize);

    try std.testing.expect(failing.has_induced_failure);
    try std.testing.expectEqual(request_before, api.next_request_id);
    try std.testing.expectEqual(sequence_before, api.driver.publication_sequencer.highWater());
    try std.testing.expectEqual(accepted_before, api.driver.accepted_total);
    try std.testing.expectEqual(@as(usize, 0), api.journal.?.len());
    try std.testing.expectEqual(@as(usize, 0), api.driver.journal_slots_reserved);
    try std.testing.expectEqual(@as(usize, 0), api.driver.completion_slots_used);
    try std.testing.expectEqual(@as(usize, 0), api.driver.queue.count());
    try std.testing.expectEqual(@as(usize, 0), api.driver.prepared_available_remaining);
    try std.testing.expect(!api.driver.active_binary_stream);
    try expectShardedBinaryFrontier(&api, sequence_before);
}

test "sharded binary stream gate keeps concurrent publisher after reserved range" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var gate: ShardedBackpressureGate = .{ .io = std.testing.io };
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 4;
    java_config.performance_cfg.matching_engines_num = 4;
    java_config.performance_cfg.risk_engines_num = 2;
    java_config.performance_cfg.msgs_in_group_limit = 1;
    java_config.performance_cfg.wait_strategy = .blocking;
    java_config.serialization_cfg = config_mod.SerializationConfig.diskJournaling();
    var api = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        java_config,
    );
    defer api.deinit();
    defer gate.release();
    api.setResultSink(.{
        .context = &gate,
        .on_result = ShardedBackpressureGate.onResult,
    });
    try api.startup();

    // Hold the sole consumer outside the driver lock so the binary producer
    // fills the ring and pauses between its already-reserved sequence chunks.
    try api.enqueueRuntime(makeNopCommand());
    const payload = try encodeAddAccountsPayloadForFrameCount(allocator, 9, 710_000);
    defer allocator.free(payload);
    var binary_publisher: ShardedBinaryAsyncPublisher = .{
        .api = &api,
        .mode = .routed,
        .payload = payload,
        .transfer_id = 46_051,
    };
    const binary_thread = try std.Thread.spawn(
        .{},
        ShardedBinaryAsyncPublisher.run,
        .{&binary_publisher},
    );
    gate.waitEntered();

    api.driver.mutex.lockUncancelable(api.driver.io);
    while (!api.driver.active_binary_stream or api.driver.blocked_producers == 0) {
        api.driver.completion_available.waitUncancelable(
            api.driver.io,
            &api.driver.mutex,
        );
    }
    api.driver.mutex.unlock(api.driver.io);

    var ordinary_publisher: ShardedOrdinaryPublisher = .{
        .api = &api,
        .mode = .split,
        .uid = 710_100,
    };
    const ordinary_thread = try std.Thread.spawn(
        .{},
        ShardedOrdinaryPublisher.run,
        .{&ordinary_publisher},
    );
    api.driver.mutex.lockUncancelable(api.driver.io);
    while (api.driver.blocked_producers < 2) {
        api.driver.completion_available.waitUncancelable(
            api.driver.io,
            &api.driver.mutex,
        );
    }
    api.driver.mutex.unlock(api.driver.io);

    gate.release();
    binary_thread.join();
    ordinary_thread.join();
    const binary_request = try binary_publisher.result.?;
    const ordinary_request = try ordinary_publisher.result.?;
    try std.testing.expectEqual(@as(usize, 10), try api.drainPending());

    var binary_response = api.takeResult(binary_request) orelse
        return error.MissingAsyncResult;
    defer binary_response.deinit(allocator);
    var ordinary_response = api.takeResult(ordinary_request) orelse
        return error.MissingAsyncResult;
    defer ordinary_response.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 10), binary_response.sequence);
    try std.testing.expectEqual(@as(i32, -1), binary_response.command.symbol);
    try std.testing.expectEqual(cmd.CommandResultCode.success, binary_response.result_code);
    try std.testing.expectEqual(@as(u64, 11), ordinary_response.sequence);
    try std.testing.expectEqual(cmd.CommandResultCode.success, ordinary_response.result_code);

    const journal = &api.journal.?;
    try std.testing.expectEqual(@as(usize, 11), journal.len());
    try std.testing.expectEqual(cmd.OrderCommandType.nop, journal.entries.items[0].input.command);
    try expectJournalBinaryTransfer(
        journal,
        1,
        9,
        46_051,
        .binary_data_command,
        .routed,
    );
    try std.testing.expectEqual(
        cmd.OrderCommandType.add_user,
        journal.entries.items[10].input.command,
    );
    try std.testing.expectEqual(ExecutionMode.split, journal.entries.items[10].mode);
    try expectShardedBinaryFrontier(&api, 11);

    api.driver.mutex.lockUncancelable(api.driver.io);
    const balanced = .{
        api.driver.accepted_total,
        api.driver.completed_total,
        api.driver.async_accepted_total,
        api.driver.async_completed_total,
        api.driver.runtime_accepted_total,
        api.driver.runtime_completed_total,
        api.driver.journal_slots_reserved,
        api.driver.completion_slots_used,
        api.driver.blocked_producers,
        api.driver.prepared_available_remaining,
        api.driver.active_binary_stream,
        api.driver.queue.count(),
    };
    api.driver.mutex.unlock(api.driver.io);
    try std.testing.expectEqual(balanced[0], balanced[1]);
    try std.testing.expectEqual(balanced[2], balanced[3]);
    try std.testing.expectEqual(balanced[4], balanced[5]);
    try std.testing.expectEqual(@as(usize, 0), balanced[6]);
    try std.testing.expectEqual(@as(usize, 0), balanced[7]);
    try std.testing.expectEqual(@as(usize, 0), balanced[8]);
    try std.testing.expectEqual(@as(usize, 0), balanced[9]);
    try std.testing.expect(!balanced[10]);
    try std.testing.expectEqual(@as(usize, 0), balanced[11]);
}

test "sharded shutdown waits through empty binary stream gaps" {
    const allocator = std.testing.allocator;
    var gate: ShardedBackpressureGate = .{ .io = std.testing.io };
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 4;
    java_config.performance_cfg.matching_engines_num = 4;
    java_config.performance_cfg.risk_engines_num = 2;
    java_config.performance_cfg.msgs_in_group_limit = 1;
    java_config.performance_cfg.wait_strategy = .blocking;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();
    defer gate.release();
    api.setResultSink(.{
        .context = &gate,
        .on_result = ShardedBackpressureGate.onResult,
    });
    try api.startup();

    try api.enqueueRuntime(makeNopCommand());
    const payload = try encodeAddAccountsPayloadForFrameCount(allocator, 12, 720_000);
    defer allocator.free(payload);
    var publisher: ShardedBinaryAsyncPublisher = .{
        .api = &api,
        .mode = .split,
        .payload = payload,
        .transfer_id = 46_101,
    };
    const publisher_thread = try std.Thread.spawn(
        .{},
        ShardedBinaryAsyncPublisher.run,
        .{&publisher},
    );
    gate.waitEntered();

    api.driver.mutex.lockUncancelable(api.driver.io);
    while (!api.driver.active_binary_stream or api.driver.blocked_producers == 0) {
        api.driver.completion_available.waitUncancelable(
            api.driver.io,
            &api.driver.mutex,
        );
    }
    api.driver.mutex.unlock(api.driver.io);

    var shutdown_runner: ShardedShutdownRunner = .{
        .api = &api,
        .io = std.testing.io,
    };
    const shutdown_thread = try std.Thread.spawn(
        .{},
        ShardedShutdownRunner.run,
        .{&shutdown_runner},
    );
    shutdown_runner.waitEntered();
    api.driver.mutex.lockUncancelable(api.driver.io);
    while (api.driver.lifecycle.current() == .running) {
        api.driver.completion_available.waitUncancelable(
            api.driver.io,
            &api.driver.mutex,
        );
    }
    try std.testing.expectEqual(runtime_mod.RuntimeLifecycle.closing, api.driver.lifecycle.current());
    api.driver.mutex.unlock(api.driver.io);
    try std.testing.expect(!shutdown_runner.isDone());

    gate.release();
    publisher_thread.join();
    const request_id = try publisher.result.?;
    shutdown_thread.join();

    try std.testing.expect(shutdown_runner.isDone());
    try std.testing.expect(api.isStopped());
    var response = api.takeResult(request_id) orelse return error.MissingAsyncResult;
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    try std.testing.expectEqual(@as(i32, -1), response.command.symbol);
    try std.testing.expectEqual(@as(u64, 13), response.sequence);
    try expectShardedBinaryFrontier(&api, 13);

    api.driver.mutex.lockUncancelable(api.driver.io);
    const balanced = .{
        api.driver.accepted_total,
        api.driver.completed_total,
        api.driver.async_accepted_total,
        api.driver.async_completed_total,
        api.driver.runtime_accepted_total,
        api.driver.runtime_completed_total,
        api.driver.journal_slots_reserved,
        api.driver.completion_slots_used,
        api.driver.blocked_producers,
        api.driver.prepared_available_remaining,
        api.driver.active_binary_stream,
        api.driver.queue.count(),
    };
    api.driver.mutex.unlock(api.driver.io);
    try std.testing.expectEqual(balanced[0], balanced[1]);
    try std.testing.expectEqual(balanced[2], balanced[3]);
    try std.testing.expectEqual(balanced[4], balanced[5]);
    try std.testing.expectEqual(@as(usize, 0), balanced[6]);
    try std.testing.expectEqual(@as(usize, 0), balanced[7]);
    try std.testing.expectEqual(@as(usize, 0), balanced[8]);
    try std.testing.expectEqual(@as(usize, 0), balanced[9]);
    try std.testing.expect(!balanced[10]);
    try std.testing.expectEqual(@as(usize, 0), balanced[11]);
    try std.testing.expectEqual(@as(usize, 0), api.readyCount());
    try std.testing.expectEqual(@as(usize, 0), api.pendingCount());
    try std.testing.expectEqual(@as(usize, 0), api.runtimePendingCount());
}

test "sharded binary stream poison settles unpublished suffix" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var gate: ShardedBackpressureGate = .{ .io = std.testing.io };
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 4;
    java_config.performance_cfg.matching_engines_num = 4;
    java_config.performance_cfg.risk_engines_num = 2;
    java_config.performance_cfg.msgs_in_group_limit = 1;
    java_config.performance_cfg.wait_strategy = .blocking;
    java_config.serialization_cfg = config_mod.SerializationConfig.diskJournaling();
    java_config.sharded_pipeline_mode = .immediate;
    var api = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        java_config,
    );
    defer api.deinit();
    defer gate.release();
    var raw_probe: RawR2Probe = .{};
    api.setRawResultSink(.{
        .context = &raw_probe,
        .on_result = RawR2Probe.onResult,
    });
    api.setResultSink(.{
        .context = &gate,
        .on_result = ShardedBackpressureGate.onResult,
    });
    try api.startup();

    try api.enqueueRuntime(makeNopCommand());
    const payload = try encodeAddAccountsPayloadForFrameCount(allocator, 9, 730_000);
    defer allocator.free(payload);
    var publisher: ShardedBinaryAsyncPublisher = .{
        .api = &api,
        .mode = .split,
        .payload = payload,
        .transfer_id = 47_001,
    };
    const publisher_thread = try std.Thread.spawn(
        .{},
        ShardedBinaryAsyncPublisher.run,
        .{&publisher},
    );
    gate.waitEntered();

    api.driver.mutex.lockUncancelable(api.driver.io);
    while (!api.driver.active_binary_stream or api.driver.blocked_producers == 0) {
        api.driver.completion_available.waitUncancelable(
            api.driver.io,
            &api.driver.mutex,
        );
    }
    api.driver.mutex.unlock(api.driver.io);

    // The runtime prefix already advanced every real shard to sequence 1.
    // Raising only the aggregate frontier makes the first streamed frame fail
    // before mutation and deterministically exercises unpublished settlement.
    api.execution_mutex.lockUncancelable(api.driver.io);
    api.core.kernels.next_seq = 100;
    api.execution_mutex.unlock(api.driver.io);
    gate.release();
    publisher_thread.join();
    try std.testing.expectError(error.CorePoisoned, publisher.result.?);
    api.shutdown();

    const fatal = api.fatalInfo() orelse return error.MissingFatalRecord;
    try std.testing.expectEqual(error.NonMonotonicPublicationSequence, fatal.cause);
    try std.testing.expectEqual(@as(?u64, 2), fatal.sequence);
    try std.testing.expectEqual(@as(?u64, null), fatal.request_id);
    try std.testing.expectEqual(
        @as(?cmd.OrderCommandType, .binary_data_command),
        fatal.command,
    );
    try std.testing.expectEqual(@as(u64, 10), api.driver.publication_sequencer.highWater());
    try std.testing.expectEqual(@as(usize, 1), api.journal.?.len());
    try std.testing.expectEqual(cmd.OrderCommandType.nop, api.journal.?.entries.items[0].input.command);
    try std.testing.expectEqual(@as(usize, 1), raw_probe.count);
    try std.testing.expectEqual(@as(u64, 1), raw_probe.sequence);

    api.driver.mutex.lockUncancelable(api.driver.io);
    const balanced = .{
        api.driver.accepted_total,
        api.driver.completed_total,
        api.driver.async_accepted_total,
        api.driver.async_completed_total,
        api.driver.runtime_accepted_total,
        api.driver.runtime_completed_total,
        api.driver.journal_slots_reserved,
        api.driver.completion_slots_used,
        api.driver.callback_completions_pending,
        api.driver.blocked_producers,
        api.driver.prepared_available_remaining,
        api.driver.active_binary_stream,
        api.driver.queue.count(),
    };
    api.driver.mutex.unlock(api.driver.io);
    try std.testing.expectEqual(balanced[0], balanced[1]);
    try std.testing.expectEqual(balanced[2], balanced[3]);
    try std.testing.expectEqual(balanced[4], balanced[5]);
    try std.testing.expectEqual(@as(usize, 0), balanced[6]);
    try std.testing.expectEqual(@as(usize, 0), balanced[7]);
    try std.testing.expectEqual(@as(usize, 0), balanced[8]);
    try std.testing.expectEqual(@as(usize, 0), balanced[9]);
    try std.testing.expectEqual(@as(usize, 0), balanced[10]);
    try std.testing.expect(!balanced[11]);
    try std.testing.expectEqual(@as(usize, 0), balanced[12]);
    try std.testing.expectEqual(@as(usize, 0), api.readyCount());
    try std.testing.expectEqual(@as(usize, 0), api.pendingCount());
    try std.testing.expectEqual(@as(usize, 0), api.runtimePendingCount());
    try std.testing.expectEqual(@as(u64, 100), api.core.kernels.next_seq);
    for (api.core.kernels.matching_shards) |*kernel| {
        try std.testing.expectEqual(@as(u64, 1), kernel.next_seq);
        try std.testing.expectEqual(@as(usize, 0), kernel.binary_frame_assembler.pendingTransfers());
    }
    for (api.core.kernels.risk_shards) |*risk_shard| {
        try std.testing.expectEqual(@as(u64, 1), risk_shard.next_seq);
        try std.testing.expectEqual(@as(usize, 0), risk_shard.binary_frame_assembler.pendingTransfers());
    }
}

test "sharded drain ignores a post-snapshot binary suffix until the earlier prefix settles" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    // Model three already-published owners followed by a two-frame
    // unpublished binary suffix. The suffix can be terminally accounted before
    // an earlier callback returns, but it must not satisfy the captured prefix.
    api.driver.mutex.lockUncancelable(api.driver.io);
    api.driver.accepted_total = 5;
    api.driver.async_accepted_total = 5;
    api.driver.mutex.unlock(api.driver.io);

    var drain: ShardedAcceptedDrainRunner = .{
        .api = &api,
        .io = api.driver.io,
    };
    const drain_thread = try std.Thread.spawn(
        .{},
        ShardedAcceptedDrainRunner.run,
        .{&drain},
    );
    defer {
        if (!drain.finished.isSet()) {
            api.driver.mutex.lockUncancelable(api.driver.io);
            if (api.driver.settled_acceptance_frontier <
                api.driver.accepted_total)
            {
                const first = api.driver.settled_acceptance_frontier + 1;
                const last = if (api.driver.deferred_accepted_settlement) |range|
                    range.first - 1
                else
                    api.driver.accepted_total;
                if (first <= last) {
                    const count = last - first + 1;
                    api.settleAcceptedRangeLocked(first, count);
                    api.driver.async_completed_total += count;
                }
            }
            api.driver.completion_available.broadcast(api.driver.io);
            api.driver.mutex.unlock(api.driver.io);
        }
        drain_thread.join();
    }
    try expectAcceptedDrainBlocked(&api, &drain);

    api.driver.mutex.lockUncancelable(api.driver.io);
    api.settleAcceptedRangeLocked(4, 2);
    api.driver.async_completed_total += 2;
    const suffix_completed = api.driver.completed_total;
    const suffix_frontier = api.driver.settled_acceptance_frontier;
    api.driver.mutex.unlock(api.driver.io);
    try std.testing.expectEqual(@as(u64, 2), suffix_completed);
    try std.testing.expectEqual(@as(u64, 0), suffix_frontier);
    try std.testing.expect(!drain.finished.isSet());

    api.driver.mutex.lockUncancelable(api.driver.io);
    api.settleAcceptedRangeLocked(1, 2);
    api.driver.async_completed_total += 2;
    const partial_frontier = api.driver.settled_acceptance_frontier;
    api.driver.mutex.unlock(api.driver.io);
    try std.testing.expectEqual(@as(u64, 2), partial_frontier);
    try std.testing.expect(!drain.finished.isSet());

    api.driver.mutex.lockUncancelable(api.driver.io);
    api.settleAcceptedRangeLocked(3, 1);
    api.driver.async_completed_total += 1;
    api.driver.mutex.unlock(api.driver.io);

    try waitForTestEvent(&drain.finished, drain.io);
    const drain_result = drain.result orelse return error.MissingDrainResult;
    try std.testing.expectEqual(@as(usize, 5), try drain_result);
    api.driver.mutex.lockUncancelable(api.driver.io);
    const final_state = .{
        api.driver.accepted_total,
        api.driver.settled_acceptance_frontier,
        api.driver.deferred_accepted_settlement,
        api.driver.async_drained_total,
    };
    api.driver.mutex.unlock(api.driver.io);
    try std.testing.expectEqual(final_state[0], final_state[1]);
    try std.testing.expect(final_state[2] == null);
    try std.testing.expectEqual(@as(u64, 5), final_state[3]);
}

test "sharded exchange api mixed routed and split work observes one fifo sequence" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    const ids = [_]u64{
        try api.addUserAsync(.routed, 210_000),
        try api.addUserAsync(.routed, 210_001),
        try api.nopAsync(.split),
        try api.addUserAsync(.routed, 210_002),
    };
    try std.testing.expectEqual(@as(usize, ids.len), try api.drainPending());

    for (ids, 1..) |request_id, expected_sequence| {
        var response = api.takeResult(request_id) orelse return error.MissingAsyncResult;
        defer response.deinit(allocator);
        try std.testing.expectEqual(@as(u64, @intCast(expected_sequence)), response.sequence);
    }
    try std.testing.expectEqual(@as(u64, 4), api.core.kernels.next_seq);
}

test "sharded exchange api persist pair remains adjacent under concurrent publication" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    try api.startup();

    const PersistPublisher = struct {
        api: *ExchangeApiSharded,
        pair: ?PersistStateRequestPair = null,
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            self.pair = self.api.persistStateAsync(.split, 210_100, 9_980_100_001) catch |err| {
                self.failure = err;
                return;
            };
        }
    };
    const RoutedPublisher = struct {
        api: *ExchangeApiSharded,
        request_id: ?u64 = null,
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            self.request_id = self.api.addUserAsync(.routed, 210_101) catch |err| {
                self.failure = err;
                return;
            };
        }
    };

    var persist: PersistPublisher = .{ .api = &api };
    var routed: RoutedPublisher = .{ .api = &api };
    const persist_thread = try std.Thread.spawn(.{}, PersistPublisher.run, .{&persist});
    const routed_thread = try std.Thread.spawn(.{}, RoutedPublisher.run, .{&routed});
    persist_thread.join();
    routed_thread.join();
    try std.testing.expectEqual(@as(?anyerror, null), persist.failure);
    try std.testing.expectEqual(@as(?anyerror, null), routed.failure);

    _ = try api.drainPending();
    const pair = persist.pair orelse return error.MissingAsyncResult;
    var matching = api.takeResult(pair.matching_request_id) orelse return error.MissingAsyncResult;
    defer matching.deinit(allocator);
    var risk_response = api.takeResult(pair.risk_request_id) orelse return error.MissingAsyncResult;
    defer risk_response.deinit(allocator);
    try std.testing.expectEqual(matching.sequence + 1, risk_response.sequence);

    var routed_response = api.takeResult(routed.request_id orelse return error.MissingAsyncResult) orelse
        return error.MissingAsyncResult;
    defer routed_response.deinit(allocator);
    try std.testing.expect(routed_response.sequence < matching.sequence or
        routed_response.sequence > risk_response.sequence);
}

test "sharded generated-order callback uses publication sequence as order id" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    try api.startup();

    // Detached work consumes publication identity without consuming a request
    // ID, proving the two domains are not aliases.
    try api.enqueueRuntime(makeNopCommand());
    var collector: CallbackCollector = .{};
    const sequence = try api.placeNewOrderGeneratedIdWithCallback(
        .routed,
        7,
        100,
        100,
        1,
        .bid,
        .gtc,
        9_999,
        210_200,
        .{
            .context = @ptrCast(&collector),
            .callback = CallbackCollector.onResponse,
        },
    );
    try std.testing.expectEqual(@as(u64, 2), sequence);
    try std.testing.expectEqual(@as(u64, 1), api.next_request_id);
    _ = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 1), collector.count);
    try std.testing.expectEqual(sequence, collector.last_sequence);
    try std.testing.expectEqual(@as(i64, @intCast(sequence)), collector.last_order_id);
    try std.testing.expect(collector.last_timestamp > 0);
}

test "sharded exchange api sync batch progresses with one free completion slot" {
    const allocator = std.testing.allocator;
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 4;
    java_config.performance_cfg.matching_engines_num = 4;
    java_config.performance_cfg.risk_engines_num = 4;
    java_config.performance_cfg.msgs_in_group_limit = 1;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();
    try api.startup();

    var retained_ids: [3]u64 = undefined;
    for (&retained_ids, 0..) |*request_id, index| {
        request_id.* = try api.addUserAsync(.split, 129_200 + @as(i64, @intCast(index)));
    }
    try std.testing.expectEqual(@as(usize, 3), try api.drainPending());
    try std.testing.expectEqual(@as(usize, 3), api.readyCount());

    const commands = [_]ApiCommand{
        .{ .add_user = 129_210 },
        .{ .add_user = 129_211 },
        .{ .add_user = 129_212 },
    };
    var result = try api.submitApiCommandsSyncResult(.split, &commands);
    defer result.deinit(allocator);
    switch (result) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.ExpectedResponseResult,
    }
    try std.testing.expectEqual(@as(usize, 3), api.readyCount());
    try std.testing.expectEqual(@as(usize, 3), api.driver.completion_slots_used);

    for (retained_ids) |request_id| {
        var response = api.takeResult(request_id) orelse return error.MissingAsyncResult;
        response.deinit(allocator);
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

test "sharded exchange api persistent grouped mode is default and immediate stays explicit" {
    const allocator = std.testing.allocator;

    var default_api = try ExchangeApiSharded.init(allocator, 1, .direct);
    defer default_api.deinit();
    try default_api.startup();
    try std.testing.expect(default_api.persistent_grouped_runtime != null);

    var immediate_config = config_mod.JavaExchangeConfig.javaDefault();
    immediate_config.sharded_pipeline_mode = .immediate;
    var immediate_api = try ExchangeApiSharded.initFromJavaConfig(
        allocator,
        immediate_config,
    );
    defer immediate_api.deinit();
    try immediate_api.startup();
    try std.testing.expect(immediate_api.persistent_grouped_runtime == null);

    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 16;
    java_config.performance_cfg.msgs_in_group_limit = 4;
    java_config.performance_cfg.orderbook_kind = .direct;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();
    try std.testing.expect(api.persistent_grouped_runtime == null);
    try api.startup();

    const runtime = api.persistent_grouped_runtime orelse
        return error.MissingPersistentGroupedRuntime;
    try std.testing.expectEqual(@as(usize, 1), runtime.matchingWorkerCount());
    try std.testing.expectEqual(@as(usize, 1), runtime.riskWorkerCount());
    const matching_worker_id = runtime.matchingWorkerId(0) orelse
        return error.MissingPersistentGroupedWorker;
    const risk_worker_id = runtime.riskWorkerId(0) orelse
        return error.MissingPersistentGroupedWorker;

    const outcome = try runBasicFlow(&api, .routed);
    for (outcome.codes) |result_code| {
        try std.testing.expectEqual(cmd.CommandResultCode.success, result_code);
    }
    try std.testing.expectEqual(@as(usize, 0), outcome.ask_levels);
    try std.testing.expectEqual(@as(usize, 0), outcome.bid_levels);

    var reset_response = try api.reset(.split);
    defer reset_response.deinit(api.allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, reset_response.result_code);
    var after_reset = try api.addUser(.routed, 221_001);
    defer after_reset.deinit(api.allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, after_reset.result_code);
    try std.testing.expectEqual(
        matching_worker_id,
        api.persistent_grouped_runtime.?.matchingWorkerId(0).?,
    );
    try std.testing.expectEqual(
        risk_worker_id,
        api.persistent_grouped_runtime.?.riskWorkerId(0).?,
    );
}

test "persistent grouped driver atomically admits one stamped fifo group" {
    const allocator = std.testing.allocator;
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 32;
    java_config.performance_cfg.msgs_in_group_limit = 8;
    java_config.performance_cfg.orderbook_kind = .direct;
    java_config.sharded_pipeline_mode = .persistent_grouped;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();
    try api.startup();

    var commands: [8]cmd.OrderCommand = undefined;
    for (&commands) |*command| command.* = makeNopCommand();
    var request_ids: [commands.len]u64 = undefined;
    try api.enqueueResponseBatch(
        &commands,
        .routed,
        .asynchronous,
        &request_ids,
    );
    try std.testing.expectEqual(commands.len, try api.drainPending());
    try std.testing.expectEqual(commands.len, api.driver.grouped_batch_high_watermark);

    for (request_ids, 1..) |request_id, expected_sequence| {
        var response = api.takeResult(request_id) orelse
            return error.MissingAsyncResult;
        defer response.deinit(allocator);
        try std.testing.expectEqual(
            @as(u64, @intCast(expected_sequence)),
            response.sequence,
        );
        try std.testing.expectEqual(
            cmd.CommandResultCode.success,
            response.result_code,
        );
    }
    try std.testing.expectEqual(@as(usize, 0), api.runtimePendingCount());
    try std.testing.expectEqual(@as(usize, 0), api.readyCount());
}

test "persistent grouped admission never crosses caller supplied event groups" {
    const allocator = std.testing.allocator;
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 32;
    java_config.performance_cfg.msgs_in_group_limit = 8;
    java_config.performance_cfg.orderbook_kind = .direct;
    java_config.sharded_pipeline_mode = .persistent_grouped;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();
    try api.startup();

    var commands: [4]cmd.OrderCommand = undefined;
    for (&commands, 0..) |*command, index| {
        command.* = makeNopCommand();
        command.events_group = if (index < 2) 70 else 71;
    }
    var request_ids: [commands.len]u64 = undefined;
    try api.enqueueResponseBatch(
        &commands,
        .routed,
        .asynchronous_raw,
        &request_ids,
    );
    try std.testing.expectEqual(commands.len, try api.drainPending());
    try std.testing.expectEqual(@as(usize, 2), api.driver.grouped_batch_high_watermark);
    for (request_ids, 1..) |request_id, expected_sequence| {
        var response = api.takeResult(request_id) orelse
            return error.MissingAsyncResult;
        try std.testing.expectEqual(
            @as(u64, @intCast(expected_sequence)),
            response.sequence,
        );
        response.deinit(allocator);
    }
}

test "persistent grouped seeded batch planning preserves every fifo boundary" {
    const seed: u64 = 0x5047_4241_5443_4831;
    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();
    var queue = try ShardedWorkQueue.init(std.testing.allocator, 16);
    defer queue.deinit();
    var model: [16]ShardedWorkItem = undefined;

    for (0..2_000) |step| {
        queue.clear();
        const len = random.uintLessThan(usize, model.len) + 1;
        var group: i64 = @intCast(random.uintLessThan(u32, 7));
        for (model[0..len], 0..) |*item, index| {
            if (index != 0 and random.uintLessThan(u32, 100) < 30) group += 1;
            var command = makeNopCommand();
            if (random.uintLessThan(u32, 100) < 12) command.command = .reset;
            command.events_group = group;
            item.* = .{
                .acceptance_id = @intCast(index + 1),
                .sequence = .fromInt(@intCast(index + 1)),
                .mode = .routed,
                .command = command,
                .completion = .detached,
                .kind = .asynchronous_raw,
            };
            try queue.push(item.*);
        }

        const prepared = random.uintLessThan(usize, len) + 1;
        const max_batch = random.uintLessThan(usize, 8) + 1;
        var model_prefix: usize = 0;
        if (max_batch >= 2 and
            sharded_pipeline.isOrdinaryCommand(model[0].command.command))
        {
            const limit = @min(prepared, max_batch);
            const first_group = model[0].command.events_group;
            while (model_prefix < limit and
                sharded_pipeline.isOrdinaryCommand(
                    model[model_prefix].command.command,
                ) and
                model[model_prefix].command.events_group == first_group)
            {
                model_prefix += 1;
            }
            if (model_prefix < 2) model_prefix = 0;
        }

        const actual = ExchangeApiSharded.persistentGroupedBatchCandidateCount(
            &queue,
            prepared,
            max_batch,
        );
        if (actual != model_prefix) {
            std.debug.print(
                "seeded persistent batch divergence seed=0x{x} step={d} " ++
                    "prepared={d} max={d} expected={d} actual={d}\n",
                .{ seed, step, prepared, max_batch, model_prefix, actual },
            );
            return error.SeededBatchPlannerDiverged;
        }
        try std.testing.expect(actual == 0 or actual >= 2);
        try std.testing.expect(actual <= prepared);
        try std.testing.expect(actual <= max_batch);
        try std.testing.expectEqual(len, queue.count());
        if (actual != 0) {
            const first_group = queue.peekConst().?.command.events_group;
            for (0..actual) |index| {
                const item = queue.atConst(index).?;
                try std.testing.expect(
                    sharded_pipeline.isOrdinaryCommand(item.command.command),
                );
                try std.testing.expectEqual(first_group, item.command.events_group);
            }
        }
    }
}

test "persistent grouped stale-prefix query waits behind a newer admitted batch" {
    const allocator = std.testing.allocator;
    const uid: i64 = 222_075;
    const currency: i32 = 15;
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 16;
    java_config.performance_cfg.msgs_in_group_limit = 4;
    java_config.performance_cfg.orderbook_kind = .direct;
    java_config.sharded_pipeline_mode = .persistent_grouped;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();

    var add_user = try api.addUser(.routed, uid);
    add_user.deinit(api.allocator);
    var initial_balance = try api.adjustUserBalance(
        .routed,
        uid,
        1,
        currency,
        10_000,
        .adjustment,
    );
    initial_balance.deinit(api.allocator);

    const first_batch_sequence = api.core.kernels.next_seq + 1;
    var matching_gate: sharded_pipeline.PersistentGroupedRuntime.StageGate = .{
        .stage = .matching,
        .sequence = first_batch_sequence,
        .shard_index = 0,
    };
    var accepted_prefix_gate: AcceptedPrefixBarrierTestGate = .{};
    api.persistent_grouped_stage_gate = &matching_gate;
    api.accepted_prefix_barrier_test_gate = &accepted_prefix_gate;
    try api.startup();

    const QueryRunner = struct {
        api: *ExchangeApiSharded,
        uid: i64,
        currency: i32,
        done: std.atomic.Value(bool) = .init(false),
        balance: ?i64 = null,
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            self.balance = self.api.queryUserBalance(
                self.uid,
                self.currency,
            ) catch |err| {
                self.failure = err;
                self.done.store(true, .release);
                return;
            };
            self.done.store(true, .release);
        }
    };
    var query: QueryRunner = .{
        .api = &api,
        .uid = uid,
        .currency = currency,
    };
    const query_thread = try std.Thread.spawn(.{}, QueryRunner.run, .{&query});
    var query_joined = false;
    defer if (!query_joined) {
        accepted_prefix_gate.open();
        matching_gate.open();
        query_thread.join();
    };

    try waitForTestAtomicTrue(&accepted_prefix_gate.entered, api.driver.io);
    var commands = [_]cmd.OrderCommand{
        makeBalanceAdjustmentCommand(uid, currency, 1, 2, .adjustment),
        makeBalanceAdjustmentCommand(uid, currency, 1, 3, .adjustment),
    };
    for (&commands) |*command| command.events_group = 700;
    var request_ids: [commands.len]u64 = undefined;
    try api.enqueueResponseBatch(
        &commands,
        .routed,
        .asynchronous_raw,
        &request_ids,
    );
    try waitForTestAtomicTrue(&matching_gate.reached, api.driver.io);
    api.driver.mutex.lockUncancelable(api.driver.io);
    const grouped_high_watermark = api.driver.grouped_batch_high_watermark;
    api.driver.mutex.unlock(api.driver.io);
    try std.testing.expectEqual(commands.len, grouped_high_watermark);

    // The query captured the empty accepted prefix before this batch existed.
    // It must wait on the batch fence until the driver has handed off every
    // admitted result; otherwise terminalQuiesce can wait on work whose driver
    // is blocked behind the query's execution ownership.
    accepted_prefix_gate.open();
    try std.testing.expect(!query.done.load(.acquire));
    matching_gate.open();
    try waitForTestAtomicTrue(&query.done, api.driver.io);
    query_thread.join();
    query_joined = true;

    try std.testing.expectEqual(@as(?anyerror, null), query.failure);
    try std.testing.expectEqual(@as(?i64, 10_002), query.balance);
    try std.testing.expectEqual(commands.len, try api.drainPending());
    for (request_ids) |request_id| {
        var response = api.takeResult(request_id) orelse
            return error.MissingAsyncResult;
        try std.testing.expectEqual(
            cmd.CommandResultCode.success,
            response.result_code,
        );
        response.deinit(api.allocator);
    }
    try std.testing.expect(api.fatalInfo() == null);
}

test "persistent grouped raw long group refills a reclaimed small ring" {
    const allocator = std.testing.allocator;
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 4;
    java_config.performance_cfg.msgs_in_group_limit = 1;
    java_config.performance_cfg.orderbook_kind = .direct;
    java_config.sharded_pipeline_mode = .persistent_grouped;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();
    var capacity_gate: PersistentCapacityTestGate = .{};
    api.persistent_capacity_test_gate = &capacity_gate;
    try api.startup();

    var first_commands: [4]cmd.OrderCommand = undefined;
    for (&first_commands) |*command| {
        command.* = makeNopCommand();
        command.events_group = 800;
    }
    var first_request_ids: [first_commands.len]u64 = undefined;
    try api.enqueueResponseBatch(
        &first_commands,
        .routed,
        .asynchronous_raw,
        &first_request_ids,
    );
    try waitForTestResultsReady(&api, &first_request_ids);
    for (first_request_ids) |request_id| {
        var response = api.takeResult(request_id) orelse
            return error.MissingAsyncResult;
        try std.testing.expectEqual(
            cmd.CommandResultCode.success,
            response.result_code,
        );
        response.deinit(api.allocator);
    }

    // All four inner slots still belong to the open raw group even though its
    // outer responses have settled. The next same-group range must close and
    // reclaim that healthy boundary, then retry without poisoning the API.
    var second_commands: [2]cmd.OrderCommand = undefined;
    for (&second_commands) |*command| {
        command.* = makeNopCommand();
        command.events_group = 800;
    }
    var second_request_ids: [second_commands.len]u64 = undefined;
    try api.enqueueResponseBatch(
        &second_commands,
        .routed,
        .asynchronous_raw,
        &second_request_ids,
    );
    try waitForTestAtomicTrue(&capacity_gate.reached, api.driver.io);
    try std.testing.expectEqual(
        first_commands.len + second_commands.len,
        try api.drainPending(),
    );
    for (second_request_ids) |request_id| {
        var response = api.takeResult(request_id) orelse
            return error.MissingAsyncResult;
        try std.testing.expectEqual(
            cmd.CommandResultCode.success,
            response.result_code,
        );
        response.deinit(api.allocator);
    }
    try std.testing.expect(api.fatalInfo() == null);
    try std.testing.expectEqual(
        @as(u64, first_commands.len + second_commands.len),
        api.persistent_grouped_runtime.?.reclaimedFrontier(),
    );
}

test "persistent grouped suffix poison preserves the inner fatal command" {
    const allocator = std.testing.allocator;
    const uid: i64 = 222_076;
    const currency: i32 = 15;
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 16;
    java_config.performance_cfg.msgs_in_group_limit = 4;
    java_config.performance_cfg.orderbook_kind = .direct;
    java_config.sharded_pipeline_mode = .persistent_grouped;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();

    var add_user = try api.addUser(.routed, uid);
    add_user.deinit(api.allocator);
    var initial_balance = try api.adjustUserBalance(
        .routed,
        uid,
        1,
        currency,
        10_000,
        .adjustment,
    );
    initial_balance.deinit(api.allocator);

    const failing_sequence = api.core.kernels.next_seq + 1;
    var r2_gate: sharded_pipeline.PersistentGroupedRuntime.StageGate = .{
        .stage = .risk_release,
        .sequence = failing_sequence,
        .shard_index = api.core.shardIndexForUid(uid),
        .fail = true,
    };
    defer r2_gate.open();
    api.persistent_grouped_stage_gate = &r2_gate;
    try api.startup();

    var failing_command = makeBalanceAdjustmentCommand(
        uid,
        currency,
        1,
        2,
        .adjustment,
    );
    failing_command.events_group = 900;
    var suffix_command = makeNopCommand();
    suffix_command.events_group = 901;
    var request_ids: [2]u64 = undefined;
    try api.enqueueResponseBatch(
        &.{ failing_command, suffix_command },
        .routed,
        .asynchronous_raw,
        &request_ids,
    );
    try waitForTestAtomicTrue(&r2_gate.reached, api.driver.io);
    r2_gate.open();
    try std.testing.expectError(error.CorePoisoned, api.drainPending());

    const fatal = api.fatalInfo() orelse return error.MissingFatalRecord;
    try std.testing.expectEqual(runtime_mod.FailureStage.r2, fatal.stage);
    try std.testing.expectEqual(@as(?u64, failing_sequence), fatal.sequence);
    try std.testing.expectEqual(
        @as(?u64, request_ids[0]),
        fatal.request_id,
    );
    try std.testing.expectEqual(
        @as(?cmd.OrderCommandType, .balance_adjustment),
        fatal.command,
    );
    try std.testing.expectEqual(error.InjectedStageFailure, fatal.cause);
}

test "persistent grouped batch publishes strict prefix before later matching fatal" {
    const allocator = std.testing.allocator;
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 32;
    java_config.performance_cfg.msgs_in_group_limit = 8;
    java_config.performance_cfg.orderbook_kind = .direct;
    java_config.sharded_pipeline_mode = .persistent_grouped;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();
    var gate: sharded_pipeline.PersistentGroupedRuntime.StageGate = .{
        .stage = .matching,
        .sequence = 2,
        .shard_index = 0,
        .fail = true,
    };
    api.persistent_grouped_stage_gate = &gate;
    try api.startup();

    var commands: [3]cmd.OrderCommand = undefined;
    for (&commands) |*command| command.* = makeNopCommand();
    var request_ids: [commands.len]u64 = undefined;
    try api.enqueueResponseBatch(
        &commands,
        .routed,
        .asynchronous,
        &request_ids,
    );
    gate.waitUntilReached();
    try std.testing.expectEqual(commands.len, api.driver.grouped_batch_high_watermark);

    var probe: FatalCallbackProbe = .{};
    const callback: CommandResponseCallback = .{
        .context = &probe,
        .callback = FatalCallbackProbe.onResponse,
        .failure_callback = FatalCallbackProbe.onFailure,
    };
    try api.registerResultCallback(.{ .request_id = request_ids[1] }, callback);
    try api.registerResultCallback(.{ .request_id = request_ids[2] }, callback);
    gate.open();

    try std.testing.expectError(error.CorePoisoned, api.drainPending());
    var prefix = api.takeResult(request_ids[0]) orelse
        return error.MissingAsyncResult;
    defer prefix.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 1), prefix.sequence);
    try std.testing.expectEqual(cmd.CommandResultCode.success, prefix.result_code);
    try std.testing.expectEqual(@as(usize, 0), probe.response_count);
    try std.testing.expectEqual(@as(usize, 2), probe.failure_count);
    try std.testing.expect(probe.all_failure_callbacks_scoped);
    try std.testing.expectEqual(request_ids[1], probe.request_ids[0]);
    try std.testing.expectEqual(request_ids[2], probe.request_ids[1]);
    try std.testing.expectEqual(
        CallbackFailureKind.command_failed,
        probe.kinds[0],
    );
    try std.testing.expectEqual(
        CallbackFailureKind.skipped_after_fatal,
        probe.kinds[1],
    );
    const fatal = api.fatalInfo() orelse return error.MissingFatalRecord;
    try std.testing.expectEqual(@as(?u64, 2), fatal.sequence);
    try std.testing.expectEqual(@as(?u64, request_ids[1]), fatal.request_id);
    try std.testing.expectEqual(
        api.driver.accepted_total,
        api.driver.settled_acceptance_frontier,
    );
    try std.testing.expectEqual(@as(usize, 0), api.runtimePendingCount());
}

test "persistent grouped sharded api reuses exact asymmetric worker fleets" {
    const allocator = std.testing.allocator;
    const topologies = [_]struct { matching: usize, risk: usize }{
        .{ .matching = 4, .risk = 2 },
        .{ .matching = 1, .risk = 4 },
    };

    for (topologies) |topology| {
        var java_config = config_mod.JavaExchangeConfig.javaDefault();
        java_config.performance_cfg.ring_buffer_size = 16;
        java_config.performance_cfg.msgs_in_group_limit = 4;
        java_config.performance_cfg.matching_engines_num = topology.matching;
        java_config.performance_cfg.risk_engines_num = topology.risk;
        java_config.performance_cfg.orderbook_kind = .direct;
        java_config.sharded_pipeline_mode = .persistent_grouped;
        var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
        defer api.deinit();
        try api.startup();

        const runtime = api.persistent_grouped_runtime orelse
            return error.MissingPersistentGroupedRuntime;
        try std.testing.expectEqual(topology.matching, runtime.matchingWorkerCount());
        try std.testing.expectEqual(topology.risk, runtime.riskWorkerCount());
        var matching_ids: [4]?std.Thread.Id = @splat(null);
        var risk_ids: [4]?std.Thread.Id = @splat(null);
        for (0..topology.matching) |index| {
            matching_ids[index] = runtime.matchingWorkerId(index);
            try std.testing.expect(matching_ids[index] != null);
        }
        for (0..topology.risk) |index| {
            risk_ids[index] = runtime.riskWorkerId(index);
            try std.testing.expect(risk_ids[index] != null);
        }

        const outcome = try runBasicFlow(&api, .split);
        for (outcome.codes) |result_code| {
            try std.testing.expectEqual(cmd.CommandResultCode.success, result_code);
        }
        var reset_response = try api.reset(.routed);
        reset_response.deinit(api.allocator);
        var after_global = try api.addUser(.split, 221_100);
        after_global.deinit(api.allocator);

        for (0..topology.matching) |index| {
            try std.testing.expectEqual(
                matching_ids[index],
                api.persistent_grouped_runtime.?.matchingWorkerId(index),
            );
        }
        for (0..topology.risk) |index| {
            try std.testing.expectEqual(
                risk_ids[index],
                api.persistent_grouped_runtime.?.riskWorkerId(index),
            );
        }
    }
}

test "persistent grouped delivery precedes r2 and settled reads close the terminal group" {
    const allocator = std.testing.allocator;
    const symbol: i32 = 222_100;
    const maker_uid: i64 = 222_101;
    const taker_uid: i64 = 222_102;

    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 16;
    java_config.performance_cfg.msgs_in_group_limit = 4;
    java_config.performance_cfg.orderbook_kind = .direct;
    java_config.sharded_pipeline_mode = .persistent_grouped;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();

    const spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 100,
        .quote_scale_k = 10,
        .taker_fee = 2,
        .maker_fee = 1,
    });
    try api.addSymbolAll(spec);
    var add_maker = try api.addUser(.routed, maker_uid);
    add_maker.deinit(api.allocator);
    var add_taker = try api.addUser(.routed, taker_uid);
    add_taker.deinit(api.allocator);
    var fund_maker = try api.adjustUserBalance(.routed, maker_uid, 1, 11, 100_000, .adjustment);
    fund_maker.deinit(api.allocator);
    var fund_taker = try api.adjustUserBalance(.routed, taker_uid, 1, 15, 100_000, .adjustment);
    fund_taker.deinit(api.allocator);
    var maker_ask = try api.placeOrder(
        .routed,
        222_110,
        maker_uid,
        symbol,
        100,
        0,
        4,
        .ask,
        .gtc,
        0,
    );
    maker_ask.deinit(api.allocator);

    const trade_sequence = api.core.kernels.next_seq + 1;
    var r2_gate: sharded_pipeline.PersistentGroupedRuntime.StageGate = .{
        .stage = .risk_release,
        .sequence = trade_sequence,
        .shard_index = 0,
    };
    var raw_probe: RawR2Probe = .{};
    api.setRawResultSink(.{
        .context = &raw_probe,
        .on_result = RawR2Probe.onResult,
    });
    api.persistent_grouped_stage_gate = &r2_gate;
    try api.startup();

    var trade = makePlaceOrderCommand(
        222_111,
        taker_uid,
        symbol,
        100,
        100,
        1,
        .bid,
        .ioc,
        0,
    );
    trade.events_group = 100;
    var response = try api.executeRawApiCommandWithMode(.routed, &trade);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    try std.testing.expectEqual(trade_sequence, response.sequence);

    const SettledQueryRunner = struct {
        api: *ExchangeApiSharded,
        uid: i64,
        currency: i32,
        entered: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),
        balance: ?i64 = null,
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            self.entered.store(true, .release);
            self.balance = self.api.queryUserBalance(self.uid, self.currency) catch |err| {
                self.failure = err;
                self.done.store(true, .release);
                return;
            };
            self.done.store(true, .release);
        }
    };
    var query_runner: SettledQueryRunner = .{
        .api = &api,
        .uid = maker_uid,
        .currency = 15,
    };
    const query_thread = try std.Thread.spawn(.{}, SettledQueryRunner.run, .{&query_runner});
    while (!query_runner.entered.load(.acquire)) std.Thread.yield() catch {};
    try waitForTestAtomicTrue(&r2_gate.reached, api.driver.io);
    // `takeNextResult` can transfer only a journal-acknowledged response. The
    // raw callback has therefore completed after that ack while this closed
    // StageGate proves final R2 still cannot retire the same sequence.
    try std.testing.expectEqual(@as(usize, 1), raw_probe.count);
    try std.testing.expectEqual(trade_sequence, raw_probe.sequence);
    try std.testing.expect(!r2_gate.release.load(.acquire));
    try std.testing.expect(!query_runner.done.load(.acquire));

    // The caller can release the matching response while the non-aliased R2
    // payload remains owned by the grouped runtime.
    api.releaseResponse(&response);
    r2_gate.open();
    query_thread.join();
    api.clearRawResultSink();
    try std.testing.expectEqual(@as(?anyerror, null), query_runner.failure);
    try std.testing.expectEqual(@as(?i64, 999), query_runner.balance);
    try std.testing.expectEqual(
        trade_sequence,
        api.persistent_grouped_runtime.?.reclaimedFrontier(),
    );

    var second_trade = makePlaceOrderCommand(
        222_112,
        taker_uid,
        symbol,
        100,
        100,
        1,
        .bid,
        .ioc,
        0,
    );
    second_trade.events_group = 200;
    var same_group_withdrawal = makeBalanceAdjustmentCommand(
        maker_uid,
        15,
        -1_500,
        2,
        .adjustment,
    );
    same_group_withdrawal.events_group = 200;
    var next_group_withdrawal = makeBalanceAdjustmentCommand(
        maker_uid,
        15,
        -1_500,
        3,
        .adjustment,
    );
    next_group_withdrawal.events_group = 201;
    var request_ids: [3]u64 = undefined;
    try api.enqueueResponseBatch(
        &.{ second_trade, same_group_withdrawal, next_group_withdrawal },
        .routed,
        .asynchronous_raw,
        request_ids[0..],
    );
    try std.testing.expectEqual(@as(usize, 3), try api.drainPending());
    var second_trade_response = api.takeResult(request_ids[0]) orelse
        return error.MissingAsyncResult;
    defer second_trade_response.deinit(api.allocator);
    var same_group_response = api.takeResult(request_ids[1]) orelse
        return error.MissingAsyncResult;
    defer same_group_response.deinit(api.allocator);
    var next_group_response = api.takeResult(request_ids[2]) orelse
        return error.MissingAsyncResult;
    defer next_group_response.deinit(api.allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, second_trade_response.result_code);
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_account_balance_adjustment_nsf,
        same_group_response.result_code,
    );
    try std.testing.expectEqual(cmd.CommandResultCode.success, next_group_response.result_code);
}

test "persistent grouped drain waits for terminal r2 and fails registered poll owners" {
    const allocator = std.testing.allocator;
    const uid: i64 = 222_151;
    const currency: i32 = 15;
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 16;
    java_config.performance_cfg.msgs_in_group_limit = 4;
    java_config.performance_cfg.orderbook_kind = .direct;
    java_config.sharded_pipeline_mode = .persistent_grouped;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();

    var add_user = try api.addUser(.routed, uid);
    add_user.deinit(api.allocator);
    var initial_balance = try api.adjustUserBalance(
        .routed,
        uid,
        1,
        currency,
        10_000,
        .adjustment,
    );
    initial_balance.deinit(api.allocator);

    const accepted_before = api.driver.accepted_total;
    const async_drained_before = api.driver.async_drained_total;
    const failing_sequence = api.core.kernels.next_seq + 1;
    var r2_gate: sharded_pipeline.PersistentGroupedRuntime.StageGate = .{
        .stage = .risk_release,
        .sequence = failing_sequence,
        .shard_index = api.core.shardIndexForUid(uid),
        .fail = true,
    };
    defer r2_gate.open();
    var poison_gate: GroupedPoisonReacquireTestGate = .{};
    defer poison_gate.open();
    api.persistent_grouped_stage_gate = &r2_gate;
    api.grouped_poison_reacquire_test_gate = &poison_gate;
    try api.startup();

    const failed_handle = try api.submitBalanceAdjustmentAsync(
        .routed,
        uid,
        2,
        currency,
        1,
        .adjustment,
    );
    const skipped_handle = try api.submitCreateUserAsync(.split, uid + 1);
    const failed_request_id = try requestIdFromHandle(failed_handle);
    const skipped_request_id = try requestIdFromHandle(skipped_handle);
    var registered_probe: FatalCallbackProbe = .{};

    var drain: ShardedAcceptedDrainRunner = .{
        .api = &api,
        .io = api.driver.io,
    };
    const drain_thread = try std.Thread.spawn(
        .{},
        ShardedAcceptedDrainRunner.run,
        .{&drain},
    );
    defer {
        r2_gate.open();
        poison_gate.open();
        drain_thread.join();
    }

    try waitForAcceptedSettlementFrontier(&api, accepted_before + 2);
    try waitForTestAtomicTrue(&r2_gate.reached, api.driver.io);
    try std.testing.expect(!drain.finished.isSet());
    try std.testing.expect(api.isResultReady(failed_request_id));
    try std.testing.expect(api.isResultReady(skipped_request_id));
    try std.testing.expectEqual(
        async_drained_before,
        api.driver.async_drained_total,
    );

    r2_gate.open();
    try waitForTestAtomicTrue(&poison_gate.reached, api.driver.io);
    const published_fatal = api.fatalInfo() orelse
        return error.MissingFatalRecord;
    try std.testing.expectEqual(
        @as(?u64, failed_request_id),
        published_fatal.request_id,
    );

    // Register after poison publication but before the drain's fatal sweep.
    // The retained response still supplies sequence identity on this path.
    try api.registerResultCallback(failed_handle, .{
        .context = &registered_probe,
        .callback = FatalCallbackProbe.onResponse,
        .failure_callback = FatalCallbackProbe.onFailure,
    });
    try std.testing.expectEqual(@as(usize, 0), registered_probe.response_count);
    try std.testing.expectEqual(@as(usize, 1), registered_probe.failure_count);
    try std.testing.expectEqual(
        CallbackFailureKind.command_failed,
        registered_probe.kinds[0],
    );
    try std.testing.expectEqual(
        failed_request_id,
        registered_probe.request_ids[0],
    );
    try std.testing.expect(!api.isResultReady(failed_request_id));
    try std.testing.expect(api.isResultReady(skipped_request_id));

    poison_gate.open();
    try waitForTestEvent(&drain.finished, drain.io);
    const drain_result = drain.result orelse return error.MissingDrainResult;
    try std.testing.expectError(error.CorePoisoned, drain_result);
    try std.testing.expectEqual(@as(usize, 0), registered_probe.response_count);
    try std.testing.expectEqual(@as(usize, 1), registered_probe.failure_count);
    try std.testing.expect(registered_probe.all_failure_callbacks_scoped);
    try std.testing.expectEqual(
        CallbackFailureKind.command_failed,
        registered_probe.kinds[0],
    );
    try std.testing.expectEqual(
        error.InjectedStageFailure,
        registered_probe.causes[0],
    );
    try std.testing.expect(!api.isResultReady(failed_request_id));
    try std.testing.expect(!api.isResultReady(skipped_request_id));
    try std.testing.expectEqual(@as(usize, 0), api.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, 0), api.driver.completion_slots_used);
    try std.testing.expectEqual(
        async_drained_before,
        api.driver.async_drained_total,
    );

    // The fatal sweep has removed both ready responses. Re-registering the
    // exact failed handle must retain the same classification as the
    // pre-cleanup path; only the true suffix is skipped.
    var late_failed_probe: FatalCallbackProbe = .{};
    try api.registerResultCallback(failed_handle, .{
        .context = &late_failed_probe,
        .callback = FatalCallbackProbe.onResponse,
        .failure_callback = FatalCallbackProbe.onFailure,
    });
    try std.testing.expectEqual(@as(usize, 0), late_failed_probe.response_count);
    try std.testing.expectEqual(@as(usize, 1), late_failed_probe.failure_count);
    try std.testing.expect(late_failed_probe.all_failure_callbacks_scoped);
    try std.testing.expectEqual(
        CallbackFailureKind.command_failed,
        late_failed_probe.kinds[0],
    );
    try std.testing.expectEqual(
        failed_request_id,
        late_failed_probe.request_ids[0],
    );
    try std.testing.expectEqual(
        @as(?u64, failed_request_id),
        late_failed_probe.fatal_request_ids[0],
    );

    var late_skipped_probe: FatalCallbackProbe = .{};
    try api.registerResultCallback(skipped_handle, .{
        .context = &late_skipped_probe,
        .callback = FatalCallbackProbe.onResponse,
        .failure_callback = FatalCallbackProbe.onFailure,
    });
    try std.testing.expectEqual(@as(usize, 0), late_skipped_probe.response_count);
    try std.testing.expectEqual(@as(usize, 1), late_skipped_probe.failure_count);
    try std.testing.expect(late_skipped_probe.all_failure_callbacks_scoped);
    try std.testing.expectEqual(
        CallbackFailureKind.skipped_after_fatal,
        late_skipped_probe.kinds[0],
    );
    try std.testing.expectEqual(
        skipped_request_id,
        late_skipped_probe.request_ids[0],
    );
    try std.testing.expectEqual(
        @as(?u64, failed_request_id),
        late_skipped_probe.fatal_request_ids[0],
    );
    try std.testing.expectEqual(
        error.InjectedStageFailure,
        late_skipped_probe.causes[0],
    );
    try std.testing.expectEqual(@as(usize, 0), api.pendingCallbackCount());
}

test "persistent grouped callback can reenter journaling enqueue outside all execution locks" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 16;
    java_config.performance_cfg.msgs_in_group_limit = 4;
    java_config.performance_cfg.orderbook_kind = .direct;
    java_config.sharded_pipeline_mode = .persistent_grouped;
    java_config.serialization_cfg = config_mod.SerializationConfig.diskJournaling();
    var api = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        java_config,
    );
    defer api.deinit();
    var raw_probe: RawJournalProbe = .{ .api = &api };
    api.setRawResultSink(.{
        .context = &raw_probe,
        .on_result = RawJournalProbe.onResult,
    });
    try api.startup();

    const ReentryProbe = struct {
        api: *ExchangeApiSharded,
        callback_count: usize = 0,
        callback_result: cmd.CommandResultCode = .new,
        journal_len_at_callback: usize = 0,
        reentrant_request_id: ?u64 = null,
        failure: ?anyerror = null,

        fn onResponse(
            context: ?*anyopaque,
            response: *const kernel_mod.CommandResponse,
        ) void {
            const self: *@This() = @ptrCast(@alignCast(context.?));
            self.callback_count += 1;
            self.callback_result = response.result_code;
            self.journal_len_at_callback = self.api.journalLen();
            self.reentrant_request_id = self.api.addUserAsync(
                .routed,
                222_201,
            ) catch |err| {
                self.failure = err;
                return;
            };
        }
    };
    var probe: ReentryProbe = .{ .api = &api };
    try api.createUserWithCallback(
        .routed,
        222_200,
        .{
            .context = &probe,
            .callback = ReentryProbe.onResponse,
        },
    );
    _ = try api.drainPending();
    _ = try api.drainPending();

    try std.testing.expectEqual(@as(usize, 1), probe.callback_count);
    try std.testing.expectEqual(cmd.CommandResultCode.success, probe.callback_result);
    try std.testing.expectEqual(@as(usize, 1), probe.journal_len_at_callback);
    try std.testing.expectEqual(@as(?anyerror, null), probe.failure);
    var reentrant_response = api.takeResult(probe.reentrant_request_id orelse
        return error.MissingAsyncResult) orelse return error.MissingAsyncResult;
    defer reentrant_response.deinit(api.allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, reentrant_response.result_code);
    try std.testing.expectEqual(@as(usize, 2), api.journalLen());
    try std.testing.expectEqual(@as(usize, 2), raw_probe.count);
    try std.testing.expect(raw_probe.durable_visible);
}

test "persistent grouped callback initiated shutdown owns complete driver finalization" {
    const allocator = std.testing.allocator;
    const symbol: i32 = 222_250;
    const maker_uid: i64 = 222_251;
    const taker_uid: i64 = 222_252;
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 16;
    java_config.performance_cfg.msgs_in_group_limit = 4;
    java_config.performance_cfg.orderbook_kind = .direct;
    java_config.sharded_pipeline_mode = .persistent_grouped;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();

    const spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 100,
        .quote_scale_k = 10,
        .taker_fee = 2,
        .maker_fee = 1,
    });
    try api.addSymbolAll(spec);
    var add_maker = try api.addUser(.routed, maker_uid);
    add_maker.deinit(api.allocator);
    var add_taker = try api.addUser(.routed, taker_uid);
    add_taker.deinit(api.allocator);
    var fund_maker = try api.adjustUserBalance(
        .routed,
        maker_uid,
        1,
        11,
        100_000,
        .adjustment,
    );
    fund_maker.deinit(api.allocator);
    var fund_taker = try api.adjustUserBalance(
        .routed,
        taker_uid,
        1,
        15,
        100_000,
        .adjustment,
    );
    fund_taker.deinit(api.allocator);
    var maker_ask = try api.placeOrder(
        .routed,
        222_260,
        maker_uid,
        symbol,
        100,
        0,
        4,
        .ask,
        .gtc,
        0,
    );
    maker_ask.deinit(api.allocator);

    try api.startup();
    var probe: RunningCallbackShutdownProbe = .{ .api = &api };
    api.setSimpleEventsHandler(.{
        .context = &probe,
        .on_command_result = RunningCallbackShutdownProbe.onCommandResult,
    });
    var trade_response = try api.placeOrder(
        .routed,
        222_261,
        taker_uid,
        symbol,
        100,
        100,
        1,
        .bid,
        .ioc,
        0,
    );
    try std.testing.expectEqual(cmd.CommandResultCode.success, trade_response.result_code);
    api.releaseResponse(&trade_response);

    // No external shutdown call completes this transition: the callback only
    // requests close, and the driver that returns from it owns final R2 and
    // teardown before publishing `.stopped`.
    api.driver.mutex.lockUncancelable(api.driver.io);
    while (api.driver.lifecycle.current() != .stopped and
        api.driver.lifecycle.current() != .poisoned)
    {
        api.driver.completion_available.waitUncancelable(
            api.driver.io,
            &api.driver.mutex,
        );
    }
    const lifecycle = api.driver.lifecycle.current();
    const accepted_total = api.driver.accepted_total;
    const completed_total = api.driver.completed_total;
    const async_accepted_total = api.driver.async_accepted_total;
    const async_completed_total = api.driver.async_completed_total;
    const journal_slots_reserved = api.driver.journal_slots_reserved;
    const completion_slots_used = api.driver.completion_slots_used;
    api.driver.mutex.unlock(api.driver.io);

    try std.testing.expectEqual(runtime_mod.LifecycleState.stopped, lifecycle);
    try std.testing.expectEqual(@as(usize, 1), probe.callback_count.load(.acquire));
    try std.testing.expectEqual(@as(?anyerror, null), probe.shutdown_error);
    try std.testing.expectEqual(
        @as(?ShutdownOutcome, .deferred),
        probe.shutdown_outcome,
    );
    try std.testing.expect(api.persistent_grouped_runtime == null);
    try std.testing.expect(api.core.isStopped());
    try std.testing.expectEqual(accepted_total, completed_total);
    try std.testing.expectEqual(async_accepted_total, async_completed_total);
    try std.testing.expectEqual(@as(usize, 0), journal_slots_reserved);
    try std.testing.expectEqual(@as(usize, 0), completion_slots_used);
    try std.testing.expectEqual(
        @as(?i64, 999),
        try api.core.queryUserBalance(maker_uid, 15),
    );
    try std.testing.expectEqual(ShutdownState.closing, api.shutdownState());
    try std.testing.expectEqual(
        ShutdownOutcome.complete,
        try api.shutdownTimeout(.none),
    );
    try std.testing.expectEqual(ShutdownState.complete, api.shutdownState());
}

test "persistent grouped shutdown drains accepted fifo and destroys workers before core" {
    const allocator = std.testing.allocator;
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 16;
    java_config.performance_cfg.msgs_in_group_limit = 4;
    java_config.performance_cfg.orderbook_kind = .direct;
    java_config.sharded_pipeline_mode = .persistent_grouped;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();
    try api.startup();

    const request_ids = [_]u64{
        try api.addUserAsync(.routed, 222_300),
        try api.addUserAsync(.split, 222_301),
        try api.resetAsync(.routed),
        try api.addUserAsync(.split, 222_300),
    };
    api.shutdown();
    try std.testing.expect(api.isStopped());
    try std.testing.expect(api.persistent_grouped_runtime == null);

    var previous_sequence: u64 = 0;
    for (request_ids) |request_id| {
        var response = api.takeResult(request_id) orelse return error.MissingAsyncResult;
        defer response.deinit(api.allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
        if (previous_sequence != 0) {
            try std.testing.expectEqual(previous_sequence + 1, response.sequence);
        }
        previous_sequence = response.sequence;
    }
}

test "persistent grouped final r2 failure during shutdown preserves inner fatal" {
    const allocator = std.testing.allocator;
    const uid: i64 = 222_350;
    const currency: i32 = 15;
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 16;
    java_config.performance_cfg.msgs_in_group_limit = 4;
    java_config.performance_cfg.orderbook_kind = .direct;
    java_config.sharded_pipeline_mode = .persistent_grouped;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();

    var add_user = try api.addUser(.routed, uid);
    add_user.deinit(api.allocator);
    var initial_balance = try api.adjustUserBalance(
        .routed,
        uid,
        1,
        currency,
        10_000,
        .adjustment,
    );
    initial_balance.deinit(api.allocator);

    const failing_sequence = api.core.kernels.next_seq + 1;
    var r2_gate: sharded_pipeline.PersistentGroupedRuntime.StageGate = .{
        .stage = .risk_release,
        .sequence = failing_sequence,
        .shard_index = 0,
        .fail = true,
    };
    api.persistent_grouped_stage_gate = &r2_gate;
    try api.startup();

    var final_response = try api.adjustUserBalance(
        .routed,
        uid,
        2,
        currency,
        1,
        .adjustment,
    );
    try std.testing.expectEqual(failing_sequence, final_response.sequence);
    try std.testing.expectEqual(cmd.CommandResultCode.success, final_response.result_code);
    api.releaseResponse(&final_response);

    try std.testing.expectError(
        error.Timeout,
        api.shutdownTimeout(immediateShutdownTimeout()),
    );
    try std.testing.expectEqual(ShutdownState.closing, api.shutdownState());
    try std.testing.expect(api.persistent_grouped_runtime != null);
    try std.testing.expectError(
        error.CoreStopped,
        api.addUserAsync(.split, uid + 1),
    );

    var shutdown_runner: ShardedShutdownRunner = .{
        .api = &api,
        .io = std.testing.io,
    };
    const shutdown_thread = try std.Thread.spawn(
        .{},
        ShardedShutdownRunner.run,
        .{&shutdown_runner},
    );
    shutdown_runner.waitEntered();
    try waitForTestAtomicTrue(&r2_gate.reached, api.driver.io);
    try std.testing.expect(!shutdown_runner.isDone());
    r2_gate.open();
    shutdown_thread.join();

    try std.testing.expect(shutdown_runner.isDone());
    try std.testing.expectEqual(ShutdownState.complete, api.shutdownState());
    try std.testing.expect(api.persistent_grouped_runtime == null);
    try std.testing.expect(api.driver.worker_id == null);
    try std.testing.expectEqual(runtime_mod.LifecycleState.poisoned, api.driver.lifecycle.current());
    const fatal = api.fatalInfo() orelse return error.MissingFatalRecord;
    try std.testing.expectEqual(runtime_mod.FailureStage.r2, fatal.stage);
    try std.testing.expectEqual(@as(?u64, failing_sequence), fatal.sequence);
    try std.testing.expectEqual(
        @as(?cmd.OrderCommandType, .balance_adjustment),
        fatal.command,
    );
    try std.testing.expectEqual(error.InjectedStageFailure, fatal.cause);
}

test "persistent grouped external join cannot outrun published poison teardown" {
    const allocator = std.testing.allocator;
    const uid: i64 = 222_365;
    const currency: i32 = 15;
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 16;
    java_config.performance_cfg.msgs_in_group_limit = 4;
    java_config.performance_cfg.orderbook_kind = .direct;
    java_config.sharded_pipeline_mode = .persistent_grouped;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();

    var add_user = try api.addUser(.routed, uid);
    add_user.deinit(api.allocator);
    var initial_balance = try api.adjustUserBalance(
        .routed,
        uid,
        1,
        currency,
        10_000,
        .adjustment,
    );
    initial_balance.deinit(api.allocator);

    const failing_sequence = api.core.kernels.next_seq + 1;
    var r2_gate: sharded_pipeline.PersistentGroupedRuntime.StageGate = .{
        .stage = .risk_release,
        .sequence = failing_sequence,
        .shard_index = 0,
        .fail = true,
    };
    var poison_gate: GroupedPoisonReacquireTestGate = .{};
    defer poison_gate.open();
    api.persistent_grouped_stage_gate = &r2_gate;
    api.grouped_poison_reacquire_test_gate = &poison_gate;
    try api.startup();

    var grouped_response = try api.adjustUserBalance(
        .routed,
        uid,
        2,
        currency,
        1,
        .adjustment,
    );
    try std.testing.expectEqual(failing_sequence, grouped_response.sequence);
    api.releaseResponse(&grouped_response);

    const QueryRunner = struct {
        api: *ExchangeApiSharded,
        uid: i64,
        currency: i32,
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = self.api.queryUserBalance(self.uid, self.currency) catch |err| {
                self.failure = err;
                return;
            };
        }
    };
    var query_runner: QueryRunner = .{
        .api = &api,
        .uid = uid,
        .currency = currency,
    };
    const query_thread = try std.Thread.spawn(.{}, QueryRunner.run, .{&query_runner});
    try waitForTestAtomicTrue(&r2_gate.reached, api.driver.io);
    r2_gate.open();
    try waitForTestAtomicTrue(&poison_gate.reached, api.driver.io);

    // The query has published the outer poison and yielded execution before
    // reacquiring it. Hold execution so final backend cleanup cannot complete:
    // the concurrent shutdown caller must leave the worker handle unclaimed
    // until the outer worker publishes quiescence.
    api.execution_mutex.lockUncancelable(api.driver.io);
    var test_execution_held = true;
    defer if (test_execution_held) {
        api.execution_mutex.unlock(api.driver.io);
    };
    var shutdown_runner: ShardedShutdownRunner = .{
        .api = &api,
        .io = std.testing.io,
    };
    const shutdown_thread = try std.Thread.spawn(
        .{},
        ShardedShutdownRunner.run,
        .{&shutdown_runner},
    );
    shutdown_runner.waitEntered();
    api.driver.mutex.lockUncancelable(api.driver.io);
    try std.testing.expect(api.driver.worker != null);
    try std.testing.expect(!api.driver.join_claimed);
    try std.testing.expect(!api.driver.worker_quiesced.isSet());
    try std.testing.expect(api.driver.worker_id != null);
    api.driver.mutex.unlock(api.driver.io);
    try std.testing.expect(!shutdown_runner.isDone());

    poison_gate.open();
    api.execution_mutex.unlock(api.driver.io);
    test_execution_held = false;
    query_thread.join();
    shutdown_thread.join();

    try std.testing.expectEqual(error.CorePoisoned, query_runner.failure.?);
    try std.testing.expect(shutdown_runner.isDone());
    try std.testing.expect(api.persistent_grouped_runtime == null);
    try std.testing.expect(api.core.isStopped());
    try std.testing.expect(api.driver.worker_id == null);
    try std.testing.expectEqual(runtime_mod.LifecycleState.poisoned, api.driver.lifecycle.current());
    try std.testing.expectEqual(api.driver.accepted_total, api.driver.completed_total);
    try std.testing.expectEqual(
        api.driver.async_accepted_total,
        api.driver.async_completed_total,
    );
    try std.testing.expectEqual(@as(usize, 0), api.driver.journal_slots_reserved);
    try std.testing.expectEqual(@as(usize, 0), api.driver.completion_slots_used);
    try std.testing.expect(!api.driver.active_binary_stream);
    const fatal = api.fatalInfo() orelse return error.MissingFatalRecord;
    try std.testing.expectEqual(runtime_mod.FailureStage.r2, fatal.stage);
    try std.testing.expectEqual(@as(?u64, failing_sequence), fatal.sequence);
    try std.testing.expectEqual(
        @as(?cmd.OrderCommandType, .balance_adjustment),
        fatal.command,
    );
    try std.testing.expectEqual(error.InjectedStageFailure, fatal.cause);
}

test "persistent grouped barrier poison yields execution before journal admission driver lock" {
    const allocator = std.testing.allocator;
    const uid: i64 = 222_375;
    const currency: i32 = 15;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 16;
    java_config.performance_cfg.msgs_in_group_limit = 4;
    java_config.performance_cfg.orderbook_kind = .direct;
    java_config.sharded_pipeline_mode = .persistent_grouped;
    java_config.init_state_cfg =
        config_mod.InitialStateConfig.cleanStart("EXPGROUPEDABBA");
    java_config.serialization_cfg = config_mod.SerializationConfig.diskJournaling();
    var api = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        java_config,
    );
    defer api.deinit();

    var add_user = try api.addUser(.routed, uid);
    add_user.deinit(api.allocator);
    var initial_balance = try api.adjustUserBalance(
        .routed,
        uid,
        1,
        currency,
        10_000,
        .adjustment,
    );
    initial_balance.deinit(api.allocator);

    const failing_sequence = api.core.kernels.next_seq + 1;
    var r2_gate: sharded_pipeline.PersistentGroupedRuntime.StageGate = .{
        .stage = .risk_release,
        .sequence = failing_sequence,
        .shard_index = 0,
        .fail = true,
    };
    api.persistent_grouped_stage_gate = &r2_gate;
    try api.startup();

    var grouped_response = try api.adjustUserBalance(
        .routed,
        uid,
        2,
        currency,
        1,
        .adjustment,
    );
    try std.testing.expectEqual(failing_sequence, grouped_response.sequence);
    api.releaseResponse(&grouped_response);

    const failed_request = try api.resetAsync(.routed);
    try waitForTestAtomicTrue(&r2_gate.reached, api.driver.io);

    var journal_gate: JournalCapacityTestGate = .{};
    api.journal_capacity_test_gate = &journal_gate;
    var producer: ShardedOrdinaryPublisher = .{
        .api = &api,
        .mode = .split,
        .uid = 222_376,
    };
    const producer_thread = try std.Thread.spawn(
        .{},
        ShardedOrdinaryPublisher.run,
        .{&producer},
    );
    // At this point the producer owns driver.mutex and is about to acquire
    // execution_mutex, while the barrier owner holds execution_mutex.
    try waitForTestAtomicTrue(&journal_gate.reached, api.driver.io);
    r2_gate.open();
    producer_thread.join();
    const suffix_request = try producer.result.?;

    api.shutdown();
    try std.testing.expect(api.persistent_grouped_runtime == null);
    try std.testing.expectEqual(runtime_mod.LifecycleState.poisoned, api.driver.lifecycle.current());
    const fatal = api.fatalInfo() orelse return error.MissingFatalRecord;
    try std.testing.expectEqual(runtime_mod.FailureStage.r2, fatal.stage);
    try std.testing.expectEqual(@as(?u64, failing_sequence), fatal.sequence);
    try std.testing.expectEqual(
        @as(?cmd.OrderCommandType, .balance_adjustment),
        fatal.command,
    );
    try std.testing.expectEqual(error.InjectedStageFailure, fatal.cause);

    try std.testing.expect(api.takeResult(failed_request) == null);
    try std.testing.expect(api.takeResult(suffix_request) == null);
    try std.testing.expectEqual(@as(usize, 3), api.journalLen());
    try std.testing.expectEqual(@as(usize, 0), api.driver.journal_slots_reserved);
    try std.testing.expectEqual(@as(usize, 0), api.driver.completion_slots_used);
    try std.testing.expectEqual(api.driver.accepted_total, api.driver.completed_total);
}

test "persistent grouped partial startup failure joins and destroys the partial fleet" {
    const allocator = std.testing.allocator;
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 16;
    java_config.performance_cfg.msgs_in_group_limit = 4;
    java_config.performance_cfg.matching_engines_num = 4;
    java_config.performance_cfg.risk_engines_num = 2;
    java_config.performance_cfg.orderbook_kind = .direct;
    java_config.sharded_pipeline_mode = .persistent_grouped;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();
    api.persistent_grouped_spawn_failure_after = 2;

    try std.testing.expectError(error.InjectedSpawnFailure, api.startup());
    try std.testing.expect(api.persistent_grouped_runtime == null);
    try std.testing.expect(!api.persistent_grouped_activated);
    try std.testing.expect(api.driver.owner == null);
    try std.testing.expect(api.driver.worker == null);
    try std.testing.expectEqual(runtime_mod.LifecycleState.cold, api.driver.lifecycle.current());
    try std.testing.expect(!api.core.isStarted());

    api.persistent_grouped_spawn_failure_after = null;
    try api.startup();
    try std.testing.expect(api.persistent_grouped_runtime != null);
    var retry_response = try api.addUser(.routed, 222_399);
    defer retry_response.deinit(api.allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, retry_response.result_code);
}

test "persistent grouped outer driver spawn rollback remains retryable" {
    const allocator = std.testing.allocator;
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 16;
    java_config.performance_cfg.msgs_in_group_limit = 4;
    java_config.performance_cfg.orderbook_kind = .direct;
    java_config.sharded_pipeline_mode = .persistent_grouped;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();
    api.persistent_grouped_driver_spawn_failure = true;

    try std.testing.expectError(error.InjectedDriverSpawnFailure, api.startup());
    try std.testing.expect(api.persistent_grouped_runtime == null);
    try std.testing.expect(!api.persistent_grouped_activated);
    try std.testing.expect(api.driver.owner == null);
    try std.testing.expect(api.driver.worker == null);
    try std.testing.expectEqual(runtime_mod.LifecycleState.cold, api.driver.lifecycle.current());
    try std.testing.expect(!api.core.isStarted());

    api.persistent_grouped_driver_spawn_failure = false;
    try api.startup();
    try std.testing.expect(api.persistent_grouped_runtime != null);
    try std.testing.expect(api.persistent_grouped_activated);
    var response = try api.addUser(.routed, 222_400);
    defer response.deinit(api.allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
}

test "persistent grouped durable failure publishes neither ack nor result" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 16;
    java_config.performance_cfg.msgs_in_group_limit = 4;
    java_config.performance_cfg.orderbook_kind = .direct;
    java_config.sharded_pipeline_mode = .persistent_grouped;
    java_config.serialization_cfg = config_mod.SerializationConfig.diskJournaling();
    var api = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        java_config,
    );
    defer api.deinit();
    try api.startup();

    api.durable_journal.?.injectSyncFailureForTesting();
    const failed_uid: i64 = 222_450;
    const skipped_uid: i64 = 222_451;
    const commands = [_]cmd.OrderCommand{
        makeAddUserCommand(failed_uid),
        makeAddUserCommand(skipped_uid),
    };
    var request_ids: [commands.len]u64 = undefined;
    try api.enqueueResponseBatch(
        &commands,
        .routed,
        .asynchronous,
        &request_ids,
    );
    try std.testing.expectError(error.CorePoisoned, api.drainPending());
    api.shutdown();
    // Durable append failure must precede all tail financial mutation. The
    // speculative grouped window is therefore disabled when an appender is
    // present, even though the in-memory journal reservation is infallible.
    try std.testing.expectEqual(@as(usize, 0), api.driver.grouped_batch_high_watermark);
    try std.testing.expect(api.durable_journal.?.poisoned);
    try std.testing.expectEqual(@as(usize, 1), api.journalLen());
    try std.testing.expectEqual(@as(usize, 0), api.ready_results.count());
    try std.testing.expectEqual(@as(usize, 0), api.driver.completion_slots_used);
    try std.testing.expectEqual(api.driver.accepted_total, api.driver.completed_total);
    try std.testing.expect(api.persistent_grouped_runtime == null);
    const fatal = api.fatalInfo() orelse return error.MissingFatalRecord;
    try std.testing.expectEqual(runtime_mod.FailureStage.journal, fatal.stage);
    try std.testing.expectEqual(@as(?u64, 1), fatal.sequence);
    try std.testing.expectEqual(error.InjectedJournalIoFailure, fatal.cause);
    const failed_shard = api.core.shardIndexForUid(failed_uid);
    const skipped_shard = api.core.shardIndexForUid(skipped_uid);
    try std.testing.expect(
        api.core.kernels.kernels[failed_shard].risk_engine.getUserProfile(failed_uid) != null,
    );
    try std.testing.expect(
        api.core.kernels.kernels[skipped_shard].risk_engine.getUserProfile(skipped_uid) == null,
    );
}

test "persistent grouped binary and persistence globals bridge without replacing workers" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 16;
    java_config.performance_cfg.msgs_in_group_limit = 4;
    java_config.performance_cfg.orderbook_kind = .direct;
    java_config.sharded_pipeline_mode = .persistent_grouped;
    java_config.init_state_cfg =
        config_mod.InitialStateConfig.cleanStart("EXPGLOBALBRIDGE");
    java_config.serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly();
    var api = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        java_config,
    );
    defer api.deinit();
    try api.startup();

    const matching_worker = api.persistent_grouped_runtime.?.matchingWorkerId(0);
    const risk_worker = api.persistent_grouped_runtime.?.riskWorkerId(0);
    const balances = [_]BinaryCurrencyBalance{
        .{ .currency = 15, .balance = 34_000 },
    };
    const users = [_]BinaryUserAccountsBatch{
        .{ .uid = 222_500, .accounts = balances[0..] },
    };
    var binary_response = try api.submitAddAccountsBinary(
        .routed,
        22_250,
        9_970_100_001,
        users[0..],
    );
    try std.testing.expectEqual(cmd.CommandResultCode.success, binary_response.result_code);
    api.releaseResponse(&binary_response);
    try std.testing.expectEqual(
        @as(?i64, 34_000),
        try api.queryUserBalance(222_500, 15),
    );

    var ordinary = try api.addUser(.split, 222_501);
    try std.testing.expectEqual(cmd.CommandResultCode.success, ordinary.result_code);
    api.releaseResponse(&ordinary);
    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try api.persistState(.routed, 500, 9_970_100_002),
    );
    try std.testing.expectEqual(@as(usize, 2), api.persistenceSnapshotCount());
    var reset_response = try api.reset(.split);
    try std.testing.expectEqual(cmd.CommandResultCode.success, reset_response.result_code);
    api.releaseResponse(&reset_response);
    var after_reset = try api.addUser(.routed, 222_500);
    try std.testing.expectEqual(cmd.CommandResultCode.success, after_reset.result_code);
    api.releaseResponse(&after_reset);

    try std.testing.expectEqual(
        matching_worker,
        api.persistent_grouped_runtime.?.matchingWorkerId(0),
    );
    try std.testing.expectEqual(
        risk_worker,
        api.persistent_grouped_runtime.?.riskWorkerId(0),
    );
}

test "sharded exchange api bootstrap and replay ownership is cold only" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try api.beginBootstrap();
    try std.testing.expectError(error.CoreReentrantCall, api.beginBootstrap());
    api.finishBootstrap();

    try api.startup();
    try std.testing.expectError(
        error.CoreAlreadyStarted,
        api.bootstrapFromPersistence(tmp.dir, null, null),
    );
    try std.testing.expectError(
        error.CoreAlreadyStarted,
        api.startupFromPersistence(tmp.dir, null, null),
    );
}

test "sharded exchange api running worker progresses async commands without manual drain" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    try api.startup();

    const request_id = try api.addUserAsync(.split, 130_001);
    var barrier = try api.queryReport(.{ .single_user = 130_001 });
    defer barrier.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        barrier.single_user.query_execution_status,
    );

    var response = api.takeResult(request_id) orelse return error.MissingAsyncResult;
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    try std.testing.expectEqual(@as(usize, 0), api.pendingCount());
}

test "sharded exchange api synchronous publication cannot overtake accepted async work" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    try api.startup();

    const async_id = try api.addUserAsync(.split, 130_101);
    var synchronous = try api.addUser(.split, 130_101);
    defer synchronous.deinit(allocator);
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_user_already_exists,
        synchronous.result_code,
    );

    var asynchronous = api.takeResult(async_id) orelse return error.MissingAsyncResult;
    defer asynchronous.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, asynchronous.result_code);
    try std.testing.expect(asynchronous.sequence < synchronous.sequence);
}

test "sharded bounded shutdown preserves routed and split work across callback timeout" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    var gate: ShardedBackpressureGate = .{ .io = std.testing.io };
    defer gate.release();
    api.setResultSink(.{
        .context = &gate,
        .on_result = ShardedBackpressureGate.onResult,
    });
    try api.startup();

    try api.enqueueRuntime(makeNopCommand());
    gate.waitEntered();
    const routed_request = try api.addUserAsync(.routed, 130_201);
    const split_request = try api.addUserAsync(.split, 130_202);

    try std.testing.expectError(
        error.Timeout,
        api.shutdownTimeout(immediateShutdownTimeout()),
    );
    try std.testing.expectEqual(ShutdownState.closing, api.shutdownState());
    try std.testing.expect(api.isStopped());
    try std.testing.expectError(
        error.CoreStopped,
        api.addUserAsync(.routed, 130_203),
    );

    api.driver.mutex.lockUncancelable(api.driver.io);
    try std.testing.expect(api.driver.worker != null);
    try std.testing.expect(!api.driver.join_claimed);
    try std.testing.expect(!api.driver.worker_quiesced.isSet());
    api.driver.mutex.unlock(api.driver.io);

    gate.release();
    try std.testing.expectEqual(
        ShutdownOutcome.complete,
        try api.shutdownTimeout(.none),
    );
    try std.testing.expectEqual(ShutdownState.complete, api.shutdownState());

    var routed = api.takeResult(routed_request) orelse
        return error.MissingAsyncResult;
    defer routed.deinit(allocator);
    var split = api.takeResult(split_request) orelse
        return error.MissingAsyncResult;
    defer split.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, routed.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, split.result_code);
    try std.testing.expect(routed.sequence < split.sequence);

    api.driver.mutex.lockUncancelable(api.driver.io);
    const totals = .{
        api.driver.accepted_total,
        api.driver.completed_total,
        api.driver.async_accepted_total,
        api.driver.async_completed_total,
        api.driver.runtime_accepted_total,
        api.driver.runtime_completed_total,
        api.driver.callback_completions_pending,
        api.driver.completion_slots_used,
        api.driver.queue.count(),
    };
    api.driver.mutex.unlock(api.driver.io);
    try std.testing.expectEqual(totals[0], totals[1]);
    try std.testing.expectEqual(@as(u64, 3), totals[0]);
    try std.testing.expectEqual(totals[2], totals[3]);
    try std.testing.expectEqual(@as(u64, 2), totals[2]);
    try std.testing.expectEqual(totals[4], totals[5]);
    try std.testing.expectEqual(@as(u64, 1), totals[4]);
    try std.testing.expectEqual(@as(usize, 0), totals[6]);
    try std.testing.expectEqual(@as(usize, 0), totals[7]);
    try std.testing.expectEqual(@as(usize, 0), totals[8]);
}

test "sharded shutdown deadline fences admission while driver mutex is contended" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    try api.startup();

    var gate: ShardedDriverLockGate = .{
        .api = &api,
        .io = std.testing.io,
    };
    const holder = try std.Thread.spawn(.{}, ShardedDriverLockGate.run, .{&gate});
    defer holder.join();
    defer gate.release.set(gate.io);
    gate.entered.waitUncancelable(gate.io);

    try std.testing.expectError(
        error.Timeout,
        api.shutdownTimeout(immediateShutdownTimeout()),
    );
    // The gate remains held until after this positive deadline expires. A
    // retry must not acquire it after expiry and report success.
    try std.testing.expectError(
        error.Timeout,
        api.shutdownTimeout(shortShutdownTimeout()),
    );
    // Both queries are lock-free after the release-store fence.
    try std.testing.expectEqual(ShutdownState.closing, api.shutdownState());
    try std.testing.expect(api.isStopped());

    gate.release.set(gate.io);
    try std.testing.expectError(
        error.CoreStopped,
        api.addUserAsync(.split, 130_301),
    );
    try std.testing.expectEqual(
        ShutdownOutcome.complete,
        try api.shutdownTimeout(.none),
    );
    try std.testing.expectEqual(ShutdownState.complete, api.shutdownState());
}

test "sharded deinit completes retained worker after bounded timeout" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    var api_live = true;
    defer if (api_live) api.deinit();

    var gate: ShardedBackpressureGate = .{ .io = std.testing.io };
    defer gate.release();
    api.setResultSink(.{
        .context = &gate,
        .on_result = ShardedBackpressureGate.onResult,
    });
    try api.startup();

    try api.enqueueRuntime(makeNopCommand());
    gate.waitEntered();
    try std.testing.expectError(
        error.Timeout,
        api.shutdownTimeout(immediateShutdownTimeout()),
    );
    try std.testing.expectEqual(ShutdownState.closing, api.shutdownState());

    gate.release();
    // No explicit shutdown retry: deinit owns the sequential unbounded wait
    // and join for the worker retained by the timed-out call.
    api.deinit();
    api_live = false;
}

test "sharded foreign callback defers on contended driver and external retry completes" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    try api.startup();

    var gate: ShardedDriverLockGate = .{
        .api = &api,
        .io = std.testing.io,
    };
    const holder = try std.Thread.spawn(.{}, ShardedDriverLockGate.run, .{&gate});
    defer holder.join();
    defer gate.release.set(gate.io);
    gate.entered.waitUncancelable(gate.io);

    {
        var callback_scope = runtime_mod.CallbackExecutionScope.enter();
        defer callback_scope.leave();
        try std.testing.expectEqual(
            ShutdownOutcome.deferred,
            try api.shutdownTimeout(.none),
        );
    }
    try std.testing.expectEqual(ShutdownState.closing, api.shutdownState());

    gate.release.set(gate.io);
    try std.testing.expectError(
        error.CoreStopped,
        api.addUserAsync(.routed, 130_331),
    );

    try std.testing.expectEqual(
        ShutdownOutcome.complete,
        try api.shutdownTimeout(.none),
    );
    try std.testing.expectEqual(ShutdownState.complete, api.shutdownState());
}

test "sharded cold bootstrap ownership rejects publication and poison settles queued work" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();

    api.driver.mutex.lockUncancelable(api.driver.io);
    api.driver.bootstrap_owner = std.Thread.getCurrentId();
    api.driver.mutex.unlock(api.driver.io);
    try std.testing.expectError(
        error.CoreNotStarted,
        api.addUserAsync(.routed, 130_351),
    );
    try std.testing.expectError(
        error.CoreNotStarted,
        api.enqueueRuntime(makeNopCommand()),
    );
    api.driver.mutex.lockUncancelable(api.driver.io);
    api.driver.bootstrap_owner = null;
    api.driver.completion_available.broadcast(api.driver.io);
    api.driver.mutex.unlock(api.driver.io);

    const skipped_request = try api.addUserAsync(.split, 130_352);
    const sequence_before_bootstrap =
        api.driver.publication_sequencer.highWater();
    const accepted_before_bootstrap = api.driver.accepted_total;
    const queued_before_bootstrap = api.driver.queue.count();
    try std.testing.expectError(
        error.CoreNotStarted,
        api.bootstrapFromPersistence(std.Io.Dir.cwd(), null, null),
    );
    try std.testing.expectEqual(
        sequence_before_bootstrap,
        api.driver.publication_sequencer.highWater(),
    );
    try std.testing.expectEqual(
        accepted_before_bootstrap,
        api.driver.accepted_total,
    );
    try std.testing.expectEqual(
        queued_before_bootstrap,
        api.driver.queue.count(),
    );

    api.driver.mutex.lockUncancelable(api.driver.io);
    _ = api.driver.lifecycle.poison(.{
        .stage = .startup,
        .sequence = null,
        .request_id = null,
        .command = null,
        .cause = error.InjectedBootstrapFailure,
    });
    api.driver.work_available.broadcast(api.driver.io);
    api.driver.space_available.broadcast(api.driver.io);
    api.driver.completion_available.broadcast(api.driver.io);
    api.driver.mutex.unlock(api.driver.io);

    try std.testing.expectEqual(
        ShutdownOutcome.complete,
        try api.shutdownTimeout(.none),
    );
    try std.testing.expectEqual(ShutdownState.complete, api.shutdownState());
    try std.testing.expect(api.takeResult(skipped_request) == null);
    try std.testing.expectEqual(
        @as(?i64, null),
        try api.core.queryUserBalance(130_352, 15),
    );

    api.driver.mutex.lockUncancelable(api.driver.io);
    const totals = .{
        api.driver.accepted_total,
        api.driver.completed_total,
        api.driver.async_accepted_total,
        api.driver.async_completed_total,
        api.driver.callback_completions_pending,
        api.driver.journal_slots_reserved,
        api.driver.completion_slots_used,
        api.driver.queue.count(),
        api.driver.prepared_available_remaining,
        api.driver.active_binary_stream,
    };
    api.driver.mutex.unlock(api.driver.io);
    try std.testing.expectEqual(totals[0], totals[1]);
    try std.testing.expectEqual(@as(u64, 1), totals[0]);
    try std.testing.expectEqual(totals[2], totals[3]);
    try std.testing.expectEqual(@as(u64, 1), totals[2]);
    try std.testing.expectEqual(@as(usize, 0), totals[4]);
    try std.testing.expectEqual(@as(usize, 0), totals[5]);
    try std.testing.expectEqual(@as(usize, 0), totals[6]);
    try std.testing.expectEqual(@as(usize, 0), totals[7]);
    try std.testing.expectEqual(@as(usize, 0), totals[8]);
    try std.testing.expect(!totals[9]);
}

test "persistent grouped bounded shutdown waits for blocked r2 and later completes" {
    const allocator = std.testing.allocator;
    const uid: i64 = 222_325;
    const currency: i32 = 15;
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 16;
    java_config.performance_cfg.msgs_in_group_limit = 4;
    java_config.performance_cfg.orderbook_kind = .direct;
    java_config.sharded_pipeline_mode = .persistent_grouped;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();

    var add_user = try api.addUser(.routed, uid);
    add_user.deinit(api.allocator);
    var initial_balance = try api.adjustUserBalance(
        .split,
        uid,
        1,
        currency,
        10_000,
        .adjustment,
    );
    initial_balance.deinit(api.allocator);

    const blocked_sequence = api.core.kernels.next_seq + 1;
    var r2_gate: sharded_pipeline.PersistentGroupedRuntime.StageGate = .{
        .stage = .risk_release,
        .sequence = blocked_sequence,
        .shard_index = 0,
    };
    defer r2_gate.open();
    api.persistent_grouped_stage_gate = &r2_gate;
    try api.startup();

    const request_id = try api.adjustUserBalanceAsync(
        .routed,
        uid,
        2,
        currency,
        1,
        .adjustment,
    );
    try std.testing.expectError(
        error.Timeout,
        api.shutdownTimeout(immediateShutdownTimeout()),
    );
    try waitForTestAtomicTrue(&r2_gate.reached, api.driver.io);
    try std.testing.expectEqual(ShutdownState.closing, api.shutdownState());
    try std.testing.expect(api.persistent_grouped_runtime != null);
    try std.testing.expectError(
        error.CoreStopped,
        api.addUserAsync(.split, uid + 1),
    );

    api.driver.mutex.lockUncancelable(api.driver.io);
    try std.testing.expect(api.driver.worker != null);
    try std.testing.expect(!api.driver.worker_quiesced.isSet());
    api.driver.mutex.unlock(api.driver.io);

    r2_gate.open();
    try std.testing.expectEqual(
        ShutdownOutcome.complete,
        try api.shutdownTimeout(.none),
    );
    try std.testing.expectEqual(ShutdownState.complete, api.shutdownState());
    try std.testing.expect(api.persistent_grouped_runtime == null);
    try std.testing.expect(api.core.isStopped());

    var response = api.takeResult(request_id) orelse
        return error.MissingAsyncResult;
    defer response.deinit(api.allocator);
    try std.testing.expectEqual(blocked_sequence, response.sequence);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);

    api.driver.mutex.lockUncancelable(api.driver.io);
    const totals = .{
        api.driver.accepted_total,
        api.driver.completed_total,
        api.driver.async_accepted_total,
        api.driver.async_completed_total,
        api.driver.completion_slots_used,
        api.driver.queue.count(),
    };
    api.driver.mutex.unlock(api.driver.io);
    try std.testing.expectEqual(totals[0], totals[1]);
    try std.testing.expectEqual(@as(u64, 1), totals[0]);
    try std.testing.expectEqual(totals[2], totals[3]);
    try std.testing.expectEqual(@as(u64, 1), totals[2]);
    try std.testing.expectEqual(@as(usize, 0), totals[4]);
    try std.testing.expectEqual(@as(usize, 0), totals[5]);
}

test "sharded exchange api graceful shutdown drains the accepted fifo prefix" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    try api.startup();

    var request_ids: [128]u64 = undefined;
    for (&request_ids, 0..) |*request_id, index| {
        request_id.* = try api.addUserAsync(.split, 131_000 + @as(i64, @intCast(index)));
    }
    api.shutdown();

    try std.testing.expect(api.isStopped());
    try std.testing.expectEqual(@as(usize, 0), api.pendingCount());
    for (request_ids) |request_id| {
        var response = api.takeResult(request_id) orelse return error.MissingAsyncResult;
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    }
    try std.testing.expectEqual(@as(usize, 0), api.readyCount());
}

test "sharded exchange api bounds result publication and wakes blocked producer on take" {
    const allocator = std.testing.allocator;
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 8;
    java_config.performance_cfg.matching_engines_num = 4;
    java_config.performance_cfg.risk_engines_num = 4;
    java_config.performance_cfg.msgs_in_group_limit = 2;
    java_config.performance_cfg.wait_strategy = .blocking;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();

    var initial_ids: [8]u64 = undefined;
    for (&initial_ids, 0..) |*request_id, index| {
        request_id.* = try api.addUserAsync(.split, 131_500 + @as(i64, @intCast(index)));
    }
    try std.testing.expectError(error.CoreQueueFull, api.addUserAsync(.split, 131_508));
    try api.startup();
    try std.testing.expectEqual(@as(usize, initial_ids.len), try api.drainPending());
    try std.testing.expectEqual(@as(usize, initial_ids.len), api.readyCount());

    const Producer = struct {
        api: *ExchangeApiSharded,
        request_id: ?u64 = null,
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            self.request_id = self.api.addUserAsync(.split, 131_508) catch |err| {
                self.failure = err;
                return;
            };
        }
    };
    var producer: Producer = .{ .api = &api };
    const producer_thread = try std.Thread.spawn(.{}, Producer.run, .{&producer});

    api.driver.mutex.lockUncancelable(api.driver.io);
    while (api.driver.blocked_producers == 0) {
        api.driver.completion_available.waitUncancelable(api.driver.io, &api.driver.mutex);
    }
    api.driver.mutex.unlock(api.driver.io);

    var released = api.takeResult(initial_ids[0]) orelse return error.MissingAsyncResult;
    defer released.deinit(allocator);
    producer_thread.join();
    try std.testing.expectEqual(@as(?anyerror, null), producer.failure);
    const producer_id = producer.request_id orelse return error.MissingAsyncResult;

    api.shutdown();
    for (initial_ids[1..]) |request_id| {
        var response = api.takeResult(request_id) orelse return error.MissingAsyncResult;
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    }
    var producer_response = api.takeResult(producer_id) orelse return error.MissingAsyncResult;
    defer producer_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, producer_response.result_code);
}

test "sharded exchange api fatal split settlement poisons and skips accepted tail" {
    const allocator = std.testing.allocator;
    const maker_uid: i64 = 132_001;
    const taker_uid: i64 = 132_002;
    const spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = 5_991,
        .symbol_type = .futures_contract,
        .base_currency = 840,
        .quote_currency = 392,
        .base_scale_k = 100_000,
        .quote_scale_k = 10,
        .taker_fee = 3,
        .maker_fee = 2,
        .margin_buy = 5_000,
        .margin_sell = 6_000,
    });
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.matching_engines_num = 4;
    java_config.performance_cfg.risk_engines_num = 4;
    java_config.performance_cfg.orderbook_kind = .direct;
    java_config.sharded_pipeline_mode = .immediate;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();
    try api.addSymbolAll(spec);

    for ([_]i64{ maker_uid, taker_uid }) |uid| {
        var add = try api.addUser(.split, uid);
        defer add.deinit(allocator);
        var deposit = try api.adjustUserBalance(.split, uid, 1, spec.quote_currency, 100_000, .adjustment);
        defer deposit.deinit(allocator);
    }
    var maker = try api.placeOrder(
        .split,
        132_101,
        maker_uid,
        spec.symbol_id,
        10_770,
        0,
        1,
        .ask,
        .gtc,
        0,
    );
    defer maker.deinit(allocator);

    const maker_shard = api.core.shardIndexForUid(maker_uid);
    const maker_profile = api.core.kernels.kernels[maker_shard].risk_engine.getUserProfile(maker_uid) orelse
        unreachable;
    _ = maker_profile.removePosition(spec.symbol_id) orelse unreachable;
    const sequence_before = api.core.kernels.next_seq;

    try api.startup();
    const failed_id = try api.placeOrderAsync(
        .split,
        132_102,
        taker_uid,
        spec.symbol_id,
        10_770,
        10_770,
        1,
        .bid,
        .ioc,
        0,
    );
    const skipped_id = try api.nopAsync(.split);
    try std.testing.expectError(error.CorePoisoned, api.drainPending());

    const fatal = api.fatalInfo() orelse return error.MissingFatalRecord;
    try std.testing.expectEqual(error.PositionRecordCorrupted, fatal.cause);
    try std.testing.expectEqual(runtime_mod.FailureStage.r2, fatal.stage);
    try std.testing.expectEqual(@as(?u64, sequence_before + 1), fatal.sequence);
    try std.testing.expectEqual(@as(?u64, failed_id), fatal.request_id);
    try std.testing.expectEqual(@as(?cmd.OrderCommandType, .place_order), fatal.command);
    try std.testing.expect(!api.isResultReady(failed_id));
    try std.testing.expect(!api.isResultReady(skipped_id));
    try std.testing.expectEqual(@as(usize, 0), api.pendingCount());
    try std.testing.expectEqual(@as(usize, 0), api.driver.completion_slots_used);
    try std.testing.expectEqual(sequence_before + 1, api.core.kernels.next_seq);
    try std.testing.expectError(error.CorePoisoned, api.nopAsync(.split));
}

test "sharded exchange api callback owners receive terminal fatal settlement" {
    const allocator = std.testing.allocator;
    const maker_uid: i64 = 132_201;
    const taker_uid: i64 = 132_202;
    const skipped_uid: i64 = 132_203;
    const skipped_detached_uid: i64 = 132_204;
    const spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = 5_992,
        .symbol_type = .futures_contract,
        .base_currency = 840,
        .quote_currency = 392,
        .base_scale_k = 100_000,
        .quote_scale_k = 10,
        .taker_fee = 3,
        .maker_fee = 2,
        .margin_buy = 5_000,
        .margin_sell = 6_000,
    });
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.matching_engines_num = 4;
    java_config.performance_cfg.risk_engines_num = 4;
    java_config.performance_cfg.orderbook_kind = .direct;
    java_config.sharded_pipeline_mode = .immediate;
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, java_config);
    defer api.deinit();
    try api.addSymbolAll(spec);

    for ([_]i64{ maker_uid, taker_uid }) |uid| {
        var add = try api.addUser(.split, uid);
        defer add.deinit(allocator);
        var deposit = try api.adjustUserBalance(.split, uid, 1, spec.quote_currency, 100_000, .adjustment);
        defer deposit.deinit(allocator);
    }
    var maker = try api.placeOrder(
        .split,
        132_301,
        maker_uid,
        spec.symbol_id,
        10_770,
        0,
        1,
        .ask,
        .gtc,
        0,
    );
    defer maker.deinit(allocator);

    const maker_shard = api.core.shardIndexForUid(maker_uid);
    const maker_profile = api.core.kernels.kernels[maker_shard].risk_engine.getUserProfile(maker_uid) orelse
        unreachable;
    _ = maker_profile.removePosition(spec.symbol_id) orelse unreachable;

    var probe: FatalDrainBarrierProbe = .{
        .api = &api,
        .io = api.driver.io,
    };
    defer if (!probe.release.isSet()) probe.release.set(probe.io);
    const callback: CommandResponseCallback = .{
        .context = &probe,
        .callback = FatalDrainBarrierProbe.onResponse,
        .failure_callback = FatalDrainBarrierProbe.onFailure,
    };
    const request_id_before = api.next_request_id;
    try api.placeNewOrderWithCallback(
        .split,
        132_302,
        0,
        10_770,
        10_770,
        1,
        .bid,
        .ioc,
        spec.symbol_id,
        taker_uid,
        callback,
    );
    try api.createUserWithCallback(.split, skipped_uid, callback);
    try api.submitCommand(.split, .{ .add_user = skipped_detached_uid });
    const skipped_poll_id = try api.nopAsync(.split);
    const async_drained_before = api.driver.async_drained_total;
    try std.testing.expectEqual(@as(usize, 2), api.pendingCallbackCount());
    try api.startup();
    try waitForTestEvent(&probe.entered, probe.io);
    try waitForTestEvent(&probe.reentrant_finished, probe.io);
    const reentrant_result = probe.reentrant_result orelse
        return error.MissingReentrantDrainResult;
    try std.testing.expectError(error.CoreReentrantCall, reentrant_result);

    var drain_runner: ShardedAcceptedDrainRunner = .{
        .api = &api,
        .io = api.driver.io,
    };
    const drain_thread = try std.Thread.spawn(
        .{},
        ShardedAcceptedDrainRunner.run,
        .{&drain_runner},
    );
    defer {
        if (!probe.release.isSet()) probe.release.set(probe.io);
        drain_thread.join();
    }
    try expectAcceptedDrainBlocked(&api, &drain_runner);
    probe.release.set(probe.io);
    try waitForTestEvent(&drain_runner.finished, drain_runner.io);
    const drain_result = drain_runner.result orelse return error.MissingDrainResult;
    try std.testing.expectError(error.CorePoisoned, drain_result);

    const failed_request_id = request_id_before + 1;
    const skipped_request_id = request_id_before + 2;
    try std.testing.expectEqual(@as(usize, 0), probe.records.response_count);
    try std.testing.expectEqual(@as(usize, 2), probe.records.failure_count);
    try std.testing.expect(probe.records.all_failure_callbacks_scoped);
    try std.testing.expectEqual(failed_request_id, probe.records.request_ids[0]);
    try std.testing.expectEqual(skipped_request_id, probe.records.request_ids[1]);
    try std.testing.expectEqual(CallbackFailureKind.command_failed, probe.records.kinds[0]);
    try std.testing.expectEqual(CallbackFailureKind.skipped_after_fatal, probe.records.kinds[1]);
    for (0..probe.records.failure_count) |index| {
        try std.testing.expectEqual(error.PositionRecordCorrupted, probe.records.causes[index]);
        try std.testing.expectEqual(@as(?u64, failed_request_id), probe.records.fatal_request_ids[index]);
    }
    try std.testing.expectEqual(@as(usize, 0), api.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, 0), api.pendingCount());
    try std.testing.expectEqual(@as(usize, 0), api.driver.completion_slots_used);
    try std.testing.expect(!api.isResultReady(skipped_poll_id));
    try std.testing.expectEqual(api.driver.accepted_total, api.driver.completed_total);
    try std.testing.expectEqual(
        api.driver.async_accepted_total,
        api.driver.async_completed_total,
    );
    try std.testing.expectEqual(async_drained_before, api.driver.async_drained_total);
    try std.testing.expectEqual(@as(usize, 0), api.driver.accepted_drain_waiters);
    try std.testing.expectEqual(@as(usize, 0), api.runtimePendingCount());
    try std.testing.expect(!probe.release_wait_failed.load(.acquire));
    const skipped_shard = api.core.shardIndexForUid(skipped_uid);
    try std.testing.expect(
        api.core.kernels.kernels[skipped_shard].risk_engine.getUserProfile(skipped_uid) == null,
    );
    const skipped_detached_shard = api.core.shardIndexForUid(skipped_detached_uid);
    try std.testing.expect(
        api.core.kernels.kernels[skipped_detached_shard].risk_engine.getUserProfile(skipped_detached_uid) == null,
    );
}

test "sharded lost first poison classifies an already popped callback as skipped" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    var probe: FatalCallbackProbe = .{};
    const callback: CommandResponseCallback = .{
        .context = &probe,
        .callback = FatalCallbackProbe.onResponse,
        .failure_callback = FatalCallbackProbe.onFailure,
    };

    api.driver.mutex.lockUncancelable(api.driver.io);
    _ = api.driver.lifecycle.poison(.{
        .stage = .r2,
        .sequence = 1,
        .request_id = 40,
        .command = .balance_adjustment,
        .cause = error.InjectedStageFailure,
    });
    api.driver.accepted_total = 1;
    api.driver.async_accepted_total = 1;
    api.driver.callback_completions_pending = 1;
    api.driver.mutex.unlock(api.driver.io);

    api.poisonFromWork(
        .{
            .acceptance_id = 1,
            .sequence = publication.PublicationSequence.fromInt(2),
            .mode = .routed,
            .command = makeAddUserCommand(132_401),
            .completion = .{ .callback = .{
                .request_id = 41,
                .callback = callback,
            } },
            .kind = .asynchronous,
        },
        error.CorePoisoned,
    );

    try std.testing.expectEqual(@as(usize, 0), probe.response_count);
    try std.testing.expectEqual(@as(usize, 1), probe.failure_count);
    try std.testing.expect(probe.all_failure_callbacks_scoped);
    try std.testing.expectEqual(@as(u64, 41), probe.request_ids[0]);
    try std.testing.expectEqual(
        CallbackFailureKind.skipped_after_fatal,
        probe.kinds[0],
    );
    try std.testing.expectEqual(error.InjectedStageFailure, probe.causes[0]);
    try std.testing.expectEqual(@as(?u64, 40), probe.fatal_request_ids[0]);
    try std.testing.expectEqual(
        api.driver.accepted_total,
        api.driver.completed_total,
    );
    try std.testing.expectEqual(
        api.driver.async_accepted_total,
        api.driver.async_completed_total,
    );
    try std.testing.expectEqual(@as(usize, 0), api.driver.callback_completions_pending);
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

    try std.testing.expectError(
        error.CoreAlreadyStarted,
        api.bootstrapFromPersistence(tmp.dir, null, null),
    );
    try std.testing.expectError(
        error.CoreAlreadyStarted,
        api.startupFromConfiguredPersistence(tmp.dir, .matching_engine_router, 0, 0),
    );
    try std.testing.expectError(
        error.CoreAlreadyStarted,
        api.startupFromDiscoveredConfiguredPersistence(tmp.dir, .matching_engine_router, 0, 0),
    );
    try std.testing.expectError(
        error.CoreAlreadyStarted,
        api.startupFromDiscoveredConfiguredPersistenceStrictMainLog(
            tmp.dir,
            .matching_engine_router,
            0,
            0,
        ),
    );

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
    try std.testing.expectError(
        error.PersistStateRejected,
        api.persistStateWithDefaultMainLog(tmp.dir, 91, 9_960_100_001, .matching_engine_router, 0),
    );
    try std.testing.expectEqual(@as(usize, 1), api.persistenceSnapshotCount());
    try std.testing.expectEqual(
        @as(bool, true),
        try api.snapshotExistsDefaultPath(tmp.dir, 91, .matching_engine_router, 0),
    );
    try std.testing.expectEqual(@as(bool, true), try api.snapshotExistsFile(tmp.dir, "snapshot.ecs"));
}

test "grouped XCM discovery fails closed only when XCM1 is newest" {
    try std.testing.expectError(
        error.LegacyXcm1RequiresImmediateOrMigration,
        selectDiscoveredXcm2(.{ .latest_xcm1 = 10 }),
    );
    try std.testing.expectError(
        error.LegacyXcm1RequiresImmediateOrMigration,
        selectDiscoveredXcm2(.{
            .latest_xcm1 = 10,
            .latest_xcm2 = 9,
        }),
    );
    try std.testing.expectEqual(
        @as(?u64, 11),
        try selectDiscoveredXcm2(.{
            .latest_xcm1 = 10,
            .latest_xcm2 = 11,
        }),
    );
    try std.testing.expectEqual(
        @as(?u64, 10),
        try selectDiscoveredXcm2(.{
            .latest_xcm1 = 10,
            .latest_xcm2 = 10,
        }),
    );
    try std.testing.expectEqual(
        @as(?u64, null),
        try selectDiscoveredXcm2(.{}),
    );
}

test "grouped XCM1 configured and discovered startup requires immediate or migration" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const exchange_id = "ECXCM1FAIL";
    const storage_folder = "xcm1-fail-closed";
    const snapshot_id: u64 = 501;

    var legacy_config = Config.javaDefault();
    legacy_config.initial_state = config_mod.InitialStateConfig.cleanStart(exchange_id);
    legacy_config.serialization = .{
        .backend = .disk,
        .storage_folder = storage_folder,
    };
    legacy_config.sharded_pipeline_mode = .immediate;
    var legacy_source = try exchange_core_mod.ExchangeCore.initWithPersistenceRoot(
        allocator,
        tmp.dir,
        legacy_config,
    );
    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try legacy_source.persistState(@intCast(snapshot_id), 9_960_501_001),
    );
    legacy_source.deinit();

    var exact_config = config_mod.JavaExchangeConfig.javaDefault();
    exact_config.init_state_cfg = config_mod.InitialStateConfig.fromSnapshotOnly(
        exchange_id,
        snapshot_id,
        0,
    );
    exact_config.serialization_cfg = .{
        .backend = .disk,
        .storage_folder = storage_folder,
    };
    var exact = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        exact_config,
    );
    defer exact.deinit();
    try std.testing.expectError(
        error.LegacyXcm1RequiresImmediateOrMigration,
        exact.startupFromConfiguredPersistence(
            tmp.dir,
            .matching_engine_router,
            0,
            0,
        ),
    );
    try std.testing.expect(!exact.isStarted());
    try std.testing.expect(exact.fatalInfo() == null);

    var discovered_config = exact_config;
    discovered_config.init_state_cfg =
        config_mod.InitialStateConfig.cleanStart(exchange_id);
    var discovered = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        discovered_config,
    );
    defer discovered.deinit();
    try std.testing.expectError(
        error.LegacyXcm1RequiresImmediateOrMigration,
        discovered.startupFromDiscoveredConfiguredPersistence(
            tmp.dir,
            .matching_engine_router,
            0,
            0,
        ),
    );
    try std.testing.expect(!discovered.isStarted());
    try std.testing.expect(discovered.fatalInfo() == null);

    var strict = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        discovered_config,
    );
    defer strict.deinit();
    try std.testing.expectError(
        error.LegacyXcm1RequiresImmediateOrMigration,
        strict.startupFromDiscoveredConfiguredPersistenceStrictMainLog(
            tmp.dir,
            .matching_engine_router,
            0,
            0,
        ),
    );
    try std.testing.expect(!strict.isStarted());
    try std.testing.expect(strict.fatalInfo() == null);
}

test "sharded exchange api discovered startup picks latest snapshot and main-log base sequence" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const source_cfg = config_mod.JavaExchangeConfig{
        .init_state_cfg = config_mod.InitialStateConfig.cleanStart("ECDISC"),
        .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
    };
    var source = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        source_cfg,
    );
    defer source.deinit();

    var add = try source.addUser(.routed, 301_001);
    defer add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add.result_code);

    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try source.persistState(.routed, 300, 9_960_300_001),
    );
    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try source.persistState(.routed, 301, 9_960_301_001),
    );
    const latest_sequence = source.persistenceCurrentSnapshot().seq;

    const target_cfg = config_mod.JavaExchangeConfig{
        .init_state_cfg = config_mod.InitialStateConfig.fromSnapshotOnly("ECDISC", 1, latest_sequence),
        .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
    };
    var target = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        target_cfg,
    );
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
    try std.testing.expectEqual(latest_sequence + 1, duplicate.sequence);
}

test "sharded exchange api strict discovered startup rejects malformed main log entries" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const cfg = config_mod.JavaExchangeConfig{
        .init_state_cfg = config_mod.InitialStateConfig.cleanStart("ECSTRICT"),
        .serialization_cfg = config_mod.SerializationConfig.diskSnapshotOnly(),
    };
    var source = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        cfg,
    );
    defer source.deinit();

    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try source.persistState(.routed, 401, 9_960_401_001),
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
    var strict_target = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        target_cfg,
    );
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var config = Config.javaDefault();
    config.matching.num_shards = 4;
    config.matching.orderbook_kind = .direct;
    config.serialization = .{
        .backend = .disk,
        .storage_folder = "xcm2-dumps",
    };
    var api = try ExchangeApiSharded.initWithPersistenceRoot(allocator, tmp.dir, config);
    defer api.deinit();

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

    var restore_config = config;
    restore_config.initial_state = config_mod.InitialStateConfig.fromSnapshotOnly(
        config.initial_state.exchange_id,
        1,
        0,
    );
    var restored = try ExchangeApiSharded.initWithPersistenceRoot(
        allocator,
        tmp.dir,
        restore_config,
    );
    defer restored.deinit();
    const outcome = try restored.startupFromDiscoveredConfiguredPersistenceStrictMainLog(
        tmp.dir,
        .matching_engine_router,
        0,
        0,
    );
    try std.testing.expect(outcome.snapshot_found);
    try std.testing.expectEqual(@as(u64, 7001), restored.persistenceCurrentSnapshot().snapshot_id);
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
    var split_add = try source.addUser(.split, 201_002);
    defer split_add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, split_add.result_code);
    try std.testing.expectEqual(@as(usize, 2), source.journalLen());
    try std.testing.expectEqual(journal_mod.ExecutionMode.routed, source.journal.?.entries.items[0].mode);
    try std.testing.expectEqual(journal_mod.ExecutionMode.split, source.journal.?.entries.items[1].mode);

    try source.writeJournalToFile(tmp.dir, "journal.ecj");
    try source.writeJournalToDefaultPath(tmp.dir, 701, 0);

    var loaded = try ExchangeApiSharded.initFromJavaConfig(
        allocator,
        journaling_cfg,
    );
    defer loaded.deinit();
    try loaded.loadJournalFromFile(tmp.dir, "journal.ecj");
    try std.testing.expectEqual(@as(usize, 2), loaded.journalLen());
    try loaded.loadJournalFromDefaultPath(tmp.dir, 701, 0);
    try std.testing.expectEqual(@as(usize, 2), loaded.journalLen());

    var replayed = try ExchangeApiSharded.initFromJavaConfig(
        allocator,
        journaling_cfg,
    );
    defer replayed.deinit();
    try std.testing.expectEqual(@as(usize, 2), try replayed.replayJournalFromFile(tmp.dir, "journal.ecj"));
    try std.testing.expectEqual(@as(usize, 2), replayed.journalLen());

    var duplicate = try replayed.addUser(.routed, 201_001);
    defer duplicate.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, duplicate.result_code);

    var replayed_default = try ExchangeApiSharded.initFromJavaConfig(
        allocator,
        journaling_cfg,
    );
    defer replayed_default.deinit();
    try std.testing.expectEqual(@as(usize, 2), try replayed_default.replayJournalFromDefaultPath(tmp.dir, 701, 0));
}

test "sharded journal replay mismatch poisons the mutated instance before retry" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journaling_cfg = config_mod.JavaExchangeConfig{
        .init_state_cfg = .{
            .exchange_id = "ECJ-REPLAY-FAIL-CLOSED",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = std.math.maxInt(u64),
            .throw_if_snapshot_not_found = false,
        },
        .serialization_cfg = config_mod.SerializationConfig.diskJournaling(),
    };

    {
        var source = try ExchangeApiSharded.initFromJavaConfig(
            allocator,
            journaling_cfg,
        );
        defer source.deinit();
        var response = try source.addUser(.routed, 201_101);
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);

        // Preserve a valid command and sequence but falsify its recorded
        // result so replay mutates the kernel before detecting corruption.
        source.journal.?.entries.items[0].result_code =
            .user_mgmt_user_already_exists;
        try source.writeJournalToFile(tmp.dir, "mismatched-result.ecj");
    }

    var replayed = try ExchangeApiSharded.initFromJavaConfig(
        allocator,
        journaling_cfg,
    );
    defer replayed.deinit();
    try std.testing.expectError(
        error.ResultMismatch,
        replayed.replayJournalFromFile(tmp.dir, "mismatched-result.ecj"),
    );
    const fatal = replayed.fatalInfo() orelse return error.MissingFatalRecord;
    try std.testing.expectEqual(error.ResultMismatch, fatal.cause);
    try std.testing.expectEqual(@as(?u64, 1), fatal.sequence);
    try std.testing.expectEqual(cmd.OrderCommandType.add_user, fatal.command);

    // The first attempt already advanced and mutated sequence 1. Both a
    // replay retry and startup must now fail closed instead of skipping it.
    try std.testing.expectError(
        error.CorePoisoned,
        replayed.replayJournalFromFile(tmp.dir, "mismatched-result.ecj"),
    );
    try std.testing.expectError(error.CorePoisoned, replayed.startup());
}

test "sharded Java durable journal predicate matches source mutability" {
    const skipped = [_]cmd.OrderCommandType{
        .order_book_request,
        .binary_data_query,
        .grouping_control,
        .nop,
        .shutdown_signal,
        .reserved_compressed,
    };
    const appended = [_]cmd.OrderCommandType{
        .place_order,
        .cancel_order,
        .move_order,
        .reduce_order,
        .add_user,
        .balance_adjustment,
        .suspend_user,
        .resume_user,
        .binary_data_command,
        .persist_state_matching,
        .persist_state_risk,
        .reset,
    };

    for (skipped) |command_type| {
        try std.testing.expect(!shouldAppendToJavaDurableJournal(command_type));
    }
    for (appended) |command_type| {
        try std.testing.expect(shouldAppendToJavaDurableJournal(command_type));
    }
}

test "sharded settlement hooks require a pristine cold core and native durable journal" {
    const allocator = std.testing.allocator;
    var recorder: SettlementLifecycleTestRecorder = .{};

    var dummy = try ExchangeApiSharded.init(allocator, 1, .direct);
    defer dummy.deinit();
    recorder.api = &dummy;
    try std.testing.expect(!dummy.settlement_hooks.isEnabled());
    try std.testing.expectError(
        error.SettlementHooksRequireNativeDurableJournal,
        dummy.setSettlementHooks(recorder.hooks()),
    );

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var durable_config = Config.javaDefault();
    durable_config.initial_state = config_mod.InitialStateConfig.cleanStart("SHSETHOOKCFG");
    durable_config.matching.num_shards = 1;
    durable_config.sharded_pipeline_mode = .immediate;
    durable_config.serialization = .{
        .enable_journaling = true,
        .backend = .disk,
        .storage_folder = "hook-store",
    };
    var durable = try ExchangeApiSharded.initWithPersistenceRoot(
        allocator,
        tmp.dir,
        durable_config,
    );
    defer durable.deinit();
    recorder.api = &durable;
    try durable.setSettlementHooks(recorder.hooks());
    try std.testing.expect(durable.settlement_hooks.isEnabled());
    try durable.clearSettlementHooks();
    try std.testing.expect(!durable.settlement_hooks.isEnabled());
    try durable.setSettlementHooks(recorder.hooks());

    var response = try durable.nop(.routed);
    defer response.deinit(allocator);
    try std.testing.expectError(
        error.SettlementHooksRequireColdCore,
        durable.clearSettlementHooks(),
    );
}

test "sharded legacy API error sets exclude settlement-only failures" {
    const settlement_error_names = [_][]const u8{
        "SettlementPreparationRejected",
        "SettlementRiskR2MarkRejected",
        "SettlementJournalMarkRejected",
        "SettlementRecoveryRejected",
    };
    inline for (settlement_error_names) |error_name| {
        try std.testing.expect(!apiErrorSetContains(ExecuteError, error_name));
        try std.testing.expect(!apiErrorSetContains(StartError, error_name));
        try std.testing.expect(!apiErrorSetContains(JournalReplayError, error_name));
    }
    try std.testing.expect(
        !apiErrorSetContains(JournalReplayError, "DurableJournalIdentityMismatch"),
    );
}

test "sharded settlement callbacks reject blocking API reentry without holding driver locks" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var config = Config.javaDefault();
    config.initial_state = config_mod.InitialStateConfig.cleanStart("SHSETREENTRY");
    config.matching.num_shards = 2;
    config.sharded_pipeline_mode = .persistent_grouped;
    config.serialization = .{
        .enable_journaling = true,
        .backend = .disk,
        .storage_folder = "reentry-store",
    };
    var api = try ExchangeApiSharded.initWithPersistenceRoot(allocator, tmp.dir, config);
    defer api.deinit();
    var probe: SettlementCallbackReentryProbe = .{
        .api = &api,
        .probe_prepare = true,
        .probe_recovery = true,
    };
    try api.setSettlementHooks(probe.hooks());

    const symbol: i32 = 61_050;
    const maker_uid: i64 = 610_501;
    const taker_uid: i64 = 610_502;
    try api.addSymbolAll(try common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 1,
        .maker_fee = 1,
    }));
    try api.startup();
    try std.testing.expect(!probe.recovery_saw_started.load(.acquire));
    try std.testing.expectEqual(
        @as(u8, @intFromEnum(SettlementCallbackReentryProbe.Outcome.core_reentrant)),
        probe.recovery_outcome.load(.acquire),
    );
    var add_maker = try api.addUser(.routed, maker_uid);
    defer add_maker.deinit(allocator);
    var add_taker = try api.addUser(.split, taker_uid);
    defer add_taker.deinit(allocator);
    var fund_maker = try api.adjustUserBalance(
        .routed,
        maker_uid,
        1,
        11,
        100_000,
        .adjustment,
    );
    defer fund_maker.deinit(allocator);
    var fund_taker = try api.adjustUserBalance(
        .split,
        taker_uid,
        1,
        15,
        100_000,
        .adjustment,
    );
    defer fund_taker.deinit(allocator);
    var maker = try api.placeOrder(
        .routed,
        610_511,
        maker_uid,
        symbol,
        100,
        0,
        2,
        .ask,
        .gtc,
        0,
    );
    defer maker.deinit(allocator);
    var taker = try api.placeOrder(
        .split,
        610_512,
        taker_uid,
        symbol,
        100,
        100,
        2,
        .bid,
        .ioc,
        0,
    );
    defer taker.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, taker.result_code);
    api.shutdown();
    try std.testing.expectEqual(
        @as(u8, @intFromEnum(SettlementCallbackReentryProbe.Outcome.core_reentrant)),
        probe.prepare_outcome.load(.acquire),
    );
}

test "strict discovered sharded startup fails if selected snapshot disappears before restore" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const exchange_id = "SHSETSTRICTGONE";
    const snapshot_id: i64 = 61_075;
    var source_config = Config.javaDefault();
    source_config.initial_state = config_mod.InitialStateConfig.cleanStart(exchange_id);
    source_config.matching.num_shards = 2;
    source_config.sharded_pipeline_mode = .immediate;
    source_config.serialization = .{
        .enable_journaling = true,
        .backend = .disk,
        .storage_folder = "strict-gone-store",
    };
    {
        var source = try ExchangeApiSharded.initWithPersistenceRoot(
            allocator,
            tmp.dir,
            source_config,
        );
        defer source.deinit();
        var add = try source.addUser(.routed, 610_751);
        defer add.deinit(allocator);
        try std.testing.expectEqual(
            cmd.CommandResultCode.success,
            try source.persistStateWithDefaultMainLog(
                tmp.dir,
                snapshot_id,
                9_961_075_001,
                .matching_engine_router,
                0,
            ),
        );
    }

    var target_config = source_config;
    target_config.initial_state = config_mod.InitialStateConfig.fromSnapshotOnly(
        exchange_id,
        @intCast(snapshot_id),
        0,
    );
    var target = try ExchangeApiSharded.initWithPersistenceRoot(
        allocator,
        tmp.dir,
        target_config,
    );
    defer target.deinit();
    var recorder: SettlementLifecycleTestRecorder = .{ .api = &target };
    try target.setSettlementHooks(recorder.hooks());
    target.strict_snapshot_disappeared_after_validation_for_testing = true;

    try std.testing.expectError(
        error.SnapshotFileNotFound,
        target.startupFromDiscoveredConfiguredPersistenceStrictMainLog(
            tmp.dir,
            .matching_engine_router,
            0,
            0,
        ),
    );
    try std.testing.expect(!target.isStarted());
    try std.testing.expect(!target.initial_state_bootstrapped);
    try std.testing.expect(!target.settlement_recovery_finished);
    try std.testing.expectEqual(@as(usize, 0), recorder.recovery_calls);
}

test "sharded immediate settlement marks every durable sequence and publishes only afterward" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var config = Config.javaDefault();
    config.initial_state = config_mod.InitialStateConfig.cleanStart("SHSETLIVE");
    config.matching.num_shards = 4;
    config.sharded_pipeline_mode = .immediate;
    config.serialization = .{
        .enable_journaling = true,
        .backend = .disk,
        .storage_folder = "live-store",
    };
    var api = try ExchangeApiSharded.initWithPersistenceRoot(allocator, tmp.dir, config);
    defer api.deinit();
    var recorder: SettlementLifecycleTestRecorder = .{ .api = &api };
    try api.setSettlementHooks(recorder.hooks());
    var raw_probe: RawR2Probe = .{};
    api.setRawResultSink(.{
        .context = &raw_probe,
        .on_result = RawR2Probe.onResult,
    });

    const symbol: i32 = 61_001;
    const maker_uid: i64 = 611_001;
    const taker_uid: i64 = 611_002;
    const spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 1,
        .maker_fee = 1,
    });
    try api.addSymbolAll(spec);

    var add_maker = try api.addUser(.routed, maker_uid);
    defer add_maker.deinit(allocator);
    var add_taker = try api.addUser(.split, taker_uid);
    defer add_taker.deinit(allocator);
    var fund_maker = try api.adjustUserBalance(
        .routed,
        maker_uid,
        1,
        11,
        100_000,
        .adjustment,
    );
    defer fund_maker.deinit(allocator);
    var fund_taker = try api.adjustUserBalance(
        .split,
        taker_uid,
        1,
        15,
        100_000,
        .adjustment,
    );
    defer fund_taker.deinit(allocator);
    var maker = try api.placeOrder(
        .routed,
        611_101,
        maker_uid,
        symbol,
        100,
        0,
        2,
        .ask,
        .gtc,
        0,
    );
    defer maker.deinit(allocator);
    recorder.resetObservations();
    const raw_before_trade = raw_probe.count;

    var taker = try api.placeOrder(
        .split,
        611_102,
        taker_uid,
        symbol,
        100,
        100,
        2,
        .bid,
        .ioc,
        0,
    );
    defer taker.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, taker.result_code);
    try std.testing.expectEqual(@as(usize, 1), recorder.prepare_calls);
    try std.testing.expectEqual(@as(usize, 1), recorder.last_prepare_trade_count);
    try std.testing.expectEqual(settlement_lifecycle.ExecutionMode.live, recorder.last_prepare_mode);
    try std.testing.expectEqual(@as(usize, 1), recorder.risk_calls);
    try std.testing.expectEqual(@as(usize, 1), recorder.journal_calls);
    try std.testing.expectEqualSlices(
        SettlementLifecycleTestEvent,
        &.{ .prepare, .risk_r2, .journal },
        recorder.events[0..recorder.events_len],
    );
    try std.testing.expect(recorder.journal_marks_followed_native_sync);
    try std.testing.expectEqual(raw_before_trade + 1, raw_probe.count);
    try std.testing.expectEqual(taker.sequence, raw_probe.sequence);

    recorder.resetObservations();
    const before_reset = api.driver.publication_sequencer.highWater();
    var reset_response = try api.reset(.routed);
    defer reset_response.deinit(allocator);
    try std.testing.expectEqual(before_reset + 1, reset_response.sequence);
    try std.testing.expectEqualSlices(
        SettlementLifecycleTestEvent,
        &.{ .risk_r2, .journal },
        recorder.events[0..recorder.events_len],
    );

    recorder.resetObservations();
    const binary_spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol + 1,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
    });
    const before_binary = api.driver.publication_sequencer.highWater();
    var binary_response = try api.submitAddSymbolsBinary(
        .split,
        61_002,
        611_000_001,
        &.{binary_spec},
    );
    defer binary_response.deinit(allocator);
    const binary_sequences = api.driver.publication_sequencer.highWater() - before_binary;
    try std.testing.expect(binary_sequences > 0);
    try std.testing.expectEqual(@as(usize, @intCast(binary_sequences)), recorder.risk_calls);
    try std.testing.expectEqual(@as(usize, @intCast(binary_sequences)), recorder.journal_calls);
    try std.testing.expectEqual(@as(usize, 0), recorder.prepare_calls);
    try std.testing.expect(recorder.journal_marks_followed_native_sync);
    try std.testing.expectEqual(
        @as(?u64, api.driver.publication_sequencer.highWater()),
        api.durable_journal.?.lastSequence(),
    );
}

test "persistent grouped API forwards settlement hooks through durable acknowledgement" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var config = Config.javaDefault();
    config.initial_state = config_mod.InitialStateConfig.cleanStart("SHSETPERSIST");
    config.matching.num_shards = 2;
    config.sharded_pipeline_mode = .persistent_grouped;
    config.serialization = .{
        .enable_journaling = true,
        .backend = .disk,
        .storage_folder = "persistent-store",
    };
    var api = try ExchangeApiSharded.initWithPersistenceRoot(allocator, tmp.dir, config);
    defer api.deinit();
    var recorder: ConcurrentSettlementLifecycleTestRecorder = .{ .api = &api };
    try api.setSettlementHooks(recorder.hooks());

    const symbol: i32 = 61_101;
    const maker_uid: i64 = 611_101;
    const taker_uid: i64 = 611_102;
    try api.addSymbolAll(try common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 1,
        .maker_fee = 1,
    }));
    try api.startup();

    var add_maker = try api.addUser(.routed, maker_uid);
    defer add_maker.deinit(allocator);
    var add_taker = try api.addUser(.split, taker_uid);
    defer add_taker.deinit(allocator);
    var fund_maker = try api.adjustUserBalance(
        .routed,
        maker_uid,
        1,
        11,
        100_000,
        .adjustment,
    );
    defer fund_maker.deinit(allocator);
    var fund_taker = try api.adjustUserBalance(
        .split,
        taker_uid,
        1,
        15,
        100_000,
        .adjustment,
    );
    defer fund_taker.deinit(allocator);
    var maker = try api.placeOrder(
        .routed,
        611_201,
        maker_uid,
        symbol,
        100,
        0,
        2,
        .ask,
        .gtc,
        0,
    );
    defer maker.deinit(allocator);
    var taker = try api.placeOrder(
        .split,
        611_202,
        taker_uid,
        symbol,
        100,
        100,
        2,
        .bid,
        .ioc,
        0,
    );
    defer taker.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, taker.result_code);

    api.shutdown();
    const high_water = api.driver.publication_sequencer.highWater();
    try std.testing.expectEqual(@as(usize, 1), recorder.prepare_calls.load(.acquire));
    try std.testing.expectEqual(
        @as(u8, @intFromEnum(settlement_lifecycle.ExecutionMode.live)),
        recorder.last_prepare_mode.load(.acquire),
    );
    try std.testing.expectEqual(
        @as(usize, @intCast(high_water)),
        recorder.risk_calls.load(.acquire),
    );
    try std.testing.expectEqual(
        @as(usize, @intCast(high_water)),
        recorder.journal_calls.load(.acquire),
    );
    try std.testing.expectEqual(@as(usize, 1), recorder.recovery_calls.load(.acquire));
    try std.testing.expect(recorder.journal_marks_followed_native_sync.load(.acquire));
    try std.testing.expectEqual(@as(?u64, high_water), api.durable_journal.?.lastSequence());
}

test "sharded immediate settlement failures poison at truthful stages without publishing success" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var config = Config.javaDefault();
        config.initial_state = config_mod.InitialStateConfig.cleanStart("SHSETRISKFAIL");
        config.matching.num_shards = 1;
        config.sharded_pipeline_mode = .immediate;
        config.serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = "risk-fail-store",
        };
        var api = try ExchangeApiSharded.initWithPersistenceRoot(allocator, tmp.dir, config);
        defer api.deinit();
        var recorder: SettlementLifecycleTestRecorder = .{
            .api = &api,
            .reject_risk = true,
        };
        try api.setSettlementHooks(recorder.hooks());
        var raw_probe: RawR2Probe = .{};
        api.setRawResultSink(.{
            .context = &raw_probe,
            .on_result = RawR2Probe.onResult,
        });

        try std.testing.expectError(
            error.CorePoisoned,
            api.addUser(.routed, 612_001),
        );
        try std.testing.expectEqual(@as(usize, 1), recorder.risk_calls);
        try std.testing.expectEqual(@as(usize, 0), recorder.journal_calls);
        try std.testing.expectEqual(@as(usize, 0), api.journal.?.entries.items.len);
        try std.testing.expectEqual(@as(?u64, null), api.durable_journal.?.lastSequence());
        try std.testing.expectEqual(@as(usize, 0), raw_probe.count);
        const fatal = api.fatalInfo() orelse return error.MissingFatalRecord;
        try std.testing.expectEqual(runtime_mod.FailureStage.r2, fatal.stage);
        try std.testing.expectEqual(error.SettlementRiskR2MarkRejected, fatal.cause);
    }

    {
        var config = Config.javaDefault();
        config.initial_state = config_mod.InitialStateConfig.cleanStart("SHSETJOURNALFAIL");
        config.matching.num_shards = 1;
        config.sharded_pipeline_mode = .immediate;
        config.serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = "journal-fail-store",
        };
        var api = try ExchangeApiSharded.initWithPersistenceRoot(allocator, tmp.dir, config);
        defer api.deinit();
        var recorder: SettlementLifecycleTestRecorder = .{
            .api = &api,
            .reject_journal = true,
        };
        try api.setSettlementHooks(recorder.hooks());
        var raw_probe: RawR2Probe = .{};
        api.setRawResultSink(.{
            .context = &raw_probe,
            .on_result = RawR2Probe.onResult,
        });

        try std.testing.expectError(
            error.CorePoisoned,
            api.addUser(.split, 612_002),
        );
        try std.testing.expectEqual(@as(usize, 1), recorder.risk_calls);
        try std.testing.expectEqual(@as(usize, 1), recorder.journal_calls);
        try std.testing.expect(recorder.journal_marks_followed_native_sync);
        try std.testing.expectEqual(@as(usize, 1), api.journal.?.entries.items.len);
        try std.testing.expectEqual(@as(?u64, 1), api.durable_journal.?.lastSequence());
        try std.testing.expectEqual(@as(usize, 0), raw_probe.count);
        const fatal = api.fatalInfo() orelse return error.MissingFatalRecord;
        try std.testing.expectEqual(runtime_mod.FailureStage.journal, fatal.stage);
        try std.testing.expectEqual(error.SettlementJournalMarkRejected, fatal.cause);
    }

    {
        var config = Config.javaDefault();
        config.initial_state = config_mod.InitialStateConfig.cleanStart("SHSETPREPFAIL");
        config.matching.num_shards = 2;
        config.sharded_pipeline_mode = .immediate;
        config.serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = "prepare-fail-store",
        };
        var api = try ExchangeApiSharded.initWithPersistenceRoot(allocator, tmp.dir, config);
        defer api.deinit();
        var recorder: SettlementLifecycleTestRecorder = .{ .api = &api };
        try api.setSettlementHooks(recorder.hooks());
        var raw_probe: RawR2Probe = .{};
        api.setRawResultSink(.{
            .context = &raw_probe,
            .on_result = RawR2Probe.onResult,
        });

        const symbol: i32 = 61_201;
        const maker_uid: i64 = 612_101;
        const taker_uid: i64 = 612_102;
        try api.addSymbolAll(try common.CoreSymbolSpecification.init(.{
            .symbol_id = symbol,
            .symbol_type = .currency_exchange_pair,
            .base_currency = 11,
            .quote_currency = 15,
            .base_scale_k = 1,
            .quote_scale_k = 1,
            .taker_fee = 1,
            .maker_fee = 1,
        }));
        var add_maker = try api.addUser(.routed, maker_uid);
        defer add_maker.deinit(allocator);
        var add_taker = try api.addUser(.split, taker_uid);
        defer add_taker.deinit(allocator);
        var fund_maker = try api.adjustUserBalance(
            .routed,
            maker_uid,
            1,
            11,
            100_000,
            .adjustment,
        );
        defer fund_maker.deinit(allocator);
        var fund_taker = try api.adjustUserBalance(
            .split,
            taker_uid,
            1,
            15,
            100_000,
            .adjustment,
        );
        defer fund_taker.deinit(allocator);
        var maker = try api.placeOrder(
            .routed,
            612_201,
            maker_uid,
            symbol,
            100,
            0,
            2,
            .ask,
            .gtc,
            0,
        );
        defer maker.deinit(allocator);
        const durable_before = api.durable_journal.?.lastSequence();
        const raw_before = raw_probe.count;
        recorder.resetObservations();
        recorder.reject_prepare = true;

        try std.testing.expectError(
            error.CorePoisoned,
            api.placeOrder(
                .split,
                612_202,
                taker_uid,
                symbol,
                100,
                100,
                2,
                .bid,
                .ioc,
                0,
            ),
        );
        try std.testing.expectEqual(@as(usize, 1), recorder.prepare_calls);
        try std.testing.expectEqual(@as(usize, 0), recorder.risk_calls);
        try std.testing.expectEqual(@as(usize, 0), recorder.journal_calls);
        try std.testing.expectEqual(durable_before, api.durable_journal.?.lastSequence());
        try std.testing.expectEqual(raw_before, raw_probe.count);
        const fatal = api.fatalInfo() orelse return error.MissingFatalRecord;
        try std.testing.expectEqual(runtime_mod.FailureStage.r2, fatal.stage);
        try std.testing.expectEqual(error.SettlementPreparationRejected, fatal.cause);
    }

    {
        var config = Config.javaDefault();
        config.initial_state = config_mod.InitialStateConfig.cleanStart("SHSETRECFAIL");
        config.matching.num_shards = 1;
        config.sharded_pipeline_mode = .persistent_grouped;
        config.serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = "recovery-fail-store",
        };
        var api = try ExchangeApiSharded.initWithPersistenceRoot(allocator, tmp.dir, config);
        defer api.deinit();
        var recorder: SettlementLifecycleTestRecorder = .{
            .api = &api,
            .reject_recovery = true,
        };
        try api.setSettlementHooks(recorder.hooks());

        try std.testing.expectError(error.CorePoisoned, api.startup());
        try std.testing.expectEqual(@as(usize, 1), recorder.recovery_calls);
        try std.testing.expect(!api.settlement_recovery_finished);
        try std.testing.expect(api.persistent_grouped_runtime == null);
        const fatal = api.fatalInfo() orelse return error.MissingFatalRecord;
        try std.testing.expectEqual(runtime_mod.FailureStage.startup, fatal.stage);
        try std.testing.expectEqual(error.SettlementRecoveryRejected, fatal.cause);
    }
}

test "sharded settlement replay prepares while fenced and full bootstrap finishes recovery once" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var source_config = Config.javaDefault();
    source_config.initial_state = config_mod.InitialStateConfig.cleanStart("SHSETRECOVERY");
    source_config.matching.num_shards = 2;
    source_config.sharded_pipeline_mode = .immediate;
    source_config.serialization = .{
        .enable_journaling = true,
        .backend = .disk,
        .storage_folder = "recovery-store",
    };
    const native_path = try nativeJournalPartitionPath(allocator, &source_config);
    defer allocator.free(native_path);
    const symbol: i32 = 61_301;
    const maker_uid: i64 = 613_001;
    const taker_uid: i64 = 613_002;
    const specification = try common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 1,
        .maker_fee = 1,
    });

    var durable_frontier: u64 = 0;
    {
        var source = try ExchangeApiSharded.initWithPersistenceRoot(
            allocator,
            tmp.dir,
            source_config,
        );
        defer source.deinit();
        var add_symbols = try source.submitAddSymbolsBinary(
            .routed,
            61_301,
            613_000_001,
            &.{specification},
        );
        defer add_symbols.deinit(allocator);
        var add_maker = try source.addUser(.routed, maker_uid);
        defer add_maker.deinit(allocator);
        var add_taker = try source.addUser(.split, taker_uid);
        defer add_taker.deinit(allocator);
        var fund_maker = try source.adjustUserBalance(
            .routed,
            maker_uid,
            1,
            11,
            100_000,
            .adjustment,
        );
        defer fund_maker.deinit(allocator);
        var fund_taker = try source.adjustUserBalance(
            .split,
            taker_uid,
            1,
            15,
            100_000,
            .adjustment,
        );
        defer fund_taker.deinit(allocator);
        var maker = try source.placeOrder(
            .routed,
            613_101,
            maker_uid,
            symbol,
            100,
            0,
            2,
            .ask,
            .gtc,
            0,
        );
        defer maker.deinit(allocator);
        var taker = try source.placeOrder(
            .split,
            613_102,
            taker_uid,
            symbol,
            100,
            100,
            2,
            .bid,
            .ioc,
            0,
        );
        defer taker.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, taker.result_code);
        durable_frontier = source.driver.publication_sequencer.highWater();
        try std.testing.expectEqual(
            @as(?u64, durable_frontier),
            source.durable_journal.?.lastSequence(),
        );
    }

    var restored_config = source_config;
    restored_config.initial_state = config_mod.InitialStateConfig.lastKnownStateFromJournal(
        source_config.initial_state.exchange_id,
        0,
        0,
    );
    {
        var standalone = try ExchangeApiSharded.initWithPersistenceRoot(
            allocator,
            tmp.dir,
            restored_config,
        );
        defer standalone.deinit();
        var recorder: SettlementLifecycleTestRecorder = .{ .api = &standalone };
        try standalone.setSettlementHooks(recorder.hooks());
        _ = try standalone.replayJournalFromFile(tmp.dir, native_path);
        try std.testing.expectEqual(@as(usize, 1), recorder.prepare_calls);
        try std.testing.expectEqual(settlement_lifecycle.ExecutionMode.replay, recorder.last_prepare_mode);
        try std.testing.expectEqual(@as(usize, 0), recorder.risk_calls);
        try std.testing.expectEqual(@as(usize, 0), recorder.journal_calls);
        try std.testing.expectEqual(@as(usize, 0), recorder.recovery_calls);
        try std.testing.expect(!standalone.settlement_recovery_finished);
    }

    {
        var restored = try ExchangeApiSharded.initWithPersistenceRoot(
            allocator,
            tmp.dir,
            restored_config,
        );
        defer restored.deinit();
        var recorder: SettlementLifecycleTestRecorder = .{ .api = &restored };
        try restored.setSettlementHooks(recorder.hooks());
        var raw_probe: RawR2Probe = .{};
        restored.setRawResultSink(.{
            .context = &raw_probe,
            .on_result = RawR2Probe.onResult,
        });

        const outcome = try restored.bootstrapFromPersistence(tmp.dir, null, native_path);
        try std.testing.expect(outcome.journal_replayed_commands > 0);
        try std.testing.expectEqual(durable_frontier, restored.core.kernels.next_seq);
        try std.testing.expectEqual(@as(usize, 1), recorder.prepare_calls);
        try std.testing.expectEqual(settlement_lifecycle.ExecutionMode.replay, recorder.last_prepare_mode);
        try std.testing.expectEqual(@as(usize, 0), recorder.risk_calls);
        try std.testing.expectEqual(@as(usize, 0), recorder.journal_calls);
        try std.testing.expectEqual(@as(usize, 1), recorder.recovery_calls);
        try std.testing.expectEqual(durable_frontier, recorder.last_sequence);
        try std.testing.expect(recorder.recovery_saw_validated_frontier);
        try std.testing.expect(restored.settlement_recovery_finished);
        try std.testing.expectEqualSlices(
            SettlementLifecycleTestEvent,
            &.{ .prepare, .recovery },
            recorder.events[0..recorder.events_len],
        );
        try std.testing.expectEqual(@as(usize, 0), raw_probe.count);
        try std.testing.expectError(
            error.SettlementHooksRequireColdCore,
            restored.clearSettlementHooks(),
        );

        try restored.startup();
        try std.testing.expectEqual(@as(usize, 1), recorder.recovery_calls);
        var next = try restored.nop(.routed);
        defer next.deinit(allocator);
        try std.testing.expectEqual(durable_frontier + 1, next.sequence);
        try std.testing.expectEqual(@as(usize, 1), recorder.risk_calls);
        try std.testing.expectEqual(@as(usize, 1), recorder.journal_calls);
        try std.testing.expectEqual(@as(usize, 1), raw_probe.count);
    }
}

test "sharded settlement replay binds the full native XCJ3 content identity before mutation" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var authoritative_config = Config.javaDefault();
    authoritative_config.initial_state = config_mod.InitialStateConfig.cleanStart("SHSETIDENTA");
    authoritative_config.matching.num_shards = 2;
    authoritative_config.sharded_pipeline_mode = .immediate;
    authoritative_config.serialization = .{
        .enable_journaling = true,
        .backend = .disk,
        .storage_folder = "identity-store",
    };
    const authoritative_path = try nativeJournalPartitionPath(allocator, &authoritative_config);
    defer allocator.free(authoritative_path);
    {
        var source = try ExchangeApiSharded.initWithPersistenceRoot(
            allocator,
            tmp.dir,
            authoritative_config,
        );
        defer source.deinit();
        var response = try source.addUser(.routed, 614_001);
        defer response.deinit(allocator);
    }

    var alternate_config = authoritative_config;
    alternate_config.initial_state = config_mod.InitialStateConfig.cleanStart("SHSETIDENTB");
    const alternate_path = try nativeJournalPartitionPath(allocator, &alternate_config);
    defer allocator.free(alternate_path);
    {
        var source = try ExchangeApiSharded.initWithPersistenceRoot(
            allocator,
            tmp.dir,
            alternate_config,
        );
        defer source.deinit();
        var response = try source.addUser(.routed, 614_002);
        defer response.deinit(allocator);
    }

    const copy_path = "identity-store/authoritative-copy.xcj3";
    {
        const input = try tmp.dir.openFile(io_context.current(), authoritative_path, .{});
        defer input.close(io_context.current());
        const bytes = try io_context.readToEndAlloc(input, allocator, 1024 * 1024);
        defer allocator.free(bytes);
        const output = try tmp.dir.createFile(io_context.current(), copy_path, .{ .truncate = true });
        defer output.close(io_context.current());
        try output.writeStreamingAll(io_context.current(), bytes);
        try output.sync(io_context.current());
    }

    var restored_config = authoritative_config;
    restored_config.initial_state = config_mod.InitialStateConfig.lastKnownStateFromJournal(
        authoritative_config.initial_state.exchange_id,
        0,
        0,
    );
    {
        var configured = try ExchangeApiSharded.initWithPersistenceRoot(
            allocator,
            tmp.dir,
            restored_config,
        );
        defer configured.deinit();
        var configured_recorder: SettlementLifecycleTestRecorder = .{ .api = &configured };
        try configured.setSettlementHooks(configured_recorder.hooks());
        const configured_outcome = try configured.startupFromConfiguredPersistence(
            tmp.dir,
            .matching_engine_router,
            0,
            0,
        );
        try std.testing.expectEqual(@as(usize, 1), configured_outcome.journal_replayed_commands);
        try std.testing.expect(configured.isStarted());
        try std.testing.expectEqual(@as(usize, 1), configured_recorder.recovery_calls);
    }

    var restored = try ExchangeApiSharded.initWithPersistenceRoot(
        allocator,
        tmp.dir,
        restored_config,
    );
    defer restored.deinit();
    var recorder: SettlementLifecycleTestRecorder = .{ .api = &restored };
    try restored.setSettlementHooks(recorder.hooks());

    try std.testing.expectError(
        error.JournalEntryMismatch,
        restored.bootstrapFromPersistence(tmp.dir, null, alternate_path),
    );
    try std.testing.expectEqual(@as(u64, 0), restored.core.kernels.next_seq);
    try std.testing.expectEqual(@as(u64, 0), restored.driver.publication_sequencer.highWater());
    try std.testing.expectEqual(@as(usize, 0), recorder.prepare_calls);
    try std.testing.expectEqual(@as(usize, 0), recorder.recovery_calls);
    try std.testing.expect(restored.fatalInfo() == null);

    const outcome = try restored.bootstrapFromPersistence(tmp.dir, null, copy_path);
    try std.testing.expectEqual(@as(usize, 1), outcome.journal_replayed_commands);
    try std.testing.expectEqual(@as(u64, 1), restored.core.kernels.next_seq);
    try std.testing.expectEqual(@as(usize, 1), recorder.recovery_calls);
    const restored_shard = restored.core.shardIndexForUid(614_001);
    try std.testing.expect(
        restored.core.kernels.kernels[restored_shard].risk_engine.getUserProfile(614_001) != null,
    );
}

test "sharded durable checkpoint preserves a trailing skipped sequence across reopen" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var config = Config.javaDefault();
    config.initial_state = config_mod.InitialStateConfig.cleanStart("SHDURTRAILING");
    config.matching.num_shards = 4;
    config.serialization = .{
        .enable_journaling = true,
        .backend = .disk,
        .storage_folder = "store",
    };
    const native_path = try nativeJournalPartitionPath(allocator, &config);
    defer allocator.free(native_path);

    {
        var source = try ExchangeApiSharded.initWithPersistenceRoot(
            allocator,
            tmp.dir,
            config,
        );
        defer source.deinit();
        var first = try source.addUser(.routed, 501_901);
        defer first.deinit(allocator);
        var nop_command = makeNopCommand();
        var trailing = try source.executeWithMode(.split, &nop_command);
        defer trailing.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 1), first.sequence);
        try std.testing.expectEqual(@as(u64, 2), trailing.sequence);
        try std.testing.expectEqual(@as(?u64, 2), source.durable_journal.?.lastSequence());
    }

    var restored_config = config;
    restored_config.initial_state = config_mod.InitialStateConfig.lastKnownStateFromJournal(
        config.initial_state.exchange_id,
        0,
        0,
    );
    var restored = try ExchangeApiSharded.initWithPersistenceRoot(
        allocator,
        tmp.dir,
        restored_config,
    );
    defer restored.deinit();
    var raw_probe: RawR2Probe = .{};
    restored.setRawResultSink(.{
        .context = &raw_probe,
        .on_result = RawR2Probe.onResult,
    });
    const outcome = try restored.bootstrapFromPersistence(tmp.dir, null, native_path);
    try std.testing.expectEqual(@as(usize, 1), outcome.journal_replayed_commands);
    try std.testing.expectEqual(@as(u64, 2), restored.core.kernels.next_seq);
    try std.testing.expectEqual(@as(u64, 2), restored.driver.publication_sequencer.highWater());
    try std.testing.expectEqual(@as(usize, 0), raw_probe.count);
    try restored.startup();
    try std.testing.expectEqual(@as(usize, 0), raw_probe.count);
    var next = try restored.addUser(.routed, 501_902);
    defer next.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 3), next.sequence);
    try std.testing.expectEqual(@as(usize, 1), raw_probe.count);
    try std.testing.expectEqual(@as(u64, 3), raw_probe.sequence);
}

test "sharded durable XCJ3 replays routed and split commands across a query gap" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var config = Config.javaDefault();
    config.initial_state = config_mod.InitialStateConfig.cleanStart("SHDURRESTART");
    config.matching.num_shards = 4;
    config.serialization = .{
        .enable_journaling = true,
        .backend = .disk,
        .storage_folder = "store",
    };
    const native_path = try nativeJournalPartitionPath(allocator, &config);
    defer allocator.free(native_path);

    const uid: i64 = 501_001;
    {
        var source = try ExchangeApiSharded.initWithPersistenceRoot(
            allocator,
            tmp.dir,
            config,
        );
        defer source.deinit();

        var add = try source.addUser(.routed, uid);
        defer add.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 1), add.sequence);
        try std.testing.expectEqual(cmd.CommandResultCode.success, add.result_code);

        var nop_command = makeNopCommand();
        var nop = try source.executeWithMode(.split, &nop_command);
        defer nop.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 2), nop.sequence);
        try std.testing.expectEqual(cmd.CommandResultCode.success, nop.result_code);

        var balance = try source.adjustUserBalance(
            .split,
            uid,
            1,
            11,
            25_000,
            .adjustment,
        );
        defer balance.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 3), balance.sequence);
        try std.testing.expectEqual(cmd.CommandResultCode.success, balance.result_code);

        var duplicate = try source.addUser(.routed, uid);
        defer duplicate.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 4), duplicate.sequence);
        try std.testing.expectEqual(
            cmd.CommandResultCode.user_mgmt_user_already_exists,
            duplicate.result_code,
        );
        try std.testing.expectEqual(@as(?u64, 4), source.durable_journal.?.lastSequence());
        try std.testing.expectEqual(@as(usize, 4), source.journal.?.entries.items.len);
    }

    {
        var durable = try journal_mod.CommandJournal.readFromFile(
            allocator,
            tmp.dir,
            native_path,
        );
        defer durable.deinit();
        try std.testing.expectEqual(@as(usize, 3), durable.entries.items.len);
        try std.testing.expectEqual(@as(u64, 1), durable.entries.items[0].sequence);
        try std.testing.expectEqual(journal_mod.ExecutionMode.routed, durable.entries.items[0].mode);
        try std.testing.expectEqual(@as(u64, 3), durable.entries.items[1].sequence);
        try std.testing.expectEqual(journal_mod.ExecutionMode.split, durable.entries.items[1].mode);
        try std.testing.expectEqual(@as(u64, 4), durable.entries.items[2].sequence);
        try std.testing.expectEqual(
            cmd.CommandResultCode.user_mgmt_user_already_exists,
            durable.entries.items[2].result_code,
        );
        for (durable.entries.items) |entry| {
            try std.testing.expect(shouldAppendToJavaDurableJournal(entry.input.command));
        }
    }

    var restored_config = config;
    restored_config.initial_state = config_mod.InitialStateConfig.lastKnownStateFromJournal(
        config.initial_state.exchange_id,
        0,
        0,
    );
    {
        var restored = try ExchangeApiSharded.initWithPersistenceRoot(
            allocator,
            tmp.dir,
            restored_config,
        );
        defer restored.deinit();

        const outcome = try restored.bootstrapFromPersistence(
            tmp.dir,
            null,
            native_path,
        );
        try std.testing.expectEqual(@as(usize, 3), outcome.journal_replayed_commands);
        try std.testing.expectEqual(@as(u64, 4), restored.driver.publication_sequencer.highWater());
        try std.testing.expectEqual(@as(?i64, 25_000), try restored.queryUserBalance(uid, 11));

        try restored.startup();
        var next = try restored.addUser(.split, uid + 1);
        defer next.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 5), next.sequence);
        try std.testing.expectEqual(cmd.CommandResultCode.success, next.result_code);
        try std.testing.expectEqual(@as(?u64, 5), restored.durable_journal.?.lastSequence());
    }
}

test "sharded committed snapshot adopts a journal record lost before append" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const snapshot_id: u64 = 501_951;

    var config = Config.javaDefault();
    config.initial_state = config_mod.InitialStateConfig.cleanStart("SHDURADOPT");
    config.matching.num_shards = 4;
    config.serialization = .{
        .enable_journaling = true,
        .backend = .disk,
        .storage_folder = "store",
    };
    const native_path = try nativeJournalPartitionPath(allocator, &config);
    defer allocator.free(native_path);

    {
        var source = try ExchangeApiSharded.initWithPersistenceRoot(
            allocator,
            tmp.dir,
            config,
        );
        defer source.deinit();
        var first = try source.addUser(.split, 501_952);
        defer first.deinit(allocator);
        source.durable_journal.?.injectPreWriteFailureAtSequenceForTesting(3);
        try std.testing.expectError(
            error.InjectedJournalIoFailure,
            source.persistState(.split, @intCast(snapshot_id), 10_000_300_001),
        );
        try std.testing.expect(source.isStopped());
    }

    {
        var before_adoption = try journal_mod.CommandJournal.readFromFile(
            allocator,
            tmp.dir,
            native_path,
        );
        defer before_adoption.deinit();
        try std.testing.expectEqual(@as(usize, 2), before_adoption.entries.items.len);
        try std.testing.expectEqual(@as(usize, 0), before_adoption.frontier_checkpoints.items.len);
    }

    var restored_config = config;
    restored_config.initial_state = config_mod.InitialStateConfig.fromSnapshotOnly(
        config.initial_state.exchange_id,
        snapshot_id,
        0,
    );
    var restored = try ExchangeApiSharded.initWithPersistenceRoot(
        allocator,
        tmp.dir,
        restored_config,
    );
    defer restored.deinit();
    const outcome = try restored.startupFromConfiguredPersistence(
        tmp.dir,
        .matching_engine_router,
        0,
        0,
    );
    try std.testing.expect(outcome.snapshot_found);
    try std.testing.expectEqual(@as(u64, 3), restored.core.kernels.next_seq);
    try std.testing.expectEqual(@as(?u64, 3), restored.durable_journal.?.lastSequence());
    var next = try restored.addUser(.routed, 501_953);
    defer next.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 4), next.sequence);

    var adopted = try journal_mod.CommandJournal.readFromFile(allocator, tmp.dir, native_path);
    defer adopted.deinit();
    try std.testing.expectEqual(@as(usize, 3), adopted.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), adopted.frontier_checkpoints.items.len);
    try std.testing.expectEqual(@as(u64, 3), adopted.frontier_checkpoints.items[0].sequence);
}

test "sharded durable append failure poisons direct and settles callback suffix" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var direct_config = Config.javaDefault();
    direct_config.matching.num_shards = 4;
    direct_config.initial_state = config_mod.InitialStateConfig.cleanStart("SHDURDIRECTFAIL");
    direct_config.serialization = .{
        .enable_journaling = true,
        .backend = .disk,
        .storage_folder = "direct-store",
    };
    {
        var api = try ExchangeApiSharded.initWithPersistenceRoot(
            allocator,
            tmp.dir,
            direct_config,
        );
        defer api.deinit();
        api.durable_journal.?.injectSyncFailureForTesting();
        try std.testing.expectError(error.InjectedJournalIoFailure, api.addUser(.routed, 502_001));
        try std.testing.expect(api.durable_journal.?.poisoned);
        const fatal = api.fatalInfo() orelse return error.MissingFatalRecord;
        try std.testing.expectEqual(error.InjectedJournalIoFailure, fatal.cause);
        try std.testing.expectEqual(@as(?u64, 1), fatal.sequence);
        try std.testing.expectError(error.CorePoisoned, api.addUser(.split, 502_002));
    }

    var callback_config = direct_config;
    callback_config.initial_state = config_mod.InitialStateConfig.cleanStart("SHDURCALLBACKFAIL");
    callback_config.serialization.storage_folder = "callback-store";
    {
        var api = try ExchangeApiSharded.initWithPersistenceRoot(
            allocator,
            tmp.dir,
            callback_config,
        );
        defer api.deinit();
        var probe: FatalDrainBarrierProbe = .{
            .api = &api,
            .io = api.driver.io,
        };
        defer if (!probe.release.isSet()) probe.release.set(probe.io);
        const callback: CommandResponseCallback = .{
            .context = &probe,
            .callback = FatalDrainBarrierProbe.onResponse,
            .failure_callback = FatalDrainBarrierProbe.onFailure,
        };
        api.durable_journal.?.injectSyncFailureForTesting();
        try api.createUserWithCallback(.routed, 502_101, callback);
        try api.createUserWithCallback(.split, 502_102, callback);
        try api.submitCommand(.routed, .{ .add_user = 502_103 });
        const skipped_poll_id = try api.addUserAsync(.split, 502_104);
        const async_drained_before = api.driver.async_drained_total;

        var first_drain: ShardedAcceptedDrainRunner = .{
            .api = &api,
            .io = api.driver.io,
        };
        const first_thread = try std.Thread.spawn(
            .{},
            ShardedAcceptedDrainRunner.run,
            .{&first_drain},
        );
        defer {
            if (!probe.release.isSet()) probe.release.set(probe.io);
            first_thread.join();
        }
        try waitForTestEvent(&probe.entered, probe.io);
        try waitForTestEvent(&probe.reentrant_finished, probe.io);
        const reentrant_result = probe.reentrant_result orelse
            return error.MissingReentrantDrainResult;
        try std.testing.expectError(error.CoreReentrantCall, reentrant_result);

        var second_drain: ShardedAcceptedDrainRunner = .{
            .api = &api,
            .io = api.driver.io,
        };
        const second_thread = try std.Thread.spawn(
            .{},
            ShardedAcceptedDrainRunner.run,
            .{&second_drain},
        );
        defer {
            if (!probe.release.isSet()) probe.release.set(probe.io);
            second_thread.join();
        }
        try expectAcceptedDrainBlocked(&api, &second_drain);
        try std.testing.expect(!first_drain.finished.isSet());

        probe.release.set(probe.io);
        try waitForTestEvent(&first_drain.finished, first_drain.io);
        try waitForTestEvent(&second_drain.finished, second_drain.io);
        const first_result = first_drain.result orelse return error.MissingDrainResult;
        const second_result = second_drain.result orelse return error.MissingDrainResult;
        try std.testing.expectError(error.CorePoisoned, first_result);
        try std.testing.expectError(error.CorePoisoned, second_result);

        try std.testing.expectEqual(@as(usize, 0), probe.records.response_count);
        try std.testing.expectEqual(@as(usize, 2), probe.records.failure_count);
        try std.testing.expect(probe.records.all_failure_callbacks_scoped);
        try std.testing.expectEqual(
            CallbackFailureKind.command_failed,
            probe.records.kinds[0],
        );
        try std.testing.expectEqual(
            CallbackFailureKind.skipped_after_fatal,
            probe.records.kinds[1],
        );
        try std.testing.expectEqual(
            error.InjectedJournalIoFailure,
            probe.records.causes[0],
        );
        try std.testing.expectEqual(
            error.InjectedJournalIoFailure,
            probe.records.causes[1],
        );
        try std.testing.expectEqual(@as(usize, 0), api.pendingCallbackCount());
        try std.testing.expectEqual(@as(usize, 0), api.pendingCount());
        try std.testing.expectEqual(@as(usize, 0), api.driver.completion_slots_used);
        try std.testing.expectEqual(@as(usize, 0), api.driver.journal_slots_reserved);
        try std.testing.expectEqual(api.driver.accepted_total, api.driver.completed_total);
        try std.testing.expectEqual(
            api.driver.async_accepted_total,
            api.driver.async_completed_total,
        );
        try std.testing.expectEqual(async_drained_before, api.driver.async_drained_total);
        try std.testing.expectEqual(@as(usize, 0), api.driver.accepted_drain_waiters);
        try std.testing.expect(!api.isResultReady(skipped_poll_id));
        try std.testing.expect(!probe.release_wait_failed.load(.acquire));
        const failed_shard = api.core.shardIndexForUid(502_101);
        try std.testing.expect(
            api.core.kernels.kernels[failed_shard].risk_engine.getUserProfile(502_101) != null,
        );
        for ([_]i64{ 502_102, 502_103, 502_104 }) |uid| {
            const shard = api.core.shardIndexForUid(uid);
            try std.testing.expect(
                api.core.kernels.kernels[shard].risk_engine.getUserProfile(uid) == null,
            );
        }
        try std.testing.expect(api.durable_journal.?.poisoned);
    }
}

test "sharded durable journal refuses incompatible files and invalid frontiers" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var bad_config = Config.javaDefault();
    bad_config.matching.num_shards = 4;
    bad_config.initial_state = config_mod.InitialStateConfig.cleanStart("SHDURBADFILE");
    bad_config.serialization = .{
        .enable_journaling = true,
        .backend = .disk,
        .storage_folder = "bad-store",
    };
    try tmp.dir.createDirPath(io_context.current(), bad_config.serialization.storage_folder);
    const bad_path = try nativeJournalPartitionPath(allocator, &bad_config);
    defer allocator.free(bad_path);
    {
        const incompatible = try tmp.dir.createFile(
            io_context.current(),
            bad_path,
            .{ .truncate = true },
        );
        defer incompatible.close(io_context.current());
        try incompatible.writeStreamingAll(io_context.current(), "XCJ2");
    }
    try std.testing.expectError(
        error.InvalidMagic,
        ExchangeApiSharded.initWithPersistenceRoot(allocator, tmp.dir, bad_config),
    );

    var ahead_config = bad_config;
    ahead_config.initial_state = config_mod.InitialStateConfig.cleanStart("SHDURAHEAD");
    ahead_config.serialization.storage_folder = "ahead-store";
    {
        var api = try ExchangeApiSharded.initWithPersistenceRoot(
            allocator,
            tmp.dir,
            ahead_config,
        );
        defer api.deinit();
        try api.durable_journal.?.append(.{
            .sequence = 1,
            .input = makeAddUserCommand(503_001),
            .result_code = .success,
        });
        try std.testing.expectError(error.DurableJournalFrontierMismatch, api.startup());
    }

    var suffix_config = bad_config;
    suffix_config.initial_state = config_mod.InitialStateConfig.cleanStart("SHDURSUFFIX");
    suffix_config.serialization.storage_folder = "suffix-store";
    {
        var api = try ExchangeApiSharded.initWithPersistenceRoot(
            allocator,
            tmp.dir,
            suffix_config,
        );
        defer api.deinit();
        var first = try api.addUser(.routed, 503_101);
        defer first.deinit(allocator);
        var nop_command = makeNopCommand();
        var suffix = try api.executeWithMode(.split, &nop_command);
        defer suffix.deinit(allocator);
        try std.testing.expectEqual(@as(?u64, 2), api.durable_journal.?.lastSequence());
        try api.startup();
    }
}

test "sharded journal rejects malformed execution mode before mutation" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const journaling_cfg = config_mod.JavaExchangeConfig{
        .init_state_cfg = .{
            .exchange_id = "ECJMODE",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = std.math.maxInt(u64),
            .throw_if_snapshot_not_found = false,
        },
        .serialization_cfg = config_mod.SerializationConfig.diskJournaling(),
    };
    var api = try ExchangeApiSharded.initFromJavaConfig(allocator, journaling_cfg);
    defer api.deinit();

    {
        const file = try tmp.dir.createFile(io_context.current(), "bad-mode.ecj", .{ .truncate = true });
        defer file.close(io_context.current());
        try file.writeStreamingAll(io_context.current(), "XCJ2\x01\x00\x00\x00\x00\x00\x00\x00\xFF");
    }

    const before_hash = try api.detailedHashFingerprint();
    try std.testing.expectError(
        error.InvalidExecutionMode,
        api.replayJournalFromFile(tmp.dir, "bad-mode.ecj"),
    );
    try std.testing.expectEqual(before_hash, try api.detailedHashFingerprint());
    try std.testing.expectEqual(@as(u64, 0), api.driver.publication_sequencer.highWater());
    try std.testing.expectEqual(@as(u64, 0), api.core.kernels.next_seq);
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

test "sharded raw observer borrows matcher L2 and report data before poll publication" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    _ = try runBasicFlow(&api, .routed);

    var first_ask = try api.placeOrder(
        .routed,
        131_001,
        11_001,
        241,
        101,
        101,
        3,
        .ask,
        .gtc,
        0,
    );
    first_ask.deinit(allocator);
    var second_ask = try api.placeOrder(
        .routed,
        131_002,
        11_001,
        241,
        102,
        102,
        2,
        .ask,
        .gtc,
        0,
    );
    second_ask.deinit(allocator);

    var probe: RawShapeProbe = .{ .api = &api };
    api.setRawResultSink(.{
        .context = &probe,
        .on_result = RawShapeProbe.onResult,
    });

    const cancel_request = try api.cancelOrderAsync(.routed, 131_001, 11_001, 241);
    probe.current_request_id = cancel_request;
    try std.testing.expectEqual(@as(usize, 1), try api.drainPending());
    var cancel = api.takeResult(cancel_request) orelse return error.MissingAsyncResult;
    cancel.deinit(allocator);

    const l2_request = try api.orderBookRequestAsync(.routed, 241, 8);
    probe.current_request_id = l2_request;
    try std.testing.expectEqual(@as(usize, 1), try api.drainPending());
    var l2 = api.takeResult(l2_request) orelse return error.MissingAsyncResult;
    l2.deinit(allocator);

    const report_request = try api.requestSingleUserReportAsync(
        .routed,
        131_003,
        9_979_200_001,
        11_001,
    );
    probe.current_request_id = report_request;
    const report_commands = try api.drainPending();
    try std.testing.expect(report_commands > 0);
    var report = api.takeResult(report_request) orelse return error.MissingAsyncResult;
    report.deinit(allocator);

    try std.testing.expect(probe.callback_count >= 3);
    try std.testing.expect(probe.matcher_seen);
    try std.testing.expect(probe.completed_command_metadata_seen);
    try std.testing.expect(probe.l2_seen);
    try std.testing.expect(probe.report_seen);
    try std.testing.expect(probe.java_report_seen);
    try std.testing.expect(!probe.poll_was_ready);
}

test "sharded raw observer precedes runtime simple-events and request callbacks" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    var probe: ObserverOrderProbe = .{};
    api.setRawResultSink(.{
        .context = &probe,
        .on_result = ObserverOrderProbe.raw,
    });
    api.setResultSink(.{
        .context = &probe,
        .on_result = ObserverOrderProbe.runtime,
    });
    api.setSimpleEventsHandler(.{
        .context = &probe,
        .on_command_result = ObserverOrderProbe.simple,
    });

    try api.enqueueRuntime(makeAddUserCommand(131_010));
    _ = try api.drainRuntime();
    try std.testing.expectEqualSlices(
        ObserverOrder,
        &.{ .raw, .runtime, .simple },
        probe.events[0..probe.len],
    );

    probe.clear();
    try api.createUserWithCallback(.routed, 131_011, .{
        .context = &probe,
        .callback = ObserverOrderProbe.request,
    });
    try std.testing.expectEqual(@as(usize, 1), try api.drainPending());
    try std.testing.expectEqualSlices(
        ObserverOrder,
        &.{ .raw, .simple, .request },
        probe.events[0..probe.len],
    );
    try std.testing.expect(probe.all_callbacks_scoped);
    try std.testing.expect(!runtime_mod.isCallbackExecutionActive());
}

test "single and sharded callbacks avoid crossed callback reconfiguration ABBA" {
    const allocator = std.testing.allocator;

    {
        var target_gate: CrossBackendCallbackGate = .{ .io = std.testing.io };
        var target = try ExchangeApiSharded.init(allocator, 4, .direct);
        defer target.deinit();
        target.setRawResultSink(.{
            .context = &target_gate,
            .on_result = CrossBackendCallbackGate.holdRaw,
        });
        try target.startup();
        const target_request = try target.addUserAsync(.routed, 131_012);
        try target_gate.waitEntered();

        var source = try exchange_core_mod.ExchangeCore.initWithIo(
            allocator,
            std.testing.io,
            .{},
        );
        defer source.deinit();
        var setter: CoreToShardedSetterProbe = .{
            .target = &target,
            .gate = &target_gate,
        };
        source.setRawResultSink(.{
            .context = &setter,
            .on_result = CoreToShardedSetterProbe.onRaw,
        });

        var source_response = try source.addUser(131_013);
        source_response.deinit(allocator);
        try target_gate.waitFinished();

        try std.testing.expectEqual(CrossBackendSetterOutcome.would_block, setter.outcome);
        try std.testing.expect(setter.unexpected_failure == null);
        try std.testing.expect(!target_gate.bounded_wait_failed.load(.acquire));

        target.shutdown();
        var target_response = target.takeResult(target_request) orelse
            return error.MissingAsyncResult;
        target_response.deinit(allocator);
    }

    {
        var target_gate: CrossBackendCallbackGate = .{ .io = std.testing.io };
        var target = try exchange_core_mod.ExchangeCore.initWithIo(
            allocator,
            std.testing.io,
            .{},
        );
        defer target.deinit();
        target.setRawResultSink(.{
            .context = &target_gate,
            .on_result = CrossBackendCallbackGate.holdRaw,
        });
        try target.startup();
        const target_request = try target.addUserAsync(131_014);
        try target_gate.waitEntered();

        var source = try ExchangeApiSharded.init(allocator, 4, .direct);
        defer source.deinit();
        var setter: ShardedToCoreSetterProbe = .{
            .target = &target,
            .gate = &target_gate,
        };
        source.setRawResultSink(.{
            .context = &setter,
            .on_result = ShardedToCoreSetterProbe.onRaw,
        });

        var source_response = try source.addUser(.routed, 131_015);
        source_response.deinit(allocator);
        try target_gate.waitFinished();

        try std.testing.expectEqual(CrossBackendSetterOutcome.would_block, setter.outcome);
        try std.testing.expect(setter.unexpected_failure == null);
        try std.testing.expect(!target_gate.bounded_wait_failed.load(.acquire));

        target.shutdown();
        var target_response = target.takeAsyncResult(target_request) orelse
            return error.MissingAsyncResult;
        target_response.deinit(allocator);
    }
}

test "sharded results grouping gate observes 1 4 5 while every completion settles" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    var probe: GroupingObserverProbe = .{};
    api.setRawResultSink(.{
        .context = &probe,
        .on_result = GroupingObserverProbe.raw,
    });
    api.setResultSink(.{
        .context = &probe,
        .on_result = GroupingObserverProbe.runtime,
    });
    api.setSimpleEventsHandler(.{
        .context = &probe,
        .on_command_result = GroupingObserverProbe.simple,
    });

    const first = try api.addUserAsync(.routed, 131_020);
    const disable = try api.groupingControlAsync(.routed, 9_979_210_001, 0);
    try api.enqueueRuntime(makeAddUserCommand(131_021));
    const enable = try api.groupingControlAsync(.routed, 9_979_210_002, 1);
    try api.enqueueRuntime(makeAddUserCommand(131_022));

    try std.testing.expectEqual(@as(usize, 3), try api.drainPending());
    _ = try api.drainRuntime();
    for ([_]u64{ first, disable, enable }) |request_id| {
        var response = api.takeResult(request_id) orelse return error.MissingAsyncResult;
        response.deinit(allocator);
    }

    try std.testing.expectEqualSlices(
        u64,
        &.{ 1, 4, 5 },
        probe.raw_sequences[0..probe.raw_len],
    );
    try std.testing.expectEqualSlices(
        u64,
        &.{5},
        probe.runtime_sequences[0..probe.runtime_len],
    );
    try std.testing.expectEqualSlices(
        u64,
        &.{ 1, 5 },
        probe.simple_sequences[0..probe.simple_len],
    );
    try std.testing.expectEqual(@as(usize, 0), api.pendingCount());
    try std.testing.expectEqual(@as(usize, 0), api.runtimePendingCount());
    try std.testing.expectEqual(@as(usize, 0), api.readyCount());
    try std.testing.expectEqual(@as(usize, 0), api.driver.completion_slots_used);
}

test "sharded raw observer covers detached and custom-report carriers exactly once" {
    const allocator = std.testing.allocator;
    const Adapted = report_registry.Adapter(ShardedApiCustomSpec);
    const registry = report_registry.Registry.fromSpecs(.{ShardedApiCustomSpec});
    var api = try ExchangeApiSharded.initWithPersistenceRoot(
        allocator,
        std.Io.Dir.cwd(),
        customReportConfig(registry, 1),
    );
    defer api.deinit();
    var probe: ExactRawSequenceProbe = .{
        .api = &api,
        .expected_custom_transfer_id = 72_330,
        .expected_custom_timestamp = 9_979_220_001,
    };
    api.setRawResultSink(.{
        .context = &probe,
        .on_result = ExactRawSequenceProbe.onResult,
    });

    const before = api.driver.publication_sequencer.highWater();
    try api.submitCommand(.routed, .{ .add_user = 131_030 });
    const query_value: ShardedApiCustomQuery = .{ .bias = 19 };
    const query = try Adapted.cloneQuery(allocator, &query_value);
    defer query.deinit();
    const pending = try api.submitCustomReportAsync(
        .routed,
        72_330,
        9_979_220_001,
        query,
    );
    probe.pending = pending;
    const after = api.driver.publication_sequencer.highWater();
    try std.testing.expect(after > before + 1);

    const drained = try api.drainPending();
    try std.testing.expect(drained > 0);
    try std.testing.expectEqual(@as(usize, @intCast(after - before)), probe.len);
    for (probe.sequences[0..probe.len], 0..) |sequence, index| {
        try std.testing.expectEqual(before + 1 + index, sequence);
    }
    try std.testing.expect(!probe.custom_storage_was_ready);
    try std.testing.expect(!probe.custom_storage_check_failed);
    try std.testing.expectEqual(@as(usize, 1), probe.custom_terminal_count);
    try std.testing.expect(probe.custom_terminal_metadata_seen);
    try std.testing.expect(probe.custom_binary_event_chain_valid);
    try std.testing.expectEqualSlices(
        i32,
        &.{ 0, 1024 },
        probe.custom_binary_event_sections[0..probe.custom_binary_event_len],
    );
    try std.testing.expect(try api.isCustomReportResultReady(pending));
    const result = (try api.takeCustomReportResult(pending)) orelse
        return error.MissingAsyncResult;
    result.deinit();
}

test "sharded durable journal is visible to raw observer and failure emits nothing" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var config = Config.javaDefault();
    config.initial_state = config_mod.InitialStateConfig.cleanStart("SHRAWJOURNAL");
    config.matching.num_shards = 4;
    config.serialization = .{
        .enable_journaling = true,
        .backend = .disk,
        .storage_folder = "store",
    };
    var api = try ExchangeApiSharded.initWithPersistenceRoot(
        allocator,
        tmp.dir,
        config,
    );
    defer api.deinit();
    var probe: RawJournalProbe = .{ .api = &api };
    api.setRawResultSink(.{
        .context = &probe,
        .on_result = RawJournalProbe.onResult,
    });

    var first = try api.addUser(.routed, 131_040);
    defer first.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), probe.count);
    try std.testing.expect(probe.durable_visible);

    api.durable_journal.?.injectSyncFailureForTesting();
    try std.testing.expectError(
        error.InjectedJournalIoFailure,
        api.addUser(.split, 131_041),
    );
    try std.testing.expectEqual(@as(usize, 1), probe.count);
}

test "sharded raw observer replacement waits for callback quiescence" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    var probe: RawResultQuiescenceProbe = .{ .io = std.testing.io };
    api.setRawResultSink(.{
        .context = &probe,
        .on_result = RawResultQuiescenceProbe.oldResult,
    });
    try api.startup();

    const first_request = try api.addUserAsync(.split, 131_050);
    probe.waitOldEntered();
    var setter: RawResultSetterRunner = .{
        .api = &api,
        .probe = &probe,
        .io = std.testing.io,
    };
    const setter_thread = try std.Thread.spawn(.{}, RawResultSetterRunner.run, .{&setter});
    setter.waitEntered();
    try std.testing.expect(!setter.isDone());

    probe.releaseOld();
    setter_thread.join();
    try std.testing.expectEqual(@as(?anyerror, null), setter.failure);
    try std.testing.expect(setter.isDone());
    _ = try api.drainPending();
    var first = api.takeResult(first_request) orelse return error.MissingAsyncResult;
    first.deinit(allocator);

    const second_request = try api.addUserAsync(.split, 131_051);
    _ = try api.drainPending();
    var second = api.takeResult(second_request) orelse return error.MissingAsyncResult;
    second.deinit(allocator);
    const counts = probe.counts();
    try std.testing.expectEqual(@as(usize, 1), counts[0]);
    try std.testing.expectEqual(@as(usize, 1), counts[1]);
}

test "sharded raw observer can reconfigure enqueue and request shutdown on same API" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    var probe: RawReconfigureShutdownProbe = .{ .api = &api };
    api.setRawResultSink(.{
        .context = &probe,
        .on_result = RawReconfigureShutdownProbe.first,
    });
    try api.startup();

    const first_request = try api.addUserAsync(.routed, 131_131);
    _ = api.drainPending() catch |err| switch (err) {
        error.CoreStopped => {},
        else => return err,
    };
    api.shutdown();
    try std.testing.expect(api.isStopped());
    try std.testing.expectEqual(@as(?anyerror, null), probe.reconfiguration_failure);
    try std.testing.expectEqual(@as(?anyerror, null), probe.enqueue_failure);
    try std.testing.expectEqual(
        @as(anyerror, error.CoreReentrantCall),
        probe.synchronous_wait_failure orelse return error.MissingCallbackFailure,
    );
    try std.testing.expectEqual(@as(usize, 1), probe.first_count);
    try std.testing.expectEqual(@as(usize, 1), probe.replacement_count);
    var first = api.takeResult(first_request) orelse return error.MissingAsyncResult;
    first.deinit(allocator);
    var queued = api.takeResult(probe.request_id orelse return error.MissingAsyncResult) orelse
        return error.MissingAsyncResult;
    queued.deinit(allocator);
}

test "sharded exchange api cold callback shutdown is deferred without reentrant deadlock" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    var probe: ColdCallbackShutdownProbe = .{ .api = &api };
    api.setSimpleEventsHandler(.{
        .context = &probe,
        .on_command_result = ColdCallbackShutdownProbe.onCommandResult,
    });

    var response = try api.addUser(.split, 131_100);
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    try std.testing.expectEqual(@as(usize, 1), probe.callback_count);
    try std.testing.expectEqual(error.CoreReentrantCall, probe.startup_error.?);
    try std.testing.expectEqual(@as(?anyerror, null), probe.shutdown_error);
    try std.testing.expectEqual(
        @as(?ShutdownOutcome, .deferred),
        probe.shutdown_outcome,
    );
    try std.testing.expect(api.isStopped());
    try std.testing.expectError(error.CoreStopped, api.addUserAsync(.split, 131_101));
    try std.testing.expectEqual(
        ShutdownOutcome.complete,
        try api.shutdownTimeout(.none),
    );
    try std.testing.expectEqual(ShutdownState.complete, api.shutdownState());
}

test "sharded exchange api simple-events replacement waits for callback quiescence" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    var probe: SimpleEventsQuiescenceProbe = .{ .io = std.testing.io };
    api.setSimpleEventsHandler(.{
        .context = &probe,
        .on_command_result = SimpleEventsQuiescenceProbe.oldCommand,
    });
    try api.startup();

    const first_request = try api.addUserAsync(.split, 131_110);
    probe.waitOldEntered();
    var setter: SimpleEventsSetterRunner = .{
        .api = &api,
        .probe = &probe,
        .io = std.testing.io,
    };
    const setter_thread = try std.Thread.spawn(.{}, SimpleEventsSetterRunner.run, .{&setter});
    setter.waitEntered();
    try std.testing.expect(!setter.isDone());

    probe.releaseOld();
    setter_thread.join();
    try std.testing.expectEqual(@as(?anyerror, null), setter.failure);
    try std.testing.expect(setter.isDone());
    _ = try api.drainPending();
    var first = api.takeResult(first_request) orelse return error.MissingAsyncResult;
    first.deinit(allocator);

    const second_request = try api.addUserAsync(.split, 131_111);
    _ = try api.drainPending();
    var second = api.takeResult(second_request) orelse return error.MissingAsyncResult;
    second.deinit(allocator);
    const counts = probe.counts();
    try std.testing.expectEqual(@as(usize, 1), counts[0]);
    try std.testing.expectEqual(@as(usize, 1), counts[1]);
}

test "sharded exchange api result-sink replacement waits for callback quiescence" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    var probe: ResultSinkQuiescenceProbe = .{ .io = std.testing.io };
    api.setResultSink(.{
        .context = &probe,
        .on_result = ResultSinkQuiescenceProbe.oldResult,
    });
    try api.startup();

    try api.enqueueRuntime(makeAddUserCommand(131_120));
    probe.waitOldEntered();
    var setter: ResultSinkSetterRunner = .{
        .api = &api,
        .probe = &probe,
        .io = std.testing.io,
    };
    const setter_thread = try std.Thread.spawn(.{}, ResultSinkSetterRunner.run, .{&setter});
    setter.waitEntered();
    try std.testing.expect(!setter.isDone());

    probe.releaseOld();
    setter_thread.join();
    try std.testing.expectEqual(@as(?anyerror, null), setter.failure);
    try std.testing.expect(setter.isDone());
    _ = try api.drainRuntime();

    try api.enqueueRuntime(makeAddUserCommand(131_121));
    _ = try api.drainRuntime();
    const counts = probe.counts();
    try std.testing.expectEqual(@as(usize, 1), counts[0]);
    try std.testing.expectEqual(@as(usize, 1), counts[1]);
}

test "sharded exchange api persist pair has one atomic concurrent consumer" {
    const allocator = std.testing.allocator;
    var api = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer api.deinit();
    const requests = try api.persistStateAsync(.split, 131_130, 9_979_100_001);
    try std.testing.expectEqual(@as(usize, 2), try api.drainPending());
    try std.testing.expect(api.isPersistStateResultReady(requests));

    var gate: PersistPairTakeGate = .{ .io = std.testing.io };
    var first: PersistPairTakeRunner = .{ .api = &api, .requests = requests, .gate = &gate };
    var second: PersistPairTakeRunner = .{ .api = &api, .requests = requests, .gate = &gate };
    const first_thread = try std.Thread.spawn(.{}, PersistPairTakeRunner.run, .{&first});
    const second_thread = try std.Thread.spawn(.{}, PersistPairTakeRunner.run, .{&second});
    gate.releaseWhenReady(2);
    first_thread.join();
    second_thread.join();

    const consumed = @as(usize, @intFromBool(first.result != null)) +
        @as(usize, @intFromBool(second.result != null));
    try std.testing.expectEqual(@as(usize, 1), consumed);
    try std.testing.expectEqual(
        cmd.CommandResultCode.state_persist_matching_engine_failed,
        first.result orelse second.result.?,
    );
    try std.testing.expect(!api.isPersistStateResultReady(requests));
    try std.testing.expectEqual(@as(usize, 0), api.readyCount());
    try std.testing.expectEqual(@as(usize, 0), api.driver.completion_slots_used);
}

test "sharded exchange api XCM2 persists and atomically restores independent topologies from owned root" {
    const allocator = std.testing.allocator;
    const io = io_context.current();
    const topologies = [_]struct { matching: u32, risk_engines: u32 }{
        .{ .matching = 4, .risk_engines = 2 },
        .{ .matching = 1, .risk_engines = 4 },
    };

    for (topologies, 0..) |topology, topology_index| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try tmp.dir.createDirPath(io, "owned-root");
        var borrowed_root = try tmp.dir.openDir(io, "owned-root", .{});

        const exchange_id = if (topology_index == 0) "XCM2_4ME2RE" else "XCM2_1ME4RE";
        var source_config = config_mod.JavaExchangeConfig.javaDefault();
        source_config.performance_cfg.matching_engines_num = topology.matching;
        source_config.performance_cfg.risk_engines_num = topology.risk_engines;
        source_config.init_state_cfg = config_mod.InitialStateConfig.cleanStart(exchange_id);
        source_config.serialization_cfg = .{
            .backend = .disk,
            .storage_folder = "xcm2-dumps",
        };
        var source = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
            allocator,
            borrowed_root,
            source_config,
        );
        borrowed_root.close(io);
        defer source.deinit();

        for (0..8) |offset| {
            var response = try source.addUser(.split, 710_000 + @as(i64, @intCast(offset)));
            response.deinit(allocator);
        }
        const source_fingerprint = try source.core.kernels.detailedHashFingerprint();
        try std.testing.expectEqual(
            cmd.CommandResultCode.success,
            try source.persistStateWithConfiguredMainLog(
                700 + @as(i64, @intCast(topology_index)),
                9_990_700_001 + @as(i64, @intCast(topology_index)),
                .risk_engine,
                0,
            ),
        );
        const committed = source.persistenceCurrentSnapshot().*;
        try std.testing.expectEqual(@as(usize, 2), source.persistenceSnapshotCount());

        var verify_root = try tmp.dir.openDir(io, "owned-root", .{});
        defer verify_root.close(io);
        const manifest_path = try persistence_paths.snapshotManifestPath(
            allocator,
            source.config.serialization.storage_folder,
            exchange_id,
            committed.snapshot_id,
        );
        defer allocator.free(manifest_path);
        try verify_root.access(io, manifest_path, .{});
        const main_log_path = try source.defaultPersistenceMainLogPath(allocator);
        defer allocator.free(main_log_path);
        try verify_root.access(io, main_log_path, .{});

        var restore_config = source_config;
        restore_config.init_state_cfg = config_mod.InitialStateConfig.fromSnapshotOnly(
            exchange_id,
            committed.snapshot_id,
            committed.seq,
        );
        var restored = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
            allocator,
            verify_root,
            restore_config,
        );
        defer restored.deinit();
        try std.testing.expectError(error.ConfiguredPersistenceBootstrapRequired, restored.startup());
        const outcome = try restored.startupFromConfiguredPersistence(
            verify_root,
            .risk_engine,
            0,
            0,
        );
        try std.testing.expect(outcome.snapshot_requested);
        try std.testing.expect(outcome.snapshot_found);
        try std.testing.expectEqual(committed.seq, restored.core.kernels.next_seq);
        try std.testing.expectEqual(committed.seq, restored.driver.publication_sequencer.highWater());
        try std.testing.expectEqual(source_fingerprint, try restored.core.kernels.detailedHashFingerprint());
        try std.testing.expectEqual(topology.matching, @as(u32, @intCast(restored.matchingShardCount())));
        try std.testing.expectEqual(topology.risk_engines, @as(u32, @intCast(restored.riskShardCount())));
    }
}

test "sharded exchange api XCM2 config topology and sequence failures preserve cold live state" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var source_config = config_mod.JavaExchangeConfig.javaDefault();
    source_config.performance_cfg.matching_engines_num = 4;
    source_config.performance_cfg.risk_engines_num = 2;
    source_config.performance_cfg.orderbook_kind = .direct;
    source_config.init_state_cfg = config_mod.InitialStateConfig.cleanStart("XCM2_MISMATCH");
    source_config.serialization_cfg = .{
        .backend = .disk,
        .storage_folder = "xcm2-dumps",
    };
    var source = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        source_config,
    );
    defer source.deinit();
    var add = try source.addUser(.split, 720_001);
    add.deinit(allocator);
    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try source.persistState(.split, 807, 9_990_807_001),
    );
    const committed_sequence = source.persistenceCurrentSnapshot().seq;

    const cases = [_]struct {
        matching: u32,
        risk_engines: u32,
        orderbook_kind: kernel_mod.OrderBookKind,
        configured_sequence: u64,
        expected: anyerror,
    }{
        .{
            .matching = 4,
            .risk_engines = 2,
            .orderbook_kind = .naive,
            .configured_sequence = committed_sequence,
            .expected = error.ShardedSnapshotConfigMismatch,
        },
        .{
            .matching = 2,
            .risk_engines = 2,
            .orderbook_kind = .direct,
            .configured_sequence = committed_sequence,
            .expected = error.ShardedSnapshotTopologyMismatch,
        },
        .{
            .matching = 4,
            .risk_engines = 2,
            .orderbook_kind = .direct,
            .configured_sequence = committed_sequence + 1,
            .expected = error.ShardedSnapshotSequenceMismatch,
        },
    };
    for (cases) |case| {
        var restore_config = source_config;
        restore_config.performance_cfg.matching_engines_num = case.matching;
        restore_config.performance_cfg.risk_engines_num = case.risk_engines;
        restore_config.performance_cfg.orderbook_kind = case.orderbook_kind;
        restore_config.init_state_cfg = config_mod.InitialStateConfig.fromSnapshotOnly(
            "XCM2_MISMATCH",
            807,
            case.configured_sequence,
        );
        var target = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
            allocator,
            tmp.dir,
            restore_config,
        );
        defer target.deinit();
        const fingerprint_before = try target.core.kernels.detailedHashFingerprint();
        try std.testing.expectError(
            case.expected,
            target.startupFromConfiguredPersistence(
                tmp.dir,
                .matching_engine_router,
                0,
                0,
            ),
        );
        try std.testing.expectEqual(@as(u64, 0), target.core.kernels.next_seq);
        try std.testing.expectEqual(@as(u64, 0), target.driver.publication_sequencer.highWater());
        try std.testing.expectEqual(@as(usize, 1), target.persistenceSnapshotCount());
        try std.testing.expectEqual(fingerprint_before, try target.core.kernels.detailedHashFingerprint());
    }

    const payload_path = try source.defaultPersistenceSnapshotPath(
        allocator,
        807,
        .matching_engine_router,
        0,
    );
    defer allocator.free(payload_path);
    var payload_file = try tmp.dir.openFile(io_context.current(), payload_path, .{});
    const payload_bytes = try io_context.readToEndAlloc(payload_file, allocator, 8 * 1024 * 1024);
    payload_file.close(io_context.current());
    defer allocator.free(payload_bytes);
    payload_bytes[payload_bytes.len - 1] ^= 1;
    const tampered_file = try tmp.dir.createFile(
        io_context.current(),
        payload_path,
        .{ .truncate = true },
    );
    var write_buffer: [4096]u8 = undefined;
    var writer = tampered_file.writer(io_context.current(), &write_buffer);
    try writer.interface.writeAll(payload_bytes);
    try writer.interface.flush();
    try tampered_file.sync(io_context.current());
    tampered_file.close(io_context.current());

    var tampered_config = source_config;
    tampered_config.init_state_cfg = config_mod.InitialStateConfig.fromSnapshotOnly(
        "XCM2_MISMATCH",
        807,
        committed_sequence,
    );
    var tampered_target = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        tampered_config,
    );
    defer tampered_target.deinit();
    const tampered_fingerprint_before = try tampered_target.core.kernels.detailedHashFingerprint();
    try std.testing.expectError(
        error.ShardedSnapshotInvalid,
        tampered_target.startupFromConfiguredPersistence(
            tmp.dir,
            .matching_engine_router,
            0,
            0,
        ),
    );
    try std.testing.expectEqual(@as(u64, 0), tampered_target.core.kernels.next_seq);
    try std.testing.expectEqual(@as(usize, 1), tampered_target.persistenceSnapshotCount());
    try std.testing.expectEqual(
        tampered_fingerprint_before,
        try tampered_target.core.kernels.detailedHashFingerprint(),
    );
}

test "sharded exchange api XCM2 rejects dummy mismatch fault orphan collision and wrong root without descriptors" {
    const allocator = std.testing.allocator;
    const io = io_context.current();

    var dummy = try ExchangeApiSharded.init(allocator, 4, .direct);
    defer dummy.deinit();
    const dummy_requests = try dummy.persistStateAsync(.split, 801, 9_990_801_001);
    try std.testing.expectEqual(@as(usize, 2), try dummy.drainPending());
    var dummy_matching = dummy.takeResult(dummy_requests.matching_request_id) orelse
        return error.MissingAsyncResult;
    defer dummy_matching.deinit(allocator);
    var dummy_risk = dummy.takeResult(dummy_requests.risk_request_id) orelse
        return error.MissingAsyncResult;
    defer dummy_risk.deinit(allocator);
    try std.testing.expectEqual(
        cmd.CommandResultCode.state_persist_matching_engine_failed,
        dummy_matching.result_code,
    );
    try std.testing.expectEqual(
        cmd.CommandResultCode.state_persist_risk_engine_failed,
        dummy_risk.result_code,
    );
    try std.testing.expectEqual(@as(usize, 1), dummy.persistenceSnapshotCount());

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var other = std.testing.tmpDir(.{});
    defer other.cleanup();
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.matching_engines_num = 4;
    java_config.performance_cfg.risk_engines_num = 2;
    java_config.init_state_cfg = config_mod.InitialStateConfig.cleanStart("XCM2_FAILURES");
    java_config.serialization_cfg = .{
        .backend = .disk,
        .storage_folder = "xcm2-dumps",
    };
    var api = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        java_config,
    );
    defer api.deinit();

    try std.testing.expectError(
        error.PersistenceRootMismatch,
        api.persistStateWithDefaultMainLog(
            other.dir,
            802,
            9_990_802_001,
            .matching_engine_router,
            0,
        ),
    );
    try std.testing.expectEqual(@as(u64, 0), api.core.kernels.next_seq);
    try std.testing.expectEqual(@as(usize, 1), api.persistenceSnapshotCount());

    var mismatched_matching = makePersistStateMatchingCommand(803, 9_990_803_001);
    var one_matching = [_]*cmd.OrderCommand{&mismatched_matching};
    const matching_batch = try api.submitCommandBatch(.routed, one_matching[0..]);
    try std.testing.expectEqual(cmd.CommandResultCode.accepted, matching_batch.result_code);
    var mismatched_risk = makePersistStateRiskCommand(803, 9_990_803_001);
    var one_risk = [_]*cmd.OrderCommand{&mismatched_risk};
    const risk_batch = try api.submitCommandBatch(.split, one_risk[0..]);
    try std.testing.expectEqual(
        cmd.CommandResultCode.state_persist_risk_engine_failed,
        risk_batch.result_code,
    );
    try std.testing.expectEqual(@as(usize, 1), api.persistenceSnapshotCount());

    api.setPersistenceFaultForTesting(.before_manifest_publish);
    const fault_requests = try api.persistStateAsync(.split, 804, 9_990_804_001);
    try std.testing.expectEqual(@as(usize, 2), try api.drainPending());
    var fault_matching = api.takeResult(fault_requests.matching_request_id) orelse
        return error.MissingAsyncResult;
    defer fault_matching.deinit(allocator);
    var fault_risk = api.takeResult(fault_requests.risk_request_id) orelse
        return error.MissingAsyncResult;
    defer fault_risk.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.accepted, fault_matching.result_code);
    try std.testing.expectEqual(
        cmd.CommandResultCode.state_persist_risk_engine_failed,
        fault_risk.result_code,
    );
    try std.testing.expectEqual(@as(usize, 1), api.persistenceSnapshotCount());

    const manifest_path = try persistence_paths.snapshotManifestPath(
        allocator,
        api.config.serialization.storage_folder,
        api.config.initial_state.exchange_id,
        804,
    );
    defer allocator.free(manifest_path);
    try std.testing.expect(!try api.snapshotExistsFile(api.persistence_root.?, manifest_path));
    const orphan_path = try api.defaultPersistenceSnapshotPath(
        allocator,
        804,
        .matching_engine_router,
        0,
    );
    defer allocator.free(orphan_path);
    try api.persistence_root.?.access(io, orphan_path, .{});
    try std.testing.expectEqual(
        @as(?u64, null),
        try sharded_snapshot_store.discoverLatestCommittedSnapshotId(
            allocator,
            api.persistence_root.?,
            api.config.serialization.storage_folder,
            api.config.initial_state.exchange_id,
            .{},
        ),
    );

    // Immutable orphan payloads make the same identity collide cleanly; they
    // still cannot become discoverable without the manifest commit record.
    try std.testing.expectEqual(
        cmd.CommandResultCode.state_persist_risk_engine_failed,
        try api.persistState(.split, 804, 9_990_804_001),
    );
    try std.testing.expect(!try api.snapshotExistsFile(api.persistence_root.?, manifest_path));
    try std.testing.expectEqual(@as(usize, 1), api.persistenceSnapshotCount());

    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try api.persistState(.split, 805, 9_990_805_001),
    );
    try std.testing.expectEqual(@as(usize, 2), api.persistenceSnapshotCount());
}

test "sharded exchange api XCM2 descriptor OOM fails before durable publication" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var config = Config.javaDefault();
    config.matching.num_shards = 2;
    config.serialization = .{
        .backend = .disk,
        .storage_folder = "xcm2-dumps",
    };
    var api = try ExchangeApiSharded.initWithPersistenceRoot(allocator, tmp.dir, config);
    defer api.deinit();

    while (api.descriptors.snapshots.items.len < api.descriptors.snapshots.capacity) {
        api.current_snapshot_ref = api.descriptors.createNextSnapshotAssumeReserved(
            api.current_snapshot_ref,
            0,
            0,
            0,
        );
    }
    const descriptor_count_before = api.persistenceSnapshotCount();
    var failing = std.testing.FailingAllocator.init(allocator, .{ .fail_index = 0 });
    api.descriptors.allocator = failing.allocator();
    const result = api.persistState(.split, 806, 9_990_806_001) catch |err| {
        api.descriptors.allocator = allocator;
        return err;
    };
    api.descriptors.allocator = allocator;

    try std.testing.expect(failing.has_induced_failure);
    try std.testing.expectEqual(
        cmd.CommandResultCode.state_persist_matching_engine_failed,
        result,
    );
    try std.testing.expectEqual(descriptor_count_before, api.persistenceSnapshotCount());
    const manifest_path = try persistence_paths.snapshotManifestPath(
        allocator,
        api.config.serialization.storage_folder,
        api.config.initial_state.exchange_id,
        806,
    );
    defer allocator.free(manifest_path);
    try std.testing.expect(!try api.snapshotExistsFile(api.persistence_root.?, manifest_path));
    const first_payload = try api.defaultPersistenceSnapshotPath(
        allocator,
        806,
        .matching_engine_router,
        0,
    );
    defer allocator.free(first_payload);
    try std.testing.expect(!try api.snapshotExistsFile(api.persistence_root.?, first_payload));
}

test "sharded exchange api XCM1 restore remains explicit and only supports 1ME 1RE" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var source = try ExchangeApiSharded.init(allocator, 1, .direct);
    defer source.deinit();
    var add = try source.addUser(.routed, 730_001);
    add.deinit(allocator);
    try source.captureSnapshotToFile(tmp.dir, "legacy-xcm1.ecs", &.{}, 8);

    var restore_config = config_mod.JavaExchangeConfig.javaDefault();
    restore_config.performance_cfg.matching_engines_num = 1;
    restore_config.performance_cfg.risk_engines_num = 1;
    restore_config.init_state_cfg = config_mod.InitialStateConfig.fromSnapshotOnly(
        "XCM1_EXPLICIT",
        901,
        1,
    );
    restore_config.serialization_cfg = .{
        .backend = .disk,
        .storage_folder = "legacy-dumps",
    };
    var restored = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        restore_config,
    );
    defer restored.deinit();
    const outcome = try restored.startupFromPersistence(
        tmp.dir,
        "legacy-xcm1.ecs",
        null,
    );
    try std.testing.expect(outcome.snapshot_found);
    var duplicate = try restored.addUser(.routed, 730_001);
    defer duplicate.deinit(allocator);
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_user_already_exists,
        duplicate.result_code,
    );

    restore_config.performance_cfg.matching_engines_num = 2;
    restore_config.performance_cfg.risk_engines_num = 2;
    var unsupported = try ExchangeApiSharded.initFromJavaConfigWithPersistenceRoot(
        allocator,
        tmp.dir,
        restore_config,
    );
    defer unsupported.deinit();
    try std.testing.expectError(
        error.UnsupportedShardedPersistence,
        unsupported.bootstrapFromPersistence(tmp.dir, "legacy-xcm1.ecs", null),
    );
}

test "sharded custom registry rejects legacy snapshot capture and bootstrap before poisoning" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var legacy_source = try ExchangeApiSharded.init(allocator, 1, .direct);
    defer legacy_source.deinit();
    try legacy_source.captureSnapshotToFile(
        tmp.dir,
        "legacy-empty.ecs",
        &.{},
        8,
    );

    const registry = report_registry.Registry.fromSpecs(.{ShardedApiCustomSpec});
    var custom_config = customReportConfig(registry, 1);
    custom_config.initial_state = config_mod.InitialStateConfig.fromSnapshotOnly(
        "CUSTOM_LEGACY",
        902,
        0,
    );
    custom_config.serialization = .{
        .backend = .disk,
        .storage_folder = "custom-legacy-dumps",
    };
    var api = try ExchangeApiSharded.initWithPersistenceRoot(
        allocator,
        tmp.dir,
        custom_config,
    );
    defer api.deinit();

    try std.testing.expectError(
        error.LegacySnapshotCustomReportRegistryUnsupported,
        api.captureSnapshotToFile(tmp.dir, "custom-legacy.ecs", &.{}, 8),
    );
    try std.testing.expect(!try api.snapshotExistsFile(
        tmp.dir,
        "custom-legacy.ecs",
    ));
    try std.testing.expectError(
        error.LegacySnapshotCustomReportRegistryUnsupported,
        api.bootstrapFromPersistence(tmp.dir, "legacy-empty.ecs", null),
    );
    try std.testing.expect(api.fatalInfo() == null);
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

const ShardedSerializationTestRecorder = struct {
    const journal_capacity = 32;
    const store_capacity = 32;

    const StoreCall = struct {
        module_type: serialization_processor.ModuleType,
        instance_id: u32,
    };

    factory_calls: usize = 0,
    startup_calls: usize = 0,
    store_calls: usize = 0,
    journal_calls: usize = 0,
    shutdown_calls: usize = 0,
    destroy_calls: usize = 0,
    matching_stores: usize = 0,
    risk_stores: usize = 0,
    matching_instances_mask: u64 = 0,
    risk_instances_mask: u64 = 0,
    factory_matching_count: usize = 0,
    factory_risk_count: usize = 0,
    startup_saw_sharded: bool = false,
    store_saw_sharded: bool = true,
    reject_store_once: ?StoreCall = null,
    store_requests: [store_capacity]StoreCall = undefined,
    journal_requests: [journal_capacity]serialization_processor.JournalRequest = undefined,

    fn fromContext(context: ?*anyopaque) *ShardedSerializationTestRecorder {
        return @ptrCast(@alignCast(context.?));
    }

    fn create(
        context: ?*anyopaque,
        _: std.mem.Allocator,
        factory_context: serialization_processor.FactoryContext,
    ) serialization_processor.CreateError!serialization_processor.Processor {
        const self = fromContext(context);
        self.factory_calls += 1;
        self.factory_matching_count = factory_context.matching_engine_count;
        self.factory_risk_count = factory_context.risk_engine_count;
        return .{
            .context = context,
            .vtable = &vtable,
        };
    }

    fn startup(
        context: ?*anyopaque,
        request: serialization_processor.StartupRequest,
    ) serialization_processor.StartupError!void {
        const self = fromContext(context);
        self.startup_calls += 1;
        self.startup_saw_sharded = switch (request.state) {
            .sharded => true,
            .single => false,
        };
    }

    fn store(
        context: ?*anyopaque,
        request: serialization_processor.StoreRequest,
    ) serialization_processor.StoreError!bool {
        const self = fromContext(context);
        std.debug.assert(self.store_calls < self.store_requests.len);
        const store_call: StoreCall = .{
            .module_type = request.module_type,
            .instance_id = request.instance_id,
        };
        self.store_requests[self.store_calls] = store_call;
        self.store_calls += 1;
        switch (request.state) {
            .sharded => {},
            .single => self.store_saw_sharded = false,
        }
        const instance_bit = @as(u64, 1) << @intCast(request.instance_id);
        switch (request.module_type) {
            .matching_engine_router => {
                self.matching_stores += 1;
                self.matching_instances_mask |= instance_bit;
            },
            .risk_engine => {
                self.risk_stores += 1;
                self.risk_instances_mask |= instance_bit;
            },
        }
        if (self.reject_store_once) |rejection| {
            if (rejection.module_type == store_call.module_type and
                rejection.instance_id == store_call.instance_id)
            {
                self.reject_store_once = null;
                return false;
            }
        }
        return true;
    }

    fn journal(
        context: ?*anyopaque,
        request: serialization_processor.JournalRequest,
    ) serialization_processor.JournalError!void {
        const self = fromContext(context);
        std.debug.assert(self.journal_calls < self.journal_requests.len);
        self.journal_requests[self.journal_calls] = request;
        self.journal_calls += 1;
    }

    fn shutdown(context: ?*anyopaque) void {
        fromContext(context).shutdown_calls += 1;
    }

    fn destroy(context: ?*anyopaque, _: std.mem.Allocator) void {
        fromContext(context).destroy_calls += 1;
    }

    const vtable: serialization_processor.VTable = .{
        .startup = startup,
        .store_data = store,
        .write_to_journal = journal,
        .shutdown = shutdown,
        .destroy = destroy,
    };
};

fn expectShardedCustomJournalBatchSchedule(
    recorder: *const ShardedSerializationTestRecorder,
    expected_batch_lengths: []const usize,
) !void {
    var expected_callbacks: usize = 0;
    for (expected_batch_lengths) |batch_len| {
        try std.testing.expect(batch_len > 0);
        expected_callbacks += batch_len;
    }
    try std.testing.expectEqual(expected_callbacks, recorder.journal_calls);

    var callback_index: usize = 0;
    var terminal_count: usize = 0;
    var previous_sequence: ?u64 = null;
    for (expected_batch_lengths) |batch_len| {
        for (0..batch_len) |batch_index| {
            const request = recorder.journal_requests[callback_index];
            if (previous_sequence) |previous| {
                try std.testing.expect(request.sequence > previous);
            }
            previous_sequence = request.sequence;
            const expected_terminal = batch_index + 1 == batch_len;
            try std.testing.expectEqual(expected_terminal, request.end_of_batch);
            if (request.end_of_batch) terminal_count += 1;
            callback_index += 1;
        }
    }
    try std.testing.expectEqual(expected_batch_lengths.len, terminal_count);
}

fn expectShardedCustomStoreOrder(
    recorder: *const ShardedSerializationTestRecorder,
    offset: usize,
    expected: []const ShardedSerializationTestRecorder.StoreCall,
) !void {
    try std.testing.expect(offset + expected.len <= recorder.store_calls);
    for (expected, recorder.store_requests[offset .. offset + expected.len]) |want, actual| {
        try std.testing.expectEqual(want.module_type, actual.module_type);
        try std.testing.expectEqual(want.instance_id, actual.instance_id);
    }
}

test "sharded custom serialization end-of-batch enumerates cold and running schedules" {
    const Schedule = enum {
        cold_batch,
        running_batch,
        running_singletons,
        running_control_prefix,
        running_persistent_grouped_batch,
    };
    const schedules = [_]Schedule{
        .cold_batch,
        .running_batch,
        .running_singletons,
        .running_control_prefix,
        .running_persistent_grouped_batch,
    };

    for (schedules) |schedule| {
        var recorder: ShardedSerializationTestRecorder = .{};
        const factory: serialization_processor.Factory = .{
            .context = &recorder,
            .create = ShardedSerializationTestRecorder.create,
        };
        var config = Config.javaDefault();
        config.matching.num_shards = 2;
        config.sharded_pipeline_mode = if (schedule == .running_persistent_grouped_batch)
            .persistent_grouped
        else
            .immediate;
        config.serialization = config_mod.SerializationConfig.custom(factory, true);
        var api = try ExchangeApiSharded.initWithPersistenceRoot(
            std.testing.allocator,
            std.Io.Dir.cwd(),
            config,
        );
        defer api.deinit();

        var commands = [_]cmd.OrderCommand{
            .{ .command = .add_user, .uid = 882_001 },
            .{ .command = .add_user, .uid = 882_002 },
            .{ .command = .add_user, .uid = 882_003 },
        };
        var command_ptrs = [_]*cmd.OrderCommand{
            &commands[0],
            &commands[1],
            &commands[2],
        };

        switch (schedule) {
            .cold_batch => {
                _ = try api.submitCommandBatch(.routed, command_ptrs[0..]);
                try expectShardedCustomJournalBatchSchedule(&recorder, &.{3});
            },
            .running_batch => {
                try api.startup();
                _ = try api.submitCommandBatch(.routed, command_ptrs[0..]);
                try expectShardedCustomJournalBatchSchedule(&recorder, &.{3});
            },
            .running_persistent_grouped_batch => {
                try api.startup();
                _ = try api.submitCommandBatch(.routed, command_ptrs[0..]);
                try expectShardedCustomJournalBatchSchedule(&recorder, &.{3});
            },
            .running_singletons => {
                try api.startup();
                for (&commands) |*command| {
                    var response = try api.executeWithMode(.routed, command);
                    response.deinit(std.testing.allocator);
                }
                try expectShardedCustomJournalBatchSchedule(
                    &recorder,
                    &.{ 1, 1, 1 },
                );
            },
            .running_control_prefix => {
                try api.startup();
                var forced = [_]cmd.OrderCommand{
                    .{ .command = .add_user, .uid = 882_011 },
                    .{ .command = .reset },
                    .{ .command = .add_user, .uid = 882_012 },
                    .{ .command = .nop },
                };
                var forced_ptrs = [_]*cmd.OrderCommand{
                    &forced[0],
                    &forced[1],
                    &forced[2],
                    &forced[3],
                };
                _ = try api.submitCommandBatch(.routed, forced_ptrs[0..]);
                try expectShardedCustomJournalBatchSchedule(
                    &recorder,
                    &.{4},
                );
            },
        }
    }
}

test "sharded custom serialization freezes captured batch before concurrent suffix" {
    var recorder: ShardedSerializationTestRecorder = .{};
    const factory: serialization_processor.Factory = .{
        .context = &recorder,
        .create = ShardedSerializationTestRecorder.create,
    };
    var config = Config.javaDefault();
    config.matching.num_shards = 2;
    config.sharded_pipeline_mode = .immediate;
    config.serialization = config_mod.SerializationConfig.custom(factory, true);
    var api = try ExchangeApiSharded.initWithPersistenceRoot(
        std.testing.allocator,
        std.Io.Dir.cwd(),
        config,
    );
    defer api.deinit();
    var gate: AvailableBatchPauseTestGate = .{};
    api.available_batch_test_gate = &gate;
    defer gate.open();
    try api.startup();

    const prefix = [_]cmd.OrderCommand{
        .{ .command = .add_user, .uid = 882_101 },
        .{ .command = .add_user, .uid = 882_102 },
    };
    try api.enqueueDetachedBatch(&prefix, .routed, .detached_api);
    gate.waitUntilEntered();

    const concurrent_suffix = [_]cmd.OrderCommand{
        .{ .command = .add_user, .uid = 882_103 },
        .{ .command = .add_user, .uid = 882_104 },
    };
    try api.enqueueDetachedBatch(&concurrent_suffix, .routed, .detached_api);
    gate.open();
    try std.testing.expectEqual(@as(usize, 0), try api.drainPending());
    try expectShardedCustomJournalBatchSchedule(&recorder, &.{ 2, 2 });
}

fn runShardedCustomPersistenceRejectionScenario(
    pipeline_mode: config_mod.ShardedPipelineMode,
    rejection: ShardedSerializationTestRecorder.StoreCall,
    expected_matching_result: cmd.CommandResultCode,
    expected_risk_result: cmd.CommandResultCode,
) !void {
    var recorder: ShardedSerializationTestRecorder = .{
        .reject_store_once = rejection,
    };
    const factory: serialization_processor.Factory = .{
        .context = &recorder,
        .create = ShardedSerializationTestRecorder.create,
    };
    var core_config = Config.javaDefault();
    core_config.matching.num_shards = 2;
    core_config.sharded_pipeline_mode = pipeline_mode;
    core_config.serialization = config_mod.SerializationConfig.custom(
        factory,
        false,
    );
    var api = try ExchangeApiSharded.initWithPersistenceRoot(
        std.testing.allocator,
        std.Io.Dir.cwd(),
        core_config,
    );
    defer api.deinit();
    try api.startup();

    const expected_family_order = [_]ShardedSerializationTestRecorder.StoreCall{
        .{ .module_type = .matching_engine_router, .instance_id = 0 },
        .{ .module_type = .matching_engine_router, .instance_id = 1 },
        .{ .module_type = .risk_engine, .instance_id = 0 },
        .{ .module_type = .risk_engine, .instance_id = 1 },
    };

    var failed_matching = makePersistStateMatchingCommand(250_101, 9_991_101);
    var failed_risk = makePersistStateRiskCommand(250_101, 9_991_101);
    var failed_commands = [_]*cmd.OrderCommand{ &failed_matching, &failed_risk };
    const failed_batch = try api.submitCommandBatch(.routed, failed_commands[0..]);

    try std.testing.expectEqual(expected_matching_result, failed_matching.result_code);
    try std.testing.expectEqual(expected_risk_result, failed_risk.result_code);
    try std.testing.expectEqual(
        cmd.CommandResultCode.mergeToFirstFailed(&.{
            expected_matching_result,
            expected_risk_result,
        }),
        failed_batch.result_code,
    );
    try std.testing.expectEqual(@as(usize, 4), recorder.store_calls);
    try std.testing.expectEqual(@as(usize, 2), recorder.matching_stores);
    try std.testing.expectEqual(@as(usize, 2), recorder.risk_stores);
    try std.testing.expectEqual(@as(u64, 0b11), recorder.matching_instances_mask);
    try std.testing.expectEqual(@as(u64, 0b11), recorder.risk_instances_mask);
    try expectShardedCustomStoreOrder(&recorder, 0, &expected_family_order);
    try std.testing.expectEqual(@as(usize, 1), api.persistenceSnapshotCount());
    try std.testing.expect(api.persistenceFindSnapshotById(250_101) == null);
    try std.testing.expect(api.pending_persist_pair == null);
    try std.testing.expect(recorder.reject_store_once == null);

    var valid_matching = makePersistStateMatchingCommand(250_102, 9_991_102);
    var valid_risk = makePersistStateRiskCommand(250_102, 9_991_102);
    var valid_commands = [_]*cmd.OrderCommand{ &valid_matching, &valid_risk };
    const valid_batch = try api.submitCommandBatch(.routed, valid_commands[0..]);

    try std.testing.expectEqual(cmd.CommandResultCode.accepted, valid_matching.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, valid_risk.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, valid_batch.result_code);
    try std.testing.expectEqual(@as(usize, 8), recorder.store_calls);
    try std.testing.expectEqual(@as(usize, 4), recorder.matching_stores);
    try std.testing.expectEqual(@as(usize, 4), recorder.risk_stores);
    try expectShardedCustomStoreOrder(&recorder, 4, &expected_family_order);
    try std.testing.expectEqual(@as(usize, 2), api.persistenceSnapshotCount());
    try std.testing.expectEqual(@as(u64, 250_102), api.persistenceCurrentSnapshot().snapshot_id);
    try std.testing.expect(api.pending_persist_pair == null);
}

test "sharded custom serialization attempts every module after store rejection" {
    const RejectionCase = struct {
        rejection: ShardedSerializationTestRecorder.StoreCall,
        matching_result: cmd.CommandResultCode,
        risk_result: cmd.CommandResultCode,
    };
    const rejection_cases = [_]RejectionCase{
        .{
            .rejection = .{ .module_type = .matching_engine_router, .instance_id = 0 },
            .matching_result = .state_persist_matching_engine_failed,
            .risk_result = .success,
        },
        .{
            .rejection = .{ .module_type = .risk_engine, .instance_id = 0 },
            .matching_result = .accepted,
            .risk_result = .state_persist_risk_engine_failed,
        },
    };
    const pipeline_modes = [_]config_mod.ShardedPipelineMode{
        .immediate,
        .persistent_grouped,
    };

    for (pipeline_modes) |pipeline_mode| {
        for (rejection_cases) |rejection_case| {
            try runShardedCustomPersistenceRejectionScenario(
                pipeline_mode,
                rejection_case.rejection,
                rejection_case.matching_result,
                rejection_case.risk_result,
            );
        }
    }
}

test "sharded exchange custom serialization delegates every module instance" {
    var recorder: ShardedSerializationTestRecorder = .{};
    const factory: serialization_processor.Factory = .{
        .context = &recorder,
        .create = ShardedSerializationTestRecorder.create,
    };
    var core_config = Config.javaDefault();
    core_config.matching.num_shards = 2;
    core_config.sharded_pipeline_mode = .immediate;
    core_config.serialization = config_mod.SerializationConfig.custom(
        factory,
        true,
    );
    var api = try ExchangeApiSharded.initWithPersistenceRoot(
        std.testing.allocator,
        std.Io.Dir.cwd(),
        core_config,
    );

    try api.startup();
    try std.testing.expectError(
        error.ConfiguredMainLogRequiresDiskBackend,
        api.persistStateWithConfiguredMainLog(
            250_000,
            9_991_000,
            .matching_engine_router,
            0,
        ),
    );
    try std.testing.expectError(
        error.ConfiguredMainLogRequiresDiskBackend,
        api.persistStateWithDefaultMainLog(
            std.Io.Dir.cwd(),
            250_000,
            9_991_000,
            .matching_engine_router,
            0,
        ),
    );
    try std.testing.expectEqual(@as(usize, 0), recorder.store_calls);
    try std.testing.expectEqual(@as(usize, 0), recorder.journal_calls);
    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try api.persistState(.routed, 250_001, 9_991_001),
    );
    api.shutdown();

    try std.testing.expectEqual(@as(usize, 1), recorder.factory_calls);
    try std.testing.expectEqual(@as(usize, 2), recorder.factory_matching_count);
    try std.testing.expectEqual(@as(usize, 2), recorder.factory_risk_count);
    try std.testing.expectEqual(@as(usize, 1), recorder.startup_calls);
    try std.testing.expect(recorder.startup_saw_sharded);
    try std.testing.expect(recorder.store_saw_sharded);
    try std.testing.expectEqual(@as(usize, 4), recorder.store_calls);
    try std.testing.expectEqual(@as(usize, 2), recorder.matching_stores);
    try std.testing.expectEqual(@as(usize, 2), recorder.risk_stores);
    try std.testing.expectEqual(@as(u64, 0b11), recorder.matching_instances_mask);
    try std.testing.expectEqual(@as(u64, 0b11), recorder.risk_instances_mask);
    try std.testing.expectEqual(@as(usize, 2), recorder.journal_calls);
    try expectShardedCustomJournalBatchSchedule(&recorder, &.{2});
    try std.testing.expectEqual(@as(usize, 1), recorder.shutdown_calls);

    api.deinit();
    try std.testing.expectEqual(@as(usize, 1), recorder.shutdown_calls);
    try std.testing.expectEqual(@as(usize, 1), recorder.destroy_calls);
}
