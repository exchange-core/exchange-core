const std = @import("std");
const builtin = @import("builtin");
const io_context = @import("io_context.zig");
const common = @import("common.zig");
const cmd = @import("cmd.zig");
const config_mod = @import("config.zig");
const binary_frames = @import("binary_frames.zig");
const binary_payload = @import("binary_payload.zig");
const java_binary_wire = @import("java_binary_wire.zig");
const api_command_mod = @import("api_command.zig");
const kernel_mod = @import("exchange_kernel.zig");
const journal_mod = @import("journal.zig");
const persistence_mod = @import("persistence_descriptors.zig");
const persistence_paths = @import("persistence_paths.zig");
const publication = @import("publication_sequence.zig");
const report_registry = @import("report_registry.zig");
const runtime_mod = @import("exchange_runtime.zig");
const runtime_queue_mod = @import("runtime_queue.zig");
const serialization_processor = @import("serialization_processor.zig");
const simple_events_mod = @import("simple_events_processor.zig");
const snapshot_mod = @import("snapshot.zig");
const log = std.log.scoped(.exchange_core);

threadlocal var active_callback_core: ?*ExchangeCore = null;
threadlocal var active_worker_core: ?*ExchangeCore = null;

pub const Config = config_mod.ExchangeCoreConfig;
pub const JavaConfig = config_mod.JavaExchangeConfig;
pub const BinaryCurrencyBalance = api_command_mod.BinaryCurrencyBalance;
pub const BinaryUserAccountsBatch = api_command_mod.BinaryUserAccountsBatch;
pub const BinaryReportQuery = api_command_mod.BinaryReportQuery;
pub const ApiCommand = api_command_mod.ApiCommand;
pub const ApiCommandResult = api_command_mod.ApiCommandResult;
pub const ApiCommandAsyncHandle = api_command_mod.ApiCommandAsyncHandle;
pub const RawResultSink = runtime_mod.RawResultSink;
pub const SimpleEventsHandler = simple_events_mod.EventsHandler;

pub const SnapshotRef = persistence_mod.SnapshotRef;
pub const JournalRef = persistence_mod.JournalRef;
pub const SnapshotDescriptorView = persistence_mod.SnapshotDescriptorView;
// Transitional alias used by the sharded facade, whose descriptor API is a
// separate migration slice.
pub const SnapshotDescriptor = persistence_mod.SnapshotDescriptor;
pub const JournalDescriptor = persistence_mod.JournalDescriptor;
pub const PersistenceModuleType = persistence_paths.ModuleType;
pub const PersistencePathError = persistence_paths.PathError;
const DescriptorInitError = persistence_mod.CreateSnapshotError || error{
    MatchingEngineCountOverflow,
};
const DescriptorTrackingError = persistence_mod.CreateSnapshotError ||
    persistence_mod.CreateJournalError ||
    persistence_mod.FinishJournalError ||
    error{
        InvalidPersistSnapshotId,
        InvalidPersistTimestamp,
        MissingPersistSequence,
        MatchingEngineCountOverflow,
        JournalSequenceNotMonotonic,
    };
pub const InitError = Config.ValidationError || kernel_mod.InitError || runtime_mod.InitError || serialization_processor.CreateError || std.mem.Allocator.Error || DescriptorInitError || std.Io.Dir.OpenError || std.Io.Dir.CreateDirPathError || journal_mod.JournalIoError || PersistencePathError || error{
    InvalidCapacity,
    CompletionCapacityOverflow,
};
pub const InitFromJavaConfigError = config_mod.PerformanceConfig.MappingError || InitError || error{
    SingleCoreRequiresOneMatchingEngine,
    SingleCoreRequiresOneRiskEngine,
};
pub const ExecuteError = LifecycleError || kernel_mod.ExecuteError || kernel_mod.ExchangeApi.AsyncSubmitError || DescriptorTrackingError || publication.PrepareError || journal_mod.JournalIoError || serialization_processor.JournalError;
pub const ReportError = LifecycleError || kernel_mod.ReportError;
pub const AsyncSubmitError = LifecycleError || kernel_mod.ExchangeApi.AsyncSubmitError || publication.PrepareError;
pub const EnqueueRuntimeError = LifecycleError || std.mem.Allocator.Error || publication.PrepareError;
pub const HelperError = kernel_mod.ExchangeApi.HelperError;
pub const PersistStateRequestPair = kernel_mod.ExchangeApi.PersistStateRequestPair;
pub const RuntimeDrainError = ExecuteError || runtime_mod.DrainError;
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
pub const SubmitApiCommandError = ExecuteError || HelperError || BinaryPayloadTypedSubmitError || BinaryReportQuerySubmitError;
pub const SubmitApiCommandAsyncError = AsyncSubmitError || HelperError || BinaryPayloadTypedAsyncSubmitError || BinaryReportQueryAsyncSubmitError;
pub const SubmitApiCommandWithCallbackError = SubmitApiCommandAsyncError || error{
    UnsupportedCallbackCommand,
    CallbackTimestampOverflow,
    GeneratedOrderIdOverflow,
};
pub const SubmitApiCommandBatchError = SubmitApiCommandAsyncError || ExecuteError || std.mem.Allocator.Error || error{
    EmptyApiCommandBatch,
    MissingBatchResult,
    UnexpectedFlushResultVariant,
};
pub const JournalApiError = LifecycleError || error{
    JournalingDisabled,
} || journal_mod.JournalIoError || DescriptorTrackingError;
pub const JournalReplayError = LifecycleError || JournalApiError || journal_mod.ReplayError;
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
pub const SnapshotExistsError = std.Io.Dir.StatFileError;
pub const DefaultSnapshotExistsError = SnapshotExistsError || PersistencePathError;
pub const PersistStateWithMainLogError = ExecuteError || PersistencePathError || StorageFolderEnsureError || MainLogWriteError || std.Io.Dir.RealPathFileError || error{
    ConfiguredMainLogRequiresDiskBackend,
    PersistStateRejected,
    PersistStateSnapshotMismatch,
    PersistStateSnapshotIdOverflow,
    PersistStateTimestampOverflow,
    PersistenceRootMismatch,
};
const PersistenceCoordinationError = snapshot_mod.DurableSnapshotError ||
    serialization_processor.StoreError ||
    PersistencePathError ||
    StorageFolderEnsureError ||
    persistence_mod.ReserveSnapshotError ||
    persistence_mod.ValidateNextSnapshotError ||
    std.Io.Dir.StatFileError ||
    error{
        PersistenceBackendDisabled,
        InvalidPersistSnapshotId,
        InvalidPersistTimestamp,
        PersistSequenceOverflow,
        PersistPairMismatch,
        SnapshotAlreadyCommitted,
        InjectedPersistenceFault,
        EngineCountOverflow,
        UnsupportedPersistenceTopology,
    };
pub const LifecycleError = error{
    CoreStopped,
    CorePoisoned,
    CoreQueueFull,
    CoreNotStarted,
    CoreAlreadyStarted,
    CoreReentrantCall,
};
pub const ShutdownState = runtime_mod.ShutdownState;
pub const ShutdownOutcome = runtime_mod.ShutdownOutcome;
pub const ShutdownError = std.Thread.SpawnError || std.Io.Event.WaitTimeoutError;
pub const CallbackConfigError = error{
    CallbackReconfigurationWouldBlock,
    CallbackRevisionOverflow,
};
pub const StartError = LifecycleError || std.Thread.SpawnError || serialization_processor.StartupError || error{
    ConfiguredPersistenceBootstrapRequired,
    DurableJournalFrontierMismatch,
};
pub const BootstrapOutcome = struct {
    snapshot_requested: bool = false,
    snapshot_found: bool = false,
    journal_replayed_commands: usize = 0,
};
pub const BootstrapError = LifecycleError || JournalReplayError || snapshot_mod.SnapshotIoError || snapshot_mod.SnapshotRestoreError ||
    snapshot_mod.DurableSnapshotRestoreError || PersistencePathError || error{
    SnapshotPathRequired,
    SnapshotFileNotFound,
    JournalPathRequired,
    DurableJournalFrontierMismatch,
};
pub const StartupError = StartError || BootstrapError;
pub const StartupFromConfiguredPersistenceError = StartupError || PersistencePathError || error{
    JournalReplayCountOverflow,
};
const MainLogSequenceParseError = error{
    MalformedMainLogEntry,
};
pub const StartupFromDiscoveredConfiguredPersistenceError = StartupFromConfiguredPersistenceError ||
    std.Io.Dir.OpenError ||
    std.Io.Dir.Iterator.Error ||
    std.Io.File.OpenError ||
    std.Io.Reader.Error;
pub const StartupFromDiscoveredConfiguredPersistenceStrictError = StartupFromDiscoveredConfiguredPersistenceError || MainLogSequenceParseError;
const ParityHelperError = error{
    UnexpectedReportVariant,
};
const BinaryFrameLayout = struct {
    words_without_padding: usize,
    frame_count: usize,
};

pub const PersistenceFaultPoint = enum {
    matching_before_write,
    matching_after_rename,
    risk_before_write,
    risk_after_rename,
    manifest_before_write,
};

const PendingPersistPair = struct {
    snapshot_id: u64,
    timestamp_ns: u64,
    matching_seq: u64,
    matching_store_succeeded: bool,
    matching_digest: ?snapshot_mod.PayloadDigest,
};

pub const CoreFatalInfo = runtime_mod.FatalRecord;

pub const CallbackFailureKind = enum {
    command_failed,
    skipped_after_fatal,
};

pub const CallbackFailure = struct {
    request_id: u64,
    kind: CallbackFailureKind,
    fatal: CoreFatalInfo,
};

/// XCJ3 is deliberately isolated from Java/XCJ1/XCJ2 `.ecj` files. Keeping a
/// native suffix makes an incompatible artifact a typed startup refusal rather
/// than a candidate for truncation or replacement.
const NativeJournalPartitionId: u16 = 1;

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

/// A Java-style per-request completion callback. The response is borrowed and
/// remains valid only until `callback` returns. `failure_callback` is optional
/// for source compatibility with Java's success-only Consumer<OrderCommand>,
/// but callers that need terminal notification after a fatal runtime error
/// should provide it.
pub const CommandResponseCallback = struct {
    context: ?*anyopaque = null,
    callback: *const fn (?*anyopaque, *const kernel_mod.CommandResponse) void,
    failure_callback: ?*const fn (?*anyopaque, CallbackFailure) void = null,
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

/// Completion ownership is exclusive from publication onward: a result is
/// either detached, poll-owned, or callback-owned. This makes callback/poll
/// stealing and submit-then-register races unrepresentable.
const CompletionOwner = union(enum) {
    detached,
    poll: u64,
    callback: CallbackCompletionOwner,

    fn requestId(self: CompletionOwner) ?u64 {
        return switch (self) {
            .detached => null,
            .poll => |request_id| request_id,
            .callback => |owner| owner.request_id,
        };
    }

    fn ownsPollSlot(self: CompletionOwner) bool {
        return switch (self) {
            .poll => true,
            .detached, .callback => false,
        };
    }
};

/// Completion is owned by the logical binary transfer, not by each carrier
/// frame. Intermediate frames are always detached; only the terminal frame
/// can publish a poll result or invoke a request callback.
const BinaryCompletionMode = union(enum) {
    synchronous,
    asynchronous,
    detached,
    callback: CommandResponseCallback,

    fn workKind(self: @This()) WorkKind {
        return switch (self) {
            .synchronous => .synchronous,
            .asynchronous, .callback => .asynchronous,
            .detached => .detached_api,
        };
    }

    fn ownsPollSlot(self: @This()) bool {
        return switch (self) {
            .synchronous, .asynchronous => true,
            .detached, .callback => false,
        };
    }

    fn ownsCallback(self: @This()) bool {
        return switch (self) {
            .callback => true,
            .synchronous, .asynchronous, .detached => false,
        };
    }

    fn hasRequestId(self: @This()) bool {
        return switch (self) {
            .detached => false,
            .synchronous, .asynchronous, .callback => true,
        };
    }

    fn countsAsAsync(self: @This()) bool {
        return switch (self) {
            .asynchronous, .callback => true,
            .synchronous, .detached => false,
        };
    }

    fn waitsForResponse(self: @This()) bool {
        return switch (self) {
            .synchronous => true,
            .asynchronous, .detached, .callback => false,
        };
    }

    fn terminalOwner(self: @This(), request_id: ?u64) CompletionOwner {
        return switch (self) {
            .synchronous, .asynchronous => .{ .poll = request_id.? },
            .detached => .detached,
            .callback => |callback| .{ .callback = .{
                .request_id = request_id.?,
                .callback = callback,
            } },
        };
    }
};

const CoreWorkItem = struct {
    sequence: publication.PublicationSequence,
    command: cmd.OrderCommand,
    completion: CompletionOwner,
    kind: WorkKind,
};

const PreparedCoreWork = struct {
    item: CoreWorkItem,
    journal_batch_position: serialization_processor.JournalBatchPosition,
};

const CoreWorkQueue = runtime_queue_mod.RuntimeQueue(CoreWorkItem);

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

const RuntimeDriverState = struct {
    const DeferredSettlementRange = struct {
        prefix_before: u64,
        prefix_after: u64,
    };

    allocator: std.mem.Allocator,
    io: std.Io,
    queue: CoreWorkQueue,
    mutex: std.Io.Mutex = .init,
    work_available: std.Io.Condition = .init,
    space_available: std.Io.Condition = .init,
    completion_available: std.Io.Condition = .init,
    owner: ?*ExchangeCore = null,
    worker: ?std.Thread = null,
    worker_id: ?std.Thread.Id = null,
    join_claimed: bool = false,
    close_requested: std.atomic.Value(bool) = .init(false),
    worker_quiesced: std.Io.Event = .unset,
    join_complete: std.Io.Event = .unset,
    accepted_total: u64 = 0,
    completed_total: u64 = 0,
    /// Largest accepted ordinal whose entire prefix has terminally settled.
    /// Unlike `completed_total`, this cannot be advanced by a later
    /// out-of-order binary-stream suffix.
    settled_prefix_total: u64 = 0,
    /// At most one binary stream is admitted at a time. If poison truncates its
    /// unpublished terminal suffix before an earlier callback returns, retain
    /// that one settled interval until the in-order prefix reaches it.
    deferred_settlement: ?DeferredSettlementRange = null,
    async_accepted_total: u64 = 0,
    async_completed_total: u64 = 0,
    async_drained_total: u64 = 0,
    runtime_accepted_total: u64 = 0,
    runtime_completed_total: u64 = 0,
    runtime_drained_total: u64 = 0,
    last_runtime_result: cmd.CommandResultCode = .accepted,
    completion_slots_used: usize = 0,
    callback_completions_pending: usize = 0,
    journal_slots_reserved: usize = 0,
    blocked_producers: usize = 0,
    accepted_drain_waiters: usize = 0,
    manual_drain_active: bool = false,
    active_cold_calls: usize = 0,
    bootstrap_owner: ?std.Thread.Id = null,
    /// A running binary transfer reserves its complete publication range, then
    /// exposes bounded chunks. While this gate is set, later publishers wait
    /// so the reserved range remains one FIFO-contiguous unit.
    active_binary_stream: bool = false,
    publication_sequencer: publication.PublicationSequencer = .init(0),
    /// Number of queue slots left in the available range captured by the sole
    /// consumer. Producers may append behind this deterministic frontier.
    prepared_available_remaining: usize = 0,

    fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        capacity: usize,
    ) CoreWorkQueue.InitError!RuntimeDriverState {
        return .{
            .allocator = allocator,
            .io = io,
            .queue = try CoreWorkQueue.init(allocator, capacity),
        };
    }

    fn deinit(self: *RuntimeDriverState) void {
        self.queue.deinit();
        self.* = undefined;
    }

    fn recordInOrderSettlementLocked(self: *RuntimeDriverState) void {
        std.debug.assert(self.completed_total < self.accepted_total);
        std.debug.assert(self.settled_prefix_total < self.accepted_total);
        self.completed_total += 1;
        self.settled_prefix_total += 1;
        if (self.deferred_settlement) |deferred| {
            std.debug.assert(self.settled_prefix_total <= deferred.prefix_before);
            if (self.settled_prefix_total == deferred.prefix_before) {
                self.settled_prefix_total = deferred.prefix_after;
                self.deferred_settlement = null;
            }
        }
        std.debug.assert(self.settled_prefix_total <= self.completed_total);
        std.debug.assert(self.completed_total <= self.accepted_total);
    }

    /// Records the admitted binary stream's never-published terminal suffix.
    /// This path is infallible after admission and cannot falsely advance the
    /// contiguous prefix past an earlier callback still in flight.
    fn recordDeferredTerminalSuffixLocked(
        self: *RuntimeDriverState,
        count: u64,
    ) void {
        std.debug.assert(count > 0);
        std.debug.assert(count <= self.accepted_total);
        std.debug.assert(self.deferred_settlement == null);
        const prefix_after = self.accepted_total;
        const prefix_before = prefix_after - count;
        std.debug.assert(self.settled_prefix_total <= prefix_before);
        std.debug.assert(self.completed_total <= self.accepted_total - count);
        self.completed_total += count;
        if (self.settled_prefix_total == prefix_before) {
            self.settled_prefix_total = prefix_after;
        } else {
            self.deferred_settlement = .{
                .prefix_before = prefix_before,
                .prefix_after = prefix_after,
            };
        }
        std.debug.assert(self.settled_prefix_total <= self.completed_total);
        std.debug.assert(self.completed_total <= self.accepted_total);
    }

    fn allAcceptedSettledLocked(self: *const RuntimeDriverState) bool {
        std.debug.assert(self.settled_prefix_total <= self.completed_total);
        std.debug.assert(self.completed_total <= self.accepted_total);
        const all_settled = self.settled_prefix_total == self.accepted_total;
        if (all_settled) {
            std.debug.assert(self.deferred_settlement == null);
            std.debug.assert(self.completed_total == self.accepted_total);
        }
        return all_settled;
    }
};

fn shutdownDeadlineExpired(timeout: std.Io.Timeout, io: std.Io) bool {
    return switch (timeout) {
        .none => false,
        .duration => unreachable,
        .deadline => |deadline| deadline.compare(
            .lte,
            std.Io.Clock.Timestamp.now(io, deadline.clock),
        ),
    };
}

fn waitForShutdownEvent(
    event: *std.Io.Event,
    io: std.Io,
    deadline: std.Io.Timeout,
) std.Io.Event.WaitTimeoutError!void {
    while (!event.isSet()) {
        event.waitTimeout(io, deadline) catch |err| {
            if (event.isSet()) return;
            switch (err) {
                error.Canceled => return error.Canceled,
                // Event.waitTimeout may report a spurious wake as Timeout.
                // Only the normalized absolute deadline decides expiration.
                error.Timeout => if (shutdownDeadlineExpired(deadline, io)) {
                    return error.Timeout;
                },
            }
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
    // An immediate timeout still permits one uncontended acquisition.
    if (mutex.tryLock()) return;
    while (true) {
        if (shutdownDeadlineExpired(deadline, io)) return error.Timeout;
        std.Thread.yield() catch {};
        if (shutdownDeadlineExpired(deadline, io)) return error.Timeout;
        if (mutex.tryLock()) {
            if (shutdownDeadlineExpired(deadline, io)) {
                mutex.unlock(io);
                return error.Timeout;
            }
            return;
        }
    }
}

fn observeCloseRequestLocked(driver: *RuntimeDriverState) void {
    if (!driver.close_requested.load(.acquire)) return;
    const owner = driver.owner orelse return;
    switch (owner.runtime.lifecycleState()) {
        .cold, .running => _ = owner.runtime.beginClose() catch unreachable,
        .closing, .stopped, .poisoned => {},
    }
    owner.stopped = true;
    driver.work_available.broadcast(driver.io);
    driver.space_available.broadcast(driver.io);
    driver.completion_available.broadcast(driver.io);
}

pub const ExchangeCore = struct {
    allocator: std.mem.Allocator,
    config: Config,
    custom_serialization_processor: ?serialization_processor.Processor,
    /// Independently opened directory handle used by every automatic durable
    /// persistence operation. It is absent for the dummy backend.
    persistence_root: ?std.Io.Dir,
    kernel: *kernel_mod.ExchangeKernel,
    journal: ?journal_mod.CommandJournal,
    durable_journal: ?journal_mod.DurableJournalAppender,
    descriptors: persistence_mod.DescriptorStore,
    current_snapshot_ref: SnapshotRef,
    last_journal_ref: ?JournalRef,
    api: kernel_mod.ExchangeApi,
    runtime: runtime_mod.ExchangeRuntime,
    raw_result_sink: RawResultSink,
    simple_events_processor: ?simple_events_mod.SimpleEventsProcessor,
    simple_events_handler: ?SimpleEventsHandler,
    simple_events_revision: u64,
    driver: *RuntimeDriverState,
    available_batch_test_gate: ?*AvailableBatchPauseTestGate = null,
    execution_mutex: std.Io.Mutex,
    callback_mutex: std.Io.Mutex,
    callback_execution_mutex: std.Io.Mutex,
    pending_persist_pair: ?PendingPersistPair,
    persistence_fault: ?PersistenceFaultPoint,
    initial_state_bootstrapped: bool,
    started: bool,
    stopped: bool,

    pub fn init(allocator: std.mem.Allocator, config: Config) InitError!ExchangeCore {
        return initWithIo(allocator, io_context.current(), config);
    }

    /// Uses the process working directory as the persistence root. The
    /// caller-supplied I/O implementation must remain valid through `deinit`.
    /// Concurrent users must pass a concurrency-capable I/O and an allocator
    /// that is safe for every producer plus the worker thread.
    pub fn initWithIo(
        allocator: std.mem.Allocator,
        io: std.Io,
        config: Config,
    ) InitError!ExchangeCore {
        return initWithIoAndPersistenceRootInternal(
            allocator,
            io,
            std.Io.Dir.cwd(),
            config,
            false,
        );
    }

    /// Opens and owns an independent handle for `persistence_root` when the
    /// disk backend is enabled. The supplied directory is borrowed only for
    /// this call and may be closed immediately after return. The owned handle
    /// is closed by `deinit`. The dummy backend never opens or closes it.
    pub fn initWithPersistenceRoot(
        allocator: std.mem.Allocator,
        persistence_root: std.Io.Dir,
        config: Config,
    ) InitError!ExchangeCore {
        return initWithIoAndPersistenceRoot(
            allocator,
            io_context.current(),
            persistence_root,
            config,
        );
    }

    /// Combines explicit runtime I/O with an explicit persistence root. The
    /// I/O value must remain valid through `deinit`; `persistence_root` itself
    /// is borrowed only during initialization because the core opens its own
    /// handle. Directory-open errors are reported only for the disk backend.
    pub fn initWithIoAndPersistenceRoot(
        allocator: std.mem.Allocator,
        io: std.Io,
        persistence_root: std.Io.Dir,
        config: Config,
    ) InitError!ExchangeCore {
        return initWithIoAndPersistenceRootInternal(
            allocator,
            io,
            persistence_root,
            config,
            true,
        );
    }

    fn initWithIoAndPersistenceRootInternal(
        allocator: std.mem.Allocator,
        io: std.Io,
        persistence_root: std.Io.Dir,
        config: Config,
        enable_durable_in_tests: bool,
    ) InitError!ExchangeCore {
        try config.validate();

        const owned_persistence_root: ?std.Io.Dir = switch (config.serialization.backend) {
            .dummy => null,
            .disk => try persistence_root.openDir(io, ".", .{}),
            .custom => null,
        };
        errdefer if (owned_persistence_root) |root_dir| root_dir.close(io);

        var custom_serialization_processor: ?serialization_processor.Processor =
            if (config.serialization.custom_processor_factory) |factory|
                try factory.createProcessor(allocator, .{
                    .exchange_id = config.initial_state.exchange_id,
                    .matching_engine_count = 1,
                    .risk_engine_count = 1,
                    .enable_journaling = config.serialization.enable_journaling,
                    .snapshot_id = config.initial_state.snapshot_id,
                    .snapshot_base_seq = config.initial_state.snapshot_base_seq,
                    .journal_timestamp_ns = config.initial_state.journal_timestamp_ns,
                    .throw_if_snapshot_not_found = config.initial_state.throw_if_snapshot_not_found,
                })
            else
                null;
        errdefer if (custom_serialization_processor) |*processor| {
            processor.deinit(allocator);
        };

        const kernel_ptr = try allocator.create(kernel_mod.ExchangeKernel);
        errdefer allocator.destroy(kernel_ptr);

        kernel_ptr.* = try kernel_mod.ExchangeKernel.init(allocator, config.toKernelConfig());
        errdefer kernel_ptr.deinit();

        var runtime = try runtime_mod.ExchangeRuntime.init(allocator, kernel_ptr, config.toRuntimeConfig());
        errdefer runtime.deinit();

        var descriptors = persistence_mod.DescriptorStore.init(allocator);
        errdefer descriptors.deinit();
        const matching_engine_count = std.math.cast(u32, config.matching.num_shards) orelse return error.MatchingEngineCountOverflow;
        const root_snapshot_ref = try descriptors.createEmptySnapshot(matching_engine_count, 1);

        var api = kernel_mod.ExchangeApi.init(kernel_ptr);
        errdefer api.deinit();
        const completion_capacity = std.math.cast(
            std.AutoHashMapUnmanaged(u64, kernel_mod.CommandResponse).Size,
            config.runtime.queue_capacity,
        ) orelse return error.CompletionCapacityOverflow;
        try api.ready_results.ensureTotalCapacity(
            allocator,
            completion_capacity,
        );
        var journal = if (config.serialization.enable_journaling)
            journal_mod.CommandJournal.init(allocator)
        else
            null;
        errdefer if (journal) |*command_journal| command_journal.deinit();

        var durable_journal: ?journal_mod.DurableJournalAppender = null;
        if (journal != null and
            config.serialization.backend == .disk and
            (!builtin.is_test or enable_durable_in_tests))
        {
            const root_dir = owned_persistence_root orelse unreachable;
            try persistence_paths.ensureDirPathDurable(
                io,
                root_dir,
                config.serialization.storage_folder,
            );
            const native_path = try nativeJournalPartitionPath(allocator, &config);
            defer allocator.free(native_path);
            durable_journal = try journal_mod.DurableJournalAppender.openWithIo(
                allocator,
                io,
                root_dir,
                native_path,
            );
        }
        errdefer if (durable_journal) |*appender| appender.deinit();

        const driver = try allocator.create(RuntimeDriverState);
        errdefer allocator.destroy(driver);
        driver.* = try RuntimeDriverState.init(allocator, io, config.runtime.queue_capacity);
        errdefer driver.deinit();

        return .{
            .allocator = allocator,
            .config = config,
            .custom_serialization_processor = custom_serialization_processor,
            .persistence_root = owned_persistence_root,
            .kernel = kernel_ptr,
            .journal = journal,
            .durable_journal = durable_journal,
            .descriptors = descriptors,
            .current_snapshot_ref = root_snapshot_ref,
            .last_journal_ref = null,
            .api = api,
            .runtime = runtime,
            .raw_result_sink = .{},
            .simple_events_processor = null,
            .simple_events_handler = null,
            .simple_events_revision = 0,
            .driver = driver,
            .execution_mutex = .init,
            .callback_mutex = .init,
            .callback_execution_mutex = .init,
            .pending_persist_pair = null,
            .persistence_fault = null,
            .initial_state_bootstrapped = false,
            .started = false,
            .stopped = false,
        };
    }

    pub fn initFromJavaConfig(
        allocator: std.mem.Allocator,
        java_config: JavaConfig,
    ) InitFromJavaConfigError!ExchangeCore {
        const config = try java_config.toExchangeCoreConfig();
        if (java_config.performance_cfg.matching_engines_num != 1) {
            return error.SingleCoreRequiresOneMatchingEngine;
        }
        if (java_config.performance_cfg.risk_engines_num != 1) {
            return error.SingleCoreRequiresOneRiskEngine;
        }
        return init(allocator, config);
    }

    /// The caller must exclude concurrent public calls, including concurrent
    /// shutdown waiters. Sequential deinit after a timed-out close is safe:
    /// its unbounded shutdown finishes or joins the retained worker first.
    pub fn deinit(self: *ExchangeCore) void {
        self.assertWiring();
        if (runtime_mod.isCallbackExecutionActive() or self.isWorkerThread()) {
            @panic("ExchangeCore.deinit cannot run from its worker callback");
        }
        self.shutdown();
        self.callback_execution_mutex.lockUncancelable(self.driver.io);
        self.callback_mutex.lockUncancelable(self.driver.io);
        self.raw_result_sink = .{};
        self.runtime.sink = .{};
        self.simple_events_handler = null;
        if (self.simple_events_processor) |*processor| processor.deinit();
        self.simple_events_processor = null;
        self.callback_mutex.unlock(self.driver.io);
        self.callback_execution_mutex.unlock(self.driver.io);
        self.runtime.deinit();
        self.api.deinit();
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
        self.kernel.deinit();
        self.allocator.destroy(self.kernel);
        if (self.persistence_root) |root_dir| {
            root_dir.close(self.driver.io);
            self.persistence_root = null;
        }
        self.driver.deinit();
        self.allocator.destroy(self.driver);
        self.* = undefined;
    }

    /// Low-level administrative escape hatch. The returned pointer is not
    /// synchronized with the runtime worker; mutate it only while the core is
    /// cold, or after shutdown/join. Production changes should use commands.
    pub fn kernelRef(self: *ExchangeCore) *kernel_mod.ExchangeKernel {
        self.assertWiring();
        return self.kernel;
    }

    pub fn configRef(self: *const ExchangeCore) *const Config {
        self.assertWiringConst();
        return &self.config;
    }

    /// Synchronization context shared by non-owning facades that must compose
    /// their own locks with core result ownership.
    pub fn ioContext(self: *const ExchangeCore) std.Io {
        self.assertWiringConst();
        return self.driver.io;
    }

    pub fn isStarted(self: *const ExchangeCore) bool {
        self.assertWiringConst();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        return self.runtime.lifecycleState() != .cold;
    }

    pub fn isStopped(self: *const ExchangeCore) bool {
        self.assertWiringConst();
        const driver = self.driver;
        if (driver.close_requested.load(.acquire)) return true;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        if (driver.close_requested.load(.acquire)) return true;
        return switch (self.runtime.lifecycleState()) {
            .closing, .stopped, .poisoned => true,
            .cold, .running => false,
        };
    }

    /// Starts the sole queue consumer. Once this succeeds, the ExchangeCore
    /// value must stay at this address until `shutdown`/`deinit` joins it.
    pub fn startup(self: *ExchangeCore) StartError!void {
        self.assertWiring();
        try self.ensureBlockingCallAllowed();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);

        try self.ensureAcceptingLocked();
        const current_thread = std.Thread.getCurrentId();
        while (driver.manual_drain_active or
            driver.active_cold_calls != 0 or
            (driver.bootstrap_owner != null and driver.bootstrap_owner.? != current_thread))
        {
            driver.completion_available.waitUncancelable(driver.io, &driver.mutex);
            try self.ensureAcceptingLocked();
        }
        switch (self.runtime.lifecycleState()) {
            .running => return,
            .poisoned => return error.CorePoisoned,
            .closing, .stopped => return error.CoreStopped,
            .cold => {},
        }
        if (self.custom_serialization_processor) |*processor| {
            try processor.startup(.{
                .exchange_id = self.config.initial_state.exchange_id,
                .snapshot_id = self.config.initial_state.snapshot_id,
                .snapshot_base_seq = self.config.initial_state.snapshot_base_seq,
                .journal_timestamp_ns = self.config.initial_state.journal_timestamp_ns,
                .throw_if_snapshot_not_found = self.config.initial_state.throw_if_snapshot_not_found,
                .enable_journaling = self.config.serialization.enable_journaling,
                .state = .{ .single = self.kernel },
            });
            // The Java custom processor owns snapshot restore, replay, and
            // journaling activation as one startup operation.
            self.initial_state_bootstrapped = true;
        }
        if ((self.config.initial_state.fromSnapshot() or
            self.config.initial_state.journal_timestamp_ns != 0) and
            !self.initial_state_bootstrapped)
        {
            return error.ConfiguredPersistenceBootstrapRequired;
        }
        try self.validateDurableJournalFrontier();
        try self.ensureAcceptingLocked();

        driver.owner = self;
        _ = self.runtime.start() catch unreachable;
        self.started = true;
        driver.worker = std.Thread.spawn(.{}, workerMain, .{driver}) catch |err| {
            self.runtime.lifecycle = .cold;
            driver.owner = null;
            self.started = false;
            return err;
        };
        driver.work_available.broadcast(driver.io);
    }

    pub fn startupFromPersistence(
        self: *ExchangeCore,
        dir: std.Io.Dir,
        snapshot_path: ?[]const u8,
        journal_path: ?[]const u8,
    ) StartupError!BootstrapOutcome {
        try self.beginBootstrap();
        defer self.finishBootstrap();
        const outcome = try self.bootstrapFromPersistenceStrict(dir, snapshot_path, journal_path);
        try self.validateDurableJournalFrontier();
        self.initial_state_bootstrapped = true;
        try self.startup();
        return outcome;
    }

    pub fn startupFromConfiguredPersistence(
        self: *ExchangeCore,
        dir: std.Io.Dir,
        snapshot_module_type: PersistenceModuleType,
        snapshot_instance_id: u32,
        journal_partition_id: u16,
    ) StartupFromConfiguredPersistenceError!BootstrapOutcome {
        try self.beginBootstrap();
        defer self.finishBootstrap();
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

        try self.validateDurableJournalFrontier();
        self.initial_state_bootstrapped = true;
        try self.startup();
        return outcome;
    }

    pub fn startupFromDiscoveredConfiguredPersistence(
        self: *ExchangeCore,
        dir: std.Io.Dir,
        snapshot_module_type: PersistenceModuleType,
        snapshot_instance_id: u32,
        journal_partition_id: u16,
    ) StartupFromDiscoveredConfiguredPersistenceError!BootstrapOutcome {
        try self.beginBootstrap();
        defer self.finishBootstrap();
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
        if (initial_state.fromSnapshot() and initial_state.throw_if_snapshot_not_found) {
            return error.SnapshotFileNotFound;
        }
        const outcome: BootstrapOutcome = .{
            .snapshot_requested = initial_state.fromSnapshot(),
            .snapshot_found = false,
            .journal_replayed_commands = 0,
        };
        try self.validateDurableJournalFrontier();
        self.initial_state_bootstrapped = true;
        try self.startup();
        return outcome;
    }

    pub fn startupFromDiscoveredConfiguredPersistenceStrictMainLog(
        self: *ExchangeCore,
        dir: std.Io.Dir,
        snapshot_module_type: PersistenceModuleType,
        snapshot_instance_id: u32,
        journal_partition_id: u16,
    ) StartupFromDiscoveredConfiguredPersistenceStrictError!BootstrapOutcome {
        try self.beginBootstrap();
        defer self.finishBootstrap();
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
        if (initial_state.fromSnapshot() and initial_state.throw_if_snapshot_not_found) {
            return error.SnapshotFileNotFound;
        }
        const outcome: BootstrapOutcome = .{
            .snapshot_requested = initial_state.fromSnapshot(),
            .snapshot_found = false,
            .journal_replayed_commands = 0,
        };
        try self.validateDurableJournalFrontier();
        self.initial_state_bootstrapped = true;
        try self.startup();
        return outcome;
    }

    fn startupFromConfiguredPersistenceWithSnapshotId(
        self: *ExchangeCore,
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

        try self.validateDurableJournalFrontier();
        self.initial_state_bootstrapped = true;
        try self.startup();
        return outcome;
    }

    fn startupFromConfiguredPersistenceWithSnapshotIdStrictMainLog(
        self: *ExchangeCore,
        dir: std.Io.Dir,
        snapshot_module_type: PersistenceModuleType,
        snapshot_instance_id: u32,
        journal_partition_id: u16,
        snapshot_id: u64,
    ) (StartupFromConfiguredPersistenceError || MainLogSequenceParseError)!BootstrapOutcome {
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

        try self.validateDurableJournalFrontier();
        self.initial_state_bootstrapped = true;
        try self.startup();
        return outcome;
    }

    fn bootstrapSnapshotPhaseWithId(
        self: *ExchangeCore,
        dir: std.Io.Dir,
        snapshot_path: []const u8,
        snapshot_id: u64,
        snapshot_base_seq: u64,
        outcome: *BootstrapOutcome,
    ) BootstrapError!void {
        const init_state = self.config.initial_state;
        outcome.snapshot_requested = true;

        const manifest_path = try self.defaultPersistenceSnapshotManifestPath(self.allocator, snapshot_id);
        defer self.allocator.free(manifest_path);
        if (try self.snapshotExistsFile(dir, manifest_path)) {
            if (self.kernel.customReportRegistry().len() != 0) {
                return error.LegacySnapshotCustomReportRegistryUnsupported;
            }
            const matching_path = try self.defaultPersistenceSnapshotPath(
                self.allocator,
                snapshot_id,
                .matching_engine_router,
                0,
            );
            defer self.allocator.free(matching_path);
            const risk_path = try self.defaultPersistenceSnapshotPath(
                self.allocator,
                snapshot_id,
                .risk_engine,
                0,
            );
            defer self.allocator.free(risk_path);
            var restore_failure_command = makeNopCommand();
            var restore_started = true;
            errdefer |failure| if (restore_started) {
                self.poisonDirectExecution(
                    publication.PublicationSequence.fromInt(self.kernel.next_seq),
                    &restore_failure_command,
                    failure,
                );
            };
            const manifest = try snapshot_mod.restoreCommittedPairIntoKernel(
                self.allocator,
                dir,
                manifest_path,
                matching_path,
                risk_path,
                self.kernel,
            );
            self.driver.publication_sequencer.restore(manifest.next_seq);
            try self.adoptDurableSnapshotFrontier(
                manifest.snapshot_id,
                manifest.next_seq,
                manifest.timestamp_ns,
            );
            outcome.snapshot_found = true;
            try self.registerSnapshotDescriptor(
                manifest.snapshot_id,
                manifest.next_seq,
                manifest.timestamp_ns,
            );
            restore_started = false;
            return;
        }

        var snapshot = snapshot_mod.KernelSnapshot.readFromFile(self.allocator, dir, snapshot_path) catch |err| switch (err) {
            error.FileNotFound => {
                if (init_state.throw_if_snapshot_not_found) return error.SnapshotFileNotFound;
                return;
            },
            else => return err,
        };
        defer snapshot.deinit();

        if (self.kernel.customReportRegistry().len() != 0) {
            return error.LegacySnapshotCustomReportRegistryUnsupported;
        }
        outcome.snapshot_found = true;
        var restore_failure_command = makeNopCommand();
        var restore_started = true;
        errdefer |failure| if (restore_started) {
            self.poisonDirectExecution(
                publication.PublicationSequence.fromInt(self.kernel.next_seq),
                &restore_failure_command,
                failure,
            );
        };
        try snapshot.restoreIntoKernel(self.kernel);
        self.kernel.next_seq = snapshot_base_seq;
        self.driver.publication_sequencer.restore(snapshot_base_seq);
        try self.adoptDurableSnapshotFrontier(snapshot_id, snapshot_base_seq, 0);
        try self.registerSnapshotDescriptor(
            snapshot_id,
            snapshot_base_seq,
            0,
        );
        restore_started = false;
    }

    fn discoverLatestDefaultSnapshotId(
        self: *const ExchangeCore,
        dir: std.Io.Dir,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) (std.Io.Dir.OpenError || std.Io.Dir.Iterator.Error || std.Io.File.OpenError || std.Io.Reader.Error)!?u64 {
        const storage_folder = self.config.serialization.storage_folder;
        var storage_dir = dir.openDir(io_context.current(), storage_folder, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer storage_dir.close(io_context.current());

        var it = storage_dir.iterate();
        var latest_snapshot_id: ?u64 = null;
        while (try it.next(io_context.current())) |entry| {
            // XCM1 is the commit advertisement for the durable XME1/XRE1
            // pair. Discover it directly so orphan payloads left by a failed
            // pre-manifest attempt can never outrank an older committed pair.
            if (instance_id == 0) {
                if (parseSnapshotIdFromManifestFileName(
                    entry.name,
                    self.config.initial_state.exchange_id,
                )) |snapshot_id| {
                    if (latest_snapshot_id == null or snapshot_id > latest_snapshot_id.?) {
                        latest_snapshot_id = snapshot_id;
                    }
                    continue;
                }
            }

            const snapshot_id = parseSnapshotIdFromDefaultFileName(
                entry.name,
                self.config.initial_state.exchange_id,
                module_type,
                instance_id,
            ) orelse continue;
            // Legacy monolithic snapshots use the same .ecs names. Only XCS1
            // and XCS2 are independently committed; XME1/XRE1 require XCM1.
            if (!try isLegacyStandaloneSnapshotFile(storage_dir, entry.name)) continue;
            if (latest_snapshot_id == null or snapshot_id > latest_snapshot_id.?) {
                latest_snapshot_id = snapshot_id;
            }
        }
        return latest_snapshot_id;
    }

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

    fn parseSnapshotIdFromManifestFileName(
        file_name: []const u8,
        exchange_id: []const u8,
    ) ?u64 {
        if (!std.mem.startsWith(u8, file_name, exchange_id)) return null;
        const marker = "_snapshot_";
        const extension = ".ecm";
        const prefix_len = exchange_id.len + marker.len;
        if (file_name.len <= prefix_len + extension.len) return null;
        if (!std.mem.eql(u8, file_name[exchange_id.len..prefix_len], marker)) return null;
        if (!std.mem.endsWith(u8, file_name, extension)) return null;

        const id_slice = file_name[prefix_len .. file_name.len - extension.len];
        if (id_slice.len == 0 or std.mem.indexOfScalar(u8, id_slice, '_') != null) return null;
        return std.fmt.parseInt(u64, id_slice, 10) catch null;
    }

    fn isLegacyStandaloneSnapshotFile(
        storage_dir: std.Io.Dir,
        file_name: []const u8,
    ) (std.Io.File.OpenError || std.Io.Reader.Error)!bool {
        const file = try storage_dir.openFile(io_context.current(), file_name, .{});
        defer file.close(io_context.current());
        var buffer: [4]u8 = undefined;
        var file_reader = file.reader(io_context.current(), &buffer);
        const magic = try file_reader.interface.takeArray(4);
        return std.mem.eql(u8, magic, "XCS1") or std.mem.eql(u8, magic, "XCS2");
    }

    fn discoverSnapshotBaseSeqFromMainLog(
        self: *const ExchangeCore,
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
        self: *const ExchangeCore,
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

    fn bootstrapSnapshotPhase(
        self: *ExchangeCore,
        dir: std.Io.Dir,
        snapshot_path: ?[]const u8,
        outcome: *BootstrapOutcome,
    ) BootstrapError!void {
        const init_state = self.config.initial_state;

        snapshot_phase: {
            if (!init_state.fromSnapshot()) break :snapshot_phase;
            outcome.snapshot_requested = true;

            const path = snapshot_path orelse {
                if (init_state.throw_if_snapshot_not_found) return error.SnapshotPathRequired;
                break :snapshot_phase;
            };

            try self.bootstrapSnapshotPhaseWithId(
                dir,
                path,
                init_state.snapshot_id,
                init_state.snapshot_base_seq,
                outcome,
            );
        }
    }

    fn bootstrapFromPersistenceStrict(
        self: *ExchangeCore,
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

    pub fn bootstrapFromPersistence(
        self: *ExchangeCore,
        dir: std.Io.Dir,
        snapshot_path: ?[]const u8,
        journal_path: ?[]const u8,
    ) BootstrapError!BootstrapOutcome {
        self.assertWiring();
        try self.beginBootstrap();
        defer self.finishBootstrap();
        const outcome = try self.bootstrapFromPersistenceStrict(
            dir,
            snapshot_path,
            journal_path,
        );
        try self.validateDurableJournalFrontier();
        self.initial_state_bootstrapped = true;
        return outcome;
    }

    /// Returns the public shutdown progress rather than exposing the runtime's
    /// internal lifecycle. A stopped or poisoned runtime is still `closing`
    /// until its worker handle has been safely joined.
    pub fn shutdownState(self: *const ExchangeCore) ShutdownState {
        self.assertWiringConst();
        const driver = self.driver;
        if (driver.join_complete.isSet()) return .complete;
        if (driver.close_requested.load(.acquire)) return .closing;

        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        if (driver.join_complete.isSet()) return .complete;
        if (driver.close_requested.load(.acquire)) return .closing;
        return switch (self.runtime.lifecycleState()) {
            .cold, .running => .open,
            .closing, .stopped, .poisoned => .closing,
        };
    }

    /// Fences admission immediately and waits at most through `timeout` for
    /// accepted work, callback delivery, and worker ownership to converge.
    /// Callback and worker callers only initiate closure: waiting for their
    /// own execution frame would deadlock, so they return `.deferred`.
    ///
    /// A cold timed/deferred close may start a worker. Once this method is
    /// called, the ExchangeCore value must remain at this address until
    /// `shutdownState()` is `.complete` or `deinit()` returns.
    /// If a self/callback caller cannot acquire the driver lock with its
    /// single nonblocking attempt, or a bounded caller reaches its deadline
    /// before acquiring it, the atomic fence remains in force but lifecycle
    /// handoff is not guaranteed: a condition wake may race registration. A
    /// later external shutdown call or deinit performs the required retry and
    /// completion.
    ///
    /// Zig 0.16 has no timed Thread.join. The deadline bounds application
    /// work, producer settlement, callbacks, and worker quiescence; after
    /// quiescence, the OS handle-reap tail is not a strict wall-clock bound.
    pub fn shutdownTimeout(
        self: *ExchangeCore,
        timeout: std.Io.Timeout,
    ) ShutdownError!ShutdownOutcome {
        self.assertWiring();
        const driver = self.driver;
        const deadline = timeout.toDeadline(driver.io);
        driver.close_requested.store(true, .release);
        // Conditions are atomic wait queues, so this lock-free broadcast is a
        // safe best-effort nudge. It is not a durable handoff: callers that
        // fail to acquire the control lock must rely on a later retry.
        driver.work_available.broadcast(driver.io);
        driver.space_available.broadcast(driver.io);
        driver.completion_available.broadcast(driver.io);
        if (driver.join_complete.isSet()) return .complete;
        const caller_is_callback = runtime_mod.isCallbackExecutionActive();
        const caller_is_worker = self.isWorkerThread();
        const caller_must_defer = caller_is_callback or caller_is_worker;
        const unbounded = switch (deadline) {
            .none => true,
            .duration => unreachable,
            .deadline => false,
        };
        var drain_inline = false;

        if (caller_must_defer) {
            if (!driver.mutex.tryLock()) return .deferred;
        } else {
            try lockShutdownDriver(&driver.mutex, driver.io, deadline);
        }
        if (driver.join_complete.isSet()) {
            driver.mutex.unlock(driver.io);
            return .complete;
        }

        switch (self.runtime.lifecycleState()) {
            .cold, .running => _ = self.runtime.beginClose() catch unreachable,
            .closing, .stopped, .poisoned => {},
        }
        self.stopped = true;
        driver.work_available.broadcast(driver.io);
        driver.space_available.broadcast(driver.io);
        driver.completion_available.broadcast(driver.io);

        const cold_backend = !self.started;
        const cold_blocked = driver.manual_drain_active or
            driver.active_cold_calls != 0 or
            driver.bootstrap_owner != null;

        if (cold_backend and driver.worker == null and !driver.join_claimed) {
            if (!cold_blocked and
                !driver.active_binary_stream and
                driver.queue.isEmpty() and
                driver.allAcceptedSettledLocked())
            {
                if (self.runtime.lifecycleState() == .closing) {
                    _ = self.runtime.markStopped() catch unreachable;
                }
                driver.join_claimed = true;
                driver.owner = null;
                driver.completion_available.broadcast(driver.io);
                driver.mutex.unlock(driver.io);
                driver.worker_quiesced.set(driver.io);
                // Final driver access publishes safe destruction.
                driver.join_complete.set(driver.io);
                return .complete;
            }

            // Preserve the allocation-free legacy path for an ordinary
            // unbounded cold shutdown. Timed and callback-initiated closure
            // needs a heap-stable owner that outlives the initiating frame.
            if (unbounded and !caller_must_defer) {
                driver.join_claimed = true;
                drain_inline = true;
            } else {
                driver.owner = self;
                driver.worker = std.Thread.spawn(.{}, workerMain, .{driver}) catch |err| {
                    driver.mutex.unlock(driver.io);
                    return err;
                };
            }
        }
        driver.mutex.unlock(driver.io);

        if (drain_inline) {
            self.drainColdShutdownInline();
            return .complete;
        }
        if (caller_must_defer) return .deferred;
        return self.awaitShutdownCompletion(deadline);
    }

    /// Source-compatible unbounded shutdown. Cancellation is retried because
    /// deinit relies on this method as its memory-safety barrier.
    pub fn shutdown(self: *ExchangeCore) void {
        while (true) {
            const outcome = self.shutdownTimeout(.none) catch |err| switch (err) {
                error.Canceled => continue,
                error.Timeout => unreachable,
                else => {
                    // Only a callback/cold-drainer spawn can fail here. Such a
                    // caller cannot synchronously take ownership without
                    // risking self-deadlock; the close fence remains active.
                    if (runtime_mod.isCallbackExecutionActive() or self.isWorkerThread()) return;
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

    fn awaitShutdownCompletion(
        self: *ExchangeCore,
        deadline: std.Io.Timeout,
    ) ShutdownError!ShutdownOutcome {
        const driver = self.driver;
        try waitForShutdownEvent(&driver.worker_quiesced, driver.io, deadline);

        var worker_to_join: ?std.Thread = null;
        try lockShutdownDriver(&driver.mutex, driver.io, deadline);
        if (driver.join_complete.isSet()) {
            driver.mutex.unlock(driver.io);
            return .complete;
        }
        if (!driver.join_claimed) {
            if (driver.worker) |worker| {
                driver.join_claimed = true;
                driver.worker = null;
                worker_to_join = worker;
            } else {
                // Defensive no-worker completion: reserve publication before
                // dropping the lock so no second caller can race destruction.
                driver.join_claimed = true;
                driver.owner = null;
                driver.mutex.unlock(driver.io);
                driver.join_complete.set(driver.io);
                return .complete;
            }
        }
        const another_joiner = worker_to_join == null;

        if (another_joiner) {
            driver.mutex.unlock(driver.io);
            try waitForShutdownEvent(&driver.join_complete, driver.io, deadline);
            return .complete;
        }
        driver.mutex.unlock(driver.io);

        // Native Thread.join has no timed form. It is called only after the
        // worker's release-published quiescence event, so no application work,
        // callback, or producer can extend this OS handle-reap tail.
        worker_to_join.?.join();
        // This release publication is the joiner's final driver access.
        // join_claimed stays monotonic; all later callers short-circuit on the
        // event. As documented on deinit, destruction is sequential with
        // public calls rather than racing still-returning waiters.
        driver.join_complete.set(driver.io);
        return .complete;
    }

    fn drainColdShutdownInline(self: *ExchangeCore) void {
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        while (driver.manual_drain_active or
            driver.active_cold_calls != 0 or
            driver.bootstrap_owner != null)
        {
            driver.completion_available.waitUncancelable(driver.io, &driver.mutex);
        }
        driver.mutex.unlock(driver.io);

        self.drainAcceptedOnCurrentThread();

        driver.mutex.lockUncancelable(driver.io);
        if (self.runtime.lifecycleState() == .closing) {
            _ = self.runtime.markStopped() catch unreachable;
        }
        std.debug.assert(driver.queue.isEmpty());
        std.debug.assert(!driver.active_binary_stream);
        std.debug.assert(driver.allAcceptedSettledLocked());
        std.debug.assert(driver.accepted_total == driver.completed_total);
        driver.owner = null;
        driver.completion_available.broadcast(driver.io);
        driver.space_available.broadcast(driver.io);
        driver.mutex.unlock(driver.io);
        driver.worker_quiesced.set(driver.io);
        // join_claimed remains true and this is the final driver access.
        driver.join_complete.set(driver.io);
    }

    pub fn journalingEnabled(self: *const ExchangeCore) bool {
        self.assertWiringConst();
        return self.journal != null;
    }

    pub fn journalLen(self: *ExchangeCore) LifecycleError!usize {
        self.assertWiring();
        const cold_claimed = try self.waitForAcceptedPrefix();
        defer if (cold_claimed) self.finishColdDirectCall();
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        return self.journalLenAssumeExecutionLocked();
    }

    fn journalLenAssumeExecutionLocked(self: *const ExchangeCore) usize {
        if (self.journal) |journal| {
            return journal.len();
        }
        return 0;
    }

    pub fn persistenceSnapshotCount(self: *ExchangeCore) LifecycleError!usize {
        self.assertWiring();
        const cold_claimed = try self.waitForAcceptedPrefix();
        defer if (cold_claimed) self.finishColdDirectCall();
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        return self.persistenceSnapshotCountAssumeExecutionLocked();
    }

    pub fn persistenceJournalCount(self: *ExchangeCore) LifecycleError!usize {
        self.assertWiring();
        const cold_claimed = try self.waitForAcceptedPrefix();
        defer if (cold_claimed) self.finishColdDirectCall();
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        return self.persistenceJournalCountAssumeExecutionLocked();
    }

    pub fn persistenceCurrentSnapshotRef(self: *ExchangeCore) LifecycleError!SnapshotRef {
        self.assertWiring();
        const cold_claimed = try self.waitForAcceptedPrefix();
        defer if (cold_claimed) self.finishColdDirectCall();
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        return self.persistenceCurrentSnapshotRefAssumeExecutionLocked();
    }

    pub fn persistenceCurrentSnapshot(self: *ExchangeCore) LifecycleError!SnapshotDescriptorView {
        self.assertWiring();
        const cold_claimed = try self.waitForAcceptedPrefix();
        defer if (cold_claimed) self.finishColdDirectCall();
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        return self.persistenceCurrentSnapshotAssumeExecutionLocked();
    }

    pub fn persistenceFindSnapshotById(
        self: *ExchangeCore,
        snapshot_id: u64,
    ) LifecycleError!?SnapshotRef {
        self.assertWiring();
        const cold_claimed = try self.waitForAcceptedPrefix();
        defer if (cold_claimed) self.finishColdDirectCall();
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        return self.persistenceFindSnapshotByIdAssumeExecutionLocked(snapshot_id);
    }

    pub fn persistenceSnapshotById(
        self: *ExchangeCore,
        snapshot_id: u64,
    ) LifecycleError!?SnapshotDescriptorView {
        self.assertWiring();
        const cold_claimed = try self.waitForAcceptedPrefix();
        defer if (cold_claimed) self.finishColdDirectCall();
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        return self.persistenceSnapshotByIdAssumeExecutionLocked(snapshot_id);
    }

    pub fn persistenceJournalByRef(
        self: *ExchangeCore,
        journal_ref: JournalRef,
    ) LifecycleError!JournalDescriptor {
        self.assertWiring();
        const cold_claimed = try self.waitForAcceptedPrefix();
        defer if (cold_claimed) self.finishColdDirectCall();
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        return self.persistenceJournalByRefAssumeExecutionLocked(journal_ref);
    }

    pub fn persistenceFindJournalForSequence(
        self: *ExchangeCore,
        sequence: u64,
    ) LifecycleError!?JournalRef {
        self.assertWiring();
        const cold_claimed = try self.waitForAcceptedPrefix();
        defer if (cold_claimed) self.finishColdDirectCall();
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        return self.persistenceFindJournalForSequenceAssumeExecutionLocked(sequence);
    }

    fn persistenceSnapshotCountAssumeExecutionLocked(self: *const ExchangeCore) usize {
        self.assertDescriptorWiringAssumeExecutionLocked();
        return self.descriptors.snapshotCount();
    }

    fn persistenceJournalCountAssumeExecutionLocked(self: *const ExchangeCore) usize {
        self.assertDescriptorWiringAssumeExecutionLocked();
        return self.descriptors.journalCount();
    }

    fn persistenceCurrentSnapshotRefAssumeExecutionLocked(self: *const ExchangeCore) SnapshotRef {
        self.assertDescriptorWiringAssumeExecutionLocked();
        return self.current_snapshot_ref;
    }

    fn persistenceCurrentSnapshotAssumeExecutionLocked(self: *const ExchangeCore) SnapshotDescriptorView {
        self.assertDescriptorWiringAssumeExecutionLocked();
        return self.descriptors.snapshotView(self.current_snapshot_ref);
    }

    fn persistenceFindSnapshotByIdAssumeExecutionLocked(
        self: *const ExchangeCore,
        snapshot_id: u64,
    ) ?SnapshotRef {
        self.assertDescriptorWiringAssumeExecutionLocked();
        return self.descriptors.findSnapshotById(snapshot_id);
    }

    fn persistenceSnapshotByIdAssumeExecutionLocked(
        self: *const ExchangeCore,
        snapshot_id: u64,
    ) ?SnapshotDescriptorView {
        const snapshot_ref = self.persistenceFindSnapshotByIdAssumeExecutionLocked(snapshot_id) orelse return null;
        return self.descriptors.snapshotView(snapshot_ref);
    }

    fn persistenceJournalByRefAssumeExecutionLocked(
        self: *const ExchangeCore,
        journal_ref: JournalRef,
    ) JournalDescriptor {
        self.assertDescriptorWiringAssumeExecutionLocked();
        return self.descriptors.journalValue(journal_ref);
    }

    fn persistenceFindJournalForSequenceAssumeExecutionLocked(
        self: *const ExchangeCore,
        sequence: u64,
    ) ?JournalRef {
        self.assertDescriptorWiringAssumeExecutionLocked();
        return self.descriptors.findJournalForSequence(self.current_snapshot_ref, sequence);
    }

    pub fn defaultPersistenceSnapshotFileName(
        self: *const ExchangeCore,
        allocator: std.mem.Allocator,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) persistence_paths.FileNameError![]u8 {
        self.assertWiringConst();
        return persistence_paths.snapshotFileName(
            allocator,
            self.config.initial_state.exchange_id,
            snapshot_id,
            module_type,
            instance_id,
        );
    }

    pub fn defaultPersistenceJournalFileName(
        self: *const ExchangeCore,
        allocator: std.mem.Allocator,
        snapshot_id: u64,
        partition_id: u16,
    ) persistence_paths.FileNameError![]u8 {
        self.assertWiringConst();
        return persistence_paths.journalFileName(
            allocator,
            self.config.initial_state.exchange_id,
            snapshot_id,
            partition_id,
        );
    }

    pub fn defaultPersistenceMainLogFileName(
        self: *const ExchangeCore,
        allocator: std.mem.Allocator,
    ) persistence_paths.FileNameError![]u8 {
        self.assertWiringConst();
        return persistence_paths.mainLogFileName(
            allocator,
            self.config.initial_state.exchange_id,
        );
    }

    pub fn defaultPersistenceSnapshotPath(
        self: *const ExchangeCore,
        allocator: std.mem.Allocator,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) persistence_paths.PathError![]u8 {
        self.assertWiringConst();
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
        self: *const ExchangeCore,
        allocator: std.mem.Allocator,
        snapshot_id: u64,
        partition_id: u16,
    ) persistence_paths.PathError![]u8 {
        self.assertWiringConst();
        return persistence_paths.journalPath(
            allocator,
            self.config.serialization.storage_folder,
            self.config.initial_state.exchange_id,
            snapshot_id,
            partition_id,
        );
    }

    pub fn defaultPersistenceMainLogPath(
        self: *const ExchangeCore,
        allocator: std.mem.Allocator,
    ) persistence_paths.PathError![]u8 {
        self.assertWiringConst();
        return persistence_paths.mainLogPath(
            allocator,
            self.config.serialization.storage_folder,
            self.config.initial_state.exchange_id,
        );
    }

    pub fn defaultPersistenceSnapshotManifestPath(
        self: *const ExchangeCore,
        allocator: std.mem.Allocator,
        snapshot_id: u64,
    ) persistence_paths.PathError![]u8 {
        self.assertWiringConst();
        return persistence_paths.snapshotManifestPath(
            allocator,
            self.config.serialization.storage_folder,
            self.config.initial_state.exchange_id,
            snapshot_id,
        );
    }

    /// Deterministic, one-shot storage fault injection used by persistence
    /// simulation tests. Production callers should leave this unset.
    pub fn setPersistenceFaultForTesting(
        self: *ExchangeCore,
        fault: ?PersistenceFaultPoint,
    ) void {
        self.assertWiring();
        self.persistence_fault = fault;
    }

    pub fn writeJournalToDefaultPath(
        self: *ExchangeCore,
        dir: std.Io.Dir,
        snapshot_id: u64,
        partition_id: u16,
    ) DefaultJournalApiError!void {
        self.assertWiring();
        const cold_claimed = try self.waitForAcceptedPrefix();
        defer if (cold_claimed) self.finishColdDirectCall();
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        return self.writeJournalToDefaultPathAssumeExecutionLocked(
            dir,
            snapshot_id,
            partition_id,
        );
    }

    fn writeJournalToDefaultPathAssumeExecutionLocked(
        self: *ExchangeCore,
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
        try self.writeJournalToFileAssumeExecutionLocked(dir, path);
    }

    pub fn loadJournalFromDefaultPath(
        self: *ExchangeCore,
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
        self: *ExchangeCore,
        dir: std.Io.Dir,
        snapshot_id: u64,
        partition_id: u16,
    ) DefaultJournalReplayError!usize {
        self.assertWiring();
        const already_owned = try self.bootstrapOwnedByCurrentThread();
        if (!already_owned) try self.beginBootstrap();
        defer if (!already_owned) self.finishBootstrap();
        return self.replayJournalFromDefaultPathAssumeBootstrapOwned(
            dir,
            snapshot_id,
            partition_id,
        );
    }

    fn replayJournalFromDefaultPathAssumeBootstrapOwned(
        self: *ExchangeCore,
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
        return self.replayJournalFromFileAssumeBootstrapOwned(dir, path);
    }

    pub fn replayJournalFromDefaultPathRange(
        self: *ExchangeCore,
        dir: std.Io.Dir,
        snapshot_id: u64,
        starting_partition_id: u16,
    ) DefaultJournalReplayRangeError!usize {
        self.assertWiring();
        const already_owned = try self.bootstrapOwnedByCurrentThread();
        if (!already_owned) try self.beginBootstrap();
        defer if (!already_owned) self.finishBootstrap();

        var total_replayed: usize = 0;
        var partition_id = starting_partition_id;
        while (true) {
            const replayed = self.replayJournalFromDefaultPathAssumeBootstrapOwned(
                dir,
                snapshot_id,
                partition_id,
            ) catch |err| switch (err) {
                error.FileNotFound => break,
                else => return err,
            };
            total_replayed = std.math.add(
                usize,
                total_replayed,
                replayed,
            ) catch return error.JournalReplayCountOverflow;
            if (partition_id == std.math.maxInt(u16)) break;
            partition_id += 1;
        }
        return total_replayed;
    }

    pub fn captureSnapshotToDefaultPath(
        self: *ExchangeCore,
        dir: std.Io.Dir,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
        symbols: []const i32,
        depth: usize,
    ) DefaultSnapshotWriteError!void {
        self.assertWiring();
        const cold_claimed = try self.waitForAcceptedPrefix();
        defer if (cold_claimed) self.finishColdDirectCall();
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        return self.captureSnapshotToDefaultPathAssumeExecutionLocked(
            dir,
            snapshot_id,
            module_type,
            instance_id,
            symbols,
            depth,
        );
    }

    fn captureSnapshotToDefaultPathAssumeExecutionLocked(
        self: *ExchangeCore,
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
        try self.captureSnapshotToFileAssumeExecutionLocked(
            dir,
            path,
            symbols,
            depth,
        );
        try self.appendDefaultPersistenceMainLogEntry(
            dir,
            self.kernel.next_seq,
            currentWallClockTimestampNs(),
            snapshot_id,
            module_type,
            instance_id,
        );
    }

    pub fn validateSnapshotDefaultPath(
        self: *ExchangeCore,
        dir: std.Io.Dir,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) DefaultSnapshotValidateError!void {
        self.assertWiring();
        const cold_claimed = try self.waitForAcceptedPrefix();
        defer if (cold_claimed) self.finishColdDirectCall();
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        return self.validateSnapshotDefaultPathAssumeExecutionLocked(
            dir,
            snapshot_id,
            module_type,
            instance_id,
        );
    }

    fn validateSnapshotDefaultPathAssumeExecutionLocked(
        self: *ExchangeCore,
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
        try self.validateSnapshotFileAssumeExecutionLocked(
            dir,
            path,
        );
    }

    pub fn snapshotExistsDefaultPath(
        self: *const ExchangeCore,
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

    fn ensureConfiguredStorageFolderExists(
        self: *const ExchangeCore,
        dir: std.Io.Dir,
    ) StorageFolderEnsureError!void {
        self.assertWiringConst();
        try persistence_paths.ensureDirPathDurable(
            io_context.current(),
            dir,
            self.config.serialization.storage_folder,
        );
    }

    fn appendDefaultPersistenceMainLogEntry(
        self: *const ExchangeCore,
        dir: std.Io.Dir,
        seq: u64,
        timestamp_ns: u64,
        snapshot_id: u64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) (PersistencePathError || StorageFolderEnsureError || MainLogWriteError)!void {
        self.assertWiringConst();
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

    pub fn writeJournalToFile(
        self: *ExchangeCore,
        dir: std.Io.Dir,
        path: []const u8,
    ) JournalApiError!void {
        self.assertWiring();
        const cold_claimed = try self.waitForAcceptedPrefix();
        defer if (cold_claimed) self.finishColdDirectCall();
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        return self.writeJournalToFileAssumeExecutionLocked(dir, path);
    }

    fn writeJournalToFileAssumeExecutionLocked(
        self: *ExchangeCore,
        dir: std.Io.Dir,
        path: []const u8,
    ) JournalApiError!void {
        if (self.journal) |*journal| {
            try journal.writeToFile(dir, path);
            try self.syncJournalDescriptorFromCurrentSnapshot();
            return;
        }
        return error.JournalingDisabled;
    }

    pub fn captureSnapshotToFile(
        self: *ExchangeCore,
        dir: std.Io.Dir,
        path: []const u8,
        symbols: []const i32,
        depth: usize,
    ) SnapshotWriteError!void {
        self.assertWiring();
        const cold_claimed = try self.waitForAcceptedPrefix();
        defer if (cold_claimed) self.finishColdDirectCall();
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        return self.captureSnapshotToFileAssumeExecutionLocked(
            dir,
            path,
            symbols,
            depth,
        );
    }

    fn captureSnapshotToFileAssumeExecutionLocked(
        self: *ExchangeCore,
        dir: std.Io.Dir,
        path: []const u8,
        symbols: []const i32,
        depth: usize,
    ) SnapshotWriteError!void {
        if (self.kernel.customReportRegistry().len() != 0) {
            return error.LegacySnapshotCustomReportRegistryUnsupported;
        }
        var snapshot = try snapshot_mod.KernelSnapshot.captureFromKernel(
            self.allocator,
            self.kernel,
            symbols,
            depth,
        );
        defer snapshot.deinit();
        try snapshot.writeToFile(dir, path);
    }

    pub fn validateSnapshotFile(
        self: *ExchangeCore,
        dir: std.Io.Dir,
        path: []const u8,
    ) SnapshotValidateError!void {
        self.assertWiring();
        const cold_claimed = try self.waitForAcceptedPrefix();
        defer if (cold_claimed) self.finishColdDirectCall();
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        return self.validateSnapshotFileAssumeExecutionLocked(dir, path);
    }

    fn validateSnapshotFileAssumeExecutionLocked(
        self: *ExchangeCore,
        dir: std.Io.Dir,
        path: []const u8,
    ) SnapshotValidateError!void {
        var snapshot = try snapshot_mod.KernelSnapshot.readFromFile(self.allocator, dir, path);
        defer snapshot.deinit();
        try snapshot.validateAgainstKernel(self.kernel);
    }

    pub fn snapshotExistsFile(
        self: *const ExchangeCore,
        dir: std.Io.Dir,
        path: []const u8,
    ) SnapshotExistsError!bool {
        self.assertWiringConst();
        _ = dir.statFile(io_context.current(), path, .{}) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
        return true;
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

    pub fn loadJournalFromFile(
        self: *ExchangeCore,
        dir: std.Io.Dir,
        path: []const u8,
    ) JournalApiError!void {
        self.assertWiring();
        try self.beginBootstrap();
        defer self.finishBootstrap();
        if (self.journal) |*journal| {
            const loaded = try journal_mod.CommandJournal.readFromFile(self.allocator, dir, path);
            journal.deinit();
            journal.* = loaded;
            try self.syncJournalDescriptorFromCurrentSnapshot();
            return;
        }
        return error.JournalingDisabled;
    }

    pub fn replayJournalFromFile(
        self: *ExchangeCore,
        dir: std.Io.Dir,
        path: []const u8,
    ) JournalReplayError!usize {
        self.assertWiring();
        const already_owned = try self.bootstrapOwnedByCurrentThread();
        if (!already_owned) try self.beginBootstrap();
        defer if (!already_owned) self.finishBootstrap();
        return self.replayJournalFromFileAssumeBootstrapOwned(dir, path);
    }

    fn replayJournalFromFileAssumeBootstrapOwned(
        self: *ExchangeCore,
        dir: std.Io.Dir,
        path: []const u8,
    ) JournalReplayError!usize {
        if (self.journal) |*journal| {
            var loaded = try journal_mod.CommandJournal.readFromFile(self.allocator, dir, path);
            var loaded_owned = true;
            errdefer if (loaded_owned) loaded.deinit();

            // The restored kernel is the source of truth. In particular, a
            // durable manifest can be newer than the configured snapshot base.
            const sequence_exclusive = self.kernel.next_seq;
            self.driver.publication_sequencer.restore(sequence_exclusive);
            const replay_limit = self.config.initial_state.journal_timestamp_ns;
            const replay_limit_i64: i64 = if (replay_limit >= @as(u64, @intCast(std.math.maxInt(i64))))
                std.math.maxInt(i64)
            else
                @as(i64, @intCast(replay_limit));
            const current_snapshot = self.persistenceCurrentSnapshotAssumeExecutionLocked();
            const restored_snapshot: ?journal_mod.RestoredSnapshotFrontier = if (current_snapshot.seq == 0)
                null
            else
                .{
                    .snapshot_id = current_snapshot.snapshot_id,
                    .sequence = current_snapshot.seq,
                };
            const replay_frontier = try loaded.replayFrontier(
                sequence_exclusive,
                replay_limit,
                restored_snapshot,
            );

            var replayed: usize = 0;
            var replay_mutation_started = false;
            var replay_failure_sequence = publication.PublicationSequence.fromInt(0);
            var replay_failure_command = makeNopCommand();
            errdefer |failure| if (replay_mutation_started) {
                self.poisonDirectExecution(
                    replay_failure_sequence,
                    &replay_failure_command,
                    failure,
                );
            };

            for (loaded.entries.items) |entry| {
                if (entry.sequence <= sequence_exclusive) continue;
                if (entry.sequence > replay_frontier) break;
                std.debug.assert(entry.input.timestamp <= replay_limit_i64);

                const assigned = publication.PublicationSequence.fromInt(entry.sequence);
                replay_failure_sequence = assigned;
                replay_failure_command = entry.input;
                if (entry.sequence <= self.driver.publication_sequencer.highWater()) {
                    return error.SequenceMismatch;
                }
                self.driver.publication_sequencer.observe(assigned) catch
                    return error.SequenceMismatch;
                replay_mutation_started = true;
                var replay_command = entry.input;
                var replay_response = try self.kernel.executeAtSequence(
                    assigned,
                    &replay_command,
                );
                defer replay_response.deinit(self.allocator);
                if (replay_response.sequence != entry.sequence) {
                    return error.SequenceMismatch;
                }
                if (replay_response.result_code != entry.result_code) {
                    return error.ResultMismatch;
                }
                try self.registerReplayedPersistDescriptorIfNeeded(
                    &replay_command,
                    replay_response.sequence,
                    replay_response.result_code,
                );
                replayed += 1;
            }

            if (replay_frontier > self.kernel.next_seq) {
                replay_failure_sequence = publication.PublicationSequence.fromInt(replay_frontier);
                replay_failure_command = makeNopCommand();
                replay_mutation_started = true;
                self.kernel.next_seq = replay_frontier;
                self.driver.publication_sequencer.restore(replay_frontier);
            }
            loaded.retainSequenceRange(sequence_exclusive, replay_frontier);

            journal.deinit();
            journal.* = loaded;
            loaded_owned = false;
            try self.syncJournalDescriptorFromCurrentSnapshot();
            return replayed;
        }
        return error.JournalingDisabled;
    }

    pub fn submitCommand(self: *ExchangeCore, command: *cmd.OrderCommand) ExecuteError!kernel_mod.CommandResponse {
        self.assertWiring();
        if (!try self.shouldUseWorkerForSynchronousCall()) {
            defer self.finishColdDirectCall();
            return self.executeCommandDirect(command);
        }

        var ids: [1]u64 = undefined;
        try self.enqueueResponseBatch(&.{command.*}, .synchronous, ids[0..]);
        const response = try self.waitTakeResponse(ids[0]);
        command.result_code = response.result_code;
        return response;
    }

    fn submitRawApiCommandSync(
        self: *ExchangeCore,
        command: *cmd.OrderCommand,
    ) ExecuteError!kernel_mod.CommandResponse {
        if (!try self.shouldUseWorkerForSynchronousCall()) {
            defer self.finishColdDirectCall();
            return self.executeCommandDirect(command);
        }

        var ids: [1]u64 = undefined;
        try self.enqueueResponseBatch(&.{command.*}, .synchronous_raw, ids[0..]);
        const response = try self.waitTakeResponse(ids[0]);
        command.result_code = response.result_code;
        return response;
    }

    pub fn submitCommandBatch(
        self: *ExchangeCore,
        commands: []*cmd.OrderCommand,
    ) ExecuteError!kernel_mod.BatchResponse {
        self.assertWiring();
        if (commands.len == 0) return .{};
        if (!try self.shouldUseWorkerForSynchronousCall()) {
            defer self.finishColdDirectCall();
            const sequences = try self.prepareColdPublicationRange(commands.len);
            var response: kernel_mod.BatchResponse = .{};
            var has_success = false;
            var first_failure: ?cmd.CommandResultCode = null;

            for (commands, 0..) |command, index| {
                var single = try self.executePreparedDirect(
                    sequences.at(index),
                    command,
                    if (index + 1 == commands.len) .terminal else .continues,
                );
                defer single.deinit(self.allocator);

                if (response.sequence_from == null) {
                    response.sequence_from = single.sequence;
                }
                response.sequence_to = single.sequence;

                switch (single.result_code) {
                    .accepted => {},
                    .success => has_success = true,
                    else => {
                        if (first_failure == null) {
                            first_failure = single.result_code;
                        }
                    },
                }
            }

            response.result_code = if (first_failure) |failed|
                failed
            else if (has_success)
                .success
            else
                .accepted;
            return response;
        }

        var command_values = try self.allocator.alloc(cmd.OrderCommand, commands.len);
        defer self.allocator.free(command_values);
        const ids = try self.allocator.alloc(u64, commands.len);
        defer self.allocator.free(ids);
        for (commands, 0..) |command, index| command_values[index] = command.*;
        try self.enqueueResponseBatch(command_values, .synchronous, ids);

        var response: kernel_mod.BatchResponse = .{};
        var has_success = false;
        var first_failure: ?cmd.CommandResultCode = null;

        for (commands, ids) |command, request_id| {
            var single = try self.waitTakeResponse(request_id);
            defer single.deinit(self.allocator);
            command.result_code = single.result_code;

            if (response.sequence_from == null) {
                response.sequence_from = single.sequence;
            }
            response.sequence_to = single.sequence;

            switch (single.result_code) {
                .accepted => {},
                .success => has_success = true,
                else => {
                    if (first_failure == null) {
                        first_failure = single.result_code;
                    }
                },
            }
        }

        response.result_code = if (first_failure) |failed|
            failed
        else if (has_success)
            .success
        else
            .accepted;
        return response;
    }

    pub fn queryReport(self: *ExchangeCore, query: kernel_mod.ReportQuery) ReportError!kernel_mod.ReportResponse {
        self.assertWiring();
        const cold_claimed = try self.waitForAcceptedPrefix();
        defer if (cold_claimed) self.finishColdDirectCall();
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        return self.api.queryReport(query);
    }

    pub fn submitCommandAsync(self: *ExchangeCore, command: *const cmd.OrderCommand) AsyncSubmitError!u64 {
        self.assertWiring();
        var ids: [1]u64 = undefined;
        try self.enqueueResponseBatch(&.{command.*}, .asynchronous, ids[0..]);
        return ids[0];
    }

    fn submitRawApiCommandAsync(self: *ExchangeCore, command: cmd.OrderCommand) AsyncSubmitError!u64 {
        var ids: [1]u64 = undefined;
        try self.enqueueResponseBatch(&.{command}, .asynchronous_raw, ids[0..]);
        return ids[0];
    }

    /// Waits until the accepted prefix visible at entry has terminally settled.
    /// A poisoned prefix still discharges callback, detached, and poll owners
    /// before this returns `CorePoisoned`.
    pub fn drainAsyncPending(self: *ExchangeCore) ExecuteError!usize {
        self.assertWiring();
        if (runtime_mod.isCallbackExecutionActive() or self.isWorkerThread()) {
            return error.CoreReentrantCall;
        }
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        const initial_state = self.runtime.lifecycleState();
        switch (initial_state) {
            .cold, .running => if (driver.close_requested.load(.acquire)) {
                driver.mutex.unlock(driver.io);
                return error.CoreStopped;
            },
            .poisoned => {},
            .closing, .stopped => {
                driver.mutex.unlock(driver.io);
                return error.CoreStopped;
            },
        }
        if (initial_state == .cold and driver.bootstrap_owner != null) {
            driver.mutex.unlock(driver.io);
            return error.CoreNotStarted;
        }

        // The async frontier determines the returned count. The global
        // accepted frontier is the terminal-settlement barrier: detached work
        // is deliberately absent from async_accepted_total, but its ownership
        // must still be discharged before this accepted prefix can return.
        const settlement_target = driver.accepted_total;
        const target = driver.async_accepted_total;

        // A poisoned cold core can have accepted work but no worker when a
        // direct execution publishes the first fatal record. Let the first
        // external drain own terminal settlement; concurrent drains wait on
        // the captured completion frontier without competing for the queue.
        const cold_backend = !self.started;
        if ((initial_state == .cold or
            (initial_state == .poisoned and cold_backend)) and
            driver.bootstrap_owner != null)
        {
            driver.mutex.unlock(driver.io);
            return error.CoreNotStarted;
        }
        var drain_on_current_thread = false;
        if (driver.settled_prefix_total < settlement_target and initial_state == .cold) {
            if (!driver.manual_drain_active) {
                driver.manual_drain_active = true;
                drain_on_current_thread = true;
            }
        } else if (driver.settled_prefix_total < settlement_target and
            initial_state == .poisoned and
            cold_backend and
            !driver.manual_drain_active and
            driver.active_cold_calls == 0 and
            driver.bootstrap_owner == null and
            driver.worker == null and
            !driver.join_claimed)
        {
            driver.manual_drain_active = true;
            drain_on_current_thread = true;
        }
        driver.mutex.unlock(driver.io);

        while (true) {
            if (drain_on_current_thread) {
                self.drainAcceptedOnCurrentThread();
                self.finishManualDrain();
                drain_on_current_thread = false;
            }

            driver.mutex.lockUncancelable(driver.io);
            if (driver.settled_prefix_total >= settlement_target) {
                if (self.runtime.lifecycleState() == .poisoned) {
                    driver.mutex.unlock(driver.io);
                    return error.CorePoisoned;
                }
                std.debug.assert(driver.async_completed_total >= target);
                const accounted = driver.async_drained_total;
                const processed_u64 = if (target > accounted) target - accounted else 0;
                if (target > accounted) driver.async_drained_total = target;
                driver.mutex.unlock(driver.io);
                return std.math.cast(usize, processed_u64) orelse
                    std.math.maxInt(usize);
            }

            const state = self.runtime.lifecycleState();
            if (state == .stopped) {
                driver.mutex.unlock(driver.io);
                return error.CoreStopped;
            }

            // A cold drain that initially waited behind another manual drainer
            // must be able to take ownership after that drainer releases it.
            // Cold publishers are intentionally allowed during a manual drain,
            // so a one-shot ownership decision can otherwise leave accepted
            // work queued forever with no worker.
            if (!self.started and
                (state == .cold or state == .poisoned) and
                !driver.manual_drain_active and
                driver.active_cold_calls == 0 and
                driver.bootstrap_owner == null and
                driver.worker == null and
                !driver.join_claimed)
            {
                driver.manual_drain_active = true;
                drain_on_current_thread = true;
                driver.mutex.unlock(driver.io);
                continue;
            }

            driver.accepted_drain_waiters += 1;
            driver.completion_available.broadcast(driver.io);
            driver.completion_available.waitUncancelable(driver.io, &driver.mutex);
            std.debug.assert(driver.accepted_drain_waiters > 0);
            driver.accepted_drain_waiters -= 1;
            driver.mutex.unlock(driver.io);
        }
    }

    pub fn takeAsyncResult(self: *ExchangeCore, request_id: u64) ?kernel_mod.CommandResponse {
        self.assertWiring();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        return self.takeReadyResultLocked(request_id);
    }

    pub fn takeAsyncResultChecked(
        self: *ExchangeCore,
        request_id: u64,
    ) LifecycleError!?kernel_mod.CommandResponse {
        self.assertWiring();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        if (self.takeReadyResultLocked(request_id)) |response| return response;
        return switch (self.runtime.lifecycleState()) {
            .poisoned => error.CorePoisoned,
            .closing, .stopped => error.CoreStopped,
            .cold, .running => null,
        };
    }

    pub fn isAsyncResultReady(self: *const ExchangeCore, request_id: u64) bool {
        self.assertWiringConst();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        return self.api.ready_results.contains(request_id);
    }

    pub fn isAsyncResultReadyChecked(
        self: *const ExchangeCore,
        request_id: u64,
    ) LifecycleError!bool {
        self.assertWiringConst();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        if (self.api.ready_results.contains(request_id)) return true;
        return switch (self.runtime.lifecycleState()) {
            .poisoned => error.CorePoisoned,
            .closing, .stopped => error.CoreStopped,
            .cold, .running => false,
        };
    }

    pub fn asyncPendingCount(self: *const ExchangeCore) usize {
        self.assertWiringConst();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        return std.math.cast(usize, driver.async_accepted_total - driver.async_completed_total) orelse
            std.math.maxInt(usize);
    }

    pub fn asyncReadyCount(self: *const ExchangeCore) usize {
        self.assertWiringConst();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        return self.api.ready_results.count();
    }

    pub fn pendingCallbackCount(self: *const ExchangeCore) usize {
        self.assertWiringConst();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        return driver.callback_completions_pending;
    }

    pub fn enqueueRuntime(
        self: *ExchangeCore,
        command: cmd.OrderCommand,
    ) EnqueueRuntimeError!void {
        self.assertWiring();
        try self.enqueueDetachedBatch(&.{command}, .runtime);
    }

    pub fn drainRuntime(self: *ExchangeCore) RuntimeDrainError!runtime_mod.DrainOutcome {
        self.assertWiring();
        if (runtime_mod.isCallbackExecutionActive() or self.isWorkerThread()) {
            return error.CoreReentrantCall;
        }
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        const initial_state = self.runtime.lifecycleState();
        switch (initial_state) {
            .cold, .running => if (driver.close_requested.load(.acquire)) {
                driver.mutex.unlock(driver.io);
                return error.CoreStopped;
            },
            .poisoned => {},
            .closing, .stopped => {
                driver.mutex.unlock(driver.io);
                return error.CoreStopped;
            },
        }
        if (initial_state == .cold and driver.bootstrap_owner != null) {
            driver.mutex.unlock(driver.io);
            return error.CoreNotStarted;
        }

        // Runtime counters determine the successful outcome. The global
        // accepted prefix remains the terminal-settlement barrier so poison
        // cannot return while a callback or other accepted owner captured at
        // entry is still in flight.
        const settlement_target = driver.accepted_total;
        const runtime_target = driver.runtime_accepted_total;
        const cold_backend = !self.started;
        if ((initial_state == .cold or
            (initial_state == .poisoned and cold_backend)) and
            driver.bootstrap_owner != null)
        {
            driver.mutex.unlock(driver.io);
            return error.CoreNotStarted;
        }
        var drain_on_current_thread = false;
        var manual_drain_poisoned = false;
        if (driver.settled_prefix_total < settlement_target and initial_state == .cold) {
            if (!driver.manual_drain_active) {
                driver.manual_drain_active = true;
                drain_on_current_thread = true;
            }
        } else if (driver.settled_prefix_total < settlement_target and
            initial_state == .poisoned and
            cold_backend and
            !driver.manual_drain_active and
            driver.active_cold_calls == 0 and
            driver.bootstrap_owner == null and
            driver.worker == null and
            !driver.join_claimed)
        {
            driver.manual_drain_active = true;
            drain_on_current_thread = true;
            manual_drain_poisoned = true;
        }
        driver.mutex.unlock(driver.io);

        while (true) {
            if (drain_on_current_thread) {
                if (manual_drain_poisoned) {
                    self.drainAcceptedOnCurrentThread();
                    self.finishManualDrain();
                    drain_on_current_thread = false;
                    manual_drain_poisoned = false;
                } else {
                    const outcome = self.drainRuntimeBeforeStart() catch |err| {
                        self.finishManualDrain();
                        drain_on_current_thread = false;
                        if (err == error.CorePoisoned) continue;
                        return err;
                    };

                    // Preserve the exact grouping-control outcome from the
                    // cold helper. Claim its completed runtime frontier before
                    // releasing manual ownership so a concurrent waiter cannot
                    // steal the count and force this owner onto the lossy
                    // scalar fallback.
                    driver.mutex.lockUncancelable(driver.io);
                    if (self.runtime.lifecycleState() != .poisoned) {
                        std.debug.assert(
                            driver.runtime_completed_total >= runtime_target,
                        );
                        std.debug.assert(
                            driver.runtime_drained_total <=
                                driver.runtime_completed_total,
                        );
                        driver.runtime_drained_total =
                            driver.runtime_completed_total;
                        driver.manual_drain_active = false;
                        driver.completion_available.broadcast(driver.io);
                        driver.mutex.unlock(driver.io);
                        return outcome;
                    }
                    driver.manual_drain_active = false;
                    driver.completion_available.broadcast(driver.io);
                    driver.mutex.unlock(driver.io);
                    drain_on_current_thread = false;
                }
            }

            driver.mutex.lockUncancelable(driver.io);
            const state = self.runtime.lifecycleState();
            if (state == .poisoned) {
                if (driver.settled_prefix_total >= settlement_target) {
                    driver.mutex.unlock(driver.io);
                    return error.CorePoisoned;
                }
            } else if (!driver.manual_drain_active and
                driver.runtime_completed_total >= runtime_target)
            {
                std.debug.assert(driver.runtime_completed_total >= runtime_target);

                // Include later contiguous runtime completions observed at the
                // same locked frontier as `last_runtime_result`, but claim
                // only the still-undrained suffix. Concurrent drains therefore
                // cannot double-report or move the drained frontier backward.
                const claim_to = driver.runtime_completed_total;
                const accounted = driver.runtime_drained_total;
                std.debug.assert(accounted <= claim_to);
                const processed_u64 = claim_to - accounted;
                driver.runtime_drained_total = claim_to;
                const processed = std.math.cast(usize, processed_u64) orelse
                    std.math.maxInt(usize);
                const fallback: runtime_mod.DrainOutcome = .{
                    .groups_processed = if (processed == 0) 0 else std.math.divCeil(
                        usize,
                        processed,
                        self.runtime.max_batch_size,
                    ) catch unreachable,
                    .commands_processed = processed,
                    .last_merged_result = if (processed == 0)
                        .accepted
                    else
                        driver.last_runtime_result,
                };
                driver.mutex.unlock(driver.io);
                return fallback;
            }

            if (state == .stopped) {
                driver.mutex.unlock(driver.io);
                return error.CoreStopped;
            }

            if (!self.started and
                (state == .cold or state == .poisoned) and
                !driver.manual_drain_active and
                driver.active_cold_calls == 0 and
                driver.bootstrap_owner == null and
                driver.worker == null and
                !driver.join_claimed and
                driver.settled_prefix_total < settlement_target)
            {
                driver.manual_drain_active = true;
                drain_on_current_thread = true;
                manual_drain_poisoned = state == .poisoned;
                driver.mutex.unlock(driver.io);
                continue;
            }

            driver.accepted_drain_waiters += 1;
            driver.completion_available.broadcast(driver.io);
            driver.completion_available.waitUncancelable(driver.io, &driver.mutex);
            std.debug.assert(driver.accepted_drain_waiters > 0);
            driver.accepted_drain_waiters -= 1;
            driver.mutex.unlock(driver.io);
        }
    }

    fn drainRuntimeBeforeStart(self: *ExchangeCore) RuntimeDrainError!runtime_mod.DrainOutcome {
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        const item_count = driver.queue.count();
        var items = self.allocator.alloc(CoreWorkItem, item_count) catch |err| {
            driver.mutex.unlock(driver.io);
            return err;
        };
        for (items) |*item| item.* = driver.queue.pop().?;
        driver.space_available.broadcast(driver.io);
        driver.mutex.unlock(driver.io);
        defer self.allocator.free(items);

        var outcome: runtime_mod.DrainOutcome = .{};
        var index: usize = 0;
        while (index < items.len) {
            if (items[index].kind != .runtime) {
                _ = self.processQueuedItem(
                    items[index],
                    if (index + 1 == items.len) .terminal else .continues,
                ) catch |err| {
                    self.poisonFromWorkWithExtractedTail(
                        items[index],
                        err,
                        items[index + 1 ..],
                    );
                    return error.CorePoisoned;
                };
                index += 1;
                continue;
            }

            const batch_start = index;
            const limit = if (self.runtime.grouping_enabled) self.runtime.max_batch_size else 1;
            var batch_len: usize = 0;
            while (index + batch_len < items.len and batch_len < limit) {
                const candidate = items[index + batch_len];
                if (candidate.kind != .runtime) break;
                const boundary = isCoreGroupBoundaryCommand(&candidate.command);
                if (batch_len > 0 and boundary) break;
                batch_len += 1;
                if (candidate.command.command == .grouping_control or !self.runtime.grouping_enabled) break;
            }

            var has_success = false;
            var first_failure: ?cmd.CommandResultCode = null;
            var batch_offset: usize = 0;
            while (batch_offset < batch_len) : (batch_offset += 1) {
                const item_index = batch_start + batch_offset;
                const item = items[item_index];
                const result_code = self.processQueuedItem(
                    item,
                    if (item_index + 1 == items.len) .terminal else .continues,
                ) catch |err| {
                    self.poisonFromWorkWithExtractedTail(
                        item,
                        err,
                        items[item_index + 1 ..],
                    );
                    return error.CorePoisoned;
                };
                switch (result_code) {
                    .accepted => {},
                    .success => has_success = true,
                    else => if (first_failure == null) {
                        first_failure = result_code;
                    },
                }
            }
            outcome.groups_processed += 1;
            outcome.commands_processed += batch_len;
            outcome.last_merged_result = if (first_failure) |failed|
                failed
            else if (has_success)
                .success
            else
                .accepted;
            index += batch_len;
        }
        return outcome;
    }

    pub fn runtimePendingCount(self: *const ExchangeCore) usize {
        self.assertWiringConst();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        return std.math.cast(usize, driver.runtime_accepted_total - driver.runtime_completed_total) orelse
            std.math.maxInt(usize);
    }

    pub fn setResultSink(self: *ExchangeCore, sink: runtime_mod.ResultSink) void {
        self.setResultSinkChecked(sink) catch |err| @panic(@errorName(err));
    }

    pub fn setResultSinkChecked(
        self: *ExchangeCore,
        sink: runtime_mod.ResultSink,
    ) CallbackConfigError!void {
        self.assertWiring();
        const owns_quiescence_barrier = try self.lockCallbackConfigBarrier();
        defer if (owns_quiescence_barrier) {
            self.callback_execution_mutex.unlock(self.driver.io);
        };
        self.callback_mutex.lockUncancelable(self.driver.io);
        defer self.callback_mutex.unlock(self.driver.io);
        self.runtime.sink = sink;
    }

    pub fn setRawResultSink(self: *ExchangeCore, sink: RawResultSink) void {
        self.setRawResultSinkChecked(sink) catch |err| @panic(@errorName(err));
    }

    pub fn setRawResultSinkChecked(
        self: *ExchangeCore,
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

    pub fn clearRawResultSink(self: *ExchangeCore) void {
        self.clearRawResultSinkChecked() catch |err| @panic(@errorName(err));
    }

    pub fn clearRawResultSinkChecked(
        self: *ExchangeCore,
    ) CallbackConfigError!void {
        try self.setRawResultSinkChecked(.{});
    }

    pub fn setSimpleEventsHandler(self: *ExchangeCore, handler: SimpleEventsHandler) void {
        self.setSimpleEventsHandlerChecked(handler) catch |err| @panic(@errorName(err));
    }

    pub fn setSimpleEventsHandlerChecked(
        self: *ExchangeCore,
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

    pub fn clearSimpleEventsHandler(self: *ExchangeCore) void {
        self.clearSimpleEventsHandlerChecked() catch |err| @panic(@errorName(err));
    }

    pub fn clearSimpleEventsHandlerChecked(
        self: *ExchangeCore,
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

    pub fn addUser(self: *ExchangeCore, uid: i64) ExecuteError!kernel_mod.CommandResponse {
        self.assertWiring();
        var command = makeAddUserCommand(uid);
        return self.submitCommand(&command);
    }

    pub fn adjustUserBalance(
        self: *ExchangeCore,
        uid: i64,
        transaction_id: i64,
        currency: i32,
        amount: i64,
        adjustment_type: common.BalanceAdjustmentType,
    ) ExecuteError!kernel_mod.CommandResponse {
        self.assertWiring();
        var command = makeBalanceAdjustmentCommand(
            uid,
            currency,
            amount,
            transaction_id,
            adjustment_type,
        );
        return self.submitCommand(&command);
    }

    pub fn placeOrder(
        self: *ExchangeCore,
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
        self.assertWiring();
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
        return self.submitCommand(&command);
    }

    pub fn moveOrder(
        self: *ExchangeCore,
        order_id: i64,
        uid: i64,
        symbol: i32,
        new_price: i64,
    ) ExecuteError!kernel_mod.CommandResponse {
        self.assertWiring();
        var command = makeMoveOrderCommand(order_id, uid, symbol, new_price);
        return self.submitCommand(&command);
    }

    pub fn cancelOrder(
        self: *ExchangeCore,
        order_id: i64,
        uid: i64,
        symbol: i32,
    ) ExecuteError!kernel_mod.CommandResponse {
        self.assertWiring();
        var command = makeCancelOrderCommand(order_id, uid, symbol);
        return self.submitCommand(&command);
    }

    pub fn reduceOrder(
        self: *ExchangeCore,
        order_id: i64,
        uid: i64,
        symbol: i32,
        reduce_size: i64,
    ) ExecuteError!kernel_mod.CommandResponse {
        self.assertWiring();
        var command = makeReduceOrderCommand(order_id, uid, symbol, reduce_size);
        return self.submitCommand(&command);
    }

    pub fn suspendUser(self: *ExchangeCore, uid: i64) ExecuteError!kernel_mod.CommandResponse {
        self.assertWiring();
        var command = makeSuspendUserCommand(uid);
        return self.submitCommand(&command);
    }

    pub fn resumeUser(self: *ExchangeCore, uid: i64) ExecuteError!kernel_mod.CommandResponse {
        self.assertWiring();
        var command = makeResumeUserCommand(uid);
        return self.submitCommand(&command);
    }

    pub fn persistState(
        self: *ExchangeCore,
        snapshot_id: i64,
        timestamp_ns: i64,
    ) ExecuteError!cmd.CommandResultCode {
        self.assertWiring();
        return (try self.persistStateBatch(snapshot_id, timestamp_ns)).result_code;
    }

    fn persistStateBatch(
        self: *ExchangeCore,
        snapshot_id: i64,
        timestamp_ns: i64,
    ) ExecuteError!kernel_mod.BatchResponse {
        var matching = makePersistStateMatchingCommand(snapshot_id, timestamp_ns);
        var risk_cmd = makePersistStateRiskCommand(snapshot_id, timestamp_ns);
        var commands = [_]*cmd.OrderCommand{
            &matching,
            &risk_cmd,
        };
        return self.submitCommandBatch(commands[0..]);
    }

    pub fn persistStateWithDefaultMainLog(
        self: *ExchangeCore,
        dir: std.Io.Dir,
        snapshot_id: i64,
        timestamp_ns: i64,
        module_type: PersistenceModuleType,
        instance_id: u32,
    ) PersistStateWithMainLogError!cmd.CommandResultCode {
        self.assertWiring();
        try self.ensureBlockingCallAllowed();

        // Preserve the directory-taking API without allowing its main-log
        // entry to advertise a pair written under another root. This check is
        // deliberately before publication, so a mismatch consumes no global
        // sequence and creates no snapshot descriptor or payload.
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

    /// Persists the durable ME/RE pair and appends its `.eca` main-log entry
    /// through the directory handle owned by the core. This is the preferred
    /// API for disk-backed cores because one root governs every artifact.
    pub fn persistStateWithConfiguredMainLog(
        self: *ExchangeCore,
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

        const batch = try self.persistStateBatch(snapshot_id, timestamp_ns);
        if (batch.result_code != .success) return error.PersistStateRejected;
        const persistence_root = self.persistence_root orelse
            return error.ConfiguredMainLogRequiresDiskBackend;

        const snapshot_id_u64 = std.math.cast(u64, snapshot_id) orelse return error.PersistStateSnapshotIdOverflow;
        const timestamp_ns_u64 = std.math.cast(u64, timestamp_ns) orelse return error.PersistStateTimestampOverflow;
        const sequence_from = batch.sequence_from orelse return error.MissingPersistSequence;
        const sequence_to = batch.sequence_to orelse return error.MissingPersistSequence;

        const cold_claimed = try self.waitForAcceptedPrefix();
        defer if (cold_claimed) self.finishColdDirectCall();
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);

        const persisted_snapshot = self.persistenceSnapshotByIdAssumeExecutionLocked(snapshot_id_u64) orelse
            return error.PersistStateSnapshotMismatch;
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
        self: *const ExchangeCore,
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

    pub fn groupingControl(
        self: *ExchangeCore,
        timestamp_ns: i64,
        mode: i64,
    ) ExecuteError!kernel_mod.CommandResponse {
        self.assertWiring();
        var command = makeGroupingControlModeCommand(timestamp_ns, mode);
        return self.submitCommand(&command);
    }

    pub fn requestOrderBook(
        self: *ExchangeCore,
        symbol: i32,
        depth: usize,
    ) (ExecuteError || HelperError)!kernel_mod.CommandResponse {
        self.assertWiring();
        const depth_i64 = std.math.cast(i64, depth) orelse return error.OrderBookDepthOverflow;
        var command = makeRequestOrderBookCommandI64(symbol, depth_i64);
        return self.submitCommand(&command);
    }

    pub fn submitBinaryPayload(
        self: *ExchangeCore,
        command_type: cmd.OrderCommandType,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
    ) BinaryPayloadSubmitError!kernel_mod.CommandResponse {
        self.assertWiring();
        switch (command_type) {
            .binary_data_command, .binary_data_query => {},
            else => return error.InvalidBinaryCommandType,
        }
        const carrier_words = try self.rawPayloadCarrierWords(payload);
        defer self.allocator.free(carrier_words);
        return self.submitBinaryCarrierWords(
            command_type,
            transfer_id,
            timestamp,
            carrier_words,
        );
    }

    pub fn submitBinaryPayloadAsync(
        self: *ExchangeCore,
        command_type: cmd.OrderCommandType,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
    ) BinaryPayloadAsyncSubmitError!u64 {
        self.assertWiring();
        switch (command_type) {
            .binary_data_command, .binary_data_query => {},
            else => return error.InvalidBinaryCommandType,
        }
        const carrier_words = try self.rawPayloadCarrierWords(payload);
        defer self.allocator.free(carrier_words);
        return self.submitBinaryCarrierWordsAsync(
            command_type,
            transfer_id,
            timestamp,
            carrier_words,
        );
    }

    /// Publishes a raw binary command or query with one terminal callback.
    /// The callback never observes intermediate carrier-frame responses.
    pub fn submitBinaryPayloadAsyncWithCallback(
        self: *ExchangeCore,
        command_type: cmd.OrderCommandType,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
        callback: CommandResponseCallback,
    ) BinaryPayloadAsyncSubmitError!void {
        self.assertWiring();
        switch (command_type) {
            .binary_data_command, .binary_data_query => {},
            else => return error.InvalidBinaryCommandType,
        }
        const carrier_words = try self.rawPayloadCarrierWords(payload);
        defer self.allocator.free(carrier_words);
        try self.submitBinaryCarrierWordsWithCallback(
            command_type,
            transfer_id,
            timestamp,
            carrier_words,
            callback,
        );
    }

    /// Publishes a raw binary command or query without retaining any response.
    pub fn submitBinaryPayloadDetached(
        self: *ExchangeCore,
        command_type: cmd.OrderCommandType,
        transfer_id: i32,
        timestamp: i64,
        payload: []const u8,
    ) BinaryPayloadAsyncSubmitError!void {
        self.assertWiring();
        switch (command_type) {
            .binary_data_command, .binary_data_query => {},
            else => return error.InvalidBinaryCommandType,
        }
        const carrier_words = try self.rawPayloadCarrierWords(payload);
        defer self.allocator.free(carrier_words);
        const frame_commands = try self.binaryFrameCommandsFromCarrierWords(
            command_type,
            transfer_id,
            timestamp,
            carrier_words,
        );
        defer self.allocator.free(frame_commands);
        _ = try self.enqueueBinaryFrames(frame_commands, .detached);
    }

    pub fn submitAddSymbolsBinary(
        self: *ExchangeCore,
        transfer_id: i32,
        timestamp: i64,
        symbols: []const common.CoreSymbolSpecification,
    ) BinaryPayloadTypedSubmitError!kernel_mod.CommandResponse {
        self.assertWiring();
        const body = try binary_payload.encodeJavaAddSymbolsToBytes(self.allocator, symbols);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        return self.submitBinaryCarrierWords(.binary_data_command, transfer_id, timestamp, carrier_words);
    }

    pub fn submitAddSymbolsBinaryAsync(
        self: *ExchangeCore,
        transfer_id: i32,
        timestamp: i64,
        symbols: []const common.CoreSymbolSpecification,
    ) BinaryPayloadTypedAsyncSubmitError!u64 {
        self.assertWiring();
        const body = try binary_payload.encodeJavaAddSymbolsToBytes(self.allocator, symbols);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        return self.submitBinaryCarrierWordsAsync(.binary_data_command, transfer_id, timestamp, carrier_words);
    }

    pub fn submitAddAccountsBinary(
        self: *ExchangeCore,
        transfer_id: i32,
        timestamp: i64,
        users: []const BinaryUserAccountsBatch,
    ) BinaryPayloadTypedSubmitError!kernel_mod.CommandResponse {
        self.assertWiring();
        const body = try binary_payload.encodeJavaAddAccountsToBytes(self.allocator, users);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        return self.submitBinaryCarrierWords(.binary_data_command, transfer_id, timestamp, carrier_words);
    }

    pub fn submitAddAccountsBinaryAsync(
        self: *ExchangeCore,
        transfer_id: i32,
        timestamp: i64,
        users: []const BinaryUserAccountsBatch,
    ) BinaryPayloadTypedAsyncSubmitError!u64 {
        self.assertWiring();
        const body = try binary_payload.encodeJavaAddAccountsToBytes(self.allocator, users);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        return self.submitBinaryCarrierWordsAsync(.binary_data_command, transfer_id, timestamp, carrier_words);
    }

    pub fn submitBinaryReportQuery(
        self: *ExchangeCore,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    ) BinaryReportQuerySubmitError!kernel_mod.CommandResponse {
        self.assertWiring();
        const body = try binary_payload.encodeJavaReportQueryToBytes(self.allocator, query);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        return self.submitBinaryCarrierWords(.binary_data_query, transfer_id, timestamp, carrier_words);
    }

    pub fn submitBinaryReportQueryAsync(
        self: *ExchangeCore,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    ) BinaryReportQueryAsyncSubmitError!u64 {
        self.assertWiring();
        const body = try binary_payload.encodeJavaReportQueryToBytes(self.allocator, query);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        return self.submitBinaryCarrierWordsAsync(.binary_data_query, transfer_id, timestamp, carrier_words);
    }

    /// Queues an exact Java report query and also requests the richer Zig-native
    /// projection on its terminal frame. The projection marker is transient;
    /// the encoded Java payload remains byte-for-byte unchanged.
    pub fn submitNativeReportQuery(
        self: *ExchangeCore,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    ) BinaryReportQuerySubmitError!kernel_mod.CommandResponse {
        self.assertWiring();
        const body = try binary_payload.encodeJavaReportQueryToBytes(self.allocator, query);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        return self.submitBinaryCarrierWordsWithProjection(
            .binary_data_query,
            transfer_id,
            timestamp,
            carrier_words,
            .exact_java_and_native,
        );
    }

    pub fn submitNativeReportQueryAsync(
        self: *ExchangeCore,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    ) BinaryReportQueryAsyncSubmitError!u64 {
        self.assertWiring();
        const body = try binary_payload.encodeJavaReportQueryToBytes(self.allocator, query);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        return self.submitBinaryCarrierWordsAsyncWithProjection(
            .binary_data_query,
            transfer_id,
            timestamp,
            carrier_words,
            .exact_java_and_native,
        );
    }

    fn rawPayloadCarrierWords(
        self: *ExchangeCore,
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
        self: *ExchangeCore,
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
        self: *ExchangeCore,
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
        self: *ExchangeCore,
        command_type: cmd.OrderCommandType,
        transfer_id: i32,
        timestamp: i64,
        carrier_words: []const i64,
    ) BinaryPayloadSubmitError!kernel_mod.CommandResponse {
        return self.submitBinaryCarrierWordsWithProjection(
            command_type,
            transfer_id,
            timestamp,
            carrier_words,
            .exact_java_only,
        );
    }

    fn submitBinaryCarrierWordsWithProjection(
        self: *ExchangeCore,
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
                .synchronous,
            )).?;
            return self.waitTakeResponse(request_id);
        }
        defer self.finishColdDirectCall();
        const sequences = try self.prepareColdPublicationRange(frame_commands.len);

        var last_response: ?kernel_mod.CommandResponse = null;
        errdefer if (last_response) |*response| response.deinit(self.allocator);
        for (frame_commands, 0..) |*frame_command, index| {
            const response = try self.executePreparedDirect(
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
        self: *ExchangeCore,
        command_type: cmd.OrderCommandType,
        transfer_id: i32,
        timestamp: i64,
        carrier_words: []const i64,
    ) BinaryPayloadAsyncSubmitError!u64 {
        return self.submitBinaryCarrierWordsAsyncWithProjection(
            command_type,
            transfer_id,
            timestamp,
            carrier_words,
            .exact_java_only,
        );
    }

    fn submitBinaryCarrierWordsAsyncWithProjection(
        self: *ExchangeCore,
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
        return (try self.enqueueBinaryFrames(frame_commands, .asynchronous)).?;
    }

    fn submitBinaryCarrierWordsDetached(
        self: *ExchangeCore,
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
        _ = try self.enqueueBinaryFrames(frame_commands, .detached);
    }

    fn submitBinaryCarrierWordsWithCallback(
        self: *ExchangeCore,
        command_type: cmd.OrderCommandType,
        transfer_id: i32,
        timestamp: i64,
        carrier_words: []const i64,
        callback: CommandResponseCallback,
    ) BinaryPayloadAsyncSubmitError!void {
        const frame_commands = try self.binaryFrameCommandsFromCarrierWords(
            command_type,
            transfer_id,
            timestamp,
            carrier_words,
        );
        defer self.allocator.free(frame_commands);
        _ = try self.enqueueBinaryFrames(frame_commands, .{ .callback = callback });
    }

    fn submitAddSymbolsBinaryDetached(
        self: *ExchangeCore,
        transfer_id: i32,
        timestamp: i64,
        symbols: []const common.CoreSymbolSpecification,
    ) SubmitApiCommandError!void {
        const body = try binary_payload.encodeJavaAddSymbolsToBytes(self.allocator, symbols);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        try self.submitBinaryCarrierWordsDetached(
            .binary_data_command,
            transfer_id,
            timestamp,
            carrier_words,
        );
    }

    fn submitAddAccountsBinaryDetached(
        self: *ExchangeCore,
        transfer_id: i32,
        timestamp: i64,
        users: []const BinaryUserAccountsBatch,
    ) SubmitApiCommandError!void {
        const body = try binary_payload.encodeJavaAddAccountsToBytes(self.allocator, users);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        try self.submitBinaryCarrierWordsDetached(
            .binary_data_command,
            transfer_id,
            timestamp,
            carrier_words,
        );
    }

    fn submitBinaryReportQueryDetached(
        self: *ExchangeCore,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    ) SubmitApiCommandError!void {
        const body = try binary_payload.encodeJavaReportQueryToBytes(self.allocator, query);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        try self.submitBinaryCarrierWordsDetached(
            .binary_data_query,
            transfer_id,
            timestamp,
            carrier_words,
        );
    }

    fn submitAddSymbolsBinaryWithCallback(
        self: *ExchangeCore,
        transfer_id: i32,
        timestamp: i64,
        symbols: []const common.CoreSymbolSpecification,
        callback: CommandResponseCallback,
    ) SubmitApiCommandWithCallbackError!void {
        const body = try binary_payload.encodeJavaAddSymbolsToBytes(self.allocator, symbols);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        try self.submitBinaryCarrierWordsWithCallback(
            .binary_data_command,
            transfer_id,
            timestamp,
            carrier_words,
            callback,
        );
    }

    fn submitAddAccountsBinaryWithCallback(
        self: *ExchangeCore,
        transfer_id: i32,
        timestamp: i64,
        users: []const BinaryUserAccountsBatch,
        callback: CommandResponseCallback,
    ) SubmitApiCommandWithCallbackError!void {
        const body = try binary_payload.encodeJavaAddAccountsToBytes(self.allocator, users);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        try self.submitBinaryCarrierWordsWithCallback(
            .binary_data_command,
            transfer_id,
            timestamp,
            carrier_words,
            callback,
        );
    }

    fn submitBinaryReportQueryWithCallback(
        self: *ExchangeCore,
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
        callback: CommandResponseCallback,
    ) SubmitApiCommandWithCallbackError!void {
        const body = try binary_payload.encodeJavaReportQueryToBytes(self.allocator, query);
        defer self.allocator.free(body);
        const carrier_words = try java_binary_wire.encodeRequestWords(self.allocator, body, .{});
        defer self.allocator.free(carrier_words);
        try self.submitBinaryCarrierWordsWithCallback(
            .binary_data_query,
            transfer_id,
            timestamp,
            carrier_words,
            callback,
        );
    }

    pub fn reset(self: *ExchangeCore) ExecuteError!kernel_mod.CommandResponse {
        self.assertWiring();
        var command = makeResetCommand();
        return self.submitCommand(&command);
    }

    pub fn nop(self: *ExchangeCore) ExecuteError!kernel_mod.CommandResponse {
        self.assertWiring();
        var command = makeNopCommand();
        return self.submitCommand(&command);
    }

    pub fn addUserAsync(self: *ExchangeCore, uid: i64) AsyncSubmitError!u64 {
        self.assertWiring();
        const command = makeAddUserCommand(uid);
        return self.submitCommandAsync(&command);
    }

    pub fn adjustUserBalanceAsync(
        self: *ExchangeCore,
        uid: i64,
        transaction_id: i64,
        currency: i32,
        amount: i64,
        adjustment_type: common.BalanceAdjustmentType,
    ) AsyncSubmitError!u64 {
        self.assertWiring();
        const command = makeBalanceAdjustmentCommand(
            uid,
            currency,
            amount,
            transaction_id,
            adjustment_type,
        );
        return self.submitCommandAsync(&command);
    }

    pub fn suspendUserAsync(self: *ExchangeCore, uid: i64) AsyncSubmitError!u64 {
        self.assertWiring();
        const command = makeSuspendUserCommand(uid);
        return self.submitCommandAsync(&command);
    }

    pub fn resumeUserAsync(self: *ExchangeCore, uid: i64) AsyncSubmitError!u64 {
        self.assertWiring();
        const command = makeResumeUserCommand(uid);
        return self.submitCommandAsync(&command);
    }

    pub fn placeOrderAsync(
        self: *ExchangeCore,
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
        self.assertWiring();
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
        return self.submitCommandAsync(&command);
    }

    pub fn moveOrderAsync(
        self: *ExchangeCore,
        order_id: i64,
        uid: i64,
        symbol: i32,
        new_price: i64,
    ) AsyncSubmitError!u64 {
        self.assertWiring();
        const command = makeMoveOrderCommand(order_id, uid, symbol, new_price);
        return self.submitCommandAsync(&command);
    }

    pub fn cancelOrderAsync(
        self: *ExchangeCore,
        order_id: i64,
        uid: i64,
        symbol: i32,
    ) AsyncSubmitError!u64 {
        self.assertWiring();
        const command = makeCancelOrderCommand(order_id, uid, symbol);
        return self.submitCommandAsync(&command);
    }

    pub fn reduceOrderAsync(
        self: *ExchangeCore,
        order_id: i64,
        uid: i64,
        symbol: i32,
        reduce_size: i64,
    ) AsyncSubmitError!u64 {
        self.assertWiring();
        const command = makeReduceOrderCommand(order_id, uid, symbol, reduce_size);
        return self.submitCommandAsync(&command);
    }

    pub fn resetAsync(self: *ExchangeCore) AsyncSubmitError!u64 {
        self.assertWiring();
        const command = makeResetCommand();
        return self.submitCommandAsync(&command);
    }

    pub fn groupingControlAsync(
        self: *ExchangeCore,
        timestamp_ns: i64,
        mode: i64,
    ) AsyncSubmitError!u64 {
        self.assertWiring();
        const command = makeGroupingControlModeCommand(timestamp_ns, mode);
        return self.submitCommandAsync(&command);
    }

    pub fn nopAsync(self: *ExchangeCore) AsyncSubmitError!u64 {
        self.assertWiring();
        const command = makeNopCommand();
        return self.submitCommandAsync(&command);
    }

    pub fn persistStateAsync(
        self: *ExchangeCore,
        snapshot_id: i64,
        timestamp_ns: i64,
    ) AsyncSubmitError!PersistStateRequestPair {
        self.assertWiring();
        const commands = [_]cmd.OrderCommand{
            makePersistStateMatchingCommand(snapshot_id, timestamp_ns),
            makePersistStateRiskCommand(snapshot_id, timestamp_ns),
        };
        var request_ids: [2]u64 = undefined;
        try self.enqueueResponseBatch(commands[0..], .asynchronous, request_ids[0..]);
        return .{
            .matching_request_id = request_ids[0],
            .risk_request_id = request_ids[1],
        };
    }

    pub fn isPersistStateResultReady(
        self: *const ExchangeCore,
        requests: PersistStateRequestPair,
    ) bool {
        self.assertWiringConst();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        return self.api.ready_results.contains(requests.matching_request_id) and
            self.api.ready_results.contains(requests.risk_request_id);
    }

    pub fn isPersistStateResultReadyChecked(
        self: *const ExchangeCore,
        requests: PersistStateRequestPair,
    ) LifecycleError!bool {
        self.assertWiringConst();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        if (self.api.ready_results.contains(requests.matching_request_id) and
            self.api.ready_results.contains(requests.risk_request_id)) return true;
        return switch (self.runtime.lifecycleState()) {
            .poisoned => error.CorePoisoned,
            .closing, .stopped => error.CoreStopped,
            .cold, .running => false,
        };
    }

    pub fn takePersistStateResult(
        self: *ExchangeCore,
        requests: PersistStateRequestPair,
    ) ?cmd.CommandResultCode {
        self.assertWiring();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        if (!self.api.ready_results.contains(requests.matching_request_id) or
            !self.api.ready_results.contains(requests.risk_request_id)) return null;
        var matching = self.takeReadyResultLocked(requests.matching_request_id).?;
        var risk_cmd = self.takeReadyResultLocked(requests.risk_request_id).?;
        defer matching.deinit(self.allocator);
        defer risk_cmd.deinit(self.allocator);
        return cmd.CommandResultCode.mergeToFirstFailed(&.{
            matching.result_code,
            risk_cmd.result_code,
        });
    }

    pub fn takePersistStateResultChecked(
        self: *ExchangeCore,
        requests: PersistStateRequestPair,
    ) LifecycleError!?cmd.CommandResultCode {
        self.assertWiring();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        if (self.api.ready_results.contains(requests.matching_request_id) and
            self.api.ready_results.contains(requests.risk_request_id))
        {
            var matching = self.takeReadyResultLocked(requests.matching_request_id).?;
            var risk_cmd = self.takeReadyResultLocked(requests.risk_request_id).?;
            defer matching.deinit(self.allocator);
            defer risk_cmd.deinit(self.allocator);
            return cmd.CommandResultCode.mergeToFirstFailed(&.{
                matching.result_code,
                risk_cmd.result_code,
            });
        }
        return switch (self.runtime.lifecycleState()) {
            .poisoned => error.CorePoisoned,
            .closing, .stopped => error.CoreStopped,
            .cold, .running => null,
        };
    }

    pub fn requestOrderBookAsync(
        self: *ExchangeCore,
        symbol: i32,
        depth: usize,
    ) (AsyncSubmitError || HelperError)!u64 {
        self.assertWiring();
        const depth_i64 = std.math.cast(i64, depth) orelse return error.OrderBookDepthOverflow;
        const command = makeRequestOrderBookCommandI64(symbol, depth_i64);
        return self.submitCommandAsync(&command);
    }

    pub fn submitApiCommand(
        self: *ExchangeCore,
        api_command: ApiCommand,
    ) SubmitApiCommandError!ApiCommandResult {
        self.assertWiring();
        try self.ensureBlockingCallAllowed();
        if (try api_command_mod.toOrdinaryOrderCommand(api_command)) |ordinary| {
            var command = ordinary;
            return .{ .response = try self.submitCommand(&command) };
        }
        return switch (api_command) {
            .raw_command => |command| blk: {
                var mutable = command;
                break :blk .{ .response = try self.submitRawApiCommandSync(&mutable) };
            },
            .persist_state => |payload| .{ .persist_state = try self.persistState(
                payload.snapshot_id,
                payload.timestamp_ns,
            ) },
            .grouping_control => |payload| .{ .response = try self.groupingControl(
                payload.timestamp_ns,
                payload.mode,
            ) },
            .binary_add_symbols => |payload| .{ .response = try self.submitAddSymbolsBinary(
                payload.transfer_id,
                payload.timestamp,
                payload.symbols,
            ) },
            .binary_add_accounts => |payload| .{ .response = try self.submitAddAccountsBinary(
                payload.transfer_id,
                payload.timestamp,
                payload.users,
            ) },
            .binary_report_query => |payload| .{ .response = try self.submitBinaryReportQuery(
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

    /// Publishes a Java-style high-level API command without allocating a
    /// poll-result slot and returns as soon as the whole publication is
    /// visible to the worker. Persistence pairs remain queue-atomic; running
    /// multi-frame binary carriers reserve one contiguous sequence range and
    /// become visible in bounded FIFO chunks.
    pub fn submitApiCommandDetached(
        self: *ExchangeCore,
        api_command: ApiCommand,
    ) SubmitApiCommandError!void {
        self.assertWiring();
        if (try api_command_mod.toOrdinaryOrderCommand(api_command)) |command| {
            return self.enqueueDetachedBatch(&.{command}, .detached_api);
        }
        return switch (api_command) {
            .raw_command => |command| self.enqueueDetachedBatch(&.{command}, .detached_api_raw),
            .persist_state => |payload| self.enqueueDetachedBatch(&.{
                makePersistStateMatchingCommand(payload.snapshot_id, payload.timestamp_ns),
                makePersistStateRiskCommand(payload.snapshot_id, payload.timestamp_ns),
            }, .detached_api),
            .grouping_control => |payload| self.enqueueDetachedBatch(
                &.{makeGroupingControlModeCommand(payload.timestamp_ns, payload.mode)},
                .detached_api,
            ),
            .binary_add_symbols => |payload| self.submitAddSymbolsBinaryDetached(
                payload.transfer_id,
                payload.timestamp,
                payload.symbols,
            ),
            .binary_add_accounts => |payload| self.submitAddAccountsBinaryDetached(
                payload.transfer_id,
                payload.timestamp,
                payload.users,
            ),
            .binary_report_query => |payload| self.submitBinaryReportQueryDetached(
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
        self: *ExchangeCore,
        api_command: ApiCommand,
    ) SubmitApiCommandBatchError!void {
        self.submitApiCommandDetached(api_command) catch |err| switch (err) {
            error.CoreQueueFull => {
                // A cold core has no consumer to free queue capacity between
                // detached prefix publications. Settle the accepted prefix,
                // then continue exactly where a running Java consumer would.
                _ = try self.drainAsyncPending();
                try self.submitApiCommandDetached(api_command);
            },
            else => return err,
        };
    }

    fn submitApiCommandAsyncForBatch(
        self: *ExchangeCore,
        api_command: ApiCommand,
    ) SubmitApiCommandBatchError!ApiCommandAsyncHandle {
        return self.submitApiCommandAsync(api_command) catch |err| switch (err) {
            error.CoreQueueFull => blk: {
                _ = try self.drainAsyncPending();
                break :blk try self.submitApiCommandAsync(api_command);
            },
            else => return err,
        };
    }

    pub fn submitApiCommandAsync(
        self: *ExchangeCore,
        api_command: ApiCommand,
    ) SubmitApiCommandAsyncError!ApiCommandAsyncHandle {
        self.assertWiring();
        if (try api_command_mod.toOrdinaryOrderCommand(api_command)) |command| {
            return .{ .request_id = try self.submitCommandAsync(&command) };
        }
        return switch (api_command) {
            .raw_command => |command| .{ .request_id = try self.submitRawApiCommandAsync(command) },
            .persist_state => |payload| .{ .persist_state = try self.persistStateAsync(
                payload.snapshot_id,
                payload.timestamp_ns,
            ) },
            .grouping_control => |payload| .{ .request_id = try self.groupingControlAsync(
                payload.timestamp_ns,
                payload.mode,
            ) },
            .binary_add_symbols => |payload| .{ .request_id = try self.submitAddSymbolsBinaryAsync(
                payload.transfer_id,
                payload.timestamp,
                payload.symbols,
            ) },
            .binary_add_accounts => |payload| .{ .request_id = try self.submitAddAccountsBinaryAsync(
                payload.transfer_id,
                payload.timestamp,
                payload.users,
            ) },
            .binary_report_query => |payload| .{ .request_id = try self.submitBinaryReportQueryAsync(
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

    /// Publishes a callback-owned command atomically. Callback ownership is
    /// installed in the queue slot before it becomes visible to the worker,
    /// matching Java ExchangeApi's translator/promise publication ordering.
    pub fn submitApiCommandWithCallback(
        self: *ExchangeCore,
        api_command: ApiCommand,
        callback: CommandResponseCallback,
    ) SubmitApiCommandWithCallbackError!void {
        self.assertWiring();
        switch (api_command) {
            .binary_add_symbols => |payload| return self.submitAddSymbolsBinaryWithCallback(
                payload.transfer_id,
                payload.timestamp,
                payload.symbols,
                callback,
            ),
            .binary_add_accounts => |payload| return self.submitAddAccountsBinaryWithCallback(
                payload.transfer_id,
                payload.timestamp,
                payload.users,
                callback,
            ),
            .binary_report_query => |payload| return self.submitBinaryReportQueryWithCallback(
                payload.transfer_id,
                payload.timestamp,
                payload.query,
                callback,
            ),
            else => {},
        }
        const translated = try api_command_mod.toOrdinaryOrderCommand(api_command);
        switch (api_command) {
            .raw_command,
            .persist_state,
            .grouping_control,
            .reset,
            .reset_with_timestamp,
            .nop,
            .nop_with_timestamp,
            => return error.UnsupportedCallbackCommand,
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
            .binary_add_symbols,
            .binary_add_accounts,
            .binary_report_query,
            => unreachable,
        }
        var command = translated orelse unreachable;

        // Java's Consumer<OrderCommand> convenience overloads stamp wall-clock
        // milliseconds at publication time when the caller omits one.
        // Preserve explicit typed timestamps and the convenience sentinel
        // metadata independently.
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
        try self.submitCommandAsyncWithCallback(&command, callback);
    }

    pub fn submitCommandAsyncWithCallback(
        self: *ExchangeCore,
        command: *const cmd.OrderCommand,
        callback: CommandResponseCallback,
    ) AsyncSubmitError!void {
        self.assertWiring();
        try self.enqueueCallback(command.*, callback);
    }

    /// Java-compatible convenience publication: the global publication
    /// sequence is also the generated order ID. The callback request ID stays
    /// a separate, process-local completion handle.
    pub fn placeNewOrderGeneratedIdWithCallback(
        self: *ExchangeCore,
        user_cookie: i32,
        price: i64,
        reserve_bid_price: i64,
        size: i64,
        action: common.OrderAction,
        order_type: common.OrderType,
        symbol: i32,
        uid: i64,
        callback: CommandResponseCallback,
    ) SubmitApiCommandWithCallbackError!u64 {
        self.assertWiring();
        const driver = self.driver;
        try self.lockDriverForPublication();
        defer driver.mutex.unlock(driver.io);

        try self.waitForColdPublisherLocked();
        try self.waitForCapacityLocked(1, 0);
        const request_id = std.math.add(u64, self.api.next_request_id, 1) catch
            return error.RequestIdOverflow;
        try self.ensureJournalCapacityLocked(1);
        const sequences = try driver.publication_sequencer.prepare(1);
        const sequence = sequences.first.toInt();
        const order_id = std.math.cast(i64, sequence) orelse
            return error.GeneratedOrderIdOverflow;
        const timestamp = std.math.cast(i64, io_context.realTimestampMs()) orelse
            return error.CallbackTimestampOverflow;

        var reservation = driver.queue.reserve(1) catch unreachable;
        reservation.at(0).* = .{
            .sequence = sequences.first,
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
        self.api.next_request_id = request_id;
        driver.callback_completions_pending += 1;
        driver.accepted_total += 1;
        driver.async_accepted_total += 1;
        driver.work_available.broadcast(driver.io);
        return sequence;
    }

    pub fn isApiCommandAsyncResultReady(
        self: *const ExchangeCore,
        handle: ApiCommandAsyncHandle,
    ) bool {
        self.assertWiringConst();
        return switch (handle) {
            .request_id => |request_id| self.isAsyncResultReady(request_id),
            .persist_state => |requests| self.isPersistStateResultReady(requests),
        };
    }

    pub fn isApiCommandAsyncResultReadyChecked(
        self: *const ExchangeCore,
        handle: ApiCommandAsyncHandle,
    ) LifecycleError!bool {
        self.assertWiringConst();
        return switch (handle) {
            .request_id => |request_id| self.isAsyncResultReadyChecked(request_id),
            .persist_state => |requests| self.isPersistStateResultReadyChecked(requests),
        };
    }

    pub fn takeApiCommandAsyncResult(
        self: *ExchangeCore,
        handle: ApiCommandAsyncHandle,
    ) ?ApiCommandResult {
        self.assertWiring();
        return switch (handle) {
            .request_id => |request_id| if (self.takeAsyncResult(request_id)) |response|
                .{ .response = response }
            else
                null,
            .persist_state => |requests| if (self.takePersistStateResult(requests)) |result_code|
                .{ .persist_state = result_code }
            else
                null,
        };
    }

    pub fn takeApiCommandAsyncResultChecked(
        self: *ExchangeCore,
        handle: ApiCommandAsyncHandle,
    ) LifecycleError!?ApiCommandResult {
        self.assertWiring();
        return switch (handle) {
            .request_id => |request_id| if (try self.takeAsyncResultChecked(request_id)) |response|
                .{ .response = response }
            else
                null,
            .persist_state => |requests| if (try self.takePersistStateResultChecked(requests)) |result_code|
                .{ .persist_state = result_code }
            else
                null,
        };
    }

    pub fn submitApiCommandsSync(
        self: *ExchangeCore,
        commands: []const ApiCommand,
    ) SubmitApiCommandBatchError!ApiCommandResult {
        self.assertWiring();
        if (commands.len == 0) return error.EmptyApiCommandBatch;
        try self.ensureBlockingCallAllowed();

        for (commands[0 .. commands.len - 1]) |api_command| {
            try self.submitApiCommandDetachedForBatch(api_command);
        }

        const final_handle = try self.submitApiCommandAsyncForBatch(commands[commands.len - 1]);
        // Java joins the final promise after its results-handler stage. Zig
        // intentionally waits through matching and R2 settlement, preserving
        // the stronger settlement-safe completion contract.
        _ = try self.drainAsyncPending();
        return self.takeApiCommandAsyncResult(final_handle) orelse
            error.MissingBatchResult;
    }

    pub fn submitApiCommandsSyncWithNop(
        self: *ExchangeCore,
        commands: []const ApiCommand,
    ) SubmitApiCommandBatchError!kernel_mod.CommandResponse {
        self.assertWiring();
        try self.ensureBlockingCallAllowed();

        for (commands) |api_command| try self.submitApiCommandDetachedForBatch(api_command);

        const flush_handle = try self.submitApiCommandAsyncForBatch(.nop);
        // The NOP is a FIFO barrier. Its result is exposed only after R2, which
        // is deliberately stronger than Java's earlier results-stage future.
        _ = try self.drainAsyncPending();
        var flush_result = self.takeApiCommandAsyncResult(flush_handle) orelse
            return error.MissingBatchResult;
        errdefer flush_result.deinit(self.allocator);
        try expectFlushResultVariant(&flush_result);
        return switch (flush_result) {
            .response => |response| response,
            .persist_state => return error.UnexpectedFlushResultVariant,
        };
    }

    fn registerSnapshotDescriptor(
        self: *ExchangeCore,
        snapshot_id: u64,
        sequence: u64,
        timestamp_ns: u64,
    ) DescriptorTrackingError!void {
        self.current_snapshot_ref = try self.descriptors.createNextSnapshot(
            self.current_snapshot_ref,
            snapshot_id,
            sequence,
            timestamp_ns,
        );
        self.last_journal_ref = null;
    }

    fn registerSnapshotDescriptorAssumeReserved(
        self: *ExchangeCore,
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
        self.last_journal_ref = null;
    }

    fn coordinatePersistenceCommand(
        self: *ExchangeCore,
        command: *cmd.OrderCommand,
        response: *kernel_mod.CommandResponse,
    ) void {
        const failure_code: cmd.CommandResultCode = switch (command.command) {
            .persist_state_matching => .state_persist_matching_engine_failed,
            .persist_state_risk => .state_persist_risk_engine_failed,
            else => return,
        };

        (switch (command.command) {
            .persist_state_matching => self.stageMatchingPersistence(command, response.sequence),
            .persist_state_risk => self.commitRiskPersistence(command, response.sequence),
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
        self: *ExchangeCore,
        command: *const cmd.OrderCommand,
        sequence: u64,
    ) PersistenceCoordinationError!void {
        self.pending_persist_pair = null;
        if (self.config.serialization.backend == .dummy) {
            return error.PersistenceBackendDisabled;
        }
        if (self.config.matching.shard_id != 0 or self.config.matching.num_shards != 1) {
            return error.UnsupportedPersistenceTopology;
        }
        if (self.config.serialization.backend == .disk and
            self.kernel.customReportRegistry().len() != 0)
        {
            return error.LegacySnapshotCustomReportRegistryUnsupported;
        }
        const snapshot_id = std.math.cast(u64, command.order_id) orelse return error.InvalidPersistSnapshotId;
        const timestamp_ns = std.math.cast(u64, command.timestamp) orelse return error.InvalidPersistTimestamp;
        const expected_risk_seq = std.math.add(u64, sequence, 1) catch return error.PersistSequenceOverflow;
        try self.descriptors.validateNextSnapshotSequence(self.current_snapshot_ref, expected_risk_seq);
        try self.descriptors.reserveNextSnapshot();

        if (self.custom_serialization_processor) |*processor| {
            var store_error: ?serialization_processor.StoreError = null;
            processor.storeData(.{
                .snapshot_id = snapshot_id,
                .sequence = sequence,
                .timestamp_ns = timestamp_ns,
                .module_type = .matching_engine_router,
                .instance_id = 0,
                .state = .{ .single = self.kernel },
            }) catch |err| {
                store_error = err;
            };
            self.pending_persist_pair = .{
                .snapshot_id = snapshot_id,
                .timestamp_ns = timestamp_ns,
                .matching_seq = sequence,
                .matching_store_succeeded = store_error == null,
                .matching_digest = null,
            };
            if (store_error) |err| return err;
            return;
        }

        const persistence_root = self.persistence_root orelse unreachable;
        try self.ensureConfiguredStorageFolderExists(persistence_root);
        const manifest_path = try self.defaultPersistenceSnapshotManifestPath(self.allocator, snapshot_id);
        defer self.allocator.free(manifest_path);
        if (self.snapshotExistsFile(persistence_root, manifest_path) catch |err| return err) {
            return error.SnapshotAlreadyCommitted;
        }

        if (self.consumePersistenceFault(.matching_before_write)) return error.InjectedPersistenceFault;
        const matching_path = try self.defaultPersistenceSnapshotPath(
            self.allocator,
            snapshot_id,
            .matching_engine_router,
            0,
        );
        defer self.allocator.free(matching_path);
        const shard_id = std.math.cast(u32, self.config.matching.shard_id) orelse return error.EngineCountOverflow;
        const shard_count = std.math.cast(u32, self.config.matching.num_shards) orelse return error.EngineCountOverflow;
        const digest = try snapshot_mod.writeMatchingPayloadAtomic(
            self.allocator,
            persistence_root,
            matching_path,
            self.kernel,
            .{
                .snapshot_id = snapshot_id,
                .command_seq = sequence,
                .next_seq = self.kernel.next_seq,
                .timestamp_ns = timestamp_ns,
                .shard_id = shard_id,
                .shard_count = shard_count,
            },
        );
        if (self.consumePersistenceFault(.matching_after_rename)) return error.InjectedPersistenceFault;
        self.pending_persist_pair = .{
            .snapshot_id = snapshot_id,
            .timestamp_ns = timestamp_ns,
            .matching_seq = sequence,
            .matching_store_succeeded = true,
            .matching_digest = digest,
        };
    }

    fn commitRiskPersistence(
        self: *ExchangeCore,
        command: *const cmd.OrderCommand,
        sequence: u64,
    ) PersistenceCoordinationError!void {
        if (self.config.serialization.backend == .dummy) {
            return error.PersistenceBackendDisabled;
        }
        // A custom risk command consumes its matching half even when command
        // validation or the risk callback fails. Disk cleanup remains tied to
        // manifest publication below.
        defer if (self.custom_serialization_processor != null) {
            self.pending_persist_pair = null;
        };
        const snapshot_id = std.math.cast(u64, command.order_id) orelse return error.InvalidPersistSnapshotId;
        const timestamp_ns = std.math.cast(u64, command.timestamp) orelse return error.InvalidPersistTimestamp;
        const pending = self.pending_persist_pair orelse return error.PersistPairMismatch;
        const expected_risk_seq = std.math.add(u64, pending.matching_seq, 1) catch return error.PersistSequenceOverflow;
        if (pending.snapshot_id != snapshot_id or pending.timestamp_ns != timestamp_ns or sequence != expected_risk_seq) {
            return error.PersistPairMismatch;
        }

        if (self.custom_serialization_processor) |*processor| {
            try processor.storeData(.{
                .snapshot_id = snapshot_id,
                .sequence = sequence,
                .timestamp_ns = timestamp_ns,
                .module_type = .risk_engine,
                .instance_id = 0,
                .state = .{ .single = self.kernel },
            });
            if (pending.matching_store_succeeded) {
                self.registerSnapshotDescriptorAssumeReserved(snapshot_id, sequence, timestamp_ns);
            }
            return;
        }

        const persistence_root = self.persistence_root orelse unreachable;
        const risk_path = try self.defaultPersistenceSnapshotPath(
            self.allocator,
            snapshot_id,
            .risk_engine,
            0,
        );
        defer self.allocator.free(risk_path);
        if (self.consumePersistenceFault(.risk_before_write)) return error.InjectedPersistenceFault;
        const risk_digest = try snapshot_mod.writeRiskPayloadAtomic(
            self.allocator,
            persistence_root,
            risk_path,
            self.kernel,
            .{
                .snapshot_id = snapshot_id,
                .command_seq = sequence,
                .next_seq = self.kernel.next_seq,
                .timestamp_ns = timestamp_ns,
                .shard_id = 0,
                .shard_count = 1,
            },
        );
        if (self.consumePersistenceFault(.risk_after_rename)) return error.InjectedPersistenceFault;

        if (self.consumePersistenceFault(.manifest_before_write)) return error.InjectedPersistenceFault;
        const manifest_path = try self.defaultPersistenceSnapshotManifestPath(self.allocator, snapshot_id);
        defer self.allocator.free(manifest_path);
        try snapshot_mod.writeManifestAtomic(
            self.allocator,
            persistence_root,
            manifest_path,
            .{
                .snapshot_id = snapshot_id,
                .matching_seq = pending.matching_seq,
                .risk_seq = sequence,
                .next_seq = self.kernel.next_seq,
                .timestamp_ns = timestamp_ns,
                .matching_shards = 1,
                .risk_shards = 1,
                .matching = pending.matching_digest.?,
                .risk = risk_digest,
            },
        );
        self.pending_persist_pair = null;
    }

    fn consumePersistenceFault(self: *ExchangeCore, point: PersistenceFaultPoint) bool {
        if (self.persistence_fault == point) {
            self.persistence_fault = null;
            return true;
        }
        return false;
    }

    fn registerPersistDescriptorIfNeeded(
        self: *ExchangeCore,
        command: *const cmd.OrderCommand,
        sequence: u64,
    ) void {
        if (command.command != .persist_state_risk) return;
        if (command.result_code != .success) return;
        // Live custom persistence registers only after both callbacks succeed,
        // while disk and replay retain their existing descriptor paths.
        if (self.custom_serialization_processor != null) return;

        const snapshot_id = std.math.cast(u64, command.order_id) orelse unreachable;
        const timestamp_ns = std.math.cast(u64, command.timestamp) orelse unreachable;
        self.registerSnapshotDescriptorAssumeReserved(snapshot_id, sequence, timestamp_ns);
    }

    /// Replay does not run the live snapshot coordinator, so a supplied Java
    /// terminal persistence pair has no pre-reserved descriptor slot. Register
    /// its committed snapshot identity fallibly after its recorded result has
    /// matched; any failure then poisons the already-mutated replay target.
    fn registerReplayedPersistDescriptorIfNeeded(
        self: *ExchangeCore,
        command: *const cmd.OrderCommand,
        sequence: u64,
        result_code: cmd.CommandResultCode,
    ) DescriptorTrackingError!void {
        if (command.command != .persist_state_risk) return;
        if (result_code != .success) return;

        const snapshot_id = std.math.cast(u64, command.order_id) orelse
            return error.InvalidPersistSnapshotId;
        const timestamp_ns = std.math.cast(u64, command.timestamp) orelse
            return error.InvalidPersistTimestamp;
        try self.registerSnapshotDescriptor(snapshot_id, sequence, timestamp_ns);
    }

    fn syncJournalDescriptorFromCurrentSnapshot(self: *ExchangeCore) DescriptorTrackingError!void {
        const journal = &(self.journal orelse return);
        if (journal.entries.items.len == 0) return;

        const current_snapshot = self.descriptors.snapshotConst(self.current_snapshot_ref);
        const snapshot_seq = current_snapshot.seq;

        var start_index: usize = 0;
        while (start_index < journal.entries.items.len and
            journal.entries.items[start_index].sequence <= snapshot_seq) : (start_index += 1)
        {}
        if (start_index >= journal.entries.items.len) return;

        const seq_first = journal.entries.items[start_index].sequence;
        const seq_last = journal.entries.items[journal.entries.items.len - 1].sequence;
        const timestamp_ns = std.math.cast(u64, journal.entries.items[start_index].input.timestamp) orelse
            return error.InvalidPersistTimestamp;

        var target_ref: ?JournalRef = null;
        if (self.last_journal_ref) |last_ref| {
            const last_desc = self.descriptors.journalConst(last_ref);
            if (last_desc.base_snapshot == self.current_snapshot_ref and
                last_desc.seq_first == seq_first)
            {
                target_ref = last_ref;
            }
        }

        if (target_ref == null) {
            if (self.descriptors.findJournalForSequence(self.current_snapshot_ref, seq_first)) |existing_ref| {
                const existing_desc = self.descriptors.journalConst(existing_ref);
                if (existing_desc.seq_first == seq_first) {
                    target_ref = existing_ref;
                }
            }
        }

        if (target_ref == null) {
            const created = try self.descriptors.createJournal(
                self.current_snapshot_ref,
                timestamp_ns,
                seq_first,
            );
            target_ref = created;
        }

        const journal_ref = target_ref.?;
        var descriptor = self.descriptors.journal(journal_ref);
        if (timestamp_ns < descriptor.timestamp_ns) {
            descriptor.timestamp_ns = timestamp_ns;
        }
        if (descriptor.seq_last) |known_last| {
            if (seq_last < known_last) return error.JournalSequenceNotMonotonic;
            if (seq_last > known_last) {
                try self.descriptors.finishJournal(journal_ref, seq_last);
            }
        } else {
            try self.descriptors.finishJournal(journal_ref, seq_last);
        }
        self.last_journal_ref = journal_ref;
    }

    fn assertWiring(self: *ExchangeCore) void {
        std.debug.assert(self.api.kernel == self.kernel);
        std.debug.assert(self.runtime.kernel == self.kernel);
        std.debug.assert((self.config.serialization.backend == .disk) == (self.persistence_root != null));
        std.debug.assert((self.config.serialization.backend == .custom) == (self.custom_serialization_processor != null));
        std.debug.assert(self.config.serialization.enable_journaling == (self.journal != null));
        std.debug.assert(self.durable_journal == null or
            (self.journal != null and self.config.serialization.backend == .disk));
    }

    fn assertWiringConst(self: *const ExchangeCore) void {
        std.debug.assert(self.api.kernel == self.kernel);
        std.debug.assert(self.runtime.kernel == self.kernel);
        std.debug.assert((self.config.serialization.backend == .disk) == (self.persistence_root != null));
        std.debug.assert((self.config.serialization.backend == .custom) == (self.custom_serialization_processor != null));
        std.debug.assert(self.config.serialization.enable_journaling == (self.journal != null));
        std.debug.assert(self.durable_journal == null or
            (self.journal != null and self.config.serialization.backend == .disk));
    }

    fn assertDescriptorWiringAssumeExecutionLocked(self: *const ExchangeCore) void {
        std.debug.assert(@intFromEnum(self.current_snapshot_ref) < self.descriptors.snapshotCount());
        if (self.last_journal_ref) |journal_ref| {
            std.debug.assert(@intFromEnum(journal_ref) < self.descriptors.journalCount());
        }
    }

    fn validateDurableJournalFrontier(
        self: *const ExchangeCore,
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

    fn adoptDurableSnapshotFrontier(
        self: *ExchangeCore,
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

    fn ensureRunning(self: *const ExchangeCore) LifecycleError!void {
        try self.ensureBlockingCallAllowed();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        return self.ensureAcceptingLocked();
    }

    /// Synchronous and lifecycle APIs can wait on driver, execution, worker,
    /// or completion ownership. Callback execution is process-thread scoped,
    /// so a callback from any backend poisons all such waits on this thread.
    fn ensureBlockingCallAllowed(self: *const ExchangeCore) LifecycleError!void {
        if (runtime_mod.isCallbackExecutionActive() or self.isWorkerThread()) {
            return error.CoreReentrantCall;
        }
    }

    /// Callback-originated publication stays legal when it can acquire every
    /// required owner immediately. A contended foreign backend must fail
    /// instead of completing a cross-backend lock cycle.
    fn lockDriverForPublication(self: *ExchangeCore) error{CoreReentrantCall}!void {
        const driver = self.driver;
        if (runtime_mod.isCallbackExecutionActive()) {
            if (!driver.mutex.tryLock()) return error.CoreReentrantCall;
            return;
        }
        driver.mutex.lockUncancelable(driver.io);
    }

    fn ensureAcceptingLocked(self: *const ExchangeCore) LifecycleError!void {
        return switch (self.runtime.lifecycleState()) {
            .cold, .running => if (self.driver.close_requested.load(.acquire))
                error.CoreStopped
            else {},
            .poisoned => error.CorePoisoned,
            .closing, .stopped => error.CoreStopped,
        };
    }

    fn bootstrapOwnedByCurrentThread(self: *ExchangeCore) LifecycleError!bool {
        try self.ensureBlockingCallAllowed();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        return driver.bootstrap_owner == std.Thread.getCurrentId();
    }

    fn beginBootstrap(self: *ExchangeCore) LifecycleError!void {
        try self.ensureBlockingCallAllowed();
        const driver = self.driver;
        const current_thread = std.Thread.getCurrentId();
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);

        try self.ensureAcceptingLocked();
        while (driver.bootstrap_owner != null or
            driver.manual_drain_active or
            driver.active_cold_calls != 0)
        {
            if (driver.bootstrap_owner == current_thread) return error.CoreReentrantCall;
            driver.completion_available.waitUncancelable(driver.io, &driver.mutex);
            try self.ensureAcceptingLocked();
        }
        switch (self.runtime.lifecycleState()) {
            .cold => {
                // Published pre-start work already owns sequence identities.
                // Restoring a different frontier underneath it would make the
                // queue's assigned identities stale.
                if (!driver.queue.isEmpty()) return error.CoreNotStarted;
                driver.bootstrap_owner = current_thread;
            },
            .running => return error.CoreAlreadyStarted,
            .poisoned => return error.CorePoisoned,
            .closing, .stopped => return error.CoreStopped,
        }
    }

    fn finishBootstrap(self: *ExchangeCore) void {
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        std.debug.assert(driver.bootstrap_owner == std.Thread.getCurrentId());
        driver.bootstrap_owner = null;
        driver.completion_available.broadcast(driver.io);
        driver.mutex.unlock(driver.io);
    }

    fn shouldUseWorkerForSynchronousCall(self: *ExchangeCore) LifecycleError!bool {
        try self.ensureBlockingCallAllowed();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        while (self.runtime.lifecycleState() == .cold and driver.active_cold_calls != 0) {
            driver.completion_available.waitUncancelable(driver.io, &driver.mutex);
        }
        try self.ensureAcceptingLocked();
        return switch (self.runtime.lifecycleState()) {
            .running => true,
            .cold => blk: {
                if (driver.bootstrap_owner != null or
                    driver.manual_drain_active or
                    !driver.queue.isEmpty())
                {
                    return error.CoreNotStarted;
                }
                driver.active_cold_calls += 1;
                break :blk false;
            },
            .closing, .stopped, .poisoned => unreachable,
        };
    }

    fn finishColdDirectCall(self: *ExchangeCore) void {
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        std.debug.assert(driver.active_cold_calls > 0);
        driver.active_cold_calls -= 1;
        driver.completion_available.broadcast(driver.io);
        driver.mutex.unlock(driver.io);
    }

    /// Admits one logical binary transfer, then exposes at most one quarter of
    /// the queue per publication step while the worker drains older frames.
    /// Every allocator, sequence, request-id, journal, and completion-capacity
    /// check is complete before the first frame becomes visible.
    fn enqueueBinaryFrames(
        self: *ExchangeCore,
        commands: []const cmd.OrderCommand,
        completion_mode: BinaryCompletionMode,
    ) AsyncSubmitError!?u64 {
        std.debug.assert(commands.len > 0);
        std.debug.assert(commands[commands.len - 1].symbol == -1);
        if (completion_mode.waitsForResponse()) {
            try self.ensureBlockingCallAllowed();
        }

        const driver = self.driver;
        try self.lockDriverForPublication();
        defer driver.mutex.unlock(driver.io);

        try self.waitForColdPublisherLocked();
        try self.waitForPublicationGateLocked();
        const running = self.runtime.lifecycleState() == .running;
        if (running and self.isWorkerThread()) {
            return error.CoreReentrantCall;
        }
        const chunk_capacity = driver.queue.capacity() / 4;
        std.debug.assert(chunk_capacity > 0);

        const poll_slots: usize = if (completion_mode.ownsPollSlot()) 1 else 0;
        if (running) {
            // Reserve only the logical terminal completion here. Queue space
            // is acquired chunk-by-chunk after the stream gate is installed.
            try self.waitForCapacityLocked(0, poll_slots);
            // A callback may publish a binary transfer only when every frame
            // fits now. Otherwise admission would commit the full accepted
            // range before a later chunk wait, leaving partial mutation on
            // the rejection path.
            if (runtime_mod.isCallbackExecutionActive() and
                driver.queue.remainingCapacity() < commands.len)
            {
                return error.CoreReentrantCall;
            }
        } else {
            // A cold core has no consumer. Preserve the existing all-or-none
            // behavior instead of admitting a transfer that cannot progress.
            try self.waitForCapacityLocked(commands.len, poll_slots);
        }

        const count_u64 = std.math.cast(u64, commands.len) orelse
            return error.RequestIdOverflow;
        const request_id: ?u64 = if (completion_mode.hasRequestId())
            std.math.add(u64, self.api.next_request_id, count_u64) catch
                return error.RequestIdOverflow
        else
            null;
        const accepted_after = std.math.add(
            u64,
            driver.accepted_total,
            count_u64,
        ) catch return error.RequestIdOverflow;
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
        const callback_pending_after = if (completion_mode.ownsCallback())
            std.math.add(usize, driver.callback_completions_pending, 1) catch
                return error.RequestIdOverflow
        else
            driver.callback_completions_pending;

        try self.ensureJournalCapacityLocked(commands.len);
        const sequences = try driver.publication_sequencer.prepare(commands.len);
        const work_kind = completion_mode.workKind();

        if (!running) {
            var reservation = driver.queue.reserve(commands.len) catch unreachable;
            for (commands, 0..) |command, index| {
                reservation.at(index).* = .{
                    .sequence = sequences.at(index),
                    .command = command,
                    .completion = if (index + 1 == commands.len)
                        completion_mode.terminalOwner(request_id)
                    else
                        .detached,
                    .kind = work_kind,
                };
            }
            driver.publication_sequencer.commit(sequences);
            driver.queue.commit(reservation);
            driver.journal_slots_reserved = journal_slots_after;
            driver.completion_slots_used += poll_slots;
            driver.callback_completions_pending = callback_pending_after;
            if (request_id) |id| self.api.next_request_id = id;
            driver.accepted_total = accepted_after;
            driver.async_accepted_total = async_accepted_after;
            driver.work_available.broadcast(driver.io);
            return request_id;
        }

        // From this point onward the transfer is admitted. There are no
        // remaining recoverable publication failures: shutdown lets it finish,
        // and a fatal worker error settles its unpublished suffix below.
        driver.publication_sequencer.commit(sequences);
        driver.journal_slots_reserved = journal_slots_after;
        driver.completion_slots_used += poll_slots;
        driver.callback_completions_pending = callback_pending_after;
        if (request_id) |id| self.api.next_request_id = id;
        driver.accepted_total = accepted_after;
        driver.async_accepted_total = async_accepted_after;
        driver.active_binary_stream = true;

        var published: usize = 0;
        while (published < commands.len) {
            const chunk_len = @min(chunk_capacity, commands.len - published);
            self.waitForBinaryChunkCapacityLocked(chunk_len) catch |err| {
                self.abortBinaryStreamLocked(
                    completion_mode,
                    request_id,
                    commands.len - published,
                );
                return err;
            };

            var reservation = driver.queue.reserve(chunk_len) catch unreachable;
            for (0..chunk_len) |chunk_index| {
                const command_index = published + chunk_index;
                reservation.at(chunk_index).* = .{
                    .sequence = sequences.at(command_index),
                    .command = commands[command_index],
                    .completion = if (command_index + 1 == commands.len)
                        completion_mode.terminalOwner(request_id)
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

    /// Settles the admitted but never-visible suffix after the worker poisons
    /// the core. Published frames are settled by the ordinary fatal drain.
    fn abortBinaryStreamLocked(
        self: *ExchangeCore,
        completion_mode: BinaryCompletionMode,
        request_id: ?u64,
        unpublished_count: usize,
    ) void {
        const driver = self.driver;
        std.debug.assert(driver.active_binary_stream);
        std.debug.assert(unpublished_count > 0);
        std.debug.assert(self.runtime.lifecycleState() == .poisoned);

        if (completion_mode.ownsCallback()) {
            const fatal = self.runtime.fatalRecord().?.*;
            const owner = completion_mode.terminalOwner(request_id).callback;
            // Match normal callback settlement ordering: deliver the terminal
            // failure before decrementing pending/completed counters.
            driver.mutex.unlock(driver.io);
            self.dispatchCommandFailure(owner, .{
                .request_id = owner.request_id,
                .kind = .skipped_after_fatal,
                .fatal = fatal,
            });
            driver.mutex.lockUncancelable(driver.io);
            std.debug.assert(driver.callback_completions_pending > 0);
            driver.callback_completions_pending -= 1;
        }

        if (completion_mode.ownsPollSlot()) {
            std.debug.assert(driver.completion_slots_used > self.api.ready_results.count());
            driver.completion_slots_used -= 1;
        }
        if (self.journal != null) {
            std.debug.assert(driver.journal_slots_reserved >= unpublished_count);
            driver.journal_slots_reserved -= unpublished_count;
        }
        const unpublished_u64 = std.math.cast(u64, unpublished_count) orelse unreachable;
        driver.recordDeferredTerminalSuffixLocked(unpublished_u64);
        if (completion_mode.countsAsAsync()) {
            driver.async_completed_total += unpublished_u64;
        }
        driver.active_binary_stream = false;
        driver.work_available.broadcast(driver.io);
        driver.space_available.broadcast(driver.io);
        driver.completion_available.broadcast(driver.io);
    }

    fn enqueueResponseBatch(
        self: *ExchangeCore,
        commands: []const cmd.OrderCommand,
        kind: WorkKind,
        request_ids: []u64,
    ) AsyncSubmitError!void {
        std.debug.assert(kind == .asynchronous or kind == .asynchronous_raw or kind == .synchronous or kind == .synchronous_raw);
        std.debug.assert(commands.len == request_ids.len);
        if (commands.len == 0) return;

        const driver = self.driver;
        if (kind == .synchronous or kind == .synchronous_raw) {
            try self.ensureBlockingCallAllowed();
        } else if (commands.len > driver.queue.capacity()) {
            // This request is impossible even on an idle backend, so preserve
            // bounded queue semantics without attempting callback lock
            // ownership first.
            return error.CoreQueueFull;
        }
        try self.lockDriverForPublication();
        defer driver.mutex.unlock(driver.io);
        try self.waitForColdPublisherLocked();
        try self.waitForCapacityLocked(commands.len, commands.len);

        const count_u64 = std.math.cast(u64, commands.len) orelse return error.RequestIdOverflow;
        const final_request_id = std.math.add(u64, self.api.next_request_id, count_u64) catch
            return error.RequestIdOverflow;
        try self.ensureJournalCapacityLocked(commands.len);
        const sequences = try driver.publication_sequencer.prepare(commands.len);
        var reservation = driver.queue.reserve(commands.len) catch unreachable;
        for (commands, 0..) |command, index| {
            const index_u64 = std.math.cast(u64, index) orelse return error.RequestIdOverflow;
            const request_id = self.api.next_request_id + index_u64 + 1;
            request_ids[index] = request_id;
            reservation.at(index).* = .{
                .sequence = sequences.at(index),
                .command = command,
                .completion = .{ .poll = request_id },
                .kind = kind,
            };
        }

        driver.publication_sequencer.commit(sequences);
        driver.queue.commit(reservation);
        if (self.journal != null) driver.journal_slots_reserved += commands.len;
        driver.completion_slots_used += commands.len;
        self.api.next_request_id = final_request_id;
        driver.accepted_total += count_u64;
        if (kind == .asynchronous or kind == .asynchronous_raw) {
            driver.async_accepted_total += count_u64;
        }
        driver.work_available.broadcast(driver.io);
    }

    fn enqueueCallback(
        self: *ExchangeCore,
        command: cmd.OrderCommand,
        callback: CommandResponseCallback,
    ) AsyncSubmitError!void {
        const driver = self.driver;
        try self.lockDriverForPublication();
        defer driver.mutex.unlock(driver.io);
        try self.waitForColdPublisherLocked();

        // Callback owners never enter ready_results, so they consume queue
        // capacity but not poll-result capacity.
        try self.waitForCapacityLocked(1, 0);
        const request_id = std.math.add(u64, self.api.next_request_id, 1) catch
            return error.RequestIdOverflow;
        try self.ensureJournalCapacityLocked(1);
        const sequences = try driver.publication_sequencer.prepare(1);
        var reservation = driver.queue.reserve(1) catch unreachable;
        reservation.at(0).* = .{
            .sequence = sequences.first,
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

        self.api.next_request_id = request_id;
        driver.callback_completions_pending += 1;
        driver.accepted_total += 1;
        driver.async_accepted_total += 1;
        driver.work_available.broadcast(driver.io);
    }

    fn enqueueDetachedBatch(
        self: *ExchangeCore,
        commands: []const cmd.OrderCommand,
        kind: WorkKind,
    ) (LifecycleError || std.mem.Allocator.Error || publication.PrepareError)!void {
        std.debug.assert(kind == .detached_api or kind == .detached_api_raw or kind == .runtime);
        if (commands.len == 0) return;

        const driver = self.driver;
        if (commands.len > driver.queue.capacity()) return error.CoreQueueFull;
        try self.lockDriverForPublication();
        defer driver.mutex.unlock(driver.io);
        try self.waitForColdPublisherLocked();
        try self.waitForCapacityLocked(commands.len, 0);
        const count_u64 = std.math.cast(u64, commands.len) orelse return error.CoreQueueFull;
        try self.ensureJournalCapacityLocked(commands.len);
        const sequences = try driver.publication_sequencer.prepare(commands.len);
        var reservation = driver.queue.reserve(commands.len) catch unreachable;
        for (commands, 0..) |command, index| {
            reservation.at(index).* = .{
                .sequence = sequences.at(index),
                .command = command,
                .completion = .detached,
                .kind = kind,
            };
        }
        driver.publication_sequencer.commit(sequences);
        driver.queue.commit(reservation);
        if (self.journal != null) driver.journal_slots_reserved += commands.len;
        driver.accepted_total += count_u64;
        if (kind == .runtime) driver.runtime_accepted_total += count_u64;
        driver.work_available.broadcast(driver.io);
    }

    /// Called with the driver mutex held. Queue visibility cannot advance
    /// until journal storage for every assigned slot has been preflighted.
    fn ensureJournalCapacityLocked(
        self: *ExchangeCore,
        additional: usize,
    ) (std.mem.Allocator.Error || error{CoreReentrantCall})!void {
        if (self.journal) |*journal| {
            if (runtime_mod.isCallbackExecutionActive()) {
                if (!self.execution_mutex.tryLock()) return error.CoreReentrantCall;
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
        self: *ExchangeCore,
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
            if (self.runtime.lifecycleState() == .cold) return error.CoreQueueFull;
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
        // A stream can win the mutex after the final capacity wake but before
        // this producer reacquires it; recheck the gate at the commit frontier.
        try self.waitForPublicationGateLocked();
        try self.ensureAcceptingLocked();
    }

    /// Later publishers must not reserve a sequence range or enter the FIFO
    /// between chunks of an already-admitted binary transfer.
    fn waitForPublicationGateLocked(self: *ExchangeCore) LifecycleError!void {
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

    /// Once a stream is admitted, `.closing` is not a cancellation signal:
    /// shutdown waits for its terminal frame. Only fatal poison can truncate
    /// the stream, in which case the unpublished suffix is explicitly settled.
    fn waitForBinaryChunkCapacityLocked(
        self: *ExchangeCore,
        chunk_len: usize,
    ) LifecycleError!void {
        const driver = self.driver;
        std.debug.assert(driver.active_binary_stream);
        std.debug.assert(chunk_len > 0 and chunk_len <= driver.queue.capacity() / 4);
        while (driver.queue.remainingCapacity() < chunk_len) {
            switch (self.runtime.lifecycleState()) {
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
        switch (self.runtime.lifecycleState()) {
            .running, .closing => {},
            .poisoned => return error.CorePoisoned,
            .stopped => return error.CoreStopped,
            .cold => unreachable,
        }
    }

    fn waitForColdPublisherLocked(self: *ExchangeCore) LifecycleError!void {
        const driver = self.driver;
        if (self.runtime.lifecycleState() == .cold and driver.bootstrap_owner != null) {
            return error.CoreNotStarted;
        }
        while (self.runtime.lifecycleState() == .cold and driver.active_cold_calls != 0) {
            if (runtime_mod.isCallbackExecutionActive() or self.isWorkerThread()) {
                return error.CoreReentrantCall;
            }
            driver.completion_available.waitUncancelable(driver.io, &driver.mutex);
        }
        try self.ensureAcceptingLocked();
        if (self.runtime.lifecycleState() == .cold and driver.bootstrap_owner != null) {
            return error.CoreNotStarted;
        }
    }

    fn takeReadyResultLocked(
        self: *ExchangeCore,
        request_id: u64,
    ) ?kernel_mod.CommandResponse {
        const removed = self.api.ready_results.fetchRemove(request_id) orelse return null;
        std.debug.assert(self.driver.completion_slots_used > 0);
        self.driver.completion_slots_used -= 1;
        self.driver.space_available.broadcast(self.driver.io);
        return removed.value;
    }

    fn waitTakeResponse(
        self: *ExchangeCore,
        request_id: u64,
    ) LifecycleError!kernel_mod.CommandResponse {
        try self.ensureBlockingCallAllowed();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        while (true) {
            if (self.takeReadyResultLocked(request_id)) |response| return response;
            switch (self.runtime.lifecycleState()) {
                .poisoned => return error.CorePoisoned,
                .stopped => return error.CoreStopped,
                .cold => return error.CoreNotStarted,
                .running, .closing => {},
            }
            switch (self.config.runtime.wait_strategy) {
                .blocking => driver.completion_available.waitUncancelable(driver.io, &driver.mutex),
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
        }
    }

    fn waitForAcceptedPrefix(self: *ExchangeCore) LifecycleError!bool {
        try self.ensureBlockingCallAllowed();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        while (self.runtime.lifecycleState() == .cold and driver.active_cold_calls != 0) {
            driver.completion_available.waitUncancelable(driver.io, &driver.mutex);
        }
        try self.ensureAcceptingLocked();
        if (self.runtime.lifecycleState() == .cold) {
            if (driver.bootstrap_owner != null or
                driver.manual_drain_active or
                !driver.queue.isEmpty())
            {
                return error.CoreNotStarted;
            }
            driver.active_cold_calls += 1;
            return true;
        }
        const target = driver.accepted_total;
        while (driver.settled_prefix_total < target) {
            if (self.runtime.lifecycleState() == .poisoned) return error.CorePoisoned;
            driver.completion_available.waitUncancelable(driver.io, &driver.mutex);
        }
        if (self.runtime.lifecycleState() == .poisoned) return error.CorePoisoned;
        return false;
    }

    /// Pops one slot from a snapshot of the currently available queue prefix.
    /// The driver mutex must be held. Later publications append behind the
    /// captured frontier and therefore form a subsequent journal batch.
    fn popPreparedWorkLocked(driver: *RuntimeDriverState) PreparedCoreWork {
        if (driver.prepared_available_remaining == 0) {
            driver.prepared_available_remaining = driver.queue.count();
            std.debug.assert(driver.prepared_available_remaining > 0);
        }
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

    fn workerMain(driver: *RuntimeDriverState) void {
        const owner = driver.owner.?;
        active_worker_core = owner;
        defer active_worker_core = null;

        driver.mutex.lockUncancelable(driver.io);
        driver.worker_id = std.Thread.getCurrentId();
        driver.mutex.unlock(driver.io);
        defer {
            driver.mutex.lockUncancelable(driver.io);
            driver.worker_id = null;
            if (owner.runtime.lifecycleState() == .closing) {
                _ = owner.runtime.markStopped() catch unreachable;
            }
            driver.owner = null;
            driver.completion_available.broadcast(driver.io);
            driver.space_available.broadcast(driver.io);
            driver.mutex.unlock(driver.io);

            // This is the worker's final driver access. The release-published
            // event makes every callback/counter update visible to joiners.
            driver.worker_quiesced.set(driver.io);
        }
        while (true) {
            driver.mutex.lockUncancelable(driver.io);
            while (driver.manual_drain_active or
                driver.active_cold_calls != 0 or
                driver.bootstrap_owner != null)
            {
                // A cold timed close may install this worker while a direct
                // caller still owns execution. It cannot touch that caller's
                // extracted prefix or publish quiescence until ownership is
                // released, including when that direct caller poisons.
                driver.completion_available.waitUncancelable(driver.io, &driver.mutex);
            }
            observeCloseRequestLocked(driver);

            if (owner.runtime.lifecycleState() == .poisoned) {
                if (driver.queue.pop()) |item| {
                    driver.prepared_available_remaining = 0;
                    const fatal = owner.runtime.fatalRecord().?.*;
                    driver.space_available.broadcast(driver.io);
                    driver.mutex.unlock(driver.io);
                    owner.settleFatalWorkItem(item, .skipped_after_fatal, fatal);
                    continue;
                }
                if (!driver.active_binary_stream and
                    driver.allAcceptedSettledLocked())
                {
                    driver.completion_available.broadcast(driver.io);
                    driver.mutex.unlock(driver.io);
                    return;
                }
                // External/direct poison may precede producer suffix and
                // callback settlement. Never publish quiescence until those
                // accepted owners have converged.
                driver.completion_available.waitUncancelable(driver.io, &driver.mutex);
                driver.mutex.unlock(driver.io);
                continue;
            }

            if (owner.runtime.lifecycleState() == .stopped) {
                std.debug.assert(driver.queue.isEmpty());
                std.debug.assert(!driver.active_binary_stream);
                std.debug.assert(driver.allAcceptedSettledLocked());
                std.debug.assert(driver.accepted_total == driver.completed_total);
                driver.completion_available.broadcast(driver.io);
                driver.mutex.unlock(driver.io);
                return;
            }

            if (!driver.queue.isEmpty()) {
                const prepared = popPreparedWorkLocked(driver);
                driver.space_available.broadcast(driver.io);
                driver.mutex.unlock(driver.io);
                if (owner.available_batch_test_gate) |gate| gate.pauseOnce();

                _ = owner.processQueuedItem(
                    prepared.item,
                    prepared.journal_batch_position,
                ) catch |err| {
                    owner.poisonFromWork(prepared.item, err);
                    // The producer owns settlement of an admitted stream
                    // suffix that never reached the queue. Keep the
                    // worker/join frontier open until its terminal ownership
                    // has been discharged.
                    driver.mutex.lockUncancelable(driver.io);
                    while (driver.active_binary_stream or
                        !driver.allAcceptedSettledLocked())
                    {
                        driver.completion_available.waitUncancelable(
                            driver.io,
                            &driver.mutex,
                        );
                    }
                    driver.mutex.unlock(driver.io);
                    return;
                };
                continue;
            }

            switch (owner.runtime.lifecycleState()) {
                .closing => {
                    if (!driver.active_binary_stream and
                        driver.allAcceptedSettledLocked())
                    {
                        driver.completion_available.broadcast(driver.io);
                        driver.mutex.unlock(driver.io);
                        return;
                    }
                    if (driver.active_binary_stream) {
                        driver.work_available.waitUncancelable(driver.io, &driver.mutex);
                    } else {
                        driver.completion_available.waitUncancelable(
                            driver.io,
                            &driver.mutex,
                        );
                    }
                    driver.mutex.unlock(driver.io);
                },
                .running => switch (owner.config.runtime.wait_strategy) {
                    .blocking => {
                        driver.work_available.waitUncancelable(driver.io, &driver.mutex);
                        driver.mutex.unlock(driver.io);
                    },
                    .yielding, .second_step_no_wait => {
                        driver.mutex.unlock(driver.io);
                        std.Thread.yield() catch {};
                    },
                    .busy_spin => {
                        driver.mutex.unlock(driver.io);
                        std.atomic.spinLoopHint();
                    },
                },
                .cold, .stopped, .poisoned => unreachable,
            }
        }
    }

    fn executeCommandDirect(
        self: *ExchangeCore,
        command: *cmd.OrderCommand,
    ) ExecuteError!kernel_mod.CommandResponse {
        const sequences = try self.prepareColdPublicationRange(1);
        return self.executePreparedDirect(sequences.first, command, .terminal);
    }

    fn prepareColdPublicationRange(
        self: *ExchangeCore,
        count: usize,
    ) ExecuteError!publication.PublicationRange {
        // Keep the only nested lock order consistent with publication: driver
        // first, then execution. Cold-call ownership excludes other publishers
        // while both the journal reservation and sequence range are prepared.
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        self.execution_mutex.lockUncancelable(self.driver.io);
        defer self.execution_mutex.unlock(self.driver.io);
        if (self.journal) |*journal| {
            try journal.entries.ensureUnusedCapacity(self.allocator, count);
        }
        const prepared = try driver.publication_sequencer.prepare(count);
        driver.publication_sequencer.commit(prepared);
        return prepared;
    }

    fn executePreparedDirect(
        self: *ExchangeCore,
        sequence: publication.PublicationSequence,
        command: *cmd.OrderCommand,
        journal_batch_position: serialization_processor.JournalBatchPosition,
    ) ExecuteError!kernel_mod.CommandResponse {
        self.execution_mutex.lockUncancelable(self.driver.io);
        var response = self.executeCommandDirectAtSequence(
            sequence,
            command,
            journal_batch_position,
        ) catch |err| {
            self.execution_mutex.unlock(self.driver.io);
            self.poisonDirectExecution(sequence, command, err);
            return err;
        };
        self.execution_mutex.unlock(self.driver.io);
        self.publishCommandResult(command, &response);
        return response;
    }

    /// Executes one already-published slot while the execution mutex is held.
    /// Journal capacity was reserved before publication became visible.
    fn executeCommandDirectAtSequence(
        self: *ExchangeCore,
        sequence: publication.PublicationSequence,
        command: *cmd.OrderCommand,
        journal_batch_position: serialization_processor.JournalBatchPosition,
    ) (kernel_mod.ExecuteError || DescriptorTrackingError || journal_mod.JournalIoError || serialization_processor.JournalError)!kernel_mod.CommandResponse {
        var response = if (self.journal) |*journal|
            try journal.executeAndRecordAtSequenceAssumeCapacity(
                self.kernel,
                sequence,
                command,
            )
        else
            try self.kernel.executeAtSequence(sequence, command);
        errdefer response.deinit(self.allocator);
        self.coordinatePersistenceCommand(command, &response);
        self.registerPersistDescriptorIfNeeded(command, response.sequence);
        try self.finalizeJournalCommit(&response, journal_batch_position);
        return response;
    }

    /// The in-memory record is reserved and appended before execution. Only
    /// after persistence coordination has finalized the command result can it
    /// be corrected and synchronously committed to XCJ3. Callers publish no
    /// result until this returns.
    fn finalizeJournalCommit(
        self: *ExchangeCore,
        response: *const kernel_mod.CommandResponse,
        journal_batch_position: serialization_processor.JournalBatchPosition,
    ) (journal_mod.JournalIoError || serialization_processor.JournalError)!void {
        const journal = &(self.journal orelse return);
        if (journal.entries.items.len == 0) return error.JournalEntryMismatch;
        const entry = &journal.entries.items[journal.entries.items.len - 1];
        if (entry.sequence != response.sequence) return error.JournalEntryMismatch;
        entry.result_code = response.result_code;

        if (self.custom_serialization_processor) |*processor| {
            try processor.writeToJournal(.{
                .sequence = entry.sequence,
                .command = entry.input,
                .end_of_batch = journal_batch_position.endOfBatch(),
            });
        }

        const appender = &(self.durable_journal orelse return);
        if (shouldAppendToJavaDurableJournal(entry.input.command)) {
            try appender.append(entry.*);
        } else {
            try appender.appendSkippedSequence(entry.sequence, entry.input.timestamp);
        }
    }

    fn poisonDirectExecution(
        self: *ExchangeCore,
        sequence: publication.PublicationSequence,
        command: *const cmd.OrderCommand,
        failure: anyerror,
    ) void {
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        const fatal: CoreFatalInfo = .{
            .stage = failureStage(failure),
            .sequence = sequence.toInt(),
            .command = command.command,
            .request_id = null,
            .cause = failure,
        };
        if (self.runtime.poison(fatal)) {
            self.stopped = true;
            driver.work_available.broadcast(driver.io);
            driver.space_available.broadcast(driver.io);
            driver.completion_available.broadcast(driver.io);
        }
        driver.mutex.unlock(driver.io);
    }

    fn processQueuedItem(
        self: *ExchangeCore,
        item: CoreWorkItem,
        journal_batch_position: serialization_processor.JournalBatchPosition,
    ) anyerror!cmd.CommandResultCode {
        var mutable = item.command;
        // Java-style live submissions traverse GroupingProcessor. Only the
        // explicit raw-command escape hatch preserves caller-owned metadata.
        if (item.kind != .asynchronous_raw and
            item.kind != .synchronous_raw and
            item.kind != .detached_api_raw)
        {
            self.runtime.stampBatchMetadata(&.{&mutable});
        }
        // Unlike Java's early results stage, readiness is published only after
        // matching and R2 settlement complete. This intentionally strengthens
        // completion to be settlement-safe and makes fatal R2 errors observable.
        self.execution_mutex.lockUncancelable(self.driver.io);
        var response = self.executeCommandDirectAtSequence(
            item.sequence,
            &mutable,
            journal_batch_position,
        ) catch |err| {
            self.execution_mutex.unlock(self.driver.io);
            return err;
        };
        self.execution_mutex.unlock(self.driver.io);
        self.publishCommandResult(&mutable, &response);
        const result_code = response.result_code;

        switch (item.completion) {
            .poll => |request_id| {
                self.driver.mutex.lockUncancelable(self.driver.io);
                std.debug.assert(self.driver.completion_slots_used > self.api.ready_results.count());
                self.api.ready_results.putAssumeCapacityNoClobber(request_id, response);
                if (item.kind == .runtime) self.driver.last_runtime_result = result_code;
                self.finishWorkLocked(item.kind);
                self.driver.mutex.unlock(self.driver.io);
            },
            .detached => {
                response.deinit(self.allocator);
                self.driver.mutex.lockUncancelable(self.driver.io);
                if (item.kind == .runtime) self.driver.last_runtime_result = result_code;
                self.finishWorkLocked(item.kind);
                self.driver.mutex.unlock(self.driver.io);
            },
            .callback => |owner| {
                self.dispatchCommandResponse(owner, &response);
                response.deinit(self.allocator);
                self.driver.mutex.lockUncancelable(self.driver.io);
                std.debug.assert(self.driver.callback_completions_pending > 0);
                self.driver.callback_completions_pending -= 1;
                if (item.kind == .runtime) self.driver.last_runtime_result = result_code;
                self.finishWorkLocked(item.kind);
                self.driver.mutex.unlock(self.driver.io);
            },
        }
        return result_code;
    }

    fn finishWorkLocked(self: *ExchangeCore, kind: WorkKind) void {
        const driver = self.driver;
        if (self.journal != null) {
            std.debug.assert(driver.journal_slots_reserved > 0);
            driver.journal_slots_reserved -= 1;
        }
        driver.recordInOrderSettlementLocked();
        switch (kind) {
            .asynchronous, .asynchronous_raw => driver.async_completed_total += 1,
            .runtime => driver.runtime_completed_total += 1,
            .synchronous, .synchronous_raw, .detached_api, .detached_api_raw => {},
        }
        driver.completion_available.broadcast(driver.io);
    }

    fn poisonFromWork(self: *ExchangeCore, item: CoreWorkItem, failure: anyerror) void {
        self.poisonFromWorkWithExtractedTail(item, failure, &.{});
    }

    fn poisonFromWorkWithExtractedTail(
        self: *ExchangeCore,
        failed_item: CoreWorkItem,
        failure: anyerror,
        extracted_tail: []const CoreWorkItem,
    ) void {
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        const fatal: CoreFatalInfo = .{
            .stage = failureStage(failure),
            .sequence = failed_item.sequence.toInt(),
            .command = failed_item.command.command,
            .request_id = failed_item.completion.requestId(),
            .cause = failure,
        };
        const published_here = self.runtime.poison(fatal);
        driver.prepared_available_remaining = 0;
        const published_fatal: CoreFatalInfo = if (published_here) blk: {
            self.stopped = true;
            driver.work_available.broadcast(driver.io);
            driver.space_available.broadcast(driver.io);
            driver.completion_available.broadcast(driver.io);
            break :blk fatal;
        } else blk: {
            // An external/direct failure may publish poison after this worker
            // has popped an item but before that item fails. First-fatal wins,
            // but the losing item and its extracted tail still own accepted
            // settlement and must converge exactly once.
            std.debug.assert(self.runtime.lifecycleState() == .poisoned);
            break :blk self.runtime.fatalRecord().?.*;
        };
        driver.mutex.unlock(driver.io);

        // Preserve accepted publication order: the failing item, any items
        // already extracted by a cold drain, then newer items still queued.
        self.settleFatalWorkItem(
            failed_item,
            if (published_here)
                .command_failed
            else
                .skipped_after_fatal,
            published_fatal,
        );
        for (extracted_tail) |item| {
            self.settleFatalWorkItem(item, .skipped_after_fatal, published_fatal);
        }
        while (true) {
            driver.mutex.lockUncancelable(driver.io);
            const item = driver.queue.pop() orelse {
                driver.mutex.unlock(driver.io);
                break;
            };
            driver.space_available.broadcast(driver.io);
            driver.mutex.unlock(driver.io);
            self.settleFatalWorkItem(item, .skipped_after_fatal, published_fatal);
        }
    }

    fn settleFatalWorkItem(
        self: *ExchangeCore,
        item: CoreWorkItem,
        failure_kind: CallbackFailureKind,
        fatal: CoreFatalInfo,
    ) void {
        switch (item.completion) {
            .detached => {},
            .poll => {
                self.driver.mutex.lockUncancelable(self.driver.io);
                std.debug.assert(self.driver.completion_slots_used > self.api.ready_results.count());
                self.driver.completion_slots_used -= 1;
                self.driver.space_available.broadcast(self.driver.io);
                self.driver.mutex.unlock(self.driver.io);
            },
            .callback => |owner| {
                self.dispatchCommandFailure(owner, .{
                    .request_id = owner.request_id,
                    .kind = failure_kind,
                    .fatal = fatal,
                });
                self.driver.mutex.lockUncancelable(self.driver.io);
                std.debug.assert(self.driver.callback_completions_pending > 0);
                self.driver.callback_completions_pending -= 1;
                self.driver.mutex.unlock(self.driver.io);
            },
        }

        self.driver.mutex.lockUncancelable(self.driver.io);
        self.finishWorkLocked(item.kind);
        self.driver.mutex.unlock(self.driver.io);
    }

    fn dispatchCommandResponse(
        self: *ExchangeCore,
        owner: CallbackCompletionOwner,
        response: *const kernel_mod.CommandResponse,
    ) void {
        self.callback_execution_mutex.lockUncancelable(self.driver.io);
        defer self.callback_execution_mutex.unlock(self.driver.io);
        var callback_scope = runtime_mod.CallbackExecutionScope.enter();
        defer callback_scope.leave();
        const previous_callback_core = active_callback_core;
        active_callback_core = self;
        defer active_callback_core = previous_callback_core;
        owner.callback.callback(owner.callback.context, response);
    }

    fn dispatchCommandFailure(
        self: *ExchangeCore,
        owner: CallbackCompletionOwner,
        failure: CallbackFailure,
    ) void {
        const callback = owner.callback.failure_callback orelse return;
        self.callback_execution_mutex.lockUncancelable(self.driver.io);
        defer self.callback_execution_mutex.unlock(self.driver.io);
        var callback_scope = runtime_mod.CallbackExecutionScope.enter();
        defer callback_scope.leave();
        const previous_callback_core = active_callback_core;
        active_callback_core = self;
        defer active_callback_core = previous_callback_core;
        callback(owner.callback.context, failure);
    }

    fn drainAcceptedOnCurrentThread(self: *ExchangeCore) void {
        while (true) {
            const driver = self.driver;
            driver.mutex.lockUncancelable(driver.io);
            if (self.runtime.lifecycleState() == .poisoned) {
                const item = driver.queue.pop() orelse {
                    driver.mutex.unlock(driver.io);
                    return;
                };
                driver.prepared_available_remaining = 0;
                const fatal = self.runtime.fatalRecord().?.*;
                driver.space_available.broadcast(driver.io);
                driver.mutex.unlock(driver.io);
                self.settleFatalWorkItem(item, .skipped_after_fatal, fatal);
                continue;
            }
            if (driver.queue.isEmpty()) {
                driver.mutex.unlock(driver.io);
                return;
            }
            const prepared = popPreparedWorkLocked(driver);
            driver.space_available.broadcast(driver.io);
            driver.mutex.unlock(driver.io);
            _ = self.processQueuedItem(
                prepared.item,
                prepared.journal_batch_position,
            ) catch |err| {
                self.poisonFromWork(prepared.item, err);
                return;
            };
        }
    }

    fn finishManualDrain(self: *ExchangeCore) void {
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        std.debug.assert(driver.manual_drain_active);
        driver.manual_drain_active = false;
        driver.completion_available.broadcast(driver.io);
        driver.mutex.unlock(driver.io);
    }

    pub fn fatalInfo(self: *const ExchangeCore) ?CoreFatalInfo {
        self.assertWiringConst();
        const driver = self.driver;
        driver.mutex.lockUncancelable(driver.io);
        defer driver.mutex.unlock(driver.io);
        const record = self.runtime.fatalRecord() orelse return null;
        return record.*;
    }

    fn isWorkerThread(self: *const ExchangeCore) bool {
        return active_worker_core == self;
    }

    fn isCallbackThread(self: *const ExchangeCore) bool {
        return active_callback_core == self;
    }

    /// Returns true when this call acquired the execution barrier and must
    /// release it. Same-core callbacks already own the barrier. A callback on
    /// any exchange backend may only acquire it non-blockingly, preventing
    /// backends from deadlocking while synchronously reconfiguring each other.
    fn lockCallbackConfigBarrier(self: *ExchangeCore) CallbackConfigError!bool {
        if (active_callback_core == self) return false;
        if (runtime_mod.isCallbackExecutionActive()) {
            if (!self.callback_execution_mutex.tryLock()) {
                return error.CallbackReconfigurationWouldBlock;
            }
            return true;
        }
        self.callback_execution_mutex.lockUncancelable(self.driver.io);
        return true;
    }

    fn advanceSimpleEventsRevisionLocked(self: *ExchangeCore) CallbackConfigError!void {
        self.simple_events_revision = std.math.add(
            u64,
            self.simple_events_revision,
            1,
        ) catch return error.CallbackRevisionOverflow;
    }

    fn expectFlushResultVariant(result: *const ApiCommandResult) error{UnexpectedFlushResultVariant}!void {
        return switch (result.*) {
            .response => {},
            .persist_state => error.UnexpectedFlushResultVariant,
        };
    }

    fn publishCommandResult(
        self: *ExchangeCore,
        command: *const cmd.OrderCommand,
        response: *const kernel_mod.CommandResponse,
    ) void {
        // Enter before taking any callback snapshot. An external setter that
        // returns has therefore quiesced every previously installed context.
        self.callback_execution_mutex.lockUncancelable(self.driver.io);
        defer self.callback_execution_mutex.unlock(self.driver.io);

        var callback_scope = runtime_mod.CallbackExecutionScope.enter();
        defer callback_scope.leave();
        const previous_callback_core = active_callback_core;
        active_callback_core = self;
        defer active_callback_core = previous_callback_core;

        self.callback_mutex.lockUncancelable(self.driver.io);
        if (!self.runtime.shouldPublishResultForCommand(command)) {
            self.callback_mutex.unlock(self.driver.io);
            return;
        }
        const raw_sink = self.raw_result_sink;
        self.callback_mutex.unlock(self.driver.io);

        if (raw_sink.on_result) |callback| {
            callback(raw_sink.context, response);
        }

        // The raw sink may reentrantly replace/clear the runtime sink.
        self.callback_mutex.lockUncancelable(self.driver.io);
        const sink = self.runtime.sink;
        self.callback_mutex.unlock(self.driver.io);

        publishRuntimeResult(sink, .{
            .sequence = response.sequence,
            .command = command.command,
            .uid = command.uid,
            .symbol = command.symbol,
            .result_code = command.result_code,
        });

        // The result sink may reentrantly replace/clear SimpleEvents. Snapshot
        // the post-sink configuration so no stale handler is invoked after its
        // setter returns.
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

    fn dropProcessedAsyncPending(self: *ExchangeCore, processed: usize) void {
        if (processed == 0) return;
        if (processed >= self.api.pending_queue.items.len) {
            self.api.pending_queue.clearRetainingCapacity();
            return;
        }

        const PendingEntry = std.meta.Child(@TypeOf(self.api.pending_queue.items));
        const remaining = self.api.pending_queue.items.len - processed;
        std.mem.copyForwards(
            PendingEntry,
            self.api.pending_queue.items[0..remaining],
            self.api.pending_queue.items[processed..],
        );
        self.api.pending_queue.items.len = remaining;
    }
};

fn makeAddUserCommand(uid: i64) cmd.OrderCommand {
    return .{
        .command = .add_user,
        .uid = uid,
        .result_code = .new,
    };
}

fn isCoreGroupBoundaryCommand(command: *const cmd.OrderCommand) bool {
    return switch (command.command) {
        .reset, .persist_state_matching, .grouping_control => true,
        .binary_data_command, .binary_data_query => command.symbol == -1,
        else => false,
    };
}

fn failureStage(failure: anyerror) runtime_mod.FailureStage {
    return switch (failure) {
        error.OrderBookInvariantViolation => .matching,
        error.PositionRecordCorrupted,
        error.PositionSymbolSpecificationMissing,
        error.InvalidMarginReleaseCommand,
        error.UnsupportedMarginReleaseSymbol,
        => .r2,
        error.SequenceOverflow => .runtime,
        else => .execute,
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

fn makeMoveOrderCommand(order_id: i64, uid: i64, symbol: i32, new_price: i64) cmd.OrderCommand {
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

fn makeReduceOrderCommand(order_id: i64, uid: i64, symbol: i32, reduce_size: i64) cmd.OrderCommand {
    return .{
        .command = .reduce_order,
        .order_id = order_id,
        .uid = uid,
        .symbol = symbol,
        .size = reduce_size,
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

fn makeResetCommand() cmd.OrderCommand {
    return .{
        .command = .reset,
        .result_code = .new,
    };
}

fn makeNopCommand() cmd.OrderCommand {
    return .{
        .command = .nop,
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

fn makeGroupingControlCommand(enabled: bool) cmd.OrderCommand {
    return makeGroupingControlModeCommand(0, if (enabled) 1 else 0);
}

fn makeGroupingControlModeCommand(timestamp_ns: i64, mode: i64) cmd.OrderCommand {
    return .{
        .command = .grouping_control,
        .order_id = mode,
        .timestamp = timestamp_ns,
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

fn currentWallClockTimestampNs() u64 {
    return io_context.realTimestampNs();
}

const LegacySnapshotGuardReportSpec = struct {
    pub const code: i32 = 70_451;
    pub const schema_id: [report_registry.schema_id_len]u8 =
        [_]u8{0x45} ** report_registry.schema_id_len;
    pub const schema_version: u32 = 1;
    pub const Query = struct {};
    pub const Section = struct {};
    pub const Result = struct {};
    pub const MatchingView = kernel_mod.MatchingReportView;
    pub const RiskView = kernel_mod.RiskReportView;

    pub fn cloneQuery(
        _: std.mem.Allocator,
        _: *const Query,
    ) report_registry.ResourceError!Query {
        return .{};
    }

    pub fn deinitQuery(_: std.mem.Allocator, query: *Query) void {
        query.* = undefined;
    }

    pub fn encodeQuery(
        _: std.mem.Allocator,
        _: *report_registry.EncodingWriter,
        _: *const Query,
    ) report_registry.EncodingError!void {}

    pub fn decodeQuery(
        _: std.mem.Allocator,
        body: []const u8,
    ) report_registry.QueryConstructionError!Query {
        if (body.len != 0) return error.InvalidQuery;
        return .{};
    }

    pub fn processMatching(
        _: std.mem.Allocator,
        _: *const Query,
        _: report_registry.MatchingContext(MatchingView),
    ) report_registry.HandlerError!?Section {
        return null;
    }

    pub fn processRisk(
        _: std.mem.Allocator,
        _: *const Query,
        _: report_registry.RiskContext(RiskView),
    ) report_registry.HandlerError!?Section {
        return null;
    }

    pub fn encodeSection(
        _: std.mem.Allocator,
        _: *report_registry.EncodingWriter,
        _: *const Section,
    ) report_registry.EncodingError!void {}

    pub fn deinitSection(_: std.mem.Allocator, section: *Section) void {
        section.* = undefined;
    }

    pub fn reduce(
        _: std.mem.Allocator,
        _: *const Query,
        _: []const report_registry.SectionBytes,
    ) report_registry.ReductionError!Result {
        return .{};
    }

    pub fn deinitResult(_: std.mem.Allocator, result: *Result) void {
        result.* = undefined;
    }
};

const LegacySnapshotGuardRegistry =
    report_registry.Registry.fromSpecs(.{LegacySnapshotGuardReportSpec});

fn makeExchangeSpecForSymbol(symbol_id: i32) !common.CoreSymbolSpecification {
    return common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol_id,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 100,
        .quote_scale_k = 10,
        .taker_fee = 2,
        .maker_fee = 1,
    });
}

fn makeRuntimePoisonMarginSpec() !common.CoreSymbolSpecification {
    return common.CoreSymbolSpecification.init(.{
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
}

fn prepareRuntimePoisonScenario(
    core: *ExchangeCore,
    allocator: std.mem.Allocator,
    maker_uid: i64,
    taker_uid: i64,
    maker_order_id: i64,
) !common.CoreSymbolSpecification {
    const spec = try makeRuntimePoisonMarginSpec();
    try std.testing.expect(try core.kernelRef().addSymbol(spec));
    for ([_]i64{ maker_uid, taker_uid }) |uid| {
        var add = try core.addUser(uid);
        defer add.deinit(allocator);
        var deposit = try core.adjustUserBalance(
            uid,
            1,
            spec.quote_currency,
            100_000,
            .adjustment,
        );
        defer deposit.deinit(allocator);
    }
    var maker = try core.placeOrder(
        maker_order_id,
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
    const maker_profile = core.kernelRef().risk_engine.getUserProfile(maker_uid) orelse
        unreachable;
    _ = maker_profile.removePosition(spec.symbol_id) orelse unreachable;
    return spec;
}

fn queryRiskStateHash(core: *ExchangeCore, allocator: std.mem.Allocator) (ReportError || ParityHelperError)!u64 {
    var report = try core.queryReport(.risk_state_hash);
    defer report.deinit(allocator);
    return switch (report) {
        .risk_state_hash => |hash| hash,
        else => error.UnexpectedReportVariant,
    };
}

fn queryDetailedStateHash(core: *ExchangeCore, allocator: std.mem.Allocator) (ReportError || ParityHelperError)!u64 {
    var report = try core.queryReport(.state_hash_detailed);
    defer report.deinit(allocator);
    return switch (report) {
        .state_hash_detailed => |detailed| detailed.aggregate_hash,
        else => error.UnexpectedReportVariant,
    };
}

fn mapValueFromI32I64Map(map: *const std.AutoHashMapUnmanaged(i32, i64), key: i32) i64 {
    return map.get(key) orelse 0;
}

fn expectExchangeCoreStateParity(
    allocator: std.mem.Allocator,
    source: *ExchangeCore,
    restored: *ExchangeCore,
    user_ids: []const i64,
    symbol: i32,
) !void {
    try std.testing.expectEqual(
        try queryRiskStateHash(source, allocator),
        try queryRiskStateHash(restored, allocator),
    );
    try std.testing.expectEqual(
        try queryDetailedStateHash(source, allocator),
        try queryDetailedStateHash(restored, allocator),
    );

    var source_total = try source.queryReport(.total_currency_balance);
    defer source_total.deinit(allocator);
    var restored_total = try restored.queryReport(.total_currency_balance);
    defer restored_total.deinit(allocator);
    switch (source_total) {
        .total_currency_balance => |*lhs| switch (restored_total) {
            .total_currency_balance => |*rhs| {
                try std.testing.expectEqual(
                    mapValueFromI32I64Map(&lhs.account_balances, 11),
                    mapValueFromI32I64Map(&rhs.account_balances, 11),
                );
                try std.testing.expectEqual(
                    mapValueFromI32I64Map(&lhs.account_balances, 15),
                    mapValueFromI32I64Map(&rhs.account_balances, 15),
                );
                try std.testing.expectEqual(
                    mapValueFromI32I64Map(&lhs.orders_balances, 11),
                    mapValueFromI32I64Map(&rhs.orders_balances, 11),
                );
                try std.testing.expectEqual(
                    mapValueFromI32I64Map(&lhs.orders_balances, 15),
                    mapValueFromI32I64Map(&rhs.orders_balances, 15),
                );
                try std.testing.expectEqual(
                    mapValueFromI32I64Map(&lhs.fees, 11),
                    mapValueFromI32I64Map(&rhs.fees, 11),
                );
                try std.testing.expectEqual(
                    mapValueFromI32I64Map(&lhs.fees, 15),
                    mapValueFromI32I64Map(&rhs.fees, 15),
                );
            },
            else => return error.UnexpectedReportVariant,
        },
        else => return error.UnexpectedReportVariant,
    }

    var source_book = try source.queryReport(.{
        .order_book = .{
            .symbol = symbol,
            .depth = 64,
        },
    });
    defer source_book.deinit(allocator);
    var restored_book = try restored.queryReport(.{
        .order_book = .{
            .symbol = symbol,
            .depth = 64,
        },
    });
    defer restored_book.deinit(allocator);
    switch (source_book) {
        .order_book => |*lhs| switch (restored_book) {
            .order_book => |*rhs| {
                try std.testing.expectEqual(lhs.asks.items.len, rhs.asks.items.len);
                try std.testing.expectEqual(lhs.bids.items.len, rhs.bids.items.len);
                const asks_len = lhs.asks.items.len;
                const bids_len = lhs.bids.items.len;
                for (0..asks_len) |idx| {
                    const left_level = lhs.asks.items[idx];
                    const right_level = rhs.asks.items[idx];
                    try std.testing.expectEqual(left_level.price, right_level.price);
                    try std.testing.expectEqual(left_level.volume, right_level.volume);
                    try std.testing.expectEqual(left_level.orders, right_level.orders);
                }
                for (0..bids_len) |idx| {
                    const left_level = lhs.bids.items[idx];
                    const right_level = rhs.bids.items[idx];
                    try std.testing.expectEqual(left_level.price, right_level.price);
                    try std.testing.expectEqual(left_level.volume, right_level.volume);
                    try std.testing.expectEqual(left_level.orders, right_level.orders);
                }
            },
            else => return error.UnexpectedReportVariant,
        },
        else => return error.UnexpectedReportVariant,
    }

    for (user_ids) |uid| {
        var source_user = try source.queryReport(.{ .single_user = uid });
        defer source_user.deinit(allocator);
        var restored_user = try restored.queryReport(.{ .single_user = uid });
        defer restored_user.deinit(allocator);
        switch (source_user) {
            .single_user => |*lhs| switch (restored_user) {
                .single_user => |*rhs| {
                    try std.testing.expectEqual(lhs.query_execution_status, rhs.query_execution_status);
                    if (lhs.query_execution_status == .ok) {
                        try std.testing.expectEqual(
                            mapValueFromI32I64Map(&lhs.accounts, 11),
                            mapValueFromI32I64Map(&rhs.accounts, 11),
                        );
                        try std.testing.expectEqual(
                            mapValueFromI32I64Map(&lhs.accounts, 15),
                            mapValueFromI32I64Map(&rhs.accounts, 15),
                        );
                        try std.testing.expectEqual(lhs.orders.items.len, rhs.orders.items.len);
                    }
                },
                else => return error.UnexpectedReportVariant,
            },
            else => return error.UnexpectedReportVariant,
        }
    }
}

const BinaryAccountSeed = struct {
    uid: i64,
    currency: i32,
    balance: i64,
};

fn encodeAddAccountsBinaryPayload(
    allocator: std.mem.Allocator,
    seeds: []const BinaryAccountSeed,
) binary_payload.EncodeError![]u8 {
    var users = try allocator.alloc(BinaryUserAccountsBatch, seeds.len);
    defer allocator.free(users);
    var accounts = try allocator.alloc(BinaryCurrencyBalance, seeds.len);
    defer allocator.free(accounts);

    for (seeds, 0..) |seed, idx| {
        accounts[idx] = .{
            .currency = seed.currency,
            .balance = seed.balance,
        };
        users[idx] = .{
            .uid = seed.uid,
            .accounts = accounts[idx .. idx + 1],
        };
    }
    return binary_payload.encodeAddAccountsToBytes(allocator, users);
}

fn encodeStateHashReportPayload(allocator: std.mem.Allocator) std.mem.Allocator.Error![]u8 {
    return binary_payload.encodeReportQueryToBytes(allocator, .state_hash);
}

fn installTestJavaIdentityHashContext(core: *ExchangeCore) void {
    core.kernelRef().java_identity_hash_context = .{
        .order_action_ask = 101,
        .order_action_bid = 102,
        .user_status_active = 103,
        .user_status_suspended = 104,
        .risk_symbol_spec_provider = 105,
        .risk_user_profile_service = 106,
        .risk_binary_cmd_processor = 107,
        .risk_last_price_cache = 108,
        .risk_fees = 109,
        .risk_adjustments = 110,
        .risk_suspends = 111,
        .risk_shard_mask = 112,
        .matching_binary_cmd_processor = 113,
        .matching_order_books = 114,
        .matching_shard_mask = 115,
    };
}

fn expectJavaStateHashResponse(response: *const kernel_mod.CommandResponse) !void {
    try std.testing.expect(response.report_response == null);
    const report = response.java_report_response orelse return error.MissingReportResponse;
    switch (report) {
        .state_hash => |state_hash| {
            try std.testing.expect(state_hash.report.entries.len > 0);
        },
        else => return error.UnexpectedReportResponseType,
    }
}

fn expectExactAndNativeReportOverflowIsolation(start_worker: bool) !void {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();

    for ([_]i64{ 6_220_001, 6_220_002 }) |uid| {
        try std.testing.expect(
            try core.kernelRef().risk_engine.user_profile_service.addEmptyUserProfile(uid),
        );
        const profile = core.kernelRef().risk_engine.getUserProfile(uid) orelse unreachable;
        try profile.putAccount(allocator, 15, std.math.maxInt(i64));
    }
    if (start_worker) try core.startup();

    var exact = try core.submitBinaryReportQuery(
        if (start_worker) 62_202 else 62_201,
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
    switch (exact.java_report_response orelse return error.MissingJavaReportResponse) {
        .total_currency_balance => |report| {
            var wrapped_balance: ?i64 = null;
            for (report.account_balances orelse &.{}) |entry| {
                if (entry.key == 15) wrapped_balance = entry.value;
            }
            try std.testing.expectEqual(@as(?i64, -2), wrapped_balance);
        },
        else => return error.UnexpectedJavaReportResponseType,
    }

    var native = try core.submitNativeReportQuery(
        if (start_worker) 62_204 else 62_203,
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

test "exchange core exact Java report is isolated from native overflow cold and worker" {
    try expectExactAndNativeReportOverflowIsolation(false);
    try expectExactAndNativeReportOverflowIsolation(true);
}

test "exchange core native report metadata marks only the terminal carrier frame" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const words = [_]i64{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    const frames = try core.binaryFrameCommandsFromCarrierWordsWithProjection(
        .binary_data_query,
        62_205,
        9_622_000_003,
        &words,
        .exact_java_and_native,
    );
    defer allocator.free(frames);
    try std.testing.expectEqual(@as(usize, 2), frames.len);
    try std.testing.expectEqual(
        cmd.BinaryReportProjection.exact_java_only,
        frames[0].binary_report_projection,
    );
    try std.testing.expectEqual(
        cmd.BinaryReportProjection.exact_java_and_native,
        frames[1].binary_report_projection,
    );
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

    const payload = try allocator.alloc(u8, @max(body.len, minimum_len));
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
        // Native add-accounts body: command/count header plus one uid/count
        // header and one currency/balance tuple per generated user.
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
    start_index: usize,
    frame_count: usize,
    transfer_id: i32,
    command_type: cmd.OrderCommandType,
) !void {
    const entries = journal.entries.items[start_index .. start_index + frame_count];
    for (entries, 0..) |entry, index| {
        try std.testing.expectEqual(command_type, entry.input.command);
        try std.testing.expectEqual(transfer_id, entry.input.user_cookie);
        try std.testing.expectEqual(
            @as(i32, if (index + 1 == frame_count) -1 else 0),
            entry.input.symbol,
        );
        if (index > 0) {
            try std.testing.expectEqual(entries[index - 1].sequence + 1, entry.sequence);
            try std.testing.expect(entry.input.events_group >= entries[index - 1].input.events_group);
        }
    }
    if (frame_count > 1) {
        try std.testing.expect(
            entries[frame_count - 1].input.events_group >
                entries[frame_count - 2].input.events_group,
        );
    }
}

const BinaryStreamCallbackProbe = struct {
    response_count: usize = 0,
    failure_count: usize = 0,
    terminal_symbol: i32 = 0,
    terminal_sequence: u64 = 0,
    terminal_result: cmd.CommandResultCode = .new,
    failure_kind: ?CallbackFailureKind = null,
    failure_cause: ?anyerror = null,

    fn onResponse(context: ?*anyopaque, response: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.response_count += 1;
        self.terminal_symbol = response.command.symbol;
        self.terminal_sequence = response.sequence;
        self.terminal_result = response.result_code;
    }

    fn onFailure(context: ?*anyopaque, failure: CallbackFailure) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.failure_count += 1;
        self.failure_kind = failure.kind;
        self.failure_cause = failure.fatal.cause;
    }
};

const BinaryAsyncPublisher = struct {
    core: *ExchangeCore,
    payload: []const u8,
    transfer_id: i32,
    result: ?(BinaryPayloadAsyncSubmitError!u64) = null,

    fn run(self: *@This()) void {
        self.result = self.core.submitBinaryPayloadAsync(
            .binary_data_query,
            self.transfer_id,
            77,
            self.payload,
        );
    }
};

const BinaryCallbackPublisher = struct {
    core: *ExchangeCore,
    command_type: cmd.OrderCommandType,
    payload: []const u8,
    transfer_id: i32,
    probe: *BinaryStreamCallbackProbe,
    result: ?(BinaryPayloadAsyncSubmitError!void) = null,

    fn run(self: *@This()) void {
        self.result = self.core.submitBinaryPayloadAsyncWithCallback(
            self.command_type,
            self.transfer_id,
            78,
            self.payload,
            .{
                .context = self.probe,
                .callback = BinaryStreamCallbackProbe.onResponse,
                .failure_callback = BinaryStreamCallbackProbe.onFailure,
            },
        );
    }
};

const ReentrantBinaryPublisherProbe = struct {
    core: *ExchangeCore,
    payload: []const u8,
    result: ?(BinaryPayloadAsyncSubmitError!u64) = null,

    fn onResponse(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.result = self.core.submitBinaryPayloadAsync(
            .binary_data_query,
            49_999,
            79,
            self.payload,
        );
    }
};

fn publishRuntimeResult(sink: runtime_mod.ResultSink, event: runtime_mod.ResultEvent) void {
    if (sink.on_result) |callback| {
        callback(sink.context, event);
    }
}

test "exchange core wraps sync api flow and report query path" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer core.deinit();

    var add = try core.addUser(8801);
    defer add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add.result_code);

    var adjust = try core.adjustUserBalance(8801, 1, 15, 5_000, .adjustment);
    defer adjust.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, adjust.result_code);

    var balance = try core.queryReport(.{
        .user_balance = .{
            .uid = 8801,
            .currency = 15,
        },
    });
    defer balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 5_000), balance.user_balance);

    try std.testing.expectError(
        error.OrderBookDepthOverflow,
        core.requestOrderBook(241, std.math.maxInt(usize)),
    );
}

const TypedTimestampRawProbe = struct {
    count: usize = 0,
    commands: [8]cmd.OrderCommand = undefined,

    fn onResult(context: ?*anyopaque, response: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        std.debug.assert(self.count < self.commands.len);
        self.commands[self.count] = response.command;
        self.count += 1;
    }
};

const TypedTimestampCallbackProbe = struct {
    raw_probe: *const TypedTimestampRawProbe,
    count: usize = 0,
    commands: [2]cmd.OrderCommand = undefined,
    raw_counts: [2]usize = undefined,

    fn onResponse(context: ?*anyopaque, response: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        std.debug.assert(self.count < self.commands.len);
        self.commands[self.count] = response.command;
        self.raw_counts[self.count] = self.raw_probe.count;
        self.count += 1;
    }
};

test "exchange core ApiCommand sync dispatch routes core and binary flows" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer core.deinit();
    installTestJavaIdentityHashContext(&core);

    const symbol_id: i32 = 932;
    const spec = try makeExchangeSpecForSymbol(symbol_id);
    var add_symbol = try core.submitApiCommand(.{
        .binary_add_symbols = .{
            .transfer_id = 701,
            .timestamp = 11,
            .symbols = &.{spec},
        },
    });
    defer add_symbol.deinit(allocator);
    switch (add_symbol) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var add_user = try core.submitApiCommand(.{ .add_user = 8_821 });
    defer add_user.deinit(allocator);
    switch (add_user) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var adjust_balance = try core.submitApiCommand(.{
        .adjust_user_balance = .{
            .uid = 8_821,
            .transaction_id = 1,
            .currency = 15,
            .amount = 12_000,
            .adjustment_type = .adjustment,
        },
    });
    defer adjust_balance.deinit(allocator);
    switch (adjust_balance) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var order_book = try core.submitApiCommand(.{
        .request_order_book = .{
            .symbol = symbol_id,
            .depth = 16,
        },
    });
    defer order_book.deinit(allocator);
    switch (order_book) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var report = try core.submitApiCommand(.{
        .binary_report_query = .{
            .transfer_id = 702,
            .timestamp = 12,
            .query = .state_hash,
        },
    });
    defer report.deinit(allocator);
    switch (report) {
        .response => |response| {
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
            try expectJavaStateHashResponse(&response);
        },
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var persist = try core.submitApiCommand(.{
        .persist_state = .{
            .snapshot_id = 88001,
            .timestamp_ns = 777_888_999,
        },
    });
    defer persist.deinit(allocator);
    switch (persist) {
        .persist_state => |result_code| try std.testing.expectEqual(
            cmd.CommandResultCode.state_persist_matching_engine_failed,
            result_code,
        ),
        .response => return error.UnexpectedResponseResult,
    }

    var grouping = try core.submitApiCommand(.{
        .grouping_control = .{
            .timestamp_ns = 14,
            .mode = 1,
        },
    });
    defer grouping.deinit(allocator);
    switch (grouping) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    try std.testing.expectError(
        error.OrderBookDepthOverflow,
        core.submitApiCommand(.{
            .request_order_book = .{
                .symbol = symbol_id,
                .depth = std.math.maxInt(usize),
            },
        }),
    );
}

test "exchange core ApiCommand raw command sync path preserves metadata and journal input" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
        },
    });
    defer core.deinit();

    var result = try core.submitApiCommand(.{
        .raw_command = .{
            .command = .add_user,
            .uid = 8_915,
            .timestamp = 444_555_666,
            .events_group = 77,
            .service_flags = 5,
            .user_cookie = 19,
            .result_code = .new,
        },
    });
    defer result.deinit(allocator);

    switch (result) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    const journal = core.journal orelse return error.ExpectedJournal;
    try std.testing.expectEqual(@as(usize, 1), journal.entries.items.len);
    const entry = journal.entries.items[0];
    try std.testing.expectEqual(cmd.OrderCommandType.add_user, entry.input.command);
    try std.testing.expectEqual(@as(i64, 8_915), entry.input.uid);
    try std.testing.expectEqual(@as(i64, 444_555_666), entry.input.timestamp);
    try std.testing.expectEqual(@as(i64, 77), entry.input.events_group);
    try std.testing.expectEqual(@as(i32, 5), entry.input.service_flags);
    try std.testing.expectEqual(@as(i32, 19), entry.input.user_cookie);
}

test "exchange core ApiCommand grouping-control sync path keeps mode and timestamp metadata" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
        },
    });
    defer core.deinit();

    var result = try core.submitApiCommand(.{
        .grouping_control = .{
            .timestamp_ns = 222_333_444,
            .mode = 0,
        },
    });
    defer result.deinit(allocator);

    switch (result) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    const journal = core.journal orelse return error.ExpectedJournal;
    try std.testing.expectEqual(@as(usize, 1), journal.entries.items.len);
    const entry = journal.entries.items[0];
    try std.testing.expectEqual(cmd.OrderCommandType.grouping_control, entry.input.command);
    try std.testing.expectEqual(@as(i64, 0), entry.input.order_id);
    try std.testing.expectEqual(@as(i64, 222_333_444), entry.input.timestamp);
}

test "exchange core ApiCommand async dispatch routes requests and resolves results" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();
    installTestJavaIdentityHashContext(&core);

    const add_req = try core.submitApiCommandAsync(.{ .add_user = 8_911 });
    const adjust_req = try core.submitApiCommandAsync(.{
        .adjust_user_balance = .{
            .uid = 8_911,
            .transaction_id = 11,
            .currency = 15,
            .amount = 19_000,
            .adjustment_type = .adjustment,
        },
    });
    const query_req = try core.submitApiCommandAsync(.{
        .binary_report_query = .{
            .transfer_id = 703,
            .timestamp = 13,
            .query = .state_hash,
        },
    });
    const grouping_req = try core.submitApiCommandAsync(.{
        .grouping_control = .{
            .timestamp_ns = 14,
            .mode = 0,
        },
    });

    const processed = try core.drainAsyncPending();
    try std.testing.expectEqual(@as(usize, 4), processed);

    var add_result = core.takeApiCommandAsyncResult(add_req) orelse return error.MissingAsyncResult;
    defer add_result.deinit(allocator);
    switch (add_result) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var adjust_result = core.takeApiCommandAsyncResult(adjust_req) orelse return error.MissingAsyncResult;
    defer adjust_result.deinit(allocator);
    switch (adjust_result) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var query_result = core.takeApiCommandAsyncResult(query_req) orelse return error.MissingAsyncResult;
    defer query_result.deinit(allocator);
    switch (query_result) {
        .response => |response| {
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
            try expectJavaStateHashResponse(&response);
        },
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var grouping_result = core.takeApiCommandAsyncResult(grouping_req) orelse return error.MissingAsyncResult;
    defer grouping_result.deinit(allocator);
    switch (grouping_result) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }
}

test "exchange core ApiCommand async raw command path enqueues and resolves metadata-bearing command" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const handle = try core.submitApiCommandAsync(.{
        .raw_command = .{
            .command = .add_user,
            .uid = 8_916,
            .timestamp = 555_666_777,
            .events_group = 88,
            .service_flags = 7,
            .result_code = .new,
        },
    });

    try std.testing.expect(!core.isApiCommandAsyncResultReady(handle));
    const processed = try core.drainAsyncPending();
    try std.testing.expectEqual(@as(usize, 1), processed);
    try std.testing.expect(core.isApiCommandAsyncResultReady(handle));

    var result = core.takeApiCommandAsyncResult(handle) orelse return error.MissingAsyncResult;
    defer result.deinit(allocator);
    switch (result) {
        .response => |response| try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code),
        .persist_state => return error.UnexpectedPersistStateResult,
    }
}

test "exchange core ApiCommand async dispatch supports persist_state handle readiness and take" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const handle = try core.submitApiCommandAsync(.{
        .persist_state = .{
            .snapshot_id = 99001,
            .timestamp_ns = 1_111_222_333,
        },
    });
    switch (handle) {
        .persist_state => {},
        .request_id => return error.UnexpectedRequestHandle,
    }

    try std.testing.expect(!core.isApiCommandAsyncResultReady(handle));
    const processed = try core.drainAsyncPending();
    try std.testing.expectEqual(@as(usize, 2), processed);
    try std.testing.expect(core.isApiCommandAsyncResultReady(handle));

    var result = core.takeApiCommandAsyncResult(handle) orelse return error.MissingAsyncResult;
    defer result.deinit(allocator);
    switch (result) {
        .persist_state => |result_code| try std.testing.expectEqual(
            cmd.CommandResultCode.state_persist_matching_engine_failed,
            result_code,
        ),
        .response => return error.UnexpectedResponseResult,
    }

    try std.testing.expect(!core.isApiCommandAsyncResultReady(handle));
    try std.testing.expect(core.takeApiCommandAsyncResult(handle) == null);
}

test "exchange core ApiCommand batch helper runs mixed command stream and returns final response result" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();
    installTestJavaIdentityHashContext(&core);

    const commands = [_]ApiCommand{
        .{ .add_user = 9_201 },
        .{
            .adjust_user_balance = .{
                .uid = 9_201,
                .transaction_id = 901,
                .currency = 15,
                .amount = 22_000,
                .adjustment_type = .adjustment,
            },
        },
        .{
            .binary_report_query = .{
                .transfer_id = 811,
                .timestamp = 14,
                .query = .state_hash,
            },
        },
    };

    var result = try core.submitApiCommandsSync(commands[0..]);
    defer result.deinit(allocator);

    switch (result) {
        .response => |response| {
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
            try expectJavaStateHashResponse(&response);
        },
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var balance = try core.queryReport(.{
        .user_balance = .{
            .uid = 9_201,
            .currency = 15,
        },
    });
    defer balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 22_000), balance.user_balance);
}

test "exchange core ApiCommand batch helper supports persist final result and empty batch rejection" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const commands = [_]ApiCommand{
        .{ .add_user = 9_301 },
        .{
            .persist_state = .{
                .snapshot_id = 77_701,
                .timestamp_ns = 4_444_555_666,
            },
        },
    };

    var result = try core.submitApiCommandsSync(commands[0..]);
    defer result.deinit(allocator);
    switch (result) {
        .persist_state => |result_code| try std.testing.expectEqual(
            cmd.CommandResultCode.state_persist_matching_engine_failed,
            result_code,
        ),
        .response => return error.UnexpectedResponseResult,
    }

    const empty: [0]ApiCommand = .{};
    try std.testing.expectError(error.EmptyApiCommandBatch, core.submitApiCommandsSync(empty[0..]));
}

test "exchange core typed timestamps preserve sync async batch callback order and fallback" {
    const allocator = std.testing.allocator;
    const symbol: i32 = 9_331;
    const uid: i64 = 88_331;
    const balance_timestamp: i64 = 7_100_001;
    const place_timestamp: i64 = 7_100_002;
    const move_timestamp: i64 = 7_100_003;
    const cancel_timestamp: i64 = 7_100_004;
    const callback_timestamp: i64 = 7_100_005;

    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .matching = .{ .orderbook_kind = .direct },
    });
    defer core.deinit();
    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(symbol)));

    var add_user = try core.addUser(uid);
    defer add_user.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_user.result_code);

    var raw_probe: TypedTimestampRawProbe = .{};
    core.setRawResultSink(.{
        .context = &raw_probe,
        .on_result = TypedTimestampRawProbe.onResult,
    });

    var balance = try core.submitApiCommand(.{
        .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 1,
            .currency = 15,
            .amount = 100_000,
            .adjustment_type = .adjustment,
            .timestamp = balance_timestamp,
        },
    });
    defer balance.deinit(allocator);
    switch (balance) {
        .response => |response| {
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
            try std.testing.expectEqual(balance_timestamp, response.command.timestamp);
        },
        .persist_state => return error.UnexpectedPersistStateResult,
    }
    try std.testing.expectEqual(@as(usize, 1), raw_probe.count);

    const place_handle = try core.submitApiCommandAsync(.{
        .place_order = .{
            .order_id = 91_331,
            .uid = uid,
            .symbol = symbol,
            .price = 101,
            .reserve_bid_price = 101,
            .size = 5,
            .action = .bid,
            .order_type = .gtc,
            .user_cookie = 17,
            .timestamp = place_timestamp,
        },
    });
    try std.testing.expectEqual(@as(usize, 1), raw_probe.count);
    try std.testing.expectEqual(@as(usize, 1), try core.drainAsyncPending());
    try std.testing.expectEqual(@as(usize, 2), raw_probe.count);
    var place = core.takeApiCommandAsyncResult(place_handle) orelse
        return error.MissingAsyncResult;
    defer place.deinit(allocator);
    switch (place) {
        .response => |response| {
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
            try std.testing.expectEqual(place_timestamp, response.command.timestamp);
        },
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    const batch = [_]ApiCommand{
        .{ .move_order = .{
            .order_id = 91_331,
            .uid = uid,
            .symbol = symbol,
            .new_price = 100,
            .timestamp = move_timestamp,
        } },
        .{ .cancel_order = .{
            .order_id = 91_331,
            .uid = uid,
            .symbol = symbol,
            .timestamp = cancel_timestamp,
        } },
    };
    var batch_result = try core.submitApiCommandsSync(&batch);
    defer batch_result.deinit(allocator);
    switch (batch_result) {
        .response => |response| {
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
            try std.testing.expectEqual(cancel_timestamp, response.command.timestamp);
        },
        .persist_state => return error.UnexpectedPersistStateResult,
    }
    try std.testing.expectEqual(@as(usize, 4), raw_probe.count);

    var callback_probe: TypedTimestampCallbackProbe = .{ .raw_probe = &raw_probe };
    try core.submitApiCommandWithCallback(.{
        .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 2,
            .currency = 15,
            .amount = 1,
            .adjustment_type = .adjustment,
            .timestamp = callback_timestamp,
        },
    }, .{
        .context = &callback_probe,
        .callback = TypedTimestampCallbackProbe.onResponse,
    });
    try std.testing.expectEqual(@as(usize, 4), raw_probe.count);
    try std.testing.expectEqual(@as(usize, 0), callback_probe.count);
    try std.testing.expectEqual(@as(usize, 1), try core.drainAsyncPending());
    try std.testing.expectEqual(@as(usize, 5), raw_probe.count);
    try std.testing.expectEqual(@as(usize, 1), callback_probe.count);
    try std.testing.expectEqual(callback_timestamp, callback_probe.commands[0].timestamp);
    try std.testing.expectEqual(@as(usize, 5), callback_probe.raw_counts[0]);

    try core.submitApiCommandWithCallback(.{
        .request_order_book = .{ .symbol = symbol, .depth = 4 },
    }, .{
        .context = &callback_probe,
        .callback = TypedTimestampCallbackProbe.onResponse,
    });
    try std.testing.expectEqual(@as(usize, 1), try core.drainAsyncPending());
    try std.testing.expectEqual(@as(usize, 6), raw_probe.count);
    try std.testing.expectEqual(@as(usize, 2), callback_probe.count);
    try std.testing.expect(callback_probe.commands[1].timestamp > 0);
    try std.testing.expectEqual(
        callback_probe.commands[1].timestamp,
        raw_probe.commands[5].timestamp,
    );
    try std.testing.expectEqual(@as(usize, 6), callback_probe.raw_counts[1]);

    const expected_types = [_]cmd.OrderCommandType{
        .balance_adjustment,
        .place_order,
        .move_order,
        .cancel_order,
        .balance_adjustment,
    };
    const expected_timestamps = [_]i64{
        balance_timestamp,
        place_timestamp,
        move_timestamp,
        cancel_timestamp,
        callback_timestamp,
    };
    for (expected_types, expected_timestamps, 0..) |command_type, timestamp, index| {
        try std.testing.expectEqual(command_type, raw_probe.commands[index].command);
        try std.testing.expectEqual(timestamp, raw_probe.commands[index].timestamp);
    }
}

test "exchange core scalar and unit timestamp carriers survive every publication path" {
    const allocator = std.testing.allocator;
    const uid: i64 = 88_334;
    const timestamps = [_]i64{
        8_300_001,
        8_300_002,
        8_300_003,
        8_300_004,
        8_300_005,
        8_300_006,
    };

    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
        },
    });
    defer core.deinit();

    var raw_probe: TypedTimestampRawProbe = .{};
    core.setRawResultSink(.{
        .context = &raw_probe,
        .on_result = TypedTimestampRawProbe.onResult,
    });

    var add = try core.submitApiCommand(.{
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

    try core.submitApiCommandDetached(.{
        .suspend_user_with_timestamp = .{
            .uid = uid,
            .timestamp = timestamps[1],
        },
    });
    const resume_handle = try core.submitApiCommandAsync(.{
        .resume_user_with_timestamp = .{
            .uid = uid,
            .timestamp = timestamps[2],
        },
    });
    var batch = try core.submitApiCommandsSync(&.{
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

    var resume_result = core.takeApiCommandAsyncResult(resume_handle) orelse
        return error.MissingAsyncResult;
    defer resume_result.deinit(allocator);
    switch (resume_result) {
        .response => |response| try std.testing.expectEqual(
            timestamps[2],
            response.command.timestamp,
        ),
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var callback_probe: TypedTimestampCallbackProbe = .{ .raw_probe = &raw_probe };
    const callback: CommandResponseCallback = .{
        .context = &callback_probe,
        .callback = TypedTimestampCallbackProbe.onResponse,
    };
    try core.submitApiCommandWithCallback(.{
        .add_user_with_timestamp = .{
            .uid = uid + 1,
            .timestamp = timestamps[5],
        },
    }, callback);
    try std.testing.expectEqual(@as(usize, 1), try core.drainAsyncPending());
    try std.testing.expectEqual(@as(usize, 1), callback_probe.count);
    try std.testing.expectEqual(timestamps[5], callback_probe.commands[0].timestamp);
    try std.testing.expectEqual(@as(i64, -1), callback_probe.commands[0].order_id);
    try std.testing.expectEqual(@as(i32, -1), callback_probe.commands[0].symbol);

    try std.testing.expect(api_command_mod.supportsAsyncFullResponse(.{
        .reset_with_timestamp = .{ .timestamp = timestamps[3] },
    }));
    try std.testing.expect(api_command_mod.supportsAsyncFullResponse(.{
        .nop_with_timestamp = .{ .timestamp = timestamps[4] },
    }));
    try std.testing.expectError(
        error.UnsupportedCallbackCommand,
        core.submitApiCommandWithCallback(.{
            .reset_with_timestamp = .{ .timestamp = timestamps[3] },
        }, callback),
    );
    try std.testing.expectError(
        error.UnsupportedCallbackCommand,
        core.submitApiCommandWithCallback(.{
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
    try std.testing.expectEqual(expected_types.len, raw_probe.count);
    for (expected_types, timestamps, 0..) |command_type, timestamp, index| {
        try std.testing.expectEqual(command_type, raw_probe.commands[index].command);
        try std.testing.expectEqual(timestamp, raw_probe.commands[index].timestamp);
    }

    const journal = core.journal orelse return error.ExpectedJournal;
    try std.testing.expectEqual(expected_types.len, journal.entries.items.len);
    for (journal.entries.items, expected_types, timestamps) |entry, command_type, timestamp| {
        try std.testing.expectEqual(command_type, entry.input.command);
        try std.testing.expectEqual(timestamp, entry.input.timestamp);
    }
}

test "exchange core typed sync and async omitted timestamps remain zero" {
    const allocator = std.testing.allocator;
    const symbol: i32 = 9_332;
    const uid: i64 = 88_332;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .matching = .{ .orderbook_kind = .direct },
    });
    defer core.deinit();
    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(symbol)));

    var add_user = try core.addUser(uid);
    defer add_user.deinit(allocator);

    var balance = try core.submitApiCommand(.{
        .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 1,
            .currency = 15,
            .amount = 100_000,
            .adjustment_type = .adjustment,
        },
    });
    defer balance.deinit(allocator);
    switch (balance) {
        .response => |response| {
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
            try std.testing.expectEqual(@as(i64, 0), response.command.timestamp);
        },
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    const place_handle = try core.submitApiCommandAsync(.{
        .place_order = .{
            .order_id = 91_332,
            .uid = uid,
            .symbol = symbol,
            .price = 101,
            .reserve_bid_price = 101,
            .size = 5,
            .action = .bid,
            .order_type = .gtc,
            .user_cookie = 0,
        },
    });
    try std.testing.expectEqual(@as(usize, 1), try core.drainAsyncPending());
    var place = core.takeApiCommandAsyncResult(place_handle) orelse
        return error.MissingAsyncResult;
    defer place.deinit(allocator);
    switch (place) {
        .response => |response| {
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
            try std.testing.expectEqual(@as(i64, 0), response.command.timestamp);
        },
        .persist_state => return error.UnexpectedPersistStateResult,
    }
}

test "exchange core live typed GTC reports retain placement timestamp after move" {
    const allocator = std.testing.allocator;
    const symbol: i32 = 9_333;
    const uid: i64 = 88_333;
    const order_id: i64 = 91_333;
    const placement_timestamp: i64 = 8_200_001;
    const move_timestamp: i64 = 8_200_002;

    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .matching = .{ .orderbook_kind = .direct },
    });
    defer core.deinit();
    installTestJavaIdentityHashContext(&core);
    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(symbol)));
    try core.startup();

    var add_user = try core.submitApiCommand(.{ .add_user = uid });
    defer add_user.deinit(allocator);
    var balance = try core.submitApiCommand(.{
        .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 1,
            .currency = 15,
            .amount = 100_000,
            .adjustment_type = .adjustment,
        },
    });
    defer balance.deinit(allocator);

    var place = try core.submitApiCommand(.{
        .place_order = .{
            .order_id = order_id,
            .uid = uid,
            .symbol = symbol,
            .price = 101,
            .reserve_bid_price = 101,
            .size = 5,
            .action = .bid,
            .order_type = .gtc,
            .user_cookie = 23,
            .timestamp = placement_timestamp,
        },
    });
    defer place.deinit(allocator);
    switch (place) {
        .response => |response| {
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
            try std.testing.expectEqual(placement_timestamp, response.command.timestamp);
        },
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var move = try core.submitApiCommand(.{
        .move_order = .{
            .order_id = order_id,
            .uid = uid,
            .symbol = symbol,
            .new_price = 100,
            .timestamp = move_timestamp,
        },
    });
    defer move.deinit(allocator);
    switch (move) {
        .response => |response| {
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
            try std.testing.expectEqual(move_timestamp, response.command.timestamp);
        },
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var native = try core.queryReport(.{ .single_user = uid });
    defer native.deinit(allocator);
    switch (native) {
        .single_user => |report| {
            try std.testing.expectEqual(@as(usize, 1), report.orders.items.len);
            try std.testing.expectEqual(order_id, report.orders.items[0].order_id);
            try std.testing.expectEqual(@as(i64, 100), report.orders.items[0].price);
            try std.testing.expectEqual(
                placement_timestamp,
                report.orders.items[0].timestamp,
            );
        },
        else => return error.UnexpectedReportVariant,
    }

    var exact = try core.submitBinaryReportQuery(
        7_333,
        8_200_003,
        .{ .single_user = uid },
    );
    defer exact.deinit(allocator);
    switch (exact.java_report_response orelse return error.MissingJavaReportResponse) {
        .single_user => |report| {
            const groups = report.orders orelse return error.MissingJavaOrders;
            try std.testing.expectEqual(@as(usize, 1), groups.len);
            try std.testing.expectEqual(symbol, groups[0].symbol);
            try std.testing.expectEqual(@as(usize, 1), groups[0].orders.len);
            try std.testing.expectEqual(order_id, groups[0].orders[0].order_id);
            try std.testing.expectEqual(@as(i64, 100), groups[0].orders[0].price);
            try std.testing.expectEqual(
                placement_timestamp,
                groups[0].orders[0].timestamp,
            );
        },
        else => return error.UnexpectedReportResponseType,
    }
}

fn expectBoundedApiCommandBatch(start_worker: bool) !void {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .runtime = .{ .queue_capacity = 4, .max_batch_size = 1, .wait_strategy = .blocking },
    });
    defer core.deinit();
    if (start_worker) try core.startup();

    const uid: i64 = if (start_worker) 9_312 else 9_311;
    const commands = [_]ApiCommand{
        .{ .add_user = uid },
        .{ .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 1,
            .currency = 15,
            .amount = 10,
            .adjustment_type = .adjustment,
        } },
        .{ .suspend_user = uid },
        .{ .resume_user = uid },
        .{ .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 2,
            .currency = 15,
            .amount = 20,
            .adjustment_type = .adjustment,
        } },
        .{ .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 3,
            .currency = 15,
            .amount = 30,
            .adjustment_type = .adjustment,
        } },
        .{ .add_user = uid },
    };

    var result = try core.submitApiCommandsSync(commands[0..]);
    defer result.deinit(allocator);
    switch (result) {
        .response => |response| {
            try std.testing.expectEqual(@as(u64, commands.len), response.sequence);
            try std.testing.expectEqual(
                cmd.CommandResultCode.user_mgmt_user_already_exists,
                response.result_code,
            );
        },
        .persist_state => return error.UnexpectedPersistStateResult,
    }

    var balance = try core.queryReport(.{ .user_balance = .{ .uid = uid, .currency = 15 } });
    defer balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 60), balance.user_balance);
    try std.testing.expectEqual(@as(usize, 0), core.asyncReadyCount());
    try std.testing.expectEqual(@as(usize, 0), core.driver.completion_slots_used);
}

test "exchange core ApiCommand batch larger than completion capacity stays ordered cold and running" {
    try expectBoundedApiCommandBatch(false);
    try expectBoundedApiCommandBatch(true);
}

test "exchange core ApiCommand flush helper accepts empty stream and returns nop response" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const empty: [0]ApiCommand = .{};
    var response = try core.submitApiCommandsSyncWithNop(empty[0..]);
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
}

test "exchange core ApiCommand flush helper processes mixed stream with persist and returns nop barrier" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const commands = [_]ApiCommand{
        .{ .add_user = 9_401 },
        .{
            .adjust_user_balance = .{
                .uid = 9_401,
                .transaction_id = 1_001,
                .currency = 15,
                .amount = 31_000,
                .adjustment_type = .adjustment,
            },
        },
        .{
            .persist_state = .{
                .snapshot_id = 78_801,
                .timestamp_ns = 5_555_666_777,
            },
        },
    };

    var response = try core.submitApiCommandsSyncWithNop(commands[0..]);
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);

    var balance = try core.queryReport(.{
        .user_balance = .{
            .uid = 9_401,
            .currency = 15,
        },
    });
    defer balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 31_000), balance.user_balance);
}

fn expectBoundedApiCommandFlush(start_worker: bool) !void {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .runtime = .{ .queue_capacity = 4, .max_batch_size = 1, .wait_strategy = .blocking },
    });
    defer core.deinit();
    if (start_worker) try core.startup();

    const uid: i64 = if (start_worker) 9_422 else 9_421;
    const commands = [_]ApiCommand{
        .{ .add_user = uid },
        .{ .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 1,
            .currency = 15,
            .amount = 5,
            .adjustment_type = .adjustment,
        } },
        .{ .suspend_user = uid },
        .{ .resume_user = uid },
        .{ .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 2,
            .currency = 15,
            .amount = 7,
            .adjustment_type = .adjustment,
        } },
        .{ .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 3,
            .currency = 15,
            .amount = 11,
            .adjustment_type = .adjustment,
        } },
        .{ .adjust_user_balance = .{
            .uid = uid,
            .transaction_id = 4,
            .currency = 15,
            .amount = 13,
            .adjustment_type = .adjustment,
        } },
    };

    var response = try core.submitApiCommandsSyncWithNop(commands[0..]);
    defer response.deinit(allocator);
    try std.testing.expectEqual(@as(u64, commands.len + 1), response.sequence);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);

    var balance = try core.queryReport(.{ .user_balance = .{ .uid = uid, .currency = 15 } });
    defer balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 36), balance.user_balance);
    try std.testing.expectEqual(@as(usize, 0), core.asyncReadyCount());
    try std.testing.expectEqual(@as(usize, 0), core.driver.completion_slots_used);
}

test "exchange core ApiCommand flush larger than completion capacity stays ordered cold and running" {
    try expectBoundedApiCommandFlush(false);
    try expectBoundedApiCommandFlush(true);
}

test "exchange core suspend resume and persist sync wrappers are wired" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    var add = try core.addUser(8851);
    defer add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add.result_code);

    var suspend_resp = try core.suspendUser(8851);
    defer suspend_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, suspend_resp.result_code);

    var suspend_again = try core.suspendUser(8851);
    defer suspend_again.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_not_found, suspend_again.result_code);

    var resume_resp = try core.resumeUser(8851);
    defer resume_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, resume_resp.result_code);

    const persist_result = try core.persistState(8001, 111_222_333);
    try std.testing.expectEqual(cmd.CommandResultCode.state_persist_matching_engine_failed, persist_result);
}

test "exchange core order sync wrappers run command lifecycle with reset and nop" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer core.deinit();

    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(241)));

    var add = try core.addUser(8901);
    defer add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add.result_code);

    var dep_quote = try core.adjustUserBalance(8901, 1, 15, 100_000, .adjustment);
    defer dep_quote.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, dep_quote.result_code);

    var placed = try core.placeOrder(98101, 8901, 241, 101, 101, 5, .bid, .gtc, 17);
    defer placed.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, placed.result_code);

    var moved = try core.moveOrder(98101, 8901, 241, 100);
    defer moved.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, moved.result_code);

    var reduced = try core.reduceOrder(98101, 8901, 241, 2);
    defer reduced.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, reduced.result_code);

    var canceled = try core.cancelOrder(98101, 8901, 241);
    defer canceled.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, canceled.result_code);

    var nop_resp = try core.nop();
    defer nop_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, nop_resp.result_code);

    var reset_resp = try core.reset();
    defer reset_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, reset_resp.result_code);

    var add_after_reset = try core.addUser(8901);
    defer add_after_reset.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_after_reset.result_code);
}

test "exchange core order async wrappers enqueue and execute deterministically" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer core.deinit();

    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(241)));

    const req_add = try core.addUserAsync(8911);
    const req_dep = try core.adjustUserBalanceAsync(8911, 1, 15, 100_000, .adjustment);
    const req_place = try core.placeOrderAsync(99101, 8911, 241, 101, 101, 5, .bid, .gtc, 19);
    const req_move = try core.moveOrderAsync(99101, 8911, 241, 100);
    const req_reduce = try core.reduceOrderAsync(99101, 8911, 241, 2);
    const req_cancel = try core.cancelOrderAsync(99101, 8911, 241);
    const req_nop = try core.nopAsync();
    const req_reset = try core.resetAsync();

    const processed = try core.drainAsyncPending();
    try std.testing.expectEqual(@as(usize, 8), processed);

    var add = core.takeAsyncResult(req_add) orelse unreachable;
    defer add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add.result_code);

    var dep = core.takeAsyncResult(req_dep) orelse unreachable;
    defer dep.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, dep.result_code);

    var place = core.takeAsyncResult(req_place) orelse unreachable;
    defer place.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, place.result_code);

    var move = core.takeAsyncResult(req_move) orelse unreachable;
    defer move.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, move.result_code);

    var reduce = core.takeAsyncResult(req_reduce) orelse unreachable;
    defer reduce.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, reduce.result_code);

    var cancel = core.takeAsyncResult(req_cancel) orelse unreachable;
    defer cancel.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, cancel.result_code);

    var nop_resp = core.takeAsyncResult(req_nop) orelse unreachable;
    defer nop_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, nop_resp.result_code);

    var reset_resp = core.takeAsyncResult(req_reset) orelse unreachable;
    defer reset_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, reset_resp.result_code);
}

test "exchange core binary command payload wrapper applies multi-frame add-accounts batch" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const seeds = [_]BinaryAccountSeed{
        .{ .uid = 8971, .currency = 15, .balance = 7_000 },
        .{ .uid = 8972, .currency = 11, .balance = 9_000 },
    };
    const payload = try encodeAddAccountsBinaryPayload(allocator, seeds[0..]);
    defer allocator.free(payload);
    try std.testing.expect(payload.len > binary_frames.FrameWordsPerCommand * @sizeOf(i64));

    var response = try core.submitBinaryPayload(
        .binary_data_command,
        501,
        1_234,
        payload,
    );
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);

    var user_1_balance = try core.queryReport(.{
        .user_balance = .{
            .uid = 8971,
            .currency = 15,
        },
    });
    defer user_1_balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 7_000), user_1_balance.user_balance);

    var user_2_balance = try core.queryReport(.{
        .user_balance = .{
            .uid = 8972,
            .currency = 11,
        },
    });
    defer user_2_balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 9_000), user_2_balance.user_balance);
}

test "exchange core binary query payload wrapper returns state-hash detailed report" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();
    installTestJavaIdentityHashContext(&core);

    const payload = try encodeStateHashReportPayload(allocator);
    defer allocator.free(payload);

    var response = try core.submitBinaryPayload(
        .binary_data_query,
        601,
        2_468,
        payload,
    );
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);

    try expectJavaStateHashResponse(&response);
}

test "exchange core typed binary add-symbols wrapper adds symbol and serves order-book report" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const symbol_id: i32 = 777;
    const spec = try makeExchangeSpecForSymbol(symbol_id);

    var response = try core.submitAddSymbolsBinary(
        610,
        2_469,
        &.{spec},
    );
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);

    var order_book = try core.requestOrderBook(symbol_id, 16);
    defer order_book.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, order_book.result_code);
}

test "exchange core typed binary add-accounts wrapper applies balances" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const accounts_1 = [_]BinaryCurrencyBalance{
        .{ .currency = 15, .balance = 13_000 },
    };
    const accounts_2 = [_]BinaryCurrencyBalance{
        .{ .currency = 11, .balance = 14_000 },
    };
    const users = [_]BinaryUserAccountsBatch{
        .{ .uid = 9171, .accounts = accounts_1[0..] },
        .{ .uid = 9172, .accounts = accounts_2[0..] },
    };

    var response = try core.submitAddAccountsBinary(
        611,
        2_470,
        users[0..],
    );
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);

    var user_1_balance = try core.queryReport(.{
        .user_balance = .{
            .uid = 9171,
            .currency = 15,
        },
    });
    defer user_1_balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 13_000), user_1_balance.user_balance);

    var user_2_balance = try core.queryReport(.{
        .user_balance = .{
            .uid = 9172,
            .currency = 11,
        },
    });
    defer user_2_balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 14_000), user_2_balance.user_balance);
}

test "exchange core typed binary helpers publish Java Chronicle request carriers" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var core = try ExchangeCore.initWithPersistenceRoot(allocator, tmp.dir, .{
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = "unused-wire-test-storage",
        },
    });
    defer core.deinit();
    installTestJavaIdentityHashContext(&core);

    const spec = try makeExchangeSpecForSymbol(778);
    var symbols_response = try core.submitAddSymbolsBinary(612, 2_471, &.{spec});
    defer symbols_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, symbols_response.result_code);

    const balances = [_]BinaryCurrencyBalance{
        .{ .currency = 11, .balance = 500 },
    };
    const users = [_]BinaryUserAccountsBatch{
        .{ .uid = 7, .accounts = &balances },
    };
    var accounts_response = try core.submitAddAccountsBinary(613, 2_472, &users);
    defer accounts_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, accounts_response.result_code);

    var query_response = try core.submitBinaryReportQuery(614, 2_473, .state_hash);
    defer query_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, query_response.result_code);

    const entries = core.journal.?.entries.items;
    try std.testing.expectEqual(@as(usize, 4), entries.len);
    try std.testing.expectEqual(@as(i32, 612), entries[0].input.user_cookie);
    try std.testing.expectEqual(@as(i32, 0), entries[0].input.symbol);
    try std.testing.expectEqual(@as(i32, -1), entries[1].input.symbol);
    try std.testing.expectEqual(@as(i32, 613), entries[2].input.user_cookie);
    try std.testing.expectEqual(@as(i32, 614), entries[3].input.user_cookie);

    const symbol_words = [_]i64{
        entries[0].input.order_id,
        entries[0].input.price,
        entries[0].input.reserve_bid_price,
        entries[0].input.size,
        entries[0].input.uid,
        entries[1].input.order_id,
        entries[1].input.price,
        entries[1].input.reserve_bid_price,
        entries[1].input.size,
        entries[1].input.uid,
    };
    var decoded_symbols = try java_binary_wire.decodeRequestWords(allocator, &symbol_words, .{});
    defer decoded_symbols.deinit(allocator);
    const expected_symbols_body = try binary_payload.encodeJavaAddSymbolsToBytes(allocator, &.{spec});
    defer allocator.free(expected_symbols_body);
    try std.testing.expectEqualSlices(u8, expected_symbols_body, decoded_symbols.body);

    const account_words = [_]i64{
        entries[2].input.order_id,
        entries[2].input.price,
        entries[2].input.reserve_bid_price,
        entries[2].input.size,
        entries[2].input.uid,
    };
    // Literal-only raw LZ4 is deterministic and Java-decompressor compatible.
    // The lz4-java fixture below compresses the same body differently (A2),
    // so exact parity is asserted on the uncompressed Chronicle body.
    const expected_literal_words = [_]i64{
        0x0000_0020_F011_EA03,
        0x0000_0100_0000_0700,
        0x0000_0000_0000_0100,
        0x0000_0B00_0000_F401,
        0,
    };
    try std.testing.expectEqualSlices(i64, &expected_literal_words, &account_words);
    const expected_accounts_body = try binary_payload.encodeJavaAddAccountsToBytes(allocator, &users);
    defer allocator.free(expected_accounts_body);
    var decoded_outbound = try java_binary_wire.decodeRequestWords(allocator, &account_words, .{});
    defer decoded_outbound.deinit(allocator);
    try std.testing.expectEqualSlices(u8, expected_accounts_body, decoded_outbound.body);

    const fresh_lz4_java_words = [_]i64{
        0x0000_0020_A2EA_0300,
        0x0001_0000_0007_0001,
        0x0000_0C00_C00B_0000,
        0x00F4_0100_0000_0000,
        0,
    };
    var decoded_java_fixture = try java_binary_wire.decodeRequestWords(
        allocator,
        &fresh_lz4_java_words,
        .{},
    );
    defer decoded_java_fixture.deinit(allocator);
    try std.testing.expectEqualSlices(u8, expected_accounts_body, decoded_java_fixture.body);

    const query_words = [_]i64{
        entries[3].input.order_id,
        entries[3].input.price,
        entries[3].input.reserve_bid_price,
        entries[3].input.size,
        entries[3].input.uid,
    };
    try std.testing.expectEqualSlices(
        i64,
        &.{ 0x0000_0004_4011_2700, 0, 0, 0, 0 },
        &query_words,
    );
    var decoded_query = try java_binary_wire.decodeRequestWords(allocator, &query_words, .{});
    defer decoded_query.deinit(allocator);
    const expected_query_body = try binary_payload.encodeJavaReportQueryToBytes(allocator, .state_hash);
    defer allocator.free(expected_query_body);
    try std.testing.expectEqualSlices(u8, expected_query_body, decoded_query.body);

    var balance_report = try core.queryReport(.{
        .user_balance = .{ .uid = 7, .currency = 11 },
    });
    defer balance_report.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 500), balance_report.user_balance);
}

test "exchange core typed binary async wrappers enqueue and resolve requests" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();
    installTestJavaIdentityHashContext(&core);

    const accounts = [_]BinaryCurrencyBalance{
        .{ .currency = 15, .balance = 15_000 },
    };
    const users = [_]BinaryUserAccountsBatch{
        .{ .uid = 9271, .accounts = accounts[0..] },
    };

    const add_req = try core.submitAddAccountsBinaryAsync(
        612,
        2_471,
        users[0..],
    );
    const query_req = try core.submitBinaryReportQueryAsync(
        613,
        2_472,
        .state_hash,
    );

    const processed = try core.drainAsyncPending();
    try std.testing.expect(processed >= 2);

    var add_response = core.takeAsyncResult(add_req) orelse return error.MissingAsyncResult;
    defer add_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_response.result_code);

    var query_response = core.takeAsyncResult(query_req) orelse return error.MissingAsyncResult;
    defer query_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, query_response.result_code);
    try expectJavaStateHashResponse(&query_response);
}

test "exchange core deterministic simulation keeps sync and async typed binary account ingestion convergent" {
    const allocator = std.testing.allocator;
    var sync_core = try ExchangeCore.init(allocator, .{});
    defer sync_core.deinit();
    var async_core = try ExchangeCore.init(allocator, .{});
    defer async_core.deinit();

    const seed: u64 = 0xA11C0DECCAFE5511;
    const rounds: usize = 256;
    const uid_range: usize = 64;
    const base_uid: i64 = 20_000;

    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    var request_ids: std.ArrayListUnmanaged(u64) = .empty;
    defer request_ids.deinit(allocator);
    try request_ids.ensureTotalCapacityPrecise(allocator, rounds);

    for (0..rounds) |idx| {
        const uid = base_uid + @as(i64, @intCast(random.uintLessThan(usize, uid_range)));
        const currency: i32 = if (random.boolean()) 11 else 15;
        const balance = @as(i64, @intCast(random.uintLessThan(u32, 8_000))) +
            1 +
            @as(i64, @intCast(idx % 7));

        const accounts = [_]BinaryCurrencyBalance{
            .{
                .currency = currency,
                .balance = balance,
            },
        };
        const users = [_]BinaryUserAccountsBatch{
            .{
                .uid = uid,
                .accounts = accounts[0..],
            },
        };

        var sync_response = try sync_core.submitAddAccountsBinary(
            @as(i32, @intCast(3_000 + idx)),
            5_000 + @as(i64, @intCast(idx)),
            users[0..],
        );
        try std.testing.expectEqual(cmd.CommandResultCode.success, sync_response.result_code);
        sync_response.deinit(allocator);

        const request_id = try async_core.submitAddAccountsBinaryAsync(
            @as(i32, @intCast(4_000 + idx)),
            6_000 + @as(i64, @intCast(idx)),
            users[0..],
        );
        request_ids.appendAssumeCapacity(request_id);
    }

    const processed = try async_core.drainAsyncPending();
    try std.testing.expectEqual(rounds, processed);

    for (request_ids.items) |request_id| {
        var async_response = async_core.takeAsyncResult(request_id) orelse return error.MissingAsyncResult;
        try std.testing.expectEqual(cmd.CommandResultCode.success, async_response.result_code);
        async_response.deinit(allocator);
    }

    const sync_risk_hash = try queryRiskStateHash(&sync_core, allocator);
    const async_risk_hash = try queryRiskStateHash(&async_core, allocator);
    try std.testing.expectEqual(sync_risk_hash, async_risk_hash);

    const sync_detailed_hash = try queryDetailedStateHash(&sync_core, allocator);
    const async_detailed_hash = try queryDetailedStateHash(&async_core, allocator);
    try std.testing.expectEqual(sync_detailed_hash, async_detailed_hash);

    for (0..uid_range) |offset| {
        const uid = base_uid + @as(i64, @intCast(offset));
        for ([_]i32{ 11, 15 }) |currency| {
            var sync_balance = try sync_core.queryReport(.{
                .user_balance = .{
                    .uid = uid,
                    .currency = currency,
                },
            });
            const sync_value = sync_balance.user_balance;
            sync_balance.deinit(allocator);

            var async_balance = try async_core.queryReport(.{
                .user_balance = .{
                    .uid = uid,
                    .currency = currency,
                },
            });
            const async_value = async_balance.user_balance;
            async_balance.deinit(allocator);

            try std.testing.expectEqual(sync_value, async_value);
        }
    }
}

test "exchange core binary payload wrapper rejects unsupported command type" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const payload = try encodeStateHashReportPayload(allocator);
    defer allocator.free(payload);

    try std.testing.expectError(
        error.InvalidBinaryCommandType,
        core.submitBinaryPayload(.add_user, 701, 99, payload),
    );
}

test "exchange core binary payload wrapper rejects empty payload" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    try std.testing.expectError(
        error.EmptyBinaryPayload,
        core.submitBinaryPayload(.binary_data_query, 702, 100, &.{}),
    );
}

test "exchange core binary frame layout helper validates payload boundaries" {
    const one_byte = try computeBinaryFrameLayout(1);
    try std.testing.expectEqual(@as(usize, 1), one_byte.words_without_padding);
    try std.testing.expectEqual(@as(usize, 1), one_byte.frame_count);

    const bytes_per_frame = binary_frames.FrameWordsPerCommand * @sizeOf(i64);
    const across_frame = try computeBinaryFrameLayout(bytes_per_frame + 1);
    try std.testing.expectEqual(binary_frames.FrameWordsPerCommand + 1, across_frame.words_without_padding);
    try std.testing.expectEqual(@as(usize, 2), across_frame.frame_count);

    try std.testing.expectError(
        error.EmptyBinaryPayload,
        computeBinaryFrameLayout(0),
    );

    const huge = try computeBinaryFrameLayout(std.math.maxInt(usize));
    try std.testing.expect(huge.words_without_padding > 0);
    try std.testing.expect(huge.frame_count > 0);
}

test "exchange core binary command payload async wrapper queues and executes framed add-accounts batch" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const seeds = [_]BinaryAccountSeed{
        .{ .uid = 9071, .currency = 15, .balance = 11_000 },
        .{ .uid = 9072, .currency = 11, .balance = 12_000 },
    };
    const payload = try encodeAddAccountsBinaryPayload(allocator, seeds[0..]);
    defer allocator.free(payload);
    try std.testing.expect(payload.len > binary_frames.FrameWordsPerCommand * @sizeOf(i64));

    const words_without_padding = std.math.divCeil(usize, payload.len, @sizeOf(i64)) catch unreachable;
    const expected_frames = std.math.divCeil(usize, words_without_padding, binary_frames.FrameWordsPerCommand) catch unreachable;

    const request_id = try core.submitBinaryPayloadAsync(
        .binary_data_command,
        801,
        3_579,
        payload,
    );
    try std.testing.expectEqual(expected_frames, core.asyncPendingCount());
    try std.testing.expectEqual(@as(u64, @intCast(expected_frames)), request_id);

    const processed = try core.drainAsyncPending();
    try std.testing.expectEqual(expected_frames, processed);

    var response = core.takeAsyncResult(request_id) orelse return error.MissingAsyncResult;
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);

    var user_1_balance = try core.queryReport(.{
        .user_balance = .{
            .uid = 9071,
            .currency = 15,
        },
    });
    defer user_1_balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 11_000), user_1_balance.user_balance);

    var user_2_balance = try core.queryReport(.{
        .user_balance = .{
            .uid = 9072,
            .currency = 11,
        },
    });
    defer user_2_balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 12_000), user_2_balance.user_balance);
}

test "exchange core binary query payload async wrapper returns state-hash detailed report" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();
    installTestJavaIdentityHashContext(&core);

    const payload = try encodeStateHashReportPayload(allocator);
    defer allocator.free(payload);

    const request_id = try core.submitBinaryPayloadAsync(
        .binary_data_query,
        901,
        4_680,
        payload,
    );

    const processed = try core.drainAsyncPending();
    try std.testing.expect(processed > 0);

    var response = core.takeAsyncResult(request_id) orelse return error.MissingAsyncResult;
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);

    try expectJavaStateHashResponse(&response);
}

test "exchange core binary payload async wrapper rejects unsupported command type" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const payload = try encodeStateHashReportPayload(allocator);
    defer allocator.free(payload);

    try std.testing.expectError(
        error.InvalidBinaryCommandType,
        core.submitBinaryPayloadAsync(.add_user, 902, 55, payload),
    );
}

test "exchange core binary payload async wrapper rejects empty payload" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    try std.testing.expectError(
        error.EmptyBinaryPayload,
        core.submitBinaryPayloadAsync(.binary_data_query, 903, 66, &.{}),
    );
}

test "exchange core running binary streams cross capacity with terminal-only completion" {
    const allocator = std.testing.allocator;
    const query_body = try encodeStateHashReportPayload(allocator);
    defer allocator.free(query_body);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    for ([_]usize{ 4, 8 }) |capacity| {
        var core = try ExchangeCore.initWithIoAndPersistenceRoot(
            allocator,
            std.testing.io,
            tmp.dir,
            .{
                .initial_state = config_mod.InitialStateConfig.cleanStart(
                    if (capacity == 4) "EXBINSTREAM4" else "EXBINSTREAM8",
                ),
                .serialization = .{
                    .enable_journaling = true,
                    .backend = .disk,
                    .storage_folder = if (capacity == 4) "stream-cap4" else "stream-cap8",
                },
                .runtime = .{
                    .queue_capacity = capacity,
                    .max_batch_size = capacity / 4,
                    .wait_strategy = .blocking,
                },
            },
        );
        defer core.deinit();
        try core.startup();

        // Advance the physical ring head so every oversized transfer exercises
        // wrapped queue reservations rather than only index-zero publication.
        const wrap_request = try core.nopAsync();
        try std.testing.expectEqual(@as(usize, 1), try core.drainAsyncPending());
        var wrap_response = core.takeAsyncResult(wrap_request) orelse
            return error.MissingAsyncResult;
        wrap_response.deinit(allocator);
        core.driver.mutex.lockUncancelable(core.driver.io);
        const head_before_sync = core.driver.queue.head;
        core.driver.mutex.unlock(core.driver.io);

        const cap_plus_one = capacity + 1;
        const sync_payload = try padBinaryPayloadToFrameCount(
            allocator,
            query_body,
            cap_plus_one,
        );
        defer allocator.free(sync_payload);
        const sync_journal_start = core.journal.?.len();
        var sync_response = try core.submitBinaryPayload(
            .binary_data_query,
            @intCast(40_000 + capacity),
            1,
            sync_payload,
        );
        // Registered report queries are intentionally smaller than one frame;
        // this non-canonical padded query exercises oversized query transport
        // and terminates with the ordinary command-level decode failure.
        try std.testing.expectEqual(
            cmd.CommandResultCode.binary_command_failed,
            sync_response.result_code,
        );
        sync_response.deinit(allocator);
        try expectJournalBinaryTransfer(
            &core.journal.?,
            sync_journal_start,
            cap_plus_one,
            @intCast(40_000 + capacity),
            .binary_data_query,
        );
        try std.testing.expectEqual(@as(usize, 0), core.asyncReadyCount());
        core.driver.mutex.lockUncancelable(core.driver.io);
        const head_after_sync = core.driver.queue.head;
        core.driver.mutex.unlock(core.driver.io);
        try std.testing.expectEqual(
            (head_before_sync + cap_plus_one) & (capacity - 1),
            head_after_sync,
        );

        const multiple_frames = capacity * 2;
        const command_payload = try encodeAddAccountsPayloadForFrameCount(
            allocator,
            multiple_frames,
            @intCast(50_000 + capacity * 1_000),
        );
        defer allocator.free(command_payload);
        const async_journal_start = core.journal.?.len();
        const async_request = try core.submitBinaryPayloadAsync(
            .binary_data_command,
            @intCast(41_000 + capacity),
            2,
            command_payload,
        );
        try std.testing.expectEqual(multiple_frames, try core.drainAsyncPending());
        try std.testing.expectEqual(@as(usize, 1), core.asyncReadyCount());
        var async_response = core.takeAsyncResult(async_request) orelse
            return error.MissingAsyncResult;
        try std.testing.expectEqual(cmd.CommandResultCode.success, async_response.result_code);
        async_response.deinit(allocator);
        try std.testing.expectEqual(@as(usize, 0), core.asyncReadyCount());
        try expectJournalBinaryTransfer(
            &core.journal.?,
            async_journal_start,
            multiple_frames,
            @intCast(41_000 + capacity),
            .binary_data_command,
        );

        const detached_payload = try padBinaryPayloadToFrameCount(
            allocator,
            query_body,
            cap_plus_one,
        );
        defer allocator.free(detached_payload);
        const detached_journal_start = core.journal.?.len();
        try core.submitBinaryPayloadDetached(
            .binary_data_query,
            @intCast(42_000 + capacity),
            3,
            detached_payload,
        );
        var detached_barrier = try core.nop();
        detached_barrier.deinit(allocator);
        try std.testing.expectEqual(@as(usize, 0), core.asyncReadyCount());
        try expectJournalBinaryTransfer(
            &core.journal.?,
            detached_journal_start,
            cap_plus_one,
            @intCast(42_000 + capacity),
            .binary_data_query,
        );

        var callback_probe: BinaryStreamCallbackProbe = .{};
        const callback_payload = try padBinaryPayloadToFrameCount(
            allocator,
            query_body,
            multiple_frames,
        );
        defer allocator.free(callback_payload);
        const callback_journal_start = core.journal.?.len();
        try core.submitBinaryPayloadAsyncWithCallback(
            .binary_data_query,
            @intCast(43_000 + capacity),
            4,
            callback_payload,
            .{
                .context = &callback_probe,
                .callback = BinaryStreamCallbackProbe.onResponse,
                .failure_callback = BinaryStreamCallbackProbe.onFailure,
            },
        );
        try std.testing.expectEqual(multiple_frames, try core.drainAsyncPending());
        try std.testing.expectEqual(@as(usize, 1), callback_probe.response_count);
        try std.testing.expectEqual(@as(usize, 0), callback_probe.failure_count);
        try std.testing.expectEqual(@as(i32, -1), callback_probe.terminal_symbol);
        try std.testing.expectEqual(
            cmd.CommandResultCode.binary_command_failed,
            callback_probe.terminal_result,
        );
        try std.testing.expectEqual(@as(usize, 0), core.asyncReadyCount());
        try expectJournalBinaryTransfer(
            &core.journal.?,
            callback_journal_start,
            multiple_frames,
            @intCast(43_000 + capacity),
            .binary_data_query,
        );

        // The high-level Java-style callback route uses the same terminal-only
        // ownership instead of rejecting typed binary commands.
        const typed_sequence_before = core.driver.publication_sequencer.highWater();
        try core.submitApiCommandWithCallback(.{
            .binary_report_query = .{
                .transfer_id = @intCast(44_000 + capacity),
                .timestamp = 5,
                .query = .{ .single_user = 123_456 },
            },
        }, .{
            .context = &callback_probe,
            .callback = BinaryStreamCallbackProbe.onResponse,
            .failure_callback = BinaryStreamCallbackProbe.onFailure,
        });
        const typed_frame_count = core.driver.publication_sequencer.highWater() -
            typed_sequence_before;
        try std.testing.expectEqual(
            @as(usize, @intCast(typed_frame_count)),
            try core.drainAsyncPending(),
        );
        try std.testing.expectEqual(@as(usize, 2), callback_probe.response_count);
        try std.testing.expectEqual(@as(usize, 0), callback_probe.failure_count);
        try std.testing.expectEqual(cmd.CommandResultCode.success, callback_probe.terminal_result);

        try std.testing.expectEqual(core.driver.accepted_total, core.driver.completed_total);
        try std.testing.expectEqual(@as(usize, 0), core.driver.journal_slots_reserved);
        try std.testing.expectEqual(@as(usize, 0), core.pendingCallbackCount());
        try std.testing.expect(!core.driver.active_binary_stream);
        try std.testing.expect(core.driver.queue.isEmpty());
    }
}

test "exchange core cold oversized binary async detached and callback reject atomically" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var core = try ExchangeCore.initWithIoAndPersistenceRoot(
        allocator,
        std.testing.io,
        tmp.dir,
        .{
            .initial_state = config_mod.InitialStateConfig.cleanStart("EXBINCOLD"),
            .serialization = .{
                .enable_journaling = true,
                .backend = .disk,
                .storage_folder = "binary-cold",
            },
            .runtime = .{
                .queue_capacity = 4,
                .max_batch_size = 1,
                .wait_strategy = .blocking,
            },
        },
    );
    defer core.deinit();

    const query_body = try encodeStateHashReportPayload(allocator);
    defer allocator.free(query_body);
    const payload = try padBinaryPayloadToFrameCount(allocator, query_body, 5);
    defer allocator.free(payload);

    // Cold synchronous execution remains direct and is not bounded by queue
    // capacity, even when the logical transfer is larger than the ring.
    var sync_response = try core.submitBinaryPayload(
        .binary_data_query,
        45_001,
        1,
        payload,
    );
    try std.testing.expectEqual(
        cmd.CommandResultCode.binary_command_failed,
        sync_response.result_code,
    );
    sync_response.deinit(allocator);

    const request_before = core.api.next_request_id;
    const sequence_before = core.driver.publication_sequencer.highWater();
    const accepted_before = core.driver.accepted_total;
    const journal_before = core.journal.?.len();
    var callback_probe: BinaryStreamCallbackProbe = .{};

    try std.testing.expectError(
        error.CoreQueueFull,
        core.submitBinaryPayloadAsync(.binary_data_query, 45_002, 2, payload),
    );
    try std.testing.expectError(
        error.CoreQueueFull,
        core.submitBinaryPayloadDetached(.binary_data_query, 45_003, 3, payload),
    );
    try std.testing.expectError(
        error.CoreQueueFull,
        core.submitBinaryPayloadAsyncWithCallback(
            .binary_data_query,
            45_004,
            4,
            payload,
            .{
                .context = &callback_probe,
                .callback = BinaryStreamCallbackProbe.onResponse,
                .failure_callback = BinaryStreamCallbackProbe.onFailure,
            },
        ),
    );

    try std.testing.expectEqual(request_before, core.api.next_request_id);
    try std.testing.expectEqual(sequence_before, core.driver.publication_sequencer.highWater());
    try std.testing.expectEqual(accepted_before, core.driver.accepted_total);
    try std.testing.expectEqual(journal_before, core.journal.?.len());
    try std.testing.expectEqual(@as(usize, 0), core.driver.queue.count());
    try std.testing.expectEqual(@as(usize, 0), core.driver.completion_slots_used);
    try std.testing.expectEqual(@as(usize, 0), core.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, 0), callback_probe.response_count);
    try std.testing.expectEqual(@as(usize, 0), callback_probe.failure_count);
}

test "exchange core binary stream journal OOM fails before admission" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var failing = std.testing.FailingAllocator.init(allocator, .{});
    var core = try ExchangeCore.initWithIoAndPersistenceRoot(
        failing.allocator(),
        std.testing.io,
        tmp.dir,
        .{
            .initial_state = config_mod.InitialStateConfig.cleanStart("EXBINSTREAMOOM"),
            .serialization = .{
                .enable_journaling = true,
                .backend = .disk,
                .storage_folder = "binary-stream-oom",
            },
            .runtime = .{
                .queue_capacity = 4,
                .max_batch_size = 1,
                .wait_strategy = .blocking,
            },
        },
    );
    defer core.deinit();
    try core.startup();

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

    const request_before = core.api.next_request_id;
    const sequence_before = core.driver.publication_sequencer.highWater();
    const accepted_before = core.driver.accepted_total;
    failing.fail_index = failing.alloc_index;
    defer failing.fail_index = std.math.maxInt(usize);
    try std.testing.expectError(
        error.OutOfMemory,
        core.enqueueBinaryFrames(&commands, .asynchronous),
    );
    failing.fail_index = std.math.maxInt(usize);

    try std.testing.expect(failing.has_induced_failure);
    try std.testing.expectEqual(request_before, core.api.next_request_id);
    try std.testing.expectEqual(sequence_before, core.driver.publication_sequencer.highWater());
    try std.testing.expectEqual(accepted_before, core.driver.accepted_total);
    try std.testing.expectEqual(@as(usize, 0), core.journal.?.len());
    try std.testing.expectEqual(@as(usize, 0), core.driver.journal_slots_reserved);
    try std.testing.expectEqual(@as(usize, 0), core.driver.completion_slots_used);
    try std.testing.expectEqual(@as(usize, 0), core.driver.queue.count());
    try std.testing.expect(!core.driver.active_binary_stream);
}

test "exchange core shutdown waits through empty binary stream gaps" {
    const allocator = std.testing.allocator;
    var gate: BackpressureGate = .{ .io = std.testing.io };
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .runtime = .{
            .queue_capacity = 4,
            .max_batch_size = 1,
            .wait_strategy = .blocking,
        },
    });
    defer core.deinit();
    core.setResultSink(.{ .context = &gate, .on_result = BackpressureGate.callback });
    try core.startup();

    const query_body = try encodeStateHashReportPayload(allocator);
    defer allocator.free(query_body);
    const payload = try padBinaryPayloadToFrameCount(allocator, query_body, 12);
    defer allocator.free(payload);

    var publisher: BinaryAsyncPublisher = .{
        .core = &core,
        .payload = payload,
        .transfer_id = 46_001,
    };
    const publisher_thread = try std.Thread.spawn(.{}, BinaryAsyncPublisher.run, .{&publisher});
    gate.waitEntered();

    core.driver.mutex.lockUncancelable(core.driver.io);
    try std.testing.expect(core.driver.active_binary_stream);
    core.driver.mutex.unlock(core.driver.io);

    var shutdown_runner: ShutdownCompletionRunner = .{
        .core = &core,
        .io = std.testing.io,
    };
    const shutdown_thread = try std.Thread.spawn(
        .{},
        ShutdownCompletionRunner.run,
        .{&shutdown_runner},
    );
    shutdown_runner.waitEntered();
    try std.testing.expect(!shutdown_runner.isDone());

    gate.release();
    publisher_thread.join();
    const request_id = try publisher.result.?;
    shutdown_thread.join();

    try std.testing.expect(shutdown_runner.isDone());
    try std.testing.expect(core.isStopped());
    var response = core.takeAsyncResult(request_id) orelse return error.MissingAsyncResult;
    try std.testing.expectEqual(cmd.CommandResultCode.binary_command_failed, response.result_code);
    try std.testing.expectEqual(@as(i32, -1), response.command.symbol);
    response.deinit(allocator);
    try std.testing.expectEqual(core.driver.accepted_total, core.driver.completed_total);
    try std.testing.expect(!core.driver.active_binary_stream);
    try std.testing.expect(core.driver.queue.isEmpty());
}

test "exchange core binary stream gate keeps concurrent publisher after reserved range" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var gate: BackpressureGate = .{ .io = std.testing.io };
    var core = try ExchangeCore.initWithIoAndPersistenceRoot(
        allocator,
        std.testing.io,
        tmp.dir,
        .{
            .initial_state = config_mod.InitialStateConfig.cleanStart("EXBINGATE"),
            .serialization = .{
                .enable_journaling = true,
                .backend = .disk,
                .storage_folder = "binary-gate",
            },
            .runtime = .{
                .queue_capacity = 4,
                .max_batch_size = 1,
                .wait_strategy = .blocking,
            },
        },
    );
    defer core.deinit();
    core.setResultSink(.{ .context = &gate, .on_result = BackpressureGate.callback });
    try core.startup();

    const query_body = try encodeStateHashReportPayload(allocator);
    defer allocator.free(query_body);
    const payload = try padBinaryPayloadToFrameCount(allocator, query_body, 9);
    defer allocator.free(payload);
    var binary_publisher: BinaryAsyncPublisher = .{
        .core = &core,
        .payload = payload,
        .transfer_id = 46_051,
    };
    const binary_thread = try std.Thread.spawn(
        .{},
        BinaryAsyncPublisher.run,
        .{&binary_publisher},
    );
    gate.waitEntered();

    var ordinary_publisher: BackpressureProducer = .{ .core = &core };
    const ordinary_thread = try std.Thread.spawn(
        .{},
        BackpressureProducer.run,
        .{&ordinary_publisher},
    );
    core.driver.mutex.lockUncancelable(core.driver.io);
    while (core.driver.blocked_producers < 2) {
        core.driver.completion_available.waitUncancelable(
            core.driver.io,
            &core.driver.mutex,
        );
    }
    core.driver.mutex.unlock(core.driver.io);

    gate.release();
    binary_thread.join();
    ordinary_thread.join();
    const binary_request = try binary_publisher.result.?;
    const ordinary_request = try ordinary_publisher.result.?;
    try std.testing.expectEqual(@as(usize, 10), try core.drainAsyncPending());

    var binary_response = core.takeAsyncResult(binary_request) orelse
        return error.MissingAsyncResult;
    defer binary_response.deinit(allocator);
    var ordinary_response = core.takeAsyncResult(ordinary_request) orelse
        return error.MissingAsyncResult;
    defer ordinary_response.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 9), binary_response.sequence);
    try std.testing.expectEqual(@as(u64, 10), ordinary_response.sequence);
    try expectJournalBinaryTransfer(
        &core.journal.?,
        0,
        9,
        46_051,
        .binary_data_query,
    );
    try std.testing.expectEqual(cmd.OrderCommandType.add_user, core.journal.?.entries.items[9].input.command);
    try std.testing.expectEqual(core.driver.accepted_total, core.driver.completed_total);
    try std.testing.expect(!core.driver.active_binary_stream);
}

test "exchange core binary stream reentrancy rejects before admission" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .runtime = .{
            .queue_capacity = 4,
            .max_batch_size = 1,
            .wait_strategy = .blocking,
        },
    });
    defer core.deinit();
    try core.startup();

    const query_body = try encodeStateHashReportPayload(allocator);
    defer allocator.free(query_body);
    const payload = try padBinaryPayloadToFrameCount(allocator, query_body, 5);
    defer allocator.free(payload);
    var probe: ReentrantBinaryPublisherProbe = .{
        .core = &core,
        .payload = payload,
    };

    try core.submitApiCommandWithCallback(.{ .add_user = 46_101 }, .{
        .context = &probe,
        .callback = ReentrantBinaryPublisherProbe.onResponse,
    });
    try std.testing.expectEqual(@as(usize, 1), try core.drainAsyncPending());
    try std.testing.expectError(error.CoreReentrantCall, probe.result.?);
    try std.testing.expectEqual(@as(u64, 1), core.driver.publication_sequencer.highWater());
    try std.testing.expectEqual(@as(u64, 1), core.api.next_request_id);
    try std.testing.expect(!core.driver.active_binary_stream);
}

test "exchange core binary stream poison settles unpublished callback suffix" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var core = try ExchangeCore.initWithIoAndPersistenceRoot(
        allocator,
        std.testing.io,
        tmp.dir,
        .{
            .initial_state = config_mod.InitialStateConfig.cleanStart("EXBINSTREAMFAIL"),
            .serialization = .{
                .enable_journaling = true,
                .backend = .disk,
                .storage_folder = "binary-stream-failure",
            },
            .runtime = .{
                .queue_capacity = 4,
                .max_batch_size = 1,
                .wait_strategy = .blocking,
            },
        },
    );
    defer core.deinit();
    core.durable_journal.?.injectSyncFailureForTesting();
    try core.startup();

    const payload = try encodeAddAccountsPayloadForFrameCount(allocator, 9, 47_100);
    defer allocator.free(payload);
    var callback_probe: BinaryStreamCallbackProbe = .{};
    var publisher: BinaryCallbackPublisher = .{
        .core = &core,
        .command_type = .binary_data_command,
        .payload = payload,
        .transfer_id = 47_001,
        .probe = &callback_probe,
    };
    const publisher_thread = try std.Thread.spawn(.{}, BinaryCallbackPublisher.run, .{&publisher});
    publisher_thread.join();
    core.shutdown();

    try std.testing.expectError(error.CorePoisoned, publisher.result.?);
    try std.testing.expectEqual(@as(usize, 0), callback_probe.response_count);
    try std.testing.expectEqual(@as(usize, 1), callback_probe.failure_count);
    try std.testing.expectEqual(
        @as(?CallbackFailureKind, .skipped_after_fatal),
        callback_probe.failure_kind,
    );
    try std.testing.expectEqual(
        @as(?anyerror, error.InjectedJournalIoFailure),
        callback_probe.failure_cause,
    );
    try std.testing.expectEqual(@as(usize, 0), core.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, 0), core.driver.completion_slots_used);
    try std.testing.expectEqual(@as(usize, 0), core.driver.journal_slots_reserved);
    try std.testing.expectEqual(core.driver.accepted_total, core.driver.completed_total);
    try std.testing.expectEqual(core.driver.async_accepted_total, core.driver.async_completed_total);
    try std.testing.expect(!core.driver.active_binary_stream);
    try std.testing.expect(core.driver.queue.isEmpty());
}

test "exchange core runtime drain groups queued commands deterministically" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .runtime = .{
            .max_batch_size = 2,
        },
    });
    defer core.deinit();

    try core.enqueueRuntime(makeAddUserCommand(8901));
    try core.enqueueRuntime(makeAddUserCommand(8902));
    try core.enqueueRuntime(makeAddUserCommand(8903));
    try core.enqueueRuntime(makeAddUserCommand(8903));
    try std.testing.expectEqual(@as(usize, 4), core.runtimePendingCount());

    const outcome = try core.drainRuntime();
    try std.testing.expectEqual(@as(usize, 2), outcome.groups_processed);
    try std.testing.expectEqual(@as(usize, 4), outcome.commands_processed);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, outcome.last_merged_result);
    try std.testing.expectEqual(@as(usize, 0), core.runtimePendingCount());
}

test "exchange core runtime drain honors grouping control mode transitions" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .runtime = .{
            .max_batch_size = 2,
        },
    });
    defer core.deinit();

    try core.enqueueRuntime(makeAddUserCommand(8911));
    try core.enqueueRuntime(makeAddUserCommand(8912));
    try core.enqueueRuntime(makeGroupingControlCommand(false));
    try core.enqueueRuntime(makeAddUserCommand(8913));
    try core.enqueueRuntime(makeAddUserCommand(8914));
    try core.enqueueRuntime(makeGroupingControlCommand(true));
    try core.enqueueRuntime(makeAddUserCommand(8915));
    try core.enqueueRuntime(makeAddUserCommand(8916));

    const outcome = try core.drainRuntime();
    try std.testing.expectEqual(@as(usize, 6), outcome.groups_processed);
    try std.testing.expectEqual(@as(usize, 8), outcome.commands_processed);
    try std.testing.expectEqual(cmd.CommandResultCode.success, outcome.last_merged_result);
    try std.testing.expect(core.runtime.grouping_enabled);
    try std.testing.expectEqual(@as(usize, 0), core.runtimePendingCount());
}

test "exchange core publishes result sink events for sync and batch submits" {
    const allocator = std.testing.allocator;

    const Collector = struct {
        allocator: std.mem.Allocator,
        events: std.ArrayListUnmanaged(runtime_mod.ResultEvent) = .empty,

        fn callback(ctx: ?*anyopaque, event: runtime_mod.ResultEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.events.append(self.allocator, event) catch @panic("collector append failed");
        }

        fn deinit(self: *@This()) void {
            self.events.deinit(self.allocator);
            self.* = undefined;
        }
    };

    var collector: Collector = .{ .allocator = allocator };
    defer collector.deinit();

    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();
    core.setResultSink(.{
        .context = &collector,
        .on_result = Collector.callback,
    });

    var add = try core.addUser(9301);
    defer add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add.result_code);

    var c1 = makeAddUserCommand(9302);
    var c2 = makeAddUserCommand(9302);
    var commands = [_]*cmd.OrderCommand{ &c1, &c2 };
    const batch = try core.submitCommandBatch(commands[0..]);
    try std.testing.expectEqual(@as(?u64, 2), batch.sequence_from);
    try std.testing.expectEqual(@as(?u64, 3), batch.sequence_to);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, batch.result_code);

    try std.testing.expectEqual(@as(usize, 3), collector.events.items.len);
    try std.testing.expectEqual(@as(u64, 1), collector.events.items[0].sequence);
    try std.testing.expectEqual(@as(u64, 2), collector.events.items[1].sequence);
    try std.testing.expectEqual(@as(u64, 3), collector.events.items[2].sequence);
    try std.testing.expectEqual(cmd.CommandResultCode.success, collector.events.items[0].result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, collector.events.items[1].result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, collector.events.items[2].result_code);
}

test "exchange core publishes result sink events during async drain" {
    const allocator = std.testing.allocator;

    const Collector = struct {
        allocator: std.mem.Allocator,
        events: std.ArrayListUnmanaged(runtime_mod.ResultEvent) = .empty,

        fn callback(ctx: ?*anyopaque, event: runtime_mod.ResultEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.events.append(self.allocator, event) catch @panic("collector append failed");
        }

        fn deinit(self: *@This()) void {
            self.events.deinit(self.allocator);
            self.* = undefined;
        }
    };

    var collector: Collector = .{ .allocator = allocator };
    defer collector.deinit();

    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();
    core.setResultSink(.{
        .context = &collector,
        .on_result = Collector.callback,
    });

    const req_ok = try core.addUserAsync(9401);
    const req_dup = try core.addUserAsync(9401);
    const processed = try core.drainAsyncPending();
    try std.testing.expectEqual(@as(usize, 2), processed);

    var ok = core.takeAsyncResult(req_ok) orelse unreachable;
    defer ok.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, ok.result_code);

    var dup = core.takeAsyncResult(req_dup) orelse unreachable;
    defer dup.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, dup.result_code);

    try std.testing.expectEqual(@as(usize, 2), collector.events.items.len);
    try std.testing.expectEqual(@as(u64, 1), collector.events.items[0].sequence);
    try std.testing.expectEqual(@as(u64, 2), collector.events.items[1].sequence);
    try std.testing.expectEqual(cmd.CommandResultCode.success, collector.events.items[0].result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, collector.events.items[1].result_code);
}

test "exchange core result sink follows grouping-control processing across sync/runtime paths" {
    const allocator = std.testing.allocator;

    const Collector = struct {
        allocator: std.mem.Allocator,
        events: std.ArrayListUnmanaged(runtime_mod.ResultEvent) = .empty,

        fn callback(ctx: ?*anyopaque, event: runtime_mod.ResultEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.events.append(self.allocator, event) catch @panic("collector append failed");
        }

        fn deinit(self: *@This()) void {
            self.events.deinit(self.allocator);
            self.* = undefined;
        }
    };

    var collector: Collector = .{ .allocator = allocator };
    defer collector.deinit();

    var core = try ExchangeCore.init(allocator, .{
        .runtime = .{
            .max_batch_size = 4,
        },
    });
    defer core.deinit();
    core.setResultSink(.{
        .context = &collector,
        .on_result = Collector.callback,
    });

    var add_visible = try core.addUser(9451);
    defer add_visible.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_visible.result_code);

    var disable = try core.groupingControl(1_000, 0);
    defer disable.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, disable.result_code);

    var add_suppressed = try core.addUser(9452);
    defer add_suppressed.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_suppressed.result_code);

    try core.enqueueRuntime(makeAddUserCommand(9453));
    try core.enqueueRuntime(makeGroupingControlCommand(true));
    try core.enqueueRuntime(makeAddUserCommand(9454));
    const runtime_outcome = try core.drainRuntime();
    try std.testing.expectEqual(@as(usize, 3), runtime_outcome.commands_processed);

    var add_visible_again = try core.addUser(9455);
    defer add_visible_again.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_visible_again.result_code);

    try std.testing.expectEqual(@as(usize, 4), collector.events.items.len);
    try std.testing.expectEqual(@as(u64, 1), collector.events.items[0].sequence);
    try std.testing.expectEqual(cmd.OrderCommandType.add_user, collector.events.items[0].command);
    try std.testing.expectEqual(@as(u64, 5), collector.events.items[1].sequence);
    try std.testing.expectEqual(cmd.OrderCommandType.grouping_control, collector.events.items[1].command);
    try std.testing.expectEqual(@as(u64, 6), collector.events.items[2].sequence);
    try std.testing.expectEqual(cmd.OrderCommandType.add_user, collector.events.items[2].command);
    try std.testing.expectEqual(@as(u64, 7), collector.events.items[3].sequence);
    try std.testing.expectEqual(cmd.OrderCommandType.add_user, collector.events.items[3].command);

    try std.testing.expect(core.runtime.processing_enabled);
}

test "exchange core raw runtime and simple observers share grouping visibility while completions settle" {
    const allocator = std.testing.allocator;

    const Collector = struct {
        raw_len: usize = 0,
        raw_sequences: [8]u64 = undefined,
        runtime_len: usize = 0,
        runtime_sequences: [8]u64 = undefined,
        simple_len: usize = 0,
        simple_sequences: [8]u64 = undefined,

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

        fn simple(context: ?*anyopaque, event: simple_events_mod.ApiCommandResult) void {
            const self: *@This() = @ptrCast(@alignCast(context.?));
            self.simple_sequences[self.simple_len] = event.sequence;
            self.simple_len += 1;
        }
    };

    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();
    var collector: Collector = .{};
    core.setRawResultSink(.{
        .context = &collector,
        .on_result = Collector.raw,
    });
    core.setResultSink(.{
        .context = &collector,
        .on_result = Collector.runtime,
    });
    core.setSimpleEventsHandler(.{
        .context = &collector,
        .on_command_result = Collector.simple,
    });

    var visible = try core.addUser(94_561);
    defer visible.deinit(allocator);
    var disable = try core.groupingControl(1_000, 0);
    defer disable.deinit(allocator);

    const hidden_request = try core.addUserAsync(94_562);
    try std.testing.expectEqual(@as(usize, 1), try core.drainAsyncPending());
    var hidden = core.takeAsyncResult(hidden_request) orelse return error.MissingAsyncResult;
    defer hidden.deinit(allocator);

    const enable_request = try core.groupingControlAsync(1_001, 1);
    const visible_request = try core.addUserAsync(94_563);
    try std.testing.expectEqual(@as(usize, 2), try core.drainAsyncPending());
    var enable = core.takeAsyncResult(enable_request) orelse return error.MissingAsyncResult;
    defer enable.deinit(allocator);
    var visible_again = core.takeAsyncResult(visible_request) orelse return error.MissingAsyncResult;
    defer visible_again.deinit(allocator);

    try std.testing.expectEqual(cmd.CommandResultCode.success, visible.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, disable.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, hidden.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, enable.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, visible_again.result_code);
    try std.testing.expectEqualSlices(u64, &.{ 1, 4, 5 }, collector.raw_sequences[0..collector.raw_len]);
    try std.testing.expectEqualSlices(u64, &.{ 1, 4, 5 }, collector.runtime_sequences[0..collector.runtime_len]);
    try std.testing.expectEqualSlices(u64, &.{ 1, 5 }, collector.simple_sequences[0..collector.simple_len]);
    try std.testing.expect(core.runtime.processing_enabled);
    try std.testing.expectEqual(@as(usize, 0), core.asyncReadyCount());
    try std.testing.expectEqual(@as(usize, 0), core.driver.completion_slots_used);
    try std.testing.expectEqual(core.driver.accepted_total, core.driver.completed_total);
}

test "exchange core raw observer covers direct queued detached and binary carriers exactly once" {
    const allocator = std.testing.allocator;

    const Collector = struct {
        core: *ExchangeCore,
        poll_request_id: ?u64 = null,
        len: usize = 0,
        sequences: [16]u64 = undefined,
        commands: [16]cmd.OrderCommandType = undefined,
        symbols: [16]i32 = undefined,
        journal_visible: bool = true,
        poll_was_ready_during_callback: bool = false,

        fn raw(context: ?*anyopaque, response: *const kernel_mod.CommandResponse) void {
            const self: *@This() = @ptrCast(@alignCast(context.?));
            const index = self.len;
            self.sequences[index] = response.sequence;
            self.commands[index] = response.command.command;
            self.symbols[index] = response.command.symbol;
            self.len += 1;

            const journal = self.core.journal orelse {
                self.journal_visible = false;
                return;
            };
            if (journal.entries.items.len == 0) {
                self.journal_visible = false;
            } else {
                const entry = journal.entries.items[journal.entries.items.len - 1];
                self.journal_visible = self.journal_visible and
                    entry.sequence == response.sequence and
                    entry.result_code == response.result_code;
            }

            if (self.poll_request_id) |request_id| {
                const driver = self.core.driver;
                driver.mutex.lockUncancelable(driver.io);
                self.poll_was_ready_during_callback =
                    self.poll_was_ready_during_callback or
                    self.core.api.ready_results.contains(request_id);
                driver.mutex.unlock(driver.io);
            }
        }
    };

    var core = try ExchangeCore.init(allocator, .{
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer core.deinit();
    var collector: Collector = .{ .core = &core };
    core.setRawResultSink(.{
        .context = &collector,
        .on_result = Collector.raw,
    });

    var direct = try core.addUser(94_571);
    defer direct.deinit(allocator);

    const queued_request = try core.addUserAsync(94_572);
    collector.poll_request_id = queued_request;
    try std.testing.expectEqual(@as(usize, 1), try core.drainAsyncPending());
    var queued = core.takeAsyncResult(queued_request) orelse return error.MissingAsyncResult;
    defer queued.deinit(allocator);
    collector.poll_request_id = null;

    try core.submitApiCommandDetached(.{ .add_user = 94_573 });
    try std.testing.expectEqual(@as(usize, 0), try core.drainAsyncPending());
    try std.testing.expectEqual(@as(usize, 3), collector.len);

    const payload = try encodeAddAccountsPayloadForFrameCount(allocator, 3, 94_580);
    defer allocator.free(payload);
    try core.submitBinaryPayloadDetached(
        .binary_data_command,
        45_710,
        9_457_100_001,
        payload,
    );
    try std.testing.expectEqual(@as(usize, 0), try core.drainAsyncPending());

    try std.testing.expectEqual(@as(usize, 6), collector.len);
    for (collector.sequences[0..collector.len], 0..) |sequence, index| {
        try std.testing.expectEqual(@as(u64, @intCast(index + 1)), sequence);
    }
    try std.testing.expectEqualSlices(cmd.OrderCommandType, &.{
        .add_user,
        .add_user,
        .add_user,
        .binary_data_command,
        .binary_data_command,
        .binary_data_command,
    }, collector.commands[0..collector.len]);
    try std.testing.expectEqualSlices(i32, &.{ 0, 0, -1 }, collector.symbols[3..6]);
    try std.testing.expect(collector.journal_visible);
    try std.testing.expect(!collector.poll_was_ready_during_callback);
    try std.testing.expectEqual(@as(usize, 6), try core.journalLen());
    try std.testing.expectEqual(@as(usize, 0), core.asyncReadyCount());
}

test "exchange core simple events handler emits command and orderbook callbacks across sync async and runtime paths" {
    const allocator = std.testing.allocator;

    const SeenCommand = enum {
        add_user,
        order_book_request,
        other,
    };

    const Collector = struct {
        allocator: std.mem.Allocator,
        commands: std.ArrayListUnmanaged(SeenCommand) = .empty,
        order_book_count: usize = 0,
        trade_count: usize = 0,

        fn classify(event: simple_events_mod.ApiCommandResult) SeenCommand {
            return switch (event.command) {
                .add_user => .add_user,
                .order_book_request => .order_book_request,
                else => .other,
            };
        }

        fn onCommandResult(ctx: ?*anyopaque, event: simple_events_mod.ApiCommandResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.commands.append(self.allocator, classify(event)) catch @panic("collector append failed");
        }

        fn onOrderBook(ctx: ?*anyopaque, _: simple_events_mod.OrderBook) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.order_book_count += 1;
        }

        fn onTrade(ctx: ?*anyopaque, _: simple_events_mod.TradeEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.trade_count += 1;
        }

        fn deinit(self: *@This()) void {
            self.commands.deinit(self.allocator);
            self.* = undefined;
        }
    };

    var collector: Collector = .{ .allocator = allocator };
    defer collector.deinit();

    var core = try ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer core.deinit();
    core.setSimpleEventsHandler(.{
        .context = &collector,
        .on_command_result = Collector.onCommandResult,
        .on_order_book = Collector.onOrderBook,
        .on_trade_event = Collector.onTrade,
    });

    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(241)));

    var sync_add = try core.addUser(9601);
    defer sync_add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, sync_add.result_code);

    const async_req = try core.addUserAsync(9602);
    try std.testing.expectEqual(@as(usize, 1), try core.drainAsyncPending());
    var async_response = core.takeAsyncResult(async_req) orelse unreachable;
    defer async_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, async_response.result_code);

    try core.enqueueRuntime(makeAddUserCommand(9603));
    const runtime_outcome = try core.drainRuntime();
    try std.testing.expectEqual(@as(usize, 1), runtime_outcome.commands_processed);

    var order_book = try core.requestOrderBook(241, 8);
    defer order_book.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, order_book.result_code);

    try std.testing.expectEqual(@as(usize, 4), collector.commands.items.len);
    try std.testing.expectEqual(SeenCommand.add_user, collector.commands.items[0]);
    try std.testing.expectEqual(SeenCommand.add_user, collector.commands.items[1]);
    try std.testing.expectEqual(SeenCommand.add_user, collector.commands.items[2]);
    try std.testing.expectEqual(SeenCommand.order_book_request, collector.commands.items[3]);
    try std.testing.expectEqual(@as(usize, 1), collector.order_book_count);
    // These successful user-management and order-book commands produce no matcher events.
    try std.testing.expectEqual(@as(usize, 0), collector.trade_count);
}

test "exchange core simple events handler honors grouping-control result suppression" {
    const allocator = std.testing.allocator;

    const SeenCommand = enum {
        add_user,
        other,
    };

    const Collector = struct {
        allocator: std.mem.Allocator,
        commands: std.ArrayListUnmanaged(SeenCommand) = .empty,

        fn classify(event: simple_events_mod.ApiCommandResult) SeenCommand {
            return switch (event.command) {
                .add_user => .add_user,
                else => .other,
            };
        }

        fn onCommandResult(ctx: ?*anyopaque, event: simple_events_mod.ApiCommandResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.commands.append(self.allocator, classify(event)) catch @panic("collector append failed");
        }

        fn deinit(self: *@This()) void {
            self.commands.deinit(self.allocator);
            self.* = undefined;
        }
    };

    var collector: Collector = .{ .allocator = allocator };
    defer collector.deinit();

    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();
    core.setSimpleEventsHandler(.{
        .context = &collector,
        .on_command_result = Collector.onCommandResult,
    });

    var add_before_disable = try core.addUser(9701);
    defer add_before_disable.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_before_disable.result_code);

    var disable = try core.groupingControl(1_000, 0);
    defer disable.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, disable.result_code);

    var add_while_disabled = try core.addUser(9702);
    defer add_while_disabled.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_while_disabled.result_code);

    var enable = try core.groupingControl(1_001, 1);
    defer enable.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, enable.result_code);

    var add_after_enable = try core.addUser(9703);
    defer add_after_enable.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_after_enable.result_code);

    try std.testing.expectEqual(@as(usize, 2), collector.commands.items.len);
    try std.testing.expectEqual(SeenCommand.add_user, collector.commands.items[0]);
    try std.testing.expectEqual(SeenCommand.add_user, collector.commands.items[1]);
    try std.testing.expect(core.runtime.processing_enabled);
}

test "exchange core can clear simple events handler without affecting command execution" {
    const allocator = std.testing.allocator;

    const Collector = struct {
        callback_count: usize = 0,

        fn onCommandResult(ctx: ?*anyopaque, _: simple_events_mod.ApiCommandResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.callback_count += 1;
        }
    };

    var collector: Collector = .{};

    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();
    core.setSimpleEventsHandler(.{
        .context = &collector,
        .on_command_result = Collector.onCommandResult,
    });

    var first = try core.addUser(9801);
    defer first.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, first.result_code);
    try std.testing.expectEqual(@as(usize, 1), collector.callback_count);

    core.clearSimpleEventsHandler();

    var second = try core.addUser(9802);
    defer second.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, second.result_code);
    try std.testing.expectEqual(@as(usize, 1), collector.callback_count);
}

test "exchange core simple events handler emits trade callbacks from live matcher events" {
    const allocator = std.testing.allocator;

    const Collector = struct {
        trade_count: usize = 0,
        reject_count: usize = 0,
        last_total_volume: i64 = 0,
        last_trades_len: usize = 0,
        last_taker_action: common.OrderAction = .ask,

        fn onTrade(ctx: ?*anyopaque, event: simple_events_mod.TradeEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.trade_count += 1;
            self.last_total_volume = event.total_volume;
            self.last_trades_len = event.trades.len;
            self.last_taker_action = event.taker_action;
        }

        fn onReject(ctx: ?*anyopaque, _: simple_events_mod.RejectEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.reject_count += 1;
        }
    };

    var collector: Collector = .{};

    var core = try ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer core.deinit();
    core.setSimpleEventsHandler(.{
        .context = &collector,
        .on_trade_event = Collector.onTrade,
        .on_reject_event = Collector.onReject,
    });

    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(241)));

    var add_maker = try core.addUser(9901);
    defer add_maker.deinit(allocator);
    var add_taker = try core.addUser(9902);
    defer add_taker.deinit(allocator);

    var fund_maker = try core.adjustUserBalance(9901, 1, 11, 10_000, .adjustment);
    defer fund_maker.deinit(allocator);
    var fund_taker = try core.adjustUserBalance(9902, 2, 15, 100_000, .adjustment);
    defer fund_taker.deinit(allocator);

    var maker_order = try core.placeOrder(
        70_001,
        9901,
        241,
        100,
        100,
        1,
        .ask,
        .gtc,
        0,
    );
    defer maker_order.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, maker_order.result_code);

    var taker_order = try core.placeOrder(
        70_002,
        9902,
        241,
        100,
        100,
        1,
        .bid,
        .gtc,
        0,
    );
    defer taker_order.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, taker_order.result_code);

    try std.testing.expectEqual(@as(usize, 1), collector.trade_count);
    try std.testing.expectEqual(@as(i64, 1), collector.last_total_volume);
    try std.testing.expectEqual(@as(usize, 1), collector.last_trades_len);
    try std.testing.expectEqual(common.OrderAction.bid, collector.last_taker_action);
    try std.testing.expectEqual(@as(usize, 0), collector.reject_count);
}

test "exchange core runtime non-journaling simple events handler emits trade callbacks" {
    const allocator = std.testing.allocator;

    const Collector = struct {
        trade_count: usize = 0,
        last_total_volume: i64 = 0,
        last_trades_len: usize = 0,
        last_taker_action: common.OrderAction = .ask,

        fn onTrade(ctx: ?*anyopaque, event: simple_events_mod.TradeEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.trade_count += 1;
            self.last_total_volume = event.total_volume;
            self.last_trades_len = event.trades.len;
            self.last_taker_action = event.taker_action;
        }
    };

    var collector: Collector = .{};

    var core = try ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
        .runtime = .{
            .max_batch_size = 4,
        },
    });
    defer core.deinit();
    core.setSimpleEventsHandler(.{
        .context = &collector,
        .on_trade_event = Collector.onTrade,
    });

    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(241)));

    var add_maker = try core.addUser(9911);
    defer add_maker.deinit(allocator);
    var add_taker = try core.addUser(9912);
    defer add_taker.deinit(allocator);

    var fund_maker = try core.adjustUserBalance(9911, 11, 11, 10_000, .adjustment);
    defer fund_maker.deinit(allocator);
    var fund_taker = try core.adjustUserBalance(9912, 12, 15, 100_000, .adjustment);
    defer fund_taker.deinit(allocator);

    try core.enqueueRuntime(makePlaceOrderCommand(71_001, 9911, 241, 100, 100, 1, .ask, .gtc, 0));
    try core.enqueueRuntime(makePlaceOrderCommand(71_002, 9912, 241, 100, 100, 1, .bid, .gtc, 0));

    const outcome = try core.drainRuntime();
    try std.testing.expectEqual(@as(usize, 2), outcome.commands_processed);
    try std.testing.expectEqual(@as(usize, 1), collector.trade_count);
    try std.testing.expectEqual(@as(i64, 1), collector.last_total_volume);
    try std.testing.expectEqual(@as(usize, 1), collector.last_trades_len);
    try std.testing.expectEqual(common.OrderAction.bid, collector.last_taker_action);
}

test "exchange core journaling runtime drain does not duplicate result sink events" {
    const allocator = std.testing.allocator;

    const Collector = struct {
        allocator: std.mem.Allocator,
        events: std.ArrayListUnmanaged(runtime_mod.ResultEvent) = .empty,

        fn callback(ctx: ?*anyopaque, event: runtime_mod.ResultEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.events.append(self.allocator, event) catch @panic("collector append failed");
        }

        fn deinit(self: *@This()) void {
            self.events.deinit(self.allocator);
            self.* = undefined;
        }
    };

    var collector: Collector = .{ .allocator = allocator };
    defer collector.deinit();

    var core = try ExchangeCore.init(allocator, .{
        .runtime = .{
            .max_batch_size = 2,
        },
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
        },
    });
    defer core.deinit();
    core.setResultSink(.{
        .context = &collector,
        .on_result = Collector.callback,
    });

    try core.enqueueRuntime(makeAddUserCommand(9501));
    try core.enqueueRuntime(makeAddUserCommand(9502));
    try core.enqueueRuntime(makeAddUserCommand(9502));

    const outcome = try core.drainRuntime();
    try std.testing.expectEqual(@as(usize, 3), outcome.commands_processed);
    try std.testing.expectEqual(@as(usize, 3), collector.events.items.len);
    try std.testing.expectEqual(@as(u64, 1), collector.events.items[0].sequence);
    try std.testing.expectEqual(@as(u64, 2), collector.events.items[1].sequence);
    try std.testing.expectEqual(@as(u64, 3), collector.events.items[2].sequence);
    try std.testing.expectEqual(cmd.CommandResultCode.success, collector.events.items[0].result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, collector.events.items[1].result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, collector.events.items[2].result_code);
}

test "exchange core async wrappers keep request/result routing semantics" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const req_ok = try core.addUserAsync(9001);
    const req_dup = try core.addUserAsync(9001);
    try std.testing.expectEqual(@as(usize, 2), core.asyncPendingCount());
    try std.testing.expect(!core.isAsyncResultReady(req_ok));
    try std.testing.expect(!core.isAsyncResultReady(req_dup));

    try std.testing.expectError(
        error.OrderBookDepthOverflow,
        core.requestOrderBookAsync(241, std.math.maxInt(usize)),
    );

    const processed = try core.drainAsyncPending();
    try std.testing.expectEqual(@as(usize, 2), processed);
    try std.testing.expectEqual(@as(usize, 0), core.asyncPendingCount());

    var ok = core.takeAsyncResult(req_ok) orelse unreachable;
    defer ok.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, ok.result_code);

    var dup = core.takeAsyncResult(req_dup) orelse unreachable;
    defer dup.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, dup.result_code);
    try std.testing.expectEqual(@as(?kernel_mod.CommandResponse, null), core.takeAsyncResult(req_dup));
}

test "exchange core flush result validator accepts response and rejects persist variant" {
    var response_result: ApiCommandResult = .{
        .response = .{
            .command = .{ .command = .nop },
            .result_code = .success,
        },
    };
    try ExchangeCore.expectFlushResultVariant(&response_result);

    var persist_result: ApiCommandResult = .{
        .persist_state = .success,
    };
    try std.testing.expectError(
        error.UnexpectedFlushResultVariant,
        ExchangeCore.expectFlushResultVariant(&persist_result),
    );
}

test "exchange core persist async wrappers expose merged paired result" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    const req_add = try core.addUserAsync(9051);
    const req_suspend = try core.suspendUserAsync(9051);
    const req_resume = try core.resumeUserAsync(9051);
    const persist_requests = try core.persistStateAsync(8002, 222_333_444);
    try std.testing.expectEqual(@as(usize, 5), core.asyncPendingCount());
    try std.testing.expect(!core.isPersistStateResultReady(persist_requests));

    const processed = try core.drainAsyncPending();
    try std.testing.expectEqual(@as(usize, 5), processed);
    try std.testing.expect(core.isPersistStateResultReady(persist_requests));
    try std.testing.expectEqual(@as(usize, 1), try core.persistenceSnapshotCount());
    try std.testing.expectEqual(@as(u64, 0), (try core.persistenceCurrentSnapshot()).snapshot_id);

    var add = core.takeAsyncResult(req_add) orelse unreachable;
    defer add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add.result_code);

    var suspend_async_resp = core.takeAsyncResult(req_suspend) orelse unreachable;
    defer suspend_async_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, suspend_async_resp.result_code);

    var resume_async_resp = core.takeAsyncResult(req_resume) orelse unreachable;
    defer resume_async_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, resume_async_resp.result_code);

    const merged = core.takePersistStateResult(persist_requests) orelse unreachable;
    try std.testing.expectEqual(cmd.CommandResultCode.state_persist_matching_engine_failed, merged);
    try std.testing.expect(core.takePersistStateResult(persist_requests) == null);
}

test "exchange core persist wrappers record both commands when journaling is enabled" {
    const allocator = std.testing.allocator;
    var tmp_storage = std.testing.tmpDir(.{});
    defer tmp_storage.cleanup();
    var storage_folder_buf: [256]u8 = undefined;
    const storage_folder = try std.fmt.bufPrint(
        &storage_folder_buf,
        ".zig-cache/tmp/{s}/persist-journal",
        .{tmp_storage.sub_path},
    );
    var core = try ExchangeCore.init(allocator, .{
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = storage_folder,
        },
    });
    defer core.deinit();

    try std.testing.expect(core.journalingEnabled());
    try std.testing.expectEqual(@as(usize, 0), try core.journalLen());
    try std.testing.expectEqual(@as(usize, 1), try core.persistenceSnapshotCount());
    try std.testing.expectEqual(@as(usize, 0), try core.persistenceJournalCount());

    const sync_result = try core.persistState(8003, 333_444_555);
    try std.testing.expectEqual(cmd.CommandResultCode.success, sync_result);
    try std.testing.expectEqual(@as(usize, 2), try core.journalLen());
    try std.testing.expectEqual(@as(usize, 2), try core.persistenceSnapshotCount());

    const persisted_snapshot = try core.persistenceCurrentSnapshot();
    try std.testing.expectEqual(@as(u64, 8003), persisted_snapshot.snapshot_id);
    try std.testing.expectEqual(@as(u64, 2), persisted_snapshot.seq);

    const persist_requests = try core.persistStateAsync(8004, 444_555_666);
    try std.testing.expectEqual(@as(usize, 2), core.asyncPendingCount());
    _ = try core.drainAsyncPending();
    const async_result = core.takePersistStateResult(persist_requests) orelse unreachable;
    try std.testing.expectEqual(cmd.CommandResultCode.success, async_result);
    try std.testing.expectEqual(@as(usize, 4), try core.journalLen());
    try std.testing.expectEqual(@as(usize, 3), try core.persistenceSnapshotCount());
    try std.testing.expectEqual(@as(u64, 8004), (try core.persistenceCurrentSnapshot()).snapshot_id);
    try std.testing.expectEqual(@as(u64, 4), (try core.persistenceCurrentSnapshot()).seq);

    var post_persist = try core.addUser(9052);
    defer post_persist.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, post_persist.result_code);
    try std.testing.expectEqual(@as(usize, 5), try core.journalLen());

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try core.writeJournalToFile(tmp.dir, "persist.journal");

    try std.testing.expectEqual(@as(usize, 1), try core.persistenceJournalCount());
    try std.testing.expectEqual(@as(?JournalRef, null), try core.persistenceFindJournalForSequence(4));
    const replay_ref = (try core.persistenceFindJournalForSequence(5)) orelse return error.ExpectedJournal;
    const replay_desc = try core.persistenceJournalByRef(replay_ref);
    try std.testing.expectEqual(@as(u64, 5), replay_desc.seq_first);
    try std.testing.expectEqual(@as(?u64, 5), replay_desc.seq_last);
    try std.testing.expectEqual(try core.persistenceCurrentSnapshotRef(), replay_desc.base_snapshot);
}

test "exchange core persistence snapshot lookup resolves known ids" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var storage_folder_buf: [256]u8 = undefined;
    const storage_folder = try std.fmt.bufPrint(
        &storage_folder_buf,
        ".zig-cache/tmp/{s}/snapshot-lookup",
        .{tmp.sub_path},
    );
    var core = try ExchangeCore.init(allocator, .{
        .serialization = .{ .backend = .disk, .storage_folder = storage_folder },
    });
    defer core.deinit();

    const root_ref = (try core.persistenceFindSnapshotById(0)) orelse return error.ExpectedSnapshot;
    try std.testing.expectEqual(try core.persistenceCurrentSnapshotRef(), root_ref);
    const root_snapshot = (try core.persistenceSnapshotById(0)) orelse return error.ExpectedSnapshot;
    try std.testing.expectEqual(@as(u64, 0), root_snapshot.snapshot_id);

    _ = try core.persistState(82_001, 1_000_111);
    _ = try core.persistState(82_002, 1_000_222);

    const first_ref = (try core.persistenceFindSnapshotById(82_001)) orelse return error.ExpectedSnapshot;
    const second_ref = (try core.persistenceFindSnapshotById(82_002)) orelse return error.ExpectedSnapshot;
    try std.testing.expect(first_ref != second_ref);

    const second_snapshot = (try core.persistenceSnapshotById(82_002)) orelse return error.ExpectedSnapshot;
    try std.testing.expectEqual(@as(u64, 82_002), second_snapshot.snapshot_id);
    try std.testing.expectEqual(@as(?SnapshotRef, null), try core.persistenceFindSnapshotById(999_999));
    try std.testing.expectEqual(@as(?SnapshotDescriptorView, null), try core.persistenceSnapshotById(999_999));
}

test "exchange core persist-state main-log wrapper appends entries using provided dir" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var storage_folder_buf: [256]u8 = undefined;
    const storage_folder = try std.fmt.bufPrint(
        &storage_folder_buf,
        ".zig-cache/tmp/{s}/persist-main-log",
        .{tmp.sub_path},
    );

    var core = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXPERSISTLOG"),
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = storage_folder,
        },
    });
    defer core.deinit();

    const first = try core.persistStateWithDefaultMainLog(
        std.Io.Dir.cwd(),
        81_001,
        1_111_222,
        .risk_engine,
        0,
    );
    try std.testing.expectEqual(cmd.CommandResultCode.success, first);

    const second = try core.persistStateWithDefaultMainLog(
        std.Io.Dir.cwd(),
        81_002,
        1_111_333,
        .matching_engine_router,
        1,
    );
    try std.testing.expectEqual(cmd.CommandResultCode.success, second);

    const main_log_path = try core.defaultPersistenceMainLogPath(allocator);
    defer allocator.free(main_log_path);
    var main_log_file = try std.Io.Dir.cwd().openFile(io_context.current(), main_log_path, .{});
    defer main_log_file.close(io_context.current());
    const main_log_contents = try io_context.readToEndAlloc(main_log_file, allocator, 8192);
    defer allocator.free(main_log_contents);

    try std.testing.expect(std.mem.indexOf(
        u8,
        main_log_contents,
        "seq=2 timestampNs=1111222 snapshotId=81001 type=RE instance=0",
    ) != null);
    try std.testing.expect(std.mem.indexOf(
        u8,
        main_log_contents,
        "seq=3 timestampNs=1111333 snapshotId=81002 type=ME instance=1",
    ) != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "snapshotId=81001") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "timestampNs=1111222") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "type=RE") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "instance=0") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "snapshotId=81002") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "timestampNs=1111333") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "type=ME") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "instance=1") != null);

    var line_count: usize = 0;
    for (main_log_contents) |ch| {
        if (ch == '\n') line_count += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), line_count);
}

test "exchange core configured persistence root owns every durable artifact and restores" {
    const allocator = std.testing.allocator;
    const io = io_context.current();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "caller-root");

    var storage_folder_buf: [128]u8 = undefined;
    const storage_folder = try std.fmt.bufPrint(
        &storage_folder_buf,
        "owned-persistence-{s}",
        .{tmp.sub_path},
    );
    // The random test-relative name lets us prove that an implementation did
    // not silently fall back to cwd without risking an unrelated directory.
    try std.testing.expectError(
        error.FileNotFound,
        std.Io.Dir.cwd().access(io, storage_folder, .{}),
    );
    defer std.Io.Dir.cwd().deleteTree(io, storage_folder) catch {};

    const config: Config = .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXROOTED"),
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = storage_folder,
        },
    };
    var core = blk: {
        var borrowed_root = try tmp.dir.openDir(io, "caller-root", .{});
        defer borrowed_root.close(io);
        break :blk try ExchangeCore.initWithIoAndPersistenceRoot(
            allocator,
            std.testing.io,
            borrowed_root,
            config,
        );
    };
    defer core.deinit();
    var persistence_root = try tmp.dir.openDir(io, "caller-root", .{});
    defer persistence_root.close(io);

    var add_user = try core.addUser(181_001);
    defer add_user.deinit(allocator);
    var add_balance = try core.adjustUserBalance(181_001, 1, 15, 87_654, .adjustment);
    defer add_balance.deinit(allocator);

    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try core.persistStateWithConfiguredMainLog(
            181_101,
            9_876_543,
            .risk_engine,
            0,
        ),
    );

    const matching_path = try core.defaultPersistenceSnapshotPath(
        allocator,
        181_101,
        .matching_engine_router,
        0,
    );
    defer allocator.free(matching_path);
    const risk_path = try core.defaultPersistenceSnapshotPath(
        allocator,
        181_101,
        .risk_engine,
        0,
    );
    defer allocator.free(risk_path);
    const manifest_path = try core.defaultPersistenceSnapshotManifestPath(allocator, 181_101);
    defer allocator.free(manifest_path);
    const main_log_path = try core.defaultPersistenceMainLogPath(allocator);
    defer allocator.free(main_log_path);

    try persistence_root.access(io, matching_path, .{});
    try persistence_root.access(io, risk_path, .{});
    try persistence_root.access(io, manifest_path, .{});
    try persistence_root.access(io, main_log_path, .{});
    try std.testing.expectError(
        error.FileNotFound,
        std.Io.Dir.cwd().access(io, storage_folder, .{}),
    );

    const restored_config: Config = .{
        .initial_state = config_mod.InitialStateConfig.fromSnapshotOnly("EXROOTED", 181_101, 0),
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = storage_folder,
        },
    };
    var restored = try ExchangeCore.initWithPersistenceRoot(
        allocator,
        persistence_root,
        restored_config,
    );
    defer restored.deinit();
    const outcome = try restored.bootstrapFromPersistence(
        persistence_root,
        matching_path,
        null,
    );
    try std.testing.expect(outcome.snapshot_found);
    try std.testing.expectEqual(core.kernelRef().next_seq, restored.kernelRef().next_seq);
    var restored_balance = try restored.queryReport(.{
        .user_balance = .{ .uid = 181_001, .currency = 15 },
    });
    defer restored_balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 87_654), restored_balance.user_balance);
}

test "exchange core rejects a mismatched persistence root before publication" {
    const allocator = std.testing.allocator;
    const io = io_context.current();
    var configured_tmp = std.testing.tmpDir(.{});
    defer configured_tmp.cleanup();
    var other_tmp = std.testing.tmpDir(.{});
    defer other_tmp.cleanup();

    const storage_folder = "mismatch-must-not-materialize";
    var core = try ExchangeCore.initWithPersistenceRoot(allocator, configured_tmp.dir, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXROOTMISMATCH"),
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = storage_folder,
        },
    });
    defer core.deinit();

    const sequence_before = core.kernelRef().next_seq;
    const snapshot_count_before = try core.persistenceSnapshotCount();
    const journal_count_before = try core.persistenceJournalCount();
    try std.testing.expectError(
        error.PersistenceRootMismatch,
        core.persistStateWithDefaultMainLog(
            other_tmp.dir,
            182_001,
            1_234_567,
            .risk_engine,
            0,
        ),
    );

    try std.testing.expectEqual(sequence_before, core.kernelRef().next_seq);
    try std.testing.expectEqual(snapshot_count_before, try core.persistenceSnapshotCount());
    try std.testing.expectEqual(journal_count_before, try core.persistenceJournalCount());
    // Core initialization owns and opens the native XCJ3 partition under the
    // configured root, but the rejected persist must not materialize anything
    // under the caller-supplied mismatched root.
    try configured_tmp.dir.access(io, storage_folder, .{});
    try std.testing.expectError(
        error.FileNotFound,
        other_tmp.dir.access(io, storage_folder, .{}),
    );
}

test "exchange core durable persist commits distinct ME and RE payloads with sequence and binary state" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var storage_folder_buf: [256]u8 = undefined;
    const storage_folder = try std.fmt.bufPrint(
        &storage_folder_buf,
        ".zig-cache/tmp/{s}/durable-pair",
        .{tmp.sub_path},
    );
    var core = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXPAIR"),
        .serialization = .{ .backend = .disk, .storage_folder = storage_folder },
    });
    defer core.deinit();

    var add_user = try core.addUser(90_001);
    defer add_user.deinit(allocator);
    var add_balance = try core.adjustUserBalance(90_001, 1, 15, 12_345, .adjustment);
    defer add_balance.deinit(allocator);

    var partial_frame: cmd.OrderCommand = .{
        .command = .binary_data_command,
        .order_id = 1,
        .price = 2,
        .reserve_bid_price = 3,
        .size = 4,
        .uid = 5,
        .symbol = 0,
        .user_cookie = 77,
        .result_code = .new,
    };
    var frame_response = try core.submitCommand(&partial_frame);
    defer frame_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.accepted, frame_response.result_code);

    const result = try core.persistState(9001, 123_456);
    try std.testing.expectEqual(cmd.CommandResultCode.success, result);

    const matching_path = try core.defaultPersistenceSnapshotPath(allocator, 9001, .matching_engine_router, 0);
    defer allocator.free(matching_path);
    const risk_path = try core.defaultPersistenceSnapshotPath(allocator, 9001, .risk_engine, 0);
    defer allocator.free(risk_path);
    const manifest_path = try core.defaultPersistenceSnapshotManifestPath(allocator, 9001);
    defer allocator.free(manifest_path);

    const manifest = try snapshot_mod.validateCommittedPair(
        allocator,
        std.Io.Dir.cwd(),
        manifest_path,
        matching_path,
        risk_path,
    );
    const matching = try snapshot_mod.readMatchingPayloadSummary(std.Io.Dir.cwd(), matching_path);
    const risk_state = try snapshot_mod.readRiskPayloadSummary(std.Io.Dir.cwd(), risk_path);
    try std.testing.expectEqual(@as(u64, 9001), manifest.snapshot_id);
    try std.testing.expectEqual(manifest.matching_seq + 1, manifest.risk_seq);
    try std.testing.expectEqual(manifest.risk_seq, manifest.next_seq);
    try std.testing.expectEqual(manifest.matching_seq, matching.metadata.command_seq);
    try std.testing.expectEqual(manifest.risk_seq, risk_state.metadata.command_seq);
    try std.testing.expectEqual(@as(u32, 1), matching.pending_transfer_count);
    try std.testing.expect((try core.persistenceSnapshotById(9001)) != null);

    var restored = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.fromSnapshotOnly("EXPAIR", 9001, 0),
        .serialization = .{ .backend = .disk, .storage_folder = storage_folder },
    });
    defer restored.deinit();
    const outcome = try restored.bootstrapFromPersistence(
        std.Io.Dir.cwd(),
        matching_path,
        null,
    );
    try std.testing.expect(outcome.snapshot_found);
    try std.testing.expectEqual(manifest.next_seq, restored.kernelRef().next_seq);
    try std.testing.expectEqual(@as(usize, 1), restored.kernelRef().binary_frame_assembler.pendingTransfers());
    var restored_balance = try restored.queryReport(.{ .user_balance = .{ .uid = 90_001, .currency = 15 } });
    defer restored_balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 12_345), restored_balance.user_balance);

    var final_frame: cmd.OrderCommand = .{
        .command = .binary_data_command,
        .order_id = 6,
        .price = 7,
        .reserve_bid_price = 8,
        .size = 9,
        .uid = 10,
        .symbol = -1,
        .user_cookie = 77,
        .result_code = .new,
    };
    var final_response = try restored.submitCommand(&final_frame);
    defer final_response.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), restored.kernelRef().binary_frame_assembler.pendingTransfers());
}

test "exchange core persistence fault campaign never advertises a failed pair and retries" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var storage_folder_buf: [256]u8 = undefined;
    const storage_folder = try std.fmt.bufPrint(
        &storage_folder_buf,
        ".zig-cache/tmp/{s}/durable-faults",
        .{tmp.sub_path},
    );
    var core = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXFAULT"),
        .serialization = .{ .backend = .disk, .storage_folder = storage_folder },
    });
    defer core.deinit();

    const faults = [_]PersistenceFaultPoint{
        .matching_before_write,
        .matching_after_rename,
        .risk_before_write,
        .risk_after_rename,
        .manifest_before_write,
    };
    for (faults, 0..) |fault, index| {
        const snapshot_id: u64 = 10_000 + index;
        core.setPersistenceFaultForTesting(fault);
        const failed = try core.persistState(@intCast(snapshot_id), @intCast(50_000 + index));
        const expected_failure: cmd.CommandResultCode = switch (fault) {
            .matching_before_write, .matching_after_rename => .state_persist_matching_engine_failed,
            .risk_before_write, .risk_after_rename, .manifest_before_write => .state_persist_risk_engine_failed,
        };
        try std.testing.expectEqual(expected_failure, failed);

        const manifest_path = try core.defaultPersistenceSnapshotManifestPath(allocator, snapshot_id);
        defer allocator.free(manifest_path);
        try std.testing.expect(!(try core.snapshotExistsFile(std.Io.Dir.cwd(), manifest_path)));
        try std.testing.expect((try core.persistenceSnapshotById(snapshot_id)) == null);

        const retry = try core.persistState(@intCast(snapshot_id), @intCast(50_000 + index));
        try std.testing.expectEqual(cmd.CommandResultCode.success, retry);
        try std.testing.expect(try core.snapshotExistsFile(std.Io.Dir.cwd(), manifest_path));
        try std.testing.expect((try core.persistenceSnapshotById(snapshot_id)) != null);
    }
}

test "exchange core snapshot discovery ignores orphan durable payloads without a manifest" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var storage_folder_buf: [256]u8 = undefined;
    const storage_folder = try std.fmt.bufPrint(
        &storage_folder_buf,
        ".zig-cache/tmp/{s}/durable-discovery",
        .{tmp.sub_path},
    );
    var core = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXDISCOVER"),
        .serialization = .{ .backend = .disk, .storage_folder = storage_folder },
    });
    defer core.deinit();

    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try core.persistState(20_001, 60_001),
    );
    core.setPersistenceFaultForTesting(.risk_after_rename);
    try std.testing.expectEqual(
        cmd.CommandResultCode.state_persist_risk_engine_failed,
        try core.persistState(20_002, 60_002),
    );

    try std.testing.expectEqual(
        @as(?u64, 20_001),
        try core.discoverLatestDefaultSnapshotId(
            std.Io.Dir.cwd(),
            .matching_engine_router,
            0,
        ),
    );
    try std.testing.expectEqual(
        @as(?u64, 20_001),
        try core.discoverLatestDefaultSnapshotId(
            std.Io.Dir.cwd(),
            .risk_engine,
            0,
        ),
    );
}

test "exchange core config validation fails before initialization for bad runtime config" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(
        error.InvalidBatchSize,
        ExchangeCore.init(allocator, .{
            .runtime = .{
                .max_batch_size = 0,
            },
        }),
    );
}

test "exchange core accepts java default configuration profile" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, Config.javaDefault());
    defer core.deinit();

    var add = try core.addUser(9101);
    defer add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add.result_code);
    const cfg = core.configRef();
    try std.testing.expect(cfg.logging.isEnabled(.warnings));
    try std.testing.expect(cfg.reports_queries.supportsReport(.state_hash));
    try std.testing.expectEqual(config_mod.SerializationBackend.dummy, cfg.serialization.backend);
}

test "exchange core persistence path helpers follow Java file naming layout" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXPATH"),
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer core.deinit();

    const snapshot_name = try core.defaultPersistenceSnapshotFileName(
        allocator,
        42,
        .risk_engine,
        1,
    );
    defer allocator.free(snapshot_name);
    try std.testing.expectEqualStrings("EXPATH_snapshot_42_RE1.ecs", snapshot_name);

    const snapshot_matching_name = try core.defaultPersistenceSnapshotFileName(
        allocator,
        42,
        .matching_engine_router,
        0,
    );
    defer allocator.free(snapshot_matching_name);
    try std.testing.expectEqualStrings("EXPATH_snapshot_42_ME0.ecs", snapshot_matching_name);

    const journal_name = try core.defaultPersistenceJournalFileName(allocator, 42, 0x000F);
    defer allocator.free(journal_name);
    try std.testing.expectEqualStrings("EXPATH_journal_42_000F.ecj", journal_name);

    const main_log_name = try core.defaultPersistenceMainLogFileName(allocator);
    defer allocator.free(main_log_name);
    try std.testing.expectEqualStrings("EXPATH.eca", main_log_name);

    const snapshot_path = try core.defaultPersistenceSnapshotPath(
        allocator,
        42,
        .risk_engine,
        1,
    );
    defer allocator.free(snapshot_path);
    try std.testing.expect(std.mem.startsWith(u8, snapshot_path, config_mod.DefaultDiskStorageFolder));
    try std.testing.expect(std.mem.endsWith(u8, snapshot_path, snapshot_name));

    const journal_path = try core.defaultPersistenceJournalPath(allocator, 42, 0x000F);
    defer allocator.free(journal_path);
    try std.testing.expect(std.mem.startsWith(u8, journal_path, config_mod.DefaultDiskStorageFolder));
    try std.testing.expect(std.mem.endsWith(u8, journal_path, journal_name));

    const main_log_path = try core.defaultPersistenceMainLogPath(allocator);
    defer allocator.free(main_log_path);
    try std.testing.expect(std.mem.startsWith(u8, main_log_path, config_mod.DefaultDiskStorageFolder));
    try std.testing.expect(std.mem.endsWith(u8, main_log_path, main_log_name));
}

test "exchange core initializes from java exchange config adapter" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initFromJavaConfig(allocator, .{
        .performance_cfg = config_mod.PerformanceConfig.latencyPreset(),
    });
    defer core.deinit();

    const cfg = core.configRef();
    try std.testing.expectEqual(kernel_mod.OrderBookKind.direct, cfg.matching.orderbook_kind);
    try std.testing.expectEqual(@as(usize, 256), cfg.runtime.max_batch_size);
    try std.testing.expectEqual(runtime_mod.DefaultL2PublishIntervalNs, cfg.runtime.l2_publish_interval_ns);
    try std.testing.expectEqual(@as(u64, 10_000), cfg.runtime.max_group_duration_ns);
}

test "exchange core rejects Java multi-engine profiles instead of collapsing topology" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(
        error.SingleCoreRequiresOneMatchingEngine,
        ExchangeCore.initFromJavaConfig(allocator, .{
            .performance_cfg = config_mod.PerformanceConfig.throughputPreset(),
        }),
    );

    var risk_heavy = config_mod.JavaExchangeConfig.javaDefault();
    risk_heavy.performance_cfg.risk_engines_num = 2;
    try std.testing.expectError(
        error.SingleCoreRequiresOneRiskEngine,
        ExchangeCore.initFromJavaConfig(allocator, risk_heavy),
    );
}

test "exchange core applies report query capability gating from config" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .reports_queries = .{
            .enable_state_hash_report = false,
            .enable_single_user_report = true,
            .enable_total_currency_balance_report = true,
        },
    });
    defer core.deinit();

    try std.testing.expectError(
        error.UnsupportedReportQueryType,
        core.queryReport(.risk_state_hash),
    );

    var single_user = try core.queryReport(.{ .single_user = 9999 });
    defer single_user.deinit(allocator);
    switch (single_user) {
        .single_user => |report| {
            try std.testing.expectEqual(kernel_mod.SingleUserQueryExecutionStatus.user_not_found, report.query_execution_status);
        },
        else => unreachable,
    }
}

test "exchange core maps binary command capability flags into kernel config" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .reports_queries = .{
            .enable_add_accounts_binary = false,
            .enable_add_symbols_binary = true,
        },
    });
    defer core.deinit();

    const kernel_cfg = core.kernelRef().report_queries_cfg;
    try std.testing.expect(!kernel_cfg.add_accounts_binary);
    try std.testing.expect(kernel_cfg.add_symbols_binary);
}

test "exchange core sync journaling records commands and supports file roundtrip" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer core.deinit();

    try std.testing.expect(core.journalingEnabled());
    try std.testing.expectEqual(@as(usize, 0), try core.journalLen());

    var add_user = try core.addUser(9201);
    defer add_user.deinit(allocator);
    var deposit = try core.adjustUserBalance(9201, 1, 15, 7_000, .adjustment);
    defer deposit.deinit(allocator);

    var c1 = makeAddUserCommand(9202);
    var c2 = makeAddUserCommand(9202);
    var batch_items = [_]*cmd.OrderCommand{ &c1, &c2 };
    const batch = try core.submitCommandBatch(batch_items[0..]);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, batch.result_code);

    try std.testing.expectEqual(@as(usize, 4), try core.journalLen());

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try core.writeJournalToFile(tmp.dir, "core.journal");

    var replay_journal = try journal_mod.CommandJournal.readFromFile(allocator, tmp.dir, "core.journal");
    defer replay_journal.deinit();
    try std.testing.expectEqual(@as(usize, 4), replay_journal.len());

    var replay_kernel = try kernel_mod.ExchangeKernel.init(allocator, core.configRef().toKernelConfig());
    defer replay_kernel.deinit();
    try replay_journal.replayIntoKernel(&replay_kernel);

    var replayed_balance = try replay_kernel.queryReport(.{
        .user_balance = .{
            .uid = 9201,
            .currency = 15,
        },
    });
    defer replayed_balance.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 7_000), replayed_balance.user_balance);

    var reloaded_core = try ExchangeCore.init(allocator, .{
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer reloaded_core.deinit();
    try std.testing.expectEqual(@as(usize, 0), try reloaded_core.journalLen());
    try reloaded_core.loadJournalFromFile(tmp.dir, "core.journal");
    try std.testing.expectEqual(@as(usize, 4), try reloaded_core.journalLen());
}

test "exchange core default-path journal wrappers support write/load/replay flows" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXJDEF"),
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer source.deinit();

    var add_user = makeAddUserCommand(9911);
    add_user.timestamp = 10;
    var add_response = try source.submitCommand(&add_user);
    defer add_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_response.result_code);
    try std.testing.expectEqual(@as(usize, 1), try source.journalLen());

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try source.writeJournalToDefaultPath(tmp.dir, 0, 1);
    var dumps_dir = try tmp.dir.openDir(io_context.current(), config_mod.DefaultDiskStorageFolder, .{});
    dumps_dir.close(io_context.current());

    var loaded = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXJDEF"),
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer loaded.deinit();
    try loaded.loadJournalFromDefaultPath(tmp.dir, 0, 1);
    try std.testing.expectEqual(@as(usize, 1), try loaded.journalLen());

    var replay = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.lastKnownStateFromJournal("EXJDEF", 0, 0),
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer replay.deinit();

    const replayed = try replay.replayJournalFromDefaultPath(tmp.dir, 0, 1);
    try std.testing.expectEqual(@as(usize, 1), replayed);

    var user_report = try replay.queryReport(.{ .single_user = 9911 });
    defer user_report.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        user_report.single_user.query_execution_status,
    );
}

test "exchange core custom registry rejects legacy capture and persistence before artifacts" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const storage_folder = "legacy-custom-persistence-guard";
    var core = try ExchangeCore.initWithPersistenceRoot(allocator, tmp.dir, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXCUSTOMLEGACY"),
        .reports_queries = .{
            .custom_report_registry = LegacySnapshotGuardRegistry,
        },
        .serialization = .{
            .backend = .disk,
            .storage_folder = storage_folder,
        },
    });
    defer core.deinit();

    var persist_matching = makePersistStateMatchingCommand(70_501, 80_501);
    try std.testing.expectError(
        error.LegacySnapshotCustomReportRegistryUnsupported,
        core.stageMatchingPersistence(&persist_matching, 1),
    );
    try std.testing.expect(core.pending_persist_pair == null);

    const matching_path = try core.defaultPersistenceSnapshotPath(
        allocator,
        70_501,
        .matching_engine_router,
        0,
    );
    defer allocator.free(matching_path);
    const risk_path = try core.defaultPersistenceSnapshotPath(
        allocator,
        70_501,
        .risk_engine,
        0,
    );
    defer allocator.free(risk_path);
    const manifest_path = try core.defaultPersistenceSnapshotManifestPath(
        allocator,
        70_501,
    );
    defer allocator.free(manifest_path);
    try std.testing.expect(!try core.snapshotExistsFile(tmp.dir, matching_path));
    try std.testing.expect(!try core.snapshotExistsFile(tmp.dir, risk_path));
    try std.testing.expect(!try core.snapshotExistsFile(tmp.dir, manifest_path));
    try std.testing.expectError(
        error.FileNotFound,
        tmp.dir.access(io_context.current(), storage_folder, .{}),
    );

    try std.testing.expectError(
        error.LegacySnapshotCustomReportRegistryUnsupported,
        core.captureSnapshotToFile(tmp.dir, "custom-registry.xcs", &.{}, 1),
    );
    try std.testing.expect(!try core.snapshotExistsFile(tmp.dir, "custom-registry.xcs"));
    try std.testing.expect(core.fatalInfo() == null);
}

test "exchange core custom registry rejects legacy monolithic bootstrap without poisoning" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var source = try ExchangeCore.init(allocator, .{});
    defer source.deinit();
    try source.captureSnapshotToFile(tmp.dir, "legacy.xcs", &.{}, 1);

    var target = try ExchangeCore.initWithPersistenceRoot(allocator, tmp.dir, .{
        .initial_state = config_mod.InitialStateConfig.fromSnapshotOnly(
            "EXCUSTOMMONO",
            70_502,
            99,
        ),
        .reports_queries = .{
            .custom_report_registry = LegacySnapshotGuardRegistry,
        },
        .serialization = .{
            .backend = .disk,
            .storage_folder = "legacy-custom-monolithic-bootstrap",
        },
    });
    defer target.deinit();

    try std.testing.expectError(
        error.LegacySnapshotCustomReportRegistryUnsupported,
        target.bootstrapFromPersistence(tmp.dir, "legacy.xcs", null),
    );
    try std.testing.expectEqual(@as(u64, 0), target.kernelRef().next_seq);
    try std.testing.expect(target.fatalInfo() == null);
}

test "exchange core custom registry rejects legacy manifest bootstrap without poisoning" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_id: u64 = 70_503;
    const storage_folder = "legacy-custom-manifest-bootstrap";
    var source = try ExchangeCore.initWithPersistenceRoot(allocator, tmp.dir, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXCUSTOMPAIR"),
        .serialization = .{
            .backend = .disk,
            .storage_folder = storage_folder,
        },
    });
    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try source.persistState(snapshot_id, 80_503),
    );
    const matching_path = try source.defaultPersistenceSnapshotPath(
        allocator,
        snapshot_id,
        .matching_engine_router,
        0,
    );
    defer allocator.free(matching_path);
    const manifest_path = try source.defaultPersistenceSnapshotManifestPath(
        allocator,
        snapshot_id,
    );
    defer allocator.free(manifest_path);
    try std.testing.expect(try source.snapshotExistsFile(tmp.dir, manifest_path));
    source.deinit();

    var target = try ExchangeCore.initWithPersistenceRoot(allocator, tmp.dir, .{
        .initial_state = config_mod.InitialStateConfig.fromSnapshotOnly(
            "EXCUSTOMPAIR",
            snapshot_id,
            0,
        ),
        .reports_queries = .{
            .custom_report_registry = LegacySnapshotGuardRegistry,
        },
        .serialization = .{
            .backend = .disk,
            .storage_folder = storage_folder,
        },
    });
    defer target.deinit();

    try std.testing.expectError(
        error.LegacySnapshotCustomReportRegistryUnsupported,
        target.bootstrapFromPersistence(tmp.dir, matching_path, null),
    );
    try std.testing.expectEqual(@as(u64, 0), target.kernelRef().next_seq);
    try std.testing.expect(target.fatalInfo() == null);
}

test "exchange core default-path snapshot wrappers capture and validate state" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXSDEF"),
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer core.deinit();

    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(241)));

    var add_user = try core.addUser(9921);
    defer add_user.deinit(allocator);
    var deposit = try core.adjustUserBalance(9921, 1, 15, 4_000, .adjustment);
    defer deposit.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try core.captureSnapshotToDefaultPath(
        tmp.dir,
        55,
        .risk_engine,
        0,
        &.{241},
        16,
    );
    var dumps_dir = try tmp.dir.openDir(io_context.current(), config_mod.DefaultDiskStorageFolder, .{});
    dumps_dir.close(io_context.current());
    try core.validateSnapshotDefaultPath(tmp.dir, 55, .risk_engine, 0);

    const main_log_path = try core.defaultPersistenceMainLogPath(allocator);
    defer allocator.free(main_log_path);
    var main_log_file = try tmp.dir.openFile(io_context.current(), main_log_path, .{});
    defer main_log_file.close(io_context.current());
    const main_log_contents = try io_context.readToEndAlloc(main_log_file, allocator, 4096);
    defer allocator.free(main_log_contents);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "seq=") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "timestampNs=") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "snapshotId=55") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "type=RE") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "instance=0") != null);
}

test "exchange core default-path snapshot capture appends .eca main log entries" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXSLOG"),
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer core.deinit();

    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(241)));

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try core.captureSnapshotToDefaultPath(
        tmp.dir,
        61,
        .risk_engine,
        0,
        &.{241},
        16,
    );
    try core.captureSnapshotToDefaultPath(
        tmp.dir,
        62,
        .matching_engine_router,
        1,
        &.{241},
        16,
    );

    const main_log_path = try core.defaultPersistenceMainLogPath(allocator);
    defer allocator.free(main_log_path);
    var main_log_file = try tmp.dir.openFile(io_context.current(), main_log_path, .{});
    defer main_log_file.close(io_context.current());
    const main_log_contents = try io_context.readToEndAlloc(main_log_file, allocator, 8192);
    defer allocator.free(main_log_contents);

    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "snapshotId=61") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "type=RE") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "instance=0") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "snapshotId=62") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "type=ME") != null);
    try std.testing.expect(std.mem.indexOf(u8, main_log_contents, "instance=1") != null);

    var line_count: usize = 0;
    for (main_log_contents) |ch| {
        if (ch == '\n') line_count += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), line_count);
}

test "exchange core snapshot exists helpers detect missing and existing files" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXSEXIST"),
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer core.deinit();

    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(241)));

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try std.testing.expect(!try core.snapshotExistsFile(tmp.dir, "state.snap"));
    try core.captureSnapshotToFile(tmp.dir, "state.snap", &.{241}, 16);
    try std.testing.expect(try core.snapshotExistsFile(tmp.dir, "state.snap"));

    try std.testing.expect(!try core.snapshotExistsDefaultPath(tmp.dir, 77, .risk_engine, 0));
    try core.captureSnapshotToDefaultPath(
        tmp.dir,
        77,
        .risk_engine,
        0,
        &.{241},
        16,
    );
    try std.testing.expect(try core.snapshotExistsDefaultPath(tmp.dir, 77, .risk_engine, 0));
}

test "exchange core journal api reports disabled when journaling config is off" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    try std.testing.expect(!core.journalingEnabled());
    try std.testing.expectEqual(@as(usize, 0), try core.journalLen());

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try std.testing.expectError(
        error.JournalingDisabled,
        core.writeJournalToFile(tmp.dir, "core.journal"),
    );
    try std.testing.expectError(
        error.JournalingDisabled,
        core.loadJournalFromFile(tmp.dir, "core.journal"),
    );
    try std.testing.expectError(
        error.JournalingDisabled,
        core.replayJournalFromFile(tmp.dir, "core.journal"),
    );
}

test "exchange core async drain journaling mode records queued commands" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer core.deinit();

    const req_ok = try core.addUserAsync(9301);
    const req_dup = try core.addUserAsync(9301);
    try std.testing.expectEqual(@as(usize, 2), core.asyncPendingCount());
    try std.testing.expectError(error.CoreNotStarted, core.journalLen());

    const processed = try core.drainAsyncPending();
    try std.testing.expectEqual(@as(usize, 2), processed);
    try std.testing.expectEqual(@as(usize, 0), core.asyncPendingCount());
    try std.testing.expectEqual(@as(usize, 2), try core.journalLen());

    var ok = core.takeAsyncResult(req_ok) orelse unreachable;
    defer ok.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, ok.result_code);

    var dup = core.takeAsyncResult(req_dup) orelse unreachable;
    defer dup.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, dup.result_code);
}

test "exchange core runtime drain journaling mode records queued commands" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .serialization = config_mod.SerializationConfig.diskJournaling(),
        .runtime = .{
            .max_batch_size = 2,
        },
    });
    defer core.deinit();

    try core.enqueueRuntime(makeAddUserCommand(9401));
    try core.enqueueRuntime(makeAddUserCommand(9402));
    try core.enqueueRuntime(makeAddUserCommand(9402));
    try std.testing.expectEqual(@as(usize, 3), core.runtimePendingCount());
    try std.testing.expectError(error.CoreNotStarted, core.journalLen());

    const outcome = try core.drainRuntime();
    try std.testing.expectEqual(@as(usize, 2), outcome.groups_processed);
    try std.testing.expectEqual(@as(usize, 3), outcome.commands_processed);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, outcome.last_merged_result);
    try std.testing.expectEqual(@as(usize, 0), core.runtimePendingCount());
    try std.testing.expectEqual(@as(usize, 3), try core.journalLen());

    var add_again = try core.addUser(9401);
    defer add_again.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, add_again.result_code);
}

test "exchange core runtime drain journaling mode keeps grouping control semantics" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .runtime = .{
            .max_batch_size = 2,
        },
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer core.deinit();

    try core.enqueueRuntime(makeGroupingControlCommand(false));
    try core.enqueueRuntime(makeAddUserCommand(8951));
    try core.enqueueRuntime(makeAddUserCommand(8952));
    try core.enqueueRuntime(makeGroupingControlCommand(true));
    try core.enqueueRuntime(makeAddUserCommand(8953));
    try core.enqueueRuntime(makeAddUserCommand(8954));

    const outcome = try core.drainRuntime();
    try std.testing.expectEqual(@as(usize, 5), outcome.groups_processed);
    try std.testing.expectEqual(@as(usize, 6), outcome.commands_processed);
    try std.testing.expectEqual(cmd.CommandResultCode.success, outcome.last_merged_result);
    try std.testing.expect(core.runtime.grouping_enabled);
    try std.testing.expectEqual(@as(usize, 6), try core.journalLen());
}

test "exchange core runtime drain journaling stamps events_group metadata deterministically" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .runtime = .{
            .max_batch_size = 2,
        },
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer core.deinit();

    try core.enqueueRuntime(makeAddUserCommand(8961));
    try core.enqueueRuntime(makeAddUserCommand(8962));
    try core.enqueueRuntime(makeGroupingControlCommand(false));
    try core.enqueueRuntime(makeAddUserCommand(8963));
    try core.enqueueRuntime(makeAddUserCommand(8964));
    try core.enqueueRuntime(makeGroupingControlCommand(true));
    try core.enqueueRuntime(makeAddUserCommand(8965));
    try core.enqueueRuntime(makePersistStateMatchingCommand(70_001, 11));
    try core.enqueueRuntime(makePersistStateRiskCommand(70_001, 11));
    try core.enqueueRuntime(makeAddUserCommand(8966));

    const outcome = try core.drainRuntime();
    try std.testing.expectEqual(@as(usize, 8), outcome.groups_processed);
    try std.testing.expectEqual(@as(usize, 10), outcome.commands_processed);
    try std.testing.expectEqual(cmd.CommandResultCode.success, outcome.last_merged_result);
    try std.testing.expect(core.runtime.grouping_enabled);
    try std.testing.expectEqual(@as(usize, 10), try core.journalLen());

    const journal = core.journal orelse return error.ExpectedJournal;
    const expected_groups = [_]i64{ 0, 0, 0, 0, 0, 2, 2, 4, 4, 4 };
    for (journal.entries.items, 0..) |entry, idx| {
        try std.testing.expectEqual(expected_groups[idx], entry.input.events_group);
        try std.testing.expectEqual(@as(i32, 0), entry.input.service_flags);
    }
}

test "exchange core replay journal honors initial-state timestamp limit" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer source.deinit();

    var c1 = makeAddUserCommand(9501);
    c1.timestamp = 10;
    var c2 = makeAddUserCommand(9502);
    c2.timestamp = 20;
    var c3 = makeAddUserCommand(9503);
    c3.timestamp = 40;
    var r1 = try source.submitCommand(&c1);
    defer r1.deinit(allocator);
    var r2 = try source.submitCommand(&c2);
    defer r2.deinit(allocator);
    var r3 = try source.submitCommand(&c3);
    defer r3.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 3), try source.journalLen());

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try source.writeJournalToFile(tmp.dir, "bootstrap.journal");

    var replay = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXBOOT",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = 25,
            .throw_if_snapshot_not_found = false,
        },
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer replay.deinit();

    const replayed = try replay.replayJournalFromFile(tmp.dir, "bootstrap.journal");
    try std.testing.expectEqual(@as(usize, 2), replayed);
    try std.testing.expectEqual(@as(usize, 2), try replay.journalLen());

    var user1_report = try replay.queryReport(.{ .single_user = 9501 });
    defer user1_report.deinit(allocator);
    var user2_report = try replay.queryReport(.{ .single_user = 9502 });
    defer user2_report.deinit(allocator);
    var user3_report = try replay.queryReport(.{ .single_user = 9503 });
    defer user3_report.deinit(allocator);

    try std.testing.expectEqual(kernel_mod.SingleUserQueryExecutionStatus.ok, user1_report.single_user.query_execution_status);
    try std.testing.expectEqual(kernel_mod.SingleUserQueryExecutionStatus.ok, user2_report.single_user.query_execution_status);
    try std.testing.expectEqual(kernel_mod.SingleUserQueryExecutionStatus.user_not_found, user3_report.single_user.query_execution_status);
}

test "exchange core replay registers supplied terminal persistence snapshot" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var native = journal_mod.CommandJournal.init(allocator);
    defer native.deinit();
    const matching = makePersistStateMatchingCommand(77_001, 765_432);
    const risk = makePersistStateRiskCommand(77_001, 765_432);
    try native.entries.append(allocator, .{
        .sequence = 1,
        .input = matching,
        .result_code = .accepted,
    });
    try native.entries.append(allocator, .{
        .sequence = 2,
        .input = risk,
        .result_code = .success,
    });
    try native.writeToFile(tmp.dir, "supplied-persist-pair.xcj");

    var replay = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.lastKnownStateFromJournal(
            "EXPERSISTREPLAY",
            0,
            0,
        ),
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer replay.deinit();

    try std.testing.expectEqual(
        @as(usize, 2),
        try replay.replayJournalFromFile(tmp.dir, "supplied-persist-pair.xcj"),
    );
    try std.testing.expectEqual(@as(usize, 2), try replay.persistenceSnapshotCount());
    const descriptor = try replay.persistenceCurrentSnapshot();
    try std.testing.expectEqual(@as(u64, 77_001), descriptor.snapshot_id);
    try std.testing.expectEqual(@as(u64, 2), descriptor.seq);
    try std.testing.expectEqual(@as(u64, 765_432), descriptor.timestamp_ns);
}

test "exchange core bootstrap policy replays journal on clean-start journal profile" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer source.deinit();

    var c1 = makeAddUserCommand(9601);
    c1.timestamp = 10;
    var c2 = makeAddUserCommand(9602);
    c2.timestamp = 20;
    var r1 = try source.submitCommand(&c1);
    defer r1.deinit(allocator);
    var r2 = try source.submitCommand(&c2);
    defer r2.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try source.writeJournalToFile(tmp.dir, "bootstrap.journal");

    var replay = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.lastKnownStateFromJournal("EXBOOT2", 0, 0),
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer replay.deinit();

    const outcome = try replay.bootstrapFromPersistence(
        tmp.dir,
        null,
        "bootstrap.journal",
    );
    try std.testing.expect(!outcome.snapshot_requested);
    try std.testing.expect(!outcome.snapshot_found);
    try std.testing.expectEqual(@as(usize, 2), outcome.journal_replayed_commands);
    try std.testing.expectEqual(@as(usize, 2), try replay.journalLen());

    var user1_report = try replay.queryReport(.{ .single_user = 9601 });
    defer user1_report.deinit(allocator);
    var user2_report = try replay.queryReport(.{ .single_user = 9602 });
    defer user2_report.deinit(allocator);
    try std.testing.expectEqual(kernel_mod.SingleUserQueryExecutionStatus.ok, user1_report.single_user.query_execution_status);
    try std.testing.expectEqual(kernel_mod.SingleUserQueryExecutionStatus.ok, user2_report.single_user.query_execution_status);
}

test "exchange core bootstrap from snapshot replays only post-snapshot journal suffix" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer source.deinit();

    var add_u1 = makeAddUserCommand(9651);
    add_u1.timestamp = 10;
    var r1 = try source.submitCommand(&add_u1);
    defer r1.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 1), r1.sequence);

    var snapshot = try snapshot_mod.KernelSnapshot.captureFromKernel(
        allocator,
        source.kernelRef(),
        &.{},
        8,
    );
    defer snapshot.deinit();

    var add_u2 = makeAddUserCommand(9652);
    add_u2.timestamp = 20;
    var r2 = try source.submitCommand(&add_u2);
    defer r2.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 2), r2.sequence);

    var add_u3 = makeAddUserCommand(9653);
    add_u3.timestamp = 30;
    var r3 = try source.submitCommand(&add_u3);
    defer r3.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 3), r3.sequence);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try snapshot.writeToFile(tmp.dir, "state.snap");
    try source.writeJournalToFile(tmp.dir, "state.journal");

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXSNAPJRN",
            .snapshot_id = 1,
            .snapshot_base_seq = 1,
            .journal_timestamp_ns = std.math.maxInt(u64),
            .throw_if_snapshot_not_found = true,
        },
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer target.deinit();

    const outcome = try target.bootstrapFromPersistence(
        tmp.dir,
        "state.snap",
        "state.journal",
    );
    try std.testing.expect(outcome.snapshot_requested);
    try std.testing.expect(outcome.snapshot_found);
    try std.testing.expectEqual(@as(usize, 2), outcome.journal_replayed_commands);
    try std.testing.expectEqual(@as(usize, 2), try target.journalLen());
    try std.testing.expectEqual(@as(u64, 3), target.kernelRef().next_seq);
    try std.testing.expectEqual(@as(usize, 2), try target.persistenceSnapshotCount());
    try std.testing.expectEqual(@as(usize, 1), try target.persistenceJournalCount());

    const restored_snapshot_desc = try target.persistenceCurrentSnapshot();
    try std.testing.expectEqual(@as(u64, 1), restored_snapshot_desc.snapshot_id);
    try std.testing.expectEqual(@as(u64, 1), restored_snapshot_desc.seq);
    try std.testing.expectEqual(@as(?JournalRef, null), try target.persistenceFindJournalForSequence(1));
    const replay_ref = (try target.persistenceFindJournalForSequence(2)) orelse return error.ExpectedJournal;
    const replay_desc = try target.persistenceJournalByRef(replay_ref);
    try std.testing.expectEqual(@as(u64, 2), replay_desc.seq_first);
    try std.testing.expectEqual(@as(?u64, 3), replay_desc.seq_last);
    try std.testing.expectEqual(try target.persistenceCurrentSnapshotRef(), replay_desc.base_snapshot);

    var user1_report = try target.queryReport(.{ .single_user = 9651 });
    defer user1_report.deinit(allocator);
    var user2_report = try target.queryReport(.{ .single_user = 9652 });
    defer user2_report.deinit(allocator);
    var user3_report = try target.queryReport(.{ .single_user = 9653 });
    defer user3_report.deinit(allocator);

    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        user1_report.single_user.query_execution_status,
    );
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        user2_report.single_user.query_execution_status,
    );
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        user3_report.single_user.query_execution_status,
    );
}

test "exchange core bootstrap policy missing snapshot obeys throw-if-not-found" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var required_snapshot = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.fromSnapshotOnly("EXSNAP", 7, 100),
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer required_snapshot.deinit();

    try std.testing.expectError(
        error.SnapshotFileNotFound,
        required_snapshot.bootstrapFromPersistence(tmp.dir, "missing.snap", null),
    );
    try std.testing.expectError(
        error.SnapshotPathRequired,
        required_snapshot.bootstrapFromPersistence(tmp.dir, null, null),
    );

    var optional_snapshot = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXSNAPOPT",
            .snapshot_id = 9,
            .snapshot_base_seq = 100,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = false,
        },
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer optional_snapshot.deinit();

    const outcome = try optional_snapshot.bootstrapFromPersistence(
        tmp.dir,
        "missing.snap",
        null,
    );
    try std.testing.expect(outcome.snapshot_requested);
    try std.testing.expect(!outcome.snapshot_found);
    try std.testing.expectEqual(@as(usize, 0), outcome.journal_replayed_commands);
}

test "exchange core bootstrap policy restores snapshot file when present" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer source.deinit();
    try std.testing.expect(try source.kernelRef().addSymbol(try makeExchangeSpecForSymbol(241)));

    var add_user = try source.addUser(9701);
    defer add_user.deinit(allocator);
    var deposit = try source.adjustUserBalance(9701, 1, 15, 6_000, .adjustment);
    defer deposit.deinit(allocator);

    var snapshot = try snapshot_mod.KernelSnapshot.captureFromKernel(
        allocator,
        source.kernelRef(),
        &.{241},
        8,
    );
    defer snapshot.deinit();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try snapshot.writeToFile(tmp.dir, "state.snap");

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.fromSnapshotOnly("EXSNAPLOAD", 11, 777),
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer target.deinit();

    const outcome = try target.bootstrapFromPersistence(tmp.dir, "state.snap", null);
    try std.testing.expect(outcome.snapshot_requested);
    try std.testing.expect(outcome.snapshot_found);
    try std.testing.expectEqual(@as(usize, 0), outcome.journal_replayed_commands);
    try std.testing.expectEqual(@as(u64, 777), target.kernelRef().next_seq);

    var user_report = try target.queryReport(.{ .single_user = 9701 });
    defer user_report.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        user_report.single_user.query_execution_status,
    );
}

test "exchange core snapshot file APIs capture and validate kernel state" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
    });
    defer core.deinit();

    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(241)));

    var add_user = try core.addUser(9801);
    defer add_user.deinit(allocator);
    var deposit = try core.adjustUserBalance(9801, 1, 15, 5_000, .adjustment);
    defer deposit.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try core.captureSnapshotToFile(tmp.dir, "core.snap", &.{241}, 16);
    try core.validateSnapshotFile(tmp.dir, "core.snap");

    var mutate = try core.addUser(9802);
    defer mutate.deinit(allocator);
    try std.testing.expectError(
        error.RiskHashMismatch,
        core.validateSnapshotFile(tmp.dir, "core.snap"),
    );
}

test "exchange core lifecycle startup and shutdown flags are deterministic" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    try std.testing.expect(!core.isStarted());
    try std.testing.expect(!core.isStopped());

    try core.startup();
    try std.testing.expect(core.isStarted());
    try std.testing.expect(!core.isStopped());

    try core.startup();
    try std.testing.expect(core.isStarted());
    try std.testing.expect(!core.isStopped());

    core.shutdown();
    try std.testing.expect(core.isStopped());
    try std.testing.expectError(error.CoreStopped, core.startup());
}

test "exchange core running worker progresses async commands without manual drain" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();
    try core.startup();

    const request_id = try core.addUserAsync(98_101);
    var barrier = try core.nop();
    defer barrier.deinit(allocator);
    var response = core.takeAsyncResult(request_id) orelse return error.MissingAsyncResult;
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
}

test "exchange core live persistence APIs wait for the accepted prefix" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var storage_folder_buf: [std.Io.Dir.max_path_bytes]u8 = undefined;
    const storage_folder = try std.fmt.bufPrint(
        &storage_folder_buf,
        ".zig-cache/tmp/{s}/live-persistence-prefix",
        .{tmp.sub_path},
    );

    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXLIVEPREFIX"),
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = storage_folder,
        },
    });
    defer core.deinit();
    try core.startup();

    // Keep the worker inside the first callback while a second accepted
    // command remains unexecuted behind it. The journal writer must wait for
    // both commands before taking its execution-locked snapshot.
    var journal_gate: CommandCallbackGate = .{ .io = std.testing.io };
    try core.submitApiCommandWithCallback(.{ .add_user = 98_111 }, .{
        .context = &journal_gate,
        .callback = CommandCallbackGate.onResponse,
    });
    journal_gate.waitEntered();
    const journal_tail = try core.addUserAsync(98_112);

    var journal_runner: PersistenceBarrierRunner = .{
        .core = &core,
        .dir = tmp.dir,
        .io = std.testing.io,
        .operation = .{ .write_journal = "accepted-prefix.journal" },
    };
    const journal_thread = try std.Thread.spawn(.{}, PersistenceBarrierRunner.run, .{&journal_runner});
    journal_runner.waitEntered();
    const journal_blocked = !journal_runner.isDone();
    journal_gate.release();
    journal_thread.join();
    try std.testing.expect(journal_blocked);
    try std.testing.expect(journal_runner.failure == null);

    var journal_tail_response = core.takeAsyncResult(journal_tail) orelse return error.MissingAsyncResult;
    defer journal_tail_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, journal_tail_response.result_code);
    var persisted_journal = try journal_mod.CommandJournal.readFromFile(
        allocator,
        tmp.dir,
        "accepted-prefix.journal",
    );
    defer persisted_journal.deinit();
    try std.testing.expectEqual(@as(usize, 2), persisted_journal.len());
    try std.testing.expectEqual(@as(i64, 98_112), persisted_journal.entries.items[1].input.uid);

    // Snapshot capture observes the same boundary: the trailing command is
    // still queued when capture starts, yet the completed snapshot validates
    // against the state after that command executes.
    var capture_gate: CommandCallbackGate = .{ .io = std.testing.io };
    try core.submitApiCommandWithCallback(.{ .add_user = 98_113 }, .{
        .context = &capture_gate,
        .callback = CommandCallbackGate.onResponse,
    });
    capture_gate.waitEntered();
    const capture_tail = try core.addUserAsync(98_114);

    var capture_runner: PersistenceBarrierRunner = .{
        .core = &core,
        .dir = tmp.dir,
        .io = std.testing.io,
        .operation = .{ .capture_snapshot = "accepted-prefix.snap" },
    };
    const capture_thread = try std.Thread.spawn(.{}, PersistenceBarrierRunner.run, .{&capture_runner});
    capture_runner.waitEntered();
    const capture_blocked = !capture_runner.isDone();
    capture_gate.release();
    capture_thread.join();
    try std.testing.expect(capture_blocked);
    try std.testing.expect(capture_runner.failure == null);

    var capture_tail_response = core.takeAsyncResult(capture_tail) orelse return error.MissingAsyncResult;
    defer capture_tail_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, capture_tail_response.result_code);
    try core.validateSnapshotFile(tmp.dir, "accepted-prefix.snap");

    // A live validation must not validate against a stale pre-prefix state.
    // Block a state-neutral NOP result, queue a mutation behind it, and expect
    // validation of the prior snapshot to observe the mutation and fail.
    var validation_gate: BackpressureGate = .{ .io = std.testing.io };
    core.setResultSink(.{
        .context = &validation_gate,
        .on_result = BackpressureGate.callback,
    });
    const validation_blocker = try core.nopAsync();
    validation_gate.waitEntered();
    const validation_tail = try core.addUserAsync(98_115);

    var validation_runner: PersistenceBarrierRunner = .{
        .core = &core,
        .dir = tmp.dir,
        .io = std.testing.io,
        .operation = .{ .validate_snapshot = "accepted-prefix.snap" },
    };
    const validation_thread = try std.Thread.spawn(.{}, PersistenceBarrierRunner.run, .{&validation_runner});
    validation_runner.waitEntered();
    const validation_blocked = !validation_runner.isDone();
    validation_gate.release();
    validation_thread.join();
    core.setResultSink(.{});
    try std.testing.expect(validation_blocked);
    try std.testing.expectEqual(@as(?anyerror, error.RiskHashMismatch), validation_runner.failure);

    var validation_blocker_response = core.takeAsyncResult(validation_blocker) orelse return error.MissingAsyncResult;
    defer validation_blocker_response.deinit(allocator);
    var validation_tail_response = core.takeAsyncResult(validation_tail) orelse return error.MissingAsyncResult;
    defer validation_tail_response.deinit(allocator);

    // Descriptor observations wait for a queued durable pair and return a
    // pointer-free value after the pair reaches its accepted frontier.
    var descriptor_gate: CommandCallbackGate = .{ .io = std.testing.io };
    try core.submitApiCommandWithCallback(.{ .add_user = 98_116 }, .{
        .context = &descriptor_gate,
        .callback = CommandCallbackGate.onResponse,
    });
    descriptor_gate.waitEntered();
    const persist_requests = try core.persistStateAsync(98_199, 9_819_900);

    var descriptor_runner: PersistenceBarrierRunner = .{
        .core = &core,
        .dir = tmp.dir,
        .io = std.testing.io,
        .operation = .{ .snapshot_by_id = 98_199 },
    };
    const descriptor_thread = try std.Thread.spawn(.{}, PersistenceBarrierRunner.run, .{&descriptor_runner});
    descriptor_runner.waitEntered();
    const descriptor_blocked = !descriptor_runner.isDone();
    descriptor_gate.release();
    descriptor_thread.join();
    try std.testing.expect(descriptor_blocked);
    try std.testing.expect(descriptor_runner.failure == null);

    const persisted_result = core.takePersistStateResult(persist_requests) orelse return error.MissingAsyncResult;
    try std.testing.expectEqual(cmd.CommandResultCode.success, persisted_result);
    const descriptor_view = descriptor_runner.snapshot orelse return error.ExpectedSnapshot;
    try std.testing.expectEqual(@as(u64, 98_199), descriptor_view.snapshot_id);
    try std.testing.expectEqual(@as(u64, 9_819_900), descriptor_view.timestamp_ns);
    try std.testing.expect(descriptor_view.prev != null);
    try std.testing.expectEqual(@as(?SnapshotRef, null), descriptor_view.next);
    try std.testing.expectEqual(@as(u32, 1), descriptor_view.num_matching_engines);
    try std.testing.expectEqual(@as(u32, 1), descriptor_view.num_risk_engines);
    try std.testing.expectEqual(@as(usize, 0), descriptor_view.journal_count);
}

test "exchange core running load and replay reject without mutation" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var source = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXRUNREJECT"),
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer source.deinit();
    var source_filler = try source.addUser(98_210);
    defer source_filler.deinit(allocator);
    var source_response = try source.addUser(98_211);
    defer source_response.deinit(allocator);
    try source.writeJournalToFile(tmp.dir, "running-reject.journal");
    try source.writeJournalToDefaultPath(tmp.dir, 0, 1);

    var target = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXRUNREJECT"),
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer target.deinit();
    try target.startup();
    var target_response = try target.addUser(98_212);
    defer target_response.deinit(allocator);

    const next_seq_before = target.kernelRef().next_seq;
    const risk_hash_before = try queryRiskStateHash(&target, allocator);
    const journal_len_before = try target.journalLen();
    const snapshot_count_before = try target.persistenceSnapshotCount();
    const journal_count_before = try target.persistenceJournalCount();
    const snapshot_before = try target.persistenceCurrentSnapshot();

    try std.testing.expectError(
        error.CoreAlreadyStarted,
        target.loadJournalFromFile(tmp.dir, "running-reject.journal"),
    );
    try std.testing.expectError(
        error.CoreAlreadyStarted,
        target.loadJournalFromDefaultPath(tmp.dir, 0, 1),
    );
    try std.testing.expectError(
        error.CoreAlreadyStarted,
        target.replayJournalFromFile(tmp.dir, "running-reject.journal"),
    );
    try std.testing.expectError(
        error.CoreAlreadyStarted,
        target.replayJournalFromDefaultPath(tmp.dir, 0, 1),
    );
    try std.testing.expectError(
        error.CoreAlreadyStarted,
        target.replayJournalFromDefaultPathRange(tmp.dir, 0, 1),
    );

    try std.testing.expectEqual(next_seq_before, target.kernelRef().next_seq);
    try std.testing.expectEqual(risk_hash_before, try queryRiskStateHash(&target, allocator));
    try std.testing.expectEqual(journal_len_before, try target.journalLen());
    try std.testing.expectEqual(snapshot_count_before, try target.persistenceSnapshotCount());
    try std.testing.expectEqual(journal_count_before, try target.persistenceJournalCount());
    try std.testing.expectEqual(snapshot_before, try target.persistenceCurrentSnapshot());

    var source_user = try target.queryReport(.{ .single_user = 98_211 });
    defer source_user.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.user_not_found,
        source_user.single_user.query_execution_status,
    );
    var target_user = try target.queryReport(.{ .single_user = 98_212 });
    defer target_user.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        target_user.single_user.query_execution_status,
    );
}

test "exchange core running runtime drain reports the completed undrained prefix" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .runtime = .{ .max_batch_size = 4 },
    });
    defer core.deinit();
    try core.startup();

    try core.enqueueRuntime(makeAddUserCommand(98_151));
    try core.enqueueRuntime(makeAddUserCommand(98_151));

    // Force both detached commands to complete before the first drain call.
    // The drain must still report the undrained completed prefix instead of
    // starting its count at `runtime_completed_total` and returning zero.
    var barrier = try core.nop();
    defer barrier.deinit(allocator);

    const first = try core.drainRuntime();
    try std.testing.expectEqual(@as(usize, 1), first.groups_processed);
    try std.testing.expectEqual(@as(usize, 2), first.commands_processed);
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_user_already_exists,
        first.last_merged_result,
    );

    const second = try core.drainRuntime();
    try std.testing.expectEqual(@as(usize, 0), second.groups_processed);
    try std.testing.expectEqual(@as(usize, 0), second.commands_processed);
    try std.testing.expectEqual(cmd.CommandResultCode.accepted, second.last_merged_result);
}

test "exchange core pre-start async queue begins in FIFO order after startup" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();

    const first = try core.addUserAsync(98_201);
    const duplicate = try core.addUserAsync(98_201);
    try std.testing.expectEqual(@as(usize, 2), core.asyncPendingCount());
    try core.startup();
    var barrier = try core.nop();
    defer barrier.deinit(allocator);

    var first_response = core.takeAsyncResult(first) orelse return error.MissingAsyncResult;
    defer first_response.deinit(allocator);
    var duplicate_response = core.takeAsyncResult(duplicate) orelse return error.MissingAsyncResult;
    defer duplicate_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, first_response.result_code);
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_user_already_exists,
        duplicate_response.result_code,
    );
}

test "exchange core graceful shutdown drains accepted work and rejects later work" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();
    try core.startup();

    const accepted = try core.addUserAsync(98_301);
    core.shutdown();
    var response = core.takeAsyncResult(accepted) orelse return error.MissingAsyncResult;
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    try std.testing.expectError(error.CoreStopped, core.addUserAsync(98_302));
}

test "exchange core graceful shutdown waits for accepted request callback delivery" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();
    try core.startup();

    var gate: CommandCallbackGate = .{ .io = std.testing.io };
    try core.submitApiCommandWithCallback(.{ .add_user = 98_311 }, .{
        .context = &gate,
        .callback = CommandCallbackGate.onResponse,
    });
    gate.waitEntered();

    var shutdown_runner: ShutdownCompletionRunner = .{
        .core = &core,
        .io = std.testing.io,
    };
    const shutdown_thread = try std.Thread.spawn(.{}, ShutdownCompletionRunner.run, .{&shutdown_runner});
    shutdown_runner.waitEntered();
    try std.testing.expect(!shutdown_runner.isDone());

    gate.release();
    shutdown_thread.join();
    try std.testing.expect(shutdown_runner.isDone());
    try std.testing.expectEqual(@as(usize, 0), core.pendingCallbackCount());
    try std.testing.expect(core.kernelRef().risk_engine.getUserProfile(98_311) != null);
}

test "exchange core bounded running shutdown times out then concurrent joiners converge" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();
    try core.startup();

    var gate: CommandCallbackGate = .{ .io = std.testing.io };
    defer gate.release();
    try core.submitApiCommandWithCallback(.{ .add_user = 98_313 }, .{
        .context = &gate,
        .callback = CommandCallbackGate.onResponse,
    });
    gate.waitEntered();

    const immediate: std.Io.Timeout = .{
        .duration = .{ .raw = .zero, .clock = .awake },
    };
    var timed_runners = [_]BoundedShutdownRunner{
        .{ .core = &core, .timeout = immediate },
        .{ .core = &core, .timeout = immediate },
    };
    var timed_threads: [timed_runners.len]std.Thread = undefined;
    for (&timed_threads, &timed_runners) |*thread, *runner| {
        thread.* = try std.Thread.spawn(.{}, BoundedShutdownRunner.run, .{runner});
    }
    for (&timed_threads) |thread| thread.join();
    for (&timed_runners) |*runner| {
        try std.testing.expectError(error.Timeout, runner.result.?);
    }

    try std.testing.expectEqual(ShutdownState.closing, core.shutdownState());
    try std.testing.expect(core.isStopped());
    try std.testing.expectError(error.CoreStopped, core.addUserAsync(98_314));

    gate.release();
    var join_runners = [_]BoundedShutdownRunner{
        .{ .core = &core, .timeout = .none },
        .{ .core = &core, .timeout = .none },
    };
    var join_threads: [join_runners.len]std.Thread = undefined;
    for (&join_threads, &join_runners) |*thread, *runner| {
        thread.* = try std.Thread.spawn(.{}, BoundedShutdownRunner.run, .{runner});
    }
    for (&join_threads) |thread| thread.join();
    for (&join_runners) |*runner| {
        try std.testing.expectEqual(ShutdownOutcome.complete, try runner.result.?);
    }

    try std.testing.expectEqual(ShutdownState.complete, core.shutdownState());
    try std.testing.expectEqual(@as(usize, 1), gate.callCount());
    try std.testing.expectEqual(core.driver.accepted_total, core.driver.completed_total);
    try std.testing.expectEqual(@as(usize, 0), core.pendingCallbackCount());
}

test "exchange core deinit completes retained worker after shutdown timeout" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    var core_live = true;
    defer if (core_live) core.deinit();
    try core.startup();

    var gate: CommandCallbackGate = .{ .io = std.testing.io };
    defer gate.release();
    try core.submitApiCommandWithCallback(.{ .add_user = 98_330 }, .{
        .context = &gate,
        .callback = CommandCallbackGate.onResponse,
    });
    gate.waitEntered();

    const immediate: std.Io.Timeout = .{
        .duration = .{ .raw = .zero, .clock = .awake },
    };
    try std.testing.expectError(error.Timeout, core.shutdownTimeout(immediate));
    gate.release();

    // No explicit retry: sequential deinit owns the unbounded completion/join
    // barrier promised by its public contract.
    core.deinit();
    core_live = false;
    try std.testing.expectEqual(@as(usize, 1), gate.callCount());
}

test "exchange core bounded cold shutdown installs stable drainer after timeout" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();

    var gate: CommandCallbackGate = .{ .io = std.testing.io };
    defer gate.release();
    try core.submitApiCommandWithCallback(.{ .add_user = 98_315 }, .{
        .context = &gate,
        .callback = CommandCallbackGate.onResponse,
    });
    var drain_runner: ColdDrainRunner = .{ .core = &core };
    const drain_thread = try std.Thread.spawn(.{}, ColdDrainRunner.run, .{&drain_runner});
    gate.waitEntered();

    const immediate: std.Io.Timeout = .{
        .duration = .{ .raw = .zero, .clock = .awake },
    };
    try std.testing.expectError(error.Timeout, core.shutdownTimeout(immediate));
    try std.testing.expectEqual(ShutdownState.closing, core.shutdownState());
    try std.testing.expectError(error.CoreStopped, core.addUserAsync(98_316));

    gate.release();
    drain_thread.join();
    try std.testing.expectEqual(@as(usize, 1), try drain_runner.result.?);
    try std.testing.expectEqual(
        ShutdownOutcome.complete,
        try core.shutdownTimeout(.none),
    );
    try std.testing.expectEqual(ShutdownState.complete, core.shutdownState());
    try std.testing.expectEqual(@as(usize, 1), gate.callCount());
    try std.testing.expectEqual(core.driver.accepted_total, core.driver.completed_total);
    try std.testing.expectEqual(@as(usize, 0), core.pendingCallbackCount());
}

test "exchange core bounded cold shutdown worker owns queued callback after return" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();

    var gate: CommandCallbackGate = .{ .io = std.testing.io };
    defer gate.release();
    try core.submitApiCommandWithCallback(.{ .add_user = 98_325 }, .{
        .context = &gate,
        .callback = CommandCallbackGate.onResponse,
    });

    const immediate: std.Io.Timeout = .{
        .duration = .{ .raw = .zero, .clock = .awake },
    };
    try std.testing.expectError(error.Timeout, core.shutdownTimeout(immediate));
    gate.waitEntered();
    try std.testing.expectEqual(ShutdownState.closing, core.shutdownState());
    try std.testing.expectError(error.CoreStopped, core.addUserAsync(98_326));

    gate.release();
    try std.testing.expectEqual(
        ShutdownOutcome.complete,
        try core.shutdownTimeout(.none),
    );
    try std.testing.expectEqual(@as(usize, 1), gate.callCount());
    try std.testing.expectEqual(core.driver.accepted_total, core.driver.completed_total);
    try std.testing.expectEqual(@as(usize, 0), core.pendingCallbackCount());
    try std.testing.expectEqual(ShutdownState.complete, core.shutdownState());
}

test "exchange core callback shutdown defers without waiting for itself" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();
    try core.startup();

    var probe: SelfShutdownCallbackProbe = .{
        .core = &core,
        .io = std.testing.io,
    };
    try core.submitApiCommandWithCallback(.{ .add_user = 98_317 }, .{
        .context = &probe,
        .callback = SelfShutdownCallbackProbe.onResponse,
    });
    probe.waitCalled();
    try std.testing.expectEqual(ShutdownOutcome.deferred, try probe.outcome());
    try std.testing.expectEqual(ShutdownState.closing, core.shutdownState());
    try std.testing.expectEqual(
        ShutdownOutcome.complete,
        try core.shutdownTimeout(.none),
    );
    try std.testing.expectEqual(core.driver.accepted_total, core.driver.completed_total);
    try std.testing.expectEqual(@as(usize, 0), core.pendingCallbackCount());
}

test "exchange core foreign callback fences contended target and defers to retry" {
    const allocator = std.testing.allocator;
    var source = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer source.deinit();
    var target = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer target.deinit();
    try source.startup();

    var probe: SelfShutdownCallbackProbe = .{
        .core = &target,
        .io = std.testing.io,
    };
    target.driver.mutex.lockUncancelable(target.driver.io);
    var target_lock_held = true;
    defer if (target_lock_held) target.driver.mutex.unlock(target.driver.io);

    try source.submitApiCommandWithCallback(.{ .add_user = 98_328 }, .{
        .context = &probe,
        .callback = SelfShutdownCallbackProbe.onResponse,
    });
    probe.waitCalled();
    try std.testing.expectEqual(ShutdownOutcome.deferred, try probe.outcome());
    try std.testing.expectEqual(ShutdownState.closing, target.shutdownState());
    try std.testing.expect(target.isStopped());

    target.driver.mutex.unlock(target.driver.io);
    target_lock_held = false;
    try std.testing.expectError(error.CoreStopped, target.addUserAsync(98_329));
    try std.testing.expectEqual(
        ShutdownOutcome.complete,
        try target.shutdownTimeout(.none),
    );
    try std.testing.expectEqual(ShutdownState.complete, target.shutdownState());
}

test "exchange core bounded shutdown fences admission while driver lock is contended" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();
    try core.startup();

    const immediate: std.Io.Timeout = .{
        .duration = .{ .raw = .zero, .clock = .awake },
    };
    core.driver.mutex.lockUncancelable(core.driver.io);
    var runner: BoundedShutdownRunner = .{
        .core = &core,
        .timeout = immediate,
    };
    const thread = try std.Thread.spawn(.{}, BoundedShutdownRunner.run, .{&runner});
    thread.join();

    try std.testing.expectError(error.Timeout, runner.result.?);
    // These observations cannot take the held mutex; they are driven by the
    // release/acquire close fence established at method entry.
    try std.testing.expectEqual(ShutdownState.closing, core.shutdownState());
    try std.testing.expect(core.isStopped());
    core.driver.mutex.unlock(core.driver.io);

    try std.testing.expectError(error.CoreStopped, core.addUserAsync(98_318));
    try std.testing.expectEqual(
        ShutdownOutcome.complete,
        try core.shutdownTimeout(.none),
    );
    try std.testing.expectEqual(ShutdownState.complete, core.shutdownState());
}

test "exchange core bounded control lock cannot be acquired after deadline" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();

    core.driver.mutex.lockUncancelable(core.driver.io);
    var driver_lock_held = true;
    defer if (driver_lock_held) core.driver.mutex.unlock(core.driver.io);

    var runner: BoundedShutdownRunner = .{
        .core = &core,
        .timeout = .{
            .duration = .{
                .raw = .fromMilliseconds(2),
                .clock = .awake,
            },
        },
    };
    const thread = try std.Thread.spawn(.{}, BoundedShutdownRunner.run, .{&runner});
    while (!core.driver.close_requested.load(.acquire)) {
        std.Thread.yield() catch {};
    }
    try (std.Io.Timeout{
        .duration = .{
            .raw = .fromMilliseconds(10),
            .clock = .awake,
        },
    }).sleep(std.testing.io);
    core.driver.mutex.unlock(core.driver.io);
    driver_lock_held = false;
    thread.join();

    try std.testing.expectError(error.Timeout, runner.result.?);
    try std.testing.expectEqual(ShutdownState.closing, core.shutdownState());
    try std.testing.expectEqual(
        ShutdownOutcome.complete,
        try core.shutdownTimeout(.none),
    );
}

test "exchange core shutdown waits for externally published poison settlement" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();
    try core.startup();

    var gate: CommandCallbackGate = .{ .io = std.testing.io };
    defer gate.release();
    try core.submitApiCommandWithCallback(.{ .add_user = 98_319 }, .{
        .context = &gate,
        .callback = CommandCallbackGate.onResponse,
    });
    gate.waitEntered();

    var skipped: FatalCallbackProbe = .{};
    try core.submitApiCommandWithCallback(.{ .add_user = 98_320 }, .{
        .context = &skipped,
        .callback = FatalCallbackProbe.onResponse,
        .failure_callback = FatalCallbackProbe.onFailure,
    });

    var poison_command = makeNopCommand();
    core.poisonDirectExecution(
        publication.PublicationSequence.fromInt(
            core.driver.publication_sequencer.highWater() + 1,
        ),
        &poison_command,
        error.InjectedPersistenceFault,
    );
    const immediate: std.Io.Timeout = .{
        .duration = .{ .raw = .zero, .clock = .awake },
    };
    try std.testing.expectError(error.Timeout, core.shutdownTimeout(immediate));
    try std.testing.expect(!core.driver.worker_quiesced.isSet());

    gate.release();
    try std.testing.expectEqual(
        ShutdownOutcome.complete,
        try core.shutdownTimeout(.none),
    );
    try std.testing.expectEqual(@as(usize, 1), gate.callCount());
    try std.testing.expectEqual(@as(usize, 0), skipped.response_count);
    try std.testing.expectEqual(@as(usize, 1), skipped.failure_count);
    try std.testing.expectEqual(
        CallbackFailureKind.skipped_after_fatal,
        skipped.kinds[0],
    );
    try std.testing.expectEqual(core.driver.accepted_total, core.driver.completed_total);
    try std.testing.expectEqual(@as(usize, 0), core.pendingCallbackCount());
    try std.testing.expectEqual(ShutdownState.complete, core.shutdownState());
}

test "exchange core losing worker poison still settles its popped callback" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();

    var callback: FatalCallbackProbe = .{};
    try core.submitApiCommandWithCallback(.{ .add_user = 98_327 }, .{
        .context = &callback,
        .callback = FatalCallbackProbe.onResponse,
        .failure_callback = FatalCallbackProbe.onFailure,
    });
    core.driver.mutex.lockUncancelable(core.driver.io);
    const popped = core.driver.queue.pop().?;
    core.driver.mutex.unlock(core.driver.io);

    // Model an item already owned by the worker. A different first fatal wins
    // before that item reports its own failure: immutable fatal identity must
    // win while the popped callback still settles exactly once.
    var poison_command = makeNopCommand();
    core.poisonDirectExecution(
        publication.PublicationSequence.fromInt(
            core.driver.publication_sequencer.highWater() + 1,
        ),
        &poison_command,
        error.InjectedPersistenceFault,
    );
    core.poisonFromWork(popped, error.InjectedJournalIoFailure);

    try std.testing.expectEqual(
        ShutdownOutcome.complete,
        try core.shutdownTimeout(.none),
    );
    try std.testing.expectEqual(@as(usize, 0), callback.response_count);
    try std.testing.expectEqual(@as(usize, 1), callback.failure_count);
    try std.testing.expectEqual(
        CallbackFailureKind.skipped_after_fatal,
        callback.kinds[0],
    );
    try std.testing.expectEqual(
        error.InjectedPersistenceFault,
        callback.causes[0],
    );
    try std.testing.expectEqual(core.driver.accepted_total, core.driver.completed_total);
    try std.testing.expectEqual(@as(usize, 0), core.pendingCallbackCount());
}

test "exchange core cold publishers cannot enter an owned bootstrap" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();

    try core.beginBootstrap();
    var bootstrap_owned = true;
    defer if (bootstrap_owned) core.finishBootstrap();

    try std.testing.expectError(error.CoreNotStarted, core.addUserAsync(98_321));
    try std.testing.expectError(
        error.CoreNotStarted,
        core.enqueueRuntime(makeAddUserCommand(98_322)),
    );
    try std.testing.expectError(
        error.CoreNotStarted,
        core.submitApiCommandWithCallback(.{ .add_user = 98_323 }, .{
            .callback = struct {
                fn callback(
                    _: ?*anyopaque,
                    _: *const kernel_mod.CommandResponse,
                ) void {}
            }.callback,
        }),
    );
    try std.testing.expectEqual(@as(u64, 0), core.driver.accepted_total);
    try std.testing.expect(core.driver.queue.isEmpty());

    core.finishBootstrap();
    bootstrap_owned = false;
    const accepted = try core.addUserAsync(98_324);
    core.shutdown();
    var response = core.takeAsyncResult(accepted) orelse return error.MissingAsyncResult;
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
}

test "exchange core cold shutdown callback can reenter shutdown without deadlock" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();

    const Probe = struct {
        core: *ExchangeCore,
        calls: usize = 0,

        fn onResponse(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
            const self: *@This() = @ptrCast(@alignCast(context.?));
            self.calls += 1;
            self.core.shutdown();
        }
    };

    var probe: Probe = .{ .core = &core };
    try core.submitApiCommandWithCallback(.{ .add_user = 98_312 }, .{
        .context = &probe,
        .callback = Probe.onResponse,
    });

    // No worker exists. shutdown() owns the cold drain and the callback above
    // recursively observes the already-closing lifecycle.
    core.shutdown();

    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    try std.testing.expectEqual(@as(usize, 0), core.pendingCallbackCount());
    try std.testing.expect(core.isStopped());
    try std.testing.expect(core.kernelRef().risk_engine.getUserProfile(98_312) != null);
}

test "exchange core worker supports blocking yielding and busy-spin waits" {
    const allocator = std.testing.allocator;
    for ([_]runtime_mod.WaitStrategy{ .blocking, .yielding, .busy_spin }) |strategy| {
        var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
            .runtime = .{
                .queue_capacity = 4,
                .max_batch_size = 1,
                .wait_strategy = strategy,
            },
        });
        defer core.deinit();
        try core.startup();
        const request_id = try core.addUserAsync(98_400 + @as(i64, @intFromEnum(strategy)));
        var barrier = try core.nop();
        defer barrier.deinit(allocator);
        var response = core.takeAsyncResult(request_id) orelse return error.MissingAsyncResult;
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    }
}

const BackpressureGate = struct {
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    condition: std.Io.Condition = .init,
    entered: bool = false,
    released: bool = false,

    fn callback(context: ?*anyopaque, _: runtime_mod.ResultEvent) void {
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

const CallbackQuiescenceProbe = struct {
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    condition: std.Io.Condition = .init,
    old_entered: bool = false,
    release_old: bool = false,
    old_count: usize = 0,
    new_count: usize = 0,

    fn oldCallback(context: ?*anyopaque, _: runtime_mod.ResultEvent) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.mutex.lockUncancelable(self.io);
        self.old_count += 1;
        self.old_entered = true;
        self.condition.broadcast(self.io);
        while (!self.release_old) {
            self.condition.waitUncancelable(self.io, &self.mutex);
        }
        self.mutex.unlock(self.io);
    }

    fn newCallback(context: ?*anyopaque, _: runtime_mod.ResultEvent) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.mutex.lockUncancelable(self.io);
        self.new_count += 1;
        self.condition.broadcast(self.io);
        self.mutex.unlock(self.io);
    }

    fn waitOldEntered(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        while (!self.old_entered) {
            self.condition.waitUncancelable(self.io, &self.mutex);
        }
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
        while (!self.release_old) {
            self.condition.waitUncancelable(self.io, &self.mutex);
        }
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
        while (!self.old_entered) {
            self.condition.waitUncancelable(self.io, &self.mutex);
        }
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
    core: *ExchangeCore,
    probe: *CallbackQuiescenceProbe,
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

        self.core.setResultSink(.{
            .context = self.probe,
            .on_result = CallbackQuiescenceProbe.newCallback,
        });

        self.mutex.lockUncancelable(self.io);
        self.done = true;
        self.condition.broadcast(self.io);
        self.mutex.unlock(self.io);
    }

    fn waitEntered(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        while (!self.entered) {
            self.condition.waitUncancelable(self.io, &self.mutex);
        }
    }

    fn isDone(self: *@This()) bool {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.done;
    }
};

const RawResultSetterRunner = struct {
    core: *ExchangeCore,
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

        self.core.setRawResultSinkChecked(.{
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
        while (!self.entered) {
            self.condition.waitUncancelable(self.io, &self.mutex);
        }
    }

    fn isDone(self: *@This()) bool {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.done;
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

fn testWaitTimeout() std.Io.Timeout {
    return .{
        .duration = .{
            .clock = .awake,
            .raw = .fromSeconds(5),
        },
    };
}

const BoundedCommandCallbackGate = struct {
    io: std.Io,
    entered: std.Io.Event = .unset,
    release_event: std.Io.Event = .unset,
    calls: usize = 0,

    fn onResponse(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.calls += 1;
        self.entered.set(self.io);
        self.release_event.waitUncancelable(self.io);
    }

    fn waitEntered(self: *@This()) std.Io.Event.WaitTimeoutError!void {
        try self.entered.waitTimeout(self.io, testWaitTimeout());
    }

    fn release(self: *@This()) void {
        if (!self.release_event.isSet()) self.release_event.set(self.io);
    }
};

const BoundedResultGate = struct {
    io: std.Io,
    entered: std.Io.Event = .unset,
    release_event: std.Io.Event = .unset,
    calls: usize = 0,

    fn onResult(context: ?*anyopaque, _: runtime_mod.ResultEvent) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.calls += 1;
        if (self.calls != 1) return;
        self.entered.set(self.io);
        self.release_event.waitUncancelable(self.io);
    }

    fn waitEntered(self: *@This()) std.Io.Event.WaitTimeoutError!void {
        try self.entered.waitTimeout(self.io, testWaitTimeout());
    }

    fn release(self: *@This()) void {
        if (!self.release_event.isSet()) self.release_event.set(self.io);
    }
};

const FatalDrainCallbackGate = struct {
    io: std.Io,
    entered: std.Io.Event = .unset,
    release_event: std.Io.Event = .unset,
    response_count: usize = 0,
    failure_count: usize = 0,
    request_ids: [4]u64 = undefined,
    kinds: [4]CallbackFailureKind = undefined,

    fn onResponse(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.response_count += 1;
    }

    fn onFailure(context: ?*anyopaque, failure: CallbackFailure) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        std.debug.assert(self.failure_count < self.request_ids.len);
        const index = self.failure_count;
        self.request_ids[index] = failure.request_id;
        self.kinds[index] = failure.kind;
        self.failure_count += 1;
        const first = index == 0;

        if (!first) return;
        // Signal before blocking so a reentrancy regression cannot prevent the
        // test controller from releasing this callback.
        self.entered.set(self.io);
        self.release_event.waitUncancelable(self.io);
    }

    fn waitEntered(self: *@This()) std.Io.Event.WaitTimeoutError!void {
        try self.entered.waitTimeout(self.io, testWaitTimeout());
    }

    fn release(self: *@This()) void {
        if (!self.release_event.isSet()) self.release_event.set(self.io);
    }
};

const AcceptedDrainRunner = struct {
    core: *ExchangeCore,
    io: std.Io,
    started: std.Io.Event = .unset,
    finished: std.Io.Event = .unset,
    result: ?(ExecuteError!usize) = null,

    fn run(self: *@This()) void {
        self.started.set(self.io);
        self.result = self.core.drainAsyncPending();
        const driver = self.core.driver;
        driver.mutex.lockUncancelable(driver.io);
        self.finished.set(self.io);
        driver.completion_available.broadcast(driver.io);
        driver.mutex.unlock(driver.io);
    }

    fn waitStarted(self: *@This()) std.Io.Event.WaitTimeoutError!void {
        try self.started.waitTimeout(self.io, testWaitTimeout());
    }

    fn waitFinished(self: *@This()) std.Io.Event.WaitTimeoutError!void {
        try self.finished.waitTimeout(self.io, testWaitTimeout());
    }
};

const RuntimeDrainRunner = struct {
    core: *ExchangeCore,
    io: std.Io,
    started: std.Io.Event = .unset,
    finished: std.Io.Event = .unset,
    result: ?(RuntimeDrainError!runtime_mod.DrainOutcome) = null,

    fn run(self: *@This()) void {
        self.started.set(self.io);
        self.result = self.core.drainRuntime();
        self.finished.set(self.io);
    }

    fn waitStarted(self: *@This()) std.Io.Event.WaitTimeoutError!void {
        try self.started.waitTimeout(self.io, testWaitTimeout());
    }

    fn waitFinished(self: *@This()) std.Io.Event.WaitTimeoutError!void {
        try self.finished.waitTimeout(self.io, testWaitTimeout());
    }
};

const TrackedBinaryAsyncPublisher = struct {
    core: *ExchangeCore,
    io: std.Io,
    payload: []const u8,
    transfer_id: i32,
    finished: std.Io.Event = .unset,
    result: ?(BinaryPayloadAsyncSubmitError!u64) = null,

    fn run(self: *@This()) void {
        self.result = self.core.submitBinaryPayloadAsync(
            .binary_data_query,
            self.transfer_id,
            79,
            self.payload,
        );
        self.finished.set(self.io);
    }

    fn waitFinished(self: *@This()) std.Io.Event.WaitTimeoutError!void {
        try self.finished.waitTimeout(self.io, testWaitTimeout());
    }
};

const CrossBackendDrainProbe = struct {
    target: *ExchangeCore,
    async_result: ?(ExecuteError!usize) = null,
    runtime_result: ?(RuntimeDrainError!runtime_mod.DrainOutcome) = null,

    fn onResult(context: ?*anyopaque, _: runtime_mod.ResultEvent) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.async_result = self.target.drainAsyncPending();
        self.runtime_result = self.target.drainRuntime();
    }
};

const CrossBackendBlockingProbe = struct {
    target: *ExchangeCore,
    allocator: std.mem.Allocator,
    startup_error: ?anyerror = null,
    bootstrap_error: ?anyerror = null,
    submit_error: ?anyerror = null,
    order_book_error: ?anyerror = null,
    query_error: ?anyerror = null,
    wait_error: ?anyerror = null,
    composite_error: ?anyerror = null,
    nop_composite_error: ?anyerror = null,
    empty_api_composite_error: ?anyerror = null,
    empty_command_batch_ok: bool = false,
    empty_command_batch_error: ?anyerror = null,

    fn onResult(context: ?*anyopaque, _: runtime_mod.ResultEvent) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));

        self.target.startup() catch |err| {
            self.startup_error = err;
        };

        bootstrap: {
            self.target.beginBootstrap() catch |err| {
                self.bootstrap_error = err;
                break :bootstrap;
            };
            self.target.finishBootstrap();
            self.bootstrap_error = error.UnexpectedBlockingSuccess;
        }

        submit: {
            var response = self.target.addUser(99_131) catch |err| {
                self.submit_error = err;
                break :submit;
            };
            response.deinit(self.allocator);
            self.submit_error = error.UnexpectedBlockingSuccess;
        }

        order_book: {
            var response = self.target.requestOrderBook(99_132, 1) catch |err| {
                self.order_book_error = err;
                break :order_book;
            };
            response.deinit(self.allocator);
            self.order_book_error = error.UnexpectedBlockingSuccess;
        }

        query: {
            var report = self.target.queryReport(.risk_state_hash) catch |err| {
                self.query_error = err;
                break :query;
            };
            report.deinit(self.allocator);
            self.query_error = error.UnexpectedBlockingSuccess;
        }

        wait: {
            var response = self.target.waitTakeResponse(1) catch |err| {
                self.wait_error = err;
                break :wait;
            };
            response.deinit(self.allocator);
            self.wait_error = error.UnexpectedBlockingSuccess;
        }

        composite: {
            var result = self.target.submitApiCommandsSync(&.{.nop}) catch |err| {
                self.composite_error = err;
                break :composite;
            };
            result.deinit(self.allocator);
            self.composite_error = error.UnexpectedBlockingSuccess;
        }

        nop_composite: {
            var response = self.target.submitApiCommandsSyncWithNop(&.{}) catch |err| {
                self.nop_composite_error = err;
                break :nop_composite;
            };
            response.deinit(self.allocator);
            self.nop_composite_error = error.UnexpectedBlockingSuccess;
        }

        const empty_api_commands: [0]ApiCommand = .{};
        _ = self.target.submitApiCommandsSync(empty_api_commands[0..]) catch |err| {
            self.empty_api_composite_error = err;
        };

        const empty_commands: [0]*cmd.OrderCommand = .{};
        _ = self.target.submitCommandBatch(empty_commands[0..]) catch |err| {
            self.empty_command_batch_error = err;
            return;
        };
        self.empty_command_batch_ok = true;
    }
};

const CrossBackendCycleProbe = struct {
    source: *ExchangeCore,
    target: *ExchangeCore,
    allocator: std.mem.Allocator,
    forward_published: bool = false,
    forward_error: ?anyerror = null,
    return_error: ?anyerror = null,

    fn onSourceResult(context: ?*anyopaque, _: runtime_mod.ResultEvent) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.target.submitApiCommandWithCallback(.{ .add_user = 99_141 }, .{
            .context = self,
            .callback = onTargetResponse,
        }) catch |err| {
            self.forward_error = err;
            return;
        };
        self.forward_published = true;
    }

    fn onTargetResponse(
        context: ?*anyopaque,
        _: *const kernel_mod.CommandResponse,
    ) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        var response = self.source.addUser(99_142) catch |err| {
            self.return_error = err;
            return;
        };
        response.deinit(self.allocator);
        self.return_error = error.UnexpectedBlockingSuccess;
    }
};

const CrossBackendAsyncPublishProbe = struct {
    target: *ExchangeCore,
    uid: i64,
    result: ?(AsyncSubmitError!u64) = null,

    fn onResult(context: ?*anyopaque, _: runtime_mod.ResultEvent) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.result = self.target.addUserAsync(self.uid);
    }
};

const CrossBackendBinaryPublishProbe = struct {
    target: *ExchangeCore,
    payload: []const u8,
    result: ?(BinaryPayloadAsyncSubmitError!u64) = null,

    fn onResult(context: ?*anyopaque, _: runtime_mod.ResultEvent) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.result = self.target.submitBinaryPayloadAsync(
            .binary_data_query,
            99_152,
            81,
            self.payload,
        );
    }
};

fn waitForAcceptedDrainWaiters(
    core: *ExchangeCore,
    minimum: usize,
) error{Timeout}!void {
    const driver = core.driver;
    const deadline = testWaitTimeout().toDeadline(driver.io);
    while (true) {
        driver.mutex.lockUncancelable(driver.io);
        const observed = driver.accepted_drain_waiters;
        driver.mutex.unlock(driver.io);
        if (observed >= minimum) return;
        if (shutdownDeadlineExpired(deadline, driver.io)) return error.Timeout;
        std.Thread.yield() catch {};
    }
}

fn waitForActiveBinaryStream(core: *ExchangeCore) error{Timeout}!void {
    const driver = core.driver;
    const deadline = testWaitTimeout().toDeadline(driver.io);
    while (true) {
        driver.mutex.lockUncancelable(driver.io);
        const active_and_blocked =
            driver.active_binary_stream and driver.blocked_producers > 0;
        driver.mutex.unlock(driver.io);
        if (active_and_blocked) return;
        if (shutdownDeadlineExpired(deadline, driver.io)) return error.Timeout;
        std.Thread.yield() catch {};
    }
}

const CommandCallbackGate = struct {
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    condition: std.Io.Condition = .init,
    entered: bool = false,
    released: bool = false,
    calls: usize = 0,

    fn onResponse(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.mutex.lockUncancelable(self.io);
        self.calls += 1;
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

    fn callCount(self: *@This()) usize {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.calls;
    }
};

const PersistenceBarrierRunner = struct {
    const Operation = union(enum) {
        write_journal: []const u8,
        capture_snapshot: []const u8,
        validate_snapshot: []const u8,
        snapshot_by_id: u64,
    };

    core: *ExchangeCore,
    dir: std.Io.Dir,
    io: std.Io,
    operation: Operation,
    mutex: std.Io.Mutex = .init,
    condition: std.Io.Condition = .init,
    entered: bool = false,
    done: bool = false,
    failure: ?anyerror = null,
    snapshot: ?SnapshotDescriptorView = null,

    fn run(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        self.entered = true;
        self.condition.broadcast(self.io);
        self.mutex.unlock(self.io);
        defer {
            self.mutex.lockUncancelable(self.io);
            self.done = true;
            self.condition.broadcast(self.io);
            self.mutex.unlock(self.io);
        }

        switch (self.operation) {
            .write_journal => |path| self.core.writeJournalToFile(self.dir, path) catch |err| {
                self.failure = err;
            },
            .capture_snapshot => |path| self.core.captureSnapshotToFile(
                self.dir,
                path,
                &.{},
                8,
            ) catch |err| {
                self.failure = err;
            },
            .validate_snapshot => |path| self.core.validateSnapshotFile(self.dir, path) catch |err| {
                self.failure = err;
            },
            .snapshot_by_id => |snapshot_id| {
                self.snapshot = self.core.persistenceSnapshotById(snapshot_id) catch |err| {
                    self.failure = err;
                    return;
                };
            },
        }
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

const ShutdownCompletionRunner = struct {
    core: *ExchangeCore,
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
        self.core.shutdown();
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

const BoundedShutdownRunner = struct {
    core: *ExchangeCore,
    timeout: std.Io.Timeout,
    result: ?(ShutdownError!ShutdownOutcome) = null,

    fn run(self: *@This()) void {
        self.result = self.core.shutdownTimeout(self.timeout);
    }
};

const SelfShutdownCallbackProbe = struct {
    core: *ExchangeCore,
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    condition: std.Io.Condition = .init,
    called: bool = false,
    result: ?(ShutdownError!ShutdownOutcome) = null,

    fn onResponse(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        const result = self.core.shutdownTimeout(.none);
        self.mutex.lockUncancelable(self.io);
        self.result = result;
        self.called = true;
        self.condition.broadcast(self.io);
        self.mutex.unlock(self.io);
    }

    fn waitCalled(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        while (!self.called) self.condition.waitUncancelable(self.io, &self.mutex);
    }

    fn outcome(self: *@This()) ShutdownError!ShutdownOutcome {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.result.?;
    }
};

const SinkClearsSimpleEventsProbe = struct {
    core: *ExchangeCore,
    handler_count: usize = 0,

    fn sink(context: ?*anyopaque, _: runtime_mod.ResultEvent) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.core.clearSimpleEventsHandler();
    }

    fn onCommand(context: ?*anyopaque, _: simple_events_mod.ApiCommandResult) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.handler_count += 1;
    }
};

const ReplacingSimpleEventsProbe = struct {
    core: *ExchangeCore,
    old_command_count: usize = 0,
    old_orderbook_count: usize = 0,
    new_command_count: usize = 0,
    new_orderbook_count: usize = 0,

    fn oldCommand(context: ?*anyopaque, _: simple_events_mod.ApiCommandResult) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.old_command_count += 1;
        self.core.setSimpleEventsHandler(.{
            .context = self,
            .on_command_result = newCommand,
            .on_order_book = newOrderBook,
        });
    }

    fn oldOrderBook(context: ?*anyopaque, _: simple_events_mod.OrderBook) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.old_orderbook_count += 1;
    }

    fn newCommand(context: ?*anyopaque, _: simple_events_mod.ApiCommandResult) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.new_command_count += 1;
    }

    fn newOrderBook(context: ?*anyopaque, _: simple_events_mod.OrderBook) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.new_orderbook_count += 1;
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
    core: *ExchangeCore,
    probe: *SimpleEventsQuiescenceProbe,
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
        self.core.setSimpleEventsHandler(.{
            .context = self.probe,
            .on_command_result = SimpleEventsQuiescenceProbe.newCommand,
        });
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

const CrossCoreSetterProbe = struct {
    target: *ExchangeCore,
    result: ?(CallbackConfigError!void) = null,

    fn onResult(context: ?*anyopaque, _: runtime_mod.ResultEvent) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.result = self.target.setResultSinkChecked(.{});
    }
};

const CallbackOrderTrace = struct {
    const Step = enum {
        raw_result,
        result_sink,
        simple_command,
        simple_orderbook,
        request_callback,
    };

    count: usize = 0,
    steps: [8]Step = undefined,
    all_callbacks_scoped: bool = true,
    raw_saw_l2_snapshot: bool = false,
    raw_sequence: u64 = 0,

    fn append(self: *@This(), step: Step) void {
        std.debug.assert(self.count < self.steps.len);
        self.all_callbacks_scoped =
            self.all_callbacks_scoped and runtime_mod.isCallbackExecutionActive();
        self.steps[self.count] = step;
        self.count += 1;
    }

    fn onRawResult(context: ?*anyopaque, response: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.raw_saw_l2_snapshot = response.l2_snapshot != null;
        self.raw_sequence = response.sequence;
        self.append(.raw_result);
    }

    fn onResult(context: ?*anyopaque, _: runtime_mod.ResultEvent) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.append(.result_sink);
    }

    fn onSimpleCommand(context: ?*anyopaque, _: simple_events_mod.ApiCommandResult) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.append(.simple_command);
    }

    fn onSimpleOrderBook(context: ?*anyopaque, _: simple_events_mod.OrderBook) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.append(.simple_orderbook);
    }

    fn onRequest(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.append(.request_callback);
    }
};

const BackpressureProducer = struct {
    core: *ExchangeCore,
    result: ?(AsyncSubmitError!u64) = null,

    fn run(self: *@This()) void {
        self.result = self.core.addUserAsync(98_999);
    }
};

const ColdDrainRunner = struct {
    core: *ExchangeCore,
    result: ?(ExecuteError!usize) = null,

    fn run(self: *@This()) void {
        self.result = self.core.drainAsyncPending();
    }
};

const StartupRunner = struct {
    core: *ExchangeCore,
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    condition: std.Io.Condition = .init,
    entered: bool = false,
    result: ?(StartError!void) = null,

    fn run(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        self.entered = true;
        self.condition.broadcast(self.io);
        self.mutex.unlock(self.io);
        self.result = self.core.startup();
    }

    fn waitEntered(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        while (!self.entered) self.condition.waitUncancelable(self.io, &self.mutex);
    }
};

const ShutdownRunner = struct {
    core: *ExchangeCore,

    fn run(self: @This()) void {
        self.core.shutdown();
    }
};

const ColdReentrantCallback = struct {
    core: *ExchangeCore,
    allocator: std.mem.Allocator,
    startup_error: ?anyerror = null,
    bootstrap_error: ?anyerror = null,
    nested_error: ?anyerror = null,

    fn callback(context: ?*anyopaque, _: runtime_mod.ResultEvent) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.core.setResultSink(.{});
        self.core.startup() catch |err| {
            self.startup_error = err;
        };
        self.core.beginBootstrap() catch |err| {
            self.bootstrap_error = err;
        };
        var response = self.core.nop() catch |err| {
            self.nested_error = err;
            self.core.shutdown();
            return;
        };
        response.deinit(self.allocator);
        self.core.shutdown();
    }
};

const RawReconfigureShutdownProbe = struct {
    core: *ExchangeCore,
    allocator: std.mem.Allocator,
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    condition: std.Io.Condition = .init,
    finished: bool = false,
    first_count: usize = 0,
    replacement_count: usize = 0,
    request_id: ?u64 = null,
    reconfiguration_failure: ?anyerror = null,
    enqueue_failure: ?anyerror = null,
    synchronous_wait_failure: ?anyerror = null,

    fn first(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        defer self.markFinished();
        self.first_count += 1;

        var response = self.core.addUser(99_072) catch |err| {
            self.synchronous_wait_failure = err;
            self.reconfigureAndShutdown();
            return;
        };
        response.deinit(self.allocator);
        self.reconfigureAndShutdown();
    }

    fn reconfigureAndShutdown(self: *@This()) void {
        self.core.setRawResultSinkChecked(.{
            .context = self,
            .on_result = replacement,
        }) catch |err| {
            self.reconfiguration_failure = err;
            return;
        };
        self.request_id = self.core.addUserAsync(99_073) catch |err| {
            self.enqueue_failure = err;
            return;
        };
        self.core.shutdown();
    }

    fn markFinished(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        self.finished = true;
        self.condition.broadcast(self.io);
        self.mutex.unlock(self.io);
    }

    fn waitFinished(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        while (!self.finished) self.condition.waitUncancelable(self.io, &self.mutex);
    }

    fn replacement(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.replacement_count += 1;
    }
};

const RawReconfiguresLaterCallbacksProbe = struct {
    core: *ExchangeCore,
    old_runtime_count: usize = 0,
    new_runtime_count: usize = 0,
    old_simple_count: usize = 0,
    new_simple_count: usize = 0,

    fn raw(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.core.setResultSink(.{
            .context = self,
            .on_result = newRuntime,
        });
        self.core.setSimpleEventsHandler(.{
            .context = self,
            .on_command_result = newSimple,
        });
    }

    fn oldRuntime(context: ?*anyopaque, _: runtime_mod.ResultEvent) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.old_runtime_count += 1;
    }

    fn newRuntime(context: ?*anyopaque, _: runtime_mod.ResultEvent) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.new_runtime_count += 1;
    }

    fn oldSimple(context: ?*anyopaque, _: simple_events_mod.ApiCommandResult) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.old_simple_count += 1;
    }

    fn newSimple(context: ?*anyopaque, _: simple_events_mod.ApiCommandResult) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.new_simple_count += 1;
    }
};

const FullRingCallbackProbe = struct {
    core: *ExchangeCore,
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    condition: std.Io.Condition = .init,
    armed: bool = true,
    entered: bool = false,
    released: bool = false,
    done: bool = false,
    result: ?(AsyncSubmitError!u64) = null,

    fn callback(context: ?*anyopaque, _: runtime_mod.ResultEvent) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        self.mutex.lockUncancelable(self.io);
        if (!self.armed) {
            self.mutex.unlock(self.io);
            return;
        }
        self.armed = false;
        self.entered = true;
        self.condition.broadcast(self.io);
        while (!self.released) self.condition.waitUncancelable(self.io, &self.mutex);
        self.mutex.unlock(self.io);

        self.result = self.core.addUserAsync(99_099);

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

    fn releaseAndWait(self: *@This()) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        self.released = true;
        self.condition.broadcast(self.io);
        while (!self.done) self.condition.waitUncancelable(self.io, &self.mutex);
    }
};

test "exchange core blocking producer wakes exactly when worker frees capacity" {
    const allocator = std.testing.allocator;
    var gate: BackpressureGate = .{ .io = std.testing.io };
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .runtime = .{ .queue_capacity = 4, .max_batch_size = 1, .wait_strategy = .blocking },
    });
    defer core.deinit();
    core.setResultSink(.{ .context = &gate, .on_result = BackpressureGate.callback });

    _ = try core.addUserAsync(98_900);
    try core.startup();
    gate.waitEntered();
    for (0..4) |index| {
        try core.enqueueRuntime(makeAddUserCommand(98_910 + @as(i64, @intCast(index))));
    }

    var producer: BackpressureProducer = .{ .core = &core };
    const producer_thread = try std.Thread.spawn(.{}, BackpressureProducer.run, .{&producer});
    core.driver.mutex.lockUncancelable(core.driver.io);
    while (core.driver.blocked_producers == 0) {
        core.driver.completion_available.waitUncancelable(core.driver.io, &core.driver.mutex);
    }
    core.driver.mutex.unlock(core.driver.io);

    gate.release();
    producer_thread.join();
    _ = try producer.result.?;
    core.shutdown();
}

test "exchange core bounds ready results and wakes producer when a result is consumed" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .runtime = .{ .queue_capacity = 4, .max_batch_size = 1, .wait_strategy = .blocking },
    });
    defer core.deinit();
    try core.startup();

    var request_ids: [4]u64 = undefined;
    for (&request_ids, 0..) |*request_id, index| {
        request_id.* = try core.addUserAsync(98_950 + @as(i64, @intCast(index)));
    }
    try std.testing.expectEqual(@as(usize, 4), try core.drainAsyncPending());
    try std.testing.expectEqual(@as(usize, 4), core.asyncReadyCount());
    try std.testing.expectEqual(@as(usize, 4), core.driver.completion_slots_used);

    var producer: BackpressureProducer = .{ .core = &core };
    const producer_thread = try std.Thread.spawn(.{}, BackpressureProducer.run, .{&producer});
    core.driver.mutex.lockUncancelable(core.driver.io);
    while (core.driver.blocked_producers == 0) {
        core.driver.completion_available.waitUncancelable(core.driver.io, &core.driver.mutex);
    }
    core.driver.mutex.unlock(core.driver.io);

    var released = core.takeAsyncResult(request_ids[0]) orelse return error.MissingAsyncResult;
    released.deinit(allocator);
    producer_thread.join();
    const producer_request_id = try producer.result.?;
    try std.testing.expectEqual(@as(usize, 1), try core.drainAsyncPending());

    for (request_ids[1..]) |request_id| {
        var response = core.takeAsyncResult(request_id) orelse return error.MissingAsyncResult;
        response.deinit(allocator);
    }
    var producer_response = core.takeAsyncResult(producer_request_id) orelse
        return error.MissingAsyncResult;
    producer_response.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), core.asyncReadyCount());
    try std.testing.expectEqual(@as(usize, 0), core.driver.completion_slots_used);
    core.shutdown();
}

test "exchange core startup waits for an active cold manual drain" {
    const allocator = std.testing.allocator;
    var gate: BackpressureGate = .{ .io = std.testing.io };
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();
    core.setResultSink(.{ .context = &gate, .on_result = BackpressureGate.callback });
    _ = try core.addUserAsync(99_001);

    var drain_runner: ColdDrainRunner = .{ .core = &core };
    const drain_thread = try std.Thread.spawn(.{}, ColdDrainRunner.run, .{&drain_runner});
    gate.waitEntered();

    var startup_runner: StartupRunner = .{ .core = &core, .io = std.testing.io };
    const startup_thread = try std.Thread.spawn(.{}, StartupRunner.run, .{&startup_runner});
    startup_runner.waitEntered();

    const observed = blk: {
        core.driver.mutex.lockUncancelable(core.driver.io);
        defer core.driver.mutex.unlock(core.driver.io);
        break :blk .{
            core.driver.manual_drain_active,
            core.runtime.lifecycleState(),
        };
    };

    // The drain has already popped its last queue item and is blocked in the
    // result callback. A cold direct mutation or report query must not overtake
    // that accepted publication while `manual_drain_active` still owns the
    // execution prefix.
    try std.testing.expectError(error.CoreNotStarted, core.addUser(99_002));
    try std.testing.expectError(
        error.CoreNotStarted,
        core.queryReport(.{ .user_balance = .{ .uid = 99_001, .currency = 15 } }),
    );

    gate.release();
    drain_thread.join();
    startup_thread.join();
    try std.testing.expect(observed[0]);
    try std.testing.expectEqual(runtime_mod.LifecycleState.cold, observed[1]);
    try std.testing.expectEqual(@as(usize, 1), try drain_runner.result.?);
    try startup_runner.result.?;
    try std.testing.expect(core.isStarted());
}

test "exchange core bootstrap claim excludes startup and cold direct execution" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();
    try core.beginBootstrap();
    var claim_active = true;
    defer if (claim_active) core.finishBootstrap();

    try std.testing.expectError(error.CoreNotStarted, core.addUser(99_025));
    try std.testing.expectError(error.CoreNotStarted, core.drainAsyncPending());
    try std.testing.expectError(error.CoreReentrantCall, core.beginBootstrap());

    var startup_runner: StartupRunner = .{ .core = &core, .io = std.testing.io };
    const startup_thread = try std.Thread.spawn(.{}, StartupRunner.run, .{&startup_runner});
    startup_runner.waitEntered();
    const observed = blk: {
        core.driver.mutex.lockUncancelable(core.driver.io);
        defer core.driver.mutex.unlock(core.driver.io);
        break :blk .{
            core.driver.bootstrap_owner,
            core.runtime.lifecycleState(),
        };
    };

    core.finishBootstrap();
    claim_active = false;
    startup_thread.join();
    try startup_runner.result.?;
    try std.testing.expect(observed[0] != null);
    try std.testing.expectEqual(runtime_mod.LifecycleState.cold, observed[1]);
    try std.testing.expect(core.isStarted());
}

test "exchange core cold callback mutation is safe and blocking reentry is rejected" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();
    var callback: ColdReentrantCallback = .{ .core = &core, .allocator = allocator };
    core.setResultSink(.{
        .context = &callback,
        .on_result = ColdReentrantCallback.callback,
    });

    var response = try core.addUser(99_050);
    defer response.deinit(allocator);
    try std.testing.expectEqual(error.CoreReentrantCall, callback.startup_error.?);
    try std.testing.expectEqual(error.CoreReentrantCall, callback.bootstrap_error.?);
    try std.testing.expectEqual(error.CoreReentrantCall, callback.nested_error.?);
    try std.testing.expect(core.isStopped());
}

test "exchange core raw callback can reconfigure enqueue and request same-core shutdown" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();
    var probe: RawReconfigureShutdownProbe = .{
        .core = &core,
        .allocator = allocator,
        .io = std.testing.io,
    };
    core.setRawResultSink(.{
        .context = &probe,
        .on_result = RawReconfigureShutdownProbe.first,
    });
    try core.startup();

    const first_request = try core.addUserAsync(99_071);
    probe.waitFinished();
    core.shutdown();
    try std.testing.expect(core.isStopped());
    try std.testing.expectEqual(@as(?anyerror, null), probe.reconfiguration_failure);
    try std.testing.expectEqual(@as(?anyerror, null), probe.enqueue_failure);
    try std.testing.expectEqual(
        @as(anyerror, error.CoreReentrantCall),
        probe.synchronous_wait_failure orelse return error.MissingCallbackFailure,
    );
    try std.testing.expectEqual(@as(usize, 1), probe.first_count);
    try std.testing.expectEqual(@as(usize, 1), probe.replacement_count);

    var first = core.takeAsyncResult(first_request) orelse return error.MissingAsyncResult;
    defer first.deinit(allocator);
    var queued = core.takeAsyncResult(probe.request_id orelse return error.MissingAsyncResult) orelse
        return error.MissingAsyncResult;
    defer queued.deinit(allocator);
}

test "exchange core result sink replacement waits for old callback quiescence" {
    const allocator = std.testing.allocator;
    var probe: CallbackQuiescenceProbe = .{ .io = std.testing.io };
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();
    core.setResultSink(.{
        .context = &probe,
        .on_result = CallbackQuiescenceProbe.oldCallback,
    });
    try core.startup();

    const first_request = try core.addUserAsync(99_060);
    probe.waitOldEntered();

    var setter: ResultSinkSetterRunner = .{
        .core = &core,
        .probe = &probe,
        .io = std.testing.io,
    };
    const setter_thread = try std.Thread.spawn(.{}, ResultSinkSetterRunner.run, .{&setter});
    setter.waitEntered();
    try std.testing.expect(!setter.isDone());

    probe.releaseOld();
    setter_thread.join();
    try std.testing.expect(setter.isDone());

    var first_response = try core.waitTakeResponse(first_request);
    defer first_response.deinit(allocator);
    const second_request = try core.addUserAsync(99_061);
    var second_response = try core.waitTakeResponse(second_request);
    defer second_response.deinit(allocator);

    const counts = probe.counts();
    try std.testing.expectEqual(@as(usize, 1), counts[0]);
    try std.testing.expectEqual(@as(usize, 1), counts[1]);
}

test "exchange core raw result sink replacement waits for old callback quiescence" {
    const allocator = std.testing.allocator;
    var probe: RawResultQuiescenceProbe = .{ .io = std.testing.io };
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();
    core.setRawResultSink(.{
        .context = &probe,
        .on_result = RawResultQuiescenceProbe.oldResult,
    });
    try core.startup();

    const first_request = try core.addUserAsync(99_060);
    probe.waitOldEntered();

    var setter: RawResultSetterRunner = .{
        .core = &core,
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

    var first_response = try core.waitTakeResponse(first_request);
    defer first_response.deinit(allocator);
    const second_request = try core.addUserAsync(99_061);
    var second_response = try core.waitTakeResponse(second_request);
    defer second_response.deinit(allocator);

    const counts = probe.counts();
    try std.testing.expectEqual(@as(usize, 1), counts[0]);
    try std.testing.expectEqual(@as(usize, 1), counts[1]);
}

test "exchange core simple events replacement waits for old callback quiescence" {
    const allocator = std.testing.allocator;
    var probe: SimpleEventsQuiescenceProbe = .{ .io = std.testing.io };
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();
    core.setSimpleEventsHandler(.{
        .context = &probe,
        .on_command_result = SimpleEventsQuiescenceProbe.oldCommand,
    });
    try core.startup();

    const first_request = try core.addUserAsync(99_062);
    probe.waitOldEntered();
    var setter: SimpleEventsSetterRunner = .{
        .core = &core,
        .probe = &probe,
        .io = std.testing.io,
    };
    const setter_thread = try std.Thread.spawn(.{}, SimpleEventsSetterRunner.run, .{&setter});
    setter.waitEntered();
    try std.testing.expect(!setter.isDone());

    probe.releaseOld();
    setter_thread.join();
    try std.testing.expect(setter.isDone());

    var first_response = try core.waitTakeResponse(first_request);
    defer first_response.deinit(allocator);
    const second_request = try core.addUserAsync(99_063);
    var second_response = try core.waitTakeResponse(second_request);
    defer second_response.deinit(allocator);

    const counts = probe.counts();
    try std.testing.expectEqual(@as(usize, 1), counts[0]);
    try std.testing.expectEqual(@as(usize, 1), counts[1]);
}

test "exchange core raw callback reconfiguration controls later observers for the same command" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();
    var probe: RawReconfiguresLaterCallbacksProbe = .{ .core = &core };
    core.setResultSink(.{
        .context = &probe,
        .on_result = RawReconfiguresLaterCallbacksProbe.oldRuntime,
    });
    core.setSimpleEventsHandler(.{
        .context = &probe,
        .on_command_result = RawReconfiguresLaterCallbacksProbe.oldSimple,
    });
    core.setRawResultSink(.{
        .context = &probe,
        .on_result = RawReconfiguresLaterCallbacksProbe.raw,
    });

    var response = try core.addUser(99_063);
    defer response.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), probe.old_runtime_count);
    try std.testing.expectEqual(@as(usize, 1), probe.new_runtime_count);
    try std.testing.expectEqual(@as(usize, 0), probe.old_simple_count);
    try std.testing.expectEqual(@as(usize, 1), probe.new_simple_count);
}

test "exchange core cross-core callback reconfiguration fails instead of deadlocking" {
    const allocator = std.testing.allocator;
    var target_gate: BackpressureGate = .{ .io = std.testing.io };
    var target = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer target.deinit();
    target.setResultSink(.{
        .context = &target_gate,
        .on_result = BackpressureGate.callback,
    });
    try target.startup();
    _ = try target.addUserAsync(99_064);
    target_gate.waitEntered();

    var source = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer source.deinit();
    var probe: CrossCoreSetterProbe = .{ .target = &target };
    source.setResultSink(.{
        .context = &probe,
        .on_result = CrossCoreSetterProbe.onResult,
    });
    var response = try source.addUser(99_065);
    defer response.deinit(allocator);
    try std.testing.expectError(
        error.CallbackReconfigurationWouldBlock,
        probe.result.?,
    );

    target_gate.release();
    target.shutdown();
}

test "exchange core result sink clear suppresses stale simple events for the same command" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();
    var probe: SinkClearsSimpleEventsProbe = .{ .core = &core };
    core.setSimpleEventsHandler(.{
        .context = &probe,
        .on_command_result = SinkClearsSimpleEventsProbe.onCommand,
    });
    core.setResultSink(.{
        .context = &probe,
        .on_result = SinkClearsSimpleEventsProbe.sink,
    });

    var response = try core.addUser(99_065);
    defer response.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), probe.handler_count);
}

test "exchange core reentrant simple events replacement suppresses old trailing callbacks" {
    const allocator = std.testing.allocator;
    const symbol: i32 = 9_906;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .matching = .{ .orderbook_kind = .direct },
    });
    defer core.deinit();
    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(symbol)));

    var probe: ReplacingSimpleEventsProbe = .{ .core = &core };
    core.setSimpleEventsHandler(.{
        .context = &probe,
        .on_command_result = ReplacingSimpleEventsProbe.oldCommand,
        .on_order_book = ReplacingSimpleEventsProbe.oldOrderBook,
    });

    var first = try core.requestOrderBook(symbol, 4);
    defer first.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), probe.old_command_count);
    try std.testing.expectEqual(@as(usize, 0), probe.old_orderbook_count);
    try std.testing.expectEqual(@as(usize, 0), probe.new_command_count);
    try std.testing.expectEqual(@as(usize, 0), probe.new_orderbook_count);

    var second = try core.requestOrderBook(symbol, 4);
    defer second.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), probe.old_command_count);
    try std.testing.expectEqual(@as(usize, 0), probe.old_orderbook_count);
    try std.testing.expectEqual(@as(usize, 1), probe.new_command_count);
    try std.testing.expectEqual(@as(usize, 1), probe.new_orderbook_count);
}

test "exchange core worker callback never blocks itself on exhausted publication capacity" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .runtime = .{ .queue_capacity = 4, .max_batch_size = 1, .wait_strategy = .blocking },
    });
    defer core.deinit();
    var probe: FullRingCallbackProbe = .{ .core = &core, .io = std.testing.io };
    core.setResultSink(.{
        .context = &probe,
        .on_result = FullRingCallbackProbe.callback,
    });

    _ = try core.addUserAsync(99_075);
    try core.startup();
    probe.waitEntered();
    for (0..3) |index| _ = try core.addUserAsync(99_080 + @as(i64, @intCast(index)));
    probe.releaseAndWait();
    try std.testing.expectError(error.CoreReentrantCall, probe.result.?);
    core.shutdown();
}

test "exchange core bounded worker cannot outrun callback ownership publication" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .runtime = .{ .queue_capacity = 4, .max_batch_size = 1, .wait_strategy = .blocking },
    });
    defer core.deinit();
    try core.startup();

    var probe: FatalCallbackProbe = .{};
    const callback: CommandResponseCallback = .{
        .context = &probe,
        .callback = FatalCallbackProbe.onResponse,
        .failure_callback = FatalCallbackProbe.onFailure,
    };
    for (0..128) |index| {
        try core.submitApiCommandWithCallback(
            .{ .add_user = 99_200 + @as(i64, @intCast(index)) },
            callback,
        );
    }
    var barrier = try core.nop();
    defer barrier.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 128), probe.response_count);
    try std.testing.expectEqual(@as(usize, 0), probe.failure_count);
    try std.testing.expectEqual(@as(usize, 0), core.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, 0), core.asyncReadyCount());
}

test "exchange core callback publication ignores full poll-result storage" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .runtime = .{ .queue_capacity = 4, .max_batch_size = 1, .wait_strategy = .blocking },
    });
    defer core.deinit();

    var poll_ids: [4]u64 = undefined;
    for (&poll_ids, 0..) |*request_id, index| {
        request_id.* = try core.addUserAsync(99_400 + @as(i64, @intCast(index)));
    }
    try std.testing.expectEqual(@as(usize, 4), try core.drainAsyncPending());
    try std.testing.expectEqual(@as(usize, 4), core.asyncReadyCount());
    try std.testing.expectEqual(@as(usize, 4), core.driver.completion_slots_used);

    var probe: FatalCallbackProbe = .{};
    try core.submitApiCommandWithCallback(.{ .add_user = 99_404 }, .{
        .context = &probe,
        .callback = FatalCallbackProbe.onResponse,
        .failure_callback = FatalCallbackProbe.onFailure,
    });
    try std.testing.expectEqual(@as(usize, 1), try core.drainAsyncPending());
    try std.testing.expectEqual(@as(usize, 1), probe.response_count);
    try std.testing.expectEqual(@as(usize, 0), probe.failure_count);
    try std.testing.expectEqual(@as(usize, 4), core.asyncReadyCount());
    try std.testing.expectEqual(@as(usize, 4), core.driver.completion_slots_used);

    for (poll_ids) |request_id| {
        var response = core.takeAsyncResult(request_id) orelse return error.MissingAsyncResult;
        response.deinit(allocator);
    }
    try std.testing.expectEqual(@as(usize, 0), core.asyncReadyCount());
    try std.testing.expectEqual(@as(usize, 0), core.driver.completion_slots_used);
}

test "exchange core publishes global handlers before the request callback" {
    const allocator = std.testing.allocator;
    const symbol: i32 = 9_907;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .matching = .{ .orderbook_kind = .direct },
    });
    defer core.deinit();
    try std.testing.expect(try core.kernelRef().addSymbol(try makeExchangeSpecForSymbol(symbol)));

    var trace: CallbackOrderTrace = .{};
    core.setRawResultSink(.{
        .context = &trace,
        .on_result = CallbackOrderTrace.onRawResult,
    });
    core.setResultSink(.{
        .context = &trace,
        .on_result = CallbackOrderTrace.onResult,
    });
    core.setSimpleEventsHandler(.{
        .context = &trace,
        .on_command_result = CallbackOrderTrace.onSimpleCommand,
        .on_order_book = CallbackOrderTrace.onSimpleOrderBook,
    });
    try core.submitApiCommandWithCallback(.{
        .request_order_book = .{ .symbol = symbol, .depth = 4 },
    }, .{
        .context = &trace,
        .callback = CallbackOrderTrace.onRequest,
    });
    try std.testing.expectEqual(@as(usize, 1), try core.drainAsyncPending());

    try std.testing.expectEqual(@as(usize, 5), trace.count);
    try std.testing.expectEqualSlices(CallbackOrderTrace.Step, &.{
        .raw_result,
        .result_sink,
        .simple_command,
        .simple_orderbook,
        .request_callback,
    }, trace.steps[0..trace.count]);
    try std.testing.expect(trace.raw_saw_l2_snapshot);
    try std.testing.expect(trace.raw_sequence > 0);
    try std.testing.expect(trace.all_callbacks_scoped);
    try std.testing.expect(!runtime_mod.isCallbackExecutionActive());
}

test "exchange core real R2 corruption poisons worker and skips accepted tail" {
    const allocator = std.testing.allocator;
    const maker_uid: i64 = 99_101;
    const taker_uid: i64 = 99_102;
    const spec = try makeRuntimePoisonMarginSpec();
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .orders_processing = .{ .margin_trading_mode = .margin_trading_enabled },
    });
    defer core.deinit();
    try std.testing.expect(try core.kernelRef().addSymbol(spec));

    for ([_]i64{ maker_uid, taker_uid }) |uid| {
        var add = try core.addUser(uid);
        defer add.deinit(allocator);
        var deposit = try core.adjustUserBalance(uid, 1, spec.quote_currency, 100_000, .adjustment);
        defer deposit.deinit(allocator);
    }
    var maker = try core.placeOrder(91_001, maker_uid, spec.symbol_id, 10_770, 0, 1, .ask, .gtc, 0);
    defer maker.deinit(allocator);

    const maker_profile = core.kernelRef().risk_engine.getUserProfile(maker_uid) orelse unreachable;
    _ = maker_profile.removePosition(spec.symbol_id) orelse unreachable;
    const sequence_before = core.kernelRef().next_seq;
    const taker_balance_before = core.kernelRef().risk_engine.getUserProfile(taker_uid).?.accountBalance(spec.quote_currency);

    try core.startup();
    const failed_request = try core.placeOrderAsync(
        91_002,
        taker_uid,
        spec.symbol_id,
        10_770,
        10_770,
        1,
        .bid,
        .ioc,
        0,
    );
    const skipped_nop = try core.nopAsync();
    try std.testing.expectError(error.CorePoisoned, core.queryReport(.state_hash_detailed));

    const fatal = core.fatalInfo() orelse return error.MissingFatalRecord;
    try std.testing.expectEqual(error.PositionRecordCorrupted, fatal.cause);
    try std.testing.expectEqual(runtime_mod.FailureStage.r2, fatal.stage);
    try std.testing.expectEqual(@as(?u64, sequence_before + 1), fatal.sequence);
    try std.testing.expectEqual(@as(?u64, failed_request), fatal.request_id);
    try std.testing.expectEqual(@as(?cmd.OrderCommandType, .place_order), fatal.command);
    try std.testing.expect(!core.isAsyncResultReady(failed_request));
    try std.testing.expect(!core.isAsyncResultReady(skipped_nop));
    try std.testing.expectError(error.CorePoisoned, core.isAsyncResultReadyChecked(failed_request));
    try std.testing.expectError(error.CorePoisoned, core.takeAsyncResultChecked(failed_request));
    try std.testing.expectError(
        error.CorePoisoned,
        core.isApiCommandAsyncResultReadyChecked(.{ .request_id = skipped_nop }),
    );
    try std.testing.expectError(
        error.CorePoisoned,
        core.takePersistStateResultChecked(.{
            .matching_request_id = skipped_nop + 1,
            .risk_request_id = skipped_nop + 2,
        }),
    );
    try std.testing.expectEqual(sequence_before + 1, core.kernelRef().next_seq);
    try std.testing.expectEqual(
        taker_balance_before,
        core.kernelRef().risk_engine.getUserProfile(taker_uid).?.accountBalance(spec.quote_currency),
    );
    try std.testing.expectError(error.CorePoisoned, core.nopAsync());

    var book = try core.kernelRef().queryReport(.{
        .order_book = .{ .symbol = spec.symbol_id, .depth = 4 },
    });
    defer book.deinit(allocator);
    switch (book) {
        .order_book => |snapshot| try std.testing.expectEqual(@as(usize, 0), snapshot.asks.items.len),
        else => return error.UnexpectedReportVariant,
    }
}

test "exchange core fatal drain settles callback owners exactly once in accepted order" {
    const allocator = std.testing.allocator;
    const maker_uid: i64 = 99_111;
    const taker_uid: i64 = 99_112;
    const skipped_uid: i64 = 99_113;
    const skipped_detached_uid: i64 = 99_114;
    const spec = try makeRuntimePoisonMarginSpec();
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .orders_processing = .{ .margin_trading_mode = .margin_trading_enabled },
    });
    defer core.deinit();
    try std.testing.expect(try core.kernelRef().addSymbol(spec));

    for ([_]i64{ maker_uid, taker_uid }) |uid| {
        var add = try core.addUser(uid);
        defer add.deinit(allocator);
        var deposit = try core.adjustUserBalance(uid, 1, spec.quote_currency, 100_000, .adjustment);
        defer deposit.deinit(allocator);
    }
    var maker = try core.placeOrder(91_011, maker_uid, spec.symbol_id, 10_770, 0, 1, .ask, .gtc, 0);
    defer maker.deinit(allocator);
    const maker_profile = core.kernelRef().risk_engine.getUserProfile(maker_uid) orelse unreachable;
    _ = maker_profile.removePosition(spec.symbol_id) orelse unreachable;

    var probe: FatalCallbackProbe = .{};
    const callback: CommandResponseCallback = .{
        .context = &probe,
        .callback = FatalCallbackProbe.onResponse,
        .failure_callback = FatalCallbackProbe.onFailure,
    };
    try core.submitApiCommandWithCallback(.{
        .place_order = .{
            .order_id = 91_012,
            .uid = taker_uid,
            .symbol = spec.symbol_id,
            .price = 10_770,
            .reserve_bid_price = 10_770,
            .size = 1,
            .action = .bid,
            .order_type = .ioc,
            .user_cookie = 0,
        },
    }, callback);
    try core.submitApiCommandWithCallback(.{ .add_user = skipped_uid }, callback);
    try core.submitApiCommandDetached(.{ .add_user = skipped_detached_uid });
    const skipped_poll = try core.nopAsync();

    try std.testing.expectEqual(@as(usize, 2), core.pendingCallbackCount());
    try std.testing.expectError(error.CorePoisoned, core.drainAsyncPending());

    try std.testing.expectEqual(@as(usize, 0), probe.response_count);
    try std.testing.expectEqual(@as(usize, 2), probe.failure_count);
    try std.testing.expect(probe.all_failure_callbacks_scoped);
    try std.testing.expectEqual(CallbackFailureKind.command_failed, probe.kinds[0]);
    try std.testing.expectEqual(CallbackFailureKind.skipped_after_fatal, probe.kinds[1]);
    try std.testing.expectEqual(skipped_poll - 2, probe.request_ids[0]);
    try std.testing.expectEqual(skipped_poll - 1, probe.request_ids[1]);
    for (0..probe.failure_count) |index| {
        try std.testing.expectEqual(error.PositionRecordCorrupted, probe.causes[index]);
        try std.testing.expectEqual(@as(?u64, probe.request_ids[0]), probe.fatal_request_ids[index]);
    }
    try std.testing.expectEqual(@as(usize, 0), core.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, 0), core.asyncPendingCount());
    try std.testing.expectEqual(@as(usize, 0), core.asyncReadyCount());
    try std.testing.expectEqual(@as(usize, 0), core.driver.completion_slots_used);
    try std.testing.expectEqual(core.driver.accepted_total, core.driver.completed_total);
    try std.testing.expectEqual(@as(usize, 0), core.runtimePendingCount());
    try std.testing.expectError(error.CorePoisoned, core.takeAsyncResultChecked(skipped_poll));
    try std.testing.expect(core.kernelRef().risk_engine.getUserProfile(skipped_uid) == null);
    try std.testing.expect(core.kernelRef().risk_engine.getUserProfile(skipped_detached_uid) == null);
}

test "exchange core overlapping same-target drains claim async completions once" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();
    try core.startup();

    var gate: BoundedCommandCallbackGate = .{ .io = std.testing.io };
    defer gate.release();
    try core.submitApiCommandWithCallback(.{ .add_user = 99_115 }, .{
        .context = &gate,
        .callback = BoundedCommandCallbackGate.onResponse,
    });
    try gate.waitEntered();

    var first: AcceptedDrainRunner = .{ .core = &core, .io = std.testing.io };
    var second: AcceptedDrainRunner = .{ .core = &core, .io = std.testing.io };
    const first_thread = try std.Thread.spawn(.{}, AcceptedDrainRunner.run, .{&first});
    var first_thread_live = true;
    defer {
        gate.release();
        if (first_thread_live) first_thread.join();
    }
    const second_thread = try std.Thread.spawn(.{}, AcceptedDrainRunner.run, .{&second});
    var second_thread_live = true;
    defer {
        gate.release();
        if (second_thread_live) second_thread.join();
    }
    try first.waitStarted();
    try second.waitStarted();
    try waitForAcceptedDrainWaiters(&core, 2);

    gate.release();
    try first.waitFinished();
    try second.waitFinished();
    first_thread.join();
    first_thread_live = false;
    second_thread.join();
    second_thread_live = false;

    const first_count = try first.result.?;
    const second_count = try second.result.?;
    try std.testing.expectEqual(@as(usize, 1), first_count + second_count);
    try std.testing.expect(
        (first_count == 1 and second_count == 0) or
            (first_count == 0 and second_count == 1),
    );
    try std.testing.expectEqual(@as(u64, 1), core.driver.async_drained_total);
    try std.testing.expectEqual(core.driver.accepted_total, core.driver.settled_prefix_total);
    try std.testing.expectEqual(@as(usize, 0), core.pendingCallbackCount());
}

test "exchange core overlapping staggered drains advance async frontier monotonically" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();
    try core.startup();

    var gate: BoundedCommandCallbackGate = .{ .io = std.testing.io };
    defer gate.release();
    try core.submitApiCommandWithCallback(.{ .add_user = 99_116 }, .{
        .context = &gate,
        .callback = BoundedCommandCallbackGate.onResponse,
    });
    try gate.waitEntered();

    var first: AcceptedDrainRunner = .{ .core = &core, .io = std.testing.io };
    const first_thread = try std.Thread.spawn(.{}, AcceptedDrainRunner.run, .{&first});
    var first_thread_live = true;
    defer {
        gate.release();
        if (first_thread_live) first_thread.join();
    }
    try first.waitStarted();
    try waitForAcceptedDrainWaiters(&core, 1);

    const second_request = try core.addUserAsync(99_117);
    var second: AcceptedDrainRunner = .{ .core = &core, .io = std.testing.io };
    const second_thread = try std.Thread.spawn(.{}, AcceptedDrainRunner.run, .{&second});
    var second_thread_live = true;
    defer {
        gate.release();
        if (second_thread_live) second_thread.join();
    }
    try second.waitStarted();
    try waitForAcceptedDrainWaiters(&core, 2);

    gate.release();
    try first.waitFinished();
    try second.waitFinished();
    first_thread.join();
    first_thread_live = false;
    second_thread.join();
    second_thread_live = false;

    const first_count = try first.result.?;
    const second_count = try second.result.?;
    try std.testing.expectEqual(@as(usize, 2), first_count + second_count);
    try std.testing.expectEqual(@as(u64, 2), core.driver.async_drained_total);
    try std.testing.expectEqual(core.driver.accepted_total, core.driver.settled_prefix_total);
    var second_response = core.takeAsyncResult(second_request) orelse
        return error.MissingAsyncResult;
    defer second_response.deinit(allocator);
}

test "exchange core overlapping same-target runtime drains claim completions once" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();

    var gate: BoundedResultGate = .{ .io = std.testing.io };
    defer gate.release();
    core.setResultSink(.{
        .context = &gate,
        .on_result = BoundedResultGate.onResult,
    });
    try core.enqueueRuntime(makeAddUserCommand(99_141));
    try core.startup();
    try gate.waitEntered();

    var first: RuntimeDrainRunner = .{ .core = &core, .io = std.testing.io };
    var second: RuntimeDrainRunner = .{ .core = &core, .io = std.testing.io };
    const first_thread = try std.Thread.spawn(.{}, RuntimeDrainRunner.run, .{&first});
    var first_thread_live = true;
    defer {
        gate.release();
        if (first_thread_live) first_thread.join();
    }
    const second_thread = try std.Thread.spawn(.{}, RuntimeDrainRunner.run, .{&second});
    var second_thread_live = true;
    defer {
        gate.release();
        if (second_thread_live) second_thread.join();
    }
    try first.waitStarted();
    try second.waitStarted();
    try waitForAcceptedDrainWaiters(&core, 2);

    gate.release();
    try first.waitFinished();
    try second.waitFinished();
    first_thread.join();
    first_thread_live = false;
    second_thread.join();
    second_thread_live = false;

    const first_outcome = try first.result.?;
    const second_outcome = try second.result.?;
    try std.testing.expectEqual(
        @as(usize, 1),
        first_outcome.commands_processed + second_outcome.commands_processed,
    );
    try std.testing.expectEqual(
        @as(usize, 1),
        first_outcome.groups_processed + second_outcome.groups_processed,
    );
    try std.testing.expectEqual(@as(u64, 1), core.driver.runtime_drained_total);
    try std.testing.expectEqual(
        core.driver.runtime_accepted_total,
        core.driver.runtime_completed_total,
    );
    try std.testing.expectEqual(core.driver.accepted_total, core.driver.settled_prefix_total);
    try std.testing.expectEqual(@as(usize, 0), core.driver.accepted_drain_waiters);
}

test "exchange core overlapping staggered runtime drains advance frontier monotonically" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();

    var gate: BoundedResultGate = .{ .io = std.testing.io };
    defer gate.release();
    core.setResultSink(.{
        .context = &gate,
        .on_result = BoundedResultGate.onResult,
    });
    try core.enqueueRuntime(makeAddUserCommand(99_142));
    try core.startup();
    try gate.waitEntered();

    var first: RuntimeDrainRunner = .{ .core = &core, .io = std.testing.io };
    const first_thread = try std.Thread.spawn(.{}, RuntimeDrainRunner.run, .{&first});
    var first_thread_live = true;
    defer {
        gate.release();
        if (first_thread_live) first_thread.join();
    }
    try first.waitStarted();
    try waitForAcceptedDrainWaiters(&core, 1);

    try core.enqueueRuntime(makeAddUserCommand(99_143));
    var second: RuntimeDrainRunner = .{ .core = &core, .io = std.testing.io };
    const second_thread = try std.Thread.spawn(.{}, RuntimeDrainRunner.run, .{&second});
    var second_thread_live = true;
    defer {
        gate.release();
        if (second_thread_live) second_thread.join();
    }
    try second.waitStarted();
    try waitForAcceptedDrainWaiters(&core, 2);

    gate.release();
    try first.waitFinished();
    try second.waitFinished();
    first_thread.join();
    first_thread_live = false;
    second_thread.join();
    second_thread_live = false;

    const first_outcome = try first.result.?;
    const second_outcome = try second.result.?;
    try std.testing.expectEqual(
        @as(usize, 2),
        first_outcome.commands_processed + second_outcome.commands_processed,
    );
    try std.testing.expectEqual(@as(u64, 2), core.driver.runtime_drained_total);
    try std.testing.expectEqual(
        core.driver.runtime_accepted_total,
        core.driver.runtime_completed_total,
    );
    try std.testing.expectEqual(core.driver.accepted_total, core.driver.settled_prefix_total);
    try std.testing.expectEqual(@as(usize, 0), core.driver.accepted_drain_waiters);
}

test "exchange core cold async drain reclaims ownership after late publication" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer core.deinit();

    var gate: BoundedResultGate = .{ .io = std.testing.io };
    defer gate.release();
    core.setResultSink(.{
        .context = &gate,
        .on_result = BoundedResultGate.onResult,
    });
    try core.enqueueRuntime(makeAddUserCommand(99_118));

    var runtime_runner: RuntimeDrainRunner = .{
        .core = &core,
        .io = std.testing.io,
    };
    const runtime_thread = try std.Thread.spawn(
        .{},
        RuntimeDrainRunner.run,
        .{&runtime_runner},
    );
    var runtime_thread_live = true;
    defer {
        gate.release();
        if (runtime_thread_live) runtime_thread.join();
    }
    try runtime_runner.waitStarted();
    try gate.waitEntered();

    const late_request = try core.addUserAsync(99_119);
    var async_runner: AcceptedDrainRunner = .{
        .core = &core,
        .io = std.testing.io,
    };
    const async_thread = try std.Thread.spawn(
        .{},
        AcceptedDrainRunner.run,
        .{&async_runner},
    );
    var async_thread_live = true;
    defer {
        gate.release();
        if (async_thread_live) {
            // A regression in cold ownership reclaim leaves this waiter with
            // no consumer. Starting the worker during timeout cleanup makes
            // the test fail boundedly instead of hanging in its deferred join.
            core.startup() catch {};
            async_thread.join();
        }
    }
    try async_runner.waitStarted();
    try waitForAcceptedDrainWaiters(&core, 1);

    gate.release();
    try runtime_runner.waitFinished();
    try async_runner.waitFinished();
    runtime_thread.join();
    runtime_thread_live = false;
    async_thread.join();
    async_thread_live = false;

    const runtime_outcome = try runtime_runner.result.?;
    try std.testing.expectEqual(@as(usize, 1), runtime_outcome.commands_processed);
    try std.testing.expectEqual(@as(usize, 1), try async_runner.result.?);
    var late_response = core.takeAsyncResult(late_request) orelse
        return error.MissingAsyncResult;
    defer late_response.deinit(allocator);
    try std.testing.expectEqual(core.driver.accepted_total, core.driver.settled_prefix_total);
    try std.testing.expectEqual(@as(usize, 0), core.driver.accepted_drain_waiters);
}

test "exchange core globally rejects cross-backend callback drains" {
    const allocator = std.testing.allocator;
    var target = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer target.deinit();
    var source = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer source.deinit();

    var probe: CrossBackendDrainProbe = .{ .target = &target };
    source.setResultSink(.{
        .context = &probe,
        .on_result = CrossBackendDrainProbe.onResult,
    });
    var response = try source.addUser(99_120);
    defer response.deinit(allocator);

    try std.testing.expectError(
        error.CoreReentrantCall,
        probe.async_result orelse return error.MissingReentrantDrainResult,
    );
    try std.testing.expectError(
        error.CoreReentrantCall,
        probe.runtime_result orelse return error.MissingReentrantDrainResult,
    );
    try std.testing.expectEqual(@as(u64, 0), target.driver.accepted_total);
    try std.testing.expect(!target.driver.manual_drain_active);
}

test "exchange core foreign callback rejects blocking surfaces before publication" {
    const allocator = std.testing.allocator;
    var target = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer target.deinit();
    var source = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer source.deinit();

    const accepted_before = target.driver.accepted_total;
    const async_before = target.driver.async_accepted_total;
    const request_before = target.api.next_request_id;
    const sequence_before = target.driver.publication_sequencer.highWater();
    const queued_before = target.driver.queue.count();
    const completions_before = target.driver.completion_slots_used;

    var probe: CrossBackendBlockingProbe = .{
        .target = &target,
        .allocator = allocator,
    };
    source.setResultSink(.{
        .context = &probe,
        .on_result = CrossBackendBlockingProbe.onResult,
    });
    var response = try source.addUser(99_130);
    defer response.deinit(allocator);

    try std.testing.expectEqual(error.CoreReentrantCall, probe.startup_error.?);
    try std.testing.expectEqual(error.CoreReentrantCall, probe.bootstrap_error.?);
    try std.testing.expectEqual(error.CoreReentrantCall, probe.submit_error.?);
    try std.testing.expectEqual(error.CoreReentrantCall, probe.order_book_error.?);
    try std.testing.expectEqual(error.CoreReentrantCall, probe.query_error.?);
    try std.testing.expectEqual(error.CoreReentrantCall, probe.wait_error.?);
    try std.testing.expectEqual(error.CoreReentrantCall, probe.composite_error.?);
    try std.testing.expectEqual(error.CoreReentrantCall, probe.nop_composite_error.?);
    try std.testing.expectEqual(
        error.EmptyApiCommandBatch,
        probe.empty_api_composite_error.?,
    );
    try std.testing.expect(probe.empty_command_batch_ok);
    try std.testing.expectEqual(@as(?anyerror, null), probe.empty_command_batch_error);

    try std.testing.expectEqual(accepted_before, target.driver.accepted_total);
    try std.testing.expectEqual(async_before, target.driver.async_accepted_total);
    try std.testing.expectEqual(request_before, target.api.next_request_id);
    try std.testing.expectEqual(
        sequence_before,
        target.driver.publication_sequencer.highWater(),
    );
    try std.testing.expectEqual(queued_before, target.driver.queue.count());
    try std.testing.expectEqual(completions_before, target.driver.completion_slots_used);
    try std.testing.expectEqual(runtime_mod.LifecycleState.cold, target.runtime.lifecycleState());
}

test "exchange core callback cycle permits forward publication and rejects blocking return" {
    const allocator = std.testing.allocator;
    var source = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer source.deinit();
    var target = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer target.deinit();

    var probe: CrossBackendCycleProbe = .{
        .source = &source,
        .target = &target,
        .allocator = allocator,
    };
    source.setResultSink(.{
        .context = &probe,
        .on_result = CrossBackendCycleProbe.onSourceResult,
    });

    var source_response = try source.addUser(99_140);
    defer source_response.deinit(allocator);
    try std.testing.expect(probe.forward_published);
    try std.testing.expectEqual(@as(?anyerror, null), probe.forward_error);
    try std.testing.expectEqual(@as(u64, 1), target.driver.accepted_total);
    try std.testing.expectEqual(@as(usize, 1), target.pendingCallbackCount());

    const source_accepted_before_return = source.driver.accepted_total;
    const source_sequence_before_return =
        source.driver.publication_sequencer.highWater();
    const source_request_before_return = source.api.next_request_id;
    _ = try target.drainAsyncPending();

    try std.testing.expectEqual(error.CoreReentrantCall, probe.return_error.?);
    try std.testing.expectEqual(
        source_accepted_before_return,
        source.driver.accepted_total,
    );
    try std.testing.expectEqual(
        source_sequence_before_return,
        source.driver.publication_sequencer.highWater(),
    );
    try std.testing.expectEqual(source_request_before_return, source.api.next_request_id);
    try std.testing.expectEqual(@as(usize, 0), target.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, 0), target.asyncReadyCount());
}

test "exchange core foreign callback rejects contended async publication without mutation" {
    const allocator = std.testing.allocator;
    var target = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer target.deinit();
    var source = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer source.deinit();

    var probe: CrossBackendAsyncPublishProbe = .{
        .target = &target,
        .uid = 99_145,
    };
    source.setResultSink(.{
        .context = &probe,
        .on_result = CrossBackendAsyncPublishProbe.onResult,
    });

    target.driver.mutex.lockUncancelable(target.driver.io);
    var target_lock_held = true;
    defer if (target_lock_held) target.driver.mutex.unlock(target.driver.io);
    const accepted_before = target.driver.accepted_total;
    const async_before = target.driver.async_accepted_total;
    const request_before = target.api.next_request_id;
    const sequence_before = target.driver.publication_sequencer.highWater();
    const queued_before = target.driver.queue.count();
    const completions_before = target.driver.completion_slots_used;

    var source_response = try source.addUser(99_144);
    defer source_response.deinit(allocator);
    try std.testing.expectError(
        error.CoreReentrantCall,
        probe.result orelse return error.MissingReentrantPublicationResult,
    );
    try std.testing.expectEqual(accepted_before, target.driver.accepted_total);
    try std.testing.expectEqual(async_before, target.driver.async_accepted_total);
    try std.testing.expectEqual(request_before, target.api.next_request_id);
    try std.testing.expectEqual(
        sequence_before,
        target.driver.publication_sequencer.highWater(),
    );
    try std.testing.expectEqual(queued_before, target.driver.queue.count());
    try std.testing.expectEqual(completions_before, target.driver.completion_slots_used);

    target.driver.mutex.unlock(target.driver.io);
    target_lock_held = false;
}

test "exchange core foreign callback rejects saturated async publication without mutation" {
    const allocator = std.testing.allocator;
    var target = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .runtime = .{
            .queue_capacity = 4,
            .max_batch_size = 1,
            .wait_strategy = .blocking,
        },
    });
    defer target.deinit();
    try target.startup();

    var request_ids: [4]u64 = undefined;
    for (&request_ids, 0..) |*request_id, index| {
        request_id.* = try target.addUserAsync(
            99_146 + @as(i64, @intCast(index)),
        );
    }
    _ = try target.drainAsyncPending();
    try std.testing.expectEqual(@as(usize, 4), target.asyncReadyCount());

    const accepted_before = target.driver.accepted_total;
    const async_before = target.driver.async_accepted_total;
    const completed_before = target.driver.completed_total;
    const request_before = target.api.next_request_id;
    const sequence_before = target.driver.publication_sequencer.highWater();
    const queued_before = target.driver.queue.count();
    const completions_before = target.driver.completion_slots_used;
    const blocked_before = target.driver.blocked_producers;

    var source = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer source.deinit();
    var probe: CrossBackendAsyncPublishProbe = .{
        .target = &target,
        .uid = 99_150,
    };
    source.setResultSink(.{
        .context = &probe,
        .on_result = CrossBackendAsyncPublishProbe.onResult,
    });
    var source_response = try source.addUser(99_151);
    defer source_response.deinit(allocator);

    try std.testing.expectError(
        error.CoreReentrantCall,
        probe.result orelse return error.MissingReentrantPublicationResult,
    );
    try std.testing.expectEqual(accepted_before, target.driver.accepted_total);
    try std.testing.expectEqual(async_before, target.driver.async_accepted_total);
    try std.testing.expectEqual(completed_before, target.driver.completed_total);
    try std.testing.expectEqual(request_before, target.api.next_request_id);
    try std.testing.expectEqual(
        sequence_before,
        target.driver.publication_sequencer.highWater(),
    );
    try std.testing.expectEqual(queued_before, target.driver.queue.count());
    try std.testing.expectEqual(completions_before, target.driver.completion_slots_used);
    try std.testing.expectEqual(blocked_before, target.driver.blocked_producers);
    try std.testing.expectEqual(@as(usize, 4), target.asyncReadyCount());

    for (request_ids) |request_id| {
        var response = target.takeAsyncResult(request_id) orelse
            return error.MissingAsyncResult;
        response.deinit(allocator);
    }
}

test "exchange core foreign callback rejects running binary before partial admission" {
    const allocator = std.testing.allocator;
    var target = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .runtime = .{
            .queue_capacity = 4,
            .max_batch_size = 1,
            .wait_strategy = .blocking,
        },
    });
    defer target.deinit();

    var gate: BoundedResultGate = .{ .io = std.testing.io };
    defer gate.release();
    target.setResultSink(.{
        .context = &gate,
        .on_result = BoundedResultGate.onResult,
    });
    try target.startup();
    const first_request = try target.addUserAsync(99_153);
    try gate.waitEntered();
    for (0..4) |index| {
        try target.enqueueRuntime(makeAddUserCommand(
            99_154 + @as(i64, @intCast(index)),
        ));
    }

    const query_body = try encodeStateHashReportPayload(allocator);
    defer allocator.free(query_body);
    const payload = try padBinaryPayloadToFrameCount(allocator, query_body, 12);
    defer allocator.free(payload);

    const accepted_before = target.driver.accepted_total;
    const async_before = target.driver.async_accepted_total;
    const runtime_before = target.driver.runtime_accepted_total;
    const request_before = target.api.next_request_id;
    const sequence_before = target.driver.publication_sequencer.highWater();
    const queued_before = target.driver.queue.count();
    const completions_before = target.driver.completion_slots_used;
    const journal_before = target.driver.journal_slots_reserved;
    const blocked_before = target.driver.blocked_producers;

    var source = try ExchangeCore.initWithIo(allocator, std.testing.io, .{});
    defer source.deinit();
    var probe: CrossBackendBinaryPublishProbe = .{
        .target = &target,
        .payload = payload,
    };
    source.setResultSink(.{
        .context = &probe,
        .on_result = CrossBackendBinaryPublishProbe.onResult,
    });
    var source_response = try source.addUser(99_160);
    defer source_response.deinit(allocator);

    try std.testing.expectError(
        error.CoreReentrantCall,
        probe.result orelse return error.MissingReentrantPublicationResult,
    );
    try std.testing.expectEqual(accepted_before, target.driver.accepted_total);
    try std.testing.expectEqual(async_before, target.driver.async_accepted_total);
    try std.testing.expectEqual(runtime_before, target.driver.runtime_accepted_total);
    try std.testing.expectEqual(request_before, target.api.next_request_id);
    try std.testing.expectEqual(
        sequence_before,
        target.driver.publication_sequencer.highWater(),
    );
    try std.testing.expectEqual(queued_before, target.driver.queue.count());
    try std.testing.expectEqual(completions_before, target.driver.completion_slots_used);
    try std.testing.expectEqual(journal_before, target.driver.journal_slots_reserved);
    try std.testing.expectEqual(blocked_before, target.driver.blocked_producers);
    try std.testing.expect(!target.driver.active_binary_stream);

    gate.release();
    _ = try target.drainAsyncPending();
    var first_response = target.takeAsyncResult(first_request) orelse
        return error.MissingAsyncResult;
    first_response.deinit(allocator);
}

test "exchange core post-target binary abort cannot bypass blocked fatal callback" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var core = try ExchangeCore.initWithIoAndPersistenceRoot(
        allocator,
        std.testing.io,
        tmp.dir,
        .{
            .initial_state = config_mod.InitialStateConfig.cleanStart("EXDRAINPREFIX"),
            .serialization = .{
                .enable_journaling = true,
                .backend = .disk,
                .storage_folder = "drain-prefix",
            },
            .runtime = .{
                .queue_capacity = 4,
                .max_batch_size = 1,
                .wait_strategy = .blocking,
            },
        },
    );
    defer core.deinit();

    var first_gate: BoundedResultGate = .{ .io = std.testing.io };
    defer first_gate.release();
    core.setResultSink(.{
        .context = &first_gate,
        .on_result = BoundedResultGate.onResult,
    });
    try core.startup();
    const first_request = try core.addUserAsync(99_125);
    try first_gate.waitEntered();

    core.durable_journal.?.injectSyncFailureForTesting();
    var fatal_gate: FatalDrainCallbackGate = .{ .io = std.testing.io };
    defer fatal_gate.release();
    try core.submitApiCommandWithCallback(.{ .add_user = 99_126 }, .{
        .context = &fatal_gate,
        .callback = FatalDrainCallbackGate.onResponse,
        .failure_callback = FatalDrainCallbackGate.onFailure,
    });
    const accepted_target = core.driver.accepted_total;

    var drain_runner: AcceptedDrainRunner = .{
        .core = &core,
        .io = std.testing.io,
    };
    const drain_thread = try std.Thread.spawn(
        .{},
        AcceptedDrainRunner.run,
        .{&drain_runner},
    );
    var drain_thread_live = true;
    defer {
        first_gate.release();
        fatal_gate.release();
        if (drain_thread_live) drain_thread.join();
    }
    try drain_runner.waitStarted();
    try waitForAcceptedDrainWaiters(&core, 1);

    const query_body = try encodeStateHashReportPayload(allocator);
    defer allocator.free(query_body);
    const payload = try padBinaryPayloadToFrameCount(allocator, query_body, 12);
    defer allocator.free(payload);
    var publisher: TrackedBinaryAsyncPublisher = .{
        .core = &core,
        .io = std.testing.io,
        .payload = payload,
        .transfer_id = 99_127,
    };
    const publisher_thread = try std.Thread.spawn(
        .{},
        TrackedBinaryAsyncPublisher.run,
        .{&publisher},
    );
    var publisher_thread_live = true;
    defer {
        first_gate.release();
        fatal_gate.release();
        if (publisher_thread_live) publisher_thread.join();
    }
    try waitForActiveBinaryStream(&core);

    first_gate.release();
    try fatal_gate.waitEntered();
    try publisher.waitFinished();
    publisher_thread.join();
    publisher_thread_live = false;
    try std.testing.expectError(error.CorePoisoned, publisher.result.?);

    const observed = blk: {
        core.driver.mutex.lockUncancelable(core.driver.io);
        defer core.driver.mutex.unlock(core.driver.io);
        break :blk .{
            core.driver.completed_total,
            core.driver.settled_prefix_total,
            core.driver.deferred_settlement,
        };
    };
    try std.testing.expect(observed[0] >= accepted_target);
    try std.testing.expect(observed[1] < accepted_target);
    const deferred = observed[2] orelse return error.MissingDeferredSettlement;
    try std.testing.expect(deferred.prefix_before > observed[1] + 1);
    try std.testing.expect(!drain_runner.finished.isSet());

    fatal_gate.release();
    try drain_runner.waitFinished();
    drain_thread.join();
    drain_thread_live = false;
    try std.testing.expectError(error.CorePoisoned, drain_runner.result.?);

    core.shutdown();
    var first_response = core.takeAsyncResult(first_request) orelse
        return error.MissingAsyncResult;
    defer first_response.deinit(allocator);
    try std.testing.expectEqual(core.driver.accepted_total, core.driver.completed_total);
    try std.testing.expectEqual(
        core.driver.accepted_total,
        core.driver.settled_prefix_total,
    );
    try std.testing.expect(core.driver.deferred_settlement == null);
}

test "exchange core running poisoned runtime drain awaits terminal callback settlement" {
    const allocator = std.testing.allocator;
    const maker_uid: i64 = 99_151;
    const taker_uid: i64 = 99_152;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .orders_processing = .{ .margin_trading_mode = .margin_trading_enabled },
    });
    defer core.deinit();
    const spec = try prepareRuntimePoisonScenario(
        &core,
        allocator,
        maker_uid,
        taker_uid,
        91_051,
    );

    var gate: FatalDrainCallbackGate = .{ .io = std.testing.io };
    defer gate.release();
    const callback: CommandResponseCallback = .{
        .context = &gate,
        .callback = FatalDrainCallbackGate.onResponse,
        .failure_callback = FatalDrainCallbackGate.onFailure,
    };
    try core.submitApiCommandWithCallback(.{
        .place_order = .{
            .order_id = 91_052,
            .uid = taker_uid,
            .symbol = spec.symbol_id,
            .price = 10_770,
            .reserve_bid_price = 10_770,
            .size = 1,
            .action = .bid,
            .order_type = .ioc,
            .user_cookie = 0,
        },
    }, callback);
    try core.submitApiCommandWithCallback(.{ .add_user = 99_153 }, callback);
    try core.enqueueRuntime(makeAddUserCommand(99_154));
    const accepted_target = core.driver.accepted_total;
    const runtime_target = core.driver.runtime_accepted_total;

    try core.startup();
    try gate.waitEntered();

    var runner: RuntimeDrainRunner = .{
        .core = &core,
        .io = std.testing.io,
    };
    const drain_thread = try std.Thread.spawn(.{}, RuntimeDrainRunner.run, .{&runner});
    var drain_thread_live = true;
    defer {
        gate.release();
        if (drain_thread_live) drain_thread.join();
    }
    try runner.waitStarted();
    try waitForAcceptedDrainWaiters(&core, 1);

    try std.testing.expect(!runner.finished.isSet());
    try std.testing.expect(core.driver.settled_prefix_total < accepted_target);
    try std.testing.expectEqual(@as(usize, 1), core.runtimePendingCount());
    gate.release();

    try runner.waitFinished();
    drain_thread.join();
    drain_thread_live = false;
    try std.testing.expectError(error.CorePoisoned, runner.result.?);
    try std.testing.expectEqual(@as(usize, 0), gate.response_count);
    try std.testing.expectEqual(@as(usize, 2), gate.failure_count);
    try std.testing.expectEqual(CallbackFailureKind.command_failed, gate.kinds[0]);
    try std.testing.expectEqual(CallbackFailureKind.skipped_after_fatal, gate.kinds[1]);
    try std.testing.expectEqual(@as(usize, 0), core.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, 0), core.runtimePendingCount());
    try std.testing.expectEqual(@as(u64, 0), core.driver.runtime_drained_total);
    try std.testing.expectEqual(runtime_target, core.driver.runtime_completed_total);
    try std.testing.expectEqual(accepted_target, core.driver.completed_total);
    try std.testing.expectEqual(accepted_target, core.driver.settled_prefix_total);
    try std.testing.expectEqual(@as(usize, 0), core.driver.accepted_drain_waiters);
}

test "exchange core cold poisoned runtime drains await terminal callback settlement" {
    const allocator = std.testing.allocator;
    const maker_uid: i64 = 99_161;
    const taker_uid: i64 = 99_162;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .orders_processing = .{ .margin_trading_mode = .margin_trading_enabled },
    });
    defer core.deinit();
    const spec = try prepareRuntimePoisonScenario(
        &core,
        allocator,
        maker_uid,
        taker_uid,
        91_061,
    );

    var gate: FatalDrainCallbackGate = .{ .io = std.testing.io };
    defer gate.release();
    const callback: CommandResponseCallback = .{
        .context = &gate,
        .callback = FatalDrainCallbackGate.onResponse,
        .failure_callback = FatalDrainCallbackGate.onFailure,
    };
    try core.submitApiCommandWithCallback(.{
        .place_order = .{
            .order_id = 91_062,
            .uid = taker_uid,
            .symbol = spec.symbol_id,
            .price = 10_770,
            .reserve_bid_price = 10_770,
            .size = 1,
            .action = .bid,
            .order_type = .ioc,
            .user_cookie = 0,
        },
    }, callback);
    try core.submitApiCommandWithCallback(.{ .add_user = 99_163 }, callback);
    try core.enqueueRuntime(makeAddUserCommand(99_164));
    const accepted_target = core.driver.accepted_total;
    const runtime_target = core.driver.runtime_accepted_total;

    var owner: RuntimeDrainRunner = .{
        .core = &core,
        .io = std.testing.io,
    };
    const owner_thread = try std.Thread.spawn(.{}, RuntimeDrainRunner.run, .{&owner});
    var owner_thread_live = true;
    defer {
        gate.release();
        if (owner_thread_live) owner_thread.join();
    }
    try owner.waitStarted();
    try gate.waitEntered();

    var waiter: RuntimeDrainRunner = .{
        .core = &core,
        .io = std.testing.io,
    };
    const waiter_thread = try std.Thread.spawn(.{}, RuntimeDrainRunner.run, .{&waiter});
    var waiter_thread_live = true;
    defer {
        gate.release();
        if (waiter_thread_live) waiter_thread.join();
    }
    try waiter.waitStarted();
    try waitForAcceptedDrainWaiters(&core, 1);

    try std.testing.expect(core.driver.manual_drain_active);
    try std.testing.expect(!owner.finished.isSet());
    try std.testing.expect(!waiter.finished.isSet());
    try std.testing.expect(core.driver.settled_prefix_total < accepted_target);
    gate.release();

    try owner.waitFinished();
    try waiter.waitFinished();
    owner_thread.join();
    owner_thread_live = false;
    waiter_thread.join();
    waiter_thread_live = false;
    try std.testing.expectError(error.CorePoisoned, owner.result.?);
    try std.testing.expectError(error.CorePoisoned, waiter.result.?);
    try std.testing.expectEqual(@as(usize, 0), gate.response_count);
    try std.testing.expectEqual(@as(usize, 2), gate.failure_count);
    try std.testing.expectEqual(CallbackFailureKind.command_failed, gate.kinds[0]);
    try std.testing.expectEqual(CallbackFailureKind.skipped_after_fatal, gate.kinds[1]);
    try std.testing.expectEqual(@as(usize, 0), core.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, 0), core.runtimePendingCount());
    try std.testing.expectEqual(@as(u64, 0), core.driver.runtime_drained_total);
    try std.testing.expectEqual(runtime_target, core.driver.runtime_completed_total);
    try std.testing.expectEqual(accepted_target, core.driver.completed_total);
    try std.testing.expectEqual(accepted_target, core.driver.settled_prefix_total);
    try std.testing.expectEqual(@as(usize, 0), core.driver.accepted_drain_waiters);
}

test "exchange core running poisoned drain awaits its accepted terminal prefix" {
    const allocator = std.testing.allocator;
    const maker_uid: i64 = 99_121;
    const taker_uid: i64 = 99_122;
    const skipped_uid: i64 = 99_123;
    const skipped_detached_uid: i64 = 99_124;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .orders_processing = .{ .margin_trading_mode = .margin_trading_enabled },
    });
    defer core.deinit();
    const spec = try prepareRuntimePoisonScenario(
        &core,
        allocator,
        maker_uid,
        taker_uid,
        91_021,
    );

    var gate: FatalDrainCallbackGate = .{ .io = std.testing.io };
    defer gate.release();
    const callback: CommandResponseCallback = .{
        .context = &gate,
        .callback = FatalDrainCallbackGate.onResponse,
        .failure_callback = FatalDrainCallbackGate.onFailure,
    };
    try core.submitApiCommandWithCallback(.{
        .place_order = .{
            .order_id = 91_022,
            .uid = taker_uid,
            .symbol = spec.symbol_id,
            .price = 10_770,
            .reserve_bid_price = 10_770,
            .size = 1,
            .action = .bid,
            .order_type = .ioc,
            .user_cookie = 0,
        },
    }, callback);
    try core.submitApiCommandWithCallback(.{ .add_user = skipped_uid }, callback);
    try core.submitApiCommandDetached(.{ .add_user = skipped_detached_uid });
    const skipped_poll = try core.nopAsync();
    const accepted_target = core.driver.accepted_total;

    try core.startup();
    try gate.waitEntered();

    var runner: AcceptedDrainRunner = .{
        .core = &core,
        .io = std.testing.io,
    };
    const drain_thread = try std.Thread.spawn(.{}, AcceptedDrainRunner.run, .{&runner});
    var drain_thread_live = true;
    defer {
        gate.release();
        if (drain_thread_live) drain_thread.join();
    }
    try runner.waitStarted();
    try waitForAcceptedDrainWaiters(&core, 1);
    try std.testing.expect(!runner.finished.isSet());
    try std.testing.expect(core.driver.settled_prefix_total < accepted_target);

    gate.release();
    try runner.waitFinished();
    drain_thread.join();
    drain_thread_live = false;
    try std.testing.expect(runner.finished.isSet());
    try std.testing.expectError(error.CorePoisoned, runner.result.?);

    try std.testing.expectEqual(@as(usize, 0), gate.response_count);
    try std.testing.expectEqual(@as(usize, 2), gate.failure_count);
    try std.testing.expectEqual(CallbackFailureKind.command_failed, gate.kinds[0]);
    try std.testing.expectEqual(CallbackFailureKind.skipped_after_fatal, gate.kinds[1]);
    try std.testing.expectEqual(skipped_poll - 2, gate.request_ids[0]);
    try std.testing.expectEqual(skipped_poll - 1, gate.request_ids[1]);
    try std.testing.expectEqual(@as(usize, 0), core.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, 0), core.asyncPendingCount());
    try std.testing.expectEqual(@as(usize, 0), core.asyncReadyCount());
    try std.testing.expectEqual(@as(usize, 0), core.driver.completion_slots_used);
    try std.testing.expectEqual(@as(u64, 0), core.driver.async_drained_total);
    try std.testing.expectEqual(@as(usize, 0), core.driver.accepted_drain_waiters);
    try std.testing.expectEqual(accepted_target, core.driver.completed_total);
    try std.testing.expectEqual(accepted_target, core.driver.settled_prefix_total);
    try std.testing.expectError(error.CorePoisoned, core.takeAsyncResultChecked(skipped_poll));
    try std.testing.expect(core.kernelRef().risk_engine.getUserProfile(skipped_uid) == null);
    try std.testing.expect(core.kernelRef().risk_engine.getUserProfile(skipped_detached_uid) == null);
}

test "exchange core concurrent cold poisoned drains share terminal settlement" {
    const allocator = std.testing.allocator;
    const maker_uid: i64 = 99_131;
    const taker_uid: i64 = 99_132;
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .orders_processing = .{ .margin_trading_mode = .margin_trading_enabled },
    });
    defer core.deinit();
    const spec = try prepareRuntimePoisonScenario(
        &core,
        allocator,
        maker_uid,
        taker_uid,
        91_031,
    );

    var gate: FatalDrainCallbackGate = .{ .io = std.testing.io };
    defer gate.release();
    const callback: CommandResponseCallback = .{
        .context = &gate,
        .callback = FatalDrainCallbackGate.onResponse,
        .failure_callback = FatalDrainCallbackGate.onFailure,
    };
    try core.submitApiCommandWithCallback(.{
        .place_order = .{
            .order_id = 91_032,
            .uid = taker_uid,
            .symbol = spec.symbol_id,
            .price = 10_770,
            .reserve_bid_price = 10_770,
            .size = 1,
            .action = .bid,
            .order_type = .ioc,
            .user_cookie = 0,
        },
    }, callback);
    try core.submitApiCommandWithCallback(.{ .add_user = 99_133 }, callback);
    try core.submitApiCommandDetached(.{ .add_user = 99_134 });
    const skipped_poll = try core.nopAsync();
    const accepted_target = core.driver.accepted_total;

    var owner: AcceptedDrainRunner = .{
        .core = &core,
        .io = std.testing.io,
    };
    const owner_thread = try std.Thread.spawn(.{}, AcceptedDrainRunner.run, .{&owner});
    var owner_thread_live = true;
    defer {
        gate.release();
        if (owner_thread_live) owner_thread.join();
    }
    try gate.waitEntered();
    var waiter: AcceptedDrainRunner = .{
        .core = &core,
        .io = std.testing.io,
    };
    const waiter_thread = try std.Thread.spawn(.{}, AcceptedDrainRunner.run, .{&waiter});
    var waiter_thread_live = true;
    defer {
        gate.release();
        if (waiter_thread_live) waiter_thread.join();
    }
    try waiter.waitStarted();
    try waitForAcceptedDrainWaiters(&core, 1);

    try std.testing.expect(core.driver.manual_drain_active);
    try std.testing.expect(!owner.finished.isSet());
    try std.testing.expect(!waiter.finished.isSet());
    gate.release();

    try owner.waitFinished();
    try waiter.waitFinished();
    owner_thread.join();
    owner_thread_live = false;
    waiter_thread.join();
    waiter_thread_live = false;
    try std.testing.expectError(error.CorePoisoned, owner.result.?);
    try std.testing.expectError(error.CorePoisoned, waiter.result.?);
    try std.testing.expectEqual(@as(usize, 2), gate.failure_count);
    try std.testing.expectEqual(@as(usize, 0), core.pendingCallbackCount());
    try std.testing.expectEqual(@as(usize, 0), core.asyncPendingCount());
    try std.testing.expectEqual(@as(usize, 0), core.driver.completion_slots_used);
    try std.testing.expectEqual(@as(u64, 0), core.driver.async_drained_total);
    try std.testing.expectEqual(@as(usize, 0), core.driver.accepted_drain_waiters);
    try std.testing.expectEqual(accepted_target, core.driver.completed_total);
    try std.testing.expectEqual(accepted_target, core.driver.settled_prefix_total);
    try std.testing.expectError(error.CorePoisoned, core.takeAsyncResultChecked(skipped_poll));
}

test "exchange core concurrent shutdown closes publication wakes producer and joins once" {
    const allocator = std.testing.allocator;
    var gate: BackpressureGate = .{ .io = std.testing.io };
    var core = try ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .runtime = .{ .queue_capacity = 4, .max_batch_size = 1, .wait_strategy = .blocking },
    });
    defer core.deinit();
    core.setResultSink(.{ .context = &gate, .on_result = BackpressureGate.callback });
    _ = try core.addUserAsync(99_500);
    try core.startup();
    gate.waitEntered();
    for (0..4) |index| {
        try core.enqueueRuntime(makeAddUserCommand(99_510 + @as(i64, @intCast(index))));
    }

    var producer: BackpressureProducer = .{ .core = &core };
    const producer_thread = try std.Thread.spawn(.{}, BackpressureProducer.run, .{&producer});
    core.driver.mutex.lockUncancelable(core.driver.io);
    while (core.driver.blocked_producers == 0) {
        core.driver.completion_available.waitUncancelable(core.driver.io, &core.driver.mutex);
    }
    core.driver.mutex.unlock(core.driver.io);

    const first_shutdown = try std.Thread.spawn(.{}, ShutdownRunner.run, .{ShutdownRunner{ .core = &core }});
    const second_shutdown = try std.Thread.spawn(.{}, ShutdownRunner.run, .{ShutdownRunner{ .core = &core }});
    producer_thread.join();
    try std.testing.expectError(error.CoreStopped, producer.result.?);
    gate.release();
    first_shutdown.join();
    second_shutdown.join();
    try std.testing.expect(core.isStopped());
}

test "exchange core blocks command submission paths after shutdown" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    core.shutdown();

    var add_cmd = makeAddUserCommand(9951);
    add_cmd.timestamp = 1;
    try std.testing.expectError(error.CoreStopped, core.submitCommand(&add_cmd));
    try std.testing.expectError(error.CoreStopped, core.addUser(9951));

    var batch_cmd = makeAddUserCommand(9952);
    batch_cmd.timestamp = 2;
    var batch = [_]*cmd.OrderCommand{&batch_cmd};
    try std.testing.expectError(error.CoreStopped, core.submitCommandBatch(batch[0..]));

    try std.testing.expectError(error.CoreStopped, core.submitCommandAsync(&batch_cmd));
    try std.testing.expectError(error.CoreStopped, core.addUserAsync(9953));
    try std.testing.expectError(error.CoreStopped, core.persistStateAsync(9001, 10));
    try std.testing.expectError(
        error.CoreStopped,
        core.submitBinaryPayloadAsync(.binary_data_query, 704, 111, &.{0x7F}),
    );

    try std.testing.expectError(error.CoreStopped, core.drainAsyncPending());
    try std.testing.expectError(error.CoreStopped, core.enqueueRuntime(makeAddUserCommand(9954)));
    try std.testing.expectError(error.CoreStopped, core.drainRuntime());
}

test "exchange core startup-from-persistence composes lifecycle and bootstrap replay" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer source.deinit();

    var add_user = makeAddUserCommand(9901);
    add_user.timestamp = 10;
    var add_response = try source.submitCommand(&add_user);
    defer add_response.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try source.writeJournalToFile(tmp.dir, "startup.journal");

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXSTART",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = std.math.maxInt(u64),
            .throw_if_snapshot_not_found = false,
        },
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer target.deinit();

    const outcome = try target.startupFromPersistence(
        tmp.dir,
        null,
        "startup.journal",
    );
    try std.testing.expect(target.isStarted());
    try std.testing.expectEqual(@as(usize, 1), outcome.journal_replayed_commands);

    var user_report = try target.queryReport(.{ .single_user = 9901 });
    defer user_report.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        user_report.single_user.query_execution_status,
    );
}

test "exchange core startup-from-persistence reports missing explicit journal file" {
    const allocator = std.testing.allocator;

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXSTARTMISS",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = std.math.maxInt(u64),
            .throw_if_snapshot_not_found = false,
        },
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer target.deinit();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try std.testing.expectError(
        error.FileNotFound,
        target.startupFromPersistence(
            tmp.dir,
            null,
            "missing.journal",
        ),
    );
}

test "exchange core startupFromConfiguredPersistence derives default paths from config" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXSTARTCFG"),
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer source.deinit();

    var add_user = makeAddUserCommand(9902);
    add_user.timestamp = 10;
    var add_response = try source.submitCommand(&add_user);
    defer add_response.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try source.writeJournalToDefaultPath(tmp.dir, 0, 1);

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXSTARTCFG",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = std.math.maxInt(u64),
            .throw_if_snapshot_not_found = false,
        },
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer target.deinit();

    const outcome = try target.startupFromConfiguredPersistence(
        tmp.dir,
        .risk_engine,
        0,
        1,
    );
    try std.testing.expect(target.isStarted());
    try std.testing.expectEqual(@as(usize, 1), outcome.journal_replayed_commands);

    var user_report = try target.queryReport(.{ .single_user = 9902 });
    defer user_report.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        user_report.single_user.query_execution_status,
    );
}

test "exchange core startupFromConfiguredPersistence tolerates missing first journal partition" {
    const allocator = std.testing.allocator;

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXSTARTCFGMISS",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = std.math.maxInt(u64),
            .throw_if_snapshot_not_found = false,
        },
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer target.deinit();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const outcome = try target.startupFromConfiguredPersistence(
        tmp.dir,
        .risk_engine,
        0,
        1,
    );
    try std.testing.expect(target.isStarted());
    try std.testing.expectEqual(@as(usize, 0), outcome.journal_replayed_commands);

    var user_report = try target.queryReport(.{ .single_user = 9905 });
    defer user_report.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.user_not_found,
        user_report.single_user.query_execution_status,
    );
}

test "exchange core startupFromDiscoveredConfiguredPersistence picks latest snapshot id by module+instance" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXAUTOSTART"),
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer source.deinit();

    try std.testing.expect(try source.kernelRef().addSymbol(try makeExchangeSpecForSymbol(241)));

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try source.captureSnapshotToDefaultPath(tmp.dir, 51, .risk_engine, 0, &.{241}, 12);
    try source.captureSnapshotToDefaultPath(tmp.dir, 52, .risk_engine, 0, &.{241}, 12);
    try source.captureSnapshotToDefaultPath(tmp.dir, 99, .matching_engine_router, 1, &.{241}, 12);

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAUTOSTART",
            .snapshot_id = 1,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = true,
        },
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer target.deinit();

    const outcome = try target.startupFromDiscoveredConfiguredPersistence(
        tmp.dir,
        .risk_engine,
        0,
        1,
    );
    try std.testing.expect(target.isStarted());
    try std.testing.expect(outcome.snapshot_requested);
    try std.testing.expect(outcome.snapshot_found);
    try std.testing.expectEqual(@as(usize, 0), outcome.journal_replayed_commands);
    try std.testing.expectEqual(@as(u64, 52), (try target.persistenceCurrentSnapshot()).snapshot_id);
}

test "exchange core startupFromDiscoveredConfiguredPersistence derives base sequence from main log metadata" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXAUTOSEQ"),
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer source.deinit();

    var add_user = makeAddUserCommand(9910);
    add_user.timestamp = 10;
    var add_response = try source.submitCommand(&add_user);
    defer add_response.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 1), source.kernelRef().next_seq);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try source.captureSnapshotToDefaultPath(tmp.dir, 71, .risk_engine, 0, &.{}, 8);

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAUTOSEQ",
            .snapshot_id = 1,
            .snapshot_base_seq = 999,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = true,
        },
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer target.deinit();

    const outcome = try target.startupFromDiscoveredConfiguredPersistence(
        tmp.dir,
        .risk_engine,
        0,
        1,
    );
    try std.testing.expect(target.isStarted());
    try std.testing.expect(outcome.snapshot_requested);
    try std.testing.expect(outcome.snapshot_found);
    try std.testing.expectEqual(@as(u64, 1), target.kernelRef().next_seq);
    try std.testing.expectEqual(@as(u64, 1), (try target.persistenceCurrentSnapshot()).seq);
}

test "exchange core startupFromDiscoveredConfiguredPersistence falls back to configured base sequence without main log metadata" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXAUTOFALLBACK"),
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer source.deinit();

    var add_user = makeAddUserCommand(9912);
    add_user.timestamp = 10;
    var add_response = try source.submitCommand(&add_user);
    defer add_response.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try source.captureSnapshotToDefaultPath(tmp.dir, 81, .risk_engine, 0, &.{}, 8);
    const main_log_path = try source.defaultPersistenceMainLogPath(allocator);
    defer allocator.free(main_log_path);
    try tmp.dir.deleteFile(io_context.current(), main_log_path);

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAUTOFALLBACK",
            .snapshot_id = 1,
            .snapshot_base_seq = 777,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = true,
        },
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer target.deinit();

    const outcome = try target.startupFromDiscoveredConfiguredPersistence(
        tmp.dir,
        .risk_engine,
        0,
        1,
    );
    try std.testing.expect(target.isStarted());
    try std.testing.expect(outcome.snapshot_requested);
    try std.testing.expect(outcome.snapshot_found);
    try std.testing.expectEqual(@as(u64, 777), target.kernelRef().next_seq);
    try std.testing.expectEqual(@as(u64, 777), (try target.persistenceCurrentSnapshot()).seq);
}

test "exchange core strict discovered startup derives base sequence from valid main log metadata" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXAUTOSTRICT"),
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer source.deinit();

    var add_user = makeAddUserCommand(9913);
    add_user.timestamp = 10;
    var add_response = try source.submitCommand(&add_user);
    defer add_response.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 1), source.kernelRef().next_seq);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try source.captureSnapshotToDefaultPath(tmp.dir, 91, .risk_engine, 0, &.{}, 8);

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAUTOSTRICT",
            .snapshot_id = 1,
            .snapshot_base_seq = 555,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = true,
        },
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer target.deinit();

    const outcome = try target.startupFromDiscoveredConfiguredPersistenceStrictMainLog(
        tmp.dir,
        .risk_engine,
        0,
        1,
    );
    try std.testing.expect(target.isStarted());
    try std.testing.expect(outcome.snapshot_requested);
    try std.testing.expect(outcome.snapshot_found);
    try std.testing.expectEqual(@as(u64, 1), target.kernelRef().next_seq);
    try std.testing.expectEqual(@as(u64, 1), (try target.persistenceCurrentSnapshot()).seq);
}

test "exchange core strict discovered startup reports malformed main log entry" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXAUTOBADLOG"),
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer source.deinit();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try source.captureSnapshotToDefaultPath(tmp.dir, 101, .risk_engine, 0, &.{}, 8);

    const main_log_path = try source.defaultPersistenceMainLogPath(allocator);
    defer allocator.free(main_log_path);
    var main_log_file = try tmp.dir.createFile(io_context.current(), main_log_path, .{ .truncate = true });
    defer main_log_file.close(io_context.current());
    try main_log_file.writeStreamingAll(io_context.current(), "this is malformed\n");

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAUTOBADLOG",
            .snapshot_id = 1,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = 0,
            .throw_if_snapshot_not_found = true,
        },
        .serialization = config_mod.SerializationConfig.diskSnapshotOnly(),
    });
    defer target.deinit();

    try std.testing.expectError(
        error.MalformedMainLogEntry,
        target.startupFromDiscoveredConfiguredPersistenceStrictMainLog(
            tmp.dir,
            .risk_engine,
            0,
            1,
        ),
    );
}

test "exchange core startupFromDiscoveredConfiguredPersistence clean-starts when snapshot missing" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var source = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXAUTONONE"),
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer source.deinit();

    var add_user = makeAddUserCommand(9911);
    add_user.timestamp = 10;
    var add_response = try source.submitCommand(&add_user);
    defer add_response.deinit(allocator);
    try source.writeJournalToDefaultPath(tmp.dir, 0, 1);

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAUTONONE",
            .snapshot_id = 10,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = std.math.maxInt(u64),
            .throw_if_snapshot_not_found = false,
        },
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer target.deinit();

    const outcome = try target.startupFromDiscoveredConfiguredPersistence(
        tmp.dir,
        .risk_engine,
        0,
        1,
    );
    try std.testing.expect(target.isStarted());
    try std.testing.expect(outcome.snapshot_requested);
    try std.testing.expect(!outcome.snapshot_found);
    try std.testing.expectEqual(@as(usize, 0), outcome.journal_replayed_commands);
    try std.testing.expectEqual(@as(usize, 0), try target.journalLen());

    var user_report = try target.queryReport(.{ .single_user = 9911 });
    defer user_report.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.user_not_found,
        user_report.single_user.query_execution_status,
    );
}

test "exchange core startupFromDiscoveredConfiguredPersistence errors when snapshot required but missing" {
    const allocator = std.testing.allocator;

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXAUTOREQ",
            .snapshot_id = 77,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = std.math.maxInt(u64),
            .throw_if_snapshot_not_found = true,
        },
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer target.deinit();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try std.testing.expectError(
        error.SnapshotFileNotFound,
        target.startupFromDiscoveredConfiguredPersistence(
            tmp.dir,
            .risk_engine,
            0,
            1,
        ),
    );
}

test "exchange core startupFromConfiguredPersistence replays sequential journal partitions" {
    const allocator = std.testing.allocator;

    var source = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXSTARTCFG2"),
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer source.deinit();

    var add_user_a = makeAddUserCommand(9903);
    add_user_a.timestamp = 10;
    var add_response_a = try source.submitCommand(&add_user_a);
    defer add_response_a.deinit(allocator);

    var add_user_b = makeAddUserCommand(9904);
    add_user_b.timestamp = 20;
    var add_response_b = try source.submitCommand(&add_user_b);
    defer add_response_b.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try source.writeJournalToDefaultPath(tmp.dir, 0, 1);

    const partition_1_path = try source.defaultPersistenceJournalPath(allocator, 0, 1);
    defer allocator.free(partition_1_path);
    const partition_2_path = try source.defaultPersistenceJournalPath(allocator, 0, 2);
    defer allocator.free(partition_2_path);

    var partition_1_journal = try journal_mod.CommandJournal.readFromFile(
        allocator,
        tmp.dir,
        partition_1_path,
    );
    defer partition_1_journal.deinit();
    try std.testing.expectEqual(@as(usize, 2), partition_1_journal.entries.items.len);
    partition_1_journal.entries.items.len = 1;
    try partition_1_journal.writeToFile(tmp.dir, partition_1_path);

    var partition_2_journal = try journal_mod.CommandJournal.readFromFile(
        allocator,
        tmp.dir,
        partition_1_path,
    );
    defer partition_2_journal.deinit();
    try std.testing.expectEqual(@as(usize, 1), partition_2_journal.entries.items.len);
    try source.writeJournalToDefaultPath(tmp.dir, 0, 1);
    var full_journal = try journal_mod.CommandJournal.readFromFile(
        allocator,
        tmp.dir,
        partition_1_path,
    );
    defer full_journal.deinit();
    try std.testing.expectEqual(@as(usize, 2), full_journal.entries.items.len);
    partition_2_journal.entries.items[0] = full_journal.entries.items[1];
    try partition_2_journal.writeToFile(tmp.dir, partition_2_path);

    try partition_1_journal.writeToFile(tmp.dir, partition_1_path);

    var target = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXSTARTCFG2",
            .snapshot_id = 0,
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = std.math.maxInt(u64),
            .throw_if_snapshot_not_found = false,
        },
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer target.deinit();

    const outcome = try target.startupFromConfiguredPersistence(
        tmp.dir,
        .risk_engine,
        0,
        1,
    );
    try std.testing.expect(target.isStarted());
    try std.testing.expectEqual(@as(usize, 2), outcome.journal_replayed_commands);

    var user_report_a = try target.queryReport(.{ .single_user = 9903 });
    defer user_report_a.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        user_report_a.single_user.query_execution_status,
    );

    var user_report_b = try target.queryReport(.{ .single_user = 9904 });
    defer user_report_b.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        user_report_b.single_user.query_execution_status,
    );
}

test "exchange core seeded configured startup detects partition corruption deterministically" {
    const allocator = std.testing.allocator;

    const Perturbation = enum {
        baseline,
        sequence_mismatch,
        result_mismatch,
    };

    const Scenario = struct {
        name: []const u8,
        seed: u64,
        perturbation: Perturbation,
    };

    const OutcomeKind = enum {
        success,
        sequence_mismatch,
        result_mismatch,
    };

    const Outcome = struct {
        kind: OutcomeKind = .success,
        replayed_commands: usize = 0,
        user_a_status: kernel_mod.SingleUserQueryExecutionStatus = .user_not_found,
        user_b_status: kernel_mod.SingleUserQueryExecutionStatus = .user_not_found,
    };

    const Harness = struct {
        fn runScenario(scenario: Scenario) !Outcome {
            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();

            var exchange_id_buf: [64]u8 = undefined;
            const exchange_id = try std.fmt.bufPrint(&exchange_id_buf, "EXCFGCORR{x}", .{scenario.seed});

            var source = try ExchangeCore.init(allocator, .{
                .initial_state = config_mod.InitialStateConfig.cleanStart(exchange_id),
                .serialization = config_mod.SerializationConfig.diskJournaling(),
            });
            defer source.deinit();

            const uid_a: i64 = 992_000 + @as(i64, @intCast(scenario.seed % 4000)) * 2;
            const uid_b: i64 = uid_a + 1;

            var add_a = makeAddUserCommand(uid_a);
            add_a.timestamp = 10;
            var add_a_response = try source.submitCommand(&add_a);
            defer add_a_response.deinit(allocator);

            var add_b = makeAddUserCommand(uid_b);
            add_b.timestamp = 20;
            var add_b_response = try source.submitCommand(&add_b);
            defer add_b_response.deinit(allocator);

            try source.writeJournalToDefaultPath(tmp.dir, 0, 1);

            const partition_1_path = try source.defaultPersistenceJournalPath(allocator, 0, 1);
            defer allocator.free(partition_1_path);
            const partition_2_path = try source.defaultPersistenceJournalPath(allocator, 0, 2);
            defer allocator.free(partition_2_path);

            var full_journal = try journal_mod.CommandJournal.readFromFile(
                allocator,
                tmp.dir,
                partition_1_path,
            );
            defer full_journal.deinit();
            try std.testing.expectEqual(@as(usize, 2), full_journal.entries.items.len);

            var partition_1_journal = try journal_mod.CommandJournal.readFromFile(
                allocator,
                tmp.dir,
                partition_1_path,
            );
            defer partition_1_journal.deinit();
            partition_1_journal.entries.items.len = 1;
            try partition_1_journal.writeToFile(tmp.dir, partition_1_path);

            var partition_2_journal = try journal_mod.CommandJournal.readFromFile(
                allocator,
                tmp.dir,
                partition_1_path,
            );
            defer partition_2_journal.deinit();
            partition_2_journal.entries.items[0] = full_journal.entries.items[1];

            switch (scenario.perturbation) {
                .baseline => {},
                .sequence_mismatch => {
                    // Publication gaps are valid because some commands may be
                    // intentionally absent from a journal. A duplicate or
                    // regressing identity is the actual corruption case.
                    try partition_2_journal.entries.append(
                        allocator,
                        partition_2_journal.entries.items[0],
                    );
                },
                .result_mismatch => {
                    partition_2_journal.entries.items[0].result_code = .drop;
                },
            }
            try partition_2_journal.writeToFile(tmp.dir, partition_2_path);

            var target = try ExchangeCore.init(allocator, .{
                .initial_state = .{
                    .exchange_id = exchange_id,
                    .snapshot_id = 0,
                    .snapshot_base_seq = 0,
                    .journal_timestamp_ns = std.math.maxInt(u64),
                    .throw_if_snapshot_not_found = false,
                },
                .serialization = config_mod.SerializationConfig.diskJournaling(),
            });
            defer target.deinit();

            const startup = target.startupFromConfiguredPersistence(
                tmp.dir,
                .risk_engine,
                0,
                1,
            ) catch |err| switch (err) {
                error.SequenceMismatch => return .{
                    .kind = .sequence_mismatch,
                },
                error.ResultMismatch => return .{
                    .kind = .result_mismatch,
                },
                else => std.debug.panic(
                    "seeded configured startup scenario='{s}' seed=0x{x} perturbation={s} failed with unexpected error {s}; replay with: zig build test --test-filter \"exchange core seeded configured startup detects partition corruption deterministically\"",
                    .{ scenario.name, scenario.seed, @tagName(scenario.perturbation), @errorName(err) },
                ),
            };

            var user_a_report = try target.queryReport(.{ .single_user = uid_a });
            defer user_a_report.deinit(allocator);
            var user_b_report = try target.queryReport(.{ .single_user = uid_b });
            defer user_b_report.deinit(allocator);

            return .{
                .kind = .success,
                .replayed_commands = startup.journal_replayed_commands,
                .user_a_status = user_a_report.single_user.query_execution_status,
                .user_b_status = user_b_report.single_user.query_execution_status,
            };
        }
    };

    const scenarios = [_]Scenario{
        .{
            .name = "baseline",
            .seed = 0xACE9201,
            .perturbation = .baseline,
        },
        .{
            .name = "sequence-mismatch",
            .seed = 0xACE9202,
            .perturbation = .sequence_mismatch,
        },
        .{
            .name = "result-mismatch",
            .seed = 0xACE9203,
            .perturbation = .result_mismatch,
        },
    };

    for (scenarios) |scenario| {
        const first = try Harness.runScenario(scenario);
        const second = try Harness.runScenario(scenario);
        try std.testing.expectEqualDeep(first, second);

        switch (scenario.perturbation) {
            .baseline => {
                try std.testing.expectEqual(OutcomeKind.success, first.kind);
                try std.testing.expectEqual(@as(usize, 2), first.replayed_commands);
                try std.testing.expectEqual(kernel_mod.SingleUserQueryExecutionStatus.ok, first.user_a_status);
                try std.testing.expectEqual(kernel_mod.SingleUserQueryExecutionStatus.ok, first.user_b_status);
            },
            .sequence_mismatch => {
                try std.testing.expectEqual(OutcomeKind.sequence_mismatch, first.kind);
            },
            .result_mismatch => {
                try std.testing.expectEqual(OutcomeKind.result_mismatch, first.kind);
            },
        }
    }
}

test "exchange core seeded persistence checkpoints keep snapshot-journal bootstrap deterministic" {
    const allocator = std.testing.allocator;

    const symbol: i32 = 241;
    const user_count: usize = 24;
    const rounds: usize = 600;
    const checkpoint_interval: usize = 120;
    const initial_balance: i64 = 1_000_000_000;

    var user_ids: [user_count]i64 = undefined;
    for (&user_ids, 0..) |*slot, idx| {
        slot.* = 20_000 + @as(i64, @intCast(idx));
    }

    var source = try ExchangeCore.init(allocator, .{
        .matching = .{
            .orderbook_kind = .direct,
        },
        .serialization = config_mod.SerializationConfig.diskJournaling(),
    });
    defer source.deinit();
    try std.testing.expect(try source.kernelRef().addSymbol(try makeExchangeSpecForSymbol(symbol)));

    var timestamp: i64 = 1;
    var transaction_id: i64 = 1;
    for (user_ids) |uid| {
        var add_user = makeAddUserCommand(uid);
        add_user.timestamp = timestamp;
        timestamp += 1;
        var add_response = try source.submitCommand(&add_user);
        defer add_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, add_response.result_code);

        var deposit_base = makeBalanceAdjustmentCommand(uid, 11, initial_balance, transaction_id, .adjustment);
        transaction_id += 1;
        deposit_base.timestamp = timestamp;
        timestamp += 1;
        var base_response = try source.submitCommand(&deposit_base);
        defer base_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, base_response.result_code);

        var deposit_quote = makeBalanceAdjustmentCommand(uid, 15, initial_balance, transaction_id, .adjustment);
        transaction_id += 1;
        deposit_quote.timestamp = timestamp;
        timestamp += 1;
        var quote_response = try source.submitCommand(&deposit_quote);
        defer quote_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, quote_response.result_code);
    }

    var prng = std.Random.DefaultPrng.init(0xC0DEC0DEC0FFEE42);
    const random = prng.random();
    var next_order_id: i64 = 70_000;
    var previous_checkpoint_seq: ?u64 = null;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    for (0..rounds) |idx| {
        const roll = random.uintLessThan(u32, 100);
        var command: cmd.OrderCommand = undefined;

        if (roll < 76) {
            const uid = user_ids[random.uintLessThan(usize, user_ids.len)];
            const action: common.OrderAction = if (random.boolean()) .ask else .bid;
            const order_type: common.OrderType = if (random.uintLessThan(u32, 100) < 82) .gtc else .ioc;
            const price = 980 + @as(i64, @intCast(random.uintLessThan(u32, 120)));
            const size = @as(i64, @intCast(random.uintLessThan(u32, 12))) + 1;
            const reserve = if (action == .bid)
                price + @as(i64, @intCast(random.uintLessThan(u32, 24)))
            else
                0;

            command = cmd.OrderCommand.newOrder(
                order_type,
                next_order_id,
                uid,
                price,
                reserve,
                size,
                action,
            );
            command.symbol = symbol;
            next_order_id += 1;
        } else if (roll < 92) {
            const uid = user_ids[random.uintLessThan(usize, user_ids.len)];
            const currency: i32 = if (random.boolean()) 11 else 15;
            const amount_abs = @as(i64, @intCast(random.uintLessThan(u32, 3_000))) + 1;
            const amount = if (random.uintLessThan(u32, 100) < 16) -amount_abs else amount_abs;
            command = makeBalanceAdjustmentCommand(uid, currency, amount, transaction_id, .adjustment);
            transaction_id += 1;
        } else {
            command = makeRequestOrderBookCommandI64(
                symbol,
                @as(i64, @intCast(random.uintLessThan(usize, 48) + 1)),
            );
        }

        command.timestamp = timestamp;
        timestamp += @as(i64, @intCast(random.uintLessThan(u32, 3))) + 1;

        var response = try source.submitCommand(&command);
        defer response.deinit(allocator);

        if ((idx + 1) % checkpoint_interval != 0) continue;

        const checkpoint_seq = source.kernelRef().next_seq;
        var snapshot_name_buf: [64]u8 = undefined;
        const snapshot_name = try std.fmt.bufPrint(
            &snapshot_name_buf,
            "core-sim-seq-{d}.snap",
            .{checkpoint_seq},
        );
        var journal_name_buf: [64]u8 = undefined;
        const journal_name = try std.fmt.bufPrint(
            &journal_name_buf,
            "core-sim-seq-{d}.journal",
            .{checkpoint_seq},
        );

        try source.captureSnapshotToFile(tmp.dir, snapshot_name, &.{symbol}, 64);
        try source.writeJournalToFile(tmp.dir, journal_name);

        var restored_from_current = try ExchangeCore.init(allocator, .{
            .initial_state = .{
                .exchange_id = "EXSIMCUR",
                .snapshot_id = 1,
                .snapshot_base_seq = checkpoint_seq,
                .journal_timestamp_ns = std.math.maxInt(u64),
                .throw_if_snapshot_not_found = true,
            },
            .matching = .{
                .orderbook_kind = .direct,
            },
            .serialization = config_mod.SerializationConfig.diskJournaling(),
        });
        defer restored_from_current.deinit();

        const from_current = try restored_from_current.startupFromPersistence(
            tmp.dir,
            snapshot_name,
            journal_name,
        );
        try std.testing.expect(from_current.snapshot_requested);
        try std.testing.expect(from_current.snapshot_found);
        try std.testing.expectEqual(@as(usize, 0), from_current.journal_replayed_commands);
        try std.testing.expectEqual(checkpoint_seq, restored_from_current.kernelRef().next_seq);
        try std.testing.expectEqual(@as(usize, 0), try restored_from_current.journalLen());
        try expectExchangeCoreStateParity(
            allocator,
            &source,
            &restored_from_current,
            user_ids[0..],
            symbol,
        );

        if (previous_checkpoint_seq) |previous_seq| {
            var previous_snapshot_name_buf: [64]u8 = undefined;
            const previous_snapshot_name = try std.fmt.bufPrint(
                &previous_snapshot_name_buf,
                "core-sim-seq-{d}.snap",
                .{previous_seq},
            );

            var restored_from_previous = try ExchangeCore.init(allocator, .{
                .initial_state = .{
                    .exchange_id = "EXSIMPRV",
                    .snapshot_id = 1,
                    .snapshot_base_seq = previous_seq,
                    .journal_timestamp_ns = std.math.maxInt(u64),
                    .throw_if_snapshot_not_found = true,
                },
                .matching = .{
                    .orderbook_kind = .direct,
                },
                .serialization = config_mod.SerializationConfig.diskJournaling(),
            });
            defer restored_from_previous.deinit();

            const from_previous = try restored_from_previous.startupFromPersistence(
                tmp.dir,
                previous_snapshot_name,
                journal_name,
            );
            const expected_replayed_u64 = checkpoint_seq - previous_seq;
            const expected_replayed = std.math.cast(usize, expected_replayed_u64) orelse unreachable;
            try std.testing.expect(from_previous.snapshot_requested);
            try std.testing.expect(from_previous.snapshot_found);
            try std.testing.expectEqual(expected_replayed, from_previous.journal_replayed_commands);
            try std.testing.expectEqual(expected_replayed, try restored_from_previous.journalLen());
            try std.testing.expectEqual(checkpoint_seq, restored_from_previous.kernelRef().next_seq);
            try expectExchangeCoreStateParity(
                allocator,
                &source,
                &restored_from_previous,
                user_ids[0..],
                symbol,
            );
        }

        previous_checkpoint_seq = checkpoint_seq;
    }

    try std.testing.expect(previous_checkpoint_seq != null);
}

test "exchange core callback receives completed command metadata with final result" {
    const allocator = std.testing.allocator;

    const Collector = struct {
        seen: bool = false,
        sequence: u64 = 0,
        result_code: cmd.CommandResultCode = .new,
        command: cmd.OrderCommand = .{ .command = .nop },

        fn onResponse(context: ?*anyopaque, response: *const kernel_mod.CommandResponse) void {
            const self: *@This() = @ptrCast(@alignCast(context.?));
            self.seen = true;
            self.sequence = response.sequence;
            self.result_code = response.result_code;
            self.command = response.command;
        }
    };

    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();
    try core.startup();

    var collector: Collector = .{};
    var source: cmd.OrderCommand = .{
        .command = .nop,
        .order_id = 91_001,
        .symbol = 7_701,
        .price = 12_345,
        .size = 17,
        .reserve_bid_price = 13_000,
        .action = .bid,
        .order_type = .gtc,
        .uid = 81_001,
        .timestamp = 7_654_321,
        .user_cookie = 41,
        .result_code = .new,
    };
    try core.submitCommandAsyncWithCallback(&source, .{
        .context = &collector,
        .callback = Collector.onResponse,
    });

    // The synchronous command is a FIFO barrier for callback execution.
    var barrier = makeNopCommand();
    var barrier_response = try core.submitCommand(&barrier);
    defer barrier_response.deinit(allocator);

    try std.testing.expect(collector.seen);
    try std.testing.expect(collector.sequence > 0);
    try std.testing.expectEqual(cmd.CommandResultCode.success, collector.result_code);
    try std.testing.expectEqual(collector.result_code, collector.command.result_code);
    try std.testing.expectEqual(source.command, collector.command.command);
    try std.testing.expectEqual(source.order_id, collector.command.order_id);
    try std.testing.expectEqual(source.symbol, collector.command.symbol);
    try std.testing.expectEqual(source.price, collector.command.price);
    try std.testing.expectEqual(source.size, collector.command.size);
    try std.testing.expectEqual(source.reserve_bid_price, collector.command.reserve_bid_price);
    try std.testing.expectEqual(source.action, collector.command.action);
    try std.testing.expectEqual(source.order_type, collector.command.order_type);
    try std.testing.expectEqual(source.uid, collector.command.uid);
    try std.testing.expectEqual(source.timestamp, collector.command.timestamp);
    try std.testing.expectEqual(source.user_cookie, collector.command.user_cookie);
}

test "exchange core compound publications reject without consuming global sequence" {
    const allocator = std.testing.allocator;
    var java_config = config_mod.JavaExchangeConfig.javaDefault();
    java_config.performance_cfg.ring_buffer_size = 4;
    java_config.performance_cfg.msgs_in_group_limit = 1;
    var core = try ExchangeCore.initFromJavaConfig(allocator, java_config);
    defer core.deinit();

    var accepted_ids: [3]u64 = undefined;
    for (&accepted_ids, 0..) |*request_id, index| {
        request_id.* = try core.addUserAsync(320_000 + @as(i64, @intCast(index)));
    }

    const request_before = core.api.next_request_id;
    const sequence_before = core.driver.publication_sequencer.highWater();
    try std.testing.expectError(
        error.CoreQueueFull,
        core.persistStateAsync(320_100, 9_999_100_001),
    );
    const two_frame_payload = [_]u8{0} ** 41;
    try std.testing.expectError(
        error.CoreQueueFull,
        core.submitBinaryPayloadAsync(
            .binary_data_query,
            320_101,
            9_999_100_002,
            &two_frame_payload,
        ),
    );

    try std.testing.expectEqual(@as(usize, 3), core.driver.queue.count());
    try std.testing.expectEqual(request_before, core.api.next_request_id);
    try std.testing.expectEqual(
        sequence_before,
        core.driver.publication_sequencer.highWater(),
    );
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try std.testing.expectError(
        error.CoreNotStarted,
        core.bootstrapFromPersistence(tmp.dir, null, null),
    );

    try core.startup();
    try std.testing.expectEqual(@as(usize, 3), try core.drainAsyncPending());
    for (accepted_ids) |request_id| {
        var response = core.takeAsyncResult(request_id) orelse
            return error.MissingAsyncResult;
        response.deinit(allocator);
    }
}

test "exchange core mixed runtime binary and async work shares one fifo sequence" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();

    try core.enqueueRuntime(makeNopCommand());
    const two_frame_payload = [_]u8{0} ** 41;
    const binary_request = try core.submitBinaryPayloadAsync(
        .binary_data_query,
        320_200,
        9_999_200_001,
        &two_frame_payload,
    );
    const add_request = try core.addUserAsync(320_201);
    try std.testing.expectEqual(@as(u64, 4), core.driver.publication_sequencer.highWater());

    try core.startup();
    try std.testing.expectEqual(@as(usize, 3), try core.drainAsyncPending());
    var binary_response = core.takeAsyncResult(binary_request) orelse
        return error.MissingAsyncResult;
    defer binary_response.deinit(allocator);
    var add_response = core.takeAsyncResult(add_request) orelse
        return error.MissingAsyncResult;
    defer add_response.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 3), binary_response.sequence);
    try std.testing.expectEqual(@as(u64, 4), add_response.sequence);
    try std.testing.expectEqual(@as(u64, 4), core.kernelRef().next_seq);
}

test "exchange core persist pair stays adjacent under concurrent publication" {
    const allocator = std.testing.allocator;
    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();
    try core.startup();

    const PersistPublisher = struct {
        core: *ExchangeCore,
        pair: ?PersistStateRequestPair = null,
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            self.pair = self.core.persistStateAsync(320_300, 9_999_300_001) catch |err| {
                self.failure = err;
                return;
            };
        }
    };
    const OrdinaryPublisher = struct {
        core: *ExchangeCore,
        request_id: ?u64 = null,
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            self.request_id = self.core.addUserAsync(320_301) catch |err| {
                self.failure = err;
                return;
            };
        }
    };

    var persist: PersistPublisher = .{ .core = &core };
    var ordinary: OrdinaryPublisher = .{ .core = &core };
    const persist_thread = try std.Thread.spawn(.{}, PersistPublisher.run, .{&persist});
    const ordinary_thread = try std.Thread.spawn(.{}, OrdinaryPublisher.run, .{&ordinary});
    persist_thread.join();
    ordinary_thread.join();
    try std.testing.expectEqual(@as(?anyerror, null), persist.failure);
    try std.testing.expectEqual(@as(?anyerror, null), ordinary.failure);

    _ = try core.drainAsyncPending();
    const pair = persist.pair orelse return error.MissingAsyncResult;
    var matching = core.takeAsyncResult(pair.matching_request_id) orelse
        return error.MissingAsyncResult;
    defer matching.deinit(allocator);
    var risk_response = core.takeAsyncResult(pair.risk_request_id) orelse
        return error.MissingAsyncResult;
    defer risk_response.deinit(allocator);
    try std.testing.expectEqual(matching.sequence + 1, risk_response.sequence);

    var ordinary_response = core.takeAsyncResult(
        ordinary.request_id orelse return error.MissingAsyncResult,
    ) orelse return error.MissingAsyncResult;
    defer ordinary_response.deinit(allocator);
    try std.testing.expect(ordinary_response.sequence < matching.sequence or
        ordinary_response.sequence > risk_response.sequence);
}

test "exchange core generated order callback uses publication sequence as order id" {
    const allocator = std.testing.allocator;
    const Collector = struct {
        count: usize = 0,
        sequence: u64 = 0,
        order_id: i64 = 0,

        fn onResponse(context: ?*anyopaque, response: *const kernel_mod.CommandResponse) void {
            const self: *@This() = @ptrCast(@alignCast(context.?));
            self.count += 1;
            self.sequence = response.sequence;
            self.order_id = response.command.order_id;
        }
    };

    var core = try ExchangeCore.init(allocator, .{});
    defer core.deinit();
    try core.enqueueRuntime(makeNopCommand());
    var collector: Collector = .{};
    const sequence = try core.placeNewOrderGeneratedIdWithCallback(
        7,
        100,
        100,
        1,
        .bid,
        .gtc,
        9_999,
        320_400,
        .{
            .context = &collector,
            .callback = Collector.onResponse,
        },
    );
    try std.testing.expectEqual(@as(u64, 2), sequence);
    try std.testing.expectEqual(@as(u64, 1), core.api.next_request_id);
    try std.testing.expectEqual(@as(usize, 1), try core.drainAsyncPending());
    try std.testing.expectEqual(@as(usize, 1), collector.count);
    try std.testing.expectEqual(sequence, collector.sequence);
    try std.testing.expectEqual(@as(i64, @intCast(sequence)), collector.order_id);
}

test "exchange core durable restore replays journal tail from manifest frontier" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var storage_folder_buf: [256]u8 = undefined;
    const storage_folder = try std.fmt.bufPrint(
        &storage_folder_buf,
        ".zig-cache/tmp/{s}/durable-journal-frontier",
        .{tmp.sub_path},
    );
    const serialization: config_mod.SerializationConfig = .{
        .enable_journaling = true,
        .backend = .disk,
        .storage_folder = storage_folder,
    };

    var source = try ExchangeCore.init(allocator, .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXFRONTIER"),
        .serialization = serialization,
    });
    defer source.deinit();
    var first = try source.addUser(320_500);
    defer first.deinit(allocator);
    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try source.persistState(320_501, 9_999_500_001),
    );
    var tail = try source.addUser(320_502);
    defer tail.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 4), tail.sequence);

    const matching_path = try source.defaultPersistenceSnapshotPath(
        allocator,
        320_501,
        .matching_engine_router,
        0,
    );
    defer allocator.free(matching_path);
    const journal_path = try source.defaultPersistenceJournalPath(allocator, 320_501, 1);
    defer allocator.free(journal_path);
    try source.writeJournalToFile(std.Io.Dir.cwd(), journal_path);

    var restored = try ExchangeCore.init(allocator, .{
        .initial_state = .{
            .exchange_id = "EXFRONTIER",
            .snapshot_id = 320_501,
            // Deliberately stale: the durable manifest records sequence 3.
            .snapshot_base_seq = 0,
            .journal_timestamp_ns = std.math.maxInt(u64),
            .throw_if_snapshot_not_found = true,
        },
        .serialization = serialization,
    });
    defer restored.deinit();
    const outcome = try restored.bootstrapFromPersistence(
        std.Io.Dir.cwd(),
        matching_path,
        journal_path,
    );
    try std.testing.expect(outcome.snapshot_found);
    try std.testing.expectEqual(@as(usize, 1), outcome.journal_replayed_commands);
    try std.testing.expectEqual(@as(u64, 4), restored.kernelRef().next_seq);
    try std.testing.expectEqual(
        @as(u64, 4),
        restored.driver.publication_sequencer.highWater(),
    );

    var next = try restored.addUser(320_503);
    defer next.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 5), next.sequence);
}

test "exchange core durable XCJ3 records finalized persistence results" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const success_config: Config = .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXDURSUCCESS"),
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = "success-store",
        },
    };
    const success_path = try nativeJournalPartitionPath(allocator, &success_config);
    defer allocator.free(success_path);
    {
        var core = try ExchangeCore.initWithPersistenceRoot(allocator, tmp.dir, success_config);
        defer core.deinit();
        try std.testing.expectEqual(
            cmd.CommandResultCode.success,
            try core.persistState(410_001, 10_000_000_001),
        );
        const entries = core.journal.?.entries.items;
        try std.testing.expectEqual(@as(usize, 2), entries.len);
        try std.testing.expectEqual(cmd.CommandResultCode.accepted, entries[0].result_code);
        try std.testing.expectEqual(cmd.CommandResultCode.success, entries[1].result_code);
    }
    var success_durable = try journal_mod.CommandJournal.readFromFile(
        allocator,
        tmp.dir,
        success_path,
    );
    defer success_durable.deinit();
    try std.testing.expectEqual(cmd.CommandResultCode.accepted, success_durable.entries.items[0].result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, success_durable.entries.items[1].result_code);

    const failure_config: Config = .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXDURFAIL"),
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = "failure-store",
        },
    };
    const failure_path = try nativeJournalPartitionPath(allocator, &failure_config);
    defer allocator.free(failure_path);
    var expected_codes: [2]cmd.CommandResultCode = undefined;
    {
        var core = try ExchangeCore.initWithPersistenceRoot(allocator, tmp.dir, failure_config);
        defer core.deinit();
        core.setPersistenceFaultForTesting(.matching_before_write);
        try std.testing.expectEqual(
            cmd.CommandResultCode.state_persist_matching_engine_failed,
            try core.persistState(410_002, 10_000_000_002),
        );
        const entries = core.journal.?.entries.items;
        try std.testing.expectEqual(@as(usize, 2), entries.len);
        for (entries, 0..) |entry, index| expected_codes[index] = entry.result_code;
        try std.testing.expectEqual(
            cmd.CommandResultCode.state_persist_matching_engine_failed,
            expected_codes[0],
        );
        try std.testing.expectEqual(
            cmd.CommandResultCode.state_persist_risk_engine_failed,
            expected_codes[1],
        );
    }
    var failure_durable = try journal_mod.CommandJournal.readFromFile(
        allocator,
        tmp.dir,
        failure_path,
    );
    defer failure_durable.deinit();
    try std.testing.expectEqual(@as(usize, expected_codes.len), failure_durable.entries.items.len);
    for (failure_durable.entries.items, 0..) |entry, index| {
        try std.testing.expectEqual(expected_codes[index], entry.result_code);
    }
}

test "exchange core Java durable journal skips queries without changing native observation" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const config: Config = .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXJAVASKIP"),
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = "store",
        },
    };
    const native_path = try nativeJournalPartitionPath(allocator, &config);
    defer allocator.free(native_path);

    const symbol: i32 = 411_001;
    const first_uid: i64 = 411_101;
    const final_uid: i64 = 411_102;
    const binary_command_frames: usize = 3;
    var binary_query_first_sequence: u64 = 0;
    var binary_query_last_sequence: u64 = 0;
    var binary_command_first_sequence: u64 = 0;
    var binary_command_last_sequence: u64 = 0;
    var final_sequence: u64 = 0;

    {
        var core = try ExchangeCore.initWithIoAndPersistenceRoot(
            allocator,
            std.testing.io,
            tmp.dir,
            config,
        );
        defer core.deinit();
        try std.testing.expect(try core.kernelRef().addSymbol(
            try makeExchangeSpecForSymbol(symbol),
        ));

        var first = try core.addUser(first_uid);
        defer first.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, first.result_code);
        try std.testing.expectEqual(@as(u64, 1), first.sequence);

        var order_book = try core.requestOrderBook(symbol, 4);
        defer order_book.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, order_book.result_code);
        try std.testing.expectEqual(@as(u64, 2), order_book.sequence);

        // The live durable frontier intentionally trails this cold query. A
        // complete non-mutating suffix must not prevent worker startup.
        try core.startup();

        var callback_probe: BinaryStreamCallbackProbe = .{};
        try core.submitApiCommandWithCallback(.{
            .request_order_book = .{ .symbol = symbol, .depth = 4 },
        }, .{
            .context = &callback_probe,
            .callback = BinaryStreamCallbackProbe.onResponse,
            .failure_callback = BinaryStreamCallbackProbe.onFailure,
        });
        try std.testing.expectEqual(@as(usize, 1), try core.drainAsyncPending());
        try std.testing.expectEqual(@as(usize, 1), callback_probe.response_count);
        try std.testing.expectEqual(@as(usize, 0), callback_probe.failure_count);
        try std.testing.expectEqual(@as(u64, 3), callback_probe.terminal_sequence);
        try std.testing.expectEqual(cmd.CommandResultCode.success, callback_probe.terminal_result);

        binary_query_first_sequence = core.driver.publication_sequencer.highWater() + 1;
        var binary_query = try core.submitBinaryReportQuery(
            411_201,
            11_000_000_001,
            .{ .single_user = first_uid },
        );
        defer binary_query.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, binary_query.result_code);
        binary_query_last_sequence = binary_query.sequence;
        try std.testing.expect(binary_query_last_sequence >= binary_query_first_sequence);

        const command_payload = try encodeAddAccountsPayloadForFrameCount(
            allocator,
            binary_command_frames,
            411_300,
        );
        defer allocator.free(command_payload);
        binary_command_first_sequence = binary_query_last_sequence + 1;
        var binary_command = try core.submitBinaryPayload(
            .binary_data_command,
            411_202,
            11_000_000_002,
            command_payload,
        );
        defer binary_command.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, binary_command.result_code);
        binary_command_last_sequence = binary_command.sequence;
        try std.testing.expectEqual(
            binary_command_first_sequence + binary_command_frames - 1,
            binary_command_last_sequence,
        );

        var final = try core.addUser(final_uid);
        defer final.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, final.result_code);
        final_sequence = final.sequence;
        try std.testing.expectEqual(binary_command_last_sequence + 1, final_sequence);

        core.shutdown();
        const observed = core.journal.?.entries.items;
        try std.testing.expectEqual(@as(usize, @intCast(final_sequence)), observed.len);
        for (observed, 0..) |entry, index| {
            try std.testing.expectEqual(@as(u64, @intCast(index + 1)), entry.sequence);
        }
        try std.testing.expectEqual(
            cmd.OrderCommandType.order_book_request,
            observed[1].input.command,
        );
        try std.testing.expectEqual(
            cmd.OrderCommandType.order_book_request,
            observed[2].input.command,
        );
        for (observed[@intCast(binary_query_first_sequence - 1)..@intCast(binary_query_last_sequence)]) |entry| {
            try std.testing.expectEqual(cmd.OrderCommandType.binary_data_query, entry.input.command);
        }
        try std.testing.expectEqual(@as(usize, 0), core.pendingCallbackCount());
        try std.testing.expectEqual(core.driver.accepted_total, core.driver.completed_total);
    }

    var durable = try journal_mod.CommandJournal.readFromFile(
        allocator,
        tmp.dir,
        native_path,
    );
    defer durable.deinit();
    try std.testing.expectEqual(
        @as(usize, 2 + binary_command_frames),
        durable.entries.items.len,
    );
    try std.testing.expectEqual(@as(u64, 1), durable.entries.items[0].sequence);
    try std.testing.expectEqual(cmd.OrderCommandType.add_user, durable.entries.items[0].input.command);
    for (durable.entries.items[1 .. 1 + binary_command_frames], 0..) |entry, index| {
        try std.testing.expectEqual(
            binary_command_first_sequence + @as(u64, @intCast(index)),
            entry.sequence,
        );
        try std.testing.expectEqual(cmd.OrderCommandType.binary_data_command, entry.input.command);
    }
    const durable_last = durable.entries.items[durable.entries.items.len - 1];
    try std.testing.expectEqual(final_sequence, durable_last.sequence);
    try std.testing.expectEqual(cmd.OrderCommandType.add_user, durable_last.input.command);
    for (durable.entries.items) |entry| {
        try std.testing.expect(shouldAppendToJavaDurableJournal(entry.input.command));
    }
}

test "exchange core Java durable journal command predicate matches source mutability" {
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

test "exchange core durable checkpoint preserves a trailing skipped sequence across reopen" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const config: Config = .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXDURTRAILING"),
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = "store",
        },
    };
    const native_path = try nativeJournalPartitionPath(allocator, &config);
    defer allocator.free(native_path);

    {
        var source = try ExchangeCore.initWithPersistenceRoot(allocator, tmp.dir, config);
        defer source.deinit();
        var first = try source.addUser(419_001);
        defer first.deinit(allocator);
        var nop_command = makeNopCommand();
        var trailing = try source.submitCommand(&nop_command);
        defer trailing.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 1), first.sequence);
        try std.testing.expectEqual(@as(u64, 2), trailing.sequence);
        try std.testing.expectEqual(@as(?u64, 2), source.durable_journal.?.lastSequence());
    }

    {
        var durable = try journal_mod.CommandJournal.readFromFile(
            allocator,
            tmp.dir,
            native_path,
        );
        defer durable.deinit();
        try std.testing.expectEqual(@as(usize, 1), durable.entries.items.len);
        try std.testing.expectEqual(@as(usize, 1), durable.frontier_checkpoints.items.len);
        try std.testing.expectEqual(
            @as(u64, 2),
            try durable.replayFrontier(0, std.math.maxInt(u64), null),
        );
    }

    var restored_config = config;
    restored_config.initial_state = config_mod.InitialStateConfig.lastKnownStateFromJournal(
        config.initial_state.exchange_id,
        0,
        0,
    );
    var restored = try ExchangeCore.initWithPersistenceRoot(
        allocator,
        tmp.dir,
        restored_config,
    );
    defer restored.deinit();
    const outcome = try restored.bootstrapFromPersistence(tmp.dir, null, native_path);
    try std.testing.expectEqual(@as(usize, 1), outcome.journal_replayed_commands);
    try std.testing.expectEqual(@as(u64, 2), restored.kernelRef().next_seq);
    try std.testing.expectEqual(@as(u64, 2), restored.driver.publication_sequencer.highWater());
    try restored.startup();
    var next = try restored.addUser(419_002);
    defer next.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 3), next.sequence);
}

test "exchange core durable XCJ3 reopens at restored snapshot frontier" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const snapshot_id: u64 = 420_001;
    const config: Config = .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXDURREOPEN"),
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = "store",
        },
    };
    const native_path = try nativeJournalPartitionPath(allocator, &config);
    defer allocator.free(native_path);
    const matching_path = try persistence_paths.snapshotPath(
        allocator,
        config.serialization.storage_folder,
        config.initial_state.exchange_id,
        snapshot_id,
        .matching_engine_router,
        0,
    );
    defer allocator.free(matching_path);

    {
        var source = try ExchangeCore.initWithPersistenceRoot(allocator, tmp.dir, config);
        defer source.deinit();
        var first = try source.addUser(420_100);
        defer first.deinit(allocator);
        try std.testing.expectEqual(
            cmd.CommandResultCode.success,
            try source.persistState(snapshot_id, 10_000_100_001),
        );
        try std.testing.expectEqual(@as(?u64, 3), source.durable_journal.?.lastSequence());
    }

    const restored_config: Config = .{
        .initial_state = config_mod.InitialStateConfig.fromSnapshotOnly(
            config.initial_state.exchange_id,
            snapshot_id,
            0,
        ),
        .serialization = config.serialization,
    };
    {
        var restored = try ExchangeCore.initWithPersistenceRoot(
            allocator,
            tmp.dir,
            restored_config,
        );
        defer restored.deinit();
        const outcome = try restored.bootstrapFromPersistence(tmp.dir, matching_path, null);
        try std.testing.expect(outcome.snapshot_found);
        try std.testing.expectEqual(@as(u64, 3), restored.driver.publication_sequencer.highWater());
        try restored.startup();
        var next = try restored.addUser(420_101);
        defer next.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 4), next.sequence);
        try std.testing.expectEqual(@as(?u64, 4), restored.durable_journal.?.lastSequence());
    }

    var durable = try journal_mod.CommandJournal.readFromFile(allocator, tmp.dir, native_path);
    defer durable.deinit();
    try std.testing.expectEqual(@as(usize, 4), durable.entries.items.len);
    try std.testing.expectEqual(@as(u64, 4), durable.entries.items[3].sequence);
}

test "exchange core committed snapshot adopts a journal record lost before append" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const snapshot_id: u64 = 421_001;
    const config: Config = .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXDURADOPT"),
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = "store",
        },
    };
    const native_path = try nativeJournalPartitionPath(allocator, &config);
    defer allocator.free(native_path);
    const matching_path = try persistence_paths.snapshotPath(
        allocator,
        config.serialization.storage_folder,
        config.initial_state.exchange_id,
        snapshot_id,
        .matching_engine_router,
        0,
    );
    defer allocator.free(matching_path);

    {
        var source = try ExchangeCore.initWithPersistenceRoot(allocator, tmp.dir, config);
        defer source.deinit();
        var first = try source.addUser(421_100);
        defer first.deinit(allocator);
        source.durable_journal.?.injectPreWriteFailureAtSequenceForTesting(3);
        try std.testing.expectError(
            error.InjectedJournalIoFailure,
            source.persistState(@intCast(snapshot_id), 10_000_200_001),
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
        try std.testing.expectEqual(@as(u64, 2), before_adoption.entries.items[1].sequence);
    }

    var restored_config = config;
    restored_config.initial_state = config_mod.InitialStateConfig.fromSnapshotOnly(
        config.initial_state.exchange_id,
        snapshot_id,
        0,
    );
    {
        var failed_restore = try ExchangeCore.initWithPersistenceRoot(
            allocator,
            tmp.dir,
            restored_config,
        );
        defer failed_restore.deinit();
        failed_restore.durable_journal.?.injectPreWriteFailureAtSequenceForTesting(3);
        try std.testing.expectError(
            error.InjectedJournalIoFailure,
            failed_restore.bootstrapFromPersistence(tmp.dir, matching_path, null),
        );
        const fatal = failed_restore.fatalInfo() orelse return error.MissingFatalRecord;
        try std.testing.expectEqual(error.InjectedJournalIoFailure, fatal.cause);
        try std.testing.expectEqual(@as(?u64, 3), fatal.sequence);
        try std.testing.expectError(error.CorePoisoned, failed_restore.addUser(421_099));
    }

    var restored = try ExchangeCore.initWithPersistenceRoot(
        allocator,
        tmp.dir,
        restored_config,
    );
    defer restored.deinit();
    const outcome = try restored.bootstrapFromPersistence(tmp.dir, matching_path, null);
    try std.testing.expect(outcome.snapshot_found);
    try std.testing.expectEqual(@as(u64, 3), restored.kernelRef().next_seq);
    try std.testing.expectEqual(@as(?u64, 3), restored.durable_journal.?.lastSequence());
    try restored.startup();
    var next = try restored.addUser(421_101);
    defer next.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 4), next.sequence);

    var adopted = try journal_mod.CommandJournal.readFromFile(allocator, tmp.dir, native_path);
    defer adopted.deinit();
    try std.testing.expectEqual(@as(usize, 3), adopted.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), adopted.frontier_checkpoints.items.len);
    try std.testing.expectEqual(@as(u64, 3), adopted.frontier_checkpoints.items[0].sequence);
}

test "exchange core native journal isolation preserves ecj and refuses incompatible XCJ3 path" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const config: Config = .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXDURFORMAT"),
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = "store",
        },
    };
    try tmp.dir.createDirPath(std.testing.io, config.serialization.storage_folder);
    const legacy_path = try persistence_paths.journalPath(
        allocator,
        config.serialization.storage_folder,
        config.initial_state.exchange_id,
        0,
        NativeJournalPartitionId,
    );
    defer allocator.free(legacy_path);
    {
        const legacy = try tmp.dir.createFile(std.testing.io, legacy_path, .{ .truncate = true });
        defer legacy.close(std.testing.io);
        try legacy.writeStreamingAll(std.testing.io, "XCJ1-java-owned");
    }
    {
        var core = try ExchangeCore.initWithIoAndPersistenceRoot(
            allocator,
            std.testing.io,
            tmp.dir,
            config,
        );
        core.deinit();
    }
    {
        const legacy = try tmp.dir.openFile(std.testing.io, legacy_path, .{});
        defer legacy.close(std.testing.io);
        var bytes: [15]u8 = undefined;
        try std.testing.expectEqual(bytes.len, try legacy.readPositionalAll(std.testing.io, &bytes, 0));
        try std.testing.expectEqualStrings("XCJ1-java-owned", &bytes);
    }

    const native_path = try nativeJournalPartitionPath(allocator, &config);
    defer allocator.free(native_path);
    {
        const incompatible = try tmp.dir.createFile(std.testing.io, native_path, .{ .truncate = true });
        defer incompatible.close(std.testing.io);
        try incompatible.writeStreamingAll(std.testing.io, "XCJ2");
    }
    try std.testing.expectError(
        error.InvalidMagic,
        ExchangeCore.initWithIoAndPersistenceRoot(
            allocator,
            std.testing.io,
            tmp.dir,
            config,
        ),
    );
}

test "exchange core journal replay mismatch poisons the mutated instance before retry" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const RawCount = struct {
        count: usize = 0,

        fn raw(context: ?*anyopaque, _: *const kernel_mod.CommandResponse) void {
            const self: *@This() = @ptrCast(@alignCast(context.?));
            self.count += 1;
        }
    };

    const source_config: Config = .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EC-REPLAY-FAIL-SOURCE"),
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = "source-store",
        },
    };
    {
        var source = try ExchangeCore.initWithIoAndPersistenceRoot(
            allocator,
            std.testing.io,
            tmp.dir,
            source_config,
        );
        defer source.deinit();
        var response = try source.addUser(429_101);
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);

        // Keep the command and sequence valid but falsify the result so
        // replay mutates sequence 1 before detecting the integrity failure.
        source.journal.?.entries.items[0].result_code =
            .user_mgmt_user_already_exists;
        try source.writeJournalToFile(tmp.dir, "mismatched-result.xcj");
    }

    const replay_config: Config = .{
        .initial_state = .{
            .exchange_id = "EC-REPLAY-FAIL-TARGET",
            .journal_timestamp_ns = std.math.maxInt(u64),
            .throw_if_snapshot_not_found = false,
        },
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = "target-store",
        },
    };
    var replayed = try ExchangeCore.initWithIoAndPersistenceRoot(
        allocator,
        std.testing.io,
        tmp.dir,
        replay_config,
    );
    defer replayed.deinit();
    var raw_count: RawCount = .{};
    replayed.setRawResultSink(.{
        .context = &raw_count,
        .on_result = RawCount.raw,
    });
    try std.testing.expectError(
        error.ResultMismatch,
        replayed.replayJournalFromFile(tmp.dir, "mismatched-result.xcj"),
    );
    try std.testing.expectEqual(@as(usize, 0), raw_count.count);
    const fatal = replayed.fatalInfo() orelse return error.MissingFatalRecord;
    try std.testing.expectEqual(error.ResultMismatch, fatal.cause);
    try std.testing.expectEqual(@as(?u64, 1), fatal.sequence);
    try std.testing.expectEqual(cmd.OrderCommandType.add_user, fatal.command);

    // The first attempt already applied sequence 1. A retry must fail closed
    // instead of skipping the command and bypassing its recorded-result check.
    try std.testing.expectError(
        error.CorePoisoned,
        replayed.replayJournalFromFile(tmp.dir, "mismatched-result.xcj"),
    );
    try std.testing.expectEqual(@as(usize, 0), raw_count.count);
    try std.testing.expectError(error.CorePoisoned, replayed.startup());
}

test "exchange core durable append failure poisons direct and callback completion" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const RawJournalProbe = struct {
        core: *ExchangeCore,
        count: usize = 0,
        durable_visible: bool = false,

        fn raw(context: ?*anyopaque, response: *const kernel_mod.CommandResponse) void {
            const self: *@This() = @ptrCast(@alignCast(context.?));
            self.count += 1;
            self.durable_visible =
                self.core.durable_journal.?.lastSequence() == response.sequence;
        }
    };

    const direct_config: Config = .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXDURDIRECTFAIL"),
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = "direct-store",
        },
    };
    {
        var core = try ExchangeCore.initWithIoAndPersistenceRoot(
            allocator,
            std.testing.io,
            tmp.dir,
            direct_config,
        );
        defer core.deinit();
        var raw_probe: RawJournalProbe = .{ .core = &core };
        core.setRawResultSink(.{
            .context = &raw_probe,
            .on_result = RawJournalProbe.raw,
        });
        var first = try core.addUser(430_000);
        defer first.deinit(allocator);
        try std.testing.expectEqual(@as(usize, 1), raw_probe.count);
        try std.testing.expect(raw_probe.durable_visible);
        core.durable_journal.?.injectSyncFailureForTesting();
        try std.testing.expectError(error.InjectedJournalIoFailure, core.addUser(430_001));
        try std.testing.expectEqual(@as(usize, 1), raw_probe.count);
        try std.testing.expect(core.durable_journal.?.poisoned);
        const fatal = core.fatalInfo() orelse return error.MissingFatalRecord;
        try std.testing.expectEqual(error.InjectedJournalIoFailure, fatal.cause);
        try std.testing.expectEqual(@as(?u64, 2), fatal.sequence);
        try std.testing.expectError(error.CorePoisoned, core.addUser(430_002));
    }

    const callback_config: Config = .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXDURCALLBACKFAIL"),
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = "callback-store",
        },
    };
    {
        var core = try ExchangeCore.initWithIoAndPersistenceRoot(
            allocator,
            std.testing.io,
            tmp.dir,
            callback_config,
        );
        defer core.deinit();
        var probe: FatalCallbackProbe = .{};
        core.durable_journal.?.injectSyncFailureForTesting();
        try core.startup();
        try core.submitApiCommandWithCallback(.{ .add_user = 430_003 }, .{
            .context = &probe,
            .callback = FatalCallbackProbe.onResponse,
            .failure_callback = FatalCallbackProbe.onFailure,
        });
        try std.testing.expectError(error.CorePoisoned, core.drainAsyncPending());
        try std.testing.expectEqual(@as(usize, 0), probe.response_count);
        try std.testing.expectEqual(@as(usize, 1), probe.failure_count);
        try std.testing.expectEqual(error.InjectedJournalIoFailure, probe.causes[0]);
        try std.testing.expectEqual(@as(usize, 0), core.asyncReadyCount());
        try std.testing.expect(core.durable_journal.?.poisoned);
    }
}

test "exchange core startup requires configured persistence bootstrap" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var snapshot_core = try ExchangeCore.initWithIoAndPersistenceRoot(
        allocator,
        std.testing.io,
        tmp.dir,
        .{
            .initial_state = config_mod.InitialStateConfig.fromSnapshotOnly("EXSTARTSNAP", 1, 0),
            .serialization = .{ .backend = .disk, .storage_folder = "snapshot-store" },
        },
    );
    defer snapshot_core.deinit();
    try std.testing.expectError(
        error.ConfiguredPersistenceBootstrapRequired,
        snapshot_core.startup(),
    );
    try std.testing.expectError(
        error.SnapshotFileNotFound,
        snapshot_core.bootstrapFromPersistence(tmp.dir, "missing-snapshot.ecs", null),
    );
    try std.testing.expect(!snapshot_core.initial_state_bootstrapped);
    try std.testing.expectError(
        error.ConfiguredPersistenceBootstrapRequired,
        snapshot_core.startup(),
    );

    var journal_core = try ExchangeCore.initWithIoAndPersistenceRoot(
        allocator,
        std.testing.io,
        tmp.dir,
        .{
            .initial_state = config_mod.InitialStateConfig.lastKnownStateFromJournal("EXSTARTJOURNAL", 0, 0),
            .serialization = .{
                .enable_journaling = true,
                .backend = .disk,
                .storage_folder = "journal-store",
            },
        },
    );
    defer journal_core.deinit();
    try std.testing.expectError(
        error.ConfiguredPersistenceBootstrapRequired,
        journal_core.startup(),
    );
    const outcome = try journal_core.startupFromConfiguredPersistence(
        tmp.dir,
        .matching_engine_router,
        0,
        1,
    );
    try std.testing.expectEqual(@as(usize, 0), outcome.journal_replayed_commands);
    try std.testing.expect(journal_core.isStarted());
}

test "exchange core validates durable journal frontier during bootstrap and startup" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const ahead_config: Config = .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXDURAHEAD"),
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = "ahead-store",
        },
    };
    {
        var core = try ExchangeCore.initWithIoAndPersistenceRoot(
            allocator,
            std.testing.io,
            tmp.dir,
            ahead_config,
        );
        defer core.deinit();
        try core.durable_journal.?.append(.{
            .sequence = 1,
            .input = makeNopCommand(),
            .result_code = .success,
        });
        try std.testing.expectError(
            error.DurableJournalFrontierMismatch,
            core.bootstrapFromPersistence(tmp.dir, null, null),
        );
        try std.testing.expect(!core.initial_state_bootstrapped);
        try std.testing.expectError(error.DurableJournalFrontierMismatch, core.startup());
    }

    const behind_config: Config = .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXDURBEHIND"),
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = "behind-store",
        },
    };
    {
        var core = try ExchangeCore.initWithIoAndPersistenceRoot(
            allocator,
            std.testing.io,
            tmp.dir,
            behind_config,
        );
        defer core.deinit();
        try core.durable_journal.?.append(.{
            .sequence = 1,
            .input = makeNopCommand(),
            .result_code = .success,
        });
        core.driver.publication_sequencer.restore(2);
        try std.testing.expectError(error.DurableJournalFrontierMismatch, core.startup());
    }

    const empty_config: Config = .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXDUREMPTY"),
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = "empty-store",
        },
    };
    {
        var core = try ExchangeCore.initWithIoAndPersistenceRoot(
            allocator,
            std.testing.io,
            tmp.dir,
            empty_config,
        );
        defer core.deinit();
        core.driver.publication_sequencer.restore(5);
        try std.testing.expectError(error.DurableJournalFrontierMismatch, core.startup());
    }
}

test "native journal path allocation failure is explicit" {
    const allocator = std.testing.allocator;
    var failing = std.testing.FailingAllocator.init(allocator, .{ .fail_index = 0 });
    const config: Config = .{
        .initial_state = config_mod.InitialStateConfig.cleanStart("EXDUROM"),
        .serialization = .{
            .enable_journaling = true,
            .backend = .disk,
            .storage_folder = "store",
        },
    };
    try std.testing.expectError(
        error.OutOfMemory,
        nativeJournalPartitionPath(failing.allocator(), &config),
    );
}

test "exchange core classifies order-book invariant failures at matching stage" {
    try std.testing.expectEqual(
        runtime_mod.FailureStage.matching,
        failureStage(error.OrderBookInvariantViolation),
    );
}

const CustomSerializationTestRecorder = struct {
    const journal_capacity = 32;
    const store_capacity = 8;

    factory_calls: usize = 0,
    startup_calls: usize = 0,
    store_calls: usize = 0,
    journal_calls: usize = 0,
    shutdown_calls: usize = 0,
    destroy_calls: usize = 0,
    matching_stores: usize = 0,
    risk_stores: usize = 0,
    factory_matching_count: usize = 0,
    factory_risk_count: usize = 0,
    startup_saw_single: bool = false,
    store_saw_single: bool = true,
    fail_startup: bool = false,
    fail_journal: bool = false,
    reject_matching_store: bool = false,
    reject_risk_store: bool = false,
    store_modules: [store_capacity]serialization_processor.ModuleType = undefined,
    journal_requests: [journal_capacity]serialization_processor.JournalRequest = undefined,

    fn fromContext(context: ?*anyopaque) *CustomSerializationTestRecorder {
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
        self.startup_saw_single = request.state == .single;
        if (self.fail_startup) return error.SerializationProcessorStartupFailed;
    }

    fn store(
        context: ?*anyopaque,
        request: serialization_processor.StoreRequest,
    ) serialization_processor.StoreError!bool {
        const self = fromContext(context);
        std.debug.assert(self.store_calls < self.store_modules.len);
        self.store_modules[self.store_calls] = request.module_type;
        self.store_calls += 1;
        const rejected = switch (request.module_type) {
            .matching_engine_router => rejected: {
                self.matching_stores += 1;
                break :rejected self.reject_matching_store;
            },
            .risk_engine => rejected: {
                self.risk_stores += 1;
                break :rejected self.reject_risk_store;
            },
        };
        switch (request.state) {
            .single => {},
            .sharded => self.store_saw_single = false,
        }
        return !rejected;
    }

    fn journal(
        context: ?*anyopaque,
        request: serialization_processor.JournalRequest,
    ) serialization_processor.JournalError!void {
        const self = fromContext(context);
        std.debug.assert(self.journal_calls < self.journal_requests.len);
        self.journal_requests[self.journal_calls] = request;
        self.journal_calls += 1;
        if (self.fail_journal) {
            return error.SerializationProcessorJournalWriteFailed;
        }
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

fn expectCustomJournalBatchSchedule(
    recorder: *const CustomSerializationTestRecorder,
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

test "exchange core custom serialization end-of-batch enumerates cold and running schedules" {
    const Schedule = enum {
        cold_batch,
        running_batch,
        running_singletons,
        running_control_prefix,
    };
    const schedules = [_]Schedule{
        .cold_batch,
        .running_batch,
        .running_singletons,
        .running_control_prefix,
    };

    for (schedules) |schedule| {
        var recorder: CustomSerializationTestRecorder = .{};
        const factory: serialization_processor.Factory = .{
            .context = &recorder,
            .create = CustomSerializationTestRecorder.create,
        };
        var core = try ExchangeCore.init(std.testing.allocator, .{
            .serialization = config_mod.SerializationConfig.custom(factory, true),
        });
        defer core.deinit();

        var commands = [_]cmd.OrderCommand{
            .{ .command = .add_user, .uid = 881_001 },
            .{ .command = .add_user, .uid = 881_002 },
            .{ .command = .add_user, .uid = 881_003 },
        };
        var command_ptrs = [_]*cmd.OrderCommand{
            &commands[0],
            &commands[1],
            &commands[2],
        };

        switch (schedule) {
            .cold_batch => {
                _ = try core.submitCommandBatch(command_ptrs[0..]);
                try expectCustomJournalBatchSchedule(&recorder, &.{3});
            },
            .running_batch => {
                try core.startup();
                _ = try core.submitCommandBatch(command_ptrs[0..]);
                try expectCustomJournalBatchSchedule(&recorder, &.{3});
            },
            .running_singletons => {
                try core.startup();
                for (&commands) |*command| {
                    var response = try core.submitCommand(command);
                    response.deinit(std.testing.allocator);
                }
                try expectCustomJournalBatchSchedule(&recorder, &.{ 1, 1, 1 });
            },
            .running_control_prefix => {
                try core.startup();
                var forced = [_]cmd.OrderCommand{
                    .{ .command = .add_user, .uid = 881_011 },
                    .{ .command = .reset },
                    .{ .command = .add_user, .uid = 881_012 },
                    .{ .command = .nop },
                };
                var forced_ptrs = [_]*cmd.OrderCommand{
                    &forced[0],
                    &forced[1],
                    &forced[2],
                    &forced[3],
                };
                _ = try core.submitCommandBatch(forced_ptrs[0..]);
                try expectCustomJournalBatchSchedule(&recorder, &.{4});
            },
        }
    }
}

test "exchange core custom serialization freezes captured batch before concurrent suffix" {
    var recorder: CustomSerializationTestRecorder = .{};
    const factory: serialization_processor.Factory = .{
        .context = &recorder,
        .create = CustomSerializationTestRecorder.create,
    };
    var core = try ExchangeCore.init(std.testing.allocator, .{
        .serialization = config_mod.SerializationConfig.custom(factory, true),
    });
    defer core.deinit();
    var gate: AvailableBatchPauseTestGate = .{};
    core.available_batch_test_gate = &gate;
    defer gate.open();
    try core.startup();

    const prefix = [_]cmd.OrderCommand{
        .{ .command = .add_user, .uid = 881_101 },
        .{ .command = .add_user, .uid = 881_102 },
    };
    try core.enqueueDetachedBatch(&prefix, .detached_api);
    gate.waitUntilEntered();

    const concurrent_suffix = [_]cmd.OrderCommand{
        .{ .command = .add_user, .uid = 881_103 },
        .{ .command = .add_user, .uid = 881_104 },
    };
    try core.enqueueDetachedBatch(&concurrent_suffix, .detached_api);
    gate.open();
    try std.testing.expectEqual(@as(usize, 0), try core.drainAsyncPending());
    try expectCustomJournalBatchSchedule(&recorder, &.{ 2, 2 });
}

test "exchange core custom serialization factory delegates lifecycle journal and stores" {
    var recorder: CustomSerializationTestRecorder = .{};
    const factory: serialization_processor.Factory = .{
        .context = &recorder,
        .create = CustomSerializationTestRecorder.create,
    };
    var core = try ExchangeCore.init(std.testing.allocator, .{
        .serialization = config_mod.SerializationConfig.custom(factory, true),
    });

    try core.startup();
    try std.testing.expectError(
        error.ConfiguredMainLogRequiresDiskBackend,
        core.persistStateWithConfiguredMainLog(
            240_000,
            9_990_000,
            .matching_engine_router,
            0,
        ),
    );
    try std.testing.expectError(
        error.ConfiguredMainLogRequiresDiskBackend,
        core.persistStateWithDefaultMainLog(
            std.Io.Dir.cwd(),
            240_000,
            9_990_000,
            .matching_engine_router,
            0,
        ),
    );
    try std.testing.expectEqual(@as(usize, 0), recorder.store_calls);
    try std.testing.expectEqual(@as(usize, 0), recorder.journal_calls);
    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try core.persistState(240_001, 9_990_001),
    );
    core.shutdown();

    try std.testing.expectEqual(@as(usize, 1), recorder.factory_calls);
    try std.testing.expectEqual(@as(usize, 1), recorder.factory_matching_count);
    try std.testing.expectEqual(@as(usize, 1), recorder.factory_risk_count);
    try std.testing.expectEqual(@as(usize, 1), recorder.startup_calls);
    try std.testing.expect(recorder.startup_saw_single);
    try std.testing.expectEqual(@as(usize, 2), recorder.store_calls);
    try std.testing.expectEqual(@as(usize, 1), recorder.matching_stores);
    try std.testing.expectEqual(@as(usize, 1), recorder.risk_stores);
    try std.testing.expect(recorder.store_saw_single);
    try std.testing.expectEqual(@as(usize, 2), recorder.journal_calls);
    try expectCustomJournalBatchSchedule(&recorder, &.{2});
    try std.testing.expectEqual(@as(usize, 1), recorder.shutdown_calls);

    core.deinit();
    try std.testing.expectEqual(@as(usize, 1), recorder.shutdown_calls);
    try std.testing.expectEqual(@as(usize, 1), recorder.destroy_calls);
}

test "exchange core custom persistence completes both callbacks and recovers after either rejection" {
    const FailureCase = struct {
        failed_snapshot_id: i64,
        valid_snapshot_id: i64,
        rejected_module: serialization_processor.ModuleType,
        expected_matching_result: cmd.CommandResultCode,
        expected_risk_result: cmd.CommandResultCode,
    };
    const cases = [_]FailureCase{
        .{
            .failed_snapshot_id = 240_101,
            .valid_snapshot_id = 240_102,
            .rejected_module = .matching_engine_router,
            .expected_matching_result = .state_persist_matching_engine_failed,
            .expected_risk_result = .success,
        },
        .{
            .failed_snapshot_id = 240_103,
            .valid_snapshot_id = 240_104,
            .rejected_module = .risk_engine,
            .expected_matching_result = .accepted,
            .expected_risk_result = .state_persist_risk_engine_failed,
        },
    };
    const expected_failed_modules = [_]serialization_processor.ModuleType{
        .matching_engine_router,
        .risk_engine,
    };
    const expected_all_modules = [_]serialization_processor.ModuleType{
        .matching_engine_router,
        .risk_engine,
        .matching_engine_router,
        .risk_engine,
    };

    for (cases) |case| {
        var recorder: CustomSerializationTestRecorder = .{
            .reject_matching_store = case.rejected_module == .matching_engine_router,
            .reject_risk_store = case.rejected_module == .risk_engine,
        };
        const factory: serialization_processor.Factory = .{
            .context = &recorder,
            .create = CustomSerializationTestRecorder.create,
        };
        var core = try ExchangeCore.init(std.testing.allocator, .{
            .serialization = config_mod.SerializationConfig.custom(factory, false),
        });
        defer core.deinit();
        try core.startup();

        const failed_requests = try core.persistStateAsync(
            case.failed_snapshot_id,
            9_990_101,
        );
        try std.testing.expectEqual(@as(usize, 2), try core.drainAsyncPending());
        try std.testing.expect(core.isPersistStateResultReady(failed_requests));

        var failed_matching = core.takeAsyncResult(failed_requests.matching_request_id) orelse
            return error.MissingAsyncResult;
        defer failed_matching.deinit(std.testing.allocator);
        var failed_risk = core.takeAsyncResult(failed_requests.risk_request_id) orelse
            return error.MissingAsyncResult;
        defer failed_risk.deinit(std.testing.allocator);
        try std.testing.expectEqual(case.expected_matching_result, failed_matching.result_code);
        try std.testing.expectEqual(case.expected_risk_result, failed_risk.result_code);
        try std.testing.expectEqual(@as(usize, 2), recorder.store_calls);
        try std.testing.expectEqualSlices(
            serialization_processor.ModuleType,
            &expected_failed_modules,
            recorder.store_modules[0..2],
        );
        try std.testing.expect(core.pending_persist_pair == null);
        try std.testing.expectEqual(@as(usize, 1), try core.persistenceSnapshotCount());
        try std.testing.expectEqual(
            @as(?SnapshotRef, null),
            try core.persistenceFindSnapshotById(@intCast(case.failed_snapshot_id)),
        );

        recorder.reject_matching_store = false;
        recorder.reject_risk_store = false;
        const valid_requests = try core.persistStateAsync(
            case.valid_snapshot_id,
            9_990_102,
        );
        try std.testing.expectEqual(@as(usize, 2), try core.drainAsyncPending());
        try std.testing.expect(core.isPersistStateResultReady(valid_requests));

        var valid_matching = core.takeAsyncResult(valid_requests.matching_request_id) orelse
            return error.MissingAsyncResult;
        defer valid_matching.deinit(std.testing.allocator);
        var valid_risk = core.takeAsyncResult(valid_requests.risk_request_id) orelse
            return error.MissingAsyncResult;
        defer valid_risk.deinit(std.testing.allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.accepted, valid_matching.result_code);
        try std.testing.expectEqual(cmd.CommandResultCode.success, valid_risk.result_code);
        try std.testing.expectEqual(@as(usize, 4), recorder.store_calls);
        try std.testing.expectEqualSlices(
            serialization_processor.ModuleType,
            &expected_all_modules,
            recorder.store_modules[0..4],
        );
        try std.testing.expect(core.pending_persist_pair == null);
        try std.testing.expectEqual(@as(usize, 2), try core.persistenceSnapshotCount());
        try std.testing.expectEqual(
            @as(u64, @intCast(case.valid_snapshot_id)),
            (try core.persistenceCurrentSnapshot()).snapshot_id,
        );
    }
}

test "exchange core custom serialization startup and journal errors propagate" {
    {
        var recorder: CustomSerializationTestRecorder = .{
            .fail_startup = true,
        };
        const factory: serialization_processor.Factory = .{
            .context = &recorder,
            .create = CustomSerializationTestRecorder.create,
        };
        var core = try ExchangeCore.init(std.testing.allocator, .{
            .serialization = config_mod.SerializationConfig.custom(factory, false),
        });
        defer core.deinit();
        try std.testing.expectError(
            error.SerializationProcessorStartupFailed,
            core.startup(),
        );
    }

    {
        var recorder: CustomSerializationTestRecorder = .{
            .fail_journal = true,
        };
        const factory: serialization_processor.Factory = .{
            .context = &recorder,
            .create = CustomSerializationTestRecorder.create,
        };
        var core = try ExchangeCore.init(std.testing.allocator, .{
            .serialization = config_mod.SerializationConfig.custom(factory, true),
        });
        defer core.deinit();
        try core.startup();
        try std.testing.expectError(
            error.CorePoisoned,
            core.persistState(240_002, 9_990_002),
        );
        const fatal = core.fatalInfo() orelse return error.MissingFatalInfo;
        try std.testing.expectEqual(
            error.SerializationProcessorJournalWriteFailed,
            fatal.cause,
        );
    }
}
