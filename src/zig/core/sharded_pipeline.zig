const std = @import("std");
const cmd = @import("cmd.zig");
const common = @import("common.zig");
const events = @import("events.zig");
const kernel_mod = @import("exchange_kernel.zig");
const locked_allocator_mod = @import("locked_allocator.zig");
const publication = @import("publication_sequence.zig");
const settlement_lifecycle = @import("settlement_lifecycle.zig");

pub const SlotPhase = enum {
    free,
    accepted,
    matching_complete,
    reclaiming,
    poisoned,
};

pub const PipelineSlot = struct {
    phase: SlotPhase = .free,
    generation: u64 = 0,
    sequence: u64 = 0,
    events_group: i64 = 0,
    command: cmd.OrderCommand = .{
        .command = .nop,
        .result_code = .new,
    },
    response: ?kernel_mod.CommandResponse = null,
    r2_events_head: ?*events.MatcherTradeEvent = null,
    r2_top: ?events.L2Top = null,
    requires_risk_release: bool = false,
    r2_release_kind: ?kernel_mod.MultiShardKernelSet.StageAdapter.RiskReleaseKind = null,
    journal_acked: bool = false,
    result_transferred: bool = false,
    r2_complete: bool = false,

    fn resetOwned(self: *PipelineSlot, allocator: std.mem.Allocator) void {
        if (self.response) |*response| response.deinit(allocator);
        events.deinitMatcherEventChain(allocator, self.r2_events_head);
        const generation = self.generation;
        self.* = .{ .generation = generation };
    }
};

pub const SlotHandle = struct {
    index: usize,
    generation: u64,
    sequence: u64,
};

/// One immutable publication captured by the API driver while it owns the
/// source queue. The persistent runtime copies every command before returning
/// from batch admission; it never retains these borrowed pointers.
pub const Submission = struct {
    sequence: publication.PublicationSequence,
    command: *const cmd.OrderCommand,
};

pub const StableSlotRing = struct {
    storage_allocator: std.mem.Allocator,
    payload_allocator: std.mem.Allocator,
    slots: []PipelineSlot,
    mask: usize,

    pub const InitError = std.mem.Allocator.Error || error{InvalidCapacity};
    pub const ClaimError = error{SlotNotReclaimed};

    pub fn init(
        storage_allocator: std.mem.Allocator,
        payload_allocator: std.mem.Allocator,
        requested_capacity: usize,
    ) InitError!StableSlotRing {
        if (requested_capacity == 0 or @popCount(requested_capacity) != 1) {
            return error.InvalidCapacity;
        }
        const slots = try storage_allocator.alloc(PipelineSlot, requested_capacity);
        for (slots) |*slot| slot.* = .{};
        return .{
            .storage_allocator = storage_allocator,
            .payload_allocator = payload_allocator,
            .slots = slots,
            .mask = requested_capacity - 1,
        };
    }

    pub fn deinit(self: *StableSlotRing) void {
        for (self.slots) |*slot| slot.resetOwned(self.payload_allocator);
        self.storage_allocator.free(self.slots);
        self.* = undefined;
    }

    pub fn capacity(self: *const StableSlotRing) usize {
        return self.slots.len;
    }

    pub fn claim(
        self: *StableSlotRing,
        sequence: u64,
    ) ClaimError!struct { handle: SlotHandle, slot: *PipelineSlot } {
        const index = self.indexFor(sequence);
        const slot = &self.slots[index];
        if (slot.phase != .free) return error.SlotNotReclaimed;
        slot.generation +%= 1;
        if (slot.generation == 0) slot.generation = 1;
        slot.sequence = sequence;
        slot.phase = .accepted;
        return .{
            .handle = .{
                .index = index,
                .generation = slot.generation,
                .sequence = sequence,
            },
            .slot = slot,
        };
    }

    pub fn get(
        self: *StableSlotRing,
        handle: SlotHandle,
    ) ?*PipelineSlot {
        if (handle.index >= self.slots.len) return null;
        const slot = &self.slots[handle.index];
        if (slot.phase == .free or
            slot.generation != handle.generation or
            slot.sequence != handle.sequence)
        {
            return null;
        }
        return slot;
    }

    pub fn getSequence(
        self: *StableSlotRing,
        sequence: u64,
    ) ?*PipelineSlot {
        const slot = &self.slots[self.indexFor(sequence)];
        if (slot.phase == .free or slot.sequence != sequence) return null;
        return slot;
    }

    pub fn release(
        self: *StableSlotRing,
        handle: SlotHandle,
    ) void {
        const slot = self.get(handle) orelse unreachable;
        slot.resetOwned(self.payload_allocator);
    }

    fn canClaim(self: *const StableSlotRing, sequence: u64) bool {
        return self.slots[self.indexFor(sequence)].phase == .free;
    }

    fn indexFor(self: *const StableSlotRing, sequence: u64) usize {
        return @as(usize, @intCast(sequence & @as(u64, @intCast(self.mask))));
    }
};

pub const StageFrontiers = struct {
    allocator: std.mem.Allocator,
    accepted: u64,
    journal: u64,
    risk_r1: []u64,
    matching: []u64,
    result: u64,
    risk_r2: []u64,
    reclaimed: u64,

    pub fn init(
        allocator: std.mem.Allocator,
        risk_count: usize,
        matching_count: usize,
        settled: u64,
    ) std.mem.Allocator.Error!StageFrontiers {
        const risk_r1 = try allocator.alloc(u64, risk_count);
        errdefer allocator.free(risk_r1);
        const matching = try allocator.alloc(u64, matching_count);
        errdefer allocator.free(matching);
        const risk_r2 = try allocator.alloc(u64, risk_count);
        @memset(risk_r1, settled);
        @memset(matching, settled);
        @memset(risk_r2, settled);
        return .{
            .allocator = allocator,
            .accepted = settled,
            .journal = settled,
            .risk_r1 = risk_r1,
            .matching = matching,
            .result = settled,
            .risk_r2 = risk_r2,
            .reclaimed = settled,
        };
    }

    pub fn deinit(self: *StageFrontiers) void {
        self.allocator.free(self.risk_r1);
        self.allocator.free(self.matching);
        self.allocator.free(self.risk_r2);
        self.* = undefined;
    }

    pub fn minRiskR2(self: *const StageFrontiers) u64 {
        var minimum = self.accepted;
        for (self.risk_r2) |frontier| minimum = @min(minimum, frontier);
        return minimum;
    }

    pub fn adoptSettled(self: *StageFrontiers, settled: u64) void {
        self.accepted = settled;
        self.journal = settled;
        @memset(self.risk_r1, settled);
        @memset(self.matching, settled);
        self.result = settled;
        @memset(self.risk_r2, settled);
        self.reclaimed = settled;
    }

    pub fn allSettledAtAccepted(self: *const StageFrontiers) bool {
        if (self.journal != self.accepted or
            self.result != self.accepted or
            self.reclaimed != self.accepted)
        {
            return false;
        }
        for (self.risk_r1) |frontier| {
            if (frontier != self.accepted) return false;
        }
        for (self.matching) |frontier| {
            if (frontier != self.accepted) return false;
        }
        for (self.risk_r2) |frontier| {
            if (frontier != self.accepted) return false;
        }
        return true;
    }
};

/// Allocation-free, monotonic discovery state for one consecutive events-group
/// range. Group identifiers are deliberately not treated as ordered or unique:
/// the sequence interval is the cache key, so wrapped and later-reused group
/// identifiers cannot alias an earlier range.
const R2GroupBoundaryCache = struct {
    const Range = struct {
        start: u64,
        group: i64,
        scanned_through: u64,
        closed_end: ?u64 = null,
    };

    range: ?Range = null,
    discovery_probes: u64 = 0,

    fn clearRange(self: *R2GroupBoundaryCache) void {
        self.range = null;
    }

    fn probeCount(self: *const R2GroupBoundaryCache) u64 {
        return self.discovery_probes;
    }

    fn closedBoundary(
        self: *R2GroupBoundaryCache,
        slots: *StableSlotRing,
        sequence: u64,
        group: i64,
        accepted: u64,
        terminal_boundary: ?u64,
    ) ?u64 {
        if (sequence > accepted) return null;

        const compatible = if (self.range) |range|
            range.group == group and
                sequence >= range.start and
                accepted >= range.scanned_through and
                (if (range.closed_end) |end|
                    sequence <= end
                else
                    sequence == range.start)
        else
            false;
        if (!compatible) {
            self.range = .{
                .start = sequence,
                .group = group,
                .scanned_through = sequence,
            };
        }

        const range = &self.range.?;
        if (range.closed_end) |end| return end;

        var cursor = range.scanned_through;
        while (cursor < accepted) {
            const next = cursor + 1;
            self.discovery_probes +%= 1;
            const next_slot = slots.getSequence(next) orelse return null;
            if (next_slot.events_group != group) {
                range.closed_end = cursor;
                return cursor;
            }
            cursor = next;
            range.scanned_through = cursor;
        }
        if (terminal_boundary == cursor) {
            range.closed_end = cursor;
            return cursor;
        }
        return null;
    }
};

pub const FailureStage = enum {
    accept,
    publication_ownership,
    risk_pre_process,
    matching,
    journal,
    risk_release,
};

pub const FatalRecord = struct {
    stage: FailureStage,
    sequence: u64,
    shard_index: ?usize = null,
    command: ?cmd.OrderCommandType = null,
    cause: anyerror,
};

pub const SerialGroupedPipeline = struct {
    pub const SubmitError = kernel_mod.ExecuteError ||
        StableSlotRing.ClaimError ||
        error{
            PipelinePoisoned,
            NonContiguousPublicationSequence,
            PublicationFrontierOwnershipLost,
        };
    pub const JournalError = error{
        PipelinePoisoned,
        UnknownSequence,
        NonContiguousJournalAck,
    };
    pub const FlushError = kernel_mod.ExecuteError || error{
        PipelinePoisoned,
        PublicationFrontierOwnershipLost,
    };

    adapter: kernel_mod.MultiShardKernelSet.StageAdapter,
    slots: StableSlotRing,
    frontiers: StageFrontiers,
    open_group: ?i64 = null,
    fatal_record: ?FatalRecord = null,

    pub fn init(
        allocator: std.mem.Allocator,
        kernels: *kernel_mod.MultiShardKernelSet,
        capacity: usize,
    ) (StableSlotRing.InitError || std.mem.Allocator.Error)!SerialGroupedPipeline {
        const adapter = kernel_mod.MultiShardKernelSet.StageAdapter.init(kernels);
        var slots = try StableSlotRing.init(
            allocator,
            adapter.allocator(),
            capacity,
        );
        errdefer slots.deinit();
        const frontiers = try StageFrontiers.init(
            allocator,
            adapter.riskShardCount(),
            adapter.matchingShardCount(),
            adapter.settledFrontier(),
        );
        return .{
            .adapter = adapter,
            .slots = slots,
            .frontiers = frontiers,
        };
    }

    pub fn deinit(self: *SerialGroupedPipeline) void {
        self.slots.deinit();
        self.frontiers.deinit();
        self.* = undefined;
    }

    pub fn fatalInfo(self: *const SerialGroupedPipeline) ?FatalRecord {
        return self.fatal_record;
    }

    pub fn observeIdle(self: *SerialGroupedPipeline) FlushError!void {
        try self.ensureRunning();
        try self.ensurePublicationOwnership();
    }

    pub fn submitAtSequence(
        self: *SerialGroupedPipeline,
        assigned_sequence: publication.PublicationSequence,
        command: *cmd.OrderCommand,
    ) SubmitError!SlotHandle {
        try self.ensureRunning();
        try self.ensurePublicationOwnership();
        const sequence = assigned_sequence.toInt();
        const expected_sequence = nextSequence(self.frontiers.accepted) catch
            return error.SequenceOverflow;
        if (sequence != expected_sequence) {
            return error.NonContiguousPublicationSequence;
        }
        try self.adapter.validatePublicationSequence(assigned_sequence);

        // Reserve before the prior group is settled. A full ring or any other
        // admission rejection must not make older R2 financial state visible.
        const claimed = try self.slots.claim(sequence);
        var reservation_only = true;
        errdefer if (reservation_only) self.slots.release(claimed.handle);

        if (self.open_group) |group| {
            if (group != command.events_group) {
                try self.settleOpenGroup();
            }
        }

        const slot = claimed.slot;
        slot.events_group = command.events_group;
        slot.command = command.*;

        self.adapter.consumePublicationSequence(assigned_sequence);
        self.frontiers.accepted = sequence;
        if (self.open_group == null) self.open_group = command.events_group;
        reservation_only = false;

        self.adapter.runRiskPreProcess(sequence, &slot.command) catch |err| {
            slot.phase = .poisoned;
            self.poison(.risk_pre_process, sequence, err);
            return err;
        };
        @memset(self.frontiers.risk_r1, sequence);

        var output = self.adapter.runMatching(sequence, &slot.command) catch |err| {
            slot.phase = .poisoned;
            self.poison(.matching, sequence, err);
            return err;
        };
        slot.response = output.response;
        output.response.matcher_events_head = null;
        output.response.l2_snapshot = null;
        output.response.report_response = null;
        output.response.java_report_response = null;
        slot.r2_events_head = output.r2_events_head;
        output.r2_events_head = null;
        slot.r2_top = output.r2_top;
        slot.requires_risk_release = output.requires_risk_release;
        slot.phase = .matching_complete;
        @memset(self.frontiers.matching, sequence);
        command.* = slot.command;
        return claimed.handle;
    }

    pub fn acknowledgeJournal(
        self: *SerialGroupedPipeline,
        sequence: u64,
    ) JournalError!void {
        try self.ensureRunning();
        if (self.frontiers.journal >= self.frontiers.accepted) {
            return error.NonContiguousJournalAck;
        }
        const expected_sequence = nextSequence(self.frontiers.journal) catch
            return error.NonContiguousJournalAck;
        if (sequence != expected_sequence) {
            return error.NonContiguousJournalAck;
        }
        const slot = self.slots.getSequence(sequence) orelse
            return error.UnknownSequence;
        slot.journal_acked = true;
        self.frontiers.journal = sequence;
    }

    /// Transfers the next response only after its journal acknowledgement.
    /// The caller exclusively owns the returned response.
    pub fn takeNextResult(
        self: *SerialGroupedPipeline,
    ) error{PipelinePoisoned}!?kernel_mod.CommandResponse {
        try self.ensureRunning();
        if (self.frontiers.result >= self.frontiers.accepted or
            self.frontiers.result >= self.frontiers.journal)
        {
            return null;
        }
        const sequence = nextSequence(self.frontiers.result) catch return null;
        const slot = self.slots.getSequence(sequence) orelse unreachable;
        std.debug.assert(slot.phase == .matching_complete);
        std.debug.assert(slot.journal_acked);
        const response = slot.response orelse unreachable;
        slot.response = null;
        slot.result_transferred = true;
        self.frontiers.result = sequence;
        self.reclaimCompleted();
        return response;
    }

    pub fn flushTerminal(self: *SerialGroupedPipeline) FlushError!void {
        try self.ensureRunning();
        try self.ensurePublicationOwnership();
        try self.settleOpenGroup();
    }

    fn settleOpenGroup(self: *SerialGroupedPipeline) FlushError!void {
        try self.ensureRunning();
        try self.ensurePublicationOwnership();
        var settled = self.frontiers.minRiskR2();
        while (settled < self.frontiers.accepted) {
            const sequence = nextSequence(settled) catch unreachable;
            const slot = self.slots.getSequence(sequence) orelse unreachable;
            if (slot.requires_risk_release) {
                var prepared = self.adapter.prepareAllRiskRelease(
                    &slot.command,
                    slot.r2_events_head,
                    slot.r2_top,
                ) catch |err| {
                    slot.phase = .poisoned;
                    self.poison(.risk_release, sequence, err);
                    return err;
                };
                self.adapter.commitPreparedRiskRelease(&prepared);
                prepared.deinit(self.adapter.allocator());
            }
            slot.r2_complete = true;
            @memset(self.frontiers.risk_r2, sequence);
            settled = sequence;
        }
        self.open_group = null;
        self.reclaimCompleted();
    }

    fn reclaimCompleted(self: *SerialGroupedPipeline) void {
        while (self.frontiers.reclaimed < self.frontiers.accepted) {
            const sequence = nextSequence(self.frontiers.reclaimed) catch
                unreachable;
            const slot = self.slots.getSequence(sequence) orelse break;
            if (!slot.result_transferred or !slot.r2_complete) break;
            const handle: SlotHandle = .{
                .index = @as(usize, @intCast(
                    sequence & @as(u64, @intCast(self.slots.mask)),
                )),
                .generation = slot.generation,
                .sequence = sequence,
            };
            self.slots.release(handle);
            self.frontiers.reclaimed = sequence;
        }
    }

    fn ensurePublicationOwnership(
        self: *SerialGroupedPipeline,
    ) error{PublicationFrontierOwnershipLost}!void {
        if (self.adapter.publicationFrontiersEqual(self.frontiers.accepted)) {
            return;
        }

        if (self.isFullyQuiescent()) {
            const observed = self.adapter.settledFrontier();
            if (observed >= self.frontiers.accepted and
                self.adapter.publicationFrontiersEqual(observed))
            {
                self.frontiers.adoptSettled(observed);
                return;
            }
        }

        self.poison(
            .publication_ownership,
            self.frontiers.accepted,
            error.PublicationFrontierOwnershipLost,
        );
        return error.PublicationFrontierOwnershipLost;
    }

    fn isFullyQuiescent(self: *const SerialGroupedPipeline) bool {
        if (self.open_group != null or
            !self.frontiers.allSettledAtAccepted())
        {
            return false;
        }
        for (self.slots.slots) |slot| {
            if (slot.phase != .free) return false;
        }
        return true;
    }

    fn ensureRunning(
        self: *const SerialGroupedPipeline,
    ) error{PipelinePoisoned}!void {
        if (self.fatal_record != null) return error.PipelinePoisoned;
    }

    fn poison(
        self: *SerialGroupedPipeline,
        stage: FailureStage,
        sequence: u64,
        cause: anyerror,
    ) void {
        if (self.fatal_record == null) {
            self.fatal_record = .{
                .stage = stage,
                .sequence = sequence,
                .command = if (self.slots.getSequence(sequence)) |slot|
                    slot.command.command
                else
                    null,
                .cause = cause,
            };
        }
    }
};

/// Ordinary-command grouped runtime used by the production sharded default.
///
/// The runtime itself and all worker contexts are heap allocated so worker
/// entry points never retain an address that can be invalidated by a caller
/// moving a value. `ExchangeApiSharded` owns this runtime whenever its pipeline
/// mode is `.persistent_grouped`; callers can explicitly pin `.immediate` for
/// the compatibility path.
///
/// `MultiShardKernelSet`'s payload allocator is used concurrently by matching
/// and risk workers. It must therefore be intrinsically thread-safe, or be one
/// stable `LockedAllocator` facade retained for the full kernel/runtime
/// lifetime. The `allocator` passed to `create` is storage-only: the runtime
/// uses it before workers start and after every worker has joined.
pub const PersistentGroupedRuntime = struct {
    pub const Lifecycle = enum {
        cold,
        starting,
        running,
        closing,
        stopped,
        poisoned,
    };

    pub const Config = struct {
        capacity: usize,
        /// Explicit integration attestation; see the runtime allocator contract.
        payload_allocator_is_thread_safe: bool = false,
        /// Deterministic test seam: fail before spawning this zero-based worker.
        spawn_failure_after: ?usize = null,
        start_gate: ?*StartGate = null,
        stage_gate: ?*StageGate = null,
        join_gate: ?*JoinGate = null,
        settlement_hooks: settlement_lifecycle.Hooks = .disabled(),
    };

    pub const CreateError = StableSlotRing.InitError || std.mem.Allocator.Error || error{
        CapacityOverflow,
        UnsafePayloadAllocator,
    };
    pub const StartError = std.Thread.SpawnError || error{
        InvalidLifecycle,
        InjectedSpawnFailure,
    };
    pub const SubmitError = kernel_mod.ExecuteError ||
        StableSlotRing.ClaimError ||
        error{
            PipelineNotRunning,
            PipelinePoisoned,
            UnsupportedCommand,
            NonContiguousPublicationSequence,
            PublicationFrontierOwnershipLost,
        };
    pub const JournalError = error{
        PipelineNotRunning,
        PipelinePoisoned,
        UnknownSequence,
        NonContiguousJournalAck,
    };
    pub const DurableJournalError = JournalError || settlement_lifecycle.Error;
    pub const ResultMetadataError = error{
        PipelinePoisoned,
        UnknownSequence,
        ResultNotMatched,
    };
    pub const WaitError = error{
        PipelinePoisoned,
        PipelineStopped,
    };
    pub const ShutdownError = error{
        PipelineNotRunning,
        PipelinePoisoned,
        ShutdownFromWorker,
    };
    pub const DestroyError = error{
        DestroyFromWorker,
        RuntimeStillActive,
        RuntimeBeingDestroyed,
    };

    pub const StartGate = struct {
        reached: std.atomic.Value(bool) = .init(false),
        competing_calls: std.atomic.Value(usize) = .init(0),
        release: std.atomic.Value(bool) = .init(false),

        pub fn waitUntilReached(self: *StartGate) void {
            while (!self.reached.load(.acquire)) std.Thread.yield() catch {};
        }

        pub fn open(self: *StartGate) void {
            self.release.store(true, .release);
        }

        pub fn waitForCompetingCalls(self: *StartGate, expected: usize) void {
            while (self.competing_calls.load(.acquire) < expected) {
                std.Thread.yield() catch {};
            }
        }
    };

    pub const StageGate = struct {
        stage: FailureStage,
        sequence: u64,
        shard_index: usize,
        fail: bool = false,
        reached: std.atomic.Value(bool) = .init(false),
        next_sequence_blocked: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        pub fn waitUntilReached(self: *StageGate) void {
            while (!self.reached.load(.acquire)) std.Thread.yield() catch {};
        }

        pub fn waitUntilNextSequenceBlocked(self: *StageGate) void {
            while (!self.next_sequence_blocked.load(.acquire)) {
                std.Thread.yield() catch {};
            }
        }

        pub fn open(self: *StageGate) void {
            self.release.store(true, .release);
        }
    };

    pub const JoinGate = struct {
        reached: std.atomic.Value(bool) = .init(false),
        competing_waiter_registered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        pub fn waitUntilReached(self: *JoinGate) void {
            while (!self.reached.load(.acquire)) std.Thread.yield() catch {};
        }

        pub fn waitUntilCompetingWaiterRegistered(self: *JoinGate) void {
            while (!self.competing_waiter_registered.load(.acquire)) {
                std.Thread.yield() catch {};
            }
        }

        pub fn open(self: *JoinGate) void {
            self.release.store(true, .release);
        }
    };

    const WorkerKind = enum {
        risk,
        matching,
    };

    const JoinState = enum {
        joined,
        joinable,
        joining,
    };

    const RiskReleaseScratchKey = struct {
        sequence: u64,
        generation: u64,
    };

    const RiskReleaseScratchState = union(enum) {
        idle,
        active: RiskReleaseScratchKey,
    };

    const SettlementPrepareState = enum {
        idle,
        preparing,
        done,
    };

    const RiskReleaseScratchSnapshot = struct {
        active: ?RiskReleaseScratchKey,
        prepared_count: usize,
        owned_count: usize,
        done_count: usize,
        committed_count: usize,
    };

    const WorkerContext = struct {
        runtime: *PersistentGroupedRuntime,
        kind: WorkerKind,
        shard_index: usize,
        thread_id: ?std.Thread.Id = null,
    };

    const RuntimeMutex = struct {
        inner: std.Io.Mutex = .init,
        threaded: *std.Io.Threaded,

        fn lock(self: *RuntimeMutex) void {
            std.Io.Threaded.mutexLock(&self.inner);
        }

        fn unlock(self: *RuntimeMutex) void {
            std.Io.Threaded.mutexUnlock(&self.inner);
        }
    };

    const RuntimeCondition = struct {
        inner: std.Io.Condition = .init,
        threaded: *std.Io.Threaded,

        fn wait(self: *RuntimeCondition, mutex: *RuntimeMutex) void {
            self.inner.waitUncancelable(self.threaded.io(), &mutex.inner);
        }

        fn broadcast(self: *RuntimeCondition) void {
            self.inner.broadcast(self.threaded.io());
        }
    };

    storage_allocator: std.mem.Allocator,
    threaded: std.Io.Threaded,
    adapter: kernel_mod.MultiShardKernelSet.StageAdapter,
    slots: StableSlotRing,
    frontiers: StageFrontiers,
    prepared: []?kernel_mod.MultiShardKernelSet.StageAdapter.PreparedShardRiskRelease,
    prepared_count: usize = 0,
    preparation_done: []bool,
    preparation_errors: []?anyerror,
    committed: []bool,
    risk_release_scratch: RiskReleaseScratchState = .idle,
    settlement_prepare_state: SettlementPrepareState = .idle,
    r2_group_boundary_cache: R2GroupBoundaryCache = .{},
    mutex: RuntimeMutex,
    condition: RuntimeCondition,
    state: Lifecycle = .cold,
    fatal_record: ?FatalRecord = null,
    risk_contexts: []WorkerContext,
    matching_contexts: []WorkerContext,
    risk_threads: []?std.Thread,
    matching_threads: []?std.Thread,
    active_workers: usize = 0,
    join_state: JoinState = .joined,
    join_callers: usize = 0,
    destroying: bool = false,
    admission_frozen: bool = false,
    quiescence_target: ?u64 = null,
    spawn_failure_after: ?usize,
    start_gate: ?*StartGate,
    stage_gate: ?*StageGate,
    join_gate: ?*JoinGate,
    settlement_hooks: settlement_lifecycle.Hooks,

    pub fn create(
        allocator: std.mem.Allocator,
        kernels: *kernel_mod.MultiShardKernelSet,
        config: Config,
    ) CreateError!*PersistentGroupedRuntime {
        if (!config.payload_allocator_is_thread_safe) {
            return error.UnsafePayloadAllocator;
        }
        const runtime = try allocator.create(PersistentGroupedRuntime);
        errdefer allocator.destroy(runtime);

        const adapter = kernel_mod.MultiShardKernelSet.StageAdapter.init(kernels);
        var slots = try StableSlotRing.init(
            allocator,
            adapter.allocator(),
            config.capacity,
        );
        errdefer slots.deinit();
        var frontiers = try StageFrontiers.init(
            allocator,
            adapter.riskShardCount(),
            adapter.matchingShardCount(),
            adapter.settledFrontier(),
        );
        errdefer frontiers.deinit();

        const prepared_len = adapter.riskShardCount();
        const prepared = try allocator.alloc(
            ?kernel_mod.MultiShardKernelSet.StageAdapter.PreparedShardRiskRelease,
            prepared_len,
        );
        errdefer allocator.free(prepared);
        @memset(prepared, null);
        const preparation_done = try allocator.alloc(bool, prepared_len);
        errdefer allocator.free(preparation_done);
        @memset(preparation_done, false);
        const preparation_errors = try allocator.alloc(?anyerror, prepared_len);
        errdefer allocator.free(preparation_errors);
        @memset(preparation_errors, null);
        const committed = try allocator.alloc(
            bool,
            prepared_len,
        );
        errdefer allocator.free(committed);
        @memset(committed, false);

        const risk_contexts = try allocator.alloc(WorkerContext, adapter.riskShardCount());
        errdefer allocator.free(risk_contexts);
        const matching_contexts = try allocator.alloc(
            WorkerContext,
            adapter.matchingShardCount(),
        );
        errdefer allocator.free(matching_contexts);
        const risk_threads = try allocator.alloc(?std.Thread, adapter.riskShardCount());
        errdefer allocator.free(risk_threads);
        @memset(risk_threads, null);
        const matching_threads = try allocator.alloc(
            ?std.Thread,
            adapter.matchingShardCount(),
        );
        errdefer allocator.free(matching_threads);
        @memset(matching_threads, null);

        runtime.* = .{
            .storage_allocator = allocator,
            .threaded = .init(allocator, .{}),
            .adapter = adapter,
            .slots = slots,
            .frontiers = frontiers,
            .prepared = prepared,
            .preparation_done = preparation_done,
            .preparation_errors = preparation_errors,
            .committed = committed,
            .risk_contexts = risk_contexts,
            .matching_contexts = matching_contexts,
            .risk_threads = risk_threads,
            .matching_threads = matching_threads,
            .spawn_failure_after = config.spawn_failure_after,
            .start_gate = config.start_gate,
            .stage_gate = config.stage_gate,
            .join_gate = config.join_gate,
            .settlement_hooks = config.settlement_hooks,
            .mutex = .{ .threaded = undefined },
            .condition = .{ .threaded = undefined },
        };
        runtime.mutex.threaded = &runtime.threaded;
        runtime.condition.threaded = &runtime.threaded;
        for (runtime.risk_contexts, 0..) |*context, shard_index| {
            context.* = .{
                .runtime = runtime,
                .kind = .risk,
                .shard_index = shard_index,
            };
        }
        for (runtime.matching_contexts, 0..) |*context, shard_index| {
            context.* = .{
                .runtime = runtime,
                .kind = .matching,
                .shard_index = shard_index,
            };
        }
        return runtime;
    }

    pub fn start(self: *PersistentGroupedRuntime) StartError!void {
        self.mutex.lock();
        if (self.state != .cold) {
            self.mutex.unlock();
            return error.InvalidLifecycle;
        }
        self.r2_group_boundary_cache.clearRange();
        self.state = .starting;
        self.join_state = .joinable;
        var spawned: usize = 0;
        for (self.risk_contexts, 0..) |*context, index| {
            if (self.spawn_failure_after == spawned) {
                self.rollbackStartLocked();
                return error.InjectedSpawnFailure;
            }
            self.active_workers += 1;
            self.risk_threads[index] = std.Thread.spawn(.{}, workerMain, .{context}) catch |err| {
                self.active_workers -= 1;
                self.rollbackStartLocked();
                return err;
            };
            spawned += 1;
        }
        for (self.matching_contexts, 0..) |*context, index| {
            if (self.spawn_failure_after == spawned) {
                self.rollbackStartLocked();
                return error.InjectedSpawnFailure;
            }
            self.active_workers += 1;
            self.matching_threads[index] = std.Thread.spawn(.{}, workerMain, .{context}) catch |err| {
                self.active_workers -= 1;
                self.rollbackStartLocked();
                return err;
            };
            spawned += 1;
        }
        if (self.start_gate) |gate| {
            gate.reached.store(true, .release);
            while (!gate.release.load(.acquire)) std.Thread.yield() catch {};
        }
        self.state = .running;
        self.condition.broadcast();
        self.mutex.unlock();

        self.mutex.lock();
        while (!self.allWorkerIdsRegisteredLocked()) {
            self.condition.wait(&self.mutex);
        }
        self.mutex.unlock();
    }

    pub fn lifecycle(self: *PersistentGroupedRuntime) Lifecycle {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.state;
    }

    pub fn fatalInfo(self: *PersistentGroupedRuntime) ?FatalRecord {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.fatal_record;
    }

    pub fn riskWorkerCount(self: *const PersistentGroupedRuntime) usize {
        return self.risk_contexts.len;
    }

    pub fn matchingWorkerCount(self: *const PersistentGroupedRuntime) usize {
        return self.matching_contexts.len;
    }

    pub fn riskReleaseFrontier(self: *PersistentGroupedRuntime) u64 {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.frontiers.minRiskR2();
    }

    pub fn riskPreProcessFrontier(self: *PersistentGroupedRuntime) u64 {
        self.mutex.lock();
        defer self.mutex.unlock();
        return minFrontier(self.frontiers.risk_r1, self.frontiers.accepted);
    }

    pub fn reclaimedFrontier(self: *PersistentGroupedRuntime) u64 {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.frontiers.reclaimed;
    }

    pub fn acceptedFrontier(self: *PersistentGroupedRuntime) u64 {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.frontiers.accepted;
    }

    pub fn occupiedSlotCount(self: *PersistentGroupedRuntime) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        var count: usize = 0;
        for (self.slots.slots) |slot| {
            if (slot.phase != .free) count += 1;
        }
        return count;
    }

    fn riskReleaseScratchCapacity(self: *const PersistentGroupedRuntime) usize {
        return self.prepared.len;
    }

    fn riskReleaseScratchSnapshot(
        self: *PersistentGroupedRuntime,
    ) RiskReleaseScratchSnapshot {
        self.mutex.lock();
        defer self.mutex.unlock();
        var owned_count: usize = 0;
        var done_count: usize = 0;
        var committed_count: usize = 0;
        for (
            self.prepared,
            self.preparation_done,
            self.committed,
        ) |prepared, done, committed| {
            if (prepared != null) owned_count += 1;
            if (done) done_count += 1;
            if (committed) committed_count += 1;
        }
        return .{
            .active = switch (self.risk_release_scratch) {
                .idle => null,
                .active => |key| key,
            },
            .prepared_count = self.prepared_count,
            .owned_count = owned_count,
            .done_count = done_count,
            .committed_count = committed_count,
        };
    }

    /// Test-only opaque identity; never exposes a dereferenceable staged pointer.
    fn stagedR2EventIdentity(
        self: *PersistentGroupedRuntime,
        sequence: u64,
    ) u64 {
        self.mutex.lock();
        defer self.mutex.unlock();
        const slot = self.slots.getSequence(sequence) orelse return 0;
        return if (slot.r2_events_head) |head| @intFromPtr(head) else 0;
    }

    pub fn riskWorkerId(
        self: *PersistentGroupedRuntime,
        shard_index: usize,
    ) ?std.Thread.Id {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.risk_contexts[shard_index].thread_id;
    }

    pub fn matchingWorkerId(
        self: *PersistentGroupedRuntime,
        shard_index: usize,
    ) ?std.Thread.Id {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.matching_contexts[shard_index].thread_id;
    }

    pub fn submitAtSequence(
        self: *PersistentGroupedRuntime,
        assigned_sequence: publication.PublicationSequence,
        command: *const cmd.OrderCommand,
    ) SubmitError!SlotHandle {
        self.noteStartCompetitor();
        if (!isOrdinaryCommand(command.command)) return error.UnsupportedCommand;

        self.mutex.lock();
        defer self.mutex.unlock();
        while (self.admission_frozen) {
            try self.ensureAcceptingLocked();
            self.condition.wait(&self.mutex);
        }
        try self.ensureAcceptingLocked();
        try self.ensurePublicationOwnershipLocked();

        const sequence = assigned_sequence.toInt();
        const expected = nextSequence(self.frontiers.accepted) catch
            return error.SequenceOverflow;
        if (sequence != expected) return error.NonContiguousPublicationSequence;
        try self.adapter.validatePublicationSequence(assigned_sequence);

        // Claim before a new group can release the previous group.
        const claimed = try self.slots.claim(sequence);
        claimed.slot.events_group = command.events_group;
        claimed.slot.command = command.*;
        self.adapter.consumePublicationSequence(assigned_sequence);
        self.frontiers.accepted = sequence;
        if (self.stage_gate) |gate| {
            if (gate.reached.load(.acquire) and !gate.release.load(.acquire)) {
                self.noteRetirementBlocked(gate.stage, sequence);
            }
        }
        self.condition.broadcast();
        return claimed.handle;
    }

    /// Atomically admits one contiguous publication range while retaining the
    /// runtime mutex. Every fallible command/sequence/slot check completes
    /// before the first slot or kernel publication frontier is mutated. This
    /// lets the API safely fall back to singleton admission when a speculative
    /// batch cannot be accepted as a whole.
    pub fn submitBatchAtSequence(
        self: *PersistentGroupedRuntime,
        submissions: []const Submission,
    ) SubmitError!void {
        self.noteStartCompetitor();
        if (submissions.len == 0) return;
        if (submissions.len > self.slots.capacity()) {
            return error.SlotNotReclaimed;
        }
        for (submissions) |submission| {
            if (!isOrdinaryCommand(submission.command.command)) {
                return error.UnsupportedCommand;
            }
        }

        self.mutex.lock();
        defer self.mutex.unlock();
        while (self.admission_frozen) {
            try self.ensureAcceptingLocked();
            self.condition.wait(&self.mutex);
        }
        try self.ensureAcceptingLocked();

        // A quiescent runtime can adopt a coherently advanced direct frontier,
        // but delay that mutation until every remaining batch check succeeds.
        const accepted_base = if (self.adapter.publicationFrontiersEqual(
            self.frontiers.accepted,
        ))
            self.frontiers.accepted
        else blk: {
            if (!self.isFullyQuiescentLocked()) {
                self.poisonLocked(
                    .publication_ownership,
                    self.frontiers.accepted,
                    null,
                    error.PublicationFrontierOwnershipLost,
                );
                return error.PublicationFrontierOwnershipLost;
            }
            const observed = self.adapter.settledFrontier();
            if (observed < self.frontiers.accepted or
                !self.adapter.publicationFrontiersEqual(observed))
            {
                self.poisonLocked(
                    .publication_ownership,
                    self.frontiers.accepted,
                    null,
                    error.PublicationFrontierOwnershipLost,
                );
                return error.PublicationFrontierOwnershipLost;
            }
            break :blk observed;
        };

        var expected = nextSequence(accepted_base) catch
            return error.SequenceOverflow;
        for (submissions, 0..) |submission, index| {
            const sequence = submission.sequence.toInt();
            if (sequence != expected) return error.NonContiguousPublicationSequence;
            try self.adapter.validatePublicationSequence(submission.sequence);
            if (!self.slots.canClaim(sequence)) return error.SlotNotReclaimed;
            if (index + 1 < submissions.len) {
                expected = nextSequence(expected) catch
                    return error.SequenceOverflow;
            }
        }

        if (accepted_base != self.frontiers.accepted) {
            self.frontiers.adoptSettled(accepted_base);
            self.r2_group_boundary_cache.clearRange();
        }
        for (submissions) |submission| {
            const sequence = submission.sequence.toInt();
            const claimed = self.slots.claim(sequence) catch unreachable;
            claimed.slot.events_group = submission.command.events_group;
            claimed.slot.command = submission.command.*;
        }
        // Workers cannot observe the newly claimed slots until this mutex is
        // released. Publish the contiguous range by advancing every canonical
        // kernel frontier once, to the already-validated final sequence.
        const final_submission = submissions[submissions.len - 1];
        self.adapter.consumePublicationSequence(final_submission.sequence);
        self.frontiers.accepted = final_submission.sequence.toInt();
        self.condition.broadcast();
    }

    pub fn acknowledgeJournal(
        self: *PersistentGroupedRuntime,
        sequence: u64,
    ) JournalError!void {
        self.mutex.lock();
        defer self.mutex.unlock();
        const slot = try self.validateJournalAckLocked(sequence);
        self.publishJournalAckLocked(slot, sequence);
    }

    /// Atomically publishes an integration's durable journal mark and the
    /// runtime's logical acknowledgement. A rejected durable mark poisons the
    /// sequence before result transfer can cross the journal frontier.
    pub fn acknowledgeDurableJournal(
        self: *PersistentGroupedRuntime,
        sequence: u64,
    ) DurableJournalError!void {
        self.mutex.lock();
        defer self.mutex.unlock();
        const slot = try self.validateJournalAckLocked(sequence);
        _ = self.settlement_hooks.markJournalCommitted(.fromInt(sequence)) catch |err| {
            self.poisonLocked(.journal, sequence, null, err);
            return err;
        };
        self.publishJournalAckLocked(slot, sequence);
    }

    fn validateJournalAckLocked(
        self: *PersistentGroupedRuntime,
        sequence: u64,
    ) JournalError!*PipelineSlot {
        if (self.state == .poisoned) {
            if (!self.isStrictPreFatalSequenceLocked(sequence)) {
                return error.PipelinePoisoned;
            }
        } else {
            try self.ensureUsableLocked();
        }
        if (self.frontiers.journal >= self.frontiers.accepted) {
            return error.NonContiguousJournalAck;
        }
        const expected = nextSequence(self.frontiers.journal) catch
            return error.NonContiguousJournalAck;
        if (sequence != expected) return error.NonContiguousJournalAck;
        const slot = self.slots.getSequence(sequence) orelse
            return error.UnknownSequence;
        return slot;
    }

    fn publishJournalAckLocked(
        self: *PersistentGroupedRuntime,
        slot: *PipelineSlot,
        sequence: u64,
    ) void {
        slot.journal_acked = true;
        self.frontiers.journal = sequence;
        self.condition.broadcast();
    }

    /// Reads journal metadata without transferring the matching response.
    pub fn matchedResultCode(
        self: *PersistentGroupedRuntime,
        sequence: u64,
    ) ResultMetadataError!cmd.CommandResultCode {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.state == .poisoned and
            !self.isStrictPreFatalSequenceLocked(sequence))
        {
            return error.PipelinePoisoned;
        }
        const slot = self.slots.getSequence(sequence) orelse
            return error.UnknownSequence;
        if (slot.phase != .matching_complete or
            minFrontier(self.frontiers.matching, self.frontiers.accepted) < sequence)
        {
            return error.ResultNotMatched;
        }
        return (slot.response orelse return error.ResultNotMatched).result_code;
    }

    pub fn takeNextResult(
        self: *PersistentGroupedRuntime,
    ) error{PipelinePoisoned}!?kernel_mod.CommandResponse {
        self.mutex.lock();
        if (self.frontiers.result >= self.frontiers.accepted or
            self.frontiers.result >= self.frontiers.journal)
        {
            if (self.state == .poisoned) {
                self.mutex.unlock();
                return error.PipelinePoisoned;
            }
            self.mutex.unlock();
            return null;
        }
        const sequence = nextSequence(self.frontiers.result) catch {
            self.mutex.unlock();
            return null;
        };
        if (self.state == .poisoned and
            !self.isStrictPreFatalSequenceLocked(sequence))
        {
            self.mutex.unlock();
            return error.PipelinePoisoned;
        }
        const slot = self.slots.getSequence(sequence) orelse unreachable;
        if (slot.phase != .matching_complete or
            minFrontier(self.frontiers.matching, self.frontiers.accepted) < sequence)
        {
            self.mutex.unlock();
            return null;
        }
        const response = slot.response orelse unreachable;
        slot.response = null;
        slot.result_transferred = true;
        self.frontiers.result = sequence;
        self.reclaimLocked();
        self.mutex.unlock();
        return response;
    }

    pub fn waitForMatching(
        self: *PersistentGroupedRuntime,
        sequence: u64,
    ) WaitError!void {
        self.mutex.lock();
        defer self.mutex.unlock();
        while (minFrontier(self.frontiers.matching, self.frontiers.accepted) < sequence) {
            try self.ensureWaitableLocked();
            self.condition.wait(&self.mutex);
        }
        if (self.state == .poisoned and
            !self.isStrictPreFatalSequenceLocked(sequence))
        {
            return error.PipelinePoisoned;
        }
    }

    pub fn waitForRiskRelease(
        self: *PersistentGroupedRuntime,
        sequence: u64,
    ) WaitError!void {
        self.mutex.lock();
        defer self.mutex.unlock();
        while (self.frontiers.minRiskR2() < sequence) {
            try self.ensureWaitableLocked();
            self.condition.wait(&self.mutex);
        }
    }

    /// Reusable terminal barrier. Admission remains frozen until the accepted
    /// target has matching, journal, result, R2, and reclaim completion.
    pub fn terminalQuiesce(
        self: *PersistentGroupedRuntime,
    ) WaitError!u64 {
        self.noteStartCompetitor();
        self.mutex.lock();
        while (self.quiescence_target != null) {
            self.ensureWaitableLocked() catch |err| {
                self.mutex.unlock();
                return err;
            };
            self.condition.wait(&self.mutex);
        }
        if (self.state != .running) {
            const failure: WaitError = if (self.state == .poisoned)
                error.PipelinePoisoned
            else
                error.PipelineStopped;
            self.mutex.unlock();
            return failure;
        }

        const target = self.frontiers.accepted;
        self.admission_frozen = true;
        self.quiescence_target = target;
        self.condition.broadcast();
        while (!self.isFullyQuiescentLocked()) {
            self.ensureWaitableLocked() catch |err| {
                self.admission_frozen = false;
                self.quiescence_target = null;
                self.condition.broadcast();
                self.mutex.unlock();
                return err;
            };
            self.condition.wait(&self.mutex);
        }
        self.r2_group_boundary_cache.clearRange();
        self.quiescence_target = null;
        self.admission_frozen = false;
        self.condition.broadcast();
        self.mutex.unlock();
        return target;
    }

    /// Closes admission, settles the terminal group, and joins every worker.
    pub fn shutdown(self: *PersistentGroupedRuntime) ShutdownError!void {
        self.noteStartCompetitor();
        if (self.isWorkerThread()) return error.ShutdownFromWorker;
        self.mutex.lock();
        if (self.state == .poisoned) {
            self.registerJoinLocked();
            self.mutex.unlock();
            self.finishRegisteredJoin();
            return error.PipelinePoisoned;
        }
        if (self.state != .running) {
            self.mutex.unlock();
            return error.PipelineNotRunning;
        }
        self.state = .closing;
        self.condition.broadcast();
        while (self.active_workers != 0 and self.state != .poisoned) {
            self.condition.wait(&self.mutex);
        }
        const poisoned = self.state == .poisoned;
        self.registerJoinLocked();
        self.mutex.unlock();
        self.finishRegisteredJoin();
        if (poisoned) return error.PipelinePoisoned;
        self.mutex.lock();
        self.r2_group_boundary_cache.clearRange();
        self.state = .stopped;
        self.condition.broadcast();
        self.mutex.unlock();
    }

    pub fn destroy(self: *PersistentGroupedRuntime) DestroyError!void {
        self.noteStartCompetitor();
        if (self.isWorkerThread()) return error.DestroyFromWorker;
        self.mutex.lock();
        if (self.destroying) {
            self.mutex.unlock();
            return error.RuntimeBeingDestroyed;
        }
        if (self.state == .starting or
            self.state == .running or
            self.state == .closing)
        {
            self.mutex.unlock();
            return error.RuntimeStillActive;
        }
        self.destroying = true;
        self.registerJoinLocked();
        self.mutex.unlock();
        self.finishRegisteredJoin();
        self.mutex.lock();
        while (self.join_callers != 0) self.condition.wait(&self.mutex);
        self.mutex.unlock();

        for (self.prepared) |*entry| {
            if (entry.*) |*prepared| prepared.deinit();
        }
        const allocator = self.storage_allocator;
        self.slots.deinit();
        self.frontiers.deinit();
        allocator.free(self.prepared);
        allocator.free(self.preparation_done);
        allocator.free(self.preparation_errors);
        allocator.free(self.committed);
        allocator.free(self.risk_contexts);
        allocator.free(self.matching_contexts);
        allocator.free(self.risk_threads);
        allocator.free(self.matching_threads);
        self.threaded.deinit();
        allocator.destroy(self);
    }

    fn workerMain(context: *WorkerContext) void {
        const self = context.runtime;
        self.mutex.lock();
        context.thread_id = std.Thread.getCurrentId();
        self.condition.broadcast();
        self.mutex.unlock();

        switch (context.kind) {
            .risk => self.riskWorkerLoop(context.shard_index),
            .matching => self.matchingWorkerLoop(context.shard_index),
        }

        self.cleanupPreparedForShard(context);
        self.mutex.lock();
        self.active_workers -= 1;
        self.condition.broadcast();
        self.mutex.unlock();
    }

    fn riskWorkerLoop(self: *PersistentGroupedRuntime, shard_index: usize) void {
        while (true) {
            self.mutex.lock();
            if (self.shouldRiskWorkerExitLocked(shard_index)) {
                self.mutex.unlock();
                return;
            }

            // Every Java risk processor retires the same ring sequence, but
            // ordinary R1 work has exactly one UID-owned shard. Advancing that
            // all-shard prefix through one stable coordinator preserves strict
            // fail-before-suffix ordering without a condition-variable
            // rendezvous between otherwise no-op workers on every command.
            if (shard_index == 0) {
                const r1_sequence = nextSequence(self.frontiers.risk_r1[0]) catch {
                    self.condition.wait(&self.mutex);
                    self.mutex.unlock();
                    continue;
                };
                if (self.canRunRiskR1Locked(r1_sequence)) {
                    self.runRiskR1CoordinatedLocked(r1_sequence);
                    self.mutex.unlock();
                    continue;
                }

                const r2_sequence = nextSequence(self.frontiers.risk_r2[0]) catch {
                    self.condition.wait(&self.mutex);
                    self.mutex.unlock();
                    continue;
                };
                if (self.canRunRiskR2Locked(r2_sequence)) {
                    self.runRiskR2CoordinatedLocked(r2_sequence);
                    self.mutex.unlock();
                    continue;
                }
            }

            self.condition.wait(&self.mutex);
            self.mutex.unlock();
        }
    }

    fn matchingWorkerLoop(self: *PersistentGroupedRuntime, shard_index: usize) void {
        while (true) {
            self.mutex.lock();
            if (self.shouldMatchingWorkerExitLocked(shard_index)) {
                self.mutex.unlock();
                return;
            }
            // Matching is likewise symbol-owned. One stable coordinator
            // performs the owner work and advances the no-op shard frontiers
            // atomically, avoiding an M-way wakeup convoy per sequence.
            if (shard_index == 0) {
                const sequence = nextSequence(self.frontiers.matching[0]) catch {
                    self.condition.wait(&self.mutex);
                    self.mutex.unlock();
                    continue;
                };
                if (sequence <= self.frontiers.accepted and
                    minFrontier(self.frontiers.risk_r1, self.frontiers.accepted) >= sequence)
                {
                    self.runMatchingCoordinatedLocked(sequence);
                    self.mutex.unlock();
                    continue;
                }
                self.noteRetirementBlocked(.matching, sequence);
            }

            self.condition.wait(&self.mutex);
            self.mutex.unlock();
        }
    }

    /// Called with the mutex held and returns with it held.
    fn runRiskR1CoordinatedLocked(
        self: *PersistentGroupedRuntime,
        sequence: u64,
    ) void {
        const slot = self.slots.getSequence(sequence) orelse unreachable;
        const owner = self.adapter.riskOwnerIndex(&slot.command);
        if (!self.checkOwnershipOrPoisonLocked(sequence, .risk_pre_process, owner)) return;
        var command = slot.command;
        self.mutex.unlock();
        if (!self.passStageGate(.risk_pre_process, sequence, owner)) {
            self.mutex.lock();
            self.poisonLocked(
                .risk_pre_process,
                sequence,
                owner,
                error.InjectedStageFailure,
            );
            return;
        }
        self.adapter.runRiskPreProcessForShard(owner, sequence, &command) catch |err| {
            self.mutex.lock();
            self.poisonLocked(.risk_pre_process, sequence, owner, err);
            return;
        };
        self.mutex.lock();
        if (self.state == .poisoned) return;
        slot.command = command;
        @memset(self.frontiers.risk_r1, sequence);
        self.condition.broadcast();
    }

    /// Called with the mutex held and returns with it held.
    fn runMatchingCoordinatedLocked(
        self: *PersistentGroupedRuntime,
        sequence: u64,
    ) void {
        const slot = self.slots.getSequence(sequence) orelse unreachable;
        const owner = self.adapter.matchingOwnerIndex(&slot.command);
        if (!self.checkOwnershipOrPoisonLocked(sequence, .matching, owner)) return;
        var command = slot.command;
        self.mutex.unlock();
        if (!self.passStageGate(.matching, sequence, owner)) {
            self.mutex.lock();
            self.poisonLocked(.matching, sequence, owner, error.InjectedStageFailure);
            return;
        }
        var output = self.adapter.runMatchingForShard(
            owner,
            sequence,
            &command,
        ) catch |err| {
            self.mutex.lock();
            self.poisonLocked(.matching, sequence, owner, err);
            return;
        };

        self.mutex.lock();
        if (self.state == .poisoned) {
            self.mutex.unlock();
            if (output) |*owned| owned.deinit(self.adapter.allocator());
            self.mutex.lock();
            return;
        }
        if (output) |*owned| {
            slot.command = command;
            slot.response = owned.response;
            owned.response.matcher_events_head = null;
            owned.response.l2_snapshot = null;
            owned.response.report_response = null;
            owned.response.java_report_response = null;
            slot.r2_events_head = owned.r2_events_head;
            owned.r2_events_head = null;
            slot.r2_top = owned.r2_top;
            slot.requires_risk_release = owned.requires_risk_release;
            slot.r2_release_kind = if (owned.requires_risk_release)
                self.adapter.riskReleaseKind(&slot.command)
            else
                .none;
            slot.phase = .matching_complete;
        }
        @memset(self.frontiers.matching, sequence);
        self.condition.broadcast();
    }

    /// Called with the mutex held and returns with it held. The coordinator
    /// prepares every risk shard before committing any of them, preserving
    /// failure atomicity while removing a per-command condition rendezvous.
    fn runRiskR2CoordinatedLocked(
        self: *PersistentGroupedRuntime,
        sequence: u64,
    ) void {
        if (!self.checkOwnershipOrPoisonLocked(sequence, .risk_release, 0)) return;
        const slot = self.slots.getSequence(sequence) orelse unreachable;
        if (!slot.requires_risk_release) {
            if (!self.riskReleaseScratchIsIdleLocked()) {
                self.poisonLocked(
                    .risk_release,
                    sequence,
                    0,
                    error.RiskReleaseScratchOverlap,
                );
                return;
            }
            @memset(self.frontiers.risk_r2, sequence);
            slot.r2_complete = true;
            if (!self.markSettlementRiskR2CommittedLocked(sequence)) return;
            self.reclaimLocked();
            self.condition.broadcast();
            return;
        }

        if (!self.activateRiskReleaseScratchLocked(slot, 0)) return;
        const kind = slot.r2_release_kind orelse unreachable;
        for (0..self.risk_contexts.len) |index| {
            var command = slot.command;
            const event_head = slot.r2_events_head;
            const top = slot.r2_top;
            self.mutex.unlock();
            if (!self.passStageGate(.risk_release, sequence, index)) {
                self.mutex.lock();
                self.poisonLocked(
                    .risk_release,
                    sequence,
                    index,
                    error.InjectedStageFailure,
                );
                return;
            }
            const prepared_result = self.adapter.prepareRiskReleaseForShard(
                index,
                kind,
                &command,
                event_head,
                top,
            );
            self.mutex.lock();
            if (self.state == .poisoned) {
                if (prepared_result) |value| {
                    var prepared = value;
                    self.mutex.unlock();
                    prepared.deinit();
                    self.mutex.lock();
                } else |_| {}
                return;
            }
            if (prepared_result) |prepared| {
                self.prepared[index] = prepared;
            } else |err| {
                self.preparation_errors[index] = err;
            }
            self.preparation_done[index] = true;
            self.prepared_count += 1;
        }

        for (self.preparation_errors, 0..) |failure, index| {
            if (failure) |err| {
                self.poisonLocked(.risk_release, sequence, index, err);
                return;
            }
        }
        if (!self.ensureSettlementPreparedLocked(slot)) return;

        for (0..self.risk_contexts.len) |index| {
            var prepared = self.prepared[index].?;
            self.prepared[index] = null;
            self.committed[index] = true;
            self.mutex.unlock();
            self.adapter.commitPreparedRiskReleaseForShard(index, &prepared);
            prepared.deinit();
            self.mutex.lock();
            if (self.state == .poisoned) return;
        }
        @memset(self.frontiers.risk_r2, sequence);
        slot.r2_complete = true;
        self.finishRiskReleaseScratchLocked(slot);
        if (!self.markSettlementRiskR2CommittedLocked(sequence)) return;
        self.reclaimLocked();
        self.condition.broadcast();
    }

    /// Called with the mutex held and returns with it held.
    fn runRiskR2Locked(
        self: *PersistentGroupedRuntime,
        shard_index: usize,
        sequence: u64,
    ) void {
        if (!self.checkOwnershipOrPoisonLocked(sequence, .risk_release, shard_index)) return;
        const slot = self.slots.getSequence(sequence) orelse unreachable;
        if (!slot.requires_risk_release) {
            if (!self.riskReleaseScratchIsIdleLocked()) {
                self.poisonLocked(
                    .risk_release,
                    sequence,
                    shard_index,
                    error.RiskReleaseScratchOverlap,
                );
                return;
            }
            self.frontiers.risk_r2[shard_index] = sequence;
            const group_complete = allEqual(self.frontiers.risk_r2, sequence);
            if (group_complete) {
                slot.r2_complete = true;
                if (!self.markSettlementRiskR2CommittedLocked(sequence)) return;
            }
            self.reclaimLocked();
            if (group_complete) {
                self.condition.broadcast();
            }
            return;
        }

        if (!self.activateRiskReleaseScratchLocked(slot, shard_index)) return;
        if (!self.preparation_done[shard_index]) {
            const kind = slot.r2_release_kind orelse unreachable;
            var command = slot.command;
            const event_head = slot.r2_events_head;
            const top = slot.r2_top;
            self.mutex.unlock();
            if (!self.passStageGate(.risk_release, sequence, shard_index)) {
                self.mutex.lock();
                self.poisonLocked(
                    .risk_release,
                    sequence,
                    shard_index,
                    error.InjectedStageFailure,
                );
                return;
            }
            const prepared_result = self.adapter.prepareRiskReleaseForShard(
                shard_index,
                kind,
                &command,
                event_head,
                top,
            );
            self.mutex.lock();
            if (self.state == .poisoned) {
                if (prepared_result) |value| {
                    var prepared = value;
                    self.mutex.unlock();
                    prepared.deinit();
                    self.mutex.lock();
                } else |_| {}
                return;
            }
            if (prepared_result) |prepared| {
                self.prepared[shard_index] = prepared;
            } else |err| {
                self.preparation_errors[shard_index] = err;
            }
            self.preparation_done[shard_index] = true;
            self.prepared_count += 1;
            if (self.prepared_count == self.risk_contexts.len) {
                self.condition.broadcast();
            }
        }

        if (self.prepared_count != self.risk_contexts.len) return;
        for (self.preparation_errors, 0..) |failure, index| {
            if (failure) |err| {
                self.poisonLocked(.risk_release, sequence, index, err);
                return;
            }
        }
        if (!self.ensureSettlementPreparedLocked(slot)) return;
        if (self.committed[shard_index]) return;

        var prepared = self.prepared[shard_index].?;
        self.prepared[shard_index] = null;
        self.committed[shard_index] = true;
        self.mutex.unlock();
        self.adapter.commitPreparedRiskReleaseForShard(shard_index, &prepared);
        prepared.deinit();
        self.mutex.lock();
        if (self.state == .poisoned) return;
        self.frontiers.risk_r2[shard_index] = sequence;
        if (allEqual(self.frontiers.risk_r2, sequence)) {
            slot.r2_complete = true;
            self.finishRiskReleaseScratchLocked(slot);
            if (!self.markSettlementRiskR2CommittedLocked(sequence)) return;
        }
        self.reclaimLocked();
        if (allEqual(self.frontiers.risk_r2, sequence)) {
            self.condition.broadcast();
        }
    }

    /// Called with the runtime mutex held and returns with it held. Only one
    /// risk worker may publish a durable settlement preparation for the active
    /// scratch sequence; other workers retry after the condition broadcast.
    fn ensureSettlementPreparedLocked(
        self: *PersistentGroupedRuntime,
        slot: *const PipelineSlot,
    ) bool {
        switch (self.settlement_prepare_state) {
            .done => return true,
            .preparing => return false,
            .idle => self.settlement_prepare_state = .preparing,
        }

        // Only spot/currency-exchange risk release maps to settlement
        // transfers. Margin and other economic release kinds still publish
        // the monotonic R2 committed mark after their internal commit.
        if (slot.r2_release_kind != .exchange or
            !self.settlement_hooks.isEnabled())
        {
            self.settlement_prepare_state = .done;
            return true;
        }
        const specification = self.adapter.canonicalSymbolSpecification(&slot.command) orelse {
            self.poisonLocked(
                .risk_release,
                slot.sequence,
                null,
                error.MissingCanonicalSymbolSpecification,
            );
            return false;
        };
        const sequence = slot.sequence;
        const batch: settlement_lifecycle.PreparedBatch = .{
            .sequence = .fromInt(sequence),
            .command = &slot.command,
            .specification = specification,
            .events_head = slot.r2_events_head,
            .mode = .live,
        };
        self.mutex.unlock();
        const observation = self.settlement_hooks.prepare(batch);
        self.mutex.lock();
        if (self.state == .poisoned) return false;
        _ = observation catch |err| {
            self.poisonLocked(.risk_release, sequence, null, err);
            return false;
        };
        self.settlement_prepare_state = .done;
        self.condition.broadcast();
        return true;
    }

    /// The internal frontier is updated before this callback. Rejection is
    /// therefore terminal evidence about an already irreversible R2 commit,
    /// never a reason to roll the truthful frontier back.
    fn markSettlementRiskR2CommittedLocked(
        self: *PersistentGroupedRuntime,
        sequence: u64,
    ) bool {
        _ = self.settlement_hooks.markRiskR2Committed(.fromInt(sequence)) catch |err| {
            self.poisonLocked(.risk_release, sequence, null, err);
            return false;
        };
        return true;
    }

    fn riskReleaseScratchIsIdleLocked(self: *const PersistentGroupedRuntime) bool {
        return switch (self.risk_release_scratch) {
            .idle => true,
            .active => false,
        };
    }

    fn activateRiskReleaseScratchLocked(
        self: *PersistentGroupedRuntime,
        slot: *const PipelineSlot,
        shard_index: usize,
    ) bool {
        const key: RiskReleaseScratchKey = .{
            .sequence = slot.sequence,
            .generation = slot.generation,
        };
        switch (self.risk_release_scratch) {
            .idle => {
                std.debug.assert(self.prepared_count == 0);
                std.debug.assert(self.settlement_prepare_state == .idle);
                std.debug.assert(self.prepared.len == self.risk_contexts.len);
                std.debug.assert(self.preparation_done.len == self.risk_contexts.len);
                std.debug.assert(self.preparation_errors.len == self.risk_contexts.len);
                std.debug.assert(self.committed.len == self.risk_contexts.len);
                for (
                    self.prepared,
                    self.preparation_done,
                    self.preparation_errors,
                    self.committed,
                ) |prepared, done, failure, committed| {
                    std.debug.assert(prepared == null);
                    std.debug.assert(!done);
                    std.debug.assert(failure == null);
                    std.debug.assert(!committed);
                }
                self.risk_release_scratch = .{ .active = key };
                return true;
            },
            .active => |active| {
                if (active.sequence == key.sequence and
                    active.generation == key.generation)
                {
                    return true;
                }
                self.poisonLocked(
                    .risk_release,
                    slot.sequence,
                    shard_index,
                    error.RiskReleaseScratchOverlap,
                );
                return false;
            },
        }
    }

    fn finishRiskReleaseScratchLocked(
        self: *PersistentGroupedRuntime,
        slot: *const PipelineSlot,
    ) void {
        const active = switch (self.risk_release_scratch) {
            .idle => unreachable,
            .active => |value| value,
        };
        std.debug.assert(active.sequence == slot.sequence);
        std.debug.assert(active.generation == slot.generation);
        std.debug.assert(self.prepared_count == self.risk_contexts.len);
        std.debug.assert(self.settlement_prepare_state == .done);
        for (
            self.prepared,
            self.preparation_done,
            self.preparation_errors,
            self.committed,
        ) |prepared, done, failure, committed| {
            std.debug.assert(prepared == null);
            std.debug.assert(done);
            std.debug.assert(failure == null);
            std.debug.assert(committed);
        }
        self.prepared_count = 0;
        @memset(self.preparation_done, false);
        @memset(self.preparation_errors, null);
        @memset(self.committed, false);
        self.settlement_prepare_state = .idle;
        self.risk_release_scratch = .idle;
    }

    fn canRunRiskR1Locked(self: *PersistentGroupedRuntime, sequence: u64) bool {
        if (sequence > self.frontiers.accepted) return false;
        if (sequence == 0) return true;
        const previous_sequence = sequence - 1;
        if (minFrontier(self.frontiers.risk_r1, self.frontiers.accepted) <
            previous_sequence)
        {
            self.noteRetirementBlocked(.risk_pre_process, sequence);
            return false;
        }
        if (previous_sequence <= self.frontiers.reclaimed) return true;
        const slot = self.slots.getSequence(sequence) orelse return false;
        const previous = self.slots.getSequence(previous_sequence) orelse return false;
        // Java TwoStepMasterProcessor keeps running R1 through the available
        // same-eventsGroup range after publishing only the prior group to
        // matching. Preserve that lookahead while retaining an all-risk-shard
        // R1 prefix above. A matching fatal can therefore leave same-group R1
        // tail mutations behind, but poison makes that kernel terminal.
        if (slot.events_group == previous.events_group) return true;
        if (minFrontier(self.frontiers.matching, self.frontiers.accepted) <
            previous_sequence)
        {
            self.noteRetirementBlocked(.matching, sequence);
            return false;
        }
        return self.frontiers.minRiskR2() >= previous_sequence;
    }

    fn canRunRiskR2Locked(self: *PersistentGroupedRuntime, sequence: u64) bool {
        if (sequence > self.frontiers.accepted) return false;
        if (sequence != 0 and self.frontiers.minRiskR2() < sequence - 1) {
            self.noteRetirementBlocked(.risk_release, sequence);
            return false;
        }
        if (minFrontier(self.frontiers.matching, self.frontiers.accepted) < sequence) {
            return false;
        }
        const slot = self.slots.getSequence(sequence) orelse return false;
        const boundary = self.closedGroupBoundaryLocked(sequence, slot.events_group) orelse
            return false;
        return minFrontier(self.frontiers.risk_r1, self.frontiers.accepted) >= boundary;
    }

    fn closedGroupBoundaryLocked(
        self: *PersistentGroupedRuntime,
        sequence: u64,
        group: i64,
    ) ?u64 {
        const terminal_boundary = if (self.state == .closing)
            self.frontiers.accepted
        else
            self.quiescence_target;
        return self.r2_group_boundary_cache.closedBoundary(
            &self.slots,
            sequence,
            group,
            self.frontiers.accepted,
            terminal_boundary,
        );
    }

    fn shouldRiskWorkerExitLocked(
        self: *PersistentGroupedRuntime,
        shard_index: usize,
    ) bool {
        if (self.state == .poisoned) return true;
        return self.state == .closing and
            self.frontiers.risk_r1[shard_index] == self.frontiers.accepted and
            self.frontiers.risk_r2[shard_index] == self.frontiers.accepted;
    }

    fn shouldMatchingWorkerExitLocked(
        self: *PersistentGroupedRuntime,
        shard_index: usize,
    ) bool {
        if (self.state == .poisoned) return true;
        return self.state == .closing and
            self.frontiers.matching[shard_index] == self.frontiers.accepted;
    }

    fn cleanupPreparedForShard(
        self: *PersistentGroupedRuntime,
        context: *WorkerContext,
    ) void {
        if (context.kind != .risk) return;
        self.mutex.lock();
        const entry = self.prepared[context.shard_index];
        self.prepared[context.shard_index] = null;
        self.mutex.unlock();
        if (entry) |value| {
            var prepared = value;
            prepared.deinit();
        }
    }

    /// Reclaim payload destruction deliberately drops the runtime mutex.
    fn reclaimLocked(self: *PersistentGroupedRuntime) void {
        while (self.frontiers.reclaimed < self.frontiers.accepted) {
            const sequence = nextSequence(self.frontiers.reclaimed) catch return;
            const slot = self.slots.getSequence(sequence) orelse return;
            if (!slot.result_transferred or !slot.r2_complete) return;
            std.debug.assert(slot.response == null);
            const event_head = slot.r2_events_head;
            slot.r2_events_head = null;
            slot.phase = .reclaiming;
            self.mutex.unlock();
            events.deinitMatcherEventChain(self.adapter.allocator(), event_head);
            self.mutex.lock();
            const generation = slot.generation;
            slot.* = .{ .generation = generation };
            self.frontiers.reclaimed = sequence;
            // Reclaim is normally driven by the result consumer, which has no
            // runtime waiter to wake. A terminal quiescence caller is the only
            // steady-state waiter whose predicate includes this frontier.
            if (self.quiescence_target != null) self.condition.broadcast();
        }
    }

    fn ensureAcceptingLocked(self: *PersistentGroupedRuntime) SubmitError!void {
        return switch (self.state) {
            .running => {},
            .poisoned => error.PipelinePoisoned,
            else => error.PipelineNotRunning,
        };
    }

    fn ensureUsableLocked(self: *PersistentGroupedRuntime) JournalError!void {
        return switch (self.state) {
            .running, .closing => {},
            .poisoned => error.PipelinePoisoned,
            else => error.PipelineNotRunning,
        };
    }

    fn ensureWaitableLocked(self: *PersistentGroupedRuntime) WaitError!void {
        return switch (self.state) {
            .running, .closing => {},
            .poisoned => error.PipelinePoisoned,
            .cold, .starting, .stopped => error.PipelineStopped,
        };
    }

    /// Matching retirement is prefix ordered: a fatal at F cannot begin until
    /// every earlier matching sequence has retired. Those already-matched
    /// commands may therefore finish their FIFO journal/result handoff after
    /// poison, preserving the outcomes they would have published before F
    /// under singleton admission. Java-compatible same-group R1 lookahead may
    /// already have mutated a suffix beyond F, but the poisoned runtime is
    /// terminal and never exposes a matching result for F or its suffix.
    fn isStrictPreFatalSequenceLocked(
        self: *const PersistentGroupedRuntime,
        sequence: u64,
    ) bool {
        const fatal = self.fatal_record orelse return false;
        return sequence < fatal.sequence;
    }

    fn noteStartCompetitor(self: *PersistentGroupedRuntime) void {
        const gate = self.start_gate orelse return;
        if (gate.reached.load(.acquire) and !gate.release.load(.acquire)) {
            _ = gate.competing_calls.fetchAdd(1, .acq_rel);
        }
    }

    /// Called without the runtime mutex so the other shard workers can retire
    /// the gated sequence and reach the global sequence barrier.
    fn passStageGate(
        self: *PersistentGroupedRuntime,
        stage: FailureStage,
        sequence: u64,
        shard_index: usize,
    ) bool {
        const gate = self.stage_gate orelse return true;
        if (gate.stage != stage or
            gate.sequence != sequence or
            gate.shard_index != shard_index)
        {
            return true;
        }
        gate.reached.store(true, .release);
        while (!gate.release.load(.acquire)) std.Thread.yield() catch {};
        return !gate.fail;
    }

    fn noteRetirementBlocked(
        self: *PersistentGroupedRuntime,
        stage: FailureStage,
        sequence: u64,
    ) void {
        const gate = self.stage_gate orelse return;
        if (gate.stage != stage or gate.sequence == std.math.maxInt(u64)) return;
        if (sequence == gate.sequence + 1) {
            gate.next_sequence_blocked.store(true, .release);
        }
    }

    fn ensurePublicationOwnershipLocked(
        self: *PersistentGroupedRuntime,
    ) error{PublicationFrontierOwnershipLost}!void {
        if (self.adapter.publicationFrontiersEqual(self.frontiers.accepted)) return;
        if (self.isFullyQuiescentLocked()) {
            const observed = self.adapter.settledFrontier();
            if (observed >= self.frontiers.accepted and
                self.adapter.publicationFrontiersEqual(observed))
            {
                self.frontiers.adoptSettled(observed);
                self.r2_group_boundary_cache.clearRange();
                return;
            }
        }
        self.poisonLocked(
            .publication_ownership,
            self.frontiers.accepted,
            null,
            error.PublicationFrontierOwnershipLost,
        );
        return error.PublicationFrontierOwnershipLost;
    }

    fn checkOwnershipOrPoisonLocked(
        self: *PersistentGroupedRuntime,
        sequence: u64,
        stage: FailureStage,
        shard_index: usize,
    ) bool {
        if (self.adapter.publicationFrontiersEqual(self.frontiers.accepted)) return true;
        self.poisonLocked(
            .publication_ownership,
            sequence,
            shard_index,
            error.PublicationFrontierOwnershipLost,
        );
        _ = stage;
        return false;
    }

    fn isFullyQuiescentLocked(self: *PersistentGroupedRuntime) bool {
        if (!self.frontiers.allSettledAtAccepted()) return false;
        for (self.slots.slots) |slot| {
            if (slot.phase != .free) return false;
        }
        return true;
    }

    fn poisonLocked(
        self: *PersistentGroupedRuntime,
        stage: FailureStage,
        sequence: u64,
        shard_index: ?usize,
        cause: anyerror,
    ) void {
        if (self.fatal_record == null) {
            self.fatal_record = .{
                .stage = stage,
                .sequence = sequence,
                .shard_index = shard_index,
                .command = if (self.slots.getSequence(sequence)) |slot|
                    slot.command.command
                else
                    null,
                .cause = cause,
            };
        }
        self.r2_group_boundary_cache.clearRange();
        self.state = .poisoned;
        self.condition.broadcast();
    }

    /// Called with the mutex held and returns with it unlocked.
    fn rollbackStartLocked(self: *PersistentGroupedRuntime) void {
        self.state = .closing;
        self.registerJoinLocked();
        self.condition.broadcast();
        self.mutex.unlock();
        self.finishRegisteredJoin();
        self.mutex.lock();
        self.r2_group_boundary_cache.clearRange();
        self.state = .cold;
        for (self.risk_contexts) |*context| context.thread_id = null;
        for (self.matching_contexts) |*context| context.thread_id = null;
        self.condition.broadcast();
        self.mutex.unlock();
    }

    fn registerJoinLocked(self: *PersistentGroupedRuntime) void {
        self.join_callers += 1;
    }

    /// Completes one join registration. Exactly one caller joins handles;
    /// competitors wait until the handles and arrays are fully retired.
    fn finishRegisteredJoin(self: *PersistentGroupedRuntime) void {
        self.mutex.lock();
        var perform_join = false;
        while (true) {
            switch (self.join_state) {
                .joined => break,
                .joinable => {
                    self.join_state = .joining;
                    perform_join = true;
                    break;
                },
                .joining => {
                    if (self.join_gate) |gate| {
                        gate.competing_waiter_registered.store(true, .release);
                    }
                    self.condition.wait(&self.mutex);
                },
            }
        }
        if (perform_join) {
            self.mutex.unlock();
            if (self.join_gate) |gate| {
                gate.reached.store(true, .release);
                while (!gate.release.load(.acquire)) std.Thread.yield() catch {};
            }
            for (self.risk_threads) |*thread| {
                if (thread.*) |worker| worker.join();
                thread.* = null;
            }
            for (self.matching_threads) |*thread| {
                if (thread.*) |worker| worker.join();
                thread.* = null;
            }
            self.mutex.lock();
            self.join_state = .joined;
            self.condition.broadcast();
        }
        std.debug.assert(self.join_callers > 0);
        self.join_callers -= 1;
        self.condition.broadcast();
        self.mutex.unlock();
    }

    fn isWorkerThread(self: *PersistentGroupedRuntime) bool {
        const current = std.Thread.getCurrentId();
        self.mutex.lock();
        defer self.mutex.unlock();
        for (self.risk_contexts) |context| {
            if (context.thread_id == current) return true;
        }
        for (self.matching_contexts) |context| {
            if (context.thread_id == current) return true;
        }
        return false;
    }

    fn allWorkerIdsRegisteredLocked(self: *PersistentGroupedRuntime) bool {
        for (self.risk_contexts) |context| {
            if (context.thread_id == null) return false;
        }
        for (self.matching_contexts) |context| {
            if (context.thread_id == null) return false;
        }
        return true;
    }
};

pub fn isOrdinaryCommand(command_type: cmd.OrderCommandType) bool {
    return switch (command_type) {
        .place_order,
        .cancel_order,
        .move_order,
        .reduce_order,
        .order_book_request,
        .add_user,
        .balance_adjustment,
        .suspend_user,
        .resume_user,
        .grouping_control,
        .nop,
        => true,
        .binary_data_query,
        .binary_data_command,
        .persist_state_matching,
        .persist_state_risk,
        .reset,
        .shutdown_signal,
        .reserved_compressed,
        => false,
    };
}

fn minFrontier(frontiers: []const u64, ceiling: u64) u64 {
    var minimum = ceiling;
    for (frontiers) |frontier| minimum = @min(minimum, frontier);
    return minimum;
}

fn allEqual(frontiers: []const u64, expected: u64) bool {
    for (frontiers) |frontier| {
        if (frontier != expected) return false;
    }
    return true;
}

fn persistentCreateAllocationFailureCampaign(
    allocator: std.mem.Allocator,
    kernels: *kernel_mod.MultiShardKernelSet,
) !void {
    const runtime = try PersistentGroupedRuntime.create(
        allocator,
        kernels,
        .{ .capacity = 8, .payload_allocator_is_thread_safe = true },
    );
    try runtime.destroy();
}

const RuntimeStartRunner = struct {
    runtime: *PersistentGroupedRuntime,
    failure: ?anyerror = null,

    fn run(self: *@This()) void {
        self.runtime.start() catch |err| {
            self.failure = err;
        };
    }
};

const RuntimeSubmitRunner = struct {
    runtime: *PersistentGroupedRuntime,
    sequence: u64,
    command: cmd.OrderCommand,
    failure: ?anyerror = null,

    fn run(self: *@This()) void {
        _ = self.runtime.submitAtSequence(
            .fromInt(self.sequence),
            &self.command,
        ) catch |err| {
            self.failure = err;
            return;
        };
    }
};

const RuntimeShutdownRunner = struct {
    runtime: *PersistentGroupedRuntime,
    failure: ?anyerror = null,

    fn run(self: *@This()) void {
        self.runtime.shutdown() catch |err| {
            self.failure = err;
        };
    }
};

const RuntimeDestroyRunner = struct {
    runtime: *PersistentGroupedRuntime,
    failure: ?anyerror = null,

    fn run(self: *@This()) void {
        self.runtime.destroy() catch |err| {
            self.failure = err;
        };
    }
};

fn nextSequence(frontier: u64) error{SequenceOverflow}!u64 {
    return std.math.add(u64, frontier, 1) catch error.SequenceOverflow;
}

fn makeNop(group: i64) cmd.OrderCommand {
    return .{
        .command = .nop,
        .result_code = .new,
        .events_group = group,
    };
}

fn makeAddUser(uid: i64) cmd.OrderCommand {
    return .{
        .command = .add_user,
        .uid = uid,
        .result_code = .new,
    };
}

fn makeBalanceAdjustment(
    uid: i64,
    currency: i32,
    amount: i64,
    transaction_id: i64,
) cmd.OrderCommand {
    return .{
        .command = .balance_adjustment,
        .uid = uid,
        .symbol = currency,
        .price = amount,
        .order_id = transaction_id,
        .order_type = .gtc,
        .result_code = .new,
    };
}

fn makeSpotSpec(symbol: i32) !common.CoreSymbolSpecification {
    return common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 100,
        .quote_scale_k = 10,
        .taker_fee = 2,
        .maker_fee = 1,
    });
}

fn executeImmediate(
    kernels: *kernel_mod.MultiShardKernelSet,
    command: cmd.OrderCommand,
    expected: cmd.CommandResultCode,
) !void {
    var mutable = command;
    var response = try kernels.executeRouted(&mutable);
    defer response.deinit(std.testing.allocator);
    try std.testing.expectEqual(expected, response.result_code);
}

const SpotFixture = struct {
    symbol: i32 = 241,
    maker_uid: i64 = 101,
    taker_uid: i64 = 100,
    maker_order_id: i64 = 10_001,

    fn setup(self: SpotFixture, kernels: *kernel_mod.MultiShardKernelSet) !void {
        try kernels.addSymbolAll(try makeSpotSpec(self.symbol));
        try executeImmediate(kernels, makeAddUser(self.maker_uid), .success);
        try executeImmediate(kernels, makeAddUser(self.taker_uid), .success);
        try executeImmediate(
            kernels,
            makeBalanceAdjustment(self.maker_uid, 11, 100_000, 1),
            .success,
        );
        try executeImmediate(
            kernels,
            makeBalanceAdjustment(self.taker_uid, 15, 100_000, 1),
            .success,
        );
        var maker_ask = cmd.OrderCommand.newOrder(
            .gtc,
            self.maker_order_id,
            self.maker_uid,
            100,
            0,
            4,
            .ask,
        );
        maker_ask.symbol = self.symbol;
        try executeImmediate(kernels, maker_ask, .success);
    }

    fn takerBid(self: SpotFixture, order_id: i64, group: i64) cmd.OrderCommand {
        var command = cmd.OrderCommand.newOrder(
            .ioc,
            order_id,
            self.taker_uid,
            100,
            100,
            2,
            .bid,
        );
        command.symbol = self.symbol;
        command.events_group = group;
        return command;
    }

    fn makerAsk(self: SpotFixture, order_id: i64, size: i64) cmd.OrderCommand {
        var command = cmd.OrderCommand.newOrder(
            .gtc,
            order_id,
            self.maker_uid,
            100,
            0,
            size,
            .ask,
        );
        command.symbol = self.symbol;
        return command;
    }
};

const SettlementLifecycleTestRecorder = struct {
    prepare_calls: std.atomic.Value(usize) = .init(0),
    risk_calls: std.atomic.Value(usize) = .init(0),
    journal_calls: std.atomic.Value(usize) = .init(0),
    last_prepare_sequence: std.atomic.Value(u64) = .init(0),
    last_risk_sequence: std.atomic.Value(u64) = .init(0),
    last_journal_sequence: std.atomic.Value(u64) = .init(0),
    last_prepare_symbol: std.atomic.Value(i32) = .init(0),
    last_prepare_trade_count: std.atomic.Value(usize) = .init(0),
    last_prepare_mode: std.atomic.Value(u8) = .init(0),
    block_prepare: bool = false,
    reject_prepare: bool = false,
    reject_risk: bool = false,
    reject_journal: bool = false,
    prepare_reached: std.atomic.Value(bool) = .init(false),
    prepare_release: std.atomic.Value(bool) = .init(false),

    fn hooks(self: *SettlementLifecycleTestRecorder) settlement_lifecycle.Hooks {
        return .enabled(self, &.{
            .prepare = prepare,
            .mark_risk_r2_committed = markRiskR2Committed,
            .mark_journal_committed = markJournalCommitted,
            .finish_recovery = finishRecovery,
        });
    }

    fn waitForPrepare(self: *SettlementLifecycleTestRecorder) void {
        while (!self.prepare_reached.load(.acquire)) {
            std.Thread.yield() catch {};
        }
    }

    fn releasePrepare(self: *SettlementLifecycleTestRecorder) void {
        self.prepare_release.store(true, .release);
    }

    fn prepare(
        context: *anyopaque,
        batch: settlement_lifecycle.PreparedBatch,
    ) settlement_lifecycle.PrepareObservation {
        const self: *SettlementLifecycleTestRecorder = @ptrCast(@alignCast(context));
        _ = self.prepare_calls.fetchAdd(1, .acq_rel);
        self.last_prepare_sequence.store(batch.sequence.toInt(), .release);
        self.last_prepare_symbol.store(batch.specification.symbol_id, .release);
        self.last_prepare_trade_count.store(batch.tradeCount(), .release);
        self.last_prepare_mode.store(@intFromEnum(batch.mode), .release);
        self.prepare_reached.store(true, .release);
        if (self.block_prepare) {
            while (!self.prepare_release.load(.acquire)) {
                std.Thread.yield() catch {};
            }
        }
        return if (self.reject_prepare) .rejected else .prepared;
    }

    fn markRiskR2Committed(
        context: *anyopaque,
        sequence: publication.PublicationSequence,
    ) settlement_lifecycle.MarkObservation {
        const self: *SettlementLifecycleTestRecorder = @ptrCast(@alignCast(context));
        _ = self.risk_calls.fetchAdd(1, .acq_rel);
        self.last_risk_sequence.store(sequence.toInt(), .release);
        return if (self.reject_risk) .rejected else .marked;
    }

    fn markJournalCommitted(
        context: *anyopaque,
        sequence: publication.PublicationSequence,
    ) settlement_lifecycle.MarkObservation {
        const self: *SettlementLifecycleTestRecorder = @ptrCast(@alignCast(context));
        _ = self.journal_calls.fetchAdd(1, .acq_rel);
        self.last_journal_sequence.store(sequence.toInt(), .release);
        return if (self.reject_journal) .rejected else .marked;
    }

    fn finishRecovery(
        _: *anyopaque,
        _: publication.PublicationSequence,
    ) settlement_lifecycle.RecoveryObservation {
        return .not_required;
    }
};

const MarginFixture = struct {
    symbol: i32 = 5991,
    maker_uid: i64 = 201,
    taker_uid: i64 = 200,

    fn setup(self: MarginFixture, kernels: *kernel_mod.MultiShardKernelSet) !void {
        try kernels.addSymbolAll(try common.CoreSymbolSpecification.init(.{
            .symbol_id = self.symbol,
            .symbol_type = .futures_contract,
            .base_currency = 840,
            .quote_currency = 392,
            .base_scale_k = 1,
            .quote_scale_k = 1,
            .taker_fee = 0,
            .maker_fee = 0,
            .margin_buy = 10,
            .margin_sell = 10,
        }));
        try executeImmediate(kernels, makeAddUser(self.maker_uid), .success);
        try executeImmediate(kernels, makeAddUser(self.taker_uid), .success);
        try executeImmediate(
            kernels,
            makeBalanceAdjustment(self.maker_uid, 392, 1_000, 1),
            .success,
        );
        try executeImmediate(
            kernels,
            makeBalanceAdjustment(self.taker_uid, 392, 1_000, 1),
            .success,
        );
        var maker = cmd.OrderCommand.newOrder(
            .gtc,
            20_001,
            self.maker_uid,
            100,
            0,
            2,
            .ask,
        );
        maker.symbol = self.symbol;
        try executeImmediate(kernels, maker, .success);
    }

    fn takerBid(self: MarginFixture, group: i64) cmd.OrderCommand {
        var command = cmd.OrderCommand.newOrder(
            .ioc,
            20_002,
            self.taker_uid,
            100,
            100,
            2,
            .bid,
        );
        command.symbol = self.symbol;
        command.events_group = group;
        return command;
    }
};

test "stable slot ring wraps with stable addresses and new generations" {
    var ring = try StableSlotRing.init(
        std.testing.allocator,
        std.testing.allocator,
        2,
    );
    defer ring.deinit();

    const first = try ring.claim(1);
    const first_address = first.slot;
    const first_generation = first.handle.generation;
    ring.release(first.handle);

    const wrapped = try ring.claim(3);
    try std.testing.expect(first_address == wrapped.slot);
    try std.testing.expect(wrapped.handle.generation != first_generation);
    try std.testing.expect(ring.get(first.handle) == null);
    ring.release(wrapped.handle);
}

test "R2 group boundary cache discovers each closed range in linear probes" {
    const group_sizes = [_]usize{ 1, 16, 256, 4096 };
    for (group_sizes) |group_size| {
        const slot_count = try std.math.ceilPowerOfTwo(usize, group_size + 1);
        var ring = try StableSlotRing.init(
            std.testing.allocator,
            std.testing.allocator,
            slot_count,
        );
        defer ring.deinit();

        const group = std.math.maxInt(i64);
        for (1..group_size + 1) |sequence| {
            const claimed = try ring.claim(@intCast(sequence));
            claimed.slot.events_group = group;
        }
        const sentinel_sequence: u64 = @intCast(group_size + 1);
        const sentinel = try ring.claim(sentinel_sequence);
        sentinel.slot.events_group = std.math.minInt(i64);

        var cache: R2GroupBoundaryCache = .{};
        const boundary = cache.closedBoundary(
            &ring,
            1,
            group,
            sentinel_sequence,
            null,
        );
        try std.testing.expectEqual(@as(?u64, @intCast(group_size)), boundary);

        for (1..group_size + 1) |sequence| {
            try std.testing.expectEqual(
                @as(?u64, @intCast(group_size)),
                cache.closedBoundary(
                    &ring,
                    @intCast(sequence),
                    group,
                    sentinel_sequence,
                    null,
                ),
            );
        }
        try std.testing.expectEqual(@as(u64, @intCast(group_size)), cache.probeCount());
    }
}

test "R2 group boundary cache resumes open range and closes terminal range" {
    var ring = try StableSlotRing.init(
        std.testing.allocator,
        std.testing.allocator,
        4,
    );
    defer ring.deinit();

    const wrapped_group = std.math.maxInt(i64);
    const first = try ring.claim(1);
    first.slot.events_group = wrapped_group;

    var cache: R2GroupBoundaryCache = .{};
    try std.testing.expectEqual(
        @as(?u64, null),
        cache.closedBoundary(&ring, 1, wrapped_group, 1, null),
    );
    try std.testing.expectEqual(@as(u64, 0), cache.probeCount());

    const second = try ring.claim(2);
    second.slot.events_group = wrapped_group;
    try std.testing.expectEqual(
        @as(?u64, null),
        cache.closedBoundary(&ring, 1, wrapped_group, 2, null),
    );
    try std.testing.expectEqual(@as(u64, 1), cache.probeCount());

    try std.testing.expectEqual(
        @as(?u64, null),
        cache.closedBoundary(&ring, 1, wrapped_group, 2, null),
    );
    try std.testing.expectEqual(@as(u64, 1), cache.probeCount());

    const third = try ring.claim(3);
    third.slot.events_group = std.math.minInt(i64);
    try std.testing.expectEqual(
        @as(?u64, 2),
        cache.closedBoundary(&ring, 1, wrapped_group, 3, null),
    );
    try std.testing.expectEqual(
        @as(?u64, 2),
        cache.closedBoundary(&ring, 2, wrapped_group, 3, null),
    );
    try std.testing.expectEqual(@as(u64, 2), cache.probeCount());

    try std.testing.expectEqual(
        @as(?u64, 3),
        cache.closedBoundary(&ring, 3, std.math.minInt(i64), 3, 3),
    );
    try std.testing.expectEqual(@as(u64, 2), cache.probeCount());
}

test "pipeline frees staged payloads with kernel allocator not ring storage allocator" {
    var storage_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer storage_arena.deinit();

    const fixture: SpotFixture = .{};
    var kernels = try kernel_mod.MultiShardKernelSet.init(
        std.testing.allocator,
        2,
        .direct,
    );
    defer kernels.deinit();
    try fixture.setup(&kernels);

    var pipeline = try SerialGroupedPipeline.init(
        storage_arena.allocator(),
        &kernels,
        2,
    );
    defer pipeline.deinit();

    const sequence = kernels.next_seq + 1;
    var trade = fixture.takerBid(10_000, 1);
    _ = try pipeline.submitAtSequence(.fromInt(sequence), &trade);
    const slot = pipeline.slots.getSequence(sequence).?;
    try std.testing.expect(slot.response.?.matcher_events_head != null);
    try std.testing.expect(slot.r2_events_head != null);

    // Deliberately leave both event chains staged. Pipeline teardown must use
    // the kernel allocator even though ring/frontier storage came from arena.
}

test "serial grouped result waits for journal and reclaims only after r2" {
    var kernels = try kernel_mod.MultiShardKernelSet.init(
        std.testing.allocator,
        1,
        .direct,
    );
    defer kernels.deinit();
    var pipeline = try SerialGroupedPipeline.init(
        std.testing.allocator,
        &kernels,
        4,
    );
    defer pipeline.deinit();

    var command = makeNop(7);
    const handle = try pipeline.submitAtSequence(.fromInt(1), &command);
    try std.testing.expect((try pipeline.takeNextResult()) == null);
    try pipeline.acknowledgeJournal(1);
    var response = (try pipeline.takeNextResult()).?;
    response.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u64, 1), pipeline.frontiers.result);
    try std.testing.expectEqual(@as(u64, 0), pipeline.frontiers.reclaimed);
    try std.testing.expect(pipeline.slots.get(handle) != null);

    try pipeline.observeIdle();
    try std.testing.expectEqual(@as(u64, 0), pipeline.frontiers.reclaimed);
    try pipeline.flushTerminal();
    try std.testing.expectEqual(@as(u64, 1), pipeline.frontiers.reclaimed);
    try std.testing.expect(pipeline.slots.get(handle) == null);
}

test "serial grouped admission rejection cannot settle prior financial state" {
    const fixture: SpotFixture = .{};
    var kernels = try kernel_mod.MultiShardKernelSet.init(
        std.testing.allocator,
        2,
        .direct,
    );
    defer kernels.deinit();
    try fixture.setup(&kernels);

    const settled = kernels.next_seq;
    var pipeline = try SerialGroupedPipeline.init(
        std.testing.allocator,
        &kernels,
        1,
    );
    defer pipeline.deinit();

    var trade = fixture.takerBid(10_010, 10);
    _ = try pipeline.submitAtSequence(.fromInt(settled + 1), &trade);
    try std.testing.expectEqual(
        @as(?i64, 0),
        try kernels.queryUserBalance(fixture.maker_uid, 15),
    );

    var before: [2]u64 = undefined;
    for (&before, 0..) |*hash, index| {
        hash.* = try kernels.riskEngineAt(index).stateHash();
    }

    var gap = makeNop(11);
    try std.testing.expectError(
        error.NonContiguousPublicationSequence,
        pipeline.submitAtSequence(.fromInt(settled + 3), &gap),
    );

    var full = makeNop(11);
    try std.testing.expectError(
        error.SlotNotReclaimed,
        pipeline.submitAtSequence(.fromInt(settled + 2), &full),
    );

    try std.testing.expectEqual(
        @as(?i64, 0),
        try kernels.queryUserBalance(fixture.maker_uid, 15),
    );
    for (before, 0..) |hash, index| {
        try std.testing.expectEqual(hash, try kernels.riskEngineAt(index).stateHash());
    }
    try std.testing.expectEqual(settled, pipeline.frontiers.minRiskR2());
    try std.testing.expect(pipeline.fatalInfo() == null);
}

test "serial grouped transitions use inequality including decrease and wrap" {
    var kernels = try kernel_mod.MultiShardKernelSet.init(
        std.testing.allocator,
        1,
        .direct,
    );
    defer kernels.deinit();
    var pipeline = try SerialGroupedPipeline.init(
        std.testing.allocator,
        &kernels,
        8,
    );
    defer pipeline.deinit();

    const groups = [_]i64{ 10, 9, std.math.maxInt(i64), std.math.minInt(i64) };
    for (groups, 0..) |group, index| {
        var command = makeNop(group);
        _ = try pipeline.submitAtSequence(.fromInt(index + 1), &command);
        try pipeline.acknowledgeJournal(index + 1);
        var response = (try pipeline.takeNextResult()).?;
        response.deinit(std.testing.allocator);
    }
    try pipeline.flushTerminal();
    try std.testing.expectEqual(@as(u64, groups.len), pipeline.frontiers.reclaimed);
}

test "pending grouped work rejects mixed immediate execution without stale r2" {
    const fixture: SpotFixture = .{};
    var kernels = try kernel_mod.MultiShardKernelSet.init(
        std.testing.allocator,
        2,
        .direct,
    );
    defer kernels.deinit();
    try fixture.setup(&kernels);

    const settled = kernels.next_seq;
    var pipeline = try SerialGroupedPipeline.init(
        std.testing.allocator,
        &kernels,
        4,
    );
    defer pipeline.deinit();

    var trade = fixture.takerBid(10_020, 20);
    _ = try pipeline.submitAtSequence(.fromInt(settled + 1), &trade);

    try executeImmediate(&kernels, makeNop(0), .success);
    try std.testing.expectError(
        error.PublicationFrontierOwnershipLost,
        pipeline.flushTerminal(),
    );
    const fatal = pipeline.fatalInfo() orelse return error.MissingFatalRecord;
    try std.testing.expectEqual(FailureStage.publication_ownership, fatal.stage);
    try std.testing.expectEqual(settled + 1, fatal.sequence);
    try std.testing.expectEqual(
        error.PublicationFrontierOwnershipLost,
        fatal.cause,
    );
    try std.testing.expectEqual(
        @as(?i64, 0),
        try kernels.queryUserBalance(fixture.maker_uid, 15),
    );

    try std.testing.expectError(error.PipelinePoisoned, pipeline.flushTerminal());
    try std.testing.expectEqual(
        @as(?i64, 0),
        try kernels.queryUserBalance(fixture.maker_uid, 15),
    );
    try std.testing.expectEqual(settled, pipeline.frontiers.minRiskR2());
}

test "quiescent grouped pipeline adopts coherent immediate frontier" {
    var kernels = try kernel_mod.MultiShardKernelSet.init(
        std.testing.allocator,
        2,
        .direct,
    );
    defer kernels.deinit();
    var pipeline = try SerialGroupedPipeline.init(
        std.testing.allocator,
        &kernels,
        4,
    );
    defer pipeline.deinit();

    var first = makeNop(30);
    _ = try pipeline.submitAtSequence(.fromInt(1), &first);
    try pipeline.acknowledgeJournal(1);
    var first_response = (try pipeline.takeNextResult()).?;
    first_response.deinit(std.testing.allocator);
    try pipeline.flushTerminal();

    try executeImmediate(&kernels, makeNop(0), .success);
    try pipeline.observeIdle();
    try std.testing.expectEqual(@as(u64, 2), pipeline.frontiers.accepted);
    try std.testing.expect(pipeline.frontiers.allSettledAtAccepted());

    // A later coherent immediate publication is adopted directly by submit,
    // without requiring a separate idle observation.
    try executeImmediate(&kernels, makeNop(0), .success);
    var resumed = makeNop(29);
    _ = try pipeline.submitAtSequence(.fromInt(4), &resumed);
    try pipeline.acknowledgeJournal(4);
    var resumed_response = (try pipeline.takeNextResult()).?;
    resumed_response.deinit(std.testing.allocator);
    try pipeline.flushTerminal();
    try std.testing.expectEqual(@as(u64, 4), pipeline.frontiers.reclaimed);
}

test "serial grouped same-group withdrawal is NSF and next-group withdrawal succeeds" {
    const fixture: SpotFixture = .{};
    var kernels = try kernel_mod.MultiShardKernelSet.init(
        std.testing.allocator,
        2,
        .direct,
    );
    defer kernels.deinit();
    try fixture.setup(&kernels);

    const settled = kernels.next_seq;
    var pipeline = try SerialGroupedPipeline.init(
        std.testing.allocator,
        &kernels,
        8,
    );
    defer pipeline.deinit();

    var trade = fixture.takerBid(10_002, 41);
    _ = try pipeline.submitAtSequence(.fromInt(settled + 1), &trade);
    try pipeline.acknowledgeJournal(settled + 1);
    var trade_response = (try pipeline.takeNextResult()).?;
    defer trade_response.deinit(std.testing.allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, trade_response.result_code);
    try std.testing.expectEqual(@as(usize, 1), events.listSize(trade_response.matcher_events_head));
    try std.testing.expectEqual(
        @as(?i64, 0),
        try kernels.queryUserBalance(fixture.maker_uid, 15),
    );

    var same_group = makeBalanceAdjustment(fixture.maker_uid, 15, -1_998, 2);
    same_group.events_group = 41;
    _ = try pipeline.submitAtSequence(.fromInt(settled + 2), &same_group);
    try pipeline.acknowledgeJournal(settled + 2);
    var same_group_response = (try pipeline.takeNextResult()).?;
    defer same_group_response.deinit(std.testing.allocator);
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_account_balance_adjustment_nsf,
        same_group_response.result_code,
    );

    var next_group = makeBalanceAdjustment(fixture.maker_uid, 15, -1_998, 3);
    next_group.events_group = 42;
    _ = try pipeline.submitAtSequence(.fromInt(settled + 3), &next_group);
    try pipeline.acknowledgeJournal(settled + 3);
    var next_group_response = (try pipeline.takeNextResult()).?;
    defer next_group_response.deinit(std.testing.allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, next_group_response.result_code);
    try std.testing.expectEqual(
        @as(?i64, 0),
        try kernels.queryUserBalance(fixture.maker_uid, 15),
    );
    try pipeline.flushTerminal();
}

test "serial grouped max sequence settles results and reclaim without overflow trap" {
    var kernels = try kernel_mod.MultiShardKernelSet.init(
        std.testing.allocator,
        1,
        .direct,
    );
    defer kernels.deinit();
    const maximum = std.math.maxInt(u64);
    kernels.restorePublicationSequence(maximum - 1);

    var pipeline = try SerialGroupedPipeline.init(
        std.testing.allocator,
        &kernels,
        2,
    );
    defer pipeline.deinit();

    var command = makeNop(80);
    _ = try pipeline.submitAtSequence(.fromInt(maximum), &command);
    try pipeline.acknowledgeJournal(maximum);
    var response = (try pipeline.takeNextResult()).?;
    response.deinit(std.testing.allocator);
    try pipeline.flushTerminal();

    try std.testing.expectEqual(maximum, pipeline.frontiers.accepted);
    try std.testing.expectEqual(maximum, pipeline.frontiers.result);
    try std.testing.expectEqual(maximum, pipeline.frontiers.minRiskR2());
    try std.testing.expectEqual(maximum, pipeline.frontiers.reclaimed);
    try std.testing.expect((try pipeline.takeNextResult()) == null);

    var exhausted = makeNop(81);
    try std.testing.expectError(
        error.SequenceOverflow,
        pipeline.submitAtSequence(.fromInt(maximum), &exhausted),
    );
    try std.testing.expect(pipeline.fatalInfo() == null);
    try std.testing.expectEqual(maximum, kernels.next_seq);
    try pipeline.flushTerminal();
}

test "serial grouped reset and binary branches preserve staged smoke semantics" {
    var kernels = try kernel_mod.MultiShardKernelSet.init(
        std.testing.allocator,
        2,
        .direct,
    );
    defer kernels.deinit();
    var pipeline = try SerialGroupedPipeline.init(
        std.testing.allocator,
        &kernels,
        4,
    );
    defer pipeline.deinit();

    var reset: cmd.OrderCommand = .{
        .command = .reset,
        .result_code = .new,
        .events_group = 90,
    };
    _ = try pipeline.submitAtSequence(.fromInt(1), &reset);
    try pipeline.acknowledgeJournal(1);
    var reset_response = (try pipeline.takeNextResult()).?;
    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        reset_response.result_code,
    );
    reset_response.deinit(std.testing.allocator);

    var invalid_binary: cmd.OrderCommand = .{
        .command = .binary_data_command,
        .user_cookie = 601,
        .symbol = 9,
        .order_id = 1,
        .price = 1,
        .reserve_bid_price = 1,
        .size = 1,
        .uid = 1,
        .result_code = .new,
        .events_group = 91,
    };
    _ = try pipeline.submitAtSequence(.fromInt(2), &invalid_binary);
    try pipeline.acknowledgeJournal(2);
    var binary_response = (try pipeline.takeNextResult()).?;
    try std.testing.expectEqual(
        cmd.CommandResultCode.binary_command_failed,
        binary_response.result_code,
    );
    binary_response.deinit(std.testing.allocator);

    try pipeline.flushTerminal();
    try std.testing.expectEqual(@as(u64, 2), pipeline.frontiers.reclaimed);
}

test "caller can deinit result matcher chain before delayed r2" {
    const fixture: SpotFixture = .{};
    var kernels = try kernel_mod.MultiShardKernelSet.init(
        std.testing.allocator,
        2,
        .direct,
    );
    defer kernels.deinit();
    try fixture.setup(&kernels);

    const settled = kernels.next_seq;
    var pipeline = try SerialGroupedPipeline.init(
        std.testing.allocator,
        &kernels,
        8,
    );
    defer pipeline.deinit();

    var trade = fixture.takerBid(10_003, 51);
    _ = try pipeline.submitAtSequence(.fromInt(settled + 1), &trade);
    const slot = pipeline.slots.getSequence(settled + 1).?;
    const r2_head = slot.r2_events_head orelse return error.MissingR2Event;
    try pipeline.acknowledgeJournal(settled + 1);
    var response = (try pipeline.takeNextResult()).?;
    try std.testing.expect(response.matcher_events_head != r2_head);
    response.deinit(std.testing.allocator);

    var fence = makeNop(52);
    _ = try pipeline.submitAtSequence(.fromInt(settled + 2), &fence);
    try std.testing.expectEqual(
        @as(?i64, 1_998),
        try kernels.queryUserBalance(fixture.maker_uid, 15),
    );
    try pipeline.acknowledgeJournal(settled + 2);
    var fence_response = (try pipeline.takeNextResult()).?;
    fence_response.deinit(std.testing.allocator);
    try pipeline.flushTerminal();
}

test "serial grouped r2 preparation failure commits no risk shard" {
    const fixture: SpotFixture = .{};
    var kernels = try kernel_mod.MultiShardKernelSet.init(
        std.testing.allocator,
        2,
        .direct,
    );
    defer kernels.deinit();
    try fixture.setup(&kernels);

    const settled = kernels.next_seq;
    var pipeline = try SerialGroupedPipeline.init(
        std.testing.allocator,
        &kernels,
        8,
    );
    defer pipeline.deinit();

    var trade = fixture.takerBid(10_004, 61);
    _ = try pipeline.submitAtSequence(.fromInt(settled + 1), &trade);

    const later_risk = kernels.riskEngineAt(1);
    _ = later_risk.symbol_spec_provider.symbols.remove(fixture.symbol);

    var before: [2]u64 = undefined;
    for (&before, 0..) |*hash, index| {
        hash.* = try kernels.riskEngineAt(index).stateHash();
    }

    const flush_result = pipeline.flushTerminal();
    try std.testing.expectError(error.PositionSymbolSpecificationMissing, flush_result);
    const fatal = pipeline.fatalInfo() orelse return error.MissingFatalRecord;
    try std.testing.expectEqual(FailureStage.risk_release, fatal.stage);
    try std.testing.expectEqual(settled + 1, fatal.sequence);
    for (before, 0..) |hash, index| {
        try std.testing.expectEqual(hash, try kernels.riskEngineAt(index).stateHash());
    }
}

test "serial grouped terminal fence settles and reclaims the final open group" {
    var kernels = try kernel_mod.MultiShardKernelSet.init(
        std.testing.allocator,
        1,
        .direct,
    );
    defer kernels.deinit();
    var pipeline = try SerialGroupedPipeline.init(
        std.testing.allocator,
        &kernels,
        4,
    );
    defer pipeline.deinit();

    var command = makeNop(71);
    _ = try pipeline.submitAtSequence(.fromInt(1), &command);
    try pipeline.acknowledgeJournal(1);
    var response = (try pipeline.takeNextResult()).?;
    response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 0), pipeline.frontiers.minRiskR2());

    try pipeline.flushTerminal();
    try std.testing.expectEqual(@as(u64, 1), pipeline.frontiers.minRiskR2());
    try std.testing.expectEqual(@as(u64, 1), pipeline.frontiers.reclaimed);
}

test "persistent grouped runtime reuses exact topology workers and settles terminal group" {
    const allocator = std.testing.allocator;
    var kernels = try kernel_mod.MultiShardKernelSet.initWithTopology(
        allocator,
        4,
        2,
        .{ .orderbook_kind = .direct },
    );
    defer kernels.deinit();

    const runtime = try PersistentGroupedRuntime.create(
        allocator,
        &kernels,
        .{ .capacity = 8, .payload_allocator_is_thread_safe = true },
    );
    defer runtime.destroy() catch unreachable;
    try runtime.start();

    try std.testing.expectEqual(@as(usize, 2), runtime.riskWorkerCount());
    try std.testing.expectEqual(@as(usize, 4), runtime.matchingWorkerCount());

    var first = makeNop(10);
    _ = try runtime.submitAtSequence(.fromInt(1), &first);
    try runtime.waitForMatching(1);
    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try runtime.matchedResultCode(1),
    );
    try std.testing.expect((try runtime.takeNextResult()) == null);
    try runtime.acknowledgeJournal(1);
    var first_response = (try runtime.takeNextResult()).?;
    first_response.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 0), runtime.riskReleaseFrontier());

    const risk_ids = [_]?std.Thread.Id{
        runtime.riskWorkerId(0),
        runtime.riskWorkerId(1),
    };
    const matching_ids = [_]?std.Thread.Id{
        runtime.matchingWorkerId(0),
        runtime.matchingWorkerId(1),
        runtime.matchingWorkerId(2),
        runtime.matchingWorkerId(3),
    };
    for (risk_ids) |id| try std.testing.expect(id != null);
    for (matching_ids) |id| try std.testing.expect(id != null);

    var second = makeNop(9);
    _ = try runtime.submitAtSequence(.fromInt(2), &second);
    try runtime.acknowledgeJournal(2);
    try runtime.waitForMatching(2);
    try runtime.waitForRiskRelease(1);
    var second_response = (try runtime.takeNextResult()).?;
    second_response.deinit(allocator);

    for (risk_ids, 0..) |id, index| {
        try std.testing.expectEqual(id, runtime.riskWorkerId(index));
    }
    for (matching_ids, 0..) |id, index| {
        try std.testing.expectEqual(id, runtime.matchingWorkerId(index));
    }

    try std.testing.expectEqual(@as(u64, 2), try runtime.terminalQuiesce());
    try std.testing.expectEqual(@as(u64, 2), runtime.riskReleaseFrontier());
    try std.testing.expectEqual(@as(u64, 2), runtime.reclaimedFrontier());

    var after_barrier = makeNop(11);
    _ = try runtime.submitAtSequence(.fromInt(3), &after_barrier);
    try runtime.waitForMatching(3);
    try runtime.acknowledgeJournal(3);
    var after_barrier_response = (try runtime.takeNextResult()).?;
    after_barrier_response.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 3), try runtime.terminalQuiesce());
    for (risk_ids, 0..) |id, index| {
        try std.testing.expectEqual(id, runtime.riskWorkerId(index));
    }
    for (matching_ids, 0..) |id, index| {
        try std.testing.expectEqual(id, runtime.matchingWorkerId(index));
    }

    try runtime.shutdown();
    try std.testing.expectEqual(PersistentGroupedRuntime.Lifecycle.stopped, runtime.lifecycle());
    try std.testing.expectEqual(@as(u64, 3), runtime.riskReleaseFrontier());
    try std.testing.expectEqual(@as(u64, 3), runtime.reclaimedFrontier());
}

test "persistent grouped R2 scratch is risk width and reuses across slot wrap" {
    const allocator = std.testing.allocator;
    const fixture: SpotFixture = .{};
    var kernels = try kernel_mod.MultiShardKernelSet.initWithTopology(
        allocator,
        1,
        2,
        .{ .orderbook_kind = .direct },
    );
    defer kernels.deinit();
    try fixture.setup(&kernels);
    for (0..3) |index| {
        try executeImmediate(
            &kernels,
            fixture.makerAsk(
                fixture.maker_order_id + 1 + @as(i64, @intCast(index)),
                2,
            ),
            .success,
        );
    }
    const settled = kernels.next_seq;

    const runtime = try PersistentGroupedRuntime.create(
        allocator,
        &kernels,
        .{ .capacity = 2, .payload_allocator_is_thread_safe = true },
    );
    defer runtime.destroy() catch unreachable;
    try std.testing.expectEqual(@as(usize, 2), runtime.riskReleaseScratchCapacity());
    const prepared_ptr = runtime.prepared.ptr;
    const preparation_done_ptr = runtime.preparation_done.ptr;
    const preparation_errors_ptr = runtime.preparation_errors.ptr;
    const committed_ptr = runtime.committed.ptr;
    try runtime.start();

    var previous_trade_handle: ?SlotHandle = null;
    var next_sequence = settled + 1;
    for (0..5) |index| {
        const group = 1_000 + @as(i64, @intCast(index)) * 2;
        const trade_sequence = next_sequence;
        const boundary_sequence = trade_sequence + 1;
        var trade = fixture.takerBid(
            40_001 + @as(i64, @intCast(index)),
            group,
        );
        const trade_handle = try runtime.submitAtSequence(
            .fromInt(trade_sequence),
            &trade,
        );
        if (previous_trade_handle) |previous| {
            try std.testing.expectEqual(previous.index, trade_handle.index);
            try std.testing.expect(previous.generation != trade_handle.generation);
        }
        previous_trade_handle = trade_handle;

        var boundary = makeNop(group + 1);
        _ = try runtime.submitAtSequence(
            .fromInt(boundary_sequence),
            &boundary,
        );
        try runtime.acknowledgeJournal(trade_sequence);
        try runtime.acknowledgeJournal(boundary_sequence);
        try runtime.waitForMatching(boundary_sequence);

        var trade_response = (try runtime.takeNextResult()).?;
        try std.testing.expectEqual(trade_sequence, trade_response.sequence);
        try std.testing.expectEqual(
            cmd.CommandResultCode.success,
            trade_response.result_code,
        );
        trade_response.deinit(allocator);
        var boundary_response = (try runtime.takeNextResult()).?;
        try std.testing.expectEqual(boundary_sequence, boundary_response.sequence);
        try std.testing.expectEqual(
            cmd.CommandResultCode.success,
            boundary_response.result_code,
        );
        boundary_response.deinit(allocator);

        try std.testing.expectEqual(
            boundary_sequence,
            try runtime.terminalQuiesce(),
        );
        const scratch = runtime.riskReleaseScratchSnapshot();
        try std.testing.expect(scratch.active == null);
        try std.testing.expectEqual(@as(usize, 0), scratch.prepared_count);
        try std.testing.expectEqual(@as(usize, 0), scratch.owned_count);
        try std.testing.expectEqual(@as(usize, 0), scratch.done_count);
        try std.testing.expectEqual(@as(usize, 0), scratch.committed_count);
        try std.testing.expect(runtime.prepared.ptr == prepared_ptr);
        try std.testing.expect(runtime.preparation_done.ptr == preparation_done_ptr);
        try std.testing.expect(runtime.preparation_errors.ptr == preparation_errors_ptr);
        try std.testing.expect(runtime.committed.ptr == committed_ptr);
        next_sequence = boundary_sequence + 1;
    }

    try runtime.shutdown();
    try std.testing.expectEqual(next_sequence - 1, runtime.reclaimedFrontier());
}

test "persistent grouped R2 scratch blocks overlap with later accepted sequences" {
    const allocator = std.testing.allocator;
    const fixture: SpotFixture = .{};
    var kernels = try kernel_mod.MultiShardKernelSet.initWithTopology(
        allocator,
        1,
        2,
        .{ .orderbook_kind = .direct },
    );
    defer kernels.deinit();
    try fixture.setup(&kernels);
    const settled = kernels.next_seq;
    const trade_sequence = settled + 1;
    var gate: PersistentGroupedRuntime.StageGate = .{
        .stage = .risk_release,
        .sequence = trade_sequence,
        .shard_index = 0,
    };
    const runtime = try PersistentGroupedRuntime.create(
        allocator,
        &kernels,
        .{
            .capacity = 4,
            .payload_allocator_is_thread_safe = true,
            .stage_gate = &gate,
        },
    );
    defer runtime.destroy() catch unreachable;
    defer gate.open();
    try runtime.start();

    var trade = fixture.takerBid(41_001, 2_001);
    const trade_handle = try runtime.submitAtSequence(
        .fromInt(trade_sequence),
        &trade,
    );
    var boundary = makeNop(2_002);
    var later = makeNop(2_003);
    const later_submissions = [_]Submission{
        .{
            .sequence = .fromInt(trade_sequence + 1),
            .command = &boundary,
        },
        .{
            .sequence = .fromInt(trade_sequence + 2),
            .command = &later,
        },
    };
    try runtime.submitBatchAtSequence(&later_submissions);
    gate.waitUntilReached();
    try runtime.waitForMatching(trade_sequence);

    var scratch = runtime.riskReleaseScratchSnapshot();
    const active = scratch.active orelse return error.MissingActiveRiskReleaseScratch;
    try std.testing.expectEqual(trade_sequence, active.sequence);
    try std.testing.expectEqual(trade_handle.generation, active.generation);
    try std.testing.expectEqual(@as(usize, 0), scratch.prepared_count);
    try std.testing.expectEqual(@as(usize, 0), scratch.owned_count);
    try std.testing.expectEqual(@as(usize, 0), scratch.done_count);
    try std.testing.expectEqual(@as(usize, 0), scratch.committed_count);
    try std.testing.expectEqual(trade_sequence + 2, runtime.acceptedFrontier());
    try std.testing.expectEqual(settled, runtime.riskReleaseFrontier());

    for (0..128) |_| std.Thread.yield() catch {};
    scratch = runtime.riskReleaseScratchSnapshot();
    const still_active = scratch.active orelse
        return error.MissingActiveRiskReleaseScratch;
    try std.testing.expectEqual(active.sequence, still_active.sequence);
    try std.testing.expectEqual(active.generation, still_active.generation);
    try std.testing.expectEqual(@as(usize, 0), scratch.prepared_count);
    try std.testing.expectEqual(@as(usize, 0), scratch.owned_count);
    try std.testing.expectEqual(@as(usize, 0), scratch.done_count);
    try std.testing.expectEqual(@as(usize, 0), scratch.committed_count);

    gate.open();
    try runtime.waitForMatching(trade_sequence + 2);
    try runtime.acknowledgeJournal(trade_sequence);
    try runtime.acknowledgeJournal(trade_sequence + 1);
    try runtime.acknowledgeJournal(trade_sequence + 2);
    for (trade_sequence..trade_sequence + 3) |expected_sequence| {
        var response = (try runtime.takeNextResult()).?;
        try std.testing.expectEqual(expected_sequence, response.sequence);
        response.deinit(allocator);
    }
    try std.testing.expectEqual(
        trade_sequence + 2,
        try runtime.terminalQuiesce(),
    );
    scratch = runtime.riskReleaseScratchSnapshot();
    try std.testing.expect(scratch.active == null);
    try std.testing.expectEqual(@as(usize, 0), scratch.prepared_count);
    try std.testing.expectEqual(@as(usize, 0), scratch.owned_count);
    try std.testing.expectEqual(@as(usize, 0), scratch.done_count);
    try std.testing.expectEqual(@as(usize, 0), scratch.committed_count);
    try runtime.shutdown();
}

test "persistent grouped start atomically publishes fleet against competing APIs" {
    const allocator = std.testing.allocator;

    // A submit that enters while start holds the publication mutex observes
    // the complete running fleet and can publish normally.
    {
        var kernels = try kernel_mod.MultiShardKernelSet.init(allocator, 2, .direct);
        defer kernels.deinit();
        var gate: PersistentGroupedRuntime.StartGate = .{};
        const runtime = try PersistentGroupedRuntime.create(
            allocator,
            &kernels,
            .{
                .capacity = 4,
                .payload_allocator_is_thread_safe = true,
                .start_gate = &gate,
            },
        );
        defer runtime.destroy() catch unreachable;
        var starter: RuntimeStartRunner = .{ .runtime = runtime };
        const start_thread = try std.Thread.spawn(.{}, RuntimeStartRunner.run, .{&starter});
        gate.waitUntilReached();
        var submitter: RuntimeSubmitRunner = .{
            .runtime = runtime,
            .sequence = 1,
            .command = makeNop(1),
        };
        const submit_thread = try std.Thread.spawn(.{}, RuntimeSubmitRunner.run, .{&submitter});
        gate.waitForCompetingCalls(1);
        gate.open();
        start_thread.join();
        submit_thread.join();
        try std.testing.expect(starter.failure == null);
        try std.testing.expect(submitter.failure == null);
        try runtime.acknowledgeJournal(1);
        try runtime.waitForMatching(1);
        var response = (try runtime.takeNextResult()).?;
        response.deinit(allocator);
        try runtime.shutdown();
    }

    // Shutdown blocks behind starting and then closes the fully published
    // fleet; it cannot observe a partially populated thread array.
    {
        var kernels = try kernel_mod.MultiShardKernelSet.init(allocator, 2, .direct);
        defer kernels.deinit();
        var gate: PersistentGroupedRuntime.StartGate = .{};
        const runtime = try PersistentGroupedRuntime.create(
            allocator,
            &kernels,
            .{
                .capacity = 4,
                .payload_allocator_is_thread_safe = true,
                .start_gate = &gate,
            },
        );
        defer runtime.destroy() catch unreachable;
        var starter: RuntimeStartRunner = .{ .runtime = runtime };
        const start_thread = try std.Thread.spawn(.{}, RuntimeStartRunner.run, .{&starter});
        gate.waitUntilReached();
        var shutdown_runner: RuntimeShutdownRunner = .{ .runtime = runtime };
        const shutdown_thread = try std.Thread.spawn(
            .{},
            RuntimeShutdownRunner.run,
            .{&shutdown_runner},
        );
        gate.waitForCompetingCalls(1);
        gate.open();
        start_thread.join();
        shutdown_thread.join();
        try std.testing.expect(starter.failure == null);
        try std.testing.expect(shutdown_runner.failure == null);
        try std.testing.expectEqual(
            PersistentGroupedRuntime.Lifecycle.stopped,
            runtime.lifecycle(),
        );
    }

    // Destroy likewise blocks behind starting and rejects the now-running
    // runtime without freeing partially installed handles.
    {
        var kernels = try kernel_mod.MultiShardKernelSet.init(allocator, 2, .direct);
        defer kernels.deinit();
        var gate: PersistentGroupedRuntime.StartGate = .{};
        const runtime = try PersistentGroupedRuntime.create(
            allocator,
            &kernels,
            .{
                .capacity = 4,
                .payload_allocator_is_thread_safe = true,
                .start_gate = &gate,
            },
        );
        defer runtime.destroy() catch unreachable;
        var starter: RuntimeStartRunner = .{ .runtime = runtime };
        const start_thread = try std.Thread.spawn(.{}, RuntimeStartRunner.run, .{&starter});
        gate.waitUntilReached();
        var destroy_runner: RuntimeDestroyRunner = .{ .runtime = runtime };
        const destroy_thread = try std.Thread.spawn(
            .{},
            RuntimeDestroyRunner.run,
            .{&destroy_runner},
        );
        gate.waitForCompetingCalls(1);
        gate.open();
        start_thread.join();
        destroy_thread.join();
        try std.testing.expect(starter.failure == null);
        try std.testing.expectEqual(error.RuntimeStillActive, destroy_runner.failure.?);
        try runtime.shutdown();
    }
}

test "persistent grouped runtime rolls back deterministic partial spawn failure" {
    const allocator = std.testing.allocator;
    var kernels = try kernel_mod.MultiShardKernelSet.initWithTopology(
        allocator,
        1,
        4,
        .{ .orderbook_kind = .direct },
    );
    defer kernels.deinit();

    const runtime = try PersistentGroupedRuntime.create(
        allocator,
        &kernels,
        .{
            .capacity = 4,
            .payload_allocator_is_thread_safe = true,
            .spawn_failure_after = 2,
        },
    );
    defer runtime.destroy() catch unreachable;
    try std.testing.expectError(error.InjectedSpawnFailure, runtime.start());
    try std.testing.expectEqual(PersistentGroupedRuntime.Lifecycle.cold, runtime.lifecycle());
}

test "persistent grouped create cleans every allocation failure" {
    var kernels = try kernel_mod.MultiShardKernelSet.initWithTopology(
        std.testing.allocator,
        4,
        2,
        .{ .orderbook_kind = .direct },
    );
    defer kernels.deinit();
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        persistentCreateAllocationFailureCampaign,
        .{&kernels},
    );
}

test "persistent grouped requires and runs through one stable locked payload allocator" {
    const storage_allocator = std.testing.allocator;
    var locked = locked_allocator_mod.LockedAllocator.init(
        std.testing.allocator,
        std.testing.io,
    );
    const payload_allocator = locked.allocator();
    var kernels = try kernel_mod.MultiShardKernelSet.initWithTopology(
        payload_allocator,
        4,
        2,
        .{ .orderbook_kind = .direct },
    );
    defer kernels.deinit();
    const fixture: SpotFixture = .{};
    try fixture.setup(&kernels);
    const sequence = kernels.next_seq + 1;

    try std.testing.expectError(
        error.UnsafePayloadAllocator,
        PersistentGroupedRuntime.create(
            storage_allocator,
            &kernels,
            .{ .capacity = 4 },
        ),
    );

    const runtime = try PersistentGroupedRuntime.create(
        storage_allocator,
        &kernels,
        .{
            .capacity = 4,
            .payload_allocator_is_thread_safe = true,
        },
    );
    defer runtime.destroy() catch unreachable;
    try runtime.start();
    var trade = fixture.takerBid(50_001, 101);
    _ = try runtime.submitAtSequence(.fromInt(sequence), &trade);
    try runtime.acknowledgeJournal(sequence);
    try runtime.waitForMatching(sequence);
    var response = (try runtime.takeNextResult()).?;
    response.deinit(payload_allocator);
    try runtime.shutdown();
}

test "persistent grouped rejects non ordinary commands before publication or slot mutation" {
    const allocator = std.testing.allocator;
    var kernels = try kernel_mod.MultiShardKernelSet.init(allocator, 1, .direct);
    defer kernels.deinit();
    const runtime = try PersistentGroupedRuntime.create(
        allocator,
        &kernels,
        .{ .capacity = 4, .payload_allocator_is_thread_safe = true },
    );
    defer runtime.destroy() catch unreachable;
    try runtime.start();

    const unsupported = [_]cmd.OrderCommandType{
        .binary_data_query,
        .binary_data_command,
        .persist_state_matching,
        .persist_state_risk,
        .reset,
        .shutdown_signal,
        .reserved_compressed,
    };
    for (unsupported) |command_type| {
        var command: cmd.OrderCommand = .{
            .command = command_type,
            .result_code = .new,
        };
        try std.testing.expectError(
            error.UnsupportedCommand,
            runtime.submitAtSequence(.fromInt(1), &command),
        );
    }
    try std.testing.expectEqual(@as(u64, 0), runtime.acceptedFrontier());
    try std.testing.expectEqual(@as(u64, 0), kernels.next_seq);
    try std.testing.expectEqual(@as(usize, 0), runtime.occupiedSlotCount());
    try runtime.shutdown();
}

test "persistent grouped batch admission is atomic and retires results in fifo order" {
    const allocator = std.testing.allocator;
    var kernels = try kernel_mod.MultiShardKernelSet.init(allocator, 1, .direct);
    defer kernels.deinit();
    const runtime = try PersistentGroupedRuntime.create(
        allocator,
        &kernels,
        .{ .capacity = 4, .payload_allocator_is_thread_safe = true },
    );
    defer runtime.destroy() catch unreachable;
    try runtime.start();

    var first = makeNop(501);
    var unsupported: cmd.OrderCommand = .{
        .command = .reset,
        .result_code = .new,
        .events_group = 501,
    };
    const rejected = [_]Submission{
        .{ .sequence = .fromInt(1), .command = &first },
        .{ .sequence = .fromInt(2), .command = &unsupported },
    };
    try std.testing.expectError(
        error.UnsupportedCommand,
        runtime.submitBatchAtSequence(&rejected),
    );
    try std.testing.expectEqual(@as(u64, 0), runtime.acceptedFrontier());
    try std.testing.expectEqual(@as(u64, 0), kernels.next_seq);
    try std.testing.expectEqual(@as(usize, 0), runtime.occupiedSlotCount());

    var second = makeNop(501);
    const non_contiguous = [_]Submission{
        .{ .sequence = .fromInt(1), .command = &first },
        .{ .sequence = .fromInt(3), .command = &second },
    };
    try std.testing.expectError(
        error.NonContiguousPublicationSequence,
        runtime.submitBatchAtSequence(&non_contiguous),
    );
    try std.testing.expectEqual(@as(u64, 0), runtime.acceptedFrontier());
    try std.testing.expectEqual(@as(usize, 0), runtime.occupiedSlotCount());

    const accepted = [_]Submission{
        .{ .sequence = .fromInt(1), .command = &first },
        .{ .sequence = .fromInt(2), .command = &second },
    };
    try runtime.submitBatchAtSequence(&accepted);
    try std.testing.expectEqual(@as(u64, 2), runtime.acceptedFrontier());
    try runtime.waitForMatching(2);
    try runtime.acknowledgeJournal(1);
    try runtime.acknowledgeJournal(2);
    var first_response = (try runtime.takeNextResult()).?;
    defer first_response.deinit(allocator);
    var second_response = (try runtime.takeNextResult()).?;
    defer second_response.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 1), first_response.sequence);
    try std.testing.expectEqual(@as(u64, 2), second_response.sequence);
    try runtime.shutdown();
}

test "persistent grouped batch preserves strict result prefix after later matching fatal" {
    const allocator = std.testing.allocator;
    var kernels = try kernel_mod.MultiShardKernelSet.init(allocator, 1, .direct);
    defer kernels.deinit();
    var gate: PersistentGroupedRuntime.StageGate = .{
        .stage = .matching,
        .sequence = 2,
        .shard_index = 0,
        .fail = true,
    };
    const runtime = try PersistentGroupedRuntime.create(
        allocator,
        &kernels,
        .{
            .capacity = 4,
            .payload_allocator_is_thread_safe = true,
            .stage_gate = &gate,
        },
    );
    defer runtime.destroy() catch unreachable;
    try runtime.start();

    var first = makeNop(601);
    var fatal = makeNop(601);
    const submissions = [_]Submission{
        .{ .sequence = .fromInt(1), .command = &first },
        .{ .sequence = .fromInt(2), .command = &fatal },
    };
    try runtime.submitBatchAtSequence(&submissions);
    gate.waitUntilReached();
    gate.open();

    // Sequence one would have completed before sequence two was submitted in
    // the legacy depth-one path. Batch admission preserves that outcome even
    // though the runtime has already observed sequence two's fatal error.
    try runtime.waitForMatching(1);
    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try runtime.matchedResultCode(1),
    );
    try runtime.acknowledgeJournal(1);
    var first_response = (try runtime.takeNextResult()).?;
    defer first_response.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 1), first_response.sequence);

    try std.testing.expectError(error.PipelinePoisoned, runtime.waitForMatching(2));
    try std.testing.expectError(error.PipelinePoisoned, runtime.matchedResultCode(2));
    try std.testing.expectError(error.PipelinePoisoned, runtime.acknowledgeJournal(2));
    try std.testing.expectError(error.PipelinePoisoned, runtime.takeNextResult());
    const fatal_info = runtime.fatalInfo() orelse return error.MissingFatalRecord;
    try std.testing.expectEqual(FailureStage.matching, fatal_info.stage);
    try std.testing.expectEqual(@as(u64, 2), fatal_info.sequence);
    try std.testing.expectError(error.PipelinePoisoned, runtime.shutdown());
}

test "persistent grouped full ring rejection cannot settle prior group" {
    const allocator = std.testing.allocator;
    var kernels = try kernel_mod.MultiShardKernelSet.init(allocator, 2, .direct);
    defer kernels.deinit();
    const runtime = try PersistentGroupedRuntime.create(
        allocator,
        &kernels,
        .{ .capacity = 1, .payload_allocator_is_thread_safe = true },
    );
    defer runtime.destroy() catch unreachable;
    try runtime.start();

    var first = makeNop(41);
    _ = try runtime.submitAtSequence(.fromInt(1), &first);
    try runtime.waitForMatching(1);
    var rejected = makeNop(42);
    try std.testing.expectError(
        error.SlotNotReclaimed,
        runtime.submitAtSequence(.fromInt(2), &rejected),
    );
    try std.testing.expectEqual(@as(u64, 1), runtime.acceptedFrontier());
    try std.testing.expectEqual(@as(u64, 0), runtime.riskReleaseFrontier());

    try runtime.acknowledgeJournal(1);
    var response = (try runtime.takeNextResult()).?;
    response.deinit(allocator);
    try runtime.shutdown();
    try std.testing.expectEqual(@as(u64, 1), runtime.reclaimedFrontier());
}

test "persistent grouped all shard prepare failure commits none and poisons once" {
    const allocator = std.testing.allocator;
    const fixture: SpotFixture = .{};
    var kernels = try kernel_mod.MultiShardKernelSet.init(allocator, 2, .direct);
    defer kernels.deinit();
    try fixture.setup(&kernels);
    const settled = kernels.next_seq;

    const runtime = try PersistentGroupedRuntime.create(
        allocator,
        &kernels,
        .{ .capacity = 4, .payload_allocator_is_thread_safe = true },
    );
    defer runtime.destroy() catch unreachable;
    try runtime.start();

    var trade = fixture.takerBid(20_001, 61);
    _ = try runtime.submitAtSequence(.fromInt(settled + 1), &trade);
    try runtime.acknowledgeJournal(settled + 1);
    try runtime.waitForMatching(settled + 1);
    var response = (try runtime.takeNextResult()).?;
    try std.testing.expect(response.matcher_events_head != null);
    response.deinit(allocator);

    const failing_shard: usize = 1;
    _ = kernels.riskEngineAt(failing_shard).symbol_spec_provider.symbols.remove(
        fixture.symbol,
    );
    var before: [2]u64 = undefined;
    for (&before, 0..) |*hash, index| {
        hash.* = try kernels.riskEngineAt(index).stateHash();
    }

    try std.testing.expectError(error.PipelinePoisoned, runtime.shutdown());
    try std.testing.expectEqual(@as(usize, 2), runtime.riskReleaseScratchCapacity());
    const scratch = runtime.riskReleaseScratchSnapshot();
    const active = scratch.active orelse return error.MissingActiveRiskReleaseScratch;
    try std.testing.expectEqual(settled + 1, active.sequence);
    try std.testing.expectEqual(@as(usize, 2), scratch.prepared_count);
    try std.testing.expectEqual(@as(usize, 0), scratch.owned_count);
    try std.testing.expectEqual(@as(usize, 2), scratch.done_count);
    try std.testing.expectEqual(@as(usize, 0), scratch.committed_count);
    const fatal = runtime.fatalInfo() orelse return error.MissingFatalRecord;
    try std.testing.expectEqual(FailureStage.risk_release, fatal.stage);
    try std.testing.expectEqual(settled + 1, fatal.sequence);
    try std.testing.expectEqual(@as(?usize, failing_shard), fatal.shard_index);
    try std.testing.expectEqual(
        error.PositionSymbolSpecificationMissing,
        fatal.cause,
    );
    for (before, 0..) |hash, index| {
        try std.testing.expectEqual(hash, try kernels.riskEngineAt(index).stateHash());
    }
    const repeated = runtime.fatalInfo() orelse return error.MissingFatalRecord;
    try std.testing.expectEqual(fatal.stage, repeated.stage);
    try std.testing.expectEqual(fatal.sequence, repeated.sequence);
    try std.testing.expectEqual(fatal.shard_index, repeated.shard_index);
    try std.testing.expectEqual(fatal.cause, repeated.cause);
}

test "persistent grouped ownership loss poisons before stale terminal r2" {
    const allocator = std.testing.allocator;
    var kernels = try kernel_mod.MultiShardKernelSet.init(allocator, 2, .direct);
    defer kernels.deinit();
    const runtime = try PersistentGroupedRuntime.create(
        allocator,
        &kernels,
        .{ .capacity = 4, .payload_allocator_is_thread_safe = true },
    );
    defer runtime.destroy() catch unreachable;
    try runtime.start();

    var pending = makeNop(71);
    _ = try runtime.submitAtSequence(.fromInt(1), &pending);
    try runtime.waitForMatching(1);
    try std.testing.expectEqual(@as(u64, 0), runtime.riskReleaseFrontier());

    var thief = makeNop(72);
    var thief_response = try kernels.executeRoutedAtSequence(.fromInt(2), &thief);
    thief_response.deinit(allocator);

    var rejected = makeNop(73);
    try std.testing.expectError(
        error.PublicationFrontierOwnershipLost,
        runtime.submitAtSequence(.fromInt(2), &rejected),
    );
    const fatal = runtime.fatalInfo() orelse return error.MissingFatalRecord;
    try std.testing.expectEqual(FailureStage.publication_ownership, fatal.stage);
    try std.testing.expectEqual(@as(u64, 1), fatal.sequence);
    try std.testing.expectEqual(@as(u64, 0), runtime.riskReleaseFrontier());
    try std.testing.expectError(error.PipelinePoisoned, runtime.shutdown());
    try std.testing.expectEqual(@as(u64, 0), runtime.riskReleaseFrontier());
}

test "persistent grouped poisoned shutdown and destroy share completed join" {
    const allocator = std.testing.allocator;
    var kernels = try kernel_mod.MultiShardKernelSet.init(allocator, 2, .direct);
    defer kernels.deinit();
    var join_gate: PersistentGroupedRuntime.JoinGate = .{};
    const runtime = try PersistentGroupedRuntime.create(
        allocator,
        &kernels,
        .{
            .capacity = 4,
            .payload_allocator_is_thread_safe = true,
            .join_gate = &join_gate,
        },
    );
    try runtime.start();

    var pending = makeNop(91);
    _ = try runtime.submitAtSequence(.fromInt(1), &pending);
    try runtime.waitForMatching(1);
    var thief = makeNop(92);
    var thief_response = try kernels.executeRoutedAtSequence(.fromInt(2), &thief);
    thief_response.deinit(allocator);
    var rejected = makeNop(93);
    try std.testing.expectError(
        error.PublicationFrontierOwnershipLost,
        runtime.submitAtSequence(.fromInt(2), &rejected),
    );

    var shutdown_runner: RuntimeShutdownRunner = .{ .runtime = runtime };
    const shutdown_thread = try std.Thread.spawn(
        .{},
        RuntimeShutdownRunner.run,
        .{&shutdown_runner},
    );
    join_gate.waitUntilReached();
    var destroy_runner: RuntimeDestroyRunner = .{ .runtime = runtime };
    const destroy_thread = try std.Thread.spawn(
        .{},
        RuntimeDestroyRunner.run,
        .{&destroy_runner},
    );
    join_gate.waitUntilCompetingWaiterRegistered();
    join_gate.open();
    shutdown_thread.join();
    destroy_thread.join();
    try std.testing.expectEqual(error.PipelinePoisoned, shutdown_runner.failure.?);
    try std.testing.expect(destroy_runner.failure == null);
}

test "persistent grouped R1 retirement blocks mutating n plus one behind failing R1" {
    const allocator = std.testing.allocator;
    // R1 owner failure cannot be overtaken by another risk owner.
    for (0..8) |_| {
        var kernels = try kernel_mod.MultiShardKernelSet.init(allocator, 2, .direct);
        defer kernels.deinit();
        var stage_gate: PersistentGroupedRuntime.StageGate = .{
            .stage = .risk_pre_process,
            .sequence = 1,
            .shard_index = 0,
            .fail = true,
        };
        const runtime = try PersistentGroupedRuntime.create(
            allocator,
            &kernels,
            .{
                .capacity = 4,
                .payload_allocator_is_thread_safe = true,
                .stage_gate = &stage_gate,
            },
        );
        defer runtime.destroy() catch unreachable;
        try runtime.start();

        var failing = makeAddUser(100);
        failing.events_group = 1;
        _ = try runtime.submitAtSequence(.fromInt(1), &failing);
        stage_gate.waitUntilReached();
        var must_not_mutate = makeAddUser(101);
        must_not_mutate.events_group = 1;
        _ = try runtime.submitAtSequence(.fromInt(2), &must_not_mutate);
        stage_gate.waitUntilNextSequenceBlocked();
        try std.testing.expect(
            kernels.riskEngineForUid(must_not_mutate.uid)
                .getUserProfile(must_not_mutate.uid) == null,
        );

        stage_gate.open();
        try std.testing.expectError(error.PipelinePoisoned, runtime.shutdown());
        const fatal = runtime.fatalInfo() orelse return error.MissingFatalRecord;
        try std.testing.expectEqual(FailureStage.risk_pre_process, fatal.stage);
        try std.testing.expectEqual(@as(u64, 1), fatal.sequence);
        try std.testing.expectEqual(@as(?usize, 0), fatal.shard_index);
        try std.testing.expectEqual(error.InjectedStageFailure, fatal.cause);
        try std.testing.expect(
            kernels.riskEngineForUid(must_not_mutate.uid)
                .getUserProfile(must_not_mutate.uid) == null,
        );
    }
}

test "persistent grouped same-group R1 overlaps blocked matching like Java" {
    const allocator = std.testing.allocator;
    for (0..8) |_| {
        var kernels = try kernel_mod.MultiShardKernelSet.init(allocator, 2, .direct);
        defer kernels.deinit();
        var stage_gate: PersistentGroupedRuntime.StageGate = .{
            .stage = .matching,
            .sequence = 1,
            .shard_index = 0,
        };
        const runtime = try PersistentGroupedRuntime.create(
            allocator,
            &kernels,
            .{
                .capacity = 4,
                .payload_allocator_is_thread_safe = true,
                .stage_gate = &stage_gate,
            },
        );
        defer runtime.destroy() catch unreachable;
        try runtime.start();

        var blocked_matching = makeNop(1);
        _ = try runtime.submitAtSequence(.fromInt(1), &blocked_matching);
        stage_gate.waitUntilReached();
        var lookahead = makeAddUser(101);
        lookahead.events_group = 1;
        _ = try runtime.submitAtSequence(.fromInt(2), &lookahead);
        while (runtime.riskPreProcessFrontier() < 2) {
            std.Thread.yield() catch {};
        }
        try std.testing.expect(
            kernels.riskEngineForUid(lookahead.uid)
                .getUserProfile(lookahead.uid) != null,
        );
        try std.testing.expectEqual(@as(u64, 0), runtime.riskReleaseFrontier());

        try runtime.acknowledgeJournal(1);
        try runtime.acknowledgeJournal(2);
        stage_gate.open();
        try runtime.waitForMatching(2);
        var first = (try runtime.takeNextResult()).?;
        first.deinit(allocator);
        var second = (try runtime.takeNextResult()).?;
        second.deinit(allocator);
        try runtime.shutdown();
    }
}

test "persistent grouped cross-group R1 still waits for matching and R2" {
    const allocator = std.testing.allocator;
    for (0..8) |_| {
        var kernels = try kernel_mod.MultiShardKernelSet.init(allocator, 2, .direct);
        defer kernels.deinit();
        var stage_gate: PersistentGroupedRuntime.StageGate = .{
            .stage = .matching,
            .sequence = 1,
            .shard_index = 0,
        };
        const runtime = try PersistentGroupedRuntime.create(
            allocator,
            &kernels,
            .{
                .capacity = 4,
                .payload_allocator_is_thread_safe = true,
                .stage_gate = &stage_gate,
            },
        );
        defer runtime.destroy() catch unreachable;
        try runtime.start();

        var blocked_matching = makeNop(1);
        _ = try runtime.submitAtSequence(.fromInt(1), &blocked_matching);
        stage_gate.waitUntilReached();
        var next_group = makeAddUser(101);
        next_group.events_group = 2;
        _ = try runtime.submitAtSequence(.fromInt(2), &next_group);
        stage_gate.waitUntilNextSequenceBlocked();
        try std.testing.expectEqual(@as(u64, 1), runtime.riskPreProcessFrontier());
        try std.testing.expect(
            kernels.riskEngineForUid(next_group.uid)
                .getUserProfile(next_group.uid) == null,
        );

        try runtime.acknowledgeJournal(1);
        try runtime.acknowledgeJournal(2);
        stage_gate.open();
        try runtime.waitForMatching(2);
        var first = (try runtime.takeNextResult()).?;
        first.deinit(allocator);
        var second = (try runtime.takeNextResult()).?;
        second.deinit(allocator);
        try runtime.shutdown();
    }
}

test "persistent grouped matching fatal retains R1 suffix and runtime cannot resume" {
    const allocator = std.testing.allocator;
    for (0..8) |_| {
        var kernels = try kernel_mod.MultiShardKernelSet.init(allocator, 2, .direct);
        defer kernels.deinit();
        var stage_gate: PersistentGroupedRuntime.StageGate = .{
            .stage = .matching,
            .sequence = 1,
            .shard_index = 0,
            .fail = true,
        };
        const runtime = try PersistentGroupedRuntime.create(
            allocator,
            &kernels,
            .{
                .capacity = 4,
                .payload_allocator_is_thread_safe = true,
                .stage_gate = &stage_gate,
            },
        );
        defer runtime.destroy() catch unreachable;
        try runtime.start();

        var failing = makeNop(1);
        _ = try runtime.submitAtSequence(.fromInt(1), &failing);
        stage_gate.waitUntilReached();
        var r1_suffix = makeAddUser(101);
        r1_suffix.events_group = 1;
        _ = try runtime.submitAtSequence(.fromInt(2), &r1_suffix);
        while (runtime.riskPreProcessFrontier() < 2) {
            std.Thread.yield() catch {};
        }
        try std.testing.expect(
            kernels.riskEngineForUid(r1_suffix.uid)
                .getUserProfile(r1_suffix.uid) != null,
        );

        stage_gate.open();
        try std.testing.expectError(error.PipelinePoisoned, runtime.shutdown());
        const fatal = runtime.fatalInfo() orelse return error.MissingFatalRecord;
        try std.testing.expectEqual(FailureStage.matching, fatal.stage);
        try std.testing.expectEqual(@as(u64, 1), fatal.sequence);
        try std.testing.expectEqual(@as(?usize, 0), fatal.shard_index);
        try std.testing.expectEqual(error.InjectedStageFailure, fatal.cause);
        try std.testing.expectEqual(PersistentGroupedRuntime.Lifecycle.poisoned, runtime.lifecycle());
        try std.testing.expect(
            kernels.riskEngineForUid(r1_suffix.uid)
                .getUserProfile(r1_suffix.uid) != null,
        );
        var rejected = makeNop(1);
        try std.testing.expectError(
            error.PipelinePoisoned,
            runtime.submitAtSequence(.fromInt(3), &rejected),
        );
        try std.testing.expectError(error.PipelinePoisoned, runtime.terminalQuiesce());
        try std.testing.expectError(error.InvalidLifecycle, runtime.start());
        try std.testing.expectEqual(
            PersistentGroupedRuntime.Lifecycle.poisoned,
            runtime.lifecycle(),
        );
    }
}

test "persistent grouped max publication frontier rejects overflow without mutation" {
    const allocator = std.testing.allocator;
    var kernels = try kernel_mod.MultiShardKernelSet.init(allocator, 1, .direct);
    defer kernels.deinit();
    kernels.restorePublicationSequence(std.math.maxInt(u64));
    const runtime = try PersistentGroupedRuntime.create(
        allocator,
        &kernels,
        .{ .capacity = 2, .payload_allocator_is_thread_safe = true },
    );
    defer runtime.destroy() catch unreachable;
    try runtime.start();

    var command = makeNop(1);
    try std.testing.expectError(
        error.SequenceOverflow,
        runtime.submitAtSequence(.fromInt(std.math.maxInt(u64)), &command),
    );
    try std.testing.expectEqual(std.math.maxInt(u64), runtime.acceptedFrontier());
    try std.testing.expectEqual(@as(usize, 0), runtime.occupiedSlotCount());
    try runtime.shutdown();
}

test "persistent grouped asymmetric topologies match immediate final state" {
    const allocator = std.testing.allocator;
    const fixture: SpotFixture = .{};
    const topologies = [_]struct { matching: usize, risk: usize }{
        .{ .matching = 4, .risk = 2 },
        .{ .matching = 1, .risk = 4 },
    };

    for (topologies) |topology| {
        var immediate = try kernel_mod.MultiShardKernelSet.initWithTopology(
            allocator,
            topology.matching,
            topology.risk,
            .{ .orderbook_kind = .direct },
        );
        defer immediate.deinit();
        var persistent = try kernel_mod.MultiShardKernelSet.initWithTopology(
            allocator,
            topology.matching,
            topology.risk,
            .{ .orderbook_kind = .direct },
        );
        defer persistent.deinit();
        try fixture.setup(&immediate);
        try fixture.setup(&persistent);
        const sequence = immediate.next_seq + 1;
        try std.testing.expectEqual(sequence, persistent.next_seq + 1);

        var immediate_trade = fixture.takerBid(30_001, 81);
        var immediate_response = try immediate.executeRoutedAtSequence(
            .fromInt(sequence),
            &immediate_trade,
        );
        defer immediate_response.deinit(allocator);

        const runtime = try PersistentGroupedRuntime.create(
            allocator,
            &persistent,
            .{ .capacity = 4, .payload_allocator_is_thread_safe = true },
        );
        defer runtime.destroy() catch unreachable;
        try runtime.start();
        try std.testing.expectEqual(topology.risk, runtime.riskWorkerCount());
        try std.testing.expectEqual(
            topology.risk,
            runtime.riskReleaseScratchCapacity(),
        );
        try std.testing.expectEqual(topology.matching, runtime.matchingWorkerCount());
        var persistent_trade = fixture.takerBid(30_001, 81);
        _ = try runtime.submitAtSequence(.fromInt(sequence), &persistent_trade);
        try runtime.acknowledgeJournal(sequence);
        try runtime.waitForMatching(sequence);
        var persistent_response = (try runtime.takeNextResult()).?;
        defer persistent_response.deinit(allocator);
        try std.testing.expectEqual(
            immediate_response.result_code,
            persistent_response.result_code,
        );
        try runtime.shutdown();

        try std.testing.expectEqual(
            try immediate.detailedHashFingerprint(),
            try persistent.detailedHashFingerprint(),
        );
    }
}

test "persistent grouped journal result and r2 chains remain ordered and non aliasing" {
    const allocator = std.testing.allocator;
    const fixture: SpotFixture = .{};
    var kernels = try kernel_mod.MultiShardKernelSet.initWithTopology(
        allocator,
        4,
        2,
        .{ .orderbook_kind = .direct },
    );
    defer kernels.deinit();
    try fixture.setup(&kernels);
    const settled = kernels.next_seq;

    const runtime = try PersistentGroupedRuntime.create(
        allocator,
        &kernels,
        .{ .capacity = 8, .payload_allocator_is_thread_safe = true },
    );
    defer runtime.destroy() catch unreachable;
    try runtime.start();

    var trade = fixture.takerBid(40_001, std.math.maxInt(i64));
    _ = try runtime.submitAtSequence(.fromInt(settled + 1), &trade);
    try runtime.waitForMatching(settled + 1);
    try std.testing.expect((try runtime.takeNextResult()) == null);
    const r2_identity = runtime.stagedR2EventIdentity(settled + 1);
    try std.testing.expect(r2_identity != 0);
    try runtime.acknowledgeJournal(settled + 1);
    var trade_response = (try runtime.takeNextResult()).?;
    try std.testing.expect(trade_response.matcher_events_head != null);
    try std.testing.expect(
        @intFromPtr(trade_response.matcher_events_head.?) != r2_identity,
    );
    trade_response.deinit(allocator);
    try std.testing.expectEqual(settled, runtime.riskReleaseFrontier());
    try std.testing.expectEqual(@as(usize, 1), runtime.occupiedSlotCount());

    var same_group = makeNop(std.math.maxInt(i64));
    _ = try runtime.submitAtSequence(.fromInt(settled + 2), &same_group);
    try runtime.acknowledgeJournal(settled + 2);
    try runtime.waitForMatching(settled + 2);
    var same_group_response = (try runtime.takeNextResult()).?;
    same_group_response.deinit(allocator);
    try std.testing.expectEqual(settled, runtime.riskReleaseFrontier());

    // Equality alone retains the group; any inequality, including signed wrap,
    // closes it and gates the next R1 on every previous R2 commit.
    var wrapped_group = makeNop(std.math.minInt(i64));
    _ = try runtime.submitAtSequence(.fromInt(settled + 3), &wrapped_group);
    try runtime.acknowledgeJournal(settled + 3);
    try runtime.waitForMatching(settled + 3);
    try runtime.waitForRiskRelease(settled + 2);
    var wrapped_response = (try runtime.takeNextResult()).?;
    wrapped_response.deinit(allocator);
    try runtime.shutdown();
    try std.testing.expectEqual(settled + 3, runtime.riskReleaseFrontier());
    try std.testing.expectEqual(settled + 3, runtime.reclaimedFrontier());
    try std.testing.expectEqual(@as(usize, 0), runtime.occupiedSlotCount());
    // The live runtime inspected each newly admitted adjacency exactly once:
    // max -> max stays open, then max -> min closes the range. Worker wakeups
    // and the terminal drain must not rescan either adjacency.
    try std.testing.expectEqual(
        @as(u64, 2),
        runtime.r2_group_boundary_cache.probeCount(),
    );
}

test "persistent grouped settlement prepares exchange before R2 independently of journal" {
    const allocator = std.testing.allocator;
    const fixture: SpotFixture = .{};
    var kernels = try kernel_mod.MultiShardKernelSet.initWithTopology(
        allocator,
        1,
        2,
        .{ .orderbook_kind = .direct },
    );
    defer kernels.deinit();
    try fixture.setup(&kernels);
    const settled = kernels.next_seq;
    const trade_sequence = settled + 1;
    const boundary_sequence = trade_sequence + 1;
    var recorder: SettlementLifecycleTestRecorder = .{};

    const runtime = try PersistentGroupedRuntime.create(
        allocator,
        &kernels,
        .{
            .capacity = 4,
            .payload_allocator_is_thread_safe = true,
            .settlement_hooks = recorder.hooks(),
        },
    );
    defer runtime.destroy() catch unreachable;
    try runtime.start();

    var trade = fixture.takerBid(42_001, 3_001);
    var boundary = makeNop(3_002);
    const submissions = [_]Submission{
        .{ .sequence = .fromInt(trade_sequence), .command = &trade },
        .{ .sequence = .fromInt(boundary_sequence), .command = &boundary },
    };
    try runtime.submitBatchAtSequence(&submissions);
    try runtime.waitForRiskRelease(trade_sequence);

    // The pre-existing contract allows R2 to commit before journal durability.
    // Results remain journal-gated even though risk and its durable mark moved.
    try std.testing.expect((try runtime.takeNextResult()) == null);
    try std.testing.expectEqual(@as(usize, 1), recorder.prepare_calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), recorder.risk_calls.load(.acquire));
    try std.testing.expectEqual(trade_sequence, recorder.last_prepare_sequence.load(.acquire));
    try std.testing.expectEqual(fixture.symbol, recorder.last_prepare_symbol.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), recorder.last_prepare_trade_count.load(.acquire));
    try std.testing.expectEqual(
        @as(u8, @intFromEnum(settlement_lifecycle.ExecutionMode.live)),
        recorder.last_prepare_mode.load(.acquire),
    );

    try runtime.waitForMatching(boundary_sequence);
    try runtime.acknowledgeDurableJournal(trade_sequence);
    try runtime.acknowledgeDurableJournal(boundary_sequence);
    try std.testing.expectEqual(@as(usize, 2), recorder.journal_calls.load(.acquire));
    try std.testing.expectEqual(boundary_sequence, recorder.last_journal_sequence.load(.acquire));
    for (trade_sequence..boundary_sequence + 1) |sequence| {
        var response = (try runtime.takeNextResult()).?;
        try std.testing.expectEqual(sequence, response.sequence);
        response.deinit(allocator);
    }

    try std.testing.expectEqual(boundary_sequence, try runtime.terminalQuiesce());
    try std.testing.expectEqual(@as(usize, 1), recorder.prepare_calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 2), recorder.risk_calls.load(.acquire));
    try std.testing.expectEqual(boundary_sequence, recorder.last_risk_sequence.load(.acquire));
    try runtime.shutdown();
}

test "persistent grouped rejected exchange preparation commits no shard or later batch" {
    const allocator = std.testing.allocator;
    const fixture: SpotFixture = .{};
    var kernels = try kernel_mod.MultiShardKernelSet.initWithTopology(
        allocator,
        1,
        2,
        .{ .orderbook_kind = .direct },
    );
    defer kernels.deinit();
    try fixture.setup(&kernels);
    const settled = kernels.next_seq;
    const first_sequence = settled + 1;
    var recorder: SettlementLifecycleTestRecorder = .{
        .block_prepare = true,
        .reject_prepare = true,
    };

    const runtime = try PersistentGroupedRuntime.create(
        allocator,
        &kernels,
        .{
            .capacity = 4,
            .payload_allocator_is_thread_safe = true,
            .settlement_hooks = recorder.hooks(),
        },
    );
    defer runtime.destroy() catch unreachable;
    defer recorder.releasePrepare();
    try runtime.start();

    var first = fixture.takerBid(43_001, 4_001);
    var later = fixture.takerBid(43_002, 4_002);
    var boundary = makeNop(4_003);
    const submissions = [_]Submission{
        .{ .sequence = .fromInt(first_sequence), .command = &first },
        .{ .sequence = .fromInt(first_sequence + 1), .command = &later },
        .{ .sequence = .fromInt(first_sequence + 2), .command = &boundary },
    };
    try runtime.submitBatchAtSequence(&submissions);
    recorder.waitForPrepare();

    try std.testing.expectEqual(@as(usize, 1), recorder.prepare_calls.load(.acquire));
    try std.testing.expectEqual(first_sequence + 2, runtime.acceptedFrontier());
    try std.testing.expectEqual(settled, runtime.riskReleaseFrontier());
    const before_rejection = runtime.riskReleaseScratchSnapshot();
    try std.testing.expectEqual(@as(usize, 2), before_rejection.prepared_count);
    try std.testing.expectEqual(@as(usize, 2), before_rejection.done_count);
    try std.testing.expectEqual(@as(usize, 2), before_rejection.owned_count);
    try std.testing.expectEqual(@as(usize, 0), before_rejection.committed_count);

    recorder.releasePrepare();
    try std.testing.expectError(error.PipelinePoisoned, runtime.shutdown());
    try std.testing.expectEqual(@as(usize, 1), recorder.prepare_calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), recorder.risk_calls.load(.acquire));
    try std.testing.expectEqual(settled, runtime.riskReleaseFrontier());
    const fatal = runtime.fatalInfo() orelse return error.MissingFatalRecord;
    try std.testing.expectEqual(FailureStage.risk_release, fatal.stage);
    try std.testing.expectEqual(first_sequence, fatal.sequence);
    try std.testing.expectEqual(error.SettlementPreparationRejected, fatal.cause);
}

test "persistent grouped margin R2 marks without settlement preparation" {
    const allocator = std.testing.allocator;
    const fixture: MarginFixture = .{};
    var kernels = try kernel_mod.MultiShardKernelSet.initWithTopology(
        allocator,
        1,
        2,
        .{
            .orderbook_kind = .direct,
            .risk = .{ .margin_trading_enabled = true },
        },
    );
    defer kernels.deinit();
    try fixture.setup(&kernels);
    const settled = kernels.next_seq;
    const trade_sequence = settled + 1;
    const boundary_sequence = trade_sequence + 1;
    var recorder: SettlementLifecycleTestRecorder = .{};

    const runtime = try PersistentGroupedRuntime.create(
        allocator,
        &kernels,
        .{
            .capacity = 4,
            .payload_allocator_is_thread_safe = true,
            .settlement_hooks = recorder.hooks(),
        },
    );
    defer runtime.destroy() catch unreachable;
    try runtime.start();

    var trade = fixture.takerBid(5_001);
    var boundary = makeNop(5_002);
    const submissions = [_]Submission{
        .{ .sequence = .fromInt(trade_sequence), .command = &trade },
        .{ .sequence = .fromInt(boundary_sequence), .command = &boundary },
    };
    try runtime.submitBatchAtSequence(&submissions);
    try runtime.waitForRiskRelease(trade_sequence);
    try std.testing.expectEqual(@as(usize, 0), recorder.prepare_calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), recorder.risk_calls.load(.acquire));

    try runtime.waitForMatching(boundary_sequence);
    try runtime.acknowledgeDurableJournal(trade_sequence);
    try runtime.acknowledgeDurableJournal(boundary_sequence);
    for (0..2) |_| {
        var response = (try runtime.takeNextResult()).?;
        response.deinit(allocator);
    }
    try std.testing.expectEqual(boundary_sequence, try runtime.terminalQuiesce());
    try std.testing.expectEqual(@as(usize, 0), recorder.prepare_calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 2), recorder.risk_calls.load(.acquire));
    try runtime.shutdown();
}

test "persistent grouped rejected R2 mark preserves irreversible internal state" {
    const allocator = std.testing.allocator;
    const fixture: SpotFixture = .{};
    var kernels = try kernel_mod.MultiShardKernelSet.initWithTopology(
        allocator,
        1,
        2,
        .{ .orderbook_kind = .direct },
    );
    defer kernels.deinit();
    try fixture.setup(&kernels);
    const settled = kernels.next_seq;
    const trade_sequence = settled + 1;
    var recorder: SettlementLifecycleTestRecorder = .{ .reject_risk = true };
    const runtime = try PersistentGroupedRuntime.create(
        allocator,
        &kernels,
        .{
            .capacity = 4,
            .payload_allocator_is_thread_safe = true,
            .settlement_hooks = recorder.hooks(),
        },
    );
    defer runtime.destroy() catch unreachable;
    try runtime.start();

    var trade = fixture.takerBid(44_001, 5_501);
    var boundary = makeNop(5_502);
    const submissions = [_]Submission{
        .{ .sequence = .fromInt(trade_sequence), .command = &trade },
        .{ .sequence = .fromInt(trade_sequence + 1), .command = &boundary },
    };
    try runtime.submitBatchAtSequence(&submissions);
    try std.testing.expectError(error.PipelinePoisoned, runtime.shutdown());

    try std.testing.expectEqual(@as(usize, 1), recorder.prepare_calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), recorder.risk_calls.load(.acquire));
    try std.testing.expectEqual(trade_sequence, runtime.riskReleaseFrontier());
    const slot = runtime.slots.getSequence(trade_sequence) orelse
        return error.MissingCommittedRiskReleaseSlot;
    try std.testing.expect(slot.r2_complete);
    const scratch = runtime.riskReleaseScratchSnapshot();
    try std.testing.expect(scratch.active == null);
    try std.testing.expectEqual(@as(usize, 0), scratch.prepared_count);
    try std.testing.expectEqual(@as(usize, 0), scratch.committed_count);
    const fatal = runtime.fatalInfo() orelse return error.MissingFatalRecord;
    try std.testing.expectEqual(FailureStage.risk_release, fatal.stage);
    try std.testing.expectEqual(trade_sequence, fatal.sequence);
    try std.testing.expectEqual(error.SettlementRiskR2MarkRejected, fatal.cause);
}

test "persistent grouped durable journal rejection poisons before result" {
    const allocator = std.testing.allocator;
    var kernels = try kernel_mod.MultiShardKernelSet.init(allocator, 1, .direct);
    defer kernels.deinit();
    var recorder: SettlementLifecycleTestRecorder = .{ .reject_journal = true };
    const runtime = try PersistentGroupedRuntime.create(
        allocator,
        &kernels,
        .{
            .capacity = 2,
            .payload_allocator_is_thread_safe = true,
            .settlement_hooks = recorder.hooks(),
        },
    );
    defer runtime.destroy() catch unreachable;
    try runtime.start();

    var command = makeNop(6_001);
    _ = try runtime.submitAtSequence(.fromInt(1), &command);
    try runtime.waitForMatching(1);
    try std.testing.expectError(
        error.SettlementJournalMarkRejected,
        runtime.acknowledgeDurableJournal(1),
    );
    try std.testing.expectEqual(@as(usize, 1), recorder.journal_calls.load(.acquire));
    try std.testing.expectError(error.PipelinePoisoned, runtime.takeNextResult());
    try std.testing.expectError(error.PipelinePoisoned, runtime.shutdown());
    try std.testing.expectEqual(@as(u64, 0), runtime.frontiers.journal);
    const slot = runtime.slots.getSequence(1) orelse return error.MissingJournalSlot;
    try std.testing.expect(!slot.journal_acked);

    const fatal = runtime.fatalInfo() orelse return error.MissingFatalRecord;
    try std.testing.expectEqual(FailureStage.journal, fatal.stage);
    try std.testing.expectEqual(@as(u64, 1), fatal.sequence);
    try std.testing.expectEqual(error.SettlementJournalMarkRejected, fatal.cause);
}
