const std = @import("std");
const cmd = @import("cmd.zig");
const common = @import("common.zig");
const events = @import("events.zig");
const publication = @import("publication_sequence.zig");

/// Distinguishes ordinary execution from deterministic journal replay without
/// changing the economic identity of a fill. Integrations may use this only
/// for diagnostics and recovery policy; mapper IDs remain sequence-derived.
pub const ExecutionMode = enum {
    live,
    replay,
};

/// Borrowed matching output presented after every risk shard has completed its
/// fallible R2 preparation and before the first shard commits. Implementations
/// must copy everything they retain before returning.
pub const PreparedBatch = struct {
    sequence: publication.PublicationSequence,
    command: *const cmd.OrderCommand,
    specification: common.CoreSymbolSpecification,
    events_head: ?*const events.MatcherTradeEvent,
    mode: ExecutionMode,

    pub fn tradeCount(self: PreparedBatch) usize {
        var count: usize = 0;
        var node = self.events_head;
        while (node) |event| : (node = event.next_event) {
            if (event.event_type == .trade) count += 1;
        }
        return count;
    }
};

/// Raw callback result. Rejection is deliberately collapsed at this boundary:
/// an integration owns its detailed diagnostic, while the exchange receives a
/// stable typed failure suitable for poisoning an already-mutated command.
pub const PrepareObservation = enum {
    skipped,
    prepared,
    rejected,
};

pub const MarkObservation = enum {
    marked,
    rejected,
};

pub const RecoveryObservation = enum {
    completed,
    not_required,
    rejected,
};

pub const PrepareDisposition = enum {
    disabled,
    skipped,
    prepared,
};

pub const MarkDisposition = enum {
    disabled,
    marked,
};

pub const RecoveryDisposition = enum {
    disabled,
    completed,
    not_required,
};

pub const Error = error{
    SettlementPreparationRejected,
    SettlementRiskR2MarkRejected,
    SettlementJournalMarkRejected,
    SettlementRecoveryRejected,
};

/// Type-erased, synchronous lifecycle boundary owned by the application.
///
/// The disabled value is dependency-free and allocation-free. Enabled
/// callbacks must be thread-safe: persistent grouped R2 and durable journal
/// completion may arrive from different owners. They must not perform external
/// ledger transport; only durable local preparation and transient frontier
/// marks belong inside exchange execution.
pub const Hooks = union(enum) {
    disabled_state,
    enabled_state: struct {
        context: *anyopaque,
        vtable: *const VTable,
    },

    pub const VTable = struct {
        prepare: *const fn (
            context: *anyopaque,
            batch: PreparedBatch,
        ) PrepareObservation,
        mark_risk_r2_committed: *const fn (
            context: *anyopaque,
            sequence: publication.PublicationSequence,
        ) MarkObservation,
        mark_journal_committed: *const fn (
            context: *anyopaque,
            sequence: publication.PublicationSequence,
        ) MarkObservation,
        finish_recovery: *const fn (
            context: *anyopaque,
            sequence: publication.PublicationSequence,
        ) RecoveryObservation,
    };

    pub fn disabled() Hooks {
        return .disabled_state;
    }

    pub fn enabled(context: *anyopaque, vtable: *const VTable) Hooks {
        return .{ .enabled_state = .{
            .context = context,
            .vtable = vtable,
        } };
    }

    pub fn isEnabled(self: Hooks) bool {
        return switch (self) {
            .disabled_state => false,
            .enabled_state => true,
        };
    }

    pub fn prepare(
        self: Hooks,
        batch: PreparedBatch,
    ) Error!PrepareDisposition {
        const enabled_state = switch (self) {
            .disabled_state => return .disabled,
            .enabled_state => |state| state,
        };
        if (batch.tradeCount() == 0) return .skipped;
        return switch (enabled_state.vtable.prepare(enabled_state.context, batch)) {
            // An enabled integration may skip only an empty economic batch,
            // which is handled before callback dispatch. Skipping a real fill
            // would silently decouple risk state from external balances.
            .skipped => error.SettlementPreparationRejected,
            .prepared => .prepared,
            .rejected => error.SettlementPreparationRejected,
        };
    }

    pub fn markRiskR2Committed(
        self: Hooks,
        sequence: publication.PublicationSequence,
    ) Error!MarkDisposition {
        const enabled_state = switch (self) {
            .disabled_state => return .disabled,
            .enabled_state => |state| state,
        };
        return switch (enabled_state.vtable.mark_risk_r2_committed(
            enabled_state.context,
            sequence,
        )) {
            .marked => .marked,
            .rejected => error.SettlementRiskR2MarkRejected,
        };
    }

    pub fn markJournalCommitted(
        self: Hooks,
        sequence: publication.PublicationSequence,
    ) Error!MarkDisposition {
        const enabled_state = switch (self) {
            .disabled_state => return .disabled,
            .enabled_state => |state| state,
        };
        return switch (enabled_state.vtable.mark_journal_committed(
            enabled_state.context,
            sequence,
        )) {
            .marked => .marked,
            .rejected => error.SettlementJournalMarkRejected,
        };
    }

    /// Releases an integration's restart fence exactly once after snapshot and
    /// journal replay have succeeded and the durable frontier was validated.
    /// Replay preparation never publishes transient marks; this callback
    /// establishes both recovered frontiers atomically.
    pub fn finishRecovery(
        self: Hooks,
        sequence: publication.PublicationSequence,
    ) Error!RecoveryDisposition {
        const enabled_state = switch (self) {
            .disabled_state => return .disabled,
            .enabled_state => |state| state,
        };
        return switch (enabled_state.vtable.finish_recovery(
            enabled_state.context,
            sequence,
        )) {
            .completed => .completed,
            .not_required => .not_required,
            .rejected => error.SettlementRecoveryRejected,
        };
    }
};

const TestRecorder = struct {
    prepare_calls: usize = 0,
    risk_calls: usize = 0,
    journal_calls: usize = 0,
    recovery_calls: usize = 0,
    last_sequence: u64 = 0,
    last_mode: ExecutionMode = .live,
    reject_prepare: bool = false,
    reject_risk: bool = false,
    reject_journal: bool = false,
    reject_recovery: bool = false,

    fn hooks(self: *TestRecorder) Hooks {
        return Hooks.enabled(self, &.{
            .prepare = prepare,
            .mark_risk_r2_committed = markRisk,
            .mark_journal_committed = markJournal,
            .finish_recovery = finishRecovery,
        });
    }

    fn prepare(context: *anyopaque, batch: PreparedBatch) PrepareObservation {
        const self: *TestRecorder = @ptrCast(@alignCast(context));
        self.prepare_calls += 1;
        self.last_sequence = batch.sequence.toInt();
        self.last_mode = batch.mode;
        if (self.reject_prepare) return .rejected;
        return if (batch.tradeCount() == 0) .skipped else .prepared;
    }

    fn markRisk(
        context: *anyopaque,
        sequence: publication.PublicationSequence,
    ) MarkObservation {
        const self: *TestRecorder = @ptrCast(@alignCast(context));
        self.risk_calls += 1;
        self.last_sequence = sequence.toInt();
        return if (self.reject_risk) .rejected else .marked;
    }

    fn markJournal(
        context: *anyopaque,
        sequence: publication.PublicationSequence,
    ) MarkObservation {
        const self: *TestRecorder = @ptrCast(@alignCast(context));
        self.journal_calls += 1;
        self.last_sequence = sequence.toInt();
        return if (self.reject_journal) .rejected else .marked;
    }

    fn finishRecovery(
        context: *anyopaque,
        sequence: publication.PublicationSequence,
    ) RecoveryObservation {
        const self: *TestRecorder = @ptrCast(@alignCast(context));
        self.recovery_calls += 1;
        self.last_sequence = sequence.toInt();
        return if (self.reject_recovery) .rejected else .completed;
    }
};

fn testCommand() cmd.OrderCommand {
    return .{
        .command = .place_order,
        .result_code = .success,
        .uid = 1001,
        .symbol = 241,
        .action = .bid,
    };
}

fn testSpecification() common.CoreSymbolSpecification {
    return common.CoreSymbolSpecification.init(.{
        .symbol_id = 241,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 100,
        .quote_scale_k = 10,
        .taker_fee = 0,
        .maker_fee = 0,
    }) catch unreachable;
}

test "disabled settlement lifecycle is inert" {
    var command = testCommand();
    const hooks = Hooks.disabled();
    try std.testing.expect(!hooks.isEnabled());
    try std.testing.expectEqual(
        PrepareDisposition.disabled,
        try hooks.prepare(.{
            .sequence = .fromInt(7),
            .command = &command,
            .specification = testSpecification(),
            .events_head = null,
            .mode = .live,
        }),
    );
    try std.testing.expectEqual(
        MarkDisposition.disabled,
        try hooks.markRiskR2Committed(.fromInt(7)),
    );
    try std.testing.expectEqual(
        MarkDisposition.disabled,
        try hooks.markJournalCommitted(.fromInt(7)),
    );
    try std.testing.expectEqual(
        RecoveryDisposition.disabled,
        try hooks.finishRecovery(.fromInt(7)),
    );
}

test "enabled settlement lifecycle preserves replay mode and translates rejection" {
    var recorder: TestRecorder = .{};
    const hooks = recorder.hooks();
    var command = testCommand();
    var trade: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = 1002,
        .price = 50,
        .size = 2,
    };

    try std.testing.expectEqual(
        PrepareDisposition.prepared,
        try hooks.prepare(.{
            .sequence = .fromInt(9),
            .command = &command,
            .specification = testSpecification(),
            .events_head = &trade,
            .mode = .replay,
        }),
    );
    try std.testing.expectEqual(@as(usize, 1), recorder.prepare_calls);
    try std.testing.expectEqual(@as(u64, 9), recorder.last_sequence);
    try std.testing.expectEqual(ExecutionMode.replay, recorder.last_mode);

    try std.testing.expectEqual(
        PrepareDisposition.skipped,
        try hooks.prepare(.{
            .sequence = .fromInt(10),
            .command = &command,
            .specification = testSpecification(),
            .events_head = null,
            .mode = .live,
        }),
    );
    try std.testing.expectEqual(@as(usize, 1), recorder.prepare_calls);

    recorder.reject_prepare = true;
    try std.testing.expectError(
        error.SettlementPreparationRejected,
        hooks.prepare(.{
            .sequence = .fromInt(10),
            .command = &command,
            .specification = testSpecification(),
            .events_head = &trade,
            .mode = .live,
        }),
    );
    recorder.reject_risk = true;
    try std.testing.expectError(
        error.SettlementRiskR2MarkRejected,
        hooks.markRiskR2Committed(.fromInt(10)),
    );
    recorder.reject_journal = true;
    try std.testing.expectError(
        error.SettlementJournalMarkRejected,
        hooks.markJournalCommitted(.fromInt(10)),
    );
    try std.testing.expectEqual(
        RecoveryDisposition.completed,
        try hooks.finishRecovery(.fromInt(10)),
    );
    try std.testing.expectEqual(@as(usize, 1), recorder.recovery_calls);

    recorder.reject_recovery = true;
    try std.testing.expectError(
        error.SettlementRecoveryRejected,
        hooks.finishRecovery(.fromInt(11)),
    );
}
