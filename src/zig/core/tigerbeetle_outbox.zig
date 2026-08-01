const std = @import("std");
const io_context = @import("io_context.zig");
const persistence_paths = @import("persistence_paths.zig");
const publication = @import("publication_sequence.zig");
const settlement = @import("tigerbeetle_settlement.zig");

const snapshot_magic = "TSO1";
const snapshot_version: u16 = 3;
const snapshot_digest_len: usize = 32;
const snapshot_header_len: usize = 66;
const snapshot_entry_header_len: usize = 47;
const encoded_transfer_len: usize = 128;
const max_outbox_capacity: usize = 4096;

pub const PublicationDomain = settlement.PublicationDomain;
pub const PublicationCursor = settlement.PublicationCursor;

/// Immediate mode preserves the original standalone plugin contract. In
/// two-frontier mode records are durable but ineligible until both the R2 and
/// journal commit frontiers cover their publication sequence.
pub const OutboxMode = enum(u8) {
    immediate = 1,
    two_frontier = 2,
};

/// Process-local provenance for the prepared store image. A decoded image is
/// always restart-owned and must pass replay recovery before dispatch. A fresh
/// image may skip recovery only when it has observed a contiguous live commit
/// prefix beginning at publication sequence one.
pub const RecoveryProvenance = enum {
    fresh,
    reopened,
};

/// Non-secret, stable identity for one external ledger destination. A durable
/// outbox file is bound to exactly one destination so `(destination, group_id)`
/// remains the idempotency key across retries and restarts.
pub const DestinationId = enum(u128) { _ };

/// Destination-bound mapper output. Keeping the destination beside the owned
/// chain prevents valid but destination-scoped IDs from being sent to the
/// wrong external ledger universe.
pub const SettlementRecord = struct {
    destination_id: DestinationId,
    chain: settlement.TransferChain,
};

/// One mapper result prepared before the command's R2 and journal commits.
/// Every record passed to one `prepare` call must carry the same cursor.
pub const PreparedSettlementRecord = struct {
    destination_id: DestinationId,
    publication: PublicationCursor,
    chain: settlement.TransferChain,
};

pub const PrepareBatchResult = struct {
    inserted: usize = 0,
    duplicates: usize = 0,
    already_finalized: usize = 0,
};

pub const RetryAt = struct {
    /// In-process logical deadline. Durable reopen deliberately resets every
    /// nonterminal deadline to immediate eligibility; callers must not treat
    /// this value as a restart-stable wall-clock timestamp.
    tick: u64,
};

pub const QuarantineReason = enum(u8) {
    permanent_submission_failure = 1,
    permanent_reconciliation_failure = 2,
    partial_application = 3,
    transfer_mismatch = 4,
    destination_mismatch = 5,
};

pub const EntryStatus = enum(u8) {
    ready = 1,
    submitting = 2,
    needs_reconciliation = 3,
    reconciling = 4,
    delivered = 5,
    quarantined = 6,
};

/// Why a submission must be looked up before the chain can be sent again.
/// A rejected linked batch may be an exact duplicate, but if every ID is
/// absent the deterministic rejection is terminal rather than retryable.
pub const ReconciliationCause = enum(u8) {
    ambiguous_submission = 1,
    rejected_submission = 2,
};

const ReconciliationState = struct {
    retry: RetryAt,
    cause: ReconciliationCause,
};

const EntryState = union(EntryStatus) {
    ready: RetryAt,
    submitting,
    needs_reconciliation: ReconciliationState,
    reconciling: ReconciliationCause,
    delivered,
    quarantined: QuarantineReason,
};

pub const WorkKind = enum(u8) {
    submit,
    reconcile,
};

pub const ClaimToken = struct {
    destination_id: DestinationId,
    group_id: u128,
    generation: u64,
    kind: WorkKind,
};

/// The transfer chain is copied by value. Dispatchers may safely retain this
/// work item without borrowing the outbox or the mapper's inline slice.
pub const DeliveryWork = struct {
    token: ClaimToken,
    chain: settlement.TransferChain,
};

pub const DispatchState = union(enum) {
    drained,
    awaiting_commit: struct {
        group_id: u128,
        publication: PublicationCursor,
        risk_r2_committed: bool,
        journal_committed: bool,
    },
    recovery_required: struct {
        domain: PublicationDomain,
    },
    waiting_submit: struct {
        group_id: u128,
        retry: RetryAt,
    },
    waiting_reconciliation: struct {
        group_id: u128,
        retry: RetryAt,
        cause: ReconciliationCause,
    },
    in_flight: struct {
        group_id: u128,
        kind: WorkKind,
    },
    quarantined: struct {
        group_id: u128,
        reason: QuarantineReason,
    },
};

pub const SubmitOutcome = union(enum) {
    committed,
    /// The adapter has proof that no request reached the destination.
    known_not_sent: RetryAt,
    /// The request may have committed. A lookup is mandatory before retrying.
    ambiguous: RetryAt,
    /// The destination rejected at least one item. Lookup is still mandatory
    /// because the response may represent an exact duplicate or be truncated.
    rejected: RetryAt,
    permanent: QuarantineReason,
};

pub const ReconcileOutcome = union(enum) {
    all_present_exact,
    all_absent: RetryAt,
    retryable: RetryAt,
    partial_application,
    transfer_mismatch,
    permanent: QuarantineReason,
};

pub const EnqueueResult = enum {
    inserted,
    empty_finalized,
    duplicate,
    already_finalized,
};

pub const ApplyResult = enum {
    updated,
    already_terminal,
};

pub const ClaimValidationResult = enum {
    active,
    already_pruned,
};

pub const ClaimResolution = union(enum) {
    active: *const settlement.TransferChain,
    already_terminal,
};

pub const OutboxError = error{
    InvalidDestination,
    InvalidGroupId,
    InvalidTransferCount,
    InvalidTransfer,
    InvalidLinkedFlags,
    ConflictingDuplicate,
    NonMonotonicGroupId,
    OutboxFull,
    UnknownGroup,
    WrongDestination,
    WrongWorkKind,
    StaleClaim,
    ClaimPayloadMismatch,
    AttemptOverflow,
    GenerationOverflow,
    InvalidPruneWatermark,
    UnsettledEntry,
    WrongMode,
    InvalidPreparedBatch,
    InvalidPublicationDomain,
    InvalidPublicationSequence,
    PublicationDomainMismatch,
    PublicationSequenceMismatch,
    AwaitingCommit,
    PreparedAfterCommit,
    RecoveryRequired,
    ProgressedBeyondRecoveryFrontier,
    UnprovenRecoveryFrontier,
};

pub const CodecError = error{
    BufferTooSmall,
    Truncated,
    InvalidMagic,
    UnsupportedVersion,
    CapacityMismatch,
    DestinationMismatch,
    EntryCountExceeded,
    DigestMismatch,
    TrailingBytes,
    InvalidState,
    InvalidSnapshotOrder,
    InvalidSnapshotWatermark,
    InvalidSnapshotChain,
    InvalidSnapshotMode,
    InvalidSnapshotDomain,
    InvalidSnapshotPublication,
    SnapshotModeMismatch,
    SnapshotDomainMismatch,
};

pub const StoreError = OutboxError || CodecError || std.mem.Allocator.Error || error{
    AlreadyOpen,
    Poisoned,
    StorageReadFailed,
    StorageAmbiguous,
};

const Entry = struct {
    chain: settlement.TransferChain,
    /// Zero in immediate mode. This durable cursor component is deliberately
    /// separate from the two transient in-memory commit frontiers.
    publication_sequence: u64 = 0,
    state: EntryState,
    submit_attempts: u32 = 0,
    generation: u64 = 0,
};

fn entryNeverAttempted(entry: Entry) bool {
    if (entry.submit_attempts != 0 or entry.generation != 0) return false;
    return switch (entry.state) {
        .ready => |retry| entry.chain.count != 0 and retry.tick == 0,
        .delivered => entry.chain.count == 0,
        else => false,
    };
}

/// Bounded, allocation-free settlement delivery state machine.
///
/// Work is claimed strictly in publication order. A delayed, in-flight, or
/// quarantined earlier entry blocks all later entries, preserving dependencies
/// between fills. Callers must durably persist the mutated state returned by
/// `beginNext` before dispatch; `DurableFileOutbox` enforces that ordering.
pub fn SettlementOutbox(comptime capacity: usize) type {
    comptime {
        if (capacity == 0) @compileError("settlement outbox capacity must be positive");
        if (capacity > max_outbox_capacity) @compileError("settlement outbox capacity exceeds durable codec limit");
    }

    return struct {
        const Self = @This();

        pub const max_encoded_size: usize = snapshot_header_len +
            capacity * (snapshot_entry_header_len + settlement.max_transfers_per_fill * encoded_transfer_len) +
            snapshot_digest_len;

        destination_id: DestinationId,
        outbox_mode: OutboxMode = .immediate,
        publication_domain: PublicationDomain = .{ .namespace = 0, .journal_epoch = 0 },
        entries: [capacity]Entry = undefined,
        count: u32 = 0,
        pruned_through: u128 = 0,
        high_watermark: u128 = 0,
        /// These monotonic commit frontiers are process-local facts. They are
        /// intentionally absent from the snapshot codec and reset on reopen.
        risk_r2_committed_through: u64 = 0,
        journal_committed_through: u64 = 0,
        recovery_required: bool = false,
        recovery_provenance: RecoveryProvenance = .fresh,
        fresh_risk_r2_prefix_through: u64 = 0,
        fresh_journal_prefix_through: u64 = 0,
        fresh_frontier_gap: bool = false,

        pub fn init(destination_id: DestinationId) OutboxError!Self {
            if (@intFromEnum(destination_id) == 0) return error.InvalidDestination;
            return .{ .destination_id = destination_id };
        }

        pub fn initPrepared(
            destination_id: DestinationId,
            domain: PublicationDomain,
        ) OutboxError!Self {
            if (@intFromEnum(destination_id) == 0) return error.InvalidDestination;
            domain.validate() catch return error.InvalidPublicationDomain;
            return .{
                .destination_id = destination_id,
                .outbox_mode = .two_frontier,
                .publication_domain = domain,
            };
        }

        pub fn destinationId(self: *const Self) DestinationId {
            return self.destination_id;
        }

        pub fn mode(self: *const Self) OutboxMode {
            return self.outbox_mode;
        }

        pub fn publicationDomain(self: *const Self) ?PublicationDomain {
            return switch (self.outbox_mode) {
                .immediate => null,
                .two_frontier => self.publication_domain,
            };
        }

        pub fn isRecoveryRequired(self: *const Self) bool {
            return self.recovery_required;
        }

        pub fn recoveryProvenance(self: *const Self) RecoveryProvenance {
            return self.recovery_provenance;
        }

        pub fn len(self: *const Self) usize {
            return @intCast(self.count);
        }

        pub fn status(self: *const Self, group_id: u128) ?EntryStatus {
            const index = self.findIndex(group_id) orelse return null;
            return std.meta.activeTag(self.entries[index].state);
        }

        pub fn submitAttempts(self: *const Self, group_id: u128) ?u32 {
            const index = self.findIndex(group_id) orelse return null;
            return self.entries[index].submit_attempts;
        }

        pub fn dispatchState(self: *const Self) DispatchState {
            if (self.recovery_required) {
                return .{ .recovery_required = .{ .domain = self.publication_domain } };
            }
            for (self.entries[0..self.len()]) |entry| {
                const group_id = entry.chain.group_id;
                if (!self.isCommitted(entry)) {
                    return .{ .awaiting_commit = .{
                        .group_id = group_id,
                        .publication = self.cursorFor(entry),
                        .risk_r2_committed = self.risk_r2_committed_through >= entry.publication_sequence,
                        .journal_committed = self.journal_committed_through >= entry.publication_sequence,
                    } };
                }
                switch (entry.state) {
                    .delivered => continue,
                    .ready => |retry| return .{ .waiting_submit = .{
                        .group_id = group_id,
                        .retry = retry,
                    } },
                    .submitting => return .{ .in_flight = .{
                        .group_id = group_id,
                        .kind = .submit,
                    } },
                    .needs_reconciliation => |reconciliation| return .{ .waiting_reconciliation = .{
                        .group_id = group_id,
                        .retry = reconciliation.retry,
                        .cause = reconciliation.cause,
                    } },
                    .reconciling => return .{ .in_flight = .{
                        .group_id = group_id,
                        .kind = .reconcile,
                    } },
                    .quarantined => |reason| return .{ .quarantined = .{
                        .group_id = group_id,
                        .reason = reason,
                    } },
                }
            }
            return .drained;
        }

        pub fn enqueue(self: *Self, record: SettlementRecord) OutboxError!EnqueueResult {
            if (self.outbox_mode != .immediate) return error.WrongMode;
            if (record.destination_id != self.destination_id) return error.WrongDestination;
            const chain = record.chain;
            if (chain.group_id == 0) return error.InvalidGroupId;
            if (chain.count > settlement.max_transfers_per_fill) return error.InvalidTransferCount;
            const owned_chain = canonicalizeChain(chain);
            try validateChain(owned_chain);

            if (self.findIndex(owned_chain.group_id)) |index| {
                if (!chainsEqual(self.entries[index].chain, owned_chain)) return error.ConflictingDuplicate;
                return .duplicate;
            }
            if (owned_chain.group_id <= self.pruned_through) return .already_finalized;
            if (owned_chain.group_id <= self.high_watermark) return error.NonMonotonicGroupId;
            if (self.len() == capacity) return error.OutboxFull;

            const empty = owned_chain.count == 0;
            self.entries[self.len()] = .{
                .chain = owned_chain,
                .publication_sequence = 0,
                .state = if (empty) .delivered else .{ .ready = .{ .tick = 0 } },
            };
            self.count += 1;
            self.high_watermark = owned_chain.group_id;
            if (empty) {
                // Empty economic chains are terminal immediately. Reclaim them
                // in the same candidate mutation so DurableFileOutbox persists
                // admission and pruning with one atomic replacement. If an
                // older unsettled entry exists this removes nothing; normal
                // delivered-prefix pruning will reclaim both later.
                _ = try self.pruneDeliveredPrefix();
                return .empty_finalized;
            }
            return .inserted;
        }

        /// Durably stages every settlement chain emitted by one command. The
        /// receiver is published only after the complete candidate succeeds,
        /// making validation and capacity failures atomic for the whole batch.
        pub fn prepare(
            self: *Self,
            records: []const PreparedSettlementRecord,
        ) OutboxError!PrepareBatchResult {
            if (self.outbox_mode != .two_frontier) return error.WrongMode;
            if (self.recovery_required) return error.RecoveryRequired;
            return self.prepareBatchCandidate(records);
        }

        /// Rebuilds records missing from the durable outbox while journal
        /// replay is still fenced. Dispatch and transient frontier marks stay
        /// disabled until `finishRecovery` publishes the authoritative replay
        /// cursor once.
        ///
        /// Replay is ordered by publication sequence. Therefore an existing
        /// record can only be an exact duplicate/already-pruned prefix, and a
        /// missing record can only append after the current watermark. A gap
        /// below a later prepared record fails closed through the ordinary
        /// monotonic-group validation instead of rewriting durable history.
        pub fn prepareRecovery(
            self: *Self,
            records: []const PreparedSettlementRecord,
        ) OutboxError!PrepareBatchResult {
            if (self.outbox_mode != .two_frontier) return error.WrongMode;
            if (!self.recovery_required) return error.RecoveryRequired;
            return self.prepareBatchCandidate(records);
        }

        fn prepareBatchCandidate(
            self: *Self,
            records: []const PreparedSettlementRecord,
        ) OutboxError!PrepareBatchResult {
            if (records.len == 0) return error.InvalidPreparedBatch;

            const publication_cursor = records[0].publication;
            try self.validatePreparedCursor(publication_cursor);
            for (records, 0..) |record, ordinal| {
                if (!record.publication.eql(publication_cursor)) return error.InvalidPreparedBatch;
                const expected_ordinal = std.math.cast(u32, ordinal) orelse
                    return error.InvalidPreparedBatch;
                const identity = settlement.decodeFillGroupId(record.chain.group_id) catch
                    return error.InvalidGroupId;
                if (identity.fill_ordinal != expected_ordinal) {
                    return error.InvalidPreparedBatch;
                }
            }

            var candidate = self.*;
            var result: PrepareBatchResult = .{};
            for (records) |record| {
                switch (try candidate.prepareOne(record)) {
                    .inserted => result.inserted += 1,
                    .duplicate => result.duplicates += 1,
                    .already_finalized => result.already_finalized += 1,
                    .empty_finalized => unreachable,
                }
            }
            self.* = candidate;
            return result;
        }

        /// Marks are deliberately infallible and allocation/I/O-free because
        /// they execute after the corresponding core commit. Invalid, stale,
        /// cross-domain, and recovery-time marks fail closed by doing nothing.
        pub fn markRiskR2CommittedThrough(self: *Self, frontier: PublicationCursor) void {
            if (!self.acceptsLiveFrontier(frontier)) return;
            self.observeFreshFrontier(&self.fresh_risk_r2_prefix_through, frontier);
            self.risk_r2_committed_through = @max(
                self.risk_r2_committed_through,
                frontier.publication_sequence,
            );
        }

        pub fn markJournalCommittedThrough(self: *Self, frontier: PublicationCursor) void {
            if (!self.acceptsLiveFrontier(frontier)) return;
            self.observeFreshFrontier(&self.fresh_journal_prefix_through, frontier);
            self.journal_committed_through = @max(
                self.journal_committed_through,
                frontier.publication_sequence,
            );
        }

        /// Claims the oldest unsettled work item. The returned chain owns its
        /// bytes. No later entry can overtake an earlier delayed or poisoned one.
        pub fn beginNext(self: *Self, now_tick: u64) OutboxError!?DeliveryWork {
            if (self.recovery_required) return null;
            for (self.entries[0..self.len()]) |*entry| {
                if (!self.isCommitted(entry.*)) return null;
                switch (entry.state) {
                    .delivered => continue,
                    .ready => |retry| {
                        if (now_tick < retry.tick) return null;
                        return try self.claim(entry, .submit);
                    },
                    .needs_reconciliation => |reconciliation| {
                        if (now_tick < reconciliation.retry.tick) return null;
                        return try self.claim(entry, .reconcile);
                    },
                    .submitting, .reconciling, .quarantined => return null,
                }
            }
            return null;
        }

        /// Verifies that an adapter retained the exact owned chain returned by
        /// `beginNext`. Completion must never acknowledge a mutated payload.
        pub fn validateClaim(
            self: *const Self,
            work: DeliveryWork,
        ) OutboxError!ClaimValidationResult {
            if (self.recovery_required) return error.RecoveryRequired;
            if (try self.isPrunedToken(work.token, work.token.kind)) return .already_pruned;
            const entry = try self.entryForTokenConst(work.token, work.token.kind);
            if (!self.isCommitted(entry.*)) return error.AwaitingCommit;
            if (!chainsEqual(entry.chain, work.chain)) return error.ClaimPayloadMismatch;
            return .active;
        }

        /// Resolves a completion token before interpreting its receipt. The
        /// returned pointer is canonical for the current entry; a pruned or
        /// retained delivered entry is terminal without needing old payload
        /// bytes that may already have moved out of the inline array.
        pub fn resolveClaim(
            self: *const Self,
            token: ClaimToken,
            expected_kind: WorkKind,
        ) OutboxError!ClaimResolution {
            if (self.recovery_required) return error.RecoveryRequired;
            if (try self.isPrunedToken(token, expected_kind)) return .already_terminal;
            const entry = try self.entryForTokenConst(token, expected_kind);
            if (!self.isCommitted(entry.*)) return error.AwaitingCommit;
            if (std.meta.activeTag(entry.state) == .delivered) return .already_terminal;
            const expected_state: EntryStatus = switch (expected_kind) {
                .submit => .submitting,
                .reconcile => .reconciling,
            };
            if (std.meta.activeTag(entry.state) != expected_state) return error.StaleClaim;
            return .{ .active = &entry.chain };
        }

        pub fn applySubmitOutcome(
            self: *Self,
            token: ClaimToken,
            outcome: SubmitOutcome,
        ) OutboxError!ApplyResult {
            if (self.recovery_required) return error.RecoveryRequired;
            if (try self.isPrunedToken(token, .submit)) {
                return switch (outcome) {
                    .committed => .already_terminal,
                    else => error.StaleClaim,
                };
            }
            const entry = try self.entryForToken(token, .submit);
            if (!self.isCommitted(entry.*)) return error.AwaitingCommit;
            if (std.meta.activeTag(entry.state) == .delivered) {
                return switch (outcome) {
                    .committed => .already_terminal,
                    else => error.StaleClaim,
                };
            }
            if (std.meta.activeTag(entry.state) != .submitting) return error.StaleClaim;

            entry.state = switch (outcome) {
                .committed => .delivered,
                .known_not_sent => |retry| .{ .ready = retry },
                .ambiguous => |retry| .{ .needs_reconciliation = .{
                    .retry = retry,
                    .cause = .ambiguous_submission,
                } },
                .rejected => |retry| .{ .needs_reconciliation = .{
                    .retry = retry,
                    .cause = .rejected_submission,
                } },
                .permanent => |reason| .{ .quarantined = reason },
            };
            return .updated;
        }

        pub fn applyReconcileOutcome(
            self: *Self,
            token: ClaimToken,
            outcome: ReconcileOutcome,
        ) OutboxError!ApplyResult {
            if (self.recovery_required) return error.RecoveryRequired;
            if (try self.isPrunedToken(token, .reconcile)) {
                return switch (outcome) {
                    .all_present_exact => .already_terminal,
                    else => error.StaleClaim,
                };
            }
            const entry = try self.entryForToken(token, .reconcile);
            if (!self.isCommitted(entry.*)) return error.AwaitingCommit;
            if (std.meta.activeTag(entry.state) == .delivered) {
                return switch (outcome) {
                    .all_present_exact => .already_terminal,
                    else => error.StaleClaim,
                };
            }
            const cause = switch (entry.state) {
                .reconciling => |value| value,
                else => return error.StaleClaim,
            };

            entry.state = switch (outcome) {
                .all_present_exact => .delivered,
                .all_absent => |retry| switch (cause) {
                    .ambiguous_submission => .{ .ready = retry },
                    .rejected_submission => .{ .quarantined = .permanent_submission_failure },
                },
                .retryable => |retry| .{ .needs_reconciliation = .{
                    .retry = retry,
                    .cause = cause,
                } },
                .partial_application => .{ .quarantined = .partial_application },
                .transfer_mismatch => .{ .quarantined = .transfer_mismatch },
                .permanent => |reason| .{ .quarantined = reason },
            };
            return .updated;
        }

        /// Makes all nonterminal work immediately eligible in the caller's new
        /// logical-clock domain. A create that may have crossed the process
        /// boundary becomes lookup work and is never blindly repeated.
        pub fn recoverAfterRestart(self: *Self, retry_tick: u64) void {
            self.risk_r2_committed_through = 0;
            self.journal_committed_through = 0;
            self.recovery_required = self.outbox_mode == .two_frontier;
            self.recovery_provenance = .reopened;
            self.fresh_risk_r2_prefix_through = 0;
            self.fresh_journal_prefix_through = 0;
            self.fresh_frontier_gap = false;
            for (self.entries[0..self.len()]) |*entry| {
                switch (entry.state) {
                    .ready => entry.state = .{ .ready = .{ .tick = retry_tick } },
                    .needs_reconciliation => |reconciliation| entry.state = .{ .needs_reconciliation = .{
                        .retry = .{ .tick = retry_tick },
                        .cause = reconciliation.cause,
                    } },
                    .submitting => {
                        entry.state = .{ .needs_reconciliation = .{
                            .retry = .{ .tick = retry_tick },
                            .cause = .ambiguous_submission,
                        } };
                    },
                    .reconciling => |cause| {
                        entry.state = .{ .needs_reconciliation = .{
                            .retry = .{ .tick = retry_tick },
                            .cause = cause,
                        } };
                    },
                    else => {},
                }
            }
        }

        /// Validates the lifecycle's `not_required` recovery path. A fresh
        /// prepared store cannot use a non-zero core frontier merely because no
        /// outbox file was found: it must have observed both live commit marks
        /// as a contiguous prefix from sequence one. Reopened stores establish
        /// their authoritative prefix only through `finishRecovery`.
        pub fn validateRecoveryFrontier(
            self: *const Self,
            common_frontier: PublicationCursor,
        ) OutboxError!void {
            if (self.outbox_mode != .two_frontier) return error.WrongMode;
            if (self.recovery_required) return error.RecoveryRequired;
            if (!common_frontier.domain.eql(self.publication_domain)) {
                return error.PublicationDomainMismatch;
            }

            const proven_through = switch (self.recovery_provenance) {
                .fresh => fresh: {
                    if (self.fresh_frontier_gap) return error.UnprovenRecoveryFrontier;
                    break :fresh @min(
                        self.fresh_risk_r2_prefix_through,
                        self.fresh_journal_prefix_through,
                    );
                },
                .reopened => @min(
                    self.risk_r2_committed_through,
                    self.journal_committed_through,
                ),
            };
            if (proven_through < common_frontier.publication_sequence) {
                return error.UnprovenRecoveryFrontier;
            }
        }

        /// Reconciles a reopened prepared outbox with the common durable core
        /// frontier. Work at or below the frontier is released in memory.
        /// A never-attempted suffix above it is forgotten durably by the file
        /// wrapper; any progressed entry above it is an invariant violation and
        /// leaves the outbox recovery-blocked.
        pub fn finishRecovery(
            self: *Self,
            common_frontier: PublicationCursor,
        ) OutboxError!usize {
            if (self.outbox_mode != .two_frontier) return error.WrongMode;
            if (!self.recovery_required) return error.RecoveryRequired;
            if (!common_frontier.domain.eql(self.publication_domain)) {
                return error.PublicationDomainMismatch;
            }
            if (self.pruned_through != 0) {
                const pruned_identity = settlement.decodeFillGroupId(self.pruned_through) catch
                    return error.InvalidGroupId;
                const pruned_cursor = pruned_identity.publicationCursor() catch
                    return error.InvalidGroupId;
                if (!pruned_cursor.domain.eql(self.publication_domain)) {
                    return error.PublicationDomainMismatch;
                }
                if (pruned_cursor.publication_sequence > common_frontier.publication_sequence) {
                    return error.ProgressedBeyondRecoveryFrontier;
                }
            }

            var first_discard = self.len();
            for (self.entries[0..self.len()], 0..) |entry, index| {
                if (entry.publication_sequence <= common_frontier.publication_sequence) {
                    if (first_discard != self.len()) return error.PublicationSequenceMismatch;
                    continue;
                }
                if (!entryNeverAttempted(entry)) {
                    return error.ProgressedBeyondRecoveryFrontier;
                }
                if (first_discard == self.len()) first_discard = index;
            }

            const removed = self.len() - first_discard;
            self.count = @intCast(first_discard);
            self.high_watermark = if (first_discard == 0)
                self.pruned_through
            else
                self.entries[first_discard - 1].chain.group_id;
            self.risk_r2_committed_through = common_frontier.publication_sequence;
            self.journal_committed_through = common_frontier.publication_sequence;
            self.recovery_required = false;
            return removed;
        }

        /// Reclaims only a fully delivered prefix. The watermark prevents an
        /// already-pruned publication group from being admitted again locally.
        pub fn pruneDeliveredThrough(self: *Self, group_id: u128) OutboxError!usize {
            if (self.recovery_required) return error.RecoveryRequired;
            if (self.outbox_mode == .two_frontier and group_id != 0) {
                const identity = settlement.decodeFillGroupId(group_id) catch return error.InvalidPruneWatermark;
                const cursor = identity.publicationCursor() catch return error.InvalidPruneWatermark;
                if (!cursor.domain.eql(self.publication_domain)) return error.InvalidPruneWatermark;
            }
            if (group_id < self.pruned_through or group_id > self.high_watermark) {
                return error.InvalidPruneWatermark;
            }
            if (group_id == self.pruned_through) return 0;

            var remove_count: usize = 0;
            for (self.entries[0..self.len()]) |entry| {
                if (entry.chain.group_id > group_id) break;
                if (!self.isCommitted(entry)) return error.AwaitingCommit;
                if (std.meta.activeTag(entry.state) != .delivered) return error.UnsettledEntry;
                remove_count += 1;
            }

            const old_len = self.len();
            if (remove_count == 0 or
                self.entries[remove_count - 1].chain.group_id != group_id)
            {
                return error.InvalidPruneWatermark;
            }
            const remaining = old_len - remove_count;
            for (0..remaining) |index| self.entries[index] = self.entries[index + remove_count];
            self.count = @intCast(remaining);
            self.pruned_through = group_id;
            return remove_count;
        }

        pub fn pruneDeliveredPrefix(self: *Self) OutboxError!usize {
            if (self.recovery_required) return error.RecoveryRequired;
            var watermark = self.pruned_through;
            for (self.entries[0..self.len()]) |entry| {
                if (!self.isCommitted(entry)) break;
                if (std.meta.activeTag(entry.state) != .delivered) break;
                watermark = entry.chain.group_id;
            }
            if (watermark == self.pruned_through) return 0;
            return self.pruneDeliveredThrough(watermark);
        }

        pub fn encodedSize(self: *const Self) usize {
            var total = snapshot_header_len + snapshot_digest_len;
            for (self.entries[0..self.len()]) |entry| {
                total += snapshot_entry_header_len + @as(usize, entry.chain.count) * encoded_transfer_len;
            }
            return total;
        }

        pub fn encodeAlloc(self: *const Self, allocator: std.mem.Allocator) (std.mem.Allocator.Error || CodecError)![]u8 {
            const bytes = try allocator.alloc(u8, self.encodedSize());
            errdefer allocator.free(bytes);
            _ = try self.encode(bytes);
            return bytes;
        }

        pub fn encode(self: *const Self, output: []u8) CodecError![]u8 {
            const total = self.encodedSize();
            if (output.len < total) return error.BufferTooSmall;
            var index: usize = 0;

            @memcpy(output[index..][0..snapshot_magic.len], snapshot_magic);
            index += snapshot_magic.len;
            putInt(u16, output, &index, snapshot_version);
            putInt(u32, output, &index, @intCast(capacity));
            putInt(u128, output, &index, @intFromEnum(self.destination_id));
            putInt(u8, output, &index, @intFromEnum(self.outbox_mode));
            putInt(u8, output, &index, self.publication_domain.namespace);
            putInt(u16, output, &index, self.publication_domain.journal_epoch);
            putInt(u32, output, &index, self.count);
            putInt(u128, output, &index, self.pruned_through);
            putInt(u128, output, &index, self.high_watermark);

            for (self.entries[0..self.len()]) |entry| {
                putInt(u128, output, &index, entry.chain.group_id);
                putInt(u64, output, &index, entry.publication_sequence);
                putInt(u8, output, &index, entry.chain.count);
                const state_tag = std.meta.activeTag(entry.state);
                putInt(u8, output, &index, @intFromEnum(state_tag));
                putInt(u8, output, &index, stateCause(entry.state));
                putInt(u64, output, &index, statePayload(entry.state));
                putInt(u32, output, &index, entry.submit_attempts);
                putInt(u64, output, &index, entry.generation);
                for (entry.chain.items()) |transfer| encodeTransfer(output, &index, transfer);
            }

            const digest_offset = total - snapshot_digest_len;
            std.debug.assert(index == digest_offset);
            const digest = sha256(output[0..digest_offset]);
            @memcpy(output[digest_offset..total], &digest);
            return output[0..total];
        }

        pub fn decode(bytes: []const u8, expected_destination: DestinationId) CodecError!Self {
            if (bytes.len < snapshot_header_len + snapshot_digest_len) return error.Truncated;
            const digest_offset = bytes.len - snapshot_digest_len;
            const expected_digest = sha256(bytes[0..digest_offset]);
            if (!std.mem.eql(u8, &expected_digest, bytes[digest_offset..])) return error.DigestMismatch;

            var index: usize = 0;
            if (!std.mem.eql(u8, bytes[index..][0..snapshot_magic.len], snapshot_magic)) return error.InvalidMagic;
            index += snapshot_magic.len;
            if (try takeInt(u16, bytes, &index, digest_offset) != snapshot_version) return error.UnsupportedVersion;
            if (try takeInt(u32, bytes, &index, digest_offset) != capacity) return error.CapacityMismatch;
            const destination_raw = try takeInt(u128, bytes, &index, digest_offset);
            if (destination_raw != @intFromEnum(expected_destination)) return error.DestinationMismatch;
            const mode_raw = try takeInt(u8, bytes, &index, digest_offset);
            const decoded_mode = std.enums.fromInt(OutboxMode, mode_raw) orelse return error.InvalidSnapshotMode;
            const domain: PublicationDomain = .{
                .namespace = try takeInt(u8, bytes, &index, digest_offset),
                .journal_epoch = try takeInt(u16, bytes, &index, digest_offset),
            };
            switch (decoded_mode) {
                .immediate => {
                    if (domain.namespace != 0 or domain.journal_epoch != 0) {
                        return error.InvalidSnapshotDomain;
                    }
                },
                .two_frontier => domain.validate() catch return error.InvalidSnapshotDomain,
            }
            const entry_count = try takeInt(u32, bytes, &index, digest_offset);
            if (entry_count > capacity) return error.EntryCountExceeded;
            const pruned = try takeInt(u128, bytes, &index, digest_offset);
            const high = try takeInt(u128, bytes, &index, digest_offset);
            if (pruned > high) return error.InvalidSnapshotWatermark;
            if (decoded_mode == .two_frontier) {
                for ([_]u128{ pruned, high }) |watermark| {
                    if (watermark == 0) continue;
                    const identity = settlement.decodeFillGroupId(watermark) catch
                        return error.InvalidSnapshotWatermark;
                    const cursor = identity.publicationCursor() catch
                        return error.InvalidSnapshotWatermark;
                    if (!cursor.domain.eql(domain)) return error.InvalidSnapshotDomain;
                }
            }

            var previous_prepared_identity: ?settlement.FillIdentity = null;
            if (decoded_mode == .two_frontier and pruned != 0) {
                previous_prepared_identity = settlement.decodeFillGroupId(pruned) catch
                    return error.InvalidSnapshotWatermark;
            }

            var outbox = switch (decoded_mode) {
                .immediate => Self.init(expected_destination) catch return error.DestinationMismatch,
                .two_frontier => Self.initPrepared(expected_destination, domain) catch return error.InvalidSnapshotDomain,
            };
            outbox.count = entry_count;
            outbox.pruned_through = pruned;
            outbox.high_watermark = high;

            var previous_group = pruned;
            var previous_publication_sequence: u64 = 0;
            for (outbox.entries[0..outbox.len()]) |*entry| {
                const group_id = try takeInt(u128, bytes, &index, digest_offset);
                const publication_sequence = try takeInt(u64, bytes, &index, digest_offset);
                const transfer_count = try takeInt(u8, bytes, &index, digest_offset);
                if (transfer_count > settlement.max_transfers_per_fill) return error.InvalidSnapshotChain;
                const state_raw = try takeInt(u8, bytes, &index, digest_offset);
                const cause_raw = try takeInt(u8, bytes, &index, digest_offset);
                const payload = try takeInt(u64, bytes, &index, digest_offset);
                const attempts = try takeInt(u32, bytes, &index, digest_offset);
                const generation = try takeInt(u64, bytes, &index, digest_offset);
                if (group_id <= previous_group or group_id > high) return error.InvalidSnapshotOrder;

                switch (decoded_mode) {
                    .immediate => if (publication_sequence != 0) return error.InvalidSnapshotPublication,
                    .two_frontier => {
                        if (publication_sequence == 0 or publication_sequence < previous_publication_sequence) {
                            return error.InvalidSnapshotPublication;
                        }
                        const identity = settlement.decodeFillGroupId(group_id) catch
                            return error.InvalidSnapshotPublication;
                        const cursor = identity.publicationCursor() catch
                            return error.InvalidSnapshotPublication;
                        if (!cursor.domain.eql(domain) or cursor.publication_sequence != publication_sequence) {
                            return error.InvalidSnapshotPublication;
                        }
                        if (previous_prepared_identity) |previous_identity| {
                            if (previous_identity.publication_sequence.toInt() == publication_sequence) {
                                const expected_ordinal = std.math.add(
                                    u32,
                                    previous_identity.fill_ordinal,
                                    1,
                                ) catch return error.InvalidSnapshotPublication;
                                if (identity.fill_ordinal != expected_ordinal) {
                                    return error.InvalidSnapshotPublication;
                                }
                            } else if (identity.fill_ordinal != 0) {
                                return error.InvalidSnapshotPublication;
                            }
                        } else if (identity.fill_ordinal != 0) {
                            return error.InvalidSnapshotPublication;
                        }
                        previous_prepared_identity = identity;
                    },
                }

                var chain: settlement.TransferChain = .{
                    .group_id = group_id,
                    .storage = [_]settlement.Transfer{zeroTransfer()} ** settlement.max_transfers_per_fill,
                    .count = transfer_count,
                };
                for (chain.storage[0..@as(usize, transfer_count)]) |*transfer| {
                    transfer.* = try decodeTransfer(bytes, &index, digest_offset);
                }
                validateChain(chain) catch return error.InvalidSnapshotChain;
                if (decoded_mode == .two_frontier) {
                    const identity = settlement.decodeFillGroupId(group_id) catch
                        return error.InvalidSnapshotChain;
                    validatePreparedMapperChain(identity, chain) catch
                        return error.InvalidSnapshotChain;
                }
                const state = try decodeState(state_raw, cause_raw, payload);
                try validateDecodedEntry(transfer_count, state, attempts, generation);
                entry.* = .{
                    .chain = chain,
                    .publication_sequence = publication_sequence,
                    .state = state,
                    .submit_attempts = attempts,
                    .generation = generation,
                };
                previous_group = group_id;
                previous_publication_sequence = publication_sequence;
            }
            if (index != digest_offset) return error.TrailingBytes;
            if (outbox.len() == 0 and high != pruned) return error.InvalidSnapshotWatermark;
            if (outbox.len() != 0 and previous_group != high) return error.InvalidSnapshotWatermark;
            outbox.recoverAfterRestart(0);
            return outbox;
        }

        fn prepareOne(
            self: *Self,
            record: PreparedSettlementRecord,
        ) OutboxError!EnqueueResult {
            if (record.destination_id != self.destination_id) return error.WrongDestination;
            try self.validatePreparedCursor(record.publication);

            if (record.chain.group_id == 0) return error.InvalidGroupId;
            if (record.chain.count > settlement.max_transfers_per_fill) {
                return error.InvalidTransferCount;
            }
            const owned_chain = canonicalizeChain(record.chain);
            try validateChain(owned_chain);
            const identity = settlement.decodeFillGroupId(owned_chain.group_id) catch
                return error.InvalidGroupId;
            const mapped_cursor = identity.publicationCursor() catch
                return error.InvalidGroupId;
            if (!mapped_cursor.domain.eql(record.publication.domain)) {
                return error.PublicationDomainMismatch;
            }
            if (mapped_cursor.publication_sequence != record.publication.publication_sequence) {
                return error.PublicationSequenceMismatch;
            }
            try validatePreparedMapperChain(identity, owned_chain);

            if (self.findIndex(owned_chain.group_id)) |index| {
                const existing = self.entries[index];
                if (existing.publication_sequence != record.publication.publication_sequence or
                    !chainsEqual(existing.chain, owned_chain))
                {
                    return error.ConflictingDuplicate;
                }
                return .duplicate;
            }
            if (owned_chain.group_id <= self.pruned_through) return .already_finalized;
            // Preparing a new fill after its R2 effects are committed can no
            // longer be made atomic with that core mutation. Journal-first is
            // intentionally allowed so replay can reconstruct missing suffix
            // records before R2 is advanced.
            if (self.risk_r2_committed_through >= record.publication.publication_sequence) {
                return error.PreparedAfterCommit;
            }
            if (owned_chain.group_id <= self.high_watermark) return error.NonMonotonicGroupId;
            if (self.len() == capacity) return error.OutboxFull;

            self.entries[self.len()] = .{
                .chain = owned_chain,
                .publication_sequence = record.publication.publication_sequence,
                .state = if (owned_chain.count == 0) .delivered else .{ .ready = .{ .tick = 0 } },
            };
            self.count += 1;
            self.high_watermark = owned_chain.group_id;
            return .inserted;
        }

        fn validatePreparedCursor(
            self: *const Self,
            cursor: PublicationCursor,
        ) OutboxError!void {
            cursor.domain.validate() catch return error.InvalidPublicationDomain;
            if (cursor.publication_sequence == 0) return error.InvalidPublicationSequence;
            if (!cursor.domain.eql(self.publication_domain)) {
                return error.PublicationDomainMismatch;
            }
        }

        fn acceptsLiveFrontier(self: *const Self, frontier: PublicationCursor) bool {
            return self.outbox_mode == .two_frontier and
                !self.recovery_required and
                frontier.publication_sequence != 0 and
                frontier.domain.eql(self.publication_domain);
        }

        fn observeFreshFrontier(
            self: *Self,
            prefix_through: *u64,
            frontier: PublicationCursor,
        ) void {
            if (self.recovery_provenance != .fresh or self.fresh_frontier_gap) return;
            if (frontier.publication_sequence <= prefix_through.*) return;
            const next = std.math.add(u64, prefix_through.*, 1) catch {
                self.fresh_frontier_gap = true;
                return;
            };
            if (frontier.publication_sequence != next) {
                self.fresh_frontier_gap = true;
                return;
            }
            prefix_through.* = frontier.publication_sequence;
        }

        fn isCommitted(self: *const Self, entry: Entry) bool {
            return switch (self.outbox_mode) {
                .immediate => true,
                .two_frontier => self.risk_r2_committed_through >= entry.publication_sequence and
                    self.journal_committed_through >= entry.publication_sequence,
            };
        }

        fn cursorFor(self: *const Self, entry: Entry) PublicationCursor {
            std.debug.assert(self.outbox_mode == .two_frontier);
            return .{
                .domain = self.publication_domain,
                .publication_sequence = entry.publication_sequence,
            };
        }

        fn claim(self: *Self, entry: *Entry, kind: WorkKind) OutboxError!DeliveryWork {
            const generation = std.math.add(u64, entry.generation, 1) catch return error.GenerationOverflow;
            if (kind == .submit) {
                entry.submit_attempts = std.math.add(u32, entry.submit_attempts, 1) catch return error.AttemptOverflow;
                entry.state = .submitting;
            } else {
                const cause = switch (entry.state) {
                    .needs_reconciliation => |reconciliation| reconciliation.cause,
                    else => unreachable,
                };
                entry.state = .{ .reconciling = cause };
            }
            entry.generation = generation;
            return .{
                .token = .{
                    .destination_id = self.destination_id,
                    .group_id = entry.chain.group_id,
                    .generation = generation,
                    .kind = kind,
                },
                .chain = entry.chain,
            };
        }

        fn entryForToken(self: *Self, token: ClaimToken, expected_kind: WorkKind) OutboxError!*Entry {
            if (token.destination_id != self.destination_id) return error.WrongDestination;
            if (token.kind != expected_kind) return error.WrongWorkKind;
            const index = self.findIndex(token.group_id) orelse return error.UnknownGroup;
            const entry = &self.entries[index];
            if (entry.generation != token.generation) return error.StaleClaim;
            return entry;
        }

        fn entryForTokenConst(
            self: *const Self,
            token: ClaimToken,
            expected_kind: WorkKind,
        ) OutboxError!*const Entry {
            if (token.destination_id != self.destination_id) return error.WrongDestination;
            if (token.kind != expected_kind) return error.WrongWorkKind;
            const index = self.findIndex(token.group_id) orelse return error.UnknownGroup;
            const entry = &self.entries[index];
            if (entry.generation != token.generation) return error.StaleClaim;
            return entry;
        }

        fn isPrunedToken(self: *const Self, token: ClaimToken, expected_kind: WorkKind) OutboxError!bool {
            if (token.destination_id != self.destination_id) return error.WrongDestination;
            if (token.kind != expected_kind) return error.WrongWorkKind;
            return self.pruned_through != 0 and token.group_id <= self.pruned_through;
        }

        fn findIndex(self: *const Self, group_id: u128) ?usize {
            for (self.entries[0..self.len()], 0..) |entry, index| {
                if (entry.chain.group_id == group_id) return index;
            }
            return null;
        }
    };
}

/// Atomic file-backed wrapper around `SettlementOutbox`.
///
/// Every mutating API first builds a candidate state, durably replaces the
/// snapshot, and only then publishes it in memory. A write error poisons the
/// handle because the atomic replace or directory sync may already have
/// committed; callers must close and reopen before doing more work.
pub fn DurableFileOutbox(comptime capacity: usize) type {
    return struct {
        const Self = @This();
        pub const Core = SettlementOutbox(capacity);

        allocator: std.mem.Allocator,
        dir: std.Io.Dir,
        path: []u8,
        lock_path: []u8,
        lock_file: std.Io.File,
        core: Core,
        poisoned: bool = false,
        fail_next_write_for_testing: bool = false,
        fail_next_post_replace_for_testing: bool = false,

        pub fn open(
            allocator: std.mem.Allocator,
            dir: std.Io.Dir,
            path: []const u8,
            destination_id: DestinationId,
        ) StoreError!Self {
            return openMode(
                allocator,
                dir,
                path,
                destination_id,
                .immediate,
                .{ .namespace = 0, .journal_epoch = 0 },
            );
        }

        pub fn openPrepared(
            allocator: std.mem.Allocator,
            dir: std.Io.Dir,
            path: []const u8,
            destination_id: DestinationId,
            domain: PublicationDomain,
        ) StoreError!Self {
            domain.validate() catch return error.InvalidPublicationDomain;
            return openMode(allocator, dir, path, destination_id, .two_frontier, domain);
        }

        fn openMode(
            allocator: std.mem.Allocator,
            dir: std.Io.Dir,
            path: []const u8,
            destination_id: DestinationId,
            expected_mode: OutboxMode,
            expected_domain: PublicationDomain,
        ) StoreError!Self {
            const path_copy = try allocator.dupe(u8, path);
            errdefer allocator.free(path_copy);
            const lock_path = try std.fmt.allocPrint(allocator, "{s}.lock", .{path});
            errdefer allocator.free(lock_path);
            const lock_file = dir.createFile(io_context.current(), lock_path, .{
                .read = true,
                .truncate = false,
                .lock = .exclusive,
                .lock_nonblocking = true,
            }) catch |err| switch (err) {
                error.WouldBlock => return error.AlreadyOpen,
                else => return error.StorageReadFailed,
            };
            errdefer lock_file.close(io_context.current());

            var core = switch (expected_mode) {
                .immediate => Core.init(destination_id) catch |err| return err,
                .two_frontier => Core.initPrepared(destination_id, expected_domain) catch |err| return err,
            };
            const file = dir.openFile(io_context.current(), path, .{}) catch |err| switch (err) {
                error.FileNotFound => null,
                else => return error.StorageReadFailed,
            };
            if (file) |opened| {
                defer opened.close(io_context.current());
                const bytes = io_context.readToEndAlloc(opened, allocator, Core.max_encoded_size + 1) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    else => return error.StorageReadFailed,
                };
                defer allocator.free(bytes);
                if (bytes.len > Core.max_encoded_size) return error.StorageReadFailed;
                core = try Core.decode(bytes, destination_id);
                if (core.mode() != expected_mode) return error.SnapshotModeMismatch;
                if (expected_mode == .two_frontier and
                    !core.publication_domain.eql(expected_domain))
                {
                    return error.SnapshotDomainMismatch;
                }
            }

            return .{
                .allocator = allocator,
                .dir = dir,
                .path = path_copy,
                .lock_path = lock_path,
                .lock_file = lock_file,
                .core = core,
            };
        }

        pub fn deinit(self: *Self) void {
            self.lock_file.close(io_context.current());
            self.allocator.free(self.lock_path);
            self.allocator.free(self.path);
            self.* = undefined;
        }

        pub fn isPoisoned(self: *const Self) bool {
            return self.poisoned;
        }

        pub fn destinationId(self: *const Self) DestinationId {
            return self.core.destinationId();
        }

        pub fn mode(self: *const Self) OutboxMode {
            return self.core.mode();
        }

        pub fn publicationDomain(self: *const Self) ?PublicationDomain {
            return self.core.publicationDomain();
        }

        pub fn isRecoveryRequired(self: *const Self) bool {
            return self.core.isRecoveryRequired();
        }

        pub fn recoveryProvenance(self: *const Self) RecoveryProvenance {
            return self.core.recoveryProvenance();
        }

        pub fn len(self: *const Self) usize {
            return self.core.len();
        }

        pub fn status(self: *const Self, group_id: u128) ?EntryStatus {
            return self.core.status(group_id);
        }

        pub fn dispatchState(self: *const Self) DispatchState {
            return self.core.dispatchState();
        }

        pub fn injectWriteFailureForTesting(self: *Self) void {
            self.fail_next_write_for_testing = true;
        }

        /// Injects ambiguity after the atomic replacement is visible but
        /// before its parent-directory durability is confirmed.
        pub fn injectPostReplaceFailureForTesting(self: *Self) void {
            self.fail_next_post_replace_for_testing = true;
        }

        pub fn enqueue(self: *Self, record: SettlementRecord) StoreError!EnqueueResult {
            try self.ensureHealthy();
            var candidate = self.core;
            const result = try candidate.enqueue(record);
            switch (result) {
                .inserted, .empty_finalized => try self.commit(candidate),
                .duplicate, .already_finalized => {},
            }
            return result;
        }

        /// Persists the complete per-command candidate in one atomic replace.
        pub fn prepare(
            self: *Self,
            records: []const PreparedSettlementRecord,
        ) StoreError!PrepareBatchResult {
            try self.ensureHealthy();
            var candidate = self.core;
            const result = try candidate.prepare(records);
            if (result.inserted != 0) try self.commit(candidate);
            return result;
        }

        /// Persists replay reconstruction without opening the dispatch fence.
        /// Exact duplicates need no rewrite; any newly reconstructed records
        /// are committed as one atomic candidate just like live preparation.
        pub fn prepareRecovery(
            self: *Self,
            records: []const PreparedSettlementRecord,
        ) StoreError!PrepareBatchResult {
            try self.ensureHealthy();
            var candidate = self.core;
            const result = try candidate.prepareRecovery(records);
            if (result.inserted != 0) try self.commit(candidate);
            return result;
        }

        pub fn markRiskR2CommittedThrough(self: *Self, frontier: PublicationCursor) void {
            if (self.poisoned) return;
            self.core.markRiskR2CommittedThrough(frontier);
        }

        pub fn markJournalCommittedThrough(self: *Self, frontier: PublicationCursor) void {
            if (self.poisoned) return;
            self.core.markJournalCommittedThrough(frontier);
        }

        pub fn finishRecovery(
            self: *Self,
            common_frontier: PublicationCursor,
        ) StoreError!usize {
            try self.ensureHealthy();
            var candidate = self.core;
            const removed = try candidate.finishRecovery(common_frontier);
            if (removed != 0) {
                try self.commit(candidate);
            } else {
                self.core = candidate;
            }
            return removed;
        }

        pub fn validateRecoveryFrontier(
            self: *const Self,
            common_frontier: PublicationCursor,
        ) StoreError!void {
            try self.ensureHealthy();
            return self.core.validateRecoveryFrontier(common_frontier);
        }

        /// Persists the in-flight marker before returning work to the adapter.
        pub fn beginNext(self: *Self, now_tick: u64) StoreError!?DeliveryWork {
            try self.ensureHealthy();
            var candidate = self.core;
            const work = try candidate.beginNext(now_tick) orelse return null;
            try self.commit(candidate);
            return work;
        }

        pub fn validateClaim(
            self: *const Self,
            work: DeliveryWork,
        ) StoreError!ClaimValidationResult {
            try self.ensureHealthy();
            return self.core.validateClaim(work);
        }

        pub fn resolveClaim(
            self: *const Self,
            token: ClaimToken,
            expected_kind: WorkKind,
        ) StoreError!ClaimResolution {
            try self.ensureHealthy();
            return self.core.resolveClaim(token, expected_kind);
        }

        pub fn applySubmitOutcome(
            self: *Self,
            token: ClaimToken,
            outcome: SubmitOutcome,
        ) StoreError!ApplyResult {
            try self.ensureHealthy();
            var candidate = self.core;
            const result = try candidate.applySubmitOutcome(token, outcome);
            if (result == .updated) try self.commit(candidate);
            return result;
        }

        pub fn applyReconcileOutcome(
            self: *Self,
            token: ClaimToken,
            outcome: ReconcileOutcome,
        ) StoreError!ApplyResult {
            try self.ensureHealthy();
            var candidate = self.core;
            const result = try candidate.applyReconcileOutcome(token, outcome);
            if (result == .updated) try self.commit(candidate);
            return result;
        }

        pub fn pruneDeliveredThrough(self: *Self, group_id: u128) StoreError!usize {
            try self.ensureHealthy();
            var candidate = self.core;
            const previous = candidate.pruned_through;
            const removed = try candidate.pruneDeliveredThrough(group_id);
            if (candidate.pruned_through != previous) try self.commit(candidate);
            return removed;
        }

        pub fn pruneDeliveredPrefix(self: *Self) StoreError!usize {
            try self.ensureHealthy();
            var candidate = self.core;
            const removed = try candidate.pruneDeliveredPrefix();
            if (removed != 0) try self.commit(candidate);
            return removed;
        }

        fn ensureHealthy(self: *const Self) StoreError!void {
            if (self.poisoned) return error.Poisoned;
        }

        fn commit(self: *Self, candidate: Core) StoreError!void {
            const bytes = try candidate.encodeAlloc(self.allocator);
            defer self.allocator.free(bytes);
            if (self.fail_next_write_for_testing) {
                self.fail_next_write_for_testing = false;
                self.poisoned = true;
                return error.StorageAmbiguous;
            }
            const fail_after_replace = self.fail_next_post_replace_for_testing;
            self.fail_next_post_replace_for_testing = false;
            writeBytesAtomic(self.dir, self.path, bytes, fail_after_replace) catch {
                self.poisoned = true;
                return error.StorageAmbiguous;
            };
            self.core = candidate;
        }
    };
}

fn validateChain(chain: settlement.TransferChain) OutboxError!void {
    if (chain.group_id == 0) return error.InvalidGroupId;
    if (chain.count > settlement.max_transfers_per_fill) return error.InvalidTransferCount;
    const transfers = chain.items();
    for (transfers, 0..) |transfer, index| {
        if (transfer.id == 0 or transfer.id == std.math.maxInt(u128) or
            transfer.debit_account_id == 0 or transfer.debit_account_id == std.math.maxInt(u128) or
            transfer.credit_account_id == 0 or transfer.credit_account_id == std.math.maxInt(u128) or
            transfer.debit_account_id == transfer.credit_account_id or transfer.amount == 0 or
            transfer.pending_id != 0 or transfer.timeout != 0 or transfer.timestamp != 0 or
            transfer.ledger == 0 or transfer.code == 0 or transfer.user_data_128 != chain.group_id)
        {
            return error.InvalidTransfer;
        }
        const expected_flags: u16 = if (index + 1 < transfers.len) settlement.linked_flag else 0;
        if (transfer.flags != expected_flags) return error.InvalidLinkedFlags;
        for (transfers[0..index]) |previous| {
            if (previous.id == transfer.id) return error.InvalidTransfer;
        }
    }
}

/// Prepared admission accepts only the exact mapper-owned ID and user-data
/// layout. Immediate/manual mode deliberately retains its broader chain
/// contract for compatibility with caller-defined transfer metadata.
fn validatePreparedMapperChain(
    identity: settlement.FillIdentity,
    chain: settlement.TransferChain,
) OutboxError!void {
    for (chain.items()) |transfer| {
        if (transfer.user_data_64 != 0 or transfer.user_data_32 != 0) {
            return error.InvalidTransfer;
        }
        const leg = std.enums.fromInt(
            settlement.SettlementLeg,
            @as(u8, @truncate(transfer.id)),
        ) orelse return error.InvalidTransfer;
        const expected_id = identity.transferId(leg) catch return error.InvalidTransfer;
        if (transfer.id != expected_id) return error.InvalidTransfer;
    }
}

fn canonicalizeChain(chain: settlement.TransferChain) settlement.TransferChain {
    var canonical: settlement.TransferChain = .{
        .group_id = chain.group_id,
        .storage = [_]settlement.Transfer{zeroTransfer()} ** settlement.max_transfers_per_fill,
        .count = chain.count,
    };
    for (chain.items(), 0..) |transfer, index| canonical.storage[index] = transfer;
    return canonical;
}

fn zeroTransfer() settlement.Transfer {
    return std.mem.zeroes(settlement.Transfer);
}

fn chainsEqual(a: settlement.TransferChain, b: settlement.TransferChain) bool {
    if (a.group_id != b.group_id or a.count != b.count) return false;
    for (a.items(), b.items()) |left, right| {
        if (!std.meta.eql(left, right)) return false;
    }
    return true;
}

fn statePayload(state: EntryState) u64 {
    return switch (state) {
        .ready => |retry| retry.tick,
        .needs_reconciliation => |reconciliation| reconciliation.retry.tick,
        .quarantined => |reason| @intFromEnum(reason),
        .submitting, .reconciling, .delivered => 0,
    };
}

fn stateCause(state: EntryState) u8 {
    return switch (state) {
        .needs_reconciliation => |reconciliation| @intFromEnum(reconciliation.cause),
        .reconciling => |cause| @intFromEnum(cause),
        else => 0,
    };
}

fn decodeState(tag_raw: u8, cause_raw: u8, payload: u64) CodecError!EntryState {
    const tag = std.enums.fromInt(EntryStatus, tag_raw) orelse return error.InvalidState;
    return switch (tag) {
        .ready => if (cause_raw == 0) .{ .ready = .{ .tick = payload } } else error.InvalidState,
        .submitting => if (cause_raw == 0 and payload == 0) .submitting else error.InvalidState,
        .needs_reconciliation => .{ .needs_reconciliation = .{
            .retry = .{ .tick = payload },
            .cause = std.enums.fromInt(ReconciliationCause, cause_raw) orelse return error.InvalidState,
        } },
        .reconciling => if (payload == 0) .{
            .reconciling = std.enums.fromInt(ReconciliationCause, cause_raw) orelse return error.InvalidState,
        } else error.InvalidState,
        .delivered => if (cause_raw == 0 and payload == 0) .delivered else error.InvalidState,
        .quarantined => blk: {
            if (cause_raw != 0) return error.InvalidState;
            const raw = std.math.cast(u8, payload) orelse return error.InvalidState;
            const reason = std.enums.fromInt(QuarantineReason, raw) orelse return error.InvalidState;
            break :blk .{ .quarantined = reason };
        },
    };
}

fn validateDecodedEntry(
    transfer_count: u8,
    state: EntryState,
    submit_attempts: u32,
    generation: u64,
) CodecError!void {
    const status = std.meta.activeTag(state);
    if (submit_attempts > generation) return error.InvalidState;
    if (transfer_count == 0) {
        if (status != .delivered or submit_attempts != 0 or generation != 0) return error.InvalidState;
        return;
    }

    switch (status) {
        .ready => {
            const initial = submit_attempts == 0 and generation == 0;
            const retried = submit_attempts > 0 and generation > 0;
            if (!initial and !retried) return error.InvalidState;
        },
        .submitting, .needs_reconciliation, .reconciling, .delivered, .quarantined => {
            if (submit_attempts == 0 or generation == 0) return error.InvalidState;
        },
    }
}

fn encodeTransfer(bytes: []u8, index: *usize, transfer: settlement.Transfer) void {
    putInt(u128, bytes, index, transfer.id);
    putInt(u128, bytes, index, transfer.debit_account_id);
    putInt(u128, bytes, index, transfer.credit_account_id);
    putInt(u128, bytes, index, transfer.amount);
    putInt(u128, bytes, index, transfer.pending_id);
    putInt(u128, bytes, index, transfer.user_data_128);
    putInt(u64, bytes, index, transfer.user_data_64);
    putInt(u32, bytes, index, transfer.user_data_32);
    putInt(u32, bytes, index, transfer.timeout);
    putInt(u32, bytes, index, transfer.ledger);
    putInt(u16, bytes, index, transfer.code);
    putInt(u16, bytes, index, transfer.flags);
    putInt(u64, bytes, index, transfer.timestamp);
}

fn decodeTransfer(bytes: []const u8, index: *usize, limit: usize) CodecError!settlement.Transfer {
    return .{
        .id = try takeInt(u128, bytes, index, limit),
        .debit_account_id = try takeInt(u128, bytes, index, limit),
        .credit_account_id = try takeInt(u128, bytes, index, limit),
        .amount = try takeInt(u128, bytes, index, limit),
        .pending_id = try takeInt(u128, bytes, index, limit),
        .user_data_128 = try takeInt(u128, bytes, index, limit),
        .user_data_64 = try takeInt(u64, bytes, index, limit),
        .user_data_32 = try takeInt(u32, bytes, index, limit),
        .timeout = try takeInt(u32, bytes, index, limit),
        .ledger = try takeInt(u32, bytes, index, limit),
        .code = try takeInt(u16, bytes, index, limit),
        .flags = try takeInt(u16, bytes, index, limit),
        .timestamp = try takeInt(u64, bytes, index, limit),
    };
}

fn putInt(comptime T: type, bytes: []u8, index: *usize, value: T) void {
    std.mem.writeInt(T, bytes[index.*..][0..@sizeOf(T)], value, .little);
    index.* += @sizeOf(T);
}

fn takeInt(
    comptime T: type,
    bytes: []const u8,
    index: *usize,
    limit: usize,
) CodecError!T {
    if (index.* > limit or limit - index.* < @sizeOf(T)) return error.Truncated;
    const value = std.mem.readInt(T, bytes[index.*..][0..@sizeOf(T)], .little);
    index.* += @sizeOf(T);
    return value;
}

fn sha256(bytes: []const u8) [snapshot_digest_len]u8 {
    var digest: [snapshot_digest_len]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return digest;
}

fn writeBytesAtomic(
    dir: std.Io.Dir,
    path: []const u8,
    bytes: []const u8,
    fail_after_replace: bool,
) !void {
    var atomic_file = try dir.createFileAtomic(io_context.current(), path, .{ .replace = true });
    defer atomic_file.deinit(io_context.current());
    const file = atomic_file.file;
    var buffer: [4096]u8 = undefined;
    var file_writer = file.writer(io_context.current(), &buffer);
    try file_writer.interface.writeAll(bytes);
    try file_writer.interface.flush();
    try file.sync(io_context.current());
    try atomic_file.replace(io_context.current());
    if (fail_after_replace) return error.InjectedPostReplaceFailure;
    try persistence_paths.syncParentDirectory(io_context.current(), dir, path);
}

fn testDestination() DestinationId {
    return @enumFromInt(77);
}

fn testRecord(chain: settlement.TransferChain) SettlementRecord {
    return .{ .destination_id = testDestination(), .chain = chain };
}

fn testPublicationDomain() PublicationDomain {
    return .{ .namespace = 0xA5, .journal_epoch = 17 };
}

fn testPublicationCursor(sequence: u64) PublicationCursor {
    return .{
        .domain = testPublicationDomain(),
        .publication_sequence = sequence,
    };
}

fn testPreparedChain(
    sequence: u64,
    fill_ordinal: u32,
    transfer_seed: u128,
    count: u8,
) !settlement.TransferChain {
    const identity: settlement.FillIdentity = .{
        .namespace = testPublicationDomain().namespace,
        .journal_epoch = testPublicationDomain().journal_epoch,
        .publication_sequence = publication.PublicationSequence.fromInt(sequence),
        .fill_ordinal = fill_ordinal,
    };
    var chain = testChain(try identity.groupId(), transfer_seed, count);
    const legs = [_]settlement.SettlementLeg{
        .quote_principal,
        .base_principal,
        .maker_fee,
        .taker_fee,
    };
    for (chain.storage[0..@as(usize, count)], 0..) |*transfer, index| {
        transfer.id = try identity.transferId(legs[index]);
    }
    return chain;
}

fn testPreparedRecord(
    sequence: u64,
    fill_ordinal: u32,
    transfer_seed: u128,
    count: u8,
) !PreparedSettlementRecord {
    return .{
        .destination_id = testDestination(),
        .publication = testPublicationCursor(sequence),
        .chain = try testPreparedChain(sequence, fill_ordinal, transfer_seed, count),
    };
}

fn testChain(group_id: u128, transfer_seed: u128, count: u8) settlement.TransferChain {
    var chain: settlement.TransferChain = .{ .group_id = group_id, .count = count };
    for (chain.storage[0..@as(usize, count)], 0..) |*transfer, index| {
        transfer.* = .{
            .id = transfer_seed + index + 1,
            .debit_account_id = 1000 + index * 2,
            .credit_account_id = 1001 + index * 2,
            .amount = 10 + index,
            .user_data_128 = group_id,
            .ledger = if (index + 1 == count) 2 else 1,
            .code = @intCast(300 + index),
            .flags = if (index + 1 < count) settlement.linked_flag else 0,
        };
    }
    return chain;
}

test "settlement outbox owns chains and enforces bounded monotonic admission" {
    const Outbox = SettlementOutbox(2);
    var outbox = try Outbox.init(testDestination());
    var first = testChain(100, 1000, 2);
    const original_amount = first.storage[0].amount;
    try std.testing.expectError(error.WrongDestination, outbox.enqueue(.{
        .destination_id = @enumFromInt(78),
        .chain = first,
    }));
    try std.testing.expectEqual(@as(usize, 0), outbox.len());
    try std.testing.expectEqual(EnqueueResult.inserted, try outbox.enqueue(testRecord(first)));
    try std.testing.expectEqual(EnqueueResult.duplicate, try outbox.enqueue(testRecord(first)));

    var conflict = first;
    conflict.storage[0].amount += 1;
    try std.testing.expectError(error.ConflictingDuplicate, outbox.enqueue(testRecord(conflict)));
    first.storage[0].amount = 9999;

    try std.testing.expectEqual(EnqueueResult.inserted, try outbox.enqueue(testRecord(testChain(101, 2000, 1))));
    try std.testing.expectError(error.OutboxFull, outbox.enqueue(testRecord(testChain(102, 3000, 1))));
    const work = (try outbox.beginNext(0)).?;
    try std.testing.expectEqual(WorkKind.submit, work.token.kind);
    try std.testing.expectEqual(@as(u128, 100), work.token.group_id);
    try std.testing.expectEqual(original_amount, work.chain.storage[0].amount);
    for (work.chain.storage[@as(usize, work.chain.count)..]) |unused| {
        try std.testing.expectEqualDeep(zeroTransfer(), unused);
    }
    try std.testing.expect((try outbox.beginNext(0)) == null);
}

test "settlement outbox retries only known absent work and reconciles ambiguity" {
    const Outbox = SettlementOutbox(2);
    var outbox = try Outbox.init(testDestination());
    const chain = testChain(200, 4000, 3);
    _ = try outbox.enqueue(testRecord(chain));

    const first = (try outbox.beginNext(0)).?;
    try std.testing.expectEqual(ApplyResult.updated, try outbox.applySubmitOutcome(
        first.token,
        .{ .known_not_sent = .{ .tick = 10 } },
    ));
    try std.testing.expect((try outbox.beginNext(9)) == null);
    const second = (try outbox.beginNext(10)).?;
    try std.testing.expect(chainsEqual(chain, second.chain));
    try std.testing.expectEqual(ApplyResult.updated, try outbox.applySubmitOutcome(
        second.token,
        .{ .ambiguous = .{ .tick = 12 } },
    ));
    try std.testing.expect((try outbox.beginNext(11)) == null);

    const lookup = (try outbox.beginNext(12)).?;
    try std.testing.expectEqual(WorkKind.reconcile, lookup.token.kind);
    try std.testing.expectEqual(ApplyResult.updated, try outbox.applyReconcileOutcome(
        lookup.token,
        .{ .all_absent = .{ .tick = 14 } },
    ));
    const third = (try outbox.beginNext(14)).?;
    try std.testing.expect(chainsEqual(chain, third.chain));
    try std.testing.expectEqual(ApplyResult.updated, try outbox.applySubmitOutcome(third.token, .committed));
    try std.testing.expectEqual(ApplyResult.already_terminal, try outbox.applySubmitOutcome(third.token, .committed));
    try std.testing.expectEqual(EntryStatus.delivered, outbox.status(200).?);
    try std.testing.expectEqual(@as(u32, 3), outbox.submitAttempts(200).?);
    _ = try outbox.pruneDeliveredThrough(200);
    try std.testing.expectEqual(ApplyResult.already_terminal, try outbox.applySubmitOutcome(third.token, .committed));
}

test "settlement outbox quarantines a rejected chain proven absent after reopen" {
    const allocator = std.testing.allocator;
    const Outbox = SettlementOutbox(2);
    var outbox = try Outbox.init(testDestination());
    _ = try outbox.enqueue(testRecord(testChain(250, 4500, 2)));

    const submit = (try outbox.beginNext(0)).?;
    _ = try outbox.applySubmitOutcome(submit.token, .{ .rejected = .{ .tick = 7 } });

    const image = try outbox.encodeAlloc(allocator);
    defer allocator.free(image);
    var restored = try Outbox.decode(image, testDestination());
    const lookup = (try restored.beginNext(0)).?;
    try std.testing.expectEqual(WorkKind.reconcile, lookup.token.kind);
    _ = try restored.applyReconcileOutcome(lookup.token, .{ .all_absent = .{ .tick = 8 } });
    try std.testing.expectEqual(EntryStatus.quarantined, restored.status(250).?);
    try std.testing.expect((try restored.beginNext(std.math.maxInt(u64))) == null);
}

test "settlement outbox quarantines partial reconciliation and blocks overtaking" {
    const Outbox = SettlementOutbox(3);
    var outbox = try Outbox.init(testDestination());
    _ = try outbox.enqueue(testRecord(testChain(300, 5000, 2)));
    _ = try outbox.enqueue(testRecord(testChain(301, 6000, 1)));

    const submit = (try outbox.beginNext(0)).?;
    _ = try outbox.applySubmitOutcome(submit.token, .{ .ambiguous = .{ .tick = 0 } });
    const lookup = (try outbox.beginNext(0)).?;
    _ = try outbox.applyReconcileOutcome(lookup.token, .partial_application);
    try std.testing.expectEqual(EntryStatus.quarantined, outbox.status(300).?);
    try std.testing.expect((try outbox.beginNext(std.math.maxInt(u64))) == null);
}

test "settlement outbox rejects a stale submit claim after reconciliation starts" {
    const Outbox = SettlementOutbox(2);
    var outbox = try Outbox.init(testDestination());
    _ = try outbox.enqueue(testRecord(testChain(350, 6500, 1)));

    const submit = (try outbox.beginNext(0)).?;
    _ = try outbox.applySubmitOutcome(submit.token, .{ .ambiguous = .{ .tick = 0 } });
    const lookup = (try outbox.beginNext(0)).?;
    try std.testing.expect(lookup.token.generation > submit.token.generation);
    try std.testing.expectError(error.StaleClaim, outbox.applySubmitOutcome(submit.token, .committed));
    _ = try outbox.applyReconcileOutcome(lookup.token, .all_present_exact);
}

test "settlement outbox codec detects corruption and recovers in flight as lookup" {
    const allocator = std.testing.allocator;
    const Outbox = SettlementOutbox(4);
    var outbox = try Outbox.init(testDestination());
    const chain = testChain(400, 7000, 4);
    _ = try outbox.enqueue(testRecord(chain));
    const submit = (try outbox.beginNext(0)).?;
    try std.testing.expectEqual(WorkKind.submit, submit.token.kind);

    const encoded = try outbox.encodeAlloc(allocator);
    defer allocator.free(encoded);
    var restored = try Outbox.decode(encoded, testDestination());
    try std.testing.expectEqual(EntryStatus.needs_reconciliation, restored.status(400).?);
    const lookup = (try restored.beginNext(0)).?;
    try std.testing.expectEqual(WorkKind.reconcile, lookup.token.kind);
    try std.testing.expect(chainsEqual(chain, lookup.chain));

    const corrupted = try allocator.dupe(u8, encoded);
    defer allocator.free(corrupted);
    corrupted[snapshot_header_len] ^= 0x80;
    try std.testing.expectError(error.DigestMismatch, Outbox.decode(corrupted, testDestination()));
    try std.testing.expectError(error.DestinationMismatch, Outbox.decode(encoded, @enumFromInt(78)));

    for (0..encoded.len) |cut| {
        _ = Outbox.decode(encoded[0..cut], testDestination()) catch continue;
        return error.TruncatedSnapshotAccepted;
    }

    var delayed = try Outbox.init(testDestination());
    _ = try delayed.enqueue(testRecord(testChain(401, 7100, 1)));
    const delayed_claim = (try delayed.beginNext(0)).?;
    _ = try delayed.applySubmitOutcome(delayed_claim.token, .{ .known_not_sent = .{ .tick = 999 } });
    const delayed_image = try delayed.encodeAlloc(allocator);
    defer allocator.free(delayed_image);
    var delayed_restored = try Outbox.decode(delayed_image, testDestination());
    try std.testing.expect((try delayed_restored.beginNext(0)) != null);
}

test "settlement outbox codec rejects authenticated impossible entry states" {
    const allocator = std.testing.allocator;
    const Outbox = SettlementOutbox(2);

    var empty = try Outbox.init(testDestination());
    empty.entries[0] = .{
        .chain = testChain(450, 7500, 0),
        .state = .delivered,
    };
    empty.count = 1;
    empty.high_watermark = 450;
    const empty_image = try empty.encodeAlloc(allocator);
    defer allocator.free(empty_image);
    empty_image[snapshot_header_len + @sizeOf(u128) + @sizeOf(u64) + @sizeOf(u8)] = @intFromEnum(EntryStatus.ready);
    const empty_digest_offset = empty_image.len - snapshot_digest_len;
    const empty_digest = sha256(empty_image[0..empty_digest_offset]);
    @memcpy(empty_image[empty_digest_offset..], &empty_digest);
    try std.testing.expectError(error.InvalidState, Outbox.decode(empty_image, testDestination()));

    var ready = try Outbox.init(testDestination());
    _ = try ready.enqueue(testRecord(testChain(451, 7600, 1)));
    const ready_image = try ready.encodeAlloc(allocator);
    defer allocator.free(ready_image);
    ready_image[snapshot_header_len + @sizeOf(u128) + @sizeOf(u64) + @sizeOf(u8)] = @intFromEnum(EntryStatus.submitting);
    const ready_digest_offset = ready_image.len - snapshot_digest_len;
    const ready_digest = sha256(ready_image[0..ready_digest_offset]);
    @memcpy(ready_image[ready_digest_offset..], &ready_digest);
    try std.testing.expectError(error.InvalidState, Outbox.decode(ready_image, testDestination()));

    var impossible_counters = try Outbox.init(testDestination());
    _ = try impossible_counters.enqueue(testRecord(testChain(452, 7700, 1)));
    impossible_counters.entries[0].submit_attempts = 2;
    impossible_counters.entries[0].generation = 1;
    const counter_image = try impossible_counters.encodeAlloc(allocator);
    defer allocator.free(counter_image);
    try std.testing.expectError(
        error.InvalidState,
        Outbox.decode(counter_image, testDestination()),
    );
}

test "settlement outbox prunes only delivered prefixes and retains watermark" {
    const Outbox = SettlementOutbox(3);
    var outbox = try Outbox.init(testDestination());
    try std.testing.expectEqual(EnqueueResult.empty_finalized, try outbox.enqueue(testRecord(testChain(500, 8000, 0))));
    try std.testing.expectEqual(@as(usize, 0), outbox.len());
    _ = try outbox.enqueue(testRecord(testChain(501, 9000, 1)));
    try std.testing.expectError(error.UnsettledEntry, outbox.pruneDeliveredThrough(501));
    try std.testing.expectEqual(@as(usize, 0), try outbox.pruneDeliveredThrough(500));
    try std.testing.expectEqual(EnqueueResult.already_finalized, try outbox.enqueue(testRecord(testChain(500, 8000, 0))));
    try std.testing.expectEqual(EnqueueResult.already_finalized, try outbox.enqueue(testRecord(testChain(500, 8100, 1))));
}

test "prepared outbox releases work only after both monotonic frontiers in either order" {
    const allocator = std.testing.allocator;
    const Outbox = SettlementOutbox(2);
    const record = try testPreparedRecord(10, 0, 20_000, 2);
    const batch = [_]PreparedSettlementRecord{record};

    var risk_first = try Outbox.initPrepared(testDestination(), testPublicationDomain());
    const prepared = try risk_first.prepare(&batch);
    try std.testing.expectEqual(@as(usize, 1), prepared.inserted);
    try std.testing.expect((try risk_first.beginNext(0)) == null);
    switch (risk_first.dispatchState()) {
        .awaiting_commit => |waiting| {
            try std.testing.expect(!waiting.risk_r2_committed);
            try std.testing.expect(!waiting.journal_committed);
            try std.testing.expect(waiting.publication.eql(testPublicationCursor(10)));
        },
        else => return error.ExpectedAwaitingCommit,
    }

    const before_marks = try risk_first.encodeAlloc(allocator);
    defer allocator.free(before_marks);
    var wrong_domain = testPublicationCursor(10);
    wrong_domain.domain.journal_epoch += 1;
    risk_first.markRiskR2CommittedThrough(wrong_domain);
    risk_first.markRiskR2CommittedThrough(testPublicationCursor(10));
    risk_first.markRiskR2CommittedThrough(testPublicationCursor(9));
    risk_first.markRiskR2CommittedThrough(testPublicationCursor(11));
    switch (risk_first.dispatchState()) {
        .awaiting_commit => |waiting| {
            try std.testing.expect(waiting.risk_r2_committed);
            try std.testing.expect(!waiting.journal_committed);
        },
        else => return error.ExpectedAwaitingJournalCommit,
    }
    const late = try testPreparedRecord(11, 0, 20_100, 1);
    const late_batch = [_]PreparedSettlementRecord{late};
    try std.testing.expectError(error.PreparedAfterCommit, risk_first.prepare(&late_batch));
    risk_first.markJournalCommittedThrough(testPublicationCursor(10));
    const after_marks = try risk_first.encodeAlloc(allocator);
    defer allocator.free(after_marks);
    try std.testing.expectEqualSlices(u8, before_marks, after_marks);
    try std.testing.expect((try risk_first.beginNext(0)) != null);

    var journal_first = try Outbox.initPrepared(testDestination(), testPublicationDomain());
    _ = try journal_first.prepare(&batch);
    journal_first.markJournalCommittedThrough(testPublicationCursor(10));
    journal_first.markJournalCommittedThrough(testPublicationCursor(10));
    const journal_late = try journal_first.prepare(&late_batch);
    try std.testing.expectEqual(@as(usize, 1), journal_late.inserted);
    switch (journal_first.dispatchState()) {
        .awaiting_commit => |waiting| {
            try std.testing.expect(!waiting.risk_r2_committed);
            try std.testing.expect(waiting.journal_committed);
        },
        else => return error.ExpectedAwaitingRiskCommit,
    }
    journal_first.markRiskR2CommittedThrough(testPublicationCursor(10));
    try std.testing.expect((try journal_first.beginNext(0)) != null);
}

test "fresh prepared store proves only a contiguous live recovery prefix" {
    const Outbox = SettlementOutbox(1);
    var fresh = try Outbox.initPrepared(testDestination(), testPublicationDomain());

    try std.testing.expectEqual(RecoveryProvenance.fresh, fresh.recoveryProvenance());
    try fresh.validateRecoveryFrontier(testPublicationCursor(0));
    try std.testing.expectError(
        error.UnprovenRecoveryFrontier,
        fresh.validateRecoveryFrontier(testPublicationCursor(1)),
    );

    fresh.markRiskR2CommittedThrough(testPublicationCursor(1));
    fresh.markJournalCommittedThrough(testPublicationCursor(1));
    try fresh.validateRecoveryFrontier(testPublicationCursor(1));

    // A first observed callback above the contiguous prefix cannot prove the
    // skipped core publications, even though ordinary dispatch frontiers remain
    // monotonic and allocation/I/O-free.
    fresh.markRiskR2CommittedThrough(testPublicationCursor(3));
    fresh.markJournalCommittedThrough(testPublicationCursor(3));
    try std.testing.expectError(
        error.UnprovenRecoveryFrontier,
        fresh.validateRecoveryFrontier(testPublicationCursor(3)),
    );

    const image = try fresh.encodeAlloc(std.testing.allocator);
    defer std.testing.allocator.free(image);
    var reopened = try Outbox.decode(image, testDestination());
    try std.testing.expectEqual(RecoveryProvenance.reopened, reopened.recoveryProvenance());
    try std.testing.expectError(
        error.RecoveryRequired,
        reopened.validateRecoveryFrontier(testPublicationCursor(0)),
    );
    try std.testing.expectEqual(@as(usize, 0), try reopened.finishRecovery(testPublicationCursor(0)));
    try reopened.validateRecoveryFrontier(testPublicationCursor(0));
}

test "prepared outbox admits one command atomically and validates mapper cursor layout" {
    const Outbox = SettlementOutbox(2);
    var outbox = try Outbox.initPrepared(testDestination(), testPublicationDomain());

    const first = try testPreparedRecord(20, 0, 21_000, 1);
    const first_batch = [_]PreparedSettlementRecord{first};
    const first_result = try outbox.prepare(&first_batch);
    try std.testing.expectEqual(@as(usize, 1), first_result.inserted);
    try std.testing.expectEqual(
        @as(usize, 1),
        (try outbox.prepare(&first_batch)).duplicates,
    );

    const second = try testPreparedRecord(21, 0, 22_000, 1);
    const third = try testPreparedRecord(21, 1, 23_000, 1);
    const overflowing_batch = [_]PreparedSettlementRecord{ second, third };
    try std.testing.expectError(error.OutboxFull, outbox.prepare(&overflowing_batch));
    try std.testing.expectEqual(@as(usize, 1), outbox.len());
    try std.testing.expect(outbox.status(second.chain.group_id) == null);
    try std.testing.expect(outbox.status(third.chain.group_id) == null);

    const second_batch = [_]PreparedSettlementRecord{second};
    try std.testing.expectEqual(@as(usize, 1), (try outbox.prepare(&second_batch)).inserted);

    var mismatched = try testPreparedRecord(22, 0, 24_000, 1);
    mismatched.publication = testPublicationCursor(23);
    const mismatched_batch = [_]PreparedSettlementRecord{mismatched};
    try std.testing.expectError(error.PublicationSequenceMismatch, outbox.prepare(&mismatched_batch));

    var oversized = try testPreparedRecord(22, 0, 24_100, 1);
    oversized.chain.count = @intCast(settlement.max_transfers_per_fill + 1);
    const oversized_batch = [_]PreparedSettlementRecord{oversized};
    try std.testing.expectError(error.InvalidTransferCount, outbox.prepare(&oversized_batch));

    var wrong_transfer_id = try testPreparedRecord(22, 0, 24_200, 1);
    wrong_transfer_id.chain.storage[0].id ^= @as(u128, 1) << 40;
    const wrong_id_batch = [_]PreparedSettlementRecord{wrong_transfer_id};
    try std.testing.expectError(error.InvalidTransfer, outbox.prepare(&wrong_id_batch));

    var wrong_user_data = try testPreparedRecord(22, 0, 24_300, 1);
    wrong_user_data.chain.storage[0].user_data_64 = 1;
    const wrong_user_data_batch = [_]PreparedSettlementRecord{wrong_user_data};
    try std.testing.expectError(error.InvalidTransfer, outbox.prepare(&wrong_user_data_batch));
}

test "prepared outbox requires one complete zero-based ordinal run" {
    const Outbox = SettlementOutbox(3);
    var outbox = try Outbox.initPrepared(testDestination(), testPublicationDomain());
    const zero = try testPreparedRecord(24, 0, 24_400, 1);
    const one = try testPreparedRecord(24, 1, 24_500, 1);
    const two = try testPreparedRecord(24, 2, 24_600, 1);

    try std.testing.expectError(error.InvalidPreparedBatch, outbox.prepare(&.{one}));
    try std.testing.expectError(error.InvalidPreparedBatch, outbox.prepare(&.{ zero, two }));
    try std.testing.expectError(error.InvalidPreparedBatch, outbox.prepare(&.{ one, zero }));
    try std.testing.expectError(error.InvalidPreparedBatch, outbox.prepare(&.{ zero, zero }));
    try std.testing.expectEqual(@as(usize, 0), outbox.len());

    const result = try outbox.prepare(&.{ zero, one });
    try std.testing.expectEqual(@as(usize, 2), result.inserted);
    try std.testing.expectEqual(@as(usize, 0), result.duplicates);
}

test "prepared snapshot preserves a pruned ordinal prefix and rejects an authenticated gap" {
    const allocator = std.testing.allocator;
    const Outbox = SettlementOutbox(3);
    var outbox = try Outbox.initPrepared(testDestination(), testPublicationDomain());
    const zero = try testPreparedRecord(25, 0, 24_700, 0);
    const one = try testPreparedRecord(25, 1, 24_800, 0);
    _ = try outbox.prepare(&.{ zero, one });
    outbox.markRiskR2CommittedThrough(testPublicationCursor(25));
    outbox.markJournalCommittedThrough(testPublicationCursor(25));
    try std.testing.expectEqual(
        @as(usize, 1),
        try outbox.pruneDeliveredThrough(zero.chain.group_id),
    );

    const valid_image = try outbox.encodeAlloc(allocator);
    defer allocator.free(valid_image);
    const restored = try Outbox.decode(valid_image, testDestination());
    try std.testing.expectEqual(@as(usize, 1), restored.len());
    try std.testing.expect(restored.status(one.chain.group_id) != null);

    const gap = try testPreparedRecord(25, 2, 24_900, 0);
    outbox.entries[0].chain = gap.chain;
    outbox.high_watermark = gap.chain.group_id;
    const gap_image = try outbox.encodeAlloc(allocator);
    defer allocator.free(gap_image);
    try std.testing.expectError(
        error.InvalidSnapshotPublication,
        Outbox.decode(gap_image, testDestination()),
    );
}

test "prepared empty chains remain gated and consume no dispatch work" {
    const Outbox = SettlementOutbox(2);
    var outbox = try Outbox.initPrepared(testDestination(), testPublicationDomain());
    const empty = try testPreparedRecord(30, 0, 25_000, 0);
    const batch = [_]PreparedSettlementRecord{empty};

    try std.testing.expectEqual(@as(usize, 1), (try outbox.prepare(&batch)).inserted);
    try std.testing.expectEqual(@as(usize, 1), outbox.len());
    try std.testing.expectEqual(EntryStatus.delivered, outbox.status(empty.chain.group_id).?);
    try std.testing.expectEqual(@as(usize, 0), try outbox.pruneDeliveredPrefix());
    try std.testing.expect((try outbox.beginNext(0)) == null);

    outbox.markJournalCommittedThrough(testPublicationCursor(30));
    try std.testing.expectEqual(@as(usize, 0), try outbox.pruneDeliveredPrefix());
    outbox.markRiskR2CommittedThrough(testPublicationCursor(30));
    try std.testing.expectEqual(DispatchState.drained, outbox.dispatchState());
    try std.testing.expectEqual(@as(usize, 1), try outbox.pruneDeliveredPrefix());
    try std.testing.expectEqual(@as(usize, 0), outbox.len());
}

test "prepared pruning cannot advance through a nonexistent group" {
    const Outbox = SettlementOutbox(3);
    var outbox = try Outbox.initPrepared(testDestination(), testPublicationDomain());
    const first = try testPreparedRecord(31, 0, 25_100, 0);
    const later = try testPreparedRecord(33, 0, 25_200, 1);
    _ = try outbox.prepare(&.{first});
    _ = try outbox.prepare(&.{later});
    outbox.markRiskR2CommittedThrough(testPublicationCursor(31));
    outbox.markJournalCommittedThrough(testPublicationCursor(31));

    const nonexistent = try (settlement.FillIdentity{
        .namespace = testPublicationDomain().namespace,
        .journal_epoch = testPublicationDomain().journal_epoch,
        .publication_sequence = publication.PublicationSequence.fromInt(32),
        .fill_ordinal = 0,
    }).groupId();
    try std.testing.expectError(
        error.InvalidPruneWatermark,
        outbox.pruneDeliveredThrough(nonexistent),
    );
    try std.testing.expectEqual(@as(usize, 2), outbox.len());
    try std.testing.expectEqual(@as(usize, 1), try outbox.pruneDeliveredPrefix());
    try std.testing.expectEqual(@as(usize, 1), outbox.len());
}

test "durable prepared outbox recovery promotes common work and discards only its untouched suffix" {
    const Store = DurableFileOutbox(4);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const first = try testPreparedRecord(40, 0, 26_000, 1);
    const second = try testPreparedRecord(41, 0, 27_000, 1);
    {
        var store = try Store.openPrepared(
            std.testing.allocator,
            tmp.dir,
            "prepared-recovery.tso",
            testDestination(),
            testPublicationDomain(),
        );
        defer store.deinit();
        const first_batch = [_]PreparedSettlementRecord{first};
        const second_batch = [_]PreparedSettlementRecord{second};
        _ = try store.prepare(&first_batch);
        _ = try store.prepare(&second_batch);
    }

    {
        var store = try Store.openPrepared(
            std.testing.allocator,
            tmp.dir,
            "prepared-recovery.tso",
            testDestination(),
            testPublicationDomain(),
        );
        defer store.deinit();
        try std.testing.expect(store.isRecoveryRequired());
        try std.testing.expectEqual(@as(usize, 1), try store.finishRecovery(testPublicationCursor(40)));
        try std.testing.expectEqual(@as(usize, 1), store.len());
        switch (store.dispatchState()) {
            .waiting_submit => |waiting| try std.testing.expectEqual(first.chain.group_id, waiting.group_id),
            else => return error.ExpectedPromotedSettlement,
        }
    }

    {
        var store = try Store.openPrepared(
            std.testing.allocator,
            tmp.dir,
            "prepared-recovery.tso",
            testDestination(),
            testPublicationDomain(),
        );
        defer store.deinit();
        try std.testing.expect(store.isRecoveryRequired());
        const reused_batch = [_]PreparedSettlementRecord{second};
        try std.testing.expectEqual(
            @as(usize, 1),
            (try store.prepareRecovery(&reused_batch)).inserted,
        );
        try std.testing.expect((try store.beginNext(0)) == null);
        try std.testing.expectEqual(@as(usize, 0), try store.finishRecovery(testPublicationCursor(41)));
    }
}

test "durable recovery reconstruction survives ambiguous write and another restart" {
    const Store = DurableFileOutbox(2);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const first = try testPreparedRecord(90, 0, 36_000, 1);
    const missing = try testPreparedRecord(91, 0, 37_000, 1);
    {
        var store = try Store.openPrepared(
            std.testing.allocator,
            tmp.dir,
            "prepared-recovery-write.tso",
            testDestination(),
            testPublicationDomain(),
        );
        defer store.deinit();
        _ = try store.prepare(&.{first});
    }

    {
        var store = try Store.openPrepared(
            std.testing.allocator,
            tmp.dir,
            "prepared-recovery-write.tso",
            testDestination(),
            testPublicationDomain(),
        );
        defer store.deinit();
        try std.testing.expect(store.isRecoveryRequired());
        try std.testing.expect((try store.beginNext(0)) == null);
        store.injectWriteFailureForTesting();
        try std.testing.expectError(
            error.StorageAmbiguous,
            store.prepareRecovery(&.{missing}),
        );
        try std.testing.expectError(error.Poisoned, store.prepareRecovery(&.{missing}));
    }

    {
        var store = try Store.openPrepared(
            std.testing.allocator,
            tmp.dir,
            "prepared-recovery-write.tso",
            testDestination(),
            testPublicationDomain(),
        );
        defer store.deinit();
        try std.testing.expect(store.isRecoveryRequired());
        try std.testing.expectEqual(
            @as(usize, 1),
            (try store.prepareRecovery(&.{missing})).inserted,
        );
        try std.testing.expect((try store.beginNext(0)) == null);
    }

    {
        var store = try Store.openPrepared(
            std.testing.allocator,
            tmp.dir,
            "prepared-recovery-write.tso",
            testDestination(),
            testPublicationDomain(),
        );
        defer store.deinit();
        try std.testing.expect(store.isRecoveryRequired());
        try std.testing.expectEqual(
            @as(usize, 1),
            (try store.prepareRecovery(&.{missing})).duplicates,
        );
        try std.testing.expect((try store.beginNext(0)) == null);
        try std.testing.expectEqual(
            @as(usize, 0),
            try store.finishRecovery(testPublicationCursor(91)),
        );
    }

    {
        var store = try Store.openPrepared(
            std.testing.allocator,
            tmp.dir,
            "prepared-recovery-post-replace.tso",
            testDestination(),
            testPublicationDomain(),
        );
        defer store.deinit();
        _ = try store.prepare(&.{first});
    }

    {
        var store = try Store.openPrepared(
            std.testing.allocator,
            tmp.dir,
            "prepared-recovery-post-replace.tso",
            testDestination(),
            testPublicationDomain(),
        );
        defer store.deinit();
        try std.testing.expect(store.isRecoveryRequired());
        try std.testing.expect((try store.beginNext(0)) == null);
        store.injectPostReplaceFailureForTesting();
        try std.testing.expectError(
            error.StorageAmbiguous,
            store.prepareRecovery(&.{missing}),
        );
        try std.testing.expect(store.isPoisoned());
    }

    {
        var store = try Store.openPrepared(
            std.testing.allocator,
            tmp.dir,
            "prepared-recovery-post-replace.tso",
            testDestination(),
            testPublicationDomain(),
        );
        defer store.deinit();
        try std.testing.expect(store.isRecoveryRequired());
        try std.testing.expectEqual(
            @as(usize, 1),
            (try store.prepareRecovery(&.{missing})).duplicates,
        );
        try std.testing.expect((try store.beginNext(0)) == null);
        try std.testing.expectEqual(
            @as(usize, 0),
            try store.finishRecovery(testPublicationCursor(91)),
        );
    }
}

test "prepared recovery reconstructs only an ordered suffix while dispatch stays fenced" {
    const Outbox = SettlementOutbox(4);
    var outbox = try Outbox.initPrepared(testDestination(), testPublicationDomain());
    const first = try testPreparedRecord(60, 0, 29_000, 1);
    const second = try testPreparedRecord(61, 0, 30_000, 1);
    const first_batch = [_]PreparedSettlementRecord{first};
    const second_batch = [_]PreparedSettlementRecord{second};
    _ = try outbox.prepare(&first_batch);

    outbox.recoverAfterRestart(0);
    try std.testing.expectError(error.RecoveryRequired, outbox.prepare(&second_batch));
    try std.testing.expectEqual(
        @as(usize, 1),
        (try outbox.prepareRecovery(&first_batch)).duplicates,
    );
    try std.testing.expectEqual(
        @as(usize, 1),
        (try outbox.prepareRecovery(&second_batch)).inserted,
    );
    try std.testing.expect((try outbox.beginNext(0)) == null);

    // Transient marks are ignored under the fence. One authoritative finish
    // publishes both recovered frontiers and releases the oldest record.
    outbox.markRiskR2CommittedThrough(testPublicationCursor(61));
    outbox.markJournalCommittedThrough(testPublicationCursor(61));
    try std.testing.expectEqual(@as(usize, 0), try outbox.finishRecovery(testPublicationCursor(61)));
    switch (outbox.dispatchState()) {
        .waiting_submit => |waiting| try std.testing.expectEqual(first.chain.group_id, waiting.group_id),
        else => return error.ExpectedRecoveredSettlement,
    }
}

test "prepared recovery requires complete ordinal runs while fenced" {
    const Outbox = SettlementOutbox(4);
    var outbox = try Outbox.initPrepared(testDestination(), testPublicationDomain());
    const first_zero = try testPreparedRecord(62, 0, 30_100, 1);
    const first_one = try testPreparedRecord(62, 1, 30_200, 1);
    _ = try outbox.prepare(&.{ first_zero, first_one });
    outbox.recoverAfterRestart(0);

    const next_zero = try testPreparedRecord(63, 0, 30_300, 1);
    const next_one = try testPreparedRecord(63, 1, 30_400, 1);
    const next_two = try testPreparedRecord(63, 2, 30_500, 1);
    try std.testing.expectError(
        error.InvalidPreparedBatch,
        outbox.prepareRecovery(&.{ next_zero, next_two }),
    );
    try std.testing.expectError(
        error.InvalidPreparedBatch,
        outbox.prepareRecovery(&.{next_one}),
    );
    try std.testing.expectEqual(@as(usize, 2), outbox.len());
    try std.testing.expect(outbox.isRecoveryRequired());
    try std.testing.expect((try outbox.beginNext(0)) == null);

    try std.testing.expectEqual(
        @as(usize, 2),
        (try outbox.prepareRecovery(&.{ first_zero, first_one })).duplicates,
    );
    try std.testing.expectEqual(
        @as(usize, 2),
        (try outbox.prepareRecovery(&.{ next_zero, next_one })).inserted,
    );
    try std.testing.expect((try outbox.beginNext(0)) == null);
}

test "prepared recovery rejects a missing record below an existing suffix atomically" {
    const Outbox = SettlementOutbox(4);
    var outbox = try Outbox.initPrepared(testDestination(), testPublicationDomain());
    const first = try testPreparedRecord(70, 0, 31_000, 1);
    const missing = try testPreparedRecord(71, 0, 32_000, 1);
    const later = try testPreparedRecord(72, 0, 33_000, 1);
    _ = try outbox.prepare(&.{first});
    _ = try outbox.prepare(&.{later});
    outbox.recoverAfterRestart(0);

    try std.testing.expectError(
        error.NonMonotonicGroupId,
        outbox.prepareRecovery(&.{missing}),
    );
    try std.testing.expectEqual(@as(usize, 2), outbox.len());
    try std.testing.expect(outbox.isRecoveryRequired());
}

test "prepared recovery accepts a full duplicate but preserves capacity failure" {
    const Outbox = SettlementOutbox(1);
    var outbox = try Outbox.initPrepared(testDestination(), testPublicationDomain());
    const retained = try testPreparedRecord(80, 0, 34_000, 1);
    const missing_suffix = try testPreparedRecord(81, 0, 35_000, 1);
    _ = try outbox.prepare(&.{retained});
    outbox.recoverAfterRestart(0);

    try std.testing.expectEqual(
        @as(usize, 1),
        (try outbox.prepareRecovery(&.{retained})).duplicates,
    );
    try std.testing.expectError(
        error.OutboxFull,
        outbox.prepareRecovery(&.{missing_suffix}),
    );
    try std.testing.expectEqual(@as(usize, 1), outbox.len());
    try std.testing.expect(outbox.isRecoveryRequired());
}

test "prepared recovery rejects progressed entries and pruned watermarks above common frontier" {
    const allocator = std.testing.allocator;
    const Outbox = SettlementOutbox(2);

    var progressed = try Outbox.initPrepared(testDestination(), testPublicationDomain());
    const record = try testPreparedRecord(50, 0, 28_000, 1);
    const batch = [_]PreparedSettlementRecord{record};
    _ = try progressed.prepare(&batch);
    progressed.markRiskR2CommittedThrough(testPublicationCursor(50));
    progressed.markJournalCommittedThrough(testPublicationCursor(50));
    _ = (try progressed.beginNext(0)).?;
    const progressed_image = try progressed.encodeAlloc(allocator);
    defer allocator.free(progressed_image);
    var progressed_reopened = try Outbox.decode(progressed_image, testDestination());
    try std.testing.expectError(
        error.ProgressedBeyondRecoveryFrontier,
        progressed_reopened.finishRecovery(testPublicationCursor(49)),
    );
    try std.testing.expect(progressed_reopened.isRecoveryRequired());

    var pruned = try Outbox.initPrepared(testDestination(), testPublicationDomain());
    const empty = try testPreparedRecord(60, 0, 29_000, 0);
    const empty_batch = [_]PreparedSettlementRecord{empty};
    _ = try pruned.prepare(&empty_batch);
    pruned.markRiskR2CommittedThrough(testPublicationCursor(60));
    pruned.markJournalCommittedThrough(testPublicationCursor(60));
    _ = try pruned.pruneDeliveredPrefix();
    const pruned_image = try pruned.encodeAlloc(allocator);
    defer allocator.free(pruned_image);
    var pruned_reopened = try Outbox.decode(pruned_image, testDestination());
    try std.testing.expectError(
        error.ProgressedBeyondRecoveryFrontier,
        pruned_reopened.finishRecovery(testPublicationCursor(59)),
    );
}

test "prepared codec rejects authenticated empty watermarks from another domain" {
    const allocator = std.testing.allocator;
    const Outbox = SettlementOutbox(1);
    var invalid = try Outbox.initPrepared(testDestination(), testPublicationDomain());
    const wrong_group_id = try (settlement.FillIdentity{
        .namespace = testPublicationDomain().namespace + 1,
        .journal_epoch = testPublicationDomain().journal_epoch,
        .publication_sequence = publication.PublicationSequence.fromInt(70),
        .fill_ordinal = 0,
    }).groupId();
    invalid.pruned_through = wrong_group_id;
    invalid.high_watermark = wrong_group_id;
    const image = try invalid.encodeAlloc(allocator);
    defer allocator.free(image);
    try std.testing.expectError(error.InvalidSnapshotDomain, Outbox.decode(image, testDestination()));
}

test "prepared codec rejects transfer metadata outside the mapper identity" {
    const allocator = std.testing.allocator;
    const Outbox = SettlementOutbox(1);
    var invalid = try Outbox.initPrepared(testDestination(), testPublicationDomain());
    const record = try testPreparedRecord(71, 0, 29_100, 1);
    _ = try invalid.prepare(&.{record});
    invalid.entries[0].chain.storage[0].user_data_32 = 1;
    const image = try invalid.encodeAlloc(allocator);
    defer allocator.free(image);
    try std.testing.expectError(
        error.InvalidSnapshotChain,
        Outbox.decode(image, testDestination()),
    );
}

test "durable frontier marks do not consume an injected write failure" {
    const Store = DurableFileOutbox(2);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = try Store.openPrepared(
        std.testing.allocator,
        tmp.dir,
        "mark-no-io.tso",
        testDestination(),
        testPublicationDomain(),
    );
    defer store.deinit();
    const record = try testPreparedRecord(80, 0, 30_000, 1);
    const batch = [_]PreparedSettlementRecord{record};
    _ = try store.prepare(&batch);

    store.injectWriteFailureForTesting();
    store.markRiskR2CommittedThrough(testPublicationCursor(80));
    store.markJournalCommittedThrough(testPublicationCursor(80));
    try std.testing.expect(!store.isPoisoned());
    try std.testing.expectError(error.StorageAmbiguous, store.beginNext(0));
    try std.testing.expect(store.isPoisoned());
}

test "durable settlement outbox persists pre-send claim and reopens into reconciliation" {
    const allocator = std.testing.allocator;
    const Store = DurableFileOutbox(4);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const chain = testChain(600, 10_000, 2);
    {
        var store = try Store.open(allocator, tmp.dir, "settlement-outbox.tso", testDestination());
        defer store.deinit();
        try std.testing.expectEqual(EnqueueResult.inserted, try store.enqueue(testRecord(chain)));
        const submit = (try store.beginNext(0)).?;
        try std.testing.expectEqual(WorkKind.submit, submit.token.kind);
    }

    {
        var store = try Store.open(allocator, tmp.dir, "settlement-outbox.tso", testDestination());
        defer store.deinit();
        try std.testing.expectEqual(EntryStatus.needs_reconciliation, store.status(600).?);
        const lookup = (try store.beginNext(0)).?;
        try std.testing.expectEqual(WorkKind.reconcile, lookup.token.kind);
        try std.testing.expect(chainsEqual(chain, lookup.chain));
        _ = try store.applyReconcileOutcome(lookup.token, .all_present_exact);
        try std.testing.expectEqual(@as(usize, 1), try store.pruneDeliveredThrough(600));
    }

    {
        var store = try Store.open(allocator, tmp.dir, "settlement-outbox.tso", testDestination());
        defer store.deinit();
        try std.testing.expectEqual(@as(usize, 0), store.len());
        try std.testing.expectEqual(EnqueueResult.already_finalized, try store.enqueue(testRecord(chain)));
    }
}

test "durable settlement outbox poisons an ambiguous atomic write" {
    const Store = DurableFileOutbox(2);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = try Store.open(std.testing.allocator, tmp.dir, "poison-outbox.tso", testDestination());
    defer store.deinit();
    store.injectWriteFailureForTesting();
    try std.testing.expectError(error.StorageAmbiguous, store.enqueue(testRecord(testChain(650, 10_500, 1))));
    try std.testing.expect(store.isPoisoned());
    try std.testing.expectError(error.Poisoned, store.beginNext(0));
}

test "durable settlement outbox holds a stable exclusive sidecar lock" {
    const Store = DurableFileOutbox(2);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var first = try Store.open(std.testing.allocator, tmp.dir, "locked-outbox.tso", testDestination());
    try std.testing.expectError(
        error.AlreadyOpen,
        Store.open(std.testing.allocator, tmp.dir, "locked-outbox.tso", testDestination()),
    );
    first.deinit();

    var reopened = try Store.open(std.testing.allocator, tmp.dir, "locked-outbox.tso", testDestination());
    reopened.deinit();
}

test "durable settlement outbox reopens an exact maximum-size snapshot" {
    const Store = DurableFileOutbox(2);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var store = try Store.open(std.testing.allocator, tmp.dir, "full-outbox.tso", testDestination());
        defer store.deinit();
        _ = try store.enqueue(testRecord(testChain(660, 10_600, settlement.max_transfers_per_fill)));
        _ = try store.enqueue(testRecord(testChain(661, 10_700, settlement.max_transfers_per_fill)));
    }

    var reopened = try Store.open(std.testing.allocator, tmp.dir, "full-outbox.tso", testDestination());
    defer reopened.deinit();
    try std.testing.expectEqual(@as(usize, 2), reopened.len());
}

const PreparedFirstFrontier = enum { risk_r2, journal };

const PreparedCrashCampaignResult = struct {
    first_frontier: PreparedFirstFrontier,
    discarded_precommit: usize,
    recovered_kind: WorkKind,
};

fn runPreparedCrashOrderingCampaign(
    allocator: std.mem.Allocator,
    seed: u64,
) !PreparedCrashCampaignResult {
    const Outbox = SettlementOutbox(1);
    const cursor = testPublicationCursor(90);
    const record = try testPreparedRecord(90, 0, 31_000, 2);
    const batch = [_]PreparedSettlementRecord{record};
    var outbox = try Outbox.initPrepared(testDestination(), testPublicationDomain());
    _ = try outbox.prepare(&batch);

    var prng = std.Random.DefaultPrng.init(seed);
    const first_frontier: PreparedFirstFrontier =
        if (prng.random().boolean()) .risk_r2 else .journal;
    switch (first_frontier) {
        .risk_r2 => outbox.markRiskR2CommittedThrough(cursor),
        .journal => outbox.markJournalCommittedThrough(cursor),
    }

    // Crash after only one commit. No record can be released, and recovery at
    // common frontier zero must discard the untouched preparation so replay
    // can reuse exactly the same deterministic group ID.
    const precommit_image = try outbox.encodeAlloc(allocator);
    defer allocator.free(precommit_image);
    outbox = try Outbox.decode(precommit_image, testDestination());
    const discarded = try outbox.finishRecovery(.{
        .domain = testPublicationDomain(),
        .publication_sequence = 0,
    });
    try std.testing.expectEqual(@as(usize, 1), discarded);
    _ = try outbox.prepare(&batch);

    if (prng.random().boolean()) {
        outbox.markRiskR2CommittedThrough(cursor);
        outbox.markJournalCommittedThrough(cursor);
    } else {
        outbox.markJournalCommittedThrough(cursor);
        outbox.markRiskR2CommittedThrough(cursor);
    }
    const submission = (try outbox.beginNext(0)).?;
    try std.testing.expectEqual(WorkKind.submit, submission.token.kind);

    // Crash after the durable in-flight claim. Because both core frontiers
    // cover the command, recovery retains it and forces lookup, never a blind
    // resubmission.
    const post_claim_image = try outbox.encodeAlloc(allocator);
    defer allocator.free(post_claim_image);
    outbox = try Outbox.decode(post_claim_image, testDestination());
    try std.testing.expectEqual(@as(usize, 0), try outbox.finishRecovery(cursor));
    const reconciliation = (try outbox.beginNext(0)).?;
    try std.testing.expectEqual(WorkKind.reconcile, reconciliation.token.kind);
    _ = try outbox.applyReconcileOutcome(reconciliation.token, .all_present_exact);

    return .{
        .first_frontier = first_frontier,
        .discarded_precommit = discarded,
        .recovered_kind = reconciliation.token.kind,
    };
}

test "prepared outbox seeded frontier and crash ordering is replayable" {
    for (0..64) |seed| {
        const first = try runPreparedCrashOrderingCampaign(std.testing.allocator, seed);
        const replay = try runPreparedCrashOrderingCampaign(std.testing.allocator, seed);
        try std.testing.expectEqualDeep(first, replay);
        try std.testing.expectEqual(@as(usize, 1), first.discarded_precommit);
        try std.testing.expectEqual(WorkKind.reconcile, first.recovered_kind);
    }
}

const CampaignResult = struct {
    steps: u32,
    submits: u32,
    reconciliations: u32,
    crashes: u32,
};

fn runSeededCampaign(allocator: std.mem.Allocator, seed: u64) !CampaignResult {
    const Outbox = SettlementOutbox(2);
    var outbox = try Outbox.init(testDestination());
    const chain = testChain(700, 11_000, 3);
    _ = try outbox.enqueue(testRecord(chain));

    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();
    var remote_present = false;
    var result: CampaignResult = .{ .steps = 0, .submits = 0, .reconciliations = 0, .crashes = 0 };

    // Always exercise the durable restart boundary, then inject additional
    // crashes from the seeded schedule below.
    const initial_image = try outbox.encodeAlloc(allocator);
    defer allocator.free(initial_image);
    outbox = try Outbox.decode(initial_image, testDestination());
    result.crashes = 1;

    while (result.steps < 256 and outbox.status(700).? != .delivered) : (result.steps += 1) {
        if (random.uintLessThan(u8, 5) == 0) {
            const image = try outbox.encodeAlloc(allocator);
            defer allocator.free(image);
            outbox = try Outbox.decode(image, testDestination());
            result.crashes += 1;
        }

        const work = (try outbox.beginNext(result.steps)) orelse continue;
        switch (work.token.kind) {
            .submit => {
                result.submits += 1;
                if (remote_present) return error.SubmitWithoutReconciliation;
                switch (random.uintLessThan(u8, 4)) {
                    0 => _ = try outbox.applySubmitOutcome(work.token, .{ .known_not_sent = .{ .tick = result.steps + 1 } }),
                    1 => _ = try outbox.applySubmitOutcome(work.token, .{ .ambiguous = .{ .tick = result.steps + 1 } }),
                    2 => {
                        remote_present = true;
                        _ = try outbox.applySubmitOutcome(work.token, .{ .ambiguous = .{ .tick = result.steps + 1 } });
                    },
                    3 => {
                        remote_present = true;
                        _ = try outbox.applySubmitOutcome(work.token, .committed);
                    },
                    else => unreachable,
                }
            },
            .reconcile => {
                result.reconciliations += 1;
                if (remote_present) {
                    _ = try outbox.applyReconcileOutcome(work.token, .all_present_exact);
                } else {
                    _ = try outbox.applyReconcileOutcome(
                        work.token,
                        .{ .all_absent = .{ .tick = result.steps + 1 } },
                    );
                }
            },
        }
    }
    if (outbox.status(700).? != .delivered or !remote_present) return error.CampaignDidNotConverge;
    return result;
}

test "settlement outbox seeded crash campaign is replayable and never blind-retries" {
    const first = try runSeededCampaign(std.testing.allocator, 0x5E771E_BEE7);
    const replay = try runSeededCampaign(std.testing.allocator, 0x5E771E_BEE7);
    try std.testing.expectEqualDeep(first, replay);
    try std.testing.expect(first.steps > 0);
    try std.testing.expect(first.crashes > 0);
}
