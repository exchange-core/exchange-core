//! Parix host integration for the opt-in TigerBeetle plugin.
const std = @import("std");
const tigerbeetle = @import("tigerbeetle");
const funding = @import("parix_funding_saga.zig");

const settlement = tigerbeetle.settlement;
const outbox = tigerbeetle.outbox;

pub const DestinationId = outbox.DestinationId;
pub const Transfer = settlement.Transfer;

pub const workflow_revision: u32 = 1;

const transfer_id_domain = "exchange-core/parix-clearing-transfer/v1";
const funding_id_domain = "exchange-core/parix-clearing-funding/v1";
const expiration_event_id_domain = "exchange-core/parix-clearing-expiration/v1";

pub const Direction = enum(u8) {
    deposit = 1,
    withdrawal = 2,
};

pub const ResolutionKind = enum(u8) {
    post = 1,
    void = 2,
};

pub const ResolutionSource = enum(u8) {
    provider = 1,
    expiration = 2,
};

pub const TransferStage = enum(u8) {
    authorization = 1,
    resolution = 2,
};

pub const FundingPurpose = enum(u8) {
    withdrawal_debit = 1,
    deposit_credit = 2,
    withdrawal_refund = 3,
};

pub const FundingReconciliationCause = enum(u8) {
    ambiguous_submission = 1,
    accepted_operation = 2,
};

pub const FundingStatusPhase = struct {
    purpose: FundingPurpose,
    cause: FundingReconciliationCause,
};

pub const PublicState = enum(u8) {
    accepted = 1,
    pending = 2,
    authorized = 3,
    completed = 4,
    failed = 5,
    quarantined = 6,
};

pub const FailureReason = enum(u8) {
    funding_rejected = 1,
    authorization_rejected = 2,
};

pub const QuarantineReason = enum(u8) {
    destination_mismatch = 1,
    transfer_mismatch = 2,
    reconciliation_failed = 3,
    funding_conflict = 4,
    funding_failed_after_external_commit = 5,
    refund_failed = 6,
    terminal_conflict = 7,
};

pub const ReconciliationCause = enum(u8) {
    ambiguous_submission = 1,
    conflict_response = 2,
};

pub const Binding = struct {
    destination_id: DestinationId,
    settlement_profile_digest: [32]u8,
    clearing_profile_digest: [32]u8,
    provider_id: u64,

    pub fn validate(self: Binding) error{InvalidBinding}!void {
        if (@intFromEnum(self.destination_id) == 0 or self.provider_id == 0) {
            return error.InvalidBinding;
        }
    }

    pub fn eql(a: Binding, b: Binding) bool {
        return a.destination_id == b.destination_id and
            a.provider_id == b.provider_id and
            std.mem.eql(u8, &a.settlement_profile_digest, &b.settlement_profile_digest) and
            std.mem.eql(u8, &a.clearing_profile_digest, &b.clearing_profile_digest);
    }
};

pub const Route = struct {
    destination_id: DestinationId,
    ledger: u32,
    treasury_account_id: u128,
    provider_control_account_id: u128,
    transfer_code: u16,

    pub fn validate(self: Route) RouteError!void {
        if (@intFromEnum(self.destination_id) == 0) return error.InvalidDestination;
        if (self.ledger == 0) return error.InvalidLedger;
        if (self.transfer_code == 0) return error.InvalidTransferCode;
        if (!validAccountId(self.treasury_account_id) or
            !validAccountId(self.provider_control_account_id) or
            self.treasury_account_id == self.provider_control_account_id)
        {
            return error.InvalidAccount;
        }
    }

    pub fn eql(a: Route, b: Route) bool {
        return a.destination_id == b.destination_id and
            a.ledger == b.ledger and
            a.treasury_account_id == b.treasury_account_id and
            a.provider_control_account_id == b.provider_control_account_id and
            a.transfer_code == b.transfer_code;
    }
};

pub const RouteError = error{
    InvalidDestination,
    InvalidLedger,
    InvalidAccount,
    InvalidTransferCode,
};

pub const Authorization = struct {
    event_id: u128,
    payment_id: u128,
    provider_id: u64,
    revision: u64,
    direction: Direction,
    uid: i64,
    currency: i32,
    amount: i64,
    timeout_seconds: u32,

    pub fn validate(self: Authorization) EventError!void {
        if (self.event_id == 0) return error.InvalidEventId;
        if (self.payment_id == 0) return error.InvalidPaymentId;
        if (self.provider_id == 0) return error.InvalidProviderId;
        if (self.revision == 0 or self.revision == std.math.maxInt(u64)) {
            return error.InvalidRevision;
        }
        if (self.uid <= 0) return error.InvalidUserId;
        if (self.currency <= 0) return error.InvalidCurrency;
        if (self.amount <= 0) return error.InvalidAmount;
        if (self.timeout_seconds == 0) return error.InvalidTimeout;
    }

    pub fn eql(a: Authorization, b: Authorization) bool {
        return std.meta.eql(a, b);
    }
};

pub const Resolution = struct {
    event_id: u128,
    payment_id: u128,
    provider_id: u64,
    revision: u64,
    kind: ResolutionKind,
    direction: Direction,
    uid: i64,
    currency: i32,
    amount: i64,

    pub fn validate(self: Resolution) EventError!void {
        if (self.event_id == 0) return error.InvalidEventId;
        if (self.payment_id == 0) return error.InvalidPaymentId;
        if (self.provider_id == 0) return error.InvalidProviderId;
        if (self.revision == 0) return error.InvalidRevision;
        if (self.uid <= 0) return error.InvalidUserId;
        if (self.currency <= 0) return error.InvalidCurrency;
        if (self.amount <= 0) return error.InvalidAmount;
    }

    pub fn eql(a: Resolution, b: Resolution) bool {
        return std.meta.eql(a, b);
    }
};

pub const EventError = error{
    InvalidEventId,
    InvalidPaymentId,
    InvalidProviderId,
    InvalidRevision,
    InvalidUserId,
    InvalidCurrency,
    InvalidAmount,
    InvalidTimeout,
};

/// Only stable ready, in-flight, waiting, and terminal phases are persisted.
pub const Phase = union(enum) {
    accepted,
    funding_submit_ready: FundingPurpose,
    funding_submit_in_flight: FundingPurpose,
    funding_status_ready: FundingStatusPhase,
    funding_status_in_flight: FundingStatusPhase,
    transfer_submit_ready: TransferStage,
    transfer_submit_in_flight: TransferStage,
    transfer_lookup_ready: TransferLookupPhase,
    transfer_lookup_in_flight: TransferLookupPhase,
    authorized,
    completed: ResolutionKind,
    failed: FailureReason,
    quarantined: QuarantineReason,
};

pub const TransferLookupPhase = struct {
    stage: TransferStage,
    cause: ReconciliationCause,
};

pub const Entry = struct {
    authorization: Authorization,
    route: Route,
    pending_transfer: Transfer,
    /// Zero is reserved for CSG1 v1 entries admitted before expiry automation.
    expires_at_unix_seconds: i64 = 0,
    resolution: ?Resolution = null,
    resolution_source: ?ResolutionSource = null,
    resolution_transfer: ?Transfer = null,
    phase: Phase = .accepted,
    generation: u64 = 1,
    remote_attempts: u32 = 0,
    funding_attempts: u32 = 0,
};

pub const Status = struct {
    authorization: Authorization,
    expires_at_unix_seconds: i64 = 0,
    resolution: ?Resolution,
    resolution_source: ?ResolutionSource = null,
    state: PublicState,
    failure_reason: ?FailureReason = null,
    quarantine_reason: ?QuarantineReason = null,
};

pub const Summary = struct {
    accepted: usize = 0,
    pending: usize = 0,
    authorized: usize = 0,
    completed: usize = 0,
    failed: usize = 0,
    quarantined: usize = 0,
};

/// Read-only aggregate for operators. Optional payment IDs are deterministic
/// representatives, never cursors: callers must take a fresh snapshot after
/// acting on the underlying incident.
pub const OperationsSnapshot = struct {
    observed_at_unix_seconds: i64,
    capacity: usize,
    total: usize,
    states: Summary,
    ready_actions: usize = 0,
    in_flight_actions: usize = 0,
    remote_reconciliations: usize = 0,
    funding_reconciliations: usize = 0,
    provider_resolutions: usize = 0,
    expiration_resolutions: usize = 0,
    remote_attempts: u64 = 0,
    funding_attempts: u64 = 0,
    expired_unresolved: usize = 0,
    legacy_manual_expiry: usize = 0,
    max_deadline_lag_seconds: u64 = 0,
    oldest_open_age_seconds: u64 = 0,
    oldest_expired_payment_id: ?u128 = null,
    first_reconciliation_payment_id: ?u128 = null,
    first_failed_payment_id: ?u128 = null,
    first_quarantined_payment_id: ?u128 = null,
};

pub const AcceptDisposition = enum {
    inserted,
    existing,
};

pub const AcceptResult = struct {
    disposition: AcceptDisposition,
    status: Status,
};

pub const FundingRequest = struct {
    operation_id: u128,
    kind: funding.Kind,
    uid: i64,
    currency: i32,
    amount: i64,
    purpose: FundingPurpose,
};

/// Status reconciliation retains the complete immutable child request so an
/// existing FSG1 operation cannot be accepted by ID alone.
pub const FundingStatusRequest = struct {
    request: FundingRequest,
    cause: FundingReconciliationCause,
};

pub const RemoteSubmit = struct {
    stage: TransferStage,
    transfer: Transfer,
};

pub const RemoteLookup = struct {
    stage: TransferStage,
    transfer: Transfer,
    cause: ReconciliationCause,
};

pub const Action = union(enum) {
    remote_submit: RemoteSubmit,
    remote_lookup: RemoteLookup,
    funding_submit: FundingRequest,
    funding_status: FundingStatusRequest,
};

pub const Claim = struct {
    payment_id: u128,
    generation: u64,
    action: Action,
};

pub const RemoteSubmitOutcome = enum {
    applied,
    conflict,
    known_not_sent,
    ambiguous,
    definitely_rejected,
    operator_action,
    destination_mismatch,
};

pub const RemoteLookupOutcome = enum {
    exact,
    absent,
    retryable,
    permanent_failure,
    destination_mismatch,
    transfer_mismatch,
};

pub const FundingSubmitOutcome = enum {
    completed,
    pending,
    known_not_sent,
    ambiguous,
    rejected,
    quarantined,
    idempotency_conflict,
};

pub const FundingStatusOutcome = enum {
    completed,
    pending,
    not_found,
    retryable,
    rejected,
    quarantined,
    idempotency_conflict,
};

pub const Completion = union(enum) {
    remote_submit: RemoteSubmitOutcome,
    remote_lookup: RemoteLookupOutcome,
    funding_submit: FundingSubmitOutcome,
    funding_status: FundingStatusOutcome,
};

pub const LedgerError = EventError || RouteError || error{
    InvalidBinding,
    CapacityExceeded,
    IdempotencyConflict,
    PaymentNotFound,
    AuthorizationNotReady,
    ImmutableFieldMismatch,
    StaleRevision,
    OutOfOrderRevision,
    AmountExceedsAuthorization,
    VoidAmountMismatch,
    TerminalConflict,
    NoPendingAction,
    StaleClaim,
    CompletionKindMismatch,
    InvalidState,
    InvalidClock,
    ExpirationOverflow,
    GenerationOverflow,
    AttemptOverflow,
};

/// Allocation-free deterministic clearing state. CSG1 durability is layered on
/// top by `parix_clearing_store.zig`, which persists candidate state before
/// publishing a provider acknowledgement or side-effect claim.
pub fn Ledger(comptime capacity: usize) type {
    if (capacity == 0 or capacity > 4_096) @compileError("invalid clearing capacity");

    return struct {
        const Self = @This();

        binding: Binding,
        entries: [capacity]Entry = undefined,
        entry_count: usize = 0,
        snapshot_generation: u64 = 0,

        pub fn init(binding: Binding) LedgerError!Self {
            var self: Self = undefined;
            try initInto(&self, binding);
            return self;
        }

        pub fn initInto(out: *Self, binding: Binding) LedgerError!void {
            binding.validate() catch return error.InvalidBinding;
            out.binding = binding;
            out.entry_count = 0;
            out.snapshot_generation = 0;
        }

        pub fn entriesSlice(self: *const Self) []const Entry {
            return self.entries[0..self.entry_count];
        }

        pub fn restore(
            binding: Binding,
            restored_entries: []const Entry,
            snapshot_generation: u64,
        ) LedgerError!Self {
            var self: Self = undefined;
            try restoreInto(&self, binding, restored_entries, snapshot_generation);
            return self;
        }

        pub fn restoreInto(
            out: *Self,
            binding: Binding,
            restored_entries: []const Entry,
            snapshot_generation: u64,
        ) LedgerError!void {
            if (restored_entries.len > capacity) return error.CapacityExceeded;
            binding.validate() catch return error.InvalidBinding;
            out.binding = binding;
            @memcpy(out.entries[0..restored_entries.len], restored_entries);
            out.entry_count = restored_entries.len;
            out.snapshot_generation = snapshot_generation;
            try out.validate();
        }

        pub fn acceptAuthorization(
            self: *Self,
            authorization: Authorization,
            route: Route,
        ) LedgerError!AcceptResult {
            return self.acceptAuthorizationAt(authorization, route, null);
        }

        pub fn acceptAuthorizationAt(
            self: *Self,
            authorization: Authorization,
            route: Route,
            accepted_unix_seconds: ?i64,
        ) LedgerError!AcceptResult {
            try authorization.validate();
            try route.validate();
            if (authorization.provider_id != self.binding.provider_id) {
                return error.InvalidProviderId;
            }
            if (route.destination_id != self.binding.destination_id) {
                return error.InvalidDestination;
            }
            if (self.findEvent(authorization.event_id)) |found| {
                const existing = self.entries[found.entry_index];
                if (!found.authorization or
                    !existing.authorization.eql(authorization) or
                    !existing.route.eql(route))
                {
                    return error.IdempotencyConflict;
                }
                return .{ .disposition = .existing, .status = statusFor(existing) };
            }
            if (self.findEntryIndex(authorization.payment_id)) |_| {
                return error.IdempotencyConflict;
            }
            if (self.entry_count == capacity) return error.CapacityExceeded;

            const pending_transfer = try authorizationTransfer(authorization, route);
            const expires_at_unix_seconds: i64 = if (accepted_unix_seconds) |accepted| blk: {
                if (accepted <= 0) return error.InvalidClock;
                break :blk std.math.add(
                    i64,
                    accepted,
                    @as(i64, authorization.timeout_seconds),
                ) catch return error.ExpirationOverflow;
            } else 0;
            try self.bumpSnapshotGeneration();
            self.entries[self.entry_count] = .{
                .authorization = authorization,
                .route = route,
                .pending_transfer = pending_transfer,
                .expires_at_unix_seconds = expires_at_unix_seconds,
            };
            self.entry_count += 1;
            return .{
                .disposition = .inserted,
                .status = statusFor(self.entries[self.entry_count - 1]),
            };
        }

        /// Resolves provider-wide event identity before app-layer route policy
        /// is evaluated. Exact replay is safe because the ledger is bound to
        /// one immutable clearing-profile digest.
        pub fn preflightAuthorizationEvent(
            self: *const Self,
            authorization: Authorization,
        ) LedgerError!?AcceptResult {
            try authorization.validate();
            const found = self.findEvent(authorization.event_id) orelse return null;
            const existing = self.entries[found.entry_index];
            if (!found.authorization or !existing.authorization.eql(authorization)) {
                return error.IdempotencyConflict;
            }
            return .{ .disposition = .existing, .status = statusFor(existing) };
        }

        pub fn acceptResolution(self: *Self, resolution: Resolution) LedgerError!AcceptResult {
            try resolution.validate();
            if (resolution.provider_id != self.binding.provider_id) {
                return error.InvalidProviderId;
            }
            if (self.findEvent(resolution.event_id)) |found| {
                const existing = self.entries[found.entry_index];
                if (found.authorization or existing.resolution == null or
                    !existing.resolution.?.eql(resolution))
                {
                    return error.IdempotencyConflict;
                }
                return .{ .disposition = .existing, .status = statusFor(existing) };
            }

            const index = self.findEntryIndex(resolution.payment_id) orelse
                return error.PaymentNotFound;
            var candidate = self.entries[index];
            if (candidate.resolution) |existing| {
                if (existing.eql(resolution)) {
                    return .{ .disposition = .existing, .status = statusFor(candidate) };
                }
                return error.TerminalConflict;
            }
            if (candidate.phase != .authorized) return error.AuthorizationNotReady;
            try validateResolution(candidate.authorization, resolution);
            candidate.resolution = resolution;
            candidate.resolution_source = .provider;
            candidate.resolution_transfer = try resolutionTransfer(
                candidate.authorization,
                resolution,
                candidate.route,
            );
            candidate.phase = .{ .transfer_submit_ready = .resolution };
            try bumpEntryGeneration(&candidate);
            try self.bumpSnapshotGeneration();
            self.entries[index] = candidate;
            return .{ .disposition = .inserted, .status = statusFor(candidate) };
        }

        /// Persists at most one automatic expiry intent. Ambiguous or exposed
        /// effects keep their reconciliation phase; only work proven absent is
        /// stopped. Legacy CSG1 v1 entries use deadline zero and are skipped.
        pub fn expireNext(self: *Self, now_unix_seconds: i64) LedgerError!?Status {
            if (now_unix_seconds <= 0) return error.InvalidClock;
            for (self.entries[0..self.entry_count]) |*entry| {
                if (entry.expires_at_unix_seconds == 0 or
                    entry.expires_at_unix_seconds > now_unix_seconds or
                    entry.resolution != null or
                    isInFlight(entry.phase) or
                    isTerminal(entry.phase))
                {
                    continue;
                }

                var candidate = entry.*;
                const resolution = expirationResolution(candidate.authorization);
                if (self.findEvent(resolution.event_id)) |_| return error.IdempotencyConflict;
                candidate.resolution = resolution;
                candidate.resolution_source = .expiration;
                candidate.resolution_transfer = try resolutionTransfer(
                    candidate.authorization,
                    resolution,
                    candidate.route,
                );
                try stageExpiration(&candidate);
                try bumpEntryGeneration(&candidate);
                try self.bumpSnapshotGeneration();
                entry.* = candidate;
                return statusFor(candidate);
            }
            return null;
        }

        /// Resolves provider-wide event identity before payment lookup or
        /// app-layer pair validation, preserving deterministic replay errors.
        pub fn preflightResolutionEvent(
            self: *const Self,
            resolution: Resolution,
        ) LedgerError!?AcceptResult {
            try resolution.validate();
            const found = self.findEvent(resolution.event_id) orelse return null;
            const existing = self.entries[found.entry_index];
            if (found.authorization or existing.resolution == null or
                !existing.resolution.?.eql(resolution))
            {
                return error.IdempotencyConflict;
            }
            return .{ .disposition = .existing, .status = statusFor(existing) };
        }

        pub fn lookup(self: *const Self, payment_id: u128) LedgerError!Status {
            const index = self.findEntryIndex(payment_id) orelse return error.PaymentNotFound;
            return statusFor(self.entries[index]);
        }

        pub fn summary(self: *const Self) Summary {
            var result: Summary = .{};
            for (self.entries[0..self.entry_count]) |entry| {
                switch (statusFor(entry).state) {
                    .accepted => result.accepted += 1,
                    .pending => result.pending += 1,
                    .authorized => result.authorized += 1,
                    .completed => result.completed += 1,
                    .failed => result.failed += 1,
                    .quarantined => result.quarantined += 1,
                }
            }
            return result;
        }

        pub fn operationsSnapshot(
            self: *const Self,
            observed_at_unix_seconds: i64,
        ) LedgerError!OperationsSnapshot {
            if (observed_at_unix_seconds <= 0) return error.InvalidClock;
            var result: OperationsSnapshot = .{
                .observed_at_unix_seconds = observed_at_unix_seconds,
                .capacity = capacity,
                .total = self.entry_count,
                .states = self.summary(),
            };
            var oldest_expired_deadline: ?i64 = null;

            for (self.entries[0..self.entry_count]) |entry| {
                result.remote_attempts += entry.remote_attempts;
                result.funding_attempts += entry.funding_attempts;

                switch (entry.phase) {
                    .accepted,
                    .funding_submit_ready,
                    .funding_status_ready,
                    .transfer_submit_ready,
                    .transfer_lookup_ready,
                    => result.ready_actions += 1,
                    .funding_submit_in_flight,
                    .funding_status_in_flight,
                    .transfer_submit_in_flight,
                    .transfer_lookup_in_flight,
                    => result.in_flight_actions += 1,
                    .authorized, .completed, .failed, .quarantined => {},
                }
                switch (entry.phase) {
                    .transfer_lookup_ready, .transfer_lookup_in_flight => {
                        result.remote_reconciliations += 1;
                        keepSmallestPaymentId(
                            &result.first_reconciliation_payment_id,
                            entry.authorization.payment_id,
                        );
                    },
                    .funding_status_ready, .funding_status_in_flight => {
                        result.funding_reconciliations += 1;
                        keepSmallestPaymentId(
                            &result.first_reconciliation_payment_id,
                            entry.authorization.payment_id,
                        );
                    },
                    else => {},
                }
                if (entry.resolution_source) |source| switch (source) {
                    .provider => result.provider_resolutions += 1,
                    .expiration => result.expiration_resolutions += 1,
                };

                const state = statusFor(entry).state;
                switch (state) {
                    .failed => keepSmallestPaymentId(
                        &result.first_failed_payment_id,
                        entry.authorization.payment_id,
                    ),
                    .quarantined => keepSmallestPaymentId(
                        &result.first_quarantined_payment_id,
                        entry.authorization.payment_id,
                    ),
                    else => {},
                }
                if (isTerminal(entry.phase)) continue;
                if (entry.expires_at_unix_seconds == 0) {
                    result.legacy_manual_expiry += 1;
                    continue;
                }

                const accepted_at = entry.expires_at_unix_seconds -
                    @as(i64, entry.authorization.timeout_seconds);
                if (observed_at_unix_seconds > accepted_at) {
                    const age = @as(u64, @intCast(observed_at_unix_seconds - accepted_at));
                    result.oldest_open_age_seconds = @max(
                        result.oldest_open_age_seconds,
                        age,
                    );
                }
                if (entry.resolution != null or
                    observed_at_unix_seconds < entry.expires_at_unix_seconds)
                {
                    continue;
                }

                result.expired_unresolved += 1;
                const lag = @as(u64, @intCast(
                    observed_at_unix_seconds - entry.expires_at_unix_seconds,
                ));
                result.max_deadline_lag_seconds = @max(
                    result.max_deadline_lag_seconds,
                    lag,
                );
                if (oldest_expired_deadline == null or
                    entry.expires_at_unix_seconds < oldest_expired_deadline.? or
                    (entry.expires_at_unix_seconds == oldest_expired_deadline.? and
                        entry.authorization.payment_id < result.oldest_expired_payment_id.?))
                {
                    oldest_expired_deadline = entry.expires_at_unix_seconds;
                    result.oldest_expired_payment_id = entry.authorization.payment_id;
                }
            }
            return result;
        }

        pub fn claimNext(self: *Self) LedgerError!?Claim {
            for (self.entries[0..self.entry_count]) |*entry| {
                if (isIdleOrTerminal(entry.phase)) continue;
                if (try self.claimEntry(entry)) |claim| return claim;
            }
            return null;
        }

        pub fn complete(
            self: *Self,
            claim: Claim,
            completion: Completion,
        ) LedgerError!Status {
            const index = self.findEntryIndex(claim.payment_id) orelse
                return error.PaymentNotFound;
            var candidate = self.entries[index];
            if (candidate.generation != claim.generation) return error.StaleClaim;
            const expected_action = actionForInFlight(self.binding, candidate) orelse
                return error.StaleClaim;
            if (!std.meta.eql(claim.action, expected_action)) return error.StaleClaim;

            switch (candidate.phase) {
                .transfer_submit_in_flight => |stage| {
                    const outcome = switch (completion) {
                        .remote_submit => |value| value,
                        else => return error.CompletionKindMismatch,
                    };
                    try completeRemoteSubmit(&candidate, stage, outcome);
                },
                .transfer_lookup_in_flight => |lookup_phase| {
                    const outcome = switch (completion) {
                        .remote_lookup => |value| value,
                        else => return error.CompletionKindMismatch,
                    };
                    try completeRemoteLookup(&candidate, lookup_phase, outcome);
                },
                .funding_submit_in_flight => |purpose| {
                    const outcome = switch (completion) {
                        .funding_submit => |value| value,
                        else => return error.CompletionKindMismatch,
                    };
                    try completeFundingSubmit(&candidate, purpose, outcome);
                },
                .funding_status_in_flight => |status_phase| {
                    const outcome = switch (completion) {
                        .funding_status => |value| value,
                        else => return error.CompletionKindMismatch,
                    };
                    try completeFundingStatus(&candidate, status_phase, outcome);
                },
                else => return error.StaleClaim,
            }
            try bumpEntryGeneration(&candidate);
            try self.bumpSnapshotGeneration();
            self.entries[index] = candidate;
            return statusFor(candidate);
        }

        pub fn recover(self: *Self) LedgerError!usize {
            var recovered: usize = 0;
            for (self.entries[0..self.entry_count]) |entry| {
                switch (entry.phase) {
                    .transfer_submit_in_flight,
                    .transfer_lookup_in_flight,
                    .funding_submit_in_flight,
                    .funding_status_in_flight,
                    => {
                        if (entry.generation == std.math.maxInt(u64)) {
                            return error.GenerationOverflow;
                        }
                        recovered += 1;
                    },
                    else => {},
                }
            }
            if (recovered == 0) return 0;
            if (self.snapshot_generation == std.math.maxInt(u64)) {
                return error.GenerationOverflow;
            }
            for (self.entries[0..self.entry_count]) |*entry| {
                switch (entry.phase) {
                    .transfer_submit_in_flight => |stage| entry.phase = .{
                        .transfer_lookup_ready = .{
                            .stage = stage,
                            .cause = .ambiguous_submission,
                        },
                    },
                    .transfer_lookup_in_flight => |lookup_phase| entry.phase = .{
                        .transfer_lookup_ready = lookup_phase,
                    },
                    .funding_submit_in_flight => |purpose| entry.phase = .{
                        .funding_status_ready = .{
                            .purpose = purpose,
                            .cause = .ambiguous_submission,
                        },
                    },
                    .funding_status_in_flight => |status_phase| entry.phase = .{
                        .funding_status_ready = status_phase,
                    },
                    else => continue,
                }
                entry.generation += 1;
            }
            self.snapshot_generation += 1;
            return recovered;
        }

        pub fn validate(self: *const Self) LedgerError!void {
            self.binding.validate() catch return error.InvalidBinding;
            if (self.entry_count > capacity) return error.InvalidState;
            const minimum_snapshot_generation = std.math.cast(u64, self.entry_count) orelse
                return error.InvalidState;
            if (self.snapshot_generation < minimum_snapshot_generation) {
                return error.InvalidState;
            }
            for (self.entries[0..self.entry_count], 0..) |entry, index| {
                entry.authorization.validate() catch return error.InvalidState;
                entry.route.validate() catch return error.InvalidState;
                const minimum_entry_generation = @as(u64, 1) +
                    @as(u64, entry.remote_attempts) + @as(u64, entry.funding_attempts);
                if (entry.authorization.provider_id != self.binding.provider_id or
                    entry.route.destination_id != self.binding.destination_id or
                    entry.expires_at_unix_seconds < 0 or
                    entry.generation < minimum_entry_generation or
                    self.snapshot_generation < entry.generation or
                    !std.meta.eql(entry.pending_transfer, authorizationTransfer(
                        entry.authorization,
                        entry.route,
                    ) catch return error.InvalidState) or
                    !validPhase(entry) or
                    !validAttemptEvidence(entry))
                {
                    return error.InvalidState;
                }
                if (entry.resolution) |resolution| {
                    const source = entry.resolution_source orelse return error.InvalidState;
                    validateResolution(entry.authorization, resolution) catch
                        return error.InvalidState;
                    if (source == .expiration and
                        (entry.expires_at_unix_seconds == 0 or
                            !resolution.eql(expirationResolution(entry.authorization))))
                    {
                        return error.InvalidState;
                    }
                    const expected = resolutionTransfer(
                        entry.authorization,
                        resolution,
                        entry.route,
                    ) catch return error.InvalidState;
                    if (entry.resolution_transfer == null or
                        !std.meta.eql(entry.resolution_transfer.?, expected))
                    {
                        return error.InvalidState;
                    }
                } else if (entry.resolution_source != null or entry.resolution_transfer != null) {
                    return error.InvalidState;
                }
                for (self.entries[0..index]) |previous| {
                    if (previous.authorization.payment_id == entry.authorization.payment_id or
                        previous.authorization.event_id == entry.authorization.event_id or
                        (previous.resolution != null and
                            previous.resolution.?.event_id == entry.authorization.event_id) or
                        previous.pending_transfer.id == entry.pending_transfer.id)
                    {
                        return error.InvalidState;
                    }
                    if (previous.resolution_transfer != null and
                        previous.resolution_transfer.?.id == entry.pending_transfer.id)
                    {
                        return error.InvalidState;
                    }
                    if (entry.resolution) |resolution| {
                        if (previous.authorization.event_id == resolution.event_id or
                            (previous.resolution != null and
                                previous.resolution.?.event_id == resolution.event_id))
                        {
                            return error.InvalidState;
                        }
                    }
                    if (entry.resolution_transfer) |terminal_transfer| {
                        if (previous.pending_transfer.id == terminal_transfer.id or
                            (previous.resolution_transfer != null and
                                previous.resolution_transfer.?.id == terminal_transfer.id))
                        {
                            return error.InvalidState;
                        }
                    }
                }
            }
        }

        fn claimEntry(self: *Self, entry: *Entry) LedgerError!?Claim {
            const action: Action = switch (entry.phase) {
                .accepted => switch (entry.authorization.direction) {
                    .deposit => .{ .remote_submit = .{
                        .stage = .authorization,
                        .transfer = entry.pending_transfer,
                    } },
                    .withdrawal => .{ .funding_submit = fundingRequest(
                        self.binding,
                        entry.*,
                        .withdrawal_debit,
                    ) },
                },
                .funding_submit_ready => |purpose| .{
                    .funding_submit = fundingRequest(self.binding, entry.*, purpose),
                },
                .funding_status_ready => |status_phase| .{
                    .funding_status = .{
                        .request = fundingRequest(
                            self.binding,
                            entry.*,
                            status_phase.purpose,
                        ),
                        .cause = status_phase.cause,
                    },
                },
                .transfer_submit_ready => |stage| .{ .remote_submit = .{
                    .stage = stage,
                    .transfer = transferFor(entry.*, stage),
                } },
                .transfer_lookup_ready => |lookup_phase| .{ .remote_lookup = .{
                    .stage = lookup_phase.stage,
                    .transfer = transferFor(entry.*, lookup_phase.stage),
                    .cause = lookup_phase.cause,
                } },
                .funding_submit_in_flight,
                .funding_status_in_flight,
                .transfer_submit_in_flight,
                .transfer_lookup_in_flight,
                => return null,
                .authorized, .completed, .failed, .quarantined => unreachable,
            };

            var candidate = entry.*;
            switch (action) {
                .remote_submit => |submit| {
                    candidate.remote_attempts = std.math.add(u32, candidate.remote_attempts, 1) catch
                        return error.AttemptOverflow;
                    candidate.phase = .{ .transfer_submit_in_flight = submit.stage };
                },
                .remote_lookup => |lookup_action| {
                    candidate.remote_attempts = std.math.add(u32, candidate.remote_attempts, 1) catch
                        return error.AttemptOverflow;
                    candidate.phase = .{ .transfer_lookup_in_flight = .{
                        .stage = lookup_action.stage,
                        .cause = lookup_action.cause,
                    } };
                },
                .funding_submit => |request| {
                    candidate.funding_attempts = std.math.add(u32, candidate.funding_attempts, 1) catch
                        return error.AttemptOverflow;
                    candidate.phase = .{ .funding_submit_in_flight = request.purpose };
                },
                .funding_status => |request| {
                    candidate.funding_attempts = std.math.add(u32, candidate.funding_attempts, 1) catch
                        return error.AttemptOverflow;
                    candidate.phase = .{ .funding_status_in_flight = .{
                        .purpose = request.request.purpose,
                        .cause = request.cause,
                    } };
                },
            }
            try bumpEntryGeneration(&candidate);
            const next_snapshot_generation = std.math.add(
                u64,
                self.snapshot_generation,
                1,
            ) catch return error.GenerationOverflow;
            entry.* = candidate;
            self.snapshot_generation = next_snapshot_generation;
            return .{
                .payment_id = candidate.authorization.payment_id,
                .generation = candidate.generation,
                .action = action,
            };
        }

        fn findEntryIndex(self: *const Self, payment_id: u128) ?usize {
            for (self.entries[0..self.entry_count], 0..) |entry, index| {
                if (entry.authorization.payment_id == payment_id) return index;
            }
            return null;
        }

        const FoundEvent = struct {
            entry_index: usize,
            authorization: bool,
        };

        fn findEvent(self: *const Self, event_id: u128) ?FoundEvent {
            for (self.entries[0..self.entry_count], 0..) |entry, index| {
                if (entry.authorization.event_id == event_id) {
                    return .{ .entry_index = index, .authorization = true };
                }
                if (entry.resolution) |resolution| {
                    if (resolution.event_id == event_id) {
                        return .{ .entry_index = index, .authorization = false };
                    }
                }
            }
            return null;
        }

        fn bumpSnapshotGeneration(self: *Self) LedgerError!void {
            self.snapshot_generation = std.math.add(u64, self.snapshot_generation, 1) catch
                return error.GenerationOverflow;
        }
    };
}

pub const DefaultLedger = Ledger(4_096);

pub fn authorizationTransfer(authorization: Authorization, route: Route) (EventError || RouteError)!Transfer {
    try authorization.validate();
    try route.validate();
    return .{
        .id = deterministicTransferId(.authorization, authorization, null, route),
        .debit_account_id = switch (authorization.direction) {
            .deposit => route.provider_control_account_id,
            .withdrawal => route.treasury_account_id,
        },
        .credit_account_id = switch (authorization.direction) {
            .deposit => route.treasury_account_id,
            .withdrawal => route.provider_control_account_id,
        },
        .amount = @intCast(authorization.amount),
        .pending_id = 0,
        .user_data_128 = authorization.payment_id,
        .user_data_64 = authorization.provider_id,
        .user_data_32 = workflow_revision,
        .timeout = authorization.timeout_seconds,
        .ledger = route.ledger,
        .code = route.transfer_code,
        .flags = settlement.pending_flag,
    };
}

pub fn resolutionTransfer(
    authorization: Authorization,
    resolution: Resolution,
    route: Route,
) (EventError || RouteError || LedgerError)!Transfer {
    try authorization.validate();
    try resolution.validate();
    try route.validate();
    try validateResolution(authorization, resolution);
    const pending = try authorizationTransfer(authorization, route);
    const id = deterministicTransferId(.resolution, authorization, resolution, route);
    if (id == pending.id) return error.InvalidState;
    return .{
        .id = id,
        .debit_account_id = pending.debit_account_id,
        .credit_account_id = pending.credit_account_id,
        .amount = @intCast(resolution.amount),
        .pending_id = pending.id,
        .user_data_128 = authorization.payment_id,
        .user_data_64 = authorization.provider_id,
        .user_data_32 = workflow_revision,
        .timeout = 0,
        .ledger = route.ledger,
        .code = route.transfer_code,
        .flags = switch (resolution.kind) {
            .post => settlement.post_pending_transfer_flag,
            .void => settlement.void_pending_transfer_flag,
        },
    };
}

pub fn deterministicTransferId(
    stage: TransferStage,
    authorization: Authorization,
    resolution: ?Resolution,
    route: Route,
) u128 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(transfer_id_domain);
    hashInt(&hasher, u8, @intFromEnum(stage));
    hashInt(&hasher, u128, @intFromEnum(route.destination_id));
    hashAuthorization(&hasher, authorization);
    hashRoute(&hasher, route);
    if (resolution) |value| hashResolution(&hasher, value);
    return finishStableId(&hasher);
}

pub fn fundingOperationId(binding: Binding, entry: Entry, purpose: FundingPurpose) u128 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(funding_id_domain);
    hashBinding(&hasher, binding);
    hashAuthorization(&hasher, entry.authorization);
    hashRoute(&hasher, entry.route);
    hashInt(&hasher, u8, @intFromEnum(purpose));
    hashInt(&hasher, i64, fundingAmount(entry, purpose));
    if (entry.resolution) |resolution| hashResolution(&hasher, resolution);
    return finishStableId(&hasher);
}

pub fn expirationResolution(authorization: Authorization) Resolution {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(expiration_event_id_domain);
    hashAuthorization(&hasher, authorization);
    var event_id = finishStableId(&hasher);
    if (event_id == authorization.event_id) {
        event_id ^= 1;
        if (event_id == std.math.maxInt(u128)) event_id -= 2;
    }
    return .{
        .event_id = event_id,
        .payment_id = authorization.payment_id,
        .provider_id = authorization.provider_id,
        .revision = authorization.revision + 1,
        .kind = .void,
        .direction = authorization.direction,
        .uid = authorization.uid,
        .currency = authorization.currency,
        .amount = authorization.amount,
    };
}

fn validateResolution(authorization: Authorization, resolution: Resolution) LedgerError!void {
    if (resolution.event_id == authorization.event_id) {
        return error.IdempotencyConflict;
    }
    if (resolution.payment_id != authorization.payment_id or
        resolution.provider_id != authorization.provider_id or
        resolution.direction != authorization.direction or
        resolution.uid != authorization.uid or
        resolution.currency != authorization.currency)
    {
        return error.ImmutableFieldMismatch;
    }
    if (resolution.revision <= authorization.revision) return error.StaleRevision;
    const next_revision = std.math.add(u64, authorization.revision, 1) catch
        return error.OutOfOrderRevision;
    if (resolution.revision != next_revision) return error.OutOfOrderRevision;
    switch (resolution.kind) {
        .post => if (resolution.amount > authorization.amount) {
            return error.AmountExceedsAuthorization;
        },
        .void => if (resolution.amount != authorization.amount) {
            return error.VoidAmountMismatch;
        },
    }
}

fn transferFor(entry: Entry, stage: TransferStage) Transfer {
    return switch (stage) {
        .authorization => entry.pending_transfer,
        .resolution => entry.resolution_transfer.?,
    };
}

fn fundingRequest(binding: Binding, entry: Entry, purpose: FundingPurpose) FundingRequest {
    return .{
        .operation_id = fundingOperationId(binding, entry, purpose),
        .kind = switch (purpose) {
            .withdrawal_debit => .withdrawal,
            .deposit_credit, .withdrawal_refund => .deposit,
        },
        .uid = entry.authorization.uid,
        .currency = entry.authorization.currency,
        .amount = fundingAmount(entry, purpose),
        .purpose = purpose,
    };
}

fn fundingAmount(entry: Entry, purpose: FundingPurpose) i64 {
    return switch (purpose) {
        .withdrawal_debit => entry.authorization.amount,
        .deposit_credit => entry.resolution.?.amount,
        .withdrawal_refund => if (entry.resolution) |resolution|
            switch (resolution.kind) {
                .void => entry.authorization.amount,
                .post => entry.authorization.amount - resolution.amount,
            }
        else
            entry.authorization.amount,
    };
}

fn actionForInFlight(binding: Binding, entry: Entry) ?Action {
    return switch (entry.phase) {
        .transfer_submit_in_flight => |stage| .{ .remote_submit = .{
            .stage = stage,
            .transfer = transferFor(entry, stage),
        } },
        .transfer_lookup_in_flight => |lookup_phase| .{ .remote_lookup = .{
            .stage = lookup_phase.stage,
            .transfer = transferFor(entry, lookup_phase.stage),
            .cause = lookup_phase.cause,
        } },
        .funding_submit_in_flight => |purpose| .{
            .funding_submit = fundingRequest(binding, entry, purpose),
        },
        .funding_status_in_flight => |status_phase| .{ .funding_status = .{
            .request = fundingRequest(binding, entry, status_phase.purpose),
            .cause = status_phase.cause,
        } },
        else => null,
    };
}

fn completeRemoteSubmit(
    entry: *Entry,
    stage: TransferStage,
    outcome: RemoteSubmitOutcome,
) LedgerError!void {
    switch (outcome) {
        .applied => try remoteCommitted(entry, stage),
        .conflict => entry.phase = .{ .transfer_lookup_ready = .{
            .stage = stage,
            .cause = .conflict_response,
        } },
        .ambiguous => entry.phase = .{ .transfer_lookup_ready = .{
            .stage = stage,
            .cause = .ambiguous_submission,
        } },
        .known_not_sent => {
            if (stage == .authorization and isExpiration(entry.*)) {
                authorizationAbsent(entry);
            } else {
                entry.phase = .{ .transfer_submit_ready = stage };
            }
        },
        .definitely_rejected => try remoteDefinitelyRejected(entry, stage),
        .operator_action => entry.phase = .{ .quarantined = .reconciliation_failed },
        .destination_mismatch => entry.phase = .{ .quarantined = .destination_mismatch },
    }
}

fn completeRemoteLookup(
    entry: *Entry,
    lookup_phase: TransferLookupPhase,
    outcome: RemoteLookupOutcome,
) LedgerError!void {
    switch (outcome) {
        .exact => try remoteCommitted(entry, lookup_phase.stage),
        .absent => switch (lookup_phase.cause) {
            .ambiguous_submission => {
                if (lookup_phase.stage == .authorization and isExpiration(entry.*)) {
                    authorizationAbsent(entry);
                } else {
                    entry.phase = .{ .transfer_submit_ready = lookup_phase.stage };
                }
            },
            .conflict_response => entry.phase = .{ .quarantined = .reconciliation_failed },
        },
        .retryable => entry.phase = .{ .transfer_lookup_ready = lookup_phase },
        .permanent_failure => entry.phase = .{ .quarantined = .reconciliation_failed },
        .destination_mismatch => entry.phase = .{ .quarantined = .destination_mismatch },
        .transfer_mismatch => entry.phase = .{ .quarantined = .transfer_mismatch },
    }
}

fn remoteCommitted(entry: *Entry, stage: TransferStage) LedgerError!void {
    switch (stage) {
        .authorization => entry.phase = if (isExpiration(entry.*))
            .{ .transfer_submit_ready = .resolution }
        else
            .authorized,
        .resolution => {
            const resolution = entry.resolution orelse return error.InvalidState;
            entry.phase = switch (entry.authorization.direction) {
                .deposit => switch (resolution.kind) {
                    .post => .{ .funding_submit_ready = .deposit_credit },
                    .void => .{ .completed = .void },
                },
                .withdrawal => switch (resolution.kind) {
                    .post => if (resolution.amount == entry.authorization.amount)
                        .{ .completed = .post }
                    else
                        .{ .funding_submit_ready = .withdrawal_refund },
                    .void => .{ .funding_submit_ready = .withdrawal_refund },
                },
            };
        },
    }
}

fn remoteDefinitelyRejected(entry: *Entry, stage: TransferStage) LedgerError!void {
    switch (stage) {
        .authorization => {
            if (isExpiration(entry.*)) {
                authorizationAbsent(entry);
            } else entry.phase = switch (entry.authorization.direction) {
                .deposit => .{ .failed = .authorization_rejected },
                .withdrawal => .{ .funding_submit_ready = .withdrawal_refund },
            };
        },
        .resolution => entry.phase = .{ .quarantined = .terminal_conflict },
    }
}

fn completeFundingSubmit(
    entry: *Entry,
    purpose: FundingPurpose,
    outcome: FundingSubmitOutcome,
) LedgerError!void {
    switch (outcome) {
        .completed => try fundingCommitted(entry, purpose),
        .pending => entry.phase = .{ .funding_status_ready = .{
            .purpose = purpose,
            .cause = .accepted_operation,
        } },
        .known_not_sent => {
            if (purpose == .withdrawal_debit and isExpiration(entry.*)) {
                entry.phase = .{ .completed = .void };
            } else {
                entry.phase = .{ .funding_submit_ready = purpose };
            }
        },
        .ambiguous => entry.phase = .{ .funding_status_ready = .{
            .purpose = purpose,
            .cause = .ambiguous_submission,
        } },
        .rejected => fundingRejected(entry, purpose),
        .quarantined, .idempotency_conflict => entry.phase = .{
            .quarantined = .funding_conflict,
        },
    }
}

fn completeFundingStatus(
    entry: *Entry,
    status_phase: FundingStatusPhase,
    outcome: FundingStatusOutcome,
) LedgerError!void {
    switch (outcome) {
        .completed => try fundingCommitted(entry, status_phase.purpose),
        .pending, .retryable => entry.phase = .{ .funding_status_ready = status_phase },
        .not_found => entry.phase = switch (status_phase.cause) {
            .ambiguous_submission => if (status_phase.purpose == .withdrawal_debit and
                isExpiration(entry.*))
                .{ .completed = .void }
            else
                .{ .funding_submit_ready = status_phase.purpose },
            .accepted_operation => .{ .quarantined = .funding_conflict },
        },
        .rejected => fundingRejected(entry, status_phase.purpose),
        .quarantined, .idempotency_conflict => entry.phase = .{
            .quarantined = .funding_conflict,
        },
    }
}

fn fundingRejected(entry: *Entry, purpose: FundingPurpose) void {
    entry.phase = switch (purpose) {
        .withdrawal_debit => if (isExpiration(entry.*))
            .{ .completed = .void }
        else
            .{ .failed = .funding_rejected },
        .deposit_credit => .{ .quarantined = .funding_failed_after_external_commit },
        .withdrawal_refund => .{ .quarantined = .refund_failed },
    };
}

fn fundingCommitted(entry: *Entry, purpose: FundingPurpose) LedgerError!void {
    entry.phase = switch (purpose) {
        .withdrawal_debit => if (entry.authorization.direction == .withdrawal and
            entry.resolution == null)
            .{ .transfer_submit_ready = .authorization }
        else if (entry.authorization.direction == .withdrawal and isExpiration(entry.*))
            .{ .funding_submit_ready = .withdrawal_refund }
        else
            return error.InvalidState,
        .deposit_credit => if (entry.authorization.direction == .deposit and
            entry.resolution != null and entry.resolution.?.kind == .post)
            .{ .completed = .post }
        else
            return error.InvalidState,
        .withdrawal_refund => if (entry.authorization.direction == .withdrawal)
            if (entry.resolution) |resolution|
                .{ .completed = resolution.kind }
            else
                .{ .failed = .authorization_rejected }
        else
            return error.InvalidState,
    };
}

fn statusFor(entry: Entry) Status {
    return switch (entry.phase) {
        .accepted => .{
            .authorization = entry.authorization,
            .expires_at_unix_seconds = entry.expires_at_unix_seconds,
            .resolution = entry.resolution,
            .resolution_source = entry.resolution_source,
            .state = .accepted,
        },
        .authorized => .{
            .authorization = entry.authorization,
            .expires_at_unix_seconds = entry.expires_at_unix_seconds,
            .resolution = entry.resolution,
            .resolution_source = entry.resolution_source,
            .state = .authorized,
        },
        .completed => .{
            .authorization = entry.authorization,
            .expires_at_unix_seconds = entry.expires_at_unix_seconds,
            .resolution = entry.resolution,
            .resolution_source = entry.resolution_source,
            .state = .completed,
        },
        .failed => |reason| .{
            .authorization = entry.authorization,
            .expires_at_unix_seconds = entry.expires_at_unix_seconds,
            .resolution = entry.resolution,
            .resolution_source = entry.resolution_source,
            .state = .failed,
            .failure_reason = reason,
        },
        .quarantined => |reason| .{
            .authorization = entry.authorization,
            .expires_at_unix_seconds = entry.expires_at_unix_seconds,
            .resolution = entry.resolution,
            .resolution_source = entry.resolution_source,
            .state = .quarantined,
            .quarantine_reason = reason,
        },
        else => .{
            .authorization = entry.authorization,
            .expires_at_unix_seconds = entry.expires_at_unix_seconds,
            .resolution = entry.resolution,
            .resolution_source = entry.resolution_source,
            .state = .pending,
        },
    };
}

fn stageExpiration(entry: *Entry) LedgerError!void {
    std.debug.assert(isExpiration(entry.*));
    entry.phase = switch (entry.phase) {
        .accepted => .{ .completed = .void },
        .authorized => .{ .transfer_submit_ready = .resolution },
        .funding_submit_ready => |purpose| switch (purpose) {
            .withdrawal_debit => .{ .completed = .void },
            .withdrawal_refund => entry.phase,
            .deposit_credit => return error.InvalidState,
        },
        .funding_status_ready,
        => entry.phase,
        .transfer_submit_ready => |stage| switch (stage) {
            .authorization => blk: {
                authorizationAbsent(entry);
                break :blk entry.phase;
            },
            .resolution => return error.InvalidState,
        },
        .transfer_lookup_ready,
        => entry.phase,
        .funding_submit_in_flight,
        .funding_status_in_flight,
        .transfer_submit_in_flight,
        .transfer_lookup_in_flight,
        => return error.InvalidState,
        .completed, .failed, .quarantined => return error.InvalidState,
    };
}

fn authorizationAbsent(entry: *Entry) void {
    std.debug.assert(isExpiration(entry.*));
    entry.phase = switch (entry.authorization.direction) {
        .deposit => .{ .completed = .void },
        .withdrawal => if (entry.funding_attempts == 0)
            .{ .completed = .void }
        else
            .{ .funding_submit_ready = .withdrawal_refund },
    };
}

fn isExpiration(entry: Entry) bool {
    return entry.resolution_source == .expiration;
}

fn isTerminal(phase: Phase) bool {
    return switch (phase) {
        .completed, .failed, .quarantined => true,
        else => false,
    };
}

fn isInFlight(phase: Phase) bool {
    return switch (phase) {
        .funding_submit_in_flight,
        .funding_status_in_flight,
        .transfer_submit_in_flight,
        .transfer_lookup_in_flight,
        => true,
        else => false,
    };
}

fn isIdleOrTerminal(phase: Phase) bool {
    return switch (phase) {
        .authorized, .completed, .failed, .quarantined => true,
        else => false,
    };
}

fn keepSmallestPaymentId(current: *?u128, candidate: u128) void {
    if (current.* == null or candidate < current.*.?) current.* = candidate;
}

fn validPhase(entry: Entry) bool {
    return switch (entry.phase) {
        .accepted => entry.resolution == null,
        .funding_submit_ready,
        .funding_submit_in_flight,
        => |purpose| validFundingPurpose(entry, purpose),
        .funding_status_ready,
        .funding_status_in_flight,
        => |status_phase| validFundingPurpose(entry, status_phase.purpose),
        .transfer_submit_ready, .transfer_submit_in_flight => |stage| validStage(entry, stage),
        .transfer_lookup_ready, .transfer_lookup_in_flight => |lookup_phase| {
            return validStage(entry, lookup_phase.stage);
        },
        .authorized => entry.resolution == null,
        .completed => |kind| entry.resolution != null and entry.resolution.?.kind == kind,
        .failed => |reason| switch (reason) {
            .funding_rejected => entry.authorization.direction == .withdrawal and
                entry.resolution == null,
            .authorization_rejected => entry.resolution == null,
        },
        .quarantined => true,
    };
}

const FundingAttemptStage = enum {
    submit_ready,
    submit_in_flight,
    status_ready,
    status_in_flight,
};

const TransferAttemptStage = enum {
    submit_ready,
    submit_in_flight,
    lookup_ready,
    lookup_in_flight,
};

fn validAttemptEvidence(entry: Entry) bool {
    return switch (entry.phase) {
        .accepted => entry.remote_attempts == 0 and entry.funding_attempts == 0,
        .funding_submit_ready => |purpose| validFundingAttemptEvidence(
            entry,
            purpose,
            .submit_ready,
        ),
        .funding_submit_in_flight => |purpose| validFundingAttemptEvidence(
            entry,
            purpose,
            .submit_in_flight,
        ),
        .funding_status_ready => |status_phase| validFundingAttemptEvidence(
            entry,
            status_phase.purpose,
            .status_ready,
        ),
        .funding_status_in_flight => |status_phase| validFundingAttemptEvidence(
            entry,
            status_phase.purpose,
            .status_in_flight,
        ),
        .transfer_submit_ready => |stage| validTransferAttemptEvidence(
            entry,
            stage,
            .submit_ready,
        ),
        .transfer_submit_in_flight => |stage| validTransferAttemptEvidence(
            entry,
            stage,
            .submit_in_flight,
        ),
        .transfer_lookup_ready => |lookup_phase| validTransferAttemptEvidence(
            entry,
            lookup_phase.stage,
            .lookup_ready,
        ),
        .transfer_lookup_in_flight => |lookup_phase| validTransferAttemptEvidence(
            entry,
            lookup_phase.stage,
            .lookup_in_flight,
        ),
        .authorized => entry.remote_attempts >= 1 and validRemoteFundingBaseline(entry),
        .completed => validCompletedAttemptEvidence(entry),
        .failed => |reason| validFailedAttemptEvidence(entry, reason),
        .quarantined => |reason| validQuarantineAttemptEvidence(entry, reason),
    };
}

fn validFundingAttemptEvidence(
    entry: Entry,
    purpose: FundingPurpose,
    stage: FundingAttemptStage,
) bool {
    return switch (purpose) {
        .withdrawal_debit => entry.remote_attempts == 0 and
            entry.funding_attempts >= @as(u32, switch (stage) {
                .submit_ready, .submit_in_flight, .status_ready => 1,
                .status_in_flight => 2,
            }),
        .deposit_credit => entry.remote_attempts >= 2 and
            entry.funding_attempts >= @as(u32, switch (stage) {
                .submit_ready => 0,
                .submit_in_flight, .status_ready => 1,
                .status_in_flight => 2,
            }),
        .withdrawal_refund => entry.remote_attempts >=
            minimumRefundRemoteAttempts(entry) and
            entry.funding_attempts >= @as(u32, switch (stage) {
                .submit_ready => 1,
                .submit_in_flight, .status_ready => 2,
                .status_in_flight => 3,
            }),
    };
}

fn validTransferAttemptEvidence(
    entry: Entry,
    stage: TransferStage,
    attempt_stage: TransferAttemptStage,
) bool {
    if (!validRemoteFundingBaseline(entry)) return false;
    const minimum_remote_attempts: u32 = switch (stage) {
        .authorization => switch (attempt_stage) {
            .submit_ready => if (entry.authorization.direction == .withdrawal) 0 else 1,
            .submit_in_flight, .lookup_ready => 1,
            .lookup_in_flight => 2,
        },
        .resolution => switch (attempt_stage) {
            .submit_ready => 1,
            .submit_in_flight, .lookup_ready => 2,
            .lookup_in_flight => 3,
        },
    };
    return entry.remote_attempts >= minimum_remote_attempts;
}

fn validRemoteFundingBaseline(entry: Entry) bool {
    return switch (entry.authorization.direction) {
        .deposit => entry.funding_attempts == 0,
        .withdrawal => entry.funding_attempts >= 1,
    };
}

fn validCompletedAttemptEvidence(entry: Entry) bool {
    const resolution = entry.resolution orelse return false;
    if (isExpiration(entry)) {
        return resolution.kind == .void and switch (entry.authorization.direction) {
            .deposit => entry.funding_attempts == 0,
            .withdrawal => true,
        };
    }
    if (entry.remote_attempts < 2) return false;
    return switch (entry.authorization.direction) {
        .deposit => switch (resolution.kind) {
            .post => entry.funding_attempts >= 1,
            .void => entry.funding_attempts == 0,
        },
        .withdrawal => switch (resolution.kind) {
            .post => entry.funding_attempts >=
                @as(u32, if (resolution.amount == entry.authorization.amount) 1 else 2),
            .void => entry.funding_attempts >= 2,
        },
    };
}

fn validFailedAttemptEvidence(entry: Entry, reason: FailureReason) bool {
    return switch (reason) {
        .funding_rejected => entry.authorization.direction == .withdrawal and
            entry.resolution == null and entry.remote_attempts == 0 and
            entry.funding_attempts >= 1,
        .authorization_rejected => entry.resolution == null and
            entry.remote_attempts >= 1 and switch (entry.authorization.direction) {
            .deposit => entry.funding_attempts == 0,
            .withdrawal => entry.funding_attempts >= 2,
        },
    };
}

fn validQuarantineAttemptEvidence(entry: Entry, reason: QuarantineReason) bool {
    return switch (reason) {
        .destination_mismatch, .reconciliation_failed => validRemoteQuarantineEvidence(
            entry,
            1,
            2,
        ),
        .transfer_mismatch => validRemoteQuarantineEvidence(entry, 2, 3),
        .funding_conflict => validFundingConflictEvidence(entry),
        .funding_failed_after_external_commit => entry.authorization.direction == .deposit and
            isPostedResolution(entry.resolution) and entry.remote_attempts >= 2 and
            entry.funding_attempts >= 1,
        .refund_failed => entry.authorization.direction == .withdrawal and
            validRefundResolutionShape(entry) and entry.remote_attempts >=
            minimumRefundRemoteAttempts(entry) and
            entry.funding_attempts >= 2,
        .terminal_conflict => entry.resolution != null and entry.remote_attempts >= 2 and
            validRemoteFundingBaseline(entry),
    };
}

fn validRemoteQuarantineEvidence(
    entry: Entry,
    authorization_minimum: u32,
    resolution_minimum: u32,
) bool {
    const minimum = if (entry.resolution == null)
        authorization_minimum
    else
        resolution_minimum;
    return entry.remote_attempts >= minimum and validRemoteFundingBaseline(entry);
}

fn validFundingConflictEvidence(entry: Entry) bool {
    return switch (entry.authorization.direction) {
        .deposit => isPostedResolution(entry.resolution) and entry.remote_attempts >= 2 and
            entry.funding_attempts >= 1,
        .withdrawal => if (entry.resolution) |_| {
            return validRefundResolutionShape(entry) and
                entry.remote_attempts >= minimumRefundRemoteAttempts(entry) and
                entry.funding_attempts >= 2;
        } else if (entry.remote_attempts == 0) {
            return entry.funding_attempts >= 1;
        } else {
            return entry.funding_attempts >= 2;
        },
    };
}

fn isPostedResolution(resolution: ?Resolution) bool {
    return resolution != null and resolution.?.kind == .post;
}

fn validRefundResolutionShape(entry: Entry) bool {
    const resolution = entry.resolution orelse return true;
    return resolution.kind == .void or resolution.amount < entry.authorization.amount;
}

fn validStage(entry: Entry, stage: TransferStage) bool {
    return switch (stage) {
        .authorization => entry.resolution == null or isExpiration(entry),
        .resolution => entry.resolution != null and entry.resolution_transfer != null,
    };
}

fn validFundingPurpose(entry: Entry, purpose: FundingPurpose) bool {
    return switch (purpose) {
        .withdrawal_debit => entry.authorization.direction == .withdrawal and
            (entry.resolution == null or isExpiration(entry)),
        .deposit_credit => entry.authorization.direction == .deposit and
            entry.resolution != null and entry.resolution.?.kind == .post,
        .withdrawal_refund => entry.authorization.direction == .withdrawal and
            fundingAmount(entry, purpose) > 0,
    };
}

fn minimumRefundRemoteAttempts(entry: Entry) u32 {
    if (isExpiration(entry)) return 0;
    return if (entry.resolution == null) 1 else 2;
}

fn bumpEntryGeneration(entry: *Entry) LedgerError!void {
    entry.generation = std.math.add(u64, entry.generation, 1) catch
        return error.GenerationOverflow;
}

fn validAccountId(id: u128) bool {
    return id != 0 and id != std.math.maxInt(u128);
}

fn hashBinding(hasher: anytype, value: Binding) void {
    hashInt(hasher, u128, @intFromEnum(value.destination_id));
    hasher.update(&value.settlement_profile_digest);
    hasher.update(&value.clearing_profile_digest);
    hashInt(hasher, u64, value.provider_id);
}

fn hashAuthorization(hasher: anytype, value: Authorization) void {
    hashInt(hasher, u128, value.event_id);
    hashInt(hasher, u128, value.payment_id);
    hashInt(hasher, u64, value.provider_id);
    hashInt(hasher, u64, value.revision);
    hashInt(hasher, u8, @intFromEnum(value.direction));
    hashInt(hasher, i64, value.uid);
    hashInt(hasher, i32, value.currency);
    hashInt(hasher, i64, value.amount);
    hashInt(hasher, u32, value.timeout_seconds);
}

fn hashResolution(hasher: anytype, value: Resolution) void {
    hashInt(hasher, u128, value.event_id);
    hashInt(hasher, u128, value.payment_id);
    hashInt(hasher, u64, value.provider_id);
    hashInt(hasher, u64, value.revision);
    hashInt(hasher, u8, @intFromEnum(value.kind));
    hashInt(hasher, u8, @intFromEnum(value.direction));
    hashInt(hasher, i64, value.uid);
    hashInt(hasher, i32, value.currency);
    hashInt(hasher, i64, value.amount);
}

fn hashRoute(hasher: anytype, value: Route) void {
    hashInt(hasher, u128, @intFromEnum(value.destination_id));
    hashInt(hasher, u32, value.ledger);
    hashInt(hasher, u128, value.treasury_account_id);
    hashInt(hasher, u128, value.provider_control_account_id);
    hashInt(hasher, u16, value.transfer_code);
}

fn hashInt(hasher: anytype, comptime T: type, value: T) void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    hasher.update(&bytes);
}

fn finishStableId(hasher: anytype) u128 {
    var digest: [32]u8 = undefined;
    hasher.final(&digest);
    var id = std.mem.readInt(u128, digest[0..16], .little) | (@as(u128, 1) << 127);
    if (id == std.math.maxInt(u128)) id -= 1;
    return id;
}

fn testBinding() Binding {
    return .{
        .destination_id = @enumFromInt(77),
        .settlement_profile_digest = [_]u8{0x11} ** 32,
        .clearing_profile_digest = [_]u8{0x22} ** 32,
        .provider_id = 9,
    };
}

fn testRoute() Route {
    return .{
        .destination_id = testBinding().destination_id,
        .ledger = 840,
        .treasury_account_id = 100,
        .provider_control_account_id = 101,
        .transfer_code = 2_010,
    };
}

fn testAuthorization(payment_id: u128, direction: Direction) Authorization {
    return .{
        .event_id = payment_id * 10 + 10_000,
        .payment_id = payment_id,
        .provider_id = testBinding().provider_id,
        .revision = 7,
        .direction = direction,
        .uid = 1_001,
        .currency = 11,
        .amount = 50,
        .timeout_seconds = 300,
    };
}

fn testResolution(
    authorization: Authorization,
    kind: ResolutionKind,
    amount: i64,
) Resolution {
    return .{
        .event_id = authorization.event_id + 1,
        .payment_id = authorization.payment_id,
        .provider_id = authorization.provider_id,
        .revision = authorization.revision + 1,
        .kind = kind,
        .direction = authorization.direction,
        .uid = authorization.uid,
        .currency = authorization.currency,
        .amount = amount,
    };
}

fn expectClaimAction(ledger: anytype, comptime tag: std.meta.Tag(Action)) !Claim {
    const claim = (try ledger.claimNext()) orelse return error.NoPendingAction;
    try std.testing.expectEqual(tag, std.meta.activeTag(claim.action));
    return claim;
}

test "automatic expiry uses the admission deadline and one deterministic void" {
    var ledger = try Ledger(8).init(testBinding());
    const authorization = testAuthorization(1_001, .deposit);
    const accepted_at: i64 = 1_700_000_000;
    const deadline = accepted_at + @as(i64, authorization.timeout_seconds);
    const inserted = try ledger.acceptAuthorizationAt(
        authorization,
        testRoute(),
        accepted_at,
    );
    try std.testing.expectEqual(deadline, inserted.status.expires_at_unix_seconds);
    try std.testing.expect((try ledger.expireNext(deadline - 1)) == null);

    const duplicate = try ledger.acceptAuthorizationAt(
        authorization,
        testRoute(),
        accepted_at + 1_000,
    );
    try std.testing.expectEqual(AcceptDisposition.existing, duplicate.disposition);
    try std.testing.expectEqual(deadline, duplicate.status.expires_at_unix_seconds);

    const expired = (try ledger.expireNext(deadline)).?;
    try std.testing.expectEqual(PublicState.completed, expired.state);
    try std.testing.expectEqual(ResolutionSource.expiration, expired.resolution_source.?);
    try std.testing.expectEqualDeep(
        expirationResolution(authorization),
        expired.resolution.?,
    );
    try std.testing.expect((try ledger.expireNext(deadline)) == null);
    try std.testing.expect((try ledger.claimNext()) == null);
    try ledger.validate();

    try std.testing.expectError(
        error.TerminalConflict,
        ledger.acceptResolution(testResolution(authorization, .post, authorization.amount)),
    );
}

test "expiry reconciles exposed authorization before void and withdrawal refund" {
    var deposit_ledger = try Ledger(8).init(testBinding());
    const deposit = testAuthorization(1_002, .deposit);
    _ = try deposit_ledger.acceptAuthorizationAt(deposit, testRoute(), 10);
    _ = try expectClaimAction(&deposit_ledger, .remote_submit);
    try std.testing.expect((try deposit_ledger.expireNext(
        10 + @as(i64, deposit.timeout_seconds),
    )) == null);
    try std.testing.expectEqual(@as(usize, 1), try deposit_ledger.recover());
    _ = (try deposit_ledger.expireNext(10 + @as(i64, deposit.timeout_seconds))).?;
    const lookup = try expectClaimAction(&deposit_ledger, .remote_lookup);
    try std.testing.expectEqual(
        ReconciliationCause.ambiguous_submission,
        lookup.action.remote_lookup.cause,
    );
    _ = try deposit_ledger.complete(lookup, .{ .remote_lookup = .exact });
    const void_claim = try expectClaimAction(&deposit_ledger, .remote_submit);
    try std.testing.expectEqual(TransferStage.resolution, void_claim.action.remote_submit.stage);
    try std.testing.expectEqual(
        deterministicTransferId(
            .resolution,
            deposit,
            expirationResolution(deposit),
            testRoute(),
        ),
        void_claim.action.remote_submit.transfer.id,
    );
    const deposit_status = try deposit_ledger.complete(
        void_claim,
        .{ .remote_submit = .applied },
    );
    try std.testing.expectEqual(PublicState.completed, deposit_status.state);

    var withdrawal_ledger = try Ledger(8).init(testBinding());
    const withdrawal = testAuthorization(1_003, .withdrawal);
    _ = try withdrawal_ledger.acceptAuthorizationAt(withdrawal, testRoute(), 20);
    const debit = try expectClaimAction(&withdrawal_ledger, .funding_submit);
    _ = try withdrawal_ledger.complete(debit, .{ .funding_submit = .completed });
    _ = try expectClaimAction(&withdrawal_ledger, .remote_submit);
    try std.testing.expect((try withdrawal_ledger.expireNext(
        20 + @as(i64, withdrawal.timeout_seconds),
    )) == null);
    try std.testing.expectEqual(@as(usize, 1), try withdrawal_ledger.recover());
    _ = (try withdrawal_ledger.expireNext(
        20 + @as(i64, withdrawal.timeout_seconds),
    )).?;
    const withdrawal_lookup = try expectClaimAction(&withdrawal_ledger, .remote_lookup);
    _ = try withdrawal_ledger.complete(withdrawal_lookup, .{ .remote_lookup = .absent });
    const refund = try expectClaimAction(&withdrawal_ledger, .funding_submit);
    try std.testing.expectEqual(FundingPurpose.withdrawal_refund, refund.action.funding_submit.purpose);
    try std.testing.expectEqual(withdrawal.amount, refund.action.funding_submit.amount);
    const withdrawal_status = try withdrawal_ledger.complete(
        refund,
        .{ .funding_submit = .completed },
    );
    try std.testing.expectEqual(PublicState.completed, withdrawal_status.state);
    try std.testing.expectEqual(
        ResolutionSource.expiration,
        withdrawal_status.resolution_source.?,
    );
    try withdrawal_ledger.validate();
}

test "expiry reconciles ambiguous withdrawal debit before stopping or refunding" {
    const deadline: i64 = 1_000;
    var absent_ledger = try Ledger(8).init(testBinding());
    const absent = testAuthorization(1_004, .withdrawal);
    _ = try absent_ledger.acceptAuthorizationAt(
        absent,
        testRoute(),
        deadline - @as(i64, absent.timeout_seconds),
    );
    _ = try expectClaimAction(&absent_ledger, .funding_submit);
    try std.testing.expectEqual(@as(usize, 1), try absent_ledger.recover());
    _ = (try absent_ledger.expireNext(deadline)).?;
    const absent_status = try expectClaimAction(&absent_ledger, .funding_status);
    const completed = try absent_ledger.complete(
        absent_status,
        .{ .funding_status = .not_found },
    );
    try std.testing.expectEqual(PublicState.completed, completed.state);
    try std.testing.expect((try absent_ledger.claimNext()) == null);

    var committed_ledger = try Ledger(8).init(testBinding());
    const committed = testAuthorization(1_005, .withdrawal);
    _ = try committed_ledger.acceptAuthorizationAt(
        committed,
        testRoute(),
        deadline - @as(i64, committed.timeout_seconds),
    );
    _ = try expectClaimAction(&committed_ledger, .funding_submit);
    try std.testing.expectEqual(@as(usize, 1), try committed_ledger.recover());
    _ = (try committed_ledger.expireNext(deadline)).?;
    const committed_status = try expectClaimAction(&committed_ledger, .funding_status);
    _ = try committed_ledger.complete(committed_status, .{ .funding_status = .completed });
    const refund = try expectClaimAction(&committed_ledger, .funding_submit);
    try std.testing.expectEqual(FundingPurpose.withdrawal_refund, refund.action.funding_submit.purpose);
    try std.testing.expectEqual(committed.amount, refund.action.funding_submit.amount);
    _ = try committed_ledger.complete(refund, .{ .funding_submit = .completed });
    try std.testing.expectEqual(
        PublicState.completed,
        (try committed_ledger.lookup(committed.payment_id)).state,
    );
    try committed_ledger.validate();
}

test "operations snapshot exposes deterministic backlog and incident evidence" {
    const observed_at: i64 = 450;
    var accepted = try Ledger(8).init(testBinding());
    _ = try accepted.acceptAuthorizationAt(
        testAuthorization(1_101, .deposit),
        testRoute(),
        100,
    );

    var remote_reconciliation = try Ledger(8).init(testBinding());
    _ = try remote_reconciliation.acceptAuthorizationAt(
        testAuthorization(1_102, .deposit),
        testRoute(),
        200,
    );
    _ = try expectClaimAction(&remote_reconciliation, .remote_submit);
    _ = try remote_reconciliation.recover();

    var funding_reconciliation = try Ledger(8).init(testBinding());
    _ = try funding_reconciliation.acceptAuthorizationAt(
        testAuthorization(1_103, .withdrawal),
        testRoute(),
        250,
    );
    _ = try expectClaimAction(&funding_reconciliation, .funding_submit);
    _ = try funding_reconciliation.recover();
    _ = try expectClaimAction(&funding_reconciliation, .funding_status);

    var provider_completed = try Ledger(8).init(testBinding());
    const provider_authorization = testAuthorization(1_104, .deposit);
    _ = try provider_completed.acceptAuthorizationAt(
        provider_authorization,
        testRoute(),
        300,
    );
    const authorization_submit = try expectClaimAction(&provider_completed, .remote_submit);
    _ = try provider_completed.complete(
        authorization_submit,
        .{ .remote_submit = .applied },
    );
    _ = try provider_completed.acceptResolution(testResolution(
        provider_authorization,
        .post,
        provider_authorization.amount,
    ));
    const resolution_submit = try expectClaimAction(&provider_completed, .remote_submit);
    _ = try provider_completed.complete(resolution_submit, .{ .remote_submit = .applied });
    const credit = try expectClaimAction(&provider_completed, .funding_submit);
    _ = try provider_completed.complete(credit, .{ .funding_submit = .completed });

    var expiration_completed = try Ledger(8).init(testBinding());
    const expiration_authorization = testAuthorization(1_105, .deposit);
    _ = try expiration_completed.acceptAuthorizationAt(
        expiration_authorization,
        testRoute(),
        100,
    );
    _ = (try expiration_completed.expireNext(
        100 + @as(i64, expiration_authorization.timeout_seconds),
    )).?;

    var failed = try Ledger(8).init(testBinding());
    _ = try failed.acceptAuthorizationAt(
        testAuthorization(1_106, .deposit),
        testRoute(),
        300,
    );
    const rejected = try expectClaimAction(&failed, .remote_submit);
    _ = try failed.complete(rejected, .{ .remote_submit = .definitely_rejected });

    var quarantined = try Ledger(8).init(testBinding());
    _ = try quarantined.acceptAuthorizationAt(
        testAuthorization(1_107, .deposit),
        testRoute(),
        300,
    );
    const conflict = try expectClaimAction(&quarantined, .remote_submit);
    _ = try quarantined.complete(conflict, .{ .remote_submit = .conflict });
    const mismatch = try expectClaimAction(&quarantined, .remote_lookup);
    _ = try quarantined.complete(mismatch, .{ .remote_lookup = .transfer_mismatch });

    const entries = [_]Entry{
        accepted.entries[0],
        remote_reconciliation.entries[0],
        funding_reconciliation.entries[0],
        provider_completed.entries[0],
        expiration_completed.entries[0],
        failed.entries[0],
        quarantined.entries[0],
    };
    var aggregate = try Ledger(8).restore(testBinding(), &entries, 100);
    const generation_before = aggregate.snapshot_generation;
    const snapshot = try aggregate.operationsSnapshot(observed_at);

    try std.testing.expectEqual(@as(usize, 8), snapshot.capacity);
    try std.testing.expectEqual(@as(usize, 7), snapshot.total);
    try std.testing.expectEqual(@as(usize, 1), snapshot.states.accepted);
    try std.testing.expectEqual(@as(usize, 2), snapshot.states.pending);
    try std.testing.expectEqual(@as(usize, 2), snapshot.states.completed);
    try std.testing.expectEqual(@as(usize, 1), snapshot.states.failed);
    try std.testing.expectEqual(@as(usize, 1), snapshot.states.quarantined);
    try std.testing.expectEqual(@as(usize, 2), snapshot.ready_actions);
    try std.testing.expectEqual(@as(usize, 1), snapshot.in_flight_actions);
    try std.testing.expectEqual(@as(usize, 1), snapshot.remote_reconciliations);
    try std.testing.expectEqual(@as(usize, 1), snapshot.funding_reconciliations);
    try std.testing.expectEqual(@as(usize, 1), snapshot.provider_resolutions);
    try std.testing.expectEqual(@as(usize, 1), snapshot.expiration_resolutions);
    try std.testing.expectEqual(@as(u64, 6), snapshot.remote_attempts);
    try std.testing.expectEqual(@as(u64, 3), snapshot.funding_attempts);
    try std.testing.expectEqual(@as(usize, 1), snapshot.expired_unresolved);
    try std.testing.expectEqual(@as(u64, 50), snapshot.max_deadline_lag_seconds);
    try std.testing.expectEqual(@as(u64, 350), snapshot.oldest_open_age_seconds);
    try std.testing.expectEqual(@as(u128, 1_101), snapshot.oldest_expired_payment_id.?);
    try std.testing.expectEqual(
        @as(u128, 1_102),
        snapshot.first_reconciliation_payment_id.?,
    );
    try std.testing.expectEqual(@as(u128, 1_106), snapshot.first_failed_payment_id.?);
    try std.testing.expectEqual(
        @as(u128, 1_107),
        snapshot.first_quarantined_payment_id.?,
    );
    try std.testing.expectEqual(generation_before, aggregate.snapshot_generation);
    try std.testing.expectError(error.InvalidClock, aggregate.operationsSnapshot(0));
}

test "clearing mapper emits complete pending post and void fields" {
    const authorization = testAuthorization(1, .deposit);
    const pending = try authorizationTransfer(authorization, testRoute());
    try std.testing.expectEqual(testRoute().provider_control_account_id, pending.debit_account_id);
    try std.testing.expectEqual(testRoute().treasury_account_id, pending.credit_account_id);
    try std.testing.expectEqual(@as(u16, 2), pending.flags);
    try std.testing.expectEqual(@as(u32, 300), pending.timeout);
    try std.testing.expectEqual(@as(u128, 0), pending.pending_id);

    const post = try resolutionTransfer(
        authorization,
        testResolution(authorization, .post, 40),
        testRoute(),
    );
    try std.testing.expectEqual(@as(u16, 4), post.flags);
    try std.testing.expectEqual(pending.id, post.pending_id);
    try std.testing.expectEqual(@as(u128, 40), post.amount);
    try std.testing.expectEqual(pending.debit_account_id, post.debit_account_id);
    try std.testing.expectEqual(pending.credit_account_id, post.credit_account_id);
    try std.testing.expectEqual(pending.ledger, post.ledger);
    try std.testing.expectEqual(pending.code, post.code);

    const withdrawal = testAuthorization(2, .withdrawal);
    const withdrawal_pending = try authorizationTransfer(withdrawal, testRoute());
    try std.testing.expectEqual(testRoute().treasury_account_id, withdrawal_pending.debit_account_id);
    try std.testing.expectEqual(testRoute().provider_control_account_id, withdrawal_pending.credit_account_id);
    const void_transfer = try resolutionTransfer(
        withdrawal,
        testResolution(withdrawal, .void, withdrawal.amount),
        testRoute(),
    );
    try std.testing.expectEqual(@as(u16, 8), void_transfer.flags);
    try std.testing.expectEqual(@as(u128, @intCast(withdrawal.amount)), void_transfer.amount);
}

test "deposit posts before deterministic FSG1 credit" {
    var ledger = try Ledger(8).init(testBinding());
    const authorization = testAuthorization(10, .deposit);
    _ = try ledger.acceptAuthorization(authorization, testRoute());

    const pending_claim = try expectClaimAction(&ledger, .remote_submit);
    try std.testing.expectEqual(TransferStage.authorization, pending_claim.action.remote_submit.stage);
    _ = try ledger.complete(pending_claim, .{ .remote_submit = .applied });
    try std.testing.expectEqual(PublicState.authorized, (try ledger.lookup(authorization.payment_id)).state);

    const resolution = testResolution(authorization, .post, 40);
    _ = try ledger.acceptResolution(resolution);
    const post_claim = try expectClaimAction(&ledger, .remote_submit);
    try std.testing.expectEqual(TransferStage.resolution, post_claim.action.remote_submit.stage);
    _ = try ledger.complete(post_claim, .{ .remote_submit = .applied });

    const funding_claim = try expectClaimAction(&ledger, .funding_submit);
    try std.testing.expectEqual(FundingPurpose.deposit_credit, funding_claim.action.funding_submit.purpose);
    try std.testing.expectEqual(@as(i64, 40), funding_claim.action.funding_submit.amount);
    _ = try ledger.complete(funding_claim, .{ .funding_submit = .completed });
    try std.testing.expectEqual(PublicState.completed, (try ledger.lookup(authorization.payment_id)).state);
    try std.testing.expect((try ledger.claimNext()) == null);
}

test "partial withdrawal posts then refunds only released remainder" {
    var ledger = try Ledger(8).init(testBinding());
    const authorization = testAuthorization(20, .withdrawal);
    _ = try ledger.acceptAuthorization(authorization, testRoute());

    const debit_claim = try expectClaimAction(&ledger, .funding_submit);
    try std.testing.expectEqual(FundingPurpose.withdrawal_debit, debit_claim.action.funding_submit.purpose);
    _ = try ledger.complete(debit_claim, .{ .funding_submit = .completed });
    const pending_claim = try expectClaimAction(&ledger, .remote_submit);
    _ = try ledger.complete(pending_claim, .{ .remote_submit = .applied });

    _ = try ledger.acceptResolution(testResolution(authorization, .post, 35));
    const post_claim = try expectClaimAction(&ledger, .remote_submit);
    _ = try ledger.complete(post_claim, .{ .remote_submit = .applied });
    const refund_claim = try expectClaimAction(&ledger, .funding_submit);
    try std.testing.expectEqual(FundingPurpose.withdrawal_refund, refund_claim.action.funding_submit.purpose);
    try std.testing.expectEqual(@as(i64, 15), refund_claim.action.funding_submit.amount);
    _ = try ledger.complete(refund_claim, .{ .funding_submit = .completed });
    try std.testing.expectEqual(PublicState.completed, (try ledger.lookup(authorization.payment_id)).state);
}

test "void creates no deposit credit and refunds a funded withdrawal" {
    var ledger = try Ledger(8).init(testBinding());
    const deposit = testAuthorization(21, .deposit);
    _ = try ledger.acceptAuthorization(deposit, testRoute());
    const deposit_pending = try expectClaimAction(&ledger, .remote_submit);
    _ = try ledger.complete(deposit_pending, .{ .remote_submit = .applied });
    _ = try ledger.acceptResolution(testResolution(deposit, .void, deposit.amount));
    const deposit_void = try expectClaimAction(&ledger, .remote_submit);
    _ = try ledger.complete(deposit_void, .{ .remote_submit = .applied });
    try std.testing.expectEqual(PublicState.completed, (try ledger.lookup(deposit.payment_id)).state);
    try std.testing.expect((try ledger.claimNext()) == null);

    const withdrawal = testAuthorization(22, .withdrawal);
    _ = try ledger.acceptAuthorization(withdrawal, testRoute());
    const debit = try expectClaimAction(&ledger, .funding_submit);
    _ = try ledger.complete(debit, .{ .funding_submit = .completed });
    const withdrawal_pending = try expectClaimAction(&ledger, .remote_submit);
    _ = try ledger.complete(withdrawal_pending, .{ .remote_submit = .applied });
    _ = try ledger.acceptResolution(testResolution(withdrawal, .void, withdrawal.amount));
    const withdrawal_void = try expectClaimAction(&ledger, .remote_submit);
    _ = try ledger.complete(withdrawal_void, .{ .remote_submit = .applied });
    const refund = try expectClaimAction(&ledger, .funding_submit);
    try std.testing.expectEqual(FundingPurpose.withdrawal_refund, refund.action.funding_submit.purpose);
    try std.testing.expectEqual(withdrawal.amount, refund.action.funding_submit.amount);
    _ = try ledger.complete(refund, .{ .funding_submit = .completed });
    try std.testing.expectEqual(PublicState.completed, (try ledger.lookup(withdrawal.payment_id)).state);
}

test "ambiguous writes reconcile before retry and absent conflicts quarantine" {
    var ledger = try Ledger(8).init(testBinding());
    const ambiguous = testAuthorization(23, .deposit);
    _ = try ledger.acceptAuthorization(ambiguous, testRoute());
    const first = try expectClaimAction(&ledger, .remote_submit);
    const transfer_id = first.action.remote_submit.transfer.id;
    _ = try ledger.complete(first, .{ .remote_submit = .ambiguous });
    const lookup = try expectClaimAction(&ledger, .remote_lookup);
    try std.testing.expectEqual(transfer_id, lookup.action.remote_lookup.transfer.id);
    try std.testing.expectEqual(ReconciliationCause.ambiguous_submission, lookup.action.remote_lookup.cause);
    _ = try ledger.complete(lookup, .{ .remote_lookup = .absent });
    const retry = try expectClaimAction(&ledger, .remote_submit);
    try std.testing.expectEqual(transfer_id, retry.action.remote_submit.transfer.id);

    var other = try Ledger(8).init(testBinding());
    const conflict = testAuthorization(24, .deposit);
    _ = try other.acceptAuthorization(conflict, testRoute());
    const submit = try expectClaimAction(&other, .remote_submit);
    _ = try other.complete(submit, .{ .remote_submit = .conflict });
    const conflict_lookup = try expectClaimAction(&other, .remote_lookup);
    _ = try other.complete(conflict_lookup, .{ .remote_lookup = .absent });
    const status = try other.lookup(conflict.payment_id);
    try std.testing.expectEqual(PublicState.quarantined, status.state);
    try std.testing.expectEqual(QuarantineReason.reconciliation_failed, status.quarantine_reason.?);
}

test "rejected outbound authorization refunds before terminal failure" {
    var ledger = try Ledger(8).init(testBinding());
    const withdrawal = testAuthorization(25, .withdrawal);
    _ = try ledger.acceptAuthorization(withdrawal, testRoute());
    const debit = try expectClaimAction(&ledger, .funding_submit);
    _ = try ledger.complete(debit, .{ .funding_submit = .completed });
    const pending = try expectClaimAction(&ledger, .remote_submit);
    _ = try ledger.complete(pending, .{ .remote_submit = .definitely_rejected });
    const refund = try expectClaimAction(&ledger, .funding_submit);
    try std.testing.expectEqual(FundingPurpose.withdrawal_refund, refund.action.funding_submit.purpose);
    _ = try ledger.complete(refund, .{ .funding_submit = .completed });
    const status = try ledger.lookup(withdrawal.payment_id);
    try std.testing.expectEqual(PublicState.failed, status.state);
    try std.testing.expectEqual(FailureReason.authorization_rejected, status.failure_reason.?);
}

test "provider event identity revision and terminal outcome are immutable" {
    var ledger = try Ledger(8).init(testBinding());
    const authorization = testAuthorization(30, .deposit);
    const first = try ledger.acceptAuthorization(authorization, testRoute());
    try std.testing.expectEqual(AcceptDisposition.inserted, first.disposition);
    const duplicate = try ledger.acceptAuthorization(authorization, testRoute());
    try std.testing.expectEqual(AcceptDisposition.existing, duplicate.disposition);
    var conflict = authorization;
    conflict.amount += 1;
    try std.testing.expectError(
        error.IdempotencyConflict,
        ledger.acceptAuthorization(conflict, testRoute()),
    );

    const pending_claim = try expectClaimAction(&ledger, .remote_submit);
    _ = try ledger.complete(pending_claim, .{ .remote_submit = .applied });
    var skipped = testResolution(authorization, .post, authorization.amount);
    skipped.revision += 1;
    try std.testing.expectError(error.OutOfOrderRevision, ledger.acceptResolution(skipped));
    const post = testResolution(authorization, .post, authorization.amount);
    _ = try ledger.acceptResolution(post);
    try std.testing.expectEqual(
        AcceptDisposition.existing,
        (try ledger.acceptResolution(post)).disposition,
    );
    var competing = testResolution(authorization, .void, authorization.amount);
    competing.event_id += 100;
    try std.testing.expectError(error.TerminalConflict, ledger.acceptResolution(competing));
}

test "restart converts possibly sent transfer into lookup and exposed funding into status" {
    var ledger = try Ledger(8).init(testBinding());
    const deposit = testAuthorization(40, .deposit);
    _ = try ledger.acceptAuthorization(deposit, testRoute());
    _ = try expectClaimAction(&ledger, .remote_submit);
    try std.testing.expectEqual(@as(usize, 1), try ledger.recover());
    const lookup_claim = try expectClaimAction(&ledger, .remote_lookup);
    try std.testing.expectEqual(TransferStage.authorization, lookup_claim.action.remote_lookup.stage);
    _ = try ledger.complete(lookup_claim, .{ .remote_lookup = .exact });

    const withdrawal = testAuthorization(41, .withdrawal);
    _ = try ledger.acceptAuthorization(withdrawal, testRoute());
    _ = try expectClaimAction(&ledger, .funding_submit);
    try std.testing.expectEqual(@as(usize, 1), try ledger.recover());
    const status_claim = try expectClaimAction(&ledger, .funding_status);
    try std.testing.expectEqual(
        FundingPurpose.withdrawal_debit,
        status_claim.action.funding_status.request.purpose,
    );
    try std.testing.expectEqual(
        FundingReconciliationCause.ambiguous_submission,
        status_claim.action.funding_status.cause,
    );
    _ = try ledger.complete(status_claim, .{ .funding_status = .not_found });
    const replay = try expectClaimAction(&ledger, .funding_submit);
    try std.testing.expectEqualDeep(
        status_claim.action.funding_status.request,
        replay.action.funding_submit,
    );
}

test "funding submit certainty controls status reconciliation and retry" {
    const binding = testBinding();

    var ambiguous = try Ledger(8).init(binding);
    const ambiguous_authorization = testAuthorization(51, .withdrawal);
    _ = try ambiguous.acceptAuthorization(ambiguous_authorization, testRoute());
    const ambiguous_submit = try expectClaimAction(&ambiguous, .funding_submit);
    _ = try ambiguous.complete(ambiguous_submit, .{ .funding_submit = .ambiguous });
    const ambiguous_status = try expectClaimAction(&ambiguous, .funding_status);
    try std.testing.expectEqual(
        FundingReconciliationCause.ambiguous_submission,
        ambiguous_status.action.funding_status.cause,
    );
    _ = try ambiguous.complete(ambiguous_status, .{ .funding_status = .not_found });
    const proven_absent_retry = try expectClaimAction(&ambiguous, .funding_submit);
    try std.testing.expectEqual(
        ambiguous_submit.action.funding_submit.operation_id,
        proven_absent_retry.action.funding_submit.operation_id,
    );

    var known_not_sent = try Ledger(8).init(binding);
    const known_not_sent_authorization = testAuthorization(52, .withdrawal);
    _ = try known_not_sent.acceptAuthorization(known_not_sent_authorization, testRoute());
    const first_submit = try expectClaimAction(&known_not_sent, .funding_submit);
    _ = try known_not_sent.complete(first_submit, .{ .funding_submit = .known_not_sent });
    const direct_retry = try expectClaimAction(&known_not_sent, .funding_submit);
    try std.testing.expectEqual(
        first_submit.action.funding_submit.operation_id,
        direct_retry.action.funding_submit.operation_id,
    );

    var accepted = try Ledger(8).init(binding);
    const accepted_authorization = testAuthorization(53, .withdrawal);
    _ = try accepted.acceptAuthorization(accepted_authorization, testRoute());
    const accepted_submit = try expectClaimAction(&accepted, .funding_submit);
    _ = try accepted.complete(accepted_submit, .{ .funding_submit = .pending });
    const accepted_status = try expectClaimAction(&accepted, .funding_status);
    try std.testing.expectEqual(
        FundingReconciliationCause.accepted_operation,
        accepted_status.action.funding_status.cause,
    );
    _ = try accepted.complete(accepted_status, .{ .funding_status = .not_found });
    const status = try accepted.lookup(accepted_authorization.payment_id);
    try std.testing.expectEqual(PublicState.quarantined, status.state);
    try std.testing.expectEqual(QuarantineReason.funding_conflict, status.quarantine_reason.?);
    try std.testing.expect((try accepted.claimNext()) == null);
}

test "completion rejects a generation-reused claim for a different canonical action" {
    var ledger = try Ledger(8).init(testBinding());
    const authorization = testAuthorization(42, .deposit);
    _ = try ledger.acceptAuthorization(authorization, testRoute());
    const pending = try expectClaimAction(&ledger, .remote_submit);
    _ = try ledger.complete(pending, .{ .remote_submit = .applied });

    const authorized_snapshot = ledger;
    _ = try ledger.acceptResolution(testResolution(authorization, .post, 40));
    const stale_claim = try expectClaimAction(&ledger, .remote_submit);

    ledger = authorized_snapshot;
    var replacement = testResolution(authorization, .post, 35);
    replacement.event_id += 1;
    _ = try ledger.acceptResolution(replacement);
    const current_claim = try expectClaimAction(&ledger, .remote_submit);
    try std.testing.expectEqual(stale_claim.generation, current_claim.generation);
    try std.testing.expect(!std.meta.eql(stale_claim.action, current_claim.action));

    try std.testing.expectError(
        error.StaleClaim,
        ledger.complete(stale_claim, .{ .remote_submit = .applied }),
    );
    _ = try ledger.complete(current_claim, .{ .remote_submit = .applied });
    const credit = try expectClaimAction(&ledger, .funding_submit);
    try std.testing.expectEqual(@as(i64, 35), credit.action.funding_submit.amount);
}

test "claim overflow leaves the ready entry and snapshot unchanged" {
    var generation_ledger = try Ledger(8).init(testBinding());
    _ = try generation_ledger.acceptAuthorization(
        testAuthorization(43, .deposit),
        testRoute(),
    );
    generation_ledger.entries[0].generation = std.math.maxInt(u64);
    const generation_entry_before = generation_ledger.entries[0];
    const generation_snapshot_before = generation_ledger.snapshot_generation;
    try std.testing.expectError(error.GenerationOverflow, generation_ledger.claimNext());
    try std.testing.expect(std.meta.eql(generation_entry_before, generation_ledger.entries[0]));
    try std.testing.expectEqual(
        generation_snapshot_before,
        generation_ledger.snapshot_generation,
    );

    var snapshot_ledger = try Ledger(8).init(testBinding());
    _ = try snapshot_ledger.acceptAuthorization(
        testAuthorization(44, .deposit),
        testRoute(),
    );
    snapshot_ledger.snapshot_generation = std.math.maxInt(u64);
    const snapshot_entry_before = snapshot_ledger.entries[0];
    try std.testing.expectError(error.GenerationOverflow, snapshot_ledger.claimNext());
    try std.testing.expect(std.meta.eql(snapshot_entry_before, snapshot_ledger.entries[0]));
    try std.testing.expectEqual(
        std.math.maxInt(u64),
        snapshot_ledger.snapshot_generation,
    );

    var attempt_ledger = try Ledger(8).init(testBinding());
    _ = try attempt_ledger.acceptAuthorization(
        testAuthorization(45, .deposit),
        testRoute(),
    );
    attempt_ledger.entries[0].remote_attempts = std.math.maxInt(u32);
    const attempt_entry_before = attempt_ledger.entries[0];
    const attempt_snapshot_before = attempt_ledger.snapshot_generation;
    try std.testing.expectError(error.AttemptOverflow, attempt_ledger.claimNext());
    try std.testing.expect(std.meta.eql(attempt_entry_before, attempt_ledger.entries[0]));
    try std.testing.expectEqual(attempt_snapshot_before, attempt_ledger.snapshot_generation);
}

test "restore rejects missing transition evidence duplicate events and generation rollback" {
    const binding = testBinding();
    const deposit = testAuthorization(46, .deposit);
    var accepted = try Ledger(8).init(binding);
    _ = try accepted.acceptAuthorization(deposit, testRoute());

    var forged_credit = accepted.entries[0];
    forged_credit.resolution = testResolution(deposit, .post, 40);
    forged_credit.resolution_transfer = try resolutionTransfer(
        deposit,
        forged_credit.resolution.?,
        testRoute(),
    );
    forged_credit.phase = .{ .funding_submit_ready = .deposit_credit };
    try std.testing.expectError(
        error.InvalidState,
        Ledger(8).restore(binding, &[_]Entry{forged_credit}, accepted.snapshot_generation),
    );

    const withdrawal = testAuthorization(47, .withdrawal);
    var unfunded = try Ledger(8).init(binding);
    _ = try unfunded.acceptAuthorization(withdrawal, testRoute());
    var forged_reservation = unfunded.entries[0];
    forged_reservation.phase = .{ .transfer_submit_ready = .authorization };
    try std.testing.expectError(
        error.InvalidState,
        Ledger(8).restore(binding, &[_]Entry{forged_reservation}, unfunded.snapshot_generation),
    );

    try std.testing.expectError(
        error.InvalidState,
        Ledger(8).restore(binding, accepted.entriesSlice(), 0),
    );

    _ = try expectClaimAction(&accepted, .remote_submit);
    var rolled_back_generation = accepted.entries[0];
    rolled_back_generation.generation -= 1;
    try std.testing.expectError(
        error.InvalidState,
        Ledger(8).restore(
            binding,
            &[_]Entry{rolled_back_generation},
            accepted.snapshot_generation,
        ),
    );

    var duplicate_event_ledger = try Ledger(8).init(binding);
    _ = try duplicate_event_ledger.acceptAuthorization(deposit, testRoute());
    const pending = try expectClaimAction(&duplicate_event_ledger, .remote_submit);
    _ = try duplicate_event_ledger.complete(pending, .{ .remote_submit = .applied });
    _ = try duplicate_event_ledger.acceptResolution(testResolution(deposit, .post, 40));
    var duplicate_event = duplicate_event_ledger.entries[0];
    duplicate_event.resolution.?.event_id = deposit.event_id;
    try std.testing.expectError(
        error.InvalidState,
        Ledger(8).restore(
            binding,
            &[_]Entry{duplicate_event},
            duplicate_event_ledger.snapshot_generation,
        ),
    );
}

test "funding identity binds full request and clearing profiles" {
    const binding = testBinding();
    var ledger = try Ledger(8).init(binding);
    const withdrawal = testAuthorization(48, .withdrawal);
    _ = try ledger.acceptAuthorization(withdrawal, testRoute());
    const entry = ledger.entries[0];
    const operation_id = fundingOperationId(binding, entry, .withdrawal_debit);
    try std.testing.expect(operation_id & (@as(u128, 1) << 127) != 0);
    try std.testing.expectEqual(
        operation_id,
        fundingOperationId(binding, entry, .withdrawal_debit),
    );

    var other_user = entry;
    other_user.authorization.uid += 1;
    try std.testing.expect(operation_id != fundingOperationId(
        binding,
        other_user,
        .withdrawal_debit,
    ));
    var other_currency = entry;
    other_currency.authorization.currency += 1;
    try std.testing.expect(operation_id != fundingOperationId(
        binding,
        other_currency,
        .withdrawal_debit,
    ));
    var other_binding = binding;
    other_binding.clearing_profile_digest[0] ^= 0xff;
    try std.testing.expect(operation_id != fundingOperationId(
        other_binding,
        entry,
        .withdrawal_debit,
    ));
}

test "funding status completion rejects a claim from another profile" {
    const authorization = testAuthorization(49, .withdrawal);
    var binding_a = testBinding();
    var binding_b = binding_a;
    binding_a.clearing_profile_digest[0] = 0xa1;
    binding_b.clearing_profile_digest[0] = 0xb2;
    var ledger_a = try Ledger(8).init(binding_a);
    var ledger_b = try Ledger(8).init(binding_b);
    _ = try ledger_a.acceptAuthorization(authorization, testRoute());
    _ = try ledger_b.acceptAuthorization(authorization, testRoute());

    _ = try expectClaimAction(&ledger_a, .funding_submit);
    _ = try expectClaimAction(&ledger_b, .funding_submit);
    _ = try ledger_a.recover();
    _ = try ledger_b.recover();
    const claim_a = try expectClaimAction(&ledger_a, .funding_status);
    const claim_b = try expectClaimAction(&ledger_b, .funding_status);
    try std.testing.expectEqual(claim_a.generation, claim_b.generation);
    try std.testing.expect(claim_a.action.funding_status.request.operation_id !=
        claim_b.action.funding_status.request.operation_id);
    try std.testing.expectError(
        error.StaleClaim,
        ledger_b.complete(claim_a, .{ .funding_status = .completed }),
    );
    _ = try ledger_b.complete(claim_b, .{ .funding_status = .completed });
}

test "authorization reserves one revision for a terminal provider event" {
    var ledger = try Ledger(8).init(testBinding());
    var authorization = testAuthorization(50, .deposit);
    authorization.revision = std.math.maxInt(u64);
    try std.testing.expectError(
        error.InvalidRevision,
        ledger.acceptAuthorization(authorization, testRoute()),
    );
}

const CampaignResult = struct {
    fingerprint: u64 = 0,
    completed: u32 = 0,
    recovered: u32 = 0,
};

fn runCampaign(seed: u64) !CampaignResult {
    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();
    var ledger = try Ledger(16).init(testBinding());
    var result: CampaignResult = .{};

    for (0..8) |index| {
        const direction: Direction = if (random.boolean()) .deposit else .withdrawal;
        const authorization = testAuthorization(1_000 + index, direction);
        _ = try ledger.acceptAuthorization(authorization, testRoute());
        while (try ledger.claimNext()) |claim| {
            if (random.uintLessThan(u8, 5) == 0) {
                result.recovered += @intCast(try ledger.recover());
                continue;
            }
            switch (claim.action) {
                .remote_submit => {
                    _ = try ledger.complete(claim, .{ .remote_submit = .applied });
                },
                .remote_lookup => {
                    _ = try ledger.complete(claim, .{ .remote_lookup = .exact });
                },
                .funding_submit => {
                    _ = try ledger.complete(claim, .{ .funding_submit = .completed });
                },
                .funding_status => {
                    _ = try ledger.complete(claim, .{ .funding_status = .completed });
                },
            }
        }
        const kind: ResolutionKind = if (random.boolean()) .post else .void;
        const amount = if (kind == .post and direction == .withdrawal and random.boolean())
            authorization.amount - 1
        else
            authorization.amount;
        _ = try ledger.acceptResolution(testResolution(authorization, kind, amount));
        while (try ledger.claimNext()) |claim| {
            switch (claim.action) {
                .remote_submit => {
                    _ = try ledger.complete(claim, .{ .remote_submit = .applied });
                },
                .remote_lookup => {
                    _ = try ledger.complete(claim, .{ .remote_lookup = .exact });
                },
                .funding_submit => {
                    _ = try ledger.complete(claim, .{ .funding_submit = .completed });
                },
                .funding_status => {
                    _ = try ledger.complete(claim, .{ .funding_status = .completed });
                },
            }
        }
        const status = try ledger.lookup(authorization.payment_id);
        try std.testing.expectEqual(PublicState.completed, status.state);
        result.completed += 1;
        result.fingerprint = (result.fingerprint *% 0x9e3779b97f4a7c15) ^
            @as(u64, @intFromEnum(status.resolution.?.kind)) ^ @as(u64, @intCast(index));
        try ledger.validate();
    }
    return result;
}

test "clearing seeded crash campaign is replayable and converges" {
    for (0..64) |index| {
        const seed = 0xc1ea_0000 + index;
        const first = try runCampaign(seed);
        const second = try runCampaign(seed);
        try std.testing.expectEqual(first, second);
        try std.testing.expectEqual(@as(u32, 8), first.completed);
    }
}
