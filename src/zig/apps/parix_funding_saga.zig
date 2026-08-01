const std = @import("std");
const exchange_core = @import("exchange_core");

const core = exchange_core.core;
const settlement = core.tigerbeetle_settlement;
const outbox = core.tigerbeetle_outbox;

pub const DestinationId = outbox.DestinationId;
pub const Transfer = settlement.Transfer;

pub const deposit_transfer_code: u16 = 2_000;
pub const withdrawal_transfer_code: u16 = 2_010;
pub const workflow_revision: u32 = 1;

const transfer_id_domain = "exchange-core/parix-funding-transfer/v1";

pub const Kind = enum(u8) {
    deposit = 1,
    withdrawal = 2,
};

pub const PublicState = enum(u8) {
    accepted = 1,
    pending = 2,
    completed = 3,
    failed = 4,
    quarantined = 5,
};

pub const FailureReason = enum(u8) {
    external_rejected = 1,
    local_rejected = 2,
};

pub const QuarantineReason = enum(u8) {
    destination_mismatch = 1,
    transfer_mismatch = 2,
    partial_application = 3,
    reconciliation_failed = 4,
    local_order_unprovable = 5,
    local_rejected_after_external_commit = 6,
    compensation_failed = 7,
};

pub const Request = struct {
    operation_id: u128,
    kind: Kind,
    uid: i64,
    currency: i32,
    amount: i64,

    pub fn validate(self: Request) RequestError!void {
        if (self.operation_id == 0) return error.InvalidOperationId;
        if (self.uid <= 0) return error.InvalidUserId;
        if (self.currency <= 0) return error.InvalidCurrency;
        if (self.amount <= 0) return error.InvalidAmount;
    }

    pub fn eql(a: Request, b: Request) bool {
        return a.operation_id == b.operation_id and
            a.kind == b.kind and
            a.uid == b.uid and
            a.currency == b.currency and
            a.amount == b.amount;
    }
};

pub const RequestError = error{
    InvalidOperationId,
    InvalidUserId,
    InvalidCurrency,
    InvalidAmount,
};

pub const Route = struct {
    destination_id: DestinationId,
    ledger: u32,
    treasury_account_id: u128,
    user_account_id: u128,

    pub fn validate(self: Route) RouteError!void {
        if (@intFromEnum(self.destination_id) == 0) return error.InvalidDestination;
        if (self.ledger == 0) return error.InvalidLedger;
        if (!validAccountId(self.treasury_account_id) or
            !validAccountId(self.user_account_id) or
            self.treasury_account_id == self.user_account_id)
        {
            return error.InvalidAccount;
        }
    }

    pub fn eql(a: Route, b: Route) bool {
        return a.destination_id == b.destination_id and
            a.ledger == b.ledger and
            a.treasury_account_id == b.treasury_account_id and
            a.user_account_id == b.user_account_id;
    }
};

pub const RouteError = error{
    InvalidDestination,
    InvalidLedger,
    InvalidAccount,
};

pub const Binding = struct {
    destination_id: DestinationId,
    registry_digest: [32]u8,
    profile_digest: [32]u8,

    pub fn validate(self: Binding) error{InvalidDestination}!void {
        if (@intFromEnum(self.destination_id) == 0) return error.InvalidDestination;
    }
};

pub const LocalIdFloor = struct {
    uid: i64,
    last_transaction_id: i64,
};

/// Durable funding ownership for one core user adjustment counter.
/// `last_reserved` may lead `last_confirmed` because acceptance reserves IDs
/// before either core side effect executes.
pub const LocalIdCounter = struct {
    uid: i64,
    last_reserved: i64,
    last_confirmed: i64,
};

pub const CorePurpose = enum(u8) {
    deposit_credit = 1,
    withdrawal_debit = 2,
    withdrawal_compensation = 3,
};

pub const ReconciliationCause = enum(u8) {
    ambiguous_submission = 1,
    conflict_response = 2,
};

/// Only claimable and terminal phases are persisted. An in-flight phase means
/// the claim was durable before the side effect was attempted.
pub const Phase = union(enum) {
    accepted,
    remote_submit_ready,
    remote_submit_in_flight,
    remote_lookup_ready: ReconciliationCause,
    remote_lookup_in_flight: ReconciliationCause,
    core_ready: CorePurpose,
    /// Recovery-only replay marker retaining proof for repeated crashes.
    core_replay_ready: CorePurpose,
    core_in_flight: CorePurpose,
    completed,
    failed: FailureReason,
    quarantined: QuarantineReason,
};

pub const Entry = struct {
    request: Request,
    route: Route,
    transfer: Transfer,
    primary_transaction_id: i64,
    compensation_transaction_id: i64,
    phase: Phase = .accepted,
    generation: u64 = 1,
    external_attempts: u32 = 0,
    core_attempts: u32 = 0,
};

pub const Status = struct {
    request: Request,
    state: PublicState,
    failure_reason: ?FailureReason = null,
    quarantine_reason: ?QuarantineReason = null,
};

pub const Summary = struct {
    accepted: usize = 0,
    pending: usize = 0,
    completed: usize = 0,
    failed: usize = 0,
    quarantined: usize = 0,
};

pub const AcceptDisposition = enum {
    inserted,
    existing,
};

pub const AcceptResult = struct {
    disposition: AcceptDisposition,
    status: Status,
};

pub const CoreAdjustment = struct {
    uid: i64,
    currency: i32,
    amount: i64,
    transaction_id: i64,
    purpose: CorePurpose,
};

pub const Action = union(enum) {
    remote_submit: Transfer,
    remote_lookup: RemoteLookup,
    core_adjust: CoreAdjustment,
};

pub const RemoteLookup = struct {
    transfer: Transfer,
    cause: ReconciliationCause,
};

pub const Claim = struct {
    operation_id: u128,
    generation: u64,
    action: Action,
};

pub const RemoteSubmitOutcome = enum {
    applied,
    conflict,
    known_not_sent,
    ambiguous,
    /// Application-specific proof that no external transfer was accepted.
    definitely_rejected,
    /// A generic transport terminal failure is not proof of economic absence.
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
    partial_application,
};

pub const CoreOutcome = enum {
    applied,
    already_applied_same,
    already_applied_many,
    retryable,
    rejected,
};

pub const Completion = union(enum) {
    remote_submit: RemoteSubmitOutcome,
    remote_lookup: RemoteLookupOutcome,
    core_adjust: CoreOutcome,
};

pub const LedgerError = RequestError || RouteError || error{
    InvalidBinding,
    InvalidFloor,
    DuplicateFloor,
    UnknownLocalIdFloor,
    LocalTransactionIdOverflow,
    CapacityExceeded,
    IdempotencyConflict,
    NotFound,
    NoPendingAction,
    StaleClaim,
    CompletionKindMismatch,
    InvalidState,
    GenerationOverflow,
    AttemptOverflow,
    MissingCoreCounter,
    DuplicateCoreCounter,
    CoreCounterRollback,
    CoreCounterMismatch,
};

const Counter = struct {
    uid: i64,
    last_reserved: i64,
    last_confirmed: i64,
};

/// Allocation-free deterministic saga state. File durability is layered on
/// top by `parix_funding_store.zig`, which persists candidate ledgers before
/// publishing them or returning a side-effect claim.
pub fn Ledger(comptime capacity: usize, comptime max_users: usize) type {
    if (capacity == 0 or capacity > 4_096) @compileError("invalid funding capacity");
    if (max_users == 0 or max_users > capacity) @compileError("invalid funding user capacity");

    return struct {
        const Self = @This();

        binding: Binding,
        entries: [capacity]Entry = undefined,
        entry_count: usize = 0,
        counters: [max_users]Counter = undefined,
        counter_count: usize = 0,
        snapshot_generation: u64 = 0,

        pub fn init(binding: Binding, floors: []const LocalIdFloor) LedgerError!Self {
            var self: Self = undefined;
            try initInto(&self, binding, floors);
            return self;
        }

        /// Initializes caller-owned storage without returning the potentially
        /// large fixed-capacity ledger through the stack.
        pub fn initInto(
            out: *Self,
            binding: Binding,
            floors: []const LocalIdFloor,
        ) LedgerError!void {
            binding.validate() catch return error.InvalidBinding;
            if (floors.len > max_users) return error.InvalidFloor;
            out.binding = binding;
            out.entry_count = 0;
            out.counter_count = 0;
            out.snapshot_generation = 0;
            for (floors) |floor| {
                if (floor.uid <= 0 or floor.last_transaction_id < 0) {
                    return error.InvalidFloor;
                }
                if (out.counterIndex(floor.uid) != null) return error.DuplicateFloor;
                out.counters[out.counter_count] = .{
                    .uid = floor.uid,
                    .last_reserved = floor.last_transaction_id,
                    .last_confirmed = floor.last_transaction_id,
                };
                out.counter_count += 1;
            }
        }

        pub fn entriesSlice(self: *const Self) []const Entry {
            return self.entries[0..self.entry_count];
        }

        pub fn counterCount(self: *const Self) usize {
            return self.counter_count;
        }

        pub fn counterAt(self: *const Self, index: usize) LocalIdCounter {
            std.debug.assert(index < self.counter_count);
            const counter = self.counters[index];
            return .{
                .uid = counter.uid,
                .last_reserved = counter.last_reserved,
                .last_confirmed = counter.last_confirmed,
            };
        }

        pub fn restore(
            binding: Binding,
            counters: []const LocalIdCounter,
            restored_entries: []const Entry,
            snapshot_generation: u64,
        ) LedgerError!Self {
            var self: Self = undefined;
            try restoreInto(
                &self,
                binding,
                counters,
                restored_entries,
                snapshot_generation,
            );
            return self;
        }

        /// Decodes into stable caller-owned storage to avoid nested copies of
        /// the 4,096-entry production ledger during restart.
        pub fn restoreInto(
            out: *Self,
            binding: Binding,
            counters: []const LocalIdCounter,
            restored_entries: []const Entry,
            snapshot_generation: u64,
        ) LedgerError!void {
            if (restored_entries.len > capacity) return error.CapacityExceeded;
            binding.validate() catch return error.InvalidBinding;
            if (counters.len > max_users) return error.InvalidFloor;
            out.binding = binding;
            out.entry_count = 0;
            out.counter_count = 0;
            out.snapshot_generation = 0;
            for (counters) |counter| {
                if (counter.uid <= 0 or counter.last_confirmed < 0 or
                    counter.last_reserved < counter.last_confirmed)
                {
                    return error.InvalidFloor;
                }
                if (out.counterIndex(counter.uid) != null) return error.DuplicateFloor;
                out.counters[out.counter_count] = .{
                    .uid = counter.uid,
                    .last_reserved = counter.last_reserved,
                    .last_confirmed = counter.last_confirmed,
                };
                out.counter_count += 1;
            }
            @memcpy(out.entries[0..restored_entries.len], restored_entries);
            out.entry_count = restored_entries.len;
            out.snapshot_generation = snapshot_generation;
            try out.validate();
        }

        pub fn accept(self: *Self, request: Request, route: Route) LedgerError!AcceptResult {
            try request.validate();
            try route.validate();
            if (route.destination_id != self.binding.destination_id) {
                return error.InvalidDestination;
            }
            if (self.findEntryIndex(request.operation_id)) |index| {
                const existing = &self.entries[index];
                if (!existing.request.eql(request) or !existing.route.eql(route)) {
                    return error.IdempotencyConflict;
                }
                return .{ .disposition = .existing, .status = statusFor(existing.*) };
            }
            if (self.entry_count == capacity) return error.CapacityExceeded;
            const counter_index = self.counterIndex(request.uid) orelse
                return error.UnknownLocalIdFloor;
            const reserve_count: i64 = if (request.kind == .withdrawal) 2 else 1;
            const previous = self.counters[counter_index].last_reserved;
            const reserved_last = std.math.add(i64, previous, reserve_count) catch
                return error.LocalTransactionIdOverflow;
            if (reserved_last <= 0) return error.LocalTransactionIdOverflow;
            const primary_id = previous + 1;
            const compensation_id = if (request.kind == .withdrawal) reserved_last else 0;
            const transfer = try fundingTransfer(request, route);

            try self.bumpSnapshotGeneration();
            self.counters[counter_index].last_reserved = reserved_last;
            self.entries[self.entry_count] = .{
                .request = request,
                .route = route,
                .transfer = transfer,
                .primary_transaction_id = primary_id,
                .compensation_transaction_id = compensation_id,
            };
            self.entry_count += 1;
            return .{
                .disposition = .inserted,
                .status = statusFor(self.entries[self.entry_count - 1]),
            };
        }

        pub fn lookup(self: *const Self, operation_id: u128) LedgerError!Status {
            const index = self.findEntryIndex(operation_id) orelse return error.NotFound;
            return statusFor(self.entries[index]);
        }

        pub fn summary(self: *const Self) Summary {
            var result: Summary = .{};
            for (self.entries[0..self.entry_count]) |entry| {
                switch (statusFor(entry).state) {
                    .accepted => result.accepted += 1,
                    .pending => result.pending += 1,
                    .completed => result.completed += 1,
                    .failed => result.failed += 1,
                    .quarantined => result.quarantined += 1,
                }
            }
            return result;
        }

        /// Validates the recovered core against FSG1 before in-flight phases
        /// are rewritten. Only the exact immutable transaction exposed by a
        /// durable `core_in_flight` phase may explain a one-step lead.
        pub fn reconcileCoreCounters(
            self: *Self,
            observed: []const LocalIdFloor,
        ) LedgerError!usize {
            if (observed.len != self.counter_count) return error.MissingCoreCounter;
            var reconciled: [max_users]i64 = undefined;
            var advanced: usize = 0;
            for (self.counters[0..self.counter_count], 0..) |counter, counter_index| {
                var found: ?i64 = null;
                for (observed) |observation| {
                    if (observation.uid != counter.uid) continue;
                    if (found != null) return error.DuplicateCoreCounter;
                    if (observation.last_transaction_id < 0) return error.CoreCounterRollback;
                    found = observation.last_transaction_id;
                }
                const actual = found orelse return error.MissingCoreCounter;
                if (actual < counter.last_confirmed) return error.CoreCounterRollback;
                reconciled[counter_index] = actual;
                if (actual == counter.last_confirmed) continue;

                var allowed: ?i64 = null;
                for (self.entries[0..self.entry_count]) |entry| {
                    if (entry.request.uid != counter.uid) continue;
                    const purpose = switch (entry.phase) {
                        .core_in_flight, .core_replay_ready => |value| value,
                        else => continue,
                    };
                    const transaction_id = coreAdjustment(entry, purpose).transaction_id;
                    if (allowed != null) return error.CoreCounterMismatch;
                    allowed = transaction_id;
                }
                if (allowed == null or actual != allowed.? or actual > counter.last_reserved) {
                    return error.CoreCounterMismatch;
                }
                advanced += 1;
            }
            for (observed) |observation| {
                if (self.counterIndex(observation.uid) == null) return error.CoreCounterMismatch;
            }
            if (advanced != 0) {
                try self.bumpSnapshotGeneration();
                for (self.counters[0..self.counter_count], 0..) |*counter, counter_index| {
                    counter.last_confirmed = reconciled[counter_index];
                }
            }
            return advanced;
        }

        /// Claims the oldest nonterminal operation. This deliberately uses one
        /// global FIFO; it is stricter than per-user ordering and makes the
        /// core's single adjustment counter impossible to overtake.
        pub fn claimNext(self: *Self) LedgerError!?Claim {
            for (self.entries[0..self.entry_count]) |*entry| {
                if (isTerminal(entry.phase)) continue;
                return try self.claimEntry(entry);
            }
            return null;
        }

        pub fn complete(
            self: *Self,
            claim: Claim,
            completion: Completion,
        ) LedgerError!Status {
            const index = self.findEntryIndex(claim.operation_id) orelse return error.NotFound;
            var candidate = self.entries[index];
            var confirmed_counter_index: ?usize = null;
            var confirmed_transaction_id: i64 = 0;
            if (candidate.generation != claim.generation) return error.StaleClaim;

            switch (candidate.phase) {
                .remote_submit_in_flight => {
                    const outcome = switch (completion) {
                        .remote_submit => |value| value,
                        else => return error.CompletionKindMismatch,
                    };
                    try completeRemoteSubmit(&candidate, outcome);
                },
                .remote_lookup_in_flight => |cause| {
                    const outcome = switch (completion) {
                        .remote_lookup => |value| value,
                        else => return error.CompletionKindMismatch,
                    };
                    try completeRemoteLookup(&candidate, cause, outcome);
                },
                .core_in_flight => |purpose| {
                    const outcome = switch (completion) {
                        .core_adjust => |value| value,
                        else => return error.CompletionKindMismatch,
                    };
                    try completeCore(&candidate, purpose, outcome);
                    switch (outcome) {
                        .applied, .already_applied_same => {
                            confirmed_transaction_id = coreAdjustment(candidate, purpose).transaction_id;
                            confirmed_counter_index = try self.validateCoreTransaction(
                                candidate.request.uid,
                                confirmed_transaction_id,
                            );
                        },
                        .already_applied_many, .retryable, .rejected => {},
                    }
                },
                else => return error.StaleClaim,
            }
            try bumpEntryGeneration(&candidate);
            try self.bumpSnapshotGeneration();
            if (confirmed_counter_index) |counter_index| {
                self.counters[counter_index].last_confirmed = confirmed_transaction_id;
            }
            self.entries[index] = candidate;
            return statusFor(candidate);
        }

        /// Restarts never blindly repeat an ambiguous external submission.
        /// Local adjustments are safe to replay with their immutable reserved
        /// transaction ID, while lookup itself is naturally idempotent.
        pub fn recover(self: *Self) LedgerError!usize {
            var recovered: usize = 0;
            for (self.entries[0..self.entry_count]) |entry| {
                switch (entry.phase) {
                    .remote_submit_in_flight,
                    .remote_lookup_in_flight,
                    .core_in_flight,
                    => {
                        if (entry.generation == std.math.maxInt(u64)) {
                            return error.GenerationOverflow;
                        }
                        recovered += 1;
                    },
                    else => continue,
                }
            }
            if (recovered != 0) {
                if (self.snapshot_generation == std.math.maxInt(u64)) {
                    return error.GenerationOverflow;
                }
                for (self.entries[0..self.entry_count]) |*entry| {
                    switch (entry.phase) {
                        .remote_submit_in_flight => entry.phase = .{
                            .remote_lookup_ready = .ambiguous_submission,
                        },
                        .remote_lookup_in_flight => |cause| entry.phase = .{
                            .remote_lookup_ready = cause,
                        },
                        .core_in_flight => |purpose| entry.phase = .{ .core_replay_ready = purpose },
                        else => continue,
                    }
                    entry.generation += 1;
                }
                self.snapshot_generation += 1;
            }
            return recovered;
        }

        pub fn validate(self: *const Self) LedgerError!void {
            self.binding.validate() catch return error.InvalidBinding;
            if (self.entry_count > capacity or self.counter_count > max_users) {
                return error.InvalidState;
            }
            for (self.counters[0..self.counter_count], 0..) |counter, index| {
                if (counter.uid <= 0 or counter.last_confirmed < 0 or
                    counter.last_reserved < counter.last_confirmed)
                {
                    return error.InvalidState;
                }
                for (self.counters[0..index]) |previous| {
                    if (previous.uid == counter.uid) return error.InvalidState;
                }
            }
            for (self.entries[0..self.entry_count], 0..) |entry, index| {
                entry.request.validate() catch return error.InvalidState;
                entry.route.validate() catch return error.InvalidState;
                if (entry.route.destination_id != self.binding.destination_id or
                    !transferMatches(entry)) return error.InvalidState;
                if (entry.primary_transaction_id <= 0 or
                    (entry.request.kind == .deposit and entry.compensation_transaction_id != 0) or
                    (entry.request.kind == .withdrawal and
                        entry.compensation_transaction_id != entry.primary_transaction_id + 1))
                {
                    return error.InvalidState;
                }
                if (!validPhaseForKind(entry.request.kind, entry.phase)) return error.InvalidState;
                for (self.entries[0..index]) |previous| {
                    if (previous.request.operation_id == entry.request.operation_id or
                        previous.transfer.id == entry.transfer.id) return error.InvalidState;
                }
                const counter_index = self.counterIndex(entry.request.uid) orelse
                    return error.InvalidState;
                const reserved_last = if (entry.request.kind == .withdrawal)
                    entry.compensation_transaction_id
                else
                    entry.primary_transaction_id;
                if (reserved_last > self.counters[counter_index].last_reserved) {
                    return error.InvalidState;
                }
            }
        }

        fn claimEntry(self: *Self, entry: *Entry) LedgerError!?Claim {
            const action: Action = switch (entry.phase) {
                .accepted => switch (entry.request.kind) {
                    .deposit => .{ .remote_submit = entry.transfer },
                    .withdrawal => .{ .core_adjust = coreAdjustment(entry.*, .withdrawal_debit) },
                },
                .remote_submit_ready => .{ .remote_submit = entry.transfer },
                .remote_lookup_ready => |cause| .{ .remote_lookup = .{
                    .transfer = entry.transfer,
                    .cause = cause,
                } },
                .core_ready, .core_replay_ready => |purpose| .{
                    .core_adjust = coreAdjustment(entry.*, purpose),
                },
                .remote_submit_in_flight,
                .remote_lookup_in_flight,
                .core_in_flight,
                => return null,
                .completed, .failed, .quarantined => unreachable,
            };

            switch (action) {
                .remote_submit => {
                    entry.external_attempts = std.math.add(u32, entry.external_attempts, 1) catch
                        return error.AttemptOverflow;
                    entry.phase = .remote_submit_in_flight;
                },
                .remote_lookup => |reconciliation| {
                    entry.external_attempts = std.math.add(u32, entry.external_attempts, 1) catch
                        return error.AttemptOverflow;
                    entry.phase = .{ .remote_lookup_in_flight = reconciliation.cause };
                },
                .core_adjust => |adjustment| {
                    entry.core_attempts = std.math.add(u32, entry.core_attempts, 1) catch
                        return error.AttemptOverflow;
                    entry.phase = .{ .core_in_flight = adjustment.purpose };
                },
            }
            try bumpEntryGeneration(entry);
            try self.bumpSnapshotGeneration();
            return .{
                .operation_id = entry.request.operation_id,
                .generation = entry.generation,
                .action = action,
            };
        }

        fn findEntryIndex(self: *const Self, operation_id: u128) ?usize {
            for (self.entries[0..self.entry_count], 0..) |entry, index| {
                if (entry.request.operation_id == operation_id) return index;
            }
            return null;
        }

        fn counterIndex(self: *const Self, uid: i64) ?usize {
            for (self.counters[0..self.counter_count], 0..) |counter, index| {
                if (counter.uid == uid) return index;
            }
            return null;
        }

        fn validateCoreTransaction(
            self: *const Self,
            uid: i64,
            transaction_id: i64,
        ) LedgerError!usize {
            const index = self.counterIndex(uid) orelse return error.UnknownLocalIdFloor;
            const counter = self.counters[index];
            if (transaction_id < counter.last_confirmed or transaction_id > counter.last_reserved) {
                return error.CoreCounterMismatch;
            }
            return index;
        }

        fn bumpSnapshotGeneration(self: *Self) LedgerError!void {
            self.snapshot_generation = std.math.add(u64, self.snapshot_generation, 1) catch
                return error.GenerationOverflow;
        }
    };
}

pub const DefaultLedger = Ledger(4_096, 16);

pub fn fundingTransfer(request: Request, route: Route) (RequestError || RouteError)!Transfer {
    try request.validate();
    try route.validate();
    const transfer_id = deterministicTransferId(request, route);
    return .{
        .id = transfer_id,
        .debit_account_id = switch (request.kind) {
            .deposit => route.treasury_account_id,
            .withdrawal => route.user_account_id,
        },
        .credit_account_id = switch (request.kind) {
            .deposit => route.user_account_id,
            .withdrawal => route.treasury_account_id,
        },
        .amount = @intCast(request.amount),
        .user_data_128 = request.operation_id,
        .user_data_32 = workflow_revision,
        .ledger = route.ledger,
        .code = switch (request.kind) {
            .deposit => deposit_transfer_code,
            .withdrawal => withdrawal_transfer_code,
        },
    };
}

pub fn deterministicTransferId(request: Request, route: Route) u128 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(transfer_id_domain);
    hashInt(&hasher, u128, @intFromEnum(route.destination_id));
    hashInt(&hasher, u128, request.operation_id);
    hashInt(&hasher, u8, @intFromEnum(request.kind));
    hashInt(&hasher, i64, request.uid);
    hashInt(&hasher, i32, request.currency);
    hashInt(&hasher, i64, request.amount);
    hashInt(&hasher, u32, route.ledger);
    hashInt(&hasher, u128, route.treasury_account_id);
    hashInt(&hasher, u128, route.user_account_id);
    var digest: [32]u8 = undefined;
    hasher.final(&digest);
    var id = std.mem.readInt(u128, digest[0..16], .little) | (@as(u128, 1) << 127);
    if (id == std.math.maxInt(u128)) id -= 1;
    return id;
}

fn hashInt(hasher: anytype, comptime T: type, value: T) void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    hasher.update(&bytes);
}

fn validAccountId(id: u128) bool {
    return id != 0 and id != std.math.maxInt(u128);
}

fn transferMatches(entry: Entry) bool {
    const expected = fundingTransfer(entry.request, entry.route) catch return false;
    return std.meta.eql(expected, entry.transfer);
}

fn statusFor(entry: Entry) Status {
    return switch (entry.phase) {
        .accepted => .{ .request = entry.request, .state = .accepted },
        .completed => .{ .request = entry.request, .state = .completed },
        .failed => |reason| .{
            .request = entry.request,
            .state = .failed,
            .failure_reason = reason,
        },
        .quarantined => |reason| .{
            .request = entry.request,
            .state = .quarantined,
            .quarantine_reason = reason,
        },
        else => .{ .request = entry.request, .state = .pending },
    };
}

fn isTerminal(phase: Phase) bool {
    return switch (phase) {
        .completed, .failed, .quarantined => true,
        else => false,
    };
}

fn coreAdjustment(entry: Entry, purpose: CorePurpose) CoreAdjustment {
    return .{
        .uid = entry.request.uid,
        .currency = entry.request.currency,
        .amount = switch (purpose) {
            .deposit_credit, .withdrawal_compensation => entry.request.amount,
            .withdrawal_debit => -entry.request.amount,
        },
        .transaction_id = switch (purpose) {
            .deposit_credit, .withdrawal_debit => entry.primary_transaction_id,
            .withdrawal_compensation => entry.compensation_transaction_id,
        },
        .purpose = purpose,
    };
}

fn completeRemoteSubmit(entry: *Entry, outcome: RemoteSubmitOutcome) LedgerError!void {
    switch (outcome) {
        .applied => remoteCommitted(entry),
        .conflict => entry.phase = .{ .remote_lookup_ready = .conflict_response },
        .ambiguous => entry.phase = .{ .remote_lookup_ready = .ambiguous_submission },
        .known_not_sent => entry.phase = .remote_submit_ready,
        .definitely_rejected => remoteDefinitelyRejected(entry),
        .operator_action => entry.phase = .{ .quarantined = .reconciliation_failed },
        .destination_mismatch => entry.phase = .{ .quarantined = .destination_mismatch },
    }
}

fn completeRemoteLookup(
    entry: *Entry,
    cause: ReconciliationCause,
    outcome: RemoteLookupOutcome,
) LedgerError!void {
    switch (outcome) {
        .exact => remoteCommitted(entry),
        .absent => switch (cause) {
            .ambiguous_submission => entry.phase = .remote_submit_ready,
            .conflict_response => entry.phase = .{ .quarantined = .reconciliation_failed },
        },
        .retryable => entry.phase = .{ .remote_lookup_ready = cause },
        .permanent_failure => entry.phase = .{ .quarantined = .reconciliation_failed },
        .destination_mismatch => entry.phase = .{ .quarantined = .destination_mismatch },
        .transfer_mismatch => entry.phase = .{ .quarantined = .transfer_mismatch },
        .partial_application => entry.phase = .{ .quarantined = .partial_application },
    }
}

fn remoteCommitted(entry: *Entry) void {
    entry.phase = switch (entry.request.kind) {
        .deposit => .{ .core_ready = .deposit_credit },
        .withdrawal => .completed,
    };
}

fn remoteDefinitelyRejected(entry: *Entry) void {
    entry.phase = switch (entry.request.kind) {
        .deposit => .{ .failed = .external_rejected },
        .withdrawal => .{ .core_ready = .withdrawal_compensation },
    };
}

fn completeCore(entry: *Entry, purpose: CorePurpose, outcome: CoreOutcome) LedgerError!void {
    switch (outcome) {
        .retryable => {
            entry.phase = .{ .core_ready = purpose };
            return;
        },
        .already_applied_many => {
            entry.phase = .{ .quarantined = .local_order_unprovable };
            return;
        },
        .rejected => {
            entry.phase = switch (purpose) {
                .withdrawal_debit => .{ .failed = .local_rejected },
                .deposit_credit => .{ .quarantined = .local_rejected_after_external_commit },
                .withdrawal_compensation => .{ .quarantined = .compensation_failed },
            };
            return;
        },
        .applied, .already_applied_same => {},
    }

    entry.phase = switch (purpose) {
        .deposit_credit => if (entry.request.kind == .deposit)
            .completed
        else
            return error.InvalidState,
        .withdrawal_debit => if (entry.request.kind == .withdrawal)
            .remote_submit_ready
        else
            return error.InvalidState,
        .withdrawal_compensation => if (entry.request.kind == .withdrawal)
            .{ .failed = .external_rejected }
        else
            return error.InvalidState,
    };
}

fn bumpEntryGeneration(entry: *Entry) LedgerError!void {
    entry.generation = std.math.add(u64, entry.generation, 1) catch
        return error.GenerationOverflow;
}

fn validPhaseForKind(kind: Kind, phase: Phase) bool {
    return switch (phase) {
        .core_ready, .core_replay_ready, .core_in_flight => |purpose| switch (kind) {
            .deposit => purpose == .deposit_credit,
            .withdrawal => purpose != .deposit_credit,
        },
        .completed,
        .accepted,
        .remote_submit_ready,
        .remote_submit_in_flight,
        .remote_lookup_ready,
        .remote_lookup_in_flight,
        .quarantined,
        => true,
        .failed => |reason| switch (kind) {
            .deposit => reason == .external_rejected,
            .withdrawal => true,
        },
    };
}

fn testBinding() Binding {
    return .{
        .destination_id = @enumFromInt(77),
        .registry_digest = [_]u8{0x11} ** 32,
        .profile_digest = [_]u8{0x22} ** 32,
    };
}

fn testRoute() Route {
    return .{
        .destination_id = testBinding().destination_id,
        .ledger = 421,
        .treasury_account_id = 100,
        .user_account_id = 101,
    };
}

fn testRequest(operation_id: u128, kind: Kind) Request {
    return .{
        .operation_id = operation_id,
        .kind = kind,
        .uid = 1_001,
        .currency = 11,
        .amount = 50,
    };
}

test "deposit commits remotely before local credit" {
    var ledger = try Ledger(8, 2).init(testBinding(), &.{
        .{ .uid = 1_001, .last_transaction_id = 8_421_011 },
    });
    const accepted = try ledger.accept(testRequest(1, .deposit), testRoute());
    try std.testing.expectEqual(AcceptDisposition.inserted, accepted.disposition);
    try std.testing.expectEqual(PublicState.accepted, accepted.status.state);

    const remote = (try ledger.claimNext()).?;
    try std.testing.expect(remote.action == .remote_submit);
    try std.testing.expectEqual(@as(i64, 8_421_012), ledger.entries[0].primary_transaction_id);
    const pending = try ledger.complete(remote, .{ .remote_submit = .applied });
    try std.testing.expectEqual(PublicState.pending, pending.state);

    const local = (try ledger.claimNext()).?;
    try std.testing.expectEqual(@as(i64, 50), local.action.core_adjust.amount);
    const completed = try ledger.complete(local, .{ .core_adjust = .applied });
    try std.testing.expectEqual(PublicState.completed, completed.state);
    try ledger.validate();
}

test "withdrawal reserves compensation id and compensates definite rejection" {
    var ledger = try Ledger(8, 2).init(testBinding(), &.{
        .{ .uid = 1_001, .last_transaction_id = 9 },
    });
    _ = try ledger.accept(testRequest(2, .withdrawal), testRoute());
    try std.testing.expectEqual(@as(i64, 10), ledger.entries[0].primary_transaction_id);
    try std.testing.expectEqual(@as(i64, 11), ledger.entries[0].compensation_transaction_id);

    const debit = (try ledger.claimNext()).?;
    try std.testing.expectEqual(@as(i64, -50), debit.action.core_adjust.amount);
    _ = try ledger.complete(debit, .{ .core_adjust = .already_applied_same });
    const remote = (try ledger.claimNext()).?;
    _ = try ledger.complete(remote, .{ .remote_submit = .definitely_rejected });
    const compensation = (try ledger.claimNext()).?;
    try std.testing.expectEqual(CorePurpose.withdrawal_compensation, compensation.action.core_adjust.purpose);
    try std.testing.expectEqual(@as(i64, 11), compensation.action.core_adjust.transaction_id);
    const failed = try ledger.complete(compensation, .{ .core_adjust = .applied });
    try std.testing.expectEqual(PublicState.failed, failed.state);
    try std.testing.expectEqual(FailureReason.external_rejected, failed.failure_reason.?);
}

test "duplicates are exact and global FIFO prevents overtaking" {
    var ledger = try Ledger(8, 2).init(testBinding(), &.{
        .{ .uid = 1_001, .last_transaction_id = 0 },
    });
    const request = testRequest(3, .deposit);
    _ = try ledger.accept(request, testRoute());
    const duplicate = try ledger.accept(request, testRoute());
    try std.testing.expectEqual(AcceptDisposition.existing, duplicate.disposition);

    var conflict = request;
    conflict.amount += 1;
    try std.testing.expectError(
        error.IdempotencyConflict,
        ledger.accept(conflict, testRoute()),
    );
    const first = (try ledger.claimNext()).?;
    try std.testing.expect((try ledger.claimNext()) == null);
    _ = try ledger.complete(first, .{ .remote_submit = .known_not_sent });
    try std.testing.expect((try ledger.claimNext()) != null);
}

test "recovery reconciles external submissions and replays local ids" {
    var deposit = try Ledger(8, 2).init(testBinding(), &.{
        .{ .uid = 1_001, .last_transaction_id = 0 },
    });
    _ = try deposit.accept(testRequest(4, .deposit), testRoute());
    _ = (try deposit.claimNext()).?;
    try std.testing.expectEqual(@as(usize, 1), try deposit.recover());
    const lookup = (try deposit.claimNext()).?;
    try std.testing.expect(lookup.action == .remote_lookup);

    var withdrawal = try Ledger(8, 2).init(testBinding(), &.{
        .{ .uid = 1_001, .last_transaction_id = 4 },
    });
    _ = try withdrawal.accept(testRequest(5, .withdrawal), testRoute());
    const first = (try withdrawal.claimNext()).?;
    const transaction_id = first.action.core_adjust.transaction_id;
    try std.testing.expectEqual(@as(usize, 1), try withdrawal.recover());
    const replay = (try withdrawal.claimNext()).?;
    try std.testing.expectEqual(transaction_id, replay.action.core_adjust.transaction_id);
}

test "core counter reconciliation is exact atomic and survives repeated recovery" {
    var ledger = try Ledger(8, 2).init(testBinding(), &.{
        .{ .uid = 1_001, .last_transaction_id = 4 },
        .{ .uid = 2_002, .last_transaction_id = 9 },
    });
    _ = try ledger.accept(testRequest(50, .withdrawal), testRoute());
    const in_flight = (try ledger.claimNext()).?;
    try std.testing.expectEqual(@as(i64, 5), in_flight.action.core_adjust.transaction_id);

    try std.testing.expectError(
        error.CoreCounterRollback,
        ledger.reconcileCoreCounters(&.{
            .{ .uid = 1_001, .last_transaction_id = 5 },
            .{ .uid = 2_002, .last_transaction_id = 8 },
        }),
    );
    try std.testing.expectEqual(@as(i64, 4), ledger.counterAt(0).last_confirmed);

    try std.testing.expectEqual(@as(usize, 1), try ledger.reconcileCoreCounters(&.{
        .{ .uid = 1_001, .last_transaction_id = 5 },
        .{ .uid = 2_002, .last_transaction_id = 9 },
    }));
    try std.testing.expectEqual(@as(usize, 1), try ledger.recover());
    try std.testing.expectEqual(@as(usize, 0), try ledger.reconcileCoreCounters(&.{
        .{ .uid = 1_001, .last_transaction_id = 5 },
        .{ .uid = 2_002, .last_transaction_id = 9 },
    }));
    const replay = (try ledger.claimNext()).?;
    try std.testing.expectEqual(@as(i64, 5), replay.action.core_adjust.transaction_id);
}

test "ambiguous transfer requires exact lookup and mismatch quarantines" {
    var ledger = try Ledger(8, 2).init(testBinding(), &.{
        .{ .uid = 1_001, .last_transaction_id = 0 },
    });
    _ = try ledger.accept(testRequest(6, .deposit), testRoute());
    const submit = (try ledger.claimNext()).?;
    _ = try ledger.complete(submit, .{ .remote_submit = .ambiguous });
    const lookup = (try ledger.claimNext()).?;
    const quarantined = try ledger.complete(
        lookup,
        .{ .remote_lookup = .transfer_mismatch },
    );
    try std.testing.expectEqual(PublicState.quarantined, quarantined.state);
    try std.testing.expectEqual(
        QuarantineReason.transfer_mismatch,
        quarantined.quarantine_reason.?,
    );
}

test "conflict absent reconciliation quarantines across crash while ambiguous absent retries" {
    var withdrawal = try Ledger(8, 1).init(testBinding(), &.{
        .{ .uid = 1_001, .last_transaction_id = 0 },
    });
    _ = try withdrawal.accept(testRequest(60, .withdrawal), testRoute());
    const debit = (try withdrawal.claimNext()).?;
    _ = try withdrawal.complete(debit, .{ .core_adjust = .applied });
    const submit = (try withdrawal.claimNext()).?;
    _ = try withdrawal.complete(submit, .{ .remote_submit = .conflict });
    const lookup = (try withdrawal.claimNext()).?;
    try std.testing.expectEqual(
        ReconciliationCause.conflict_response,
        lookup.action.remote_lookup.cause,
    );
    try std.testing.expectEqual(@as(usize, 1), try withdrawal.recover());
    const replay = (try withdrawal.claimNext()).?;
    try std.testing.expectEqual(
        ReconciliationCause.conflict_response,
        replay.action.remote_lookup.cause,
    );
    const quarantined = try withdrawal.complete(replay, .{ .remote_lookup = .absent });
    try std.testing.expectEqual(PublicState.quarantined, quarantined.state);
    try std.testing.expectEqual(QuarantineReason.reconciliation_failed, quarantined.quarantine_reason.?);
    try std.testing.expect((try withdrawal.claimNext()) == null);
    try std.testing.expectEqual(@as(u32, 1), withdrawal.entries[0].core_attempts);

    var deposit = try Ledger(8, 1).init(testBinding(), &.{
        .{ .uid = 1_001, .last_transaction_id = 0 },
    });
    _ = try deposit.accept(testRequest(61, .deposit), testRoute());
    const ambiguous = (try deposit.claimNext()).?;
    _ = try deposit.complete(ambiguous, .{ .remote_submit = .ambiguous });
    const absent_lookup = (try deposit.claimNext()).?;
    try std.testing.expectEqual(
        ReconciliationCause.ambiguous_submission,
        absent_lookup.action.remote_lookup.cause,
    );
    _ = try deposit.complete(absent_lookup, .{ .remote_lookup = .absent });
    try std.testing.expect((try deposit.claimNext()).?.action == .remote_submit);
}

test "transfer ids are deterministic high-bit and domain separated" {
    const deposit = try fundingTransfer(testRequest(7, .deposit), testRoute());
    const same = try fundingTransfer(testRequest(7, .deposit), testRoute());
    const withdrawal = try fundingTransfer(testRequest(7, .withdrawal), testRoute());
    try std.testing.expectEqual(deposit, same);
    try std.testing.expect(deposit.id != withdrawal.id);
    try std.testing.expect((deposit.id & (@as(u128, 1) << 127)) != 0);
    try std.testing.expectEqual(@as(u128, 7), deposit.user_data_128);
}

test "validated state restores counters entries and generation" {
    const SmallLedger = Ledger(8, 2);
    var original = try SmallLedger.init(testBinding(), &.{
        .{ .uid = 1_001, .last_transaction_id = 8_421_011 },
    });
    _ = try original.accept(testRequest(8, .withdrawal), testRoute());
    const floor = original.counterAt(0);
    const restored = try SmallLedger.restore(
        original.binding,
        &.{floor},
        original.entriesSlice(),
        original.snapshot_generation,
    );
    try std.testing.expectEqual(original.snapshot_generation, restored.snapshot_generation);
    try std.testing.expectEqualDeep(original.entriesSlice(), restored.entriesSlice());
    try std.testing.expectEqualDeep(floor, restored.counterAt(0));
}

const CampaignResult = struct {
    fingerprint: u64 = 0xcbf2_9ce4_8422_2325,
    crashes: u32 = 0,
    steps: u32 = 0,
};

fn runSeededCampaign(seed: u64) !CampaignResult {
    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();
    const kind: Kind = if (random.boolean()) .deposit else .withdrawal;
    var ledger = try Ledger(4, 1).init(testBinding(), &.{
        .{ .uid = 1_001, .last_transaction_id = 100 },
    });
    _ = try ledger.accept(testRequest(10_000 + seed, kind), testRoute());
    var result: CampaignResult = .{};

    while (result.steps < 64) : (result.steps += 1) {
        const state = (try ledger.lookup(10_000 + seed)).state;
        if (state == .completed or state == .failed or state == .quarantined) break;
        const claim = (try ledger.claimNext()) orelse continue;
        mixCampaign(&result, @intFromEnum(std.meta.activeTag(claim.action)));
        try expectDurableClaimPhase(ledger.entries[0].phase, claim.action);

        if (random.uintLessThan(u8, 5) == 0) {
            _ = try ledger.recover();
            result.crashes += 1;
            mixCampaign(&result, 0xff);
            try ledger.validate();
            continue;
        }

        const completion: Completion = switch (claim.action) {
            .remote_submit => .{ .remote_submit = switch (random.uintLessThan(u8, 3)) {
                0 => .known_not_sent,
                1 => .ambiguous,
                else => .applied,
            } },
            .remote_lookup => .{ .remote_lookup = switch (random.uintLessThan(u8, 3)) {
                0 => .absent,
                1 => .retryable,
                else => .exact,
            } },
            .core_adjust => .{ .core_adjust = switch (random.uintLessThan(u8, 3)) {
                0 => .retryable,
                1 => .already_applied_same,
                else => .applied,
            } },
        };
        _ = try ledger.complete(claim, completion);
        try ledger.validate();
    }

    // A deterministic fault-free suffix must always prove convergence.
    var suffix_steps: usize = 0;
    while ((try ledger.lookup(10_000 + seed)).state != .completed) : (suffix_steps += 1) {
        if (suffix_steps == 16) return error.CampaignDidNotConverge;
        const claim = (try ledger.claimNext()) orelse return error.CampaignBlocked;
        try expectDurableClaimPhase(ledger.entries[0].phase, claim.action);
        const completion: Completion = switch (claim.action) {
            .remote_submit => .{ .remote_submit = .applied },
            .remote_lookup => .{ .remote_lookup = .exact },
            .core_adjust => .{ .core_adjust = .applied },
        };
        _ = try ledger.complete(claim, completion);
        mixCampaign(
            &result,
            @as(u8, 0x80) | @as(u8, @intFromEnum(std.meta.activeTag(claim.action))),
        );
        try ledger.validate();
    }
    return result;
}

fn expectDurableClaimPhase(phase: Phase, action: Action) !void {
    const valid = switch (action) {
        .remote_submit => phase == .remote_submit_in_flight,
        .remote_lookup => |lookup| switch (phase) {
            .remote_lookup_in_flight => |cause| cause == lookup.cause,
            else => false,
        },
        .core_adjust => phase == .core_in_flight,
    };
    try std.testing.expect(valid);
}

fn mixCampaign(result: *CampaignResult, value: u8) void {
    result.fingerprint = (result.fingerprint ^ value) *% 0x0000_0100_0000_01b3;
}

test "seeded crash schedules are reproducible and converge" {
    var saw_crash = false;
    for (0..64) |seed_index| {
        const seed: u64 = @intCast(seed_index + 1);
        const first = try runSeededCampaign(seed);
        const replay = try runSeededCampaign(seed);
        try std.testing.expectEqual(first, replay);
        saw_crash = saw_crash or first.crashes != 0;
    }
    try std.testing.expect(saw_crash);
}
