//! Transport-neutral dispatcher for the opt-in TigerBeetle plugin.
const std = @import("std");
const outbox = @import("tigerbeetle_outbox.zig");
const settlement = @import("tigerbeetle_settlement.zig");

pub const RetryTick = enum(u64) {
    _,

    pub fn fromInt(value: u64) RetryTick {
        return @enumFromInt(value);
    }

    pub fn toInt(self: RetryTick) u64 {
        return @intFromEnum(self);
    }
};

pub const RetryDelay = enum(u64) {
    _,

    pub fn fromInt(value: u64) RetryDelay {
        return @enumFromInt(value);
    }

    pub fn toInt(self: RetryDelay) u64 {
        return @intFromEnum(self);
    }
};

pub const Backoff = struct {
    initial: RetryDelay,
    maximum: RetryDelay,

    fn validate(self: Backoff) PolicyError!void {
        if (self.initial.toInt() > self.maximum.toInt()) return error.InvalidBackoff;
        if (self.initial.toInt() == 0 and self.maximum.toInt() != 0) return error.InvalidBackoff;
    }

    fn deadline(self: Backoff, now: RetryTick, generation: u64) DeadlineError!RetryTick {
        var delay = self.initial.toInt();
        const maximum = self.maximum.toInt();
        var exponent = generation -| 1;
        while (exponent != 0 and delay < maximum) : (exponent -= 1) {
            delay = @min(std.math.mul(u64, delay, 2) catch maximum, maximum);
        }
        return .fromInt(std.math.add(u64, now.toInt(), delay) catch return error.ClockExhausted);
    }
};

pub const RetryPolicy = struct {
    known_not_sent: Backoff,
    reconciliation: Backoff,
    confirmed_absent: Backoff,

    pub fn validate(self: RetryPolicy) PolicyError!void {
        try self.known_not_sent.validate();
        try self.reconciliation.validate();
        try self.confirmed_absent.validate();
    }
};

pub const PolicyError = error{InvalidBackoff};
pub const DeadlineError = error{ClockExhausted};

pub const SubmitClaim = struct {
    token: outbox.ClaimToken,
    /// Canonical send-only borrow. Serialize it immediately; completion uses
    /// the token to resolve the durable entry again and never trusts this
    /// pointer after the outbox changes.
    chain: *const settlement.TransferChain,
};

pub const ReconciliationClaim = struct {
    token: outbox.ClaimToken,
    chain: *const settlement.TransferChain,
    cause: outbox.ReconciliationCause,
};

pub const ClaimResult = union(enum) {
    submit: SubmitClaim,
    reconcile: ReconciliationClaim,
    blocked: outbox.DispatchState,
};

pub const ResponseOperation = enum {
    create_transfers,
    lookup_transfers,
};

pub const CreateReceipt = struct {
    destination_id: outbox.DestinationId,
    operation: ResponseOperation,
    persisted: bool,
};

/// Parix HTTP 409 is a problem-details response, not a normal create envelope.
/// The request metadata binds that response to the claim without pretending it
/// was present in the response body.
pub const ConflictReceipt = struct {
    request_destination_id: outbox.DestinationId,
    request_operation: ResponseOperation,
    item_error_count: u32,
};

/// Semantic certainty reported by a submit adapter. HTTP status handling,
/// credentials, JSON, and raw TigerBeetle result codes remain adapter-owned.
pub const SubmitDisposition = union(enum) {
    success: CreateReceipt,
    conflict: ConflictReceipt,
    /// Proof exists that no bytes could reach the destination.
    known_not_sent,
    /// The write may have committed and must be looked up.
    ambiguous,
    /// An authoritative response proves the requested economic effect was
    /// rejected. Adapters must not use this for policy, quota, or malformed
    /// responses whose commit status is not proven.
    definitely_rejected,
    /// Operator action is required, but this alone says nothing about whether
    /// the economic effect committed.
    permanent,
    destination_mismatch,
};

pub const LookupBatch = struct {
    storage: [settlement.max_transfers_per_fill]settlement.Transfer =
        [_]settlement.Transfer{std.mem.zeroes(settlement.Transfer)} ** settlement.max_transfers_per_fill,
    count: u8 = 0,

    pub fn init(transfers: []const settlement.Transfer) LookupBatchError!LookupBatch {
        if (transfers.len > settlement.max_transfers_per_fill) return error.TooManyTransfers;
        var batch: LookupBatch = .{};
        for (transfers, 0..) |transfer, index| batch.storage[index] = transfer;
        batch.count = @intCast(transfers.len);
        return batch;
    }

    pub fn items(self: *const LookupBatch) LookupBatchError![]const settlement.Transfer {
        if (self.count > settlement.max_transfers_per_fill) return error.TooManyTransfers;
        return self.storage[0..@as(usize, self.count)];
    }
};

pub const LookupBatchError = error{TooManyTransfers};

pub const LookupReceipt = struct {
    destination_id: outbox.DestinationId,
    operation: ResponseOperation,
    persisted: bool,
    batch: LookupBatch,
};

pub const LookupDisposition = union(enum) {
    transfers: LookupReceipt,
    retryable,
    permanent,
    destination_mismatch,
};

pub const ReconciliationEvidence = enum {
    all_present_exact,
    all_absent,
    partial_application,
    transfer_mismatch,
};

pub const Completion = struct {
    result: outbox.ApplyResult,
    pruned: usize,
};

pub const DispatchError = outbox.StoreError || DeadlineError || error{ClockRegressed};

/// Pure, caller-driven settlement dispatcher.
///
/// The caller invokes `claim` only when its transport has capacity, performs
/// exactly one submit or lookup using the owned claim value, then completes the
/// claim with semantic certainty. This keeps asynchronous HTTP and credentials
/// outside exchange-core while preserving the durable pre-send marker.
pub fn Dispatcher(comptime Store: type) type {
    return struct {
        const Self = @This();

        store: *Store,
        policy: RetryPolicy,
        last_tick: ?RetryTick = null,

        pub fn init(store: *Store, policy: RetryPolicy) PolicyError!Self {
            try policy.validate();
            return .{ .store = store, .policy = policy };
        }

        pub fn claim(self: *Self, now: RetryTick) DispatchError!ClaimResult {
            try self.observeTick(now);
            _ = try self.store.pruneDeliveredPrefix();
            const before = self.store.dispatchState();
            const work = try self.store.beginNext(now.toInt()) orelse {
                return .{ .blocked = self.store.dispatchState() };
            };
            const canonical_chain = switch (try self.store.resolveClaim(work.token, work.token.kind)) {
                .active => |chain| chain,
                .already_terminal => unreachable,
            };
            return switch (work.token.kind) {
                .submit => .{ .submit = .{
                    .token = work.token,
                    .chain = canonical_chain,
                } },
                .reconcile => .{ .reconcile = .{
                    .token = work.token,
                    .chain = canonical_chain,
                    .cause = switch (before) {
                        .waiting_reconciliation => |waiting| waiting.cause,
                        else => unreachable,
                    },
                } },
            };
        }

        pub fn completeSubmit(
            self: *Self,
            claim_value: SubmitClaim,
            disposition: SubmitDisposition,
            now: RetryTick,
        ) DispatchError!Completion {
            const canonical_chain = switch (try self.store.resolveClaim(claim_value.token, .submit)) {
                .active => |chain| chain,
                .already_terminal => {
                    if (!terminalSubmitDisposition(claim_value.token, disposition)) return error.StaleClaim;
                    const result = try self.store.applySubmitOutcome(claim_value.token, .committed);
                    return .{ .result = result, .pruned = try self.store.pruneDeliveredPrefix() };
                },
            };
            _ = canonical_chain;
            _ = try self.store.validateClaim(.{
                .token = claim_value.token,
                .chain = claim_value.chain.*,
            });
            try self.observeTick(now);
            const outcome: outbox.SubmitOutcome = switch (disposition) {
                .success => |receipt| response: {
                    if (receipt.destination_id != claim_value.token.destination_id) {
                        break :response .{ .permanent = .destination_mismatch };
                    }
                    if (receipt.operation != .create_transfers or !receipt.persisted) {
                        break :response .{ .permanent = .permanent_submission_failure };
                    }
                    break :response .committed;
                },
                .conflict => |receipt| conflict: {
                    if (receipt.request_destination_id != claim_value.token.destination_id) {
                        break :conflict .{ .permanent = .destination_mismatch };
                    }
                    if (receipt.item_error_count == 0 or receipt.request_operation != .create_transfers) {
                        break :conflict .{ .ambiguous = try self.retryAt(
                            self.policy.reconciliation,
                            now,
                            claim_value.token.generation,
                        ) };
                    }
                    break :conflict .{ .rejected = try self.retryAt(
                        self.policy.reconciliation,
                        now,
                        claim_value.token.generation,
                    ) };
                },
                .known_not_sent => .{ .known_not_sent = try self.retryAt(
                    self.policy.known_not_sent,
                    now,
                    claim_value.token.generation,
                ) },
                .ambiguous => .{ .ambiguous = try self.retryAt(
                    self.policy.reconciliation,
                    now,
                    claim_value.token.generation,
                ) },
                .definitely_rejected => .{ .permanent = .permanent_submission_failure },
                .permanent => .{ .permanent = .permanent_submission_failure },
                .destination_mismatch => .{ .permanent = .destination_mismatch },
            };
            const result = try self.store.applySubmitOutcome(claim_value.token, outcome);
            return .{ .result = result, .pruned = try self.store.pruneDeliveredPrefix() };
        }

        pub fn completeLookup(
            self: *Self,
            claim_value: ReconciliationClaim,
            disposition: LookupDisposition,
            now: RetryTick,
        ) DispatchError!Completion {
            const canonical_chain = switch (try self.store.resolveClaim(claim_value.token, .reconcile)) {
                .active => |chain| chain,
                .already_terminal => {
                    if (!terminalLookupDisposition(claim_value.token, disposition)) return error.StaleClaim;
                    const result = try self.store.applyReconcileOutcome(claim_value.token, .all_present_exact);
                    return .{ .result = result, .pruned = try self.store.pruneDeliveredPrefix() };
                },
            };
            _ = try self.store.validateClaim(.{
                .token = claim_value.token,
                .chain = claim_value.chain.*,
            });
            try self.observeTick(now);
            const outcome: outbox.ReconcileOutcome = switch (disposition) {
                .transfers => |receipt| if (receipt.destination_id != claim_value.token.destination_id)
                    .{ .permanent = .destination_mismatch }
                else if (receipt.operation != .lookup_transfers or receipt.persisted)
                    .{ .permanent = .permanent_reconciliation_failure }
                else switch (classifyLookup(canonical_chain.*, receipt.batch)) {
                    .all_present_exact => .all_present_exact,
                    .all_absent => .{ .all_absent = try self.retryAt(
                        self.policy.confirmed_absent,
                        now,
                        claim_value.token.generation,
                    ) },
                    .partial_application => .partial_application,
                    .transfer_mismatch => .transfer_mismatch,
                },
                .retryable => .{ .retryable = try self.retryAt(
                    self.policy.reconciliation,
                    now,
                    claim_value.token.generation,
                ) },
                .permanent => .{ .permanent = .permanent_reconciliation_failure },
                .destination_mismatch => .{ .permanent = .destination_mismatch },
            };
            const result = try self.store.applyReconcileOutcome(claim_value.token, outcome);
            return .{ .result = result, .pruned = try self.store.pruneDeliveredPrefix() };
        }

        fn observeTick(self: *Self, now: RetryTick) DispatchError!void {
            if (self.last_tick) |previous| {
                if (now.toInt() < previous.toInt()) return error.ClockRegressed;
            }
            self.last_tick = now;
        }

        fn retryAt(
            self: *const Self,
            backoff: Backoff,
            now: RetryTick,
            generation: u64,
        ) DeadlineError!outbox.RetryAt {
            _ = self;
            return .{ .tick = (try backoff.deadline(now, generation)).toInt() };
        }
    };
}

fn terminalSubmitDisposition(token: outbox.ClaimToken, disposition: SubmitDisposition) bool {
    return switch (disposition) {
        .success => |receipt| receipt.destination_id == token.destination_id and
            receipt.operation == .create_transfers and receipt.persisted,
        else => false,
    };
}

fn terminalLookupDisposition(token: outbox.ClaimToken, disposition: LookupDisposition) bool {
    return switch (disposition) {
        .transfers => |receipt| receipt.destination_id == token.destination_id and
            receipt.operation == .lookup_transfers and !receipt.persisted,
        else => false,
    };
}

/// Classifies an authoritative lookup as an ID-keyed set. Every persisted,
/// caller-owned immutable field is compared. The server-assigned timestamp and
/// request-only `linked` bit are ignored because TigerBeetle stores neither as
/// submitted reconciliation evidence.
pub fn classifyLookup(
    expected: settlement.TransferChain,
    observed: LookupBatch,
) ReconciliationEvidence {
    const actual = observed.items() catch return .transfer_mismatch;
    if (actual.len == 0) return .all_absent;
    if (expected.count == 0 or expected.count > settlement.max_transfers_per_fill) {
        return .transfer_mismatch;
    }

    var seen = [_]bool{false} ** settlement.max_transfers_per_fill;
    for (actual) |remote| {
        var matched_index: ?usize = null;
        for (expected.items(), 0..) |intended, index| {
            if (remote.id == intended.id) {
                matched_index = index;
                if (!sameImmutableTransfer(remote, intended)) return .transfer_mismatch;
                break;
            }
        }
        const index = matched_index orelse return .transfer_mismatch;
        if (seen[index]) return .transfer_mismatch;
        seen[index] = true;
    }

    return if (actual.len == expected.items().len)
        .all_present_exact
    else
        .partial_application;
}

fn sameImmutableTransfer(actual: settlement.Transfer, expected: settlement.Transfer) bool {
    return actual.id == expected.id and
        actual.debit_account_id == expected.debit_account_id and
        actual.credit_account_id == expected.credit_account_id and
        actual.amount == expected.amount and
        actual.pending_id == expected.pending_id and
        actual.user_data_128 == expected.user_data_128 and
        actual.user_data_64 == expected.user_data_64 and
        actual.user_data_32 == expected.user_data_32 and
        actual.timeout == expected.timeout and
        actual.ledger == expected.ledger and
        actual.code == expected.code and
        (actual.flags & ~settlement.linked_flag) ==
            (expected.flags & ~settlement.linked_flag);
}

fn testDestination() outbox.DestinationId {
    return @enumFromInt(77);
}

fn testRecord(chain: settlement.TransferChain) outbox.SettlementRecord {
    return .{ .destination_id = testDestination(), .chain = chain };
}

fn testPolicy() RetryPolicy {
    return .{
        .known_not_sent = .{
            .initial = .fromInt(2),
            .maximum = .fromInt(16),
        },
        .reconciliation = .{
            .initial = .fromInt(3),
            .maximum = .fromInt(24),
        },
        .confirmed_absent = .{
            .initial = .fromInt(5),
            .maximum = .fromInt(40),
        },
    };
}

fn testChain(group_id: u128, transfer_base: u128, count: u8) settlement.TransferChain {
    var chain: settlement.TransferChain = .{
        .group_id = group_id,
        .storage = [_]settlement.Transfer{std.mem.zeroes(settlement.Transfer)} ** settlement.max_transfers_per_fill,
        .count = count,
    };
    for (chain.storage[0..@as(usize, count)], 0..) |*transfer, index| {
        transfer.* = .{
            .id = transfer_base + index,
            .debit_account_id = 10_000 + index * 2,
            .credit_account_id = 10_001 + index * 2,
            .amount = 100 + index,
            .user_data_128 = group_id,
            .user_data_64 = 915_000,
            .user_data_32 = 1,
            .ledger = if (index + 1 == count) 978 else 840,
            .code = @intCast(300 + index),
            .flags = if (index + 1 < count) settlement.linked_flag else 0,
        };
    }
    return chain;
}

fn expectSubmit(result: ClaimResult) !SubmitClaim {
    return switch (result) {
        .submit => |claim_value| claim_value,
        else => error.ExpectedSubmitClaim,
    };
}

fn expectReconciliation(result: ClaimResult) !ReconciliationClaim {
    return switch (result) {
        .reconcile => |claim_value| claim_value,
        else => error.ExpectedReconciliationClaim,
    };
}

fn testCommit(claim_value: SubmitClaim) SubmitDisposition {
    return .{ .success = .{
        .destination_id = claim_value.token.destination_id,
        .operation = .create_transfers,
        .persisted = true,
    } };
}

fn testConflict(claim_value: SubmitClaim, item_error_count: u32) SubmitDisposition {
    return .{ .conflict = .{
        .request_destination_id = claim_value.token.destination_id,
        .request_operation = .create_transfers,
        .item_error_count = item_error_count,
    } };
}

fn testLookup(
    claim_value: ReconciliationClaim,
    batch: LookupBatch,
) LookupDisposition {
    return .{ .transfers = .{
        .destination_id = claim_value.token.destination_id,
        .operation = .lookup_transfers,
        .persisted = false,
        .batch = batch,
    } };
}

test "dispatcher validates policy and computes capped saturating backoff" {
    const invalid: RetryPolicy = .{
        .known_not_sent = .{ .initial = .fromInt(2), .maximum = .fromInt(1) },
        .reconciliation = .{ .initial = .fromInt(0), .maximum = .fromInt(0) },
        .confirmed_absent = .{ .initial = .fromInt(0), .maximum = .fromInt(0) },
    };
    try std.testing.expectError(error.InvalidBackoff, invalid.validate());

    const zero_initial_positive_maximum: RetryPolicy = .{
        .known_not_sent = .{ .initial = .fromInt(0), .maximum = .fromInt(1) },
        .reconciliation = .{ .initial = .fromInt(0), .maximum = .fromInt(0) },
        .confirmed_absent = .{ .initial = .fromInt(0), .maximum = .fromInt(0) },
    };
    try std.testing.expectError(error.InvalidBackoff, zero_initial_positive_maximum.validate());

    const backoff: Backoff = .{ .initial = .fromInt(3), .maximum = .fromInt(10) };
    try std.testing.expectEqual(@as(u64, 103), (try backoff.deadline(.fromInt(100), 1)).toInt());
    try std.testing.expectEqual(@as(u64, 106), (try backoff.deadline(.fromInt(100), 2)).toInt());
    try std.testing.expectEqual(@as(u64, 110), (try backoff.deadline(.fromInt(100), 9)).toInt());
    try std.testing.expectError(
        error.ClockExhausted,
        backoff.deadline(.fromInt(std.math.maxInt(u64) - 1), 1),
    );
}

test "dispatcher commits and prunes one immutable submit claim" {
    const Store = outbox.SettlementOutbox(2);
    const Driver = Dispatcher(Store);
    var store = try Store.init(testDestination());
    const chain = testChain(100, 1000, 3);
    _ = try store.enqueue(testRecord(chain));
    var driver = try Driver.init(&store, testPolicy());

    const claim_value = try expectSubmit(try driver.claim(.fromInt(0)));
    try std.testing.expectEqualDeep(chain, claim_value.chain.*);
    const completion = try driver.completeSubmit(claim_value, testCommit(claim_value), .fromInt(0));
    try std.testing.expectEqual(outbox.ApplyResult.updated, completion.result);
    try std.testing.expectEqual(@as(usize, 1), completion.pruned);
    try std.testing.expectEqual(@as(usize, 0), store.len());

    const duplicate = try driver.completeSubmit(claim_value, testCommit(claim_value), .fromInt(0));
    try std.testing.expectEqual(outbox.ApplyResult.already_terminal, duplicate.result);
    try std.testing.expectEqual(@as(usize, 0), duplicate.pruned);
    try std.testing.expectError(
        error.StaleClaim,
        driver.completeSubmit(claim_value, .ambiguous, .fromInt(0)),
    );
}

test "dispatcher rejects a mutated claimed payload before acknowledgement" {
    const Store = outbox.SettlementOutbox(2);
    const Driver = Dispatcher(Store);
    var store = try Store.init(testDestination());
    _ = try store.enqueue(testRecord(testChain(150, 1500, 2)));
    var driver = try Driver.init(&store, testPolicy());

    const original = try expectSubmit(try driver.claim(.fromInt(0)));
    var mutated_chain = original.chain.*;
    mutated_chain.storage[0].amount += 1;
    var mutated = original;
    mutated.chain = &mutated_chain;
    try std.testing.expectError(
        error.ClaimPayloadMismatch,
        driver.completeSubmit(mutated, testCommit(mutated), .fromInt(0)),
    );
    _ = try driver.completeSubmit(original, testCommit(original), .fromInt(0));
    try std.testing.expectEqual(@as(usize, 0), store.len());
}

test "lookup classifier is order independent and ignores only timestamp" {
    const chain = testChain(200, 2000, 3);
    var reordered = [_]settlement.Transfer{
        chain.storage[2],
        chain.storage[0],
        chain.storage[1],
    };
    reordered[0].timestamp = 99;
    reordered[1].timestamp = 100;
    reordered[2].timestamp = 101;
    for (&reordered) |*transfer| transfer.flags &= ~settlement.linked_flag;
    try std.testing.expectEqual(
        ReconciliationEvidence.all_present_exact,
        classifyLookup(chain, try LookupBatch.init(&reordered)),
    );
    try std.testing.expectEqual(
        ReconciliationEvidence.all_absent,
        classifyLookup(chain, try LookupBatch.init(&.{})),
    );
    try std.testing.expectEqual(
        ReconciliationEvidence.partial_application,
        classifyLookup(chain, try LookupBatch.init(reordered[0..2])),
    );

    var mismatch = reordered;
    mismatch[1].amount += 1;
    try std.testing.expectEqual(
        ReconciliationEvidence.transfer_mismatch,
        classifyLookup(chain, try LookupBatch.init(&mismatch)),
    );
    const duplicate = [_]settlement.Transfer{ reordered[0], reordered[0] };
    try std.testing.expectEqual(
        ReconciliationEvidence.transfer_mismatch,
        classifyLookup(chain, try LookupBatch.init(&duplicate)),
    );
}

test "ambiguous absent lookup retries the same chain only at its deadline" {
    const Store = outbox.SettlementOutbox(2);
    const Driver = Dispatcher(Store);
    var store = try Store.init(testDestination());
    const chain = testChain(300, 3000, 2);
    _ = try store.enqueue(testRecord(chain));
    var driver = try Driver.init(&store, testPolicy());

    const submit = try expectSubmit(try driver.claim(.fromInt(0)));
    _ = try driver.completeSubmit(submit, .ambiguous, .fromInt(0));
    switch (try driver.claim(.fromInt(2))) {
        .blocked => |state| switch (state) {
            .waiting_reconciliation => |waiting| try std.testing.expectEqual(@as(u64, 3), waiting.retry.tick),
            else => return error.ExpectedReconciliationDelay,
        },
        else => return error.ExpectedBlockedClaim,
    }

    const lookup = try expectReconciliation(try driver.claim(.fromInt(3)));
    try std.testing.expectEqual(outbox.ReconciliationCause.ambiguous_submission, lookup.cause);
    _ = try driver.completeLookup(
        lookup,
        testLookup(lookup, try LookupBatch.init(&.{})),
        .fromInt(3),
    );
    switch (try driver.claim(.fromInt(12))) {
        .blocked => |state| switch (state) {
            .waiting_submit => |waiting| try std.testing.expectEqual(@as(u64, 13), waiting.retry.tick),
            else => return error.ExpectedSubmitDelay,
        },
        else => return error.ExpectedBlockedClaim,
    }
    const retried = try expectSubmit(try driver.claim(.fromInt(13)));
    try std.testing.expectEqualDeep(chain, retried.chain.*);
    _ = try driver.completeSubmit(retried, testCommit(retried), .fromInt(13));
    try std.testing.expectEqual(@as(usize, 0), store.len());
}

test "rejected absent lookup quarantines instead of retrying forever" {
    const Store = outbox.SettlementOutbox(2);
    const Driver = Dispatcher(Store);
    var store = try Store.init(testDestination());
    _ = try store.enqueue(testRecord(testChain(400, 4000, 2)));
    var driver = try Driver.init(&store, testPolicy());

    const submit = try expectSubmit(try driver.claim(.fromInt(0)));
    _ = try driver.completeSubmit(submit, testConflict(submit, 1), .fromInt(0));
    const lookup = try expectReconciliation(try driver.claim(.fromInt(3)));
    try std.testing.expectEqual(outbox.ReconciliationCause.rejected_submission, lookup.cause);
    _ = try driver.completeLookup(
        lookup,
        testLookup(lookup, try LookupBatch.init(&.{})),
        .fromInt(3),
    );
    switch (try driver.claim(.fromInt(std.math.maxInt(u64)))) {
        .blocked => |state| switch (state) {
            .quarantined => |quarantine| try std.testing.expectEqual(
                outbox.QuarantineReason.permanent_submission_failure,
                quarantine.reason,
            ),
            else => return error.ExpectedQuarantine,
        },
        else => return error.ExpectedBlockedClaim,
    }
}

test "rejected exact-duplicate lookup delivers and malformed conflict remains ambiguous" {
    const Store = outbox.SettlementOutbox(2);
    const Driver = Dispatcher(Store);

    var duplicate_store = try Store.init(testDestination());
    const duplicate_chain = testChain(450, 4500, 2);
    _ = try duplicate_store.enqueue(testRecord(duplicate_chain));
    var duplicate_driver = try Driver.init(&duplicate_store, testPolicy());
    const duplicate_submit = try expectSubmit(try duplicate_driver.claim(.fromInt(0)));
    _ = try duplicate_driver.completeSubmit(
        duplicate_submit,
        testConflict(duplicate_submit, 1),
        .fromInt(0),
    );
    const duplicate_lookup = try expectReconciliation(try duplicate_driver.claim(.fromInt(3)));
    try std.testing.expectEqual(outbox.ReconciliationCause.rejected_submission, duplicate_lookup.cause);
    _ = try duplicate_driver.completeLookup(
        duplicate_lookup,
        testLookup(duplicate_lookup, try LookupBatch.init(duplicate_chain.items())),
        .fromInt(3),
    );
    try std.testing.expectEqual(@as(usize, 0), duplicate_store.len());

    const malformed_conflicts = [_]ConflictReceipt{
        .{
            .request_destination_id = testDestination(),
            .request_operation = .create_transfers,
            .item_error_count = 0,
        },
        .{
            .request_destination_id = testDestination(),
            .request_operation = .lookup_transfers,
            .item_error_count = 1,
        },
    };
    for (malformed_conflicts, 0..) |receipt, index| {
        var malformed_store = try Store.init(testDestination());
        _ = try malformed_store.enqueue(testRecord(testChain(451 + index, 4510 + index, 1)));
        var malformed_driver = try Driver.init(&malformed_store, testPolicy());
        const malformed_submit = try expectSubmit(try malformed_driver.claim(.fromInt(0)));
        _ = try malformed_driver.completeSubmit(
            malformed_submit,
            .{ .conflict = receipt },
            .fromInt(0),
        );
        const malformed_lookup = try expectReconciliation(try malformed_driver.claim(.fromInt(3)));
        try std.testing.expectEqual(
            outbox.ReconciliationCause.ambiguous_submission,
            malformed_lookup.cause,
        );
    }
}

test "exact lookup delivers while partial and mismatching lookups quarantine" {
    const cases = [_]ReconciliationEvidence{
        .all_present_exact,
        .partial_application,
        .transfer_mismatch,
    };
    for (cases, 0..) |evidence, case_index| {
        const Store = outbox.SettlementOutbox(2);
        const Driver = Dispatcher(Store);
        var store = try Store.init(testDestination());
        const chain = testChain(500 + case_index, 5000 + case_index * 10, 2);
        _ = try store.enqueue(testRecord(chain));
        var driver = try Driver.init(&store, testPolicy());
        const submit = try expectSubmit(try driver.claim(.fromInt(0)));
        _ = try driver.completeSubmit(submit, .ambiguous, .fromInt(0));
        const lookup = try expectReconciliation(try driver.claim(.fromInt(3)));

        var remote = [_]settlement.Transfer{ chain.storage[1], chain.storage[0] };
        const observed = switch (evidence) {
            .all_present_exact => try LookupBatch.init(&remote),
            .partial_application => try LookupBatch.init(remote[0..1]),
            .transfer_mismatch => blk: {
                remote[0].credit_account_id += 99;
                break :blk try LookupBatch.init(&remote);
            },
            .all_absent => unreachable,
        };
        _ = try driver.completeLookup(lookup, testLookup(lookup, observed), .fromInt(3));

        if (evidence == .all_present_exact) {
            try std.testing.expectEqual(@as(usize, 0), store.len());
        } else {
            const expected_reason: outbox.QuarantineReason = if (evidence == .partial_application)
                .partial_application
            else
                .transfer_mismatch;
            switch (store.dispatchState()) {
                .quarantined => |quarantine| try std.testing.expectEqual(expected_reason, quarantine.reason),
                else => return error.ExpectedQuarantine,
            }
        }
    }
}

test "terminal lookup replay stays idempotent after prefix pruning shifts the next entry" {
    const Store = outbox.SettlementOutbox(2);
    const Driver = Dispatcher(Store);
    var store = try Store.init(testDestination());
    const first_chain = testChain(550, 5500, 2);
    const second_chain = testChain(551, 5510, 1);
    _ = try store.enqueue(testRecord(first_chain));
    _ = try store.enqueue(testRecord(second_chain));
    var driver = try Driver.init(&store, testPolicy());

    const submit = try expectSubmit(try driver.claim(.fromInt(0)));
    _ = try driver.completeSubmit(submit, .ambiguous, .fromInt(0));
    const lookup = try expectReconciliation(try driver.claim(.fromInt(3)));
    const receipt = testLookup(lookup, try LookupBatch.init(first_chain.items()));
    const completed = try driver.completeLookup(lookup, receipt, .fromInt(3));
    try std.testing.expectEqual(outbox.ApplyResult.updated, completed.result);
    try std.testing.expectEqual(@as(usize, 1), completed.pruned);

    // The old pointer now aliases shifted inline storage. A terminal replay is
    // resolved by token and must neither dereference it nor advance the clock.
    try std.testing.expectError(
        error.StaleClaim,
        driver.completeLookup(lookup, .retryable, .fromInt(0)),
    );
    const replayed = try driver.completeLookup(lookup, receipt, .fromInt(0));
    try std.testing.expectEqual(outbox.ApplyResult.already_terminal, replayed.result);
    const next = try expectSubmit(try driver.claim(.fromInt(3)));
    try std.testing.expectEqual(second_chain.group_id, next.token.group_id);
    try std.testing.expectEqualDeep(second_chain, next.chain.*);
}

test "lookup retry and clock regression cannot cause a blind submit" {
    const Store = outbox.SettlementOutbox(2);
    const Driver = Dispatcher(Store);
    var store = try Store.init(testDestination());
    _ = try store.enqueue(testRecord(testChain(600, 6000, 1)));
    var driver = try Driver.init(&store, testPolicy());

    const submit = try expectSubmit(try driver.claim(.fromInt(5)));
    try std.testing.expectError(
        error.ClockRegressed,
        driver.completeSubmit(submit, .ambiguous, .fromInt(4)),
    );
    _ = try driver.completeSubmit(submit, .ambiguous, .fromInt(5));
    const first_lookup = try expectReconciliation(try driver.claim(.fromInt(8)));
    _ = try driver.completeLookup(first_lookup, .retryable, .fromInt(8));
    switch (try driver.claim(.fromInt(13))) {
        .blocked => |state| switch (state) {
            .waiting_reconciliation => |waiting| try std.testing.expectEqual(@as(u64, 14), waiting.retry.tick),
            else => return error.ExpectedReconciliationDelay,
        },
        else => return error.ExpectedBlockedClaim,
    }
    const second_lookup = try expectReconciliation(try driver.claim(.fromInt(14)));
    try std.testing.expectEqual(outbox.WorkKind.reconcile, second_lookup.token.kind);
    try std.testing.expectError(
        error.StaleClaim,
        driver.completeLookup(first_lookup, .retryable, .fromInt(14)),
    );
    switch (store.dispatchState()) {
        .in_flight => |in_flight| {
            try std.testing.expectEqual(outbox.WorkKind.reconcile, in_flight.kind);
            try std.testing.expectEqual(second_lookup.token.group_id, in_flight.group_id);
        },
        else => return error.ExpectedInFlight,
    }
}

test "late submit completion cannot overwrite a newer reconciliation claim" {
    const Store = outbox.SettlementOutbox(2);
    const Driver = Dispatcher(Store);
    var store = try Store.init(testDestination());
    const chain = testChain(650, 6500, 2);
    _ = try store.enqueue(testRecord(chain));
    var driver = try Driver.init(&store, testPolicy());

    const submit = try expectSubmit(try driver.claim(.fromInt(0)));
    _ = try driver.completeSubmit(submit, .ambiguous, .fromInt(0));
    const lookup = try expectReconciliation(try driver.claim(.fromInt(3)));
    try std.testing.expect(lookup.token.generation > submit.token.generation);
    try std.testing.expectError(
        error.StaleClaim,
        driver.completeSubmit(submit, testCommit(submit), .fromInt(3)),
    );
    const observed = try LookupBatch.init(chain.items());
    _ = try driver.completeLookup(lookup, testLookup(lookup, observed), .fromInt(3));
    try std.testing.expectEqual(@as(usize, 0), store.len());
}

test "dispatcher validates normalized response envelope metadata" {
    const Store = outbox.SettlementOutbox(2);
    const Driver = Dispatcher(Store);
    const wrong_destination: outbox.DestinationId = @enumFromInt(78);

    const CreateCase = struct {
        receipt: CreateReceipt,
        reason: outbox.QuarantineReason,
    };
    const create_cases = [_]CreateCase{
        .{ .receipt = .{
            .destination_id = wrong_destination,
            .operation = .create_transfers,
            .persisted = true,
        }, .reason = .destination_mismatch },
        .{ .receipt = .{
            .destination_id = testDestination(),
            .operation = .lookup_transfers,
            .persisted = true,
        }, .reason = .permanent_submission_failure },
        .{ .receipt = .{
            .destination_id = testDestination(),
            .operation = .create_transfers,
            .persisted = false,
        }, .reason = .permanent_submission_failure },
    };
    for (create_cases, 0..) |case, index| {
        var store = try Store.init(testDestination());
        _ = try store.enqueue(testRecord(testChain(675 + index, 6750 + index, 1)));
        var driver = try Driver.init(&store, testPolicy());
        const submit = try expectSubmit(try driver.claim(.fromInt(0)));
        _ = try driver.completeSubmit(submit, .{ .success = case.receipt }, .fromInt(0));
        switch (store.dispatchState()) {
            .quarantined => |quarantine| try std.testing.expectEqual(case.reason, quarantine.reason),
            else => return error.ExpectedQuarantine,
        }
    }

    const LookupCase = struct {
        destination_id: outbox.DestinationId,
        operation: ResponseOperation,
        persisted: bool,
        reason: outbox.QuarantineReason,
    };
    const lookup_cases = [_]LookupCase{
        .{
            .destination_id = wrong_destination,
            .operation = .lookup_transfers,
            .persisted = false,
            .reason = .destination_mismatch,
        },
        .{
            .destination_id = testDestination(),
            .operation = .create_transfers,
            .persisted = false,
            .reason = .permanent_reconciliation_failure,
        },
        .{
            .destination_id = testDestination(),
            .operation = .lookup_transfers,
            .persisted = true,
            .reason = .permanent_reconciliation_failure,
        },
    };
    for (lookup_cases, 0..) |case, index| {
        var store = try Store.init(testDestination());
        _ = try store.enqueue(testRecord(testChain(680 + index, 6800 + index, 1)));
        var driver = try Driver.init(&store, testPolicy());
        const initial = try expectSubmit(try driver.claim(.fromInt(0)));
        _ = try driver.completeSubmit(initial, .ambiguous, .fromInt(0));
        const lookup = try expectReconciliation(try driver.claim(.fromInt(3)));
        _ = try driver.completeLookup(lookup, .{ .transfers = .{
            .destination_id = case.destination_id,
            .operation = case.operation,
            .persisted = case.persisted,
            .batch = try LookupBatch.init(&.{}),
        } }, .fromInt(3));
        switch (store.dispatchState()) {
            .quarantined => |quarantine| try std.testing.expectEqual(case.reason, quarantine.reason),
            else => return error.ExpectedQuarantine,
        }
    }

    var conflict_store = try Store.init(testDestination());
    _ = try conflict_store.enqueue(testRecord(testChain(685, 6850, 1)));
    var conflict_driver = try Driver.init(&conflict_store, testPolicy());
    const conflict_submit = try expectSubmit(try conflict_driver.claim(.fromInt(0)));
    _ = try conflict_driver.completeSubmit(conflict_submit, .{ .conflict = .{
        .request_destination_id = wrong_destination,
        .request_operation = .create_transfers,
        .item_error_count = 0,
    } }, .fromInt(0));
    switch (conflict_store.dispatchState()) {
        .quarantined => |quarantine| try std.testing.expectEqual(
            outbox.QuarantineReason.destination_mismatch,
            quarantine.reason,
        ),
        else => return error.ExpectedQuarantine,
    }
}

test "positive retry delay rejects an exhausted logical clock" {
    const Store = outbox.SettlementOutbox(1);
    const Driver = Dispatcher(Store);
    var store = try Store.init(testDestination());
    _ = try store.enqueue(testRecord(testChain(690, 6900, 1)));
    var driver = try Driver.init(&store, testPolicy());

    const submit = try expectSubmit(try driver.claim(.fromInt(std.math.maxInt(u64))));
    try std.testing.expectError(
        error.ClockExhausted,
        driver.completeSubmit(submit, .ambiguous, .fromInt(std.math.maxInt(u64))),
    );
    switch (store.dispatchState()) {
        .in_flight => |in_flight| try std.testing.expectEqual(outbox.WorkKind.submit, in_flight.kind),
        else => return error.ExpectedInFlight,
    }
}

test "durable dispatcher restart turns an exposed submit into lookup work" {
    const allocator = std.testing.allocator;
    const Store = outbox.DurableFileOutbox(2);
    const Driver = Dispatcher(Store);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const chain = testChain(700, 7000, 2);
    {
        var store = try Store.open(allocator, tmp.dir, "dispatcher.tso", testDestination());
        defer store.deinit();
        _ = try store.enqueue(testRecord(chain));
        var driver = try Driver.init(&store, testPolicy());
        _ = try expectSubmit(try driver.claim(.fromInt(0)));
    }

    {
        var store = try Store.open(allocator, tmp.dir, "dispatcher.tso", testDestination());
        defer store.deinit();
        var driver = try Driver.init(&store, testPolicy());
        const lookup = try expectReconciliation(try driver.claim(.fromInt(0)));
        try std.testing.expectEqual(outbox.ReconciliationCause.ambiguous_submission, lookup.cause);
        try std.testing.expectEqualDeep(chain, lookup.chain.*);
    }
}

const FakeRemote = struct {
    chain: ?settlement.TransferChain = null,
    reconciliation_required: bool = false,
    economic_effects: u32 = 0,
    submits: u32 = 0,
    lookups: u32 = 0,

    fn apply(self: *FakeRemote, chain: settlement.TransferChain) !void {
        if (self.reconciliation_required) return error.BlindResubmitBeforeReconciliation;
        self.submits += 1;
        if (self.chain != null) return error.BlindResubmitBeforeReconciliation;
        self.chain = chain;
        self.economic_effects += 1;
    }

    fn recordKnownNotSent(self: *FakeRemote) !void {
        if (self.reconciliation_required) return error.BlindResubmitBeforeReconciliation;
        if (self.chain != null) return error.BlindResubmitBeforeReconciliation;
        self.submits += 1;
    }

    fn lookup(self: *FakeRemote, reverse: bool) !LookupBatch {
        self.lookups += 1;
        self.reconciliation_required = false;
        return self.inspect(reverse);
    }

    fn inspect(self: *const FakeRemote, reverse: bool) !LookupBatch {
        const chain = self.chain orelse return LookupBatch.init(&.{});
        var observed = [_]settlement.Transfer{std.mem.zeroes(settlement.Transfer)} ** settlement.max_transfers_per_fill;
        for (chain.items(), 0..) |transfer, index| {
            const destination_index = if (reverse)
                @as(usize, chain.count) - index - 1
            else
                index;
            observed[destination_index] = transfer;
            observed[destination_index].timestamp = 10_000 + index;
            observed[destination_index].flags &= ~settlement.linked_flag;
        }
        return LookupBatch.init(observed[0..@as(usize, chain.count)]);
    }
};

const CampaignResult = struct {
    steps: u32 = 0,
    restarts: u32 = 0,
    submits: u32 = 0,
    lookups: u32 = 0,
    economic_effects: u32 = 0,
    fingerprint: u64 = 0,
    coverage: u16 = 0,
    suffix_steps: u8 = 0,
    final_tick: u64 = 0,
};

fn mixCampaign(
    result: *CampaignResult,
    action: u64,
    tick: u64,
    token: outbox.ClaimToken,
    remote_present: bool,
) void {
    const group_low: u64 = @truncate(token.group_id);
    const kind: u64 = @intFromEnum(token.kind);
    result.fingerprint = (result.fingerprint *% 0x9E37_79B1_85EB_CA87) ^
        action ^ tick ^ group_low ^ (token.generation *% 0xD6E8_FEB8_6659_FD93) ^
        (kind << 61) ^ @intFromBool(remote_present);
}

fn mixCampaignWait(result: *CampaignResult, action: u64, tick: u64, deadline: u64) void {
    result.fingerprint = (result.fingerprint *% 0xA24B_AED4_963E_E407) ^
        action ^ tick ^ (deadline *% 0x9FB2_1C65_1E98_DF25);
}

fn restartCampaignStore(
    allocator: std.mem.Allocator,
    comptime Store: type,
    store: *Store,
) !void {
    const image = try store.encodeAlloc(allocator);
    defer allocator.free(image);
    store.* = try Store.decode(image, testDestination());
}

fn runDispatcherCampaign(allocator: std.mem.Allocator, seed: u64) !CampaignResult {
    const Store = outbox.SettlementOutbox(2);
    const Driver = Dispatcher(Store);
    const chain = testChain(800, 8000, 3);
    var store = try Store.init(testDestination());
    _ = try store.enqueue(testRecord(chain));
    var driver = try Driver.init(&store, testPolicy());
    var remote: FakeRemote = .{};
    var result: CampaignResult = .{};
    var tick: u64 = 0;
    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    while (store.len() != 0 and result.steps < 256) : (result.steps += 1) {
        const claim_result = try driver.claim(.fromInt(tick));
        switch (claim_result) {
            .blocked => |state| switch (state) {
                .waiting_submit => |waiting| {
                    mixCampaignWait(&result, 32, tick, waiting.retry.tick);
                    tick = waiting.retry.tick;
                },
                .waiting_reconciliation => |waiting| {
                    mixCampaignWait(&result, 33, tick, waiting.retry.tick);
                    tick = waiting.retry.tick;
                },
                .drained => {},
                .awaiting_commit, .recovery_required => return error.UnexpectedPublicationBlock,
                .in_flight => return error.UnexpectedInFlightBlock,
                .quarantined => return error.UnexpectedCampaignQuarantine,
            },
            .submit => |claim_value| {
                if (remote.chain != null or remote.reconciliation_required) {
                    return error.BlindResubmitBeforeReconciliation;
                }
                const action = random.uintLessThan(u8, 6);
                result.coverage |= @as(u16, 1) << @intCast(action);
                mixCampaign(&result, action, tick, claim_value.token, remote.chain != null);
                switch (action) {
                    0 => {
                        remote.reconciliation_required = true;
                        try restartCampaignStore(allocator, Store, &store);
                        driver = try Driver.init(&store, testPolicy());
                        result.restarts += 1;
                    },
                    1 => {
                        try remote.recordKnownNotSent();
                        _ = try driver.completeSubmit(claim_value, .known_not_sent, .fromInt(tick));
                    },
                    2 => {
                        try remote.recordKnownNotSent();
                        remote.reconciliation_required = true;
                        _ = try driver.completeSubmit(claim_value, .ambiguous, .fromInt(tick));
                    },
                    3 => {
                        try remote.apply(claim_value.chain.*);
                        remote.reconciliation_required = true;
                        _ = try driver.completeSubmit(claim_value, .ambiguous, .fromInt(tick));
                    },
                    4 => {
                        try remote.apply(claim_value.chain.*);
                        remote.reconciliation_required = true;
                        try restartCampaignStore(allocator, Store, &store);
                        driver = try Driver.init(&store, testPolicy());
                        result.restarts += 1;
                    },
                    5 => {
                        try remote.apply(claim_value.chain.*);
                        _ = try driver.completeSubmit(claim_value, testCommit(claim_value), .fromInt(tick));
                    },
                    else => unreachable,
                }
            },
            .reconcile => |claim_value| {
                const action = random.uintLessThan(u8, 4);
                mixCampaign(&result, 16 + action, tick, claim_value.token, remote.chain != null);
                switch (action) {
                    0 => {
                        try restartCampaignStore(allocator, Store, &store);
                        driver = try Driver.init(&store, testPolicy());
                        result.restarts += 1;
                        result.coverage |= @as(u16, 1) << 6;
                    },
                    1 => {
                        remote.lookups += 1;
                        _ = try driver.completeLookup(claim_value, .retryable, .fromInt(tick));
                        result.coverage |= @as(u16, 1) << 7;
                    },
                    2, 3 => {
                        const nonempty_reverse = action == 3 and remote.chain != null;
                        const observed = try remote.lookup(action == 3);
                        _ = try driver.completeLookup(
                            claim_value,
                            testLookup(claim_value, observed),
                            .fromInt(tick),
                        );
                        if (action == 2) {
                            result.coverage |= @as(u16, 1) << 8;
                        } else if (nonempty_reverse) {
                            result.coverage |= @as(u16, 1) << 9;
                        }
                    },
                    else => unreachable,
                }
            },
        }
        tick +|= 1;
    }

    // A deterministic fault-free suffix proves eventual convergence without
    // weakening the random prefix's crash and ambiguity coverage.
    var suffix_steps: u8 = 0;
    while (store.len() != 0 and suffix_steps < 16) : (suffix_steps += 1) {
        switch (try driver.claim(.fromInt(tick))) {
            .blocked => |state| switch (state) {
                .waiting_submit => |waiting| {
                    mixCampaignWait(&result, 40, tick, waiting.retry.tick);
                    tick = waiting.retry.tick;
                },
                .waiting_reconciliation => |waiting| {
                    mixCampaignWait(&result, 41, tick, waiting.retry.tick);
                    tick = waiting.retry.tick;
                },
                .drained => {},
                .awaiting_commit,
                .recovery_required,
                .in_flight,
                .quarantined,
                => return error.CampaignDidNotConverge,
            },
            .submit => |claim_value| {
                mixCampaign(&result, 42, tick, claim_value.token, remote.chain != null);
                try remote.apply(claim_value.chain.*);
                _ = try driver.completeSubmit(claim_value, testCommit(claim_value), .fromInt(tick));
            },
            .reconcile => |claim_value| {
                mixCampaign(&result, 43, tick, claim_value.token, remote.chain != null);
                const observed = try remote.lookup(false);
                _ = try driver.completeLookup(
                    claim_value,
                    testLookup(claim_value, observed),
                    .fromInt(tick),
                );
            },
        }
        tick +|= 1;
    }
    if (store.len() != 0 or remote.chain == null) return error.CampaignDidNotConverge;
    if (remote.reconciliation_required) return error.CampaignInvariantViolation;
    const exact = classifyLookup(chain, try remote.inspect(false));
    if (exact != .all_present_exact or remote.economic_effects != 1) {
        return error.CampaignInvariantViolation;
    }
    result.submits = remote.submits;
    result.lookups = remote.lookups;
    result.economic_effects = remote.economic_effects;
    result.suffix_steps = suffix_steps;
    result.final_tick = tick;
    result.fingerprint ^= @as(u64, result.submits) << 32;
    result.fingerprint ^= @as(u64, result.lookups) << 16;
    result.fingerprint ^= @as(u64, result.restarts) << 1;
    result.fingerprint ^= result.economic_effects;
    result.fingerprint ^= @as(u64, result.suffix_steps) << 56;
    result.fingerprint ^= result.final_tick *% 0xDB4F_0B91_75AE_2165;
    return result;
}

test "dispatcher seeded fake-ledger campaign is replayable and applies once" {
    const first = try runDispatcherCampaign(std.testing.allocator, 0xD15A_7C4E);
    const replay = try runDispatcherCampaign(std.testing.allocator, 0xD15A_7C4E);
    const different = try runDispatcherCampaign(std.testing.allocator, 0xD15A_7C4F);
    try std.testing.expectEqualDeep(first, replay);
    try std.testing.expect(first.restarts > 0);
    try std.testing.expect(first.lookups > 0);
    try std.testing.expectEqual(@as(u32, 1), first.economic_effects);
    try std.testing.expect(first.fingerprint != different.fingerprint);

    var coverage: u16 = 0;
    for (0..32) |offset| {
        const campaign = try runDispatcherCampaign(
            std.testing.allocator,
            0xD15A_7C50 + offset,
        );
        coverage |= campaign.coverage;
    }
    // Every submit fault and every lookup fault/ordering branch appeared in
    // the deterministic campaign set.
    try std.testing.expectEqual(@as(u16, 0x03ff), coverage & 0x03ff);
}
