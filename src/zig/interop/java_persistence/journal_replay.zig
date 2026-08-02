const std = @import("std");
const exchange_core = @import("exchange_core");
const cmd = exchange_core.core.cmd;
const java_journal_wire = @import("journal_wire.zig");
const journal = exchange_core.core.journal;
const kernel_mod = exchange_core.core.exchange_kernel;
const publication = exchange_core.core.publication_sequence;

pub const ReplayError = kernel_mod.ExecuteError || error{
    InvalidJavaSequence,
    NonMonotonicJavaSequence,
    SequenceNotAfterTarget,
    NonReplayablePersistenceRecord,
    SequenceMismatch,
};

pub const ReplaySummary = struct {
    replayed: usize,
    first_sequence: ?u64,
    last_sequence: ?u64,
    sequence_gap_count: usize,
    non_success_result_count: usize,
};

/// Replays an already decoded Java journal into an unpublished staged kernel.
///
/// The preflight validates every entry before the first mutation, including the
/// upstream persistence-record omission. Execution failures can still leave
/// `target` partially mutated, so callers must discard the staged kernel on any
/// error and publish/swap it only after this function succeeds.
pub fn replayIntoStagedKernel(
    decoded: *const java_journal_wire.DecodedJournal,
    target: *kernel_mod.ExchangeKernel,
) ReplayError!ReplaySummary {
    try preflight(decoded.entries.items, target.next_seq);

    var summary = emptySummary(countGaps(decoded.entries.items, target.next_seq));
    for (decoded.entries.items) |entry| {
        const sequence: u64 = @intCast(entry.java_sequence);
        var command = entry.command;
        var response = try target.executeAtSequence(
            publication.PublicationSequence.fromInt(sequence),
            &command,
        );
        defer response.deinit(target.allocator);
        if (response.sequence != sequence) return error.SequenceMismatch;
        observeResult(&summary, sequence, response.result_code);
    }
    return summary;
}

/// Sharded counterpart to `replayIntoStagedKernel`. Java journals do not
/// encode Zig's routed/split execution policy, so the owning configuration must
/// supply the mode that is already bound by the snapshot/config digest.
pub fn replayIntoStagedKernelSet(
    decoded: *const java_journal_wire.DecodedJournal,
    target: *kernel_mod.MultiShardKernelSet,
    mode: journal.ExecutionMode,
) ReplayError!ReplaySummary {
    try preflight(decoded.entries.items, target.next_seq);

    var summary = emptySummary(countGaps(decoded.entries.items, target.next_seq));
    for (decoded.entries.items) |entry| {
        const sequence: u64 = @intCast(entry.java_sequence);
        var command = entry.command;
        var response = switch (mode) {
            .routed => try target.executeRoutedAtSequence(
                publication.PublicationSequence.fromInt(sequence),
                &command,
            ),
            .split => try target.executeSplitAtSequence(
                publication.PublicationSequence.fromInt(sequence),
                &command,
            ),
        };
        defer response.deinit(target.allocator);
        if (response.sequence != sequence) return error.SequenceMismatch;
        observeResult(&summary, sequence, response.result_code);
    }
    return summary;
}

fn preflight(entries: []const java_journal_wire.Entry, target_sequence: u64) ReplayError!void {
    var previous = target_sequence;
    for (entries, 0..) |entry, index| {
        if (!entry.isReplayable()) return error.NonReplayablePersistenceRecord;
        if (entry.java_sequence <= 0) return error.InvalidJavaSequence;
        const sequence = std.math.cast(u64, entry.java_sequence) orelse
            return error.InvalidJavaSequence;
        if (index == 0 and sequence <= target_sequence) return error.SequenceNotAfterTarget;
        if (index != 0 and sequence <= previous) return error.NonMonotonicJavaSequence;
        previous = sequence;
    }
}

fn countGaps(entries: []const java_journal_wire.Entry, target_sequence: u64) usize {
    var previous = target_sequence;
    var count: usize = 0;
    for (entries) |entry| {
        const sequence: u64 = @intCast(entry.java_sequence);
        if (sequence > previous + 1) count += 1;
        previous = sequence;
    }
    return count;
}

fn emptySummary(sequence_gap_count: usize) ReplaySummary {
    return .{
        .replayed = 0,
        .first_sequence = null,
        .last_sequence = null,
        .sequence_gap_count = sequence_gap_count,
        .non_success_result_count = 0,
    };
}

fn observeResult(
    summary: *ReplaySummary,
    sequence: u64,
    result_code: cmd.CommandResultCode,
) void {
    if (summary.first_sequence == null) summary.first_sequence = sequence;
    summary.last_sequence = sequence;
    summary.replayed += 1;
    switch (result_code) {
        .success, .accepted => {},
        else => summary.non_success_result_count += 1,
    }
}

fn makeDecoded(
    allocator: std.mem.Allocator,
    entries: []const java_journal_wire.Entry,
    gaps: []const java_journal_wire.SequenceGap,
) !java_journal_wire.DecodedJournal {
    var decoded: java_journal_wire.DecodedJournal = .{
        .allocator = allocator,
        .last_sequence = if (entries.len == 0) 0 else entries[entries.len - 1].java_sequence,
    };
    errdefer decoded.deinit();
    try decoded.entries.appendSlice(allocator, entries);
    try decoded.sequence_gaps.appendSlice(allocator, gaps);
    return decoded;
}

test "Java journal replay preserves exact gapped sequence and command metadata" {
    const allocator = std.testing.allocator;
    var decoded = try makeDecoded(
        allocator,
        &.{
            .{
                .java_sequence = 2,
                .command = .{
                    .command = .add_user,
                    .uid = 101,
                    .order_id = -1,
                    .symbol = -1,
                    .timestamp = 20,
                    .events_group = 30,
                    .service_flags = 40,
                },
            },
            .{
                .java_sequence = 4,
                .command = .{
                    .command = .balance_adjustment,
                    .uid = 101,
                    .symbol = 15,
                    .order_id = 501,
                    .price = 12_345,
                    .order_type = .gtc,
                    .timestamp = 21,
                    .events_group = 31,
                    .service_flags = 41,
                },
                .balance_adjustment_type = .adjustment,
            },
        },
        &.{
            .{ .previous_sequence = 0, .next_sequence = 2 },
            .{ .previous_sequence = 2, .next_sequence = 4 },
        },
    );
    defer decoded.deinit();

    var target = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer target.deinit();
    const summary = try replayIntoStagedKernel(&decoded, &target);

    try std.testing.expectEqual(@as(usize, 2), summary.replayed);
    try std.testing.expectEqual(@as(?u64, 2), summary.first_sequence);
    try std.testing.expectEqual(@as(?u64, 4), summary.last_sequence);
    try std.testing.expectEqual(@as(usize, 2), summary.sequence_gap_count);
    try std.testing.expectEqual(@as(usize, 0), summary.non_success_result_count);
    try std.testing.expectEqual(@as(u64, 4), target.next_seq);
    const profile = target.risk_engine.getUserProfile(101) orelse
        return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(?i64, 12_345), profile.accounts.get(15));
}

test "Java journal preflight rejects omitted persistence ids before mutation" {
    const allocator = std.testing.allocator;
    var decoded = try makeDecoded(
        allocator,
        &.{
            .{
                .java_sequence = 1,
                .command = .{ .command = .add_user, .uid = 201 },
            },
            .{
                .java_sequence = 2,
                .command = .{ .command = .persist_state_matching },
                .persistence_record = .matching_snapshot_id_not_encoded,
            },
        },
        &.{},
    );
    defer decoded.deinit();

    var target = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer target.deinit();
    try std.testing.expectError(
        error.NonReplayablePersistenceRecord,
        replayIntoStagedKernel(&decoded, &target),
    );
    try std.testing.expectEqual(@as(u64, 0), target.next_seq);
    try std.testing.expectEqual(@as(usize, 0), target.risk_engine.user_profile_service.user_profiles.count());
}

test "Java journal replay validates the target frontier before mutation" {
    const allocator = std.testing.allocator;
    var decoded = try makeDecoded(
        allocator,
        &.{.{
            .java_sequence = 5,
            .command = .{ .command = .add_user, .uid = 301 },
        }},
        &.{},
    );
    defer decoded.deinit();

    var target = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer target.deinit();
    target.next_seq = 5;
    try std.testing.expectError(
        error.SequenceNotAfterTarget,
        replayIntoStagedKernel(&decoded, &target),
    );
    try std.testing.expectEqual(@as(usize, 0), target.risk_engine.user_profile_service.user_profiles.count());
}

test "Java journal replay applies the configured sharded execution mode" {
    const allocator = std.testing.allocator;
    var decoded = try makeDecoded(
        allocator,
        &.{
            .{
                .java_sequence = 1,
                .command = .{ .command = .add_user, .uid = 401 },
            },
            .{
                .java_sequence = 2,
                .command = .{
                    .command = .balance_adjustment,
                    .uid = 401,
                    .symbol = 11,
                    .order_id = 601,
                    .price = 700,
                    .order_type = .gtc,
                },
                .balance_adjustment_type = .adjustment,
            },
        },
        &.{},
    );
    defer decoded.deinit();

    var target = try kernel_mod.MultiShardKernelSet.initWithTopology(allocator, 4, 2, .{});
    defer target.deinit();
    const summary = try replayIntoStagedKernelSet(&decoded, &target, .split);
    try std.testing.expectEqual(@as(usize, 2), summary.replayed);
    try std.testing.expectEqual(@as(u64, 2), target.next_seq);
    const profile = target.riskEngineForUid(401).getUserProfile(401) orelse
        return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(?i64, 700), profile.accounts.get(11));
}
