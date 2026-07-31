const std = @import("std");

pub const SnapshotRef = enum(u32) { _ };
pub const JournalRef = enum(u32) { _ };

const RefCastError = error{
    IndexOverflow,
};

pub const ValidateNextSnapshotError = error{
    SnapshotSeqNotMonotonic,
};

pub const CreateSnapshotError = std.mem.Allocator.Error || RefCastError || ValidateNextSnapshotError || error{
    InvalidEngineCounts,
};

pub const ReserveSnapshotError = std.mem.Allocator.Error || RefCastError;

pub const CreateJournalError = std.mem.Allocator.Error || RefCastError || error{
    DuplicateJournalStartSequence,
};

pub const FinishJournalError = error{
    InvalidJournalRange,
};

pub const SnapshotDescriptor = struct {
    snapshot_id: u64,
    seq: u64,
    timestamp_ns: u64,
    prev: ?SnapshotRef,
    next: ?SnapshotRef = null,
    num_matching_engines: u32,
    num_risk_engines: u32,
    journals: std.ArrayListUnmanaged(JournalRef) = .empty,

    fn deinit(self: *SnapshotDescriptor, allocator: std.mem.Allocator) void {
        self.journals.deinit(allocator);
        self.* = undefined;
    }
};

/// Pointer-free observation of a snapshot descriptor. The backing descriptor
/// store can reallocate without invalidating previously returned views.
pub const SnapshotDescriptorView = struct {
    snapshot_id: u64,
    seq: u64,
    timestamp_ns: u64,
    prev: ?SnapshotRef,
    next: ?SnapshotRef,
    num_matching_engines: u32,
    num_risk_engines: u32,
    journal_count: usize,
};

pub const JournalDescriptor = struct {
    timestamp_ns: u64,
    seq_first: u64,
    seq_last: ?u64 = null,
    base_snapshot: SnapshotRef,
    prev: ?JournalRef = null,
    next: ?JournalRef = null,
};

pub const DescriptorStore = struct {
    allocator: std.mem.Allocator,
    snapshots: std.ArrayListUnmanaged(SnapshotDescriptor) = .empty,
    journals: std.ArrayListUnmanaged(JournalDescriptor) = .empty,

    pub fn init(allocator: std.mem.Allocator) DescriptorStore {
        return .{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *DescriptorStore) void {
        for (self.snapshots.items) |*snapshot_desc| {
            snapshot_desc.deinit(self.allocator);
        }
        self.snapshots.deinit(self.allocator);
        self.journals.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn snapshotCount(self: *const DescriptorStore) usize {
        return self.snapshots.items.len;
    }

    pub fn journalCount(self: *const DescriptorStore) usize {
        return self.journals.items.len;
    }

    /// Reserve the descriptor slot before an external durable commit point.
    /// Once this succeeds, the immediately following
    /// createNextSnapshotAssumeReserved call cannot fail from allocation or
    /// index-width exhaustion.
    pub fn reserveNextSnapshot(self: *DescriptorStore) ReserveSnapshotError!void {
        _ = try snapshotRefFromIndex(self.snapshots.items.len);
        try self.snapshots.ensureUnusedCapacity(self.allocator, 1);
    }

    pub fn createEmptySnapshot(
        self: *DescriptorStore,
        num_matching_engines: u32,
        num_risk_engines: u32,
    ) CreateSnapshotError!SnapshotRef {
        if (num_matching_engines == 0 or num_risk_engines == 0) {
            return error.InvalidEngineCounts;
        }

        const snapshot_ref = try snapshotRefFromIndex(self.snapshots.items.len);
        try self.snapshots.append(self.allocator, .{
            .snapshot_id = 0,
            .seq = 0,
            .timestamp_ns = 0,
            .prev = null,
            .num_matching_engines = num_matching_engines,
            .num_risk_engines = num_risk_engines,
        });
        return snapshot_ref;
    }

    pub fn createNextSnapshot(
        self: *DescriptorStore,
        prev_ref: SnapshotRef,
        snapshot_id: u64,
        seq: u64,
        timestamp_ns: u64,
    ) CreateSnapshotError!SnapshotRef {
        try self.validateNextSnapshotSequence(prev_ref, seq);
        try self.reserveNextSnapshot();
        return self.createNextSnapshotAssumeReserved(
            prev_ref,
            snapshot_id,
            seq,
            timestamp_ns,
        );
    }

    pub fn validateNextSnapshotSequence(
        self: *const DescriptorStore,
        prev_ref: SnapshotRef,
        seq: u64,
    ) ValidateNextSnapshotError!void {
        if (seq < self.snapshotConst(prev_ref).seq) {
            return error.SnapshotSeqNotMonotonic;
        }
    }

    /// Stores a descriptor after `reserveNextSnapshot` succeeds. This is the
    /// post-durable-commit path: it performs no allocation and cannot fail due
    /// to reference-width exhaustion.
    pub fn createNextSnapshotAssumeReserved(
        self: *DescriptorStore,
        prev_ref: SnapshotRef,
        snapshot_id: u64,
        seq: u64,
        timestamp_ns: u64,
    ) SnapshotRef {
        const prev_idx = snapshotIndex(prev_ref);
        const prev_snapshot = self.snapshotConst(prev_ref);
        std.debug.assert(seq >= prev_snapshot.seq);
        std.debug.assert(self.snapshots.items.len < self.snapshots.capacity);

        const num_matching_engines = prev_snapshot.num_matching_engines;
        const num_risk_engines = prev_snapshot.num_risk_engines;
        const next_ref = snapshotRefFromIndex(self.snapshots.items.len) catch unreachable;

        self.snapshots.appendAssumeCapacity(.{
            .snapshot_id = snapshot_id,
            .seq = seq,
            .timestamp_ns = timestamp_ns,
            .prev = prev_ref,
            .num_matching_engines = num_matching_engines,
            .num_risk_engines = num_risk_engines,
        });

        self.snapshots.items[prev_idx].next = next_ref;
        return next_ref;
    }

    pub fn createJournal(
        self: *DescriptorStore,
        base_snapshot_ref: SnapshotRef,
        timestamp_ns: u64,
        seq_first: u64,
    ) CreateJournalError!JournalRef {
        var snapshot_desc = self.snapshot(base_snapshot_ref);
        var insert_idx: usize = snapshot_desc.journals.items.len;
        var prev_ref: ?JournalRef = null;
        var next_ref: ?JournalRef = null;

        for (snapshot_desc.journals.items, 0..) |existing_ref, idx| {
            const existing = self.journalConst(existing_ref);
            if (existing.seq_first == seq_first) return error.DuplicateJournalStartSequence;

            if (seq_first < existing.seq_first) {
                insert_idx = idx;
                next_ref = existing_ref;
                if (idx > 0) {
                    prev_ref = snapshot_desc.journals.items[idx - 1];
                }
                break;
            }
            prev_ref = existing_ref;
        }

        // Preflight every fallible operation before publishing the descriptor
        // or changing neighbor links. Retrying after either reservation fails
        // therefore observes the same descriptor graph.
        const journal_ref = try journalRefFromIndex(self.journals.items.len);
        try self.journals.ensureUnusedCapacity(self.allocator, 1);
        try snapshot_desc.journals.ensureUnusedCapacity(self.allocator, 1);

        self.journals.appendAssumeCapacity(.{
            .timestamp_ns = timestamp_ns,
            .seq_first = seq_first,
            .base_snapshot = base_snapshot_ref,
            .prev = prev_ref,
            .next = next_ref,
        });
        snapshot_desc.journals.insertAssumeCapacity(insert_idx, journal_ref);

        if (prev_ref) |prev_journal_ref| {
            self.journal(prev_journal_ref).next = journal_ref;
        }
        if (next_ref) |next_journal_ref| {
            self.journal(next_journal_ref).prev = journal_ref;
        }
        return journal_ref;
    }

    pub fn finishJournal(
        self: *DescriptorStore,
        journal_ref: JournalRef,
        seq_last: u64,
    ) FinishJournalError!void {
        const journal_desc = self.journal(journal_ref);
        if (seq_last < journal_desc.seq_first) return error.InvalidJournalRange;
        journal_desc.seq_last = seq_last;
    }

    pub fn findJournalForSequence(
        self: *const DescriptorStore,
        snapshot_ref: SnapshotRef,
        seq: u64,
    ) ?JournalRef {
        const snapshot_desc = self.snapshotConst(snapshot_ref);
        var candidate: ?JournalRef = null;

        for (snapshot_desc.journals.items) |journal_ref| {
            const journal_desc = self.journalConst(journal_ref);
            if (seq < journal_desc.seq_first) break;
            candidate = journal_ref;
        }

        if (candidate) |journal_ref| {
            const journal_desc = self.journalConst(journal_ref);
            if (journal_desc.seq_last) |seq_last| {
                if (seq > seq_last) return null;
            }
            return journal_ref;
        }
        return null;
    }

    pub fn findSnapshotById(
        self: *const DescriptorStore,
        snapshot_id: u64,
    ) ?SnapshotRef {
        var idx = self.snapshots.items.len;
        while (idx > 0) {
            idx -= 1;
            if (self.snapshots.items[idx].snapshot_id == snapshot_id) {
                const raw = std.math.cast(u32, idx) orelse return null;
                return @enumFromInt(raw);
            }
        }
        return null;
    }

    pub fn snapshot(self: *DescriptorStore, snapshot_ref: SnapshotRef) *SnapshotDescriptor {
        const idx = snapshotIndex(snapshot_ref);
        std.debug.assert(idx < self.snapshots.items.len);
        return &self.snapshots.items[idx];
    }

    pub fn snapshotConst(self: *const DescriptorStore, snapshot_ref: SnapshotRef) *const SnapshotDescriptor {
        const idx = snapshotIndex(snapshot_ref);
        std.debug.assert(idx < self.snapshots.items.len);
        return &self.snapshots.items[idx];
    }

    pub fn snapshotView(self: *const DescriptorStore, snapshot_ref: SnapshotRef) SnapshotDescriptorView {
        const descriptor = self.snapshotConst(snapshot_ref);
        return .{
            .snapshot_id = descriptor.snapshot_id,
            .seq = descriptor.seq,
            .timestamp_ns = descriptor.timestamp_ns,
            .prev = descriptor.prev,
            .next = descriptor.next,
            .num_matching_engines = descriptor.num_matching_engines,
            .num_risk_engines = descriptor.num_risk_engines,
            .journal_count = descriptor.journals.items.len,
        };
    }

    pub fn journal(self: *DescriptorStore, journal_ref: JournalRef) *JournalDescriptor {
        const idx = journalIndex(journal_ref);
        std.debug.assert(idx < self.journals.items.len);
        return &self.journals.items[idx];
    }

    pub fn journalConst(self: *const DescriptorStore, journal_ref: JournalRef) *const JournalDescriptor {
        const idx = journalIndex(journal_ref);
        std.debug.assert(idx < self.journals.items.len);
        return &self.journals.items[idx];
    }

    pub fn journalValue(self: *const DescriptorStore, journal_ref: JournalRef) JournalDescriptor {
        return self.journalConst(journal_ref).*;
    }
};

fn snapshotRefFromIndex(idx: usize) RefCastError!SnapshotRef {
    const raw = std.math.cast(u32, idx) orelse return error.IndexOverflow;
    return @enumFromInt(raw);
}

fn snapshotIndex(snapshot_ref: SnapshotRef) usize {
    return @intFromEnum(snapshot_ref);
}

fn journalRefFromIndex(idx: usize) RefCastError!JournalRef {
    const raw = std.math.cast(u32, idx) orelse return error.IndexOverflow;
    return @enumFromInt(raw);
}

fn journalIndex(journal_ref: JournalRef) usize {
    return @intFromEnum(journal_ref);
}

test "snapshot descriptor createEmpty and createNext maintain linkage" {
    var store = DescriptorStore.init(std.testing.allocator);
    defer store.deinit();

    const root = try store.createEmptySnapshot(1, 1);
    try std.testing.expectEqual(@as(usize, 1), store.snapshotCount());

    const root_snapshot = store.snapshotConst(root);
    try std.testing.expectEqual(@as(u64, 0), root_snapshot.snapshot_id);
    try std.testing.expectEqual(@as(u64, 0), root_snapshot.seq);
    try std.testing.expectEqual(@as(u64, 0), root_snapshot.timestamp_ns);
    try std.testing.expectEqual(@as(?SnapshotRef, null), root_snapshot.prev);
    try std.testing.expectEqual(@as(?SnapshotRef, null), root_snapshot.next);

    const next = try store.createNextSnapshot(root, 77, 100, 1_000_000);
    try std.testing.expectEqual(@as(usize, 2), store.snapshotCount());

    try std.testing.expectEqual(next, store.snapshotConst(root).next.?);
    const next_snapshot = store.snapshotConst(next);
    try std.testing.expectEqual(root, next_snapshot.prev.?);
    try std.testing.expectEqual(@as(u64, 77), next_snapshot.snapshot_id);
    try std.testing.expectEqual(@as(u64, 100), next_snapshot.seq);
    try std.testing.expectEqual(@as(u64, 1_000_000), next_snapshot.timestamp_ns);
    try std.testing.expectEqual(@as(u32, 1), next_snapshot.num_matching_engines);
    try std.testing.expectEqual(@as(u32, 1), next_snapshot.num_risk_engines);
}

test "empty snapshot allocation failure leaves store unchanged and retry succeeds" {
    var failing_allocator_state = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = failing_allocator_state.allocator();
    var store = DescriptorStore.init(allocator);
    defer {
        failing_allocator_state.fail_index = std.math.maxInt(usize);
        store.deinit();
    }

    failing_allocator_state.fail_index = failing_allocator_state.alloc_index;
    try std.testing.expectError(error.OutOfMemory, store.createEmptySnapshot(1, 1));
    try std.testing.expect(failing_allocator_state.has_induced_failure);
    try std.testing.expectEqual(@as(usize, 0), store.snapshotCount());

    failing_allocator_state.fail_index = std.math.maxInt(usize);
    const root = try store.createEmptySnapshot(1, 1);
    try std.testing.expectEqual(@as(SnapshotRef, @enumFromInt(0)), root);
    try std.testing.expectEqual(@as(usize, 1), store.snapshotCount());
}

test "next snapshot allocation failure leaves linkage unchanged and retry succeeds" {
    var failing_allocator_state = std.testing.FailingAllocator.init(std.testing.allocator, .{
        .resize_fail_index = 0,
    });
    const allocator = failing_allocator_state.allocator();
    var store = DescriptorStore.init(allocator);
    defer {
        failing_allocator_state.fail_index = std.math.maxInt(usize);
        store.deinit();
    }

    const root = try store.createEmptySnapshot(1, 1);
    try store.snapshots.shrinkToLen(allocator);
    try std.testing.expectEqual(store.snapshots.items.len, store.snapshots.capacity);
    const root_before = store.snapshotConst(root).*;

    failing_allocator_state.fail_index = failing_allocator_state.alloc_index;
    try std.testing.expectError(
        error.OutOfMemory,
        store.createNextSnapshot(root, 77, 2, 1_000),
    );
    try std.testing.expect(failing_allocator_state.has_induced_failure);
    try std.testing.expectEqual(@as(usize, 1), store.snapshotCount());
    try std.testing.expectEqual(root_before, store.snapshotConst(root).*);

    failing_allocator_state.fail_index = std.math.maxInt(usize);
    const next = try store.createNextSnapshot(root, 77, 2, 1_000);
    try std.testing.expectEqual(@as(usize, 2), store.snapshotCount());
    try std.testing.expectEqual(next, store.snapshotConst(root).next.?);
    try std.testing.expectEqual(root, store.snapshotConst(next).prev.?);
}

test "snapshot descriptor rejects non-monotonic sequence" {
    var store = DescriptorStore.init(std.testing.allocator);
    defer store.deinit();

    const root = try store.createEmptySnapshot(2, 2);
    const first = try store.createNextSnapshot(root, 1, 5, 10);
    try std.testing.expectError(
        error.SnapshotSeqNotMonotonic,
        store.createNextSnapshot(first, 2, 4, 20),
    );
}

test "reserved snapshot descriptor store is allocation-free after durable commit" {
    var failing_allocator_state = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = failing_allocator_state.allocator();
    var store = DescriptorStore.init(allocator);
    defer {
        failing_allocator_state.fail_index = std.math.maxInt(usize);
        store.deinit();
    }

    const root = try store.createEmptySnapshot(1, 1);
    try store.validateNextSnapshotSequence(root, 2);
    try store.reserveNextSnapshot();

    failing_allocator_state.fail_index = failing_allocator_state.alloc_index;
    const next = store.createNextSnapshotAssumeReserved(root, 77, 2, 1_000);
    try std.testing.expect(!failing_allocator_state.has_induced_failure);
    try std.testing.expectEqual(@as(u64, 77), store.snapshotView(next).snapshot_id);
}

test "snapshot descriptor views remain stable across store reallocation" {
    var store = DescriptorStore.init(std.testing.allocator);
    defer store.deinit();

    var current = try store.createEmptySnapshot(1, 1);
    const root_view = store.snapshotView(current);
    for (1..257) |index| {
        current = try store.createNextSnapshot(
            current,
            @intCast(index),
            @intCast(index),
            @intCast(index * 10),
        );
    }

    try std.testing.expectEqual(@as(u64, 0), root_view.snapshot_id);
    try std.testing.expectEqual(@as(?SnapshotRef, null), root_view.prev);
    try std.testing.expectEqual(@as(?SnapshotRef, null), root_view.next);
    try std.testing.expectEqual(@as(usize, 0), root_view.journal_count);
    try std.testing.expectEqual(@as(u64, 0), store.snapshotView(@enumFromInt(0)).snapshot_id);
    try std.testing.expectEqual(@as(u64, 256), store.snapshotView(current).snapshot_id);
}

test "snapshot descriptor lookup by id returns ref or null" {
    var store = DescriptorStore.init(std.testing.allocator);
    defer store.deinit();

    const root = try store.createEmptySnapshot(1, 1);
    const first = try store.createNextSnapshot(root, 101, 50, 1_010);
    const second = try store.createNextSnapshot(first, 202, 100, 2_020);

    try std.testing.expectEqual(root, store.findSnapshotById(0).?);
    try std.testing.expectEqual(first, store.findSnapshotById(101).?);
    try std.testing.expectEqual(second, store.findSnapshotById(202).?);
    try std.testing.expectEqual(@as(?SnapshotRef, null), store.findSnapshotById(999));
}

test "journal descriptor insertion is ordered and links prev/next" {
    var store = DescriptorStore.init(std.testing.allocator);
    defer store.deinit();

    const snapshot_ref = try store.createEmptySnapshot(1, 1);
    const j200 = try store.createJournal(snapshot_ref, 2_000, 200);
    const j100 = try store.createJournal(snapshot_ref, 1_000, 100);
    const j300 = try store.createJournal(snapshot_ref, 3_000, 300);
    const j250 = try store.createJournal(snapshot_ref, 2_500, 250);

    const snapshot = store.snapshotConst(snapshot_ref);
    try std.testing.expectEqual(@as(usize, 4), snapshot.journals.items.len);
    try std.testing.expectEqual(j100, snapshot.journals.items[0]);
    try std.testing.expectEqual(j200, snapshot.journals.items[1]);
    try std.testing.expectEqual(j250, snapshot.journals.items[2]);
    try std.testing.expectEqual(j300, snapshot.journals.items[3]);

    const first = store.journalConst(j100);
    const middle_left = store.journalConst(j200);
    const middle_right = store.journalConst(j250);
    const last = store.journalConst(j300);

    try std.testing.expectEqual(@as(?JournalRef, null), first.prev);
    try std.testing.expectEqual(j200, first.next.?);
    try std.testing.expectEqual(j100, middle_left.prev.?);
    try std.testing.expectEqual(j250, middle_left.next.?);
    try std.testing.expectEqual(j200, middle_right.prev.?);
    try std.testing.expectEqual(j300, middle_right.next.?);
    try std.testing.expectEqual(j250, last.prev.?);
    try std.testing.expectEqual(@as(?JournalRef, null), last.next);
}

test "journal preflight allocation failures preserve graph and retry exactly once" {
    for ([_]usize{ 0, 1 }) |failure_offset| {
        var failing_allocator_state = std.testing.FailingAllocator.init(std.testing.allocator, .{
            .resize_fail_index = 0,
        });
        const allocator = failing_allocator_state.allocator();
        var store = DescriptorStore.init(allocator);
        defer {
            failing_allocator_state.fail_index = std.math.maxInt(usize);
            store.deinit();
        }

        const snapshot_ref = try store.createEmptySnapshot(1, 1);
        const j100 = try store.createJournal(snapshot_ref, 1_000, 100);
        const j300 = try store.createJournal(snapshot_ref, 3_000, 300);

        // Make both preflight reservations allocate. Offset zero fails the
        // global descriptor reservation; offset one fails the snapshot-local
        // reference reservation after the global reservation succeeds.
        try store.journals.shrinkToLen(allocator);
        try store.snapshot(snapshot_ref).journals.shrinkToLen(allocator);
        try std.testing.expectEqual(store.journals.items.len, store.journals.capacity);
        try std.testing.expectEqual(
            store.snapshotConst(snapshot_ref).journals.items.len,
            store.snapshotConst(snapshot_ref).journals.capacity,
        );

        const snapshot_before = store.snapshotConst(snapshot_ref).*;
        const journals_before = try std.testing.allocator.dupe(
            JournalDescriptor,
            store.journals.items,
        );
        defer std.testing.allocator.free(journals_before);
        const refs_before = try std.testing.allocator.dupe(
            JournalRef,
            store.snapshotConst(snapshot_ref).journals.items,
        );
        defer std.testing.allocator.free(refs_before);

        failing_allocator_state.fail_index =
            failing_allocator_state.alloc_index + failure_offset;
        try std.testing.expectError(
            error.OutOfMemory,
            store.createJournal(snapshot_ref, 2_000, 200),
        );
        try std.testing.expect(failing_allocator_state.has_induced_failure);

        try std.testing.expectEqual(@as(usize, 2), store.journalCount());
        try std.testing.expectEqual(snapshot_before, store.snapshotConst(snapshot_ref).*);
        try std.testing.expectEqualSlices(
            JournalDescriptor,
            journals_before,
            store.journals.items,
        );
        try std.testing.expectEqualSlices(
            JournalRef,
            refs_before,
            store.snapshotConst(snapshot_ref).journals.items,
        );
        try std.testing.expectEqual(@as(?JournalRef, null), store.journalConst(j100).prev);
        try std.testing.expectEqual(j300, store.journalConst(j100).next.?);
        try std.testing.expectEqual(j100, store.journalConst(j300).prev.?);
        try std.testing.expectEqual(@as(?JournalRef, null), store.journalConst(j300).next);

        failing_allocator_state.fail_index = std.math.maxInt(usize);
        const j200 = try store.createJournal(snapshot_ref, 2_000, 200);
        const refs = store.snapshotConst(snapshot_ref).journals.items;
        try std.testing.expectEqual(@as(usize, 3), store.journalCount());
        try std.testing.expectEqualSlices(JournalRef, &.{ j100, j200, j300 }, refs);
        try std.testing.expectEqual(j200, store.journalConst(j100).next.?);
        try std.testing.expectEqual(j100, store.journalConst(j200).prev.?);
        try std.testing.expectEqual(j300, store.journalConst(j200).next.?);
        try std.testing.expectEqual(j200, store.journalConst(j300).prev.?);

        try std.testing.expectError(
            error.DuplicateJournalStartSequence,
            store.createJournal(snapshot_ref, 9_999, 200),
        );
        try std.testing.expectEqual(@as(usize, 3), store.journalCount());
        try std.testing.expectEqualSlices(
            JournalRef,
            &.{ j100, j200, j300 },
            store.snapshotConst(snapshot_ref).journals.items,
        );
    }
}

test "journal descriptor finish and sequence lookup semantics" {
    var store = DescriptorStore.init(std.testing.allocator);
    defer store.deinit();

    const snapshot_ref = try store.createEmptySnapshot(1, 1);
    const first = try store.createJournal(snapshot_ref, 10, 100);
    const second = try store.createJournal(snapshot_ref, 20, 200);

    try store.finishJournal(first, 150);

    try std.testing.expectEqual(first, store.findJournalForSequence(snapshot_ref, 120).?);
    try std.testing.expectEqual(@as(?JournalRef, null), store.findJournalForSequence(snapshot_ref, 160));
    try std.testing.expectEqual(second, store.findJournalForSequence(snapshot_ref, 220).?);

    try store.finishJournal(second, 250);
    try std.testing.expectEqual(@as(?JournalRef, null), store.findJournalForSequence(snapshot_ref, 251));
}

test "journal descriptor rejects duplicate start sequence and invalid finish range" {
    var store = DescriptorStore.init(std.testing.allocator);
    defer store.deinit();

    const snapshot_ref = try store.createEmptySnapshot(1, 1);
    const first = try store.createJournal(snapshot_ref, 10, 100);

    try std.testing.expectError(
        error.DuplicateJournalStartSequence,
        store.createJournal(snapshot_ref, 99, 100),
    );
    try std.testing.expectError(error.InvalidJournalRange, store.finishJournal(first, 99));
}

test "descriptor references reject indices wider than their storage" {
    const overflow = std.math.cast(usize, @as(u64, std.math.maxInt(u32)) + 1) orelse return;
    try std.testing.expectError(error.IndexOverflow, snapshotRefFromIndex(overflow));
    try std.testing.expectError(error.IndexOverflow, journalRefFromIndex(overflow));
}
