const std = @import("std");

pub const SnapshotRef = enum(u32) { _ };
pub const JournalRef = enum(u32) { _ };

const RefCastError = error{
    IndexOverflow,
};

pub const CreateSnapshotError = std.mem.Allocator.Error || RefCastError || error{
    InvalidEngineCounts,
    SnapshotSeqNotMonotonic,
};

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

    pub fn createEmptySnapshot(
        self: *DescriptorStore,
        num_matching_engines: u32,
        num_risk_engines: u32,
    ) CreateSnapshotError!SnapshotRef {
        if (num_matching_engines == 0 or num_risk_engines == 0) {
            return error.InvalidEngineCounts;
        }

        try self.snapshots.append(self.allocator, .{
            .snapshot_id = 0,
            .seq = 0,
            .timestamp_ns = 0,
            .prev = null,
            .num_matching_engines = num_matching_engines,
            .num_risk_engines = num_risk_engines,
        });
        return snapshotRefFromIndex(self.snapshots.items.len - 1);
    }

    pub fn createNextSnapshot(
        self: *DescriptorStore,
        prev_ref: SnapshotRef,
        snapshot_id: u64,
        seq: u64,
        timestamp_ns: u64,
    ) CreateSnapshotError!SnapshotRef {
        const prev_idx = snapshotIndex(prev_ref);
        const prev_snapshot = self.snapshotConst(prev_ref);
        if (seq < prev_snapshot.seq) return error.SnapshotSeqNotMonotonic;

        const num_matching_engines = prev_snapshot.num_matching_engines;
        const num_risk_engines = prev_snapshot.num_risk_engines;

        try self.snapshots.append(self.allocator, .{
            .snapshot_id = snapshot_id,
            .seq = seq,
            .timestamp_ns = timestamp_ns,
            .prev = prev_ref,
            .num_matching_engines = num_matching_engines,
            .num_risk_engines = num_risk_engines,
        });

        const next_ref = try snapshotRefFromIndex(self.snapshots.items.len - 1);
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

        try self.journals.append(self.allocator, .{
            .timestamp_ns = timestamp_ns,
            .seq_first = seq_first,
            .base_snapshot = base_snapshot_ref,
            .prev = prev_ref,
            .next = next_ref,
        });
        const journal_ref = try journalRefFromIndex(self.journals.items.len - 1);

        if (prev_ref) |prev_journal_ref| {
            self.journal(prev_journal_ref).next = journal_ref;
        }
        if (next_ref) |next_journal_ref| {
            self.journal(next_journal_ref).prev = journal_ref;
        }

        try snapshot_desc.journals.insert(self.allocator, insert_idx, journal_ref);
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

    const snapshot = store.snapshotConst(snapshot_ref);
    try std.testing.expectEqual(@as(usize, 3), snapshot.journals.items.len);
    try std.testing.expectEqual(j100, snapshot.journals.items[0]);
    try std.testing.expectEqual(j200, snapshot.journals.items[1]);
    try std.testing.expectEqual(j300, snapshot.journals.items[2]);

    const first = store.journalConst(j100);
    const middle = store.journalConst(j200);
    const last = store.journalConst(j300);

    try std.testing.expectEqual(@as(?JournalRef, null), first.prev);
    try std.testing.expectEqual(j200, first.next.?);
    try std.testing.expectEqual(j100, middle.prev.?);
    try std.testing.expectEqual(j300, middle.next.?);
    try std.testing.expectEqual(j200, last.prev.?);
    try std.testing.expectEqual(@as(?JournalRef, null), last.next);
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
