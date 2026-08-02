const std = @import("std");
const builtin = @import("builtin");
const io_context = @import("io_context.zig");
const common = @import("common.zig");
const cmd = @import("cmd.zig");
const kernel_mod = @import("exchange_kernel.zig");
const publication = @import("publication_sequence.zig");
const settlement_lifecycle = @import("settlement_lifecycle.zig");
const snapshot_mod = @import("snapshot.zig");

/// Stored here rather than in the sharded API so journal decoding stays
/// cycle-free and can validate execution policy before replay mutates a core.
pub const ExecutionMode = enum(u8) {
    routed = 0,
    split = 1,

    pub fn fromCode(code: u8) error{InvalidExecutionMode}!ExecutionMode {
        return switch (code) {
            0 => .routed,
            1 => .split,
            else => error.InvalidExecutionMode,
        };
    }
};

pub const JournalEntry = struct {
    sequence: u64,
    mode: ExecutionMode = .routed,
    input: cmd.OrderCommand,
    result_code: cmd.CommandResultCode,
};

pub const ReplayError = kernel_mod.ExecuteError || error{
    SequenceMismatch,
    ResultMismatch,
    SnapshotCheckpointRequiresSnapshot,
    SnapshotCheckpointMismatch,
};

pub const ReplayWindow = struct {
    start_index: usize,
    replayed: usize,
};

pub const JournalIoError = std.mem.Allocator.Error ||
    std.Io.File.OpenError ||
    std.Io.File.SyncError ||
    std.Io.File.StatError ||
    std.Io.File.ReadPositionalError ||
    std.Io.File.SetLengthError ||
    std.Io.File.Writer.SeekError ||
    std.Io.Dir.CreateFileAtomicError ||
    std.Io.File.Atomic.LinkError ||
    std.Io.Reader.Error ||
    std.Io.Writer.Error ||
    io_context.ReadToEndAllocError ||
    cmd.OrderCommandType.FromCodeError ||
    cmd.CommandResultCode.FromCodeError ||
    common.OrderAction.FromCodeError ||
    common.OrderType.FromCodeError ||
    error{
        InvalidMagic,
        UnsupportedLegacyLz4Format,
        InvalidOptionalTag,
        InvalidExecutionMode,
        InvalidRecordLength,
        ChecksumMismatch,
        NonMonotonicSequence,
        JournalTooLarge,
        JournalChangedDuringRead,
        JournalEntryMismatch,
        AppenderPoisoned,
        SequenceGap,
        SnapshotCheckpointMismatch,
        InjectedJournalIoFailure,
    };

/// Native append format:
/// `XCJ3` followed by repeated little-endian `u32 body_len`, `u64 Wyhash`,
/// and either the exact XCJ2 entry bytes (`mode` plus the existing entry body)
/// or a frontier-only checkpoint. Checkpoints preserve publication identity
/// for commands that Java deliberately omits from its journal; they are never
/// replayed as commands.
const XCJ3_MAGIC = "XCJ3";
const XCJ3_RECORD_HEADER_BYTES: usize = @sizeOf(u32) + @sizeOf(u64);
const XCJ3_MIN_ENTRY_BYTES: u32 = 88;
const XCJ3_MAX_ENTRY_BYTES: u32 = 90;
const XCJ3_CHECKPOINT_TAG = "XCP1";
const XCJ3_CHECKPOINT_BYTES: u32 = XCJ3_CHECKPOINT_TAG.len + @sizeOf(u64) + @sizeOf(i64);
const XCJ3_SNAPSHOT_CHECKPOINT_TAG = "XSP1";
const XCJ3_SNAPSHOT_CHECKPOINT_BYTES: u32 = XCJ3_SNAPSHOT_CHECKPOINT_TAG.len + @sizeOf(u64) + @sizeOf(i64) + @sizeOf(u64);
const XCJ3_MAX_RECORD_BYTES: usize = XCJ3_RECORD_HEADER_BYTES + XCJ3_MAX_ENTRY_BYTES;
const XCJ3_CHECKSUM_SEED: u64 = 0x5843_4A33_4348_4B31;

/// Content identity of the checksum-validated XCJ3 prefix. A torn final
/// record is deliberately excluded so reopening/truncating the authoritative
/// appender and reading an otherwise identical copy produce the same value.
pub const DurableJournalFingerprint = struct {
    valid_len: u64,
    digest: [std.crypto.hash.Blake3.digest_length]u8,

    pub fn eql(a: DurableJournalFingerprint, b: DurableJournalFingerprint) bool {
        return a.valid_len == b.valid_len and std.mem.eql(u8, &a.digest, &b.digest);
    }
};

pub const FrontierCheckpoint = struct {
    kind: Kind = .skipped_command,
    sequence: u64,
    timestamp: i64,
    snapshot_id: u64 = 0,

    pub const Kind = enum {
        skipped_command,
        snapshot_base,
    };
};

/// Identity of the snapshot that established the restored kernel state. A
/// snapshot-base checkpoint at the same sequence must name this exact
/// snapshot; older checkpoints are covered transitively by the newer state.
pub const RestoredSnapshotFrontier = struct {
    snapshot_id: u64,
    sequence: u64,
};

fn validateRestoredSnapshotCheckpoint(
    checkpoint: FrontierCheckpoint,
    restored_snapshot: ?RestoredSnapshotFrontier,
) error{ SnapshotCheckpointRequiresSnapshot, SnapshotCheckpointMismatch }!void {
    if (checkpoint.kind != .snapshot_base) return;
    const snapshot = restored_snapshot orelse
        return error.SnapshotCheckpointRequiresSnapshot;
    if (checkpoint.sequence > snapshot.sequence) {
        return error.SnapshotCheckpointRequiresSnapshot;
    }
    if (checkpoint.sequence == snapshot.sequence and
        checkpoint.snapshot_id != snapshot.snapshot_id)
    {
        return error.SnapshotCheckpointMismatch;
    }
}

const Xcj3ScanResult = struct {
    valid_len: usize,
    record_count: usize,
    last_sequence: ?u64,
    last_snapshot_checkpoint: ?RestoredSnapshotFrontier,
    partial_tail: bool,
};

/// Crash-recoverable XCJ3 append handle. An append that reaches disk I/O and
/// then fails poisons the handle because durability is ambiguous; close and
/// reopen it to validate or truncate the tail before retrying.
pub const DurableJournalAppender = struct {
    io: std.Io,
    file: std.Io.File,
    end_offset: u64,
    last_sequence: ?u64,
    last_snapshot_checkpoint: ?RestoredSnapshotFrontier,
    poisoned: bool = false,
    fail_next_sync_for_testing: bool = false,
    fail_sync_at_sequence_for_testing: ?u64 = null,
    fail_before_write_at_sequence_for_testing: ?u64 = null,

    pub fn open(
        allocator: std.mem.Allocator,
        dir: std.Io.Dir,
        path: []const u8,
    ) JournalIoError!DurableJournalAppender {
        return openWithIo(allocator, io_context.current(), dir, path);
    }

    pub fn openWithIo(
        allocator: std.mem.Allocator,
        io: std.Io,
        dir: std.Io.Dir,
        path: []const u8,
    ) JournalIoError!DurableJournalAppender {
        const file = dir.openFile(io, path, .{
            .mode = .read_write,
            .lock = if (builtin.is_test) .none else .exclusive,
        }) catch |err| switch (err) {
            error.FileNotFound => blk: {
                try createXcj3FileAtomic(io, dir, path);
                break :blk try dir.openFile(io, path, .{
                    .mode = .read_write,
                    .lock = if (builtin.is_test) .none else .exclusive,
                });
            },
            else => |e| return e,
        };
        errdefer file.close(io);

        const bytes = try readFileBytes(allocator, io, file);
        defer allocator.free(bytes);
        const scan = try scanXcj3(bytes, null);
        if (scan.partial_tail) {
            try file.setLength(io, scan.valid_len);
            try file.sync(io);
        }

        return .{
            .io = io,
            .file = file,
            .end_offset = scan.valid_len,
            .last_sequence = scan.last_sequence,
            .last_snapshot_checkpoint = scan.last_snapshot_checkpoint,
        };
    }

    pub fn deinit(self: *DurableJournalAppender) void {
        self.file.close(self.io);
        self.* = undefined;
    }

    pub fn lastSequence(self: *const DurableJournalAppender) ?u64 {
        return self.last_sequence;
    }

    /// Re-reads the open file so recovery attests the bytes that are actually
    /// durable, rather than trusting only the appender's in-memory frontier.
    pub fn fingerprint(
        self: *const DurableJournalAppender,
        allocator: std.mem.Allocator,
    ) JournalIoError!DurableJournalFingerprint {
        const bytes = try readFileBytes(allocator, self.io, self.file);
        defer allocator.free(bytes);
        const scan = try scanXcj3(bytes, null);
        if (scan.valid_len != self.end_offset or
            scan.last_sequence != self.last_sequence)
        {
            return error.JournalChangedDuringRead;
        }
        return fingerprintXcj3Prefix(bytes, scan.valid_len);
    }

    /// Deterministic test seam for the otherwise platform-dependent window
    /// between a completed write and a failed durability barrier.
    pub fn injectSyncFailureForTesting(self: *DurableJournalAppender) void {
        self.fail_next_sync_for_testing = true;
    }

    /// Targets a later record so paired operations can cross their first
    /// durable boundary before deterministically failing the second.
    pub fn injectSyncFailureAtSequenceForTesting(
        self: *DurableJournalAppender,
        sequence: u64,
    ) void {
        self.fail_sync_at_sequence_for_testing = sequence;
    }

    /// Models a process loss after an external commit point but before this
    /// appender writes the corresponding record.
    pub fn injectPreWriteFailureAtSequenceForTesting(
        self: *DurableJournalAppender,
        sequence: u64,
    ) void {
        self.fail_before_write_at_sequence_for_testing = sequence;
    }

    /// Appends exactly one complete record and does not report success until
    /// both buffered bytes and file metadata have reached File.sync.
    pub fn append(
        self: *DurableJournalAppender,
        entry: JournalEntry,
    ) JournalIoError!void {
        if (self.poisoned) return error.AppenderPoisoned;
        if (self.last_sequence) |previous| {
            if (entry.sequence <= previous) return error.NonMonotonicSequence;
            const expected = std.math.add(u64, previous, 1) catch
                return error.SequenceGap;
            if (entry.sequence != expected) return error.SequenceGap;
        } else if (entry.sequence != 1) {
            return error.SequenceGap;
        }

        var encoded: [XCJ3_MAX_RECORD_BYTES]u8 = undefined;
        const record = try encodeXcj3Record(&encoded, entry);
        try self.commitRecord(record, entry.sequence);
        self.last_snapshot_checkpoint = null;
    }

    /// Durably advances one Java-skipped publication identity without storing
    /// or replaying that command. Live callers use this after every omitted
    /// command and before its result becomes observable.
    pub fn appendSkippedSequence(
        self: *DurableJournalAppender,
        sequence: u64,
        timestamp: i64,
    ) JournalIoError!void {
        if (self.poisoned) return error.AppenderPoisoned;
        if (self.last_sequence) |previous| {
            if (sequence <= previous) return error.NonMonotonicSequence;
            const expected = std.math.add(u64, previous, 1) catch
                return error.SequenceGap;
            if (sequence != expected) return error.SequenceGap;
        } else if (sequence != 1) {
            return error.SequenceGap;
        }

        var encoded: [XCJ3_MAX_RECORD_BYTES]u8 = undefined;
        const record = try encodeXcj3CheckpointRecord(&encoded, .{
            .sequence = sequence,
            .timestamp = timestamp,
        });
        try self.commitRecord(record, sequence);
        self.last_snapshot_checkpoint = null;
    }

    /// Makes a fully verified committed snapshot the authoritative base for
    /// future appends. A forward jump is safe here because the snapshot
    /// attests every preceding state transition; an equal/ahead journal needs
    /// no new record and is left untouched for tail replay.
    pub fn adoptSnapshotFrontier(
        self: *DurableJournalAppender,
        snapshot_id: u64,
        sequence: u64,
        timestamp: i64,
    ) JournalIoError!void {
        if (self.poisoned) return error.AppenderPoisoned;
        if (self.last_sequence) |previous| {
            if (previous > sequence) return;
            if (previous == sequence) {
                if (self.last_snapshot_checkpoint) |checkpoint| {
                    if (checkpoint.snapshot_id != snapshot_id) {
                        return error.SnapshotCheckpointMismatch;
                    }
                }
                return;
            }
        } else if (sequence == 0) {
            return;
        }

        var encoded: [XCJ3_MAX_RECORD_BYTES]u8 = undefined;
        const record = try encodeXcj3CheckpointRecord(&encoded, .{
            .kind = .snapshot_base,
            .sequence = sequence,
            .timestamp = timestamp,
            .snapshot_id = snapshot_id,
        });
        try self.commitRecord(record, sequence);
        self.last_snapshot_checkpoint = .{
            .snapshot_id = snapshot_id,
            .sequence = sequence,
        };
    }

    fn commitRecord(
        self: *DurableJournalAppender,
        record: []const u8,
        sequence: u64,
    ) JournalIoError!void {
        if (self.fail_before_write_at_sequence_for_testing == sequence) {
            self.fail_before_write_at_sequence_for_testing = null;
            return error.InjectedJournalIoFailure;
        }
        const new_end = std.math.add(u64, self.end_offset, record.len) catch
            return error.JournalTooLarge;

        // Clear any tail left by an earlier failed append before writing from
        // the last known-valid boundary. Once disk I/O begins, callers must
        // reopen after any error so recovery can resolve an ambiguous sync.
        errdefer self.poisoned = true;
        try self.file.setLength(self.io, self.end_offset);
        var file_buffer: [256]u8 = undefined;
        var file_writer = self.file.writer(self.io, &file_buffer);
        try file_writer.seekTo(self.end_offset);
        try file_writer.interface.writeAll(record);
        try file_writer.interface.flush();
        if (self.fail_next_sync_for_testing or
            self.fail_sync_at_sequence_for_testing == sequence)
        {
            self.fail_next_sync_for_testing = false;
            self.fail_sync_at_sequence_for_testing = null;
            return error.InjectedJournalIoFailure;
        }
        try self.file.sync(self.io);

        self.end_offset = new_end;
        self.last_sequence = sequence;
    }
};

pub const CommandJournal = struct {
    allocator: std.mem.Allocator,
    entries: std.ArrayListUnmanaged(JournalEntry) = .empty,
    frontier_checkpoints: std.ArrayListUnmanaged(FrontierCheckpoint) = .empty,
    durable_prefix_fingerprint: ?DurableJournalFingerprint = null,

    pub fn init(allocator: std.mem.Allocator) CommandJournal {
        return .{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *CommandJournal) void {
        self.entries.deinit(self.allocator);
        self.frontier_checkpoints.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn len(self: *const CommandJournal) usize {
        return self.entries.items.len;
    }

    /// Present only when this journal was decoded from XCJ3. Legacy XCJ1/2
    /// files have no native durable-prefix identity and cannot attest
    /// settlement recovery.
    pub fn durablePrefixFingerprint(self: *const CommandJournal) ?DurableJournalFingerprint {
        return self.durable_prefix_fingerprint;
    }

    /// Returns the last durable identity whose record timestamp is inside the
    /// same prefix cutoff used by journal replay. Entries and frontier-only
    /// checkpoints are merged by sequence; the first record newer than the
    /// cutoff terminates the prefix even if later timestamps regress.
    pub fn replayFrontier(
        self: *const CommandJournal,
        sequence_exclusive: u64,
        timestamp_ns: u64,
        restored_snapshot: ?RestoredSnapshotFrontier,
    ) error{ SnapshotCheckpointRequiresSnapshot, SnapshotCheckpointMismatch }!u64 {
        if (restored_snapshot) |snapshot| {
            if (snapshot.sequence > sequence_exclusive) {
                return error.SnapshotCheckpointMismatch;
            }
        }
        const timestamp_limit: i64 = if (timestamp_ns >= @as(u64, @intCast(std.math.maxInt(i64))))
            std.math.maxInt(i64)
        else
            @intCast(timestamp_ns);

        var entry_index: usize = 0;
        while (entry_index < self.entries.items.len and
            self.entries.items[entry_index].sequence <= sequence_exclusive) : (entry_index += 1)
        {}
        var checkpoint_index: usize = 0;
        while (checkpoint_index < self.frontier_checkpoints.items.len and
            self.frontier_checkpoints.items[checkpoint_index].sequence <= sequence_exclusive) : (checkpoint_index += 1)
        {
            try validateRestoredSnapshotCheckpoint(
                self.frontier_checkpoints.items[checkpoint_index],
                restored_snapshot,
            );
        }
        // A zero timestamp disables suffix replay, but it must not bypass the
        // identity check for the snapshot checkpoint that established the
        // already-restored prefix.
        if (timestamp_ns == 0) return sequence_exclusive;

        var frontier = sequence_exclusive;
        while (entry_index < self.entries.items.len or
            checkpoint_index < self.frontier_checkpoints.items.len)
        {
            const take_entry = checkpoint_index >= self.frontier_checkpoints.items.len or
                (entry_index < self.entries.items.len and
                    self.entries.items[entry_index].sequence <
                        self.frontier_checkpoints.items[checkpoint_index].sequence);
            const sequence, const timestamp, const checkpoint_kind: ?FrontierCheckpoint.Kind = if (take_entry) blk: {
                const entry = self.entries.items[entry_index];
                entry_index += 1;
                break :blk .{ entry.sequence, entry.input.timestamp, null };
            } else blk: {
                const checkpoint = self.frontier_checkpoints.items[checkpoint_index];
                checkpoint_index += 1;
                break :blk .{ checkpoint.sequence, checkpoint.timestamp, checkpoint.kind };
            };
            if (timestamp > timestamp_limit) break;
            if (checkpoint_kind == .snapshot_base) {
                return error.SnapshotCheckpointRequiresSnapshot;
            }
            frontier = sequence;
        }
        return frontier;
    }

    /// Retains exactly the durable replay prefix after a restored snapshot.
    /// Frontier records remain metadata and therefore do not affect `len()`.
    pub fn retainSequenceRange(
        self: *CommandJournal,
        sequence_exclusive: u64,
        sequence_inclusive: u64,
    ) void {
        var write_index: usize = 0;
        for (self.entries.items) |entry| {
            if (entry.sequence <= sequence_exclusive or entry.sequence > sequence_inclusive) continue;
            self.entries.items[write_index] = entry;
            write_index += 1;
        }
        self.entries.items.len = write_index;

        write_index = 0;
        for (self.frontier_checkpoints.items) |checkpoint| {
            if (checkpoint.sequence <= sequence_exclusive or
                checkpoint.sequence > sequence_inclusive) continue;
            self.frontier_checkpoints.items[write_index] = checkpoint;
            write_index += 1;
        }
        self.frontier_checkpoints.items.len = write_index;
    }

    pub fn executeAndRecord(
        self: *CommandJournal,
        kernel: *kernel_mod.ExchangeKernel,
        command: *cmd.OrderCommand,
    ) kernel_mod.ExecuteError!kernel_mod.CommandResponse {
        return self.executeAndRecordWithSettlement(
            kernel,
            command,
            settlement_lifecycle.Hooks.disabled(),
            .live,
        ) catch |err| switch (err) {
            error.SettlementPreparationRejected,
            error.SettlementRiskR2MarkRejected,
            error.SettlementJournalMarkRejected,
            error.SettlementRecoveryRejected,
            => unreachable,
            else => return @errorCast(err),
        };
    }

    pub fn executeAndRecordWithSettlement(
        self: *CommandJournal,
        kernel: *kernel_mod.ExchangeKernel,
        command: *cmd.OrderCommand,
        settlement_hooks: settlement_lifecycle.Hooks,
        settlement_mode: settlement_lifecycle.ExecutionMode,
    ) kernel_mod.ExecuteWithSettlementError!kernel_mod.CommandResponse {
        // A journal entry is part of the command's commit record. Reserve its
        // storage before the kernel can mutate so an allocator failure cannot
        // leave executed state without a replayable entry.
        try self.entries.ensureUnusedCapacity(self.allocator, 1);

        const input = command.*;
        var response = try kernel.executeWithSettlement(
            command,
            settlement_hooks,
            settlement_mode,
        );
        errdefer response.deinit(kernel.allocator);

        self.entries.appendAssumeCapacity(.{
            .sequence = response.sequence,
            .input = input,
            .result_code = response.result_code,
        });
        return response;
    }

    /// Executes a command at an identity assigned by the core-wide
    /// publication driver. Callers that expose queued work reserve journal
    /// storage before queue commit and use the assume-capacity variant below.
    pub fn executeAndRecordAtSequence(
        self: *CommandJournal,
        kernel: *kernel_mod.ExchangeKernel,
        sequence: publication.PublicationSequence,
        command: *cmd.OrderCommand,
    ) kernel_mod.ExecuteError!kernel_mod.CommandResponse {
        return self.executeAndRecordAtSequenceWithSettlement(
            kernel,
            sequence,
            command,
            settlement_lifecycle.Hooks.disabled(),
            .live,
        ) catch |err| switch (err) {
            error.SettlementPreparationRejected,
            error.SettlementRiskR2MarkRejected,
            error.SettlementJournalMarkRejected,
            error.SettlementRecoveryRejected,
            => unreachable,
            else => return @errorCast(err),
        };
    }

    pub fn executeAndRecordAtSequenceWithSettlement(
        self: *CommandJournal,
        kernel: *kernel_mod.ExchangeKernel,
        sequence: publication.PublicationSequence,
        command: *cmd.OrderCommand,
        settlement_hooks: settlement_lifecycle.Hooks,
        settlement_mode: settlement_lifecycle.ExecutionMode,
    ) kernel_mod.ExecuteWithSettlementError!kernel_mod.CommandResponse {
        try self.entries.ensureUnusedCapacity(self.allocator, 1);
        return self.executeAndRecordAtSequenceAssumeCapacityWithSettlement(
            kernel,
            sequence,
            command,
            settlement_hooks,
            settlement_mode,
        );
    }

    pub fn executeAndRecordAtSequenceAssumeCapacity(
        self: *CommandJournal,
        kernel: *kernel_mod.ExchangeKernel,
        sequence: publication.PublicationSequence,
        command: *cmd.OrderCommand,
    ) kernel_mod.ExecuteError!kernel_mod.CommandResponse {
        return self.executeAndRecordAtSequenceAssumeCapacityWithSettlement(
            kernel,
            sequence,
            command,
            settlement_lifecycle.Hooks.disabled(),
            .live,
        ) catch |err| switch (err) {
            error.SettlementPreparationRejected,
            error.SettlementRiskR2MarkRejected,
            error.SettlementJournalMarkRejected,
            error.SettlementRecoveryRejected,
            => unreachable,
            else => return @errorCast(err),
        };
    }

    pub fn executeAndRecordAtSequenceAssumeCapacityWithSettlement(
        self: *CommandJournal,
        kernel: *kernel_mod.ExchangeKernel,
        sequence: publication.PublicationSequence,
        command: *cmd.OrderCommand,
        settlement_hooks: settlement_lifecycle.Hooks,
        settlement_mode: settlement_lifecycle.ExecutionMode,
    ) kernel_mod.ExecuteWithSettlementError!kernel_mod.CommandResponse {
        const input = command.*;
        var response = try kernel.executeAtSequenceWithSettlement(
            sequence,
            command,
            settlement_hooks,
            settlement_mode,
        );
        errdefer response.deinit(kernel.allocator);

        self.entries.appendAssumeCapacity(.{
            .sequence = response.sequence,
            .input = input,
            .result_code = response.result_code,
        });
        return response;
    }

    pub fn replayIntoKernel(
        self: *const CommandJournal,
        kernel: *kernel_mod.ExchangeKernel,
    ) ReplayError!void {
        for (self.entries.items) |entry| {
            var command = entry.input;
            var replayed = try kernel.executeAtSequence(
                .fromInt(entry.sequence),
                &command,
            );
            defer replayed.deinit(kernel.allocator);

            if (replayed.sequence != entry.sequence) {
                return error.SequenceMismatch;
            }
            if (replayed.result_code != entry.result_code) {
                return error.ResultMismatch;
            }
        }
    }

    pub fn replayIntoKernelUntilTimestamp(
        self: *const CommandJournal,
        kernel: *kernel_mod.ExchangeKernel,
        timestamp_ns: u64,
    ) ReplayError!usize {
        const window = try self.replayIntoKernelFromSequenceUntilTimestamp(
            kernel,
            0,
            timestamp_ns,
        );
        return window.replayed;
    }

    pub fn replayIntoKernelFromSequenceUntilTimestamp(
        self: *const CommandJournal,
        kernel: *kernel_mod.ExchangeKernel,
        sequence_exclusive: u64,
        timestamp_ns: u64,
    ) ReplayError!ReplayWindow {
        if (timestamp_ns == 0) {
            return .{
                .start_index = self.entries.items.len,
                .replayed = 0,
            };
        }

        const replay_limit_i64 = if (timestamp_ns >= @as(u64, @intCast(std.math.maxInt(i64))))
            std.math.maxInt(i64)
        else
            @as(i64, @intCast(timestamp_ns));

        var first_index: ?usize = null;
        var replayed: usize = 0;
        for (self.entries.items, 0..) |entry, idx| {
            if (entry.sequence <= sequence_exclusive) continue;
            if (entry.input.timestamp > replay_limit_i64) break;
            if (first_index == null) first_index = idx;

            var command = entry.input;
            var replayed_response = try kernel.executeAtSequence(
                .fromInt(entry.sequence),
                &command,
            );
            defer replayed_response.deinit(kernel.allocator);

            if (replayed_response.sequence != entry.sequence) {
                return error.SequenceMismatch;
            }
            if (replayed_response.result_code != entry.result_code) {
                return error.ResultMismatch;
            }

            replayed += 1;
        }
        return .{
            .start_index = first_index orelse self.entries.items.len,
            .replayed = replayed,
        };
    }

    pub fn writeToFile(
        self: *const CommandJournal,
        dir: std.Io.Dir,
        path: []const u8,
    ) JournalIoError!void {
        const file = try dir.createFile(io_context.current(), path, .{ .truncate = true });
        defer file.close(io_context.current());

        var buffer: [4096]u8 = undefined;
        var file_writer = file.writer(io_context.current(), &buffer);
        const writer = &file_writer.interface;

        if (self.frontier_checkpoints.items.len != 0) {
            try writer.writeAll(XCJ3_MAGIC);
            var entry_index: usize = 0;
            var checkpoint_index: usize = 0;
            var encoded: [XCJ3_MAX_RECORD_BYTES]u8 = undefined;
            while (entry_index < self.entries.items.len or
                checkpoint_index < self.frontier_checkpoints.items.len)
            {
                const take_entry = checkpoint_index >= self.frontier_checkpoints.items.len or
                    (entry_index < self.entries.items.len and
                        self.entries.items[entry_index].sequence <
                            self.frontier_checkpoints.items[checkpoint_index].sequence);
                const record = if (take_entry) blk: {
                    const entry = self.entries.items[entry_index];
                    entry_index += 1;
                    break :blk try encodeXcj3Record(&encoded, entry);
                } else blk: {
                    const checkpoint = self.frontier_checkpoints.items[checkpoint_index];
                    checkpoint_index += 1;
                    break :blk try encodeXcj3CheckpointRecord(&encoded, checkpoint);
                };
                try writer.writeAll(record);
            }
            try writer.flush();
            return;
        }

        try writer.writeAll("XCJ2");
        try writeInt(u64, @TypeOf(writer), writer, self.entries.items.len);
        for (self.entries.items) |entry| {
            try writeEntryV2(@TypeOf(writer), writer, entry);
        }
        try writer.flush();
    }

    pub fn readFromFile(
        allocator: std.mem.Allocator,
        dir: std.Io.Dir,
        path: []const u8,
    ) JournalIoError!CommandJournal {
        const file = try dir.openFile(io_context.current(), path, .{});
        defer file.close(io_context.current());

        var buffer: [4096]u8 = undefined;
        var file_reader = file.reader(io_context.current(), &buffer);
        const reader = &file_reader.interface;

        const magic = try reader.takeArray(4);
        const is_v1 = std.mem.eql(u8, magic[0..], "XCJ1");
        const is_v2 = std.mem.eql(u8, magic[0..], "XCJ2");
        const is_v3 = std.mem.eql(u8, magic[0..], XCJ3_MAGIC);
        if (!is_v1 and !is_v2 and !is_v3) {
            if (isLegacyLz4FrameMagic(magic[0..])) {
                return error.UnsupportedLegacyLz4Format;
            }
            return error.InvalidMagic;
        }

        if (is_v3) {
            const bytes = try readFileBytes(allocator, io_context.current(), file);
            defer allocator.free(bytes);

            var journal = CommandJournal.init(allocator);
            errdefer journal.deinit();
            const scan = try scanXcj3(bytes, &journal);
            journal.durable_prefix_fingerprint = fingerprintXcj3Prefix(bytes, scan.valid_len);
            return journal;
        }

        const count = try readInt(u64, @TypeOf(reader), reader);
        var journal = CommandJournal.init(allocator);
        errdefer journal.deinit();
        try journal.entries.ensureTotalCapacityPrecise(allocator, count);

        var i: u64 = 0;
        while (i < count) : (i += 1) {
            journal.entries.appendAssumeCapacity(if (is_v1)
                try readEntryV1(@TypeOf(reader), reader)
            else
                try readEntryV2(@TypeOf(reader), reader));
        }
        return journal;
    }
};

fn createXcj3FileAtomic(
    io: std.Io,
    dir: std.Io.Dir,
    path: []const u8,
) JournalIoError!void {
    var atomic_file = try dir.createFileAtomic(io, path, .{
        .replace = false,
    });
    defer atomic_file.deinit(io);

    var buffer: [16]u8 = undefined;
    var file_writer = atomic_file.file.writer(io, &buffer);
    try file_writer.interface.writeAll(XCJ3_MAGIC);
    try file_writer.interface.flush();
    try atomic_file.file.sync(io);
    try atomic_file.link(io);
    try syncParentDirectory(io, dir, path);
}

fn syncParentDirectory(io: std.Io, dir: std.Io.Dir, path: []const u8) JournalIoError!void {
    const parent_path = std.fs.path.dirname(path) orelse ".";
    const parent = try dir.openFile(io, parent_path, .{
        .mode = .read_only,
        .allow_directory = true,
        .path_only = false,
    });
    defer parent.close(io);
    try parent.sync(io);
}

fn readFileBytes(
    allocator: std.mem.Allocator,
    io: std.Io,
    file: std.Io.File,
) JournalIoError![]u8 {
    const byte_len_u64 = try file.length(io);
    const byte_len = std.math.cast(usize, byte_len_u64) orelse return error.JournalTooLarge;
    const bytes = try allocator.alloc(u8, byte_len);
    errdefer allocator.free(bytes);
    if (try file.readPositionalAll(io, bytes, 0) != byte_len) {
        return error.JournalChangedDuringRead;
    }
    if (try file.length(io) != byte_len_u64) {
        return error.JournalChangedDuringRead;
    }
    return bytes;
}

fn scanXcj3(
    bytes: []const u8,
    destination: ?*CommandJournal,
) JournalIoError!Xcj3ScanResult {
    if (bytes.len < XCJ3_MAGIC.len) return error.EndOfStream;
    if (!std.mem.eql(u8, bytes[0..XCJ3_MAGIC.len], XCJ3_MAGIC)) return error.InvalidMagic;

    var offset: usize = XCJ3_MAGIC.len;
    var record_count: usize = 0;
    var last_sequence: ?u64 = null;
    var last_snapshot_checkpoint: ?RestoredSnapshotFrontier = null;
    while (offset < bytes.len) {
        const remaining = bytes.len - offset;
        if (remaining < XCJ3_RECORD_HEADER_BYTES) {
            return .{
                .valid_len = offset,
                .record_count = record_count,
                .last_sequence = last_sequence,
                .last_snapshot_checkpoint = last_snapshot_checkpoint,
                .partial_tail = true,
            };
        }

        const body_len_u32 = std.mem.readInt(u32, bytes[offset..][0..@sizeOf(u32)], .little);
        const checkpoint_kind: ?FrontierCheckpoint.Kind = if (body_len_u32 == XCJ3_CHECKPOINT_BYTES)
            .skipped_command
        else if (body_len_u32 == XCJ3_SNAPSHOT_CHECKPOINT_BYTES)
            .snapshot_base
        else
            null;
        const is_checkpoint = checkpoint_kind != null;
        if (!is_checkpoint and
            (body_len_u32 < XCJ3_MIN_ENTRY_BYTES or body_len_u32 > XCJ3_MAX_ENTRY_BYTES))
        {
            return error.InvalidRecordLength;
        }
        const body_len: usize = body_len_u32;
        const record_len = XCJ3_RECORD_HEADER_BYTES + body_len;
        if (remaining < record_len) {
            return .{
                .valid_len = offset,
                .record_count = record_count,
                .last_sequence = last_sequence,
                .last_snapshot_checkpoint = last_snapshot_checkpoint,
                .partial_tail = true,
            };
        }

        const expected_checksum = std.mem.readInt(
            u64,
            bytes[offset + @sizeOf(u32) ..][0..@sizeOf(u64)],
            .little,
        );
        const body_start = offset + XCJ3_RECORD_HEADER_BYTES;
        const body = bytes[body_start .. body_start + body_len];
        if (xcj3Checksum(body) != expected_checksum) return error.ChecksumMismatch;

        var body_reader = std.Io.Reader.fixed(body);
        const sequence = if (is_checkpoint) blk: {
            const checkpoint = try readXcj3Checkpoint(&body_reader, checkpoint_kind.?);
            if (destination) |journal| {
                try journal.frontier_checkpoints.append(journal.allocator, checkpoint);
            }
            last_snapshot_checkpoint = if (checkpoint.kind == .snapshot_base)
                .{
                    .snapshot_id = checkpoint.snapshot_id,
                    .sequence = checkpoint.sequence,
                }
            else
                null;
            break :blk checkpoint.sequence;
        } else blk: {
            const entry = try readEntryV2(*std.Io.Reader, &body_reader);
            if (destination) |journal| {
                try journal.entries.append(journal.allocator, entry);
            }
            last_snapshot_checkpoint = null;
            break :blk entry.sequence;
        };
        if (body_reader.seek != body_reader.end) return error.InvalidRecordLength;
        if (last_sequence) |previous| {
            if (sequence <= previous) return error.NonMonotonicSequence;
        }

        last_sequence = sequence;
        record_count += 1;
        offset += record_len;
    }

    return .{
        .valid_len = offset,
        .record_count = record_count,
        .last_sequence = last_sequence,
        .last_snapshot_checkpoint = last_snapshot_checkpoint,
        .partial_tail = false,
    };
}

fn fingerprintXcj3Prefix(
    bytes: []const u8,
    valid_len: usize,
) DurableJournalFingerprint {
    std.debug.assert(valid_len >= XCJ3_MAGIC.len);
    std.debug.assert(valid_len <= bytes.len);
    var digest: [std.crypto.hash.Blake3.digest_length]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes[0..valid_len], &digest, .{});
    return .{
        .valid_len = @intCast(valid_len),
        .digest = digest,
    };
}

fn encodeXcj3Record(
    output: *[XCJ3_MAX_RECORD_BYTES]u8,
    entry: JournalEntry,
) JournalIoError![]const u8 {
    var body_writer = std.Io.Writer.fixed(output[XCJ3_RECORD_HEADER_BYTES..]);
    try writeEntryV2(*std.Io.Writer, &body_writer, entry);
    const body = body_writer.buffered();
    if (body.len < XCJ3_MIN_ENTRY_BYTES or body.len > XCJ3_MAX_ENTRY_BYTES) {
        return error.InvalidRecordLength;
    }

    std.mem.writeInt(u32, output[0..@sizeOf(u32)], @intCast(body.len), .little);
    std.mem.writeInt(
        u64,
        output[@sizeOf(u32)..XCJ3_RECORD_HEADER_BYTES],
        xcj3Checksum(body),
        .little,
    );
    return output[0 .. XCJ3_RECORD_HEADER_BYTES + body.len];
}

fn encodeXcj3CheckpointRecord(
    output: *[XCJ3_MAX_RECORD_BYTES]u8,
    checkpoint: FrontierCheckpoint,
) JournalIoError![]const u8 {
    var body_writer = std.Io.Writer.fixed(output[XCJ3_RECORD_HEADER_BYTES..]);
    try body_writer.writeAll(switch (checkpoint.kind) {
        .skipped_command => XCJ3_CHECKPOINT_TAG,
        .snapshot_base => XCJ3_SNAPSHOT_CHECKPOINT_TAG,
    });
    try body_writer.writeInt(u64, checkpoint.sequence, .little);
    try body_writer.writeInt(i64, checkpoint.timestamp, .little);
    if (checkpoint.kind == .snapshot_base) {
        try body_writer.writeInt(u64, checkpoint.snapshot_id, .little);
    }
    const body = body_writer.buffered();
    std.debug.assert(body.len == switch (checkpoint.kind) {
        .skipped_command => XCJ3_CHECKPOINT_BYTES,
        .snapshot_base => XCJ3_SNAPSHOT_CHECKPOINT_BYTES,
    });

    std.mem.writeInt(u32, output[0..@sizeOf(u32)], @intCast(body.len), .little);
    std.mem.writeInt(
        u64,
        output[@sizeOf(u32)..XCJ3_RECORD_HEADER_BYTES],
        xcj3Checksum(body),
        .little,
    );
    return output[0 .. XCJ3_RECORD_HEADER_BYTES + body.len];
}

fn readXcj3Checkpoint(
    reader: *std.Io.Reader,
    kind: FrontierCheckpoint.Kind,
) JournalIoError!FrontierCheckpoint {
    const expected_tag = switch (kind) {
        .skipped_command => XCJ3_CHECKPOINT_TAG,
        .snapshot_base => XCJ3_SNAPSHOT_CHECKPOINT_TAG,
    };
    const tag = try reader.takeArray(XCJ3_CHECKPOINT_TAG.len);
    if (!std.mem.eql(u8, tag, expected_tag)) return error.InvalidMagic;
    return .{
        .kind = kind,
        .sequence = try reader.takeInt(u64, .little),
        .timestamp = try reader.takeInt(i64, .little),
        .snapshot_id = if (kind == .snapshot_base)
            try reader.takeInt(u64, .little)
        else
            0,
    };
}

fn xcj3Checksum(body: []const u8) u64 {
    return std.hash.Wyhash.hash(XCJ3_CHECKSUM_SEED, body);
}

fn isLegacyLz4FrameMagic(magic: []const u8) bool {
    std.debug.assert(magic.len == 4);

    // LZ4 frame magic used by Java persistence snapshots/journals.
    if (magic[0] == 0x04 and magic[1] == 0x22 and magic[2] == 0x4D and magic[3] == 0x18) {
        return true;
    }

    // LZ4 skippable frame range (0x184D2A50..0x184D2A5F).
    if (magic[1] == 0x2A and magic[2] == 0x4D and magic[3] == 0x18 and magic[0] >= 0x50 and magic[0] <= 0x5F) {
        return true;
    }
    return false;
}

fn writeEntryV2(comptime Writer: type, writer: Writer, entry: JournalEntry) !void {
    try writer.writeByte(@intFromEnum(entry.mode));
    try writeEntryBody(Writer, writer, entry);
}

fn writeEntryV1(comptime Writer: type, writer: Writer, entry: JournalEntry) !void {
    try writeEntryBody(Writer, writer, entry);
}

fn writeEntryBody(comptime Writer: type, writer: Writer, entry: JournalEntry) !void {
    try writeInt(u64, Writer, writer, entry.sequence);
    try writeInt(i32, Writer, writer, entry.result_code.code());
    try writeOrderCommand(Writer, writer, entry.input);
}

fn readEntryV1(comptime Reader: type, reader: Reader) JournalIoError!JournalEntry {
    return readEntryBody(Reader, reader, .routed);
}

fn readEntryV2(comptime Reader: type, reader: Reader) JournalIoError!JournalEntry {
    const mode = try ExecutionMode.fromCode(try reader.takeByte());
    return readEntryBody(Reader, reader, mode);
}

fn readEntryBody(
    comptime Reader: type,
    reader: Reader,
    mode: ExecutionMode,
) JournalIoError!JournalEntry {
    const sequence = try readInt(u64, Reader, reader);
    const result_raw = try readInt(i32, Reader, reader);
    const result_code = try cmd.CommandResultCode.fromCode(result_raw);
    const input = try readOrderCommand(Reader, reader);
    return .{
        .sequence = sequence,
        .mode = mode,
        .input = input,
        .result_code = result_code,
    };
}

fn writeOrderCommand(comptime Writer: type, writer: Writer, value: cmd.OrderCommand) !void {
    try writeInt(i8, Writer, writer, value.command.code());
    try writeInt(i64, Writer, writer, value.order_id);
    try writeInt(i32, Writer, writer, value.symbol);
    try writeInt(i64, Writer, writer, value.price);
    try writeInt(i64, Writer, writer, value.size);
    try writeInt(i64, Writer, writer, value.reserve_bid_price);
    try writeOptionalOrderAction(Writer, writer, value.action);
    try writeOptionalOrderType(Writer, writer, value.order_type);
    try writeInt(i64, Writer, writer, value.uid);
    try writeInt(i64, Writer, writer, value.timestamp);
    try writeInt(i32, Writer, writer, value.user_cookie);
    try writeInt(i64, Writer, writer, value.events_group);
    try writeInt(i32, Writer, writer, value.service_flags);
    try writeInt(i32, Writer, writer, value.result_code.code());
}

fn readOrderCommand(comptime Reader: type, reader: Reader) JournalIoError!cmd.OrderCommand {
    const command_type = try cmd.OrderCommandType.fromCode(try readInt(i8, Reader, reader));
    const order_id = try readInt(i64, Reader, reader);
    const symbol = try readInt(i32, Reader, reader);
    const price = try readInt(i64, Reader, reader);
    const size = try readInt(i64, Reader, reader);
    const reserve_bid_price = try readInt(i64, Reader, reader);
    const action = try readOptionalOrderAction(Reader, reader);
    const order_type = try readOptionalOrderType(Reader, reader);
    const uid = try readInt(i64, Reader, reader);
    const timestamp = try readInt(i64, Reader, reader);
    const user_cookie = try readInt(i32, Reader, reader);
    const events_group = try readInt(i64, Reader, reader);
    const service_flags = try readInt(i32, Reader, reader);
    const result_code = try cmd.CommandResultCode.fromCode(try readInt(i32, Reader, reader));

    return .{
        .command = command_type,
        .order_id = order_id,
        .symbol = symbol,
        .price = price,
        .size = size,
        .reserve_bid_price = reserve_bid_price,
        .action = action,
        .order_type = order_type,
        .uid = uid,
        .timestamp = timestamp,
        .user_cookie = user_cookie,
        .events_group = events_group,
        .service_flags = service_flags,
        .result_code = result_code,
    };
}

fn writeOptionalOrderAction(comptime Writer: type, writer: Writer, value: ?common.OrderAction) !void {
    if (value) |present| {
        try writer.writeByte(1);
        try writeInt(i8, Writer, writer, present.code());
    } else {
        try writer.writeByte(0);
    }
}

fn readOptionalOrderAction(comptime Reader: type, reader: Reader) JournalIoError!?common.OrderAction {
    const tag = try reader.takeByte();
    return switch (tag) {
        0 => null,
        1 => try common.OrderAction.fromCode(try readInt(i8, Reader, reader)),
        else => error.InvalidOptionalTag,
    };
}

fn writeOptionalOrderType(comptime Writer: type, writer: Writer, value: ?common.OrderType) !void {
    if (value) |present| {
        try writer.writeByte(1);
        try writeInt(i8, Writer, writer, present.code());
    } else {
        try writer.writeByte(0);
    }
}

fn readOptionalOrderType(comptime Reader: type, reader: Reader) JournalIoError!?common.OrderType {
    const tag = try reader.takeByte();
    return switch (tag) {
        0 => null,
        1 => try common.OrderType.fromCode(try readInt(i8, Reader, reader)),
        else => error.InvalidOptionalTag,
    };
}

fn writeInt(
    comptime T: type,
    comptime Writer: type,
    writer: Writer,
    value: T,
) !void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    try writer.writeAll(&bytes);
}

fn readInt(
    comptime T: type,
    comptime Reader: type,
    reader: Reader,
) JournalIoError!T {
    return try reader.takeInt(T, .little);
}

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

fn makeExchangeSpec() !common.CoreSymbolSpecification {
    return makeExchangeSpecForSymbol(241);
}

fn makeAddUserCommand(uid: i64) cmd.OrderCommand {
    return .{
        .command = .add_user,
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

fn makeRequestOrderBookCommand(symbol: i32, depth: usize) cmd.OrderCommand {
    return .{
        .command = .order_book_request,
        .symbol = symbol,
        .size = @as(i64, @intCast(depth)),
        .result_code = .new,
    };
}

fn simulationPrice(random: std.Random, base: i64) i64 {
    return base + @as(i64, @intCast(random.uintLessThan(u32, 25)));
}

fn makeXcj3TestEntry(sequence: u64, mode: ExecutionMode) JournalEntry {
    var input = makeAddUserCommand(@intCast(20_000 + sequence));
    input.timestamp = @intCast(100_000 + sequence);
    return .{
        .sequence = sequence,
        .mode = mode,
        .input = input,
        .result_code = .success,
    };
}

fn appendEncodedTestRecord(
    allocator: std.mem.Allocator,
    bytes: *std.ArrayListUnmanaged(u8),
    entry: JournalEntry,
) JournalIoError!usize {
    var encoded: [XCJ3_MAX_RECORD_BYTES]u8 = undefined;
    const record = try encodeXcj3Record(&encoded, entry);
    try bytes.appendSlice(allocator, record);
    return bytes.items.len;
}

fn appendEncodedCheckpointTestRecord(
    allocator: std.mem.Allocator,
    bytes: *std.ArrayListUnmanaged(u8),
    checkpoint: FrontierCheckpoint,
) JournalIoError!usize {
    var encoded: [XCJ3_MAX_RECORD_BYTES]u8 = undefined;
    const record = try encodeXcj3CheckpointRecord(&encoded, checkpoint);
    try bytes.appendSlice(allocator, record);
    return bytes.items.len;
}

fn writeTestBytes(
    dir: std.Io.Dir,
    path: []const u8,
    bytes: []const u8,
) (std.Io.File.OpenError || std.Io.File.Writer.Error)!void {
    const file = try dir.createFile(io_context.current(), path, .{ .truncate = true });
    defer file.close(io_context.current());
    try file.writeStreamingAll(io_context.current(), bytes);
}

fn appendTestBytes(
    dir: std.Io.Dir,
    path: []const u8,
    bytes: []const u8,
) (std.Io.File.OpenError || io_context.AppendAllError)!void {
    const file = try dir.openFile(io_context.current(), path, .{ .mode = .read_write });
    defer file.close(io_context.current());
    try io_context.appendAll(file, bytes);
}

test "journal reserves commit storage before command mutation" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer kernel.deinit();

    var failing_allocator_state = std.testing.FailingAllocator.init(allocator, .{});
    var journal = CommandJournal.init(failing_allocator_state.allocator());
    defer {
        failing_allocator_state.fail_index = std.math.maxInt(usize);
        journal.deinit();
    }

    const uid: i64 = 9_001;
    var add_user = makeAddUserCommand(uid);
    const sequence_before = kernel.next_seq;

    failing_allocator_state.fail_index = failing_allocator_state.alloc_index;
    try std.testing.expectError(
        error.OutOfMemory,
        journal.executeAndRecord(&kernel, &add_user),
    );
    try std.testing.expect(failing_allocator_state.has_induced_failure);
    try std.testing.expectEqual(sequence_before, kernel.next_seq);
    try std.testing.expectEqual(cmd.CommandResultCode.new, add_user.result_code);
    try std.testing.expect(kernel.risk_engine.getUserProfile(uid) == null);
    try std.testing.expectEqual(@as(usize, 0), journal.len());

    failing_allocator_state.fail_index = std.math.maxInt(usize);
    var response = try journal.executeAndRecord(&kernel, &add_user);
    defer response.deinit(allocator);

    try std.testing.expectEqual(sequence_before + 1, kernel.next_seq);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    try std.testing.expect(kernel.risk_engine.getUserProfile(uid) != null);
    try std.testing.expectEqual(@as(usize, 1), journal.len());
    try std.testing.expectEqual(response.sequence, journal.entries.items[0].sequence);
}

test "journal replay reproduces sequence, results, risk hash, and L2 view" {
    const allocator = std.testing.allocator;

    var kernel_a = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer kernel_a.deinit();
    var kernel_b = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer kernel_b.deinit();

    const spec = try makeExchangeSpec();
    try std.testing.expect(try kernel_a.addSymbol(spec));
    try std.testing.expect(try kernel_b.addSymbol(spec));

    var journal = CommandJournal.init(allocator);
    defer journal.deinit();

    var add_maker = makeAddUserCommand(9101);
    var add_taker = makeAddUserCommand(9102);
    var dep_quote = makeBalanceAdjustmentCommand(9101, 15, 100_000, 1, .adjustment);
    var dep_base = makeBalanceAdjustmentCommand(9102, 11, 100_000, 1, .adjustment);
    var maker_bid = cmd.OrderCommand.newOrder(.gtc, 9901, 9101, 100, 100, 5, .bid);
    maker_bid.symbol = 241;
    var taker_ask = cmd.OrderCommand.newOrder(.gtc, 9902, 9102, 99, 0, 3, .ask);
    taker_ask.symbol = 241;
    var reduce = cmd.OrderCommand.reduce(9901, 9101, 1);
    reduce.symbol = 241;

    var r1 = try journal.executeAndRecord(&kernel_a, &add_maker);
    defer r1.deinit(allocator);
    var r2 = try journal.executeAndRecord(&kernel_a, &add_taker);
    defer r2.deinit(allocator);
    var r3 = try journal.executeAndRecord(&kernel_a, &dep_quote);
    defer r3.deinit(allocator);
    var r4 = try journal.executeAndRecord(&kernel_a, &dep_base);
    defer r4.deinit(allocator);
    var r5 = try journal.executeAndRecord(&kernel_a, &maker_bid);
    defer r5.deinit(allocator);
    var r6 = try journal.executeAndRecord(&kernel_a, &taker_ask);
    defer r6.deinit(allocator);
    var r7 = try journal.executeAndRecord(&kernel_a, &reduce);
    defer r7.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 7), journal.len());
    try journal.replayIntoKernel(&kernel_b);

    const hash_a = try kernel_a.risk_engine.stateHash();
    const hash_b = try kernel_b.risk_engine.stateHash();
    try std.testing.expectEqual(hash_a, hash_b);

    var req_a = makeRequestOrderBookCommand(241, 5);
    var req_b = makeRequestOrderBookCommand(241, 5);
    var ob_a = try kernel_a.execute(&req_a);
    defer ob_a.deinit(allocator);
    var ob_b = try kernel_b.execute(&req_b);
    defer ob_b.deinit(allocator);

    const snap_a = ob_a.l2_snapshot orelse unreachable;
    const snap_b = ob_b.l2_snapshot orelse unreachable;
    try std.testing.expectEqual(snap_a.asks.items.len, snap_b.asks.items.len);
    try std.testing.expectEqual(snap_a.bids.items.len, snap_b.bids.items.len);
    try std.testing.expectEqual(snap_a.bids.items[0].price, snap_b.bids.items[0].price);
    try std.testing.expectEqual(snap_a.bids.items[0].volume, snap_b.bids.items[0].volume);
}

test "journal file roundtrip preserves entries and replay behavior" {
    const allocator = std.testing.allocator;

    var kernel_a = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .naive,
    });
    defer kernel_a.deinit();
    var kernel_b = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .naive,
    });
    defer kernel_b.deinit();

    const spec = try makeExchangeSpec();
    try std.testing.expect(try kernel_a.addSymbol(spec));
    try std.testing.expect(try kernel_b.addSymbol(spec));

    var journal = CommandJournal.init(allocator);
    defer journal.deinit();

    var add_user = makeAddUserCommand(9301);
    var deposit = makeBalanceAdjustmentCommand(9301, 15, 2_000, 1, .adjustment);
    var bid = cmd.OrderCommand.newOrder(.gtc, 9951, 9301, 50, 50, 2, .bid);
    bid.symbol = 241;

    var r1 = try journal.executeAndRecord(&kernel_a, &add_user);
    defer r1.deinit(allocator);
    var r2 = try journal.executeAndRecord(&kernel_a, &deposit);
    defer r2.deinit(allocator);
    var r3 = try journal.executeAndRecord(&kernel_a, &bid);
    defer r3.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try journal.writeToFile(tmp.dir, "journal.bin");

    var loaded = try CommandJournal.readFromFile(allocator, tmp.dir, "journal.bin");
    defer loaded.deinit();

    try std.testing.expectEqual(journal.len(), loaded.len());
    try std.testing.expectEqualDeep(journal.entries.items, loaded.entries.items);

    try loaded.replayIntoKernel(&kernel_b);
    const hash_a = try kernel_a.risk_engine.stateHash();
    const hash_b = try kernel_b.risk_engine.stateHash();
    try std.testing.expectEqual(hash_a, hash_b);
}

test "journal reads XCJ1 entries as routed execution" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const file = try tmp.dir.createFile(io_context.current(), "journal-v1.bin", .{ .truncate = true });
        defer file.close(io_context.current());
        var buffer: [512]u8 = undefined;
        var file_writer = file.writer(io_context.current(), &buffer);
        const writer = &file_writer.interface;
        try writer.writeAll("XCJ1");
        try writeInt(u64, @TypeOf(writer), writer, 1);
        try writeEntryV1(@TypeOf(writer), writer, .{
            .sequence = 9,
            .input = makeAddUserCommand(9_901),
            .result_code = .success,
        });
        try writer.flush();
    }

    var loaded = try CommandJournal.readFromFile(allocator, tmp.dir, "journal-v1.bin");
    defer loaded.deinit();
    try std.testing.expectEqual(@as(usize, 1), loaded.len());
    try std.testing.expectEqual(ExecutionMode.routed, loaded.entries.items[0].mode);
    try std.testing.expectEqual(@as(u64, 9), loaded.entries.items[0].sequence);
}

test "journal readFromFile reports unsupported legacy lz4 frame explicitly" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const file = try tmp.dir.createFile(io_context.current(), "legacy_lz4.journal", .{ .truncate = true });
    defer file.close(io_context.current());
    try file.writeStreamingAll(io_context.current(), &.{ 0x04, 0x22, 0x4D, 0x18 });

    try std.testing.expectError(
        error.UnsupportedLegacyLz4Format,
        CommandJournal.readFromFile(allocator, tmp.dir, "legacy_lz4.journal"),
    );
}

test "XCJ3 deterministic crash tails are ignored at every byte boundary" {
    const allocator = std.testing.allocator;
    var bytes: std.ArrayListUnmanaged(u8) = .empty;
    defer bytes.deinit(allocator);
    try bytes.appendSlice(allocator, XCJ3_MAGIC);

    var entries = [_]JournalEntry{
        makeXcj3TestEntry(10, .routed),
        makeXcj3TestEntry(11, .split),
        makeXcj3TestEntry(12, .routed),
    };
    entries[1].input.action = .bid;
    entries[2].input.action = .ask;
    entries[2].input.order_type = .gtc;

    var boundaries: [entries.len + 1]usize = undefined;
    boundaries[0] = XCJ3_MAGIC.len;
    for (entries, 0..) |entry, index| {
        boundaries[index + 1] = try appendEncodedTestRecord(allocator, &bytes, entry);
    }

    var short_len: usize = 0;
    while (short_len < XCJ3_MAGIC.len) : (short_len += 1) {
        try std.testing.expectError(error.EndOfStream, scanXcj3(bytes.items[0..short_len], null));
    }

    var cut: usize = XCJ3_MAGIC.len;
    while (cut <= bytes.items.len) : (cut += 1) {
        var expected_count: usize = 0;
        for (boundaries[1..]) |boundary| {
            if (cut < boundary) break;
            expected_count += 1;
        }
        const scan = try scanXcj3(bytes.items[0..cut], null);
        try std.testing.expectEqual(expected_count, scan.record_count);
        try std.testing.expectEqual(boundaries[expected_count], scan.valid_len);
        try std.testing.expectEqual(cut != boundaries[expected_count], scan.partial_tail);
        try std.testing.expectEqual(
            if (expected_count == 0) null else entries[expected_count - 1].sequence,
            scan.last_sequence,
        );
    }
}

test "XCJ3 rejects complete checksum and record-length corruption" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var valid: std.ArrayListUnmanaged(u8) = .empty;
    defer valid.deinit(allocator);
    try valid.appendSlice(allocator, XCJ3_MAGIC);
    _ = try appendEncodedTestRecord(allocator, &valid, makeXcj3TestEntry(20, .routed));

    var checksum_bad = try allocator.dupe(u8, valid.items);
    defer allocator.free(checksum_bad);
    checksum_bad[XCJ3_MAGIC.len + XCJ3_RECORD_HEADER_BYTES + 7] ^= 0x80;
    try writeTestBytes(tmp.dir, "checksum.xcj", checksum_bad);
    try std.testing.expectError(
        error.ChecksumMismatch,
        CommandJournal.readFromFile(allocator, tmp.dir, "checksum.xcj"),
    );
    try std.testing.expectError(
        error.ChecksumMismatch,
        DurableJournalAppender.open(allocator, tmp.dir, "checksum.xcj"),
    );
    {
        const unchanged = try tmp.dir.openFile(io_context.current(), "checksum.xcj", .{});
        defer unchanged.close(io_context.current());
        try std.testing.expectEqual(
            @as(u64, @intCast(checksum_bad.len)),
            try unchanged.length(io_context.current()),
        );
    }

    var invalid_length: [XCJ3_MAGIC.len + XCJ3_RECORD_HEADER_BYTES]u8 = @splat(0);
    @memcpy(invalid_length[0..XCJ3_MAGIC.len], XCJ3_MAGIC);
    std.mem.writeInt(
        u32,
        invalid_length[XCJ3_MAGIC.len..][0..@sizeOf(u32)],
        XCJ3_MAX_ENTRY_BYTES + 1,
        .little,
    );
    try writeTestBytes(tmp.dir, "length.xcj", &invalid_length);
    try std.testing.expectError(
        error.InvalidRecordLength,
        CommandJournal.readFromFile(allocator, tmp.dir, "length.xcj"),
    );
    try std.testing.expectError(
        error.InvalidRecordLength,
        DurableJournalAppender.open(allocator, tmp.dir, "length.xcj"),
    );
    {
        const unchanged = try tmp.dir.openFile(io_context.current(), "length.xcj", .{});
        defer unchanged.close(io_context.current());
        try std.testing.expectEqual(
            @as(u64, invalid_length.len),
            try unchanged.length(io_context.current()),
        );
    }
}

test "XCJ3 rejects duplicate and regressing sequence records" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const corrupt_sequences = [_][2]u64{
        .{ 30, 30 },
        .{ 30, 29 },
    };
    for (corrupt_sequences, 0..) |sequences, index| {
        var bytes: std.ArrayListUnmanaged(u8) = .empty;
        defer bytes.deinit(allocator);
        try bytes.appendSlice(allocator, XCJ3_MAGIC);
        _ = try appendEncodedTestRecord(allocator, &bytes, makeXcj3TestEntry(sequences[0], .routed));
        _ = try appendEncodedTestRecord(allocator, &bytes, makeXcj3TestEntry(sequences[1], .split));

        var path_buffer: [32]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buffer, "sequence-{d}.xcj", .{index});
        try writeTestBytes(tmp.dir, path, bytes.items);
        try std.testing.expectError(
            error.NonMonotonicSequence,
            CommandJournal.readFromFile(allocator, tmp.dir, path),
        );
        try std.testing.expectError(
            error.NonMonotonicSequence,
            DurableJournalAppender.open(allocator, tmp.dir, path),
        );
        const unchanged = try tmp.dir.openFile(io_context.current(), path, .{});
        defer unchanged.close(io_context.current());
        try std.testing.expectEqual(
            @as(u64, @intCast(bytes.items.len)),
            try unchanged.length(io_context.current()),
        );
    }
}

test "XCJ3 roundtrip preserves command skipped and snapshot checkpoint order" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var journal = CommandJournal.init(allocator);
    defer journal.deinit();
    var first = makeXcj3TestEntry(1, .routed);
    first.input.timestamp = 10;
    var third = makeXcj3TestEntry(3, .split);
    third.input.timestamp = 30;
    try journal.entries.append(allocator, first);
    try journal.entries.append(allocator, third);
    try journal.frontier_checkpoints.append(allocator, .{
        .sequence = 2,
        .timestamp = 20,
    });
    try journal.frontier_checkpoints.append(allocator, .{
        .kind = .snapshot_base,
        .sequence = 4,
        .timestamp = 40,
        .snapshot_id = 7004,
    });

    try journal.writeToFile(tmp.dir, "mixed.xcj");
    var loaded = try CommandJournal.readFromFile(allocator, tmp.dir, "mixed.xcj");
    defer loaded.deinit();

    try std.testing.expectEqualDeep(journal.entries.items, loaded.entries.items);
    try std.testing.expectEqualDeep(
        journal.frontier_checkpoints.items,
        loaded.frontier_checkpoints.items,
    );
    try std.testing.expectEqual(
        @as(u64, 3),
        try loaded.replayFrontier(0, 35, null),
    );
    try std.testing.expectError(
        error.SnapshotCheckpointRequiresSnapshot,
        loaded.replayFrontier(0, std.math.maxInt(u64), null),
    );
    try std.testing.expectEqual(
        @as(u64, 4),
        try loaded.replayFrontier(4, std.math.maxInt(u64), .{
            .snapshot_id = 7004,
            .sequence = 4,
        }),
    );
    try std.testing.expectError(
        error.SnapshotCheckpointMismatch,
        loaded.replayFrontier(4, std.math.maxInt(u64), .{
            .snapshot_id = 7005,
            .sequence = 4,
        }),
    );
    try std.testing.expectError(
        error.SnapshotCheckpointMismatch,
        loaded.replayFrontier(4, 0, .{
            .snapshot_id = 7005,
            .sequence = 4,
        }),
    );

    var appender = try DurableJournalAppender.open(allocator, tmp.dir, "mixed.xcj");
    defer appender.deinit();
    try std.testing.expectError(
        error.SnapshotCheckpointMismatch,
        appender.adoptSnapshotFrontier(7005, 4, 40),
    );
    try appender.adoptSnapshotFrontier(7004, 4, 40);
}

test "XCJ3 checkpoint crash tails repair only at complete record boundaries" {
    const allocator = std.testing.allocator;
    var bytes: std.ArrayListUnmanaged(u8) = .empty;
    defer bytes.deinit(allocator);
    try bytes.appendSlice(allocator, XCJ3_MAGIC);
    _ = try appendEncodedTestRecord(allocator, &bytes, makeXcj3TestEntry(1, .routed));
    const first_boundary = bytes.items.len;
    _ = try appendEncodedCheckpointTestRecord(allocator, &bytes, .{
        .sequence = 2,
        .timestamp = 102,
    });
    const skipped_boundary = bytes.items.len;
    _ = try appendEncodedCheckpointTestRecord(allocator, &bytes, .{
        .kind = .snapshot_base,
        .sequence = 3,
        .timestamp = 103,
        .snapshot_id = 9003,
    });
    const snapshot_boundary = bytes.items.len;

    var cut = first_boundary + 1;
    while (cut <= bytes.items.len) : (cut += 1) {
        const scan = try scanXcj3(bytes.items[0..cut], null);
        const expected_count: usize = if (cut < skipped_boundary)
            1
        else if (cut < snapshot_boundary)
            2
        else
            3;
        const expected_boundary = switch (expected_count) {
            1 => first_boundary,
            2 => skipped_boundary,
            3 => snapshot_boundary,
            else => unreachable,
        };
        try std.testing.expectEqual(expected_count, scan.record_count);
        try std.testing.expectEqual(expected_boundary, scan.valid_len);
        try std.testing.expectEqual(cut != expected_boundary, scan.partial_tail);
    }
}

test "XCJ3 rejects checkpoint tag checksum and sequence corruption" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var valid: std.ArrayListUnmanaged(u8) = .empty;
    defer valid.deinit(allocator);
    try valid.appendSlice(allocator, XCJ3_MAGIC);
    _ = try appendEncodedCheckpointTestRecord(allocator, &valid, .{
        .sequence = 1,
        .timestamp = 10,
    });

    var bad_checksum = try allocator.dupe(u8, valid.items);
    defer allocator.free(bad_checksum);
    bad_checksum[bad_checksum.len - 1] ^= 0x01;
    try writeTestBytes(tmp.dir, "checkpoint-checksum.xcj", bad_checksum);
    try std.testing.expectError(
        error.ChecksumMismatch,
        CommandJournal.readFromFile(allocator, tmp.dir, "checkpoint-checksum.xcj"),
    );

    var bad_tag = try allocator.dupe(u8, valid.items);
    defer allocator.free(bad_tag);
    const body_start = XCJ3_MAGIC.len + XCJ3_RECORD_HEADER_BYTES;
    bad_tag[body_start] = 'Z';
    const body_len = std.mem.readInt(
        u32,
        bad_tag[XCJ3_MAGIC.len..][0..@sizeOf(u32)],
        .little,
    );
    const body = bad_tag[body_start .. body_start + body_len];
    std.mem.writeInt(
        u64,
        bad_tag[XCJ3_MAGIC.len + @sizeOf(u32) ..][0..@sizeOf(u64)],
        xcj3Checksum(body),
        .little,
    );
    try writeTestBytes(tmp.dir, "checkpoint-tag.xcj", bad_tag);
    try std.testing.expectError(
        error.InvalidMagic,
        CommandJournal.readFromFile(allocator, tmp.dir, "checkpoint-tag.xcj"),
    );

    for ([_][2]u64{ .{ 2, 2 }, .{ 2, 1 } }, 0..) |sequences, index| {
        var corrupt: std.ArrayListUnmanaged(u8) = .empty;
        defer corrupt.deinit(allocator);
        try corrupt.appendSlice(allocator, XCJ3_MAGIC);
        _ = try appendEncodedCheckpointTestRecord(allocator, &corrupt, .{
            .sequence = sequences[0],
            .timestamp = 20,
        });
        _ = try appendEncodedCheckpointTestRecord(allocator, &corrupt, .{
            .sequence = sequences[1],
            .timestamp = 21,
        });
        var path_buffer: [48]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buffer, "checkpoint-sequence-{d}.xcj", .{index});
        try writeTestBytes(tmp.dir, path, corrupt.items);
        try std.testing.expectError(
            error.NonMonotonicSequence,
            CommandJournal.readFromFile(allocator, tmp.dir, path),
        );
    }
}

test "XCJ3 appender reopens repairs a partial tail and preserves routed split modes" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io_context.current(), "nested/journals");
    const path = "nested/journals/native.xcj";

    var appender = try DurableJournalAppender.open(allocator, tmp.dir, path);
    try std.testing.expectEqual(@as(?u64, null), appender.lastSequence());
    try appender.append(makeXcj3TestEntry(1, .routed));
    appender.deinit();

    appender = try DurableJournalAppender.open(allocator, tmp.dir, path);
    try std.testing.expectEqual(@as(?u64, 1), appender.lastSequence());
    try appender.append(makeXcj3TestEntry(2, .split));
    try std.testing.expectError(
        error.NonMonotonicSequence,
        appender.append(makeXcj3TestEntry(2, .routed)),
    );
    try std.testing.expectError(
        error.NonMonotonicSequence,
        appender.append(makeXcj3TestEntry(1, .routed)),
    );
    try std.testing.expectError(
        error.SequenceGap,
        appender.append(makeXcj3TestEntry(4, .routed)),
    );
    appender.deinit();

    var encoded: [XCJ3_MAX_RECORD_BYTES]u8 = undefined;
    const third_record = try encodeXcj3Record(&encoded, makeXcj3TestEntry(3, .routed));
    const torn_len = XCJ3_RECORD_HEADER_BYTES + 5;
    try appendTestBytes(tmp.dir, path, third_record[0..torn_len]);

    var before_repair = try CommandJournal.readFromFile(allocator, tmp.dir, path);
    defer before_repair.deinit();
    try std.testing.expectEqual(@as(usize, 2), before_repair.len());

    appender = try DurableJournalAppender.open(allocator, tmp.dir, path);
    try std.testing.expectEqual(@as(?u64, 2), appender.lastSequence());
    try std.testing.expectEqual(appender.end_offset, try appender.file.length(io_context.current()));
    try appender.append(makeXcj3TestEntry(3, .routed));
    appender.deinit();

    var loaded = try CommandJournal.readFromFile(allocator, tmp.dir, path);
    defer loaded.deinit();
    try std.testing.expectEqual(@as(usize, 3), loaded.len());
    try std.testing.expectEqual(ExecutionMode.routed, loaded.entries.items[0].mode);
    try std.testing.expectEqual(ExecutionMode.split, loaded.entries.items[1].mode);
    try std.testing.expectEqual(ExecutionMode.routed, loaded.entries.items[2].mode);
    try std.testing.expectEqual(@as(u64, 3), loaded.entries.items[2].sequence);
}

test "XCJ3 support preserves XCJ1 and XCJ2 reader behavior" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const v1_entry = makeXcj3TestEntry(50, .split);
    {
        const file = try tmp.dir.createFile(io_context.current(), "legacy-v1.xcj", .{ .truncate = true });
        defer file.close(io_context.current());
        var buffer: [256]u8 = undefined;
        var file_writer = file.writer(io_context.current(), &buffer);
        try file_writer.interface.writeAll("XCJ1");
        try writeInt(u64, *std.Io.Writer, &file_writer.interface, 1);
        try writeEntryV1(*std.Io.Writer, &file_writer.interface, v1_entry);
        try file_writer.interface.flush();
    }

    const v2_entries = [_]JournalEntry{
        makeXcj3TestEntry(60, .routed),
        makeXcj3TestEntry(61, .split),
    };
    {
        const file = try tmp.dir.createFile(io_context.current(), "legacy-v2.xcj", .{ .truncate = true });
        defer file.close(io_context.current());
        var buffer: [256]u8 = undefined;
        var file_writer = file.writer(io_context.current(), &buffer);
        try file_writer.interface.writeAll("XCJ2");
        try writeInt(u64, *std.Io.Writer, &file_writer.interface, v2_entries.len);
        for (v2_entries) |entry| try writeEntryV2(*std.Io.Writer, &file_writer.interface, entry);
        try file_writer.interface.flush();
    }

    var loaded_v1 = try CommandJournal.readFromFile(allocator, tmp.dir, "legacy-v1.xcj");
    defer loaded_v1.deinit();
    try std.testing.expectEqual(@as(usize, 1), loaded_v1.len());
    try std.testing.expectEqual(ExecutionMode.routed, loaded_v1.entries.items[0].mode);
    try std.testing.expectEqual(v1_entry.sequence, loaded_v1.entries.items[0].sequence);

    var loaded_v2 = try CommandJournal.readFromFile(allocator, tmp.dir, "legacy-v2.xcj");
    defer loaded_v2.deinit();
    try std.testing.expectEqualDeep(&v2_entries, loaded_v2.entries.items);
}

test "journal replay cutoff replays commands up to timestamp limit" {
    const allocator = std.testing.allocator;

    var source = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer source.deinit();
    var replay = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer replay.deinit();

    const spec = try makeExchangeSpec();
    try std.testing.expect(try source.addSymbol(spec));
    try std.testing.expect(try replay.addSymbol(spec));

    var journal = CommandJournal.init(allocator);
    defer journal.deinit();

    var add_u1 = makeAddUserCommand(9701);
    add_u1.timestamp = 10;
    var add_u2 = makeAddUserCommand(9702);
    add_u2.timestamp = 20;
    var add_u3 = makeAddUserCommand(9703);
    add_u3.timestamp = 30;

    var r1 = try journal.executeAndRecord(&source, &add_u1);
    defer r1.deinit(allocator);
    var r2 = try journal.executeAndRecord(&source, &add_u2);
    defer r2.deinit(allocator);
    var r3 = try journal.executeAndRecord(&source, &add_u3);
    defer r3.deinit(allocator);

    const replayed = try journal.replayIntoKernelUntilTimestamp(&replay, 20);
    try std.testing.expectEqual(@as(usize, 2), replayed);

    var user1_report = try replay.queryReport(.{ .single_user = 9701 });
    defer user1_report.deinit(allocator);
    var user2_report = try replay.queryReport(.{ .single_user = 9702 });
    defer user2_report.deinit(allocator);
    var user3_report = try replay.queryReport(.{ .single_user = 9703 });
    defer user3_report.deinit(allocator);

    try std.testing.expectEqual(kernel_mod.SingleUserQueryExecutionStatus.ok, user1_report.single_user.query_execution_status);
    try std.testing.expectEqual(kernel_mod.SingleUserQueryExecutionStatus.ok, user2_report.single_user.query_execution_status);
    try std.testing.expectEqual(kernel_mod.SingleUserQueryExecutionStatus.user_not_found, user3_report.single_user.query_execution_status);

    const none = try journal.replayIntoKernelUntilTimestamp(&replay, 0);
    try std.testing.expectEqual(@as(usize, 0), none);
}

test "journal replay window can skip pre-snapshot sequence prefix" {
    const allocator = std.testing.allocator;

    var source = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer source.deinit();
    var replay = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer replay.deinit();

    var journal = CommandJournal.init(allocator);
    defer journal.deinit();

    var add_u1 = makeAddUserCommand(9801);
    add_u1.timestamp = 10;
    var add_u2 = makeAddUserCommand(9802);
    add_u2.timestamp = 20;
    var add_u3 = makeAddUserCommand(9803);
    add_u3.timestamp = 30;

    var r1 = try journal.executeAndRecord(&source, &add_u1);
    defer r1.deinit(allocator);
    var r2 = try journal.executeAndRecord(&source, &add_u2);
    defer r2.deinit(allocator);
    var r3 = try journal.executeAndRecord(&source, &add_u3);
    defer r3.deinit(allocator);

    // Simulate restored snapshot baseline at sequence=1.
    replay.next_seq = 1;

    const window = try journal.replayIntoKernelFromSequenceUntilTimestamp(
        &replay,
        1,
        25,
    );
    try std.testing.expectEqual(@as(usize, 1), window.start_index);
    try std.testing.expectEqual(@as(usize, 1), window.replayed);

    var user2_report = try replay.queryReport(.{ .single_user = 9802 });
    defer user2_report.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        user2_report.single_user.query_execution_status,
    );

    var user3_report = try replay.queryReport(.{ .single_user = 9803 });
    defer user3_report.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.user_not_found,
        user3_report.single_user.query_execution_status,
    );
}

test "journal, snapshot, and report queries stay deterministic under large seeded stream" {
    const allocator = std.testing.allocator;

    var primary = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer primary.deinit();
    var replay = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer replay.deinit();
    var replay_from_file = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer replay_from_file.deinit();

    const spec = try makeExchangeSpec();
    try std.testing.expect(try primary.addSymbol(spec));
    try std.testing.expect(try replay.addSymbol(spec));
    try std.testing.expect(try replay_from_file.addSymbol(spec));

    var journal = CommandJournal.init(allocator);
    defer journal.deinit();

    const user_ids = [_]i64{ 10001, 10002, 10003, 10004, 10005, 10006, 10007, 10008 };
    var tx_counter_by_uid: std.AutoHashMapUnmanaged(i64, i64) = .empty;
    defer tx_counter_by_uid.deinit(allocator);

    for (user_ids) |uid| {
        var add = makeAddUserCommand(uid);
        var add_resp = try journal.executeAndRecord(&primary, &add);
        add_resp.deinit(allocator);

        var dep_base = makeBalanceAdjustmentCommand(uid, 11, 2_000_000, 1, .adjustment);
        var dep_base_resp = try journal.executeAndRecord(&primary, &dep_base);
        dep_base_resp.deinit(allocator);

        var dep_quote = makeBalanceAdjustmentCommand(uid, 15, 2_000_000, 2, .adjustment);
        var dep_quote_resp = try journal.executeAndRecord(&primary, &dep_quote);
        dep_quote_resp.deinit(allocator);

        try tx_counter_by_uid.put(allocator, uid, 2);
    }

    const Candidate = struct {
        order_id: i64,
        uid: i64,
    };
    var candidates: std.ArrayListUnmanaged(Candidate) = .empty;
    defer candidates.deinit(allocator);

    var prng = std.Random.DefaultPrng.init(0xA11CE55D77EE9911);
    const random = prng.random();
    var next_order_id: i64 = 60_000;
    const rounds: usize = 1600;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var previous_checkpoint_seq: ?u64 = null;

    for (0..rounds) |idx| {
        const roll = random.uintLessThan(u32, 100);
        var command: cmd.OrderCommand = undefined;

        if (roll < 50) {
            const uid = user_ids[random.uintLessThan(usize, user_ids.len)];
            const action: common.OrderAction = if (random.boolean()) .ask else .bid;
            const order_type: common.OrderType = blk: {
                const t = random.uintLessThan(u32, 100);
                if (t < 75) break :blk .gtc;
                if (t < 92) break :blk .ioc;
                break :blk .fok_budget;
            };

            const price = simulationPrice(random, 100);
            const size = @as(i64, @intCast(random.uintLessThan(u32, 11))) + 1;
            const reserve = if (action == .bid)
                price + @as(i64, @intCast(random.uintLessThan(u32, 30)))
            else
                0;

            command = cmd.OrderCommand.newOrder(order_type, next_order_id, uid, price, reserve, size, action);
            command.symbol = 241;

            if (order_type == .gtc) {
                try candidates.append(allocator, .{
                    .order_id = next_order_id,
                    .uid = uid,
                });
            }
            next_order_id += 1;
        } else if (roll < 68) {
            const uid = user_ids[random.uintLessThan(usize, user_ids.len)];
            const currency: i32 = if (random.boolean()) 11 else 15;
            const amount_abs = @as(i64, @intCast(random.uintLessThan(u32, 1000))) + 1;
            const amount: i64 = if (random.uintLessThan(u32, 100) < 20) -amount_abs else amount_abs;
            const tx_counter = tx_counter_by_uid.getPtr(uid) orelse unreachable;
            tx_counter.* += 1;
            command = makeBalanceAdjustmentCommand(uid, currency, amount, tx_counter.*, .adjustment);
        } else if (roll < 82) {
            const picked = if (candidates.items.len > 0 and random.boolean())
                candidates.items[random.uintLessThan(usize, candidates.items.len)]
            else
                Candidate{
                    .order_id = @as(i64, @intCast(random.uintLessThan(u32, 15_000))) + 1,
                    .uid = user_ids[random.uintLessThan(usize, user_ids.len)],
                };
            const uid = if (random.uintLessThan(u32, 100) < 85)
                picked.uid
            else
                user_ids[random.uintLessThan(usize, user_ids.len)];
            command = cmd.OrderCommand.cancel(picked.order_id, uid);
            command.symbol = 241;
        } else if (roll < 92) {
            const picked = if (candidates.items.len > 0 and random.boolean())
                candidates.items[random.uintLessThan(usize, candidates.items.len)]
            else
                Candidate{
                    .order_id = @as(i64, @intCast(random.uintLessThan(u32, 15_000))) + 1,
                    .uid = user_ids[random.uintLessThan(usize, user_ids.len)],
                };
            const uid = if (random.uintLessThan(u32, 100) < 85)
                picked.uid
            else
                user_ids[random.uintLessThan(usize, user_ids.len)];
            command = cmd.OrderCommand.update(picked.order_id, uid, simulationPrice(random, 100));
            command.symbol = 241;
        } else if (roll < 98) {
            const picked = if (candidates.items.len > 0 and random.boolean())
                candidates.items[random.uintLessThan(usize, candidates.items.len)]
            else
                Candidate{
                    .order_id = @as(i64, @intCast(random.uintLessThan(u32, 15_000))) + 1,
                    .uid = user_ids[random.uintLessThan(usize, user_ids.len)],
                };
            const uid = if (random.uintLessThan(u32, 100) < 85)
                picked.uid
            else
                user_ids[random.uintLessThan(usize, user_ids.len)];
            const reduce_size = if (random.uintLessThan(u32, 100) < 5)
                0
            else
                @as(i64, @intCast(random.uintLessThan(u32, 8))) + 1;
            command = cmd.OrderCommand.reduce(picked.order_id, uid, reduce_size);
            command.symbol = 241;
        } else {
            command = makeRequestOrderBookCommand(241, random.uintLessThan(usize, 32) + 1);
        }

        var response = try journal.executeAndRecord(&primary, &command);
        response.deinit(allocator);

        if ((idx + 1) % 160 == 0) {
            const checkpoint_seq = primary.next_seq;
            var risk_report = try primary.queryReport(.risk_state_hash);
            defer risk_report.deinit(allocator);
            var detailed_report = try primary.queryReport(.state_hash_detailed);
            defer detailed_report.deinit(allocator);
            var fee_report = try primary.queryReport(.{ .fee_collected = 15 });
            defer fee_report.deinit(allocator);
            var total_balance_report = try primary.queryReport(.total_currency_balance);
            defer total_balance_report.deinit(allocator);

            switch (detailed_report) {
                .state_hash_detailed => |detailed| {
                    try std.testing.expect(detailed.submodules.items.len > 0);
                },
                else => unreachable,
            }
            switch (total_balance_report) {
                .total_currency_balance => {},
                else => unreachable,
            }
            try std.testing.expectEqual(primary.risk_engine.getFeeCollected(15), fee_report.fee_collected);

            const sample_uid = user_ids[random.uintLessThan(usize, user_ids.len)];
            var single_user_report = try primary.queryReport(.{ .single_user = sample_uid });
            defer single_user_report.deinit(allocator);
            var quote_balance_report = try primary.queryReport(.{
                .user_balance = .{
                    .uid = sample_uid,
                    .currency = 15,
                },
            });
            defer quote_balance_report.deinit(allocator);
            var base_balance_report = try primary.queryReport(.{
                .user_balance = .{
                    .uid = sample_uid,
                    .currency = 11,
                },
            });
            defer base_balance_report.deinit(allocator);

            switch (single_user_report) {
                .single_user => |single_user| {
                    switch (single_user.query_execution_status) {
                        .ok => {
                            const quote_balance = single_user.accounts.get(15) orelse 0;
                            const base_balance = single_user.accounts.get(11) orelse 0;
                            try std.testing.expectEqual(@as(i64, quote_balance), quote_balance_report.user_balance orelse 0);
                            try std.testing.expectEqual(@as(i64, base_balance), base_balance_report.user_balance orelse 0);
                        },
                        .user_not_found => {
                            try std.testing.expectEqual(@as(?i64, null), quote_balance_report.user_balance);
                            try std.testing.expectEqual(@as(?i64, null), base_balance_report.user_balance);
                        },
                    }
                },
                else => unreachable,
            }

            var snapshot = try snapshot_mod.KernelSnapshot.captureFromKernel(allocator, &primary, &.{241}, 32);
            defer snapshot.deinit();
            try snapshot.validateAgainstKernel(&primary);

            var snapshot_name_buf: [64]u8 = undefined;
            const snapshot_name = try std.fmt.bufPrint(&snapshot_name_buf, "sim-seq-{d}.snap", .{checkpoint_seq});
            try snapshot.writeToFile(tmp.dir, snapshot_name);

            var loaded_snapshot = try snapshot_mod.KernelSnapshot.readFromFile(allocator, tmp.dir, snapshot_name);
            defer loaded_snapshot.deinit();
            try loaded_snapshot.validateAgainstKernel(&primary);
            try std.testing.expectEqual(snapshot.risk_state_hash, loaded_snapshot.risk_state_hash);

            var restored_checkpoint = try kernel_mod.ExchangeKernel.init(allocator, .{
                .orderbook_kind = .direct,
            });
            defer restored_checkpoint.deinit();
            try loaded_snapshot.restoreIntoKernel(&restored_checkpoint);
            restored_checkpoint.next_seq = checkpoint_seq;

            var restored_risk = try restored_checkpoint.queryReport(.risk_state_hash);
            defer restored_risk.deinit(allocator);
            try std.testing.expectEqual(risk_report.risk_state_hash, restored_risk.risk_state_hash);

            if (previous_checkpoint_seq) |previous_seq| {
                var previous_snapshot_name_buf: [64]u8 = undefined;
                const previous_snapshot_name = try std.fmt.bufPrint(
                    &previous_snapshot_name_buf,
                    "sim-seq-{d}.snap",
                    .{previous_seq},
                );

                var previous_snapshot = try snapshot_mod.KernelSnapshot.readFromFile(
                    allocator,
                    tmp.dir,
                    previous_snapshot_name,
                );
                defer previous_snapshot.deinit();

                var replay_from_previous_snapshot = try kernel_mod.ExchangeKernel.init(allocator, .{
                    .orderbook_kind = .direct,
                });
                defer replay_from_previous_snapshot.deinit();
                try previous_snapshot.restoreIntoKernel(&replay_from_previous_snapshot);
                replay_from_previous_snapshot.next_seq = previous_seq;

                const replay_window = try journal.replayIntoKernelFromSequenceUntilTimestamp(
                    &replay_from_previous_snapshot,
                    previous_seq,
                    std.math.maxInt(u64),
                );

                const expected_replayed_u64 = checkpoint_seq - previous_seq;
                const expected_replayed = std.math.cast(usize, expected_replayed_u64) orelse unreachable;
                try std.testing.expectEqual(expected_replayed, replay_window.replayed);

                var replayed_risk = try replay_from_previous_snapshot.queryReport(.risk_state_hash);
                defer replayed_risk.deinit(allocator);
                try std.testing.expectEqual(risk_report.risk_state_hash, replayed_risk.risk_state_hash);
            }

            previous_checkpoint_seq = checkpoint_seq;
            _ = risk_report.risk_state_hash;
        }
    }

    try std.testing.expect(journal.len() > user_ids.len);
    try journal.replayIntoKernel(&replay);

    try journal.writeToFile(tmp.dir, "simulation.journal");
    var loaded_journal = try CommandJournal.readFromFile(allocator, tmp.dir, "simulation.journal");
    defer loaded_journal.deinit();
    try std.testing.expectEqual(journal.len(), loaded_journal.len());
    try loaded_journal.replayIntoKernel(&replay_from_file);

    var risk_primary = try primary.queryReport(.risk_state_hash);
    defer risk_primary.deinit(allocator);
    var risk_replay = try replay.queryReport(.risk_state_hash);
    defer risk_replay.deinit(allocator);
    var risk_file = try replay_from_file.queryReport(.risk_state_hash);
    defer risk_file.deinit(allocator);
    try std.testing.expectEqual(risk_primary.risk_state_hash, risk_replay.risk_state_hash);
    try std.testing.expectEqual(risk_primary.risk_state_hash, risk_file.risk_state_hash);

    var detailed_primary = try primary.queryReport(.state_hash_detailed);
    defer detailed_primary.deinit(allocator);
    var detailed_replay = try replay.queryReport(.state_hash_detailed);
    defer detailed_replay.deinit(allocator);
    var detailed_file = try replay_from_file.queryReport(.state_hash_detailed);
    defer detailed_file.deinit(allocator);
    try std.testing.expectEqual(
        detailed_primary.state_hash_detailed.aggregate_hash,
        detailed_replay.state_hash_detailed.aggregate_hash,
    );
    try std.testing.expectEqual(
        detailed_primary.state_hash_detailed.aggregate_hash,
        detailed_file.state_hash_detailed.aggregate_hash,
    );

    var final_snapshot = try snapshot_mod.KernelSnapshot.captureFromKernel(allocator, &primary, &.{241}, 64);
    defer final_snapshot.deinit();
    try final_snapshot.validateAgainstKernel(&replay);
    try final_snapshot.validateAgainstKernel(&replay_from_file);
}

test "journal, snapshot, and report queries stay deterministic under multi-symbol seeded stream" {
    const allocator = std.testing.allocator;

    var primary = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer primary.deinit();
    var replay = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer replay.deinit();
    var replay_from_file = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer replay_from_file.deinit();

    const symbols = [_]i32{ 241, 242 };
    for (symbols) |symbol| {
        const spec = try makeExchangeSpecForSymbol(symbol);
        try std.testing.expect(try primary.addSymbol(spec));
        try std.testing.expect(try replay.addSymbol(spec));
        try std.testing.expect(try replay_from_file.addSymbol(spec));
    }

    var journal = CommandJournal.init(allocator);
    defer journal.deinit();

    const user_ids = [_]i64{ 11001, 11002, 11003, 11004, 11005, 11006, 11007, 11008, 11009, 11010 };
    var tx_counter_by_uid: std.AutoHashMapUnmanaged(i64, i64) = .empty;
    defer tx_counter_by_uid.deinit(allocator);

    for (user_ids) |uid| {
        var add = makeAddUserCommand(uid);
        var add_resp = try journal.executeAndRecord(&primary, &add);
        add_resp.deinit(allocator);

        var dep_base = makeBalanceAdjustmentCommand(uid, 11, 2_500_000, 1, .adjustment);
        var dep_base_resp = try journal.executeAndRecord(&primary, &dep_base);
        dep_base_resp.deinit(allocator);

        var dep_quote = makeBalanceAdjustmentCommand(uid, 15, 2_500_000, 2, .adjustment);
        var dep_quote_resp = try journal.executeAndRecord(&primary, &dep_quote);
        dep_quote_resp.deinit(allocator);

        try tx_counter_by_uid.put(allocator, uid, 2);
    }

    const Candidate = struct {
        order_id: i64,
        uid: i64,
        symbol: i32,
    };
    var candidates: std.ArrayListUnmanaged(Candidate) = .empty;
    defer candidates.deinit(allocator);

    var prng = std.Random.DefaultPrng.init(0xBEEFCAFEA11C9911);
    const random = prng.random();
    var next_order_id: i64 = 80_000;
    const rounds: usize = 2200;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var previous_checkpoint_seq: ?u64 = null;

    for (0..rounds) |idx| {
        const roll = random.uintLessThan(u32, 100);
        var command: cmd.OrderCommand = undefined;

        if (roll < 52) {
            const uid = user_ids[random.uintLessThan(usize, user_ids.len)];
            const symbol = symbols[random.uintLessThan(usize, symbols.len)];
            const action: common.OrderAction = if (random.boolean()) .ask else .bid;
            const order_type: common.OrderType = blk: {
                const t = random.uintLessThan(u32, 100);
                if (t < 72) break :blk .gtc;
                if (t < 90) break :blk .ioc;
                break :blk .fok_budget;
            };

            const base_price: i64 = if (symbol == 241) 100 else 160;
            const price = simulationPrice(random, base_price);
            const size = @as(i64, @intCast(random.uintLessThan(u32, 13))) + 1;
            const reserve = if (action == .bid)
                price + @as(i64, @intCast(random.uintLessThan(u32, 24)))
            else
                0;

            command = cmd.OrderCommand.newOrder(order_type, next_order_id, uid, price, reserve, size, action);
            command.symbol = symbol;

            if (order_type == .gtc) {
                try candidates.append(allocator, .{
                    .order_id = next_order_id,
                    .uid = uid,
                    .symbol = symbol,
                });
            }
            next_order_id += 1;
        } else if (roll < 68) {
            const uid = user_ids[random.uintLessThan(usize, user_ids.len)];
            const currency: i32 = if (random.boolean()) 11 else 15;
            const amount_abs = @as(i64, @intCast(random.uintLessThan(u32, 1200))) + 1;
            const amount: i64 = if (random.uintLessThan(u32, 100) < 18) -amount_abs else amount_abs;
            const tx_counter = tx_counter_by_uid.getPtr(uid) orelse unreachable;
            tx_counter.* += 1;
            command = makeBalanceAdjustmentCommand(uid, currency, amount, tx_counter.*, .adjustment);
        } else if (roll < 82) {
            const picked = if (candidates.items.len > 0 and random.boolean())
                candidates.items[random.uintLessThan(usize, candidates.items.len)]
            else
                Candidate{
                    .order_id = @as(i64, @intCast(random.uintLessThan(u32, 20_000))) + 1,
                    .uid = user_ids[random.uintLessThan(usize, user_ids.len)],
                    .symbol = symbols[random.uintLessThan(usize, symbols.len)],
                };
            const uid = if (random.uintLessThan(u32, 100) < 85)
                picked.uid
            else
                user_ids[random.uintLessThan(usize, user_ids.len)];
            command = cmd.OrderCommand.cancel(picked.order_id, uid);
            command.symbol = picked.symbol;
        } else if (roll < 92) {
            const picked = if (candidates.items.len > 0 and random.boolean())
                candidates.items[random.uintLessThan(usize, candidates.items.len)]
            else
                Candidate{
                    .order_id = @as(i64, @intCast(random.uintLessThan(u32, 20_000))) + 1,
                    .uid = user_ids[random.uintLessThan(usize, user_ids.len)],
                    .symbol = symbols[random.uintLessThan(usize, symbols.len)],
                };
            const uid = if (random.uintLessThan(u32, 100) < 85)
                picked.uid
            else
                user_ids[random.uintLessThan(usize, user_ids.len)];
            const base_price: i64 = if (picked.symbol == 241) 100 else 160;
            command = cmd.OrderCommand.update(picked.order_id, uid, simulationPrice(random, base_price));
            command.symbol = picked.symbol;
        } else if (roll < 98) {
            const picked = if (candidates.items.len > 0 and random.boolean())
                candidates.items[random.uintLessThan(usize, candidates.items.len)]
            else
                Candidate{
                    .order_id = @as(i64, @intCast(random.uintLessThan(u32, 20_000))) + 1,
                    .uid = user_ids[random.uintLessThan(usize, user_ids.len)],
                    .symbol = symbols[random.uintLessThan(usize, symbols.len)],
                };
            const uid = if (random.uintLessThan(u32, 100) < 85)
                picked.uid
            else
                user_ids[random.uintLessThan(usize, user_ids.len)];
            const reduce_size = if (random.uintLessThan(u32, 100) < 5)
                0
            else
                @as(i64, @intCast(random.uintLessThan(u32, 9))) + 1;
            command = cmd.OrderCommand.reduce(picked.order_id, uid, reduce_size);
            command.symbol = picked.symbol;
        } else {
            const symbol = symbols[random.uintLessThan(usize, symbols.len)];
            command = makeRequestOrderBookCommand(symbol, random.uintLessThan(usize, 32) + 1);
        }

        var response = try journal.executeAndRecord(&primary, &command);
        response.deinit(allocator);

        if ((idx + 1) % 220 == 0) {
            const checkpoint_seq = primary.next_seq;
            var risk_report = try primary.queryReport(.risk_state_hash);
            defer risk_report.deinit(allocator);
            var detailed_report = try primary.queryReport(.state_hash_detailed);
            defer detailed_report.deinit(allocator);
            var fee_report = try primary.queryReport(.{ .fee_collected = 15 });
            defer fee_report.deinit(allocator);
            var total_balance_report = try primary.queryReport(.total_currency_balance);
            defer total_balance_report.deinit(allocator);

            switch (detailed_report) {
                .state_hash_detailed => |detailed| {
                    try std.testing.expect(detailed.submodules.items.len > 0);
                },
                else => unreachable,
            }
            switch (total_balance_report) {
                .total_currency_balance => {},
                else => unreachable,
            }
            try std.testing.expectEqual(primary.risk_engine.getFeeCollected(15), fee_report.fee_collected);

            const sample_uid = user_ids[random.uintLessThan(usize, user_ids.len)];
            var single_user_report = try primary.queryReport(.{ .single_user = sample_uid });
            defer single_user_report.deinit(allocator);
            var quote_balance_report = try primary.queryReport(.{
                .user_balance = .{
                    .uid = sample_uid,
                    .currency = 15,
                },
            });
            defer quote_balance_report.deinit(allocator);
            var base_balance_report = try primary.queryReport(.{
                .user_balance = .{
                    .uid = sample_uid,
                    .currency = 11,
                },
            });
            defer base_balance_report.deinit(allocator);

            switch (single_user_report) {
                .single_user => |single_user| {
                    switch (single_user.query_execution_status) {
                        .ok => {
                            const quote_balance = single_user.accounts.get(15) orelse 0;
                            const base_balance = single_user.accounts.get(11) orelse 0;
                            try std.testing.expectEqual(@as(i64, quote_balance), quote_balance_report.user_balance orelse 0);
                            try std.testing.expectEqual(@as(i64, base_balance), base_balance_report.user_balance orelse 0);
                        },
                        .user_not_found => {
                            try std.testing.expectEqual(@as(?i64, null), quote_balance_report.user_balance);
                            try std.testing.expectEqual(@as(?i64, null), base_balance_report.user_balance);
                        },
                    }
                },
                else => unreachable,
            }

            for (symbols) |symbol| {
                var order_book_report = try primary.queryReport(.{
                    .order_book = .{
                        .symbol = symbol,
                        .depth = 24,
                    },
                });
                defer order_book_report.deinit(allocator);
                switch (order_book_report) {
                    .order_book => {},
                    else => unreachable,
                }
            }

            var snapshot = try snapshot_mod.KernelSnapshot.captureFromKernel(allocator, &primary, symbols[0..], 24);
            defer snapshot.deinit();
            try snapshot.validateAgainstKernel(&primary);

            var snapshot_name_buf: [64]u8 = undefined;
            const snapshot_name = try std.fmt.bufPrint(
                &snapshot_name_buf,
                "multi-sim-seq-{d}.snap",
                .{checkpoint_seq},
            );
            try snapshot.writeToFile(tmp.dir, snapshot_name);

            var loaded_snapshot = try snapshot_mod.KernelSnapshot.readFromFile(allocator, tmp.dir, snapshot_name);
            defer loaded_snapshot.deinit();
            try loaded_snapshot.validateAgainstKernel(&primary);
            try std.testing.expectEqual(snapshot.risk_state_hash, loaded_snapshot.risk_state_hash);

            var restored_checkpoint = try kernel_mod.ExchangeKernel.init(allocator, .{
                .orderbook_kind = .direct,
            });
            defer restored_checkpoint.deinit();
            try loaded_snapshot.restoreIntoKernel(&restored_checkpoint);
            restored_checkpoint.next_seq = checkpoint_seq;

            var restored_risk = try restored_checkpoint.queryReport(.risk_state_hash);
            defer restored_risk.deinit(allocator);
            try std.testing.expectEqual(risk_report.risk_state_hash, restored_risk.risk_state_hash);

            if (previous_checkpoint_seq) |previous_seq| {
                var previous_snapshot_name_buf: [64]u8 = undefined;
                const previous_snapshot_name = try std.fmt.bufPrint(
                    &previous_snapshot_name_buf,
                    "multi-sim-seq-{d}.snap",
                    .{previous_seq},
                );

                var previous_snapshot = try snapshot_mod.KernelSnapshot.readFromFile(
                    allocator,
                    tmp.dir,
                    previous_snapshot_name,
                );
                defer previous_snapshot.deinit();

                var replay_from_previous_snapshot = try kernel_mod.ExchangeKernel.init(allocator, .{
                    .orderbook_kind = .direct,
                });
                defer replay_from_previous_snapshot.deinit();
                try previous_snapshot.restoreIntoKernel(&replay_from_previous_snapshot);
                replay_from_previous_snapshot.next_seq = previous_seq;

                const replay_window = try journal.replayIntoKernelFromSequenceUntilTimestamp(
                    &replay_from_previous_snapshot,
                    previous_seq,
                    std.math.maxInt(u64),
                );

                const expected_replayed_u64 = checkpoint_seq - previous_seq;
                const expected_replayed = std.math.cast(usize, expected_replayed_u64) orelse unreachable;
                try std.testing.expectEqual(expected_replayed, replay_window.replayed);

                var replayed_risk = try replay_from_previous_snapshot.queryReport(.risk_state_hash);
                defer replayed_risk.deinit(allocator);
                try std.testing.expectEqual(risk_report.risk_state_hash, replayed_risk.risk_state_hash);
            }

            previous_checkpoint_seq = checkpoint_seq;
            _ = risk_report.risk_state_hash;
        }
    }

    try std.testing.expect(journal.len() > user_ids.len);
    try journal.replayIntoKernel(&replay);

    try journal.writeToFile(tmp.dir, "multi-simulation.journal");
    var loaded_journal = try CommandJournal.readFromFile(allocator, tmp.dir, "multi-simulation.journal");
    defer loaded_journal.deinit();
    try std.testing.expectEqual(journal.len(), loaded_journal.len());
    try loaded_journal.replayIntoKernel(&replay_from_file);

    var risk_primary = try primary.queryReport(.risk_state_hash);
    defer risk_primary.deinit(allocator);
    var risk_replay = try replay.queryReport(.risk_state_hash);
    defer risk_replay.deinit(allocator);
    var risk_file = try replay_from_file.queryReport(.risk_state_hash);
    defer risk_file.deinit(allocator);
    try std.testing.expectEqual(risk_primary.risk_state_hash, risk_replay.risk_state_hash);
    try std.testing.expectEqual(risk_primary.risk_state_hash, risk_file.risk_state_hash);

    var detailed_primary = try primary.queryReport(.state_hash_detailed);
    defer detailed_primary.deinit(allocator);
    var detailed_replay = try replay.queryReport(.state_hash_detailed);
    defer detailed_replay.deinit(allocator);
    var detailed_file = try replay_from_file.queryReport(.state_hash_detailed);
    defer detailed_file.deinit(allocator);
    try std.testing.expectEqual(
        detailed_primary.state_hash_detailed.aggregate_hash,
        detailed_replay.state_hash_detailed.aggregate_hash,
    );
    try std.testing.expectEqual(
        detailed_primary.state_hash_detailed.aggregate_hash,
        detailed_file.state_hash_detailed.aggregate_hash,
    );

    var final_snapshot = try snapshot_mod.KernelSnapshot.captureFromKernel(allocator, &primary, symbols[0..], 48);
    defer final_snapshot.deinit();
    try final_snapshot.validateAgainstKernel(&replay);
    try final_snapshot.validateAgainstKernel(&replay_from_file);
}

// ---------------------------------------------------------------------------
// Seeded virtual-I/O fault campaign (TigerBeetle DST style)
//
// Uses DurableJournalAppender's existing inject seams (pre-write / post-write
// sync failure) plus reopen recovery as a deterministic virtual-disk schedule
// under DefaultPrng. Same seed dual-runs; prints seed + test-filter on fail.
// ---------------------------------------------------------------------------

const journal_io_campaign_filter =
    "seeded journal I/O fault campaign is deterministic and seed-sensitive";

fn printJournalIoCampaignHint(seed: u64, step: []const u8) void {
    std.debug.print(
        "seed=0x{x} step={s}; replay with: zig build test --test-filter \"{s}\"\n",
        .{ seed, step, journal_io_campaign_filter },
    );
}

const JournalIoCampaignResult = struct {
    durable_count: usize,
    injected_faults: usize,
    reopens: usize,
    fingerprint: u64,
};

fn journalIoFingerprint(entries: []const JournalEntry) u64 {
    var hash: u64 = 0xcbf29ce484222325;
    for (entries) |entry| {
        hash ^= entry.sequence +% 0x9e3779b97f4a7c15 +% (hash << 6) +% (hash >> 2);
        hash ^= @intFromEnum(entry.mode);
        hash ^= @as(u64, @bitCast(@as(i64, @intFromEnum(entry.result_code))));
        hash ^= @as(u64, @bitCast(entry.input.uid));
    }
    return hash;
}

fn runSeededJournalIoFaultCampaign(
    allocator: std.mem.Allocator,
    seed: u64,
    records: usize,
) !JournalIoCampaignResult {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = "seeded-io-fault.xcj";

    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    var appender = try DurableJournalAppender.open(allocator, tmp.dir, path);
    var injected_faults: usize = 0;
    var reopens: usize = 0;
    var next_seq: u64 = 1;
    var planned: usize = 0;

    while (planned < records) {
        const mode: ExecutionMode = if (random.boolean()) .routed else .split;
        const entry = makeXcj3TestEntry(next_seq, mode);
        const fault_roll = random.uintLessThan(u32, 100);

        if (fault_roll < 12) {
            // Pre-write loss: sequence never becomes durable; reopen must keep prior frontier.
            appender.injectPreWriteFailureAtSequenceForTesting(next_seq);
            injected_faults += 1;
            try std.testing.expectError(error.InjectedJournalIoFailure, appender.append(entry));
            appender.deinit();
            reopens += 1;
            appender = try DurableJournalAppender.open(allocator, tmp.dir, path);
            // Sequence is still free after pre-write fail — retry same seq later without advancing planned.
            continue;
        } else if (fault_roll < 24) {
            // Sync failure after write: poison; reopen recovers durable prefix (may exclude this seq).
            appender.injectSyncFailureAtSequenceForTesting(next_seq);
            injected_faults += 1;
            const result = appender.append(entry);
            if (result) |_| {
                // Should not succeed when inject targets this sequence.
                printJournalIoCampaignHint(seed, "sync-inject-unexpected-success");
                return error.TestUnexpectedResult;
            } else |err| {
                if (err != error.InjectedJournalIoFailure) {
                    printJournalIoCampaignHint(seed, "sync-inject-unexpected-error");
                    return err;
                }
            }
            appender.deinit();
            reopens += 1;
            appender = try DurableJournalAppender.open(allocator, tmp.dir, path);
            // After sync fail the record may or may not be durable; resume from last_sequence+1.
            if (appender.lastSequence()) |last| {
                if (last >= next_seq) {
                    planned += 1;
                    next_seq = last + 1;
                }
                // else: next_seq still free, retry without counting planned
            }
            continue;
        } else if (fault_roll < 30 and next_seq > 1) {
            // Offline torn tail: close cleanly, tear next record bytes, reopen repairs.
            appender.deinit();
            var encoded: [XCJ3_MAX_RECORD_BYTES]u8 = undefined;
            const record = try encodeXcj3Record(&encoded, entry);
            const tear = 1 + random.uintLessThan(u32, @intCast(@max(record.len / 2, 1)));
            try appendTestBytes(tmp.dir, path, record[0..tear]);
            injected_faults += 1;
            reopens += 1;
            appender = try DurableJournalAppender.open(allocator, tmp.dir, path);
            // Torn record discarded; append full entry at same sequence.
            try appender.append(entry);
            planned += 1;
            next_seq += 1;
            continue;
        }

        try appender.append(entry);
        planned += 1;
        next_seq += 1;
    }
    appender.deinit();

    var loaded = try CommandJournal.readFromFile(allocator, tmp.dir, path);
    defer loaded.deinit();

    // Clean dual-write of the durable sequence must match the recovered file.
    var clean_tmp = std.testing.tmpDir(.{});
    defer clean_tmp.cleanup();
    const clean_path = "clean.xcj";
    var clean = try DurableJournalAppender.open(allocator, clean_tmp.dir, clean_path);
    for (loaded.entries.items) |entry| {
        try clean.append(entry);
    }
    clean.deinit();
    var clean_loaded = try CommandJournal.readFromFile(allocator, clean_tmp.dir, clean_path);
    defer clean_loaded.deinit();

    if (loaded.len() != clean_loaded.len()) {
        printJournalIoCampaignHint(seed, "clean-len-mismatch");
        return error.TestUnexpectedResult;
    }
    for (loaded.entries.items, clean_loaded.entries.items) |a, b| {
        if (a.sequence != b.sequence or a.mode != b.mode or a.result_code != b.result_code) {
            printJournalIoCampaignHint(seed, "clean-entry-mismatch");
            return error.TestUnexpectedResult;
        }
    }

    return .{
        .durable_count = loaded.len(),
        .injected_faults = injected_faults,
        .reopens = reopens,
        .fingerprint = journalIoFingerprint(loaded.entries.items),
    };
}

test "seeded journal I/O fault campaign is deterministic and seed-sensitive" {
    const allocator = std.testing.allocator;
    const seeds = [_]u64{ 42, 7, 0xC0FFEE, 0xDEADBEEF12345678 };
    const records: usize = 40;

    var first_by_seed: [seeds.len]JournalIoCampaignResult = undefined;
    for (seeds, 0..) |seed, idx| {
        const first = runSeededJournalIoFaultCampaign(allocator, seed, records) catch |err| {
            printJournalIoCampaignHint(seed, "campaign");
            return err;
        };
        const second = runSeededJournalIoFaultCampaign(allocator, seed, records) catch |err| {
            printJournalIoCampaignHint(seed, "campaign-rerun");
            return err;
        };
        if (first.fingerprint != second.fingerprint or
            first.durable_count != second.durable_count or
            first.injected_faults != second.injected_faults or
            first.reopens != second.reopens)
        {
            printJournalIoCampaignHint(seed, "determinism");
            return error.TestUnexpectedResult;
        }
        try std.testing.expect(first.durable_count == records);
        try std.testing.expect(first.injected_faults > 0);
        first_by_seed[idx] = first;
    }

    var saw_diff = false;
    for (0..seeds.len) |i| {
        for (i + 1..seeds.len) |j| {
            if (first_by_seed[i].fingerprint != first_by_seed[j].fingerprint or
                first_by_seed[i].injected_faults != first_by_seed[j].injected_faults)
            {
                saw_diff = true;
            }
        }
    }
    if (!saw_diff) {
        printJournalIoCampaignHint(seeds[0], "seed-sensitivity");
        return error.TestUnexpectedResult;
    }
}

test "seeded journal I/O fault multi-seed sweep includes seed 42" {
    const allocator = std.testing.allocator;
    const records: usize = 20;
    const seed_count: u64 = 32;

    var seed: u64 = 0;
    while (seed < seed_count) : (seed += 1) {
        const first = runSeededJournalIoFaultCampaign(allocator, seed, records) catch |err| {
            printJournalIoCampaignHint(seed, "multi-seed-sweep");
            return err;
        };
        const second = runSeededJournalIoFaultCampaign(allocator, seed, records) catch |err| {
            printJournalIoCampaignHint(seed, "multi-seed-sweep-rerun");
            return err;
        };
        if (first.fingerprint != second.fingerprint or first.durable_count != records) {
            printJournalIoCampaignHint(seed, "multi-seed-sweep-determinism");
            return error.TestUnexpectedResult;
        }
    }
    _ = try runSeededJournalIoFaultCampaign(allocator, 42, records);
}
