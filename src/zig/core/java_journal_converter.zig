const std = @import("std");
const cmd = @import("cmd.zig");
const io_context = @import("io_context.zig");
const java_binary_wire = @import("java_binary_wire.zig");
const java_journal_replay = @import("java_journal_replay.zig");
const java_journal_wire = @import("java_journal_wire.zig");
const java_persistence_converter = @import("java_persistence_converter.zig");
const journal = @import("journal.zig");
const kernel_mod = @import("exchange_kernel.zig");
const publication = @import("publication_sequence.zig");
const bounded_file = @import("sharded_snapshot.zig");
const snapshot_store = @import("sharded_snapshot_store.zig");

const log = std.log.scoped(.java_journal_converter);

pub const RequestValidationError = error{
    EmptyPartitionFamily,
    TooManyPartitions,
    InvalidPartitionRange,
    PartitionIdMismatch,
    EmptyPartitionPath,
    EmptyOutputPath,
    EmptyStagingPath,
    InvalidOutputPath,
    InvalidStagingPath,
    OutputPathEqualsStagingPath,
    InvalidInputLimit,
    InvalidEntryLimit,
    InvalidCompressedBlockLimit,
    InvalidSequenceBoundary,
    PartitionFrontierDoesNotMatchSnapshot,
    InvalidFreshTopology,
    FreshTargetRequiresZeroFrontier,
    InvalidPersistenceSnapshotProof,
};

pub const Partition = struct {
    id: u16,
    path: []const u8,
};

/// The caller supplies the sequence immediately before `first_id`. This keeps
/// duplicate/regression validation well-defined even when conversion starts at
/// a partition other than zero.
pub const PartitionRange = struct {
    first_id: u16,
    last_id: u16,
    sequence_before_first: i64 = 0,
};

pub const FreshTarget = struct {
    matching_count: usize,
    risk_count: usize,
    base_config: kernel_mod.Config = .{},
};

/// A staged target is an unpublished snapshot import owned by the caller. Once
/// conversion starts replay, any conversion error means it may be partially
/// mutated and must be discarded. A fresh target is private to this conversion
/// and is always destroyed before return.
pub const Target = union(enum) {
    fresh: FreshTarget,
    staged: *kernel_mod.MultiShardKernelSet,
};

pub const GapPolicy = enum {
    /// Reject any sequence not immediately adjacent to the snapshot frontier
    /// or preceding replayable record.
    reject,
    /// Preserve sparse Java identities. Such gaps normally represent query
    /// commands that Java deliberately did not journal.
    preserve,
};

/// Capability and digest for an already committed native snapshot manifest.
/// The converter reads and validates this XCM2 advertisement itself, then
/// binds Java's payload-less terminal persistence pair to the manifest header.
/// Caller-provided scalar identities are intentionally insufficient.
/// `Request` has no exchange identifier, so selecting the authenticated
/// manifest for the correct exchange remains a caller-owned migration-inventory
/// boundary; the converter additionally binds topology and live target config.
pub const SnapshotInventoryProof = struct {
    manifest_dir: std.Io.Dir,
    manifest_path: []const u8,
    manifest_trailer_sha256: [bounded_file.sha256_len]u8,
    limits: bounded_file.Limits = .{},
};

pub const PersistenceBarrierPolicy = union(enum) {
    /// Safest default: Java did not encode the snapshot id, so the barrier
    /// cannot be reconstructed without external inventory data.
    reject_missing_snapshot_id,
    /// Omit the barrier while retaining later commands at their original
    /// identities. This can introduce an effective sequence gap.
    skip_missing_snapshot_id,
    /// Reconstruct only an exact terminal MATCHING/RISK pair bound to the
    /// converter-validated committed XCM2 manifest and trusted digest.
    supply_snapshot_id: SnapshotInventoryProof,
};

pub const ReplacementPolicy = enum {
    no_replace,
    replace,
};

pub const Output = struct {
    dir: std.Io.Dir,
    path: []const u8,
    /// Caller-visible label retained for deterministic diagnostics. The
    /// implementation never materializes this pathname: it uses
    /// a random private directory capability so caller-controlled aliases and
    /// other-user pathname replacement cannot redirect publication at `path`.
    ///
    /// This offline migration API requires exclusive process ownership of the
    /// destination directory. A hostile process running as the same OS user is
    /// outside the trust boundary: portable Zig/Darwin APIs cannot rename an
    /// already-open file descriptor, and such a process can mutate any
    /// user-owned artifact before or after publication. Source and destination
    /// inode checks still detect non-adversarial pathname replacement.
    staging_path: []const u8,
    replacement: ReplacementPolicy = .no_replace,
};

pub const Limits = struct {
    max_partitions: usize = 65_536,
    max_partition_bytes: usize = 1024 * 1024 * 1024,
    max_total_input_bytes: usize = 4 * 1024 * 1024 * 1024,
    max_total_entries: usize = 10_000_000,
    max_total_compressed_blocks: usize = 1_000_000,
    /// `initial_sequence` is overwritten from `partition_range` for each
    /// partition; every other decoder limit is honored directly.
    wire: java_journal_wire.Config = .{},
};

/// Explicit offline conversion request for an ordered raw Java `.ecj` family.
///
/// Java journals are not LZ4 frames. They are raw records and may contain
/// embedded `0xff | i32be compressed_len | i32be original_len | raw-LZ4`
/// batches, which are decoded only by `java_journal_wire`.
pub const Request = struct {
    input_dir: std.Io.Dir,
    partitions: []const Partition,
    partition_range: PartitionRange,
    post_snapshot_sequence_high_water: u64,
    execution_mode: journal.ExecutionMode,
    target: Target,
    gap_policy: GapPolicy = .reject,
    persistence_barrier_policy: PersistenceBarrierPolicy = .reject_missing_snapshot_id,
    limits: Limits = .{},
    output: Output,

    pub fn validate(self: Request) RequestValidationError!void {
        if (self.partitions.len == 0) return error.EmptyPartitionFamily;
        if (self.partitions.len > self.limits.max_partitions) return error.TooManyPartitions;
        if (self.partition_range.last_id < self.partition_range.first_id) {
            return error.InvalidPartitionRange;
        }
        const range_len = @as(usize, self.partition_range.last_id) -
            @as(usize, self.partition_range.first_id) + 1;
        if (range_len != self.partitions.len) return error.InvalidPartitionRange;
        for (self.partitions, 0..) |partition, index| {
            const expected = @as(usize, self.partition_range.first_id) + index;
            if (partition.id != expected) return error.PartitionIdMismatch;
            if (partition.path.len == 0) return error.EmptyPartitionPath;
        }
        if (self.output.path.len == 0) return error.EmptyOutputPath;
        if (self.output.staging_path.len == 0) return error.EmptyStagingPath;
        if (!validAtomicOutputBasename(self.output.path)) return error.InvalidOutputPath;
        if (!validAtomicOutputBasename(self.output.staging_path)) {
            return error.InvalidStagingPath;
        }
        if (std.mem.eql(u8, self.output.path, self.output.staging_path)) {
            return error.OutputPathEqualsStagingPath;
        }
        if (self.limits.max_partition_bytes == 0 or self.limits.max_total_input_bytes == 0) {
            return error.InvalidInputLimit;
        }
        if (self.limits.max_total_entries == 0) return error.InvalidEntryLimit;
        if (self.limits.max_total_compressed_blocks == 0) {
            return error.InvalidCompressedBlockLimit;
        }
        if (self.partition_range.sequence_before_first < 0 or
            self.post_snapshot_sequence_high_water > std.math.maxInt(i64))
        {
            return error.InvalidSequenceBoundary;
        }
        if (self.partition_range.sequence_before_first !=
            @as(i64, @intCast(self.post_snapshot_sequence_high_water)))
        {
            return error.PartitionFrontierDoesNotMatchSnapshot;
        }
        switch (self.target) {
            .fresh => |fresh| {
                if (!validShardCount(fresh.matching_count) or
                    !validShardCount(fresh.risk_count))
                {
                    return error.InvalidFreshTopology;
                }
                if (self.post_snapshot_sequence_high_water != 0) {
                    return error.FreshTargetRequiresZeroFrontier;
                }
            },
            .staged => {},
        }
        switch (self.persistence_barrier_policy) {
            .supply_snapshot_id => |proof| {
                if (!validRelativeProofPath(proof.manifest_path) or
                    proof.limits.max_manifest_bytes == 0 or
                    proof.limits.max_entries == 0)
                {
                    return error.InvalidPersistenceSnapshotProof;
                }
            },
            .reject_missing_snapshot_id, .skip_missing_snapshot_id => {},
        }
    }
};

pub const NativeFormat = enum {
    xcj3,
};

pub const ConversionSummary = struct {
    partition_count: usize,
    source_bytes: usize,
    decoded_entry_count: usize,
    skipped_persistence_barrier_count: usize,
    compressed_block_count: usize,
    replay: java_journal_replay.ReplaySummary,
    final_state_fingerprint: u64,
    native_format: NativeFormat,
};

const PreparedFamily = struct {
    decoded: java_journal_wire.DecodedJournal,
    source_bytes: usize,
    decoded_entry_count: usize,
    skipped_persistence_barrier_count: usize,
    compressed_block_count: usize,

    fn deinit(self: *PreparedFamily) void {
        self.decoded.deinit();
        self.* = undefined;
    }
};

/// Converts an ordered raw Java journal family into one atomically advertised
/// native journal. Every partition is bounded, decoded, family-bound, and policy
/// checked before staged kernel mutation begins. The output path is untouched
/// until the complete native journal has been written, synced, and read back
/// through a capability-owned atomic staging file.
pub fn convertJournalFamily(
    allocator: std.mem.Allocator,
    request: Request,
) !ConversionSummary {
    try request.validate();
    try preflightOutputLocation(request.output);

    var prepared = try prepareFamily(allocator, request);
    defer prepared.deinit();

    const effective_gap_count = try validateEffectiveSequencePolicy(
        prepared.decoded.entries.items,
        request.post_snapshot_sequence_high_water,
        request.gap_policy,
    );
    const native_format: NativeFormat = .xcj3;

    var fresh_target: ?kernel_mod.MultiShardKernelSet = null;
    defer if (fresh_target) |*target| target.deinit();
    const target = switch (request.target) {
        .fresh => |fresh| blk: {
            fresh_target = try kernel_mod.MultiShardKernelSet.initWithTopology(
                allocator,
                fresh.matching_count,
                fresh.risk_count,
                fresh.base_config,
            );
            fresh_target.?.restorePublicationSequence(
                request.post_snapshot_sequence_high_water,
            );
            break :blk &fresh_target.?;
        },
        .staged => |staged| staged,
    };
    try validateTargetFrontier(target, request.post_snapshot_sequence_high_water);

    var native = journal.CommandJournal.init(allocator);
    defer native.deinit();
    const replay_summary = try replayAndCapture(
        allocator,
        &prepared.decoded,
        target,
        request.execution_mode,
        request.post_snapshot_sequence_high_water,
        effective_gap_count,
        &native,
    );
    const fingerprint = try target.detailedHashFingerprint();

    try writeVerifiedAtomicOutput(
        allocator,
        request.output,
        native_format,
        &native,
    );

    log.info(
        "converted {d} Java journal partitions ({d} records) to {s}",
        .{ request.partitions.len, replay_summary.replayed, @tagName(native_format) },
    );
    return .{
        .partition_count = request.partitions.len,
        .source_bytes = prepared.source_bytes,
        .decoded_entry_count = prepared.decoded_entry_count,
        .skipped_persistence_barrier_count = prepared.skipped_persistence_barrier_count,
        .compressed_block_count = prepared.compressed_block_count,
        .replay = replay_summary,
        .final_state_fingerprint = fingerprint,
        .native_format = native_format,
    };
}

fn prepareFamily(allocator: std.mem.Allocator, request: Request) !PreparedFamily {
    var prepared: PreparedFamily = .{
        .decoded = .{
            .allocator = allocator,
            .last_sequence = @intCast(request.post_snapshot_sequence_high_water),
        },
        .source_bytes = 0,
        .decoded_entry_count = 0,
        .skipped_persistence_barrier_count = 0,
        .compressed_block_count = 0,
    };
    errdefer prepared.deinit();

    var source_frontier = request.partition_range.sequence_before_first;
    for (request.partitions) |partition| {
        const remaining_total = request.limits.max_total_input_bytes - prepared.source_bytes;
        const read_limit = @min(request.limits.max_partition_bytes, remaining_total);
        if (read_limit == 0) return error.TotalInputTooLarge;
        const bytes = bounded_file.readBoundedFileAlloc(
            allocator,
            request.input_dir,
            partition.path,
            read_limit,
        ) catch |err| switch (err) {
            error.FileTooLarge => return if (request.limits.max_partition_bytes <= remaining_total)
                error.PartitionTooLarge
            else
                error.TotalInputTooLarge,
            else => |e| return e,
        };
        defer allocator.free(bytes);
        prepared.source_bytes = std.math.add(usize, prepared.source_bytes, bytes.len) catch
            return error.TotalInputTooLarge;

        var wire_config = request.limits.wire;
        wire_config.initial_sequence = source_frontier;
        wire_config.max_file_bytes = @min(wire_config.max_file_bytes, read_limit);
        wire_config.max_entries = @min(
            wire_config.max_entries,
            request.limits.max_total_entries - prepared.decoded_entry_count,
        );
        wire_config.max_compressed_blocks = @min(
            wire_config.max_compressed_blocks,
            request.limits.max_total_compressed_blocks - prepared.compressed_block_count,
        );
        var decoded = try java_journal_wire.decode(allocator, bytes, wire_config);
        defer decoded.deinit();
        source_frontier = decoded.last_sequence;
        prepared.compressed_block_count = std.math.add(
            usize,
            prepared.compressed_block_count,
            decoded.compressed_block_count,
        ) catch return error.TooManyCompressedBlocks;
        if (prepared.compressed_block_count > request.limits.max_total_compressed_blocks) {
            return error.TooManyCompressedBlocks;
        }
        prepared.decoded_entry_count = std.math.add(
            usize,
            prepared.decoded_entry_count,
            decoded.entries.items.len,
        ) catch return error.TooManyEntries;
        if (prepared.decoded_entry_count > request.limits.max_total_entries) {
            return error.TooManyEntries;
        }

        for (decoded.entries.items) |source_entry| {
            const sequence = std.math.cast(u64, source_entry.java_sequence) orelse
                return error.InvalidJavaSequence;
            if (sequence <= request.post_snapshot_sequence_high_water) {
                return error.WrongSnapshotJournalFamily;
            }
            if (prepared.decoded.entries.items.len >= request.limits.max_total_entries) {
                return error.TooManyEntries;
            }
            try prepared.decoded.entries.append(allocator, source_entry);
        }
    }

    prepared.skipped_persistence_barrier_count = try applyPersistenceBarrierPolicy(
        prepared.decoded.allocator,
        &prepared.decoded.entries,
        request.persistence_barrier_policy,
        request.target,
    );

    prepared.decoded.last_sequence = if (prepared.decoded.entries.items.len == 0)
        @intCast(request.post_snapshot_sequence_high_water)
    else
        prepared.decoded.entries.items[prepared.decoded.entries.items.len - 1].java_sequence;
    return prepared;
}

fn applyPersistenceBarrierPolicy(
    allocator: std.mem.Allocator,
    entries: *std.ArrayListUnmanaged(java_journal_wire.Entry),
    policy: PersistenceBarrierPolicy,
    target: Target,
) !usize {
    var barrier_count: usize = 0;
    for (entries.items) |entry| {
        if (!entry.isReplayable()) barrier_count += 1;
    }
    if (barrier_count == 0) return 0;

    switch (policy) {
        .reject_missing_snapshot_id => return error.MissingPersistenceSnapshotId,
        .skip_missing_snapshot_id => {
            if (!entries.items[entries.items.len - 1].isReplayable()) {
                // A dropped terminal barrier has no later retained identity to
                // expose the gap. Publishing the shortened frontier would let
                // the next native command reuse a Java sequence that existed.
                return error.TerminalPersistenceBarrierCannotBeSkipped;
            }
            var write_index: usize = 0;
            for (entries.items) |entry| {
                if (!entry.isReplayable()) continue;
                entries.items[write_index] = entry;
                write_index += 1;
            }
            entries.items.len = write_index;
            return barrier_count;
        },
        .supply_snapshot_id => |proof| {
            var manifest = try bounded_file.readManifestFileAlloc(
                allocator,
                proof.manifest_dir,
                proof.manifest_path,
                proof.limits,
                .{},
            );
            defer manifest.deinit();
            if (!std.mem.eql(
                u8,
                &manifest.trailer_sha256,
                &proof.manifest_trailer_sha256,
            )) {
                return error.PersistenceSnapshotInventoryDigestMismatch;
            }

            const header = manifest.header;
            try validateSnapshotInventoryTarget(header, target);
            const snapshot_id = std.math.cast(i64, header.snapshot_id) orelse
                return error.InvalidPersistenceSnapshotProof;
            const matching_sequence = std.math.cast(i64, header.matching_command_seq) orelse
                return error.InvalidPersistenceSnapshotProof;
            const risk_sequence = std.math.cast(i64, header.risk_command_seq) orelse
                return error.InvalidPersistenceSnapshotProof;
            const timestamp = std.math.cast(i64, header.timestamp_ns) orelse
                return error.InvalidPersistenceSnapshotProof;
            if (snapshot_id <= 0 or matching_sequence <= 0) {
                return error.InvalidPersistenceSnapshotProof;
            }
            if (barrier_count != 2 or entries.items.len < 2) {
                return error.PersistenceBarrierProofMismatch;
            }
            const matching_index = entries.items.len - 2;
            const risk_index = entries.items.len - 1;
            const matching = &entries.items[matching_index];
            const risk = &entries.items[risk_index];
            if (matching.persistence_record != .matching_snapshot_id_not_encoded or
                risk.persistence_record != .risk_snapshot_id_not_encoded or
                matching.command.command != .persist_state_matching or
                risk.command.command != .persist_state_risk or
                matching.java_sequence != matching_sequence or
                risk.java_sequence != risk_sequence or
                matching.command.timestamp != timestamp or
                risk.command.timestamp != timestamp)
            {
                return error.PersistenceBarrierProofMismatch;
            }

            matching.command.order_id = snapshot_id;
            matching.persistence_record = .none;
            risk.command.order_id = snapshot_id;
            risk.persistence_record = .none;
            return 0;
        },
    }
}

fn validateSnapshotInventoryTarget(
    header: bounded_file.ManifestHeader,
    target: Target,
) !void {
    const expected_matching_count: usize, const expected_risk_count: usize = switch (target) {
        .fresh => |fresh| .{ fresh.matching_count, fresh.risk_count },
        .staged => |staged| .{ staged.matchingShardCount(), staged.riskShardCount() },
    };
    const matching_count_u32 = std.math.cast(u32, expected_matching_count) orelse
        return error.PersistenceSnapshotInventoryTopologyMismatch;
    const risk_count_u32 = std.math.cast(u32, expected_risk_count) orelse
        return error.PersistenceSnapshotInventoryTopologyMismatch;
    if (header.matching_count != matching_count_u32 or
        header.risk_count != risk_count_u32 or
        header.matching_mask != @as(u64, matching_count_u32) - 1 or
        header.risk_mask != @as(u64, risk_count_u32) - 1)
    {
        return error.PersistenceSnapshotInventoryTopologyMismatch;
    }

    const base_config = switch (target) {
        .fresh => |fresh| fresh.base_config,
        .staged => |staged| try stagedTargetBaseConfig(staged),
    };
    const expected_config = snapshot_store.kernelConfigSha256(
        base_config,
        header.matching_count,
        header.risk_count,
    );
    if (!std.mem.eql(u8, &header.config_sha256, &expected_config)) {
        return error.PersistenceSnapshotInventoryConfigMismatch;
    }
}

/// Reconstructs exactly the semantic subset committed by
/// `kernelConfigSha256` and rejects an internally inconsistent staged family.
/// Topology validation above guarantees both shard slices are non-empty.
fn stagedTargetBaseConfig(
    staged: *const kernel_mod.MultiShardKernelSet,
) error{PersistenceSnapshotInventoryConfigMismatch}!kernel_mod.Config {
    const matching = &staged.matching_shards[0];
    const risk_engine = staged.risk_shards[0].engine;
    const base_config: kernel_mod.Config = .{
        .orderbook_kind = matching.orderbook_kind,
        .send_l2_for_every_cmd = matching.send_l2_for_every_cmd,
        .l2_refresh_depth = matching.l2_refresh_depth,
        .risk = .{
            .ignore_risk_processing = risk_engine.cfg_ignore_risk_processing,
            .margin_trading_enabled = risk_engine.cfg_margin_trading_enabled,
        },
        .report_queries = matching.report_queries_cfg,
    };

    for (staged.matching_shards[1..]) |other| {
        if (other.orderbook_kind != base_config.orderbook_kind or
            other.send_l2_for_every_cmd != base_config.send_l2_for_every_cmd or
            other.l2_refresh_depth != base_config.l2_refresh_depth or
            !std.meta.eql(other.report_queries_cfg, base_config.report_queries))
        {
            return error.PersistenceSnapshotInventoryConfigMismatch;
        }
    }
    for (staged.risk_shards[1..]) |other| {
        if (other.engine.cfg_ignore_risk_processing !=
            base_config.risk.ignore_risk_processing or
            other.engine.cfg_margin_trading_enabled !=
                base_config.risk.margin_trading_enabled)
        {
            return error.PersistenceSnapshotInventoryConfigMismatch;
        }
    }
    return base_config;
}

fn validateEffectiveSequencePolicy(
    entries: []const java_journal_wire.Entry,
    target_sequence: u64,
    policy: GapPolicy,
) !usize {
    var previous = target_sequence;
    var gaps: usize = 0;
    for (entries) |entry| {
        if (!entry.isReplayable()) return error.NonReplayablePersistenceRecord;
        if (entry.java_sequence <= 0) return error.InvalidJavaSequence;
        const sequence = std.math.cast(u64, entry.java_sequence) orelse
            return error.InvalidJavaSequence;
        if (sequence <= previous) return error.NonMonotonicJavaSequence;
        const expected = std.math.add(u64, previous, 1) catch
            return error.NonMonotonicJavaSequence;
        if (sequence != expected) {
            if (policy == .reject) return error.SequenceGap;
            gaps += 1;
        }
        previous = sequence;
    }
    return gaps;
}

fn validateTargetFrontier(
    target: *const kernel_mod.MultiShardKernelSet,
    expected: u64,
) !void {
    if (target.next_seq != expected) return error.TargetSequenceMismatch;
    for (0..target.matchingShardCount()) |shard_id| {
        if (target.matchingObservedSequence(shard_id) != expected) {
            return error.TargetSequenceMismatch;
        }
    }
    for (0..target.riskShardCount()) |shard_id| {
        if (target.riskObservedSequence(shard_id) != expected) {
            return error.TargetSequenceMismatch;
        }
    }
}

/// `java_journal_replay` exposes the correct staged execution semantics and an
/// aggregate `ReplaySummary`, but its public functions intentionally discard
/// each `CommandResponse`. A converter must persist the actual result code, so
/// this result-sink variant follows the same preflight and routed/split calls
/// while reserving the complete native journal before the first mutation.
fn replayAndCapture(
    allocator: std.mem.Allocator,
    decoded: *const java_journal_wire.DecodedJournal,
    target: *kernel_mod.MultiShardKernelSet,
    mode: journal.ExecutionMode,
    target_sequence: u64,
    sequence_gap_count: usize,
    native: *journal.CommandJournal,
) !java_journal_replay.ReplaySummary {
    _ = try validateEffectiveSequencePolicy(decoded.entries.items, target_sequence, .preserve);
    try native.entries.ensureTotalCapacityPrecise(allocator, decoded.entries.items.len);

    var summary: java_journal_replay.ReplaySummary = .{
        .replayed = 0,
        .first_sequence = null,
        .last_sequence = null,
        .sequence_gap_count = sequence_gap_count,
        .non_success_result_count = 0,
    };
    for (decoded.entries.items) |entry| {
        const sequence: u64 = @intCast(entry.java_sequence);
        const input = entry.command;
        var command = input;
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

        native.entries.appendAssumeCapacity(.{
            .sequence = sequence,
            .mode = mode,
            .input = input,
            .result_code = response.result_code,
        });
        if (summary.first_sequence == null) summary.first_sequence = sequence;
        summary.last_sequence = sequence;
        summary.replayed += 1;
        switch (response.result_code) {
            .success, .accepted => {},
            else => summary.non_success_result_count += 1,
        }
    }
    return summary;
}

fn preflightOutputLocation(output: Output) !void {
    const io = io_context.current();
    if (output.replacement == .no_replace) {
        if (output.dir.statFile(io, output.path, .{})) |_| {
            return error.PathAlreadyExists;
        } else |err| switch (err) {
            error.FileNotFound => {},
            else => |e| return e,
        }
    }
}

fn writeVerifiedAtomicOutput(
    allocator: std.mem.Allocator,
    output: Output,
    native_format: NativeFormat,
    native: *const journal.CommandJournal,
) !void {
    const io = io_context.current();
    const encoded = encodeNativeJournalAlloc(
        allocator,
        native_format,
        native.entries.items,
    ) catch |err| switch (err) {
        // `Writer.Allocating` deliberately erases allocator errors into the
        // generic writer surface; recover the only possible drain failure.
        error.WriteFailed => return error.OutOfMemory,
        else => |e| return e,
    };
    defer allocator.free(encoded);

    // A random, owner-only directory is created inside the destination
    // filesystem and retained by capability. Publication renames from that
    // capability, so platforms lacking unnamed temporary files never depend
    // on a mutable staging pathname between verification and publication.
    var staging = try PrivateStagingDirectory.create(io, output.dir);
    defer staging.deinit(io);

    var staging_file = try staging.dir.createFile(io, PrivateStagingFileName, .{
        .read = true,
        .exclusive = true,
        .permissions = privateFilePermissions(),
    });
    var staging_file_open = true;
    defer if (staging_file_open) staging_file.close(io);
    staging.file_present = true;

    var file_buffer: [4096]u8 = undefined;
    var file_writer = staging_file.writer(io, &file_buffer);
    try file_writer.interface.writeAll(encoded);
    try file_writer.interface.flush();
    try staging_file.sync(io);
    try verifyOpenFileBytes(allocator, staging_file, encoded);

    // Keep the verified handle open through publication and bind the source
    // pathname back to that exact inode immediately before rename. The random
    // owner-only directory is the primary isolation boundary; this check also
    // rejects a same-user pathname replacement discovered before publication.
    const verified_stat = try staging_file.stat(io);
    const source_stat = try staging.dir.statFile(io, PrivateStagingFileName, .{
        .follow_symlinks = false,
    });
    if (verified_stat.inode != source_stat.inode) {
        return error.PrivateStagingFileIdentityChanged;
    }

    switch (output.replacement) {
        .no_replace => try staging.dir.renamePreserve(
            PrivateStagingFileName,
            output.dir,
            output.path,
            io,
        ),
        .replace => try staging.dir.rename(
            PrivateStagingFileName,
            output.dir,
            output.path,
            io,
        ),
    }
    staging.file_present = false;
    const published_stat = try output.dir.statFile(io, output.path, .{
        .follow_symlinks = false,
    });
    if (verified_stat.inode != published_stat.inode) {
        return error.PrivateStagingFileIdentityChanged;
    }
    // Publication becomes crash-durable only after the destination directory
    // records the link/rename. A sync failure is necessarily ambiguous: the
    // path may already be visible, so callers must inspect it before retrying.
    try syncOutputDirectory(output.dir);
    staging_file.close(io);
    staging_file_open = false;
}

const PrivateStagingPrefix = ".xcj-private-";
const PrivateStagingNameLength = PrivateStagingPrefix.len + 16;
const PrivateStagingFileName = "journal";
const PrivateStagingCreateFailurePoint = enum {
    before_open,
    after_open,
    after_directory_stat,
    after_path_stat,
};

const PrivateStagingDirectory = struct {
    parent: std.Io.Dir,
    dir: std.Io.Dir,
    name: [PrivateStagingNameLength]u8,
    inode: u64,
    dir_open: bool = true,
    file_present: bool = false,

    fn create(io: std.Io, parent: std.Io.Dir) !PrivateStagingDirectory {
        return createWithFailureForTesting(io, parent, null);
    }

    fn createWithFailureForTesting(
        io: std.Io,
        parent: std.Io.Dir,
        failure_point: ?PrivateStagingCreateFailurePoint,
    ) !PrivateStagingDirectory {
        var attempts: usize = 0;
        while (attempts < 128) : (attempts += 1) {
            var random: u64 = undefined;
            io.random(std.mem.asBytes(&random));
            var name: [PrivateStagingNameLength]u8 = undefined;
            @memcpy(name[0..PrivateStagingPrefix.len], PrivateStagingPrefix);
            name[PrivateStagingPrefix.len..].* = std.fmt.hex(random);
            parent.createDir(io, &name, privateDirectoryPermissions()) catch |err| switch (err) {
                error.PathAlreadyExists => continue,
                else => |e| return e,
            };
            var opened_dir: ?std.Io.Dir = null;
            errdefer cleanupUnreturnedPrivateStagingDirectory(
                io,
                parent,
                &name,
                opened_dir,
            );
            try injectPrivateStagingCreateFailure(failure_point, .before_open);
            const dir = try parent.openDir(io, &name, .{
                .follow_symlinks = false,
            });
            opened_dir = dir;
            try injectPrivateStagingCreateFailure(failure_point, .after_open);
            const dir_stat = try dir.stat(io);
            try injectPrivateStagingCreateFailure(failure_point, .after_directory_stat);
            const path_stat = try parent.statFile(io, &name, .{
                .follow_symlinks = false,
            });
            try injectPrivateStagingCreateFailure(failure_point, .after_path_stat);
            if (dir_stat.inode != path_stat.inode) {
                return error.PrivateStagingIdentityChanged;
            }
            return .{
                .parent = parent,
                .dir = dir,
                .name = name,
                .inode = dir_stat.inode,
            };
        }
        return error.PrivateStagingNameExhausted;
    }

    fn deinit(self: *PrivateStagingDirectory, io: std.Io) void {
        if (self.file_present) {
            self.dir.deleteFile(io, PrivateStagingFileName) catch {};
            self.file_present = false;
        }
        if (self.dir_open) {
            self.dir.close(io);
            self.dir_open = false;
        }
        const path_stat = self.parent.statFile(io, &self.name, .{
            .follow_symlinks = false,
        }) catch |err| switch (err) {
            error.FileNotFound => return,
            else => {
                log.warn("could not inspect private conversion staging directory: {}", .{err});
                return;
            },
        };
        if (path_stat.inode != self.inode) {
            log.warn("private conversion staging directory identity changed; leaving path untouched", .{});
            return;
        }
        self.parent.deleteDir(io, &self.name) catch |err| {
            log.warn("could not remove private conversion staging directory: {}", .{err});
        };
    }
};

fn injectPrivateStagingCreateFailure(
    configured: ?PrivateStagingCreateFailurePoint,
    current: PrivateStagingCreateFailurePoint,
) error{InjectedPrivateStagingCreateFailure}!void {
    if (configured == current) return error.InjectedPrivateStagingCreateFailure;
}

/// Cleans a directory created by a failed `PrivateStagingDirectory.create`
/// only while an open no-follow capability and the parent pathname still name
/// the same inode. If that identity cannot be re-established, leaving the
/// random owner-only directory behind is safer than deleting a replacement.
fn cleanupUnreturnedPrivateStagingDirectory(
    io: std.Io,
    parent: std.Io.Dir,
    name: []const u8,
    opened_dir: ?std.Io.Dir,
) void {
    var dir = opened_dir orelse parent.openDir(io, name, .{
        .follow_symlinks = false,
    }) catch return;
    var dir_open = true;
    defer if (dir_open) dir.close(io);

    const dir_stat = dir.stat(io) catch return;
    const path_stat = parent.statFile(io, name, .{
        .follow_symlinks = false,
    }) catch return;
    if (dir_stat.inode != path_stat.inode) return;

    dir.close(io);
    dir_open = false;
    parent.deleteDir(io, name) catch {};
}

fn privateDirectoryPermissions() std.Io.Dir.Permissions {
    return if (std.Io.Dir.Permissions.has_executable_bit)
        @enumFromInt(0o700)
    else
        .default_dir;
}

fn privateFilePermissions() std.Io.Dir.Permissions {
    return if (std.Io.Dir.Permissions.has_executable_bit)
        @enumFromInt(0o600)
    else
        .default_file;
}

fn verifyOpenFileBytes(
    allocator: std.mem.Allocator,
    file: std.Io.File,
    expected: []const u8,
) !void {
    const io = io_context.current();
    const actual_len = try file.length(io);
    if (actual_len != expected.len) return error.NativeJournalVerificationFailed;
    const actual = try allocator.alloc(u8, expected.len);
    defer allocator.free(actual);
    if (try file.readPositionalAll(io, actual, 0) != actual.len or
        !std.mem.eql(u8, expected, actual))
    {
        return error.NativeJournalVerificationFailed;
    }
}

fn encodeNativeJournalAlloc(
    allocator: std.mem.Allocator,
    native_format: NativeFormat,
    entries: []const journal.JournalEntry,
) ![]u8 {
    var output: std.Io.Writer.Allocating = .init(allocator);
    errdefer output.deinit();
    const writer = &output.writer;

    switch (native_format) {
        .xcj3 => {
            try writer.writeAll("XCJ3");
            for (entries) |entry| {
                var body_bytes: [90]u8 = undefined;
                var body_writer = std.Io.Writer.fixed(&body_bytes);
                try writeNativeEntryV2(&body_writer, entry);
                const body = body_writer.buffered();
                try writer.writeInt(u32, @intCast(body.len), .little);
                try writer.writeInt(
                    u64,
                    std.hash.Wyhash.hash(0x5843_4A33_4348_4B31, body),
                    .little,
                );
                try writer.writeAll(body);
            }
        },
    }
    return output.toOwnedSlice();
}

fn writeNativeEntryV2(writer: *std.Io.Writer, entry: journal.JournalEntry) !void {
    try writer.writeByte(@intFromEnum(entry.mode));
    try writer.writeInt(u64, entry.sequence, .little);
    try writer.writeInt(i32, entry.result_code.code(), .little);
    try writeNativeOrderCommand(writer, entry.input);
}

fn writeNativeOrderCommand(writer: *std.Io.Writer, value: cmd.OrderCommand) !void {
    try writer.writeInt(i8, value.command.code(), .little);
    try writer.writeInt(i64, value.order_id, .little);
    try writer.writeInt(i32, value.symbol, .little);
    try writer.writeInt(i64, value.price, .little);
    try writer.writeInt(i64, value.size, .little);
    try writer.writeInt(i64, value.reserve_bid_price, .little);
    if (value.action) |action| {
        try writer.writeByte(1);
        try writer.writeInt(i8, action.code(), .little);
    } else {
        try writer.writeByte(0);
    }
    if (value.order_type) |order_type| {
        try writer.writeByte(1);
        try writer.writeInt(i8, order_type.code(), .little);
    } else {
        try writer.writeByte(0);
    }
    try writer.writeInt(i64, value.uid, .little);
    try writer.writeInt(i64, value.timestamp, .little);
    try writer.writeInt(i32, value.user_cookie, .little);
    try writer.writeInt(i64, value.events_group, .little);
    try writer.writeInt(i32, value.service_flags, .little);
    try writer.writeInt(i32, value.result_code.code(), .little);
}

fn syncOutputDirectory(dir: std.Io.Dir) !void {
    const io = io_context.current();
    const parent = try dir.openFile(io, ".", .{
        .mode = .read_only,
        .allow_directory = true,
        .path_only = false,
    });
    defer parent.close(io);
    try parent.sync(io);
}

fn validAtomicOutputBasename(path: []const u8) bool {
    if (path.len == 0 or std.mem.eql(u8, path, ".") or std.mem.eql(u8, path, "..")) {
        return false;
    }
    return std.mem.eql(u8, path, std.fs.path.basename(path));
}

fn validRelativeProofPath(path: []const u8) bool {
    if (path.len == 0 or std.mem.eql(u8, path, ".") or std.mem.eql(u8, path, "..")) {
        return false;
    }
    var components = std.fs.path.componentIterator(path);
    if (components.root() != null) return false;
    while (components.next()) |component| {
        if (std.mem.eql(u8, component.name, "..")) return false;
    }
    return true;
}

fn validShardCount(count: usize) bool {
    return count != 0 and @popCount(count) == 1;
}

const TestRecord = union(enum) {
    add_user: struct {
        sequence: i64,
        uid: i64,
        timestamp: i64,
    },
    balance_adjustment: struct {
        sequence: i64,
        uid: i64,
        currency: i32,
        transaction_id: i64,
        amount: i64,
        timestamp: i64,
    },
    reset: struct {
        sequence: i64,
        timestamp: i64,
    },
    persist_matching: struct {
        sequence: i64,
        timestamp: i64,
    },
    persist_risk: struct {
        sequence: i64,
        timestamp: i64,
    },
};

fn appendTestRecord(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: TestRecord,
) !void {
    const command_type: cmd.OrderCommandType = switch (record) {
        .add_user => .add_user,
        .balance_adjustment => .balance_adjustment,
        .reset => .reset,
        .persist_matching => .persist_state_matching,
        .persist_risk => .persist_state_risk,
    };
    const sequence: i64, const timestamp: i64 = switch (record) {
        .add_user => |value| .{ value.sequence, value.timestamp },
        .balance_adjustment => |value| .{ value.sequence, value.timestamp },
        .reset => |value| .{ value.sequence, value.timestamp },
        .persist_matching => |value| .{ value.sequence, value.timestamp },
        .persist_risk => |value| .{ value.sequence, value.timestamp },
    };
    try out.append(allocator, @bitCast(command_type.code()));
    try appendTestInt(allocator, out, i64, sequence);
    try appendTestInt(allocator, out, i64, timestamp);
    try appendTestInt(allocator, out, i32, 70 + @as(i32, @intCast(sequence)));
    try appendTestInt(allocator, out, i64, 80 + sequence);

    switch (record) {
        .add_user => |value| try appendTestInt(allocator, out, i64, value.uid),
        .balance_adjustment => |value| {
            try appendTestInt(allocator, out, i64, value.uid);
            try appendTestInt(allocator, out, i32, value.currency);
            try appendTestInt(allocator, out, i64, value.transaction_id);
            try appendTestInt(allocator, out, i64, value.amount);
            try out.append(allocator, 0); // BalanceAdjustmentType.ADJUSTMENT
        },
        .reset, .persist_matching, .persist_risk => {},
    }
}

fn appendTestCompressedBatch(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    records: []const TestRecord,
) !void {
    var raw: std.ArrayListUnmanaged(u8) = .empty;
    defer raw.deinit(allocator);
    for (records) |record| try appendTestRecord(allocator, &raw, record);
    const compressed = try java_binary_wire.encodeRawLz4LiteralOnly(allocator, raw.items);
    defer allocator.free(compressed);

    try out.append(allocator, 0xff);
    try appendTestInt(allocator, out, i32, @intCast(compressed.len));
    try appendTestInt(allocator, out, i32, @intCast(raw.items.len));
    try out.appendSlice(allocator, compressed);
}

fn appendTestInt(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    comptime T: type,
    value: T,
) !void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .big);
    try out.appendSlice(allocator, &bytes);
}

fn writeTestFile(dir: std.Io.Dir, path: []const u8, bytes: []const u8) !void {
    const io = io_context.current();
    const file = try dir.createFile(io, path, .{ .truncate = true });
    defer file.close(io);
    try file.writeStreamingAll(io, bytes);
}

fn testRequest(
    dir: std.Io.Dir,
    partitions: []const Partition,
    target: Target,
    mode: journal.ExecutionMode,
    high_water: u64,
    output_path: []const u8,
    staging_path: []const u8,
) Request {
    return .{
        .input_dir = dir,
        .partitions = partitions,
        .partition_range = .{
            .first_id = partitions[0].id,
            .last_id = partitions[partitions.len - 1].id,
            .sequence_before_first = @intCast(high_water),
        },
        .post_snapshot_sequence_high_water = high_water,
        .execution_mode = mode,
        .target = target,
        .output = .{
            .dir = dir,
            .path = output_path,
            .staging_path = staging_path,
        },
    };
}

fn writeTestSnapshotInventory(
    dir: std.Io.Dir,
    path: []const u8,
    snapshot_id: u64,
    matching_sequence: u64,
    risk_sequence: u64,
    timestamp_ns: u64,
) !SnapshotInventoryProof {
    return writeTestSnapshotInventoryWithTarget(
        dir,
        path,
        snapshot_id,
        matching_sequence,
        risk_sequence,
        timestamp_ns,
        1,
        1,
        .{},
    );
}

fn writeTestSnapshotInventoryWithTarget(
    dir: std.Io.Dir,
    path: []const u8,
    snapshot_id: u64,
    matching_sequence: u64,
    risk_sequence: u64,
    timestamp_ns: u64,
    matching_count: u32,
    risk_count: u32,
    base_config: kernel_mod.Config,
) !SnapshotInventoryProof {
    const entry_count = try std.math.add(u32, matching_count, risk_count);
    const matching_count_usize: usize = @intCast(matching_count);
    const risk_count_usize: usize = @intCast(risk_count);
    const header: bounded_file.ManifestHeader = .{
        .snapshot_id = snapshot_id,
        .matching_command_seq = matching_sequence,
        .risk_command_seq = risk_sequence,
        .publication_high_water = risk_sequence,
        .timestamp_ns = timestamp_ns,
        .matching_count = matching_count,
        .risk_count = risk_count,
        .matching_mask = @as(u64, matching_count) - 1,
        .risk_mask = @as(u64, risk_count) - 1,
        .aggregate_state_hash = 0xA11C_E001,
        .config_sha256 = snapshot_store.kernelConfigSha256(
            base_config,
            matching_count,
            risk_count,
        ),
        .exchange_id_sha256 = [_]u8{0x22} ** bounded_file.sha256_len,
        .bundle_id = [_]u8{0x33} ** 16,
        .entry_count = entry_count,
    };
    const entries = try std.testing.allocator.alloc(
        bounded_file.ManifestEntry,
        @intCast(entry_count),
    );
    defer std.testing.allocator.free(entries);
    for (0..matching_count_usize) |shard_id| {
        entries[shard_id] = .{
            .module_kind = .matching_engine,
            .shard_id = @intCast(shard_id),
            .command_seq = matching_sequence,
            .byte_len = bounded_file.payload_header_len,
            .payload_sha256 = [_]u8{0x44} ** bounded_file.sha256_len,
            .module_state_hash = 0xA11C_E002 + @as(u64, @intCast(shard_id)),
        };
    }
    for (0..risk_count_usize) |shard_id| {
        entries[matching_count_usize + shard_id] = .{
            .module_kind = .risk_engine,
            .shard_id = @intCast(shard_id),
            .command_seq = risk_sequence,
            .byte_len = bounded_file.payload_header_len,
            .payload_sha256 = [_]u8{0x55} ** bounded_file.sha256_len,
            .module_state_hash = 0xA11C_F003 + @as(u64, @intCast(shard_id)),
        };
    }
    try bounded_file.writeManifestFileAtomic(
        std.testing.allocator,
        dir,
        path,
        header,
        entries,
        .{},
    );
    var manifest = try bounded_file.readManifestFileAlloc(
        std.testing.allocator,
        dir,
        path,
        .{},
        .{},
    );
    defer manifest.deinit();
    return .{
        .manifest_dir = dir,
        .manifest_path = path,
        .manifest_trailer_sha256 = manifest.trailer_sha256,
    };
}

fn buildBaseline(
    allocator: std.mem.Allocator,
    matching_count: usize,
    risk_count: usize,
    mode: journal.ExecutionMode,
) !kernel_mod.MultiShardKernelSet {
    var result = try kernel_mod.MultiShardKernelSet.initWithTopology(
        allocator,
        matching_count,
        risk_count,
        .{},
    );
    errdefer result.deinit();
    var add_user: cmd.OrderCommand = .{
        .command = .add_user,
        .uid = 101,
        .order_id = -1,
        .symbol = -1,
        .timestamp = 1_001,
    };
    var response = switch (mode) {
        .routed => try result.executeRoutedAtSequence(.fromInt(1), &add_user),
        .split => try result.executeSplitAtSequence(.fromInt(1), &add_user),
    };
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    return result;
}

fn replayNativeIntoSet(
    loaded: *const journal.CommandJournal,
    target: *kernel_mod.MultiShardKernelSet,
) !void {
    for (loaded.entries.items) |entry| {
        var command = entry.input;
        var response = switch (entry.mode) {
            .routed => try target.executeRoutedAtSequence(.fromInt(entry.sequence), &command),
            .split => try target.executeSplitAtSequence(.fromInt(entry.sequence), &command),
        };
        defer response.deinit(target.allocator);
        try std.testing.expectEqual(entry.sequence, response.sequence);
        try std.testing.expectEqual(entry.result_code, response.result_code);
    }
}

fn expectPathMissing(dir: std.Io.Dir, path: []const u8) !void {
    try std.testing.expectError(
        error.FileNotFound,
        dir.access(io_context.current(), path, .{}),
    );
}

fn countPrivateStagingDirectories(dir: std.Io.Dir) !usize {
    const io = io_context.current();
    var iterable = try dir.openDir(io, ".", .{ .iterate = true });
    defer iterable.close(io);
    var iterator = iterable.iterate();
    var count: usize = 0;
    while (try iterator.next(io)) |entry| {
        if (entry.kind == .directory and
            std.mem.startsWith(u8, entry.name, PrivateStagingPrefix))
        {
            count += 1;
        }
    }
    return count;
}

fn expectConversionFailure(request: Request) !void {
    if (convertJournalFamily(std.testing.allocator, request)) |_| {
        return error.TestExpectedError;
    } else |_| {}
    try expectPathMissing(request.output.dir, request.output.path);
    try expectPathMissing(request.output.dir, request.output.staging_path);
}

test "private staging creation failures remove only the owned directory" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    for (std.enums.values(PrivateStagingCreateFailurePoint)) |failure_point| {
        try std.testing.expectError(
            error.InjectedPrivateStagingCreateFailure,
            PrivateStagingDirectory.createWithFailureForTesting(
                io_context.current(),
                tmp.dir,
                failure_point,
            ),
        );
        try std.testing.expectEqual(@as(usize, 0), try countPrivateStagingDirectories(tmp.dir));
    }
}

test "Java journal converter handles raw and compressed partitions after a snapshot in routed and split modes" {
    const allocator = std.testing.allocator;
    for ([_]journal.ExecutionMode{ .routed, .split }) |mode| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();

        var raw: std.ArrayListUnmanaged(u8) = .empty;
        defer raw.deinit(allocator);
        try appendTestRecord(allocator, &raw, .{ .balance_adjustment = .{
            .sequence = 2,
            .uid = 101,
            .currency = 15,
            .transaction_id = 501,
            .amount = 12_345,
            .timestamp = 1_002,
        } });
        try writeTestFile(tmp.dir, "part-0.ecj", raw.items);

        var compressed: std.ArrayListUnmanaged(u8) = .empty;
        defer compressed.deinit(allocator);
        try appendTestCompressedBatch(allocator, &compressed, &.{.{ .add_user = .{
            .sequence = 3,
            .uid = 101,
            .timestamp = 1_003,
        } }});
        try writeTestFile(tmp.dir, "part-1.ecj", compressed.items);

        var converted_target = try buildBaseline(allocator, 2, 1, mode);
        defer converted_target.deinit();
        var replay_target = try buildBaseline(allocator, 2, 1, mode);
        defer replay_target.deinit();

        const partitions = [_]Partition{
            .{ .id = 0, .path = "part-0.ecj" },
            .{ .id = 1, .path = "part-1.ecj" },
        };
        const request = testRequest(
            tmp.dir,
            &partitions,
            .{ .staged = &converted_target },
            mode,
            1,
            "converted.xcj",
            "converted.stage",
        );
        const summary = try convertJournalFamily(allocator, request);
        try std.testing.expectEqual(NativeFormat.xcj3, summary.native_format);
        try std.testing.expectEqual(@as(usize, 2), summary.partition_count);
        try std.testing.expectEqual(@as(usize, 2), summary.decoded_entry_count);
        try std.testing.expectEqual(@as(usize, 1), summary.compressed_block_count);
        try std.testing.expectEqual(@as(usize, 2), summary.replay.replayed);
        try std.testing.expectEqual(@as(usize, 1), summary.replay.non_success_result_count);
        try std.testing.expectEqual(@as(?u64, 2), summary.replay.first_sequence);
        try std.testing.expectEqual(@as(?u64, 3), summary.replay.last_sequence);

        var loaded = try journal.CommandJournal.readFromFile(
            allocator,
            tmp.dir,
            "converted.xcj",
        );
        defer loaded.deinit();
        try std.testing.expectEqual(@as(usize, 2), loaded.entries.items.len);
        try std.testing.expectEqual(mode, loaded.entries.items[0].mode);
        try std.testing.expectEqual(@as(i64, 1_002), loaded.entries.items[0].input.timestamp);
        try std.testing.expectEqual(cmd.CommandResultCode.success, loaded.entries.items[0].result_code);
        try std.testing.expectEqual(
            cmd.CommandResultCode.user_mgmt_user_already_exists,
            loaded.entries.items[1].result_code,
        );

        try replayNativeIntoSet(&loaded, &replay_target);
        try std.testing.expectEqual(converted_target.next_seq, replay_target.next_seq);
        try std.testing.expectEqual(
            try converted_target.detailedHashFingerprint(),
            try replay_target.detailedHashFingerprint(),
        );
        try std.testing.expectEqual(
            summary.final_state_fingerprint,
            try replay_target.detailedHashFingerprint(),
        );
    }
}

test "Java journal converter replays committed DiskSerializationProcessor fixture after XCM2 conversion" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io_context.current(), "java-input");

    const fixture_prefix = "../fixtures/java-persistence/v1/";
    const me_name = "JAVAPERSISTV1_snapshot_4242424242_ME0.ecs";
    const re_name = "JAVAPERSISTV1_snapshot_4242424242_RE0.ecs";
    const journal_name = "JAVAPERSISTV1_journal_4242424242_0001.ecj";
    try writeTestFile(tmp.dir, "java-input/" ++ me_name, @embedFile(fixture_prefix ++ me_name));
    try writeTestFile(tmp.dir, "java-input/" ++ re_name, @embedFile(fixture_prefix ++ re_name));
    try writeTestFile(
        tmp.dir,
        "java-input/" ++ journal_name,
        @embedFile(fixture_prefix ++ journal_name),
    );

    const base_config: kernel_mod.Config = .{
        .orderbook_kind = .direct,
        .send_l2_for_every_cmd = true,
        .risk = .{ .margin_trading_enabled = true },
    };
    const snapshot_request: java_persistence_converter.Request = .{
        .input_dir = tmp.dir,
        .output_dir = tmp.dir,
        .input_storage_folder = "java-input",
        .output_storage_folder = "native-output",
        .exchange_id = "JAVAPERSISTV1",
        .snapshot_id = 4_242_424_242,
        .matching_count = 1,
        .risk_count = 1,
        .base_config = base_config,
        .matching_command_seq = 16,
        .risk_command_seq = 17,
        .timestamp_ns = 1_700_000_000_000_000_020,
    };
    _ = try java_persistence_converter.convertSnapshotFamily(allocator, snapshot_request);
    var target = try snapshot_store.restoreFresh(
        allocator,
        tmp.dir,
        snapshot_request.output_storage_folder,
        snapshot_request.exchange_id,
        .{
            .identity = snapshot_request.bundleIdentity(),
            .matching_count = snapshot_request.matching_count,
            .risk_count = snapshot_request.risk_count,
            .base_config = base_config,
        },
    );
    defer target.deinit();

    const partitions = [_]Partition{.{
        .id = 1,
        .path = "java-input/" ++ journal_name,
    }};
    const request: Request = .{
        .input_dir = tmp.dir,
        .partitions = &partitions,
        .partition_range = .{
            .first_id = 1,
            .last_id = 1,
            .sequence_before_first = 17,
        },
        .post_snapshot_sequence_high_water = 17,
        .execution_mode = .routed,
        .target = .{ .staged = &target },
        .gap_policy = .preserve,
        .output = .{
            .dir = tmp.dir,
            .path = "converted-fixture.xcj3",
            .staging_path = "converted-fixture.stage",
        },
    };
    const summary = try convertJournalFamily(allocator, request);
    try std.testing.expectEqual(@as(usize, 1), summary.partition_count);
    try std.testing.expectEqual(@as(usize, 136), summary.source_bytes);
    try std.testing.expectEqual(@as(usize, 2), summary.decoded_entry_count);
    try std.testing.expectEqual(@as(usize, 2), summary.replay.replayed);
    try std.testing.expectEqual(@as(usize, 1), summary.replay.sequence_gap_count);
    try std.testing.expectEqual(@as(?u64, 19), summary.replay.first_sequence);
    try std.testing.expectEqual(@as(?u64, 20), summary.replay.last_sequence);
    try std.testing.expectEqual(@as(u64, 20), target.next_seq);

    try std.testing.expectEqual(@as(?i64, 252_285), try target.queryUserBalance(7_001, 392));
    try std.testing.expectEqual(@as(?i64, 149_910), try target.queryUserBalance(7_002, 392));
    var totals = try target.aggregateTotalCurrencyBalance();
    defer totals.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 402_195), totals.account_balances.get(392));
    try std.testing.expectEqual(@as(?i64, -402_345), totals.adjustments.get(392));
    try std.testing.expectEqual(@as(?i64, 150), totals.fees.get(392));
    try std.testing.expectEqual(@as(?i64, 30), totals.open_interest_long.get(5_991));
    try std.testing.expectEqual(@as(?i64, 30), totals.open_interest_short.get(5_991));

    var taker_response = try target.queryReport(.{ .single_user = 7_002 });
    defer taker_response.deinit(allocator);
    const taker = taker_response.single_user;
    try std.testing.expectEqual(@as(usize, 1), taker.orders.items.len);
    try std.testing.expectEqual(@as(i64, 103), taker.orders.items[0].order_id);
    try std.testing.expectEqual(@as(i64, 5), taker.positions.get(5_991).?.pending_buy_size);

    var book_response = try target.queryReport(.{
        .order_book = .{ .symbol = 5_991, .depth = 8 },
    });
    defer book_response.deinit(allocator);
    const book = book_response.order_book;
    try std.testing.expectEqual(@as(usize, 1), book.asks.items.len);
    try std.testing.expectEqual(@as(usize, 1), book.bids.items.len);
    try std.testing.expectEqual(@as(i64, 10_600), book.bids.items[0].price);
    try std.testing.expectEqual(@as(i64, 5), book.bids.items[0].volume);

    var native = try journal.CommandJournal.readFromFile(
        allocator,
        tmp.dir,
        request.output.path,
    );
    defer native.deinit();
    try std.testing.expectEqual(@as(usize, 2), native.entries.items.len);
    try std.testing.expectEqual(@as(u64, 19), native.entries.items[0].sequence);
    try std.testing.expectEqual(@as(u64, 20), native.entries.items[1].sequence);
    try std.testing.expectEqual(cmd.CommandResultCode.success, native.entries.items[0].result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, native.entries.items[1].result_code);
}

test "Java journal converter binds the first partition frontier exactly to the snapshot" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var raw: std.ArrayListUnmanaged(u8) = .empty;
    defer raw.deinit(allocator);
    try appendTestRecord(allocator, &raw, .{ .reset = .{
        .sequence = 1,
        .timestamp = 1_101,
    } });
    try writeTestFile(tmp.dir, "wrong-family.ecj", raw.items);
    const partitions = [_]Partition{.{ .id = 4, .path = "wrong-family.ecj" }};

    var target = try buildBaseline(allocator, 1, 1, .routed);
    defer target.deinit();
    const before = try target.detailedHashFingerprint();
    var request = testRequest(
        tmp.dir,
        &partitions,
        .{ .staged = &target },
        .routed,
        1,
        "wrong-family.xcj",
        "wrong-family.stage",
    );

    // A family that repeats the snapshot high-water is rejected by the wire
    // decoder rather than silently filtering the overlap.
    try std.testing.expectError(
        error.SequenceNotIncreasing,
        convertJournalFamily(allocator, request),
    );
    try std.testing.expectEqual(before, try target.detailedHashFingerprint());
    try expectPathMissing(tmp.dir, request.output.path);

    request.partition_range.sequence_before_first = 0;
    try std.testing.expectError(
        error.PartitionFrontierDoesNotMatchSnapshot,
        convertJournalFamily(allocator, request),
    );
    try std.testing.expectEqual(before, try target.detailedHashFingerprint());
}

test "Java journal converter preserves validated sparse identities in appendable XCJ3" {
    const allocator = std.testing.allocator;
    var raw: std.ArrayListUnmanaged(u8) = .empty;
    defer raw.deinit(allocator);
    try appendTestRecord(allocator, &raw, .{ .add_user = .{
        .sequence = 1,
        .uid = 501,
        .timestamp = 2_001,
    } });
    try appendTestRecord(allocator, &raw, .{ .balance_adjustment = .{
        .sequence = 3,
        .uid = 501,
        .currency = 11,
        .transaction_id = 601,
        .amount = 900,
        .timestamp = 2_003,
    } });
    const partitions = [_]Partition{.{ .id = 7, .path = "sparse.ecj" }};

    {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try writeTestFile(tmp.dir, "sparse.ecj", raw.items);
        var request = testRequest(
            tmp.dir,
            &partitions,
            .{ .fresh = .{ .matching_count = 1, .risk_count = 1 } },
            .routed,
            0,
            "reject.xcj",
            "reject.stage",
        );
        request.partition_range.sequence_before_first = 0;
        try std.testing.expectError(error.SequenceGap, convertJournalFamily(allocator, request));
        try expectPathMissing(tmp.dir, request.output.path);
    }

    {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try writeTestFile(tmp.dir, "sparse.ecj", raw.items);
        var request = testRequest(
            tmp.dir,
            &partitions,
            .{ .fresh = .{ .matching_count = 1, .risk_count = 1 } },
            .routed,
            0,
            "sparse.xcj",
            "sparse.stage",
        );
        request.gap_policy = .preserve;
        const summary = try convertJournalFamily(allocator, request);
        try std.testing.expectEqual(NativeFormat.xcj3, summary.native_format);
        try std.testing.expectEqual(@as(usize, 1), summary.replay.sequence_gap_count);

        const file = try tmp.dir.openFile(io_context.current(), request.output.path, .{});
        defer file.close(io_context.current());
        var magic: [4]u8 = undefined;
        try std.testing.expectEqual(@as(usize, 4), try file.readPositionalAll(
            io_context.current(),
            &magic,
            0,
        ));
        try std.testing.expectEqualStrings("XCJ3", &magic);

        var appender = try journal.DurableJournalAppender.open(
            allocator,
            tmp.dir,
            request.output.path,
        );
        defer appender.deinit();
        try std.testing.expectEqual(@as(?u64, 3), appender.lastSequence());
        const next = journal.JournalEntry{
            .sequence = 4,
            .input = .{
                .command = .reset,
                .timestamp = 2_004,
            },
            .result_code = .success,
        };
        try appender.append(next);
    }
}

test "Java journal converter rejects duplicate and regressing cross-partition sequences before mutation" {
    const allocator = std.testing.allocator;
    for ([_]i64{ 1, 0 }) |bad_sequence| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var first: std.ArrayListUnmanaged(u8) = .empty;
        defer first.deinit(allocator);
        try appendTestRecord(allocator, &first, .{ .reset = .{
            .sequence = 1,
            .timestamp = 3_001,
        } });
        try writeTestFile(tmp.dir, "first.ecj", first.items);
        var second: std.ArrayListUnmanaged(u8) = .empty;
        defer second.deinit(allocator);
        try appendTestRecord(allocator, &second, .{ .reset = .{
            .sequence = bad_sequence,
            .timestamp = 3_002,
        } });
        try writeTestFile(tmp.dir, "second.ecj", second.items);

        var target = try kernel_mod.MultiShardKernelSet.initWithTopology(allocator, 1, 1, .{});
        defer target.deinit();
        const before = try target.detailedHashFingerprint();
        const partitions = [_]Partition{
            .{ .id = 2, .path = "first.ecj" },
            .{ .id = 3, .path = "second.ecj" },
        };
        const request = testRequest(
            tmp.dir,
            &partitions,
            .{ .staged = &target },
            .routed,
            0,
            "bad-sequence.xcj",
            "bad-sequence.stage",
        );
        try std.testing.expectError(
            error.SequenceNotIncreasing,
            convertJournalFamily(allocator, request),
        );
        try std.testing.expectEqual(@as(u64, 0), target.next_seq);
        try std.testing.expectEqual(before, try target.detailedHashFingerprint());
        try expectPathMissing(tmp.dir, request.output.path);
        try expectPathMissing(tmp.dir, request.output.staging_path);
    }
}

test "Java journal converter rejects state-less fresh frontiers and invalid output labels" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const partitions = [_]Partition{.{ .id = 0, .path = "unused.ecj" }};

    var request = testRequest(
        tmp.dir,
        &partitions,
        .{ .fresh = .{ .matching_count = 1, .risk_count = 1 } },
        .routed,
        1,
        "fresh.xcj",
        "fresh.stage",
    );
    try std.testing.expectError(
        error.FreshTargetRequiresZeroFrontier,
        convertJournalFamily(allocator, request),
    );
    try expectPathMissing(tmp.dir, request.output.path);

    request.post_snapshot_sequence_high_water = 0;
    request.partition_range.sequence_before_first = 0;
    request.output.staging_path = "./fresh.xcj";
    try std.testing.expectError(
        error.InvalidStagingPath,
        convertJournalFamily(allocator, request),
    );
    try expectPathMissing(tmp.dir, request.output.path);

    request.output.staging_path = "fresh.stage";
    request.persistence_barrier_policy = .{ .supply_snapshot_id = .{
        .manifest_dir = tmp.dir,
        .manifest_path = "../outside.ecm",
        .manifest_trailer_sha256 = [_]u8{0} ** bounded_file.sha256_len,
    } };
    try std.testing.expectError(
        error.InvalidPersistenceSnapshotProof,
        convertJournalFamily(allocator, request),
    );
    try expectPathMissing(tmp.dir, request.output.path);
}

test "Java journal converter rejects terminal skipped barriers and negative retained timestamps" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var terminal: std.ArrayListUnmanaged(u8) = .empty;
    defer terminal.deinit(allocator);
    try appendTestRecord(allocator, &terminal, .{ .add_user = .{
        .sequence = 1,
        .uid = 9_101,
        .timestamp = 4_101,
    } });
    try appendTestRecord(allocator, &terminal, .{ .persist_matching = .{
        .sequence = 2,
        .timestamp = 4_102,
    } });
    try writeTestFile(tmp.dir, "terminal.ecj", terminal.items);
    const terminal_partitions = [_]Partition{.{ .id = 0, .path = "terminal.ecj" }};
    var terminal_request = testRequest(
        tmp.dir,
        &terminal_partitions,
        .{ .fresh = .{ .matching_count = 1, .risk_count = 1 } },
        .split,
        0,
        "terminal.xcj",
        "terminal.stage",
    );
    terminal_request.persistence_barrier_policy = .skip_missing_snapshot_id;
    try std.testing.expectError(
        error.TerminalPersistenceBarrierCannotBeSkipped,
        convertJournalFamily(allocator, terminal_request),
    );
    try expectPathMissing(tmp.dir, terminal_request.output.path);
    try expectPathMissing(tmp.dir, terminal_request.output.staging_path);

    var middle: std.ArrayListUnmanaged(u8) = .empty;
    defer middle.deinit(allocator);
    try appendTestRecord(allocator, &middle, .{ .add_user = .{
        .sequence = 1,
        .uid = 9_103,
        .timestamp = 4_103,
    } });
    try appendTestRecord(allocator, &middle, .{ .persist_matching = .{
        .sequence = 2,
        .timestamp = 4_104,
    } });
    try appendTestRecord(allocator, &middle, .{ .reset = .{
        .sequence = 3,
        .timestamp = 4_105,
    } });
    try writeTestFile(tmp.dir, "middle.ecj", middle.items);
    const middle_partitions = [_]Partition{.{ .id = 0, .path = "middle.ecj" }};
    var middle_request = testRequest(
        tmp.dir,
        &middle_partitions,
        .{ .fresh = .{ .matching_count = 1, .risk_count = 1 } },
        .routed,
        0,
        "middle.xcj",
        "middle.stage",
    );
    middle_request.persistence_barrier_policy = .skip_missing_snapshot_id;
    middle_request.gap_policy = .preserve;
    const middle_summary = try convertJournalFamily(allocator, middle_request);
    try std.testing.expectEqual(@as(usize, 1), middle_summary.skipped_persistence_barrier_count);
    try std.testing.expectEqual(@as(usize, 1), middle_summary.replay.sequence_gap_count);
    try std.testing.expectEqual(NativeFormat.xcj3, middle_summary.native_format);

    var negative: std.ArrayListUnmanaged(u8) = .empty;
    defer negative.deinit(allocator);
    try appendTestRecord(allocator, &negative, .{ .persist_matching = .{
        .sequence = 1,
        .timestamp = -1,
    } });
    try appendTestRecord(allocator, &negative, .{ .persist_risk = .{
        .sequence = 2,
        .timestamp = -1,
    } });
    try writeTestFile(tmp.dir, "negative.ecj", negative.items);
    const negative_partitions = [_]Partition{.{ .id = 0, .path = "negative.ecj" }};
    var negative_request = testRequest(
        tmp.dir,
        &negative_partitions,
        .{ .fresh = .{ .matching_count = 1, .risk_count = 1 } },
        .routed,
        0,
        "negative.xcj",
        "negative.stage",
    );
    negative_request.persistence_barrier_policy = .{ .supply_snapshot_id = try writeTestSnapshotInventory(tmp.dir, "negative.ecm", 9_102, 1, 2, 0) };
    try std.testing.expectError(
        error.PersistenceBarrierProofMismatch,
        convertJournalFamily(allocator, negative_request),
    );
    try expectPathMissing(tmp.dir, negative_request.output.path);
    try expectPathMissing(tmp.dir, negative_request.output.staging_path);
}

test "Java journal converter applies explicit missing persistence snapshot-id policy" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var raw: std.ArrayListUnmanaged(u8) = .empty;
    defer raw.deinit(allocator);
    try appendTestRecord(allocator, &raw, .{ .persist_matching = .{
        .sequence = 1,
        .timestamp = 4_001,
    } });
    try appendTestRecord(allocator, &raw, .{ .persist_risk = .{
        .sequence = 2,
        .timestamp = 4_001,
    } });
    try writeTestFile(tmp.dir, "barrier.ecj", raw.items);
    const partitions = [_]Partition{.{ .id = 0, .path = "barrier.ecj" }};

    const rejected = testRequest(
        tmp.dir,
        &partitions,
        .{ .fresh = .{ .matching_count = 1, .risk_count = 1 } },
        .split,
        0,
        "barrier-reject.xcj",
        "barrier-reject.stage",
    );
    try std.testing.expectError(
        error.MissingPersistenceSnapshotId,
        convertJournalFamily(allocator, rejected),
    );
    try expectPathMissing(tmp.dir, rejected.output.path);

    const inventory = try writeTestSnapshotInventory(
        tmp.dir,
        "barrier.ecm",
        9_001,
        1,
        2,
        4_001,
    );
    var wrong_digest = inventory;
    wrong_digest.manifest_trailer_sha256[0] ^= 0xFF;
    var unverified = rejected;
    unverified.persistence_barrier_policy = .{ .supply_snapshot_id = wrong_digest };
    unverified.output.path = "barrier-unverified.xcj";
    unverified.output.staging_path = "barrier-unverified.stage";
    try std.testing.expectError(
        error.PersistenceSnapshotInventoryDigestMismatch,
        convertJournalFamily(allocator, unverified),
    );
    try expectPathMissing(tmp.dir, unverified.output.path);

    var missing = rejected;
    var missing_inventory = inventory;
    missing_inventory.manifest_path = "missing-barrier.ecm";
    missing.persistence_barrier_policy = .{ .supply_snapshot_id = missing_inventory };
    missing.output.path = "barrier-missing-inventory.xcj";
    missing.output.staging_path = "barrier-missing-inventory.stage";
    try std.testing.expectError(
        error.FileNotFound,
        convertJournalFamily(allocator, missing),
    );
    try expectPathMissing(tmp.dir, missing.output.path);

    var zero_identity = rejected;
    zero_identity.persistence_barrier_policy = .{ .supply_snapshot_id = try writeTestSnapshotInventory(tmp.dir, "zero-barrier.ecm", 0, 1, 2, 4_001) };
    zero_identity.output.path = "barrier-zero-identity.xcj";
    zero_identity.output.staging_path = "barrier-zero-identity.stage";
    try std.testing.expectError(
        error.InvalidPersistenceSnapshotProof,
        convertJournalFamily(allocator, zero_identity),
    );
    try expectPathMissing(tmp.dir, zero_identity.output.path);

    var wrong_topology = rejected;
    wrong_topology.persistence_barrier_policy = .{ .supply_snapshot_id = try writeTestSnapshotInventoryWithTarget(
        tmp.dir,
        "wrong-topology-barrier.ecm",
        9_002,
        1,
        2,
        4_001,
        2,
        1,
        .{},
    ) };
    wrong_topology.output.path = "barrier-wrong-topology.xcj";
    wrong_topology.output.staging_path = "barrier-wrong-topology.stage";
    try std.testing.expectError(
        error.PersistenceSnapshotInventoryTopologyMismatch,
        convertJournalFamily(allocator, wrong_topology),
    );
    try expectPathMissing(tmp.dir, wrong_topology.output.path);

    var staged_topology = try kernel_mod.MultiShardKernelSet.initWithTopology(
        allocator,
        2,
        1,
        .{},
    );
    defer staged_topology.deinit();
    const staged_before = try staged_topology.detailedHashFingerprint();
    var wrong_staged_topology = rejected;
    wrong_staged_topology.target = .{ .staged = &staged_topology };
    wrong_staged_topology.persistence_barrier_policy = .{ .supply_snapshot_id = inventory };
    wrong_staged_topology.output.path = "barrier-wrong-staged-topology.xcj";
    wrong_staged_topology.output.staging_path = "barrier-wrong-staged-topology.stage";
    try std.testing.expectError(
        error.PersistenceSnapshotInventoryTopologyMismatch,
        convertJournalFamily(allocator, wrong_staged_topology),
    );
    try std.testing.expectEqual(@as(u64, 0), staged_topology.next_seq);
    try std.testing.expectEqual(staged_before, try staged_topology.detailedHashFingerprint());
    try expectPathMissing(tmp.dir, wrong_staged_topology.output.path);

    var staged_config: kernel_mod.Config = .{};
    staged_config.send_l2_for_every_cmd = true;
    var staged_wrong_config_target = try kernel_mod.MultiShardKernelSet.initWithTopology(
        allocator,
        1,
        1,
        staged_config,
    );
    defer staged_wrong_config_target.deinit();
    const staged_wrong_config_before = try staged_wrong_config_target.detailedHashFingerprint();
    var wrong_staged_config = rejected;
    wrong_staged_config.target = .{ .staged = &staged_wrong_config_target };
    wrong_staged_config.persistence_barrier_policy = .{ .supply_snapshot_id = inventory };
    wrong_staged_config.output.path = "barrier-wrong-staged-config.xcj";
    wrong_staged_config.output.staging_path = "barrier-wrong-staged-config.stage";
    try std.testing.expectError(
        error.PersistenceSnapshotInventoryConfigMismatch,
        convertJournalFamily(allocator, wrong_staged_config),
    );
    try std.testing.expectEqual(@as(u64, 0), staged_wrong_config_target.next_seq);
    try std.testing.expectEqual(
        staged_wrong_config_before,
        try staged_wrong_config_target.detailedHashFingerprint(),
    );
    try expectPathMissing(tmp.dir, wrong_staged_config.output.path);

    var mismatching_config: kernel_mod.Config = .{};
    mismatching_config.send_l2_for_every_cmd = true;
    var wrong_config = rejected;
    wrong_config.persistence_barrier_policy = .{ .supply_snapshot_id = try writeTestSnapshotInventoryWithTarget(
        tmp.dir,
        "wrong-config-barrier.ecm",
        9_003,
        1,
        2,
        4_001,
        1,
        1,
        mismatching_config,
    ) };
    wrong_config.output.path = "barrier-wrong-config.xcj";
    wrong_config.output.staging_path = "barrier-wrong-config.stage";
    try std.testing.expectError(
        error.PersistenceSnapshotInventoryConfigMismatch,
        convertJournalFamily(allocator, wrong_config),
    );
    try expectPathMissing(tmp.dir, wrong_config.output.path);

    var supplied = rejected;
    supplied.persistence_barrier_policy = .{ .supply_snapshot_id = inventory };
    supplied.output.path = "barrier-supplied.xcj";
    supplied.output.staging_path = "barrier-supplied.stage";
    const summary = try convertJournalFamily(allocator, supplied);
    try std.testing.expectEqual(@as(usize, 2), summary.replay.replayed);
    var loaded = try journal.CommandJournal.readFromFile(
        allocator,
        tmp.dir,
        supplied.output.path,
    );
    defer loaded.deinit();
    try std.testing.expectEqual(@as(i64, 9_001), loaded.entries.items[0].input.order_id);
    try std.testing.expectEqual(@as(i64, 4_001), loaded.entries.items[0].input.timestamp);
    try std.testing.expectEqual(journal.ExecutionMode.split, loaded.entries.items[0].mode);
    try std.testing.expectEqual(cmd.CommandResultCode.accepted, loaded.entries.items[0].result_code);
    try std.testing.expectEqual(@as(i64, 9_001), loaded.entries.items[1].input.order_id);
    try std.testing.expectEqual(@as(i64, 4_001), loaded.entries.items[1].input.timestamp);
    try std.testing.expectEqual(cmd.CommandResultCode.success, loaded.entries.items[1].result_code);
}

test "Java journal converter requires an exact terminal persistence pair inventory proof" {
    const allocator = std.testing.allocator;
    const scenarios = [_]struct {
        records: []const TestRecord,
        snapshot_id: u64,
        matching_sequence: u64,
        risk_sequence: u64,
        timestamp_ns: u64,
    }{
        .{
            .records = &.{.{ .persist_matching = .{ .sequence = 1, .timestamp = 5_001 } }},
            .snapshot_id = 10_001,
            .matching_sequence = 1,
            .risk_sequence = 2,
            .timestamp_ns = 5_001,
        },
        .{
            .records = &.{
                .{ .persist_risk = .{ .sequence = 1, .timestamp = 5_002 } },
                .{ .persist_matching = .{ .sequence = 2, .timestamp = 5_002 } },
            },
            .snapshot_id = 10_002,
            .matching_sequence = 1,
            .risk_sequence = 2,
            .timestamp_ns = 5_002,
        },
        .{
            .records = &.{
                .{ .persist_matching = .{ .sequence = 1, .timestamp = 5_003 } },
                .{ .persist_risk = .{ .sequence = 2, .timestamp = 5_003 } },
                .{ .reset = .{ .sequence = 3, .timestamp = 5_004 } },
            },
            .snapshot_id = 10_003,
            .matching_sequence = 1,
            .risk_sequence = 2,
            .timestamp_ns = 5_003,
        },
        .{
            .records = &.{
                .{ .persist_matching = .{ .sequence = 1, .timestamp = 5_005 } },
                .{ .persist_risk = .{ .sequence = 2, .timestamp = 5_005 } },
            },
            .snapshot_id = 10_004,
            .matching_sequence = 2,
            .risk_sequence = 3,
            .timestamp_ns = 5_005,
        },
        .{
            .records = &.{
                .{ .persist_matching = .{ .sequence = 1, .timestamp = 5_006 } },
                .{ .persist_risk = .{ .sequence = 2, .timestamp = 5_007 } },
            },
            .snapshot_id = 10_005,
            .matching_sequence = 1,
            .risk_sequence = 2,
            .timestamp_ns = 5_006,
        },
    };

    for (scenarios, 0..) |scenario, index| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var raw: std.ArrayListUnmanaged(u8) = .empty;
        defer raw.deinit(allocator);
        for (scenario.records) |record| try appendTestRecord(allocator, &raw, record);
        try writeTestFile(tmp.dir, "invalid-barrier.ecj", raw.items);

        var target = try kernel_mod.MultiShardKernelSet.initWithTopology(allocator, 1, 1, .{});
        defer target.deinit();
        const before = try target.detailedHashFingerprint();
        const partitions = [_]Partition{.{ .id = 0, .path = "invalid-barrier.ecj" }};
        var request = testRequest(
            tmp.dir,
            &partitions,
            .{ .staged = &target },
            .routed,
            0,
            "invalid-barrier.xcj",
            "invalid-barrier.stage",
        );
        var inventory_path_buf: [64]u8 = undefined;
        const inventory_path = try std.fmt.bufPrint(
            &inventory_path_buf,
            "invalid-barrier-{d}.ecm",
            .{index},
        );
        request.persistence_barrier_policy = .{ .supply_snapshot_id = try writeTestSnapshotInventory(
            tmp.dir,
            inventory_path,
            scenario.snapshot_id,
            scenario.matching_sequence,
            scenario.risk_sequence,
            scenario.timestamp_ns,
        ) };
        try std.testing.expectError(
            error.PersistenceBarrierProofMismatch,
            convertJournalFamily(allocator, request),
        );
        try std.testing.expectEqual(@as(u64, 0), target.next_seq);
        try std.testing.expectEqual(before, try target.detailedHashFingerprint());
        try expectPathMissing(tmp.dir, request.output.path);
        try std.testing.expectEqual(@as(usize, 0), try countPrivateStagingDirectories(tmp.dir));
    }
}

test "Java journal converter authenticates inventory before staged mutation" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var raw: std.ArrayListUnmanaged(u8) = .empty;
    defer raw.deinit(allocator);
    try appendTestRecord(allocator, &raw, .{ .persist_matching = .{
        .sequence = 1,
        .timestamp = 5_101,
    } });
    try appendTestRecord(allocator, &raw, .{ .persist_risk = .{
        .sequence = 2,
        .timestamp = 5_101,
    } });
    try writeTestFile(tmp.dir, "authenticated-barrier.ecj", raw.items);
    const partitions = [_]Partition{.{ .id = 0, .path = "authenticated-barrier.ecj" }};

    var target = try kernel_mod.MultiShardKernelSet.initWithTopology(allocator, 1, 1, .{});
    defer target.deinit();
    const before = try target.detailedHashFingerprint();
    var request = testRequest(
        tmp.dir,
        &partitions,
        .{ .staged = &target },
        .routed,
        0,
        "authenticated-barrier.xcj",
        "authenticated-barrier.stage",
    );

    var wrong_digest = try writeTestSnapshotInventory(
        tmp.dir,
        "authenticated-barrier.ecm",
        10_101,
        1,
        2,
        5_101,
    );
    wrong_digest.manifest_trailer_sha256[0] ^= 0xFF;
    request.persistence_barrier_policy = .{ .supply_snapshot_id = wrong_digest };
    try std.testing.expectError(
        error.PersistenceSnapshotInventoryDigestMismatch,
        convertJournalFamily(allocator, request),
    );
    try std.testing.expectEqual(@as(u64, 0), target.next_seq);
    try std.testing.expectEqual(before, try target.detailedHashFingerprint());
    try expectPathMissing(tmp.dir, request.output.path);

    try writeTestFile(tmp.dir, "corrupt-barrier.ecm", "NOPE");
    request.persistence_barrier_policy = .{ .supply_snapshot_id = .{
        .manifest_dir = tmp.dir,
        .manifest_path = "corrupt-barrier.ecm",
        .manifest_trailer_sha256 = [_]u8{0} ** bounded_file.sha256_len,
    } };
    try std.testing.expectError(
        error.Truncated,
        convertJournalFamily(allocator, request),
    );
    try std.testing.expectEqual(@as(u64, 0), target.next_seq);
    try std.testing.expectEqual(before, try target.detailedHashFingerprint());
    try expectPathMissing(tmp.dir, request.output.path);
}

test "Java journal converter rejects truncation and corruption without replacing output" {
    const allocator = std.testing.allocator;
    const corruptions = [_][]const u8{
        &.{ @bitCast(cmd.OrderCommandType.add_user.code()), 0, 0, 0 },
        &.{ 0x55, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
        &.{ 0xff, 0, 0, 0, 1, 0, 0, 0, 8, 0 },
    };
    for (corruptions, 0..) |bytes, index| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try writeTestFile(tmp.dir, "bad.ecj", bytes);
        try writeTestFile(tmp.dir, "existing.xcj", "ORIGINAL");
        const partitions = [_]Partition{.{ .id = 0, .path = "bad.ecj" }};
        var request = testRequest(
            tmp.dir,
            &partitions,
            .{ .fresh = .{ .matching_count = 1, .risk_count = 1 } },
            .routed,
            0,
            "existing.xcj",
            "bad.stage",
        );
        request.output.replacement = .replace;
        if (convertJournalFamily(allocator, request)) |_| {
            return error.TestExpectedError;
        } else |_| {}

        const existing = try bounded_file.readBoundedFileAlloc(
            allocator,
            tmp.dir,
            request.output.path,
            64,
        );
        defer allocator.free(existing);
        try std.testing.expectEqualStrings("ORIGINAL", existing);
        try expectPathMissing(tmp.dir, request.output.staging_path);
        _ = index;
    }
}

test "Java journal converter atomically publishes without following caller pathnames" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var raw: std.ArrayListUnmanaged(u8) = .empty;
    defer raw.deinit(allocator);
    try appendTestRecord(allocator, &raw, .{ .add_user = .{
        .sequence = 1,
        .uid = 9_501,
        .timestamp = 9_601,
    } });
    try writeTestFile(tmp.dir, "atomic.ecj", raw.items);
    const partitions = [_]Partition{.{ .id = 0, .path = "atomic.ecj" }};

    // The caller's staging label is never opened, truncated, or cleaned up.
    try writeTestFile(tmp.dir, "reserved.stage", "FOREIGN-STAGING");
    const foreign_private_dir = ".xcj-private-0000000000000000";
    try tmp.dir.createDir(io_context.current(), foreign_private_dir, .default_dir);
    try writeTestFile(
        tmp.dir,
        ".xcj-private-0000000000000000/foreign",
        "FOREIGN-PRIVATE",
    );
    try std.testing.expectEqual(@as(usize, 1), try countPrivateStagingDirectories(tmp.dir));
    var request = testRequest(
        tmp.dir,
        &partitions,
        .{ .fresh = .{ .matching_count = 1, .risk_count = 1 } },
        .routed,
        0,
        "atomic.xcj",
        "reserved.stage",
    );
    _ = try convertJournalFamily(allocator, request);
    const staging_bytes = try bounded_file.readBoundedFileAlloc(
        allocator,
        tmp.dir,
        request.output.staging_path,
        64,
    );
    defer allocator.free(staging_bytes);
    try std.testing.expectEqualStrings("FOREIGN-STAGING", staging_bytes);
    var published = try journal.CommandJournal.readFromFile(
        allocator,
        tmp.dir,
        request.output.path,
    );
    defer published.deinit();
    try std.testing.expectEqual(@as(usize, 1), published.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), try countPrivateStagingDirectories(tmp.dir));

    // Replacement targets the directory entry itself. A symlink destination
    // must be replaced without following it and corrupting its referent.
    try writeTestFile(tmp.dir, "victim.bin", "VICTIM");
    try tmp.dir.symLink(
        io_context.current(),
        "victim.bin",
        "replace.xcj",
        .{},
    );
    request.output.path = "replace.xcj";
    request.output.staging_path = "replace.stage";
    request.output.replacement = .replace;
    _ = try convertJournalFamily(allocator, request);
    const victim = try bounded_file.readBoundedFileAlloc(
        allocator,
        tmp.dir,
        "victim.bin",
        64,
    );
    defer allocator.free(victim);
    try std.testing.expectEqualStrings("VICTIM", victim);
    var replaced = try journal.CommandJournal.readFromFile(
        allocator,
        tmp.dir,
        request.output.path,
    );
    defer replaced.deinit();
    try std.testing.expectEqual(@as(usize, 1), replaced.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), try countPrivateStagingDirectories(tmp.dir));
    const foreign = try bounded_file.readBoundedFileAlloc(
        allocator,
        tmp.dir,
        ".xcj-private-0000000000000000/foreign",
        64,
    );
    defer allocator.free(foreign);
    try std.testing.expectEqualStrings("FOREIGN-PRIVATE", foreign);
}

fn conversionAllocationCampaign(allocator: std.mem.Allocator) !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var raw: std.ArrayListUnmanaged(u8) = .empty;
    defer raw.deinit(std.testing.allocator);
    try appendTestRecord(std.testing.allocator, &raw, .{ .add_user = .{
        .sequence = 1,
        .uid = 8_001,
        .timestamp = 8_101,
    } });
    try appendTestRecord(std.testing.allocator, &raw, .{ .balance_adjustment = .{
        .sequence = 2,
        .uid = 8_001,
        .currency = 15,
        .transaction_id = 8_201,
        .amount = 99,
        .timestamp = 8_102,
    } });
    try writeTestFile(tmp.dir, "oom.ecj", raw.items);
    const partitions = [_]Partition{.{ .id = 0, .path = "oom.ecj" }};
    const request = testRequest(
        tmp.dir,
        &partitions,
        .{ .fresh = .{ .matching_count = 2, .risk_count = 1 } },
        .split,
        0,
        "oom.xcj",
        "oom.stage",
    );
    errdefer {
        expectPathMissing(tmp.dir, request.output.path) catch
            @panic("failed to verify absent final journal after OOM");
        expectPathMissing(tmp.dir, request.output.staging_path) catch
            @panic("failed to verify absent staging journal after OOM");
    }
    _ = try convertJournalFamily(allocator, request);
}

test "Java journal converter is leak-free and never advertises output across allocation failures" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        conversionAllocationCampaign,
        .{},
    );
}
