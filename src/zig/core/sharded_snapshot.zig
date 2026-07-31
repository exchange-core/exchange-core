const std = @import("std");
const io_context = @import("io_context.zig");

pub const manifest_magic = "XCM2";
pub const matching_payload_magic = "XME2";
pub const risk_payload_magic = "XRE2";

pub const format_version: u16 = 2;
pub const payload_version: u8 = 2;
pub const manifest_header_len: usize = 168;
pub const manifest_entry_len: usize = 64;
pub const manifest_trailer_len: usize = 32;
pub const payload_header_len: usize = 96;
pub const sha256_len: usize = 32;

const Sha256 = std.crypto.hash.sha2.Sha256;

comptime {
    std.debug.assert(Sha256.digest_length == sha256_len);
}

pub const ModuleKind = enum(u8) {
    matching_engine = 1,
    risk_engine = 2,
};

pub const DigestAlgorithm = enum(u8) {
    sha256 = 1,
};

pub const Limits = struct {
    max_manifest_bytes: usize = 4 * 1024 * 1024,
    max_entries: u32 = 4096,
    max_payload_bytes: usize = 256 * 1024 * 1024,
};

pub const FormatError = error{
    InvalidMagic,
    UnsupportedVersion,
    UnknownManifestFlags,
    UnknownEntryFlags,
    UnknownPayloadFlags,
    NonZeroReserved,
    UnsupportedDigestAlgorithm,
    InvalidDigestLength,
    InvalidMatchingCount,
    InvalidRiskCount,
    InvalidMatchingMask,
    InvalidRiskMask,
    EntryCountOverflow,
    EntryCountMismatch,
    InvalidSequence,
    InvalidPublicationHighWater,
    InvalidObservedHighWater,
    InvalidModuleKind,
    UnsupportedPayloadVersion,
    InvalidEntryShard,
    InvalidEntrySequence,
    InvalidPayloadLength,
    DuplicateShard,
    MissingShard,
    NonCanonicalEntryOrder,
    DigestMismatch,
    Truncated,
    TrailingBytes,
    LimitExceeded,
    SnapshotIdMismatch,
    SequenceMismatch,
    TopologyMismatch,
    ConfigMismatch,
    ExchangeMismatch,
    BundleMismatch,
    PayloadKindMismatch,
    PayloadShardMismatch,
    PayloadLengthMismatch,
    PayloadHeaderMismatch,
    PayloadDigestMismatch,
};

pub const ReadFileError = std.mem.Allocator.Error ||
    std.Io.File.OpenError ||
    std.Io.File.StatError ||
    io_context.ReadToEndAllocError ||
    error{
        FileTooLarge,
        FileSizeChanged,
    };

pub const AtomicPublishError = std.Io.Dir.CreateFileAtomicError ||
    std.Io.File.Atomic.LinkError ||
    std.Io.File.SyncError ||
    std.Io.File.OpenError ||
    std.Io.Writer.Error;

pub const ManifestHeader = struct {
    flags: u16 = 0,
    snapshot_id: u64,
    matching_command_seq: u64,
    risk_command_seq: u64,
    publication_high_water: u64,
    timestamp_ns: u64,
    matching_count: u32,
    risk_count: u32,
    matching_mask: u64,
    risk_mask: u64,
    /// Opaque at the codec boundary. The sharded restore integration must
    /// recompute and compare the aggregate after all module bodies are staged.
    aggregate_state_hash: u64,
    config_sha256: [sha256_len]u8,
    exchange_id_sha256: [sha256_len]u8,
    bundle_id: [16]u8,
    entry_count: u32,
    digest_algorithm: DigestAlgorithm = .sha256,
    digest_length: u8 = sha256_len,
    reserved: u16 = 0,
};

pub const ManifestEntry = struct {
    module_kind: ModuleKind,
    payload_version: u8 = payload_version,
    flags: u16 = 0,
    shard_id: u32,
    command_seq: u64,
    byte_len: u64,
    payload_sha256: [sha256_len]u8,
    module_state_hash: u64,
};

pub const ManifestExpectations = struct {
    snapshot_id: ?u64 = null,
    matching_command_seq: ?u64 = null,
    risk_command_seq: ?u64 = null,
    publication_high_water: ?u64 = null,
    matching_count: ?u32 = null,
    risk_count: ?u32 = null,
    matching_mask: ?u64 = null,
    risk_mask: ?u64 = null,
    config_sha256: ?[sha256_len]u8 = null,
    exchange_id_sha256: ?[sha256_len]u8 = null,
    bundle_id: ?[16]u8 = null,
};

pub const Manifest = struct {
    allocator: std.mem.Allocator,
    header: ManifestHeader,
    entries: []ManifestEntry,
    trailer_sha256: [sha256_len]u8,

    pub fn deinit(self: *Manifest) void {
        self.allocator.free(self.entries);
        self.* = undefined;
    }

    pub fn encodedLen(self: *const Manifest) usize {
        return manifest_header_len + self.entries.len * manifest_entry_len + manifest_trailer_len;
    }

    pub fn entry(self: *const Manifest, kind: ModuleKind, shard_id: u32) ?*const ManifestEntry {
        for (self.entries) |*candidate| {
            if (candidate.module_kind == kind and candidate.shard_id == shard_id) return candidate;
        }
        return null;
    }
};

pub const PayloadHeader = struct {
    flags: u16 = 0,
    snapshot_id: u64,
    /// The persist command that captured this module: S for ME, S+1 for RE.
    command_seq: u64,
    /// The globally accepted/restorable frontier. Both halves of a committed
    /// bundle carry S+1 because the risk-persist command does not mutate ME.
    observed_high_water: u64,
    timestamp_ns: u64,
    shard_id: u32,
    shard_count: u32,
    shard_mask: u64,
    config_sha256: [sha256_len]u8,
    module_state_hash: u64,
};

pub const PayloadExpectations = struct {
    module_kind: ?ModuleKind = null,
    snapshot_id: ?u64 = null,
    command_seq: ?u64 = null,
    observed_high_water: ?u64 = null,
    timestamp_ns: ?u64 = null,
    shard_id: ?u32 = null,
    shard_count: ?u32 = null,
    shard_mask: ?u64 = null,
    config_sha256: ?[sha256_len]u8 = null,
    module_state_hash: ?u64 = null,
    byte_len: ?u64 = null,
    payload_sha256: ?[sha256_len]u8 = null,

    pub fn fromManifest(manifest: *const Manifest, entry_value: ManifestEntry) PayloadExpectations {
        const header = manifest.header;
        return .{
            .module_kind = entry_value.module_kind,
            .snapshot_id = header.snapshot_id,
            .command_seq = entry_value.command_seq,
            // Every staged module is restored at the manifest's committed
            // global frontier, even though ME itself was captured at S.
            .observed_high_water = header.publication_high_water,
            .timestamp_ns = header.timestamp_ns,
            .shard_id = entry_value.shard_id,
            .shard_count = switch (entry_value.module_kind) {
                .matching_engine => header.matching_count,
                .risk_engine => header.risk_count,
            },
            .shard_mask = switch (entry_value.module_kind) {
                .matching_engine => header.matching_mask,
                .risk_engine => header.risk_mask,
            },
            .config_sha256 = header.config_sha256,
            .module_state_hash = entry_value.module_state_hash,
            .byte_len = entry_value.byte_len,
            .payload_sha256 = entry_value.payload_sha256,
        };
    }
};

pub const PayloadFile = struct {
    allocator: std.mem.Allocator,
    module_kind: ModuleKind,
    header: PayloadHeader,
    body: []u8,
    byte_len: u64,
    payload_sha256: [sha256_len]u8,

    pub fn deinit(self: *PayloadFile) void {
        self.allocator.free(self.body);
        self.* = undefined;
    }
};

pub fn sha256(bytes: []const u8) [sha256_len]u8 {
    var digest: [sha256_len]u8 = undefined;
    Sha256.hash(bytes, &digest, .{});
    return digest;
}

pub fn manifestEncodedLength(entry_count: u32, limits: Limits) FormatError!usize {
    if (entry_count > limits.max_entries) return error.LimitExceeded;
    const entries_len = std.math.mul(usize, @as(usize, entry_count), manifest_entry_len) catch
        return error.LimitExceeded;
    const without_trailer = std.math.add(usize, manifest_header_len, entries_len) catch
        return error.LimitExceeded;
    const total = std.math.add(usize, without_trailer, manifest_trailer_len) catch
        return error.LimitExceeded;
    if (total > limits.max_manifest_bytes) return error.LimitExceeded;
    return total;
}

pub fn validateManifest(
    header: ManifestHeader,
    entries: []const ManifestEntry,
    limits: Limits,
    expected: ManifestExpectations,
) FormatError!void {
    try validateManifestHeader(header, limits, expected);
    if (entries.len != @as(usize, header.entry_count)) return error.EntryCountMismatch;
    try validateManifestEntries(header, entries, limits);
}

pub fn encodeManifestAlloc(
    allocator: std.mem.Allocator,
    header: ManifestHeader,
    entries: []const ManifestEntry,
    limits: Limits,
) (std.mem.Allocator.Error || FormatError)![]u8 {
    try validateManifest(header, entries, limits, .{});
    const total_len = try manifestEncodedLength(header.entry_count, limits);
    const bytes = try allocator.alloc(u8, total_len);
    errdefer allocator.free(bytes);

    encodeManifestHeader(bytes[0..manifest_header_len], header);
    for (entries, 0..) |entry_value, index| {
        const offset = manifest_header_len + index * manifest_entry_len;
        encodeManifestEntry(bytes[offset..][0..manifest_entry_len], entry_value);
    }
    const trailer_offset = total_len - manifest_trailer_len;
    const digest = sha256(bytes[0..trailer_offset]);
    @memcpy(bytes[trailer_offset..], &digest);
    return bytes;
}

pub fn decodeManifestAlloc(
    allocator: std.mem.Allocator,
    bytes: []const u8,
    limits: Limits,
    expected: ManifestExpectations,
) (std.mem.Allocator.Error || FormatError)!Manifest {
    if (bytes.len > limits.max_manifest_bytes) return error.LimitExceeded;
    if (bytes.len < manifest_header_len + manifest_trailer_len) return error.Truncated;

    const header = try decodeManifestHeader(bytes[0..manifest_header_len]);
    try validateManifestHeader(header, limits, expected);
    const total_len = try manifestEncodedLength(header.entry_count, limits);
    if (bytes.len < total_len) return error.Truncated;
    if (bytes.len > total_len) return error.TrailingBytes;

    const trailer_offset = total_len - manifest_trailer_len;
    const computed = sha256(bytes[0..trailer_offset]);
    if (!std.mem.eql(u8, &computed, bytes[trailer_offset..])) return error.DigestMismatch;

    const entries = try allocator.alloc(ManifestEntry, header.entry_count);
    errdefer allocator.free(entries);
    for (entries, 0..) |*entry_value, index| {
        const offset = manifest_header_len + index * manifest_entry_len;
        entry_value.* = try decodeManifestEntry(bytes[offset..][0..manifest_entry_len]);
    }
    try validateManifestEntries(header, entries, limits);

    return .{
        .allocator = allocator,
        .header = header,
        .entries = entries,
        .trailer_sha256 = computed,
    };
}

pub fn encodePayloadAlloc(
    allocator: std.mem.Allocator,
    kind: ModuleKind,
    header: PayloadHeader,
    body: []const u8,
    limits: Limits,
) (std.mem.Allocator.Error || FormatError)![]u8 {
    try validatePayloadHeader(header, .{});
    const total_len = std.math.add(usize, payload_header_len, body.len) catch return error.LimitExceeded;
    if (total_len > limits.max_payload_bytes) return error.LimitExceeded;
    const bytes = try allocator.alloc(u8, total_len);
    errdefer allocator.free(bytes);
    encodePayloadHeader(bytes[0..payload_header_len], kind, header);
    @memcpy(bytes[payload_header_len..], body);
    return bytes;
}

pub fn decodePayloadAlloc(
    allocator: std.mem.Allocator,
    bytes: []const u8,
    limits: Limits,
    expected: PayloadExpectations,
) (std.mem.Allocator.Error || FormatError)!PayloadFile {
    if (bytes.len > limits.max_payload_bytes) return error.LimitExceeded;
    if (bytes.len < payload_header_len) return error.Truncated;
    if (expected.byte_len) |expected_len| {
        const actual_len = std.math.cast(u64, bytes.len) orelse return error.LimitExceeded;
        if (actual_len < expected_len) return error.Truncated;
        if (actual_len > expected_len) return error.TrailingBytes;
    }

    const decoded = try decodePayloadHeader(bytes[0..payload_header_len]);
    try validatePayloadHeader(decoded.header, expected);
    if (expected.module_kind) |kind| {
        if (decoded.module_kind != kind) return error.PayloadKindMismatch;
    }
    const digest = sha256(bytes);
    if (expected.payload_sha256) |wanted| {
        if (!std.mem.eql(u8, &digest, &wanted)) return error.PayloadDigestMismatch;
    }

    const body = try allocator.dupe(u8, bytes[payload_header_len..]);
    errdefer allocator.free(body);
    return .{
        .allocator = allocator,
        .module_kind = decoded.module_kind,
        .header = decoded.header,
        .body = body,
        .byte_len = @intCast(bytes.len),
        .payload_sha256 = digest,
    };
}

pub fn readBoundedFileAlloc(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    path: []const u8,
    max_bytes: usize,
) ReadFileError![]u8 {
    const file = try dir.openFile(io_context.current(), path, .{});
    defer file.close(io_context.current());
    const stat = try file.stat(io_context.current());
    const stat_len = std.math.cast(usize, stat.size) orelse return error.FileTooLarge;
    if (stat_len > max_bytes) return error.FileTooLarge;
    const read_limit = std.math.add(usize, max_bytes, 1) catch return error.FileTooLarge;
    const bytes = try io_context.readToEndAlloc(file, allocator, read_limit);
    errdefer allocator.free(bytes);
    if (bytes.len != stat_len) return error.FileSizeChanged;
    return bytes;
}

pub fn readManifestFileAlloc(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    path: []const u8,
    limits: Limits,
    expected: ManifestExpectations,
) (ReadFileError || FormatError)!Manifest {
    const bytes = try readBoundedFileAlloc(allocator, dir, path, limits.max_manifest_bytes);
    defer allocator.free(bytes);
    return decodeManifestAlloc(allocator, bytes, limits, expected);
}

pub fn readPayloadFileAlloc(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    path: []const u8,
    limits: Limits,
    expected: PayloadExpectations,
) (ReadFileError || FormatError)!PayloadFile {
    const bytes = try readBoundedFileAlloc(allocator, dir, path, limits.max_payload_bytes);
    defer allocator.free(bytes);
    return decodePayloadAlloc(allocator, bytes, limits, expected);
}

pub fn publishBytesAtomicNoReplace(
    dir: std.Io.Dir,
    path: []const u8,
    bytes: []const u8,
) AtomicPublishError!void {
    var atomic_file = try dir.createFileAtomic(io_context.current(), path, .{ .replace = false });
    defer atomic_file.deinit(io_context.current());
    const file = atomic_file.file;
    var buffer: [4096]u8 = undefined;
    var file_writer = file.writer(io_context.current(), &buffer);
    try file_writer.interface.writeAll(bytes);
    try file_writer.interface.flush();
    try file.sync(io_context.current());
    try atomic_file.link(io_context.current());
    try syncParentDirectory(dir, path);
}

pub fn writeManifestFileAtomic(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    path: []const u8,
    header: ManifestHeader,
    entries: []const ManifestEntry,
    limits: Limits,
) (std.mem.Allocator.Error || FormatError || AtomicPublishError)!void {
    const bytes = try encodeManifestAlloc(allocator, header, entries, limits);
    defer allocator.free(bytes);
    try publishBytesAtomicNoReplace(dir, path, bytes);
}

pub fn writePayloadFileAtomic(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    path: []const u8,
    kind: ModuleKind,
    header: PayloadHeader,
    body: []const u8,
    limits: Limits,
) (std.mem.Allocator.Error || FormatError || AtomicPublishError)!ManifestEntry {
    const bytes = try encodePayloadAlloc(allocator, kind, header, body, limits);
    defer allocator.free(bytes);
    try publishBytesAtomicNoReplace(dir, path, bytes);
    return .{
        .module_kind = kind,
        .shard_id = header.shard_id,
        .command_seq = header.command_seq,
        .byte_len = @intCast(bytes.len),
        .payload_sha256 = sha256(bytes),
        .module_state_hash = header.module_state_hash,
    };
}

fn validateManifestHeader(
    header: ManifestHeader,
    limits: Limits,
    expected: ManifestExpectations,
) FormatError!void {
    if (header.flags != 0) return error.UnknownManifestFlags;
    if (header.reserved != 0) return error.NonZeroReserved;
    if (header.digest_algorithm != .sha256) return error.UnsupportedDigestAlgorithm;
    if (header.digest_length != sha256_len) return error.InvalidDigestLength;
    if (!isValidShardCount(header.matching_count)) return error.InvalidMatchingCount;
    if (!isValidShardCount(header.risk_count)) return error.InvalidRiskCount;
    if (header.matching_mask != maskForCount(header.matching_count)) return error.InvalidMatchingMask;
    if (header.risk_mask != maskForCount(header.risk_count)) return error.InvalidRiskMask;
    const entry_count = std.math.add(u32, header.matching_count, header.risk_count) catch
        return error.EntryCountOverflow;
    if (header.entry_count != entry_count) return error.EntryCountMismatch;
    _ = try manifestEncodedLength(entry_count, limits);
    const expected_risk_seq = std.math.add(u64, header.matching_command_seq, 1) catch
        return error.InvalidSequence;
    if (header.risk_command_seq != expected_risk_seq) return error.InvalidSequence;
    if (header.publication_high_water != header.risk_command_seq) return error.InvalidPublicationHighWater;

    if (expected.snapshot_id) |value| if (header.snapshot_id != value) return error.SnapshotIdMismatch;
    if (expected.matching_command_seq) |value| if (header.matching_command_seq != value) return error.SequenceMismatch;
    if (expected.risk_command_seq) |value| if (header.risk_command_seq != value) return error.SequenceMismatch;
    if (expected.publication_high_water) |value| if (header.publication_high_water != value) return error.SequenceMismatch;
    if (expected.matching_count) |value| if (header.matching_count != value) return error.TopologyMismatch;
    if (expected.risk_count) |value| if (header.risk_count != value) return error.TopologyMismatch;
    if (expected.matching_mask) |value| if (header.matching_mask != value) return error.TopologyMismatch;
    if (expected.risk_mask) |value| if (header.risk_mask != value) return error.TopologyMismatch;
    if (expected.config_sha256) |value| if (!std.mem.eql(u8, &header.config_sha256, &value)) return error.ConfigMismatch;
    if (expected.exchange_id_sha256) |value| if (!std.mem.eql(u8, &header.exchange_id_sha256, &value)) return error.ExchangeMismatch;
    if (expected.bundle_id) |value| if (!std.mem.eql(u8, &header.bundle_id, &value)) return error.BundleMismatch;
}

fn validateManifestEntries(
    header: ManifestHeader,
    entries: []const ManifestEntry,
    limits: Limits,
) FormatError!void {
    if (entries.len != @as(usize, header.entry_count)) return error.EntryCountMismatch;

    var actual_matching_count: u32 = 0;
    var actual_risk_count: u32 = 0;
    for (entries) |entry_value| {
        if (entry_value.payload_version != payload_version) return error.UnsupportedPayloadVersion;
        if (entry_value.flags != 0) return error.UnknownEntryFlags;
        const count = switch (entry_value.module_kind) {
            .matching_engine => blk: {
                actual_matching_count += 1;
                break :blk header.matching_count;
            },
            .risk_engine => blk: {
                actual_risk_count += 1;
                break :blk header.risk_count;
            },
        };
        if (entry_value.shard_id >= count) return error.InvalidEntryShard;
        const expected_seq = switch (entry_value.module_kind) {
            .matching_engine => header.matching_command_seq,
            .risk_engine => header.risk_command_seq,
        };
        if (entry_value.command_seq != expected_seq) return error.InvalidEntrySequence;
        if (entry_value.byte_len < payload_header_len) return error.InvalidPayloadLength;
        const payload_len = std.math.cast(usize, entry_value.byte_len) orelse return error.LimitExceeded;
        if (payload_len > limits.max_payload_bytes) return error.LimitExceeded;
    }

    if (actual_matching_count != header.matching_count or actual_risk_count != header.risk_count) {
        return error.MissingShard;
    }

    for (entries, 0..) |entry_value, index| {
        for (entries[0..index]) |prior| {
            if (prior.module_kind == entry_value.module_kind and prior.shard_id == entry_value.shard_id) {
                return error.DuplicateShard;
            }
        }
    }

    var shard_id: u32 = 0;
    while (shard_id < header.matching_count) : (shard_id += 1) {
        if (!containsEntry(entries, .matching_engine, shard_id)) return error.MissingShard;
    }
    shard_id = 0;
    while (shard_id < header.risk_count) : (shard_id += 1) {
        if (!containsEntry(entries, .risk_engine, shard_id)) return error.MissingShard;
    }

    for (entries, 0..) |entry_value, index| {
        const expected_kind: ModuleKind = if (index < header.matching_count)
            .matching_engine
        else
            .risk_engine;
        const expected_shard: u32 = if (expected_kind == .matching_engine)
            @intCast(index)
        else
            @intCast(index - header.matching_count);
        if (entry_value.module_kind != expected_kind or entry_value.shard_id != expected_shard) {
            return error.NonCanonicalEntryOrder;
        }
    }
}

fn containsEntry(entries: []const ManifestEntry, kind: ModuleKind, shard_id: u32) bool {
    for (entries) |entry_value| {
        if (entry_value.module_kind == kind and entry_value.shard_id == shard_id) return true;
    }
    return false;
}

fn validatePayloadHeader(header: PayloadHeader, expected: PayloadExpectations) FormatError!void {
    if (header.flags != 0) return error.UnknownPayloadFlags;
    if (!isValidShardCount(header.shard_count)) return error.TopologyMismatch;
    if (header.shard_mask != maskForCount(header.shard_count)) return error.TopologyMismatch;
    if (header.shard_id >= header.shard_count) return error.InvalidEntryShard;
    if (header.observed_high_water < header.command_seq) return error.InvalidObservedHighWater;

    if (expected.snapshot_id) |value| if (header.snapshot_id != value) return error.SnapshotIdMismatch;
    if (expected.command_seq) |value| if (header.command_seq != value) return error.SequenceMismatch;
    if (expected.observed_high_water) |value| if (header.observed_high_water != value) return error.PayloadHeaderMismatch;
    if (expected.timestamp_ns) |value| if (header.timestamp_ns != value) return error.PayloadHeaderMismatch;
    if (expected.shard_id) |value| if (header.shard_id != value) return error.PayloadShardMismatch;
    if (expected.shard_count) |value| if (header.shard_count != value) return error.TopologyMismatch;
    if (expected.shard_mask) |value| if (header.shard_mask != value) return error.TopologyMismatch;
    if (expected.config_sha256) |value| if (!std.mem.eql(u8, &header.config_sha256, &value)) return error.ConfigMismatch;
    if (expected.module_state_hash) |value| if (header.module_state_hash != value) return error.PayloadHeaderMismatch;
}

fn isValidShardCount(count: u32) bool {
    return count != 0 and (count & (count - 1)) == 0;
}

fn maskForCount(count: u32) u64 {
    std.debug.assert(count != 0);
    return @as(u64, count) - 1;
}

fn encodeManifestHeader(bytes: *[manifest_header_len]u8, header: ManifestHeader) void {
    @memcpy(bytes[0..4], manifest_magic);
    putInt(u16, bytes, 4, format_version);
    putInt(u16, bytes, 6, header.flags);
    putInt(u64, bytes, 8, header.snapshot_id);
    putInt(u64, bytes, 16, header.matching_command_seq);
    putInt(u64, bytes, 24, header.risk_command_seq);
    putInt(u64, bytes, 32, header.publication_high_water);
    putInt(u64, bytes, 40, header.timestamp_ns);
    putInt(u32, bytes, 48, header.matching_count);
    putInt(u32, bytes, 52, header.risk_count);
    putInt(u64, bytes, 56, header.matching_mask);
    putInt(u64, bytes, 64, header.risk_mask);
    putInt(u64, bytes, 72, header.aggregate_state_hash);
    @memcpy(bytes[80..112], &header.config_sha256);
    @memcpy(bytes[112..144], &header.exchange_id_sha256);
    @memcpy(bytes[144..160], &header.bundle_id);
    putInt(u32, bytes, 160, header.entry_count);
    bytes[164] = @intFromEnum(header.digest_algorithm);
    bytes[165] = header.digest_length;
    putInt(u16, bytes, 166, header.reserved);
}

fn decodeManifestHeader(bytes: *const [manifest_header_len]u8) FormatError!ManifestHeader {
    if (!std.mem.eql(u8, bytes[0..4], manifest_magic)) return error.InvalidMagic;
    if (getInt(u16, bytes, 4) != format_version) return error.UnsupportedVersion;
    const digest_raw = bytes[164];
    if (digest_raw != @intFromEnum(DigestAlgorithm.sha256)) return error.UnsupportedDigestAlgorithm;
    return .{
        .flags = getInt(u16, bytes, 6),
        .snapshot_id = getInt(u64, bytes, 8),
        .matching_command_seq = getInt(u64, bytes, 16),
        .risk_command_seq = getInt(u64, bytes, 24),
        .publication_high_water = getInt(u64, bytes, 32),
        .timestamp_ns = getInt(u64, bytes, 40),
        .matching_count = getInt(u32, bytes, 48),
        .risk_count = getInt(u32, bytes, 52),
        .matching_mask = getInt(u64, bytes, 56),
        .risk_mask = getInt(u64, bytes, 64),
        .aggregate_state_hash = getInt(u64, bytes, 72),
        .config_sha256 = bytes[80..112].*,
        .exchange_id_sha256 = bytes[112..144].*,
        .bundle_id = bytes[144..160].*,
        .entry_count = getInt(u32, bytes, 160),
        .digest_algorithm = .sha256,
        .digest_length = bytes[165],
        .reserved = getInt(u16, bytes, 166),
    };
}

fn encodeManifestEntry(bytes: *[manifest_entry_len]u8, entry_value: ManifestEntry) void {
    bytes[0] = @intFromEnum(entry_value.module_kind);
    bytes[1] = entry_value.payload_version;
    putInt(u16, bytes, 2, entry_value.flags);
    putInt(u32, bytes, 4, entry_value.shard_id);
    putInt(u64, bytes, 8, entry_value.command_seq);
    putInt(u64, bytes, 16, entry_value.byte_len);
    @memcpy(bytes[24..56], &entry_value.payload_sha256);
    putInt(u64, bytes, 56, entry_value.module_state_hash);
}

fn decodeManifestEntry(bytes: *const [manifest_entry_len]u8) FormatError!ManifestEntry {
    const kind: ModuleKind = switch (bytes[0]) {
        @intFromEnum(ModuleKind.matching_engine) => .matching_engine,
        @intFromEnum(ModuleKind.risk_engine) => .risk_engine,
        else => return error.InvalidModuleKind,
    };
    return .{
        .module_kind = kind,
        .payload_version = bytes[1],
        .flags = getInt(u16, bytes, 2),
        .shard_id = getInt(u32, bytes, 4),
        .command_seq = getInt(u64, bytes, 8),
        .byte_len = getInt(u64, bytes, 16),
        .payload_sha256 = bytes[24..56].*,
        .module_state_hash = getInt(u64, bytes, 56),
    };
}

const DecodedPayloadHeader = struct {
    module_kind: ModuleKind,
    header: PayloadHeader,
};

fn encodePayloadHeader(bytes: *[payload_header_len]u8, kind: ModuleKind, header: PayloadHeader) void {
    @memcpy(bytes[0..4], magicForModule(kind));
    putInt(u16, bytes, 4, format_version);
    putInt(u16, bytes, 6, header.flags);
    putInt(u64, bytes, 8, header.snapshot_id);
    putInt(u64, bytes, 16, header.command_seq);
    putInt(u64, bytes, 24, header.observed_high_water);
    putInt(u64, bytes, 32, header.timestamp_ns);
    putInt(u32, bytes, 40, header.shard_id);
    putInt(u32, bytes, 44, header.shard_count);
    putInt(u64, bytes, 48, header.shard_mask);
    @memcpy(bytes[56..88], &header.config_sha256);
    putInt(u64, bytes, 88, header.module_state_hash);
}

fn decodePayloadHeader(bytes: *const [payload_header_len]u8) FormatError!DecodedPayloadHeader {
    const kind: ModuleKind = if (std.mem.eql(u8, bytes[0..4], matching_payload_magic))
        .matching_engine
    else if (std.mem.eql(u8, bytes[0..4], risk_payload_magic))
        .risk_engine
    else
        return error.InvalidMagic;
    if (getInt(u16, bytes, 4) != format_version) return error.UnsupportedVersion;
    return .{
        .module_kind = kind,
        .header = .{
            .flags = getInt(u16, bytes, 6),
            .snapshot_id = getInt(u64, bytes, 8),
            .command_seq = getInt(u64, bytes, 16),
            .observed_high_water = getInt(u64, bytes, 24),
            .timestamp_ns = getInt(u64, bytes, 32),
            .shard_id = getInt(u32, bytes, 40),
            .shard_count = getInt(u32, bytes, 44),
            .shard_mask = getInt(u64, bytes, 48),
            .config_sha256 = bytes[56..88].*,
            .module_state_hash = getInt(u64, bytes, 88),
        },
    };
}

fn magicForModule(kind: ModuleKind) *const [4]u8 {
    return switch (kind) {
        .matching_engine => matching_payload_magic,
        .risk_engine => risk_payload_magic,
    };
}

fn putInt(comptime T: type, bytes: []u8, offset: usize, value: T) void {
    std.mem.writeInt(T, bytes[offset..][0..@sizeOf(T)], value, .little);
}

fn getInt(comptime T: type, bytes: []const u8, offset: usize) T {
    return std.mem.readInt(T, bytes[offset..][0..@sizeOf(T)], .little);
}

fn syncParentDirectory(dir: std.Io.Dir, path: []const u8) (std.Io.File.OpenError || std.Io.File.SyncError)!void {
    const parent_path = std.fs.path.dirname(path) orelse ".";
    const parent = try dir.openFile(io_context.current(), parent_path, .{
        .mode = .read_only,
        .allow_directory = true,
        .path_only = false,
    });
    defer parent.close(io_context.current());
    try parent.sync(io_context.current());
}

fn testDigest(seed: u8) [sha256_len]u8 {
    var digest: [sha256_len]u8 = undefined;
    for (&digest, 0..) |*byte, index| byte.* = seed +% @as(u8, @truncate(index));
    return digest;
}

fn testBundle(seed: u8) [16]u8 {
    var bundle: [16]u8 = undefined;
    for (&bundle, 0..) |*byte, index| byte.* = seed +% @as(u8, @truncate(index));
    return bundle;
}

fn testManifestHeader(matching_count: u32, risk_count: u32) ManifestHeader {
    return .{
        .snapshot_id = 0x0102_0304_0506_0708,
        .matching_command_seq = 1000,
        .risk_command_seq = 1001,
        .publication_high_water = 1001,
        .timestamp_ns = 0x1112_1314_1516_1718,
        .matching_count = matching_count,
        .risk_count = risk_count,
        .matching_mask = maskForCount(matching_count),
        .risk_mask = maskForCount(risk_count),
        .aggregate_state_hash = 0x2122_2324_2526_2728,
        .config_sha256 = testDigest(0x30),
        .exchange_id_sha256 = testDigest(0x60),
        .bundle_id = testBundle(0x90),
        .entry_count = matching_count + risk_count,
    };
}

fn fillTestEntries(buffer: []ManifestEntry, header: ManifestHeader) void {
    std.debug.assert(buffer.len == @as(usize, header.entry_count));
    var cursor: usize = 0;
    var shard_id: u32 = 0;
    while (shard_id < header.matching_count) : (shard_id += 1) {
        buffer[cursor] = .{
            .module_kind = .matching_engine,
            .shard_id = shard_id,
            .command_seq = header.matching_command_seq,
            .byte_len = payload_header_len + 17 + shard_id,
            .payload_sha256 = testDigest(@intCast(1 + shard_id)),
            .module_state_hash = 0x1000 + shard_id,
        };
        cursor += 1;
    }
    shard_id = 0;
    while (shard_id < header.risk_count) : (shard_id += 1) {
        buffer[cursor] = .{
            .module_kind = .risk_engine,
            .shard_id = shard_id,
            .command_seq = header.risk_command_seq,
            .byte_len = payload_header_len + 23 + shard_id,
            .payload_sha256 = testDigest(@intCast(0x40 + shard_id)),
            .module_state_hash = 0x2000 + shard_id,
        };
        cursor += 1;
    }
}

fn resignManifestBytes(bytes: []u8) void {
    const trailer_offset = bytes.len - manifest_trailer_len;
    const digest = sha256(bytes[0..trailer_offset]);
    @memcpy(bytes[trailer_offset..], &digest);
}

fn decodeAllocationTest(allocator: std.mem.Allocator, bytes: []const u8) !void {
    var manifest = try decodeManifestAlloc(allocator, bytes, .{}, .{});
    defer manifest.deinit();
}

fn decodePayloadAllocationTest(
    allocator: std.mem.Allocator,
    bytes: []const u8,
    expected: PayloadExpectations,
) !void {
    var payload = try decodePayloadAlloc(allocator, bytes, .{}, expected);
    defer payload.deinit();
}

test "XCM2 manifest roundtrips independent 4ME/2RE and 1ME/4RE topology" {
    const allocator = std.testing.allocator;
    const topologies = [_][2]u32{
        .{ 4, 2 },
        .{ 1, 4 },
    };
    for (topologies) |topology| {
        const header = testManifestHeader(topology[0], topology[1]);
        const entries = try allocator.alloc(ManifestEntry, header.entry_count);
        defer allocator.free(entries);
        fillTestEntries(entries, header);

        const encoded = try encodeManifestAlloc(allocator, header, entries, .{});
        defer allocator.free(encoded);
        var decoded = try decodeManifestAlloc(allocator, encoded, .{}, .{
            .snapshot_id = header.snapshot_id,
            .matching_command_seq = header.matching_command_seq,
            .risk_command_seq = header.risk_command_seq,
            .publication_high_water = header.publication_high_water,
            .matching_count = header.matching_count,
            .risk_count = header.risk_count,
            .matching_mask = header.matching_mask,
            .risk_mask = header.risk_mask,
            .config_sha256 = header.config_sha256,
            .exchange_id_sha256 = header.exchange_id_sha256,
            .bundle_id = header.bundle_id,
        });
        defer decoded.deinit();

        try std.testing.expect(std.meta.eql(header, decoded.header));
        try std.testing.expectEqualSlices(ManifestEntry, entries, decoded.entries);
        try std.testing.expectEqualSlices(u8, &sha256(encoded[0 .. encoded.len - manifest_trailer_len]), &decoded.trailer_sha256);
        try std.testing.expectEqual(encoded.len, decoded.encodedLen());
        try std.testing.expect(decoded.entry(.matching_engine, header.matching_count - 1) != null);
        try std.testing.expect(decoded.entry(.risk_engine, header.risk_count - 1) != null);
    }
}

test "XCM2 golden layout uses exact little-endian offsets and lengths" {
    const allocator = std.testing.allocator;
    const header = testManifestHeader(4, 2);
    var entries: [6]ManifestEntry = undefined;
    fillTestEntries(&entries, header);
    const encoded = try encodeManifestAlloc(allocator, header, &entries, .{});
    defer allocator.free(encoded);

    try std.testing.expectEqual(@as(usize, 168 + 6 * 64 + 32), encoded.len);
    try std.testing.expectEqualSlices(u8, manifest_magic, encoded[0..4]);
    try std.testing.expectEqual(format_version, getInt(u16, encoded, 4));
    try std.testing.expectEqual(header.flags, getInt(u16, encoded, 6));
    try std.testing.expectEqual(header.snapshot_id, getInt(u64, encoded, 8));
    try std.testing.expectEqual(header.matching_command_seq, getInt(u64, encoded, 16));
    try std.testing.expectEqual(header.risk_command_seq, getInt(u64, encoded, 24));
    try std.testing.expectEqual(header.publication_high_water, getInt(u64, encoded, 32));
    try std.testing.expectEqual(header.timestamp_ns, getInt(u64, encoded, 40));
    try std.testing.expectEqual(header.matching_count, getInt(u32, encoded, 48));
    try std.testing.expectEqual(header.risk_count, getInt(u32, encoded, 52));
    try std.testing.expectEqual(header.matching_mask, getInt(u64, encoded, 56));
    try std.testing.expectEqual(header.risk_mask, getInt(u64, encoded, 64));
    try std.testing.expectEqual(header.aggregate_state_hash, getInt(u64, encoded, 72));
    try std.testing.expectEqualSlices(u8, &header.config_sha256, encoded[80..112]);
    try std.testing.expectEqualSlices(u8, &header.exchange_id_sha256, encoded[112..144]);
    try std.testing.expectEqualSlices(u8, &header.bundle_id, encoded[144..160]);
    try std.testing.expectEqual(header.entry_count, getInt(u32, encoded, 160));
    try std.testing.expectEqual(@intFromEnum(DigestAlgorithm.sha256), encoded[164]);
    try std.testing.expectEqual(@as(u8, sha256_len), encoded[165]);
    try std.testing.expectEqual(@as(u16, 0), getInt(u16, encoded, 166));

    const first = manifest_header_len;
    try std.testing.expectEqual(@intFromEnum(ModuleKind.matching_engine), encoded[first]);
    try std.testing.expectEqual(payload_version, encoded[first + 1]);
    try std.testing.expectEqual(@as(u32, 0), getInt(u32, encoded, first + 4));
    try std.testing.expectEqual(header.matching_command_seq, getInt(u64, encoded, first + 8));
    try std.testing.expectEqual(entries[0].byte_len, getInt(u64, encoded, first + 16));
    try std.testing.expectEqualSlices(u8, &entries[0].payload_sha256, encoded[first + 24 .. first + 56]);
    try std.testing.expectEqual(entries[0].module_state_hash, getInt(u64, encoded, first + 56));

    const first_risk = manifest_header_len + 4 * manifest_entry_len;
    try std.testing.expectEqual(@intFromEnum(ModuleKind.risk_engine), encoded[first_risk]);
    try std.testing.expectEqual(@as(u32, 0), getInt(u32, encoded, first_risk + 4));
    try std.testing.expectEqual(header.risk_command_seq, getInt(u64, encoded, first_risk + 8));
    const trailer = encoded.len - manifest_trailer_len;
    try std.testing.expectEqualSlices(u8, &sha256(encoded[0..trailer]), encoded[trailer..]);
}

test "XCM2 SHA-256 digest algorithm matches the standard known vector" {
    const expected = [_]u8{
        0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea,
        0x41, 0x41, 0x40, 0xde, 0x5d, 0xae, 0x22, 0x23,
        0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c,
        0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00, 0x15, 0xad,
    };
    try std.testing.expectEqualSlices(u8, &expected, &sha256("abc"));
}

test "XCM2 decoder rejects every manifest header corruption class" {
    const allocator = std.testing.allocator;
    const header = testManifestHeader(4, 2);
    var entries: [6]ManifestEntry = undefined;
    fillTestEntries(&entries, header);
    const original = try encodeManifestAlloc(allocator, header, &entries, .{});
    defer allocator.free(original);
    const bytes = try allocator.dupe(u8, original);
    defer allocator.free(bytes);

    bytes[0] ^= 1;
    resignManifestBytes(bytes);
    try std.testing.expectError(error.InvalidMagic, decodeManifestAlloc(allocator, bytes, .{}, .{}));
    @memcpy(bytes, original);
    putInt(u16, bytes, 4, 3);
    resignManifestBytes(bytes);
    try std.testing.expectError(error.UnsupportedVersion, decodeManifestAlloc(allocator, bytes, .{}, .{}));
    @memcpy(bytes, original);
    putInt(u16, bytes, 6, 1);
    resignManifestBytes(bytes);
    try std.testing.expectError(error.UnknownManifestFlags, decodeManifestAlloc(allocator, bytes, .{}, .{}));
    @memcpy(bytes, original);
    putInt(u32, bytes, 48, 3);
    resignManifestBytes(bytes);
    try std.testing.expectError(error.InvalidMatchingCount, decodeManifestAlloc(allocator, bytes, .{}, .{}));
    @memcpy(bytes, original);
    putInt(u32, bytes, 52, 0);
    resignManifestBytes(bytes);
    try std.testing.expectError(error.InvalidRiskCount, decodeManifestAlloc(allocator, bytes, .{}, .{}));
    @memcpy(bytes, original);
    putInt(u64, bytes, 56, 2);
    resignManifestBytes(bytes);
    try std.testing.expectError(error.InvalidMatchingMask, decodeManifestAlloc(allocator, bytes, .{}, .{}));
    @memcpy(bytes, original);
    putInt(u64, bytes, 64, 2);
    resignManifestBytes(bytes);
    try std.testing.expectError(error.InvalidRiskMask, decodeManifestAlloc(allocator, bytes, .{}, .{}));
    @memcpy(bytes, original);
    putInt(u64, bytes, 24, header.risk_command_seq + 1);
    resignManifestBytes(bytes);
    try std.testing.expectError(error.InvalidSequence, decodeManifestAlloc(allocator, bytes, .{}, .{}));
    @memcpy(bytes, original);
    putInt(u64, bytes, 32, header.publication_high_water + 1);
    resignManifestBytes(bytes);
    try std.testing.expectError(error.InvalidPublicationHighWater, decodeManifestAlloc(allocator, bytes, .{}, .{}));
    @memcpy(bytes, original);
    putInt(u32, bytes, 160, header.entry_count - 1);
    resignManifestBytes(bytes);
    try std.testing.expectError(error.EntryCountMismatch, decodeManifestAlloc(allocator, bytes, .{}, .{}));
    @memcpy(bytes, original);
    bytes[164] = 2;
    resignManifestBytes(bytes);
    try std.testing.expectError(error.UnsupportedDigestAlgorithm, decodeManifestAlloc(allocator, bytes, .{}, .{}));
    @memcpy(bytes, original);
    bytes[165] = 31;
    resignManifestBytes(bytes);
    try std.testing.expectError(error.InvalidDigestLength, decodeManifestAlloc(allocator, bytes, .{}, .{}));
    @memcpy(bytes, original);
    putInt(u16, bytes, 166, 1);
    resignManifestBytes(bytes);
    try std.testing.expectError(error.NonZeroReserved, decodeManifestAlloc(allocator, bytes, .{}, .{}));

    @memcpy(bytes, original);
    putInt(u32, bytes, 48, 0x8000_0000);
    putInt(u32, bytes, 52, 0x8000_0000);
    putInt(u64, bytes, 56, 0x7fff_ffff);
    putInt(u64, bytes, 64, 0x7fff_ffff);
    resignManifestBytes(bytes);
    try std.testing.expectError(error.EntryCountOverflow, decodeManifestAlloc(allocator, bytes, .{}, .{}));

    try std.testing.expectError(
        error.LimitExceeded,
        decodeManifestAlloc(allocator, original, .{ .max_entries = 5 }, .{}),
    );
}

test "XCM2 decoder rejects entry corruption duplicate gap and swapped canonical order" {
    const allocator = std.testing.allocator;
    const header = testManifestHeader(4, 2);
    var entries: [6]ManifestEntry = undefined;
    fillTestEntries(&entries, header);
    const original = try encodeManifestAlloc(allocator, header, &entries, .{});
    defer allocator.free(original);
    const bytes = try allocator.dupe(u8, original);
    defer allocator.free(bytes);
    const first = manifest_header_len;

    bytes[first] = 99;
    resignManifestBytes(bytes);
    try std.testing.expectError(error.InvalidModuleKind, decodeManifestAlloc(allocator, bytes, .{}, .{}));
    @memcpy(bytes, original);
    bytes[first + 1] = 1;
    resignManifestBytes(bytes);
    try std.testing.expectError(error.UnsupportedPayloadVersion, decodeManifestAlloc(allocator, bytes, .{}, .{}));
    @memcpy(bytes, original);
    putInt(u16, bytes, first + 2, 1);
    resignManifestBytes(bytes);
    try std.testing.expectError(error.UnknownEntryFlags, decodeManifestAlloc(allocator, bytes, .{}, .{}));
    @memcpy(bytes, original);
    putInt(u32, bytes, first + 4, header.matching_count);
    resignManifestBytes(bytes);
    try std.testing.expectError(error.InvalidEntryShard, decodeManifestAlloc(allocator, bytes, .{}, .{}));
    @memcpy(bytes, original);
    putInt(u64, bytes, first + 8, header.matching_command_seq + 1);
    resignManifestBytes(bytes);
    try std.testing.expectError(error.InvalidEntrySequence, decodeManifestAlloc(allocator, bytes, .{}, .{}));
    @memcpy(bytes, original);
    putInt(u64, bytes, first + 16, payload_header_len - 1);
    resignManifestBytes(bytes);
    try std.testing.expectError(error.InvalidPayloadLength, decodeManifestAlloc(allocator, bytes, .{}, .{}));

    @memcpy(bytes, original);
    putInt(u32, bytes, first + manifest_entry_len + 4, 0);
    resignManifestBytes(bytes);
    try std.testing.expectError(error.DuplicateShard, decodeManifestAlloc(allocator, bytes, .{}, .{}));

    @memcpy(bytes, original);
    putInt(u32, bytes, first + 2 * manifest_entry_len + 4, header.matching_count + 1);
    resignManifestBytes(bytes);
    try std.testing.expectError(error.InvalidEntryShard, decodeManifestAlloc(allocator, bytes, .{}, .{}));

    @memcpy(bytes, original);
    bytes[first + 3 * manifest_entry_len] = @intFromEnum(ModuleKind.risk_engine);
    putInt(u32, bytes, first + 3 * manifest_entry_len + 4, 0);
    putInt(u64, bytes, first + 3 * manifest_entry_len + 8, header.risk_command_seq);
    resignManifestBytes(bytes);
    try std.testing.expectError(error.MissingShard, decodeManifestAlloc(allocator, bytes, .{}, .{}));

    @memcpy(bytes, original);
    var scratch: [manifest_entry_len]u8 = undefined;
    @memcpy(&scratch, bytes[first..][0..manifest_entry_len]);
    @memcpy(bytes[first..][0..manifest_entry_len], bytes[first + manifest_entry_len ..][0..manifest_entry_len]);
    @memcpy(bytes[first + manifest_entry_len ..][0..manifest_entry_len], &scratch);
    resignManifestBytes(bytes);
    try std.testing.expectError(error.NonCanonicalEntryOrder, decodeManifestAlloc(allocator, bytes, .{}, .{}));
}

test "XCM2 decoder rejects checksum every truncation and trailing bytes" {
    const allocator = std.testing.allocator;
    const header = testManifestHeader(1, 4);
    var entries: [5]ManifestEntry = undefined;
    fillTestEntries(&entries, header);
    const encoded = try encodeManifestAlloc(allocator, header, &entries, .{});
    defer allocator.free(encoded);

    for (0..encoded.len) |cut| {
        try std.testing.expectError(error.Truncated, decodeManifestAlloc(allocator, encoded[0..cut], .{}, .{}));
    }

    const corrupt = try allocator.dupe(u8, encoded);
    defer allocator.free(corrupt);
    corrupt[80] ^= 1;
    try std.testing.expectError(error.DigestMismatch, decodeManifestAlloc(allocator, corrupt, .{}, .{}));

    const trailing = try allocator.alloc(u8, encoded.len + 1);
    defer allocator.free(trailing);
    @memcpy(trailing[0..encoded.len], encoded);
    trailing[encoded.len] = 0;
    try std.testing.expectError(error.TrailingBytes, decodeManifestAlloc(allocator, trailing, .{}, .{}));
}

test "XCM2 expectations reject wrong identity topology sequences config and bundle" {
    const allocator = std.testing.allocator;
    const header = testManifestHeader(4, 2);
    var entries: [6]ManifestEntry = undefined;
    fillTestEntries(&entries, header);
    const encoded = try encodeManifestAlloc(allocator, header, &entries, .{});
    defer allocator.free(encoded);

    try std.testing.expectError(error.SnapshotIdMismatch, decodeManifestAlloc(allocator, encoded, .{}, .{ .snapshot_id = header.snapshot_id + 1 }));
    try std.testing.expectError(error.SequenceMismatch, decodeManifestAlloc(allocator, encoded, .{}, .{ .matching_command_seq = header.matching_command_seq + 1 }));
    try std.testing.expectError(error.SequenceMismatch, decodeManifestAlloc(allocator, encoded, .{}, .{ .risk_command_seq = header.risk_command_seq + 1 }));
    try std.testing.expectError(error.SequenceMismatch, decodeManifestAlloc(allocator, encoded, .{}, .{ .publication_high_water = header.publication_high_water + 1 }));
    try std.testing.expectError(error.TopologyMismatch, decodeManifestAlloc(allocator, encoded, .{}, .{ .matching_count = 2 }));
    try std.testing.expectError(error.TopologyMismatch, decodeManifestAlloc(allocator, encoded, .{}, .{ .risk_count = 4 }));
    try std.testing.expectError(error.TopologyMismatch, decodeManifestAlloc(allocator, encoded, .{}, .{ .matching_mask = 1 }));
    try std.testing.expectError(error.TopologyMismatch, decodeManifestAlloc(allocator, encoded, .{}, .{ .risk_mask = 3 }));
    try std.testing.expectError(error.ConfigMismatch, decodeManifestAlloc(allocator, encoded, .{}, .{ .config_sha256 = testDigest(0x31) }));
    try std.testing.expectError(error.ExchangeMismatch, decodeManifestAlloc(allocator, encoded, .{}, .{ .exchange_id_sha256 = testDigest(0x61) }));
    try std.testing.expectError(error.BundleMismatch, decodeManifestAlloc(allocator, encoded, .{}, .{ .bundle_id = testBundle(0x91) }));
}

test "XCM2 format applies manifest and payload size caps before allocation" {
    const allocator = std.testing.allocator;
    const header = testManifestHeader(1, 1);
    var entries: [2]ManifestEntry = undefined;
    fillTestEntries(&entries, header);
    try std.testing.expectError(
        error.LimitExceeded,
        encodeManifestAlloc(allocator, header, &entries, .{ .max_manifest_bytes = 200 }),
    );

    entries[0].byte_len = 1025;
    try std.testing.expectError(
        error.LimitExceeded,
        encodeManifestAlloc(allocator, header, &entries, .{ .max_payload_bytes = 1024 }),
    );

    const payload_header: PayloadHeader = .{
        .snapshot_id = 1,
        .command_seq = 2,
        .observed_high_water = 2,
        .timestamp_ns = 3,
        .shard_id = 0,
        .shard_count = 1,
        .shard_mask = 0,
        .config_sha256 = testDigest(4),
        .module_state_hash = 5,
    };
    try std.testing.expectError(
        error.LimitExceeded,
        encodePayloadAlloc(allocator, .matching_engine, payload_header, "body", .{ .max_payload_bytes = payload_header_len + 3 }),
    );
    const encoded = try encodePayloadAlloc(allocator, .matching_engine, payload_header, "body", .{});
    defer allocator.free(encoded);
    try std.testing.expectError(
        error.LimitExceeded,
        decodePayloadAlloc(allocator, encoded, .{ .max_payload_bytes = payload_header_len + 3 }, .{}),
    );
}

test "XME2 and XRE2 payload containers roundtrip opaque bodies with exact 96-byte headers" {
    const allocator = std.testing.allocator;
    const cases = [_]struct { kind: ModuleKind, shard_id: u32, shard_count: u32 }{
        .{ .kind = .matching_engine, .shard_id = 3, .shard_count = 4 },
        .{ .kind = .risk_engine, .shard_id = 1, .shard_count = 2 },
    };
    for (cases) |case| {
        const header: PayloadHeader = .{
            .snapshot_id = 77,
            .command_seq = if (case.kind == .matching_engine) 500 else 501,
            .observed_high_water = 501,
            .timestamp_ns = 123_456_789,
            .shard_id = case.shard_id,
            .shard_count = case.shard_count,
            .shard_mask = maskForCount(case.shard_count),
            .config_sha256 = testDigest(9),
            .module_state_hash = 0xfeed_0000 + case.shard_id,
        };
        const body = "opaque-module-codec-body";
        const encoded = try encodePayloadAlloc(allocator, case.kind, header, body, .{});
        defer allocator.free(encoded);
        const digest = sha256(encoded);
        var decoded = try decodePayloadAlloc(allocator, encoded, .{}, .{
            .module_kind = case.kind,
            .snapshot_id = header.snapshot_id,
            .command_seq = header.command_seq,
            .observed_high_water = header.observed_high_water,
            .timestamp_ns = header.timestamp_ns,
            .shard_id = header.shard_id,
            .shard_count = header.shard_count,
            .shard_mask = header.shard_mask,
            .config_sha256 = header.config_sha256,
            .module_state_hash = header.module_state_hash,
            .byte_len = encoded.len,
            .payload_sha256 = digest,
        });
        defer decoded.deinit();
        try std.testing.expectEqual(case.kind, decoded.module_kind);
        try std.testing.expect(std.meta.eql(header, decoded.header));
        try std.testing.expectEqualSlices(u8, body, decoded.body);
        try std.testing.expectEqual(@as(u64, @intCast(encoded.len)), decoded.byte_len);
        try std.testing.expectEqualSlices(u8, &digest, &decoded.payload_sha256);

        try std.testing.expectEqualSlices(u8, magicForModule(case.kind), encoded[0..4]);
        try std.testing.expectEqual(format_version, getInt(u16, encoded, 4));
        try std.testing.expectEqual(header.snapshot_id, getInt(u64, encoded, 8));
        try std.testing.expectEqual(header.command_seq, getInt(u64, encoded, 16));
        try std.testing.expectEqual(header.observed_high_water, getInt(u64, encoded, 24));
        try std.testing.expectEqual(header.timestamp_ns, getInt(u64, encoded, 32));
        try std.testing.expectEqual(header.shard_id, getInt(u32, encoded, 40));
        try std.testing.expectEqual(header.shard_count, getInt(u32, encoded, 44));
        try std.testing.expectEqual(header.shard_mask, getInt(u64, encoded, 48));
        try std.testing.expectEqualSlices(u8, &header.config_sha256, encoded[56..88]);
        try std.testing.expectEqual(header.module_state_hash, getInt(u64, encoded, 88));
        try std.testing.expectEqualSlices(u8, body, encoded[payload_header_len..]);
    }
}

test "payload decoder validates manifest entry kind shard digest length header and EOF" {
    const allocator = std.testing.allocator;
    const header: PayloadHeader = .{
        .snapshot_id = 88,
        .command_seq = 700,
        .observed_high_water = 701,
        .timestamp_ns = 555,
        .shard_id = 1,
        .shard_count = 4,
        .shard_mask = 3,
        .config_sha256 = testDigest(4),
        .module_state_hash = 909,
    };
    const encoded = try encodePayloadAlloc(allocator, .matching_engine, header, "body", .{});
    defer allocator.free(encoded);
    const digest = sha256(encoded);
    const expected: PayloadExpectations = .{
        .module_kind = .matching_engine,
        .snapshot_id = header.snapshot_id,
        .command_seq = header.command_seq,
        .observed_high_water = header.observed_high_water,
        .timestamp_ns = header.timestamp_ns,
        .shard_id = header.shard_id,
        .shard_count = header.shard_count,
        .shard_mask = header.shard_mask,
        .config_sha256 = header.config_sha256,
        .module_state_hash = header.module_state_hash,
        .byte_len = encoded.len,
        .payload_sha256 = digest,
    };
    try std.testing.expectError(error.PayloadKindMismatch, decodePayloadAlloc(allocator, encoded, .{}, .{ .module_kind = .risk_engine }));
    try std.testing.expectError(error.PayloadShardMismatch, decodePayloadAlloc(allocator, encoded, .{}, .{ .shard_id = 2 }));
    try std.testing.expectError(error.PayloadDigestMismatch, decodePayloadAlloc(allocator, encoded, .{}, .{ .payload_sha256 = testDigest(33) }));
    try std.testing.expectError(error.Truncated, decodePayloadAlloc(allocator, encoded, .{}, .{ .byte_len = encoded.len + 1 }));
    try std.testing.expectError(error.TrailingBytes, decodePayloadAlloc(allocator, encoded, .{}, .{ .byte_len = encoded.len - 1 }));
    try std.testing.expectError(error.SnapshotIdMismatch, decodePayloadAlloc(allocator, encoded, .{}, .{ .snapshot_id = header.snapshot_id + 1 }));
    try std.testing.expectError(error.SequenceMismatch, decodePayloadAlloc(allocator, encoded, .{}, .{ .command_seq = header.command_seq + 1 }));
    try std.testing.expectError(error.PayloadHeaderMismatch, decodePayloadAlloc(allocator, encoded, .{}, .{ .observed_high_water = header.observed_high_water + 1 }));
    try std.testing.expectError(error.PayloadHeaderMismatch, decodePayloadAlloc(allocator, encoded, .{}, .{ .timestamp_ns = header.timestamp_ns + 1 }));
    try std.testing.expectError(error.TopologyMismatch, decodePayloadAlloc(allocator, encoded, .{}, .{ .shard_count = 2 }));
    try std.testing.expectError(error.TopologyMismatch, decodePayloadAlloc(allocator, encoded, .{}, .{ .shard_mask = 1 }));
    try std.testing.expectError(error.ConfigMismatch, decodePayloadAlloc(allocator, encoded, .{}, .{ .config_sha256 = testDigest(5) }));
    try std.testing.expectError(error.PayloadHeaderMismatch, decodePayloadAlloc(allocator, encoded, .{}, .{ .module_state_hash = header.module_state_hash + 1 }));

    const corrupt = try allocator.dupe(u8, encoded);
    defer allocator.free(corrupt);
    corrupt[payload_header_len] ^= 1;
    try std.testing.expectError(error.PayloadDigestMismatch, decodePayloadAlloc(allocator, corrupt, .{}, expected));

    const trailing = try allocator.alloc(u8, encoded.len + 1);
    defer allocator.free(trailing);
    @memcpy(trailing[0..encoded.len], encoded);
    trailing[encoded.len] = 1;
    try std.testing.expectError(error.TrailingBytes, decodePayloadAlloc(allocator, trailing, .{}, expected));

    const malformed = try allocator.dupe(u8, encoded);
    defer allocator.free(malformed);
    malformed[0] = 'Z';
    try std.testing.expectError(error.InvalidMagic, decodePayloadAlloc(allocator, malformed, .{}, .{}));
    @memcpy(malformed, encoded);
    putInt(u16, malformed, 4, 3);
    try std.testing.expectError(error.UnsupportedVersion, decodePayloadAlloc(allocator, malformed, .{}, .{}));
    @memcpy(malformed, encoded);
    putInt(u16, malformed, 6, 1);
    try std.testing.expectError(error.UnknownPayloadFlags, decodePayloadAlloc(allocator, malformed, .{}, .{}));
    @memcpy(malformed, encoded);
    putInt(u64, malformed, 24, header.command_seq - 1);
    try std.testing.expectError(error.InvalidObservedHighWater, decodePayloadAlloc(allocator, malformed, .{}, .{}));
    @memcpy(malformed, encoded);
    putInt(u32, malformed, 44, 3);
    try std.testing.expectError(error.TopologyMismatch, decodePayloadAlloc(allocator, malformed, .{}, .{}));
    @memcpy(malformed, encoded);
    putInt(u64, malformed, 48, 1);
    try std.testing.expectError(error.TopologyMismatch, decodePayloadAlloc(allocator, malformed, .{}, .{}));
    @memcpy(malformed, encoded);
    putInt(u32, malformed, 40, header.shard_count);
    try std.testing.expectError(error.InvalidEntryShard, decodePayloadAlloc(allocator, malformed, .{}, .{}));
}

test "payload expectations derived from XCM2 bind every module identity field" {
    const allocator = std.testing.allocator;
    var manifest_header = testManifestHeader(1, 1);
    const payload_header: PayloadHeader = .{
        .snapshot_id = manifest_header.snapshot_id,
        .command_seq = manifest_header.matching_command_seq,
        .observed_high_water = manifest_header.publication_high_water,
        .timestamp_ns = manifest_header.timestamp_ns,
        .shard_id = 0,
        .shard_count = 1,
        .shard_mask = 0,
        .config_sha256 = manifest_header.config_sha256,
        .module_state_hash = 1234,
    };
    const payload_bytes = try encodePayloadAlloc(allocator, .matching_engine, payload_header, "me-body", .{});
    defer allocator.free(payload_bytes);
    var entries: [2]ManifestEntry = undefined;
    fillTestEntries(&entries, manifest_header);
    entries[0].byte_len = @intCast(payload_bytes.len);
    entries[0].payload_sha256 = sha256(payload_bytes);
    entries[0].module_state_hash = payload_header.module_state_hash;
    const manifest_bytes = try encodeManifestAlloc(allocator, manifest_header, &entries, .{});
    defer allocator.free(manifest_bytes);
    var manifest = try decodeManifestAlloc(allocator, manifest_bytes, .{}, .{});
    defer manifest.deinit();
    const entry_value = manifest.entry(.matching_engine, 0).?.*;
    try std.testing.expectEqual(manifest.header.matching_command_seq, entry_value.command_seq);
    try std.testing.expect(entry_value.command_seq != manifest.header.publication_high_water);
    const derived = PayloadExpectations.fromManifest(&manifest, entry_value);
    try std.testing.expectEqual(manifest.header.publication_high_water, derived.observed_high_water.?);
    var payload = try decodePayloadAlloc(allocator, payload_bytes, .{}, PayloadExpectations.fromManifest(&manifest, entry_value));
    defer payload.deinit();
    try std.testing.expectEqualSlices(u8, "me-body", payload.body);

    manifest_header.bundle_id[0] ^= 1;
    try std.testing.expectError(error.BundleMismatch, decodeManifestAlloc(allocator, manifest_bytes, .{}, .{ .bundle_id = manifest_header.bundle_id }));
}

test "XCM2 atomic no-replace collision preserves committed bytes" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const header = testManifestHeader(1, 1);
    var entries: [2]ManifestEntry = undefined;
    fillTestEntries(&entries, header);
    try writeManifestFileAtomic(allocator, tmp.dir, "snapshot.ecm", header, &entries, .{});
    const before = try readBoundedFileAlloc(allocator, tmp.dir, "snapshot.ecm", 4096);
    defer allocator.free(before);

    var conflicting = header;
    conflicting.timestamp_ns += 1;
    try std.testing.expectError(
        error.PathAlreadyExists,
        writeManifestFileAtomic(allocator, tmp.dir, "snapshot.ecm", conflicting, &entries, .{}),
    );
    const after = try readBoundedFileAlloc(allocator, tmp.dir, "snapshot.ecm", 4096);
    defer allocator.free(after);
    try std.testing.expectEqualSlices(u8, before, after);

    const payload_header: PayloadHeader = .{
        .snapshot_id = 1,
        .command_seq = 2,
        .observed_high_water = 2,
        .timestamp_ns = 3,
        .shard_id = 0,
        .shard_count = 1,
        .shard_mask = 0,
        .config_sha256 = testDigest(4),
        .module_state_hash = 5,
    };
    _ = try writePayloadFileAtomic(allocator, tmp.dir, "payload.ecs", .matching_engine, payload_header, "committed", .{});
    const payload_before = try readBoundedFileAlloc(allocator, tmp.dir, "payload.ecs", 4096);
    defer allocator.free(payload_before);
    try std.testing.expectError(
        error.PathAlreadyExists,
        writePayloadFileAtomic(allocator, tmp.dir, "payload.ecs", .matching_engine, payload_header, "conflict", .{}),
    );
    const payload_after = try readBoundedFileAlloc(allocator, tmp.dir, "payload.ecs", 4096);
    defer allocator.free(payload_after);
    try std.testing.expectEqualSlices(u8, payload_before, payload_after);
}

test "XCM2 atomic publication syncs nested parent and bounded readers validate files" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io_context.current(), "nested/checkpoints");
    const header = testManifestHeader(1, 1);
    var entries: [2]ManifestEntry = undefined;
    fillTestEntries(&entries, header);
    const path = "nested/checkpoints/snapshot.ecm";
    try writeManifestFileAtomic(allocator, tmp.dir, path, header, &entries, .{});
    var decoded = try readManifestFileAlloc(allocator, tmp.dir, path, .{}, .{ .snapshot_id = header.snapshot_id });
    defer decoded.deinit();
    try std.testing.expectEqual(@as(usize, 2), decoded.entries.len);
    try std.testing.expectError(error.FileTooLarge, readBoundedFileAlloc(allocator, tmp.dir, path, 8));

    const payload_header: PayloadHeader = .{
        .snapshot_id = 9,
        .command_seq = 10,
        .observed_high_water = 10,
        .timestamp_ns = 11,
        .shard_id = 0,
        .shard_count = 1,
        .shard_mask = 0,
        .config_sha256 = testDigest(12),
        .module_state_hash = 13,
    };
    const entry_value = try writePayloadFileAtomic(
        allocator,
        tmp.dir,
        "nested/checkpoints/me.ecs",
        .matching_engine,
        payload_header,
        "opaque",
        .{},
    );
    var payload = try readPayloadFileAlloc(allocator, tmp.dir, "nested/checkpoints/me.ecs", .{}, .{
        .module_kind = entry_value.module_kind,
        .snapshot_id = payload_header.snapshot_id,
        .command_seq = entry_value.command_seq,
        .shard_id = entry_value.shard_id,
        .byte_len = entry_value.byte_len,
        .payload_sha256 = entry_value.payload_sha256,
        .module_state_hash = entry_value.module_state_hash,
    });
    defer payload.deinit();
    try std.testing.expectEqualSlices(u8, "opaque", payload.body);
}

test "XCM2 manifest and payload decoders deinit every allocation-failure path" {
    const allocator = std.testing.allocator;
    const header = testManifestHeader(4, 2);
    var entries: [6]ManifestEntry = undefined;
    fillTestEntries(&entries, header);
    const manifest_bytes = try encodeManifestAlloc(allocator, header, &entries, .{});
    defer allocator.free(manifest_bytes);
    try std.testing.checkAllAllocationFailures(allocator, decodeAllocationTest, .{manifest_bytes});

    const payload_header: PayloadHeader = .{
        .snapshot_id = 1,
        .command_seq = 2,
        .observed_high_water = 3,
        .timestamp_ns = 4,
        .shard_id = 0,
        .shard_count = 1,
        .shard_mask = 0,
        .config_sha256 = testDigest(5),
        .module_state_hash = 6,
    };
    const payload_bytes = try encodePayloadAlloc(allocator, .risk_engine, payload_header, "allocation-body", .{});
    defer allocator.free(payload_bytes);
    try std.testing.checkAllAllocationFailures(allocator, decodePayloadAllocationTest, .{
        payload_bytes,
        PayloadExpectations{
            .module_kind = .risk_engine,
            .byte_len = payload_bytes.len,
            .payload_sha256 = sha256(payload_bytes),
        },
    });
}
