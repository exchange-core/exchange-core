const std = @import("std");
const io_context = @import("io_context.zig");
const java_binary_wire = @import("java_binary_wire.zig");

const Lz4FrameMagic: u32 = 0x184D2204;
const Lz4SkippableMagicMin: u32 = 0x184D2A50;
const Lz4SkippableMagicMax: u32 = 0x184D2A5F;
const HeaderProbeBytes: usize = 32;
const JavaSnapshotBlockBytes: usize = 4 * 1024 * 1024;
const Lz4RawBlockMask: u32 = 0x8000_0000;

pub const MagicKind = enum {
    zig_snapshot_v1,
    zig_snapshot_v2,
    zig_journal_v1,
    zig_journal_v2,
    legacy_lz4,
    unknown,
    truncated,
};

pub const LegacyLz4FrameKind = enum {
    standard,
    skippable,
};

pub const LegacyLz4StandardHeader = struct {
    version: u2,
    block_independence: bool,
    block_checksum: bool,
    content_size_present: bool,
    content_checksum: bool,
    dictionary_id_present: bool,
    block_max_size_code: u3,
    content_size: ?u64,
    dictionary_id: ?u32,
    descriptor_checksum: u8,
    descriptor_len: usize,
    header_len: usize,
};

pub const LegacyLz4SkippableHeader = struct {
    user_magic_tag: u8,
    payload_len: u32,
    header_len: usize,
};

pub const LegacyLz4FrameHeader = union(LegacyLz4FrameKind) {
    standard: LegacyLz4StandardHeader,
    skippable: LegacyLz4SkippableHeader,

    pub fn headerLength(self: LegacyLz4FrameHeader) usize {
        return switch (self) {
            .standard => |header| header.header_len,
            .skippable => |header| header.header_len,
        };
    }
};

pub const ProbeFileError = std.Io.File.OpenError || std.Io.File.Reader.Error;

pub const ParseLegacyLz4HeaderError = error{
    TruncatedFrameMagic,
    InvalidLegacyMagic,
    TruncatedStandardHeaderDescriptor,
    TruncatedSkippableHeader,
    UnsupportedFrameVersion,
    ReservedFlagBitSet,
    ReservedBlockDescriptorBitsSet,
    UnsupportedBlockMaxSizeCode,
    HeaderLengthOverflow,
    InvalidHeaderChecksum,
};

pub const LegacyLz4DecodeLimits = struct {
    /// Bounds the complete in-memory archive before parsing any length field.
    max_file_size: usize = 512 * 1024 * 1024,
    /// Bounds the aggregate uncompressed Chronicle Wire payload.
    max_output_size: usize = 512 * 1024 * 1024,
    /// Bounds both stored block bytes and decoded block bytes. The Java snapshot
    /// profile advertises exactly 4 MiB blocks, so smaller values intentionally
    /// disable decoding that profile.
    max_block_size: usize = JavaSnapshotBlockBytes,
};

pub const DecodeLegacyLz4SnapshotError = ParseLegacyLz4HeaderError ||
    java_binary_wire.BoundedRawLz4DecodeError || error{
    FileSizeLimitExceeded,
    OutputSizeLimitExceeded,
    BlockSizeLimitExceeded,
    UnsupportedSnapshotBlockMaxSize,
    UnsupportedDependentBlocks,
    UnsupportedDictionaryId,
    UnsupportedSkippableFrame,
    TruncatedBlockHeader,
    InvalidRawEndMarker,
    TruncatedBlockData,
    TruncatedBlockChecksum,
    BlockChecksumMismatch,
    DecodedBlockTooLarge,
    EmptyDecodedBlock,
    TruncatedContentChecksum,
    ContentChecksumMismatch,
    ContentSizeValueTooLarge,
    ContentSizeMismatch,
    TrailingFrameData,
};

pub const DecodedLegacyLz4Snapshot = struct {
    bytes: []u8,

    pub fn deinit(self: *DecodedLegacyLz4Snapshot, allocator: std.mem.Allocator) void {
        allocator.free(self.bytes);
        self.* = undefined;
    }
};

pub const ParseLegacyLz4FileHeaderError = ProbeFileError || ParseLegacyLz4HeaderError;

pub const LegacyImportRequest = struct {
    snapshot_archive_path: ?[]const u8 = null,
    journal_archive_path: ?[]const u8 = null,
    output_folder: []const u8,

    pub const ValidationError = error{
        EmptyOutputFolder,
        MissingInputArchive,
    };

    pub fn validate(self: LegacyImportRequest) ValidationError!void {
        if (self.output_folder.len == 0) return error.EmptyOutputFolder;
        if (self.snapshot_archive_path == null and self.journal_archive_path == null) {
            return error.MissingInputArchive;
        }
    }
};

pub const PreflightResult = struct {
    snapshot_magic: ?MagicKind = null,
    journal_magic: ?MagicKind = null,
    snapshot_header: ?LegacyLz4FrameHeader = null,
    journal_header: ?LegacyLz4FrameHeader = null,
};

pub const PreflightError = ParseLegacyLz4FileHeaderError || LegacyImportRequest.ValidationError || error{
    SnapshotArchiveNotLegacyLz4,
    JournalArchiveNotLegacyLz4,
};

pub fn detectMagic(magic_prefix: []const u8) MagicKind {
    if (magic_prefix.len < 4) return .truncated;

    const magic = magic_prefix[0..4];
    if (std.mem.eql(u8, magic, "XCS1")) return .zig_snapshot_v1;
    if (std.mem.eql(u8, magic, "XCS2")) return .zig_snapshot_v2;
    if (std.mem.eql(u8, magic, "XCJ1")) return .zig_journal_v1;
    if (std.mem.eql(u8, magic, "XCJ2")) return .zig_journal_v2;
    if (isLegacyLz4FrameMagic(magic)) return .legacy_lz4;
    return .unknown;
}

pub fn probeFileMagic(
    dir: std.Io.Dir,
    path: []const u8,
) ProbeFileError!MagicKind {
    const file = try dir.openFile(io_context.current(), path, .{});
    defer file.close(io_context.current());

    var magic: [4]u8 = undefined;
    const bytes_read = try io_context.readAll(file, &magic);
    return detectMagic(magic[0..bytes_read]);
}

pub fn parseLegacyLz4FrameHeader(
    header_prefix: []const u8,
) ParseLegacyLz4HeaderError!LegacyLz4FrameHeader {
    if (header_prefix.len < 4) return error.TruncatedFrameMagic;

    const magic = std.mem.readInt(u32, header_prefix[0..4], .little);
    if (magic == Lz4FrameMagic) {
        return parseStandardFrameHeader(header_prefix);
    }
    if (magic >= Lz4SkippableMagicMin and magic <= Lz4SkippableMagicMax) {
        return parseSkippableFrameHeader(magic, header_prefix);
    }
    return error.InvalidLegacyMagic;
}

pub fn parseLegacyLz4FileHeader(
    dir: std.Io.Dir,
    path: []const u8,
) ParseLegacyLz4FileHeaderError!LegacyLz4FrameHeader {
    const file = try dir.openFile(io_context.current(), path, .{});
    defer file.close(io_context.current());

    var header_probe: [HeaderProbeBytes]u8 = undefined;
    const bytes_read = try io_context.readAll(file, &header_probe);
    return parseLegacyLz4FrameHeader(header_probe[0..bytes_read]);
}

pub fn preflightLegacyImport(
    dir: std.Io.Dir,
    request: LegacyImportRequest,
) PreflightError!PreflightResult {
    try request.validate();

    var result: PreflightResult = .{};

    if (request.snapshot_archive_path) |path| {
        const snapshot_header = parseLegacyLz4FileHeader(dir, path) catch |err| switch (err) {
            error.InvalidLegacyMagic => return error.SnapshotArchiveNotLegacyLz4,
            else => return err,
        };
        result.snapshot_magic = .legacy_lz4;
        result.snapshot_header = snapshot_header;
    }

    if (request.journal_archive_path) |path| {
        const journal_header = parseLegacyLz4FileHeader(dir, path) catch |err| switch (err) {
            error.InvalidLegacyMagic => return error.JournalArchiveNotLegacyLz4,
            else => return err,
        };
        result.journal_magic = .legacy_lz4;
        result.journal_header = journal_header;
    }

    return result;
}

/// Decodes exactly one lz4-java `LZ4FrameOutputStream` snapshot frame in
/// memory. DiskSerializationProcessor writes independent 4 MiB blocks and no
/// dictionary; accepting any other profile here risks treating a different LZ4
/// use (notably journals or concatenated/skippable streams) as a snapshot.
pub fn decodeLegacyLz4SnapshotFrame(
    allocator: std.mem.Allocator,
    frame: []const u8,
    limits: LegacyLz4DecodeLimits,
) DecodeLegacyLz4SnapshotError!DecodedLegacyLz4Snapshot {
    if (frame.len > limits.max_file_size) return error.FileSizeLimitExceeded;

    const parsed_header = try parseLegacyLz4FrameHeader(frame);
    const header = switch (parsed_header) {
        .standard => |standard| standard,
        .skippable => return error.UnsupportedSkippableFrame,
    };
    if (!header.block_independence) return error.UnsupportedDependentBlocks;
    if (header.dictionary_id_present) return error.UnsupportedDictionaryId;
    if (header.block_max_size_code != 7) return error.UnsupportedSnapshotBlockMaxSize;

    const advertised_block_size = blockMaxSize(header.block_max_size_code);
    if (advertised_block_size > limits.max_block_size) return error.BlockSizeLimitExceeded;

    const expected_content_size: ?usize = if (header.content_size) |content_size_u64| blk: {
        const content_size = std.math.cast(usize, content_size_u64) orelse {
            return error.ContentSizeValueTooLarge;
        };
        if (content_size > limits.max_output_size) return error.OutputSizeLimitExceeded;
        break :blk content_size;
    } else null;

    var output: std.ArrayListUnmanaged(u8) = .empty;
    defer output.deinit(allocator);

    var content_hash = std.hash.XxHash32.init(0);
    var cursor = header.header_len;
    while (true) {
        if (frame.len - cursor < @sizeOf(u32)) return error.TruncatedBlockHeader;
        const encoded_block_size = std.mem.readInt(u32, frame[cursor..][0..4], .little);
        cursor += @sizeOf(u32);

        const is_raw = (encoded_block_size & Lz4RawBlockMask) != 0;
        const block_size_u32 = encoded_block_size & ~Lz4RawBlockMask;
        const block_size: usize = block_size_u32;
        if (block_size == 0) {
            if (is_raw) return error.InvalidRawEndMarker;

            if (header.content_checksum) {
                if (frame.len - cursor < @sizeOf(u32)) return error.TruncatedContentChecksum;
                const expected_hash = std.mem.readInt(u32, frame[cursor..][0..4], .little);
                cursor += @sizeOf(u32);
                if (expected_hash != content_hash.final()) return error.ContentChecksumMismatch;
            }
            if (expected_content_size) |expected_size| {
                if (expected_size != output.items.len) return error.ContentSizeMismatch;
            }
            if (cursor != frame.len) return error.TrailingFrameData;

            return .{ .bytes = try output.toOwnedSlice(allocator) };
        }

        if (block_size > advertised_block_size or block_size > limits.max_block_size) {
            return error.BlockSizeLimitExceeded;
        }
        if (block_size > frame.len - cursor) return error.TruncatedBlockData;
        const block_payload = frame[cursor .. cursor + block_size];
        cursor += block_size;

        if (header.block_checksum) {
            if (frame.len - cursor < @sizeOf(u32)) return error.TruncatedBlockChecksum;
            const expected_hash = std.mem.readInt(u32, frame[cursor..][0..4], .little);
            cursor += @sizeOf(u32);
            if (expected_hash != std.hash.XxHash32.hash(0, block_payload)) {
                return error.BlockChecksumMismatch;
            }
        }

        if (is_raw) {
            try appendFrameOutput(allocator, &output, block_payload, limits.max_output_size);
            if (header.content_checksum) content_hash.update(block_payload);
        } else {
            const remaining_output = limits.max_output_size - output.items.len;
            const decoded_block_limit = @min(advertised_block_size, remaining_output);
            var decoded = java_binary_wire.decodeRawLz4Bounded(
                allocator,
                block_payload,
                decoded_block_limit,
            ) catch |err| switch (err) {
                error.OutputLimitExceeded => if (remaining_output < advertised_block_size)
                    return error.OutputSizeLimitExceeded
                else
                    return error.DecodedBlockTooLarge,
                else => return err,
            };
            defer decoded.deinit(allocator);
            if (decoded.bytes.len == 0) return error.EmptyDecodedBlock;
            try appendFrameOutput(allocator, &output, decoded.bytes, limits.max_output_size);
            if (header.content_checksum) content_hash.update(decoded.bytes);
        }
    }
}

fn parseStandardFrameHeader(
    header_prefix: []const u8,
) ParseLegacyLz4HeaderError!LegacyLz4FrameHeader {
    if (header_prefix.len < 7) return error.TruncatedStandardHeaderDescriptor;

    const flg = header_prefix[4];
    const bd = header_prefix[5];

    const version_raw: u8 = (flg >> 6) & 0b11;
    if (version_raw != 0b01) return error.UnsupportedFrameVersion;
    if ((flg & 0b0000_0010) != 0) return error.ReservedFlagBitSet;

    if ((bd & 0b1000_1111) != 0) return error.ReservedBlockDescriptorBitsSet;
    const block_max_size_code_raw: u8 = (bd >> 4) & 0b111;
    if (block_max_size_code_raw < 4 or block_max_size_code_raw > 7) {
        return error.UnsupportedBlockMaxSizeCode;
    }

    const descriptor_len = try standardDescriptorLengthFromFlags(flg);
    const header_len = std.math.add(usize, 4, descriptor_len) catch return error.HeaderLengthOverflow;
    if (header_prefix.len < header_len) return error.TruncatedStandardHeaderDescriptor;

    const checksum_index = header_len - 1;
    const descriptor_checksum = header_prefix[checksum_index];
    const computed_checksum: u8 = @truncate(std.hash.XxHash32.hash(
        0,
        header_prefix[4..checksum_index],
    ) >> 8);
    if (descriptor_checksum != computed_checksum) return error.InvalidHeaderChecksum;

    var descriptor_cursor: usize = 6;
    const content_size = if ((flg & 0b0000_1000) != 0) blk: {
        const value = std.mem.readInt(u64, header_prefix[descriptor_cursor..][0..8], .little);
        descriptor_cursor += @sizeOf(u64);
        break :blk value;
    } else null;
    const dictionary_id = if ((flg & 0b0000_0001) != 0) blk: {
        const value = std.mem.readInt(u32, header_prefix[descriptor_cursor..][0..4], .little);
        descriptor_cursor += @sizeOf(u32);
        break :blk value;
    } else null;
    std.debug.assert(descriptor_cursor == checksum_index);

    return .{
        .standard = .{
            .version = @intCast(version_raw),
            .block_independence = (flg & 0b0010_0000) != 0,
            .block_checksum = (flg & 0b0001_0000) != 0,
            .content_size_present = (flg & 0b0000_1000) != 0,
            .content_checksum = (flg & 0b0000_0100) != 0,
            .dictionary_id_present = (flg & 0b0000_0001) != 0,
            .block_max_size_code = @intCast(block_max_size_code_raw),
            .content_size = content_size,
            .dictionary_id = dictionary_id,
            .descriptor_checksum = descriptor_checksum,
            .descriptor_len = descriptor_len,
            .header_len = header_len,
        },
    };
}

fn parseSkippableFrameHeader(
    magic: u32,
    header_prefix: []const u8,
) ParseLegacyLz4HeaderError!LegacyLz4FrameHeader {
    if (header_prefix.len < 8) return error.TruncatedSkippableHeader;

    const payload_len = std.mem.readInt(u32, header_prefix[4..8], .little);
    const low_byte: u8 = @truncate(magic & 0xFF);
    const user_magic_tag: u8 = low_byte - 0x50;

    return .{
        .skippable = .{
            .user_magic_tag = user_magic_tag,
            .payload_len = payload_len,
            .header_len = 8,
        },
    };
}

fn standardDescriptorLengthFromFlags(flg: u8) ParseLegacyLz4HeaderError!usize {
    var descriptor_len: usize = 3; // FLG + BD + HC
    if ((flg & 0b0000_1000) != 0) {
        descriptor_len = std.math.add(usize, descriptor_len, 8) catch return error.HeaderLengthOverflow;
    }
    if ((flg & 0b0000_0001) != 0) {
        descriptor_len = std.math.add(usize, descriptor_len, 4) catch return error.HeaderLengthOverflow;
    }
    return descriptor_len;
}

fn blockMaxSize(code: u3) usize {
    return @as(usize, 1) << @intCast(2 * @as(u8, code) + 8);
}

fn appendFrameOutput(
    allocator: std.mem.Allocator,
    output: *std.ArrayListUnmanaged(u8),
    bytes: []const u8,
    max_output_size: usize,
) DecodeLegacyLz4SnapshotError!void {
    if (bytes.len > max_output_size -| output.items.len) return error.OutputSizeLimitExceeded;
    try output.appendSlice(allocator, bytes);
}

fn isLegacyLz4FrameMagic(magic: []const u8) bool {
    std.debug.assert(magic.len == 4);

    if (magic[0] == 0x04 and magic[1] == 0x22 and magic[2] == 0x4D and magic[3] == 0x18) {
        return true;
    }
    if (magic[1] == 0x2A and magic[2] == 0x4D and magic[3] == 0x18 and magic[0] >= 0x50 and magic[0] <= 0x5F) {
        return true;
    }
    return false;
}

test "detectMagic classifies zig, legacy lz4, unknown, and truncated prefixes" {
    try std.testing.expectEqual(MagicKind.zig_snapshot_v1, detectMagic("XCS1"));
    try std.testing.expectEqual(MagicKind.zig_snapshot_v2, detectMagic("XCS2"));
    try std.testing.expectEqual(MagicKind.zig_journal_v1, detectMagic("XCJ1"));
    try std.testing.expectEqual(MagicKind.zig_journal_v2, detectMagic("XCJ2"));

    const legacy_frame = [_]u8{ 0x04, 0x22, 0x4D, 0x18 };
    try std.testing.expectEqual(MagicKind.legacy_lz4, detectMagic(legacy_frame[0..]));

    const legacy_skippable = [_]u8{ 0x50, 0x2A, 0x4D, 0x18 };
    try std.testing.expectEqual(MagicKind.legacy_lz4, detectMagic(legacy_skippable[0..]));

    try std.testing.expectEqual(MagicKind.unknown, detectMagic("ABCD"));
    try std.testing.expectEqual(MagicKind.truncated, detectMagic("XC"));
}

test "parseLegacyLz4FrameHeader parses standard frame descriptor fields" {
    const header = [_]u8{
        0x04, 0x22, 0x4D, 0x18, // magic
        0x60, // FLG: version=01, block independence
        0x70, // BD: block max size code=7
        0x73, // HC: (XXH32(0, { 0x60, 0x70 }) >> 8) & 0xff
    };

    const parsed = try parseLegacyLz4FrameHeader(header[0..]);
    switch (parsed) {
        .standard => |standard| {
            try std.testing.expectEqual(@as(u2, 0b01), standard.version);
            try std.testing.expect(standard.block_independence);
            try std.testing.expect(!standard.block_checksum);
            try std.testing.expect(!standard.content_size_present);
            try std.testing.expect(!standard.content_checksum);
            try std.testing.expect(!standard.dictionary_id_present);
            try std.testing.expectEqual(@as(u3, 7), standard.block_max_size_code);
            try std.testing.expectEqual(@as(?u64, null), standard.content_size);
            try std.testing.expectEqual(@as(?u32, null), standard.dictionary_id);
            try std.testing.expectEqual(@as(u8, 0x73), standard.descriptor_checksum);
            try std.testing.expectEqual(@as(usize, 3), standard.descriptor_len);
            try std.testing.expectEqual(@as(usize, 7), standard.header_len);
        },
        .skippable => return error.ExpectedStandardHeader,
    }
}

test "parseLegacyLz4FrameHeader parses standard frame with content-size and dict-id flags" {
    var header = [_]u8{
        0x04, 0x22, 0x4D, 0x18, // magic
        0x69, // FLG: version=01, block independence, content-size, dict-id
        0x70, // BD: block max size code=7
        // content size (8)
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        // dict id (4)
        0x11,
        0x22,
        0x33,
        0x44,
        // HC
        0x00,
    };
    header[header.len - 1] = @truncate(std.hash.XxHash32.hash(0, header[4 .. header.len - 1]) >> 8);

    const parsed = try parseLegacyLz4FrameHeader(header[0..]);
    switch (parsed) {
        .standard => |standard| {
            try std.testing.expect(standard.content_size_present);
            try std.testing.expect(standard.dictionary_id_present);
            try std.testing.expectEqual(@as(?u64, 0), standard.content_size);
            try std.testing.expectEqual(@as(?u32, 0x4433_2211), standard.dictionary_id);
            try std.testing.expectEqual(@as(usize, 15), standard.descriptor_len);
            try std.testing.expectEqual(@as(usize, 19), standard.header_len);
        },
        .skippable => return error.ExpectedStandardHeader,
    }
}

test "parseLegacyLz4FrameHeader parses skippable frame header" {
    const header = [_]u8{
        0x53, 0x2A, 0x4D, 0x18, // skippable magic tag=3
        0x10, 0x00, 0x00, 0x00, // payload len 16
    };

    const parsed = try parseLegacyLz4FrameHeader(header[0..]);
    switch (parsed) {
        .standard => return error.ExpectedSkippableHeader,
        .skippable => |skippable| {
            try std.testing.expectEqual(@as(u8, 3), skippable.user_magic_tag);
            try std.testing.expectEqual(@as(u32, 16), skippable.payload_len);
            try std.testing.expectEqual(@as(usize, 8), skippable.header_len);
        },
    }
}

test "parseLegacyLz4FrameHeader rejects malformed and truncated descriptors explicitly" {
    try std.testing.expectError(
        error.TruncatedFrameMagic,
        parseLegacyLz4FrameHeader("XC"),
    );

    try std.testing.expectError(
        error.InvalidLegacyMagic,
        parseLegacyLz4FrameHeader("ABCD"),
    );

    try std.testing.expectError(
        error.UnsupportedFrameVersion,
        parseLegacyLz4FrameHeader(&[_]u8{
            0x04, 0x22, 0x4D, 0x18,
            0xA0, // version=10
            0x70,
            0x00,
        }),
    );

    try std.testing.expectError(
        error.ReservedFlagBitSet,
        parseLegacyLz4FrameHeader(&[_]u8{
            0x04, 0x22, 0x4D, 0x18,
            0x62, // reserved FLG bit1 set
            0x70,
            0x00,
        }),
    );

    try std.testing.expectError(
        error.ReservedBlockDescriptorBitsSet,
        parseLegacyLz4FrameHeader(&[_]u8{
            0x04, 0x22, 0x4D, 0x18,
            0x60,
            0x71, // reserved bit set in BD
            0x00,
        }),
    );

    try std.testing.expectError(
        error.UnsupportedBlockMaxSizeCode,
        parseLegacyLz4FrameHeader(&[_]u8{
            0x04, 0x22, 0x4D, 0x18,
            0x60,
            0x30, // block max size code=3 (unsupported)
            0x00,
        }),
    );

    try std.testing.expectError(
        error.TruncatedStandardHeaderDescriptor,
        parseLegacyLz4FrameHeader(&[_]u8{
            0x04, 0x22, 0x4D, 0x18,
            0x69, // requires content-size + dict-id extension bytes
            0x70,
            0x00,
        }),
    );

    try std.testing.expectError(
        error.TruncatedSkippableHeader,
        parseLegacyLz4FrameHeader(&[_]u8{
            0x50, 0x2A, 0x4D, 0x18,
            0x01, 0x00, 0x00, // only 3 length bytes
        }),
    );

    try std.testing.expectError(
        error.InvalidHeaderChecksum,
        parseLegacyLz4FrameHeader(&[_]u8{
            0x04, 0x22, 0x4D, 0x18,
            0x60, 0x70,
            0x72, // valid Java header uses 0x73
        }),
    );
}

test "probeFileMagic reports truncated on short files" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const file = try tmp.dir.createFile(io_context.current(), "short.bin", .{ .truncate = true });
    defer file.close(io_context.current());
    try file.writeStreamingAll(io_context.current(), "XC");

    const detected = try probeFileMagic(tmp.dir, "short.bin");
    try std.testing.expectEqual(MagicKind.truncated, detected);
}

test "probeFileMagic distinguishes XCJ1 and XCJ2 journal formats" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_formats = [_]struct {
        path: []const u8,
        magic: []const u8,
        expected: MagicKind,
    }{
        .{ .path = "journal-v1.bin", .magic = "XCJ1", .expected = .zig_journal_v1 },
        .{ .path = "journal-v2.bin", .magic = "XCJ2", .expected = .zig_journal_v2 },
    };

    for (journal_formats) |format| {
        {
            const file = try tmp.dir.createFile(io_context.current(), format.path, .{ .truncate = true });
            defer file.close(io_context.current());
            try file.writeStreamingAll(io_context.current(), format.magic);
        }

        try std.testing.expectEqual(format.expected, try probeFileMagic(tmp.dir, format.path));
    }
}

test "parseLegacyLz4FileHeader parses skippable header from file" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const file = try tmp.dir.createFile(io_context.current(), "archive.lz4", .{ .truncate = true });
    defer file.close(io_context.current());
    try file.writeStreamingAll(io_context.current(), &[_]u8{
        0x5C, 0x2A, 0x4D, 0x18, // skippable tag=12
        0x20, 0x00, 0x00, 0x00, // payload len
    });

    const parsed = try parseLegacyLz4FileHeader(tmp.dir, "archive.lz4");
    switch (parsed) {
        .standard => return error.ExpectedSkippableHeader,
        .skippable => |skippable| {
            try std.testing.expectEqual(@as(u8, 12), skippable.user_magic_tag);
            try std.testing.expectEqual(@as(u32, 32), skippable.payload_len);
        },
    }
}

test "preflightLegacyImport accepts legacy lz4 archives and captures parsed header metadata" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const file = try tmp.dir.createFile(io_context.current(), "snapshot.lz4", .{ .truncate = true });
        defer file.close(io_context.current());
        try file.writeStreamingAll(io_context.current(), &[_]u8{
            0x04, 0x22, 0x4D, 0x18,
            0x60, 0x70, 0x73,
        });
    }
    {
        const file = try tmp.dir.createFile(io_context.current(), "journal.lz4", .{ .truncate = true });
        defer file.close(io_context.current());
        try file.writeStreamingAll(io_context.current(), &[_]u8{
            0x5F, 0x2A, 0x4D, 0x18,
            0x01, 0x00, 0x00, 0x00,
        });
    }

    const request: LegacyImportRequest = .{
        .snapshot_archive_path = "snapshot.lz4",
        .journal_archive_path = "journal.lz4",
        .output_folder = "converted",
    };
    const preflight = try preflightLegacyImport(tmp.dir, request);
    try std.testing.expectEqual(@as(?MagicKind, .legacy_lz4), preflight.snapshot_magic);
    try std.testing.expectEqual(@as(?MagicKind, .legacy_lz4), preflight.journal_magic);

    const snapshot_header = preflight.snapshot_header orelse return error.ExpectedSnapshotHeader;
    try std.testing.expectEqual(@as(usize, 7), snapshot_header.headerLength());

    const journal_header = preflight.journal_header orelse return error.ExpectedJournalHeader;
    switch (journal_header) {
        .standard => return error.ExpectedSkippableHeader,
        .skippable => |header| try std.testing.expectEqual(@as(u8, 15), header.user_magic_tag),
    }
}

test "preflightLegacyImport rejects non legacy inputs and invalid requests" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const file = try tmp.dir.createFile(io_context.current(), "zig.snapshot", .{ .truncate = true });
        defer file.close(io_context.current());
        try file.writeStreamingAll(io_context.current(), "XCS2");
    }

    try std.testing.expectError(
        error.EmptyOutputFolder,
        preflightLegacyImport(tmp.dir, .{
            .snapshot_archive_path = "zig.snapshot",
            .output_folder = "",
        }),
    );

    try std.testing.expectError(
        error.MissingInputArchive,
        preflightLegacyImport(tmp.dir, .{
            .output_folder = "converted",
        }),
    );

    try std.testing.expectError(
        error.SnapshotArchiveNotLegacyLz4,
        preflightLegacyImport(tmp.dir, .{
            .snapshot_archive_path = "zig.snapshot",
            .output_folder = "converted",
        }),
    );
}

test "preflightLegacyImport rejects XCJ1 and XCJ2 as native non legacy journals" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_formats = [_]struct {
        path: []const u8,
        magic: []const u8,
    }{
        .{ .path = "journal-v1.bin", .magic = "XCJ1" },
        .{ .path = "journal-v2.bin", .magic = "XCJ2" },
    };

    for (journal_formats) |format| {
        {
            const file = try tmp.dir.createFile(io_context.current(), format.path, .{ .truncate = true });
            defer file.close(io_context.current());
            try file.writeStreamingAll(io_context.current(), format.magic);
        }

        try std.testing.expectError(
            error.JournalArchiveNotLegacyLz4,
            preflightLegacyImport(tmp.dir, .{
                .journal_archive_path = format.path,
                .output_folder = "converted",
            }),
        );
    }
}

// Fixed output from lz4-java 1.8.0 (cached artifact SHA-1
// 4b986a99445e49ea5fbf5d149c4b63f6ed6c6780) for a one-byte Chronicle body:
// big-endian body length 1 followed by 0x2a. The five-byte block is stored raw.
// The fixture also round-trips through the system lz4 1.10.0 decoder.
const lz4_java_1_8_0_snapshot_fixture = [_]u8{
    0x04, 0x22, 0x4D, 0x18, 0x60, 0x70, 0x73,
    0x05, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00,
    0x01, 0x2A, 0x00, 0x00, 0x00, 0x00,
};

// The same cached lz4-java 1.8.0 writer's compressed path for a big-endian
// length 32 followed by 32 zero bytes. SHA-256:
// 19a7fe2f9643509091efcb073204bb7819275c9d793a4791bc9c80fc201f8fb4.
const lz4_java_1_8_0_compressed_snapshot_fixture = [_]u8{
    0x04, 0x22, 0x4D, 0x18, 0x60, 0x70, 0x73,
    0x0F, 0x00, 0x00, 0x00, 0x5F, 0x00, 0x00,
    0x00, 0x20, 0x00, 0x01, 0x00, 0x07, 0x50,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00,
};

const TestFrameOptions = struct {
    raw_block: bool = true,
    block_checksum: bool = false,
    content_checksum: bool = false,
    declared_content_size: ?u64 = null,
    dictionary_id: ?u32 = null,
    independent: bool = true,
    block_max_size_code: u3 = 7,
};

fn buildTestFrame(
    allocator: std.mem.Allocator,
    stored_block: []const u8,
    decoded_block: []const u8,
    options: TestFrameOptions,
) std.mem.Allocator.Error![]u8 {
    var frame: std.ArrayListUnmanaged(u8) = .empty;
    defer frame.deinit(allocator);

    try appendTestU32(allocator, &frame, Lz4FrameMagic);
    var flg: u8 = 0b0100_0000;
    if (options.independent) flg |= 0b0010_0000;
    if (options.block_checksum) flg |= 0b0001_0000;
    if (options.declared_content_size != null) flg |= 0b0000_1000;
    if (options.content_checksum) flg |= 0b0000_0100;
    if (options.dictionary_id != null) flg |= 0b0000_0001;
    try frame.append(allocator, flg);
    try frame.append(allocator, @as(u8, options.block_max_size_code) << 4);
    if (options.declared_content_size) |content_size| {
        try appendTestU64(allocator, &frame, content_size);
    }
    if (options.dictionary_id) |dictionary_id| {
        try appendTestU32(allocator, &frame, dictionary_id);
    }
    try frame.append(
        allocator,
        @truncate(std.hash.XxHash32.hash(0, frame.items[4..]) >> 8),
    );

    const stored_size: u32 = @intCast(stored_block.len);
    try appendTestU32(
        allocator,
        &frame,
        stored_size | if (options.raw_block) Lz4RawBlockMask else 0,
    );
    try frame.appendSlice(allocator, stored_block);
    if (options.block_checksum) {
        try appendTestU32(allocator, &frame, std.hash.XxHash32.hash(0, stored_block));
    }
    try appendTestU32(allocator, &frame, 0);
    if (options.content_checksum) {
        try appendTestU32(allocator, &frame, std.hash.XxHash32.hash(0, decoded_block));
    }
    return frame.toOwnedSlice(allocator);
}

fn buildTestMultiBlockFrame(
    allocator: std.mem.Allocator,
    raw_block: []const u8,
    compressed_block: []const u8,
    decoded_compressed_block: []const u8,
) std.mem.Allocator.Error![]u8 {
    var frame: std.ArrayListUnmanaged(u8) = .empty;
    defer frame.deinit(allocator);

    try appendTestU32(allocator, &frame, Lz4FrameMagic);
    const flg: u8 = 0b0111_0100; // independent + block checksum + content checksum
    try frame.append(allocator, flg);
    try frame.append(allocator, 0x70);
    try frame.append(
        allocator,
        @truncate(std.hash.XxHash32.hash(0, frame.items[4..]) >> 8),
    );

    try appendTestU32(allocator, &frame, @as(u32, @intCast(raw_block.len)) | Lz4RawBlockMask);
    try frame.appendSlice(allocator, raw_block);
    try appendTestU32(allocator, &frame, std.hash.XxHash32.hash(0, raw_block));

    try appendTestU32(allocator, &frame, @intCast(compressed_block.len));
    try frame.appendSlice(allocator, compressed_block);
    try appendTestU32(allocator, &frame, std.hash.XxHash32.hash(0, compressed_block));

    try appendTestU32(allocator, &frame, 0);
    var content_hash = std.hash.XxHash32.init(0);
    content_hash.update(raw_block);
    content_hash.update(decoded_compressed_block);
    try appendTestU32(allocator, &frame, content_hash.final());
    return frame.toOwnedSlice(allocator);
}

fn appendTestU32(
    allocator: std.mem.Allocator,
    bytes: *std.ArrayListUnmanaged(u8),
    value: u32,
) std.mem.Allocator.Error!void {
    var encoded: [4]u8 = undefined;
    std.mem.writeInt(u32, &encoded, value, .little);
    try bytes.appendSlice(allocator, &encoded);
}

fn appendTestU64(
    allocator: std.mem.Allocator,
    bytes: *std.ArrayListUnmanaged(u8),
    value: u64,
) std.mem.Allocator.Error!void {
    var encoded: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded, value, .little);
    try bytes.appendSlice(allocator, &encoded);
}

test "decodeLegacyLz4SnapshotFrame decodes fixed lz4-java 1.8.0 raw fixture" {
    var decoded = try decodeLegacyLz4SnapshotFrame(
        std.testing.allocator,
        &lz4_java_1_8_0_snapshot_fixture,
        .{},
    );
    defer decoded.deinit(std.testing.allocator);

    try std.testing.expectEqualSlices(u8, &.{ 0x00, 0x00, 0x00, 0x01, 0x2A }, decoded.bytes);
    try std.testing.expectEqual(@as(u32, 1), std.mem.readInt(u32, decoded.bytes[0..4], .big));
}

test "decodeLegacyLz4SnapshotFrame decodes fixed lz4-java 1.8.0 compressed fixture" {
    var decoded = try decodeLegacyLz4SnapshotFrame(
        std.testing.allocator,
        &lz4_java_1_8_0_compressed_snapshot_fixture,
        .{},
    );
    defer decoded.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 36), decoded.bytes.len);
    try std.testing.expectEqual(@as(u32, 32), std.mem.readInt(u32, decoded.bytes[0..4], .big));
    for (decoded.bytes[4..]) |byte| try std.testing.expectEqual(@as(u8, 0), byte);
}

test "decodeLegacyLz4SnapshotFrame decodes compressed block and optional checksums" {
    const compressed = [_]u8{
        0x47, 'a', 'b', 'c', 'd', 0x04, 0x00,
        0x50, 'd', 'a', 'b', 'c', 'd',
    };
    const expected = "abcdabcdabcdabcdabcd";
    const frame = try buildTestFrame(
        std.testing.allocator,
        &compressed,
        expected,
        .{
            .raw_block = false,
            .block_checksum = true,
            .content_checksum = true,
            .declared_content_size = expected.len,
        },
    );
    defer std.testing.allocator.free(frame);

    var decoded = try decodeLegacyLz4SnapshotFrame(std.testing.allocator, frame, .{});
    defer decoded.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings(expected, decoded.bytes);
}

test "decodeLegacyLz4SnapshotFrame resets independent blocks and hashes aggregate content" {
    const compressed = [_]u8{
        0x47, 'a', 'b', 'c', 'd', 0x04, 0x00,
        0x50, 'd', 'a', 'b', 'c', 'd',
    };
    const compressed_output = "abcdabcdabcdabcdabcd";
    const frame = try buildTestMultiBlockFrame(
        std.testing.allocator,
        "raw-",
        &compressed,
        compressed_output,
    );
    defer std.testing.allocator.free(frame);

    var decoded = try decodeLegacyLz4SnapshotFrame(std.testing.allocator, frame, .{});
    defer decoded.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("raw-abcdabcdabcdabcdabcd", decoded.bytes);
}

test "decodeLegacyLz4SnapshotFrame rejects unsupported snapshot profiles" {
    const allocator = std.testing.allocator;

    const skippable = [_]u8{ 0x50, 0x2A, 0x4D, 0x18, 0, 0, 0, 0 };
    try std.testing.expectError(
        error.UnsupportedSkippableFrame,
        decodeLegacyLz4SnapshotFrame(allocator, &skippable, .{}),
    );

    const dependent = try buildTestFrame(allocator, "x", "x", .{ .independent = false });
    defer allocator.free(dependent);
    try std.testing.expectError(
        error.UnsupportedDependentBlocks,
        decodeLegacyLz4SnapshotFrame(allocator, dependent, .{}),
    );

    const dictionary = try buildTestFrame(allocator, "x", "x", .{ .dictionary_id = 42 });
    defer allocator.free(dictionary);
    try std.testing.expectError(
        error.UnsupportedDictionaryId,
        decodeLegacyLz4SnapshotFrame(allocator, dictionary, .{}),
    );

    const smaller_blocks = try buildTestFrame(allocator, "x", "x", .{ .block_max_size_code = 6 });
    defer allocator.free(smaller_blocks);
    try std.testing.expectError(
        error.UnsupportedSnapshotBlockMaxSize,
        decodeLegacyLz4SnapshotFrame(allocator, smaller_blocks, .{}),
    );
}

test "decodeLegacyLz4SnapshotFrame enforces file output and block caps" {
    const allocator = std.testing.allocator;

    try std.testing.expectError(
        error.FileSizeLimitExceeded,
        decodeLegacyLz4SnapshotFrame(allocator, &lz4_java_1_8_0_snapshot_fixture, .{
            .max_file_size = lz4_java_1_8_0_snapshot_fixture.len - 1,
        }),
    );
    try std.testing.expectError(
        error.OutputSizeLimitExceeded,
        decodeLegacyLz4SnapshotFrame(allocator, &lz4_java_1_8_0_snapshot_fixture, .{
            .max_output_size = 4,
        }),
    );
    try std.testing.expectError(
        error.OutputSizeLimitExceeded,
        decodeLegacyLz4SnapshotFrame(allocator, &lz4_java_1_8_0_compressed_snapshot_fixture, .{
            .max_output_size = 35,
        }),
    );
    try std.testing.expectError(
        error.BlockSizeLimitExceeded,
        decodeLegacyLz4SnapshotFrame(allocator, &lz4_java_1_8_0_snapshot_fixture, .{
            .max_block_size = JavaSnapshotBlockBytes - 1,
        }),
    );

    const oversized_block = [_]u8{
        0x04, 0x22, 0x4D, 0x18, 0x60, 0x70, 0x73,
        0x01, 0x00, 0x40, 0x00, // 4 MiB + 1 compressed bytes
    };
    try std.testing.expectError(
        error.BlockSizeLimitExceeded,
        decodeLegacyLz4SnapshotFrame(allocator, &oversized_block, .{}),
    );
}

test "decodeLegacyLz4SnapshotFrame rejects truncation and trailing bytes" {
    const allocator = std.testing.allocator;

    for (0..lz4_java_1_8_0_snapshot_fixture.len) |prefix_len| {
        if (decodeLegacyLz4SnapshotFrame(
            allocator,
            lz4_java_1_8_0_snapshot_fixture[0..prefix_len],
            .{},
        )) |decoded_value| {
            var decoded = decoded_value;
            decoded.deinit(allocator);
            return error.ExpectedTruncatedFrameFailure;
        } else |_| {}
    }

    const trailing = lz4_java_1_8_0_snapshot_fixture ++ [_]u8{0};
    try std.testing.expectError(
        error.TrailingFrameData,
        decodeLegacyLz4SnapshotFrame(allocator, &trailing, .{}),
    );

    const invalid_raw_end = [_]u8{
        0x04, 0x22, 0x4D, 0x18, 0x60, 0x70, 0x73,
        0x00, 0x00, 0x00, 0x80,
    };
    try std.testing.expectError(
        error.InvalidRawEndMarker,
        decodeLegacyLz4SnapshotFrame(allocator, &invalid_raw_end, .{}),
    );
}

test "decodeLegacyLz4SnapshotFrame verifies block and content integrity" {
    const allocator = std.testing.allocator;

    const checked_block = try buildTestFrame(allocator, "integrity", "integrity", .{
        .block_checksum = true,
    });
    defer allocator.free(checked_block);
    checked_block[checked_block.len - 5] ^= 1;
    try std.testing.expectError(
        error.BlockChecksumMismatch,
        decodeLegacyLz4SnapshotFrame(allocator, checked_block, .{}),
    );
    try std.testing.expectError(
        error.TruncatedBlockChecksum,
        decodeLegacyLz4SnapshotFrame(allocator, checked_block[0 .. checked_block.len - 5], .{}),
    );

    const checked_content = try buildTestFrame(allocator, "integrity", "integrity", .{
        .content_checksum = true,
    });
    defer allocator.free(checked_content);
    checked_content[checked_content.len - 1] ^= 1;
    try std.testing.expectError(
        error.ContentChecksumMismatch,
        decodeLegacyLz4SnapshotFrame(allocator, checked_content, .{}),
    );
    try std.testing.expectError(
        error.TruncatedContentChecksum,
        decodeLegacyLz4SnapshotFrame(allocator, checked_content[0 .. checked_content.len - 1], .{}),
    );

    const wrong_size = try buildTestFrame(allocator, "abc", "abc", .{
        .declared_content_size = 4,
    });
    defer allocator.free(wrong_size);
    try std.testing.expectError(
        error.ContentSizeMismatch,
        decodeLegacyLz4SnapshotFrame(allocator, wrong_size, .{}),
    );
}

fn snapshotFrameAllocationTest(allocator: std.mem.Allocator, frame: []const u8) !void {
    var decoded = try decodeLegacyLz4SnapshotFrame(allocator, frame, .{});
    defer decoded.deinit(allocator);
    try std.testing.expectEqualStrings("abcdabcdabcdabcdabcd", decoded.bytes);
}

test "decodeLegacyLz4SnapshotFrame cleans up across every allocation failure" {
    const compressed = [_]u8{
        0x47, 'a', 'b', 'c', 'd', 0x04, 0x00,
        0x50, 'd', 'a', 'b', 'c', 'd',
    };
    const expected = "abcdabcdabcdabcdabcd";
    const frame = try buildTestFrame(std.testing.allocator, &compressed, expected, .{
        .raw_block = false,
        .block_checksum = true,
        .content_checksum = true,
        .declared_content_size = expected.len,
    });
    defer std.testing.allocator.free(frame);

    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        snapshotFrameAllocationTest,
        .{frame},
    );
}
