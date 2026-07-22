const std = @import("std");
const io_context = @import("io_context.zig");

const Lz4FrameMagic: u32 = 0x184D2204;
const Lz4SkippableMagicMin: u32 = 0x184D2A50;
const Lz4SkippableMagicMax: u32 = 0x184D2A5F;
const HeaderProbeBytes: usize = 32;

pub const MagicKind = enum {
    zig_snapshot_v1,
    zig_snapshot_v2,
    zig_journal_v1,
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

pub const ConvertError = PreflightError || error{
    NotImplemented,
};

pub fn detectMagic(magic_prefix: []const u8) MagicKind {
    if (magic_prefix.len < 4) return .truncated;

    const magic = magic_prefix[0..4];
    if (std.mem.eql(u8, magic, "XCS1")) return .zig_snapshot_v1;
    if (std.mem.eql(u8, magic, "XCS2")) return .zig_snapshot_v2;
    if (std.mem.eql(u8, magic, "XCJ1")) return .zig_journal_v1;
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

pub fn convertLegacyPersistenceArchives(
    _: std.mem.Allocator,
    dir: std.Io.Dir,
    request: LegacyImportRequest,
) ConvertError!void {
    _ = try preflightLegacyImport(dir, request);
    return error.NotImplemented;
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

    return .{
        .standard = .{
            .version = @intCast(version_raw),
            .block_independence = (flg & 0b0010_0000) != 0,
            .block_checksum = (flg & 0b0001_0000) != 0,
            .content_size_present = (flg & 0b0000_1000) != 0,
            .content_checksum = (flg & 0b0000_0100) != 0,
            .dictionary_id_present = (flg & 0b0000_0001) != 0,
            .block_max_size_code = @intCast(block_max_size_code_raw),
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
        0x00, // HC
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
            try std.testing.expectEqual(@as(usize, 3), standard.descriptor_len);
            try std.testing.expectEqual(@as(usize, 7), standard.header_len);
        },
        .skippable => return error.ExpectedStandardHeader,
    }
}

test "parseLegacyLz4FrameHeader parses standard frame with content-size and dict-id flags" {
    const header = [_]u8{
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

    const parsed = try parseLegacyLz4FrameHeader(header[0..]);
    switch (parsed) {
        .standard => |standard| {
            try std.testing.expect(standard.content_size_present);
            try std.testing.expect(standard.dictionary_id_present);
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
            0x60, 0x70, 0x00,
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

    try std.testing.expectError(
        error.NotImplemented,
        convertLegacyPersistenceArchives(std.testing.allocator, tmp.dir, request),
    );
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
