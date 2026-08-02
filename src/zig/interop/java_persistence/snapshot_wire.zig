const std = @import("std");

pub const raw_document_length_mask: u32 = 0x3fff_ffff;
pub const raw_document_meta_data_bit: u32 = 0x4000_0000;
pub const raw_document_not_complete_bit: u32 = 0x8000_0000;

pub const Config = struct {
    max_body_bytes: usize = 512 * 1024 * 1024,
};

pub const UnwrapError = error{
    TruncatedOuterLength,
    NegativeOuterLength,
    OuterLengthTooSmall,
    OuterLengthTooLarge,
    TruncatedDocument,
    TrailingBytes,
    DocumentNotComplete,
    MetadataDocument,
    EmptyDocument,
    BodyLengthMismatch,
};

pub const DocumentView = struct {
    /// Borrowed directly from the decompressed LZ4-frame bytes.
    body: []const u8,
    outer_length: u32,
    raw_header: u32,
};

/// Unwraps the exact stream emitted by `WireToOutputStream2.flush()`:
///
/// `u32be outer_length | u32le Chronicle RAW header | object body`
///
/// The RAW header is deliberately parsed as little-endian. Chronicle uses
/// native-order Unsafe writes; the supported historical Java producers are
/// x86_64/arm64 little-endian, and silently using the Zig host endianness would
/// make an archive's meaning host-dependent.
pub fn unwrapRawDocument(
    decompressed: []const u8,
    config: Config,
) UnwrapError!DocumentView {
    if (decompressed.len < @sizeOf(i32)) return error.TruncatedOuterLength;
    const outer_signed = std.mem.readInt(i32, decompressed[0..4], .big);
    if (outer_signed < 0) return error.NegativeOuterLength;
    if (outer_signed < @sizeOf(u32)) return error.OuterLengthTooSmall;
    const outer_length: u32 = @intCast(outer_signed);
    const body_limit_with_header = std.math.add(usize, config.max_body_bytes, @sizeOf(u32)) catch
        return error.OuterLengthTooLarge;
    if (@as(usize, outer_length) > body_limit_with_header) return error.OuterLengthTooLarge;

    const total_length = std.math.add(usize, @sizeOf(u32), @as(usize, outer_length)) catch
        return error.OuterLengthTooLarge;
    if (decompressed.len < total_length) return error.TruncatedDocument;
    if (decompressed.len > total_length) return error.TrailingBytes;

    const raw_header = std.mem.readInt(u32, decompressed[4..8], .little);
    if ((raw_header & raw_document_not_complete_bit) != 0) return error.DocumentNotComplete;
    if ((raw_header & raw_document_meta_data_bit) != 0) return error.MetadataDocument;
    const body_length: usize = raw_header & raw_document_length_mask;
    if (body_length == 0) return error.EmptyDocument;
    if (body_length > config.max_body_bytes) return error.OuterLengthTooLarge;
    if (body_length != @as(usize, outer_length) - @sizeOf(u32)) {
        return error.BodyLengthMismatch;
    }

    return .{
        .body = decompressed[8..],
        .outer_length = outer_length,
        .raw_header = raw_header,
    };
}

const golden = [_]u8{
    0x00, 0x00, 0x00, 0x08, // BE outer length: header + four body bytes
    0x04, 0x00, 0x00, 0x00, // LE Chronicle complete data header
    0x11, 0x22, 0x33, 0x44,
};

test "Chronicle RAW snapshot wrapper has exact mixed-endian layout" {
    const document = try unwrapRawDocument(&golden, .{});
    try std.testing.expectEqual(@as(u32, 8), document.outer_length);
    try std.testing.expectEqual(@as(u32, 4), document.raw_header);
    try std.testing.expectEqualSlices(u8, &.{ 0x11, 0x22, 0x33, 0x44 }, document.body);
}

test "Chronicle RAW snapshot wrapper rejects every truncation and trailing byte" {
    for (0..golden.len) |end| {
        try std.testing.expectError(
            if (end < 4) error.TruncatedOuterLength else error.TruncatedDocument,
            unwrapRawDocument(golden[0..end], .{}),
        );
    }
    const trailing = golden ++ [_]u8{0};
    try std.testing.expectError(error.TrailingBytes, unwrapRawDocument(&trailing, .{}));
}

test "Chronicle RAW snapshot wrapper rejects lengths flags and unsupported empty documents" {
    var bytes = golden;

    std.mem.writeInt(i32, bytes[0..4], -1, .big);
    try std.testing.expectError(error.NegativeOuterLength, unwrapRawDocument(&bytes, .{}));

    bytes = golden;
    std.mem.writeInt(i32, bytes[0..4], 3, .big);
    try std.testing.expectError(error.OuterLengthTooSmall, unwrapRawDocument(&bytes, .{}));

    bytes = golden;
    std.mem.writeInt(u32, bytes[4..8], raw_document_not_complete_bit | 4, .little);
    try std.testing.expectError(error.DocumentNotComplete, unwrapRawDocument(&bytes, .{}));

    bytes = golden;
    std.mem.writeInt(u32, bytes[4..8], raw_document_meta_data_bit | 4, .little);
    try std.testing.expectError(error.MetadataDocument, unwrapRawDocument(&bytes, .{}));

    bytes = golden;
    std.mem.writeInt(u32, bytes[4..8], 0, .little);
    try std.testing.expectError(error.EmptyDocument, unwrapRawDocument(&bytes, .{}));

    bytes = golden;
    std.mem.writeInt(u32, bytes[4..8], 3, .little);
    try std.testing.expectError(error.BodyLengthMismatch, unwrapRawDocument(&bytes, .{}));

    try std.testing.expectError(
        error.OuterLengthTooLarge,
        unwrapRawDocument(&golden, .{ .max_body_bytes = 3 }),
    );
}
