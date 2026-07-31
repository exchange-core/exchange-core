//! Owned raw report-query decoding at the completed binary-transfer boundary.
//!
//! Built-in reports can still use `DecodedReportQuery`, while custom report
//! dispatch retains every uninterpreted byte after the type code for the full
//! lifetime of all per-engine constructor calls.

const std = @import("std");
const binary_payload = @import("binary_payload.zig");
const java_binary_wire = @import("java_binary_wire.zig");

pub const BodyFormat = enum {
    java_little,
    legacy_big,
};

pub const DecodeError = java_binary_wire.RequestDecodeError ||
    java_binary_wire.CarrierUnpackError ||
    binary_payload.DecodeRawReportQueryError;

pub const OwnedReportEnvelope = struct {
    allocator: std.mem.Allocator,
    backing: []u8,
    format: BodyFormat,
    raw: binary_payload.RawReportQuery,

    pub fn deinit(self: *OwnedReportEnvelope) void {
        self.allocator.free(self.backing);
        self.* = undefined;
    }

    /// Built-in constructors keep their Java behavior: an unknown code or a
    /// constructor that cannot read its required fields becomes `.unknown`.
    /// Reserved built-in codes are never reinterpreted through a custom
    /// registry, even when their body is malformed.
    pub fn decodeBuiltInOrIgnored(
        self: *const OwnedReportEnvelope,
    ) binary_payload.DecodeQueryError!binary_payload.DecodedReportQuery {
        return switch (self.format) {
            .java_little => binary_payload.decodeJavaQueryOrIgnoredFromBytes(self.backing),
            .legacy_big => binary_payload.decodeQueryFromBytes(self.backing) catch |err| switch (err) {
                error.TruncatedPayload, error.UnsupportedReportQueryType => .{ .unknown = self.raw.code },
                error.OutOfMemory => error.OutOfMemory,
            },
        };
    }

    pub fn isReservedBuiltInCode(self: *const OwnedReportEnvelope) bool {
        _ = binary_payload.ReportQueryTypeCode.fromCode(self.raw.code) catch return false;
        return true;
    }
};

/// Decodes Java's size-prefixed raw-LZ4 carrier first. If that envelope is not
/// valid, the historical Zig big-endian raw body remains available as a
/// compatibility fallback; its body necessarily includes carrier zero padding
/// because that format has no explicit byte length.
pub fn decodeWordsJavaFirst(
    allocator: std.mem.Allocator,
    words: []const i64,
    config: java_binary_wire.Config,
) DecodeError!OwnedReportEnvelope {
    var java_request = java_binary_wire.decodeRequestWords(allocator, words, config) catch |java_err| switch (java_err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return decodeLegacyWords(allocator, words),
    };
    errdefer java_request.deinit(allocator);
    const raw = try binary_payload.decodeJavaRawReportQueryFromBytes(java_request.body);
    const backing = java_request.body;
    return .{
        .allocator = allocator,
        .backing = backing,
        .format = .java_little,
        .raw = .{
            .code = raw.code,
            .body = backing[@sizeOf(i32)..],
        },
    };
}

fn decodeLegacyWords(
    allocator: std.mem.Allocator,
    words: []const i64,
) DecodeError!OwnedReportEnvelope {
    const backing = try java_binary_wire.unpackSignedWordsBigEndian(allocator, words);
    errdefer allocator.free(backing);
    const raw = try binary_payload.decodeRawReportQueryFromBytes(backing);
    return .{
        .allocator = allocator,
        .backing = backing,
        .format = .legacy_big,
        .raw = .{
            .code = raw.code,
            .body = backing[@sizeOf(i32)..],
        },
    };
}

test "owned Java envelope preserves arbitrary custom body exactly" {
    const allocator = std.testing.allocator;
    const body = [_]u8{ 0, 1, 0xff, 7, 0, 9, 3 };
    const encoded = try binary_payload.encodeJavaRawReportQueryToBytes(allocator, 70_201, &body);
    defer allocator.free(encoded);
    const words = try java_binary_wire.encodeRequestWords(allocator, encoded, .{});
    defer allocator.free(words);

    var envelope = try decodeWordsJavaFirst(allocator, words, .{});
    defer envelope.deinit();
    try std.testing.expectEqual(BodyFormat.java_little, envelope.format);
    try std.testing.expectEqual(@as(i32, 70_201), envelope.raw.code);
    try std.testing.expectEqualSlices(u8, &body, envelope.raw.body);
    try std.testing.expect(!envelope.isReservedBuiltInCode());
    try std.testing.expectEqual(
        binary_payload.DecodedReportQuery{ .unknown = 70_201 },
        try envelope.decodeBuiltInOrIgnored(),
    );
}

test "owned envelope keeps malformed reserved built-in code out of custom fallback" {
    const allocator = std.testing.allocator;
    const encoded = try binary_payload.encodeJavaRawReportQueryToBytes(
        allocator,
        @intFromEnum(binary_payload.ReportQueryTypeCode.single_user),
        &.{ 1, 2, 3 },
    );
    defer allocator.free(encoded);
    const words = try java_binary_wire.encodeRequestWords(allocator, encoded, .{});
    defer allocator.free(words);

    var envelope = try decodeWordsJavaFirst(allocator, words, .{});
    defer envelope.deinit();
    try std.testing.expect(envelope.isReservedBuiltInCode());
    try std.testing.expectEqual(
        binary_payload.DecodedReportQuery{
            .unknown = @intFromEnum(binary_payload.ReportQueryTypeCode.single_user),
        },
        try envelope.decodeBuiltInOrIgnored(),
    );
}

test "owned envelope decodes canonical legacy raw query with explicit padding" {
    const allocator = std.testing.allocator;
    const encoded = try binary_payload.encodeRawReportQueryToBytes(
        allocator,
        70_202,
        &.{ 0xaa, 0xbb, 0xcc },
    );
    defer allocator.free(encoded);
    const words = try java_binary_wire.packSignedWordsBigEndian(allocator, encoded);
    defer allocator.free(words);

    var envelope = try decodeWordsJavaFirst(allocator, words, .{});
    defer envelope.deinit();
    try std.testing.expectEqual(BodyFormat.legacy_big, envelope.format);
    try std.testing.expectEqual(@as(i32, 70_202), envelope.raw.code);
    try std.testing.expectEqualSlices(u8, &.{ 0xaa, 0xbb, 0xcc }, envelope.raw.body[0..3]);
    for (envelope.raw.body[3..]) |padding| try std.testing.expectEqual(@as(u8, 0), padding);
}

fn allocationCampaign(allocator: std.mem.Allocator, words: []const i64) !void {
    var envelope = try decodeWordsJavaFirst(allocator, words, .{});
    defer envelope.deinit();
    try std.testing.expectEqual(@as(i32, 70_203), envelope.raw.code);
}

test "owned raw envelope cleans up across every allocation failure" {
    const allocator = std.testing.allocator;
    const encoded = try binary_payload.encodeJavaRawReportQueryToBytes(
        allocator,
        70_203,
        &.{ 1, 2, 3, 4, 5, 6, 7, 8 },
    );
    defer allocator.free(encoded);
    const words = try java_binary_wire.encodeRequestWords(allocator, encoded, .{});
    defer allocator.free(words);

    try std.testing.checkAllAllocationFailures(
        allocator,
        allocationCampaign,
        .{words},
    );
}
