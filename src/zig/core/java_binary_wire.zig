const std = @import("std");
const binary_payload = @import("binary_payload.zig");
const common = @import("common.zig");

/// Java's ExchangeApi places five signed long carrier words in every ring-buffer
/// command. The longs are only signed containers; their bits are serialized in
/// Java ByteBuffer's default big-endian order.
pub const words_per_frame: usize = 5;
pub const max_java_original_size: usize = std.math.maxInt(i32);

pub const Config = struct {
    /// Bounds allocation before trusting the big-endian original-size prefix.
    max_original_size: usize = 64 * 1024 * 1024,
};

pub const RawLz4DecodeError = std.mem.Allocator.Error || error{
    TruncatedToken,
    TruncatedLiteralLength,
    TruncatedLiterals,
    TruncatedMatchOffset,
    TruncatedMatchLength,
    LengthOverflow,
    ZeroMatchOffset,
    MatchOffsetBeyondOutput,
    OutputOverrun,
    OutputUnderrun,
    MissingFinalLiteralSequence,
    FinalSequenceHasMatch,
    FinalLiteralRunTooShort,
    LastMatchTooCloseToEnd,
};

pub const BoundedRawLz4DecodeError = std.mem.Allocator.Error || error{
    TruncatedToken,
    TruncatedLiteralLength,
    TruncatedLiterals,
    TruncatedMatchOffset,
    TruncatedMatchLength,
    LengthOverflow,
    ZeroMatchOffset,
    MatchOffsetBeyondOutput,
    OutputLimitExceeded,
    MissingFinalLiteralSequence,
    FinalLiteralRunTooShort,
    LastMatchTooCloseToEnd,
};

pub const RawLz4EncodeError = std.mem.Allocator.Error || error{
    InputTooLarge,
};

pub const RequestEncodeError = RawLz4EncodeError || error{
    OriginalSizeTooLarge,
    CarrierTooLarge,
};

pub const RequestDecodeError = RawLz4DecodeError || error{
    TruncatedEnvelope,
    OriginalSizeTooLarge,
    NonZeroCarrierPadding,
    CarrierWordCountNotMultipleOfFive,
    CarrierTooLarge,
};

pub const CarrierPackError = std.mem.Allocator.Error || error{
    CarrierTooLarge,
};

pub const CarrierUnpackError = CarrierPackError || error{
    CarrierWordCountNotMultipleOfFive,
};

pub const DecodedRawBlock = struct {
    bytes: []u8,
    compressed_bytes_consumed: usize,

    pub fn deinit(self: *DecodedRawBlock, allocator: std.mem.Allocator) void {
        allocator.free(self.bytes);
        self.* = undefined;
    }
};

pub const DecodedRequest = struct {
    body: []u8,
    /// Includes the four-byte original-size prefix and excludes carrier padding.
    envelope_bytes_consumed: usize,

    pub fn deinit(self: *DecodedRequest, allocator: std.mem.Allocator) void {
        allocator.free(self.body);
        self.* = undefined;
    }
};

/// Decodes one raw LZ4 block using the known output size, matching
/// LZ4FastDecompressor. It stops only when exactly `original_size` bytes have
/// been produced and reports how much input formed the block.
pub fn decodeRawLz4(
    allocator: std.mem.Allocator,
    compressed: []const u8,
    original_size: usize,
) RawLz4DecodeError!DecodedRawBlock {
    const output = try allocator.alloc(u8, original_size);
    errdefer allocator.free(output);

    var input_pos: usize = 0;
    var output_pos: usize = 0;
    var last_match_start: ?usize = null;

    while (true) {
        if (input_pos == compressed.len) return error.TruncatedToken;
        const token = compressed[input_pos];
        input_pos += 1;

        var literal_length: usize = token >> 4;
        if (literal_length == 15) {
            literal_length = try readExtendedLength(
                compressed,
                &input_pos,
                literal_length,
                error.TruncatedLiteralLength,
            );
        }

        if (literal_length > compressed.len - input_pos) return error.TruncatedLiterals;
        if (literal_length > output.len - output_pos) return error.OutputOverrun;
        @memcpy(
            output[output_pos .. output_pos + literal_length],
            compressed[input_pos .. input_pos + literal_length],
        );
        input_pos += literal_length;
        output_pos += literal_length;

        // A raw block's final sequence contains literals only. Knowing the exact
        // output length lets us identify its end even when zero carrier padding
        // follows it.
        if (output_pos == output.len) {
            if ((token & 0x0F) != 0) return error.FinalSequenceHasMatch;
            if (last_match_start) |match_start| {
                if (literal_length < 5) return error.FinalLiteralRunTooShort;
                if (output_pos - match_start < 12) return error.LastMatchTooCloseToEnd;
            }
            return .{
                .bytes = output,
                .compressed_bytes_consumed = input_pos,
            };
        }

        if (input_pos == compressed.len) return error.OutputUnderrun;
        if (compressed.len - input_pos < @sizeOf(u16)) return error.TruncatedMatchOffset;
        const match_offset = std.mem.readInt(u16, compressed[input_pos..][0..2], .little);
        input_pos += 2;
        if (match_offset == 0) return error.ZeroMatchOffset;
        if (@as(usize, match_offset) > output_pos) return error.MatchOffsetBeyondOutput;

        var match_length: usize = @as(usize, token & 0x0F) + 4;
        if ((token & 0x0F) == 15) {
            match_length = try readExtendedLength(
                compressed,
                &input_pos,
                match_length,
                error.TruncatedMatchLength,
            );
        }
        if (match_length > output.len - output_pos) return error.OutputOverrun;

        // Byte-at-a-time copy is intentional: LZ4 offsets may overlap the bytes
        // being produced by this same match.
        last_match_start = output_pos;
        const offset: usize = match_offset;
        for (0..match_length) |_| {
            output[output_pos] = output[output_pos - offset];
            output_pos += 1;
        }

        if (output_pos == output.len) return error.MissingFinalLiteralSequence;
    }
}

/// Decodes one complete raw LZ4 block when its compressed boundary is known but
/// its original size is not. Output growth is checked against
/// `max_output_size` before every allocation, and the returned slice length is
/// the actual decoded size.
///
/// Unlike `decodeRawLz4`, this function consumes the entire compressed slice and
/// requires the block's final literal-only sequence. Keep using the exact-size
/// API for Java's `LZ4FastDecompressor` envelope, whose boundary is inferred
/// from the expected output size instead.
pub fn decodeRawLz4Bounded(
    allocator: std.mem.Allocator,
    compressed: []const u8,
    max_output_size: usize,
) BoundedRawLz4DecodeError!DecodedRawBlock {
    var output: std.ArrayListUnmanaged(u8) = .empty;
    defer output.deinit(allocator);

    var input_pos: usize = 0;
    var last_match_start: ?usize = null;
    while (true) {
        if (input_pos == compressed.len) return error.TruncatedToken;
        const token = compressed[input_pos];
        input_pos += 1;

        var literal_length: usize = token >> 4;
        if (literal_length == 15) {
            literal_length = try readExtendedBoundedLength(
                compressed,
                &input_pos,
                literal_length,
                error.TruncatedLiteralLength,
            );
        }

        if (literal_length > compressed.len - input_pos) return error.TruncatedLiterals;
        try appendBounded(
            allocator,
            &output,
            compressed[input_pos .. input_pos + literal_length],
            max_output_size,
        );
        input_pos += literal_length;

        // The compressed slice is one complete block, so exhausting it after
        // literals identifies the mandatory final sequence without knowing the
        // original size in advance.
        if (input_pos == compressed.len) {
            if (last_match_start) |match_start| {
                if (literal_length < 5) return error.FinalLiteralRunTooShort;
                if (output.items.len - match_start < 12) return error.LastMatchTooCloseToEnd;
            }
            return .{
                .bytes = try output.toOwnedSlice(allocator),
                .compressed_bytes_consumed = input_pos,
            };
        }

        if (compressed.len - input_pos < @sizeOf(u16)) return error.TruncatedMatchOffset;
        const match_offset = std.mem.readInt(u16, compressed[input_pos..][0..2], .little);
        input_pos += 2;
        if (match_offset == 0) return error.ZeroMatchOffset;
        if (@as(usize, match_offset) > output.items.len) return error.MatchOffsetBeyondOutput;

        var match_length: usize = @as(usize, token & 0x0F) + 4;
        if ((token & 0x0F) == 15) {
            match_length = try readExtendedBoundedLength(
                compressed,
                &input_pos,
                match_length,
                error.TruncatedMatchLength,
            );
        }
        if (match_length > max_output_size -| output.items.len) {
            return error.OutputLimitExceeded;
        }
        try output.ensureUnusedCapacity(allocator, match_length);

        // Byte-at-a-time copy is required because a match may overlap bytes
        // emitted by the same match.
        last_match_start = output.items.len;
        const offset: usize = match_offset;
        for (0..match_length) |_| {
            output.appendAssumeCapacity(output.items[output.items.len - offset]);
        }
        if (input_pos == compressed.len) return error.MissingFinalLiteralSequence;
    }
}

/// Produces a standards-compatible raw LZ4 block consisting of one final
/// literal-only sequence. It favors interoperability and simplicity over ratio.
pub fn encodeRawLz4LiteralOnly(
    allocator: std.mem.Allocator,
    input: []const u8,
) RawLz4EncodeError![]u8 {
    const extension_size = if (input.len < 15)
        @as(usize, 0)
    else
        (input.len - 15) / 255 + 1;
    const header_size = std.math.add(usize, 1, extension_size) catch return error.InputTooLarge;
    const encoded_size = std.math.add(usize, header_size, input.len) catch return error.InputTooLarge;
    const encoded = try allocator.alloc(u8, encoded_size);
    errdefer allocator.free(encoded);

    encoded[0] = @as(u8, @intCast(@min(input.len, 15))) << 4;
    var cursor: usize = 1;
    if (input.len >= 15) {
        var remaining = input.len - 15;
        while (remaining >= 255) {
            encoded[cursor] = 255;
            cursor += 1;
            remaining -= 255;
        }
        encoded[cursor] = @intCast(remaining);
        cursor += 1;
    }
    @memcpy(encoded[cursor..], input);
    return encoded;
}

/// Encodes Java's request envelope: a big-endian signed-positive i32
/// original-size prefix followed by a raw LZ4 block.
pub fn encodeRequestEnvelope(
    allocator: std.mem.Allocator,
    body: []const u8,
    config: Config,
) RequestEncodeError![]u8 {
    try validateOriginalSize(body.len, config);
    const original_size = std.math.cast(u32, body.len) orelse return error.OriginalSizeTooLarge;

    const compressed = try encodeRawLz4LiteralOnly(allocator, body);
    defer allocator.free(compressed);
    const envelope_size = std.math.add(usize, @sizeOf(u32), compressed.len) catch return error.CarrierTooLarge;
    const envelope = try allocator.alloc(u8, envelope_size);
    std.mem.writeInt(u32, envelope[0..4], original_size, .big);
    @memcpy(envelope[4..], compressed);
    return envelope;
}

/// Decodes an envelope and permits only zero carrier padding after the exact
/// raw-LZ4 boundary inferred from the original output size.
pub fn decodeRequestEnvelope(
    allocator: std.mem.Allocator,
    envelope_and_padding: []const u8,
    config: Config,
) RequestDecodeError!DecodedRequest {
    if (envelope_and_padding.len < @sizeOf(u32)) return error.TruncatedEnvelope;
    const original_size_u32 = std.mem.readInt(u32, envelope_and_padding[0..4], .big);
    const original_size: usize = original_size_u32;
    try validateOriginalSize(original_size, config);

    var decoded = try decodeRawLz4(allocator, envelope_and_padding[4..], original_size);
    errdefer decoded.deinit(allocator);
    const consumed = 4 + decoded.compressed_bytes_consumed;
    for (envelope_and_padding[consumed..]) |padding_byte| {
        if (padding_byte != 0) return error.NonZeroCarrierPadding;
    }

    const body = decoded.bytes;
    decoded.bytes = &.{};
    return .{
        .body = body,
        .envelope_bytes_consumed = consumed,
    };
}

fn validateOriginalSize(
    original_size: usize,
    config: Config,
) error{OriginalSizeTooLarge}!void {
    if (original_size > config.max_original_size or
        original_size > max_java_original_size)
    {
        return error.OriginalSizeTooLarge;
    }
}

/// Packs bytes into signed Java long carriers using big-endian bit order and
/// zero-pads the word count to a whole five-word message.
pub fn packSignedWordsBigEndian(
    allocator: std.mem.Allocator,
    bytes: []const u8,
) CarrierPackError![]i64 {
    const rounded_bytes = std.math.add(usize, bytes.len, 7) catch return error.CarrierTooLarge;
    const words_without_frame_padding = rounded_bytes / 8;
    const remainder = words_without_frame_padding % words_per_frame;
    const frame_padding = if (remainder == 0) @as(usize, 0) else words_per_frame - remainder;
    const word_count = std.math.add(
        usize,
        words_without_frame_padding,
        frame_padding,
    ) catch return error.CarrierTooLarge;

    const words = try allocator.alloc(i64, word_count);
    @memset(words, 0);

    const complete_words = bytes.len / @sizeOf(i64);
    var word_index: usize = 0;
    if (std.simd.suggestVectorLength(u64)) |lanes| {
        const WordVector = @Vector(lanes, u64);
        const vector_bytes = lanes * @sizeOf(u64);
        while (word_index + lanes <= complete_words) : (word_index += lanes) {
            const byte_index = word_index * @sizeOf(u64);
            const native_words: WordVector = @bitCast(
                bytes[byte_index..][0..vector_bytes].*,
            );
            const big_endian_words = std.mem.nativeToBig(WordVector, native_words);
            const signed_words: @Vector(lanes, i64) = @bitCast(big_endian_words);
            words[word_index..][0..lanes].* = signed_words;
        }
    }
    while (word_index < complete_words) : (word_index += 1) {
        const start = word_index * @sizeOf(i64);
        words[word_index] = std.mem.readInt(i64, bytes[start..][0..8], .big);
    }
    if (word_index < words_without_frame_padding) {
        var raw = [_]u8{0} ** 8;
        const start = word_index * 8;
        @memcpy(raw[0 .. bytes.len - start], bytes[start..]);
        words[word_index] = std.mem.readInt(i64, &raw, .big);
    }
    return words;
}

/// Restores the carrier bytes, including zero word/frame padding. Envelope
/// decoding is responsible for proving that all bytes past the LZ4 block are 0.
pub fn unpackSignedWordsBigEndian(
    allocator: std.mem.Allocator,
    words: []const i64,
) CarrierUnpackError![]u8 {
    if (words.len % words_per_frame != 0) return error.CarrierWordCountNotMultipleOfFive;
    const byte_count = std.math.mul(usize, words.len, @sizeOf(i64)) catch return error.CarrierTooLarge;
    const bytes = try allocator.alloc(u8, byte_count);

    var word_index: usize = 0;
    if (std.simd.suggestVectorLength(u64)) |lanes| {
        const WordVector = @Vector(lanes, u64);
        const ByteVector = @Vector(lanes * @sizeOf(u64), u8);
        while (word_index + lanes <= words.len) : (word_index += lanes) {
            const native_words: WordVector = @bitCast(words[word_index..][0..lanes].*);
            const big_endian_words = std.mem.nativeToBig(WordVector, native_words);
            const big_endian_bytes: ByteVector = @bitCast(big_endian_words);
            const byte_index = word_index * @sizeOf(i64);
            bytes[byte_index..][0 .. lanes * @sizeOf(u64)].* = big_endian_bytes;
        }
    }
    while (word_index < words.len) : (word_index += 1) {
        const word = words[word_index];
        std.mem.writeInt(i64, bytes[word_index * 8 ..][0..8], word, .big);
    }
    return bytes;
}

pub fn encodeRequestWords(
    allocator: std.mem.Allocator,
    body: []const u8,
    config: Config,
) RequestEncodeError![]i64 {
    const envelope = try encodeRequestEnvelope(allocator, body, config);
    defer allocator.free(envelope);
    return packSignedWordsBigEndian(allocator, envelope);
}

pub fn decodeRequestWords(
    allocator: std.mem.Allocator,
    words: []const i64,
    config: Config,
) RequestDecodeError!DecodedRequest {
    const bytes = try unpackSignedWordsBigEndian(allocator, words);
    defer allocator.free(bytes);
    return decodeRequestEnvelope(allocator, bytes, config);
}

pub fn frameCount(words: []const i64) error{CarrierWordCountNotMultipleOfFive}!usize {
    if (words.len % words_per_frame != 0) return error.CarrierWordCountNotMultipleOfFive;
    return words.len / words_per_frame;
}

fn readExtendedLength(
    input: []const u8,
    cursor: *usize,
    base: usize,
    comptime truncated_error: RawLz4DecodeError,
) RawLz4DecodeError!usize {
    var length = base;
    while (true) {
        if (cursor.* == input.len) return truncated_error;
        const extension = input[cursor.*];
        cursor.* += 1;
        length = std.math.add(usize, length, extension) catch return error.LengthOverflow;
        if (extension != 255) return length;
    }
}

fn readExtendedBoundedLength(
    input: []const u8,
    cursor: *usize,
    base: usize,
    comptime truncated_error: BoundedRawLz4DecodeError,
) BoundedRawLz4DecodeError!usize {
    var length = base;
    while (true) {
        if (cursor.* == input.len) return truncated_error;
        const extension = input[cursor.*];
        cursor.* += 1;
        length = std.math.add(usize, length, extension) catch return error.LengthOverflow;
        if (extension != 255) return length;
    }
}

fn appendBounded(
    allocator: std.mem.Allocator,
    output: *std.ArrayListUnmanaged(u8),
    bytes: []const u8,
    max_output_size: usize,
) BoundedRawLz4DecodeError!void {
    if (bytes.len > max_output_size -| output.items.len) return error.OutputLimitExceeded;
    try output.appendSlice(allocator, bytes);
}

test "literal-only raw LZ4 covers short and extended literal lengths" {
    const allocator = std.testing.allocator;
    const lengths = [_]usize{ 0, 1, 14, 15, 16, 269, 270, 271, 600 };
    for (lengths) |length| {
        const input = try allocator.alloc(u8, length);
        defer allocator.free(input);
        for (input, 0..) |*byte, index| byte.* = @truncate(index *% 131);

        const encoded = try encodeRawLz4LiteralOnly(allocator, input);
        defer allocator.free(encoded);
        if (length == 15) {
            try std.testing.expectEqualSlices(u8, &.{ 0xF0, 0x00 }, encoded[0..2]);
        } else if (length == 270) {
            try std.testing.expectEqualSlices(u8, &.{ 0xF0, 0xFF, 0x00 }, encoded[0..3]);
        }

        var decoded = try decodeRawLz4(allocator, encoded, input.len);
        defer decoded.deinit(allocator);
        try std.testing.expectEqual(encoded.len, decoded.compressed_bytes_consumed);
        try std.testing.expectEqualSlices(u8, input, decoded.bytes);
    }
}

test "raw LZ4 decoder supports matches and overlapping copies" {
    const allocator = std.testing.allocator;

    const repeated_block = [_]u8{
        0x47, 'a', 'b', 'c', 'd', 0x04, 0x00,
        0x50, 'd', 'a', 'b', 'c', 'd',
    };
    var repeated = try decodeRawLz4(allocator, &repeated_block, 20);
    defer repeated.deinit(allocator);
    try std.testing.expectEqualStrings("abcdabcdabcdabcdabcd", repeated.bytes);

    const overlapping_block = [_]u8{
        0x18, 'x', 0x01, 0x00,
        0x50, 'x', 'x',  'x',
        'x',  'x',
    };
    var overlapping = try decodeRawLz4(allocator, &overlapping_block, 18);
    defer overlapping.deinit(allocator);
    try std.testing.expectEqualStrings("xxxxxxxxxxxxxxxxxx", overlapping.bytes);

    const extended_match_block = [_]u8{
        0x1F, 'z', 0x01, 0x00, 0x00,
        0x50, 'z', 'z',  'z',  'z',
        'z',
    };
    var extended_match = try decodeRawLz4(allocator, &extended_match_block, 25);
    defer extended_match.deinit(allocator);
    try std.testing.expectEqualStrings("zzzzzzzzzzzzzzzzzzzzzzzzz", extended_match.bytes);
}

test "bounded raw LZ4 decoder returns actual size and requires exact input" {
    const allocator = std.testing.allocator;

    const compressed = [_]u8{
        0x47, 'a', 'b', 'c', 'd', 0x04, 0x00,
        0x50, 'd', 'a', 'b', 'c', 'd',
    };
    var decoded = try decodeRawLz4Bounded(allocator, &compressed, 20);
    defer decoded.deinit(allocator);
    try std.testing.expectEqualStrings("abcdabcdabcdabcdabcd", decoded.bytes);
    try std.testing.expectEqual(compressed.len, decoded.compressed_bytes_consumed);

    var empty = try decodeRawLz4Bounded(allocator, &.{0x00}, 0);
    defer empty.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), empty.bytes.len);
}

test "bounded raw LZ4 decoder enforces cap and malformed boundaries" {
    const allocator = std.testing.allocator;

    try std.testing.expectError(error.TruncatedToken, decodeRawLz4Bounded(allocator, &.{}, 16));
    try std.testing.expectError(error.TruncatedLiteralLength, decodeRawLz4Bounded(allocator, &.{0xF0}, 16));
    try std.testing.expectError(error.TruncatedLiterals, decodeRawLz4Bounded(allocator, &.{ 0x20, 'a' }, 16));
    try std.testing.expectError(error.OutputLimitExceeded, decodeRawLz4Bounded(allocator, &.{ 0x20, 'a', 'b' }, 1));
    try std.testing.expectError(error.TruncatedMatchOffset, decodeRawLz4Bounded(allocator, &.{ 0x10, 'a', 0x01 }, 16));
    try std.testing.expectError(error.ZeroMatchOffset, decodeRawLz4Bounded(allocator, &.{ 0x10, 'a', 0x00, 0x00, 0x00 }, 16));
    try std.testing.expectError(error.MatchOffsetBeyondOutput, decodeRawLz4Bounded(allocator, &.{ 0x10, 'a', 0x02, 0x00, 0x00 }, 16));
    try std.testing.expectError(error.TruncatedMatchLength, decodeRawLz4Bounded(allocator, &.{ 0x1F, 'a', 0x01, 0x00 }, 32));
    try std.testing.expectError(error.MissingFinalLiteralSequence, decodeRawLz4Bounded(allocator, &.{ 0x10, 'a', 0x01, 0x00 }, 16));
    try std.testing.expectError(error.FinalLiteralRunTooShort, decodeRawLz4Bounded(allocator, &.{ 0x10, 'a', 0x01, 0x00, 0x00 }, 16));
    try std.testing.expectError(error.LastMatchTooCloseToEnd, decodeRawLz4Bounded(allocator, &.{ 0x10, 'a', 0x01, 0x00, 0x50, 'a', 'a', 'a', 'a', 'a' }, 16));
    try std.testing.expectError(error.OutputLimitExceeded, decodeRawLz4Bounded(allocator, &.{ 0x1A, 'a', 0x01, 0x00, 0x00 }, 14));
}

fn boundedRawAllocationTest(allocator: std.mem.Allocator, compressed: []const u8) !void {
    var decoded = try decodeRawLz4Bounded(allocator, compressed, 20);
    defer decoded.deinit(allocator);
    try std.testing.expectEqualStrings("abcdabcdabcdabcdabcd", decoded.bytes);
}

test "bounded raw LZ4 decoder cleans up across every allocation failure" {
    const compressed = [_]u8{
        0x47, 'a', 'b', 'c', 'd', 0x04, 0x00,
        0x50, 'd', 'a', 'b', 'c', 'd',
    };
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        boundedRawAllocationTest,
        .{compressed[0..]},
    );
}

test "raw LZ4 decoder rejects malformed lengths offsets and output sizes" {
    const allocator = std.testing.allocator;

    try std.testing.expectError(error.TruncatedToken, decodeRawLz4(allocator, &.{}, 1));
    try std.testing.expectError(error.TruncatedLiteralLength, decodeRawLz4(allocator, &.{0xF0}, 15));
    try std.testing.expectError(error.TruncatedLiterals, decodeRawLz4(allocator, &.{ 0x20, 'a' }, 2));
    try std.testing.expectError(error.OutputOverrun, decodeRawLz4(allocator, &.{ 0x20, 'a', 'b' }, 1));
    try std.testing.expectError(error.OutputUnderrun, decodeRawLz4(allocator, &.{ 0x10, 'a' }, 2));
    try std.testing.expectError(error.TruncatedMatchOffset, decodeRawLz4(allocator, &.{ 0x10, 'a', 0x01 }, 5));
    try std.testing.expectError(error.ZeroMatchOffset, decodeRawLz4(allocator, &.{ 0x10, 'a', 0x00, 0x00 }, 5));
    try std.testing.expectError(error.MatchOffsetBeyondOutput, decodeRawLz4(allocator, &.{ 0x10, 'a', 0x02, 0x00 }, 5));
    try std.testing.expectError(error.TruncatedMatchLength, decodeRawLz4(allocator, &.{ 0x1F, 'a', 0x01, 0x00 }, 20));
    try std.testing.expectError(error.OutputOverrun, decodeRawLz4(allocator, &.{ 0x10, 'a', 0x01, 0x00 }, 4));
    try std.testing.expectError(error.FinalSequenceHasMatch, decodeRawLz4(allocator, &.{ 0x11, 'a' }, 1));
    try std.testing.expectError(error.MissingFinalLiteralSequence, decodeRawLz4(allocator, &.{ 0x10, 'a', 0x01, 0x00 }, 5));
}

test "request envelope enforces size cap and exact zero carrier padding" {
    const allocator = std.testing.allocator;
    const config: Config = .{ .max_original_size = 16 };
    const envelope = try encodeRequestEnvelope(allocator, "java-wire", config);
    defer allocator.free(envelope);
    try std.testing.expectEqual(@as(u32, 9), std.mem.readInt(u32, envelope[0..4], .big));

    const padded = try allocator.alloc(u8, envelope.len + 7);
    defer allocator.free(padded);
    @memcpy(padded[0..envelope.len], envelope);
    @memset(padded[envelope.len..], 0);
    var decoded = try decodeRequestEnvelope(allocator, padded, config);
    defer decoded.deinit(allocator);
    try std.testing.expectEqualStrings("java-wire", decoded.body);
    try std.testing.expectEqual(envelope.len, decoded.envelope_bytes_consumed);

    padded[padded.len - 1] = 1;
    try std.testing.expectError(error.NonZeroCarrierPadding, decodeRequestEnvelope(allocator, padded, config));
    try std.testing.expectError(
        error.OriginalSizeTooLarge,
        encodeRequestEnvelope(allocator, "seventeen bytes!!!", config),
    );
    var oversized = [_]u8{ 0x00, 0x00, 0x00, 0x11, 0x00 };
    try std.testing.expectError(error.OriginalSizeTooLarge, decodeRequestEnvelope(allocator, &oversized, config));

    try std.testing.expectError(
        error.OriginalSizeTooLarge,
        validateOriginalSize(max_java_original_size + 1, .{
            .max_original_size = std.math.maxInt(u32),
        }),
    );
    const negative_java_size = [_]u8{ 0x80, 0x00, 0x00, 0x00, 0x00 };
    try std.testing.expectError(
        error.OriginalSizeTooLarge,
        decodeRequestEnvelope(
            allocator,
            &negative_java_size,
            .{ .max_original_size = std.math.maxInt(u32) },
        ),
    );
}

test "signed big-endian carriers preserve 8 and 40-byte boundaries" {
    const allocator = std.testing.allocator;
    const sizes = [_]usize{ 8, 40, 41 };
    const expected_words = [_]usize{ 5, 5, 10 };
    for (sizes, expected_words) |size, expected| {
        const input = try allocator.alloc(u8, size);
        defer allocator.free(input);
        for (input, 0..) |*byte, index| byte.* = @truncate(0x80 +% index);

        const words = try packSignedWordsBigEndian(allocator, input);
        defer allocator.free(words);
        try std.testing.expectEqual(expected, words.len);
        try std.testing.expectEqual(@as(usize, expected / 5), try frameCount(words));
        if (size != 0) try std.testing.expect(words[0] < 0);

        const unpacked = try unpackSignedWordsBigEndian(allocator, words);
        defer allocator.free(unpacked);
        try std.testing.expectEqualSlices(u8, input, unpacked[0..input.len]);
        for (unpacked[input.len..]) |padding_byte| try std.testing.expectEqual(@as(u8, 0), padding_byte);
    }
    try std.testing.expectError(
        error.CarrierWordCountNotMultipleOfFive,
        unpackSignedWordsBigEndian(allocator, &.{ 1, 2, 3, 4 }),
    );
}

test "SIMD carrier transforms match scalar big-endian semantics across vector tails" {
    const allocator = std.testing.allocator;
    const vector_words = std.simd.suggestVectorLength(u64) orelse 1;
    const vector_bytes = vector_words * @sizeOf(u64);
    const lengths = [_]usize{
        0,
        1,
        7,
        8,
        9,
        vector_bytes - 1,
        vector_bytes,
        vector_bytes + 1,
        vector_bytes * 2 - 1,
        vector_bytes * 2,
        vector_bytes * 2 + 1,
        39,
        40,
        41,
        127,
        128,
        129,
        4099,
    };

    for (lengths) |length| {
        const input = try allocator.alloc(u8, length);
        defer allocator.free(input);
        for (input, 0..) |*byte, index| {
            byte.* = @truncate(index *% 193 +% length);
        }

        const words = try packSignedWordsBigEndian(allocator, input);
        defer allocator.free(words);
        try std.testing.expectEqual(@as(usize, 0), words.len % words_per_frame);

        const live_words = (input.len + 7) / 8;
        for (0..live_words) |index| {
            var raw = [_]u8{0} ** 8;
            const start = index * 8;
            const end = @min(start + 8, input.len);
            @memcpy(raw[0 .. end - start], input[start..end]);
            try std.testing.expectEqual(std.mem.readInt(i64, &raw, .big), words[index]);
        }
        for (words[live_words..]) |padding_word| {
            try std.testing.expectEqual(@as(i64, 0), padding_word);
        }

        const unpacked = try unpackSignedWordsBigEndian(allocator, words);
        defer allocator.free(unpacked);
        try std.testing.expectEqualSlices(u8, input, unpacked[0..input.len]);
        for (unpacked[input.len..]) |padding_byte| {
            try std.testing.expectEqual(@as(u8, 0), padding_byte);
        }
    }
}

test "Java little-endian single-user query body has exact bytes" {
    const allocator = std.testing.allocator;
    const body = try binary_payload.encodeJavaReportQueryToBytes(
        allocator,
        .{ .single_user = 0x0102030405060708 },
    );
    defer allocator.free(body);
    try std.testing.expectEqualSlices(
        u8,
        &.{ 0x12, 0x27, 0x00, 0x00, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01 },
        body,
    );
    try std.testing.expectEqualDeep(
        binary_payload.DecodedReportQuery{ .single_user = 0x0102030405060708 },
        try binary_payload.decodeJavaQueryFromBytes(body),
    );
}

test "all standard Java command and query body schemas roundtrip" {
    const allocator = std.testing.allocator;
    const symbols = [_]common.CoreSymbolSpecification{
        try common.CoreSymbolSpecification.init(.{
            .symbol_id = 241,
            .symbol_type = .currency_exchange_pair,
            .base_currency = 11,
            .quote_currency = 15,
            .base_scale_k = 100,
            .quote_scale_k = 10,
            .taker_fee = 2,
            .maker_fee = 1,
        }),
        try common.CoreSymbolSpecification.init(.{
            .symbol_id = 551,
            .symbol_type = .futures_contract,
            .base_currency = 12,
            .quote_currency = 16,
            .base_scale_k = 200,
            .quote_scale_k = 20,
            .taker_fee = 4,
            .maker_fee = 3,
            .margin_buy = 90,
            .margin_sell = 100,
        }),
    };
    const symbols_body = try binary_payload.encodeJavaAddSymbolsToBytes(allocator, &symbols);
    defer allocator.free(symbols_body);
    var decoded_symbols = try binary_payload.decodeJavaFromBytes(allocator, symbols_body);
    defer decoded_symbols.deinit();
    switch (decoded_symbols.payload) {
        .add_symbols => |actual| try std.testing.expectEqualDeep(symbols[0..], actual),
        else => unreachable,
    }

    const balances_a = [_]binary_payload.CurrencyBalance{
        .{ .currency = 11, .balance = 500 },
        .{ .currency = 15, .balance = -900 },
    };
    const balances_b = [_]binary_payload.CurrencyBalance{
        .{ .currency = 12, .balance = 1200 },
    };
    const users = [_]binary_payload.UserAccountsBatch{
        .{ .uid = 1001, .accounts = &balances_a },
        .{ .uid = 1002, .accounts = &balances_b },
    };
    const accounts_body = try binary_payload.encodeJavaAddAccountsToBytes(allocator, &users);
    defer allocator.free(accounts_body);
    var decoded_accounts = try binary_payload.decodeJavaFromBytes(allocator, accounts_body);
    defer decoded_accounts.deinit();
    switch (decoded_accounts.payload) {
        .add_accounts => |actual| {
            try std.testing.expectEqual(@as(usize, 2), actual.len);
            try std.testing.expectEqual(@as(i64, 1001), actual[0].uid);
            try std.testing.expectEqualDeep(balances_a[0..], actual[0].accounts);
            try std.testing.expectEqual(@as(i64, 1002), actual[1].uid);
            try std.testing.expectEqualDeep(balances_b[0..], actual[1].accounts);
        },
        else => unreachable,
    }

    const queries = [_]binary_payload.DecodedReportQuery{
        .state_hash,
        .{ .single_user = -7711 },
        .total_currency_balance,
    };
    for (queries) |query| {
        const query_body = try binary_payload.encodeJavaReportQueryToBytes(allocator, query);
        defer allocator.free(query_body);
        try std.testing.expectEqualDeep(query, try binary_payload.decodeJavaQueryFromBytes(query_body));
    }
}

test "request carrier spans multiple five-word frames and roundtrips" {
    const allocator = std.testing.allocator;
    var body: [64]u8 = undefined;
    for (&body, 0..) |*byte, index| byte.* = @truncate(index *% 17 +% 3);

    const words = try encodeRequestWords(allocator, &body, .{});
    defer allocator.free(words);
    try std.testing.expectEqual(@as(usize, 10), words.len);
    try std.testing.expectEqual(@as(usize, 2), try frameCount(words));

    var decoded = try decodeRequestWords(allocator, words, .{});
    defer decoded.deinit(allocator);
    try std.testing.expectEqualSlices(u8, &body, decoded.body);
}
