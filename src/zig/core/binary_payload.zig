const std = @import("std");
const common = @import("common.zig");
const binary_commands = @import("binary_commands.zig");
const ByteReader = std.Io.Reader;

pub const CurrencyBalance = binary_commands.CurrencyBalance;
pub const UserAccountsBatch = binary_commands.UserAccountsBatch;

pub const BinaryCommandTypeCode = enum(i32) {
    add_accounts = 1002,
    add_symbols = 1003,

    pub const FromCodeError = error{
        UnsupportedBinaryCommandType,
    };

    pub fn fromCode(raw: i32) FromCodeError!BinaryCommandTypeCode {
        return switch (raw) {
            1002 => .add_accounts,
            1003 => .add_symbols,
            else => error.UnsupportedBinaryCommandType,
        };
    }
};

pub const DecodedPayload = union(enum) {
    add_symbols: []common.CoreSymbolSpecification,
    add_accounts: []UserAccountsBatch,
};

pub const DecodedBinaryCommand = struct {
    arena: std.heap.ArenaAllocator,
    payload: DecodedPayload,

    pub fn deinit(self: *DecodedBinaryCommand) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub const DecodeError = std.mem.Allocator.Error ||
    common.SymbolType.FromCodeError ||
    common.CoreSymbolSpecification.ValidationError ||
    BinaryCommandTypeCode.FromCodeError ||
    error{
        TruncatedPayload,
        NegativeCollectionSize,
        SymbolKeyMismatch,
    };

pub const ReportQueryTypeCode = enum(i32) {
    state_hash = 10001,
    single_user = 10002,
    total_currency_balance = 10003,

    pub const FromCodeError = error{
        UnsupportedReportQueryType,
    };

    pub fn fromCode(raw: i32) FromCodeError!ReportQueryTypeCode {
        return switch (raw) {
            10001 => .state_hash,
            10002 => .single_user,
            10003 => .total_currency_balance,
            else => error.UnsupportedReportQueryType,
        };
    }
};

pub const DecodedReportQuery = union(enum) {
    state_hash,
    single_user: i64,
    total_currency_balance,
    /// Java logs and ignores an unregistered constructor or a registered
    /// constructor that fails while reading its body, while the binary
    /// transfer itself still completes successfully.
    unknown: i32,
};

/// Borrowed report-query wire envelope. `body` contains every byte after the
/// four-byte type code, including bytes not understood by built-in decoders.
pub const RawReportQuery = struct {
    code: i32,
    body: []const u8,
};

pub const DecodeRawReportQueryError = error{
    TruncatedPayload,
};

pub const DecodeQueryError = std.mem.Allocator.Error ||
    ReportQueryTypeCode.FromCodeError ||
    error{
        TruncatedPayload,
    };

pub const EncodeError = std.mem.Allocator.Error || error{
    CollectionTooLarge,
};

pub fn decodeFromBytes(
    allocator: std.mem.Allocator,
    bytes: []const u8,
) DecodeError!DecodedBinaryCommand {
    return decodeFromBytesEndian(allocator, bytes, .big);
}

/// Decodes the native Chronicle Bytes body emitted by the Java implementation.
/// Java's direct NativeBytes use little-endian primitive encoding on the
/// supported little-endian hosts. This is intentionally separate from the
/// existing Zig big-endian codec so current Zig callers keep their wire format.
pub fn decodeJavaFromBytes(
    allocator: std.mem.Allocator,
    bytes: []const u8,
) DecodeError!DecodedBinaryCommand {
    return decodeFromBytesEndian(allocator, bytes, .little);
}

fn decodeFromBytesEndian(
    allocator: std.mem.Allocator,
    bytes: []const u8,
    endian: std.builtin.Endian,
) DecodeError!DecodedBinaryCommand {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const arena_allocator = arena.allocator();

    var reader = std.Io.Reader.fixed(bytes);

    const raw_code = try readInt(i32, &reader, endian);
    const code = try BinaryCommandTypeCode.fromCode(raw_code);

    const payload: DecodedPayload = switch (code) {
        .add_symbols => .{ .add_symbols = try decodeAddSymbols(&reader, arena_allocator, endian) },
        .add_accounts => .{ .add_accounts = try decodeAddAccounts(&reader, arena_allocator, endian) },
    };

    return .{
        .arena = arena,
        .payload = payload,
    };
}

pub fn decodeFromWordsBigEndian(
    allocator: std.mem.Allocator,
    words: []const i64,
) DecodeError!DecodedBinaryCommand {
    const bytes = try allocator.alloc(u8, words.len * @sizeOf(i64));
    defer allocator.free(bytes);

    for (words, 0..) |word, idx| {
        const start = idx * @sizeOf(i64);
        var raw: [8]u8 = undefined;
        std.mem.writeInt(i64, &raw, word, .big);
        @memcpy(bytes[start .. start + @sizeOf(i64)], raw[0..]);
    }

    return decodeFromBytes(allocator, bytes);
}

pub fn decodeQueryFromBytes(bytes: []const u8) DecodeQueryError!DecodedReportQuery {
    return decodeQueryFromBytesEndian(bytes, .big);
}

/// Decodes a report-query body written by Java Chronicle NativeBytes.
pub fn decodeJavaQueryFromBytes(bytes: []const u8) DecodeQueryError!DecodedReportQuery {
    return decodeQueryFromBytesEndian(bytes, .little);
}

/// Decodes the legacy Zig big-endian report-query envelope without interpreting
/// or copying its body.
pub fn decodeRawReportQueryFromBytes(
    bytes: []const u8,
) DecodeRawReportQueryError!RawReportQuery {
    return decodeRawReportQueryFromBytesEndian(bytes, .big);
}

/// Decodes the Java Chronicle NativeBytes little-endian report-query envelope
/// without interpreting or copying its body.
pub fn decodeJavaRawReportQueryFromBytes(
    bytes: []const u8,
) DecodeRawReportQueryError!RawReportQuery {
    return decodeRawReportQueryFromBytesEndian(bytes, .little);
}

fn decodeRawReportQueryFromBytesEndian(
    bytes: []const u8,
    endian: std.builtin.Endian,
) DecodeRawReportQueryError!RawReportQuery {
    if (bytes.len < @sizeOf(i32)) return error.TruncatedPayload;
    return .{
        .code = std.mem.readInt(i32, bytes[0..@sizeOf(i32)], endian),
        .body = bytes[@sizeOf(i32)..],
    };
}

/// Matches `BinaryCommandsProcessor.deserializeQuery`: an unknown class code
/// or a known constructor that cannot consume its required fields is ignored,
/// and constructors are not required to consume the remaining bytes.
pub fn decodeJavaQueryOrIgnoredFromBytes(bytes: []const u8) DecodeQueryError!DecodedReportQuery {
    if (bytes.len < @sizeOf(i32)) return error.TruncatedPayload;
    const raw_code = std.mem.readInt(i32, bytes[0..@sizeOf(i32)], .little);
    return decodeQueryFromBytesEndian(bytes, .little) catch |err| switch (err) {
        error.TruncatedPayload => .{ .unknown = raw_code },
        error.OutOfMemory => error.OutOfMemory,
        error.UnsupportedReportQueryType => .{ .unknown = raw_code },
    };
}

fn decodeQueryFromBytesEndian(
    bytes: []const u8,
    endian: std.builtin.Endian,
) DecodeQueryError!DecodedReportQuery {
    var reader = std.Io.Reader.fixed(bytes);

    const raw_code = try readQueryInt(i32, &reader, endian);
    const query = ReportQueryTypeCode.fromCode(raw_code) catch {
        return .{ .unknown = raw_code };
    };
    return switch (query) {
        .state_hash => .state_hash,
        .single_user => .{
            .single_user = try readQueryInt(i64, &reader, endian),
        },
        .total_currency_balance => .total_currency_balance,
    };
}

pub fn decodeQueryFromWordsBigEndian(
    allocator: std.mem.Allocator,
    words: []const i64,
) DecodeQueryError!DecodedReportQuery {
    const bytes = try allocator.alloc(u8, words.len * @sizeOf(i64));
    defer allocator.free(bytes);

    for (words, 0..) |word, idx| {
        const start = idx * @sizeOf(i64);
        var raw: [8]u8 = undefined;
        std.mem.writeInt(i64, &raw, word, .big);
        @memcpy(bytes[start .. start + @sizeOf(i64)], raw[0..]);
    }

    return decodeQueryFromBytes(bytes);
}

pub fn encodeAddSymbolsToBytes(
    allocator: std.mem.Allocator,
    symbols: []const common.CoreSymbolSpecification,
) EncodeError![]u8 {
    return encodeAddSymbolsToBytesEndian(allocator, symbols, .big);
}

/// Encodes the uncompressed command body expected by Java Chronicle NativeBytes.
pub fn encodeJavaAddSymbolsToBytes(
    allocator: std.mem.Allocator,
    symbols: []const common.CoreSymbolSpecification,
) EncodeError![]u8 {
    return encodeAddSymbolsToBytesEndian(allocator, symbols, .little);
}

fn encodeAddSymbolsToBytesEndian(
    allocator: std.mem.Allocator,
    symbols: []const common.CoreSymbolSpecification,
    endian: std.builtin.Endian,
) EncodeError![]u8 {
    var bytes: std.Io.Writer.Allocating = .init(allocator);
    errdefer bytes.deinit();
    const writer = &bytes.writer;
    writer.writeInt(i32, @intFromEnum(BinaryCommandTypeCode.add_symbols), endian) catch return error.OutOfMemory;
    writer.writeInt(i32, try castLenI32(symbols.len), endian) catch return error.OutOfMemory;

    for (symbols) |spec| {
        writer.writeInt(i32, spec.symbol_id, endian) catch return error.OutOfMemory;
        writer.writeInt(i32, spec.symbol_id, endian) catch return error.OutOfMemory;
        writer.writeInt(i8, spec.symbol_type.code(), endian) catch return error.OutOfMemory;
        writer.writeInt(i32, spec.base_currency, endian) catch return error.OutOfMemory;
        writer.writeInt(i32, spec.quote_currency, endian) catch return error.OutOfMemory;
        writer.writeInt(i64, spec.base_scale_k, endian) catch return error.OutOfMemory;
        writer.writeInt(i64, spec.quote_scale_k, endian) catch return error.OutOfMemory;
        writer.writeInt(i64, spec.taker_fee, endian) catch return error.OutOfMemory;
        writer.writeInt(i64, spec.maker_fee, endian) catch return error.OutOfMemory;
        writer.writeInt(i64, spec.margin_buy, endian) catch return error.OutOfMemory;
        writer.writeInt(i64, spec.margin_sell, endian) catch return error.OutOfMemory;
    }

    return bytes.toOwnedSlice();
}

pub fn encodeAddAccountsToBytes(
    allocator: std.mem.Allocator,
    users: []const UserAccountsBatch,
) EncodeError![]u8 {
    return encodeAddAccountsToBytesEndian(allocator, users, .big);
}

/// Encodes the uncompressed account-batch body expected by Java NativeBytes.
pub fn encodeJavaAddAccountsToBytes(
    allocator: std.mem.Allocator,
    users: []const UserAccountsBatch,
) EncodeError![]u8 {
    return encodeAddAccountsToBytesEndian(allocator, users, .little);
}

fn encodeAddAccountsToBytesEndian(
    allocator: std.mem.Allocator,
    users: []const UserAccountsBatch,
    endian: std.builtin.Endian,
) EncodeError![]u8 {
    var bytes: std.Io.Writer.Allocating = .init(allocator);
    errdefer bytes.deinit();
    const writer = &bytes.writer;
    writer.writeInt(i32, @intFromEnum(BinaryCommandTypeCode.add_accounts), endian) catch return error.OutOfMemory;
    writer.writeInt(i32, try castLenI32(users.len), endian) catch return error.OutOfMemory;

    for (users) |user| {
        writer.writeInt(i64, user.uid, endian) catch return error.OutOfMemory;
        writer.writeInt(i32, try castLenI32(user.accounts.len), endian) catch return error.OutOfMemory;
        for (user.accounts) |account| {
            writer.writeInt(i32, account.currency, endian) catch return error.OutOfMemory;
            writer.writeInt(i64, account.balance, endian) catch return error.OutOfMemory;
        }
    }

    return bytes.toOwnedSlice();
}

pub fn encodeReportQueryToBytes(
    allocator: std.mem.Allocator,
    query: DecodedReportQuery,
) std.mem.Allocator.Error![]u8 {
    return encodeReportQueryToBytesEndian(allocator, query, .big);
}

/// Encodes the uncompressed query body expected by Java NativeBytes.
pub fn encodeJavaReportQueryToBytes(
    allocator: std.mem.Allocator,
    query: DecodedReportQuery,
) std.mem.Allocator.Error![]u8 {
    return encodeReportQueryToBytesEndian(allocator, query, .little);
}

/// Encodes an arbitrary report-query code and uninterpreted body using the
/// legacy Zig big-endian code representation.
pub fn encodeRawReportQueryToBytes(
    allocator: std.mem.Allocator,
    code: i32,
    body: []const u8,
) std.mem.Allocator.Error![]u8 {
    return encodeRawReportQueryToBytesEndian(allocator, code, body, .big);
}

/// Encodes an arbitrary report-query code and uninterpreted body using the Java
/// Chronicle NativeBytes little-endian code representation.
pub fn encodeJavaRawReportQueryToBytes(
    allocator: std.mem.Allocator,
    code: i32,
    body: []const u8,
) std.mem.Allocator.Error![]u8 {
    return encodeRawReportQueryToBytesEndian(allocator, code, body, .little);
}

fn encodeRawReportQueryToBytesEndian(
    allocator: std.mem.Allocator,
    code: i32,
    body: []const u8,
    endian: std.builtin.Endian,
) std.mem.Allocator.Error![]u8 {
    var bytes: std.Io.Writer.Allocating = .init(allocator);
    errdefer bytes.deinit();
    const writer = &bytes.writer;
    writer.writeInt(i32, code, endian) catch return error.OutOfMemory;
    writer.writeAll(body) catch return error.OutOfMemory;
    return bytes.toOwnedSlice();
}

fn encodeReportQueryToBytesEndian(
    allocator: std.mem.Allocator,
    query: DecodedReportQuery,
    endian: std.builtin.Endian,
) std.mem.Allocator.Error![]u8 {
    var bytes: std.Io.Writer.Allocating = .init(allocator);
    errdefer bytes.deinit();
    const writer = &bytes.writer;
    switch (query) {
        .state_hash => {
            writer.writeInt(i32, @intFromEnum(ReportQueryTypeCode.state_hash), endian) catch return error.OutOfMemory;
        },
        .single_user => |uid| {
            writer.writeInt(i32, @intFromEnum(ReportQueryTypeCode.single_user), endian) catch return error.OutOfMemory;
            writer.writeInt(i64, uid, endian) catch return error.OutOfMemory;
        },
        .total_currency_balance => {
            writer.writeInt(i32, @intFromEnum(ReportQueryTypeCode.total_currency_balance), endian) catch return error.OutOfMemory;
        },
        .unknown => |raw_code| {
            writer.writeInt(i32, raw_code, endian) catch return error.OutOfMemory;
        },
    }

    return bytes.toOwnedSlice();
}

fn decodeAddSymbols(
    reader: *ByteReader,
    allocator: std.mem.Allocator,
    endian: std.builtin.Endian,
) DecodeError![]common.CoreSymbolSpecification {
    const count_i32 = try readInt(i32, reader, endian);
    if (count_i32 < 0) return error.NegativeCollectionSize;
    const count: usize = @intCast(count_i32);
    const bytes_per_symbol: usize = 2 * @sizeOf(i32) + @sizeOf(i8) +
        2 * @sizeOf(i32) + 6 * @sizeOf(i64);
    if (count > readerRemaining(reader) / bytes_per_symbol) return error.TruncatedPayload;

    const specs = try allocator.alloc(common.CoreSymbolSpecification, count);
    for (0..count) |idx| {
        const key_symbol_id = try readInt(i32, reader, endian);
        const symbol_id = try readInt(i32, reader, endian);
        const symbol_type_code = try readInt(i8, reader, endian);
        const symbol_type = try common.SymbolType.fromCode(@as(i32, symbol_type_code));
        const spec = try common.CoreSymbolSpecification.init(.{
            .symbol_id = symbol_id,
            .symbol_type = symbol_type,
            .base_currency = try readInt(i32, reader, endian),
            .quote_currency = try readInt(i32, reader, endian),
            .base_scale_k = try readInt(i64, reader, endian),
            .quote_scale_k = try readInt(i64, reader, endian),
            .taker_fee = try readInt(i64, reader, endian),
            .maker_fee = try readInt(i64, reader, endian),
            .margin_buy = try readInt(i64, reader, endian),
            .margin_sell = try readInt(i64, reader, endian),
        });
        if (key_symbol_id != spec.symbol_id) return error.SymbolKeyMismatch;
        specs[idx] = spec;
    }
    return specs;
}

fn decodeAddAccounts(
    reader: *ByteReader,
    allocator: std.mem.Allocator,
    endian: std.builtin.Endian,
) DecodeError![]UserAccountsBatch {
    const count_i32 = try readInt(i32, reader, endian);
    if (count_i32 < 0) return error.NegativeCollectionSize;
    const count: usize = @intCast(count_i32);
    const bytes_per_user_header: usize = @sizeOf(i64) + @sizeOf(i32);
    if (count > readerRemaining(reader) / bytes_per_user_header) return error.TruncatedPayload;

    const users = try allocator.alloc(UserAccountsBatch, count);
    for (0..count) |idx| {
        const uid = try readInt(i64, reader, endian);
        const accounts_count_i32 = try readInt(i32, reader, endian);
        if (accounts_count_i32 < 0) return error.NegativeCollectionSize;
        const accounts_count: usize = @intCast(accounts_count_i32);
        const bytes_per_account: usize = @sizeOf(i32) + @sizeOf(i64);
        const future_user_headers = (count - idx - 1) * bytes_per_user_header;
        const remaining = readerRemaining(reader);
        if (remaining < future_user_headers) return error.TruncatedPayload;
        if (accounts_count > (remaining - future_user_headers) / bytes_per_account) {
            return error.TruncatedPayload;
        }
        const accounts = try allocator.alloc(CurrencyBalance, accounts_count);
        for (0..accounts_count) |acc_idx| {
            accounts[acc_idx] = .{
                .currency = try readInt(i32, reader, endian),
                .balance = try readInt(i64, reader, endian),
            };
        }
        users[idx] = .{
            .uid = uid,
            .accounts = accounts,
        };
    }
    return users;
}

fn readInt(comptime T: type, reader: *ByteReader, endian: std.builtin.Endian) DecodeError!T {
    return reader.takeInt(T, endian) catch return error.TruncatedPayload;
}

fn readQueryInt(comptime T: type, reader: *ByteReader, endian: std.builtin.Endian) DecodeQueryError!T {
    return reader.takeInt(T, endian) catch return error.TruncatedPayload;
}

fn readerRemaining(reader: *const ByteReader) usize {
    std.debug.assert(reader.seek <= reader.end);
    return reader.end - reader.seek;
}

fn castLenI32(len: usize) error{CollectionTooLarge}!i32 {
    return std.math.cast(i32, len) orelse error.CollectionTooLarge;
}

test "encode add symbols payload roundtrips through decode" {
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
            .symbol_id = 555,
            .symbol_type = .futures_contract,
            .base_currency = 11,
            .quote_currency = 15,
            .base_scale_k = 100,
            .quote_scale_k = 10,
            .taker_fee = 3,
            .maker_fee = 2,
            .margin_buy = 50,
            .margin_sell = 60,
        }),
    };

    const payload = try encodeAddSymbolsToBytes(allocator, symbols[0..]);
    defer allocator.free(payload);

    var decoded = try decodeFromBytes(allocator, payload);
    defer decoded.deinit();
    switch (decoded.payload) {
        .add_symbols => |decoded_symbols| {
            try std.testing.expectEqual(@as(usize, 2), decoded_symbols.len);
            try std.testing.expectEqualDeep(symbols[0], decoded_symbols[0]);
            try std.testing.expectEqualDeep(symbols[1], decoded_symbols[1]);
        },
        else => unreachable,
    }
}

test "encode add accounts payload roundtrips through decode" {
    const allocator = std.testing.allocator;

    const accounts_1 = [_]CurrencyBalance{
        .{ .currency = 11, .balance = 500 },
        .{ .currency = 15, .balance = 900 },
    };
    const accounts_2 = [_]CurrencyBalance{
        .{ .currency = 15, .balance = 1200 },
    };
    const users = [_]UserAccountsBatch{
        .{ .uid = 1001, .accounts = accounts_1[0..] },
        .{ .uid = 1002, .accounts = accounts_2[0..] },
    };

    const payload = try encodeAddAccountsToBytes(allocator, users[0..]);
    defer allocator.free(payload);

    var decoded = try decodeFromBytes(allocator, payload);
    defer decoded.deinit();
    switch (decoded.payload) {
        .add_accounts => |decoded_users| {
            try std.testing.expectEqual(@as(usize, 2), decoded_users.len);
            try std.testing.expectEqual(@as(i64, 1001), decoded_users[0].uid);
            try std.testing.expectEqual(@as(usize, 2), decoded_users[0].accounts.len);
            try std.testing.expectEqual(@as(i32, 11), decoded_users[0].accounts[0].currency);
            try std.testing.expectEqual(@as(i64, 500), decoded_users[0].accounts[0].balance);
            try std.testing.expectEqual(@as(i64, 1002), decoded_users[1].uid);
            try std.testing.expectEqual(@as(usize, 1), decoded_users[1].accounts.len);
            try std.testing.expectEqual(@as(i64, 1200), decoded_users[1].accounts[0].balance);
        },
        else => unreachable,
    }
}

test "encode report query payload roundtrips through decode query" {
    const allocator = std.testing.allocator;

    const state_hash = try encodeReportQueryToBytes(allocator, .state_hash);
    defer allocator.free(state_hash);
    try std.testing.expectEqualDeep(DecodedReportQuery.state_hash, try decodeQueryFromBytes(state_hash));

    const single_user = try encodeReportQueryToBytes(allocator, .{ .single_user = 9901 });
    defer allocator.free(single_user);
    try std.testing.expectEqualDeep(
        DecodedReportQuery{ .single_user = 9901 },
        try decodeQueryFromBytes(single_user),
    );

    const total_balance = try encodeReportQueryToBytes(allocator, .total_currency_balance);
    defer allocator.free(total_balance);
    try std.testing.expectEqualDeep(
        DecodedReportQuery.total_currency_balance,
        try decodeQueryFromBytes(total_balance),
    );
}

test "raw report query decoding preserves built-in unknown and custom bodies" {
    const built_in_body = [_]u8{ 0xde, 0xad, 0xbe, 0xef };
    var built_in_wire: [@sizeOf(i32) + built_in_body.len]u8 = undefined;
    std.mem.writeInt(
        i32,
        built_in_wire[0..@sizeOf(i32)],
        @intFromEnum(ReportQueryTypeCode.state_hash),
        .big,
    );
    @memcpy(built_in_wire[@sizeOf(i32)..], &built_in_body);

    const built_in = try decodeRawReportQueryFromBytes(&built_in_wire);
    try std.testing.expectEqual(
        @as(i32, @intFromEnum(ReportQueryTypeCode.state_hash)),
        built_in.code,
    );
    try std.testing.expectEqualSlices(u8, &built_in_body, built_in.body);

    var unknown_wire: [@sizeOf(i32)]u8 = undefined;
    std.mem.writeInt(i32, &unknown_wire, 9_999, .little);
    const unknown = try decodeJavaRawReportQueryFromBytes(&unknown_wire);
    try std.testing.expectEqual(@as(i32, 9_999), unknown.code);
    try std.testing.expectEqual(@as(usize, 0), unknown.body.len);

    const custom_body = [_]u8{ 0x00, 0x11, 0x22, 0x00, 0xff };
    var custom_wire: [@sizeOf(i32) + custom_body.len]u8 = undefined;
    std.mem.writeInt(i32, custom_wire[0..@sizeOf(i32)], 70_001, .little);
    @memcpy(custom_wire[@sizeOf(i32)..], &custom_body);

    const custom = try decodeJavaRawReportQueryFromBytes(&custom_wire);
    try std.testing.expectEqual(@as(i32, 70_001), custom.code);
    try std.testing.expectEqualSlices(u8, &custom_body, custom.body);
}

test "raw report query envelopes roundtrip in legacy and Java endian modes" {
    const allocator = std.testing.allocator;
    const code: i32 = -1_234_567;
    const body = [_]u8{ 0x01, 0x00, 0x80, 0xff, 0x7f, 0x42 };

    const legacy_bytes = try encodeRawReportQueryToBytes(allocator, code, &body);
    defer allocator.free(legacy_bytes);
    const legacy = try decodeRawReportQueryFromBytes(legacy_bytes);
    try std.testing.expectEqual(code, legacy.code);
    try std.testing.expectEqualSlices(u8, &body, legacy.body);

    const java_bytes = try encodeJavaRawReportQueryToBytes(allocator, code, &body);
    defer allocator.free(java_bytes);
    const java = try decodeJavaRawReportQueryFromBytes(java_bytes);
    try std.testing.expectEqual(code, java.code);
    try std.testing.expectEqualSlices(u8, &body, java.body);

    try std.testing.expect(!std.mem.eql(u8, legacy_bytes[0..4], java_bytes[0..4]));
}

test "raw report query decoding rejects every truncated code" {
    const truncated = [_]u8{ 0x01, 0x02, 0x03 };
    for (0..@sizeOf(i32)) |len| {
        try std.testing.expectError(
            error.TruncatedPayload,
            decodeRawReportQueryFromBytes(truncated[0..len]),
        );
        try std.testing.expectError(
            error.TruncatedPayload,
            decodeJavaRawReportQueryFromBytes(truncated[0..len]),
        );
    }
}

fn rawReportQueryEncodeAllocationCampaign(
    allocator: std.mem.Allocator,
) !void {
    var body: [1_024]u8 = undefined;
    for (&body, 0..) |*byte, index| byte.* = @truncate(index);

    const legacy = try encodeRawReportQueryToBytes(allocator, 70_001, &body);
    defer allocator.free(legacy);
    try std.testing.expectEqual(@as(usize, @sizeOf(i32) + body.len), legacy.len);

    const java = try encodeJavaRawReportQueryToBytes(allocator, 70_001, &body);
    defer allocator.free(java);
    try std.testing.expectEqual(@as(usize, @sizeOf(i32) + body.len), java.len);
}

test "raw report query encoding cleans up every allocation failure" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        rawReportQueryEncodeAllocationCampaign,
        .{},
    );
}

test "decode add symbols payload from bytes" {
    var bytes: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer bytes.deinit();
    const writer = &bytes.writer;

    writer.writeInt(i32, 1003, .big) catch return error.OutOfMemory;
    writer.writeInt(i32, 2, .big) catch return error.OutOfMemory;

    const specs = [_]common.CoreSymbolSpecification{
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
            .symbol_id = 555,
            .symbol_type = .futures_contract,
            .base_currency = 11,
            .quote_currency = 15,
            .base_scale_k = 100,
            .quote_scale_k = 10,
            .taker_fee = 3,
            .maker_fee = 2,
            .margin_buy = 50,
            .margin_sell = 60,
        }),
    };

    for (specs) |spec| {
        writer.writeInt(i32, spec.symbol_id, .big) catch return error.OutOfMemory;
        writer.writeInt(i32, spec.symbol_id, .big) catch return error.OutOfMemory;
        writer.writeInt(i8, spec.symbol_type.code(), .big) catch return error.OutOfMemory;
        writer.writeInt(i32, spec.base_currency, .big) catch return error.OutOfMemory;
        writer.writeInt(i32, spec.quote_currency, .big) catch return error.OutOfMemory;
        writer.writeInt(i64, spec.base_scale_k, .big) catch return error.OutOfMemory;
        writer.writeInt(i64, spec.quote_scale_k, .big) catch return error.OutOfMemory;
        writer.writeInt(i64, spec.taker_fee, .big) catch return error.OutOfMemory;
        writer.writeInt(i64, spec.maker_fee, .big) catch return error.OutOfMemory;
        writer.writeInt(i64, spec.margin_buy, .big) catch return error.OutOfMemory;
        writer.writeInt(i64, spec.margin_sell, .big) catch return error.OutOfMemory;
    }

    var decoded = try decodeFromBytes(std.testing.allocator, bytes.written());
    defer decoded.deinit();
    switch (decoded.payload) {
        .add_symbols => |decoded_specs| {
            try std.testing.expectEqual(@as(usize, 2), decoded_specs.len);
            try std.testing.expectEqualDeep(specs[0], decoded_specs[0]);
            try std.testing.expectEqualDeep(specs[1], decoded_specs[1]);
        },
        else => unreachable,
    }
}

test "decode add accounts payload from bytes and from big-endian words" {
    var bytes: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer bytes.deinit();
    const writer = &bytes.writer;

    writer.writeInt(i32, 1002, .big) catch return error.OutOfMemory;
    writer.writeInt(i32, 2, .big) catch return error.OutOfMemory;

    writer.writeInt(i64, 1001, .big) catch return error.OutOfMemory;
    writer.writeInt(i32, 2, .big) catch return error.OutOfMemory;
    writer.writeInt(i32, 11, .big) catch return error.OutOfMemory;
    writer.writeInt(i64, 500, .big) catch return error.OutOfMemory;
    writer.writeInt(i32, 15, .big) catch return error.OutOfMemory;
    writer.writeInt(i64, 900, .big) catch return error.OutOfMemory;

    writer.writeInt(i64, 1002, .big) catch return error.OutOfMemory;
    writer.writeInt(i32, 1, .big) catch return error.OutOfMemory;
    writer.writeInt(i32, 15, .big) catch return error.OutOfMemory;
    writer.writeInt(i64, 1200, .big) catch return error.OutOfMemory;

    var decoded = try decodeFromBytes(std.testing.allocator, bytes.written());
    defer decoded.deinit();
    switch (decoded.payload) {
        .add_accounts => |users| {
            try std.testing.expectEqual(@as(usize, 2), users.len);
            try std.testing.expectEqual(@as(i64, 1001), users[0].uid);
            try std.testing.expectEqual(@as(usize, 2), users[0].accounts.len);
            try std.testing.expectEqual(@as(i32, 11), users[0].accounts[0].currency);
            try std.testing.expectEqual(@as(i64, 500), users[0].accounts[0].balance);
            try std.testing.expectEqual(@as(i64, 1002), users[1].uid);
            try std.testing.expectEqual(@as(usize, 1), users[1].accounts.len);
            try std.testing.expectEqual(@as(i64, 1200), users[1].accounts[0].balance);
        },
        else => unreachable,
    }

    // Convert to big-endian words with zero padding and decode through word path.
    const word_count_no_padding = (bytes.written().len + 7) / 8;
    const padded_word_count = if (word_count_no_padding % 5 == 0)
        word_count_no_padding
    else
        word_count_no_padding + (5 - (word_count_no_padding % 5));

    var words = try std.testing.allocator.alloc(i64, padded_word_count);
    defer std.testing.allocator.free(words);
    @memset(words, 0);

    for (0..word_count_no_padding) |idx| {
        var raw: [8]u8 = [_]u8{0} ** 8;
        const start = idx * 8;
        const end = @min(start + 8, bytes.written().len);
        @memcpy(raw[0 .. end - start], bytes.written()[start..end]);
        words[idx] = std.mem.readInt(i64, &raw, .big);
    }

    var decoded_from_words = try decodeFromWordsBigEndian(std.testing.allocator, words);
    defer decoded_from_words.deinit();
    switch (decoded_from_words.payload) {
        .add_accounts => |users| {
            try std.testing.expectEqual(@as(usize, 2), users.len);
            try std.testing.expectEqual(@as(i64, 1001), users[0].uid);
            try std.testing.expectEqual(@as(i64, 1002), users[1].uid);
        },
        else => unreachable,
    }
}

test "unsupported binary command code returns error" {
    var bytes: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer bytes.deinit();
    const writer = &bytes.writer;
    writer.writeInt(i32, 9999, .big) catch return error.OutOfMemory;

    try std.testing.expectError(
        error.UnsupportedBinaryCommandType,
        decodeFromBytes(std.testing.allocator, bytes.written()),
    );
}

test "decode query payload from bytes and words" {
    var bytes: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer bytes.deinit();
    const writer = &bytes.writer;
    writer.writeInt(i32, 10001, .big) catch return error.OutOfMemory;

    const decoded = try decodeQueryFromBytes(bytes.written());
    try std.testing.expectEqual(DecodedReportQuery.state_hash, decoded);

    const word_count_no_padding = (bytes.written().len + 7) / 8;
    const padded_word_count = if (word_count_no_padding % 5 == 0)
        word_count_no_padding
    else
        word_count_no_padding + (5 - (word_count_no_padding % 5));

    var words = try std.testing.allocator.alloc(i64, padded_word_count);
    defer std.testing.allocator.free(words);
    @memset(words, 0);

    for (0..word_count_no_padding) |idx| {
        var raw: [8]u8 = [_]u8{0} ** 8;
        const start = idx * 8;
        const end = @min(start + 8, bytes.written().len);
        @memcpy(raw[0 .. end - start], bytes.written()[start..end]);
        words[idx] = std.mem.readInt(i64, &raw, .big);
    }

    const decoded_words = try decodeQueryFromWordsBigEndian(std.testing.allocator, words);
    try std.testing.expectEqual(DecodedReportQuery.state_hash, decoded_words);
}

test "unsupported query type code remains distinct from malformed input" {
    var bytes: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer bytes.deinit();
    const writer = &bytes.writer;
    writer.writeInt(i32, 9999, .big) catch return error.OutOfMemory;

    const decoded = try decodeQueryFromBytes(bytes.written());
    switch (decoded) {
        .unknown => |raw_code| try std.testing.expectEqual(@as(i32, 9999), raw_code),
        else => return error.TestExpectedUnknownQuery,
    }
    try std.testing.expectError(error.TruncatedPayload, decodeQueryFromBytes(&.{ 1, 2, 3 }));
}

test "Java report query decoding ignores unknown tails and constructor failures" {
    var unknown: [9]u8 = undefined;
    std.mem.writeInt(i32, unknown[0..4], 77_777, .little);
    @memset(unknown[4..], 0xa5);
    switch (try decodeJavaQueryOrIgnoredFromBytes(&unknown)) {
        .unknown => |raw_code| try std.testing.expectEqual(@as(i32, 77_777), raw_code),
        else => return error.TestExpectedUnknownQuery,
    }

    var state_with_tail: [7]u8 = undefined;
    std.mem.writeInt(i32, state_with_tail[0..4], 10_001, .little);
    @memset(state_with_tail[4..], 0x5a);
    try std.testing.expectEqual(
        DecodedReportQuery.state_hash,
        try decodeJavaQueryOrIgnoredFromBytes(&state_with_tail),
    );

    var truncated_single: [8]u8 = undefined;
    std.mem.writeInt(i32, truncated_single[0..4], 10_002, .little);
    @memset(truncated_single[4..], 0);
    switch (try decodeJavaQueryOrIgnoredFromBytes(&truncated_single)) {
        .unknown => |raw_code| try std.testing.expectEqual(@as(i32, 10_002), raw_code),
        else => return error.TestExpectedUnknownQuery,
    }

    try std.testing.expectError(
        error.TruncatedPayload,
        decodeJavaQueryOrIgnoredFromBytes(&.{ 1, 2, 3 }),
    );
}

test "decode single user query payload" {
    var bytes: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer bytes.deinit();
    const writer = &bytes.writer;
    writer.writeInt(i32, 10002, .big) catch return error.OutOfMemory;
    writer.writeInt(i64, 4242, .big) catch return error.OutOfMemory;

    const decoded = try decodeQueryFromBytes(bytes.written());
    switch (decoded) {
        .single_user => |uid| try std.testing.expectEqual(@as(i64, 4242), uid),
        else => unreachable,
    }
}

test "huge command counts in tiny Java and legacy bodies fail before allocation" {
    const allocator = std.testing.allocator;
    const cases = [_]BinaryCommandTypeCode{ .add_accounts, .add_symbols };
    for (cases) |command_type| {
        var legacy_body: [8]u8 = undefined;
        std.mem.writeInt(i32, legacy_body[0..4], @intFromEnum(command_type), .big);
        std.mem.writeInt(i32, legacy_body[4..8], std.math.maxInt(i32), .big);
        try std.testing.expectError(
            error.TruncatedPayload,
            decodeFromBytes(allocator, &legacy_body),
        );

        var java_body: [8]u8 = undefined;
        std.mem.writeInt(i32, java_body[0..4], @intFromEnum(command_type), .little);
        std.mem.writeInt(i32, java_body[4..8], std.math.maxInt(i32), .little);
        try std.testing.expectError(
            error.TruncatedPayload,
            decodeJavaFromBytes(allocator, &java_body),
        );
    }
}
