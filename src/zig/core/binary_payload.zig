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
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const arena_allocator = arena.allocator();

    var reader = std.Io.Reader.fixed(bytes);

    const raw_code = try readInt(i32, &reader);
    const code = try BinaryCommandTypeCode.fromCode(raw_code);

    const payload: DecodedPayload = switch (code) {
        .add_symbols => .{ .add_symbols = try decodeAddSymbols(&reader, arena_allocator) },
        .add_accounts => .{ .add_accounts = try decodeAddAccounts(&reader, arena_allocator) },
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
    var reader = std.Io.Reader.fixed(bytes);

    const raw_code = try readQueryInt(i32, &reader);
    const query = try ReportQueryTypeCode.fromCode(raw_code);
    return switch (query) {
        .state_hash => .state_hash,
        .single_user => .{
            .single_user = try readQueryInt(i64, &reader),
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
    var bytes: std.Io.Writer.Allocating = .init(allocator);
    errdefer bytes.deinit();
    const writer = &bytes.writer;
    writer.writeInt(i32, @intFromEnum(BinaryCommandTypeCode.add_symbols), .big) catch return error.OutOfMemory;
    writer.writeInt(i32, try castLenI32(symbols.len), .big) catch return error.OutOfMemory;

    for (symbols) |spec| {
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

    return bytes.toOwnedSlice();
}

pub fn encodeAddAccountsToBytes(
    allocator: std.mem.Allocator,
    users: []const UserAccountsBatch,
) EncodeError![]u8 {
    var bytes: std.Io.Writer.Allocating = .init(allocator);
    errdefer bytes.deinit();
    const writer = &bytes.writer;
    writer.writeInt(i32, @intFromEnum(BinaryCommandTypeCode.add_accounts), .big) catch return error.OutOfMemory;
    writer.writeInt(i32, try castLenI32(users.len), .big) catch return error.OutOfMemory;

    for (users) |user| {
        writer.writeInt(i64, user.uid, .big) catch return error.OutOfMemory;
        writer.writeInt(i32, try castLenI32(user.accounts.len), .big) catch return error.OutOfMemory;
        for (user.accounts) |account| {
            writer.writeInt(i32, account.currency, .big) catch return error.OutOfMemory;
            writer.writeInt(i64, account.balance, .big) catch return error.OutOfMemory;
        }
    }

    return bytes.toOwnedSlice();
}

pub fn encodeReportQueryToBytes(
    allocator: std.mem.Allocator,
    query: DecodedReportQuery,
) std.mem.Allocator.Error![]u8 {
    var bytes: std.Io.Writer.Allocating = .init(allocator);
    errdefer bytes.deinit();
    const writer = &bytes.writer;
    switch (query) {
        .state_hash => {
            writer.writeInt(i32, @intFromEnum(ReportQueryTypeCode.state_hash), .big) catch return error.OutOfMemory;
        },
        .single_user => |uid| {
            writer.writeInt(i32, @intFromEnum(ReportQueryTypeCode.single_user), .big) catch return error.OutOfMemory;
            writer.writeInt(i64, uid, .big) catch return error.OutOfMemory;
        },
        .total_currency_balance => {
            writer.writeInt(i32, @intFromEnum(ReportQueryTypeCode.total_currency_balance), .big) catch return error.OutOfMemory;
        },
    }

    return bytes.toOwnedSlice();
}

fn decodeAddSymbols(
    reader: *ByteReader,
    allocator: std.mem.Allocator,
) DecodeError![]common.CoreSymbolSpecification {
    const count_i32 = try readInt(i32, reader);
    if (count_i32 < 0) return error.NegativeCollectionSize;
    const count: usize = @intCast(count_i32);

    const specs = try allocator.alloc(common.CoreSymbolSpecification, count);
    for (0..count) |idx| {
        const key_symbol_id = try readInt(i32, reader);
        const symbol_id = try readInt(i32, reader);
        const symbol_type_code = try readInt(i8, reader);
        const symbol_type = try common.SymbolType.fromCode(@as(i32, symbol_type_code));
        const spec = try common.CoreSymbolSpecification.init(.{
            .symbol_id = symbol_id,
            .symbol_type = symbol_type,
            .base_currency = try readInt(i32, reader),
            .quote_currency = try readInt(i32, reader),
            .base_scale_k = try readInt(i64, reader),
            .quote_scale_k = try readInt(i64, reader),
            .taker_fee = try readInt(i64, reader),
            .maker_fee = try readInt(i64, reader),
            .margin_buy = try readInt(i64, reader),
            .margin_sell = try readInt(i64, reader),
        });
        if (key_symbol_id != spec.symbol_id) return error.SymbolKeyMismatch;
        specs[idx] = spec;
    }
    return specs;
}

fn decodeAddAccounts(
    reader: *ByteReader,
    allocator: std.mem.Allocator,
) DecodeError![]UserAccountsBatch {
    const count_i32 = try readInt(i32, reader);
    if (count_i32 < 0) return error.NegativeCollectionSize;
    const count: usize = @intCast(count_i32);

    const users = try allocator.alloc(UserAccountsBatch, count);
    for (0..count) |idx| {
        const uid = try readInt(i64, reader);
        const accounts_count_i32 = try readInt(i32, reader);
        if (accounts_count_i32 < 0) return error.NegativeCollectionSize;
        const accounts_count: usize = @intCast(accounts_count_i32);
        const accounts = try allocator.alloc(CurrencyBalance, accounts_count);
        for (0..accounts_count) |acc_idx| {
            accounts[acc_idx] = .{
                .currency = try readInt(i32, reader),
                .balance = try readInt(i64, reader),
            };
        }
        users[idx] = .{
            .uid = uid,
            .accounts = accounts,
        };
    }
    return users;
}

fn readInt(comptime T: type, reader: *ByteReader) DecodeError!T {
    return reader.takeInt(T, .big) catch return error.TruncatedPayload;
}

fn readQueryInt(comptime T: type, reader: *ByteReader) DecodeQueryError!T {
    return reader.takeInt(T, .big) catch return error.TruncatedPayload;
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

test "unsupported query type code returns error" {
    var bytes: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer bytes.deinit();
    const writer = &bytes.writer;
    writer.writeInt(i32, 9999, .big) catch return error.OutOfMemory;

    try std.testing.expectError(
        error.UnsupportedReportQueryType,
        decodeQueryFromBytes(bytes.written()),
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
