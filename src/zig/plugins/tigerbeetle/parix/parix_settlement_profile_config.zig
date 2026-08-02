//! Parix host integration for the opt-in TigerBeetle plugin.
const std = @import("std");

pub const format_version: u16 = 1;
pub const max_profile_bytes: usize = 64 * 1024;
pub const max_database_uuid_bytes: usize = 36;
pub const max_users: usize = 16;
pub const max_initial_funding: usize = 4;

pub const Publication = struct {
    namespace: u8,
    journal_epoch: u16,
};

pub const Symbol = struct {
    symbol_id: i32,
    symbol_type: SymbolType,
    base_currency: i32,
    quote_currency: i32,
    base_scale_k: i64,
    quote_scale_k: i64,
    taker_fee: i64,
    maker_fee: i64,
    margin_buy: i64,
    margin_sell: i64,
};

pub const SymbolType = enum {
    currency_exchange_pair,
};

pub const Currency = struct {
    currency: i32,
    ledger: u32,
    treasury_account_id: u128,
    fee_account_id: u128,
};

pub const User = struct {
    uid: i64,
    base_account_id: u128,
    quote_account_id: u128,
};

pub const InitialFunding = struct {
    id: u128,
    uid: i64,
    currency: i32,
    amount: u128,
};

/// Fully bounded representation copied out of the temporary JSON parse tree.
/// No slice in this value borrows the input JSON buffer.
pub const Document = struct {
    version: u16,
    database_uuid: [max_database_uuid_bytes]u8,
    publication: Publication,
    symbol: Symbol,
    currencies: [2]Currency,
    users_storage: [max_users]User = undefined,
    user_count: usize,
    funding_storage: [max_initial_funding]InitialFunding = undefined,
    funding_count: usize,

    pub fn users(self: *const Document) []const User {
        return self.users_storage[0..self.user_count];
    }

    pub fn initialFunding(self: *const Document) []const InitialFunding {
        return self.funding_storage[0..self.funding_count];
    }
};

pub const ParseError = std.mem.Allocator.Error || error{
    EmptyProfile,
    ProfileTooLarge,
    JsonNestingTooDeep,
    InvalidJson,
    InvalidRoot,
    InvalidObjectShape,
    MissingField,
    InvalidFieldType,
    InvalidInteger,
    InvalidString,
    UnsupportedVersion,
    InvalidCurrencyCount,
    InvalidUserCount,
    TooManyUsers,
    TooManyInitialFundingEntries,
};

pub fn parse(
    allocator: std.mem.Allocator,
    bytes: []const u8,
) ParseError!Document {
    if (bytes.len == 0) return error.EmptyProfile;
    if (bytes.len >= max_profile_bytes) return error.ProfileTooLarge;
    if (!jsonNestingWithinLimit(bytes, 8)) return error.JsonNestingTooDeep;

    var parsed = std.json.parseFromSlice(std.json.Value, allocator, bytes, .{
        .duplicate_field_behavior = .@"error",
        .parse_numbers = false,
        .max_value_len = 256,
    }) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        return error.InvalidJson;
    };
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidRoot,
    };
    try exactShape(root, 7);

    const version = try requiredInt(u16, root, "version");
    if (version != format_version) return error.UnsupportedVersion;

    const database_uuid_text = try requiredString(root, "database_uuid");
    if (database_uuid_text.len != max_database_uuid_bytes) {
        return error.InvalidString;
    }
    var database_uuid: [max_database_uuid_bytes]u8 = undefined;
    @memcpy(&database_uuid, database_uuid_text);

    const publication_object = try requiredObject(root, "publication");
    try exactShape(publication_object, 2);
    const publication: Publication = .{
        .namespace = try requiredInt(u8, publication_object, "namespace"),
        .journal_epoch = try requiredInt(u16, publication_object, "journal_epoch"),
    };

    const symbol_object = try requiredObject(root, "symbol");
    try exactShape(symbol_object, 10);
    const raw_symbol_type = try requiredString(symbol_object, "symbol_type");
    const symbol_type: SymbolType = if (std.mem.eql(
        u8,
        raw_symbol_type,
        "currency_exchange_pair",
    )) .currency_exchange_pair else return error.InvalidString;
    const symbol: Symbol = .{
        .symbol_id = try requiredInt(i32, symbol_object, "symbol_id"),
        .symbol_type = symbol_type,
        .base_currency = try requiredInt(i32, symbol_object, "base_currency"),
        .quote_currency = try requiredInt(i32, symbol_object, "quote_currency"),
        .base_scale_k = try requiredInt(i64, symbol_object, "base_scale_k"),
        .quote_scale_k = try requiredInt(i64, symbol_object, "quote_scale_k"),
        .taker_fee = try requiredInt(i64, symbol_object, "taker_fee"),
        .maker_fee = try requiredInt(i64, symbol_object, "maker_fee"),
        .margin_buy = try requiredInt(i64, symbol_object, "margin_buy"),
        .margin_sell = try requiredInt(i64, symbol_object, "margin_sell"),
    };

    const currencies_array = try requiredArray(root, "currencies");
    if (currencies_array.items.len != 2) return error.InvalidCurrencyCount;
    var currencies: [2]Currency = undefined;
    for (currencies_array.items, 0..) |value, index| {
        const object = try valueObject(value);
        try exactShape(object, 4);
        currencies[index] = .{
            .currency = try requiredInt(i32, object, "currency"),
            .ledger = try requiredInt(u32, object, "ledger"),
            .treasury_account_id = try requiredDecimalString(
                u128,
                object,
                "treasury_account_id",
            ),
            .fee_account_id = try requiredDecimalString(
                u128,
                object,
                "fee_account_id",
            ),
        };
    }

    const users_array = try requiredArray(root, "users");
    if (users_array.items.len == 0) return error.InvalidUserCount;
    if (users_array.items.len > max_users) return error.TooManyUsers;
    var users_storage: [max_users]User = undefined;
    for (users_array.items, 0..) |value, index| {
        const object = try valueObject(value);
        try exactShape(object, 3);
        users_storage[index] = .{
            .uid = try requiredInt(i64, object, "uid"),
            .base_account_id = try requiredDecimalString(
                u128,
                object,
                "base_account_id",
            ),
            .quote_account_id = try requiredDecimalString(
                u128,
                object,
                "quote_account_id",
            ),
        };
    }

    const funding_array = try requiredArray(root, "initial_funding");
    if (funding_array.items.len > max_initial_funding) {
        return error.TooManyInitialFundingEntries;
    }
    var funding_storage: [max_initial_funding]InitialFunding = undefined;
    for (funding_array.items, 0..) |value, index| {
        const object = try valueObject(value);
        try exactShape(object, 4);
        funding_storage[index] = .{
            .id = try requiredDecimalString(u128, object, "id"),
            .uid = try requiredInt(i64, object, "uid"),
            .currency = try requiredInt(i32, object, "currency"),
            .amount = try requiredDecimalString(u128, object, "amount"),
        };
    }

    return .{
        .version = version,
        .database_uuid = database_uuid,
        .publication = publication,
        .symbol = symbol,
        .currencies = currencies,
        .users_storage = users_storage,
        .user_count = users_array.items.len,
        .funding_storage = funding_storage,
        .funding_count = funding_array.items.len,
    };
}

fn exactShape(object: std.json.ObjectMap, expected_fields: usize) ParseError!void {
    if (object.count() != expected_fields) return error.InvalidObjectShape;
}

fn valueObject(value: std.json.Value) ParseError!std.json.ObjectMap {
    return switch (value) {
        .object => |object| object,
        else => error.InvalidFieldType,
    };
}

fn requiredObject(
    object: std.json.ObjectMap,
    key: []const u8,
) ParseError!std.json.ObjectMap {
    return valueObject(object.get(key) orelse return error.MissingField);
}

fn requiredArray(
    object: std.json.ObjectMap,
    key: []const u8,
) ParseError!std.json.Array {
    const value = object.get(key) orelse return error.MissingField;
    return switch (value) {
        .array => |array| array,
        else => error.InvalidFieldType,
    };
}

fn requiredString(
    object: std.json.ObjectMap,
    key: []const u8,
) ParseError![]const u8 {
    const value = object.get(key) orelse return error.MissingField;
    return switch (value) {
        .string => |text| text,
        else => error.InvalidFieldType,
    };
}

fn requiredInt(
    comptime T: type,
    object: std.json.ObjectMap,
    key: []const u8,
) ParseError!T {
    const value = object.get(key) orelse return error.MissingField;
    const text = switch (value) {
        .number_string => |number| number,
        else => return error.InvalidFieldType,
    };
    return parseDecimal(T, text);
}

fn requiredDecimalString(
    comptime T: type,
    object: std.json.ObjectMap,
    key: []const u8,
) ParseError!T {
    const value = object.get(key) orelse return error.MissingField;
    const text = switch (value) {
        .string => |number| number,
        else => return error.InvalidFieldType,
    };
    return parseDecimal(T, text);
}

fn parseDecimal(comptime T: type, text: []const u8) ParseError!T {
    if (text.len == 0) return error.InvalidInteger;
    var index: usize = 0;
    if (@typeInfo(T).int.signedness == .signed and text[0] == '-') {
        if (text.len == 1) return error.InvalidInteger;
        index = 1;
    }
    for (text[index..]) |byte| {
        if (byte < '0' or byte > '9') return error.InvalidInteger;
    }
    return std.fmt.parseInt(T, text, 10) catch error.InvalidInteger;
}

fn jsonNestingWithinLimit(bytes: []const u8, maximum: usize) bool {
    var depth: usize = 0;
    var in_string = false;
    var escaped = false;
    for (bytes) |byte| {
        if (in_string) {
            if (escaped) {
                escaped = false;
            } else if (byte == '\\') {
                escaped = true;
            } else if (byte == '"') {
                in_string = false;
            }
            continue;
        }
        switch (byte) {
            '"' => in_string = true,
            '{', '[' => {
                depth += 1;
                if (depth > maximum) return false;
            },
            '}', ']' => {
                if (depth == 0) return false;
                depth -= 1;
            },
            else => {},
        }
    }
    return !in_string and depth == 0;
}

test "parser accepts bounded profile and rejects duplicate JSON fields" {
    const valid =
        \\{
        \\  "version": 1,
        \\  "database_uuid": "0e46f90e-c38b-487a-b9f9-630f7839cc3f",
        \\  "publication": {"namespace": 1, "journal_epoch": 0},
        \\  "symbol": {"symbol_id": 241, "symbol_type": "currency_exchange_pair", "base_currency": 11, "quote_currency": 15, "base_scale_k": 100, "quote_scale_k": 10, "taker_fee": 2, "maker_fee": 1, "margin_buy": 0, "margin_sell": 0},
        \\  "currencies": [{"currency": 11, "ledger": 421, "treasury_account_id": "420011", "fee_account_id": "42011"}, {"currency": 15, "ledger": 425, "treasury_account_id": "420015", "fee_account_id": "42015"}],
        \\  "users": [{"uid": 1001, "base_account_id": "421011", "quote_account_id": "421015"}],
        \\  "initial_funding": []
        \\}
    ;
    const document = try parse(std.testing.allocator, valid);
    try std.testing.expectEqual(@as(usize, 1), document.users().len);
    try std.testing.expectEqual(@as(u128, 420_011), document.currencies[0].treasury_account_id);

    const duplicate =
        \\{"version":1,"version":1,"database_uuid":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","publication":{"namespace":1,"journal_epoch":0},"symbol":{"symbol_id":241,"symbol_type":"currency_exchange_pair","base_currency":11,"quote_currency":15,"base_scale_k":100,"quote_scale_k":10,"taker_fee":2,"maker_fee":1,"margin_buy":0,"margin_sell":0},"currencies":[],"users":[],"initial_funding":[]}
    ;
    try std.testing.expectError(error.InvalidJson, parse(std.testing.allocator, duplicate));
}

test "parser enforces user and funding bounds" {
    var users_buffer: [4096]u8 = undefined;
    var users_writer = std.Io.Writer.fixed(&users_buffer);
    try users_writer.writeAll("[");
    for (0..max_users + 1) |index| {
        if (index != 0) try users_writer.writeByte(',');
        try users_writer.print(
            "{{\"uid\":{d},\"base_account_id\":\"{d}\",\"quote_account_id\":\"{d}\"}}",
            .{ index + 1, index + 100, index + 200 },
        );
    }
    try users_writer.writeByte(']');

    var profile_buffer: [8192]u8 = undefined;
    var writer = std.Io.Writer.fixed(&profile_buffer);
    try writer.print(
        "{{\"version\":1,\"database_uuid\":\"0e46f90e-c38b-487a-b9f9-630f7839cc3f\",\"publication\":{{\"namespace\":1,\"journal_epoch\":0}},\"symbol\":{{\"symbol_id\":241,\"symbol_type\":\"currency_exchange_pair\",\"base_currency\":11,\"quote_currency\":15,\"base_scale_k\":100,\"quote_scale_k\":10,\"taker_fee\":2,\"maker_fee\":1,\"margin_buy\":0,\"margin_sell\":0}},\"currencies\":[{{\"currency\":11,\"ledger\":421,\"treasury_account_id\":\"1\",\"fee_account_id\":\"2\"}},{{\"currency\":15,\"ledger\":425,\"treasury_account_id\":\"3\",\"fee_account_id\":\"4\"}}],\"users\":{s},\"initial_funding\":[]}}",
        .{users_writer.buffered()},
    );
    try std.testing.expectError(
        error.TooManyUsers,
        parse(std.testing.allocator, writer.buffered()),
    );

    var funding_buffer: [1024]u8 = undefined;
    var funding_writer = std.Io.Writer.fixed(&funding_buffer);
    try funding_writer.writeByte('[');
    for (0..max_initial_funding + 1) |index| {
        if (index != 0) try funding_writer.writeByte(',');
        try funding_writer.print(
            "{{\"id\":\"{d}\",\"uid\":1,\"currency\":11,\"amount\":\"1\"}}",
            .{index + 1},
        );
    }
    try funding_writer.writeByte(']');

    var funding_profile_buffer: [8192]u8 = undefined;
    var funding_profile_writer = std.Io.Writer.fixed(&funding_profile_buffer);
    try funding_profile_writer.print(
        "{{\"version\":1,\"database_uuid\":\"0e46f90e-c38b-487a-b9f9-630f7839cc3f\",\"publication\":{{\"namespace\":1,\"journal_epoch\":0}},\"symbol\":{{\"symbol_id\":241,\"symbol_type\":\"currency_exchange_pair\",\"base_currency\":11,\"quote_currency\":15,\"base_scale_k\":100,\"quote_scale_k\":10,\"taker_fee\":2,\"maker_fee\":1,\"margin_buy\":0,\"margin_sell\":0}},\"currencies\":[{{\"currency\":11,\"ledger\":421,\"treasury_account_id\":\"1\",\"fee_account_id\":\"2\"}},{{\"currency\":15,\"ledger\":425,\"treasury_account_id\":\"3\",\"fee_account_id\":\"4\"}}],\"users\":[{{\"uid\":1,\"base_account_id\":\"5\",\"quote_account_id\":\"6\"}}],\"initial_funding\":{s}}}",
        .{funding_writer.buffered()},
    );
    try std.testing.expectError(
        error.TooManyInitialFundingEntries,
        parse(std.testing.allocator, funding_profile_writer.buffered()),
    );
}
