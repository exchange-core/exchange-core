const std = @import("std");
const exchange_core = @import("exchange_core");

const core = exchange_core.core;
const adapter = core.tigerbeetle_parix_adapter;
const registry = core.tigerbeetle_destination_registry;
const settlement = core.tigerbeetle_settlement;

pub const database_uuid = "0e46f90e-c38b-487a-b9f9-630f7839cc3f";
pub const symbol_id: i32 = 241;
pub const base_currency: i32 = 11;
pub const quote_currency: i32 = 15;
pub const base_ledger: u32 = 421;
pub const quote_ledger: u32 = 425;
pub const seller_uid: i64 = 1001;
pub const buyer_uid: i64 = 1002;

pub const treasury_account_code: u16 = 100;
pub const user_account_code: u16 = 110;
pub const fee_account_code: u16 = 210;
pub const funding_transfer_code: u16 = 10;
pub const history_flag: u16 = 8;
pub const constrained_user_flags: u16 = history_flag | 2;

pub const base_treasury_account_id: u128 = 420_011;
pub const quote_treasury_account_id: u128 = 420_015;
pub const base_fee_account_id: u128 = 42_011;
pub const quote_fee_account_id: u128 = 42_015;
pub const seller_base_account_id: u128 = 421_011;
pub const seller_quote_account_id: u128 = 421_015;
pub const buyer_base_account_id: u128 = 422_011;
pub const buyer_quote_account_id: u128 = 422_015;

pub const base_funding_transfer_id: u128 = 8_421_011;
pub const quote_funding_transfer_id: u128 = 8_422_015;
pub const seller_base_initial_balance: u128 = 1_000;
pub const buyer_quote_initial_balance: u128 = 2_000;

pub const UuidParseError = error{
    InvalidUuidLength,
    InvalidUuidSeparator,
    InvalidUuidHex,
};

pub const AccountSpec = struct {
    id: u128,
    user_data_128: u128 = 0,
    user_data_64: u64 = 0,
    user_data_32: u32 = 0,
    ledger: u32,
    code: u16,
    flags: u16,
};

/// The subset returned by `lookup_accounts` that is relevant to provisioning.
/// Mutable balances and the server-assigned timestamp are deliberately absent.
pub const AccountRecord = AccountSpec;

pub const accounts = [_]AccountSpec{
    .{ .id = base_treasury_account_id, .ledger = base_ledger, .code = treasury_account_code, .flags = history_flag },
    .{ .id = quote_treasury_account_id, .ledger = quote_ledger, .code = treasury_account_code, .flags = history_flag },
    .{ .id = base_fee_account_id, .ledger = base_ledger, .code = fee_account_code, .flags = history_flag },
    .{ .id = quote_fee_account_id, .ledger = quote_ledger, .code = fee_account_code, .flags = history_flag },
    .{
        .id = seller_base_account_id,
        .user_data_64 = @intCast(seller_uid),
        .user_data_32 = @intCast(base_currency),
        .ledger = base_ledger,
        .code = user_account_code,
        .flags = constrained_user_flags,
    },
    .{
        .id = seller_quote_account_id,
        .user_data_64 = @intCast(seller_uid),
        .user_data_32 = @intCast(quote_currency),
        .ledger = quote_ledger,
        .code = user_account_code,
        .flags = constrained_user_flags,
    },
    .{
        .id = buyer_base_account_id,
        .user_data_64 = @intCast(buyer_uid),
        .user_data_32 = @intCast(base_currency),
        .ledger = base_ledger,
        .code = user_account_code,
        .flags = constrained_user_flags,
    },
    .{
        .id = buyer_quote_account_id,
        .user_data_64 = @intCast(buyer_uid),
        .user_data_32 = @intCast(quote_currency),
        .ledger = quote_ledger,
        .code = user_account_code,
        .flags = constrained_user_flags,
    },
};

pub const funding_transfers = [_]settlement.Transfer{
    .{
        .id = base_funding_transfer_id,
        .debit_account_id = base_treasury_account_id,
        .credit_account_id = seller_base_account_id,
        .amount = seller_base_initial_balance,
        .user_data_128 = 0,
        .ledger = base_ledger,
        .code = funding_transfer_code,
    },
    .{
        .id = quote_funding_transfer_id,
        .debit_account_id = quote_treasury_account_id,
        .credit_account_id = buyer_quote_account_id,
        .amount = buyer_quote_initial_balance,
        .user_data_128 = 0,
        .ledger = quote_ledger,
        .code = funding_transfer_code,
    },
};

pub fn parseUuidDestination(raw: []const u8) UuidParseError!registry.DestinationId {
    if (raw.len != database_uuid.len) return error.InvalidUuidLength;
    const separator_indices = [_]usize{ 8, 13, 18, 23 };
    for (separator_indices) |index| {
        if (raw[index] != '-') return error.InvalidUuidSeparator;
    }

    var value: u128 = 0;
    for (raw, 0..) |byte, index| {
        if (index == 8 or index == 13 or index == 18 or index == 23) continue;
        value = (value << 4) | try hexNibble(byte);
    }
    return @enumFromInt(value);
}

pub fn destinationToUuid(
    writer: *std.Io.Writer,
    destination: registry.DestinationId,
) std.Io.Writer.Error!void {
    const value: u128 = @intFromEnum(destination);
    const bytes: [16]u8 = @bitCast(std.mem.nativeToBig(u128, value));
    for (bytes, 0..) |byte, index| {
        if (index == 4 or index == 6 or index == 8 or index == 10) {
            try writer.writeByte('-');
        }
        try writer.print("{x:0>2}", .{byte});
    }
}

pub fn defaultDestination() registry.DestinationId {
    return parseUuidDestination(database_uuid) catch unreachable;
}

pub fn isConfiguredUser(uid: i64) bool {
    return uid == seller_uid or uid == buyer_uid;
}

pub fn createRegistry(
    allocator: std.mem.Allocator,
    destination: registry.DestinationId,
) registry.RegistryInitError!*registry.DestinationRegistry {
    const destinations = [_]registry.DestinationSpec{.{ .id = destination }};
    const symbols = [_]registry.SymbolRoute{.{
        .symbol_id = symbol_id,
        .base_currency = base_currency,
        .quote_currency = quote_currency,
        .destination_id = destination,
    }};
    const currencies = [_]registry.CurrencyRoute{
        .{
            .destination_id = destination,
            .currency = base_currency,
            .ledger = base_ledger,
            .fee_account_id = base_fee_account_id,
        },
        .{
            .destination_id = destination,
            .currency = quote_currency,
            .ledger = quote_ledger,
            .fee_account_id = quote_fee_account_id,
        },
    };
    const routes = [_]registry.AccountRoute{
        .{ .destination_id = destination, .uid = seller_uid, .currency = base_currency, .account_id = seller_base_account_id },
        .{ .destination_id = destination, .uid = seller_uid, .currency = quote_currency, .account_id = seller_quote_account_id },
        .{ .destination_id = destination, .uid = buyer_uid, .currency = base_currency, .account_id = buyer_base_account_id },
        .{ .destination_id = destination, .uid = buyer_uid, .currency = quote_currency, .account_id = buyer_quote_account_id },
    };
    return registry.DestinationRegistry.initValidated(allocator, .{
        .destinations = &destinations,
        .symbols = &symbols,
        .currencies = &currencies,
        .accounts = &routes,
    });
}

pub fn accountMatches(expected: AccountSpec, actual: AccountRecord) bool {
    return std.meta.eql(expected, actual);
}

/// Compares every caller-owned transfer field and ignores only the timestamp
/// assigned by TigerBeetle after the create request commits.
pub fn transferMatches(
    expected: settlement.Transfer,
    actual: settlement.Transfer,
) bool {
    var normalized = actual;
    normalized.timestamp = expected.timestamp;
    return std.meta.eql(expected, normalized);
}

pub fn fundingChain() settlement.TransferChain {
    var chain: settlement.TransferChain = .{ .group_id = base_funding_transfer_id };
    for (funding_transfers, 0..) |transfer, index| {
        chain.storage[index] = transfer;
    }
    chain.count = funding_transfers.len;
    return chain;
}

pub fn writeCreateAccounts(writer: *std.Io.Writer) std.Io.Writer.Error!void {
    try writer.writeByte('[');
    for (accounts, 0..) |account, index| {
        if (index != 0) try writer.writeByte(',');
        try writer.print(
            "{{\"id\":\"{d}\",\"user_data_128\":\"{d}\",\"user_data_64\":\"{d}\",\"user_data_32\":{d},\"ledger\":{d},\"code\":{d},\"flags\":{d}}}",
            .{
                account.id,
                account.user_data_128,
                account.user_data_64,
                account.user_data_32,
                account.ledger,
                account.code,
                account.flags,
            },
        );
    }
    try writer.writeByte(']');
}

pub fn writeLookupAccounts(writer: *std.Io.Writer) std.Io.Writer.Error!void {
    try writer.writeByte('[');
    for (accounts, 0..) |account, index| {
        if (index != 0) try writer.writeByte(',');
        try writer.print("\"{d}\"", .{account.id});
    }
    try writer.writeByte(']');
}

pub fn writeCreateFundingTransfers(
    writer: *std.Io.Writer,
) adapter.EncodeError!void {
    const chain = fundingChain();
    try adapter.writeCreateTransfers(writer, &chain);
}

pub fn writeLookupFundingTransfers(
    writer: *std.Io.Writer,
) adapter.EncodeError!void {
    const chain = fundingChain();
    try adapter.writeLookupTransfers(writer, &chain);
}

fn hexNibble(byte: u8) UuidParseError!u128 {
    return switch (byte) {
        '0'...'9' => byte - '0',
        'a'...'f' => byte - 'a' + 10,
        'A'...'F' => byte - 'A' + 10,
        else => error.InvalidUuidHex,
    };
}

test "database UUID has a canonical destination identity" {
    const destination = try parseUuidDestination(database_uuid);
    try std.testing.expectEqual(
        @as(u128, 0x0e46f90ec38b487ab9f9630f7839cc3f),
        @intFromEnum(destination),
    );
    try std.testing.expectEqual(
        destination,
        try parseUuidDestination("0E46F90E-C38B-487A-B9F9-630F7839CC3F"),
    );

    var buffer: [database_uuid.len]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);
    try destinationToUuid(&writer, destination);
    try std.testing.expectEqualStrings(database_uuid, writer.buffered());
}

test "database UUID parser rejects noncanonical input" {
    try std.testing.expectError(error.InvalidUuidLength, parseUuidDestination("0e46"));
    try std.testing.expectError(
        error.InvalidUuidSeparator,
        parseUuidDestination("0e46f90e_c38b-487a-b9f9-630f7839cc3f"),
    );
    try std.testing.expectError(
        error.InvalidUuidHex,
        parseUuidDestination("0e46f90e-c38b-487a-b9f9-630f7839cc3x"),
    );
}

test "profile registry resolves every configured participant" {
    var destination_registry = try createRegistry(std.testing.allocator, defaultDestination());
    defer destination_registry.deinit();

    try std.testing.expect(isConfiguredUser(seller_uid));
    try std.testing.expect(isConfiguredUser(buyer_uid));
    try std.testing.expect(!isConfiguredUser(1003));

    const specification = try core.common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol_id,
        .symbol_type = .currency_exchange_pair,
        .base_currency = base_currency,
        .quote_currency = quote_currency,
        .base_scale_k = 100,
        .quote_scale_k = 10,
        .taker_fee = 2,
        .maker_fee = 1,
    });
    const resolved = try destination_registry.resolve(
        specification,
        buyer_uid,
        seller_uid,
    );
    try std.testing.expectEqual(defaultDestination(), resolved.destination_id);
    try std.testing.expectEqual(base_ledger, resolved.ledgers.base);
    try std.testing.expectEqual(quote_ledger, resolved.ledgers.quote);
    try std.testing.expectEqual(buyer_base_account_id, resolved.taker_accounts.base_account_id);
    try std.testing.expectEqual(seller_quote_account_id, resolved.maker_accounts.quote_account_id);
    try std.testing.expectEqual(quote_fee_account_id, resolved.quote_fee_account_id);
}

test "profile IDs are unique and exact match helpers fail closed" {
    for (accounts, 0..) |account, index| {
        try std.testing.expect(account.id != 0);
        for (accounts[index + 1 ..]) |other| {
            try std.testing.expect(account.id != other.id);
        }
    }
    for (funding_transfers, 0..) |transfer, index| {
        try std.testing.expect(transfer.id != 0);
        for (funding_transfers[index + 1 ..]) |other| {
            try std.testing.expect(transfer.id != other.id);
        }
    }

    try std.testing.expect(accountMatches(accounts[0], accounts[0]));
    var changed_account = accounts[0];
    changed_account.flags ^= history_flag;
    try std.testing.expect(!accountMatches(accounts[0], changed_account));

    try std.testing.expect(transferMatches(funding_transfers[0], funding_transfers[0]));
    var timestamped = funding_transfers[0];
    timestamped.timestamp = 999;
    try std.testing.expect(transferMatches(funding_transfers[0], timestamped));
    timestamped.amount += 1;
    try std.testing.expect(!transferMatches(funding_transfers[0], timestamped));
}

test "profile writers emit canonical bare Parix arrays" {
    var account_buffer: [2048]u8 = undefined;
    var account_writer = std.Io.Writer.fixed(&account_buffer);
    try writeCreateAccounts(&account_writer);
    try std.testing.expect(std.mem.startsWith(
        u8,
        account_writer.buffered(),
        "[{\"id\":\"420011\",\"user_data_128\":\"0\"",
    ));
    try std.testing.expect(std.mem.endsWith(
        u8,
        account_writer.buffered(),
        "\"ledger\":425,\"code\":110,\"flags\":10}]",
    ));

    var lookup_buffer: [256]u8 = undefined;
    var lookup_writer = std.Io.Writer.fixed(&lookup_buffer);
    try writeLookupAccounts(&lookup_writer);
    try std.testing.expectEqualStrings(
        "[\"420011\",\"420015\",\"42011\",\"42015\",\"421011\",\"421015\",\"422011\",\"422015\"]",
        lookup_writer.buffered(),
    );

    var transfer_buffer: [2048]u8 = undefined;
    var transfer_writer = std.Io.Writer.fixed(&transfer_buffer);
    try writeCreateFundingTransfers(&transfer_writer);
    try std.testing.expect(std.mem.indexOf(
        u8,
        transfer_writer.buffered(),
        "\"id\":\"8421011\",\"debit_account_id\":\"420011\"",
    ) != null);

    var transfer_lookup_buffer: [128]u8 = undefined;
    var transfer_lookup_writer = std.Io.Writer.fixed(&transfer_lookup_buffer);
    try writeLookupFundingTransfers(&transfer_lookup_writer);
    try std.testing.expectEqualStrings(
        "[\"8421011\",\"8422015\"]",
        transfer_lookup_writer.buffered(),
    );
}
