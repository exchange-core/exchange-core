const std = @import("std");
const core = @import("exchange_core").core;
const common = core.common;
const publication = core.publication_sequence;

/// TigerBeetle's raw transfer flags. The fill mapper uses only `linked`; the
/// application-owned clearing workflow uses the mutually exclusive lifecycle
/// flags without weakening fill-outbox validation.
pub const linked_flag: u16 = 1;
pub const pending_flag: u16 = 2;
pub const post_pending_transfer_flag: u16 = 4;
pub const void_pending_transfer_flag: u16 = 8;
pub const pending_lifecycle_flags: u16 =
    pending_flag | post_pending_transfer_flag | void_pending_transfer_flag;
pub const max_transfers_per_fill: usize = 4;

const identity_namespace_shift = 120;
const identity_journal_epoch_shift = 104;
const identity_publication_sequence_shift = 40;
const identity_fill_ordinal_shift = 8;

/// Durable publication namespace shared by fills and recovered commit
/// frontiers. An epoch change creates a different domain even when sequence
/// numbers are reused from one.
pub const PublicationDomain = struct {
    namespace: u8,
    journal_epoch: u16,

    pub fn validate(self: PublicationDomain) SettlementError!void {
        if (self.namespace == 0) return error.InvalidIdentityNamespace;
    }

    pub fn eql(self: PublicationDomain, other: PublicationDomain) bool {
        return self.namespace == other.namespace and
            self.journal_epoch == other.journal_epoch;
    }
};

/// A domain-tagged publication frontier. The representation intentionally
/// permits sequence zero so recovery can express "nothing committed" in a
/// known domain; prepared records and live commit marks must call `validate`.
pub const PublicationCursor = struct {
    domain: PublicationDomain,
    publication_sequence: u64,

    pub fn validate(self: PublicationCursor) SettlementError!void {
        try self.domain.validate();
        if (self.publication_sequence == 0) {
            return error.InvalidPublicationSequence;
        }
    }

    pub fn eql(self: PublicationCursor, other: PublicationCursor) bool {
        return self.domain.eql(other.domain) and
            self.publication_sequence == other.publication_sequence;
    }

    /// Returns false across domains because sequence numbers are comparable
    /// only inside one namespace and journal epoch.
    pub fn covers(self: PublicationCursor, other: PublicationCursor) bool {
        return self.domain.eql(other.domain) and
            self.publication_sequence >= other.publication_sequence;
    }
};

/// Stable economic leg discriminators occupy the low byte of every generated
/// transfer ID. Zero is reserved for the fill group ID.
pub const SettlementLeg = enum(u8) {
    quote_principal = 1,
    base_principal = 2,
    maker_fee = 3,
    taker_fee = 4,
};

/// A lossless, sortable identity derived from the durable publication stream.
///
/// ID layout, from most to least significant bits:
/// namespace(8) | journal_epoch(16) | publication_sequence(64) |
/// fill_ordinal(32) | leg(8)
///
/// Callers must reserve one non-zero namespace for this mapper and must never
/// substitute a process-local request ID for `publication_sequence`.
/// `fill_ordinal` is zero-based among trade events in one command response.
pub const FillIdentity = struct {
    namespace: u8,
    journal_epoch: u16,
    publication_sequence: publication.PublicationSequence,
    fill_ordinal: u32,

    pub fn groupId(self: FillIdentity) SettlementError!u128 {
        return self.pack(0);
    }

    pub fn transferId(self: FillIdentity, leg: SettlementLeg) SettlementError!u128 {
        return self.pack(@intFromEnum(leg));
    }

    pub fn publicationDomain(self: FillIdentity) SettlementError!PublicationDomain {
        const result: PublicationDomain = .{
            .namespace = self.namespace,
            .journal_epoch = self.journal_epoch,
        };
        try result.validate();
        return result;
    }

    pub fn publicationCursor(self: FillIdentity) SettlementError!PublicationCursor {
        const result: PublicationCursor = .{
            .domain = try self.publicationDomain(),
            .publication_sequence = self.publication_sequence.toInt(),
        };
        try result.validate();
        return result;
    }

    fn pack(self: FillIdentity, discriminator: u8) SettlementError!u128 {
        const cursor = try self.publicationCursor();

        return (@as(u128, cursor.domain.namespace) << identity_namespace_shift) |
            (@as(u128, cursor.domain.journal_epoch) << identity_journal_epoch_shift) |
            (@as(u128, cursor.publication_sequence) << identity_publication_sequence_shift) |
            (@as(u128, self.fill_ordinal) << identity_fill_ordinal_shift) |
            @as(u128, discriminator);
    }
};

/// Decodes and validates a fill group ID. Transfer IDs are deliberately
/// rejected: the low-byte discriminator of a group ID must remain zero.
pub fn decodeFillGroupId(group_id: u128) SettlementError!FillIdentity {
    const discriminator: u8 = @truncate(group_id);
    if (discriminator != 0) return error.InvalidFillGroupDiscriminator;

    const identity: FillIdentity = .{
        .namespace = @truncate(group_id >> identity_namespace_shift),
        .journal_epoch = @truncate(group_id >> identity_journal_epoch_shift),
        .publication_sequence = .fromInt(
            @truncate(group_id >> identity_publication_sequence_shift),
        ),
        .fill_ordinal = @truncate(group_id >> identity_fill_ordinal_shift),
    };
    _ = try identity.publicationCursor();
    return identity;
}

pub const ParticipantAccounts = struct {
    base_account_id: u128,
    quote_account_id: u128,
};

pub const SettlementLedgers = struct {
    base: u32,
    quote: u32,
};

/// Codes are application-defined but must be non-zero and distinct so that
/// reconciliation can identify the economic purpose of every leg.
pub const SettlementCodes = struct {
    quote_principal: u16 = 300,
    base_principal: u16 = 302,
    maker_fee: u16 = 303,
    taker_fee: u16 = 304,

    fn forLeg(self: SettlementCodes, leg: SettlementLeg) u16 {
        return switch (leg) {
            .quote_principal => self.quote_principal,
            .base_principal => self.base_principal,
            .maker_fee => self.maker_fee,
            .taker_fee => self.taker_fee,
        };
    }
};

/// Transport-neutral representation of TigerBeetle's single-phase Transfer.
/// The later Parix/native client adapter is responsible only for serialization.
pub const Transfer = struct {
    id: u128,
    debit_account_id: u128,
    credit_account_id: u128,
    amount: u128,
    pending_id: u128 = 0,
    user_data_128: u128,
    user_data_64: u64 = 0,
    user_data_32: u32 = 0,
    timeout: u32 = 0,
    ledger: u32,
    code: u16,
    flags: u16 = 0,
    timestamp: u64 = 0,
};

/// One fill needs at most two principal legs and two fee legs. Fixed storage
/// keeps the mapper allocation-free and makes its maximum output explicit.
pub const TransferChain = struct {
    group_id: u128,
    storage: [max_transfers_per_fill]Transfer = undefined,
    count: u8 = 0,

    /// Returns a borrowed view valid only while this chain remains alive and
    /// stationary. Async/outbox adapters must own or copy the chain instead of
    /// retaining this slice.
    pub fn items(self: *const TransferChain) []const Transfer {
        return self.storage[0..@as(usize, self.count)];
    }

    fn append(self: *TransferChain, transfer: Transfer) void {
        std.debug.assert(self.count < max_transfers_per_fill);
        self.storage[@as(usize, self.count)] = transfer;
        self.count += 1;
    }

    fn finishLinking(self: *TransferChain) void {
        const emitted = self.storage[0..@as(usize, self.count)];
        for (emitted, 0..) |*transfer, index| {
            transfer.flags = if (index + 1 < emitted.len) linked_flag else 0;
        }
    }
};

/// Immutable inputs needed to map one completed matcher trade. Account IDs are
/// resolved outside this module so the hot-path mapper has no storage lookup or
/// transport dependency.
pub const SettlementFill = struct {
    identity: FillIdentity,
    taker_uid: i64,
    maker_uid: i64,
    taker_action: common.OrderAction,
    size: i64,
    price: i64,
    spec: common.CoreSymbolSpecification,
    ledgers: SettlementLedgers,
    taker_accounts: ParticipantAccounts,
    maker_accounts: ParticipantAccounts,
    quote_fee_account_id: u128,
    codes: SettlementCodes = .{},
};

pub const SettlementError = error{
    UnsupportedSymbolType,
    NonPositiveSize,
    NegativePrice,
    NonPositiveBaseScale,
    NonPositiveQuoteScale,
    NegativeMakerFee,
    NegativeTakerFee,
    InvalidIdentityNamespace,
    InvalidPublicationSequence,
    InvalidFillGroupDiscriminator,
    InvalidBaseLedger,
    InvalidQuoteLedger,
    DuplicateLedger,
    InvalidAccountId,
    DuplicateAccountId,
    SelfTradeAccountMismatch,
    InvalidTransferCode,
    DuplicateTransferCode,
    ArithmeticOverflow,
};

/// Maps a completed currency-exchange fill into one atomic linked chain.
///
/// For ordinary fills the order is quote principal, seller-role fee,
/// buyer-role fee, then base principal. Quote principal first lets a constrained
/// seller pay its fee from the proceeds. The base leg closes the chain.
///
/// Current exchange-core semantics permit self-trades and charge both role
/// fees. Their principal movements net to zero, so self-trades emit fee legs
/// only; an all-zero-fee self-trade is a deterministic empty chain.
pub fn mapFill(fill: SettlementFill) SettlementError!TransferChain {
    try validateFill(fill);

    const group_id = try fill.identity.groupId();
    const quote_step = try checkedMul(fill.price, fill.spec.quote_scale_k);
    const quote_amount = try checkedMul(fill.size, quote_step);
    const base_amount = try checkedMul(fill.size, fill.spec.base_scale_k);
    const maker_fee = try checkedMul(fill.size, fill.spec.maker_fee);
    const taker_fee = try checkedMul(fill.size, fill.spec.taker_fee);

    // R2 also accumulates both fees into one signed i64 total.
    _ = try checkedAdd(maker_fee, taker_fee);

    var chain: TransferChain = .{ .group_id = group_id };
    const self_trade = fill.taker_uid == fill.maker_uid;
    if (self_trade) {
        try appendPositive(
            &chain,
            fill,
            .maker_fee,
            fill.maker_accounts.quote_account_id,
            fill.quote_fee_account_id,
            maker_fee,
            fill.ledgers.quote,
        );
        try appendPositive(
            &chain,
            fill,
            .taker_fee,
            fill.taker_accounts.quote_account_id,
            fill.quote_fee_account_id,
            taker_fee,
            fill.ledgers.quote,
        );
        chain.finishLinking();
        return chain;
    }

    const buyer_accounts = switch (fill.taker_action) {
        .bid => fill.taker_accounts,
        .ask => fill.maker_accounts,
    };
    const seller_accounts = switch (fill.taker_action) {
        .bid => fill.maker_accounts,
        .ask => fill.taker_accounts,
    };
    const buyer_fee = switch (fill.taker_action) {
        .bid => taker_fee,
        .ask => maker_fee,
    };
    const buyer_fee_leg: SettlementLeg = switch (fill.taker_action) {
        .bid => .taker_fee,
        .ask => .maker_fee,
    };
    const seller_fee = switch (fill.taker_action) {
        .bid => maker_fee,
        .ask => taker_fee,
    };
    const seller_fee_leg: SettlementLeg = switch (fill.taker_action) {
        .bid => .maker_fee,
        .ask => .taker_fee,
    };

    // A completed R2 buyer debit must fit the same signed i64 balance domain.
    _ = try checkedAdd(quote_amount, buyer_fee);

    try appendPositive(
        &chain,
        fill,
        .quote_principal,
        buyer_accounts.quote_account_id,
        seller_accounts.quote_account_id,
        quote_amount,
        fill.ledgers.quote,
    );
    try appendPositive(
        &chain,
        fill,
        seller_fee_leg,
        seller_accounts.quote_account_id,
        fill.quote_fee_account_id,
        seller_fee,
        fill.ledgers.quote,
    );
    try appendPositive(
        &chain,
        fill,
        buyer_fee_leg,
        buyer_accounts.quote_account_id,
        fill.quote_fee_account_id,
        buyer_fee,
        fill.ledgers.quote,
    );
    try appendPositive(
        &chain,
        fill,
        .base_principal,
        seller_accounts.base_account_id,
        buyer_accounts.base_account_id,
        base_amount,
        fill.ledgers.base,
    );

    chain.finishLinking();
    return chain;
}

fn validateFill(fill: SettlementFill) SettlementError!void {
    if (fill.spec.symbol_type != .currency_exchange_pair) {
        return error.UnsupportedSymbolType;
    }
    if (fill.size <= 0) return error.NonPositiveSize;
    if (fill.price < 0) return error.NegativePrice;
    if (fill.spec.base_scale_k <= 0) return error.NonPositiveBaseScale;
    if (fill.spec.quote_scale_k <= 0) return error.NonPositiveQuoteScale;
    if (fill.spec.maker_fee < 0) return error.NegativeMakerFee;
    if (fill.spec.taker_fee < 0) return error.NegativeTakerFee;

    // Validate identity before doing arithmetic so an invalid ID can never
    // accompany otherwise valid amounts.
    _ = try fill.identity.groupId();

    if (fill.ledgers.base == 0) return error.InvalidBaseLedger;
    if (fill.ledgers.quote == 0) return error.InvalidQuoteLedger;
    if (fill.ledgers.base == fill.ledgers.quote) return error.DuplicateLedger;

    try validateCodes(fill.codes);

    const self_trade = fill.taker_uid == fill.maker_uid;
    if (self_trade) {
        if (fill.taker_accounts.base_account_id != fill.maker_accounts.base_account_id or
            fill.taker_accounts.quote_account_id != fill.maker_accounts.quote_account_id)
        {
            return error.SelfTradeAccountMismatch;
        }
        const ids = [_]u128{
            fill.taker_accounts.base_account_id,
            fill.taker_accounts.quote_account_id,
            fill.quote_fee_account_id,
        };
        try validateUniqueAccountIds(&ids);
        return;
    }

    const ids = [_]u128{
        fill.taker_accounts.base_account_id,
        fill.taker_accounts.quote_account_id,
        fill.maker_accounts.base_account_id,
        fill.maker_accounts.quote_account_id,
        fill.quote_fee_account_id,
    };
    try validateUniqueAccountIds(&ids);
}

fn validateCodes(codes: SettlementCodes) SettlementError!void {
    const values = [_]u16{
        codes.quote_principal,
        codes.base_principal,
        codes.maker_fee,
        codes.taker_fee,
    };
    for (values, 0..) |value, index| {
        if (value == 0) return error.InvalidTransferCode;
        for (values[index + 1 ..]) |other| {
            if (value == other) return error.DuplicateTransferCode;
        }
    }
}

fn validateUniqueAccountIds(ids: []const u128) SettlementError!void {
    for (ids, 0..) |id, index| {
        if (id == 0 or id == std.math.maxInt(u128)) {
            return error.InvalidAccountId;
        }
        for (ids[index + 1 ..]) |other| {
            if (id == other) return error.DuplicateAccountId;
        }
    }
}

fn checkedMul(a: i64, b: i64) SettlementError!i64 {
    return std.math.mul(i64, a, b) catch error.ArithmeticOverflow;
}

fn checkedAdd(a: i64, b: i64) SettlementError!i64 {
    return std.math.add(i64, a, b) catch error.ArithmeticOverflow;
}

fn appendPositive(
    chain: *TransferChain,
    fill: SettlementFill,
    leg: SettlementLeg,
    debit_account_id: u128,
    credit_account_id: u128,
    amount: i64,
    ledger: u32,
) SettlementError!void {
    if (amount == 0) return;
    std.debug.assert(amount > 0);
    std.debug.assert(debit_account_id != credit_account_id);

    chain.append(.{
        .id = try fill.identity.transferId(leg),
        .debit_account_id = debit_account_id,
        .credit_account_id = credit_account_id,
        .amount = @intCast(amount),
        .user_data_128 = chain.group_id,
        .ledger = ledger,
        .code = fill.codes.forLeg(leg),
    });
}

fn testSpec() common.CoreSymbolSpecification {
    return common.CoreSymbolSpecification.init(.{
        .symbol_id = 241,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 100,
        .quote_scale_k = 10,
        .taker_fee = 2,
        .maker_fee = 1,
    }) catch unreachable;
}

fn testFill(action: common.OrderAction) SettlementFill {
    return .{
        .identity = .{
            .namespace = 7,
            .journal_epoch = 9,
            .publication_sequence = .fromInt(42),
            .fill_ordinal = 3,
        },
        .taker_uid = 1001,
        .maker_uid = 2002,
        .taker_action = action,
        .size = 3,
        .price = 7,
        .spec = testSpec(),
        .ledgers = .{ .base = 11, .quote = 15 },
        .taker_accounts = .{ .base_account_id = 101, .quote_account_id = 102 },
        .maker_accounts = .{ .base_account_id = 201, .quote_account_id = 202 },
        .quote_fee_account_id = 301,
    };
}

fn expectTransfer(
    actual: Transfer,
    fill: SettlementFill,
    leg: SettlementLeg,
    debit_account_id: u128,
    credit_account_id: u128,
    amount: u128,
    ledger: u32,
    linked: bool,
) !void {
    try std.testing.expectEqual(try fill.identity.transferId(leg), actual.id);
    try std.testing.expectEqual(debit_account_id, actual.debit_account_id);
    try std.testing.expectEqual(credit_account_id, actual.credit_account_id);
    try std.testing.expectEqual(amount, actual.amount);
    try std.testing.expectEqual(@as(u128, 0), actual.pending_id);
    try std.testing.expectEqual(try fill.identity.groupId(), actual.user_data_128);
    try std.testing.expectEqual(@as(u64, 0), actual.user_data_64);
    try std.testing.expectEqual(@as(u32, 0), actual.user_data_32);
    try std.testing.expectEqual(@as(u32, 0), actual.timeout);
    try std.testing.expectEqual(ledger, actual.ledger);
    try std.testing.expectEqual(fill.codes.forLeg(leg), actual.code);
    try std.testing.expectEqual(if (linked) linked_flag else 0, actual.flags);
    try std.testing.expectEqual(@as(u64, 0), actual.timestamp);
}

test "TigerBeetle transfer flags keep clearing lifecycle values explicit" {
    try std.testing.expectEqual(@as(u16, 1), linked_flag);
    try std.testing.expectEqual(@as(u16, 2), pending_flag);
    try std.testing.expectEqual(@as(u16, 4), post_pending_transfer_flag);
    try std.testing.expectEqual(@as(u16, 8), void_pending_transfer_flag);
    try std.testing.expectEqual(@as(u16, 14), pending_lifecycle_flags);
    try std.testing.expectEqual(@as(u16, 0), linked_flag & pending_lifecycle_flags);
}

test "TigerBeetle fill group identity decodes into a domain-tagged cursor" {
    const identity = testFill(.bid).identity;
    const group_id = try identity.groupId();
    const decoded = try decodeFillGroupId(group_id);

    try std.testing.expectEqualDeep(identity, decoded);
    try std.testing.expectEqual(group_id, try decoded.groupId());
    try std.testing.expectEqualDeep(
        PublicationDomain{ .namespace = 7, .journal_epoch = 9 },
        try decoded.publicationDomain(),
    );
    try std.testing.expectEqualDeep(
        PublicationCursor{
            .domain = .{ .namespace = 7, .journal_epoch = 9 },
            .publication_sequence = 42,
        },
        try decoded.publicationCursor(),
    );
}

test "TigerBeetle fill group identity rejects discriminators and zero fields" {
    const identity = testFill(.bid).identity;
    try std.testing.expectError(
        error.InvalidFillGroupDiscriminator,
        decodeFillGroupId(try identity.transferId(.base_principal)),
    );
    try std.testing.expectError(
        error.InvalidFillGroupDiscriminator,
        decodeFillGroupId(std.math.maxInt(u128)),
    );

    const zero_namespace = (@as(u128, 9) << identity_journal_epoch_shift) |
        (@as(u128, 42) << identity_publication_sequence_shift) |
        (@as(u128, 3) << identity_fill_ordinal_shift);
    try std.testing.expectError(
        error.InvalidIdentityNamespace,
        decodeFillGroupId(zero_namespace),
    );

    const zero_sequence = (@as(u128, 7) << identity_namespace_shift) |
        (@as(u128, 9) << identity_journal_epoch_shift) |
        (@as(u128, 3) << identity_fill_ordinal_shift);
    try std.testing.expectError(
        error.InvalidPublicationSequence,
        decodeFillGroupId(zero_sequence),
    );
}

test "TigerBeetle publication cursors compare only inside one durable domain" {
    const domain: PublicationDomain = .{ .namespace = 7, .journal_epoch = 9 };
    const same_domain: PublicationDomain = .{ .namespace = 7, .journal_epoch = 9 };
    const next_epoch: PublicationDomain = .{ .namespace = 7, .journal_epoch = 10 };
    try domain.validate();
    try std.testing.expect(domain.eql(same_domain));
    try std.testing.expect(!domain.eql(next_epoch));

    const committed: PublicationCursor = .{
        .domain = domain,
        .publication_sequence = 42,
    };
    const earlier: PublicationCursor = .{
        .domain = same_domain,
        .publication_sequence = 41,
    };
    const reused_in_next_epoch: PublicationCursor = .{
        .domain = next_epoch,
        .publication_sequence = 41,
    };
    try committed.validate();
    try std.testing.expect(committed.eql(committed));
    try std.testing.expect(committed.covers(earlier));
    try std.testing.expect(!earlier.covers(committed));
    try std.testing.expect(!committed.covers(reused_in_next_epoch));

    const recovery_zero: PublicationCursor = .{
        .domain = domain,
        .publication_sequence = 0,
    };
    try std.testing.expectError(
        error.InvalidPublicationSequence,
        recovery_zero.validate(),
    );
    try std.testing.expectError(
        error.InvalidIdentityNamespace,
        (PublicationDomain{ .namespace = 0, .journal_epoch = 9 }).validate(),
    );
}

test "TigerBeetle settlement maps taker bid with role fees and stable IDs" {
    const fill = testFill(.bid);
    const chain = try mapFill(fill);
    const items = chain.items();

    try std.testing.expectEqual(@as(usize, 4), items.len);
    try expectTransfer(items[0], fill, .quote_principal, 102, 202, 210, 15, true);
    try expectTransfer(items[1], fill, .maker_fee, 202, 301, 3, 15, true);
    try expectTransfer(items[2], fill, .taker_fee, 102, 301, 6, 15, true);
    try expectTransfer(items[3], fill, .base_principal, 201, 101, 300, 11, false);

    const expected_group = (@as(u128, 7) << 120) |
        (@as(u128, 9) << 104) |
        (@as(u128, 42) << 40) |
        (@as(u128, 3) << 8);
    try std.testing.expectEqual(expected_group, chain.group_id);
    for (items, 0..) |item, index| {
        try std.testing.expect(item.id != 0);
        try std.testing.expect(item.id != std.math.maxInt(u128));
        for (items[index + 1 ..]) |other| {
            try std.testing.expect(item.id != other.id);
        }
    }

    const replay = try mapFill(fill);
    try std.testing.expectEqualDeep(items, replay.items());
}

test "TigerBeetle settlement maps taker ask without swapping fee ownership" {
    const fill = testFill(.ask);
    const chain = try mapFill(fill);
    const items = chain.items();

    try std.testing.expectEqual(@as(usize, 4), items.len);
    try expectTransfer(items[0], fill, .quote_principal, 202, 102, 210, 15, true);
    try expectTransfer(items[1], fill, .taker_fee, 102, 301, 6, 15, true);
    try expectTransfer(items[2], fill, .maker_fee, 202, 301, 3, 15, true);
    try expectTransfer(items[3], fill, .base_principal, 101, 201, 300, 11, false);
}

test "TigerBeetle settlement omits zero and one-sided fee legs" {
    var fill = testFill(.bid);
    fill.spec.maker_fee = 0;
    fill.spec.taker_fee = 0;

    const no_fees = try mapFill(fill);
    try std.testing.expectEqual(@as(usize, 2), no_fees.items().len);
    try expectTransfer(no_fees.items()[0], fill, .quote_principal, 102, 202, 210, 15, true);
    try expectTransfer(no_fees.items()[1], fill, .base_principal, 201, 101, 300, 11, false);

    fill.spec.taker_fee = 2;
    const taker_only = try mapFill(fill);
    try std.testing.expectEqual(@as(usize, 3), taker_only.items().len);
    try expectTransfer(taker_only.items()[0], fill, .quote_principal, 102, 202, 210, 15, true);
    try expectTransfer(taker_only.items()[1], fill, .taker_fee, 102, 301, 6, 15, true);
    try expectTransfer(taker_only.items()[2], fill, .base_principal, 201, 101, 300, 11, false);
}

test "TigerBeetle settlement accepts zero price and omits zero quote principal" {
    var fill = testFill(.ask);
    fill.price = 0;
    fill.spec.maker_fee = 0;
    fill.spec.taker_fee = 0;

    const chain = try mapFill(fill);
    try std.testing.expectEqual(@as(usize, 1), chain.items().len);
    try expectTransfer(chain.items()[0], fill, .base_principal, 101, 201, 300, 11, false);
}

test "TigerBeetle settlement preserves self-trade fees without invalid principal transfers" {
    var fill = testFill(.bid);
    fill.maker_uid = fill.taker_uid;
    fill.maker_accounts = fill.taker_accounts;

    const fees = try mapFill(fill);
    try std.testing.expectEqual(@as(usize, 2), fees.items().len);
    try expectTransfer(fees.items()[0], fill, .maker_fee, 102, 301, 3, 15, true);
    try expectTransfer(fees.items()[1], fill, .taker_fee, 102, 301, 6, 15, false);

    fill.spec.maker_fee = 0;
    fill.spec.taker_fee = 0;
    const no_op = try mapFill(fill);
    try std.testing.expectEqual(@as(usize, 0), no_op.items().len);
    try std.testing.expectEqual(try fill.identity.groupId(), no_op.group_id);

    fill.maker_accounts.base_account_id = 999;
    try std.testing.expectError(error.SelfTradeAccountMismatch, mapFill(fill));
}

test "TigerBeetle settlement validates symbol fill identity ledgers accounts and codes" {
    var fill = testFill(.bid);

    fill.spec.symbol_type = .futures_contract;
    try std.testing.expectError(error.UnsupportedSymbolType, mapFill(fill));
    fill = testFill(.bid);
    fill.size = 0;
    try std.testing.expectError(error.NonPositiveSize, mapFill(fill));
    fill = testFill(.bid);
    fill.price = -1;
    try std.testing.expectError(error.NegativePrice, mapFill(fill));
    fill = testFill(.bid);
    fill.spec.base_scale_k = 0;
    try std.testing.expectError(error.NonPositiveBaseScale, mapFill(fill));
    fill = testFill(.bid);
    fill.spec.quote_scale_k = 0;
    try std.testing.expectError(error.NonPositiveQuoteScale, mapFill(fill));
    fill = testFill(.bid);
    fill.spec.maker_fee = -1;
    try std.testing.expectError(error.NegativeMakerFee, mapFill(fill));
    fill = testFill(.bid);
    fill.spec.taker_fee = -1;
    try std.testing.expectError(error.NegativeTakerFee, mapFill(fill));

    fill = testFill(.bid);
    fill.identity.namespace = 0;
    try std.testing.expectError(error.InvalidIdentityNamespace, mapFill(fill));
    fill = testFill(.bid);
    fill.identity.publication_sequence = .fromInt(0);
    try std.testing.expectError(error.InvalidPublicationSequence, mapFill(fill));

    fill = testFill(.bid);
    fill.ledgers.base = 0;
    try std.testing.expectError(error.InvalidBaseLedger, mapFill(fill));
    fill = testFill(.bid);
    fill.ledgers.quote = 0;
    try std.testing.expectError(error.InvalidQuoteLedger, mapFill(fill));
    fill = testFill(.bid);
    fill.ledgers.quote = fill.ledgers.base;
    try std.testing.expectError(error.DuplicateLedger, mapFill(fill));

    fill = testFill(.bid);
    fill.taker_accounts.base_account_id = 0;
    try std.testing.expectError(error.InvalidAccountId, mapFill(fill));
    fill = testFill(.bid);
    fill.taker_accounts.base_account_id = std.math.maxInt(u128);
    try std.testing.expectError(error.InvalidAccountId, mapFill(fill));
    fill = testFill(.bid);
    fill.maker_accounts.quote_account_id = fill.taker_accounts.quote_account_id;
    try std.testing.expectError(error.DuplicateAccountId, mapFill(fill));

    fill = testFill(.bid);
    fill.codes.quote_principal = 0;
    try std.testing.expectError(error.InvalidTransferCode, mapFill(fill));
    fill = testFill(.bid);
    fill.codes.taker_fee = fill.codes.maker_fee;
    try std.testing.expectError(error.DuplicateTransferCode, mapFill(fill));
}

test "TigerBeetle settlement rejects every signed i64 arithmetic overflow" {
    var fill = testFill(.bid);
    fill.size = std.math.maxInt(i64);
    fill.price = 0;
    fill.spec.base_scale_k = 2;
    fill.spec.maker_fee = 0;
    fill.spec.taker_fee = 0;
    try std.testing.expectError(error.ArithmeticOverflow, mapFill(fill));

    fill = testFill(.bid);
    fill.size = 2;
    fill.price = std.math.maxInt(i64);
    fill.spec.base_scale_k = 1;
    fill.spec.quote_scale_k = 1;
    fill.spec.maker_fee = 0;
    fill.spec.taker_fee = 0;
    try std.testing.expectError(error.ArithmeticOverflow, mapFill(fill));

    fill = testFill(.bid);
    fill.size = 1;
    fill.price = std.math.maxInt(i64);
    fill.spec.base_scale_k = 1;
    fill.spec.quote_scale_k = 2;
    fill.spec.maker_fee = 0;
    fill.spec.taker_fee = 0;
    try std.testing.expectError(error.ArithmeticOverflow, mapFill(fill));

    fill = testFill(.bid);
    fill.size = std.math.maxInt(i64);
    fill.price = 0;
    fill.spec.base_scale_k = 1;
    fill.spec.quote_scale_k = 1;
    fill.spec.maker_fee = 2;
    fill.spec.taker_fee = 0;
    try std.testing.expectError(error.ArithmeticOverflow, mapFill(fill));

    fill = testFill(.bid);
    fill.size = std.math.maxInt(i64);
    fill.price = 0;
    fill.spec.base_scale_k = 1;
    fill.spec.quote_scale_k = 1;
    fill.spec.maker_fee = 0;
    fill.spec.taker_fee = 2;
    try std.testing.expectError(error.ArithmeticOverflow, mapFill(fill));

    fill = testFill(.bid);
    fill.size = 1;
    fill.price = 0;
    fill.spec.base_scale_k = 1;
    fill.spec.quote_scale_k = 1;
    fill.spec.maker_fee = std.math.maxInt(i64);
    fill.spec.taker_fee = 1;
    try std.testing.expectError(error.ArithmeticOverflow, mapFill(fill));

    fill = testFill(.bid);
    fill.size = 1;
    fill.price = std.math.maxInt(i64);
    fill.spec.base_scale_k = 1;
    fill.spec.quote_scale_k = 1;
    fill.spec.maker_fee = 0;
    fill.spec.taker_fee = 1;
    try std.testing.expectError(error.ArithmeticOverflow, mapFill(fill));
}

test "TigerBeetle settlement does not invent a maker fee ordering invariant" {
    var fill = testFill(.ask);
    fill.spec.maker_fee = 5;
    fill.spec.taker_fee = 2;

    const chain = try mapFill(fill);
    try std.testing.expectEqual(@as(usize, 4), chain.items().len);
    try expectTransfer(chain.items()[1], fill, .taker_fee, 102, 301, 6, 15, true);
    try expectTransfer(chain.items()[2], fill, .maker_fee, 202, 301, 15, 15, true);
}
