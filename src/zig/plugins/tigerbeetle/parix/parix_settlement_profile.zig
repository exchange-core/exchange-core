//! Parix host integration for the opt-in TigerBeetle plugin.
const std = @import("std");
const exchange_core = @import("exchange_core");
const tigerbeetle = @import("tigerbeetle");
const config = @import("parix_settlement_profile_config.zig");

const core = exchange_core.core;
const adapter = tigerbeetle.parix_adapter;
const registry = tigerbeetle.destination_registry;
const settlement = tigerbeetle.settlement;
const Sha256 = std.crypto.hash.sha2.Sha256;

const profile_digest_domain = "exchange-core/parix-settlement-profile/v1";

pub const digest_len: usize = Sha256.digest_length;
pub const max_users: usize = config.max_users;
pub const max_initial_funding: usize = config.max_initial_funding;
pub const max_accounts: usize = 4 + (2 * max_users);

pub const treasury_account_code: u16 = 100;
pub const user_account_code: u16 = 110;
pub const fee_account_code: u16 = 210;
pub const funding_transfer_code: u16 = 10;
pub const history_flag: u16 = 8;
pub const constrained_user_flags: u16 = history_flag | 2;

pub const UuidParseError = error{
    InvalidUuidLength,
    InvalidUuidSeparator,
    InvalidUuidHex,
    InvalidDestinationId,
};

pub const ValidationError = core.common.CoreSymbolSpecification.ValidationError ||
    registry.ValidationError || error{
    InvalidPublicationNamespace,
    InvalidSymbolId,
    InvalidBaseCurrency,
    InvalidQuoteCurrency,
    DuplicateSymbolCurrency,
    InvalidCurrencyRoute,
    DuplicateCurrencyRoute,
    InvalidLedger,
    DuplicateLedger,
    InvalidAccountId,
    DuplicateAccountId,
    InvalidUserCount,
    InvalidUserId,
    DuplicateUserId,
    InvalidInitialFundingId,
    DuplicateInitialFundingId,
    DuplicateInitialFundingUser,
    InvalidInitialFundingAmount,
    UnknownFundingUser,
    UnknownFundingCurrency,
};

pub const InitError = std.mem.Allocator.Error ||
    UuidParseError ||
    ValidationError;

pub const LoadError = std.Io.Dir.ReadFileAllocError ||
    config.ParseError ||
    InitError;

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

pub const CurrencySpec = struct {
    currency: i32,
    ledger: u32,
    treasury_account_id: u128,
    fee_account_id: u128,
};

pub const UserSpec = struct {
    uid: i64,
    base_account_id: u128,
    quote_account_id: u128,
};

/// Complete TigerBeetle route for an externally initiated deposit or
/// withdrawal. This app-layer view is derived from the validated profile and
/// does not extend the persisted profile schema.
pub const FundingRoute = struct {
    destination_id: registry.DestinationId,
    ledger: u32,
    treasury_account_id: u128,
    user_account_id: u128,
};

pub const FundingRouteError = error{
    UnknownFundingUser,
    UnknownFundingCurrency,
    InvalidFundingRoute,
};

/// Successful construction guarantees both `id` and `amount` fit a positive
/// `i64`, matching the current core balance-adjustment command contract.
pub const InitialFundingSpec = struct {
    id: u128,
    uid: i64,
    currency: i32,
    amount: u128,
};

/// Heap-owned immutable, bounded single-market settlement configuration.
/// Accessor slices borrow this profile and remain valid until `deinit`.
pub const Profile = opaque {
    pub fn loadFile(
        allocator: std.mem.Allocator,
        io: std.Io,
        path: []const u8,
    ) LoadError!*Profile {
        const bytes = try std.Io.Dir.cwd().readFileAlloc(
            io,
            path,
            allocator,
            .limited(config.max_profile_bytes),
        );
        defer allocator.free(bytes);
        return parseJson(allocator, bytes);
    }

    pub fn parseJson(
        allocator: std.mem.Allocator,
        bytes: []const u8,
    ) (config.ParseError || InitError)!*Profile {
        const document = try config.parse(allocator, bytes);
        return initFromDocument(allocator, document);
    }

    pub fn developmentExample(allocator: std.mem.Allocator) InitError!*Profile {
        return initFromDocument(allocator, developmentDocument());
    }

    /// Test/development fixture for exercising validate-only startup without
    /// provisioning seed transfers. Production callers should load a file.
    pub fn developmentExampleWithoutFunding(
        allocator: std.mem.Allocator,
    ) InitError!*Profile {
        var document = developmentDocument();
        document.funding_count = 0;
        return initFromDocument(allocator, document);
    }

    pub fn deinit(self: *Profile) void {
        const impl = profileImplMut(self);
        const allocator = impl.allocator;
        impl.* = undefined;
        allocator.destroy(impl);
    }

    pub fn databaseUuid(self: *const Profile) []const u8 {
        return &profileImpl(self).database_uuid;
    }

    pub fn destinationId(self: *const Profile) registry.DestinationId {
        return profileImpl(self).destination_id;
    }

    pub fn publicationDomain(self: *const Profile) settlement.PublicationDomain {
        return profileImpl(self).publication_domain;
    }

    pub fn symbolSpecification(self: *const Profile) core.common.CoreSymbolSpecification {
        return profileImpl(self).symbol;
    }

    pub fn currencies(self: *const Profile) []const CurrencySpec {
        return &profileImpl(self).currencies;
    }

    pub fn users(self: *const Profile) []const UserSpec {
        const impl = profileImpl(self);
        return impl.users_storage[0..impl.user_count];
    }

    pub fn accounts(self: *const Profile) []const AccountSpec {
        const impl = profileImpl(self);
        return impl.account_storage[0..impl.account_count];
    }

    pub fn initialFunding(self: *const Profile) []const InitialFundingSpec {
        const impl = profileImpl(self);
        return impl.funding_storage[0..impl.funding_count];
    }

    pub fn fundingTransfers(self: *const Profile) []const settlement.Transfer {
        const impl = profileImpl(self);
        return impl.transfer_storage[0..impl.funding_count];
    }

    pub fn isConfiguredUser(self: *const Profile, uid: i64) bool {
        return findUser(self.users(), uid) != null;
    }

    /// Resolves the external asset movement route for one configured user and
    /// one of the profile's base/quote currencies.
    pub fn fundingRoute(
        self: *const Profile,
        uid: i64,
        currency: i32,
    ) FundingRouteError!FundingRoute {
        const impl = profileImpl(self);
        const user = findUser(impl.users_storage[0..impl.user_count], uid) orelse
            return error.UnknownFundingUser;
        const currency_spec = findCurrency(impl.currencies, currency) orelse
            return error.UnknownFundingCurrency;
        const user_account_id = if (currency == impl.symbol.base_currency)
            user.base_account_id
        else if (currency == impl.symbol.quote_currency)
            user.quote_account_id
        else
            return error.InvalidFundingRoute;
        if (currency_spec.ledger == 0 or
            !validExternalAccountId(currency_spec.treasury_account_id) or
            !validExternalAccountId(user_account_id))
        {
            return error.InvalidFundingRoute;
        }
        return .{
            .destination_id = impl.destination_id,
            .ledger = currency_spec.ledger,
            .treasury_account_id = currency_spec.treasury_account_id,
            .user_account_id = user_account_id,
        };
    }

    pub fn createRegistry(
        self: *const Profile,
        allocator: std.mem.Allocator,
    ) registry.RegistryInitError!*registry.DestinationRegistry {
        return createRegistryForImpl(allocator, profileImpl(self));
    }

    pub fn registryDigest(self: *const Profile) [registry.registry_digest_len]u8 {
        return profileImpl(self).registry_digest;
    }

    pub fn digest(self: *const Profile) [digest_len]u8 {
        return profileImpl(self).profile_digest;
    }

    pub fn fundingChain(self: *const Profile) settlement.TransferChain {
        const impl = profileImpl(self);
        var chain: settlement.TransferChain = .{
            .group_id = if (impl.funding_count == 0) 0 else impl.transfer_storage[0].id,
        };
        for (impl.transfer_storage[0..impl.funding_count], 0..) |transfer, index| {
            chain.storage[index] = transfer;
        }
        chain.count = @intCast(impl.funding_count);
        return chain;
    }

    pub fn writeCreateAccounts(
        self: *const Profile,
        writer: *std.Io.Writer,
    ) std.Io.Writer.Error!void {
        try writer.writeByte('[');
        for (self.accounts(), 0..) |account, index| {
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

    pub fn writeLookupAccounts(
        self: *const Profile,
        writer: *std.Io.Writer,
    ) std.Io.Writer.Error!void {
        try writer.writeByte('[');
        for (self.accounts(), 0..) |account, index| {
            if (index != 0) try writer.writeByte(',');
            try writer.print("\"{d}\"", .{account.id});
        }
        try writer.writeByte(']');
    }

    pub fn writeCreateFundingTransfers(
        self: *const Profile,
        writer: *std.Io.Writer,
    ) adapter.EncodeError!void {
        if (self.initialFunding().len == 0) return writer.writeAll("[]");
        const chain = self.fundingChain();
        try adapter.writeCreateTransfers(writer, &chain);
    }

    pub fn writeLookupFundingTransfers(
        self: *const Profile,
        writer: *std.Io.Writer,
    ) adapter.EncodeError!void {
        if (self.initialFunding().len == 0) return writer.writeAll("[]");
        const chain = self.fundingChain();
        try adapter.writeLookupTransfers(writer, &chain);
    }
};

const ProfileImpl = struct {
    allocator: std.mem.Allocator,
    database_uuid: [config.max_database_uuid_bytes]u8,
    destination_id: registry.DestinationId,
    publication_domain: settlement.PublicationDomain,
    symbol: core.common.CoreSymbolSpecification,
    currencies: [2]CurrencySpec,
    users_storage: [max_users]UserSpec = undefined,
    user_count: usize,
    account_storage: [max_accounts]AccountSpec = undefined,
    account_count: usize,
    funding_storage: [max_initial_funding]InitialFundingSpec = undefined,
    transfer_storage: [max_initial_funding]settlement.Transfer = undefined,
    funding_count: usize,
    registry_digest: [registry.registry_digest_len]u8,
    profile_digest: [digest_len]u8,
};

const development_database_uuid = "0e46f90e-c38b-487a-b9f9-630f7839cc3f";

fn developmentDocument() config.Document {
    var database_uuid: [config.max_database_uuid_bytes]u8 = undefined;
    @memcpy(&database_uuid, development_database_uuid);
    var document: config.Document = .{
        .version = config.format_version,
        .database_uuid = database_uuid,
        .publication = .{ .namespace = 1, .journal_epoch = 0 },
        .symbol = .{
            .symbol_id = 241,
            .symbol_type = .currency_exchange_pair,
            .base_currency = 11,
            .quote_currency = 15,
            .base_scale_k = 100,
            .quote_scale_k = 10,
            .taker_fee = 2,
            .maker_fee = 1,
            .margin_buy = 0,
            .margin_sell = 0,
        },
        .currencies = .{
            .{
                .currency = 11,
                .ledger = 421,
                .treasury_account_id = 420_011,
                .fee_account_id = 42_011,
            },
            .{
                .currency = 15,
                .ledger = 425,
                .treasury_account_id = 420_015,
                .fee_account_id = 42_015,
            },
        },
        .user_count = 2,
        .funding_count = 2,
    };
    document.users_storage[0] = .{
        .uid = 1001,
        .base_account_id = 421_011,
        .quote_account_id = 421_015,
    };
    document.users_storage[1] = .{
        .uid = 1002,
        .base_account_id = 422_011,
        .quote_account_id = 422_015,
    };
    document.funding_storage[0] = .{
        .id = 8_421_011,
        .uid = 1001,
        .currency = 11,
        .amount = 1_000,
    };
    document.funding_storage[1] = .{
        .id = 8_422_015,
        .uid = 1002,
        .currency = 15,
        .amount = 2_000,
    };
    return document;
}

fn initFromDocument(
    allocator: std.mem.Allocator,
    document: config.Document,
) InitError!*Profile {
    const destination_id = try parseUuidDestination(&document.database_uuid);
    const publication_domain: settlement.PublicationDomain = .{
        .namespace = document.publication.namespace,
        .journal_epoch = document.publication.journal_epoch,
    };
    publication_domain.validate() catch return error.InvalidPublicationNamespace;

    if (document.symbol.symbol_id <= 0) return error.InvalidSymbolId;
    if (document.symbol.base_currency <= 0) return error.InvalidBaseCurrency;
    if (document.symbol.quote_currency <= 0) return error.InvalidQuoteCurrency;
    if (document.symbol.base_currency == document.symbol.quote_currency) {
        return error.DuplicateSymbolCurrency;
    }
    const symbol = try core.common.CoreSymbolSpecification.init(.{
        .symbol_id = document.symbol.symbol_id,
        .symbol_type = switch (document.symbol.symbol_type) {
            .currency_exchange_pair => .currency_exchange_pair,
        },
        .base_currency = document.symbol.base_currency,
        .quote_currency = document.symbol.quote_currency,
        .base_scale_k = document.symbol.base_scale_k,
        .quote_scale_k = document.symbol.quote_scale_k,
        .taker_fee = document.symbol.taker_fee,
        .maker_fee = document.symbol.maker_fee,
        .margin_buy = document.symbol.margin_buy,
        .margin_sell = document.symbol.margin_sell,
    });

    var currencies: [2]CurrencySpec = undefined;
    for (document.currencies, 0..) |currency, index| {
        currencies[index] = .{
            .currency = currency.currency,
            .ledger = currency.ledger,
            .treasury_account_id = currency.treasury_account_id,
            .fee_account_id = currency.fee_account_id,
        };
    }
    std.sort.heap(CurrencySpec, &currencies, {}, currencyLessThan);
    try validateCurrencies(currencies, symbol);

    if (document.user_count == 0 or document.user_count > max_users) {
        return error.InvalidUserCount;
    }
    var users_storage: [max_users]UserSpec = undefined;
    for (document.users(), 0..) |user, index| {
        users_storage[index] = .{
            .uid = user.uid,
            .base_account_id = user.base_account_id,
            .quote_account_id = user.quote_account_id,
        };
    }
    const users = users_storage[0..document.user_count];
    std.sort.heap(UserSpec, users, {}, userLessThan);
    try validateUsers(users);

    var funding_storage: [max_initial_funding]InitialFundingSpec = undefined;
    for (document.initialFunding(), 0..) |funding, index| {
        funding_storage[index] = .{
            .id = funding.id,
            .uid = funding.uid,
            .currency = funding.currency,
            .amount = funding.amount,
        };
    }
    const funding = funding_storage[0..document.funding_count];
    std.sort.heap(InitialFundingSpec, funding, {}, fundingLessThan);
    try validateFunding(funding, users, symbol);

    const impl = try allocator.create(ProfileImpl);
    errdefer allocator.destroy(impl);
    impl.* = .{
        .allocator = allocator,
        .database_uuid = undefined,
        .destination_id = destination_id,
        .publication_domain = publication_domain,
        .symbol = symbol,
        .currencies = currencies,
        .users_storage = users_storage,
        .user_count = document.user_count,
        .account_count = 0,
        .funding_storage = funding_storage,
        .funding_count = document.funding_count,
        .registry_digest = undefined,
        .profile_digest = undefined,
    };

    var uuid_writer = std.Io.Writer.fixed(&impl.database_uuid);
    destinationToUuid(&uuid_writer, destination_id) catch unreachable;
    std.debug.assert(uuid_writer.buffered().len == impl.database_uuid.len);

    try buildAccounts(impl);
    buildFundingTransfers(impl);

    const destination_registry = try createRegistryForImpl(allocator, impl);
    defer destination_registry.deinit();
    impl.registry_digest = destination_registry.digest();
    impl.profile_digest = calculateProfileDigest(impl);
    return profileToken(impl);
}

fn validateCurrencies(
    currencies: [2]CurrencySpec,
    symbol: core.common.CoreSymbolSpecification,
) ValidationError!void {
    if (currencies[0].currency == currencies[1].currency) {
        return error.DuplicateCurrencyRoute;
    }
    var saw_base = false;
    var saw_quote = false;
    for (currencies) |currency| {
        if (currency.currency <= 0) return error.InvalidCurrencyRoute;
        if (currency.ledger == 0) return error.InvalidLedger;
        if (!validExternalAccountId(currency.treasury_account_id) or
            !validExternalAccountId(currency.fee_account_id))
        {
            return error.InvalidAccountId;
        }
        if (currency.treasury_account_id == currency.fee_account_id) {
            return error.DuplicateAccountId;
        }
        if (currency.currency == symbol.base_currency) saw_base = true;
        if (currency.currency == symbol.quote_currency) saw_quote = true;
    }
    if (!saw_base or !saw_quote) return error.InvalidCurrencyRoute;
    if (currencies[0].ledger == currencies[1].ledger) return error.DuplicateLedger;
    if (currencies[0].treasury_account_id == currencies[1].treasury_account_id or
        currencies[0].treasury_account_id == currencies[1].fee_account_id or
        currencies[0].fee_account_id == currencies[1].treasury_account_id or
        currencies[0].fee_account_id == currencies[1].fee_account_id)
    {
        return error.DuplicateAccountId;
    }
}

fn validateUsers(users: []const UserSpec) ValidationError!void {
    for (users, 0..) |user, index| {
        if (user.uid <= 0) return error.InvalidUserId;
        if (!validExternalAccountId(user.base_account_id) or
            !validExternalAccountId(user.quote_account_id))
        {
            return error.InvalidAccountId;
        }
        if (user.base_account_id == user.quote_account_id) {
            return error.DuplicateAccountId;
        }
        if (index > 0 and users[index - 1].uid == user.uid) {
            return error.DuplicateUserId;
        }
        for (users[0..index]) |previous| {
            if (user.base_account_id == previous.base_account_id or
                user.base_account_id == previous.quote_account_id or
                user.quote_account_id == previous.base_account_id or
                user.quote_account_id == previous.quote_account_id)
            {
                return error.DuplicateAccountId;
            }
        }
    }
}

fn validateFunding(
    funding: []const InitialFundingSpec,
    users: []const UserSpec,
    symbol: core.common.CoreSymbolSpecification,
) ValidationError!void {
    for (funding, 0..) |entry, index| {
        if (entry.id == 0 or entry.id > std.math.maxInt(i64)) {
            return error.InvalidInitialFundingId;
        }
        if (entry.amount == 0 or entry.amount > std.math.maxInt(i64)) {
            return error.InvalidInitialFundingAmount;
        }
        if (index > 0 and funding[index - 1].id == entry.id) {
            return error.DuplicateInitialFundingId;
        }
        for (funding[0..index]) |previous| {
            // Risk keeps one monotonic adjustment counter per user. More than
            // one seed for a UID cannot be proven idempotent on restart once
            // the later transaction ID has committed.
            if (previous.uid == entry.uid) {
                return error.DuplicateInitialFundingUser;
            }
        }
        if (findUser(users, entry.uid) == null) return error.UnknownFundingUser;
        if (entry.currency != symbol.base_currency and
            entry.currency != symbol.quote_currency)
        {
            return error.UnknownFundingCurrency;
        }
    }
}

fn buildAccounts(impl: *ProfileImpl) ValidationError!void {
    var count: usize = 0;
    for (impl.currencies) |currency| {
        impl.account_storage[count] = .{
            .id = currency.treasury_account_id,
            .ledger = currency.ledger,
            .code = treasury_account_code,
            .flags = history_flag,
        };
        count += 1;
        impl.account_storage[count] = .{
            .id = currency.fee_account_id,
            .ledger = currency.ledger,
            .code = fee_account_code,
            .flags = history_flag,
        };
        count += 1;
    }
    for (impl.users_storage[0..impl.user_count]) |user| {
        impl.account_storage[count] = userAccount(
            user.base_account_id,
            user.uid,
            impl.symbol.base_currency,
            currencyFor(impl.currencies, impl.symbol.base_currency).ledger,
        );
        count += 1;
        impl.account_storage[count] = userAccount(
            user.quote_account_id,
            user.uid,
            impl.symbol.quote_currency,
            currencyFor(impl.currencies, impl.symbol.quote_currency).ledger,
        );
        count += 1;
    }
    impl.account_count = count;
    const accounts = impl.account_storage[0..count];
    std.sort.heap(AccountSpec, accounts, {}, accountLessThan);
    for (accounts, 0..) |account, index| {
        if (index > 0 and accounts[index - 1].id == account.id) {
            return error.DuplicateAccountId;
        }
    }
}

fn buildFundingTransfers(impl: *ProfileImpl) void {
    for (impl.funding_storage[0..impl.funding_count], 0..) |funding, index| {
        const currency = currencyFor(impl.currencies, funding.currency);
        const user = findUser(impl.users_storage[0..impl.user_count], funding.uid).?;
        const credit_account_id = if (funding.currency == impl.symbol.base_currency)
            user.base_account_id
        else
            user.quote_account_id;
        impl.transfer_storage[index] = .{
            .id = funding.id,
            .debit_account_id = currency.treasury_account_id,
            .credit_account_id = credit_account_id,
            .amount = funding.amount,
            .user_data_128 = 0,
            .ledger = currency.ledger,
            .code = funding_transfer_code,
        };
    }
}

fn createRegistryForImpl(
    allocator: std.mem.Allocator,
    impl: *const ProfileImpl,
) registry.RegistryInitError!*registry.DestinationRegistry {
    const destinations = [_]registry.DestinationSpec{.{
        .id = impl.destination_id,
        .codes = .{},
    }};
    const symbols = [_]registry.SymbolRoute{.{
        .symbol_id = impl.symbol.symbol_id,
        .base_currency = impl.symbol.base_currency,
        .quote_currency = impl.symbol.quote_currency,
        .destination_id = impl.destination_id,
    }};
    var currencies: [2]registry.CurrencyRoute = undefined;
    for (impl.currencies, 0..) |currency, index| {
        currencies[index] = .{
            .destination_id = impl.destination_id,
            .currency = currency.currency,
            .ledger = currency.ledger,
            .fee_account_id = currency.fee_account_id,
        };
    }
    var routes: [2 * max_users]registry.AccountRoute = undefined;
    var route_count: usize = 0;
    for (impl.users_storage[0..impl.user_count]) |user| {
        routes[route_count] = .{
            .destination_id = impl.destination_id,
            .uid = user.uid,
            .currency = impl.symbol.base_currency,
            .account_id = user.base_account_id,
        };
        route_count += 1;
        routes[route_count] = .{
            .destination_id = impl.destination_id,
            .uid = user.uid,
            .currency = impl.symbol.quote_currency,
            .account_id = user.quote_account_id,
        };
        route_count += 1;
    }
    return registry.DestinationRegistry.initValidated(allocator, .{
        .destinations = &destinations,
        .symbols = &symbols,
        .currencies = &currencies,
        .accounts = routes[0..route_count],
    });
}

fn calculateProfileDigest(impl: *const ProfileImpl) [digest_len]u8 {
    var hasher = Sha256.init(.{});
    hasher.update(profile_digest_domain);
    hashInt(u16, &hasher, config.format_version);
    hashInt(u128, &hasher, @intFromEnum(impl.destination_id));
    hashInt(u8, &hasher, impl.publication_domain.namespace);
    hashInt(u16, &hasher, impl.publication_domain.journal_epoch);

    hashInt(i32, &hasher, impl.symbol.symbol_id);
    hashInt(i8, &hasher, impl.symbol.symbol_type.code());
    hashInt(i32, &hasher, impl.symbol.base_currency);
    hashInt(i32, &hasher, impl.symbol.quote_currency);
    hashInt(i64, &hasher, impl.symbol.base_scale_k);
    hashInt(i64, &hasher, impl.symbol.quote_scale_k);
    hashInt(i64, &hasher, impl.symbol.taker_fee);
    hashInt(i64, &hasher, impl.symbol.maker_fee);
    hashInt(i64, &hasher, impl.symbol.margin_buy);
    hashInt(i64, &hasher, impl.symbol.margin_sell);

    hashInt(u16, &hasher, treasury_account_code);
    hashInt(u16, &hasher, user_account_code);
    hashInt(u16, &hasher, fee_account_code);
    hashInt(u16, &hasher, funding_transfer_code);
    hashInt(u16, &hasher, history_flag);
    hashInt(u16, &hasher, constrained_user_flags);
    const settlement_codes: settlement.SettlementCodes = .{};
    hashInt(u16, &hasher, settlement_codes.quote_principal);
    hashInt(u16, &hasher, settlement_codes.base_principal);
    hashInt(u16, &hasher, settlement_codes.maker_fee);
    hashInt(u16, &hasher, settlement_codes.taker_fee);

    hashInt(u8, &hasher, @intCast(impl.currencies.len));
    for (impl.currencies) |currency| {
        hashInt(i32, &hasher, currency.currency);
        hashInt(u32, &hasher, currency.ledger);
        hashInt(u128, &hasher, currency.treasury_account_id);
        hashInt(u128, &hasher, currency.fee_account_id);
    }
    hashInt(u8, &hasher, @intCast(impl.user_count));
    for (impl.users_storage[0..impl.user_count]) |user| {
        hashInt(i64, &hasher, user.uid);
        hashInt(u128, &hasher, user.base_account_id);
        hashInt(u128, &hasher, user.quote_account_id);
    }
    hashInt(u8, &hasher, @intCast(impl.funding_count));
    for (impl.funding_storage[0..impl.funding_count]) |funding| {
        hashInt(u128, &hasher, funding.id);
        hashInt(i64, &hasher, funding.uid);
        hashInt(i32, &hasher, funding.currency);
        hashInt(u128, &hasher, funding.amount);
    }
    hashInt(u8, &hasher, @intCast(impl.account_count));
    for (impl.account_storage[0..impl.account_count]) |account| {
        hashInt(u128, &hasher, account.id);
        hashInt(u128, &hasher, account.user_data_128);
        hashInt(u64, &hasher, account.user_data_64);
        hashInt(u32, &hasher, account.user_data_32);
        hashInt(u32, &hasher, account.ledger);
        hashInt(u16, &hasher, account.code);
        hashInt(u16, &hasher, account.flags);
    }
    hashInt(u8, &hasher, @intCast(impl.funding_count));
    for (impl.transfer_storage[0..impl.funding_count]) |transfer| {
        hashInt(u128, &hasher, transfer.id);
        hashInt(u128, &hasher, transfer.debit_account_id);
        hashInt(u128, &hasher, transfer.credit_account_id);
        hashInt(u128, &hasher, transfer.amount);
        hashInt(u128, &hasher, transfer.pending_id);
        hashInt(u128, &hasher, transfer.user_data_128);
        hashInt(u64, &hasher, transfer.user_data_64);
        hashInt(u32, &hasher, transfer.user_data_32);
        hashInt(u32, &hasher, transfer.timeout);
        hashInt(u32, &hasher, transfer.ledger);
        hashInt(u16, &hasher, transfer.code);
        hashInt(u16, &hasher, transfer.flags);
        hashInt(u64, &hasher, transfer.timestamp);
    }
    hasher.update(&impl.registry_digest);

    var result: [digest_len]u8 = undefined;
    hasher.final(&result);
    return result;
}

fn hashInt(comptime T: type, hasher: *Sha256, value: T) void {
    const bits = @typeInfo(T).int.bits;
    const U = std.meta.Int(.unsigned, bits);
    const unsigned: U = @bitCast(value);
    const little = std.mem.nativeToLittle(U, unsigned);
    hasher.update(std.mem.asBytes(&little));
}

fn userAccount(id: u128, uid: i64, currency: i32, ledger: u32) AccountSpec {
    return .{
        .id = id,
        .user_data_64 = @intCast(uid),
        .user_data_32 = @intCast(currency),
        .ledger = ledger,
        .code = user_account_code,
        .flags = constrained_user_flags,
    };
}

fn currencyFor(currencies: [2]CurrencySpec, currency_id: i32) CurrencySpec {
    return findCurrency(currencies, currency_id) orelse unreachable;
}

fn findCurrency(currencies: [2]CurrencySpec, currency_id: i32) ?CurrencySpec {
    for (currencies) |currency| {
        if (currency.currency == currency_id) return currency;
    }
    return null;
}

fn findUser(users: []const UserSpec, uid: i64) ?UserSpec {
    for (users) |user| {
        if (user.uid == uid) return user;
    }
    return null;
}

fn currencyLessThan(_: void, lhs: CurrencySpec, rhs: CurrencySpec) bool {
    return lhs.currency < rhs.currency;
}

fn userLessThan(_: void, lhs: UserSpec, rhs: UserSpec) bool {
    return lhs.uid < rhs.uid;
}

fn fundingLessThan(_: void, lhs: InitialFundingSpec, rhs: InitialFundingSpec) bool {
    return lhs.id < rhs.id;
}

fn accountLessThan(_: void, lhs: AccountSpec, rhs: AccountSpec) bool {
    return lhs.id < rhs.id;
}

fn validExternalAccountId(id: u128) bool {
    return id != 0 and id != std.math.maxInt(u128);
}

fn profileToken(impl: *ProfileImpl) *Profile {
    return @ptrCast(impl);
}

fn profileImpl(profile: *const Profile) *const ProfileImpl {
    return @ptrCast(@alignCast(profile));
}

fn profileImplMut(profile: *Profile) *ProfileImpl {
    return @ptrCast(@alignCast(profile));
}

pub fn parseUuidDestination(raw: []const u8) UuidParseError!registry.DestinationId {
    if (raw.len != config.max_database_uuid_bytes) return error.InvalidUuidLength;
    const separator_indices = [_]usize{ 8, 13, 18, 23 };
    for (separator_indices) |index| {
        if (raw[index] != '-') return error.InvalidUuidSeparator;
    }

    var value: u128 = 0;
    for (raw, 0..) |byte, index| {
        if (index == 8 or index == 13 or index == 18 or index == 23) continue;
        value = (value << 4) | try hexNibble(byte);
    }
    if (value == 0) return error.InvalidDestinationId;
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

fn hexNibble(byte: u8) UuidParseError!u128 {
    return switch (byte) {
        '0'...'9' => byte - '0',
        'a'...'f' => byte - 'a' + 10,
        'A'...'F' => byte - 'A' + 10,
        else => error.InvalidUuidHex,
    };
}

test "development profile exposes canonical dynamic routing and writers" {
    const profile = try Profile.developmentExample(std.testing.allocator);
    defer profile.deinit();

    try std.testing.expectEqualStrings(development_database_uuid, profile.databaseUuid());
    try std.testing.expectEqual(@as(usize, 2), profile.users().len);
    try std.testing.expectEqual(@as(usize, 8), profile.accounts().len);
    try std.testing.expectEqual(@as(usize, 2), profile.initialFunding().len);
    try std.testing.expect(profile.isConfiguredUser(1001));
    try std.testing.expect(!profile.isConfiguredUser(1003));

    const destination_registry = try profile.createRegistry(std.testing.allocator);
    defer destination_registry.deinit();
    try std.testing.expectEqualSlices(u8, &profile.registryDigest(), &destination_registry.digest());
    const resolved = try destination_registry.resolve(
        profile.symbolSpecification(),
        1002,
        1001,
    );
    try std.testing.expectEqual(@as(u128, 422_011), resolved.taker_accounts.base_account_id);
    try std.testing.expectEqual(@as(u128, 421_015), resolved.maker_accounts.quote_account_id);

    var lookup_buffer: [512]u8 = undefined;
    var lookup_writer = std.Io.Writer.fixed(&lookup_buffer);
    try profile.writeLookupAccounts(&lookup_writer);
    try std.testing.expectEqualStrings(
        "[\"42011\",\"42015\",\"420011\",\"420015\",\"421011\",\"421015\",\"422011\",\"422015\"]",
        lookup_writer.buffered(),
    );
}

test "funding routes resolve base and quote accounts without digest drift" {
    const profile = try Profile.developmentExample(std.testing.allocator);
    defer profile.deinit();
    const digest_before = profile.digest();

    const base = try profile.fundingRoute(1001, 11);
    try std.testing.expectEqual(profile.destinationId(), base.destination_id);
    try std.testing.expectEqual(@as(u32, 421), base.ledger);
    try std.testing.expectEqual(@as(u128, 420_011), base.treasury_account_id);
    try std.testing.expectEqual(@as(u128, 421_011), base.user_account_id);

    const quote = try profile.fundingRoute(1002, 15);
    try std.testing.expectEqual(profile.destinationId(), quote.destination_id);
    try std.testing.expectEqual(@as(u32, 425), quote.ledger);
    try std.testing.expectEqual(@as(u128, 420_015), quote.treasury_account_id);
    try std.testing.expectEqual(@as(u128, 422_015), quote.user_account_id);

    try std.testing.expectEqualSlices(u8, &digest_before, &profile.digest());
}

test "funding routes reject unknown users and currencies" {
    const profile = try Profile.developmentExample(std.testing.allocator);
    defer profile.deinit();

    try std.testing.expectError(
        error.UnknownFundingUser,
        profile.fundingRoute(9_999, 11),
    );
    try std.testing.expectError(
        error.UnknownFundingCurrency,
        profile.fundingRoute(1001, 99),
    );
}

test "checked-in example profile matches the development fixture" {
    const from_file = try Profile.loadFile(
        std.testing.allocator,
        std.testing.io,
        "src/zig/fixtures/parix-settlement-profile.example.json",
    );
    defer from_file.deinit();
    const built_in = try Profile.developmentExample(std.testing.allocator);
    defer built_in.deinit();

    try std.testing.expectEqualStrings(
        built_in.databaseUuid(),
        from_file.databaseUuid(),
    );
    try std.testing.expectEqualSlices(u8, &built_in.digest(), &from_file.digest());
}

test "JSON profile digest ignores array ordering" {
    const ordered = development_example_json;
    const reordered =
        \\{
        \\  "version": 1,
        \\  "database_uuid": "0E46F90E-C38B-487A-B9F9-630F7839CC3F",
        \\  "publication": {"namespace": 1, "journal_epoch": 0},
        \\  "symbol": {"symbol_id": 241, "symbol_type": "currency_exchange_pair", "base_currency": 11, "quote_currency": 15, "base_scale_k": 100, "quote_scale_k": 10, "taker_fee": 2, "maker_fee": 1, "margin_buy": 0, "margin_sell": 0},
        \\  "currencies": [{"currency": 15, "ledger": 425, "treasury_account_id": "420015", "fee_account_id": "42015"}, {"currency": 11, "ledger": 421, "treasury_account_id": "420011", "fee_account_id": "42011"}],
        \\  "users": [{"uid": 1002, "base_account_id": "422011", "quote_account_id": "422015"}, {"uid": 1001, "base_account_id": "421011", "quote_account_id": "421015"}],
        \\  "initial_funding": [{"id": "8422015", "uid": 1002, "currency": 15, "amount": "2000"}, {"id": "8421011", "uid": 1001, "currency": 11, "amount": "1000"}]
        \\}
    ;
    const first = try Profile.parseJson(std.testing.allocator, ordered);
    defer first.deinit();
    const second = try Profile.parseJson(std.testing.allocator, reordered);
    defer second.deinit();
    try std.testing.expectEqualSlices(u8, &first.digest(), &second.digest());
}

test "profile digest changes for economic and operational drift" {
    const original = development_example_json;
    const economic = try replaceOnceAlloc(std.testing.allocator, original, "\"taker_fee\": 2", "\"taker_fee\": 3");
    defer std.testing.allocator.free(economic);
    const operational = try replaceOnceAlloc(std.testing.allocator, original, "\"namespace\": 1", "\"namespace\": 2");
    defer std.testing.allocator.free(operational);
    const account_route = try replaceOnceAlloc(std.testing.allocator, original, "\"base_account_id\": \"421011\"", "\"base_account_id\": \"421012\"");
    defer std.testing.allocator.free(account_route);
    const funding_amount = try replaceOnceAlloc(std.testing.allocator, original, "\"amount\": \"1000\"", "\"amount\": \"1001\"");
    defer std.testing.allocator.free(funding_amount);

    const baseline = try Profile.parseJson(std.testing.allocator, original);
    defer baseline.deinit();
    const changed_economic = try Profile.parseJson(std.testing.allocator, economic);
    defer changed_economic.deinit();
    const changed_operational = try Profile.parseJson(std.testing.allocator, operational);
    defer changed_operational.deinit();
    const changed_account = try Profile.parseJson(std.testing.allocator, account_route);
    defer changed_account.deinit();
    const changed_funding = try Profile.parseJson(std.testing.allocator, funding_amount);
    defer changed_funding.deinit();
    try std.testing.expect(!std.mem.eql(u8, &baseline.digest(), &changed_economic.digest()));
    try std.testing.expect(!std.mem.eql(u8, &baseline.digest(), &changed_operational.digest()));
    try std.testing.expect(!std.mem.eql(u8, &baseline.digest(), &changed_account.digest()));
    try std.testing.expect(!std.mem.eql(u8, &baseline.digest(), &changed_funding.digest()));
    try std.testing.expectEqualSlices(
        u8,
        &baseline.registryDigest(),
        &changed_economic.registryDigest(),
    );
    try std.testing.expect(!std.mem.eql(
        u8,
        &baseline.registryDigest(),
        &changed_account.registryDigest(),
    ));
    try std.testing.expectEqualSlices(
        u8,
        &baseline.registryDigest(),
        &changed_funding.registryDigest(),
    );
}

test "profile rejects duplicates cross-reference errors and unsafe funding IDs" {
    const duplicate_users =
        \\{"version":1,"database_uuid":"0e46f90e-c38b-487a-b9f9-630f7839cc3f","publication":{"namespace":1,"journal_epoch":0},"symbol":{"symbol_id":241,"symbol_type":"currency_exchange_pair","base_currency":11,"quote_currency":15,"base_scale_k":100,"quote_scale_k":10,"taker_fee":2,"maker_fee":1,"margin_buy":0,"margin_sell":0},"currencies":[{"currency":11,"ledger":421,"treasury_account_id":"420011","fee_account_id":"42011"},{"currency":15,"ledger":425,"treasury_account_id":"420015","fee_account_id":"42015"}],"users":[{"uid":1001,"base_account_id":"421011","quote_account_id":"421015"},{"uid":1001,"base_account_id":"422011","quote_account_id":"422015"}],"initial_funding":[]}
    ;
    try expectProfileError(
        error.DuplicateUserId,
        duplicate_users,
    );

    const cases = [_]struct {
        expected: anyerror,
        needle: []const u8,
        replacement: []const u8,
    }{
        .{
            .expected = error.DuplicateAccountId,
            .needle = "\"base_account_id\": \"421011\"",
            .replacement = "\"base_account_id\": \"420011\"",
        },
        .{
            .expected = error.UnknownFundingUser,
            .needle = "\"id\": \"8421011\", \"uid\": 1001",
            .replacement = "\"id\": \"8421011\", \"uid\": 9999",
        },
        .{
            .expected = error.UnknownFundingCurrency,
            .needle = "\"uid\": 1001, \"currency\": 11, \"amount\": \"1000\"",
            .replacement = "\"uid\": 1001, \"currency\": 99, \"amount\": \"1000\"",
        },
        .{
            .expected = error.DuplicateInitialFundingUser,
            .needle = "\"id\": \"8422015\", \"uid\": 1002",
            .replacement = "\"id\": \"8422015\", \"uid\": 1001",
        },
        .{
            .expected = error.InvalidInitialFundingId,
            .needle = "\"id\": \"8421011\"",
            .replacement = "\"id\": \"9223372036854775808\"",
        },
        .{
            .expected = error.InvalidInitialFundingAmount,
            .needle = "\"amount\": \"1000\"",
            .replacement = "\"amount\": \"9223372036854775808\"",
        },
        .{
            .expected = error.InvalidInitialFundingAmount,
            .needle = "\"amount\": \"1000\"",
            .replacement = "\"amount\": \"0\"",
        },
    };
    for (cases) |case| {
        const changed = try replaceOnceAlloc(
            std.testing.allocator,
            development_example_json,
            case.needle,
            case.replacement,
        );
        defer std.testing.allocator.free(changed);
        try expectProfileError(case.expected, changed);
    }
}

test "empty funding profile emits canonical empty arrays" {
    const profile = try Profile.developmentExampleWithoutFunding(std.testing.allocator);
    defer profile.deinit();
    try std.testing.expectEqual(@as(usize, 0), profile.initialFunding().len);

    var create_buffer: [8]u8 = undefined;
    var create_writer = std.Io.Writer.fixed(&create_buffer);
    try profile.writeCreateFundingTransfers(&create_writer);
    try std.testing.expectEqualStrings("[]", create_writer.buffered());

    var lookup_buffer: [8]u8 = undefined;
    var lookup_writer = std.Io.Writer.fixed(&lookup_buffer);
    try profile.writeLookupFundingTransfers(&lookup_writer);
    try std.testing.expectEqualStrings("[]", lookup_writer.buffered());
}

fn expectProfileError(expected: anyerror, json: []const u8) !void {
    const result = Profile.parseJson(std.testing.allocator, json);
    if (result) |profile| {
        profile.deinit();
        return error.TestUnexpectedResult;
    } else |actual| {
        try std.testing.expectEqual(expected, actual);
    }
}

fn replaceOnceAlloc(
    allocator: std.mem.Allocator,
    input: []const u8,
    needle: []const u8,
    replacement: []const u8,
) std.mem.Allocator.Error![]u8 {
    const index = std.mem.indexOf(u8, input, needle) orelse unreachable;
    const output = try allocator.alloc(u8, input.len - needle.len + replacement.len);
    @memcpy(output[0..index], input[0..index]);
    @memcpy(output[index .. index + replacement.len], replacement);
    @memcpy(output[index + replacement.len ..], input[index + needle.len ..]);
    return output;
}

const development_example_json =
    \\{
    \\  "version": 1,
    \\  "database_uuid": "0e46f90e-c38b-487a-b9f9-630f7839cc3f",
    \\  "publication": {"namespace": 1, "journal_epoch": 0},
    \\  "symbol": {"symbol_id": 241, "symbol_type": "currency_exchange_pair", "base_currency": 11, "quote_currency": 15, "base_scale_k": 100, "quote_scale_k": 10, "taker_fee": 2, "maker_fee": 1, "margin_buy": 0, "margin_sell": 0},
    \\  "currencies": [{"currency": 11, "ledger": 421, "treasury_account_id": "420011", "fee_account_id": "42011"}, {"currency": 15, "ledger": 425, "treasury_account_id": "420015", "fee_account_id": "42015"}],
    \\  "users": [{"uid": 1001, "base_account_id": "421011", "quote_account_id": "421015"}, {"uid": 1002, "base_account_id": "422011", "quote_account_id": "422015"}],
    \\  "initial_funding": [{"id": "8421011", "uid": 1001, "currency": 11, "amount": "1000"}, {"id": "8422015", "uid": 1002, "currency": 15, "amount": "2000"}]
    \\}
;
