const std = @import("std");
const common = @import("exchange_core").core.common;
const outbox = @import("tigerbeetle_outbox.zig");
const settlement = @import("tigerbeetle_settlement.zig");

const Sha256 = std.crypto.hash.sha2.Sha256;
const digest_domain = "exchange-core/tigerbeetle-destination-registry/v1";

pub const DestinationId = outbox.DestinationId;
pub const registry_digest_len: usize = Sha256.digest_length;

/// Stable, non-secret metadata for one external ledger destination. Network
/// addresses and credentials intentionally remain in the transport adapter.
pub const DestinationSpec = struct {
    id: DestinationId,
    codes: settlement.SettlementCodes = .{},
};

/// Binds one exchange symbol identity and expected currency pair to a durable
/// destination. Requiring the currencies to match at resolution prevents a
/// reused symbol ID from silently selecting stale routing.
pub const SymbolRoute = struct {
    symbol_id: i32,
    base_currency: i32,
    quote_currency: i32,
    destination_id: DestinationId,
};

/// Maps one core currency to its TigerBeetle ledger and fee-revenue account.
/// Account IDs are scoped by destination because independent databases may use
/// the same numeric IDs.
pub const CurrencyRoute = struct {
    destination_id: DestinationId,
    currency: i32,
    ledger: u32,
    fee_account_id: u128,
};

/// Maps one exchange user/currency balance to one external account.
pub const AccountRoute = struct {
    destination_id: DestinationId,
    uid: i64,
    currency: i32,
    account_id: u128,
};

/// Borrowed only for `DestinationRegistry.initValidated`; all four slices are
/// copied before the immutable registry is published.
pub const RegistryInput = struct {
    destinations: []const DestinationSpec = &.{},
    symbols: []const SymbolRoute = &.{},
    currencies: []const CurrencyRoute = &.{},
    accounts: []const AccountRoute = &.{},
};

pub const RegistryCounts = struct {
    destinations: usize,
    symbols: usize,
    currencies: usize,
    accounts: usize,
};

/// Exact mapper inputs selected by routing. `FillIdentity` is deliberately not
/// included: namespace and journal epoch belong to the durable publication
/// boundary rather than destination/account configuration.
pub const ResolvedSettlementInputs = struct {
    destination_id: DestinationId,
    ledgers: settlement.SettlementLedgers,
    taker_accounts: settlement.ParticipantAccounts,
    maker_accounts: settlement.ParticipantAccounts,
    quote_fee_account_id: u128,
    codes: settlement.SettlementCodes,

    pub fn bindChain(
        self: ResolvedSettlementInputs,
        chain: settlement.TransferChain,
    ) outbox.SettlementRecord {
        return .{ .destination_id = self.destination_id, .chain = chain };
    }
};

pub const ValidationError = error{
    RegistryTooLarge,
    InvalidDestination,
    DuplicateDestination,
    InvalidTransferCode,
    DuplicateTransferCode,
    DuplicateSymbolRoute,
    DuplicateSymbolCurrency,
    UnknownDestination,
    InvalidLedger,
    DuplicateCurrencyRoute,
    DuplicateLedger,
    InvalidFeeAccountId,
    MissingSymbolBaseCurrencyRoute,
    MissingSymbolQuoteCurrencyRoute,
    InvalidAccountId,
    DuplicateAccountRoute,
    UnknownCurrencyRoute,
    DuplicateExternalAccountId,
};

pub const RegistryInitError = std.mem.Allocator.Error || ValidationError;

pub const ResolveError = error{
    UnsupportedSymbolType,
    UnknownSymbol,
    SymbolCurrencyMismatch,
    MissingTakerBaseAccount,
    MissingTakerQuoteAccount,
    MissingMakerBaseAccount,
    MissingMakerQuoteAccount,
    RegistryCorrupted,
};

/// Opaque immutable registry. Construction deep-copies, sorts, and validates
/// every route before publication; all public lookups return values rather than
/// exposing internal mutable storage.
pub const DestinationRegistry = opaque {
    pub fn initValidated(
        allocator: std.mem.Allocator,
        input: RegistryInput,
    ) RegistryInitError!*DestinationRegistry {
        try validateInputLengths(input);

        const destinations = try allocator.dupe(DestinationSpec, input.destinations);
        errdefer allocator.free(destinations);
        const symbols = try allocator.dupe(SymbolRoute, input.symbols);
        errdefer allocator.free(symbols);
        const currencies = try allocator.dupe(CurrencyRoute, input.currencies);
        errdefer allocator.free(currencies);
        const accounts = try allocator.dupe(AccountRoute, input.accounts);
        errdefer allocator.free(accounts);

        std.sort.heap(DestinationSpec, destinations, {}, destinationLessThan);
        std.sort.heap(SymbolRoute, symbols, {}, symbolLessThan);
        std.sort.heap(CurrencyRoute, currencies, {}, currencyLessThan);
        std.sort.heap(AccountRoute, accounts, {}, accountLessThan);

        try validateOwned(
            allocator,
            destinations,
            symbols,
            currencies,
            accounts,
        );

        const impl = try allocator.create(RegistryImpl);
        impl.* = .{
            .allocator = allocator,
            .destinations = destinations,
            .symbols = symbols,
            .currencies = currencies,
            .accounts = accounts,
        };
        return registryToken(impl);
    }

    /// Returns an independently owned immutable clone. It remains valid after
    /// the source registry is deinitialized.
    pub fn cloneOwned(
        self: *const DestinationRegistry,
        allocator: std.mem.Allocator,
    ) RegistryInitError!*DestinationRegistry {
        const impl = registryImpl(self);
        return initValidated(allocator, .{
            .destinations = impl.destinations,
            .symbols = impl.symbols,
            .currencies = impl.currencies,
            .accounts = impl.accounts,
        });
    }

    pub fn deinit(self: *DestinationRegistry) void {
        const impl = registryImplMut(self);
        const allocator = impl.allocator;
        allocator.free(impl.accounts);
        allocator.free(impl.currencies);
        allocator.free(impl.symbols);
        allocator.free(impl.destinations);
        allocator.destroy(impl);
    }

    pub fn counts(self: *const DestinationRegistry) RegistryCounts {
        const impl = registryImpl(self);
        return .{
            .destinations = impl.destinations.len,
            .symbols = impl.symbols.len,
            .currencies = impl.currencies.len,
            .accounts = impl.accounts.len,
        };
    }

    pub fn lookupDestination(
        self: *const DestinationRegistry,
        destination_id: DestinationId,
    ) ?DestinationSpec {
        const destination = findDestination(
            registryImpl(self).destinations,
            destination_id,
        ) orelse return null;
        return destination.*;
    }

    /// Resolves all external accounts and ledgers needed by `SettlementFill`.
    /// The returned value owns no borrowed slices and may be retained by the
    /// caller independently of registry lookups.
    pub fn resolve(
        self: *const DestinationRegistry,
        spec: common.CoreSymbolSpecification,
        taker_uid: i64,
        maker_uid: i64,
    ) ResolveError!ResolvedSettlementInputs {
        if (spec.symbol_type != .currency_exchange_pair) {
            return error.UnsupportedSymbolType;
        }

        const impl = registryImpl(self);
        const symbol = findSymbol(impl.symbols, spec.symbol_id) orelse
            return error.UnknownSymbol;
        if (symbol.base_currency != spec.base_currency or
            symbol.quote_currency != spec.quote_currency)
        {
            return error.SymbolCurrencyMismatch;
        }

        const destination = findDestination(
            impl.destinations,
            symbol.destination_id,
        ) orelse return error.RegistryCorrupted;
        const base = findCurrency(
            impl.currencies,
            symbol.destination_id,
            symbol.base_currency,
        ) orelse return error.RegistryCorrupted;
        const quote = findCurrency(
            impl.currencies,
            symbol.destination_id,
            symbol.quote_currency,
        ) orelse return error.RegistryCorrupted;

        const taker_base = findAccount(
            impl.accounts,
            symbol.destination_id,
            taker_uid,
            symbol.base_currency,
        ) orelse return error.MissingTakerBaseAccount;
        const taker_quote = findAccount(
            impl.accounts,
            symbol.destination_id,
            taker_uid,
            symbol.quote_currency,
        ) orelse return error.MissingTakerQuoteAccount;
        const maker_base = findAccount(
            impl.accounts,
            symbol.destination_id,
            maker_uid,
            symbol.base_currency,
        ) orelse return error.MissingMakerBaseAccount;
        const maker_quote = findAccount(
            impl.accounts,
            symbol.destination_id,
            maker_uid,
            symbol.quote_currency,
        ) orelse return error.MissingMakerQuoteAccount;

        return .{
            .destination_id = symbol.destination_id,
            .ledgers = .{
                .base = base.ledger,
                .quote = quote.ledger,
            },
            .taker_accounts = .{
                .base_account_id = taker_base.account_id,
                .quote_account_id = taker_quote.account_id,
            },
            .maker_accounts = .{
                .base_account_id = maker_base.account_id,
                .quote_account_id = maker_quote.account_id,
            },
            .quote_fee_account_id = quote.fee_account_id,
            .codes = destination.codes,
        };
    }

    /// Canonical, architecture-independent identity for the complete routing
    /// snapshot. Future production capture may bind this digest into journal or
    /// snapshot compatibility metadata without persisting this registry in the
    /// delivery outbox.
    pub fn digest(
        self: *const DestinationRegistry,
    ) [registry_digest_len]u8 {
        const impl = registryImpl(self);
        var hasher = Sha256.init(.{});
        hasher.update(digest_domain);

        hashInt(u32, &hasher, @intCast(impl.destinations.len));
        for (impl.destinations) |destination| {
            hashInt(u128, &hasher, destinationRaw(destination.id));
            hashCodes(&hasher, destination.codes);
        }

        hashInt(u32, &hasher, @intCast(impl.symbols.len));
        for (impl.symbols) |symbol| {
            hashInt(i32, &hasher, symbol.symbol_id);
            hashInt(i32, &hasher, symbol.base_currency);
            hashInt(i32, &hasher, symbol.quote_currency);
            hashInt(u128, &hasher, destinationRaw(symbol.destination_id));
        }

        hashInt(u32, &hasher, @intCast(impl.currencies.len));
        for (impl.currencies) |currency| {
            hashInt(u128, &hasher, destinationRaw(currency.destination_id));
            hashInt(i32, &hasher, currency.currency);
            hashInt(u32, &hasher, currency.ledger);
            hashInt(u128, &hasher, currency.fee_account_id);
        }

        hashInt(u32, &hasher, @intCast(impl.accounts.len));
        for (impl.accounts) |account| {
            hashInt(u128, &hasher, destinationRaw(account.destination_id));
            hashInt(i64, &hasher, account.uid);
            hashInt(i32, &hasher, account.currency);
            hashInt(u128, &hasher, account.account_id);
        }

        var result: [registry_digest_len]u8 = undefined;
        hasher.final(&result);
        return result;
    }
};

const RegistryImpl = struct {
    allocator: std.mem.Allocator,
    destinations: []DestinationSpec,
    symbols: []SymbolRoute,
    currencies: []CurrencyRoute,
    accounts: []AccountRoute,
};

const LedgerKey = struct {
    destination_id: DestinationId,
    ledger: u32,
};

const ExternalAccountKey = struct {
    destination_id: DestinationId,
    account_id: u128,
};

fn validateInputLengths(input: RegistryInput) ValidationError!void {
    if (input.destinations.len > std.math.maxInt(u32) or
        input.symbols.len > std.math.maxInt(u32) or
        input.currencies.len > std.math.maxInt(u32) or
        input.accounts.len > std.math.maxInt(u32))
    {
        return error.RegistryTooLarge;
    }
}

fn validateOwned(
    allocator: std.mem.Allocator,
    destinations: []const DestinationSpec,
    symbols: []const SymbolRoute,
    currencies: []const CurrencyRoute,
    accounts: []const AccountRoute,
) RegistryInitError!void {
    for (destinations) |destination| {
        if (destinationRaw(destination.id) == 0) return error.InvalidDestination;
        try validateCodes(destination.codes);
    }
    if (destinations.len > 1) {
        for (1..destinations.len) |index| {
            if (destinations[index - 1].id == destinations[index].id) {
                return error.DuplicateDestination;
            }
        }
    }

    for (currencies) |currency| {
        if (findDestination(destinations, currency.destination_id) == null) {
            return error.UnknownDestination;
        }
        if (currency.ledger == 0) return error.InvalidLedger;
        if (!validAccountId(currency.fee_account_id)) {
            return error.InvalidFeeAccountId;
        }
    }
    if (currencies.len > 1) {
        for (1..currencies.len) |index| {
            if (sameCurrencyKey(currencies[index - 1], currencies[index])) {
                return error.DuplicateCurrencyRoute;
            }
        }
    }
    try validateUniqueLedgers(allocator, currencies);

    for (symbols) |symbol| {
        if (symbol.base_currency == symbol.quote_currency) {
            return error.DuplicateSymbolCurrency;
        }
        if (findDestination(destinations, symbol.destination_id) == null) {
            return error.UnknownDestination;
        }
    }
    if (symbols.len > 1) {
        for (1..symbols.len) |index| {
            if (symbols[index - 1].symbol_id == symbols[index].symbol_id) {
                return error.DuplicateSymbolRoute;
            }
        }
    }
    for (symbols) |symbol| {
        if (findCurrency(
            currencies,
            symbol.destination_id,
            symbol.base_currency,
        ) == null) return error.MissingSymbolBaseCurrencyRoute;
        if (findCurrency(
            currencies,
            symbol.destination_id,
            symbol.quote_currency,
        ) == null) return error.MissingSymbolQuoteCurrencyRoute;
    }

    for (accounts) |account| {
        if (!validAccountId(account.account_id)) return error.InvalidAccountId;
        if (findDestination(destinations, account.destination_id) == null) {
            return error.UnknownDestination;
        }
        if (findCurrency(
            currencies,
            account.destination_id,
            account.currency,
        ) == null) return error.UnknownCurrencyRoute;
    }
    if (accounts.len > 1) {
        for (1..accounts.len) |index| {
            if (sameAccountKey(accounts[index - 1], accounts[index])) {
                return error.DuplicateAccountRoute;
            }
        }
    }
    try validateUniqueExternalAccountIds(allocator, currencies, accounts);
}

fn validateCodes(codes: settlement.SettlementCodes) ValidationError!void {
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

fn validateUniqueLedgers(
    allocator: std.mem.Allocator,
    currencies: []const CurrencyRoute,
) RegistryInitError!void {
    const keys = try allocator.alloc(LedgerKey, currencies.len);
    defer allocator.free(keys);
    for (currencies, 0..) |currency, index| {
        keys[index] = .{
            .destination_id = currency.destination_id,
            .ledger = currency.ledger,
        };
    }
    std.sort.heap(LedgerKey, keys, {}, ledgerLessThan);
    if (keys.len > 1) {
        for (1..keys.len) |index| {
            if (keys[index - 1].destination_id == keys[index].destination_id and
                keys[index - 1].ledger == keys[index].ledger)
            {
                return error.DuplicateLedger;
            }
        }
    }
}

fn validateUniqueExternalAccountIds(
    allocator: std.mem.Allocator,
    currencies: []const CurrencyRoute,
    accounts: []const AccountRoute,
) RegistryInitError!void {
    const count = std.math.add(usize, currencies.len, accounts.len) catch
        return error.RegistryTooLarge;
    const keys = try allocator.alloc(ExternalAccountKey, count);
    defer allocator.free(keys);

    for (currencies, 0..) |currency, index| {
        keys[index] = .{
            .destination_id = currency.destination_id,
            .account_id = currency.fee_account_id,
        };
    }
    for (accounts, 0..) |account, index| {
        keys[currencies.len + index] = .{
            .destination_id = account.destination_id,
            .account_id = account.account_id,
        };
    }
    std.sort.heap(ExternalAccountKey, keys, {}, externalAccountLessThan);
    if (keys.len > 1) {
        for (1..keys.len) |index| {
            if (keys[index - 1].destination_id == keys[index].destination_id and
                keys[index - 1].account_id == keys[index].account_id)
            {
                return error.DuplicateExternalAccountId;
            }
        }
    }
}

fn validAccountId(account_id: u128) bool {
    return account_id != 0 and account_id != std.math.maxInt(u128);
}

fn destinationRaw(destination_id: DestinationId) u128 {
    return @intFromEnum(destination_id);
}

fn destinationLessThan(
    _: void,
    left: DestinationSpec,
    right: DestinationSpec,
) bool {
    return destinationRaw(left.id) < destinationRaw(right.id);
}

fn symbolLessThan(_: void, left: SymbolRoute, right: SymbolRoute) bool {
    return left.symbol_id < right.symbol_id;
}

fn currencyLessThan(_: void, left: CurrencyRoute, right: CurrencyRoute) bool {
    const left_destination = destinationRaw(left.destination_id);
    const right_destination = destinationRaw(right.destination_id);
    if (left_destination != right_destination) {
        return left_destination < right_destination;
    }
    return left.currency < right.currency;
}

fn accountLessThan(_: void, left: AccountRoute, right: AccountRoute) bool {
    const left_destination = destinationRaw(left.destination_id);
    const right_destination = destinationRaw(right.destination_id);
    if (left_destination != right_destination) {
        return left_destination < right_destination;
    }
    if (left.uid != right.uid) return left.uid < right.uid;
    return left.currency < right.currency;
}

fn ledgerLessThan(_: void, left: LedgerKey, right: LedgerKey) bool {
    const left_destination = destinationRaw(left.destination_id);
    const right_destination = destinationRaw(right.destination_id);
    if (left_destination != right_destination) {
        return left_destination < right_destination;
    }
    return left.ledger < right.ledger;
}

fn externalAccountLessThan(
    _: void,
    left: ExternalAccountKey,
    right: ExternalAccountKey,
) bool {
    const left_destination = destinationRaw(left.destination_id);
    const right_destination = destinationRaw(right.destination_id);
    if (left_destination != right_destination) {
        return left_destination < right_destination;
    }
    return left.account_id < right.account_id;
}

fn sameCurrencyKey(left: CurrencyRoute, right: CurrencyRoute) bool {
    return left.destination_id == right.destination_id and
        left.currency == right.currency;
}

fn sameAccountKey(left: AccountRoute, right: AccountRoute) bool {
    return left.destination_id == right.destination_id and
        left.uid == right.uid and
        left.currency == right.currency;
}

fn findDestination(
    destinations: []const DestinationSpec,
    destination_id: DestinationId,
) ?*const DestinationSpec {
    const target = destinationRaw(destination_id);
    var low: usize = 0;
    var high = destinations.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        const candidate = destinationRaw(destinations[mid].id);
        if (candidate < target) {
            low = mid + 1;
        } else if (candidate > target) {
            high = mid;
        } else {
            return &destinations[mid];
        }
    }
    return null;
}

fn findSymbol(
    symbols: []const SymbolRoute,
    symbol_id: i32,
) ?*const SymbolRoute {
    var low: usize = 0;
    var high = symbols.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        const candidate = symbols[mid].symbol_id;
        if (candidate < symbol_id) {
            low = mid + 1;
        } else if (candidate > symbol_id) {
            high = mid;
        } else {
            return &symbols[mid];
        }
    }
    return null;
}

fn findCurrency(
    currencies: []const CurrencyRoute,
    destination_id: DestinationId,
    currency: i32,
) ?*const CurrencyRoute {
    const target_destination = destinationRaw(destination_id);
    var low: usize = 0;
    var high = currencies.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        const candidate = currencies[mid];
        const candidate_destination = destinationRaw(candidate.destination_id);
        if (candidate_destination < target_destination or
            (candidate_destination == target_destination and
                candidate.currency < currency))
        {
            low = mid + 1;
        } else if (candidate_destination > target_destination or
            candidate.currency > currency)
        {
            high = mid;
        } else {
            return &currencies[mid];
        }
    }
    return null;
}

fn findAccount(
    accounts: []const AccountRoute,
    destination_id: DestinationId,
    uid: i64,
    currency: i32,
) ?*const AccountRoute {
    const target_destination = destinationRaw(destination_id);
    var low: usize = 0;
    var high = accounts.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        const candidate = accounts[mid];
        const candidate_destination = destinationRaw(candidate.destination_id);
        if (candidate_destination < target_destination or
            (candidate_destination == target_destination and candidate.uid < uid) or
            (candidate_destination == target_destination and candidate.uid == uid and
                candidate.currency < currency))
        {
            low = mid + 1;
        } else if (candidate_destination > target_destination or
            candidate.uid > uid or
            (candidate.uid == uid and candidate.currency > currency))
        {
            high = mid;
        } else {
            return &accounts[mid];
        }
    }
    return null;
}

fn hashCodes(hasher: *Sha256, codes: settlement.SettlementCodes) void {
    hashInt(u16, hasher, codes.quote_principal);
    hashInt(u16, hasher, codes.base_principal);
    hashInt(u16, hasher, codes.maker_fee);
    hashInt(u16, hasher, codes.taker_fee);
}

fn hashInt(comptime T: type, hasher: *Sha256, value: T) void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .big);
    hasher.update(&bytes);
}

fn registryToken(impl: *RegistryImpl) *DestinationRegistry {
    return @ptrCast(impl);
}

fn registryImpl(registry: *const DestinationRegistry) *const RegistryImpl {
    return @ptrCast(@alignCast(registry));
}

fn registryImplMut(registry: *DestinationRegistry) *RegistryImpl {
    return @ptrCast(@alignCast(registry));
}

fn testDestination(raw: u128) DestinationId {
    return @enumFromInt(raw);
}

fn testSpec(
    symbol_id: i32,
    symbol_type: common.SymbolType,
    base_currency: i32,
    quote_currency: i32,
) common.CoreSymbolSpecification {
    return common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol_id,
        .symbol_type = symbol_type,
        .base_currency = base_currency,
        .quote_currency = quote_currency,
        .base_scale_k = 100,
        .quote_scale_k = 10,
        .taker_fee = 2,
        .maker_fee = 1,
    }) catch unreachable;
}

fn expectValidationError(
    expected: ValidationError,
    input: RegistryInput,
) !void {
    try std.testing.expectError(
        expected,
        DestinationRegistry.initValidated(std.testing.allocator, input),
    );
}

fn registryAllocationCampaign(allocator: std.mem.Allocator) !void {
    const destination_id = testDestination(42);
    const destinations = [_]DestinationSpec{.{ .id = destination_id }};
    const symbols = [_]SymbolRoute{.{
        .symbol_id = 241,
        .base_currency = 11,
        .quote_currency = 15,
        .destination_id = destination_id,
    }};
    const currencies = [_]CurrencyRoute{
        .{ .destination_id = destination_id, .currency = 11, .ledger = 421, .fee_account_id = 42_011 },
        .{ .destination_id = destination_id, .currency = 15, .ledger = 425, .fee_account_id = 42_015 },
    };
    const accounts = [_]AccountRoute{
        .{ .destination_id = destination_id, .uid = 1001, .currency = 11, .account_id = 421_011 },
        .{ .destination_id = destination_id, .uid = 1001, .currency = 15, .account_id = 421_015 },
        .{ .destination_id = destination_id, .uid = 1002, .currency = 11, .account_id = 422_011 },
        .{ .destination_id = destination_id, .uid = 1002, .currency = 15, .account_id = 422_015 },
    };
    var registry = try DestinationRegistry.initValidated(allocator, .{
        .destinations = &destinations,
        .symbols = &symbols,
        .currencies = &currencies,
        .accounts = &accounts,
    });
    defer registry.deinit();

    var clone = try registry.cloneOwned(allocator);
    defer clone.deinit();
    _ = try clone.resolve(
        testSpec(241, .currency_exchange_pair, 11, 15),
        1001,
        1002,
    );
}

test "destination registry owns sorted routes and resolves settlement inputs" {
    const allocator = std.testing.allocator;
    const destination_id = testDestination(11);
    var destinations = [_]DestinationSpec{
        .{ .id = testDestination(22), .codes = .{
            .quote_principal = 401,
            .base_principal = 402,
            .maker_fee = 403,
            .taker_fee = 404,
        } },
        .{ .id = destination_id },
    };
    var symbols = [_]SymbolRoute{
        .{ .symbol_id = 900, .base_currency = 21, .quote_currency = 25, .destination_id = testDestination(22) },
        .{ .symbol_id = 241, .base_currency = 11, .quote_currency = 15, .destination_id = destination_id },
    };
    var currencies = [_]CurrencyRoute{
        .{ .destination_id = testDestination(22), .currency = 25, .ledger = 225, .fee_account_id = 22_025 },
        .{ .destination_id = destination_id, .currency = 15, .ledger = 115, .fee_account_id = 11_015 },
        .{ .destination_id = testDestination(22), .currency = 21, .ledger = 221, .fee_account_id = 22_021 },
        .{ .destination_id = destination_id, .currency = 11, .ledger = 111, .fee_account_id = 11_011 },
    };
    var accounts = [_]AccountRoute{
        .{ .destination_id = destination_id, .uid = 1002, .currency = 15, .account_id = 102_015 },
        .{ .destination_id = destination_id, .uid = 1001, .currency = 11, .account_id = 101_011 },
        .{ .destination_id = destination_id, .uid = 1002, .currency = 11, .account_id = 102_011 },
        .{ .destination_id = destination_id, .uid = 1001, .currency = 15, .account_id = 101_015 },
    };

    var registry = try DestinationRegistry.initValidated(allocator, .{
        .destinations = &destinations,
        .symbols = &symbols,
        .currencies = &currencies,
        .accounts = &accounts,
    });
    defer registry.deinit();

    // The caller's source storage may be changed immediately after init.
    destinations[1].codes.quote_principal = 999;
    symbols[1].destination_id = testDestination(99);
    currencies[1].ledger = 999;
    accounts[0].account_id = 999;

    const counts = registry.counts();
    try std.testing.expectEqual(@as(usize, 2), counts.destinations);
    try std.testing.expectEqual(@as(usize, 2), counts.symbols);
    try std.testing.expectEqual(@as(usize, 4), counts.currencies);
    try std.testing.expectEqual(@as(usize, 4), counts.accounts);

    const destination = registry.lookupDestination(destination_id) orelse
        return error.MissingDestination;
    try std.testing.expectEqual(@as(u16, 300), destination.codes.quote_principal);
    try std.testing.expect(registry.lookupDestination(testDestination(99)) == null);

    const spec = testSpec(241, .currency_exchange_pair, 11, 15);
    const resolved = try registry.resolve(spec, 1001, 1002);
    try std.testing.expectEqual(destination_id, resolved.destination_id);
    try std.testing.expectEqual(@as(u32, 111), resolved.ledgers.base);
    try std.testing.expectEqual(@as(u32, 115), resolved.ledgers.quote);
    try std.testing.expectEqual(@as(u128, 101_011), resolved.taker_accounts.base_account_id);
    try std.testing.expectEqual(@as(u128, 101_015), resolved.taker_accounts.quote_account_id);
    try std.testing.expectEqual(@as(u128, 102_011), resolved.maker_accounts.base_account_id);
    try std.testing.expectEqual(@as(u128, 102_015), resolved.maker_accounts.quote_account_id);
    try std.testing.expectEqual(@as(u128, 11_015), resolved.quote_fee_account_id);

    const chain = try settlement.mapFill(.{
        .identity = .{
            .namespace = 7,
            .journal_epoch = 3,
            .publication_sequence = .fromInt(9),
            .fill_ordinal = 0,
        },
        .taker_uid = 1001,
        .maker_uid = 1002,
        .taker_action = .bid,
        .size = 2,
        .price = 10,
        .spec = spec,
        .ledgers = resolved.ledgers,
        .taker_accounts = resolved.taker_accounts,
        .maker_accounts = resolved.maker_accounts,
        .quote_fee_account_id = resolved.quote_fee_account_id,
        .codes = resolved.codes,
    });
    try std.testing.expectEqual(@as(u8, 4), chain.count);
    const record = resolved.bindChain(chain);
    try std.testing.expectEqual(destination_id, record.destination_id);
    try std.testing.expectEqualDeep(chain, record.chain);
}

test "destination registry rejects invalid and ambiguous routing" {
    const destination_id = testDestination(1);
    const valid_destination = [_]DestinationSpec{.{ .id = destination_id }};
    const valid_currencies = [_]CurrencyRoute{
        .{ .destination_id = destination_id, .currency = 11, .ledger = 101, .fee_account_id = 10_011 },
        .{ .destination_id = destination_id, .currency = 15, .ledger = 105, .fee_account_id = 10_015 },
    };

    try expectValidationError(error.InvalidDestination, .{
        .destinations = &.{.{ .id = testDestination(0) }},
    });
    try expectValidationError(error.DuplicateDestination, .{
        .destinations = &.{ .{ .id = destination_id }, .{ .id = destination_id } },
    });
    try expectValidationError(error.InvalidTransferCode, .{
        .destinations = &.{.{ .id = destination_id, .codes = .{ .quote_principal = 0 } }},
    });
    try expectValidationError(error.DuplicateTransferCode, .{
        .destinations = &.{.{ .id = destination_id, .codes = .{ .base_principal = 300 } }},
    });
    try expectValidationError(error.UnknownDestination, .{
        .currencies = &.{.{ .destination_id = destination_id, .currency = 11, .ledger = 1, .fee_account_id = 1 }},
    });
    try expectValidationError(error.InvalidLedger, .{
        .destinations = &valid_destination,
        .currencies = &.{.{ .destination_id = destination_id, .currency = 11, .ledger = 0, .fee_account_id = 1 }},
    });
    try expectValidationError(error.DuplicateCurrencyRoute, .{
        .destinations = &valid_destination,
        .currencies = &.{
            .{ .destination_id = destination_id, .currency = 11, .ledger = 1, .fee_account_id = 1 },
            .{ .destination_id = destination_id, .currency = 11, .ledger = 2, .fee_account_id = 2 },
        },
    });
    try expectValidationError(error.DuplicateLedger, .{
        .destinations = &valid_destination,
        .currencies = &.{
            .{ .destination_id = destination_id, .currency = 11, .ledger = 1, .fee_account_id = 1 },
            .{ .destination_id = destination_id, .currency = 15, .ledger = 1, .fee_account_id = 2 },
        },
    });
    try expectValidationError(error.InvalidFeeAccountId, .{
        .destinations = &valid_destination,
        .currencies = &.{.{ .destination_id = destination_id, .currency = 11, .ledger = 1, .fee_account_id = 0 }},
    });
    try expectValidationError(error.DuplicateSymbolCurrency, .{
        .destinations = &valid_destination,
        .symbols = &.{.{ .symbol_id = 1, .base_currency = 11, .quote_currency = 11, .destination_id = destination_id }},
    });
    try expectValidationError(error.DuplicateSymbolRoute, .{
        .destinations = &valid_destination,
        .currencies = &valid_currencies,
        .symbols = &.{
            .{ .symbol_id = 1, .base_currency = 11, .quote_currency = 15, .destination_id = destination_id },
            .{ .symbol_id = 1, .base_currency = 15, .quote_currency = 11, .destination_id = destination_id },
        },
    });
    try expectValidationError(error.MissingSymbolBaseCurrencyRoute, .{
        .destinations = &valid_destination,
        .symbols = &.{.{ .symbol_id = 1, .base_currency = 11, .quote_currency = 15, .destination_id = destination_id }},
        .currencies = valid_currencies[1..],
    });
    try expectValidationError(error.InvalidAccountId, .{
        .destinations = &valid_destination,
        .currencies = &valid_currencies,
        .accounts = &.{.{ .destination_id = destination_id, .uid = 1, .currency = 11, .account_id = 0 }},
    });
    try expectValidationError(error.UnknownCurrencyRoute, .{
        .destinations = &valid_destination,
        .currencies = &valid_currencies,
        .accounts = &.{.{ .destination_id = destination_id, .uid = 1, .currency = 99, .account_id = 99 }},
    });
    try expectValidationError(error.DuplicateAccountRoute, .{
        .destinations = &valid_destination,
        .currencies = &valid_currencies,
        .accounts = &.{
            .{ .destination_id = destination_id, .uid = 1, .currency = 11, .account_id = 20_001 },
            .{ .destination_id = destination_id, .uid = 1, .currency = 11, .account_id = 20_002 },
        },
    });
    try expectValidationError(error.DuplicateExternalAccountId, .{
        .destinations = &valid_destination,
        .currencies = &valid_currencies,
        .accounts = &.{.{ .destination_id = destination_id, .uid = 1, .currency = 11, .account_id = 10_015 }},
    });
}

test "destination registry returns typed resolution failures and supports self trades" {
    const destination_id = testDestination(5);
    const destinations = [_]DestinationSpec{.{ .id = destination_id }};
    const symbols = [_]SymbolRoute{.{
        .symbol_id = 241,
        .base_currency = 11,
        .quote_currency = 15,
        .destination_id = destination_id,
    }};
    const currencies = [_]CurrencyRoute{
        .{ .destination_id = destination_id, .currency = 11, .ledger = 511, .fee_account_id = 50_011 },
        .{ .destination_id = destination_id, .currency = 15, .ledger = 515, .fee_account_id = 50_015 },
    };
    const accounts = [_]AccountRoute{
        .{ .destination_id = destination_id, .uid = 1001, .currency = 11, .account_id = 51_011 },
        .{ .destination_id = destination_id, .uid = 1001, .currency = 15, .account_id = 51_015 },
    };
    var registry = try DestinationRegistry.initValidated(std.testing.allocator, .{
        .destinations = &destinations,
        .symbols = &symbols,
        .currencies = &currencies,
        .accounts = &accounts,
    });
    defer registry.deinit();

    try std.testing.expectError(
        error.UnsupportedSymbolType,
        registry.resolve(testSpec(241, .futures_contract, 11, 15), 1001, 1001),
    );
    try std.testing.expectError(
        error.UnknownSymbol,
        registry.resolve(testSpec(999, .currency_exchange_pair, 11, 15), 1001, 1001),
    );
    try std.testing.expectError(
        error.SymbolCurrencyMismatch,
        registry.resolve(testSpec(241, .currency_exchange_pair, 11, 16), 1001, 1001),
    );
    try std.testing.expectError(
        error.MissingMakerBaseAccount,
        registry.resolve(testSpec(241, .currency_exchange_pair, 11, 15), 1001, 1002),
    );

    const self_trade = try registry.resolve(
        testSpec(241, .currency_exchange_pair, 11, 15),
        1001,
        1001,
    );
    try std.testing.expectEqual(
        self_trade.taker_accounts,
        self_trade.maker_accounts,
    );
}

test "destination registry digest is canonical and owned clones outlive sources" {
    const allocator = std.testing.allocator;
    const destination_id = testDestination(8);
    const destinations = [_]DestinationSpec{.{ .id = destination_id }};
    const symbols = [_]SymbolRoute{.{
        .symbol_id = 241,
        .base_currency = 11,
        .quote_currency = 15,
        .destination_id = destination_id,
    }};
    const currencies_forward = [_]CurrencyRoute{
        .{ .destination_id = destination_id, .currency = 11, .ledger = 811, .fee_account_id = 80_011 },
        .{ .destination_id = destination_id, .currency = 15, .ledger = 815, .fee_account_id = 80_015 },
    };
    const currencies_reverse = [_]CurrencyRoute{
        currencies_forward[1],
        currencies_forward[0],
    };
    const accounts_forward = [_]AccountRoute{
        .{ .destination_id = destination_id, .uid = 1001, .currency = 11, .account_id = 81_011 },
        .{ .destination_id = destination_id, .uid = 1001, .currency = 15, .account_id = 81_015 },
        .{ .destination_id = destination_id, .uid = 1002, .currency = 11, .account_id = 82_011 },
        .{ .destination_id = destination_id, .uid = 1002, .currency = 15, .account_id = 82_015 },
    };
    const accounts_reverse = [_]AccountRoute{
        accounts_forward[3],
        accounts_forward[2],
        accounts_forward[1],
        accounts_forward[0],
    };

    var forward = try DestinationRegistry.initValidated(allocator, .{
        .destinations = &destinations,
        .symbols = &symbols,
        .currencies = &currencies_forward,
        .accounts = &accounts_forward,
    });
    var forward_live = true;
    defer if (forward_live) forward.deinit();
    var reverse = try DestinationRegistry.initValidated(allocator, .{
        .destinations = &destinations,
        .symbols = &symbols,
        .currencies = &currencies_reverse,
        .accounts = &accounts_reverse,
    });
    defer reverse.deinit();

    const expected_digest = forward.digest();
    try std.testing.expectEqual(expected_digest, reverse.digest());

    var changed_accounts = accounts_forward;
    changed_accounts[3].account_id += 1;
    var changed = try DestinationRegistry.initValidated(allocator, .{
        .destinations = &destinations,
        .symbols = &symbols,
        .currencies = &currencies_forward,
        .accounts = &changed_accounts,
    });
    defer changed.deinit();
    try std.testing.expect(!std.mem.eql(u8, &expected_digest, &changed.digest()));

    var clone = try forward.cloneOwned(allocator);
    forward.deinit();
    forward_live = false;
    try std.testing.expectEqual(expected_digest, clone.digest());
    const resolved = try clone.resolve(
        testSpec(241, .currency_exchange_pair, 11, 15),
        1001,
        1002,
    );
    try std.testing.expectEqual(@as(u128, 82_015), resolved.maker_accounts.quote_account_id);
    clone.deinit();
}

test "destination registry cleans every allocation failure" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        registryAllocationCampaign,
        .{},
    );
}
