const std = @import("std");
const common = @import("common.zig");
const cmd = @import("cmd.zig");
const user = @import("user_profile.zig");
const events = @import("events.zig");
const int_map_order = @import("eclipse_int_map_order.zig");
const long_map_order = @import("eclipse_long_map_order.zig");

pub const SymbolSpecificationProvider = struct {
    allocator: std.mem.Allocator,
    symbols: std.AutoHashMapUnmanaged(i32, common.CoreSymbolSpecification) = .empty,

    pub fn init(allocator: std.mem.Allocator) SymbolSpecificationProvider {
        return .{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SymbolSpecificationProvider) void {
        self.symbols.deinit(self.allocator);
        self.* = undefined;
    }

    pub const AddSymbolError = std.mem.Allocator.Error || common.CoreSymbolSpecification.ValidationError;

    pub fn addSymbol(
        self: *SymbolSpecificationProvider,
        spec: common.CoreSymbolSpecification,
    ) AddSymbolError!void {
        try spec.validate();
        try self.symbols.put(self.allocator, spec.symbol_id, spec);
    }

    pub fn getSymbolSpecification(self: *const SymbolSpecificationProvider, symbol_id: i32) ?common.CoreSymbolSpecification {
        return self.symbols.get(symbol_id);
    }

    pub fn reset(self: *SymbolSpecificationProvider) void {
        self.symbols.clearRetainingCapacity();
    }

    pub fn stateHash(self: *const SymbolSpecificationProvider) StateHashError!u64 {
        var hasher = std.hash.Wyhash.init(0);
        var ids: std.ArrayListUnmanaged(i32) = .empty;
        defer ids.deinit(self.allocator);

        try ids.ensureTotalCapacityPrecise(self.allocator, self.symbols.count());
        var it = self.symbols.iterator();
        while (it.next()) |entry| {
            ids.appendAssumeCapacity(entry.key_ptr.*);
        }
        std.sort.heap(i32, ids.items, {}, lessThanI32);

        for (ids.items) |symbol_id| {
            const spec = self.symbols.get(symbol_id) orelse return error.StateHashCorrupted;
            hashI32(&hasher, symbol_id);
            hashCoreSymbolSpecification(&hasher, spec);
        }
        return hasher.final();
    }
};

pub const Config = struct {
    ignore_risk_processing: bool = false,
    margin_trading_enabled: bool = false,
};

pub const InitError = std.mem.Allocator.Error || error{
    InvalidNumShards,
};

pub const StateHashError = std.mem.Allocator.Error || error{
    StateHashCorrupted,
};

pub const ReleaseError = std.mem.Allocator.Error || user.MarginMathError || error{
    CapacityOverflow,
    InvalidExchangeReleaseCommand,
    InvalidMarginReleaseCommand,
    PositionRecordCorrupted,
    PositionSymbolSpecificationMissing,
    UnsupportedExchangeReleaseSymbol,
    UnsupportedMarginReleaseSymbol,
};

const StagedMarginUser = struct {
    profile: *user.UserProfile,
    position: user.SymbolPositionRecord,
    account_balance: i64,
    accounts_history: int_map_order.History,
    positions_history: int_map_order.History,
    account_touched: bool = false,
    remove_position: bool = false,
    histories_moved: bool = false,
};

const StagedExchangeUser = struct {
    uid: i64,
    base_balance: i64,
    quote_balance: i64,
    base_touched: bool = false,
    quote_touched: bool = false,
    accounts_history: int_map_order.History,
    history_moved: bool = false,
    /// Owned only while the user does not yet exist in the engine. Preparation may
    /// allocate its account map; commit moves the complete value into user_profiles.
    new_profile: ?user.UserProfile = null,
};

pub const RiskEngine = struct {
    pub const LastPriceCacheRecord = struct {
        ask_price: i64 = std.math.maxInt(i64),
        bid_price: i64 = 0,
    };

    const PreparedLastPriceUpdate = struct {
        symbol: i32,
        record: LastPriceCacheRecord,
    };

    /// Prepared releases borrow user profiles from `owner`; callers must not mutate that
    /// risk engine between preparation and commit. `deinit` aborts an uncommitted release.
    pub const PreparedMarginRelease = struct {
        allocator: std.mem.Allocator,
        owner: *RiskEngine,
        staged_users: std.AutoHashMapUnmanaged(i64, StagedMarginUser) = .empty,
        user_order: std.ArrayListUnmanaged(i64) = .empty,
        quote_currency: i32,
        fee_touched: bool = false,
        next_fee: i64 = 0,
        fee_history: ?int_map_order.History = null,
        fee_history_moved: bool = false,
        last_price_update: ?PreparedLastPriceUpdate = null,
        committed: bool = false,

        pub fn deinit(self: *PreparedMarginRelease) void {
            var staged_it = self.staged_users.valueIterator();
            while (staged_it.next()) |staged| {
                if (!staged.histories_moved) {
                    staged.accounts_history.deinit(self.allocator);
                    staged.positions_history.deinit(self.allocator);
                }
            }
            self.staged_users.deinit(self.allocator);
            self.user_order.deinit(self.allocator);
            if (self.fee_history) |*history| {
                if (!self.fee_history_moved) history.deinit(self.allocator);
            }
            self.* = undefined;
        }
    };

    /// A prepared currency-exchange release owns only temporary state. Existing user
    /// profiles are addressed by uid rather than pointer because reserving capacity for
    /// a missing suspended user may relocate the profile map before commit.
    pub const PreparedExchangeRelease = struct {
        allocator: std.mem.Allocator,
        owner: *RiskEngine,
        staged_users: std.AutoHashMapUnmanaged(i64, StagedExchangeUser) = .empty,
        user_order: std.ArrayListUnmanaged(i64) = .empty,
        base_currency: i32,
        quote_currency: i32,
        missing_user_count: u32 = 0,
        fee_touched: bool = false,
        next_fee: i64 = 0,
        fee_history: ?int_map_order.History = null,
        fee_history_moved: bool = false,
        user_profiles_history: ?long_map_order.History = null,
        user_profiles_history_moved: bool = false,
        last_price_update: ?PreparedLastPriceUpdate = null,
        committed: bool = false,

        pub fn deinit(self: *PreparedExchangeRelease) void {
            var staged_it = self.staged_users.valueIterator();
            while (staged_it.next()) |staged| {
                if (staged.new_profile) |*profile| {
                    profile.deinit(self.allocator);
                }
                if (!staged.history_moved) staged.accounts_history.deinit(self.allocator);
            }
            self.staged_users.deinit(self.allocator);
            self.user_order.deinit(self.allocator);
            if (self.fee_history) |*history| {
                if (!self.fee_history_moved) history.deinit(self.allocator);
            }
            if (self.user_profiles_history) |*history| {
                if (!self.user_profiles_history_moved) history.deinit(self.allocator);
            }
            self.* = undefined;
        }
    };

    allocator: std.mem.Allocator,
    shard_id: u64,
    shard_mask: u64,
    cfg_ignore_risk_processing: bool,
    cfg_margin_trading_enabled: bool,
    /// Phase C: applied after successful add_user. Zero = no per-user map reserve.
    pre_reserve_accounts_per_user: usize = 0,
    pre_reserve_positions_per_user: usize = 0,
    symbol_spec_provider: SymbolSpecificationProvider,
    user_profile_service: user.UserProfileService,
    last_price_cache: std.AutoHashMapUnmanaged(i32, LastPriceCacheRecord) = .empty,
    fees: std.AutoHashMapUnmanaged(i32, i64) = .empty,
    fees_history: int_map_order.History = int_map_order.History.init(),
    adjustments: std.AutoHashMapUnmanaged(i32, i64) = .empty,
    adjustments_history: int_map_order.History = int_map_order.History.init(),
    suspends: std.AutoHashMapUnmanaged(i32, i64) = .empty,
    suspends_history: int_map_order.History = int_map_order.History.init(),

    pub fn init(
        allocator: std.mem.Allocator,
        shard_id: u64,
        num_shards: u64,
        config: Config,
    ) InitError!RiskEngine {
        if (num_shards == 0 or @popCount(num_shards) != 1) {
            return error.InvalidNumShards;
        }
        return .{
            .allocator = allocator,
            .shard_id = shard_id,
            .shard_mask = num_shards - 1,
            .cfg_ignore_risk_processing = config.ignore_risk_processing,
            .cfg_margin_trading_enabled = config.margin_trading_enabled,
            .symbol_spec_provider = SymbolSpecificationProvider.init(allocator),
            .user_profile_service = user.UserProfileService.init(allocator),
        };
    }

    pub fn deinit(self: *RiskEngine) void {
        self.last_price_cache.deinit(self.allocator);
        self.fees.deinit(self.allocator);
        self.fees_history.deinit(self.allocator);
        self.adjustments.deinit(self.allocator);
        self.adjustments_history.deinit(self.allocator);
        self.suspends.deinit(self.allocator);
        self.suspends_history.deinit(self.allocator);
        self.symbol_spec_provider.deinit();
        self.user_profile_service.deinit();
        self.* = undefined;
    }

    pub fn addSymbol(
        self: *RiskEngine,
        spec: common.CoreSymbolSpecification,
    ) SymbolSpecificationProvider.AddSymbolError!void {
        try self.symbol_spec_provider.addSymbol(spec);
    }

    /// Applies Java's live BatchAddSymbols admission gate. Raw state restore
    /// must use `addSymbol` so the serialized provider image is reconstructed
    /// exactly even when its configuration would reject that symbol today.
    pub fn addSymbolForLiveAdmission(
        self: *RiskEngine,
        spec: common.CoreSymbolSpecification,
    ) SymbolSpecificationProvider.AddSymbolError!void {
        // Java RiskEngine ignores margin instruments when margin trading is
        // disabled. MatchingEngineRouter still builds their order books, so
        // admission fails as INVALID_SYMBOL in the risk stage rather than as
        // a margin-risk rejection.
        if (spec.symbol_type != .currency_exchange_pair and !self.cfg_margin_trading_enabled) {
            return;
        }
        try self.addSymbol(spec);
    }

    pub fn getUserProfile(self: *RiskEngine, uid: i64) ?*user.UserProfile {
        return self.user_profile_service.getUserProfile(uid);
    }

    /// Phase C: pre-grow user profile map capacity (no inserts).
    pub fn preReserveUsers(self: *RiskEngine, additional: usize) std.mem.Allocator.Error!void {
        try self.user_profile_service.preReserveUsers(additional);
    }

    pub fn preReserveProfileMaps(
        self: *RiskEngine,
        uid: i64,
        accounts: usize,
        positions: usize,
    ) std.mem.Allocator.Error!void {
        try self.user_profile_service.preReserveProfileMaps(uid, accounts, positions);
    }

    pub fn getLastPrice(self: *const RiskEngine, symbol: i32) ?LastPriceCacheRecord {
        return self.last_price_cache.get(symbol);
    }

    pub fn getFeeCollected(self: *const RiskEngine, currency: i32) i64 {
        return self.fees.get(currency) orelse 0;
    }

    pub const HistoryValidationError = int_map_order.HistoryError || error{
        HistorySemanticKeySetMismatch,
    };

    pub fn feeKeysInHistoryOrder(self: *const RiskEngine) HistoryValidationError![]i32 {
        return validatedIntLongHistoryKeys(
            self.allocator,
            &self.fees_history,
            &self.fees,
        );
    }

    pub fn adjustmentKeysInHistoryOrder(self: *const RiskEngine) HistoryValidationError![]i32 {
        return validatedIntLongHistoryKeys(
            self.allocator,
            &self.adjustments_history,
            &self.adjustments,
        );
    }

    pub fn suspendKeysInHistoryOrder(self: *const RiskEngine) HistoryValidationError![]i32 {
        return validatedIntLongHistoryKeys(
            self.allocator,
            &self.suspends_history,
            &self.suspends,
        );
    }

    pub fn validatedFeesHistory(
        self: *const RiskEngine,
    ) HistoryValidationError!*const int_map_order.History {
        const keys = try self.feeKeysInHistoryOrder();
        self.allocator.free(keys);
        return &self.fees_history;
    }

    pub fn validatedAdjustmentsHistory(
        self: *const RiskEngine,
    ) HistoryValidationError!*const int_map_order.History {
        const keys = try self.adjustmentKeysInHistoryOrder();
        self.allocator.free(keys);
        return &self.adjustments_history;
    }

    pub fn validatedSuspendsHistory(
        self: *const RiskEngine,
    ) HistoryValidationError!*const int_map_order.History {
        const keys = try self.suspendKeysInHistoryOrder();
        self.allocator.free(keys);
        return &self.suspends_history;
    }

    pub fn cloneFeesHistory(
        self: *const RiskEngine,
        allocator: std.mem.Allocator,
    ) HistoryValidationError!int_map_order.History {
        if (self.fees_history.isKnown()) {
            const keys = try validatedIntLongHistoryKeys(allocator, &self.fees_history, &self.fees);
            allocator.free(keys);
        }
        return self.fees_history.clone(allocator);
    }

    pub fn cloneAdjustmentsHistory(
        self: *const RiskEngine,
        allocator: std.mem.Allocator,
    ) HistoryValidationError!int_map_order.History {
        if (self.adjustments_history.isKnown()) {
            const keys = try validatedIntLongHistoryKeys(
                allocator,
                &self.adjustments_history,
                &self.adjustments,
            );
            allocator.free(keys);
        }
        return self.adjustments_history.clone(allocator);
    }

    pub fn cloneSuspendsHistory(
        self: *const RiskEngine,
        allocator: std.mem.Allocator,
    ) HistoryValidationError!int_map_order.History {
        if (self.suspends_history.isKnown()) {
            const keys = try validatedIntLongHistoryKeys(
                allocator,
                &self.suspends_history,
                &self.suspends,
            );
            allocator.free(keys);
        }
        return self.suspends_history.clone(allocator);
    }

    pub fn replaceFeesHistory(
        self: *RiskEngine,
        next: *int_map_order.History,
    ) HistoryValidationError!void {
        try replaceValidatedIntLongHistory(
            self.allocator,
            &self.fees_history,
            next,
            &self.fees,
        );
    }

    pub fn replaceAdjustmentsHistory(
        self: *RiskEngine,
        next: *int_map_order.History,
    ) HistoryValidationError!void {
        try replaceValidatedIntLongHistory(
            self.allocator,
            &self.adjustments_history,
            next,
            &self.adjustments,
        );
    }

    pub fn replaceSuspendsHistory(
        self: *RiskEngine,
        next: *int_map_order.History,
    ) HistoryValidationError!void {
        try replaceValidatedIntLongHistory(
            self.allocator,
            &self.suspends_history,
            next,
            &self.suspends,
        );
    }

    pub fn markAggregateHistoriesUnavailable(self: *RiskEngine) void {
        markHistoryUnavailable(self.allocator, &self.fees_history);
        markHistoryUnavailable(self.allocator, &self.adjustments_history);
        markHistoryUnavailable(self.allocator, &self.suspends_history);
    }

    pub fn estimatePositionProfitForReport(
        self: *const RiskEngine,
        position: *const user.SymbolPositionRecord,
        spec: common.CoreSymbolSpecification,
    ) user.MarginMathError!i64 {
        const average_price: i64 = if (self.last_price_cache.get(position.symbol)) |price| blk: {
            // Java LastPriceCacheRecord.averagingRecord uses signed long
            // addition followed by an arithmetic shift, including wraparound.
            break :blk (price.ask_price +% price.bid_price) >> 1;
        } else 42;
        return position.estimateProfit(spec, average_price, average_price);
    }

    pub fn stateHash(self: *const RiskEngine) StateHashError!u64 {
        var hasher = std.hash.Wyhash.init(0);
        hashU64(&hasher, self.shard_id);
        hashU64(&hasher, self.shard_mask);
        hashBool(&hasher, self.cfg_ignore_risk_processing);
        hashBool(&hasher, self.cfg_margin_trading_enabled);
        hashU64(&hasher, try self.symbol_spec_provider.stateHash());
        hashU64(&hasher, try self.user_profile_service.stateHash());
        try hashLastPriceMap(self.allocator, &self.last_price_cache, &hasher);
        try hashI32I64Map(self.allocator, &self.fees, &hasher);
        try hashI32I64Map(self.allocator, &self.adjustments, &hasher);
        try hashI32I64Map(self.allocator, &self.suspends, &hasher);
        return hasher.final();
    }

    pub fn uidForThisHandler(self: *const RiskEngine, uid: i64) bool {
        if (self.shard_mask == 0) return true;
        return ((@as(u64, @bitCast(uid)) & self.shard_mask) == self.shard_id);
    }

    pub fn preProcessCommand(
        self: *RiskEngine,
        _: u64,
        command: *cmd.OrderCommand,
    ) ReleaseError!bool {
        switch (command.command) {
            .move_order,
            .cancel_order,
            .reduce_order,
            .order_book_request,
            => return false,

            .place_order => {
                if (self.uidForThisHandler(command.uid)) {
                    command.result_code = try self.placeOrderRiskCheck(command);
                }
                return false;
            },

            .add_user => {
                if (self.uidForThisHandler(command.uid)) {
                    if (try self.user_profile_service.addEmptyUserProfile(command.uid)) {
                        try self.user_profile_service.preReserveProfileMaps(
                            command.uid,
                            self.pre_reserve_accounts_per_user,
                            self.pre_reserve_positions_per_user,
                        );
                        command.result_code = .success;
                    } else {
                        command.result_code = .user_mgmt_user_already_exists;
                    }
                }
                return false;
            },

            .balance_adjustment => {
                if (self.uidForThisHandler(command.uid)) {
                    const order_type = command.order_type orelse {
                        command.result_code = .matching_unsupported_command;
                        return false;
                    };
                    const adjustment_type = common.BalanceAdjustmentType.fromCode(order_type.code()) catch {
                        command.result_code = .matching_unsupported_command;
                        return false;
                    };
                    command.result_code = try self.adjustBalance(
                        command.uid,
                        command.symbol,
                        command.price,
                        command.order_id,
                        adjustment_type,
                    );
                }
                return false;
            },

            .suspend_user => {
                if (self.uidForThisHandler(command.uid)) {
                    command.result_code = self.user_profile_service.suspendUserProfile(command.uid);
                }
                return false;
            },

            .resume_user => {
                if (self.uidForThisHandler(command.uid)) {
                    command.result_code = try self.user_profile_service.resumeUserProfile(command.uid);
                }
                return false;
            },

            .reset => {
                self.reset();
                if (self.shard_id == 0) {
                    command.result_code = .success;
                }
                return false;
            },

            .persist_state_matching => {
                // Java parity: risk stage marks matching persistence command as
                // valid for matching engine on shard 0.
                if (self.shard_id == 0) {
                    command.result_code = .valid_for_matching_engine;
                }
                return true;
            },

            .persist_state_risk => {
                // Current Zig persistence path is in-memory/disk APIs and does
                // not surface a serialization failure in this command path.
                command.result_code = .success;
                return false;
            },

            else => return false,
        }
    }

    fn adjustBalance(
        self: *RiskEngine,
        uid: i64,
        currency: i32,
        amount_diff: i64,
        funding_transaction_id: i64,
        adjustment_type: common.BalanceAdjustmentType,
    ) ReleaseError!cmd.CommandResultCode {
        const aggregate_map: *std.AutoHashMapUnmanaged(i32, i64) = switch (adjustment_type) {
            .adjustment => &self.adjustments,
            .@"suspend" => &self.suspends,
        };
        const aggregate_history: *int_map_order.History = switch (adjustment_type) {
            .adjustment => &self.adjustments_history,
            .@"suspend" => &self.suspends_history,
        };

        var staged_history: ?int_map_order.History = null;
        defer if (staged_history) |*history| history.deinit(self.allocator);
        if (!aggregate_map.contains(currency)) {
            staged_history = try aggregate_history.clone(self.allocator);
            try staged_history.?.recordPut(self.allocator, currency);
            try aggregate_map.ensureUnusedCapacity(self.allocator, 1);
        }

        const res = try self.user_profile_service.balanceAdjustment(
            uid,
            currency,
            amount_diff,
            funding_transaction_id,
        );
        if (res == .success) {
            if (aggregate_map.getPtr(currency)) |value| {
                value.* += -amount_diff;
            } else {
                aggregate_map.putAssumeCapacityNoClobber(currency, -amount_diff);
                var old_history = aggregate_history.*;
                aggregate_history.* = staged_history.?;
                staged_history = null;
                old_history.deinit(self.allocator);
            }
        }
        return res;
    }

    fn placeOrderRiskCheck(
        self: *RiskEngine,
        command: *cmd.OrderCommand,
    ) ReleaseError!cmd.CommandResultCode {
        const profile = self.user_profile_service.getUserProfile(command.uid) orelse return .auth_invalid_user;
        const spec = self.symbol_spec_provider.getSymbolSpecification(command.symbol) orelse return .invalid_symbol;

        if (invalidPlaceOrderResult(command)) |result| {
            return result;
        }

        if (self.cfg_ignore_risk_processing) {
            return .valid_for_matching_engine;
        }

        const result = try self.placeOrder(command, profile, spec);
        if (result != .valid_for_matching_engine) {
            return .risk_nsf;
        }
        return result;
    }

    fn placeOrder(
        self: *RiskEngine,
        command: *cmd.OrderCommand,
        profile: *user.UserProfile,
        spec: common.CoreSymbolSpecification,
    ) ReleaseError!cmd.CommandResultCode {
        switch (spec.symbol_type) {
            .currency_exchange_pair => return try self.placeExchangeOrder(command, profile, spec),
            .futures_contract => {
                if (!self.cfg_margin_trading_enabled) {
                    return .risk_margin_trading_disabled;
                }

                const action = command.action orelse return .matching_unsupported_command;
                var next_positions_history = try profile.positions_history.clone(self.allocator);
                defer next_positions_history.deinit(self.allocator);
                try next_positions_history.recordPut(self.allocator, spec.symbol_id);
                var next_accounts_history = try profile.accounts_history.clone(self.allocator);
                defer next_accounts_history.deinit(self.allocator);
                try next_accounts_history.recordPut(self.allocator, spec.quote_currency);

                const inserted = !profile.positions.contains(spec.symbol_id);
                if (inserted) try profile.positions.ensureUnusedCapacity(self.allocator, 1);
                if (!profile.accounts.contains(spec.quote_currency)) {
                    try profile.accounts.ensureUnusedCapacity(self.allocator, 1);
                }

                if (inserted) {
                    profile.positions.putAssumeCapacityNoClobber(
                        spec.symbol_id,
                        user.SymbolPositionRecord.init(
                            profile.uid,
                            spec.symbol_id,
                            spec.quote_currency,
                        ),
                    );
                }
                errdefer if (inserted) {
                    _ = profile.positions.fetchRemove(spec.symbol_id);
                };
                const position = profile.positions.getPtr(spec.symbol_id) orelse
                    return error.PositionRecordCorrupted;

                if (try self.canPlaceMarginOrder(command, profile, spec, position)) {
                    try position.pendingHold(action, command.size);
                    var old_positions_history = profile.positions_history;
                    profile.positions_history = next_positions_history;
                    next_positions_history = int_map_order.History.initUnavailable();
                    old_positions_history.deinit(self.allocator);
                    return .valid_for_matching_engine;
                }

                if (position.isEmpty()) {
                    const position_symbol = position.symbol;
                    const position_currency = position.currency;
                    const position_profit = position.profit;
                    const updated_balance = checkedAdd(
                        profile.accountBalance(position_currency),
                        position_profit,
                    ) orelse return error.ArithmeticOverflow;
                    if (profile.accounts.getPtr(position_currency)) |account| {
                        account.* = updated_balance;
                    } else {
                        profile.accounts.putAssumeCapacityNoClobber(
                            position_currency,
                            updated_balance,
                        );
                    }
                    _ = profile.positions.fetchRemove(position_symbol);
                    next_positions_history.recordRemove(position_symbol);

                    var old_accounts_history = profile.accounts_history;
                    profile.accounts_history = next_accounts_history;
                    next_accounts_history = int_map_order.History.initUnavailable();
                    old_accounts_history.deinit(self.allocator);
                    var old_positions_history = profile.positions_history;
                    profile.positions_history = next_positions_history;
                    next_positions_history = int_map_order.History.initUnavailable();
                    old_positions_history.deinit(self.allocator);
                }
                return .risk_nsf;
            },
            else => return .unsupported_symbol_type,
        }
    }

    fn placeExchangeOrder(
        self: *RiskEngine,
        command: *cmd.OrderCommand,
        profile: *user.UserProfile,
        spec: common.CoreSymbolSpecification,
    ) ReleaseError!cmd.CommandResultCode {
        const order_type = command.order_type orelse return .matching_unsupported_command;

        const currency: i32 = if (command.action == .bid) spec.quote_currency else spec.base_currency;
        const free_futures_margin = if (self.cfg_margin_trading_enabled)
            try self.freeFuturesMarginForCurrency(profile, currency)
        else
            0;
        const hold_amount: i64 = if (command.action == .bid) blk: {
            if (order_type == .fok_budget or order_type == .ioc_budget) {
                if (command.reserve_bid_price != command.price) {
                    return .risk_invalid_reserve_bid_price;
                }
                break :blk calcAmountBidTakerFeeForBudget(command.size, command.price, spec) orelse
                    return .risk_nsf;
            } else {
                if (command.reserve_bid_price < command.price) {
                    return .risk_invalid_reserve_bid_price;
                }
                break :blk calcAmountBidTakerFee(command.size, command.reserve_bid_price, spec) orelse return .risk_nsf;
            }
        } else blk: {
            const scaled_price = @as(i128, command.price) * @as(i128, spec.quote_scale_k);
            if (scaled_price < spec.taker_fee) {
                return .risk_ask_price_lower_than_fee;
            }
            break :blk calcAmountAsk(command.size, spec) orelse return .risk_nsf;
        };

        const old_balance = profile.accountBalance(currency);
        const new_balance = checkedSub(old_balance, hold_amount) orelse return error.ArithmeticOverflow;
        const available = checkedAddI128(@as(i128, new_balance), free_futures_margin) orelse
            return error.ArithmeticOverflow;
        if (available < 0) {
            // Eclipse Collections addToValue inserts an absent key before returning
            // the speculative negative balance. Java then rolls the hold back to zero,
            // leaving that currency as an active zero-valued account entry. Perform
            // every fallible calculation before reproducing that observable side
            // effect, so an allocation failure cannot leave a partial balance change.
            if (!profile.accounts.contains(currency)) {
                try profile.putAccount(self.allocator, currency, 0);
            }
            return .risk_nsf;
        }

        if (profile.accounts.getPtr(currency)) |balance| {
            balance.* = new_balance;
        } else {
            try profile.putAccount(self.allocator, currency, new_balance);
        }

        return .valid_for_matching_engine;
    }

    fn canPlaceMarginOrder(
        self: *const RiskEngine,
        command: *const cmd.OrderCommand,
        profile: *const user.UserProfile,
        spec: common.CoreSymbolSpecification,
        position: *const user.SymbolPositionRecord,
    ) ReleaseError!bool {
        const action = command.action orelse return false;
        const new_required_margin = try position.calculateRequiredMarginForOrder(spec, action, command.size);
        if (new_required_margin == -1) {
            // Java parity: orders that do not increase exposure never need extra collateral.
            return true;
        }

        var free_margin = @as(i128, try self.estimatePositionProfit(position, spec));
        var positions = profile.positions.valueIterator();
        while (positions.next()) |record| {
            if (record.symbol == spec.symbol_id or record.currency != spec.quote_currency) {
                continue;
            }

            const other_spec = self.symbol_spec_provider.getSymbolSpecification(record.symbol) orelse
                return error.PositionSymbolSpecificationMissing;
            const profit = try self.estimatePositionProfit(record, other_spec);
            const required_margin = try record.calculateRequiredMarginForFutures(other_spec);
            const contribution = checkedSubI128(@as(i128, profit), @as(i128, required_margin)) orelse
                return error.ArithmeticOverflow;
            free_margin = checkedAddI128(free_margin, contribution) orelse return error.ArithmeticOverflow;
        }

        const available = checkedAddI128(
            @as(i128, profile.accountBalance(position.currency)),
            free_margin,
        ) orelse return error.ArithmeticOverflow;
        return @as(i128, new_required_margin) <= available;
    }

    fn freeFuturesMarginForCurrency(
        self: *const RiskEngine,
        profile: *const user.UserProfile,
        currency: i32,
    ) ReleaseError!i128 {
        var free_margin: i128 = 0;
        var positions = profile.positions.valueIterator();
        while (positions.next()) |record| {
            if (record.currency != currency) continue;

            const position_spec = self.symbol_spec_provider.getSymbolSpecification(record.symbol) orelse
                return error.PositionSymbolSpecificationMissing;
            const profit = try self.estimatePositionProfit(record, position_spec);
            const required_margin = try record.calculateRequiredMarginForFutures(position_spec);
            const contribution = checkedSubI128(@as(i128, profit), @as(i128, required_margin)) orelse
                return error.ArithmeticOverflow;
            free_margin = checkedAddI128(free_margin, contribution) orelse return error.ArithmeticOverflow;
        }
        return free_margin;
    }

    fn estimatePositionProfit(
        self: *const RiskEngine,
        position: *const user.SymbolPositionRecord,
        spec: common.CoreSymbolSpecification,
    ) user.MarginMathError!i64 {
        const last_price = self.last_price_cache.get(position.symbol);
        return position.estimateProfit(
            spec,
            if (last_price) |price| price.bid_price else null,
            if (last_price) |price| price.ask_price else null,
        );
    }

    pub fn handlerRiskRelease(
        self: *RiskEngine,
        _: u64,
        command: *cmd.OrderCommand,
        event_head: ?*events.MatcherTradeEvent,
        market_data: ?events.L2Top,
    ) ReleaseError!bool {
        const node = event_head;

        if (market_data == null and (node == null or node.?.event_type == .binary_event)) {
            return false;
        }

        const spec = self.symbol_spec_provider.getSymbolSpecification(command.symbol) orelse
            return error.PositionSymbolSpecificationMissing;

        if (spec.symbol_type == .currency_exchange_pair) {
            var prepared = try self.prepareExchangeRiskReleaseForSpec(
                spec,
                command,
                node,
                market_data,
            );
            defer prepared.deinit();
            self.commitPreparedExchangeRiskRelease(&prepared);
            return false;
        } else {
            const taker_action = if (node != null and node.?.event_type != .binary_event)
                command.action orelse return false
            else
                .bid;
            var prepared = try self.prepareMarginRiskReleaseForSpec(
                spec,
                command.uid,
                taker_action,
                node,
                market_data,
            );
            defer prepared.deinit();
            self.commitPreparedMarginRiskRelease(&prepared);
            return false;
        }
    }

    /// Prepares one shard's currency-exchange settlement without mutating engine state.
    /// This mirrors the Java R2 release formulas while making every checked arithmetic
    /// operation and allocation complete before the first observable write.
    pub fn prepareExchangeRiskRelease(
        self: *RiskEngine,
        command: *const cmd.OrderCommand,
        event_head: ?*events.MatcherTradeEvent,
        market_data: ?events.L2Top,
    ) ReleaseError!PreparedExchangeRelease {
        const spec = self.symbol_spec_provider.getSymbolSpecification(command.symbol) orelse
            return error.PositionSymbolSpecificationMissing;
        if (spec.symbol_type != .currency_exchange_pair) {
            return error.UnsupportedExchangeReleaseSymbol;
        }
        return self.prepareExchangeRiskReleaseForSpec(spec, command, event_head, market_data);
    }

    fn prepareExchangeRiskReleaseForSpec(
        self: *RiskEngine,
        spec: common.CoreSymbolSpecification,
        command: *const cmd.OrderCommand,
        event_head: ?*events.MatcherTradeEvent,
        market_data: ?events.L2Top,
    ) ReleaseError!PreparedExchangeRelease {
        try validateBudgetReleaseEventChain(command, event_head);

        var prepared: PreparedExchangeRelease = .{
            .allocator = self.allocator,
            .owner = self,
            .base_currency = spec.base_currency,
            .quote_currency = spec.quote_currency,
        };
        errdefer prepared.deinit();

        var node = if (event_head != null and event_head.?.event_type != .binary_event)
            event_head
        else
            null;
        if (node != null) {
            const taker_action = command.action orelse return error.InvalidExchangeReleaseCommand;
            const is_ioc_budget_bid = taker_action == .bid and
                command.command == .place_order and
                command.order_type == .ioc_budget;
            const taker_for_this_handler = self.uidForThisHandler(command.uid);
            if (taker_for_this_handler) {
                _ = try self.stageExchangeUser(&prepared, command.uid);
            }

            if (node.?.event_type == .reduce or node.?.event_type == .reject) {
                // IOC_BUDGET holds one aggregate quote budget rather than a
                // per-lot reserve. Its unused budget and unfilled-lot fees can
                // only be released after the following trades are aggregated.
                if (taker_for_this_handler and !is_ioc_budget_bid) {
                    const refund = if (taker_action == .ask)
                        (calcAmountAsk(node.?.size, spec) orelse return error.ArithmeticOverflow)
                    else if (command.command == .place_order and command.order_type == .fok_budget)
                        (calcAmountBidTakerFeeForBudget(node.?.size, node.?.price, spec) orelse
                            return error.ArithmeticOverflow)
                    else
                        (calcAmountBidTakerFee(node.?.size, node.?.bidder_hold_price, spec) orelse
                            return error.ArithmeticOverflow);
                    try self.stageExchangeBalanceDelta(
                        &prepared,
                        command.uid,
                        if (taker_action == .ask) spec.base_currency else spec.quote_currency,
                        refund,
                    );
                }
                node = node.?.next_event;
            }

            if (node != null or is_ioc_budget_bid) {
                if (taker_action == .ask) {
                    try self.prepareExchangeSellTrades(
                        &prepared,
                        spec,
                        command.uid,
                        taker_for_this_handler,
                        node,
                    );
                } else {
                    try self.prepareExchangeBuyTrades(
                        &prepared,
                        spec,
                        command,
                        taker_for_this_handler,
                        node,
                    );
                }
            }
        }

        if (market_data) |md| {
            if (self.cfg_margin_trading_enabled) {
                prepared.last_price_update = .{
                    .symbol = spec.symbol_id,
                    .record = .{
                        .ask_price = if (md.ask_size != 0) md.ask_price_0 else std.math.maxInt(i64),
                        .bid_price = if (md.bid_size != 0) md.bid_price_0 else 0,
                    },
                };
            }
        }

        try self.reservePreparedExchangeRelease(&prepared);
        return prepared;
    }

    fn prepareExchangeSellTrades(
        self: *RiskEngine,
        prepared: *PreparedExchangeRelease,
        spec: common.CoreSymbolSpecification,
        taker_uid: i64,
        taker_for_this_handler: bool,
        event_head: ?*events.MatcherTradeEvent,
    ) ReleaseError!void {
        var taker_size: i64 = 0;
        var maker_size: i64 = 0;
        var taker_size_price: i64 = 0;

        var node = event_head;
        while (node) |ev| : (node = ev.next_event) {
            std.debug.assert(ev.event_type == .trade);

            if (taker_for_this_handler) {
                const price_size = checkedMul(ev.size, ev.price) orelse
                    return error.ArithmeticOverflow;
                taker_size_price = checkedAdd(taker_size_price, price_size) orelse
                    return error.ArithmeticOverflow;
                taker_size = checkedAdd(taker_size, ev.size) orelse
                    return error.ArithmeticOverflow;
            }

            if (self.uidForThisHandler(ev.matched_order_uid)) {
                _ = try self.stageExchangeUser(prepared, ev.matched_order_uid);
                const price_diff = checkedSub(ev.bidder_hold_price, ev.price) orelse
                    return error.ArithmeticOverflow;
                const release_quote = calcAmountBidReleaseCorrMaker(ev.size, price_diff, spec) orelse
                    return error.ArithmeticOverflow;
                const gain_base = calcAmountAsk(ev.size, spec) orelse
                    return error.ArithmeticOverflow;
                try self.stageExchangeBalanceDelta(
                    prepared,
                    ev.matched_order_uid,
                    spec.quote_currency,
                    release_quote,
                );
                try self.stageExchangeBalanceDelta(
                    prepared,
                    ev.matched_order_uid,
                    spec.base_currency,
                    gain_base,
                );
                maker_size = checkedAdd(maker_size, ev.size) orelse
                    return error.ArithmeticOverflow;
            }
        }

        if (taker_for_this_handler) {
            const scaled = checkedMul(taker_size_price, spec.quote_scale_k) orelse
                return error.ArithmeticOverflow;
            const fee = checkedMul(spec.taker_fee, taker_size) orelse
                return error.ArithmeticOverflow;
            const taker_delta = checkedSub(scaled, fee) orelse
                return error.ArithmeticOverflow;
            try self.stageExchangeBalanceDelta(
                prepared,
                taker_uid,
                spec.quote_currency,
                taker_delta,
            );
        }

        try self.prepareExchangeFeeTotal(prepared, spec, taker_size, maker_size);
    }

    fn prepareExchangeBuyTrades(
        self: *RiskEngine,
        prepared: *PreparedExchangeRelease,
        spec: common.CoreSymbolSpecification,
        command: *const cmd.OrderCommand,
        taker_for_this_handler: bool,
        event_head: ?*events.MatcherTradeEvent,
    ) ReleaseError!void {
        var taker_size: i64 = 0;
        var maker_size: i64 = 0;
        var taker_size_price_sum: i64 = 0;
        var taker_size_price_held_sum: i64 = 0;

        var node = event_head;
        while (node) |ev| : (node = ev.next_event) {
            std.debug.assert(ev.event_type == .trade);

            if (taker_for_this_handler) {
                const matched_quote = checkedMul(ev.size, ev.price) orelse
                    return error.ArithmeticOverflow;
                const held_quote = checkedMul(ev.size, ev.bidder_hold_price) orelse
                    return error.ArithmeticOverflow;
                taker_size_price_sum = checkedAdd(taker_size_price_sum, matched_quote) orelse
                    return error.ArithmeticOverflow;
                taker_size_price_held_sum = checkedAdd(taker_size_price_held_sum, held_quote) orelse
                    return error.ArithmeticOverflow;
                taker_size = checkedAdd(taker_size, ev.size) orelse
                    return error.ArithmeticOverflow;
            }

            if (self.uidForThisHandler(ev.matched_order_uid)) {
                _ = try self.stageExchangeUser(prepared, ev.matched_order_uid);
                const gained_quote = calcAmountBid(ev.size, ev.price, spec) orelse
                    return error.ArithmeticOverflow;
                const maker_fee = checkedMul(spec.maker_fee, ev.size) orelse
                    return error.ArithmeticOverflow;
                const maker_delta = checkedSub(gained_quote, maker_fee) orelse
                    return error.ArithmeticOverflow;
                try self.stageExchangeBalanceDelta(
                    prepared,
                    ev.matched_order_uid,
                    spec.quote_currency,
                    maker_delta,
                );
                maker_size = checkedAdd(maker_size, ev.size) orelse
                    return error.ArithmeticOverflow;
            }
        }

        if (taker_for_this_handler) {
            const quote_delta = if (command.command == .place_order and command.order_type == .ioc_budget) quote_delta: {
                const unused_budget_steps = checkedSub(command.price, taker_size_price_sum) orelse
                    return error.ArithmeticOverflow;
                const unfilled_size = checkedSub(command.size, taker_size) orelse
                    return error.ArithmeticOverflow;
                if (unused_budget_steps < 0 or unfilled_size < 0) {
                    return error.InvalidExchangeReleaseCommand;
                }
                const unused_budget = checkedMul(unused_budget_steps, spec.quote_scale_k) orelse
                    return error.ArithmeticOverflow;
                const unused_fee = checkedMul(unfilled_size, spec.taker_fee) orelse
                    return error.ArithmeticOverflow;
                break :quote_delta checkedAdd(unused_budget, unused_fee) orelse
                    return error.ArithmeticOverflow;
            } else quote_delta: {
                if (command.command == .place_order and command.order_type == .fok_budget) {
                    // FOK_BUDGET also holds one aggregate quote budget, but it
                    // either fills completely or emits a standalone reject.
                    taker_size_price_held_sum = command.price;
                }
                const quote_delta_steps = checkedSub(
                    taker_size_price_held_sum,
                    taker_size_price_sum,
                ) orelse return error.ArithmeticOverflow;
                break :quote_delta checkedMul(quote_delta_steps, spec.quote_scale_k) orelse
                    return error.ArithmeticOverflow;
            };
            const base_delta = checkedMul(taker_size, spec.base_scale_k) orelse
                return error.ArithmeticOverflow;
            try self.stageExchangeBalanceDelta(
                prepared,
                command.uid,
                spec.quote_currency,
                quote_delta,
            );
            try self.stageExchangeBalanceDelta(
                prepared,
                command.uid,
                spec.base_currency,
                base_delta,
            );
        }

        try self.prepareExchangeFeeTotal(prepared, spec, taker_size, maker_size);
    }

    fn prepareExchangeFeeTotal(
        self: *RiskEngine,
        prepared: *PreparedExchangeRelease,
        spec: common.CoreSymbolSpecification,
        taker_size: i64,
        maker_size: i64,
    ) ReleaseError!void {
        if (taker_size == 0 and maker_size == 0) return;

        const taker_fee = checkedMul(spec.taker_fee, taker_size) orelse
            return error.ArithmeticOverflow;
        const maker_fee = checkedMul(spec.maker_fee, maker_size) orelse
            return error.ArithmeticOverflow;
        const total_fee = checkedAdd(taker_fee, maker_fee) orelse
            return error.ArithmeticOverflow;
        prepared.next_fee = checkedAdd(
            self.fees.get(spec.quote_currency) orelse 0,
            total_fee,
        ) orelse return error.ArithmeticOverflow;
        if (prepared.fee_history == null) {
            prepared.fee_history = try self.fees_history.clone(self.allocator);
            try prepared.fee_history.?.recordPut(self.allocator, spec.quote_currency);
        }
        prepared.fee_touched = true;
    }

    fn stageExchangeUser(
        self: *RiskEngine,
        prepared: *PreparedExchangeRelease,
        uid: i64,
    ) ReleaseError!*StagedExchangeUser {
        if (prepared.staged_users.getPtr(uid)) |staged| return staged;

        const profile = self.user_profile_service.getUserProfile(uid);
        const base_balance = if (profile) |existing|
            existing.accountBalance(prepared.base_currency)
        else
            0;
        const quote_balance = if (profile) |existing|
            existing.accountBalance(prepared.quote_currency)
        else
            0;
        var accounts_history = if (profile) |existing|
            try existing.accounts_history.clone(self.allocator)
        else
            int_map_order.History.init();
        var accounts_history_moved = false;
        errdefer if (!accounts_history_moved) accounts_history.deinit(self.allocator);
        try prepared.staged_users.put(self.allocator, uid, .{
            .uid = uid,
            .base_balance = base_balance,
            .quote_balance = quote_balance,
            .accounts_history = accounts_history,
            .new_profile = if (profile == null)
                user.UserProfile.init(uid, .suspended)
            else
                null,
        });
        accounts_history_moved = true;
        try prepared.user_order.append(self.allocator, uid);
        if (profile == null) {
            prepared.missing_user_count += 1;
            if (prepared.user_profiles_history == null) {
                prepared.user_profiles_history =
                    try self.user_profile_service.user_profiles_history.clone(self.allocator);
            }
            try prepared.user_profiles_history.?.recordPut(self.allocator, uid);
        }
        return prepared.staged_users.getPtr(uid) orelse return error.PositionRecordCorrupted;
    }

    fn stageExchangeBalanceDelta(
        self: *RiskEngine,
        prepared: *PreparedExchangeRelease,
        uid: i64,
        currency: i32,
        delta: i64,
    ) ReleaseError!void {
        const staged = try self.stageExchangeUser(prepared, uid);
        try staged.accounts_history.recordPut(self.allocator, currency);
        if (currency == prepared.base_currency) {
            const next_balance = checkedAdd(staged.base_balance, delta) orelse
                return error.ArithmeticOverflow;
            if (staged.new_profile) |*profile| {
                if (profile.accounts.getPtr(currency)) |balance| {
                    balance.* = next_balance;
                } else {
                    try profile.accounts.put(self.allocator, currency, next_balance);
                }
            }
            staged.base_balance = next_balance;
            staged.base_touched = true;
            return;
        }

        std.debug.assert(currency == prepared.quote_currency);
        const next_balance = checkedAdd(staged.quote_balance, delta) orelse
            return error.ArithmeticOverflow;
        if (staged.new_profile) |*profile| {
            if (profile.accounts.getPtr(currency)) |balance| {
                balance.* = next_balance;
            } else {
                try profile.accounts.put(self.allocator, currency, next_balance);
            }
        }
        staged.quote_balance = next_balance;
        staged.quote_touched = true;
    }

    fn reservePreparedExchangeRelease(
        self: *RiskEngine,
        prepared: *PreparedExchangeRelease,
    ) ReleaseError!void {
        if (prepared.missing_user_count != 0) {
            try self.user_profile_service.user_profiles.ensureUnusedCapacity(
                self.allocator,
                prepared.missing_user_count,
            );
        }

        for (prepared.user_order.items) |uid| {
            const staged = prepared.staged_users.getPtr(uid) orelse
                return error.PositionRecordCorrupted;
            if (staged.new_profile != null) {
                continue;
            }

            const profile = self.user_profile_service.getUserProfile(staged.uid) orelse
                return error.PositionRecordCorrupted;
            var missing_accounts: u32 = 0;
            if (staged.base_touched and !profile.accounts.contains(prepared.base_currency)) {
                missing_accounts += 1;
            }
            if (staged.quote_touched and !profile.accounts.contains(prepared.quote_currency)) {
                missing_accounts += 1;
            }
            if (missing_accounts != 0) {
                try profile.accounts.ensureUnusedCapacity(self.allocator, missing_accounts);
            }
        }

        if (prepared.fee_touched and !self.fees.contains(prepared.quote_currency)) {
            try self.fees.ensureUnusedCapacity(self.allocator, 1);
        }
        if (prepared.last_price_update) |update| {
            if (!self.last_price_cache.contains(update.symbol)) {
                try self.last_price_cache.ensureUnusedCapacity(self.allocator, 1);
            }
        }
    }

    /// Commits a prepared currency-exchange release without allocation or arithmetic.
    pub fn commitPreparedExchangeRiskRelease(
        self: *RiskEngine,
        prepared: *PreparedExchangeRelease,
    ) void {
        std.debug.assert(prepared.owner == self);
        std.debug.assert(!prepared.committed);

        for (prepared.user_order.items) |uid| {
            const staged = prepared.staged_users.getPtr(uid) orelse unreachable;
            if (staged.new_profile) |new_profile| {
                var moved_profile = new_profile;
                moved_profile.accounts_history.deinit(self.allocator);
                moved_profile.accounts_history = staged.accounts_history;
                staged.history_moved = true;
                self.user_profile_service.user_profiles.putAssumeCapacityNoClobber(
                    staged.uid,
                    moved_profile,
                );
                staged.new_profile = null;
                continue;
            }

            const profile = self.user_profile_service.getUserProfile(staged.uid) orelse unreachable;
            if (staged.base_touched) {
                if (profile.accounts.getPtr(prepared.base_currency)) |balance| {
                    balance.* = staged.base_balance;
                } else {
                    profile.accounts.putAssumeCapacityNoClobber(
                        prepared.base_currency,
                        staged.base_balance,
                    );
                }
            }
            if (staged.quote_touched) {
                if (profile.accounts.getPtr(prepared.quote_currency)) |balance| {
                    balance.* = staged.quote_balance;
                } else {
                    profile.accounts.putAssumeCapacityNoClobber(
                        prepared.quote_currency,
                        staged.quote_balance,
                    );
                }
            }
            var old_history = profile.accounts_history;
            profile.accounts_history = staged.accounts_history;
            staged.history_moved = true;
            old_history.deinit(self.allocator);
        }

        if (prepared.fee_touched) {
            if (self.fees.getPtr(prepared.quote_currency)) |fee| {
                fee.* = prepared.next_fee;
            } else {
                self.fees.putAssumeCapacityNoClobber(prepared.quote_currency, prepared.next_fee);
            }
            var old_history = self.fees_history;
            self.fees_history = prepared.fee_history.?;
            prepared.fee_history_moved = true;
            old_history.deinit(self.allocator);
        }
        if (prepared.user_profiles_history) |history| {
            var old_history = self.user_profile_service.user_profiles_history;
            self.user_profile_service.user_profiles_history = history;
            prepared.user_profiles_history_moved = true;
            old_history.deinit(self.allocator);
        }
        if (prepared.last_price_update) |update| {
            if (self.last_price_cache.getPtr(update.symbol)) |record| {
                record.* = update.record;
            } else {
                self.last_price_cache.putAssumeCapacityNoClobber(update.symbol, update.record);
            }
        }

        prepared.committed = true;
    }

    /// Prepares one shard's margin release without mutating engine state. Callers may
    /// prepare every risk shard first, abort all prepared values with `deinit` on error,
    /// and then commit each value with the infallible commit API.
    pub fn prepareMarginRiskRelease(
        self: *RiskEngine,
        command: *const cmd.OrderCommand,
        event_head: ?*events.MatcherTradeEvent,
        market_data: ?events.L2Top,
    ) ReleaseError!PreparedMarginRelease {
        const spec = self.symbol_spec_provider.getSymbolSpecification(command.symbol) orelse
            return error.PositionSymbolSpecificationMissing;
        if (spec.symbol_type == .currency_exchange_pair) {
            return error.UnsupportedMarginReleaseSymbol;
        }
        const taker_action = if (event_head != null and event_head.?.event_type != .binary_event)
            command.action orelse return error.InvalidMarginReleaseCommand
        else
            .bid;
        return self.prepareMarginRiskReleaseForSpec(
            spec,
            command.uid,
            taker_action,
            event_head,
            market_data,
        );
    }

    fn prepareMarginRiskReleaseForSpec(
        self: *RiskEngine,
        spec: common.CoreSymbolSpecification,
        taker_uid: i64,
        taker_action: common.OrderAction,
        event_head: ?*events.MatcherTradeEvent,
        market_data: ?events.L2Top,
    ) ReleaseError!PreparedMarginRelease {
        var prepared: PreparedMarginRelease = .{
            .allocator = self.allocator,
            .owner = self,
            .quote_currency = spec.quote_currency,
        };
        errdefer prepared.deinit();

        const taker_for_this_handler = self.uidForThisHandler(taker_uid);
        if (taker_for_this_handler and event_head != null and event_head.?.event_type != .binary_event) {
            _ = try self.stageMarginUser(&prepared, taker_uid, spec);
        }

        var fee_delta: i64 = 0;
        // A binary head is a transport marker, not an order event. Java skips the
        // complete event path in this case while still accepting attached market data.
        var node = if (event_head != null and event_head.?.event_type != .binary_event)
            event_head
        else
            null;
        while (node) |ev| : (node = ev.next_event) {
            if (taker_for_this_handler) {
                const staged_taker = prepared.staged_users.getPtr(taker_uid) orelse
                    return error.PositionRecordCorrupted;
                switch (ev.event_type) {
                    .trade => {
                        const size_open = try staged_taker.position.updatePositionForMarginTrade(
                            taker_action,
                            ev.size,
                            ev.price,
                        );
                        const fee = checkedMul(spec.taker_fee, size_open) orelse
                            return error.ArithmeticOverflow;
                        staged_taker.account_balance = checkedSub(staged_taker.account_balance, fee) orelse
                            return error.ArithmeticOverflow;
                        staged_taker.account_touched = true;
                        fee_delta = checkedAdd(fee_delta, fee) orelse return error.ArithmeticOverflow;
                        prepared.fee_touched = true;
                    },
                    .reject, .reduce => try staged_taker.position.pendingRelease(taker_action, ev.size),
                    .binary_event => {},
                }
            }

            if (ev.event_type == .trade and self.uidForThisHandler(ev.matched_order_uid)) {
                const staged_maker = try self.stageMarginUser(
                    &prepared,
                    ev.matched_order_uid,
                    spec,
                );
                const size_open = try staged_maker.position.updatePositionForMarginTrade(
                    taker_action.opposite(),
                    ev.size,
                    ev.price,
                );
                const fee = checkedMul(spec.maker_fee, size_open) orelse
                    return error.ArithmeticOverflow;
                staged_maker.account_balance = checkedSub(staged_maker.account_balance, fee) orelse
                    return error.ArithmeticOverflow;
                staged_maker.account_touched = true;
                fee_delta = checkedAdd(fee_delta, fee) orelse return error.ArithmeticOverflow;
                prepared.fee_touched = true;
            }
        }

        for (prepared.user_order.items) |uid| {
            const staged = prepared.staged_users.getPtr(uid) orelse
                return error.PositionRecordCorrupted;
            if (staged.position.isEmpty()) {
                staged.account_balance = checkedAdd(staged.account_balance, staged.position.profit) orelse
                    return error.ArithmeticOverflow;
                staged.account_touched = true;
                staged.remove_position = true;
            }
            if (staged.account_touched) {
                try staged.accounts_history.recordPut(self.allocator, spec.quote_currency);
            }
            if (staged.remove_position) {
                staged.positions_history.recordRemove(spec.symbol_id);
            }
        }

        prepared.next_fee = if (prepared.fee_touched)
            checkedAdd(self.fees.get(spec.quote_currency) orelse 0, fee_delta) orelse
                return error.ArithmeticOverflow
        else
            0;
        if (prepared.fee_touched) {
            prepared.fee_history = try self.fees_history.clone(self.allocator);
            try prepared.fee_history.?.recordPut(self.allocator, spec.quote_currency);
        }

        if (market_data) |md| {
            if (self.cfg_margin_trading_enabled) {
                prepared.last_price_update = .{
                    .symbol = spec.symbol_id,
                    .record = .{
                        .ask_price = if (md.ask_size != 0) md.ask_price_0 else std.math.maxInt(i64),
                        .bid_price = if (md.bid_size != 0) md.bid_price_0 else 0,
                    },
                };
            }
        }

        // Reserve every potentially missing entry before committing logical state. Capacity
        // growth may allocate, but every operation after this section is infallible.
        for (prepared.user_order.items) |uid| {
            const staged = prepared.staged_users.getPtr(uid) orelse
                return error.PositionRecordCorrupted;
            if (staged.account_touched and !staged.profile.accounts.contains(spec.quote_currency)) {
                try staged.profile.accounts.ensureUnusedCapacity(self.allocator, 1);
            }
        }
        if (prepared.fee_touched and !self.fees.contains(spec.quote_currency)) {
            try self.fees.ensureUnusedCapacity(self.allocator, 1);
        }
        if (prepared.last_price_update) |update| {
            if (!self.last_price_cache.contains(update.symbol)) {
                try self.last_price_cache.ensureUnusedCapacity(self.allocator, 1);
            }
        }

        return prepared;
    }

    /// Commits a prepared margin release. This function is deliberately infallible so
    /// multi-shard callers can prepare all shards before making any logical mutation.
    pub fn commitPreparedMarginRiskRelease(
        self: *RiskEngine,
        prepared: *PreparedMarginRelease,
    ) void {
        std.debug.assert(prepared.owner == self);
        std.debug.assert(!prepared.committed);

        for (prepared.user_order.items) |uid| {
            const staged = prepared.staged_users.getPtr(uid) orelse unreachable;
            if (staged.account_touched) {
                if (staged.profile.accounts.getPtr(prepared.quote_currency)) |balance| {
                    balance.* = staged.account_balance;
                } else {
                    staged.profile.accounts.putAssumeCapacityNoClobber(
                        prepared.quote_currency,
                        staged.account_balance,
                    );
                }
            }

            if (staged.remove_position) {
                const removed = staged.profile.positions.fetchRemove(staged.position.symbol);
                std.debug.assert(removed != null);
            } else {
                const position = staged.profile.positions.getPtr(staged.position.symbol) orelse unreachable;
                position.* = staged.position;
            }
            var old_accounts_history = staged.profile.accounts_history;
            staged.profile.accounts_history = staged.accounts_history;
            old_accounts_history.deinit(self.allocator);
            var old_positions_history = staged.profile.positions_history;
            staged.profile.positions_history = staged.positions_history;
            old_positions_history.deinit(self.allocator);
            staged.histories_moved = true;
        }

        if (prepared.fee_touched) {
            if (self.fees.getPtr(prepared.quote_currency)) |fee| {
                fee.* = prepared.next_fee;
            } else {
                self.fees.putAssumeCapacityNoClobber(prepared.quote_currency, prepared.next_fee);
            }
            var old_history = self.fees_history;
            self.fees_history = prepared.fee_history.?;
            prepared.fee_history_moved = true;
            old_history.deinit(self.allocator);
        }

        if (prepared.last_price_update) |update| {
            if (self.last_price_cache.getPtr(update.symbol)) |record| {
                record.* = update.record;
            } else {
                self.last_price_cache.putAssumeCapacityNoClobber(update.symbol, update.record);
            }
        }

        prepared.committed = true;
    }

    fn stageMarginUser(
        self: *RiskEngine,
        prepared: *PreparedMarginRelease,
        uid: i64,
        spec: common.CoreSymbolSpecification,
    ) ReleaseError!*StagedMarginUser {
        if (prepared.staged_users.getPtr(uid)) |staged| {
            return staged;
        }

        const profile = self.user_profile_service.getUserProfile(uid) orelse
            return error.PositionRecordCorrupted;
        const position = profile.positions.get(spec.symbol_id) orelse
            return error.PositionRecordCorrupted;
        var accounts_history = try profile.accounts_history.clone(self.allocator);
        var accounts_history_moved = false;
        errdefer if (!accounts_history_moved) accounts_history.deinit(self.allocator);
        var positions_history = try profile.positions_history.clone(self.allocator);
        var positions_history_moved = false;
        errdefer if (!positions_history_moved) positions_history.deinit(self.allocator);
        try prepared.staged_users.put(self.allocator, uid, .{
            .profile = profile,
            .position = position,
            .account_balance = profile.accountBalance(spec.quote_currency),
            .accounts_history = accounts_history,
            .positions_history = positions_history,
        });
        accounts_history_moved = true;
        positions_history_moved = true;
        try prepared.user_order.append(self.allocator, uid);
        return prepared.staged_users.getPtr(uid) orelse return error.PositionRecordCorrupted;
    }

    pub fn reset(self: *RiskEngine) void {
        self.user_profile_service.reset();
        self.symbol_spec_provider.reset();
        self.last_price_cache.clearRetainingCapacity();
        self.fees.clearRetainingCapacity();
        self.fees_history.clear();
        self.adjustments.clearRetainingCapacity();
        self.adjustments_history.clear();
        self.suspends.clearRetainingCapacity();
        self.suspends_history.clear();
    }
};

fn lessThanI32(_: void, lhs: i32, rhs: i32) bool {
    return lhs < rhs;
}

fn hashI8(hasher: *std.hash.Wyhash, value: i8) void {
    var n = value;
    hasher.update(std.mem.asBytes(&n));
}

fn hashI32(hasher: *std.hash.Wyhash, value: i32) void {
    var n = value;
    hasher.update(std.mem.asBytes(&n));
}

fn hashI64(hasher: *std.hash.Wyhash, value: i64) void {
    var n = value;
    hasher.update(std.mem.asBytes(&n));
}

fn hashU64(hasher: *std.hash.Wyhash, value: u64) void {
    var n = value;
    hasher.update(std.mem.asBytes(&n));
}

fn hashBool(hasher: *std.hash.Wyhash, value: bool) void {
    hashI8(hasher, if (value) 1 else 0);
}

fn hashCoreSymbolSpecification(hasher: *std.hash.Wyhash, spec: common.CoreSymbolSpecification) void {
    hashI32(hasher, spec.symbol_id);
    hashI8(hasher, @intFromEnum(spec.symbol_type));
    hashI32(hasher, spec.base_currency);
    hashI32(hasher, spec.quote_currency);
    hashI64(hasher, spec.base_scale_k);
    hashI64(hasher, spec.quote_scale_k);
    hashI64(hasher, spec.taker_fee);
    hashI64(hasher, spec.maker_fee);
    hashI64(hasher, spec.margin_buy);
    hashI64(hasher, spec.margin_sell);
}

fn hashI32I64Map(
    allocator: std.mem.Allocator,
    map: *const std.AutoHashMapUnmanaged(i32, i64),
    hasher: *std.hash.Wyhash,
) StateHashError!void {
    var keys: std.ArrayListUnmanaged(i32) = .empty;
    defer keys.deinit(allocator);

    try keys.ensureTotalCapacityPrecise(allocator, map.count());
    var it = map.iterator();
    while (it.next()) |entry| {
        keys.appendAssumeCapacity(entry.key_ptr.*);
    }
    std.sort.heap(i32, keys.items, {}, lessThanI32);

    for (keys.items) |key| {
        const value = map.get(key) orelse return error.StateHashCorrupted;
        hashI32(hasher, key);
        hashI64(hasher, value);
    }
}

fn hashLastPriceMap(
    allocator: std.mem.Allocator,
    map: *const std.AutoHashMapUnmanaged(i32, RiskEngine.LastPriceCacheRecord),
    hasher: *std.hash.Wyhash,
) StateHashError!void {
    var keys: std.ArrayListUnmanaged(i32) = .empty;
    defer keys.deinit(allocator);

    try keys.ensureTotalCapacityPrecise(allocator, map.count());
    var it = map.iterator();
    while (it.next()) |entry| {
        keys.appendAssumeCapacity(entry.key_ptr.*);
    }
    std.sort.heap(i32, keys.items, {}, lessThanI32);

    for (keys.items) |key| {
        const value = map.get(key) orelse return error.StateHashCorrupted;
        hashI32(hasher, key);
        hashI64(hasher, value.ask_price);
        hashI64(hasher, value.bid_price);
    }
}

fn validatedIntLongHistoryKeys(
    allocator: std.mem.Allocator,
    history: *const int_map_order.History,
    map: *const std.AutoHashMapUnmanaged(i32, i64),
) RiskEngine.HistoryValidationError![]i32 {
    const keys = try history.orderedKeys(allocator);
    errdefer allocator.free(keys);
    if (keys.len != map.count()) return error.HistorySemanticKeySetMismatch;
    for (keys) |key| {
        if (!map.contains(key)) return error.HistorySemanticKeySetMismatch;
    }
    return keys;
}

fn replaceValidatedIntLongHistory(
    allocator: std.mem.Allocator,
    current: *int_map_order.History,
    next: *int_map_order.History,
    map: *const std.AutoHashMapUnmanaged(i32, i64),
) RiskEngine.HistoryValidationError!void {
    const keys = try validatedIntLongHistoryKeys(allocator, next, map);
    allocator.free(keys);
    var old = current.*;
    current.* = next.*;
    next.* = int_map_order.History.initUnavailable();
    old.deinit(allocator);
}

fn markHistoryUnavailable(
    allocator: std.mem.Allocator,
    history: *int_map_order.History,
) void {
    if (!history.isKnown()) return;
    history.deinit(allocator);
    history.* = int_map_order.History.initUnavailable();
}

fn checkedMul(a: i64, b: i64) ?i64 {
    const pair = @mulWithOverflow(a, b);
    return if (pair[1] == 0) pair[0] else null;
}

fn checkedAdd(a: i64, b: i64) ?i64 {
    const pair = @addWithOverflow(a, b);
    return if (pair[1] == 0) pair[0] else null;
}

fn checkedSub(a: i64, b: i64) ?i64 {
    const pair = @subWithOverflow(a, b);
    return if (pair[1] == 0) pair[0] else null;
}

fn checkedAddI128(a: i128, b: i128) ?i128 {
    const pair = @addWithOverflow(a, b);
    return if (pair[1] == 0) pair[0] else null;
}

fn checkedSubI128(a: i128, b: i128) ?i128 {
    const pair = @subWithOverflow(a, b);
    return if (pair[1] == 0) pair[0] else null;
}

fn invalidPlaceOrderResult(command: *const cmd.OrderCommand) ?cmd.CommandResultCode {
    // Java trusts the API boundary here. The Zig core also accepts decoded and
    // replayed commands directly, so reject values that could turn an R1 hold
    // negative or create a hold that matching can never release.
    const action = command.action orelse return .matching_unsupported_command;
    _ = command.order_type orelse return .matching_unsupported_command;

    if (command.size <= 0) return .matching_unsupported_command;
    if (command.price < 0) return .invalid_price_step;
    if (action == .bid and command.reserve_bid_price < 0) {
        return .risk_invalid_reserve_bid_price;
    }
    return null;
}

/// Budget-order release events are matching-engine invariants, not untrusted
/// accounting hints. Validate them before staging any balance, fee, or user
/// mutation so an impossible aggregate fails the command boundary atomically.
fn validateBudgetReleaseEventChain(
    command: *const cmd.OrderCommand,
    event_head: ?*const events.MatcherTradeEvent,
) ReleaseError!void {
    if (command.command != .place_order) return;
    const order_type = command.order_type orelse return error.InvalidExchangeReleaseCommand;

    if (order_type == .fok_budget) {
        if (command.size <= 0) return error.InvalidExchangeReleaseCommand;
        const head = event_head orelse return;

        if (head.event_type == .reject) {
            if (head.size != command.size or head.next_event != null) {
                return error.InvalidExchangeReleaseCommand;
            }
            return;
        }

        var filled: i64 = 0;
        var node: ?*const events.MatcherTradeEvent = head;
        while (node) |event| : (node = event.next_event) {
            if (event.event_type != .trade or event.size <= 0) {
                return error.InvalidExchangeReleaseCommand;
            }
            filled = checkedAdd(filled, event.size) orelse return error.ArithmeticOverflow;
        }
        if (filled != command.size) return error.InvalidExchangeReleaseCommand;
        return;
    }

    if (order_type != .ioc_budget or command.action != .bid) return;
    if (command.size <= 0 or command.price < 0) return error.InvalidExchangeReleaseCommand;

    var traded_size: i64 = 0;
    var traded_budget: i64 = 0;
    var node = event_head;
    while (node) |event| : (node = event.next_event) {
        switch (event.event_type) {
            .trade => {
                if (event.size <= 0 or event.price < 0) {
                    return error.InvalidExchangeReleaseCommand;
                }
                const event_budget = checkedMul(event.size, event.price) orelse
                    return error.ArithmeticOverflow;
                traded_size = checkedAdd(traded_size, event.size) orelse
                    return error.ArithmeticOverflow;
                traded_budget = checkedAdd(traded_budget, event_budget) orelse
                    return error.ArithmeticOverflow;
            },
            .reject, .reduce => {
                if (event.size < 0) return error.InvalidExchangeReleaseCommand;
            },
            .binary_event => return error.InvalidExchangeReleaseCommand,
        }
    }

    if (traded_size > command.size or traded_budget > command.price) {
        return error.InvalidExchangeReleaseCommand;
    }
}

fn calcAmountBidTakerFee(size: i64, price: i64, spec: common.CoreSymbolSpecification) ?i64 {
    const step = checkedMul(price, spec.quote_scale_k) orelse return null;
    const notional = checkedMul(size, step) orelse return null;
    const fee = checkedMul(size, spec.taker_fee) orelse return null;
    return checkedAdd(notional, fee);
}

fn calcAmountBid(size: i64, price: i64, spec: common.CoreSymbolSpecification) ?i64 {
    const step = checkedMul(price, spec.quote_scale_k) orelse return null;
    return checkedMul(size, step);
}

fn calcAmountBidReleaseCorrMaker(size: i64, price_diff: i64, spec: common.CoreSymbolSpecification) ?i64 {
    const step = checkedMul(price_diff, spec.quote_scale_k) orelse return null;
    const fee_diff = spec.taker_fee - spec.maker_fee;
    const correction_per_lot = checkedAdd(step, fee_diff) orelse return null;
    return checkedMul(size, correction_per_lot);
}

fn calcAmountBidTakerFeeForBudget(size: i64, budget_in_steps: i64, spec: common.CoreSymbolSpecification) ?i64 {
    const quote_part = checkedMul(budget_in_steps, spec.quote_scale_k) orelse return null;
    const fee_part = checkedMul(size, spec.taker_fee) orelse return null;
    return checkedAdd(quote_part, fee_part);
}

fn calcAmountAsk(size: i64, spec: common.CoreSymbolSpecification) ?i64 {
    return checkedMul(size, spec.base_scale_k);
}

fn makeExchangeSpec() !common.CoreSymbolSpecification {
    return common.CoreSymbolSpecification.init(.{
        .symbol_id = 241,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 100,
        .quote_scale_k = 10,
        .taker_fee = 2,
        .maker_fee = 1,
    });
}

fn makeFuturesSpecForSymbol(
    symbol_id: i32,
    margin_buy: i64,
    margin_sell: i64,
) !common.CoreSymbolSpecification {
    return common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol_id,
        .symbol_type = .futures_contract,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 100,
        .quote_scale_k = 10,
        .taker_fee = 3,
        .maker_fee = 2,
        .margin_buy = margin_buy,
        .margin_sell = margin_sell,
    });
}

test "risk preprocessor handles add user and balance adjustment paths" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{});
    defer engine.deinit();

    var add_user: cmd.OrderCommand = .{
        .command = .add_user,
        .uid = 501,
        .result_code = .new,
    };
    _ = try engine.preProcessCommand(1, &add_user);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_user.result_code);

    var add_user_dup = add_user;
    _ = try engine.preProcessCommand(2, &add_user_dup);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, add_user_dup.result_code);

    var bal: cmd.OrderCommand = .{
        .command = .balance_adjustment,
        .uid = 501,
        .symbol = 15,
        .price = 1000,
        .order_id = 10,
        .order_type = .gtc, // BalanceAdjustmentType.ADJUSTMENT code = 0
        .result_code = .new,
    };
    _ = try engine.preProcessCommand(3, &bal);
    try std.testing.expectEqual(cmd.CommandResultCode.success, bal.result_code);

    var bal_same = bal;
    _ = try engine.preProcessCommand(4, &bal_same);
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_account_balance_adjustment_already_applied_same,
        bal_same.result_code,
    );

    const profile = engine.getUserProfile(501) orelse unreachable;
    try std.testing.expectEqual(@as(i64, 1000), profile.accountBalance(15));
}

test "zero adjustments remain active in Eclipse histories and reset retains capacity" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{});
    defer engine.deinit();
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(502));

    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try engine.adjustBalance(502, 7, 0, 1, .adjustment),
    );
    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try engine.adjustBalance(502, 9, 0, 2, .@"suspend"),
    );
    const adjustment_keys = try engine.adjustmentKeysInHistoryOrder();
    defer allocator.free(adjustment_keys);
    try std.testing.expectEqualSlices(i32, &.{7}, adjustment_keys);
    const suspend_keys = try engine.suspendKeysInHistoryOrder();
    defer allocator.free(suspend_keys);
    try std.testing.expectEqualSlices(i32, &.{9}, suspend_keys);

    engine.reset();
    try std.testing.expectEqual(@as(?usize, 16), engine.adjustments_history.tableSizeIfKnown());
    try std.testing.expectEqual(@as(?usize, 16), engine.suspends_history.tableSizeIfKnown());
    const empty_adjustments = try engine.adjustmentKeysInHistoryOrder();
    defer allocator.free(empty_adjustments);
    try std.testing.expectEqual(@as(usize, 0), empty_adjustments.len);
}

test "risk preprocessor exchange place-order checks and holds" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{});
    defer engine.deinit();
    try engine.addSymbol(try makeExchangeSpec());

    var add_user: cmd.OrderCommand = .{
        .command = .add_user,
        .uid = 601,
        .result_code = .new,
    };
    _ = try engine.preProcessCommand(1, &add_user);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_user.result_code);

    var deposit: cmd.OrderCommand = .{
        .command = .balance_adjustment,
        .uid = 601,
        .symbol = 15,
        .price = 1000,
        .order_id = 11,
        .order_type = .gtc,
        .result_code = .new,
    };
    _ = try engine.preProcessCommand(2, &deposit);
    try std.testing.expectEqual(cmd.CommandResultCode.success, deposit.result_code);

    var missing_symbol = cmd.OrderCommand.newOrder(.gtc, 7001, 601, 20, 25, 3, .bid);
    missing_symbol.symbol = 999;
    _ = try engine.preProcessCommand(3, &missing_symbol);
    try std.testing.expectEqual(cmd.CommandResultCode.invalid_symbol, missing_symbol.result_code);

    var bad_reserve = cmd.OrderCommand.newOrder(.gtc, 7002, 601, 20, 19, 3, .bid);
    bad_reserve.symbol = 241;
    _ = try engine.preProcessCommand(4, &bad_reserve);
    try std.testing.expectEqual(cmd.CommandResultCode.risk_nsf, bad_reserve.result_code);

    var valid_bid = cmd.OrderCommand.newOrder(.gtc, 7003, 601, 20, 20, 3, .bid);
    valid_bid.symbol = 241;
    _ = try engine.preProcessCommand(5, &valid_bid);
    try std.testing.expectEqual(cmd.CommandResultCode.valid_for_matching_engine, valid_bid.result_code);

    const profile = engine.getUserProfile(601) orelse unreachable;
    // Hold = size * (price * quoteScaleK + takerFee) = 3 * (20 * 10 + 2) = 606
    try std.testing.expectEqual(@as(i64, 394), profile.accountBalance(15));

    var nsf_bid = cmd.OrderCommand.newOrder(.gtc, 7004, 601, 30, 30, 20, .bid);
    nsf_bid.symbol = 241;
    _ = try engine.preProcessCommand(6, &nsf_bid);
    try std.testing.expectEqual(cmd.CommandResultCode.risk_nsf, nsf_bid.result_code);
    try std.testing.expectEqual(@as(i64, 394), profile.accountBalance(15));
}

test "spot NSF restores existing balances and materializes absent currency keys" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{});
    defer engine.deinit();
    try engine.addSymbol(try makeExchangeSpec());

    for ([_]struct {
        uid: i64,
        order_id: i64,
        action: common.OrderAction,
        currency: i32,
        initial_balance: ?i64,
    }{
        .{
            .uid = 604,
            .order_id = 7_101,
            .action = .bid,
            .currency = 15,
            .initial_balance = null,
        },
        .{
            .uid = 605,
            .order_id = 7_102,
            .action = .ask,
            .currency = 11,
            .initial_balance = null,
        },
        .{
            .uid = 606,
            .order_id = 7_103,
            .action = .bid,
            .currency = 15,
            .initial_balance = 11,
        },
        .{
            .uid = 607,
            .order_id = 7_104,
            .action = .ask,
            .currency = 11,
            .initial_balance = 99,
        },
    }) |case| {
        try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(case.uid));
        const profile = engine.getUserProfile(case.uid) orelse unreachable;
        if (case.initial_balance) |initial_balance| {
            try profile.accounts.put(allocator, case.currency, initial_balance);
        }

        var rejected = cmd.OrderCommand.newOrder(
            .gtc,
            case.order_id,
            case.uid,
            1,
            1,
            1,
            case.action,
        );
        rejected.symbol = 241;
        _ = try engine.preProcessCommand(1, &rejected);

        try std.testing.expectEqual(cmd.CommandResultCode.risk_nsf, rejected.result_code);
        try std.testing.expect(profile.accounts.contains(case.currency));
        try std.testing.expectEqual(
            case.initial_balance orelse 0,
            profile.accounts.get(case.currency) orelse unreachable,
        );
        try std.testing.expectEqual(@as(usize, 1), profile.accounts.count());
        if (case.initial_balance == null) {
            const keys = try profile.accountKeysInHistoryOrder(allocator);
            defer allocator.free(keys);
            try std.testing.expectEqualSlices(i32, &.{case.currency}, keys);
        }
    }
}

test "absent spot NSF zero-key insertion is failure atomic" {
    var failing_allocator_state = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = failing_allocator_state.allocator();
    var engine = try RiskEngine.init(allocator, 0, 1, .{});
    defer {
        failing_allocator_state.fail_index = std.math.maxInt(usize);
        engine.deinit();
    }
    try engine.addSymbol(try makeExchangeSpec());
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(608));

    const state_before = try engine.stateHash();
    const profile_before = engine.getUserProfile(608) orelse unreachable;
    var history_before = try profile_before.accounts_history.exportRaw(allocator);
    defer history_before.deinit(allocator);
    failing_allocator_state.fail_index = failing_allocator_state.alloc_index;

    var rejected = cmd.OrderCommand.newOrder(.gtc, 7_105, 608, 1, 1, 1, .bid);
    rejected.symbol = 241;
    rejected.result_code = .new;
    try std.testing.expectError(error.OutOfMemory, engine.preProcessCommand(1, &rejected));
    try std.testing.expect(failing_allocator_state.has_induced_failure);

    failing_allocator_state.fail_index = std.math.maxInt(usize);
    const profile = engine.getUserProfile(608) orelse unreachable;
    try std.testing.expectEqual(@as(usize, 0), profile.accounts.count());
    try std.testing.expectEqual(state_before, try engine.stateHash());
    try std.testing.expectEqual(cmd.CommandResultCode.new, rejected.result_code);
    var history_after = try profile.accounts_history.exportRaw(allocator);
    defer history_after.deinit(allocator);
    try std.testing.expectEqual(history_before.table_length, history_after.table_length);
    try std.testing.expectEqual(history_before.zero_key_flag, history_after.zero_key_flag);
    try std.testing.expectEqual(history_before.one_key_flag, history_after.one_key_flag);
    try std.testing.expectEqualSlices(i32, history_before.slots, history_after.slots);
}

test "risk preprocessor rejects malformed place orders before balance or position holds" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{});
    defer engine.deinit();
    try engine.addSymbol(try makeExchangeSpec());
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(6_011));

    const profile = engine.getUserProfile(6_011) orelse unreachable;
    try profile.accounts.put(allocator, 11, 10_000);
    try profile.accounts.put(allocator, 15, 10_000);
    const state_before = try engine.stateHash();

    var cases = [_]struct {
        order: cmd.OrderCommand,
        expected: cmd.CommandResultCode,
    }{
        .{
            .order = cmd.OrderCommand.newOrder(.gtc, 71_001, 6_011, 10, 10, 0, .bid),
            .expected = .matching_unsupported_command,
        },
        .{
            .order = cmd.OrderCommand.newOrder(.gtc, 71_002, 6_011, 10, 0, -1, .ask),
            .expected = .matching_unsupported_command,
        },
        .{
            .order = cmd.OrderCommand.newOrder(.gtc, 71_003, 6_011, -1, 0, 1, .ask),
            .expected = .invalid_price_step,
        },
        .{
            .order = cmd.OrderCommand.newOrder(.fok_budget, 71_004, 6_011, -1, -1, 1, .bid),
            .expected = .invalid_price_step,
        },
        .{
            .order = cmd.OrderCommand.newOrder(.gtc, 71_005, 6_011, 10, -1, 1, .bid),
            .expected = .risk_invalid_reserve_bid_price,
        },
    };

    for (&cases) |*case| {
        case.order.symbol = 241;
        _ = try engine.preProcessCommand(1, &case.order);
        try std.testing.expectEqual(case.expected, case.order.result_code);
        try std.testing.expectEqual(state_before, try engine.stateHash());
    }

    var missing_action = cmd.OrderCommand.newOrder(.gtc, 71_006, 6_011, 10, 10, 1, .bid);
    missing_action.symbol = 241;
    missing_action.action = null;
    _ = try engine.preProcessCommand(2, &missing_action);
    try std.testing.expectEqual(cmd.CommandResultCode.matching_unsupported_command, missing_action.result_code);
    try std.testing.expectEqual(state_before, try engine.stateHash());

    var missing_type = cmd.OrderCommand.newOrder(.gtc, 71_007, 6_011, 10, 10, 1, .bid);
    missing_type.symbol = 241;
    missing_type.order_type = null;
    _ = try engine.preProcessCommand(3, &missing_type);
    try std.testing.expectEqual(cmd.CommandResultCode.matching_unsupported_command, missing_type.result_code);
    try std.testing.expectEqual(state_before, try engine.stateHash());

    // Input validation is structural and must not be bypassed by no-risk mode.
    engine.cfg_ignore_risk_processing = true;
    const bypass_state_before = try engine.stateHash();
    var bypassed = cmd.OrderCommand.newOrder(.gtc, 71_008, 6_011, 10, 10, 0, .bid);
    bypassed.symbol = 241;
    _ = try engine.preProcessCommand(4, &bypassed);
    try std.testing.expectEqual(cmd.CommandResultCode.matching_unsupported_command, bypassed.result_code);
    try std.testing.expectEqual(bypass_state_before, try engine.stateHash());
}

test "risk preprocessor budget bids hold aggregate budget plus per-lot fees" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{});
    defer engine.deinit();
    try engine.addSymbol(try makeExchangeSpec());

    for ([_]struct {
        uid: i64,
        order_id: i64,
        order_type: common.OrderType,
        budget: i64,
        size: i64,
        expected_balance: i64,
    }{
        .{
            .uid = 602,
            .order_id = 7_005,
            .order_type = .fok_budget,
            .budget = 20,
            .size = 3,
            .expected_balance = 794,
        },
        .{
            .uid = 603,
            .order_id = 7_006,
            .order_type = .ioc_budget,
            .budget = 30,
            .size = 4,
            .expected_balance = 692,
        },
    }) |case| {
        try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(case.uid));
        const profile = engine.getUserProfile(case.uid) orelse unreachable;
        try profile.accounts.put(allocator, 15, 1_000);

        var order = cmd.OrderCommand.newOrder(
            case.order_type,
            case.order_id,
            case.uid,
            case.budget,
            case.budget,
            case.size,
            .bid,
        );
        order.symbol = 241;
        _ = try engine.preProcessCommand(1, &order);

        try std.testing.expectEqual(cmd.CommandResultCode.valid_for_matching_engine, order.result_code);
        try std.testing.expectEqual(case.expected_balance, profile.accountBalance(15));
    }
}

test "risk preprocessor admits collateralized futures orders and cleans rejected empty positions" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{
        .margin_trading_enabled = true,
    });
    defer engine.deinit();
    try engine.addSymbol(try makeFuturesSpecForSymbol(555, 10, 12));

    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(6101));
    const funded = engine.getUserProfile(6101) orelse unreachable;
    try funded.accounts.put(allocator, 15, 50);

    var accepted = cmd.OrderCommand.newOrder(.gtc, 7101, 6101, 100, 100, 5, .bid);
    accepted.symbol = 555;
    _ = try engine.preProcessCommand(1, &accepted);
    try std.testing.expectEqual(cmd.CommandResultCode.valid_for_matching_engine, accepted.result_code);
    const funded_position = funded.positions.getPtr(555) orelse unreachable;
    try std.testing.expectEqual(@as(i64, 5), funded_position.pending_buy_size);
    try std.testing.expectEqual(@as(i64, 50), funded.accountBalance(15));

    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(6102));
    const underfunded = engine.getUserProfile(6102) orelse unreachable;
    try underfunded.accounts.put(allocator, 15, 49);

    var rejected = cmd.OrderCommand.newOrder(.gtc, 7102, 6102, 100, 100, 5, .bid);
    rejected.symbol = 555;
    _ = try engine.preProcessCommand(2, &rejected);
    try std.testing.expectEqual(cmd.CommandResultCode.risk_nsf, rejected.result_code);
    try std.testing.expectEqual(@as(usize, 0), underfunded.positions.count());
    try std.testing.expectEqual(@as(i64, 49), underfunded.accountBalance(15));
}

test "rejected margin order leaves position tombstone and active zero quote account" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{
        .margin_trading_enabled = true,
    });
    defer engine.deinit();
    try engine.addSymbol(try makeFuturesSpecForSymbol(555, 10, 12));
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(6_103));

    var rejected = cmd.OrderCommand.newOrder(.gtc, 7_103, 6_103, 100, 100, 5, .bid);
    rejected.symbol = 555;
    _ = try engine.preProcessCommand(1, &rejected);
    try std.testing.expectEqual(cmd.CommandResultCode.risk_nsf, rejected.result_code);

    const profile = engine.getUserProfile(6_103) orelse unreachable;
    try std.testing.expectEqual(@as(i64, 0), profile.accountBalance(15));
    const account_keys = try profile.accountKeysInHistoryOrder(allocator);
    defer allocator.free(account_keys);
    try std.testing.expectEqualSlices(i32, &.{15}, account_keys);
    const position_keys = try profile.positionKeysInHistoryOrder(allocator);
    defer allocator.free(position_keys);
    try std.testing.expectEqual(@as(usize, 0), position_keys.len);

    var raw = try profile.positions_history.exportRaw(allocator);
    defer raw.deinit(allocator);
    var tombstones: usize = 0;
    for (raw.slots) |key| tombstones += @intFromBool(key == 1);
    try std.testing.expectEqual(@as(usize, 1), tombstones);
}

test "risk preprocessor always admits futures orders that do not increase exposure" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{
        .margin_trading_enabled = true,
    });
    defer engine.deinit();
    try engine.addSymbol(try makeFuturesSpecForSymbol(555, 10, 12));

    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(6201));
    const profile = engine.getUserProfile(6201) orelse unreachable;
    try profile.accounts.put(allocator, 15, -1_000);
    try profile.positions.put(allocator, 555, user.SymbolPositionRecord.init(6201, 555, 15));
    const position = profile.positions.getPtr(555) orelse unreachable;
    position.direction = .long;
    position.open_volume = 5;
    position.open_price_sum = 500;

    var reducing = cmd.OrderCommand.newOrder(.gtc, 7201, 6201, 100, 0, 5, .ask);
    reducing.symbol = 555;
    _ = try engine.preProcessCommand(1, &reducing);
    try std.testing.expectEqual(cmd.CommandResultCode.valid_for_matching_engine, reducing.result_code);
    try std.testing.expectEqual(@as(i64, 5), position.pending_sell_size);
    try std.testing.expectEqual(@as(i64, -1_000), profile.accountBalance(15));
}

test "risk preprocessor uses same-currency futures P and L for cross-symbol collateral" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{
        .margin_trading_enabled = true,
    });
    defer engine.deinit();
    try engine.addSymbol(try makeFuturesSpecForSymbol(555, 10, 12));
    try engine.addSymbol(try makeFuturesSpecForSymbol(556, 5, 6));

    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(6301));
    const profile = engine.getUserProfile(6301) orelse unreachable;
    try profile.positions.put(allocator, 556, user.SymbolPositionRecord.init(6301, 556, 15));
    const collateral_position = profile.positions.getPtr(556) orelse unreachable;
    collateral_position.direction = .long;
    collateral_position.open_volume = 10;
    collateral_position.open_price_sum = 1_000;
    try engine.last_price_cache.put(allocator, 556, .{
        .ask_price = 111,
        .bid_price = 110,
    });

    // P&L is 100, existing required margin is 50, leaving exactly 50 for this order.
    var order = cmd.OrderCommand.newOrder(.gtc, 7301, 6301, 100, 100, 5, .bid);
    order.symbol = 555;
    _ = try engine.preProcessCommand(1, &order);
    try std.testing.expectEqual(cmd.CommandResultCode.valid_for_matching_engine, order.result_code);
    try std.testing.expectEqual(@as(i64, 5), profile.positions.getPtr(555).?.pending_buy_size);

    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(6302));
    const one_below = engine.getUserProfile(6302) orelse unreachable;
    try one_below.positions.put(allocator, 556, user.SymbolPositionRecord.init(6302, 556, 15));
    const one_below_collateral = one_below.positions.getPtr(556) orelse unreachable;
    one_below_collateral.direction = .long;
    one_below_collateral.open_volume = 10;
    one_below_collateral.open_price_sum = 1_000;
    one_below_collateral.profit = -1;
    const collateral_before = one_below_collateral.*;

    // P&L is 99 and existing required margin is 50, so only 49 remains for
    // the new symbol's required margin of 50. The rejected temporary record
    // must not remain in the profile.
    var rejected = cmd.OrderCommand.newOrder(.gtc, 7302, 6302, 100, 100, 5, .bid);
    rejected.symbol = 555;
    _ = try engine.preProcessCommand(2, &rejected);
    try std.testing.expectEqual(cmd.CommandResultCode.risk_nsf, rejected.result_code);
    try std.testing.expect(one_below.positions.get(555) == null);
    try std.testing.expect(one_below.accounts.contains(15));
    try std.testing.expectEqualDeep(
        collateral_before,
        one_below.positions.get(556) orelse unreachable,
    );
    try std.testing.expectEqual(@as(i64, 0), one_below.accountBalance(15));
}

test "risk preprocessor lets free futures margin collateralize a spot order" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{
        .margin_trading_enabled = true,
    });
    defer engine.deinit();
    try engine.addSymbol(try makeExchangeSpec());
    try engine.addSymbol(try makeFuturesSpecForSymbol(556, 5, 6));

    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(6401));
    const collateralized = engine.getUserProfile(6401) orelse unreachable;
    try collateralized.positions.put(allocator, 556, user.SymbolPositionRecord.init(6401, 556, 15));
    const position = collateralized.positions.getPtr(556) orelse unreachable;
    position.direction = .long;
    position.open_volume = 10;
    position.open_price_sum = 1_000;
    try engine.last_price_cache.put(allocator, 556, .{
        .ask_price = 111,
        .bid_price = 110,
    });

    // Spot hold is 42; the futures position contributes 100 P&L - 50 margin.
    var accepted = cmd.OrderCommand.newOrder(.gtc, 7401, 6401, 4, 4, 1, .bid);
    accepted.symbol = 241;
    _ = try engine.preProcessCommand(1, &accepted);
    try std.testing.expectEqual(cmd.CommandResultCode.valid_for_matching_engine, accepted.result_code);
    try std.testing.expectEqual(@as(i64, -42), collateralized.accountBalance(15));

    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(6402));
    const uncollateralized = engine.getUserProfile(6402) orelse unreachable;
    var rejected = cmd.OrderCommand.newOrder(.gtc, 7402, 6402, 4, 4, 1, .bid);
    rejected.symbol = 241;
    _ = try engine.preProcessCommand(2, &rejected);
    try std.testing.expectEqual(cmd.CommandResultCode.risk_nsf, rejected.result_code);
    try std.testing.expectEqual(@as(i64, 0), uncollateralized.accountBalance(15));
}

test "risk preprocessor supports suspend and resume commands" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{});
    defer engine.deinit();

    var add_user: cmd.OrderCommand = .{
        .command = .add_user,
        .uid = 701,
        .result_code = .new,
    };
    _ = try engine.preProcessCommand(1, &add_user);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_user.result_code);

    var suspend_cmd: cmd.OrderCommand = .{
        .command = .suspend_user,
        .uid = 701,
        .result_code = .new,
    };
    _ = try engine.preProcessCommand(2, &suspend_cmd);
    try std.testing.expectEqual(cmd.CommandResultCode.success, suspend_cmd.result_code);

    var resume_cmd: cmd.OrderCommand = .{
        .command = .resume_user,
        .uid = 701,
        .result_code = .new,
    };
    _ = try engine.preProcessCommand(3, &resume_cmd);
    try std.testing.expectEqual(cmd.CommandResultCode.success, resume_cmd.result_code);
}

test "risk engine state hash is order-independent and mutation-sensitive" {
    const allocator = std.testing.allocator;
    var left = try RiskEngine.init(allocator, 0, 1, .{
        .margin_trading_enabled = true,
    });
    defer left.deinit();
    var right = try RiskEngine.init(allocator, 0, 1, .{
        .margin_trading_enabled = true,
    });
    defer right.deinit();

    const spec = try makeExchangeSpec();
    try left.addSymbol(spec);
    try right.addSymbol(spec);

    var left_add_1: cmd.OrderCommand = .{ .command = .add_user, .uid = 9001, .result_code = .new };
    var left_add_2: cmd.OrderCommand = .{ .command = .add_user, .uid = 9002, .result_code = .new };
    _ = try left.preProcessCommand(1, &left_add_1);
    _ = try left.preProcessCommand(2, &left_add_2);

    var left_dep_1: cmd.OrderCommand = .{
        .command = .balance_adjustment,
        .uid = 9001,
        .symbol = 15,
        .price = 1_000,
        .order_id = 1,
        .order_type = .gtc,
        .result_code = .new,
    };
    var left_dep_2: cmd.OrderCommand = .{
        .command = .balance_adjustment,
        .uid = 9002,
        .symbol = 11,
        .price = 500,
        .order_id = 1,
        .order_type = .gtc,
        .result_code = .new,
    };
    _ = try left.preProcessCommand(3, &left_dep_1);
    _ = try left.preProcessCommand(4, &left_dep_2);

    var right_add_2: cmd.OrderCommand = .{ .command = .add_user, .uid = 9002, .result_code = .new };
    var right_add_1: cmd.OrderCommand = .{ .command = .add_user, .uid = 9001, .result_code = .new };
    _ = try right.preProcessCommand(1, &right_add_2);
    _ = try right.preProcessCommand(2, &right_add_1);

    var right_dep_2: cmd.OrderCommand = .{
        .command = .balance_adjustment,
        .uid = 9002,
        .symbol = 11,
        .price = 500,
        .order_id = 1,
        .order_type = .gtc,
        .result_code = .new,
    };
    var right_dep_1: cmd.OrderCommand = .{
        .command = .balance_adjustment,
        .uid = 9001,
        .symbol = 15,
        .price = 1_000,
        .order_id = 1,
        .order_type = .gtc,
        .result_code = .new,
    };
    _ = try right.preProcessCommand(3, &right_dep_2);
    _ = try right.preProcessCommand(4, &right_dep_1);

    const left_hash = try left.stateHash();
    const right_hash = try right.stateHash();
    try std.testing.expectEqual(left_hash, right_hash);

    var right_mutation: cmd.OrderCommand = .{
        .command = .balance_adjustment,
        .uid = 9001,
        .symbol = 15,
        .price = 1,
        .order_id = 2,
        .order_type = .gtc,
        .result_code = .new,
    };
    _ = try right.preProcessCommand(5, &right_mutation);

    const mutated_hash = try right.stateHash();
    try std.testing.expect(left_hash != mutated_hash);
}

test "risk engine seeded user lifecycle campaign is deterministic and seed-sensitive" {
    const allocator = std.testing.allocator;
    const rounds: usize = 480;
    const user_pool: usize = 48;

    const Scenario = struct {
        name: []const u8,
        seed: u64,
    };

    const Operation = enum {
        add_user,
        balance_adjustment,
        suspend_user,
        resume_user,
        place_order,
    };

    const Outcome = struct {
        digest: u64 = 0,
        add_user_success: usize = 0,
        add_user_duplicates: usize = 0,
        balance_success: usize = 0,
        balance_rejected: usize = 0,
        suspend_success: usize = 0,
        suspend_rejected: usize = 0,
        resume_success: usize = 0,
        resume_rejected: usize = 0,
        valid_orders: usize = 0,
        rejected_orders: usize = 0,
        final_hash: u64 = 0,
        present_users: usize = 0,
    };

    const Harness = struct {
        fn mixDigest(digest: *u64, token: u64) void {
            digest.* = (digest.* ^ token) *% 0x9E3779B185EBCA87;
            digest.* +%= 0x632BE59BD9B4E019;
        }

        fn digestResultCode(digest: *u64, result_code: cmd.CommandResultCode) void {
            const code_u32: u32 = @bitCast(result_code.code());
            mixDigest(digest, @as(u64, code_u32));
        }

        fn panicUnexpectedResult(
            scenario: Scenario,
            round: usize,
            operation: Operation,
            result_code: cmd.CommandResultCode,
        ) noreturn {
            std.debug.panic(
                "risk seeded lifecycle scenario='{s}' seed=0x{x} round={d} operation={s} unexpected result={s}; replay with: zig build test --test-filter \"risk engine seeded user lifecycle campaign is deterministic and seed-sensitive\"",
                .{
                    scenario.name,
                    scenario.seed,
                    round,
                    @tagName(operation),
                    @tagName(result_code),
                },
            );
        }

        fn assertAllowedResult(
            scenario: Scenario,
            round: usize,
            operation: Operation,
            result_code: cmd.CommandResultCode,
        ) void {
            const allowed = switch (operation) {
                .add_user => switch (result_code) {
                    .success,
                    .user_mgmt_user_already_exists,
                    => true,
                    else => false,
                },
                .balance_adjustment => switch (result_code) {
                    .success,
                    .auth_invalid_user,
                    .user_mgmt_account_balance_adjustment_nsf,
                    .user_mgmt_account_balance_adjustment_already_applied_same,
                    .user_mgmt_account_balance_adjustment_already_applied_many,
                    => true,
                    else => false,
                },
                .suspend_user => switch (result_code) {
                    .success,
                    .user_mgmt_user_not_found,
                    .user_mgmt_user_already_suspended,
                    .user_mgmt_user_not_suspendable_has_positions,
                    .user_mgmt_user_not_suspendable_non_empty_accounts,
                    => true,
                    else => false,
                },
                .resume_user => switch (result_code) {
                    .success,
                    .user_mgmt_user_not_suspended,
                    => true,
                    else => false,
                },
                .place_order => switch (result_code) {
                    .valid_for_matching_engine,
                    .auth_invalid_user,
                    .invalid_symbol,
                    .risk_nsf,
                    .risk_invalid_reserve_bid_price,
                    .risk_ask_price_lower_than_fee,
                    => true,
                    else => false,
                },
            };
            if (!allowed) {
                panicUnexpectedResult(scenario, round, operation, result_code);
            }
        }

        fn checkUserBalanceInvariant(engine: *RiskEngine, uid: i64) !void {
            const profile = engine.getUserProfile(uid) orelse return;
            if (profile.accounts.get(11)) |balance| {
                try std.testing.expect(balance >= 0);
            }
            if (profile.accounts.get(15)) |balance| {
                try std.testing.expect(balance >= 0);
            }
        }

        fn runScenario(scenario: Scenario) !Outcome {
            var engine = try RiskEngine.init(allocator, 0, 1, .{});
            defer engine.deinit();
            try engine.addSymbol(try makeExchangeSpec());

            var outcome: Outcome = .{
                .digest = 0xA7B1C9D3E5F70819 ^ scenario.seed,
            };
            var prng = std.Random.DefaultPrng.init(scenario.seed);
            const random = prng.random();

            const base_uid: i64 = 4_000_000 + @as(i64, @intCast(scenario.seed % 10_000)) * 4;
            var next_funding_tx_id: i64 = 1;
            var next_order_id: i64 = 10_000;

            for (0..rounds) |round| {
                const op_roll = random.uintLessThan(u32, 100);
                const uid_offset = random.uintLessThan(usize, user_pool);
                const uid = base_uid + @as(i64, @intCast(uid_offset));
                const sequence = @as(u64, @intCast(round + 1));

                var operation: Operation = .add_user;
                var command: cmd.OrderCommand = undefined;

                if (op_roll < 18) {
                    operation = .add_user;
                    command = .{
                        .command = .add_user,
                        .uid = uid,
                        .result_code = .new,
                    };
                } else if (op_roll < 48) {
                    operation = .balance_adjustment;
                    const currency: i32 = if (random.uintLessThan(u32, 100) < 70) 15 else 11;
                    const magnitude = 20 + @as(i64, @intCast(random.uintLessThan(u32, 280)));
                    const amount = if (random.uintLessThan(u32, 100) < 28) -magnitude else magnitude;
                    command = .{
                        .command = .balance_adjustment,
                        .uid = uid,
                        .symbol = currency,
                        .price = amount,
                        .order_id = next_funding_tx_id,
                        .order_type = .gtc,
                        .result_code = .new,
                    };
                    next_funding_tx_id += 1;
                } else if (op_roll < 62) {
                    operation = .suspend_user;
                    command = .{
                        .command = .suspend_user,
                        .uid = uid,
                        .result_code = .new,
                    };
                } else if (op_roll < 76) {
                    operation = .resume_user;
                    command = .{
                        .command = .resume_user,
                        .uid = uid,
                        .result_code = .new,
                    };
                } else {
                    operation = .place_order;
                    const is_bid = random.boolean();
                    const price = 10 + @as(i64, @intCast(random.uintLessThan(u32, 90)));
                    const size = 1 + @as(i64, @intCast(random.uintLessThan(u32, 5)));
                    const reserve_delta = @as(i64, @intCast(random.uintLessThan(u32, 4)));
                    var reserve_price = price + reserve_delta;
                    if (is_bid and random.uintLessThan(u32, 100) < 12) {
                        reserve_price = price - 1;
                    }
                    var order = cmd.OrderCommand.newOrder(
                        .gtc,
                        next_order_id,
                        uid,
                        price,
                        reserve_price,
                        size,
                        if (is_bid) .bid else .ask,
                    );
                    order.symbol = if (random.uintLessThan(u32, 100) < 5) 9_999 else 241;
                    command = order;
                    next_order_id += 1;
                }

                _ = try engine.preProcessCommand(sequence, &command);
                assertAllowedResult(scenario, round, operation, command.result_code);

                mixDigest(&outcome.digest, sequence);
                mixDigest(&outcome.digest, @as(u64, @intFromEnum(operation)));
                mixDigest(&outcome.digest, @as(u64, @bitCast(uid)));
                digestResultCode(&outcome.digest, command.result_code);

                switch (operation) {
                    .add_user => switch (command.result_code) {
                        .success => outcome.add_user_success += 1,
                        .user_mgmt_user_already_exists => outcome.add_user_duplicates += 1,
                        else => unreachable,
                    },
                    .balance_adjustment => switch (command.result_code) {
                        .success => outcome.balance_success += 1,
                        .auth_invalid_user,
                        .user_mgmt_account_balance_adjustment_nsf,
                        .user_mgmt_account_balance_adjustment_already_applied_same,
                        .user_mgmt_account_balance_adjustment_already_applied_many,
                        => outcome.balance_rejected += 1,
                        else => unreachable,
                    },
                    .suspend_user => switch (command.result_code) {
                        .success => outcome.suspend_success += 1,
                        .user_mgmt_user_not_found,
                        .user_mgmt_user_already_suspended,
                        .user_mgmt_user_not_suspendable_has_positions,
                        .user_mgmt_user_not_suspendable_non_empty_accounts,
                        => outcome.suspend_rejected += 1,
                        else => unreachable,
                    },
                    .resume_user => switch (command.result_code) {
                        .success => outcome.resume_success += 1,
                        .user_mgmt_user_not_suspended => outcome.resume_rejected += 1,
                        else => unreachable,
                    },
                    .place_order => switch (command.result_code) {
                        .valid_for_matching_engine => outcome.valid_orders += 1,
                        .auth_invalid_user,
                        .invalid_symbol,
                        .risk_nsf,
                        .risk_invalid_reserve_bid_price,
                        .risk_ask_price_lower_than_fee,
                        => outcome.rejected_orders += 1,
                        else => unreachable,
                    },
                }

                try checkUserBalanceInvariant(&engine, uid);
                const round_hash = try engine.stateHash();
                mixDigest(&outcome.digest, round_hash);
            }

            outcome.final_hash = try engine.stateHash();
            mixDigest(&outcome.digest, outcome.final_hash);
            outcome.present_users = engine.user_profile_service.user_profiles.count();
            return outcome;
        }
    };

    const scenarios = [_]Scenario{
        .{
            .name = "risk-seed-a",
            .seed = 0xBEEF1001,
        },
        .{
            .name = "risk-seed-b",
            .seed = 0xBEEF1002,
        },
    };

    var digest_a: ?u64 = null;
    var digest_b: ?u64 = null;

    for (scenarios, 0..) |scenario, idx| {
        const first = try Harness.runScenario(scenario);
        const second = try Harness.runScenario(scenario);
        try std.testing.expectEqualDeep(first, second);

        try std.testing.expect(first.add_user_success > 0);
        try std.testing.expect(first.balance_success > 0);
        try std.testing.expect(first.suspend_success > 0);
        try std.testing.expect(first.resume_success > 0);
        try std.testing.expect(first.valid_orders > 0);
        try std.testing.expect(first.rejected_orders > 0);
        try std.testing.expect(first.present_users > 0);

        if (idx == 0) {
            digest_a = first.digest;
        } else {
            digest_b = first.digest;
        }
    }

    const first_digest = digest_a orelse return error.MissingSeedCoverage;
    const second_digest = digest_b orelse return error.MissingSeedCoverage;
    try std.testing.expect(first_digest != second_digest);
}

test "risk release exchange sell trade settles taker maker and fees" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{});
    defer engine.deinit();
    try engine.addSymbol(try makeExchangeSpec());

    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(1001));
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(1002));

    var command = cmd.OrderCommand.newOrder(.gtc, 8001, 1001, 50, 55, 2, .ask);
    command.symbol = 241;
    command.command = .place_order;

    var trade: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = 1002,
        .price = 50,
        .size = 2,
        .bidder_hold_price = 55,
    };

    _ = try engine.handlerRiskRelease(1, &command, &trade, null);

    const taker = engine.getUserProfile(1001) orelse unreachable;
    const maker = engine.getUserProfile(1002) orelse unreachable;

    try std.testing.expectEqual(@as(i64, 996), taker.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 102), maker.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 200), maker.accountBalance(11));
    try std.testing.expectEqual(@as(i64, 6), engine.getFeeCollected(15));
}

test "risk release exchange buy handles reject then trades" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{});
    defer engine.deinit();
    try engine.addSymbol(try makeExchangeSpec());

    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(2001));
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(2002));

    var command = cmd.OrderCommand.newOrder(.gtc, 8002, 2001, 60, 60, 4, .bid);
    command.symbol = 241;
    command.command = .place_order;

    var reject: events.MatcherTradeEvent = .{
        .event_type = .reject,
        .price = 60,
        .size = 1,
        .bidder_hold_price = 60,
    };
    var trade: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = 2002,
        .price = 50,
        .size = 3,
        .bidder_hold_price = 60,
    };
    reject.next_event = &trade;

    _ = try engine.handlerRiskRelease(2, &command, &reject, null);

    const taker = engine.getUserProfile(2001) orelse unreachable;
    const maker = engine.getUserProfile(2002) orelse unreachable;

    try std.testing.expectEqual(@as(i64, 902), taker.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 300), taker.accountBalance(11));
    try std.testing.expectEqual(@as(i64, 1497), maker.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 9), engine.getFeeCollected(15));
}

test "IOC budget buy releases unused aggregate budget and unfilled fees" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{});
    defer engine.deinit();
    try engine.addSymbol(try makeExchangeSpec());

    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(2_101));
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(2_102));
    const taker = engine.getUserProfile(2_101) orelse unreachable;
    try taker.accounts.put(allocator, 15, 10_000);

    // Aggregate quote budget is 300 steps for five lots. R1 must hold
    // 300 * 10 + 5 * 2 = 3_010, not 5 * (300 * 10 + 2).
    var command = cmd.OrderCommand.newOrder(.ioc_budget, 8_003, 2_101, 300, 300, 5, .bid);
    command.symbol = 241;
    _ = try engine.preProcessCommand(1, &command);
    try std.testing.expectEqual(cmd.CommandResultCode.valid_for_matching_engine, command.result_code);
    try std.testing.expectEqual(@as(i64, 6_990), taker.accountBalance(15));

    // Three lots fill for 240 total price steps; two are rejected. R2 returns
    // the unused 60 steps and both unfilled taker fees: 60 * 10 + 2 * 2.
    var reject: events.MatcherTradeEvent = .{
        .event_type = .reject,
        .price = 300,
        .size = 2,
        .bidder_hold_price = 300,
    };
    var trade: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = 2_102,
        .price = 80,
        .size = 3,
        .bidder_hold_price = 300,
    };
    reject.next_event = &trade;

    _ = try engine.handlerRiskRelease(2, &command, &reject, null);

    try std.testing.expectEqual(@as(i64, 7_594), taker.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 300), taker.accountBalance(11));
    try std.testing.expectEqual(@as(i64, 2_397), engine.getUserProfile(2_102).?.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 9), engine.getFeeCollected(15));
}

test "IOC budget impossible trade aggregates propagate without settlement" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{});
    defer engine.deinit();
    try engine.addSymbol(try makeExchangeSpec());
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(2_111));
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(2_112));
    try engine.getUserProfile(2_111).?.accounts.put(allocator, 15, 7_000);
    try engine.getUserProfile(2_112).?.accounts.put(allocator, 15, 9_000);
    const state_before = try engine.stateHash();

    const cases = [_]struct {
        size: i64,
        budget: i64,
        trade_size: i64,
        trade_price: i64,
    }{
        // P > B: one lot costs more than the complete aggregate budget.
        .{ .size = 5, .budget = 100, .trade_size = 1, .trade_price = 101 },
        // V > S: the matcher claims more traded lots than the command size.
        .{ .size = 2, .budget = 1_000, .trade_size = 3, .trade_price = 100 },
    };

    for (cases) |case| {
        var command = cmd.OrderCommand.newOrder(
            .ioc_budget,
            81_001,
            2_111,
            case.budget,
            case.budget,
            case.size,
            .bid,
        );
        command.symbol = 241;
        var trade: events.MatcherTradeEvent = .{
            .event_type = .trade,
            .matched_order_uid = 2_112,
            .price = case.trade_price,
            .size = case.trade_size,
            .bidder_hold_price = case.budget,
        };

        try std.testing.expectError(
            error.InvalidExchangeReleaseCommand,
            engine.handlerRiskRelease(1, &command, &trade, null),
        );
        try std.testing.expectEqual(state_before, try engine.stateHash());
    }
}

test "FOK budget partial trade and reject chain is an atomic invariant failure" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{});
    defer engine.deinit();
    try engine.addSymbol(try makeExchangeSpec());
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(2_121));
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(2_122));
    try engine.getUserProfile(2_121).?.accounts.put(allocator, 15, 7_000);
    try engine.getUserProfile(2_122).?.accounts.put(allocator, 15, 9_000);
    const state_before = try engine.stateHash();

    var command = cmd.OrderCommand.newOrder(.fok_budget, 81_002, 2_121, 300, 300, 3, .bid);
    command.symbol = 241;
    var reject: events.MatcherTradeEvent = .{
        .event_type = .reject,
        .price = 300,
        .size = 1,
        .bidder_hold_price = 300,
    };
    var trade: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = 2_122,
        .price = 100,
        .size = 2,
        .bidder_hold_price = 300,
    };
    reject.next_event = &trade;

    try std.testing.expectError(
        error.InvalidExchangeReleaseCommand,
        engine.handlerRiskRelease(1, &command, &reject, null),
    );
    try std.testing.expectEqual(state_before, try engine.stateHash());
}

test "prepared exchange release is read-only until infallible commit" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{
        .margin_trading_enabled = true,
    });
    defer engine.deinit();
    try engine.addSymbol(try makeExchangeSpec());
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(2011));
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(2012));

    const state_before = try engine.stateHash();
    var command = cmd.OrderCommand.newOrder(.gtc, 8011, 2011, 50, 55, 2, .ask);
    command.symbol = 241;
    var trade: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = 2012,
        .price = 50,
        .size = 2,
        .bidder_hold_price = 55,
    };

    var prepared = try engine.prepareExchangeRiskRelease(&command, &trade, .{
        .ask_size = 1,
        .bid_size = 1,
        .ask_price_0 = 51,
        .bid_price_0 = 49,
    });
    defer prepared.deinit();

    try std.testing.expectEqual(state_before, try engine.stateHash());
    try std.testing.expectEqual(@as(i64, 0), engine.getUserProfile(2011).?.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 0), engine.getUserProfile(2012).?.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 0), engine.getFeeCollected(15));
    try std.testing.expect(engine.getLastPrice(241) == null);

    engine.commitPreparedExchangeRiskRelease(&prepared);
    try std.testing.expectEqual(@as(i64, 996), engine.getUserProfile(2011).?.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 102), engine.getUserProfile(2012).?.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 200), engine.getUserProfile(2012).?.accountBalance(11));
    try std.testing.expectEqual(@as(i64, 6), engine.getFeeCollected(15));
    try std.testing.expectEqualDeep(
        RiskEngine.LastPriceCacheRecord{ .ask_price = 51, .bid_price = 49 },
        engine.getLastPrice(241) orelse unreachable,
    );
}

test "exchange R2 histories preserve taker before maker and quote before base staging" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{});
    defer engine.deinit();
    try engine.addSymbol(try makeExchangeSpec());

    var command = cmd.OrderCommand.newOrder(.gtc, 8_021, 9_201, 50, 55, 2, .ask);
    command.symbol = 241;
    var trade: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = 9_202,
        .price = 50,
        .size = 2,
        .bidder_hold_price = 55,
    };
    var prepared = try engine.prepareExchangeRiskRelease(&command, &trade, null);
    defer prepared.deinit();
    engine.commitPreparedExchangeRiskRelease(&prepared);

    var expected_users = try long_map_order.History.initWithExpectedSize(1024);
    defer expected_users.deinit(allocator);
    try expected_users.recordPut(allocator, 9_201);
    try expected_users.recordPut(allocator, 9_202);
    var expected_users_raw = try expected_users.exportRaw(allocator);
    defer expected_users_raw.deinit(allocator);
    var actual_users_raw =
        try engine.user_profile_service.user_profiles_history.exportRaw(allocator);
    defer actual_users_raw.deinit(allocator);
    try std.testing.expectEqualSlices(i64, expected_users_raw.slots, actual_users_raw.slots);

    const maker = engine.getUserProfile(9_202) orelse unreachable;
    var expected_maker_accounts = int_map_order.History.init();
    defer expected_maker_accounts.deinit(allocator);
    try expected_maker_accounts.recordPut(allocator, 15);
    try expected_maker_accounts.recordPut(allocator, 11);
    var expected_accounts_raw = try expected_maker_accounts.exportRaw(allocator);
    defer expected_accounts_raw.deinit(allocator);
    var actual_accounts_raw = try maker.accounts_history.exportRaw(allocator);
    defer actual_accounts_raw.deinit(allocator);
    try std.testing.expectEqualSlices(i32, expected_accounts_raw.slots, actual_accounts_raw.slots);

    const taker = engine.getUserProfile(9_201) orelse unreachable;
    const taker_keys = try taker.accountKeysInHistoryOrder(allocator);
    defer allocator.free(taker_keys);
    try std.testing.expectEqualSlices(i32, &.{15}, taker_keys);
    const fee_keys = try engine.feeKeysInHistoryOrder();
    defer allocator.free(fee_keys);
    try std.testing.expectEqualSlices(i32, &.{15}, fee_keys);
}

test "exchange release stays unchanged across every allocation failure" {
    const Harness = struct {
        fn run(fail_offset: ?usize) !usize {
            var failing_allocator_state = std.testing.FailingAllocator.init(std.testing.allocator, .{});
            const allocator = failing_allocator_state.allocator();
            var engine = try RiskEngine.init(allocator, 0, 1, .{
                .margin_trading_enabled = true,
            });
            defer engine.deinit();
            try engine.addSymbol(try makeExchangeSpec());

            const taker_uid: i64 = 2021;
            const missing_maker_uid: i64 = 2022;
            try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(taker_uid));
            const state_before = try engine.stateHash();

            var command = cmd.OrderCommand.newOrder(.gtc, 8021, taker_uid, 50, 55, 2, .ask);
            command.symbol = 241;
            var trade: events.MatcherTradeEvent = .{
                .event_type = .trade,
                .matched_order_uid = missing_maker_uid,
                .price = 50,
                .size = 2,
                .bidder_hold_price = 55,
            };
            const market_data: events.L2Top = .{
                .ask_size = 1,
                .bid_size = 1,
                .ask_price_0 = 51,
                .bid_price_0 = 49,
            };

            const release_alloc_start = failing_allocator_state.alloc_index;
            if (fail_offset) |offset| {
                failing_allocator_state.fail_index = release_alloc_start + offset;
                try std.testing.expectError(
                    error.OutOfMemory,
                    engine.handlerRiskRelease(1, &command, &trade, market_data),
                );
                try std.testing.expect(failing_allocator_state.has_induced_failure);

                failing_allocator_state.fail_index = std.math.maxInt(usize);
                try std.testing.expectEqual(state_before, try engine.stateHash());
                try std.testing.expect(engine.getUserProfile(missing_maker_uid) == null);
                const taker = engine.getUserProfile(taker_uid) orelse unreachable;
                try std.testing.expectEqual(@as(usize, 0), taker.accounts.count());
                try std.testing.expectEqual(@as(i64, 0), engine.getFeeCollected(15));
                try std.testing.expect(engine.getLastPrice(241) == null);
                return 0;
            }

            _ = try engine.handlerRiskRelease(1, &command, &trade, market_data);
            const release_allocation_count = failing_allocator_state.alloc_index - release_alloc_start;
            const taker = engine.getUserProfile(taker_uid) orelse unreachable;
            const maker = engine.getUserProfile(missing_maker_uid) orelse unreachable;
            try std.testing.expectEqual(@as(i64, 996), taker.accountBalance(15));
            try std.testing.expectEqual(common.UserStatus.suspended, maker.user_status);
            try std.testing.expectEqual(@as(i64, 102), maker.accountBalance(15));
            try std.testing.expectEqual(@as(i64, 200), maker.accountBalance(11));
            try std.testing.expectEqual(@as(i64, 6), engine.getFeeCollected(15));
            try std.testing.expectEqualDeep(
                RiskEngine.LastPriceCacheRecord{ .ask_price = 51, .bid_price = 49 },
                engine.getLastPrice(241) orelse unreachable,
            );
            return release_allocation_count;
        }
    };

    const release_allocation_count = try Harness.run(null);
    try std.testing.expect(release_allocation_count > 0);
    for (0..release_allocation_count) |fail_offset| {
        _ = try Harness.run(fail_offset);
    }
}

test "exchange sell release arithmetic failure leaves all risk state unchanged" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{});
    defer engine.deinit();
    try engine.addSymbol(try makeExchangeSpec());
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(2031));
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(2032));

    const state_before = try engine.stateHash();
    var command = cmd.OrderCommand.newOrder(.gtc, 8031, 2031, 1, 1, 1, .ask);
    command.symbol = 241;
    var first_trade: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = 2032,
        .price = 1,
        .size = 1,
        .bidder_hold_price = 1,
    };
    var overflowing_trade: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = 2032,
        .price = 2,
        .size = std.math.maxInt(i64),
        .bidder_hold_price = 2,
    };
    first_trade.next_event = &overflowing_trade;

    try std.testing.expectError(
        error.ArithmeticOverflow,
        engine.handlerRiskRelease(1, &command, &first_trade, null),
    );
    try std.testing.expectEqual(state_before, try engine.stateHash());
    try std.testing.expectEqual(@as(usize, 0), engine.getUserProfile(2031).?.accounts.count());
    try std.testing.expectEqual(@as(usize, 0), engine.getUserProfile(2032).?.accounts.count());
    try std.testing.expectEqual(@as(i64, 0), engine.getFeeCollected(15));
}

test "exchange buy reject then arithmetic failure does not commit refund" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{});
    defer engine.deinit();
    try engine.addSymbol(try makeExchangeSpec());
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(2041));
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(2042));

    const state_before = try engine.stateHash();
    var command = cmd.OrderCommand.newOrder(.gtc, 8041, 2041, 60, 60, 2, .bid);
    command.symbol = 241;
    var reject: events.MatcherTradeEvent = .{
        .event_type = .reject,
        .price = 60,
        .size = 1,
        .bidder_hold_price = 60,
    };
    var overflowing_trade: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = 2042,
        .price = 2,
        .size = std.math.maxInt(i64),
        .bidder_hold_price = 2,
    };
    reject.next_event = &overflowing_trade;

    try std.testing.expectError(
        error.ArithmeticOverflow,
        engine.handlerRiskRelease(1, &command, &reject, null),
    );
    try std.testing.expectEqual(state_before, try engine.stateHash());
    try std.testing.expectEqual(@as(usize, 0), engine.getUserProfile(2041).?.accounts.count());
    try std.testing.expectEqual(@as(usize, 0), engine.getUserProfile(2042).?.accounts.count());
    try std.testing.expectEqual(@as(i64, 0), engine.getFeeCollected(15));
}

test "exchange release fee-total overflow leaves prepared account changes uncommitted" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{});
    defer engine.deinit();
    try engine.addSymbol(try makeExchangeSpec());
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(2051));
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(2052));
    try engine.fees.put(allocator, 15, std.math.maxInt(i64));

    const state_before = try engine.stateHash();
    var command = cmd.OrderCommand.newOrder(.gtc, 8051, 2051, 50, 55, 1, .ask);
    command.symbol = 241;
    var trade: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = 2052,
        .price = 50,
        .size = 1,
        .bidder_hold_price = 55,
    };

    try std.testing.expectError(
        error.ArithmeticOverflow,
        engine.handlerRiskRelease(1, &command, &trade, null),
    );
    try std.testing.expectEqual(state_before, try engine.stateHash());
    try std.testing.expectEqual(@as(usize, 0), engine.getUserProfile(2051).?.accounts.count());
    try std.testing.expectEqual(@as(usize, 0), engine.getUserProfile(2052).?.accounts.count());
    try std.testing.expectEqual(std.math.maxInt(i64), engine.getFeeCollected(15));
}

test "risk release updates last price cache when margin mode enabled" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{
        .margin_trading_enabled = true,
    });
    defer engine.deinit();
    try engine.addSymbol(try makeExchangeSpec());

    var command = cmd.OrderCommand.newOrder(.gtc, 8003, 1, 1, 1, 1, .bid);
    command.symbol = 241;

    _ = try engine.handlerRiskRelease(3, &command, null, .{
        .ask_size = 1,
        .bid_size = 1,
        .ask_price_0 = 1234,
        .bid_price_0 = 1230,
    });

    const first = engine.getLastPrice(241) orelse unreachable;
    try std.testing.expectEqual(@as(i64, 1234), first.ask_price);
    try std.testing.expectEqual(@as(i64, 1230), first.bid_price);

    _ = try engine.handlerRiskRelease(4, &command, null, .{
        .ask_size = 0,
        .bid_size = 0,
    });
    const second = engine.getLastPrice(241) orelse unreachable;
    try std.testing.expectEqual(std.math.maxInt(i64), second.ask_price);
    try std.testing.expectEqual(@as(i64, 0), second.bid_price);
}

test "risk release fails when an event or market data references an unknown symbol" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{
        .margin_trading_enabled = true,
    });
    defer engine.deinit();

    var command = cmd.OrderCommand.newOrder(.gtc, 8005, 1, 100, 100, 1, .bid);
    command.symbol = 999;

    try std.testing.expectError(
        error.PositionSymbolSpecificationMissing,
        engine.handlerRiskRelease(1, &command, null, .{
            .ask_size = 1,
            .bid_size = 1,
            .ask_price_0 = 101,
            .bid_price_0 = 99,
        }),
    );

    // Java returns before resolving the specification when there is no release work.
    try std.testing.expect(!try engine.handlerRiskRelease(2, &command, null, null));
}

test "risk release margin trade updates positions and fees" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{
        .margin_trading_enabled = true,
    });
    defer engine.deinit();

    const futures_spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = 555,
        .symbol_type = .futures_contract,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 100,
        .quote_scale_k = 10,
        .taker_fee = 3,
        .maker_fee = 1,
        .margin_buy = 10,
        .margin_sell = 12,
    });
    try engine.addSymbol(futures_spec);
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(3001));
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(3002));

    const taker = engine.getUserProfile(3001) orelse unreachable;
    const maker = engine.getUserProfile(3002) orelse unreachable;

    try taker.positions.put(allocator, 555, user.SymbolPositionRecord.init(3001, 555, 15));
    try maker.positions.put(allocator, 555, user.SymbolPositionRecord.init(3002, 555, 15));
    try taker.positions.getPtr(555).?.pendingHold(.bid, 2);
    try maker.positions.getPtr(555).?.pendingHold(.ask, 2);

    var command = cmd.OrderCommand.newOrder(.gtc, 8004, 3001, 100, 100, 2, .bid);
    command.symbol = 555;

    var trade: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = 3002,
        .price = 100,
        .size = 2,
        .bidder_hold_price = 100,
    };

    _ = try engine.handlerRiskRelease(5, &command, &trade, null);

    const taker_pos = taker.positions.getPtr(555) orelse unreachable;
    const maker_pos = maker.positions.getPtr(555) orelse unreachable;

    try std.testing.expectEqual(common.PositionDirection.long, taker_pos.direction);
    try std.testing.expectEqual(common.PositionDirection.short, maker_pos.direction);
    try std.testing.expectEqual(@as(i64, 0), taker_pos.pending_buy_size);
    try std.testing.expectEqual(@as(i64, 0), maker_pos.pending_sell_size);
    try std.testing.expectEqual(@as(i64, -6), taker.accountBalance(15));
    try std.testing.expectEqual(@as(i64, -2), maker.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 8), engine.getFeeCollected(15));
}

test "prepared margin release is read-only until infallible commit" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{
        .margin_trading_enabled = true,
    });
    defer engine.deinit();
    try engine.addSymbol(try makeFuturesSpecForSymbol(555, 10, 12));
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(3011));
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(3012));

    const taker = engine.getUserProfile(3011) orelse unreachable;
    const maker = engine.getUserProfile(3012) orelse unreachable;
    try taker.positions.put(allocator, 555, user.SymbolPositionRecord.init(3011, 555, 15));
    try maker.positions.put(allocator, 555, user.SymbolPositionRecord.init(3012, 555, 15));
    try taker.positions.getPtr(555).?.pendingHold(.bid, 2);
    try maker.positions.getPtr(555).?.pendingHold(.ask, 2);
    const taker_before = taker.positions.get(555) orelse unreachable;
    const maker_before = maker.positions.get(555) orelse unreachable;

    var command = cmd.OrderCommand.newOrder(.gtc, 8011, 3011, 100, 100, 2, .bid);
    command.symbol = 555;
    var trade: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = 3012,
        .price = 100,
        .size = 2,
        .bidder_hold_price = 100,
    };

    var prepared = try engine.prepareMarginRiskRelease(&command, &trade, .{
        .ask_size = 1,
        .bid_size = 1,
        .ask_price_0 = 101,
        .bid_price_0 = 99,
    });
    defer prepared.deinit();

    try std.testing.expectEqualDeep(taker_before, taker.positions.get(555) orelse unreachable);
    try std.testing.expectEqualDeep(maker_before, maker.positions.get(555) orelse unreachable);
    try std.testing.expectEqual(@as(i64, 0), taker.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 0), maker.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 0), engine.getFeeCollected(15));
    try std.testing.expect(engine.getLastPrice(555) == null);

    engine.commitPreparedMarginRiskRelease(&prepared);
    try std.testing.expectEqual(common.PositionDirection.long, taker.positions.get(555).?.direction);
    try std.testing.expectEqual(common.PositionDirection.short, maker.positions.get(555).?.direction);
    try std.testing.expectEqual(@as(i64, -6), taker.accountBalance(15));
    try std.testing.expectEqual(@as(i64, -4), maker.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 10), engine.getFeeCollected(15));
    try std.testing.expectEqualDeep(
        RiskEngine.LastPriceCacheRecord{ .ask_price = 101, .bid_price = 99 },
        engine.getLastPrice(555) orelse unreachable,
    );
}

test "margin release stays unchanged across every allocation failure" {
    const Harness = struct {
        fn run(fail_offset: ?usize) !usize {
            var failing_allocator_state = std.testing.FailingAllocator.init(std.testing.allocator, .{});
            const allocator = failing_allocator_state.allocator();
            var engine = try RiskEngine.init(allocator, 0, 1, .{
                .margin_trading_enabled = true,
            });
            defer engine.deinit();
            try engine.addSymbol(try makeFuturesSpecForSymbol(555, 10, 12));

            const taker_uid: i64 = 3021;
            const maker_uid: i64 = 3022;
            try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(taker_uid));
            try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(maker_uid));

            const taker = engine.getUserProfile(taker_uid) orelse unreachable;
            const maker = engine.getUserProfile(maker_uid) orelse unreachable;
            try taker.putPosition(
                allocator,
                555,
                user.SymbolPositionRecord.init(taker_uid, 555, 15),
            );
            try maker.putPosition(
                allocator,
                555,
                user.SymbolPositionRecord.init(maker_uid, 555, 15),
            );
            try taker.positions.getPtr(555).?.pendingHold(.bid, 2);
            try maker.positions.getPtr(555).?.pendingHold(.ask, 2);

            const state_before = try engine.stateHash();
            const taker_before = taker.positions.get(555) orelse unreachable;
            const maker_before = maker.positions.get(555) orelse unreachable;

            var command = cmd.OrderCommand.newOrder(.gtc, 8021, taker_uid, 100, 100, 2, .bid);
            command.symbol = 555;
            var trade: events.MatcherTradeEvent = .{
                .event_type = .trade,
                .matched_order_uid = maker_uid,
                .price = 100,
                .size = 2,
                .bidder_hold_price = 100,
            };
            const market_data: events.L2Top = .{
                .ask_size = 1,
                .bid_size = 1,
                .ask_price_0 = 101,
                .bid_price_0 = 99,
            };

            const release_alloc_start = failing_allocator_state.alloc_index;
            if (fail_offset) |offset| {
                failing_allocator_state.fail_index = release_alloc_start + offset;
                try std.testing.expectError(
                    error.OutOfMemory,
                    engine.handlerRiskRelease(1, &command, &trade, market_data),
                );
                try std.testing.expect(failing_allocator_state.has_induced_failure);

                // A FailingAllocator keeps rejecting at the selected index. Disable the
                // fault before hashing the complete logical state for comparison.
                failing_allocator_state.fail_index = std.math.maxInt(usize);
                try std.testing.expectEqual(state_before, try engine.stateHash());
                try std.testing.expectEqualDeep(taker_before, taker.positions.get(555) orelse unreachable);
                try std.testing.expectEqualDeep(maker_before, maker.positions.get(555) orelse unreachable);
                try std.testing.expectEqual(@as(usize, 0), taker.accounts.count());
                try std.testing.expectEqual(@as(usize, 0), maker.accounts.count());
                try std.testing.expectEqual(@as(i64, 0), engine.getFeeCollected(15));
                try std.testing.expect(engine.getLastPrice(555) == null);
                const taker_position_keys =
                    try taker.positionKeysInHistoryOrder(allocator);
                defer allocator.free(taker_position_keys);
                try std.testing.expectEqualSlices(i32, &.{555}, taker_position_keys);
                const maker_position_keys =
                    try maker.positionKeysInHistoryOrder(allocator);
                defer allocator.free(maker_position_keys);
                try std.testing.expectEqualSlices(i32, &.{555}, maker_position_keys);
                const fee_keys = try engine.feeKeysInHistoryOrder();
                defer allocator.free(fee_keys);
                try std.testing.expectEqual(@as(usize, 0), fee_keys.len);
                return 0;
            }

            _ = try engine.handlerRiskRelease(1, &command, &trade, market_data);
            const release_allocation_count = failing_allocator_state.alloc_index - release_alloc_start;
            try std.testing.expectEqual(common.PositionDirection.long, taker.positions.get(555).?.direction);
            try std.testing.expectEqual(common.PositionDirection.short, maker.positions.get(555).?.direction);
            try std.testing.expectEqual(@as(i64, -6), taker.accountBalance(15));
            try std.testing.expectEqual(@as(i64, -4), maker.accountBalance(15));
            try std.testing.expectEqual(@as(i64, 10), engine.getFeeCollected(15));
            try std.testing.expectEqualDeep(
                RiskEngine.LastPriceCacheRecord{ .ask_price = 101, .bid_price = 99 },
                engine.getLastPrice(555) orelse unreachable,
            );
            return release_allocation_count;
        }
    };

    const release_allocation_count = try Harness.run(null);
    try std.testing.expect(release_allocation_count > 0);
    for (0..release_allocation_count) |fail_offset| {
        _ = try Harness.run(fail_offset);
    }
}

test "binary margin marker skips position processing and still commits market data" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{
        .margin_trading_enabled = true,
    });
    defer engine.deinit();
    try engine.addSymbol(try makeFuturesSpecForSymbol(555, 10, 12));

    var command = cmd.OrderCommand.newOrder(.gtc, 8031, 3031, 100, 100, 1, .bid);
    command.symbol = 555;
    var binary: events.MatcherTradeEvent = .{ .event_type = .binary_event };

    _ = try engine.handlerRiskRelease(1, &command, &binary, .{
        .ask_size = 1,
        .bid_size = 1,
        .ask_price_0 = 111,
        .bid_price_0 = 109,
    });
    try std.testing.expectEqualDeep(
        RiskEngine.LastPriceCacheRecord{ .ask_price = 111, .bid_price = 109 },
        engine.getLastPrice(555) orelse unreachable,
    );
    try std.testing.expectEqual(@as(usize, 0), engine.user_profile_service.user_profiles.count());
}

test "risk release margin reduce removes an empty position and realizes its profit" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{
        .margin_trading_enabled = true,
    });
    defer engine.deinit();
    try engine.addSymbol(try makeFuturesSpecForSymbol(555, 10, 12));
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(3051));

    const taker = engine.getUserProfile(3051) orelse unreachable;
    try taker.positions.put(allocator, 555, user.SymbolPositionRecord.init(3051, 555, 15));
    const position = taker.positions.getPtr(555) orelse unreachable;
    position.profit = 9;
    try position.pendingHold(.bid, 2);

    var command = cmd.OrderCommand.newOrder(.gtc, 8051, 3051, 100, 100, 2, .bid);
    command.symbol = 555;
    var reduce: events.MatcherTradeEvent = .{
        .event_type = .reduce,
        .price = 100,
        .size = 2,
        .bidder_hold_price = 100,
    };

    _ = try engine.handlerRiskRelease(1, &command, &reduce, null);
    try std.testing.expectEqual(@as(usize, 0), taker.positions.count());
    try std.testing.expectEqual(@as(i64, 9), taker.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 0), engine.getFeeCollected(15));
}

test "risk release exact close realizes profit and charges no opening fee" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 2, .{
        .margin_trading_enabled = true,
    });
    defer engine.deinit();
    try engine.addSymbol(try makeFuturesSpecForSymbol(555, 10, 12));

    const taker_uid: i64 = 3_402;
    const remote_maker_uid: i64 = 3_403;
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(taker_uid));
    const taker = engine.getUserProfile(taker_uid) orelse unreachable;
    try taker.putAccount(allocator, 15, 100);

    var position = user.SymbolPositionRecord.init(taker_uid, 555, 15);
    position.direction = .long;
    position.open_volume = 2;
    position.open_price_sum = 200;
    position.pending_sell_size = 2;
    try taker.putPosition(allocator, 555, position);

    var command = cmd.OrderCommand.newOrder(.ioc, 8_052, taker_uid, 110, 0, 2, .ask);
    command.symbol = 555;
    var trade: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = remote_maker_uid,
        .price = 110,
        .size = 2,
        .bidder_hold_price = 110,
    };

    _ = try engine.handlerRiskRelease(1, &command, &trade, null);
    try std.testing.expectEqual(@as(i64, 120), taker.accountBalance(15));
    try std.testing.expectEqual(@as(usize, 0), taker.positions.count());
    try std.testing.expectEqual(@as(i64, 0), engine.getFeeCollected(15));
    const position_keys = try taker.positionKeysInHistoryOrder(allocator);
    defer allocator.free(position_keys);
    try std.testing.expectEqual(@as(usize, 0), position_keys.len);
    var raw = try taker.positions_history.exportRaw(allocator);
    defer raw.deinit(allocator);
    var tombstones: usize = 0;
    for (raw.slots) |key| tombstones += @intFromBool(key == 1);
    try std.testing.expectEqual(@as(usize, 1), tombstones);
}

test "risk release margin requires existing positions without inserting or partially settling" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{
        .margin_trading_enabled = true,
    });
    defer engine.deinit();
    try engine.addSymbol(try makeFuturesSpecForSymbol(555, 10, 12));
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(3101));
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(3102));

    const taker = engine.getUserProfile(3101) orelse unreachable;
    const maker = engine.getUserProfile(3102) orelse unreachable;
    try taker.accounts.put(allocator, 15, 100);
    try maker.accounts.put(allocator, 15, 200);

    var command = cmd.OrderCommand.newOrder(.gtc, 8101, 3101, 100, 100, 2, .bid);
    command.symbol = 555;
    var trade: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = 3102,
        .price = 100,
        .size = 2,
        .bidder_hold_price = 100,
    };

    // The taker record must have been created by order admission.
    try std.testing.expectError(
        error.PositionRecordCorrupted,
        engine.handlerRiskRelease(1, &command, &trade, null),
    );
    try std.testing.expectEqual(@as(usize, 0), taker.positions.count());
    try std.testing.expectEqual(@as(usize, 0), maker.positions.count());

    try taker.positions.put(allocator, 555, user.SymbolPositionRecord.init(3101, 555, 15));
    try taker.positions.getPtr(555).?.pendingHold(.bid, 2);
    const taker_before = taker.positions.get(555) orelse unreachable;

    // A missing maker record is also corruption, and the already-staged taker update
    // must not escape when that later validation fails.
    try std.testing.expectError(
        error.PositionRecordCorrupted,
        engine.handlerRiskRelease(2, &command, &trade, null),
    );
    try std.testing.expectEqualDeep(taker_before, taker.positions.get(555) orelse unreachable);
    try std.testing.expectEqual(@as(usize, 0), maker.positions.count());
    try std.testing.expectEqual(@as(i64, 100), taker.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 200), maker.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 0), engine.getFeeCollected(15));
}

test "risk release margin event chain is atomic when a later settlement overflows" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{
        .margin_trading_enabled = true,
    });
    defer engine.deinit();
    try engine.addSymbol(try makeFuturesSpecForSymbol(555, 10, 12));

    const taker_uid = 3201;
    const first_maker_uid = 3202;
    const second_maker_uid = 3203;
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(taker_uid));
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(first_maker_uid));
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(second_maker_uid));

    const taker = engine.getUserProfile(taker_uid) orelse unreachable;
    const first_maker = engine.getUserProfile(first_maker_uid) orelse unreachable;
    const second_maker = engine.getUserProfile(second_maker_uid) orelse unreachable;
    try taker.accounts.put(allocator, 15, 100);
    try first_maker.accounts.put(allocator, 15, 100);
    try second_maker.accounts.put(allocator, 15, std.math.minInt(i64));
    try engine.fees.put(allocator, 15, 7);

    try taker.positions.put(allocator, 555, user.SymbolPositionRecord.init(taker_uid, 555, 15));
    try first_maker.positions.put(allocator, 555, user.SymbolPositionRecord.init(first_maker_uid, 555, 15));
    try second_maker.positions.put(allocator, 555, user.SymbolPositionRecord.init(second_maker_uid, 555, 15));
    try taker.positions.getPtr(555).?.pendingHold(.bid, 2);
    try first_maker.positions.getPtr(555).?.pendingHold(.ask, 1);
    try second_maker.positions.getPtr(555).?.pendingHold(.ask, 1);

    const taker_before = taker.positions.get(555) orelse unreachable;
    const first_maker_before = first_maker.positions.get(555) orelse unreachable;
    const second_maker_before = second_maker.positions.get(555) orelse unreachable;

    var command = cmd.OrderCommand.newOrder(.gtc, 8201, taker_uid, 100, 100, 2, .bid);
    command.symbol = 555;
    var first_trade: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = first_maker_uid,
        .price = 100,
        .size = 1,
        .bidder_hold_price = 100,
    };
    var second_trade: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = second_maker_uid,
        .price = 100,
        .size = 1,
        .bidder_hold_price = 100,
    };
    first_trade.next_event = &second_trade;

    try std.testing.expectError(
        error.ArithmeticOverflow,
        engine.handlerRiskRelease(1, &command, &first_trade, null),
    );
    try std.testing.expectEqualDeep(taker_before, taker.positions.get(555) orelse unreachable);
    try std.testing.expectEqualDeep(first_maker_before, first_maker.positions.get(555) orelse unreachable);
    try std.testing.expectEqualDeep(second_maker_before, second_maker.positions.get(555) orelse unreachable);
    try std.testing.expectEqual(@as(i64, 100), taker.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 100), first_maker.accountBalance(15));
    try std.testing.expectEqual(std.math.minInt(i64), second_maker.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 7), engine.getFeeCollected(15));
}

test "risk report estimation handles bid-only and ask-only price caches" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{
        .margin_trading_enabled = true,
    });
    defer engine.deinit();
    const futures_spec = try makeFuturesSpecForSymbol(555, 10, 12);

    var position = user.SymbolPositionRecord.init(3301, 555, 15);
    position.direction = .long;
    position.open_volume = 1;
    position.open_price_sum = 10;
    position.profit = 3;
    try engine.last_price_cache.put(allocator, 555, .{
        .ask_price = std.math.maxInt(i64),
        .bid_price = 100,
    });
    const bid_only_average = (std.math.maxInt(i64) +% @as(i64, 100)) >> 1;
    try std.testing.expectEqual(
        bid_only_average - 10 + 3,
        try engine.estimatePositionProfitForReport(&position, futures_spec),
    );

    position.direction = .short;
    position.open_price_sum = 80;
    try engine.last_price_cache.put(allocator, 555, .{
        .ask_price = 100,
        .bid_price = 0,
    });
    try std.testing.expectEqual(
        @as(i64, 33),
        try engine.estimatePositionProfitForReport(&position, futures_spec),
    );
}

test "risk release returns arithmetic overflow for oversized trade math" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{});
    defer engine.deinit();
    try engine.addSymbol(try makeExchangeSpec());
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(4001));

    var command = cmd.OrderCommand.newOrder(.gtc, 9001, 4001, 10, 10, 1, .ask);
    command.symbol = 241;
    command.command = .place_order;

    var trade: events.MatcherTradeEvent = .{
        .event_type = .trade,
        .matched_order_uid = 4999,
        .price = 2,
        .size = std.math.maxInt(i64),
        .bidder_hold_price = 2,
    };

    try std.testing.expectError(
        error.ArithmeticOverflow,
        engine.handlerRiskRelease(6, &command, &trade, null),
    );
}

test "risk live symbol admission filters disabled futures while raw registration remains unconditional" {
    const allocator = std.testing.allocator;
    const futures = try makeFuturesSpecForSymbol(555, 10, 12);

    var live_disabled = try RiskEngine.init(allocator, 0, 1, .{});
    defer live_disabled.deinit();
    const disabled_empty_hash = try live_disabled.stateHash();
    try live_disabled.addSymbolForLiveAdmission(futures);
    try std.testing.expect(live_disabled.symbol_spec_provider.getSymbolSpecification(futures.symbol_id) == null);
    try std.testing.expectEqual(@as(usize, 0), live_disabled.symbol_spec_provider.symbols.count());
    try std.testing.expectEqual(disabled_empty_hash, try live_disabled.stateHash());

    const exchange = try makeExchangeSpec();
    try live_disabled.addSymbolForLiveAdmission(exchange);
    try std.testing.expect(live_disabled.symbol_spec_provider.getSymbolSpecification(exchange.symbol_id) != null);

    var raw_disabled = try RiskEngine.init(allocator, 0, 1, .{});
    defer raw_disabled.deinit();
    try raw_disabled.addSymbol(futures);
    try std.testing.expect(raw_disabled.symbol_spec_provider.getSymbolSpecification(futures.symbol_id) != null);

    var enabled = try RiskEngine.init(allocator, 0, 1, .{
        .margin_trading_enabled = true,
    });
    defer enabled.deinit();
    try enabled.addSymbolForLiveAdmission(futures);
    try std.testing.expect(enabled.symbol_spec_provider.getSymbolSpecification(futures.symbol_id) != null);
}

test "preReserveUsers grows profile map without inserts" {
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{});
    defer engine.deinit();
    try engine.preReserveUsers(32);
    try std.testing.expectEqual(@as(usize, 0), engine.user_profile_service.user_profiles.count());
    try std.testing.expect(try engine.user_profile_service.addEmptyUserProfile(7));
}

test "pure capacity reject event restores exchange R1 hold via R2" {
    // Models PR5 pure-capacity settlement: R1 holds, matching returns -3060 with
    // a full-size reject, R2 refunds using existing reject math.
    const allocator = std.testing.allocator;
    var engine = try RiskEngine.init(allocator, 0, 1, .{});
    defer engine.deinit();
    try engine.addSymbol(try makeExchangeSpec());

    var add_user: cmd.OrderCommand = .{
        .command = .add_user,
        .uid = 7701,
        .result_code = .new,
    };
    _ = try engine.preProcessCommand(1, &add_user);

    var deposit: cmd.OrderCommand = .{
        .command = .balance_adjustment,
        .uid = 7701,
        .symbol = 15,
        .price = 10_000,
        .order_id = 1,
        .order_type = .gtc,
        .result_code = .new,
    };
    _ = try engine.preProcessCommand(2, &deposit);

    const profile = engine.getUserProfile(7701) orelse unreachable;
    const balance_before = profile.accountBalance(15);

    var place = cmd.OrderCommand.newOrder(.gtc, 8801, 7701, 20, 20, 3, .bid);
    place.symbol = 241;
    _ = try engine.preProcessCommand(3, &place);
    try std.testing.expectEqual(cmd.CommandResultCode.valid_for_matching_engine, place.result_code);
    const held = balance_before - profile.accountBalance(15);
    try std.testing.expect(held > 0);

    // Full-size reject as pure-capacity path emits (size == remaining == 3).
    var reject: events.MatcherTradeEvent = .{
        .event_type = .reject,
        .active_order_completed = true,
        .price = 20,
        .size = 3,
        .bidder_hold_price = 20,
    };
    place.result_code = .matching_engine_capacity;
    _ = try engine.handlerRiskRelease(3, &place, &reject, null);

    try std.testing.expectEqual(balance_before, profile.accountBalance(15));
}
