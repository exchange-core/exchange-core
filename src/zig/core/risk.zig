const std = @import("std");
const common = @import("common.zig");
const cmd = @import("cmd.zig");
const user = @import("user_profile.zig");
const events = @import("events.zig");

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

pub const ReleaseError = std.mem.Allocator.Error || error{
    ArithmeticOverflow,
    PositionRecordCorrupted,
};

pub const RiskEngine = struct {
    pub const LastPriceCacheRecord = struct {
        ask_price: i64 = std.math.maxInt(i64),
        bid_price: i64 = 0,
    };

    allocator: std.mem.Allocator,
    shard_id: u64,
    shard_mask: u64,
    cfg_ignore_risk_processing: bool,
    cfg_margin_trading_enabled: bool,
    symbol_spec_provider: SymbolSpecificationProvider,
    user_profile_service: user.UserProfileService,
    last_price_cache: std.AutoHashMapUnmanaged(i32, LastPriceCacheRecord) = .empty,
    fees: std.AutoHashMapUnmanaged(i32, i64) = .empty,
    adjustments: std.AutoHashMapUnmanaged(i32, i64) = .empty,
    suspends: std.AutoHashMapUnmanaged(i32, i64) = .empty,

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
        self.adjustments.deinit(self.allocator);
        self.suspends.deinit(self.allocator);
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

    pub fn getUserProfile(self: *RiskEngine, uid: i64) ?*user.UserProfile {
        return self.user_profile_service.getUserProfile(uid);
    }

    pub fn getLastPrice(self: *const RiskEngine, symbol: i32) ?LastPriceCacheRecord {
        return self.last_price_cache.get(symbol);
    }

    pub fn getFeeCollected(self: *const RiskEngine, currency: i32) i64 {
        return self.fees.get(currency) orelse 0;
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
    ) std.mem.Allocator.Error!bool {
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
                    command.result_code = if (try self.user_profile_service.addEmptyUserProfile(command.uid))
                        .success
                    else
                        .user_mgmt_user_already_exists;
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
    ) std.mem.Allocator.Error!cmd.CommandResultCode {
        const res = try self.user_profile_service.balanceAdjustment(
            uid,
            currency,
            amount_diff,
            funding_transaction_id,
        );
        if (res == .success) {
            switch (adjustment_type) {
                .adjustment => try addToMapValue(self.allocator, &self.adjustments, currency, -amount_diff),
                .@"suspend" => try addToMapValue(self.allocator, &self.suspends, currency, -amount_diff),
            }
        }
        return res;
    }

    fn placeOrderRiskCheck(
        self: *RiskEngine,
        command: *cmd.OrderCommand,
    ) std.mem.Allocator.Error!cmd.CommandResultCode {
        const profile = self.user_profile_service.getUserProfile(command.uid) orelse return .auth_invalid_user;
        const spec = self.symbol_spec_provider.getSymbolSpecification(command.symbol) orelse return .invalid_symbol;

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
    ) std.mem.Allocator.Error!cmd.CommandResultCode {
        switch (spec.symbol_type) {
            .currency_exchange_pair => return try self.placeExchangeOrder(command, profile, spec),
            .futures_contract => {
                if (!self.cfg_margin_trading_enabled) {
                    return .risk_margin_trading_disabled;
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
    ) std.mem.Allocator.Error!cmd.CommandResultCode {
        const order_type = command.order_type orelse return .matching_unsupported_command;

        const currency: i32 = if (command.action == .bid) spec.quote_currency else spec.base_currency;
        const hold_amount: i64 = if (command.action == .bid) blk: {
            if (order_type == .fok_budget or order_type == .ioc_budget) {
                if (command.reserve_bid_price != command.price) {
                    return .risk_invalid_reserve_bid_price;
                }
                break :blk calcAmountBidTakerFee(command.size, command.price, spec) orelse return .risk_nsf;
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
        const new_balance = old_balance - hold_amount;
        if (new_balance < 0) {
            return .risk_nsf;
        }

        if (profile.accounts.getPtr(currency)) |balance| {
            balance.* = new_balance;
        } else {
            try profile.accounts.put(self.allocator, currency, new_balance);
        }

        return .valid_for_matching_engine;
    }

    pub fn handlerRiskRelease(
        self: *RiskEngine,
        _: u64,
        command: *cmd.OrderCommand,
        event_head: ?*events.MatcherTradeEvent,
        market_data: ?events.L2Top,
    ) ReleaseError!bool {
        var node = event_head;

        if (market_data == null and (node == null or node.?.event_type == .binary_event)) {
            return false;
        }

        const spec = self.symbol_spec_provider.getSymbolSpecification(command.symbol) orelse return false;

        if (node != null and node.?.event_type != .binary_event) {
            const taker_action = command.action orelse return false;
            const taker_sell = taker_action == .ask;
            if (spec.symbol_type == .currency_exchange_pair) {
                const taker_up = if (self.uidForThisHandler(command.uid))
                    try self.user_profile_service.getUserProfileOrAddSuspended(command.uid)
                else
                    null;

                if (node.?.event_type == .reduce or node.?.event_type == .reject) {
                    if (taker_up) |up| {
                        try self.handleMatcherRejectReduceEventExchange(command, node.?, spec, taker_sell, up);
                    }
                    node = node.?.next_event;
                }

                if (node != null) {
                    if (taker_sell) {
                        try self.handleMatcherEventsExchangeSell(node, spec, taker_up);
                    } else {
                        try self.handleMatcherEventsExchangeBuy(node, spec, taker_up, command);
                    }
                }
            } else {
                const taker_up = if (self.uidForThisHandler(command.uid))
                    try self.user_profile_service.getUserProfileOrAddSuspended(command.uid)
                else
                    null;
                const taker_spr = if (taker_up) |up|
                    try self.getPositionRecordOrInit(up, spec)
                else
                    null;

                while (node) |ev| : (node = ev.next_event) {
                    try self.handleMatcherEventMargin(ev, spec, taker_action, taker_up, taker_spr);
                }
            }
        }

        if (market_data) |md| {
            if (self.cfg_margin_trading_enabled) {
                var rec = self.last_price_cache.get(command.symbol) orelse LastPriceCacheRecord{};
                rec.ask_price = if (md.ask_size != 0) md.ask_price_0 else std.math.maxInt(i64);
                rec.bid_price = if (md.bid_size != 0) md.bid_price_0 else 0;
                try self.last_price_cache.put(self.allocator, command.symbol, rec);
            }
        }

        return false;
    }

    fn handleMatcherEventMargin(
        self: *RiskEngine,
        ev: *events.MatcherTradeEvent,
        spec: common.CoreSymbolSpecification,
        taker_action: common.OrderAction,
        taker_up: ?*user.UserProfile,
        taker_spr: ?*user.SymbolPositionRecord,
    ) ReleaseError!void {
        if (taker_up) |taker| {
            if (taker_spr) |spr| {
                switch (ev.event_type) {
                    .trade => {
                        const size_open = spr.updatePositionForMarginTrade(taker_action, ev.size, ev.price);
                        const fee = checkedMul(spec.taker_fee, size_open) orelse return error.ArithmeticOverflow;
                        try addToMapValue(self.allocator, &taker.accounts, spec.quote_currency, -fee);
                        try addToMapValue(self.allocator, &self.fees, spec.quote_currency, fee);
                    },
                    .reject, .reduce => {
                        spr.pendingRelease(taker_action, ev.size);
                    },
                    else => {},
                }

                if (spr.isEmpty()) {
                    try self.removePositionRecord(spr, taker);
                }
            }
        }

        if (ev.event_type == .trade and self.uidForThisHandler(ev.matched_order_uid)) {
            const maker = try self.user_profile_service.getUserProfileOrAddSuspended(ev.matched_order_uid);
            const maker_spr = try self.getPositionRecordOrInit(maker, spec);
            const size_open = maker_spr.updatePositionForMarginTrade(taker_action.opposite(), ev.size, ev.price);
            const fee = checkedMul(spec.maker_fee, size_open) orelse return error.ArithmeticOverflow;
            try addToMapValue(self.allocator, &maker.accounts, spec.quote_currency, -fee);
            try addToMapValue(self.allocator, &self.fees, spec.quote_currency, fee);
            if (maker_spr.isEmpty()) {
                try self.removePositionRecord(maker_spr, maker);
            }
        }
    }

    fn handleMatcherRejectReduceEventExchange(
        self: *RiskEngine,
        command: *cmd.OrderCommand,
        ev: *events.MatcherTradeEvent,
        spec: common.CoreSymbolSpecification,
        taker_sell: bool,
        taker: *user.UserProfile,
    ) ReleaseError!void {
        if (taker_sell) {
            const amount = calcAmountAsk(ev.size, spec) orelse return error.ArithmeticOverflow;
            try addToMapValue(self.allocator, &taker.accounts, spec.base_currency, amount);
            return;
        }

        const amount = if (command.command == .place_order and command.order_type == .fok_budget)
            (calcAmountBidTakerFeeForBudget(ev.size, ev.price, spec) orelse return error.ArithmeticOverflow)
        else
            (calcAmountBidTakerFee(ev.size, ev.bidder_hold_price, spec) orelse return error.ArithmeticOverflow);
        try addToMapValue(self.allocator, &taker.accounts, spec.quote_currency, amount);
    }

    fn handleMatcherEventsExchangeSell(
        self: *RiskEngine,
        event_head: ?*events.MatcherTradeEvent,
        spec: common.CoreSymbolSpecification,
        taker: ?*user.UserProfile,
    ) ReleaseError!void {
        var taker_size: i64 = 0;
        var maker_size: i64 = 0;
        var taker_size_price: i64 = 0;
        const quote_currency = spec.quote_currency;

        var node = event_head;
        while (node) |ev| : (node = ev.next_event) {
            std.debug.assert(ev.event_type == .trade);

            if (taker != null) {
                const price_size = checkedMul(ev.size, ev.price) orelse return error.ArithmeticOverflow;
                taker_size_price = checkedAdd(taker_size_price, price_size) orelse return error.ArithmeticOverflow;
                taker_size = checkedAdd(taker_size, ev.size) orelse return error.ArithmeticOverflow;
            }

            if (self.uidForThisHandler(ev.matched_order_uid)) {
                const maker = try self.user_profile_service.getUserProfileOrAddSuspended(ev.matched_order_uid);
                const price_diff = checkedSub(ev.bidder_hold_price, ev.price) orelse return error.ArithmeticOverflow;
                const release_quote = calcAmountBidReleaseCorrMaker(ev.size, price_diff, spec) orelse return error.ArithmeticOverflow;
                const gain_base = calcAmountAsk(ev.size, spec) orelse return error.ArithmeticOverflow;
                try addToMapValue(self.allocator, &maker.accounts, quote_currency, release_quote);
                try addToMapValue(self.allocator, &maker.accounts, spec.base_currency, gain_base);
                maker_size = checkedAdd(maker_size, ev.size) orelse return error.ArithmeticOverflow;
            }
        }

        if (taker) |up| {
            const scaled = checkedMul(taker_size_price, spec.quote_scale_k) orelse return error.ArithmeticOverflow;
            const fee = checkedMul(spec.taker_fee, taker_size) orelse return error.ArithmeticOverflow;
            const taker_delta = checkedSub(scaled, fee) orelse return error.ArithmeticOverflow;
            try addToMapValue(self.allocator, &up.accounts, quote_currency, taker_delta);
        }

        if (taker_size != 0 or maker_size != 0) {
            const taker_fee = checkedMul(spec.taker_fee, taker_size) orelse return error.ArithmeticOverflow;
            const maker_fee = checkedMul(spec.maker_fee, maker_size) orelse return error.ArithmeticOverflow;
            const total_fee = checkedAdd(taker_fee, maker_fee) orelse return error.ArithmeticOverflow;
            try addToMapValue(self.allocator, &self.fees, quote_currency, total_fee);
        }
    }

    fn handleMatcherEventsExchangeBuy(
        self: *RiskEngine,
        event_head: ?*events.MatcherTradeEvent,
        spec: common.CoreSymbolSpecification,
        taker: ?*user.UserProfile,
        command: *cmd.OrderCommand,
    ) ReleaseError!void {
        var taker_size: i64 = 0;
        var maker_size: i64 = 0;
        var taker_size_price_sum: i64 = 0;
        var taker_size_price_held_sum: i64 = 0;
        const quote_currency = spec.quote_currency;

        var node = event_head;
        while (node) |ev| : (node = ev.next_event) {
            std.debug.assert(ev.event_type == .trade);

            if (taker != null) {
                const taker_matched_quote = checkedMul(ev.size, ev.price) orelse return error.ArithmeticOverflow;
                const taker_held_quote = checkedMul(ev.size, ev.bidder_hold_price) orelse return error.ArithmeticOverflow;
                taker_size_price_sum = checkedAdd(taker_size_price_sum, taker_matched_quote) orelse return error.ArithmeticOverflow;
                taker_size_price_held_sum = checkedAdd(taker_size_price_held_sum, taker_held_quote) orelse return error.ArithmeticOverflow;
                taker_size = checkedAdd(taker_size, ev.size) orelse return error.ArithmeticOverflow;
            }

            if (self.uidForThisHandler(ev.matched_order_uid)) {
                const maker = try self.user_profile_service.getUserProfileOrAddSuspended(ev.matched_order_uid);
                const gained_quote = calcAmountBid(ev.size, ev.price, spec) orelse return error.ArithmeticOverflow;
                const maker_fee = checkedMul(spec.maker_fee, ev.size) orelse return error.ArithmeticOverflow;
                const maker_delta = checkedSub(gained_quote, maker_fee) orelse return error.ArithmeticOverflow;
                try addToMapValue(self.allocator, &maker.accounts, quote_currency, maker_delta);
                maker_size = checkedAdd(maker_size, ev.size) orelse return error.ArithmeticOverflow;
            }
        }

        if (taker) |up| {
            if (command.command == .place_order and command.order_type == .fok_budget) {
                taker_size_price_held_sum = command.price;
            }

            const quote_delta_steps = checkedSub(taker_size_price_held_sum, taker_size_price_sum) orelse return error.ArithmeticOverflow;
            const quote_delta = checkedMul(quote_delta_steps, spec.quote_scale_k) orelse return error.ArithmeticOverflow;
            const base_delta = checkedMul(taker_size, spec.base_scale_k) orelse return error.ArithmeticOverflow;
            try addToMapValue(self.allocator, &up.accounts, quote_currency, quote_delta);
            try addToMapValue(self.allocator, &up.accounts, spec.base_currency, base_delta);
        }

        if (taker_size != 0 or maker_size != 0) {
            const taker_fee = checkedMul(spec.taker_fee, taker_size) orelse return error.ArithmeticOverflow;
            const maker_fee = checkedMul(spec.maker_fee, maker_size) orelse return error.ArithmeticOverflow;
            const total_fee = checkedAdd(taker_fee, maker_fee) orelse return error.ArithmeticOverflow;
            try addToMapValue(self.allocator, &self.fees, quote_currency, total_fee);
        }
    }

    fn removePositionRecord(
        self: *RiskEngine,
        record: *user.SymbolPositionRecord,
        profile: *user.UserProfile,
    ) std.mem.Allocator.Error!void {
        try addToMapValue(self.allocator, &profile.accounts, record.currency, record.profit);
        _ = profile.positions.fetchRemove(record.symbol);
    }

    fn getPositionRecordOrInit(
        self: *RiskEngine,
        profile: *user.UserProfile,
        spec: common.CoreSymbolSpecification,
    ) ReleaseError!*user.SymbolPositionRecord {
        if (profile.positions.getPtr(spec.symbol_id)) |existing| {
            return existing;
        }
        try profile.positions.put(
            self.allocator,
            spec.symbol_id,
            user.SymbolPositionRecord.init(profile.uid, spec.symbol_id, spec.quote_currency),
        );
        return profile.positions.getPtr(spec.symbol_id) orelse return error.PositionRecordCorrupted;
    }

    pub fn reset(self: *RiskEngine) void {
        self.user_profile_service.reset();
        self.symbol_spec_provider.reset();
        self.last_price_cache.clearRetainingCapacity();
        self.fees.clearRetainingCapacity();
        self.adjustments.clearRetainingCapacity();
        self.suspends.clearRetainingCapacity();
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

fn addToMapValue(
    allocator: std.mem.Allocator,
    map: *std.AutoHashMapUnmanaged(i32, i64),
    key: i32,
    delta: i64,
) std.mem.Allocator.Error!void {
    if (map.getPtr(key)) |value| {
        value.* += delta;
    } else {
        try map.put(allocator, key, delta);
    }
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
                        .result_code = .@"new",
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
                        .result_code = .@"new",
                    };
                    next_funding_tx_id += 1;
                } else if (op_roll < 62) {
                    operation = .suspend_user;
                    command = .{
                        .command = .suspend_user,
                        .uid = uid,
                        .result_code = .@"new",
                    };
                } else if (op_roll < 76) {
                    operation = .resume_user;
                    command = .{
                        .command = .resume_user,
                        .uid = uid,
                        .result_code = .@"new",
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
    taker.positions.getPtr(555).?.pendingHold(.bid, 2);
    maker.positions.getPtr(555).?.pendingHold(.ask, 2);

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
