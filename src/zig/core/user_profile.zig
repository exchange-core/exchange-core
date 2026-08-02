const std = @import("std");
const common = @import("common.zig");
const cmd = @import("cmd.zig");
const int_map_order = @import("compat/java/eclipse_collections/int_map_order.zig");
const long_map_order = @import("compat/java/eclipse_collections/long_map_order.zig");

pub const StateHashError = std.mem.Allocator.Error || error{
    StateHashCorrupted,
};

pub const MarginMathError = error{
    ArithmeticOverflow,
};

fn checkedMarginAdd(a: i64, b: i64) MarginMathError!i64 {
    return std.math.add(i64, a, b) catch error.ArithmeticOverflow;
}

fn checkedMarginSub(a: i64, b: i64) MarginMathError!i64 {
    return std.math.sub(i64, a, b) catch error.ArithmeticOverflow;
}

fn checkedMarginMul(a: i64, b: i64) MarginMathError!i64 {
    return std.math.mul(i64, a, b) catch error.ArithmeticOverflow;
}

fn narrowMarginI128(value: i128) MarginMathError!i64 {
    return std.math.cast(i64, value) orelse error.ArithmeticOverflow;
}

fn invariantViolation(comptime message: []const u8) noreturn {
    @panic(message);
}

pub const SymbolPositionRecord = struct {
    uid: i64,
    symbol: i32,
    currency: i32,
    direction: common.PositionDirection = .empty,
    open_volume: i64 = 0,
    open_price_sum: i64 = 0,
    profit: i64 = 0,
    pending_sell_size: i64 = 0,
    pending_buy_size: i64 = 0,

    pub const ValidationError = error{
        InvalidOpenStateForEmptyDirection,
        InvalidOpenStateForNonEmptyDirection,
        NegativePendingSize,
    };

    pub fn init(uid: i64, symbol: i32, currency: i32) SymbolPositionRecord {
        return .{
            .uid = uid,
            .symbol = symbol,
            .currency = currency,
        };
    }

    pub fn isEmpty(self: SymbolPositionRecord) bool {
        return self.direction == .empty and self.pending_sell_size == 0 and self.pending_buy_size == 0;
    }

    pub fn pendingHold(
        self: *SymbolPositionRecord,
        action: common.OrderAction,
        size: i64,
    ) MarginMathError!void {
        std.debug.assert(size >= 0);
        if (action == .ask) {
            self.pending_sell_size = try checkedMarginAdd(self.pending_sell_size, size);
        } else {
            self.pending_buy_size = try checkedMarginAdd(self.pending_buy_size, size);
        }
    }

    pub fn pendingRelease(
        self: *SymbolPositionRecord,
        action: common.OrderAction,
        size: i64,
    ) MarginMathError!void {
        std.debug.assert(size >= 0);
        if (action == .ask) {
            self.pending_sell_size = try checkedMarginSub(self.pending_sell_size, size);
        } else {
            self.pending_buy_size = try checkedMarginSub(self.pending_buy_size, size);
        }
    }

    pub fn estimateProfit(
        self: SymbolPositionRecord,
        spec: common.CoreSymbolSpecification,
        last_bid_price: ?i64,
        last_ask_price: ?i64,
    ) MarginMathError!i64 {
        const estimated_profit: i128 = @as(i128, self.profit) + switch (self.direction) {
            .empty => 0,
            .long => if (last_bid_price) |bid_price|
                if (bid_price != 0)
                    @as(i128, self.open_volume) * @as(i128, bid_price) -
                        @as(i128, self.open_price_sum)
                else
                    @as(i128, spec.margin_buy) * @as(i128, self.open_volume)
            else
                @as(i128, spec.margin_buy) * @as(i128, self.open_volume),
            .short => if (last_ask_price) |ask_price|
                if (ask_price != std.math.maxInt(i64))
                    @as(i128, self.open_price_sum) -
                        @as(i128, self.open_volume) * @as(i128, ask_price)
                else
                    @as(i128, spec.margin_sell) * @as(i128, self.open_volume)
            else
                @as(i128, spec.margin_sell) * @as(i128, self.open_volume),
        };
        return narrowMarginI128(estimated_profit);
    }

    pub fn calculateRequiredMarginForFutures(
        self: SymbolPositionRecord,
        spec: common.CoreSymbolSpecification,
    ) MarginMathError!i64 {
        const signed_position = try checkedMarginMul(
            self.open_volume,
            @as(i64, self.direction.multiplier()),
        );
        const current_risk_buy_size = try checkedMarginAdd(self.pending_buy_size, signed_position);
        const current_risk_sell_size = try checkedMarginSub(self.pending_sell_size, signed_position);

        const margin_buy = try checkedMarginMul(spec.margin_buy, current_risk_buy_size);
        const margin_sell = try checkedMarginMul(spec.margin_sell, current_risk_sell_size);
        return @max(margin_buy, margin_sell);
    }

    pub fn calculateRequiredMarginForOrder(
        self: SymbolPositionRecord,
        spec: common.CoreSymbolSpecification,
        action: common.OrderAction,
        size: i64,
    ) MarginMathError!i64 {
        const signed_position = try checkedMarginMul(
            self.open_volume,
            @as(i64, self.direction.multiplier()),
        );
        const current_risk_buy_size = try checkedMarginAdd(self.pending_buy_size, signed_position);
        const current_risk_sell_size = try checkedMarginSub(self.pending_sell_size, signed_position);

        var margin_buy = try checkedMarginMul(spec.margin_buy, current_risk_buy_size);
        var margin_sell = try checkedMarginMul(spec.margin_sell, current_risk_sell_size);
        const current_margin = @max(margin_buy, margin_sell);

        if (action == .bid) {
            margin_buy = try checkedMarginAdd(
                margin_buy,
                try checkedMarginMul(spec.margin_buy, size),
            );
        } else {
            margin_sell = try checkedMarginAdd(
                margin_sell,
                try checkedMarginMul(spec.margin_sell, size),
            );
        }
        const new_margin = @max(margin_buy, margin_sell);

        return if (new_margin <= current_margin) -1 else new_margin;
    }

    pub fn updatePositionForMarginTrade(
        self: *SymbolPositionRecord,
        action: common.OrderAction,
        size: i64,
        price: i64,
    ) MarginMathError!i64 {
        var next = self.*;
        try next.pendingRelease(action, size);
        const size_to_open = try next.closeCurrentPositionFutures(action, size, price);
        if (size_to_open > 0) {
            try next.openPositionMargin(action, size_to_open, price);
        }
        self.* = next;
        return size_to_open;
    }

    fn closeCurrentPositionFutures(
        self: *SymbolPositionRecord,
        action: common.OrderAction,
        trade_size: i64,
        trade_price: i64,
    ) MarginMathError!i64 {
        if (self.direction == .empty or self.direction == common.PositionDirection.fromAction(action)) {
            return trade_size;
        }

        if (self.open_volume > trade_size) {
            self.open_volume = try checkedMarginSub(self.open_volume, trade_size);
            self.open_price_sum = try checkedMarginSub(
                self.open_price_sum,
                try checkedMarginMul(trade_size, trade_price),
            );
            return 0;
        }

        const closing_value = try checkedMarginMul(self.open_volume, trade_price);
        const closing_profit = try checkedMarginMul(
            try checkedMarginSub(closing_value, self.open_price_sum),
            @as(i64, self.direction.multiplier()),
        );
        self.profit = try checkedMarginAdd(self.profit, closing_profit);
        self.open_price_sum = 0;
        self.direction = .empty;
        const size_to_open = try checkedMarginSub(trade_size, self.open_volume);
        self.open_volume = 0;
        return size_to_open;
    }

    fn openPositionMargin(
        self: *SymbolPositionRecord,
        action: common.OrderAction,
        size_to_open: i64,
        trade_price: i64,
    ) MarginMathError!void {
        self.open_volume = try checkedMarginAdd(self.open_volume, size_to_open);
        self.open_price_sum = try checkedMarginAdd(
            self.open_price_sum,
            try checkedMarginMul(trade_price, size_to_open),
        );
        self.direction = common.PositionDirection.fromAction(action);
    }

    pub fn validateInternalState(self: SymbolPositionRecord) ValidationError!void {
        if (self.direction == .empty and (self.open_volume != 0 or self.open_price_sum != 0)) {
            return error.InvalidOpenStateForEmptyDirection;
        }
        if (self.direction != .empty and (self.open_volume <= 0 or self.open_price_sum <= 0)) {
            return error.InvalidOpenStateForNonEmptyDirection;
        }
        if (self.pending_sell_size < 0 or self.pending_buy_size < 0) {
            return error.NegativePendingSize;
        }
    }

    pub fn hashInto(self: SymbolPositionRecord, hasher: *std.hash.Wyhash) void {
        hashI64(hasher, self.uid);
        hashI32(hasher, self.symbol);
        hashI32(hasher, self.currency);
        hashI8(hasher, @intFromEnum(self.direction));
        hashI64(hasher, self.open_volume);
        hashI64(hasher, self.open_price_sum);
        hashI64(hasher, self.profit);
        hashI64(hasher, self.pending_sell_size);
        hashI64(hasher, self.pending_buy_size);
    }
};

pub const UserProfile = struct {
    uid: i64,
    positions: std.AutoHashMapUnmanaged(i32, SymbolPositionRecord) = .empty,
    positions_history: int_map_order.History = int_map_order.History.init(),
    adjustments_counter: i64 = 0,
    accounts: std.AutoHashMapUnmanaged(i32, i64) = .empty,
    accounts_history: int_map_order.History = int_map_order.History.init(),
    user_status: common.UserStatus,

    pub fn init(uid: i64, user_status: common.UserStatus) UserProfile {
        return .{
            .uid = uid,
            .user_status = user_status,
        };
    }

    pub fn deinit(self: *UserProfile, allocator: std.mem.Allocator) void {
        self.positions.deinit(allocator);
        self.positions_history.deinit(allocator);
        self.accounts.deinit(allocator);
        self.accounts_history.deinit(allocator);
        self.* = undefined;
    }

    pub const HistoryValidationError = int_map_order.HistoryError || error{
        HistorySemanticKeySetMismatch,
    };

    fn validateHistoryKeys(
        comptime Value: type,
        allocator: std.mem.Allocator,
        history: *const int_map_order.History,
        map: *const std.AutoHashMapUnmanaged(i32, Value),
    ) HistoryValidationError![]i32 {
        const keys = try history.orderedKeys(allocator);
        errdefer allocator.free(keys);
        if (keys.len != map.count()) return error.HistorySemanticKeySetMismatch;
        for (keys) |key| {
            if (!map.contains(key)) return error.HistorySemanticKeySetMismatch;
        }
        return keys;
    }

    pub fn accountKeysInHistoryOrder(
        self: *const UserProfile,
        allocator: std.mem.Allocator,
    ) HistoryValidationError![]i32 {
        return validateHistoryKeys(i64, allocator, &self.accounts_history, &self.accounts);
    }

    pub fn positionKeysInHistoryOrder(
        self: *const UserProfile,
        allocator: std.mem.Allocator,
    ) HistoryValidationError![]i32 {
        return validateHistoryKeys(
            SymbolPositionRecord,
            allocator,
            &self.positions_history,
            &self.positions,
        );
    }

    pub fn validatedAccountsHistory(
        self: *const UserProfile,
        allocator: std.mem.Allocator,
    ) HistoryValidationError!*const int_map_order.History {
        const keys = try self.accountKeysInHistoryOrder(allocator);
        allocator.free(keys);
        return &self.accounts_history;
    }

    pub fn validatedPositionsHistory(
        self: *const UserProfile,
        allocator: std.mem.Allocator,
    ) HistoryValidationError!*const int_map_order.History {
        const keys = try self.positionKeysInHistoryOrder(allocator);
        allocator.free(keys);
        return &self.positions_history;
    }

    pub fn cloneAccountsHistory(
        self: *const UserProfile,
        allocator: std.mem.Allocator,
    ) HistoryValidationError!int_map_order.History {
        if (self.accounts_history.isKnown()) {
            _ = try self.validatedAccountsHistory(allocator);
        }
        return self.accounts_history.clone(allocator);
    }

    pub fn clonePositionsHistory(
        self: *const UserProfile,
        allocator: std.mem.Allocator,
    ) HistoryValidationError!int_map_order.History {
        if (self.positions_history.isKnown()) {
            _ = try self.validatedPositionsHistory(allocator);
        }
        return self.positions_history.clone(allocator);
    }

    /// Replaces the exact EC sidecar only after proving that it describes the
    /// current semantic account key set. Ownership of `next` moves on success.
    pub fn replaceAccountsHistory(
        self: *UserProfile,
        allocator: std.mem.Allocator,
        next: *int_map_order.History,
    ) HistoryValidationError!void {
        const keys = try validateHistoryKeys(i64, allocator, next, &self.accounts);
        allocator.free(keys);
        var old = self.accounts_history;
        self.accounts_history = next.*;
        next.* = int_map_order.History.initUnavailable();
        old.deinit(allocator);
    }

    /// Replaces the exact EC sidecar only after proving that it describes the
    /// current semantic position key set. Ownership of `next` moves on success.
    pub fn replacePositionsHistory(
        self: *UserProfile,
        allocator: std.mem.Allocator,
        next: *int_map_order.History,
    ) HistoryValidationError!void {
        const keys = try validateHistoryKeys(
            SymbolPositionRecord,
            allocator,
            next,
            &self.positions,
        );
        allocator.free(keys);
        var old = self.positions_history;
        self.positions_history = next.*;
        next.* = int_map_order.History.initUnavailable();
        old.deinit(allocator);
    }

    pub fn markAccountsHistoryUnavailable(
        self: *UserProfile,
        allocator: std.mem.Allocator,
    ) void {
        if (!self.accounts_history.isKnown()) return;
        self.accounts_history.deinit(allocator);
        self.accounts_history = int_map_order.History.initUnavailable();
    }

    pub fn markPositionsHistoryUnavailable(
        self: *UserProfile,
        allocator: std.mem.Allocator,
    ) void {
        if (!self.positions_history.isKnown()) return;
        self.positions_history.deinit(allocator);
        self.positions_history = int_map_order.History.initUnavailable();
    }

    pub fn putAccount(
        self: *UserProfile,
        allocator: std.mem.Allocator,
        currency: i32,
        value: i64,
    ) int_map_order.LayoutError!void {
        if (self.accounts.getPtr(currency)) |balance| {
            balance.* = value;
            return;
        }
        var next_history = try self.accounts_history.clone(allocator);
        errdefer next_history.deinit(allocator);
        try next_history.recordPut(allocator, currency);
        try self.accounts.ensureUnusedCapacity(allocator, 1);
        self.accounts.putAssumeCapacityNoClobber(currency, value);
        var old_history = self.accounts_history;
        self.accounts_history = next_history;
        old_history.deinit(allocator);
    }

    pub fn addToAccountValue(
        self: *UserProfile,
        allocator: std.mem.Allocator,
        currency: i32,
        delta: i64,
    ) int_map_order.LayoutError!void {
        if (self.accounts.getPtr(currency)) |balance| {
            balance.* += delta;
            return;
        }
        try self.putAccount(allocator, currency, delta);
    }

    pub fn putPosition(
        self: *UserProfile,
        allocator: std.mem.Allocator,
        symbol: i32,
        position: SymbolPositionRecord,
    ) int_map_order.LayoutError!void {
        if (self.positions.getPtr(symbol)) |existing| {
            existing.* = position;
            return;
        }
        var next_history = try self.positions_history.clone(allocator);
        errdefer next_history.deinit(allocator);
        try next_history.recordPut(allocator, symbol);
        try self.positions.ensureUnusedCapacity(allocator, 1);
        self.positions.putAssumeCapacityNoClobber(symbol, position);
        var old_history = self.positions_history;
        self.positions_history = next_history;
        old_history.deinit(allocator);
    }

    pub fn removePosition(self: *UserProfile, symbol: i32) ?SymbolPositionRecord {
        const removed = self.positions.fetchRemove(symbol) orelse return null;
        self.positions_history.recordRemove(symbol);
        return removed.value;
    }

    pub fn clearAccountsRetainingCapacity(self: *UserProfile) void {
        self.accounts.clearRetainingCapacity();
        self.accounts_history.clear();
    }

    pub fn clearPositionsRetainingCapacity(self: *UserProfile) void {
        self.positions.clearRetainingCapacity();
        self.positions_history.clear();
    }

    pub fn accountBalance(self: *const UserProfile, currency: i32) i64 {
        return self.accounts.get(currency) orelse 0;
    }

    pub fn hasOpenPositions(self: *const UserProfile) bool {
        var it = self.positions.valueIterator();
        while (it.next()) |record| {
            if (!record.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    pub fn hasNonZeroAccounts(self: *const UserProfile) bool {
        var it = self.accounts.valueIterator();
        while (it.next()) |balance| {
            if (balance.* != 0) {
                return true;
            }
        }
        return false;
    }

    pub fn stateHash(
        self: *const UserProfile,
        allocator: std.mem.Allocator,
    ) StateHashError!u64 {
        var hasher = std.hash.Wyhash.init(0);
        hashI64(&hasher, self.uid);
        hashI64(&hasher, self.adjustments_counter);
        hashI8(&hasher, @intFromEnum(self.user_status));
        try hashI32I64Map(allocator, &self.accounts, &hasher);
        try hashPositionsMap(allocator, &self.positions, &hasher);
        return hasher.final();
    }
};

pub const UserProfileService = struct {
    allocator: std.mem.Allocator,
    user_profiles: std.AutoHashMapUnmanaged(i64, UserProfile) = .empty,
    user_profiles_history: long_map_order.History,

    pub fn init(allocator: std.mem.Allocator) UserProfileService {
        return .{
            .allocator = allocator,
            .user_profiles_history = long_map_order.History.initWithExpectedSize(1024) catch
                unreachable,
        };
    }

    pub fn deinit(self: *UserProfileService) void {
        var it = self.user_profiles.valueIterator();
        while (it.next()) |profile| {
            profile.deinit(self.allocator);
        }
        self.user_profiles.deinit(self.allocator);
        self.user_profiles_history.deinit(self.allocator);
        self.* = undefined;
    }

    pub const HistoryValidationError = long_map_order.HistoryError || error{
        HistorySemanticKeySetMismatch,
    };

    pub fn userProfileKeysInHistoryOrder(
        self: *const UserProfileService,
    ) HistoryValidationError![]i64 {
        const keys = try self.user_profiles_history.orderedKeys(self.allocator);
        errdefer self.allocator.free(keys);
        if (keys.len != self.user_profiles.count()) {
            return error.HistorySemanticKeySetMismatch;
        }
        for (keys) |uid| {
            if (!self.user_profiles.contains(uid)) {
                return error.HistorySemanticKeySetMismatch;
            }
        }
        return keys;
    }

    pub fn validatedUserProfilesHistory(
        self: *const UserProfileService,
    ) HistoryValidationError!*const long_map_order.History {
        const keys = try self.userProfileKeysInHistoryOrder();
        self.allocator.free(keys);
        return &self.user_profiles_history;
    }

    pub fn cloneUserProfilesHistory(
        self: *const UserProfileService,
        allocator: std.mem.Allocator,
    ) HistoryValidationError!long_map_order.History {
        if (self.user_profiles_history.isKnown()) {
            const keys = try self.user_profiles_history.orderedKeys(allocator);
            defer allocator.free(keys);
            if (keys.len != self.user_profiles.count()) {
                return error.HistorySemanticKeySetMismatch;
            }
            for (keys) |uid| {
                if (!self.user_profiles.contains(uid)) {
                    return error.HistorySemanticKeySetMismatch;
                }
            }
        }
        return self.user_profiles_history.clone(allocator);
    }

    /// Ownership of `next` moves on success. On error both values are unchanged.
    pub fn replaceUserProfilesHistory(
        self: *UserProfileService,
        next: *long_map_order.History,
    ) HistoryValidationError!void {
        const keys = try next.orderedKeys(self.allocator);
        defer self.allocator.free(keys);
        if (keys.len != self.user_profiles.count()) {
            return error.HistorySemanticKeySetMismatch;
        }
        for (keys) |uid| {
            if (!self.user_profiles.contains(uid)) {
                return error.HistorySemanticKeySetMismatch;
            }
        }
        var old = self.user_profiles_history;
        self.user_profiles_history = next.*;
        next.* = long_map_order.History.initUnavailable();
        old.deinit(self.allocator);
    }

    pub fn markUserProfilesHistoryUnavailable(self: *UserProfileService) void {
        if (!self.user_profiles_history.isKnown()) return;
        self.user_profiles_history.deinit(self.allocator);
        self.user_profiles_history = long_map_order.History.initUnavailable();
    }

    fn addProfile(
        self: *UserProfileService,
        uid: i64,
        status: common.UserStatus,
    ) (std.mem.Allocator.Error || long_map_order.LayoutError)!*UserProfile {
        var next_history = try self.user_profiles_history.clone(self.allocator);
        errdefer next_history.deinit(self.allocator);
        try next_history.recordPut(self.allocator, uid);
        try self.user_profiles.ensureUnusedCapacity(self.allocator, 1);
        self.user_profiles.putAssumeCapacityNoClobber(uid, UserProfile.init(uid, status));
        var old_history = self.user_profiles_history;
        self.user_profiles_history = next_history;
        old_history.deinit(self.allocator);
        return self.user_profiles.getPtr(uid) orelse
            invariantViolation("user profile map missing inserted profile");
    }

    pub fn getUserProfile(self: *UserProfileService, uid: i64) ?*UserProfile {
        return self.user_profiles.getPtr(uid);
    }

    /// Phase C: grow the user map capacity without inserting profiles.
    pub fn preReserveUsers(self: *UserProfileService, additional: usize) std.mem.Allocator.Error!void {
        if (additional == 0) return;
        const n: u32 = std.math.cast(u32, additional) orelse return error.OutOfMemory;
        try self.user_profiles.ensureUnusedCapacity(self.allocator, n);
    }

    /// Phase C: pre-size accounts/positions maps on an existing profile.
    pub fn preReserveProfileMaps(
        self: *UserProfileService,
        uid: i64,
        accounts: usize,
        positions: usize,
    ) std.mem.Allocator.Error!void {
        const profile = self.user_profiles.getPtr(uid) orelse return;
        if (accounts > 0) {
            const n: u32 = std.math.cast(u32, accounts) orelse return error.OutOfMemory;
            try profile.accounts.ensureUnusedCapacity(self.allocator, n);
        }
        if (positions > 0) {
            const n: u32 = std.math.cast(u32, positions) orelse return error.OutOfMemory;
            try profile.positions.ensureUnusedCapacity(self.allocator, n);
        }
    }

    pub fn getUserProfileOrAddSuspended(
        self: *UserProfileService,
        uid: i64,
    ) long_map_order.LayoutError!*UserProfile {
        if (self.user_profiles.getPtr(uid)) |existing| {
            return existing;
        }
        return try self.addProfile(uid, .suspended);
    }

    pub fn addEmptyUserProfile(self: *UserProfileService, uid: i64) long_map_order.LayoutError!bool {
        if (self.user_profiles.contains(uid)) {
            return false;
        }
        _ = try self.addProfile(uid, .active);
        return true;
    }

    pub fn balanceAdjustment(
        self: *UserProfileService,
        uid: i64,
        currency: i32,
        amount: i64,
        funding_transaction_id: i64,
    ) int_map_order.LayoutError!cmd.CommandResultCode {
        const profile = self.user_profiles.getPtr(uid) orelse return .auth_invalid_user;

        if (profile.adjustments_counter == funding_transaction_id) {
            return .user_mgmt_account_balance_adjustment_already_applied_same;
        }
        if (profile.adjustments_counter > funding_transaction_id) {
            return .user_mgmt_account_balance_adjustment_already_applied_many;
        }

        const old_balance = profile.accountBalance(currency);
        if (amount < 0 and old_balance + amount < 0) {
            return .user_mgmt_account_balance_adjustment_nsf;
        }

        try profile.addToAccountValue(self.allocator, currency, amount);
        profile.adjustments_counter = funding_transaction_id;

        return .success;
    }

    pub fn suspendUserProfile(self: *UserProfileService, uid: i64) cmd.CommandResultCode {
        const profile = self.user_profiles.getPtr(uid) orelse return .user_mgmt_user_not_found;

        if (profile.user_status == .suspended) {
            return .user_mgmt_user_already_suspended;
        }
        if (profile.hasOpenPositions()) {
            return .user_mgmt_user_not_suspendable_has_positions;
        }
        if (profile.hasNonZeroAccounts()) {
            return .user_mgmt_user_not_suspendable_non_empty_accounts;
        }

        const removed = self.user_profiles.fetchRemove(uid) orelse
            invariantViolation("user profile map missing removed suspended profile");
        self.user_profiles_history.recordRemove(uid);
        var removed_profile = removed.value;
        removed_profile.deinit(self.allocator);
        return .success;
    }

    pub fn resumeUserProfile(
        self: *UserProfileService,
        uid: i64,
    ) long_map_order.LayoutError!cmd.CommandResultCode {
        const profile = self.user_profiles.getPtr(uid);
        if (profile == null) {
            _ = try self.addProfile(uid, .active);
            return .success;
        }

        if (profile.?.user_status != .suspended) {
            return .user_mgmt_user_not_suspended;
        }

        profile.?.user_status = .active;
        return .success;
    }

    pub fn reset(self: *UserProfileService) void {
        var it = self.user_profiles.valueIterator();
        while (it.next()) |profile| {
            profile.deinit(self.allocator);
        }
        self.user_profiles.clearRetainingCapacity();
        self.user_profiles_history.clear();
    }

    pub fn stateHash(self: *const UserProfileService) StateHashError!u64 {
        var hasher = std.hash.Wyhash.init(0);
        var uids: std.ArrayListUnmanaged(i64) = .empty;
        defer uids.deinit(self.allocator);

        try uids.ensureTotalCapacityPrecise(self.allocator, self.user_profiles.count());
        var it = self.user_profiles.iterator();
        while (it.next()) |entry| {
            uids.appendAssumeCapacity(entry.key_ptr.*);
        }
        std.sort.heap(i64, uids.items, {}, lessThanI64);

        for (uids.items) |uid| {
            const profile = self.user_profiles.get(uid) orelse return error.StateHashCorrupted;
            hashI64(&hasher, uid);
            hashU64(&hasher, try profile.stateHash(self.allocator));
        }
        return hasher.final();
    }
};

fn lessThanI32(_: void, lhs: i32, rhs: i32) bool {
    return lhs < rhs;
}

fn lessThanI64(_: void, lhs: i64, rhs: i64) bool {
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

fn hashPositionsMap(
    allocator: std.mem.Allocator,
    map: *const std.AutoHashMapUnmanaged(i32, SymbolPositionRecord),
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
        const record = map.get(key) orelse return error.StateHashCorrupted;
        hashI32(hasher, key);
        record.hashInto(hasher);
    }
}

fn marginTestSpecification() common.CoreSymbolSpecification.ValidationError!common.CoreSymbolSpecification {
    return common.CoreSymbolSpecification.init(.{
        .symbol_id = 241,
        .symbol_type = .futures_contract,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 1_000_000,
        .quote_scale_k = 10_000,
        .taker_fee = 1900,
        .maker_fee = 700,
        .margin_buy = 10,
        .margin_sell = 20,
    });
}

test "user profile service add and balance adjustment semantics" {
    const allocator = std.testing.allocator;
    var svc = UserProfileService.init(allocator);
    defer svc.deinit();

    try std.testing.expect(try svc.addEmptyUserProfile(1001));
    try std.testing.expect(!(try svc.addEmptyUserProfile(1001)));

    try std.testing.expectEqual(
        cmd.CommandResultCode.auth_invalid_user,
        try svc.balanceAdjustment(9999, 11, 100, 1),
    );

    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try svc.balanceAdjustment(1001, 11, 100, 1),
    );
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_account_balance_adjustment_already_applied_same,
        try svc.balanceAdjustment(1001, 11, 100, 1),
    );
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_account_balance_adjustment_already_applied_many,
        try svc.balanceAdjustment(1001, 11, 100, 0),
    );
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_account_balance_adjustment_nsf,
        try svc.balanceAdjustment(1001, 11, -200, 2),
    );
    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try svc.balanceAdjustment(1001, 11, -90, 2),
    );

    const profile = svc.getUserProfile(1001) orelse unreachable;
    try std.testing.expectEqual(@as(i64, 10), profile.accountBalance(11));
}

test "user profile service suspend and resume constraints" {
    const allocator = std.testing.allocator;
    var svc = UserProfileService.init(allocator);
    defer svc.deinit();

    try std.testing.expect(try svc.addEmptyUserProfile(2001));

    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try svc.balanceAdjustment(2001, 7, 50, 1),
    );
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_user_not_suspendable_non_empty_accounts,
        svc.suspendUserProfile(2001),
    );

    try std.testing.expectEqual(
        cmd.CommandResultCode.success,
        try svc.balanceAdjustment(2001, 7, -50, 2),
    );

    const profile = svc.getUserProfile(2001) orelse unreachable;
    try profile.positions.put(allocator, 241, SymbolPositionRecord.init(2001, 241, 7));
    const pos = profile.positions.getPtr(241) orelse unreachable;
    try pos.pendingHold(.bid, 2);

    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_user_not_suspendable_has_positions,
        svc.suspendUserProfile(2001),
    );

    _ = profile.positions.fetchRemove(241);
    try std.testing.expectEqual(cmd.CommandResultCode.success, svc.suspendUserProfile(2001));
    try std.testing.expectEqual(@as(?*UserProfile, null), svc.getUserProfile(2001));

    try std.testing.expectEqual(cmd.CommandResultCode.success, try svc.resumeUserProfile(2001));
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_user_not_suspended,
        try svc.resumeUserProfile(2001),
    );
}

test "getUserProfileOrAddSuspended creates suspended profile and resume activates it" {
    const allocator = std.testing.allocator;
    var svc = UserProfileService.init(allocator);
    defer svc.deinit();

    const suspended = try svc.getUserProfileOrAddSuspended(3001);
    try std.testing.expectEqual(common.UserStatus.suspended, suspended.user_status);
    try std.testing.expectEqual(cmd.CommandResultCode.success, try svc.resumeUserProfile(3001));
    try std.testing.expectEqual(common.UserStatus.active, suspended.user_status);
}

test "user profile Eclipse histories use Java capacities and validate raw state" {
    const allocator = std.testing.allocator;
    var svc = UserProfileService.init(allocator);
    defer svc.deinit();

    try std.testing.expectEqual(
        @as(?usize, 2048),
        svc.user_profiles_history.tableSizeIfKnown(),
    );
    try std.testing.expect(try svc.addEmptyUserProfile(41));
    const profile = svc.getUserProfile(41) orelse unreachable;
    try std.testing.expectEqual(@as(?usize, 16), profile.accounts_history.tableSizeIfKnown());
    try std.testing.expectEqual(@as(?usize, 16), profile.positions_history.tableSizeIfKnown());

    try profile.putAccount(allocator, 7, 0);
    const account_keys = try profile.accountKeysInHistoryOrder(allocator);
    defer allocator.free(account_keys);
    try std.testing.expectEqualSlices(i32, &.{7}, account_keys);

    try profile.accounts.put(allocator, 9, 0);
    try std.testing.expectError(
        error.HistorySemanticKeySetMismatch,
        profile.accountKeysInHistoryOrder(allocator),
    );
}

test "suspend resume reuses the outer tombstone and reset retains Java capacity" {
    const allocator = std.testing.allocator;
    var svc = UserProfileService.init(allocator);
    defer svc.deinit();

    try std.testing.expect(try svc.addEmptyUserProfile(42));
    try std.testing.expectEqual(cmd.CommandResultCode.success, svc.suspendUserProfile(42));
    try std.testing.expectEqual(cmd.CommandResultCode.success, try svc.resumeUserProfile(42));

    const keys = try svc.userProfileKeysInHistoryOrder();
    defer allocator.free(keys);
    try std.testing.expectEqualSlices(i64, &.{42}, keys);
    var raw = try svc.user_profiles_history.exportRaw(allocator);
    defer raw.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 2048), raw.table_length);
    var tombstones: usize = 0;
    for (raw.slots) |key| tombstones += @intFromBool(key == 1);
    try std.testing.expectEqual(@as(usize, 0), tombstones);

    svc.reset();
    try std.testing.expectEqual(@as(?usize, 2048), svc.user_profiles_history.tableSizeIfKnown());
    const empty = try svc.userProfileKeysInHistoryOrder();
    defer allocator.free(empty);
    try std.testing.expectEqual(@as(usize, 0), empty.len);
}

test "profile clear retains grown Eclipse history capacities" {
    const allocator = std.testing.allocator;
    var profile = UserProfile.init(99, .active);
    defer profile.deinit(allocator);

    for (2..12) |key| {
        const currency: i32 = @intCast(key);
        try profile.putAccount(allocator, currency, 0);
        try profile.putPosition(
            allocator,
            currency,
            SymbolPositionRecord.init(99, currency, currency),
        );
    }
    const accounts_capacity = profile.accounts_history.tableSizeIfKnown().?;
    const positions_capacity = profile.positions_history.tableSizeIfKnown().?;
    try std.testing.expect(accounts_capacity > 16);
    try std.testing.expect(positions_capacity > 16);

    profile.clearAccountsRetainingCapacity();
    profile.clearPositionsRetainingCapacity();
    try std.testing.expectEqual(accounts_capacity, profile.accounts_history.tableSizeIfKnown().?);
    try std.testing.expectEqual(positions_capacity, profile.positions_history.tableSizeIfKnown().?);
}

test "user profile service state hash is order-independent and mutation-sensitive" {
    const allocator = std.testing.allocator;
    var left = UserProfileService.init(allocator);
    defer left.deinit();
    var right = UserProfileService.init(allocator);
    defer right.deinit();

    try std.testing.expect(try left.addEmptyUserProfile(11));
    try std.testing.expect(try left.addEmptyUserProfile(12));
    try std.testing.expectEqual(cmd.CommandResultCode.success, try left.balanceAdjustment(11, 7, 100, 1));
    try std.testing.expectEqual(cmd.CommandResultCode.success, try left.balanceAdjustment(12, 9, 50, 1));

    try std.testing.expect(try right.addEmptyUserProfile(12));
    try std.testing.expect(try right.addEmptyUserProfile(11));
    try std.testing.expectEqual(cmd.CommandResultCode.success, try right.balanceAdjustment(12, 9, 50, 1));
    try std.testing.expectEqual(cmd.CommandResultCode.success, try right.balanceAdjustment(11, 7, 100, 1));

    const left_hash = try left.stateHash();
    const right_hash = try right.stateHash();
    try std.testing.expectEqual(left_hash, right_hash);

    try std.testing.expectEqual(cmd.CommandResultCode.success, try right.balanceAdjustment(11, 7, -1, 2));
    const changed_hash = try right.stateHash();
    try std.testing.expect(left_hash != changed_hash);
}

test "symbol position record margin math and transitions" {
    var record = SymbolPositionRecord.init(4001, 241, 15);
    const spec = try marginTestSpecification();

    try record.pendingHold(.bid, 5);
    try std.testing.expectEqual(@as(i64, 50), try record.calculateRequiredMarginForFutures(spec));
    try std.testing.expectEqual(@as(i64, 80), try record.calculateRequiredMarginForOrder(spec, .ask, 4));

    _ = try record.updatePositionForMarginTrade(.bid, 5, 100);
    try std.testing.expectEqual(common.PositionDirection.long, record.direction);
    try std.testing.expectEqual(@as(i64, 5), record.open_volume);
    try std.testing.expectEqual(@as(i64, 500), record.open_price_sum);
    try record.validateInternalState();
}

test "symbol position estimate profit preserves empty and long Java semantics" {
    const spec = try marginTestSpecification();

    var empty = SymbolPositionRecord.init(4002, 241, 15);
    empty.profit = 55;
    try std.testing.expectEqual(@as(i64, 55), try empty.estimateProfit(spec, null, null));

    var long = SymbolPositionRecord.init(4002, 241, 15);
    long.direction = .long;
    long.open_volume = 3;
    long.open_price_sum = 270;
    long.profit = 7;

    try std.testing.expectEqual(@as(i64, 97), try long.estimateProfit(spec, 120, null));
    try std.testing.expectEqual(@as(i64, 37), try long.estimateProfit(spec, null, null));
    try std.testing.expectEqual(@as(i64, 37), try long.estimateProfit(spec, 0, null));
}

test "symbol position estimate profit preserves short Java semantics" {
    const spec = try marginTestSpecification();
    var short = SymbolPositionRecord.init(4003, 241, 15);
    short.direction = .short;
    short.open_volume = 4;
    short.open_price_sum = 520;
    short.profit = -5;

    try std.testing.expectEqual(@as(i64, 115), try short.estimateProfit(spec, null, 100));
    try std.testing.expectEqual(@as(i64, 75), try short.estimateProfit(spec, null, null));
    try std.testing.expectEqual(
        @as(i64, 75),
        try short.estimateProfit(spec, null, std.math.maxInt(i64)),
    );
}

test "symbol position estimate profit narrows only the final wide result" {
    var spec = try marginTestSpecification();
    spec.margin_buy = 2;
    spec.margin_sell = 2;

    const max_i64 = std.math.maxInt(i64);
    const min_i64 = std.math.minInt(i64);

    var long_marked = SymbolPositionRecord.init(4004, 241, 15);
    long_marked.direction = .long;
    long_marked.open_volume = max_i64;
    long_marked.open_price_sum = max_i64;
    try std.testing.expectEqual(max_i64, try long_marked.estimateProfit(spec, 2, null));

    var short_marked = SymbolPositionRecord.init(4005, 241, 15);
    short_marked.direction = .short;
    short_marked.open_volume = max_i64;
    short_marked.open_price_sum = max_i64;
    try std.testing.expectEqual(min_i64 + 1, try short_marked.estimateProfit(spec, null, 2));

    var long_fallback = SymbolPositionRecord.init(4006, 241, 15);
    long_fallback.direction = .long;
    long_fallback.open_volume = max_i64;
    long_fallback.open_price_sum = 1;
    long_fallback.profit = min_i64;
    try std.testing.expectEqual(max_i64 - 1, try long_fallback.estimateProfit(spec, null, null));
    try std.testing.expectEqual(max_i64 - 1, try long_fallback.estimateProfit(spec, 0, null));

    var short_fallback = SymbolPositionRecord.init(4007, 241, 15);
    short_fallback.direction = .short;
    short_fallback.open_volume = max_i64;
    short_fallback.open_price_sum = 1;
    short_fallback.profit = min_i64;
    try std.testing.expectEqual(max_i64 - 1, try short_fallback.estimateProfit(spec, null, null));
    try std.testing.expectEqual(
        max_i64 - 1,
        try short_fallback.estimateProfit(spec, null, max_i64),
    );
}

test "margin calculation returns minus one for exposure-reducing order" {
    const spec = try marginTestSpecification();
    var record = SymbolPositionRecord.init(4004, 241, 15);
    record.direction = .long;
    record.open_volume = 10;
    record.open_price_sum = 1_000;

    try std.testing.expectEqual(
        @as(i64, -1),
        try record.calculateRequiredMarginForOrder(spec, .ask, 4),
    );
    try std.testing.expectEqual(
        @as(i64, 110),
        try record.calculateRequiredMarginForOrder(spec, .bid, 1),
    );
}

test "margin math reports arithmetic overflow without partial pending mutation" {
    const spec = try marginTestSpecification();

    var pending = SymbolPositionRecord.init(4005, 241, 15);
    pending.pending_buy_size = std.math.maxInt(i64);
    try std.testing.expectError(error.ArithmeticOverflow, pending.pendingHold(.bid, 1));
    try std.testing.expectEqual(std.math.maxInt(i64), pending.pending_buy_size);

    var required = SymbolPositionRecord.init(4006, 241, 15);
    required.direction = .long;
    required.open_volume = std.math.maxInt(i64);
    required.open_price_sum = 1;
    try std.testing.expectError(
        error.ArithmeticOverflow,
        required.calculateRequiredMarginForFutures(spec),
    );
    try std.testing.expectError(
        error.ArithmeticOverflow,
        required.calculateRequiredMarginForOrder(spec, .bid, 1),
    );
    try std.testing.expectError(
        error.ArithmeticOverflow,
        required.estimateProfit(spec, 2, null),
    );

    var release = SymbolPositionRecord.init(4007, 241, 15);
    release.pending_buy_size = std.math.minInt(i64);
    try std.testing.expectError(error.ArithmeticOverflow, release.pendingRelease(.bid, 1));
    try std.testing.expectEqual(std.math.minInt(i64), release.pending_buy_size);

    var trade = SymbolPositionRecord.init(4008, 241, 15);
    trade.direction = .long;
    trade.open_volume = std.math.maxInt(i64);
    trade.open_price_sum = 1;
    trade.pending_buy_size = 1;
    const before = trade;
    try std.testing.expectError(
        error.ArithmeticOverflow,
        trade.updatePositionForMarginTrade(.bid, 1, 2),
    );
    try std.testing.expectEqualDeep(before, trade);

    // Exercise an overflow after the close path has already computed a smaller
    // open volume. The public operation must still leave the complete record intact.
    var partial_close = SymbolPositionRecord.init(4009, 241, 15);
    partial_close.direction = .long;
    partial_close.open_volume = 3;
    partial_close.open_price_sum = 300;
    partial_close.pending_sell_size = 2;
    const partial_close_before = partial_close;
    try std.testing.expectError(
        error.ArithmeticOverflow,
        partial_close.updatePositionForMarginTrade(.ask, 2, std.math.maxInt(i64)),
    );
    try std.testing.expectEqualDeep(partial_close_before, partial_close);
}

test "preReserveUsers grows capacity without inserting" {
    const allocator = std.testing.allocator;
    var svc = UserProfileService.init(allocator);
    defer svc.deinit();

    try svc.preReserveUsers(16);
    try std.testing.expectEqual(@as(usize, 0), svc.user_profiles.count());
    // Inserting should not need further map growth for a single put.
    try std.testing.expect(try svc.addEmptyUserProfile(42));
    try std.testing.expectEqual(@as(usize, 1), svc.user_profiles.count());
}
