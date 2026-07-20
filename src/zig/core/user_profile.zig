const std = @import("std");
const common = @import("common.zig");
const cmd = @import("cmd.zig");

pub const StateHashError = std.mem.Allocator.Error || error{
    StateHashCorrupted,
};

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

    pub fn pendingHold(self: *SymbolPositionRecord, action: common.OrderAction, size: i64) void {
        std.debug.assert(size >= 0);
        if (action == .ask) {
            self.pending_sell_size += size;
        } else {
            self.pending_buy_size += size;
        }
    }

    pub fn pendingRelease(self: *SymbolPositionRecord, action: common.OrderAction, size: i64) void {
        std.debug.assert(size >= 0);
        if (action == .ask) {
            self.pending_sell_size -= size;
        } else {
            self.pending_buy_size -= size;
        }
    }

    pub fn calculateRequiredMarginForFutures(self: SymbolPositionRecord, spec: common.CoreSymbolSpecification) i64 {
        const signed_position = self.open_volume * @as(i64, self.direction.multiplier());
        const current_risk_buy_size = self.pending_buy_size + signed_position;
        const current_risk_sell_size = self.pending_sell_size - signed_position;

        const margin_buy = spec.margin_buy * current_risk_buy_size;
        const margin_sell = spec.margin_sell * current_risk_sell_size;
        return @max(margin_buy, margin_sell);
    }

    pub fn calculateRequiredMarginForOrder(
        self: SymbolPositionRecord,
        spec: common.CoreSymbolSpecification,
        action: common.OrderAction,
        size: i64,
    ) i64 {
        const signed_position = self.open_volume * @as(i64, self.direction.multiplier());
        const current_risk_buy_size = self.pending_buy_size + signed_position;
        const current_risk_sell_size = self.pending_sell_size - signed_position;

        var margin_buy = spec.margin_buy * current_risk_buy_size;
        var margin_sell = spec.margin_sell * current_risk_sell_size;
        const current_margin = @max(margin_buy, margin_sell);

        if (action == .bid) {
            margin_buy += spec.margin_buy * size;
        } else {
            margin_sell += spec.margin_sell * size;
        }
        const new_margin = @max(margin_buy, margin_sell);

        return if (new_margin <= current_margin) -1 else new_margin;
    }

    pub fn updatePositionForMarginTrade(
        self: *SymbolPositionRecord,
        action: common.OrderAction,
        size: i64,
        price: i64,
    ) i64 {
        self.pendingRelease(action, size);
        const size_to_open = self.closeCurrentPositionFutures(action, size, price);
        if (size_to_open > 0) {
            self.openPositionMargin(action, size_to_open, price);
        }
        return size_to_open;
    }

    fn closeCurrentPositionFutures(
        self: *SymbolPositionRecord,
        action: common.OrderAction,
        trade_size: i64,
        trade_price: i64,
    ) i64 {
        if (self.direction == .empty or self.direction == common.PositionDirection.fromAction(action)) {
            return trade_size;
        }

        if (self.open_volume > trade_size) {
            self.open_volume -= trade_size;
            self.open_price_sum -= trade_size * trade_price;
            return 0;
        }

        self.profit += (self.open_volume * trade_price - self.open_price_sum) * @as(i64, self.direction.multiplier());
        self.open_price_sum = 0;
        self.direction = .empty;
        const size_to_open = trade_size - self.open_volume;
        self.open_volume = 0;
        return size_to_open;
    }

    fn openPositionMargin(
        self: *SymbolPositionRecord,
        action: common.OrderAction,
        size_to_open: i64,
        trade_price: i64,
    ) void {
        self.open_volume += size_to_open;
        self.open_price_sum += trade_price * size_to_open;
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
    adjustments_counter: i64 = 0,
    accounts: std.AutoHashMapUnmanaged(i32, i64) = .empty,
    user_status: common.UserStatus,

    pub fn init(uid: i64, user_status: common.UserStatus) UserProfile {
        return .{
            .uid = uid,
            .user_status = user_status,
        };
    }

    pub fn deinit(self: *UserProfile, allocator: std.mem.Allocator) void {
        self.positions.deinit(allocator);
        self.accounts.deinit(allocator);
        self.* = undefined;
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

    pub fn init(allocator: std.mem.Allocator) UserProfileService {
        return .{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *UserProfileService) void {
        var it = self.user_profiles.valueIterator();
        while (it.next()) |profile| {
            profile.deinit(self.allocator);
        }
        self.user_profiles.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn getUserProfile(self: *UserProfileService, uid: i64) ?*UserProfile {
        return self.user_profiles.getPtr(uid);
    }

    pub fn getUserProfileOrAddSuspended(
        self: *UserProfileService,
        uid: i64,
    ) std.mem.Allocator.Error!*UserProfile {
        if (self.user_profiles.getPtr(uid)) |existing| {
            return existing;
        }
        try self.user_profiles.put(self.allocator, uid, UserProfile.init(uid, .suspended));
        return self.user_profiles.getPtr(uid) orelse
            invariantViolation("user profile map missing inserted suspended profile");
    }

    pub fn addEmptyUserProfile(self: *UserProfileService, uid: i64) std.mem.Allocator.Error!bool {
        if (self.user_profiles.contains(uid)) {
            return false;
        }
        try self.user_profiles.put(self.allocator, uid, UserProfile.init(uid, .active));
        return true;
    }

    pub fn balanceAdjustment(
        self: *UserProfileService,
        uid: i64,
        currency: i32,
        amount: i64,
        funding_transaction_id: i64,
    ) std.mem.Allocator.Error!cmd.CommandResultCode {
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

        profile.adjustments_counter = funding_transaction_id;
        if (profile.accounts.getPtr(currency)) |balance| {
            balance.* += amount;
        } else {
            try profile.accounts.put(self.allocator, currency, amount);
        }

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
        var removed_profile = removed.value;
        removed_profile.deinit(self.allocator);
        return .success;
    }

    pub fn resumeUserProfile(
        self: *UserProfileService,
        uid: i64,
    ) std.mem.Allocator.Error!cmd.CommandResultCode {
        const profile = self.user_profiles.getPtr(uid);
        if (profile == null) {
            try self.user_profiles.put(self.allocator, uid, UserProfile.init(uid, .active));
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
    pos.pendingHold(.bid, 2);

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
    const spec = try common.CoreSymbolSpecification.init(.{
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

    record.pendingHold(.bid, 5);
    try std.testing.expectEqual(@as(i64, 50), record.calculateRequiredMarginForFutures(spec));
    try std.testing.expectEqual(@as(i64, 80), record.calculateRequiredMarginForOrder(spec, .ask, 4));

    _ = record.updatePositionForMarginTrade(.bid, 5, 100);
    try std.testing.expectEqual(common.PositionDirection.long, record.direction);
    try std.testing.expectEqual(@as(i64, 5), record.open_volume);
    try std.testing.expectEqual(@as(i64, 500), record.open_price_sum);
    try record.validateInternalState();
}
