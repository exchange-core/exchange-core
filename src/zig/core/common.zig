const std = @import("std");

pub const SymbolType = enum(i8) {
    currency_exchange_pair = 0,
    futures_contract = 1,
    option = 2,

    pub const FromCodeError = error{
        UnknownSymbolTypeCode,
    };

    pub fn fromCode(raw_code: i32) FromCodeError!SymbolType {
        return switch (raw_code) {
            0 => .currency_exchange_pair,
            1 => .futures_contract,
            2 => .option,
            else => error.UnknownSymbolTypeCode,
        };
    }

    pub fn code(self: SymbolType) i8 {
        return @intFromEnum(self);
    }
};

pub const OrderAction = enum(i8) {
    ask = 0,
    bid = 1,

    pub const FromCodeError = error{
        UnknownOrderActionCode,
    };

    pub fn fromCode(raw_code: i8) FromCodeError!OrderAction {
        return switch (raw_code) {
            0 => .ask,
            1 => .bid,
            else => error.UnknownOrderActionCode,
        };
    }

    pub fn code(self: OrderAction) i8 {
        return @intFromEnum(self);
    }

    pub fn opposite(self: OrderAction) OrderAction {
        return switch (self) {
            .ask => .bid,
            .bid => .ask,
        };
    }
};

pub const OrderType = enum(i8) {
    gtc = 0,
    ioc = 1,
    ioc_budget = 2,
    fok = 3,
    fok_budget = 4,

    pub const FromCodeError = error{
        UnknownOrderTypeCode,
    };

    pub fn fromCode(raw_code: i8) FromCodeError!OrderType {
        return switch (raw_code) {
            0 => .gtc,
            1 => .ioc,
            2 => .ioc_budget,
            3 => .fok,
            4 => .fok_budget,
            else => error.UnknownOrderTypeCode,
        };
    }

    pub fn code(self: OrderType) i8 {
        return @intFromEnum(self);
    }
};

pub const BalanceAdjustmentType = enum(i8) {
    adjustment = 0,
    @"suspend" = 1,

    pub const FromCodeError = error{
        UnknownBalanceAdjustmentTypeCode,
    };

    pub fn fromCode(raw_code: i8) FromCodeError!BalanceAdjustmentType {
        return switch (raw_code) {
            0 => .adjustment,
            1 => .@"suspend",
            else => error.UnknownBalanceAdjustmentTypeCode,
        };
    }

    pub fn code(self: BalanceAdjustmentType) i8 {
        return @intFromEnum(self);
    }
};

pub const UserStatus = enum(i8) {
    active = 0,
    suspended = 1,

    pub const FromCodeError = error{
        UnknownUserStatusCode,
    };

    pub fn fromCode(raw_code: i8) FromCodeError!UserStatus {
        return switch (raw_code) {
            0 => .active,
            1 => .suspended,
            else => error.UnknownUserStatusCode,
        };
    }

    pub fn code(self: UserStatus) i8 {
        return @intFromEnum(self);
    }
};

pub const PositionDirection = enum(i8) {
    long = 1,
    short = -1,
    empty = 0,

    pub const FromCodeError = error{
        UnknownPositionDirectionCode,
    };

    pub fn fromAction(action: OrderAction) PositionDirection {
        return switch (action) {
            .bid => .long,
            .ask => .short,
        };
    }

    pub fn fromCode(raw_code: i8) FromCodeError!PositionDirection {
        return switch (raw_code) {
            1 => .long,
            -1 => .short,
            0 => .empty,
            else => error.UnknownPositionDirectionCode,
        };
    }

    pub fn multiplier(self: PositionDirection) i8 {
        return @intFromEnum(self);
    }

    pub fn isOppositeToAction(self: PositionDirection, action: OrderAction) bool {
        return (self == .long and action == .ask) or (self == .short and action == .bid);
    }

    pub fn isSameAsAction(self: PositionDirection, action: OrderAction) bool {
        return (self == .long and action == .bid) or (self == .short and action == .ask);
    }
};

pub const CoreSymbolSpecification = struct {
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

    pub const InitArgs = struct {
        symbol_id: i32,
        symbol_type: SymbolType,
        base_currency: i32,
        quote_currency: i32,
        base_scale_k: i64,
        quote_scale_k: i64,
        taker_fee: i64,
        maker_fee: i64,
        margin_buy: i64 = 0,
        margin_sell: i64 = 0,
    };

    pub const ValidationError = error{
        NonPositiveBaseScale,
        NonPositiveQuoteScale,
        NegativeTakerFee,
        NegativeMakerFee,
        NegativeMarginBuy,
        NegativeMarginSell,
    };

    pub fn init(args: InitArgs) ValidationError!CoreSymbolSpecification {
        const spec: CoreSymbolSpecification = .{
            .symbol_id = args.symbol_id,
            .symbol_type = args.symbol_type,
            .base_currency = args.base_currency,
            .quote_currency = args.quote_currency,
            .base_scale_k = args.base_scale_k,
            .quote_scale_k = args.quote_scale_k,
            .taker_fee = args.taker_fee,
            .maker_fee = args.maker_fee,
            .margin_buy = args.margin_buy,
            .margin_sell = args.margin_sell,
        };
        try spec.validate();
        return spec;
    }

    pub fn validate(self: CoreSymbolSpecification) ValidationError!void {
        if (self.base_scale_k <= 0) return error.NonPositiveBaseScale;
        if (self.quote_scale_k <= 0) return error.NonPositiveQuoteScale;
        if (self.taker_fee < 0) return error.NegativeTakerFee;
        if (self.maker_fee < 0) return error.NegativeMakerFee;
        if (self.margin_buy < 0) return error.NegativeMarginBuy;
        if (self.margin_sell < 0) return error.NegativeMarginSell;
    }

    pub fn comptimeValidate(comptime spec: CoreSymbolSpecification) void {
        if (spec.base_scale_k <= 0) @compileError("base_scale_k must be positive");
        if (spec.quote_scale_k <= 0) @compileError("quote_scale_k must be positive");
        if (spec.taker_fee < 0) @compileError("taker_fee must be non-negative");
        if (spec.maker_fee < 0) @compileError("maker_fee must be non-negative");
        if (spec.margin_buy < 0) @compileError("margin_buy must be non-negative");
        if (spec.margin_sell < 0) @compileError("margin_sell must be non-negative");
    }
};

test "symbol type decode and unknown handling" {
    try std.testing.expectEqual(SymbolType.currency_exchange_pair, try SymbolType.fromCode(0));
    try std.testing.expectEqual(SymbolType.option, try SymbolType.fromCode(2));
    try std.testing.expectError(error.UnknownSymbolTypeCode, SymbolType.fromCode(3));
}

test "order action opposite" {
    try std.testing.expectEqual(OrderAction.bid, OrderAction.ask.opposite());
    try std.testing.expectEqual(OrderAction.ask, OrderAction.bid.opposite());
    try std.testing.expectError(error.UnknownOrderActionCode, OrderAction.fromCode(12));
}

test "order type decode and unknown handling" {
    try std.testing.expectEqual(OrderType.gtc, try OrderType.fromCode(0));
    try std.testing.expectEqual(OrderType.fok_budget, try OrderType.fromCode(4));
    try std.testing.expectError(error.UnknownOrderTypeCode, OrderType.fromCode(5));
}

test "balance adjustment decode and unknown handling" {
    try std.testing.expectEqual(BalanceAdjustmentType.adjustment, try BalanceAdjustmentType.fromCode(0));
    try std.testing.expectEqual(BalanceAdjustmentType.@"suspend", try BalanceAdjustmentType.fromCode(1));
    try std.testing.expectError(error.UnknownBalanceAdjustmentTypeCode, BalanceAdjustmentType.fromCode(2));
}

test "user status decode and unknown handling" {
    try std.testing.expectEqual(UserStatus.active, try UserStatus.fromCode(0));
    try std.testing.expectEqual(UserStatus.suspended, try UserStatus.fromCode(1));
    try std.testing.expectError(error.UnknownUserStatusCode, UserStatus.fromCode(2));
}

test "position direction conversions" {
    try std.testing.expectEqual(PositionDirection.long, PositionDirection.fromAction(.bid));
    try std.testing.expectEqual(PositionDirection.short, PositionDirection.fromAction(.ask));
    try std.testing.expectEqual(PositionDirection.empty, try PositionDirection.fromCode(0));
    try std.testing.expectError(error.UnknownPositionDirectionCode, PositionDirection.fromCode(2));
    try std.testing.expect(PositionDirection.long.isOppositeToAction(.ask));
    try std.testing.expect(PositionDirection.short.isSameAsAction(.ask));
}

test "core symbol specification validates construction invariants" {
    const valid = try CoreSymbolSpecification.init(.{
        .symbol_id = 241,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 1_000_000,
        .quote_scale_k = 10_000,
        .taker_fee = 1900,
        .maker_fee = 700,
    });
    try std.testing.expectEqual(@as(i32, 241), valid.symbol_id);

    try std.testing.expectError(error.NonPositiveBaseScale, CoreSymbolSpecification.init(.{
        .symbol_id = 1,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 1,
        .quote_currency = 2,
        .base_scale_k = 0,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
    }));
}
