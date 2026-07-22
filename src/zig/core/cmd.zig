const std = @import("std");
const common = @import("common.zig");

pub const OrderCommandType = enum(i8) {
    place_order = 1,
    cancel_order = 2,
    move_order = 3,
    reduce_order = 4,
    order_book_request = 6,
    add_user = 10,
    balance_adjustment = 11,
    suspend_user = 12,
    resume_user = 13,
    binary_data_query = 90,
    binary_data_command = 91,
    persist_state_matching = 110,
    persist_state_risk = 111,
    grouping_control = 118,
    nop = 120,
    reset = 124,
    shutdown_signal = 127,
    reserved_compressed = -1,

    pub const FromCodeError = error{
        UnknownOrderCommandTypeCode,
    };

    pub fn fromCode(raw_code: i8) FromCodeError!OrderCommandType {
        return switch (raw_code) {
            1 => .place_order,
            2 => .cancel_order,
            3 => .move_order,
            4 => .reduce_order,
            6 => .order_book_request,
            10 => .add_user,
            11 => .balance_adjustment,
            12 => .suspend_user,
            13 => .resume_user,
            90 => .binary_data_query,
            91 => .binary_data_command,
            110 => .persist_state_matching,
            111 => .persist_state_risk,
            118 => .grouping_control,
            120 => .nop,
            124 => .reset,
            127 => .shutdown_signal,
            -1 => .reserved_compressed,
            else => error.UnknownOrderCommandTypeCode,
        };
    }

    pub fn code(self: OrderCommandType) i8 {
        return @intFromEnum(self);
    }

    pub fn isMutating(self: OrderCommandType) bool {
        return switch (self) {
            .place_order,
            .cancel_order,
            .move_order,
            .reduce_order,
            .add_user,
            .balance_adjustment,
            .suspend_user,
            .resume_user,
            .binary_data_command,
            .persist_state_matching,
            .persist_state_risk,
            .reset,
            => true,
            .order_book_request,
            .binary_data_query,
            .grouping_control,
            .nop,
            .shutdown_signal,
            .reserved_compressed,
            => false,
        };
    }
};

pub const CommandResultCode = enum(i32) {
    new = 0,
    valid_for_matching_engine = 1,
    success = 100,
    accepted = 110,
    auth_invalid_user = -1001,
    auth_token_expired = -1002,
    invalid_symbol = -1201,
    invalid_price_step = -1202,
    unsupported_symbol_type = -1203,
    risk_nsf = -2001,
    risk_invalid_reserve_bid_price = -2002,
    risk_ask_price_lower_than_fee = -2003,
    risk_margin_trading_disabled = -2004,
    matching_unknown_order_id = -3002,
    matching_unsupported_command = -3004,
    matching_invalid_order_book_id = -3005,
    matching_move_failed_price_over_risk_limit = -3041,
    matching_reduce_failed_wrong_size = -3051,
    user_mgmt_user_already_exists = -4001,
    user_mgmt_account_balance_adjustment_already_applied_same = -4101,
    user_mgmt_account_balance_adjustment_already_applied_many = -4102,
    user_mgmt_account_balance_adjustment_nsf = -4103,
    user_mgmt_non_zero_account_balance = -4104,
    user_mgmt_user_not_suspendable_has_positions = -4130,
    user_mgmt_user_not_suspendable_non_empty_accounts = -4131,
    user_mgmt_user_not_suspended = -4132,
    user_mgmt_user_already_suspended = -4133,
    user_mgmt_user_not_found = -4201,
    symbol_mgmt_symbol_already_exists = -5001,
    binary_command_failed = -8001,
    report_query_unknown_type = -8003,
    state_persist_risk_engine_failed = -8010,
    state_persist_matching_engine_failed = -8020,
    drop = -9999,

    pub const FromCodeError = error{
        UnknownCommandResultCode,
    };

    pub fn fromCode(raw_code: i32) FromCodeError!CommandResultCode {
        return std.enums.fromInt(CommandResultCode, raw_code) orelse error.UnknownCommandResultCode;
    }

    pub fn code(self: CommandResultCode) i32 {
        return @intFromEnum(self);
    }

    pub fn mergeToFirstFailed(results: []const CommandResultCode) CommandResultCode {
        var has_success = false;
        for (results) |result| {
            switch (result) {
                .success => has_success = true,
                .accepted => {},
                else => return result,
            }
        }
        return if (has_success) .success else .accepted;
    }
};

pub const OrderCommand = struct {
    command: OrderCommandType,
    order_id: i64 = 0,
    symbol: i32 = 0,
    price: i64 = 0,
    size: i64 = 0,
    reserve_bid_price: i64 = 0,
    action: ?common.OrderAction = null,
    order_type: ?common.OrderType = null,
    uid: i64 = 0,
    timestamp: i64 = 0,
    user_cookie: i32 = 0,
    events_group: i64 = 0,
    service_flags: i32 = 0,
    result_code: CommandResultCode = .new,

    pub fn newOrder(
        order_type: common.OrderType,
        order_id: i64,
        uid: i64,
        price: i64,
        reserve_bid_price: i64,
        size: i64,
        action: common.OrderAction,
    ) OrderCommand {
        return .{
            .command = .place_order,
            .order_id = order_id,
            .uid = uid,
            .price = price,
            .reserve_bid_price = reserve_bid_price,
            .size = size,
            .action = action,
            .order_type = order_type,
            .result_code = .valid_for_matching_engine,
        };
    }

    pub fn cancel(order_id: i64, uid: i64) OrderCommand {
        return .{
            .command = .cancel_order,
            .order_id = order_id,
            .uid = uid,
            .result_code = .valid_for_matching_engine,
        };
    }

    pub fn reduce(order_id: i64, uid: i64, reduce_size: i64) OrderCommand {
        return .{
            .command = .reduce_order,
            .order_id = order_id,
            .uid = uid,
            .size = reduce_size,
            .result_code = .valid_for_matching_engine,
        };
    }

    pub fn update(order_id: i64, uid: i64, price: i64) OrderCommand {
        return .{
            .command = .move_order,
            .order_id = order_id,
            .uid = uid,
            .price = price,
            .result_code = .valid_for_matching_engine,
        };
    }

    pub fn writeTo(self: OrderCommand, dest: *OrderCommand) void {
        dest.command = self.command;
        dest.order_id = self.order_id;
        dest.symbol = self.symbol;
        dest.uid = self.uid;
        dest.timestamp = self.timestamp;
        dest.reserve_bid_price = self.reserve_bid_price;
        dest.price = self.price;
        dest.size = self.size;
        dest.action = self.action;
        dest.order_type = self.order_type;
    }
};

test "order command type mappings and mutability" {
    try std.testing.expectEqual(OrderCommandType.place_order, try OrderCommandType.fromCode(1));
    try std.testing.expectEqual(OrderCommandType.reserved_compressed, try OrderCommandType.fromCode(-1));
    try std.testing.expect(OrderCommandType.place_order.isMutating());
    try std.testing.expect(!OrderCommandType.nop.isMutating());
    try std.testing.expectError(error.UnknownOrderCommandTypeCode, OrderCommandType.fromCode(5));
}

test "command result merge matches java semantics" {
    try std.testing.expectEqual(CommandResultCode.accepted, CommandResultCode.mergeToFirstFailed(&.{}));
    try std.testing.expectEqual(CommandResultCode.success, CommandResultCode.mergeToFirstFailed(&.{ .accepted, .success }));
    try std.testing.expectEqual(
        CommandResultCode.matching_unknown_order_id,
        CommandResultCode.mergeToFirstFailed(&.{ .success, .matching_unknown_order_id, .accepted }),
    );
}

test "order command constructors set matching defaults" {
    const placed = OrderCommand.newOrder(.gtc, 5001, 301, 15_400, 15_600, 12, .bid);
    try std.testing.expectEqual(OrderCommandType.place_order, placed.command);
    try std.testing.expectEqual(CommandResultCode.valid_for_matching_engine, placed.result_code);
    try std.testing.expectEqual(@as(?common.OrderAction, .bid), placed.action);

    const cancelled = OrderCommand.cancel(5001, 301);
    try std.testing.expectEqual(OrderCommandType.cancel_order, cancelled.command);
    try std.testing.expectEqual(@as(i64, 5001), cancelled.order_id);
    try std.testing.expectEqual(CommandResultCode.valid_for_matching_engine, cancelled.result_code);
}

test "writeTo copies command data but not result code" {
    const source = OrderCommand.newOrder(.ioc, 42, 100, 200, 210, 9, .ask);
    var destination: OrderCommand = .{
        .command = .nop,
        .result_code = .matching_unsupported_command,
    };

    source.writeTo(&destination);

    try std.testing.expectEqual(OrderCommandType.place_order, destination.command);
    try std.testing.expectEqual(@as(i64, 42), destination.order_id);
    try std.testing.expectEqual(CommandResultCode.matching_unsupported_command, destination.result_code);
}
