const std = @import("std");
const common = @import("common.zig");
const cmd = @import("cmd.zig");
const binary_payload = @import("binary_payload.zig");
const kernel_mod = @import("exchange_kernel.zig");

pub const BinaryCurrencyBalance = binary_payload.CurrencyBalance;
pub const BinaryUserAccountsBatch = binary_payload.UserAccountsBatch;
pub const BinaryReportQuery = binary_payload.DecodedReportQuery;
pub const PersistStateRequestPair = kernel_mod.ExchangeApi.PersistStateRequestPair;

pub const TimestampedUserCommand = struct {
    uid: i64,
    timestamp: i64,
};

pub const TimestampedUnitCommand = struct {
    timestamp: i64,
};

pub const ApiCommand = union(enum) {
    raw_command: cmd.OrderCommand,
    add_user: i64,
    add_user_with_timestamp: TimestampedUserCommand,
    adjust_user_balance: struct {
        uid: i64,
        transaction_id: i64,
        currency: i32,
        amount: i64,
        adjustment_type: common.BalanceAdjustmentType,
        timestamp: i64 = 0,
    },
    suspend_user: i64,
    suspend_user_with_timestamp: TimestampedUserCommand,
    resume_user: i64,
    resume_user_with_timestamp: TimestampedUserCommand,
    place_order: struct {
        order_id: i64,
        uid: i64,
        symbol: i32,
        price: i64,
        reserve_bid_price: i64,
        size: i64,
        action: common.OrderAction,
        order_type: common.OrderType,
        user_cookie: i32,
        timestamp: i64 = 0,
    },
    move_order: struct {
        order_id: i64,
        uid: i64,
        symbol: i32,
        new_price: i64,
        timestamp: i64 = 0,
    },
    cancel_order: struct {
        order_id: i64,
        uid: i64,
        symbol: i32,
        timestamp: i64 = 0,
    },
    reduce_order: struct {
        order_id: i64,
        uid: i64,
        symbol: i32,
        reduce_size: i64,
        timestamp: i64 = 0,
    },
    request_order_book: struct {
        symbol: i32,
        depth: usize,
        timestamp: i64 = 0,
    },
    persist_state: struct {
        snapshot_id: i64,
        timestamp_ns: i64,
    },
    grouping_control: struct {
        timestamp_ns: i64,
        mode: i64,
    },
    reset,
    reset_with_timestamp: TimestampedUnitCommand,
    nop,
    nop_with_timestamp: TimestampedUnitCommand,
    binary_add_symbols: struct {
        transfer_id: i32,
        timestamp: i64,
        symbols: []const common.CoreSymbolSpecification,
    },
    binary_add_accounts: struct {
        transfer_id: i32,
        timestamp: i64,
        users: []const BinaryUserAccountsBatch,
    },
    binary_report_query: struct {
        transfer_id: i32,
        timestamp: i64,
        query: BinaryReportQuery,
    },
};

pub const ApiCommandResult = union(enum) {
    response: kernel_mod.CommandResponse,
    persist_state: cmd.CommandResultCode,

    pub fn deinit(self: *ApiCommandResult, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .response => |*response| response.deinit(allocator),
            .persist_state => {},
        }
        self.* = undefined;
    }
};

pub const ApiCommandAsyncHandle = union(enum) {
    request_id: u64,
    persist_state: PersistStateRequestPair,
};

pub const TranslationError = error{
    OrderBookDepthOverflow,
};

/// Translates the one-command Java-style API surface into an owned
/// `OrderCommand`. Multi-command and metadata-preserving escape hatches stay
/// with their dedicated publication paths and therefore return `null`.
pub fn toOrdinaryOrderCommand(api_command: ApiCommand) TranslationError!?cmd.OrderCommand {
    return switch (api_command) {
        .add_user => |uid| .{
            .command = .add_user,
            .uid = uid,
            .result_code = .new,
        },
        .add_user_with_timestamp => |payload| .{
            .command = .add_user,
            .uid = payload.uid,
            .timestamp = payload.timestamp,
            .result_code = .new,
        },
        .adjust_user_balance => |payload| .{
            .command = .balance_adjustment,
            .order_id = payload.transaction_id,
            .symbol = payload.currency,
            .price = payload.amount,
            .order_type = switch (payload.adjustment_type) {
                .adjustment => .gtc,
                .@"suspend" => .ioc,
            },
            .uid = payload.uid,
            .timestamp = payload.timestamp,
            .result_code = .new,
        },
        .suspend_user => |uid| .{
            .command = .suspend_user,
            .uid = uid,
            .result_code = .new,
        },
        .suspend_user_with_timestamp => |payload| .{
            .command = .suspend_user,
            .uid = payload.uid,
            .timestamp = payload.timestamp,
            .result_code = .new,
        },
        .resume_user => |uid| .{
            .command = .resume_user,
            .uid = uid,
            .result_code = .new,
        },
        .resume_user_with_timestamp => |payload| .{
            .command = .resume_user,
            .uid = payload.uid,
            .timestamp = payload.timestamp,
            .result_code = .new,
        },
        .place_order => |payload| .{
            .command = .place_order,
            .order_id = payload.order_id,
            .symbol = payload.symbol,
            .price = payload.price,
            .size = payload.size,
            .reserve_bid_price = payload.reserve_bid_price,
            .action = payload.action,
            .order_type = payload.order_type,
            .uid = payload.uid,
            .timestamp = payload.timestamp,
            .user_cookie = payload.user_cookie,
            .result_code = .new,
        },
        .move_order => |payload| .{
            .command = .move_order,
            .order_id = payload.order_id,
            .symbol = payload.symbol,
            .price = payload.new_price,
            .uid = payload.uid,
            .timestamp = payload.timestamp,
            .result_code = .new,
        },
        .cancel_order => |payload| .{
            .command = .cancel_order,
            .order_id = payload.order_id,
            .symbol = payload.symbol,
            .uid = payload.uid,
            .timestamp = payload.timestamp,
            .result_code = .new,
        },
        .reduce_order => |payload| .{
            .command = .reduce_order,
            .order_id = payload.order_id,
            .symbol = payload.symbol,
            .size = payload.reduce_size,
            .uid = payload.uid,
            .timestamp = payload.timestamp,
            .result_code = .new,
        },
        .request_order_book => |payload| .{
            .command = .order_book_request,
            .symbol = payload.symbol,
            .size = std.math.cast(i64, payload.depth) orelse
                return error.OrderBookDepthOverflow,
            .timestamp = payload.timestamp,
            .result_code = .new,
        },
        .reset => .{
            .command = .reset,
            .result_code = .new,
        },
        .reset_with_timestamp => |payload| .{
            .command = .reset,
            .timestamp = payload.timestamp,
            .result_code = .new,
        },
        .nop => .{
            .command = .nop,
            .result_code = .new,
        },
        .nop_with_timestamp => |payload| .{
            .command = .nop,
            .timestamp = payload.timestamp,
            .result_code = .new,
        },
        .raw_command,
        .persist_state,
        .grouping_control,
        .binary_add_symbols,
        .binary_add_accounts,
        .binary_report_query,
        => null,
    };
}

/// Java `ExchangeApi.submitCommandAsyncFullResponse` accepts only commands
/// whose translator publishes one ordinary `OrderCommand`. Raw commands,
/// grouping control, persistence pairs, and binary carrier batches must use
/// their result-code or typed report APIs instead.
pub fn supportsAsyncFullResponse(command: ApiCommand) bool {
    return switch (command) {
        .move_order,
        .place_order,
        .cancel_order,
        .reduce_order,
        .request_order_book,
        .add_user,
        .add_user_with_timestamp,
        .adjust_user_balance,
        .resume_user,
        .resume_user_with_timestamp,
        .suspend_user,
        .suspend_user_with_timestamp,
        .reset,
        .reset_with_timestamp,
        .nop,
        .nop_with_timestamp,
        => true,
        .raw_command,
        .grouping_control,
        .persist_state,
        .binary_add_symbols,
        .binary_add_accounts,
        .binary_report_query,
        => false,
    };
}

test "api command result deinit leaves persist-state variant untouched" {
    var result: ApiCommandResult = .{ .persist_state = .success };
    result.deinit(std.testing.allocator);
}

test "api command full-response support matches the Java allowlist" {
    const supported = [_]ApiCommand{
        .{ .move_order = .{ .order_id = 1, .uid = 2, .symbol = 3, .new_price = 4 } },
        .{ .place_order = .{
            .order_id = 1,
            .uid = 2,
            .symbol = 3,
            .price = 4,
            .reserve_bid_price = 5,
            .size = 6,
            .action = .bid,
            .order_type = .gtc,
            .user_cookie = 7,
        } },
        .{ .cancel_order = .{ .order_id = 1, .uid = 2, .symbol = 3 } },
        .{ .reduce_order = .{ .order_id = 1, .uid = 2, .symbol = 3, .reduce_size = 4 } },
        .{ .request_order_book = .{ .symbol = 3, .depth = 4 } },
        .{ .add_user = 2 },
        .{ .add_user_with_timestamp = .{ .uid = 2, .timestamp = 8 } },
        .{ .adjust_user_balance = .{
            .uid = 2,
            .transaction_id = 1,
            .currency = 3,
            .amount = 4,
            .adjustment_type = .adjustment,
        } },
        .{ .resume_user = 2 },
        .{ .resume_user_with_timestamp = .{ .uid = 2, .timestamp = 9 } },
        .{ .suspend_user = 2 },
        .{ .suspend_user_with_timestamp = .{ .uid = 2, .timestamp = 10 } },
        .reset,
        .{ .reset_with_timestamp = .{ .timestamp = 11 } },
        .nop,
        .{ .nop_with_timestamp = .{ .timestamp = 12 } },
    };
    for (supported) |command| try std.testing.expect(supportsAsyncFullResponse(command));

    const unsupported = [_]ApiCommand{
        .{ .raw_command = .{ .command = .nop } },
        .{ .grouping_control = .{ .timestamp_ns = 1, .mode = 0 } },
        .{ .persist_state = .{ .snapshot_id = 1, .timestamp_ns = 2 } },
        .{ .binary_add_symbols = .{ .transfer_id = 1, .timestamp = 2, .symbols = &.{} } },
        .{ .binary_add_accounts = .{ .transfer_id = 1, .timestamp = 2, .users = &.{} } },
        .{ .binary_report_query = .{ .transfer_id = 1, .timestamp = 2, .query = .state_hash } },
    };
    for (unsupported) |command| try std.testing.expect(!supportsAsyncFullResponse(command));
}

test "ordinary api command translation keeps old literals source compatible and zero timestamped" {
    const commands = [_]ApiCommand{
        .{ .adjust_user_balance = .{
            .uid = 2,
            .transaction_id = 1,
            .currency = 3,
            .amount = 4,
            .adjustment_type = .adjustment,
        } },
        .{ .place_order = .{
            .order_id = 5,
            .uid = 2,
            .symbol = 3,
            .price = 6,
            .reserve_bid_price = 7,
            .size = 8,
            .action = .bid,
            .order_type = .gtc,
            .user_cookie = 9,
        } },
        .{ .move_order = .{ .order_id = 5, .uid = 2, .symbol = 3, .new_price = 10 } },
        .{ .cancel_order = .{ .order_id = 5, .uid = 2, .symbol = 3 } },
        .{ .reduce_order = .{ .order_id = 5, .uid = 2, .symbol = 3, .reduce_size = 1 } },
        .{ .request_order_book = .{ .symbol = 3, .depth = 11 } },
        .{ .add_user = 2 },
        .{ .suspend_user = 2 },
        .{ .resume_user = 2 },
        .reset,
        .nop,
    };

    for (commands) |api_command| {
        const translated = (try toOrdinaryOrderCommand(api_command)) orelse
            return error.ExpectedOrdinaryCommand;
        try std.testing.expectEqual(@as(i64, 0), translated.timestamp);
        try std.testing.expectEqual(cmd.CommandResultCode.new, translated.result_code);
    }
}

test "ordinary api command translation copies explicit timestamps and payload fields" {
    const timestamps = [_]i64{ 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111 };
    const commands = [_]ApiCommand{
        .{ .adjust_user_balance = .{
            .uid = 12,
            .transaction_id = 11,
            .currency = 13,
            .amount = 14,
            .adjustment_type = .@"suspend",
            .timestamp = timestamps[0],
        } },
        .{ .place_order = .{
            .order_id = 21,
            .uid = 22,
            .symbol = 23,
            .price = 24,
            .reserve_bid_price = 25,
            .size = 26,
            .action = .ask,
            .order_type = .ioc,
            .user_cookie = 27,
            .timestamp = timestamps[1],
        } },
        .{ .move_order = .{
            .order_id = 31,
            .uid = 32,
            .symbol = 33,
            .new_price = 34,
            .timestamp = timestamps[2],
        } },
        .{ .cancel_order = .{
            .order_id = 41,
            .uid = 42,
            .symbol = 43,
            .timestamp = timestamps[3],
        } },
        .{ .reduce_order = .{
            .order_id = 51,
            .uid = 52,
            .symbol = 53,
            .reduce_size = 54,
            .timestamp = timestamps[4],
        } },
        .{ .request_order_book = .{
            .symbol = 63,
            .depth = 64,
            .timestamp = timestamps[5],
        } },
        .{ .add_user_with_timestamp = .{
            .uid = 71,
            .timestamp = timestamps[6],
        } },
        .{ .suspend_user_with_timestamp = .{
            .uid = 72,
            .timestamp = timestamps[7],
        } },
        .{ .resume_user_with_timestamp = .{
            .uid = 73,
            .timestamp = timestamps[8],
        } },
        .{ .reset_with_timestamp = .{
            .timestamp = timestamps[9],
        } },
        .{ .nop_with_timestamp = .{
            .timestamp = timestamps[10],
        } },
    };
    const expected_types = [_]cmd.OrderCommandType{
        .balance_adjustment,
        .place_order,
        .move_order,
        .cancel_order,
        .reduce_order,
        .order_book_request,
        .add_user,
        .suspend_user,
        .resume_user,
        .reset,
        .nop,
    };

    for (commands, expected_types, timestamps) |api_command, command_type, timestamp| {
        const translated = (try toOrdinaryOrderCommand(api_command)) orelse
            return error.ExpectedOrdinaryCommand;
        try std.testing.expectEqual(command_type, translated.command);
        try std.testing.expectEqual(timestamp, translated.timestamp);
        try std.testing.expectEqual(cmd.CommandResultCode.new, translated.result_code);
    }

    const balance = (try toOrdinaryOrderCommand(commands[0])).?;
    try std.testing.expectEqual(@as(i64, 11), balance.order_id);
    try std.testing.expectEqual(@as(i64, 12), balance.uid);
    try std.testing.expectEqual(@as(i32, 13), balance.symbol);
    try std.testing.expectEqual(@as(i64, 14), balance.price);
    try std.testing.expectEqual(@as(?common.OrderType, .ioc), balance.order_type);

    const placed = (try toOrdinaryOrderCommand(commands[1])).?;
    try std.testing.expectEqual(@as(i64, 21), placed.order_id);
    try std.testing.expectEqual(@as(i64, 22), placed.uid);
    try std.testing.expectEqual(@as(i32, 23), placed.symbol);
    try std.testing.expectEqual(@as(i64, 24), placed.price);
    try std.testing.expectEqual(@as(i64, 25), placed.reserve_bid_price);
    try std.testing.expectEqual(@as(i64, 26), placed.size);
    try std.testing.expectEqual(@as(?common.OrderAction, .ask), placed.action);
    try std.testing.expectEqual(@as(?common.OrderType, .ioc), placed.order_type);
    try std.testing.expectEqual(@as(i32, 27), placed.user_cookie);

    const moved = (try toOrdinaryOrderCommand(commands[2])).?;
    try std.testing.expectEqual(@as(i64, 34), moved.price);
    const reduced = (try toOrdinaryOrderCommand(commands[4])).?;
    try std.testing.expectEqual(@as(i64, 54), reduced.size);
    const order_book = (try toOrdinaryOrderCommand(commands[5])).?;
    try std.testing.expectEqual(@as(i64, 64), order_book.size);
    const add_user = (try toOrdinaryOrderCommand(commands[6])).?;
    try std.testing.expectEqual(@as(i64, 71), add_user.uid);
    const suspend_user = (try toOrdinaryOrderCommand(commands[7])).?;
    try std.testing.expectEqual(@as(i64, 72), suspend_user.uid);
    const resume_user = (try toOrdinaryOrderCommand(commands[8])).?;
    try std.testing.expectEqual(@as(i64, 73), resume_user.uid);
}

test "ordinary api command translation rejects overflowing depth and excludes special paths" {
    try std.testing.expectError(
        error.OrderBookDepthOverflow,
        toOrdinaryOrderCommand(.{
            .request_order_book = .{
                .symbol = 1,
                .depth = std.math.maxInt(usize),
            },
        }),
    );

    const special = [_]ApiCommand{
        .{ .raw_command = .{ .command = .nop } },
        .{ .persist_state = .{ .snapshot_id = 1, .timestamp_ns = 2 } },
        .{ .grouping_control = .{ .timestamp_ns = 3, .mode = 1 } },
        .{ .binary_add_symbols = .{ .transfer_id = 4, .timestamp = 5, .symbols = &.{} } },
        .{ .binary_add_accounts = .{ .transfer_id = 6, .timestamp = 7, .users = &.{} } },
        .{ .binary_report_query = .{ .transfer_id = 8, .timestamp = 9, .query = .state_hash } },
    };
    for (special) |api_command| {
        try std.testing.expect((try toOrdinaryOrderCommand(api_command)) == null);
    }
}
