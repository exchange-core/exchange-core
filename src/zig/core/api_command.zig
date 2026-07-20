const std = @import("std");
const common = @import("common.zig");
const cmd = @import("cmd.zig");
const binary_payload = @import("binary_payload.zig");
const kernel_mod = @import("exchange_kernel.zig");

pub const BinaryCurrencyBalance = binary_payload.CurrencyBalance;
pub const BinaryUserAccountsBatch = binary_payload.UserAccountsBatch;
pub const BinaryReportQuery = binary_payload.DecodedReportQuery;
pub const PersistStateRequestPair = kernel_mod.ExchangeApi.PersistStateRequestPair;

pub const ApiCommand = union(enum) {
    raw_command: cmd.OrderCommand,
    add_user: i64,
    adjust_user_balance: struct {
        uid: i64,
        transaction_id: i64,
        currency: i32,
        amount: i64,
        adjustment_type: common.BalanceAdjustmentType,
    },
    suspend_user: i64,
    resume_user: i64,
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
    },
    move_order: struct {
        order_id: i64,
        uid: i64,
        symbol: i32,
        new_price: i64,
    },
    cancel_order: struct {
        order_id: i64,
        uid: i64,
        symbol: i32,
    },
    reduce_order: struct {
        order_id: i64,
        uid: i64,
        symbol: i32,
        reduce_size: i64,
    },
    request_order_book: struct {
        symbol: i32,
        depth: usize,
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
    nop,
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

test "api command result deinit leaves persist-state variant untouched" {
    var result: ApiCommandResult = .{ .persist_state = .success };
    result.deinit(std.testing.allocator);
}
