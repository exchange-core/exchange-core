const std = @import("std");
const common = @import("common.zig");
const cmd = @import("cmd.zig");
const config_mod = @import("config.zig");
const kernel_mod = @import("exchange_kernel.zig");
const orderbook_naive = @import("orderbook_naive.zig");
const risk = @import("risk.zig");

pub const OrderBookKind = kernel_mod.OrderBookKind;
pub const CommandResponse = kernel_mod.CommandResponse;
pub const LifecycleError = error{
    CoreStopped,
};
pub const ExecuteError = LifecycleError || kernel_mod.ExecuteError;
pub const ReportError = kernel_mod.ReportError;
pub const InitError = kernel_mod.MultiShardKernelSet.ShardedInitError;
pub const AggregateError = kernel_mod.MultiShardKernelSet.AggregateError;
pub const QueryReportError = AggregateError;
pub const InitFromJavaConfigError = config_mod.PerformanceConfig.ValidationError || InitError || error{
    MatchingEnginesNumOverflow,
    RiskEnginesExceedMatchingEngines,
};

pub const ShardedExchangeCore = struct {
    allocator: std.mem.Allocator,
    kernels: kernel_mod.MultiShardKernelSet,
    started: bool,
    stopped: bool,

    pub fn init(
        allocator: std.mem.Allocator,
        shard_count: usize,
        orderbook_kind: OrderBookKind,
    ) InitError!ShardedExchangeCore {
        return .{
            .allocator = allocator,
            .kernels = try kernel_mod.MultiShardKernelSet.init(
                allocator,
                shard_count,
                orderbook_kind,
            ),
            .started = false,
            .stopped = false,
        };
    }

    pub fn initFromJavaConfig(
        allocator: std.mem.Allocator,
        java_config: config_mod.JavaExchangeConfig,
    ) InitFromJavaConfigError!ShardedExchangeCore {
        const perf = java_config.performance_cfg;
        try perf.validate();
        if (perf.risk_engines_num > perf.matching_engines_num) {
            return error.RiskEnginesExceedMatchingEngines;
        }
        const shard_count = std.math.cast(usize, perf.matching_engines_num) orelse
            return error.MatchingEnginesNumOverflow;
        return init(allocator, shard_count, perf.orderbook_kind);
    }

    pub fn deinit(self: *ShardedExchangeCore) void {
        self.kernels.deinit();
        self.* = undefined;
    }

    pub fn shardCount(self: *const ShardedExchangeCore) usize {
        return self.kernels.kernels.len;
    }

    pub fn isStarted(self: *const ShardedExchangeCore) bool {
        return self.started;
    }

    pub fn isStopped(self: *const ShardedExchangeCore) bool {
        return self.stopped;
    }

    pub fn startup(self: *ShardedExchangeCore) LifecycleError!void {
        if (self.stopped) return error.CoreStopped;
        self.started = true;
    }

    pub fn shutdown(self: *ShardedExchangeCore) void {
        self.stopped = true;
    }

    pub fn addSymbolAll(
        self: *ShardedExchangeCore,
        spec: common.CoreSymbolSpecification,
    ) risk.SymbolSpecificationProvider.AddSymbolError!void {
        try self.kernels.addSymbolAll(spec);
    }

    pub fn shardIndexForUid(self: *const ShardedExchangeCore, uid: i64) usize {
        return self.kernels.shardIndexForUid(uid);
    }

    pub fn shardIndexForSymbol(self: *const ShardedExchangeCore, symbol: i32) usize {
        return self.kernels.shardIndexForSymbol(symbol);
    }

    pub fn shardIndexForCommand(
        self: *const ShardedExchangeCore,
        command: *const cmd.OrderCommand,
    ) usize {
        return self.kernels.shardIndexForCommand(command);
    }

    pub fn executeRouted(
        self: *ShardedExchangeCore,
        command: *cmd.OrderCommand,
    ) ExecuteError!CommandResponse {
        try self.ensureRunning();
        return self.kernels.executeRouted(command);
    }

    pub fn executeSplit(
        self: *ShardedExchangeCore,
        command: *cmd.OrderCommand,
    ) ExecuteError!CommandResponse {
        try self.ensureRunning();
        return self.kernels.executeSplit(command);
    }

    pub fn queryUserBalance(
        self: *ShardedExchangeCore,
        uid: i64,
        currency: i32,
    ) ReportError!?i64 {
        return self.kernels.queryUserBalance(uid, currency);
    }

    pub fn queryReport(
        self: *ShardedExchangeCore,
        query: kernel_mod.ReportQuery,
    ) QueryReportError!kernel_mod.ReportResponse {
        return switch (query) {
            .total_currency_balance => .{
                .total_currency_balance = try self.kernels.aggregateTotalCurrencyBalance(),
            },
            .user_balance => |target| .{
                .user_balance = try self.kernels.queryUserBalance(target.uid, target.currency),
            },
            .single_user => |uid| blk: {
                const idx = self.kernels.shardIndexForUid(uid);
                break :blk try self.kernels.kernels[idx].queryReport(query);
            },
            .order_book => |request| blk: {
                const idx = self.kernels.shardIndexForSymbol(request.symbol);
                break :blk try self.kernels.kernels[idx].queryReport(query);
            },
            .risk_state_hash => blk: {
                var hasher = std.hash.Wyhash.init(0);
                for (self.kernels.kernels, 0..) |*kernel, shard_idx| {
                    var report = try kernel.queryReport(.risk_state_hash);
                    defer report.deinit(self.allocator);
                    switch (report) {
                        .risk_state_hash => |value| {
                            const shard_i64 = std.math.cast(i64, shard_idx) orelse
                                return error.ArithmeticOverflow;
                            hasher.update(std.mem.asBytes(&shard_i64));
                            hasher.update(std.mem.asBytes(&value));
                        },
                        else => return error.StateHashCorrupted,
                    }
                }
                break :blk .{ .risk_state_hash = hasher.final() };
            },
            .state_hash_detailed => blk: {
                var combined: kernel_mod.DetailedStateHashReport = .{
                    .aggregate_hash = 0,
                };
                errdefer combined.deinit(self.allocator);

                for (self.kernels.kernels) |*kernel| {
                    var report = try kernel.queryReport(.state_hash_detailed);
                    defer report.deinit(self.allocator);
                    switch (report) {
                        .state_hash_detailed => |*detailed| {
                            try combined.submodules.appendSlice(
                                self.allocator,
                                detailed.submodules.items,
                            );
                        },
                        else => return error.StateHashCorrupted,
                    }
                }

                combined.aggregate_hash = try self.kernels.detailedHashFingerprint();
                break :blk .{ .state_hash_detailed = combined };
            },
            .fee_collected => |currency| blk: {
                var total: i64 = 0;
                for (self.kernels.kernels) |*kernel| {
                    var report = try kernel.queryReport(.{ .fee_collected = currency });
                    defer report.deinit(self.allocator);
                    switch (report) {
                        .fee_collected => |value| {
                            total = std.math.add(i64, total, value) catch
                                return error.ArithmeticOverflow;
                        },
                        else => return error.StateHashCorrupted,
                    }
                }
                break :blk .{ .fee_collected = total };
            },
        };
    }

    pub fn aggregateTotalCurrencyBalance(
        self: *ShardedExchangeCore,
    ) AggregateError!kernel_mod.TotalCurrencyBalanceReport {
        return self.kernels.aggregateTotalCurrencyBalance();
    }

    pub fn detailedHashFingerprint(self: *ShardedExchangeCore) ReportError!u64 {
        return self.kernels.detailedHashFingerprint();
    }

    fn ensureRunning(self: *const ShardedExchangeCore) LifecycleError!void {
        if (self.stopped) return error.CoreStopped;
    }
};

fn makeAddUserCommand(uid: i64) cmd.OrderCommand {
    return .{
        .command = .add_user,
        .uid = uid,
        .result_code = .new,
    };
}

fn makeBalanceAdjustmentCommand(
    uid: i64,
    currency: i32,
    amount: i64,
    transaction_id: i64,
) cmd.OrderCommand {
    return .{
        .command = .balance_adjustment,
        .uid = uid,
        .symbol = currency,
        .price = amount,
        .order_id = transaction_id,
        .order_type = .gtc,
        .result_code = .new,
    };
}

fn makePlaceOrderCommand(
    order_id: i64,
    uid: i64,
    symbol: i32,
    price: i64,
    reserve_bid_price: i64,
    size: i64,
    action: common.OrderAction,
    order_type: common.OrderType,
) cmd.OrderCommand {
    return .{
        .command = .place_order,
        .order_id = order_id,
        .uid = uid,
        .symbol = symbol,
        .price = price,
        .reserve_bid_price = reserve_bid_price,
        .size = size,
        .action = action,
        .order_type = order_type,
        .result_code = .new,
    };
}

fn makeOrderBookRequestCommand(symbol: i32, depth: usize) cmd.OrderCommand {
    return .{
        .command = .order_book_request,
        .symbol = symbol,
        .size = @as(i64, @intCast(depth)),
        .result_code = .new,
    };
}

fn makeMoveOrderCommand(order_id: i64, uid: i64, symbol: i32, new_price: i64) cmd.OrderCommand {
    return .{
        .command = .move_order,
        .order_id = order_id,
        .uid = uid,
        .symbol = symbol,
        .price = new_price,
        .result_code = .new,
    };
}

fn makeCancelOrderCommand(order_id: i64, uid: i64, symbol: i32) cmd.OrderCommand {
    return .{
        .command = .cancel_order,
        .order_id = order_id,
        .uid = uid,
        .symbol = symbol,
        .result_code = .new,
    };
}

fn makeReduceOrderCommand(order_id: i64, uid: i64, symbol: i32, reduce_size: i64) cmd.OrderCommand {
    return .{
        .command = .reduce_order,
        .order_id = order_id,
        .uid = uid,
        .symbol = symbol,
        .size = reduce_size,
        .result_code = .new,
    };
}

fn expectL2MarketDataEqual(
    lhs: *const orderbook_naive.L2MarketData,
    rhs: *const orderbook_naive.L2MarketData,
) !void {
    try std.testing.expectEqual(lhs.asks.items.len, rhs.asks.items.len);
    for (lhs.asks.items, rhs.asks.items) |left, right| {
        try std.testing.expectEqual(left.price, right.price);
        try std.testing.expectEqual(left.volume, right.volume);
        try std.testing.expectEqual(left.orders, right.orders);
    }

    try std.testing.expectEqual(lhs.bids.items.len, rhs.bids.items.len);
    for (lhs.bids.items, rhs.bids.items) |left, right| {
        try std.testing.expectEqual(left.price, right.price);
        try std.testing.expectEqual(left.volume, right.volume);
        try std.testing.expectEqual(left.orders, right.orders);
    }
}

fn expectMapEqualI32I64(
    lhs: *const std.AutoHashMapUnmanaged(i32, i64),
    rhs: *const std.AutoHashMapUnmanaged(i32, i64),
) !void {
    try std.testing.expectEqual(lhs.count(), rhs.count());
    var lhs_it = lhs.iterator();
    while (lhs_it.next()) |entry| {
        const key = entry.key_ptr.*;
        try std.testing.expect(rhs.contains(key));
        try std.testing.expectEqual(entry.value_ptr.*, rhs.get(key).?);
    }
}

test "sharded exchange core init enforces power-of-two shard counts" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(error.InvalidShardCount, ShardedExchangeCore.init(allocator, 0, .direct));
    try std.testing.expectError(error.InvalidShardCount, ShardedExchangeCore.init(allocator, 3, .direct));

    var core = try ShardedExchangeCore.init(allocator, 4, .direct);
    defer core.deinit();
}

test "sharded exchange core lifecycle startup and shutdown flags are deterministic" {
    const allocator = std.testing.allocator;
    var core = try ShardedExchangeCore.init(allocator, 4, .direct);
    defer core.deinit();

    try std.testing.expect(!core.isStarted());
    try std.testing.expect(!core.isStopped());

    try core.startup();
    try std.testing.expect(core.isStarted());
    try std.testing.expect(!core.isStopped());

    try core.startup();
    try std.testing.expect(core.isStarted());
    try std.testing.expect(!core.isStopped());

    core.shutdown();
    try std.testing.expect(core.isStopped());
    try std.testing.expectError(error.CoreStopped, core.startup());
}

test "sharded exchange core blocks routed and split execution after shutdown" {
    const allocator = std.testing.allocator;
    var core = try ShardedExchangeCore.init(allocator, 4, .direct);
    defer core.deinit();

    core.shutdown();

    var command = makeAddUserCommand(99_501);
    try std.testing.expectError(error.CoreStopped, core.executeRouted(&command));
    try std.testing.expectError(error.CoreStopped, core.executeSplit(&command));
}

test "sharded exchange core initFromJavaConfig supports throughput profile shard mapping" {
    const allocator = std.testing.allocator;
    var core = try ShardedExchangeCore.initFromJavaConfig(allocator, .{
        .performance_cfg = config_mod.PerformanceConfig.throughputPreset(),
    });
    defer core.deinit();

    try std.testing.expectEqual(@as(usize, 4), core.shardCount());
}

test "sharded exchange core initFromJavaConfig rejects risk engines over matching engines" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(
        error.RiskEnginesExceedMatchingEngines,
        ShardedExchangeCore.initFromJavaConfig(allocator, .{
            .performance_cfg = .{
                .matching_engines_num = 2,
                .risk_engines_num = 4,
            },
        }),
    );
}

test "sharded exchange core routes uid and symbol deterministically" {
    const allocator = std.testing.allocator;
    var core = try ShardedExchangeCore.init(allocator, 4, .direct);
    defer core.deinit();

    try std.testing.expectEqual(@as(usize, 1), core.shardIndexForUid(1));
    try std.testing.expectEqual(@as(usize, 0), core.shardIndexForUid(4));

    const symbol: i32 = 241;
    try std.testing.expectEqual(@as(usize, 1), core.shardIndexForSymbol(symbol));

    var request = makeOrderBookRequestCommand(symbol, 8);
    request.uid = 12_345;
    try std.testing.expectEqual(core.shardIndexForSymbol(symbol), core.shardIndexForCommand(&request));

    var binary_command: cmd.OrderCommand = .{
        .command = .binary_data_command,
        .user_cookie = 17,
        .uid = 999_999,
        .result_code = .new,
    };
    try std.testing.expectEqual(
        core.shardIndexForSymbol(17),
        core.shardIndexForCommand(&binary_command),
    );
}

test "sharded exchange core split execution keeps single-kernel result-code parity on basic flow" {
    const allocator = std.testing.allocator;

    var single = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer single.deinit();

    var sharded = try ShardedExchangeCore.init(allocator, 4, .direct);
    defer sharded.deinit();

    const symbol: i32 = 241;
    const base_currency: i32 = 11;
    const quote_currency: i32 = 15;
    const maker_uid: i64 = 9_002;
    const taker_uid: i64 = 9_003;

    const spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol,
        .symbol_type = .currency_exchange_pair,
        .base_currency = base_currency,
        .quote_currency = quote_currency,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
    });

    _ = try single.addSymbol(spec);
    try sharded.addSymbolAll(spec);

    var commands = [_]cmd.OrderCommand{
        makeAddUserCommand(maker_uid),
        makeAddUserCommand(taker_uid),
        makeBalanceAdjustmentCommand(maker_uid, base_currency, 1_000, 1),
        makeBalanceAdjustmentCommand(taker_uid, quote_currency, 1_000, 2),
        makePlaceOrderCommand(7_101, maker_uid, symbol, 100, 100, 5, .ask, .gtc),
        makePlaceOrderCommand(7_201, taker_uid, symbol, 100, 100, 5, .bid, .gtc),
        makeOrderBookRequestCommand(symbol, 8),
    };

    for (&commands) |*source| {
        var single_command = source.*;
        var split_command = source.*;

        var single_response = try single.execute(&single_command);
        defer single_response.deinit(allocator);

        var split_response = try sharded.executeSplit(&split_command);
        defer split_response.deinit(allocator);

        try std.testing.expectEqual(single_response.sequence, split_response.sequence);
        try std.testing.expectEqual(single_response.result_code, split_response.result_code);

        if (single_response.l2_snapshot) |single_l2| {
            try std.testing.expect(split_response.l2_snapshot != null);
            const split_l2 = split_response.l2_snapshot.?;
            try std.testing.expectEqual(single_l2.asks.items.len, split_l2.asks.items.len);
            try std.testing.expectEqual(single_l2.bids.items.len, split_l2.bids.items.len);
        } else {
            try std.testing.expect(split_response.l2_snapshot == null);
        }
    }
}

test "sharded exchange core split execution keeps lifecycle parity for move-reduce-cancel flow" {
    const allocator = std.testing.allocator;

    var single = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer single.deinit();

    var sharded = try ShardedExchangeCore.init(allocator, 4, .direct);
    defer sharded.deinit();

    const symbol: i32 = 242;
    const base_currency: i32 = 11;
    const quote_currency: i32 = 15;
    const maker_uid: i64 = 10_002;
    const taker_uid: i64 = 10_003;

    const spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol,
        .symbol_type = .currency_exchange_pair,
        .base_currency = base_currency,
        .quote_currency = quote_currency,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
    });

    _ = try single.addSymbol(spec);
    try sharded.addSymbolAll(spec);

    var commands = [_]cmd.OrderCommand{
        makeAddUserCommand(maker_uid),
        makeAddUserCommand(taker_uid),
        makeBalanceAdjustmentCommand(maker_uid, base_currency, 50_000, 101),
        makeBalanceAdjustmentCommand(taker_uid, quote_currency, 50_000, 102),
        makePlaceOrderCommand(8_101, maker_uid, symbol, 103, 103, 10, .ask, .gtc),
        makeMoveOrderCommand(8_101, maker_uid, symbol, 101),
        makePlaceOrderCommand(8_201, taker_uid, symbol, 101, 101, 4, .bid, .ioc),
        makeReduceOrderCommand(8_101, maker_uid, symbol, 2),
        makeCancelOrderCommand(8_101, maker_uid, symbol),
        makeOrderBookRequestCommand(symbol, 8),
    };

    for (&commands) |*source| {
        var single_command = source.*;
        var split_command = source.*;

        var single_response = try single.execute(&single_command);
        defer single_response.deinit(allocator);

        var split_response = try sharded.executeSplit(&split_command);
        defer split_response.deinit(allocator);

        try std.testing.expectEqual(single_response.sequence, split_response.sequence);
        try std.testing.expectEqual(single_response.result_code, split_response.result_code);

        if (single_response.l2_snapshot) |single_l2| {
            try std.testing.expect(split_response.l2_snapshot != null);
            const split_l2 = split_response.l2_snapshot.?;
            try expectL2MarketDataEqual(&single_l2, &split_l2);
        } else {
            try std.testing.expect(split_response.l2_snapshot == null);
        }
    }

    const currencies = [_]i32{ base_currency, quote_currency };
    for ([_]i64{ maker_uid, taker_uid }) |uid| {
        for (currencies) |currency| {
            var single_balance = try single.queryReport(.{
                .user_balance = .{
                    .uid = uid,
                    .currency = currency,
                },
            });
            defer single_balance.deinit(allocator);
            const split_balance = try sharded.queryUserBalance(uid, currency);
            try std.testing.expectEqual(single_balance.user_balance, split_balance);
        }
    }

    var single_order_book = try single.queryReport(.{
        .order_book = .{
            .symbol = symbol,
            .depth = 8,
        },
    });
    defer single_order_book.deinit(allocator);

    var split_order_book = try sharded.queryReport(.{
        .order_book = .{
            .symbol = symbol,
            .depth = 8,
        },
    });
    defer split_order_book.deinit(allocator);

    switch (single_order_book) {
        .order_book => |*single_snapshot| {
            switch (split_order_book) {
                .order_book => |*split_snapshot| {
                    try expectL2MarketDataEqual(single_snapshot, split_snapshot);
                },
                else => unreachable,
            }
        },
        else => unreachable,
    }

    var single_total_report = try single.queryReport(.total_currency_balance);
    defer single_total_report.deinit(allocator);
    var split_total_report = try sharded.aggregateTotalCurrencyBalance();
    defer split_total_report.deinit(allocator);

    switch (single_total_report) {
        .total_currency_balance => |*single_total| {
            try expectMapEqualI32I64(&single_total.account_balances, &split_total_report.account_balances);
            try expectMapEqualI32I64(&single_total.orders_balances, &split_total_report.orders_balances);
            try expectMapEqualI32I64(&single_total.fees, &split_total_report.fees);
            try expectMapEqualI32I64(&single_total.adjustments, &split_total_report.adjustments);
            try expectMapEqualI32I64(&single_total.suspends, &split_total_report.suspends);
            try expectMapEqualI32I64(&single_total.open_interest_long, &split_total_report.open_interest_long);
            try expectMapEqualI32I64(&single_total.open_interest_short, &split_total_report.open_interest_short);
        },
        else => unreachable,
    }
}
