const std = @import("std");
const cmd = @import("cmd.zig");
const common = @import("common.zig");
const events = @import("events.zig");
const kernel = @import("exchange_kernel.zig");

pub const ApiCommand = union(enum) {
    place_order: PlaceOrder,
    move_order: MoveOrder,
    cancel_order: CancelOrder,
    reduce_order: ReduceOrder,
    order_book_request: OrderBookRequest,
    add_user: AddUser,
    balance_adjustment: BalanceAdjustment,
    suspend_user: SuspendUser,
    resume_user: ResumeUser,
    binary_data_query: BinaryDataQuery,
    binary_data_command: BinaryDataCommand,
    persist_state_matching: PersistState,
    persist_state_risk: PersistState,
    grouping_control: GroupingControl,
    nop: Nop,
    reset: Reset,
    shutdown_signal: ShutdownSignal,
    reserved_compressed: ReservedCompressed,
};

pub const PlaceOrder = struct {
    price: i64,
    size: i64,
    order_id: i64,
    action: common.OrderAction,
    order_type: common.OrderType,
    uid: i64,
    symbol: i32,
    user_cookie: i32,
    reserve_bid_price: i64,
    timestamp: i64,
};

pub const MoveOrder = struct {
    order_id: i64,
    new_price: i64,
    uid: i64,
    symbol: i32,
    timestamp: i64,
};

pub const CancelOrder = struct {
    order_id: i64,
    uid: i64,
    symbol: i32,
    timestamp: i64,
};

pub const ReduceOrder = struct {
    order_id: i64,
    uid: i64,
    symbol: i32,
    reduce_size: i64,
    timestamp: i64,
};

pub const OrderBookRequest = struct {
    symbol: i32,
    depth: i64,
    timestamp: i64,
};

pub const AddUser = struct {
    uid: i64,
    timestamp: i64,
};

pub const BalanceAdjustment = struct {
    uid: i64,
    currency: i32,
    amount: i64,
    transaction_id: i64,
    timestamp: i64,
};

pub const SuspendUser = struct {
    uid: i64,
    timestamp: i64,
};

pub const ResumeUser = struct {
    uid: i64,
    timestamp: i64,
};

pub const BinaryDataQuery = struct {
    transfer_id: i32,
    timestamp: i64,
};

pub const BinaryDataCommand = struct {
    transfer_id: i32,
    timestamp: i64,
};

pub const PersistState = struct {
    dump_id: i64,
    timestamp: i64,
};

pub const GroupingControl = struct {
    mode: i64,
    timestamp: i64,
};

pub const Nop = struct {
    timestamp: i64,
};

pub const Reset = struct {
    timestamp: i64,
};

pub const ShutdownSignal = struct {
    timestamp: i64,
};

pub const ReservedCompressed = struct {
    timestamp: i64,
};

pub const ApiCommandResult = struct {
    command: ApiCommand,
    result_code: cmd.CommandResultCode,
    sequence: u64,
};

pub const Trade = struct {
    maker_order_id: i64,
    maker_uid: i64,
    maker_order_completed: bool,
    price: i64,
    volume: i64,
};

pub const TradeEvent = struct {
    symbol: i32,
    total_volume: i64,
    taker_order_id: i64,
    taker_uid: i64,
    taker_action: common.OrderAction,
    taker_order_completed: bool,
    timestamp: i64,
    trades: []const Trade,
};

pub const ReduceEvent = struct {
    symbol: i32,
    reduced_volume: i64,
    order_completed: bool,
    price: i64,
    order_id: i64,
    uid: i64,
    timestamp: i64,
};

pub const RejectEvent = struct {
    symbol: i32,
    rejected_volume: i64,
    price: i64,
    order_id: i64,
    uid: i64,
    timestamp: i64,
};

pub const OrderBookRecord = struct {
    price: i64,
    volume: i64,
    orders: u32,
};

pub const OrderBook = struct {
    symbol: i32,
    asks: []const OrderBookRecord,
    bids: []const OrderBookRecord,
    timestamp: i64,
};

pub const EventsHandler = struct {
    context: ?*anyopaque = null,
    on_command_result: ?*const fn (?*anyopaque, ApiCommandResult) void = null,
    on_trade_event: ?*const fn (?*anyopaque, TradeEvent) void = null,
    on_reject_event: ?*const fn (?*anyopaque, RejectEvent) void = null,
    on_reduce_event: ?*const fn (?*anyopaque, ReduceEvent) void = null,
    on_order_book: ?*const fn (?*anyopaque, OrderBook) void = null,
};

/// Guards an immutable handler snapshot against same-thread reconfiguration
/// during a multi-callback command. External mutations are excluded by the
/// owning core's callback execution barrier; reentrant mutations change the
/// revision and suppress the remaining callbacks from the stale snapshot.
pub const InvocationGuard = struct {
    revision: *const u64,
    expected: u64,

    pub fn current(self: InvocationGuard) bool {
        return self.revision.* == self.expected;
    }
};

pub const ProcessError = std.mem.Allocator.Error || error{
    MissingPlaceOrderAction,
    MissingPlaceOrderType,
    ReduceEventMustBeSingle,
    ArithmeticOverflow,
};

pub const SimpleEventsProcessor = struct {
    allocator: std.mem.Allocator,
    handler: EventsHandler,
    trades: std.ArrayListUnmanaged(Trade) = .empty,
    asks: std.ArrayListUnmanaged(OrderBookRecord) = .empty,
    bids: std.ArrayListUnmanaged(OrderBookRecord) = .empty,

    pub fn init(allocator: std.mem.Allocator, handler: EventsHandler) SimpleEventsProcessor {
        return .{
            .allocator = allocator,
            .handler = handler,
        };
    }

    pub fn deinit(self: *SimpleEventsProcessor) void {
        self.trades.deinit(self.allocator);
        self.asks.deinit(self.allocator);
        self.bids.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn accept(
        self: *SimpleEventsProcessor,
        command: *const cmd.OrderCommand,
        sequence: u64,
        response: *const kernel.CommandResponse,
        matcher_events_head: ?*const events.MatcherTradeEvent,
    ) ProcessError!void {
        try self.acceptWithHandler(
            self.handler,
            null,
            command,
            sequence,
            response,
            matcher_events_head,
        );
    }

    /// Processes one result with an immutable handler snapshot. Runtime owners
    /// can replace their configured handler concurrently without racing an
    /// in-flight callback or retaining the configuration lock during user code.
    pub fn acceptWithHandler(
        self: *SimpleEventsProcessor,
        handler: EventsHandler,
        invocation_guard: ?InvocationGuard,
        command: *const cmd.OrderCommand,
        sequence: u64,
        response: *const kernel.CommandResponse,
        matcher_events_head: ?*const events.MatcherTradeEvent,
    ) ProcessError!void {
        if (!invocationIsCurrent(invocation_guard)) return;
        try self.sendCommandResult(handler, invocation_guard, command, sequence);
        if (!invocationIsCurrent(invocation_guard)) return;
        try self.sendMatcherEvents(handler, invocation_guard, command, matcher_events_head);
        if (!invocationIsCurrent(invocation_guard)) return;
        try self.sendOrderBook(handler, invocation_guard, command, response);
    }

    fn sendCommandResult(
        self: *SimpleEventsProcessor,
        handler: EventsHandler,
        invocation_guard: ?InvocationGuard,
        command: *const cmd.OrderCommand,
        sequence: u64,
    ) ProcessError!void {
        _ = self;
        // Keep Java parity: only the allow-list below emits commandResult callbacks.
        // Control commands and accepted binary payload fragments are intentionally silent.
        const command_payload: ApiCommand = switch (command.command) {
            .place_order => .{
                .place_order = .{
                    .price = command.price,
                    .size = command.size,
                    .order_id = command.order_id,
                    .action = command.action orelse return error.MissingPlaceOrderAction,
                    .order_type = command.order_type orelse return error.MissingPlaceOrderType,
                    .uid = command.uid,
                    .symbol = command.symbol,
                    .user_cookie = command.user_cookie,
                    .reserve_bid_price = command.reserve_bid_price,
                    .timestamp = command.timestamp,
                },
            },
            .move_order => .{
                .move_order = .{
                    .order_id = command.order_id,
                    .new_price = command.price,
                    .uid = command.uid,
                    .symbol = command.symbol,
                    .timestamp = command.timestamp,
                },
            },
            .cancel_order => .{
                .cancel_order = .{
                    .order_id = command.order_id,
                    .uid = command.uid,
                    .symbol = command.symbol,
                    .timestamp = command.timestamp,
                },
            },
            .reduce_order => .{
                .reduce_order = .{
                    .order_id = command.order_id,
                    .uid = command.uid,
                    .symbol = command.symbol,
                    .reduce_size = command.size,
                    .timestamp = command.timestamp,
                },
            },
            .order_book_request => .{
                .order_book_request = .{
                    .symbol = command.symbol,
                    .depth = command.size,
                    .timestamp = command.timestamp,
                },
            },
            .add_user => .{
                .add_user = .{
                    .uid = command.uid,
                    .timestamp = command.timestamp,
                },
            },
            .balance_adjustment => .{
                .balance_adjustment = .{
                    .uid = command.uid,
                    .currency = command.symbol,
                    .amount = command.price,
                    .transaction_id = command.order_id,
                    .timestamp = command.timestamp,
                },
            },
            .binary_data_command => blk: {
                if (command.result_code == .accepted) return;
                break :blk .{
                    .binary_data_command = .{
                        .transfer_id = command.user_cookie,
                        .timestamp = command.timestamp,
                    },
                };
            },
            .suspend_user,
            .resume_user,
            .binary_data_query,
            .persist_state_matching,
            .persist_state_risk,
            .grouping_control,
            .nop,
            .reset,
            .shutdown_signal,
            .reserved_compressed,
            => return,
        };

        if (!invocationIsCurrent(invocation_guard)) return;
        if (handler.on_command_result) |callback| {
            callback(handler.context, .{
                .command = command_payload,
                .result_code = command.result_code,
                .sequence = sequence,
            });
        }
    }

    fn sendMatcherEvents(
        self: *SimpleEventsProcessor,
        handler: EventsHandler,
        invocation_guard: ?InvocationGuard,
        command: *const cmd.OrderCommand,
        matcher_events_head: ?*const events.MatcherTradeEvent,
    ) ProcessError!void {
        const head = matcher_events_head orelse return;

        if (head.event_type == .reduce) {
            if (head.next_event != null) return error.ReduceEventMustBeSingle;
            if (!invocationIsCurrent(invocation_guard)) return;
            if (handler.on_reduce_event) |callback| {
                callback(handler.context, .{
                    .symbol = command.symbol,
                    .reduced_volume = head.size,
                    .order_completed = head.active_order_completed,
                    .price = head.price,
                    .order_id = command.order_id,
                    .uid = command.uid,
                    .timestamp = command.timestamp,
                });
            }
            return;
        }

        self.trades.clearRetainingCapacity();
        var reject_event: ?RejectEvent = null;
        var total_volume: i64 = 0;
        var taker_order_completed = false;

        var node = matcher_events_head;
        while (node) |evt| : (node = evt.next_event) {
            switch (evt.event_type) {
                .trade => {
                    try self.trades.append(self.allocator, .{
                        .maker_order_id = evt.matched_order_id,
                        .maker_uid = evt.matched_order_uid,
                        .maker_order_completed = evt.matched_order_completed,
                        .price = evt.price,
                        .volume = evt.size,
                    });
                    total_volume = std.math.add(i64, total_volume, evt.size) catch return error.ArithmeticOverflow;
                    if (evt.active_order_completed) {
                        taker_order_completed = true;
                    }
                },
                .reject => {
                    reject_event = .{
                        .symbol = command.symbol,
                        .rejected_volume = evt.size,
                        .price = evt.price,
                        .order_id = command.order_id,
                        .uid = command.uid,
                        .timestamp = command.timestamp,
                    };
                },
                .reduce,
                .binary_event,
                => {},
            }
        }

        if (self.trades.items.len > 0) {
            if (!invocationIsCurrent(invocation_guard)) return;
            if (handler.on_trade_event) |callback| {
                callback(handler.context, .{
                    .symbol = command.symbol,
                    .total_volume = total_volume,
                    .taker_order_id = command.order_id,
                    .taker_uid = command.uid,
                    .taker_action = command.action orelse return error.MissingPlaceOrderAction,
                    .taker_order_completed = taker_order_completed,
                    .timestamp = command.timestamp,
                    .trades = self.trades.items,
                });
            }
        }

        if (reject_event) |evt| {
            if (!invocationIsCurrent(invocation_guard)) return;
            if (handler.on_reject_event) |callback| {
                callback(handler.context, evt);
            }
        }
    }

    fn sendOrderBook(
        self: *SimpleEventsProcessor,
        handler: EventsHandler,
        invocation_guard: ?InvocationGuard,
        command: *const cmd.OrderCommand,
        response: *const kernel.CommandResponse,
    ) ProcessError!void {
        const snapshot = response.l2_snapshot orelse return;
        if (handler.on_order_book == null) return;

        self.asks.clearRetainingCapacity();
        self.bids.clearRetainingCapacity();

        for (snapshot.asks.items) |level| {
            try self.asks.append(self.allocator, .{
                .price = level.price,
                .volume = level.volume,
                .orders = level.orders,
            });
        }
        for (snapshot.bids.items) |level| {
            try self.bids.append(self.allocator, .{
                .price = level.price,
                .volume = level.volume,
                .orders = level.orders,
            });
        }

        if (!invocationIsCurrent(invocation_guard)) return;
        const callback = handler.on_order_book.?;
        callback(handler.context, .{
            .symbol = command.symbol,
            .asks = self.asks.items,
            .bids = self.bids.items,
            .timestamp = command.timestamp,
        });
    }
};

fn invocationIsCurrent(guard: ?InvocationGuard) bool {
    return if (guard) |value| value.current() else true;
}

test "simple events processor emits command, reduce, and orderbook callbacks in order" {
    const allocator = std.testing.allocator;

    const Step = enum { command, reduce, orderbook };
    const Collector = struct {
        allocator: std.mem.Allocator,
        steps: std.ArrayListUnmanaged(Step) = .empty,
        last_sequence: u64 = 0,
        reduced_volume: i64 = 0,
        ask_levels: usize = 0,
        bid_levels: usize = 0,

        fn onCommand(ctx: ?*anyopaque, event: ApiCommandResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.steps.append(self.allocator, .command) catch @panic("append");
            self.last_sequence = event.sequence;
        }

        fn onReduce(ctx: ?*anyopaque, event: ReduceEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.steps.append(self.allocator, .reduce) catch @panic("append");
            self.reduced_volume = event.reduced_volume;
        }

        fn onOrderBook(ctx: ?*anyopaque, event: OrderBook) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.steps.append(self.allocator, .orderbook) catch @panic("append");
            self.ask_levels = event.asks.len;
            self.bid_levels = event.bids.len;
        }

        fn deinit(self: *@This()) void {
            self.steps.deinit(self.allocator);
            self.* = undefined;
        }
    };

    var collector: Collector = .{ .allocator = allocator };
    defer collector.deinit();

    var processor = SimpleEventsProcessor.init(allocator, .{
        .context = &collector,
        .on_command_result = Collector.onCommand,
        .on_reduce_event = Collector.onReduce,
        .on_order_book = Collector.onOrderBook,
    });
    defer processor.deinit();

    var command: cmd.OrderCommand = .{
        .command = .reduce_order,
        .order_id = 9001,
        .symbol = 241,
        .uid = 7001,
        .size = 3,
        .timestamp = 111,
        .result_code = .success,
    };

    var response: kernel.CommandResponse = .{
        .sequence = 41,
        .command = command,
        .result_code = .success,
        .l2_snapshot = .{},
    };
    defer response.deinit(allocator);
    try response.l2_snapshot.?.asks.append(allocator, .{ .price = 101, .volume = 7, .orders = 2 });
    try response.l2_snapshot.?.bids.append(allocator, .{ .price = 99, .volume = 5, .orders = 1 });

    var reduce_event = events.MatcherTradeEvent{
        .event_type = .reduce,
        .size = 3,
        .active_order_completed = true,
        .price = 101,
    };

    try processor.accept(&command, response.sequence, &response, &reduce_event);

    try std.testing.expectEqual(@as(usize, 3), collector.steps.items.len);
    try std.testing.expectEqual(Step.command, collector.steps.items[0]);
    try std.testing.expectEqual(Step.reduce, collector.steps.items[1]);
    try std.testing.expectEqual(Step.orderbook, collector.steps.items[2]);
    try std.testing.expectEqual(@as(u64, 41), collector.last_sequence);
    try std.testing.expectEqual(@as(i64, 3), collector.reduced_volume);
    try std.testing.expectEqual(@as(usize, 1), collector.ask_levels);
    try std.testing.expectEqual(@as(usize, 1), collector.bid_levels);
}

test "simple events processor emits trade then reject for mixed matcher events" {
    const allocator = std.testing.allocator;

    const Step = enum { command, trade, reject };
    const Collector = struct {
        allocator: std.mem.Allocator,
        steps: std.ArrayListUnmanaged(Step) = .empty,
        total_volume: i64 = 0,
        trade_count: usize = 0,
        taker_order_completed: bool = false,
        rejected_volume: i64 = 0,

        fn onCommand(ctx: ?*anyopaque, _: ApiCommandResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.steps.append(self.allocator, .command) catch @panic("append");
        }

        fn onTrade(ctx: ?*anyopaque, event: TradeEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.steps.append(self.allocator, .trade) catch @panic("append");
            self.total_volume = event.total_volume;
            self.trade_count = event.trades.len;
            self.taker_order_completed = event.taker_order_completed;
        }

        fn onReject(ctx: ?*anyopaque, event: RejectEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.steps.append(self.allocator, .reject) catch @panic("append");
            self.rejected_volume = event.rejected_volume;
        }

        fn deinit(self: *@This()) void {
            self.steps.deinit(self.allocator);
            self.* = undefined;
        }
    };

    var collector: Collector = .{ .allocator = allocator };
    defer collector.deinit();

    var processor = SimpleEventsProcessor.init(allocator, .{
        .context = &collector,
        .on_command_result = Collector.onCommand,
        .on_trade_event = Collector.onTrade,
        .on_reject_event = Collector.onReject,
    });
    defer processor.deinit();

    var command: cmd.OrderCommand = .{
        .command = .place_order,
        .order_id = 5001,
        .symbol = 241,
        .uid = 7002,
        .action = .bid,
        .order_type = .gtc,
        .timestamp = 222,
        .result_code = .success,
    };

    const response: kernel.CommandResponse = .{
        .sequence = 77,
        .command = command,
        .result_code = .success,
    };

    var trade_1 = events.MatcherTradeEvent{
        .event_type = .trade,
        .matched_order_id = 9101,
        .matched_order_uid = 7001,
        .matched_order_completed = false,
        .price = 101,
        .size = 2,
        .active_order_completed = false,
    };
    var trade_2 = events.MatcherTradeEvent{
        .event_type = .trade,
        .matched_order_id = 9102,
        .matched_order_uid = 7001,
        .matched_order_completed = true,
        .price = 102,
        .size = 1,
        .active_order_completed = true,
    };
    var reject = events.MatcherTradeEvent{
        .event_type = .reject,
        .price = 103,
        .size = 4,
    };
    trade_1.next_event = &trade_2;
    trade_2.next_event = &reject;

    try processor.accept(&command, response.sequence, &response, &trade_1);

    try std.testing.expectEqual(@as(usize, 3), collector.steps.items.len);
    try std.testing.expectEqual(Step.command, collector.steps.items[0]);
    try std.testing.expectEqual(Step.trade, collector.steps.items[1]);
    try std.testing.expectEqual(Step.reject, collector.steps.items[2]);
    try std.testing.expectEqual(@as(i64, 3), collector.total_volume);
    try std.testing.expectEqual(@as(usize, 2), collector.trade_count);
    try std.testing.expect(collector.taker_order_completed);
    try std.testing.expectEqual(@as(i64, 4), collector.rejected_volume);
}

test "simple events processor ignores standalone binary matcher event without mutation" {
    const allocator = std.testing.allocator;

    const Collector = struct {
        trade_count: usize = 0,
        reject_count: usize = 0,
        reduce_count: usize = 0,

        fn onTrade(ctx: ?*anyopaque, _: TradeEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.trade_count += 1;
        }

        fn onReject(ctx: ?*anyopaque, _: RejectEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.reject_count += 1;
        }

        fn onReduce(ctx: ?*anyopaque, _: ReduceEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.reduce_count += 1;
        }
    };

    var collector: Collector = .{};
    var processor = SimpleEventsProcessor.init(allocator, .{
        .context = &collector,
        .on_trade_event = Collector.onTrade,
        .on_reject_event = Collector.onReject,
        .on_reduce_event = Collector.onReduce,
    });
    defer processor.deinit();

    var command: cmd.OrderCommand = .{
        .command = .nop,
        .result_code = .success,
    };
    const response: kernel.CommandResponse = .{
        .sequence = 78,
        .command = command,
        .result_code = .success,
    };
    var binary_event = events.MatcherTradeEvent{
        .event_type = .binary_event,
        .section = 7,
        .price = 123,
        .size = 456,
    };

    try processor.accept(&command, response.sequence, &response, &binary_event);

    try std.testing.expectEqual(@as(usize, 0), collector.trade_count);
    try std.testing.expectEqual(@as(usize, 0), collector.reject_count);
    try std.testing.expectEqual(@as(usize, 0), collector.reduce_count);
    try std.testing.expectEqual(events.MatcherEventType.binary_event, binary_event.event_type);
    try std.testing.expectEqual(@as(i32, 7), binary_event.section);
    try std.testing.expectEqual(@as(i64, 123), binary_event.price);
    try std.testing.expectEqual(@as(i64, 456), binary_event.size);
    try std.testing.expect(binary_event.next_event == null);
}

test "simple events processor skips non-simple nodes while preserving trade and reject" {
    const allocator = std.testing.allocator;

    const Collector = struct {
        trade_count: usize = 0,
        reject_count: usize = 0,
        reduce_count: usize = 0,
        total_volume: i64 = 0,

        fn onTrade(ctx: ?*anyopaque, event: TradeEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.trade_count += 1;
            self.total_volume = event.total_volume;
        }

        fn onReject(ctx: ?*anyopaque, _: RejectEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.reject_count += 1;
        }

        fn onReduce(ctx: ?*anyopaque, _: ReduceEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.reduce_count += 1;
        }
    };

    var collector: Collector = .{};
    var processor = SimpleEventsProcessor.init(allocator, .{
        .context = &collector,
        .on_trade_event = Collector.onTrade,
        .on_reject_event = Collector.onReject,
        .on_reduce_event = Collector.onReduce,
    });
    defer processor.deinit();

    var command: cmd.OrderCommand = .{
        .command = .place_order,
        .order_id = 5002,
        .symbol = 241,
        .uid = 7003,
        .action = .ask,
        .order_type = .gtc,
        .result_code = .success,
    };
    const response: kernel.CommandResponse = .{
        .sequence = 79,
        .command = command,
        .result_code = .success,
    };

    var binary_event = events.MatcherTradeEvent{ .event_type = .binary_event };
    var trade = events.MatcherTradeEvent{
        .event_type = .trade,
        .matched_order_id = 9103,
        .matched_order_uid = 7004,
        .price = 101,
        .size = 2,
    };
    var embedded_reduce = events.MatcherTradeEvent{
        .event_type = .reduce,
        .price = 101,
        .size = 99,
    };
    var reject = events.MatcherTradeEvent{
        .event_type = .reject,
        .price = 102,
        .size = 3,
    };
    binary_event.next_event = &trade;
    trade.next_event = &embedded_reduce;
    embedded_reduce.next_event = &reject;

    try processor.accept(&command, response.sequence, &response, &binary_event);

    try std.testing.expectEqual(@as(usize, 1), collector.trade_count);
    try std.testing.expectEqual(@as(usize, 1), collector.reject_count);
    try std.testing.expectEqual(@as(usize, 0), collector.reduce_count);
    try std.testing.expectEqual(@as(i64, 2), collector.total_volume);
}

test "simple events processor rejects place-order command without action" {
    const allocator = std.testing.allocator;

    var processor = SimpleEventsProcessor.init(allocator, .{});
    defer processor.deinit();

    var command: cmd.OrderCommand = .{
        .command = .place_order,
        .order_type = .gtc,
        .result_code = .success,
    };
    const response: kernel.CommandResponse = .{
        .sequence = 1,
        .command = command,
        .result_code = .success,
    };

    try std.testing.expectError(
        error.MissingPlaceOrderAction,
        processor.accept(&command, response.sequence, &response, null),
    );
}

test "simple events processor suppresses accepted binary-data-command command result callback" {
    const allocator = std.testing.allocator;

    const Collector = struct {
        callback_count: usize = 0,

        fn onCommand(ctx: ?*anyopaque, _: ApiCommandResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.callback_count += 1;
        }
    };

    var collector: Collector = .{};
    var processor = SimpleEventsProcessor.init(allocator, .{
        .context = &collector,
        .on_command_result = Collector.onCommand,
    });
    defer processor.deinit();

    var command: cmd.OrderCommand = .{
        .command = .binary_data_command,
        .user_cookie = 17,
        .timestamp = 555,
        .result_code = .accepted,
    };
    const response: kernel.CommandResponse = .{
        .sequence = 12,
        .command = command,
        .result_code = .accepted,
    };

    try processor.accept(&command, response.sequence, &response, null);
    try std.testing.expectEqual(@as(usize, 0), collector.callback_count);
}

test "simple events processor emits failed binary-data-command command result callback" {
    const allocator = std.testing.allocator;

    const Collector = struct {
        callback_count: usize = 0,
        last_transfer_id: i32 = 0,
        last_result: cmd.CommandResultCode = .new,

        fn onCommand(ctx: ?*anyopaque, event: ApiCommandResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.callback_count += 1;
            self.last_result = event.result_code;
            switch (event.command) {
                .binary_data_command => |payload| self.last_transfer_id = payload.transfer_id,
                else => {},
            }
        }
    };

    var collector: Collector = .{};
    var processor = SimpleEventsProcessor.init(allocator, .{
        .context = &collector,
        .on_command_result = Collector.onCommand,
    });
    defer processor.deinit();

    var command: cmd.OrderCommand = .{
        .command = .binary_data_command,
        .user_cookie = 23,
        .timestamp = 777,
        .result_code = .binary_command_failed,
    };
    const response: kernel.CommandResponse = .{
        .sequence = 13,
        .command = command,
        .result_code = .binary_command_failed,
    };

    try processor.accept(&command, response.sequence, &response, null);
    try std.testing.expectEqual(@as(usize, 1), collector.callback_count);
    try std.testing.expectEqual(cmd.CommandResultCode.binary_command_failed, collector.last_result);
    try std.testing.expectEqual(@as(i32, 23), collector.last_transfer_id);
}

test "simple events processor suppresses nop command result callback for Java parity" {
    const allocator = std.testing.allocator;

    const Collector = struct {
        callback_count: usize = 0,

        fn onCommand(ctx: ?*anyopaque, _: ApiCommandResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.callback_count += 1;
        }
    };

    var collector: Collector = .{};
    var processor = SimpleEventsProcessor.init(allocator, .{
        .context = &collector,
        .on_command_result = Collector.onCommand,
    });
    defer processor.deinit();

    var command: cmd.OrderCommand = .{
        .command = .nop,
        .timestamp = 888,
        .result_code = .success,
    };
    const response: kernel.CommandResponse = .{
        .sequence = 14,
        .command = command,
        .result_code = .success,
    };

    try processor.accept(&command, response.sequence, &response, null);
    try std.testing.expectEqual(@as(usize, 0), collector.callback_count);
}
