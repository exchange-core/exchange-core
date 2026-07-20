const std = @import("std");
const common = @import("common.zig");
const cmd = @import("cmd.zig");
const events = @import("events.zig");

pub const L2Level = struct {
    price: i64,
    volume: i64,
    orders: u32,
};

pub const L2MarketData = struct {
    asks: std.ArrayListUnmanaged(L2Level) = .empty,
    bids: std.ArrayListUnmanaged(L2Level) = .empty,

    pub fn deinit(self: *L2MarketData, allocator: std.mem.Allocator) void {
        self.asks.deinit(allocator);
        self.bids.deinit(allocator);
        self.* = .{};
    }
};

pub const UserOrderView = struct {
    symbol: i32,
    order_id: i64,
    price: i64,
    size: i64,
    filled: i64,
    reserve_bid_price: i64,
    action: common.OrderAction,
    uid: i64,
    timestamp: i64,
};

pub const HoldAggregationError = std.mem.Allocator.Error || error{
    ArithmeticOverflow,
};
pub const RestoreOrdersError = std.mem.Allocator.Error || error{
    InvalidOrderSymbol,
    InvalidOrderSize,
    InvalidOrderFill,
    DuplicateOrderId,
};

const OrderDigest = struct {
    order_id: i64,
    price: i64,
    size: i64,
    filled: i64,
    reserve_bid_price: i64,
    action: common.OrderAction,
    uid: i64,
    timestamp: i64,
};

pub const Order = struct {
    order_id: i64,
    price: i64,
    size: i64,
    filled: i64,
    reserve_bid_price: i64,
    action: common.OrderAction,
    uid: i64,
    timestamp: i64,

    pub fn remaining(self: Order) i64 {
        return self.size - self.filled;
    }
};

const OrderLocation = struct {
    side: common.OrderAction,
    price: i64,
    bucket_index: usize,
    order_index: usize,
};

const OrderLocator = struct {
    side: common.OrderAction,
    price: i64,
};

const ActiveOrder = struct {
    order_id: i64,
    price: i64,
    size: i64,
    filled: i64,
    reserve_bid_price: i64,
    action: common.OrderAction,
    uid: i64,
    timestamp: i64,

    pub fn fromCommand(command: *const cmd.OrderCommand) ?ActiveOrder {
        const action = command.action orelse return null;
        return .{
            .order_id = command.order_id,
            .price = command.price,
            .size = command.size,
            .filled = 0,
            .reserve_bid_price = command.reserve_bid_price,
            .action = action,
            .uid = command.uid,
            .timestamp = command.timestamp,
        };
    }

    pub fn fromOrder(order: Order) ActiveOrder {
        return .{
            .order_id = order.order_id,
            .price = order.price,
            .size = order.size,
            .filled = order.filled,
            .reserve_bid_price = order.reserve_bid_price,
            .action = order.action,
            .uid = order.uid,
            .timestamp = order.timestamp,
        };
    }

    pub fn toOrder(self: ActiveOrder) Order {
        return .{
            .order_id = self.order_id,
            .price = self.price,
            .size = self.size,
            .filled = self.filled,
            .reserve_bid_price = self.reserve_bid_price,
            .action = self.action,
            .uid = self.uid,
            .timestamp = self.timestamp,
        };
    }

    pub fn remaining(self: ActiveOrder) i64 {
        return self.size - self.filled;
    }
};

const Bucket = struct {
    price: i64,
    total_volume: i64 = 0,
    orders: std.ArrayListUnmanaged(Order) = .empty,

    pub fn init(price: i64) Bucket {
        return .{ .price = price };
    }

    pub fn deinit(self: *Bucket, allocator: std.mem.Allocator) void {
        self.orders.deinit(allocator);
        self.* = .{
            .price = self.price,
        };
    }
};

pub const NaiveOrderBook = struct {
    allocator: std.mem.Allocator,
    symbol_spec: common.CoreSymbolSpecification,
    asks: std.ArrayListUnmanaged(Bucket) = .empty, // ascending prices
    bids: std.ArrayListUnmanaged(Bucket) = .empty, // descending prices
    id_index: std.AutoHashMapUnmanaged(i64, OrderLocator) = .empty,

    pub fn init(
        allocator: std.mem.Allocator,
        symbol_spec: common.CoreSymbolSpecification,
    ) common.CoreSymbolSpecification.ValidationError!NaiveOrderBook {
        try symbol_spec.validate();
        return .{
            .allocator = allocator,
            .symbol_spec = symbol_spec,
        };
    }

    pub fn deinit(self: *NaiveOrderBook) void {
        for (self.asks.items) |*bucket| {
            bucket.deinit(self.allocator);
        }
        self.asks.deinit(self.allocator);

        for (self.bids.items) |*bucket| {
            bucket.deinit(self.allocator);
        }
        self.bids.deinit(self.allocator);

        self.id_index.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn processCommand(
        self: *NaiveOrderBook,
        command: *cmd.OrderCommand,
    ) std.mem.Allocator.Error!cmd.CommandResultCode {
        return self.processCommandWithEvents(command, null);
    }

    pub fn processCommandWithEvents(
        self: *NaiveOrderBook,
        command: *cmd.OrderCommand,
        event_chain: ?*events.EventChain,
    ) std.mem.Allocator.Error!cmd.CommandResultCode {
        return switch (command.command) {
            .move_order => self.moveOrder(command, event_chain),
            .cancel_order => try self.cancelOrder(command, event_chain),
            .reduce_order => try self.reduceOrder(command, event_chain),
            .place_order => blk: {
                if (command.result_code == .valid_for_matching_engine) {
                    try self.newOrder(command, event_chain);
                    break :blk .success;
                }
                break :blk command.result_code;
            },
            .order_book_request => .success,
            else => .matching_unsupported_command,
        };
    }

    pub fn l2Snapshot(
        self: *const NaiveOrderBook,
        allocator: std.mem.Allocator,
        max_levels: usize,
    ) std.mem.Allocator.Error!L2MarketData {
        var data: L2MarketData = .{};
        errdefer data.deinit(allocator);

        const ask_levels = @min(max_levels, self.asks.items.len);
        const bid_levels = @min(max_levels, self.bids.items.len);

        try data.asks.ensureTotalCapacityPrecise(allocator, ask_levels);
        try data.bids.ensureTotalCapacityPrecise(allocator, bid_levels);

        for (self.asks.items[0..ask_levels]) |bucket| {
            data.asks.appendAssumeCapacity(.{
                .price = bucket.price,
                .volume = bucket.total_volume,
                .orders = @as(u32, @intCast(bucket.orders.items.len)),
            });
        }
        for (self.bids.items[0..bid_levels]) |bucket| {
            data.bids.appendAssumeCapacity(.{
                .price = bucket.price,
                .volume = bucket.total_volume,
                .orders = @as(u32, @intCast(bucket.orders.items.len)),
            });
        }

        return data;
    }

    pub fn validateInternalState(self: *const NaiveOrderBook) void {
        var order_count: usize = 0;

        var prev_ask_price: ?i64 = null;
        for (self.asks.items) |bucket| {
            if (prev_ask_price) |prev| {
                std.debug.assert(prev < bucket.price);
            }
            prev_ask_price = bucket.price;
            order_count += bucket.orders.items.len;
            self.validateBucket(bucket, .ask);
        }

        var prev_bid_price: ?i64 = null;
        for (self.bids.items) |bucket| {
            if (prev_bid_price) |prev| {
                std.debug.assert(prev > bucket.price);
            }
            prev_bid_price = bucket.price;
            order_count += bucket.orders.items.len;
            self.validateBucket(bucket, .bid);
        }

        std.debug.assert(order_count == self.id_index.count());
    }

    pub fn ordersNum(self: *const NaiveOrderBook, side: common.OrderAction) usize {
        const buckets = self.sideBucketsConst(side);
        var count: usize = 0;
        for (buckets.items) |bucket| {
            count += bucket.orders.items.len;
        }
        return count;
    }

    pub fn totalOrdersVolume(self: *const NaiveOrderBook, side: common.OrderAction) i64 {
        const buckets = self.sideBucketsConst(side);
        var total: i64 = 0;
        for (buckets.items) |bucket| {
            total += bucket.total_volume;
        }
        return total;
    }

    pub fn appendUserOrders(
        self: *const NaiveOrderBook,
        allocator: std.mem.Allocator,
        uid: i64,
        out: *std.ArrayListUnmanaged(UserOrderView),
    ) std.mem.Allocator.Error!void {
        for (self.asks.items) |bucket| {
            for (bucket.orders.items) |order| {
                if (order.uid != uid) continue;
                try out.append(allocator, .{
                    .symbol = self.symbol_spec.symbol_id,
                    .order_id = order.order_id,
                    .price = order.price,
                    .size = order.size,
                    .filled = order.filled,
                    .reserve_bid_price = order.reserve_bid_price,
                    .action = order.action,
                    .uid = order.uid,
                    .timestamp = order.timestamp,
                });
            }
        }
        for (self.bids.items) |bucket| {
            for (bucket.orders.items) |order| {
                if (order.uid != uid) continue;
                try out.append(allocator, .{
                    .symbol = self.symbol_spec.symbol_id,
                    .order_id = order.order_id,
                    .price = order.price,
                    .size = order.size,
                    .filled = order.filled,
                    .reserve_bid_price = order.reserve_bid_price,
                    .action = order.action,
                    .uid = order.uid,
                    .timestamp = order.timestamp,
                });
            }
        }
    }

    pub fn appendAllOrders(
        self: *const NaiveOrderBook,
        allocator: std.mem.Allocator,
        out: *std.ArrayListUnmanaged(UserOrderView),
    ) std.mem.Allocator.Error!void {
        for (self.asks.items) |bucket| {
            for (bucket.orders.items) |order| {
                try out.append(allocator, .{
                    .symbol = self.symbol_spec.symbol_id,
                    .order_id = order.order_id,
                    .price = order.price,
                    .size = order.size,
                    .filled = order.filled,
                    .reserve_bid_price = order.reserve_bid_price,
                    .action = order.action,
                    .uid = order.uid,
                    .timestamp = order.timestamp,
                });
            }
        }
        for (self.bids.items) |bucket| {
            for (bucket.orders.items) |order| {
                try out.append(allocator, .{
                    .symbol = self.symbol_spec.symbol_id,
                    .order_id = order.order_id,
                    .price = order.price,
                    .size = order.size,
                    .filled = order.filled,
                    .reserve_bid_price = order.reserve_bid_price,
                    .action = order.action,
                    .uid = order.uid,
                    .timestamp = order.timestamp,
                });
            }
        }
    }

    pub fn restoreOrders(
        self: *NaiveOrderBook,
        symbol: i32,
        orders: []const UserOrderView,
    ) RestoreOrdersError!void {
        if (symbol != self.symbol_spec.symbol_id) return error.InvalidOrderSymbol;

        self.clearRestingOrders();

        for (orders) |order| {
            if (order.symbol != symbol) return error.InvalidOrderSymbol;
            if (order.size <= 0) return error.InvalidOrderSize;
            if (order.filled < 0 or order.filled >= order.size) return error.InvalidOrderFill;

            const added = try self.placeRestingOrder(.{
                .order_id = order.order_id,
                .price = order.price,
                .size = order.size,
                .filled = order.filled,
                .reserve_bid_price = order.reserve_bid_price,
                .action = order.action,
                .uid = order.uid,
                .timestamp = order.timestamp,
            });
            if (!added) return error.DuplicateOrderId;
        }

        self.validateInternalState();
    }

    pub fn accumulateOrderHolds(
        self: *const NaiveOrderBook,
        allocator: std.mem.Allocator,
        out: *std.AutoHashMapUnmanaged(i32, i64),
    ) HoldAggregationError!void {
        for (self.asks.items) |bucket| {
            for (bucket.orders.items) |order| {
                const hold = calcAmountAsk(order.remaining(), self.symbol_spec) orelse
                    return error.ArithmeticOverflow;
                try addToMapValue(allocator, out, self.symbol_spec.base_currency, hold);
            }
        }

        for (self.bids.items) |bucket| {
            for (bucket.orders.items) |order| {
                const hold = calcAmountBidTakerFee(
                    order.remaining(),
                    order.reserve_bid_price,
                    self.symbol_spec,
                ) orelse return error.ArithmeticOverflow;
                try addToMapValue(allocator, out, self.symbol_spec.quote_currency, hold);
            }
        }
    }

    pub fn stateHash(self: *const NaiveOrderBook) std.mem.Allocator.Error!u64 {
        var hasher = std.hash.Wyhash.init(0);
        hashCoreSymbolSpecification(&hasher, self.symbol_spec);

        var digests: std.ArrayListUnmanaged(OrderDigest) = .empty;
        defer digests.deinit(self.allocator);
        try digests.ensureTotalCapacityPrecise(self.allocator, self.id_index.count());

        for (self.asks.items) |bucket| {
            for (bucket.orders.items) |order| {
                try digests.append(self.allocator, .{
                    .order_id = order.order_id,
                    .price = order.price,
                    .size = order.size,
                    .filled = order.filled,
                    .reserve_bid_price = order.reserve_bid_price,
                    .action = order.action,
                    .uid = order.uid,
                    .timestamp = order.timestamp,
                });
            }
        }
        for (self.bids.items) |bucket| {
            for (bucket.orders.items) |order| {
                try digests.append(self.allocator, .{
                    .order_id = order.order_id,
                    .price = order.price,
                    .size = order.size,
                    .filled = order.filled,
                    .reserve_bid_price = order.reserve_bid_price,
                    .action = order.action,
                    .uid = order.uid,
                    .timestamp = order.timestamp,
                });
            }
        }

        std.sort.heap(OrderDigest, digests.items, {}, lessThanOrderDigest);
        hashU64(&hasher, @as(u64, @intCast(digests.items.len)));
        for (digests.items) |digest| {
            hashI64(&hasher, digest.order_id);
            hashI64(&hasher, digest.price);
            hashI64(&hasher, digest.size);
            hashI64(&hasher, digest.filled);
            hashI64(&hasher, digest.reserve_bid_price);
            hashI8(&hasher, @intFromEnum(digest.action));
            hashI64(&hasher, digest.uid);
            hashI64(&hasher, digest.timestamp);
        }
        return hasher.final();
    }

    fn validateBucket(self: *const NaiveOrderBook, bucket: Bucket, side: common.OrderAction) void {
        var sum: i64 = 0;
        for (bucket.orders.items) |order| {
            std.debug.assert(order.action == side);
            std.debug.assert(order.filled >= 0 and order.filled <= order.size);
            std.debug.assert(order.price == bucket.price);
            sum += order.remaining();

            const locator = self.id_index.get(order.order_id);
            std.debug.assert(locator != null);
            std.debug.assert(locator.?.side == side);
            std.debug.assert(locator.?.price == bucket.price);
        }
        std.debug.assert(sum == bucket.total_volume);
    }

    fn sideBuckets(self: *NaiveOrderBook, side: common.OrderAction) *std.ArrayListUnmanaged(Bucket) {
        return switch (side) {
            .ask => &self.asks,
            .bid => &self.bids,
        };
    }

    fn sideBucketsConst(self: *const NaiveOrderBook, side: common.OrderAction) *const std.ArrayListUnmanaged(Bucket) {
        return switch (side) {
            .ask => &self.asks,
            .bid => &self.bids,
        };
    }

    fn clearRestingOrders(self: *NaiveOrderBook) void {
        for (self.asks.items) |*bucket| {
            bucket.deinit(self.allocator);
        }
        self.asks.clearRetainingCapacity();

        for (self.bids.items) |*bucket| {
            bucket.deinit(self.allocator);
        }
        self.bids.clearRetainingCapacity();

        self.id_index.clearRetainingCapacity();
    }

    fn findBucketIndex(self: *const NaiveOrderBook, side: common.OrderAction, price: i64) ?usize {
        const buckets = self.sideBucketsConst(side).items;
        for (buckets, 0..) |bucket, idx| {
            if (bucket.price == price) {
                return idx;
            }
            if (side == .ask and bucket.price > price) break;
            if (side == .bid and bucket.price < price) break;
        }
        return null;
    }

    fn insertBucketIndex(self: *const NaiveOrderBook, side: common.OrderAction, price: i64) usize {
        const buckets = self.sideBucketsConst(side).items;
        for (buckets, 0..) |bucket, idx| {
            if (side == .ask and price < bucket.price) return idx;
            if (side == .bid and price > bucket.price) return idx;
        }
        return buckets.len;
    }

    fn getOrCreateBucketIndex(
        self: *NaiveOrderBook,
        side: common.OrderAction,
        price: i64,
    ) std.mem.Allocator.Error!usize {
        if (self.findBucketIndex(side, price)) |existing| {
            return existing;
        }
        const idx = self.insertBucketIndex(side, price);
        try self.sideBuckets(side).insert(self.allocator, idx, Bucket.init(price));
        return idx;
    }

    fn locateOrder(self: *NaiveOrderBook, order_id: i64, uid: i64) ?OrderLocation {
        const locator = self.id_index.get(order_id) orelse return null;
        const bucket_index = self.findBucketIndex(locator.side, locator.price) orelse {
            _ = self.id_index.remove(order_id);
            return null;
        };
        const buckets = self.sideBuckets(locator.side);
        const bucket = &buckets.items[bucket_index];

        for (bucket.orders.items, 0..) |order, order_index| {
            if (order.order_id != order_id) continue;
            if (order.uid != uid) return null;
            return .{
                .side = locator.side,
                .price = locator.price,
                .bucket_index = bucket_index,
                .order_index = order_index,
            };
        }

        _ = self.id_index.remove(order_id);
        return null;
    }

    fn removeOrderAt(self: *NaiveOrderBook, location: OrderLocation) Order {
        const buckets = self.sideBuckets(location.side);
        var bucket = &buckets.items[location.bucket_index];
        const removed = bucket.orders.orderedRemove(location.order_index);
        bucket.total_volume -= removed.remaining();
        std.debug.assert(bucket.total_volume >= 0);

        _ = self.id_index.remove(removed.order_id);

        if (bucket.total_volume == 0) {
            bucket.deinit(self.allocator);
            _ = buckets.orderedRemove(location.bucket_index);
        }
        return removed;
    }

    fn placeRestingOrder(self: *NaiveOrderBook, order: Order) std.mem.Allocator.Error!bool {
        std.debug.assert(order.remaining() > 0);
        if (self.id_index.contains(order.order_id)) {
            return false;
        }

        try self.id_index.put(self.allocator, order.order_id, .{
            .side = order.action,
            .price = order.price,
        });
        errdefer _ = self.id_index.remove(order.order_id);

        const bucket_index = try self.getOrCreateBucketIndex(order.action, order.price);
        var bucket = &self.sideBuckets(order.action).items[bucket_index];
        try bucket.orders.append(self.allocator, order);
        bucket.total_volume += order.remaining();
        return true;
    }

    fn isPriceMatch(action: common.OrderAction, limit: i64, maker_price: i64) bool {
        return switch (action) {
            .bid => maker_price <= limit,
            .ask => maker_price >= limit,
        };
    }

    fn tryMatchInstantly(
        self: *NaiveOrderBook,
        active: *ActiveOrder,
        respect_price_limit: bool,
        event_chain: ?*events.EventChain,
    ) std.mem.Allocator.Error!void {
        const opposite_buckets = self.sideBuckets(active.action.opposite());
        var bucket_index: usize = 0;

        while (bucket_index < opposite_buckets.items.len and active.remaining() > 0) {
            var remove_bucket = false;
            {
                const bucket = &opposite_buckets.items[bucket_index];
                if (respect_price_limit and !isPriceMatch(active.action, active.price, bucket.price)) {
                    break;
                }

                var order_index: usize = 0;
                while (order_index < bucket.orders.items.len and active.remaining() > 0) {
                    var maker = &bucket.orders.items[order_index];
                    const maker_remaining = maker.remaining();
                    const taker_remaining = active.remaining();
                    const fill_size: i64 = @min(maker_remaining, taker_remaining);
                    std.debug.assert(fill_size > 0);

                    try appendTradeEvent(event_chain, active.*, maker.*, fill_size);

                    maker.filled += fill_size;
                    active.filled += fill_size;
                    bucket.total_volume -= fill_size;

                    if (maker.filled == maker.size) {
                        const removed = bucket.orders.orderedRemove(order_index);
                        _ = self.id_index.remove(removed.order_id);
                    } else {
                        order_index += 1;
                    }
                }

                if (bucket.total_volume == 0) {
                    remove_bucket = true;
                    bucket.deinit(self.allocator);
                }
            }

            if (remove_bucket) {
                _ = opposite_buckets.orderedRemove(bucket_index);
            } else {
                bucket_index += 1;
            }
        }
    }

    fn checkBudgetToFill(self: *const NaiveOrderBook, action: common.OrderAction, size: i64) ?i128 {
        if (size <= 0) return null;
        const opposite = self.sideBucketsConst(action.opposite());

        var remaining = size;
        var budget: i128 = 0;

        for (opposite.items) |bucket| {
            const available = bucket.total_volume;
            if (remaining > available) {
                remaining -= available;
                budget += @as(i128, available) * @as(i128, bucket.price);
            } else {
                return budget + @as(i128, remaining) * @as(i128, bucket.price);
            }
        }
        return null;
    }

    fn isBudgetLimitSatisfied(action: common.OrderAction, calculated: i128, limit: i64) bool {
        const limit128 = @as(i128, limit);
        if (calculated == limit128) return true;
        return switch (action) {
            .bid => calculated <= limit128,
            .ask => calculated > limit128,
        };
    }

    fn appendTradeEvent(
        event_chain: ?*events.EventChain,
        taker: ActiveOrder,
        maker: Order,
        fill_size: i64,
    ) std.mem.Allocator.Error!void {
        if (event_chain == null) return;
        if (fill_size <= 0) return;

        const bidder_hold_price = if (taker.action == .bid)
            taker.reserve_bid_price
        else
            maker.reserve_bid_price;
        _ = try event_chain.?.append(.{
            .event_type = .trade,
            .active_order_completed = taker.filled + fill_size == taker.size,
            .matched_order_id = maker.order_id,
            .matched_order_uid = maker.uid,
            .matched_order_completed = maker.filled + fill_size == maker.size,
            .price = maker.price,
            .size = fill_size,
            .bidder_hold_price = bidder_hold_price,
        });
    }

    fn appendRejectEvent(
        event_chain: ?*events.EventChain,
        active: ActiveOrder,
        reject_size: i64,
        order_type: common.OrderType,
        reject_price: i64,
    ) std.mem.Allocator.Error!void {
        if (event_chain == null) return;
        if (reject_size <= 0) return;

        _ = try event_chain.?.prepend(.{
            .event_type = .reject,
            .active_order_completed = true,
            .price = if (order_type == .fok_budget) reject_price else active.price,
            .size = reject_size,
            .bidder_hold_price = if (active.action == .bid) active.reserve_bid_price else 0,
        });
    }

    fn appendReduceEvent(
        event_chain: ?*events.EventChain,
        action: common.OrderAction,
        price: i64,
        reserve_bid_price: i64,
        reduced_size: i64,
    ) std.mem.Allocator.Error!void {
        if (event_chain == null) return;
        if (reduced_size <= 0) return;

        _ = try event_chain.?.append(.{
            .event_type = .reduce,
            .active_order_completed = true,
            .price = price,
            .size = reduced_size,
            .bidder_hold_price = if (action == .bid) reserve_bid_price else 0,
        });
    }

    fn newOrder(
        self: *NaiveOrderBook,
        command: *cmd.OrderCommand,
        event_chain: ?*events.EventChain,
    ) std.mem.Allocator.Error!void {
        const order_type = command.order_type orelse return;
        return switch (order_type) {
            .gtc => try self.newOrderPlaceGtc(command, event_chain),
            .ioc => try self.newOrderMatchIoc(command, event_chain),
            .fok_budget => try self.newOrderMatchFokBudget(command, event_chain),
            else => {},
        };
    }

    fn newOrderPlaceGtc(
        self: *NaiveOrderBook,
        command: *cmd.OrderCommand,
        event_chain: ?*events.EventChain,
    ) std.mem.Allocator.Error!void {
        var active = ActiveOrder.fromCommand(command) orelse return;
        try self.tryMatchInstantly(&active, true, event_chain);
        if (active.filled == active.size) return;

        _ = try self.placeRestingOrder(active.toOrder());
    }

    fn newOrderMatchIoc(
        self: *NaiveOrderBook,
        command: *cmd.OrderCommand,
        event_chain: ?*events.EventChain,
    ) std.mem.Allocator.Error!void {
        var active = ActiveOrder.fromCommand(command) orelse return;
        try self.tryMatchInstantly(&active, true, event_chain);
        const reject_size = active.remaining();
        if (reject_size > 0) {
            try appendRejectEvent(event_chain, active, reject_size, command.order_type orelse .ioc, command.price);
        }
    }

    fn newOrderMatchFokBudget(
        self: *NaiveOrderBook,
        command: *cmd.OrderCommand,
        event_chain: ?*events.EventChain,
    ) std.mem.Allocator.Error!void {
        var active = ActiveOrder.fromCommand(command) orelse return;
        const budget = self.checkBudgetToFill(active.action, active.size) orelse {
            try appendRejectEvent(event_chain, active, active.remaining(), .fok_budget, command.price);
            return;
        };
        if (!isBudgetLimitSatisfied(active.action, budget, command.price)) {
            try appendRejectEvent(event_chain, active, active.remaining(), .fok_budget, command.price);
            return;
        }
        try self.tryMatchInstantly(&active, false, event_chain);
        const reject_size = active.remaining();
        if (reject_size > 0) {
            try appendRejectEvent(event_chain, active, reject_size, .fok_budget, command.price);
        }
    }

    fn cancelOrder(
        self: *NaiveOrderBook,
        command: *cmd.OrderCommand,
        event_chain: ?*events.EventChain,
    ) std.mem.Allocator.Error!cmd.CommandResultCode {
        const location = self.locateOrder(command.order_id, command.uid) orelse {
            return .matching_unknown_order_id;
        };
        const removed = self.removeOrderAt(location);
        command.action = removed.action;
        try appendReduceEvent(event_chain, removed.action, removed.price, removed.reserve_bid_price, removed.remaining());
        return .success;
    }

    fn reduceOrder(
        self: *NaiveOrderBook,
        command: *cmd.OrderCommand,
        event_chain: ?*events.EventChain,
    ) std.mem.Allocator.Error!cmd.CommandResultCode {
        if (command.size <= 0) {
            return .matching_reduce_failed_wrong_size;
        }
        const location = self.locateOrder(command.order_id, command.uid) orelse {
            return .matching_unknown_order_id;
        };

        const buckets = self.sideBuckets(location.side);
        var bucket = &buckets.items[location.bucket_index];
        var order = &bucket.orders.items[location.order_index];
        const remaining = order.remaining();
        const reduce_by: i64 = @min(remaining, command.size);

        if (reduce_by == remaining) {
            const removed = self.removeOrderAt(location);
            command.action = removed.action;
            try appendReduceEvent(event_chain, removed.action, removed.price, removed.reserve_bid_price, reduce_by);
        } else {
            order.size -= reduce_by;
            bucket.total_volume -= reduce_by;
            std.debug.assert(bucket.total_volume >= 0);
            command.action = order.action;
            try appendReduceEvent(event_chain, order.action, order.price, order.reserve_bid_price, reduce_by);
        }
        return .success;
    }

    fn moveOrder(
        self: *NaiveOrderBook,
        command: *cmd.OrderCommand,
        event_chain: ?*events.EventChain,
    ) std.mem.Allocator.Error!cmd.CommandResultCode {
        const location = self.locateOrder(command.order_id, command.uid) orelse {
            return .matching_unknown_order_id;
        };

        const buckets = self.sideBuckets(location.side);
        const order = buckets.items[location.bucket_index].orders.items[location.order_index];
        command.action = order.action;

        if (self.symbol_spec.symbol_type == .currency_exchange_pair and
            order.action == .bid and
            command.price > order.reserve_bid_price)
        {
            return .matching_move_failed_price_over_risk_limit;
        }

        var moving_order = self.removeOrderAt(location);
        moving_order.price = command.price;

        var active = ActiveOrder.fromOrder(moving_order);
        try self.tryMatchInstantly(&active, true, event_chain);

        if (active.filled == active.size) {
            return .success;
        }

        const placed = try self.placeRestingOrder(active.toOrder());
        std.debug.assert(placed);
        return .success;
    }
};

fn addToMapValue(
    allocator: std.mem.Allocator,
    map: *std.AutoHashMapUnmanaged(i32, i64),
    key: i32,
    delta: i64,
) std.mem.Allocator.Error!void {
    const gop = try map.getOrPut(allocator, key);
    if (!gop.found_existing) {
        gop.value_ptr.* = 0;
    }
    gop.value_ptr.* += delta;
}

fn checkedMul(a: i64, b: i64) ?i64 {
    const pair = @mulWithOverflow(a, b);
    return if (pair[1] == 0) pair[0] else null;
}

fn checkedAdd(a: i64, b: i64) ?i64 {
    const pair = @addWithOverflow(a, b);
    return if (pair[1] == 0) pair[0] else null;
}

fn calcAmountBidTakerFee(size: i64, price: i64, spec: common.CoreSymbolSpecification) ?i64 {
    const step = checkedMul(price, spec.quote_scale_k) orelse return null;
    const notional = checkedMul(size, step) orelse return null;
    const fee = checkedMul(size, spec.taker_fee) orelse return null;
    return checkedAdd(notional, fee);
}

fn calcAmountAsk(size: i64, spec: common.CoreSymbolSpecification) ?i64 {
    return checkedMul(size, spec.base_scale_k);
}

fn lessThanOrderDigest(_: void, lhs: OrderDigest, rhs: OrderDigest) bool {
    if (lhs.order_id != rhs.order_id) return lhs.order_id < rhs.order_id;
    if (lhs.uid != rhs.uid) return lhs.uid < rhs.uid;
    if (lhs.price != rhs.price) return lhs.price < rhs.price;
    return lhs.timestamp < rhs.timestamp;
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

fn makeTestSpec() !common.CoreSymbolSpecification {
    return common.CoreSymbolSpecification.init(.{
        .symbol_id = 241,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 1_000_000,
        .quote_scale_k = 10_000,
        .taker_fee = 1900,
        .maker_fee = 700,
        .margin_buy = 0,
        .margin_sell = 0,
    });
}

fn process(book: *NaiveOrderBook, command: *cmd.OrderCommand) !cmd.CommandResultCode {
    return try book.processCommand(command);
}

test "gtc placement aggregates same-price buckets and keeps sorted L2" {
    const allocator = std.testing.allocator;
    var book = try NaiveOrderBook.init(allocator, try makeTestSpec());
    defer book.deinit();

    var ask_1 = cmd.OrderCommand.newOrder(.gtc, 1, 100, 101, 0, 7, .ask);
    var ask_2 = cmd.OrderCommand.newOrder(.gtc, 2, 101, 101, 0, 3, .ask);
    var ask_3 = cmd.OrderCommand.newOrder(.gtc, 3, 102, 103, 0, 11, .ask);
    var bid_1 = cmd.OrderCommand.newOrder(.gtc, 4, 103, 99, 120, 5, .bid);
    var bid_2 = cmd.OrderCommand.newOrder(.gtc, 5, 104, 98, 120, 2, .bid);

    try std.testing.expectEqual(cmd.CommandResultCode.success, try process(&book, &ask_1));
    try std.testing.expectEqual(cmd.CommandResultCode.success, try process(&book, &ask_2));
    try std.testing.expectEqual(cmd.CommandResultCode.success, try process(&book, &ask_3));
    try std.testing.expectEqual(cmd.CommandResultCode.success, try process(&book, &bid_1));
    try std.testing.expectEqual(cmd.CommandResultCode.success, try process(&book, &bid_2));

    var snapshot = try book.l2Snapshot(allocator, 16);
    defer snapshot.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), snapshot.asks.items.len);
    try std.testing.expectEqual(@as(i64, 101), snapshot.asks.items[0].price);
    try std.testing.expectEqual(@as(i64, 10), snapshot.asks.items[0].volume);
    try std.testing.expectEqual(@as(u32, 2), snapshot.asks.items[0].orders);
    try std.testing.expectEqual(@as(i64, 103), snapshot.asks.items[1].price);
    try std.testing.expectEqual(@as(i64, 11), snapshot.asks.items[1].volume);

    try std.testing.expectEqual(@as(usize, 2), snapshot.bids.items.len);
    try std.testing.expectEqual(@as(i64, 99), snapshot.bids.items[0].price);
    try std.testing.expectEqual(@as(i64, 98), snapshot.bids.items[1].price);

    book.validateInternalState();
}

test "ioc matches immediately and never rests" {
    const allocator = std.testing.allocator;
    var book = try NaiveOrderBook.init(allocator, try makeTestSpec());
    defer book.deinit();

    var ask = cmd.OrderCommand.newOrder(.gtc, 10, 200, 100, 0, 10, .ask);
    _ = try process(&book, &ask);

    var ioc_fill = cmd.OrderCommand.newOrder(.ioc, 11, 201, 100, 100, 7, .bid);
    try std.testing.expectEqual(cmd.CommandResultCode.success, try process(&book, &ioc_fill));
    try std.testing.expectEqual(@as(usize, 0), book.ordersNum(.bid));
    try std.testing.expectEqual(@as(i64, 3), book.totalOrdersVolume(.ask));

    var ioc_no_fill = cmd.OrderCommand.newOrder(.ioc, 12, 202, 99, 99, 5, .bid);
    try std.testing.expectEqual(cmd.CommandResultCode.success, try process(&book, &ioc_no_fill));
    try std.testing.expectEqual(@as(usize, 0), book.ordersNum(.bid));
    try std.testing.expectEqual(@as(i64, 3), book.totalOrdersVolume(.ask));

    book.validateInternalState();
}

test "cancel and reduce maintain bucket/index invariants" {
    const allocator = std.testing.allocator;
    var book = try NaiveOrderBook.init(allocator, try makeTestSpec());
    defer book.deinit();

    var bid = cmd.OrderCommand.newOrder(.gtc, 20, 300, 100, 120, 10, .bid);
    _ = try process(&book, &bid);

    var reduce_bad = cmd.OrderCommand.reduce(20, 300, 0);
    try std.testing.expectEqual(
        cmd.CommandResultCode.matching_reduce_failed_wrong_size,
        try process(&book, &reduce_bad),
    );

    var cancel_wrong_uid = cmd.OrderCommand.cancel(20, 301);
    try std.testing.expectEqual(
        cmd.CommandResultCode.matching_unknown_order_id,
        try process(&book, &cancel_wrong_uid),
    );

    var reduce_partial = cmd.OrderCommand.reduce(20, 300, 3);
    try std.testing.expectEqual(cmd.CommandResultCode.success, try process(&book, &reduce_partial));
    try std.testing.expectEqual(@as(?common.OrderAction, .bid), reduce_partial.action);
    try std.testing.expectEqual(@as(i64, 7), book.totalOrdersVolume(.bid));

    var reduce_full = cmd.OrderCommand.reduce(20, 300, 100);
    try std.testing.expectEqual(cmd.CommandResultCode.success, try process(&book, &reduce_full));
    try std.testing.expectEqual(@as(usize, 0), book.ordersNum(.bid));

    book.validateInternalState();
}

test "move enforces reserve risk limit and can rematch" {
    const allocator = std.testing.allocator;
    var book = try NaiveOrderBook.init(allocator, try makeTestSpec());
    defer book.deinit();

    var ask = cmd.OrderCommand.newOrder(.gtc, 30, 400, 100, 0, 5, .ask);
    var bid = cmd.OrderCommand.newOrder(.gtc, 31, 401, 90, 110, 10, .bid);
    _ = try process(&book, &ask);
    _ = try process(&book, &bid);

    var move_over_limit = cmd.OrderCommand.update(31, 401, 111);
    try std.testing.expectEqual(
        cmd.CommandResultCode.matching_move_failed_price_over_risk_limit,
        try process(&book, &move_over_limit),
    );
    try std.testing.expectEqual(@as(i64, 5), book.totalOrdersVolume(.ask));
    try std.testing.expectEqual(@as(i64, 10), book.totalOrdersVolume(.bid));

    var move_match = cmd.OrderCommand.update(31, 401, 100);
    try std.testing.expectEqual(cmd.CommandResultCode.success, try process(&book, &move_match));
    try std.testing.expectEqual(@as(usize, 0), book.ordersNum(.ask));
    try std.testing.expectEqual(@as(i64, 5), book.totalOrdersVolume(.bid));

    var snapshot = try book.l2Snapshot(allocator, 8);
    defer snapshot.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), snapshot.bids.items.len);
    try std.testing.expectEqual(@as(i64, 100), snapshot.bids.items[0].price);
    try std.testing.expectEqual(@as(i64, 5), snapshot.bids.items[0].volume);

    book.validateInternalState();
}

test "accumulateOrderHolds reports resting ask/bid currency holds" {
    const allocator = std.testing.allocator;
    var book = try NaiveOrderBook.init(allocator, try makeTestSpec());
    defer book.deinit();

    var ask = cmd.OrderCommand.newOrder(.gtc, 50, 610, 101, 0, 7, .ask);
    var bid = cmd.OrderCommand.newOrder(.gtc, 51, 611, 99, 120, 5, .bid);
    _ = try process(&book, &ask);
    _ = try process(&book, &bid);

    var holds: std.AutoHashMapUnmanaged(i32, i64) = .empty;
    defer holds.deinit(allocator);
    try book.accumulateOrderHolds(allocator, &holds);

    try std.testing.expectEqual(@as(i64, 7_000_000), holds.get(11) orelse 0);
    try std.testing.expectEqual(@as(i64, 6_009_500), holds.get(15) orelse 0);
}

test "duplicate resting order id is ignored" {
    const allocator = std.testing.allocator;
    var book = try NaiveOrderBook.init(allocator, try makeTestSpec());
    defer book.deinit();

    var ask = cmd.OrderCommand.newOrder(.gtc, 40, 500, 100, 0, 5, .ask);
    _ = try process(&book, &ask);

    var first = cmd.OrderCommand.newOrder(.gtc, 41, 501, 100, 150, 10, .bid);
    _ = try process(&book, &first);
    try std.testing.expectEqual(@as(i64, 5), book.totalOrdersVolume(.bid));

    var duplicate = cmd.OrderCommand.newOrder(.gtc, 41, 502, 80, 120, 30, .bid);
    _ = try process(&book, &duplicate);
    try std.testing.expectEqual(@as(usize, 1), book.ordersNum(.bid));
    try std.testing.expectEqual(@as(i64, 5), book.totalOrdersVolume(.bid));

    book.validateInternalState();
}

test "naive book state hash is deterministic and mutation sensitive" {
    const allocator = std.testing.allocator;
    const spec = try makeTestSpec();

    var left = try NaiveOrderBook.init(allocator, spec);
    defer left.deinit();
    var right = try NaiveOrderBook.init(allocator, spec);
    defer right.deinit();

    var left_a = cmd.OrderCommand.newOrder(.gtc, 7001, 801, 101, 0, 4, .ask);
    var left_b = cmd.OrderCommand.newOrder(.gtc, 7002, 802, 99, 120, 5, .bid);
    _ = try left.processCommand(&left_a);
    _ = try left.processCommand(&left_b);

    var right_b = cmd.OrderCommand.newOrder(.gtc, 7002, 802, 99, 120, 5, .bid);
    var right_a = cmd.OrderCommand.newOrder(.gtc, 7001, 801, 101, 0, 4, .ask);
    _ = try right.processCommand(&right_b);
    _ = try right.processCommand(&right_a);

    try std.testing.expectEqual(try left.stateHash(), try right.stateHash());

    var reduce = cmd.OrderCommand.reduce(7002, 802, 1);
    _ = try right.processCommand(&reduce);
    try std.testing.expect((try left.stateHash()) != (try right.stateHash()));
}
