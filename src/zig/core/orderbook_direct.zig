const std = @import("std");
const common = @import("common.zig");
const cmd = @import("cmd.zig");
const naive = @import("orderbook_naive.zig");
const events = @import("events.zig");

pub const L2Level = naive.L2Level;
pub const L2MarketData = naive.L2MarketData;
pub const UserOrderView = naive.UserOrderView;
pub const HoldAggregationError = std.mem.Allocator.Error || error{
    ArithmeticOverflow,
};
pub const RestoreOrdersError = std.mem.Allocator.Error || error{
    InvalidOrderSymbol,
    InvalidOrderSize,
    InvalidOrderFill,
    DuplicateOrderId,
};

fn invariantViolation(comptime message: []const u8) noreturn {
    @panic(message);
}

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

const ActiveOrder = struct {
    order_id: i64,
    price: i64,
    size: i64,
    filled: i64,
    reserve_bid_price: i64,
    action: common.OrderAction,
    uid: i64,
    timestamp: i64,

    fn fromCommand(command: *const cmd.OrderCommand) ?ActiveOrder {
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

    fn remaining(self: ActiveOrder) i64 {
        return self.size - self.filled;
    }
};

const Bucket = struct {
    price: i64,
    volume: i64 = 0,
    num_orders: usize = 0,
    tail: ?*DirectOrder = null,
    pool_next: ?*Bucket = null,
};

const DirectOrder = struct {
    order_id: i64,
    price: i64,
    size: i64,
    filled: i64,
    reserve_bid_price: i64,
    action: common.OrderAction,
    uid: i64,
    timestamp: i64,
    parent: *Bucket,
    next: ?*DirectOrder,
    prev: ?*DirectOrder,
    pool_next: ?*DirectOrder,

    fn remaining(self: DirectOrder) i64 {
        return self.size - self.filled;
    }
};

const OrderPool = struct {
    allocator: std.mem.Allocator,
    head: ?*DirectOrder = null,
    allocations: usize = 0,
    reuses: usize = 0,
    free_count: usize = 0,

    fn init(allocator: std.mem.Allocator) OrderPool {
        return .{ .allocator = allocator };
    }

    fn acquire(self: *OrderPool) std.mem.Allocator.Error!*DirectOrder {
        if (self.head) |item| {
            self.head = item.pool_next;
            item.pool_next = null;
            self.reuses += 1;
            self.free_count -= 1;
            return item;
        }
        const item = try self.allocator.create(DirectOrder);
        self.allocations += 1;
        return item;
    }

    fn release(self: *OrderPool, item: *DirectOrder) void {
        item.pool_next = self.head;
        self.head = item;
        self.free_count += 1;
    }

    fn deinit(self: *OrderPool) void {
        var node = self.head;
        while (node) |item| {
            node = item.pool_next;
            self.allocator.destroy(item);
        }
        self.* = undefined;
    }
};

const BucketPool = struct {
    allocator: std.mem.Allocator,
    head: ?*Bucket = null,
    allocations: usize = 0,
    reuses: usize = 0,
    free_count: usize = 0,

    fn init(allocator: std.mem.Allocator) BucketPool {
        return .{ .allocator = allocator };
    }

    fn acquire(self: *BucketPool) std.mem.Allocator.Error!*Bucket {
        if (self.head) |item| {
            self.head = item.pool_next;
            item.pool_next = null;
            self.reuses += 1;
            self.free_count -= 1;
            return item;
        }
        const item = try self.allocator.create(Bucket);
        self.allocations += 1;
        return item;
    }

    fn release(self: *BucketPool, item: *Bucket) void {
        item.pool_next = self.head;
        self.head = item;
        self.free_count += 1;
    }

    fn deinit(self: *BucketPool) void {
        var node = self.head;
        while (node) |item| {
            node = item.pool_next;
            self.allocator.destroy(item);
        }
        self.* = undefined;
    }
};

pub const PoolStats = struct {
    order_allocations: usize,
    order_reuses: usize,
    order_free_count: usize,
    bucket_allocations: usize,
    bucket_reuses: usize,
    bucket_free_count: usize,
};

pub const DirectOrderBook = struct {
    allocator: std.mem.Allocator,
    symbol_spec: common.CoreSymbolSpecification,

    ask_price_buckets: std.ArrayListUnmanaged(*Bucket) = .empty, // ascending
    bid_price_buckets: std.ArrayListUnmanaged(*Bucket) = .empty, // descending

    ask_price_index: std.AutoHashMapUnmanaged(i64, *Bucket) = .empty,
    bid_price_index: std.AutoHashMapUnmanaged(i64, *Bucket) = .empty,
    order_id_index: std.AutoHashMapUnmanaged(i64, *DirectOrder) = .empty,

    best_ask_order: ?*DirectOrder = null,
    best_bid_order: ?*DirectOrder = null,
    order_pool: OrderPool,
    bucket_pool: BucketPool,

    pub fn init(
        allocator: std.mem.Allocator,
        symbol_spec: common.CoreSymbolSpecification,
    ) common.CoreSymbolSpecification.ValidationError!DirectOrderBook {
        try symbol_spec.validate();
        return .{
            .allocator = allocator,
            .symbol_spec = symbol_spec,
            .order_pool = OrderPool.init(allocator),
            .bucket_pool = BucketPool.init(allocator),
        };
    }

    pub fn deinit(self: *DirectOrderBook) void {
        var order_it = self.order_id_index.valueIterator();
        while (order_it.next()) |order_ptr| {
            self.allocator.destroy(order_ptr.*);
        }
        self.order_id_index.deinit(self.allocator);

        for (self.ask_price_buckets.items) |bucket| {
            self.allocator.destroy(bucket);
        }
        self.ask_price_buckets.deinit(self.allocator);

        for (self.bid_price_buckets.items) |bucket| {
            self.allocator.destroy(bucket);
        }
        self.bid_price_buckets.deinit(self.allocator);

        self.ask_price_index.deinit(self.allocator);
        self.bid_price_index.deinit(self.allocator);
        self.order_pool.deinit();
        self.bucket_pool.deinit();
        self.* = undefined;
    }

    pub fn poolStats(self: *const DirectOrderBook) PoolStats {
        return .{
            .order_allocations = self.order_pool.allocations,
            .order_reuses = self.order_pool.reuses,
            .order_free_count = self.order_pool.free_count,
            .bucket_allocations = self.bucket_pool.allocations,
            .bucket_reuses = self.bucket_pool.reuses,
            .bucket_free_count = self.bucket_pool.free_count,
        };
    }

    pub fn processCommand(
        self: *DirectOrderBook,
        command: *cmd.OrderCommand,
    ) std.mem.Allocator.Error!cmd.CommandResultCode {
        return self.processCommandWithEvents(command, null);
    }

    pub fn processCommandWithEvents(
        self: *DirectOrderBook,
        command: *cmd.OrderCommand,
        event_chain: ?*events.EventChain,
    ) std.mem.Allocator.Error!cmd.CommandResultCode {
        return switch (command.command) {
            .move_order => try self.moveOrder(command, event_chain),
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
        self: *const DirectOrderBook,
        allocator: std.mem.Allocator,
        max_levels: usize,
    ) std.mem.Allocator.Error!L2MarketData {
        var data: L2MarketData = .{};
        errdefer data.deinit(allocator);

        const ask_levels = @min(max_levels, self.ask_price_buckets.items.len);
        const bid_levels = @min(max_levels, self.bid_price_buckets.items.len);

        try data.asks.ensureTotalCapacityPrecise(allocator, ask_levels);
        try data.bids.ensureTotalCapacityPrecise(allocator, bid_levels);

        for (self.ask_price_buckets.items[0..ask_levels]) |bucket| {
            data.asks.appendAssumeCapacity(.{
                .price = bucket.price,
                .volume = bucket.volume,
                .orders = @as(u32, @intCast(bucket.num_orders)),
            });
        }

        for (self.bid_price_buckets.items[0..bid_levels]) |bucket| {
            data.bids.appendAssumeCapacity(.{
                .price = bucket.price,
                .volume = bucket.volume,
                .orders = @as(u32, @intCast(bucket.num_orders)),
            });
        }

        return data;
    }

    pub fn validateInternalState(self: *const DirectOrderBook) void {
        std.debug.assert(self.ask_price_index.count() == self.ask_price_buckets.items.len);
        std.debug.assert(self.bid_price_index.count() == self.bid_price_buckets.items.len);

        if (self.ask_price_buckets.items.len == 0) {
            std.debug.assert(self.best_ask_order == null);
        } else {
            const best = self.best_ask_order;
            std.debug.assert(best != null);
            std.debug.assert(best.?.price == self.ask_price_buckets.items[0].price);
        }

        if (self.bid_price_buckets.items.len == 0) {
            std.debug.assert(self.best_bid_order == null);
        } else {
            const best = self.best_bid_order;
            std.debug.assert(best != null);
            std.debug.assert(best.?.price == self.bid_price_buckets.items[0].price);
        }

        var chain_orders: usize = 0;
        chain_orders += self.validateSideChain(.ask);
        chain_orders += self.validateSideChain(.bid);
        std.debug.assert(chain_orders == self.order_id_index.count());

        self.validateSideBuckets(.ask);
        self.validateSideBuckets(.bid);
    }

    pub fn ordersNum(self: *const DirectOrderBook, side: common.OrderAction) usize {
        const buckets = self.sideBucketsConst(side).items;
        var count: usize = 0;
        for (buckets) |bucket| {
            count += bucket.num_orders;
        }
        return count;
    }

    pub fn totalOrdersVolume(self: *const DirectOrderBook, side: common.OrderAction) i64 {
        const buckets = self.sideBucketsConst(side).items;
        var total: i64 = 0;
        for (buckets) |bucket| {
            total += bucket.volume;
        }
        return total;
    }

    pub fn appendUserOrders(
        self: *const DirectOrderBook,
        allocator: std.mem.Allocator,
        uid: i64,
        out: *std.ArrayListUnmanaged(UserOrderView),
    ) std.mem.Allocator.Error!void {
        for (self.ask_price_buckets.items) |bucket| {
            var node = bucket.tail;
            while (node) |order| {
                if (order.parent != bucket) break;
                if (order.uid == uid) {
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
                node = order.next;
            }
        }
        for (self.bid_price_buckets.items) |bucket| {
            var node = bucket.tail;
            while (node) |order| {
                if (order.parent != bucket) break;
                if (order.uid == uid) {
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
                node = order.next;
            }
        }
    }

    pub fn appendAllOrders(
        self: *const DirectOrderBook,
        allocator: std.mem.Allocator,
        out: *std.ArrayListUnmanaged(UserOrderView),
    ) std.mem.Allocator.Error!void {
        for (self.ask_price_buckets.items) |bucket| {
            const first = oldestInBucket(bucket) orelse continue;
            var node: ?*DirectOrder = first;
            while (node) |order| {
                if (order.parent != bucket) break;
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
                node = order.prev;
            }
        }

        for (self.bid_price_buckets.items) |bucket| {
            const first = oldestInBucket(bucket) orelse continue;
            var node: ?*DirectOrder = first;
            while (node) |order| {
                if (order.parent != bucket) break;
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
                node = order.prev;
            }
        }
    }

    pub fn restoreOrders(
        self: *DirectOrderBook,
        symbol: i32,
        orders: []const UserOrderView,
    ) RestoreOrdersError!void {
        if (symbol != self.symbol_spec.symbol_id) return error.InvalidOrderSymbol;
        self.clearRestingOrders();

        for (orders) |order| {
            if (order.symbol != symbol) return error.InvalidOrderSymbol;
            if (order.size <= 0) return error.InvalidOrderSize;
            if (order.filled < 0 or order.filled >= order.size) return error.InvalidOrderFill;
            if (self.order_id_index.contains(order.order_id)) return error.DuplicateOrderId;

            const node = try self.order_pool.acquire();
            errdefer self.order_pool.release(node);

            node.* = .{
                .order_id = order.order_id,
                .price = order.price,
                .size = order.size,
                .filled = order.filled,
                .reserve_bid_price = order.reserve_bid_price,
                .action = order.action,
                .uid = order.uid,
                .timestamp = order.timestamp,
                .parent = undefined,
                .next = null,
                .prev = null,
                .pool_next = null,
            };

            try self.order_id_index.put(self.allocator, node.order_id, node);
            errdefer _ = self.order_id_index.remove(node.order_id);
            try self.insertOrder(node);
        }

        self.validateInternalState();
    }

    pub fn accumulateOrderHolds(
        self: *const DirectOrderBook,
        allocator: std.mem.Allocator,
        out: *std.AutoHashMapUnmanaged(i32, i64),
    ) HoldAggregationError!void {
        for (self.ask_price_buckets.items) |bucket| {
            var node = bucket.tail;
            while (node) |order| {
                if (order.parent != bucket) break;
                const hold = calcAmountAsk(order.remaining(), self.symbol_spec) orelse
                    return error.ArithmeticOverflow;
                try addToMapValue(allocator, out, self.symbol_spec.base_currency, hold);
                node = order.next;
            }
        }

        for (self.bid_price_buckets.items) |bucket| {
            var node = bucket.tail;
            while (node) |order| {
                if (order.parent != bucket) break;
                const hold = calcAmountBidTakerFee(
                    order.remaining(),
                    order.reserve_bid_price,
                    self.symbol_spec,
                ) orelse return error.ArithmeticOverflow;
                try addToMapValue(allocator, out, self.symbol_spec.quote_currency, hold);
                node = order.next;
            }
        }
    }

    pub fn stateHash(self: *const DirectOrderBook) std.mem.Allocator.Error!u64 {
        var hasher = std.hash.Wyhash.init(0);
        hashCoreSymbolSpecification(&hasher, self.symbol_spec);

        var digests: std.ArrayListUnmanaged(OrderDigest) = .empty;
        defer digests.deinit(self.allocator);
        try digests.ensureTotalCapacityPrecise(self.allocator, self.order_id_index.count());

        var order_it = self.order_id_index.valueIterator();
        while (order_it.next()) |order_ptr| {
            const order = order_ptr.*;
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

    fn validateSideChain(self: *const DirectOrderBook, side: common.OrderAction) usize {
        var count: usize = 0;
        var node = self.sideBestConst(side).*;
        var last: ?*DirectOrder = null;
        var last_price: ?i64 = null;

        while (node) |order| {
            count += 1;
            std.debug.assert(order.action == side);
            std.debug.assert(order.filled >= 0 and order.filled <= order.size);

            if (last) |l| {
                std.debug.assert(order.next == l);
            } else {
                std.debug.assert(order.next == null);
            }

            if (last_price) |price| {
                if (side == .ask) {
                    std.debug.assert(order.price >= price);
                } else {
                    std.debug.assert(order.price <= price);
                }
            }
            last_price = order.price;

            const indexed = self.order_id_index.get(order.order_id);
            std.debug.assert(indexed != null);
            std.debug.assert(indexed.? == order);

            const mapped_bucket = self.sideBucketIndexConst(side).get(order.price);
            std.debug.assert(mapped_bucket != null);
            std.debug.assert(mapped_bucket.? == order.parent);

            if (order.prev) |p| {
                std.debug.assert(p.next == order);
            }

            last = order;
            node = order.prev;
        }

        if (last) |tailmost| {
            std.debug.assert(tailmost.prev == null);
        }

        return count;
    }

    fn validateSideBuckets(self: *const DirectOrderBook, side: common.OrderAction) void {
        const buckets = self.sideBucketsConst(side).items;
        var previous_price: ?i64 = null;

        for (buckets) |bucket| {
            const mapped = self.sideBucketIndexConst(side).get(bucket.price);
            std.debug.assert(mapped != null);
            std.debug.assert(mapped.? == bucket);

            if (previous_price) |prev| {
                if (side == .ask) {
                    std.debug.assert(bucket.price > prev);
                } else {
                    std.debug.assert(bucket.price < prev);
                }
            }
            previous_price = bucket.price;

            std.debug.assert(bucket.num_orders > 0);
            std.debug.assert(bucket.volume > 0);

            var count: usize = 0;
            var volume: i64 = 0;
            var node = bucket.tail;
            while (node) |order| {
                if (order.parent != bucket) break;
                count += 1;
                volume += order.remaining();
                node = order.next;
            }

            std.debug.assert(count == bucket.num_orders);
            std.debug.assert(volume == bucket.volume);
        }
    }

    fn sideBuckets(self: *DirectOrderBook, side: common.OrderAction) *std.ArrayListUnmanaged(*Bucket) {
        return switch (side) {
            .ask => &self.ask_price_buckets,
            .bid => &self.bid_price_buckets,
        };
    }

    fn sideBucketsConst(self: *const DirectOrderBook, side: common.OrderAction) *const std.ArrayListUnmanaged(*Bucket) {
        return switch (side) {
            .ask => &self.ask_price_buckets,
            .bid => &self.bid_price_buckets,
        };
    }

    fn sideBucketIndex(self: *DirectOrderBook, side: common.OrderAction) *std.AutoHashMapUnmanaged(i64, *Bucket) {
        return switch (side) {
            .ask => &self.ask_price_index,
            .bid => &self.bid_price_index,
        };
    }

    fn sideBucketIndexConst(self: *const DirectOrderBook, side: common.OrderAction) *const std.AutoHashMapUnmanaged(i64, *Bucket) {
        return switch (side) {
            .ask => &self.ask_price_index,
            .bid => &self.bid_price_index,
        };
    }

    fn sideBest(self: *DirectOrderBook, side: common.OrderAction) *?*DirectOrder {
        return switch (side) {
            .ask => &self.best_ask_order,
            .bid => &self.best_bid_order,
        };
    }

    fn sideBestConst(self: *const DirectOrderBook, side: common.OrderAction) *const ?*DirectOrder {
        return switch (side) {
            .ask => &self.best_ask_order,
            .bid => &self.best_bid_order,
        };
    }

    fn clearRestingOrders(self: *DirectOrderBook) void {
        var order_it = self.order_id_index.valueIterator();
        while (order_it.next()) |order_ptr| {
            self.order_pool.release(order_ptr.*);
        }
        self.order_id_index.clearRetainingCapacity();

        for (self.ask_price_buckets.items) |bucket| {
            self.bucket_pool.release(bucket);
        }
        self.ask_price_buckets.clearRetainingCapacity();
        self.ask_price_index.clearRetainingCapacity();

        for (self.bid_price_buckets.items) |bucket| {
            self.bucket_pool.release(bucket);
        }
        self.bid_price_buckets.clearRetainingCapacity();
        self.bid_price_index.clearRetainingCapacity();

        self.best_ask_order = null;
        self.best_bid_order = null;
    }

    fn findBucketListIndex(self: *const DirectOrderBook, side: common.OrderAction, price: i64) ?usize {
        for (self.sideBucketsConst(side).items, 0..) |bucket, idx| {
            if (bucket.price == price) return idx;
            if (side == .ask and bucket.price > price) break;
            if (side == .bid and bucket.price < price) break;
        }
        return null;
    }

    fn insertBucketListIndex(self: *const DirectOrderBook, side: common.OrderAction, price: i64) usize {
        for (self.sideBucketsConst(side).items, 0..) |bucket, idx| {
            if (side == .ask and price < bucket.price) return idx;
            if (side == .bid and price > bucket.price) return idx;
        }
        return self.sideBucketsConst(side).items.len;
    }

    fn findBetterBucket(self: *const DirectOrderBook, side: common.OrderAction, price: i64) ?*Bucket {
        var best: ?*Bucket = null;
        for (self.sideBucketsConst(side).items) |bucket| {
            if (side == .ask) {
                if (bucket.price < price) {
                    best = bucket;
                } else break;
            } else {
                if (bucket.price > price) {
                    best = bucket;
                } else break;
            }
        }
        return best;
    }

    fn createBucket(self: *DirectOrderBook, side: common.OrderAction, price: i64) std.mem.Allocator.Error!*Bucket {
        const bucket = try self.bucket_pool.acquire();
        bucket.* = .{
            .price = price,
        };
        errdefer self.bucket_pool.release(bucket);

        try self.sideBucketIndex(side).put(self.allocator, price, bucket);
        errdefer _ = self.sideBucketIndex(side).remove(price);

        const idx = self.insertBucketListIndex(side, price);
        try self.sideBuckets(side).insert(self.allocator, idx, bucket);

        return bucket;
    }

    fn destroyBucket(self: *DirectOrderBook, side: common.OrderAction, bucket: *Bucket) void {
        const removed = self.sideBucketIndex(side).fetchRemove(bucket.price) orelse
            invariantViolation("direct orderbook bucket index missing during destroy");
        std.debug.assert(removed.value == bucket);

        const idx = self.findBucketListIndex(side, bucket.price) orelse
            invariantViolation("direct orderbook bucket list index missing during destroy");
        std.debug.assert(self.sideBuckets(side).items[idx] == bucket);
        _ = self.sideBuckets(side).orderedRemove(idx);

        self.bucket_pool.release(bucket);
    }

    fn removeOrderFromChain(self: *DirectOrderBook, order: *DirectOrder) void {
        const side = order.action;
        const bucket = order.parent;
        bucket.volume -= order.remaining();
        bucket.num_orders -= 1;

        const bucket_removed = if (bucket.tail == order) blk: {
            if (order.next == null or order.next.?.parent != bucket) {
                break :blk true;
            }
            bucket.tail = order.next;
            break :blk false;
        } else false;

        if (order.next) |next_order| {
            next_order.prev = order.prev;
        }
        if (order.prev) |prev_order| {
            prev_order.next = order.next;
        }

        if (self.sideBest(side).* == order) {
            self.sideBest(side).* = order.prev;
        }

        order.next = null;
        order.prev = null;

        if (bucket_removed) {
            self.destroyBucket(side, bucket);
        }
    }

    fn removeOrderAndFree(self: *DirectOrderBook, order: *DirectOrder) void {
        const removed = self.order_id_index.fetchRemove(order.order_id) orelse
            invariantViolation("direct orderbook id index missing during remove");
        std.debug.assert(removed.value == order);
        self.removeOrderFromChain(order);
        self.order_pool.release(order);
    }

    fn insertOrder(self: *DirectOrderBook, order: *DirectOrder) std.mem.Allocator.Error!void {
        const side = order.action;
        const existing_bucket = self.sideBucketIndex(side).get(order.price);
        if (existing_bucket) |bucket| {
            const old_tail = bucket.tail orelse
                invariantViolation("direct orderbook existing bucket missing tail");
            const prev_order = old_tail.prev;

            bucket.volume += order.remaining();
            bucket.num_orders += 1;
            bucket.tail = order;

            old_tail.prev = order;
            if (prev_order) |prev| {
                prev.next = order;
            }
            order.next = old_tail;
            order.prev = prev_order;
            order.parent = bucket;
            return;
        }

        const bucket = try self.createBucket(side, order.price);
        bucket.volume = order.remaining();
        bucket.num_orders = 1;
        bucket.tail = order;
        order.parent = bucket;

        if (self.findBetterBucket(side, order.price)) |better_bucket| {
            const lower_tail = better_bucket.tail orelse
                invariantViolation("direct orderbook better bucket missing tail");
            const prev_order = lower_tail.prev;
            lower_tail.prev = order;
            if (prev_order) |prev| {
                prev.next = order;
            }
            order.next = lower_tail;
            order.prev = prev_order;
        } else {
            const old_best = self.sideBest(side).*;
            if (old_best) |best| {
                best.next = order;
            }
            self.sideBest(side).* = order;
            order.next = null;
            order.prev = old_best;
        }
    }

    fn isPriceMatch(action: common.OrderAction, maker_price: i64, limit_price: i64) bool {
        return switch (action) {
            .bid => maker_price <= limit_price,
            .ask => maker_price >= limit_price,
        };
    }

    fn tryMatchInstantly(
        self: *DirectOrderBook,
        taker: *ActiveOrder,
        limit_price: i64,
        event_chain: ?*events.EventChain,
    ) std.mem.Allocator.Error!void {
        var maker = self.sideBest(taker.action.opposite()).*;
        if (maker == null) return;
        if (!isPriceMatch(taker.action, maker.?.price, limit_price)) return;

        var remaining = taker.remaining();
        if (remaining <= 0) return;

        while (maker) |maker_order| {
            if (remaining <= 0) break;
            if (!isPriceMatch(taker.action, maker_order.price, limit_price)) break;

            const next_maker = maker_order.prev;
            const fill_size: i64 = @min(remaining, maker_order.remaining());
            std.debug.assert(fill_size > 0);

            try appendTradeEvent(event_chain, taker.*, maker_order.*, fill_size);

            maker_order.filled += fill_size;
            maker_order.parent.volume -= fill_size;
            remaining -= fill_size;

            if (maker_order.filled == maker_order.size) {
                self.removeOrderAndFree(maker_order);
                maker = next_maker;
            } else {
                maker = maker_order;
                break;
            }
        }

        if (maker) |maker_order| {
            maker_order.next = null;
        }

        taker.filled = taker.size - remaining;
    }

    fn checkBudgetToFill(self: *const DirectOrderBook, action: common.OrderAction, size: i64) ?i128 {
        if (size <= 0) return null;

        const opposite = self.sideBucketsConst(action.opposite()).items;
        var remaining = size;
        var budget: i128 = 0;

        for (opposite) |bucket| {
            if (remaining > bucket.volume) {
                remaining -= bucket.volume;
                budget += @as(i128, bucket.volume) * @as(i128, bucket.price);
            } else {
                return budget + @as(i128, remaining) * @as(i128, bucket.price);
            }
        }

        return null;
    }

    fn isBudgetLimitSatisfied(action: common.OrderAction, calculated: i128, limit: i64) bool {
        const limit128 = @as(i128, limit);
        if (calculated == limit128) return true;
        return (action == .bid) != (calculated > limit128);
    }

    fn appendTradeEvent(
        event_chain: ?*events.EventChain,
        taker: ActiveOrder,
        maker: DirectOrder,
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
        taker: ActiveOrder,
        reject_size: i64,
        order_type: common.OrderType,
        reject_price: i64,
    ) std.mem.Allocator.Error!void {
        if (event_chain == null) return;
        if (reject_size <= 0) return;

        _ = try event_chain.?.prepend(.{
            .event_type = .reject,
            .active_order_completed = true,
            .price = if (order_type == .fok_budget) reject_price else taker.price,
            .size = reject_size,
            .bidder_hold_price = if (taker.action == .bid) taker.reserve_bid_price else 0,
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
        self: *DirectOrderBook,
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
        self: *DirectOrderBook,
        command: *cmd.OrderCommand,
        event_chain: ?*events.EventChain,
    ) std.mem.Allocator.Error!void {
        var taker = ActiveOrder.fromCommand(command) orelse return;
        try self.tryMatchInstantly(&taker, taker.price, event_chain);
        if (taker.filled == taker.size) return;

        if (self.order_id_index.contains(command.order_id)) {
            return;
        }

        const order = try self.order_pool.acquire();
        errdefer self.order_pool.release(order);
        order.* = .{
            .order_id = taker.order_id,
            .price = taker.price,
            .size = taker.size,
            .filled = taker.filled,
            .reserve_bid_price = taker.reserve_bid_price,
            .action = taker.action,
            .uid = taker.uid,
            .timestamp = taker.timestamp,
            .parent = undefined,
            .next = null,
            .prev = null,
            .pool_next = null,
        };

        try self.order_id_index.put(self.allocator, order.order_id, order);
        errdefer _ = self.order_id_index.remove(order.order_id);

        try self.insertOrder(order);
    }

    fn newOrderMatchIoc(
        self: *DirectOrderBook,
        command: *cmd.OrderCommand,
        event_chain: ?*events.EventChain,
    ) std.mem.Allocator.Error!void {
        var taker = ActiveOrder.fromCommand(command) orelse return;
        try self.tryMatchInstantly(&taker, taker.price, event_chain);
        const reject_size = taker.remaining();
        if (reject_size > 0) {
            try appendRejectEvent(event_chain, taker, reject_size, command.order_type orelse .ioc, command.price);
        }
    }

    fn newOrderMatchFokBudget(
        self: *DirectOrderBook,
        command: *cmd.OrderCommand,
        event_chain: ?*events.EventChain,
    ) std.mem.Allocator.Error!void {
        var taker = ActiveOrder.fromCommand(command) orelse return;
        const budget = self.checkBudgetToFill(taker.action, taker.size) orelse {
            try appendRejectEvent(event_chain, taker, taker.remaining(), .fok_budget, command.price);
            return;
        };
        if (!isBudgetLimitSatisfied(taker.action, budget, command.price)) {
            try appendRejectEvent(event_chain, taker, taker.remaining(), .fok_budget, command.price);
            return;
        }
        const limit_price = if (taker.action == .ask) 0 else taker.price;
        try self.tryMatchInstantly(&taker, limit_price, event_chain);
        const reject_size = taker.remaining();
        if (reject_size > 0) {
            try appendRejectEvent(event_chain, taker, reject_size, .fok_budget, command.price);
        }
    }

    fn cancelOrder(
        self: *DirectOrderBook,
        command: *cmd.OrderCommand,
        event_chain: ?*events.EventChain,
    ) std.mem.Allocator.Error!cmd.CommandResultCode {
        const order = self.order_id_index.get(command.order_id) orelse return .matching_unknown_order_id;
        if (order.uid != command.uid) return .matching_unknown_order_id;

        command.action = order.action;
        try appendReduceEvent(event_chain, order.action, order.price, order.reserve_bid_price, order.remaining());
        self.removeOrderAndFree(order);
        return .success;
    }

    fn reduceOrder(
        self: *DirectOrderBook,
        command: *cmd.OrderCommand,
        event_chain: ?*events.EventChain,
    ) std.mem.Allocator.Error!cmd.CommandResultCode {
        if (command.size <= 0) return .matching_reduce_failed_wrong_size;

        const order = self.order_id_index.get(command.order_id) orelse return .matching_unknown_order_id;
        if (order.uid != command.uid) return .matching_unknown_order_id;

        const reduce_by: i64 = @min(order.remaining(), command.size);
        if (reduce_by == order.remaining()) {
            command.action = order.action;
            try appendReduceEvent(event_chain, order.action, order.price, order.reserve_bid_price, reduce_by);
            self.removeOrderAndFree(order);
            return .success;
        }

        order.size -= reduce_by;
        order.parent.volume -= reduce_by;
        std.debug.assert(order.parent.volume >= 0);

        command.action = order.action;
        try appendReduceEvent(event_chain, order.action, order.price, order.reserve_bid_price, reduce_by);
        return .success;
    }

    fn moveOrder(
        self: *DirectOrderBook,
        command: *cmd.OrderCommand,
        event_chain: ?*events.EventChain,
    ) std.mem.Allocator.Error!cmd.CommandResultCode {
        const order = self.order_id_index.get(command.order_id) orelse return .matching_unknown_order_id;
        if (order.uid != command.uid) return .matching_unknown_order_id;

        if (self.symbol_spec.symbol_type == .currency_exchange_pair and
            order.action == .bid and
            command.price > order.reserve_bid_price)
        {
            return .matching_move_failed_price_over_risk_limit;
        }

        command.action = order.action;
        self.removeOrderFromChain(order);

        order.price = command.price;

        var taker = ActiveOrder{
            .order_id = order.order_id,
            .price = order.price,
            .size = order.size,
            .filled = order.filled,
            .reserve_bid_price = order.reserve_bid_price,
            .action = order.action,
            .uid = order.uid,
            .timestamp = order.timestamp,
        };

        try self.tryMatchInstantly(&taker, taker.price, event_chain);
        if (taker.filled == taker.size) {
            const removed = self.order_id_index.fetchRemove(order.order_id) orelse
                invariantViolation("direct orderbook id index missing after full move match");
            std.debug.assert(removed.value == order);
            self.order_pool.release(order);
            return .success;
        }

        order.filled = taker.filled;
        try self.insertOrder(order);
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

fn expectSameL2(
    allocator: std.mem.Allocator,
    naive_book: *naive.NaiveOrderBook,
    direct_book: *DirectOrderBook,
    depth: usize,
) !void {
    var naive_l2 = try naive_book.l2Snapshot(allocator, depth);
    defer naive_l2.deinit(allocator);

    var direct_l2 = try direct_book.l2Snapshot(allocator, depth);
    defer direct_l2.deinit(allocator);

    try std.testing.expectEqual(naive_l2.asks.items.len, direct_l2.asks.items.len);
    try std.testing.expectEqual(naive_l2.bids.items.len, direct_l2.bids.items.len);

    for (naive_l2.asks.items, 0..) |level, idx| {
        try std.testing.expectEqual(level.price, direct_l2.asks.items[idx].price);
        try std.testing.expectEqual(level.volume, direct_l2.asks.items[idx].volume);
        try std.testing.expectEqual(level.orders, direct_l2.asks.items[idx].orders);
    }

    for (naive_l2.bids.items, 0..) |level, idx| {
        try std.testing.expectEqual(level.price, direct_l2.bids.items[idx].price);
        try std.testing.expectEqual(level.volume, direct_l2.bids.items[idx].volume);
        try std.testing.expectEqual(level.orders, direct_l2.bids.items[idx].orders);
    }
}

fn randomPrice(random: std.Random, base: i64) i64 {
    const delta = @as(i64, @intCast(random.uintLessThan(u32, 161))) - 80;
    return base + delta;
}

fn oldestInBucket(bucket: *Bucket) ?*DirectOrder {
    var node = bucket.tail orelse return null;
    while (node.next) |older| {
        if (older.parent != bucket) break;
        node = older;
    }
    return node;
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

test "direct book fixed-flow parity with naive" {
    const allocator = std.testing.allocator;
    const spec = try makeTestSpec();

    var direct_book = try DirectOrderBook.init(allocator, spec);
    defer direct_book.deinit();

    var naive_book = try naive.NaiveOrderBook.init(allocator, spec);
    defer naive_book.deinit();

    var c1 = cmd.OrderCommand.newOrder(.gtc, 1, 100, 101, 0, 7, .ask);
    var c2 = c1;
    try std.testing.expectEqual(try naive_book.processCommand(&c1), try direct_book.processCommand(&c2));

    var c3 = cmd.OrderCommand.newOrder(.gtc, 2, 101, 99, 130, 20, .bid);
    var c4 = c3;
    try std.testing.expectEqual(try naive_book.processCommand(&c3), try direct_book.processCommand(&c4));

    var c5 = cmd.OrderCommand.update(2, 101, 101);
    var c6 = c5;
    try std.testing.expectEqual(try naive_book.processCommand(&c5), try direct_book.processCommand(&c6));

    var c7 = cmd.OrderCommand.reduce(2, 101, 5);
    var c8 = c7;
    try std.testing.expectEqual(try naive_book.processCommand(&c7), try direct_book.processCommand(&c8));

    var c9 = cmd.OrderCommand.cancel(2, 101);
    var c10 = c9;
    try std.testing.expectEqual(try naive_book.processCommand(&c9), try direct_book.processCommand(&c10));

    try expectSameL2(allocator, &naive_book, &direct_book, 64);
    naive_book.validateInternalState();
    direct_book.validateInternalState();
}

test "direct book accumulateOrderHolds matches naive" {
    const allocator = std.testing.allocator;
    const spec = try makeTestSpec();

    var direct_book = try DirectOrderBook.init(allocator, spec);
    defer direct_book.deinit();
    var naive_book = try naive.NaiveOrderBook.init(allocator, spec);
    defer naive_book.deinit();

    var ask = cmd.OrderCommand.newOrder(.gtc, 60, 701, 101, 0, 7, .ask);
    var bid = cmd.OrderCommand.newOrder(.gtc, 61, 702, 99, 120, 5, .bid);
    var ask_direct = ask;
    var bid_direct = bid;
    _ = try naive_book.processCommand(&ask);
    _ = try naive_book.processCommand(&bid);
    _ = try direct_book.processCommand(&ask_direct);
    _ = try direct_book.processCommand(&bid_direct);

    var naive_holds: std.AutoHashMapUnmanaged(i32, i64) = .empty;
    defer naive_holds.deinit(allocator);
    var direct_holds: std.AutoHashMapUnmanaged(i32, i64) = .empty;
    defer direct_holds.deinit(allocator);

    try naive_book.accumulateOrderHolds(allocator, &naive_holds);
    try direct_book.accumulateOrderHolds(allocator, &direct_holds);

    try std.testing.expectEqual(naive_holds.get(11) orelse 0, direct_holds.get(11) orelse 0);
    try std.testing.expectEqual(naive_holds.get(15) orelse 0, direct_holds.get(15) orelse 0);
    try std.testing.expectEqual(@as(i64, 7_000_000), direct_holds.get(11) orelse 0);
    try std.testing.expectEqual(@as(i64, 6_009_500), direct_holds.get(15) orelse 0);
}

test "direct book deterministic random parity against naive" {
    const allocator = std.testing.allocator;
    const spec = try makeTestSpec();

    var direct_book = try DirectOrderBook.init(allocator, spec);
    defer direct_book.deinit();

    var naive_book = try naive.NaiveOrderBook.init(allocator, spec);
    defer naive_book.deinit();

    var prng = std.Random.DefaultPrng.init(0xD1EC7B00);
    const random = prng.random();

    const Candidate = struct { order_id: i64, uid: i64 };
    var candidates: std.ArrayListUnmanaged(Candidate) = .empty;
    defer candidates.deinit(allocator);

    var next_order_id: i64 = 1000;
    const base_price: i64 = 10_000;

    for (0..3000) |idx| {
        const roll = random.uintLessThan(u32, 100);

        var naive_cmd: cmd.OrderCommand = undefined;
        var direct_cmd: cmd.OrderCommand = undefined;

        if (roll < 55) {
            const action: common.OrderAction = if (random.boolean()) .ask else .bid;
            const order_type: common.OrderType = blk: {
                const t = random.uintLessThan(u32, 100);
                if (t < 70) break :blk .gtc;
                if (t < 90) break :blk .ioc;
                break :blk .fok_budget;
            };

            const uid = @as(i64, @intCast(random.uintLessThan(u32, 24))) + 1;
            const price = randomPrice(random, base_price);
            const size = @as(i64, @intCast(random.uintLessThan(u32, 40))) + 1;
            const reserve = if (action == .bid) price + @as(i64, @intCast(random.uintLessThan(u32, 50))) else 0;

            const use_duplicate = candidates.items.len > 0 and random.uintLessThan(u32, 100) < 8;
            const order_id = if (use_duplicate) candidates.items[random.uintLessThan(usize, candidates.items.len)].order_id else blk: {
                const id = next_order_id;
                next_order_id += 1;
                try candidates.append(allocator, .{ .order_id = id, .uid = uid });
                break :blk id;
            };

            naive_cmd = cmd.OrderCommand.newOrder(order_type, order_id, uid, price, reserve, size, action);
            direct_cmd = naive_cmd;
        } else if (roll < 70) {
            const pick_known = candidates.items.len > 0 and random.boolean();
            const picked = if (pick_known) candidates.items[random.uintLessThan(usize, candidates.items.len)] else Candidate{
                .order_id = @as(i64, @intCast(random.uintLessThan(u32, 900))) + 1,
                .uid = @as(i64, @intCast(random.uintLessThan(u32, 24))) + 1,
            };
            const uid = if (random.uintLessThan(u32, 100) < 80) picked.uid else picked.uid + 1000;
            naive_cmd = cmd.OrderCommand.cancel(picked.order_id, uid);
            direct_cmd = naive_cmd;
        } else if (roll < 85) {
            const pick_known = candidates.items.len > 0 and random.boolean();
            const picked = if (pick_known) candidates.items[random.uintLessThan(usize, candidates.items.len)] else Candidate{
                .order_id = @as(i64, @intCast(random.uintLessThan(u32, 900))) + 1,
                .uid = @as(i64, @intCast(random.uintLessThan(u32, 24))) + 1,
            };
            const uid = if (random.uintLessThan(u32, 100) < 80) picked.uid else picked.uid + 1000;
            const reduce_by = if (random.uintLessThan(u32, 100) < 5) 0 else (@as(i64, @intCast(random.uintLessThan(u32, 50))) + 1);
            naive_cmd = cmd.OrderCommand.reduce(picked.order_id, uid, reduce_by);
            direct_cmd = naive_cmd;
        } else {
            const pick_known = candidates.items.len > 0 and random.boolean();
            const picked = if (pick_known) candidates.items[random.uintLessThan(usize, candidates.items.len)] else Candidate{
                .order_id = @as(i64, @intCast(random.uintLessThan(u32, 900))) + 1,
                .uid = @as(i64, @intCast(random.uintLessThan(u32, 24))) + 1,
            };
            const uid = if (random.uintLessThan(u32, 100) < 80) picked.uid else picked.uid + 1000;
            const new_price = randomPrice(random, base_price);
            naive_cmd = cmd.OrderCommand.update(picked.order_id, uid, new_price);
            direct_cmd = naive_cmd;
        }

        const naive_res = try naive_book.processCommand(&naive_cmd);
        const direct_res = try direct_book.processCommand(&direct_cmd);
        try std.testing.expectEqual(naive_res, direct_res);

        if (idx % 25 == 0) {
            try expectSameL2(allocator, &naive_book, &direct_book, 64);
            naive_book.validateInternalState();
            direct_book.validateInternalState();
        }
    }

    try expectSameL2(allocator, &naive_book, &direct_book, 64);
    naive_book.validateInternalState();
    direct_book.validateInternalState();
}

test "direct book state hash matches naive and is mutation sensitive" {
    const allocator = std.testing.allocator;
    const spec = try makeTestSpec();

    var direct_book = try DirectOrderBook.init(allocator, spec);
    defer direct_book.deinit();
    var naive_book = try naive.NaiveOrderBook.init(allocator, spec);
    defer naive_book.deinit();

    var ask_direct = cmd.OrderCommand.newOrder(.gtc, 8111, 901, 101, 0, 4, .ask);
    var bid_direct = cmd.OrderCommand.newOrder(.gtc, 8112, 902, 99, 120, 5, .bid);
    var ask_naive = ask_direct;
    var bid_naive = bid_direct;
    _ = try direct_book.processCommand(&ask_direct);
    _ = try direct_book.processCommand(&bid_direct);
    _ = try naive_book.processCommand(&ask_naive);
    _ = try naive_book.processCommand(&bid_naive);

    try std.testing.expectEqual(try naive_book.stateHash(), try direct_book.stateHash());

    var reduce_direct = cmd.OrderCommand.reduce(8112, 902, 1);
    var reduce_naive = reduce_direct;
    _ = try direct_book.processCommand(&reduce_direct);
    _ = try naive_book.processCommand(&reduce_naive);

    try std.testing.expectEqual(try naive_book.stateHash(), try direct_book.stateHash());
}

test "direct book pools reuse order and bucket nodes under churn" {
    const allocator = std.testing.allocator;
    const spec = try makeTestSpec();

    var book = try DirectOrderBook.init(allocator, spec);
    defer book.deinit();

    const initial = book.poolStats();
    try std.testing.expectEqual(@as(usize, 0), initial.order_reuses);
    try std.testing.expectEqual(@as(usize, 0), initial.bucket_reuses);

    var a1 = cmd.OrderCommand.newOrder(.gtc, 9001, 1, 10_000, 0, 3, .ask);
    _ = try book.processCommand(&a1);
    var b1 = cmd.OrderCommand.newOrder(.gtc, 9002, 2, 9_900, 10_100, 2, .bid);
    _ = try book.processCommand(&b1);

    var c1 = cmd.OrderCommand.cancel(9001, 1);
    _ = try book.processCommand(&c1);
    var c2 = cmd.OrderCommand.cancel(9002, 2);
    _ = try book.processCommand(&c2);

    const after_release = book.poolStats();
    try std.testing.expect(after_release.order_free_count >= 2);
    try std.testing.expect(after_release.bucket_free_count >= 2);

    var a2 = cmd.OrderCommand.newOrder(.gtc, 9003, 3, 10_010, 0, 4, .ask);
    _ = try book.processCommand(&a2);
    var b2 = cmd.OrderCommand.newOrder(.gtc, 9004, 4, 9_910, 10_200, 5, .bid);
    _ = try book.processCommand(&b2);

    const after_reuse = book.poolStats();
    try std.testing.expect(after_reuse.order_reuses >= 2);
    try std.testing.expect(after_reuse.bucket_reuses >= 2);

    book.validateInternalState();
}
