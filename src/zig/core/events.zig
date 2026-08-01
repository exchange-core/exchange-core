const std = @import("std");
const builtin = @import("builtin");
const static_pool = @import("static_pool.zig");

pub const MatcherEventType = enum(u8) {
    trade,
    reject,
    reduce,
    binary_event,
};

pub const MatcherTradeEvent = struct {
    event_type: MatcherEventType,
    section: i32 = 0,
    active_order_completed: bool = false,
    matched_order_id: i64 = 0,
    matched_order_uid: i64 = 0,
    matched_order_completed: bool = false,
    price: i64 = 0,
    size: i64 = 0,
    bidder_hold_price: i64 = 0,
    next_event: ?*MatcherTradeEvent = null,
};

pub const L2Top = struct {
    ask_size: usize = 0,
    bid_size: usize = 0,
    ask_price_0: i64 = std.math.maxInt(i64),
    bid_price_0: i64 = 0,
};

/// Owns lifetime of matcher event nodes. Never free a pool node with the heap
/// allocator or a heap node via the pool.
pub const EventNodesOwner = union(enum) {
    heap: std.mem.Allocator,
    pool: *MatcherEventPool,
};

/// Kernel-set-wide matcher event pool. Leaf-level locking for multi-worker
/// native builds; no-op lock under `single_threaded` (WASM browser target).
/// Uses an atomic spinlock so freestanding WASM does not pull `std.Io.Threaded`.
pub const MatcherEventPool = struct {
    locked: std.atomic.Value(bool) = .init(false),
    inner: static_pool.StaticPool(MatcherTradeEvent),

    pub fn init(allocator: std.mem.Allocator, node_capacity: usize) std.mem.Allocator.Error!MatcherEventPool {
        return .{
            .inner = try static_pool.StaticPool(MatcherTradeEvent).init(allocator, node_capacity),
        };
    }

    pub fn deinit(self: *MatcherEventPool) void {
        self.inner.deinit();
        self.* = undefined;
    }

    pub fn deinitForce(self: *MatcherEventPool) void {
        self.inner.deinitForce();
        self.* = undefined;
    }

    fn lock(self: *MatcherEventPool) void {
        if (comptime builtin.single_threaded) return;
        while (self.locked.cmpxchgWeak(false, true, .acquire, .monotonic) != null) {
            std.atomic.spinLoopHint();
        }
    }

    fn unlock(self: *MatcherEventPool) void {
        if (comptime builtin.single_threaded) return;
        self.locked.store(false, .release);
    }

    pub fn acquire(self: *MatcherEventPool) error{PoolExhausted}!*MatcherTradeEvent {
        self.lock();
        defer self.unlock();
        return self.inner.acquire();
    }

    pub fn release(self: *MatcherEventPool, node: *MatcherTradeEvent) void {
        self.lock();
        defer self.unlock();
        node.next_event = null;
        self.inner.release(node);
    }

    pub fn poolCapacity(self: *const MatcherEventPool) usize {
        return self.inner.capacity;
    }

    pub fn inUse(self: *const MatcherEventPool) usize {
        return self.inner.in_use;
    }

    pub fn highWater(self: *const MatcherEventPool) usize {
        return self.inner.high_water_in_use;
    }
};

pub const EventChain = struct {
    owner: EventNodesOwner,
    head: ?*MatcherTradeEvent = null,
    tail: ?*MatcherTradeEvent = null,
    len: usize = 0,

    /// Heap-backed chain (default / tests / WASM).
    pub fn init(allocator: std.mem.Allocator) EventChain {
        return .{ .owner = .{ .heap = allocator } };
    }

    pub fn initHeap(allocator: std.mem.Allocator) EventChain {
        return init(allocator);
    }

    pub fn initPool(pool: *MatcherEventPool) EventChain {
        return .{ .owner = .{ .pool = pool } };
    }

    pub fn initOwner(owner: EventNodesOwner) EventChain {
        return .{ .owner = owner };
    }

    pub fn deinit(self: *EventChain) void {
        deinitMatcherEventChain(self.owner, self.head);
        self.* = undefined;
    }

    pub fn takeOwnedHead(self: *EventChain) ?*MatcherTradeEvent {
        const head = self.head;
        self.head = null;
        self.tail = null;
        self.len = 0;
        return head;
    }

    /// Transfer ownership of the chain as a head + owner pair so free sites
    /// cannot drop the backend tag.
    pub fn takeOwned(self: *EventChain) OwnedMatcherEvents {
        return .{
            .head = self.takeOwnedHead(),
            .owner = self.owner,
        };
    }

    /// Acquire a node without linking. Maps pool exhaust to OutOfMemory.
    pub fn reserveNode(self: *EventChain) std.mem.Allocator.Error!*MatcherTradeEvent {
        return acquireNode(self.owner);
    }

    pub fn releaseReserved(self: *EventChain, node: *MatcherTradeEvent) void {
        releaseNode(self.owner, node);
    }

    pub fn adoptReservedFront(
        self: *EventChain,
        node: *MatcherTradeEvent,
        filled: MatcherTradeEvent,
    ) void {
        node.* = filled;
        node.next_event = self.head;
        self.head = node;
        if (self.tail == null) self.tail = node;
        self.len += 1;
    }

    pub fn append(
        self: *EventChain,
        event: MatcherTradeEvent,
    ) std.mem.Allocator.Error!*MatcherTradeEvent {
        const node = try acquireNode(self.owner);
        node.* = event;
        node.next_event = null;

        if (self.tail) |tail| {
            tail.next_event = node;
        } else {
            self.head = node;
        }
        self.tail = node;
        self.len += 1;
        return node;
    }

    pub fn prepend(
        self: *EventChain,
        event: MatcherTradeEvent,
    ) std.mem.Allocator.Error!*MatcherTradeEvent {
        const node = try acquireNode(self.owner);
        node.* = event;
        node.next_event = self.head;
        self.head = node;
        if (self.tail == null) {
            self.tail = node;
        }
        self.len += 1;
        return node;
    }
};

fn acquireNode(owner: EventNodesOwner) std.mem.Allocator.Error!*MatcherTradeEvent {
    return switch (owner) {
        .heap => |allocator| allocator.create(MatcherTradeEvent),
        .pool => |pool| pool.acquire() catch error.OutOfMemory,
    };
}

fn releaseNode(owner: EventNodesOwner, node: *MatcherTradeEvent) void {
    switch (owner) {
        .heap => |allocator| allocator.destroy(node),
        .pool => |pool| pool.release(node),
    }
}

pub fn deinitMatcherEventChain(owner: EventNodesOwner, head: ?*MatcherTradeEvent) void {
    var node = head;
    while (node) |ev| {
        const next = ev.next_event;
        releaseNode(owner, ev);
        node = next;
    }
}

/// Owned head transferred from an EventChain (or equivalent). Always free via
/// `deinit` so pool-backed nodes never hit the general allocator.
pub const OwnedMatcherEvents = struct {
    head: ?*MatcherTradeEvent = null,
    owner: EventNodesOwner,

    pub fn empty(owner: EventNodesOwner) OwnedMatcherEvents {
        return .{ .head = null, .owner = owner };
    }

    pub fn deinit(self: *OwnedMatcherEvents) void {
        deinitMatcherEventChain(self.owner, self.head);
        self.head = null;
    }

    pub fn attachToResponse(self: OwnedMatcherEvents, response: anytype) void {
        response.matcher_events_head = self.head;
        response.events_owner = self.owner;
    }
};

pub fn cloneMatcherEventChain(
    owner: EventNodesOwner,
    head: ?*const MatcherTradeEvent,
) std.mem.Allocator.Error!?*MatcherTradeEvent {
    var cloned_head: ?*MatcherTradeEvent = null;
    errdefer deinitMatcherEventChain(owner, cloned_head);

    var cloned_tail: ?*MatcherTradeEvent = null;
    var source = head;
    while (source) |event| : (source = event.next_event) {
        const cloned = try acquireNode(owner);
        cloned.* = event.*;
        cloned.next_event = null;

        if (cloned_tail) |tail| {
            tail.next_event = cloned;
        } else {
            cloned_head = cloned;
        }
        cloned_tail = cloned;
    }

    return cloned_head;
}

pub fn listSize(head: ?*const MatcherTradeEvent) usize {
    var size: usize = 0;
    var node = head;
    while (node) |ev| : (node = ev.next_event) {
        size += 1;
    }
    return size;
}

test "event list size traversal" {
    var e1: MatcherTradeEvent = .{ .event_type = .trade, .size = 1 };
    var e2: MatcherTradeEvent = .{ .event_type = .reduce, .size = 2 };
    var e3: MatcherTradeEvent = .{ .event_type = .reject, .size = 3 };
    e1.next_event = &e2;
    e2.next_event = &e3;

    try std.testing.expectEqual(@as(usize, 3), listSize(&e1));
    try std.testing.expectEqual(@as(usize, 0), listSize(null));
}

test "event chain append and traversal" {
    const allocator = std.testing.allocator;
    var chain = EventChain.init(allocator);
    defer chain.deinit();

    _ = try chain.append(.{ .event_type = .trade, .size = 1 });
    _ = try chain.append(.{ .event_type = .reduce, .size = 2 });

    try std.testing.expectEqual(@as(usize, 2), chain.len);
    try std.testing.expectEqual(@as(usize, 2), listSize(chain.head));
}

test "event chain can transfer ownership of matcher events" {
    const allocator = std.testing.allocator;
    var chain = EventChain.init(allocator);
    defer chain.deinit();

    _ = try chain.append(.{ .event_type = .trade, .size = 5 });
    _ = try chain.append(.{ .event_type = .reject, .size = 2 });

    const owned_head = chain.takeOwnedHead();
    defer deinitMatcherEventChain(.{ .heap = allocator }, owned_head);

    try std.testing.expectEqual(@as(usize, 0), chain.len);
    try std.testing.expectEqual(@as(usize, 0), listSize(chain.head));
    try std.testing.expectEqual(@as(usize, 2), listSize(owned_head));
}

test "matcher event chain clone preserves null" {
    try std.testing.expectEqual(
        @as(?*MatcherTradeEvent, null),
        try cloneMatcherEventChain(.{ .heap = std.testing.allocator }, null),
    );
}

test "matcher event chain clone preserves fields and order without aliasing" {
    var source_1: MatcherTradeEvent = .{
        .event_type = .trade,
        .section = -11,
        .active_order_completed = true,
        .matched_order_id = 12,
        .matched_order_uid = 13,
        .matched_order_completed = false,
        .price = 14,
        .size = 15,
        .bidder_hold_price = 16,
    };
    var source_2: MatcherTradeEvent = .{
        .event_type = .binary_event,
        .section = 21,
        .active_order_completed = false,
        .matched_order_id = -22,
        .matched_order_uid = -23,
        .matched_order_completed = true,
        .price = -24,
        .size = 25,
        .bidder_hold_price = -26,
    };
    var source_3: MatcherTradeEvent = .{
        .event_type = .reduce,
        .section = 31,
        .active_order_completed = true,
        .matched_order_id = 32,
        .matched_order_uid = 33,
        .matched_order_completed = true,
        .price = 34,
        .size = 35,
        .bidder_hold_price = 36,
    };
    source_1.next_event = &source_2;
    source_2.next_event = &source_3;

    const owner: EventNodesOwner = .{ .heap = std.testing.allocator };
    const cloned_head = try cloneMatcherEventChain(owner, &source_1);
    defer deinitMatcherEventChain(owner, cloned_head);

    var source: ?*const MatcherTradeEvent = &source_1;
    var cloned: ?*MatcherTradeEvent = cloned_head;
    while (source) |source_event| {
        const cloned_event = cloned orelse return error.TestUnexpectedResult;
        try std.testing.expect(@intFromPtr(source_event) != @intFromPtr(cloned_event));
        try std.testing.expectEqual(source_event.event_type, cloned_event.event_type);
        try std.testing.expectEqual(source_event.section, cloned_event.section);
        try std.testing.expectEqual(source_event.active_order_completed, cloned_event.active_order_completed);
        try std.testing.expectEqual(source_event.matched_order_id, cloned_event.matched_order_id);
        try std.testing.expectEqual(source_event.matched_order_uid, cloned_event.matched_order_uid);
        try std.testing.expectEqual(source_event.matched_order_completed, cloned_event.matched_order_completed);
        try std.testing.expectEqual(source_event.price, cloned_event.price);
        try std.testing.expectEqual(source_event.size, cloned_event.size);
        try std.testing.expectEqual(source_event.bidder_hold_price, cloned_event.bidder_hold_price);
        source = source_event.next_event;
        cloned = cloned_event.next_event;
    }
    try std.testing.expectEqual(@as(?*MatcherTradeEvent, null), cloned);

    const cloned_first = cloned_head orelse return error.TestUnexpectedResult;
    const cloned_second = cloned_first.next_event orelse return error.TestUnexpectedResult;
    const cloned_third = cloned_second.next_event orelse return error.TestUnexpectedResult;
    cloned_first.price = 1_000;
    cloned_second.next_event = null;
    defer cloned_second.next_event = cloned_third;

    try std.testing.expectEqual(@as(i64, 14), source_1.price);
    try std.testing.expectEqual(@as(?*MatcherTradeEvent, &source_3), source_2.next_event);
}

fn cloneMatcherEventChainAllocationTest(
    allocator: std.mem.Allocator,
    head: ?*const MatcherTradeEvent,
) !void {
    const owner: EventNodesOwner = .{ .heap = allocator };
    const cloned_head = try cloneMatcherEventChain(owner, head);
    defer deinitMatcherEventChain(owner, cloned_head);
    try std.testing.expectEqual(listSize(head), listSize(cloned_head));
}

test "matcher event chain clone cleans every partial allocation on failure" {
    var source_1: MatcherTradeEvent = .{ .event_type = .trade, .size = 1 };
    var source_2: MatcherTradeEvent = .{ .event_type = .reject, .size = 2 };
    var source_3: MatcherTradeEvent = .{ .event_type = .reduce, .size = 3 };
    var source_4: MatcherTradeEvent = .{ .event_type = .binary_event, .size = 4 };
    source_1.next_event = &source_2;
    source_2.next_event = &source_3;
    source_3.next_event = &source_4;

    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        cloneMatcherEventChainAllocationTest,
        .{&source_1},
    );
}

test "pool-backed event chain acquire release clone without heap nodes" {
    const allocator = std.testing.allocator;
    var pool = try MatcherEventPool.init(allocator, 8);
    defer pool.deinit();

    var chain = EventChain.initPool(&pool);
    defer chain.deinit();

    _ = try chain.append(.{ .event_type = .trade, .size = 10 });
    _ = try chain.prepend(.{ .event_type = .reject, .size = 3 });
    try std.testing.expectEqual(@as(usize, 2), chain.len);
    try std.testing.expectEqual(@as(usize, 2), pool.inUse());

    const owner: EventNodesOwner = .{ .pool = &pool };
    const cloned = try cloneMatcherEventChain(owner, chain.head);
    defer deinitMatcherEventChain(owner, cloned);
    try std.testing.expectEqual(@as(usize, 4), pool.inUse());
    try std.testing.expectEqual(@as(usize, 2), listSize(cloned));
}

test "pool-backed event chain exhaust maps to OutOfMemory" {
    const allocator = std.testing.allocator;
    var pool = try MatcherEventPool.init(allocator, 1);
    defer pool.deinit();

    var chain = EventChain.initPool(&pool);
    defer chain.deinit();

    _ = try chain.append(.{ .event_type = .trade, .size = 1 });
    try std.testing.expectError(error.OutOfMemory, chain.append(.{ .event_type = .reject, .size = 1 }));
}

test "pool reserveNode adopt and releaseReserved" {
    const allocator = std.testing.allocator;
    var pool = try MatcherEventPool.init(allocator, 2);
    defer pool.deinit();

    var chain = EventChain.initPool(&pool);
    defer chain.deinit();

    const reserved = try chain.reserveNode();
    chain.releaseReserved(reserved);
    try std.testing.expectEqual(@as(usize, 0), pool.inUse());

    const node = try chain.reserveNode();
    chain.adoptReservedFront(node, .{
        .event_type = .reject,
        .size = 7,
        .active_order_completed = true,
    });
    try std.testing.expectEqual(@as(usize, 1), chain.len);
    try std.testing.expectEqual(@as(i64, 7), chain.head.?.size);
}
