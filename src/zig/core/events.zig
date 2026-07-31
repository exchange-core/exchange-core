const std = @import("std");

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

pub const EventChain = struct {
    allocator: std.mem.Allocator,
    head: ?*MatcherTradeEvent = null,
    tail: ?*MatcherTradeEvent = null,
    len: usize = 0,

    pub fn init(allocator: std.mem.Allocator) EventChain {
        return .{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *EventChain) void {
        deinitMatcherEventChain(self.allocator, self.head);
        self.* = undefined;
    }

    pub fn takeOwnedHead(self: *EventChain) ?*MatcherTradeEvent {
        const head = self.head;
        self.head = null;
        self.tail = null;
        self.len = 0;
        return head;
    }

    pub fn append(
        self: *EventChain,
        event: MatcherTradeEvent,
    ) std.mem.Allocator.Error!*MatcherTradeEvent {
        const node = try self.allocator.create(MatcherTradeEvent);
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
        const node = try self.allocator.create(MatcherTradeEvent);
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

pub fn deinitMatcherEventChain(allocator: std.mem.Allocator, head: ?*MatcherTradeEvent) void {
    var node = head;
    while (node) |ev| {
        const next = ev.next_event;
        allocator.destroy(ev);
        node = next;
    }
}

pub fn cloneMatcherEventChain(
    allocator: std.mem.Allocator,
    head: ?*const MatcherTradeEvent,
) std.mem.Allocator.Error!?*MatcherTradeEvent {
    var cloned_head: ?*MatcherTradeEvent = null;
    errdefer deinitMatcherEventChain(allocator, cloned_head);

    var cloned_tail: ?*MatcherTradeEvent = null;
    var source = head;
    while (source) |event| : (source = event.next_event) {
        const cloned = try allocator.create(MatcherTradeEvent);
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
    defer deinitMatcherEventChain(allocator, owned_head);

    try std.testing.expectEqual(@as(usize, 0), chain.len);
    try std.testing.expectEqual(@as(usize, 0), listSize(chain.head));
    try std.testing.expectEqual(@as(usize, 2), listSize(owned_head));
}

test "matcher event chain clone preserves null" {
    try std.testing.expectEqual(
        @as(?*MatcherTradeEvent, null),
        try cloneMatcherEventChain(std.testing.allocator, null),
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

    const cloned_head = try cloneMatcherEventChain(std.testing.allocator, &source_1);
    defer deinitMatcherEventChain(std.testing.allocator, cloned_head);

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
    const cloned_head = try cloneMatcherEventChain(allocator, head);
    defer deinitMatcherEventChain(allocator, cloned_head);
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
