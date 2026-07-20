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
