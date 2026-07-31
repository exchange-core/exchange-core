const std = @import("std");

/// A bounded FIFO ring whose storage is allocated once during initialization.
///
/// The queue does not synchronize access. Callers that publish or consume from
/// multiple threads must hold an external lock around each operation. `pushSlice`
/// is all-or-nothing, which lets a caller publish a logical batch atomically
/// while holding that lock.
pub fn RuntimeQueue(comptime T: type) type {
    return struct {
        const Self = @This();

        pub const InitError = std.mem.Allocator.Error || error{
            InvalidCapacity,
        };

        pub const PushError = error{
            Full,
        };

        pub const Reservation = struct {
            first: []T,
            second: []T,
            head_before: usize,
            len_before: usize,

            pub fn count(self: Reservation) usize {
                return self.first.len + self.second.len;
            }

            pub fn at(self: *Reservation, index: usize) *T {
                std.debug.assert(index < self.count());
                if (index < self.first.len) return &self.first[index];
                return &self.second[index - self.first.len];
            }
        };

        allocator: std.mem.Allocator,
        storage: []T,
        head: usize = 0,
        len: usize = 0,
        mask: usize,

        pub fn init(allocator: std.mem.Allocator, requested_capacity: usize) InitError!Self {
            if (!isValidCapacity(requested_capacity)) {
                return error.InvalidCapacity;
            }

            return .{
                .allocator = allocator,
                .storage = try allocator.alloc(T, requested_capacity),
                .mask = requested_capacity - 1,
            };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.storage);
            self.* = undefined;
        }

        pub fn capacity(self: *const Self) usize {
            return self.storage.len;
        }

        pub fn count(self: *const Self) usize {
            return self.len;
        }

        pub fn remainingCapacity(self: *const Self) usize {
            return self.capacity() - self.len;
        }

        pub fn isEmpty(self: *const Self) bool {
            return self.len == 0;
        }

        pub fn isFull(self: *const Self) bool {
            return self.len == self.capacity();
        }

        pub fn push(self: *Self, value: T) PushError!void {
            if (self.isFull()) return error.Full;

            self.storage[self.physicalIndex(self.len)] = value;
            self.len += 1;
        }

        /// Publishes every value or none of them when capacity is insufficient.
        pub fn pushSlice(self: *Self, values: []const T) PushError!void {
            if (values.len > self.remainingCapacity()) return error.Full;

            for (values) |value| {
                self.storage[self.physicalIndex(self.len)] = value;
                self.len += 1;
            }
        }

        /// Reserves unreachable slots for an atomic logical batch. The caller
        /// must hold its external queue lock from `reserve` through `commit`.
        /// Abandoning a reservation leaves the queue unchanged.
        pub fn reserve(self: *Self, requested_count: usize) PushError!Reservation {
            if (requested_count > self.remainingCapacity()) return error.Full;

            const tail = self.physicalIndex(self.len);
            const first_len = @min(requested_count, self.capacity() - tail);
            return .{
                .first = self.storage[tail..][0..first_len],
                .second = self.storage[0..][0 .. requested_count - first_len],
                .head_before = self.head,
                .len_before = self.len,
            };
        }

        /// Makes every initialized slot in a reservation visible at once.
        pub fn commit(self: *Self, reservation: Reservation) void {
            std.debug.assert(self.head == reservation.head_before);
            std.debug.assert(self.len == reservation.len_before);
            std.debug.assert(reservation.count() <= self.remainingCapacity());
            self.len += reservation.count();
        }

        pub fn pop(self: *Self) ?T {
            if (self.isEmpty()) return null;

            const value = self.storage[self.head];
            self.head = (self.head +% 1) & self.mask;
            self.len -= 1;
            return value;
        }

        pub fn peek(self: *Self) ?*T {
            if (self.isEmpty()) return null;
            return &self.storage[self.head];
        }

        pub fn peekConst(self: *const Self) ?*const T {
            if (self.isEmpty()) return null;
            return &self.storage[self.head];
        }

        /// Returns an item by logical FIFO position without removing it.
        pub fn at(self: *Self, logical_index: usize) ?*T {
            if (logical_index >= self.len) return null;
            return &self.storage[self.physicalIndex(logical_index)];
        }

        pub fn atConst(self: *const Self, logical_index: usize) ?*const T {
            if (logical_index >= self.len) return null;
            return &self.storage[self.physicalIndex(logical_index)];
        }

        /// Removes `remove_count` items from the front without copying them.
        pub fn discard(self: *Self, remove_count: usize) void {
            std.debug.assert(remove_count <= self.len);
            self.head = (self.head +% remove_count) & self.mask;
            self.len -= remove_count;
        }

        pub fn clear(self: *Self) void {
            self.head = 0;
            self.len = 0;
        }

        fn physicalIndex(self: *const Self, logical_index: usize) usize {
            return (self.head +% logical_index) & self.mask;
        }

        fn isValidCapacity(value: usize) bool {
            return value != 0 and @popCount(value) == 1;
        }
    };
}

test "runtime queue validates power-of-two capacity" {
    const Queue = RuntimeQueue(u8);
    const allocator = std.testing.allocator;

    try std.testing.expectError(error.InvalidCapacity, Queue.init(allocator, 0));
    try std.testing.expectError(error.InvalidCapacity, Queue.init(allocator, 3));

    var queue = try Queue.init(allocator, 4);
    defer queue.deinit();
    try std.testing.expectEqual(@as(usize, 4), queue.capacity());
}

test "runtime queue reports bounded backpressure without partial batch publication" {
    const Queue = RuntimeQueue(u32);
    var queue = try Queue.init(std.testing.allocator, 4);
    defer queue.deinit();

    try queue.pushSlice(&.{ 10, 11, 12 });
    try std.testing.expectEqual(@as(usize, 1), queue.remainingCapacity());
    try std.testing.expectError(error.Full, queue.pushSlice(&.{ 13, 14 }));
    try std.testing.expectEqual(@as(usize, 3), queue.count());
    try std.testing.expectEqual(@as(u32, 10), queue.peekConst().?.*);

    try queue.push(13);
    try std.testing.expect(queue.isFull());
    try std.testing.expectError(error.Full, queue.push(14));
}

test "runtime queue reserves wrapped batches and commits them atomically" {
    const Queue = RuntimeQueue(u32);
    var queue = try Queue.init(std.testing.allocator, 4);
    defer queue.deinit();

    try queue.pushSlice(&.{ 1, 2, 3 });
    try std.testing.expectEqual(@as(u32, 1), queue.pop().?);
    try std.testing.expectEqual(@as(u32, 2), queue.pop().?);

    var abandoned = try queue.reserve(2);
    abandoned.at(0).* = 90;
    abandoned.at(1).* = 91;
    try std.testing.expectEqual(@as(usize, 1), queue.count());

    var reservation = try queue.reserve(3);
    try std.testing.expectEqual(@as(usize, 1), reservation.first.len);
    try std.testing.expectEqual(@as(usize, 2), reservation.second.len);
    for ([_]u32{ 4, 5, 6 }, 0..) |value, index| {
        reservation.at(index).* = value;
    }
    queue.commit(reservation);
    try std.testing.expect(queue.isFull());

    for ([_]u32{ 3, 4, 5, 6 }) |expected| {
        try std.testing.expectEqual(expected, queue.pop().?);
    }
}

test "runtime queue preserves FIFO order across wraparound" {
    const Queue = RuntimeQueue(u32);
    var queue = try Queue.init(std.testing.allocator, 4);
    defer queue.deinit();

    try queue.pushSlice(&.{ 1, 2, 3, 4 });
    try std.testing.expectEqual(@as(u32, 1), queue.pop().?);
    try std.testing.expectEqual(@as(u32, 2), queue.pop().?);
    try queue.pushSlice(&.{ 5, 6 });

    for ([_]u32{ 3, 4, 5, 6 }) |expected| {
        try std.testing.expectEqual(expected, queue.pop().?);
    }
    try std.testing.expect(queue.pop() == null);
}

test "runtime queue seeded schedules preserve capacity and FIFO invariants" {
    const Queue = RuntimeQueue(u64);
    const seed: u64 = 0x52554E54494D4551;
    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    var queue = try Queue.init(std.testing.allocator, 16);
    defer queue.deinit();

    var expected: [16]u64 = undefined;
    var expected_head: usize = 0;
    var expected_len: usize = 0;
    var next_value: u64 = 1;

    for (0..2_000) |step| {
        const publish = expected_len == 0 or
            (expected_len < expected.len and random.boolean());
        if (publish) {
            queue.push(next_value) catch |err| {
                std.debug.panic(
                    "runtime queue seeded publish failed seed=0x{x} step={d} err={s}; replay with: zig build test --test-filter \"runtime queue seeded schedules preserve capacity and FIFO invariants\"",
                    .{ seed, step, @errorName(err) },
                );
            };
            expected[(expected_head + expected_len) & (expected.len - 1)] = next_value;
            expected_len += 1;
            next_value += 1;
        } else {
            const actual = queue.pop() orelse std.debug.panic(
                "runtime queue seeded pop was empty seed=0x{x} step={d}; replay with: zig build test --test-filter \"runtime queue seeded schedules preserve capacity and FIFO invariants\"",
                .{ seed, step },
            );
            const wanted = expected[expected_head];
            if (actual != wanted) {
                std.debug.panic(
                    "runtime queue seeded FIFO mismatch seed=0x{x} step={d} expected={d} actual={d}; replay with: zig build test --test-filter \"runtime queue seeded schedules preserve capacity and FIFO invariants\"",
                    .{ seed, step, wanted, actual },
                );
            }
            expected_head = (expected_head + 1) & (expected.len - 1);
            expected_len -= 1;
        }

        if (queue.count() != expected_len or
            queue.remainingCapacity() + queue.count() != queue.capacity())
        {
            std.debug.panic(
                "runtime queue seeded capacity invariant failed seed=0x{x} step={d}; replay with: zig build test --test-filter \"runtime queue seeded schedules preserve capacity and FIFO invariants\"",
                .{ seed, step },
            );
        }
    }
}
