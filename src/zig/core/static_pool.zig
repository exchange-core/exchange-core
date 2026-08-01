const std = @import("std");

/// Bootstrap-sized freelist pool. Capacity is fixed at init; acquire never grows.
/// Nodes are stored in one contiguous slab; free list links via a side array of
/// optional next indices so T need not embed pool links.
pub fn StaticPool(comptime T: type) type {
    return struct {
        const Self = @This();

        pub const Error = error{PoolExhausted};

        allocator: std.mem.Allocator,
        slab: []T,
        next_free: []?u32,
        free_head: ?u32,
        capacity: usize,
        in_use: usize = 0,
        high_water_in_use: usize = 0,
        acquire_count: u64 = 0,
        release_count: u64 = 0,
        exhaust_count: u64 = 0,

        pub fn init(allocator: std.mem.Allocator, capacity: usize) std.mem.Allocator.Error!Self {
            if (capacity == 0) return error.OutOfMemory;
            const slab = try allocator.alloc(T, capacity);
            errdefer allocator.free(slab);
            const next_free = try allocator.alloc(?u32, capacity);
            errdefer allocator.free(next_free);

            var i: usize = 0;
            while (i + 1 < capacity) : (i += 1) {
                next_free[i] = @intCast(i + 1);
            }
            next_free[capacity - 1] = null;

            return .{
                .allocator = allocator,
                .slab = slab,
                .next_free = next_free,
                .free_head = 0,
                .capacity = capacity,
            };
        }

        pub fn deinit(self: *Self) void {
            std.debug.assert(self.in_use == 0);
            self.allocator.free(self.next_free);
            self.allocator.free(self.slab);
            self.* = undefined;
        }

        /// Like deinit but allows in_use > 0 (for fail paths that abandon nodes).
        pub fn deinitForce(self: *Self) void {
            self.allocator.free(self.next_free);
            self.allocator.free(self.slab);
            self.* = undefined;
        }

        pub fn acquire(self: *Self) Error!*T {
            const idx = self.free_head orelse {
                self.exhaust_count += 1;
                return error.PoolExhausted;
            };
            self.free_head = self.next_free[idx];
            self.next_free[idx] = null;
            self.in_use += 1;
            self.acquire_count += 1;
            if (self.in_use > self.high_water_in_use) {
                self.high_water_in_use = self.in_use;
            }
            return &self.slab[idx];
        }

        pub fn release(self: *Self, node: *T) void {
            const base = @intFromPtr(self.slab.ptr);
            const addr = @intFromPtr(node);
            std.debug.assert(addr >= base);
            const offset = addr - base;
            std.debug.assert(offset % @sizeOf(T) == 0);
            const idx: usize = offset / @sizeOf(T);
            std.debug.assert(idx < self.capacity);
            std.debug.assert(self.next_free[idx] == null);
            std.debug.assert(self.in_use > 0);

            self.next_free[idx] = self.free_head;
            self.free_head = @intCast(idx);
            self.in_use -= 1;
            self.release_count += 1;
        }

        pub fn available(self: *const Self) usize {
            return self.capacity - self.in_use;
        }

        pub fn owns(self: *const Self, node: *const T) bool {
            const base = @intFromPtr(self.slab.ptr);
            const addr = @intFromPtr(node);
            if (addr < base) return false;
            const offset = addr - base;
            if (offset % @sizeOf(T) != 0) return false;
            const idx = offset / @sizeOf(T);
            return idx < self.capacity;
        }
    };
}

test "StaticPool acquire release exhaust watermark" {
    const allocator = std.testing.allocator;
    var pool = try StaticPool(u64).init(allocator, 3);
    defer pool.deinit();

    const a = try pool.acquire();
    a.* = 1;
    const b = try pool.acquire();
    b.* = 2;
    const c = try pool.acquire();
    c.* = 3;

    try std.testing.expectEqual(@as(usize, 3), pool.in_use);
    try std.testing.expectEqual(@as(usize, 3), pool.high_water_in_use);
    try std.testing.expectEqual(@as(usize, 0), pool.available());
    try std.testing.expectError(error.PoolExhausted, pool.acquire());
    try std.testing.expectEqual(@as(u64, 1), pool.exhaust_count);

    pool.release(b);
    try std.testing.expectEqual(@as(usize, 2), pool.in_use);
    try std.testing.expectEqual(@as(usize, 1), pool.available());

    const d = try pool.acquire();
    try std.testing.expectEqual(@as(u64, 2), d.*); // recycled slot
    try std.testing.expect(pool.owns(d));

    pool.release(a);
    pool.release(c);
    pool.release(d);
    try std.testing.expectEqual(@as(usize, 0), pool.in_use);
    try std.testing.expectEqual(@as(u64, 4), pool.acquire_count);
    try std.testing.expectEqual(@as(u64, 4), pool.release_count);
}

test "StaticPool zero capacity fails init" {
    try std.testing.expectError(
        error.OutOfMemory,
        StaticPool(u8).init(std.testing.allocator, 0),
    );
}

test "StaticPool freelist reuses last released first" {
    const allocator = std.testing.allocator;
    var pool = try StaticPool(i32).init(allocator, 2);
    defer pool.deinit();

    const a = try pool.acquire();
    const b = try pool.acquire();
    pool.release(a);
    pool.release(b);
    const c = try pool.acquire();
    try std.testing.expectEqual(@intFromPtr(b), @intFromPtr(c));
    pool.release(c);
}
