const std = @import("std");

/// Preallocated reusable buffer for fixed-size hot-path scratch (Phase D).
/// Capacity is fixed at init; grow is not supported.
pub fn StaticScratch(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        items: []T,
        len: usize = 0,

        pub fn init(allocator: std.mem.Allocator, node_capacity: usize) std.mem.Allocator.Error!Self {
            if (node_capacity == 0) return error.OutOfMemory;
            return .{
                .allocator = allocator,
                .items = try allocator.alloc(T, node_capacity),
            };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.items);
            self.* = undefined;
        }

        pub fn bufferCapacity(self: *const Self) usize {
            return self.items.len;
        }

        pub fn reset(self: *Self) void {
            self.len = 0;
        }

        /// Borrow a prefix of the scratch of exact `count` elements.
        pub fn borrow(self: *Self, count: usize) error{ScratchExhausted}![]T {
            if (count > self.items.len) return error.ScratchExhausted;
            self.len = count;
            return self.items[0..count];
        }
    };
}

test "StaticScratch borrow and reset" {
    const allocator = std.testing.allocator;
    var scratch = try StaticScratch(u32).init(allocator, 4);
    defer scratch.deinit();

    const slice = try scratch.borrow(3);
    try std.testing.expectEqual(@as(usize, 3), slice.len);
    slice[0] = 7;
    try std.testing.expectEqual(@as(u32, 7), scratch.items[0]);

    try std.testing.expectError(error.ScratchExhausted, scratch.borrow(5));
    scratch.reset();
    _ = try scratch.borrow(4);
}

test "StaticScratch zero capacity fails" {
    try std.testing.expectError(
        error.OutOfMemory,
        StaticScratch(u8).init(std.testing.allocator, 0),
    );
}
