const std = @import("std");

/// A mutex-serialized facade over an existing allocator.
///
/// `LockedAllocator` owns no backing memory and does not manage the backing
/// allocator's lifetime. The wrapper object must remain at a stable address
/// from the first call to `allocator()` until no operation through that facade
/// can still be in flight. Allocated blocks do not retain the facade: after
/// all facade users are quiescent, an outstanding block may instead be
/// resized, remapped, or freed directly through the original backing
/// allocator, and the wrapper may be destroyed before that direct release.
///
/// To serialize a non-thread-safe backing allocator, heap-allocate one stable
/// `LockedAllocator` facade and route every concurrent allocation operation
/// through that one facade. A second facade has a different mutex, and direct
/// calls to the backing allocator bypass this mutex; either may race with this
/// facade and therefore violates the synchronization contract.
pub const LockedAllocator = struct {
    const Self = @This();

    backing: std.mem.Allocator,
    io: std.Io,
    mutex: std.Io.Mutex = .init,

    pub fn init(backing: std.mem.Allocator, io: std.Io) Self {
        return .{
            .backing = backing,
            .io = io,
        };
    }

    /// Returns a non-owning allocator handle backed by `self`.
    ///
    /// Do not move or copy `self` after obtaining this handle.
    pub fn allocator(self: *Self) std.mem.Allocator {
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }

    const vtable: std.mem.Allocator.VTable = .{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };

    fn alloc(
        context: *anyopaque,
        len: usize,
        alignment: std.mem.Alignment,
        return_address: usize,
    ) ?[*]u8 {
        const self: *Self = @ptrCast(@alignCast(context));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.backing.rawAlloc(len, alignment, return_address);
    }

    fn resize(
        context: *anyopaque,
        memory: []u8,
        alignment: std.mem.Alignment,
        new_len: usize,
        return_address: usize,
    ) bool {
        const self: *Self = @ptrCast(@alignCast(context));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.backing.rawResize(memory, alignment, new_len, return_address);
    }

    fn remap(
        context: *anyopaque,
        memory: []u8,
        alignment: std.mem.Alignment,
        new_len: usize,
        return_address: usize,
    ) ?[*]u8 {
        const self: *Self = @ptrCast(@alignCast(context));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.backing.rawRemap(memory, alignment, new_len, return_address);
    }

    fn free(
        context: *anyopaque,
        memory: []u8,
        alignment: std.mem.Alignment,
        return_address: usize,
    ) void {
        const self: *Self = @ptrCast(@alignCast(context));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        self.backing.rawFree(memory, alignment, return_address);
    }
};

const ContentionGate = struct {
    first_backing_call_inside: std.atomic.Value(bool) = .init(false),
    contender_calling: std.atomic.Value(bool) = .init(false),
    abort: std.atomic.Value(bool) = .init(false),
    facade_mutex: ?*std.Io.Mutex = null,
};

const SerializationProbeAllocator = struct {
    const Self = @This();

    backing: std.mem.Allocator,
    contention_gate: ?*ContentionGate = null,
    active_calls: std.atomic.Value(usize) = .init(0),
    overlapping_call_observed: std.atomic.Value(bool) = .init(false),
    alloc_calls: std.atomic.Value(usize) = .init(0),
    resize_calls: std.atomic.Value(usize) = .init(0),
    remap_calls: std.atomic.Value(usize) = .init(0),
    free_calls: std.atomic.Value(usize) = .init(0),

    fn allocator(self: *Self) std.mem.Allocator {
        return .{
            .ptr = self,
            .vtable = &.{
                .alloc = alloc,
                .resize = resize,
                .remap = remap,
                .free = free,
            },
        };
    }

    fn enter(self: *Self) void {
        const previous = self.active_calls.fetchAdd(1, .acq_rel);
        if (previous != 0) {
            self.overlapping_call_observed.store(true, .release);
        }
        if (self.contention_gate) |gate| {
            if (!gate.first_backing_call_inside.swap(true, .acq_rel)) {
                while (!gate.contender_calling.load(.acquire)) {
                    std.atomic.spinLoopHint();
                }
                const facade_mutex = gate.facade_mutex orelse unreachable;
                while (!gate.abort.load(.acquire) and
                    !self.overlapping_call_observed.load(.acquire) and
                    facade_mutex.state.load(.acquire) != .contended)
                {
                    std.Thread.yield() catch {};
                }
            }
        }
        for (0..4) |_| std.Thread.yield() catch {};
    }

    fn leave(self: *Self) void {
        const previous = self.active_calls.fetchSub(1, .acq_rel);
        std.debug.assert(previous != 0);
    }

    fn alloc(
        context: *anyopaque,
        len: usize,
        alignment: std.mem.Alignment,
        return_address: usize,
    ) ?[*]u8 {
        const self: *Self = @ptrCast(@alignCast(context));
        self.enter();
        defer self.leave();

        const result = self.backing.rawAlloc(len, alignment, return_address);
        _ = self.alloc_calls.fetchAdd(1, .monotonic);
        return result;
    }

    fn resize(
        context: *anyopaque,
        memory: []u8,
        alignment: std.mem.Alignment,
        new_len: usize,
        return_address: usize,
    ) bool {
        const self: *Self = @ptrCast(@alignCast(context));
        self.enter();
        defer self.leave();

        const result = self.backing.rawResize(
            memory,
            alignment,
            new_len,
            return_address,
        );
        _ = self.resize_calls.fetchAdd(1, .monotonic);
        return result;
    }

    fn remap(
        context: *anyopaque,
        memory: []u8,
        alignment: std.mem.Alignment,
        new_len: usize,
        return_address: usize,
    ) ?[*]u8 {
        const self: *Self = @ptrCast(@alignCast(context));
        self.enter();
        defer self.leave();

        const result = self.backing.rawRemap(
            memory,
            alignment,
            new_len,
            return_address,
        );
        _ = self.remap_calls.fetchAdd(1, .monotonic);
        return result;
    }

    fn free(
        context: *anyopaque,
        memory: []u8,
        alignment: std.mem.Alignment,
        return_address: usize,
    ) void {
        const self: *Self = @ptrCast(@alignCast(context));
        self.enter();
        defer self.leave();

        self.backing.rawFree(memory, alignment, return_address);
        _ = self.free_calls.fetchAdd(1, .monotonic);
    }
};

/// Single-threaded argument recorder. Keeping it separate from
/// `SerializationProbeAllocator` ensures that a lock regression can only race
/// atomic fields in the concurrent tests.
const RecordingAllocator = struct {
    const Self = @This();

    backing: std.mem.Allocator,
    last_alloc: AllocRecord = undefined,
    last_resize: ResizeRecord = undefined,
    last_remap: RemapRecord = undefined,
    last_free: FreeRecord = undefined,

    const AllocRecord = struct {
        len: usize,
        alignment: std.mem.Alignment,
        return_address: usize,
        result: ?[*]u8,
    };

    const ResizeRecord = struct {
        address: usize,
        len: usize,
        alignment: std.mem.Alignment,
        new_len: usize,
        return_address: usize,
        result: bool,
    };

    const RemapRecord = struct {
        address: usize,
        len: usize,
        alignment: std.mem.Alignment,
        new_len: usize,
        return_address: usize,
        result: ?[*]u8,
    };

    const FreeRecord = struct {
        address: usize,
        len: usize,
        alignment: std.mem.Alignment,
        return_address: usize,
    };

    fn allocator(self: *Self) std.mem.Allocator {
        return .{
            .ptr = self,
            .vtable = &.{
                .alloc = alloc,
                .resize = resize,
                .remap = remap,
                .free = free,
            },
        };
    }

    fn alloc(
        context: *anyopaque,
        len: usize,
        alignment: std.mem.Alignment,
        return_address: usize,
    ) ?[*]u8 {
        const self: *Self = @ptrCast(@alignCast(context));
        const result = self.backing.rawAlloc(len, alignment, return_address);
        self.last_alloc = .{
            .len = len,
            .alignment = alignment,
            .return_address = return_address,
            .result = result,
        };
        return result;
    }

    fn resize(
        context: *anyopaque,
        memory: []u8,
        alignment: std.mem.Alignment,
        new_len: usize,
        return_address: usize,
    ) bool {
        const self: *Self = @ptrCast(@alignCast(context));
        const result = self.backing.rawResize(
            memory,
            alignment,
            new_len,
            return_address,
        );
        self.last_resize = .{
            .address = @intFromPtr(memory.ptr),
            .len = memory.len,
            .alignment = alignment,
            .new_len = new_len,
            .return_address = return_address,
            .result = result,
        };
        return result;
    }

    fn remap(
        context: *anyopaque,
        memory: []u8,
        alignment: std.mem.Alignment,
        new_len: usize,
        return_address: usize,
    ) ?[*]u8 {
        const self: *Self = @ptrCast(@alignCast(context));
        const result = self.backing.rawRemap(
            memory,
            alignment,
            new_len,
            return_address,
        );
        self.last_remap = .{
            .address = @intFromPtr(memory.ptr),
            .len = memory.len,
            .alignment = alignment,
            .new_len = new_len,
            .return_address = return_address,
            .result = result,
        };
        return result;
    }

    fn free(
        context: *anyopaque,
        memory: []u8,
        alignment: std.mem.Alignment,
        return_address: usize,
    ) void {
        const self: *Self = @ptrCast(@alignCast(context));
        self.last_free = .{
            .address = @intFromPtr(memory.ptr),
            .len = memory.len,
            .alignment = alignment,
            .return_address = return_address,
        };
        self.backing.rawFree(memory, alignment, return_address);
    }
};

const ContentionWorker = struct {
    allocator: std.mem.Allocator,
    gate: *ContentionGate,
    contender: bool,
    failure: ?std.mem.Allocator.Error = null,

    fn run(self: *ContentionWorker) void {
        if (self.contender) {
            while (!self.gate.first_backing_call_inside.load(.acquire)) {
                std.atomic.spinLoopHint();
            }
            self.gate.contender_calling.store(true, .release);
        }

        const memory = self.allocator.alloc(u8, 64) catch |err| {
            self.failure = err;
            return;
        };
        self.allocator.free(memory);
    }
};

const StressWorker = struct {
    allocator: std.mem.Allocator,
    worker_index: usize,
    failure: ?std.mem.Allocator.Error = null,

    const iterations = 128;

    fn run(self: *StressWorker) void {
        for (0..iterations) |iteration| {
            const initial_len = 12 + (self.worker_index + iteration) % 13;
            var memory = self.allocator.alloc(u64, initial_len) catch |err| {
                self.failure = err;
                return;
            };

            for (memory, 0..) |*value, index| {
                value.* = @as(u64, @intCast(
                    self.worker_index * iterations + iteration + index,
                ));
            }

            const shrink_len = initial_len - 3;
            if (self.allocator.resize(memory, shrink_len)) {
                memory = memory.ptr[0..shrink_len];
            }

            const grow_len = memory.len + 5;
            if (self.allocator.remap(memory, grow_len)) |remapped| {
                memory = remapped;
            } else {
                memory = self.allocator.realloc(memory, grow_len) catch |err| {
                    self.allocator.free(memory);
                    self.failure = err;
                    return;
                };
            }

            self.allocator.free(memory);
        }
    }
};

fn expectOptionalPointerEqual(
    expected: ?[*]u8,
    actual: ?[*]u8,
) !void {
    if (expected) |expected_pointer| {
        const actual_pointer = actual orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(
            @intFromPtr(expected_pointer),
            @intFromPtr(actual_pointer),
        );
    } else {
        try std.testing.expectEqual(null, actual);
    }
}

test "locked allocator serializes an orchestrated contender" {
    var gate: ContentionGate = .{};
    var probe: SerializationProbeAllocator = .{
        .backing = std.testing.allocator,
        .contention_gate = &gate,
    };
    var locked = LockedAllocator.init(probe.allocator(), std.testing.io);
    gate.facade_mutex = &locked.mutex;
    const allocator = locked.allocator();

    var first: ContentionWorker = .{
        .allocator = allocator,
        .gate = &gate,
        .contender = false,
    };
    var contender: ContentionWorker = .{
        .allocator = allocator,
        .gate = &gate,
        .contender = true,
    };

    const first_thread = try std.Thread.spawn(.{}, ContentionWorker.run, .{&first});
    const contender_thread = std.Thread.spawn(
        .{},
        ContentionWorker.run,
        .{&contender},
    ) catch |err| {
        gate.abort.store(true, .release);
        gate.contender_calling.store(true, .release);
        first_thread.join();
        return err;
    };
    first_thread.join();
    contender_thread.join();

    if (first.failure) |err| return err;
    if (contender.failure) |err| return err;
    try std.testing.expect(gate.first_backing_call_inside.load(.acquire));
    try std.testing.expect(gate.contender_calling.load(.acquire));
    try std.testing.expect(!probe.overlapping_call_observed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 2), probe.alloc_calls.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 2), probe.free_calls.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 0), probe.active_calls.load(.acquire));
}

test "locked allocator serializes concurrent backing stress" {
    var probe: SerializationProbeAllocator = .{
        .backing = std.testing.allocator,
    };
    var locked = LockedAllocator.init(probe.allocator(), std.testing.io);
    const allocator = locked.allocator();

    const worker_count = 8;
    var workers: [worker_count]StressWorker = undefined;
    var threads: [worker_count]std.Thread = undefined;
    var started_threads: usize = 0;
    errdefer for (threads[0..started_threads]) |thread| thread.join();

    for (&workers, 0..) |*worker, index| {
        worker.* = .{
            .allocator = allocator,
            .worker_index = index,
        };
        threads[index] = try std.Thread.spawn(.{}, StressWorker.run, .{worker});
        started_threads += 1;
    }
    for (threads[0..started_threads]) |thread| thread.join();
    started_threads = 0;

    for (workers) |worker| {
        if (worker.failure) |err| return err;
    }

    try std.testing.expect(!probe.overlapping_call_observed.load(.acquire));
    try std.testing.expect(
        probe.alloc_calls.load(.monotonic) >=
            worker_count * StressWorker.iterations,
    );
    try std.testing.expectEqual(
        probe.alloc_calls.load(.monotonic),
        probe.free_calls.load(.monotonic),
    );
    try std.testing.expectEqual(
        @as(usize, worker_count * StressWorker.iterations),
        probe.resize_calls.load(.monotonic),
    );
    try std.testing.expect(
        probe.remap_calls.load(.monotonic) >=
            worker_count * StressWorker.iterations,
    );
    try std.testing.expectEqual(@as(usize, 0), probe.active_calls.load(.acquire));
}

test "locked allocator preserves raw resize remap and free semantics" {
    var probe: RecordingAllocator = .{ .backing = std.testing.allocator };
    var locked = LockedAllocator.init(probe.allocator(), std.testing.io);
    const allocator = locked.allocator();

    const alignment = std.mem.Alignment.fromByteUnits(32);
    const alloc_return_address = 101;
    const resize_return_address = 202;
    const remap_return_address = 303;
    const free_return_address = 404;

    const pointer = allocator.rawAlloc(
        96,
        alignment,
        alloc_return_address,
    ) orelse return error.OutOfMemory;
    var memory: []u8 = pointer[0..96];

    try std.testing.expectEqual(@as(usize, 96), probe.last_alloc.len);
    try std.testing.expectEqual(alignment, probe.last_alloc.alignment);
    try std.testing.expectEqual(
        alloc_return_address,
        probe.last_alloc.return_address,
    );
    try expectOptionalPointerEqual(pointer, probe.last_alloc.result);

    const resize_result = allocator.rawResize(
        memory,
        alignment,
        80,
        resize_return_address,
    );
    try std.testing.expectEqual(resize_result, probe.last_resize.result);
    try std.testing.expectEqual(@intFromPtr(memory.ptr), probe.last_resize.address);
    try std.testing.expectEqual(memory.len, probe.last_resize.len);
    try std.testing.expectEqual(alignment, probe.last_resize.alignment);
    try std.testing.expectEqual(@as(usize, 80), probe.last_resize.new_len);
    try std.testing.expectEqual(
        resize_return_address,
        probe.last_resize.return_address,
    );
    if (resize_result) memory = pointer[0..80];

    const remap_result = allocator.rawRemap(
        memory,
        alignment,
        64,
        remap_return_address,
    );
    try expectOptionalPointerEqual(remap_result, probe.last_remap.result);
    try std.testing.expectEqual(@intFromPtr(memory.ptr), probe.last_remap.address);
    try std.testing.expectEqual(memory.len, probe.last_remap.len);
    try std.testing.expectEqual(alignment, probe.last_remap.alignment);
    try std.testing.expectEqual(@as(usize, 64), probe.last_remap.new_len);
    try std.testing.expectEqual(
        remap_return_address,
        probe.last_remap.return_address,
    );
    if (remap_result) |remapped_pointer| {
        memory = remapped_pointer[0..64];
    }

    const free_address = @intFromPtr(memory.ptr);
    const free_len = memory.len;
    allocator.rawFree(
        memory,
        alignment,
        free_return_address,
    );
    try std.testing.expectEqual(free_address, probe.last_free.address);
    try std.testing.expectEqual(free_len, probe.last_free.len);
    try std.testing.expectEqual(alignment, probe.last_free.alignment);
    try std.testing.expectEqual(
        free_return_address,
        probe.last_free.return_address,
    );
}

test "locked allocator preserves allocation failure without leaks" {
    var failing = std.testing.FailingAllocator.init(
        std.testing.allocator,
        .{ .fail_index = 1 },
    );
    var locked = LockedAllocator.init(failing.allocator(), std.testing.io);
    const allocator = locked.allocator();

    const first = try allocator.alloc(u8, 37);
    try std.testing.expectError(error.OutOfMemory, allocator.alloc(u8, 19));
    allocator.free(first);

    try std.testing.expect(failing.has_induced_failure);
    try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
    try std.testing.expectEqual(failing.allocations, failing.deallocations);
}
