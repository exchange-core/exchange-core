//! Eclipse Collections 11.0.0 primitive-int hash-map key layout.
//!
//! `IntLongHashMap` and `IntObjectHashMap` serialize by visiting keys `0` and
//! `1` first, followed by occupied backing-table slots in ascending index
//! order. The backing layout depends on insertion, removal, and rehash history,
//! so a sorted final key set is not sufficient for byte-exact Java report
//! production. This type records that layout without owning map values.

const std = @import("std");

pub const default_expected_size: usize = 8;
pub const default_table_size: usize = 16;

const max_table_size: usize = 1 << 30;

pub const LayoutError = std.mem.Allocator.Error || error{CapacityOverflow};
pub const RawSnapshotError = LayoutError || error{
    InvalidTableSize,
    InvalidFlag,
    InvalidOccupancy,
    DuplicateActiveKey,
    UnreachableActiveKey,
};
pub const HistoryError = RawSnapshotError || error{HistoryUnavailable};

/// Persistence representation of the complete EC backing state. `slots`
/// includes empty slots and removal tombstones; callers own it.
pub const RawSnapshot = struct {
    table_length: usize,
    zero_key_flag: u8,
    one_key_flag: u8,
    slots: []i32,

    pub fn deinit(self: *RawSnapshot, allocator: std.mem.Allocator) void {
        allocator.free(self.slots);
        self.* = undefined;
    }

    pub fn clone(
        self: *const RawSnapshot,
        allocator: std.mem.Allocator,
    ) std.mem.Allocator.Error!RawSnapshot {
        return .{
            .table_length = self.table_length,
            .zero_key_flag = self.zero_key_flag,
            .one_key_flag = self.one_key_flag,
            .slots = try allocator.dupe(i32, self.slots),
        };
    }
};

pub const Layout = struct {
    const empty_key: i32 = 0;
    const removed_key: i32 = 1;
    const initial_linear_probe: usize = 8;

    keys: []i32,
    occupied_with_data: usize = 0,
    occupied_with_sentinels: usize = 0,
    contains_zero_key: bool = false,
    contains_one_key: bool = false,

    pub fn init(allocator: std.mem.Allocator) LayoutError!Layout {
        return initTable(allocator, default_table_size);
    }

    pub fn initWithExpectedSize(
        allocator: std.mem.Allocator,
        expected_size: usize,
    ) LayoutError!Layout {
        return initTable(allocator, try tableSizeForExpectedSize(expected_size));
    }

    /// Reconstructs the layout Java creates when a serialized map is decoded
    /// into `new Int*HashMap(serialized_size)` and keys are inserted in wire
    /// encounter order.
    pub fn initFromEncounterOrder(
        allocator: std.mem.Allocator,
        expected_size: usize,
        encounter_keys: []const i32,
    ) LayoutError!Layout {
        var result = try initWithExpectedSize(allocator, expected_size);
        errdefer result.deinit(allocator);
        for (encounter_keys) |key| _ = try result.put(allocator, key);
        return result;
    }

    fn initTable(
        allocator: std.mem.Allocator,
        table_size: usize,
    ) LayoutError!Layout {
        if (table_size == 0 or
            table_size > max_table_size or
            !std.math.isPowerOfTwo(table_size))
        {
            return error.CapacityOverflow;
        }
        const keys = try allocator.alloc(i32, table_size);
        @memset(keys, empty_key);
        return .{ .keys = keys };
    }

    pub fn deinit(self: *Layout, allocator: std.mem.Allocator) void {
        allocator.free(self.keys);
        self.* = undefined;
    }

    pub fn clone(self: *const Layout, allocator: std.mem.Allocator) std.mem.Allocator.Error!Layout {
        const keys = try allocator.dupe(i32, self.keys);
        return .{
            .keys = keys,
            .occupied_with_data = self.occupied_with_data,
            .occupied_with_sentinels = self.occupied_with_sentinels,
            .contains_zero_key = self.contains_zero_key,
            .contains_one_key = self.contains_one_key,
        };
    }

    /// EC's `IntLongHashMap(IntLongMap)` raw-copies a concrete hash map when it
    /// has no removal tombstones. Special-key state is copied alongside the
    /// arrays and does not prevent this fast path.
    pub fn copyForIntLong(
        self: *const Layout,
        allocator: std.mem.Allocator,
    ) LayoutError!Layout {
        if (self.occupied_with_sentinels == 0) {
            return self.clone(allocator);
        }
        return self.copyBySequentialInsertion(allocator);
    }

    /// EC's `IntObjectHashMap(IntObjectMap)` uses a sized constructor followed
    /// by sequential insertion with `max(source.size(), 8)` expected entries,
    /// even when the source is a concrete hash map.
    pub fn copyForIntObject(
        self: *const Layout,
        allocator: std.mem.Allocator,
    ) LayoutError!Layout {
        return self.copyBySequentialInsertion(allocator);
    }

    fn copyBySequentialInsertion(
        self: *const Layout,
        allocator: std.mem.Allocator,
    ) LayoutError!Layout {
        var copy = try initWithExpectedSize(
            allocator,
            @max(self.count(), default_expected_size),
        );
        errdefer copy.deinit(allocator);
        if (self.contains_zero_key) _ = try copy.put(allocator, empty_key);
        if (self.contains_one_key) _ = try copy.put(allocator, removed_key);
        for (self.keys) |key| {
            if (isNonSentinel(key)) _ = try copy.put(allocator, key);
        }
        return copy;
    }

    pub fn exportRaw(
        self: *const Layout,
        allocator: std.mem.Allocator,
    ) std.mem.Allocator.Error!RawSnapshot {
        return .{
            .table_length = self.keys.len,
            .zero_key_flag = @intFromBool(self.contains_zero_key),
            .one_key_flag = @intFromBool(self.contains_one_key),
            .slots = try allocator.dupe(i32, self.keys),
        };
    }

    pub fn importRaw(
        allocator: std.mem.Allocator,
        raw: *const RawSnapshot,
    ) RawSnapshotError!Layout {
        if (raw.table_length == 0 or
            raw.table_length > max_table_size or
            !std.math.isPowerOfTwo(raw.table_length) or
            raw.table_length != raw.slots.len)
        {
            return error.InvalidTableSize;
        }
        if (raw.zero_key_flag > 1 or raw.one_key_flag > 1) {
            return error.InvalidFlag;
        }

        var data_count: usize = 0;
        var tombstone_count: usize = 0;
        for (raw.slots) |key| {
            if (key == removed_key) {
                tombstone_count += 1;
            } else if (key != empty_key) {
                data_count += 1;
            }
        }
        if (data_count + tombstone_count > raw.table_length / 2) {
            return error.InvalidOccupancy;
        }

        var result = Layout{
            .keys = try allocator.dupe(i32, raw.slots),
            .occupied_with_data = data_count,
            .occupied_with_sentinels = tombstone_count,
            .contains_zero_key = raw.zero_key_flag == 1,
            .contains_one_key = raw.one_key_flag == 1,
        };
        errdefer result.deinit(allocator);

        for (result.keys, 0..) |key, index| {
            if (!isNonSentinel(key)) continue;
            const reached = result.probe(key);
            if (reached != index) {
                if (result.keys[reached] == key) return error.DuplicateActiveKey;
                return error.UnreachableActiveKey;
            }
        }
        return result;
    }

    /// Atomically replaces this layout only after allocation and validation.
    pub fn replaceFromRaw(
        self: *Layout,
        allocator: std.mem.Allocator,
        raw: *const RawSnapshot,
    ) RawSnapshotError!void {
        const replacement = try importRaw(allocator, raw);
        var previous = self.*;
        self.* = replacement;
        previous.deinit(allocator);
    }

    pub fn count(self: *const Layout) usize {
        return self.occupied_with_data +
            @intFromBool(self.contains_zero_key) +
            @intFromBool(self.contains_one_key);
    }

    pub fn tableSize(self: *const Layout) usize {
        return self.keys.len;
    }

    /// Returns true only when the key was newly inserted. Updating an existing
    /// Java map entry does not change its backing-table position.
    pub fn put(
        self: *Layout,
        allocator: std.mem.Allocator,
        key: i32,
    ) LayoutError!bool {
        if (key == empty_key) {
            const inserted = !self.contains_zero_key;
            self.contains_zero_key = true;
            return inserted;
        }
        if (key == removed_key) {
            const inserted = !self.contains_one_key;
            self.contains_one_key = true;
            return inserted;
        }

        const index = self.probe(key);
        if (self.keys[index] == key) return false;

        const replaces_removed = self.keys[index] == removed_key;
        const next_data = self.occupied_with_data + 1;
        const next_sentinels = self.occupied_with_sentinels - @intFromBool(replaces_removed);
        if (next_data + next_sentinels > self.maxOccupiedWithData()) {
            // Java inserts first and rehashes afterward. Stage the same state so
            // an allocation failure cannot partially mutate the live layout.
            var staged = try self.clone(allocator);
            errdefer staged.deinit(allocator);
            staged.insertAt(index, key);
            try staged.rehashAndGrow(allocator);

            var previous = self.*;
            self.* = staged;
            previous.deinit(allocator);
            return true;
        }

        self.insertAt(index, key);
        return true;
    }

    pub fn remove(self: *Layout, key: i32) bool {
        if (key == empty_key) {
            const removed = self.contains_zero_key;
            self.contains_zero_key = false;
            return removed;
        }
        if (key == removed_key) {
            const removed = self.contains_one_key;
            self.contains_one_key = false;
            return removed;
        }

        const index = self.probe(key);
        if (self.keys[index] != key) return false;
        self.keys[index] = removed_key;
        self.occupied_with_data -= 1;
        self.occupied_with_sentinels += 1;
        return true;
    }

    pub fn clear(self: *Layout) void {
        @memset(self.keys, empty_key);
        self.occupied_with_data = 0;
        self.occupied_with_sentinels = 0;
        self.contains_zero_key = false;
        self.contains_one_key = false;
    }

    pub fn compact(self: *Layout, allocator: std.mem.Allocator) LayoutError!void {
        try self.rehash(allocator, try smallestPowerOfTwoAtLeast(self.count()));
    }

    pub fn orderedKeys(
        self: *const Layout,
        allocator: std.mem.Allocator,
    ) std.mem.Allocator.Error![]i32 {
        const ordered = try allocator.alloc(i32, self.count());
        var index: usize = 0;
        if (self.contains_zero_key) {
            ordered[index] = empty_key;
            index += 1;
        }
        if (self.contains_one_key) {
            ordered[index] = removed_key;
            index += 1;
        }
        for (self.keys) |key| {
            if (!isNonSentinel(key)) continue;
            ordered[index] = key;
            index += 1;
        }
        std.debug.assert(index == ordered.len);
        return ordered;
    }

    fn insertAt(self: *Layout, index: usize, key: i32) void {
        if (self.keys[index] == removed_key) self.occupied_with_sentinels -= 1;
        self.keys[index] = key;
        self.occupied_with_data += 1;
    }

    fn rehashAndGrow(self: *Layout, allocator: std.mem.Allocator) LayoutError!void {
        const maximum_occupied = self.maxOccupiedWithData();
        const next_data = std.math.add(usize, self.occupied_with_data, 1) catch
            return error.CapacityOverflow;
        const doubled = std.math.mul(usize, next_data, 2) catch
            return error.CapacityOverflow;
        var new_size = @max(maximum_occupied, try smallestPowerOfTwoAtLeast(doubled));
        if (self.occupied_with_sentinels > 0 and
            maximum_occupied / 2 + maximum_occupied / 4 < self.occupied_with_data)
        {
            new_size = std.math.mul(usize, new_size, 2) catch
                return error.CapacityOverflow;
        }
        if (new_size > max_table_size) return error.CapacityOverflow;
        try self.rehash(allocator, new_size);
    }

    fn rehash(
        self: *Layout,
        allocator: std.mem.Allocator,
        requested_size: usize,
    ) LayoutError!void {
        if (requested_size == 0 or
            requested_size > max_table_size or
            !std.math.isPowerOfTwo(requested_size))
        {
            return error.CapacityOverflow;
        }

        var replacement = try initTable(allocator, requested_size);
        errdefer replacement.deinit(allocator);
        replacement.contains_zero_key = self.contains_zero_key;
        replacement.contains_one_key = self.contains_one_key;
        for (self.keys) |key| {
            if (isNonSentinel(key)) _ = try replacement.put(allocator, key);
        }

        var previous = self.*;
        self.* = replacement;
        previous.deinit(allocator);
    }

    fn maxOccupiedWithData(self: *const Layout) usize {
        return self.keys.len >> 1;
    }

    fn probe(self: *const Layout, key: i32) usize {
        const mask = self.keys.len - 1;
        const initial_index = @as(usize, @intCast(@as(u32, @bitCast(key)))) & mask;
        var index = initial_index;
        var current = self.keys[index];
        if (current == key or current == empty_key) return index;

        var first_removed: ?usize = if (current == removed_key) index else null;
        for (1..initial_linear_probe) |offset| {
            index = (initial_index + offset) & mask;
            current = self.keys[index];
            if (current == key) return index;
            if (current == empty_key) return first_removed orelse index;
            if (current == removed_key and first_removed == null) first_removed = index;
        }
        return self.probeTwo(key, first_removed);
    }

    fn probeTwo(self: *const Layout, key: i32, first_removed_input: ?usize) usize {
        const mask = self.keys.len - 1;
        const spread_index = @as(usize, spreadTwo(key)) & mask;
        var first_removed = first_removed_input;
        for (0..initial_linear_probe) |offset| {
            const index = (spread_index + offset) & mask;
            const current = self.keys[index];
            if (current == key) return index;
            if (current == empty_key) return first_removed orelse index;
            if (current == removed_key and first_removed == null) first_removed = index;
        }
        return self.probeThree(key, first_removed);
    }

    fn probeThree(self: *const Layout, key: i32, first_removed_input: ?usize) usize {
        const mask = self.keys.len - 1;
        var spread_index = spreadOne(key);
        const step = @bitReverse(spreadTwo(key)) | 1;
        var first_removed = first_removed_input;
        while (true) {
            spread_index +%= step;
            const index = @as(usize, spread_index) & mask;
            const current = self.keys[index];
            if (current == key) return index;
            if (current == empty_key) return first_removed orelse index;
            if (current == removed_key and first_removed == null) first_removed = index;
        }
    }
};

/// Tracks whether byte-exact EC layout history is available. A newly-created
/// clean map is represented without allocation by `lazy_known_empty`; the first
/// operation that needs slots materializes its exact retained table capacity.
/// Once history is unavailable, mutations and copies deliberately keep it
/// unavailable instead of inventing a plausible but incorrect layout.
pub const History = union(enum) {
    lazy_known_empty: usize,
    unavailable,
    known: Layout,

    pub fn init() History {
        return .{ .lazy_known_empty = default_table_size };
    }

    pub fn initWithExpectedSize(expected_size: usize) LayoutError!History {
        return .{
            .lazy_known_empty = try tableSizeForExpectedSize(expected_size),
        };
    }

    pub fn initUnavailable() History {
        return .unavailable;
    }

    pub fn initKnown(layout: Layout) History {
        return .{ .known = layout };
    }

    /// Reconstructs a history from the serialized encounter sequence used to
    /// populate a Java sized constructor. Empty histories retain only their
    /// exact lazy capacity; non-empty histories own the reconstructed layout.
    pub fn initFromEncounterOrder(
        allocator: std.mem.Allocator,
        expected_size: usize,
        encounter_keys: []const i32,
    ) LayoutError!History {
        if (encounter_keys.len == 0) {
            return initWithExpectedSize(expected_size);
        }
        return .{
            .known = try Layout.initFromEncounterOrder(
                allocator,
                expected_size,
                encounter_keys,
            ),
        };
    }

    pub fn deinit(self: *History, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .known => |*layout| layout.deinit(allocator),
            .lazy_known_empty, .unavailable => {},
        }
        self.* = undefined;
    }

    pub fn isKnown(self: *const History) bool {
        return switch (self.*) {
            .lazy_known_empty, .known => true,
            .unavailable => false,
        };
    }

    pub fn tableSizeIfKnown(self: *const History) ?usize {
        return switch (self.*) {
            .lazy_known_empty => |table_size| table_size,
            .known => |*layout| layout.tableSize(),
            .unavailable => null,
        };
    }

    pub fn clone(
        self: *const History,
        allocator: std.mem.Allocator,
    ) std.mem.Allocator.Error!History {
        return switch (self.*) {
            .lazy_known_empty => |table_size| .{
                .lazy_known_empty = table_size,
            },
            .unavailable => .unavailable,
            .known => |*layout| .{ .known = try layout.clone(allocator) },
        };
    }

    pub fn copyForIntLong(
        self: *const History,
        allocator: std.mem.Allocator,
    ) LayoutError!History {
        return switch (self.*) {
            .lazy_known_empty => |table_size| .{
                .lazy_known_empty = table_size,
            },
            .unavailable => .unavailable,
            .known => |*layout| .{ .known = try layout.copyForIntLong(allocator) },
        };
    }

    pub fn copyForIntObject(
        self: *const History,
        allocator: std.mem.Allocator,
    ) LayoutError!History {
        return switch (self.*) {
            .lazy_known_empty => try initWithExpectedSize(default_expected_size),
            .unavailable => .unavailable,
            .known => |*layout| .{ .known = try layout.copyForIntObject(allocator) },
        };
    }

    pub fn recordPut(
        self: *History,
        allocator: std.mem.Allocator,
        key: i32,
    ) LayoutError!void {
        switch (self.*) {
            .unavailable => {},
            .known => |*layout| _ = try layout.put(allocator, key),
            .lazy_known_empty => |table_size| {
                var layout = try Layout.initTable(allocator, table_size);
                errdefer layout.deinit(allocator);
                _ = try layout.put(allocator, key);
                self.* = .{ .known = layout };
            },
        }
    }

    pub fn orderedKeys(
        self: *const History,
        allocator: std.mem.Allocator,
    ) HistoryError![]i32 {
        return switch (self.*) {
            .unavailable => error.HistoryUnavailable,
            .lazy_known_empty => try allocator.alloc(i32, 0),
            .known => |*layout| try layout.orderedKeys(allocator),
        };
    }

    pub fn recordRemove(self: *History, key: i32) void {
        switch (self.*) {
            .known => |*layout| _ = layout.remove(key),
            .lazy_known_empty, .unavailable => {},
        }
    }

    pub fn clear(self: *History) void {
        switch (self.*) {
            .known => |*layout| layout.clear(),
            .lazy_known_empty, .unavailable => {},
        }
    }

    pub fn exportRaw(
        self: *const History,
        allocator: std.mem.Allocator,
    ) HistoryError!RawSnapshot {
        return switch (self.*) {
            .unavailable => error.HistoryUnavailable,
            .known => |*layout| layout.exportRaw(allocator),
            .lazy_known_empty => |table_size| blk: {
                var empty = try Layout.initTable(allocator, table_size);
                defer empty.deinit(allocator);
                break :blk try empty.exportRaw(allocator);
            },
        };
    }

    pub fn importRaw(
        allocator: std.mem.Allocator,
        raw: *const RawSnapshot,
    ) RawSnapshotError!History {
        return .{ .known = try Layout.importRaw(allocator, raw) };
    }

    /// Atomically replaces this history only after allocation and validation.
    pub fn replaceFromRaw(
        self: *History,
        allocator: std.mem.Allocator,
        raw: *const RawSnapshot,
    ) RawSnapshotError!void {
        const replacement = try importRaw(allocator, raw);
        var previous = self.*;
        self.* = replacement;
        previous.deinit(allocator);
    }
};

fn isNonSentinel(key: i32) bool {
    return key != 0 and key != 1;
}

fn smallestPowerOfTwoAtLeast(value: usize) error{CapacityOverflow}!usize {
    if (value <= 1) return 1;
    if (value > max_table_size) return error.CapacityOverflow;
    const result = std.math.ceilPowerOfTwo(usize, value) catch
        return error.CapacityOverflow;
    if (result > max_table_size) return error.CapacityOverflow;
    return result;
}

fn tableSizeForExpectedSize(expected_size: usize) error{CapacityOverflow}!usize {
    const doubled = std.math.mul(usize, expected_size, 2) catch
        return error.CapacityOverflow;
    if (doubled > max_table_size) return error.CapacityOverflow;
    return smallestPowerOfTwoAtLeast(doubled);
}

fn spreadOne(key: i32) u32 {
    var spread: u32 = @bitCast(key);
    spread ^= spread >> 15;
    spread *%= @bitCast(@as(i32, -1_398_068_659));
    spread ^= spread >> 15;
    spread *%= 1_556_602_707;
    spread ^= spread >> 12;
    return spread;
}

fn spreadTwo(key: i32) u32 {
    var spread: u32 = @bitCast(key);
    spread ^= spread >> 14;
    spread *%= @bitCast(@as(i32, -1_172_517_581));
    spread ^= spread >> 13;
    spread *%= @bitCast(@as(i32, -1_688_037_685));
    spread ^= spread >> 12;
    return spread;
}

fn expectInsertionOrder(insertions: []const i32, expected: []const i32) !void {
    const allocator = std.testing.allocator;
    var layout = try Layout.init(allocator);
    defer layout.deinit(allocator);
    for (insertions) |key| try std.testing.expect(try layout.put(allocator, key));
    const ordered = try layout.orderedKeys(allocator);
    defer allocator.free(ordered);
    try std.testing.expectEqualSlices(i32, expected, ordered);
}

test "Eclipse 11 collision vectors retain backing-table iteration order" {
    try expectInsertionOrder(&.{ 2, 17, 33 }, &.{ 17, 2, 33 });
    try expectInsertionOrder(&.{ 17, 33, 2 }, &.{ 17, 33, 2 });
    try expectInsertionOrder(&.{ 33, 2, 17 }, &.{ 33, 2, 17 });
    // EC probes initial_index + offset. Advancing from the previously probed
    // index instead would visit +1, +3, +6 and incorrectly leave 18 after 5.
    try expectInsertionOrder(&.{ 2, 3, 5, 18 }, &.{ 2, 3, 18, 5 });
}

test "Eclipse sentinels precede backing slots and updates retain layout" {
    const allocator = std.testing.allocator;
    var layout = try Layout.init(allocator);
    defer layout.deinit(allocator);
    for ([_]i32{ 2, 17, 33, 1, 0 }) |key| {
        try std.testing.expect(try layout.put(allocator, key));
    }
    try std.testing.expect(!try layout.put(allocator, 17));
    const ordered = try layout.orderedKeys(allocator);
    defer allocator.free(ordered);
    try std.testing.expectEqualSlices(i32, &.{ 0, 1, 17, 2, 33 }, ordered);
}

test "Eclipse removal tombstones and reinsertion preserve exact slot history" {
    const allocator = std.testing.allocator;
    var layout = try Layout.init(allocator);
    defer layout.deinit(allocator);
    for ([_]i32{ 2, 17, 33 }) |key| _ = try layout.put(allocator, key);
    try std.testing.expect(layout.remove(17));
    try std.testing.expect(!layout.remove(17));

    const removed = try layout.orderedKeys(allocator);
    defer allocator.free(removed);
    try std.testing.expectEqualSlices(i32, &.{ 2, 33 }, removed);

    try std.testing.expect(try layout.put(allocator, 17));
    const reinserted = try layout.orderedKeys(allocator);
    defer allocator.free(reinserted);
    try std.testing.expectEqualSlices(i32, &.{ 17, 2, 33 }, reinserted);
}

test "growth allocation failure leaves Eclipse layout unchanged" {
    const allocator = std.testing.allocator;
    var layout = try Layout.init(allocator);
    defer layout.deinit(allocator);
    for ([_]i32{ 2, 3, 4, 5, 6, 7, 8, 9 }) |key| _ = try layout.put(allocator, key);
    const before = try layout.orderedKeys(allocator);
    defer allocator.free(before);

    var failing = std.testing.FailingAllocator.init(allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, layout.put(failing.allocator(), 10));
    const after = try layout.orderedKeys(allocator);
    defer allocator.free(after);
    try std.testing.expectEqualSlices(i32, before, after);
    try std.testing.expectEqual(@as(usize, 16), layout.tableSize());
}

test "Eclipse 11 resize removal compact and sized-constructor probes match Java" {
    const allocator = std.testing.allocator;
    var growth = try Layout.init(allocator);
    defer growth.deinit(allocator);
    for ([_]i32{ 2, 3, 4, 5, 6, 7, 8, 9, 10 }) |key| _ = try growth.put(allocator, key);
    try std.testing.expectEqual(@as(usize, 32), growth.tableSize());
    var ordered = try growth.orderedKeys(allocator);
    try std.testing.expectEqualSlices(i32, &.{ 2, 3, 4, 5, 6, 7, 8, 9, 10 }, ordered);
    allocator.free(ordered);

    try std.testing.expect(growth.remove(4));
    try std.testing.expect(growth.remove(7));
    ordered = try growth.orderedKeys(allocator);
    try std.testing.expectEqualSlices(i32, &.{ 2, 3, 5, 6, 8, 9, 10 }, ordered);
    allocator.free(ordered);

    _ = try growth.put(allocator, 17);
    _ = try growth.put(allocator, 33);
    ordered = try growth.orderedKeys(allocator);
    const reused = [_]i32{ 33, 2, 3, 5, 6, 8, 9, 10, 17 };
    try std.testing.expectEqualSlices(i32, &reused, ordered);
    allocator.free(ordered);

    try growth.compact(allocator);
    try std.testing.expectEqual(@as(usize, 32), growth.tableSize());
    ordered = try growth.orderedKeys(allocator);
    try std.testing.expectEqualSlices(i32, &reused, ordered);
    allocator.free(ordered);

    var sized = try Layout.initWithExpectedSize(allocator, 3);
    defer sized.deinit(allocator);
    for ([_]i32{ 33, 2, 17, 0, 1 }) |key| _ = try sized.put(allocator, key);
    try std.testing.expectEqual(@as(usize, 8), sized.tableSize());
    ordered = try sized.orderedKeys(allocator);
    defer allocator.free(ordered);
    try std.testing.expectEqualSlices(i32, &.{ 0, 1, 33, 2, 17 }, ordered);
}

test "default and zero-sized constructors retain distinct exact capacities" {
    const allocator = std.testing.allocator;
    var default_layout = try Layout.init(allocator);
    defer default_layout.deinit(allocator);
    try std.testing.expectEqual(
        default_table_size,
        default_layout.tableSize(),
    );

    var zero_sized = try Layout.initWithExpectedSize(allocator, 0);
    defer zero_sized.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), zero_sized.tableSize());
    _ = try zero_sized.put(allocator, 2);
    try std.testing.expectEqual(@as(usize, 4), zero_sized.tableSize());
}

test "table sizing and raw import reject values beyond the supported maximum" {
    const allocator = std.testing.allocator;
    try std.testing.expectEqual(
        max_table_size,
        try tableSizeForExpectedSize(max_table_size / 2),
    );
    try std.testing.expectError(
        error.CapacityOverflow,
        tableSizeForExpectedSize(max_table_size / 2 + 1),
    );
    try std.testing.expectError(
        error.CapacityOverflow,
        smallestPowerOfTwoAtLeast(max_table_size + 1),
    );
    try std.testing.expectError(
        error.CapacityOverflow,
        Layout.initTable(allocator, max_table_size + 1),
    );
    try std.testing.expectError(
        error.CapacityOverflow,
        Layout.initTable(allocator, 3),
    );

    var no_slots: [0]i32 = .{};
    const oversized_raw = RawSnapshot{
        .table_length = max_table_size + 1,
        .zero_key_flag = 0,
        .one_key_flag = 0,
        .slots = no_slots[0..],
    };
    try std.testing.expectError(
        error.InvalidTableSize,
        Layout.importRaw(allocator, &oversized_raw),
    );
}

test "raw snapshot roundtrip preserves every slot and rejects tampering" {
    const allocator = std.testing.allocator;
    var layout = try Layout.init(allocator);
    defer layout.deinit(allocator);
    for ([_]i32{ 2, 17, 33, 0, 1 }) |key| _ = try layout.put(allocator, key);
    try std.testing.expect(layout.remove(17));

    var raw = try layout.exportRaw(allocator);
    defer raw.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 16), raw.table_length);
    try std.testing.expectEqual(@as(u8, 1), raw.zero_key_flag);
    try std.testing.expectEqual(@as(u8, 1), raw.one_key_flag);
    try std.testing.expect(std.mem.indexOfScalar(i32, raw.slots, 1) != null);

    var restored = try Layout.importRaw(allocator, &raw);
    defer restored.deinit(allocator);
    var restored_raw = try restored.exportRaw(allocator);
    defer restored_raw.deinit(allocator);
    try std.testing.expectEqual(raw.table_length, restored_raw.table_length);
    try std.testing.expectEqual(raw.zero_key_flag, restored_raw.zero_key_flag);
    try std.testing.expectEqual(raw.one_key_flag, restored_raw.one_key_flag);
    try std.testing.expectEqualSlices(i32, raw.slots, restored_raw.slots);

    var bad_flag = try raw.clone(allocator);
    defer bad_flag.deinit(allocator);
    bad_flag.zero_key_flag = 2;
    try std.testing.expectError(error.InvalidFlag, Layout.importRaw(allocator, &bad_flag));

    var bad_size = try raw.clone(allocator);
    defer bad_size.deinit(allocator);
    bad_size.table_length = 15;
    try std.testing.expectError(error.InvalidTableSize, Layout.importRaw(allocator, &bad_size));

    var duplicate = try raw.clone(allocator);
    defer duplicate.deinit(allocator);
    const active_index = std.mem.indexOfScalar(i32, duplicate.slots, 2).?;
    const empty_index = std.mem.indexOfScalar(i32, duplicate.slots, 0).?;
    duplicate.slots[empty_index] = duplicate.slots[active_index];
    try std.testing.expectError(
        error.DuplicateActiveKey,
        Layout.importRaw(allocator, &duplicate),
    );

    var unreachable_raw = try raw.clone(allocator);
    defer unreachable_raw.deinit(allocator);
    unreachable_raw.slots[active_index] = 0;
    unreachable_raw.slots[empty_index] = 2;
    try std.testing.expectError(
        error.UnreachableActiveKey,
        Layout.importRaw(allocator, &unreachable_raw),
    );
}

test "history distinguishes lazy known empty from propagated unavailable" {
    const allocator = std.testing.allocator;
    var empty = History.init();
    defer empty.deinit(allocator);
    try std.testing.expect(empty.isKnown());
    var empty_raw = try empty.exportRaw(allocator);
    defer empty_raw.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 16), empty_raw.table_length);

    var unavailable = History.initUnavailable();
    defer unavailable.deinit(allocator);
    try std.testing.expect(!unavailable.isKnown());
    try unavailable.recordPut(allocator, 42);
    unavailable.recordRemove(42);
    unavailable.clear();
    try std.testing.expectError(
        error.HistoryUnavailable,
        unavailable.exportRaw(allocator),
    );

    var cloned = try unavailable.clone(allocator);
    defer cloned.deinit(allocator);
    var long_copy = try unavailable.copyForIntLong(allocator);
    defer long_copy.deinit(allocator);
    var object_copy = try unavailable.copyForIntObject(allocator);
    defer object_copy.deinit(allocator);
    try std.testing.expect(!cloned.isKnown());
    try std.testing.expect(!long_copy.isKnown());
    try std.testing.expect(!object_copy.isKnown());
}

test "history preserves lazy capacity through clone and IntLong copy" {
    const allocator = std.testing.allocator;
    var default_history = History.init();
    defer default_history.deinit(allocator);
    try std.testing.expectEqual(
        @as(?usize, default_table_size),
        default_history.tableSizeIfKnown(),
    );

    var zero_sized = try History.initWithExpectedSize(0);
    defer zero_sized.deinit(allocator);
    try std.testing.expect(zero_sized == .lazy_known_empty);
    try std.testing.expectEqual(
        @as(?usize, 1),
        zero_sized.tableSizeIfKnown(),
    );
    const empty_keys = try zero_sized.orderedKeys(allocator);
    defer allocator.free(empty_keys);
    try std.testing.expectEqual(@as(usize, 0), empty_keys.len);

    var cloned = try zero_sized.clone(allocator);
    defer cloned.deinit(allocator);
    try std.testing.expectEqual(@as(?usize, 1), cloned.tableSizeIfKnown());

    var long_copy = try zero_sized.copyForIntLong(allocator);
    defer long_copy.deinit(allocator);
    try std.testing.expectEqual(@as(?usize, 1), long_copy.tableSizeIfKnown());

    var object_copy = try zero_sized.copyForIntObject(allocator);
    defer object_copy.deinit(allocator);
    try std.testing.expectEqual(
        @as(?usize, default_table_size),
        object_copy.tableSizeIfKnown(),
    );
}

test "history encounter reconstruction preserves Java sized insertion order" {
    const allocator = std.testing.allocator;
    const encounter = [_]i32{ 33, 2, 17, 0, 1 };
    var history = try History.initFromEncounterOrder(
        allocator,
        encounter.len,
        &encounter,
    );
    defer history.deinit(allocator);
    try std.testing.expectEqual(
        @as(?usize, default_table_size),
        history.tableSizeIfKnown(),
    );
    const ordered = try history.orderedKeys(allocator);
    defer allocator.free(ordered);
    try std.testing.expectEqualSlices(
        i32,
        &.{ 0, 1, 33, 2, 17 },
        ordered,
    );

    var empty = try History.initFromEncounterOrder(allocator, 0, &.{});
    defer empty.deinit(allocator);
    try std.testing.expect(empty == .lazy_known_empty);
    try std.testing.expectEqual(@as(?usize, 1), empty.tableSizeIfKnown());
}

test "clear retains layout capacity and resets raw state" {
    const allocator = std.testing.allocator;
    var layout = try Layout.init(allocator);
    defer layout.deinit(allocator);
    for (2..11) |key| _ = try layout.put(allocator, @intCast(key));
    try std.testing.expectEqual(@as(usize, 32), layout.tableSize());
    try std.testing.expect(layout.remove(4));

    layout.clear();
    try std.testing.expectEqual(@as(usize, 32), layout.tableSize());
    try std.testing.expectEqual(@as(usize, 0), layout.count());
    var raw = try layout.exportRaw(allocator);
    defer raw.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 32), raw.table_length);
    for (raw.slots) |slot| try std.testing.expectEqual(@as(i32, 0), slot);
}

test "EC IntLong and IntObject constructor copy paths differ" {
    const allocator = std.testing.allocator;
    var source = try Layout.init(allocator);
    defer source.deinit(allocator);
    for (2..11) |key| _ = try source.put(allocator, @intCast(key));
    try std.testing.expectEqual(@as(usize, 32), source.tableSize());

    // Special keys do not block IntLong's raw-copy fast path.
    source.clear();
    for ([_]i32{ 33, 2, 0 }) |key| _ = try source.put(allocator, key);
    var long_raw_copy = try source.copyForIntLong(allocator);
    defer long_raw_copy.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 32), long_raw_copy.tableSize());
    try std.testing.expectEqualSlices(i32, source.keys, long_raw_copy.keys);
    try std.testing.expect(long_raw_copy.contains_zero_key);

    // IntObject always reconstructs sequentially, with a minimum expected size
    // of eight and therefore a minimum 16-slot backing table.
    var object_sequential = try source.copyForIntObject(allocator);
    defer object_sequential.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 16), object_sequential.tableSize());

    // A tombstone blocks IntLong's raw-copy path even without special keys.
    source.clear();
    for ([_]i32{ 33, 2, 17 }) |key| _ = try source.put(allocator, key);
    try std.testing.expect(source.remove(17));
    try std.testing.expectEqual(@as(usize, 1), source.occupied_with_sentinels);
    var long_sequential = try source.copyForIntLong(allocator);
    defer long_sequential.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 16), long_sequential.tableSize());
    try std.testing.expectEqual(@as(usize, 0), long_sequential.occupied_with_sentinels);
    const ordered = try long_sequential.orderedKeys(allocator);
    defer allocator.free(ordered);
    try std.testing.expectEqualSlices(i32, &.{ 33, 2 }, ordered);
}

test "raw replacement and lazy history materialization are OOM atomic" {
    const allocator = std.testing.allocator;
    var layout = try Layout.init(allocator);
    defer layout.deinit(allocator);
    for ([_]i32{ 2, 17, 33 }) |key| _ = try layout.put(allocator, key);
    var before = try layout.exportRaw(allocator);
    defer before.deinit(allocator);

    var replacement_source = try Layout.initWithExpectedSize(allocator, 20);
    defer replacement_source.deinit(allocator);
    _ = try replacement_source.put(allocator, 99);
    var replacement_raw = try replacement_source.exportRaw(allocator);
    defer replacement_raw.deinit(allocator);

    var failing = std.testing.FailingAllocator.init(allocator, .{ .fail_index = 0 });
    try std.testing.expectError(
        error.OutOfMemory,
        layout.replaceFromRaw(failing.allocator(), &replacement_raw),
    );
    var after = try layout.exportRaw(allocator);
    defer after.deinit(allocator);
    try std.testing.expectEqualSlices(i32, before.slots, after.slots);

    var history = History.init();
    defer history.deinit(allocator);
    var history_failing = std.testing.FailingAllocator.init(
        allocator,
        .{ .fail_index = 0 },
    );
    try std.testing.expectError(
        error.OutOfMemory,
        history.recordPut(history_failing.allocator(), 42),
    );
    try std.testing.expect(history == .lazy_known_empty);
}

test "history raw replacement is valid and failure atomic" {
    const allocator = std.testing.allocator;
    var original = try Layout.init(allocator);
    for ([_]i32{ 2, 17, 33 }) |key| _ = try original.put(allocator, key);
    var history = History.initKnown(original);
    defer history.deinit(allocator);
    var before = try history.exportRaw(allocator);
    defer before.deinit(allocator);

    var replacement_layout = try Layout.initWithExpectedSize(allocator, 20);
    defer replacement_layout.deinit(allocator);
    for ([_]i32{ 0, 1, 42, 99 }) |key| {
        _ = try replacement_layout.put(allocator, key);
    }
    var replacement_raw = try replacement_layout.exportRaw(allocator);
    defer replacement_raw.deinit(allocator);

    var failing = std.testing.FailingAllocator.init(
        allocator,
        .{ .fail_index = 0 },
    );
    try std.testing.expectError(
        error.OutOfMemory,
        history.replaceFromRaw(failing.allocator(), &replacement_raw),
    );
    var after_oom = try history.exportRaw(allocator);
    defer after_oom.deinit(allocator);
    try std.testing.expectEqual(before.table_length, after_oom.table_length);
    try std.testing.expectEqual(before.zero_key_flag, after_oom.zero_key_flag);
    try std.testing.expectEqual(before.one_key_flag, after_oom.one_key_flag);
    try std.testing.expectEqualSlices(i32, before.slots, after_oom.slots);

    var invalid_raw = try replacement_raw.clone(allocator);
    defer invalid_raw.deinit(allocator);
    invalid_raw.one_key_flag = 2;
    try std.testing.expectError(
        error.InvalidFlag,
        history.replaceFromRaw(allocator, &invalid_raw),
    );
    var after_invalid = try history.exportRaw(allocator);
    defer after_invalid.deinit(allocator);
    try std.testing.expectEqual(before.table_length, after_invalid.table_length);
    try std.testing.expectEqual(before.zero_key_flag, after_invalid.zero_key_flag);
    try std.testing.expectEqual(before.one_key_flag, after_invalid.one_key_flag);
    try std.testing.expectEqualSlices(i32, before.slots, after_invalid.slots);

    try history.replaceFromRaw(allocator, &replacement_raw);
    var after_valid = try history.exportRaw(allocator);
    defer after_valid.deinit(allocator);
    try std.testing.expectEqual(
        replacement_raw.table_length,
        after_valid.table_length,
    );
    try std.testing.expectEqual(
        replacement_raw.zero_key_flag,
        after_valid.zero_key_flag,
    );
    try std.testing.expectEqual(
        replacement_raw.one_key_flag,
        after_valid.one_key_flag,
    );
    try std.testing.expectEqualSlices(
        i32,
        replacement_raw.slots,
        after_valid.slots,
    );
}
