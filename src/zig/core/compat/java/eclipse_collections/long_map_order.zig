//! Eclipse Collections 11.0.0 `LongObjectHashMap` key-table layout.
//!
//! EC visits active keys `0` and `1` first, followed by occupied backing-table
//! slots in ascending index order. The slot layout depends on insertion,
//! removal, growth, compaction, and clear history. This module records that
//! layout without owning map values so Java-compatible report traversal can be
//! reproduced exactly.

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
/// includes empty slots (`0`) and removal tombstones (`1`); callers own it.
/// Active sentinel keys are represented separately by the two flags.
pub const RawSnapshot = struct {
    table_length: usize,
    zero_key_flag: u8,
    one_key_flag: u8,
    slots: []i64,

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
            .slots = try allocator.dupe(i64, self.slots),
        };
    }
};

pub const Layout = struct {
    const empty_key: i64 = 0;
    const removed_key: i64 = 1;
    // EC 11 uses half of a 64-byte cache line for the first two probe phases.
    const initial_linear_probe: usize = 4;

    keys: []i64,
    occupied_with_data: usize = 0,
    occupied_with_sentinels: usize = 0,
    contains_zero_key: bool = false,
    contains_one_key: bool = false,

    /// EC's default constructor allocates sixteen backing slots.
    pub fn init(allocator: std.mem.Allocator) std.mem.Allocator.Error!Layout {
        return initTable(allocator, default_table_size);
    }

    /// Models `new LongObjectHashMap<>(expected_size)`.
    pub fn initWithExpectedSize(
        allocator: std.mem.Allocator,
        expected_size: usize,
    ) LayoutError!Layout {
        return initTable(allocator, try tableSizeForExpectedSize(expected_size));
    }

    /// Reconstructs a Java map from its serialized encounter sequence.
    pub fn initFromEncounterOrder(
        allocator: std.mem.Allocator,
        expected_size: usize,
        encounter_keys: []const i64,
    ) LayoutError!Layout {
        var result = try initWithExpectedSize(allocator, expected_size);
        errdefer result.deinit(allocator);
        for (encounter_keys) |key| _ = try result.put(allocator, key);
        return result;
    }

    fn initTable(
        allocator: std.mem.Allocator,
        table_size: usize,
    ) std.mem.Allocator.Error!Layout {
        std.debug.assert(table_size != 0);
        std.debug.assert(table_size <= max_table_size);
        std.debug.assert(std.math.isPowerOfTwo(table_size));
        const keys = try allocator.alloc(i64, table_size);
        @memset(keys, empty_key);
        return .{ .keys = keys };
    }

    pub fn deinit(self: *Layout, allocator: std.mem.Allocator) void {
        allocator.free(self.keys);
        self.* = undefined;
    }

    pub fn clone(
        self: *const Layout,
        allocator: std.mem.Allocator,
    ) std.mem.Allocator.Error!Layout {
        return .{
            .keys = try allocator.dupe(i64, self.keys),
            .occupied_with_data = self.occupied_with_data,
            .occupied_with_sentinels = self.occupied_with_sentinels,
            .contains_zero_key = self.contains_zero_key,
            .contains_one_key = self.contains_one_key,
        };
    }

    /// EC's `LongObjectHashMap(LongObjectMap)` always creates a map sized for
    /// at least eight entries and then inserts the source encounter sequence.
    pub fn copyForLongObject(
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
            .slots = try allocator.dupe(i64, self.keys),
        };
    }

    /// Validates and imports a complete backing table. Validation rejects
    /// layouts that EC cannot produce, including duplicate or unreachable
    /// active keys and occupancy beyond EC's one-half threshold.
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
            .keys = try allocator.dupe(i64, raw.slots),
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
                if (result.keys[reached] == key) {
                    return error.DuplicateActiveKey;
                }
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
    /// Java map entry does not move its backing-table key.
    pub fn put(
        self: *Layout,
        allocator: std.mem.Allocator,
        key: i64,
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
        const next_data = std.math.add(usize, self.occupied_with_data, 1) catch
            return error.CapacityOverflow;
        const next_sentinels = self.occupied_with_sentinels -
            @intFromBool(replaces_removed);
        if (next_data + next_sentinels > self.maxOccupiedWithData()) {
            // Java inserts first and grows afterward. Stage that same logical
            // state so allocation failure cannot partially mutate live state.
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

    pub fn remove(self: *Layout, key: i64) bool {
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

    /// EC retains the allocated table when clearing a map.
    pub fn clear(self: *Layout) void {
        @memset(self.keys, empty_key);
        self.occupied_with_data = 0;
        self.occupied_with_sentinels = 0;
        self.contains_zero_key = false;
        self.contains_one_key = false;
    }

    pub fn compact(self: *Layout, allocator: std.mem.Allocator) LayoutError!void {
        try self.rehash(
            allocator,
            try smallestPowerOfTwoAtLeast(self.count()),
        );
    }

    pub fn orderedKeys(
        self: *const Layout,
        allocator: std.mem.Allocator,
    ) std.mem.Allocator.Error![]i64 {
        const ordered = try allocator.alloc(i64, self.count());
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

    fn insertAt(self: *Layout, index: usize, key: i64) void {
        if (self.keys[index] == removed_key) {
            self.occupied_with_sentinels -= 1;
        }
        self.keys[index] = key;
        self.occupied_with_data += 1;
    }

    fn rehashAndGrow(
        self: *Layout,
        allocator: std.mem.Allocator,
    ) LayoutError!void {
        const maximum_occupied = self.maxOccupiedWithData();
        const next_data = std.math.add(usize, self.occupied_with_data, 1) catch
            return error.CapacityOverflow;
        const doubled = std.math.mul(usize, next_data, 2) catch
            return error.CapacityOverflow;
        var new_size = @max(
            maximum_occupied,
            try smallestPowerOfTwoAtLeast(doubled),
        );
        if (self.occupied_with_sentinels > 0 and
            maximum_occupied / 2 + maximum_occupied / 4 <
                self.occupied_with_data)
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

    fn probe(self: *const Layout, key: i64) usize {
        const mask = self.keys.len - 1;
        const initial_index = lowBits(key) & mask;
        var current = self.keys[initial_index];
        if (current == key or current == empty_key) return initial_index;

        var first_removed: ?usize =
            if (current == removed_key) initial_index else null;
        for (1..initial_linear_probe) |offset| {
            const index = (initial_index + offset) & mask;
            current = self.keys[index];
            if (current == key) return index;
            if (current == empty_key) return first_removed orelse index;
            if (current == removed_key and first_removed == null) {
                first_removed = index;
            }
        }
        return self.probeTwo(key, first_removed);
    }

    fn probeTwo(
        self: *const Layout,
        key: i64,
        first_removed_input: ?usize,
    ) usize {
        const mask = self.keys.len - 1;
        const spread_index = lowBitsFromUnsigned(spreadTwo(key)) & mask;
        var first_removed = first_removed_input;
        for (0..initial_linear_probe) |offset| {
            const index = (spread_index + offset) & mask;
            const current = self.keys[index];
            if (current == key) return index;
            if (current == empty_key) return first_removed orelse index;
            if (current == removed_key and first_removed == null) {
                first_removed = index;
            }
        }
        return self.probeThree(key, first_removed);
    }

    fn probeThree(
        self: *const Layout,
        key: i64,
        first_removed_input: ?usize,
    ) usize {
        const mask = self.keys.len - 1;
        var spread_index: u32 = @truncate(spreadOne(key));
        const step: u32 = @as(u32, @truncate(@bitReverse(spreadTwo(key)))) | 1;
        var first_removed = first_removed_input;
        while (true) {
            spread_index +%= step;
            const index = @as(usize, spread_index) & mask;
            const current = self.keys[index];
            if (current == key) return index;
            if (current == empty_key) return first_removed orelse index;
            if (current == removed_key and first_removed == null) {
                first_removed = index;
            }
        }
    }
};

/// Tracks whether byte-exact EC history is available. A known empty map keeps
/// its exact table capacity without allocating until the first insertion.
/// Once unavailable, mutation deliberately keeps history unavailable instead
/// of inventing a plausible but incorrect layout.
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
        encounter_keys: []const i64,
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

    pub fn copyForLongObject(
        self: *const History,
        allocator: std.mem.Allocator,
    ) LayoutError!History {
        return switch (self.*) {
            .lazy_known_empty => try initWithExpectedSize(default_expected_size),
            .unavailable => .unavailable,
            .known => |*layout| .{
                .known = try layout.copyForLongObject(allocator),
            },
        };
    }

    pub fn recordPut(
        self: *History,
        allocator: std.mem.Allocator,
        key: i64,
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

    pub fn recordRemove(self: *History, key: i64) void {
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

    pub fn orderedKeys(
        self: *const History,
        allocator: std.mem.Allocator,
    ) HistoryError![]i64 {
        return switch (self.*) {
            .unavailable => error.HistoryUnavailable,
            .lazy_known_empty => try allocator.alloc(i64, 0),
            .known => |*layout| try layout.orderedKeys(allocator),
        };
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
        var replacement = try importRaw(allocator, raw);
        errdefer replacement.deinit(allocator);
        self.deinit(allocator);
        self.* = replacement;
    }
};

fn isNonSentinel(key: i64) bool {
    return key != 0 and key != 1;
}

fn tableSizeForExpectedSize(
    expected_size: usize,
) error{CapacityOverflow}!usize {
    const doubled = std.math.mul(usize, expected_size, 2) catch
        return error.CapacityOverflow;
    if (doubled > max_table_size) return error.CapacityOverflow;

    // EC converts the doubled Java int through float before its fast-ceil
    // helper. Preserve that rounding behavior for large, but valid, sizes.
    const doubled_float: f32 = @floatFromInt(doubled);
    const java_fast_ceil: usize = @intFromFloat(doubled_float);
    return smallestPowerOfTwoAtLeast(java_fast_ceil);
}

fn smallestPowerOfTwoAtLeast(
    value: usize,
) error{CapacityOverflow}!usize {
    if (value <= 1) return 1;
    const result = std.math.ceilPowerOfTwo(usize, value) catch
        return error.CapacityOverflow;
    if (result > max_table_size) return error.CapacityOverflow;
    return result;
}

fn lowBits(key: i64) usize {
    const bits: u64 = @bitCast(key);
    return lowBitsFromUnsigned(bits);
}

fn lowBitsFromUnsigned(bits: u64) usize {
    return @as(usize, @as(u32, @truncate(bits)));
}

fn spreadOne(key: i64) u64 {
    var spread: u64 = @bitCast(key);
    spread ^= spread >> 28;
    spread *%= @bitCast(@as(i64, -4_254_747_342_703_917_655));
    spread ^= spread >> 43;
    spread *%= @bitCast(@as(i64, -908_430_792_394_475_837));
    spread ^= spread >> 23;
    return spread;
}

fn spreadTwo(key: i64) u64 {
    var spread: u64 = @bitCast(key);
    spread ^= spread >> 23;
    spread *%= @bitCast(@as(i64, -6_261_870_919_139_520_145));
    spread ^= spread >> 39;
    spread *%= 2_747_051_607_443_084_853;
    spread ^= spread >> 37;
    return spread;
}

fn expectInsertionOrder(
    insertions: []const i64,
    expected: []const i64,
) !void {
    const allocator = std.testing.allocator;
    var layout = try Layout.init(allocator);
    defer layout.deinit(allocator);
    for (insertions) |key| {
        try std.testing.expect(try layout.put(allocator, key));
    }
    const ordered = try layout.orderedKeys(allocator);
    defer allocator.free(ordered);
    try std.testing.expectEqualSlices(i64, expected, ordered);
}

test "EC 11 long high-bit collision and spread vectors match Java" {
    const allocator = std.testing.allocator;
    const collision_keys = [_]i64{
        2,
        4_294_967_298,
        8_589_934_594,
        12_884_901_890,
        17_179_869_186,
        21_474_836_482,
    };
    var layout = try Layout.init(allocator);
    defer layout.deinit(allocator);
    for (collision_keys) |key| _ = try layout.put(allocator, key);

    const expected_slots = [_]i64{
        0,             0,              2,              4_294_967_298,
        8_589_934_594, 12_884_901_890, 21_474_836_482, 0,
        0,             0,              0,              0,
        0,             0,              17_179_869_186, 0,
    };
    try std.testing.expectEqualSlices(i64, &expected_slots, layout.keys);
    const ordered = try layout.orderedKeys(allocator);
    defer allocator.free(ordered);
    try std.testing.expectEqualSlices(
        i64,
        &.{
            2,
            4_294_967_298,
            8_589_934_594,
            12_884_901_890,
            21_474_836_482,
            17_179_869_186,
        },
        ordered,
    );
}

test "EC 11 third probe phase and growth match Java" {
    const allocator = std.testing.allocator;
    const collision_keys = [_]i64{
        12_884_901_890,
        51_539_607_554,
        450_971_566_082,
        725_849_473_026,
        906_238_099_458,
        1_073_741_824_002,
        1_202_590_842_882,
        1_331_439_861_762,
        1_404_454_305_794,
    };
    var layout = try Layout.init(allocator);
    defer layout.deinit(allocator);
    for (collision_keys) |key| _ = try layout.put(allocator, key);

    try std.testing.expectEqual(@as(usize, 32), layout.tableSize());
    try std.testing.expectEqualSlices(
        i64,
        &.{
            450_971_566_082,   725_849_473_026,   906_238_099_458,
            1_073_741_824_002, 12_884_901_890,    51_539_607_554,
            0,                 0,                 0,
            0,                 0,                 0,
            0,                 0,                 0,
            0,                 0,                 0,
            0,                 0,                 0,
            0,                 1_202_590_842_882, 0,
            0,                 0,                 1_404_454_305_794,
            0,                 0,                 1_331_439_861_762,
            0,                 0,
        },
        layout.keys,
    );
    const ordered = try layout.orderedKeys(allocator);
    defer allocator.free(ordered);
    try std.testing.expectEqualSlices(
        i64,
        &.{
            450_971_566_082,
            725_849_473_026,
            906_238_099_458,
            1_073_741_824_002,
            12_884_901_890,
            51_539_607_554,
            1_202_590_842_882,
            1_404_454_305_794,
            1_331_439_861_762,
        },
        ordered,
    );
}

test "EC sentinel keys precede long backing slots and updates do not move" {
    const allocator = std.testing.allocator;
    var layout = try Layout.init(allocator);
    defer layout.deinit(allocator);
    for ([_]i64{ 2, 4_294_967_298, 8_589_934_594, 1, 0 }) |key| {
        try std.testing.expect(try layout.put(allocator, key));
    }
    try std.testing.expect(!try layout.put(allocator, 4_294_967_298));

    const ordered = try layout.orderedKeys(allocator);
    defer allocator.free(ordered);
    try std.testing.expectEqualSlices(
        i64,
        &.{ 0, 1, 2, 4_294_967_298, 8_589_934_594 },
        ordered,
    );
    try std.testing.expect(layout.remove(0));
    try std.testing.expect(layout.remove(1));
    try std.testing.expect(!layout.remove(0));
}

test "EC removal tombstones and reinsertion preserve exact long history" {
    const allocator = std.testing.allocator;
    var layout = try Layout.init(allocator);
    defer layout.deinit(allocator);
    for ([_]i64{
        2,
        4_294_967_298,
        8_589_934_594,
        12_884_901_890,
        17_179_869_186,
        21_474_836_482,
    }) |key| {
        _ = try layout.put(allocator, key);
    }
    try std.testing.expect(layout.remove(4_294_967_298));
    try std.testing.expect(layout.remove(12_884_901_890));
    try std.testing.expect(!layout.remove(4_294_967_298));
    try std.testing.expectEqualSlices(
        i64,
        &.{
            0,             0, 2,              1,
            8_589_934_594, 1, 21_474_836_482, 0,
            0,             0, 0,              0,
            0,             0, 17_179_869_186, 0,
        },
        layout.keys,
    );

    _ = try layout.put(allocator, 12_884_901_890);
    _ = try layout.put(allocator, 4_294_967_298);
    try std.testing.expectEqualSlices(
        i64,
        &.{
            0,             0,             2,              12_884_901_890,
            8_589_934_594, 4_294_967_298, 21_474_836_482, 0,
            0,             0,             0,              0,
            0,             0,             17_179_869_186, 0,
        },
        layout.keys,
    );
}

test "EC long growth compact and negative-key vectors match Java" {
    const allocator = std.testing.allocator;
    var growth = try Layout.init(allocator);
    defer growth.deinit(allocator);
    for (2..11) |key| _ = try growth.put(allocator, @intCast(key));
    try std.testing.expectEqual(@as(usize, 32), growth.tableSize());

    try std.testing.expect(growth.remove(4));
    try std.testing.expect(growth.remove(7));
    _ = try growth.put(allocator, 4_294_967_298);
    _ = try growth.put(allocator, 8_589_934_594);
    const expected_reused = [_]i64{
        0, 0, 2,  3, 4_294_967_298, 5, 6, 8_589_934_594,
        8, 9, 10, 0, 0,             0, 0, 0,
        0, 0, 0,  0, 0,             0, 0, 0,
        0, 0, 0,  0, 0,             0, 0, 0,
    };
    try std.testing.expectEqualSlices(i64, &expected_reused, growth.keys);
    try growth.compact(allocator);
    try std.testing.expectEqual(@as(usize, 32), growth.tableSize());
    try std.testing.expectEqualSlices(i64, &expected_reused, growth.keys);

    var negative = try Layout.init(allocator);
    defer negative.deinit(allocator);
    for ([_]i64{
        std.math.minInt(i64),
        -4_294_967_294,
        -2,
        4_294_967_298,
        8_589_934_594,
    }) |key| {
        _ = try negative.put(allocator, key);
    }
    try std.testing.expectEqualSlices(
        i64,
        &.{
            std.math.minInt(i64), 0, -4_294_967_294, 4_294_967_298,
            8_589_934_594,        0, 0,              0,
            0,                    0, 0,              0,
            0,                    0, -2,             0,
        },
        negative.keys,
    );
}

test "sized constructors include user-profile capacity and clear retains it" {
    const allocator = std.testing.allocator;
    var user_profiles = try Layout.initWithExpectedSize(allocator, 1024);
    defer user_profiles.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 2048), user_profiles.tableSize());
    _ = try user_profiles.put(allocator, 2);
    try std.testing.expect(user_profiles.remove(2));
    user_profiles.clear();
    try std.testing.expectEqual(@as(usize, 2048), user_profiles.tableSize());
    try std.testing.expectEqual(@as(usize, 0), user_profiles.count());
    for (user_profiles.keys) |slot| {
        try std.testing.expectEqual(@as(i64, 0), slot);
    }

    var zero_sized = try Layout.initWithExpectedSize(allocator, 0);
    defer zero_sized.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), zero_sized.tableSize());
    _ = try zero_sized.put(allocator, 2);
    try std.testing.expectEqual(@as(usize, 4), zero_sized.tableSize());
}

test "LongObject copy constructor uses minimum expected size eight" {
    const allocator = std.testing.allocator;
    var source = try Layout.initWithExpectedSize(allocator, 1024);
    defer source.deinit(allocator);
    for ([_]i64{ 0, 1, 2, 4_294_967_298, 8_589_934_594 }) |key| {
        _ = try source.put(allocator, key);
    }

    var copy = try source.copyForLongObject(allocator);
    defer copy.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 16), copy.tableSize());
    const ordered = try copy.orderedKeys(allocator);
    defer allocator.free(ordered);
    try std.testing.expectEqualSlices(
        i64,
        &.{ 0, 1, 2, 4_294_967_298, 8_589_934_594 },
        ordered,
    );
}

test "raw long snapshot roundtrip preserves slots and rejects tampering" {
    const allocator = std.testing.allocator;
    var layout = try Layout.init(allocator);
    defer layout.deinit(allocator);
    for ([_]i64{
        0,
        1,
        2,
        4_294_967_298,
        8_589_934_594,
        12_884_901_890,
        17_179_869_186,
        21_474_836_482,
    }) |key| {
        _ = try layout.put(allocator, key);
    }
    try std.testing.expect(layout.remove(4_294_967_298));

    var raw = try layout.exportRaw(allocator);
    defer raw.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 16), raw.table_length);
    try std.testing.expectEqual(@as(u8, 1), raw.zero_key_flag);
    try std.testing.expectEqual(@as(u8, 1), raw.one_key_flag);

    var restored = try Layout.importRaw(allocator, &raw);
    defer restored.deinit(allocator);
    var restored_raw = try restored.exportRaw(allocator);
    defer restored_raw.deinit(allocator);
    try std.testing.expectEqual(raw.table_length, restored_raw.table_length);
    try std.testing.expectEqual(raw.zero_key_flag, restored_raw.zero_key_flag);
    try std.testing.expectEqual(raw.one_key_flag, restored_raw.one_key_flag);
    try std.testing.expectEqualSlices(i64, raw.slots, restored_raw.slots);

    var bad_flag = try raw.clone(allocator);
    defer bad_flag.deinit(allocator);
    bad_flag.one_key_flag = 2;
    try std.testing.expectError(
        error.InvalidFlag,
        Layout.importRaw(allocator, &bad_flag),
    );

    var bad_size = try raw.clone(allocator);
    defer bad_size.deinit(allocator);
    bad_size.table_length = 15;
    try std.testing.expectError(
        error.InvalidTableSize,
        Layout.importRaw(allocator, &bad_size),
    );

    const active_index = std.mem.indexOfScalar(i64, raw.slots, 2).?;
    const empty_index = std.mem.indexOfScalar(i64, raw.slots, 0).?;

    var duplicate = try raw.clone(allocator);
    defer duplicate.deinit(allocator);
    duplicate.slots[empty_index] = 2;
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

    var overfull = try raw.clone(allocator);
    defer overfull.deinit(allocator);
    var occupied = layout.occupied_with_data + layout.occupied_with_sentinels;
    for (overfull.slots) |*slot| {
        if (occupied > overfull.table_length / 2) break;
        if (slot.* == 0) {
            slot.* = 1;
            occupied += 1;
        }
    }
    try std.testing.expectError(
        error.InvalidOccupancy,
        Layout.importRaw(allocator, &overfull),
    );
}

test "history preserves sized lazy empty and propagates unavailable" {
    const allocator = std.testing.allocator;
    var user_profiles = try History.initWithExpectedSize(1024);
    defer user_profiles.deinit(allocator);
    try std.testing.expect(user_profiles.isKnown());
    try std.testing.expectEqual(
        @as(?usize, 2048),
        user_profiles.tableSizeIfKnown(),
    );
    var empty_raw = try user_profiles.exportRaw(allocator);
    defer empty_raw.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 2048), empty_raw.table_length);

    try user_profiles.recordPut(allocator, 42);
    try std.testing.expectEqual(
        @as(?usize, 2048),
        user_profiles.tableSizeIfKnown(),
    );
    user_profiles.recordRemove(42);
    user_profiles.clear();
    try std.testing.expectEqual(
        @as(?usize, 2048),
        user_profiles.tableSizeIfKnown(),
    );

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
    try std.testing.expectError(
        error.HistoryUnavailable,
        unavailable.orderedKeys(allocator),
    );

    var copied = try unavailable.copyForLongObject(allocator);
    defer copied.deinit(allocator);
    try std.testing.expect(!copied.isKnown());
}

test "growth raw replacement and lazy materialization are OOM atomic" {
    const allocator = std.testing.allocator;
    var layout = try Layout.init(allocator);
    defer layout.deinit(allocator);
    for (2..10) |key| _ = try layout.put(allocator, @intCast(key));
    var before = try layout.exportRaw(allocator);
    defer before.deinit(allocator);

    for (0..2) |fail_index| {
        var failing = std.testing.FailingAllocator.init(
            allocator,
            .{ .fail_index = fail_index },
        );
        try std.testing.expectError(
            error.OutOfMemory,
            layout.put(failing.allocator(), 10),
        );
        var after = try layout.exportRaw(allocator);
        defer after.deinit(allocator);
        try std.testing.expectEqual(before.table_length, after.table_length);
        try std.testing.expectEqualSlices(i64, before.slots, after.slots);
    }

    var replacement_source = try Layout.initWithExpectedSize(allocator, 20);
    defer replacement_source.deinit(allocator);
    _ = try replacement_source.put(allocator, 99);
    var replacement_raw = try replacement_source.exportRaw(allocator);
    defer replacement_raw.deinit(allocator);
    var replace_failing = std.testing.FailingAllocator.init(
        allocator,
        .{ .fail_index = 0 },
    );
    try std.testing.expectError(
        error.OutOfMemory,
        layout.replaceFromRaw(replace_failing.allocator(), &replacement_raw),
    );
    var after_replace = try layout.exportRaw(allocator);
    defer after_replace.deinit(allocator);
    try std.testing.expectEqualSlices(i64, before.slots, after_replace.slots);

    var history = try History.initWithExpectedSize(1024);
    defer history.deinit(allocator);
    var history_failing = std.testing.FailingAllocator.init(
        allocator,
        .{ .fail_index = 0 },
    );
    try std.testing.expectError(
        error.OutOfMemory,
        history.recordPut(history_failing.allocator(), 42),
    );
    try std.testing.expectEqual(@as(?usize, 2048), history.tableSizeIfKnown());
    try std.testing.expect(history == .lazy_known_empty);
}

test "encounter reconstruction uses supplied sized constructor" {
    const allocator = std.testing.allocator;
    const encounter = [_]i64{
        0,
        1,
        2,
        4_294_967_298,
        8_589_934_594,
        12_884_901_890,
    };
    var layout = try Layout.initFromEncounterOrder(
        allocator,
        encounter.len,
        &encounter,
    );
    defer layout.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 16), layout.tableSize());
    const ordered = try layout.orderedKeys(allocator);
    defer allocator.free(ordered);
    try std.testing.expectEqualSlices(i64, &encounter, ordered);

    try expectInsertionOrder(
        &.{ 4_294_967_298, 8_589_934_594, 2 },
        &.{ 4_294_967_298, 8_589_934_594, 2 },
    );
}

test "history encounter reconstruction preserves Java sized insertion order" {
    const allocator = std.testing.allocator;
    const encounter = [_]i64{
        0,
        1,
        2,
        4_294_967_298,
        8_589_934_594,
        12_884_901_890,
    };
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
    try std.testing.expectEqualSlices(i64, &encounter, ordered);

    var empty = try History.initFromEncounterOrder(allocator, 0, &.{});
    defer empty.deinit(allocator);
    try std.testing.expect(empty == .lazy_known_empty);
    try std.testing.expectEqual(@as(?usize, 1), empty.tableSizeIfKnown());
}
