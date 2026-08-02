//! Bounded, canonical persistence codec for exact Eclipse Collections map
//! layout histories.
//!
//! Each history is encoded independently:
//!   * `0`: exact layout history is unavailable.
//!   * `1`: exact layout history follows as a little-endian table length,
//!     the two EC sentinel-key flags, and every backing-table slot.
//!
//! Empty slots and removal tombstones are intentionally retained. Decoding
//! stages the wire slots in owned memory, then delegates semantic validation to
//! the corresponding `History.importRaw` implementation before returning a
//! separately owned history.

const std = @import("std");
const int_order = @import("int_map_order.zig");
const long_order = @import("long_map_order.zig");

const unavailable_tag: u8 = 0;
const exact_known_tag: u8 = 1;
const known_header_len: usize = 1 + @sizeOf(u32) + 1 + 1;
const ec_max_slots: usize = 1 << 30;

pub const Limits = struct {
    /// Maximum number of backing-table slots accepted or emitted for one
    /// history. A zero limit still permits the one-byte unavailable form.
    max_slots: usize,
};

pub const CodecError = std.mem.Allocator.Error || error{
    UnknownTag,
    Truncated,
    TrailingBytes,
    SlotLimitExceeded,
    LengthOverflow,
    InvalidTableSize,
    InvalidFlag,
    InvalidOccupancy,
    DuplicateActiveKey,
    UnreachableActiveKey,
    CapacityOverflow,
};

const Parsed = union(enum) {
    unavailable,
    known: struct {
        table_length: usize,
        zero_key_flag: u8,
        one_key_flag: u8,
        slot_bytes: []const u8,
    },
};

pub fn encodedIntLength(
    history: *const int_order.History,
    limits: Limits,
) CodecError!usize {
    const table_length = history.tableSizeIfKnown() orelse return 1;
    return knownEncodedLength(table_length, @sizeOf(i32), limits);
}

pub fn encodedLongLength(
    history: *const long_order.History,
    limits: Limits,
) CodecError!usize {
    const table_length = history.tableSizeIfKnown() orelse return 1;
    return knownEncodedLength(table_length, @sizeOf(i64), limits);
}

/// Returns an owned canonical encoding. The caller owns the returned bytes.
pub fn encodeIntAlloc(
    allocator: std.mem.Allocator,
    history: *const int_order.History,
    limits: Limits,
) CodecError![]u8 {
    const encoded_len = try encodedIntLength(history, limits);
    if (!history.isKnown()) {
        const output = try allocator.alloc(u8, encoded_len);
        output[0] = unavailable_tag;
        return output;
    }

    var raw = history.exportRaw(allocator) catch |err| switch (err) {
        error.HistoryUnavailable => unreachable,
        error.OutOfMemory => return error.OutOfMemory,
        error.CapacityOverflow => return error.CapacityOverflow,
        error.InvalidTableSize => return error.InvalidTableSize,
        error.InvalidFlag => return error.InvalidFlag,
        error.InvalidOccupancy => return error.InvalidOccupancy,
        error.DuplicateActiveKey => return error.DuplicateActiveKey,
        error.UnreachableActiveKey => return error.UnreachableActiveKey,
    };
    defer raw.deinit(allocator);

    const output = try allocator.alloc(u8, encoded_len);
    writeKnown(i32, output, raw.table_length, raw.zero_key_flag, raw.one_key_flag, raw.slots);
    return output;
}

/// Returns an owned canonical encoding. The caller owns the returned bytes.
pub fn encodeLongAlloc(
    allocator: std.mem.Allocator,
    history: *const long_order.History,
    limits: Limits,
) CodecError![]u8 {
    const encoded_len = try encodedLongLength(history, limits);
    if (!history.isKnown()) {
        const output = try allocator.alloc(u8, encoded_len);
        output[0] = unavailable_tag;
        return output;
    }

    var raw = history.exportRaw(allocator) catch |err| switch (err) {
        error.HistoryUnavailable => unreachable,
        error.OutOfMemory => return error.OutOfMemory,
        error.CapacityOverflow => return error.CapacityOverflow,
        error.InvalidTableSize => return error.InvalidTableSize,
        error.InvalidFlag => return error.InvalidFlag,
        error.InvalidOccupancy => return error.InvalidOccupancy,
        error.DuplicateActiveKey => return error.DuplicateActiveKey,
        error.UnreachableActiveKey => return error.UnreachableActiveKey,
    };
    defer raw.deinit(allocator);

    const output = try allocator.alloc(u8, encoded_len);
    writeKnown(i64, output, raw.table_length, raw.zero_key_flag, raw.one_key_flag, raw.slots);
    return output;
}

/// Decodes either unavailable or exact-known state into an owned history.
pub fn decodeIntExactAlloc(
    allocator: std.mem.Allocator,
    bytes: []const u8,
    limits: Limits,
) CodecError!int_order.History {
    return switch (try parse(bytes, @sizeOf(i32), limits)) {
        .unavailable => int_order.History.initUnavailable(),
        .known => |known| blk: {
            const slots = try allocator.alloc(i32, known.table_length);
            defer allocator.free(slots);
            decodeSlots(i32, slots, known.slot_bytes);

            const raw = int_order.RawSnapshot{
                .table_length = known.table_length,
                .zero_key_flag = known.zero_key_flag,
                .one_key_flag = known.one_key_flag,
                .slots = slots,
            };
            break :blk try int_order.History.importRaw(allocator, &raw);
        },
    };
}

/// Decodes either unavailable or exact-known state into an owned history.
pub fn decodeLongExactAlloc(
    allocator: std.mem.Allocator,
    bytes: []const u8,
    limits: Limits,
) CodecError!long_order.History {
    return switch (try parse(bytes, @sizeOf(i64), limits)) {
        .unavailable => long_order.History.initUnavailable(),
        .known => |known| blk: {
            const slots = try allocator.alloc(i64, known.table_length);
            defer allocator.free(slots);
            decodeSlots(i64, slots, known.slot_bytes);

            const raw = long_order.RawSnapshot{
                .table_length = known.table_length,
                .zero_key_flag = known.zero_key_flag,
                .one_key_flag = known.one_key_flag,
                .slots = slots,
            };
            break :blk try long_order.History.importRaw(allocator, &raw);
        },
    };
}

fn knownEncodedLength(
    table_length: usize,
    key_size: usize,
    limits: Limits,
) CodecError!usize {
    try validateTableLengthBeforeAllocation(table_length, limits);
    const slots_len = std.math.mul(usize, table_length, key_size) catch
        return error.LengthOverflow;
    return std.math.add(usize, known_header_len, slots_len) catch
        return error.LengthOverflow;
}

fn validateTableLengthBeforeAllocation(
    table_length: usize,
    limits: Limits,
) CodecError!void {
    if (table_length > limits.max_slots) return error.SlotLimitExceeded;
    if (table_length == 0 or
        table_length > ec_max_slots or
        !std.math.isPowerOfTwo(table_length))
    {
        return error.InvalidTableSize;
    }
}

fn parse(
    bytes: []const u8,
    key_size: usize,
    limits: Limits,
) CodecError!Parsed {
    if (bytes.len == 0) return error.Truncated;

    return switch (bytes[0]) {
        unavailable_tag => blk: {
            if (bytes.len != 1) return error.TrailingBytes;
            break :blk .unavailable;
        },
        exact_known_tag => blk: {
            if (bytes.len < known_header_len) return error.Truncated;

            const table_length_u32 = std.mem.readInt(
                u32,
                bytes[1..][0..@sizeOf(u32)],
                .little,
            );
            const table_length: usize = table_length_u32;
            const expected_len = try knownEncodedLength(
                table_length,
                key_size,
                limits,
            );
            if (bytes.len < expected_len) return error.Truncated;
            if (bytes.len > expected_len) return error.TrailingBytes;

            break :blk .{ .known = .{
                .table_length = table_length,
                .zero_key_flag = bytes[1 + @sizeOf(u32)],
                .one_key_flag = bytes[1 + @sizeOf(u32) + 1],
                .slot_bytes = bytes[known_header_len..],
            } };
        },
        else => error.UnknownTag,
    };
}

fn writeKnown(
    comptime Key: type,
    output: []u8,
    table_length: usize,
    zero_key_flag: u8,
    one_key_flag: u8,
    slots: []const Key,
) void {
    std.debug.assert(slots.len == table_length);
    std.debug.assert(output.len == known_header_len + table_length * @sizeOf(Key));

    output[0] = exact_known_tag;
    std.mem.writeInt(
        u32,
        output[1..][0..@sizeOf(u32)],
        @intCast(table_length),
        .little,
    );
    output[1 + @sizeOf(u32)] = zero_key_flag;
    output[1 + @sizeOf(u32) + 1] = one_key_flag;

    var offset = known_header_len;
    for (slots) |key| {
        std.mem.writeInt(
            Key,
            output[offset..][0..@sizeOf(Key)],
            key,
            .little,
        );
        offset += @sizeOf(Key);
    }
    std.debug.assert(offset == output.len);
}

fn decodeSlots(
    comptime Key: type,
    output: []Key,
    bytes: []const u8,
) void {
    std.debug.assert(bytes.len == output.len * @sizeOf(Key));
    var offset: usize = 0;
    for (output) |*slot| {
        slot.* = std.mem.readInt(
            Key,
            bytes[offset..][0..@sizeOf(Key)],
            .little,
        );
        offset += @sizeOf(Key);
    }
    std.debug.assert(offset == bytes.len);
}

fn expectIntRawEqual(
    expected: *const int_order.History,
    actual: *const int_order.History,
) !void {
    const allocator = std.testing.allocator;
    var expected_raw = try expected.exportRaw(allocator);
    defer expected_raw.deinit(allocator);
    var actual_raw = try actual.exportRaw(allocator);
    defer actual_raw.deinit(allocator);

    try std.testing.expectEqual(expected_raw.table_length, actual_raw.table_length);
    try std.testing.expectEqual(expected_raw.zero_key_flag, actual_raw.zero_key_flag);
    try std.testing.expectEqual(expected_raw.one_key_flag, actual_raw.one_key_flag);
    try std.testing.expectEqualSlices(i32, expected_raw.slots, actual_raw.slots);
}

fn expectLongRawEqual(
    expected: *const long_order.History,
    actual: *const long_order.History,
) !void {
    const allocator = std.testing.allocator;
    var expected_raw = try expected.exportRaw(allocator);
    defer expected_raw.deinit(allocator);
    var actual_raw = try actual.exportRaw(allocator);
    defer actual_raw.deinit(allocator);

    try std.testing.expectEqual(expected_raw.table_length, actual_raw.table_length);
    try std.testing.expectEqual(expected_raw.zero_key_flag, actual_raw.zero_key_flag);
    try std.testing.expectEqual(expected_raw.one_key_flag, actual_raw.one_key_flag);
    try std.testing.expectEqualSlices(i64, expected_raw.slots, actual_raw.slots);
}

test "unavailable histories use the strict one-byte form" {
    const allocator = std.testing.allocator;
    const limits = Limits{ .max_slots = 16 };

    var int_history = int_order.History.initUnavailable();
    defer int_history.deinit(allocator);
    const int_bytes = try encodeIntAlloc(allocator, &int_history, limits);
    defer allocator.free(int_bytes);
    try std.testing.expectEqualSlices(u8, &.{unavailable_tag}, int_bytes);
    try std.testing.expectEqual(@as(usize, 1), try encodedIntLength(&int_history, limits));

    var decoded_int = try decodeIntExactAlloc(
        std.testing.failing_allocator,
        int_bytes,
        .{ .max_slots = 0 },
    );
    defer decoded_int.deinit(std.testing.failing_allocator);
    try std.testing.expect(!decoded_int.isKnown());

    var long_history = long_order.History.initUnavailable();
    defer long_history.deinit(allocator);
    const long_bytes = try encodeLongAlloc(allocator, &long_history, limits);
    defer allocator.free(long_bytes);
    try std.testing.expectEqualSlices(u8, &.{unavailable_tag}, long_bytes);
    try std.testing.expectEqual(@as(usize, 1), try encodedLongLength(&long_history, limits));

    var decoded_long = try decodeLongExactAlloc(
        std.testing.failing_allocator,
        long_bytes,
        .{ .max_slots = 0 },
    );
    defer decoded_long.deinit(std.testing.failing_allocator);
    try std.testing.expect(!decoded_long.isKnown());

    try std.testing.expectError(
        error.TrailingBytes,
        decodeIntExactAlloc(allocator, &.{ unavailable_tag, 0 }, limits),
    );
}

test "zero-sized and default histories retain distinct exact capacities" {
    const allocator = std.testing.allocator;
    const limits = Limits{ .max_slots = 16 };

    var int_zero = try int_order.History.initWithExpectedSize(0);
    defer int_zero.deinit(allocator);
    const int_zero_bytes = try encodeIntAlloc(allocator, &int_zero, limits);
    defer allocator.free(int_zero_bytes);
    try std.testing.expectEqual(@as(usize, known_header_len + @sizeOf(i32)), int_zero_bytes.len);
    try std.testing.expectEqual(
        @as(u32, 1),
        std.mem.readInt(u32, int_zero_bytes[1..][0..@sizeOf(u32)], .little),
    );
    var int_zero_decoded = try decodeIntExactAlloc(allocator, int_zero_bytes, limits);
    defer int_zero_decoded.deinit(allocator);
    try std.testing.expectEqual(@as(?usize, 1), int_zero_decoded.tableSizeIfKnown());
    try expectIntRawEqual(&int_zero, &int_zero_decoded);

    var int_default = int_order.History.init();
    defer int_default.deinit(allocator);
    const int_default_bytes = try encodeIntAlloc(allocator, &int_default, limits);
    defer allocator.free(int_default_bytes);
    try std.testing.expectEqual(
        @as(usize, known_header_len + 16 * @sizeOf(i32)),
        int_default_bytes.len,
    );
    var int_default_decoded = try decodeIntExactAlloc(
        allocator,
        int_default_bytes,
        limits,
    );
    defer int_default_decoded.deinit(allocator);
    try std.testing.expectEqual(
        @as(?usize, 16),
        int_default_decoded.tableSizeIfKnown(),
    );
    try expectIntRawEqual(&int_default, &int_default_decoded);

    var long_zero = try long_order.History.initWithExpectedSize(0);
    defer long_zero.deinit(allocator);
    const long_zero_bytes = try encodeLongAlloc(allocator, &long_zero, limits);
    defer allocator.free(long_zero_bytes);
    try std.testing.expectEqual(@as(usize, known_header_len + @sizeOf(i64)), long_zero_bytes.len);
    var long_zero_decoded = try decodeLongExactAlloc(allocator, long_zero_bytes, limits);
    defer long_zero_decoded.deinit(allocator);
    try std.testing.expectEqual(@as(?usize, 1), long_zero_decoded.tableSizeIfKnown());
    try expectLongRawEqual(&long_zero, &long_zero_decoded);

    var long_default = long_order.History.init();
    defer long_default.deinit(allocator);
    const long_default_bytes = try encodeLongAlloc(allocator, &long_default, limits);
    defer allocator.free(long_default_bytes);
    try std.testing.expectEqual(
        @as(usize, known_header_len + 16 * @sizeOf(i64)),
        long_default_bytes.len,
    );
    var long_default_decoded = try decodeLongExactAlloc(
        allocator,
        long_default_bytes,
        limits,
    );
    defer long_default_decoded.deinit(allocator);
    try std.testing.expectEqual(
        @as(?usize, 16),
        long_default_decoded.tableSizeIfKnown(),
    );
    try expectLongRawEqual(&long_default, &long_default_decoded);
}

test "int exact roundtrip preserves flags tombstones and little-endian slots" {
    const allocator = std.testing.allocator;
    const limits = Limits{ .max_slots = 64 };

    var history = int_order.History.init();
    defer history.deinit(allocator);
    for ([_]i32{ 0, 1, 2, 17, 33, -42 }) |key| {
        try history.recordPut(allocator, key);
    }
    history.recordRemove(17);

    var raw = try history.exportRaw(allocator);
    defer raw.deinit(allocator);
    try std.testing.expectEqual(@as(u8, 1), raw.zero_key_flag);
    try std.testing.expectEqual(@as(u8, 1), raw.one_key_flag);
    try std.testing.expect(std.mem.indexOfScalar(i32, raw.slots, 1) != null);

    const bytes = try encodeIntAlloc(allocator, &history, limits);
    defer allocator.free(bytes);
    try std.testing.expectEqual(exact_known_tag, bytes[0]);
    try std.testing.expectEqual(@as(u8, 1), bytes[1 + @sizeOf(u32)]);
    try std.testing.expectEqual(@as(u8, 1), bytes[1 + @sizeOf(u32) + 1]);
    for (raw.slots, 0..) |slot, index| {
        const offset = known_header_len + index * @sizeOf(i32);
        try std.testing.expectEqual(
            slot,
            std.mem.readInt(i32, bytes[offset..][0..@sizeOf(i32)], .little),
        );
    }

    var restored = try decodeIntExactAlloc(allocator, bytes, limits);
    defer restored.deinit(allocator);
    try expectIntRawEqual(&history, &restored);
}

test "long exact roundtrip preserves flags tombstones and little-endian slots" {
    const allocator = std.testing.allocator;
    const limits = Limits{ .max_slots = 64 };

    var history = long_order.History.init();
    defer history.deinit(allocator);
    for ([_]i64{
        0,
        1,
        2,
        4_294_967_298,
        8_589_934_594,
        -9_000_000_000_000_000_000,
    }) |key| {
        try history.recordPut(allocator, key);
    }
    history.recordRemove(4_294_967_298);

    var raw = try history.exportRaw(allocator);
    defer raw.deinit(allocator);
    try std.testing.expectEqual(@as(u8, 1), raw.zero_key_flag);
    try std.testing.expectEqual(@as(u8, 1), raw.one_key_flag);
    try std.testing.expect(std.mem.indexOfScalar(i64, raw.slots, 1) != null);

    const bytes = try encodeLongAlloc(allocator, &history, limits);
    defer allocator.free(bytes);
    for (raw.slots, 0..) |slot, index| {
        const offset = known_header_len + index * @sizeOf(i64);
        try std.testing.expectEqual(
            slot,
            std.mem.readInt(i64, bytes[offset..][0..@sizeOf(i64)], .little),
        );
    }

    var restored = try decodeLongExactAlloc(allocator, bytes, limits);
    defer restored.deinit(allocator);
    try expectLongRawEqual(&history, &restored);
}

test "structural errors and slot limits are rejected before allocation" {
    const allocator = std.testing.allocator;
    const limits = Limits{ .max_slots = 16 };

    try std.testing.expectError(
        error.Truncated,
        decodeIntExactAlloc(allocator, &.{}, limits),
    );
    try std.testing.expectError(
        error.UnknownTag,
        decodeIntExactAlloc(allocator, &.{2}, limits),
    );
    try std.testing.expectError(
        error.Truncated,
        decodeLongExactAlloc(allocator, &.{exact_known_tag}, limits),
    );

    var limited_header = [_]u8{0} ** known_header_len;
    limited_header[0] = exact_known_tag;
    std.mem.writeInt(u32, limited_header[1..][0..@sizeOf(u32)], 16, .little);
    var failing = std.testing.FailingAllocator.init(
        allocator,
        .{ .fail_index = 0 },
    );
    try std.testing.expectError(
        error.SlotLimitExceeded,
        decodeIntExactAlloc(
            failing.allocator(),
            &limited_header,
            .{ .max_slots = 15 },
        ),
    );

    var oversized_header = [_]u8{0} ** known_header_len;
    oversized_header[0] = exact_known_tag;
    std.mem.writeInt(
        u32,
        oversized_header[1..][0..@sizeOf(u32)],
        ec_max_slots + 1,
        .little,
    );
    try std.testing.expectError(
        error.InvalidTableSize,
        decodeLongExactAlloc(
            failing.allocator(),
            &oversized_header,
            .{ .max_slots = std.math.maxInt(usize) },
        ),
    );

    var truncated = [_]u8{0} ** (known_header_len + @sizeOf(i32) - 1);
    truncated[0] = exact_known_tag;
    std.mem.writeInt(u32, truncated[1..][0..@sizeOf(u32)], 1, .little);
    try std.testing.expectError(
        error.Truncated,
        decodeIntExactAlloc(failing.allocator(), &truncated, limits),
    );

    var trailing = [_]u8{0} ** (known_header_len + @sizeOf(i32) + 1);
    trailing[0] = exact_known_tag;
    std.mem.writeInt(u32, trailing[1..][0..@sizeOf(u32)], 1, .little);
    try std.testing.expectError(
        error.TrailingBytes,
        decodeIntExactAlloc(failing.allocator(), &trailing, limits),
    );

    var default_history = int_order.History.init();
    defer default_history.deinit(allocator);
    try std.testing.expectError(
        error.SlotLimitExceeded,
        encodeIntAlloc(
            failing.allocator(),
            &default_history,
            .{ .max_slots = 15 },
        ),
    );
}

test "int decoder delegates flag occupancy and probe validation to importRaw" {
    const allocator = std.testing.allocator;
    const limits = Limits{ .max_slots = 16 };

    var bad_flag = [_]u8{0} ** (known_header_len + @sizeOf(i32));
    bad_flag[0] = exact_known_tag;
    std.mem.writeInt(u32, bad_flag[1..][0..@sizeOf(u32)], 1, .little);
    bad_flag[1 + @sizeOf(u32)] = 2;
    try std.testing.expectError(
        error.InvalidFlag,
        decodeIntExactAlloc(allocator, &bad_flag, limits),
    );

    var bad_occupancy = [_]u8{0} ** (known_header_len + 2 * @sizeOf(i32));
    bad_occupancy[0] = exact_known_tag;
    std.mem.writeInt(u32, bad_occupancy[1..][0..@sizeOf(u32)], 2, .little);
    std.mem.writeInt(i32, bad_occupancy[known_header_len..][0..@sizeOf(i32)], 1, .little);
    std.mem.writeInt(
        i32,
        bad_occupancy[known_header_len + @sizeOf(i32) ..][0..@sizeOf(i32)],
        1,
        .little,
    );
    try std.testing.expectError(
        error.InvalidOccupancy,
        decodeIntExactAlloc(allocator, &bad_occupancy, limits),
    );

    var history = int_order.History.init();
    defer history.deinit(allocator);
    for ([_]i32{ 2, 17, 33 }) |key| try history.recordPut(allocator, key);
    const bytes = try encodeIntAlloc(allocator, &history, limits);
    defer allocator.free(bytes);
    const duplicate = try allocator.dupe(u8, bytes);
    defer allocator.free(duplicate);

    var active_index: ?usize = null;
    var empty_index: ?usize = null;
    for (0..16) |index| {
        const offset = known_header_len + index * @sizeOf(i32);
        const key = std.mem.readInt(i32, duplicate[offset..][0..@sizeOf(i32)], .little);
        if (key == 2) active_index = index;
        if (key == 0 and empty_index == null) empty_index = index;
    }
    const active_offset = known_header_len + active_index.? * @sizeOf(i32);
    const empty_offset = known_header_len + empty_index.? * @sizeOf(i32);
    std.mem.copyForwards(
        u8,
        duplicate[empty_offset..][0..@sizeOf(i32)],
        duplicate[active_offset..][0..@sizeOf(i32)],
    );
    try std.testing.expectError(
        error.DuplicateActiveKey,
        decodeIntExactAlloc(allocator, duplicate, limits),
    );

    const unreachable_wire = try allocator.dupe(u8, bytes);
    defer allocator.free(unreachable_wire);
    std.mem.writeInt(
        i32,
        unreachable_wire[active_offset..][0..@sizeOf(i32)],
        0,
        .little,
    );
    std.mem.writeInt(
        i32,
        unreachable_wire[empty_offset..][0..@sizeOf(i32)],
        2,
        .little,
    );
    try std.testing.expectError(
        error.UnreachableActiveKey,
        decodeIntExactAlloc(allocator, unreachable_wire, limits),
    );
}

test "long decoder delegates flag occupancy and table validation to importRaw" {
    const allocator = std.testing.allocator;
    const limits = Limits{ .max_slots = 16 };

    var invalid_table = [_]u8{0} ** (known_header_len + 3 * @sizeOf(i64));
    invalid_table[0] = exact_known_tag;
    std.mem.writeInt(u32, invalid_table[1..][0..@sizeOf(u32)], 3, .little);
    try std.testing.expectError(
        error.InvalidTableSize,
        decodeLongExactAlloc(allocator, &invalid_table, limits),
    );

    var bad_flag = [_]u8{0} ** (known_header_len + @sizeOf(i64));
    bad_flag[0] = exact_known_tag;
    std.mem.writeInt(u32, bad_flag[1..][0..@sizeOf(u32)], 1, .little);
    bad_flag[1 + @sizeOf(u32) + 1] = 7;
    try std.testing.expectError(
        error.InvalidFlag,
        decodeLongExactAlloc(allocator, &bad_flag, limits),
    );

    var bad_occupancy = [_]u8{0} ** (known_header_len + 2 * @sizeOf(i64));
    bad_occupancy[0] = exact_known_tag;
    std.mem.writeInt(u32, bad_occupancy[1..][0..@sizeOf(u32)], 2, .little);
    std.mem.writeInt(i64, bad_occupancy[known_header_len..][0..@sizeOf(i64)], 1, .little);
    std.mem.writeInt(
        i64,
        bad_occupancy[known_header_len + @sizeOf(i64) ..][0..@sizeOf(i64)],
        1,
        .little,
    );
    try std.testing.expectError(
        error.InvalidOccupancy,
        decodeLongExactAlloc(allocator, &bad_occupancy, limits),
    );
}

fn allocationFailureCampaign(allocator: std.mem.Allocator) !void {
    const limits = Limits{ .max_slots = 64 };

    var lazy_int = int_order.History.init();
    defer lazy_int.deinit(allocator);
    const lazy_int_bytes = try encodeIntAlloc(allocator, &lazy_int, limits);
    defer allocator.free(lazy_int_bytes);
    var lazy_int_restored = try decodeIntExactAlloc(allocator, lazy_int_bytes, limits);
    defer lazy_int_restored.deinit(allocator);

    var int_history = int_order.History.init();
    defer int_history.deinit(allocator);
    for ([_]i32{ 0, 1, 2, 17, 33 }) |key| {
        try int_history.recordPut(allocator, key);
    }
    int_history.recordRemove(17);
    const int_bytes = try encodeIntAlloc(allocator, &int_history, limits);
    defer allocator.free(int_bytes);
    var int_restored = try decodeIntExactAlloc(allocator, int_bytes, limits);
    defer int_restored.deinit(allocator);

    var long_history = long_order.History.init();
    defer long_history.deinit(allocator);
    for ([_]i64{ 0, 1, 2, 4_294_967_298, 8_589_934_594 }) |key| {
        try long_history.recordPut(allocator, key);
    }
    long_history.recordRemove(4_294_967_298);
    const long_bytes = try encodeLongAlloc(allocator, &long_history, limits);
    defer allocator.free(long_bytes);
    var long_restored = try decodeLongExactAlloc(allocator, long_bytes, limits);
    defer long_restored.deinit(allocator);
}

test "codec cleans up every allocation failure" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        allocationFailureCampaign,
        .{},
    );
}
