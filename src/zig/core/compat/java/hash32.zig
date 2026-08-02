//! Pure arithmetic for the signed 32-bit hashes used by legacy Java state
//! aggregation. This is a building block for a separately versioned
//! compatibility/forensic path; it does not replace Exchange Core's Zig u64
//! fingerprints. Callers must supply explicit value hashes, so JVM identity
//! hashes (notably enum `hashCode()`) remain deliberately outside this module.

const std = @import("std");

/// A pre-hashed entry from Java's IntObjectHashMap state-hash path.
/// `state_hash` is the signed `int` returned by the value's `stateHash()`.
/// Entry slices passed to map helpers must contain each logical key once.
pub const IntObjectStateEntry = struct {
    key: i32,
    state_hash: i32,
};

/// A pre-hashed entry from Java's LongObjectHashMap state-hash path.
/// `state_hash` is the signed `int` returned by the value's `stateHash()`.
/// Entry slices passed to map helpers must contain each logical key once.
pub const LongObjectStateEntry = struct {
    key: i64,
    state_hash: i32,
};

/// One logical entry of Eclipse Collections' IntLongHashMap.
/// Entry slices passed to map helpers must contain each logical key once.
pub const IntLongMapEntry = struct {
    key: i32,
    value: i64,
};

/// Java signed integer addition: discard carry above bit 31.
pub fn wrappingAddI32(lhs: i32, rhs: i32) i32 {
    return lhs +% rhs;
}

/// Java signed integer multiplication: discard bits above bit 31.
pub fn wrappingMulI32(lhs: i32, rhs: i32) i32 {
    return lhs *% rhs;
}

/// Java's `accumulator * multiplier + value`, with wrapping after each
/// signed 32-bit operation.
pub fn wrappingMulAddI32(accumulator: i32, multiplier: i32, value: i32) i32 {
    return wrappingAddI32(wrappingMulI32(accumulator, multiplier), value);
}

/// Java signed long addition: discard carry above bit 63.
pub fn wrappingAddI64(lhs: i64, rhs: i64) i64 {
    return lhs +% rhs;
}

/// Java signed long multiplication: discard bits above bit 63.
pub fn wrappingMulI64(lhs: i64, rhs: i64) i64 {
    return lhs *% rhs;
}

/// Java's `accumulator * multiplier + value`, with wrapping after each
/// signed 64-bit operation.
pub fn wrappingMulAddI64(accumulator: i64, multiplier: i64, value: i64) i64 {
    return wrappingAddI64(wrappingMulI64(accumulator, multiplier), value);
}

/// Java `Long.hashCode(long)`: `(int) (value ^ (value >>> 32))`.
/// Bit casts make the logical right shift and signed narrowing explicit.
pub fn longHashCode(value: i64) i32 {
    const bits: u64 = @bitCast(value);
    const folded: u32 = @truncate(bits ^ (bits >> 32));
    return @bitCast(folded);
}

/// Applies Java's order-dependent polynomial hash to already-computed,
/// signed Java hash codes.
pub fn foldHashCodes(initial: i32, value_hashes: []const i32) i32 {
    var hash = initial;
    for (value_hashes) |value_hash| {
        hash = wrappingMulAddI32(hash, 31, value_hash);
    }
    return hash;
}

/// Java `Arrays.hashCode(int[])` for a non-null array. Zig slices cannot
/// represent Java's distinct null-array case, whose hash is zero.
pub fn arraysHashCodeI32(values: []const i32) i32 {
    return foldHashCodes(1, values);
}

/// Java `Arrays.hashCode(long[])` for a non-null array. Zig slices cannot
/// represent Java's distinct null-array case, whose hash is zero.
pub fn arraysHashCodeI64(values: []const i64) i32 {
    var hash: i32 = 1;
    for (values) |value| {
        hash = wrappingMulAddI32(hash, 31, longHashCode(value));
    }
    return hash;
}

/// Java `Objects.hash(Object...)` over a non-null varargs array of signed value
/// hashes. Supplying precomputed codes keeps JVM identity hashes outside this
/// module; an explicitly null Java varargs array hashes to zero and is not
/// representable as a Zig slice.
pub fn objectsHash(value_hashes: []const i32) i32 {
    return foldHashCodes(1, value_hashes);
}

/// Exchange Core's `HashingUtils.stateHashStream`: the same polynomial fold
/// as an object array, but initialized with zero instead of one.
pub fn stateHashStream(value_state_hashes: []const i32) i32 {
    return foldHashCodes(0, value_state_hashes);
}

/// Generic `Arrays.hashCode(Object[])` arithmetic. `hashCodeOf` must return
/// the signed Java hash code for a value; it is never inferred from Zig bytes.
pub fn arraysHashCodeBy(
    comptime T: type,
    values: []const T,
    hashCodeOf: *const fn (T) i32,
) i32 {
    var hash: i32 = 1;
    for (values) |value| {
        hash = wrappingMulAddI32(hash, 31, hashCodeOf(value));
    }
    return hash;
}

/// Generic `Objects.hash(Object...)` arithmetic using explicit value hashes.
pub fn objectsHashBy(
    comptime T: type,
    values: []const T,
    hashCodeOf: *const fn (T) i32,
) i32 {
    return arraysHashCodeBy(T, values, hashCodeOf);
}

/// Generic Exchange Core state-stream fold using explicit value state hashes.
pub fn stateHashStreamBy(
    comptime T: type,
    values: []const T,
    stateHashOf: *const fn (T) i32,
) i32 {
    var hash: i32 = 0;
    for (values) |value| {
        hash = wrappingMulAddI32(hash, 31, stateHashOf(value));
    }
    return hash;
}

/// Wrapping signed-long sum used by `HashingUtils.stateHash(IntObjectHashMap)`.
/// Each entry contributes `Objects.hash(key, value.stateHash())` after the
/// signed `int` result is promoted to a signed Java `long`.
pub fn intObjectStateMapSum(entries: []const IntObjectStateEntry) i64 {
    var sum: i64 = 0;
    for (entries) |entry| {
        const fields = [_]i32{ entry.key, entry.state_hash };
        sum = wrappingAddI64(sum, objectsHash(&fields));
    }
    return sum;
}

/// Java Exchange Core's final IntObjectHashMap state hash.
pub fn intObjectStateMapHash(entries: []const IntObjectStateEntry) i32 {
    return longHashCode(intObjectStateMapSum(entries));
}

/// Wrapping signed-long sum used by `HashingUtils.stateHash(LongObjectHashMap)`.
pub fn longObjectStateMapSum(entries: []const LongObjectStateEntry) i64 {
    var sum: i64 = 0;
    for (entries) |entry| {
        const fields = [_]i32{ longHashCode(entry.key), entry.state_hash };
        sum = wrappingAddI64(sum, objectsHash(&fields));
    }
    return sum;
}

/// Java Exchange Core's final LongObjectHashMap state hash.
pub fn longObjectStateMapHash(entries: []const LongObjectStateEntry) i32 {
    return longHashCode(longObjectStateMapSum(entries));
}

/// Eclipse Collections IntLongHashMap follows `Map.hashCode()`: sum the
/// wrapping signed-int hash of each entry, where an entry is
/// `Integer.hashCode(key) ^ Long.hashCode(value)`.
pub fn eclipseIntLongMapHash(entries: []const IntLongMapEntry) i32 {
    var hash: i32 = 0;
    for (entries) |entry| {
        const key_bits: u32 = @bitCast(entry.key);
        const value_hash_bits: u32 = @bitCast(longHashCode(entry.value));
        const entry_hash: i32 = @bitCast(key_bits ^ value_hash_bits);
        hash = wrappingAddI32(hash, entry_hash);
    }
    return hash;
}

fn referenceWrapI32(value: i128) i32 {
    const modulus: i128 = @as(i128, 1) << 32;
    const sign_bit: i128 = @as(i128, 1) << 31;
    const bits = @mod(value, modulus);
    return @intCast(if (bits >= sign_bit) bits - modulus else bits);
}

fn referenceWrapI64(value: i128) i64 {
    const modulus: i128 = @as(i128, 1) << 64;
    const sign_bit: i128 = @as(i128, 1) << 63;
    const bits = @mod(value, modulus);
    return @intCast(if (bits >= sign_bit) bits - modulus else bits);
}

fn referenceFold(initial: i32, values: []const i32) i32 {
    var hash = initial;
    for (values) |value| {
        hash = referenceWrapI32(@as(i128, hash) * 31 + value);
    }
    return hash;
}

fn referenceIntObjectStateMapSum(entries: []const IntObjectStateEntry) i64 {
    var sum: i64 = 0;
    for (entries) |entry| {
        const fields = [_]i32{ entry.key, entry.state_hash };
        const entry_hash = referenceFold(1, &fields);
        sum = referenceWrapI64(@as(i128, sum) + entry_hash);
    }
    return sum;
}

fn referenceLongObjectStateMapSum(entries: []const LongObjectStateEntry) i64 {
    var sum: i64 = 0;
    for (entries) |entry| {
        const fields = [_]i32{ longHashCode(entry.key), entry.state_hash };
        const entry_hash = referenceFold(1, &fields);
        sum = referenceWrapI64(@as(i128, sum) + entry_hash);
    }
    return sum;
}

fn referenceEclipseIntLongMapHash(entries: []const IntLongMapEntry) i32 {
    var sum: i128 = 0;
    for (entries) |entry| {
        const entry_hash: i32 = @bitCast(
            @as(u32, @bitCast(entry.key)) ^ @as(u32, @bitCast(longHashCode(entry.value))),
        );
        sum += entry_hash;
    }
    return referenceWrapI32(sum);
}

test "signed wrapping arithmetic matches Java overflow" {
    try std.testing.expectEqual(
        std.math.minInt(i32),
        wrappingAddI32(std.math.maxInt(i32), 1),
    );
    try std.testing.expectEqual(
        std.math.maxInt(i32),
        wrappingAddI32(std.math.minInt(i32), -1),
    );
    try std.testing.expectEqual(
        std.math.minInt(i32),
        wrappingMulI32(std.math.minInt(i32), -1),
    );
    try std.testing.expectEqual(
        referenceWrapI32(@as(i128, 1_500_000_000) * 31 - 900_000_000),
        wrappingMulAddI32(1_500_000_000, 31, -900_000_000),
    );

    try std.testing.expectEqual(
        std.math.minInt(i64),
        wrappingAddI64(std.math.maxInt(i64), 1),
    );
    try std.testing.expectEqual(
        std.math.maxInt(i64),
        wrappingAddI64(std.math.minInt(i64), -1),
    );
    try std.testing.expectEqual(
        std.math.minInt(i64),
        wrappingMulI64(std.math.minInt(i64), -1),
    );
    try std.testing.expectEqual(
        referenceWrapI64(@as(i128, 4_000_000_000_000_000_000) * -7 + 19),
        wrappingMulAddI64(4_000_000_000_000_000_000, -7, 19),
    );
}

test "Long.hashCode covers signed long boundaries" {
    try std.testing.expectEqual(@as(i32, 0), longHashCode(0));
    try std.testing.expectEqual(@as(i32, 1), longHashCode(1));
    try std.testing.expectEqual(@as(i32, 0), longHashCode(-1));
    try std.testing.expectEqual(std.math.minInt(i32), longHashCode(std.math.maxInt(i64)));
    try std.testing.expectEqual(std.math.minInt(i32), longHashCode(std.math.minInt(i64)));
    try std.testing.expectEqual(@as(i32, 1), longHashCode(0x0000_0001_0000_0000));
    try std.testing.expectEqual(@as(i32, -1), longHashCode(-0x0000_0001_0000_0000));
}

test "Objects.hash and int array hash use the one-seeded fold" {
    const values = [_]i32{
        std.math.maxInt(i32),
        -7,
        std.math.minInt(i32),
        42,
    };
    const expected = referenceFold(1, &values);

    try std.testing.expectEqual(@as(i32, 1), objectsHash(&.{}));
    try std.testing.expectEqual(@as(i32, 1), arraysHashCodeI32(&.{}));
    try std.testing.expectEqual(expected, objectsHash(&values));
    try std.testing.expectEqual(expected, arraysHashCodeI32(&values));
}

test "long array hash folds Long.hashCode values" {
    const values = [_]i64{
        std.math.minInt(i64),
        -4_294_967_296,
        -1,
        0,
        4_294_967_296,
        std.math.maxInt(i64),
    };
    var value_hashes: [values.len]i32 = undefined;
    for (values, 0..) |value, index| value_hashes[index] = longHashCode(value);

    try std.testing.expectEqual(@as(i32, 1), arraysHashCodeI64(&.{}));
    try std.testing.expectEqual(referenceFold(1, &value_hashes), arraysHashCodeI64(&values));
}

test "state stream starts at zero and sequence folds are order dependent" {
    const forward = [_]i32{ -1, 2, std.math.maxInt(i32), -4 };
    const reverse = [_]i32{ -4, std.math.maxInt(i32), 2, -1 };

    try std.testing.expectEqual(@as(i32, 0), stateHashStream(&.{}));
    try std.testing.expectEqual(referenceFold(0, &forward), stateHashStream(&forward));
    try std.testing.expect(objectsHash(&forward) != objectsHash(&reverse));
    try std.testing.expect(stateHashStream(&forward) != stateHashStream(&reverse));
}

test "generic object and stream folds use explicit signed hashes" {
    const Value = struct {
        hash: i32,

        fn hashCode(value: @This()) i32 {
            return value.hash;
        }
    };
    const values = [_]Value{
        .{ .hash = -99 },
        .{ .hash = std.math.maxInt(i32) },
        .{ .hash = 17 },
    };
    const hashes = [_]i32{ -99, std.math.maxInt(i32), 17 };

    try std.testing.expectEqual(
        referenceFold(1, &hashes),
        arraysHashCodeBy(Value, &values, Value.hashCode),
    );
    try std.testing.expectEqual(
        referenceFold(1, &hashes),
        objectsHashBy(Value, &values, Value.hashCode),
    );
    try std.testing.expectEqual(
        referenceFold(0, &hashes),
        stateHashStreamBy(Value, &values, Value.hashCode),
    );
}

test "IntObject state-map sum is order independent" {
    const entries = [_]IntObjectStateEntry{
        .{ .key = -7, .state_hash = std.math.maxInt(i32) },
        .{ .key = 0, .state_hash = -456 },
        .{ .key = std.math.minInt(i32), .state_hash = 123 },
        .{ .key = 42, .state_hash = std.math.minInt(i32) },
    };
    const permuted = [_]IntObjectStateEntry{
        entries[2],
        entries[0],
        entries[3],
        entries[1],
    };
    const expected_sum = referenceIntObjectStateMapSum(&entries);

    try std.testing.expectEqual(expected_sum, intObjectStateMapSum(&entries));
    try std.testing.expectEqual(expected_sum, intObjectStateMapSum(&permuted));
    try std.testing.expectEqual(longHashCode(expected_sum), intObjectStateMapHash(&entries));
    try std.testing.expectEqual(
        intObjectStateMapHash(&entries),
        intObjectStateMapHash(&permuted),
    );
}

test "LongObject state-map sum handles boundary and negative keys" {
    const entries = [_]LongObjectStateEntry{
        .{ .key = std.math.minInt(i64), .state_hash = -1 },
        .{ .key = -4_294_967_296, .state_hash = std.math.minInt(i32) },
        .{ .key = 4_294_967_296, .state_hash = std.math.maxInt(i32) },
        .{ .key = std.math.maxInt(i64), .state_hash = 91 },
    };
    const permuted = [_]LongObjectStateEntry{
        entries[3],
        entries[1],
        entries[0],
        entries[2],
    };
    const expected_sum = referenceLongObjectStateMapSum(&entries);

    try std.testing.expectEqual(expected_sum, longObjectStateMapSum(&entries));
    try std.testing.expectEqual(expected_sum, longObjectStateMapSum(&permuted));
    try std.testing.expectEqual(longHashCode(expected_sum), longObjectStateMapHash(&entries));
    try std.testing.expectEqual(
        longObjectStateMapHash(&entries),
        longObjectStateMapHash(&permuted),
    );
}

test "Eclipse IntLongHashMap hash is an order-independent entry sum" {
    const entries = [_]IntLongMapEntry{
        .{ .key = std.math.minInt(i32), .value = std.math.maxInt(i64) },
        .{ .key = -77, .value = -4_294_967_296 },
        .{ .key = 0, .value = -1 },
        .{ .key = std.math.maxInt(i32), .value = std.math.minInt(i64) },
    };
    const permuted = [_]IntLongMapEntry{
        entries[1],
        entries[3],
        entries[2],
        entries[0],
    };
    const expected = referenceEclipseIntLongMapHash(&entries);

    try std.testing.expectEqual(expected, eclipseIntLongMapHash(&entries));
    try std.testing.expectEqual(expected, eclipseIntLongMapHash(&permuted));

    // Eclipse Collections stores keys 0 and 1 through sentinel slots, but its
    // public hashCode still applies the same logical-entry formula to them.
    const sentinel_entries = [_]IntLongMapEntry{
        .{ .key = 0, .value = std.math.minInt(i64) },
        .{ .key = 1, .value = -1 },
    };
    try std.testing.expectEqual(
        referenceEclipseIntLongMapHash(&sentinel_entries),
        eclipseIntLongMapHash(&sentinel_entries),
    );

    const wrapping_entries = [_]IntLongMapEntry{
        .{ .key = std.math.maxInt(i32), .value = 0 },
        .{ .key = std.math.maxInt(i32) - 1, .value = 0 },
    };
    try std.testing.expectEqual(@as(i32, -3), eclipseIntLongMapHash(&wrapping_entries));
}

test "arithmetic helpers match naive modular references" {
    const i32_values = [_]i32{
        std.math.minInt(i32),
        -1_000_000_007,
        -1,
        0,
        1,
        1_000_000_007,
        std.math.maxInt(i32),
    };
    for (i32_values) |lhs| {
        for (i32_values) |rhs| {
            try std.testing.expectEqual(
                referenceWrapI32(@as(i128, lhs) + rhs),
                wrappingAddI32(lhs, rhs),
            );
            try std.testing.expectEqual(
                referenceWrapI32(@as(i128, lhs) * rhs),
                wrappingMulI32(lhs, rhs),
            );
        }
    }

    const i64_values = [_]i64{
        std.math.minInt(i64),
        -4_000_000_000_000_000_000,
        -1,
        0,
        1,
        4_000_000_000_000_000_000,
        std.math.maxInt(i64),
    };
    for (i64_values) |lhs| {
        for (i64_values) |rhs| {
            try std.testing.expectEqual(
                referenceWrapI64(@as(i128, lhs) + rhs),
                wrappingAddI64(lhs, rhs),
            );
            try std.testing.expectEqual(
                referenceWrapI64(@as(i128, lhs) * rhs),
                wrappingMulI64(lhs, rhs),
            );
        }
    }
}
