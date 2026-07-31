const std = @import("std");
const cmd = @import("cmd.zig");
const java_hash32 = @import("java_hash32.zig");

pub const FrameWordsPerCommand: usize = 5;

pub const CompletedTransfer = struct {
    transfer_id: i32,
    command_type: cmd.OrderCommandType,
    words: []i64,

    pub fn deinit(self: *CompletedTransfer, allocator: std.mem.Allocator) void {
        allocator.free(self.words);
        self.* = undefined;
    }
};

pub const AcceptResult = union(enum) {
    accepted,
    complete: CompletedTransfer,
};

pub const AssemblyError = std.mem.Allocator.Error || error{
    UnsupportedCommandType,
    InvalidLastFlag,
    TransferTypeMismatch,
    TransferStateCorrupted,
};

pub const StateHashError = std.mem.Allocator.Error || error{
    TransferStateCorrupted,
};

pub const JavaStateHashError = error{
    JavaStateHashUnavailable,
    TransferStateCorrupted,
};

const TransferRecord = struct {
    /// Legacy Java snapshots did not persist whether a partial transfer was a
    /// command or query. `null` preserves that exact state until the first
    /// subsequent frame supplies the type.
    command_type: ?cmd.OrderCommandType,
    /// Java's TransferRecord hashes its complete backing array, including its
    /// zero tail. `null` means an older/native payload did not retain enough
    /// envelope information to reconstruct that array exactly.
    java_backing_capacity: ?u32,
    words: std.ArrayListUnmanaged(i64) = .empty,

    fn deinit(self: *TransferRecord, allocator: std.mem.Allocator) void {
        self.words.deinit(allocator);
        self.* = undefined;
    }
};

pub const PendingTransfer = struct {
    transfer_id: i32,
    command_type: ?cmd.OrderCommandType,
    /// Exact Java `TransferRecord.dataArray.length`, when reconstructible.
    /// Native snapshots written before this field are represented by `null`.
    java_backing_capacity: ?u32 = null,
    words: []i64,

    pub fn deinit(self: *PendingTransfer, allocator: std.mem.Allocator) void {
        allocator.free(self.words);
        self.* = undefined;
    }
};

/// Exact shape persisted by Java's BinaryCommandsProcessor.TransferRecord.
/// `data` is the full backing array; only the transferred prefix is live.
pub const LegacyUntypedPendingTransfer = struct {
    transfer_id: i64,
    words_transferred: i32,
    data: []const i64,
};

pub const PendingState = struct {
    allocator: std.mem.Allocator,
    transfers: []PendingTransfer,

    pub fn deinit(self: *PendingState) void {
        for (self.transfers) |*transfer| transfer.deinit(self.allocator);
        self.allocator.free(self.transfers);
        self.* = undefined;
    }
};

pub const BinaryFrameAssembler = struct {
    allocator: std.mem.Allocator,
    transfers: std.AutoHashMapUnmanaged(i32, TransferRecord) = .empty,

    pub fn init(allocator: std.mem.Allocator) BinaryFrameAssembler {
        return .{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *BinaryFrameAssembler) void {
        self.reset();
        self.transfers.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn pendingTransfers(self: *const BinaryFrameAssembler) usize {
        return self.transfers.count();
    }

    pub fn stateHash(self: *const BinaryFrameAssembler) StateHashError!u64 {
        var hasher = std.hash.Wyhash.init(0);
        var transfer_ids: std.ArrayListUnmanaged(i32) = .empty;
        defer transfer_ids.deinit(self.allocator);

        try transfer_ids.ensureTotalCapacityPrecise(self.allocator, self.transfers.count());
        var it = self.transfers.iterator();
        while (it.next()) |entry| {
            transfer_ids.appendAssumeCapacity(entry.key_ptr.*);
        }
        std.sort.heap(i32, transfer_ids.items, {}, lessThanI32);

        for (transfer_ids.items) |transfer_id| {
            const record = try self.getTransferRecord(transfer_id);
            hashI32(&hasher, transfer_id);
            hashI8(&hasher, pendingCommandTypeCode(record.command_type));
            hashU64(&hasher, @as(u64, @intCast(record.words.items.len)));
            for (record.words.items) |word| {
                hashI64(&hasher, word);
            }
        }

        return hasher.final();
    }

    /// Exact signed Java `BinaryCommandsProcessor.stateHash()` compatibility
    /// path. This is intentionally separate from `stateHash`, whose native
    /// u64 fingerprint and inputs must remain unchanged.
    pub fn javaStateHash32(self: *const BinaryFrameAssembler) JavaStateHashError!i32 {
        var map_sum: i64 = 0;
        var it = self.transfers.iterator();
        while (it.next()) |entry| {
            const record_hash = try javaTransferRecordStateHash(entry.value_ptr);
            // Java's int transfer ID is widened with sign extension before it
            // becomes the LongObjectHashMap key.
            const java_key: i64 = entry.key_ptr.*;
            const key_hash = java_hash32.longHashCode(java_key);
            const entry_fields = [_]i32{ key_hash, record_hash };
            map_sum = java_hash32.wrappingAddI64(
                map_sum,
                java_hash32.objectsHash(&entry_fields),
            );
        }
        return java_hash32.longHashCode(map_sum);
    }

    /// Returns an owned, transfer-id-sorted image suitable for durable
    /// serialization. Sorting removes hash-map iteration order from the file
    /// format and makes retries byte-for-byte deterministic.
    pub fn capturePendingState(
        self: *const BinaryFrameAssembler,
        allocator: std.mem.Allocator,
    ) std.mem.Allocator.Error!PendingState {
        var ids: std.ArrayListUnmanaged(i32) = .empty;
        defer ids.deinit(allocator);
        try ids.ensureTotalCapacityPrecise(allocator, self.transfers.count());

        var it = self.transfers.keyIterator();
        while (it.next()) |transfer_id| ids.appendAssumeCapacity(transfer_id.*);
        std.sort.heap(i32, ids.items, {}, lessThanI32);

        const transfers = try allocator.alloc(PendingTransfer, ids.items.len);
        var initialized: usize = 0;
        errdefer {
            for (transfers[0..initialized]) |*transfer| transfer.deinit(allocator);
            allocator.free(transfers);
        }

        for (ids.items, 0..) |transfer_id, index| {
            const record = self.transfers.get(transfer_id) orelse unreachable;
            transfers[index] = .{
                .transfer_id = transfer_id,
                .command_type = record.command_type,
                .java_backing_capacity = record.java_backing_capacity,
                .words = try allocator.dupe(i64, record.words.items),
            };
            initialized += 1;
        }
        return .{ .allocator = allocator, .transfers = transfers };
    }

    /// Replaces all in-flight transfers from a validated durable image.
    pub fn restorePendingState(
        self: *BinaryFrameAssembler,
        state: []const PendingTransfer,
    ) AssemblyError!void {
        var replacement = BinaryFrameAssembler.init(self.allocator);
        defer replacement.deinit();
        try replacement.transfers.ensureTotalCapacity(self.allocator, @intCast(state.len));

        for (state) |transfer| {
            if (transfer.command_type) |command_type| {
                if (!isBinaryFrameCommand(command_type)) return error.UnsupportedCommandType;
            }
            if (transfer.words.len == 0 or transfer.words.len % FrameWordsPerCommand != 0) {
                return error.TransferStateCorrupted;
            }
            if (transfer.java_backing_capacity) |capacity| {
                if (javaBackingCapacityForWords(transfer.words) != capacity) {
                    return error.TransferStateCorrupted;
                }
            }
            const gop = try replacement.transfers.getOrPut(self.allocator, transfer.transfer_id);
            if (gop.found_existing) return error.TransferStateCorrupted;
            gop.value_ptr.* = .{
                .command_type = transfer.command_type,
                .java_backing_capacity = transfer.java_backing_capacity,
            };
            try gop.value_ptr.words.appendSlice(self.allocator, transfer.words);
        }

        std.mem.swap(BinaryFrameAssembler, self, &replacement);
    }

    /// Restores the deliberately untyped partial-transfer representation used
    /// by legacy Java snapshots. The replacement is staged and swapped only
    /// after every id/count/allocation validates.
    pub fn restoreLegacyUntypedPendingState(
        self: *BinaryFrameAssembler,
        state: []const LegacyUntypedPendingTransfer,
    ) AssemblyError!void {
        var replacement = BinaryFrameAssembler.init(self.allocator);
        defer replacement.deinit();
        try replacement.transfers.ensureTotalCapacity(self.allocator, @intCast(state.len));

        for (state) |transfer| {
            const transfer_id = std.math.cast(i32, transfer.transfer_id) orelse
                return error.TransferStateCorrupted;
            if (transfer.words_transferred <= 0) return error.TransferStateCorrupted;
            const word_count: usize = @intCast(transfer.words_transferred);
            if (word_count > transfer.data.len or word_count % FrameWordsPerCommand != 0) {
                return error.TransferStateCorrupted;
            }
            for (transfer.data[word_count..]) |tail_word| {
                // TransferRecord only ever grows a freshly zeroed Java array,
                // so a nonzero unused tail cannot be produced by Java.
                if (tail_word != 0) return error.TransferStateCorrupted;
            }
            const data_len = std.math.cast(u32, transfer.data.len);
            const derived_capacity = javaBackingCapacityForWords(transfer.data[0..word_count]);
            const gop = try replacement.transfers.getOrPut(self.allocator, transfer_id);
            if (gop.found_existing) return error.TransferStateCorrupted;
            gop.value_ptr.* = .{
                .command_type = null,
                .java_backing_capacity = if (data_len != null and derived_capacity == data_len)
                    data_len
                else
                    null,
            };
            try gop.value_ptr.words.appendSlice(self.allocator, transfer.data[0..word_count]);
        }

        std.mem.swap(BinaryFrameAssembler, self, &replacement);
    }

    pub fn reset(self: *BinaryFrameAssembler) void {
        var it = self.transfers.valueIterator();
        while (it.next()) |record| {
            record.deinit(self.allocator);
        }
        self.transfers.clearRetainingCapacity();
    }

    pub fn acceptFrame(
        self: *BinaryFrameAssembler,
        frame: *const cmd.OrderCommand,
    ) AssemblyError!AcceptResult {
        if (!isBinaryFrameCommand(frame.command)) return error.UnsupportedCommandType;
        if (!(frame.symbol == 0 or frame.symbol == -1)) return error.InvalidLastFlag;

        const frame_words = [_]i64{
            frame.order_id,
            frame.price,
            frame.reserve_bid_price,
            frame.size,
            frame.uid,
        };

        if (self.transfers.getPtr(frame.user_cookie)) |record| {
            if (record.command_type) |command_type| {
                if (command_type != frame.command) return error.TransferTypeMismatch;
            }

            if (frame.symbol == -1) {
                const completed_len = std.math.add(
                    usize,
                    record.words.items.len,
                    FrameWordsPerCommand,
                ) catch return error.TransferStateCorrupted;
                const owned_words = try self.allocator.alloc(i64, completed_len);
                @memcpy(owned_words[0..record.words.items.len], record.words.items);
                @memcpy(owned_words[record.words.items.len..], &frame_words);

                var removed = self.removeTransferRecord(frame.user_cookie) catch {
                    self.allocator.free(owned_words);
                    return error.TransferStateCorrupted;
                };
                removed.deinit(self.allocator);
                return .{
                    .complete = .{
                        .transfer_id = frame.user_cookie,
                        .command_type = frame.command,
                        .words = owned_words,
                    },
                };
            }

            // Reserve all storage before binding an untyped legacy record or
            // exposing appended words. Unknown Java capacity stays unknown;
            // only records whose exact capacity was already known advance it.
            const next_word_count = std.math.add(
                usize,
                record.words.items.len,
                FrameWordsPerCommand,
            ) catch return error.TransferStateCorrupted;
            const next_java_capacity = if (record.java_backing_capacity != null)
                expectedJavaBackingCapacity(record.words.items[0], next_word_count) orelse
                    return error.TransferStateCorrupted
            else
                null;
            try record.words.ensureUnusedCapacity(self.allocator, FrameWordsPerCommand);
            if (record.command_type == null) record.command_type = frame.command;
            record.words.appendSliceAssumeCapacity(&frame_words);
            record.java_backing_capacity = next_java_capacity;
            return .accepted;
        }

        if (frame.symbol == -1) {
            return .{
                .complete = .{
                    .transfer_id = frame.user_cookie,
                    .command_type = frame.command,
                    .words = try self.allocator.dupe(i64, &frame_words),
                },
            };
        }

        var new_record = TransferRecord{
            .command_type = frame.command,
            .java_backing_capacity = null,
        };
        errdefer new_record.deinit(self.allocator);
        try new_record.words.appendSlice(self.allocator, &frame_words);
        new_record.java_backing_capacity = javaBackingCapacityForWords(new_record.words.items);

        const gop = try self.transfers.getOrPut(self.allocator, frame.user_cookie);
        if (gop.found_existing) {
            // The assembler is single-threaded; observing an insertion between
            // the prior lookup and this write would indicate external misuse.
            if (gop.value_ptr.command_type) |command_type| {
                if (command_type != frame.command) return error.TransferTypeMismatch;
            }
            return error.TransferStateCorrupted;
        }
        gop.value_ptr.* = new_record;
        return .accepted;
    }

    fn getTransferRecord(
        self: *const BinaryFrameAssembler,
        transfer_id: i32,
    ) error{TransferStateCorrupted}!TransferRecord {
        return self.transfers.get(transfer_id) orelse error.TransferStateCorrupted;
    }

    fn removeTransferRecord(
        self: *BinaryFrameAssembler,
        transfer_id: i32,
    ) error{TransferStateCorrupted}!TransferRecord {
        const removed = self.transfers.fetchRemove(transfer_id) orelse return error.TransferStateCorrupted;
        return removed.value;
    }
};

pub fn pendingCommandTypeCode(command_type: ?cmd.OrderCommandType) i8 {
    return if (command_type) |present| present.code() else 0;
}

pub fn pendingCommandTypeFromCode(raw: i8) error{UnsupportedCommandType}!?cmd.OrderCommandType {
    if (raw == 0) return null;
    const decoded = cmd.OrderCommandType.fromCode(raw) catch return error.UnsupportedCommandType;
    if (!isBinaryFrameCommand(decoded)) return error.UnsupportedCommandType;
    return decoded;
}

fn isBinaryFrameCommand(command_type: cmd.OrderCommandType) bool {
    return switch (command_type) {
        .binary_data_command, .binary_data_query => true,
        else => false,
    };
}

fn lessThanI32(_: void, lhs: i32, rhs: i32) bool {
    return lhs < rhs;
}

/// Java reads the original uncompressed byte length from the high 32 bits of
/// the first carrier word, masks off the sign bit, rounds bytes to longs, then
/// rounds longs up to ExchangeApi.LONGS_PER_MESSAGE (five).
fn javaInitialBackingCapacity(first_word: i64) ?u32 {
    const bits: u64 = @bitCast(first_word);
    const bytes_length: u32 = @as(u32, @truncate(bits >> 32)) & 0x7fff_ffff;
    if (bytes_length == 0) return null;

    const longs = ((bytes_length - 1) >> 3) + 1;
    const words_per_command: u32 = FrameWordsPerCommand;
    const remainder = longs % words_per_command;
    return if (remainder == 0) longs else longs + (words_per_command - remainder);
}

/// Replays Java TransferRecord's exact `if (wordsTransfered == length) length
/// *= 2` growth rule. A null result means Java could not have represented the
/// state (for example a zero envelope length or overflowing array length).
/// Reconstructs Java TransferRecord's backing-array capacity from its request
/// envelope and transferred prefix length. Persistence codecs use this to
/// validate nonzero explicit capacities without affecting native state hashes.
pub fn expectedJavaBackingCapacity(first_word: i64, word_count: usize) ?u32 {
    if (word_count == 0 or word_count > std.math.maxInt(i32)) return null;
    var capacity = javaInitialBackingCapacity(first_word) orelse return null;
    const words_transferred: u32 = @intCast(word_count);
    while (words_transferred > capacity) {
        capacity = std.math.mul(u32, capacity, 2) catch return null;
        if (capacity > std.math.maxInt(i32)) return null;
    }
    return capacity;
}

fn javaBackingCapacityForWords(words: []const i64) ?u32 {
    if (words.len == 0) return null;
    return expectedJavaBackingCapacity(words[0], words.len);
}

fn javaTransferRecordStateHash(record: *const TransferRecord) JavaStateHashError!i32 {
    const capacity = record.java_backing_capacity orelse return error.JavaStateHashUnavailable;
    if (record.words.items.len > std.math.maxInt(i32)) return error.JavaStateHashUnavailable;
    const derived_capacity = javaBackingCapacityForWords(record.words.items) orelse
        return error.JavaStateHashUnavailable;
    if (capacity != derived_capacity or record.words.items.len > capacity) {
        return error.TransferStateCorrupted;
    }

    const live_array_hash = java_hash32.arraysHashCodeI64(record.words.items);
    const zero_tail = @as(usize, capacity) - record.words.items.len;
    const array_hash = javaArrayHashAfterZeroTail(live_array_hash, zero_tail);
    const fields = [_]i32{ array_hash, @intCast(record.words.items.len) };
    return java_hash32.objectsHash(&fields);
}

/// Appending a zero to Java's array hash multiplies it by 31. Exponentiation
/// by squaring avoids an attacker-controlled O(declared request size) loop for
/// a five-word partial frame whose envelope can declare nearly 2 GiB.
fn javaArrayHashAfterZeroTail(live_hash: i32, zero_count: usize) i32 {
    var result = live_hash;
    var factor: i32 = 31;
    var remaining = zero_count;
    while (remaining != 0) : (remaining >>= 1) {
        if (remaining & 1 != 0) result = java_hash32.wrappingMulI32(result, factor);
        factor = java_hash32.wrappingMulI32(factor, factor);
    }
    return result;
}

fn hashI8(hasher: *std.hash.Wyhash, value: i8) void {
    var n = value;
    hasher.update(std.mem.asBytes(&n));
}

fn hashI32(hasher: *std.hash.Wyhash, value: i32) void {
    var n = value;
    hasher.update(std.mem.asBytes(&n));
}

fn hashI64(hasher: *std.hash.Wyhash, value: i64) void {
    var n = value;
    hasher.update(std.mem.asBytes(&n));
}

fn hashU64(hasher: *std.hash.Wyhash, value: u64) void {
    var n = value;
    hasher.update(std.mem.asBytes(&n));
}

fn makeFrame(
    command_type: cmd.OrderCommandType,
    transfer_id: i32,
    last_flag: i32,
    words: [FrameWordsPerCommand]i64,
) cmd.OrderCommand {
    return .{
        .command = command_type,
        .user_cookie = transfer_id,
        .symbol = last_flag,
        .order_id = words[0],
        .price = words[1],
        .reserve_bid_price = words[2],
        .size = words[3],
        .uid = words[4],
        .result_code = .new,
    };
}

fn legacyUntypedRestoreAllocationTest(allocator: std.mem.Allocator) !void {
    var assembler = BinaryFrameAssembler.init(allocator);
    defer assembler.deinit();
    const first = [_]i64{ 1, 2, 3, 4, 5 };
    const second = [_]i64{ 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    try assembler.restoreLegacyUntypedPendingState(&.{
        .{ .transfer_id = 1, .words_transferred = 5, .data = &first },
        .{ .transfer_id = 2, .words_transferred = 10, .data = &second },
    });
}

test "binary frame assembler accepts fragments and completes command transfer" {
    const allocator = std.testing.allocator;
    var assembler = BinaryFrameAssembler.init(allocator);
    defer assembler.deinit();

    const first = makeFrame(.binary_data_command, 77, 0, .{ 1, 2, 3, 4, 5 });
    try std.testing.expectEqual(AcceptResult.accepted, try assembler.acceptFrame(&first));
    try std.testing.expectEqual(@as(usize, 1), assembler.pendingTransfers());

    const second = makeFrame(.binary_data_command, 77, -1, .{ 6, 7, 8, 9, 10 });
    var result = try assembler.acceptFrame(&second);
    switch (result) {
        .accepted => unreachable,
        .complete => |*complete| {
            defer complete.deinit(allocator);
            try std.testing.expectEqual(@as(i32, 77), complete.transfer_id);
            try std.testing.expectEqual(cmd.OrderCommandType.binary_data_command, complete.command_type);
            try std.testing.expectEqual(@as(usize, 10), complete.words.len);
            try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, complete.words);
        },
    }
    try std.testing.expectEqual(@as(usize, 0), assembler.pendingTransfers());
}

test "binary frame assembler supports query transfers" {
    const allocator = std.testing.allocator;
    var assembler = BinaryFrameAssembler.init(allocator);
    defer assembler.deinit();

    const first = makeFrame(.binary_data_query, 13, 0, .{ 101, 102, 103, 104, 105 });
    try std.testing.expectEqual(AcceptResult.accepted, try assembler.acceptFrame(&first));

    const second = makeFrame(.binary_data_query, 13, -1, .{ 106, 107, 108, 109, 110 });
    var result = try assembler.acceptFrame(&second);
    switch (result) {
        .accepted => unreachable,
        .complete => |*complete| {
            defer complete.deinit(allocator);
            try std.testing.expectEqual(cmd.OrderCommandType.binary_data_query, complete.command_type);
            try std.testing.expectEqualSlices(i64, &[_]i64{ 101, 102, 103, 104, 105, 106, 107, 108, 109, 110 }, complete.words);
        },
    }
}

test "binary frame assembler rejects invalid flags and command mismatches" {
    const allocator = std.testing.allocator;
    var assembler = BinaryFrameAssembler.init(allocator);
    defer assembler.deinit();

    const invalid_flag = makeFrame(.binary_data_command, 21, 5, .{ 1, 1, 1, 1, 1 });
    try std.testing.expectError(error.InvalidLastFlag, assembler.acceptFrame(&invalid_flag));
    try std.testing.expectEqual(@as(usize, 0), assembler.pendingTransfers());

    const first = makeFrame(.binary_data_command, 22, 0, .{ 2, 2, 2, 2, 2 });
    try std.testing.expectEqual(AcceptResult.accepted, try assembler.acceptFrame(&first));
    const mismatch = makeFrame(.binary_data_query, 22, -1, .{ 3, 3, 3, 3, 3 });
    try std.testing.expectError(error.TransferTypeMismatch, assembler.acceptFrame(&mismatch));
    try std.testing.expectEqual(@as(usize, 1), assembler.pendingTransfers());

    const final_frame = makeFrame(.binary_data_command, 22, -1, .{ 4, 4, 4, 4, 4 });
    var completed = try assembler.acceptFrame(&final_frame);
    switch (completed) {
        .accepted => unreachable,
        .complete => |*complete| {
            defer complete.deinit(allocator);
            try std.testing.expectEqual(@as(usize, 10), complete.words.len);
        },
    }
}

test "binary frame assembler rejects non-binary command types and reset clears state" {
    const allocator = std.testing.allocator;
    var assembler = BinaryFrameAssembler.init(allocator);
    defer assembler.deinit();

    const not_binary = makeFrame(.add_user, 90, 0, .{ 7, 7, 7, 7, 7 });
    try std.testing.expectError(error.UnsupportedCommandType, assembler.acceptFrame(&not_binary));

    const pending = makeFrame(.binary_data_command, 91, 0, .{ 8, 8, 8, 8, 8 });
    try std.testing.expectEqual(AcceptResult.accepted, try assembler.acceptFrame(&pending));
    try std.testing.expectEqual(@as(usize, 1), assembler.pendingTransfers());
    assembler.reset();
    try std.testing.expectEqual(@as(usize, 0), assembler.pendingTransfers());
}

test "binary frame assembler transfer record helpers detect missing ids" {
    const allocator = std.testing.allocator;
    var assembler = BinaryFrameAssembler.init(allocator);
    defer assembler.deinit();

    try std.testing.expectError(
        error.TransferStateCorrupted,
        assembler.getTransferRecord(101),
    );
    try std.testing.expectError(
        error.TransferStateCorrupted,
        assembler.removeTransferRecord(101),
    );

    const frame = makeFrame(.binary_data_command, 101, 0, .{ 1, 2, 3, 4, 5 });
    _ = try assembler.acceptFrame(&frame);

    const record = try assembler.getTransferRecord(101);
    try std.testing.expectEqual(cmd.OrderCommandType.binary_data_command, record.command_type);
}

test "binary frame assembler state hash is deterministic and mutation sensitive" {
    const allocator = std.testing.allocator;
    var left = BinaryFrameAssembler.init(allocator);
    defer left.deinit();
    var right = BinaryFrameAssembler.init(allocator);
    defer right.deinit();

    const a = makeFrame(.binary_data_query, 100, 0, .{ 1, 2, 3, 4, 5 });
    const b = makeFrame(.binary_data_command, 200, 0, .{ 6, 7, 8, 9, 10 });
    _ = try left.acceptFrame(&a);
    _ = try left.acceptFrame(&b);
    _ = try right.acceptFrame(&b);
    _ = try right.acceptFrame(&a);

    try std.testing.expectEqual(try left.stateHash(), try right.stateHash());

    const c = makeFrame(.binary_data_query, 100, -1, .{ 11, 12, 13, 14, 15 });
    var complete = try right.acceptFrame(&c);
    switch (complete) {
        .accepted => unreachable,
        .complete => |*transfer| transfer.deinit(allocator),
    }
    try std.testing.expect((try left.stateHash()) != (try right.stateHash()));
}

test "binary frame assembler pending state capture and restore is deterministic" {
    const allocator = std.testing.allocator;
    var source = BinaryFrameAssembler.init(allocator);
    defer source.deinit();
    var restored = BinaryFrameAssembler.init(allocator);
    defer restored.deinit();

    const high = makeFrame(.binary_data_command, 200, 0, .{ 6, 7, 8, 9, 10 });
    const low = makeFrame(.binary_data_query, 100, 0, .{ 1, 2, 3, 4, 5 });
    _ = try source.acceptFrame(&high);
    _ = try source.acceptFrame(&low);

    var pending = try source.capturePendingState(allocator);
    defer pending.deinit();
    try std.testing.expectEqual(@as(usize, 2), pending.transfers.len);
    try std.testing.expectEqual(@as(i32, 100), pending.transfers[0].transfer_id);
    try std.testing.expectEqual(@as(i32, 200), pending.transfers[1].transfer_id);

    try restored.restorePendingState(pending.transfers);
    try std.testing.expectEqual(try source.stateHash(), try restored.stateHash());
}

test "binary frame assembler pending state restore preserves live state on allocation failure" {
    var failing_allocator_state = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = failing_allocator_state.allocator();
    var assembler = BinaryFrameAssembler.init(allocator);
    defer {
        failing_allocator_state.fail_index = std.math.maxInt(usize);
        assembler.deinit();
    }

    const live_frame = makeFrame(.binary_data_query, 100, 0, .{ 1, 2, 3, 4, 5 });
    _ = try assembler.acceptFrame(&live_frame);
    const live_hash = try assembler.stateHash();

    var replacement_words = [_]i64{ 6, 7, 8, 9, 10 };
    const replacement = [_]PendingTransfer{.{
        .transfer_id = 200,
        .command_type = .binary_data_command,
        .words = &replacement_words,
    }};

    failing_allocator_state.fail_index = failing_allocator_state.alloc_index;
    try std.testing.expectError(error.OutOfMemory, assembler.restorePendingState(&replacement));
    try std.testing.expect(failing_allocator_state.has_induced_failure);

    failing_allocator_state.fail_index = std.math.maxInt(usize);
    try std.testing.expectEqual(@as(usize, 1), assembler.pendingTransfers());
    try std.testing.expectEqual(live_hash, try assembler.stateHash());
}

test "legacy Java untyped pending state binds to the first later frame type" {
    const allocator = std.testing.allocator;
    var assembler = BinaryFrameAssembler.init(allocator);
    defer assembler.deinit();

    const backing = [_]i64{ 1, 2, 3, 4, 5, 0, 0, 0, 0, 0 };
    try assembler.restoreLegacyUntypedPendingState(&.{.{
        .transfer_id = 77,
        .words_transferred = 5,
        .data = &backing,
    }});

    var pending = try assembler.capturePendingState(allocator);
    defer pending.deinit();
    try std.testing.expectEqual(@as(usize, 1), pending.transfers.len);
    try std.testing.expectEqual(@as(?cmd.OrderCommandType, null), pending.transfers[0].command_type);
    try std.testing.expectEqualSlices(i64, backing[0..5], pending.transfers[0].words);

    const final = makeFrame(.binary_data_query, 77, -1, .{ 6, 7, 8, 9, 10 });
    var completed = try assembler.acceptFrame(&final);
    switch (completed) {
        .accepted => return error.TestUnexpectedResult,
        .complete => |*transfer| {
            defer transfer.deinit(allocator);
            try std.testing.expectEqual(cmd.OrderCommandType.binary_data_query, transfer.command_type);
            try std.testing.expectEqualSlices(
                i64,
                &.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 },
                transfer.words,
            );
        },
    }
    try std.testing.expectEqual(@as(usize, 0), assembler.pendingTransfers());
}

test "legacy Java untyped pending state validates transactionally and binds once" {
    const allocator = std.testing.allocator;
    var assembler = BinaryFrameAssembler.init(allocator);
    defer assembler.deinit();

    const existing = makeFrame(.binary_data_command, 5, 0, .{ 1, 2, 3, 4, 5 });
    _ = try assembler.acceptFrame(&existing);
    const before = try assembler.stateHash();
    const words = [_]i64{ 1, 2, 3, 4, 5 };

    const invalid = [_]LegacyUntypedPendingTransfer{
        .{ .transfer_id = @as(i64, std.math.maxInt(i32)) + 1, .words_transferred = 5, .data = &words },
        .{ .transfer_id = 1, .words_transferred = 0, .data = &words },
        .{ .transfer_id = 1, .words_transferred = 4, .data = &words },
        .{ .transfer_id = 1, .words_transferred = 10, .data = &words },
    };
    for (invalid) |entry| {
        try std.testing.expectError(
            error.TransferStateCorrupted,
            assembler.restoreLegacyUntypedPendingState(&.{entry}),
        );
        try std.testing.expectEqual(before, try assembler.stateHash());
    }
    try std.testing.expectError(
        error.TransferStateCorrupted,
        assembler.restoreLegacyUntypedPendingState(&.{
            .{ .transfer_id = 9, .words_transferred = 5, .data = &words },
            .{ .transfer_id = 9, .words_transferred = 5, .data = &words },
        }),
    );
    try std.testing.expectEqual(before, try assembler.stateHash());

    try assembler.restoreLegacyUntypedPendingState(&.{.{
        .transfer_id = 9,
        .words_transferred = 5,
        .data = &words,
    }});
    const bind = makeFrame(.binary_data_query, 9, 0, .{ 6, 7, 8, 9, 10 });
    _ = try assembler.acceptFrame(&bind);
    const wrong = makeFrame(.binary_data_command, 9, -1, .{ 11, 12, 13, 14, 15 });
    try std.testing.expectError(error.TransferTypeMismatch, assembler.acceptFrame(&wrong));
}

test "legacy Java untyped pending restore releases every allocation failure" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        legacyUntypedRestoreAllocationTest,
        .{},
    );
}

test "Java binary processor hash matches source-derived command and query vectors" {
    const allocator = std.testing.allocator;
    var left = BinaryFrameAssembler.init(allocator);
    defer left.deinit();
    var right = BinaryFrameAssembler.init(allocator);
    defer right.deinit();

    // Generated with Java 25 Arrays.hashCode, Objects.hash, and Long.hashCode
    // using BinaryCommandsProcessor.java lines 205-247 and HashingUtils.java
    // lines 37-42. The command has capacity 5; the query has capacity 10 and
    // therefore hashes a five-long zero tail.
    const command = makeFrame(
        .binary_data_command,
        7,
        0,
        .{ (@as(i64, 1) << 32) | 0x0102_0304, 2, 3, 4, 5 },
    );
    const query = makeFrame(
        .binary_data_query,
        -11,
        0,
        .{
            (@as(i64, 41) << 32) | 0x1122_3344,
            -2,
            std.math.maxInt(i64),
            std.math.minInt(i64),
            99,
        },
    );

    _ = try left.acceptFrame(&command);
    try std.testing.expectEqual(@as(i32, 1_403_783_301), try left.javaStateHash32());
    _ = try left.acceptFrame(&query);
    try std.testing.expectEqual(@as(i32, 1_594_454_010), try left.javaStateHash32());

    _ = try right.acceptFrame(&query);
    _ = try right.acceptFrame(&command);
    try std.testing.expectEqual(try left.javaStateHash32(), try right.javaStateHash32());

    var pending = try left.capturePendingState(allocator);
    defer pending.deinit();
    var restored = BinaryFrameAssembler.init(allocator);
    defer restored.deinit();
    try restored.restorePendingState(pending.transfers);
    try std.testing.expectEqual(try left.stateHash(), try restored.stateHash());
    try std.testing.expectEqual(try left.javaStateHash32(), try restored.javaStateHash32());
}

test "Java backing capacity follows envelope rounding and exact doubling boundaries" {
    const allocator = std.testing.allocator;
    var assembler = BinaryFrameAssembler.init(allocator);
    defer assembler.deinit();

    const bytes_40 = makeFrame(.binary_data_command, 40, 0, .{
        (@as(i64, 40) << 32) | 1, 2, 3, 4, 5,
    });
    const bytes_41 = makeFrame(.binary_data_query, 41, 0, .{
        (@as(i64, 41) << 32) | 1, 2, 3, 4, 5,
    });
    _ = try assembler.acceptFrame(&bytes_40);
    _ = try assembler.acceptFrame(&bytes_41);

    var pending = try assembler.capturePendingState(allocator);
    defer pending.deinit();
    try std.testing.expectEqual(@as(?u32, 5), pending.transfers[0].java_backing_capacity);
    try std.testing.expectEqual(@as(?u32, 10), pending.transfers[1].java_backing_capacity);

    assembler.reset();
    const first = makeFrame(.binary_data_command, 7, 0, .{
        (@as(i64, 1) << 32) | 0x0102_0304, 2, 3, 4, 5,
    });
    const second = makeFrame(.binary_data_command, 7, 0, .{ 6, 7, 8, 9, 10 });
    const third = makeFrame(.binary_data_command, 7, 0, .{ 11, 12, 13, 14, 15 });
    _ = try assembler.acceptFrame(&first);
    try std.testing.expectEqual(@as(i32, 1_403_783_301), try assembler.javaStateHash32());
    _ = try assembler.acceptFrame(&second);
    try std.testing.expectEqual(@as(i32, 483_063_747), try assembler.javaStateHash32());
    _ = try assembler.acceptFrame(&third);
    try std.testing.expectEqual(@as(i32, 2_142_397_973), try assembler.javaStateHash32());

    var doubled = try assembler.capturePendingState(allocator);
    defer doubled.deinit();
    try std.testing.expectEqual(@as(?u32, 20), doubled.transfers[0].java_backing_capacity);
}

test "Java hash handles the maximum request envelope in logarithmic work" {
    const allocator = std.testing.allocator;
    var assembler = BinaryFrameAssembler.init(allocator);
    defer assembler.deinit();

    const frame = makeFrame(.binary_data_command, 123, 0, .{
        (@as(i64, 0x7fff_ffff) << 32) | 0x5566_7788, 2, 3, 4, 5,
    });
    _ = try assembler.acceptFrame(&frame);

    var pending = try assembler.capturePendingState(allocator);
    defer pending.deinit();
    try std.testing.expectEqual(@as(?u32, 268_435_460), pending.transfers[0].java_backing_capacity);
    try std.testing.expectEqual(@as(i32, 1_160_226_683), try assembler.javaStateHash32());
}

test "legacy untyped transfer preserves Java capacity and rejects a nonzero tail" {
    const allocator = std.testing.allocator;
    var assembler = BinaryFrameAssembler.init(allocator);
    defer assembler.deinit();

    const backing = [_]i64{
        (@as(i64, 41) << 32) | 0x1122_3344,
        -2,
        std.math.maxInt(i64),
        std.math.minInt(i64),
        99,
        0,
        0,
        0,
        0,
        0,
    };
    try assembler.restoreLegacyUntypedPendingState(&.{.{
        .transfer_id = -11,
        .words_transferred = 5,
        .data = &backing,
    }});
    try std.testing.expectEqual(@as(i32, 190_670_708), try assembler.javaStateHash32());

    var pending = try assembler.capturePendingState(allocator);
    defer pending.deinit();
    try std.testing.expectEqual(@as(?cmd.OrderCommandType, null), pending.transfers[0].command_type);
    try std.testing.expectEqual(@as(?u32, 10), pending.transfers[0].java_backing_capacity);

    var invalid = backing;
    invalid[9] = 1;
    try std.testing.expectError(
        error.TransferStateCorrupted,
        assembler.restoreLegacyUntypedPendingState(&.{.{
            .transfer_id = -11,
            .words_transferred = 5,
            .data = &invalid,
        }}),
    );
    try std.testing.expectEqual(@as(i32, 190_670_708), try assembler.javaStateHash32());
}

test "Java hash is explicitly unavailable when old native state cannot reconstruct capacity" {
    const allocator = std.testing.allocator;
    var assembler = BinaryFrameAssembler.init(allocator);
    defer assembler.deinit();

    var opaque_words = [_]i64{ 1, 2, 3, 4, 5 };
    try assembler.restorePendingState(&.{.{
        .transfer_id = 1,
        .command_type = null,
        .words = &opaque_words,
    }});
    try std.testing.expectEqual(@as(usize, 1), assembler.pendingTransfers());
    try std.testing.expectError(error.JavaStateHashUnavailable, assembler.javaStateHash32());

    try std.testing.expectError(
        error.TransferStateCorrupted,
        assembler.restorePendingState(&.{.{
            .transfer_id = 1,
            .command_type = null,
            .java_backing_capacity = 5,
            .words = &opaque_words,
        }}),
    );
}

test "native pending restore preserves explicit unavailable Java capacity while resuming" {
    const allocator = std.testing.allocator;
    var assembler = BinaryFrameAssembler.init(allocator);
    defer assembler.deinit();

    var derivable_words = [_]i64{
        (@as(i64, 41) << 32) | 0x1122_3344,
        -2,
        std.math.maxInt(i64),
        std.math.minInt(i64),
        99,
    };
    try assembler.restorePendingState(&.{.{
        .transfer_id = -11,
        .command_type = null,
        .java_backing_capacity = null,
        .words = &derivable_words,
    }});

    var restored = try assembler.capturePendingState(allocator);
    defer restored.deinit();
    try std.testing.expectEqual(@as(?u32, null), restored.transfers[0].java_backing_capacity);
    try std.testing.expectError(error.JavaStateHashUnavailable, assembler.javaStateHash32());

    const resumed_frame = makeFrame(.binary_data_query, -11, 0, .{ 6, 7, 8, 9, 10 });
    _ = try assembler.acceptFrame(&resumed_frame);
    var resumed = try assembler.capturePendingState(allocator);
    defer resumed.deinit();
    try std.testing.expectEqual(@as(?cmd.OrderCommandType, .binary_data_query), resumed.transfers[0].command_type);
    try std.testing.expectEqual(@as(?u32, null), resumed.transfers[0].java_backing_capacity);
    try std.testing.expectError(error.JavaStateHashUnavailable, assembler.javaStateHash32());
}

test "existing intermediate and terminal frame OOM leave pending state unchanged" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = failing.allocator();
    var assembler = BinaryFrameAssembler.init(allocator);
    defer {
        failing.fail_index = std.math.maxInt(usize);
        assembler.deinit();
    }

    const words = [_]i64{
        (@as(i64, 41) << 32) | 0x1122_3344,
        -2,
        std.math.maxInt(i64),
        std.math.minInt(i64),
        99,
    };
    const owned_words = try allocator.dupe(i64, &words);
    assembler.transfers.put(allocator, 77, .{
        .command_type = null,
        .java_backing_capacity = null,
        .words = .{ .items = owned_words, .capacity = owned_words.len },
    }) catch |err| {
        allocator.free(owned_words);
        return err;
    };
    const before_hash = try assembler.stateHash();

    const intermediate = makeFrame(.binary_data_query, 77, 0, .{ 6, 7, 8, 9, 10 });
    failing.fail_index = failing.alloc_index;
    try std.testing.expectError(error.OutOfMemory, assembler.acceptFrame(&intermediate));
    try std.testing.expect(failing.has_induced_failure);
    failing.fail_index = std.math.maxInt(usize);
    try std.testing.expectEqual(before_hash, try assembler.stateHash());
    var after_intermediate = try assembler.capturePendingState(std.testing.allocator);
    defer after_intermediate.deinit();
    try std.testing.expectEqual(@as(?cmd.OrderCommandType, null), after_intermediate.transfers[0].command_type);
    try std.testing.expectEqual(@as(?u32, null), after_intermediate.transfers[0].java_backing_capacity);
    try std.testing.expectEqualSlices(i64, &words, after_intermediate.transfers[0].words);

    failing.has_induced_failure = false;
    const terminal = makeFrame(.binary_data_query, 77, -1, .{ 6, 7, 8, 9, 10 });
    failing.fail_index = failing.alloc_index;
    try std.testing.expectError(error.OutOfMemory, assembler.acceptFrame(&terminal));
    try std.testing.expect(failing.has_induced_failure);
    failing.fail_index = std.math.maxInt(usize);
    try std.testing.expectEqual(before_hash, try assembler.stateHash());

    var completed = try assembler.acceptFrame(&terminal);
    defer switch (completed) {
        .accepted => {},
        .complete => |*transfer| transfer.deinit(allocator),
    };
    switch (completed) {
        .accepted => return error.TestUnexpectedResult,
        .complete => |transfer| try std.testing.expectEqualSlices(
            i64,
            &.{
                (@as(i64, 41) << 32) | 0x1122_3344,
                -2,
                std.math.maxInt(i64),
                std.math.minInt(i64),
                99,
                6,
                7,
                8,
                9,
                10,
            },
            transfer.words,
        ),
    }
    try std.testing.expectEqual(@as(usize, 0), assembler.pendingTransfers());
}

test "completed query is removed before Java report-state hashing" {
    const allocator = std.testing.allocator;
    var assembler = BinaryFrameAssembler.init(allocator);
    defer assembler.deinit();

    const first = makeFrame(.binary_data_query, 88, 0, .{
        (@as(i64, 41) << 32) | 7, 2, 3, 4, 5,
    });
    const last = makeFrame(.binary_data_query, 88, -1, .{ 6, 7, 8, 9, 10 });
    _ = try assembler.acceptFrame(&first);
    var completed = try assembler.acceptFrame(&last);
    defer switch (completed) {
        .accepted => {},
        .complete => |*transfer| transfer.deinit(allocator),
    };

    try std.testing.expectEqual(@as(usize, 0), assembler.pendingTransfers());
    try std.testing.expectEqual(@as(i32, 0), try assembler.javaStateHash32());
}

test "Java state hash performs no allocation after state construction" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = failing.allocator();
    var assembler = BinaryFrameAssembler.init(allocator);
    defer {
        failing.fail_index = std.math.maxInt(usize);
        assembler.deinit();
    }

    const frame = makeFrame(.binary_data_command, 7, 0, .{
        (@as(i64, 1) << 32) | 0x0102_0304, 2, 3, 4, 5,
    });
    _ = try assembler.acceptFrame(&frame);
    failing.fail_index = failing.alloc_index;
    try std.testing.expectEqual(@as(i32, 1_403_783_301), try assembler.javaStateHash32());
    try std.testing.expect(!failing.has_induced_failure);
}

test "new transfer append failure removes the provisional Java hash record" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = failing.allocator();
    var assembler = BinaryFrameAssembler.init(allocator);
    defer {
        failing.fail_index = std.math.maxInt(usize);
        assembler.deinit();
    }
    try assembler.transfers.ensureTotalCapacity(allocator, 1);

    const frame = makeFrame(.binary_data_command, 7, 0, .{
        (@as(i64, 1) << 32) | 0x0102_0304, 2, 3, 4, 5,
    });
    failing.fail_index = failing.alloc_index;
    try std.testing.expectError(error.OutOfMemory, assembler.acceptFrame(&frame));
    try std.testing.expect(failing.has_induced_failure);
    try std.testing.expectEqual(@as(usize, 0), assembler.pendingTransfers());
    try std.testing.expectEqual(@as(i32, 0), try assembler.javaStateHash32());
}
