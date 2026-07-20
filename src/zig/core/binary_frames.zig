const std = @import("std");
const cmd = @import("cmd.zig");

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

const TransferRecord = struct {
    command_type: cmd.OrderCommandType,
    words: std.ArrayListUnmanaged(i64) = .empty,

    fn deinit(self: *TransferRecord, allocator: std.mem.Allocator) void {
        self.words.deinit(allocator);
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
            hashI8(&hasher, @intFromEnum(record.command_type));
            hashU64(&hasher, @as(u64, @intCast(record.words.items.len)));
            for (record.words.items) |word| {
                hashI64(&hasher, word);
            }
        }

        return hasher.final();
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

        const gop = try self.transfers.getOrPut(self.allocator, frame.user_cookie);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{
                .command_type = frame.command,
            };
        } else if (gop.value_ptr.command_type != frame.command) {
            return error.TransferTypeMismatch;
        }

        try gop.value_ptr.words.appendSlice(self.allocator, &[_]i64{
            frame.order_id,
            frame.price,
            frame.reserve_bid_price,
            frame.size,
            frame.uid,
        });

        if (frame.symbol == 0) return .accepted;

        const owned_words = try self.allocator.dupe(i64, gop.value_ptr.words.items);
        var record = try self.removeTransferRecord(frame.user_cookie);
        record.deinit(self.allocator);

        return .{
            .complete = .{
                .transfer_id = frame.user_cookie,
                .command_type = frame.command,
                .words = owned_words,
            },
        };
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

fn isBinaryFrameCommand(command_type: cmd.OrderCommandType) bool {
    return switch (command_type) {
        .binary_data_command, .binary_data_query => true,
        else => false,
    };
}

fn lessThanI32(_: void, lhs: i32, rhs: i32) bool {
    return lhs < rhs;
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
