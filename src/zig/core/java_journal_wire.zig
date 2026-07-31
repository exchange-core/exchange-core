const std = @import("std");
const cmd = @import("cmd.zig");
const common = @import("common.zig");
const java_binary_wire = @import("java_binary_wire.zig");

/// Safety limits for one upstream Java `.ecj` byte stream. Java itself caps
/// compressed and original block lengths at 1,000,000 bytes when reading.
pub const Config = struct {
    max_file_bytes: usize = 1024 * 1024 * 1024,
    max_compressed_block_bytes: usize = 1_000_000,
    max_original_block_bytes: usize = 1_000_000,
    max_compressed_blocks: usize = 1_000_000,
    max_entries: usize = 10_000_000,
    /// Mirrors `MutableLong lastSeq`, which starts at zero for Java's full
    /// replay and is carried across journal partitions.
    initial_sequence: i64 = 0,
};

pub const DecodeError = std.mem.Allocator.Error || java_binary_wire.RawLz4DecodeError || error{
    FileTooLarge,
    TruncatedRecord,
    TruncatedCompressedHeader,
    TruncatedCompressedBlock,
    NegativeCompressedLength,
    NegativeOriginalLength,
    EmptyCompressedBlock,
    CompressedBlockTooLarge,
    OriginalBlockTooLarge,
    TooManyCompressedBlocks,
    TooManyEntries,
    NestedCompressedBlock,
    CompressedLengthMismatch,
    DecompressedLengthMismatch,
    UnknownCommandCode,
    NonMutatingCommand,
    InvalidOrderActionCode,
    InvalidOrderTypeCode,
    InvalidBalanceAdjustmentTypeCode,
    SequenceNotIncreasing,
};

/// The Java writer emits persistence commands but deliberately does not emit
/// their `orderId` (snapshot id): the payload write in `writeToJournal` is
/// commented out upstream. Keep the records visible for sequence accounting,
/// while making the missing payload impossible to overlook during integration.
pub const PersistenceRecord = enum {
    none,
    matching_snapshot_id_not_encoded,
    risk_snapshot_id_not_encoded,
};

pub const Entry = struct {
    java_sequence: i64,
    command: cmd.OrderCommand,
    /// Present only for BALANCE_ADJUSTMENT. `OrderCommand.order_type` also
    /// carries the Java replay mapping (ADJUSTMENT=>GTC, SUSPEND=>IOC).
    balance_adjustment_type: ?common.BalanceAdjustmentType = null,
    persistence_record: PersistenceRecord = .none,

    pub fn isReplayable(self: Entry) bool {
        return self.persistence_record == .none;
    }
};

/// Java accepts forward sequence gaps and logs the two endpoints. Preserve
/// exactly those endpoints instead of relying on logging side effects.
pub const SequenceGap = struct {
    previous_sequence: i64,
    next_sequence: i64,
};

pub const DecodedJournal = struct {
    allocator: std.mem.Allocator,
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    sequence_gaps: std.ArrayListUnmanaged(SequenceGap) = .empty,
    compressed_block_count: usize = 0,
    last_sequence: i64,

    pub fn deinit(self: *DecodedJournal) void {
        self.entries.deinit(self.allocator);
        self.sequence_gaps.deinit(self.allocator);
        self.* = undefined;
    }
};

pub fn decode(
    allocator: std.mem.Allocator,
    file_bytes: []const u8,
    config: Config,
) DecodeError!DecodedJournal {
    if (file_bytes.len > config.max_file_bytes) return error.FileTooLarge;

    var result: DecodedJournal = .{
        .allocator = allocator,
        .last_sequence = config.initial_sequence,
    };
    errdefer result.deinit();

    var decoder: Decoder = .{
        .allocator = allocator,
        .config = config,
        .result = &result,
    };
    try decoder.decodeStream(file_bytes, false);
    return result;
}

const Decoder = struct {
    allocator: std.mem.Allocator,
    config: Config,
    result: *DecodedJournal,

    fn decodeStream(
        self: *Decoder,
        bytes: []const u8,
        inside_compressed_block: bool,
    ) DecodeError!void {
        var cursor: Cursor = .{ .bytes = bytes };
        while (cursor.remaining() != 0) {
            const raw_code = cursor.readByte() orelse unreachable;
            if (raw_code == 0xff) {
                try self.decodeCompressedBlock(&cursor, inside_compressed_block);
            } else {
                try self.decodeRecord(&cursor, raw_code);
            }
        }
    }

    fn decodeCompressedBlock(
        self: *Decoder,
        cursor: *Cursor,
        inside_compressed_block: bool,
    ) DecodeError!void {
        if (inside_compressed_block) return error.NestedCompressedBlock;
        if (self.result.compressed_block_count >= self.config.max_compressed_blocks) {
            return error.TooManyCompressedBlocks;
        }

        const compressed_len_i32 = cursor.readInt(i32) orelse
            return error.TruncatedCompressedHeader;
        const original_len_i32 = cursor.readInt(i32) orelse
            return error.TruncatedCompressedHeader;
        if (compressed_len_i32 < 0) return error.NegativeCompressedLength;
        if (original_len_i32 < 0) return error.NegativeOriginalLength;

        const compressed_len: usize = @intCast(compressed_len_i32);
        const original_len: usize = @intCast(original_len_i32);
        if (compressed_len == 0 or original_len == 0) return error.EmptyCompressedBlock;
        if (compressed_len > self.config.max_compressed_block_bytes) {
            return error.CompressedBlockTooLarge;
        }
        if (original_len > self.config.max_original_block_bytes) {
            return error.OriginalBlockTooLarge;
        }

        const compressed = cursor.take(compressed_len) orelse
            return error.TruncatedCompressedBlock;
        self.result.compressed_block_count += 1;

        var decoded = java_binary_wire.decodeRawLz4(
            self.allocator,
            compressed,
            original_len,
        ) catch |err| switch (err) {
            error.OutputOverrun, error.OutputUnderrun => return error.DecompressedLengthMismatch,
            else => return err,
        };
        defer decoded.deinit(self.allocator);
        if (decoded.compressed_bytes_consumed != compressed.len) {
            return error.CompressedLengthMismatch;
        }

        try self.decodeStream(decoded.bytes, true);
    }

    fn decodeRecord(self: *Decoder, cursor: *Cursor, raw_code: u8) DecodeError!void {
        // DataInputStream reads these mandatory fields before dispatching the
        // command type. All integer fields are Java ByteBuffer big-endian.
        const sequence = cursor.readInt(i64) orelse return error.TruncatedRecord;
        const timestamp = cursor.readInt(i64) orelse return error.TruncatedRecord;
        const service_flags = cursor.readInt(i32) orelse return error.TruncatedRecord;
        const events_group = cursor.readInt(i64) orelse return error.TruncatedRecord;

        const command_type = cmd.OrderCommandType.fromCode(@bitCast(raw_code)) catch
            return error.UnknownCommandCode;
        if (!command_type.isMutating()) return error.NonMutatingCommand;

        var entry: Entry = .{
            .java_sequence = sequence,
            .command = .{
                .command = command_type,
                .timestamp = timestamp,
                .service_flags = service_flags,
                .events_group = events_group,
                .result_code = .new,
            },
        };

        switch (command_type) {
            .move_order => {
                entry.command.uid = cursor.readInt(i64) orelse return error.TruncatedRecord;
                entry.command.symbol = cursor.readInt(i32) orelse return error.TruncatedRecord;
                entry.command.order_id = cursor.readInt(i64) orelse return error.TruncatedRecord;
                entry.command.price = cursor.readInt(i64) orelse return error.TruncatedRecord;
            },
            .cancel_order => {
                entry.command.uid = cursor.readInt(i64) orelse return error.TruncatedRecord;
                entry.command.symbol = cursor.readInt(i32) orelse return error.TruncatedRecord;
                entry.command.order_id = cursor.readInt(i64) orelse return error.TruncatedRecord;
            },
            .reduce_order => {
                entry.command.uid = cursor.readInt(i64) orelse return error.TruncatedRecord;
                entry.command.symbol = cursor.readInt(i32) orelse return error.TruncatedRecord;
                entry.command.order_id = cursor.readInt(i64) orelse return error.TruncatedRecord;
                entry.command.size = cursor.readInt(i64) orelse return error.TruncatedRecord;
            },
            .place_order => {
                entry.command.uid = cursor.readInt(i64) orelse return error.TruncatedRecord;
                entry.command.symbol = cursor.readInt(i32) orelse return error.TruncatedRecord;
                entry.command.order_id = cursor.readInt(i64) orelse return error.TruncatedRecord;
                entry.command.price = cursor.readInt(i64) orelse return error.TruncatedRecord;
                entry.command.reserve_bid_price = cursor.readInt(i64) orelse return error.TruncatedRecord;
                entry.command.size = cursor.readInt(i64) orelse return error.TruncatedRecord;
                entry.command.user_cookie = cursor.readInt(i32) orelse return error.TruncatedRecord;

                // Java packs order type in the high seven bits and action in
                // the low bit. Consequently an encoded action is necessarily
                // 0 or 1; both values are still decoded through the enum gate.
                const action_and_type = cursor.readByte() orelse return error.TruncatedRecord;
                const action_code: i8 = @intCast(action_and_type & 1);
                const order_type_code: i8 = @intCast(action_and_type >> 1);
                entry.command.action = common.OrderAction.fromCode(action_code) catch
                    return error.InvalidOrderActionCode;
                entry.command.order_type = common.OrderType.fromCode(order_type_code) catch
                    return error.InvalidOrderTypeCode;
            },
            .balance_adjustment => {
                entry.command.uid = cursor.readInt(i64) orelse return error.TruncatedRecord;
                entry.command.symbol = cursor.readInt(i32) orelse return error.TruncatedRecord;
                entry.command.order_id = cursor.readInt(i64) orelse return error.TruncatedRecord;
                entry.command.price = cursor.readInt(i64) orelse return error.TruncatedRecord;
                const adjustment_code: i8 = @bitCast(
                    cursor.readByte() orelse return error.TruncatedRecord,
                );
                const adjustment_type = common.BalanceAdjustmentType.fromCode(adjustment_code) catch
                    return error.InvalidBalanceAdjustmentTypeCode;
                entry.balance_adjustment_type = adjustment_type;
                entry.command.order_type = switch (adjustment_type) {
                    .adjustment => .gtc,
                    .@"suspend" => .ioc,
                };
            },
            .add_user, .suspend_user, .resume_user => {
                entry.command.uid = cursor.readInt(i64) orelse return error.TruncatedRecord;
                // These sentinels are omitted by the writer but restored by
                // the corresponding Java ExchangeApi replay entrypoints.
                entry.command.order_id = -1;
                entry.command.symbol = -1;
            },
            .binary_data_command => {
                const last_flag: i8 = @bitCast(
                    cursor.readByte() orelse return error.TruncatedRecord,
                );
                entry.command.symbol = last_flag;
                entry.command.order_id = cursor.readInt(i64) orelse return error.TruncatedRecord;
                entry.command.price = cursor.readInt(i64) orelse return error.TruncatedRecord;
                entry.command.reserve_bid_price = cursor.readInt(i64) orelse return error.TruncatedRecord;
                entry.command.size = cursor.readInt(i64) orelse return error.TruncatedRecord;
                entry.command.uid = cursor.readInt(i64) orelse return error.TruncatedRecord;
            },
            .reset => {},
            .persist_state_matching => {
                entry.persistence_record = .matching_snapshot_id_not_encoded;
            },
            .persist_state_risk => {
                entry.persistence_record = .risk_snapshot_id_not_encoded;
            },
            .order_book_request,
            .binary_data_query,
            .grouping_control,
            .nop,
            .shutdown_signal,
            .reserved_compressed,
            => unreachable,
        }

        if (self.result.entries.items.len >= self.config.max_entries) {
            return error.TooManyEntries;
        }
        const previous_sequence = self.result.last_sequence;
        if (sequence <= previous_sequence) return error.SequenceNotIncreasing;
        // `sequence > previous_sequence` proves this addition cannot overflow.
        if (sequence > previous_sequence + 1) {
            try self.result.sequence_gaps.append(self.allocator, .{
                .previous_sequence = previous_sequence,
                .next_sequence = sequence,
            });
        }
        try self.result.entries.append(self.allocator, entry);
        self.result.last_sequence = sequence;
    }
};

const Cursor = struct {
    bytes: []const u8,
    position: usize = 0,

    fn remaining(self: Cursor) usize {
        return self.bytes.len - self.position;
    }

    fn take(self: *Cursor, len: usize) ?[]const u8 {
        if (len > self.remaining()) return null;
        const start = self.position;
        self.position += len;
        return self.bytes[start..self.position];
    }

    fn readByte(self: *Cursor) ?u8 {
        const bytes = self.take(1) orelse return null;
        return bytes[0];
    }

    fn readInt(self: *Cursor, comptime T: type) ?T {
        const bytes = self.take(@sizeOf(T)) orelse return null;
        return std.mem.readInt(T, bytes[0..@sizeOf(T)], .big);
    }
};

const raw_reset_golden = [_]u8{
    0x7c,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x01,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x02,
    0x00,
    0x00,
    0x00,
    0x03,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x04,
};

// Hand-verified raw LZ4 literal-only encoding of `raw_reset_golden`:
// token F0, one extended literal-length byte (29 - 15), then 29 literals.
// lz4-java 1.8.0's safe decompressor accepts this standards-valid block.
const compressed_reset_golden = [_]u8{
    0xff,
    0x00,
    0x00,
    0x00,
    0x1f,
    0x00,
    0x00,
    0x00,
    0x1d,
    0xf0,
    0x0e,
} ++ raw_reset_golden;

fn appendTestInt(
    bytes: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    comptime T: type,
    value: T,
) !void {
    var raw: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &raw, value, .big);
    try bytes.appendSlice(allocator, &raw);
}

fn appendTestHeader(
    bytes: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    raw_code: u8,
    sequence: i64,
    timestamp: i64,
    service_flags: i32,
    events_group: i64,
) !void {
    try bytes.append(allocator, raw_code);
    try appendTestInt(bytes, allocator, i64, sequence);
    try appendTestInt(bytes, allocator, i64, timestamp);
    try appendTestInt(bytes, allocator, i32, service_flags);
    try appendTestInt(bytes, allocator, i64, events_group);
}

fn appendTestCommandHeader(
    bytes: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    command_type: cmd.OrderCommandType,
    sequence: i64,
) !void {
    try appendTestHeader(
        bytes,
        allocator,
        @bitCast(command_type.code()),
        sequence,
        1_000 + sequence,
        @intCast(sequence),
        2_000 + sequence,
    );
}

fn appendTestPlaceRecord(
    bytes: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    action_and_type: u8,
) !void {
    try appendTestCommandHeader(bytes, allocator, .place_order, 1);
    try appendTestInt(bytes, allocator, i64, 101);
    try appendTestInt(bytes, allocator, i32, 202);
    try appendTestInt(bytes, allocator, i64, 303);
    try appendTestInt(bytes, allocator, i64, 404);
    try appendTestInt(bytes, allocator, i64, 505);
    try appendTestInt(bytes, allocator, i64, 606);
    try appendTestInt(bytes, allocator, i32, 707);
    try bytes.append(allocator, action_and_type);
}

fn expectDecodeFailure(bytes: []const u8) !void {
    if (decode(std.testing.allocator, bytes, .{})) |decoded_value| {
        var decoded = decoded_value;
        decoded.deinit();
        return error.TestUnexpectedResult;
    } else |_| {}
}

test "fixed raw and compressed Java journal goldens decode identically" {
    var raw = try decode(std.testing.allocator, &raw_reset_golden, .{});
    defer raw.deinit();
    var compressed = try decode(std.testing.allocator, &compressed_reset_golden, .{});
    defer compressed.deinit();

    try std.testing.expectEqual(@as(usize, 1), raw.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), compressed.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), raw.compressed_block_count);
    try std.testing.expectEqual(@as(usize, 1), compressed.compressed_block_count);
    try std.testing.expectEqual(@as(i64, 1), raw.entries.items[0].java_sequence);
    try std.testing.expectEqual(cmd.OrderCommandType.reset, raw.entries.items[0].command.command);
    try std.testing.expectEqual(@as(i64, 2), raw.entries.items[0].command.timestamp);
    try std.testing.expectEqual(@as(i32, 3), raw.entries.items[0].command.service_flags);
    try std.testing.expectEqual(@as(i64, 4), raw.entries.items[0].command.events_group);
    try std.testing.expectEqual(raw.entries.items[0], compressed.entries.items[0]);
}

test "raw and compressed batches share one sequence frontier" {
    const allocator = std.testing.allocator;
    var file_bytes: std.ArrayListUnmanaged(u8) = .empty;
    defer file_bytes.deinit(allocator);
    try appendTestCommandHeader(&file_bytes, allocator, .reset, 1);

    var inner: std.ArrayListUnmanaged(u8) = .empty;
    defer inner.deinit(allocator);
    try appendTestCommandHeader(&inner, allocator, .reset, 2);
    const compressed = try java_binary_wire.encodeRawLz4LiteralOnly(allocator, inner.items);
    defer allocator.free(compressed);
    try file_bytes.append(allocator, 0xff);
    try appendTestInt(&file_bytes, allocator, i32, @intCast(compressed.len));
    try appendTestInt(&file_bytes, allocator, i32, @intCast(inner.items.len));
    try file_bytes.appendSlice(allocator, compressed);

    try appendTestCommandHeader(&file_bytes, allocator, .reset, 3);
    var decoded = try decode(allocator, file_bytes.items, .{});
    defer decoded.deinit();
    try std.testing.expectEqual(@as(usize, 3), decoded.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), decoded.compressed_block_count);
    try std.testing.expectEqual(@as(usize, 0), decoded.sequence_gaps.items.len);
    try std.testing.expectEqual(@as(i64, 3), decoded.last_sequence);
}

test "all Java mutating command schemas preserve exact fields" {
    const allocator = std.testing.allocator;
    var bytes: std.ArrayListUnmanaged(u8) = .empty;
    defer bytes.deinit(allocator);

    try appendTestCommandHeader(&bytes, allocator, .move_order, 1);
    try appendTestInt(&bytes, allocator, i64, -101);
    try appendTestInt(&bytes, allocator, i32, 102);
    try appendTestInt(&bytes, allocator, i64, 103);
    try appendTestInt(&bytes, allocator, i64, 104);

    try appendTestCommandHeader(&bytes, allocator, .cancel_order, 2);
    try appendTestInt(&bytes, allocator, i64, 201);
    try appendTestInt(&bytes, allocator, i32, -202);
    try appendTestInt(&bytes, allocator, i64, 203);

    try appendTestCommandHeader(&bytes, allocator, .reduce_order, 3);
    try appendTestInt(&bytes, allocator, i64, 301);
    try appendTestInt(&bytes, allocator, i32, 302);
    try appendTestInt(&bytes, allocator, i64, 303);
    try appendTestInt(&bytes, allocator, i64, 304);

    try appendTestCommandHeader(&bytes, allocator, .place_order, 4);
    try appendTestInt(&bytes, allocator, i64, 401);
    try appendTestInt(&bytes, allocator, i32, 402);
    try appendTestInt(&bytes, allocator, i64, 403);
    try appendTestInt(&bytes, allocator, i64, 404);
    try appendTestInt(&bytes, allocator, i64, 405);
    try appendTestInt(&bytes, allocator, i64, 406);
    try appendTestInt(&bytes, allocator, i32, -407);
    try bytes.append(allocator, 0x09); // FOK_BUDGET (4), BID (1)

    try appendTestCommandHeader(&bytes, allocator, .balance_adjustment, 5);
    try appendTestInt(&bytes, allocator, i64, 501);
    try appendTestInt(&bytes, allocator, i32, 502);
    try appendTestInt(&bytes, allocator, i64, 503);
    try appendTestInt(&bytes, allocator, i64, -504);
    try bytes.append(allocator, 0x01); // BalanceAdjustmentType.SUSPEND

    try appendTestCommandHeader(&bytes, allocator, .add_user, 6);
    try appendTestInt(&bytes, allocator, i64, 601);
    try appendTestCommandHeader(&bytes, allocator, .suspend_user, 7);
    try appendTestInt(&bytes, allocator, i64, 701);
    try appendTestCommandHeader(&bytes, allocator, .resume_user, 8);
    try appendTestInt(&bytes, allocator, i64, 801);

    try appendTestCommandHeader(&bytes, allocator, .binary_data_command, 9);
    try bytes.append(allocator, 0xff); // signed Java byte -1
    try appendTestInt(&bytes, allocator, i64, 901);
    try appendTestInt(&bytes, allocator, i64, 902);
    try appendTestInt(&bytes, allocator, i64, 903);
    try appendTestInt(&bytes, allocator, i64, 904);
    try appendTestInt(&bytes, allocator, i64, 905);

    try appendTestCommandHeader(&bytes, allocator, .reset, 10);
    try appendTestCommandHeader(&bytes, allocator, .persist_state_matching, 11);
    try appendTestCommandHeader(&bytes, allocator, .persist_state_risk, 12);

    var decoded = try decode(allocator, bytes.items, .{});
    defer decoded.deinit();
    try std.testing.expectEqual(@as(usize, 12), decoded.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), decoded.sequence_gaps.items.len);
    try std.testing.expectEqual(@as(i64, 12), decoded.last_sequence);

    const move = decoded.entries.items[0];
    try std.testing.expectEqual(cmd.OrderCommandType.move_order, move.command.command);
    try std.testing.expectEqual(@as(i64, -101), move.command.uid);
    try std.testing.expectEqual(@as(i32, 102), move.command.symbol);
    try std.testing.expectEqual(@as(i64, 103), move.command.order_id);
    try std.testing.expectEqual(@as(i64, 104), move.command.price);
    try std.testing.expectEqual(@as(i64, 1_001), move.command.timestamp);
    try std.testing.expectEqual(@as(i32, 1), move.command.service_flags);
    try std.testing.expectEqual(@as(i64, 2_001), move.command.events_group);

    const cancel = decoded.entries.items[1].command;
    try std.testing.expectEqual(@as(i64, 201), cancel.uid);
    try std.testing.expectEqual(@as(i32, -202), cancel.symbol);
    try std.testing.expectEqual(@as(i64, 203), cancel.order_id);

    const reduce = decoded.entries.items[2].command;
    try std.testing.expectEqual(@as(i64, 301), reduce.uid);
    try std.testing.expectEqual(@as(i32, 302), reduce.symbol);
    try std.testing.expectEqual(@as(i64, 303), reduce.order_id);
    try std.testing.expectEqual(@as(i64, 304), reduce.size);

    const place = decoded.entries.items[3].command;
    try std.testing.expectEqual(@as(i64, 401), place.uid);
    try std.testing.expectEqual(@as(i32, 402), place.symbol);
    try std.testing.expectEqual(@as(i64, 403), place.order_id);
    try std.testing.expectEqual(@as(i64, 404), place.price);
    try std.testing.expectEqual(@as(i64, 405), place.reserve_bid_price);
    try std.testing.expectEqual(@as(i64, 406), place.size);
    try std.testing.expectEqual(@as(i32, -407), place.user_cookie);
    try std.testing.expectEqual(@as(?common.OrderAction, .bid), place.action);
    try std.testing.expectEqual(@as(?common.OrderType, .fok_budget), place.order_type);

    const adjustment = decoded.entries.items[4];
    try std.testing.expectEqual(@as(i64, 501), adjustment.command.uid);
    try std.testing.expectEqual(@as(i32, 502), adjustment.command.symbol);
    try std.testing.expectEqual(@as(i64, 503), adjustment.command.order_id);
    try std.testing.expectEqual(@as(i64, -504), adjustment.command.price);
    try std.testing.expectEqual(
        @as(?common.BalanceAdjustmentType, .@"suspend"),
        adjustment.balance_adjustment_type,
    );
    try std.testing.expectEqual(@as(?common.OrderType, .ioc), adjustment.command.order_type);

    for (decoded.entries.items[5..8], [_]i64{ 601, 701, 801 }) |entry, uid| {
        try std.testing.expectEqual(uid, entry.command.uid);
        try std.testing.expectEqual(@as(i64, -1), entry.command.order_id);
        try std.testing.expectEqual(@as(i32, -1), entry.command.symbol);
    }

    const binary = decoded.entries.items[8].command;
    try std.testing.expectEqual(@as(i32, -1), binary.symbol);
    try std.testing.expectEqual(@as(i64, 901), binary.order_id);
    try std.testing.expectEqual(@as(i64, 902), binary.price);
    try std.testing.expectEqual(@as(i64, 903), binary.reserve_bid_price);
    try std.testing.expectEqual(@as(i64, 904), binary.size);
    try std.testing.expectEqual(@as(i64, 905), binary.uid);

    try std.testing.expectEqual(cmd.OrderCommandType.reset, decoded.entries.items[9].command.command);
    try std.testing.expectEqual(
        PersistenceRecord.matching_snapshot_id_not_encoded,
        decoded.entries.items[10].persistence_record,
    );
    try std.testing.expectEqual(
        PersistenceRecord.risk_snapshot_id_not_encoded,
        decoded.entries.items[11].persistence_record,
    );
    try std.testing.expect(!decoded.entries.items[10].isReplayable());
    try std.testing.expect(!decoded.entries.items[11].isReplayable());
    try std.testing.expectEqual(@as(i64, 0), decoded.entries.items[10].command.order_id);
    try std.testing.expectEqual(@as(i64, 0), decoded.entries.items[11].command.order_id);
}

test "raw and compressed truncation fails at every nonempty boundary" {
    for (1..raw_reset_golden.len) |end| {
        try std.testing.expectError(
            error.TruncatedRecord,
            decode(std.testing.allocator, raw_reset_golden[0..end], .{}),
        );
    }

    var place_bytes: std.ArrayListUnmanaged(u8) = .empty;
    defer place_bytes.deinit(std.testing.allocator);
    try appendTestPlaceRecord(&place_bytes, std.testing.allocator, 0x09);
    for (1..place_bytes.items.len) |end| {
        try std.testing.expectError(
            error.TruncatedRecord,
            decode(std.testing.allocator, place_bytes.items[0..end], .{}),
        );
    }

    for (1..compressed_reset_golden.len) |end| {
        try expectDecodeFailure(compressed_reset_golden[0..end]);
    }
}

test "command code and enum corruption is rejected" {
    const allocator = std.testing.allocator;
    for ([_]u8{ 6, 90, 118, 120, 127 }) |nonmutating_code| {
        var bytes: std.ArrayListUnmanaged(u8) = .empty;
        defer bytes.deinit(allocator);
        try appendTestHeader(&bytes, allocator, nonmutating_code, 1, 2, 3, 4);
        try std.testing.expectError(error.NonMutatingCommand, decode(allocator, bytes.items, .{}));
    }

    var unknown: std.ArrayListUnmanaged(u8) = .empty;
    defer unknown.deinit(allocator);
    try appendTestHeader(&unknown, allocator, 5, 1, 2, 3, 4);
    try std.testing.expectError(error.UnknownCommandCode, decode(allocator, unknown.items, .{}));

    var invalid_order_type: std.ArrayListUnmanaged(u8) = .empty;
    defer invalid_order_type.deinit(allocator);
    try appendTestPlaceRecord(&invalid_order_type, allocator, 0x0a);
    try std.testing.expectError(
        error.InvalidOrderTypeCode,
        decode(allocator, invalid_order_type.items, .{}),
    );

    var invalid_adjustment: std.ArrayListUnmanaged(u8) = .empty;
    defer invalid_adjustment.deinit(allocator);
    try appendTestCommandHeader(&invalid_adjustment, allocator, .balance_adjustment, 1);
    try appendTestInt(&invalid_adjustment, allocator, i64, 1);
    try appendTestInt(&invalid_adjustment, allocator, i32, 2);
    try appendTestInt(&invalid_adjustment, allocator, i64, 3);
    try appendTestInt(&invalid_adjustment, allocator, i64, 4);
    try invalid_adjustment.append(allocator, 2);
    try std.testing.expectError(
        error.InvalidBalanceAdjustmentTypeCode,
        decode(allocator, invalid_adjustment.items, .{}),
    );
}

test "sequence gaps are reported while duplicate and regressing values fail" {
    const allocator = std.testing.allocator;
    var gaps_bytes: std.ArrayListUnmanaged(u8) = .empty;
    defer gaps_bytes.deinit(allocator);
    try appendTestCommandHeader(&gaps_bytes, allocator, .reset, 2);
    try appendTestCommandHeader(&gaps_bytes, allocator, .reset, 4);

    var with_gaps = try decode(allocator, gaps_bytes.items, .{});
    defer with_gaps.deinit();
    try std.testing.expectEqualSlices(
        SequenceGap,
        &.{
            .{ .previous_sequence = 0, .next_sequence = 2 },
            .{ .previous_sequence = 2, .next_sequence = 4 },
        },
        with_gaps.sequence_gaps.items,
    );

    var duplicate: std.ArrayListUnmanaged(u8) = .empty;
    defer duplicate.deinit(allocator);
    try appendTestCommandHeader(&duplicate, allocator, .reset, 1);
    try appendTestCommandHeader(&duplicate, allocator, .reset, 1);
    try std.testing.expectError(error.SequenceNotIncreasing, decode(allocator, duplicate.items, .{}));

    var regression: std.ArrayListUnmanaged(u8) = .empty;
    defer regression.deinit(allocator);
    try appendTestCommandHeader(&regression, allocator, .reset, 2);
    try appendTestCommandHeader(&regression, allocator, .reset, 1);
    try std.testing.expectError(error.SequenceNotIncreasing, decode(allocator, regression.items, .{}));
}

test "compressed block headers nesting and payload lengths are strict" {
    const allocator = std.testing.allocator;
    const negative_compressed = [_]u8{
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0x00,
        0x00,
        0x00,
        0x01,
    };
    try std.testing.expectError(
        error.NegativeCompressedLength,
        decode(allocator, &negative_compressed, .{}),
    );

    const negative_original = [_]u8{
        0xff,
        0x00,
        0x00,
        0x00,
        0x01,
        0xff,
        0xff,
        0xff,
        0xff,
    };
    try std.testing.expectError(
        error.NegativeOriginalLength,
        decode(allocator, &negative_original, .{}),
    );

    const empty_block = [_]u8{
        0xff,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x01,
    };
    try std.testing.expectError(error.EmptyCompressedBlock, decode(allocator, &empty_block, .{}));

    const nested_block = [_]u8{
        0xff,
        0x00,
        0x00,
        0x00,
        0x02,
        0x00,
        0x00,
        0x00,
        0x01,
        0x10,
        0xff,
    };
    try std.testing.expectError(error.NestedCompressedBlock, decode(allocator, &nested_block, .{}));

    var too_short = compressed_reset_golden;
    std.mem.writeInt(i32, too_short[5..9], 28, .big);
    try std.testing.expectError(
        error.DecompressedLengthMismatch,
        decode(allocator, &too_short, .{}),
    );

    var too_long = compressed_reset_golden;
    std.mem.writeInt(i32, too_long[5..9], 30, .big);
    try std.testing.expectError(
        error.DecompressedLengthMismatch,
        decode(allocator, &too_long, .{}),
    );

    var trailing: std.ArrayListUnmanaged(u8) = .empty;
    defer trailing.deinit(allocator);
    try trailing.appendSlice(allocator, &compressed_reset_golden);
    std.mem.writeInt(i32, trailing.items[1..5], 32, .big);
    try trailing.append(allocator, 0);
    try std.testing.expectError(
        error.CompressedLengthMismatch,
        decode(allocator, trailing.items, .{}),
    );
}

test "file block and entry limits are enforced before unsafe work" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(
        error.FileTooLarge,
        decode(allocator, &raw_reset_golden, .{ .max_file_bytes = raw_reset_golden.len - 1 }),
    );
    try std.testing.expectError(
        error.CompressedBlockTooLarge,
        decode(allocator, &compressed_reset_golden, .{ .max_compressed_block_bytes = 30 }),
    );
    try std.testing.expectError(
        error.OriginalBlockTooLarge,
        decode(allocator, &compressed_reset_golden, .{ .max_original_block_bytes = 28 }),
    );
    try std.testing.expectError(
        error.TooManyCompressedBlocks,
        decode(allocator, &compressed_reset_golden, .{ .max_compressed_blocks = 0 }),
    );
    try std.testing.expectError(
        error.TooManyEntries,
        decode(allocator, &raw_reset_golden, .{ .max_entries = 0 }),
    );

    const oversized_compressed = [_]u8{
        0xff,
        0x00, 0x0f, 0x42, 0x41, // 1,000,001
        0x00, 0x00, 0x00, 0x01,
    };
    try std.testing.expectError(
        error.CompressedBlockTooLarge,
        decode(allocator, &oversized_compressed, .{}),
    );
    const oversized_original = [_]u8{
        0xff,
        0x00,
        0x00,
        0x00,
        0x01,
        0x00, 0x0f, 0x42, 0x41, // 1,000,001
    };
    try std.testing.expectError(
        error.OriginalBlockTooLarge,
        decode(allocator, &oversized_original, .{}),
    );
}

fn decodeAllocationFailureCampaign(
    allocator: std.mem.Allocator,
    bytes: []const u8,
    expected_entries: usize,
    expected_gaps: usize,
) !void {
    var decoded = try decode(allocator, bytes, .{});
    defer decoded.deinit();
    try std.testing.expectEqual(expected_entries, decoded.entries.items.len);
    try std.testing.expectEqual(expected_gaps, decoded.sequence_gaps.items.len);
    try std.testing.expectEqual(@as(usize, 1), decoded.compressed_block_count);
}

test "decode is leak-free and atomic across every allocation failure" {
    const allocator = std.testing.allocator;
    var original: std.ArrayListUnmanaged(u8) = .empty;
    defer original.deinit(allocator);
    for (0..40) |index| {
        const sequence: i64 = @intCast(index * 2 + 1);
        try appendTestCommandHeader(&original, allocator, .reset, sequence);
    }

    const compressed = try java_binary_wire.encodeRawLz4LiteralOnly(allocator, original.items);
    defer allocator.free(compressed);
    var framed: std.ArrayListUnmanaged(u8) = .empty;
    defer framed.deinit(allocator);
    try framed.append(allocator, 0xff);
    try appendTestInt(&framed, allocator, i32, @intCast(compressed.len));
    try appendTestInt(&framed, allocator, i32, @intCast(original.items.len));
    try framed.appendSlice(allocator, compressed);

    try std.testing.checkAllAllocationFailures(
        allocator,
        decodeAllocationFailureCampaign,
        .{ framed.items, 40, 39 },
    );
}
