const std = @import("std");
const io_context = @import("io_context.zig");
const common = @import("common.zig");
const cmd = @import("cmd.zig");
const kernel_mod = @import("exchange_kernel.zig");
const snapshot_mod = @import("snapshot.zig");

pub const JournalEntry = struct {
    sequence: u64,
    input: cmd.OrderCommand,
    result_code: cmd.CommandResultCode,
};

pub const ReplayError = kernel_mod.ExecuteError || error{
    SequenceMismatch,
    ResultMismatch,
};

pub const ReplayWindow = struct {
    start_index: usize,
    replayed: usize,
};

pub const JournalIoError = std.mem.Allocator.Error ||
    std.Io.File.OpenError ||
    std.Io.Reader.Error ||
    std.Io.Writer.Error ||
    cmd.OrderCommandType.FromCodeError ||
    cmd.CommandResultCode.FromCodeError ||
    common.OrderAction.FromCodeError ||
    common.OrderType.FromCodeError ||
    error{
        InvalidMagic,
        UnsupportedLegacyLz4Format,
        InvalidOptionalTag,
    };

pub const CommandJournal = struct {
    allocator: std.mem.Allocator,
    entries: std.ArrayListUnmanaged(JournalEntry) = .empty,

    pub fn init(allocator: std.mem.Allocator) CommandJournal {
        return .{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *CommandJournal) void {
        self.entries.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn len(self: *const CommandJournal) usize {
        return self.entries.items.len;
    }

    pub fn executeAndRecord(
        self: *CommandJournal,
        kernel: *kernel_mod.ExchangeKernel,
        command: *cmd.OrderCommand,
    ) kernel_mod.ExecuteError!kernel_mod.CommandResponse {
        const input = command.*;
        var response = try kernel.execute(command);
        errdefer response.deinit(kernel.allocator);

        try self.entries.append(self.allocator, .{
            .sequence = response.sequence,
            .input = input,
            .result_code = response.result_code,
        });
        return response;
    }

    pub fn replayIntoKernel(
        self: *const CommandJournal,
        kernel: *kernel_mod.ExchangeKernel,
    ) ReplayError!void {
        for (self.entries.items) |entry| {
            var command = entry.input;
            var replayed = try kernel.execute(&command);
            defer replayed.deinit(kernel.allocator);

            if (replayed.sequence != entry.sequence) {
                return error.SequenceMismatch;
            }
            if (replayed.result_code != entry.result_code) {
                return error.ResultMismatch;
            }
        }
    }

    pub fn replayIntoKernelUntilTimestamp(
        self: *const CommandJournal,
        kernel: *kernel_mod.ExchangeKernel,
        timestamp_ns: u64,
    ) ReplayError!usize {
        const window = try self.replayIntoKernelFromSequenceUntilTimestamp(
            kernel,
            0,
            timestamp_ns,
        );
        return window.replayed;
    }

    pub fn replayIntoKernelFromSequenceUntilTimestamp(
        self: *const CommandJournal,
        kernel: *kernel_mod.ExchangeKernel,
        sequence_exclusive: u64,
        timestamp_ns: u64,
    ) ReplayError!ReplayWindow {
        if (timestamp_ns == 0) {
            return .{
                .start_index = self.entries.items.len,
                .replayed = 0,
            };
        }

        const replay_limit_i64 = if (timestamp_ns >= @as(u64, @intCast(std.math.maxInt(i64))))
            std.math.maxInt(i64)
        else
            @as(i64, @intCast(timestamp_ns));

        var first_index: ?usize = null;
        var replayed: usize = 0;
        for (self.entries.items, 0..) |entry, idx| {
            if (entry.sequence <= sequence_exclusive) continue;
            if (entry.input.timestamp > replay_limit_i64) break;
            if (first_index == null) first_index = idx;

            var command = entry.input;
            var replayed_response = try kernel.execute(&command);
            defer replayed_response.deinit(kernel.allocator);

            if (replayed_response.sequence != entry.sequence) {
                return error.SequenceMismatch;
            }
            if (replayed_response.result_code != entry.result_code) {
                return error.ResultMismatch;
            }

            replayed += 1;
        }
        return .{
            .start_index = first_index orelse self.entries.items.len,
            .replayed = replayed,
        };
    }

    pub fn writeToFile(
        self: *const CommandJournal,
        dir: std.Io.Dir,
        path: []const u8,
    ) JournalIoError!void {
        const file = try dir.createFile(io_context.current(), path, .{ .truncate = true });
        defer file.close(io_context.current());

        var buffer: [4096]u8 = undefined;
        var file_writer = file.writer(io_context.current(), &buffer);
        const writer = &file_writer.interface;

        try writer.writeAll("XCJ1");
        try writeInt(u64, @TypeOf(writer), writer, self.entries.items.len);
        for (self.entries.items) |entry| {
            try writeEntry(@TypeOf(writer), writer, entry);
        }
        try writer.flush();
    }

    pub fn readFromFile(
        allocator: std.mem.Allocator,
        dir: std.Io.Dir,
        path: []const u8,
    ) JournalIoError!CommandJournal {
        const file = try dir.openFile(io_context.current(), path, .{});
        defer file.close(io_context.current());

        var buffer: [4096]u8 = undefined;
        var file_reader = file.reader(io_context.current(), &buffer);
        const reader = &file_reader.interface;

        const magic = try reader.takeArray(4);
        if (!std.mem.eql(u8, magic[0..], "XCJ1")) {
            if (isLegacyLz4FrameMagic(magic[0..])) {
                return error.UnsupportedLegacyLz4Format;
            }
            return error.InvalidMagic;
        }

        const count = try readInt(u64, @TypeOf(reader), reader);
        var journal = CommandJournal.init(allocator);
        errdefer journal.deinit();
        try journal.entries.ensureTotalCapacityPrecise(allocator, count);

        var i: u64 = 0;
        while (i < count) : (i += 1) {
            journal.entries.appendAssumeCapacity(try readEntry(@TypeOf(reader), reader));
        }
        return journal;
    }
};

fn isLegacyLz4FrameMagic(magic: []const u8) bool {
    std.debug.assert(magic.len == 4);

    // LZ4 frame magic used by Java persistence snapshots/journals.
    if (magic[0] == 0x04 and magic[1] == 0x22 and magic[2] == 0x4D and magic[3] == 0x18) {
        return true;
    }

    // LZ4 skippable frame range (0x184D2A50..0x184D2A5F).
    if (magic[1] == 0x2A and magic[2] == 0x4D and magic[3] == 0x18 and magic[0] >= 0x50 and magic[0] <= 0x5F) {
        return true;
    }
    return false;
}

fn writeEntry(comptime Writer: type, writer: Writer, entry: JournalEntry) !void {
    try writeInt(u64, Writer, writer, entry.sequence);
    try writeInt(i32, Writer, writer, entry.result_code.code());
    try writeOrderCommand(Writer, writer, entry.input);
}

fn readEntry(comptime Reader: type, reader: Reader) JournalIoError!JournalEntry {
    const sequence = try readInt(u64, Reader, reader);
    const result_raw = try readInt(i32, Reader, reader);
    const result_code = try cmd.CommandResultCode.fromCode(result_raw);
    const input = try readOrderCommand(Reader, reader);
    return .{
        .sequence = sequence,
        .input = input,
        .result_code = result_code,
    };
}

fn writeOrderCommand(comptime Writer: type, writer: Writer, value: cmd.OrderCommand) !void {
    try writeInt(i8, Writer, writer, value.command.code());
    try writeInt(i64, Writer, writer, value.order_id);
    try writeInt(i32, Writer, writer, value.symbol);
    try writeInt(i64, Writer, writer, value.price);
    try writeInt(i64, Writer, writer, value.size);
    try writeInt(i64, Writer, writer, value.reserve_bid_price);
    try writeOptionalOrderAction(Writer, writer, value.action);
    try writeOptionalOrderType(Writer, writer, value.order_type);
    try writeInt(i64, Writer, writer, value.uid);
    try writeInt(i64, Writer, writer, value.timestamp);
    try writeInt(i32, Writer, writer, value.user_cookie);
    try writeInt(i64, Writer, writer, value.events_group);
    try writeInt(i32, Writer, writer, value.service_flags);
    try writeInt(i32, Writer, writer, value.result_code.code());
}

fn readOrderCommand(comptime Reader: type, reader: Reader) JournalIoError!cmd.OrderCommand {
    const command_type = try cmd.OrderCommandType.fromCode(try readInt(i8, Reader, reader));
    const order_id = try readInt(i64, Reader, reader);
    const symbol = try readInt(i32, Reader, reader);
    const price = try readInt(i64, Reader, reader);
    const size = try readInt(i64, Reader, reader);
    const reserve_bid_price = try readInt(i64, Reader, reader);
    const action = try readOptionalOrderAction(Reader, reader);
    const order_type = try readOptionalOrderType(Reader, reader);
    const uid = try readInt(i64, Reader, reader);
    const timestamp = try readInt(i64, Reader, reader);
    const user_cookie = try readInt(i32, Reader, reader);
    const events_group = try readInt(i64, Reader, reader);
    const service_flags = try readInt(i32, Reader, reader);
    const result_code = try cmd.CommandResultCode.fromCode(try readInt(i32, Reader, reader));

    return .{
        .command = command_type,
        .order_id = order_id,
        .symbol = symbol,
        .price = price,
        .size = size,
        .reserve_bid_price = reserve_bid_price,
        .action = action,
        .order_type = order_type,
        .uid = uid,
        .timestamp = timestamp,
        .user_cookie = user_cookie,
        .events_group = events_group,
        .service_flags = service_flags,
        .result_code = result_code,
    };
}

fn writeOptionalOrderAction(comptime Writer: type, writer: Writer, value: ?common.OrderAction) !void {
    if (value) |present| {
        try writer.writeByte(1);
        try writeInt(i8, Writer, writer, present.code());
    } else {
        try writer.writeByte(0);
    }
}

fn readOptionalOrderAction(comptime Reader: type, reader: Reader) JournalIoError!?common.OrderAction {
    const tag = try reader.takeByte();
    return switch (tag) {
        0 => null,
        1 => try common.OrderAction.fromCode(try readInt(i8, Reader, reader)),
        else => error.InvalidOptionalTag,
    };
}

fn writeOptionalOrderType(comptime Writer: type, writer: Writer, value: ?common.OrderType) !void {
    if (value) |present| {
        try writer.writeByte(1);
        try writeInt(i8, Writer, writer, present.code());
    } else {
        try writer.writeByte(0);
    }
}

fn readOptionalOrderType(comptime Reader: type, reader: Reader) JournalIoError!?common.OrderType {
    const tag = try reader.takeByte();
    return switch (tag) {
        0 => null,
        1 => try common.OrderType.fromCode(try readInt(i8, Reader, reader)),
        else => error.InvalidOptionalTag,
    };
}

fn writeInt(
    comptime T: type,
    comptime Writer: type,
    writer: Writer,
    value: T,
) !void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    try writer.writeAll(&bytes);
}

fn readInt(
    comptime T: type,
    comptime Reader: type,
    reader: Reader,
) JournalIoError!T {
    return try reader.takeInt(T, .little);
}

fn makeExchangeSpecForSymbol(symbol_id: i32) !common.CoreSymbolSpecification {
    return common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol_id,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 100,
        .quote_scale_k = 10,
        .taker_fee = 2,
        .maker_fee = 1,
    });
}

fn makeExchangeSpec() !common.CoreSymbolSpecification {
    return makeExchangeSpecForSymbol(241);
}

fn makeAddUserCommand(uid: i64) cmd.OrderCommand {
    return .{
        .command = .add_user,
        .uid = uid,
        .result_code = .new,
    };
}

fn makeBalanceAdjustmentCommand(
    uid: i64,
    currency: i32,
    amount: i64,
    transaction_id: i64,
    adjustment_type: common.BalanceAdjustmentType,
) cmd.OrderCommand {
    return .{
        .command = .balance_adjustment,
        .uid = uid,
        .symbol = currency,
        .price = amount,
        .order_id = transaction_id,
        .order_type = switch (adjustment_type) {
            .adjustment => .gtc,
            .@"suspend" => .ioc,
        },
        .result_code = .new,
    };
}

fn makeRequestOrderBookCommand(symbol: i32, depth: usize) cmd.OrderCommand {
    return .{
        .command = .order_book_request,
        .symbol = symbol,
        .size = @as(i64, @intCast(depth)),
        .result_code = .new,
    };
}

fn simulationPrice(random: std.Random, base: i64) i64 {
    return base + @as(i64, @intCast(random.uintLessThan(u32, 25)));
}

test "journal replay reproduces sequence, results, risk hash, and L2 view" {
    const allocator = std.testing.allocator;

    var kernel_a = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer kernel_a.deinit();
    var kernel_b = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer kernel_b.deinit();

    const spec = try makeExchangeSpec();
    try std.testing.expect(try kernel_a.addSymbol(spec));
    try std.testing.expect(try kernel_b.addSymbol(spec));

    var journal = CommandJournal.init(allocator);
    defer journal.deinit();

    var add_maker = makeAddUserCommand(9101);
    var add_taker = makeAddUserCommand(9102);
    var dep_quote = makeBalanceAdjustmentCommand(9101, 15, 100_000, 1, .adjustment);
    var dep_base = makeBalanceAdjustmentCommand(9102, 11, 100_000, 1, .adjustment);
    var maker_bid = cmd.OrderCommand.newOrder(.gtc, 9901, 9101, 100, 100, 5, .bid);
    maker_bid.symbol = 241;
    var taker_ask = cmd.OrderCommand.newOrder(.gtc, 9902, 9102, 99, 0, 3, .ask);
    taker_ask.symbol = 241;
    var reduce = cmd.OrderCommand.reduce(9901, 9101, 1);
    reduce.symbol = 241;

    var r1 = try journal.executeAndRecord(&kernel_a, &add_maker);
    defer r1.deinit(allocator);
    var r2 = try journal.executeAndRecord(&kernel_a, &add_taker);
    defer r2.deinit(allocator);
    var r3 = try journal.executeAndRecord(&kernel_a, &dep_quote);
    defer r3.deinit(allocator);
    var r4 = try journal.executeAndRecord(&kernel_a, &dep_base);
    defer r4.deinit(allocator);
    var r5 = try journal.executeAndRecord(&kernel_a, &maker_bid);
    defer r5.deinit(allocator);
    var r6 = try journal.executeAndRecord(&kernel_a, &taker_ask);
    defer r6.deinit(allocator);
    var r7 = try journal.executeAndRecord(&kernel_a, &reduce);
    defer r7.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 7), journal.len());
    try journal.replayIntoKernel(&kernel_b);

    const hash_a = try kernel_a.risk_engine.stateHash();
    const hash_b = try kernel_b.risk_engine.stateHash();
    try std.testing.expectEqual(hash_a, hash_b);

    var req_a = makeRequestOrderBookCommand(241, 5);
    var req_b = makeRequestOrderBookCommand(241, 5);
    var ob_a = try kernel_a.execute(&req_a);
    defer ob_a.deinit(allocator);
    var ob_b = try kernel_b.execute(&req_b);
    defer ob_b.deinit(allocator);

    const snap_a = ob_a.l2_snapshot orelse unreachable;
    const snap_b = ob_b.l2_snapshot orelse unreachable;
    try std.testing.expectEqual(snap_a.asks.items.len, snap_b.asks.items.len);
    try std.testing.expectEqual(snap_a.bids.items.len, snap_b.bids.items.len);
    try std.testing.expectEqual(snap_a.bids.items[0].price, snap_b.bids.items[0].price);
    try std.testing.expectEqual(snap_a.bids.items[0].volume, snap_b.bids.items[0].volume);
}

test "journal file roundtrip preserves entries and replay behavior" {
    const allocator = std.testing.allocator;

    var kernel_a = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .naive,
    });
    defer kernel_a.deinit();
    var kernel_b = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .naive,
    });
    defer kernel_b.deinit();

    const spec = try makeExchangeSpec();
    try std.testing.expect(try kernel_a.addSymbol(spec));
    try std.testing.expect(try kernel_b.addSymbol(spec));

    var journal = CommandJournal.init(allocator);
    defer journal.deinit();

    var add_user = makeAddUserCommand(9301);
    var deposit = makeBalanceAdjustmentCommand(9301, 15, 2_000, 1, .adjustment);
    var bid = cmd.OrderCommand.newOrder(.gtc, 9951, 9301, 50, 50, 2, .bid);
    bid.symbol = 241;

    var r1 = try journal.executeAndRecord(&kernel_a, &add_user);
    defer r1.deinit(allocator);
    var r2 = try journal.executeAndRecord(&kernel_a, &deposit);
    defer r2.deinit(allocator);
    var r3 = try journal.executeAndRecord(&kernel_a, &bid);
    defer r3.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try journal.writeToFile(tmp.dir, "journal.bin");

    var loaded = try CommandJournal.readFromFile(allocator, tmp.dir, "journal.bin");
    defer loaded.deinit();

    try std.testing.expectEqual(journal.len(), loaded.len());
    try std.testing.expectEqualDeep(journal.entries.items, loaded.entries.items);

    try loaded.replayIntoKernel(&kernel_b);
    const hash_a = try kernel_a.risk_engine.stateHash();
    const hash_b = try kernel_b.risk_engine.stateHash();
    try std.testing.expectEqual(hash_a, hash_b);
}

test "journal readFromFile reports unsupported legacy lz4 frame explicitly" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const file = try tmp.dir.createFile(io_context.current(), "legacy_lz4.journal", .{ .truncate = true });
    defer file.close(io_context.current());
    try file.writeStreamingAll(io_context.current(), &.{ 0x04, 0x22, 0x4D, 0x18 });

    try std.testing.expectError(
        error.UnsupportedLegacyLz4Format,
        CommandJournal.readFromFile(allocator, tmp.dir, "legacy_lz4.journal"),
    );
}

test "journal replay cutoff replays commands up to timestamp limit" {
    const allocator = std.testing.allocator;

    var source = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer source.deinit();
    var replay = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer replay.deinit();

    const spec = try makeExchangeSpec();
    try std.testing.expect(try source.addSymbol(spec));
    try std.testing.expect(try replay.addSymbol(spec));

    var journal = CommandJournal.init(allocator);
    defer journal.deinit();

    var add_u1 = makeAddUserCommand(9701);
    add_u1.timestamp = 10;
    var add_u2 = makeAddUserCommand(9702);
    add_u2.timestamp = 20;
    var add_u3 = makeAddUserCommand(9703);
    add_u3.timestamp = 30;

    var r1 = try journal.executeAndRecord(&source, &add_u1);
    defer r1.deinit(allocator);
    var r2 = try journal.executeAndRecord(&source, &add_u2);
    defer r2.deinit(allocator);
    var r3 = try journal.executeAndRecord(&source, &add_u3);
    defer r3.deinit(allocator);

    const replayed = try journal.replayIntoKernelUntilTimestamp(&replay, 20);
    try std.testing.expectEqual(@as(usize, 2), replayed);

    var user1_report = try replay.queryReport(.{ .single_user = 9701 });
    defer user1_report.deinit(allocator);
    var user2_report = try replay.queryReport(.{ .single_user = 9702 });
    defer user2_report.deinit(allocator);
    var user3_report = try replay.queryReport(.{ .single_user = 9703 });
    defer user3_report.deinit(allocator);

    try std.testing.expectEqual(kernel_mod.SingleUserQueryExecutionStatus.ok, user1_report.single_user.query_execution_status);
    try std.testing.expectEqual(kernel_mod.SingleUserQueryExecutionStatus.ok, user2_report.single_user.query_execution_status);
    try std.testing.expectEqual(kernel_mod.SingleUserQueryExecutionStatus.user_not_found, user3_report.single_user.query_execution_status);

    const none = try journal.replayIntoKernelUntilTimestamp(&replay, 0);
    try std.testing.expectEqual(@as(usize, 0), none);
}

test "journal replay window can skip pre-snapshot sequence prefix" {
    const allocator = std.testing.allocator;

    var source = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer source.deinit();
    var replay = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer replay.deinit();

    var journal = CommandJournal.init(allocator);
    defer journal.deinit();

    var add_u1 = makeAddUserCommand(9801);
    add_u1.timestamp = 10;
    var add_u2 = makeAddUserCommand(9802);
    add_u2.timestamp = 20;
    var add_u3 = makeAddUserCommand(9803);
    add_u3.timestamp = 30;

    var r1 = try journal.executeAndRecord(&source, &add_u1);
    defer r1.deinit(allocator);
    var r2 = try journal.executeAndRecord(&source, &add_u2);
    defer r2.deinit(allocator);
    var r3 = try journal.executeAndRecord(&source, &add_u3);
    defer r3.deinit(allocator);

    // Simulate restored snapshot baseline at sequence=1.
    replay.next_seq = 1;

    const window = try journal.replayIntoKernelFromSequenceUntilTimestamp(
        &replay,
        1,
        25,
    );
    try std.testing.expectEqual(@as(usize, 1), window.start_index);
    try std.testing.expectEqual(@as(usize, 1), window.replayed);

    var user2_report = try replay.queryReport(.{ .single_user = 9802 });
    defer user2_report.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.ok,
        user2_report.single_user.query_execution_status,
    );

    var user3_report = try replay.queryReport(.{ .single_user = 9803 });
    defer user3_report.deinit(allocator);
    try std.testing.expectEqual(
        kernel_mod.SingleUserQueryExecutionStatus.user_not_found,
        user3_report.single_user.query_execution_status,
    );
}

test "journal, snapshot, and report queries stay deterministic under large seeded stream" {
    const allocator = std.testing.allocator;

    var primary = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer primary.deinit();
    var replay = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer replay.deinit();
    var replay_from_file = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer replay_from_file.deinit();

    const spec = try makeExchangeSpec();
    try std.testing.expect(try primary.addSymbol(spec));
    try std.testing.expect(try replay.addSymbol(spec));
    try std.testing.expect(try replay_from_file.addSymbol(spec));

    var journal = CommandJournal.init(allocator);
    defer journal.deinit();

    const user_ids = [_]i64{ 10001, 10002, 10003, 10004, 10005, 10006, 10007, 10008 };
    var tx_counter_by_uid: std.AutoHashMapUnmanaged(i64, i64) = .empty;
    defer tx_counter_by_uid.deinit(allocator);

    for (user_ids) |uid| {
        var add = makeAddUserCommand(uid);
        var add_resp = try journal.executeAndRecord(&primary, &add);
        add_resp.deinit(allocator);

        var dep_base = makeBalanceAdjustmentCommand(uid, 11, 2_000_000, 1, .adjustment);
        var dep_base_resp = try journal.executeAndRecord(&primary, &dep_base);
        dep_base_resp.deinit(allocator);

        var dep_quote = makeBalanceAdjustmentCommand(uid, 15, 2_000_000, 2, .adjustment);
        var dep_quote_resp = try journal.executeAndRecord(&primary, &dep_quote);
        dep_quote_resp.deinit(allocator);

        try tx_counter_by_uid.put(allocator, uid, 2);
    }

    const Candidate = struct {
        order_id: i64,
        uid: i64,
    };
    var candidates: std.ArrayListUnmanaged(Candidate) = .empty;
    defer candidates.deinit(allocator);

    var prng = std.Random.DefaultPrng.init(0xA11CE55D77EE9911);
    const random = prng.random();
    var next_order_id: i64 = 60_000;
    const rounds: usize = 1600;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var previous_checkpoint_seq: ?u64 = null;

    for (0..rounds) |idx| {
        const roll = random.uintLessThan(u32, 100);
        var command: cmd.OrderCommand = undefined;

        if (roll < 50) {
            const uid = user_ids[random.uintLessThan(usize, user_ids.len)];
            const action: common.OrderAction = if (random.boolean()) .ask else .bid;
            const order_type: common.OrderType = blk: {
                const t = random.uintLessThan(u32, 100);
                if (t < 75) break :blk .gtc;
                if (t < 92) break :blk .ioc;
                break :blk .fok_budget;
            };

            const price = simulationPrice(random, 100);
            const size = @as(i64, @intCast(random.uintLessThan(u32, 11))) + 1;
            const reserve = if (action == .bid)
                price + @as(i64, @intCast(random.uintLessThan(u32, 30)))
            else
                0;

            command = cmd.OrderCommand.newOrder(order_type, next_order_id, uid, price, reserve, size, action);
            command.symbol = 241;

            if (order_type == .gtc) {
                try candidates.append(allocator, .{
                    .order_id = next_order_id,
                    .uid = uid,
                });
            }
            next_order_id += 1;
        } else if (roll < 68) {
            const uid = user_ids[random.uintLessThan(usize, user_ids.len)];
            const currency: i32 = if (random.boolean()) 11 else 15;
            const amount_abs = @as(i64, @intCast(random.uintLessThan(u32, 1000))) + 1;
            const amount: i64 = if (random.uintLessThan(u32, 100) < 20) -amount_abs else amount_abs;
            const tx_counter = tx_counter_by_uid.getPtr(uid) orelse unreachable;
            tx_counter.* += 1;
            command = makeBalanceAdjustmentCommand(uid, currency, amount, tx_counter.*, .adjustment);
        } else if (roll < 82) {
            const picked = if (candidates.items.len > 0 and random.boolean())
                candidates.items[random.uintLessThan(usize, candidates.items.len)]
            else
                Candidate{
                    .order_id = @as(i64, @intCast(random.uintLessThan(u32, 15_000))) + 1,
                    .uid = user_ids[random.uintLessThan(usize, user_ids.len)],
                };
            const uid = if (random.uintLessThan(u32, 100) < 85)
                picked.uid
            else
                user_ids[random.uintLessThan(usize, user_ids.len)];
            command = cmd.OrderCommand.cancel(picked.order_id, uid);
            command.symbol = 241;
        } else if (roll < 92) {
            const picked = if (candidates.items.len > 0 and random.boolean())
                candidates.items[random.uintLessThan(usize, candidates.items.len)]
            else
                Candidate{
                    .order_id = @as(i64, @intCast(random.uintLessThan(u32, 15_000))) + 1,
                    .uid = user_ids[random.uintLessThan(usize, user_ids.len)],
                };
            const uid = if (random.uintLessThan(u32, 100) < 85)
                picked.uid
            else
                user_ids[random.uintLessThan(usize, user_ids.len)];
            command = cmd.OrderCommand.update(picked.order_id, uid, simulationPrice(random, 100));
            command.symbol = 241;
        } else if (roll < 98) {
            const picked = if (candidates.items.len > 0 and random.boolean())
                candidates.items[random.uintLessThan(usize, candidates.items.len)]
            else
                Candidate{
                    .order_id = @as(i64, @intCast(random.uintLessThan(u32, 15_000))) + 1,
                    .uid = user_ids[random.uintLessThan(usize, user_ids.len)],
                };
            const uid = if (random.uintLessThan(u32, 100) < 85)
                picked.uid
            else
                user_ids[random.uintLessThan(usize, user_ids.len)];
            const reduce_size = if (random.uintLessThan(u32, 100) < 5)
                0
            else
                @as(i64, @intCast(random.uintLessThan(u32, 8))) + 1;
            command = cmd.OrderCommand.reduce(picked.order_id, uid, reduce_size);
            command.symbol = 241;
        } else {
            command = makeRequestOrderBookCommand(241, random.uintLessThan(usize, 32) + 1);
        }

        var response = try journal.executeAndRecord(&primary, &command);
        response.deinit(allocator);

        if ((idx + 1) % 160 == 0) {
            const checkpoint_seq = primary.next_seq;
            var risk_report = try primary.queryReport(.risk_state_hash);
            defer risk_report.deinit(allocator);
            var detailed_report = try primary.queryReport(.state_hash_detailed);
            defer detailed_report.deinit(allocator);
            var fee_report = try primary.queryReport(.{ .fee_collected = 15 });
            defer fee_report.deinit(allocator);
            var total_balance_report = try primary.queryReport(.total_currency_balance);
            defer total_balance_report.deinit(allocator);

            switch (detailed_report) {
                .state_hash_detailed => |detailed| {
                    try std.testing.expect(detailed.submodules.items.len > 0);
                },
                else => unreachable,
            }
            switch (total_balance_report) {
                .total_currency_balance => {},
                else => unreachable,
            }
            try std.testing.expectEqual(primary.risk_engine.getFeeCollected(15), fee_report.fee_collected);

            const sample_uid = user_ids[random.uintLessThan(usize, user_ids.len)];
            var single_user_report = try primary.queryReport(.{ .single_user = sample_uid });
            defer single_user_report.deinit(allocator);
            var quote_balance_report = try primary.queryReport(.{
                .user_balance = .{
                    .uid = sample_uid,
                    .currency = 15,
                },
            });
            defer quote_balance_report.deinit(allocator);
            var base_balance_report = try primary.queryReport(.{
                .user_balance = .{
                    .uid = sample_uid,
                    .currency = 11,
                },
            });
            defer base_balance_report.deinit(allocator);

            switch (single_user_report) {
                .single_user => |single_user| {
                    switch (single_user.query_execution_status) {
                        .ok => {
                            const quote_balance = single_user.accounts.get(15) orelse 0;
                            const base_balance = single_user.accounts.get(11) orelse 0;
                            try std.testing.expectEqual(@as(i64, quote_balance), quote_balance_report.user_balance orelse 0);
                            try std.testing.expectEqual(@as(i64, base_balance), base_balance_report.user_balance orelse 0);
                        },
                        .user_not_found => {
                            try std.testing.expectEqual(@as(?i64, null), quote_balance_report.user_balance);
                            try std.testing.expectEqual(@as(?i64, null), base_balance_report.user_balance);
                        },
                    }
                },
                else => unreachable,
            }

            var snapshot = try snapshot_mod.KernelSnapshot.captureFromKernel(allocator, &primary, &.{241}, 32);
            defer snapshot.deinit();
            try snapshot.validateAgainstKernel(&primary);

            var snapshot_name_buf: [64]u8 = undefined;
            const snapshot_name = try std.fmt.bufPrint(&snapshot_name_buf, "sim-seq-{d}.snap", .{checkpoint_seq});
            try snapshot.writeToFile(tmp.dir, snapshot_name);

            var loaded_snapshot = try snapshot_mod.KernelSnapshot.readFromFile(allocator, tmp.dir, snapshot_name);
            defer loaded_snapshot.deinit();
            try loaded_snapshot.validateAgainstKernel(&primary);
            try std.testing.expectEqual(snapshot.risk_state_hash, loaded_snapshot.risk_state_hash);

            var restored_checkpoint = try kernel_mod.ExchangeKernel.init(allocator, .{
                .orderbook_kind = .direct,
            });
            defer restored_checkpoint.deinit();
            try loaded_snapshot.restoreIntoKernel(&restored_checkpoint);
            restored_checkpoint.next_seq = checkpoint_seq;

            var restored_risk = try restored_checkpoint.queryReport(.risk_state_hash);
            defer restored_risk.deinit(allocator);
            try std.testing.expectEqual(risk_report.risk_state_hash, restored_risk.risk_state_hash);

            if (previous_checkpoint_seq) |previous_seq| {
                var previous_snapshot_name_buf: [64]u8 = undefined;
                const previous_snapshot_name = try std.fmt.bufPrint(
                    &previous_snapshot_name_buf,
                    "sim-seq-{d}.snap",
                    .{previous_seq},
                );

                var previous_snapshot = try snapshot_mod.KernelSnapshot.readFromFile(
                    allocator,
                    tmp.dir,
                    previous_snapshot_name,
                );
                defer previous_snapshot.deinit();

                var replay_from_previous_snapshot = try kernel_mod.ExchangeKernel.init(allocator, .{
                    .orderbook_kind = .direct,
                });
                defer replay_from_previous_snapshot.deinit();
                try previous_snapshot.restoreIntoKernel(&replay_from_previous_snapshot);
                replay_from_previous_snapshot.next_seq = previous_seq;

                const replay_window = try journal.replayIntoKernelFromSequenceUntilTimestamp(
                    &replay_from_previous_snapshot,
                    previous_seq,
                    std.math.maxInt(u64),
                );

                const expected_replayed_u64 = checkpoint_seq - previous_seq;
                const expected_replayed = std.math.cast(usize, expected_replayed_u64) orelse unreachable;
                try std.testing.expectEqual(expected_replayed, replay_window.replayed);

                var replayed_risk = try replay_from_previous_snapshot.queryReport(.risk_state_hash);
                defer replayed_risk.deinit(allocator);
                try std.testing.expectEqual(risk_report.risk_state_hash, replayed_risk.risk_state_hash);
            }

            previous_checkpoint_seq = checkpoint_seq;
            _ = risk_report.risk_state_hash;
        }
    }

    try std.testing.expect(journal.len() > user_ids.len);
    try journal.replayIntoKernel(&replay);

    try journal.writeToFile(tmp.dir, "simulation.journal");
    var loaded_journal = try CommandJournal.readFromFile(allocator, tmp.dir, "simulation.journal");
    defer loaded_journal.deinit();
    try std.testing.expectEqual(journal.len(), loaded_journal.len());
    try loaded_journal.replayIntoKernel(&replay_from_file);

    var risk_primary = try primary.queryReport(.risk_state_hash);
    defer risk_primary.deinit(allocator);
    var risk_replay = try replay.queryReport(.risk_state_hash);
    defer risk_replay.deinit(allocator);
    var risk_file = try replay_from_file.queryReport(.risk_state_hash);
    defer risk_file.deinit(allocator);
    try std.testing.expectEqual(risk_primary.risk_state_hash, risk_replay.risk_state_hash);
    try std.testing.expectEqual(risk_primary.risk_state_hash, risk_file.risk_state_hash);

    var detailed_primary = try primary.queryReport(.state_hash_detailed);
    defer detailed_primary.deinit(allocator);
    var detailed_replay = try replay.queryReport(.state_hash_detailed);
    defer detailed_replay.deinit(allocator);
    var detailed_file = try replay_from_file.queryReport(.state_hash_detailed);
    defer detailed_file.deinit(allocator);
    try std.testing.expectEqual(
        detailed_primary.state_hash_detailed.aggregate_hash,
        detailed_replay.state_hash_detailed.aggregate_hash,
    );
    try std.testing.expectEqual(
        detailed_primary.state_hash_detailed.aggregate_hash,
        detailed_file.state_hash_detailed.aggregate_hash,
    );

    var final_snapshot = try snapshot_mod.KernelSnapshot.captureFromKernel(allocator, &primary, &.{241}, 64);
    defer final_snapshot.deinit();
    try final_snapshot.validateAgainstKernel(&replay);
    try final_snapshot.validateAgainstKernel(&replay_from_file);
}

test "journal, snapshot, and report queries stay deterministic under multi-symbol seeded stream" {
    const allocator = std.testing.allocator;

    var primary = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer primary.deinit();
    var replay = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer replay.deinit();
    var replay_from_file = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer replay_from_file.deinit();

    const symbols = [_]i32{ 241, 242 };
    for (symbols) |symbol| {
        const spec = try makeExchangeSpecForSymbol(symbol);
        try std.testing.expect(try primary.addSymbol(spec));
        try std.testing.expect(try replay.addSymbol(spec));
        try std.testing.expect(try replay_from_file.addSymbol(spec));
    }

    var journal = CommandJournal.init(allocator);
    defer journal.deinit();

    const user_ids = [_]i64{ 11001, 11002, 11003, 11004, 11005, 11006, 11007, 11008, 11009, 11010 };
    var tx_counter_by_uid: std.AutoHashMapUnmanaged(i64, i64) = .empty;
    defer tx_counter_by_uid.deinit(allocator);

    for (user_ids) |uid| {
        var add = makeAddUserCommand(uid);
        var add_resp = try journal.executeAndRecord(&primary, &add);
        add_resp.deinit(allocator);

        var dep_base = makeBalanceAdjustmentCommand(uid, 11, 2_500_000, 1, .adjustment);
        var dep_base_resp = try journal.executeAndRecord(&primary, &dep_base);
        dep_base_resp.deinit(allocator);

        var dep_quote = makeBalanceAdjustmentCommand(uid, 15, 2_500_000, 2, .adjustment);
        var dep_quote_resp = try journal.executeAndRecord(&primary, &dep_quote);
        dep_quote_resp.deinit(allocator);

        try tx_counter_by_uid.put(allocator, uid, 2);
    }

    const Candidate = struct {
        order_id: i64,
        uid: i64,
        symbol: i32,
    };
    var candidates: std.ArrayListUnmanaged(Candidate) = .empty;
    defer candidates.deinit(allocator);

    var prng = std.Random.DefaultPrng.init(0xBEEFCAFEA11C9911);
    const random = prng.random();
    var next_order_id: i64 = 80_000;
    const rounds: usize = 2200;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var previous_checkpoint_seq: ?u64 = null;

    for (0..rounds) |idx| {
        const roll = random.uintLessThan(u32, 100);
        var command: cmd.OrderCommand = undefined;

        if (roll < 52) {
            const uid = user_ids[random.uintLessThan(usize, user_ids.len)];
            const symbol = symbols[random.uintLessThan(usize, symbols.len)];
            const action: common.OrderAction = if (random.boolean()) .ask else .bid;
            const order_type: common.OrderType = blk: {
                const t = random.uintLessThan(u32, 100);
                if (t < 72) break :blk .gtc;
                if (t < 90) break :blk .ioc;
                break :blk .fok_budget;
            };

            const base_price: i64 = if (symbol == 241) 100 else 160;
            const price = simulationPrice(random, base_price);
            const size = @as(i64, @intCast(random.uintLessThan(u32, 13))) + 1;
            const reserve = if (action == .bid)
                price + @as(i64, @intCast(random.uintLessThan(u32, 24)))
            else
                0;

            command = cmd.OrderCommand.newOrder(order_type, next_order_id, uid, price, reserve, size, action);
            command.symbol = symbol;

            if (order_type == .gtc) {
                try candidates.append(allocator, .{
                    .order_id = next_order_id,
                    .uid = uid,
                    .symbol = symbol,
                });
            }
            next_order_id += 1;
        } else if (roll < 68) {
            const uid = user_ids[random.uintLessThan(usize, user_ids.len)];
            const currency: i32 = if (random.boolean()) 11 else 15;
            const amount_abs = @as(i64, @intCast(random.uintLessThan(u32, 1200))) + 1;
            const amount: i64 = if (random.uintLessThan(u32, 100) < 18) -amount_abs else amount_abs;
            const tx_counter = tx_counter_by_uid.getPtr(uid) orelse unreachable;
            tx_counter.* += 1;
            command = makeBalanceAdjustmentCommand(uid, currency, amount, tx_counter.*, .adjustment);
        } else if (roll < 82) {
            const picked = if (candidates.items.len > 0 and random.boolean())
                candidates.items[random.uintLessThan(usize, candidates.items.len)]
            else
                Candidate{
                    .order_id = @as(i64, @intCast(random.uintLessThan(u32, 20_000))) + 1,
                    .uid = user_ids[random.uintLessThan(usize, user_ids.len)],
                    .symbol = symbols[random.uintLessThan(usize, symbols.len)],
                };
            const uid = if (random.uintLessThan(u32, 100) < 85)
                picked.uid
            else
                user_ids[random.uintLessThan(usize, user_ids.len)];
            command = cmd.OrderCommand.cancel(picked.order_id, uid);
            command.symbol = picked.symbol;
        } else if (roll < 92) {
            const picked = if (candidates.items.len > 0 and random.boolean())
                candidates.items[random.uintLessThan(usize, candidates.items.len)]
            else
                Candidate{
                    .order_id = @as(i64, @intCast(random.uintLessThan(u32, 20_000))) + 1,
                    .uid = user_ids[random.uintLessThan(usize, user_ids.len)],
                    .symbol = symbols[random.uintLessThan(usize, symbols.len)],
                };
            const uid = if (random.uintLessThan(u32, 100) < 85)
                picked.uid
            else
                user_ids[random.uintLessThan(usize, user_ids.len)];
            const base_price: i64 = if (picked.symbol == 241) 100 else 160;
            command = cmd.OrderCommand.update(picked.order_id, uid, simulationPrice(random, base_price));
            command.symbol = picked.symbol;
        } else if (roll < 98) {
            const picked = if (candidates.items.len > 0 and random.boolean())
                candidates.items[random.uintLessThan(usize, candidates.items.len)]
            else
                Candidate{
                    .order_id = @as(i64, @intCast(random.uintLessThan(u32, 20_000))) + 1,
                    .uid = user_ids[random.uintLessThan(usize, user_ids.len)],
                    .symbol = symbols[random.uintLessThan(usize, symbols.len)],
                };
            const uid = if (random.uintLessThan(u32, 100) < 85)
                picked.uid
            else
                user_ids[random.uintLessThan(usize, user_ids.len)];
            const reduce_size = if (random.uintLessThan(u32, 100) < 5)
                0
            else
                @as(i64, @intCast(random.uintLessThan(u32, 9))) + 1;
            command = cmd.OrderCommand.reduce(picked.order_id, uid, reduce_size);
            command.symbol = picked.symbol;
        } else {
            const symbol = symbols[random.uintLessThan(usize, symbols.len)];
            command = makeRequestOrderBookCommand(symbol, random.uintLessThan(usize, 32) + 1);
        }

        var response = try journal.executeAndRecord(&primary, &command);
        response.deinit(allocator);

        if ((idx + 1) % 220 == 0) {
            const checkpoint_seq = primary.next_seq;
            var risk_report = try primary.queryReport(.risk_state_hash);
            defer risk_report.deinit(allocator);
            var detailed_report = try primary.queryReport(.state_hash_detailed);
            defer detailed_report.deinit(allocator);
            var fee_report = try primary.queryReport(.{ .fee_collected = 15 });
            defer fee_report.deinit(allocator);
            var total_balance_report = try primary.queryReport(.total_currency_balance);
            defer total_balance_report.deinit(allocator);

            switch (detailed_report) {
                .state_hash_detailed => |detailed| {
                    try std.testing.expect(detailed.submodules.items.len > 0);
                },
                else => unreachable,
            }
            switch (total_balance_report) {
                .total_currency_balance => {},
                else => unreachable,
            }
            try std.testing.expectEqual(primary.risk_engine.getFeeCollected(15), fee_report.fee_collected);

            const sample_uid = user_ids[random.uintLessThan(usize, user_ids.len)];
            var single_user_report = try primary.queryReport(.{ .single_user = sample_uid });
            defer single_user_report.deinit(allocator);
            var quote_balance_report = try primary.queryReport(.{
                .user_balance = .{
                    .uid = sample_uid,
                    .currency = 15,
                },
            });
            defer quote_balance_report.deinit(allocator);
            var base_balance_report = try primary.queryReport(.{
                .user_balance = .{
                    .uid = sample_uid,
                    .currency = 11,
                },
            });
            defer base_balance_report.deinit(allocator);

            switch (single_user_report) {
                .single_user => |single_user| {
                    switch (single_user.query_execution_status) {
                        .ok => {
                            const quote_balance = single_user.accounts.get(15) orelse 0;
                            const base_balance = single_user.accounts.get(11) orelse 0;
                            try std.testing.expectEqual(@as(i64, quote_balance), quote_balance_report.user_balance orelse 0);
                            try std.testing.expectEqual(@as(i64, base_balance), base_balance_report.user_balance orelse 0);
                        },
                        .user_not_found => {
                            try std.testing.expectEqual(@as(?i64, null), quote_balance_report.user_balance);
                            try std.testing.expectEqual(@as(?i64, null), base_balance_report.user_balance);
                        },
                    }
                },
                else => unreachable,
            }

            for (symbols) |symbol| {
                var order_book_report = try primary.queryReport(.{
                    .order_book = .{
                        .symbol = symbol,
                        .depth = 24,
                    },
                });
                defer order_book_report.deinit(allocator);
                switch (order_book_report) {
                    .order_book => {},
                    else => unreachable,
                }
            }

            var snapshot = try snapshot_mod.KernelSnapshot.captureFromKernel(allocator, &primary, symbols[0..], 24);
            defer snapshot.deinit();
            try snapshot.validateAgainstKernel(&primary);

            var snapshot_name_buf: [64]u8 = undefined;
            const snapshot_name = try std.fmt.bufPrint(
                &snapshot_name_buf,
                "multi-sim-seq-{d}.snap",
                .{checkpoint_seq},
            );
            try snapshot.writeToFile(tmp.dir, snapshot_name);

            var loaded_snapshot = try snapshot_mod.KernelSnapshot.readFromFile(allocator, tmp.dir, snapshot_name);
            defer loaded_snapshot.deinit();
            try loaded_snapshot.validateAgainstKernel(&primary);
            try std.testing.expectEqual(snapshot.risk_state_hash, loaded_snapshot.risk_state_hash);

            var restored_checkpoint = try kernel_mod.ExchangeKernel.init(allocator, .{
                .orderbook_kind = .direct,
            });
            defer restored_checkpoint.deinit();
            try loaded_snapshot.restoreIntoKernel(&restored_checkpoint);
            restored_checkpoint.next_seq = checkpoint_seq;

            var restored_risk = try restored_checkpoint.queryReport(.risk_state_hash);
            defer restored_risk.deinit(allocator);
            try std.testing.expectEqual(risk_report.risk_state_hash, restored_risk.risk_state_hash);

            if (previous_checkpoint_seq) |previous_seq| {
                var previous_snapshot_name_buf: [64]u8 = undefined;
                const previous_snapshot_name = try std.fmt.bufPrint(
                    &previous_snapshot_name_buf,
                    "multi-sim-seq-{d}.snap",
                    .{previous_seq},
                );

                var previous_snapshot = try snapshot_mod.KernelSnapshot.readFromFile(
                    allocator,
                    tmp.dir,
                    previous_snapshot_name,
                );
                defer previous_snapshot.deinit();

                var replay_from_previous_snapshot = try kernel_mod.ExchangeKernel.init(allocator, .{
                    .orderbook_kind = .direct,
                });
                defer replay_from_previous_snapshot.deinit();
                try previous_snapshot.restoreIntoKernel(&replay_from_previous_snapshot);
                replay_from_previous_snapshot.next_seq = previous_seq;

                const replay_window = try journal.replayIntoKernelFromSequenceUntilTimestamp(
                    &replay_from_previous_snapshot,
                    previous_seq,
                    std.math.maxInt(u64),
                );

                const expected_replayed_u64 = checkpoint_seq - previous_seq;
                const expected_replayed = std.math.cast(usize, expected_replayed_u64) orelse unreachable;
                try std.testing.expectEqual(expected_replayed, replay_window.replayed);

                var replayed_risk = try replay_from_previous_snapshot.queryReport(.risk_state_hash);
                defer replayed_risk.deinit(allocator);
                try std.testing.expectEqual(risk_report.risk_state_hash, replayed_risk.risk_state_hash);
            }

            previous_checkpoint_seq = checkpoint_seq;
            _ = risk_report.risk_state_hash;
        }
    }

    try std.testing.expect(journal.len() > user_ids.len);
    try journal.replayIntoKernel(&replay);

    try journal.writeToFile(tmp.dir, "multi-simulation.journal");
    var loaded_journal = try CommandJournal.readFromFile(allocator, tmp.dir, "multi-simulation.journal");
    defer loaded_journal.deinit();
    try std.testing.expectEqual(journal.len(), loaded_journal.len());
    try loaded_journal.replayIntoKernel(&replay_from_file);

    var risk_primary = try primary.queryReport(.risk_state_hash);
    defer risk_primary.deinit(allocator);
    var risk_replay = try replay.queryReport(.risk_state_hash);
    defer risk_replay.deinit(allocator);
    var risk_file = try replay_from_file.queryReport(.risk_state_hash);
    defer risk_file.deinit(allocator);
    try std.testing.expectEqual(risk_primary.risk_state_hash, risk_replay.risk_state_hash);
    try std.testing.expectEqual(risk_primary.risk_state_hash, risk_file.risk_state_hash);

    var detailed_primary = try primary.queryReport(.state_hash_detailed);
    defer detailed_primary.deinit(allocator);
    var detailed_replay = try replay.queryReport(.state_hash_detailed);
    defer detailed_replay.deinit(allocator);
    var detailed_file = try replay_from_file.queryReport(.state_hash_detailed);
    defer detailed_file.deinit(allocator);
    try std.testing.expectEqual(
        detailed_primary.state_hash_detailed.aggregate_hash,
        detailed_replay.state_hash_detailed.aggregate_hash,
    );
    try std.testing.expectEqual(
        detailed_primary.state_hash_detailed.aggregate_hash,
        detailed_file.state_hash_detailed.aggregate_hash,
    );

    var final_snapshot = try snapshot_mod.KernelSnapshot.captureFromKernel(allocator, &primary, symbols[0..], 48);
    defer final_snapshot.deinit();
    try final_snapshot.validateAgainstKernel(&replay);
    try final_snapshot.validateAgainstKernel(&replay_from_file);
}
