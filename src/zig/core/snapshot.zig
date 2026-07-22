const std = @import("std");
const io_context = @import("io_context.zig");
const kernel_mod = @import("exchange_kernel.zig");
const naive = @import("orderbook_naive.zig");
const cmd = @import("cmd.zig");
const common = @import("common.zig");
const risk = @import("risk.zig");
const user = @import("user_profile.zig");

const MAGIC_V1 = "XCS1";
const MAGIC_V2 = "XCS2";

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

pub const SnapshotCaptureError = kernel_mod.ReportError ||
    kernel_mod.OrderSnapshotReadError ||
    error{
        SnapshotStateCorrupted,
    };

pub const SnapshotValidationError = kernel_mod.ReportError || error{
    RiskHashMismatch,
    OrderBookMismatch,
};

pub const SnapshotRestoreError = SnapshotValidationError ||
    kernel_mod.OrderSnapshotRestoreError ||
    risk.SymbolSpecificationProvider.AddSymbolError ||
    user.SymbolPositionRecord.ValidationError ||
    std.mem.Allocator.Error ||
    error{
        DuplicateSnapshotSymbol,
        PositionSymbolMissing,
    };

pub const SnapshotIoError = std.mem.Allocator.Error ||
    std.Io.File.OpenError ||
    std.Io.Reader.Error ||
    std.Io.Writer.Error ||
    error{
        InvalidMagic,
        UnsupportedLegacyLz4Format,
        InvalidFormat,
    };

pub const SymbolSnapshot = struct {
    symbol: i32,
    order_book: naive.L2MarketData,
};

pub const I32I64Entry = struct {
    key: i32,
    value: i64,
};

pub const LastPriceCacheEntry = struct {
    symbol: i32,
    ask_price: i64,
    bid_price: i64,
};

pub const PositionSnapshot = struct {
    symbol: i32,
    currency: i32,
    direction: common.PositionDirection,
    open_volume: i64,
    open_price_sum: i64,
    profit: i64,
    pending_sell_size: i64,
    pending_buy_size: i64,
};

pub const UserSnapshot = struct {
    uid: i64,
    user_status: common.UserStatus,
    adjustments_counter: i64,
    accounts: std.ArrayListUnmanaged(I32I64Entry) = .empty,
    positions: std.ArrayListUnmanaged(PositionSnapshot) = .empty,

    pub fn deinit(self: *UserSnapshot, allocator: std.mem.Allocator) void {
        self.accounts.deinit(allocator);
        self.positions.deinit(allocator);
        self.* = undefined;
    }
};

pub const BookSnapshot = struct {
    symbol_spec: common.CoreSymbolSpecification,
    orders: std.ArrayListUnmanaged(kernel_mod.RestingOrder) = .empty,

    pub fn deinit(self: *BookSnapshot, allocator: std.mem.Allocator) void {
        self.orders.deinit(allocator);
        self.* = undefined;
    }
};

pub const KernelSnapshot = struct {
    allocator: std.mem.Allocator,
    depth: usize,
    risk_state_hash: u64,
    symbols: std.ArrayListUnmanaged(SymbolSnapshot) = .empty,
    books: std.ArrayListUnmanaged(BookSnapshot) = .empty,
    users: std.ArrayListUnmanaged(UserSnapshot) = .empty,
    last_price_cache: std.ArrayListUnmanaged(LastPriceCacheEntry) = .empty,
    fees: std.ArrayListUnmanaged(I32I64Entry) = .empty,
    adjustments: std.ArrayListUnmanaged(I32I64Entry) = .empty,
    suspends: std.ArrayListUnmanaged(I32I64Entry) = .empty,

    pub fn deinit(self: *KernelSnapshot) void {
        for (self.symbols.items) |*entry| {
            entry.order_book.deinit(self.allocator);
        }
        self.symbols.deinit(self.allocator);

        for (self.books.items) |*book| {
            book.deinit(self.allocator);
        }
        self.books.deinit(self.allocator);

        for (self.users.items) |*entry| {
            entry.deinit(self.allocator);
        }
        self.users.deinit(self.allocator);

        self.last_price_cache.deinit(self.allocator);
        self.fees.deinit(self.allocator);
        self.adjustments.deinit(self.allocator);
        self.suspends.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn captureFromKernel(
        allocator: std.mem.Allocator,
        kernel: *kernel_mod.ExchangeKernel,
        symbols: []const i32,
        depth: usize,
    ) SnapshotCaptureError!KernelSnapshot {
        var risk_hash_report = try kernel.queryReport(.risk_state_hash);
        defer risk_hash_report.deinit(allocator);

        var snapshot: KernelSnapshot = .{
            .allocator = allocator,
            .depth = depth,
            .risk_state_hash = risk_hash_report.risk_state_hash,
        };
        errdefer snapshot.deinit();

        try snapshot.captureL2Symbols(kernel, symbols);
        try snapshot.captureBooks(kernel);
        try snapshot.captureUsers(kernel);
        try snapshot.captureRiskMaps(kernel);

        return snapshot;
    }

    pub fn validateAgainstKernel(
        self: *const KernelSnapshot,
        kernel: *kernel_mod.ExchangeKernel,
    ) SnapshotValidationError!void {
        var risk_hash_report = try kernel.queryReport(.risk_state_hash);
        defer risk_hash_report.deinit(kernel.allocator);
        if (risk_hash_report.risk_state_hash != self.risk_state_hash) {
            return error.RiskHashMismatch;
        }

        for (self.symbols.items) |entry| {
            var ob_report = try kernel.queryReport(.{
                .order_book = .{
                    .symbol = entry.symbol,
                    .depth = self.depth,
                },
            });
            defer ob_report.deinit(kernel.allocator);

            if (!equalOrderBooks(entry.order_book, ob_report.order_book)) {
                return error.OrderBookMismatch;
            }
        }
    }

    pub fn restoreIntoKernel(
        self: *const KernelSnapshot,
        kernel: *kernel_mod.ExchangeKernel,
    ) SnapshotRestoreError!void {
        kernel.resetForSnapshotLoad();

        for (self.books.items) |book| {
            const inserted = try kernel.addSymbol(book.symbol_spec);
            if (!inserted) return error.DuplicateSnapshotSymbol;
        }

        for (self.books.items) |book| {
            try kernel.restoreOrdersForSymbol(book.symbol_spec.symbol_id, book.orders.items);
        }

        kernel.risk_engine.user_profile_service.reset();
        kernel.risk_engine.last_price_cache.clearRetainingCapacity();
        kernel.risk_engine.fees.clearRetainingCapacity();
        kernel.risk_engine.adjustments.clearRetainingCapacity();
        kernel.risk_engine.suspends.clearRetainingCapacity();

        for (self.users.items) |snapshot_user| {
            var profile = user.UserProfile.init(snapshot_user.uid, snapshot_user.user_status);
            errdefer profile.deinit(kernel.allocator);
            profile.adjustments_counter = snapshot_user.adjustments_counter;

            for (snapshot_user.accounts.items) |entry| {
                try profile.accounts.put(kernel.allocator, entry.key, entry.value);
            }

            for (snapshot_user.positions.items) |entry| {
                if (!kernel.risk_engine.symbol_spec_provider.symbols.contains(entry.symbol)) {
                    return error.PositionSymbolMissing;
                }

                var restored = user.SymbolPositionRecord{
                    .uid = snapshot_user.uid,
                    .symbol = entry.symbol,
                    .currency = entry.currency,
                    .direction = entry.direction,
                    .open_volume = entry.open_volume,
                    .open_price_sum = entry.open_price_sum,
                    .profit = entry.profit,
                    .pending_sell_size = entry.pending_sell_size,
                    .pending_buy_size = entry.pending_buy_size,
                };
                try restored.validateInternalState();
                try profile.positions.put(kernel.allocator, entry.symbol, restored);
            }

            try kernel.risk_engine.user_profile_service.user_profiles.put(
                kernel.allocator,
                snapshot_user.uid,
                profile,
            );
        }

        for (self.last_price_cache.items) |entry| {
            try kernel.risk_engine.last_price_cache.put(kernel.allocator, entry.symbol, .{
                .ask_price = entry.ask_price,
                .bid_price = entry.bid_price,
            });
        }
        for (self.fees.items) |entry| {
            try kernel.risk_engine.fees.put(kernel.allocator, entry.key, entry.value);
        }
        for (self.adjustments.items) |entry| {
            try kernel.risk_engine.adjustments.put(kernel.allocator, entry.key, entry.value);
        }
        for (self.suspends.items) |entry| {
            try kernel.risk_engine.suspends.put(kernel.allocator, entry.key, entry.value);
        }

        try self.validateAgainstKernel(kernel);
    }

    pub fn writeToFile(
        self: *const KernelSnapshot,
        dir: std.Io.Dir,
        path: []const u8,
    ) SnapshotIoError!void {
        const file = try dir.createFile(io_context.current(), path, .{ .truncate = true });
        defer file.close(io_context.current());

        var buffer: [4096]u8 = undefined;
        var file_writer = file.writer(io_context.current(), &buffer);
        const writer = &file_writer.interface;

        try writer.writeAll(MAGIC_V2);
        try writeInt(u64, @TypeOf(writer), writer, self.depth);
        try writeInt(u64, @TypeOf(writer), writer, self.risk_state_hash);
        try writeInt(u32, @TypeOf(writer), writer, try castLenU32(self.symbols.items.len));
        for (self.symbols.items) |entry| {
            try writeInt(i32, @TypeOf(writer), writer, entry.symbol);
            try writeOrderBook(@TypeOf(writer), writer, entry.order_book);
        }

        try writeInt(u32, @TypeOf(writer), writer, try castLenU32(self.books.items.len));
        for (self.books.items) |book| {
            try writeSymbolSpec(@TypeOf(writer), writer, book.symbol_spec);
            try writeInt(u32, @TypeOf(writer), writer, try castLenU32(book.orders.items.len));
            for (book.orders.items) |order| {
                try writeRestingOrder(@TypeOf(writer), writer, order);
            }
        }

        try writeInt(u32, @TypeOf(writer), writer, try castLenU32(self.users.items.len));
        for (self.users.items) |snapshot_user| {
            try writeInt(i64, @TypeOf(writer), writer, snapshot_user.uid);
            try writeInt(i8, @TypeOf(writer), writer, snapshot_user.user_status.code());
            try writeInt(i64, @TypeOf(writer), writer, snapshot_user.adjustments_counter);

            try writeInt(u32, @TypeOf(writer), writer, try castLenU32(snapshot_user.accounts.items.len));
            for (snapshot_user.accounts.items) |entry| {
                try writeI32I64Entry(@TypeOf(writer), writer, entry);
            }

            try writeInt(u32, @TypeOf(writer), writer, try castLenU32(snapshot_user.positions.items.len));
            for (snapshot_user.positions.items) |entry| {
                try writePositionSnapshot(@TypeOf(writer), writer, entry);
            }
        }

        try writeInt(u32, @TypeOf(writer), writer, try castLenU32(self.last_price_cache.items.len));
        for (self.last_price_cache.items) |entry| {
            try writeInt(i32, @TypeOf(writer), writer, entry.symbol);
            try writeInt(i64, @TypeOf(writer), writer, entry.ask_price);
            try writeInt(i64, @TypeOf(writer), writer, entry.bid_price);
        }

        try writeInt(u32, @TypeOf(writer), writer, try castLenU32(self.fees.items.len));
        for (self.fees.items) |entry| {
            try writeI32I64Entry(@TypeOf(writer), writer, entry);
        }

        try writeInt(u32, @TypeOf(writer), writer, try castLenU32(self.adjustments.items.len));
        for (self.adjustments.items) |entry| {
            try writeI32I64Entry(@TypeOf(writer), writer, entry);
        }

        try writeInt(u32, @TypeOf(writer), writer, try castLenU32(self.suspends.items.len));
        for (self.suspends.items) |entry| {
            try writeI32I64Entry(@TypeOf(writer), writer, entry);
        }

        try writer.flush();
    }

    pub fn readFromFile(
        allocator: std.mem.Allocator,
        dir: std.Io.Dir,
        path: []const u8,
    ) SnapshotIoError!KernelSnapshot {
        const file = try dir.openFile(io_context.current(), path, .{});
        defer file.close(io_context.current());

        var buffer: [4096]u8 = undefined;
        var file_reader = file.reader(io_context.current(), &buffer);
        const reader = &file_reader.interface;

        const magic = try reader.takeArray(4);
        const is_v1 = std.mem.eql(u8, magic[0..], MAGIC_V1);
        const is_v2 = std.mem.eql(u8, magic[0..], MAGIC_V2);
        if (!is_v1 and !is_v2) {
            if (isLegacyLz4FrameMagic(magic[0..])) {
                return error.UnsupportedLegacyLz4Format;
            }
            return error.InvalidMagic;
        }

        const depth_u64 = try readInt(u64, @TypeOf(reader), reader);
        const depth = std.math.cast(usize, depth_u64) orelse return error.InvalidFormat;
        const risk_state_hash = try readInt(u64, @TypeOf(reader), reader);
        const symbol_count = try readCount(@TypeOf(reader), reader);

        var snapshot: KernelSnapshot = .{
            .allocator = allocator,
            .depth = depth,
            .risk_state_hash = risk_state_hash,
        };
        errdefer snapshot.deinit();

        try snapshot.symbols.ensureTotalCapacityPrecise(allocator, symbol_count);
        var i: usize = 0;
        while (i < symbol_count) : (i += 1) {
            const symbol = try readInt(i32, @TypeOf(reader), reader);
            const order_book = try readOrderBook(allocator, @TypeOf(reader), reader);
            snapshot.symbols.appendAssumeCapacity(.{
                .symbol = symbol,
                .order_book = order_book,
            });
        }

        if (is_v1) {
            return snapshot;
        }

        const book_count = try readCount(@TypeOf(reader), reader);
        try snapshot.books.ensureTotalCapacityPrecise(allocator, book_count);
        i = 0;
        while (i < book_count) : (i += 1) {
            var book: BookSnapshot = .{
                .symbol_spec = try readSymbolSpec(@TypeOf(reader), reader),
            };
            errdefer book.deinit(allocator);

            const order_count = try readCount(@TypeOf(reader), reader);
            try book.orders.ensureTotalCapacityPrecise(allocator, order_count);
            var order_i: usize = 0;
            while (order_i < order_count) : (order_i += 1) {
                book.orders.appendAssumeCapacity(try readRestingOrder(@TypeOf(reader), reader));
            }

            snapshot.books.appendAssumeCapacity(book);
        }

        const user_count = try readCount(@TypeOf(reader), reader);
        try snapshot.users.ensureTotalCapacityPrecise(allocator, user_count);
        i = 0;
        while (i < user_count) : (i += 1) {
            var snapshot_user: UserSnapshot = .{
                .uid = try readInt(i64, @TypeOf(reader), reader),
                .user_status = common.UserStatus.fromCode(try readInt(i8, @TypeOf(reader), reader)) catch
                    return error.InvalidFormat,
                .adjustments_counter = try readInt(i64, @TypeOf(reader), reader),
            };
            errdefer snapshot_user.deinit(allocator);

            const account_count = try readCount(@TypeOf(reader), reader);
            try snapshot_user.accounts.ensureTotalCapacityPrecise(allocator, account_count);
            var account_i: usize = 0;
            while (account_i < account_count) : (account_i += 1) {
                snapshot_user.accounts.appendAssumeCapacity(try readI32I64Entry(@TypeOf(reader), reader));
            }

            const position_count = try readCount(@TypeOf(reader), reader);
            try snapshot_user.positions.ensureTotalCapacityPrecise(allocator, position_count);
            var position_i: usize = 0;
            while (position_i < position_count) : (position_i += 1) {
                snapshot_user.positions.appendAssumeCapacity(try readPositionSnapshot(@TypeOf(reader), reader));
            }

            snapshot.users.appendAssumeCapacity(snapshot_user);
        }

        const last_price_count = try readCount(@TypeOf(reader), reader);
        try snapshot.last_price_cache.ensureTotalCapacityPrecise(allocator, last_price_count);
        i = 0;
        while (i < last_price_count) : (i += 1) {
            snapshot.last_price_cache.appendAssumeCapacity(.{
                .symbol = try readInt(i32, @TypeOf(reader), reader),
                .ask_price = try readInt(i64, @TypeOf(reader), reader),
                .bid_price = try readInt(i64, @TypeOf(reader), reader),
            });
        }

        const fees_count = try readCount(@TypeOf(reader), reader);
        try snapshot.fees.ensureTotalCapacityPrecise(allocator, fees_count);
        i = 0;
        while (i < fees_count) : (i += 1) {
            snapshot.fees.appendAssumeCapacity(try readI32I64Entry(@TypeOf(reader), reader));
        }

        const adjustments_count = try readCount(@TypeOf(reader), reader);
        try snapshot.adjustments.ensureTotalCapacityPrecise(allocator, adjustments_count);
        i = 0;
        while (i < adjustments_count) : (i += 1) {
            snapshot.adjustments.appendAssumeCapacity(try readI32I64Entry(@TypeOf(reader), reader));
        }

        const suspends_count = try readCount(@TypeOf(reader), reader);
        try snapshot.suspends.ensureTotalCapacityPrecise(allocator, suspends_count);
        i = 0;
        while (i < suspends_count) : (i += 1) {
            snapshot.suspends.appendAssumeCapacity(try readI32I64Entry(@TypeOf(reader), reader));
        }

        return snapshot;
    }

    fn captureL2Symbols(
        self: *KernelSnapshot,
        kernel: *kernel_mod.ExchangeKernel,
        symbols: []const i32,
    ) SnapshotCaptureError!void {
        try self.symbols.ensureTotalCapacityPrecise(self.allocator, symbols.len);
        for (symbols) |symbol| {
            var ob_report = try kernel.queryReport(.{
                .order_book = .{
                    .symbol = symbol,
                    .depth = self.depth,
                },
            });
            defer ob_report.deinit(self.allocator);

            const order_book_copy = try cloneOrderBook(self.allocator, ob_report.order_book);
            self.symbols.appendAssumeCapacity(.{
                .symbol = symbol,
                .order_book = order_book_copy,
            });
        }
    }

    fn captureBooks(
        self: *KernelSnapshot,
        kernel: *kernel_mod.ExchangeKernel,
    ) SnapshotCaptureError!void {
        var symbol_ids: std.ArrayListUnmanaged(i32) = .empty;
        defer symbol_ids.deinit(self.allocator);

        try symbol_ids.ensureTotalCapacityPrecise(
            self.allocator,
            kernel.risk_engine.symbol_spec_provider.symbols.count(),
        );

        var symbol_it = kernel.risk_engine.symbol_spec_provider.symbols.iterator();
        while (symbol_it.next()) |entry| {
            symbol_ids.appendAssumeCapacity(entry.key_ptr.*);
        }
        std.sort.heap(i32, symbol_ids.items, {}, lessThanI32);

        try self.books.ensureTotalCapacityPrecise(self.allocator, symbol_ids.items.len);
        for (symbol_ids.items) |symbol| {
            const spec = kernel.risk_engine.symbol_spec_provider.symbols.get(symbol) orelse
                return error.SnapshotStateCorrupted;

            var book: BookSnapshot = .{
                .symbol_spec = spec,
            };
            errdefer book.deinit(self.allocator);

            try kernel.appendOrdersForSymbol(self.allocator, symbol, &book.orders);
            self.books.appendAssumeCapacity(book);
        }
    }

    fn captureUsers(
        self: *KernelSnapshot,
        kernel: *kernel_mod.ExchangeKernel,
    ) SnapshotCaptureError!void {
        var uids: std.ArrayListUnmanaged(i64) = .empty;
        defer uids.deinit(self.allocator);

        const user_profiles = &kernel.risk_engine.user_profile_service.user_profiles;
        try uids.ensureTotalCapacityPrecise(self.allocator, user_profiles.count());

        var uid_it = user_profiles.iterator();
        while (uid_it.next()) |entry| {
            uids.appendAssumeCapacity(entry.key_ptr.*);
        }
        std.sort.heap(i64, uids.items, {}, lessThanI64);

        try self.users.ensureTotalCapacityPrecise(self.allocator, uids.items.len);
        for (uids.items) |uid| {
            const profile = user_profiles.getPtr(uid) orelse return error.SnapshotStateCorrupted;

            var snapshot_user: UserSnapshot = .{
                .uid = uid,
                .user_status = profile.user_status,
                .adjustments_counter = profile.adjustments_counter,
            };
            errdefer snapshot_user.deinit(self.allocator);

            try copyI32I64MapSorted(
                self.allocator,
                &snapshot_user.accounts,
                &profile.accounts,
            );

            var symbols: std.ArrayListUnmanaged(i32) = .empty;
            defer symbols.deinit(self.allocator);

            try symbols.ensureTotalCapacityPrecise(self.allocator, profile.positions.count());
            var pos_it = profile.positions.iterator();
            while (pos_it.next()) |entry| {
                symbols.appendAssumeCapacity(entry.key_ptr.*);
            }
            std.sort.heap(i32, symbols.items, {}, lessThanI32);

            try snapshot_user.positions.ensureTotalCapacityPrecise(self.allocator, symbols.items.len);
            for (symbols.items) |symbol| {
                const position = profile.positions.get(symbol) orelse return error.SnapshotStateCorrupted;
                snapshot_user.positions.appendAssumeCapacity(.{
                    .symbol = position.symbol,
                    .currency = position.currency,
                    .direction = position.direction,
                    .open_volume = position.open_volume,
                    .open_price_sum = position.open_price_sum,
                    .profit = position.profit,
                    .pending_sell_size = position.pending_sell_size,
                    .pending_buy_size = position.pending_buy_size,
                });
            }

            self.users.appendAssumeCapacity(snapshot_user);
        }
    }

    fn captureRiskMaps(
        self: *KernelSnapshot,
        kernel: *kernel_mod.ExchangeKernel,
    ) SnapshotCaptureError!void {
        var symbols: std.ArrayListUnmanaged(i32) = .empty;
        defer symbols.deinit(self.allocator);

        try symbols.ensureTotalCapacityPrecise(self.allocator, kernel.risk_engine.last_price_cache.count());
        var price_it = kernel.risk_engine.last_price_cache.iterator();
        while (price_it.next()) |entry| {
            symbols.appendAssumeCapacity(entry.key_ptr.*);
        }
        std.sort.heap(i32, symbols.items, {}, lessThanI32);

        try self.last_price_cache.ensureTotalCapacityPrecise(self.allocator, symbols.items.len);
        for (symbols.items) |symbol| {
            const record = kernel.risk_engine.last_price_cache.get(symbol) orelse return error.SnapshotStateCorrupted;
            self.last_price_cache.appendAssumeCapacity(.{
                .symbol = symbol,
                .ask_price = record.ask_price,
                .bid_price = record.bid_price,
            });
        }

        try copyI32I64MapSorted(self.allocator, &self.fees, &kernel.risk_engine.fees);
        try copyI32I64MapSorted(self.allocator, &self.adjustments, &kernel.risk_engine.adjustments);
        try copyI32I64MapSorted(self.allocator, &self.suspends, &kernel.risk_engine.suspends);
    }
};

fn copyI32I64MapSorted(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(I32I64Entry),
    map: *const std.AutoHashMapUnmanaged(i32, i64),
) SnapshotCaptureError!void {
    var keys: std.ArrayListUnmanaged(i32) = .empty;
    defer keys.deinit(allocator);

    try keys.ensureTotalCapacityPrecise(allocator, map.count());
    var it = map.iterator();
    while (it.next()) |entry| {
        keys.appendAssumeCapacity(entry.key_ptr.*);
    }
    std.sort.heap(i32, keys.items, {}, lessThanI32);

    try out.ensureTotalCapacityPrecise(allocator, keys.items.len);
    for (keys.items) |key| {
        const value = map.get(key) orelse return error.SnapshotStateCorrupted;
        out.appendAssumeCapacity(.{
            .key = key,
            .value = value,
        });
    }
}

fn castLenU32(len: usize) SnapshotIoError!u32 {
    return std.math.cast(u32, len) orelse error.InvalidFormat;
}

fn readCount(comptime Reader: type, reader: Reader) SnapshotIoError!usize {
    const count_u32 = try readInt(u32, Reader, reader);
    return @as(usize, count_u32);
}

fn cloneOrderBook(
    allocator: std.mem.Allocator,
    source: naive.L2MarketData,
) std.mem.Allocator.Error!naive.L2MarketData {
    var cloned: naive.L2MarketData = .{};
    errdefer cloned.deinit(allocator);

    try cloned.asks.ensureTotalCapacityPrecise(allocator, source.asks.items.len);
    for (source.asks.items) |entry| {
        cloned.asks.appendAssumeCapacity(entry);
    }

    try cloned.bids.ensureTotalCapacityPrecise(allocator, source.bids.items.len);
    for (source.bids.items) |entry| {
        cloned.bids.appendAssumeCapacity(entry);
    }

    return cloned;
}

fn equalOrderBooks(lhs: naive.L2MarketData, rhs: naive.L2MarketData) bool {
    if (lhs.asks.items.len != rhs.asks.items.len) return false;
    if (lhs.bids.items.len != rhs.bids.items.len) return false;

    for (lhs.asks.items, rhs.asks.items) |left, right| {
        if (left.price != right.price) return false;
        if (left.volume != right.volume) return false;
        if (left.orders != right.orders) return false;
    }
    for (lhs.bids.items, rhs.bids.items) |left, right| {
        if (left.price != right.price) return false;
        if (left.volume != right.volume) return false;
        if (left.orders != right.orders) return false;
    }
    return true;
}

fn writeOrderBook(
    comptime Writer: type,
    writer: Writer,
    order_book: naive.L2MarketData,
) !void {
    try writeInt(u32, Writer, writer, @as(u32, @intCast(order_book.asks.items.len)));
    for (order_book.asks.items) |entry| {
        try writeInt(i64, Writer, writer, entry.price);
        try writeInt(i64, Writer, writer, entry.volume);
        try writeInt(u32, Writer, writer, entry.orders);
    }

    try writeInt(u32, Writer, writer, @as(u32, @intCast(order_book.bids.items.len)));
    for (order_book.bids.items) |entry| {
        try writeInt(i64, Writer, writer, entry.price);
        try writeInt(i64, Writer, writer, entry.volume);
        try writeInt(u32, Writer, writer, entry.orders);
    }
}

fn readOrderBook(
    allocator: std.mem.Allocator,
    comptime Reader: type,
    reader: Reader,
) SnapshotIoError!naive.L2MarketData {
    var result: naive.L2MarketData = .{};
    errdefer result.deinit(allocator);

    const asks_count = try readCount(Reader, reader);
    try result.asks.ensureTotalCapacityPrecise(allocator, asks_count);
    var ask_i: usize = 0;
    while (ask_i < asks_count) : (ask_i += 1) {
        result.asks.appendAssumeCapacity(.{
            .price = try readInt(i64, Reader, reader),
            .volume = try readInt(i64, Reader, reader),
            .orders = try readInt(u32, Reader, reader),
        });
    }

    const bids_count = try readCount(Reader, reader);
    try result.bids.ensureTotalCapacityPrecise(allocator, bids_count);
    var bid_i: usize = 0;
    while (bid_i < bids_count) : (bid_i += 1) {
        result.bids.appendAssumeCapacity(.{
            .price = try readInt(i64, Reader, reader),
            .volume = try readInt(i64, Reader, reader),
            .orders = try readInt(u32, Reader, reader),
        });
    }

    return result;
}

fn writeI32I64Entry(
    comptime Writer: type,
    writer: Writer,
    entry: I32I64Entry,
) !void {
    try writeInt(i32, Writer, writer, entry.key);
    try writeInt(i64, Writer, writer, entry.value);
}

fn readI32I64Entry(comptime Reader: type, reader: Reader) SnapshotIoError!I32I64Entry {
    return .{
        .key = try readInt(i32, Reader, reader),
        .value = try readInt(i64, Reader, reader),
    };
}

fn writeSymbolSpec(
    comptime Writer: type,
    writer: Writer,
    spec: common.CoreSymbolSpecification,
) !void {
    try writeInt(i32, Writer, writer, spec.symbol_id);
    try writeInt(i8, Writer, writer, spec.symbol_type.code());
    try writeInt(i32, Writer, writer, spec.base_currency);
    try writeInt(i32, Writer, writer, spec.quote_currency);
    try writeInt(i64, Writer, writer, spec.base_scale_k);
    try writeInt(i64, Writer, writer, spec.quote_scale_k);
    try writeInt(i64, Writer, writer, spec.taker_fee);
    try writeInt(i64, Writer, writer, spec.maker_fee);
    try writeInt(i64, Writer, writer, spec.margin_buy);
    try writeInt(i64, Writer, writer, spec.margin_sell);
}

fn readSymbolSpec(comptime Reader: type, reader: Reader) SnapshotIoError!common.CoreSymbolSpecification {
    const symbol_id = try readInt(i32, Reader, reader);
    const symbol_type_raw = try readInt(i8, Reader, reader);
    const symbol_type = common.SymbolType.fromCode(@as(i32, symbol_type_raw)) catch
        return error.InvalidFormat;

    return common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol_id,
        .symbol_type = symbol_type,
        .base_currency = try readInt(i32, Reader, reader),
        .quote_currency = try readInt(i32, Reader, reader),
        .base_scale_k = try readInt(i64, Reader, reader),
        .quote_scale_k = try readInt(i64, Reader, reader),
        .taker_fee = try readInt(i64, Reader, reader),
        .maker_fee = try readInt(i64, Reader, reader),
        .margin_buy = try readInt(i64, Reader, reader),
        .margin_sell = try readInt(i64, Reader, reader),
    }) catch return error.InvalidFormat;
}

fn writeRestingOrder(
    comptime Writer: type,
    writer: Writer,
    order: kernel_mod.RestingOrder,
) !void {
    try writeInt(i32, Writer, writer, order.symbol);
    try writeInt(i64, Writer, writer, order.order_id);
    try writeInt(i64, Writer, writer, order.price);
    try writeInt(i64, Writer, writer, order.size);
    try writeInt(i64, Writer, writer, order.filled);
    try writeInt(i64, Writer, writer, order.reserve_bid_price);
    try writeInt(i8, Writer, writer, order.action.code());
    try writeInt(i64, Writer, writer, order.uid);
    try writeInt(i64, Writer, writer, order.timestamp);
}

fn readRestingOrder(
    comptime Reader: type,
    reader: Reader,
) SnapshotIoError!kernel_mod.RestingOrder {
    return .{
        .symbol = try readInt(i32, Reader, reader),
        .order_id = try readInt(i64, Reader, reader),
        .price = try readInt(i64, Reader, reader),
        .size = try readInt(i64, Reader, reader),
        .filled = try readInt(i64, Reader, reader),
        .reserve_bid_price = try readInt(i64, Reader, reader),
        .action = common.OrderAction.fromCode(try readInt(i8, Reader, reader)) catch
            return error.InvalidFormat,
        .uid = try readInt(i64, Reader, reader),
        .timestamp = try readInt(i64, Reader, reader),
    };
}

fn writePositionSnapshot(
    comptime Writer: type,
    writer: Writer,
    entry: PositionSnapshot,
) !void {
    try writeInt(i32, Writer, writer, entry.symbol);
    try writeInt(i32, Writer, writer, entry.currency);
    try writeInt(i8, Writer, writer, @intFromEnum(entry.direction));
    try writeInt(i64, Writer, writer, entry.open_volume);
    try writeInt(i64, Writer, writer, entry.open_price_sum);
    try writeInt(i64, Writer, writer, entry.profit);
    try writeInt(i64, Writer, writer, entry.pending_sell_size);
    try writeInt(i64, Writer, writer, entry.pending_buy_size);
}

fn readPositionSnapshot(
    comptime Reader: type,
    reader: Reader,
) SnapshotIoError!PositionSnapshot {
    return .{
        .symbol = try readInt(i32, Reader, reader),
        .currency = try readInt(i32, Reader, reader),
        .direction = common.PositionDirection.fromCode(try readInt(i8, Reader, reader)) catch
            return error.InvalidFormat,
        .open_volume = try readInt(i64, Reader, reader),
        .open_price_sum = try readInt(i64, Reader, reader),
        .profit = try readInt(i64, Reader, reader),
        .pending_sell_size = try readInt(i64, Reader, reader),
        .pending_buy_size = try readInt(i64, Reader, reader),
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
) SnapshotIoError!T {
    return try reader.takeInt(T, .little);
}

fn lessThanI32(_: void, lhs: i32, rhs: i32) bool {
    return lhs < rhs;
}

fn lessThanI64(_: void, lhs: i64, rhs: i64) bool {
    return lhs < rhs;
}

fn makeExchangeSpec() !common.CoreSymbolSpecification {
    return common.CoreSymbolSpecification.init(.{
        .symbol_id = 241,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 100,
        .quote_scale_k = 10,
        .taker_fee = 2,
        .maker_fee = 1,
    });
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

fn findOrder(
    orders: []const naive.UserOrderView,
    order_id: i64,
) ?naive.UserOrderView {
    for (orders) |order| {
        if (order.order_id == order_id) return order;
    }
    return null;
}

test "snapshot roundtrip and kernel validation" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer kernel.deinit();
    try std.testing.expect(try kernel.addSymbol(try makeExchangeSpec()));

    var add_bidder = makeAddUserCommand(9801);
    var add_asker = makeAddUserCommand(9802);
    var dep_quote = makeBalanceAdjustmentCommand(9801, 15, 50_000, 1, .adjustment);
    var dep_base = makeBalanceAdjustmentCommand(9802, 11, 50_000, 1, .adjustment);
    _ = try kernel.execute(&add_bidder);
    _ = try kernel.execute(&add_asker);
    _ = try kernel.execute(&dep_quote);
    _ = try kernel.execute(&dep_base);

    var bid = cmd.OrderCommand.newOrder(.gtc, 12001, 9801, 100, 100, 5, .bid);
    bid.symbol = 241;
    var ask = cmd.OrderCommand.newOrder(.gtc, 12002, 9802, 102, 0, 3, .ask);
    ask.symbol = 241;
    _ = try kernel.execute(&bid);
    _ = try kernel.execute(&ask);

    var snapshot = try KernelSnapshot.captureFromKernel(allocator, &kernel, &.{241}, 10);
    defer snapshot.deinit();
    try snapshot.validateAgainstKernel(&kernel);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try snapshot.writeToFile(tmp.dir, "snapshot.bin");

    var loaded = try KernelSnapshot.readFromFile(allocator, tmp.dir, "snapshot.bin");
    defer loaded.deinit();
    try loaded.validateAgainstKernel(&kernel);
    try std.testing.expectEqual(snapshot.risk_state_hash, loaded.risk_state_hash);
    try std.testing.expectEqual(snapshot.symbols.items.len, loaded.symbols.items.len);
    try std.testing.expectEqual(snapshot.books.items.len, loaded.books.items.len);
}

test "snapshot restore reproduces full kernel state" {
    const allocator = std.testing.allocator;

    var source = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer source.deinit();
    try std.testing.expect(try source.addSymbol(try makeExchangeSpec()));

    var add_u1 = makeAddUserCommand(9811);
    var add_u2 = makeAddUserCommand(9812);
    var add_u3 = makeAddUserCommand(9813);
    var add_u1_response = try source.execute(&add_u1);
    defer add_u1_response.deinit(allocator);
    var add_u2_response = try source.execute(&add_u2);
    defer add_u2_response.deinit(allocator);
    var add_u3_response = try source.execute(&add_u3);
    defer add_u3_response.deinit(allocator);

    var dep_u1 = makeBalanceAdjustmentCommand(9811, 15, 100_000, 1, .adjustment);
    var dep_u2 = makeBalanceAdjustmentCommand(9812, 11, 100_000, 1, .adjustment);
    var dep_u3 = makeBalanceAdjustmentCommand(9813, 11, 100_000, 1, .adjustment);
    var dep_u1_response = try source.execute(&dep_u1);
    defer dep_u1_response.deinit(allocator);
    var dep_u2_response = try source.execute(&dep_u2);
    defer dep_u2_response.deinit(allocator);
    var dep_u3_response = try source.execute(&dep_u3);
    defer dep_u3_response.deinit(allocator);

    var resting_bid = cmd.OrderCommand.newOrder(.gtc, 12101, 9811, 100, 100, 5, .bid);
    resting_bid.symbol = 241;
    var resting_bid_response = try source.execute(&resting_bid);
    defer resting_bid_response.deinit(allocator);

    var partial_fill = cmd.OrderCommand.newOrder(.gtc, 12102, 9812, 99, 0, 2, .ask);
    partial_fill.symbol = 241;
    var partial_fill_response = try source.execute(&partial_fill);
    defer partial_fill_response.deinit(allocator);

    var resting_ask = cmd.OrderCommand.newOrder(.gtc, 12103, 9813, 105, 0, 4, .ask);
    resting_ask.symbol = 241;
    var resting_ask_response = try source.execute(&resting_ask);
    defer resting_ask_response.deinit(allocator);

    var source_hash = try source.queryReport(.risk_state_hash);
    defer source_hash.deinit(allocator);

    var snapshot = try KernelSnapshot.captureFromKernel(allocator, &source, &.{241}, 20);
    defer snapshot.deinit();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try snapshot.writeToFile(tmp.dir, "restore.bin");

    var loaded = try KernelSnapshot.readFromFile(allocator, tmp.dir, "restore.bin");
    defer loaded.deinit();

    var target = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer target.deinit();

    try loaded.restoreIntoKernel(&target);

    var target_hash = try target.queryReport(.risk_state_hash);
    defer target_hash.deinit(allocator);
    try std.testing.expectEqual(source_hash.risk_state_hash, target_hash.risk_state_hash);

    var source_user = try source.queryReport(.{ .single_user = 9811 });
    defer source_user.deinit(allocator);
    var target_user = try target.queryReport(.{ .single_user = 9811 });
    defer target_user.deinit(allocator);

    const source_order = findOrder(source_user.single_user.orders.items, 12101) orelse unreachable;
    const target_order = findOrder(target_user.single_user.orders.items, 12101) orelse unreachable;
    try std.testing.expectEqual(source_order.filled, target_order.filled);
    try std.testing.expectEqual(source_order.size, target_order.size);
}

test "snapshot validation detects mismatched state" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .naive,
    });
    defer kernel.deinit();
    try std.testing.expect(try kernel.addSymbol(try makeExchangeSpec()));

    var add_u = makeAddUserCommand(9901);
    var dep_q = makeBalanceAdjustmentCommand(9901, 15, 10_000, 1, .adjustment);
    _ = try kernel.execute(&add_u);
    _ = try kernel.execute(&dep_q);

    var snapshot = try KernelSnapshot.captureFromKernel(allocator, &kernel, &.{241}, 10);
    defer snapshot.deinit();

    var bid = cmd.OrderCommand.newOrder(.gtc, 13001, 9901, 100, 100, 2, .bid);
    bid.symbol = 241;
    _ = try kernel.execute(&bid);

    try std.testing.expectError(error.RiskHashMismatch, snapshot.validateAgainstKernel(&kernel));
}

test "snapshot readFromFile reports unsupported legacy lz4 frame explicitly" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const file = try tmp.dir.createFile(io_context.current(), "legacy_lz4.snapshot", .{ .truncate = true });
    defer file.close(io_context.current());
    try file.writeStreamingAll(io_context.current(), &.{ 0x04, 0x22, 0x4D, 0x18 });

    try std.testing.expectError(
        error.UnsupportedLegacyLz4Format,
        KernelSnapshot.readFromFile(allocator, tmp.dir, "legacy_lz4.snapshot"),
    );
}
