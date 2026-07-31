const std = @import("std");
const binary_frames = @import("binary_frames.zig");
const int_map_order = @import("eclipse_int_map_order.zig");
const io_context = @import("io_context.zig");
const kernel_mod = @import("exchange_kernel.zig");
const naive = @import("orderbook_naive.zig");
const cmd = @import("cmd.zig");
const common = @import("common.zig");
const report_registry = @import("report_registry.zig");
const risk = @import("risk.zig");
const user = @import("user_profile.zig");

const MAGIC_V1 = "XCS1";
const MAGIC_V2 = "XCS2";
const MATCHING_MAGIC_V1 = "XME1";
const RISK_MAGIC_V1 = "XRE1";
const MANIFEST_MAGIC_V1 = "XCM1";
const DURABLE_FORMAT_VERSION_V1: u16 = 1;
const MATCHING_FORMAT_VERSION: u16 = 2;

pub const LegacySnapshotCompatibilityError = error{
    LegacySnapshotCustomReportRegistryUnsupported,
};

fn rejectNonEmptyCustomReportRegistry(
    kernel: *const kernel_mod.ExchangeKernel,
) LegacySnapshotCompatibilityError!void {
    if (kernel.customReportRegistry().len() != 0) {
        return error.LegacySnapshotCustomReportRegistryUnsupported;
    }
}

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
    LegacySnapshotCompatibilityError ||
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
    int_map_order.LayoutError ||
    user.SymbolPositionRecord.ValidationError ||
    std.mem.Allocator.Error ||
    LegacySnapshotCompatibilityError ||
    error{
        DuplicateSnapshotSymbol,
        PositionSymbolMissing,
    };

pub const SnapshotIoError = std.mem.Allocator.Error ||
    std.Io.File.OpenError ||
    std.Io.Dir.CreateFileAtomicError ||
    std.Io.File.Atomic.LinkError ||
    std.Io.File.SyncError ||
    std.Io.Reader.Error ||
    std.Io.Writer.Error ||
    error{
        InvalidMagic,
        UnsupportedLegacyLz4Format,
        InvalidFormat,
    };

pub const DurableSnapshotIoError = SnapshotIoError ||
    std.Io.File.Atomic.ReplaceError ||
    std.Io.File.StatError ||
    std.Io.Dir.RenameError ||
    io_context.ReadToEndAllocError ||
    error{
        SnapshotPayloadTooLarge,
        PayloadDigestMismatch,
        SnapshotMetadataMismatch,
    };

pub const DurableSnapshotError = SnapshotCaptureError || DurableSnapshotIoError;
pub const DurableSnapshotRestoreError = DurableSnapshotIoError ||
    SnapshotRestoreError ||
    kernel_mod.InitError ||
    binary_frames.AssemblyError;

pub const DurableSnapshotMetadata = struct {
    snapshot_id: u64,
    command_seq: u64,
    next_seq: u64,
    timestamp_ns: u64,
    shard_id: u32,
    shard_count: u32,
};

const DurablePayloadHeader = struct {
    format_version: u16,
    metadata: DurableSnapshotMetadata,
};

const ParentDirectorySyncError = std.Io.File.OpenError || std.Io.File.SyncError;

pub const PayloadDigest = struct {
    byte_len: u64,
    hash: u64,
};

pub const SnapshotPairManifest = struct {
    snapshot_id: u64,
    matching_seq: u64,
    risk_seq: u64,
    next_seq: u64,
    timestamp_ns: u64,
    matching_shards: u32,
    risk_shards: u32,
    matching: PayloadDigest,
    risk: PayloadDigest,
};

pub const MatchingPayloadSummary = struct {
    metadata: DurableSnapshotMetadata,
    book_count: u32,
    pending_transfer_count: u32,
};

pub const RiskPayloadSummary = struct {
    metadata: DurableSnapshotMetadata,
    symbol_count: u32,
    user_count: u32,
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
        try rejectNonEmptyCustomReportRegistry(kernel);

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
        try rejectNonEmptyCustomReportRegistry(kernel);

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

        // XCS/XME/XRE encode sorted semantic entries only. They do not contain
        // Eclipse Collections backing slots, sentinel flags, tombstones, or
        // lazy constructor capacities. Keep every affected history explicitly
        // unavailable rather than exposing the temporary empty histories used
        // while rebuilding the semantic maps above.
        var restored_profiles = kernel.risk_engine.user_profile_service.user_profiles.valueIterator();
        while (restored_profiles.next()) |profile| {
            profile.markAccountsHistoryUnavailable(kernel.allocator);
            profile.markPositionsHistoryUnavailable(kernel.allocator);
        }
        kernel.risk_engine.user_profile_service.markUserProfilesHistoryUnavailable();
        kernel.risk_engine.markAggregateHistoriesUnavailable();

        try self.validateAgainstKernel(kernel);
    }

    pub fn writeToFile(
        self: *const KernelSnapshot,
        dir: std.Io.Dir,
        path: []const u8,
    ) SnapshotIoError!void {
        var atomic_file = try dir.createFileAtomic(io_context.current(), path, .{
            .replace = false,
        });
        defer atomic_file.deinit(io_context.current());
        const file = atomic_file.file;

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
        try file.sync(io_context.current());
        try atomic_file.link(io_context.current());
        try syncParentDirectory(dir, path);
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

/// Persist the directory entry created by an atomic publication. `path_only`
/// must remain false because directory synchronization requires a real file
/// descriptor rather than an O_PATH-style metadata handle.
fn syncParentDirectory(
    dir: std.Io.Dir,
    path: []const u8,
) ParentDirectorySyncError!void {
    const parent_path = std.fs.path.dirname(path) orelse ".";
    const parent = try dir.openFile(io_context.current(), parent_path, .{
        .mode = .read_only,
        .allow_directory = true,
        .path_only = false,
    });
    defer parent.close(io_context.current());
    try parent.sync(io_context.current());
}

/// Writes a matching-engine-only payload through a durable temporary file and
/// atomically publishes it at `path`. Payload publication deliberately permits
/// replacement: an XME/XRE orphan may be retried with the same snapshot ID
/// before the no-replace XCM manifest commit makes that ID authoritative. The
/// payload contains symbol/order-book state and every in-flight binary
/// transfer, but no risk/user state.
pub fn writeMatchingPayloadAtomic(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    path: []const u8,
    kernel: *kernel_mod.ExchangeKernel,
    metadata: DurableSnapshotMetadata,
) DurableSnapshotError!PayloadDigest {
    var snapshot = try KernelSnapshot.captureFromKernel(allocator, kernel, &.{}, 1);
    defer snapshot.deinit();
    var pending = try kernel.binary_frame_assembler.capturePendingState(allocator);
    defer pending.deinit();

    var atomic_file = try dir.createFileAtomic(io_context.current(), path, .{
        .replace = true,
    });
    defer atomic_file.deinit(io_context.current());
    const file = atomic_file.file;
    var buffer: [4096]u8 = undefined;
    var file_writer = file.writer(io_context.current(), &buffer);
    const writer = &file_writer.interface;

    try writer.writeAll(MATCHING_MAGIC_V1);
    try writeDurableMetadata(@TypeOf(writer), writer, MATCHING_FORMAT_VERSION, metadata);
    try writeInt(u32, @TypeOf(writer), writer, try castLenU32(snapshot.books.items.len));
    for (snapshot.books.items) |book| {
        try writeSymbolSpec(@TypeOf(writer), writer, book.symbol_spec);
        try writeInt(u32, @TypeOf(writer), writer, try castLenU32(book.orders.items.len));
        for (book.orders.items) |order| try writeRestingOrder(@TypeOf(writer), writer, order);
    }
    try writeInt(u32, @TypeOf(writer), writer, try castLenU32(pending.transfers.len));
    for (pending.transfers) |transfer| {
        try writeInt(i32, @TypeOf(writer), writer, transfer.transfer_id);
        try writeInt(
            i8,
            @TypeOf(writer),
            writer,
            binary_frames.pendingCommandTypeCode(transfer.command_type),
        );
        try writeInt(
            u32,
            @TypeOf(writer),
            writer,
            transfer.java_backing_capacity orelse 0,
        );
        try writeInt(u32, @TypeOf(writer), writer, try castLenU32(transfer.words.len));
        for (transfer.words) |word| try writeInt(i64, @TypeOf(writer), writer, word);
    }
    try writer.flush();
    try file.sync(io_context.current());
    try atomic_file.replace(io_context.current());
    try syncParentDirectory(dir, path);
    return digestFile(allocator, dir, path);
}

/// Writes a risk-engine-only payload. Symbol specifications are duplicated in
/// this half deliberately so risk recovery can validate positions without
/// interpreting matching-engine order data.
pub fn writeRiskPayloadAtomic(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    path: []const u8,
    kernel: *kernel_mod.ExchangeKernel,
    metadata: DurableSnapshotMetadata,
) DurableSnapshotError!PayloadDigest {
    var snapshot = try KernelSnapshot.captureFromKernel(allocator, kernel, &.{}, 1);
    defer snapshot.deinit();

    var atomic_file = try dir.createFileAtomic(io_context.current(), path, .{
        .replace = true,
    });
    defer atomic_file.deinit(io_context.current());
    const file = atomic_file.file;
    var buffer: [4096]u8 = undefined;
    var file_writer = file.writer(io_context.current(), &buffer);
    const writer = &file_writer.interface;

    try writer.writeAll(RISK_MAGIC_V1);
    try writeDurableMetadata(@TypeOf(writer), writer, DURABLE_FORMAT_VERSION_V1, metadata);
    try writeInt(u64, @TypeOf(writer), writer, snapshot.risk_state_hash);
    try writeInt(u32, @TypeOf(writer), writer, try castLenU32(snapshot.books.items.len));
    for (snapshot.books.items) |book| try writeSymbolSpec(@TypeOf(writer), writer, book.symbol_spec);

    try writeInt(u32, @TypeOf(writer), writer, try castLenU32(snapshot.users.items.len));
    for (snapshot.users.items) |snapshot_user| {
        try writeInt(i64, @TypeOf(writer), writer, snapshot_user.uid);
        try writeInt(i8, @TypeOf(writer), writer, snapshot_user.user_status.code());
        try writeInt(i64, @TypeOf(writer), writer, snapshot_user.adjustments_counter);
        try writeInt(u32, @TypeOf(writer), writer, try castLenU32(snapshot_user.accounts.items.len));
        for (snapshot_user.accounts.items) |entry| try writeI32I64Entry(@TypeOf(writer), writer, entry);
        try writeInt(u32, @TypeOf(writer), writer, try castLenU32(snapshot_user.positions.items.len));
        for (snapshot_user.positions.items) |entry| try writePositionSnapshot(@TypeOf(writer), writer, entry);
    }

    try writeInt(u32, @TypeOf(writer), writer, try castLenU32(snapshot.last_price_cache.items.len));
    for (snapshot.last_price_cache.items) |entry| {
        try writeInt(i32, @TypeOf(writer), writer, entry.symbol);
        try writeInt(i64, @TypeOf(writer), writer, entry.ask_price);
        try writeInt(i64, @TypeOf(writer), writer, entry.bid_price);
    }
    try writeEntryList(@TypeOf(writer), writer, snapshot.fees.items);
    try writeEntryList(@TypeOf(writer), writer, snapshot.adjustments.items);
    try writeEntryList(@TypeOf(writer), writer, snapshot.suspends.items);
    try writer.flush();
    try file.sync(io_context.current());
    try atomic_file.replace(io_context.current());
    try syncParentDirectory(dir, path);
    return digestFile(allocator, dir, path);
}

/// The manifest link is the single commit point for an ME/RE pair. It is
/// deliberately no-replace: once XCM exists, the committed snapshot ID cannot
/// be reused even though pre-commit XME/XRE orphan payloads are replaceable.
/// Callers must not register an in-memory descriptor until this succeeds.
pub fn writeManifestAtomic(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    path: []const u8,
    manifest: SnapshotPairManifest,
) DurableSnapshotIoError!void {
    _ = allocator;
    var atomic_file = try dir.createFileAtomic(io_context.current(), path, .{
        .replace = false,
    });
    defer atomic_file.deinit(io_context.current());
    const file = atomic_file.file;
    var buffer: [256]u8 = undefined;
    var file_writer = file.writer(io_context.current(), &buffer);
    const writer = &file_writer.interface;
    try writer.writeAll(MANIFEST_MAGIC_V1);
    try writeInt(u16, @TypeOf(writer), writer, DURABLE_FORMAT_VERSION_V1);
    try writeInt(u64, @TypeOf(writer), writer, manifest.snapshot_id);
    try writeInt(u64, @TypeOf(writer), writer, manifest.matching_seq);
    try writeInt(u64, @TypeOf(writer), writer, manifest.risk_seq);
    try writeInt(u64, @TypeOf(writer), writer, manifest.next_seq);
    try writeInt(u64, @TypeOf(writer), writer, manifest.timestamp_ns);
    try writeInt(u32, @TypeOf(writer), writer, manifest.matching_shards);
    try writeInt(u32, @TypeOf(writer), writer, manifest.risk_shards);
    try writeDigest(@TypeOf(writer), writer, manifest.matching);
    try writeDigest(@TypeOf(writer), writer, manifest.risk);
    try writer.flush();
    try file.sync(io_context.current());
    try atomic_file.link(io_context.current());
    try syncParentDirectory(dir, path);
}

pub fn readManifest(
    dir: std.Io.Dir,
    path: []const u8,
) DurableSnapshotIoError!SnapshotPairManifest {
    const file = try dir.openFile(io_context.current(), path, .{});
    defer file.close(io_context.current());
    var buffer: [256]u8 = undefined;
    var file_reader = file.reader(io_context.current(), &buffer);
    const reader = &file_reader.interface;
    const magic = try reader.takeArray(4);
    if (!std.mem.eql(u8, magic, MANIFEST_MAGIC_V1)) return error.InvalidMagic;
    if (try readInt(u16, @TypeOf(reader), reader) != DURABLE_FORMAT_VERSION_V1) return error.InvalidFormat;
    return .{
        .snapshot_id = try readInt(u64, @TypeOf(reader), reader),
        .matching_seq = try readInt(u64, @TypeOf(reader), reader),
        .risk_seq = try readInt(u64, @TypeOf(reader), reader),
        .next_seq = try readInt(u64, @TypeOf(reader), reader),
        .timestamp_ns = try readInt(u64, @TypeOf(reader), reader),
        .matching_shards = try readInt(u32, @TypeOf(reader), reader),
        .risk_shards = try readInt(u32, @TypeOf(reader), reader),
        .matching = try readDigest(@TypeOf(reader), reader),
        .risk = try readDigest(@TypeOf(reader), reader),
    };
}

pub fn readMatchingPayloadSummary(
    dir: std.Io.Dir,
    path: []const u8,
) SnapshotIoError!MatchingPayloadSummary {
    const file = try dir.openFile(io_context.current(), path, .{});
    defer file.close(io_context.current());
    var buffer: [4096]u8 = undefined;
    var file_reader = file.reader(io_context.current(), &buffer);
    const reader = &file_reader.interface;
    return readMatchingPayloadSummaryFromReader(@TypeOf(reader), reader);
}

fn readMatchingPayloadSummaryFromReader(
    comptime Reader: type,
    reader: Reader,
) SnapshotIoError!MatchingPayloadSummary {
    const magic = try reader.takeArray(4);
    if (!std.mem.eql(u8, magic, MATCHING_MAGIC_V1)) return error.InvalidMagic;
    const header = try readDurableMetadata(Reader, reader);
    if (!isSupportedMatchingFormat(header.format_version)) return error.InvalidFormat;
    const book_count = try readInt(u32, Reader, reader);
    var book_i: u32 = 0;
    while (book_i < book_count) : (book_i += 1) {
        _ = try readSymbolSpec(Reader, reader);
        const order_count = try readInt(u32, Reader, reader);
        var order_i: u32 = 0;
        while (order_i < order_count) : (order_i += 1) _ = try readRestingOrder(Reader, reader);
    }
    const pending_transfer_count = try readInt(u32, Reader, reader);
    var transfer_i: u32 = 0;
    while (transfer_i < pending_transfer_count) : (transfer_i += 1) {
        _ = try readInt(i32, Reader, reader);
        _ = binary_frames.pendingCommandTypeFromCode(
            try readInt(i8, Reader, reader),
        ) catch return error.InvalidFormat;
        const raw_capacity = if (header.format_version >= MATCHING_FORMAT_VERSION)
            try readInt(u32, Reader, reader)
        else
            0;
        const word_count = try readInt(u32, Reader, reader);
        if (word_count == 0 or word_count % binary_frames.FrameWordsPerCommand != 0) return error.InvalidFormat;
        var word_i: u32 = 0;
        var first_word: i64 = undefined;
        while (word_i < word_count) : (word_i += 1) {
            const word = try readInt(i64, Reader, reader);
            if (word_i == 0) first_word = word;
        }
        if (raw_capacity != 0 and
            binary_frames.expectedJavaBackingCapacity(first_word, word_count) != raw_capacity)
        {
            return error.InvalidFormat;
        }
    }
    return .{
        .metadata = header.metadata,
        .book_count = book_count,
        .pending_transfer_count = pending_transfer_count,
    };
}

pub fn readRiskPayloadSummary(
    dir: std.Io.Dir,
    path: []const u8,
) SnapshotIoError!RiskPayloadSummary {
    const file = try dir.openFile(io_context.current(), path, .{});
    defer file.close(io_context.current());
    var buffer: [4096]u8 = undefined;
    var file_reader = file.reader(io_context.current(), &buffer);
    const reader = &file_reader.interface;
    return readRiskPayloadSummaryFromReader(@TypeOf(reader), reader);
}

fn readRiskPayloadSummaryFromReader(
    comptime Reader: type,
    reader: Reader,
) SnapshotIoError!RiskPayloadSummary {
    const magic = try reader.takeArray(4);
    if (!std.mem.eql(u8, magic, RISK_MAGIC_V1)) return error.InvalidMagic;
    const header = try readDurableMetadata(Reader, reader);
    if (header.format_version != DURABLE_FORMAT_VERSION_V1) return error.InvalidFormat;
    _ = try readInt(u64, Reader, reader);
    const symbol_count = try readInt(u32, Reader, reader);
    var symbol_i: u32 = 0;
    while (symbol_i < symbol_count) : (symbol_i += 1) _ = try readSymbolSpec(Reader, reader);
    const user_count = try readInt(u32, Reader, reader);
    var user_i: u32 = 0;
    while (user_i < user_count) : (user_i += 1) {
        _ = try readInt(i64, Reader, reader);
        _ = common.UserStatus.fromCode(try readInt(i8, Reader, reader)) catch return error.InvalidFormat;
        _ = try readInt(i64, Reader, reader);
        try skipI32I64Entries(Reader, reader);
        const position_count = try readInt(u32, Reader, reader);
        var position_i: u32 = 0;
        while (position_i < position_count) : (position_i += 1) _ = try readPositionSnapshot(Reader, reader);
    }
    const price_count = try readInt(u32, Reader, reader);
    var price_i: u32 = 0;
    while (price_i < price_count) : (price_i += 1) {
        _ = try readInt(i32, Reader, reader);
        _ = try readInt(i64, Reader, reader);
        _ = try readInt(i64, Reader, reader);
    }
    try skipI32I64Entries(Reader, reader);
    try skipI32I64Entries(Reader, reader);
    try skipI32I64Entries(Reader, reader);
    return .{
        .metadata = header.metadata,
        .symbol_count = symbol_count,
        .user_count = user_count,
    };
}

pub fn validateCommittedPair(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    manifest_path: []const u8,
    matching_path: []const u8,
    risk_path: []const u8,
) DurableSnapshotIoError!SnapshotPairManifest {
    const manifest = try readManifest(dir, manifest_path);
    try validateRequestedPaths(manifest, manifest_path, matching_path, risk_path);
    var matching_payload = try loadPayload(allocator, dir, matching_path);
    defer matching_payload.deinit();
    var risk_payload = try loadPayload(allocator, dir, risk_path);
    defer risk_payload.deinit();
    try validateLoadedPayloadDigests(manifest, matching_payload.digest, risk_payload.digest);

    var matching_reader = std.Io.Reader.fixed(matching_payload.bytes);
    const matching = try readMatchingPayloadSummaryFromReader(*std.Io.Reader, &matching_reader);
    var risk_reader = std.Io.Reader.fixed(risk_payload.bytes);
    const risk_state = try readRiskPayloadSummaryFromReader(*std.Io.Reader, &risk_reader);
    try validatePairMetadata(manifest, matching.metadata, risk_state.metadata);
    return manifest;
}

/// Restores only a fully committed and digest-validated pair. The manifest is
/// read first, so orphan ME/RE files left by a failed attempt are invisible.
pub fn restoreCommittedPairIntoKernel(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    manifest_path: []const u8,
    matching_path: []const u8,
    risk_path: []const u8,
    kernel: *kernel_mod.ExchangeKernel,
) DurableSnapshotRestoreError!SnapshotPairManifest {
    try rejectNonEmptyCustomReportRegistry(kernel);

    const manifest = try readManifest(dir, manifest_path);
    try validateRequestedPaths(manifest, manifest_path, matching_path, risk_path);
    var matching_payload = try loadPayload(allocator, dir, matching_path);
    defer matching_payload.deinit();
    var risk_payload = try loadPayload(allocator, dir, risk_path);
    defer risk_payload.deinit();
    try validateLoadedPayloadDigests(manifest, matching_payload.digest, risk_payload.digest);

    var snapshot: KernelSnapshot = .{
        .allocator = allocator,
        .depth = 1,
        .risk_state_hash = 0,
    };
    defer snapshot.deinit();
    var pending: binary_frames.PendingState = .{
        .allocator = allocator,
        .transfers = try allocator.alloc(binary_frames.PendingTransfer, 0),
    };
    defer pending.deinit();
    var matching_metadata: DurableSnapshotMetadata = undefined;
    var risk_metadata: DurableSnapshotMetadata = undefined;

    {
        var fixed_reader = std.Io.Reader.fixed(matching_payload.bytes);
        const reader = &fixed_reader;
        const magic = try reader.takeArray(4);
        if (!std.mem.eql(u8, magic, MATCHING_MAGIC_V1)) return error.InvalidMagic;
        const matching_header = try readDurableMetadata(@TypeOf(reader), reader);
        if (!isSupportedMatchingFormat(matching_header.format_version)) return error.InvalidFormat;
        matching_metadata = matching_header.metadata;

        const book_count = try readCount(@TypeOf(reader), reader);
        try snapshot.books.ensureTotalCapacityPrecise(allocator, book_count);
        var book_i: usize = 0;
        while (book_i < book_count) : (book_i += 1) {
            var book: BookSnapshot = .{ .symbol_spec = try readSymbolSpec(@TypeOf(reader), reader) };
            errdefer book.deinit(allocator);
            const order_count = try readCount(@TypeOf(reader), reader);
            try book.orders.ensureTotalCapacityPrecise(allocator, order_count);
            var order_i: usize = 0;
            while (order_i < order_count) : (order_i += 1) {
                book.orders.appendAssumeCapacity(try readRestingOrder(@TypeOf(reader), reader));
            }
            snapshot.books.appendAssumeCapacity(book);
        }

        const transfer_count = try readCount(@TypeOf(reader), reader);
        const restored_transfers = try allocator.alloc(binary_frames.PendingTransfer, transfer_count);
        allocator.free(pending.transfers);
        pending.transfers = restored_transfers;
        var initialized: usize = 0;
        errdefer {
            for (pending.transfers[0..initialized]) |*transfer| transfer.deinit(allocator);
            allocator.free(pending.transfers);
            pending.transfers = &.{};
        }
        while (initialized < transfer_count) : (initialized += 1) {
            const transfer_id = try readInt(i32, @TypeOf(reader), reader);
            const command_type = binary_frames.pendingCommandTypeFromCode(
                try readInt(i8, @TypeOf(reader), reader),
            ) catch return error.InvalidFormat;
            const raw_capacity = if (matching_header.format_version >= MATCHING_FORMAT_VERSION)
                try readInt(u32, @TypeOf(reader), reader)
            else
                0;
            const word_count = try readCount(@TypeOf(reader), reader);
            if (word_count == 0 or word_count % binary_frames.FrameWordsPerCommand != 0) {
                return error.InvalidFormat;
            }
            const words = try allocator.alloc(i64, word_count);
            errdefer allocator.free(words);
            for (words) |*word| word.* = try readInt(i64, @TypeOf(reader), reader);
            pending.transfers[initialized] = .{
                .transfer_id = transfer_id,
                .command_type = command_type,
                .java_backing_capacity = if (raw_capacity == 0) null else raw_capacity,
                .words = words,
            };
        }
    }

    {
        var fixed_reader = std.Io.Reader.fixed(risk_payload.bytes);
        const reader = &fixed_reader;
        const magic = try reader.takeArray(4);
        if (!std.mem.eql(u8, magic, RISK_MAGIC_V1)) return error.InvalidMagic;
        const risk_header = try readDurableMetadata(@TypeOf(reader), reader);
        if (risk_header.format_version != DURABLE_FORMAT_VERSION_V1) return error.InvalidFormat;
        risk_metadata = risk_header.metadata;
        snapshot.risk_state_hash = try readInt(u64, @TypeOf(reader), reader);

        const symbol_count = try readCount(@TypeOf(reader), reader);
        if (symbol_count != snapshot.books.items.len) return error.SnapshotMetadataMismatch;
        var symbol_i: usize = 0;
        while (symbol_i < symbol_count) : (symbol_i += 1) {
            const spec = try readSymbolSpec(@TypeOf(reader), reader);
            if (!std.meta.eql(spec, snapshot.books.items[symbol_i].symbol_spec)) {
                return error.SnapshotMetadataMismatch;
            }
        }

        const user_count = try readCount(@TypeOf(reader), reader);
        try snapshot.users.ensureTotalCapacityPrecise(allocator, user_count);
        var user_i: usize = 0;
        while (user_i < user_count) : (user_i += 1) {
            var snapshot_user: UserSnapshot = .{
                .uid = try readInt(i64, @TypeOf(reader), reader),
                .user_status = common.UserStatus.fromCode(
                    try readInt(i8, @TypeOf(reader), reader),
                ) catch return error.InvalidFormat,
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

        const price_count = try readCount(@TypeOf(reader), reader);
        try snapshot.last_price_cache.ensureTotalCapacityPrecise(allocator, price_count);
        var price_i: usize = 0;
        while (price_i < price_count) : (price_i += 1) {
            snapshot.last_price_cache.appendAssumeCapacity(.{
                .symbol = try readInt(i32, @TypeOf(reader), reader),
                .ask_price = try readInt(i64, @TypeOf(reader), reader),
                .bid_price = try readInt(i64, @TypeOf(reader), reader),
            });
        }
        try readEntryList(allocator, @TypeOf(reader), reader, &snapshot.fees);
        try readEntryList(allocator, @TypeOf(reader), reader, &snapshot.adjustments);
        try readEntryList(allocator, @TypeOf(reader), reader, &snapshot.suspends);
    }

    // These metadata fields and the digests above came from the same owned
    // byte images used to construct the replacement kernel. Renaming or
    // rewriting a payload after `loadPayload` cannot change recovered state.
    try validatePairMetadata(manifest, matching_metadata, risk_metadata);

    var replacement = try initKernelLike(kernel);
    defer replacement.deinit();
    try snapshot.restoreIntoKernel(&replacement);
    try replacement.binary_frame_assembler.restorePendingState(pending.transfers);
    replacement.next_seq = manifest.next_seq;

    // Keep the caller-visible kernel address stable. After the swap,
    // `replacement` owns the former live state and the defer destroys it.
    std.mem.swap(kernel_mod.ExchangeKernel, kernel, &replacement);
    return manifest;
}

fn initKernelLike(kernel: *const kernel_mod.ExchangeKernel) kernel_mod.InitError!kernel_mod.ExchangeKernel {
    const num_shards = std.math.add(u64, kernel.risk_engine.shard_mask, 1) catch
        return error.InvalidNumShards;
    return kernel_mod.ExchangeKernel.init(kernel.allocator, .{
        .orderbook_kind = kernel.orderbook_kind,
        .shard_id = kernel.risk_engine.shard_id,
        .num_shards = num_shards,
        .send_l2_for_every_cmd = kernel.send_l2_for_every_cmd,
        .l2_refresh_depth = kernel.l2_refresh_depth,
        .risk = .{
            .ignore_risk_processing = kernel.risk_engine.cfg_ignore_risk_processing,
            .margin_trading_enabled = kernel.risk_engine.cfg_margin_trading_enabled,
        },
        .report_queries = kernel.report_queries_cfg,
    });
}

fn validateLoadedPayloadDigests(
    manifest: SnapshotPairManifest,
    matching: PayloadDigest,
    risk_state: PayloadDigest,
) error{PayloadDigestMismatch}!void {
    if (!std.meta.eql(manifest.matching, matching)) {
        return error.PayloadDigestMismatch;
    }
    if (!std.meta.eql(manifest.risk, risk_state)) {
        return error.PayloadDigestMismatch;
    }
}

fn validatePairMetadata(
    manifest: SnapshotPairManifest,
    matching: DurableSnapshotMetadata,
    risk_state: DurableSnapshotMetadata,
) error{SnapshotMetadataMismatch}!void {
    if (manifest.matching_shards == 0 or manifest.risk_shards == 0 or
        matching.shard_count == 0 or risk_state.shard_count == 0)
    {
        return error.SnapshotMetadataMismatch;
    }

    // XCM1 currently persists exactly one ME and one RE image. Accepting a
    // larger topology here would silently restore only shard zero.
    if (manifest.matching_shards != 1 or manifest.risk_shards != 1 or
        matching.shard_id != 0 or matching.shard_count != 1 or
        risk_state.shard_id != 0 or risk_state.shard_count != 1)
    {
        return error.SnapshotMetadataMismatch;
    }

    const expected_risk_seq = std.math.add(u64, manifest.matching_seq, 1) catch
        return error.SnapshotMetadataMismatch;
    if (manifest.risk_seq != expected_risk_seq or manifest.next_seq != manifest.risk_seq) {
        return error.SnapshotMetadataMismatch;
    }

    if (matching.snapshot_id != manifest.snapshot_id or
        matching.command_seq != manifest.matching_seq or
        matching.next_seq != manifest.matching_seq or
        matching.timestamp_ns != manifest.timestamp_ns or
        matching.shard_count != manifest.matching_shards or
        risk_state.snapshot_id != manifest.snapshot_id or
        risk_state.command_seq != manifest.risk_seq or
        risk_state.next_seq != manifest.risk_seq or
        risk_state.timestamp_ns != manifest.timestamp_ns or
        risk_state.shard_count != manifest.risk_shards)
    {
        return error.SnapshotMetadataMismatch;
    }
}

const SnapshotPathIdentity = struct {
    exchange_id: []const u8,
    snapshot_id: u64,
};

fn parseSnapshotPathIdentity(path: []const u8, suffix: []const u8) ?SnapshotPathIdentity {
    const basename = std.fs.path.basename(path);
    if (!std.mem.endsWith(u8, basename, suffix)) return null;
    const prefix_and_id = basename[0 .. basename.len - suffix.len];
    const marker = "_snapshot_";
    const marker_index = std.mem.lastIndexOf(u8, prefix_and_id, marker) orelse return null;
    const exchange_id = prefix_and_id[0..marker_index];
    const id_text = prefix_and_id[marker_index + marker.len ..];
    if (exchange_id.len == 0 or id_text.len == 0) return null;
    const snapshot_id = std.fmt.parseUnsigned(u64, id_text, 10) catch return null;
    return .{ .exchange_id = exchange_id, .snapshot_id = snapshot_id };
}

fn validateRequestedPaths(
    manifest: SnapshotPairManifest,
    manifest_path: []const u8,
    matching_path: []const u8,
    risk_path: []const u8,
) error{SnapshotMetadataMismatch}!void {
    const requested = parseSnapshotPathIdentity(manifest_path, ".ecm");
    const matching = parseSnapshotPathIdentity(matching_path, "_ME0.ecs");
    const risk_state = parseSnapshotPathIdentity(risk_path, "_RE0.ecs");
    const recognized = @as(u2, @intFromBool(requested != null)) +
        @as(u2, @intFromBool(matching != null)) +
        @as(u2, @intFromBool(risk_state != null));
    if (recognized == 0) return;
    if (recognized != 3) return error.SnapshotMetadataMismatch;

    const requested_identity = requested.?;
    const matching_identity = matching.?;
    const risk_identity = risk_state.?;
    if (requested_identity.snapshot_id != manifest.snapshot_id or
        matching_identity.snapshot_id != manifest.snapshot_id or
        risk_identity.snapshot_id != manifest.snapshot_id or
        !std.mem.eql(u8, requested_identity.exchange_id, matching_identity.exchange_id) or
        !std.mem.eql(u8, requested_identity.exchange_id, risk_identity.exchange_id))
    {
        return error.SnapshotMetadataMismatch;
    }
}

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

fn writeDurableMetadata(
    comptime Writer: type,
    writer: Writer,
    format_version: u16,
    metadata: DurableSnapshotMetadata,
) !void {
    try writeInt(u16, Writer, writer, format_version);
    try writeInt(u64, Writer, writer, metadata.snapshot_id);
    try writeInt(u64, Writer, writer, metadata.command_seq);
    try writeInt(u64, Writer, writer, metadata.next_seq);
    try writeInt(u64, Writer, writer, metadata.timestamp_ns);
    try writeInt(u32, Writer, writer, metadata.shard_id);
    try writeInt(u32, Writer, writer, metadata.shard_count);
}

fn readDurableMetadata(
    comptime Reader: type,
    reader: Reader,
) SnapshotIoError!DurablePayloadHeader {
    return .{
        .format_version = try readInt(u16, Reader, reader),
        .metadata = .{
            .snapshot_id = try readInt(u64, Reader, reader),
            .command_seq = try readInt(u64, Reader, reader),
            .next_seq = try readInt(u64, Reader, reader),
            .timestamp_ns = try readInt(u64, Reader, reader),
            .shard_id = try readInt(u32, Reader, reader),
            .shard_count = try readInt(u32, Reader, reader),
        },
    };
}

fn isSupportedMatchingFormat(format_version: u16) bool {
    return format_version == DURABLE_FORMAT_VERSION_V1 or
        format_version == MATCHING_FORMAT_VERSION;
}

fn skipI32I64Entries(comptime Reader: type, reader: Reader) SnapshotIoError!void {
    const count = try readInt(u32, Reader, reader);
    var i: u32 = 0;
    while (i < count) : (i += 1) _ = try readI32I64Entry(Reader, reader);
}

fn readEntryList(
    allocator: std.mem.Allocator,
    comptime Reader: type,
    reader: Reader,
    entries: *std.ArrayListUnmanaged(I32I64Entry),
) SnapshotIoError!void {
    const count = try readCount(Reader, reader);
    try entries.ensureTotalCapacityPrecise(allocator, count);
    var i: usize = 0;
    while (i < count) : (i += 1) entries.appendAssumeCapacity(try readI32I64Entry(Reader, reader));
}

fn writeEntryList(
    comptime Writer: type,
    writer: Writer,
    entries: []const I32I64Entry,
) !void {
    try writeInt(u32, Writer, writer, try castLenU32(entries.len));
    for (entries) |entry| try writeI32I64Entry(Writer, writer, entry);
}

fn writeDigest(comptime Writer: type, writer: Writer, digest: PayloadDigest) !void {
    try writeInt(u64, Writer, writer, digest.byte_len);
    try writeInt(u64, Writer, writer, digest.hash);
}

fn readDigest(comptime Reader: type, reader: Reader) SnapshotIoError!PayloadDigest {
    return .{
        .byte_len = try readInt(u64, Reader, reader),
        .hash = try readInt(u64, Reader, reader),
    };
}

const LoadedPayload = struct {
    allocator: std.mem.Allocator,
    bytes: []u8,
    digest: PayloadDigest,

    fn deinit(self: *LoadedPayload) void {
        self.allocator.free(self.bytes);
        self.* = undefined;
    }
};

fn loadPayload(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    path: []const u8,
) DurableSnapshotIoError!LoadedPayload {
    const file = try dir.openFile(io_context.current(), path, .{});
    defer file.close(io_context.current());
    const stat = try file.stat(io_context.current());
    const byte_len = stat.size;
    const max_len = std.math.cast(usize, byte_len) orelse return error.SnapshotPayloadTooLarge;
    const read_limit = std.math.add(usize, max_len, 1) catch return error.SnapshotPayloadTooLarge;
    const bytes = try io_context.readToEndAlloc(file, allocator, read_limit);
    errdefer allocator.free(bytes);
    if (bytes.len != max_len) return error.InvalidFormat;
    return .{
        .allocator = allocator,
        .bytes = bytes,
        .digest = .{
            .byte_len = byte_len,
            .hash = std.hash.Wyhash.hash(0, bytes),
        },
    };
}

fn digestFile(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    path: []const u8,
) DurableSnapshotIoError!PayloadDigest {
    var payload = try loadPayload(allocator, dir, path);
    defer payload.deinit();
    return payload.digest;
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

pub fn writeI32I64Entry(
    comptime Writer: type,
    writer: Writer,
    entry: I32I64Entry,
) !void {
    try writeInt(i32, Writer, writer, entry.key);
    try writeInt(i64, Writer, writer, entry.value);
}

pub fn readI32I64Entry(comptime Reader: type, reader: Reader) SnapshotIoError!I32I64Entry {
    return .{
        .key = try readInt(i32, Reader, reader),
        .value = try readInt(i64, Reader, reader),
    };
}

pub fn writeSymbolSpec(
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

pub fn readSymbolSpec(comptime Reader: type, reader: Reader) SnapshotIoError!common.CoreSymbolSpecification {
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

pub fn writeRestingOrder(
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

pub fn readRestingOrder(
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

pub fn writePositionSnapshot(
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

pub fn readPositionSnapshot(
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

const SnapshotCompatibilityCustomReportSpec = struct {
    pub const code: i32 = 70_451;
    pub const schema_id: [report_registry.schema_id_len]u8 =
        [_]u8{0x73} ** report_registry.schema_id_len;
    pub const schema_version: u32 = 1;
    pub const Query = u8;
    pub const Section = u8;
    pub const Result = u8;
    pub const MatchingView = kernel_mod.MatchingReportView;
    pub const RiskView = kernel_mod.RiskReportView;

    pub fn cloneQuery(
        _: std.mem.Allocator,
        query: *const Query,
    ) report_registry.ResourceError!Query {
        return query.*;
    }

    pub fn deinitQuery(_: std.mem.Allocator, query: *Query) void {
        query.* = undefined;
    }

    pub fn encodeQuery(
        _: std.mem.Allocator,
        writer: *report_registry.EncodingWriter,
        query: *const Query,
    ) report_registry.EncodingError!void {
        try writer.writeByte(query.*);
    }

    pub fn decodeQuery(
        _: std.mem.Allocator,
        body: []const u8,
    ) report_registry.QueryConstructionError!Query {
        if (body.len != 1) return error.InvalidQuery;
        return body[0];
    }

    pub fn processMatching(
        _: std.mem.Allocator,
        _: *const Query,
        _: report_registry.MatchingContext(MatchingView),
    ) report_registry.HandlerError!?Section {
        return null;
    }

    pub fn processRisk(
        _: std.mem.Allocator,
        _: *const Query,
        _: report_registry.RiskContext(RiskView),
    ) report_registry.HandlerError!?Section {
        return null;
    }

    pub fn encodeSection(
        _: std.mem.Allocator,
        writer: *report_registry.EncodingWriter,
        section: *const Section,
    ) report_registry.EncodingError!void {
        try writer.writeByte(section.*);
    }

    pub fn deinitSection(_: std.mem.Allocator, section: *Section) void {
        section.* = undefined;
    }

    pub fn reduce(
        _: std.mem.Allocator,
        _: *const Query,
        sections: []const report_registry.SectionBytes,
    ) report_registry.ReductionError!Result {
        return std.math.cast(u8, sections.len) orelse error.ReducerFailed;
    }

    pub fn deinitResult(_: std.mem.Allocator, result: *Result) void {
        result.* = undefined;
    }
};

fn findOrder(
    orders: []const naive.UserOrderView,
    order_id: i64,
) ?naive.UserOrderView {
    for (orders) |order| {
        if (order.order_id == order_id) return order;
    }
    return null;
}

fn expectLossySnapshotHistoriesUnavailable(
    kernel: *kernel_mod.ExchangeKernel,
) !void {
    try std.testing.expectError(
        error.HistoryUnavailable,
        kernel.matchingBookKeysInHistoryOrder(kernel.allocator),
    );
    try std.testing.expectError(
        error.HistoryUnavailable,
        kernel.risk_engine.user_profile_service.userProfileKeysInHistoryOrder(),
    );

    var profiles = kernel.risk_engine.user_profile_service.user_profiles.valueIterator();
    while (profiles.next()) |profile| {
        try std.testing.expectError(
            error.HistoryUnavailable,
            profile.accountKeysInHistoryOrder(kernel.allocator),
        );
        try std.testing.expectError(
            error.HistoryUnavailable,
            profile.positionKeysInHistoryOrder(kernel.allocator),
        );
    }

    try std.testing.expectError(
        error.HistoryUnavailable,
        kernel.risk_engine.feeKeysInHistoryOrder(),
    );
    try std.testing.expectError(
        error.HistoryUnavailable,
        kernel.risk_engine.adjustmentKeysInHistoryOrder(),
    );
    try std.testing.expectError(
        error.HistoryUnavailable,
        kernel.risk_engine.suspendKeysInHistoryOrder(),
    );
}

fn writeTestPairPayloads(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    matching_path: []const u8,
    risk_path: []const u8,
    kernel: *kernel_mod.ExchangeKernel,
    snapshot_id: u64,
    matching_seq: u64,
    timestamp_ns: u64,
) !SnapshotPairManifest {
    const risk_seq = try std.math.add(u64, matching_seq, 1);
    const matching_digest = try writeMatchingPayloadAtomic(
        allocator,
        dir,
        matching_path,
        kernel,
        .{
            .snapshot_id = snapshot_id,
            .command_seq = matching_seq,
            .next_seq = matching_seq,
            .timestamp_ns = timestamp_ns,
            .shard_id = 0,
            .shard_count = 1,
        },
    );
    const risk_digest = try writeRiskPayloadAtomic(
        allocator,
        dir,
        risk_path,
        kernel,
        .{
            .snapshot_id = snapshot_id,
            .command_seq = risk_seq,
            .next_seq = risk_seq,
            .timestamp_ns = timestamp_ns,
            .shard_id = 0,
            .shard_count = 1,
        },
    );
    return .{
        .snapshot_id = snapshot_id,
        .matching_seq = matching_seq,
        .risk_seq = risk_seq,
        .next_seq = risk_seq,
        .timestamp_ns = timestamp_ns,
        .matching_shards = 1,
        .risk_shards = 1,
        .matching = matching_digest,
        .risk = risk_digest,
    };
}

fn readTestFileBytes(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    path: []const u8,
) (std.Io.File.OpenError || io_context.ReadToEndAllocError)![]u8 {
    const file = try dir.openFile(io_context.current(), path, .{});
    defer file.close(io_context.current());
    return io_context.readToEndAlloc(file, allocator, 16 * 1024 * 1024);
}

fn writeTestFileBytes(dir: std.Io.Dir, path: []const u8, bytes: []const u8) !void {
    const file = try dir.createFile(io_context.current(), path, .{ .truncate = true });
    defer file.close(io_context.current());
    try file.writeStreamingAll(io_context.current(), bytes);
}

fn addJavaCapacityPendingTransfer(kernel: *kernel_mod.ExchangeKernel) !void {
    const frame: cmd.OrderCommand = .{
        .command = .binary_data_query,
        .order_id = (@as(i64, 41) << 32) | 0x1122_3344,
        .price = -2,
        .reserve_bid_price = std.math.maxInt(i64),
        .size = std.math.minInt(i64),
        .uid = 99,
        .symbol = 0,
        .user_cookie = -11,
    };
    try std.testing.expectEqual(binary_frames.AcceptResult.accepted, try kernel.binary_frame_assembler.acceptFrame(&frame));
}

const single_pending_capacity_offset = 4 + @sizeOf(u16) +
    4 * @sizeOf(u64) + 2 * @sizeOf(u32) +
    @sizeOf(u32) + @sizeOf(u32) + @sizeOf(i32) + @sizeOf(i8);

fn downgradeSinglePendingMatchingPayloadToV1(
    allocator: std.mem.Allocator,
    current: []const u8,
) ![]u8 {
    std.debug.assert(std.mem.eql(u8, current[0..4], MATCHING_MAGIC_V1));
    std.debug.assert(std.mem.readInt(u16, current[4..6], .little) == MATCHING_FORMAT_VERSION);
    const legacy = try allocator.alloc(u8, current.len - @sizeOf(u32));
    @memcpy(legacy[0..single_pending_capacity_offset], current[0..single_pending_capacity_offset]);
    @memcpy(
        legacy[single_pending_capacity_offset..],
        current[single_pending_capacity_offset + @sizeOf(u32) ..],
    );
    std.mem.writeInt(u16, legacy[4..6], DURABLE_FORMAT_VERSION_V1, .little);
    return legacy;
}

fn durableRestoreAllocationCampaign(
    allocator: std.mem.Allocator,
    dir: std.Io.Dir,
    manifest_path: []const u8,
    matching_path: []const u8,
    risk_path: []const u8,
) !void {
    var target = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer target.deinit();
    const live_frame: cmd.OrderCommand = .{
        .command = .binary_data_command,
        .order_id = 1,
        .price = 2,
        .reserve_bid_price = 3,
        .size = 4,
        .uid = 5,
        .symbol = 0,
        .user_cookie = 900,
    };
    _ = try target.binary_frame_assembler.acceptFrame(&live_frame);
    target.next_seq = 77;
    const before_hash = try target.binary_frame_assembler.stateHash();

    _ = restoreCommittedPairIntoKernel(
        allocator,
        dir,
        manifest_path,
        matching_path,
        risk_path,
        &target,
    ) catch |err| {
        if (err == error.OutOfMemory) {
            try std.testing.expectEqual(@as(u64, 77), target.next_seq);
            try std.testing.expectEqual(@as(usize, 1), target.binary_frame_assembler.pendingTransfers());
            try std.testing.expectEqual(before_hash, try target.binary_frame_assembler.stateHash());
        }
        return err;
    };
}

test "XME1 schema 2 roundtrips exact Java capacity while XRE1 and XCM1 remain version 1" {
    const allocator = std.testing.allocator;
    var source = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer source.deinit();
    try addJavaCapacityPendingTransfer(&source);
    const expected_java_hash = try source.binary_frame_assembler.javaStateHash32();
    const expected_native_hash = try source.binary_frame_assembler.stateHash();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const manifest_path = "EX0_snapshot_81.ecm";
    const matching_path = "EX0_snapshot_81_ME0.ecs";
    const risk_path = "EX0_snapshot_81_RE0.ecs";
    const manifest = try writeTestPairPayloads(
        allocator,
        tmp.dir,
        matching_path,
        risk_path,
        &source,
        81,
        800,
        81_000,
    );
    try writeManifestAtomic(allocator, tmp.dir, manifest_path, manifest);

    const matching_bytes = try readTestFileBytes(allocator, tmp.dir, matching_path);
    defer allocator.free(matching_bytes);
    const risk_bytes = try readTestFileBytes(allocator, tmp.dir, risk_path);
    defer allocator.free(risk_bytes);
    const manifest_bytes = try readTestFileBytes(allocator, tmp.dir, manifest_path);
    defer allocator.free(manifest_bytes);
    try std.testing.expectEqualSlices(u8, MATCHING_MAGIC_V1, matching_bytes[0..4]);
    try std.testing.expectEqual(MATCHING_FORMAT_VERSION, std.mem.readInt(u16, matching_bytes[4..6], .little));
    try std.testing.expectEqual(@as(u32, 10), std.mem.readInt(u32, matching_bytes[single_pending_capacity_offset..][0..4], .little));
    try std.testing.expectEqual(@as(u32, 5), std.mem.readInt(u32, matching_bytes[single_pending_capacity_offset + 4 ..][0..4], .little));
    try std.testing.expectEqual(DURABLE_FORMAT_VERSION_V1, std.mem.readInt(u16, risk_bytes[4..6], .little));
    try std.testing.expectEqual(DURABLE_FORMAT_VERSION_V1, std.mem.readInt(u16, manifest_bytes[4..6], .little));

    var restored = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer restored.deinit();
    _ = try restoreCommittedPairIntoKernel(
        allocator,
        tmp.dir,
        manifest_path,
        matching_path,
        risk_path,
        &restored,
    );
    try std.testing.expectEqual(expected_native_hash, try restored.binary_frame_assembler.stateHash());
    try std.testing.expectEqual(expected_java_hash, try restored.binary_frame_assembler.javaStateHash32());

    try std.testing.checkAllAllocationFailures(
        allocator,
        durableRestoreAllocationCampaign,
        .{ tmp.dir, manifest_path, matching_path, risk_path },
    );
}

test "XME1 schema 1 and schema 2 null sentinel preserve unavailable Java capacity" {
    const allocator = std.testing.allocator;
    var source = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer source.deinit();
    var derivable_words = [_]i64{
        (@as(i64, 41) << 32) | 0x1122_3344,
        -2,
        std.math.maxInt(i64),
        std.math.minInt(i64),
        99,
    };
    try source.binary_frame_assembler.restorePendingState(&.{.{
        .transfer_id = -11,
        .command_type = null,
        .java_backing_capacity = null,
        .words = &derivable_words,
    }});
    const expected_native_hash = try source.binary_frame_assembler.stateHash();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const current_matching = "EX0_snapshot_82_ME0.ecs";
    const current_risk = "EX0_snapshot_82_RE0.ecs";
    const current_manifest = try writeTestPairPayloads(
        allocator,
        tmp.dir,
        current_matching,
        current_risk,
        &source,
        82,
        820,
        82_000,
    );
    const current_bytes = try readTestFileBytes(allocator, tmp.dir, current_matching);
    defer allocator.free(current_bytes);
    try std.testing.expectEqual(@as(u32, 0), std.mem.readInt(u32, current_bytes[single_pending_capacity_offset..][0..4], .little));
    try std.testing.expectEqual(
        @as(u32, 1),
        (try readMatchingPayloadSummary(tmp.dir, current_matching)).pending_transfer_count,
    );
    try writeManifestAtomic(allocator, tmp.dir, "EX0_snapshot_82.ecm", current_manifest);

    var current_target = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer current_target.deinit();
    _ = try restoreCommittedPairIntoKernel(
        allocator,
        tmp.dir,
        "EX0_snapshot_82.ecm",
        current_matching,
        current_risk,
        &current_target,
    );
    try std.testing.expectEqual(expected_native_hash, try current_target.binary_frame_assembler.stateHash());
    try std.testing.expectError(error.JavaStateHashUnavailable, current_target.binary_frame_assembler.javaStateHash32());

    const legacy_matching = "EX0_snapshot_83_ME0.ecs";
    const legacy_risk = "EX0_snapshot_83_RE0.ecs";
    var legacy_manifest = try writeTestPairPayloads(
        allocator,
        tmp.dir,
        legacy_matching,
        legacy_risk,
        &source,
        83,
        830,
        83_000,
    );
    const legacy_current = try readTestFileBytes(allocator, tmp.dir, legacy_matching);
    defer allocator.free(legacy_current);
    const legacy_bytes = try downgradeSinglePendingMatchingPayloadToV1(allocator, legacy_current);
    defer allocator.free(legacy_bytes);
    try writeTestFileBytes(tmp.dir, legacy_matching, legacy_bytes);
    legacy_manifest.matching = try digestFile(allocator, tmp.dir, legacy_matching);
    try writeManifestAtomic(allocator, tmp.dir, "EX0_snapshot_83.ecm", legacy_manifest);
    const legacy_summary = try readMatchingPayloadSummary(tmp.dir, legacy_matching);
    try std.testing.expectEqual(@as(u32, 1), legacy_summary.pending_transfer_count);

    var legacy_target = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer legacy_target.deinit();
    _ = try restoreCommittedPairIntoKernel(
        allocator,
        tmp.dir,
        "EX0_snapshot_83.ecm",
        legacy_matching,
        legacy_risk,
        &legacy_target,
    );
    try std.testing.expectEqual(expected_native_hash, try legacy_target.binary_frame_assembler.stateHash());
    try std.testing.expectError(error.JavaStateHashUnavailable, legacy_target.binary_frame_assembler.javaStateHash32());
}

test "XME1 schema 2 rejects mismatched explicit Java capacity" {
    const allocator = std.testing.allocator;
    var source = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer source.deinit();
    try addJavaCapacityPendingTransfer(&source);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const matching_path = "corrupt.me";
    _ = try writeMatchingPayloadAtomic(
        allocator,
        tmp.dir,
        matching_path,
        &source,
        .{
            .snapshot_id = 84,
            .command_seq = 840,
            .next_seq = 840,
            .timestamp_ns = 84_000,
            .shard_id = 0,
            .shard_count = 1,
        },
    );
    const bytes = try readTestFileBytes(allocator, tmp.dir, matching_path);
    defer allocator.free(bytes);
    const corrupted = try allocator.dupe(u8, bytes);
    defer allocator.free(corrupted);
    std.mem.writeInt(u32, corrupted[single_pending_capacity_offset..][0..4], 5, .little);
    try writeTestFileBytes(tmp.dir, matching_path, corrupted);
    try std.testing.expectError(error.InvalidFormat, readMatchingPayloadSummary(tmp.dir, matching_path));
}

test "ordinary XCS publication rejects collision and preserves existing bytes" {
    const allocator = std.testing.allocator;
    var snapshot: KernelSnapshot = .{
        .allocator = allocator,
        .depth = 7,
        .risk_state_hash = 99,
    };
    defer snapshot.deinit();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = "snapshot.ecs";
    const sentinel = "already-committed-xcs";
    const existing = try tmp.dir.createFile(io_context.current(), path, .{
        .truncate = true,
    });
    try existing.writeStreamingAll(io_context.current(), sentinel);
    existing.close(io_context.current());

    try std.testing.expectError(
        error.PathAlreadyExists,
        snapshot.writeToFile(tmp.dir, path),
    );
    const preserved = try readTestFileBytes(allocator, tmp.dir, path);
    defer allocator.free(preserved);
    try std.testing.expectEqualSlices(u8, sentinel, preserved);
}

test "XCM manifest publication rejects collision and preserves committed bytes" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = "EX0_snapshot_71.ecm";
    const committed: SnapshotPairManifest = .{
        .snapshot_id = 71,
        .matching_seq = 300,
        .risk_seq = 301,
        .next_seq = 301,
        .timestamp_ns = 456_789,
        .matching_shards = 1,
        .risk_shards = 1,
        .matching = .{ .byte_len = 11, .hash = 12 },
        .risk = .{ .byte_len = 13, .hash = 14 },
    };
    try writeManifestAtomic(allocator, tmp.dir, path, committed);
    const before = try readTestFileBytes(allocator, tmp.dir, path);
    defer allocator.free(before);

    var conflicting = committed;
    conflicting.timestamp_ns += 1;
    conflicting.matching.hash += 1;
    try std.testing.expectError(
        error.PathAlreadyExists,
        writeManifestAtomic(allocator, tmp.dir, path, conflicting),
    );

    const after = try readTestFileBytes(allocator, tmp.dir, path);
    defer allocator.free(after);
    try std.testing.expectEqualSlices(u8, before, after);
    try std.testing.expect(std.meta.eql(committed, try readManifest(tmp.dir, path)));
}

test "orphan XME and XRE payloads remain atomically replaceable" {
    const allocator = std.testing.allocator;
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();
    try std.testing.expect(try kernel.addSymbol(try makeExchangeSpec()));

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const matching_path = "EX0_snapshot_72_ME0.ecs";
    const risk_path = "EX0_snapshot_72_RE0.ecs";
    const initial_matching: DurableSnapshotMetadata = .{
        .snapshot_id = 72,
        .command_seq = 400,
        .next_seq = 400,
        .timestamp_ns = 567_890,
        .shard_id = 0,
        .shard_count = 1,
    };
    const initial_risk: DurableSnapshotMetadata = .{
        .snapshot_id = 72,
        .command_seq = 401,
        .next_seq = 401,
        .timestamp_ns = 567_890,
        .shard_id = 0,
        .shard_count = 1,
    };
    const matching_before = try writeMatchingPayloadAtomic(
        allocator,
        tmp.dir,
        matching_path,
        &kernel,
        initial_matching,
    );
    const risk_before = try writeRiskPayloadAtomic(
        allocator,
        tmp.dir,
        risk_path,
        &kernel,
        initial_risk,
    );

    var replacement_matching = initial_matching;
    replacement_matching.command_seq = 500;
    replacement_matching.next_seq = 500;
    replacement_matching.timestamp_ns += 1;
    var replacement_risk = initial_risk;
    replacement_risk.command_seq = 501;
    replacement_risk.next_seq = 501;
    replacement_risk.timestamp_ns += 1;
    const matching_after = try writeMatchingPayloadAtomic(
        allocator,
        tmp.dir,
        matching_path,
        &kernel,
        replacement_matching,
    );
    const risk_after = try writeRiskPayloadAtomic(
        allocator,
        tmp.dir,
        risk_path,
        &kernel,
        replacement_risk,
    );

    try std.testing.expect(!std.meta.eql(matching_before, matching_after));
    try std.testing.expect(!std.meta.eql(risk_before, risk_after));
    try std.testing.expect(std.meta.eql(
        replacement_matching,
        (try readMatchingPayloadSummary(tmp.dir, matching_path)).metadata,
    ));
    try std.testing.expect(std.meta.eql(
        replacement_risk,
        (try readRiskPayloadSummary(tmp.dir, risk_path)).metadata,
    ));
}

test "atomic publication syncs a nested relative parent directory" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io_context.current(), "nested/checkpoints");

    const manifest: SnapshotPairManifest = .{
        .snapshot_id = 73,
        .matching_seq = 600,
        .risk_seq = 601,
        .next_seq = 601,
        .timestamp_ns = 678_901,
        .matching_shards = 1,
        .risk_shards = 1,
        .matching = .{ .byte_len = 21, .hash = 22 },
        .risk = .{ .byte_len = 23, .hash = 24 },
    };
    const path = "nested/checkpoints/EX0_snapshot_73.ecm";
    try writeManifestAtomic(allocator, tmp.dir, path, manifest);
    try std.testing.expect(std.meta.eql(manifest, try readManifest(tmp.dir, path)));
}

test "durable pair validation rejects malformed sequence topology and metadata" {
    const valid_manifest: SnapshotPairManifest = .{
        .snapshot_id = 42,
        .matching_seq = 100,
        .risk_seq = 101,
        .next_seq = 101,
        .timestamp_ns = 987_654,
        .matching_shards = 1,
        .risk_shards = 1,
        .matching = .{ .byte_len = 1, .hash = 2 },
        .risk = .{ .byte_len = 3, .hash = 4 },
    };
    const valid_matching: DurableSnapshotMetadata = .{
        .snapshot_id = 42,
        .command_seq = 100,
        .next_seq = 100,
        .timestamp_ns = 987_654,
        .shard_id = 0,
        .shard_count = 1,
    };
    const valid_risk: DurableSnapshotMetadata = .{
        .snapshot_id = 42,
        .command_seq = 101,
        .next_seq = 101,
        .timestamp_ns = 987_654,
        .shard_id = 0,
        .shard_count = 1,
    };
    try validatePairMetadata(valid_manifest, valid_matching, valid_risk);
    try validateRequestedPaths(
        valid_manifest,
        "EX0_snapshot_42.ecm",
        "EX0_snapshot_42_ME0.ecs",
        "EX0_snapshot_42_RE0.ecs",
    );

    var invalid_manifest = valid_manifest;
    invalid_manifest.risk_seq = 102;
    invalid_manifest.next_seq = 102;
    try std.testing.expectError(
        error.SnapshotMetadataMismatch,
        validatePairMetadata(invalid_manifest, valid_matching, valid_risk),
    );

    invalid_manifest = valid_manifest;
    invalid_manifest.next_seq = 102;
    try std.testing.expectError(
        error.SnapshotMetadataMismatch,
        validatePairMetadata(invalid_manifest, valid_matching, valid_risk),
    );

    invalid_manifest = valid_manifest;
    invalid_manifest.matching_shards = 0;
    try std.testing.expectError(
        error.SnapshotMetadataMismatch,
        validatePairMetadata(invalid_manifest, valid_matching, valid_risk),
    );

    var invalid_matching = valid_matching;
    invalid_matching.next_seq = 101;
    try std.testing.expectError(
        error.SnapshotMetadataMismatch,
        validatePairMetadata(valid_manifest, invalid_matching, valid_risk),
    );

    invalid_matching = valid_matching;
    invalid_matching.shard_id = 1;
    try std.testing.expectError(
        error.SnapshotMetadataMismatch,
        validatePairMetadata(valid_manifest, invalid_matching, valid_risk),
    );

    var invalid_risk = valid_risk;
    invalid_risk.shard_count = 2;
    try std.testing.expectError(
        error.SnapshotMetadataMismatch,
        validatePairMetadata(valid_manifest, valid_matching, invalid_risk),
    );

    try std.testing.expectError(
        error.SnapshotMetadataMismatch,
        validateRequestedPaths(
            valid_manifest,
            "EX0_snapshot_41.ecm",
            "EX0_snapshot_41_ME0.ecs",
            "EX0_snapshot_41_RE0.ecs",
        ),
    );
}

test "durable pair restore preserves live kernel on malformed manifest metadata" {
    const allocator = std.testing.allocator;
    var source = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer source.deinit();
    try std.testing.expect(try source.addSymbol(try makeExchangeSpec()));

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const manifest_path = "pair.ecm";
    const matching_path = "pair.me";
    const risk_path = "pair.re";
    var manifest = try writeTestPairPayloads(
        allocator,
        tmp.dir,
        matching_path,
        risk_path,
        &source,
        51,
        200,
        123_456,
    );
    manifest.next_seq += 1;
    try writeManifestAtomic(allocator, tmp.dir, manifest_path, manifest);

    var target = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer target.deinit();
    var add_user = makeAddUserCommand(80_001);
    var add_response = try target.execute(&add_user);
    defer add_response.deinit(allocator);
    target.next_seq = 77;
    const before_hash = (try target.queryReport(.risk_state_hash)).risk_state_hash;

    try std.testing.expectError(
        error.SnapshotMetadataMismatch,
        restoreCommittedPairIntoKernel(
            allocator,
            tmp.dir,
            manifest_path,
            matching_path,
            risk_path,
            &target,
        ),
    );
    try std.testing.expectEqual(@as(u64, 77), target.next_seq);
    try std.testing.expect(target.risk_engine.getUserProfile(80_001) != null);
    try std.testing.expectEqual(before_hash, (try target.queryReport(.risk_state_hash)).risk_state_hash);
}

test "durable pair restore preserves live kernel on staged allocation failure" {
    const allocator = std.testing.allocator;
    var source = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer source.deinit();
    try std.testing.expect(try source.addSymbol(try makeExchangeSpec()));

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const manifest_path = "EX0_snapshot_52.ecm";
    const matching_path = "EX0_snapshot_52_ME0.ecs";
    const risk_path = "EX0_snapshot_52_RE0.ecs";
    const manifest = try writeTestPairPayloads(
        allocator,
        tmp.dir,
        matching_path,
        risk_path,
        &source,
        52,
        300,
        234_567,
    );
    try writeManifestAtomic(allocator, tmp.dir, manifest_path, manifest);

    var failing_allocator_state = std.testing.FailingAllocator.init(allocator, .{});
    const target_allocator = failing_allocator_state.allocator();
    var target = try kernel_mod.ExchangeKernel.init(target_allocator, .{});
    defer {
        failing_allocator_state.fail_index = std.math.maxInt(usize);
        target.deinit();
    }
    var add_user = makeAddUserCommand(80_002);
    var add_response = try target.execute(&add_user);
    defer add_response.deinit(target_allocator);
    const live_frame: cmd.OrderCommand = .{
        .command = .binary_data_query,
        .order_id = 1,
        .price = 2,
        .reserve_bid_price = 3,
        .size = 4,
        .uid = 5,
        .symbol = 0,
        .user_cookie = 900,
    };
    _ = try target.binary_frame_assembler.acceptFrame(&live_frame);
    target.next_seq = 88;
    const before_risk_hash = (try target.queryReport(.risk_state_hash)).risk_state_hash;
    const before_binary_hash = try target.binary_frame_assembler.stateHash();

    failing_allocator_state.fail_index = failing_allocator_state.alloc_index;
    try std.testing.expectError(
        error.OutOfMemory,
        restoreCommittedPairIntoKernel(
            allocator,
            tmp.dir,
            manifest_path,
            matching_path,
            risk_path,
            &target,
        ),
    );
    try std.testing.expect(failing_allocator_state.has_induced_failure);

    failing_allocator_state.fail_index = std.math.maxInt(usize);
    try std.testing.expectEqual(@as(u64, 88), target.next_seq);
    try std.testing.expect(target.risk_engine.getUserProfile(80_002) != null);
    try std.testing.expectEqual(@as(usize, 1), target.binary_frame_assembler.pendingTransfers());
    try std.testing.expectEqual(before_risk_hash, (try target.queryReport(.risk_state_hash)).risk_state_hash);
    try std.testing.expectEqual(before_binary_hash, try target.binary_frame_assembler.stateHash());
}

test "legacy snapshots reject custom report registries before work or mutation" {
    const allocator = std.testing.allocator;
    const custom_registry = report_registry.Registry.fromSpecs(.{
        SnapshotCompatibilityCustomReportSpec,
    });
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{
        .report_queries = .{ .custom_registry = custom_registry },
    });
    defer kernel.deinit();

    try std.testing.expect(
        try kernel.risk_engine.user_profile_service.addEmptyUserProfile(84_001),
    );
    kernel.next_seq = 73;

    try std.testing.expectError(
        error.LegacySnapshotCustomReportRegistryUnsupported,
        KernelSnapshot.captureFromKernel(
            std.testing.failing_allocator,
            &kernel,
            &.{},
            1,
        ),
    );
    try std.testing.expectEqual(@as(u64, 73), kernel.next_seq);
    try std.testing.expect(kernel.risk_engine.getUserProfile(84_001) != null);

    var snapshot: KernelSnapshot = .{
        .allocator = allocator,
        .depth = 1,
        .risk_state_hash = 0,
    };
    defer snapshot.deinit();
    try std.testing.expectError(
        error.LegacySnapshotCustomReportRegistryUnsupported,
        snapshot.restoreIntoKernel(&kernel),
    );
    try std.testing.expectEqual(@as(u64, 73), kernel.next_seq);
    try std.testing.expect(kernel.risk_engine.getUserProfile(84_001) != null);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try std.testing.expectError(
        error.LegacySnapshotCustomReportRegistryUnsupported,
        restoreCommittedPairIntoKernel(
            allocator,
            tmp.dir,
            "missing-manifest",
            "missing-matching",
            "missing-risk",
            &kernel,
        ),
    );
    try std.testing.expectEqual(@as(u64, 73), kernel.next_seq);
    try std.testing.expect(kernel.risk_engine.getUserProfile(84_001) != null);
}

test "legacy snapshots remain compatible with an empty custom report registry" {
    const allocator = std.testing.allocator;
    var source = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer source.deinit();
    try std.testing.expectEqual(@as(usize, 0), source.customReportRegistry().len());

    var snapshot = try KernelSnapshot.captureFromKernel(allocator, &source, &.{}, 1);
    defer snapshot.deinit();

    var target = try kernel_mod.ExchangeKernel.init(allocator, .{});
    defer target.deinit();
    try std.testing.expect(
        try target.risk_engine.user_profile_service.addEmptyUserProfile(84_002),
    );
    target.next_seq = 74;

    try snapshot.restoreIntoKernel(&target);
    try std.testing.expectEqual(@as(u64, 0), target.next_seq);
    try std.testing.expect(target.risk_engine.getUserProfile(84_002) == null);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try std.testing.expectError(
        error.FileNotFound,
        restoreCommittedPairIntoKernel(
            allocator,
            tmp.dir,
            "missing-manifest",
            "missing-matching",
            "missing-risk",
            &target,
        ),
    );
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
    var suspend_u1 = makeBalanceAdjustmentCommand(9811, 15, 7, 2, .@"suspend");
    var dep_u1_response = try source.execute(&dep_u1);
    defer dep_u1_response.deinit(allocator);
    var dep_u2_response = try source.execute(&dep_u2);
    defer dep_u2_response.deinit(allocator);
    var dep_u3_response = try source.execute(&dep_u3);
    defer dep_u3_response.deinit(allocator);
    var suspend_u1_response = try source.execute(&suspend_u1);
    defer suspend_u1_response.deinit(allocator);

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
    try std.testing.expect(target.risk_engine.user_profile_service.user_profiles.count() > 0);
    try std.testing.expect(target.risk_engine.getUserProfile(9811).?.accounts.count() > 0);
    try std.testing.expect(target.risk_engine.fees.count() > 0);
    try std.testing.expect(target.risk_engine.adjustments.count() > 0);
    try std.testing.expect(target.risk_engine.suspends.count() > 0);
    try expectLossySnapshotHistoriesUnavailable(&target);

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

test "empty legacy native snapshot restore keeps all Eclipse histories unavailable" {
    const allocator = std.testing.allocator;

    var source = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer source.deinit();

    var snapshot = try KernelSnapshot.captureFromKernel(allocator, &source, &.{}, 1);
    defer snapshot.deinit();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try snapshot.writeToFile(tmp.dir, "empty-legacy.bin");

    var loaded = try KernelSnapshot.readFromFile(allocator, tmp.dir, "empty-legacy.bin");
    defer loaded.deinit();

    var target = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer target.deinit();

    try loaded.restoreIntoKernel(&target);
    try expectLossySnapshotHistoriesUnavailable(&target);
}

test "populated legacy native snapshot restore keeps all Eclipse histories unavailable" {
    const allocator = std.testing.allocator;

    var source = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer source.deinit();

    try std.testing.expect(
        try source.risk_engine.user_profile_service.addEmptyUserProfile(98_021),
    );
    const source_profile = source.risk_engine.getUserProfile(98_021) orelse unreachable;
    try source_profile.putAccount(allocator, 15, 45_000);

    try source.risk_engine.fees.ensureUnusedCapacity(allocator, 1);
    try source.risk_engine.fees_history.recordPut(allocator, 15);
    source.risk_engine.fees.putAssumeCapacityNoClobber(15, 3);

    try source.risk_engine.adjustments.ensureUnusedCapacity(allocator, 1);
    try source.risk_engine.adjustments_history.recordPut(allocator, 15);
    source.risk_engine.adjustments.putAssumeCapacityNoClobber(15, -45_000);

    try source.risk_engine.suspends.ensureUnusedCapacity(allocator, 1);
    try source.risk_engine.suspends_history.recordPut(allocator, 15);
    source.risk_engine.suspends.putAssumeCapacityNoClobber(15, -7);

    var snapshot = try KernelSnapshot.captureFromKernel(allocator, &source, &.{}, 1);
    defer snapshot.deinit();

    var target = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer target.deinit();

    try snapshot.restoreIntoKernel(&target);
    try std.testing.expectEqual(@as(usize, 1), target.risk_engine.user_profile_service.user_profiles.count());
    try std.testing.expectEqual(
        @as(i64, 45_000),
        target.risk_engine.getUserProfile(98_021).?.accountBalance(15),
    );
    try std.testing.expectEqual(@as(i64, 3), target.risk_engine.getFeeCollected(15));
    try std.testing.expectEqual(@as(?i64, -45_000), target.risk_engine.adjustments.get(15));
    try std.testing.expectEqual(@as(?i64, -7), target.risk_engine.suspends.get(15));
    try expectLossySnapshotHistoriesUnavailable(&target);
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
