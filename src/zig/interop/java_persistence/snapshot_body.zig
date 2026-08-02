const std = @import("std");

/// Strict decoder for the object body written by the legacy Java
/// `WriteBytesMarshallable` snapshot implementations. The LZ4 frame, the
/// `WireToOutputStream2` outer length, and the Chronicle RAW document header
/// are intentionally outside this module's scope.
///
/// Chronicle `BytesOut.writeInt/writeLong` use native-order fixed-width writes.
/// The supported historical snapshot producers are little-endian x86_64/arm64,
/// so this archive codec makes that byte order explicit instead of depending on
/// the host running the importer.
pub const Limits = struct {
    max_body_bytes: usize = 512 * 1024 * 1024,
    max_collection_entries: usize = 1_000_000,
    max_total_elements: usize = 4_000_000,
    max_transfer_words: usize = 1_000_000,
    /// Cumulative bytes requested for returned slices and temporary key arrays.
    /// A cumulative budget is deliberately stricter than a peak-live budget and
    /// remains deterministic when an allocator rounds allocation sizes.
    max_allocated_bytes: usize = 1024 * 1024 * 1024,
};

pub const ShardIdentity = struct {
    shard_id: i32,
    shard_mask: i64,
};

pub const DecodeError = std.mem.Allocator.Error || error{
    BodyLimitExceeded,
    UnexpectedEnd,
    TrailingBytes,
    NegativeCollectionLength,
    CollectionLimitExceeded,
    TotalElementLimitExceeded,
    TransferWordLimitExceeded,
    AllocationSizeOverflow,
    AllocationBudgetExceeded,
    ShardIdMismatch,
    ShardMaskMismatch,
    DuplicateKey,
    OrderBookSymbolKeyMismatch,
    SymbolSpecificationKeyMismatch,
    UserProfileKeyMismatch,
    PositionSymbolKeyMismatch,
    BucketPriceKeyMismatch,
    OrderIdKeyMismatch,
    OrderSideMismatch,
    InvalidSymbolType,
    InvalidUserStatus,
    InvalidPositionDirection,
    InvalidOrderAction,
    InvalidOrderBookImplementation,
    InvalidTransferWordCount,
};

pub const SymbolType = enum(u8) {
    currency_exchange_pair = 0,
    futures_contract = 1,
    option = 2,
};

pub const UserStatus = enum(u8) {
    active = 0,
    suspended = 1,
};

pub const PositionDirection = enum(i8) {
    short = -1,
    empty = 0,
    long = 1,
};

pub const OrderAction = enum(u8) {
    ask = 0,
    bid = 1,
};

pub const CoreSymbolSpecification = struct {
    symbol_id: i32,
    symbol_type: SymbolType,
    base_currency: i32,
    quote_currency: i32,
    base_scale_k: i64,
    quote_scale_k: i64,
    taker_fee: i64,
    maker_fee: i64,
    margin_buy: i64,
    margin_sell: i64,
};

pub const TransferRecord = struct {
    transfer_id: i64,
    words_transferred: i32,
    data: []i64,

    pub fn deinit(self: *TransferRecord, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
        self.* = undefined;
    }
};

pub const BinaryCommandsState = struct {
    incoming_transfers: []TransferRecord,

    pub fn deinit(self: *BinaryCommandsState, allocator: std.mem.Allocator) void {
        for (self.incoming_transfers) |*record| record.deinit(allocator);
        allocator.free(self.incoming_transfers);
        self.* = undefined;
    }
};

pub const Order = struct {
    order_id: i64,
    price: i64,
    size: i64,
    filled: i64,
    reserve_bid_price: i64,
    action: OrderAction,
    uid: i64,
    timestamp: i64,
};

pub const NaiveBucket = struct {
    price: i64,
    orders: []Order,
    total_volume: i64,

    pub fn deinit(self: *NaiveBucket, allocator: std.mem.Allocator) void {
        allocator.free(self.orders);
        self.* = undefined;
    }
};

pub const NaiveOrderBook = struct {
    symbol_specification: CoreSymbolSpecification,
    ask_buckets: []NaiveBucket,
    bid_buckets: []NaiveBucket,

    pub fn deinit(self: *NaiveOrderBook, allocator: std.mem.Allocator) void {
        deinitBuckets(allocator, self.ask_buckets);
        deinitBuckets(allocator, self.bid_buckets);
        self.* = undefined;
    }
};

pub const DirectOrderBook = struct {
    symbol_specification: CoreSymbolSpecification,
    /// Java writes a single count, then the sorted ask stream followed by the
    /// sorted bid stream. `action` preserves the only on-wire side boundary.
    orders: []Order,

    pub fn deinit(self: *DirectOrderBook, allocator: std.mem.Allocator) void {
        allocator.free(self.orders);
        self.* = undefined;
    }
};

pub const OrderBook = union(enum) {
    naive: NaiveOrderBook,
    direct: DirectOrderBook,

    pub fn symbolId(self: OrderBook) i32 {
        return switch (self) {
            .naive => |book| book.symbol_specification.symbol_id,
            .direct => |book| book.symbol_specification.symbol_id,
        };
    }

    pub fn deinit(self: *OrderBook, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .naive => |*book| book.deinit(allocator),
            .direct => |*book| book.deinit(allocator),
        }
        self.* = undefined;
    }
};

pub const OrderBookEntry = struct {
    symbol_id: i32,
    book: OrderBook,

    pub fn deinit(self: *OrderBookEntry, allocator: std.mem.Allocator) void {
        self.book.deinit(allocator);
        self.* = undefined;
    }
};

pub const MatchingEngineState = struct {
    shard: ShardIdentity,
    binary_commands: BinaryCommandsState,
    order_books: []OrderBookEntry,

    pub fn deinit(self: *MatchingEngineState, allocator: std.mem.Allocator) void {
        self.binary_commands.deinit(allocator);
        for (self.order_books) |*entry| entry.deinit(allocator);
        allocator.free(self.order_books);
        self.* = undefined;
    }
};

pub const SymbolPositionRecord = struct {
    symbol: i32,
    currency: i32,
    direction: PositionDirection,
    open_volume: i64,
    open_price_sum: i64,
    profit: i64,
    pending_sell_size: i64,
    pending_buy_size: i64,
};

pub const IntLongEntry = struct {
    key: i32,
    value: i64,
};

pub const UserProfile = struct {
    uid: i64,
    positions: []SymbolPositionRecord,
    adjustments_counter: i64,
    accounts: []IntLongEntry,
    status: UserStatus,

    pub fn deinit(self: *UserProfile, allocator: std.mem.Allocator) void {
        allocator.free(self.positions);
        allocator.free(self.accounts);
        self.* = undefined;
    }
};

pub const LastPriceEntry = struct {
    symbol_id: i32,
    ask_price: i64,
    bid_price: i64,
};

pub const RiskEngineState = struct {
    shard: ShardIdentity,
    symbol_specifications: []CoreSymbolSpecification,
    user_profiles: []UserProfile,
    binary_commands: BinaryCommandsState,
    last_prices: []LastPriceEntry,
    fees: []IntLongEntry,
    adjustments: []IntLongEntry,
    suspends: []IntLongEntry,

    pub fn deinit(self: *RiskEngineState, allocator: std.mem.Allocator) void {
        allocator.free(self.symbol_specifications);
        for (self.user_profiles) |*profile| profile.deinit(allocator);
        allocator.free(self.user_profiles);
        self.binary_commands.deinit(allocator);
        allocator.free(self.last_prices);
        allocator.free(self.fees);
        allocator.free(self.adjustments);
        allocator.free(self.suspends);
        self.* = undefined;
    }
};

/// Decodes exactly one `MatchingEngineRouter.writeMarshallable` body. Requiring
/// the descriptor-derived shard identity mirrors the Java recovery constructor,
/// which rejects a body belonging to another shard or shard topology.
pub fn decodeMatchingEngineBody(
    allocator: std.mem.Allocator,
    body: []const u8,
    expected_shard: ShardIdentity,
    limits: Limits,
) DecodeError!MatchingEngineState {
    var decoder = try Decoder.init(allocator, body, limits);

    const shard = try decoder.readAndValidateShard(expected_shard);
    var binary_commands = try decoder.readBinaryCommandsState();
    errdefer binary_commands.deinit(allocator);
    const order_books = try decoder.readOrderBooks();
    errdefer deinitOrderBooks(allocator, order_books);

    try decoder.finish();
    return .{
        .shard = shard,
        .binary_commands = binary_commands,
        .order_books = order_books,
    };
}

/// Decodes exactly one `RiskEngine.writeMarshallable` body.
pub fn decodeRiskEngineBody(
    allocator: std.mem.Allocator,
    body: []const u8,
    expected_shard: ShardIdentity,
    limits: Limits,
) DecodeError!RiskEngineState {
    var decoder = try Decoder.init(allocator, body, limits);

    const shard = try decoder.readAndValidateShard(expected_shard);
    const symbol_specifications = try decoder.readSymbolSpecifications();
    errdefer allocator.free(symbol_specifications);
    const user_profiles = try decoder.readUserProfiles();
    errdefer deinitUserProfiles(allocator, user_profiles);
    var binary_commands = try decoder.readBinaryCommandsState();
    errdefer binary_commands.deinit(allocator);
    const last_prices = try decoder.readLastPrices();
    errdefer allocator.free(last_prices);
    const fees = try decoder.readIntLongMap();
    errdefer allocator.free(fees);
    const adjustments = try decoder.readIntLongMap();
    errdefer allocator.free(adjustments);
    const suspends = try decoder.readIntLongMap();
    errdefer allocator.free(suspends);

    try decoder.finish();
    return .{
        .shard = shard,
        .symbol_specifications = symbol_specifications,
        .user_profiles = user_profiles,
        .binary_commands = binary_commands,
        .last_prices = last_prices,
        .fees = fees,
        .adjustments = adjustments,
        .suspends = suspends,
    };
}

const Decoder = struct {
    allocator: std.mem.Allocator,
    input: []const u8,
    position: usize = 0,
    limits: Limits,
    total_elements: usize = 0,
    allocated_bytes: usize = 0,

    fn init(
        allocator: std.mem.Allocator,
        body: []const u8,
        limits: Limits,
    ) DecodeError!Decoder {
        if (body.len > limits.max_body_bytes) return error.BodyLimitExceeded;
        return .{ .allocator = allocator, .input = body, .limits = limits };
    }

    fn finish(self: *const Decoder) DecodeError!void {
        if (self.position != self.input.len) return error.TrailingBytes;
    }

    fn take(self: *Decoder, length: usize) DecodeError![]const u8 {
        if (length > self.input.len - self.position) return error.UnexpectedEnd;
        const result = self.input[self.position .. self.position + length];
        self.position += length;
        return result;
    }

    fn readU8(self: *Decoder) DecodeError!u8 {
        return (try self.take(1))[0];
    }

    fn readI32(self: *Decoder) DecodeError!i32 {
        return std.mem.readInt(i32, (try self.take(4))[0..4], .little);
    }

    fn readI64(self: *Decoder) DecodeError!i64 {
        return std.mem.readInt(i64, (try self.take(8))[0..8], .little);
    }

    fn readAndValidateShard(
        self: *Decoder,
        expected: ShardIdentity,
    ) DecodeError!ShardIdentity {
        const actual: ShardIdentity = .{
            .shard_id = try self.readI32(),
            .shard_mask = try self.readI64(),
        };
        if (actual.shard_id != expected.shard_id) return error.ShardIdMismatch;
        if (actual.shard_mask != expected.shard_mask) return error.ShardMaskMismatch;
        return actual;
    }

    fn readCount(self: *Decoder) DecodeError!usize {
        const signed = try self.readI32();
        if (signed < 0) return error.NegativeCollectionLength;
        const count: usize = @intCast(signed);
        if (count > self.limits.max_collection_entries) {
            return error.CollectionLimitExceeded;
        }
        self.total_elements = std.math.add(usize, self.total_elements, count) catch
            return error.TotalElementLimitExceeded;
        if (self.total_elements > self.limits.max_total_elements) {
            return error.TotalElementLimitExceeded;
        }
        return count;
    }

    fn allocSlice(self: *Decoder, comptime T: type, count: usize) DecodeError![]T {
        const byte_count = std.math.mul(usize, @sizeOf(T), count) catch
            return error.AllocationSizeOverflow;
        self.allocated_bytes = std.math.add(usize, self.allocated_bytes, byte_count) catch
            return error.AllocationBudgetExceeded;
        if (self.allocated_bytes > self.limits.max_allocated_bytes) {
            return error.AllocationBudgetExceeded;
        }
        return self.allocator.alloc(T, count);
    }

    fn readSymbolType(self: *Decoder) DecodeError!SymbolType {
        return switch (try self.readU8()) {
            0 => .currency_exchange_pair,
            1 => .futures_contract,
            2 => .option,
            else => error.InvalidSymbolType,
        };
    }

    fn readUserStatus(self: *Decoder) DecodeError!UserStatus {
        return switch (try self.readU8()) {
            0 => .active,
            1 => .suspended,
            else => error.InvalidUserStatus,
        };
    }

    fn readPositionDirection(self: *Decoder) DecodeError!PositionDirection {
        return switch (try self.readU8()) {
            0xff => .short,
            0 => .empty,
            1 => .long,
            else => error.InvalidPositionDirection,
        };
    }

    fn readOrderAction(self: *Decoder) DecodeError!OrderAction {
        return switch (try self.readU8()) {
            0 => .ask,
            1 => .bid,
            else => error.InvalidOrderAction,
        };
    }

    fn readCoreSymbolSpecification(self: *Decoder) DecodeError!CoreSymbolSpecification {
        return .{
            .symbol_id = try self.readI32(),
            .symbol_type = try self.readSymbolType(),
            .base_currency = try self.readI32(),
            .quote_currency = try self.readI32(),
            .base_scale_k = try self.readI64(),
            .quote_scale_k = try self.readI64(),
            .taker_fee = try self.readI64(),
            .maker_fee = try self.readI64(),
            .margin_buy = try self.readI64(),
            .margin_sell = try self.readI64(),
        };
    }

    fn readOrder(self: *Decoder) DecodeError!Order {
        return .{
            .order_id = try self.readI64(),
            .price = try self.readI64(),
            .size = try self.readI64(),
            .filled = try self.readI64(),
            .reserve_bid_price = try self.readI64(),
            .action = try self.readOrderAction(),
            .uid = try self.readI64(),
            .timestamp = try self.readI64(),
        };
    }

    fn readBinaryCommandsState(self: *Decoder) DecodeError!BinaryCommandsState {
        const count = try self.readCount();
        const records = try self.allocSlice(TransferRecord, count);
        errdefer self.allocator.free(records);
        const keys = try self.allocSlice(i64, count);
        defer self.allocator.free(keys);

        var initialized: usize = 0;
        errdefer for (records[0..initialized]) |*record| record.deinit(self.allocator);

        for (records, 0..) |*record, index| {
            const transfer_id = try self.readI64();
            keys[index] = transfer_id;
            const words_transferred = try self.readI32();
            const word_count = try self.readCount();
            if (word_count > self.limits.max_transfer_words) return error.TransferWordLimitExceeded;
            if (words_transferred < 0 or @as(usize, @intCast(words_transferred)) > word_count) {
                return error.InvalidTransferWordCount;
            }
            const data = try self.allocSlice(i64, word_count);
            errdefer self.allocator.free(data);
            for (data) |*word| word.* = try self.readI64();
            record.* = .{
                .transfer_id = transfer_id,
                .words_transferred = words_transferred,
                .data = data,
            };
            initialized += 1;
        }
        try rejectDuplicateI64(keys);
        return .{ .incoming_transfers = records };
    }

    fn readOrderBooks(self: *Decoder) DecodeError![]OrderBookEntry {
        const count = try self.readCount();
        const entries = try self.allocSlice(OrderBookEntry, count);
        errdefer self.allocator.free(entries);
        const keys = try self.allocSlice(i32, count);
        defer self.allocator.free(keys);

        var initialized: usize = 0;
        errdefer for (entries[0..initialized]) |*entry| entry.deinit(self.allocator);

        for (entries, 0..) |*entry, index| {
            const symbol_id = try self.readI32();
            keys[index] = symbol_id;
            var book = try self.readOrderBook();
            errdefer book.deinit(self.allocator);
            if (book.symbolId() != symbol_id) return error.OrderBookSymbolKeyMismatch;
            entry.* = .{ .symbol_id = symbol_id, .book = book };
            initialized += 1;
        }
        try rejectDuplicateI32(keys);
        return entries;
    }

    fn readOrderBook(self: *Decoder) DecodeError!OrderBook {
        return switch (try self.readU8()) {
            0 => .{ .naive = try self.readNaiveOrderBook() },
            2 => .{ .direct = try self.readDirectOrderBook() },
            else => error.InvalidOrderBookImplementation,
        };
    }

    fn readNaiveOrderBook(self: *Decoder) DecodeError!NaiveOrderBook {
        const specification = try self.readCoreSymbolSpecification();
        const ask_buckets = try self.readNaiveBuckets(.ask);
        errdefer deinitBuckets(self.allocator, ask_buckets);
        const bid_buckets = try self.readNaiveBuckets(.bid);
        errdefer deinitBuckets(self.allocator, bid_buckets);
        return .{
            .symbol_specification = specification,
            .ask_buckets = ask_buckets,
            .bid_buckets = bid_buckets,
        };
    }

    fn readNaiveBuckets(
        self: *Decoder,
        expected_action: OrderAction,
    ) DecodeError![]NaiveBucket {
        const count = try self.readCount();
        const buckets = try self.allocSlice(NaiveBucket, count);
        errdefer self.allocator.free(buckets);
        const keys = try self.allocSlice(i64, count);
        defer self.allocator.free(keys);

        var initialized: usize = 0;
        errdefer for (buckets[0..initialized]) |*bucket| bucket.deinit(self.allocator);

        for (buckets, 0..) |*bucket, index| {
            const map_price = try self.readI64();
            keys[index] = map_price;
            const price = try self.readI64();
            if (price != map_price) return error.BucketPriceKeyMismatch;
            const orders = try self.readNaiveOrders(expected_action);
            errdefer self.allocator.free(orders);
            bucket.* = .{
                .price = price,
                .orders = orders,
                .total_volume = try self.readI64(),
            };
            initialized += 1;
        }
        try rejectDuplicateI64(keys);
        return buckets;
    }

    fn readNaiveOrders(
        self: *Decoder,
        expected_action: OrderAction,
    ) DecodeError![]Order {
        const count = try self.readCount();
        const orders = try self.allocSlice(Order, count);
        errdefer self.allocator.free(orders);
        const keys = try self.allocSlice(i64, count);
        defer self.allocator.free(keys);

        for (orders, 0..) |*order, index| {
            const order_id = try self.readI64();
            keys[index] = order_id;
            order.* = try self.readOrder();
            if (order.order_id != order_id) return error.OrderIdKeyMismatch;
            if (order.action != expected_action) return error.OrderSideMismatch;
        }
        try rejectDuplicateI64(keys);
        return orders;
    }

    fn readDirectOrderBook(self: *Decoder) DecodeError!DirectOrderBook {
        const specification = try self.readCoreSymbolSpecification();
        const count = try self.readCount();
        const orders = try self.allocSlice(Order, count);
        errdefer self.allocator.free(orders);
        const keys = try self.allocSlice(i64, count);
        defer self.allocator.free(keys);

        var seen_bid = false;
        for (orders, 0..) |*order, index| {
            order.* = try self.readOrder();
            keys[index] = order.order_id;
            if (order.action == .bid) {
                seen_bid = true;
            } else if (seen_bid) {
                // The Java writer emits askOrdersStream followed by bidOrdersStream.
                return error.OrderSideMismatch;
            }
        }
        try rejectDuplicateI64(keys);
        return .{ .symbol_specification = specification, .orders = orders };
    }

    fn readSymbolSpecifications(self: *Decoder) DecodeError![]CoreSymbolSpecification {
        const count = try self.readCount();
        const specifications = try self.allocSlice(CoreSymbolSpecification, count);
        errdefer self.allocator.free(specifications);
        const keys = try self.allocSlice(i32, count);
        defer self.allocator.free(keys);

        for (specifications, 0..) |*specification, index| {
            const symbol_id = try self.readI32();
            keys[index] = symbol_id;
            specification.* = try self.readCoreSymbolSpecification();
            if (specification.symbol_id != symbol_id) {
                return error.SymbolSpecificationKeyMismatch;
            }
        }
        try rejectDuplicateI32(keys);
        return specifications;
    }

    fn readUserProfiles(self: *Decoder) DecodeError![]UserProfile {
        const count = try self.readCount();
        const profiles = try self.allocSlice(UserProfile, count);
        errdefer self.allocator.free(profiles);
        const keys = try self.allocSlice(i64, count);
        defer self.allocator.free(keys);

        var initialized: usize = 0;
        errdefer for (profiles[0..initialized]) |*profile| profile.deinit(self.allocator);

        for (profiles, 0..) |*profile, index| {
            const uid = try self.readI64();
            keys[index] = uid;
            const embedded_uid = try self.readI64();
            if (embedded_uid != uid) return error.UserProfileKeyMismatch;
            const positions = try self.readPositions();
            errdefer self.allocator.free(positions);
            const adjustments_counter = try self.readI64();
            const accounts = try self.readIntLongMap();
            errdefer self.allocator.free(accounts);
            profile.* = .{
                .uid = embedded_uid,
                .positions = positions,
                .adjustments_counter = adjustments_counter,
                .accounts = accounts,
                .status = try self.readUserStatus(),
            };
            initialized += 1;
        }
        try rejectDuplicateI64(keys);
        return profiles;
    }

    fn readPositions(self: *Decoder) DecodeError![]SymbolPositionRecord {
        const count = try self.readCount();
        const positions = try self.allocSlice(SymbolPositionRecord, count);
        errdefer self.allocator.free(positions);
        const keys = try self.allocSlice(i32, count);
        defer self.allocator.free(keys);

        for (positions, 0..) |*position, index| {
            const symbol = try self.readI32();
            keys[index] = symbol;
            const embedded_symbol = try self.readI32();
            if (embedded_symbol != symbol) return error.PositionSymbolKeyMismatch;
            position.* = .{
                .symbol = embedded_symbol,
                .currency = try self.readI32(),
                .direction = try self.readPositionDirection(),
                .open_volume = try self.readI64(),
                .open_price_sum = try self.readI64(),
                .profit = try self.readI64(),
                .pending_sell_size = try self.readI64(),
                .pending_buy_size = try self.readI64(),
            };
        }
        try rejectDuplicateI32(keys);
        return positions;
    }

    fn readLastPrices(self: *Decoder) DecodeError![]LastPriceEntry {
        const count = try self.readCount();
        const prices = try self.allocSlice(LastPriceEntry, count);
        errdefer self.allocator.free(prices);
        const keys = try self.allocSlice(i32, count);
        defer self.allocator.free(keys);

        for (prices, 0..) |*price, index| {
            const symbol_id = try self.readI32();
            keys[index] = symbol_id;
            price.* = .{
                .symbol_id = symbol_id,
                .ask_price = try self.readI64(),
                .bid_price = try self.readI64(),
            };
        }
        try rejectDuplicateI32(keys);
        return prices;
    }

    fn readIntLongMap(self: *Decoder) DecodeError![]IntLongEntry {
        const count = try self.readCount();
        const entries = try self.allocSlice(IntLongEntry, count);
        errdefer self.allocator.free(entries);
        const keys = try self.allocSlice(i32, count);
        defer self.allocator.free(keys);

        for (entries, 0..) |*entry, index| {
            const key = try self.readI32();
            keys[index] = key;
            entry.* = .{ .key = key, .value = try self.readI64() };
        }
        try rejectDuplicateI32(keys);
        return entries;
    }
};

fn deinitBuckets(allocator: std.mem.Allocator, buckets: []NaiveBucket) void {
    for (buckets) |*bucket| bucket.deinit(allocator);
    allocator.free(buckets);
}

fn deinitOrderBooks(allocator: std.mem.Allocator, entries: []OrderBookEntry) void {
    for (entries) |*entry| entry.deinit(allocator);
    allocator.free(entries);
}

fn deinitUserProfiles(allocator: std.mem.Allocator, profiles: []UserProfile) void {
    for (profiles) |*profile| profile.deinit(allocator);
    allocator.free(profiles);
}

fn rejectDuplicateI32(keys: []i32) DecodeError!void {
    if (keys.len < 2) return;
    std.sort.heap(i32, keys, {}, lessThanI32);
    for (keys[1..], keys[0 .. keys.len - 1]) |current, previous| {
        if (current == previous) return error.DuplicateKey;
    }
}

fn rejectDuplicateI64(keys: []i64) DecodeError!void {
    if (keys.len < 2) return;
    std.sort.heap(i64, keys, {}, lessThanI64);
    for (keys[1..], keys[0 .. keys.len - 1]) |current, previous| {
        if (current == previous) return error.DuplicateKey;
    }
}

fn lessThanI32(_: void, lhs: i32, rhs: i32) bool {
    return lhs < rhs;
}

fn lessThanI64(_: void, lhs: i64, rhs: i64) bool {
    return lhs < rhs;
}

fn leI32(value: i32) [4]u8 {
    var bytes: [4]u8 = undefined;
    std.mem.writeInt(i32, &bytes, value, .little);
    return bytes;
}

fn leI64(value: i64) [8]u8 {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(i64, &bytes, value, .little);
    return bytes;
}

const me_fixture =
    // MatchingEngineRouter: shard id/mask.
    leI32(1) ++ leI64(3) ++
    // BinaryCommandsProcessor: transfer 7, one of two allocated words filled.
    leI32(1) ++ leI64(7) ++ leI32(1) ++ leI32(2) ++
    leI64(0x0102_0304_0506_0708) ++ leI64(-2) ++
    // orderBooks: one naive book, keyed by the embedded symbol id.
    leI32(1) ++ leI32(1001) ++ [_]u8{0} ++
    // CoreSymbolSpecification.
    leI32(1001) ++ [_]u8{0} ++ leI32(11) ++ leI32(15) ++
    leI64(100) ++ leI64(10) ++ leI64(3) ++ leI64(2) ++ leI64(0) ++ leI64(0) ++
    // askBuckets: key/embedded price, one keyed order, total volume.
    leI32(1) ++ leI64(101) ++ leI64(101) ++ leI32(1) ++ leI64(9001) ++
    leI64(9001) ++ leI64(101) ++ leI64(10) ++ leI64(2) ++ leI64(0) ++
    [_]u8{0} ++ leI64(501) ++ leI64(1234) ++ leI64(8) ++
    // bidBuckets.
    leI32(0);

const direct_me_fixture =
    leI32(0) ++ leI64(0) ++
    // Empty BinaryCommandsProcessor and one direct order book.
    leI32(0) ++ leI32(1) ++ leI32(3003) ++ [_]u8{2} ++
    // CoreSymbolSpecification.
    leI32(3003) ++ [_]u8{2} ++ leI32(21) ++ leI32(22) ++
    leI64(1000) ++ leI64(5) ++ leI64(2) ++ leI64(1) ++ leI64(30) ++ leI64(40) ++
    // One ask followed by one bid under the single direct-book order count.
    leI32(2) ++
    leI64(70) ++ leI64(500) ++ leI64(4) ++ leI64(1) ++ leI64(0) ++
    [_]u8{0} ++ leI64(700) ++ leI64(900) ++
    leI64(71) ++ leI64(490) ++ leI64(6) ++ leI64(0) ++ leI64(510) ++
    [_]u8{1} ++ leI64(701) ++ leI64(901);

const re_fixture =
    // RiskEngine: shard id/mask.
    leI32(0) ++ leI64(1) ++
    // SymbolSpecificationProvider: one keyed specification.
    leI32(1) ++ leI32(2002) ++ leI32(2002) ++ [_]u8{1} ++
    leI32(15) ++ leI32(15) ++ leI64(1) ++ leI64(1) ++
    leI64(4) ++ leI64(3) ++ leI64(100) ++ leI64(120) ++
    // UserProfileService: user map key and embedded uid.
    leI32(1) ++ leI64(42) ++ leI64(42) ++
    // positions: map key and embedded symbol, then position body.
    leI32(1) ++ leI32(2002) ++ leI32(2002) ++ leI32(15) ++ [_]u8{0xff} ++
    leI64(7) ++ leI64(700) ++ leI64(9) ++ leI64(2) ++ leI64(3) ++
    // adjustment counter, accounts, status.
    leI64(55) ++ leI32(1) ++ leI32(15) ++ leI64(10_000) ++ [_]u8{1} ++
    // Empty BinaryCommandsProcessor.
    leI32(0) ++
    // last prices.
    leI32(1) ++ leI32(2002) ++ leI64(105) ++ leI64(99) ++
    // fees, adjustments, suspends.
    leI32(1) ++ leI32(15) ++ leI64(8) ++
    leI32(1) ++ leI32(15) ++ leI64(-10_000) ++
    leI32(1) ++ leI32(15) ++ leI64(25);

test "exact matching-engine Chronicle body fixture decodes nested binary and naive book state" {
    const allocator = std.testing.allocator;
    var state = try decodeMatchingEngineBody(
        allocator,
        &me_fixture,
        .{ .shard_id = 1, .shard_mask = 3 },
        .{},
    );
    defer state.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), state.binary_commands.incoming_transfers.len);
    const transfer = state.binary_commands.incoming_transfers[0];
    try std.testing.expectEqual(@as(i64, 7), transfer.transfer_id);
    try std.testing.expectEqual(@as(i32, 1), transfer.words_transferred);
    try std.testing.expectEqualSlices(i64, &.{ 0x0102_0304_0506_0708, -2 }, transfer.data);

    try std.testing.expectEqual(@as(usize, 1), state.order_books.len);
    try std.testing.expectEqual(@as(i32, 1001), state.order_books[0].symbol_id);
    const naive = state.order_books[0].book.naive;
    try std.testing.expectEqual(SymbolType.currency_exchange_pair, naive.symbol_specification.symbol_type);
    try std.testing.expectEqual(@as(usize, 1), naive.ask_buckets.len);
    try std.testing.expectEqual(@as(i64, 101), naive.ask_buckets[0].price);
    try std.testing.expectEqual(@as(i64, 8), naive.ask_buckets[0].total_volume);
    try std.testing.expectEqual(@as(i64, 9001), naive.ask_buckets[0].orders[0].order_id);
    try std.testing.expectEqual(OrderAction.ask, naive.ask_buckets[0].orders[0].action);
    try std.testing.expectEqual(@as(usize, 0), naive.bid_buckets.len);
}

test "exact matching-engine direct-book fixture preserves ask then bid stream" {
    const allocator = std.testing.allocator;
    var state = try decodeMatchingEngineBody(
        allocator,
        &direct_me_fixture,
        .{ .shard_id = 0, .shard_mask = 0 },
        .{},
    );
    defer state.deinit(allocator);

    const direct = state.order_books[0].book.direct;
    try std.testing.expectEqual(@as(i32, 3003), direct.symbol_specification.symbol_id);
    try std.testing.expectEqual(SymbolType.option, direct.symbol_specification.symbol_type);
    try std.testing.expectEqual(@as(usize, 2), direct.orders.len);
    try std.testing.expectEqual(OrderAction.ask, direct.orders[0].action);
    try std.testing.expectEqual(@as(i64, 70), direct.orders[0].order_id);
    try std.testing.expectEqual(OrderAction.bid, direct.orders[1].action);
    try std.testing.expectEqual(@as(i64, 71), direct.orders[1].order_id);
}

test "exact risk-engine Chronicle body fixture decodes every top-level component" {
    const allocator = std.testing.allocator;
    var state = try decodeRiskEngineBody(
        allocator,
        &re_fixture,
        .{ .shard_id = 0, .shard_mask = 1 },
        .{},
    );
    defer state.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), state.symbol_specifications.len);
    try std.testing.expectEqual(SymbolType.futures_contract, state.symbol_specifications[0].symbol_type);
    try std.testing.expectEqual(@as(usize, 1), state.user_profiles.len);
    const profile = state.user_profiles[0];
    try std.testing.expectEqual(@as(i64, 42), profile.uid);
    try std.testing.expectEqual(UserStatus.suspended, profile.status);
    try std.testing.expectEqual(PositionDirection.short, profile.positions[0].direction);
    try std.testing.expectEqual(@as(i64, 10_000), profile.accounts[0].value);
    try std.testing.expectEqual(@as(usize, 0), state.binary_commands.incoming_transfers.len);
    try std.testing.expectEqual(@as(i64, 105), state.last_prices[0].ask_price);
    try std.testing.expectEqual(@as(i64, 8), state.fees[0].value);
    try std.testing.expectEqual(@as(i64, -10_000), state.adjustments[0].value);
    try std.testing.expectEqual(@as(i64, 25), state.suspends[0].value);
}

test "strict body boundaries shard identity and implementation codes are enforced" {
    const allocator = std.testing.allocator;
    const expected: ShardIdentity = .{ .shard_id = 1, .shard_mask = 3 };

    try std.testing.expectError(
        error.UnexpectedEnd,
        decodeMatchingEngineBody(allocator, me_fixture[0 .. me_fixture.len - 1], expected, .{}),
    );
    const trailing = me_fixture ++ [_]u8{0};
    try std.testing.expectError(
        error.TrailingBytes,
        decodeMatchingEngineBody(allocator, &trailing, expected, .{}),
    );
    try std.testing.expectError(
        error.ShardIdMismatch,
        decodeMatchingEngineBody(allocator, &me_fixture, .{ .shard_id = 0, .shard_mask = 3 }, .{}),
    );
    try std.testing.expectError(
        error.ShardMaskMismatch,
        decodeMatchingEngineBody(allocator, &me_fixture, .{ .shard_id = 1, .shard_mask = 7 }, .{}),
    );

    var invalid_implementation = me_fixture;
    // 12-byte shard identity + 36-byte binary state + 4-byte book count/key.
    invalid_implementation[56] = 1;
    try std.testing.expectError(
        error.InvalidOrderBookImplementation,
        decodeMatchingEngineBody(allocator, &invalid_implementation, expected, .{}),
    );
}

test "map identity keys duplicates and configured bounds are enforced" {
    const allocator = std.testing.allocator;
    const expected: ShardIdentity = .{ .shard_id = 1, .shard_mask = 3 };

    var mismatched_symbol = me_fixture;
    std.mem.writeInt(i32, mismatched_symbol[52..56], 1002, .little);
    try std.testing.expectError(
        error.OrderBookSymbolKeyMismatch,
        decodeMatchingEngineBody(allocator, &mismatched_symbol, expected, .{}),
    );

    try std.testing.expectError(
        error.BodyLimitExceeded,
        decodeMatchingEngineBody(allocator, &me_fixture, expected, .{ .max_body_bytes = me_fixture.len - 1 }),
    );
    try std.testing.expectError(
        error.CollectionLimitExceeded,
        decodeMatchingEngineBody(allocator, &me_fixture, expected, .{ .max_collection_entries = 0 }),
    );
    try std.testing.expectError(
        error.AllocationBudgetExceeded,
        decodeMatchingEngineBody(allocator, &me_fixture, expected, .{ .max_allocated_bytes = 0 }),
    );

    const duplicate_transfer_fixture =
        leI32(0) ++ leI64(0) ++
        leI32(2) ++
        leI64(9) ++ leI32(0) ++ leI32(0) ++
        leI64(9) ++ leI32(0) ++ leI32(0) ++
        leI32(0);
    try std.testing.expectError(
        error.DuplicateKey,
        decodeMatchingEngineBody(
            allocator,
            &duplicate_transfer_fixture,
            .{ .shard_id = 0, .shard_mask = 0 },
            .{},
        ),
    );
}

fn matchingAllocationTest(allocator: std.mem.Allocator, body: []const u8) !void {
    var state = try decodeMatchingEngineBody(
        allocator,
        body,
        .{ .shard_id = 1, .shard_mask = 3 },
        .{},
    );
    defer state.deinit(allocator);
}

fn riskAllocationTest(allocator: std.mem.Allocator, body: []const u8) !void {
    var state = try decodeRiskEngineBody(
        allocator,
        body,
        .{ .shard_id = 0, .shard_mask = 1 },
        .{},
    );
    defer state.deinit(allocator);
}

test "both body decoders clean up every allocation-failure path" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        matchingAllocationTest,
        .{me_fixture[0..]},
    );
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        riskAllocationTest,
        .{re_fixture[0..]},
    );
}
