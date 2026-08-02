const std = @import("std");
const exchange_core = @import("exchange_core");
const binary_frames = exchange_core.core.binary_frames;
const common = exchange_core.core.common;
const int_map_order = exchange_core.core.java_compat.eclipse_collections.int_map_order;
const java_body = @import("snapshot_body.zig");
const kernel_mod = exchange_core.core.exchange_kernel;
const long_map_order = exchange_core.core.java_compat.eclipse_collections.long_map_order;
const risk_mod = exchange_core.core.risk;
const sharded_state = exchange_core.core.sharded_snapshot_state;
const snapshot = exchange_core.core.snapshot;
const user = exchange_core.core.user_profile;

pub const Request = struct {
    matching_states: []const java_body.MatchingEngineState,
    risk_states: []const java_body.RiskEngineState,
    matching_shard_count: usize,
    risk_shard_count: usize,
    base_config: kernel_mod.Config = .{},
    publication_frontier: u64,
};

pub const ImportError = std.mem.Allocator.Error || error{
    InvalidMatchingShardCount,
    InvalidRiskShardCount,
    MatchingShardCountMismatch,
    RiskShardCountMismatch,
    MatchingShardIdOutOfRange,
    RiskShardIdOutOfRange,
    MatchingShardMaskMismatch,
    RiskShardMaskMismatch,
    DuplicateMatchingShard,
    DuplicateRiskShard,
    InvalidKernelConfiguration,
    DuplicateSymbol,
    MissingSymbolReplica,
    SymbolSpecificationMismatch,
    OrderBookKindMismatch,
    WrongSymbolOwner,
    InvalidOrder,
    InvalidNaiveBucket,
    DuplicateMapKey,
    WrongUidOwner,
    PositionSymbolMissing,
    LastPriceReplicaMismatch,
    InvalidUser,
    InvalidTransfer,
    BinaryProcessorMismatch,
    MatchingRestoreFailed,
    RiskRestoreFailed,
};

const MatchingIndex = []*const java_body.MatchingEngineState;
const RiskIndex = []*const java_body.RiskEngineState;

const PendingBuild = struct {
    state: binary_frames.PendingState,
    native_state_hash: u64,
    java_state_hash: i32,
};

const RiskHistoryEncounterBuffers = struct {
    allocator: std.mem.Allocator,
    user_uids: []i64,
    users: []sharded_state.UserHistoryEncounter,
    fee_keys: []i32,
    adjustment_keys: []i32,
    suspend_keys: []i32,

    fn deinit(self: *RiskHistoryEncounterBuffers) void {
        for (self.users) |encounter| {
            self.allocator.free(encounter.account_keys);
            self.allocator.free(encounter.position_keys);
        }
        self.allocator.free(self.user_uids);
        self.allocator.free(self.users);
        self.allocator.free(self.fee_keys);
        self.allocator.free(self.adjustment_keys);
        self.allocator.free(self.suspend_keys);
        self.* = undefined;
    }

    fn view(self: *const RiskHistoryEncounterBuffers) sharded_state.RiskHistoryEncounters {
        return .{
            .user_uids = self.user_uids,
            .users = self.users,
            .fee_keys = self.fee_keys,
            .adjustment_keys = self.adjustment_keys,
            .suspend_keys = self.suspend_keys,
        };
    }
};

/// Converts a complete legacy Java shard family into a newly allocated native
/// set. The returned set is the only observable mutation: every intermediate
/// target is private to this function and is deinitialized on any error.
pub fn importFresh(
    allocator: std.mem.Allocator,
    request: Request,
) ImportError!kernel_mod.MultiShardKernelSet {
    const matching = try indexMatchingStates(
        allocator,
        request.matching_states,
        request.matching_shard_count,
    );
    defer allocator.free(matching);
    const risks = try indexRiskStates(
        allocator,
        request.risk_states,
        request.risk_shard_count,
    );
    defer allocator.free(risks);

    var matching_specs: std.AutoHashMapUnmanaged(i32, common.CoreSymbolSpecification) = .empty;
    defer matching_specs.deinit(allocator);
    try validateMatchingFamily(
        allocator,
        matching,
        request.matching_shard_count,
        request.base_config.orderbook_kind,
        &matching_specs,
    );
    try validateRiskFamily(
        allocator,
        risks,
        request.risk_shard_count,
        &matching_specs,
    );
    try validateBinaryReplicas(allocator, matching, risks);

    var staged = kernel_mod.MultiShardKernelSet.initWithTopology(
        allocator,
        request.matching_shard_count,
        request.risk_shard_count,
        request.base_config,
    ) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidKernelConfiguration,
    };
    errdefer staged.deinit();

    var java_binary_state_hash: ?i32 = null;

    // Matching first is mandatory: its public restore path temporarily uses
    // the embedded carrier risk engine, then clears it. Active risk shards can
    // alias those engines and therefore must be restored afterwards.
    for (matching, 0..) |legacy, shard_id| {
        var legacy_binary_hash: i32 = undefined;
        var native = try buildMatchingState(
            allocator,
            legacy,
            request.base_config.orderbook_kind,
            &legacy_binary_hash,
        );
        defer native.deinit();
        try requireBinaryConsensus(&java_binary_state_hash, legacy_binary_hash);
        try restoreMatchingState(
            allocator,
            &native,
            staged.matchingShardAt(shard_id),
            shard_id,
            request.risk_shard_count,
            request.base_config,
        );
    }

    for (risks, 0..) |legacy, shard_id| {
        var legacy_binary_hash: i32 = undefined;
        var native = try buildRiskState(allocator, legacy, &legacy_binary_hash);
        defer native.deinit();
        try requireBinaryConsensus(&java_binary_state_hash, legacy_binary_hash);
        try restoreRiskState(
            allocator,
            &native,
            staged.riskEngineAt(shard_id),
            &staged.risk_shards[shard_id].binary_frame_assembler,
            shard_id,
            request.risk_shard_count,
            request.base_config.risk,
        );
    }

    // No fallible operation may follow this publication step.
    staged.restorePublicationSequence(request.publication_frontier);
    return staged;
}

fn validateBinaryReplicas(
    allocator: std.mem.Allocator,
    matching: MatchingIndex,
    risks: RiskIndex,
) ImportError!void {
    var canonical: std.AutoHashMapUnmanaged(i64, *const java_body.TransferRecord) = .empty;
    defer canonical.deinit(allocator);
    const first = matching[0].binary_commands.incoming_transfers;
    try canonical.ensureTotalCapacity(allocator, @intCast(first.len));
    for (first) |*record| {
        if (std.math.cast(i32, record.transfer_id) == null or
            record.words_transferred <= 0)
        {
            return error.InvalidTransfer;
        }
        const word_count: usize = @intCast(record.words_transferred);
        if (word_count > record.data.len or word_count % binary_frames.FrameWordsPerCommand != 0) {
            return error.InvalidTransfer;
        }
        const gop = try canonical.getOrPut(allocator, record.transfer_id);
        if (gop.found_existing) return error.InvalidTransfer;
        gop.value_ptr.* = record;
    }
    for (matching) |state| {
        try requireTransferReplica(allocator, &canonical, state.binary_commands.incoming_transfers);
    }
    for (risks) |state| {
        try requireTransferReplica(allocator, &canonical, state.binary_commands.incoming_transfers);
    }
}

fn requireTransferReplica(
    allocator: std.mem.Allocator,
    canonical: *const std.AutoHashMapUnmanaged(i64, *const java_body.TransferRecord),
    transfers: []const java_body.TransferRecord,
) ImportError!void {
    if (transfers.len != canonical.count()) return error.BinaryProcessorMismatch;
    var seen: std.AutoHashMapUnmanaged(i64, void) = .empty;
    defer seen.deinit(allocator);
    try seen.ensureTotalCapacity(allocator, @intCast(transfers.len));
    for (transfers) |record| {
        const expected = canonical.get(record.transfer_id) orelse
            return error.BinaryProcessorMismatch;
        const gop = try seen.getOrPut(allocator, record.transfer_id);
        if (gop.found_existing) return error.InvalidTransfer;
        if (record.words_transferred != expected.words_transferred or
            !std.mem.eql(i64, record.data, expected.data))
        {
            return error.BinaryProcessorMismatch;
        }
    }
}

fn indexMatchingStates(
    allocator: std.mem.Allocator,
    states: []const java_body.MatchingEngineState,
    expected_count: usize,
) ImportError!MatchingIndex {
    if (!validShardCount(expected_count)) return error.InvalidMatchingShardCount;
    if (states.len != expected_count) return error.MatchingShardCountMismatch;
    const expected_mask = shardMaskI64(expected_count) orelse
        return error.InvalidMatchingShardCount;
    const indexed = try allocator.alloc(*const java_body.MatchingEngineState, expected_count);
    errdefer allocator.free(indexed);
    const seen = try allocator.alloc(bool, expected_count);
    defer allocator.free(seen);
    @memset(seen, false);

    for (states) |*state| {
        if (state.shard.shard_mask != expected_mask) return error.MatchingShardMaskMismatch;
        if (state.shard.shard_id < 0) return error.MatchingShardIdOutOfRange;
        const shard_id: usize = @intCast(state.shard.shard_id);
        if (shard_id >= expected_count) return error.MatchingShardIdOutOfRange;
        if (seen[shard_id]) return error.DuplicateMatchingShard;
        seen[shard_id] = true;
        indexed[shard_id] = state;
    }
    return indexed;
}

fn indexRiskStates(
    allocator: std.mem.Allocator,
    states: []const java_body.RiskEngineState,
    expected_count: usize,
) ImportError!RiskIndex {
    if (!validShardCount(expected_count)) return error.InvalidRiskShardCount;
    if (states.len != expected_count) return error.RiskShardCountMismatch;
    const expected_mask = shardMaskI64(expected_count) orelse
        return error.InvalidRiskShardCount;
    const indexed = try allocator.alloc(*const java_body.RiskEngineState, expected_count);
    errdefer allocator.free(indexed);
    const seen = try allocator.alloc(bool, expected_count);
    defer allocator.free(seen);
    @memset(seen, false);

    for (states) |*state| {
        if (state.shard.shard_mask != expected_mask) return error.RiskShardMaskMismatch;
        if (state.shard.shard_id < 0) return error.RiskShardIdOutOfRange;
        const shard_id: usize = @intCast(state.shard.shard_id);
        if (shard_id >= expected_count) return error.RiskShardIdOutOfRange;
        if (seen[shard_id]) return error.DuplicateRiskShard;
        seen[shard_id] = true;
        indexed[shard_id] = state;
    }
    return indexed;
}

fn validShardCount(count: usize) bool {
    return count != 0 and @popCount(count) == 1 and shardMaskI64(count) != null;
}

fn shardMaskI64(count: usize) ?i64 {
    if (count == 0) return null;
    return std.math.cast(i64, count - 1);
}

fn validateMatchingFamily(
    allocator: std.mem.Allocator,
    states: MatchingIndex,
    matching_count: usize,
    target_kind: kernel_mod.OrderBookKind,
    canonical: *std.AutoHashMapUnmanaged(i32, common.CoreSymbolSpecification),
) ImportError!void {
    const first = states[0];
    try canonical.ensureTotalCapacity(allocator, @intCast(first.order_books.len));
    for (first.order_books) |entry| {
        if (entry.symbol_id != entry.book.symbolId()) return error.SymbolSpecificationMismatch;
        try requireBookKind(entry.book, target_kind);
        const specification = try nativeSpecification(bookSpecification(entry.book));
        const gop = try canonical.getOrPut(allocator, entry.symbol_id);
        if (gop.found_existing) return error.DuplicateSymbol;
        gop.value_ptr.* = specification;
    }

    for (states, 0..) |state, shard_id| {
        if (state.order_books.len != canonical.count()) return error.MissingSymbolReplica;
        var seen: std.AutoHashMapUnmanaged(i32, void) = .empty;
        defer seen.deinit(allocator);
        try seen.ensureTotalCapacity(allocator, @intCast(state.order_books.len));

        for (state.order_books) |entry| {
            if (entry.symbol_id != entry.book.symbolId()) return error.SymbolSpecificationMismatch;
            try requireBookKind(entry.book, target_kind);
            const specification = try nativeSpecification(bookSpecification(entry.book));
            const expected = canonical.get(entry.symbol_id) orelse return error.MissingSymbolReplica;
            if (!std.meta.eql(expected, specification)) return error.SymbolSpecificationMismatch;
            const gop = try seen.getOrPut(allocator, entry.symbol_id);
            if (gop.found_existing) return error.DuplicateSymbol;

            const order_count = try validateLegacyBook(allocator, entry.book);
            if (order_count != 0 and symbolOwner(entry.symbol_id, matching_count) != shard_id) {
                return error.WrongSymbolOwner;
            }
        }
    }
}

fn validateRiskFamily(
    allocator: std.mem.Allocator,
    states: RiskIndex,
    risk_count: usize,
    matching_specs: *const std.AutoHashMapUnmanaged(i32, common.CoreSymbolSpecification),
) ImportError!void {
    var canonical: std.AutoHashMapUnmanaged(i32, common.CoreSymbolSpecification) = .empty;
    defer canonical.deinit(allocator);
    try canonical.ensureTotalCapacity(allocator, @intCast(states[0].symbol_specifications.len));
    for (states[0].symbol_specifications) |legacy_spec| {
        const specification = try nativeSpecification(legacy_spec);
        const matching_spec = matching_specs.get(specification.symbol_id) orelse
            return error.SymbolSpecificationMismatch;
        if (!std.meta.eql(matching_spec, specification)) return error.SymbolSpecificationMismatch;
        const gop = try canonical.getOrPut(allocator, specification.symbol_id);
        if (gop.found_existing) return error.DuplicateSymbol;
        gop.value_ptr.* = specification;
    }

    var canonical_prices: std.AutoHashMapUnmanaged(i32, java_body.LastPriceEntry) = .empty;
    defer canonical_prices.deinit(allocator);
    try canonical_prices.ensureTotalCapacity(allocator, @intCast(states[0].last_prices.len));
    for (states[0].last_prices) |price| {
        if (!canonical.contains(price.symbol_id)) return error.PositionSymbolMissing;
        const gop = try canonical_prices.getOrPut(allocator, price.symbol_id);
        if (gop.found_existing) return error.DuplicateMapKey;
        gop.value_ptr.* = price;
    }

    for (states, 0..) |state, shard_id| {
        if (state.symbol_specifications.len != canonical.count()) return error.MissingSymbolReplica;
        var seen: std.AutoHashMapUnmanaged(i32, void) = .empty;
        defer seen.deinit(allocator);
        try seen.ensureTotalCapacity(allocator, @intCast(state.symbol_specifications.len));
        for (state.symbol_specifications) |legacy_spec| {
            const specification = try nativeSpecification(legacy_spec);
            const expected = canonical.get(specification.symbol_id) orelse
                return error.MissingSymbolReplica;
            if (!std.meta.eql(expected, specification)) return error.SymbolSpecificationMismatch;
            const gop = try seen.getOrPut(allocator, specification.symbol_id);
            if (gop.found_existing) return error.DuplicateSymbol;
        }

        for (state.user_profiles) |profile| {
            if (uidOwner(profile.uid, risk_count) != shard_id) return error.WrongUidOwner;
        }
        if (state.last_prices.len != canonical_prices.count()) {
            return error.LastPriceReplicaMismatch;
        }
        var seen_prices: std.AutoHashMapUnmanaged(i32, void) = .empty;
        defer seen_prices.deinit(allocator);
        try seen_prices.ensureTotalCapacity(allocator, @intCast(state.last_prices.len));
        for (state.last_prices) |price| {
            if (!canonical.contains(price.symbol_id)) return error.PositionSymbolMissing;
            const expected = canonical_prices.get(price.symbol_id) orelse
                return error.LastPriceReplicaMismatch;
            if (!std.meta.eql(expected, price)) return error.LastPriceReplicaMismatch;
            const gop = try seen_prices.getOrPut(allocator, price.symbol_id);
            if (gop.found_existing) return error.DuplicateMapKey;
        }
    }
}

fn requireBookKind(
    book: java_body.OrderBook,
    target: kernel_mod.OrderBookKind,
) ImportError!void {
    switch (book) {
        .naive => if (target != .naive) return error.OrderBookKindMismatch,
        .direct => if (target != .direct) return error.OrderBookKindMismatch,
    }
}

fn bookSpecification(book: java_body.OrderBook) java_body.CoreSymbolSpecification {
    return switch (book) {
        .naive => |value| value.symbol_specification,
        .direct => |value| value.symbol_specification,
    };
}

fn nativeSpecification(
    legacy: java_body.CoreSymbolSpecification,
) ImportError!common.CoreSymbolSpecification {
    const symbol_type = common.SymbolType.fromCode(@intFromEnum(legacy.symbol_type)) catch
        return error.SymbolSpecificationMismatch;
    const specification: common.CoreSymbolSpecification = .{
        .symbol_id = legacy.symbol_id,
        .symbol_type = symbol_type,
        .base_currency = legacy.base_currency,
        .quote_currency = legacy.quote_currency,
        .base_scale_k = legacy.base_scale_k,
        .quote_scale_k = legacy.quote_scale_k,
        .taker_fee = legacy.taker_fee,
        .maker_fee = legacy.maker_fee,
        .margin_buy = legacy.margin_buy,
        .margin_sell = legacy.margin_sell,
    };
    specification.validate() catch return error.SymbolSpecificationMismatch;
    return specification;
}

fn validateLegacyBook(
    allocator: std.mem.Allocator,
    book: java_body.OrderBook,
) ImportError!usize {
    var seen_orders: std.AutoHashMapUnmanaged(i64, void) = .empty;
    defer seen_orders.deinit(allocator);

    return switch (book) {
        .direct => |direct| validateDirectOrders(allocator, direct.orders, &seen_orders),
        .naive => |naive| blk: {
            const ask_count = try validateNaiveBuckets(allocator, naive.ask_buckets, .ask, &seen_orders);
            const bid_count = try validateNaiveBuckets(allocator, naive.bid_buckets, .bid, &seen_orders);
            break :blk std.math.add(usize, ask_count, bid_count) catch return error.InvalidOrder;
        },
    };
}

fn validateDirectOrders(
    allocator: std.mem.Allocator,
    orders: []const java_body.Order,
    seen_orders: *std.AutoHashMapUnmanaged(i64, void),
) ImportError!usize {
    try seen_orders.ensureTotalCapacity(allocator, @intCast(orders.len));
    var phase: java_body.OrderAction = .ask;
    var prior_price: ?i64 = null;
    for (orders) |order| {
        try validateLegacyOrder(order, seen_orders, allocator);
        if (order.action != phase) {
            if (phase != .ask or order.action != .bid) return error.InvalidOrder;
            phase = .bid;
            prior_price = null;
        }
        if (prior_price) |prior| {
            if ((phase == .ask and order.price < prior) or
                (phase == .bid and order.price > prior))
            {
                return error.InvalidOrder;
            }
        }
        prior_price = order.price;
    }
    return orders.len;
}

fn validateNaiveBuckets(
    allocator: std.mem.Allocator,
    buckets: []const java_body.NaiveBucket,
    action: java_body.OrderAction,
    seen_orders: *std.AutoHashMapUnmanaged(i64, void),
) ImportError!usize {
    var total_orders: usize = 0;
    var prior_price: ?i64 = null;
    for (buckets) |bucket| {
        if (bucket.orders.len == 0) return error.InvalidNaiveBucket;
        if (prior_price) |prior| {
            if ((action == .ask and bucket.price <= prior) or
                (action == .bid and bucket.price >= prior))
            {
                return error.InvalidNaiveBucket;
            }
        }
        prior_price = bucket.price;

        var calculated_volume: i64 = 0;
        for (bucket.orders) |order| {
            if (order.action != action or order.price != bucket.price) {
                return error.InvalidNaiveBucket;
            }
            try validateLegacyOrder(order, seen_orders, allocator);
            calculated_volume = std.math.add(i64, calculated_volume, order.size - order.filled) catch
                return error.InvalidNaiveBucket;
        }
        if (calculated_volume <= 0 or calculated_volume != bucket.total_volume) {
            return error.InvalidNaiveBucket;
        }
        total_orders = std.math.add(usize, total_orders, bucket.orders.len) catch
            return error.InvalidOrder;
    }
    return total_orders;
}

fn validateLegacyOrder(
    order: java_body.Order,
    seen_orders: *std.AutoHashMapUnmanaged(i64, void),
    allocator: std.mem.Allocator,
) ImportError!void {
    if (order.size <= 0 or order.filled < 0 or order.filled >= order.size) {
        return error.InvalidOrder;
    }
    const gop = try seen_orders.getOrPut(allocator, order.order_id);
    if (gop.found_existing) return error.InvalidOrder;
}

fn buildMatchingState(
    allocator: std.mem.Allocator,
    legacy: *const java_body.MatchingEngineState,
    target_kind: kernel_mod.OrderBookKind,
    java_binary_state_hash: *i32,
) ImportError!sharded_state.MatchingState {
    const book_encounter_keys = try allocator.alloc(i32, legacy.order_books.len);
    defer allocator.free(book_encounter_keys);
    for (legacy.order_books, book_encounter_keys) |entry, *key| {
        key.* = entry.symbol_id;
    }

    const pending = try buildPendingState(allocator, legacy.binary_commands.incoming_transfers);
    var result: sharded_state.MatchingState = .{
        .allocator = allocator,
        .books_state_hash = 0,
        .binary_state_hash = pending.native_state_hash,
        .books_history = int_map_order.History.initUnavailable(),
        .pending = pending.state,
    };
    errdefer result.deinit();

    try result.books.ensureTotalCapacityPrecise(allocator, legacy.order_books.len);
    for (legacy.order_books) |entry| {
        try requireBookKind(entry.book, target_kind);
        var native_book: snapshot.BookSnapshot = .{
            .symbol_spec = try nativeSpecification(bookSpecification(entry.book)),
        };
        errdefer native_book.deinit(allocator);
        const count = try validateLegacyBook(allocator, entry.book);
        try native_book.orders.ensureTotalCapacityPrecise(allocator, count);
        try appendNativeOrders(&native_book.orders, entry.symbol_id, entry.book);
        result.books.appendAssumeCapacity(native_book);
    }
    std.sort.heap(snapshot.BookSnapshot, result.books.items, {}, lessThanBookSnapshot);
    if (result.books.items.len >= 2) {
        for (result.books.items[1..], result.books.items[0 .. result.books.items.len - 1]) |current, prior| {
            if (current.symbol_spec.symbol_id == prior.symbol_spec.symbol_id) return error.DuplicateSymbol;
        }
    }
    result.initializeBooksHistoryFromEncounterOrder(book_encounter_keys) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.MatchingRestoreFailed,
    };
    java_binary_state_hash.* = pending.java_state_hash;
    return result;
}

fn appendNativeOrders(
    out: *std.ArrayListUnmanaged(kernel_mod.RestingOrder),
    symbol: i32,
    book: java_body.OrderBook,
) ImportError!void {
    switch (book) {
        .direct => |direct| {
            for (direct.orders) |order| out.appendAssumeCapacity(try nativeOrder(symbol, order));
        },
        .naive => |naive| {
            for (naive.ask_buckets) |bucket| {
                for (bucket.orders) |order| out.appendAssumeCapacity(try nativeOrder(symbol, order));
            }
            for (naive.bid_buckets) |bucket| {
                for (bucket.orders) |order| out.appendAssumeCapacity(try nativeOrder(symbol, order));
            }
        },
    }
}

fn nativeOrder(symbol: i32, legacy: java_body.Order) ImportError!kernel_mod.RestingOrder {
    const action = common.OrderAction.fromCode(@intCast(@intFromEnum(legacy.action))) catch
        return error.InvalidOrder;
    return .{
        .symbol = symbol,
        .order_id = legacy.order_id,
        .price = legacy.price,
        .size = legacy.size,
        .filled = legacy.filled,
        .reserve_bid_price = legacy.reserve_bid_price,
        .action = action,
        .uid = legacy.uid,
        .timestamp = legacy.timestamp,
    };
}

fn buildRiskState(
    allocator: std.mem.Allocator,
    legacy: *const java_body.RiskEngineState,
    java_binary_state_hash: *i32,
) ImportError!sharded_state.RiskState {
    var history_encounters = try buildRiskHistoryEncounterBuffers(allocator, legacy);
    defer history_encounters.deinit();

    const pending = try buildPendingState(allocator, legacy.binary_commands.incoming_transfers);
    var result: sharded_state.RiskState = .{
        .allocator = allocator,
        .risk_state_hash = 0,
        .binary_state_hash = pending.native_state_hash,
        .user_profiles_history = long_map_order.History.initUnavailable(),
        .fees_history = int_map_order.History.initUnavailable(),
        .adjustments_history = int_map_order.History.initUnavailable(),
        .suspends_history = int_map_order.History.initUnavailable(),
        .pending = pending.state,
    };
    errdefer result.deinit();

    try result.symbols.ensureTotalCapacityPrecise(allocator, legacy.symbol_specifications.len);
    for (legacy.symbol_specifications) |specification| {
        result.symbols.appendAssumeCapacity(try nativeSpecification(specification));
    }
    std.sort.heap(common.CoreSymbolSpecification, result.symbols.items, {}, lessThanSpecification);
    try rejectDuplicateSpecifications(result.symbols.items);

    try result.users.ensureTotalCapacityPrecise(allocator, legacy.user_profiles.len);
    for (legacy.user_profiles) |profile| {
        var native_profile: snapshot.UserSnapshot = .{
            .uid = profile.uid,
            .user_status = common.UserStatus.fromCode(@intCast(@intFromEnum(profile.status))) catch
                return error.InvalidUser,
            .adjustments_counter = profile.adjustments_counter,
        };
        errdefer native_profile.deinit(allocator);

        try native_profile.accounts.ensureTotalCapacityPrecise(allocator, profile.accounts.len);
        for (profile.accounts) |entry| native_profile.accounts.appendAssumeCapacity(.{
            .key = entry.key,
            .value = entry.value,
        });
        std.sort.heap(snapshot.I32I64Entry, native_profile.accounts.items, {}, lessThanI32I64Entry);
        try rejectDuplicateI32I64(native_profile.accounts.items);

        try native_profile.positions.ensureTotalCapacityPrecise(allocator, profile.positions.len);
        for (profile.positions) |position| {
            const direction = common.PositionDirection.fromCode(@intFromEnum(position.direction)) catch
                return error.InvalidUser;
            const native_position: user.SymbolPositionRecord = .{
                .uid = profile.uid,
                .symbol = position.symbol,
                .currency = position.currency,
                .direction = direction,
                .open_volume = position.open_volume,
                .open_price_sum = position.open_price_sum,
                .profit = position.profit,
                .pending_sell_size = position.pending_sell_size,
                .pending_buy_size = position.pending_buy_size,
            };
            native_position.validateInternalState() catch return error.InvalidUser;
            native_profile.positions.appendAssumeCapacity(.{
                .symbol = position.symbol,
                .currency = position.currency,
                .direction = direction,
                .open_volume = position.open_volume,
                .open_price_sum = position.open_price_sum,
                .profit = position.profit,
                .pending_sell_size = position.pending_sell_size,
                .pending_buy_size = position.pending_buy_size,
            });
        }
        std.sort.heap(snapshot.PositionSnapshot, native_profile.positions.items, {}, lessThanPosition);
        try rejectDuplicatePositions(native_profile.positions.items);
        for (native_profile.positions.items) |position| {
            if (!containsSpecification(result.symbols.items, position.symbol)) {
                return error.PositionSymbolMissing;
            }
        }
        result.users.appendAssumeCapacity(native_profile);
    }
    std.sort.heap(snapshot.UserSnapshot, result.users.items, {}, lessThanUserSnapshot);
    try rejectDuplicateUsers(result.users.items);

    try result.last_prices.ensureTotalCapacityPrecise(allocator, legacy.last_prices.len);
    for (legacy.last_prices) |price| result.last_prices.appendAssumeCapacity(.{
        .symbol = price.symbol_id,
        .ask_price = price.ask_price,
        .bid_price = price.bid_price,
    });
    std.sort.heap(snapshot.LastPriceCacheEntry, result.last_prices.items, {}, lessThanLastPrice);
    try rejectDuplicateLastPrices(result.last_prices.items);
    for (result.last_prices.items) |price| {
        if (!containsSpecification(result.symbols.items, price.symbol)) {
            return error.PositionSymbolMissing;
        }
    }

    try appendSortedI32I64(allocator, &result.fees, legacy.fees);
    try appendSortedI32I64(allocator, &result.adjustments, legacy.adjustments);
    try appendSortedI32I64(allocator, &result.suspends, legacy.suspends);
    result.initializeHistoriesFromEncounterOrder(history_encounters.view()) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.RiskRestoreFailed,
    };
    java_binary_state_hash.* = pending.java_state_hash;
    return result;
}

fn buildRiskHistoryEncounterBuffers(
    allocator: std.mem.Allocator,
    legacy: *const java_body.RiskEngineState,
) std.mem.Allocator.Error!RiskHistoryEncounterBuffers {
    const user_uids = try allocator.alloc(i64, legacy.user_profiles.len);
    errdefer allocator.free(user_uids);
    const users = try allocator.alloc(
        sharded_state.UserHistoryEncounter,
        legacy.user_profiles.len,
    );
    var initialized_users: usize = 0;
    errdefer {
        for (users[0..initialized_users]) |encounter| {
            allocator.free(encounter.account_keys);
            allocator.free(encounter.position_keys);
        }
        allocator.free(users);
    }

    for (legacy.user_profiles, 0..) |profile, index| {
        user_uids[index] = profile.uid;
        users[index] = try buildUserHistoryEncounter(allocator, profile);
        initialized_users += 1;
    }

    const fee_keys = try copyIntLongEncounterKeys(allocator, legacy.fees);
    errdefer allocator.free(fee_keys);
    const adjustment_keys = try copyIntLongEncounterKeys(allocator, legacy.adjustments);
    errdefer allocator.free(adjustment_keys);
    const suspend_keys = try copyIntLongEncounterKeys(allocator, legacy.suspends);
    errdefer allocator.free(suspend_keys);

    return .{
        .allocator = allocator,
        .user_uids = user_uids,
        .users = users,
        .fee_keys = fee_keys,
        .adjustment_keys = adjustment_keys,
        .suspend_keys = suspend_keys,
    };
}

fn buildUserHistoryEncounter(
    allocator: std.mem.Allocator,
    profile: java_body.UserProfile,
) std.mem.Allocator.Error!sharded_state.UserHistoryEncounter {
    const account_keys = try copyIntLongEncounterKeys(allocator, profile.accounts);
    errdefer allocator.free(account_keys);
    const position_keys = try allocator.alloc(i32, profile.positions.len);
    errdefer allocator.free(position_keys);
    for (profile.positions, position_keys) |position, *key| {
        key.* = position.symbol;
    }
    return .{
        .uid = profile.uid,
        .account_keys = account_keys,
        .position_keys = position_keys,
    };
}

fn copyIntLongEncounterKeys(
    allocator: std.mem.Allocator,
    entries: []const java_body.IntLongEntry,
) std.mem.Allocator.Error![]i32 {
    const keys = try allocator.alloc(i32, entries.len);
    for (entries, keys) |entry, *key| key.* = entry.key;
    return keys;
}

fn buildPendingState(
    allocator: std.mem.Allocator,
    legacy: []const java_body.TransferRecord,
) ImportError!PendingBuild {
    const views = try allocator.alloc(binary_frames.LegacyUntypedPendingTransfer, legacy.len);
    defer allocator.free(views);
    for (legacy, views) |record, *view| view.* = .{
        .transfer_id = record.transfer_id,
        .words_transferred = record.words_transferred,
        .data = record.data,
    };

    var assembler = binary_frames.BinaryFrameAssembler.init(allocator);
    defer assembler.deinit();
    assembler.restoreLegacyUntypedPendingState(views) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidTransfer,
    };
    var state = try assembler.capturePendingState(allocator);
    errdefer state.deinit();
    const native_state_hash = assembler.stateHash() catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidTransfer,
    };
    const java_state_hash = assembler.javaStateHash32() catch return error.InvalidTransfer;
    return .{
        .state = state,
        .native_state_hash = native_state_hash,
        .java_state_hash = java_state_hash,
    };
}

fn restoreMatchingState(
    allocator: std.mem.Allocator,
    state: *sharded_state.MatchingState,
    target: *kernel_mod.ExchangeKernel,
    shard_id: usize,
    risk_count: usize,
    base_config: kernel_mod.Config,
) ImportError!void {
    var probe_config = base_config;
    probe_config.shard_id = std.math.cast(u64, shard_id) orelse
        return error.InvalidMatchingShardCount;
    probe_config.num_shards = std.math.cast(u64, risk_count) orelse
        return error.InvalidRiskShardCount;
    var probe = kernel_mod.ExchangeKernel.init(allocator, probe_config) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidKernelConfiguration,
    };
    defer probe.deinit();

    state.restoreIntoFreshKernel(&probe) catch |err| switch (err) {
        error.MatchingStateHashMismatch => {},
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.MatchingRestoreFailed,
    };
    state.books_state_hash = sharded_state.matchingBooksStateHash(&probe) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.MatchingRestoreFailed,
    };
    state.restoreIntoFreshKernel(target) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.MatchingRestoreFailed,
    };
}

fn restoreRiskState(
    allocator: std.mem.Allocator,
    state: *sharded_state.RiskState,
    target_engine: *risk_mod.RiskEngine,
    target_assembler: *binary_frames.BinaryFrameAssembler,
    shard_id: usize,
    risk_count: usize,
    risk_config: risk_mod.Config,
) ImportError!void {
    const shard_id_u64 = std.math.cast(u64, shard_id) orelse
        return error.InvalidRiskShardCount;
    const risk_count_u64 = std.math.cast(u64, risk_count) orelse
        return error.InvalidRiskShardCount;
    var probe_engine = risk_mod.RiskEngine.init(
        allocator,
        shard_id_u64,
        risk_count_u64,
        risk_config,
    ) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidKernelConfiguration,
    };
    defer probe_engine.deinit();
    var probe_assembler = binary_frames.BinaryFrameAssembler.init(allocator);
    defer probe_assembler.deinit();

    state.restoreIntoFreshRisk(&probe_engine, &probe_assembler) catch |err| switch (err) {
        error.RiskStateHashMismatch => {},
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.RiskRestoreFailed,
    };
    state.risk_state_hash = probe_engine.stateHash() catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.RiskRestoreFailed,
    };
    state.restoreIntoFreshRisk(target_engine, target_assembler) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.RiskRestoreFailed,
    };
}

fn requireBinaryConsensus(expected: *?i32, actual: i32) ImportError!void {
    if (expected.*) |value| {
        if (value != actual) return error.BinaryProcessorMismatch;
    } else {
        expected.* = actual;
    }
}

fn appendSortedI32I64(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(snapshot.I32I64Entry),
    source: []const java_body.IntLongEntry,
) ImportError!void {
    try out.ensureTotalCapacityPrecise(allocator, source.len);
    for (source) |entry| out.appendAssumeCapacity(.{ .key = entry.key, .value = entry.value });
    std.sort.heap(snapshot.I32I64Entry, out.items, {}, lessThanI32I64Entry);
    try rejectDuplicateI32I64(out.items);
}

fn rejectDuplicateSpecifications(specifications: []const common.CoreSymbolSpecification) ImportError!void {
    if (specifications.len < 2) return;
    for (specifications[1..], specifications[0 .. specifications.len - 1]) |current, prior| {
        if (current.symbol_id == prior.symbol_id) return error.DuplicateSymbol;
    }
}

fn rejectDuplicateI32I64(entries: []const snapshot.I32I64Entry) ImportError!void {
    if (entries.len < 2) return;
    for (entries[1..], entries[0 .. entries.len - 1]) |current, prior| {
        if (current.key == prior.key) return error.DuplicateMapKey;
    }
}

fn rejectDuplicatePositions(entries: []const snapshot.PositionSnapshot) ImportError!void {
    if (entries.len < 2) return;
    for (entries[1..], entries[0 .. entries.len - 1]) |current, prior| {
        if (current.symbol == prior.symbol) return error.DuplicateMapKey;
    }
}

fn rejectDuplicateUsers(entries: []const snapshot.UserSnapshot) ImportError!void {
    if (entries.len < 2) return;
    for (entries[1..], entries[0 .. entries.len - 1]) |current, prior| {
        if (current.uid == prior.uid) return error.DuplicateMapKey;
    }
}

fn rejectDuplicateLastPrices(entries: []const snapshot.LastPriceCacheEntry) ImportError!void {
    if (entries.len < 2) return;
    for (entries[1..], entries[0 .. entries.len - 1]) |current, prior| {
        if (current.symbol == prior.symbol) return error.DuplicateMapKey;
    }
}

fn containsSpecification(specifications: []const common.CoreSymbolSpecification, symbol: i32) bool {
    var low: usize = 0;
    var high = specifications.len;
    while (low < high) {
        const middle = low + (high - low) / 2;
        if (specifications[middle].symbol_id < symbol) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    return low < specifications.len and specifications[low].symbol_id == symbol;
}

fn symbolOwner(symbol: i32, count: usize) usize {
    const bits: u32 = @bitCast(symbol);
    return @as(usize, @intCast(bits)) & (count - 1);
}

fn uidOwner(uid: i64, count: usize) usize {
    const bits: u64 = @bitCast(uid);
    return @as(usize, @intCast(bits & @as(u64, @intCast(count - 1))));
}

fn lessThanBookSnapshot(_: void, lhs: snapshot.BookSnapshot, rhs: snapshot.BookSnapshot) bool {
    return lhs.symbol_spec.symbol_id < rhs.symbol_spec.symbol_id;
}

fn lessThanSpecification(
    _: void,
    lhs: common.CoreSymbolSpecification,
    rhs: common.CoreSymbolSpecification,
) bool {
    return lhs.symbol_id < rhs.symbol_id;
}

fn lessThanUserSnapshot(_: void, lhs: snapshot.UserSnapshot, rhs: snapshot.UserSnapshot) bool {
    return lhs.uid < rhs.uid;
}

fn lessThanPosition(_: void, lhs: snapshot.PositionSnapshot, rhs: snapshot.PositionSnapshot) bool {
    return lhs.symbol < rhs.symbol;
}

fn lessThanLastPrice(_: void, lhs: snapshot.LastPriceCacheEntry, rhs: snapshot.LastPriceCacheEntry) bool {
    return lhs.symbol < rhs.symbol;
}

fn lessThanI32I64Entry(_: void, lhs: snapshot.I32I64Entry, rhs: snapshot.I32I64Entry) bool {
    return lhs.key < rhs.key;
}

const test_symbol_ids = [_]i32{ 100, 101, 102, 103 };

const LegacyTestFixture = struct {
    allocator: std.mem.Allocator,
    matching: []java_body.MatchingEngineState,
    risks: []java_body.RiskEngineState,
    orderbook_kind: kernel_mod.OrderBookKind,

    fn init(
        allocator: std.mem.Allocator,
        matching_count: usize,
        risk_count: usize,
        orderbook_kind: kernel_mod.OrderBookKind,
    ) !LegacyTestFixture {
        const matching = try allocator.alloc(java_body.MatchingEngineState, matching_count);
        var matching_initialized: usize = 0;
        errdefer {
            for (matching[0..matching_initialized]) |*state| state.deinit(allocator);
            allocator.free(matching);
        }
        for (matching, 0..) |*state, shard_id| {
            state.* = try makeLegacyMatchingState(allocator, shard_id, matching_count, orderbook_kind);
            matching_initialized += 1;
        }

        const risks = try allocator.alloc(java_body.RiskEngineState, risk_count);
        var risks_initialized: usize = 0;
        errdefer {
            for (risks[0..risks_initialized]) |*state| state.deinit(allocator);
            allocator.free(risks);
        }
        for (risks, 0..) |*state, shard_id| {
            state.* = try makeLegacyRiskState(allocator, shard_id, risk_count);
            risks_initialized += 1;
        }

        return .{
            .allocator = allocator,
            .matching = matching,
            .risks = risks,
            .orderbook_kind = orderbook_kind,
        };
    }

    fn deinit(self: *LegacyTestFixture) void {
        for (self.matching) |*state| state.deinit(self.allocator);
        self.allocator.free(self.matching);
        for (self.risks) |*state| state.deinit(self.allocator);
        self.allocator.free(self.risks);
        self.* = undefined;
    }

    fn request(self: *const LegacyTestFixture, frontier: u64) Request {
        return .{
            .matching_states = self.matching,
            .risk_states = self.risks,
            .matching_shard_count = self.matching.len,
            .risk_shard_count = self.risks.len,
            .base_config = .{
                .orderbook_kind = self.orderbook_kind,
                // All fixture specifications are margin instruments. Raw
                // restore must retain them despite this live-admission gate.
                .risk = .{ .margin_trading_enabled = false },
            },
            .publication_frontier = frontier,
        };
    }
};

fn makeLegacySpecification(symbol_id: i32) java_body.CoreSymbolSpecification {
    return .{
        .symbol_id = symbol_id,
        .symbol_type = .futures_contract,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 1_000,
        .quote_scale_k = 10,
        .taker_fee = 3,
        .maker_fee = 1,
        .margin_buy = 25,
        .margin_sell = 30,
    };
}

fn makeLegacyBinaryState(allocator: std.mem.Allocator) !java_body.BinaryCommandsState {
    const transfers = try allocator.alloc(java_body.TransferRecord, 1);
    errdefer allocator.free(transfers);
    const data = try allocator.alloc(i64, 10);
    data[0] = @bitCast(@as(u64, 72) << 32);
    data[1] = 2;
    data[2] = 3;
    data[3] = 4;
    data[4] = 5;
    @memset(data[5..], 0);
    transfers[0] = .{
        .transfer_id = 77,
        .words_transferred = 5,
        .data = data,
    };
    return .{ .incoming_transfers = transfers };
}

fn makeLegacyOrder(symbol: i32, ordinal: i64) java_body.Order {
    return .{
        .order_id = @as(i64, symbol) * 100 + ordinal,
        .price = 1000 + symbol,
        .size = 10 + ordinal,
        .filled = ordinal,
        .reserve_bid_price = 1300 + symbol,
        .action = .ask,
        .uid = 8000 + ordinal,
        .timestamp = 9000 + ordinal,
    };
}

fn makeLegacyBook(
    allocator: std.mem.Allocator,
    symbol: i32,
    populated: bool,
    orderbook_kind: kernel_mod.OrderBookKind,
) !java_body.OrderBook {
    const order_count: usize = if (populated) 2 else 0;
    return switch (orderbook_kind) {
        .direct => blk: {
            const orders = try allocator.alloc(java_body.Order, order_count);
            if (populated) {
                orders[0] = makeLegacyOrder(symbol, 1);
                orders[1] = makeLegacyOrder(symbol, 2);
            }
            break :blk .{ .direct = .{
                .symbol_specification = makeLegacySpecification(symbol),
                .orders = orders,
            } };
        },
        .naive => blk: {
            const asks = try allocator.alloc(java_body.NaiveBucket, if (populated) 1 else 0);
            var asks_initialized: usize = 0;
            errdefer {
                for (asks[0..asks_initialized]) |*bucket| bucket.deinit(allocator);
                allocator.free(asks);
            }
            if (populated) {
                const orders = try allocator.alloc(java_body.Order, 2);
                orders[0] = makeLegacyOrder(symbol, 1);
                orders[1] = makeLegacyOrder(symbol, 2);
                asks[0] = .{
                    .price = 1000 + symbol,
                    .orders = orders,
                    .total_volume = (orders[0].size - orders[0].filled) +
                        (orders[1].size - orders[1].filled),
                };
                asks_initialized = 1;
            }
            const bids = try allocator.alloc(java_body.NaiveBucket, 0);
            break :blk .{ .naive = .{
                .symbol_specification = makeLegacySpecification(symbol),
                .ask_buckets = asks,
                .bid_buckets = bids,
            } };
        },
    };
}

fn makeLegacyMatchingState(
    allocator: std.mem.Allocator,
    shard_id: usize,
    matching_count: usize,
    orderbook_kind: kernel_mod.OrderBookKind,
) !java_body.MatchingEngineState {
    const books = try allocator.alloc(java_body.OrderBookEntry, test_symbol_ids.len);
    var books_initialized: usize = 0;
    errdefer {
        for (books[0..books_initialized]) |*entry| entry.deinit(allocator);
        allocator.free(books);
    }
    for (test_symbol_ids, books) |symbol, *entry| {
        entry.* = .{
            .symbol_id = symbol,
            .book = try makeLegacyBook(
                allocator,
                symbol,
                symbolOwner(symbol, matching_count) == shard_id,
                orderbook_kind,
            ),
        };
        books_initialized += 1;
    }
    var binary = try makeLegacyBinaryState(allocator);
    errdefer binary.deinit(allocator);
    return .{
        .shard = .{
            .shard_id = @intCast(shard_id),
            .shard_mask = @intCast(matching_count - 1),
        },
        .binary_commands = binary,
        .order_books = books,
    };
}

fn makeLegacyRiskState(
    allocator: std.mem.Allocator,
    shard_id: usize,
    risk_count: usize,
) !java_body.RiskEngineState {
    const specifications = try allocator.alloc(java_body.CoreSymbolSpecification, test_symbol_ids.len);
    errdefer allocator.free(specifications);
    for (test_symbol_ids, specifications) |symbol, *specification| {
        specification.* = makeLegacySpecification(symbol);
    }

    const profiles = try allocator.alloc(java_body.UserProfile, 1);
    var profiles_initialized: usize = 0;
    errdefer {
        for (profiles[0..profiles_initialized]) |*profile| profile.deinit(allocator);
        allocator.free(profiles);
    }
    profiles[0] = try makeLegacyUserProfile(allocator, shard_id);
    profiles_initialized = 1;

    var binary = try makeLegacyBinaryState(allocator);
    errdefer binary.deinit(allocator);
    const prices = try allocator.alloc(java_body.LastPriceEntry, 1);
    errdefer allocator.free(prices);
    prices[0] = .{ .symbol_id = 100, .ask_price = 1120, .bid_price = 1110 };
    const fees = try allocator.alloc(java_body.IntLongEntry, 1);
    errdefer allocator.free(fees);
    fees[0] = .{ .key = 15, .value = 70 + @as(i64, @intCast(shard_id)) };
    const adjustments = try allocator.alloc(java_body.IntLongEntry, 1);
    errdefer allocator.free(adjustments);
    adjustments[0] = .{ .key = 15, .value = -80 - @as(i64, @intCast(shard_id)) };
    const suspends = try allocator.alloc(java_body.IntLongEntry, 1);
    errdefer allocator.free(suspends);
    suspends[0] = .{ .key = 15, .value = 90 + @as(i64, @intCast(shard_id)) };

    return .{
        .shard = .{
            .shard_id = @intCast(shard_id),
            .shard_mask = @intCast(risk_count - 1),
        },
        .symbol_specifications = specifications,
        .user_profiles = profiles,
        .binary_commands = binary,
        .last_prices = prices,
        .fees = fees,
        .adjustments = adjustments,
        .suspends = suspends,
    };
}

fn makeLegacyUserProfile(
    allocator: std.mem.Allocator,
    shard_id: usize,
) !java_body.UserProfile {
    const accounts = try allocator.alloc(java_body.IntLongEntry, 1);
    errdefer allocator.free(accounts);
    accounts[0] = .{ .key = 15, .value = 10_000 + @as(i64, @intCast(shard_id)) };
    const positions = try allocator.alloc(java_body.SymbolPositionRecord, 1);
    errdefer allocator.free(positions);
    positions[0] = .{
        .symbol = test_symbol_ids[shard_id & (test_symbol_ids.len - 1)],
        .currency = 15,
        .direction = .long,
        .open_volume = 2,
        .open_price_sum = 2_200,
        .profit = -7,
        .pending_sell_size = 1,
        .pending_buy_size = 3,
    };
    return .{
        .uid = 4000 + @as(i64, @intCast(shard_id)),
        .positions = positions,
        .adjustments_counter = 41 + @as(i64, @intCast(shard_id)),
        .accounts = accounts,
        .status = .active,
    };
}

fn expectPendingRestored(assembler: *const binary_frames.BinaryFrameAssembler) !void {
    var pending = try assembler.capturePendingState(std.testing.allocator);
    defer pending.deinit();
    try std.testing.expectEqual(@as(usize, 1), pending.transfers.len);
    const transfer = pending.transfers[0];
    try std.testing.expectEqual(@as(i32, 77), transfer.transfer_id);
    try std.testing.expect(transfer.command_type == null);
    try std.testing.expectEqual(@as(?u32, 10), transfer.java_backing_capacity);
    try std.testing.expectEqual(@as(usize, 5), transfer.words.len);
    try std.testing.expectEqual(@as(i64, @bitCast(@as(u64, 72) << 32)), transfer.words[0]);
}

fn expectSuccessfulImport(fixture: *const LegacyTestFixture, frontier: u64) !void {
    var set = try importFresh(std.testing.allocator, fixture.request(frontier));
    defer set.deinit();

    try std.testing.expectEqual(fixture.matching.len, set.matchingShardCount());
    try std.testing.expectEqual(fixture.risks.len, set.riskShardCount());
    for (0..fixture.matching.len) |shard_id| {
        try std.testing.expectEqual(frontier, set.matchingObservedSequence(shard_id));
        const matching = set.matchingShardAt(shard_id);
        try expectPendingRestored(&matching.binary_frame_assembler);
        for (test_symbol_ids) |symbol| {
            var orders: std.ArrayListUnmanaged(kernel_mod.RestingOrder) = .empty;
            defer orders.deinit(std.testing.allocator);
            try matching.appendOrdersForSymbol(std.testing.allocator, symbol, &orders);
            if (symbolOwner(symbol, fixture.matching.len) == shard_id) {
                try std.testing.expectEqual(@as(usize, 2), orders.items.len);
                // Java serializes oldest-to-newest within a price level; both
                // native implementations must retain that FIFO sequence.
                try std.testing.expectEqual(makeLegacyOrder(symbol, 1).order_id, orders.items[0].order_id);
                try std.testing.expectEqual(makeLegacyOrder(symbol, 2).order_id, orders.items[1].order_id);
            } else {
                try std.testing.expectEqual(@as(usize, 0), orders.items.len);
            }
        }
    }

    for (0..fixture.risks.len) |shard_id| {
        try std.testing.expectEqual(frontier, set.riskObservedSequence(shard_id));
        const engine = set.riskEngineAt(shard_id);
        // The configured live gate disables margin symbols. Snapshot import is
        // raw restoration and therefore must still retain all persisted specs.
        try std.testing.expectEqual(test_symbol_ids.len, engine.symbol_spec_provider.symbols.count());
        for (test_symbol_ids) |symbol| {
            try std.testing.expect(engine.symbol_spec_provider.getSymbolSpecification(symbol) != null);
        }
        const uid = 4000 + @as(i64, @intCast(shard_id));
        const profile = engine.getUserProfile(uid) orelse return error.TestExpectedUser;
        try std.testing.expectEqual(@as(i64, 10_000 + @as(i64, @intCast(shard_id))), profile.accountBalance(15));
        try std.testing.expectEqual(@as(usize, 1), profile.positions.count());
        try std.testing.expectEqual(@as(i64, 70 + @as(i64, @intCast(shard_id))), engine.getFeeCollected(15));
        const price = engine.getLastPrice(100) orelse return error.TestExpectedPrice;
        try std.testing.expectEqual(@as(i64, 1120), price.ask_price);
        try std.testing.expectEqual(@as(i64, 1110), price.bid_price);
        try expectPendingRestored(&set.risk_shards[shard_id].binary_frame_assembler);
    }
}

fn makeHistoryIntLongEntries(
    allocator: std.mem.Allocator,
    keys: []const i32,
) ![]java_body.IntLongEntry {
    const entries = try allocator.alloc(java_body.IntLongEntry, keys.len);
    for (keys, entries, 0..) |key, *entry, index| {
        entry.* = .{ .key = key, .value = @intCast(index + 1) };
    }
    return entries;
}

fn makeHistoryPositions(
    allocator: std.mem.Allocator,
    symbols: []const i32,
) ![]java_body.SymbolPositionRecord {
    const positions = try allocator.alloc(java_body.SymbolPositionRecord, symbols.len);
    for (symbols, positions, 0..) |symbol, *position, index| {
        const volume: i64 = @intCast(index + 1);
        position.* = .{
            .symbol = symbol,
            .currency = 15,
            .direction = .long,
            .open_volume = volume,
            .open_price_sum = volume * 1_100,
            .profit = -volume,
            .pending_sell_size = 0,
            .pending_buy_size = volume,
        };
    }
    return positions;
}

fn expectIntEncounterHistory(
    actual: *const int_map_order.History,
    encounter: []const i32,
) !void {
    const allocator = std.testing.allocator;
    var expected = try int_map_order.History.initFromEncounterOrder(
        allocator,
        encounter.len,
        encounter,
    );
    defer expected.deinit(allocator);
    try std.testing.expectEqual(expected.tableSizeIfKnown(), actual.tableSizeIfKnown());
    const expected_keys = try expected.orderedKeys(allocator);
    defer allocator.free(expected_keys);
    const actual_keys = try actual.orderedKeys(allocator);
    defer allocator.free(actual_keys);
    try std.testing.expectEqualSlices(i32, expected_keys, actual_keys);
}

fn expectLongEncounterHistory(
    actual: *const long_map_order.History,
    encounter: []const i64,
) !void {
    const allocator = std.testing.allocator;
    var expected = try long_map_order.History.initFromEncounterOrder(
        allocator,
        encounter.len,
        encounter,
    );
    defer expected.deinit(allocator);
    try std.testing.expectEqual(expected.tableSizeIfKnown(), actual.tableSizeIfKnown());
    const expected_keys = try expected.orderedKeys(allocator);
    defer allocator.free(expected_keys);
    const actual_keys = try actual.orderedKeys(allocator);
    defer allocator.free(actual_keys);
    try std.testing.expectEqualSlices(i64, expected_keys, actual_keys);
}

fn importAllocationTest(allocator: std.mem.Allocator, request: Request) !void {
    var set = try importFresh(allocator, request);
    defer set.deinit();
}

test "legacy Java importer restores 4 matching and 2 risk shards with direct FIFO" {
    var fixture = try LegacyTestFixture.init(std.testing.allocator, 4, 2, .direct);
    defer fixture.deinit();
    try expectSuccessfulImport(&fixture, 811);
}

test "legacy Java importer restores 1 matching and 4 risk shards with naive FIFO" {
    var fixture = try LegacyTestFixture.init(std.testing.allocator, 1, 4, .naive);
    defer fixture.deinit();
    try expectSuccessfulImport(&fixture, 912);
}

test "legacy Java importer reconstructs sized Eclipse histories before semantic sorting" {
    const allocator = std.testing.allocator;

    var legacy_matching = try makeLegacyMatchingState(allocator, 0, 1, .direct);
    defer legacy_matching.deinit(allocator);
    const book_encounter = [_]i32{ 33, 2, 17, 1 };
    for (legacy_matching.order_books, book_encounter) |*entry, symbol| {
        entry.symbol_id = symbol;
        switch (entry.book) {
            .direct => |*book| book.symbol_specification.symbol_id = symbol,
            .naive => unreachable,
        }
    }
    var matching_binary_hash: i32 = undefined;
    var matching = try buildMatchingState(
        allocator,
        &legacy_matching,
        .direct,
        &matching_binary_hash,
    );
    defer matching.deinit();
    try expectIntEncounterHistory(&matching.books_history, &book_encounter);
    try std.testing.expectEqual(@as(i32, 1), matching.books.items[0].symbol_spec.symbol_id);
    try std.testing.expectEqual(@as(i32, 33), matching.books.items[3].symbol_spec.symbol_id);

    var legacy_risk = try makeLegacyRiskState(allocator, 0, 1);
    defer legacy_risk.deinit(allocator);
    const int_encounter = [_]i32{ 33, 2, 17, 0, 1 };
    const position_encounter = [_]i32{ 103, 100, 102, 101 };
    const accounts = try makeHistoryIntLongEntries(allocator, &int_encounter);
    allocator.free(legacy_risk.user_profiles[0].accounts);
    legacy_risk.user_profiles[0].accounts = accounts;
    const positions = try makeHistoryPositions(allocator, &position_encounter);
    allocator.free(legacy_risk.user_profiles[0].positions);
    legacy_risk.user_profiles[0].positions = positions;
    const fees = try makeHistoryIntLongEntries(allocator, &int_encounter);
    allocator.free(legacy_risk.fees);
    legacy_risk.fees = fees;
    const adjustments = try makeHistoryIntLongEntries(allocator, &int_encounter);
    allocator.free(legacy_risk.adjustments);
    legacy_risk.adjustments = adjustments;
    const suspends = try makeHistoryIntLongEntries(allocator, &int_encounter);
    allocator.free(legacy_risk.suspends);
    legacy_risk.suspends = suspends;

    var risk_binary_hash: i32 = undefined;
    var risk_state = try buildRiskState(allocator, &legacy_risk, &risk_binary_hash);
    defer risk_state.deinit();
    var engine = try risk_mod.RiskEngine.init(allocator, 0, 1, .{});
    defer engine.deinit();
    var assembler = binary_frames.BinaryFrameAssembler.init(allocator);
    defer assembler.deinit();
    try restoreRiskState(
        allocator,
        &risk_state,
        &engine,
        &assembler,
        0,
        1,
        .{},
    );

    const uid_encounter = [_]i64{4000};
    try expectLongEncounterHistory(
        &engine.user_profile_service.user_profiles_history,
        &uid_encounter,
    );
    const profile = engine.getUserProfile(4000) orelse return error.TestExpectedUser;
    try expectIntEncounterHistory(&profile.accounts_history, &int_encounter);
    try expectIntEncounterHistory(&profile.positions_history, &position_encounter);
    try expectIntEncounterHistory(&engine.fees_history, &int_encounter);
    try expectIntEncounterHistory(&engine.adjustments_history, &int_encounter);
    try expectIntEncounterHistory(&engine.suspends_history, &int_encounter);
}

test "legacy Java importer rejects topology ownership specification order and price replica corruption" {
    var fixture = try LegacyTestFixture.init(std.testing.allocator, 4, 2, .direct);
    defer fixture.deinit();

    var request = fixture.request(1);
    request.matching_shard_count = 2;
    try std.testing.expectError(error.MatchingShardCountMismatch, importFresh(std.testing.allocator, request));
    request = fixture.request(1);
    fixture.matching[0].shard.shard_mask = 1;
    try std.testing.expectError(error.MatchingShardMaskMismatch, importFresh(std.testing.allocator, request));
    fixture.matching[0].shard.shard_mask = 3;

    std.mem.swap(
        java_body.OrderBook,
        &fixture.matching[0].order_books[0].book,
        &fixture.matching[1].order_books[0].book,
    );
    try std.testing.expectError(error.WrongSymbolOwner, importFresh(std.testing.allocator, fixture.request(1)));
    std.mem.swap(
        java_body.OrderBook,
        &fixture.matching[0].order_books[0].book,
        &fixture.matching[1].order_books[0].book,
    );

    fixture.risks[0].symbol_specifications[0].taker_fee += 1;
    try std.testing.expectError(error.SymbolSpecificationMismatch, importFresh(std.testing.allocator, fixture.request(1)));
    fixture.risks[0].symbol_specifications[0].taker_fee -= 1;

    fixture.matching[0].order_books[0].book.direct.orders[0].size = 0;
    try std.testing.expectError(error.InvalidOrder, importFresh(std.testing.allocator, fixture.request(1)));
    fixture.matching[0].order_books[0].book.direct.orders[0].size = 11;

    fixture.risks[1].last_prices[0].ask_price += 1;
    try std.testing.expectError(error.LastPriceReplicaMismatch, importFresh(std.testing.allocator, fixture.request(1)));
    fixture.risks[1].last_prices[0].ask_price -= 1;

    request = fixture.request(1);
    request.base_config.orderbook_kind = .naive;
    try std.testing.expectError(error.OrderBookKindMismatch, importFresh(std.testing.allocator, request));
}

test "legacy Java importer rejects transfer identity capacity hash and naive aggregate tampering" {
    var direct = try LegacyTestFixture.init(std.testing.allocator, 1, 1, .direct);
    defer direct.deinit();

    direct.matching[0].binary_commands.incoming_transfers[0].transfer_id = std.math.maxInt(i64);
    try std.testing.expectError(error.InvalidTransfer, importFresh(std.testing.allocator, direct.request(1)));
    direct.matching[0].binary_commands.incoming_transfers[0].transfer_id = 77;

    direct.risks[0].binary_commands.incoming_transfers[0].data[1] += 1;
    try std.testing.expectError(error.BinaryProcessorMismatch, importFresh(std.testing.allocator, direct.request(1)));
    direct.risks[0].binary_commands.incoming_transfers[0].data[1] -= 1;

    const full_backing = direct.risks[0].binary_commands.incoming_transfers[0].data;
    direct.risks[0].binary_commands.incoming_transfers[0].data = full_backing[0..5];
    try std.testing.expectError(error.BinaryProcessorMismatch, importFresh(std.testing.allocator, direct.request(1)));
    direct.risks[0].binary_commands.incoming_transfers[0].data = full_backing;

    direct.matching[0].binary_commands.incoming_transfers[0].data[9] = 1;
    direct.risks[0].binary_commands.incoming_transfers[0].data[9] = 1;
    try std.testing.expectError(error.InvalidTransfer, importFresh(std.testing.allocator, direct.request(1)));
    direct.matching[0].binary_commands.incoming_transfers[0].data[9] = 0;
    direct.risks[0].binary_commands.incoming_transfers[0].data[9] = 0;

    direct.matching[0].binary_commands.incoming_transfers[0].words_transferred = 4;
    direct.risks[0].binary_commands.incoming_transfers[0].words_transferred = 4;
    try std.testing.expectError(error.InvalidTransfer, importFresh(std.testing.allocator, direct.request(1)));

    var naive = try LegacyTestFixture.init(std.testing.allocator, 1, 1, .naive);
    defer naive.deinit();
    naive.matching[0].order_books[0].book.naive.ask_buckets[0].total_volume += 1;
    try std.testing.expectError(error.InvalidNaiveBucket, importFresh(std.testing.allocator, naive.request(1)));
}

test "legacy Java importer releases every staged allocation failure" {
    var fixture = try LegacyTestFixture.init(std.testing.allocator, 1, 1, .direct);
    defer fixture.deinit();
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        importAllocationTest,
        .{fixture.request(13)},
    );
}
