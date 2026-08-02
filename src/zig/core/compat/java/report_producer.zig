//! Exact Java-compatible report section production.
//!
//! Native reports intentionally use Zig fingerprints and checked arithmetic.
//! This module is a separate compatibility lane: it reconstructs the Java
//! section bodies, BINARY_EVENT carriers, reducers, wrapping arithmetic, and
//! JVM identity-dependent hashes without narrowing a native fingerprint.

const std = @import("std");
const binary_frames = @import("../../binary_frames.zig");
const binary_payload = @import("../../binary_payload.zig");
const common = @import("../../common.zig");
const core_events = @import("../../events.zig");
const int_map_order = @import("eclipse_collections/int_map_order.zig");
const java_hash32 = @import("hash32.zig");
const wire = @import("report_wire.zig");
const orderbook = @import("../../orderbook_naive.zig");
const risk = @import("../../risk.zig");
const user = @import("../../user_profile.zig");

pub const JavaIdentityHashContext = struct {
    order_action_ask: i32,
    order_action_bid: i32,
    user_status_active: i32,
    user_status_suspended: i32,
    risk_symbol_spec_provider: i32,
    risk_user_profile_service: i32,
    risk_binary_cmd_processor: i32,
    risk_last_price_cache: i32,
    risk_fees: i32,
    risk_adjustments: i32,
    risk_suspends: i32,
    risk_shard_mask: i32,
    matching_binary_cmd_processor: i32,
    matching_order_books: i32,
    matching_shard_mask: i32,

    fn orderAction(self: JavaIdentityHashContext, action: common.OrderAction) i32 {
        return switch (action) {
            .ask => self.order_action_ask,
            .bid => self.order_action_bid,
        };
    }

    fn userStatus(self: JavaIdentityHashContext, status: common.UserStatus) i32 {
        return switch (status) {
            .active => self.user_status_active,
            .suspended => self.user_status_suspended,
        };
    }

    fn stateHashSubmodule(
        self: JavaIdentityHashContext,
        submodule: wire.StateHashSubmodule,
    ) i32 {
        return switch (submodule) {
            .risk_symbol_spec_provider => self.risk_symbol_spec_provider,
            .risk_user_profile_service => self.risk_user_profile_service,
            .risk_binary_cmd_processor => self.risk_binary_cmd_processor,
            .risk_last_price_cache => self.risk_last_price_cache,
            .risk_fees => self.risk_fees,
            .risk_adjustments => self.risk_adjustments,
            .risk_suspends => self.risk_suspends,
            .risk_shard_mask => self.risk_shard_mask,
            .matching_binary_cmd_processor => self.matching_binary_cmd_processor,
            .matching_order_books => self.matching_order_books,
            .matching_shard_mask => self.matching_shard_mask,
        };
    }
};

pub const JavaStateHashReport = struct {
    state_hash: i32,
    report: wire.StateHashReport,

    pub fn deinit(self: *JavaStateHashReport, allocator: std.mem.Allocator) void {
        self.report.deinit(allocator);
        self.* = undefined;
    }
};

pub const JavaReportResponse = union(enum) {
    state_hash: JavaStateHashReport,
    single_user: wire.SingleUserReport,
    total_currency_balance: wire.TotalCurrencyBalanceReport,

    pub fn deinit(self: *JavaReportResponse, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .state_hash => |*report| report.deinit(allocator),
            .single_user => |*report| report.deinit(allocator),
            .total_currency_balance => |*report| report.deinit(allocator),
        }
        self.* = undefined;
    }
};

pub const ProducedReport = struct {
    response: JavaReportResponse,
    events_head: ?*core_events.MatcherTradeEvent,
    /// Always heap for produce() today; field prevents future pool cross-free.
    events_owner: core_events.EventNodesOwner,

    pub fn deinit(self: *ProducedReport, allocator: std.mem.Allocator) void {
        self.response.deinit(allocator);
        core_events.deinitMatcherEventChain(self.events_owner, self.events_head);
        self.* = undefined;
    }
};

pub const BookTraversal = enum {
    naive,
    direct,
};

pub const MatchingBookView = struct {
    traversal: BookTraversal,
    symbol_specification: common.CoreSymbolSpecification,
    /// Java ask stream followed by bid stream, FIFO within each price level.
    orders: []const orderbook.UserOrderView,
};

pub const MatchingSectionView = struct {
    shard_id: i32,
    shard_mask: u64,
    binary_processor: *const binary_frames.BinaryFrameAssembler,
    books: []const MatchingBookView,
};

pub const RiskSectionView = struct {
    shard_id: i32,
    shard_mask: u64,
    binary_processor: *const binary_frames.BinaryFrameAssembler,
    engine: *risk.RiskEngine,
};

pub const ProducerError = wire.WireError || binary_frames.JavaStateHashError || error{
    MissingJavaIdentityHashContext,
    UnknownReportQuery,
    MissingSymbolSpecification,
};

fn mapHistoryError(err: anyerror) ProducerError {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        error.CapacityOverflow => error.CapacityOverflow,
        error.HistoryUnavailable => error.HistoryUnavailable,
        error.HistorySemanticKeySetMismatch => error.HistorySemanticKeySetMismatch,
        else => error.HistorySemanticKeySetMismatch,
    };
}

fn orderedHistoryKeys(
    allocator: std.mem.Allocator,
    history: *const int_map_order.History,
) ProducerError![]i32 {
    return history.orderedKeys(allocator) catch |err| return mapHistoryError(err);
}

pub fn produce(
    allocator: std.mem.Allocator,
    query: binary_payload.DecodedReportQuery,
    matching_sections: []const MatchingSectionView,
    risk_sections: []const RiskSectionView,
    identity_context: ?JavaIdentityHashContext,
    config: wire.Config,
) ProducerError!ProducedReport {
    const identity: ?JavaIdentityHashContext = switch (query) {
        .state_hash => identity_context orelse return error.MissingJavaIdentityHashContext,
        .single_user, .total_currency_balance => null,
        .unknown => return error.UnknownReportQuery,
    };

    var event_chain = core_events.EventChain.init(allocator);
    defer event_chain.deinit();

    for (risk_sections) |section| {
        switch (query) {
            .state_hash => try appendRiskStateHashSection(
                allocator,
                &event_chain,
                section,
                identity.?,
                config,
            ),
            .single_user => |uid| {
                if (section.engine.uidForThisHandler(uid)) {
                    try appendRiskSingleUserSection(allocator, &event_chain, section, uid, config);
                }
            },
            .total_currency_balance => try appendRiskTotalBalanceSection(
                allocator,
                &event_chain,
                section,
                config,
            ),
            .unknown => unreachable,
        }
    }
    for (matching_sections) |section| {
        switch (query) {
            .state_hash => try appendMatchingStateHashSection(
                allocator,
                &event_chain,
                section,
                identity.?,
                config,
            ),
            .single_user => |uid| try appendMatchingSingleUserSection(
                allocator,
                &event_chain,
                section,
                uid,
                config,
            ),
            .total_currency_balance => try appendMatchingTotalBalanceSection(
                allocator,
                &event_chain,
                section,
                config,
            ),
            .unknown => unreachable,
        }
    }

    var response: JavaReportResponse = switch (query) {
        .state_hash => blk: {
            var report = try wire.mergeStateHashEventChain(allocator, event_chain.head, config);
            errdefer report.deinit(allocator);
            break :blk .{ .state_hash = .{
                .state_hash = stateHashResult(report.entries, identity.?),
                .report = report,
            } };
        },
        .single_user => .{
            .single_user = try wire.mergeSingleUserEventChain(allocator, event_chain.head, config),
        },
        .total_currency_balance => .{
            .total_currency_balance = try wire.mergeTotalCurrencyBalanceEventChain(
                allocator,
                event_chain.head,
                config,
            ),
        },
        .unknown => unreachable,
    };
    errdefer response.deinit(allocator);
    return .{
        .response = response,
        .events_head = event_chain.takeOwnedHead(),
        .events_owner = event_chain.owner,
    };
}

pub fn emptyResponse(
    allocator: std.mem.Allocator,
    query: binary_payload.DecodedReportQuery,
    identity_context: ?JavaIdentityHashContext,
    config: wire.Config,
) ProducerError!JavaReportResponse {
    return switch (query) {
        .state_hash => blk: {
            _ = identity_context;
            var grouped = wire.GroupedBinaryEvents{
                .sections = try allocator.alloc(wire.GroupedBinaryEventSection, 0),
            };
            defer grouped.deinit(allocator);
            const report = try wire.mergeStateHashGroupedEvents(allocator, &grouped, config);
            break :blk .{
                .state_hash = .{
                    // Java's Arrays.hashCode over an empty entry-hash array is 1.
                    // No enum identity is observed for a locally disabled query.
                    .state_hash = 1,
                    .report = report,
                },
            };
        },
        .single_user => blk: {
            var grouped = wire.GroupedBinaryEvents{
                .sections = try allocator.alloc(wire.GroupedBinaryEventSection, 0),
            };
            defer grouped.deinit(allocator);
            break :blk .{
                .single_user = try wire.mergeSingleUserGroupedEvents(allocator, &grouped, config),
            };
        },
        .total_currency_balance => blk: {
            var grouped = wire.GroupedBinaryEvents{
                .sections = try allocator.alloc(wire.GroupedBinaryEventSection, 0),
            };
            defer grouped.deinit(allocator);
            break :blk .{
                .total_currency_balance = try wire.mergeTotalCurrencyBalanceGroupedEvents(
                    allocator,
                    &grouped,
                    config,
                ),
            };
        },
        .unknown => error.UnknownReportQuery,
    };
}

fn appendRiskStateHashSection(
    allocator: std.mem.Allocator,
    chain: *core_events.EventChain,
    section: RiskSectionView,
    identity: JavaIdentityHashContext,
    config: wire.Config,
) ProducerError!void {
    const binary_hash = try section.binary_processor.javaStateHash32();
    // Java 0.5.0 has an observable copy/paste bug: code 0 uses the BCP hash,
    // not SymbolSpecificationProvider.stateHash(). Preserve it exactly.
    const entries = [_]wire.StateHashEntry{
        .{ .module_id = section.shard_id, .submodule = .risk_symbol_spec_provider, .hash = binary_hash },
        .{
            .module_id = section.shard_id,
            .submodule = .risk_user_profile_service,
            .hash = javaUserProfileServiceHash(section.engine, identity),
        },
        .{ .module_id = section.shard_id, .submodule = .risk_binary_cmd_processor, .hash = binary_hash },
        .{
            .module_id = section.shard_id,
            .submodule = .risk_last_price_cache,
            .hash = javaLastPriceCacheHash(section.engine),
        },
        .{ .module_id = section.shard_id, .submodule = .risk_fees, .hash = javaIntLongMapHash(&section.engine.fees) },
        .{
            .module_id = section.shard_id,
            .submodule = .risk_adjustments,
            .hash = javaIntLongMapHash(&section.engine.adjustments),
        },
        .{
            .module_id = section.shard_id,
            .submodule = .risk_suspends,
            .hash = javaIntLongMapHash(&section.engine.suspends),
        },
        .{
            .module_id = section.shard_id,
            .submodule = .risk_shard_mask,
            .hash = java_hash32.longHashCode(@bitCast(section.shard_mask)),
        },
    };
    const body = try wire.encodeStateHashBody(allocator, .{ .entries = &entries }, config);
    defer allocator.free(body);
    try appendBodyEvents(allocator, chain, try wire.SectionLabel.risk(section.shard_id), body);
}

fn appendMatchingStateHashSection(
    allocator: std.mem.Allocator,
    chain: *core_events.EventChain,
    section: MatchingSectionView,
    identity: JavaIdentityHashContext,
    config: wire.Config,
) ProducerError!void {
    const entries = [_]wire.StateHashEntry{
        .{
            .module_id = section.shard_id,
            .submodule = .matching_binary_cmd_processor,
            .hash = try section.binary_processor.javaStateHash32(),
        },
        .{
            .module_id = section.shard_id,
            .submodule = .matching_order_books,
            .hash = javaOrderBooksHash(section.books, identity),
        },
        .{
            .module_id = section.shard_id,
            .submodule = .matching_shard_mask,
            .hash = java_hash32.longHashCode(@bitCast(section.shard_mask)),
        },
    };
    const body = try wire.encodeStateHashBody(allocator, .{ .entries = &entries }, config);
    defer allocator.free(body);
    try appendBodyEvents(allocator, chain, try wire.SectionLabel.matching(section.shard_id), body);
}

fn appendRiskSingleUserSection(
    allocator: std.mem.Allocator,
    chain: *core_events.EventChain,
    section: RiskSectionView,
    uid: i64,
    config: wire.Config,
) ProducerError!void {
    var report = wire.SingleUserReport{
        .uid = uid,
        .user_status = null,
        .accounts = null,
        .positions = null,
        .orders = null,
        .query_execution_status = .ok,
    };
    defer report.deinit(allocator);

    if (section.engine.user_profile_service.getUserProfile(uid)) |profile| {
        report.user_status = switch (profile.user_status) {
            .active => .active,
            .suspended => .suspended,
        };
        const accounts_history = profile.validatedAccountsHistory(allocator) catch |err|
            return mapHistoryError(err);
        report.accounts = try ownIntLongMapWithHistory(
            allocator,
            &profile.accounts,
            accounts_history,
        );

        // Java creates a fresh IntObjectHashMap(profile.positions.size()) and
        // inserts the live positions in the source EC traversal order.
        const positions_history = profile.validatedPositionsHistory(allocator) catch |err|
            return mapHistoryError(err);
        const live_position_keys = try orderedHistoryKeys(allocator, positions_history);
        defer allocator.free(live_position_keys);
        var copied_positions_history = try int_map_order.History.initFromEncounterOrder(
            allocator,
            profile.positions.count(),
            live_position_keys,
        );
        defer copied_positions_history.deinit(allocator);
        report.positions = try ownPositionsWithHistory(
            allocator,
            &profile.positions,
            &copied_positions_history,
        );
    } else {
        report.query_execution_status = .user_not_found;
    }

    const body = try wire.encodeSingleUserBody(allocator, report, config);
    defer allocator.free(body);
    try appendBodyEvents(allocator, chain, try wire.SectionLabel.risk(section.shard_id), body);
}

fn appendMatchingSingleUserSection(
    allocator: std.mem.Allocator,
    chain: *core_events.EventChain,
    section: MatchingSectionView,
    uid: i64,
    config: wire.Config,
) ProducerError!void {
    var report = wire.SingleUserReport{
        .uid = uid,
        .user_status = null,
        .accounts = null,
        .positions = null,
        .orders = try ownUserOrderGroups(allocator, section.books, uid),
        .query_execution_status = .ok,
    };
    defer report.deinit(allocator);
    const body = try wire.encodeSingleUserBody(allocator, report, config);
    defer allocator.free(body);
    try appendBodyEvents(allocator, chain, try wire.SectionLabel.matching(section.shard_id), body);
}

fn appendRiskTotalBalanceSection(
    allocator: std.mem.Allocator,
    chain: *core_events.EventChain,
    section: RiskSectionView,
    config: wire.Config,
) ProducerError!void {
    var report = try riskTotalBalanceReport(allocator, section.engine);
    defer report.deinit(allocator);
    const body = try wire.encodeTotalCurrencyBalanceBody(allocator, report, config);
    defer allocator.free(body);
    try appendBodyEvents(allocator, chain, try wire.SectionLabel.risk(section.shard_id), body);
}

fn appendMatchingTotalBalanceSection(
    allocator: std.mem.Allocator,
    chain: *core_events.EventChain,
    section: MatchingSectionView,
    config: wire.Config,
) ProducerError!void {
    var report = try matchingTotalBalanceReport(allocator, section.books);
    defer report.deinit(allocator);
    const body = try wire.encodeTotalCurrencyBalanceBody(allocator, report, config);
    defer allocator.free(body);
    try appendBodyEvents(allocator, chain, try wire.SectionLabel.matching(section.shard_id), body);
}

fn appendBodyEvents(
    allocator: std.mem.Allocator,
    chain: *core_events.EventChain,
    label: wire.SectionLabel,
    body: []const u8,
) ProducerError!void {
    const carriers = try wire.packSectionEvents(allocator, label, body);
    defer allocator.free(carriers);
    for (carriers) |carrier| {
        _ = try chain.append(.{
            .event_type = .binary_event,
            .section = carrier.section_id,
            .matched_order_id = carrier.words[0],
            .matched_order_uid = carrier.words[1],
            .price = carrier.words[2],
            .size = carrier.words[3],
            .bidder_hold_price = carrier.words[4],
        });
    }
}

fn ownUserOrderGroups(
    allocator: std.mem.Allocator,
    books: []const MatchingBookView,
    uid: i64,
) ProducerError![]const wire.SymbolOrders {
    var history = int_map_order.History.init();
    defer history.deinit(allocator);
    var indexes: std.AutoHashMapUnmanaged(i32, usize) = .empty;
    defer indexes.deinit(allocator);

    const groups = try allocator.alloc(wire.SymbolOrders, books.len);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| allocator.free(group.orders);
        allocator.free(groups);
    }
    for (books) |book| {
        var count: usize = 0;
        for (book.orders) |order| {
            if (order.uid == uid) count += 1;
        }
        if (count == 0) continue;
        const symbol = book.symbol_specification.symbol_id;
        if (indexes.contains(symbol)) return error.DuplicateKey;
        const orders = try allocator.alloc(wire.Order, count);
        errdefer allocator.free(orders);
        var index: usize = 0;
        for (book.orders) |order| {
            if (order.uid != uid) continue;
            orders[index] = wireOrder(order);
            index += 1;
        }
        if (book.traversal == .direct) {
            // DirectOrderBook.findUserOrders walks the order-id ART, unlike
            // the book streams used by state hashing and the naive report.
            std.sort.heap(wire.Order, orders, {}, lessOrderId);
        }
        try indexes.ensureUnusedCapacity(allocator, 1);
        try history.recordPut(allocator, symbol);
        groups[initialized] = .{ .symbol = symbol, .orders = orders };
        indexes.putAssumeCapacityNoClobber(symbol, initialized);
        initialized += 1;
    }

    const ordered_keys = try orderedHistoryKeys(allocator, &history);
    defer allocator.free(ordered_keys);
    if (ordered_keys.len != initialized) return error.HistorySemanticKeySetMismatch;
    const exact = try allocator.alloc(wire.SymbolOrders, initialized);
    errdefer allocator.free(exact);
    for (ordered_keys, 0..) |symbol, index| {
        exact[index] = groups[
            indexes.get(symbol) orelse
                return error.HistorySemanticKeySetMismatch
        ];
    }
    allocator.free(groups);
    return exact;
}

fn wireOrder(order: orderbook.UserOrderView) wire.Order {
    return .{
        .order_id = order.order_id,
        .price = order.price,
        .size = order.size,
        .filled = order.filled,
        .reserve_bid_price = order.reserve_bid_price,
        .action = switch (order.action) {
            .ask => .ask,
            .bid => .bid,
        },
        .uid = order.uid,
        .timestamp = order.timestamp,
    };
}

fn ownIntLongMapWithHistory(
    allocator: std.mem.Allocator,
    map: *const std.AutoHashMapUnmanaged(i32, i64),
    history: *const int_map_order.History,
) ProducerError![]const wire.IntLongEntry {
    const keys = try orderedHistoryKeys(allocator, history);
    defer allocator.free(keys);
    if (keys.len != map.count()) return error.HistorySemanticKeySetMismatch;
    const entries = try allocator.alloc(wire.IntLongEntry, keys.len);
    errdefer allocator.free(entries);
    for (keys, 0..) |key, index| {
        entries[index] = .{
            .key = key,
            .value = map.get(key) orelse return error.HistorySemanticKeySetMismatch,
        };
    }
    return entries;
}

fn ownPositionsWithHistory(
    allocator: std.mem.Allocator,
    map: *const std.AutoHashMapUnmanaged(i32, user.SymbolPositionRecord),
    history: *const int_map_order.History,
) ProducerError![]const wire.PositionEntry {
    const keys = try orderedHistoryKeys(allocator, history);
    defer allocator.free(keys);
    if (keys.len != map.count()) return error.HistorySemanticKeySetMismatch;
    const entries = try allocator.alloc(wire.PositionEntry, keys.len);
    errdefer allocator.free(entries);
    for (keys, 0..) |key, index| {
        const position = map.get(key) orelse return error.HistorySemanticKeySetMismatch;
        entries[index] = .{
            .symbol = key,
            .quote_currency = position.currency,
            .direction = switch (position.direction) {
                .short => .short,
                .empty => .empty,
                .long => .long,
            },
            .open_volume = position.open_volume,
            .open_price_sum = position.open_price_sum,
            .profit = position.profit,
            .pending_sell_size = position.pending_sell_size,
            .pending_buy_size = position.pending_buy_size,
        };
    }
    return entries;
}

const WrappingMap = std.AutoHashMapUnmanaged(i32, i64);

const OrderedWrappingMap = struct {
    values: WrappingMap = .empty,
    history: int_map_order.History = int_map_order.History.init(),

    fn deinit(self: *OrderedWrappingMap, allocator: std.mem.Allocator) void {
        self.values.deinit(allocator);
        self.history.deinit(allocator);
        self.* = undefined;
    }

    fn add(
        self: *OrderedWrappingMap,
        allocator: std.mem.Allocator,
        key: i32,
        value: i64,
    ) ProducerError!void {
        if (self.values.getPtr(key)) |present| {
            present.* +%= value;
            return;
        }
        try self.values.ensureUnusedCapacity(allocator, 1);
        try self.history.recordPut(allocator, key);
        // EC addToValue inserts an active key even when `value == 0`.
        self.values.putAssumeCapacityNoClobber(key, value);
    }

    fn ownEntries(
        self: *const OrderedWrappingMap,
        allocator: std.mem.Allocator,
    ) ProducerError![]const wire.IntLongEntry {
        return ownIntLongMapWithHistory(allocator, &self.values, &self.history);
    }
};

fn matchingTotalBalanceReport(
    allocator: std.mem.Allocator,
    books: []const MatchingBookView,
) ProducerError!wire.TotalCurrencyBalanceReport {
    var order_balances: OrderedWrappingMap = .{};
    defer order_balances.deinit(allocator);
    for (books) |book| {
        const spec = book.symbol_specification;
        if (spec.symbol_type != .currency_exchange_pair) continue;
        var ask_total: i64 = 0;
        var bid_total: i64 = 0;
        for (book.orders) |order| {
            const remaining = order.size -% order.filled;
            switch (order.action) {
                .ask => ask_total +%= remaining *% spec.base_scale_k,
                .bid => bid_total +%= remaining *%
                    (order.reserve_bid_price *% spec.quote_scale_k +% spec.taker_fee),
            }
        }
        try order_balances.add(allocator, spec.base_currency, ask_total);
        try order_balances.add(allocator, spec.quote_currency, bid_total);
    }
    return .{
        .account_balances = null,
        .fees = null,
        .adjustments = null,
        .suspends = null,
        .order_balances = try order_balances.ownEntries(allocator),
        .open_interest_long = null,
        .open_interest_short = null,
    };
}

fn riskTotalBalanceReport(
    allocator: std.mem.Allocator,
    engine: *const risk.RiskEngine,
) ProducerError!wire.TotalCurrencyBalanceReport {
    var balances: OrderedWrappingMap = .{};
    defer balances.deinit(allocator);
    var open_long: OrderedWrappingMap = .{};
    defer open_long.deinit(allocator);
    var open_short: OrderedWrappingMap = .{};
    defer open_short.deinit(allocator);

    _ = engine.user_profile_service.validatedUserProfilesHistory() catch |err|
        return mapHistoryError(err);
    const uids = engine.user_profile_service.userProfileKeysInHistoryOrder() catch |err|
        return mapHistoryError(err);
    defer engine.allocator.free(uids);
    for (uids) |uid| {
        const profile = engine.user_profile_service.user_profiles.getPtr(uid) orelse
            return error.HistorySemanticKeySetMismatch;
        const account_keys = profile.accountKeysInHistoryOrder(allocator) catch |err|
            return mapHistoryError(err);
        defer allocator.free(account_keys);
        for (account_keys) |currency| {
            try balances.add(
                allocator,
                currency,
                profile.accounts.get(currency) orelse
                    return error.HistorySemanticKeySetMismatch,
            );
        }
        const position_keys = profile.positionKeysInHistoryOrder(allocator) catch |err|
            return mapHistoryError(err);
        defer allocator.free(position_keys);
        for (position_keys) |symbol| {
            const position = profile.positions.get(symbol) orelse
                return error.HistorySemanticKeySetMismatch;
            const specification = engine.symbol_spec_provider.getSymbolSpecification(position.symbol) orelse
                return error.MissingSymbolSpecification;
            try balances.add(
                allocator,
                position.currency,
                javaEstimatedProfit(engine, position, specification),
            );
            switch (position.direction) {
                .long => try open_long.add(allocator, position.symbol, position.open_volume),
                .short => try open_short.add(allocator, position.symbol, position.open_volume),
                .empty => {},
            }
        }
    }

    var report: wire.TotalCurrencyBalanceReport = .{
        .account_balances = null,
        .fees = null,
        .adjustments = null,
        .suspends = null,
        .order_balances = null,
        .open_interest_long = null,
        .open_interest_short = null,
    };
    errdefer report.deinit(allocator);
    report.account_balances = try balances.ownEntries(allocator);
    report.fees = try ownCopiedIntLongMap(
        allocator,
        &engine.fees,
        engine.validatedFeesHistory() catch |err| return mapHistoryError(err),
    );
    report.adjustments = try ownCopiedIntLongMap(
        allocator,
        &engine.adjustments,
        engine.validatedAdjustmentsHistory() catch |err| return mapHistoryError(err),
    );
    report.suspends = try ownCopiedIntLongMap(
        allocator,
        &engine.suspends,
        engine.validatedSuspendsHistory() catch |err| return mapHistoryError(err),
    );
    report.open_interest_long = try open_long.ownEntries(allocator);
    report.open_interest_short = try open_short.ownEntries(allocator);
    return report;
}

fn ownCopiedIntLongMap(
    allocator: std.mem.Allocator,
    map: *const std.AutoHashMapUnmanaged(i32, i64),
    source_history: *const int_map_order.History,
) ProducerError![]const wire.IntLongEntry {
    var copied_history = source_history.copyForIntLong(allocator) catch |err|
        return mapHistoryError(err);
    defer copied_history.deinit(allocator);
    return ownIntLongMapWithHistory(allocator, map, &copied_history);
}

fn javaEstimatedProfit(
    engine: *const risk.RiskEngine,
    position: user.SymbolPositionRecord,
    specification: common.CoreSymbolSpecification,
) i64 {
    const average: i64 = if (engine.last_price_cache.get(position.symbol)) |price|
        (price.ask_price +% price.bid_price) >> 1
    else
        42;
    return switch (position.direction) {
        .empty => position.profit,
        .long => position.profit +% (if (average != 0)
            position.open_volume *% average -% position.open_price_sum
        else
            specification.margin_buy *% position.open_volume),
        .short => position.profit +% (if (average != std.math.maxInt(i64))
            position.open_price_sum -% position.open_volume *% average
        else
            specification.margin_sell *% position.open_volume),
    };
}

fn javaUserProfileServiceHash(
    engine: *const risk.RiskEngine,
    identity: JavaIdentityHashContext,
) i32 {
    var sum: i64 = 0;
    var iterator = engine.user_profile_service.user_profiles.iterator();
    while (iterator.next()) |entry| {
        const fields = [_]i32{
            java_hash32.longHashCode(entry.key_ptr.*),
            javaUserProfileHash(entry.value_ptr, identity),
        };
        sum +%= java_hash32.objectsHash(&fields);
    }
    return java_hash32.longHashCode(sum);
}

fn javaUserProfileHash(profile: *const user.UserProfile, identity: JavaIdentityHashContext) i32 {
    const fields = [_]i32{
        java_hash32.longHashCode(profile.uid),
        javaPositionMapHash(&profile.positions),
        java_hash32.longHashCode(profile.adjustments_counter),
        javaIntLongMapHash(&profile.accounts),
        identity.userStatus(profile.user_status),
    };
    return java_hash32.objectsHash(&fields);
}

fn javaPositionMapHash(map: *const std.AutoHashMapUnmanaged(i32, user.SymbolPositionRecord)) i32 {
    var sum: i64 = 0;
    var iterator = map.iterator();
    while (iterator.next()) |entry| {
        const position = entry.value_ptr.*;
        const position_fields = [_]i32{
            position.symbol,
            position.currency,
            @intFromEnum(position.direction),
            java_hash32.longHashCode(position.open_volume),
            java_hash32.longHashCode(position.open_price_sum),
            java_hash32.longHashCode(position.profit),
            java_hash32.longHashCode(position.pending_sell_size),
            java_hash32.longHashCode(position.pending_buy_size),
        };
        const map_fields = [_]i32{ entry.key_ptr.*, java_hash32.objectsHash(&position_fields) };
        sum +%= java_hash32.objectsHash(&map_fields);
    }
    return java_hash32.longHashCode(sum);
}

fn javaLastPriceCacheHash(engine: *const risk.RiskEngine) i32 {
    var sum: i64 = 0;
    var iterator = engine.last_price_cache.iterator();
    while (iterator.next()) |entry| {
        const price = entry.value_ptr.*;
        const price_fields = [_]i32{
            java_hash32.longHashCode(price.ask_price),
            java_hash32.longHashCode(price.bid_price),
        };
        const map_fields = [_]i32{ entry.key_ptr.*, java_hash32.objectsHash(&price_fields) };
        sum +%= java_hash32.objectsHash(&map_fields);
    }
    return java_hash32.longHashCode(sum);
}

fn javaIntLongMapHash(map: *const std.AutoHashMapUnmanaged(i32, i64)) i32 {
    var hash: i32 = 0;
    var iterator = map.iterator();
    while (iterator.next()) |entry| {
        const key_bits: u32 = @bitCast(entry.key_ptr.*);
        const value_bits: u32 = @bitCast(java_hash32.longHashCode(entry.value_ptr.*));
        hash +%= @as(i32, @bitCast(key_bits ^ value_bits));
    }
    return hash;
}

fn javaOrderBooksHash(
    books: []const MatchingBookView,
    identity: JavaIdentityHashContext,
) i32 {
    var sum: i64 = 0;
    for (books) |book| {
        const map_fields = [_]i32{
            book.symbol_specification.symbol_id,
            javaOrderBookHash(book, identity),
        };
        sum +%= java_hash32.objectsHash(&map_fields);
    }
    return java_hash32.longHashCode(sum);
}

fn javaOrderBookHash(book: MatchingBookView, identity: JavaIdentityHashContext) i32 {
    var asks: i32 = 0;
    var bids: i32 = 0;
    for (book.orders) |order| {
        const hash = javaOrderHash(order, identity);
        switch (order.action) {
            .ask => asks = java_hash32.wrappingMulAddI32(asks, 31, hash),
            .bid => bids = java_hash32.wrappingMulAddI32(bids, 31, hash),
        }
    }
    const fields = [_]i32{ asks, bids, javaSpecificationHash(book.symbol_specification) };
    return java_hash32.objectsHash(&fields);
}

fn javaOrderHash(order: orderbook.UserOrderView, identity: JavaIdentityHashContext) i32 {
    const fields = [_]i32{
        java_hash32.longHashCode(order.order_id),
        identity.orderAction(order.action),
        java_hash32.longHashCode(order.price),
        java_hash32.longHashCode(order.size),
        java_hash32.longHashCode(order.reserve_bid_price),
        java_hash32.longHashCode(order.filled),
        java_hash32.longHashCode(order.uid),
    };
    return java_hash32.objectsHash(&fields);
}

fn javaSpecificationHash(specification: common.CoreSymbolSpecification) i32 {
    const fields = [_]i32{
        specification.symbol_id,
        specification.symbol_type.code(),
        specification.base_currency,
        specification.quote_currency,
        java_hash32.longHashCode(specification.base_scale_k),
        java_hash32.longHashCode(specification.quote_scale_k),
        java_hash32.longHashCode(specification.taker_fee),
        java_hash32.longHashCode(specification.maker_fee),
        java_hash32.longHashCode(specification.margin_buy),
        java_hash32.longHashCode(specification.margin_sell),
    };
    return java_hash32.objectsHash(&fields);
}

fn stateHashResult(
    entries: []const wire.StateHashEntry,
    identity: JavaIdentityHashContext,
) i32 {
    var result: i32 = 1;
    for (entries) |entry| {
        // Lombok @EqualsAndHashCode for SubmoduleKey uses prime 59 in field
        // declaration order and the enum object's JVM identity hash.
        var key_hash: i32 = 1;
        key_hash = java_hash32.wrappingMulAddI32(key_hash, 59, entry.module_id);
        key_hash = java_hash32.wrappingMulAddI32(
            key_hash,
            59,
            identity.stateHashSubmodule(entry.submodule),
        );
        const entry_fields = [_]i32{ key_hash, entry.hash };
        result = java_hash32.wrappingMulAddI32(
            result,
            31,
            java_hash32.objectsHash(&entry_fields),
        );
    }
    return result;
}

fn lessOrderId(_: void, lhs: wire.Order, rhs: wire.Order) bool {
    return lhs.order_id < rhs.order_id;
}

fn testExchangeSpecification(base_scale_k: i64) !common.CoreSymbolSpecification {
    return common.CoreSymbolSpecification.init(.{
        .symbol_id = 241,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = base_scale_k,
        .quote_scale_k = 10,
        .taker_fee = 2,
        .maker_fee = 1,
    });
}

fn testMapValue(entries: ?[]const wire.IntLongEntry, key: i32) ?i64 {
    for (entries orelse return null) |entry| {
        if (entry.key == key) return entry.value;
    }
    return null;
}

test "exact total balance keeps zero keys and Java wrapping through real sections" {
    const allocator = std.testing.allocator;
    var engine = try risk.RiskEngine.init(allocator, 0, 1, .{});
    defer engine.deinit();
    var processor = binary_frames.BinaryFrameAssembler.init(allocator);
    defer processor.deinit();

    const orders = [_]orderbook.UserOrderView{.{
        .symbol = 241,
        .order_id = 7,
        .price = 100,
        .size = 2,
        .filled = 0,
        .reserve_bid_price = 0,
        .action = .ask,
        .uid = 42,
        .timestamp = 1,
    }};
    const books = [_]MatchingBookView{.{
        .traversal = .naive,
        .symbol_specification = try testExchangeSpecification(std.math.maxInt(i64)),
        .orders = &orders,
    }};
    const matching = [_]MatchingSectionView{.{
        .shard_id = 0,
        .shard_mask = 0,
        .binary_processor = &processor,
        .books = &books,
    }};
    const risks = [_]RiskSectionView{.{
        .shard_id = 0,
        .shard_mask = 0,
        .binary_processor = &processor,
        .engine = &engine,
    }};

    var produced = try produce(
        allocator,
        .total_currency_balance,
        &matching,
        &risks,
        null,
        .{},
    );
    defer produced.deinit(allocator);
    switch (produced.response) {
        .total_currency_balance => |report| {
            try std.testing.expectEqual(@as(?i64, -2), testMapValue(report.order_balances, 11));
            try std.testing.expectEqual(@as(?i64, 0), testMapValue(report.order_balances, 15));
            try std.testing.expect(report.account_balances != null);
            try std.testing.expect(report.fees != null);
            try std.testing.expect(report.adjustments != null);
            try std.testing.expect(report.suspends != null);
            try std.testing.expect(report.open_interest_long != null);
            try std.testing.expect(report.open_interest_short != null);
        },
        else => unreachable,
    }
    var grouped = try wire.groupMatcherEventChain(allocator, produced.events_head, .{});
    defer grouped.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 2), grouped.sections.len);
    try std.testing.expectEqual(wire.EngineSection.risk, grouped.sections[0].label.engine);
    try std.testing.expectEqual(wire.EngineSection.matching, grouped.sections[1].label.engine);
}

test "report producers preserve EC sentinel order and IntLong tombstone copy semantics" {
    const allocator = std.testing.allocator;

    var accumulator: OrderedWrappingMap = .{};
    defer accumulator.deinit(allocator);
    for ([_]i32{ 2, 17, 33, 1, 0 }) |key| {
        try accumulator.add(allocator, key, if (key == 0) 0 else key);
    }
    const entries = try accumulator.ownEntries(allocator);
    defer allocator.free(entries);
    const expected = [_]i32{ 0, 1, 17, 2, 33 };
    for (expected, 0..) |key, index| {
        try std.testing.expectEqual(key, entries[index].key);
    }

    const empty = wire.TotalCurrencyBalanceReport{
        .account_balances = null,
        .fees = null,
        .adjustments = null,
        .suspends = null,
        .order_balances = entries,
        .open_interest_long = null,
        .open_interest_short = null,
    };
    const body = try wire.encodeTotalCurrencyBalanceBody(allocator, empty, .{});
    defer allocator.free(body);
    // N N N N, Y, count, then five key/value pairs.
    for (expected, 0..) |key, index| {
        const offset = 9 + index * 12;
        try std.testing.expectEqual(
            key,
            std.mem.readInt(i32, body[offset..][0..4], .little),
        );
    }

    var engine = try risk.RiskEngine.init(allocator, 0, 1, .{});
    defer engine.deinit();
    try engine.fees.put(allocator, 33, 330);
    try engine.fees.put(allocator, 2, 20);
    var layout = try int_map_order.Layout.init(allocator);
    for ([_]i32{ 33, 2, 17 }) |key| _ = try layout.put(allocator, key);
    try std.testing.expect(layout.remove(17));
    var history = int_map_order.History.initKnown(layout);
    errdefer history.deinit(allocator);
    try engine.replaceFeesHistory(&history);

    var report = try riskTotalBalanceReport(allocator, &engine);
    defer report.deinit(allocator);
    try std.testing.expectEqualSlices(
        i32,
        &.{ 33, 2 },
        &.{ report.fees.?[0].key, report.fees.?[1].key },
    );
}

test "report producer fails closed on semantic map and history mismatch" {
    const allocator = std.testing.allocator;
    var engine = try risk.RiskEngine.init(allocator, 0, 1, .{});
    defer engine.deinit();
    try std.testing.expect(
        try engine.user_profile_service.addEmptyUserProfile(8001),
    );
    const profile = engine.getUserProfile(8001) orelse unreachable;
    // Deliberately model an old/native caller that changed semantic state
    // without reconstructing the required EC sidecar.
    try profile.accounts.put(allocator, 15, 100);
    try std.testing.expectError(
        error.HistorySemanticKeySetMismatch,
        riskTotalBalanceReport(allocator, &engine),
    );
}

fn produceAllocationFailure(allocator: std.mem.Allocator) !void {
    var engine = try risk.RiskEngine.init(std.testing.allocator, 0, 1, .{});
    defer engine.deinit();
    var processor = binary_frames.BinaryFrameAssembler.init(std.testing.allocator);
    defer processor.deinit();
    const books = [_]MatchingBookView{.{
        .traversal = .direct,
        .symbol_specification = try testExchangeSpecification(100),
        .orders = &.{},
    }};
    const matching = [_]MatchingSectionView{.{
        .shard_id = 0,
        .shard_mask = 0,
        .binary_processor = &processor,
        .books = &books,
    }};
    const risks = [_]RiskSectionView{.{
        .shard_id = 0,
        .shard_mask = 0,
        .binary_processor = &processor,
        .engine = &engine,
    }};
    var produced = try produce(
        allocator,
        .total_currency_balance,
        &matching,
        &risks,
        null,
        .{},
    );
    produced.deinit(allocator);
}

test "exact report production is allocation-failure atomic" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        produceAllocationFailure,
        .{},
    );
}
