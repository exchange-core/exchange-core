const std = @import("std");
const core_events = @import("../../events.zig");
const int_map_order = @import("eclipse_collections/int_map_order.zig");

fn orderedHistoryKeys(
    history: *const int_map_order.History,
    allocator: std.mem.Allocator,
) WireError![]i32 {
    return history.orderedKeys(allocator) catch |err| switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        error.CapacityOverflow => error.CapacityOverflow,
        error.HistoryUnavailable => error.HistoryUnavailable,
        error.InvalidTableSize,
        error.InvalidFlag,
        error.InvalidOccupancy,
        error.DuplicateActiveKey,
        error.UnreachableActiveKey,
        => error.HistorySemanticKeySetMismatch,
    };
}

/// Java's `OrderBookEventsHelper` stores five signed long carrier words in
/// every BINARY_EVENT. The signedness is incidental: the bytes are packed in
/// Java `ByteBuffer`'s default big-endian order.
pub const words_per_event: usize = 5;
pub const bytes_per_event: usize = words_per_event * @sizeOf(i64);
pub const matching_section_offset: i32 = 1024;

/// The Java section numbering is only unambiguous while both shard sets stay
/// below the matching-engine offset.
pub const max_unambiguous_shard_id: i32 = matching_section_offset - 1;

pub const Config = struct {
    max_body_bytes: usize = 64 * 1024 * 1024,
    max_collection_entries: usize = 1_000_000,
    max_total_orders: usize = 1_000_000,
};

pub const WireError = std.mem.Allocator.Error || error{
    BodyTooLarge,
    CarrierTooLarge,
    CapacityOverflow,
    CountLimitExceeded,
    DuplicateKey,
    EmptyEventChain,
    HistorySemanticKeySetMismatch,
    HistoryUnavailable,
    InvalidSectionId,
    InvalidShardId,
    NegativeCount,
    NonZeroCarrierPadding,
    NonBinaryEvent,
    SectionMismatch,
    TrailingBytes,
    TruncatedPayload,
    UnknownOrderAction,
    UnknownPositionDirection,
    UnknownQueryExecutionStatus,
    UnknownStateHashSubmodule,
    UnknownUserStatus,
};

pub const EngineSection = enum {
    risk,
    matching,
};

/// A canonical label for the external section id supplied to Java's risk or
/// matching `BinaryCommandsProcessor`.
pub const SectionLabel = struct {
    engine: EngineSection,
    shard_id: i32,

    pub fn risk(shard_id: i32) WireError!SectionLabel {
        try validateShardId(shard_id);
        return .{ .engine = .risk, .shard_id = shard_id };
    }

    pub fn matching(shard_id: i32) WireError!SectionLabel {
        try validateShardId(shard_id);
        return .{ .engine = .matching, .shard_id = shard_id };
    }

    pub fn externalId(self: SectionLabel) i32 {
        return switch (self.engine) {
            .risk => self.shard_id,
            .matching => self.shard_id + matching_section_offset,
        };
    }

    pub fn fromExternalId(section_id: i32) WireError!SectionLabel {
        if (section_id < 0 or section_id > matching_section_offset + max_unambiguous_shard_id) {
            return error.InvalidSectionId;
        }
        if (section_id < matching_section_offset) {
            return .{ .engine = .risk, .shard_id = section_id };
        }
        return .{
            .engine = .matching,
            .shard_id = section_id - matching_section_offset,
        };
    }
};

fn validateShardId(shard_id: i32) WireError!void {
    if (shard_id < 0 or shard_id > max_unambiguous_shard_id) return error.InvalidShardId;
}

pub const BinaryEventCarrier = struct {
    section_id: i32,
    words: [words_per_event]i64,
};

/// One Java `TreeMap` entry after mixed matcher events have been grouped by
/// their external section id. Carrier order inside a section is the encounter
/// order from the matcher-event chain.
pub const GroupedBinaryEventSection = struct {
    label: SectionLabel,
    carriers: []const BinaryEventCarrier,

    fn deinit(self: *GroupedBinaryEventSection, allocator: std.mem.Allocator) void {
        allocator.free(self.carriers);
        self.* = undefined;
    }
};

/// Owned, external-section-sorted equivalent of Java's
/// `OrderBookEventsHelper.deserializeEvents` result.
pub const GroupedBinaryEvents = struct {
    sections: []GroupedBinaryEventSection,

    pub fn deinit(self: *GroupedBinaryEvents, allocator: std.mem.Allocator) void {
        for (self.sections) |*section| section.deinit(allocator);
        allocator.free(self.sections);
        self.* = undefined;
    }
};

const IndexedCarrier = struct {
    carrier: BinaryEventCarrier,
    encounter_index: usize,
};

/// Groups an out-of-order carrier stream exactly as Java groups matcher
/// events: all carriers with one external section id are concatenated in
/// encounter order, and the resulting sections are sorted by external id.
/// Repeated section ids therefore become one logical byte stream rather than
/// multiple independently decoded report bodies.
pub fn groupBinaryEventCarriers(
    allocator: std.mem.Allocator,
    carriers: []const BinaryEventCarrier,
    config: Config,
) WireError!GroupedBinaryEvents {
    if (carriers.len > config.max_collection_entries) return error.CountLimitExceeded;

    const indexed = try allocator.alloc(IndexedCarrier, carriers.len);
    defer allocator.free(indexed);
    for (carriers, 0..) |carrier, index| {
        _ = try SectionLabel.fromExternalId(carrier.section_id);
        indexed[index] = .{
            .carrier = carrier,
            .encounter_index = index,
        };
    }
    std.sort.heap(IndexedCarrier, indexed, {}, lessIndexedCarrier);

    var section_count: usize = 0;
    var cursor: usize = 0;
    while (cursor < indexed.len) {
        section_count += 1;
        const section_id = indexed[cursor].carrier.section_id;
        cursor += 1;
        while (cursor < indexed.len and indexed[cursor].carrier.section_id == section_id) {
            cursor += 1;
        }
    }
    if (section_count > config.max_collection_entries) return error.CountLimitExceeded;

    const sections = try allocator.alloc(GroupedBinaryEventSection, section_count);
    var initialized: usize = 0;
    errdefer {
        for (sections[0..initialized]) |*section| section.deinit(allocator);
        allocator.free(sections);
    }

    cursor = 0;
    while (cursor < indexed.len) {
        const start = cursor;
        const section_id = indexed[start].carrier.section_id;
        cursor += 1;
        while (cursor < indexed.len and indexed[cursor].carrier.section_id == section_id) {
            cursor += 1;
        }

        const carrier_count = cursor - start;
        const padded_size = std.math.mul(usize, carrier_count, bytes_per_event) catch
            return error.CarrierTooLarge;
        try checkPaddedBodySize(padded_size, config);

        const owned = try allocator.alloc(BinaryEventCarrier, carrier_count);
        errdefer allocator.free(owned);
        for (owned, indexed[start..cursor]) |*destination, source| {
            destination.* = source.carrier;
        }
        sections[initialized] = .{
            .label = try SectionLabel.fromExternalId(section_id),
            .carriers = owned,
        };
        initialized += 1;
    }

    return .{ .sections = sections };
}

/// Converts a real matcher-event chain into Java's grouped section view.
/// Query report chains may contain only BINARY_EVENT nodes; accepting a trade,
/// reduce, or reject node would reinterpret unrelated matching data as bytes.
pub fn groupMatcherEventChain(
    allocator: std.mem.Allocator,
    head: ?*const core_events.MatcherTradeEvent,
    config: Config,
) WireError!GroupedBinaryEvents {
    var carriers: std.ArrayListUnmanaged(BinaryEventCarrier) = .empty;
    defer carriers.deinit(allocator);

    var node = head;
    while (node) |event| : (node = event.next_event) {
        if (event.event_type != .binary_event) return error.NonBinaryEvent;
        if (carriers.items.len == config.max_collection_entries) {
            return error.CountLimitExceeded;
        }
        try carriers.append(allocator, .{
            .section_id = event.section,
            .words = .{
                event.matched_order_id,
                event.matched_order_uid,
                event.price,
                event.size,
                event.bidder_hold_price,
            },
        });
    }

    return groupBinaryEventCarriers(allocator, carriers.items, config);
}

pub const UnpackedSection = struct {
    label: SectionLabel,
    /// Includes the zero tail that pads the last event to five whole longs.
    padded_bytes: []u8,

    pub fn deinit(self: *UnpackedSection, allocator: std.mem.Allocator) void {
        allocator.free(self.padded_bytes);
        self.* = undefined;
    }
};

pub const StateHashSubmodule = enum(i32) {
    risk_symbol_spec_provider = 0,
    risk_user_profile_service = 1,
    risk_binary_cmd_processor = 2,
    risk_last_price_cache = 3,
    risk_fees = 4,
    risk_adjustments = 5,
    risk_suspends = 6,
    risk_shard_mask = 7,
    matching_binary_cmd_processor = 64,
    matching_order_books = 65,
    matching_shard_mask = 66,

    fn fromCode(code: i32) WireError!StateHashSubmodule {
        return switch (code) {
            0 => .risk_symbol_spec_provider,
            1 => .risk_user_profile_service,
            2 => .risk_binary_cmd_processor,
            3 => .risk_last_price_cache,
            4 => .risk_fees,
            5 => .risk_adjustments,
            6 => .risk_suspends,
            7 => .risk_shard_mask,
            64 => .matching_binary_cmd_processor,
            65 => .matching_order_books,
            66 => .matching_shard_mask,
            else => error.UnknownStateHashSubmodule,
        };
    }
};

pub const StateHashEntry = struct {
    module_id: i32,
    submodule: StateHashSubmodule,
    hash: i32,
};

pub const StateHashReport = struct {
    entries: []const StateHashEntry,

    pub fn deinit(self: *StateHashReport, allocator: std.mem.Allocator) void {
        allocator.free(self.entries);
        self.* = undefined;
    }
};

pub const IntLongEntry = struct {
    key: i32,
    value: i64,
};

pub const TotalCurrencyBalanceReport = struct {
    account_balances: ?[]const IntLongEntry,
    fees: ?[]const IntLongEntry,
    adjustments: ?[]const IntLongEntry,
    suspends: ?[]const IntLongEntry,
    order_balances: ?[]const IntLongEntry,
    open_interest_long: ?[]const IntLongEntry,
    open_interest_short: ?[]const IntLongEntry,

    pub fn deinit(self: *TotalCurrencyBalanceReport, allocator: std.mem.Allocator) void {
        freeOptionalIntLongMap(allocator, self.account_balances);
        freeOptionalIntLongMap(allocator, self.fees);
        freeOptionalIntLongMap(allocator, self.adjustments);
        freeOptionalIntLongMap(allocator, self.suspends);
        freeOptionalIntLongMap(allocator, self.order_balances);
        freeOptionalIntLongMap(allocator, self.open_interest_long);
        freeOptionalIntLongMap(allocator, self.open_interest_short);
        self.* = undefined;
    }
};

fn freeOptionalIntLongMap(allocator: std.mem.Allocator, entries: ?[]const IntLongEntry) void {
    if (entries) |present| allocator.free(present);
}

pub const UserStatus = enum(i8) {
    active = 0,
    suspended = 1,

    fn fromCode(code: i8) WireError!UserStatus {
        return switch (code) {
            0 => .active,
            1 => .suspended,
            else => error.UnknownUserStatus,
        };
    }
};

pub const PositionDirection = enum(i8) {
    short = -1,
    empty = 0,
    long = 1,

    fn fromCode(code: i8) WireError!PositionDirection {
        return switch (code) {
            -1 => .short,
            0 => .empty,
            1 => .long,
            else => error.UnknownPositionDirection,
        };
    }
};

pub const OrderAction = enum(i8) {
    ask = 0,
    bid = 1,

    fn fromCode(code: i8) WireError!OrderAction {
        return switch (code) {
            0 => .ask,
            1 => .bid,
            else => error.UnknownOrderAction,
        };
    }
};

pub const QueryExecutionStatus = enum(i32) {
    ok = 0,
    user_not_found = 1,

    fn fromCode(code: i32) WireError!QueryExecutionStatus {
        return switch (code) {
            0 => .ok,
            1 => .user_not_found,
            else => error.UnknownQueryExecutionStatus,
        };
    }
};

pub const PositionEntry = struct {
    symbol: i32,
    quote_currency: i32,
    direction: PositionDirection,
    open_volume: i64,
    open_price_sum: i64,
    profit: i64,
    pending_sell_size: i64,
    pending_buy_size: i64,
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

pub const SymbolOrders = struct {
    symbol: i32,
    orders: []const Order,
};

pub const SingleUserReport = struct {
    uid: i64,
    user_status: ?UserStatus,
    accounts: ?[]const IntLongEntry,
    positions: ?[]const PositionEntry,
    orders: ?[]const SymbolOrders,
    query_execution_status: QueryExecutionStatus,

    pub fn deinit(self: *SingleUserReport, allocator: std.mem.Allocator) void {
        freeOptionalIntLongMap(allocator, self.accounts);
        if (self.positions) |positions| allocator.free(positions);
        if (self.orders) |groups| {
            for (groups) |group| allocator.free(group.orders);
            allocator.free(groups);
        }
        self.* = undefined;
    }
};

pub const StateHashEventSection = struct {
    label: SectionLabel,
    report: StateHashReport,

    pub fn deinit(self: *StateHashEventSection, allocator: std.mem.Allocator) void {
        self.report.deinit(allocator);
        self.* = undefined;
    }
};

pub const TotalBalanceEventSection = struct {
    label: SectionLabel,
    report: TotalCurrencyBalanceReport,

    pub fn deinit(self: *TotalBalanceEventSection, allocator: std.mem.Allocator) void {
        self.report.deinit(allocator);
        self.* = undefined;
    }
};

pub const SingleUserEventSection = struct {
    label: SectionLabel,
    report: SingleUserReport,

    pub fn deinit(self: *SingleUserEventSection, allocator: std.mem.Allocator) void {
        self.report.deinit(allocator);
        self.* = undefined;
    }
};

/// Packs an uncompressed native-endian Chronicle report body into Java's
/// five-long BINARY_EVENT carriers.
pub fn packSectionEvents(
    allocator: std.mem.Allocator,
    label: SectionLabel,
    body: []const u8,
) WireError![]BinaryEventCarrier {
    try validateShardId(label.shard_id);
    const rounded = std.math.add(usize, body.len, bytes_per_event - 1) catch return error.CarrierTooLarge;
    const event_count = rounded / bytes_per_event;
    const events = try allocator.alloc(BinaryEventCarrier, event_count);
    errdefer allocator.free(events);

    for (events, 0..) |*event, event_index| {
        event.section_id = label.externalId();
        event.words = [_]i64{0} ** words_per_event;
        for (0..words_per_event) |word_index| {
            const body_start = event_index * bytes_per_event + word_index * @sizeOf(i64);
            if (body_start >= body.len) break;
            const body_end = @min(body_start + @sizeOf(i64), body.len);
            var raw = [_]u8{0} ** @sizeOf(i64);
            @memcpy(raw[0 .. body_end - body_start], body[body_start..body_end]);
            event.words[word_index] = std.mem.readInt(i64, &raw, .big);
        }
    }
    return events;
}

/// Reassembles carrier words. Schema-specific decoders below identify the
/// actual body boundary and require every byte after it to be zero.
pub fn unpackSectionEvents(
    allocator: std.mem.Allocator,
    events: []const BinaryEventCarrier,
) WireError!UnpackedSection {
    if (events.len == 0) return error.EmptyEventChain;
    const label = try SectionLabel.fromExternalId(events[0].section_id);
    const byte_count = std.math.mul(usize, events.len, bytes_per_event) catch return error.CarrierTooLarge;
    const bytes = try allocator.alloc(u8, byte_count);
    errdefer allocator.free(bytes);

    for (events, 0..) |event, event_index| {
        if (event.section_id != events[0].section_id) return error.SectionMismatch;
        for (event.words, 0..) |word, word_index| {
            const start = event_index * bytes_per_event + word_index * @sizeOf(i64);
            std.mem.writeInt(i64, bytes[start..][0..@sizeOf(i64)], word, .big);
        }
    }
    return .{ .label = label, .padded_bytes = bytes };
}

pub fn encodeStateHashBody(
    allocator: std.mem.Allocator,
    report: StateHashReport,
    config: Config,
) WireError![]u8 {
    var output: std.Io.Writer.Allocating = .init(allocator);
    errdefer output.deinit();
    const writer = &output.writer;

    const sorted = try allocator.dupe(StateHashEntry, report.entries);
    defer allocator.free(sorted);
    std.sort.heap(StateHashEntry, sorted, {}, lessStateHashEntry);
    try rejectDuplicateStateHashKeys(sorted);
    try writeCount(writer, sorted.len, config);
    for (sorted) |entry| {
        try writeIntLe(writer, i32, entry.module_id);
        try writeIntLe(writer, i32, @intFromEnum(entry.submodule));
        try writeIntLe(writer, i32, entry.hash);
    }
    return finishEncodedBody(&output, config);
}

pub fn decodeStateHashBody(
    allocator: std.mem.Allocator,
    body: []const u8,
    config: Config,
) WireError!StateHashReport {
    try checkExactBodySize(body.len, config);
    var reader = SliceReader.init(body);
    var report = try parseStateHash(allocator, &reader, config);
    errdefer report.deinit(allocator);
    try reader.finishExact();
    return report;
}

pub fn decodeStateHashEvents(
    allocator: std.mem.Allocator,
    events: []const BinaryEventCarrier,
    config: Config,
) WireError!StateHashEventSection {
    var unpacked = try unpackSectionEvents(allocator, events);
    defer unpacked.deinit(allocator);
    try checkPaddedBodySize(unpacked.padded_bytes.len, config);
    var reader = SliceReader.init(unpacked.padded_bytes);
    var report = try parseStateHash(allocator, &reader, config);
    errdefer report.deinit(allocator);
    try checkConsumedBodySize(reader.pos, config);
    try reader.finishZeroPadding();
    return .{ .label = unpacked.label, .report = report };
}

pub fn encodeTotalCurrencyBalanceBody(
    allocator: std.mem.Allocator,
    report: TotalCurrencyBalanceReport,
    config: Config,
) WireError![]u8 {
    var output: std.Io.Writer.Allocating = .init(allocator);
    errdefer output.deinit();
    const writer = &output.writer;

    try writeNullableIntLongMap(allocator, writer, report.account_balances, config);
    try writeNullableIntLongMap(allocator, writer, report.fees, config);
    try writeNullableIntLongMap(allocator, writer, report.adjustments, config);
    try writeNullableIntLongMap(allocator, writer, report.suspends, config);
    try writeNullableIntLongMap(allocator, writer, report.order_balances, config);
    try writeNullableIntLongMap(allocator, writer, report.open_interest_long, config);
    try writeNullableIntLongMap(allocator, writer, report.open_interest_short, config);
    return finishEncodedBody(&output, config);
}

pub fn decodeTotalCurrencyBalanceBody(
    allocator: std.mem.Allocator,
    body: []const u8,
    config: Config,
) WireError!TotalCurrencyBalanceReport {
    try checkExactBodySize(body.len, config);
    var reader = SliceReader.init(body);
    var report = try parseTotalCurrencyBalance(allocator, &reader, config);
    errdefer report.deinit(allocator);
    try reader.finishExact();
    return report;
}

pub fn decodeTotalCurrencyBalanceEvents(
    allocator: std.mem.Allocator,
    events: []const BinaryEventCarrier,
    config: Config,
) WireError!TotalBalanceEventSection {
    var unpacked = try unpackSectionEvents(allocator, events);
    defer unpacked.deinit(allocator);
    try checkPaddedBodySize(unpacked.padded_bytes.len, config);
    var reader = SliceReader.init(unpacked.padded_bytes);
    var report = try parseTotalCurrencyBalance(allocator, &reader, config);
    errdefer report.deinit(allocator);
    try checkConsumedBodySize(reader.pos, config);
    try reader.finishZeroPadding();
    return .{ .label = unpacked.label, .report = report };
}

pub fn encodeSingleUserBody(
    allocator: std.mem.Allocator,
    report: SingleUserReport,
    config: Config,
) WireError![]u8 {
    var output: std.Io.Writer.Allocating = .init(allocator);
    errdefer output.deinit();
    const writer = &output.writer;

    try writeIntLe(writer, i64, report.uid);
    try writeBoolean(writer, report.user_status != null);
    if (report.user_status) |status| try writeIntLe(writer, i8, @intFromEnum(status));
    try writeNullableIntLongMap(allocator, writer, report.accounts, config);
    try writeNullablePositions(allocator, writer, report.positions, config);
    try writeNullableOrders(allocator, writer, report.orders, config);
    try writeIntLe(writer, i32, @intFromEnum(report.query_execution_status));
    return finishEncodedBody(&output, config);
}

pub fn decodeSingleUserBody(
    allocator: std.mem.Allocator,
    body: []const u8,
    config: Config,
) WireError!SingleUserReport {
    try checkExactBodySize(body.len, config);
    var reader = SliceReader.init(body);
    var report = try parseSingleUser(allocator, &reader, config);
    errdefer report.deinit(allocator);
    try reader.finishExact();
    return report;
}

pub fn decodeSingleUserEvents(
    allocator: std.mem.Allocator,
    events: []const BinaryEventCarrier,
    config: Config,
) WireError!SingleUserEventSection {
    var unpacked = try unpackSectionEvents(allocator, events);
    defer unpacked.deinit(allocator);
    try checkPaddedBodySize(unpacked.padded_bytes.len, config);
    var reader = SliceReader.init(unpacked.padded_bytes);
    var report = try parseSingleUser(allocator, &reader, config);
    errdefer report.deinit(allocator);
    try checkConsumedBodySize(reader.pos, config);
    try reader.finishZeroPadding();
    return .{ .label = unpacked.label, .report = report };
}

const StateHashMergeKey = struct {
    module_id: i32,
    submodule: StateHashSubmodule,
};

/// Decodes every grouped section and applies Java's `TreeMap.putAll` reducer.
/// A repeated logical state key from a later external section replaces the
/// earlier value. An empty section set returns an owned empty report.
pub fn mergeStateHashGroupedEvents(
    allocator: std.mem.Allocator,
    grouped: *const GroupedBinaryEvents,
    config: Config,
) WireError!StateHashReport {
    try validateGroupedBinaryEvents(grouped, config);

    var values: std.AutoHashMapUnmanaged(StateHashMergeKey, i32) = .empty;
    defer values.deinit(allocator);

    for (grouped.sections) |section| {
        var decoded = try decodeStateHashEvents(allocator, section.carriers, config);
        defer decoded.deinit(allocator);
        for (decoded.report.entries) |entry| {
            const key = StateHashMergeKey{
                .module_id = entry.module_id,
                .submodule = entry.submodule,
            };
            if (!values.contains(key) and values.count() == config.max_collection_entries) {
                return error.CountLimitExceeded;
            }
            try values.put(allocator, key, entry.hash);
        }
    }

    const entries = try allocator.alloc(StateHashEntry, values.count());
    errdefer allocator.free(entries);
    var index: usize = 0;
    var iterator = values.iterator();
    while (iterator.next()) |entry| : (index += 1) {
        entries[index] = .{
            .module_id = entry.key_ptr.module_id,
            .submodule = entry.key_ptr.submodule,
            .hash = entry.value_ptr.*,
        };
    }
    std.sort.heap(StateHashEntry, entries, {}, lessStateHashEntry);
    return .{ .entries = entries };
}

/// Groups and merges a matcher-event chain into an owned Java StateHash
/// result. The returned report must be released with `StateHashReport.deinit`.
pub fn mergeStateHashEventChain(
    allocator: std.mem.Allocator,
    head: ?*const core_events.MatcherTradeEvent,
    config: Config,
) WireError!StateHashReport {
    var grouped = try groupMatcherEventChain(allocator, head, config);
    defer grouped.deinit(allocator);
    return mergeStateHashGroupedEvents(allocator, &grouped, config);
}

const OwnedOrderMap = std.AutoHashMapUnmanaged(i32, []Order);

const OrderedOrderMap = struct {
    values: OwnedOrderMap = .empty,
    history: int_map_order.History = int_map_order.History.init(),
    total_orders: usize = 0,

    fn deinit(self: *OrderedOrderMap, allocator: std.mem.Allocator) void {
        freeOwnedOrderMapValues(allocator, &self.values);
        self.values.deinit(allocator);
        self.history.deinit(allocator);
        self.* = undefined;
    }

    /// Models `new IntObjectHashMap<>(a)` before each Java reduction step.
    fn copyForReduction(
        self: *const OrderedOrderMap,
        allocator: std.mem.Allocator,
    ) WireError!OrderedOrderMap {
        var copy = OrderedOrderMap{
            .history = try self.history.copyForIntObject(allocator),
            .total_orders = self.total_orders,
        };
        errdefer copy.deinit(allocator);
        try copy.values.ensureTotalCapacity(allocator, self.values.count());
        var iterator = self.values.iterator();
        while (iterator.next()) |entry| {
            const orders = try allocator.dupe(Order, entry.value_ptr.*);
            copy.values.putAssumeCapacityNoClobber(entry.key_ptr.*, orders);
        }
        return copy;
    }

    fn putAllEncounterOrder(
        self: *OrderedOrderMap,
        allocator: std.mem.Allocator,
        groups: []const SymbolOrders,
        config: Config,
    ) WireError!void {
        for (groups) |group| {
            const previous_count = if (self.values.get(group.symbol)) |orders| orders.len else 0;
            const is_new = !self.values.contains(group.symbol);
            if (is_new and self.values.count() == config.max_collection_entries) {
                return error.CountLimitExceeded;
            }
            const without_previous = self.total_orders - previous_count;
            const next_total = std.math.add(usize, without_previous, group.orders.len) catch
                return error.CountLimitExceeded;
            if (next_total > config.max_total_orders) return error.CountLimitExceeded;

            const owned = try allocator.dupe(Order, group.orders);
            errdefer allocator.free(owned);
            if (is_new) {
                try self.values.ensureUnusedCapacity(allocator, 1);
                try self.history.recordPut(allocator, group.symbol);
                self.values.putAssumeCapacityNoClobber(group.symbol, owned);
            } else {
                const existing = self.values.getPtr(group.symbol) orelse unreachable;
                allocator.free(existing.*);
                existing.* = owned;
            }
            self.total_orders = next_total;
        }
    }

    fn takeGroupsInHistoryOrder(
        self: *OrderedOrderMap,
        allocator: std.mem.Allocator,
    ) WireError![]const SymbolOrders {
        const keys = try orderedHistoryKeys(&self.history, allocator);
        defer allocator.free(keys);
        if (keys.len != self.values.count()) return error.HistorySemanticKeySetMismatch;

        const groups = try allocator.alloc(SymbolOrders, keys.len);
        errdefer allocator.free(groups);
        for (keys, 0..) |key, index| {
            const orders = self.values.get(key) orelse
                return error.HistorySemanticKeySetMismatch;
            groups[index] = .{ .symbol = key, .orders = orders };
        }
        // Ownership of every nested slice moves to the result.
        self.values.clearRetainingCapacity();
        self.total_orders = 0;
        return groups;
    }
};

/// Applies Java's SingleUser reducer, including its observable identity bug:
/// the merged uid is always zero because each reduction retains `a.uid` after
/// starting from `IDENTITY`. Any non-empty section set also makes `orders`
/// present, even when no section contributes an order.
///
/// Java's stream may choose a parallel reduction tree. This implementation
/// intentionally models the deterministic sequential fold selected by the
/// externally sorted section stream; a different Java parallel tree can have
/// a different EC backing layout even when the logical result is identical.
pub fn mergeSingleUserGroupedEvents(
    allocator: std.mem.Allocator,
    grouped: *const GroupedBinaryEvents,
    config: Config,
) WireError!SingleUserReport {
    try validateGroupedBinaryEvents(grouped, config);

    var result = SingleUserReport{
        .uid = 0,
        .user_status = null,
        .accounts = null,
        .positions = null,
        .orders = null,
        .query_execution_status = .ok,
    };
    errdefer result.deinit(allocator);

    var merged_orders: ?OrderedOrderMap = null;
    defer if (merged_orders) |*orders| orders.deinit(allocator);

    for (grouped.sections) |section| {
        var decoded = try decodeSingleUserEvents(allocator, section.carriers, config);
        defer decoded.deinit(allocator);

        if (result.user_status == null) result.user_status = decoded.report.user_status;
        if (result.accounts == null and decoded.report.accounts != null) {
            result.accounts = decoded.report.accounts;
            decoded.report.accounts = null;
        }
        if (result.positions == null and decoded.report.positions != null) {
            result.positions = decoded.report.positions;
            decoded.report.positions = null;
        }
        // Java performs mergeOverride on every reduction edge. `a == null`
        // starts from a fresh default IntObjectHashMap; otherwise the map is
        // copied with IntObjectHashMap's sequential-copy constructor before
        // `b` is traversed in its own EC encounter order.
        var next = if (merged_orders) |*orders|
            try orders.copyForReduction(allocator)
        else
            OrderedOrderMap{};
        errdefer next.deinit(allocator);
        if (decoded.report.orders) |groups| {
            try next.putAllEncounterOrder(allocator, groups, config);
        }
        if (merged_orders) |*orders| orders.deinit(allocator);
        merged_orders = next;
        if (result.query_execution_status == .ok and decoded.report.query_execution_status != .ok) {
            result.query_execution_status = decoded.report.query_execution_status;
        }
    }

    if (merged_orders) |*orders| {
        result.orders = try orders.takeGroupsInHistoryOrder(allocator);
    }

    return result;
}

/// Groups and merges a matcher-event chain into an owned Java SingleUser
/// result. The returned report must be released with `SingleUserReport.deinit`.
pub fn mergeSingleUserEventChain(
    allocator: std.mem.Allocator,
    head: ?*const core_events.MatcherTradeEvent,
    config: Config,
) WireError!SingleUserReport {
    var grouped = try groupMatcherEventChain(allocator, head, config);
    defer grouped.deinit(allocator);
    return mergeSingleUserGroupedEvents(allocator, &grouped, config);
}

const IntLongAccumulator = std.AutoHashMapUnmanaged(i32, i64);

const OrderedIntLongAccumulator = struct {
    values: IntLongAccumulator = .empty,
    history: int_map_order.History = int_map_order.History.init(),

    fn deinit(self: *OrderedIntLongAccumulator, allocator: std.mem.Allocator) void {
        self.values.deinit(allocator);
        self.history.deinit(allocator);
        self.* = undefined;
    }

    fn fromDecodedAndCopyForIntLong(
        allocator: std.mem.Allocator,
        entries: []const IntLongEntry,
    ) WireError!OrderedIntLongAccumulator {
        const keys = try allocator.alloc(i32, entries.len);
        defer allocator.free(keys);
        for (entries, 0..) |entry, index| keys[index] = entry.key;

        var decoded_history = try int_map_order.History.initFromEncounterOrder(
            allocator,
            entries.len,
            keys,
        );
        defer decoded_history.deinit(allocator);
        var result = OrderedIntLongAccumulator{
            .history = try decoded_history.copyForIntLong(allocator),
        };
        errdefer result.deinit(allocator);
        try result.values.ensureTotalCapacity(
            allocator,
            std.math.cast(u32, entries.len) orelse return error.CountLimitExceeded,
        );
        for (entries) |entry| {
            result.values.putAssumeCapacityNoClobber(entry.key, entry.value);
        }
        return result;
    }

    fn copyForIntLong(
        self: *const OrderedIntLongAccumulator,
        allocator: std.mem.Allocator,
    ) WireError!OrderedIntLongAccumulator {
        var result = OrderedIntLongAccumulator{
            .history = try self.history.copyForIntLong(allocator),
        };
        errdefer result.deinit(allocator);
        try result.values.ensureTotalCapacity(allocator, self.values.count());
        var iterator = self.values.iterator();
        while (iterator.next()) |entry| {
            result.values.putAssumeCapacityNoClobber(entry.key_ptr.*, entry.value_ptr.*);
        }
        return result;
    }

    fn addEntriesWrapping(
        self: *OrderedIntLongAccumulator,
        allocator: std.mem.Allocator,
        entries: []const IntLongEntry,
        config: Config,
    ) WireError!void {
        for (entries) |entry| {
            if (self.values.getPtr(entry.key)) |value| {
                value.* +%= entry.value;
                continue;
            }
            if (self.values.count() == config.max_collection_entries) {
                return error.CountLimitExceeded;
            }
            try self.values.ensureUnusedCapacity(allocator, 1);
            try self.history.recordPut(allocator, entry.key);
            // addToValue(key, 0) creates an active zero-valued key in EC.
            self.values.putAssumeCapacityNoClobber(entry.key, entry.value);
        }
    }

    fn mergeSumStep(
        allocator: std.mem.Allocator,
        current: ?*const OrderedIntLongAccumulator,
        maybe_entries: ?[]const IntLongEntry,
        config: Config,
    ) WireError!OrderedIntLongAccumulator {
        if (current) |present| {
            var result = try present.copyForIntLong(allocator);
            errdefer result.deinit(allocator);
            if (maybe_entries) |entries| {
                try result.addEntriesWrapping(allocator, entries, config);
            }
            return result;
        }
        if (maybe_entries) |entries| {
            return fromDecodedAndCopyForIntLong(allocator, entries);
        }
        // Both operands null: mergeSum returns a fresh default empty map.
        return .{};
    }

    fn ownEntriesInHistoryOrder(
        self: *const OrderedIntLongAccumulator,
        allocator: std.mem.Allocator,
    ) WireError![]const IntLongEntry {
        const keys = try orderedHistoryKeys(&self.history, allocator);
        defer allocator.free(keys);
        if (keys.len != self.values.count()) return error.HistorySemanticKeySetMismatch;
        const entries = try allocator.alloc(IntLongEntry, keys.len);
        errdefer allocator.free(entries);
        for (keys, 0..) |key, index| {
            entries[index] = .{
                .key = key,
                .value = self.values.get(key) orelse
                    return error.HistorySemanticKeySetMismatch,
            };
        }
        return entries;
    }
};

const TotalBalanceAccumulator = struct {
    account_balances: OrderedIntLongAccumulator = .{},
    fees: OrderedIntLongAccumulator = .{},
    adjustments: OrderedIntLongAccumulator = .{},
    suspends: OrderedIntLongAccumulator = .{},
    order_balances: OrderedIntLongAccumulator = .{},
    open_interest_long: OrderedIntLongAccumulator = .{},
    open_interest_short: OrderedIntLongAccumulator = .{},

    fn deinit(self: *TotalBalanceAccumulator, allocator: std.mem.Allocator) void {
        self.account_balances.deinit(allocator);
        self.fees.deinit(allocator);
        self.adjustments.deinit(allocator);
        self.suspends.deinit(allocator);
        self.order_balances.deinit(allocator);
        self.open_interest_long.deinit(allocator);
        self.open_interest_short.deinit(allocator);
        self.* = undefined;
    }

    fn mergeStep(
        self: *const TotalBalanceAccumulator,
        allocator: std.mem.Allocator,
        has_current: bool,
        report: TotalCurrencyBalanceReport,
        config: Config,
    ) WireError!TotalBalanceAccumulator {
        var next: TotalBalanceAccumulator = undefined;
        var initialized: usize = 0;
        errdefer {
            const fields = [_]*OrderedIntLongAccumulator{
                &next.account_balances,
                &next.fees,
                &next.adjustments,
                &next.suspends,
                &next.order_balances,
                &next.open_interest_long,
                &next.open_interest_short,
            };
            for (fields[0..initialized]) |field| field.deinit(allocator);
        }
        const current_account = if (has_current) &self.account_balances else null;
        next.account_balances = try OrderedIntLongAccumulator.mergeSumStep(
            allocator,
            current_account,
            report.account_balances,
            config,
        );
        initialized += 1;
        const current_fees = if (has_current) &self.fees else null;
        next.fees = try OrderedIntLongAccumulator.mergeSumStep(
            allocator,
            current_fees,
            report.fees,
            config,
        );
        initialized += 1;
        const current_adjustments = if (has_current) &self.adjustments else null;
        next.adjustments = try OrderedIntLongAccumulator.mergeSumStep(
            allocator,
            current_adjustments,
            report.adjustments,
            config,
        );
        initialized += 1;
        const current_suspends = if (has_current) &self.suspends else null;
        next.suspends = try OrderedIntLongAccumulator.mergeSumStep(
            allocator,
            current_suspends,
            report.suspends,
            config,
        );
        initialized += 1;
        const current_orders = if (has_current) &self.order_balances else null;
        next.order_balances = try OrderedIntLongAccumulator.mergeSumStep(
            allocator,
            current_orders,
            report.order_balances,
            config,
        );
        initialized += 1;
        const current_long = if (has_current) &self.open_interest_long else null;
        next.open_interest_long = try OrderedIntLongAccumulator.mergeSumStep(
            allocator,
            current_long,
            report.open_interest_long,
            config,
        );
        initialized += 1;
        const current_short = if (has_current) &self.open_interest_short else null;
        next.open_interest_short = try OrderedIntLongAccumulator.mergeSumStep(
            allocator,
            current_short,
            report.open_interest_short,
            config,
        );
        return next;
    }
};

/// Decodes and sums all TotalCurrencyBalance sections with Java signed-long
/// wrapping. The all-null identity is preserved only for an empty section set;
/// after any section, every one of the seven result maps is present. As with
/// SingleUser, exact map order is for this deterministic sequential fold; Java
/// parallel reduction can select a different (logically equivalent) tree.
pub fn mergeTotalCurrencyBalanceGroupedEvents(
    allocator: std.mem.Allocator,
    grouped: *const GroupedBinaryEvents,
    config: Config,
) WireError!TotalCurrencyBalanceReport {
    try validateGroupedBinaryEvents(grouped, config);

    var result = TotalCurrencyBalanceReport{
        .account_balances = null,
        .fees = null,
        .adjustments = null,
        .suspends = null,
        .order_balances = null,
        .open_interest_long = null,
        .open_interest_short = null,
    };
    errdefer result.deinit(allocator);
    if (grouped.sections.len == 0) return result;

    var sums: TotalBalanceAccumulator = .{};
    defer sums.deinit(allocator);
    var has_reduced = false;

    for (grouped.sections) |section| {
        var decoded = try decodeTotalCurrencyBalanceEvents(allocator, section.carriers, config);
        defer decoded.deinit(allocator);
        const next = try sums.mergeStep(
            allocator,
            has_reduced,
            decoded.report,
            config,
        );
        sums.deinit(allocator);
        sums = next;
        has_reduced = true;
    }

    result.account_balances = try sums.account_balances.ownEntriesInHistoryOrder(allocator);
    result.fees = try sums.fees.ownEntriesInHistoryOrder(allocator);
    result.adjustments = try sums.adjustments.ownEntriesInHistoryOrder(allocator);
    result.suspends = try sums.suspends.ownEntriesInHistoryOrder(allocator);
    result.order_balances = try sums.order_balances.ownEntriesInHistoryOrder(allocator);
    result.open_interest_long = try sums.open_interest_long.ownEntriesInHistoryOrder(allocator);
    result.open_interest_short = try sums.open_interest_short.ownEntriesInHistoryOrder(allocator);
    return result;
}

/// Groups and merges a matcher-event chain into an owned Java
/// TotalCurrencyBalance result. Release it with
/// `TotalCurrencyBalanceReport.deinit`.
pub fn mergeTotalCurrencyBalanceEventChain(
    allocator: std.mem.Allocator,
    head: ?*const core_events.MatcherTradeEvent,
    config: Config,
) WireError!TotalCurrencyBalanceReport {
    var grouped = try groupMatcherEventChain(allocator, head, config);
    defer grouped.deinit(allocator);
    return mergeTotalCurrencyBalanceGroupedEvents(allocator, &grouped, config);
}

fn parseStateHash(
    allocator: std.mem.Allocator,
    reader: *SliceReader,
    config: Config,
) WireError!StateHashReport {
    const count = try reader.readCount(config.max_collection_entries, 12, 0);
    const entries = try allocator.alloc(StateHashEntry, count);
    errdefer allocator.free(entries);
    for (entries) |*entry| {
        entry.* = .{
            .module_id = try reader.readInt(i32),
            .submodule = try StateHashSubmodule.fromCode(try reader.readInt(i32)),
            .hash = try reader.readInt(i32),
        };
    }
    std.sort.heap(StateHashEntry, entries, {}, lessStateHashEntry);
    try rejectDuplicateStateHashKeys(entries);
    return .{ .entries = entries };
}

fn parseTotalCurrencyBalance(
    allocator: std.mem.Allocator,
    reader: *SliceReader,
    config: Config,
) WireError!TotalCurrencyBalanceReport {
    var report = TotalCurrencyBalanceReport{
        .account_balances = null,
        .fees = null,
        .adjustments = null,
        .suspends = null,
        .order_balances = null,
        .open_interest_long = null,
        .open_interest_short = null,
    };
    errdefer report.deinit(allocator);

    report.account_balances = try readNullableIntLongMap(allocator, reader, config, 6);
    report.fees = try readNullableIntLongMap(allocator, reader, config, 5);
    report.adjustments = try readNullableIntLongMap(allocator, reader, config, 4);
    report.suspends = try readNullableIntLongMap(allocator, reader, config, 3);
    report.order_balances = try readNullableIntLongMap(allocator, reader, config, 2);
    report.open_interest_long = try readNullableIntLongMap(allocator, reader, config, 1);
    report.open_interest_short = try readNullableIntLongMap(allocator, reader, config, 0);
    return report;
}

fn parseSingleUser(
    allocator: std.mem.Allocator,
    reader: *SliceReader,
    config: Config,
) WireError!SingleUserReport {
    var report = SingleUserReport{
        .uid = try reader.readInt(i64),
        .user_status = null,
        .accounts = null,
        .positions = null,
        .orders = null,
        .query_execution_status = .ok,
    };
    errdefer report.deinit(allocator);

    if (try reader.readBoolean()) {
        report.user_status = try UserStatus.fromCode(try reader.readInt(i8));
    }
    report.accounts = try readNullableIntLongMap(allocator, reader, config, 6);
    report.positions = try readNullablePositions(allocator, reader, config, 5);
    report.orders = try readNullableOrders(allocator, reader, config, 4);
    report.query_execution_status = try QueryExecutionStatus.fromCode(try reader.readInt(i32));
    return report;
}

fn readNullableIntLongMap(
    allocator: std.mem.Allocator,
    reader: *SliceReader,
    config: Config,
    reserved_tail: usize,
) WireError!?[]const IntLongEntry {
    if (!try reader.readBoolean()) return null;
    const count = try reader.readCount(config.max_collection_entries, 12, reserved_tail);
    const entries = try allocator.alloc(IntLongEntry, count);
    errdefer allocator.free(entries);
    for (entries) |*entry| {
        entry.* = .{
            .key = try reader.readInt(i32),
            .value = try reader.readInt(i64),
        };
    }
    try validateUniqueIntLongKeysPreservingOrder(allocator, entries);
    return @as(
        ?[]const IntLongEntry,
        try reorderIntLongEntriesLikeJavaDecode(allocator, entries),
    );
}

fn readNullablePositions(
    allocator: std.mem.Allocator,
    reader: *SliceReader,
    config: Config,
    reserved_tail: usize,
) WireError!?[]const PositionEntry {
    if (!try reader.readBoolean()) return null;
    const count = try reader.readCount(config.max_collection_entries, 49, reserved_tail);
    const positions = try allocator.alloc(PositionEntry, count);
    errdefer allocator.free(positions);
    for (positions) |*position| {
        position.* = .{
            .symbol = try reader.readInt(i32),
            .quote_currency = try reader.readInt(i32),
            .direction = try PositionDirection.fromCode(try reader.readInt(i8)),
            .open_volume = try reader.readInt(i64),
            .open_price_sum = try reader.readInt(i64),
            .profit = try reader.readInt(i64),
            .pending_sell_size = try reader.readInt(i64),
            .pending_buy_size = try reader.readInt(i64),
        };
    }
    try validateUniquePositionKeysPreservingOrder(allocator, positions);
    return @as(
        ?[]const PositionEntry,
        try reorderPositionEntriesLikeJavaDecode(allocator, positions),
    );
}

fn readNullableOrders(
    allocator: std.mem.Allocator,
    reader: *SliceReader,
    config: Config,
    reserved_tail: usize,
) WireError!?[]const SymbolOrders {
    if (!try reader.readBoolean()) return null;
    const group_count = try reader.readCount(config.max_collection_entries, 8, reserved_tail);
    const groups = try allocator.alloc(SymbolOrders, group_count);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| allocator.free(group.orders);
        allocator.free(groups);
    }

    var total_orders: usize = 0;
    for (groups, 0..) |*group, group_index| {
        const symbol = try reader.readInt(i32);
        const remaining_group_count = group_count - group_index - 1;
        const future_headers = std.math.mul(usize, remaining_group_count, 8) catch return error.CountLimitExceeded;
        const future_and_tail = std.math.add(usize, future_headers, reserved_tail) catch return error.CountLimitExceeded;
        const remaining_order_capacity = config.max_total_orders - total_orders;
        const order_count = try reader.readCount(remaining_order_capacity, 57, future_and_tail);
        const orders = try allocator.alloc(Order, order_count);
        errdefer allocator.free(orders);
        for (orders) |*order| {
            order.* = .{
                .order_id = try reader.readInt(i64),
                .price = try reader.readInt(i64),
                .size = try reader.readInt(i64),
                .filled = try reader.readInt(i64),
                .reserve_bid_price = try reader.readInt(i64),
                .action = try OrderAction.fromCode(try reader.readInt(i8)),
                .uid = try reader.readInt(i64),
                .timestamp = try reader.readInt(i64),
            };
        }
        total_orders += order_count;
        group.* = .{ .symbol = symbol, .orders = orders };
        initialized += 1;
    }
    try validateUniqueOrderGroupKeysPreservingOrder(allocator, groups);
    return @as(
        ?[]const SymbolOrders,
        try reorderOrderGroupsLikeJavaDecode(allocator, groups),
    );
}

/// Chronicle writes an Eclipse map in its current traversal order, but Java's
/// reader does not retain that byte order directly: it constructs a fresh
/// `Int*HashMap(serializedCount)` and inserts the encountered keys. Reordering
/// here exposes the traversal order of that reconstructed map to reducers and
/// to a subsequent Java-compatible serialization.
fn reorderIntLongEntriesLikeJavaDecode(
    allocator: std.mem.Allocator,
    encounter_entries: []IntLongEntry,
) WireError![]const IntLongEntry {
    const encounter_keys = try allocator.alloc(i32, encounter_entries.len);
    defer allocator.free(encounter_keys);
    for (encounter_entries, 0..) |entry, index| encounter_keys[index] = entry.key;

    var history = try int_map_order.History.initFromEncounterOrder(
        allocator,
        encounter_entries.len,
        encounter_keys,
    );
    defer history.deinit(allocator);
    const ordered_keys = try orderedHistoryKeys(&history, allocator);
    defer allocator.free(ordered_keys);

    var indexes: std.AutoHashMapUnmanaged(i32, usize) = .empty;
    defer indexes.deinit(allocator);
    try indexes.ensureTotalCapacity(
        allocator,
        std.math.cast(u32, encounter_entries.len) orelse return error.CountLimitExceeded,
    );
    for (encounter_entries, 0..) |entry, index| {
        indexes.putAssumeCapacityNoClobber(entry.key, index);
    }

    const reordered = try allocator.alloc(IntLongEntry, encounter_entries.len);
    errdefer allocator.free(reordered);
    for (ordered_keys, 0..) |key, index| {
        reordered[index] = encounter_entries[indexes.get(key) orelse unreachable];
    }
    allocator.free(encounter_entries);
    return reordered;
}

fn reorderPositionEntriesLikeJavaDecode(
    allocator: std.mem.Allocator,
    encounter_entries: []PositionEntry,
) WireError![]const PositionEntry {
    const encounter_keys = try allocator.alloc(i32, encounter_entries.len);
    defer allocator.free(encounter_keys);
    for (encounter_entries, 0..) |entry, index| encounter_keys[index] = entry.symbol;

    var history = try int_map_order.History.initFromEncounterOrder(
        allocator,
        encounter_entries.len,
        encounter_keys,
    );
    defer history.deinit(allocator);
    const ordered_keys = try orderedHistoryKeys(&history, allocator);
    defer allocator.free(ordered_keys);

    var indexes: std.AutoHashMapUnmanaged(i32, usize) = .empty;
    defer indexes.deinit(allocator);
    try indexes.ensureTotalCapacity(
        allocator,
        std.math.cast(u32, encounter_entries.len) orelse return error.CountLimitExceeded,
    );
    for (encounter_entries, 0..) |entry, index| {
        indexes.putAssumeCapacityNoClobber(entry.symbol, index);
    }

    const reordered = try allocator.alloc(PositionEntry, encounter_entries.len);
    errdefer allocator.free(reordered);
    for (ordered_keys, 0..) |key, index| {
        reordered[index] = encounter_entries[indexes.get(key) orelse unreachable];
    }
    allocator.free(encounter_entries);
    return reordered;
}

fn reorderOrderGroupsLikeJavaDecode(
    allocator: std.mem.Allocator,
    encounter_groups: []SymbolOrders,
) WireError![]const SymbolOrders {
    const encounter_keys = try allocator.alloc(i32, encounter_groups.len);
    defer allocator.free(encounter_keys);
    for (encounter_groups, 0..) |group, index| encounter_keys[index] = group.symbol;

    var history = try int_map_order.History.initFromEncounterOrder(
        allocator,
        encounter_groups.len,
        encounter_keys,
    );
    defer history.deinit(allocator);
    const ordered_keys = try orderedHistoryKeys(&history, allocator);
    defer allocator.free(ordered_keys);

    var indexes: std.AutoHashMapUnmanaged(i32, usize) = .empty;
    defer indexes.deinit(allocator);
    try indexes.ensureTotalCapacity(
        allocator,
        std.math.cast(u32, encounter_groups.len) orelse return error.CountLimitExceeded,
    );
    for (encounter_groups, 0..) |group, index| {
        indexes.putAssumeCapacityNoClobber(group.symbol, index);
    }

    const reordered = try allocator.alloc(SymbolOrders, encounter_groups.len);
    errdefer allocator.free(reordered);
    for (ordered_keys, 0..) |key, index| {
        reordered[index] = encounter_groups[indexes.get(key) orelse unreachable];
    }
    // The nested order slices have moved shallowly into `reordered`.
    allocator.free(encounter_groups);
    return reordered;
}

fn writeNullableIntLongMap(
    allocator: std.mem.Allocator,
    writer: *std.Io.Writer,
    maybe_entries: ?[]const IntLongEntry,
    config: Config,
) WireError!void {
    try writeBoolean(writer, maybe_entries != null);
    const entries = maybe_entries orelse return;
    try validateUniqueIntLongKeysPreservingOrder(allocator, entries);
    try writeCount(writer, entries.len, config);
    for (entries) |entry| {
        try writeIntLe(writer, i32, entry.key);
        try writeIntLe(writer, i64, entry.value);
    }
}

fn writeNullablePositions(
    allocator: std.mem.Allocator,
    writer: *std.Io.Writer,
    maybe_positions: ?[]const PositionEntry,
    config: Config,
) WireError!void {
    try writeBoolean(writer, maybe_positions != null);
    const positions = maybe_positions orelse return;
    try validateUniquePositionKeysPreservingOrder(allocator, positions);
    try writeCount(writer, positions.len, config);
    for (positions) |position| {
        try writeIntLe(writer, i32, position.symbol);
        try writeIntLe(writer, i32, position.quote_currency);
        try writeIntLe(writer, i8, @intFromEnum(position.direction));
        try writeIntLe(writer, i64, position.open_volume);
        try writeIntLe(writer, i64, position.open_price_sum);
        try writeIntLe(writer, i64, position.profit);
        try writeIntLe(writer, i64, position.pending_sell_size);
        try writeIntLe(writer, i64, position.pending_buy_size);
    }
}

fn writeNullableOrders(
    allocator: std.mem.Allocator,
    writer: *std.Io.Writer,
    maybe_groups: ?[]const SymbolOrders,
    config: Config,
) WireError!void {
    try writeBoolean(writer, maybe_groups != null);
    const groups = maybe_groups orelse return;
    try validateUniqueOrderGroupKeysPreservingOrder(allocator, groups);
    try writeCount(writer, groups.len, config);

    var total_orders: usize = 0;
    for (groups) |group| {
        total_orders = std.math.add(usize, total_orders, group.orders.len) catch return error.CountLimitExceeded;
        if (total_orders > config.max_total_orders) return error.CountLimitExceeded;
        try writeIntLe(writer, i32, group.symbol);
        try writeCount(writer, group.orders.len, .{
            .max_body_bytes = config.max_body_bytes,
            .max_collection_entries = config.max_total_orders,
            .max_total_orders = config.max_total_orders,
        });
        for (group.orders) |order| {
            try writeIntLe(writer, i64, order.order_id);
            try writeIntLe(writer, i64, order.price);
            try writeIntLe(writer, i64, order.size);
            try writeIntLe(writer, i64, order.filled);
            try writeIntLe(writer, i64, order.reserve_bid_price);
            try writeIntLe(writer, i8, @intFromEnum(order.action));
            try writeIntLe(writer, i64, order.uid);
            try writeIntLe(writer, i64, order.timestamp);
        }
    }
}

fn writeCount(writer: *std.Io.Writer, count: usize, config: Config) WireError!void {
    if (count > config.max_collection_entries) return error.CountLimitExceeded;
    const wire_count = std.math.cast(i32, count) orelse return error.CountLimitExceeded;
    try writeIntLe(writer, i32, wire_count);
}

fn writeBoolean(writer: *std.Io.Writer, value: bool) WireError!void {
    // Chronicle Bytes 2.19.x writes textual boolean markers, not numeric
    // bytes: `Y` for true and `N` for false.
    writer.writeByte(if (value) 'Y' else 'N') catch return error.OutOfMemory;
}

fn writeIntLe(writer: *std.Io.Writer, comptime T: type, value: T) WireError!void {
    writer.writeInt(T, value, .little) catch return error.OutOfMemory;
}

fn finishEncodedBody(output: *std.Io.Writer.Allocating, config: Config) WireError![]u8 {
    if (output.written().len > config.max_body_bytes) return error.BodyTooLarge;
    return output.toOwnedSlice();
}

fn checkExactBodySize(body_len: usize, config: Config) WireError!void {
    if (body_len > config.max_body_bytes) return error.BodyTooLarge;
}

fn checkPaddedBodySize(body_len: usize, config: Config) WireError!void {
    const maximum_padded = std.math.add(usize, config.max_body_bytes, bytes_per_event - 1) catch return error.BodyTooLarge;
    if (body_len > maximum_padded) return error.BodyTooLarge;
}

fn checkConsumedBodySize(consumed: usize, config: Config) WireError!void {
    if (consumed > config.max_body_bytes) return error.BodyTooLarge;
}

const SliceReader = struct {
    bytes: []const u8,
    pos: usize,

    fn init(bytes: []const u8) SliceReader {
        return .{ .bytes = bytes, .pos = 0 };
    }

    fn remaining(self: SliceReader) usize {
        return self.bytes.len - self.pos;
    }

    fn readInt(self: *SliceReader, comptime T: type) WireError!T {
        const size = @sizeOf(T);
        if (self.remaining() < size) return error.TruncatedPayload;
        const value = std.mem.readInt(T, self.bytes[self.pos..][0..size], .little);
        self.pos += size;
        return value;
    }

    fn readBoolean(self: *SliceReader) WireError!bool {
        const raw = try self.readInt(u8);
        // Match StreamingDataInput.readBoolean: zero and N/n are false; all
        // other byte values are true. This also retains compatibility with
        // Zig's earlier noncanonical 0/1 encoder while emitting Java-exact
        // bytes for every new body.
        return raw != 0 and raw != 'N' and raw != 'n';
    }

    fn readCount(
        self: *SliceReader,
        maximum: usize,
        minimum_entry_bytes: usize,
        reserved_tail: usize,
    ) WireError!usize {
        const raw = try self.readInt(i32);
        if (raw < 0) return error.NegativeCount;
        const count: usize = @intCast(raw);
        if (count > maximum) return error.CountLimitExceeded;
        if (reserved_tail > self.remaining()) return error.TruncatedPayload;
        const available = self.remaining() - reserved_tail;
        if (count > available / minimum_entry_bytes) return error.TruncatedPayload;
        return count;
    }

    fn finishExact(self: SliceReader) WireError!void {
        if (self.pos != self.bytes.len) return error.TrailingBytes;
    }

    fn finishZeroPadding(self: SliceReader) WireError!void {
        for (self.bytes[self.pos..]) |byte| {
            if (byte != 0) return error.NonZeroCarrierPadding;
        }
    }
};

fn validateGroupedBinaryEvents(
    grouped: *const GroupedBinaryEvents,
    config: Config,
) WireError!void {
    if (grouped.sections.len > config.max_collection_entries) return error.CountLimitExceeded;
    var previous_external_id: ?i32 = null;
    for (grouped.sections) |section| {
        try validateShardId(section.label.shard_id);
        const external_id = section.label.externalId();
        if (previous_external_id) |previous| {
            if (external_id <= previous) return error.SectionMismatch;
        }
        previous_external_id = external_id;
        if (section.carriers.len == 0) return error.EmptyEventChain;
        const padded_size = std.math.mul(usize, section.carriers.len, bytes_per_event) catch
            return error.CarrierTooLarge;
        try checkPaddedBodySize(padded_size, config);
        for (section.carriers) |carrier| {
            if (carrier.section_id != external_id) return error.SectionMismatch;
        }
    }
}

fn freeOwnedOrderMapValues(allocator: std.mem.Allocator, map: *OwnedOrderMap) void {
    var iterator = map.valueIterator();
    while (iterator.next()) |orders| allocator.free(orders.*);
}

fn lessIndexedCarrier(_: void, lhs: IndexedCarrier, rhs: IndexedCarrier) bool {
    if (lhs.carrier.section_id != rhs.carrier.section_id) {
        return lhs.carrier.section_id < rhs.carrier.section_id;
    }
    return lhs.encounter_index < rhs.encounter_index;
}

fn lessStateHashEntry(_: void, lhs: StateHashEntry, rhs: StateHashEntry) bool {
    const lhs_submodule = @intFromEnum(lhs.submodule);
    const rhs_submodule = @intFromEnum(rhs.submodule);
    return lhs_submodule < rhs_submodule or
        (lhs_submodule == rhs_submodule and lhs.module_id < rhs.module_id);
}

fn lessIntLongEntry(_: void, lhs: IntLongEntry, rhs: IntLongEntry) bool {
    return lhs.key < rhs.key;
}

fn lessPositionEntry(_: void, lhs: PositionEntry, rhs: PositionEntry) bool {
    return lhs.symbol < rhs.symbol;
}

fn lessSymbolOrders(_: void, lhs: SymbolOrders, rhs: SymbolOrders) bool {
    return lhs.symbol < rhs.symbol;
}

fn rejectDuplicateStateHashKeys(entries: []const StateHashEntry) WireError!void {
    if (entries.len < 2) return;
    for (entries[1..], 1..) |entry, index| {
        const previous = entries[index - 1];
        if (entry.module_id == previous.module_id and entry.submodule == previous.submodule) {
            return error.DuplicateKey;
        }
    }
}

fn rejectDuplicateIntLongKeys(entries: []const IntLongEntry) WireError!void {
    if (entries.len < 2) return;
    for (entries[1..], 1..) |entry, index| {
        if (entry.key == entries[index - 1].key) return error.DuplicateKey;
    }
}

fn rejectDuplicatePositionKeys(entries: []const PositionEntry) WireError!void {
    if (entries.len < 2) return;
    for (entries[1..], 1..) |entry, index| {
        if (entry.symbol == entries[index - 1].symbol) return error.DuplicateKey;
    }
}

fn rejectDuplicateOrderGroupKeys(groups: []const SymbolOrders) WireError!void {
    if (groups.len < 2) return;
    for (groups[1..], 1..) |group, index| {
        if (group.symbol == groups[index - 1].symbol) return error.DuplicateKey;
    }
}

fn validateUniqueIntLongKeysPreservingOrder(
    allocator: std.mem.Allocator,
    entries: []const IntLongEntry,
) WireError!void {
    if (entries.len < 2) return;
    const sorted = try allocator.dupe(IntLongEntry, entries);
    defer allocator.free(sorted);
    std.sort.heap(IntLongEntry, sorted, {}, lessIntLongEntry);
    try rejectDuplicateIntLongKeys(sorted);
}

fn validateUniquePositionKeysPreservingOrder(
    allocator: std.mem.Allocator,
    entries: []const PositionEntry,
) WireError!void {
    if (entries.len < 2) return;
    const sorted = try allocator.dupe(PositionEntry, entries);
    defer allocator.free(sorted);
    std.sort.heap(PositionEntry, sorted, {}, lessPositionEntry);
    try rejectDuplicatePositionKeys(sorted);
}

fn validateUniqueOrderGroupKeysPreservingOrder(
    allocator: std.mem.Allocator,
    groups: []const SymbolOrders,
) WireError!void {
    if (groups.len < 2) return;
    const sorted = try allocator.dupe(SymbolOrders, groups);
    defer allocator.free(sorted);
    std.sort.heap(SymbolOrders, sorted, {}, lessSymbolOrders);
    try rejectDuplicateOrderGroupKeys(sorted);
}

test "RE and ME section labels use Java external ids" {
    const risk = try SectionLabel.risk(7);
    const matching = try SectionLabel.matching(7);
    try std.testing.expectEqual(@as(i32, 7), risk.externalId());
    try std.testing.expectEqual(@as(i32, 1031), matching.externalId());
    try std.testing.expectEqualDeep(risk, try SectionLabel.fromExternalId(7));
    try std.testing.expectEqualDeep(matching, try SectionLabel.fromExternalId(1031));
    try std.testing.expectError(error.InvalidShardId, SectionLabel.risk(-1));
    try std.testing.expectError(error.InvalidShardId, SectionLabel.matching(1024));
    try std.testing.expectError(error.InvalidSectionId, SectionLabel.fromExternalId(-1));
    try std.testing.expectError(error.InvalidSectionId, SectionLabel.fromExternalId(2048));
}

test "Chronicle boolean wire emits Y N and retains Java reader compatibility" {
    const allocator = std.testing.allocator;
    var output: std.Io.Writer.Allocating = .init(allocator);
    defer output.deinit();
    try writeBoolean(&output.writer, true);
    try writeBoolean(&output.writer, false);
    try std.testing.expectEqualSlices(u8, "YN", output.written());

    var reader = SliceReader.init(&.{ 0, 'N', 'n', 'Y', 'y', 1, 2 });
    try std.testing.expect(!try reader.readBoolean());
    try std.testing.expect(!try reader.readBoolean());
    try std.testing.expect(!try reader.readBoolean());
    try std.testing.expect(try reader.readBoolean());
    try std.testing.expect(try reader.readBoolean());
    try std.testing.expect(try reader.readBoolean());
    try std.testing.expect(try reader.readBoolean());
    try reader.finishExact();
}

test "event carriers preserve big-endian signed words at one and five-word boundaries" {
    const allocator = std.testing.allocator;
    const risk = try SectionLabel.risk(2);

    const one_byte = [_]u8{0x80};
    const one_event = try packSectionEvents(allocator, risk, &one_byte);
    defer allocator.free(one_event);
    try std.testing.expectEqual(@as(usize, 1), one_event.len);
    try std.testing.expectEqual(@as(i32, 2), one_event[0].section_id);
    try std.testing.expectEqual(@as(i64, @bitCast(@as(u64, 0x8000_0000_0000_0000))), one_event[0].words[0]);
    try std.testing.expectEqualSlices(i64, &([_]i64{0} ** 4), one_event[0].words[1..]);

    var unpacked = try unpackSectionEvents(allocator, one_event);
    defer unpacked.deinit(allocator);
    try std.testing.expectEqualDeep(risk, unpacked.label);
    try std.testing.expectEqual(@as(usize, bytes_per_event), unpacked.padded_bytes.len);
    try std.testing.expectEqual(@as(u8, 0x80), unpacked.padded_bytes[0]);
    for (unpacked.padded_bytes[1..]) |byte| try std.testing.expectEqual(@as(u8, 0), byte);

    var five_words: [bytes_per_event]u8 = undefined;
    for (&five_words, 0..) |*byte, index| byte.* = @intCast(index);
    const exactly_one_event = try packSectionEvents(allocator, risk, &five_words);
    defer allocator.free(exactly_one_event);
    try std.testing.expectEqual(@as(usize, 1), exactly_one_event.len);

    var across_boundary: [bytes_per_event + 1]u8 = undefined;
    for (&across_boundary, 0..) |*byte, index| byte.* = @intCast(index);
    const two_events = try packSectionEvents(allocator, risk, &across_boundary);
    defer allocator.free(two_events);
    try std.testing.expectEqual(@as(usize, 2), two_events.len);

    var mismatched = try allocator.dupe(BinaryEventCarrier, two_events);
    defer allocator.free(mismatched);
    mismatched[1].section_id = (try SectionLabel.matching(2)).externalId();
    try std.testing.expectError(error.SectionMismatch, unpackSectionEvents(allocator, mismatched));
    try std.testing.expectError(error.EmptyEventChain, unpackSectionEvents(allocator, &.{}));
}

const state_hash_golden = [_]u8{
    0x02, 0x00, 0x00, 0x00,
    0xfe, 0xff, 0xff, 0xff,
    0x00, 0x00, 0x00, 0x00,
    0xff, 0xff, 0xff, 0xff,
    0x03, 0x00, 0x00, 0x00,
    0x41, 0x00, 0x00, 0x00,
    0x04, 0x03, 0x02, 0x01,
};

const state_hash_unsorted = [_]u8{
    0x02, 0x00, 0x00, 0x00,
    0x03, 0x00, 0x00, 0x00,
    0x41, 0x00, 0x00, 0x00,
    0x04, 0x03, 0x02, 0x01,
    0xfe, 0xff, 0xff, 0xff,
    0x00, 0x00, 0x00, 0x00,
    0xff, 0xff, 0xff, 0xff,
};

test "state hash body matches exact Chronicle little-endian golden and canonical ordering" {
    const allocator = std.testing.allocator;
    const entries = [_]StateHashEntry{
        .{ .module_id = 3, .submodule = .matching_order_books, .hash = 0x0102_0304 },
        .{ .module_id = -2, .submodule = .risk_symbol_spec_provider, .hash = -1 },
    };
    const encoded = try encodeStateHashBody(allocator, .{ .entries = &entries }, .{});
    defer allocator.free(encoded);
    try std.testing.expectEqualSlices(u8, &state_hash_golden, encoded);

    var decoded = try decodeStateHashBody(allocator, &state_hash_unsorted, .{});
    defer decoded.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 2), decoded.entries.len);
    try std.testing.expectEqual(StateHashSubmodule.risk_symbol_spec_provider, decoded.entries[0].submodule);
    try std.testing.expectEqual(@as(i32, -2), decoded.entries[0].module_id);
    try std.testing.expectEqual(StateHashSubmodule.matching_order_books, decoded.entries[1].submodule);

    const canonical = try encodeStateHashBody(allocator, decoded, .{});
    defer allocator.free(canonical);
    try std.testing.expectEqualSlices(u8, &state_hash_golden, canonical);
}

test "state hash rejects every truncated prefix, unsafe counts, unknown codes, duplicates, and trailing bytes" {
    const allocator = std.testing.allocator;
    for (0..state_hash_golden.len) |cut| {
        try std.testing.expectError(
            error.TruncatedPayload,
            decodeStateHashBody(allocator, state_hash_golden[0..cut], .{}),
        );
    }

    try std.testing.expectError(
        error.NegativeCount,
        decodeStateHashBody(allocator, &.{ 0xff, 0xff, 0xff, 0xff }, .{}),
    );
    try std.testing.expectError(
        error.CountLimitExceeded,
        decodeStateHashBody(allocator, &.{ 0xff, 0xff, 0xff, 0x7f }, .{}),
    );

    var unknown = state_hash_golden;
    unknown[8] = 8;
    try std.testing.expectError(
        error.UnknownStateHashSubmodule,
        decodeStateHashBody(allocator, &unknown, .{}),
    );

    var duplicate_writer: std.Io.Writer.Allocating = .init(allocator);
    defer duplicate_writer.deinit();
    try writeIntLe(&duplicate_writer.writer, i32, 2);
    for (0..2) |_| {
        try writeIntLe(&duplicate_writer.writer, i32, 9);
        try writeIntLe(&duplicate_writer.writer, i32, 4);
        try writeIntLe(&duplicate_writer.writer, i32, 1);
    }
    try std.testing.expectError(
        error.DuplicateKey,
        decodeStateHashBody(allocator, duplicate_writer.written(), .{}),
    );
    const duplicate_entries = [_]StateHashEntry{
        .{ .module_id = 9, .submodule = .risk_fees, .hash = 1 },
        .{ .module_id = 9, .submodule = .risk_fees, .hash = 2 },
    };
    try std.testing.expectError(
        error.DuplicateKey,
        encodeStateHashBody(allocator, .{ .entries = &duplicate_entries }, .{}),
    );

    var trailing = state_hash_golden ++ [_]u8{0};
    try std.testing.expectError(
        error.TrailingBytes,
        decodeStateHashBody(allocator, &trailing, .{}),
    );
}

test "state hash event decode accepts zero padding, preserves RE label, and rejects nonzero padding" {
    const allocator = std.testing.allocator;
    const label = try SectionLabel.risk(5);
    const events = try packSectionEvents(allocator, label, &state_hash_golden);
    defer allocator.free(events);
    try std.testing.expectEqual(@as(usize, 1), events.len);

    var decoded = try decodeStateHashEvents(allocator, events, .{});
    defer decoded.deinit(allocator);
    try std.testing.expectEqualDeep(label, decoded.label);
    try std.testing.expectEqual(@as(usize, 2), decoded.report.entries.len);

    var corrupt = try allocator.dupe(BinaryEventCarrier, events);
    defer allocator.free(corrupt);
    corrupt[0].words[4] = 1;
    try std.testing.expectError(
        error.NonZeroCarrierPadding,
        decodeStateHashEvents(allocator, corrupt, .{}),
    );
}

test "total balance preserves all seven nullable maps and Java encounter order" {
    const allocator = std.testing.allocator;
    const all_null = TotalCurrencyBalanceReport{
        .account_balances = null,
        .fees = null,
        .adjustments = null,
        .suspends = null,
        .order_balances = null,
        .open_interest_long = null,
        .open_interest_short = null,
    };
    const null_golden = [_]u8{'N'} ** 7;
    const null_bytes = try encodeTotalCurrencyBalanceBody(allocator, all_null, .{});
    defer allocator.free(null_bytes);
    try std.testing.expectEqualSlices(u8, &null_golden, null_bytes);
    var decoded_null = try decodeTotalCurrencyBalanceBody(allocator, null_bytes, .{});
    defer decoded_null.deinit(allocator);
    try std.testing.expect(decoded_null.account_balances == null);
    try std.testing.expect(decoded_null.open_interest_short == null);

    const empty = [_]IntLongEntry{};
    const present_empty = TotalCurrencyBalanceReport{
        .account_balances = &empty,
        .fees = null,
        .adjustments = null,
        .suspends = null,
        .order_balances = null,
        .open_interest_long = null,
        .open_interest_short = null,
    };
    const empty_golden = [_]u8{ 'Y', 0, 0, 0, 0, 'N', 'N', 'N', 'N', 'N', 'N' };
    const empty_bytes = try encodeTotalCurrencyBalanceBody(allocator, present_empty, .{});
    defer allocator.free(empty_bytes);
    try std.testing.expectEqualSlices(u8, &empty_golden, empty_bytes);
    var decoded_empty = try decodeTotalCurrencyBalanceBody(allocator, empty_bytes, .{});
    defer decoded_empty.deinit(allocator);
    try std.testing.expect(decoded_empty.account_balances != null);
    try std.testing.expectEqual(@as(usize, 0), decoded_empty.account_balances.?.len);

    const accounts = [_]IntLongEntry{
        .{ .key = 9, .value = -90 },
        .{ .key = -2, .value = std.math.minInt(i64) },
    };
    const fees = [_]IntLongEntry{.{ .key = -1, .value = 11 }};
    const adjustments = [_]IntLongEntry{.{ .key = 2, .value = -12 }};
    const suspends = [_]IntLongEntry{.{ .key = 3, .value = 13 }};
    const order_balances = [_]IntLongEntry{.{ .key = 4, .value = -14 }};
    const open_long = [_]IntLongEntry{.{ .key = 5, .value = 15 }};
    const open_short = [_]IntLongEntry{.{ .key = 6, .value = -16 }};
    const full = TotalCurrencyBalanceReport{
        .account_balances = &accounts,
        .fees = &fees,
        .adjustments = &adjustments,
        .suspends = &suspends,
        .order_balances = &order_balances,
        .open_interest_long = &open_long,
        .open_interest_short = &open_short,
    };
    const full_bytes = try encodeTotalCurrencyBalanceBody(allocator, full, .{});
    defer allocator.free(full_bytes);
    try std.testing.expectEqual(@as(i32, 9), std.mem.readInt(i32, full_bytes[5..9], .little));
    var decoded_full = try decodeTotalCurrencyBalanceBody(allocator, full_bytes, .{});
    defer decoded_full.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 2), decoded_full.account_balances.?.len);
    try std.testing.expectEqual(@as(i32, 9), decoded_full.account_balances.?[0].key);
    try std.testing.expectEqual(@as(i64, -90), decoded_full.account_balances.?[0].value);
    try std.testing.expectEqual(@as(i32, -2), decoded_full.account_balances.?[1].key);
    try std.testing.expectEqual(std.math.minInt(i64), decoded_full.account_balances.?[1].value);
    try std.testing.expectEqual(@as(i64, -16), decoded_full.open_interest_short.?[0].value);

    const reencoded = try encodeTotalCurrencyBalanceBody(allocator, decoded_full, .{});
    defer allocator.free(reencoded);
    try std.testing.expectEqualSlices(u8, full_bytes, reencoded);
}

test "total balance rejects every truncated prefix, huge counts, duplicates, and trailing bytes" {
    const allocator = std.testing.allocator;
    const entries = [_]IntLongEntry{
        .{ .key = 2, .value = -2 },
        .{ .key = 1, .value = 1 },
    };
    const report = TotalCurrencyBalanceReport{
        .account_balances = &entries,
        .fees = &entries,
        .adjustments = &entries,
        .suspends = &entries,
        .order_balances = &entries,
        .open_interest_long = &entries,
        .open_interest_short = &entries,
    };
    const body = try encodeTotalCurrencyBalanceBody(allocator, report, .{});
    defer allocator.free(body);
    for (0..body.len) |cut| {
        try std.testing.expectError(
            error.TruncatedPayload,
            decodeTotalCurrencyBalanceBody(allocator, body[0..cut], .{}),
        );
    }

    try std.testing.expectError(
        error.CountLimitExceeded,
        decodeTotalCurrencyBalanceBody(allocator, &.{ 'Y', 0xff, 0xff, 0xff, 0x7f }, .{}),
    );

    var duplicate_writer: std.Io.Writer.Allocating = .init(allocator);
    defer duplicate_writer.deinit();
    try writeBoolean(&duplicate_writer.writer, true);
    try writeIntLe(&duplicate_writer.writer, i32, 2);
    for (0..2) |_| {
        try writeIntLe(&duplicate_writer.writer, i32, 4);
        try writeIntLe(&duplicate_writer.writer, i64, -1);
    }
    for (0..6) |_| try writeBoolean(&duplicate_writer.writer, false);
    try std.testing.expectError(
        error.DuplicateKey,
        decodeTotalCurrencyBalanceBody(allocator, duplicate_writer.written(), .{}),
    );

    const duplicate_entries = [_]IntLongEntry{
        .{ .key = 1, .value = 1 },
        .{ .key = 1, .value = 2 },
    };
    const duplicate_report = TotalCurrencyBalanceReport{
        .account_balances = &duplicate_entries,
        .fees = null,
        .adjustments = null,
        .suspends = null,
        .order_balances = null,
        .open_interest_long = null,
        .open_interest_short = null,
    };
    try std.testing.expectError(
        error.DuplicateKey,
        encodeTotalCurrencyBalanceBody(allocator, duplicate_report, .{}),
    );

    const null_golden = [_]u8{'N'} ** 7;
    var trailing = null_golden ++ [_]u8{0};
    try std.testing.expectError(
        error.TrailingBytes,
        decodeTotalCurrencyBalanceBody(allocator, &trailing, .{}),
    );
}

test "single user minimal body matches exact Java golden and absent fields remain absent" {
    const allocator = std.testing.allocator;
    const report = SingleUserReport{
        .uid = 0x0102_0304_0506_0708,
        .user_status = null,
        .accounts = null,
        .positions = null,
        .orders = null,
        .query_execution_status = .user_not_found,
    };
    const golden = [_]u8{
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
        'N',  'N',  'N',  'N',  0x01, 0x00, 0x00, 0x00,
    };
    const body = try encodeSingleUserBody(allocator, report, .{});
    defer allocator.free(body);
    try std.testing.expectEqualSlices(u8, &golden, body);
    var decoded = try decodeSingleUserBody(allocator, body, .{});
    defer decoded.deinit(allocator);
    try std.testing.expectEqual(report.uid, decoded.uid);
    try std.testing.expect(decoded.user_status == null);
    try std.testing.expect(decoded.accounts == null);
    try std.testing.expect(decoded.positions == null);
    try std.testing.expect(decoded.orders == null);
    try std.testing.expectEqual(QueryExecutionStatus.user_not_found, decoded.query_execution_status);
}

test "single user decode reconstructs sized Eclipse maps before exposing order" {
    const allocator = std.testing.allocator;
    const accounts = [_]IntLongEntry{
        .{ .key = 8, .value = -80 },
        .{ .key = -3, .value = 30 },
    };
    const positions = [_]PositionEntry{
        .{
            .symbol = 22,
            .quote_currency = -11,
            .direction = .short,
            .open_volume = -1,
            .open_price_sum = 2,
            .profit = -3,
            .pending_sell_size = 4,
            .pending_buy_size = -5,
        },
        .{
            .symbol = -7,
            .quote_currency = 15,
            .direction = .empty,
            .open_volume = 0,
            .open_price_sum = -20,
            .profit = 30,
            .pending_sell_size = -40,
            .pending_buy_size = 50,
        },
    };
    const orders_22 = [_]Order{
        .{
            .order_id = -1,
            .price = 2,
            .size = -3,
            .filled = 4,
            .reserve_bid_price = -5,
            .action = .ask,
            .uid = -6,
            .timestamp = 7,
        },
    };
    const orders_negative = [_]Order{
        .{
            .order_id = 8,
            .price = -9,
            .size = 10,
            .filled = -11,
            .reserve_bid_price = 12,
            .action = .bid,
            .uid = 13,
            .timestamp = -14,
        },
        .{
            .order_id = 15,
            .price = 16,
            .size = 17,
            .filled = 18,
            .reserve_bid_price = 19,
            .action = .ask,
            .uid = 20,
            .timestamp = 21,
        },
    };
    const groups = [_]SymbolOrders{
        .{ .symbol = 22, .orders = &orders_22 },
        .{ .symbol = -7, .orders = &orders_negative },
    };
    const report = SingleUserReport{
        .uid = -900,
        .user_status = .suspended,
        .accounts = &accounts,
        .positions = &positions,
        .orders = &groups,
        .query_execution_status = .ok,
    };
    const body = try encodeSingleUserBody(allocator, report, .{});
    defer allocator.free(body);
    var decoded = try decodeSingleUserBody(allocator, body, .{});
    defer decoded.deinit(allocator);
    try std.testing.expectEqual(@as(i64, -900), decoded.uid);
    try std.testing.expectEqual(UserStatus.suspended, decoded.user_status.?);
    try std.testing.expectEqual(@as(i32, 8), decoded.accounts.?[0].key);
    try std.testing.expectEqual(@as(i32, -7), decoded.positions.?[0].symbol);
    try std.testing.expectEqual(PositionDirection.empty, decoded.positions.?[0].direction);
    try std.testing.expectEqual(@as(i32, -7), decoded.orders.?[0].symbol);
    try std.testing.expectEqual(@as(usize, 2), decoded.orders.?[0].orders.len);
    try std.testing.expectEqual(OrderAction.bid, decoded.orders.?[0].orders[0].action);
    try std.testing.expectEqual(@as(i64, -14), decoded.orders.?[0].orders[0].timestamp);
    try std.testing.expectEqual(@as(i32, 22), decoded.orders.?[1].symbol);
    try std.testing.expectEqual(@as(usize, 1), decoded.orders.?[1].orders.len);

    const reencoded = try encodeSingleUserBody(allocator, decoded, .{});
    defer allocator.free(reencoded);
    try std.testing.expect(!std.mem.eql(u8, body, reencoded));
    var decoded_again = try decodeSingleUserBody(allocator, reencoded, .{});
    defer decoded_again.deinit(allocator);
    try std.testing.expectEqual(@as(i32, -7), decoded_again.positions.?[0].symbol);
    try std.testing.expectEqual(@as(i32, -7), decoded_again.orders.?[0].symbol);

    const empty_entries = [_]IntLongEntry{};
    const empty_positions = [_]PositionEntry{};
    const empty_groups = [_]SymbolOrders{};
    const present_empty = SingleUserReport{
        .uid = 1,
        .user_status = .active,
        .accounts = &empty_entries,
        .positions = &empty_positions,
        .orders = &empty_groups,
        .query_execution_status = .ok,
    };
    const empty_body = try encodeSingleUserBody(allocator, present_empty, .{});
    defer allocator.free(empty_body);
    var decoded_empty = try decodeSingleUserBody(allocator, empty_body, .{});
    defer decoded_empty.deinit(allocator);
    try std.testing.expect(decoded_empty.accounts != null);
    try std.testing.expect(decoded_empty.positions != null);
    try std.testing.expect(decoded_empty.orders != null);
    try std.testing.expectEqual(@as(usize, 0), decoded_empty.orders.?.len);
}

test "sized Eclipse decode and sequential reducers retain collision sentinel and zero-key order" {
    const allocator = std.testing.allocator;

    const collision_entries = [_]IntLongEntry{
        .{ .key = 2, .value = 20 },
        .{ .key = 33, .value = 330 },
    };
    const collision_report = TotalCurrencyBalanceReport{
        .account_balances = &collision_entries,
        .fees = null,
        .adjustments = null,
        .suspends = null,
        .order_balances = null,
        .open_interest_long = null,
        .open_interest_short = null,
    };
    const collision_body = try encodeTotalCurrencyBalanceBody(allocator, collision_report, .{});
    defer allocator.free(collision_body);
    var collision_decoded = try decodeTotalCurrencyBalanceBody(allocator, collision_body, .{});
    defer collision_decoded.deinit(allocator);
    try std.testing.expectEqualSlices(
        i32,
        &.{ 33, 2 },
        &.{
            collision_decoded.account_balances.?[0].key,
            collision_decoded.account_balances.?[1].key,
        },
    );

    const sentinel_entries = [_]IntLongEntry{
        .{ .key = 2, .value = 0 },
        .{ .key = 17, .value = 170 },
        .{ .key = 33, .value = 330 },
        .{ .key = 1, .value = 10 },
        .{ .key = 0, .value = 0 },
    };
    const sentinel_report = TotalCurrencyBalanceReport{
        .account_balances = null,
        .fees = null,
        .adjustments = null,
        .suspends = null,
        .order_balances = &sentinel_entries,
        .open_interest_long = null,
        .open_interest_short = null,
    };

    var carriers: std.ArrayListUnmanaged(BinaryEventCarrier) = .empty;
    defer carriers.deinit(allocator);
    try appendTotalBalanceTestSection(
        allocator,
        &carriers,
        try SectionLabel.risk(0),
        collision_report,
    );
    try appendTotalBalanceTestSection(
        allocator,
        &carriers,
        try SectionLabel.matching(0),
        sentinel_report,
    );
    var grouped = try groupBinaryEventCarriers(allocator, carriers.items, .{});
    defer grouped.deinit(allocator);
    var merged = try mergeTotalCurrencyBalanceGroupedEvents(allocator, &grouped, .{});
    defer merged.deinit(allocator);

    const expected = [_]i32{ 0, 1, 17, 2, 33 };
    try std.testing.expectEqual(@as(usize, 2), merged.account_balances.?.len);
    try std.testing.expectEqual(@as(i32, 33), merged.account_balances.?[0].key);
    try std.testing.expectEqual(@as(i32, 2), merged.account_balances.?[1].key);
    try std.testing.expectEqual(expected.len, merged.order_balances.?.len);
    for (expected, 0..) |key, index| {
        try std.testing.expectEqual(key, merged.order_balances.?[index].key);
    }
    // addToValue(key, 0) still materializes the sentinel key.
    try std.testing.expectEqual(@as(i64, 0), merged.order_balances.?[0].value);
}

test "SingleUser sequential override uses IntObject copy and putAll layout" {
    const allocator = std.testing.allocator;
    const early_groups = [_]SymbolOrders{
        .{ .symbol = 2, .orders = &.{testOrder(2)} },
        .{ .symbol = 33, .orders = &.{testOrder(33)} },
    };
    const late_groups = [_]SymbolOrders{
        .{ .symbol = 17, .orders = &.{testOrder(17)} },
        .{ .symbol = 2, .orders = &.{testOrder(200)} },
    };
    const early = SingleUserReport{
        .uid = 1,
        .user_status = null,
        .accounts = null,
        .positions = null,
        .orders = &early_groups,
        .query_execution_status = .ok,
    };
    const late = SingleUserReport{
        .uid = 1,
        .user_status = null,
        .accounts = null,
        .positions = null,
        .orders = &late_groups,
        .query_execution_status = .ok,
    };
    var carriers: std.ArrayListUnmanaged(BinaryEventCarrier) = .empty;
    defer carriers.deinit(allocator);
    try appendSingleUserTestSection(allocator, &carriers, try SectionLabel.risk(0), early);
    try appendSingleUserTestSection(allocator, &carriers, try SectionLabel.matching(0), late);
    var grouped = try groupBinaryEventCarriers(allocator, carriers.items, .{});
    defer grouped.deinit(allocator);
    var merged = try mergeSingleUserGroupedEvents(allocator, &grouped, .{});
    defer merged.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), merged.orders.?.len);
    try std.testing.expectEqualSlices(
        i32,
        &.{ 33, 2, 17 },
        &.{
            merged.orders.?[0].symbol,
            merged.orders.?[1].symbol,
            merged.orders.?[2].symbol,
        },
    );
    try std.testing.expectEqual(@as(i64, 200), merged.orders.?[1].orders[0].order_id);
}

test "single user rejects every truncated prefix, huge nested counts, duplicate keys, and unknown enum codes" {
    const allocator = std.testing.allocator;
    const accounts = [_]IntLongEntry{.{ .key = 1, .value = -1 }};
    const positions = [_]PositionEntry{.{
        .symbol = 2,
        .quote_currency = 3,
        .direction = .long,
        .open_volume = 4,
        .open_price_sum = 5,
        .profit = -6,
        .pending_sell_size = 7,
        .pending_buy_size = 8,
    }};
    const orders = [_]Order{.{
        .order_id = 9,
        .price = 10,
        .size = 11,
        .filled = 12,
        .reserve_bid_price = 13,
        .action = .bid,
        .uid = 14,
        .timestamp = -15,
    }};
    const groups = [_]SymbolOrders{.{ .symbol = 2, .orders = &orders }};
    const report = SingleUserReport{
        .uid = -1,
        .user_status = .active,
        .accounts = &accounts,
        .positions = &positions,
        .orders = &groups,
        .query_execution_status = .ok,
    };
    const body = try encodeSingleUserBody(allocator, report, .{});
    defer allocator.free(body);
    for (0..body.len) |cut| {
        try std.testing.expectError(
            error.TruncatedPayload,
            decodeSingleUserBody(allocator, body[0..cut], .{}),
        );
    }

    try std.testing.expectError(error.UnknownUserStatus, UserStatus.fromCode(2));
    try std.testing.expectError(error.UnknownPositionDirection, PositionDirection.fromCode(2));
    try std.testing.expectError(error.UnknownOrderAction, OrderAction.fromCode(2));
    try std.testing.expectError(error.UnknownQueryExecutionStatus, QueryExecutionStatus.fromCode(2));

    var bad_query = try allocator.dupe(u8, body);
    defer allocator.free(bad_query);
    std.mem.writeInt(i32, bad_query[bad_query.len - 4 ..][0..4], 2, .little);
    try std.testing.expectError(
        error.UnknownQueryExecutionStatus,
        decodeSingleUserBody(allocator, bad_query, .{}),
    );

    const duplicate_positions = [_]PositionEntry{ positions[0], positions[0] };
    var duplicate_report = report;
    duplicate_report.positions = &duplicate_positions;
    try std.testing.expectError(
        error.DuplicateKey,
        encodeSingleUserBody(allocator, duplicate_report, .{}),
    );

    const duplicate_groups = [_]SymbolOrders{ groups[0], groups[0] };
    duplicate_report.positions = &positions;
    duplicate_report.orders = &duplicate_groups;
    try std.testing.expectError(
        error.DuplicateKey,
        encodeSingleUserBody(allocator, duplicate_report, .{}),
    );

    var huge_orders: std.Io.Writer.Allocating = .init(allocator);
    defer huge_orders.deinit();
    try writeIntLe(&huge_orders.writer, i64, 1);
    try writeBoolean(&huge_orders.writer, false);
    try writeBoolean(&huge_orders.writer, false);
    try writeBoolean(&huge_orders.writer, false);
    try writeBoolean(&huge_orders.writer, true);
    try writeIntLe(&huge_orders.writer, i32, 1);
    try writeIntLe(&huge_orders.writer, i32, 7);
    try writeIntLe(&huge_orders.writer, i32, std.math.maxInt(i32));
    try writeIntLe(&huge_orders.writer, i32, 0);
    try std.testing.expectError(
        error.CountLimitExceeded,
        decodeSingleUserBody(allocator, huge_orders.written(), .{}),
    );
}

test "single user event decode preserves ME label and rejects nonzero carrier tail" {
    const allocator = std.testing.allocator;
    const report = SingleUserReport{
        .uid = -5,
        .user_status = null,
        .accounts = null,
        .positions = null,
        .orders = null,
        .query_execution_status = .user_not_found,
    };
    const body = try encodeSingleUserBody(allocator, report, .{});
    defer allocator.free(body);
    const label = try SectionLabel.matching(3);
    const events = try packSectionEvents(allocator, label, body);
    defer allocator.free(events);
    var decoded = try decodeSingleUserEvents(allocator, events, .{});
    defer decoded.deinit(allocator);
    try std.testing.expectEqualDeep(label, decoded.label);
    try std.testing.expectEqual(@as(i64, -5), decoded.report.uid);

    var corrupt = try allocator.dupe(BinaryEventCarrier, events);
    defer allocator.free(corrupt);
    corrupt[0].words[4] = 1;
    try std.testing.expectError(
        error.NonZeroCarrierPadding,
        decodeSingleUserEvents(allocator, corrupt, .{}),
    );
}

fn singleUserAllocationFixture() SingleUserReport {
    const Static = struct {
        const accounts = [_]IntLongEntry{
            .{ .key = 2, .value = -2 },
            .{ .key = 1, .value = 1 },
        };
        const positions = [_]PositionEntry{.{
            .symbol = 3,
            .quote_currency = 4,
            .direction = .short,
            .open_volume = 5,
            .open_price_sum = -6,
            .profit = 7,
            .pending_sell_size = 8,
            .pending_buy_size = 9,
        }};
        const orders = [_]Order{.{
            .order_id = 10,
            .price = 11,
            .size = 12,
            .filled = 13,
            .reserve_bid_price = 14,
            .action = .ask,
            .uid = 15,
            .timestamp = -16,
        }};
        const groups = [_]SymbolOrders{.{ .symbol = 3, .orders = &orders }};
    };
    return .{
        .uid = 17,
        .user_status = .active,
        .accounts = &Static.accounts,
        .positions = &Static.positions,
        .orders = &Static.groups,
        .query_execution_status = .ok,
    };
}

fn encodeSingleUserAllocationCampaign(allocator: std.mem.Allocator) !void {
    const body = try encodeSingleUserBody(allocator, singleUserAllocationFixture(), .{});
    defer allocator.free(body);
    try std.testing.expect(body.len > 0);
}

fn decodeSingleUserAllocationCampaign(allocator: std.mem.Allocator, body: []const u8) !void {
    var decoded = try decodeSingleUserBody(allocator, body, .{});
    defer decoded.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), decoded.orders.?.len);
}

test "single user codec cleans up across every allocation failure" {
    const allocator = std.testing.allocator;
    try std.testing.checkAllAllocationFailures(
        allocator,
        encodeSingleUserAllocationCampaign,
        .{},
    );

    const body = try encodeSingleUserBody(allocator, singleUserAllocationFixture(), .{});
    defer allocator.free(body);
    try std.testing.checkAllAllocationFailures(
        allocator,
        decodeSingleUserAllocationCampaign,
        .{body},
    );
}

fn appendPackedTestBody(
    allocator: std.mem.Allocator,
    destination: *std.ArrayListUnmanaged(BinaryEventCarrier),
    label: SectionLabel,
    body: []const u8,
) !void {
    const carriers = try packSectionEvents(allocator, label, body);
    defer allocator.free(carriers);
    try destination.appendSlice(allocator, carriers);
}

fn appendStateHashTestSection(
    allocator: std.mem.Allocator,
    destination: *std.ArrayListUnmanaged(BinaryEventCarrier),
    label: SectionLabel,
    entries: []const StateHashEntry,
) !void {
    const body = try encodeStateHashBody(allocator, .{ .entries = entries }, .{});
    defer allocator.free(body);
    try appendPackedTestBody(allocator, destination, label, body);
}

fn appendSingleUserTestSection(
    allocator: std.mem.Allocator,
    destination: *std.ArrayListUnmanaged(BinaryEventCarrier),
    label: SectionLabel,
    report: SingleUserReport,
) !void {
    const body = try encodeSingleUserBody(allocator, report, .{});
    defer allocator.free(body);
    try appendPackedTestBody(allocator, destination, label, body);
}

fn appendTotalBalanceTestSection(
    allocator: std.mem.Allocator,
    destination: *std.ArrayListUnmanaged(BinaryEventCarrier),
    label: SectionLabel,
    report: TotalCurrencyBalanceReport,
) !void {
    const body = try encodeTotalCurrencyBalanceBody(allocator, report, .{});
    defer allocator.free(body);
    try appendPackedTestBody(allocator, destination, label, body);
}

fn makeMatcherEventNodes(
    allocator: std.mem.Allocator,
    carriers: []const BinaryEventCarrier,
) ![]core_events.MatcherTradeEvent {
    const nodes = try allocator.alloc(core_events.MatcherTradeEvent, carriers.len);
    for (nodes, carriers) |*node, carrier| {
        node.* = .{
            .event_type = .binary_event,
            .section = carrier.section_id,
            .matched_order_id = carrier.words[0],
            .matched_order_uid = carrier.words[1],
            .price = carrier.words[2],
            .size = carrier.words[3],
            .bidder_hold_price = carrier.words[4],
        };
    }
    if (nodes.len > 1) {
        for (nodes[0 .. nodes.len - 1], 0..) |*node, index| {
            node.next_event = &nodes[index + 1];
        }
    }
    return nodes;
}

fn matcherHead(nodes: []const core_events.MatcherTradeEvent) ?*const core_events.MatcherTradeEvent {
    return if (nodes.len == 0) null else &nodes[0];
}

test "mixed matcher chain groups the exact 4ME 2RE external section set in TreeMap order" {
    const allocator = std.testing.allocator;
    var carriers: std.ArrayListUnmanaged(BinaryEventCarrier) = .empty;
    defer carriers.deinit(allocator);

    const me2 = [_]StateHashEntry{.{
        .module_id = 2,
        .submodule = .matching_order_books,
        .hash = 202,
    }};
    const re0 = [_]StateHashEntry{.{
        .module_id = 0,
        .submodule = .risk_fees,
        .hash = 10,
    }};
    const me0 = [_]StateHashEntry{.{
        .module_id = 0,
        .submodule = .matching_order_books,
        .hash = 200,
    }};
    const re1 = [_]StateHashEntry{.{
        .module_id = 1,
        .submodule = .risk_fees,
        .hash = 11,
    }};
    const me3 = [_]StateHashEntry{.{
        .module_id = 3,
        .submodule = .matching_order_books,
        .hash = 203,
    }};
    const me1 = [_]StateHashEntry{.{
        .module_id = 1,
        .submodule = .matching_order_books,
        .hash = 201,
    }};

    try appendStateHashTestSection(allocator, &carriers, try SectionLabel.matching(2), &me2);
    try appendStateHashTestSection(allocator, &carriers, try SectionLabel.risk(0), &re0);
    try appendStateHashTestSection(allocator, &carriers, try SectionLabel.matching(0), &me0);
    try appendStateHashTestSection(allocator, &carriers, try SectionLabel.risk(1), &re1);
    try appendStateHashTestSection(allocator, &carriers, try SectionLabel.matching(3), &me3);
    try appendStateHashTestSection(allocator, &carriers, try SectionLabel.matching(1), &me1);

    const nodes = try makeMatcherEventNodes(allocator, carriers.items);
    defer allocator.free(nodes);
    var grouped = try groupMatcherEventChain(allocator, matcherHead(nodes), .{});
    defer grouped.deinit(allocator);

    const expected_ids = [_]i32{ 0, 1, 1024, 1025, 1026, 1027 };
    try std.testing.expectEqual(expected_ids.len, grouped.sections.len);
    for (grouped.sections, expected_ids) |section, expected_id| {
        try std.testing.expectEqual(expected_id, section.label.externalId());
    }

    var merged = try mergeStateHashGroupedEvents(allocator, &grouped, .{});
    defer merged.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 6), merged.entries.len);
}

test "grouping preserves encounter order for repeated section carriers and later StateHash section wins" {
    const allocator = std.testing.allocator;
    const long_entries = [_]StateHashEntry{
        .{ .module_id = 0, .submodule = .risk_symbol_spec_provider, .hash = 1 },
        .{ .module_id = 0, .submodule = .risk_user_profile_service, .hash = 2 },
        .{ .module_id = 0, .submodule = .risk_binary_cmd_processor, .hash = 3 },
        .{ .module_id = 0, .submodule = .risk_last_price_cache, .hash = 4 },
    };
    const long_body = try encodeStateHashBody(allocator, .{ .entries = &long_entries }, .{});
    defer allocator.free(long_body);
    const risk_events = try packSectionEvents(allocator, try SectionLabel.risk(0), long_body);
    defer allocator.free(risk_events);
    try std.testing.expectEqual(@as(usize, 2), risk_events.len);

    const other_entry = [_]StateHashEntry{.{
        .module_id = 9,
        .submodule = .matching_order_books,
        .hash = 9,
    }};
    const other_body = try encodeStateHashBody(allocator, .{ .entries = &other_entry }, .{});
    defer allocator.free(other_body);
    const matching_events = try packSectionEvents(allocator, try SectionLabel.matching(0), other_body);
    defer allocator.free(matching_events);

    const interleaved = [_]BinaryEventCarrier{
        risk_events[0],
        matching_events[0],
        risk_events[1],
    };
    var grouped = try groupBinaryEventCarriers(allocator, &interleaved, .{});
    defer grouped.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 2), grouped.sections.len);
    try std.testing.expectEqual(@as(usize, 2), grouped.sections[0].carriers.len);
    try std.testing.expectEqualDeep(risk_events[0], grouped.sections[0].carriers[0]);
    try std.testing.expectEqualDeep(risk_events[1], grouped.sections[0].carriers[1]);

    var decoded = try decodeStateHashEvents(allocator, grouped.sections[0].carriers, .{});
    defer decoded.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 4), decoded.report.entries.len);

    var override_carriers: std.ArrayListUnmanaged(BinaryEventCarrier) = .empty;
    defer override_carriers.deinit(allocator);
    const early = [_]StateHashEntry{.{
        .module_id = 7,
        .submodule = .risk_fees,
        .hash = 11,
    }};
    const late = [_]StateHashEntry{.{
        .module_id = 7,
        .submodule = .risk_fees,
        .hash = 22,
    }};
    // Deliberately place the higher external section first in the chain. Java
    // TreeMap encounter order still makes it the later merge operand.
    try appendStateHashTestSection(allocator, &override_carriers, try SectionLabel.matching(0), &late);
    try appendStateHashTestSection(allocator, &override_carriers, try SectionLabel.risk(0), &early);
    var override_grouped = try groupBinaryEventCarriers(allocator, override_carriers.items, .{});
    defer override_grouped.deinit(allocator);
    var merged = try mergeStateHashGroupedEvents(allocator, &override_grouped, .{});
    defer merged.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), merged.entries.len);
    try std.testing.expectEqual(@as(i32, 22), merged.entries[0].hash);
}

fn testOrder(order_id: i64) Order {
    return .{
        .order_id = order_id,
        .price = 100 + order_id,
        .size = 10,
        .filled = 1,
        .reserve_bid_price = 200 + order_id,
        .action = .bid,
        .uid = 42,
        .timestamp = 300 + order_id,
    };
}

test "SingleUser merge preserves Java identity bug nullability status and whole-symbol override" {
    const allocator = std.testing.allocator;
    const accounts = [_]IntLongEntry{.{ .key = 11, .value = 500 }};
    const positions = [_]PositionEntry{.{
        .symbol = 77,
        .quote_currency = 15,
        .direction = .long,
        .open_volume = 2,
        .open_price_sum = 20,
        .profit = 3,
        .pending_sell_size = 4,
        .pending_buy_size = 5,
    }};
    const early_orders = [_]Order{testOrder(1)};
    const late_orders = [_]Order{testOrder(2)};
    const early_groups = [_]SymbolOrders{.{ .symbol = 77, .orders = &early_orders }};
    const late_groups = [_]SymbolOrders{.{ .symbol = 77, .orders = &late_orders }};
    const risk_found = SingleUserReport{
        .uid = 42,
        .user_status = .active,
        .accounts = &accounts,
        .positions = &positions,
        .orders = null,
        .query_execution_status = .ok,
    };
    const matching_early = SingleUserReport{
        .uid = 42,
        .user_status = null,
        .accounts = null,
        .positions = null,
        .orders = &early_groups,
        .query_execution_status = .ok,
    };
    const matching_late = SingleUserReport{
        .uid = 42,
        .user_status = null,
        .accounts = null,
        .positions = null,
        .orders = &late_groups,
        .query_execution_status = .ok,
    };

    var carriers: std.ArrayListUnmanaged(BinaryEventCarrier) = .empty;
    defer carriers.deinit(allocator);
    try appendSingleUserTestSection(allocator, &carriers, try SectionLabel.matching(1), matching_late);
    try appendSingleUserTestSection(allocator, &carriers, try SectionLabel.risk(0), risk_found);
    try appendSingleUserTestSection(allocator, &carriers, try SectionLabel.matching(0), matching_early);
    var grouped = try groupBinaryEventCarriers(allocator, carriers.items, .{});
    defer grouped.deinit(allocator);
    var merged = try mergeSingleUserGroupedEvents(allocator, &grouped, .{});
    defer merged.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 0), merged.uid);
    try std.testing.expectEqual(UserStatus.active, merged.user_status.?);
    try std.testing.expectEqual(@as(i64, 500), merged.accounts.?[0].value);
    try std.testing.expectEqual(@as(i32, 77), merged.positions.?[0].symbol);
    try std.testing.expectEqual(@as(usize, 1), merged.orders.?.len);
    try std.testing.expectEqual(@as(i64, 2), merged.orders.?[0].orders[0].order_id);
    try std.testing.expectEqual(QueryExecutionStatus.ok, merged.query_execution_status);

    const empty_groups = [_]SymbolOrders{};
    const risk_missing = SingleUserReport{
        .uid = 42,
        .user_status = null,
        .accounts = null,
        .positions = null,
        .orders = null,
        .query_execution_status = .user_not_found,
    };
    const matching_empty = SingleUserReport{
        .uid = 42,
        .user_status = null,
        .accounts = null,
        .positions = null,
        .orders = &empty_groups,
        .query_execution_status = .ok,
    };
    var missing_carriers: std.ArrayListUnmanaged(BinaryEventCarrier) = .empty;
    defer missing_carriers.deinit(allocator);
    try appendSingleUserTestSection(allocator, &missing_carriers, try SectionLabel.matching(0), matching_empty);
    try appendSingleUserTestSection(allocator, &missing_carriers, try SectionLabel.risk(0), risk_missing);
    var missing_grouped = try groupBinaryEventCarriers(allocator, missing_carriers.items, .{});
    defer missing_grouped.deinit(allocator);
    var missing = try mergeSingleUserGroupedEvents(allocator, &missing_grouped, .{});
    defer missing.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 0), missing.uid);
    try std.testing.expect(missing.user_status == null);
    try std.testing.expect(missing.accounts == null);
    try std.testing.expect(missing.positions == null);
    try std.testing.expect(missing.orders != null);
    try std.testing.expectEqual(@as(usize, 0), missing.orders.?.len);
    try std.testing.expectEqual(QueryExecutionStatus.user_not_found, missing.query_execution_status);

    var identity = try mergeSingleUserEventChain(allocator, null, .{});
    defer identity.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 0), identity.uid);
    try std.testing.expect(identity.user_status == null);
    try std.testing.expect(identity.accounts == null);
    try std.testing.expect(identity.positions == null);
    try std.testing.expect(identity.orders == null);
    try std.testing.expectEqual(QueryExecutionStatus.ok, identity.query_execution_status);
}

test "TotalCurrencyBalance merge preserves empty identity present-empty fields and wrapping sums" {
    const allocator = std.testing.allocator;
    var identity = try mergeTotalCurrencyBalanceEventChain(allocator, null, .{});
    defer identity.deinit(allocator);
    try std.testing.expect(identity.account_balances == null);
    try std.testing.expect(identity.fees == null);
    try std.testing.expect(identity.adjustments == null);
    try std.testing.expect(identity.suspends == null);
    try std.testing.expect(identity.order_balances == null);
    try std.testing.expect(identity.open_interest_long == null);
    try std.testing.expect(identity.open_interest_short == null);

    const all_null = TotalCurrencyBalanceReport{
        .account_balances = null,
        .fees = null,
        .adjustments = null,
        .suspends = null,
        .order_balances = null,
        .open_interest_long = null,
        .open_interest_short = null,
    };
    var null_carriers: std.ArrayListUnmanaged(BinaryEventCarrier) = .empty;
    defer null_carriers.deinit(allocator);
    try appendTotalBalanceTestSection(allocator, &null_carriers, try SectionLabel.risk(0), all_null);
    var null_grouped = try groupBinaryEventCarriers(allocator, null_carriers.items, .{});
    defer null_grouped.deinit(allocator);
    var present_empty = try mergeTotalCurrencyBalanceGroupedEvents(allocator, &null_grouped, .{});
    defer present_empty.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), present_empty.account_balances.?.len);
    try std.testing.expectEqual(@as(usize, 0), present_empty.fees.?.len);
    try std.testing.expectEqual(@as(usize, 0), present_empty.adjustments.?.len);
    try std.testing.expectEqual(@as(usize, 0), present_empty.suspends.?.len);
    try std.testing.expectEqual(@as(usize, 0), present_empty.order_balances.?.len);
    try std.testing.expectEqual(@as(usize, 0), present_empty.open_interest_long.?.len);
    try std.testing.expectEqual(@as(usize, 0), present_empty.open_interest_short.?.len);

    const max_entry = [_]IntLongEntry{.{ .key = 5, .value = std.math.maxInt(i64) }};
    const one_entry = [_]IntLongEntry{.{ .key = 5, .value = 1 }};
    var early = all_null;
    early.account_balances = &max_entry;
    var late = all_null;
    late.account_balances = &one_entry;
    var overflow_carriers: std.ArrayListUnmanaged(BinaryEventCarrier) = .empty;
    defer overflow_carriers.deinit(allocator);
    try appendTotalBalanceTestSection(allocator, &overflow_carriers, try SectionLabel.matching(0), late);
    try appendTotalBalanceTestSection(allocator, &overflow_carriers, try SectionLabel.risk(0), early);
    var overflow_grouped = try groupBinaryEventCarriers(allocator, overflow_carriers.items, .{});
    defer overflow_grouped.deinit(allocator);
    var overflow = try mergeTotalCurrencyBalanceGroupedEvents(allocator, &overflow_grouped, .{});
    defer overflow.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), overflow.account_balances.?.len);
    try std.testing.expectEqual(std.math.minInt(i64), overflow.account_balances.?[0].value);
}

test "mixed event grouping rejects non-binary invalid-section and malformed report events" {
    const allocator = std.testing.allocator;
    var trade = core_events.MatcherTradeEvent{ .event_type = .trade };
    try std.testing.expectError(error.NonBinaryEvent, groupMatcherEventChain(allocator, &trade, .{}));

    var invalid_section = core_events.MatcherTradeEvent{
        .event_type = .binary_event,
        .section = -1,
    };
    try std.testing.expectError(
        error.InvalidSectionId,
        groupMatcherEventChain(allocator, &invalid_section, .{}),
    );

    var binary = core_events.MatcherTradeEvent{
        .event_type = .binary_event,
        .section = 0,
    };
    binary.next_event = &trade;
    try std.testing.expectError(error.NonBinaryEvent, groupMatcherEventChain(allocator, &binary, .{}));

    const malformed = [_]BinaryEventCarrier{.{
        .section_id = 0,
        .words = .{ -1, 0, 0, 0, 0 },
    }};
    var malformed_grouped = try groupBinaryEventCarriers(allocator, &malformed, .{});
    defer malformed_grouped.deinit(allocator);
    try std.testing.expectError(
        error.NegativeCount,
        mergeStateHashGroupedEvents(allocator, &malformed_grouped, .{}),
    );
}

fn mergedReportsAllocationCampaign(
    allocator: std.mem.Allocator,
    state_carriers: []const BinaryEventCarrier,
    single_carriers: []const BinaryEventCarrier,
    total_carriers: []const BinaryEventCarrier,
) !void {
    var state_grouped = try groupBinaryEventCarriers(allocator, state_carriers, .{});
    defer state_grouped.deinit(allocator);
    var state = try mergeStateHashGroupedEvents(allocator, &state_grouped, .{});
    defer state.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), state.entries.len);

    var single_grouped = try groupBinaryEventCarriers(allocator, single_carriers, .{});
    defer single_grouped.deinit(allocator);
    var single = try mergeSingleUserGroupedEvents(allocator, &single_grouped, .{});
    defer single.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), single.orders.?.len);

    var total_grouped = try groupBinaryEventCarriers(allocator, total_carriers, .{});
    defer total_grouped.deinit(allocator);
    var total = try mergeTotalCurrencyBalanceGroupedEvents(allocator, &total_grouped, .{});
    defer total.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), total.account_balances.?.len);
}

test "mixed grouping and all three mergers clean up across every allocation failure" {
    const allocator = std.testing.allocator;
    var state_carriers: std.ArrayListUnmanaged(BinaryEventCarrier) = .empty;
    defer state_carriers.deinit(allocator);
    const state_entries = [_]StateHashEntry{.{
        .module_id = 0,
        .submodule = .risk_binary_cmd_processor,
        .hash = -1,
    }};
    try appendStateHashTestSection(allocator, &state_carriers, try SectionLabel.risk(0), &state_entries);

    var single_carriers: std.ArrayListUnmanaged(BinaryEventCarrier) = .empty;
    defer single_carriers.deinit(allocator);
    const orders = [_]Order{testOrder(7)};
    const groups = [_]SymbolOrders{.{ .symbol = 9, .orders = &orders }};
    const single = SingleUserReport{
        .uid = 42,
        .user_status = null,
        .accounts = null,
        .positions = null,
        .orders = &groups,
        .query_execution_status = .ok,
    };
    try appendSingleUserTestSection(allocator, &single_carriers, try SectionLabel.matching(0), single);

    var total_carriers: std.ArrayListUnmanaged(BinaryEventCarrier) = .empty;
    defer total_carriers.deinit(allocator);
    const balances = [_]IntLongEntry{.{ .key = 11, .value = 12 }};
    const total = TotalCurrencyBalanceReport{
        .account_balances = &balances,
        .fees = null,
        .adjustments = null,
        .suspends = null,
        .order_balances = null,
        .open_interest_long = null,
        .open_interest_short = null,
    };
    try appendTotalBalanceTestSection(allocator, &total_carriers, try SectionLabel.risk(0), total);

    try std.testing.checkAllAllocationFailures(
        allocator,
        mergedReportsAllocationCampaign,
        .{ state_carriers.items, single_carriers.items, total_carriers.items },
    );
}
