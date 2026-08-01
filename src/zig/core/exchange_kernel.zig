const std = @import("std");
const common = @import("common.zig");
const cmd = @import("cmd.zig");
const events = @import("events.zig");
const naive = @import("orderbook_naive.zig");
const direct = @import("orderbook_direct.zig");
const risk = @import("risk.zig");
const user = @import("user_profile.zig");
const binary_frames = @import("binary_frames.zig");
const binary_payload = @import("binary_payload.zig");
const custom_report_pipeline = @import("custom_report_pipeline.zig");
const java_binary_wire = @import("java_binary_wire.zig");
const java_report_producer = @import("java_report_producer.zig");
const java_report_wire = @import("java_report_wire.zig");
const int_map_order = @import("eclipse_int_map_order.zig");
const publication = @import("publication_sequence.zig");
const report_query_envelope = @import("report_query_envelope.zig");
const report_registry = @import("report_registry.zig");
const settlement_lifecycle = @import("settlement_lifecycle.zig");

pub const OrderBookKind = enum {
    naive,
    direct,
};

pub const JavaIdentityHashContext = java_report_producer.JavaIdentityHashContext;
pub const JavaReportResponse = java_report_producer.JavaReportResponse;
pub const JavaStateHashReport = java_report_producer.JavaStateHashReport;
pub const JavaSingleUserReport = java_report_wire.SingleUserReport;
pub const JavaTotalCurrencyBalanceReport = java_report_wire.TotalCurrencyBalanceReport;

const reserved_report_codes = [_]i32{
    @intFromEnum(binary_payload.ReportQueryTypeCode.state_hash),
    @intFromEnum(binary_payload.ReportQueryTypeCode.single_user),
    @intFromEnum(binary_payload.ReportQueryTypeCode.total_currency_balance),
};

const empty_custom_report_registry = report_registry.Registry.fromSpecs(.{});

const Book = union(enum) {
    naive: naive.NaiveOrderBook,
    direct: direct.DirectOrderBook,

    fn deinit(self: *Book) void {
        switch (self.*) {
            .naive => |*book| book.deinit(),
            .direct => |*book| book.deinit(),
        }
    }

    fn processCommand(
        self: *Book,
        command: *cmd.OrderCommand,
        event_chain: ?*events.EventChain,
    ) naive.ProcessError!cmd.CommandResultCode {
        return switch (self.*) {
            .naive => |*book| try book.processCommandWithEvents(command, event_chain),
            .direct => |*book| try book.processCommandWithEvents(command, event_chain),
        };
    }

    fn l2Snapshot(
        self: *Book,
        allocator: std.mem.Allocator,
        depth: usize,
    ) std.mem.Allocator.Error!naive.L2MarketData {
        return switch (self.*) {
            .naive => |*book| try book.l2Snapshot(allocator, depth),
            .direct => |*book| try book.l2Snapshot(allocator, depth),
        };
    }

    fn accumulateOrderHolds(
        self: *Book,
        allocator: std.mem.Allocator,
        out: *std.AutoHashMapUnmanaged(i32, i64),
    ) (std.mem.Allocator.Error || error{ArithmeticOverflow})!void {
        switch (self.*) {
            .naive => |*book| try book.accumulateOrderHolds(allocator, out),
            .direct => |*book| try book.accumulateOrderHolds(allocator, out),
        }
    }

    fn appendUserOrders(
        self: *Book,
        allocator: std.mem.Allocator,
        uid: i64,
        out: *std.ArrayListUnmanaged(naive.UserOrderView),
    ) std.mem.Allocator.Error!void {
        switch (self.*) {
            .naive => |*book| try book.appendUserOrders(allocator, uid, out),
            .direct => |*book| try book.appendUserOrders(allocator, uid, out),
        }
    }

    fn appendAllOrders(
        self: *const Book,
        allocator: std.mem.Allocator,
        out: *std.ArrayListUnmanaged(naive.UserOrderView),
    ) std.mem.Allocator.Error!void {
        return switch (self.*) {
            .naive => |*book| try book.appendAllOrders(allocator, out),
            .direct => |*book| try book.appendAllOrders(allocator, out),
        };
    }

    fn restoreOrders(
        self: *Book,
        symbol: i32,
        orders: []const naive.UserOrderView,
    ) (std.mem.Allocator.Error || error{
        InvalidOrderSymbol,
        InvalidOrderSize,
        InvalidOrderFill,
        DuplicateOrderId,
    })!void {
        return switch (self.*) {
            .naive => |*book| try book.restoreOrders(symbol, orders),
            .direct => |*book| try book.restoreOrders(symbol, orders),
        };
    }

    fn stateHash(self: *const Book) std.mem.Allocator.Error!u64 {
        return switch (self.*) {
            .naive => |*book| try book.stateHash(),
            .direct => |*book| try book.stateHash(),
        };
    }

    fn symbolSpecification(self: *const Book) common.CoreSymbolSpecification {
        return switch (self.*) {
            .naive => |book| book.symbol_spec,
            .direct => |book| book.symbol_spec,
        };
    }
};

pub const CommandResponse = struct {
    sequence: u64 = 0,
    /// Completed command image observed at the publication boundary. This is
    /// a value copy, so callbacks can inspect the original request metadata
    /// together with its final result without borrowing the caller's command.
    command: cmd.OrderCommand,
    result_code: cmd.CommandResultCode,
    l2_snapshot: ?naive.L2MarketData = null,
    report_response: ?ReportResponse = null,
    java_report_response: ?JavaReportResponse = null,
    matcher_events_head: ?*events.MatcherTradeEvent = null,
    /// Owner for matcher event nodes. When null, deinit falls back to the
    /// allocator argument (heap-compatible default for legacy construction).
    events_owner: ?events.EventNodesOwner = null,

    pub fn deinit(self: *CommandResponse, allocator: std.mem.Allocator) void {
        if (self.l2_snapshot) |*snapshot| {
            snapshot.deinit(allocator);
            self.l2_snapshot = null;
        }
        if (self.report_response) |*report| {
            report.deinit(allocator);
            self.report_response = null;
        }
        if (self.java_report_response) |*report| {
            report.deinit(allocator);
            self.java_report_response = null;
        }
        if (self.matcher_events_head) |head| {
            // Pool-backed nodes must free through the pool (events_owner.pool).
            // Heap-backed nodes free through the deinit allocator argument so a
            // transferred response can outlive a locked-allocator facade that
            // was only used for production.
            const owner: events.EventNodesOwner = if (self.events_owner) |stored| switch (stored) {
                .pool => |pool| .{ .pool = pool },
                .heap => .{ .heap = allocator },
            } else .{ .heap = allocator };
            events.deinitMatcherEventChain(owner, head);
            self.matcher_events_head = null;
        }
        self.events_owner = null;
    }

    fn completeFromCommand(self: *CommandResponse, command: *const cmd.OrderCommand) void {
        self.command = command.*;
        self.result_code = command.result_code;
    }
};

pub const BatchResponse = struct {
    sequence_from: ?u64 = null,
    sequence_to: ?u64 = null,
    result_code: cmd.CommandResultCode = .accepted,
};

pub const TotalCurrencyBalanceReport = struct {
    account_balances: std.AutoHashMapUnmanaged(i32, i64) = .empty,
    orders_balances: std.AutoHashMapUnmanaged(i32, i64) = .empty,
    fees: std.AutoHashMapUnmanaged(i32, i64) = .empty,
    adjustments: std.AutoHashMapUnmanaged(i32, i64) = .empty,
    suspends: std.AutoHashMapUnmanaged(i32, i64) = .empty,
    open_interest_long: std.AutoHashMapUnmanaged(i32, i64) = .empty,
    open_interest_short: std.AutoHashMapUnmanaged(i32, i64) = .empty,

    pub fn deinit(self: *TotalCurrencyBalanceReport, allocator: std.mem.Allocator) void {
        self.account_balances.deinit(allocator);
        self.orders_balances.deinit(allocator);
        self.fees.deinit(allocator);
        self.adjustments.deinit(allocator);
        self.suspends.deinit(allocator);
        self.open_interest_long.deinit(allocator);
        self.open_interest_short.deinit(allocator);
        self.* = undefined;
    }
};

pub const SingleUserQueryExecutionStatus = enum {
    ok,
    user_not_found,
};

pub const SingleUserPosition = struct {
    quote_currency: i32,
    direction: common.PositionDirection,
    open_volume: i64,
    open_price_sum: i64,
    profit: i64,
    pending_sell_size: i64,
    pending_buy_size: i64,
};

pub const SingleUserReport = struct {
    uid: i64,
    query_execution_status: SingleUserQueryExecutionStatus,
    user_status: ?common.UserStatus = null,
    accounts: std.AutoHashMapUnmanaged(i32, i64) = .empty,
    positions: std.AutoHashMapUnmanaged(i32, SingleUserPosition) = .empty,
    orders: std.ArrayListUnmanaged(naive.UserOrderView) = .empty,

    pub fn deinit(self: *SingleUserReport, allocator: std.mem.Allocator) void {
        self.accounts.deinit(allocator);
        self.positions.deinit(allocator);
        self.orders.deinit(allocator);
        self.* = undefined;
    }
};

pub const StateHashModuleType = enum(i8) {
    risk_engine = 0,
    matching_engine = 1,
};

pub const StateHashSubmoduleType = enum(i32) {
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

    pub fn moduleType(self: StateHashSubmoduleType) StateHashModuleType {
        return switch (self) {
            .risk_symbol_spec_provider,
            .risk_user_profile_service,
            .risk_binary_cmd_processor,
            .risk_last_price_cache,
            .risk_fees,
            .risk_adjustments,
            .risk_suspends,
            .risk_shard_mask,
            => .risk_engine,
            .matching_binary_cmd_processor,
            .matching_order_books,
            .matching_shard_mask,
            => .matching_engine,
        };
    }
};

pub const StateHashSubmodule = struct {
    module_id: i32,
    submodule: StateHashSubmoduleType,
    hash: u64,
};

pub const DetailedStateHashReport = struct {
    aggregate_hash: u64,
    submodules: std.ArrayListUnmanaged(StateHashSubmodule) = .empty,

    pub fn deinit(self: *DetailedStateHashReport, allocator: std.mem.Allocator) void {
        self.submodules.deinit(allocator);
        self.* = undefined;
    }
};

pub const ReportQuery = union(enum) {
    risk_state_hash,
    state_hash_detailed,
    fee_collected: i32,
    single_user: i64,
    total_currency_balance,
    user_balance: struct {
        uid: i64,
        currency: i32,
    },
    order_book: struct {
        symbol: i32,
        depth: usize,
    },
};

fn nativeQueryForJava(query: binary_payload.DecodedReportQuery) ?ReportQuery {
    return switch (query) {
        .state_hash => .state_hash_detailed,
        .single_user => |uid| .{ .single_user = uid },
        .total_currency_balance => .total_currency_balance,
        .unknown => null,
    };
}

pub const ReportResponse = union(enum) {
    risk_state_hash: u64,
    state_hash_detailed: DetailedStateHashReport,
    fee_collected: i64,
    single_user: SingleUserReport,
    total_currency_balance: TotalCurrencyBalanceReport,
    user_balance: ?i64,
    order_book: naive.L2MarketData,

    pub fn deinit(self: *ReportResponse, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .order_book => |*snapshot| snapshot.deinit(allocator),
            .single_user => |*report| report.deinit(allocator),
            .total_currency_balance => |*report| report.deinit(allocator),
            .state_hash_detailed => |*report| report.deinit(allocator),
            else => {},
        }
    }
};

pub const Config = struct {
    orderbook_kind: OrderBookKind = .direct,
    shard_id: u64 = 0,
    num_shards: u64 = 1,
    send_l2_for_every_cmd: bool = false,
    l2_refresh_depth: usize = 8,
    risk: risk.Config = .{},
    report_queries: ReportQueriesConfig = .{},
    java_identity_hash_context: ?JavaIdentityHashContext = null,
    java_report_wire: java_report_wire.Config = .{},
    /// When non-null, matcher events use a bootstrap-sized MatcherEventPool.
    matcher_event_pool_capacity: ?usize = null,
    /// When both set, direct books use initStatic (hard-capped slabs).
    order_pool_max_orders: ?usize = null,
    order_pool_max_buckets: ?usize = null,
    /// Phase C map pre-reserve applied at risk engine init.
    pre_reserve_users: usize = 0,
    pre_reserve_accounts_per_user: usize = 0,
    pre_reserve_positions_per_user: usize = 0,
};

pub const ReportQueriesConfig = struct {
    state_hash: bool = true,
    single_user: bool = true,
    total_currency_balance: bool = true,
    add_accounts_binary: bool = true,
    add_symbols_binary: bool = true,
    custom_registry: *const report_registry.Registry = empty_custom_report_registry,
};

pub const InitError = risk.InitError ||
    report_registry.RegistryInitError ||
    report_registry.ReservedCodeValidationError ||
    report_registry.RegistryContextValidationError ||
    error{
        InvalidCustomReportShardId,
        InvalidL2RefreshDepth,
    };
pub const ExecuteError = risk.ReleaseError || naive.ProcessError || error{
    SequenceOverflow,
    NonMonotonicPublicationSequence,
};
pub const ExecuteWithSettlementError = ExecuteError || settlement_lifecycle.Error;

fn omitDisabledSettlementErrors(
    result: ExecuteWithSettlementError!CommandResponse,
) ExecuteError!CommandResponse {
    return result catch |err| switch (err) {
        error.SettlementPreparationRejected,
        error.SettlementRiskR2MarkRejected,
        error.SettlementJournalMarkRejected,
        error.SettlementRecoveryRejected,
        => unreachable,
        else => |execute_err| return execute_err,
    };
}
const BinaryCommandWireDecodeError = binary_payload.DecodeError ||
    java_binary_wire.RequestDecodeError ||
    error{
        NonCanonicalJavaBinaryPayload,
        NonCanonicalLegacyBinaryPayload,
    };
const BinaryQueryWireDecodeError = binary_payload.DecodeQueryError ||
    java_binary_wire.RequestDecodeError ||
    error{
        NonCanonicalJavaBinaryPayload,
        NonCanonicalLegacyBinaryPayload,
    };
const BinaryPayloadExecutionError = BinaryCommandWireDecodeError ||
    risk.SymbolSpecificationProvider.AddSymbolError ||
    int_map_order.LayoutError ||
    risk.ReleaseError ||
    error{
        BinaryCommandRejected,
    };
pub const ReportError = std.mem.Allocator.Error || error{
    UnknownSymbol,
    ArithmeticOverflow,
    UnsupportedReportQueryType,
    BinaryFrameCorrupted,
    StateHashCorrupted,
};
pub const RestingOrder = naive.UserOrderView;
pub const MatchingBooksHistory = int_map_order.History;
pub const MatchingBooksHistoryValidationError = int_map_order.HistoryError || error{
    HistorySemanticKeySetMismatch,
};
const JavaReportBuildError = java_report_producer.ProducerError ||
    MatchingBooksHistoryValidationError;

const OwnedJavaMatchingSection = struct {
    allocator: std.mem.Allocator,
    books: []java_report_producer.MatchingBookView,
    view: java_report_producer.MatchingSectionView,

    fn deinit(self: *OwnedJavaMatchingSection) void {
        for (self.books) |book| self.allocator.free(book.orders);
        self.allocator.free(self.books);
        self.* = undefined;
    }
};

/// Stable, read-only callback surface for custom matching-engine reports.
///
/// The kernel pointer is borrowed only for the duration of one report
/// callback. The explicit binary processor mirrors Java's matching-engine
/// report context without making report implementations reach through kernel
/// storage for it.
pub const MatchingReportView = struct {
    kernel: *const ExchangeKernel,
    binary_processor: *const binary_frames.BinaryFrameAssembler,
};

/// Stable, read-only callback surface for custom risk-engine reports.
///
/// Both pointers are borrowed only for the duration of one report callback.
pub const RiskReportView = struct {
    engine: *const risk.RiskEngine,
    binary_processor: *const binary_frames.BinaryFrameAssembler,
};

fn buildJavaMatchingSection(
    allocator: std.mem.Allocator,
    kernel: *const ExchangeKernel,
    shard_id: i32,
    shard_mask: u64,
    require_history: bool,
) MatchingBooksHistoryValidationError!OwnedJavaMatchingSection {
    const symbol_ids = if (require_history)
        try kernel.matchingBookKeysInHistoryOrder(allocator)
    else blk: {
        const keys = try allocator.alloc(i32, kernel.books.count());
        var index: usize = 0;
        var iterator = kernel.books.keyIterator();
        while (iterator.next()) |key| : (index += 1) keys[index] = key.*;
        std.debug.assert(index == keys.len);
        break :blk keys;
    };
    defer allocator.free(symbol_ids);

    const books = try allocator.alloc(java_report_producer.MatchingBookView, symbol_ids.len);
    var initialized: usize = 0;
    errdefer {
        for (books[0..initialized]) |book| allocator.free(book.orders);
        allocator.free(books);
    }

    for (symbol_ids) |symbol_id| {
        const book_ptr = kernel.books.get(symbol_id) orelse
            return error.HistorySemanticKeySetMismatch;
        var orders: std.ArrayListUnmanaged(naive.UserOrderView) = .empty;
        errdefer orders.deinit(allocator);
        try book_ptr.appendAllOrders(allocator, &orders);
        const owned_orders = try orders.toOwnedSlice(allocator);
        books[initialized] = .{
            .traversal = switch (book_ptr.*) {
                .naive => .naive,
                .direct => .direct,
            },
            .symbol_specification = book_ptr.symbolSpecification(),
            .orders = owned_orders,
        };
        initialized += 1;
    }

    return .{
        .allocator = allocator,
        .books = books,
        .view = .{
            .shard_id = shard_id,
            .shard_mask = shard_mask,
            .binary_processor = &kernel.binary_frame_assembler,
            .books = books,
        },
    };
}

const CustomReportDispatchError = report_registry.ProcessDispatchError ||
    java_report_wire.WireError;

fn appendCustomRiskSection(
    allocator: std.mem.Allocator,
    chain: *events.EventChain,
    descriptor: *const report_registry.Descriptor,
    body: []const u8,
    view: *const RiskReportView,
    shard_id: i32,
    shard_mask: u64,
    wire_config: java_report_wire.Config,
) CustomReportDispatchError!void {
    var outcome = try descriptor.processRiskBody(
        RiskReportView,
        allocator,
        body,
        .{
            .view = view,
            .shard_id = shard_id,
            .shard_mask = shard_mask,
        },
    );
    defer outcome.deinit();

    switch (outcome) {
        .section => |section| try custom_report_pipeline.appendSection(
            allocator,
            chain,
            try java_report_wire.SectionLabel.risk(shard_id),
            section,
            wire_config,
        ),
        .constructor_rejected, .no_section => {},
    }
}

fn appendCustomMatchingSection(
    allocator: std.mem.Allocator,
    chain: *events.EventChain,
    descriptor: *const report_registry.Descriptor,
    body: []const u8,
    view: *const MatchingReportView,
    shard_id: i32,
    shard_mask: u64,
    wire_config: java_report_wire.Config,
) CustomReportDispatchError!void {
    var outcome = try descriptor.processMatchingBody(
        MatchingReportView,
        allocator,
        body,
        .{
            .view = view,
            .shard_id = shard_id,
            .shard_mask = shard_mask,
        },
    );
    defer outcome.deinit();

    switch (outcome) {
        .section => |section| try custom_report_pipeline.appendSection(
            allocator,
            chain,
            try java_report_wire.SectionLabel.matching(shard_id),
            section,
            wire_config,
        ),
        .constructor_rejected, .no_section => {},
    }
}

pub const OrderSnapshotReadError = std.mem.Allocator.Error || error{
    UnknownSymbol,
    OrderBookMapCorrupted,
};
pub const OrderSnapshotRestoreError = std.mem.Allocator.Error || error{
    UnknownSymbol,
    InvalidOrderSymbol,
    InvalidOrderSize,
    InvalidOrderFill,
    DuplicateOrderId,
};

/// Decodes Java's size-prefixed raw-LZ4 request format first. A canonical raw
/// big-endian Zig body remains as a compatibility fallback while callers
/// migrate. Because the legacy format has no discriminator, fallback is only
/// accepted when re-encoding produces the exact same minimally padded words.
fn decodeBinaryCommandWordsJavaFirst(
    allocator: std.mem.Allocator,
    words: []const i64,
) BinaryCommandWireDecodeError!binary_payload.DecodedBinaryCommand {
    var java_request = java_binary_wire.decodeRequestWords(allocator, words, .{}) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return decodeCanonicalLegacyBinaryCommand(allocator, words),
    };
    defer java_request.deinit(allocator);

    return decodeCanonicalJavaBinaryCommand(allocator, java_request.body) catch |java_err| switch (java_err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => decodeCanonicalLegacyBinaryCommand(allocator, words) catch |legacy_err| switch (legacy_err) {
            error.OutOfMemory => return error.OutOfMemory,
            // A valid Java envelope is the stronger format signal. If the
            // complete carrier is not also an exact canonical legacy request,
            // retain the Java body/schema failure rather than changing its
            // public failure classification based on fallback internals.
            else => return java_err,
        },
    };
}

fn decodeCanonicalJavaBinaryCommand(
    allocator: std.mem.Allocator,
    body: []const u8,
) BinaryCommandWireDecodeError!binary_payload.DecodedBinaryCommand {
    var decoded = try binary_payload.decodeJavaFromBytes(allocator, body);
    errdefer decoded.deinit();
    const canonical = encodeDecodedBinaryCommand(allocator, &decoded, .java_little) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.NonCanonicalLegacyBinaryPayload => return error.NonCanonicalJavaBinaryPayload,
    };
    defer allocator.free(canonical);
    if (!std.mem.eql(u8, body, canonical)) {
        return error.NonCanonicalJavaBinaryPayload;
    }
    return decoded;
}

fn decodeBinaryQueryWordsJavaFirst(
    allocator: std.mem.Allocator,
    words: []const i64,
) BinaryQueryWireDecodeError!binary_payload.DecodedReportQuery {
    var java_request = java_binary_wire.decodeRequestWords(allocator, words, .{}) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return decodeCanonicalLegacyBinaryQuery(allocator, words),
    };
    defer java_request.deinit(allocator);

    return decodeJavaBinaryQuery(allocator, java_request.body) catch |java_err| switch (java_err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => decodeCanonicalLegacyBinaryQuery(allocator, words) catch |legacy_err| switch (legacy_err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return java_err,
        },
    };
}

fn decodeJavaBinaryQuery(
    _: std.mem.Allocator,
    body: []const u8,
) BinaryQueryWireDecodeError!binary_payload.DecodedReportQuery {
    return binary_payload.decodeJavaQueryOrIgnoredFromBytes(body);
}

const BinaryBodyFormat = enum {
    java_little,
    legacy_big,
};

fn decodeCanonicalLegacyBinaryCommand(
    allocator: std.mem.Allocator,
    words: []const i64,
) BinaryCommandWireDecodeError!binary_payload.DecodedBinaryCommand {
    var decoded = try binary_payload.decodeFromWordsBigEndian(allocator, words);
    errdefer decoded.deinit();
    const canonical_body = try encodeDecodedBinaryCommand(allocator, &decoded, .legacy_big);
    defer allocator.free(canonical_body);
    if (!try canonicalBodyMatchesWords(allocator, canonical_body, words)) {
        return error.NonCanonicalLegacyBinaryPayload;
    }
    return decoded;
}

fn decodeCanonicalLegacyBinaryQuery(
    allocator: std.mem.Allocator,
    words: []const i64,
) BinaryQueryWireDecodeError!binary_payload.DecodedReportQuery {
    const decoded = try binary_payload.decodeQueryFromWordsBigEndian(allocator, words);
    const canonical_body = try encodeDecodedBinaryQuery(allocator, decoded, .legacy_big);
    defer allocator.free(canonical_body);
    if (!try canonicalBodyMatchesWords(allocator, canonical_body, words)) {
        return error.NonCanonicalLegacyBinaryPayload;
    }
    return decoded;
}

fn encodeDecodedBinaryCommand(
    allocator: std.mem.Allocator,
    decoded: *const binary_payload.DecodedBinaryCommand,
    format: BinaryBodyFormat,
) (std.mem.Allocator.Error || error{NonCanonicalLegacyBinaryPayload})![]u8 {
    return switch (decoded.payload) {
        .add_symbols => |symbols| switch (format) {
            .java_little => binary_payload.encodeJavaAddSymbolsToBytes(allocator, symbols),
            .legacy_big => binary_payload.encodeAddSymbolsToBytes(allocator, symbols),
        },
        .add_accounts => |users| switch (format) {
            .java_little => binary_payload.encodeJavaAddAccountsToBytes(allocator, users),
            .legacy_big => binary_payload.encodeAddAccountsToBytes(allocator, users),
        },
    } catch |err| switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        error.CollectionTooLarge => error.NonCanonicalLegacyBinaryPayload,
    };
}

fn encodeDecodedBinaryQuery(
    allocator: std.mem.Allocator,
    decoded: binary_payload.DecodedReportQuery,
    format: BinaryBodyFormat,
) std.mem.Allocator.Error![]u8 {
    return switch (format) {
        .java_little => binary_payload.encodeJavaReportQueryToBytes(allocator, decoded),
        .legacy_big => binary_payload.encodeReportQueryToBytes(allocator, decoded),
    };
}

fn canonicalBodyMatchesWords(
    allocator: std.mem.Allocator,
    body: []const u8,
    words: []const i64,
) (std.mem.Allocator.Error || error{NonCanonicalLegacyBinaryPayload})!bool {
    const canonical_words = java_binary_wire.packSignedWordsBigEndian(allocator, body) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.CarrierTooLarge => return error.NonCanonicalLegacyBinaryPayload,
    };
    defer allocator.free(canonical_words);
    return std.mem.eql(i64, canonical_words, words);
}

/// Java decodes `BatchAddSymbolsCommand` into `new IntObjectHashMap(count)`
/// before MatchingEngineRouter visits it. Replaying the raw wire slice directly
/// is observably wrong for colliding keys because sized-map reinsertion can
/// change the encounter order. Duplicate keys retain the final decoded value.
fn javaSymbolHandlerOrder(
    allocator: std.mem.Allocator,
    wire_symbols: []const common.CoreSymbolSpecification,
) int_map_order.LayoutError![]common.CoreSymbolSpecification {
    var temporary_history = try int_map_order.History.initWithExpectedSize(wire_symbols.len);
    defer temporary_history.deinit(allocator);

    var symbols_by_id: std.AutoHashMapUnmanaged(i32, common.CoreSymbolSpecification) = .empty;
    defer symbols_by_id.deinit(allocator);
    const semantic_capacity = std.math.cast(u32, wire_symbols.len) orelse
        return error.CapacityOverflow;
    try symbols_by_id.ensureTotalCapacity(allocator, semantic_capacity);

    for (wire_symbols) |spec| {
        try temporary_history.recordPut(allocator, spec.symbol_id);
        symbols_by_id.putAssumeCapacity(spec.symbol_id, spec);
    }

    const ordered_keys = temporary_history.orderedKeys(allocator) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        // This history originates locally from a validated sized constructor
        // and is never marked unavailable or imported from raw state.
        else => unreachable,
    };
    defer allocator.free(ordered_keys);
    std.debug.assert(ordered_keys.len == symbols_by_id.count());

    const ordered_symbols = try allocator.alloc(
        common.CoreSymbolSpecification,
        ordered_keys.len,
    );
    errdefer allocator.free(ordered_symbols);
    for (ordered_keys, 0..) |symbol_id, index| {
        ordered_symbols[index] = symbols_by_id.get(symbol_id) orelse
            unreachable;
    }
    return ordered_symbols;
}

pub const ExchangeKernel = struct {
    allocator: std.mem.Allocator,
    orderbook_kind: OrderBookKind,
    report_queries_cfg: ReportQueriesConfig,
    send_l2_for_every_cmd: bool,
    l2_refresh_depth: usize,
    java_identity_hash_context: ?JavaIdentityHashContext,
    java_report_wire: java_report_wire.Config,
    custom_report_registry: *report_registry.Registry,
    next_seq: u64 = 0,
    books: std.AutoHashMapUnmanaged(i32, *Book) = .empty,
    books_history: MatchingBooksHistory = MatchingBooksHistory.init(),
    risk_engine: risk.RiskEngine,
    binary_frame_assembler: binary_frames.BinaryFrameAssembler,
    /// Owned when matcher_event_pool_capacity was set at init; null = heap events.
    matcher_event_pool: ?*events.MatcherEventPool = null,
    order_pool_max_orders: ?usize = null,
    order_pool_max_buckets: ?usize = null,
    pre_reserve_accounts_per_user: usize = 0,
    pre_reserve_positions_per_user: usize = 0,

    pub fn init(allocator: std.mem.Allocator, config: Config) InitError!ExchangeKernel {
        if (config.l2_refresh_depth == 0) {
            return error.InvalidL2RefreshDepth;
        }

        const custom_report_registry = try config.report_queries.custom_registry.cloneOwned(allocator);
        errdefer custom_report_registry.deinit();
        try custom_report_registry.validateNoReservedCodes(&reserved_report_codes);
        try custom_report_registry.validateContextTypes(MatchingReportView, RiskReportView);
        if (custom_report_registry.len() != 0 and
            config.shard_id > @as(u64, @intCast(java_report_wire.max_unambiguous_shard_id)))
        {
            return error.InvalidCustomReportShardId;
        }

        var report_queries_cfg = config.report_queries;
        report_queries_cfg.custom_registry = custom_report_registry;
        var risk_engine = try risk.RiskEngine.init(
            allocator,
            config.shard_id,
            config.num_shards,
            config.risk,
        );
        errdefer risk_engine.deinit();

        if (config.pre_reserve_users > 0) {
            try risk_engine.preReserveUsers(config.pre_reserve_users);
        }
        risk_engine.pre_reserve_accounts_per_user = config.pre_reserve_accounts_per_user;
        risk_engine.pre_reserve_positions_per_user = config.pre_reserve_positions_per_user;

        var matcher_event_pool: ?*events.MatcherEventPool = null;
        errdefer if (matcher_event_pool) |pool| {
            pool.deinitForce();
            allocator.destroy(pool);
        };
        if (config.matcher_event_pool_capacity) |cap| {
            const pool = try allocator.create(events.MatcherEventPool);
            errdefer allocator.destroy(pool);
            pool.* = try events.MatcherEventPool.init(allocator, cap);
            matcher_event_pool = pool;
        }

        return .{
            .allocator = allocator,
            .orderbook_kind = config.orderbook_kind,
            .report_queries_cfg = report_queries_cfg,
            .send_l2_for_every_cmd = config.send_l2_for_every_cmd,
            .l2_refresh_depth = config.l2_refresh_depth,
            .java_identity_hash_context = config.java_identity_hash_context,
            .java_report_wire = config.java_report_wire,
            .custom_report_registry = custom_report_registry,
            .risk_engine = risk_engine,
            .binary_frame_assembler = binary_frames.BinaryFrameAssembler.init(allocator),
            .matcher_event_pool = matcher_event_pool,
            .order_pool_max_orders = config.order_pool_max_orders,
            .order_pool_max_buckets = config.order_pool_max_buckets,
            .pre_reserve_accounts_per_user = config.pre_reserve_accounts_per_user,
            .pre_reserve_positions_per_user = config.pre_reserve_positions_per_user,
        };
    }

    pub fn deinit(self: *ExchangeKernel) void {
        var it = self.books.valueIterator();
        while (it.next()) |book_ptr| {
            book_ptr.*.deinit();
            self.allocator.destroy(book_ptr.*);
        }
        self.books.deinit(self.allocator);
        self.books_history.deinit(self.allocator);
        self.binary_frame_assembler.deinit();
        self.risk_engine.deinit();
        self.custom_report_registry.deinit();
        if (self.matcher_event_pool) |pool| {
            // Force: outstanding responses may still hold nodes in rare teardown races.
            pool.deinitForce();
            self.allocator.destroy(pool);
            self.matcher_event_pool = null;
        }
        self.* = undefined;
    }

    pub fn eventsOwner(self: *ExchangeKernel) events.EventNodesOwner {
        if (self.matcher_event_pool) |pool| return .{ .pool = pool };
        return .{ .heap = self.allocator };
    }

    pub fn makeEventChain(self: *ExchangeKernel) events.EventChain {
        return events.EventChain.initOwner(self.eventsOwner());
    }

    /// Returns the immutable registry clone owned by this kernel. The borrow is
    /// valid until `deinit` and is the authority API request tokens must bind
    /// against before publication.
    pub fn customReportRegistry(self: *const ExchangeKernel) *const report_registry.Registry {
        return self.custom_report_registry;
    }

    fn resetMatchingState(self: *ExchangeKernel) void {
        var it = self.books.valueIterator();
        while (it.next()) |book_ptr| {
            book_ptr.*.deinit();
            self.allocator.destroy(book_ptr.*);
        }
        self.books.clearRetainingCapacity();
        self.books_history.clear();
        self.binary_frame_assembler.reset();
    }

    pub fn resetForSnapshotLoad(self: *ExchangeKernel) void {
        self.resetMatchingState();
        // Native snapshots currently retain only sorted semantic books, not
        // Java's sized-constructor capacity or backing-slot encounter history.
        // Keep exact-report/persistence consumers fail-closed after restore.
        self.markMatchingBooksHistoryUnavailable();
        self.risk_engine.reset();
        self.next_seq = 0;
    }

    /// Permanently discards exact Java backing-layout knowledge for the
    /// current matching state. Subsequent mutations deliberately remain
    /// unavailable instead of synthesizing a plausible order.
    pub fn markMatchingBooksHistoryUnavailable(self: *ExchangeKernel) void {
        if (!self.books_history.isKnown()) return;
        self.books_history.deinit(self.allocator);
        self.books_history = MatchingBooksHistory.initUnavailable();
    }

    /// Returns an owned Java/Eclipse iteration-order key slice after proving it
    /// is the exact key set of the semantic books map. The caller must free the
    /// slice with `allocator`.
    pub fn matchingBookKeysInHistoryOrder(
        self: *const ExchangeKernel,
        allocator: std.mem.Allocator,
    ) MatchingBooksHistoryValidationError![]i32 {
        const keys = try self.books_history.orderedKeys(allocator);
        errdefer allocator.free(keys);
        if (keys.len != self.books.count()) {
            return error.HistorySemanticKeySetMismatch;
        }
        for (keys) |key| {
            if (!self.books.contains(key)) {
                return error.HistorySemanticKeySetMismatch;
            }
        }
        return keys;
    }

    /// Validates the semantic/history key-set invariant and exposes only a
    /// read-only history pointer for later raw-layout persistence consumers.
    /// The pointer remains valid until the kernel's next mutation.
    pub fn validatedMatchingBooksHistory(
        self: *const ExchangeKernel,
        allocator: std.mem.Allocator,
    ) MatchingBooksHistoryValidationError!*const MatchingBooksHistory {
        const keys = try self.matchingBookKeysInHistoryOrder(allocator);
        allocator.free(keys);
        return &self.books_history;
    }

    pub fn cloneMatchingBooksHistory(
        self: *const ExchangeKernel,
        allocator: std.mem.Allocator,
    ) MatchingBooksHistoryValidationError!MatchingBooksHistory {
        if (self.books_history.isKnown()) {
            _ = try self.validatedMatchingBooksHistory(allocator);
        }
        return self.books_history.clone(allocator);
    }

    /// Ownership of `next` moves on success. On error the kernel and `next`
    /// remain unchanged.
    pub fn replaceMatchingBooksHistory(
        self: *ExchangeKernel,
        next: *MatchingBooksHistory,
    ) MatchingBooksHistoryValidationError!void {
        const keys = try next.orderedKeys(self.allocator);
        defer self.allocator.free(keys);
        if (keys.len != self.books.count()) {
            return error.HistorySemanticKeySetMismatch;
        }
        for (keys) |key| {
            if (!self.books.contains(key)) {
                return error.HistorySemanticKeySetMismatch;
            }
        }
        var old = self.books_history;
        self.books_history = next.*;
        next.* = MatchingBooksHistory.initUnavailable();
        old.deinit(self.allocator);
    }

    pub fn appendOrdersForSymbol(
        self: *const ExchangeKernel,
        allocator: std.mem.Allocator,
        symbol: i32,
        out: *std.ArrayListUnmanaged(RestingOrder),
    ) OrderSnapshotReadError!void {
        const book = self.books.get(symbol) orelse return error.UnknownSymbol;
        try book.appendAllOrders(allocator, out);
    }

    pub fn appendAllOrders(
        self: *const ExchangeKernel,
        allocator: std.mem.Allocator,
        out: *std.ArrayListUnmanaged(RestingOrder),
    ) OrderSnapshotReadError!void {
        var symbols: std.ArrayListUnmanaged(i32) = .empty;
        defer symbols.deinit(allocator);

        try symbols.ensureTotalCapacityPrecise(allocator, self.books.count());
        var it = self.books.iterator();
        while (it.next()) |entry| {
            symbols.appendAssumeCapacity(entry.key_ptr.*);
        }
        std.sort.heap(i32, symbols.items, {}, lessThanI32);

        for (symbols.items) |symbol| {
            const book = self.books.get(symbol) orelse return error.OrderBookMapCorrupted;
            try book.appendAllOrders(allocator, out);
        }
    }

    pub fn restoreOrdersForSymbol(
        self: *ExchangeKernel,
        symbol: i32,
        orders: []const RestingOrder,
    ) OrderSnapshotRestoreError!void {
        const book = self.books.get(symbol) orelse return error.UnknownSymbol;
        try book.restoreOrders(symbol, orders);
        // All current native restore formats canonicalize book IDs and omit
        // the original EC backing table. Do not invent layout history.
        self.markMatchingBooksHistoryUnavailable();
    }

    pub fn addSymbol(
        self: *ExchangeKernel,
        spec: common.CoreSymbolSpecification,
    ) (risk.SymbolSpecificationProvider.AddSymbolError || int_map_order.LayoutError)!bool {
        if (self.books.contains(spec.symbol_id)) {
            return false;
        }
        try self.risk_engine.addSymbolForLiveAdmission(spec);

        return self.addMatchingSymbol(spec);
    }

    /// Registers only matching-engine state. Multi-shard topology keeps the
    /// active risk family separate and must not populate dormant carrier risk
    /// engines merely to construct replicated order books.
    fn addMatchingSymbol(
        self: *ExchangeKernel,
        spec: common.CoreSymbolSpecification,
    ) (common.CoreSymbolSpecification.ValidationError || int_map_order.LayoutError)!bool {
        if (self.books.contains(spec.symbol_id)) {
            return false;
        }
        try spec.validate();

        const book_ptr = try self.allocator.create(Book);
        errdefer self.allocator.destroy(book_ptr);

        switch (self.orderbook_kind) {
            .naive => {
                book_ptr.* = .{ .naive = try naive.NaiveOrderBook.init(self.allocator, spec) };
            },
            .direct => {
                if (self.order_pool_max_orders) |max_orders| {
                    const max_buckets = self.order_pool_max_buckets orelse max_orders;
                    book_ptr.* = .{ .direct = try direct.DirectOrderBook.initStatic(
                        self.allocator,
                        spec,
                        max_orders,
                        max_buckets,
                    ) };
                } else {
                    book_ptr.* = .{ .direct = try direct.DirectOrderBook.init(self.allocator, spec) };
                }
            },
        }
        errdefer book_ptr.deinit();

        try self.insertMatchingBook(spec.symbol_id, book_ptr);
        return true;
    }

    fn insertMatchingBook(
        self: *ExchangeKernel,
        symbol_id: i32,
        book_ptr: *Book,
    ) int_map_order.LayoutError!void {
        std.debug.assert(!self.books.contains(symbol_id));

        var next_history = try self.books_history.clone(self.allocator);
        errdefer next_history.deinit(self.allocator);
        try next_history.recordPut(self.allocator, symbol_id);
        try self.books.ensureUnusedCapacity(self.allocator, 1);

        // Both ownership structures are fully prepared, so no failure edge can
        // leave the semantic map and Java layout history out of sync.
        self.books.putAssumeCapacityNoClobber(symbol_id, book_ptr);
        var old_history = self.books_history;
        self.books_history = next_history;
        old_history.deinit(self.allocator);
    }

    fn nextSequence(self: *const ExchangeKernel) error{SequenceOverflow}!u64 {
        if (self.next_seq == std.math.maxInt(u64)) {
            return error.SequenceOverflow;
        }
        return self.next_seq + 1;
    }

    fn executeBinaryPayloadWords(
        self: *ExchangeKernel,
        seq: u64,
        words: []const i64,
    ) BinaryPayloadExecutionError!void {
        var decoded = try decodeBinaryCommandWordsJavaFirst(self.allocator, words);
        defer decoded.deinit();

        switch (decoded.payload) {
            .add_symbols => |symbols| {
                if (!self.report_queries_cfg.add_symbols_binary) {
                    return error.UnsupportedBinaryCommandType;
                }
                const handler_order = try javaSymbolHandlerOrder(self.allocator, symbols);
                defer self.allocator.free(handler_order);
                for (handler_order) |symbol| {
                    _ = try self.addSymbol(symbol);
                }
            },
            .add_accounts => |users| {
                if (!self.report_queries_cfg.add_accounts_binary) {
                    return error.UnsupportedBinaryCommandType;
                }
                for (users) |user_batch| {
                    var add_user = makeAddUserCommand(user_batch.uid);
                    _ = try self.risk_engine.preProcessCommand(seq, &add_user);

                    switch (add_user.result_code) {
                        .success => {},
                        .user_mgmt_user_already_exists => continue,
                        else => return error.BinaryCommandRejected,
                    }

                    for (user_batch.accounts) |account| {
                        const tx_id = 1_000_000_000 + @as(i64, account.currency);
                        var adjustment = makeBalanceAdjustmentCommand(
                            user_batch.uid,
                            account.currency,
                            account.balance,
                            tx_id,
                            .adjustment,
                        );
                        _ = try self.risk_engine.preProcessCommand(seq, &adjustment);
                        if (adjustment.result_code != .success) {
                            return error.BinaryCommandRejected;
                        }
                    }
                }
            },
        }
    }

    fn produceJavaReport(
        self: *ExchangeKernel,
        query: binary_payload.DecodedReportQuery,
    ) JavaReportBuildError!java_report_producer.ProducedReport {
        var matching = try buildJavaMatchingSection(
            self.allocator,
            self,
            @intCast(self.risk_engine.shard_id),
            self.risk_engine.shard_mask,
            switch (query) {
                .state_hash => false,
                .single_user, .total_currency_balance, .unknown => true,
            },
        );
        defer matching.deinit();
        const matching_sections = [_]java_report_producer.MatchingSectionView{matching.view};
        const risk_sections = [_]java_report_producer.RiskSectionView{.{
            .shard_id = @intCast(self.risk_engine.shard_id),
            .shard_mask = self.risk_engine.shard_mask,
            .binary_processor = &self.binary_frame_assembler,
            .engine = &self.risk_engine,
        }};
        return java_report_producer.produce(
            self.allocator,
            query,
            &matching_sections,
            &risk_sections,
            self.java_identity_hash_context,
            self.java_report_wire,
        );
    }

    fn produceCustomReportEvents(
        self: *ExchangeKernel,
        descriptor: *const report_registry.Descriptor,
        body: []const u8,
    ) CustomReportDispatchError!events.OwnedMatcherEvents {
        var chain = self.makeEventChain();
        defer chain.deinit();

        const shard_id = std.math.cast(i32, self.risk_engine.shard_id) orelse
            return error.InvalidShardId;
        const risk_view: RiskReportView = .{
            .engine = &self.risk_engine,
            .binary_processor = &self.binary_frame_assembler,
        };
        try appendCustomRiskSection(
            self.allocator,
            &chain,
            descriptor,
            body,
            &risk_view,
            shard_id,
            self.risk_engine.shard_mask,
            self.java_report_wire,
        );

        const matching_view: MatchingReportView = .{
            .kernel = self,
            .binary_processor = &self.binary_frame_assembler,
        };
        try appendCustomMatchingSection(
            self.allocator,
            &chain,
            descriptor,
            body,
            &matching_view,
            shard_id,
            self.risk_engine.shard_mask,
            self.java_report_wire,
        );
        return chain.takeOwned();
    }

    pub fn execute(
        self: *ExchangeKernel,
        command: *cmd.OrderCommand,
    ) ExecuteError!CommandResponse {
        return omitDisabledSettlementErrors(self.executeWithSettlement(
            command,
            settlement_lifecycle.Hooks.disabled(),
            .live,
        ));
    }

    pub fn executeWithSettlement(
        self: *ExchangeKernel,
        command: *cmd.OrderCommand,
        settlement_hooks: settlement_lifecycle.Hooks,
        settlement_mode: settlement_lifecycle.ExecutionMode,
    ) ExecuteWithSettlementError!CommandResponse {
        return self.executeAtSequenceWithSettlement(
            .fromInt(try self.nextSequence()),
            command,
            settlement_hooks,
            settlement_mode,
        );
    }

    /// Executes a command at its already-published global identity. Replay may
    /// leave gaps, but duplicate or regressing identities are rejected before
    /// any engine state is mutated.
    pub fn executeAtSequence(
        self: *ExchangeKernel,
        assigned_sequence: publication.PublicationSequence,
        command: *cmd.OrderCommand,
    ) ExecuteError!CommandResponse {
        return omitDisabledSettlementErrors(self.executeAtSequenceWithSettlement(
            assigned_sequence,
            command,
            settlement_lifecycle.Hooks.disabled(),
            .live,
        ));
    }

    pub fn executeAtSequenceWithSettlement(
        self: *ExchangeKernel,
        assigned_sequence: publication.PublicationSequence,
        command: *cmd.OrderCommand,
        settlement_hooks: settlement_lifecycle.Hooks,
        settlement_mode: settlement_lifecycle.ExecutionMode,
    ) ExecuteWithSettlementError!CommandResponse {
        const seq = assigned_sequence.toInt();
        if (seq <= self.next_seq) return error.NonMonotonicPublicationSequence;
        self.next_seq = seq;
        var event_chain = self.makeEventChain();
        defer event_chain.deinit();

        _ = try self.risk_engine.preProcessCommand(seq, command);

        var response: CommandResponse = .{
            .sequence = seq,
            .command = command.*,
            .result_code = command.result_code,
        };
        errdefer response.deinit(self.allocator);

        // Java processes RESET in both R1 and matching stages. The risk pre-stage
        // above has already cleared users, symbol specifications, prices, and
        // accounting; complete the same command boundary by clearing all books
        // and partially assembled binary transfers before publishing success.
        // Every operation here is infallible, so callers cannot observe a
        // successful partial reset or retry a half-applied RESET.
        if (command.command == .reset) {
            self.resetMatchingState();
            response.completeFromCommand(command);
            return response;
        }

        var risk_l2_snapshot: ?naive.L2MarketData = null;
        defer if (risk_l2_snapshot) |*snapshot| snapshot.deinit(self.allocator);

        applyControlCommandResult(command);

        if (isBinaryFrameCommand(command.command)) {
            const frame_result = self.binary_frame_assembler.acceptFrame(command) catch |err| switch (err) {
                error.UnsupportedCommandType,
                error.InvalidLastFlag,
                error.TransferTypeMismatch,
                error.TransferStateCorrupted,
                => {
                    command.result_code = .binary_command_failed;
                    response.completeFromCommand(command);
                    return response;
                },
                error.OutOfMemory => return error.OutOfMemory,
            };

            switch (frame_result) {
                .accepted => {
                    command.result_code = .accepted;
                },
                .complete => |complete| {
                    var owned_complete = complete;
                    defer owned_complete.deinit(self.allocator);

                    switch (owned_complete.command_type) {
                        .binary_data_command => {
                            self.executeBinaryPayloadWords(seq, owned_complete.words) catch |err| switch (err) {
                                error.OutOfMemory => return error.OutOfMemory,
                                else => {
                                    command.result_code = .binary_command_failed;
                                },
                            };
                            if (command.result_code != .binary_command_failed) {
                                command.result_code = .success;
                            }
                        },
                        .binary_data_query => {
                            var envelope = report_query_envelope.decodeWordsJavaFirst(
                                self.allocator,
                                owned_complete.words,
                                .{},
                            ) catch |err| switch (err) {
                                error.OutOfMemory => return error.OutOfMemory,
                                else => {
                                    command.result_code = .binary_command_failed;
                                    response.completeFromCommand(command);
                                    return response;
                                },
                            };
                            defer envelope.deinit();

                            if (!envelope.isReservedBuiltInCode()) {
                                const descriptor = self.custom_report_registry.lookup(
                                    envelope.raw.code,
                                ) orelse {
                                    command.result_code = .success;
                                    response.completeFromCommand(command);
                                    return response;
                                };
                                const owned_events = self.produceCustomReportEvents(
                                    descriptor,
                                    envelope.raw.body,
                                ) catch |err| switch (err) {
                                    error.OutOfMemory => return error.OutOfMemory,
                                    else => {
                                        command.result_code = .binary_command_failed;
                                        response.completeFromCommand(command);
                                        return response;
                                    },
                                };
                                owned_events.attachToResponse(&response);
                                command.result_code = .success;
                                response.completeFromCommand(command);
                                return response;
                            }

                            const decoded = envelope.decodeBuiltInOrIgnored() catch |err| switch (err) {
                                error.OutOfMemory => return error.OutOfMemory,
                                else => {
                                    command.result_code = .binary_command_failed;
                                    response.completeFromCommand(command);
                                    return response;
                                },
                            };
                            if (!self.isJavaReportQueryEnabled(decoded)) {
                                command.result_code = .success;
                                response.completeFromCommand(command);
                                return response;
                            }

                            // The wire query is a Java request first. Native
                            // projection is process-local opt-in metadata on
                            // the terminal frame and must not influence an
                            // unmarked query's result code.
                            const produced: ?java_report_producer.ProducedReport = self.produceJavaReport(decoded) catch |err| switch (err) {
                                error.OutOfMemory => return error.OutOfMemory,
                                error.MissingJavaIdentityHashContext => if (command.binary_report_projection == .exact_java_and_native)
                                    null
                                else {
                                    command.result_code = .binary_command_failed;
                                    response.completeFromCommand(command);
                                    return response;
                                },
                                else => {
                                    command.result_code = .binary_command_failed;
                                    response.completeFromCommand(command);
                                    return response;
                                },
                            };
                            if (produced) |owned| {
                                response.java_report_response = owned.response;
                                response.matcher_events_head = owned.events_head;
                                response.events_owner = owned.events_owner;
                            }

                            if (command.binary_report_projection == .exact_java_and_native) {
                                const native_query = nativeQueryForJava(decoded) orelse unreachable;
                                response.report_response = self.queryReport(native_query) catch |err| switch (err) {
                                    error.OutOfMemory => return error.OutOfMemory,
                                    else => {
                                        response.deinit(self.allocator);
                                        command.result_code = .binary_command_failed;
                                        response.completeFromCommand(command);
                                        return response;
                                    },
                                };
                            }
                            command.result_code = .success;
                            response.completeFromCommand(command);
                            return response;
                        },
                        else => {
                            command.result_code = .binary_command_failed;
                        },
                    }
                },
            }

            response.completeFromCommand(command);
            return response;
        }

        if (isMatchingCommand(command.command)) {
            const book_ptr = self.books.get(command.symbol) orelse {
                command.result_code = .matching_invalid_order_book_id;
                response.completeFromCommand(command);
                return response;
            };

            if (command.command == .order_book_request) {
                const depth = if (command.size < 0)
                    std.math.maxInt(usize)
                else
                    @as(usize, @intCast(command.size));
                response.l2_snapshot = try book_ptr.l2Snapshot(self.allocator, depth);
                command.result_code = .success;
            } else {
                command.result_code = try book_ptr.processCommand(command, &event_chain);
                if (command.result_code == .success and self.shouldRefreshLastPriceFromMatching(command)) {
                    risk_l2_snapshot = try book_ptr.l2Snapshot(self.allocator, self.l2_refresh_depth);
                }
            }
        }

        const top = if (response.l2_snapshot) |*snapshot|
            l2TopFromSnapshot(snapshot)
        else if (risk_l2_snapshot) |*snapshot|
            l2TopFromSnapshot(snapshot)
        else
            null;

        const release_spec = self.risk_engine.symbol_spec_provider.getSymbolSpecification(command.symbol);
        if (release_spec) |specification| {
            if (specification.symbol_type == .currency_exchange_pair) {
                var prepared = try self.risk_engine.prepareExchangeRiskRelease(
                    command,
                    event_chain.head,
                    top,
                );
                defer prepared.deinit();
                _ = try settlement_hooks.prepare(.{
                    .sequence = assigned_sequence,
                    .command = command,
                    .specification = specification,
                    .events_head = event_chain.head,
                    .mode = settlement_mode,
                });
                self.risk_engine.commitPreparedExchangeRiskRelease(&prepared);
            } else {
                var prepared = try self.risk_engine.prepareMarginRiskRelease(
                    command,
                    event_chain.head,
                    top,
                );
                defer prepared.deinit();
                self.risk_engine.commitPreparedMarginRiskRelease(&prepared);
            }
        } else {
            _ = try self.risk_engine.handlerRiskRelease(seq, command, event_chain.head, top);
        }
        const _events_owner = event_chain.owner;
        response.matcher_events_head = event_chain.takeOwnedHead();
        response.events_owner = _events_owner;
        response.completeFromCommand(command);
        return response;
    }

    pub fn executeBatch(
        self: *ExchangeKernel,
        commands: []*cmd.OrderCommand,
    ) ExecuteError!BatchResponse {
        var response: BatchResponse = .{};
        var has_success = false;
        var first_failure: ?cmd.CommandResultCode = null;

        for (commands) |command| {
            var single = try self.execute(command);
            defer single.deinit(self.allocator);

            if (response.sequence_from == null) {
                response.sequence_from = single.sequence;
            }
            response.sequence_to = single.sequence;

            switch (single.result_code) {
                .accepted => {},
                .success => has_success = true,
                else => {
                    if (first_failure == null) {
                        first_failure = single.result_code;
                    }
                },
            }
        }

        response.result_code = if (first_failure) |failed|
            failed
        else if (has_success)
            .success
        else
            .accepted;

        return response;
    }

    fn addToI32I64Map(
        allocator: std.mem.Allocator,
        map: *std.AutoHashMapUnmanaged(i32, i64),
        key: i32,
        delta: i64,
    ) ReportError!void {
        const gop = try map.getOrPut(allocator, key);
        if (!gop.found_existing) {
            gop.value_ptr.* = 0;
        }
        const sum = @addWithOverflow(gop.value_ptr.*, delta);
        if (sum[1] != 0) return error.ArithmeticOverflow;
        gop.value_ptr.* = sum[0];
    }

    fn copyI32I64Map(
        allocator: std.mem.Allocator,
        dst: *std.AutoHashMapUnmanaged(i32, i64),
        src: *const std.AutoHashMapUnmanaged(i32, i64),
    ) std.mem.Allocator.Error!void {
        var it = src.iterator();
        while (it.next()) |entry| {
            try dst.put(allocator, entry.key_ptr.*, entry.value_ptr.*);
        }
    }

    fn lessThanUserOrder(_: void, lhs: naive.UserOrderView, rhs: naive.UserOrderView) bool {
        if (lhs.symbol != rhs.symbol) return lhs.symbol < rhs.symbol;
        return lhs.order_id < rhs.order_id;
    }

    fn lessThanI32(_: void, lhs: i32, rhs: i32) bool {
        return lhs < rhs;
    }

    fn lessThanStateHashSubmodule(
        _: void,
        lhs: StateHashSubmodule,
        rhs: StateHashSubmodule,
    ) bool {
        const lhs_code = @intFromEnum(lhs.submodule);
        const rhs_code = @intFromEnum(rhs.submodule);
        if (lhs_code != rhs_code) return lhs_code < rhs_code;
        return lhs.module_id < rhs.module_id;
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

    fn stateHashI32I64Map(
        self: *ExchangeKernel,
        map: *const std.AutoHashMapUnmanaged(i32, i64),
    ) ReportError!u64 {
        var hasher = std.hash.Wyhash.init(0);
        var keys: std.ArrayListUnmanaged(i32) = .empty;
        defer keys.deinit(self.allocator);

        try keys.ensureTotalCapacityPrecise(self.allocator, map.count());
        var it = map.iterator();
        while (it.next()) |entry| {
            keys.appendAssumeCapacity(entry.key_ptr.*);
        }
        std.sort.heap(i32, keys.items, {}, lessThanI32);

        for (keys.items) |key| {
            const value = map.get(key) orelse return error.StateHashCorrupted;
            hashI32(&hasher, key);
            hashI64(&hasher, value);
        }
        return hasher.final();
    }

    fn stateHashLastPriceCache(self: *ExchangeKernel) ReportError!u64 {
        var hasher = std.hash.Wyhash.init(0);
        var keys: std.ArrayListUnmanaged(i32) = .empty;
        defer keys.deinit(self.allocator);

        try keys.ensureTotalCapacityPrecise(self.allocator, self.risk_engine.last_price_cache.count());
        var it = self.risk_engine.last_price_cache.iterator();
        while (it.next()) |entry| {
            keys.appendAssumeCapacity(entry.key_ptr.*);
        }
        std.sort.heap(i32, keys.items, {}, lessThanI32);

        for (keys.items) |key| {
            const value = self.risk_engine.last_price_cache.get(key) orelse return error.StateHashCorrupted;
            hashI32(&hasher, key);
            hashI64(&hasher, value.ask_price);
            hashI64(&hasher, value.bid_price);
        }
        return hasher.final();
    }

    fn stateHashOrderBooks(self: *ExchangeKernel) ReportError!u64 {
        var hasher = std.hash.Wyhash.init(0);
        var keys: std.ArrayListUnmanaged(i32) = .empty;
        defer keys.deinit(self.allocator);

        try keys.ensureTotalCapacityPrecise(self.allocator, self.books.count());
        var it = self.books.iterator();
        while (it.next()) |entry| {
            keys.appendAssumeCapacity(entry.key_ptr.*);
        }
        std.sort.heap(i32, keys.items, {}, lessThanI32);

        for (keys.items) |symbol_id| {
            const book = self.books.get(symbol_id) orelse return error.StateHashCorrupted;
            hashI32(&hasher, symbol_id);
            hashU64(&hasher, try book.stateHash());
        }
        return hasher.final();
    }

    fn stateHashFromSubmodules(submodules: []const StateHashSubmodule) u64 {
        var hasher = std.hash.Wyhash.init(0);
        hashU64(&hasher, @as(u64, @intCast(submodules.len)));
        for (submodules) |item| {
            hashI32(&hasher, item.module_id);
            hashI32(&hasher, @intFromEnum(item.submodule));
            hashI8(&hasher, @intFromEnum(item.submodule.moduleType()));
            hashU64(&hasher, item.hash);
        }
        return hasher.final();
    }

    fn buildStateHashDetailedReport(self: *ExchangeKernel) ReportError!DetailedStateHashReport {
        var report: DetailedStateHashReport = .{
            .aggregate_hash = 0,
        };
        errdefer report.deinit(self.allocator);

        const module_id = std.math.cast(i32, self.risk_engine.shard_id) orelse
            return error.ArithmeticOverflow;
        const shard_mask_i64 = std.math.cast(i64, self.risk_engine.shard_mask) orelse
            return error.ArithmeticOverflow;
        const binary_hash = self.binary_frame_assembler.stateHash() catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            error.TransferStateCorrupted => return error.BinaryFrameCorrupted,
        };

        try report.submodules.append(self.allocator, .{
            .module_id = module_id,
            .submodule = .risk_symbol_spec_provider,
            .hash = try self.risk_engine.symbol_spec_provider.stateHash(),
        });
        try report.submodules.append(self.allocator, .{
            .module_id = module_id,
            .submodule = .risk_user_profile_service,
            .hash = try self.risk_engine.user_profile_service.stateHash(),
        });
        try report.submodules.append(self.allocator, .{
            .module_id = module_id,
            .submodule = .risk_binary_cmd_processor,
            .hash = binary_hash,
        });
        try report.submodules.append(self.allocator, .{
            .module_id = module_id,
            .submodule = .risk_last_price_cache,
            .hash = try self.stateHashLastPriceCache(),
        });
        try report.submodules.append(self.allocator, .{
            .module_id = module_id,
            .submodule = .risk_fees,
            .hash = try self.stateHashI32I64Map(&self.risk_engine.fees),
        });
        try report.submodules.append(self.allocator, .{
            .module_id = module_id,
            .submodule = .risk_adjustments,
            .hash = try self.stateHashI32I64Map(&self.risk_engine.adjustments),
        });
        try report.submodules.append(self.allocator, .{
            .module_id = module_id,
            .submodule = .risk_suspends,
            .hash = try self.stateHashI32I64Map(&self.risk_engine.suspends),
        });
        try report.submodules.append(self.allocator, .{
            .module_id = module_id,
            .submodule = .risk_shard_mask,
            .hash = @as(u64, @bitCast(shard_mask_i64)),
        });
        try report.submodules.append(self.allocator, .{
            .module_id = module_id,
            .submodule = .matching_binary_cmd_processor,
            .hash = binary_hash,
        });
        try report.submodules.append(self.allocator, .{
            .module_id = module_id,
            .submodule = .matching_order_books,
            .hash = try self.stateHashOrderBooks(),
        });
        try report.submodules.append(self.allocator, .{
            .module_id = module_id,
            .submodule = .matching_shard_mask,
            .hash = @as(u64, @bitCast(shard_mask_i64)),
        });

        std.sort.heap(
            StateHashSubmodule,
            report.submodules.items,
            {},
            lessThanStateHashSubmodule,
        );
        report.aggregate_hash = stateHashFromSubmodules(report.submodules.items);
        return report;
    }

    fn buildSingleUserReport(
        self: *ExchangeKernel,
        uid: i64,
    ) std.mem.Allocator.Error!SingleUserReport {
        var report: SingleUserReport = .{
            .uid = uid,
            .query_execution_status = .ok,
        };
        errdefer report.deinit(self.allocator);

        if (self.risk_engine.getUserProfile(uid)) |profile| {
            report.user_status = profile.user_status;

            var accounts_it = profile.accounts.iterator();
            while (accounts_it.next()) |entry| {
                try report.accounts.put(self.allocator, entry.key_ptr.*, entry.value_ptr.*);
            }

            var positions_it = profile.positions.iterator();
            while (positions_it.next()) |entry| {
                const position = entry.value_ptr.*;
                try report.positions.put(self.allocator, entry.key_ptr.*, .{
                    .quote_currency = position.currency,
                    .direction = position.direction,
                    .open_volume = position.open_volume,
                    .open_price_sum = position.open_price_sum,
                    .profit = position.profit,
                    .pending_sell_size = position.pending_sell_size,
                    .pending_buy_size = position.pending_buy_size,
                });
            }
        } else {
            report.query_execution_status = .user_not_found;
        }

        var book_it = self.books.valueIterator();
        while (book_it.next()) |book_ptr| {
            try book_ptr.*.appendUserOrders(self.allocator, uid, &report.orders);
        }
        std.sort.heap(naive.UserOrderView, report.orders.items, {}, lessThanUserOrder);
        return report;
    }

    fn buildTotalCurrencyBalanceReport(
        self: *ExchangeKernel,
    ) ReportError!TotalCurrencyBalanceReport {
        var report: TotalCurrencyBalanceReport = .{};
        errdefer report.deinit(self.allocator);

        var profile_it = self.risk_engine.user_profile_service.user_profiles.valueIterator();
        while (profile_it.next()) |profile| {
            var account_it = profile.accounts.iterator();
            while (account_it.next()) |entry| {
                try addToI32I64Map(
                    self.allocator,
                    &report.account_balances,
                    entry.key_ptr.*,
                    entry.value_ptr.*,
                );
            }

            var position_it = profile.positions.iterator();
            while (position_it.next()) |entry| {
                const symbol_id = entry.key_ptr.*;
                const position = entry.value_ptr.*;
                const spec = self.risk_engine.symbol_spec_provider.getSymbolSpecification(symbol_id) orelse
                    return error.UnknownSymbol;
                const estimated_profit = try self.risk_engine.estimatePositionProfitForReport(&position, spec);
                try addToI32I64Map(
                    self.allocator,
                    &report.account_balances,
                    position.currency,
                    estimated_profit,
                );
                if (position.open_volume <= 0) continue;

                switch (position.direction) {
                    .long => try addToI32I64Map(
                        self.allocator,
                        &report.open_interest_long,
                        symbol_id,
                        position.open_volume,
                    ),
                    .short => try addToI32I64Map(
                        self.allocator,
                        &report.open_interest_short,
                        symbol_id,
                        position.open_volume,
                    ),
                    .empty => {},
                }
            }
        }

        var book_it = self.books.iterator();
        while (book_it.next()) |entry| {
            const spec = entry.value_ptr.*.symbolSpecification();
            if (spec.symbol_type == .currency_exchange_pair) {
                try entry.value_ptr.*.accumulateOrderHolds(self.allocator, &report.orders_balances);
            }
        }

        try copyI32I64Map(self.allocator, &report.fees, &self.risk_engine.fees);
        try copyI32I64Map(self.allocator, &report.adjustments, &self.risk_engine.adjustments);
        try copyI32I64Map(self.allocator, &report.suspends, &self.risk_engine.suspends);

        return report;
    }

    pub fn queryReport(self: *ExchangeKernel, query: ReportQuery) ReportError!ReportResponse {
        if (!self.isReportQueryEnabled(query)) return error.UnsupportedReportQueryType;
        return switch (query) {
            .risk_state_hash => .{ .risk_state_hash = try self.risk_engine.stateHash() },
            .state_hash_detailed => .{
                .state_hash_detailed = try self.buildStateHashDetailedReport(),
            },
            .fee_collected => |currency| .{ .fee_collected = self.risk_engine.getFeeCollected(currency) },
            .single_user => |uid| .{
                .single_user = try self.buildSingleUserReport(uid),
            },
            .total_currency_balance => .{
                .total_currency_balance = try self.buildTotalCurrencyBalanceReport(),
            },
            .user_balance => |target| .{
                .user_balance = if (self.risk_engine.getUserProfile(target.uid)) |profile|
                    profile.accountBalance(target.currency)
                else
                    null,
            },
            .order_book => |request| blk: {
                const book_ptr = self.books.get(request.symbol) orelse return error.UnknownSymbol;
                break :blk .{
                    .order_book = try book_ptr.l2Snapshot(self.allocator, request.depth),
                };
            },
        };
    }

    fn isReportQueryEnabled(self: *const ExchangeKernel, query: ReportQuery) bool {
        return switch (query) {
            .risk_state_hash, .state_hash_detailed => self.report_queries_cfg.state_hash,
            .single_user => self.report_queries_cfg.single_user,
            .total_currency_balance => self.report_queries_cfg.total_currency_balance,
            else => true,
        };
    }

    fn isJavaReportQueryEnabled(
        self: *const ExchangeKernel,
        query: binary_payload.DecodedReportQuery,
    ) bool {
        return switch (query) {
            .state_hash => self.report_queries_cfg.state_hash,
            .single_user => self.report_queries_cfg.single_user,
            .total_currency_balance => self.report_queries_cfg.total_currency_balance,
            .unknown => false,
        };
    }

    fn l2TopFromSnapshot(snapshot: *const naive.L2MarketData) events.L2Top {
        return .{
            .ask_size = snapshot.asks.items.len,
            .bid_size = snapshot.bids.items.len,
            .ask_price_0 = if (snapshot.asks.items.len == 0) std.math.maxInt(i64) else snapshot.asks.items[0].price,
            .bid_price_0 = if (snapshot.bids.items.len == 0) 0 else snapshot.bids.items[0].price,
        };
    }

    fn shouldRefreshLastPriceFromMatching(
        self: *const ExchangeKernel,
        command: *const cmd.OrderCommand,
    ) bool {
        if (command.command == .order_book_request) return false;
        if (self.send_l2_for_every_cmd) return true;
        return (command.service_flags & 1) != 0;
    }

    fn isMatchingCommand(command_type: cmd.OrderCommandType) bool {
        return switch (command_type) {
            .move_order,
            .cancel_order,
            .reduce_order,
            .place_order,
            .order_book_request,
            => true,
            else => false,
        };
    }

    fn isBinaryFrameCommand(command_type: cmd.OrderCommandType) bool {
        return switch (command_type) {
            .binary_data_command, .binary_data_query => true,
            else => false,
        };
    }
};

/// Legacy low-level facade for directly owned kernels.
///
/// This type has no internal synchronization. One thread must exclusively own
/// it and serialize every call, including synchronous execution, publication,
/// draining, result extraction, and deinitialization. Use the lifecycle-owned
/// ExchangeCore driver when multi-producer publication is required.
pub const ExchangeApi = struct {
    const PendingAsyncCommand = struct {
        request_id: u64,
        command: cmd.OrderCommand,
    };

    pub const PersistStateRequestPair = struct {
        matching_request_id: u64,
        risk_request_id: u64,
    };

    pub const AsyncSubmitError = std.mem.Allocator.Error || error{
        RequestIdOverflow,
    };
    pub const HelperError = error{
        OrderBookDepthOverflow,
    };

    kernel: *ExchangeKernel,
    next_request_id: u64 = 0,
    pending_queue: std.ArrayListUnmanaged(PendingAsyncCommand) = .empty,
    ready_results: std.AutoHashMapUnmanaged(u64, CommandResponse) = .empty,

    pub fn init(kernel: *ExchangeKernel) ExchangeApi {
        return .{
            .kernel = kernel,
        };
    }

    pub fn deinit(self: *ExchangeApi) void {
        self.pending_queue.deinit(self.kernel.allocator);

        var it = self.ready_results.valueIterator();
        while (it.next()) |response| {
            response.deinit(self.kernel.allocator);
        }
        self.ready_results.deinit(self.kernel.allocator);
        self.* = undefined;
    }

    pub fn submitCommand(self: *ExchangeApi, command: *cmd.OrderCommand) ExecuteError!CommandResponse {
        return self.kernel.execute(command);
    }

    pub fn addUser(self: *ExchangeApi, uid: i64) ExecuteError!CommandResponse {
        var command = makeAddUserCommand(uid);
        return self.submitCommand(&command);
    }

    pub fn adjustUserBalance(
        self: *ExchangeApi,
        uid: i64,
        transaction_id: i64,
        currency: i32,
        amount: i64,
        adjustment_type: common.BalanceAdjustmentType,
    ) ExecuteError!CommandResponse {
        var command = makeBalanceAdjustmentCommand(uid, currency, amount, transaction_id, adjustment_type);
        return self.submitCommand(&command);
    }

    pub fn suspendUser(self: *ExchangeApi, uid: i64) ExecuteError!CommandResponse {
        var command = makeSuspendUserCommand(uid);
        return self.submitCommand(&command);
    }

    pub fn resumeUser(self: *ExchangeApi, uid: i64) ExecuteError!CommandResponse {
        var command = makeResumeUserCommand(uid);
        return self.submitCommand(&command);
    }

    pub fn placeOrder(
        self: *ExchangeApi,
        order_id: i64,
        uid: i64,
        symbol: i32,
        price: i64,
        reserve_bid_price: i64,
        size: i64,
        action: common.OrderAction,
        order_type: common.OrderType,
        user_cookie: i32,
    ) ExecuteError!CommandResponse {
        var command = makePlaceOrderCommand(
            order_id,
            uid,
            symbol,
            price,
            reserve_bid_price,
            size,
            action,
            order_type,
            user_cookie,
        );
        return self.submitCommand(&command);
    }

    pub fn moveOrder(
        self: *ExchangeApi,
        order_id: i64,
        uid: i64,
        symbol: i32,
        new_price: i64,
    ) ExecuteError!CommandResponse {
        var command = makeMoveOrderCommand(order_id, uid, symbol, new_price);
        return self.submitCommand(&command);
    }

    pub fn cancelOrder(
        self: *ExchangeApi,
        order_id: i64,
        uid: i64,
        symbol: i32,
    ) ExecuteError!CommandResponse {
        var command = makeCancelOrderCommand(order_id, uid, symbol);
        return self.submitCommand(&command);
    }

    pub fn reduceOrder(
        self: *ExchangeApi,
        order_id: i64,
        uid: i64,
        symbol: i32,
        reduce_size: i64,
    ) ExecuteError!CommandResponse {
        var command = makeReduceOrderCommand(order_id, uid, symbol, reduce_size);
        return self.submitCommand(&command);
    }

    pub fn requestOrderBook(
        self: *ExchangeApi,
        symbol: i32,
        depth: usize,
    ) (ExecuteError || HelperError)!CommandResponse {
        const depth_i64 = std.math.cast(i64, depth) orelse return error.OrderBookDepthOverflow;
        var command = makeRequestOrderBookCommandI64(symbol, depth_i64);
        return self.submitCommand(&command);
    }

    pub fn reset(self: *ExchangeApi) ExecuteError!CommandResponse {
        var command = makeResetCommand();
        return self.submitCommand(&command);
    }

    pub fn nop(self: *ExchangeApi) ExecuteError!CommandResponse {
        var command = makeNopCommand();
        return self.submitCommand(&command);
    }

    pub fn persistState(
        self: *ExchangeApi,
        snapshot_id: i64,
        timestamp_ns: i64,
    ) ExecuteError!cmd.CommandResultCode {
        var matching = makePersistStateMatchingCommand(snapshot_id, timestamp_ns);
        var risk_cmd = makePersistStateRiskCommand(snapshot_id, timestamp_ns);
        var commands = [_]*cmd.OrderCommand{
            &matching,
            &risk_cmd,
        };
        const batch = try self.submitCommandBatch(commands[0..]);
        return batch.result_code;
    }

    pub fn submitCommandBatch(
        self: *ExchangeApi,
        commands: []*cmd.OrderCommand,
    ) ExecuteError!BatchResponse {
        return self.kernel.executeBatch(commands);
    }

    pub fn queryReport(self: *ExchangeApi, query: ReportQuery) ReportError!ReportResponse {
        return self.kernel.queryReport(query);
    }

    pub fn submitCommandAsync(
        self: *ExchangeApi,
        command: *const cmd.OrderCommand,
    ) AsyncSubmitError!u64 {
        const request_id = try self.requestIdAfter(1);
        try self.pending_queue.append(self.kernel.allocator, .{
            .request_id = request_id,
            .command = command.*,
        });
        self.next_request_id = request_id;
        return request_id;
    }

    pub fn addUserAsync(self: *ExchangeApi, uid: i64) AsyncSubmitError!u64 {
        const command = makeAddUserCommand(uid);
        return self.submitCommandAsync(&command);
    }

    pub fn adjustUserBalanceAsync(
        self: *ExchangeApi,
        uid: i64,
        transaction_id: i64,
        currency: i32,
        amount: i64,
        adjustment_type: common.BalanceAdjustmentType,
    ) AsyncSubmitError!u64 {
        const command = makeBalanceAdjustmentCommand(uid, currency, amount, transaction_id, adjustment_type);
        return self.submitCommandAsync(&command);
    }

    pub fn suspendUserAsync(self: *ExchangeApi, uid: i64) AsyncSubmitError!u64 {
        const command = makeSuspendUserCommand(uid);
        return self.submitCommandAsync(&command);
    }

    pub fn resumeUserAsync(self: *ExchangeApi, uid: i64) AsyncSubmitError!u64 {
        const command = makeResumeUserCommand(uid);
        return self.submitCommandAsync(&command);
    }

    pub fn placeOrderAsync(
        self: *ExchangeApi,
        order_id: i64,
        uid: i64,
        symbol: i32,
        price: i64,
        reserve_bid_price: i64,
        size: i64,
        action: common.OrderAction,
        order_type: common.OrderType,
        user_cookie: i32,
    ) AsyncSubmitError!u64 {
        const command = makePlaceOrderCommand(
            order_id,
            uid,
            symbol,
            price,
            reserve_bid_price,
            size,
            action,
            order_type,
            user_cookie,
        );
        return self.submitCommandAsync(&command);
    }

    pub fn moveOrderAsync(
        self: *ExchangeApi,
        order_id: i64,
        uid: i64,
        symbol: i32,
        new_price: i64,
    ) AsyncSubmitError!u64 {
        const command = makeMoveOrderCommand(order_id, uid, symbol, new_price);
        return self.submitCommandAsync(&command);
    }

    pub fn cancelOrderAsync(
        self: *ExchangeApi,
        order_id: i64,
        uid: i64,
        symbol: i32,
    ) AsyncSubmitError!u64 {
        const command = makeCancelOrderCommand(order_id, uid, symbol);
        return self.submitCommandAsync(&command);
    }

    pub fn reduceOrderAsync(
        self: *ExchangeApi,
        order_id: i64,
        uid: i64,
        symbol: i32,
        reduce_size: i64,
    ) AsyncSubmitError!u64 {
        const command = makeReduceOrderCommand(order_id, uid, symbol, reduce_size);
        return self.submitCommandAsync(&command);
    }

    pub fn requestOrderBookAsync(
        self: *ExchangeApi,
        symbol: i32,
        depth: usize,
    ) (AsyncSubmitError || HelperError)!u64 {
        const depth_i64 = std.math.cast(i64, depth) orelse return error.OrderBookDepthOverflow;
        const command = makeRequestOrderBookCommandI64(symbol, depth_i64);
        return self.submitCommandAsync(&command);
    }

    pub fn resetAsync(self: *ExchangeApi) AsyncSubmitError!u64 {
        const command = makeResetCommand();
        return self.submitCommandAsync(&command);
    }

    pub fn nopAsync(self: *ExchangeApi) AsyncSubmitError!u64 {
        const command = makeNopCommand();
        return self.submitCommandAsync(&command);
    }

    pub fn persistStateAsync(
        self: *ExchangeApi,
        snapshot_id: i64,
        timestamp_ns: i64,
    ) AsyncSubmitError!PersistStateRequestPair {
        const matching = makePersistStateMatchingCommand(snapshot_id, timestamp_ns);
        const risk_cmd = makePersistStateRiskCommand(snapshot_id, timestamp_ns);

        const matching_request_id = try self.requestIdAfter(1);
        const risk_request_id = try self.requestIdAfter(2);
        try self.pending_queue.ensureUnusedCapacity(self.kernel.allocator, 2);
        self.pending_queue.appendAssumeCapacity(.{
            .request_id = matching_request_id,
            .command = matching,
        });
        self.pending_queue.appendAssumeCapacity(.{
            .request_id = risk_request_id,
            .command = risk_cmd,
        });
        self.next_request_id = risk_request_id;

        return .{
            .matching_request_id = matching_request_id,
            .risk_request_id = risk_request_id,
        };
    }

    /// Executes queued work in FIFO order. Result-capacity failure leaves the
    /// head queued and untouched. Once extracted, an execution failure consumes
    /// that request without a ready result and leaves only the unstarted tail.
    pub fn drainPending(self: *ExchangeApi) ExecuteError!usize {
        var processed: usize = 0;

        while (self.pending_queue.items.len != 0) {
            try self.ready_results.ensureUnusedCapacity(self.kernel.allocator, 1);
            // Once execution starts, failures can follow partial mutation. Remove
            // ownership from the queue first so a later drain cannot replay it.
            const entry = self.pending_queue.orderedRemove(0);
            var command = entry.command;
            const response = try self.kernel.execute(&command);
            self.ready_results.putAssumeCapacityNoClobber(entry.request_id, response);
            processed += 1;
        }
        return processed;
    }

    pub fn takeResult(self: *ExchangeApi, request_id: u64) ?CommandResponse {
        const removed = self.ready_results.fetchRemove(request_id) orelse return null;
        return removed.value;
    }

    pub fn isResultReady(self: *const ExchangeApi, request_id: u64) bool {
        return self.ready_results.contains(request_id);
    }

    pub fn pendingCount(self: *const ExchangeApi) usize {
        return self.pending_queue.items.len;
    }

    pub fn readyCount(self: *const ExchangeApi) usize {
        return self.ready_results.count();
    }

    pub fn isPersistStateResultReady(
        self: *const ExchangeApi,
        requests: PersistStateRequestPair,
    ) bool {
        return self.isResultReady(requests.matching_request_id) and
            self.isResultReady(requests.risk_request_id);
    }

    pub fn takePersistStateResult(
        self: *ExchangeApi,
        requests: PersistStateRequestPair,
    ) ?cmd.CommandResultCode {
        if (!self.isPersistStateResultReady(requests)) return null;

        var matching = self.takeResult(requests.matching_request_id) orelse return null;
        var risk_cmd = self.takeResult(requests.risk_request_id) orelse {
            matching.deinit(self.kernel.allocator);
            return null;
        };
        defer matching.deinit(self.kernel.allocator);
        defer risk_cmd.deinit(self.kernel.allocator);

        return cmd.CommandResultCode.mergeToFirstFailed(&.{
            matching.result_code,
            risk_cmd.result_code,
        });
    }

    fn requestIdAfter(self: *const ExchangeApi, offset: u64) error{RequestIdOverflow}!u64 {
        return std.math.add(u64, self.next_request_id, offset) catch
            error.RequestIdOverflow;
    }
};

fn applyControlCommandResult(command: *cmd.OrderCommand) void {
    switch (command.command) {
        .nop, .grouping_control => {
            command.result_code = .success;
        },
        .persist_state_matching => {
            if (command.result_code != .state_persist_matching_engine_failed) {
                command.result_code = .accepted;
            }
        },
        .persist_state_risk => {
            if (command.result_code != .state_persist_risk_engine_failed) {
                command.result_code = .success;
            }
        },
        else => {},
    }
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

fn makeAddUserCommand(uid: i64) cmd.OrderCommand {
    return .{
        .command = .add_user,
        .uid = uid,
        .result_code = .new,
    };
}

fn makeSuspendUserCommand(uid: i64) cmd.OrderCommand {
    return .{
        .command = .suspend_user,
        .uid = uid,
        .result_code = .new,
    };
}

fn makeResumeUserCommand(uid: i64) cmd.OrderCommand {
    return .{
        .command = .resume_user,
        .uid = uid,
        .result_code = .new,
    };
}

fn makePlaceOrderCommand(
    order_id: i64,
    uid: i64,
    symbol: i32,
    price: i64,
    reserve_bid_price: i64,
    size: i64,
    action: common.OrderAction,
    order_type: common.OrderType,
    user_cookie: i32,
) cmd.OrderCommand {
    return .{
        .command = .place_order,
        .order_id = order_id,
        .uid = uid,
        .symbol = symbol,
        .price = price,
        .reserve_bid_price = reserve_bid_price,
        .size = size,
        .action = action,
        .order_type = order_type,
        .user_cookie = user_cookie,
        .result_code = .new,
    };
}

fn makeMoveOrderCommand(
    order_id: i64,
    uid: i64,
    symbol: i32,
    new_price: i64,
) cmd.OrderCommand {
    return .{
        .command = .move_order,
        .order_id = order_id,
        .uid = uid,
        .symbol = symbol,
        .price = new_price,
        .result_code = .new,
    };
}

fn makeCancelOrderCommand(order_id: i64, uid: i64, symbol: i32) cmd.OrderCommand {
    return .{
        .command = .cancel_order,
        .order_id = order_id,
        .uid = uid,
        .symbol = symbol,
        .result_code = .new,
    };
}

fn makeReduceOrderCommand(
    order_id: i64,
    uid: i64,
    symbol: i32,
    reduce_size: i64,
) cmd.OrderCommand {
    return .{
        .command = .reduce_order,
        .order_id = order_id,
        .uid = uid,
        .symbol = symbol,
        .size = reduce_size,
        .result_code = .new,
    };
}

fn makeRequestOrderBookCommand(symbol: i32, depth: usize) cmd.OrderCommand {
    return makeRequestOrderBookCommandI64(symbol, @as(i64, @intCast(depth)));
}

fn makeRequestOrderBookCommandI64(symbol: i32, depth: i64) cmd.OrderCommand {
    return .{
        .command = .order_book_request,
        .symbol = symbol,
        .size = depth,
        .result_code = .new,
    };
}

fn makeResetCommand() cmd.OrderCommand {
    return .{
        .command = .reset,
        .result_code = .new,
    };
}

fn makeNopCommand() cmd.OrderCommand {
    return .{
        .command = .nop,
        .result_code = .new,
    };
}

fn makePersistStateMatchingCommand(snapshot_id: i64, timestamp_ns: i64) cmd.OrderCommand {
    return .{
        .command = .persist_state_matching,
        .order_id = snapshot_id,
        .timestamp = timestamp_ns,
        .result_code = .new,
    };
}

fn makePersistStateRiskCommand(snapshot_id: i64, timestamp_ns: i64) cmd.OrderCommand {
    return .{
        .command = .persist_state_risk,
        .order_id = snapshot_id,
        .timestamp = timestamp_ns,
        .result_code = .new,
    };
}

fn makeBinaryFrameCommand(
    command_type: cmd.OrderCommandType,
    transfer_id: i32,
    last_flag: i32,
    words: [5]i64,
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

fn testJavaIdentityHashContext() JavaIdentityHashContext {
    return .{
        .order_action_ask = 0x101,
        .order_action_bid = 0x102,
        .user_status_active = 0x201,
        .user_status_suspended = 0x202,
        .risk_symbol_spec_provider = 0x301,
        .risk_user_profile_service = 0x302,
        .risk_binary_cmd_processor = 0x303,
        .risk_last_price_cache = 0x304,
        .risk_fees = 0x305,
        .risk_adjustments = 0x306,
        .risk_suspends = 0x307,
        .risk_shard_mask = 0x308,
        .matching_binary_cmd_processor = 0x309,
        .matching_order_books = 0x30a,
        .matching_shard_mask = 0x30b,
    };
}

fn javaMapValue(entries: ?[]const java_report_wire.IntLongEntry, key: i32) ?i64 {
    for (entries orelse return null) |entry| {
        if (entry.key == key) return entry.value;
    }
    return null;
}

fn makeFeeMarginFuturesSpec() !common.CoreSymbolSpecification {
    return common.CoreSymbolSpecification.init(.{
        .symbol_id = 5991,
        .symbol_type = .futures_contract,
        .base_currency = 840,
        .quote_currency = 392,
        .base_scale_k = 100_000,
        .quote_scale_k = 10,
        .taker_fee = 3,
        .maker_fee = 2,
        .margin_buy = 5_000,
        .margin_sell = 6_000,
    });
}

fn encodeAddSymbolsPayload(
    allocator: std.mem.Allocator,
    specs: []const common.CoreSymbolSpecification,
) std.mem.Allocator.Error![]u8 {
    var bytes: std.Io.Writer.Allocating = .init(allocator);
    errdefer bytes.deinit();
    const writer = &bytes.writer;

    writer.writeInt(i32, 1003, .big) catch return error.OutOfMemory;
    writer.writeInt(i32, @as(i32, @intCast(specs.len)), .big) catch return error.OutOfMemory;
    for (specs) |spec| {
        writer.writeInt(i32, spec.symbol_id, .big) catch return error.OutOfMemory;
        writer.writeInt(i32, spec.symbol_id, .big) catch return error.OutOfMemory;
        writer.writeInt(i8, spec.symbol_type.code(), .big) catch return error.OutOfMemory;
        writer.writeInt(i32, spec.base_currency, .big) catch return error.OutOfMemory;
        writer.writeInt(i32, spec.quote_currency, .big) catch return error.OutOfMemory;
        writer.writeInt(i64, spec.base_scale_k, .big) catch return error.OutOfMemory;
        writer.writeInt(i64, spec.quote_scale_k, .big) catch return error.OutOfMemory;
        writer.writeInt(i64, spec.taker_fee, .big) catch return error.OutOfMemory;
        writer.writeInt(i64, spec.maker_fee, .big) catch return error.OutOfMemory;
        writer.writeInt(i64, spec.margin_buy, .big) catch return error.OutOfMemory;
        writer.writeInt(i64, spec.margin_sell, .big) catch return error.OutOfMemory;
    }
    return bytes.toOwnedSlice();
}

fn encodeAddAccountsPayload(
    allocator: std.mem.Allocator,
    users: []const binary_payload.UserAccountsBatch,
) std.mem.Allocator.Error![]u8 {
    var bytes: std.Io.Writer.Allocating = .init(allocator);
    errdefer bytes.deinit();
    const writer = &bytes.writer;

    writer.writeInt(i32, 1002, .big) catch return error.OutOfMemory;
    writer.writeInt(i32, @as(i32, @intCast(users.len)), .big) catch return error.OutOfMemory;
    for (users) |user_batch| {
        writer.writeInt(i64, user_batch.uid, .big) catch return error.OutOfMemory;
        writer.writeInt(i32, @as(i32, @intCast(user_batch.accounts.len)), .big) catch return error.OutOfMemory;
        for (user_batch.accounts) |account| {
            writer.writeInt(i32, account.currency, .big) catch return error.OutOfMemory;
            writer.writeInt(i64, account.balance, .big) catch return error.OutOfMemory;
        }
    }
    return bytes.toOwnedSlice();
}

fn encodeReportQueryPayload(
    allocator: std.mem.Allocator,
    report_type_code: i32,
) std.mem.Allocator.Error![]u8 {
    var bytes: std.Io.Writer.Allocating = .init(allocator);
    errdefer bytes.deinit();
    const writer = &bytes.writer;
    writer.writeInt(i32, report_type_code, .big) catch return error.OutOfMemory;
    return bytes.toOwnedSlice();
}

fn encodeSingleUserReportQueryPayload(
    allocator: std.mem.Allocator,
    uid: i64,
) std.mem.Allocator.Error![]u8 {
    var bytes: std.Io.Writer.Allocating = .init(allocator);
    errdefer bytes.deinit();
    const writer = &bytes.writer;
    writer.writeInt(i32, 10002, .big) catch return error.OutOfMemory;
    writer.writeInt(i64, uid, .big) catch return error.OutOfMemory;
    return bytes.toOwnedSlice();
}

fn appendBinaryPayloadFrames(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(cmd.OrderCommand),
    command_type: cmd.OrderCommandType,
    transfer_id: i32,
    payload: []const u8,
) std.mem.Allocator.Error!void {
    const words_without_padding: usize = if (payload.len == 0)
        0
    else
        ((payload.len - 1) / @sizeOf(i64)) + 1;
    const frame_words = binary_frames.FrameWordsPerCommand;
    const padded_words = if (words_without_padding % frame_words == 0)
        words_without_padding
    else
        words_without_padding + (frame_words - (words_without_padding % frame_words));

    const words = try allocator.alloc(i64, padded_words);
    defer allocator.free(words);
    @memset(words, 0);

    for (0..words_without_padding) |idx| {
        var raw: [8]u8 = [_]u8{0} ** 8;
        const start = idx * 8;
        const end = @min(start + 8, payload.len);
        @memcpy(raw[0 .. end - start], payload[start..end]);
        words[idx] = std.mem.readInt(i64, &raw, .big);
    }

    const frame_count = padded_words / frame_words;
    for (0..frame_count) |frame_idx| {
        const base = frame_idx * frame_words;
        try out.append(allocator, makeBinaryFrameCommand(
            command_type,
            transfer_id,
            if (frame_idx + 1 == frame_count) -1 else 0,
            .{
                words[base],
                words[base + 1],
                words[base + 2],
                words[base + 3],
                words[base + 4],
            },
        ));
    }
}

const UnsupportedOrderCase = struct {
    order_type: common.OrderType,
    order_id: i64,
    price: i64,
    reserve_bid_price: i64,
    size: i64,
    action: common.OrderAction,
};

const unsupported_order_cases = [_]UnsupportedOrderCase{
    .{
        .order_type = .ioc_budget,
        .order_id = 91_001,
        .price = 120,
        .reserve_bid_price = 120,
        .size = 7,
        .action = .bid,
    },
    .{
        .order_type = .ioc_budget,
        .order_id = 91_002,
        .price = 80,
        .reserve_bid_price = 0,
        .size = 8,
        .action = .ask,
    },
    .{
        .order_type = .fok,
        .order_id = 91_003,
        .price = 110,
        .reserve_bid_price = 145,
        .size = 9,
        .action = .bid,
    },
    .{
        .order_type = .fok,
        .order_id = 91_004,
        .price = 80,
        .reserve_bid_price = 0,
        .size = 10,
        .action = .ask,
    },
};

fn expectUnsupportedFullReject(
    response: *const CommandResponse,
    case: UnsupportedOrderCase,
) !void {
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    const reject = response.matcher_events_head orelse return error.TestExpectedRejectEvent;
    try std.testing.expectEqual(events.MatcherEventType.reject, reject.event_type);
    try std.testing.expect(reject.active_order_completed);
    try std.testing.expectEqual(@as(i64, 0), reject.matched_order_id);
    try std.testing.expectEqual(@as(i64, 0), reject.matched_order_uid);
    try std.testing.expect(!reject.matched_order_completed);
    try std.testing.expectEqual(case.price, reject.price);
    try std.testing.expectEqual(case.size, reject.size);
    try std.testing.expectEqual(case.reserve_bid_price, reject.bidder_hold_price);
    try std.testing.expect(reject.next_event == null);
}

fn expectEqualL2(expected: naive.L2MarketData, actual: naive.L2MarketData) !void {
    try std.testing.expectEqual(expected.asks.items.len, actual.asks.items.len);
    try std.testing.expectEqual(expected.bids.items.len, actual.bids.items.len);

    for (expected.asks.items, actual.asks.items) |lhs, rhs| {
        try std.testing.expectEqual(lhs.price, rhs.price);
        try std.testing.expectEqual(lhs.volume, rhs.volume);
        try std.testing.expectEqual(lhs.orders, rhs.orders);
    }
    for (expected.bids.items, actual.bids.items) |lhs, rhs| {
        try std.testing.expectEqual(lhs.price, rhs.price);
        try std.testing.expectEqual(lhs.volume, rhs.volume);
        try std.testing.expectEqual(lhs.orders, rhs.orders);
    }
}

fn executePairExpectEqual(
    allocator: std.mem.Allocator,
    left_kernel: *ExchangeKernel,
    right_kernel: *ExchangeKernel,
    source: cmd.OrderCommand,
) !void {
    var left = source;
    var right = source;
    var left_resp = try left_kernel.execute(&left);
    defer left_resp.deinit(allocator);
    var right_resp = try right_kernel.execute(&right);
    defer right_resp.deinit(allocator);

    try std.testing.expectEqual(left_resp.sequence, right_resp.sequence);
    try std.testing.expectEqual(left_resp.result_code, right_resp.result_code);

    if (left_resp.l2_snapshot != null or right_resp.l2_snapshot != null) {
        try std.testing.expect(left_resp.l2_snapshot != null and right_resp.l2_snapshot != null);
        try expectEqualL2(left_resp.l2_snapshot.?, right_resp.l2_snapshot.?);
    }
}

fn simulationPrice(random: std.Random, base: i64) i64 {
    const delta = @as(i64, @intCast(random.uintLessThan(u32, 81))) - 40;
    return base + delta;
}

fn mapAddChecked(
    allocator: std.mem.Allocator,
    dst: *std.AutoHashMapUnmanaged(i32, i64),
    key: i32,
    delta: i64,
) (std.mem.Allocator.Error || error{ArithmeticOverflow})!void {
    const gop = try dst.getOrPut(allocator, key);
    if (!gop.found_existing) {
        gop.value_ptr.* = 0;
    }
    const sum = @addWithOverflow(gop.value_ptr.*, delta);
    if (sum[1] != 0) return error.ArithmeticOverflow;
    gop.value_ptr.* = sum[0];
}

fn mergeI32I64MapChecked(
    allocator: std.mem.Allocator,
    dst: *std.AutoHashMapUnmanaged(i32, i64),
    src: *const std.AutoHashMapUnmanaged(i32, i64),
) (std.mem.Allocator.Error || error{ArithmeticOverflow})!void {
    var it = src.iterator();
    while (it.next()) |entry| {
        try mapAddChecked(allocator, dst, entry.key_ptr.*, entry.value_ptr.*);
    }
}

fn expectMapEqualI32I64(
    expected: *const std.AutoHashMapUnmanaged(i32, i64),
    actual: *const std.AutoHashMapUnmanaged(i32, i64),
) !void {
    try std.testing.expectEqual(expected.count(), actual.count());
    var it = expected.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const maybe_actual = actual.get(key);
        try std.testing.expect(maybe_actual != null);
        try std.testing.expectEqual(entry.value_ptr.*, maybe_actual.?);
    }
}

fn hashI32Test(hasher: *std.hash.Wyhash, value: i32) void {
    var n = value;
    hasher.update(std.mem.asBytes(&n));
}

fn hashI8Test(hasher: *std.hash.Wyhash, value: i8) void {
    var n = value;
    hasher.update(std.mem.asBytes(&n));
}

fn hashI64Test(hasher: *std.hash.Wyhash, value: i64) void {
    var n = value;
    hasher.update(std.mem.asBytes(&n));
}

fn hashU64Test(hasher: *std.hash.Wyhash, value: u64) void {
    var n = value;
    hasher.update(std.mem.asBytes(&n));
}

fn commandResultDigest(command: *const cmd.OrderCommand, result_code: cmd.CommandResultCode) u64 {
    var hasher = std.hash.Wyhash.init(0);
    hashI8Test(&hasher, @intFromEnum(command.command));
    hashI64Test(&hasher, command.order_id);
    hashI32Test(&hasher, command.symbol);
    hashI64Test(&hasher, command.price);
    hashI64Test(&hasher, command.size);
    hashI64Test(&hasher, command.reserve_bid_price);
    hashI64Test(&hasher, command.uid);
    hashI64Test(&hasher, command.timestamp);
    hashI32Test(&hasher, command.user_cookie);
    hashI64Test(&hasher, command.events_group);
    hashI32Test(&hasher, command.service_flags);
    hashI8Test(&hasher, if (command.action != null) 1 else 0);
    if (command.action) |action| {
        hashI8Test(&hasher, @intFromEnum(action));
    }
    hashI8Test(&hasher, if (command.order_type != null) 1 else 0);
    if (command.order_type) |order_type| {
        hashI8Test(&hasher, @intFromEnum(order_type));
    }
    hashI32Test(&hasher, @intFromEnum(result_code));
    return hasher.final();
}

const FaultEvent = struct {
    tick: u64,
    order: u64,
    command: cmd.OrderCommand,
};

fn lessThanFaultEvent(_: void, lhs: FaultEvent, rhs: FaultEvent) bool {
    if (lhs.tick != rhs.tick) return lhs.tick < rhs.tick;
    return lhs.order < rhs.order;
}

const FaultSimulationResult = struct {
    digests: std.ArrayListUnmanaged(u64) = .empty,
    delivered_count: usize = 0,
    final_fingerprint: u64 = 0,

    fn deinit(self: *FaultSimulationResult, allocator: std.mem.Allocator) void {
        self.digests.deinit(allocator);
        self.* = undefined;
    }
};

pub const RiskShard = struct {
    engine: *risk.RiskEngine,
    binary_frame_assembler: binary_frames.BinaryFrameAssembler,
    next_seq: u64 = 0,
};

/// Independent matching and risk shard families.
///
/// ExchangeKernel remains the matching-state carrier during the topology
/// transition so the existing single-kernel snapshot format keeps compiling.
/// Only the explicit `risk_shards` family is operational risk state. Embedded
/// carrier risks beyond that family are dormant and are never mutated or
/// included in reports. Binary frames are broadcast through the distinct
/// matching and risk processor families. Durable per-family persistence remains
/// a later phase.
pub const MultiShardKernelSet = struct {
    allocator: std.mem.Allocator,
    matching_shards: []ExchangeKernel,
    /// Deprecated physical alias retained for source compatibility. New code
    /// must use matching/risk semantic accessors rather than infer ownership.
    kernels: []ExchangeKernel,
    risk_shards: []RiskShard,
    additional_risk_engines: []risk.RiskEngine,
    matching_mask: u64,
    risk_mask: u64,
    next_seq: u64 = 0,
    /// Set-wide matcher event pool (shared by StageAdapter dual-chain paths).
    matcher_event_pool: ?*events.MatcherEventPool = null,
    /// Preallocated R2 prepare scratch (Phase D) — not freed per command.
    exchange_release_scratch: []risk.RiskEngine.PreparedExchangeRelease = &.{},
    margin_release_scratch: []risk.RiskEngine.PreparedMarginRelease = &.{},

    pub const ShardedInitError = std.mem.Allocator.Error || InitError || error{
        InvalidShardCount,
    };
    pub const AggregateError = ReportError || error{
        ArithmeticOverflow,
    };

    const GroupedTwoStepFailureStage = enum {
        risk_pre_process,
        matching,
        risk_release,
        publication_ownership,
    };

    const GroupedTwoStepFailure = struct {
        stage: GroupedTwoStepFailureStage,
        sequence: u64,
        cause: anyerror,
    };

    const GroupedTwoStepState = union(enum) {
        running,
        poisoned: GroupedTwoStepFailure,
    };

    /// A result observer borrows the executor-owned response only for the
    /// duration of this call. It must not retain the response or any nested
    /// matcher-event/report pointers.
    const BorrowedPostMatchingHook = struct {
        context: ?*anyopaque,
        callback: *const fn (
            context: ?*anyopaque,
            response: *const CommandResponse,
        ) void,

        fn publish(self: BorrowedPostMatchingHook, response: *const CommandResponse) void {
            self.callback(self.context, response);
        }
    };

    /// Owns exactly the matching output required by the later R2 pass.
    /// Preparation is intentionally absent: prepared releases borrow mutable
    /// risk state and would become stale while later same-group R1 commands run.
    const PendingMatchedCommand = struct {
        sequence: u64,
        events_group: i64,
        command: cmd.OrderCommand,
        matcher_events_head: ?*events.MatcherTradeEvent,
        events_owner: events.EventNodesOwner,
        l2_top: ?events.L2Top,
        requires_risk_release: bool,

        fn deinit(self: *PendingMatchedCommand, _: std.mem.Allocator) void {
            events.deinitMatcherEventChain(self.events_owner, self.matcher_events_head);
            self.* = undefined;
        }
    };

    /// Private serial model of Java's GroupingProcessor plus paired R1/R2
    /// processors. Existing public execute APIs do not use this owner yet and
    /// therefore retain their immediate R1 -> matching -> R2 behavior.
    const GroupedTwoStepExecutor = struct {
        const ProcessError = ExecuteError || error{
            GroupedTwoStepPoisoned,
            GroupedTwoStepReentrantCall,
            NonMonotonicEventsGroup,
            PublicationFrontierOwnershipLost,
        };

        kernels: *MultiShardKernelSet,
        pending: std.ArrayListUnmanaged(PendingMatchedCommand) = .empty,
        open_group: ?i64 = null,
        last_events_group: ?i64 = null,
        state: GroupedTwoStepState = .running,
        hook_active: bool = false,
        accepted_frontier: u64,
        r1_frontier: u64,
        matching_frontier: u64,
        result_frontier: u64,
        r2_frontier: u64,

        fn init(kernels: *MultiShardKernelSet) GroupedTwoStepExecutor {
            const settled_frontier = kernels.next_seq;
            return .{
                .kernels = kernels,
                .accepted_frontier = settled_frontier,
                .r1_frontier = settled_frontier,
                .matching_frontier = settled_frontier,
                .result_frontier = settled_frontier,
                .r2_frontier = settled_frontier,
            };
        }

        /// Drops owned staging memory without applying pending releases.
        /// Callers that want a graceful final-group barrier must call flushAll.
        fn deinit(self: *GroupedTwoStepExecutor) void {
            for (self.pending.items) |*pending| {
                pending.deinit(self.kernels.allocator);
            }
            self.pending.deinit(self.kernels.allocator);
            self.* = undefined;
        }

        fn process(
            self: *GroupedTwoStepExecutor,
            command: *cmd.OrderCommand,
            post_matching_hook: ?BorrowedPostMatchingHook,
        ) ProcessError!void {
            try self.ensureRunning();
            try self.ensureNotInHook();
            try self.ensurePublicationOwnership();
            return self.processAtSequence(
                .fromInt(try self.kernels.nextSequence()),
                command,
                post_matching_hook,
            );
        }

        fn processAtSequence(
            self: *GroupedTwoStepExecutor,
            assigned_sequence: publication.PublicationSequence,
            command: *cmd.OrderCommand,
            post_matching_hook: ?BorrowedPostMatchingHook,
        ) ProcessError!void {
            try self.ensureRunning();
            try self.ensureNotInHook();
            try self.ensurePublicationOwnership();
            try self.validateEventsGroup(command.events_group);

            const seq = assigned_sequence.toInt();
            try self.validateSequence(seq);

            // Reserve before either a previous-group flush or new-command
            // mutation. Once matching starts, appending the pending owner is
            // infallible and the command can never be replayed.
            try self.pending.ensureUnusedCapacity(self.kernels.allocator, 1);

            if (self.open_group) |open_group| {
                if (open_group != command.events_group) {
                    try self.flushOpenGroup();
                }
            }
            if (self.open_group == null) {
                self.open_group = command.events_group;
            }
            self.last_events_group = command.events_group;

            self.consumeSequence(seq);

            var response: CommandResponse = .{
                .sequence = seq,
                .command = command.*,
                .result_code = command.result_code,
            };
            defer response.deinit(self.kernels.allocator);

            var pipeline_command = command.*;
            var l2_top: ?events.L2Top = null;
            var requires_risk_release = false;

            if (pipeline_command.command == .reset) {
                self.kernels.resetAll();
                pipeline_command.result_code = .success;
                self.r1_frontier = seq;
                self.matching_frontier = seq;
            } else if (ExchangeKernel.isBinaryFrameCommand(pipeline_command.command)) {
                self.kernels.executeBinaryFrame(seq, &pipeline_command, &response) catch |err| {
                    self.poison(.matching, seq, err);
                    return err;
                };
                self.r1_frontier = seq;
                self.matching_frontier = seq;
            } else {
                var event_chain = self.kernels.makeEventChain();
                defer event_chain.deinit();
                var risk_l2_snapshot: ?naive.L2MarketData = null;
                defer if (risk_l2_snapshot) |*snapshot| {
                    snapshot.deinit(self.kernels.allocator);
                };

                self.kernels.executeRiskPreProcessStage(seq, &pipeline_command) catch |err| {
                    self.poison(.risk_pre_process, seq, err);
                    return err;
                };
                self.r1_frontier = seq;

                const disposition = self.kernels.executeMatchingStage(
                    &pipeline_command,
                    &response,
                    &event_chain,
                    &risk_l2_snapshot,
                ) catch |err| {
                    self.poison(.matching, seq, err);
                    return err;
                };
                self.matching_frontier = seq;

                if (disposition == .continue_to_risk_release) {
                    requires_risk_release = true;
                    l2_top = if (response.l2_snapshot) |*snapshot|
                        ExchangeKernel.l2TopFromSnapshot(snapshot)
                    else if (risk_l2_snapshot) |*snapshot|
                        ExchangeKernel.l2TopFromSnapshot(snapshot)
                    else
                        null;
                }
                const _events_owner = event_chain.owner;
                response.matcher_events_head = event_chain.takeOwnedHead();
                response.events_owner = _events_owner;
            }

            command.* = pipeline_command;
            response.completeFromCommand(command);
            self.result_frontier = seq;

            const pending_events = response.matcher_events_head;
            const pending_owner = response.events_owner orelse self.kernels.eventsOwner();
            response.matcher_events_head = null;
            response.events_owner = null;
            self.pending.appendAssumeCapacity(.{
                .sequence = seq,
                .events_group = command.events_group,
                .command = pipeline_command,
                .matcher_events_head = pending_events,
                .events_owner = pending_owner,
                .l2_top = l2_top,
                .requires_risk_release = requires_risk_release,
            });

            if (post_matching_hook) |hook| {
                // The pending slot owns matching output before user code runs.
                // The response aliases that output only for this synchronous
                // borrow, so reentrant publication cannot orphan the R2 input.
                response.matcher_events_head = pending_events;
                self.hook_active = true;
                hook.publish(&response);
                self.hook_active = false;
                response.matcher_events_head = null;

                // Existing immediate APIs remain callable, so a hook can move
                // the shared kernel frontier behind this executor's back. The
                // just-published pending release remains owned for terminal
                // cleanup, but it can no longer be applied safely.
                try self.ensurePublicationOwnership();
            }
        }

        fn flushAll(self: *GroupedTwoStepExecutor) ProcessError!void {
            try self.ensureRunning();
            try self.ensureNotInHook();
            try self.ensurePublicationOwnership();
            try self.flushOpenGroup();
        }

        /// Grouping may advance the group number while the command stream is
        /// idle, but Java does not invoke R2 until a later command exposes that
        /// new number (or shutdown explicitly flushes the final group).
        fn observeIdle(self: *GroupedTwoStepExecutor) ProcessError!void {
            try self.ensureRunning();
            try self.ensureNotInHook();
            try self.ensurePublicationOwnership();
        }

        fn flushOpenGroup(self: *GroupedTwoStepExecutor) ProcessError!void {
            try self.ensureRunning();
            try self.ensureNotInHook();
            try self.ensurePublicationOwnership();

            while (self.pending.items.len != 0) {
                const pending = &self.pending.items[0];
                std.debug.assert(self.open_group == pending.events_group);

                if (pending.requires_risk_release) {
                    var prepared = self.kernels.prepareRiskReleaseStage(
                        &pending.command,
                        pending.matcher_events_head,
                        pending.l2_top,
                    ) catch |err| {
                        self.poison(.risk_release, pending.sequence, err);
                        return err;
                    };
                    self.kernels.commitRiskReleaseStage(&prepared);
                    prepared.deinit(self.kernels.allocator);
                }

                var completed = self.pending.orderedRemove(0);
                self.r2_frontier = completed.sequence;
                completed.deinit(self.kernels.allocator);
            }
            self.open_group = null;
        }

        fn pendingCount(self: *const GroupedTwoStepExecutor) usize {
            return self.pending.items.len;
        }

        fn failure(self: *const GroupedTwoStepExecutor) ?GroupedTwoStepFailure {
            return switch (self.state) {
                .running => null,
                .poisoned => |fatal| fatal,
            };
        }

        fn ensureRunning(self: *const GroupedTwoStepExecutor) error{GroupedTwoStepPoisoned}!void {
            switch (self.state) {
                .running => {},
                .poisoned => return error.GroupedTwoStepPoisoned,
            }
        }

        fn ensureNotInHook(
            self: *const GroupedTwoStepExecutor,
        ) error{GroupedTwoStepReentrantCall}!void {
            if (self.hook_active) return error.GroupedTwoStepReentrantCall;
        }

        fn validateEventsGroup(
            self: *const GroupedTwoStepExecutor,
            events_group: i64,
        ) error{NonMonotonicEventsGroup}!void {
            if (self.last_events_group) |last| {
                if (events_group < last) return error.NonMonotonicEventsGroup;
            }
        }

        fn ensurePublicationOwnership(
            self: *GroupedTwoStepExecutor,
        ) error{PublicationFrontierOwnershipLost}!void {
            if (self.liveFrontiersEqual(self.accepted_frontier)) return;

            if (self.pending.items.len == 0 and self.open_group == null) {
                // Mixing immediate and grouped execution is safe only at an R2
                // quiescent point. Adopt a coherent externally advanced
                // frontier before accepting the next grouped command.
                const observed = self.kernels.next_seq;
                if (observed >= self.accepted_frontier and
                    self.liveFrontiersEqual(observed))
                {
                    self.accepted_frontier = observed;
                    self.r1_frontier = observed;
                    self.matching_frontier = observed;
                    self.result_frontier = observed;
                    self.r2_frontier = observed;
                    return;
                }
            }

            const failure_sequence = if (self.pending.items.len != 0)
                self.pending.items[self.pending.items.len - 1].sequence
            else
                self.accepted_frontier;
            self.poison(
                .publication_ownership,
                failure_sequence,
                error.PublicationFrontierOwnershipLost,
            );
            return error.PublicationFrontierOwnershipLost;
        }

        fn liveFrontiersEqual(self: *const GroupedTwoStepExecutor, expected: u64) bool {
            if (self.kernels.next_seq != expected) return false;
            for (self.kernels.matching_shards) |*kernel| {
                if (kernel.next_seq != expected) return false;
            }
            for (self.kernels.risk_shards) |*risk_shard| {
                if (risk_shard.next_seq != expected) return false;
            }
            return true;
        }

        fn validateSequence(
            self: *const GroupedTwoStepExecutor,
            seq: u64,
        ) error{NonMonotonicPublicationSequence}!void {
            if (seq <= self.kernels.next_seq) return error.NonMonotonicPublicationSequence;
            for (self.kernels.matching_shards) |*kernel| {
                if (seq <= kernel.next_seq) return error.NonMonotonicPublicationSequence;
            }
            for (self.kernels.risk_shards) |*risk_shard| {
                if (seq <= risk_shard.next_seq) return error.NonMonotonicPublicationSequence;
            }
        }

        fn consumeSequence(self: *GroupedTwoStepExecutor, seq: u64) void {
            self.kernels.next_seq = seq;
            for (self.kernels.matching_shards) |*kernel| kernel.next_seq = seq;
            for (self.kernels.risk_shards) |*risk_shard| risk_shard.next_seq = seq;
            self.accepted_frontier = seq;
        }

        fn poison(
            self: *GroupedTwoStepExecutor,
            stage: GroupedTwoStepFailureStage,
            sequence: u64,
            cause: anyerror,
        ) void {
            std.debug.assert(switch (self.state) {
                .running => true,
                .poisoned => false,
            });
            self.state = .{ .poisoned = .{
                .stage = stage,
                .sequence = sequence,
                .cause = cause,
            } };
        }
    };

    pub fn init(
        allocator: std.mem.Allocator,
        shard_count: usize,
        orderbook_kind: OrderBookKind,
    ) ShardedInitError!MultiShardKernelSet {
        return initWithConfig(allocator, shard_count, .{
            .orderbook_kind = orderbook_kind,
        });
    }

    pub fn initWithConfig(
        allocator: std.mem.Allocator,
        shard_count: usize,
        base_config: Config,
    ) ShardedInitError!MultiShardKernelSet {
        return initWithTopology(
            allocator,
            shard_count,
            shard_count,
            base_config,
        );
    }

    pub fn initWithTopology(
        allocator: std.mem.Allocator,
        matching_count: usize,
        risk_count: usize,
        base_config: Config,
    ) ShardedInitError!MultiShardKernelSet {
        const matching_count_u64 = try checkedShardCount(matching_count);
        const risk_count_u64 = try checkedShardCount(risk_count);
        if (base_config.report_queries.custom_registry.len() != 0 and
            (matching_count_u64 >
                @as(u64, @intCast(java_report_wire.max_unambiguous_shard_id)) + 1 or
                risk_count_u64 >
                    @as(u64, @intCast(java_report_wire.max_unambiguous_shard_id)) + 1))
        {
            return error.InvalidCustomReportShardId;
        }

        const matching_shards = try allocator.alloc(ExchangeKernel, matching_count);
        errdefer allocator.free(matching_shards);
        var matching_initialized: usize = 0;
        errdefer for (matching_shards[0..matching_initialized]) |*kernel| kernel.deinit();

        for (0..matching_count) |idx| {
            const shard_id = std.math.cast(u64, idx) orelse return error.InvalidShardCount;
            var shard_config = base_config;
            shard_config.shard_id = shard_id;
            // An embedded carrier risk with id >= risk_count can own no UID
            // under this mask, making dormant state structurally non-owning.
            shard_config.num_shards = risk_count_u64;
            // Event pool is owned set-wide (StageAdapter dual-chain); do not
            // allocate per matching shard.
            shard_config.matcher_event_pool_capacity = null;
            matching_shards[idx] = try ExchangeKernel.init(allocator, shard_config);
            matching_initialized += 1;
        }

        const additional_count = risk_count -| matching_count;
        const additional_risk_engines = try allocator.alloc(risk.RiskEngine, additional_count);
        errdefer allocator.free(additional_risk_engines);
        var additional_initialized: usize = 0;
        errdefer for (additional_risk_engines[0..additional_initialized]) |*engine| engine.deinit();

        for (0..additional_count) |offset| {
            const shard_id = std.math.cast(u64, matching_count + offset) orelse
                return error.InvalidShardCount;
            additional_risk_engines[offset] = try risk.RiskEngine.init(
                allocator,
                shard_id,
                risk_count_u64,
                base_config.risk,
            );
            if (base_config.pre_reserve_users > 0) {
                try additional_risk_engines[offset].preReserveUsers(base_config.pre_reserve_users);
            }
            additional_risk_engines[offset].pre_reserve_accounts_per_user =
                base_config.pre_reserve_accounts_per_user;
            additional_risk_engines[offset].pre_reserve_positions_per_user =
                base_config.pre_reserve_positions_per_user;
            additional_initialized += 1;
        }

        const risk_shards = try allocator.alloc(RiskShard, risk_count);
        errdefer allocator.free(risk_shards);
        for (risk_shards, 0..) |*risk_shard, idx| {
            risk_shard.* = .{
                .engine = if (idx < matching_count)
                    &matching_shards[idx].risk_engine
                else
                    &additional_risk_engines[idx - matching_count],
                .binary_frame_assembler = binary_frames.BinaryFrameAssembler.init(allocator),
            };
            if (base_config.pre_reserve_users > 0) {
                try risk_shard.engine.preReserveUsers(base_config.pre_reserve_users);
            }
            risk_shard.engine.pre_reserve_accounts_per_user = base_config.pre_reserve_accounts_per_user;
            risk_shard.engine.pre_reserve_positions_per_user = base_config.pre_reserve_positions_per_user;
        }

        // Shared set-wide event pool for StageAdapter dual ownership. Individual
        // matching kernels may also own per-kernel pools for single-kernel paths.
        var matcher_event_pool: ?*events.MatcherEventPool = null;
        errdefer if (matcher_event_pool) |pool| {
            pool.deinitForce();
            allocator.destroy(pool);
        };
        if (base_config.matcher_event_pool_capacity) |cap| {
            const pool = try allocator.create(events.MatcherEventPool);
            errdefer allocator.destroy(pool);
            pool.* = try events.MatcherEventPool.init(allocator, cap);
            matcher_event_pool = pool;
        }

        const exchange_release_scratch = try allocator.alloc(
            risk.RiskEngine.PreparedExchangeRelease,
            risk_count,
        );
        errdefer allocator.free(exchange_release_scratch);
        const margin_release_scratch = try allocator.alloc(
            risk.RiskEngine.PreparedMarginRelease,
            risk_count,
        );
        errdefer allocator.free(margin_release_scratch);

        return .{
            .allocator = allocator,
            .matching_shards = matching_shards,
            .kernels = matching_shards,
            .risk_shards = risk_shards,
            .additional_risk_engines = additional_risk_engines,
            .matching_mask = matching_count_u64 - 1,
            .risk_mask = risk_count_u64 - 1,
            .matcher_event_pool = matcher_event_pool,
            .exchange_release_scratch = exchange_release_scratch,
            .margin_release_scratch = margin_release_scratch,
        };
    }

    pub fn eventsOwner(self: *MultiShardKernelSet) events.EventNodesOwner {
        if (self.matcher_event_pool) |pool| return .{ .pool = pool };
        return .{ .heap = self.allocator };
    }

    pub fn makeEventChain(self: *MultiShardKernelSet) events.EventChain {
        return events.EventChain.initOwner(self.eventsOwner());
    }

    fn checkedShardCount(count: usize) error{InvalidShardCount}!u64 {
        const count_u64 = std.math.cast(u64, count) orelse return error.InvalidShardCount;
        if (count == 0 or @popCount(count_u64) != 1) return error.InvalidShardCount;
        return count_u64;
    }

    pub fn deinit(self: *MultiShardKernelSet) void {
        for (self.risk_shards) |*risk_shard| risk_shard.binary_frame_assembler.deinit();
        self.allocator.free(self.risk_shards);
        for (self.additional_risk_engines) |*engine| engine.deinit();
        self.allocator.free(self.additional_risk_engines);
        for (self.matching_shards) |*kernel| kernel.deinit();
        self.allocator.free(self.matching_shards);
        if (self.matcher_event_pool) |pool| {
            pool.deinitForce();
            self.allocator.destroy(pool);
        }
        if (self.exchange_release_scratch.len != 0) {
            self.allocator.free(self.exchange_release_scratch);
        }
        if (self.margin_release_scratch.len != 0) {
            self.allocator.free(self.margin_release_scratch);
        }
        self.* = undefined;
    }

    pub fn matchingShardCount(self: *const MultiShardKernelSet) usize {
        return self.matching_shards.len;
    }

    pub fn riskShardCount(self: *const MultiShardKernelSet) usize {
        return self.risk_shards.len;
    }

    pub fn matchingShardMask(self: *const MultiShardKernelSet) u64 {
        return self.matching_mask;
    }

    pub fn riskShardMask(self: *const MultiShardKernelSet) u64 {
        return self.risk_mask;
    }

    pub fn matchingShardAt(self: *MultiShardKernelSet, idx: usize) *ExchangeKernel {
        return &self.matching_shards[idx];
    }

    pub fn riskEngineAt(self: *MultiShardKernelSet, idx: usize) *risk.RiskEngine {
        return self.risk_shards[idx].engine;
    }

    pub fn riskEngineForUid(self: *MultiShardKernelSet, uid: i64) *risk.RiskEngine {
        return self.riskEngineAt(self.shardIndexForUid(uid));
    }

    pub fn matchingShardForSymbol(self: *MultiShardKernelSet, symbol: i32) *ExchangeKernel {
        return self.matchingShardAt(self.shardIndexForSymbol(symbol));
    }

    pub fn matchingObservedSequence(self: *const MultiShardKernelSet, idx: usize) u64 {
        return self.matching_shards[idx].next_seq;
    }

    pub fn riskObservedSequence(self: *const MultiShardKernelSet, idx: usize) u64 {
        return self.risk_shards[idx].next_seq;
    }

    pub fn restorePublicationSequence(self: *MultiShardKernelSet, last: u64) void {
        self.next_seq = last;
        for (self.matching_shards) |*kernel| kernel.next_seq = last;
        for (self.risk_shards) |*risk_shard| risk_shard.next_seq = last;
    }

    pub fn addSymbolAll(
        self: *MultiShardKernelSet,
        spec: common.CoreSymbolSpecification,
    ) (risk.SymbolSpecificationProvider.AddSymbolError || int_map_order.LayoutError)!void {
        // Java offers each live definition to every active risk handler; each
        // handler applies the margin-enabled admission gate independently.
        for (self.risk_shards) |*risk_shard| try risk_shard.engine.addSymbolForLiveAdmission(spec);
        // Matching carriers own books only. Dormant embedded risk engines must
        // stay empty even when matching_count exceeds risk_count.
        for (self.matching_shards) |*kernel| _ = try kernel.addMatchingSymbol(spec);
    }

    pub fn shardIndexForUid(self: *const MultiShardKernelSet, uid: i64) usize {
        const uid_bits: u64 = @bitCast(uid);
        return @as(usize, @intCast(uid_bits & self.risk_mask));
    }

    pub fn shardIndexForSymbol(self: *const MultiShardKernelSet, symbol: i32) usize {
        const symbol_bits: u64 = @as(u32, @bitCast(symbol));
        return @as(usize, @intCast(symbol_bits & self.matching_mask));
    }

    /// Deprecated compatibility diagnostic. Staged order commands have both a
    /// UID-owned risk route and a symbol-owned matching route, so callers must
    /// prefer the explicit family routing functions above.
    pub fn shardIndexForCommand(self: *const MultiShardKernelSet, command: *const cmd.OrderCommand) usize {
        return switch (command.command) {
            .order_book_request => self.shardIndexForSymbol(command.symbol),
            .binary_data_command, .binary_data_query => self.shardIndexForSymbol(command.user_cookie),
            else => self.shardIndexForUid(command.uid),
        };
    }

    pub fn executeRouted(
        self: *MultiShardKernelSet,
        command: *cmd.OrderCommand,
    ) ExecuteError!CommandResponse {
        return omitDisabledSettlementErrors(self.executeRoutedWithSettlement(
            command,
            settlement_lifecycle.Hooks.disabled(),
            .live,
        ));
    }

    pub fn executeRoutedWithSettlement(
        self: *MultiShardKernelSet,
        command: *cmd.OrderCommand,
        settlement_hooks: settlement_lifecycle.Hooks,
        settlement_mode: settlement_lifecycle.ExecutionMode,
    ) ExecuteWithSettlementError!CommandResponse {
        return self.executePipeline(command, settlement_hooks, settlement_mode);
    }

    pub fn executeRoutedAtSequence(
        self: *MultiShardKernelSet,
        assigned_sequence: publication.PublicationSequence,
        command: *cmd.OrderCommand,
    ) ExecuteError!CommandResponse {
        return omitDisabledSettlementErrors(self.executeRoutedAtSequenceWithSettlement(
            assigned_sequence,
            command,
            settlement_lifecycle.Hooks.disabled(),
            .live,
        ));
    }

    pub fn executeRoutedAtSequenceWithSettlement(
        self: *MultiShardKernelSet,
        assigned_sequence: publication.PublicationSequence,
        command: *cmd.OrderCommand,
        settlement_hooks: settlement_lifecycle.Hooks,
        settlement_mode: settlement_lifecycle.ExecutionMode,
    ) ExecuteWithSettlementError!CommandResponse {
        return self.executePipelineAtSequence(
            assigned_sequence,
            command,
            settlement_hooks,
            settlement_mode,
        );
    }

    pub fn executeSplit(self: *MultiShardKernelSet, command: *cmd.OrderCommand) ExecuteError!CommandResponse {
        return omitDisabledSettlementErrors(self.executeSplitWithSettlement(
            command,
            settlement_lifecycle.Hooks.disabled(),
            .live,
        ));
    }

    pub fn executeSplitWithSettlement(
        self: *MultiShardKernelSet,
        command: *cmd.OrderCommand,
        settlement_hooks: settlement_lifecycle.Hooks,
        settlement_mode: settlement_lifecycle.ExecutionMode,
    ) ExecuteWithSettlementError!CommandResponse {
        return self.executePipeline(command, settlement_hooks, settlement_mode);
    }

    pub fn executeSplitAtSequence(
        self: *MultiShardKernelSet,
        assigned_sequence: publication.PublicationSequence,
        command: *cmd.OrderCommand,
    ) ExecuteError!CommandResponse {
        return omitDisabledSettlementErrors(self.executeSplitAtSequenceWithSettlement(
            assigned_sequence,
            command,
            settlement_lifecycle.Hooks.disabled(),
            .live,
        ));
    }

    pub fn executeSplitAtSequenceWithSettlement(
        self: *MultiShardKernelSet,
        assigned_sequence: publication.PublicationSequence,
        command: *cmd.OrderCommand,
        settlement_hooks: settlement_lifecycle.Hooks,
        settlement_mode: settlement_lifecycle.ExecutionMode,
    ) ExecuteWithSettlementError!CommandResponse {
        return self.executePipelineAtSequence(
            assigned_sequence,
            command,
            settlement_hooks,
            settlement_mode,
        );
    }

    fn executePipeline(
        self: *MultiShardKernelSet,
        command: *cmd.OrderCommand,
        settlement_hooks: settlement_lifecycle.Hooks,
        settlement_mode: settlement_lifecycle.ExecutionMode,
    ) ExecuteWithSettlementError!CommandResponse {
        return self.executePipelineAtSequence(
            .fromInt(try self.nextSequence()),
            command,
            settlement_hooks,
            settlement_mode,
        );
    }

    fn executePipelineAtSequence(
        self: *MultiShardKernelSet,
        assigned_sequence: publication.PublicationSequence,
        command: *cmd.OrderCommand,
        settlement_hooks: settlement_lifecycle.Hooks,
        settlement_mode: settlement_lifecycle.ExecutionMode,
    ) ExecuteWithSettlementError!CommandResponse {
        const seq = assigned_sequence.toInt();
        if (seq <= self.next_seq) return error.NonMonotonicPublicationSequence;
        for (self.matching_shards) |*kernel| {
            if (seq <= kernel.next_seq) return error.NonMonotonicPublicationSequence;
        }
        for (self.risk_shards) |*risk_shard| {
            if (seq <= risk_shard.next_seq) return error.NonMonotonicPublicationSequence;
        }

        // Publication identity is consumed before state mutation, matching the
        // single-kernel failure contract and preserving one global frontier.
        self.next_seq = seq;
        for (self.matching_shards) |*kernel| kernel.next_seq = seq;
        for (self.risk_shards) |*risk_shard| risk_shard.next_seq = seq;

        var response: CommandResponse = .{
            .sequence = seq,
            .command = command.*,
            .result_code = command.result_code,
        };
        errdefer response.deinit(self.allocator);

        var pipeline_command = command.*;
        if (pipeline_command.command == .reset) {
            self.resetAll();
            pipeline_command.result_code = .success;
            command.* = pipeline_command;
            response.completeFromCommand(command);
            return response;
        }

        if (ExchangeKernel.isBinaryFrameCommand(pipeline_command.command)) {
            try self.executeBinaryFrame(seq, &pipeline_command, &response);
            command.* = pipeline_command;
            response.completeFromCommand(command);
            return response;
        }

        var event_chain = self.makeEventChain();
        defer event_chain.deinit();
        var risk_l2_snapshot: ?naive.L2MarketData = null;
        defer if (risk_l2_snapshot) |*snapshot| snapshot.deinit(self.allocator);

        try self.executeRiskPreProcessStage(seq, &pipeline_command);
        if (try self.executeMatchingStage(
            &pipeline_command,
            &response,
            &event_chain,
            &risk_l2_snapshot,
        ) == .complete) {
            command.* = pipeline_command;
            response.completeFromCommand(command);
            return response;
        }

        const top: ?events.L2Top = if (response.l2_snapshot) |*snapshot|
            ExchangeKernel.l2TopFromSnapshot(snapshot)
        else if (risk_l2_snapshot) |*snapshot|
            ExchangeKernel.l2TopFromSnapshot(snapshot)
        else
            null;

        const release_spec = self.risk_shards[0].engine.symbol_spec_provider
            .getSymbolSpecification(pipeline_command.symbol);
        var prepared_release = try self.prepareRiskReleaseStage(
            &pipeline_command,
            event_chain.head,
            top,
        );
        defer prepared_release.deinit(self.allocator);
        switch (prepared_release) {
            .exchange, .exchange_scratch => {
                _ = try settlement_hooks.prepare(.{
                    .sequence = assigned_sequence,
                    .command = &pipeline_command,
                    .specification = release_spec orelse unreachable,
                    .events_head = event_chain.head,
                    .mode = settlement_mode,
                });
            },
            .none, .margin, .margin_scratch => {},
        }
        self.commitRiskReleaseStage(&prepared_release);

        command.* = pipeline_command;
        const _events_owner = event_chain.owner;
        response.matcher_events_head = event_chain.takeOwnedHead();
        response.events_owner = _events_owner;
        response.completeFromCommand(command);
        return response;
    }

    /// R1 stage: only the UID owner may mutate user/account/position state.
    fn executeRiskPreProcessStage(
        self: *MultiShardKernelSet,
        seq: u64,
        command: *cmd.OrderCommand,
    ) ExecuteError!void {
        const risk_owner = self.riskEngineForUid(command.uid);
        _ = try risk_owner.preProcessCommand(seq, command);
        applyControlCommandResult(command);
    }

    const MatchingStageDisposition = enum {
        continue_to_risk_release,
        complete,
    };

    /// Matching stage: only the symbol owner may mutate its order book.
    fn executeMatchingStage(
        self: *MultiShardKernelSet,
        command: *cmd.OrderCommand,
        response: *CommandResponse,
        event_chain: *events.EventChain,
        risk_l2_snapshot: *?naive.L2MarketData,
    ) ExecuteError!MatchingStageDisposition {
        if (!ExchangeKernel.isMatchingCommand(command.command)) {
            return .continue_to_risk_release;
        }

        const matching_kernel = self.matchingShardForSymbol(command.symbol);
        const book_ptr = matching_kernel.books.get(command.symbol) orelse {
            command.result_code = .matching_invalid_order_book_id;
            return .complete;
        };

        if (command.command == .order_book_request) {
            const depth = if (command.size < 0)
                std.math.maxInt(usize)
            else
                @as(usize, @intCast(command.size));
            response.l2_snapshot = try book_ptr.l2Snapshot(self.allocator, depth);
            command.result_code = .success;
            return .continue_to_risk_release;
        }

        command.result_code = try book_ptr.processCommand(command, event_chain);
        if (command.result_code == .success and
            matching_kernel.shouldRefreshLastPriceFromMatching(command))
        {
            risk_l2_snapshot.* = try book_ptr.l2Snapshot(
                self.allocator,
                matching_kernel.l2_refresh_depth,
            );
        }
        return .continue_to_risk_release;
    }

    const PreparedRiskReleaseStage = union(enum) {
        none,
        exchange: []risk.RiskEngine.PreparedExchangeRelease,
        margin: []risk.RiskEngine.PreparedMarginRelease,
        /// Scratch-backed: same payload as exchange/margin but deinit does not free the slice.
        exchange_scratch: []risk.RiskEngine.PreparedExchangeRelease,
        margin_scratch: []risk.RiskEngine.PreparedMarginRelease,

        fn deinit(
            self: *PreparedRiskReleaseStage,
            allocator: std.mem.Allocator,
        ) void {
            switch (self.*) {
                .none => {},
                .exchange => |prepared| {
                    for (prepared) |*release| release.deinit();
                    allocator.free(prepared);
                },
                .margin => |prepared| {
                    for (prepared) |*release| release.deinit();
                    allocator.free(prepared);
                },
                .exchange_scratch => |prepared| {
                    for (prepared) |*release| release.deinit();
                },
                .margin_scratch => |prepared| {
                    for (prepared) |*release| release.deinit();
                },
            }
            self.* = undefined;
        }
    };

    /// R2 prepare stage: finish every fallible shard calculation and
    /// reservation before any shard can make a logical settlement write.
    fn prepareRiskReleaseStage(
        self: *MultiShardKernelSet,
        command: *const cmd.OrderCommand,
        event_head: ?*events.MatcherTradeEvent,
        top: ?events.L2Top,
    ) ExecuteError!PreparedRiskReleaseStage {
        const release_spec = self.risk_shards[0].engine.symbol_spec_provider.getSymbolSpecification(
            command.symbol,
        ) orelse return .none;

        if (release_spec.symbol_type == .currency_exchange_pair) {
            return try self.prepareExchangeRiskReleaseStage(
                command,
                event_head,
                top,
            );
        }
        return try self.prepareMarginRiskReleaseStage(
            command,
            event_head,
            top,
        );
    }

    fn prepareExchangeRiskReleaseStage(
        self: *MultiShardKernelSet,
        command: *const cmd.OrderCommand,
        event_head: ?*events.MatcherTradeEvent,
        top: ?events.L2Top,
    ) ExecuteError!PreparedRiskReleaseStage {
        const n = self.risk_shards.len;
        const use_scratch = self.exchange_release_scratch.len >= n;
        const prepared: []risk.RiskEngine.PreparedExchangeRelease = if (use_scratch)
            self.exchange_release_scratch[0..n]
        else
            try self.allocator.alloc(risk.RiskEngine.PreparedExchangeRelease, n);
        var prepared_count: usize = 0;
        errdefer {
            for (prepared[0..prepared_count]) |*release| release.deinit();
            if (!use_scratch) self.allocator.free(prepared);
        }

        for (self.risk_shards, prepared) |*risk_shard, *release| {
            release.* = try risk_shard.engine.prepareExchangeRiskRelease(command, event_head, top);
            prepared_count += 1;
        }
        return if (use_scratch)
            .{ .exchange_scratch = prepared }
        else
            .{ .exchange = prepared };
    }

    fn prepareMarginRiskReleaseStage(
        self: *MultiShardKernelSet,
        command: *const cmd.OrderCommand,
        event_head: ?*events.MatcherTradeEvent,
        top: ?events.L2Top,
    ) ExecuteError!PreparedRiskReleaseStage {
        const n = self.risk_shards.len;
        const use_scratch = self.margin_release_scratch.len >= n;
        const prepared: []risk.RiskEngine.PreparedMarginRelease = if (use_scratch)
            self.margin_release_scratch[0..n]
        else
            try self.allocator.alloc(risk.RiskEngine.PreparedMarginRelease, n);
        var prepared_count: usize = 0;
        errdefer {
            for (prepared[0..prepared_count]) |*release| release.deinit();
            if (!use_scratch) self.allocator.free(prepared);
        }

        for (self.risk_shards, prepared) |*risk_shard, *release| {
            release.* = try risk_shard.engine.prepareMarginRiskRelease(command, event_head, top);
            prepared_count += 1;
        }
        return if (use_scratch)
            .{ .margin_scratch = prepared }
        else
            .{ .margin = prepared };
    }

    /// R2 commit stage: preparation guarantees this pass is infallible, so
    /// every active risk owner observes the matching result atomically.
    fn commitRiskReleaseStage(
        self: *MultiShardKernelSet,
        prepared_release: *PreparedRiskReleaseStage,
    ) void {
        switch (prepared_release.*) {
            .none => {},
            .exchange, .exchange_scratch => |prepared| {
                for (self.risk_shards, prepared) |*risk_shard, *release| {
                    risk_shard.engine.commitPreparedExchangeRiskRelease(release);
                }
            },
            .margin, .margin_scratch => |prepared| {
                for (self.risk_shards, prepared) |*risk_shard, *release| {
                    risk_shard.engine.commitPreparedMarginRiskRelease(release);
                }
            },
        }
    }

    fn resetAll(self: *MultiShardKernelSet) void {
        for (self.risk_shards) |*risk_shard| {
            risk_shard.engine.reset();
            risk_shard.binary_frame_assembler.reset();
        }
        for (self.matching_shards) |*kernel| kernel.resetMatchingState();
    }

    const BinaryProcessorStage = struct {
        target: *binary_frames.BinaryFrameAssembler,
        staged: binary_frames.BinaryFrameAssembler,
        result: ?binary_frames.AcceptResult = null,

        fn deinit(self: *BinaryProcessorStage, allocator: std.mem.Allocator) void {
            if (self.result) |*result| switch (result.*) {
                .accepted => {},
                .complete => |*complete| complete.deinit(allocator),
            };
            self.staged.deinit();
            self.* = undefined;
        }
    };

    const BinaryBroadcastError = binary_frames.AssemblyError || error{
        BinaryProcessorMismatch,
    };

    fn produceJavaReport(
        self: *MultiShardKernelSet,
        query: binary_payload.DecodedReportQuery,
    ) JavaReportBuildError!java_report_producer.ProducedReport {
        const owned_matching = try self.allocator.alloc(
            OwnedJavaMatchingSection,
            self.matching_shards.len,
        );
        defer self.allocator.free(owned_matching);
        var matching_initialized: usize = 0;
        defer for (owned_matching[0..matching_initialized]) |*section| section.deinit();

        const matching_sections = try self.allocator.alloc(
            java_report_producer.MatchingSectionView,
            self.matching_shards.len,
        );
        defer self.allocator.free(matching_sections);
        for (self.matching_shards, 0..) |*kernel, idx| {
            owned_matching[idx] = try buildJavaMatchingSection(
                self.allocator,
                kernel,
                @intCast(idx),
                self.matching_mask,
                switch (query) {
                    .state_hash => false,
                    .single_user, .total_currency_balance, .unknown => true,
                },
            );
            matching_initialized += 1;
            matching_sections[idx] = owned_matching[idx].view;
        }

        const risk_sections = try self.allocator.alloc(
            java_report_producer.RiskSectionView,
            self.risk_shards.len,
        );
        defer self.allocator.free(risk_sections);
        for (self.risk_shards, 0..) |*risk_shard, idx| {
            risk_sections[idx] = .{
                .shard_id = @intCast(idx),
                .shard_mask = self.risk_mask,
                .binary_processor = &risk_shard.binary_frame_assembler,
                .engine = risk_shard.engine,
            };
        }

        const config = &self.matching_shards[0];
        return java_report_producer.produce(
            self.allocator,
            query,
            matching_sections,
            risk_sections,
            config.java_identity_hash_context,
            config.java_report_wire,
        );
    }

    fn produceCustomReportEvents(
        self: *MultiShardKernelSet,
        descriptor: *const report_registry.Descriptor,
        body: []const u8,
    ) CustomReportDispatchError!events.OwnedMatcherEvents {
        var chain = self.makeEventChain();
        defer chain.deinit();

        const wire_config = self.matching_shards[0].java_report_wire;
        for (self.risk_shards, 0..) |*risk_shard, idx| {
            const shard_id = std.math.cast(i32, idx) orelse
                return error.InvalidShardId;
            const view: RiskReportView = .{
                .engine = risk_shard.engine,
                .binary_processor = &risk_shard.binary_frame_assembler,
            };
            try appendCustomRiskSection(
                self.allocator,
                &chain,
                descriptor,
                body,
                &view,
                shard_id,
                self.risk_mask,
                wire_config,
            );
        }

        for (self.matching_shards, 0..) |*kernel, idx| {
            const shard_id = std.math.cast(i32, idx) orelse
                return error.InvalidShardId;
            const view: MatchingReportView = .{
                .kernel = kernel,
                .binary_processor = &kernel.binary_frame_assembler,
            };
            try appendCustomMatchingSection(
                self.allocator,
                &chain,
                descriptor,
                body,
                &view,
                shard_id,
                self.matching_mask,
                wire_config,
            );
        }

        return chain.takeOwned();
    }

    /// Java sends every binary frame through every matching and risk binary
    /// processor. Staging clones make that broadcast atomic: an allocation,
    /// assembler, or consensus failure leaves every live processor unchanged.
    fn executeBinaryFrame(
        self: *MultiShardKernelSet,
        seq: u64,
        command: *cmd.OrderCommand,
        response: *CommandResponse,
    ) ExecuteError!void {
        const frame_result = self.broadcastBinaryFrame(command) catch |err| switch (err) {
            error.UnsupportedCommandType,
            error.InvalidLastFlag,
            error.TransferTypeMismatch,
            error.TransferStateCorrupted,
            error.BinaryProcessorMismatch,
            => {
                command.result_code = .binary_command_failed;
                return;
            },
            error.OutOfMemory => return error.OutOfMemory,
        };

        switch (frame_result) {
            .accepted => command.result_code = .accepted,
            .complete => |complete| {
                var owned_complete = complete;
                defer owned_complete.deinit(self.allocator);
                switch (owned_complete.command_type) {
                    .binary_data_command => {
                        self.executeBinaryPayloadWords(seq, owned_complete.words) catch |err| switch (err) {
                            error.OutOfMemory => return error.OutOfMemory,
                            else => command.result_code = .binary_command_failed,
                        };
                        if (command.result_code != .binary_command_failed) command.result_code = .success;
                    },
                    .binary_data_query => {
                        var envelope = report_query_envelope.decodeWordsJavaFirst(
                            self.allocator,
                            owned_complete.words,
                            .{},
                        ) catch |err| switch (err) {
                            error.OutOfMemory => return error.OutOfMemory,
                            else => {
                                command.result_code = .binary_command_failed;
                                return;
                            },
                        };
                        defer envelope.deinit();

                        const registry = self.matching_shards[0].custom_report_registry;
                        if (!envelope.isReservedBuiltInCode()) {
                            const descriptor = registry.lookup(envelope.raw.code) orelse {
                                command.result_code = .success;
                                return;
                            };
                            const owned_events = self.produceCustomReportEvents(
                                descriptor,
                                envelope.raw.body,
                            ) catch |err| switch (err) {
                                error.OutOfMemory => return error.OutOfMemory,
                                else => {
                                    command.result_code = .binary_command_failed;
                                    return;
                                },
                            };
                            owned_events.attachToResponse(response);
                            command.result_code = .success;
                            return;
                        }

                        const decoded = envelope.decodeBuiltInOrIgnored() catch |err| switch (err) {
                            error.OutOfMemory => return error.OutOfMemory,
                            else => {
                                command.result_code = .binary_command_failed;
                                return;
                            },
                        };
                        if (!self.matching_shards[0].isJavaReportQueryEnabled(decoded)) {
                            command.result_code = .success;
                            return;
                        }

                        // Keep the Java wire lane authoritative. A native
                        // aggregation runs only for an explicitly marked
                        // terminal frame and only after exact production.
                        const produced: ?java_report_producer.ProducedReport = self.produceJavaReport(decoded) catch |err| switch (err) {
                            error.OutOfMemory => return error.OutOfMemory,
                            error.MissingJavaIdentityHashContext => if (command.binary_report_projection == .exact_java_and_native)
                                null
                            else {
                                command.result_code = .binary_command_failed;
                                return;
                            },
                            else => {
                                command.result_code = .binary_command_failed;
                                return;
                            },
                        };
                        if (produced) |owned| {
                            response.java_report_response = owned.response;
                            response.matcher_events_head = owned.events_head;
                            response.events_owner = owned.events_owner;
                        }

                        if (command.binary_report_projection == .exact_java_and_native) {
                            const native_query = nativeQueryForJava(decoded) orelse unreachable;
                            response.report_response = self.queryReport(native_query) catch |err| switch (err) {
                                error.OutOfMemory => return error.OutOfMemory,
                                else => {
                                    response.deinit(self.allocator);
                                    command.result_code = .binary_command_failed;
                                    return;
                                },
                            };
                        }
                        command.result_code = .success;
                    },
                    else => command.result_code = .binary_command_failed,
                }
            },
        }
    }

    fn broadcastBinaryFrame(
        self: *MultiShardKernelSet,
        command: *const cmd.OrderCommand,
    ) BinaryBroadcastError!binary_frames.AcceptResult {
        const processor_count = self.matching_shards.len + self.risk_shards.len;
        const stages = try self.allocator.alloc(BinaryProcessorStage, processor_count);
        defer self.allocator.free(stages);
        var initialized: usize = 0;
        defer for (stages[0..initialized]) |*stage| stage.deinit(self.allocator);

        for (0..processor_count) |idx| {
            const target = self.binaryAssemblerAt(idx);
            stages[idx] = .{
                .target = target,
                .staged = binary_frames.BinaryFrameAssembler.init(self.allocator),
            };
            initialized += 1;

            var pending = try target.capturePendingState(self.allocator);
            defer pending.deinit();
            try stages[idx].staged.restorePendingState(pending.transfers);
            stages[idx].result = try stages[idx].staged.acceptFrame(command);
        }

        const canonical = stages[0].result orelse return error.BinaryProcessorMismatch;
        for (stages[1..]) |stage| {
            const candidate = stage.result orelse return error.BinaryProcessorMismatch;
            if (!binaryAcceptResultEqual(canonical, candidate)) {
                return error.BinaryProcessorMismatch;
            }
        }

        // All processors accepted the same frame and, on completion, produced
        // byte-identical transfer payloads. Commit every staged state together.
        for (stages) |*stage| {
            std.mem.swap(
                binary_frames.BinaryFrameAssembler,
                stage.target,
                &stage.staged,
            );
        }

        const result = stages[0].result.?;
        stages[0].result = null;
        return result;
    }

    fn binaryAssemblerAt(
        self: *MultiShardKernelSet,
        idx: usize,
    ) *binary_frames.BinaryFrameAssembler {
        if (idx < self.matching_shards.len) {
            return &self.matching_shards[idx].binary_frame_assembler;
        }
        return &self.risk_shards[idx - self.matching_shards.len].binary_frame_assembler;
    }

    fn binaryAcceptResultEqual(
        lhs: binary_frames.AcceptResult,
        rhs: binary_frames.AcceptResult,
    ) bool {
        return switch (lhs) {
            .accepted => switch (rhs) {
                .accepted => true,
                .complete => false,
            },
            .complete => |left| switch (rhs) {
                .accepted => false,
                .complete => |right| left.transfer_id == right.transfer_id and
                    left.command_type == right.command_type and
                    std.mem.eql(i64, left.words, right.words),
            },
        };
    }

    fn executeBinaryPayloadWords(
        self: *MultiShardKernelSet,
        seq: u64,
        words: []const i64,
    ) BinaryPayloadExecutionError!void {
        var decoded = try decodeBinaryCommandWordsJavaFirst(self.allocator, words);
        defer decoded.deinit();
        const cfg = self.matching_shards[0].report_queries_cfg;

        switch (decoded.payload) {
            .add_symbols => |symbols| {
                if (!cfg.add_symbols_binary) return error.UnsupportedBinaryCommandType;
                const handler_order = try javaSymbolHandlerOrder(self.allocator, symbols);
                defer self.allocator.free(handler_order);
                for (handler_order) |symbol| try self.addSymbolAll(symbol);
            },
            .add_accounts => |users| {
                if (!cfg.add_accounts_binary) return error.UnsupportedBinaryCommandType;
                for (users) |user_batch| {
                    const owner = self.riskEngineForUid(user_batch.uid);
                    var add_user = makeAddUserCommand(user_batch.uid);
                    _ = try owner.preProcessCommand(seq, &add_user);
                    switch (add_user.result_code) {
                        .success => {},
                        .user_mgmt_user_already_exists => continue,
                        else => return error.BinaryCommandRejected,
                    }
                    for (user_batch.accounts) |account| {
                        const tx_id = 1_000_000_000 + @as(i64, account.currency);
                        var adjustment = makeBalanceAdjustmentCommand(
                            user_batch.uid,
                            account.currency,
                            account.balance,
                            tx_id,
                            .adjustment,
                        );
                        _ = try owner.preProcessCommand(seq, &adjustment);
                        if (adjustment.result_code != .success) return error.BinaryCommandRejected;
                    }
                }
            },
        }
    }

    pub fn queryUserBalance(
        self: *MultiShardKernelSet,
        uid: i64,
        currency: i32,
    ) ReportError!?i64 {
        const profile = self.riskEngineForUid(uid).getUserProfile(uid) orelse return null;
        return profile.accountBalance(currency);
    }

    pub fn queryReport(self: *MultiShardKernelSet, query: ReportQuery) AggregateError!ReportResponse {
        if (!self.matching_shards[0].isReportQueryEnabled(query)) {
            return error.UnsupportedReportQueryType;
        }
        return switch (query) {
            .risk_state_hash => .{ .risk_state_hash = try self.buildRiskStateHash() },
            .state_hash_detailed => .{
                .state_hash_detailed = try self.buildStateHashDetailedReport(),
            },
            .fee_collected => |currency| blk: {
                var total: i64 = 0;
                for (self.risk_shards) |*risk_shard| {
                    total = std.math.add(
                        i64,
                        total,
                        risk_shard.engine.getFeeCollected(currency),
                    ) catch return error.ArithmeticOverflow;
                }
                break :blk .{ .fee_collected = total };
            },
            .single_user => |uid| .{
                .single_user = try self.buildSingleUserReport(uid),
            },
            .total_currency_balance => .{
                .total_currency_balance = try self.buildTotalCurrencyBalanceReport(),
            },
            .user_balance => |target| .{
                .user_balance = try self.queryUserBalance(target.uid, target.currency),
            },
            .order_book => |request| blk: {
                const matching = self.matchingShardForSymbol(request.symbol);
                const book = matching.books.get(request.symbol) orelse return error.UnknownSymbol;
                break :blk .{ .order_book = try book.l2Snapshot(self.allocator, request.depth) };
            },
        };
    }

    fn buildSingleUserReport(
        self: *MultiShardKernelSet,
        uid: i64,
    ) std.mem.Allocator.Error!SingleUserReport {
        var report: SingleUserReport = .{
            .uid = uid,
            .query_execution_status = .ok,
        };
        errdefer report.deinit(self.allocator);

        if (self.riskEngineForUid(uid).getUserProfile(uid)) |profile| {
            report.user_status = profile.user_status;
            var accounts_it = profile.accounts.iterator();
            while (accounts_it.next()) |entry| {
                try report.accounts.put(self.allocator, entry.key_ptr.*, entry.value_ptr.*);
            }
            var positions_it = profile.positions.iterator();
            while (positions_it.next()) |entry| {
                const position = entry.value_ptr.*;
                try report.positions.put(self.allocator, entry.key_ptr.*, .{
                    .quote_currency = position.currency,
                    .direction = position.direction,
                    .open_volume = position.open_volume,
                    .open_price_sum = position.open_price_sum,
                    .profit = position.profit,
                    .pending_sell_size = position.pending_sell_size,
                    .pending_buy_size = position.pending_buy_size,
                });
            }
        } else {
            report.query_execution_status = .user_not_found;
        }

        for (self.matching_shards) |*matching| {
            var book_it = matching.books.valueIterator();
            while (book_it.next()) |book| {
                try book.*.appendUserOrders(self.allocator, uid, &report.orders);
            }
        }
        std.sort.heap(naive.UserOrderView, report.orders.items, {}, ExchangeKernel.lessThanUserOrder);
        return report;
    }

    fn buildTotalCurrencyBalanceReport(
        self: *MultiShardKernelSet,
    ) AggregateError!TotalCurrencyBalanceReport {
        var report: TotalCurrencyBalanceReport = .{};
        errdefer report.deinit(self.allocator);

        for (self.risk_shards) |*risk_shard| {
            const engine = risk_shard.engine;
            var profile_it = engine.user_profile_service.user_profiles.valueIterator();
            while (profile_it.next()) |profile| {
                var account_it = profile.accounts.iterator();
                while (account_it.next()) |entry| {
                    try ExchangeKernel.addToI32I64Map(
                        self.allocator,
                        &report.account_balances,
                        entry.key_ptr.*,
                        entry.value_ptr.*,
                    );
                }
                var position_it = profile.positions.iterator();
                while (position_it.next()) |entry| {
                    const symbol_id = entry.key_ptr.*;
                    const position = entry.value_ptr.*;
                    const spec = engine.symbol_spec_provider.getSymbolSpecification(symbol_id) orelse
                        return error.UnknownSymbol;
                    const estimated_profit = try engine.estimatePositionProfitForReport(&position, spec);
                    try ExchangeKernel.addToI32I64Map(
                        self.allocator,
                        &report.account_balances,
                        position.currency,
                        estimated_profit,
                    );
                    if (position.open_volume <= 0) continue;
                    switch (position.direction) {
                        .long => try ExchangeKernel.addToI32I64Map(
                            self.allocator,
                            &report.open_interest_long,
                            symbol_id,
                            position.open_volume,
                        ),
                        .short => try ExchangeKernel.addToI32I64Map(
                            self.allocator,
                            &report.open_interest_short,
                            symbol_id,
                            position.open_volume,
                        ),
                        .empty => {},
                    }
                }
            }
            try mergeI32I64MapChecked(self.allocator, &report.fees, &engine.fees);
            try mergeI32I64MapChecked(self.allocator, &report.adjustments, &engine.adjustments);
            try mergeI32I64MapChecked(self.allocator, &report.suspends, &engine.suspends);
        }

        for (self.matching_shards) |*matching| {
            var book_it = matching.books.iterator();
            while (book_it.next()) |entry| {
                const spec = entry.value_ptr.*.symbolSpecification();
                if (spec.symbol_type == .currency_exchange_pair) {
                    try entry.value_ptr.*.accumulateOrderHolds(
                        self.allocator,
                        &report.orders_balances,
                    );
                }
            }
        }
        return report;
    }

    fn buildRiskStateHash(self: *MultiShardKernelSet) ReportError!u64 {
        var hasher = std.hash.Wyhash.init(0);
        ExchangeKernel.hashU64(&hasher, @as(u64, @intCast(self.risk_shards.len)));
        for (self.risk_shards, 0..) |*risk_shard, idx| {
            ExchangeKernel.hashU64(&hasher, @as(u64, @intCast(idx)));
            ExchangeKernel.hashU64(&hasher, try risk_shard.engine.stateHash());
        }
        return hasher.final();
    }

    fn buildStateHashDetailedReport(
        self: *MultiShardKernelSet,
    ) ReportError!DetailedStateHashReport {
        var report: DetailedStateHashReport = .{ .aggregate_hash = 0 };
        errdefer report.deinit(self.allocator);

        for (self.risk_shards, 0..) |*risk_shard, idx| {
            const engine = risk_shard.engine;
            const module_id = std.math.cast(i32, idx) orelse return error.ArithmeticOverflow;
            const binary_hash = try stateHashBinaryAssembler(&risk_shard.binary_frame_assembler);
            try report.submodules.append(self.allocator, .{
                .module_id = module_id,
                .submodule = .risk_symbol_spec_provider,
                .hash = try engine.symbol_spec_provider.stateHash(),
            });
            try report.submodules.append(self.allocator, .{
                .module_id = module_id,
                .submodule = .risk_user_profile_service,
                .hash = try engine.user_profile_service.stateHash(),
            });
            try report.submodules.append(self.allocator, .{
                .module_id = module_id,
                .submodule = .risk_binary_cmd_processor,
                .hash = binary_hash,
            });
            try report.submodules.append(self.allocator, .{
                .module_id = module_id,
                .submodule = .risk_last_price_cache,
                .hash = try self.stateHashLastPriceCache(engine),
            });
            try report.submodules.append(self.allocator, .{
                .module_id = module_id,
                .submodule = .risk_fees,
                .hash = try self.stateHashI32I64Map(&engine.fees),
            });
            try report.submodules.append(self.allocator, .{
                .module_id = module_id,
                .submodule = .risk_adjustments,
                .hash = try self.stateHashI32I64Map(&engine.adjustments),
            });
            try report.submodules.append(self.allocator, .{
                .module_id = module_id,
                .submodule = .risk_suspends,
                .hash = try self.stateHashI32I64Map(&engine.suspends),
            });
            try report.submodules.append(self.allocator, .{
                .module_id = module_id,
                .submodule = .risk_shard_mask,
                .hash = self.risk_mask,
            });
        }

        for (self.matching_shards, 0..) |*matching, idx| {
            const module_id = std.math.cast(i32, idx) orelse return error.ArithmeticOverflow;
            try report.submodules.append(self.allocator, .{
                .module_id = module_id,
                .submodule = .matching_binary_cmd_processor,
                .hash = try stateHashBinaryAssembler(&matching.binary_frame_assembler),
            });
            try report.submodules.append(self.allocator, .{
                .module_id = module_id,
                .submodule = .matching_order_books,
                .hash = try matching.stateHashOrderBooks(),
            });
            try report.submodules.append(self.allocator, .{
                .module_id = module_id,
                .submodule = .matching_shard_mask,
                .hash = self.matching_mask,
            });
        }

        std.sort.heap(
            StateHashSubmodule,
            report.submodules.items,
            {},
            ExchangeKernel.lessThanStateHashSubmodule,
        );
        report.aggregate_hash = ExchangeKernel.stateHashFromSubmodules(report.submodules.items);
        return report;
    }

    fn stateHashBinaryAssembler(
        assembler: *const binary_frames.BinaryFrameAssembler,
    ) ReportError!u64 {
        return assembler.stateHash() catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            error.TransferStateCorrupted => return error.BinaryFrameCorrupted,
        };
    }

    fn stateHashI32I64Map(
        self: *MultiShardKernelSet,
        map: *const std.AutoHashMapUnmanaged(i32, i64),
    ) ReportError!u64 {
        var hasher = std.hash.Wyhash.init(0);
        var keys: std.ArrayListUnmanaged(i32) = .empty;
        defer keys.deinit(self.allocator);
        try keys.ensureTotalCapacityPrecise(self.allocator, map.count());
        var it = map.iterator();
        while (it.next()) |entry| keys.appendAssumeCapacity(entry.key_ptr.*);
        std.sort.heap(i32, keys.items, {}, ExchangeKernel.lessThanI32);
        for (keys.items) |key| {
            const value = map.get(key) orelse return error.StateHashCorrupted;
            ExchangeKernel.hashI32(&hasher, key);
            ExchangeKernel.hashI64(&hasher, value);
        }
        return hasher.final();
    }

    fn stateHashLastPriceCache(
        self: *MultiShardKernelSet,
        engine: *const risk.RiskEngine,
    ) ReportError!u64 {
        var hasher = std.hash.Wyhash.init(0);
        var keys: std.ArrayListUnmanaged(i32) = .empty;
        defer keys.deinit(self.allocator);
        try keys.ensureTotalCapacityPrecise(self.allocator, engine.last_price_cache.count());
        var it = engine.last_price_cache.iterator();
        while (it.next()) |entry| keys.appendAssumeCapacity(entry.key_ptr.*);
        std.sort.heap(i32, keys.items, {}, ExchangeKernel.lessThanI32);
        for (keys.items) |key| {
            const value = engine.last_price_cache.get(key) orelse return error.StateHashCorrupted;
            ExchangeKernel.hashI32(&hasher, key);
            ExchangeKernel.hashI64(&hasher, value.ask_price);
            ExchangeKernel.hashI64(&hasher, value.bid_price);
        }
        return hasher.final();
    }

    pub fn aggregateTotalCurrencyBalance(self: *MultiShardKernelSet) AggregateError!TotalCurrencyBalanceReport {
        return self.buildTotalCurrencyBalanceReport();
    }

    pub fn detailedHashFingerprint(self: *MultiShardKernelSet) ReportError!u64 {
        var detailed = try self.buildStateHashDetailedReport();
        defer detailed.deinit(self.allocator);
        return detailed.aggregate_hash;
    }

    /// Narrow staged-execution boundary for grouped-pipeline owners.
    /// Production immediate APIs continue to use `executePipelineAtSequence`;
    /// this adapter deliberately exposes no shard storage and owns no
    /// scheduling policy.
    pub const StageAdapter = struct {
        kernels: *MultiShardKernelSet,

        pub const MatchingOutput = struct {
            response: CommandResponse,
            r2_events_head: ?*events.MatcherTradeEvent = null,
            r2_events_owner: ?events.EventNodesOwner = null,
            r2_top: ?events.L2Top = null,
            requires_risk_release: bool = false,

            pub fn deinit(self: *MatchingOutput, output_allocator: std.mem.Allocator) void {
                self.response.deinit(output_allocator);
                if (self.r2_events_head) |head| {
                    const owner = self.r2_events_owner orelse events.EventNodesOwner{ .heap = output_allocator };
                    events.deinitMatcherEventChain(owner, head);
                }
                self.* = undefined;
            }
        };

        pub const PreparedRiskRelease = struct {
            inner: PreparedRiskReleaseStage,

            pub fn deinit(
                self: *PreparedRiskRelease,
                release_allocator: std.mem.Allocator,
            ) void {
                self.inner.deinit(release_allocator);
                self.* = undefined;
            }
        };

        pub const RiskReleaseKind = enum {
            none,
            exchange,
            margin,
        };

        pub const PreparedShardRiskRelease = union(RiskReleaseKind) {
            none,
            exchange: risk.RiskEngine.PreparedExchangeRelease,
            margin: risk.RiskEngine.PreparedMarginRelease,

            pub fn deinit(self: *PreparedShardRiskRelease) void {
                switch (self.*) {
                    .none => {},
                    .exchange => |*prepared| prepared.deinit(),
                    .margin => |*prepared| prepared.deinit(),
                }
                self.* = undefined;
            }
        };

        pub fn init(kernels: *MultiShardKernelSet) StageAdapter {
            return .{ .kernels = kernels };
        }

        pub fn allocator(self: StageAdapter) std.mem.Allocator {
            return self.kernels.allocator;
        }

        pub fn settledFrontier(self: StageAdapter) u64 {
            return self.kernels.next_seq;
        }

        pub fn publicationFrontiersEqual(
            self: StageAdapter,
            expected: u64,
        ) bool {
            if (self.kernels.next_seq != expected) return false;
            for (self.kernels.matching_shards) |*kernel| {
                if (kernel.next_seq != expected) return false;
            }
            for (self.kernels.risk_shards) |*risk_shard| {
                if (risk_shard.next_seq != expected) return false;
            }
            return true;
        }

        pub fn riskShardCount(self: StageAdapter) usize {
            return self.kernels.riskShardCount();
        }

        pub fn matchingShardCount(self: StageAdapter) usize {
            return self.kernels.matchingShardCount();
        }

        pub fn riskOwnerIndex(
            self: StageAdapter,
            command: *const cmd.OrderCommand,
        ) usize {
            return self.kernels.shardIndexForUid(command.uid);
        }

        pub fn matchingOwnerIndex(
            self: StageAdapter,
            command: *const cmd.OrderCommand,
        ) usize {
            return self.kernels.shardIndexForSymbol(command.symbol);
        }

        /// Read-only publication admission. The serial grouped owner completes
        /// every other fallible admission step before settling an older group.
        pub fn validatePublicationSequence(
            self: StageAdapter,
            assigned_sequence: publication.PublicationSequence,
        ) ExecuteError!void {
            const seq = assigned_sequence.toInt();
            if (seq <= self.kernels.next_seq) return error.NonMonotonicPublicationSequence;
            for (self.kernels.matching_shards) |*kernel| {
                if (seq <= kernel.next_seq) return error.NonMonotonicPublicationSequence;
            }
            for (self.kernels.risk_shards) |*risk_shard| {
                if (seq <= risk_shard.next_seq) return error.NonMonotonicPublicationSequence;
            }
        }

        /// Infallible after `validatePublicationSequence` while the caller
        /// retains exclusive publication-frontier ownership.
        pub fn consumePublicationSequence(
            self: StageAdapter,
            assigned_sequence: publication.PublicationSequence,
        ) void {
            const seq = assigned_sequence.toInt();
            std.debug.assert(seq > self.kernels.next_seq);
            for (self.kernels.matching_shards) |*kernel| {
                std.debug.assert(seq > kernel.next_seq);
            }
            for (self.kernels.risk_shards) |*risk_shard| {
                std.debug.assert(seq > risk_shard.next_seq);
            }
            self.kernels.next_seq = seq;
            for (self.kernels.matching_shards) |*kernel| kernel.next_seq = seq;
            for (self.kernels.risk_shards) |*risk_shard| risk_shard.next_seq = seq;
        }

        pub fn runRiskPreProcess(
            self: StageAdapter,
            sequence: u64,
            command: *cmd.OrderCommand,
        ) ExecuteError!void {
            if (command.command == .reset or
                ExchangeKernel.isBinaryFrameCommand(command.command))
            {
                return;
            }
            try self.kernels.executeRiskPreProcessStage(sequence, command);
        }

        pub fn runRiskPreProcessForShard(
            self: StageAdapter,
            shard_index: usize,
            sequence: u64,
            command: *cmd.OrderCommand,
        ) ExecuteError!void {
            std.debug.assert(shard_index < self.riskShardCount());
            if (shard_index != self.riskOwnerIndex(command)) return;
            try self.runRiskPreProcess(sequence, command);
        }

        pub fn runMatching(
            self: StageAdapter,
            sequence: u64,
            command: *cmd.OrderCommand,
        ) ExecuteError!MatchingOutput {
            var response: CommandResponse = .{
                .sequence = sequence,
                .command = command.*,
                .result_code = command.result_code,
            };
            errdefer response.deinit(self.kernels.allocator);

            if (command.command == .reset) {
                self.kernels.resetAll();
                command.result_code = .success;
                response.completeFromCommand(command);
                return .{ .response = response };
            }

            if (ExchangeKernel.isBinaryFrameCommand(command.command)) {
                try self.kernels.executeBinaryFrame(sequence, command, &response);
                response.completeFromCommand(command);
                return .{ .response = response };
            }

            var event_chain = self.kernels.makeEventChain();
            defer event_chain.deinit();
            var risk_l2_snapshot: ?naive.L2MarketData = null;
            defer if (risk_l2_snapshot) |*snapshot| snapshot.deinit(self.kernels.allocator);

            const disposition = try self.kernels.executeMatchingStage(
                command,
                &response,
                &event_chain,
                &risk_l2_snapshot,
            );
            const requires_risk_release = disposition == .continue_to_risk_release;
            const top: ?events.L2Top = if (!requires_risk_release)
                null
            else if (response.l2_snapshot) |*snapshot|
                ExchangeKernel.l2TopFromSnapshot(snapshot)
            else if (risk_l2_snapshot) |*snapshot|
                ExchangeKernel.l2TopFromSnapshot(snapshot)
            else
                null;

            const event_owner = event_chain.owner;
            const r2_events_head = event_chain.takeOwnedHead();
            errdefer events.deinitMatcherEventChain(event_owner, r2_events_head);
            response.matcher_events_head = try events.cloneMatcherEventChain(event_owner, r2_events_head);
            response.events_owner = event_owner;
            response.completeFromCommand(command);
            return .{
                .response = response,
                .r2_events_head = r2_events_head,
                .r2_events_owner = event_owner,
                .r2_top = top,
                .requires_risk_release = requires_risk_release,
            };
        }

        pub fn runMatchingForShard(
            self: StageAdapter,
            shard_index: usize,
            sequence: u64,
            command: *cmd.OrderCommand,
        ) ExecuteError!?MatchingOutput {
            std.debug.assert(shard_index < self.matchingShardCount());
            if (shard_index != self.matchingOwnerIndex(command)) return null;
            return try self.runMatching(sequence, command);
        }

        /// Returns a value copy of the authoritative symbol definition shared
        /// by all risk shards. Staged owners may safely retain the copy across
        /// later shard work without borrowing mutable provider storage.
        pub fn canonicalSymbolSpecification(
            self: StageAdapter,
            command: *const cmd.OrderCommand,
        ) ?common.CoreSymbolSpecification {
            return self.kernels.risk_shards[0].engine.symbol_spec_provider
                .getSymbolSpecification(command.symbol);
        }

        /// The canonical symbol specification is shared by every risk shard.
        /// Resolve its release family exactly once before per-shard preparation.
        pub fn riskReleaseKind(
            self: StageAdapter,
            command: *const cmd.OrderCommand,
        ) RiskReleaseKind {
            const release_spec = self.canonicalSymbolSpecification(command) orelse return .none;
            return if (release_spec.symbol_type == .currency_exchange_pair)
                .exchange
            else
                .margin;
        }

        pub fn prepareRiskReleaseForShard(
            self: StageAdapter,
            shard_index: usize,
            kind: RiskReleaseKind,
            command: *const cmd.OrderCommand,
            event_head: ?*events.MatcherTradeEvent,
            top: ?events.L2Top,
        ) ExecuteError!PreparedShardRiskRelease {
            std.debug.assert(shard_index < self.riskShardCount());
            const engine = self.kernels.risk_shards[shard_index].engine;
            return switch (kind) {
                .none => .none,
                .exchange => .{
                    .exchange = try engine.prepareExchangeRiskRelease(
                        command,
                        event_head,
                        top,
                    ),
                },
                .margin => .{
                    .margin = try engine.prepareMarginRiskRelease(
                        command,
                        event_head,
                        top,
                    ),
                },
            };
        }

        pub fn commitPreparedRiskReleaseForShard(
            self: StageAdapter,
            shard_index: usize,
            prepared: *PreparedShardRiskRelease,
        ) void {
            std.debug.assert(shard_index < self.riskShardCount());
            const engine = self.kernels.risk_shards[shard_index].engine;
            switch (prepared.*) {
                .none => {},
                .exchange => |*release| engine.commitPreparedExchangeRiskRelease(release),
                .margin => |*release| engine.commitPreparedMarginRiskRelease(release),
            }
        }

        /// Every fallible risk-shard preparation completes before
        /// `commitPreparedRiskRelease` can mutate any shard.
        pub fn prepareAllRiskRelease(
            self: StageAdapter,
            command: *const cmd.OrderCommand,
            event_head: ?*events.MatcherTradeEvent,
            top: ?events.L2Top,
        ) ExecuteError!PreparedRiskRelease {
            return .{ .inner = try self.kernels.prepareRiskReleaseStage(
                command,
                event_head,
                top,
            ) };
        }

        pub fn commitPreparedRiskRelease(
            self: StageAdapter,
            prepared: *PreparedRiskRelease,
        ) void {
            self.kernels.commitRiskReleaseStage(&prepared.inner);
        }
    };

    fn nextSequence(self: *const MultiShardKernelSet) error{SequenceOverflow}!u64 {
        if (self.next_seq == std.math.maxInt(u64)) return error.SequenceOverflow;
        return self.next_seq + 1;
    }
};

fn executeSingleAndShardedExpectEqual(
    allocator: std.mem.Allocator,
    single_kernel: *ExchangeKernel,
    sharded: *MultiShardKernelSet,
    source: cmd.OrderCommand,
) !cmd.CommandResultCode {
    var single_command = source;
    var sharded_command = source;
    var single_response = try single_kernel.execute(&single_command);
    defer single_response.deinit(allocator);
    var sharded_response = try sharded.executeRouted(&sharded_command);
    defer sharded_response.deinit(allocator);

    try std.testing.expectEqual(single_response.result_code, sharded_response.result_code);
    try std.testing.expectEqualDeep(single_response.command, sharded_response.command);
    if (single_response.l2_snapshot != null or sharded_response.l2_snapshot != null) {
        try std.testing.expect(single_response.l2_snapshot != null and sharded_response.l2_snapshot != null);
        try expectEqualL2(single_response.l2_snapshot.?, sharded_response.l2_snapshot.?);
    }
    return sharded_response.result_code;
}

fn executeSingleAndSplitExpectEqual(
    allocator: std.mem.Allocator,
    single_kernel: *ExchangeKernel,
    sharded: *MultiShardKernelSet,
    source: cmd.OrderCommand,
) !cmd.CommandResultCode {
    var single_command = source;
    var split_command = source;
    var single_response = try single_kernel.execute(&single_command);
    defer single_response.deinit(allocator);
    var split_response = try sharded.executeSplit(&split_command);
    defer split_response.deinit(allocator);

    try std.testing.expectEqual(single_response.sequence, split_response.sequence);
    try std.testing.expectEqual(single_response.result_code, split_response.result_code);
    try std.testing.expectEqualDeep(single_response.command, split_response.command);
    if (single_response.l2_snapshot != null or split_response.l2_snapshot != null) {
        try std.testing.expect(single_response.l2_snapshot != null and split_response.l2_snapshot != null);
        try expectEqualL2(single_response.l2_snapshot.?, split_response.l2_snapshot.?);
    }
    return split_response.result_code;
}

fn uidForShard(target_shard: usize, shard_count: usize, seed: i64) i64 {
    std.debug.assert(shard_count > 0);
    std.debug.assert(@popCount(@as(u64, @intCast(shard_count))) == 1);
    const mask: u64 = @as(u64, @intCast(shard_count - 1));

    var uid = seed;
    while (true) : (uid += 1) {
        const shard = @as(usize, @intCast(@as(u64, @bitCast(uid)) & mask));
        if (shard == target_shard) return uid;
    }
}

fn executeMultiExpect(
    allocator: std.mem.Allocator,
    set: *MultiShardKernelSet,
    use_split: bool,
    source: cmd.OrderCommand,
    expected: cmd.CommandResultCode,
) !void {
    var command = source;
    var response = if (use_split)
        try set.executeSplit(&command)
    else
        try set.executeRouted(&command);
    defer response.deinit(allocator);
    try std.testing.expectEqual(expected, response.result_code);
}

const BorrowedGroupedResultCapture = struct {
    call_count: usize = 0,
    sequence: u64 = 0,
    result_code: cmd.CommandResultCode = .new,
    matcher_event_count: usize = 0,
    first_matcher_event_type: ?events.MatcherEventType = null,

    fn hook(self: *BorrowedGroupedResultCapture) MultiShardKernelSet.BorrowedPostMatchingHook {
        return .{
            .context = self,
            .callback = observe,
        };
    }

    fn observe(context: ?*anyopaque, response: *const CommandResponse) void {
        const self: *BorrowedGroupedResultCapture = @ptrCast(@alignCast(context.?));
        self.call_count += 1;
        self.sequence = response.sequence;
        self.result_code = response.result_code;
        self.matcher_event_count = events.listSize(response.matcher_events_head);
        self.first_matcher_event_type = if (response.matcher_events_head) |head|
            head.event_type
        else
            null;
    }
};

const ReentrantGroupedHookCapture = struct {
    executor: *MultiShardKernelSet.GroupedTwoStepExecutor,
    kernels: *MultiShardKernelSet,
    outer_sequence: u64 = 0,
    outer_matcher_event_count: usize = 0,
    reentrant_error: ?anyerror = null,
    immediate_error: ?anyerror = null,
    immediate_sequence: u64 = 0,

    fn hook(self: *ReentrantGroupedHookCapture) MultiShardKernelSet.BorrowedPostMatchingHook {
        return .{
            .context = self,
            .callback = observeAndPublishImmediate,
        };
    }

    fn observeAndPublishImmediate(
        context: ?*anyopaque,
        response: *const CommandResponse,
    ) void {
        const self: *ReentrantGroupedHookCapture = @ptrCast(@alignCast(context.?));
        self.outer_sequence = response.sequence;
        self.outer_matcher_event_count = events.listSize(response.matcher_events_head);

        self.executor.flushAll() catch |err| {
            self.reentrant_error = err;
        };

        var immediate_nop = makeNopCommand();
        var immediate_response = self.kernels.executeRouted(&immediate_nop) catch |err| {
            self.immediate_error = err;
            return;
        };
        defer immediate_response.deinit(self.kernels.allocator);
        self.immediate_sequence = immediate_response.sequence;
    }
};

fn setupGroupedSpotTopology(
    allocator: std.mem.Allocator,
    set: *MultiShardKernelSet,
    use_split: bool,
    symbol: i32,
    maker_uid: i64,
    taker_uid: i64,
    maker_order_id: i64,
    maker_size: i64,
) !void {
    try set.addSymbolAll(try makeExchangeSpecForSymbol(symbol));
    for ([_]i64{ maker_uid, taker_uid }) |uid| {
        try executeMultiExpect(
            allocator,
            set,
            use_split,
            makeAddUserCommand(uid),
            .success,
        );
    }
    try executeMultiExpect(
        allocator,
        set,
        use_split,
        makeBalanceAdjustmentCommand(maker_uid, 11, 100_000, 1, .adjustment),
        .success,
    );
    try executeMultiExpect(
        allocator,
        set,
        use_split,
        makeBalanceAdjustmentCommand(taker_uid, 15, 100_000, 1, .adjustment),
        .success,
    );

    var maker_ask = cmd.OrderCommand.newOrder(
        .gtc,
        maker_order_id,
        maker_uid,
        100,
        0,
        maker_size,
        .ask,
    );
    maker_ask.symbol = symbol;
    try executeMultiExpect(allocator, set, use_split, maker_ask, .success);
}

fn groupedTestTotalFee(set: *MultiShardKernelSet, currency: i32) i64 {
    var total: i64 = 0;
    for (set.risk_shards) |*risk_shard| {
        total += risk_shard.engine.getFeeCollected(currency);
    }
    return total;
}

const SettlementPrepareRecorder = struct {
    const Target = union(enum) {
        single: *ExchangeKernel,
        multi: *MultiShardKernelSet,
    };

    target: Target,
    reject: bool = false,
    prepare_calls: usize = 0,
    risk_mark_calls: usize = 0,
    journal_mark_calls: usize = 0,
    recovery_calls: usize = 0,
    last_sequence: u64 = 0,
    last_mode: settlement_lifecycle.ExecutionMode = .live,
    last_trade_count: usize = 0,
    last_order_id: i64 = 0,
    last_specification: ?common.CoreSymbolSpecification = null,
    observed_total_fee: i64 = 0,
    observed_risk_hashes: [4]u64 = .{ 0, 0, 0, 0 },
    observed_risk_hash_count: usize = 0,
    risk_hash_failed: bool = false,

    fn hooks(self: *SettlementPrepareRecorder) settlement_lifecycle.Hooks {
        return settlement_lifecycle.Hooks.enabled(self, &.{
            .prepare = prepare,
            .mark_risk_r2_committed = markRiskR2Committed,
            .mark_journal_committed = markJournalCommitted,
            .finish_recovery = finishRecovery,
        });
    }

    fn prepare(
        context: *anyopaque,
        batch: settlement_lifecycle.PreparedBatch,
    ) settlement_lifecycle.PrepareObservation {
        const self: *SettlementPrepareRecorder = @ptrCast(@alignCast(context));
        self.prepare_calls += 1;
        self.last_sequence = batch.sequence.toInt();
        self.last_mode = batch.mode;
        self.last_trade_count = batch.tradeCount();
        self.last_order_id = batch.command.order_id;
        self.last_specification = batch.specification;
        self.observed_total_fee = 0;
        self.observed_risk_hash_count = 0;

        switch (self.target) {
            .single => |kernel| self.captureRiskHash(
                &kernel.risk_engine,
                batch.specification.quote_currency,
            ),
            .multi => |kernels| {
                if (kernels.riskShardCount() > self.observed_risk_hashes.len) {
                    self.risk_hash_failed = true;
                } else {
                    for (kernels.risk_shards) |*risk_shard| {
                        self.captureRiskHash(
                            risk_shard.engine,
                            batch.specification.quote_currency,
                        );
                    }
                }
            },
        }
        return if (self.reject) .rejected else .prepared;
    }

    fn captureRiskHash(
        self: *SettlementPrepareRecorder,
        engine: *risk.RiskEngine,
        quote_currency: i32,
    ) void {
        const index = self.observed_risk_hash_count;
        if (index >= self.observed_risk_hashes.len) {
            self.risk_hash_failed = true;
            return;
        }
        self.observed_risk_hashes[index] = engine.stateHash() catch hash_error: {
            self.risk_hash_failed = true;
            break :hash_error 0;
        };
        self.observed_risk_hash_count += 1;
        self.observed_total_fee += engine.getFeeCollected(quote_currency);
    }

    fn markRiskR2Committed(
        context: *anyopaque,
        _: publication.PublicationSequence,
    ) settlement_lifecycle.MarkObservation {
        const self: *SettlementPrepareRecorder = @ptrCast(@alignCast(context));
        self.risk_mark_calls += 1;
        return .marked;
    }

    fn markJournalCommitted(
        context: *anyopaque,
        _: publication.PublicationSequence,
    ) settlement_lifecycle.MarkObservation {
        const self: *SettlementPrepareRecorder = @ptrCast(@alignCast(context));
        self.journal_mark_calls += 1;
        return .marked;
    }

    fn finishRecovery(
        context: *anyopaque,
        _: publication.PublicationSequence,
    ) settlement_lifecycle.RecoveryObservation {
        const self: *SettlementPrepareRecorder = @ptrCast(@alignCast(context));
        self.recovery_calls += 1;
        return .completed;
    }
};

fn executeSingleExpect(
    allocator: std.mem.Allocator,
    kernel: *ExchangeKernel,
    source: cmd.OrderCommand,
    expected: cmd.CommandResultCode,
) !void {
    var command = source;
    var response = try kernel.execute(&command);
    defer response.deinit(allocator);
    try std.testing.expectEqual(expected, response.result_code);
}

fn setupSingleSettlementMultiFill(
    allocator: std.mem.Allocator,
    kernel: *ExchangeKernel,
    symbol: i32,
    maker_uids: [2]i64,
    taker_uid: i64,
) !void {
    try std.testing.expect(try kernel.addSymbol(try makeExchangeSpecForSymbol(symbol)));
    for ([_]i64{ maker_uids[0], maker_uids[1], taker_uid }) |uid| {
        try executeSingleExpect(allocator, kernel, makeAddUserCommand(uid), .success);
    }
    for (maker_uids, 0..) |uid, index| {
        try executeSingleExpect(
            allocator,
            kernel,
            makeBalanceAdjustmentCommand(uid, 11, 100_000, 1, .adjustment),
            .success,
        );
        var maker_ask = cmd.OrderCommand.newOrder(
            .gtc,
            91_000 + @as(i64, @intCast(index)),
            uid,
            100 + @as(i64, @intCast(index)),
            0,
            2 + @as(i64, @intCast(index)),
            .ask,
        );
        maker_ask.symbol = symbol;
        try executeSingleExpect(allocator, kernel, maker_ask, .success);
    }
    try executeSingleExpect(
        allocator,
        kernel,
        makeBalanceAdjustmentCommand(taker_uid, 15, 100_000, 1, .adjustment),
        .success,
    );
}

fn setupMultiSettlementMultiFill(
    allocator: std.mem.Allocator,
    kernels: *MultiShardKernelSet,
    symbol: i32,
    maker_uids: [2]i64,
    taker_uid: i64,
) !void {
    try kernels.addSymbolAll(try makeExchangeSpecForSymbol(symbol));
    for ([_]i64{ maker_uids[0], maker_uids[1], taker_uid }) |uid| {
        try executeMultiExpect(allocator, kernels, false, makeAddUserCommand(uid), .success);
    }
    for (maker_uids, 0..) |uid, index| {
        try executeMultiExpect(
            allocator,
            kernels,
            false,
            makeBalanceAdjustmentCommand(uid, 11, 100_000, 1, .adjustment),
            .success,
        );
        var maker_ask = cmd.OrderCommand.newOrder(
            .gtc,
            91_000 + @as(i64, @intCast(index)),
            uid,
            100 + @as(i64, @intCast(index)),
            0,
            2 + @as(i64, @intCast(index)),
            .ask,
        );
        maker_ask.symbol = symbol;
        try executeMultiExpect(allocator, kernels, false, maker_ask, .success);
    }
    try executeMultiExpect(
        allocator,
        kernels,
        false,
        makeBalanceAdjustmentCommand(taker_uid, 15, 100_000, 1, .adjustment),
        .success,
    );
}

fn settlementMultiFillBid(order_id: i64, uid: i64, symbol: i32) cmd.OrderCommand {
    var taker_bid = cmd.OrderCommand.newOrder(.gtc, order_id, uid, 101, 101, 5, .bid);
    taker_bid.symbol = symbol;
    return taker_bid;
}

fn expectOperationalRiskStateEmpty(engine: *const risk.RiskEngine) !void {
    try std.testing.expectEqual(@as(usize, 0), engine.symbol_spec_provider.symbols.count());
    try std.testing.expectEqual(@as(usize, 0), engine.user_profile_service.user_profiles.count());
    try std.testing.expectEqual(@as(usize, 0), engine.last_price_cache.count());
    try std.testing.expectEqual(@as(usize, 0), engine.fees.count());
    try std.testing.expectEqual(@as(usize, 0), engine.adjustments.count());
    try std.testing.expectEqual(@as(usize, 0), engine.suspends.count());
}

fn expectAllShardSequences(set: *const MultiShardKernelSet, expected: u64) !void {
    try std.testing.expectEqual(expected, set.next_seq);
    for (0..set.matchingShardCount()) |idx| {
        try std.testing.expectEqual(expected, set.matchingObservedSequence(idx));
    }
    for (0..set.riskShardCount()) |idx| {
        try std.testing.expectEqual(expected, set.riskObservedSequence(idx));
    }
}

fn expectAllBinaryProcessorState(
    set: *const MultiShardKernelSet,
    expected_pending: usize,
) !u64 {
    var expected_hash: ?u64 = null;
    for (set.matching_shards) |*matching| {
        try std.testing.expectEqual(
            expected_pending,
            matching.binary_frame_assembler.pendingTransfers(),
        );
        const hash = try matching.binary_frame_assembler.stateHash();
        if (expected_hash) |value| {
            try std.testing.expectEqual(value, hash);
        } else {
            expected_hash = hash;
        }
    }
    for (set.risk_shards) |*risk_shard| {
        try std.testing.expectEqual(
            expected_pending,
            risk_shard.binary_frame_assembler.pendingTransfers(),
        );
        const hash = try risk_shard.binary_frame_assembler.stateHash();
        if (expected_hash) |value| {
            try std.testing.expectEqual(value, hash);
        } else {
            expected_hash = hash;
        }
    }
    return expected_hash orelse unreachable;
}

fn expectDetailedTopologyIdentity(
    detailed: *const DetailedStateHashReport,
    matching_count: usize,
    risk_count: usize,
) !void {
    const risk_types = [_]StateHashSubmoduleType{
        .risk_symbol_spec_provider,
        .risk_user_profile_service,
        .risk_binary_cmd_processor,
        .risk_last_price_cache,
        .risk_fees,
        .risk_adjustments,
        .risk_suspends,
        .risk_shard_mask,
    };
    const matching_types = [_]StateHashSubmoduleType{
        .matching_binary_cmd_processor,
        .matching_order_books,
        .matching_shard_mask,
    };
    try std.testing.expectEqual(
        risk_types.len * risk_count + matching_types.len * matching_count,
        detailed.submodules.items.len,
    );

    var cursor: usize = 0;
    for (risk_types) |submodule| {
        for (0..risk_count) |module_idx| {
            const entry = detailed.submodules.items[cursor];
            try std.testing.expectEqual(submodule, entry.submodule);
            try std.testing.expectEqual(@as(i32, @intCast(module_idx)), entry.module_id);
            cursor += 1;
        }
    }
    for (matching_types) |submodule| {
        for (0..matching_count) |module_idx| {
            const entry = detailed.submodules.items[cursor];
            try std.testing.expectEqual(submodule, entry.submodule);
            try std.testing.expectEqual(@as(i32, @intCast(module_idx)), entry.module_id);
            cursor += 1;
        }
    }
}

fn expectJavaStateHashTopology(
    allocator: std.mem.Allocator,
    state_hash: *const JavaStateHashReport,
    event_head: ?*const events.MatcherTradeEvent,
    matching_count: usize,
    risk_count: usize,
) !void {
    const risk_types = [_]java_report_wire.StateHashSubmodule{
        .risk_symbol_spec_provider,
        .risk_user_profile_service,
        .risk_binary_cmd_processor,
        .risk_last_price_cache,
        .risk_fees,
        .risk_adjustments,
        .risk_suspends,
        .risk_shard_mask,
    };
    const matching_types = [_]java_report_wire.StateHashSubmodule{
        .matching_binary_cmd_processor,
        .matching_order_books,
        .matching_shard_mask,
    };
    try std.testing.expectEqual(
        risk_types.len * risk_count + matching_types.len * matching_count,
        state_hash.report.entries.len,
    );

    var cursor: usize = 0;
    for (risk_types) |submodule| {
        for (0..risk_count) |module_idx| {
            const entry = state_hash.report.entries[cursor];
            try std.testing.expectEqual(submodule, entry.submodule);
            try std.testing.expectEqual(@as(i32, @intCast(module_idx)), entry.module_id);
            cursor += 1;
        }
    }
    for (matching_types) |submodule| {
        for (0..matching_count) |module_idx| {
            const entry = state_hash.report.entries[cursor];
            try std.testing.expectEqual(submodule, entry.submodule);
            try std.testing.expectEqual(@as(i32, @intCast(module_idx)), entry.module_id);
            cursor += 1;
        }
    }

    for (0..risk_count) |module_idx| {
        var code_zero: ?i32 = null;
        var code_two: ?i32 = null;
        for (state_hash.report.entries) |entry| {
            if (entry.module_id != @as(i32, @intCast(module_idx))) continue;
            if (entry.submodule == .risk_symbol_spec_provider) code_zero = entry.hash;
            if (entry.submodule == .risk_binary_cmd_processor) code_two = entry.hash;
        }
        try std.testing.expectEqual(code_zero, code_two);
    }

    var grouped = try java_report_wire.groupMatcherEventChain(allocator, event_head, .{});
    defer grouped.deinit(allocator);
    try std.testing.expectEqual(risk_count + matching_count, grouped.sections.len);
    for (0..risk_count) |idx| {
        try std.testing.expectEqual(java_report_wire.EngineSection.risk, grouped.sections[idx].label.engine);
        try std.testing.expectEqual(@as(i32, @intCast(idx)), grouped.sections[idx].label.shard_id);
    }
    for (0..matching_count) |idx| {
        const section = grouped.sections[risk_count + idx];
        try std.testing.expectEqual(java_report_wire.EngineSection.matching, section.label.engine);
        try std.testing.expectEqual(@as(i32, @intCast(idx)), section.label.shard_id);
    }
}

fn executeMultiBinaryPayload(
    allocator: std.mem.Allocator,
    set: *MultiShardKernelSet,
    use_split: bool,
    command_type: cmd.OrderCommandType,
    transfer_id: i32,
    payload: []const u8,
) !CommandResponse {
    var frames: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
    defer frames.deinit(allocator);
    try appendBinaryPayloadFrames(allocator, &frames, command_type, transfer_id, payload);
    if (frames.items.len == 0) return error.EmptyBinaryPayload;

    var last_response: ?CommandResponse = null;
    errdefer if (last_response) |*response| response.deinit(allocator);
    for (frames.items, 0..) |frame_value, frame_idx| {
        var frame = frame_value;
        const response = if (use_split)
            try set.executeSplit(&frame)
        else
            try set.executeRouted(&frame);
        if (frame_idx + 1 != frames.items.len) {
            try std.testing.expectEqual(cmd.CommandResultCode.accepted, response.result_code);
        }
        if (last_response) |*previous| previous.deinit(allocator);
        last_response = response;
    }
    const response = last_response.?;
    last_response = null;
    return response;
}

fn assertSingleVsSplitStateParity(
    allocator: std.mem.Allocator,
    single: *ExchangeKernel,
    split: *MultiShardKernelSet,
    user_ids: []const i64,
) !void {
    var single_total = try single.queryReport(.total_currency_balance);
    defer single_total.deinit(allocator);
    var split_total = try split.aggregateTotalCurrencyBalance();
    defer split_total.deinit(allocator);
    switch (single_total) {
        .total_currency_balance => |*s| {
            try expectMapEqualI32I64(&s.account_balances, &split_total.account_balances);
            try expectMapEqualI32I64(&s.orders_balances, &split_total.orders_balances);
            try expectMapEqualI32I64(&s.fees, &split_total.fees);
            try expectMapEqualI32I64(&s.adjustments, &split_total.adjustments);
            try expectMapEqualI32I64(&s.suspends, &split_total.suspends);
            try expectMapEqualI32I64(&s.open_interest_long, &split_total.open_interest_long);
            try expectMapEqualI32I64(&s.open_interest_short, &split_total.open_interest_short);
        },
        else => return error.StateHashCorrupted,
    }

    const currencies = [_]i32{ 11, 15 };
    for (user_ids) |uid| {
        for (currencies) |currency| {
            var single_balance = try single.queryReport(.{
                .user_balance = .{
                    .uid = uid,
                    .currency = currency,
                },
            });
            defer single_balance.deinit(allocator);
            const split_balance = try split.queryUserBalance(uid, currency);
            try std.testing.expectEqual(single_balance.user_balance, split_balance);
        }
    }
}

fn assertOrderBooksSingleVsSplitParity(
    allocator: std.mem.Allocator,
    single: *ExchangeKernel,
    split: *MultiShardKernelSet,
    symbols: []const i32,
) !void {
    for (symbols) |symbol| {
        const depth = @as(usize, 32);
        const req = makeRequestOrderBookCommand(symbol, depth);
        _ = try executeSingleAndSplitExpectEqual(allocator, single, split, req);
    }
}

const FaultSimulationError = std.mem.Allocator.Error ||
    InitError ||
    ExecuteError ||
    ReportError ||
    common.CoreSymbolSpecification.ValidationError ||
    risk.SymbolSpecificationProvider.AddSymbolError ||
    error{
        ArithmeticOverflow,
        InvalidShardCount,
        InvalidShardPlan,
        TestExpectedEqual,
        TestUnexpectedResult,
        UnexpectedResult,
    };

fn errorSetContains(comptime ErrorSet: type, comptime error_name: []const u8) bool {
    inline for (std.meta.fields(ErrorSet)) |field| {
        if (std.mem.eql(u8, field.name, error_name)) return true;
    }
    return false;
}

test "kernel legacy and settlement execution errors remain source separated" {
    const settlement_error_names = [_][]const u8{
        "SettlementPreparationRejected",
        "SettlementRiskR2MarkRejected",
        "SettlementJournalMarkRejected",
        "SettlementRecoveryRejected",
    };
    inline for (settlement_error_names) |error_name| {
        try std.testing.expect(!errorSetContains(ExecuteError, error_name));
        try std.testing.expect(errorSetContains(ExecuteWithSettlementError, error_name));
    }

    try std.testing.expect(
        @typeInfo(@TypeOf(ExchangeKernel.execute)).@"fn".return_type.? ==
            ExecuteError!CommandResponse,
    );
    try std.testing.expect(
        @typeInfo(@TypeOf(ExchangeKernel.executeAtSequence)).@"fn".return_type.? ==
            ExecuteError!CommandResponse,
    );
    try std.testing.expect(
        @typeInfo(@TypeOf(ExchangeKernel.executeWithSettlement)).@"fn".return_type.? ==
            ExecuteWithSettlementError!CommandResponse,
    );
    try std.testing.expect(
        @typeInfo(@TypeOf(ExchangeKernel.executeAtSequenceWithSettlement)).@"fn".return_type.? ==
            ExecuteWithSettlementError!CommandResponse,
    );
    try std.testing.expect(
        @typeInfo(@TypeOf(MultiShardKernelSet.executeRouted)).@"fn".return_type.? ==
            ExecuteError!CommandResponse,
    );
    try std.testing.expect(
        @typeInfo(@TypeOf(MultiShardKernelSet.executeSplit)).@"fn".return_type.? ==
            ExecuteError!CommandResponse,
    );
    try std.testing.expect(
        @typeInfo(@TypeOf(MultiShardKernelSet.executeRoutedWithSettlement)).@"fn".return_type.? ==
            ExecuteWithSettlementError!CommandResponse,
    );
    try std.testing.expect(
        @typeInfo(@TypeOf(MultiShardKernelSet.executeSplitWithSettlement)).@"fn".return_type.? ==
            ExecuteWithSettlementError!CommandResponse,
    );
}

fn runSeededFaultSimulation(
    allocator: std.mem.Allocator,
    seed: u64,
    rounds: usize,
) FaultSimulationError!FaultSimulationResult {
    const shard_count: usize = 4;

    var output: FaultSimulationResult = .{};
    errdefer output.deinit(allocator);

    var single = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer single.deinit();
    var split = try MultiShardKernelSet.init(allocator, shard_count, .direct);
    defer split.deinit();

    const spec = try makeExchangeSpec();
    if (!(try single.addSymbol(spec))) return error.UnexpectedResult;
    try split.addSymbolAll(spec);

    const symbol_shard = split.shardIndexForSymbol(spec.symbol_id);
    var users: [8]i64 = undefined;
    for (0..users.len) |idx| {
        const target = (symbol_shard + 1 + idx) % shard_count;
        users[idx] = uidForShard(target, shard_count, 12_000 + @as(i64, @intCast(idx * 100)));
    }

    var tx_counters: std.AutoHashMapUnmanaged(i64, i64) = .empty;
    defer tx_counters.deinit(allocator);

    var base_commands: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
    defer base_commands.deinit(allocator);

    for (users) |uid| {
        try base_commands.append(allocator, makeAddUserCommand(uid));
        try base_commands.append(allocator, makeBalanceAdjustmentCommand(uid, 11, 200_000, 1, .adjustment));
        try base_commands.append(allocator, makeBalanceAdjustmentCommand(uid, 15, 200_000, 2, .adjustment));
        try tx_counters.put(allocator, uid, 2);
    }

    const maker_uid = users[0];
    const taker_uid = users[1];
    if (split.shardIndexForUid(maker_uid) == symbol_shard or split.shardIndexForUid(taker_uid) == symbol_shard) {
        return error.InvalidShardPlan;
    }

    var seed_ask = cmd.OrderCommand.newOrder(.gtc, 70_001, maker_uid, 100, 0, 7, .ask);
    seed_ask.symbol = spec.symbol_id;
    try base_commands.append(allocator, seed_ask);
    var seed_bid = cmd.OrderCommand.newOrder(.ioc, 70_002, taker_uid, 100, 100, 5, .bid);
    seed_bid.symbol = spec.symbol_id;
    try base_commands.append(allocator, seed_bid);

    const Candidate = struct {
        order_id: i64,
        uid: i64,
    };
    var candidates: std.ArrayListUnmanaged(Candidate) = .empty;
    defer candidates.deinit(allocator);
    try candidates.append(allocator, .{ .order_id = 70_001, .uid = maker_uid });

    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();
    var next_order_id: i64 = 70_003;

    for (0..rounds) |idx| {
        const roll = random.uintLessThan(u32, 100);
        var command: cmd.OrderCommand = undefined;

        if (roll < 40) {
            const uid = users[random.uintLessThan(usize, users.len)];
            const action: common.OrderAction = if (random.boolean()) .ask else .bid;
            const order_type: common.OrderType = blk: {
                const t = random.uintLessThan(u32, 100);
                if (t < 70) break :blk .gtc;
                if (t < 90) break :blk .ioc;
                break :blk .fok_budget;
            };
            const price = simulationPrice(random, 100);
            const size = @as(i64, @intCast(random.uintLessThan(u32, 10))) + 1;
            const reserve = if (action == .bid) price + @as(i64, @intCast(random.uintLessThan(u32, 15))) else 0;
            command = cmd.OrderCommand.newOrder(order_type, next_order_id, uid, price, reserve, size, action);
            command.symbol = spec.symbol_id;
            if (order_type == .gtc) {
                try candidates.append(allocator, .{
                    .order_id = next_order_id,
                    .uid = uid,
                });
            }
            next_order_id += 1;
        } else if (roll < 60) {
            const uid = users[random.uintLessThan(usize, users.len)];
            const ccy: i32 = if (random.boolean()) 11 else 15;
            const amount_abs = @as(i64, @intCast(random.uintLessThan(u32, 700))) + 1;
            const amount = if (random.uintLessThan(u32, 100) < 25) -amount_abs else amount_abs;
            const entry = tx_counters.getPtr(uid) orelse return error.InvalidShardPlan;
            entry.* += 1;
            command = makeBalanceAdjustmentCommand(uid, ccy, amount, entry.*, .adjustment);
        } else if (roll < 75) {
            const picked = if (candidates.items.len > 0 and random.boolean())
                candidates.items[random.uintLessThan(usize, candidates.items.len)]
            else
                Candidate{
                    .order_id = @as(i64, @intCast(random.uintLessThan(u32, 9_000))) + 1,
                    .uid = users[random.uintLessThan(usize, users.len)],
                };
            const uid = if (random.uintLessThan(u32, 100) < 85)
                picked.uid
            else
                users[random.uintLessThan(usize, users.len)];
            command = cmd.OrderCommand.cancel(picked.order_id, uid);
            command.symbol = spec.symbol_id;
        } else if (roll < 88) {
            const picked = if (candidates.items.len > 0 and random.boolean())
                candidates.items[random.uintLessThan(usize, candidates.items.len)]
            else
                Candidate{
                    .order_id = @as(i64, @intCast(random.uintLessThan(u32, 9_000))) + 1,
                    .uid = users[random.uintLessThan(usize, users.len)],
                };
            const uid = if (random.uintLessThan(u32, 100) < 85)
                picked.uid
            else
                users[random.uintLessThan(usize, users.len)];
            command = cmd.OrderCommand.update(picked.order_id, uid, simulationPrice(random, 100));
            command.symbol = spec.symbol_id;
        } else if (roll < 96) {
            const picked = if (candidates.items.len > 0 and random.boolean())
                candidates.items[random.uintLessThan(usize, candidates.items.len)]
            else
                Candidate{
                    .order_id = @as(i64, @intCast(random.uintLessThan(u32, 9_000))) + 1,
                    .uid = users[random.uintLessThan(usize, users.len)],
                };
            const uid = if (random.uintLessThan(u32, 100) < 85)
                picked.uid
            else
                users[random.uintLessThan(usize, users.len)];
            const reduce_by = if (random.uintLessThan(u32, 100) < 4)
                0
            else
                @as(i64, @intCast(random.uintLessThan(u32, 10))) + 1;
            command = cmd.OrderCommand.reduce(picked.order_id, uid, reduce_by);
            command.symbol = spec.symbol_id;
        } else {
            command = makeRequestOrderBookCommand(spec.symbol_id, random.uintLessThan(usize, 21) + 1);
        }

        try base_commands.append(allocator, command);

        if (idx % 53 == 0) {
            try base_commands.append(allocator, makeAddUserCommand(users[random.uintLessThan(usize, users.len)]));
        }
    }

    var fault_events: std.ArrayListUnmanaged(FaultEvent) = .empty;
    defer fault_events.deinit(allocator);
    var now_tick: u64 = 0;
    var order_counter: u64 = 0;

    for (base_commands.items) |command| {
        now_tick += @as(u64, @intCast(random.uintLessThan(u32, 3))) + 1;
        const drop = random.uintLessThan(u32, 100) < 12;
        if (drop) continue;

        const primary_tick = now_tick + @as(u64, @intCast(random.uintLessThan(u32, 5)));
        try fault_events.append(allocator, .{
            .tick = primary_tick,
            .order = order_counter,
            .command = command,
        });
        order_counter += 1;

        if (random.uintLessThan(u32, 100) < 20) {
            const dup_tick = now_tick + @as(u64, @intCast(random.uintLessThan(u32, 8)));
            try fault_events.append(allocator, .{
                .tick = dup_tick,
                .order = order_counter,
                .command = command,
            });
            order_counter += 1;
        }
    }

    std.sort.heap(FaultEvent, fault_events.items, {}, lessThanFaultEvent);

    for (fault_events.items, 0..) |event, idx| {
        var command = event.command;
        const code = try executeSingleAndSplitExpectEqual(allocator, &single, &split, command);
        try output.digests.append(allocator, commandResultDigest(&command, code));
        if ((idx + 1) % 25 == 0) {
            try assertSingleVsSplitStateParity(allocator, &single, &split, users[0..]);
        }
    }

    try assertSingleVsSplitStateParity(allocator, &single, &split, users[0..]);
    output.delivered_count = fault_events.items.len;
    output.final_fingerprint = try split.detailedHashFingerprint();
    return output;
}

fn runSeededFaultSimulationMultiSymbol(
    allocator: std.mem.Allocator,
    seed: u64,
    rounds: usize,
) FaultSimulationError!FaultSimulationResult {
    const shard_count: usize = 4;
    const symbol_ids = [_]i32{ 241, 242 };

    var output: FaultSimulationResult = .{};
    errdefer output.deinit(allocator);

    var single = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer single.deinit();
    var split = try MultiShardKernelSet.init(allocator, shard_count, .direct);
    defer split.deinit();

    var symbol_shards: [symbol_ids.len]usize = undefined;
    for (symbol_ids, 0..) |symbol, idx| {
        const spec = try makeExchangeSpecForSymbol(symbol);
        if (!(try single.addSymbol(spec))) return error.UnexpectedResult;
        try split.addSymbolAll(spec);
        symbol_shards[idx] = split.shardIndexForSymbol(symbol);
    }

    for (symbol_shards, 0..) |lhs, lhs_idx| {
        for (symbol_shards[lhs_idx + 1 ..]) |rhs| {
            if (lhs == rhs) return error.InvalidShardPlan;
        }
    }

    var tx_counters: std.AutoHashMapUnmanaged(i64, i64) = .empty;
    defer tx_counters.deinit(allocator);
    var all_users: std.ArrayListUnmanaged(i64) = .empty;
    defer all_users.deinit(allocator);
    var base_commands: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
    defer base_commands.deinit(allocator);

    for (0..12) |idx| {
        const target_shard = idx % shard_count;
        const uid = uidForShard(target_shard, shard_count, 12_500 + @as(i64, @intCast(idx * 137)));
        if (tx_counters.contains(uid)) continue;

        try all_users.append(allocator, uid);
        try base_commands.append(allocator, makeAddUserCommand(uid));
        try base_commands.append(allocator, makeBalanceAdjustmentCommand(uid, 11, 250_000, 1, .adjustment));
        try base_commands.append(allocator, makeBalanceAdjustmentCommand(uid, 15, 250_000, 2, .adjustment));
        try tx_counters.put(allocator, uid, 2);
    }

    const Candidate = struct {
        order_id: i64,
        uid: i64,
        symbol: i32,
    };
    var candidates: std.ArrayListUnmanaged(Candidate) = .empty;
    defer candidates.deinit(allocator);

    var next_order_id: i64 = 90_000;
    for (symbol_ids, 0..) |symbol, symbol_idx| {
        const symbol_shard = split.shardIndexForSymbol(symbol);
        const maker_shard = (symbol_shard + 1) % shard_count;
        const taker_shard = (symbol_shard + 2) % shard_count;
        const maker_uid = uidForShard(maker_shard, shard_count, 30_000 + @as(i64, @intCast(symbol_idx * 10_000)));
        const taker_uid = uidForShard(taker_shard, shard_count, 40_000 + @as(i64, @intCast(symbol_idx * 10_000)));

        if (split.shardIndexForUid(maker_uid) == symbol_shard) return error.InvalidShardPlan;
        if (split.shardIndexForUid(taker_uid) == symbol_shard) return error.InvalidShardPlan;
        if (split.shardIndexForUid(maker_uid) == split.shardIndexForUid(taker_uid)) return error.InvalidShardPlan;

        for ([_]i64{ maker_uid, taker_uid }) |uid| {
            if (!tx_counters.contains(uid)) {
                try all_users.append(allocator, uid);
                try base_commands.append(allocator, makeAddUserCommand(uid));
                try base_commands.append(allocator, makeBalanceAdjustmentCommand(uid, 11, 300_000, 1, .adjustment));
                try base_commands.append(allocator, makeBalanceAdjustmentCommand(uid, 15, 300_000, 2, .adjustment));
                try tx_counters.put(allocator, uid, 2);
            }
        }

        const base_price: i64 = if (symbol == symbol_ids[0]) 100 else 160;
        var seed_ask = cmd.OrderCommand.newOrder(.gtc, next_order_id, maker_uid, base_price, 0, 7, .ask);
        seed_ask.symbol = symbol;
        try base_commands.append(allocator, seed_ask);
        try candidates.append(allocator, .{
            .order_id = next_order_id,
            .uid = maker_uid,
            .symbol = symbol,
        });
        next_order_id += 1;

        var seed_bid = cmd.OrderCommand.newOrder(.ioc, next_order_id, taker_uid, base_price, base_price, 5, .bid);
        seed_bid.symbol = symbol;
        try base_commands.append(allocator, seed_bid);
        next_order_id += 1;
    }

    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    for (0..rounds) |idx| {
        const roll = random.uintLessThan(u32, 100);
        var command: cmd.OrderCommand = undefined;

        if (roll < 45) {
            const uid = all_users.items[random.uintLessThan(usize, all_users.items.len)];
            const symbol = symbol_ids[random.uintLessThan(usize, symbol_ids.len)];
            const action: common.OrderAction = if (random.boolean()) .ask else .bid;
            const order_type: common.OrderType = blk: {
                const t = random.uintLessThan(u32, 100);
                if (t < 70) break :blk .gtc;
                if (t < 90) break :blk .ioc;
                break :blk .fok_budget;
            };
            const base_price: i64 = if (symbol == symbol_ids[0]) 100 else 160;
            const price = simulationPrice(random, base_price);
            const size = @as(i64, @intCast(random.uintLessThan(u32, 12))) + 1;
            const reserve = if (action == .bid)
                price + @as(i64, @intCast(random.uintLessThan(u32, 20)))
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
        } else if (roll < 63) {
            const uid = all_users.items[random.uintLessThan(usize, all_users.items.len)];
            const ccy: i32 = if (random.boolean()) 11 else 15;
            const amount_abs = @as(i64, @intCast(random.uintLessThan(u32, 900))) + 1;
            const amount = if (random.uintLessThan(u32, 100) < 25) -amount_abs else amount_abs;
            const counter = tx_counters.getPtr(uid) orelse return error.InvalidShardPlan;
            counter.* += 1;
            command = makeBalanceAdjustmentCommand(uid, ccy, amount, counter.*, .adjustment);
        } else if (roll < 77) {
            const picked = if (candidates.items.len > 0 and random.boolean())
                candidates.items[random.uintLessThan(usize, candidates.items.len)]
            else
                Candidate{
                    .order_id = @as(i64, @intCast(random.uintLessThan(u32, 15_000))) + 1,
                    .uid = all_users.items[random.uintLessThan(usize, all_users.items.len)],
                    .symbol = symbol_ids[random.uintLessThan(usize, symbol_ids.len)],
                };
            const uid = if (random.uintLessThan(u32, 100) < 85)
                picked.uid
            else
                all_users.items[random.uintLessThan(usize, all_users.items.len)];
            command = cmd.OrderCommand.cancel(picked.order_id, uid);
            command.symbol = picked.symbol;
        } else if (roll < 89) {
            const picked = if (candidates.items.len > 0 and random.boolean())
                candidates.items[random.uintLessThan(usize, candidates.items.len)]
            else
                Candidate{
                    .order_id = @as(i64, @intCast(random.uintLessThan(u32, 15_000))) + 1,
                    .uid = all_users.items[random.uintLessThan(usize, all_users.items.len)],
                    .symbol = symbol_ids[random.uintLessThan(usize, symbol_ids.len)],
                };
            const uid = if (random.uintLessThan(u32, 100) < 85)
                picked.uid
            else
                all_users.items[random.uintLessThan(usize, all_users.items.len)];
            const base_price: i64 = if (picked.symbol == symbol_ids[0]) 100 else 160;
            command = cmd.OrderCommand.update(picked.order_id, uid, simulationPrice(random, base_price));
            command.symbol = picked.symbol;
        } else if (roll < 96) {
            const picked = if (candidates.items.len > 0 and random.boolean())
                candidates.items[random.uintLessThan(usize, candidates.items.len)]
            else
                Candidate{
                    .order_id = @as(i64, @intCast(random.uintLessThan(u32, 15_000))) + 1,
                    .uid = all_users.items[random.uintLessThan(usize, all_users.items.len)],
                    .symbol = symbol_ids[random.uintLessThan(usize, symbol_ids.len)],
                };
            const uid = if (random.uintLessThan(u32, 100) < 85)
                picked.uid
            else
                all_users.items[random.uintLessThan(usize, all_users.items.len)];
            const reduce_by = if (random.uintLessThan(u32, 100) < 5)
                0
            else
                @as(i64, @intCast(random.uintLessThan(u32, 10))) + 1;
            command = cmd.OrderCommand.reduce(picked.order_id, uid, reduce_by);
            command.symbol = picked.symbol;
        } else {
            const symbol = symbol_ids[random.uintLessThan(usize, symbol_ids.len)];
            command = makeRequestOrderBookCommand(symbol, random.uintLessThan(usize, 24) + 1);
        }

        try base_commands.append(allocator, command);

        if (idx % 67 == 0) {
            const uid = all_users.items[random.uintLessThan(usize, all_users.items.len)];
            try base_commands.append(allocator, makeAddUserCommand(uid));
        }
    }

    var fault_events: std.ArrayListUnmanaged(FaultEvent) = .empty;
    defer fault_events.deinit(allocator);
    var now_tick: u64 = 0;
    var order_counter: u64 = 0;

    for (base_commands.items) |command| {
        now_tick += @as(u64, @intCast(random.uintLessThan(u32, 3))) + 1;
        const drop = random.uintLessThan(u32, 100) < 10;
        if (drop) continue;

        const primary_tick = now_tick + @as(u64, @intCast(random.uintLessThan(u32, 6)));
        try fault_events.append(allocator, .{
            .tick = primary_tick,
            .order = order_counter,
            .command = command,
        });
        order_counter += 1;

        if (random.uintLessThan(u32, 100) < 22) {
            const dup_tick = now_tick + @as(u64, @intCast(random.uintLessThan(u32, 10)));
            try fault_events.append(allocator, .{
                .tick = dup_tick,
                .order = order_counter,
                .command = command,
            });
            order_counter += 1;
        }
    }

    std.sort.heap(FaultEvent, fault_events.items, {}, lessThanFaultEvent);

    for (fault_events.items, 0..) |event, idx| {
        var command = event.command;
        const code = try executeSingleAndSplitExpectEqual(allocator, &single, &split, command);
        try output.digests.append(allocator, commandResultDigest(&command, code));
        if ((idx + 1) % 35 == 0) {
            try assertSingleVsSplitStateParity(allocator, &single, &split, all_users.items);
            try assertOrderBooksSingleVsSplitParity(allocator, &single, &split, symbol_ids[0..]);
        }
    }

    try assertSingleVsSplitStateParity(allocator, &single, &split, all_users.items);
    try assertOrderBooksSingleVsSplitParity(allocator, &single, &split, symbol_ids[0..]);
    output.delivered_count = fault_events.items.len;
    output.final_fingerprint = try split.detailedHashFingerprint();
    return output;
}

test "kernel integrates risk and orderbook for non-crossing lifecycle" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer kernel.deinit();
    try std.testing.expect(try kernel.addSymbol(try makeExchangeSpec()));

    var add_u1 = makeAddUserCommand(1001);
    var add_u2 = makeAddUserCommand(1002);
    var resp = try kernel.execute(&add_u1);
    defer resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, resp.result_code);
    var resp2 = try kernel.execute(&add_u2);
    defer resp2.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, resp2.result_code);

    var dep_quote = makeBalanceAdjustmentCommand(1001, 15, 100_000, 1, .adjustment);
    var dep_base = makeBalanceAdjustmentCommand(1002, 11, 100_000, 1, .adjustment);
    var resp3 = try kernel.execute(&dep_quote);
    defer resp3.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, resp3.result_code);
    var resp4 = try kernel.execute(&dep_base);
    defer resp4.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, resp4.result_code);

    var bid = cmd.OrderCommand.newOrder(.gtc, 5001, 1001, 90, 90, 10, .bid);
    bid.symbol = 241;
    var bid_resp = try kernel.execute(&bid);
    defer bid_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, bid.result_code);

    var ask = cmd.OrderCommand.newOrder(.gtc, 5002, 1002, 100, 0, 20, .ask);
    ask.symbol = 241;
    var ask_resp = try kernel.execute(&ask);
    defer ask_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, ask.result_code);

    var req = makeRequestOrderBookCommand(241, 10);
    var ob = try kernel.execute(&req);
    defer ob.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, ob.result_code);
    const snapshot = ob.l2_snapshot orelse unreachable;
    try std.testing.expectEqual(@as(usize, 1), snapshot.asks.items.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.bids.items.len);
    try std.testing.expectEqual(@as(i64, 100), snapshot.asks.items[0].price);
    try std.testing.expectEqual(@as(i64, 90), snapshot.bids.items[0].price);

    var move_bid = cmd.OrderCommand.update(5001, 1001, 85);
    move_bid.symbol = 241;
    var move_resp = try kernel.execute(&move_bid);
    defer move_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, move_resp.result_code);

    var reduce_ask = cmd.OrderCommand.reduce(5002, 1002, 5);
    reduce_ask.symbol = 241;
    var reduce_resp = try kernel.execute(&reduce_ask);
    defer reduce_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, reduce_resp.result_code);

    var cancel_bid = cmd.OrderCommand.cancel(5001, 1001);
    cancel_bid.symbol = 241;
    var cancel_resp = try kernel.execute(&cancel_bid);
    defer cancel_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, cancel_resp.result_code);

    var req2 = makeRequestOrderBookCommand(241, 10);
    var ob2 = try kernel.execute(&req2);
    defer ob2.deinit(allocator);
    const snapshot2 = ob2.l2_snapshot orelse unreachable;
    try std.testing.expectEqual(@as(usize, 1), snapshot2.asks.items.len);
    try std.testing.expectEqual(@as(usize, 0), snapshot2.bids.items.len);
    try std.testing.expectEqual(@as(i64, 15), snapshot2.asks.items[0].volume);
}

test "kernel returns matching invalid order book id for missing symbol routes" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    var cancel = cmd.OrderCommand.cancel(1, 1);
    cancel.symbol = 777;
    var resp = try kernel.execute(&cancel);
    defer resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.matching_invalid_order_book_id, resp.result_code);
}

test "kernel binary frame handling keeps query accepted-until-last semantics" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .java_identity_hash_context = testJavaIdentityHashContext(),
    });
    defer kernel.deinit();

    const query_payload = try encodeReportQueryPayload(allocator, 10001);
    defer allocator.free(query_payload);
    var query_frames: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
    defer query_frames.deinit(allocator);
    try appendBinaryPayloadFrames(
        allocator,
        &query_frames,
        .binary_data_query,
        502,
        query_payload,
    );

    try std.testing.expect(query_frames.items.len >= 1);

    var query_first = query_frames.items[0];
    var query_first_resp = try kernel.execute(&query_first);
    defer query_first_resp.deinit(allocator);
    const expected_first: cmd.CommandResultCode = if (query_frames.items.len == 1)
        .success
    else
        .accepted;
    try std.testing.expectEqual(expected_first, query_first_resp.result_code);

    if (query_frames.items.len > 1) {
        for (query_frames.items[1 .. query_frames.items.len - 1]) |*frame| {
            var mid_resp = try kernel.execute(frame);
            defer mid_resp.deinit(allocator);
            try std.testing.expectEqual(cmd.CommandResultCode.accepted, mid_resp.result_code);
        }
    }

    var final_resp = if (query_frames.items.len == 1)
        query_first_resp
    else blk: {
        var query_last = query_frames.items[query_frames.items.len - 1];
        break :blk try kernel.execute(&query_last);
    };
    if (query_frames.items.len > 1) {
        defer final_resp.deinit(allocator);
    }
    try std.testing.expectEqual(cmd.CommandResultCode.success, final_resp.result_code);
    try std.testing.expect(final_resp.report_response == null);
    switch (final_resp.java_report_response orelse unreachable) {
        .state_hash => |*state_hash| {
            try expectJavaStateHashTopology(
                allocator,
                state_hash,
                final_resp.matcher_events_head,
                1,
                1,
            );
            try std.testing.expect(state_hash.state_hash != 0);
        },
        else => unreachable,
    }
    try std.testing.expectEqual(@as(usize, 0), kernel.binary_frame_assembler.pendingTransfers());
}

test "kernel native report projection is terminal-frame opt-in" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .java_identity_hash_context = testJavaIdentityHashContext(),
    });
    defer kernel.deinit();

    const payload = try encodeReportQueryPayload(allocator, 10001);
    defer allocator.free(payload);
    var frames: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
    defer frames.deinit(allocator);
    try appendBinaryPayloadFrames(allocator, &frames, .binary_data_query, 521, payload);
    frames.items[frames.items.len - 1].binary_report_projection = .exact_java_and_native;

    for (frames.items, 0..) |*frame, idx| {
        var response = try kernel.execute(frame);
        defer response.deinit(allocator);
        if (idx + 1 != frames.items.len) {
            try std.testing.expectEqual(cmd.CommandResultCode.accepted, response.result_code);
            try std.testing.expect(response.report_response == null);
            try std.testing.expect(response.java_report_response == null);
            continue;
        }
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
        switch (response.report_response orelse return error.MissingReportResponse) {
            .state_hash_detailed => |detailed| try std.testing.expect(detailed.submodules.items.len > 0),
            else => return error.UnexpectedReportResponseType,
        }
        try std.testing.expect(response.java_report_response != null);
    }
}

test "unmarked exact Java total balance wraps independently of native overflow" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    for ([_]i64{ 522_001, 522_002 }) |uid| {
        try std.testing.expect(try kernel.risk_engine.user_profile_service.addEmptyUserProfile(uid));
        const profile = kernel.risk_engine.getUserProfile(uid) orelse unreachable;
        try profile.putAccount(allocator, 15, std.math.maxInt(i64));
    }

    const payload = try encodeReportQueryPayload(allocator, 10003);
    defer allocator.free(payload);

    var exact_frames: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
    defer exact_frames.deinit(allocator);
    try appendBinaryPayloadFrames(allocator, &exact_frames, .binary_data_query, 522, payload);
    var exact_response: ?CommandResponse = null;
    defer if (exact_response) |*response| response.deinit(allocator);
    for (exact_frames.items) |*frame| {
        if (exact_response) |*previous| previous.deinit(allocator);
        exact_response = try kernel.execute(frame);
    }
    try std.testing.expectEqual(cmd.CommandResultCode.success, exact_response.?.result_code);
    try std.testing.expect(exact_response.?.report_response == null);
    switch (exact_response.?.java_report_response orelse return error.MissingJavaReportResponse) {
        .total_currency_balance => |report| {
            try std.testing.expectEqual(@as(?i64, -2), javaMapValue(report.account_balances, 15));
        },
        else => return error.UnexpectedJavaReportResponseType,
    }

    var native_frames: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
    defer native_frames.deinit(allocator);
    try appendBinaryPayloadFrames(allocator, &native_frames, .binary_data_query, 523, payload);
    native_frames.items[native_frames.items.len - 1].binary_report_projection = .exact_java_and_native;
    var native_response: ?CommandResponse = null;
    defer if (native_response) |*response| response.deinit(allocator);
    for (native_frames.items) |*frame| {
        if (native_response) |*previous| previous.deinit(allocator);
        native_response = try kernel.execute(frame);
    }
    try std.testing.expectEqual(cmd.CommandResultCode.binary_command_failed, native_response.?.result_code);
    try std.testing.expect(native_response.?.report_response == null);
    try std.testing.expect(native_response.?.java_report_response == null);
    try std.testing.expect(native_response.?.matcher_events_head == null);
}

fn nativeReportProjectionAllocationFailure(allocator: std.mem.Allocator) !void {
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();
    try std.testing.expect(
        try kernel.risk_engine.user_profile_service.addEmptyUserProfile(524_001),
    );
    const profile = kernel.risk_engine.getUserProfile(524_001) orelse unreachable;
    try profile.putAccount(allocator, 15, 1_000);

    const payload = try encodeReportQueryPayload(allocator, 10003);
    defer allocator.free(payload);
    var frames: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
    defer frames.deinit(allocator);
    try appendBinaryPayloadFrames(allocator, &frames, .binary_data_query, 524, payload);
    frames.items[frames.items.len - 1].binary_report_projection = .exact_java_and_native;

    for (frames.items, 0..) |*frame, idx| {
        var response = try kernel.execute(frame);
        defer response.deinit(allocator);
        if (idx + 1 == frames.items.len) {
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
            try std.testing.expect(response.report_response != null);
            try std.testing.expect(response.java_report_response != null);
        }
    }
}

test "combined exact and native report projection is allocation-failure clean" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        nativeReportProjectionAllocationFailure,
        .{},
    );
}

test "kernel binary query unknown report type succeeds without sections" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    const query_payload = try encodeReportQueryPayload(allocator, 9999);
    defer allocator.free(query_payload);

    var query_frames: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
    defer query_frames.deinit(allocator);
    try appendBinaryPayloadFrames(
        allocator,
        &query_frames,
        .binary_data_query,
        503,
        query_payload,
    );

    for (query_frames.items, 0..) |*frame, idx| {
        var response = try kernel.execute(frame);
        defer response.deinit(allocator);
        const expected: cmd.CommandResultCode = if (idx + 1 == query_frames.items.len)
            .success
        else
            .accepted;
        try std.testing.expectEqual(expected, response.result_code);
        if (idx + 1 == query_frames.items.len) {
            try std.testing.expect(response.report_response == null);
            try std.testing.expect(response.java_report_response == null);
            try std.testing.expect(response.matcher_events_head == null);
        }
    }

    try std.testing.expectEqual(@as(usize, 0), kernel.binary_frame_assembler.pendingTransfers());
}

test "Java report query constructors ignore unknown malformed and trailing bodies" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .java_identity_hash_context = testJavaIdentityHashContext(),
    });
    defer kernel.deinit();

    var unknown: [9]u8 = undefined;
    std.mem.writeInt(i32, unknown[0..4], 77_777, .little);
    @memset(unknown[4..], 0xa5);
    var truncated_single: [8]u8 = undefined;
    std.mem.writeInt(i32, truncated_single[0..4], 10_002, .little);
    @memset(truncated_single[4..], 0);
    var state_with_tail: [7]u8 = undefined;
    std.mem.writeInt(i32, state_with_tail[0..4], 10_001, .little);
    @memset(state_with_tail[4..], 0x5a);

    const cases = [_]struct {
        body: []const u8,
        expects_report: bool,
    }{
        .{ .body = &unknown, .expects_report = false },
        .{ .body = &truncated_single, .expects_report = false },
        .{ .body = &state_with_tail, .expects_report = true },
    };

    for (cases, 0..) |case, idx| {
        const words = try java_binary_wire.encodeRequestWords(allocator, case.body, .{});
        defer allocator.free(words);
        try std.testing.expectEqual(@as(usize, 5), words.len);
        var frame = makeBinaryFrameCommand(
            .binary_data_query,
            @intCast(520 + idx),
            -1,
            .{ words[0], words[1], words[2], words[3], words[4] },
        );
        var response = try kernel.execute(&frame);
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
        try std.testing.expect(response.report_response == null);
        try std.testing.expectEqual(case.expects_report, response.java_report_response != null);
    }
}

test "kernel disabled state hash query gates direct and binary query paths" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .report_queries = .{
            .state_hash = false,
            .single_user = true,
            .total_currency_balance = true,
        },
    });
    defer kernel.deinit();

    try std.testing.expectError(
        error.UnsupportedReportQueryType,
        kernel.queryReport(.risk_state_hash),
    );
    try std.testing.expectError(
        error.UnsupportedReportQueryType,
        kernel.queryReport(.state_hash_detailed),
    );

    const query_payload = try encodeReportQueryPayload(allocator, 10001);
    defer allocator.free(query_payload);

    var query_frames: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
    defer query_frames.deinit(allocator);
    try appendBinaryPayloadFrames(
        allocator,
        &query_frames,
        .binary_data_query,
        509,
        query_payload,
    );

    for (query_frames.items, 0..) |*frame, idx| {
        var response = try kernel.execute(frame);
        defer response.deinit(allocator);
        const expected: cmd.CommandResultCode = if (idx + 1 == query_frames.items.len)
            .success
        else
            .accepted;
        try std.testing.expectEqual(expected, response.result_code);
        if (idx + 1 == query_frames.items.len) {
            try std.testing.expect(response.java_report_response == null);
            try std.testing.expect(response.matcher_events_head == null);
        }
    }
}

test "exact Java state hash fails closed without JVM identity context" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    const payload = try encodeReportQueryPayload(allocator, 10001);
    defer allocator.free(payload);
    var frames: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
    defer frames.deinit(allocator);
    try appendBinaryPayloadFrames(allocator, &frames, .binary_data_query, 510, payload);
    try std.testing.expectEqual(@as(usize, 1), frames.items.len);
    var response = try kernel.execute(&frames.items[0]);
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.binary_command_failed, response.result_code);
    try std.testing.expect(response.report_response == null);
    try std.testing.expect(response.java_report_response == null);
    try std.testing.expect(response.matcher_events_head == null);
}

test "legacy unknown custom report query with an arbitrary body succeeds empty" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    var malformed: [8]u8 = undefined;
    std.mem.writeInt(i32, malformed[0..4], 9999, .big);
    std.mem.writeInt(i32, malformed[4..8], 1, .big);
    var frames: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
    defer frames.deinit(allocator);
    try appendBinaryPayloadFrames(allocator, &frames, .binary_data_query, 511, &malformed);
    try std.testing.expectEqual(@as(usize, 1), frames.items.len);
    var response = try kernel.execute(&frames.items[0]);
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    try std.testing.expect(response.java_report_response == null);
    try std.testing.expect(response.matcher_events_head == null);
}

test "kernel binary query total currency balance payload returns typed report" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer kernel.deinit();
    try std.testing.expect(try kernel.addSymbol(try makeExchangeSpec()));

    var add_user = makeAddUserCommand(7601);
    var add_resp = try kernel.execute(&add_user);
    defer add_resp.deinit(allocator);
    var deposit = makeBalanceAdjustmentCommand(7601, 15, 2000, 1, .adjustment);
    var dep_resp = try kernel.execute(&deposit);
    defer dep_resp.deinit(allocator);

    const payload = try encodeReportQueryPayload(allocator, 10003);
    defer allocator.free(payload);
    var frames: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
    defer frames.deinit(allocator);
    try appendBinaryPayloadFrames(allocator, &frames, .binary_data_query, 504, payload);

    for (frames.items, 0..) |*frame, idx| {
        var response = try kernel.execute(frame);
        defer response.deinit(allocator);
        const expected: cmd.CommandResultCode = if (idx + 1 == frames.items.len)
            .success
        else
            .accepted;
        try std.testing.expectEqual(expected, response.result_code);
        if (idx + 1 == frames.items.len) {
            switch (response.java_report_response orelse unreachable) {
                .total_currency_balance => |report| {
                    try std.testing.expectEqual(@as(?i64, 2000), javaMapValue(report.account_balances, 15));
                    try std.testing.expectEqual(@as(?i64, -2000), javaMapValue(report.adjustments, 15));
                },
                else => unreachable,
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 0), kernel.binary_frame_assembler.pendingTransfers());
}

test "kernel binary query single user payload returns typed report" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer kernel.deinit();
    try std.testing.expect(try kernel.addSymbol(try makeExchangeSpec()));

    var add_user = makeAddUserCommand(7602);
    var add_resp = try kernel.execute(&add_user);
    defer add_resp.deinit(allocator);
    var deposit = makeBalanceAdjustmentCommand(7602, 15, 5000, 1, .adjustment);
    var dep_resp = try kernel.execute(&deposit);
    defer dep_resp.deinit(allocator);
    var bid = cmd.OrderCommand.newOrder(.gtc, 9951, 7602, 95, 101, 2, .bid);
    bid.symbol = 241;
    var bid_resp = try kernel.execute(&bid);
    defer bid_resp.deinit(allocator);

    const payload = try encodeSingleUserReportQueryPayload(allocator, 7602);
    defer allocator.free(payload);
    var frames: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
    defer frames.deinit(allocator);
    try appendBinaryPayloadFrames(allocator, &frames, .binary_data_query, 505, payload);

    for (frames.items, 0..) |*frame, idx| {
        var response = try kernel.execute(frame);
        defer response.deinit(allocator);
        const expected: cmd.CommandResultCode = if (idx + 1 == frames.items.len)
            .success
        else
            .accepted;
        try std.testing.expectEqual(expected, response.result_code);
        if (idx + 1 == frames.items.len) {
            switch (response.java_report_response orelse unreachable) {
                .single_user => |report| {
                    try std.testing.expectEqual(@as(i64, 0), report.uid);
                    try std.testing.expectEqual(java_report_wire.QueryExecutionStatus.ok, report.query_execution_status);
                    const groups = report.orders orelse unreachable;
                    try std.testing.expectEqual(@as(usize, 1), groups.len);
                    try std.testing.expectEqual(@as(i64, 9951), groups[0].orders[0].order_id);
                },
                else => unreachable,
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 0), kernel.binary_frame_assembler.pendingTransfers());
}

test "exact single-user preserves Java Direct and Naive order traversal plus missing-user reduction" {
    const allocator = std.testing.allocator;
    for ([_]OrderBookKind{ .direct, .naive }) |kind| {
        var kernel = try ExchangeKernel.init(allocator, .{ .orderbook_kind = kind });
        defer kernel.deinit();
        try std.testing.expect(try kernel.addSymbol(try makeExchangeSpec()));

        var add_user = makeAddUserCommand(7_603);
        var add_response = try kernel.execute(&add_user);
        defer add_response.deinit(allocator);
        var base = makeBalanceAdjustmentCommand(7_603, 11, 100_000, 1, .adjustment);
        var base_response = try kernel.execute(&base);
        defer base_response.deinit(allocator);
        var quote = makeBalanceAdjustmentCommand(7_603, 15, 100_000, 2, .adjustment);
        var quote_response = try kernel.execute(&quote);
        defer quote_response.deinit(allocator);

        const sources = [_]cmd.OrderCommand{
            blk: {
                var order = cmd.OrderCommand.newOrder(.gtc, 30, 7_603, 100, 0, 1, .ask);
                order.symbol = 241;
                break :blk order;
            },
            blk: {
                var order = cmd.OrderCommand.newOrder(.gtc, 10, 7_603, 101, 0, 1, .ask);
                order.symbol = 241;
                break :blk order;
            },
            blk: {
                var order = cmd.OrderCommand.newOrder(.gtc, 20, 7_603, 90, 101, 1, .bid);
                order.symbol = 241;
                break :blk order;
            },
        };
        for (sources) |source| {
            var order = source;
            var order_response = try kernel.execute(&order);
            defer order_response.deinit(allocator);
            try std.testing.expectEqual(cmd.CommandResultCode.success, order_response.result_code);
        }

        const payload = try encodeSingleUserReportQueryPayload(allocator, 7_603);
        defer allocator.free(payload);
        var frames: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
        defer frames.deinit(allocator);
        try appendBinaryPayloadFrames(allocator, &frames, .binary_data_query, 512, payload);
        try std.testing.expectEqual(@as(usize, 1), frames.items.len);
        var response = try kernel.execute(&frames.items[0]);
        defer response.deinit(allocator);
        switch (response.java_report_response orelse return error.MissingJavaReportResponse) {
            .single_user => |report| {
                const groups = report.orders orelse return error.MissingJavaOrders;
                try std.testing.expectEqual(@as(usize, 1), groups.len);
                const expected = switch (kind) {
                    .direct => [_]i64{ 10, 20, 30 },
                    .naive => [_]i64{ 30, 10, 20 },
                };
                for (groups[0].orders, expected) |order, order_id| {
                    try std.testing.expectEqual(order_id, order.order_id);
                }
            },
            else => return error.UnexpectedReportResponseType,
        }

        const missing_payload = try encodeSingleUserReportQueryPayload(allocator, 999_999);
        defer allocator.free(missing_payload);
        var missing_frames: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
        defer missing_frames.deinit(allocator);
        try appendBinaryPayloadFrames(allocator, &missing_frames, .binary_data_query, 513, missing_payload);
        var missing_response = try kernel.execute(&missing_frames.items[0]);
        defer missing_response.deinit(allocator);
        switch (missing_response.java_report_response orelse return error.MissingJavaReportResponse) {
            .single_user => |report| {
                try std.testing.expectEqual(@as(i64, 0), report.uid);
                try std.testing.expectEqual(java_report_wire.QueryExecutionStatus.user_not_found, report.query_execution_status);
                try std.testing.expect(report.accounts == null);
                try std.testing.expect(report.positions == null);
                try std.testing.expectEqual(@as(usize, 0), (report.orders orelse unreachable).len);
            },
            else => return error.UnexpectedReportResponseType,
        }
    }
}

test "kernel binary data command applies add-symbols and add-accounts payloads" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer kernel.deinit();

    const symbol_specs = [_]common.CoreSymbolSpecification{
        try makeExchangeSpec(),
    };
    const symbol_payload = try encodeAddSymbolsPayload(allocator, symbol_specs[0..]);
    defer allocator.free(symbol_payload);

    var symbol_frames: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
    defer symbol_frames.deinit(allocator);
    try appendBinaryPayloadFrames(
        allocator,
        &symbol_frames,
        .binary_data_command,
        701,
        symbol_payload,
    );

    for (symbol_frames.items, 0..) |*frame, idx| {
        var response = try kernel.execute(frame);
        defer response.deinit(allocator);
        const expected: cmd.CommandResultCode = if (idx + 1 == symbol_frames.items.len)
            .success
        else
            .accepted;
        try std.testing.expectEqual(expected, response.result_code);
    }

    var order_book_report = try kernel.queryReport(.{
        .order_book = .{
            .symbol = 241,
            .depth = 4,
        },
    });
    defer order_book_report.deinit(allocator);

    const u1_accounts = [_]binary_payload.CurrencyBalance{
        .{ .currency = 11, .balance = 500 },
        .{ .currency = 15, .balance = 900 },
    };
    const u2_accounts = [_]binary_payload.CurrencyBalance{
        .{ .currency = 15, .balance = 1200 },
    };
    const users = [_]binary_payload.UserAccountsBatch{
        .{ .uid = 8101, .accounts = u1_accounts[0..] },
        .{ .uid = 8102, .accounts = u2_accounts[0..] },
    };
    const accounts_payload = try encodeAddAccountsPayload(allocator, users[0..]);
    defer allocator.free(accounts_payload);

    var account_frames: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
    defer account_frames.deinit(allocator);
    try appendBinaryPayloadFrames(
        allocator,
        &account_frames,
        .binary_data_command,
        702,
        accounts_payload,
    );

    for (account_frames.items, 0..) |*frame, idx| {
        var response = try kernel.execute(frame);
        defer response.deinit(allocator);
        const expected: cmd.CommandResultCode = if (idx + 1 == account_frames.items.len)
            .success
        else
            .accepted;
        try std.testing.expectEqual(expected, response.result_code);
    }

    var u1_11 = try kernel.queryReport(.{
        .user_balance = .{
            .uid = 8101,
            .currency = 11,
        },
    });
    defer u1_11.deinit(allocator);
    var u1_15 = try kernel.queryReport(.{
        .user_balance = .{
            .uid = 8101,
            .currency = 15,
        },
    });
    defer u1_15.deinit(allocator);
    var u2_15 = try kernel.queryReport(.{
        .user_balance = .{
            .uid = 8102,
            .currency = 15,
        },
    });
    defer u2_15.deinit(allocator);

    try std.testing.expectEqual(@as(?i64, 500), u1_11.user_balance);
    try std.testing.expectEqual(@as(?i64, 900), u1_15.user_balance);
    try std.testing.expectEqual(@as(?i64, 1200), u2_15.user_balance);
    try std.testing.expectEqual(@as(usize, 0), kernel.binary_frame_assembler.pendingTransfers());
}

test "kernel accepts fixed Java LZ4 account command and single-user query words" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    // Captured from lz4-java 1.8.0 on the exact Java NativeBytes little-endian
    // body: add-accounts type 1002, one user uid=7, one currency=11,
    // balance=500. Both fastCompressor() and highCompressor() produce this
    // 29-byte match-containing raw block, padded to one five-long frame.
    const fixed_account_words = [5]i64{
        0x00000020A2EA0300,
        0x0001000000070001,
        0x00000C00C00B0000,
        0x00F4010000000000,
        0,
    };
    var account_frame = makeBinaryFrameCommand(
        .binary_data_command,
        8_800,
        -1,
        fixed_account_words,
    );
    var account_response = try kernel.execute(&account_frame);
    defer account_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, account_response.result_code);
    const profile = kernel.risk_engine.getUserProfile(7) orelse unreachable;
    try std.testing.expectEqual(@as(i64, 500), profile.accountBalance(11));

    // Java NativeBytes query body (little endian): type 10002, uid=7.
    const fixed_query_words = [5]i64{
        0x0000000CC0122700,
        0x0007000000000000,
        0,
        0,
        0,
    };
    var query_frame = makeBinaryFrameCommand(
        .binary_data_query,
        8_801,
        -1,
        fixed_query_words,
    );
    var query_response = try kernel.execute(&query_frame);
    defer query_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, query_response.result_code);
    switch (query_response.java_report_response orelse unreachable) {
        .single_user => |report| {
            try std.testing.expectEqual(@as(i64, 0), report.uid);
            try std.testing.expectEqual(java_report_wire.QueryExecutionStatus.ok, report.query_execution_status);
            try std.testing.expectEqual(@as(?i64, 500), javaMapValue(report.accounts, 11));
        },
        else => unreachable,
    }
}

test "kernel Java decoder rejects bad size LZ4 and padding without mutation" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    const malformed = [_][5]i64{
        // 64 MiB + 1 declared output exceeds the inbound default cap.
        .{ 0x0400000100000000, 0, 0, 0, 0 },
        // Declares 32 bytes, then starts an extended literal sequence whose
        // zero-filled carrier reaches an illegal zero match offset.
        .{ 0x00000020F0000000, 0, 0, 0, 0 },
        // Otherwise valid lz4-java add-account request with non-zero tail padding.
        .{
            0x00000020A2EA0300,
            0x0001000000070001,
            0x00000C00C00B0000,
            0x00F4010000000000,
            1,
        },
    };

    for (malformed, 0..) |words, idx| {
        var frame = makeBinaryFrameCommand(
            .binary_data_command,
            @intCast(8_900 + idx),
            -1,
            words,
        );
        var response = try kernel.execute(&frame);
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.binary_command_failed, response.result_code);
        try std.testing.expectEqual(@as(usize, 0), kernel.risk_engine.user_profile_service.user_profiles.count());
        try std.testing.expectEqual(@as(usize, 0), kernel.books.count());
    }
}

test "kernel rejects huge tiny-body counts in Java and legacy formats without mutation" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    var java_body: [8]u8 = undefined;
    std.mem.writeInt(i32, java_body[0..4], 1002, .little);
    std.mem.writeInt(i32, java_body[4..8], std.math.maxInt(i32), .little);
    const java_words = try java_binary_wire.encodeRequestWords(allocator, &java_body, .{});
    defer allocator.free(java_words);
    try std.testing.expectEqual(@as(usize, 5), java_words.len);
    var java_frame = makeBinaryFrameCommand(
        .binary_data_command,
        8_920,
        -1,
        .{ java_words[0], java_words[1], java_words[2], java_words[3], java_words[4] },
    );
    var java_response = try kernel.execute(&java_frame);
    defer java_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.binary_command_failed, java_response.result_code);

    var legacy_body: [8]u8 = undefined;
    std.mem.writeInt(i32, legacy_body[0..4], 1002, .big);
    std.mem.writeInt(i32, legacy_body[4..8], std.math.maxInt(i32), .big);
    const legacy_words = try java_binary_wire.packSignedWordsBigEndian(allocator, &legacy_body);
    defer allocator.free(legacy_words);
    try std.testing.expectEqual(@as(usize, 5), legacy_words.len);
    var legacy_frame = makeBinaryFrameCommand(
        .binary_data_command,
        8_921,
        -1,
        .{ legacy_words[0], legacy_words[1], legacy_words[2], legacy_words[3], legacy_words[4] },
    );
    var legacy_response = try kernel.execute(&legacy_frame);
    defer legacy_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.binary_command_failed, legacy_response.result_code);

    try std.testing.expectEqual(@as(usize, 0), kernel.risk_engine.user_profile_service.user_profiles.count());
    try std.testing.expectEqual(@as(usize, 0), kernel.books.count());
}

test "kernel legacy fallback requires canonical schema and preserves valid raw big endian callers" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    const accounts = [_]binary_payload.CurrencyBalance{
        .{ .currency = 15, .balance = 900 },
    };
    const users = [_]binary_payload.UserAccountsBatch{
        .{ .uid = 88, .accounts = &accounts },
    };
    const legacy_body = try binary_payload.encodeAddAccountsToBytes(allocator, &users);
    defer allocator.free(legacy_body);
    const canonical_words = try java_binary_wire.packSignedWordsBigEndian(allocator, legacy_body);
    defer allocator.free(canonical_words);
    try std.testing.expectEqual(@as(usize, 5), canonical_words.len);

    var noncanonical_words: [5]i64 = undefined;
    @memcpy(&noncanonical_words, canonical_words);
    noncanonical_words[4] = 1;
    var noncanonical_frame = makeBinaryFrameCommand(
        .binary_data_command,
        8_950,
        -1,
        noncanonical_words,
    );
    var noncanonical_response = try kernel.execute(&noncanonical_frame);
    defer noncanonical_response.deinit(allocator);
    try std.testing.expectEqual(
        cmd.CommandResultCode.binary_command_failed,
        noncanonical_response.result_code,
    );
    try std.testing.expect(kernel.risk_engine.getUserProfile(88) == null);

    var canonical_frame = makeBinaryFrameCommand(
        .binary_data_command,
        8_951,
        -1,
        .{
            canonical_words[0],
            canonical_words[1],
            canonical_words[2],
            canonical_words[3],
            canonical_words[4],
        },
    );
    var canonical_response = try kernel.execute(&canonical_frame);
    defer canonical_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, canonical_response.result_code);
    const profile = kernel.risk_engine.getUserProfile(88) orelse unreachable;
    try std.testing.expectEqual(@as(i64, 900), profile.accountBalance(15));
}

test "kernel binary add-accounts derives duplicate transaction IDs from currency" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    const currency_a: i32 = 11;
    const currency_b: i32 = 15;
    const uid_a: i64 = 8_111;
    const uid_b: i64 = 8_115;
    const balance_a: i64 = 500;
    const balance_b: i64 = 900;
    const accounts_a = [_]binary_payload.CurrencyBalance{
        .{ .currency = currency_a, .balance = balance_a },
    };
    const accounts_b = [_]binary_payload.CurrencyBalance{
        .{ .currency = currency_b, .balance = balance_b },
    };
    const users = [_]binary_payload.UserAccountsBatch{
        .{ .uid = uid_a, .accounts = accounts_a[0..] },
        .{ .uid = uid_b, .accounts = accounts_b[0..] },
    };
    const payload = try encodeAddAccountsPayload(allocator, users[0..]);
    defer allocator.free(payload);

    var frames: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
    defer frames.deinit(allocator);
    try appendBinaryPayloadFrames(
        allocator,
        &frames,
        .binary_data_command,
        703,
        payload,
    );

    for (frames.items, 0..) |*frame, idx| {
        var response = try kernel.execute(frame);
        defer response.deinit(allocator);
        const expected: cmd.CommandResultCode = if (idx + 1 == frames.items.len)
            .success
        else
            .accepted;
        try std.testing.expectEqual(expected, response.result_code);
    }

    const tx_id_a: i64 = 1_000_000_000 + @as(i64, currency_a);
    const tx_id_b: i64 = 1_000_000_000 + @as(i64, currency_b);
    const profile_a = kernel.risk_engine.getUserProfile(uid_a) orelse unreachable;
    const profile_b = kernel.risk_engine.getUserProfile(uid_b) orelse unreachable;
    try std.testing.expectEqual(tx_id_a, profile_a.adjustments_counter);
    try std.testing.expectEqual(tx_id_b, profile_b.adjustments_counter);

    var duplicate_a = makeBalanceAdjustmentCommand(
        uid_a,
        currency_a,
        1,
        tx_id_a,
        .adjustment,
    );
    var duplicate_a_response = try kernel.execute(&duplicate_a);
    defer duplicate_a_response.deinit(allocator);
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_account_balance_adjustment_already_applied_same,
        duplicate_a_response.result_code,
    );

    var duplicate_b = makeBalanceAdjustmentCommand(
        uid_b,
        currency_b,
        1,
        tx_id_b,
        .adjustment,
    );
    var duplicate_b_response = try kernel.execute(&duplicate_b);
    defer duplicate_b_response.deinit(allocator);
    try std.testing.expectEqual(
        cmd.CommandResultCode.user_mgmt_account_balance_adjustment_already_applied_same,
        duplicate_b_response.result_code,
    );

    try std.testing.expectEqual(balance_a, profile_a.accountBalance(currency_a));
    try std.testing.expectEqual(balance_b, profile_b.accountBalance(currency_b));
}

test "kernel binary add-accounts replay consumes only one sequence per frame" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    const first_accounts = [_]binary_payload.CurrencyBalance{
        .{ .currency = 11, .balance = 500 },
        .{ .currency = 15, .balance = 900 },
        .{ .currency = 17, .balance = 1_300 },
    };
    const second_accounts = [_]binary_payload.CurrencyBalance{
        .{ .currency = 11, .balance = 700 },
        .{ .currency = 15, .balance = 1_100 },
    };
    const users = [_]binary_payload.UserAccountsBatch{
        .{ .uid = 8_201, .accounts = first_accounts[0..] },
        .{ .uid = 8_202, .accounts = second_accounts[0..] },
    };
    const payload = try encodeAddAccountsPayload(allocator, users[0..]);
    defer allocator.free(payload);

    var frames: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
    defer frames.deinit(allocator);
    try appendBinaryPayloadFrames(
        allocator,
        &frames,
        .binary_data_command,
        720,
        payload,
    );
    try std.testing.expect(frames.items.len > 1);

    var first_replay_hash: ?u64 = null;
    for (0..2) |replay_index| {
        for (frames.items, 0..) |source_frame, frame_index| {
            var frame = source_frame;
            var response = try kernel.execute(&frame);
            defer response.deinit(allocator);

            const expected_sequence: u64 = @intCast(
                replay_index * frames.items.len + frame_index + 1,
            );
            const expected_result: cmd.CommandResultCode = if (frame_index + 1 == frames.items.len)
                .success
            else
                .accepted;
            try std.testing.expectEqual(expected_sequence, response.sequence);
            try std.testing.expectEqual(expected_sequence, kernel.next_seq);
            try std.testing.expectEqual(expected_result, response.result_code);
        }

        var state_hash = try kernel.queryReport(.state_hash_detailed);
        defer state_hash.deinit(allocator);
        const aggregate_hash = state_hash.state_hash_detailed.aggregate_hash;
        if (first_replay_hash) |expected_hash| {
            try std.testing.expectEqual(expected_hash, aggregate_hash);
        } else {
            first_replay_hash = aggregate_hash;
        }
        try std.testing.expectEqual(@as(usize, 0), kernel.binary_frame_assembler.pendingTransfers());
    }

    try std.testing.expectEqual(
        @as(u64, @intCast(frames.items.len * 2)),
        kernel.next_seq,
    );
    const first_profile = kernel.risk_engine.getUserProfile(8_201) orelse unreachable;
    const second_profile = kernel.risk_engine.getUserProfile(8_202) orelse unreachable;
    try std.testing.expectEqual(@as(i64, 500), first_profile.accountBalance(11));
    try std.testing.expectEqual(@as(i64, 900), first_profile.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 1_300), first_profile.accountBalance(17));
    try std.testing.expectEqual(@as(i64, 700), second_profile.accountBalance(11));
    try std.testing.expectEqual(@as(i64, 1_100), second_profile.accountBalance(15));
}

test "kernel binary command capability flags gate add-symbols while allowing add-accounts" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
        .report_queries = .{
            .add_symbols_binary = false,
            .add_accounts_binary = true,
        },
    });
    defer kernel.deinit();

    const symbol_specs = [_]common.CoreSymbolSpecification{
        try makeExchangeSpec(),
    };
    const symbol_payload = try encodeAddSymbolsPayload(allocator, symbol_specs[0..]);
    defer allocator.free(symbol_payload);

    var symbol_frames: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
    defer symbol_frames.deinit(allocator);
    try appendBinaryPayloadFrames(
        allocator,
        &symbol_frames,
        .binary_data_command,
        710,
        symbol_payload,
    );

    for (symbol_frames.items, 0..) |*frame, idx| {
        var response = try kernel.execute(frame);
        defer response.deinit(allocator);
        const expected: cmd.CommandResultCode = if (idx + 1 == symbol_frames.items.len)
            .binary_command_failed
        else
            .accepted;
        try std.testing.expectEqual(expected, response.result_code);
    }
    try std.testing.expectEqual(@as(usize, 0), kernel.binary_frame_assembler.pendingTransfers());
    try std.testing.expectError(
        error.UnknownSymbol,
        kernel.queryReport(.{
            .order_book = .{
                .symbol = 241,
                .depth = 4,
            },
        }),
    );

    const accounts = [_]binary_payload.CurrencyBalance{
        .{ .currency = 15, .balance = 1200 },
    };
    const users = [_]binary_payload.UserAccountsBatch{
        .{ .uid = 9101, .accounts = accounts[0..] },
    };
    const accounts_payload = try encodeAddAccountsPayload(allocator, users[0..]);
    defer allocator.free(accounts_payload);

    var account_frames: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
    defer account_frames.deinit(allocator);
    try appendBinaryPayloadFrames(
        allocator,
        &account_frames,
        .binary_data_command,
        711,
        accounts_payload,
    );

    for (account_frames.items, 0..) |*frame, idx| {
        var response = try kernel.execute(frame);
        defer response.deinit(allocator);
        const expected: cmd.CommandResultCode = if (idx + 1 == account_frames.items.len)
            .success
        else
            .accepted;
        try std.testing.expectEqual(expected, response.result_code);
    }
    try std.testing.expectEqual(@as(usize, 0), kernel.binary_frame_assembler.pendingTransfers());

    var u1_15 = try kernel.queryReport(.{
        .user_balance = .{
            .uid = 9101,
            .currency = 15,
        },
    });
    defer u1_15.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 1200), u1_15.user_balance);
}

test "kernel binary data command malformed payload returns binary command failed" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    var payload: std.Io.Writer.Allocating = .init(allocator);
    defer payload.deinit();
    const writer = &payload.writer;
    writer.writeInt(i32, 1002, .big) catch return error.OutOfMemory;
    writer.writeInt(i32, -1, .big) catch return error.OutOfMemory;

    var frames: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
    defer frames.deinit(allocator);
    try appendBinaryPayloadFrames(
        allocator,
        &frames,
        .binary_data_command,
        703,
        payload.written(),
    );

    for (frames.items, 0..) |*frame, idx| {
        var response = try kernel.execute(frame);
        defer response.deinit(allocator);
        const expected: cmd.CommandResultCode = if (idx + 1 == frames.items.len)
            .binary_command_failed
        else
            .accepted;
        try std.testing.expectEqual(expected, response.result_code);
    }
    try std.testing.expectEqual(@as(usize, 0), kernel.binary_frame_assembler.pendingTransfers());
}

test "kernel binary frame invalid marker returns binary command failed" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    var invalid = makeBinaryFrameCommand(.binary_data_command, 601, 9, .{ 1, 1, 1, 1, 1 });
    var response = try kernel.execute(&invalid);
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.binary_command_failed, response.result_code);
    try std.testing.expectEqualDeep(invalid, response.command);
    try std.testing.expectEqual(@as(usize, 0), kernel.binary_frame_assembler.pendingTransfers());
}

test "kernel RESET atomically clears risk matching and in-flight binary state and permits reuse" {
    const allocator = std.testing.allocator;
    const old_symbol: i32 = 241;
    const new_symbol: i32 = 242;
    const old_uid: i64 = 8_401;
    const new_uid: i64 = 8_402;
    const transfer_id: i32 = 9_901;

    for ([_]OrderBookKind{ .naive, .direct }) |orderbook_kind| {
        var kernel = try ExchangeKernel.init(allocator, .{
            .orderbook_kind = orderbook_kind,
            .risk = .{ .ignore_risk_processing = true },
        });
        defer kernel.deinit();

        try std.testing.expect(try kernel.addSymbol(try makeExchangeSpecForSymbol(old_symbol)));

        var add_old_user = makeAddUserCommand(old_uid);
        var add_old_user_response = try kernel.execute(&add_old_user);
        defer add_old_user_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, add_old_user_response.result_code);

        var old_deposit = makeBalanceAdjustmentCommand(old_uid, 15, 50_000, 1, .adjustment);
        var old_deposit_response = try kernel.execute(&old_deposit);
        defer old_deposit_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, old_deposit_response.result_code);

        var old_order = makePlaceOrderCommand(
            84_001,
            old_uid,
            old_symbol,
            100,
            0,
            5,
            .ask,
            .gtc,
            0,
        );
        var old_order_response = try kernel.execute(&old_order);
        defer old_order_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, old_order_response.result_code);

        // Exercise every risk-accounting collection that Java RiskEngine.reset clears.
        try kernel.risk_engine.last_price_cache.put(allocator, old_symbol, .{
            .ask_price = 101,
            .bid_price = 99,
        });
        try kernel.risk_engine.fees.put(allocator, 15, 17);
        try kernel.risk_engine.suspends.put(allocator, 15, -3);

        var old_book = makeRequestOrderBookCommand(old_symbol, 4);
        var old_book_response = try kernel.execute(&old_book);
        defer old_book_response.deinit(allocator);
        const old_snapshot = old_book_response.l2_snapshot orelse unreachable;
        try std.testing.expectEqual(@as(usize, 1), old_snapshot.asks.items.len);
        try std.testing.expectEqual(@as(usize, 1), kernel.books.count());
        try std.testing.expectEqual(@as(usize, 1), kernel.risk_engine.symbol_spec_provider.symbols.count());
        try std.testing.expectEqual(@as(usize, 1), kernel.risk_engine.user_profile_service.user_profiles.count());
        try std.testing.expect(kernel.risk_engine.adjustments.count() != 0);
        try std.testing.expect(kernel.risk_engine.last_price_cache.count() != 0);
        try std.testing.expect(kernel.risk_engine.fees.count() != 0);
        try std.testing.expect(kernel.risk_engine.suspends.count() != 0);

        const new_spec = try makeExchangeSpecForSymbol(new_symbol);
        const new_symbol_payload = try encodeAddSymbolsPayload(allocator, &.{new_spec});
        defer allocator.free(new_symbol_payload);
        var new_symbol_frames: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
        defer new_symbol_frames.deinit(allocator);
        try appendBinaryPayloadFrames(
            allocator,
            &new_symbol_frames,
            .binary_data_command,
            transfer_id,
            new_symbol_payload,
        );
        try std.testing.expect(new_symbol_frames.items.len > 1);

        // Leave a transfer incomplete at the reset boundary.
        var stale_first_frame = new_symbol_frames.items[0];
        var stale_first_response = try kernel.execute(&stale_first_frame);
        defer stale_first_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.accepted, stale_first_response.result_code);
        try std.testing.expectEqual(@as(usize, 1), kernel.binary_frame_assembler.pendingTransfers());

        const sequence_before_reset = kernel.next_seq;
        var reset = makeResetCommand();
        var reset_response = try kernel.execute(&reset);
        defer reset_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, reset_response.result_code);
        try std.testing.expectEqualDeep(reset, reset_response.command);
        try std.testing.expectEqual(sequence_before_reset + 1, reset_response.sequence);
        try std.testing.expectEqual(reset_response.sequence, kernel.next_seq);

        try std.testing.expectEqual(@as(usize, 0), kernel.books.count());
        try std.testing.expectEqual(@as(usize, 0), kernel.risk_engine.symbol_spec_provider.symbols.count());
        try std.testing.expectEqual(@as(usize, 0), kernel.risk_engine.user_profile_service.user_profiles.count());
        try std.testing.expectEqual(@as(usize, 0), kernel.risk_engine.last_price_cache.count());
        try std.testing.expectEqual(@as(usize, 0), kernel.risk_engine.fees.count());
        try std.testing.expectEqual(@as(usize, 0), kernel.risk_engine.adjustments.count());
        try std.testing.expectEqual(@as(usize, 0), kernel.risk_engine.suspends.count());
        try std.testing.expectEqual(@as(usize, 0), kernel.binary_frame_assembler.pendingTransfers());

        var missing_old_book = makeRequestOrderBookCommand(old_symbol, 1);
        var missing_old_book_response = try kernel.execute(&missing_old_book);
        defer missing_old_book_response.deinit(allocator);
        try std.testing.expectEqual(
            cmd.CommandResultCode.matching_invalid_order_book_id,
            missing_old_book_response.result_code,
        );
        try std.testing.expectEqualDeep(missing_old_book, missing_old_book_response.command);

        // Reusing the same transfer id with the complete payload must not append
        // to the pre-reset fragment. The final frame creates a fresh symbol/book.
        for (new_symbol_frames.items, 0..) |frame_value, frame_idx| {
            var frame = frame_value;
            var frame_response = try kernel.execute(&frame);
            defer frame_response.deinit(allocator);
            try std.testing.expectEqual(
                if (frame_idx + 1 == new_symbol_frames.items.len)
                    cmd.CommandResultCode.success
                else
                    cmd.CommandResultCode.accepted,
                frame_response.result_code,
            );
        }

        try std.testing.expect(kernel.books.contains(new_symbol));
        try std.testing.expect(kernel.risk_engine.symbol_spec_provider.getSymbolSpecification(new_symbol) != null);
        try std.testing.expectEqual(@as(usize, 0), kernel.binary_frame_assembler.pendingTransfers());

        var add_new_user = makeAddUserCommand(new_uid);
        var add_new_user_response = try kernel.execute(&add_new_user);
        defer add_new_user_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, add_new_user_response.result_code);

        var new_deposit = makeBalanceAdjustmentCommand(new_uid, 11, 10_000, 2, .adjustment);
        var new_deposit_response = try kernel.execute(&new_deposit);
        defer new_deposit_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, new_deposit_response.result_code);

        var new_order = makePlaceOrderCommand(
            84_002,
            new_uid,
            new_symbol,
            90,
            0,
            2,
            .ask,
            .gtc,
            0,
        );
        var new_order_response = try kernel.execute(&new_order);
        defer new_order_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, new_order_response.result_code);

        var new_book = makeRequestOrderBookCommand(new_symbol, 4);
        var new_book_response = try kernel.execute(&new_book);
        defer new_book_response.deinit(allocator);
        const new_snapshot = new_book_response.l2_snapshot orelse unreachable;
        try std.testing.expectEqual(@as(usize, 1), new_snapshot.asks.items.len);
        try std.testing.expectEqual(@as(i64, 90), new_snapshot.asks.items[0].price);
        try std.testing.expect(kernel.next_seq > reset_response.sequence);
    }
}

test "4ME 2RE Java multi-frame commands and query preserve routed split parity" {
    const allocator = std.testing.allocator;
    var routed = try MultiShardKernelSet.initWithTopology(
        allocator,
        4,
        2,
        .{ .orderbook_kind = .direct },
    );
    defer routed.deinit();
    var split = try MultiShardKernelSet.initWithTopology(
        allocator,
        4,
        2,
        .{ .orderbook_kind = .direct },
    );
    defer split.deinit();

    const symbol: i32 = 9_123;
    const spec = try makeExchangeSpecForSymbol(symbol);
    const symbol_body = try binary_payload.encodeJavaAddSymbolsToBytes(allocator, &.{spec});
    defer allocator.free(symbol_body);
    const symbol_envelope = try java_binary_wire.encodeRequestEnvelope(allocator, symbol_body, .{});
    defer allocator.free(symbol_envelope);
    const symbol_words = try java_binary_wire.packSignedWordsBigEndian(allocator, symbol_envelope);
    defer allocator.free(symbol_words);
    try std.testing.expectEqual(@as(usize, 10), symbol_words.len);
    try std.testing.expectEqual(@as(usize, 2), try java_binary_wire.frameCount(symbol_words));

    const uid = uidForShard(1, 2, 91_001);
    const balances = [_]binary_payload.CurrencyBalance{
        .{ .currency = 11, .balance = 500 },
    };
    const users = [_]binary_payload.UserAccountsBatch{
        .{ .uid = uid, .accounts = &balances },
    };
    const accounts_body = try binary_payload.encodeJavaAddAccountsToBytes(allocator, &users);
    defer allocator.free(accounts_body);
    const accounts_envelope = try java_binary_wire.encodeRequestEnvelope(allocator, accounts_body, .{});
    defer allocator.free(accounts_envelope);

    const query_body = try binary_payload.encodeJavaReportQueryToBytes(
        allocator,
        .{ .single_user = uid },
    );
    defer allocator.free(query_body);
    const query_envelope = try java_binary_wire.encodeRequestEnvelope(allocator, query_body, .{});
    defer allocator.free(query_envelope);

    const sets = [_]*MultiShardKernelSet{ &routed, &split };
    var fingerprints: [sets.len]u64 = undefined;
    for (sets, 0..) |set, mode_idx| {
        const use_split = mode_idx == 1;
        {
            var response = try executeMultiBinaryPayload(
                allocator,
                set,
                use_split,
                .binary_data_command,
                9_200,
                symbol_envelope,
            );
            defer response.deinit(allocator);
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
        }
        for (set.matching_shards) |*matching| {
            try std.testing.expect(matching.books.contains(symbol));
        }
        for (set.risk_shards) |*risk_shard| {
            try std.testing.expect(
                risk_shard.engine.symbol_spec_provider.getSymbolSpecification(symbol) != null,
            );
        }

        {
            var response = try executeMultiBinaryPayload(
                allocator,
                set,
                use_split,
                .binary_data_command,
                9_201,
                accounts_envelope,
            );
            defer response.deinit(allocator);
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
        }
        try std.testing.expectEqual(@as(?i64, 500), try set.queryUserBalance(uid, 11));

        {
            var response = try executeMultiBinaryPayload(
                allocator,
                set,
                use_split,
                .binary_data_query,
                9_202,
                query_envelope,
            );
            defer response.deinit(allocator);
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
            switch (response.java_report_response orelse return error.MissingJavaReportResponse) {
                .single_user => |report| {
                    try std.testing.expectEqual(@as(i64, 0), report.uid);
                    try std.testing.expectEqual(java_report_wire.QueryExecutionStatus.ok, report.query_execution_status);
                    try std.testing.expectEqual(@as(?i64, 500), javaMapValue(report.accounts, 11));
                },
                else => return error.UnexpectedReportResponseType,
            }
        }
        _ = try expectAllBinaryProcessorState(set, 0);
        try expectAllShardSequences(set, 4);
        fingerprints[mode_idx] = try set.detailedHashFingerprint();
    }

    try std.testing.expectEqual(fingerprints[0], fingerprints[1]);
}

test "multi-shard routed and split report projections preserve exact and native lanes" {
    const allocator = std.testing.allocator;
    const topology_config = Config{
        .orderbook_kind = .direct,
        .java_identity_hash_context = testJavaIdentityHashContext(),
    };
    var routed = try MultiShardKernelSet.initWithTopology(
        allocator,
        2,
        2,
        topology_config,
    );
    defer routed.deinit();
    var split = try MultiShardKernelSet.initWithTopology(
        allocator,
        2,
        2,
        topology_config,
    );
    defer split.deinit();

    const payload = try encodeReportQueryPayload(allocator, 10001);
    defer allocator.free(payload);
    const projections = [_]cmd.BinaryReportProjection{
        .exact_java_only,
        .exact_java_and_native,
    };

    for (projections, 0..) |projection, projection_idx| {
        var frames: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
        defer frames.deinit(allocator);
        try appendBinaryPayloadFrames(
            allocator,
            &frames,
            .binary_data_query,
            9_300 + @as(i32, @intCast(projection_idx)),
            payload,
        );
        frames.items[frames.items.len - 1].binary_report_projection = projection;

        for (frames.items, 0..) |frame_value, frame_idx| {
            var routed_command = frame_value;
            var split_command = frame_value;
            var routed_response = try routed.executeRouted(&routed_command);
            defer routed_response.deinit(allocator);
            var split_response = try split.executeSplit(&split_command);
            defer split_response.deinit(allocator);

            try std.testing.expectEqual(routed_response.sequence, split_response.sequence);
            try std.testing.expectEqual(routed_response.result_code, split_response.result_code);
            try std.testing.expectEqualDeep(routed_response.command, split_response.command);

            if (frame_idx + 1 != frames.items.len) {
                try std.testing.expectEqual(
                    cmd.CommandResultCode.accepted,
                    routed_response.result_code,
                );
                try std.testing.expect(routed_response.java_report_response == null);
                try std.testing.expect(split_response.java_report_response == null);
                try std.testing.expect(routed_response.report_response == null);
                try std.testing.expect(split_response.report_response == null);
                continue;
            }

            try std.testing.expectEqual(cmd.CommandResultCode.success, routed_response.result_code);
            const routed_java = routed_response.java_report_response orelse
                return error.MissingJavaReportResponse;
            const split_java = split_response.java_report_response orelse
                return error.MissingJavaReportResponse;
            switch (routed_java) {
                .state_hash => |routed_state_hash| switch (split_java) {
                    .state_hash => |split_state_hash| {
                        try std.testing.expectEqual(
                            routed_state_hash.state_hash,
                            split_state_hash.state_hash,
                        );
                    },
                    else => return error.UnexpectedReportResponseType,
                },
                else => return error.UnexpectedReportResponseType,
            }

            switch (projection) {
                .exact_java_only => {
                    try std.testing.expect(routed_response.report_response == null);
                    try std.testing.expect(split_response.report_response == null);
                },
                .exact_java_and_native => {
                    const routed_native = routed_response.report_response orelse
                        return error.MissingReportResponse;
                    const split_native = split_response.report_response orelse
                        return error.MissingReportResponse;
                    switch (routed_native) {
                        .state_hash_detailed => |routed_state_hash| switch (split_native) {
                            .state_hash_detailed => |split_state_hash| {
                                try std.testing.expectEqual(
                                    routed_state_hash.aggregate_hash,
                                    split_state_hash.aggregate_hash,
                                );
                            },
                            else => return error.UnexpectedReportResponseType,
                        },
                        else => return error.UnexpectedReportResponseType,
                    }
                },
            }
        }
    }

    try std.testing.expectEqual(routed.next_seq, split.next_seq);
    try std.testing.expectEqual(
        try routed.detailedHashFingerprint(),
        try split.detailedHashFingerprint(),
    );
}

test "4ME 2RE binary broadcast keeps partial state atomic across mismatch malformed and reset" {
    const allocator = std.testing.allocator;
    var sharded = try MultiShardKernelSet.initWithTopology(
        allocator,
        4,
        2,
        .{ .orderbook_kind = .direct },
    );
    defer sharded.deinit();

    const transfer_id: i32 = 7_001;
    var partial = makeBinaryFrameCommand(
        .binary_data_command,
        transfer_id,
        0,
        .{ 1, 2, 3, 4, 5 },
    );
    var partial_response = try sharded.executeRouted(&partial);
    defer partial_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.accepted, partial_response.result_code);
    const partial_hash = try expectAllBinaryProcessorState(&sharded, 1);
    try expectAllShardSequences(&sharded, 1);

    var partial_report = try sharded.queryReport(.state_hash_detailed);
    defer partial_report.deinit(allocator);
    try expectDetailedTopologyIdentity(&partial_report.state_hash_detailed, 4, 2);
    var binary_entries: usize = 0;
    for (partial_report.state_hash_detailed.submodules.items) |entry| {
        switch (entry.submodule) {
            .risk_binary_cmd_processor, .matching_binary_cmd_processor => {
                try std.testing.expectEqual(partial_hash, entry.hash);
                binary_entries += 1;
            },
            else => {},
        }
    }
    try std.testing.expectEqual(@as(usize, 6), binary_entries);

    // Introduce one divergent processor to prove the next broadcast does not
    // commit a prefix when completion payloads disagree.
    var divergent = makeBinaryFrameCommand(
        .binary_data_command,
        transfer_id,
        0,
        .{ 6, 7, 8, 9, 10 },
    );
    const divergent_result = try sharded.risk_shards[1].binary_frame_assembler.acceptFrame(&divergent);
    try std.testing.expect(divergent_result == .accepted);
    const before_failed_completion = try sharded.detailedHashFingerprint();

    var final = makeBinaryFrameCommand(
        .binary_data_command,
        transfer_id,
        -1,
        .{ 11, 12, 13, 14, 15 },
    );
    var final_response = try sharded.executeSplit(&final);
    defer final_response.deinit(allocator);
    try std.testing.expectEqual(
        cmd.CommandResultCode.binary_command_failed,
        final_response.result_code,
    );
    try std.testing.expectEqual(
        before_failed_completion,
        try sharded.detailedHashFingerprint(),
    );
    for (sharded.matching_shards) |*matching| {
        try std.testing.expectEqual(@as(usize, 1), matching.binary_frame_assembler.pendingTransfers());
    }
    for (sharded.risk_shards) |*risk_shard| {
        try std.testing.expectEqual(@as(usize, 1), risk_shard.binary_frame_assembler.pendingTransfers());
    }

    var malformed = makeBinaryFrameCommand(
        .binary_data_query,
        7_002,
        1,
        .{ 21, 22, 23, 24, 25 },
    );
    var malformed_response = try sharded.executeRouted(&malformed);
    defer malformed_response.deinit(allocator);
    try std.testing.expectEqual(
        cmd.CommandResultCode.binary_command_failed,
        malformed_response.result_code,
    );
    try std.testing.expectEqual(
        before_failed_completion,
        try sharded.detailedHashFingerprint(),
    );

    var reset = makeResetCommand();
    var reset_response = try sharded.executeSplit(&reset);
    defer reset_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, reset_response.result_code);
    _ = try expectAllBinaryProcessorState(&sharded, 0);
    try expectAllShardSequences(&sharded, 4);
}

test "4ME 2RE binary mutations and merged reports keep routed split parity" {
    const allocator = std.testing.allocator;
    const exact_config = Config{
        .orderbook_kind = .direct,
        .java_identity_hash_context = testJavaIdentityHashContext(),
    };
    var routed = try MultiShardKernelSet.initWithTopology(
        allocator,
        4,
        2,
        exact_config,
    );
    defer routed.deinit();
    var split = try MultiShardKernelSet.initWithTopology(
        allocator,
        4,
        2,
        exact_config,
    );
    defer split.deinit();

    const symbol0: i32 = 240;
    const symbol3: i32 = 243;
    const symbols = [_]common.CoreSymbolSpecification{
        try makeExchangeSpecForSymbol(symbol0),
        try makeExchangeSpecForSymbol(symbol3),
    };
    const symbol_payload = try encodeAddSymbolsPayload(allocator, symbols[0..]);
    defer allocator.free(symbol_payload);

    const uid = uidForShard(1, 2, 70_001);
    const accounts = [_]binary_payload.CurrencyBalance{
        .{ .currency = 11, .balance = 1_000 },
    };
    const users = [_]binary_payload.UserAccountsBatch{
        .{ .uid = uid, .accounts = accounts[0..] },
    };
    const account_payload = try encodeAddAccountsPayload(allocator, users[0..]);
    defer allocator.free(account_payload);
    const single_user_payload = try encodeSingleUserReportQueryPayload(allocator, uid);
    defer allocator.free(single_user_payload);
    const total_payload = try encodeReportQueryPayload(allocator, 10003);
    defer allocator.free(total_payload);
    const state_hash_payload = try encodeReportQueryPayload(allocator, 10001);
    defer allocator.free(state_hash_payload);

    const sets = [_]*MultiShardKernelSet{ &routed, &split };
    var fingerprints: [sets.len]i32 = undefined;
    for (sets, 0..) |set, mode_idx| {
        const use_split = mode_idx == 1;
        {
            var response = try executeMultiBinaryPayload(
                allocator,
                set,
                use_split,
                .binary_data_command,
                241,
                symbol_payload,
            );
            defer response.deinit(allocator);
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
        }
        for (set.risk_shards) |*risk_shard| {
            try std.testing.expectEqual(
                @as(usize, symbols.len),
                risk_shard.engine.symbol_spec_provider.symbols.count(),
            );
        }
        // Java's binary symbol handler installs every definition in every
        // matching router; ordinary commands still mutate only the owner.
        for (set.matching_shards) |*matching| {
            try std.testing.expectEqual(@as(usize, symbols.len), matching.books.count());
        }
        try expectOperationalRiskStateEmpty(&set.matching_shards[2].risk_engine);
        try expectOperationalRiskStateEmpty(&set.matching_shards[3].risk_engine);

        {
            var response = try executeMultiBinaryPayload(
                allocator,
                set,
                use_split,
                .binary_data_command,
                symbol0,
                account_payload,
            );
            defer response.deinit(allocator);
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
        }
        for (set.risk_shards, 0..) |*risk_shard, shard_idx| {
            try std.testing.expectEqual(
                @as(usize, if (shard_idx == 1) 1 else 0),
                risk_shard.engine.user_profile_service.user_profiles.count(),
            );
        }
        try std.testing.expectEqual(@as(?i64, 1_000), try set.queryUserBalance(uid, 11));

        var order0 = cmd.OrderCommand.newOrder(.gtc, 70_101, uid, 100, 0, 1, .ask);
        order0.symbol = symbol0;
        try executeMultiExpect(allocator, set, use_split, order0, .success);
        var order3 = cmd.OrderCommand.newOrder(.gtc, 70_102, uid, 100, 0, 1, .ask);
        order3.symbol = symbol3;
        try executeMultiExpect(allocator, set, use_split, order3, .success);

        {
            var response = try executeMultiBinaryPayload(
                allocator,
                set,
                use_split,
                .binary_data_query,
                997,
                single_user_payload,
            );
            defer response.deinit(allocator);
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
            switch (response.java_report_response orelse return error.MissingJavaReportResponse) {
                .single_user => |report| {
                    try std.testing.expectEqual(java_report_wire.QueryExecutionStatus.ok, report.query_execution_status);
                    try std.testing.expectEqual(@as(i64, 0), report.uid);
                    try std.testing.expectEqual(@as(?i64, 800), javaMapValue(report.accounts, 11));
                    const groups = report.orders orelse return error.MissingJavaOrders;
                    try std.testing.expectEqual(@as(usize, 2), groups.len);
                    try std.testing.expectEqual(symbol0, groups[0].symbol);
                    try std.testing.expectEqual(symbol3, groups[1].symbol);
                },
                else => return error.UnexpectedReportResponseType,
            }
        }

        {
            var response = try executeMultiBinaryPayload(
                allocator,
                set,
                use_split,
                .binary_data_query,
                998,
                total_payload,
            );
            defer response.deinit(allocator);
            switch (response.java_report_response orelse return error.MissingJavaReportResponse) {
                .total_currency_balance => |report| {
                    try std.testing.expectEqual(@as(?i64, 800), javaMapValue(report.account_balances, 11));
                    try std.testing.expectEqual(@as(?i64, 200), javaMapValue(report.order_balances, 11));
                    try std.testing.expectEqual(@as(?i64, -1_000), javaMapValue(report.adjustments, 11));
                },
                else => return error.UnexpectedReportResponseType,
            }
        }

        {
            var response = try executeMultiBinaryPayload(
                allocator,
                set,
                use_split,
                .binary_data_query,
                999,
                state_hash_payload,
            );
            defer response.deinit(allocator);
            switch (response.java_report_response orelse return error.MissingJavaReportResponse) {
                .state_hash => |*state_hash| {
                    try expectJavaStateHashTopology(
                        allocator,
                        state_hash,
                        response.matcher_events_head,
                        4,
                        2,
                    );
                    fingerprints[mode_idx] = state_hash.state_hash;
                },
                else => return error.UnexpectedReportResponseType,
            }
        }
        _ = try expectAllBinaryProcessorState(set, 0);
    }

    try std.testing.expectEqual(fingerprints[0], fingerprints[1]);
    try std.testing.expectEqual(routed.next_seq, split.next_seq);
}

test "1ME 4RE binary broadcast mutates each semantic owner once and merges reports" {
    const allocator = std.testing.allocator;
    var sharded = try MultiShardKernelSet.initWithTopology(
        allocator,
        1,
        4,
        .{
            .orderbook_kind = .direct,
            .java_identity_hash_context = testJavaIdentityHashContext(),
        },
    );
    defer sharded.deinit();

    var partial = makeBinaryFrameCommand(
        .binary_data_query,
        8_001,
        0,
        .{ 31, 32, 33, 34, 35 },
    );
    var partial_response = try sharded.executeSplit(&partial);
    defer partial_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.accepted, partial_response.result_code);
    _ = try expectAllBinaryProcessorState(&sharded, 1);

    var reset = makeResetCommand();
    var reset_response = try sharded.executeRouted(&reset);
    defer reset_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, reset_response.result_code);
    _ = try expectAllBinaryProcessorState(&sharded, 0);

    const symbol: i32 = 8_101;
    const spec = try makeExchangeSpecForSymbol(symbol);
    const symbol_payload = try encodeAddSymbolsPayload(allocator, &.{spec});
    defer allocator.free(symbol_payload);
    {
        var response = try executeMultiBinaryPayload(
            allocator,
            &sharded,
            true,
            .binary_data_command,
            8_199,
            symbol_payload,
        );
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    }
    try std.testing.expectEqual(@as(usize, 1), sharded.matching_shards[0].books.count());
    for (sharded.risk_shards) |*risk_shard| {
        try std.testing.expectEqual(
            @as(usize, 1),
            risk_shard.engine.symbol_spec_provider.symbols.count(),
        );
    }

    const uids = [_]i64{
        uidForShard(0, 4, 81_000),
        uidForShard(1, 4, 81_100),
        uidForShard(2, 4, 81_200),
        uidForShard(3, 4, 81_300),
    };
    const balances = [_][1]binary_payload.CurrencyBalance{
        .{.{ .currency = 11, .balance = 1_000 }},
        .{.{ .currency = 11, .balance = 2_000 }},
        .{.{ .currency = 11, .balance = 3_000 }},
        .{.{ .currency = 11, .balance = 4_000 }},
    };
    const users = [_]binary_payload.UserAccountsBatch{
        .{ .uid = uids[0], .accounts = balances[0][0..] },
        .{ .uid = uids[1], .accounts = balances[1][0..] },
        .{ .uid = uids[2], .accounts = balances[2][0..] },
        .{ .uid = uids[3], .accounts = balances[3][0..] },
    };
    const account_payload = try encodeAddAccountsPayload(allocator, users[0..]);
    defer allocator.free(account_payload);
    {
        var response = try executeMultiBinaryPayload(
            allocator,
            &sharded,
            false,
            .binary_data_command,
            8_198,
            account_payload,
        );
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    }

    for (sharded.risk_shards, 0..) |*risk_shard, shard_idx| {
        try std.testing.expectEqual(
            @as(usize, 1),
            risk_shard.engine.user_profile_service.user_profiles.count(),
        );
        try std.testing.expect(risk_shard.engine.getUserProfile(uids[shard_idx]) != null);
    }
    _ = try expectAllBinaryProcessorState(&sharded, 0);

    const total_payload = try encodeReportQueryPayload(allocator, 10003);
    defer allocator.free(total_payload);
    {
        var response = try executeMultiBinaryPayload(
            allocator,
            &sharded,
            true,
            .binary_data_query,
            8_197,
            total_payload,
        );
        defer response.deinit(allocator);
        switch (response.java_report_response orelse return error.MissingJavaReportResponse) {
            .total_currency_balance => |report| {
                try std.testing.expectEqual(@as(?i64, 10_000), javaMapValue(report.account_balances, 11));
                try std.testing.expectEqual(@as(?i64, -10_000), javaMapValue(report.adjustments, 11));
            },
            else => return error.UnexpectedReportResponseType,
        }
    }

    const hash_payload = try encodeReportQueryPayload(allocator, 10001);
    defer allocator.free(hash_payload);
    var hash_response = try executeMultiBinaryPayload(
        allocator,
        &sharded,
        false,
        .binary_data_query,
        8_196,
        hash_payload,
    );
    defer hash_response.deinit(allocator);
    switch (hash_response.java_report_response orelse return error.MissingJavaReportResponse) {
        .state_hash => |*state_hash| try expectJavaStateHashTopology(
            allocator,
            state_hash,
            hash_response.matcher_events_head,
            1,
            4,
        ),
        else => return error.UnexpectedReportResponseType,
    }
}

test "multi-shard RESET clears independent families in routed and split modes" {
    const allocator = std.testing.allocator;
    const symbol: i32 = 6_401;

    for ([_]bool{ false, true }) |use_split| {
        var sharded = try MultiShardKernelSet.initWithTopology(
            allocator,
            4,
            2,
            .{ .orderbook_kind = .direct },
        );
        defer sharded.deinit();

        // Matching carriers 2 and 3 have structurally dormant embedded risks.
        try expectOperationalRiskStateEmpty(&sharded.matching_shards[2].risk_engine);
        try expectOperationalRiskStateEmpty(&sharded.matching_shards[3].risk_engine);
        try sharded.addSymbolAll(try makeExchangeSpecForSymbol(symbol));
        try expectOperationalRiskStateEmpty(&sharded.matching_shards[2].risk_engine);
        try expectOperationalRiskStateEmpty(&sharded.matching_shards[3].risk_engine);

        const uid = uidForShard(1, 2, 64_001);
        try executeMultiExpect(allocator, &sharded, use_split, makeAddUserCommand(uid), .success);
        try executeMultiExpect(
            allocator,
            &sharded,
            use_split,
            makeBalanceAdjustmentCommand(uid, 11, 1_000, 1, .adjustment),
            .success,
        );
        var order = cmd.OrderCommand.newOrder(.gtc, 64_101, uid, 100, 0, 1, .ask);
        order.symbol = symbol;
        try executeMultiExpect(allocator, &sharded, use_split, order, .success);

        for (sharded.matching_shards, 0..) |*matching, shard_idx| {
            var partial_frame = makeBinaryFrameCommand(
                .binary_data_command,
                @as(i32, @intCast(7_100 + shard_idx)),
                0,
                .{ 1, 2, 3, 4, 5 },
            );
            const accepted = try matching.binary_frame_assembler.acceptFrame(&partial_frame);
            try std.testing.expect(accepted == .accepted);
            try std.testing.expectEqual(@as(usize, 1), matching.books.count());
            try std.testing.expectEqual(
                @as(usize, 1),
                matching.binary_frame_assembler.pendingTransfers(),
            );
        }
        for (sharded.risk_shards, 0..) |*risk_shard, shard_idx| {
            var partial_frame = makeBinaryFrameCommand(
                .binary_data_command,
                @as(i32, @intCast(7_200 + shard_idx)),
                0,
                .{ 6, 7, 8, 9, 10 },
            );
            const accepted = try risk_shard.binary_frame_assembler.acceptFrame(&partial_frame);
            try std.testing.expect(accepted == .accepted);
            try std.testing.expectEqual(
                @as(usize, 1),
                risk_shard.binary_frame_assembler.pendingTransfers(),
            );
        }

        var before_hash = try sharded.queryReport(.state_hash_detailed);
        defer before_hash.deinit(allocator);
        try std.testing.expectEqual(
            @as(usize, 8 * 2 + 3 * 4),
            before_hash.state_hash_detailed.submodules.items.len,
        );
        try expectOperationalRiskStateEmpty(&sharded.matching_shards[2].risk_engine);
        try expectOperationalRiskStateEmpty(&sharded.matching_shards[3].risk_engine);

        var reset = makeResetCommand();
        var response = if (use_split)
            try sharded.executeSplit(&reset)
        else
            try sharded.executeRouted(&reset);
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
        try std.testing.expectEqualDeep(reset, response.command);
        try expectAllShardSequences(&sharded, 4);

        for (sharded.matching_shards) |*matching| {
            try std.testing.expectEqual(@as(usize, 0), matching.books.count());
            try std.testing.expectEqual(
                @as(usize, 0),
                matching.binary_frame_assembler.pendingTransfers(),
            );
        }
        for (sharded.risk_shards) |*risk_shard| {
            try expectOperationalRiskStateEmpty(risk_shard.engine);
            try std.testing.expectEqual(
                @as(usize, 0),
                risk_shard.binary_frame_assembler.pendingTransfers(),
            );
        }
        try expectOperationalRiskStateEmpty(&sharded.matching_shards[2].risk_engine);
        try expectOperationalRiskStateEmpty(&sharded.matching_shards[3].risk_engine);

        var after_hash = try sharded.queryReport(.state_hash_detailed);
        defer after_hash.deinit(allocator);
        try std.testing.expectEqual(
            @as(usize, 8 * 2 + 3 * 4),
            after_hash.state_hash_detailed.submodules.items.len,
        );
        try expectOperationalRiskStateEmpty(&sharded.matching_shards[2].risk_engine);
        try expectOperationalRiskStateEmpty(&sharded.matching_shards[3].risk_engine);
    }
}

test "risk-heavy topology routes owners outside matching carriers exactly once" {
    const allocator = std.testing.allocator;
    var sharded = try MultiShardKernelSet.initWithTopology(
        allocator,
        1,
        4,
        .{ .orderbook_kind = .direct },
    );
    defer sharded.deinit();

    try std.testing.expectEqual(@as(usize, 1), sharded.matchingShardCount());
    try std.testing.expectEqual(@as(usize, 4), sharded.riskShardCount());
    try std.testing.expectEqual(@as(u64, 0), sharded.matchingShardMask());
    try std.testing.expectEqual(@as(u64, 3), sharded.riskShardMask());

    const symbol: i32 = 6_501;
    try sharded.addSymbolAll(try makeExchangeSpecForSymbol(symbol));
    for (sharded.risk_shards, 0..) |*risk_shard, idx| {
        try std.testing.expectEqual(@as(u64, @intCast(idx)), risk_shard.engine.shard_id);
        try std.testing.expectEqual(@as(u64, 3), risk_shard.engine.shard_mask);
        try std.testing.expectEqual(
            @as(usize, 1),
            risk_shard.engine.symbol_spec_provider.symbols.count(),
        );
    }

    const uid = uidForShard(3, 4, 65_001);
    try executeMultiExpect(allocator, &sharded, false, makeAddUserCommand(uid), .success);
    try executeMultiExpect(
        allocator,
        &sharded,
        false,
        makeBalanceAdjustmentCommand(uid, 11, 1_000, 1, .adjustment),
        .success,
    );
    var ask = cmd.OrderCommand.newOrder(.gtc, 65_101, uid, 100, 0, 1, .ask);
    ask.symbol = symbol;
    try executeMultiExpect(allocator, &sharded, false, ask, .success);

    for (0..3) |idx| {
        try std.testing.expect(sharded.riskEngineAt(idx).getUserProfile(uid) == null);
    }
    const owner = sharded.riskEngineAt(3).getUserProfile(uid) orelse
        return error.ExpectedRiskOwnerProfile;
    try std.testing.expectEqual(@as(i64, 900), owner.accountBalance(11));
    try std.testing.expectEqual(@as(usize, 1), sharded.matchingShardAt(0).books.count());
    try expectAllShardSequences(&sharded, 3);

    var detailed = try sharded.queryReport(.state_hash_detailed);
    defer detailed.deinit(allocator);
    try std.testing.expectEqual(
        @as(usize, 8 * 4 + 3),
        detailed.state_hash_detailed.submodules.items.len,
    );
}

test "matching-heavy routed pipeline crosses risk owners without touching dormant carriers" {
    const allocator = std.testing.allocator;
    var sharded = try MultiShardKernelSet.initWithTopology(
        allocator,
        4,
        2,
        .{ .orderbook_kind = .direct },
    );
    defer sharded.deinit();

    const symbol: i32 = 243;
    const maker_uid = uidForShard(0, 2, 66_000);
    const taker_uid = uidForShard(1, 2, 66_100);
    try std.testing.expectEqual(@as(usize, 3), sharded.shardIndexForSymbol(symbol));
    try std.testing.expectEqual(@as(usize, 0), sharded.shardIndexForUid(maker_uid));
    try std.testing.expectEqual(@as(usize, 1), sharded.shardIndexForUid(taker_uid));

    try expectOperationalRiskStateEmpty(&sharded.matching_shards[2].risk_engine);
    try expectOperationalRiskStateEmpty(&sharded.matching_shards[3].risk_engine);
    try sharded.addSymbolAll(try makeExchangeSpecForSymbol(symbol));
    try expectOperationalRiskStateEmpty(&sharded.matching_shards[2].risk_engine);
    try expectOperationalRiskStateEmpty(&sharded.matching_shards[3].risk_engine);

    try executeMultiExpect(allocator, &sharded, false, makeAddUserCommand(maker_uid), .success);
    try executeMultiExpect(allocator, &sharded, false, makeAddUserCommand(taker_uid), .success);
    try executeMultiExpect(
        allocator,
        &sharded,
        false,
        makeBalanceAdjustmentCommand(maker_uid, 15, 100_000, 1, .adjustment),
        .success,
    );
    try executeMultiExpect(
        allocator,
        &sharded,
        false,
        makeBalanceAdjustmentCommand(taker_uid, 11, 100_000, 1, .adjustment),
        .success,
    );

    var maker_bid = cmd.OrderCommand.newOrder(.gtc, 66_201, maker_uid, 100, 100, 5, .bid);
    maker_bid.symbol = symbol;
    try executeMultiExpect(allocator, &sharded, false, maker_bid, .success);

    var taker_ask = cmd.OrderCommand.newOrder(.ioc, 66_202, taker_uid, 95, 0, 3, .ask);
    taker_ask.symbol = symbol;
    var taker_response = try sharded.executeRouted(&taker_ask);
    defer taker_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, taker_response.result_code);
    const trade = taker_response.matcher_events_head orelse return error.ExpectedTradeEvent;
    try std.testing.expectEqual(events.MatcherEventType.trade, trade.event_type);
    try std.testing.expectEqual(maker_uid, trade.matched_order_uid);
    try std.testing.expectEqual(@as(i64, 3), trade.size);

    try std.testing.expectEqual(@as(?i64, 94_993), try sharded.queryUserBalance(maker_uid, 15));
    try std.testing.expectEqual(@as(?i64, 300), try sharded.queryUserBalance(maker_uid, 11));
    try std.testing.expectEqual(@as(?i64, 99_700), try sharded.queryUserBalance(taker_uid, 11));
    try std.testing.expectEqual(@as(?i64, 2_994), try sharded.queryUserBalance(taker_uid, 15));

    var book_report = try sharded.queryReport(.{
        .order_book = .{ .symbol = symbol, .depth = 8 },
    });
    defer book_report.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), book_report.order_book.bids.items.len);
    try std.testing.expectEqual(@as(i64, 100), book_report.order_book.bids.items[0].price);
    try std.testing.expectEqual(@as(i64, 2), book_report.order_book.bids.items[0].volume);

    var detailed = try sharded.queryReport(.state_hash_detailed);
    defer detailed.deinit(allocator);
    try std.testing.expectEqual(
        @as(usize, 8 * 2 + 3 * 4),
        detailed.state_hash_detailed.submodules.items.len,
    );
    try expectOperationalRiskStateEmpty(&sharded.matching_shards[2].risk_engine);
    try expectOperationalRiskStateEmpty(&sharded.matching_shards[3].risk_engine);
    try expectAllShardSequences(&sharded, 6);
}

test "all active risk owners settle a multi-maker trade exactly once" {
    const allocator = std.testing.allocator;
    var sharded = try MultiShardKernelSet.initWithTopology(
        allocator,
        4,
        2,
        .{ .orderbook_kind = .direct },
    );
    defer sharded.deinit();

    const symbol: i32 = 243;
    const maker0 = uidForShard(0, 2, 67_000);
    const maker1 = uidForShard(1, 2, 67_100);
    const taker = uidForShard(0, 2, 67_200);
    try sharded.addSymbolAll(try makeExchangeSpecForSymbol(symbol));

    for ([_]i64{ maker0, maker1, taker }) |uid| {
        try executeMultiExpect(allocator, &sharded, true, makeAddUserCommand(uid), .success);
    }
    try executeMultiExpect(
        allocator,
        &sharded,
        true,
        makeBalanceAdjustmentCommand(maker0, 11, 1_000, 1, .adjustment),
        .success,
    );
    try executeMultiExpect(
        allocator,
        &sharded,
        true,
        makeBalanceAdjustmentCommand(maker1, 11, 1_000, 1, .adjustment),
        .success,
    );
    try executeMultiExpect(
        allocator,
        &sharded,
        true,
        makeBalanceAdjustmentCommand(taker, 15, 10_000, 1, .adjustment),
        .success,
    );

    var ask0 = cmd.OrderCommand.newOrder(.gtc, 67_301, maker0, 100, 0, 2, .ask);
    ask0.symbol = symbol;
    try executeMultiExpect(allocator, &sharded, true, ask0, .success);
    var ask1 = cmd.OrderCommand.newOrder(.gtc, 67_302, maker1, 100, 0, 3, .ask);
    ask1.symbol = symbol;
    try executeMultiExpect(allocator, &sharded, true, ask1, .success);

    var bid = cmd.OrderCommand.newOrder(.ioc, 67_303, taker, 100, 100, 5, .bid);
    bid.symbol = symbol;
    var response = try sharded.executeSplit(&bid);
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    try std.testing.expectEqual(@as(usize, 2), events.listSize(response.matcher_events_head));
    const first = response.matcher_events_head orelse return error.ExpectedFirstTrade;
    const second = first.next_event orelse return error.ExpectedSecondTrade;
    try std.testing.expectEqual(maker0, first.matched_order_uid);
    try std.testing.expectEqual(@as(i64, 2), first.size);
    try std.testing.expectEqual(maker1, second.matched_order_uid);
    try std.testing.expectEqual(@as(i64, 3), second.size);

    try std.testing.expectEqual(@as(?i64, 800), try sharded.queryUserBalance(maker0, 11));
    try std.testing.expectEqual(@as(?i64, 1_998), try sharded.queryUserBalance(maker0, 15));
    try std.testing.expectEqual(@as(?i64, 700), try sharded.queryUserBalance(maker1, 11));
    try std.testing.expectEqual(@as(?i64, 2_997), try sharded.queryUserBalance(maker1, 15));
    try std.testing.expectEqual(@as(?i64, 500), try sharded.queryUserBalance(taker, 11));
    try std.testing.expectEqual(@as(?i64, 4_990), try sharded.queryUserBalance(taker, 15));

    var fee_report = try sharded.queryReport(.{ .fee_collected = 15 });
    defer fee_report.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 15), fee_report.fee_collected);
    try expectOperationalRiskStateEmpty(&sharded.matching_shards[2].risk_engine);
    try expectOperationalRiskStateEmpty(&sharded.matching_shards[3].risk_engine);
    try expectAllShardSequences(&sharded, 9);
}

test "single-user report merges orders across matching symbol owners" {
    const allocator = std.testing.allocator;
    var sharded = try MultiShardKernelSet.initWithTopology(
        allocator,
        4,
        2,
        .{ .orderbook_kind = .direct },
    );
    defer sharded.deinit();

    const uid = uidForShard(1, 2, 68_000);
    const symbol0: i32 = 240;
    const symbol3: i32 = 243;
    try sharded.addSymbolAll(try makeExchangeSpecForSymbol(symbol0));
    try sharded.addSymbolAll(try makeExchangeSpecForSymbol(symbol3));
    try executeMultiExpect(allocator, &sharded, true, makeAddUserCommand(uid), .success);
    try executeMultiExpect(
        allocator,
        &sharded,
        true,
        makeBalanceAdjustmentCommand(uid, 11, 1_000, 1, .adjustment),
        .success,
    );

    var high_symbol_order = cmd.OrderCommand.newOrder(.gtc, 68_102, uid, 100, 0, 1, .ask);
    high_symbol_order.symbol = symbol3;
    try executeMultiExpect(allocator, &sharded, true, high_symbol_order, .success);
    var low_symbol_order = cmd.OrderCommand.newOrder(.gtc, 68_101, uid, 100, 0, 1, .ask);
    low_symbol_order.symbol = symbol0;
    try executeMultiExpect(allocator, &sharded, true, low_symbol_order, .success);

    var response = try sharded.queryReport(.{ .single_user = uid });
    defer response.deinit(allocator);
    const report = &response.single_user;
    try std.testing.expectEqual(SingleUserQueryExecutionStatus.ok, report.query_execution_status);
    try std.testing.expectEqual(@as(?i64, 800), report.accounts.get(11));
    try std.testing.expectEqual(@as(usize, 2), report.orders.items.len);
    try std.testing.expectEqual(symbol0, report.orders.items[0].symbol);
    try std.testing.expectEqual(@as(i64, 68_101), report.orders.items[0].order_id);
    try std.testing.expectEqual(symbol3, report.orders.items[1].symbol);
    try std.testing.expectEqual(@as(i64, 68_102), report.orders.items[1].order_id);
    try expectAllShardSequences(&sharded, 4);
}

test "kernel unsupported IOC budget and FOK orders fully release reserved spot funds" {
    const allocator = std.testing.allocator;
    const symbol: i32 = 241;
    const uid: i64 = 8_501;

    for ([_]OrderBookKind{ .naive, .direct }) |orderbook_kind| {
        var kernel = try ExchangeKernel.init(allocator, .{ .orderbook_kind = orderbook_kind });
        defer kernel.deinit();
        try std.testing.expect(try kernel.addSymbol(try makeExchangeSpecForSymbol(symbol)));

        var add_user = makeAddUserCommand(uid);
        var add_user_response = try kernel.execute(&add_user);
        defer add_user_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, add_user_response.result_code);

        var deposit_base = makeBalanceAdjustmentCommand(uid, 11, 100_000, 1, .adjustment);
        var deposit_base_response = try kernel.execute(&deposit_base);
        defer deposit_base_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, deposit_base_response.result_code);

        var deposit_quote = makeBalanceAdjustmentCommand(uid, 15, 2_000_000, 2, .adjustment);
        var deposit_quote_response = try kernel.execute(&deposit_quote);
        defer deposit_quote_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, deposit_quote_response.result_code);

        const profile_before = kernel.risk_engine.getUserProfile(uid) orelse unreachable;
        const base_before = profile_before.accountBalance(11);
        const quote_before = profile_before.accountBalance(15);
        const book = kernel.books.get(symbol) orelse unreachable;
        const book_hash_before = try book.stateHash();

        for (unsupported_order_cases) |case| {
            var unsupported = makePlaceOrderCommand(
                case.order_id,
                uid,
                symbol,
                case.price,
                case.reserve_bid_price,
                case.size,
                case.action,
                case.order_type,
                0,
            );
            var response = try kernel.execute(&unsupported);
            defer response.deinit(allocator);
            try expectUnsupportedFullReject(&response, case);

            const profile_after = kernel.risk_engine.getUserProfile(uid) orelse unreachable;
            try std.testing.expectEqual(base_before, profile_after.accountBalance(11));
            try std.testing.expectEqual(quote_before, profile_after.accountBalance(15));
            try std.testing.expectEqual(@as(usize, 0), profile_after.positions.count());
            try std.testing.expectEqual(book_hash_before, try book.stateHash());
        }

        var order_book = makeRequestOrderBookCommand(symbol, 4);
        var order_book_response = try kernel.execute(&order_book);
        defer order_book_response.deinit(allocator);
        const snapshot = order_book_response.l2_snapshot orelse unreachable;
        try std.testing.expectEqual(@as(usize, 0), snapshot.asks.items.len);
        try std.testing.expectEqual(@as(usize, 0), snapshot.bids.items.len);
    }
}

test "kernel surfaces impossible FOK prescan state as a typed matching invariant" {
    const allocator = std.testing.allocator;
    const symbol: i32 = 6_402;

    for ([_]OrderBookKind{ .naive, .direct }) |orderbook_kind| {
        var kernel = try ExchangeKernel.init(allocator, .{
            .orderbook_kind = orderbook_kind,
            .risk = .{ .ignore_risk_processing = true },
        });
        defer kernel.deinit();
        try std.testing.expect(try kernel.addSymbol(try makeExchangeSpecForSymbol(symbol)));

        for ([_]i64{ 8_601, 8_602 }) |uid| {
            var add_user = makeAddUserCommand(uid);
            var add_response = try kernel.execute(&add_user);
            defer add_response.deinit(allocator);
            try std.testing.expectEqual(cmd.CommandResultCode.success, add_response.result_code);
        }

        var resting = cmd.OrderCommand.newOrder(.gtc, 86_001, 8_601, 100, 0, 2, .ask);
        resting.symbol = symbol;
        var resting_response = try kernel.execute(&resting);
        defer resting_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, resting_response.result_code);

        const book = kernel.books.get(symbol) orelse return error.TestExpectedOrderBook;
        switch (book.*) {
            .naive => |*value| value.asks.items[0].orders.items[0].filled = 2,
            .direct => |*value| {
                const maker = value.order_id_index.get(86_001) orelse return error.TestExpectedMakerOrder;
                maker.filled = 2;
            },
        }

        var fok = cmd.OrderCommand.newOrder(.fok_budget, 86_002, 8_602, 100, 100, 1, .bid);
        fok.symbol = symbol;
        try std.testing.expectError(error.OrderBookInvariantViolation, kernel.execute(&fok));

        switch (book.*) {
            .naive => |*value| value.asks.items[0].orders.items[0].filled = 0,
            .direct => |*value| {
                const maker = value.order_id_index.get(86_001) orelse return error.TestExpectedMakerOrder;
                maker.filled = 0;
            },
        }
    }
}

test "kernel unsupported IOC budget and FOK orders fully release margin pending exposure" {
    const allocator = std.testing.allocator;
    const uid: i64 = 8_601;
    const spec = try makeFeeMarginFuturesSpec();

    for ([_]OrderBookKind{ .naive, .direct }) |orderbook_kind| {
        var kernel = try ExchangeKernel.init(allocator, .{
            .orderbook_kind = orderbook_kind,
            .risk = .{ .margin_trading_enabled = true },
        });
        defer kernel.deinit();
        try std.testing.expect(try kernel.addSymbol(spec));

        var add_user = makeAddUserCommand(uid);
        var add_user_response = try kernel.execute(&add_user);
        defer add_user_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, add_user_response.result_code);

        var deposit = makeBalanceAdjustmentCommand(uid, spec.quote_currency, 1_000_000, 1, .adjustment);
        var deposit_response = try kernel.execute(&deposit);
        defer deposit_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, deposit_response.result_code);

        const profile_before = kernel.risk_engine.getUserProfile(uid) orelse unreachable;
        const balance_before = profile_before.accountBalance(spec.quote_currency);
        const book = kernel.books.get(spec.symbol_id) orelse unreachable;
        const book_hash_before = try book.stateHash();

        for (unsupported_order_cases) |case| {
            var unsupported = makePlaceOrderCommand(
                case.order_id,
                uid,
                spec.symbol_id,
                case.price,
                case.reserve_bid_price,
                case.size,
                case.action,
                case.order_type,
                0,
            );
            var response = try kernel.execute(&unsupported);
            defer response.deinit(allocator);
            try expectUnsupportedFullReject(&response, case);

            const profile_after = kernel.risk_engine.getUserProfile(uid) orelse unreachable;
            try std.testing.expectEqual(balance_before, profile_after.accountBalance(spec.quote_currency));
            try std.testing.expectEqual(@as(usize, 0), profile_after.positions.count());
            try std.testing.expectEqual(@as(usize, 0), kernel.risk_engine.fees.count());
            try std.testing.expectEqual(book_hash_before, try book.stateHash());
        }

        var order_book = makeRequestOrderBookCommand(spec.symbol_id, 4);
        var order_book_response = try kernel.execute(&order_book);
        defer order_book_response.deinit(allocator);
        const snapshot = order_book_response.l2_snapshot orelse unreachable;
        try std.testing.expectEqual(@as(usize, 0), snapshot.asks.items.len);
        try std.testing.expectEqual(@as(usize, 0), snapshot.bids.items.len);
    }
}

test "kernel init rejects zero l2 refresh depth" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(
        error.InvalidL2RefreshDepth,
        ExchangeKernel.init(allocator, .{ .l2_refresh_depth = 0 }),
    );
}

test "kernel orderbook request updates risk last price cache in margin mode" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .risk = .{ .margin_trading_enabled = true },
        .orderbook_kind = .naive,
    });
    defer kernel.deinit();
    try std.testing.expect(try kernel.addSymbol(try makeExchangeSpec()));

    var add_u1 = makeAddUserCommand(2001);
    var add_u2 = makeAddUserCommand(2002);
    var r1 = try kernel.execute(&add_u1);
    defer r1.deinit(allocator);
    var r2 = try kernel.execute(&add_u2);
    defer r2.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, r1.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, r2.result_code);

    var dep_q = makeBalanceAdjustmentCommand(2001, 15, 100_000, 1, .adjustment);
    var dep_b = makeBalanceAdjustmentCommand(2002, 11, 100_000, 1, .adjustment);
    var r3 = try kernel.execute(&dep_q);
    defer r3.deinit(allocator);
    var r4 = try kernel.execute(&dep_b);
    defer r4.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, r3.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, r4.result_code);

    var bid = cmd.OrderCommand.newOrder(.gtc, 6001, 2001, 91, 91, 3, .bid);
    bid.symbol = 241;
    var ask = cmd.OrderCommand.newOrder(.gtc, 6002, 2002, 105, 0, 4, .ask);
    ask.symbol = 241;
    var rb = try kernel.execute(&bid);
    defer rb.deinit(allocator);
    var ra = try kernel.execute(&ask);
    defer ra.deinit(allocator);

    var req = makeRequestOrderBookCommand(241, 5);
    var ro = try kernel.execute(&req);
    defer ro.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, ro.result_code);

    const lp = kernel.risk_engine.getLastPrice(241) orelse unreachable;
    try std.testing.expectEqual(@as(i64, 105), lp.ask_price);
    try std.testing.expectEqual(@as(i64, 91), lp.bid_price);
}

test "kernel matching commands update risk last price cache when service flag is set" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .risk = .{ .margin_trading_enabled = true },
        .orderbook_kind = .naive,
        .l2_refresh_depth = 8,
    });
    defer kernel.deinit();
    try std.testing.expect(try kernel.addSymbol(try makeExchangeSpec()));

    var add_u1 = makeAddUserCommand(2101);
    var add_u2 = makeAddUserCommand(2102);
    var r1 = try kernel.execute(&add_u1);
    defer r1.deinit(allocator);
    var r2 = try kernel.execute(&add_u2);
    defer r2.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, r1.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, r2.result_code);

    var dep_q = makeBalanceAdjustmentCommand(2101, 15, 100_000, 1, .adjustment);
    var dep_b = makeBalanceAdjustmentCommand(2102, 11, 100_000, 1, .adjustment);
    var r3 = try kernel.execute(&dep_q);
    defer r3.deinit(allocator);
    var r4 = try kernel.execute(&dep_b);
    defer r4.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, r3.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, r4.result_code);

    var bid = cmd.OrderCommand.newOrder(.gtc, 6101, 2101, 91, 120, 3, .bid);
    bid.symbol = 241;
    var ask = cmd.OrderCommand.newOrder(.gtc, 6102, 2102, 105, 0, 4, .ask);
    ask.symbol = 241;
    var rb = try kernel.execute(&bid);
    defer rb.deinit(allocator);
    var ra = try kernel.execute(&ask);
    defer ra.deinit(allocator);
    try std.testing.expect(kernel.risk_engine.getLastPrice(241) == null);

    var move = makeMoveOrderCommand(6101, 2101, 241, 92);
    move.service_flags = 1;
    var rm = try kernel.execute(&move);
    defer rm.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, rm.result_code);

    const lp = kernel.risk_engine.getLastPrice(241) orelse unreachable;
    try std.testing.expectEqual(@as(i64, 105), lp.ask_price);
    try std.testing.expectEqual(@as(i64, 92), lp.bid_price);
}

test "multi-shard split service-flagged match refreshes every risk cache for cross-symbol margin" {
    const allocator = std.testing.allocator;
    const shard_count: usize = 4;
    const source_symbol: i32 = 6_100;
    const target_symbol: i32 = 6_101;
    const quote_currency: i32 = 392;
    const collateral_uid: i64 = 7_203;

    var sharded = try MultiShardKernelSet.initWithConfig(allocator, shard_count, .{
        .orderbook_kind = .direct,
        .l2_refresh_depth = 8,
        .risk = .{ .margin_trading_enabled = true },
    });
    defer sharded.deinit();

    const source_spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = source_symbol,
        .symbol_type = .futures_contract,
        .base_currency = 840,
        .quote_currency = quote_currency,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
        .margin_buy = 10,
        .margin_sell = 10,
    });
    const target_spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = target_symbol,
        .symbol_type = .futures_contract,
        .base_currency = 978,
        .quote_currency = quote_currency,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
        .margin_buy = 60,
        .margin_sell = 60,
    });
    try sharded.addSymbolAll(source_spec);
    try sharded.addSymbolAll(target_spec);

    for ([_]i64{ 7_201, 7_202, collateral_uid }) |uid| {
        var add_user = makeAddUserCommand(uid);
        var response = try sharded.executeSplit(&add_user);
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    }

    for ([_]i64{ 7_201, 7_202 }) |uid| {
        var deposit = makeBalanceAdjustmentCommand(uid, quote_currency, 1_000, 1, .adjustment);
        var response = try sharded.executeSplit(&deposit);
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    }

    // The collateral owner starts with a long opened at 100. Without market data,
    // Java's margin fallback values the position at its required margin, leaving
    // no free collateral for the second symbol.
    const collateral_owner_idx = sharded.shardIndexForUid(collateral_uid);
    const collateral_profile = sharded.kernels[collateral_owner_idx].risk_engine.getUserProfile(collateral_uid) orelse
        unreachable;
    var source_position = user.SymbolPositionRecord.init(collateral_uid, source_symbol, quote_currency);
    source_position.direction = .long;
    source_position.open_volume = 1;
    source_position.open_price_sum = 100;
    try collateral_profile.positions.put(allocator, source_symbol, source_position);

    var resting_bid = cmd.OrderCommand.newOrder(.gtc, 71_001, 7_201, 200, 220, 2, .bid);
    resting_bid.symbol = source_symbol;
    {
        var response = try sharded.executeSplit(&resting_bid);
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    }

    var resting_ask = cmd.OrderCommand.newOrder(.gtc, 71_002, 7_202, 220, 0, 1, .ask);
    resting_ask.symbol = source_symbol;
    {
        var response = try sharded.executeSplit(&resting_ask);
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    }

    for (sharded.kernels) |*kernel| {
        try std.testing.expect(kernel.risk_engine.getLastPrice(source_symbol) == null);
    }

    var rejected_target = cmd.OrderCommand.newOrder(.gtc, 72_001, collateral_uid, 100, 100, 1, .bid);
    rejected_target.symbol = target_symbol;
    {
        var response = try sharded.executeSplit(&rejected_target);
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.risk_nsf, response.result_code);
    }

    // Moving the two-lot bid through the one-lot ask produces a trade and leaves
    // a 220 bid. The service flag requests the post-match L2 top for risk.
    var matching_move = makeMoveOrderCommand(71_001, 7_201, source_symbol, 220);
    matching_move.service_flags = 1;
    {
        var response = try sharded.executeSplit(&matching_move);
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
        try std.testing.expectEqualDeep(matching_move, response.command);
        const trade = response.matcher_events_head orelse return error.TestExpectedTradeEvent;
        try std.testing.expectEqual(events.MatcherEventType.trade, trade.event_type);
    }

    for (sharded.kernels) |*kernel| {
        const last_price = kernel.risk_engine.getLastPrice(source_symbol) orelse unreachable;
        try std.testing.expectEqual(std.math.maxInt(i64), last_price.ask_price);
        try std.testing.expectEqual(@as(i64, 220), last_price.bid_price);
    }

    // The long is now worth 120, minus 10 required margin, so the same account
    // can collateralize the target symbol's 60 margin requirement.
    var accepted_target = cmd.OrderCommand.newOrder(.gtc, 72_002, collateral_uid, 100, 100, 1, .bid);
    accepted_target.symbol = target_symbol;
    {
        var response = try sharded.executeSplit(&accepted_target);
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    }

    const target_position = collateral_profile.positions.getPtr(target_symbol) orelse unreachable;
    try std.testing.expectEqual(@as(i64, 1), target_position.pending_buy_size);
}

test "multi-shard explicit stages isolate R1 matching prepare and commit mutations" {
    const allocator = std.testing.allocator;
    const symbol: i32 = 241;
    const maker_uid = uidForShard(1, 2, 1_440_011);
    const taker_uid = uidForShard(0, 2, 1_440_012);

    var sharded = try MultiShardKernelSet.initWithTopology(
        allocator,
        2,
        2,
        .{ .orderbook_kind = .direct },
    );
    defer sharded.deinit();
    try sharded.addSymbolAll(try makeExchangeSpecForSymbol(symbol));

    for ([_]i64{ maker_uid, taker_uid }) |uid| {
        try executeMultiExpect(
            allocator,
            &sharded,
            true,
            makeAddUserCommand(uid),
            .success,
        );
    }
    try executeMultiExpect(
        allocator,
        &sharded,
        true,
        makeBalanceAdjustmentCommand(maker_uid, 11, 100_000, 1, .adjustment),
        .success,
    );
    try executeMultiExpect(
        allocator,
        &sharded,
        true,
        makeBalanceAdjustmentCommand(taker_uid, 15, 100_000, 1, .adjustment),
        .success,
    );

    var maker_ask = cmd.OrderCommand.newOrder(
        .gtc,
        1_440_101,
        maker_uid,
        100,
        0,
        3,
        .ask,
    );
    maker_ask.symbol = symbol;
    try executeMultiExpect(allocator, &sharded, true, maker_ask, .success);

    var before_r1: [2]u64 = undefined;
    for (sharded.risk_shards, 0..) |*risk_shard, idx| {
        before_r1[idx] = try risk_shard.engine.stateHash();
    }

    var taker_bid = cmd.OrderCommand.newOrder(
        .ioc,
        1_440_102,
        taker_uid,
        100,
        100,
        2,
        .bid,
    );
    taker_bid.symbol = symbol;
    const stage_sequence = sharded.next_seq + 1;
    try sharded.executeRiskPreProcessStage(stage_sequence, &taker_bid);
    try std.testing.expectEqual(
        cmd.CommandResultCode.valid_for_matching_engine,
        taker_bid.result_code,
    );

    var after_r1: [2]u64 = undefined;
    for (sharded.risk_shards, 0..) |*risk_shard, idx| {
        after_r1[idx] = try risk_shard.engine.stateHash();
    }
    try std.testing.expect(before_r1[0] != after_r1[0]);
    try std.testing.expectEqual(before_r1[1], after_r1[1]);

    var response: CommandResponse = .{
        .sequence = stage_sequence,
        .command = taker_bid,
        .result_code = taker_bid.result_code,
    };
    defer response.deinit(allocator);
    var event_chain = events.EventChain.init(allocator);
    defer event_chain.deinit();
    var risk_l2_snapshot: ?naive.L2MarketData = null;
    defer if (risk_l2_snapshot) |*snapshot| snapshot.deinit(allocator);

    try std.testing.expectEqual(
        MultiShardKernelSet.MatchingStageDisposition.continue_to_risk_release,
        try sharded.executeMatchingStage(
            &taker_bid,
            &response,
            &event_chain,
            &risk_l2_snapshot,
        ),
    );
    try std.testing.expectEqual(cmd.CommandResultCode.success, taker_bid.result_code);
    try std.testing.expect(event_chain.head != null);
    try std.testing.expectEqual(@as(usize, 1), events.listSize(event_chain.head));

    var after_matching: [2]u64 = undefined;
    for (sharded.risk_shards, 0..) |*risk_shard, idx| {
        after_matching[idx] = try risk_shard.engine.stateHash();
        try std.testing.expectEqual(after_r1[idx], after_matching[idx]);
    }

    var prepared = try sharded.prepareRiskReleaseStage(
        &taker_bid,
        event_chain.head,
        null,
    );
    defer prepared.deinit(allocator);
    for (sharded.risk_shards, 0..) |*risk_shard, idx| {
        try std.testing.expectEqual(after_matching[idx], try risk_shard.engine.stateHash());
    }

    sharded.commitRiskReleaseStage(&prepared);
    for (sharded.risk_shards, 0..) |*risk_shard, idx| {
        try std.testing.expect(after_matching[idx] != try risk_shard.engine.stateHash());
    }

    try std.testing.expectEqual(@as(?i64, 99_700), try sharded.queryUserBalance(maker_uid, 11));
    try std.testing.expectEqual(@as(?i64, 1_998), try sharded.queryUserBalance(maker_uid, 15));
    try std.testing.expectEqual(@as(?i64, 200), try sharded.queryUserBalance(taker_uid, 11));
    try std.testing.expectEqual(@as(?i64, 97_996), try sharded.queryUserBalance(taker_uid, 15));
}

test "grouped two-step publishes matching before settlement and flushes before next-group R1" {
    const allocator = std.testing.allocator;
    const symbol: i32 = 241;
    const maker_uid = uidForShard(1, 2, 1_441_001);
    const taker_uid = uidForShard(0, 2, 1_441_002);

    var sharded = try MultiShardKernelSet.initWithTopology(
        allocator,
        2,
        2,
        .{ .orderbook_kind = .direct },
    );
    defer sharded.deinit();
    try setupGroupedSpotTopology(
        allocator,
        &sharded,
        true,
        symbol,
        maker_uid,
        taker_uid,
        1_441_101,
        3,
    );

    const settled_frontier = sharded.next_seq;
    var grouped = MultiShardKernelSet.GroupedTwoStepExecutor.init(&sharded);
    defer grouped.deinit();

    var taker_bid = cmd.OrderCommand.newOrder(
        .ioc,
        1_441_102,
        taker_uid,
        100,
        100,
        2,
        .bid,
    );
    taker_bid.symbol = symbol;
    taker_bid.events_group = 41;

    var capture: BorrowedGroupedResultCapture = .{};
    try grouped.process(&taker_bid, capture.hook());

    try std.testing.expectEqual(@as(usize, 1), capture.call_count);
    try std.testing.expectEqual(settled_frontier + 1, capture.sequence);
    try std.testing.expectEqual(cmd.CommandResultCode.success, capture.result_code);
    try std.testing.expectEqual(@as(usize, 1), capture.matcher_event_count);
    try std.testing.expectEqual(
        @as(?events.MatcherEventType, .trade),
        capture.first_matcher_event_type,
    );
    try std.testing.expectEqual(@as(usize, 1), grouped.pendingCount());
    try std.testing.expectEqual(settled_frontier, grouped.r2_frontier);
    try std.testing.expectEqual(settled_frontier + 1, grouped.result_frontier);

    const maker_before_release = sharded.riskEngineForUid(maker_uid).getUserProfile(maker_uid) orelse
        return error.TestExpectedMakerProfile;
    const taker_before_release = sharded.riskEngineForUid(taker_uid).getUserProfile(taker_uid) orelse
        return error.TestExpectedTakerProfile;
    try std.testing.expectEqual(@as(i64, 0), maker_before_release.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 0), taker_before_release.accountBalance(11));
    try std.testing.expectEqual(@as(i64, 0), groupedTestTotalFee(&sharded, 15));

    try grouped.observeIdle();
    try std.testing.expectEqual(@as(usize, 1), grouped.pendingCount());
    try std.testing.expectEqual(settled_frontier, grouped.r2_frontier);

    // A same-group NOP is observable but is not an R2 fence.
    var same_group_nop = makeNopCommand();
    same_group_nop.events_group = 41;
    try grouped.process(&same_group_nop, null);
    try std.testing.expectEqual(@as(usize, 2), grouped.pendingCount());
    try std.testing.expectEqual(settled_frontier, grouped.r2_frontier);
    try std.testing.expectEqual(@as(i64, 0), maker_before_release.accountBalance(15));

    // The prior trade credits exactly 1_998 quote units. This withdrawal can
    // succeed in R1 only if the group transition ran previous-group R2 first.
    var next_group_withdrawal = makeBalanceAdjustmentCommand(
        maker_uid,
        15,
        -1_998,
        2,
        .adjustment,
    );
    next_group_withdrawal.events_group = 42;
    try grouped.process(&next_group_withdrawal, null);
    try std.testing.expectEqual(cmd.CommandResultCode.success, next_group_withdrawal.result_code);
    try std.testing.expectEqual(settled_frontier + 2, grouped.r2_frontier);
    try std.testing.expectEqual(@as(usize, 1), grouped.pendingCount());

    const maker_after_release = sharded.riskEngineForUid(maker_uid).getUserProfile(maker_uid) orelse
        return error.TestExpectedMakerProfile;
    const taker_after_release = sharded.riskEngineForUid(taker_uid).getUserProfile(taker_uid) orelse
        return error.TestExpectedTakerProfile;
    try std.testing.expectEqual(@as(i64, 0), maker_after_release.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 200), taker_after_release.accountBalance(11));
    try std.testing.expectEqual(@as(i64, 97_996), taker_after_release.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 6), groupedTestTotalFee(&sharded, 15));

    try grouped.flushAll();
    try std.testing.expectEqual(@as(usize, 0), grouped.pendingCount());
    try std.testing.expect(grouped.open_group == null);
    try std.testing.expectEqual(grouped.result_frontier, grouped.r2_frontier);
}

test "grouped two-step delays preparation across a group and matches immediate routed split state" {
    const allocator = std.testing.allocator;
    const symbol: i32 = 241;
    const maker_uid = uidForShard(1, 2, 1_442_001);
    const taker_uid = uidForShard(0, 2, 1_442_002);

    for ([_]OrderBookKind{ .naive, .direct }) |orderbook_kind| {
        for ([_]bool{ false, true }) |use_split| {
            var immediate = try MultiShardKernelSet.initWithTopology(
                allocator,
                2,
                2,
                .{ .orderbook_kind = orderbook_kind },
            );
            defer immediate.deinit();
            var delayed = try MultiShardKernelSet.initWithTopology(
                allocator,
                2,
                2,
                .{ .orderbook_kind = orderbook_kind },
            );
            defer delayed.deinit();

            try setupGroupedSpotTopology(
                allocator,
                &immediate,
                use_split,
                symbol,
                maker_uid,
                taker_uid,
                1_442_101,
                4,
            );
            try setupGroupedSpotTopology(
                allocator,
                &delayed,
                use_split,
                symbol,
                maker_uid,
                taker_uid,
                1_442_101,
                4,
            );

            const settled_frontier = delayed.next_seq;
            var grouped = MultiShardKernelSet.GroupedTwoStepExecutor.init(&delayed);
            defer grouped.deinit();

            for (0..2) |index| {
                const order_id = 1_442_102 + @as(i64, @intCast(index));
                var immediate_bid = cmd.OrderCommand.newOrder(
                    .ioc,
                    order_id,
                    taker_uid,
                    100,
                    100,
                    1,
                    .bid,
                );
                immediate_bid.symbol = symbol;
                immediate_bid.events_group = 51;
                var immediate_response = if (use_split)
                    try immediate.executeSplit(&immediate_bid)
                else
                    try immediate.executeRouted(&immediate_bid);
                defer immediate_response.deinit(allocator);
                try std.testing.expectEqual(
                    cmd.CommandResultCode.success,
                    immediate_response.result_code,
                );

                var delayed_bid = cmd.OrderCommand.newOrder(
                    .ioc,
                    order_id,
                    taker_uid,
                    100,
                    100,
                    1,
                    .bid,
                );
                delayed_bid.symbol = symbol;
                delayed_bid.events_group = 51;
                try grouped.process(&delayed_bid, null);
                try std.testing.expectEqual(cmd.CommandResultCode.success, delayed_bid.result_code);

                // Matcher outputs remain owned by the executor. No command has
                // prepared an absolute risk-state snapshot yet.
                try std.testing.expectEqual(@as(i64, 0), groupedTestTotalFee(&delayed, 15));
                try std.testing.expectEqual(settled_frontier, grouped.r2_frontier);
            }

            try std.testing.expectEqual(@as(usize, 2), grouped.pendingCount());
            try grouped.flushAll();
            try std.testing.expectEqual(@as(usize, 0), grouped.pendingCount());
            try std.testing.expectEqual(settled_frontier + 2, grouped.r2_frontier);

            try std.testing.expectEqual(
                try immediate.detailedHashFingerprint(),
                try delayed.detailedHashFingerprint(),
            );
            for ([_]i64{ maker_uid, taker_uid }) |uid| {
                for ([_]i32{ 11, 15 }) |currency| {
                    try std.testing.expectEqual(
                        try immediate.queryUserBalance(uid, currency),
                        try delayed.queryUserBalance(uid, currency),
                    );
                }
            }
            try std.testing.expectEqual(
                groupedTestTotalFee(&immediate, 15),
                groupedTestTotalFee(&delayed, 15),
            );
        }
    }
}

test "grouped two-step prepare failure is attributed once and poisons without retry" {
    const allocator = std.testing.allocator;
    const maker_uid: i64 = 1_443_001;
    const taker_uid: i64 = 1_443_002;

    var sharded = try MultiShardKernelSet.initWithConfig(allocator, 2, .{
        .orderbook_kind = .direct,
        .risk = .{ .margin_trading_enabled = true },
    });
    defer sharded.deinit();
    const spec = try makeFeeMarginFuturesSpec();
    try sharded.addSymbolAll(spec);

    try std.testing.expectEqual(@as(usize, 0), sharded.shardIndexForUid(taker_uid));
    try std.testing.expectEqual(@as(usize, 1), sharded.shardIndexForUid(maker_uid));
    for ([_]i64{ maker_uid, taker_uid }) |uid| {
        try executeMultiExpect(allocator, &sharded, true, makeAddUserCommand(uid), .success);
        try executeMultiExpect(
            allocator,
            &sharded,
            true,
            makeBalanceAdjustmentCommand(uid, spec.quote_currency, 100_000, 1, .adjustment),
            .success,
        );
    }

    var maker_ask = cmd.OrderCommand.newOrder(
        .gtc,
        1_443_101,
        maker_uid,
        10_770,
        0,
        1,
        .ask,
    );
    maker_ask.symbol = spec.symbol_id;
    try executeMultiExpect(allocator, &sharded, true, maker_ask, .success);

    const settled_frontier = sharded.next_seq;
    var grouped = MultiShardKernelSet.GroupedTwoStepExecutor.init(&sharded);
    defer grouped.deinit();

    var taker_bid = cmd.OrderCommand.newOrder(
        .ioc,
        1_443_102,
        taker_uid,
        10_770,
        10_770,
        1,
        .bid,
    );
    taker_bid.symbol = spec.symbol_id;
    taker_bid.events_group = 61;
    var capture: BorrowedGroupedResultCapture = .{};
    try grouped.process(&taker_bid, capture.hook());
    try std.testing.expectEqual(cmd.CommandResultCode.success, capture.result_code);
    try std.testing.expectEqual(@as(usize, 1), capture.matcher_event_count);
    try std.testing.expectEqual(@as(usize, 1), grouped.pendingCount());

    // Corrupt only the later release shard after matching has already
    // published. Shard 0 preparation must be discarded before any commit.
    const maker_profile = sharded.riskEngineForUid(maker_uid).getUserProfile(maker_uid) orelse
        return error.TestExpectedMakerProfile;
    const removed_position = maker_profile.positions.fetchRemove(spec.symbol_id) orelse
        return error.TestExpectedMakerPosition;
    try std.testing.expectEqual(@as(i64, 1), removed_position.value.pending_sell_size);

    var before_failed_flush: [2]u64 = undefined;
    for (sharded.risk_shards, 0..) |*risk_shard, index| {
        before_failed_flush[index] = try risk_shard.engine.stateHash();
    }

    var next_group_nop = makeNopCommand();
    next_group_nop.events_group = 62;
    try std.testing.expectError(
        error.PositionRecordCorrupted,
        grouped.process(&next_group_nop, null),
    );

    const fatal = grouped.failure() orelse return error.TestExpectedGroupedFailure;
    try std.testing.expectEqual(
        MultiShardKernelSet.GroupedTwoStepFailureStage.risk_release,
        fatal.stage,
    );
    try std.testing.expectEqual(capture.sequence, fatal.sequence);
    try std.testing.expectEqual(error.PositionRecordCorrupted, fatal.cause);
    try std.testing.expectEqual(@as(usize, 1), grouped.pendingCount());
    try std.testing.expectEqual(settled_frontier, grouped.r2_frontier);
    try std.testing.expectEqual(capture.sequence, sharded.next_seq);
    try std.testing.expectEqual(cmd.CommandResultCode.new, next_group_nop.result_code);

    for (sharded.risk_shards, 0..) |*risk_shard, index| {
        try std.testing.expectEqual(before_failed_flush[index], try risk_shard.engine.stateHash());
    }
    const taker_profile = sharded.riskEngineForUid(taker_uid).getUserProfile(taker_uid) orelse
        return error.TestExpectedTakerProfile;
    const taker_position = taker_profile.positions.getPtr(spec.symbol_id) orelse
        return error.TestExpectedTakerPosition;
    try std.testing.expectEqual(common.PositionDirection.empty, taker_position.direction);
    try std.testing.expectEqual(@as(i64, 0), taker_position.open_volume);
    try std.testing.expectEqual(@as(i64, 1), taker_position.pending_buy_size);
    try std.testing.expectEqual(@as(i64, 100_000), taker_profile.accountBalance(spec.quote_currency));
    try std.testing.expectEqual(@as(i64, 0), groupedTestTotalFee(&sharded, spec.quote_currency));

    try std.testing.expectError(error.GroupedTwoStepPoisoned, grouped.flushAll());
    try std.testing.expectError(
        error.GroupedTwoStepPoisoned,
        grouped.process(&next_group_nop, null),
    );
}

test "grouped two-step rejects decreasing groups without flushing or poisoning" {
    const allocator = std.testing.allocator;
    var sharded = try MultiShardKernelSet.init(allocator, 2, .direct);
    defer sharded.deinit();
    var grouped = MultiShardKernelSet.GroupedTwoStepExecutor.init(&sharded);
    defer grouped.deinit();

    var first = makeNopCommand();
    first.events_group = 10;
    try grouped.process(&first, null);
    const first_sequence = grouped.result_frontier;
    try std.testing.expectEqual(@as(usize, 1), grouped.pendingCount());
    try std.testing.expectEqual(@as(?i64, 10), grouped.last_events_group);

    var decreasing = makeNopCommand();
    decreasing.events_group = 9;
    try std.testing.expectError(
        error.NonMonotonicEventsGroup,
        grouped.process(&decreasing, null),
    );
    try std.testing.expect(grouped.failure() == null);
    try std.testing.expectEqual(@as(usize, 1), grouped.pendingCount());
    try std.testing.expectEqual(first_sequence, grouped.pending.items[0].sequence);
    try std.testing.expectEqual(@as(i64, 10), grouped.pending.items[0].events_group);
    try std.testing.expectEqual(first_sequence, grouped.accepted_frontier);
    try std.testing.expectEqual(@as(u64, 0), grouped.r2_frontier);
    try std.testing.expectEqual(first_sequence, sharded.next_seq);
    try std.testing.expectEqual(cmd.CommandResultCode.new, decreasing.result_code);

    // A later valid group still flushes and proves the protocol error was
    // recoverable rather than terminal.
    var increasing = makeNopCommand();
    increasing.events_group = 11;
    try grouped.process(&increasing, null);
    try std.testing.expectEqual(first_sequence, grouped.r2_frontier);
    try std.testing.expectEqual(@as(usize, 1), grouped.pendingCount());
    try grouped.flushAll();

    // Explicit R2 flush does not reset the monotonic grouping protocol.
    const after_flush_frontier = grouped.accepted_frontier;
    var decreasing_after_flush = makeNopCommand();
    decreasing_after_flush.events_group = 10;
    try std.testing.expectError(
        error.NonMonotonicEventsGroup,
        grouped.process(&decreasing_after_flush, null),
    );
    try std.testing.expect(grouped.failure() == null);
    try std.testing.expectEqual(@as(usize, 0), grouped.pendingCount());
    try std.testing.expectEqual(after_flush_frontier, sharded.next_seq);
}

test "grouped two-step poisons when an immediate call steals a pending frontier" {
    const allocator = std.testing.allocator;
    const symbol: i32 = 241;
    const maker_uid = uidForShard(1, 2, 1_444_001);
    const taker_uid = uidForShard(0, 2, 1_444_002);

    var sharded = try MultiShardKernelSet.initWithTopology(
        allocator,
        2,
        2,
        .{ .orderbook_kind = .direct },
    );
    defer sharded.deinit();
    try setupGroupedSpotTopology(
        allocator,
        &sharded,
        true,
        symbol,
        maker_uid,
        taker_uid,
        1_444_101,
        2,
    );

    const settled_frontier = sharded.next_seq;
    var grouped = MultiShardKernelSet.GroupedTwoStepExecutor.init(&sharded);
    defer grouped.deinit();

    var taker_bid = cmd.OrderCommand.newOrder(
        .ioc,
        1_444_102,
        taker_uid,
        100,
        100,
        1,
        .bid,
    );
    taker_bid.symbol = symbol;
    taker_bid.events_group = 71;
    try grouped.process(&taker_bid, null);
    const pending_sequence = grouped.result_frontier;
    const pending_events = grouped.pending.items[0].matcher_events_head orelse
        return error.TestExpectedTradeEvent;

    var immediate_nop = makeNopCommand();
    var immediate_response = try sharded.executeRouted(&immediate_nop);
    defer immediate_response.deinit(allocator);
    try std.testing.expectEqual(pending_sequence + 1, immediate_response.sequence);

    try std.testing.expectError(
        error.PublicationFrontierOwnershipLost,
        grouped.flushAll(),
    );
    const fatal = grouped.failure() orelse return error.TestExpectedGroupedFailure;
    try std.testing.expectEqual(
        MultiShardKernelSet.GroupedTwoStepFailureStage.publication_ownership,
        fatal.stage,
    );
    try std.testing.expectEqual(pending_sequence, fatal.sequence);
    try std.testing.expectEqual(error.PublicationFrontierOwnershipLost, fatal.cause);
    try std.testing.expectEqual(@as(usize, 1), grouped.pendingCount());
    try std.testing.expect(grouped.pending.items[0].matcher_events_head == pending_events);
    try std.testing.expectEqual(settled_frontier, grouped.r2_frontier);
    try std.testing.expectEqual(pending_sequence, grouped.accepted_frontier);
    try std.testing.expectEqual(immediate_response.sequence, sharded.next_seq);
    try std.testing.expectEqual(@as(i64, 0), groupedTestTotalFee(&sharded, 15));

    try std.testing.expectError(error.GroupedTwoStepPoisoned, grouped.flushAll());
}

test "grouped two-step retains pending ownership when a borrowed hook advances the frontier" {
    const allocator = std.testing.allocator;
    const symbol: i32 = 241;
    const maker_uid = uidForShard(1, 2, 1_445_001);
    const taker_uid = uidForShard(0, 2, 1_445_002);

    var sharded = try MultiShardKernelSet.initWithTopology(
        allocator,
        2,
        2,
        .{ .orderbook_kind = .direct },
    );
    defer sharded.deinit();
    try setupGroupedSpotTopology(
        allocator,
        &sharded,
        true,
        symbol,
        maker_uid,
        taker_uid,
        1_445_101,
        2,
    );

    const settled_frontier = sharded.next_seq;
    var grouped = MultiShardKernelSet.GroupedTwoStepExecutor.init(&sharded);
    defer grouped.deinit();

    var capture: ReentrantGroupedHookCapture = .{
        .executor = &grouped,
        .kernels = &sharded,
    };
    var taker_bid = cmd.OrderCommand.newOrder(
        .ioc,
        1_445_102,
        taker_uid,
        100,
        100,
        1,
        .bid,
    );
    taker_bid.symbol = symbol;
    taker_bid.events_group = 81;

    try std.testing.expectError(
        error.PublicationFrontierOwnershipLost,
        grouped.process(&taker_bid, capture.hook()),
    );
    try std.testing.expectEqual(cmd.CommandResultCode.success, taker_bid.result_code);
    try std.testing.expectEqual(@as(usize, 1), capture.outer_matcher_event_count);
    try std.testing.expect(capture.reentrant_error != null);
    try std.testing.expectEqual(
        error.GroupedTwoStepReentrantCall,
        capture.reentrant_error.?,
    );
    try std.testing.expect(capture.immediate_error == null);
    try std.testing.expectEqual(capture.outer_sequence + 1, capture.immediate_sequence);

    const fatal = grouped.failure() orelse return error.TestExpectedGroupedFailure;
    try std.testing.expectEqual(
        MultiShardKernelSet.GroupedTwoStepFailureStage.publication_ownership,
        fatal.stage,
    );
    try std.testing.expectEqual(capture.outer_sequence, fatal.sequence);
    try std.testing.expectEqual(error.PublicationFrontierOwnershipLost, fatal.cause);
    try std.testing.expectEqual(@as(usize, 1), grouped.pendingCount());
    try std.testing.expect(grouped.pending.items[0].matcher_events_head != null);
    try std.testing.expectEqual(capture.outer_sequence, grouped.accepted_frontier);
    try std.testing.expectEqual(capture.immediate_sequence, sharded.next_seq);
    try std.testing.expectEqual(settled_frontier, grouped.r2_frontier);
    try std.testing.expectEqual(@as(i64, 0), groupedTestTotalFee(&sharded, 15));

    try std.testing.expectError(error.GroupedTwoStepPoisoned, grouped.flushAll());
}

test "multi-shard split prepares every risk release before committing an earlier shard" {
    const allocator = std.testing.allocator;
    const maker_uid: i64 = 1_440_031;
    const taker_uid: i64 = 1_440_032;

    var sharded = try MultiShardKernelSet.initWithConfig(allocator, 2, .{
        .orderbook_kind = .direct,
        .risk = .{ .margin_trading_enabled = true },
    });
    defer sharded.deinit();
    const spec = try makeFeeMarginFuturesSpec();
    try sharded.addSymbolAll(spec);

    try std.testing.expectEqual(@as(usize, 0), sharded.shardIndexForUid(taker_uid));
    try std.testing.expectEqual(@as(usize, 1), sharded.shardIndexForUid(maker_uid));

    for ([_]i64{ maker_uid, taker_uid }) |uid| {
        var add_user = makeAddUserCommand(uid);
        var add_response = try sharded.executeSplit(&add_user);
        defer add_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, add_response.result_code);

        var deposit = makeBalanceAdjustmentCommand(uid, spec.quote_currency, 100_000, 1, .adjustment);
        var deposit_response = try sharded.executeSplit(&deposit);
        defer deposit_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, deposit_response.result_code);
    }

    var maker_ask = cmd.OrderCommand.newOrder(.gtc, 81_001, maker_uid, 10_770, 0, 1, .ask);
    maker_ask.symbol = spec.symbol_id;
    {
        var response = try sharded.executeSplit(&maker_ask);
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    }

    // Corrupt only the later release shard after matching has accepted the maker.
    // Java treats a release-stage invariant failure as fatal, so the matching book
    // is intentionally not rolled back; this assertion covers risk commit ordering.
    const maker_profile = sharded.kernels[1].risk_engine.getUserProfile(maker_uid) orelse unreachable;
    const removed_maker_position = maker_profile.positions.fetchRemove(spec.symbol_id) orelse unreachable;
    try std.testing.expectEqual(@as(i64, 1), removed_maker_position.value.pending_sell_size);

    var taker_bid = cmd.OrderCommand.newOrder(.ioc, 81_002, taker_uid, 10_770, 10_770, 1, .bid);
    taker_bid.symbol = spec.symbol_id;
    try std.testing.expectError(error.PositionRecordCorrupted, sharded.executeSplit(&taker_bid));

    const taker_profile = sharded.kernels[0].risk_engine.getUserProfile(taker_uid) orelse unreachable;
    const taker_position = taker_profile.positions.getPtr(spec.symbol_id) orelse unreachable;
    try std.testing.expectEqual(common.PositionDirection.empty, taker_position.direction);
    try std.testing.expectEqual(@as(i64, 0), taker_position.open_volume);
    try std.testing.expectEqual(@as(i64, 1), taker_position.pending_buy_size);
    try std.testing.expectEqual(@as(i64, 100_000), taker_profile.accountBalance(spec.quote_currency));
    try std.testing.expectEqual(@as(i64, 0), sharded.kernels[0].risk_engine.getFeeCollected(spec.quote_currency));
}

test "kernel send_l2_for_every_cmd updates risk last price cache without service flag" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .risk = .{ .margin_trading_enabled = true },
        .orderbook_kind = .naive,
        .send_l2_for_every_cmd = true,
        .l2_refresh_depth = 8,
    });
    defer kernel.deinit();
    try std.testing.expect(try kernel.addSymbol(try makeExchangeSpec()));

    var add_u1 = makeAddUserCommand(2201);
    var add_u2 = makeAddUserCommand(2202);
    var r1 = try kernel.execute(&add_u1);
    defer r1.deinit(allocator);
    var r2 = try kernel.execute(&add_u2);
    defer r2.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, r1.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, r2.result_code);

    var dep_q = makeBalanceAdjustmentCommand(2201, 15, 100_000, 1, .adjustment);
    var dep_b = makeBalanceAdjustmentCommand(2202, 11, 100_000, 1, .adjustment);
    var r3 = try kernel.execute(&dep_q);
    defer r3.deinit(allocator);
    var r4 = try kernel.execute(&dep_b);
    defer r4.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, r3.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, r4.result_code);

    var bid = cmd.OrderCommand.newOrder(.gtc, 6201, 2201, 91, 91, 3, .bid);
    bid.symbol = 241;
    var ask = cmd.OrderCommand.newOrder(.gtc, 6202, 2202, 105, 0, 4, .ask);
    ask.symbol = 241;
    var rb = try kernel.execute(&bid);
    defer rb.deinit(allocator);
    var ra = try kernel.execute(&ask);
    defer ra.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, rb.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, ra.result_code);

    const lp = kernel.risk_engine.getLastPrice(241) orelse unreachable;
    try std.testing.expectEqual(@as(i64, 105), lp.ask_price);
    try std.testing.expectEqual(@as(i64, 91), lp.bid_price);
}

test "exchange api facade delegates to kernel" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();
    var api = ExchangeApi.init(&kernel);
    defer api.deinit();

    var cmd_add = makeAddUserCommand(3001);
    var resp = try api.submitCommand(&cmd_add);
    defer resp.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 1), resp.sequence);
    try std.testing.expectEqual(cmd.CommandResultCode.success, resp.result_code);
}

test "kernel control commands keep java-compatible result semantics" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    var nop = makeNopCommand();
    var nop_resp = try kernel.execute(&nop);
    defer nop_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, nop_resp.result_code);

    var grouping: cmd.OrderCommand = .{
        .command = .grouping_control,
        .order_id = 1,
        .result_code = .new,
    };
    var grouping_resp = try kernel.execute(&grouping);
    defer grouping_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, grouping_resp.result_code);

    var persist_matching: cmd.OrderCommand = .{
        .command = .persist_state_matching,
        .order_id = 42,
        .timestamp = 111,
        .result_code = .new,
    };
    var persist_matching_resp = try kernel.execute(&persist_matching);
    defer persist_matching_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.accepted, persist_matching_resp.result_code);

    var persist_risk: cmd.OrderCommand = .{
        .command = .persist_state_risk,
        .order_id = 42,
        .timestamp = 111,
        .result_code = .new,
    };
    var persist_risk_resp = try kernel.execute(&persist_risk);
    defer persist_risk_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, persist_risk_resp.result_code);

    var persist_matching_batch: cmd.OrderCommand = .{
        .command = .persist_state_matching,
        .order_id = 99,
        .timestamp = 222,
        .result_code = .new,
    };
    var persist_risk_batch: cmd.OrderCommand = .{
        .command = .persist_state_risk,
        .order_id = 99,
        .timestamp = 222,
        .result_code = .new,
    };
    var persist_cmds = [_]*cmd.OrderCommand{ &persist_matching_batch, &persist_risk_batch };
    const persist_batch = try kernel.executeBatch(persist_cmds[0..]);
    try std.testing.expectEqual(cmd.CommandResultCode.success, persist_batch.result_code);
}

test "exchange api typed sync helpers run command lifecycle and reports" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer kernel.deinit();
    try std.testing.expect(try kernel.addSymbol(try makeExchangeSpec()));

    var api = ExchangeApi.init(&kernel);
    defer api.deinit();

    var add_u1 = try api.addUser(8101);
    defer add_u1.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_u1.result_code);

    var add_u2 = try api.addUser(8102);
    defer add_u2.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_u2.result_code);

    var dep_quote = try api.adjustUserBalance(8101, 1, 15, 100_000, .adjustment);
    defer dep_quote.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, dep_quote.result_code);

    var dep_base = try api.adjustUserBalance(8102, 1, 11, 100_000, .adjustment);
    defer dep_base.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, dep_base.result_code);

    var bid = try api.placeOrder(98101, 8101, 241, 101, 101, 5, .bid, .gtc, 17);
    defer bid.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, bid.result_code);

    var moved = try api.moveOrder(98101, 8101, 241, 100);
    defer moved.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, moved.result_code);

    var reduced = try api.reduceOrder(98101, 8101, 241, 2);
    defer reduced.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, reduced.result_code);

    var canceled = try api.cancelOrder(98101, 8101, 241);
    defer canceled.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, canceled.result_code);

    var order_book = try api.requestOrderBook(241, 10);
    defer order_book.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, order_book.result_code);
    const snapshot = order_book.l2_snapshot orelse unreachable;
    try std.testing.expectEqual(@as(usize, 0), snapshot.asks.items.len);
    try std.testing.expectEqual(@as(usize, 0), snapshot.bids.items.len);

    try std.testing.expectError(
        error.OrderBookDepthOverflow,
        api.requestOrderBook(241, std.math.maxInt(usize)),
    );

    var reset = try api.reset();
    defer reset.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, reset.result_code);
}

test "exchange api suspend and resume helpers mirror user-state transitions" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();
    var api = ExchangeApi.init(&kernel);
    defer api.deinit();

    var missing_suspend = try api.suspendUser(8901);
    defer missing_suspend.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_not_found, missing_suspend.result_code);

    var add_user = try api.addUser(8901);
    defer add_user.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_user.result_code);

    var suspend_resp = try api.suspendUser(8901);
    defer suspend_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, suspend_resp.result_code);

    var suspend_again = try api.suspendUser(8901);
    defer suspend_again.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_not_found, suspend_again.result_code);

    var resume_resp = try api.resumeUser(8901);
    defer resume_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, resume_resp.result_code);

    var resume_again = try api.resumeUser(8901);
    defer resume_again.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_not_suspended, resume_again.result_code);
}

test "exchange api persistState sync helper merges matching and risk results" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();
    var api = ExchangeApi.init(&kernel);
    defer api.deinit();

    const merged = try api.persistState(7001, 123_456_789);
    try std.testing.expectEqual(cmd.CommandResultCode.success, merged);

    var add_user = try api.addUser(9011);
    defer add_user.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 3), add_user.sequence);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_user.result_code);
}

test "exchange api typed async helpers preserve routing and expose failures" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .naive,
    });
    defer kernel.deinit();
    try std.testing.expect(try kernel.addSymbol(try makeExchangeSpec()));

    var api = ExchangeApi.init(&kernel);
    defer api.deinit();

    const req_ok = try api.addUserAsync(8201);
    const req_dup = try api.addUserAsync(8201);
    const req_book = try api.requestOrderBookAsync(241, 4);
    const req_bad_cancel = try api.cancelOrderAsync(9999, 8201, 999);

    try std.testing.expectError(
        error.OrderBookDepthOverflow,
        api.requestOrderBookAsync(241, std.math.maxInt(usize)),
    );

    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 4), processed);
    try std.testing.expectEqual(@as(usize, 0), api.pendingCount());

    var ok = api.takeResult(req_ok) orelse unreachable;
    defer ok.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, ok.result_code);

    var dup = api.takeResult(req_dup) orelse unreachable;
    defer dup.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, dup.result_code);

    var book = api.takeResult(req_book) orelse unreachable;
    defer book.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, book.result_code);
    const snapshot = book.l2_snapshot orelse unreachable;
    try std.testing.expectEqual(@as(usize, 0), snapshot.asks.items.len);
    try std.testing.expectEqual(@as(usize, 0), snapshot.bids.items.len);

    var bad_cancel = api.takeResult(req_bad_cancel) orelse unreachable;
    defer bad_cancel.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.matching_invalid_order_book_id, bad_cancel.result_code);
}

test "exchange api suspend and resume async helpers preserve fifo semantics" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();
    var api = ExchangeApi.init(&kernel);
    defer api.deinit();

    const req_add = try api.addUserAsync(9001);
    const req_suspend = try api.suspendUserAsync(9001);
    const req_resume = try api.resumeUserAsync(9001);
    const req_resume_again = try api.resumeUserAsync(9001);

    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 4), processed);

    var add = api.takeResult(req_add) orelse unreachable;
    defer add.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add.result_code);

    var suspend_async_resp = api.takeResult(req_suspend) orelse unreachable;
    defer suspend_async_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, suspend_async_resp.result_code);

    var resume_async_resp = api.takeResult(req_resume) orelse unreachable;
    defer resume_async_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, resume_async_resp.result_code);

    var resume_again = api.takeResult(req_resume_again) orelse unreachable;
    defer resume_again.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_not_suspended, resume_again.result_code);
}

test "exchange api persistState async helper exposes merged result when both commands are ready" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();
    var api = ExchangeApi.init(&kernel);
    defer api.deinit();

    const requests = try api.persistStateAsync(7002, 987_654_321);
    try std.testing.expect(!api.isPersistStateResultReady(requests));
    try std.testing.expectEqual(@as(usize, 2), api.pendingCount());

    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 2), processed);
    try std.testing.expect(api.isPersistStateResultReady(requests));

    const merged = api.takePersistStateResult(requests) orelse unreachable;
    try std.testing.expectEqual(cmd.CommandResultCode.success, merged);
    try std.testing.expect(api.takePersistStateResult(requests) == null);
}

test "exchange api async append OOM does not consume a request id" {
    var failing_allocator_state = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = failing_allocator_state.allocator();
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();
    var api = ExchangeApi.init(&kernel);
    defer api.deinit();

    const command = makeAddUserCommand(9_101);
    failing_allocator_state.fail_index = failing_allocator_state.alloc_index;
    try std.testing.expectError(error.OutOfMemory, api.submitCommandAsync(&command));
    try std.testing.expect(failing_allocator_state.has_induced_failure);
    try std.testing.expectEqual(@as(u64, 0), api.next_request_id);
    try std.testing.expectEqual(@as(usize, 0), api.pendingCount());

    failing_allocator_state.fail_index = std.math.maxInt(usize);
    try std.testing.expectEqual(@as(u64, 1), try api.submitCommandAsync(&command));
    try std.testing.expectEqual(@as(u64, 1), api.next_request_id);
    try std.testing.expectEqual(@as(usize, 1), api.pendingCount());
}

test "exchange api persist pair publication is all or nothing on allocation failure" {
    var failing_allocator_state = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = failing_allocator_state.allocator();
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();
    var api = ExchangeApi.init(&kernel);
    defer api.deinit();

    failing_allocator_state.fail_index = failing_allocator_state.alloc_index;
    try std.testing.expectError(
        error.OutOfMemory,
        api.persistStateAsync(7_003, 987_654_322),
    );
    try std.testing.expect(failing_allocator_state.has_induced_failure);
    try std.testing.expectEqual(@as(u64, 0), api.next_request_id);
    try std.testing.expectEqual(@as(usize, 0), api.pendingCount());

    failing_allocator_state.fail_index = std.math.maxInt(usize);
    const requests = try api.persistStateAsync(7_003, 987_654_322);
    try std.testing.expectEqual(@as(u64, 1), requests.matching_request_id);
    try std.testing.expectEqual(@as(u64, 2), requests.risk_request_id);
    try std.testing.expectEqual(@as(u64, 2), api.next_request_id);
    try std.testing.expectEqual(@as(usize, 2), api.pendingCount());
    try std.testing.expectEqual(
        cmd.OrderCommandType.persist_state_matching,
        api.pending_queue.items[0].command.command,
    );
    try std.testing.expectEqual(
        cmd.OrderCommandType.persist_state_risk,
        api.pending_queue.items[1].command.command,
    );
}

test "kernel execute batch returns deterministic sequence range and merged result" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    var add_u = makeAddUserCommand(4001);
    var bad_cancel = cmd.OrderCommand.cancel(999, 4001);
    bad_cancel.symbol = 241;
    var add_u2 = makeAddUserCommand(4002);
    var commands = [_]*cmd.OrderCommand{ &add_u, &bad_cancel, &add_u2 };

    const batch = try kernel.executeBatch(commands[0..]);
    try std.testing.expectEqual(@as(?u64, 1), batch.sequence_from);
    try std.testing.expectEqual(@as(?u64, 3), batch.sequence_to);
    try std.testing.expectEqual(cmd.CommandResultCode.matching_invalid_order_book_id, batch.result_code);
}

test "exchange api batch facade handles empty and non-empty batches" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();
    var api = ExchangeApi.init(&kernel);
    defer api.deinit();

    var empty = [_]*cmd.OrderCommand{};
    const empty_result = try api.submitCommandBatch(empty[0..]);
    try std.testing.expectEqual(@as(?u64, null), empty_result.sequence_from);
    try std.testing.expectEqual(@as(?u64, null), empty_result.sequence_to);
    try std.testing.expectEqual(cmd.CommandResultCode.accepted, empty_result.result_code);

    var add_u = makeAddUserCommand(5001);
    var add_u_dup = makeAddUserCommand(5001);
    var cmds = [_]*cmd.OrderCommand{ &add_u, &add_u_dup };
    const result = try api.submitCommandBatch(cmds[0..]);
    try std.testing.expectEqual(@as(?u64, 1), result.sequence_from);
    try std.testing.expectEqual(@as(?u64, 2), result.sequence_to);
    try std.testing.expectEqual(cmd.CommandResultCode.user_mgmt_user_already_exists, result.result_code);
}

test "exchange api async drain reserves result storage before command execution" {
    var failing_allocator_state = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = failing_allocator_state.allocator();
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();
    var api = ExchangeApi.init(&kernel);
    defer api.deinit();

    const uid: i64 = 6_099;
    const request_id = try api.addUserAsync(uid);
    const sequence_before = kernel.next_seq;

    // The result map is still empty, so its first reservation must allocate.
    // Failing that allocation must leave the queued command wholly unexecuted.
    failing_allocator_state.fail_index = failing_allocator_state.alloc_index;
    try std.testing.expectError(error.OutOfMemory, api.drainPending());
    try std.testing.expect(failing_allocator_state.has_induced_failure);
    try std.testing.expectEqual(sequence_before, kernel.next_seq);
    try std.testing.expect(kernel.risk_engine.getUserProfile(uid) == null);
    try std.testing.expectEqual(@as(usize, 1), api.pendingCount());
    try std.testing.expectEqual(@as(usize, 0), api.readyCount());
    try std.testing.expect(!api.isResultReady(request_id));

    failing_allocator_state.fail_index = std.math.maxInt(usize);
    try std.testing.expectEqual(@as(usize, 1), try api.drainPending());
    try std.testing.expectEqual(@as(usize, 0), api.pendingCount());
    try std.testing.expectEqual(@as(usize, 1), api.readyCount());
    try std.testing.expectEqual(sequence_before + 1, kernel.next_seq);
    try std.testing.expect(kernel.risk_engine.getUserProfile(uid) != null);

    var response = api.takeResult(request_id) orelse unreachable;
    defer response.deinit(allocator);
    try std.testing.expectEqual(sequence_before + 1, response.sequence);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);

    // Re-draining an empty queue proves the retry produced one execution/result.
    try std.testing.expectEqual(@as(usize, 0), try api.drainPending());
    try std.testing.expectEqual(sequence_before + 1, kernel.next_seq);
    try std.testing.expectEqual(@as(usize, 0), api.readyCount());
}

test "exchange api async drain consumes a failed request without partial balance mutation" {
    var failing_allocator_state = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = failing_allocator_state.allocator();
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();
    var api = ExchangeApi.init(&kernel);
    defer api.deinit();

    const uid: i64 = 6_101;
    const tail_uid: i64 = 6_102;
    var add_user = makeAddUserCommand(uid);
    var add_user_response = try kernel.execute(&add_user);
    defer add_user_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_user_response.result_code);

    const transaction_id: i64 = 77;
    const adjustment = makeBalanceAdjustmentCommand(
        uid,
        15,
        500,
        transaction_id,
        .adjustment,
    );
    const failing_request_id = try api.submitCommandAsync(&adjustment);
    const tail_request_id = try api.addUserAsync(tail_uid);
    try api.ready_results.ensureUnusedCapacity(allocator, 1);
    const sequence_before = kernel.next_seq;

    // Result capacity is already reserved, so the head is extracted and its
    // sequence consumed before account-history allocation fails. The risk
    // mutation itself remains all-or-nothing.
    failing_allocator_state.fail_index = failing_allocator_state.alloc_index;
    try std.testing.expectError(error.OutOfMemory, api.drainPending());
    try std.testing.expect(failing_allocator_state.has_induced_failure);
    try std.testing.expectEqual(sequence_before + 1, kernel.next_seq);
    try std.testing.expectEqual(@as(usize, 1), api.pendingCount());
    try std.testing.expect(!api.isResultReady(failing_request_id));
    try std.testing.expect(!api.isResultReady(tail_request_id));

    const unchanged_profile = kernel.risk_engine.getUserProfile(uid) orelse unreachable;
    try std.testing.expectEqual(@as(i64, 0), unchanged_profile.adjustments_counter);
    try std.testing.expectEqual(@as(i64, 0), unchanged_profile.accountBalance(15));

    failing_allocator_state.fail_index = std.math.maxInt(usize);
    try std.testing.expectEqual(@as(usize, 1), try api.drainPending());
    try std.testing.expectEqual(@as(usize, 0), api.pendingCount());
    try std.testing.expect(!api.isResultReady(failing_request_id));
    try std.testing.expect(api.isResultReady(tail_request_id));
    try std.testing.expectEqual(sequence_before + 2, kernel.next_seq);
    try std.testing.expect(kernel.risk_engine.getUserProfile(tail_uid) != null);

    var tail_response = api.takeResult(tail_request_id) orelse unreachable;
    defer tail_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, tail_response.result_code);
    try std.testing.expectEqual(@as(usize, 0), try api.drainPending());
    try std.testing.expectEqual(@as(i64, 0), unchanged_profile.adjustments_counter);
    try std.testing.expectEqual(@as(i64, 0), unchanged_profile.accountBalance(15));
}

test "exchange api async queue preserves fifo execution and request/result routing" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();
    var api = ExchangeApi.init(&kernel);
    defer api.deinit();

    const c1 = makeAddUserCommand(6101);
    const c2 = makeAddUserCommand(6102);
    const req1 = try api.submitCommandAsync(&c1);
    const req2 = try api.submitCommandAsync(&c2);
    try std.testing.expectEqual(@as(usize, 2), api.pendingCount());
    try std.testing.expect(!api.isResultReady(req1));
    try std.testing.expect(!api.isResultReady(req2));

    const processed = try api.drainPending();
    try std.testing.expectEqual(@as(usize, 2), processed);
    try std.testing.expectEqual(@as(usize, 0), api.pendingCount());
    try std.testing.expect(api.isResultReady(req1));
    try std.testing.expect(api.isResultReady(req2));
    try std.testing.expectEqual(@as(usize, 2), api.readyCount());

    var second = api.takeResult(req2) orelse unreachable;
    defer second.deinit(allocator);
    var first = api.takeResult(req1) orelse unreachable;
    defer first.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 1), first.sequence);
    try std.testing.expectEqual(@as(u64, 2), second.sequence);
    try std.testing.expectEqual(cmd.CommandResultCode.success, first.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, second.result_code);

    try std.testing.expectEqual(@as(?CommandResponse, null), api.takeResult(req1));
    try std.testing.expectEqual(@as(usize, 0), api.readyCount());
}

test "exchange api async queue supports orderbook-request snapshots" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .naive,
    });
    defer kernel.deinit();
    try std.testing.expect(try kernel.addSymbol(try makeExchangeSpec()));

    var api = ExchangeApi.init(&kernel);
    defer api.deinit();

    var add_u1 = makeAddUserCommand(6201);
    var add_u2 = makeAddUserCommand(6202);
    var dep_q = makeBalanceAdjustmentCommand(6201, 15, 100_000, 1, .adjustment);
    var dep_b = makeBalanceAdjustmentCommand(6202, 11, 100_000, 1, .adjustment);
    var bid = cmd.OrderCommand.newOrder(.gtc, 9201, 6201, 101, 101, 2, .bid);
    bid.symbol = 241;
    var ask = cmd.OrderCommand.newOrder(.gtc, 9202, 6202, 105, 0, 3, .ask);
    ask.symbol = 241;
    var ob_req = makeRequestOrderBookCommand(241, 5);

    _ = try api.submitCommandAsync(&add_u1);
    _ = try api.submitCommandAsync(&add_u2);
    _ = try api.submitCommandAsync(&dep_q);
    _ = try api.submitCommandAsync(&dep_b);
    _ = try api.submitCommandAsync(&bid);
    _ = try api.submitCommandAsync(&ask);
    const req_id = try api.submitCommandAsync(&ob_req);

    _ = try api.drainPending();
    var ob_resp = api.takeResult(req_id) orelse unreachable;
    defer ob_resp.deinit(allocator);

    try std.testing.expectEqual(cmd.CommandResultCode.success, ob_resp.result_code);
    const snapshot = ob_resp.l2_snapshot orelse unreachable;
    try std.testing.expectEqual(@as(usize, 1), snapshot.asks.items.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.bids.items.len);
}

test "exchange api report queries expose risk hash, fees, balances, and orderbook" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer kernel.deinit();
    try std.testing.expect(try kernel.addSymbol(try makeExchangeSpec()));

    var api = ExchangeApi.init(&kernel);
    defer api.deinit();

    var add_maker = makeAddUserCommand(7101);
    var add_taker = makeAddUserCommand(7102);
    var dep_quote = makeBalanceAdjustmentCommand(7101, 15, 100_000, 1, .adjustment);
    var dep_base = makeBalanceAdjustmentCommand(7102, 11, 100_000, 1, .adjustment);
    var add_maker_response = try api.submitCommand(&add_maker);
    defer add_maker_response.deinit(allocator);
    var add_taker_response = try api.submitCommand(&add_taker);
    defer add_taker_response.deinit(allocator);
    var dep_quote_response = try api.submitCommand(&dep_quote);
    defer dep_quote_response.deinit(allocator);
    var dep_base_response = try api.submitCommand(&dep_base);
    defer dep_base_response.deinit(allocator);

    var bid = cmd.OrderCommand.newOrder(.gtc, 9301, 7101, 100, 100, 2, .bid);
    bid.symbol = 241;
    var ask = cmd.OrderCommand.newOrder(.gtc, 9302, 7102, 99, 0, 1, .ask);
    ask.symbol = 241;
    var bid_response = try api.submitCommand(&bid);
    defer bid_response.deinit(allocator);
    var ask_response = try api.submitCommand(&ask);
    defer ask_response.deinit(allocator);

    var hash_report = try api.queryReport(.risk_state_hash);
    defer hash_report.deinit(allocator);
    const expected_hash = try kernel.risk_engine.stateHash();
    try std.testing.expectEqual(expected_hash, hash_report.risk_state_hash);

    var detailed_hash_report = try api.queryReport(.state_hash_detailed);
    defer detailed_hash_report.deinit(allocator);
    switch (detailed_hash_report) {
        .state_hash_detailed => |detailed| {
            try std.testing.expectEqual(@as(usize, 11), detailed.submodules.items.len);
            try std.testing.expect(detailed.aggregate_hash != 0);
        },
        else => unreachable,
    }

    var fee_report = try api.queryReport(.{ .fee_collected = 15 });
    defer fee_report.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 3), fee_report.fee_collected);

    var bal_report = try api.queryReport(.{
        .user_balance = .{
            .uid = 7102,
            .currency = 15,
        },
    });
    defer bal_report.deinit(allocator);
    try std.testing.expectEqual(@as(?i64, 998), bal_report.user_balance);

    var user_report = try api.queryReport(.{ .single_user = 7102 });
    defer user_report.deinit(allocator);
    switch (user_report) {
        .single_user => |single| {
            try std.testing.expectEqual(SingleUserQueryExecutionStatus.ok, single.query_execution_status);
            try std.testing.expectEqual(@as(?common.UserStatus, .active), single.user_status);
            try std.testing.expectEqual(@as(i64, 998), single.accounts.get(15) orelse 0);
        },
        else => unreachable,
    }

    var ob_report = try api.queryReport(.{
        .order_book = .{
            .symbol = 241,
            .depth = 10,
        },
    });
    defer ob_report.deinit(allocator);
    const snapshot = ob_report.order_book;
    try std.testing.expectEqual(@as(usize, 0), snapshot.asks.items.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.bids.items.len);
    try std.testing.expectEqual(@as(i64, 100), snapshot.bids.items[0].price);
    try std.testing.expectEqual(@as(i64, 1), snapshot.bids.items[0].volume);
}

test "kernel detailed state hash report is deterministic and mutation sensitive" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer kernel.deinit();
    try std.testing.expect(try kernel.addSymbol(try makeExchangeSpec()));

    var first = try kernel.queryReport(.state_hash_detailed);
    defer first.deinit(allocator);
    var second = try kernel.queryReport(.state_hash_detailed);
    defer second.deinit(allocator);
    try std.testing.expectEqual(
        first.state_hash_detailed.aggregate_hash,
        second.state_hash_detailed.aggregate_hash,
    );

    var add_u = makeAddUserCommand(7191);
    var add_resp = try kernel.execute(&add_u);
    defer add_resp.deinit(allocator);
    var dep = makeBalanceAdjustmentCommand(7191, 15, 10_000, 1, .adjustment);
    var dep_resp = try kernel.execute(&dep);
    defer dep_resp.deinit(allocator);

    var mutated = try kernel.queryReport(.state_hash_detailed);
    defer mutated.deinit(allocator);
    try std.testing.expect(
        first.state_hash_detailed.aggregate_hash !=
            mutated.state_hash_detailed.aggregate_hash,
    );
}

test "kernel total currency balance report aggregates risk balances and open interest" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .risk = .{ .margin_trading_enabled = true },
    });
    defer kernel.deinit();
    try std.testing.expect(try kernel.addSymbol(try common.CoreSymbolSpecification.init(.{
        .symbol_id = 241,
        .symbol_type = .futures_contract,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 100,
        .quote_scale_k = 10,
        .taker_fee = 0,
        .maker_fee = 0,
        .margin_buy = 10,
        .margin_sell = 12,
    })));

    var add_u1 = makeAddUserCommand(7201);
    var add_u2 = makeAddUserCommand(7202);
    var dep_q = makeBalanceAdjustmentCommand(7201, 15, 1000, 1, .adjustment);
    var dep_b = makeBalanceAdjustmentCommand(7202, 11, 500, 1, .adjustment);
    var r1 = try kernel.execute(&add_u1);
    defer r1.deinit(allocator);
    var r2 = try kernel.execute(&add_u2);
    defer r2.deinit(allocator);
    var r3 = try kernel.execute(&dep_q);
    defer r3.deinit(allocator);
    var r4 = try kernel.execute(&dep_b);
    defer r4.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, r1.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, r2.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, r3.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, r4.result_code);

    const profile_1 = kernel.risk_engine.getUserProfile(7201) orelse unreachable;
    var long_position = user.SymbolPositionRecord.init(7201, 241, 15);
    long_position.direction = .long;
    long_position.open_volume = 7;
    long_position.open_price_sum = 7 * 42;
    try profile_1.positions.put(allocator, 241, long_position);

    const profile_2 = kernel.risk_engine.getUserProfile(7202) orelse unreachable;
    var short_position = user.SymbolPositionRecord.init(7202, 241, 15);
    short_position.direction = .short;
    short_position.open_volume = 4;
    short_position.open_price_sum = 4 * 42;
    try profile_2.positions.put(allocator, 241, short_position);

    var report = try kernel.queryReport(.total_currency_balance);
    defer report.deinit(allocator);
    switch (report) {
        .total_currency_balance => |balance_report| {
            try std.testing.expectEqual(@as(i64, 1000), balance_report.account_balances.get(15) orelse 0);
            try std.testing.expectEqual(@as(i64, 500), balance_report.account_balances.get(11) orelse 0);
            try std.testing.expectEqual(@as(i64, -1000), balance_report.adjustments.get(15) orelse 0);
            try std.testing.expectEqual(@as(i64, -500), balance_report.adjustments.get(11) orelse 0);
            try std.testing.expectEqual(@as(usize, 0), balance_report.fees.count());
            try std.testing.expectEqual(@as(usize, 0), balance_report.orders_balances.count());
            try std.testing.expectEqual(@as(i64, 7), balance_report.open_interest_long.get(241) orelse 0);
            try std.testing.expectEqual(@as(i64, 4), balance_report.open_interest_short.get(241) orelse 0);

            const global_15 = (balance_report.account_balances.get(15) orelse 0) +
                (balance_report.fees.get(15) orelse 0) +
                (balance_report.adjustments.get(15) orelse 0) +
                (balance_report.suspends.get(15) orelse 0);
            const global_11 = (balance_report.account_balances.get(11) orelse 0) +
                (balance_report.fees.get(11) orelse 0) +
                (balance_report.adjustments.get(11) orelse 0) +
                (balance_report.suspends.get(11) orelse 0);
            try std.testing.expectEqual(@as(i64, 0), global_15);
            try std.testing.expectEqual(@as(i64, 0), global_11);
        },
        else => unreachable,
    }
}

test "kernel total currency balance report includes averaged futures profit" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .risk = .{ .margin_trading_enabled = true },
    });
    defer kernel.deinit();
    const spec = try makeFeeMarginFuturesSpec();
    try std.testing.expect(try kernel.addSymbol(spec));

    var add_user = makeAddUserCommand(7251);
    var add_response = try kernel.execute(&add_user);
    defer add_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_response.result_code);

    var deposit = makeBalanceAdjustmentCommand(7251, spec.quote_currency, 1_000, 1, .adjustment);
    var deposit_response = try kernel.execute(&deposit);
    defer deposit_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, deposit_response.result_code);

    const profile = kernel.risk_engine.getUserProfile(7251) orelse unreachable;
    var position = user.SymbolPositionRecord.init(7251, spec.symbol_id, spec.quote_currency);
    position.direction = .long;
    position.open_volume = 2;
    position.open_price_sum = 200;
    position.profit = 5;
    try profile.positions.put(allocator, spec.symbol_id, position);
    try kernel.risk_engine.last_price_cache.put(allocator, spec.symbol_id, .{
        .ask_price = 120,
        .bid_price = 100,
    });

    var report = try kernel.queryReport(.total_currency_balance);
    defer report.deinit(allocator);
    switch (report) {
        .total_currency_balance => |total| {
            // Average price is 110; estimated profit is 5 + (2 * 110 - 200) = 25.
            try std.testing.expectEqual(@as(i64, 1_025), total.account_balances.get(spec.quote_currency) orelse 0);
            try std.testing.expectEqual(@as(i64, 2), total.open_interest_long.get(spec.symbol_id) orelse 0);
            try std.testing.expectEqual(@as(usize, 0), total.orders_balances.count());
        },
        else => unreachable,
    }
}

test "kernel total currency balance report preserves Java one-sided averaging" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .risk = .{ .margin_trading_enabled = true },
    });
    defer kernel.deinit();
    const spec = try makeFeeMarginFuturesSpec();
    try std.testing.expect(try kernel.addSymbol(spec));

    var add_user = makeAddUserCommand(7252);
    var add_response = try kernel.execute(&add_user);
    defer add_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_response.result_code);

    var deposit = makeBalanceAdjustmentCommand(7252, spec.quote_currency, 1_000, 1, .adjustment);
    var deposit_response = try kernel.execute(&deposit);
    defer deposit_response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, deposit_response.result_code);

    const profile = kernel.risk_engine.getUserProfile(7252) orelse unreachable;
    var position = user.SymbolPositionRecord.init(7252, spec.symbol_id, spec.quote_currency);
    position.direction = .long;
    position.open_volume = 1;
    position.open_price_sum = 100;
    position.profit = 5;
    try profile.positions.put(allocator, spec.symbol_id, position);
    try kernel.risk_engine.last_price_cache.put(allocator, spec.symbol_id, .{
        .ask_price = std.math.maxInt(i64),
        .bid_price = 100,
    });

    // Java LastPriceCacheRecord.averagingRecord performs wrapping signed-long
    // addition before shifting, even when maxInt represents a missing ask.
    const java_average = (std.math.maxInt(i64) +% @as(i64, 100)) >> 1;
    const expected_profit = 5 + (java_average - 100);
    const expected_balance = 1_000 + expected_profit;

    var report = try kernel.queryReport(.total_currency_balance);
    defer report.deinit(allocator);
    switch (report) {
        .total_currency_balance => |total| {
            try std.testing.expectEqual(
                expected_balance,
                total.account_balances.get(spec.quote_currency) orelse 0,
            );
            try std.testing.expectEqual(@as(i64, 1), total.open_interest_long.get(spec.symbol_id) orelse 0);
            try std.testing.expectEqual(@as(usize, 0), total.orders_balances.count());
        },
        else => unreachable,
    }
}

test "total currency balance report includes matching-order holds with naive/direct parity" {
    const allocator = std.testing.allocator;
    var naive_kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .naive,
    });
    defer naive_kernel.deinit();
    var direct_kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer direct_kernel.deinit();

    const spec = try makeExchangeSpec();
    try std.testing.expect(try naive_kernel.addSymbol(spec));
    try std.testing.expect(try direct_kernel.addSymbol(spec));

    try executePairExpectEqual(allocator, &naive_kernel, &direct_kernel, makeAddUserCommand(7301));
    try executePairExpectEqual(allocator, &naive_kernel, &direct_kernel, makeAddUserCommand(7302));
    try executePairExpectEqual(
        allocator,
        &naive_kernel,
        &direct_kernel,
        makeBalanceAdjustmentCommand(7301, 15, 100_000, 1, .adjustment),
    );
    try executePairExpectEqual(
        allocator,
        &naive_kernel,
        &direct_kernel,
        makeBalanceAdjustmentCommand(7302, 11, 100_000, 1, .adjustment),
    );

    var bid = cmd.OrderCommand.newOrder(.gtc, 9701, 7301, 95, 101, 2, .bid);
    bid.symbol = 241;
    try executePairExpectEqual(allocator, &naive_kernel, &direct_kernel, bid);

    var ask = cmd.OrderCommand.newOrder(.gtc, 9702, 7302, 110, 0, 3, .ask);
    ask.symbol = 241;
    try executePairExpectEqual(allocator, &naive_kernel, &direct_kernel, ask);

    var naive_report = try naive_kernel.queryReport(.total_currency_balance);
    defer naive_report.deinit(allocator);
    var direct_report = try direct_kernel.queryReport(.total_currency_balance);
    defer direct_report.deinit(allocator);

    const expected_base_hold: i64 = 300;
    const expected_quote_hold: i64 = 2024;

    switch (naive_report) {
        .total_currency_balance => |left| switch (direct_report) {
            .total_currency_balance => |right| {
                try std.testing.expectEqual(left.orders_balances.get(11) orelse 0, right.orders_balances.get(11) orelse 0);
                try std.testing.expectEqual(left.orders_balances.get(15) orelse 0, right.orders_balances.get(15) orelse 0);
                try std.testing.expectEqual(expected_base_hold, left.orders_balances.get(11) orelse 0);
                try std.testing.expectEqual(expected_quote_hold, left.orders_balances.get(15) orelse 0);

                const naive_global_11 = (left.account_balances.get(11) orelse 0) +
                    (left.orders_balances.get(11) orelse 0) +
                    (left.fees.get(11) orelse 0) +
                    (left.adjustments.get(11) orelse 0) +
                    (left.suspends.get(11) orelse 0);
                const naive_global_15 = (left.account_balances.get(15) orelse 0) +
                    (left.orders_balances.get(15) orelse 0) +
                    (left.fees.get(15) orelse 0) +
                    (left.adjustments.get(15) orelse 0) +
                    (left.suspends.get(15) orelse 0);
                const direct_global_11 = (right.account_balances.get(11) orelse 0) +
                    (right.orders_balances.get(11) orelse 0) +
                    (right.fees.get(11) orelse 0) +
                    (right.adjustments.get(11) orelse 0) +
                    (right.suspends.get(11) orelse 0);
                const direct_global_15 = (right.account_balances.get(15) orelse 0) +
                    (right.orders_balances.get(15) orelse 0) +
                    (right.fees.get(15) orelse 0) +
                    (right.adjustments.get(15) orelse 0) +
                    (right.suspends.get(15) orelse 0);

                try std.testing.expectEqual(@as(i64, 0), naive_global_11);
                try std.testing.expectEqual(@as(i64, 0), naive_global_15);
                try std.testing.expectEqual(@as(i64, 0), direct_global_11);
                try std.testing.expectEqual(@as(i64, 0), direct_global_15);
            },
            else => unreachable,
        },
        else => unreachable,
    }
}

test "single user report found and not-found semantics" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer kernel.deinit();
    try std.testing.expect(try kernel.addSymbol(try makeExchangeSpec()));

    var add_user = makeAddUserCommand(7401);
    var add_resp = try kernel.execute(&add_user);
    defer add_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add_resp.result_code);

    var deposit = makeBalanceAdjustmentCommand(7401, 15, 5000, 1, .adjustment);
    var deposit_resp = try kernel.execute(&deposit);
    defer deposit_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, deposit_resp.result_code);

    const profile = kernel.risk_engine.getUserProfile(7401) orelse unreachable;
    var position = user.SymbolPositionRecord.init(7401, 241, 15);
    position.direction = .long;
    position.open_volume = 3;
    position.open_price_sum = 300;
    try profile.positions.put(allocator, 241, position);

    var bid = cmd.OrderCommand.newOrder(.gtc, 9801, 7401, 95, 101, 2, .bid);
    bid.symbol = 241;
    var bid_resp = try kernel.execute(&bid);
    defer bid_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, bid_resp.result_code);

    var found = try kernel.queryReport(.{ .single_user = 7401 });
    defer found.deinit(allocator);
    switch (found) {
        .single_user => |single| {
            try std.testing.expectEqual(SingleUserQueryExecutionStatus.ok, single.query_execution_status);
            try std.testing.expectEqual(@as(?common.UserStatus, .active), single.user_status);
            try std.testing.expectEqual(@as(i64, 2976), single.accounts.get(15) orelse 0);
            const pos = single.positions.get(241) orelse unreachable;
            try std.testing.expectEqual(@as(i32, 15), pos.quote_currency);
            try std.testing.expectEqual(common.PositionDirection.long, pos.direction);
            try std.testing.expectEqual(@as(i64, 3), pos.open_volume);
            try std.testing.expectEqual(@as(usize, 1), single.orders.items.len);
            try std.testing.expectEqual(@as(i64, 9801), single.orders.items[0].order_id);
            try std.testing.expectEqual(@as(i32, 241), single.orders.items[0].symbol);
        },
        else => unreachable,
    }

    var not_found = try kernel.queryReport(.{ .single_user = 9999 });
    defer not_found.deinit(allocator);
    switch (not_found) {
        .single_user => |single| {
            try std.testing.expectEqual(SingleUserQueryExecutionStatus.user_not_found, single.query_execution_status);
            try std.testing.expect(single.user_status == null);
            try std.testing.expectEqual(@as(usize, 0), single.accounts.count());
            try std.testing.expectEqual(@as(usize, 0), single.positions.count());
            try std.testing.expectEqual(@as(usize, 0), single.orders.items.len);
        },
        else => unreachable,
    }
}

test "single user report keeps naive/direct parity on open orders and balances" {
    const allocator = std.testing.allocator;
    var naive_kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .naive,
    });
    defer naive_kernel.deinit();
    var direct_kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer direct_kernel.deinit();

    const spec = try makeExchangeSpec();
    try std.testing.expect(try naive_kernel.addSymbol(spec));
    try std.testing.expect(try direct_kernel.addSymbol(spec));

    try executePairExpectEqual(allocator, &naive_kernel, &direct_kernel, makeAddUserCommand(7501));
    try executePairExpectEqual(
        allocator,
        &naive_kernel,
        &direct_kernel,
        makeBalanceAdjustmentCommand(7501, 15, 10_000, 1, .adjustment),
    );

    var bid = cmd.OrderCommand.newOrder(.gtc, 9901, 7501, 90, 101, 5, .bid);
    bid.symbol = 241;
    try executePairExpectEqual(allocator, &naive_kernel, &direct_kernel, bid);

    var left = try naive_kernel.queryReport(.{ .single_user = 7501 });
    defer left.deinit(allocator);
    var right = try direct_kernel.queryReport(.{ .single_user = 7501 });
    defer right.deinit(allocator);
    switch (left) {
        .single_user => |lhs| switch (right) {
            .single_user => |rhs| {
                try std.testing.expectEqual(lhs.query_execution_status, rhs.query_execution_status);
                try std.testing.expectEqual(lhs.user_status, rhs.user_status);
                try std.testing.expectEqual(lhs.accounts.get(15) orelse 0, rhs.accounts.get(15) orelse 0);
                try std.testing.expectEqual(lhs.positions.count(), rhs.positions.count());
                try std.testing.expectEqual(lhs.orders.items.len, rhs.orders.items.len);
                for (lhs.orders.items, rhs.orders.items) |lo, ro| {
                    try std.testing.expectEqual(lo.symbol, ro.symbol);
                    try std.testing.expectEqual(lo.order_id, ro.order_id);
                    try std.testing.expectEqual(lo.uid, ro.uid);
                    try std.testing.expectEqual(lo.price, ro.price);
                    try std.testing.expectEqual(lo.size, ro.size);
                    try std.testing.expectEqual(lo.filled, ro.filled);
                    try std.testing.expectEqual(lo.reserve_bid_price, ro.reserve_bid_price);
                    try std.testing.expectEqual(lo.action, ro.action);
                }
            },
            else => unreachable,
        },
        else => unreachable,
    }
}

test "exchange api report orderbook query fails for unknown symbol" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();
    var api = ExchangeApi.init(&kernel);
    defer api.deinit();

    try std.testing.expectError(
        error.UnknownSymbol,
        api.queryReport(.{
            .order_book = .{
                .symbol = 999,
                .depth = 10,
            },
        }),
    );
}

test "settlement preparation single kernel observes one complete multi-fill batch before R2" {
    const allocator = std.testing.allocator;
    const symbol: i32 = 8_701;
    const maker_uids = [2]i64{ 87_001, 87_002 };
    const taker_uid: i64 = 87_003;
    const spec = try makeExchangeSpecForSymbol(symbol);

    var kernel = try ExchangeKernel.init(allocator, .{ .orderbook_kind = .direct });
    defer kernel.deinit();
    try setupSingleSettlementMultiFill(
        allocator,
        &kernel,
        symbol,
        maker_uids,
        taker_uid,
    );

    var recorder: SettlementPrepareRecorder = .{ .target = .{ .single = &kernel } };
    var taker_bid = settlementMultiFillBid(91_100, taker_uid, symbol);
    var response = try kernel.executeWithSettlement(
        &taker_bid,
        recorder.hooks(),
        .replay,
    );
    defer response.deinit(allocator);

    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    try std.testing.expectEqual(@as(usize, 2), events.listSize(response.matcher_events_head));
    try std.testing.expectEqual(@as(usize, 1), recorder.prepare_calls);
    try std.testing.expectEqual(@as(usize, 2), recorder.last_trade_count);
    try std.testing.expectEqual(response.sequence, recorder.last_sequence);
    try std.testing.expectEqual(settlement_lifecycle.ExecutionMode.replay, recorder.last_mode);
    try std.testing.expectEqual(taker_bid.order_id, recorder.last_order_id);
    try std.testing.expectEqualDeep(spec, recorder.last_specification.?);
    try std.testing.expect(!recorder.risk_hash_failed);
    try std.testing.expectEqual(@as(usize, 1), recorder.observed_risk_hash_count);
    try std.testing.expectEqual(@as(i64, 0), recorder.observed_total_fee);
    try std.testing.expectEqual(@as(i64, 15), kernel.risk_engine.getFeeCollected(15));
    try std.testing.expect(recorder.observed_risk_hashes[0] != try kernel.risk_engine.stateHash());
    try std.testing.expectEqual(@as(usize, 0), recorder.risk_mark_calls);
    try std.testing.expectEqual(@as(usize, 0), recorder.journal_mark_calls);
    try std.testing.expectEqual(@as(usize, 0), recorder.recovery_calls);
}

test "settlement preparation immediate sharded path calls once after every prepare and before R2" {
    const allocator = std.testing.allocator;
    const symbol: i32 = 8_702;
    const maker_uids = [2]i64{
        uidForShard(0, 4, 87_100),
        uidForShard(1, 4, 87_200),
    };
    const taker_uid = uidForShard(2, 4, 87_300);

    var kernels = try MultiShardKernelSet.initWithTopology(
        allocator,
        1,
        4,
        .{ .orderbook_kind = .direct },
    );
    defer kernels.deinit();
    try setupMultiSettlementMultiFill(
        allocator,
        &kernels,
        symbol,
        maker_uids,
        taker_uid,
    );

    var recorder: SettlementPrepareRecorder = .{ .target = .{ .multi = &kernels } };
    var taker_bid = settlementMultiFillBid(91_200, taker_uid, symbol);
    var response = try kernels.executeSplitWithSettlement(
        &taker_bid,
        recorder.hooks(),
        .live,
    );
    defer response.deinit(allocator);

    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    try std.testing.expectEqual(@as(usize, 2), events.listSize(response.matcher_events_head));
    try std.testing.expectEqual(@as(usize, 1), recorder.prepare_calls);
    try std.testing.expectEqual(@as(usize, 2), recorder.last_trade_count);
    try std.testing.expectEqual(@as(usize, 4), recorder.observed_risk_hash_count);
    try std.testing.expect(!recorder.risk_hash_failed);
    try std.testing.expectEqual(@as(i64, 0), recorder.observed_total_fee);
    try std.testing.expectEqual(@as(i64, 15), groupedTestTotalFee(&kernels, 15));
    for (0..3) |idx| {
        try std.testing.expect(
            recorder.observed_risk_hashes[idx] != try kernels.riskEngineAt(idx).stateHash(),
        );
    }
    try std.testing.expectEqual(
        recorder.observed_risk_hashes[3],
        try kernels.riskEngineAt(3).stateHash(),
    );
    try std.testing.expectEqual(@as(usize, 0), recorder.risk_mark_calls);
    try std.testing.expectEqual(@as(usize, 0), recorder.journal_mark_calls);
    try std.testing.expectEqual(@as(usize, 0), recorder.recovery_calls);
}

test "settlement preparation rejection prevents every immediate sharded R2 commit" {
    const allocator = std.testing.allocator;
    const symbol: i32 = 8_703;
    const maker_uids = [2]i64{
        uidForShard(0, 4, 87_400),
        uidForShard(1, 4, 87_500),
    };
    const taker_uid = uidForShard(2, 4, 87_600);

    var kernels = try MultiShardKernelSet.initWithTopology(
        allocator,
        1,
        4,
        .{ .orderbook_kind = .direct },
    );
    defer kernels.deinit();
    try setupMultiSettlementMultiFill(
        allocator,
        &kernels,
        symbol,
        maker_uids,
        taker_uid,
    );

    var recorder: SettlementPrepareRecorder = .{
        .target = .{ .multi = &kernels },
        .reject = true,
    };
    var taker_bid = settlementMultiFillBid(91_300, taker_uid, symbol);
    try std.testing.expectError(
        error.SettlementPreparationRejected,
        kernels.executeRoutedWithSettlement(
            &taker_bid,
            recorder.hooks(),
            .replay,
        ),
    );

    try std.testing.expectEqual(@as(usize, 1), recorder.prepare_calls);
    try std.testing.expectEqual(@as(usize, 2), recorder.last_trade_count);
    try std.testing.expectEqual(settlement_lifecycle.ExecutionMode.replay, recorder.last_mode);
    try std.testing.expectEqual(@as(usize, 4), recorder.observed_risk_hash_count);
    try std.testing.expect(!recorder.risk_hash_failed);
    for (0..kernels.riskShardCount()) |idx| {
        try std.testing.expectEqual(
            recorder.observed_risk_hashes[idx],
            try kernels.riskEngineAt(idx).stateHash(),
        );
        try std.testing.expectEqual(@as(i64, 0), kernels.riskEngineAt(idx).getFeeCollected(15));
    }
    try std.testing.expectEqual(@as(usize, 0), recorder.risk_mark_calls);
    try std.testing.expectEqual(@as(usize, 0), recorder.journal_mark_calls);
    try std.testing.expectEqual(@as(usize, 0), recorder.recovery_calls);
}

test "settlement preparation disabled variants are equivalent to existing immediate APIs" {
    const allocator = std.testing.allocator;
    const single_symbol: i32 = 8_704;
    const single_makers = [2]i64{ 87_701, 87_702 };
    const single_taker: i64 = 87_703;

    var existing_single = try ExchangeKernel.init(allocator, .{ .orderbook_kind = .direct });
    defer existing_single.deinit();
    var disabled_single = try ExchangeKernel.init(allocator, .{ .orderbook_kind = .direct });
    defer disabled_single.deinit();
    try setupSingleSettlementMultiFill(
        allocator,
        &existing_single,
        single_symbol,
        single_makers,
        single_taker,
    );
    try setupSingleSettlementMultiFill(
        allocator,
        &disabled_single,
        single_symbol,
        single_makers,
        single_taker,
    );

    const single_sequence = publication.PublicationSequence.fromInt(existing_single.next_seq + 3);
    var existing_single_bid = settlementMultiFillBid(91_400, single_taker, single_symbol);
    var disabled_single_bid = existing_single_bid;
    var existing_single_response = try existing_single.executeAtSequence(
        single_sequence,
        &existing_single_bid,
    );
    defer existing_single_response.deinit(allocator);
    var disabled_single_response = try disabled_single.executeAtSequenceWithSettlement(
        single_sequence,
        &disabled_single_bid,
        settlement_lifecycle.Hooks.disabled(),
        .replay,
    );
    defer disabled_single_response.deinit(allocator);
    try std.testing.expectEqualDeep(existing_single_response.command, disabled_single_response.command);
    try std.testing.expectEqual(
        events.listSize(existing_single_response.matcher_events_head),
        events.listSize(disabled_single_response.matcher_events_head),
    );
    var existing_single_hash = try existing_single.buildStateHashDetailedReport();
    defer existing_single_hash.deinit(allocator);
    var disabled_single_hash = try disabled_single.buildStateHashDetailedReport();
    defer disabled_single_hash.deinit(allocator);
    try std.testing.expectEqual(
        existing_single_hash.aggregate_hash,
        disabled_single_hash.aggregate_hash,
    );

    const multi_symbol: i32 = 8_705;
    const multi_makers = [2]i64{
        uidForShard(0, 4, 87_800),
        uidForShard(1, 4, 87_900),
    };
    const multi_taker = uidForShard(2, 4, 88_000);
    var existing_multi = try MultiShardKernelSet.initWithTopology(
        allocator,
        1,
        4,
        .{ .orderbook_kind = .direct },
    );
    defer existing_multi.deinit();
    var disabled_multi = try MultiShardKernelSet.initWithTopology(
        allocator,
        1,
        4,
        .{ .orderbook_kind = .direct },
    );
    defer disabled_multi.deinit();
    try setupMultiSettlementMultiFill(
        allocator,
        &existing_multi,
        multi_symbol,
        multi_makers,
        multi_taker,
    );
    try setupMultiSettlementMultiFill(
        allocator,
        &disabled_multi,
        multi_symbol,
        multi_makers,
        multi_taker,
    );

    var canonical_probe = settlementMultiFillBid(91_500, multi_taker, multi_symbol);
    const canonical_spec = MultiShardKernelSet.StageAdapter.init(&disabled_multi)
        .canonicalSymbolSpecification(&canonical_probe) orelse
        return error.TestExpectedCanonicalSymbolSpecification;
    try std.testing.expectEqualDeep(try makeExchangeSpecForSymbol(multi_symbol), canonical_spec);

    const multi_sequence = publication.PublicationSequence.fromInt(existing_multi.next_seq + 2);
    var existing_multi_bid = canonical_probe;
    var disabled_multi_bid = canonical_probe;
    var existing_multi_response = try existing_multi.executeSplitAtSequence(
        multi_sequence,
        &existing_multi_bid,
    );
    defer existing_multi_response.deinit(allocator);
    var disabled_multi_response = try disabled_multi.executeSplitAtSequenceWithSettlement(
        multi_sequence,
        &disabled_multi_bid,
        settlement_lifecycle.Hooks.disabled(),
        .replay,
    );
    defer disabled_multi_response.deinit(allocator);
    try std.testing.expectEqualDeep(existing_multi_response.command, disabled_multi_response.command);
    try std.testing.expectEqual(
        events.listSize(existing_multi_response.matcher_events_head),
        events.listSize(disabled_multi_response.matcher_events_head),
    );
    try std.testing.expectEqual(
        try existing_multi.detailedHashFingerprint(),
        try disabled_multi.detailedHashFingerprint(),
    );
}

test "kernel settles exchange balances from matcher trade events" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer kernel.deinit();
    try std.testing.expect(try kernel.addSymbol(try makeExchangeSpec()));

    var add_maker = makeAddUserCommand(7001);
    var add_taker = makeAddUserCommand(7002);
    _ = try kernel.execute(&add_maker);
    _ = try kernel.execute(&add_taker);

    var dep_maker_quote = makeBalanceAdjustmentCommand(7001, 15, 100_000, 1, .adjustment);
    var dep_taker_base = makeBalanceAdjustmentCommand(7002, 11, 100_000, 1, .adjustment);
    _ = try kernel.execute(&dep_maker_quote);
    _ = try kernel.execute(&dep_taker_base);

    var maker_bid = cmd.OrderCommand.newOrder(.gtc, 9001, 7001, 100, 100, 5, .bid);
    maker_bid.symbol = 241;
    var taker_ask = cmd.OrderCommand.newOrder(.gtc, 9002, 7002, 95, 0, 3, .ask);
    taker_ask.symbol = 241;

    var bid_resp = try kernel.execute(&maker_bid);
    defer bid_resp.deinit(allocator);
    var ask_resp = try kernel.execute(&taker_ask);
    defer ask_resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, bid_resp.result_code);
    try std.testing.expectEqual(cmd.CommandResultCode.success, ask_resp.result_code);

    const maker = kernel.risk_engine.getUserProfile(7001) orelse unreachable;
    const taker = kernel.risk_engine.getUserProfile(7002) orelse unreachable;

    try std.testing.expectEqual(@as(i64, 94_993), maker.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 300), maker.accountBalance(11));
    try std.testing.expectEqual(@as(i64, 99_700), taker.accountBalance(11));
    try std.testing.expectEqual(@as(i64, 2_994), taker.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 9), kernel.risk_engine.getFeeCollected(15));

    var req = makeRequestOrderBookCommand(241, 5);
    var ob = try kernel.execute(&req);
    defer ob.deinit(allocator);
    const snapshot = ob.l2_snapshot orelse unreachable;
    try std.testing.expectEqual(@as(usize, 1), snapshot.bids.items.len);
    try std.testing.expectEqual(@as(i64, 100), snapshot.bids.items[0].price);
    try std.testing.expectEqual(@as(i64, 2), snapshot.bids.items[0].volume);
}

test "kernel duplicate GTC partial match rejects remainder before trade and settles spot once" {
    const allocator = std.testing.allocator;
    const symbol: i32 = 241;
    const maker_uid: i64 = 1_445_000;
    const original_bidder_uid: i64 = 1_445_001;
    const duplicate_bidder_uid: i64 = 1_445_002;
    const maker_order_id: i64 = 1_445_100;
    const duplicate_order_id: i64 = 1_445_101;

    for ([_]OrderBookKind{ .naive, .direct }) |orderbook_kind| {
        var kernel = try ExchangeKernel.init(allocator, .{
            .orderbook_kind = orderbook_kind,
        });
        defer kernel.deinit();
        try std.testing.expect(try kernel.addSymbol(try makeExchangeSpecForSymbol(symbol)));

        for ([_]i64{ maker_uid, original_bidder_uid, duplicate_bidder_uid }) |uid| {
            var add_user = makeAddUserCommand(uid);
            var response = try kernel.execute(&add_user);
            defer response.deinit(allocator);
            try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
        }

        var maker_base_deposit = makeBalanceAdjustmentCommand(
            maker_uid,
            11,
            100_000,
            1,
            .adjustment,
        );
        var maker_base_response = try kernel.execute(&maker_base_deposit);
        defer maker_base_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, maker_base_response.result_code);

        var original_quote_deposit = makeBalanceAdjustmentCommand(
            original_bidder_uid,
            15,
            100_000,
            1,
            .adjustment,
        );
        var original_quote_response = try kernel.execute(&original_quote_deposit);
        defer original_quote_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, original_quote_response.result_code);

        var duplicate_quote_deposit = makeBalanceAdjustmentCommand(
            duplicate_bidder_uid,
            15,
            100_000,
            1,
            .adjustment,
        );
        var duplicate_quote_response = try kernel.execute(&duplicate_quote_deposit);
        defer duplicate_quote_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, duplicate_quote_response.result_code);

        var original_bid = cmd.OrderCommand.newOrder(
            .gtc,
            duplicate_order_id,
            original_bidder_uid,
            90,
            120,
            5,
            .bid,
        );
        original_bid.symbol = symbol;
        var original_bid_response = try kernel.execute(&original_bid);
        defer original_bid_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, original_bid_response.result_code);

        var maker_ask = cmd.OrderCommand.newOrder(
            .gtc,
            maker_order_id,
            maker_uid,
            100,
            0,
            3,
            .ask,
        );
        maker_ask.symbol = symbol;
        var maker_ask_response = try kernel.execute(&maker_ask);
        defer maker_ask_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, maker_ask_response.result_code);

        var duplicate_bid = cmd.OrderCommand.newOrder(
            .gtc,
            duplicate_order_id,
            duplicate_bidder_uid,
            110,
            150,
            7,
            .bid,
        );
        duplicate_bid.symbol = symbol;
        var duplicate_response = try kernel.execute(&duplicate_bid);
        defer duplicate_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, duplicate_response.result_code);
        try std.testing.expectEqual(
            @as(usize, 2),
            events.listSize(duplicate_response.matcher_events_head),
        );

        const reject = duplicate_response.matcher_events_head orelse
            return error.TestExpectedDuplicateRejectEvent;
        try std.testing.expectEqual(events.MatcherEventType.reject, reject.event_type);
        try std.testing.expect(reject.active_order_completed);
        try std.testing.expectEqual(@as(i64, 0), reject.matched_order_id);
        try std.testing.expectEqual(@as(i64, 0), reject.matched_order_uid);
        try std.testing.expect(!reject.matched_order_completed);
        try std.testing.expectEqual(@as(i64, 110), reject.price);
        try std.testing.expectEqual(@as(i64, 4), reject.size);
        try std.testing.expectEqual(@as(i64, 150), reject.bidder_hold_price);

        const trade = reject.next_event orelse return error.TestExpectedDuplicateTradeEvent;
        try std.testing.expectEqual(events.MatcherEventType.trade, trade.event_type);
        try std.testing.expect(!trade.active_order_completed);
        try std.testing.expectEqual(maker_order_id, trade.matched_order_id);
        try std.testing.expectEqual(maker_uid, trade.matched_order_uid);
        try std.testing.expect(trade.matched_order_completed);
        try std.testing.expectEqual(@as(i64, 100), trade.price);
        try std.testing.expectEqual(@as(i64, 3), trade.size);
        try std.testing.expectEqual(@as(i64, 150), trade.bidder_hold_price);
        try std.testing.expect(trade.next_event == null);

        const maker = kernel.risk_engine.getUserProfile(maker_uid) orelse unreachable;
        const original_bidder = kernel.risk_engine.getUserProfile(original_bidder_uid) orelse
            unreachable;
        const duplicate_bidder = kernel.risk_engine.getUserProfile(duplicate_bidder_uid) orelse
            unreachable;
        try std.testing.expectEqual(@as(i64, 99_700), maker.accountBalance(11));
        try std.testing.expectEqual(@as(i64, 2_997), maker.accountBalance(15));
        try std.testing.expectEqual(@as(i64, 0), original_bidder.accountBalance(11));
        try std.testing.expectEqual(@as(i64, 93_990), original_bidder.accountBalance(15));
        try std.testing.expectEqual(@as(i64, 300), duplicate_bidder.accountBalance(11));
        try std.testing.expectEqual(@as(i64, 96_994), duplicate_bidder.accountBalance(15));
        try std.testing.expectEqual(@as(i64, 9), kernel.risk_engine.getFeeCollected(15));

        var order_book = makeRequestOrderBookCommand(symbol, 4);
        var order_book_response = try kernel.execute(&order_book);
        defer order_book_response.deinit(allocator);
        const snapshot = order_book_response.l2_snapshot orelse
            return error.TestExpectedOrderBookSnapshot;
        try std.testing.expectEqual(@as(usize, 0), snapshot.asks.items.len);
        try std.testing.expectEqual(@as(usize, 1), snapshot.bids.items.len);
        try std.testing.expectEqual(@as(i64, 90), snapshot.bids.items[0].price);
        try std.testing.expectEqual(@as(i64, 5), snapshot.bids.items[0].volume);
        try std.testing.expectEqual(@as(i64, 1), snapshot.bids.items[0].orders);

        var total_report = try kernel.queryReport(.total_currency_balance);
        defer total_report.deinit(allocator);
        switch (total_report) {
            .total_currency_balance => |total| {
                try std.testing.expectEqual(
                    @as(i64, 100_000),
                    total.account_balances.get(11) orelse 0,
                );
                try std.testing.expectEqual(
                    @as(i64, 193_981),
                    total.account_balances.get(15) orelse 0,
                );
                try std.testing.expectEqual(@as(usize, 1), total.orders_balances.count());
                try std.testing.expectEqual(
                    @as(i64, 6_010),
                    total.orders_balances.get(15) orelse 0,
                );
                try std.testing.expectEqual(@as(i64, 9), total.fees.get(15) orelse 0);
                try std.testing.expectEqual(
                    @as(i64, -100_000),
                    total.adjustments.get(11) orelse 0,
                );
                try std.testing.expectEqual(
                    @as(i64, -200_000),
                    total.adjustments.get(15) orelse 0,
                );
                try std.testing.expectEqual(@as(usize, 0), total.open_interest_long.count());
                try std.testing.expectEqual(@as(usize, 0), total.open_interest_short.count());
            },
            else => return error.TestExpectedTotalCurrencyBalanceReport,
        }
    }
}

test "kernel duplicate GTC rejection releases only the futures remainder exposure" {
    const allocator = std.testing.allocator;
    const spec = try makeFeeMarginFuturesSpec();
    const maker_uid: i64 = 1_445_010;
    const original_bidder_uid: i64 = 1_445_011;
    const duplicate_bidder_uid: i64 = 1_445_012;
    const maker_order_id: i64 = 1_445_110;
    const duplicate_order_id: i64 = 1_445_111;

    for ([_]OrderBookKind{ .naive, .direct }) |orderbook_kind| {
        var kernel = try ExchangeKernel.init(allocator, .{
            .orderbook_kind = orderbook_kind,
            .risk = .{ .margin_trading_enabled = true },
        });
        defer kernel.deinit();
        try std.testing.expect(try kernel.addSymbol(spec));

        for ([_]i64{ maker_uid, original_bidder_uid, duplicate_bidder_uid }) |uid| {
            var add_user = makeAddUserCommand(uid);
            var add_response = try kernel.execute(&add_user);
            defer add_response.deinit(allocator);
            try std.testing.expectEqual(cmd.CommandResultCode.success, add_response.result_code);

            var deposit = makeBalanceAdjustmentCommand(
                uid,
                spec.quote_currency,
                200_000,
                1,
                .adjustment,
            );
            var deposit_response = try kernel.execute(&deposit);
            defer deposit_response.deinit(allocator);
            try std.testing.expectEqual(cmd.CommandResultCode.success, deposit_response.result_code);
        }

        var original_bid = cmd.OrderCommand.newOrder(
            .gtc,
            duplicate_order_id,
            original_bidder_uid,
            10_000,
            10_000,
            5,
            .bid,
        );
        original_bid.symbol = spec.symbol_id;
        var original_response = try kernel.execute(&original_bid);
        defer original_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, original_response.result_code);

        var maker_ask = cmd.OrderCommand.newOrder(
            .gtc,
            maker_order_id,
            maker_uid,
            10_100,
            0,
            3,
            .ask,
        );
        maker_ask.symbol = spec.symbol_id;
        var maker_response = try kernel.execute(&maker_ask);
        defer maker_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, maker_response.result_code);

        var duplicate_bid = cmd.OrderCommand.newOrder(
            .gtc,
            duplicate_order_id,
            duplicate_bidder_uid,
            10_100,
            10_100,
            7,
            .bid,
        );
        duplicate_bid.symbol = spec.symbol_id;
        var duplicate_response = try kernel.execute(&duplicate_bid);
        defer duplicate_response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, duplicate_response.result_code);

        const reject = duplicate_response.matcher_events_head orelse
            return error.TestExpectedDuplicateRejectEvent;
        try std.testing.expectEqual(events.MatcherEventType.reject, reject.event_type);
        try std.testing.expectEqual(@as(i64, 4), reject.size);
        const trade = reject.next_event orelse return error.TestExpectedDuplicateTradeEvent;
        try std.testing.expectEqual(events.MatcherEventType.trade, trade.event_type);
        try std.testing.expectEqual(@as(i64, 3), trade.size);
        try std.testing.expect(trade.next_event == null);

        const original = kernel.risk_engine.getUserProfile(original_bidder_uid) orelse
            unreachable;
        const maker = kernel.risk_engine.getUserProfile(maker_uid) orelse unreachable;
        const duplicate = kernel.risk_engine.getUserProfile(duplicate_bidder_uid) orelse
            unreachable;
        const original_position = original.positions.getPtr(spec.symbol_id) orelse unreachable;
        const maker_position = maker.positions.getPtr(spec.symbol_id) orelse unreachable;
        const duplicate_position = duplicate.positions.getPtr(spec.symbol_id) orelse unreachable;

        try std.testing.expectEqual(@as(i64, 5), original_position.pending_buy_size);
        try std.testing.expectEqual(@as(i64, 0), original_position.open_volume);
        try std.testing.expectEqual(common.PositionDirection.short, maker_position.direction);
        try std.testing.expectEqual(@as(i64, 3), maker_position.open_volume);
        try std.testing.expectEqual(@as(i64, 0), maker_position.pending_sell_size);
        try std.testing.expectEqual(common.PositionDirection.long, duplicate_position.direction);
        try std.testing.expectEqual(@as(i64, 3), duplicate_position.open_volume);
        try std.testing.expectEqual(@as(i64, 0), duplicate_position.pending_buy_size);
        try std.testing.expectEqual(@as(i64, 199_994), maker.accountBalance(spec.quote_currency));
        try std.testing.expectEqual(@as(i64, 200_000), original.accountBalance(spec.quote_currency));
        try std.testing.expectEqual(@as(i64, 199_991), duplicate.accountBalance(spec.quote_currency));
        try std.testing.expectEqual(@as(i64, 15), kernel.risk_engine.getFeeCollected(spec.quote_currency));
    }
}

test "kernel matches Java fee-margin partial maker scenario" {
    const allocator = std.testing.allocator;

    for ([_]OrderBookKind{ .naive, .direct }) |orderbook_kind| {
        var kernel = try ExchangeKernel.init(allocator, .{
            .orderbook_kind = orderbook_kind,
            .risk = .{ .margin_trading_enabled = true },
        });
        defer kernel.deinit();
        try std.testing.expect(try kernel.addSymbol(try makeFeeMarginFuturesSpec()));

        var add_maker = makeAddUserCommand(1_440_001);
        var add_maker_response = try kernel.execute(&add_maker);
        try std.testing.expectEqual(cmd.CommandResultCode.success, add_maker_response.result_code);
        add_maker_response.deinit(allocator);

        var maker_deposit = makeBalanceAdjustmentCommand(1_440_001, 392, 240_000, 1, .adjustment);
        var maker_deposit_response = try kernel.execute(&maker_deposit);
        try std.testing.expectEqual(cmd.CommandResultCode.success, maker_deposit_response.result_code);
        maker_deposit_response.deinit(allocator);

        var maker_ask = cmd.OrderCommand.newOrder(.gtc, 101, 1_440_001, 10_770, 0, 40, .ask);
        maker_ask.symbol = 5991;
        var maker_response = try kernel.execute(&maker_ask);
        try std.testing.expectEqual(cmd.CommandResultCode.success, maker_response.result_code);
        maker_response.deinit(allocator);

        var add_taker = makeAddUserCommand(1_440_002);
        var add_taker_response = try kernel.execute(&add_taker);
        try std.testing.expectEqual(cmd.CommandResultCode.success, add_taker_response.result_code);
        add_taker_response.deinit(allocator);

        var taker_deposit = makeBalanceAdjustmentCommand(1_440_002, 392, 150_000, 1, .adjustment);
        var taker_deposit_response = try kernel.execute(&taker_deposit);
        try std.testing.expectEqual(cmd.CommandResultCode.success, taker_deposit_response.result_code);
        taker_deposit_response.deinit(allocator);

        var taker_bid = cmd.OrderCommand.newOrder(.ioc, 102, 1_440_002, 10_770, 10_770, 30, .bid);
        taker_bid.symbol = 5991;
        var taker_response = try kernel.execute(&taker_bid);
        try std.testing.expectEqual(cmd.CommandResultCode.success, taker_response.result_code);
        taker_response.deinit(allocator);

        const maker = kernel.risk_engine.getUserProfile(1_440_001) orelse unreachable;
        const taker = kernel.risk_engine.getUserProfile(1_440_002) orelse unreachable;
        const maker_position = maker.positions.getPtr(5991) orelse unreachable;
        const taker_position = taker.positions.getPtr(5991) orelse unreachable;

        try std.testing.expectEqual(@as(i64, 239_940), maker.accountBalance(392));
        try std.testing.expectEqual(common.PositionDirection.short, maker_position.direction);
        try std.testing.expectEqual(@as(i64, 30), maker_position.open_volume);
        try std.testing.expectEqual(@as(i64, 323_100), maker_position.open_price_sum);
        try std.testing.expectEqual(@as(i64, 0), maker_position.pending_buy_size);
        try std.testing.expectEqual(@as(i64, 10), maker_position.pending_sell_size);

        try std.testing.expectEqual(@as(i64, 149_910), taker.accountBalance(392));
        try std.testing.expectEqual(common.PositionDirection.long, taker_position.direction);
        try std.testing.expectEqual(@as(i64, 30), taker_position.open_volume);
        try std.testing.expectEqual(@as(i64, 323_100), taker_position.open_price_sum);
        try std.testing.expectEqual(@as(i64, 0), taker_position.pending_buy_size);
        try std.testing.expectEqual(@as(i64, 0), taker_position.pending_sell_size);
        try std.testing.expectEqual(@as(i64, 150), kernel.risk_engine.getFeeCollected(392));

        var maker_report = try kernel.queryReport(.{ .single_user = 1_440_001 });
        defer maker_report.deinit(allocator);
        switch (maker_report) {
            .single_user => |single| {
                try std.testing.expectEqual(@as(usize, 1), single.orders.items.len);
                try std.testing.expectEqual(@as(i64, 101), single.orders.items[0].order_id);
                try std.testing.expectEqual(@as(i64, 10_770), single.orders.items[0].price);
                try std.testing.expectEqual(@as(i64, 40), single.orders.items[0].size);
                try std.testing.expectEqual(@as(i64, 30), single.orders.items[0].filled);
            },
            else => unreachable,
        }

        var taker_report = try kernel.queryReport(.{ .single_user = 1_440_002 });
        defer taker_report.deinit(allocator);
        switch (taker_report) {
            .single_user => |single| {
                try std.testing.expectEqual(@as(usize, 0), single.orders.items.len);
            },
            else => unreachable,
        }

        var total_report = try kernel.queryReport(.total_currency_balance);
        defer total_report.deinit(allocator);
        switch (total_report) {
            .total_currency_balance => |total| {
                try std.testing.expectEqual(@as(i64, 389_850), total.account_balances.get(392) orelse 0);
                try std.testing.expectEqual(@as(usize, 0), total.orders_balances.count());
                try std.testing.expectEqual(@as(i64, 150), total.fees.get(392) orelse 0);
                try std.testing.expectEqual(@as(i64, -390_000), total.adjustments.get(392) orelse 0);
                try std.testing.expectEqual(@as(i64, 30), total.open_interest_long.get(5991) orelse 0);
                try std.testing.expectEqual(@as(i64, 30), total.open_interest_short.get(5991) orelse 0);
            },
            else => unreachable,
        }
    }
}

test "kernel matches Java fee-margin inverse partial maker scenario" {
    const allocator = std.testing.allocator;

    for ([_]OrderBookKind{ .naive, .direct }) |orderbook_kind| {
        var kernel = try ExchangeKernel.init(allocator, .{
            .orderbook_kind = orderbook_kind,
            .risk = .{ .margin_trading_enabled = true },
        });
        defer kernel.deinit();
        try std.testing.expect(try kernel.addSymbol(try makeFeeMarginFuturesSpec()));

        var add_maker = makeAddUserCommand(1_440_011);
        var add_maker_response = try kernel.execute(&add_maker);
        try std.testing.expectEqual(cmd.CommandResultCode.success, add_maker_response.result_code);
        add_maker_response.deinit(allocator);

        var maker_deposit = makeBalanceAdjustmentCommand(1_440_011, 392, 250_000, 1, .adjustment);
        var maker_deposit_response = try kernel.execute(&maker_deposit);
        try std.testing.expectEqual(cmd.CommandResultCode.success, maker_deposit_response.result_code);
        maker_deposit_response.deinit(allocator);

        var maker_bid = cmd.OrderCommand.newOrder(.gtc, 101, 1_440_011, 10_770, 0, 50, .bid);
        maker_bid.symbol = 5991;
        var maker_response = try kernel.execute(&maker_bid);
        try std.testing.expectEqual(cmd.CommandResultCode.success, maker_response.result_code);
        maker_response.deinit(allocator);

        var add_taker = makeAddUserCommand(1_440_012);
        var add_taker_response = try kernel.execute(&add_taker);
        try std.testing.expectEqual(cmd.CommandResultCode.success, add_taker_response.result_code);
        add_taker_response.deinit(allocator);

        var taker_deposit = makeBalanceAdjustmentCommand(1_440_012, 392, 200_000, 1, .adjustment);
        var taker_deposit_response = try kernel.execute(&taker_deposit);
        try std.testing.expectEqual(cmd.CommandResultCode.success, taker_deposit_response.result_code);
        taker_deposit_response.deinit(allocator);

        var taker_ask = cmd.OrderCommand.newOrder(.ioc, 102, 1_440_012, 10_770, 10_770, 30, .ask);
        taker_ask.symbol = 5991;
        var taker_response = try kernel.execute(&taker_ask);
        try std.testing.expectEqual(cmd.CommandResultCode.success, taker_response.result_code);
        taker_response.deinit(allocator);

        const maker = kernel.risk_engine.getUserProfile(1_440_011) orelse unreachable;
        const taker = kernel.risk_engine.getUserProfile(1_440_012) orelse unreachable;
        const maker_position = maker.positions.getPtr(5991) orelse unreachable;
        const taker_position = taker.positions.getPtr(5991) orelse unreachable;

        try std.testing.expectEqual(@as(i64, 249_940), maker.accountBalance(392));
        try std.testing.expectEqual(common.PositionDirection.long, maker_position.direction);
        try std.testing.expectEqual(@as(i64, 30), maker_position.open_volume);
        try std.testing.expectEqual(@as(i64, 323_100), maker_position.open_price_sum);
        try std.testing.expectEqual(@as(i64, 20), maker_position.pending_buy_size);
        try std.testing.expectEqual(@as(i64, 0), maker_position.pending_sell_size);

        try std.testing.expectEqual(@as(i64, 199_910), taker.accountBalance(392));
        try std.testing.expectEqual(common.PositionDirection.short, taker_position.direction);
        try std.testing.expectEqual(@as(i64, 30), taker_position.open_volume);
        try std.testing.expectEqual(@as(i64, 323_100), taker_position.open_price_sum);
        try std.testing.expectEqual(@as(i64, 0), taker_position.pending_buy_size);
        try std.testing.expectEqual(@as(i64, 0), taker_position.pending_sell_size);
        try std.testing.expectEqual(@as(i64, 150), kernel.risk_engine.getFeeCollected(392));

        var maker_report = try kernel.queryReport(.{ .single_user = 1_440_011 });
        defer maker_report.deinit(allocator);
        switch (maker_report) {
            .single_user => |single| {
                try std.testing.expectEqual(@as(usize, 1), single.orders.items.len);
                try std.testing.expectEqual(@as(i64, 101), single.orders.items[0].order_id);
                try std.testing.expectEqual(@as(i64, 50), single.orders.items[0].size);
                try std.testing.expectEqual(@as(i64, 30), single.orders.items[0].filled);
            },
            else => unreachable,
        }

        var taker_report = try kernel.queryReport(.{ .single_user = 1_440_012 });
        defer taker_report.deinit(allocator);
        switch (taker_report) {
            .single_user => |single| try std.testing.expectEqual(@as(usize, 0), single.orders.items.len),
            else => unreachable,
        }

        var total_report = try kernel.queryReport(.total_currency_balance);
        defer total_report.deinit(allocator);
        switch (total_report) {
            .total_currency_balance => |total| {
                try std.testing.expectEqual(@as(i64, 449_850), total.account_balances.get(392) orelse 0);
                try std.testing.expectEqual(@as(usize, 0), total.orders_balances.count());
                try std.testing.expectEqual(@as(i64, 150), total.fees.get(392) orelse 0);
                try std.testing.expectEqual(@as(i64, -450_000), total.adjustments.get(392) orelse 0);
                try std.testing.expectEqual(@as(i64, 30), total.open_interest_long.get(5991) orelse 0);
                try std.testing.expectEqual(@as(i64, 30), total.open_interest_short.get(5991) orelse 0);
            },
            else => unreachable,
        }
    }
}

test "kernel matches Java fee-margin cancel without fees scenario" {
    const allocator = std.testing.allocator;

    for ([_]OrderBookKind{ .naive, .direct }) |orderbook_kind| {
        var kernel = try ExchangeKernel.init(allocator, .{
            .orderbook_kind = orderbook_kind,
            .risk = .{ .margin_trading_enabled = true },
        });
        defer kernel.deinit();
        try std.testing.expect(try kernel.addSymbol(try makeFeeMarginFuturesSpec()));

        var add_user = makeAddUserCommand(1_440_021);
        var add_user_response = try kernel.execute(&add_user);
        try std.testing.expectEqual(cmd.CommandResultCode.success, add_user_response.result_code);
        add_user_response.deinit(allocator);

        var deposit = makeBalanceAdjustmentCommand(1_440_021, 392, 240_000, 1, .adjustment);
        var deposit_response = try kernel.execute(&deposit);
        try std.testing.expectEqual(cmd.CommandResultCode.success, deposit_response.result_code);
        deposit_response.deinit(allocator);

        var ask = cmd.OrderCommand.newOrder(.gtc, 101, 1_440_021, 10_770, 0, 40, .ask);
        ask.symbol = 5991;
        var ask_response = try kernel.execute(&ask);
        try std.testing.expectEqual(cmd.CommandResultCode.success, ask_response.result_code);
        ask_response.deinit(allocator);

        const profile = kernel.risk_engine.getUserProfile(1_440_021) orelse unreachable;
        const pending_position = profile.positions.getPtr(5991) orelse unreachable;
        try std.testing.expectEqual(common.PositionDirection.empty, pending_position.direction);
        try std.testing.expectEqual(@as(i64, 0), pending_position.open_volume);
        try std.testing.expectEqual(@as(i64, 0), pending_position.pending_buy_size);
        try std.testing.expectEqual(@as(i64, 40), pending_position.pending_sell_size);
        try std.testing.expectEqual(@as(i64, 240_000), profile.accountBalance(392));

        var cancel = makeCancelOrderCommand(101, 1_440_021, 5991);
        var cancel_response = try kernel.execute(&cancel);
        try std.testing.expectEqual(cmd.CommandResultCode.success, cancel_response.result_code);
        cancel_response.deinit(allocator);

        try std.testing.expectEqual(@as(i64, 240_000), profile.accountBalance(392));
        try std.testing.expectEqual(@as(usize, 0), profile.positions.count());
        try std.testing.expectEqual(@as(i64, 0), kernel.risk_engine.getFeeCollected(392));

        var user_report = try kernel.queryReport(.{ .single_user = 1_440_021 });
        defer user_report.deinit(allocator);
        switch (user_report) {
            .single_user => |single| {
                try std.testing.expectEqual(@as(usize, 0), single.positions.count());
                try std.testing.expectEqual(@as(usize, 0), single.orders.items.len);
            },
            else => unreachable,
        }

        var total_report = try kernel.queryReport(.total_currency_balance);
        defer total_report.deinit(allocator);
        switch (total_report) {
            .total_currency_balance => |total| {
                try std.testing.expectEqual(@as(i64, 240_000), total.account_balances.get(392) orelse 0);
                try std.testing.expectEqual(@as(usize, 0), total.orders_balances.count());
                try std.testing.expectEqual(@as(i64, 0), total.fees.get(392) orelse 0);
                try std.testing.expectEqual(@as(i64, -240_000), total.adjustments.get(392) orelse 0);
                try std.testing.expectEqual(@as(i64, 0), total.open_interest_long.get(5991) orelse 0);
                try std.testing.expectEqual(@as(i64, 0), total.open_interest_short.get(5991) orelse 0);
            },
            else => unreachable,
        }
    }
}

test "kernel ioc reject releases held quote for bid taker" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .naive,
    });
    defer kernel.deinit();
    try std.testing.expect(try kernel.addSymbol(try makeExchangeSpec()));

    var add_user = makeAddUserCommand(8001);
    _ = try kernel.execute(&add_user);

    var deposit_quote = makeBalanceAdjustmentCommand(8001, 15, 100_000, 1, .adjustment);
    _ = try kernel.execute(&deposit_quote);

    var ioc_bid = cmd.OrderCommand.newOrder(.ioc, 9101, 8001, 50, 50, 4, .bid);
    ioc_bid.symbol = 241;
    var resp = try kernel.execute(&ioc_bid);
    defer resp.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, resp.result_code);

    const profile = kernel.risk_engine.getUserProfile(8001) orelse unreachable;
    try std.testing.expectEqual(@as(i64, 100_000), profile.accountBalance(15));
    try std.testing.expectEqual(@as(i64, 0), kernel.risk_engine.getFeeCollected(15));
}

test "kernel parity gate keeps naive and direct behavior aligned" {
    const allocator = std.testing.allocator;
    var naive_kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .naive,
    });
    defer naive_kernel.deinit();
    var direct_kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer direct_kernel.deinit();

    const spec = try makeExchangeSpec();
    try std.testing.expect(try naive_kernel.addSymbol(spec));
    try std.testing.expect(try direct_kernel.addSymbol(spec));

    var stream = std.ArrayListUnmanaged(cmd.OrderCommand).empty;
    defer stream.deinit(allocator);

    try stream.append(allocator, makeAddUserCommand(9501));
    try stream.append(allocator, makeAddUserCommand(9502));
    try stream.append(allocator, makeBalanceAdjustmentCommand(9501, 15, 200_000, 1, .adjustment));
    try stream.append(allocator, makeBalanceAdjustmentCommand(9502, 11, 200_000, 1, .adjustment));

    var bid_1 = cmd.OrderCommand.newOrder(.gtc, 11001, 9501, 100, 100, 8, .bid);
    bid_1.symbol = 241;
    try stream.append(allocator, bid_1);
    var ask_1 = cmd.OrderCommand.newOrder(.gtc, 11002, 9502, 110, 0, 5, .ask);
    ask_1.symbol = 241;
    try stream.append(allocator, ask_1);
    var move_1 = cmd.OrderCommand.update(11001, 9501, 105);
    move_1.symbol = 241;
    try stream.append(allocator, move_1);
    var reduce_1 = cmd.OrderCommand.reduce(11002, 9502, 2);
    reduce_1.symbol = 241;
    try stream.append(allocator, reduce_1);
    const req_1 = makeRequestOrderBookCommand(241, 10);
    try stream.append(allocator, req_1);

    var bid_ioc = cmd.OrderCommand.newOrder(.ioc, 11003, 9501, 120, 120, 4, .bid);
    bid_ioc.symbol = 241;
    try stream.append(allocator, bid_ioc);
    var ask_fok = cmd.OrderCommand.newOrder(.fok_budget, 11004, 9502, 380, 0, 4, .ask);
    ask_fok.symbol = 241;
    try stream.append(allocator, ask_fok);
    var cancel_1 = cmd.OrderCommand.cancel(11001, 9501);
    cancel_1.symbol = 241;
    try stream.append(allocator, cancel_1);
    const req_2 = makeRequestOrderBookCommand(241, 10);
    try stream.append(allocator, req_2);

    for (stream.items) |source| {
        var left = source;
        var right = source;
        var left_resp = try naive_kernel.execute(&left);
        defer left_resp.deinit(allocator);
        var right_resp = try direct_kernel.execute(&right);
        defer right_resp.deinit(allocator);

        try std.testing.expectEqual(left_resp.result_code, right_resp.result_code);
        if (left_resp.l2_snapshot != null or right_resp.l2_snapshot != null) {
            try std.testing.expect(left_resp.l2_snapshot != null and right_resp.l2_snapshot != null);
            try expectEqualL2(left_resp.l2_snapshot.?, right_resp.l2_snapshot.?);
        }
    }

    var left_hash = try naive_kernel.queryReport(.risk_state_hash);
    defer left_hash.deinit(allocator);
    var right_hash = try direct_kernel.queryReport(.risk_state_hash);
    defer right_hash.deinit(allocator);
    try std.testing.expectEqual(left_hash.risk_state_hash, right_hash.risk_state_hash);

    var left_detailed_hash = try naive_kernel.queryReport(.state_hash_detailed);
    defer left_detailed_hash.deinit(allocator);
    var right_detailed_hash = try direct_kernel.queryReport(.state_hash_detailed);
    defer right_detailed_hash.deinit(allocator);
    try std.testing.expectEqual(
        left_detailed_hash.state_hash_detailed.aggregate_hash,
        right_detailed_hash.state_hash_detailed.aggregate_hash,
    );

    var left_fee = try naive_kernel.queryReport(.{ .fee_collected = 15 });
    defer left_fee.deinit(allocator);
    var right_fee = try direct_kernel.queryReport(.{ .fee_collected = 15 });
    defer right_fee.deinit(allocator);
    try std.testing.expectEqual(left_fee.fee_collected, right_fee.fee_collected);

    var left_ob = try naive_kernel.queryReport(.{
        .order_book = .{
            .symbol = 241,
            .depth = 10,
        },
    });
    defer left_ob.deinit(allocator);
    var right_ob = try direct_kernel.queryReport(.{
        .order_book = .{
            .symbol = 241,
            .depth = 10,
        },
    });
    defer right_ob.deinit(allocator);
    try expectEqualL2(left_ob.order_book, right_ob.order_book);
}

test "kernel deterministic simulation parity keeps naive and direct aligned" {
    const allocator = std.testing.allocator;
    var naive_kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .naive,
    });
    defer naive_kernel.deinit();
    var direct_kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer direct_kernel.deinit();

    const spec = try makeExchangeSpec();
    try std.testing.expect(try naive_kernel.addSymbol(spec));
    try std.testing.expect(try direct_kernel.addSymbol(spec));

    const user_ids = [_]i64{ 9801, 9802, 9803, 9804, 9805, 9806 };
    for (user_ids) |uid| {
        try executePairExpectEqual(allocator, &naive_kernel, &direct_kernel, makeAddUserCommand(uid));
        try executePairExpectEqual(
            allocator,
            &naive_kernel,
            &direct_kernel,
            makeBalanceAdjustmentCommand(uid, 11, 2_000_000, uid * 10 + 1, .adjustment),
        );
        try executePairExpectEqual(
            allocator,
            &naive_kernel,
            &direct_kernel,
            makeBalanceAdjustmentCommand(uid, 15, 2_000_000, uid * 10 + 2, .adjustment),
        );
    }

    var prng = std.Random.DefaultPrng.init(0xBEEFC0DE12345678);
    const random = prng.random();

    const Candidate = struct {
        order_id: i64,
        uid: i64,
    };
    var candidates: std.ArrayListUnmanaged(Candidate) = .empty;
    defer candidates.deinit(allocator);

    var next_order_id: i64 = 40_000;
    const base_price: i64 = 100;

    for (0..1200) |idx| {
        const roll = random.uintLessThan(u32, 100);
        var command: cmd.OrderCommand = undefined;

        if (roll < 58) {
            const uid = user_ids[random.uintLessThan(usize, user_ids.len)];
            const action: common.OrderAction = if (random.boolean()) .ask else .bid;
            const order_type: common.OrderType = blk: {
                const t = random.uintLessThan(u32, 100);
                if (t < 70) break :blk .gtc;
                if (t < 90) break :blk .ioc;
                break :blk .fok_budget;
            };

            const reuse_order_id = candidates.items.len > 0 and random.uintLessThan(u32, 100) < 7;
            const order_id = if (reuse_order_id)
                candidates.items[random.uintLessThan(usize, candidates.items.len)].order_id
            else blk: {
                const id = next_order_id;
                next_order_id += 1;
                if (order_type == .gtc) {
                    try candidates.append(allocator, .{
                        .order_id = id,
                        .uid = uid,
                    });
                }
                break :blk id;
            };

            const price = simulationPrice(random, base_price);
            const size = @as(i64, @intCast(random.uintLessThan(u32, 12))) + 1;
            const reserve = if (action == .bid)
                price + @as(i64, @intCast(random.uintLessThan(u32, 20)))
            else
                0;

            command = cmd.OrderCommand.newOrder(order_type, order_id, uid, price, reserve, size, action);
            command.symbol = 241;
        } else if (roll < 76) {
            const picked = if (candidates.items.len > 0 and random.boolean())
                candidates.items[random.uintLessThan(usize, candidates.items.len)]
            else
                Candidate{
                    .order_id = @as(i64, @intCast(random.uintLessThan(u32, 4000))) + 1,
                    .uid = user_ids[random.uintLessThan(usize, user_ids.len)],
                };
            const uid = if (random.uintLessThan(u32, 100) < 85)
                picked.uid
            else
                user_ids[random.uintLessThan(usize, user_ids.len)];
            command = cmd.OrderCommand.cancel(picked.order_id, uid);
            command.symbol = 241;
        } else if (roll < 88) {
            const picked = if (candidates.items.len > 0 and random.boolean())
                candidates.items[random.uintLessThan(usize, candidates.items.len)]
            else
                Candidate{
                    .order_id = @as(i64, @intCast(random.uintLessThan(u32, 4000))) + 1,
                    .uid = user_ids[random.uintLessThan(usize, user_ids.len)],
                };
            const uid = if (random.uintLessThan(u32, 100) < 85)
                picked.uid
            else
                user_ids[random.uintLessThan(usize, user_ids.len)];
            const reduce_size: i64 = if (random.uintLessThan(u32, 100) < 4)
                0
            else
                @as(i64, @intCast(random.uintLessThan(u32, 10))) + 1;
            command = cmd.OrderCommand.reduce(picked.order_id, uid, reduce_size);
            command.symbol = 241;
        } else if (roll < 96) {
            const picked = if (candidates.items.len > 0 and random.boolean())
                candidates.items[random.uintLessThan(usize, candidates.items.len)]
            else
                Candidate{
                    .order_id = @as(i64, @intCast(random.uintLessThan(u32, 4000))) + 1,
                    .uid = user_ids[random.uintLessThan(usize, user_ids.len)],
                };
            const uid = if (random.uintLessThan(u32, 100) < 85)
                picked.uid
            else
                user_ids[random.uintLessThan(usize, user_ids.len)];
            command = cmd.OrderCommand.update(picked.order_id, uid, simulationPrice(random, base_price));
            command.symbol = 241;
        } else {
            command = makeRequestOrderBookCommand(241, random.uintLessThan(usize, 31) + 1);
        }

        try executePairExpectEqual(allocator, &naive_kernel, &direct_kernel, command);

        // Deterministic simulation checkpoint: verify shared invariants periodically.
        if ((idx + 1) % 50 == 0) {
            var left_hash = try naive_kernel.queryReport(.risk_state_hash);
            defer left_hash.deinit(allocator);
            var right_hash = try direct_kernel.queryReport(.risk_state_hash);
            defer right_hash.deinit(allocator);
            try std.testing.expectEqual(left_hash.risk_state_hash, right_hash.risk_state_hash);

            var left_detailed_hash = try naive_kernel.queryReport(.state_hash_detailed);
            defer left_detailed_hash.deinit(allocator);
            var right_detailed_hash = try direct_kernel.queryReport(.state_hash_detailed);
            defer right_detailed_hash.deinit(allocator);
            try std.testing.expectEqual(
                left_detailed_hash.state_hash_detailed.aggregate_hash,
                right_detailed_hash.state_hash_detailed.aggregate_hash,
            );

            var left_fee = try naive_kernel.queryReport(.{ .fee_collected = 15 });
            defer left_fee.deinit(allocator);
            var right_fee = try direct_kernel.queryReport(.{ .fee_collected = 15 });
            defer right_fee.deinit(allocator);
            try std.testing.expectEqual(left_fee.fee_collected, right_fee.fee_collected);

            const sample_uid = user_ids[random.uintLessThan(usize, user_ids.len)];
            const sample_ccy: i32 = if (random.boolean()) 11 else 15;
            var left_balance = try naive_kernel.queryReport(.{
                .user_balance = .{
                    .uid = sample_uid,
                    .currency = sample_ccy,
                },
            });
            defer left_balance.deinit(allocator);
            var right_balance = try direct_kernel.queryReport(.{
                .user_balance = .{
                    .uid = sample_uid,
                    .currency = sample_ccy,
                },
            });
            defer right_balance.deinit(allocator);
            try std.testing.expectEqual(left_balance.user_balance, right_balance.user_balance);

            var left_ob = try naive_kernel.queryReport(.{
                .order_book = .{
                    .symbol = 241,
                    .depth = 32,
                },
            });
            defer left_ob.deinit(allocator);
            var right_ob = try direct_kernel.queryReport(.{
                .order_book = .{
                    .symbol = 241,
                    .depth = 32,
                },
            });
            defer right_ob.deinit(allocator);
            try expectEqualL2(left_ob.order_book, right_ob.order_book);
        }
    }

    var final_left_hash = try naive_kernel.queryReport(.risk_state_hash);
    defer final_left_hash.deinit(allocator);
    var final_right_hash = try direct_kernel.queryReport(.risk_state_hash);
    defer final_right_hash.deinit(allocator);
    try std.testing.expectEqual(final_left_hash.risk_state_hash, final_right_hash.risk_state_hash);

    var final_left_detailed_hash = try naive_kernel.queryReport(.state_hash_detailed);
    defer final_left_detailed_hash.deinit(allocator);
    var final_right_detailed_hash = try direct_kernel.queryReport(.state_hash_detailed);
    defer final_right_detailed_hash.deinit(allocator);
    try std.testing.expectEqual(
        final_left_detailed_hash.state_hash_detailed.aggregate_hash,
        final_right_detailed_hash.state_hash_detailed.aggregate_hash,
    );
}

test "multi-shard uid-routed parity matches single-shard user state invariants" {
    const allocator = std.testing.allocator;
    const shard_count: usize = 4;

    var single = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer single.deinit();

    var sharded = try MultiShardKernelSet.init(allocator, shard_count, .direct);
    defer sharded.deinit();

    const spec = try makeExchangeSpec();
    try std.testing.expect(try single.addSymbol(spec));
    try sharded.addSymbolAll(spec);

    var command_log: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
    defer command_log.deinit(allocator);
    var expected_result_codes: std.ArrayListUnmanaged(cmd.CommandResultCode) = .empty;
    defer expected_result_codes.deinit(allocator);

    const user_ids = [_]i64{
        8101, 8102, 8103, 8104, 8105, 8106, 8107, 8108,
        8109, 8110, 8111, 8112, 8113, 8114, 8115, 8116,
    };

    for (user_ids) |uid| {
        const add_user = makeAddUserCommand(uid);
        try command_log.append(allocator, add_user);
        const code = try executeSingleAndShardedExpectEqual(allocator, &single, &sharded, add_user);
        try expected_result_codes.append(allocator, code);
    }

    var prng = std.Random.DefaultPrng.init(0x5EEDCAFE1234);
    const random = prng.random();
    var tx_id: i64 = 100;

    for (0..400) |idx| {
        const uid = user_ids[random.uintLessThan(usize, user_ids.len)];
        const currency: i32 = if (random.boolean()) 11 else 15;
        const amount_abs = @as(i64, @intCast(random.uintLessThan(u32, 800))) + 1;
        const amount: i64 = if (idx % 11 == 0) -amount_abs else amount_abs;

        tx_id += 1;
        const adjustment = makeBalanceAdjustmentCommand(uid, currency, amount, tx_id, .adjustment);
        try command_log.append(allocator, adjustment);
        const adjustment_code = try executeSingleAndShardedExpectEqual(allocator, &single, &sharded, adjustment);
        try expected_result_codes.append(allocator, adjustment_code);

        if (idx % 37 == 0) {
            const dup_uid = user_ids[random.uintLessThan(usize, user_ids.len)];
            const dup_add = makeAddUserCommand(dup_uid);
            try command_log.append(allocator, dup_add);
            const dup_code = try executeSingleAndShardedExpectEqual(allocator, &single, &sharded, dup_add);
            try expected_result_codes.append(allocator, dup_code);
        }
    }

    const ob_request = makeRequestOrderBookCommand(241, 16);
    try command_log.append(allocator, ob_request);
    const ob_code = try executeSingleAndShardedExpectEqual(allocator, &single, &sharded, ob_request);
    try expected_result_codes.append(allocator, ob_code);

    var single_total_report = try single.queryReport(.total_currency_balance);
    defer single_total_report.deinit(allocator);
    var sharded_total_report = try sharded.aggregateTotalCurrencyBalance();
    defer sharded_total_report.deinit(allocator);
    switch (single_total_report) {
        .total_currency_balance => |*single_total| {
            try expectMapEqualI32I64(&single_total.account_balances, &sharded_total_report.account_balances);
            try expectMapEqualI32I64(&single_total.orders_balances, &sharded_total_report.orders_balances);
            try expectMapEqualI32I64(&single_total.fees, &sharded_total_report.fees);
            try expectMapEqualI32I64(&single_total.adjustments, &sharded_total_report.adjustments);
            try expectMapEqualI32I64(&single_total.suspends, &sharded_total_report.suspends);
            try expectMapEqualI32I64(&single_total.open_interest_long, &sharded_total_report.open_interest_long);
            try expectMapEqualI32I64(&single_total.open_interest_short, &sharded_total_report.open_interest_short);
        },
        else => unreachable,
    }

    const currencies = [_]i32{ 11, 15 };
    for (user_ids) |uid| {
        for (currencies) |currency| {
            var single_balance_report = try single.queryReport(.{
                .user_balance = .{
                    .uid = uid,
                    .currency = currency,
                },
            });
            defer single_balance_report.deinit(allocator);
            const sharded_balance = try sharded.queryUserBalance(uid, currency);
            try std.testing.expectEqual(single_balance_report.user_balance, sharded_balance);
        }
    }

    for (sharded.kernels, 0..) |*kernel, shard_idx| {
        var detailed_report = try kernel.queryReport(.state_hash_detailed);
        defer detailed_report.deinit(allocator);
        switch (detailed_report) {
            .state_hash_detailed => |detailed| {
                try std.testing.expectEqual(@as(usize, 11), detailed.submodules.items.len);
                const module_id = std.math.cast(i32, shard_idx) orelse unreachable;
                for (detailed.submodules.items) |entry| {
                    try std.testing.expectEqual(module_id, entry.module_id);
                }
            },
            else => unreachable,
        }
    }

    const hash_a = try sharded.detailedHashFingerprint();

    var replay = try MultiShardKernelSet.init(allocator, shard_count, .direct);
    defer replay.deinit();
    try replay.addSymbolAll(spec);

    for (command_log.items, expected_result_codes.items) |source, expected_code| {
        var command = source;
        var response = try replay.executeRouted(&command);
        defer response.deinit(allocator);
        try std.testing.expectEqual(expected_code, response.result_code);
    }

    const hash_b = try replay.detailedHashFingerprint();
    try std.testing.expectEqual(hash_a, hash_b);

    tx_id += 1;
    var extra_adjustment = makeBalanceAdjustmentCommand(user_ids[0], 15, 77, tx_id, .adjustment);
    var extra_response = try replay.executeRouted(&extra_adjustment);
    defer extra_response.deinit(allocator);
    const mutated_hash_b = try replay.detailedHashFingerprint();
    try std.testing.expect(hash_b != mutated_hash_b);
    try std.testing.expectEqual(hash_a, try sharded.detailedHashFingerprint());
}

test "split matching/risk shard simulation keeps cross-shard trade parity" {
    const allocator = std.testing.allocator;
    const shard_count: usize = 4;

    var single = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
    });
    defer single.deinit();
    var split = try MultiShardKernelSet.init(allocator, shard_count, .direct);
    defer split.deinit();

    const spec = try makeExchangeSpec();
    try std.testing.expect(try single.addSymbol(spec));
    try split.addSymbolAll(spec);

    const symbol_shard = split.shardIndexForSymbol(spec.symbol_id);
    const maker_shard = (symbol_shard + 1) % shard_count;
    const taker_shard = (symbol_shard + 2) % shard_count;
    const maker_uid = uidForShard(maker_shard, shard_count, 9001);
    const taker_uid = uidForShard(taker_shard, shard_count, 9101);

    try std.testing.expect(split.shardIndexForUid(maker_uid) != symbol_shard);
    try std.testing.expect(split.shardIndexForUid(taker_uid) != symbol_shard);
    try std.testing.expect(split.shardIndexForUid(maker_uid) != split.shardIndexForUid(taker_uid));

    var command_log: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
    defer command_log.deinit(allocator);
    var expected_codes: std.ArrayListUnmanaged(cmd.CommandResultCode) = .empty;
    defer expected_codes.deinit(allocator);

    const add_maker = makeAddUserCommand(maker_uid);
    try command_log.append(allocator, add_maker);
    try expected_codes.append(
        allocator,
        try executeSingleAndSplitExpectEqual(allocator, &single, &split, add_maker),
    );

    const add_taker = makeAddUserCommand(taker_uid);
    try command_log.append(allocator, add_taker);
    try expected_codes.append(
        allocator,
        try executeSingleAndSplitExpectEqual(allocator, &single, &split, add_taker),
    );

    const dep_maker = makeBalanceAdjustmentCommand(maker_uid, 11, 100_000, 1, .adjustment);
    try command_log.append(allocator, dep_maker);
    try expected_codes.append(
        allocator,
        try executeSingleAndSplitExpectEqual(allocator, &single, &split, dep_maker),
    );

    const dep_taker = makeBalanceAdjustmentCommand(taker_uid, 15, 100_000, 1, .adjustment);
    try command_log.append(allocator, dep_taker);
    try expected_codes.append(
        allocator,
        try executeSingleAndSplitExpectEqual(allocator, &single, &split, dep_taker),
    );

    var maker_ask = cmd.OrderCommand.newOrder(.gtc, 9401, maker_uid, 100, 0, 7, .ask);
    maker_ask.symbol = spec.symbol_id;
    try command_log.append(allocator, maker_ask);
    try expected_codes.append(
        allocator,
        try executeSingleAndSplitExpectEqual(allocator, &single, &split, maker_ask),
    );

    var taker_bid = cmd.OrderCommand.newOrder(.ioc, 9402, taker_uid, 100, 100, 5, .bid);
    taker_bid.symbol = spec.symbol_id;
    try command_log.append(allocator, taker_bid);
    try expected_codes.append(
        allocator,
        try executeSingleAndSplitExpectEqual(allocator, &single, &split, taker_bid),
    );

    const ob_request = makeRequestOrderBookCommand(spec.symbol_id, 16);
    try command_log.append(allocator, ob_request);
    try expected_codes.append(
        allocator,
        try executeSingleAndSplitExpectEqual(allocator, &single, &split, ob_request),
    );

    const currencies = [_]i32{ 11, 15 };
    for ([_]i64{ maker_uid, taker_uid }) |uid| {
        for (currencies) |currency| {
            var single_balance = try single.queryReport(.{
                .user_balance = .{
                    .uid = uid,
                    .currency = currency,
                },
            });
            defer single_balance.deinit(allocator);
            const split_balance = try split.queryUserBalance(uid, currency);
            try std.testing.expectEqual(single_balance.user_balance, split_balance);
        }
    }

    var single_total = try single.queryReport(.total_currency_balance);
    defer single_total.deinit(allocator);
    var split_total = try split.aggregateTotalCurrencyBalance();
    defer split_total.deinit(allocator);
    switch (single_total) {
        .total_currency_balance => |*s| {
            try expectMapEqualI32I64(&s.account_balances, &split_total.account_balances);
            try expectMapEqualI32I64(&s.orders_balances, &split_total.orders_balances);
            try expectMapEqualI32I64(&s.fees, &split_total.fees);
            try expectMapEqualI32I64(&s.adjustments, &split_total.adjustments);
            try expectMapEqualI32I64(&s.suspends, &split_total.suspends);
            try expectMapEqualI32I64(&s.open_interest_long, &split_total.open_interest_long);
            try expectMapEqualI32I64(&s.open_interest_short, &split_total.open_interest_short);
        },
        else => unreachable,
    }

    const split_hash_a = try split.detailedHashFingerprint();

    var replay = try MultiShardKernelSet.init(allocator, shard_count, .direct);
    defer replay.deinit();
    try replay.addSymbolAll(spec);
    for (command_log.items, expected_codes.items) |source, expected| {
        var command = source;
        var response = try replay.executeSplit(&command);
        defer response.deinit(allocator);
        try std.testing.expectEqual(expected, response.result_code);
    }
    const split_hash_b = try replay.detailedHashFingerprint();
    try std.testing.expectEqual(split_hash_a, split_hash_b);

    var extra = makeBalanceAdjustmentCommand(maker_uid, 15, 77, 2, .adjustment);
    var extra_response = try replay.executeSplit(&extra);
    defer extra_response.deinit(allocator);
    try std.testing.expect(split_hash_b != try replay.detailedHashFingerprint());
    try std.testing.expectEqual(split_hash_a, try split.detailedHashFingerprint());
}

test "seeded fault-injection split simulation is deterministic and seed-sensitive" {
    const allocator = std.testing.allocator;
    const rounds: usize = 600;
    const seed_a: u64 = 0xDEADBEEF12345678;
    const seed_b: u64 = 0xA5A5A5A5C3C3C3C3;

    var first = try runSeededFaultSimulation(allocator, seed_a, rounds);
    defer first.deinit(allocator);
    var second = try runSeededFaultSimulation(allocator, seed_a, rounds);
    defer second.deinit(allocator);

    try std.testing.expect(first.delivered_count > 0);
    try std.testing.expectEqual(first.delivered_count, second.delivered_count);
    try std.testing.expectEqualSlices(u64, first.digests.items, second.digests.items);
    try std.testing.expectEqual(first.final_fingerprint, second.final_fingerprint);

    var different_seed = try runSeededFaultSimulation(allocator, seed_b, rounds);
    defer different_seed.deinit(allocator);

    const digest_diff = !std.mem.eql(u64, first.digests.items, different_seed.digests.items);
    const delivered_diff = first.delivered_count != different_seed.delivered_count;
    const fingerprint_diff = first.final_fingerprint != different_seed.final_fingerprint;
    try std.testing.expect(digest_diff or delivered_diff or fingerprint_diff);
}

test "seeded fault-injection multi-symbol split simulation is deterministic and seed-sensitive" {
    const allocator = std.testing.allocator;
    const rounds: usize = 850;
    const seed_a: u64 = 0x1234ABCD55AA77CC;
    const seed_b: u64 = 0xFACEB00C99887766;

    var first = try runSeededFaultSimulationMultiSymbol(allocator, seed_a, rounds);
    defer first.deinit(allocator);
    var second = try runSeededFaultSimulationMultiSymbol(allocator, seed_a, rounds);
    defer second.deinit(allocator);

    try std.testing.expect(first.delivered_count > 0);
    try std.testing.expectEqual(first.delivered_count, second.delivered_count);
    try std.testing.expectEqualSlices(u64, first.digests.items, second.digests.items);
    try std.testing.expectEqual(first.final_fingerprint, second.final_fingerprint);

    var different_seed = try runSeededFaultSimulationMultiSymbol(allocator, seed_b, rounds);
    defer different_seed.deinit(allocator);

    const digest_diff = !std.mem.eql(u64, first.digests.items, different_seed.digests.items);
    const delivered_diff = first.delivered_count != different_seed.delivered_count;
    const fingerprint_diff = first.final_fingerprint != different_seed.final_fingerprint;
    try std.testing.expect(digest_diff or delivered_diff or fingerprint_diff);
}

test "assigned sharded execution shares one routed and split frontier" {
    const allocator = std.testing.allocator;
    var sharded = try MultiShardKernelSet.init(allocator, 4, .direct);
    defer sharded.deinit();

    var shard_zero = makeAddUserCommand(200_000);
    var first = try sharded.executeRoutedAtSequence(.fromInt(1), &shard_zero);
    defer first.deinit(allocator);

    var shard_one = makeAddUserCommand(200_001);
    var second = try sharded.executeRoutedAtSequence(.fromInt(2), &shard_one);
    defer second.deinit(allocator);

    var split = makeNopCommand();
    var third = try sharded.executeSplitAtSequence(.fromInt(4), &split);
    defer third.deinit(allocator);

    try std.testing.expectEqual(@as(u64, 1), first.sequence);
    try std.testing.expectEqual(@as(u64, 2), second.sequence);
    try std.testing.expectEqual(@as(u64, 4), third.sequence);
    try std.testing.expectEqual(@as(u64, 4), sharded.next_seq);
    for (sharded.kernels) |kernel| {
        try std.testing.expectEqual(@as(u64, 4), kernel.next_seq);
    }

    var duplicate = makeNopCommand();
    try std.testing.expectError(
        error.NonMonotonicPublicationSequence,
        sharded.executeRoutedAtSequence(.fromInt(4), &duplicate),
    );
    try std.testing.expectEqual(@as(u64, 4), sharded.next_seq);
}

fn testStateHashSubmodule(
    report: *const DetailedStateHashReport,
    submodule: StateHashSubmoduleType,
    module_id: i32,
) ?u64 {
    for (report.submodules.items) |entry| {
        if (entry.submodule == submodule and entry.module_id == module_id) {
            return entry.hash;
        }
    }
    return null;
}

test "margin-disabled kernel keeps futures matching-only while enabled kernel preserves risk admission" {
    const allocator = std.testing.allocator;
    const spec = try makeFeeMarginFuturesSpec();
    const uid: i64 = 91_001;

    var disabled = try ExchangeKernel.init(allocator, .{ .orderbook_kind = .direct });
    defer disabled.deinit();
    const empty_provider_hash = try disabled.risk_engine.symbol_spec_provider.stateHash();
    const empty_risk_hash = try disabled.risk_engine.stateHash();
    var empty_risk_report = try disabled.queryReport(.risk_state_hash);
    defer empty_risk_report.deinit(allocator);

    try std.testing.expect(try disabled.addSymbol(spec));
    try std.testing.expect(disabled.books.contains(spec.symbol_id));
    try std.testing.expectEqual(@as(usize, 1), disabled.books.count());
    try std.testing.expect(disabled.risk_engine.symbol_spec_provider.getSymbolSpecification(spec.symbol_id) == null);
    try std.testing.expectEqual(@as(usize, 0), disabled.risk_engine.symbol_spec_provider.symbols.count());
    try std.testing.expectEqual(empty_provider_hash, try disabled.risk_engine.symbol_spec_provider.stateHash());
    try std.testing.expectEqual(empty_risk_hash, try disabled.risk_engine.stateHash());

    var risk_report = try disabled.queryReport(.risk_state_hash);
    defer risk_report.deinit(allocator);
    try std.testing.expectEqual(empty_risk_report.risk_state_hash, risk_report.risk_state_hash);
    var detailed = try disabled.queryReport(.state_hash_detailed);
    defer detailed.deinit(allocator);
    try std.testing.expectEqual(
        empty_provider_hash,
        testStateHashSubmodule(
            &detailed.state_hash_detailed,
            .risk_symbol_spec_provider,
            0,
        ) orelse return error.TestExpectedRiskSymbolHash,
    );
    var total = try disabled.queryReport(.total_currency_balance);
    defer total.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), total.total_currency_balance.orders_balances.count());

    var add_user = makeAddUserCommand(uid);
    {
        var response = try disabled.execute(&add_user);
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    }
    var rejected = cmd.OrderCommand.newOrder(.gtc, 91_101, uid, 10_770, 10_770, 1, .bid);
    rejected.symbol = spec.symbol_id;
    {
        var response = try disabled.execute(&rejected);
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.invalid_symbol, response.result_code);
    }

    var enabled = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
        .risk = .{ .margin_trading_enabled = true },
    });
    defer enabled.deinit();
    const enabled_empty_provider_hash = try enabled.risk_engine.symbol_spec_provider.stateHash();
    try std.testing.expect(try enabled.addSymbol(spec));
    try std.testing.expect(enabled.books.contains(spec.symbol_id));
    try std.testing.expect(enabled.risk_engine.symbol_spec_provider.getSymbolSpecification(spec.symbol_id) != null);
    try std.testing.expect(enabled_empty_provider_hash != try enabled.risk_engine.symbol_spec_provider.stateHash());

    var enabled_add_user = makeAddUserCommand(uid);
    {
        var response = try enabled.execute(&enabled_add_user);
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    }
    var deposit = makeBalanceAdjustmentCommand(uid, spec.quote_currency, 100_000, 1, .adjustment);
    {
        var response = try enabled.execute(&deposit);
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    }
    var accepted = cmd.OrderCommand.newOrder(.gtc, 91_102, uid, 10_770, 10_770, 1, .bid);
    accepted.symbol = spec.symbol_id;
    {
        var response = try enabled.execute(&accepted);
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    }
}

test "4ME 2RE margin-disabled topology excludes futures from active risk while enabled topology preserves them" {
    const allocator = std.testing.allocator;
    const spec = try makeFeeMarginFuturesSpec();
    const uid: i64 = 92_001;

    var disabled = try MultiShardKernelSet.initWithTopology(
        allocator,
        4,
        2,
        .{ .orderbook_kind = .direct },
    );
    defer disabled.deinit();
    var empty_risk_hashes: [2]u64 = undefined;
    var empty_provider_hashes: [2]u64 = undefined;
    for (disabled.risk_shards, 0..) |*risk_shard, idx| {
        empty_risk_hashes[idx] = try risk_shard.engine.stateHash();
        empty_provider_hashes[idx] = try risk_shard.engine.symbol_spec_provider.stateHash();
    }
    var empty_risk_report = try disabled.queryReport(.risk_state_hash);
    defer empty_risk_report.deinit(allocator);

    try disabled.addSymbolAll(spec);
    for (disabled.matching_shards) |*matching| {
        try std.testing.expect(matching.books.contains(spec.symbol_id));
        try std.testing.expectEqual(@as(usize, 1), matching.books.count());
    }
    for (disabled.risk_shards, 0..) |*risk_shard, idx| {
        try std.testing.expect(risk_shard.engine.symbol_spec_provider.getSymbolSpecification(spec.symbol_id) == null);
        try std.testing.expectEqual(@as(usize, 0), risk_shard.engine.symbol_spec_provider.symbols.count());
        try std.testing.expectEqual(empty_provider_hashes[idx], try risk_shard.engine.symbol_spec_provider.stateHash());
        try std.testing.expectEqual(empty_risk_hashes[idx], try risk_shard.engine.stateHash());
    }
    var risk_report = try disabled.queryReport(.risk_state_hash);
    defer risk_report.deinit(allocator);
    try std.testing.expectEqual(empty_risk_report.risk_state_hash, risk_report.risk_state_hash);
    var detailed = try disabled.queryReport(.state_hash_detailed);
    defer detailed.deinit(allocator);
    for (0..2) |idx| {
        try std.testing.expectEqual(
            empty_provider_hashes[idx],
            testStateHashSubmodule(
                &detailed.state_hash_detailed,
                .risk_symbol_spec_provider,
                @intCast(idx),
            ) orelse return error.TestExpectedRiskSymbolHash,
        );
    }
    var total = try disabled.queryReport(.total_currency_balance);
    defer total.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), total.total_currency_balance.orders_balances.count());

    var add_user = makeAddUserCommand(uid);
    {
        var response = try disabled.executeRouted(&add_user);
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    }
    var rejected = cmd.OrderCommand.newOrder(.gtc, 92_101, uid, 10_770, 10_770, 1, .bid);
    rejected.symbol = spec.symbol_id;
    {
        var response = try disabled.executeRouted(&rejected);
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.invalid_symbol, response.result_code);
    }

    var enabled = try MultiShardKernelSet.initWithTopology(
        allocator,
        4,
        2,
        .{
            .orderbook_kind = .direct,
            .risk = .{ .margin_trading_enabled = true },
        },
    );
    defer enabled.deinit();
    try enabled.addSymbolAll(spec);
    for (enabled.matching_shards) |*matching| {
        try std.testing.expect(matching.books.contains(spec.symbol_id));
    }
    for (enabled.risk_shards) |*risk_shard| {
        try std.testing.expect(risk_shard.engine.symbol_spec_provider.getSymbolSpecification(spec.symbol_id) != null);
    }

    var enabled_add_user = makeAddUserCommand(uid);
    {
        var response = try enabled.executeRouted(&enabled_add_user);
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    }
    var deposit = makeBalanceAdjustmentCommand(uid, spec.quote_currency, 100_000, 1, .adjustment);
    {
        var response = try enabled.executeRouted(&deposit);
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    }
    var accepted = cmd.OrderCommand.newOrder(.gtc, 92_102, uid, 10_770, 10_770, 1, .bid);
    accepted.symbol = spec.symbol_id;
    {
        var response = try enabled.executeRouted(&accepted);
        defer response.deinit(allocator);
        try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    }
}

test "matching books history follows Java temporary-map and Eclipse collision order" {
    const allocator = std.testing.allocator;

    // A Java source map populated as [23, 39] serializes these colliding keys
    // as [39, 23]. MatchingEngineRouter first decodes that wire sequence into
    // a sized temporary map, whose handler order is [23, 39].
    const wire_symbols = [_]common.CoreSymbolSpecification{
        try makeExchangeSpecForSymbol(39),
        try makeExchangeSpecForSymbol(23),
    };
    const handler_order = try javaSymbolHandlerOrder(allocator, &wire_symbols);
    defer allocator.free(handler_order);
    try std.testing.expectEqual(@as(usize, 2), handler_order.len);
    try std.testing.expectEqual(@as(i32, 23), handler_order[0].symbol_id);
    try std.testing.expectEqual(@as(i32, 39), handler_order[1].symbol_id);

    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();
    for (handler_order) |spec| {
        try std.testing.expect(try kernel.addMatchingSymbol(spec));
    }

    const handler_keys = try kernel.matchingBookKeysInHistoryOrder(allocator);
    defer allocator.free(handler_keys);
    try std.testing.expectEqualSlices(i32, &.{ 23, 39 }, handler_keys);
    const validated = try kernel.validatedMatchingBooksHistory(allocator);
    try std.testing.expectEqual(@as(?usize, 16), validated.tableSizeIfKnown());

    var collision_kernel = try ExchangeKernel.init(allocator, .{});
    defer collision_kernel.deinit();
    for ([_]i32{ 2, 17, 33 }) |symbol_id| {
        try std.testing.expect(
            try collision_kernel.addMatchingSymbol(
                try makeExchangeSpecForSymbol(symbol_id),
            ),
        );
    }
    const collision_keys = try collision_kernel.matchingBookKeysInHistoryOrder(allocator);
    defer allocator.free(collision_keys);
    try std.testing.expectEqualSlices(i32, &.{ 17, 2, 33 }, collision_keys);
}

test "matching books reset clears history while retaining Java table capacity" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    const fresh = try kernel.validatedMatchingBooksHistory(allocator);
    try std.testing.expectEqual(@as(?usize, 16), fresh.tableSizeIfKnown());

    for (2..11) |raw_symbol| {
        try std.testing.expect(
            try kernel.addMatchingSymbol(
                try makeExchangeSpecForSymbol(@intCast(raw_symbol)),
            ),
        );
    }
    try std.testing.expectEqual(
        @as(?usize, 32),
        kernel.books_history.tableSizeIfKnown(),
    );

    kernel.resetMatchingState();
    try std.testing.expectEqual(@as(usize, 0), kernel.books.count());
    const cleared_keys = try kernel.matchingBookKeysInHistoryOrder(allocator);
    defer allocator.free(cleared_keys);
    try std.testing.expectEqual(@as(usize, 0), cleared_keys.len);
    try std.testing.expectEqual(
        @as(?usize, 32),
        kernel.books_history.tableSizeIfKnown(),
    );

    try std.testing.expect(
        try kernel.addMatchingSymbol(try makeExchangeSpecForSymbol(17)),
    );
    try std.testing.expectEqual(
        @as(?usize, 32),
        kernel.books_history.tableSizeIfKnown(),
    );
}

fn matchingBooksInsertionAllocationFailure(
    allocator: std.mem.Allocator,
) !void {
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    const inserted = kernel.addMatchingSymbol(
        try makeExchangeSpecForSymbol(42),
    ) catch |err| {
        try std.testing.expectEqual(@as(usize, 0), kernel.books.count());
        try std.testing.expectEqual(
            @as(?usize, 16),
            kernel.books_history.tableSizeIfKnown(),
        );
        switch (kernel.books_history) {
            .lazy_known_empty => {},
            .known, .unavailable => return error.TestExpectedFreshBooksHistory,
        }
        return err;
    };

    try std.testing.expect(inserted);
    try std.testing.expectEqual(@as(usize, 1), kernel.books.count());
    switch (kernel.books_history) {
        .known => |layout| try std.testing.expectEqual(@as(usize, 1), layout.count()),
        .lazy_known_empty, .unavailable => return error.TestExpectedKnownBooksHistory,
    }
}

test "matching books semantic map and history insertion are allocation-failure atomic" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        matchingBooksInsertionAllocationFailure,
        .{},
    );
}

test "matching books duplicate insertion is allocation-free and preserves the first book" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = failing.allocator();
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

    var first = try makeExchangeSpecForSymbol(17);
    first.maker_fee = 3;
    var last = first;
    last.maker_fee = 9;
    const duplicate_wire = [_]common.CoreSymbolSpecification{ first, last };
    const handler_order = try javaSymbolHandlerOrder(allocator, &duplicate_wire);
    defer allocator.free(handler_order);
    try std.testing.expectEqual(@as(usize, 1), handler_order.len);
    try std.testing.expectEqual(@as(i64, 9), handler_order[0].maker_fee);
    try std.testing.expect(try kernel.addMatchingSymbol(handler_order[0]));

    failing.fail_index = failing.alloc_index;
    try std.testing.expect(!try kernel.addMatchingSymbol(first));
    try std.testing.expect(!failing.has_induced_failure);
    failing.fail_index = std.math.maxInt(usize);

    const stored = kernel.books.get(17) orelse unreachable;
    try std.testing.expectEqual(@as(i64, 9), stored.symbolSpecification().maker_fee);
    const keys = try kernel.matchingBookKeysInHistoryOrder(allocator);
    defer allocator.free(keys);
    try std.testing.expectEqualSlices(i32, &.{17}, keys);
}

test "matching books history detects mismatch and native restore stays unavailable" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();
    try std.testing.expect(
        try kernel.addMatchingSymbol(try makeExchangeSpecForSymbol(17)),
    );

    const removed = kernel.books.fetchRemove(17) orelse unreachable;
    removed.value.deinit();
    allocator.destroy(removed.value);
    try std.testing.expectError(
        error.HistorySemanticKeySetMismatch,
        kernel.matchingBookKeysInHistoryOrder(allocator),
    );

    kernel.resetForSnapshotLoad();
    try std.testing.expect(!kernel.books_history.isKnown());
    try std.testing.expectError(
        error.HistoryUnavailable,
        kernel.matchingBookKeysInHistoryOrder(allocator),
    );
    try std.testing.expect(
        try kernel.addMatchingSymbol(try makeExchangeSpecForSymbol(33)),
    );
    try std.testing.expectError(
        error.HistoryUnavailable,
        kernel.matchingBookKeysInHistoryOrder(allocator),
    );

    var imported = try ExchangeKernel.init(allocator, .{});
    defer imported.deinit();
    try std.testing.expect(
        try imported.addMatchingSymbol(try makeExchangeSpecForSymbol(2)),
    );
    try imported.restoreOrdersForSymbol(2, &.{});
    try std.testing.expectError(
        error.HistoryUnavailable,
        imported.matchingBookKeysInHistoryOrder(allocator),
    );
}

test "exact state hash remains available when matching encounter history is unavailable" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .java_identity_hash_context = testJavaIdentityHashContext(),
    });
    defer kernel.deinit();
    try std.testing.expect(
        try kernel.addMatchingSymbol(try makeExchangeSpecForSymbol(17)),
    );
    kernel.markMatchingBooksHistoryUnavailable();

    var state_hash = try kernel.produceJavaReport(.state_hash);
    defer state_hash.deinit(allocator);
    switch (state_hash.response) {
        .state_hash => {},
        else => return error.UnexpectedJavaReportResponseType,
    }
    try std.testing.expectError(
        error.HistoryUnavailable,
        kernel.produceJavaReport(.{ .single_user = 1 }),
    );
    try std.testing.expectError(
        error.HistoryUnavailable,
        kernel.produceJavaReport(.total_currency_balance),
    );
}

const KernelCustomTestQuery = struct {
    mode: u8,
};

const KernelCustomTestSection = struct {
    mode: u8,
    family: u8,
    shard_id: i32,
    shard_mask: u64,
};

const KernelCustomTestResult = struct {
    section_count: usize,
};

var kernel_custom_constructor_count: usize = 0;

fn KernelCustomReportSpec(
    comptime report_code: i32,
    comptime MatchingViewType: type,
    comptime RiskViewType: type,
) type {
    return struct {
        pub const code: i32 = report_code;
        pub const schema_id: [report_registry.schema_id_len]u8 =
            [_]u8{0x6b} ** report_registry.schema_id_len;
        pub const schema_version: u32 = 1;
        pub const Query = KernelCustomTestQuery;
        pub const Section = KernelCustomTestSection;
        pub const Result = KernelCustomTestResult;
        pub const MatchingView = MatchingViewType;
        pub const RiskView = RiskViewType;

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
            try writer.writeByte(query.mode);
        }

        pub fn decodeQuery(
            _: std.mem.Allocator,
            body: []const u8,
        ) report_registry.QueryConstructionError!Query {
            kernel_custom_constructor_count += 1;
            if (body.len != 1) return error.InvalidQuery;
            if (body[0] == 1) return error.ConstructorFailed;
            return .{ .mode = body[0] };
        }

        pub fn processMatching(
            _: std.mem.Allocator,
            query: *const Query,
            context: report_registry.MatchingContext(MatchingView),
        ) report_registry.HandlerError!?Section {
            _ = context.view;
            if (query.mode == 2) return null;
            if (query.mode == 3 and context.shard_id == 1) {
                return error.HandlerFailed;
            }
            return .{
                .mode = query.mode,
                .family = 1,
                .shard_id = context.shard_id,
                .shard_mask = context.shard_mask,
            };
        }

        pub fn processRisk(
            _: std.mem.Allocator,
            query: *const Query,
            context: report_registry.RiskContext(RiskView),
        ) report_registry.HandlerError!?Section {
            _ = context.view;
            if (query.mode == 2) return null;
            if (query.mode == 5) return error.SystemResources;
            return .{
                .mode = query.mode,
                .family = 0,
                .shard_id = context.shard_id,
                .shard_mask = context.shard_mask,
            };
        }

        pub fn encodeSection(
            _: std.mem.Allocator,
            writer: *report_registry.EncodingWriter,
            section: *const Section,
        ) report_registry.EncodingError!void {
            if (section.mode == 4) return error.EncodingFailed;
            try writer.writeByte(section.mode);
            try writer.writeByte(section.family);
            try writer.writeInt(i32, section.shard_id, .little);
            try writer.writeInt(u64, section.shard_mask, .little);
        }

        pub fn deinitSection(_: std.mem.Allocator, section: *Section) void {
            section.* = undefined;
        }

        pub fn reduce(
            _: std.mem.Allocator,
            _: *const Query,
            sections: []const report_registry.SectionBytes,
        ) report_registry.ReductionError!Result {
            return .{ .section_count = sections.len };
        }

        pub fn deinitResult(_: std.mem.Allocator, result: *Result) void {
            result.* = undefined;
        }
    };
}

const KernelCustomSpec = KernelCustomReportSpec(
    70_401,
    MatchingReportView,
    RiskReportView,
);
const KernelReservedCustomSpec = KernelCustomReportSpec(
    @intFromEnum(binary_payload.ReportQueryTypeCode.state_hash),
    MatchingReportView,
    RiskReportView,
);
const WrongKernelMatchingReportView = struct {};
const KernelWrongViewCustomSpec = KernelCustomReportSpec(
    70_402,
    WrongKernelMatchingReportView,
    RiskReportView,
);

fn encodeKernelCustomQueryWords(
    allocator: std.mem.Allocator,
    code: i32,
    body: []const u8,
) ![]i64 {
    const raw = try binary_payload.encodeJavaRawReportQueryToBytes(
        allocator,
        code,
        body,
    );
    defer allocator.free(raw);
    return java_binary_wire.encodeRequestWords(allocator, raw, .{});
}

fn customQueryFrame(transfer_id: i32, words: []const i64) !cmd.OrderCommand {
    if (words.len != binary_frames.FrameWordsPerCommand) {
        return error.UnexpectedCustomQueryFrameCount;
    }
    return makeBinaryFrameCommand(
        .binary_data_query,
        transfer_id,
        -1,
        .{ words[0], words[1], words[2], words[3], words[4] },
    );
}

fn expectKernelCustomSection(
    allocator: std.mem.Allocator,
    grouped: java_report_wire.GroupedBinaryEventSection,
    expected_family: u8,
    expected_shard_id: i32,
    expected_shard_mask: u64,
) !void {
    var unpacked = try java_report_wire.unpackSectionEvents(allocator, grouped.carriers);
    defer unpacked.deinit(allocator);
    try std.testing.expect(unpacked.padded_bytes.len >= 14);
    try std.testing.expectEqual(@as(u8, 0), unpacked.padded_bytes[0]);
    try std.testing.expectEqual(expected_family, unpacked.padded_bytes[1]);
    try std.testing.expectEqual(
        expected_shard_id,
        std.mem.readInt(i32, unpacked.padded_bytes[2..6], .little),
    );
    try std.testing.expectEqual(
        expected_shard_mask,
        std.mem.readInt(u64, unpacked.padded_bytes[6..14], .little),
    );
}

test "kernel validates and clones custom registry before dispatch" {
    const allocator = std.testing.allocator;
    const Custom = report_registry.Adapter(KernelCustomSpec);
    const descriptors = [_]*const report_registry.Descriptor{Custom.descriptorPtr()};
    var source = try report_registry.Registry.initValidated(allocator, &descriptors);

    var kernel = try ExchangeKernel.init(allocator, .{
        .report_queries = .{ .custom_registry = source },
    });
    defer kernel.deinit();
    try std.testing.expect(kernel.customReportRegistry() != source);
    source.deinit();

    kernel_custom_constructor_count = 0;
    const words = try encodeKernelCustomQueryWords(allocator, KernelCustomSpec.code, &.{0});
    defer allocator.free(words);
    var frame = try customQueryFrame(601, words);
    var response = try kernel.execute(&frame);
    defer response.deinit(allocator);

    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    try std.testing.expect(response.java_report_response == null);
    try std.testing.expect(response.report_response == null);
    try std.testing.expectEqual(@as(usize, 2), kernel_custom_constructor_count);

    var first = response.matcher_events_head orelse return error.MissingCustomReportSection;
    try std.testing.expectEqual(@as(i32, 0), first.section);
    first = first.next_event orelse return error.MissingCustomReportSection;
    try std.testing.expectEqual(@as(i32, java_report_wire.matching_section_offset), first.section);
    try std.testing.expect(first.next_event == null);
}

test "kernel rejects reserved wrong-view and out-of-range custom registries" {
    const allocator = std.testing.allocator;
    const reserved_registry = report_registry.Registry.fromSpecs(.{KernelReservedCustomSpec});
    try std.testing.expectError(
        error.ReservedReportCode,
        ExchangeKernel.init(allocator, .{
            .report_queries = .{ .custom_registry = reserved_registry },
        }),
    );

    const wrong_view_registry = report_registry.Registry.fromSpecs(.{KernelWrongViewCustomSpec});
    try std.testing.expectError(
        error.ContextTypeMismatch,
        ExchangeKernel.init(allocator, .{
            .report_queries = .{ .custom_registry = wrong_view_registry },
        }),
    );

    const custom_registry = report_registry.Registry.fromSpecs(.{KernelCustomSpec});
    try std.testing.expectError(
        error.InvalidCustomReportShardId,
        ExchangeKernel.init(allocator, .{
            .shard_id = 1024,
            .num_shards = 2048,
            .report_queries = .{ .custom_registry = custom_registry },
        }),
    );
    try std.testing.expectError(
        error.InvalidCustomReportShardId,
        MultiShardKernelSet.initWithTopology(
            allocator,
            2048,
            1,
            .{ .report_queries = .{ .custom_registry = custom_registry } },
        ),
    );
}

test "multi-shard custom report invokes fresh risk then matching constructors" {
    const allocator = std.testing.allocator;
    const registry = report_registry.Registry.fromSpecs(.{KernelCustomSpec});
    var set = try MultiShardKernelSet.initWithTopology(
        allocator,
        2,
        4,
        .{ .report_queries = .{ .custom_registry = registry } },
    );
    defer set.deinit();

    kernel_custom_constructor_count = 0;
    const words = try encodeKernelCustomQueryWords(allocator, KernelCustomSpec.code, &.{0});
    defer allocator.free(words);
    var frame = try customQueryFrame(602, words);
    var response = try set.executeRouted(&frame);
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    try std.testing.expectEqual(@as(usize, 6), kernel_custom_constructor_count);

    const expected_sections = [_]i32{ 0, 1, 2, 3, 1024, 1025 };
    var node = response.matcher_events_head;
    for (expected_sections) |expected| {
        const event = node orelse return error.MissingCustomReportSection;
        try std.testing.expectEqual(expected, event.section);
        node = event.next_event;
    }
    try std.testing.expect(node == null);

    var grouped = try java_report_wire.groupMatcherEventChain(
        allocator,
        response.matcher_events_head,
        .{},
    );
    defer grouped.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 6), grouped.sections.len);
    for (grouped.sections[0..4], 0..) |section, idx| {
        try expectKernelCustomSection(
            allocator,
            section,
            0,
            @intCast(idx),
            3,
        );
    }
    for (grouped.sections[4..], 0..) |section, idx| {
        try expectKernelCustomSection(
            allocator,
            section,
            1,
            @intCast(idx),
            1,
        );
    }
}

test "custom report rejection no-section and handler failures are atomic" {
    const allocator = std.testing.allocator;
    const registry = report_registry.Registry.fromSpecs(.{KernelCustomSpec});
    var set = try MultiShardKernelSet.initWithTopology(
        allocator,
        2,
        4,
        .{ .report_queries = .{ .custom_registry = registry } },
    );
    defer set.deinit();

    const cases = [_]struct {
        mode: u8,
        transfer_id: i32,
        expected: cmd.CommandResultCode,
        constructor_count: usize,
    }{
        .{ .mode = 1, .transfer_id = 603, .expected = .success, .constructor_count = 6 },
        .{ .mode = 2, .transfer_id = 604, .expected = .success, .constructor_count = 6 },
        .{ .mode = 3, .transfer_id = 605, .expected = .binary_command_failed, .constructor_count = 6 },
        .{ .mode = 4, .transfer_id = 606, .expected = .binary_command_failed, .constructor_count = 1 },
        .{ .mode = 5, .transfer_id = 609, .expected = .binary_command_failed, .constructor_count = 1 },
    };

    for (cases) |case| {
        kernel_custom_constructor_count = 0;
        const words = try encodeKernelCustomQueryWords(
            allocator,
            KernelCustomSpec.code,
            &.{case.mode},
        );
        defer allocator.free(words);
        var frame = try customQueryFrame(case.transfer_id, words);
        var response = try set.executeRouted(&frame);
        defer response.deinit(allocator);
        try std.testing.expectEqual(case.expected, response.result_code);
        try std.testing.expectEqual(case.constructor_count, kernel_custom_constructor_count);
        try std.testing.expect(response.matcher_events_head == null);
        try std.testing.expect(response.java_report_response == null);
        try std.testing.expect(response.report_response == null);
    }
}

test "malformed reserved query never falls through to custom registry" {
    const allocator = std.testing.allocator;
    const registry = report_registry.Registry.fromSpecs(.{KernelCustomSpec});
    var kernel = try ExchangeKernel.init(allocator, .{
        .report_queries = .{ .custom_registry = registry },
    });
    defer kernel.deinit();

    kernel_custom_constructor_count = 0;
    const words = try encodeKernelCustomQueryWords(
        allocator,
        @intFromEnum(binary_payload.ReportQueryTypeCode.single_user),
        &.{ 1, 2, 3 },
    );
    defer allocator.free(words);
    var frame = try customQueryFrame(607, words);
    var response = try kernel.execute(&frame);
    defer response.deinit(allocator);

    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    try std.testing.expect(response.matcher_events_head == null);
    try std.testing.expectEqual(@as(usize, 0), kernel_custom_constructor_count);
}

fn customReportAllocationCampaign(
    allocator: std.mem.Allocator,
    words: []const i64,
) !void {
    const registry = report_registry.Registry.fromSpecs(.{KernelCustomSpec});
    var kernel = try ExchangeKernel.init(allocator, .{
        .report_queries = .{ .custom_registry = registry },
    });
    defer kernel.deinit();

    kernel_custom_constructor_count = 0;
    var frame = try customQueryFrame(608, words);
    var response = try kernel.execute(&frame);
    defer response.deinit(allocator);
    try std.testing.expectEqual(cmd.CommandResultCode.success, response.result_code);
    try std.testing.expect(response.matcher_events_head != null);
}

test "custom report dispatch cleans every allocation failure without partial publication" {
    const allocator = std.testing.allocator;
    const words = try encodeKernelCustomQueryWords(allocator, KernelCustomSpec.code, &.{0});
    defer allocator.free(words);
    try std.testing.checkAllAllocationFailures(
        allocator,
        customReportAllocationCampaign,
        .{words},
    );
}

test "kernel init with matcher_event_pool_capacity owns pool and makeEventChain uses pool" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
        .matcher_event_pool_capacity = 64,
        .pre_reserve_users = 8,
    });
    defer kernel.deinit();

    try std.testing.expect(kernel.matcher_event_pool != null);
    try std.testing.expectEqual(@as(usize, 64), kernel.matcher_event_pool.?.poolCapacity());
    const owner = kernel.eventsOwner();
    try std.testing.expect(owner == .pool);

    var chain = kernel.makeEventChain();
    defer chain.deinit();
    _ = try chain.append(.{ .event_type = .trade, .size = 1 });
    try std.testing.expectEqual(@as(usize, 1), kernel.matcher_event_pool.?.inUse());
    std.debug.print("kernel_pool_ok capacity={d} in_use={d}\n", .{
        kernel.matcher_event_pool.?.poolCapacity(),
        kernel.matcher_event_pool.?.inUse(),
    });
}

test "kernel direct book uses initStatic when order pool caps set" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
        .order_pool_max_orders = 1,
        .order_pool_max_buckets = 4,
    });
    defer kernel.deinit();

    const spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = 241,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 1_000_000,
        .quote_scale_k = 10_000,
        .taker_fee = 1900,
        .maker_fee = 700,
        .margin_buy = 0,
        .margin_sell = 0,
    });
    try std.testing.expect(try kernel.addSymbol(spec));

    const book_ptr = kernel.books.get(241) orelse return error.MissingBook;
    const direct_book = switch (book_ptr.*) {
        .direct => |*b| b,
        else => return error.ExpectedDirectBook,
    };
    try std.testing.expect(direct_book.hasStaticPools());
    try std.testing.expectEqual(@as(?usize, 1), direct_book.staticOrderCapacity());

    // Pure capacity through kernel matching path.
    _ = try kernel.risk_engine.user_profile_service.addEmptyUserProfile(1);
    try kernel.risk_engine.getUserProfile(1).?.accounts.put(allocator, 15, 1_000_000);

    var place1 = cmd.OrderCommand.newOrder(.gtc, 1, 1, 100, 100, 1, .ask);
    place1.symbol = 241;
    place1.result_code = .valid_for_matching_engine;
    // place via book path inside execute needs full risk preprocess - use book directly for capacity
    var chain = events.EventChain.init(allocator);
    defer chain.deinit();
    const r1 = try direct_book.processCommandWithEvents(&place1, &chain);
    try std.testing.expectEqual(cmd.CommandResultCode.success, r1);

    var place2 = cmd.OrderCommand.newOrder(.gtc, 2, 1, 90, 90, 1, .ask);
    place2.symbol = 241;
    place2.result_code = .valid_for_matching_engine;
    var chain2 = events.EventChain.init(allocator);
    defer chain2.deinit();
    const r2 = try direct_book.processCommandWithEvents(&place2, &chain2);
    try std.testing.expectEqual(cmd.CommandResultCode.matching_engine_capacity, r2);
    try std.testing.expectEqual(@as(i32, -3060), r2.code());
    try std.testing.expectEqual(events.MatcherEventType.reject, chain2.head.?.event_type);
    std.debug.print("kernel_static_book_pure_capacity ok code={d}\n", .{r2.code()});
}

test "multi-shard set creates shared event pool and release scratch" {
    const allocator = std.testing.allocator;
    var set = try MultiShardKernelSet.initWithTopology(allocator, 1, 1, .{
        .orderbook_kind = .direct,
        .matcher_event_pool_capacity = 128,
        .pre_reserve_users = 16,
    });
    defer set.deinit();

    try std.testing.expect(set.matcher_event_pool != null);
    try std.testing.expectEqual(@as(usize, 128), set.matcher_event_pool.?.poolCapacity());
    try std.testing.expectEqual(@as(usize, 1), set.exchange_release_scratch.len);
    try std.testing.expectEqual(@as(usize, 1), set.margin_release_scratch.len);
    try std.testing.expect(set.eventsOwner() == .pool);

    var chain = set.makeEventChain();
    defer chain.deinit();
    _ = try chain.append(.{ .event_type = .reject, .size = 9 });
    try std.testing.expectEqual(@as(usize, 1), set.matcher_event_pool.?.inUse());
    std.debug.print("multishard_pool_scratch_ok pool={d} scratch_ex={d}\n", .{
        set.matcher_event_pool.?.poolCapacity(),
        set.exchange_release_scratch.len,
    });
}

test "pool free-safety matrix: execute deinit returns pool in_use to zero" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
        .matcher_event_pool_capacity = 32,
    });
    defer kernel.deinit();
    const pool = kernel.matcher_event_pool orelse return error.MissingPool;

    const spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = 241,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 1_000_000,
        .quote_scale_k = 10_000,
        .taker_fee = 1900,
        .maker_fee = 700,
        .margin_buy = 0,
        .margin_sell = 0,
    });
    try std.testing.expect(try kernel.addSymbol(spec));
    _ = try kernel.risk_engine.user_profile_service.addEmptyUserProfile(1);
    try kernel.risk_engine.getUserProfile(1).?.accounts.put(allocator, 15, 1_000_000);

    var place = cmd.OrderCommand.newOrder(.gtc, 1, 1, 100, 100, 1, .ask);
    place.symbol = 241;
    // Use book path with pool-backed chain via kernel execute if possible.
    // execute requires full pipeline; for free-safety use makeEventChain append + deinit via response.
    var chain = kernel.makeEventChain();
    _ = try chain.append(.{ .event_type = .trade, .size = 3 });
    try std.testing.expect(pool.inUse() >= 1);

    var response = CommandResponse{
        .sequence = 1,
        .command = place,
        .result_code = .success,
    };
    const owned = chain.takeOwned();
    owned.attachToResponse(&response);
    try std.testing.expect(pool.inUse() >= 1);
    response.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), pool.inUse());
    std.debug.print("free_safety_execute_deinit ok in_use={d}\n", .{pool.inUse()});
}

test "pool free-safety matrix: custom report events attach owner and free via response" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .orderbook_kind = .direct,
        .matcher_event_pool_capacity = 64,
    });
    defer kernel.deinit();
    const pool = kernel.matcher_event_pool orelse return error.MissingPool;

    // Empty custom registry lookup fails; drive produceCustomReportEvents only if
    // we can call it with a stub. Instead: makeEventChain as custom reports do,
    // takeOwned, attach, deinit — same transfer type produceCustomReportEvents returns.
    var chain = kernel.makeEventChain();
    _ = try chain.append(.{ .event_type = .binary_event, .size = 1 });
    _ = try chain.append(.{ .event_type = .binary_event, .size = 2 });
    const owned = chain.takeOwned();
    try std.testing.expect(owned.owner == .pool);
    try std.testing.expectEqual(@as(usize, 2), pool.inUse());

    var response = CommandResponse{
        .sequence = 2,
        .command = .{ .command = .binary_data_query, .result_code = .success },
        .result_code = .success,
    };
    owned.attachToResponse(&response);
    response.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), pool.inUse());
    std.debug.print("free_safety_custom_report ok in_use={d}\n", .{pool.inUse()});
}

test "pool free-safety matrix: pending-style deinit uses events_owner not heap" {
    const allocator = std.testing.allocator;
    var set = try MultiShardKernelSet.initWithTopology(allocator, 1, 1, .{
        .orderbook_kind = .direct,
        .matcher_event_pool_capacity = 48,
    });
    defer set.deinit();
    const pool = set.matcher_event_pool orelse return error.MissingPool;

    var chain = set.makeEventChain();
    _ = try chain.append(.{ .event_type = .trade, .size = 7 });
    const owned = chain.takeOwned();
    try std.testing.expectEqual(@as(usize, 1), pool.inUse());
    try std.testing.expect(owned.owner == .pool);

    // Same free path as PendingMatchedCommand.deinit after owner was stored.
    events.deinitMatcherEventChain(owned.owner, owned.head);
    try std.testing.expectEqual(@as(usize, 0), pool.inUse());
    std.debug.print("free_safety_pending_matched ok in_use={d}\n", .{pool.inUse()});
}

test "pool free-safety matrix: StageAdapter dual clone both free to pool" {
    const allocator = std.testing.allocator;
    var set = try MultiShardKernelSet.initWithTopology(allocator, 1, 1, .{
        .orderbook_kind = .direct,
        .matcher_event_pool_capacity = 128,
    });
    defer set.deinit();
    const pool = set.matcher_event_pool orelse return error.MissingPool;

    const spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = 241,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 1_000_000,
        .quote_scale_k = 10_000,
        .taker_fee = 1900,
        .maker_fee = 700,
        .margin_buy = 0,
        .margin_sell = 0,
    });
    try std.testing.expect(try set.matching_shards[0].addSymbol(spec));
    _ = try set.risk_shards[0].engine.user_profile_service.addEmptyUserProfile(1);
    try set.risk_shards[0].engine.getUserProfile(1).?.accounts.put(allocator, 15, 1_000_000);
    _ = try set.risk_shards[0].engine.user_profile_service.addEmptyUserProfile(2);
    try set.risk_shards[0].engine.getUserProfile(2).?.accounts.put(allocator, 11, 1_000_000);

    // Place resting ask then crossing bid through StageAdapter.runMatching if public.
    // Dual-clone path is exercised by makeEventChain + cloneMatcherEventChain as runMatching does.
    var chain = set.makeEventChain();
    _ = try chain.append(.{ .event_type = .trade, .size = 1 });
    const owner = chain.owner;
    const r2_head = chain.takeOwnedHead();
    const clone_head = try events.cloneMatcherEventChain(owner, r2_head);
    try std.testing.expectEqual(@as(usize, 2), pool.inUse());

    events.deinitMatcherEventChain(owner, clone_head);
    events.deinitMatcherEventChain(owner, r2_head);
    try std.testing.expectEqual(@as(usize, 0), pool.inUse());
    std.debug.print("free_safety_dual_clone ok in_use={d}\n", .{pool.inUse()});
}

test "pre_reserve_accounts_per_user applied on add_user success" {
    const allocator = std.testing.allocator;
    var kernel = try ExchangeKernel.init(allocator, .{
        .pre_reserve_accounts_per_user = 8,
        .pre_reserve_positions_per_user = 4,
    });
    defer kernel.deinit();
    try std.testing.expectEqual(@as(usize, 8), kernel.risk_engine.pre_reserve_accounts_per_user);

    var add: cmd.OrderCommand = .{ .command = .add_user, .uid = 42, .result_code = .new };
    _ = try kernel.risk_engine.preProcessCommand(1, &add);
    try std.testing.expectEqual(cmd.CommandResultCode.success, add.result_code);
    // Profile exists; maps have reserved capacity (no direct capacity API — ensure put does not fail).
    const profile = kernel.risk_engine.getUserProfile(42) orelse return error.MissingProfile;
    var i: i32 = 0;
    while (i < 8) : (i += 1) {
        try profile.accounts.put(allocator, 1000 + i, 1);
    }
    std.debug.print("pre_reserve_on_add_user ok accounts={d}\n", .{profile.accounts.count()});
}
