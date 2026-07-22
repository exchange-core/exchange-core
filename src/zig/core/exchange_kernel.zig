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

pub const OrderBookKind = enum {
    naive,
    direct,
};

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
    ) std.mem.Allocator.Error!cmd.CommandResultCode {
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
};

pub const CommandResponse = struct {
    sequence: u64 = 0,
    result_code: cmd.CommandResultCode,
    l2_snapshot: ?naive.L2MarketData = null,
    report_response: ?ReportResponse = null,
    matcher_events_head: ?*events.MatcherTradeEvent = null,

    pub fn deinit(self: *CommandResponse, allocator: std.mem.Allocator) void {
        if (self.l2_snapshot) |*snapshot| {
            snapshot.deinit(allocator);
            self.l2_snapshot = null;
        }
        if (self.report_response) |*report| {
            report.deinit(allocator);
            self.report_response = null;
        }
        events.deinitMatcherEventChain(allocator, self.matcher_events_head);
        self.matcher_events_head = null;
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
};

pub const ReportQueriesConfig = struct {
    state_hash: bool = true,
    single_user: bool = true,
    total_currency_balance: bool = true,
    add_accounts_binary: bool = true,
    add_symbols_binary: bool = true,
};

pub const InitError = risk.InitError || error{
    InvalidL2RefreshDepth,
};
pub const ExecuteError = risk.ReleaseError || error{
    SequenceOverflow,
};
const BinaryPayloadExecutionError = binary_payload.DecodeError ||
    risk.SymbolSpecificationProvider.AddSymbolError ||
    ExecuteError ||
    error{
        BinaryCommandRejected,
    };
const BinaryQueryExecutionError = binary_payload.DecodeQueryError || ReportError;
pub const ReportError = std.mem.Allocator.Error || error{
    UnknownSymbol,
    ArithmeticOverflow,
    UnsupportedReportQueryType,
    BinaryFrameCorrupted,
    StateHashCorrupted,
};
pub const RestingOrder = naive.UserOrderView;
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

pub const ExchangeKernel = struct {
    allocator: std.mem.Allocator,
    orderbook_kind: OrderBookKind,
    report_queries_cfg: ReportQueriesConfig,
    send_l2_for_every_cmd: bool,
    l2_refresh_depth: usize,
    next_seq: u64 = 0,
    books: std.AutoHashMapUnmanaged(i32, *Book) = .empty,
    risk_engine: risk.RiskEngine,
    binary_frame_assembler: binary_frames.BinaryFrameAssembler,

    pub fn init(allocator: std.mem.Allocator, config: Config) InitError!ExchangeKernel {
        if (config.l2_refresh_depth == 0) {
            return error.InvalidL2RefreshDepth;
        }
        return .{
            .allocator = allocator,
            .orderbook_kind = config.orderbook_kind,
            .report_queries_cfg = config.report_queries,
            .send_l2_for_every_cmd = config.send_l2_for_every_cmd,
            .l2_refresh_depth = config.l2_refresh_depth,
            .risk_engine = try risk.RiskEngine.init(
                allocator,
                config.shard_id,
                config.num_shards,
                config.risk,
            ),
            .binary_frame_assembler = binary_frames.BinaryFrameAssembler.init(allocator),
        };
    }

    pub fn deinit(self: *ExchangeKernel) void {
        var it = self.books.valueIterator();
        while (it.next()) |book_ptr| {
            book_ptr.*.deinit();
            self.allocator.destroy(book_ptr.*);
        }
        self.books.deinit(self.allocator);
        self.binary_frame_assembler.deinit();
        self.risk_engine.deinit();
        self.* = undefined;
    }

    pub fn resetForSnapshotLoad(self: *ExchangeKernel) void {
        var it = self.books.valueIterator();
        while (it.next()) |book_ptr| {
            book_ptr.*.deinit();
            self.allocator.destroy(book_ptr.*);
        }
        self.books.clearRetainingCapacity();
        self.risk_engine.reset();
        self.binary_frame_assembler.reset();
        self.next_seq = 0;
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
    }

    pub fn addSymbol(
        self: *ExchangeKernel,
        spec: common.CoreSymbolSpecification,
    ) (risk.SymbolSpecificationProvider.AddSymbolError || std.mem.Allocator.Error)!bool {
        if (self.books.contains(spec.symbol_id)) {
            return false;
        }
        try self.risk_engine.addSymbol(spec);

        const book_ptr = try self.allocator.create(Book);
        errdefer self.allocator.destroy(book_ptr);

        switch (self.orderbook_kind) {
            .naive => {
                book_ptr.* = .{ .naive = try naive.NaiveOrderBook.init(self.allocator, spec) };
            },
            .direct => {
                book_ptr.* = .{ .direct = try direct.DirectOrderBook.init(self.allocator, spec) };
            },
        }
        errdefer book_ptr.deinit();

        try self.books.put(self.allocator, spec.symbol_id, book_ptr);
        return true;
    }

    fn nextSequence(self: *ExchangeKernel) error{SequenceOverflow}!u64 {
        if (self.next_seq == std.math.maxInt(u64)) {
            return error.SequenceOverflow;
        }
        self.next_seq += 1;
        return self.next_seq;
    }

    fn executeBinaryPayloadWords(
        self: *ExchangeKernel,
        words: []const i64,
    ) BinaryPayloadExecutionError!void {
        var decoded = try binary_payload.decodeFromWordsBigEndian(self.allocator, words);
        defer decoded.deinit();

        switch (decoded.payload) {
            .add_symbols => |symbols| {
                if (!self.report_queries_cfg.add_symbols_binary) {
                    return error.UnsupportedBinaryCommandType;
                }
                for (symbols) |symbol| {
                    _ = try self.addSymbol(symbol);
                }
            },
            .add_accounts => |users| {
                if (!self.report_queries_cfg.add_accounts_binary) {
                    return error.UnsupportedBinaryCommandType;
                }
                for (users) |user_batch| {
                    var add_user = makeAddUserCommand(user_batch.uid);
                    var add_user_response = try self.execute(&add_user);
                    defer add_user_response.deinit(self.allocator);

                    switch (add_user_response.result_code) {
                        .success => {},
                        .user_mgmt_user_already_exists => continue,
                        else => return error.BinaryCommandRejected,
                    }

                    for (user_batch.accounts, 0..) |account, account_idx| {
                        const tx_id = 1_000_000_000 + @as(i64, @intCast(account_idx + 1));
                        var adjustment = makeBalanceAdjustmentCommand(
                            user_batch.uid,
                            account.currency,
                            account.balance,
                            tx_id,
                            .adjustment,
                        );
                        var adjustment_response = try self.execute(&adjustment);
                        defer adjustment_response.deinit(self.allocator);
                        if (adjustment_response.result_code != .success) {
                            return error.BinaryCommandRejected;
                        }
                    }
                }
            },
        }
    }

    fn executeBinaryQueryWords(
        self: *ExchangeKernel,
        words: []const i64,
    ) BinaryQueryExecutionError!ReportResponse {
        const decoded = try binary_payload.decodeQueryFromWordsBigEndian(self.allocator, words);
        return switch (decoded) {
            .state_hash => try self.queryReport(.state_hash_detailed),
            .single_user => |uid| try self.queryReport(.{ .single_user = uid }),
            .total_currency_balance => try self.queryReport(.total_currency_balance),
        };
    }

    pub fn execute(
        self: *ExchangeKernel,
        command: *cmd.OrderCommand,
    ) ExecuteError!CommandResponse {
        const seq = try self.nextSequence();
        var event_chain = events.EventChain.init(self.allocator);
        defer event_chain.deinit();

        _ = try self.risk_engine.preProcessCommand(seq, command);

        var response: CommandResponse = .{
            .sequence = seq,
            .result_code = command.result_code,
        };
        errdefer response.deinit(self.allocator);
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
                    response.result_code = command.result_code;
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
                            self.executeBinaryPayloadWords(owned_complete.words) catch |err| switch (err) {
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
                            const maybe_report: ?ReportResponse = self.executeBinaryQueryWords(owned_complete.words) catch |err| switch (err) {
                                error.OutOfMemory => return error.OutOfMemory,
                                error.UnsupportedReportQueryType => blk: {
                                    command.result_code = .report_query_unknown_type;
                                    break :blk null;
                                },
                                else => blk: {
                                    command.result_code = .binary_command_failed;
                                    break :blk null;
                                },
                            };

                            if (maybe_report) |report| {
                                response.report_response = report;
                                command.result_code = .success;
                            }
                        },
                        else => {
                            command.result_code = .binary_command_failed;
                        },
                    }
                },
            }
        }

        if (isMatchingCommand(command.command)) {
            const book_ptr = self.books.get(command.symbol) orelse {
                command.result_code = .matching_invalid_order_book_id;
                response.result_code = command.result_code;
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

        _ = try self.risk_engine.handlerRiskRelease(seq, command, event_chain.head, top);
        response.matcher_events_head = event_chain.takeOwnedHead();
        response.result_code = command.result_code;
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
    ) std.mem.Allocator.Error!void {
        const gop = try map.getOrPut(allocator, key);
        if (!gop.found_existing) {
            gop.value_ptr.* = 0;
        }
        gop.value_ptr.* += delta;
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

        var book_it = self.books.valueIterator();
        while (book_it.next()) |book_ptr| {
            try book_ptr.*.accumulateOrderHolds(self.allocator, &report.orders_balances);
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
        const request_id = try self.nextRequestId();
        try self.pending_queue.append(self.kernel.allocator, .{
            .request_id = request_id,
            .command = command.*,
        });
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
        return .{
            .matching_request_id = try self.submitCommandAsync(&matching),
            .risk_request_id = try self.submitCommandAsync(&risk_cmd),
        };
    }

    pub fn drainPending(self: *ExchangeApi) ExecuteError!usize {
        var processed: usize = 0;
        defer self.dropProcessedPending(processed);

        while (processed < self.pending_queue.items.len) {
            const entry = self.pending_queue.items[processed];
            var command = entry.command;
            var response = try self.kernel.execute(&command);
            errdefer response.deinit(self.kernel.allocator);
            try self.ready_results.put(self.kernel.allocator, entry.request_id, response);
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

    fn nextRequestId(self: *ExchangeApi) error{RequestIdOverflow}!u64 {
        if (self.next_request_id == std.math.maxInt(u64)) {
            return error.RequestIdOverflow;
        }
        self.next_request_id += 1;
        return self.next_request_id;
    }

    fn dropProcessedPending(self: *ExchangeApi, processed: usize) void {
        if (processed == 0) return;
        if (processed >= self.pending_queue.items.len) {
            self.pending_queue.clearRetainingCapacity();
            return;
        }

        const remaining = self.pending_queue.items.len - processed;
        std.mem.copyForwards(
            PendingAsyncCommand,
            self.pending_queue.items[0..remaining],
            self.pending_queue.items[processed..],
        );
        self.pending_queue.items.len = remaining;
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

pub const MultiShardKernelSet = struct {
    allocator: std.mem.Allocator,
    kernels: []ExchangeKernel,
    next_seq: u64 = 0,

    pub const ShardedInitError = std.mem.Allocator.Error || InitError || error{
        InvalidShardCount,
    };
    pub const AggregateError = ReportError || error{
        ArithmeticOverflow,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        shard_count: usize,
        orderbook_kind: OrderBookKind,
    ) ShardedInitError!MultiShardKernelSet {
        const shard_count_u64 = std.math.cast(u64, shard_count) orelse return error.InvalidShardCount;
        if (shard_count == 0 or @popCount(shard_count_u64) != 1) {
            return error.InvalidShardCount;
        }

        const kernels = try allocator.alloc(ExchangeKernel, shard_count);
        errdefer allocator.free(kernels);

        var initialized: usize = 0;
        errdefer {
            for (kernels[0..initialized]) |*kernel| {
                kernel.deinit();
            }
        }

        for (0..shard_count) |idx| {
            const shard_id = std.math.cast(u64, idx) orelse return error.InvalidShardCount;
            kernels[idx] = try ExchangeKernel.init(allocator, .{
                .orderbook_kind = orderbook_kind,
                .shard_id = shard_id,
                .num_shards = shard_count_u64,
            });
            initialized += 1;
        }

        return .{
            .allocator = allocator,
            .kernels = kernels,
        };
    }

    pub fn deinit(self: *MultiShardKernelSet) void {
        for (self.kernels) |*kernel| {
            kernel.deinit();
        }
        self.allocator.free(self.kernels);
        self.* = undefined;
    }

    pub fn addSymbolAll(self: *MultiShardKernelSet, spec: common.CoreSymbolSpecification) risk.SymbolSpecificationProvider.AddSymbolError!void {
        for (self.kernels) |*kernel| {
            _ = try kernel.addSymbol(spec);
        }
    }

    pub fn shardIndexForUid(self: *const MultiShardKernelSet, uid: i64) usize {
        std.debug.assert(self.kernels.len > 0);
        if (self.kernels.len == 1) return 0;
        const mask: u64 = @as(u64, @intCast(self.kernels.len - 1));
        const uid_bits: u64 = @bitCast(uid);
        return @as(usize, @intCast(uid_bits & mask));
    }

    pub fn shardIndexForSymbol(self: *const MultiShardKernelSet, symbol: i32) usize {
        std.debug.assert(self.kernels.len > 0);
        if (self.kernels.len == 1) return 0;
        const mask: u64 = @as(u64, @intCast(self.kernels.len - 1));
        const symbol_bits: u64 = @as(u32, @bitCast(symbol));
        return @as(usize, @intCast(symbol_bits & mask));
    }

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
        const idx = self.shardIndexForCommand(command);
        return self.kernels[idx].execute(command);
    }

    pub fn queryUserBalance(
        self: *MultiShardKernelSet,
        uid: i64,
        currency: i32,
    ) ReportError!?i64 {
        const idx = self.shardIndexForUid(uid);
        var report = try self.kernels[idx].queryReport(.{
            .user_balance = .{
                .uid = uid,
                .currency = currency,
            },
        });
        defer report.deinit(self.allocator);
        return report.user_balance;
    }

    pub fn aggregateTotalCurrencyBalance(self: *MultiShardKernelSet) AggregateError!TotalCurrencyBalanceReport {
        var total: TotalCurrencyBalanceReport = .{};
        errdefer total.deinit(self.allocator);

        for (self.kernels) |*kernel| {
            var report = try kernel.queryReport(.total_currency_balance);
            defer report.deinit(self.allocator);
            switch (report) {
                .total_currency_balance => |*partial| {
                    try mergeI32I64MapChecked(self.allocator, &total.account_balances, &partial.account_balances);
                    try mergeI32I64MapChecked(self.allocator, &total.orders_balances, &partial.orders_balances);
                    try mergeI32I64MapChecked(self.allocator, &total.fees, &partial.fees);
                    try mergeI32I64MapChecked(self.allocator, &total.adjustments, &partial.adjustments);
                    try mergeI32I64MapChecked(self.allocator, &total.suspends, &partial.suspends);
                    try mergeI32I64MapChecked(self.allocator, &total.open_interest_long, &partial.open_interest_long);
                    try mergeI32I64MapChecked(self.allocator, &total.open_interest_short, &partial.open_interest_short);
                },
                else => return error.StateHashCorrupted,
            }
        }
        return total;
    }

    pub fn detailedHashFingerprint(self: *MultiShardKernelSet) ReportError!u64 {
        var hasher = std.hash.Wyhash.init(0);
        hashU64Test(&hasher, @as(u64, @intCast(self.kernels.len)));
        for (self.kernels) |*kernel| {
            var report = try kernel.queryReport(.state_hash_detailed);
            defer report.deinit(self.allocator);
            switch (report) {
                .state_hash_detailed => |detailed| {
                    const module_id = std.math.cast(i32, kernel.risk_engine.shard_id) orelse
                        return error.ArithmeticOverflow;
                    hashI32Test(&hasher, module_id);
                    hashU64Test(&hasher, detailed.aggregate_hash);
                },
                else => return error.StateHashCorrupted,
            }
        }
        return hasher.final();
    }

    fn nextSequence(self: *MultiShardKernelSet) error{SequenceOverflow}!u64 {
        if (self.next_seq == std.math.maxInt(u64)) {
            return error.SequenceOverflow;
        }
        self.next_seq += 1;
        return self.next_seq;
    }

    pub fn executeSplit(self: *MultiShardKernelSet, command: *cmd.OrderCommand) ExecuteError!CommandResponse {
        const seq = try self.nextSequence();
        var event_chain = events.EventChain.init(self.allocator);
        defer event_chain.deinit();

        var response: CommandResponse = .{
            .sequence = seq,
            .result_code = command.result_code,
        };
        errdefer response.deinit(self.allocator);

        var pipeline_command = command.*;
        const risk_owner_idx = self.shardIndexForUid(pipeline_command.uid);
        {
            var owner_pre = pipeline_command;
            _ = try self.kernels[risk_owner_idx].risk_engine.preProcessCommand(seq, &owner_pre);
            pipeline_command.result_code = owner_pre.result_code;
            pipeline_command.action = owner_pre.action;
        }
        for (0..self.kernels.len) |idx| {
            if (idx == risk_owner_idx) continue;
            var pre = pipeline_command;
            _ = try self.kernels[idx].risk_engine.preProcessCommand(seq, &pre);
        }

        applyControlCommandResult(&pipeline_command);

        const matching_idx = self.shardIndexForSymbol(pipeline_command.symbol);
        if (switch (pipeline_command.command) {
            .move_order,
            .cancel_order,
            .reduce_order,
            .place_order,
            .order_book_request,
            => true,
            else => false,
        }) {
            const matching_kernel = &self.kernels[matching_idx];
            const book_ptr = matching_kernel.books.get(pipeline_command.symbol) orelse {
                pipeline_command.result_code = .matching_invalid_order_book_id;
                command.result_code = pipeline_command.result_code;
                response.result_code = pipeline_command.result_code;
                return response;
            };

            if (pipeline_command.command == .order_book_request) {
                const depth = if (pipeline_command.size < 0)
                    std.math.maxInt(usize)
                else
                    @as(usize, @intCast(pipeline_command.size));
                response.l2_snapshot = try book_ptr.l2Snapshot(self.allocator, depth);
                pipeline_command.result_code = .success;
            } else {
                pipeline_command.result_code = try book_ptr.processCommand(&pipeline_command, &event_chain);
            }
        }

        const top: ?events.L2Top = if (response.l2_snapshot) |*snapshot|
            .{
                .ask_size = snapshot.asks.items.len,
                .bid_size = snapshot.bids.items.len,
                .ask_price_0 = if (snapshot.asks.items.len == 0) std.math.maxInt(i64) else snapshot.asks.items[0].price,
                .bid_price_0 = if (snapshot.bids.items.len == 0) 0 else snapshot.bids.items[0].price,
            }
        else
            null;

        for (self.kernels) |*kernel| {
            var release = pipeline_command;
            _ = try kernel.risk_engine.handlerRiskRelease(
                seq,
                &release,
                event_chain.head,
                top,
            );
        }

        command.result_code = pipeline_command.result_code;
        command.action = pipeline_command.action;
        response.result_code = pipeline_command.result_code;
        return response;
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
    var kernel = try ExchangeKernel.init(allocator, .{});
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
    var expected_report = try kernel.queryReport(.state_hash_detailed);
    defer expected_report.deinit(allocator);
    switch (final_resp.report_response orelse unreachable) {
        .state_hash_detailed => |detailed| {
            try std.testing.expectEqual(expected_report.state_hash_detailed.aggregate_hash, detailed.aggregate_hash);
            try std.testing.expectEqual(@as(usize, 11), detailed.submodules.items.len);
        },
        else => unreachable,
    }
    try std.testing.expectEqual(@as(usize, 0), kernel.binary_frame_assembler.pendingTransfers());
}

test "kernel binary query unknown report type returns report_query_unknown_type" {
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
            .report_query_unknown_type
        else
            .accepted;
        try std.testing.expectEqual(expected, response.result_code);
        if (idx + 1 == query_frames.items.len) {
            try std.testing.expect(response.report_response == null);
        }
    }

    try std.testing.expectEqual(@as(usize, 0), kernel.binary_frame_assembler.pendingTransfers());
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
            .report_query_unknown_type
        else
            .accepted;
        try std.testing.expectEqual(expected, response.result_code);
    }
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
            switch (response.report_response orelse unreachable) {
                .total_currency_balance => |report| {
                    try std.testing.expectEqual(@as(i64, 2000), report.account_balances.get(15) orelse 0);
                    try std.testing.expectEqual(@as(i64, -2000), report.adjustments.get(15) orelse 0);
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
            switch (response.report_response orelse unreachable) {
                .single_user => |report| {
                    try std.testing.expectEqual(@as(i64, 7602), report.uid);
                    try std.testing.expectEqual(SingleUserQueryExecutionStatus.ok, report.query_execution_status);
                    try std.testing.expectEqual(@as(usize, 1), report.orders.items.len);
                    try std.testing.expectEqual(@as(i64, 9951), report.orders.items[0].order_id);
                },
                else => unreachable,
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 0), kernel.binary_frame_assembler.pendingTransfers());
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
    try std.testing.expectEqual(@as(usize, 0), kernel.binary_frame_assembler.pendingTransfers());
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
    var kernel = try ExchangeKernel.init(allocator, .{});
    defer kernel.deinit();

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
    long_position.open_price_sum = 700;
    try profile_1.positions.put(allocator, 241, long_position);

    const profile_2 = kernel.risk_engine.getUserProfile(7202) orelse unreachable;
    var short_position = user.SymbolPositionRecord.init(7202, 241, 15);
    short_position.direction = .short;
    short_position.open_volume = 4;
    short_position.open_price_sum = 400;
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
