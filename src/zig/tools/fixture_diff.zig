const std = @import("std");
const core = @import("exchange_core").core;

const common = core.common;
const cmd = core.cmd;
const kernel_mod = core.exchange_kernel;
const journal_mod = core.journal;
const naive = core.orderbook_naive;
const simple_events = core.simple_events_processor;
const snapshot_mod = core.snapshot;

const MAKER_UID: i64 = 7001;
const TAKER_UID: i64 = 7002;
const MISSING_UID: i64 = 9999;
const BASE_CCY: i32 = 11;
const QUOTE_CCY: i32 = 15;
const EXCHANGE_SYMBOL: i32 = 241;
const BASIC_EXECUTED_BASE: i64 = 5;
const BASIC_EXECUTED_QUOTE: i64 = 500;
const EXTENDED_EXECUTED_BASE: i64 = 6;
const EXTENDED_EXECUTED_QUOTE: i64 = 612;
const MULTISYMBOL_EXECUTED_BASE: i64 = 7;
const MULTISYMBOL_EXECUTED_QUOTE: i64 = 884;

const FixtureScenario = enum {
    user_balance_basic,
    exchange_control,
    exchange_trade_basic,
    exchange_trade_extended,
    exchange_trade_bootstrap,
    exchange_trade_bootstrap_long,
    exchange_trade_multisymbol,
    exchange_trade_reports,
    exchange_trade_reports_long,
};

const FixtureOrderBookMode = enum {
    na,
    naive,
    direct,
};

const StateHashKind = enum {
    java_report,
    zig_risk_low32,
    zig_detailed_low32,
};

const SettlementMode = enum {
    full,
    pre_release,
};

const Level = struct {
    index: usize,
    price: i64,
    volume: i64,
    orders: u32,
};

const IndexedTextLine = struct {
    index: usize,
    value: []const u8,
};

const Fixture = struct {
    version: u32,
    scenario: FixtureScenario,
    orderbook_mode: FixtureOrderBookMode,
    symbol: i32,
    command_results: std.ArrayListUnmanaged(cmd.CommandResultCode) = .empty,
    has_simple_events: bool = false,
    simple_events_size: usize = 0,
    simple_events: std.ArrayListUnmanaged(IndexedTextLine) = .empty,
    state_hash_kind: StateHashKind = .java_report,
    settlement_mode: SettlementMode = .full,
    state_hash: i64,
    fee_15: i64,
    maker_balance_15: i64,
    maker_balance_11: i64,
    taker_balance_15: i64,
    taker_balance_11: i64,
    report_maker_status: i64 = 0,
    report_missing_status: i64 = 0,
    report_maker_orders: i64 = 0,
    report_maker_balance_15: i64 = 0,
    report_maker_balance_11: i64 = 0,
    report_taker_status: i64 = 0,
    report_taker_orders: i64 = 0,
    report_taker_balance_15: i64 = 0,
    report_taker_balance_11: i64 = 0,
    report_total_accounts_15: i64 = 0,
    report_total_accounts_11: i64 = 0,
    report_total_orders_15: i64 = 0,
    report_total_orders_11: i64 = 0,
    report_total_fees_15: i64 = 0,
    report_total_fees_11: i64 = 0,
    orderbook_ask_size: usize,
    orderbook_bid_size: usize,
    asks: std.ArrayListUnmanaged(Level) = .empty,
    bids: std.ArrayListUnmanaged(Level) = .empty,

    fn deinit(self: *Fixture, allocator: std.mem.Allocator) void {
        self.command_results.deinit(allocator);
        self.simple_events.deinit(allocator);
        self.asks.deinit(allocator);
        self.bids.deinit(allocator);
        self.* = undefined;
    }
};

const ScenarioResult = struct {
    command_results: std.ArrayListUnmanaged(cmd.CommandResultCode) = .empty,
    simple_events: std.ArrayListUnmanaged([]u8) = .empty,
    state_hash_u64: u64,
    state_hash_java_i32: i64,
    state_hash_detailed_u64: u64,
    state_hash_detailed_java_i32: i64,
    fee_15: i64,
    maker_balance_15: i64,
    maker_balance_11: i64,
    taker_balance_15: i64,
    taker_balance_11: i64,
    report_maker_status: i64 = 0,
    report_missing_status: i64 = 0,
    report_maker_orders: i64 = 0,
    report_maker_balance_15: i64 = 0,
    report_maker_balance_11: i64 = 0,
    report_taker_status: i64 = 0,
    report_taker_orders: i64 = 0,
    report_taker_balance_15: i64 = 0,
    report_taker_balance_11: i64 = 0,
    report_total_accounts_15: i64 = 0,
    report_total_accounts_11: i64 = 0,
    report_total_orders_15: i64 = 0,
    report_total_orders_11: i64 = 0,
    report_total_fees_15: i64 = 0,
    report_total_fees_11: i64 = 0,
    asks: std.ArrayListUnmanaged(Level) = .empty,
    bids: std.ArrayListUnmanaged(Level) = .empty,

    fn deinit(self: *ScenarioResult, allocator: std.mem.Allocator) void {
        self.command_results.deinit(allocator);
        for (self.simple_events.items) |line| allocator.free(line);
        self.simple_events.deinit(allocator);
        self.asks.deinit(allocator);
        self.bids.deinit(allocator);
        self.* = undefined;
    }
};

const DiffError = std.mem.Allocator.Error ||
    simple_events.ProcessError ||
    kernel_mod.InitError ||
    kernel_mod.ExecuteError ||
    kernel_mod.ReportError ||
    journal_mod.ReplayError ||
    snapshot_mod.SnapshotCaptureError ||
    snapshot_mod.SnapshotRestoreError ||
    common.CoreSymbolSpecification.ValidationError ||
    core.risk.SymbolSpecificationProvider.AddSymbolError ||
    error{
        InvalidArgs,
        NoFixtureInputs,
        InvalidFixtureLine,
        MissingRequiredField,
        InvalidFieldValue,
        UnknownScenario,
        UnknownOrderBookMode,
        UnknownCommandResult,
        UnsupportedScenario,
        UnexpectedResult,
        ComparisonFailed,
    };

fn boolAsInt(value: bool) u8 {
    return if (value) 1 else 0;
}

fn toSimpleEventCommandName(command: simple_events.ApiCommand, out: *[64]u8) []const u8 {
    const tag = @tagName(command);
    std.debug.assert(tag.len <= out.len);
    std.mem.copyForwards(u8, out[0..tag.len], tag);
    return out[0..tag.len];
}

fn toJavaOrderActionName(action: common.OrderAction, out: *[16]u8) []const u8 {
    const tag = @tagName(action);
    std.debug.assert(tag.len <= out.len);
    for (tag, 0..) |ch, idx| {
        out[idx] = std.ascii.toUpper(ch);
    }
    return out[0..tag.len];
}

const SimpleEventsCollector = struct {
    allocator: std.mem.Allocator,
    lines: std.ArrayListUnmanaged([]u8) = .empty,

    fn init(allocator: std.mem.Allocator) SimpleEventsCollector {
        return .{ .allocator = allocator };
    }

    fn deinit(self: *SimpleEventsCollector) void {
        for (self.lines.items) |line| self.allocator.free(line);
        self.lines.deinit(self.allocator);
        self.* = undefined;
    }

    fn handler(self: *SimpleEventsCollector) simple_events.EventsHandler {
        return .{
            .context = self,
            .on_command_result = onCommandResult,
            .on_trade_event = onTradeEvent,
            .on_reject_event = onRejectEvent,
            .on_reduce_event = onReduceEvent,
            .on_order_book = onOrderBook,
        };
    }

    fn appendOwned(self: *SimpleEventsCollector, line: []const u8) std.mem.Allocator.Error!void {
        const owned = try self.allocator.dupe(u8, line);
        errdefer self.allocator.free(owned);
        try self.lines.append(self.allocator, owned);
    }

    fn onCommandResult(ctx: ?*anyopaque, event: simple_events.ApiCommandResult) void {
        const self: *SimpleEventsCollector = @ptrCast(@alignCast(ctx orelse return));
        var command_buf: [64]u8 = undefined;
        var result_buf: [64]u8 = undefined;
        const line = std.fmt.allocPrint(self.allocator, "command_result:{s}:{s}", .{
            toSimpleEventCommandName(event.command, &command_buf),
            toJavaResultName(event.result_code, &result_buf),
        }) catch @panic("simple-events oom");
        self.lines.append(self.allocator, line) catch @panic("simple-events oom");
    }

    fn onTradeEvent(ctx: ?*anyopaque, event: simple_events.TradeEvent) void {
        const self: *SimpleEventsCollector = @ptrCast(@alignCast(ctx orelse return));
        var line: std.Io.Writer.Allocating = .init(self.allocator);
        defer line.deinit();

        var action_buf: [16]u8 = undefined;
        const writer = &line.writer;
        writer.print("trade:{d}:{d}:{d}:{d}:{s}:{d}:{d}:", .{
            event.symbol,
            event.total_volume,
            event.taker_order_id,
            event.taker_uid,
            toJavaOrderActionName(event.taker_action, &action_buf),
            boolAsInt(event.taker_order_completed),
            event.timestamp,
        }) catch @panic("simple-events oom");

        for (event.trades, 0..) |trade, idx| {
            if (idx != 0) {
                writer.writeByte(';') catch @panic("simple-events oom");
            }
            writer.print("{d}@{d}@{d}@{d}@{d}", .{
                trade.maker_order_id,
                trade.maker_uid,
                boolAsInt(trade.maker_order_completed),
                trade.price,
                trade.volume,
            }) catch @panic("simple-events oom");
        }

        self.appendOwned(line.written()) catch @panic("simple-events oom");
    }

    fn onRejectEvent(ctx: ?*anyopaque, event: simple_events.RejectEvent) void {
        const self: *SimpleEventsCollector = @ptrCast(@alignCast(ctx orelse return));
        const line = std.fmt.allocPrint(self.allocator, "reject:{d}:{d}:{d}:{d}:{d}:{d}", .{
            event.symbol,
            event.rejected_volume,
            event.price,
            event.order_id,
            event.uid,
            event.timestamp,
        }) catch @panic("simple-events oom");
        self.lines.append(self.allocator, line) catch @panic("simple-events oom");
    }

    fn onReduceEvent(ctx: ?*anyopaque, event: simple_events.ReduceEvent) void {
        const self: *SimpleEventsCollector = @ptrCast(@alignCast(ctx orelse return));
        const line = std.fmt.allocPrint(self.allocator, "reduce:{d}:{d}:{d}:{d}:{d}:{d}:{d}", .{
            event.symbol,
            event.reduced_volume,
            boolAsInt(event.order_completed),
            event.price,
            event.order_id,
            event.uid,
            event.timestamp,
        }) catch @panic("simple-events oom");
        self.lines.append(self.allocator, line) catch @panic("simple-events oom");
    }

    fn onOrderBook(ctx: ?*anyopaque, event: simple_events.OrderBook) void {
        const self: *SimpleEventsCollector = @ptrCast(@alignCast(ctx orelse return));
        var line: std.Io.Writer.Allocating = .init(self.allocator);
        defer line.deinit();

        const writer = &line.writer;
        writer.print("order_book:{d}:{d}:", .{ event.symbol, event.timestamp }) catch @panic("simple-events oom");

        for (event.asks, 0..) |level, idx| {
            if (idx != 0) {
                writer.writeByte(';') catch @panic("simple-events oom");
            }
            writer.print("{d}@{d}@{d}", .{ level.price, level.volume, level.orders }) catch @panic("simple-events oom");
        }

        writer.writeByte(':') catch @panic("simple-events oom");

        for (event.bids, 0..) |level, idx| {
            if (idx != 0) {
                writer.writeByte(';') catch @panic("simple-events oom");
            }
            writer.print("{d}@{d}@{d}", .{ level.price, level.volume, level.orders }) catch @panic("simple-events oom");
        }

        self.appendOwned(line.written()) catch @panic("simple-events oom");
    }
};

const SimpleEventsRuntime = struct {
    collector: SimpleEventsCollector,
    processor: simple_events.SimpleEventsProcessor,

    fn init(self: *SimpleEventsRuntime, allocator: std.mem.Allocator) void {
        self.collector = SimpleEventsCollector.init(allocator);
        self.processor = simple_events.SimpleEventsProcessor.init(allocator, self.collector.handler());
    }

    fn deinit(self: *SimpleEventsRuntime) void {
        self.processor.deinit();
        self.collector.deinit();
        self.* = undefined;
    }

    fn copyLinesToResult(
        self: *SimpleEventsRuntime,
        allocator: std.mem.Allocator,
        result: *ScenarioResult,
    ) std.mem.Allocator.Error!void {
        try result.simple_events.ensureTotalCapacityPrecise(allocator, self.collector.lines.items.len);
        for (self.collector.lines.items) |line| {
            const owned = try allocator.dupe(u8, line);
            errdefer allocator.free(owned);
            result.simple_events.appendAssumeCapacity(owned);
        }
    }
};

pub fn main(init: std.process.Init) DiffError!void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var fixture_paths = try collectFixturePaths(init.minimal.args, init.io, allocator);
    defer deinitOwnedPaths(allocator, &fixture_paths);

    if (fixture_paths.items.len == 0) return error.NoFixtureInputs;

    var total_mismatches: usize = 0;
    for (fixture_paths.items) |path| {
        total_mismatches += try runFixtureDiff(init.io, allocator, path);
    }

    if (total_mismatches != 0) {
        return error.ComparisonFailed;
    }
}

fn collectFixturePaths(
    process_args: std.process.Args,
    io: std.Io,
    allocator: std.mem.Allocator,
) DiffError!std.ArrayListUnmanaged([]u8) {
    var paths: std.ArrayListUnmanaged([]u8) = .empty;
    errdefer deinitOwnedPaths(allocator, &paths);

    var args = try std.process.Args.Iterator.initAllocator(process_args, allocator);
    defer args.deinit();

    _ = args.next();
    while (args.next()) |arg| {
        try appendOwnedPath(allocator, &paths, arg);
    }

    if (paths.items.len != 0) return paths;

    const defaults = [_][]const u8{
        "src/zig/fixtures/java_user_balance_basic.fixture",
        "src/zig/fixtures/java_user_balance_basic_low32.fixture",
        "src/zig/fixtures/java_user_balance_basic_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_control.fixture",
        "src/zig/fixtures/java_exchange_control_low32.fixture",
        "src/zig/fixtures/java_exchange_control_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_basic.fixture",
        "src/zig/fixtures/java_exchange_trade_basic_full.fixture",
        "src/zig/fixtures/java_exchange_trade_basic_direct.fixture",
        "src/zig/fixtures/java_exchange_trade_basic_direct_full.fixture",
        "src/zig/fixtures/java_exchange_trade_basic_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_basic_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_basic_direct_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_basic_direct_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_basic_full_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_basic_full_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_basic_direct_full_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_basic_direct_full_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_extended.fixture",
        "src/zig/fixtures/java_exchange_trade_extended_direct.fixture",
        "src/zig/fixtures/java_exchange_trade_bootstrap.fixture",
        "src/zig/fixtures/java_exchange_trade_bootstrap_direct.fixture",
        "src/zig/fixtures/java_exchange_trade_bootstrap_full.fixture",
        "src/zig/fixtures/java_exchange_trade_bootstrap_direct_full.fixture",
        "src/zig/fixtures/java_exchange_trade_bootstrap_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_bootstrap_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_bootstrap_direct_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_bootstrap_direct_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_bootstrap_full_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_bootstrap_full_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_bootstrap_direct_full_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_bootstrap_direct_full_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_bootstrap_long.fixture",
        "src/zig/fixtures/java_exchange_trade_bootstrap_long_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_bootstrap_long_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_bootstrap_long_direct.fixture",
        "src/zig/fixtures/java_exchange_trade_bootstrap_long_direct_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_bootstrap_long_direct_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_bootstrap_long_full.fixture",
        "src/zig/fixtures/java_exchange_trade_bootstrap_long_full_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_bootstrap_long_full_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_bootstrap_long_direct_full.fixture",
        "src/zig/fixtures/java_exchange_trade_bootstrap_long_direct_full_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_bootstrap_long_direct_full_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_extended_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_extended_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_extended_full.fixture",
        "src/zig/fixtures/java_exchange_trade_extended_direct_full.fixture",
        "src/zig/fixtures/java_exchange_trade_extended_full_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_extended_full_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_extended_direct_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_extended_direct_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_extended_direct_full_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_extended_direct_full_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_multisymbol_241.fixture",
        "src/zig/fixtures/java_exchange_trade_multisymbol_242.fixture",
        "src/zig/fixtures/java_exchange_trade_multisymbol_full_241.fixture",
        "src/zig/fixtures/java_exchange_trade_multisymbol_full_242.fixture",
        "src/zig/fixtures/java_exchange_trade_multisymbol_direct_241.fixture",
        "src/zig/fixtures/java_exchange_trade_multisymbol_direct_242.fixture",
        "src/zig/fixtures/java_exchange_trade_multisymbol_direct_full_241.fixture",
        "src/zig/fixtures/java_exchange_trade_multisymbol_direct_full_242.fixture",
        "src/zig/fixtures/java_exchange_trade_multisymbol_241_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_multisymbol_241_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_multisymbol_242_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_multisymbol_242_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_multisymbol_direct_241_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_multisymbol_direct_241_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_multisymbol_direct_242_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_multisymbol_direct_242_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_multisymbol_full_241_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_multisymbol_full_241_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_multisymbol_full_242_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_multisymbol_full_242_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_multisymbol_direct_full_241_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_multisymbol_direct_full_241_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_multisymbol_direct_full_242_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_multisymbol_direct_full_242_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_reports.fixture",
        "src/zig/fixtures/java_exchange_trade_reports_direct.fixture",
        "src/zig/fixtures/java_exchange_trade_reports_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_reports_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_reports_direct_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_reports_direct_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_reports_long.fixture",
        "src/zig/fixtures/java_exchange_trade_reports_long_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_reports_long_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_reports_long_direct.fixture",
        "src/zig/fixtures/java_exchange_trade_reports_long_direct_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_reports_long_direct_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_reports_long_full.fixture",
        "src/zig/fixtures/java_exchange_trade_reports_long_full_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_reports_long_full_detailed_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_reports_long_direct_full.fixture",
        "src/zig/fixtures/java_exchange_trade_reports_long_direct_full_low32.fixture",
        "src/zig/fixtures/java_exchange_trade_reports_long_direct_full_detailed_low32.fixture",
    };
    for (defaults) |path| {
        if (fileExists(io, path)) {
            try appendOwnedPath(allocator, &paths, path);
        }
    }
    return paths;
}

fn appendOwnedPath(
    allocator: std.mem.Allocator,
    paths: *std.ArrayListUnmanaged([]u8),
    path: []const u8,
) DiffError!void {
    const owned = try allocator.dupe(u8, path);
    errdefer allocator.free(owned);
    try paths.append(allocator, owned);
}

fn deinitOwnedPaths(allocator: std.mem.Allocator, paths: *std.ArrayListUnmanaged([]u8)) void {
    for (paths.items) |path| allocator.free(path);
    paths.deinit(allocator);
    paths.* = .empty;
}

fn fileExists(io: std.Io, path: []const u8) bool {
    std.Io.Dir.cwd().access(io, path, .{}) catch return false;
    return true;
}

fn runFixtureDiff(io: std.Io, allocator: std.mem.Allocator, path: []const u8) DiffError!usize {
    const bytes = std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(1024 * 1024)) catch |err| {
        std.debug.print("  {s}: read error={}\n", .{ path, err });
        return error.InvalidArgs;
    };
    defer allocator.free(bytes);

    var fixture = try parseFixture(allocator, bytes);
    defer fixture.deinit(allocator);

    var actual = try runScenario(allocator, &fixture);
    defer actual.deinit(allocator);

    const mismatches = compareFixture(path, &fixture, &actual);
    if (mismatches == 0) {
        std.debug.print("[PASS] {s} scenario={s}\n", .{ path, @tagName(fixture.scenario) });
    } else {
        std.debug.print("[FAIL] {s} scenario={s} mismatches={d}\n", .{ path, @tagName(fixture.scenario), mismatches });
    }
    return mismatches;
}

fn parseFixture(allocator: std.mem.Allocator, bytes: []const u8) DiffError!Fixture {
    var fixture = Fixture{
        .version = 0,
        .scenario = undefined,
        .orderbook_mode = undefined,
        .symbol = 0,
        .state_hash = 0,
        .fee_15 = 0,
        .maker_balance_15 = 0,
        .maker_balance_11 = 0,
        .taker_balance_15 = 0,
        .taker_balance_11 = 0,
        .orderbook_ask_size = 0,
        .orderbook_bid_size = 0,
    };
    errdefer fixture.deinit(allocator);

    var seen = struct {
        version: bool = false,
        scenario: bool = false,
        orderbook_mode: bool = false,
        symbol: bool = false,
        command_results: bool = false,
        state_hash: bool = false,
        state_hash_kind: bool = false,
        settlement_mode: bool = false,
        fee_15: bool = false,
        maker_balance_15: bool = false,
        maker_balance_11: bool = false,
        taker_balance_15: bool = false,
        taker_balance_11: bool = false,
        report_maker_status: bool = false,
        report_missing_status: bool = false,
        report_maker_orders: bool = false,
        report_maker_balance_15: bool = false,
        report_maker_balance_11: bool = false,
        report_taker_status: bool = false,
        report_taker_orders: bool = false,
        report_taker_balance_15: bool = false,
        report_taker_balance_11: bool = false,
        report_total_accounts_15: bool = false,
        report_total_accounts_11: bool = false,
        report_total_orders_15: bool = false,
        report_total_orders_11: bool = false,
        report_total_fees_15: bool = false,
        report_total_fees_11: bool = false,
        simple_events_size: bool = false,
        orderbook_ask_size: bool = false,
        orderbook_bid_size: bool = false,
    }{};

    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, " \t\r");
        if (line.len == 0 or line[0] == '#') continue;

        const equal_index = std.mem.indexOfScalar(u8, line, '=') orelse return error.InvalidFixtureLine;
        const key = std.mem.trim(u8, line[0..equal_index], " \t");
        const value = std.mem.trim(u8, line[equal_index + 1 ..], " \t");
        if (key.len == 0) return error.InvalidFixtureLine;

        if (std.mem.eql(u8, key, "version")) {
            fixture.version = try parseInt(u32, value);
            seen.version = true;
        } else if (std.mem.eql(u8, key, "scenario")) {
            fixture.scenario = parseScenario(value) catch return error.UnknownScenario;
            seen.scenario = true;
        } else if (std.mem.eql(u8, key, "orderbook_mode")) {
            fixture.orderbook_mode = parseOrderBookMode(value) catch return error.UnknownOrderBookMode;
            seen.orderbook_mode = true;
        } else if (std.mem.eql(u8, key, "symbol")) {
            fixture.symbol = try parseInt(i32, value);
            seen.symbol = true;
        } else if (std.mem.eql(u8, key, "command_results")) {
            try parseCommandResults(allocator, value, &fixture.command_results);
            seen.command_results = true;
        } else if (std.mem.eql(u8, key, "simple_events_size")) {
            fixture.simple_events_size = try parseInt(usize, value);
            fixture.has_simple_events = true;
            seen.simple_events_size = true;
        } else if (std.mem.eql(u8, key, "state_hash_kind")) {
            fixture.state_hash_kind = parseStateHashKind(value) catch return error.InvalidFieldValue;
            seen.state_hash_kind = true;
        } else if (std.mem.eql(u8, key, "settlement_mode")) {
            fixture.settlement_mode = parseSettlementMode(value) catch return error.InvalidFieldValue;
            seen.settlement_mode = true;
        } else if (std.mem.eql(u8, key, "state_hash")) {
            fixture.state_hash = try parseInt(i64, value);
            seen.state_hash = true;
        } else if (std.mem.eql(u8, key, "fee_15")) {
            fixture.fee_15 = try parseInt(i64, value);
            seen.fee_15 = true;
        } else if (std.mem.eql(u8, key, "maker_balance_15")) {
            fixture.maker_balance_15 = try parseInt(i64, value);
            seen.maker_balance_15 = true;
        } else if (std.mem.eql(u8, key, "maker_balance_11")) {
            fixture.maker_balance_11 = try parseInt(i64, value);
            seen.maker_balance_11 = true;
        } else if (std.mem.eql(u8, key, "taker_balance_15")) {
            fixture.taker_balance_15 = try parseInt(i64, value);
            seen.taker_balance_15 = true;
        } else if (std.mem.eql(u8, key, "taker_balance_11")) {
            fixture.taker_balance_11 = try parseInt(i64, value);
            seen.taker_balance_11 = true;
        } else if (std.mem.eql(u8, key, "report_maker_status")) {
            fixture.report_maker_status = try parseInt(i64, value);
            seen.report_maker_status = true;
        } else if (std.mem.eql(u8, key, "report_missing_status")) {
            fixture.report_missing_status = try parseInt(i64, value);
            seen.report_missing_status = true;
        } else if (std.mem.eql(u8, key, "report_maker_orders")) {
            fixture.report_maker_orders = try parseInt(i64, value);
            seen.report_maker_orders = true;
        } else if (std.mem.eql(u8, key, "report_maker_balance_15")) {
            fixture.report_maker_balance_15 = try parseInt(i64, value);
            seen.report_maker_balance_15 = true;
        } else if (std.mem.eql(u8, key, "report_maker_balance_11")) {
            fixture.report_maker_balance_11 = try parseInt(i64, value);
            seen.report_maker_balance_11 = true;
        } else if (std.mem.eql(u8, key, "report_taker_status")) {
            fixture.report_taker_status = try parseInt(i64, value);
            seen.report_taker_status = true;
        } else if (std.mem.eql(u8, key, "report_taker_orders")) {
            fixture.report_taker_orders = try parseInt(i64, value);
            seen.report_taker_orders = true;
        } else if (std.mem.eql(u8, key, "report_taker_balance_15")) {
            fixture.report_taker_balance_15 = try parseInt(i64, value);
            seen.report_taker_balance_15 = true;
        } else if (std.mem.eql(u8, key, "report_taker_balance_11")) {
            fixture.report_taker_balance_11 = try parseInt(i64, value);
            seen.report_taker_balance_11 = true;
        } else if (std.mem.eql(u8, key, "report_total_accounts_15")) {
            fixture.report_total_accounts_15 = try parseInt(i64, value);
            seen.report_total_accounts_15 = true;
        } else if (std.mem.eql(u8, key, "report_total_accounts_11")) {
            fixture.report_total_accounts_11 = try parseInt(i64, value);
            seen.report_total_accounts_11 = true;
        } else if (std.mem.eql(u8, key, "report_total_orders_15")) {
            fixture.report_total_orders_15 = try parseInt(i64, value);
            seen.report_total_orders_15 = true;
        } else if (std.mem.eql(u8, key, "report_total_orders_11")) {
            fixture.report_total_orders_11 = try parseInt(i64, value);
            seen.report_total_orders_11 = true;
        } else if (std.mem.eql(u8, key, "report_total_fees_15")) {
            fixture.report_total_fees_15 = try parseInt(i64, value);
            seen.report_total_fees_15 = true;
        } else if (std.mem.eql(u8, key, "report_total_fees_11")) {
            fixture.report_total_fees_11 = try parseInt(i64, value);
            seen.report_total_fees_11 = true;
        } else if (std.mem.eql(u8, key, "orderbook_ask_size")) {
            fixture.orderbook_ask_size = try parseInt(usize, value);
            seen.orderbook_ask_size = true;
        } else if (std.mem.eql(u8, key, "orderbook_bid_size")) {
            fixture.orderbook_bid_size = try parseInt(usize, value);
            seen.orderbook_bid_size = true;
        } else if (std.mem.startsWith(u8, key, "ask_")) {
            const level = try parseLevelFromKeyValue(key, value, "ask_");
            try fixture.asks.append(allocator, level);
        } else if (std.mem.startsWith(u8, key, "bid_")) {
            const level = try parseLevelFromKeyValue(key, value, "bid_");
            try fixture.bids.append(allocator, level);
        } else if (std.mem.startsWith(u8, key, "simple_event_")) {
            const line_entry = try parseIndexedTextLineFromKeyValue(key, value, "simple_event_");
            try fixture.simple_events.append(allocator, line_entry);
        } else {
            return error.InvalidFixtureLine;
        }
    }

    if (!seen.version or
        !seen.scenario or
        !seen.orderbook_mode or
        !seen.symbol or
        !seen.command_results or
        !seen.state_hash or
        !seen.fee_15 or
        !seen.maker_balance_15 or
        !seen.maker_balance_11 or
        !seen.taker_balance_15 or
        !seen.taker_balance_11 or
        !seen.orderbook_ask_size or
        !seen.orderbook_bid_size)
    {
        return error.MissingRequiredField;
    }

    if (scenarioHasReportFields(fixture.scenario) and
        (!seen.report_maker_status or
            !seen.report_missing_status or
            !seen.report_maker_orders or
            !seen.report_maker_balance_15 or
            !seen.report_maker_balance_11 or
            !seen.report_taker_status or
            !seen.report_taker_orders or
            !seen.report_taker_balance_15 or
            !seen.report_taker_balance_11 or
            !seen.report_total_accounts_15 or
            !seen.report_total_accounts_11 or
            !seen.report_total_orders_15 or
            !seen.report_total_orders_11 or
            !seen.report_total_fees_15 or
            !seen.report_total_fees_11))
    {
        return error.MissingRequiredField;
    }

    std.sort.heap(Level, fixture.asks.items, {}, lessLevelByIndex);
    std.sort.heap(Level, fixture.bids.items, {}, lessLevelByIndex);
    std.sort.heap(IndexedTextLine, fixture.simple_events.items, {}, lessIndexedTextLineByIndex);

    if (fixture.asks.items.len != fixture.orderbook_ask_size) return error.InvalidFieldValue;
    if (fixture.bids.items.len != fixture.orderbook_bid_size) return error.InvalidFieldValue;
    if (fixture.simple_events.items.len != 0 and !seen.simple_events_size) return error.MissingRequiredField;
    if (seen.simple_events_size) {
        if (fixture.simple_events.items.len != fixture.simple_events_size) return error.InvalidFieldValue;
        try validateIndexedTextLineIndices(fixture.simple_events.items);
    }
    try validateLevelIndices(fixture.asks.items);
    try validateLevelIndices(fixture.bids.items);

    return fixture;
}

fn parseScenario(value: []const u8) error{UnknownScenario}!FixtureScenario {
    if (std.mem.eql(u8, value, "user_balance_basic")) return .user_balance_basic;
    if (std.mem.eql(u8, value, "exchange_control")) return .exchange_control;
    if (std.mem.eql(u8, value, "exchange_trade_basic")) return .exchange_trade_basic;
    if (std.mem.eql(u8, value, "exchange_trade_extended")) return .exchange_trade_extended;
    if (std.mem.eql(u8, value, "exchange_trade_bootstrap")) return .exchange_trade_bootstrap;
    if (std.mem.eql(u8, value, "exchange_trade_bootstrap_long")) return .exchange_trade_bootstrap_long;
    if (std.mem.eql(u8, value, "exchange_trade_multisymbol")) return .exchange_trade_multisymbol;
    if (std.mem.eql(u8, value, "exchange_trade_reports")) return .exchange_trade_reports;
    if (std.mem.eql(u8, value, "exchange_trade_reports_long")) return .exchange_trade_reports_long;
    return error.UnknownScenario;
}

fn parseOrderBookMode(value: []const u8) error{UnknownOrderBookMode}!FixtureOrderBookMode {
    if (std.mem.eql(u8, value, "na")) return .na;
    if (std.mem.eql(u8, value, "naive")) return .naive;
    if (std.mem.eql(u8, value, "direct")) return .direct;
    return error.UnknownOrderBookMode;
}

fn parseStateHashKind(value: []const u8) error{InvalidFieldValue}!StateHashKind {
    if (std.mem.eql(u8, value, "java_report")) return .java_report;
    if (std.mem.eql(u8, value, "zig_risk_low32")) return .zig_risk_low32;
    if (std.mem.eql(u8, value, "zig_detailed_low32")) return .zig_detailed_low32;
    return error.InvalidFieldValue;
}

fn parseSettlementMode(value: []const u8) error{InvalidFieldValue}!SettlementMode {
    if (std.mem.eql(u8, value, "full")) return .full;
    if (std.mem.eql(u8, value, "pre_release")) return .pre_release;
    return error.InvalidFieldValue;
}

fn parseCommandResults(
    allocator: std.mem.Allocator,
    value: []const u8,
    out: *std.ArrayListUnmanaged(cmd.CommandResultCode),
) DiffError!void {
    var tokens = std.mem.splitScalar(u8, value, ',');
    while (tokens.next()) |raw| {
        const token = std.mem.trim(u8, raw, " \t");
        if (token.len == 0) continue;
        try out.append(allocator, try parseCommandResultCode(token));
    }
}

fn parseCommandResultCode(value: []const u8) DiffError!cmd.CommandResultCode {
    inline for (std.meta.fields(cmd.CommandResultCode)) |field| {
        if (std.ascii.eqlIgnoreCase(value, field.name)) {
            return @field(cmd.CommandResultCode, field.name);
        }
    }
    return error.UnknownCommandResult;
}

fn parseLevelFromKeyValue(
    key: []const u8,
    value: []const u8,
    prefix: []const u8,
) DiffError!Level {
    const idx_text = key[prefix.len..];
    const index = try parseInt(usize, idx_text);

    var parts = std.mem.splitScalar(u8, value, ':');
    const price_text = parts.next() orelse return error.InvalidFieldValue;
    const volume_text = parts.next() orelse return error.InvalidFieldValue;
    const orders_text = parts.next() orelse return error.InvalidFieldValue;
    if (parts.next() != null) return error.InvalidFieldValue;

    const orders_i64 = try parseInt(i64, orders_text);
    if (orders_i64 < 0 or orders_i64 > std.math.maxInt(u32)) return error.InvalidFieldValue;

    return .{
        .index = index,
        .price = try parseInt(i64, price_text),
        .volume = try parseInt(i64, volume_text),
        .orders = @as(u32, @intCast(orders_i64)),
    };
}

fn parseIndexedTextLineFromKeyValue(
    key: []const u8,
    value: []const u8,
    prefix: []const u8,
) DiffError!IndexedTextLine {
    const idx_text = key[prefix.len..];
    const index = try parseInt(usize, idx_text);
    return .{
        .index = index,
        .value = value,
    };
}

fn validateLevelIndices(levels: []const Level) DiffError!void {
    for (levels, 0..) |level, idx| {
        if (level.index != idx) return error.InvalidFieldValue;
    }
}

fn validateIndexedTextLineIndices(lines: []const IndexedTextLine) DiffError!void {
    for (lines, 0..) |line, idx| {
        if (line.index != idx) return error.InvalidFieldValue;
    }
}

fn lessLevelByIndex(_: void, lhs: Level, rhs: Level) bool {
    return lhs.index < rhs.index;
}

fn lessIndexedTextLineByIndex(_: void, lhs: IndexedTextLine, rhs: IndexedTextLine) bool {
    return lhs.index < rhs.index;
}

fn parseInt(comptime T: type, raw: []const u8) DiffError!T {
    return std.fmt.parseInt(T, raw, 10) catch return error.InvalidFieldValue;
}

fn runScenario(allocator: std.mem.Allocator, fixture: *const Fixture) DiffError!ScenarioResult {
    if (fixture.scenario == .exchange_trade_bootstrap or fixture.scenario == .exchange_trade_bootstrap_long) {
        return runExchangeTradeBootstrapScenario(allocator, fixture);
    }

    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = switch (fixture.orderbook_mode) {
            .naive => .naive,
            .direct => .direct,
            .na => .direct,
        },
    });
    defer kernel.deinit();

    var result = ScenarioResult{
        .state_hash_u64 = 0,
        .state_hash_java_i32 = 0,
        .state_hash_detailed_u64 = 0,
        .state_hash_detailed_java_i32 = 0,
        .fee_15 = 0,
        .maker_balance_15 = 0,
        .maker_balance_11 = 0,
        .taker_balance_15 = 0,
        .taker_balance_11 = 0,
    };
    errdefer result.deinit(allocator);

    var simple_events_runtime_storage: SimpleEventsRuntime = undefined;
    var simple_events_runtime: ?*SimpleEventsRuntime = null;
    if (fixture.has_simple_events) {
        simple_events_runtime_storage.init(allocator);
        simple_events_runtime = &simple_events_runtime_storage;
    }
    defer if (simple_events_runtime) |runtime| runtime.deinit();

    switch (fixture.scenario) {
        .user_balance_basic => try runUserBalanceScenario(allocator, &kernel, &result),
        .exchange_control => try runExchangeControlScenario(allocator, &kernel, &result),
        .exchange_trade_basic => try runExchangeTradeScenario(allocator, &kernel, &result, simple_events_runtime),
        .exchange_trade_extended => try runExchangeTradeExtendedScenario(allocator, &kernel, &result, simple_events_runtime),
        .exchange_trade_bootstrap => return error.UnexpectedResult,
        .exchange_trade_bootstrap_long => return error.UnexpectedResult,
        .exchange_trade_multisymbol => try runExchangeTradeMultiSymbolScenario(allocator, &kernel, &result, simple_events_runtime),
        .exchange_trade_reports => try runExchangeTradeReportsScenario(allocator, &kernel, &result, simple_events_runtime),
        .exchange_trade_reports_long => try runExchangeTradeReportsLongScenario(allocator, &kernel, &result, simple_events_runtime),
    }

    if (simple_events_runtime) |runtime| {
        try runtime.copyLinesToResult(allocator, &result);
    }

    result.state_hash_u64 = try kernel.risk_engine.stateHash();
    result.state_hash_java_i32 = toJavaSignedIntFromLow32(result.state_hash_u64);
    var detailed_hash_report = try kernel.queryReport(.state_hash_detailed);
    defer detailed_hash_report.deinit(allocator);
    result.state_hash_detailed_u64 = switch (detailed_hash_report) {
        .state_hash_detailed => |detailed| detailed.aggregate_hash,
        else => return error.UnexpectedResult,
    };
    result.state_hash_detailed_java_i32 = toJavaSignedIntFromLow32(result.state_hash_detailed_u64);
    result.fee_15 = kernel.risk_engine.getFeeCollected(QUOTE_CCY);
    result.maker_balance_15 = accountBalance(&kernel, MAKER_UID, QUOTE_CCY);
    result.maker_balance_11 = accountBalance(&kernel, MAKER_UID, BASE_CCY);
    result.taker_balance_15 = accountBalance(&kernel, TAKER_UID, QUOTE_CCY);
    result.taker_balance_11 = accountBalance(&kernel, TAKER_UID, BASE_CCY);

    if (fixture.symbol != 0) {
        var report = try kernel.queryReport(.{
            .order_book = .{
                .symbol = fixture.symbol,
                .depth = std.math.maxInt(usize),
            },
        });
        defer report.deinit(allocator);

        switch (report) {
            .order_book => |*snapshot| {
                try appendLevelsFromSnapshot(allocator, snapshot, &result.asks, &result.bids);
            },
            else => return error.UnexpectedResult,
        }
    }

    return result;
}

fn runExchangeTradeBootstrapScenario(
    allocator: std.mem.Allocator,
    fixture: *const Fixture,
) DiffError!ScenarioResult {
    const orderbook_kind: kernel_mod.OrderBookKind = switch (fixture.orderbook_mode) {
        .naive => .naive,
        .direct => .direct,
        .na => .direct,
    };

    var source = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = orderbook_kind,
    });
    defer source.deinit();

    const spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = EXCHANGE_SYMBOL,
        .symbol_type = .currency_exchange_pair,
        .base_currency = BASE_CCY,
        .quote_currency = QUOTE_CCY,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
    });
    if (!(try source.addSymbol(spec))) return error.UnexpectedResult;

    var result = ScenarioResult{
        .state_hash_u64 = 0,
        .state_hash_java_i32 = 0,
        .state_hash_detailed_u64 = 0,
        .state_hash_detailed_java_i32 = 0,
        .fee_15 = 0,
        .maker_balance_15 = 0,
        .maker_balance_11 = 0,
        .taker_balance_15 = 0,
        .taker_balance_11 = 0,
    };
    errdefer result.deinit(allocator);

    var simple_events_runtime_storage: SimpleEventsRuntime = undefined;
    var simple_events_runtime: ?*SimpleEventsRuntime = null;
    if (fixture.has_simple_events) {
        simple_events_runtime_storage.init(allocator);
        simple_events_runtime = &simple_events_runtime_storage;
    }
    defer if (simple_events_runtime) |runtime| runtime.deinit();

    var journal = journal_mod.CommandJournal.init(allocator);
    defer journal.deinit();

    var commands: std.ArrayListUnmanaged(cmd.OrderCommand) = .empty;
    defer commands.deinit(allocator);

    switch (fixture.scenario) {
        .exchange_trade_bootstrap => {
            try commands.append(allocator, makeAddUserCommand(MAKER_UID));
            try commands.append(allocator, makeAddUserCommand(TAKER_UID));
            try commands.append(allocator, makeAddUserCommand(MAKER_UID));
            try commands.append(allocator, makeBalanceAdjustmentCommand(MAKER_UID, BASE_CCY, 150_000, 1));
            try commands.append(allocator, makeBalanceAdjustmentCommand(TAKER_UID, QUOTE_CCY, 200_000, 1));

            var maker_ask = cmd.OrderCommand.newOrder(.gtc, 9101, MAKER_UID, 101, 0, 10, .ask);
            maker_ask.symbol = EXCHANGE_SYMBOL;
            try commands.append(allocator, maker_ask);

            var maker_move = cmd.OrderCommand.update(9101, MAKER_UID, 102);
            maker_move.symbol = EXCHANGE_SYMBOL;
            try commands.append(allocator, maker_move);

            var taker_bid_rest = cmd.OrderCommand.newOrder(.gtc, 9201, TAKER_UID, 99, 99, 4, .bid);
            taker_bid_rest.symbol = EXCHANGE_SYMBOL;
            try commands.append(allocator, taker_bid_rest);

            var taker_bid_ioc = cmd.OrderCommand.newOrder(.ioc, 9202, TAKER_UID, 102, 102, 5, .bid);
            taker_bid_ioc.symbol = EXCHANGE_SYMBOL;
            try commands.append(allocator, taker_bid_ioc);

            var maker_ask_2 = cmd.OrderCommand.newOrder(.gtc, 9102, MAKER_UID, 103, 0, 4, .ask);
            maker_ask_2.symbol = EXCHANGE_SYMBOL;
            try commands.append(allocator, maker_ask_2);

            var taker_bid_cross = cmd.OrderCommand.newOrder(.gtc, 9203, TAKER_UID, 102, 102, 1, .bid);
            taker_bid_cross.symbol = EXCHANGE_SYMBOL;
            try commands.append(allocator, taker_bid_cross);

            var taker_move_cross = cmd.OrderCommand.update(9203, TAKER_UID, 104);
            taker_move_cross.symbol = EXCHANGE_SYMBOL;
            try commands.append(allocator, taker_move_cross);

            var maker_reduce_missing = cmd.OrderCommand.reduce(9999, MAKER_UID, 1);
            maker_reduce_missing.symbol = EXCHANGE_SYMBOL;
            try commands.append(allocator, maker_reduce_missing);

            var maker_cancel_missing = cmd.OrderCommand.cancel(9999, MAKER_UID);
            maker_cancel_missing.symbol = EXCHANGE_SYMBOL;
            try commands.append(allocator, maker_cancel_missing);
        },
        .exchange_trade_bootstrap_long => {
            try commands.append(allocator, makeAddUserCommand(MAKER_UID));
            try commands.append(allocator, makeAddUserCommand(TAKER_UID));
            try commands.append(allocator, makeAddUserCommand(MAKER_UID));
            try commands.append(allocator, makeBalanceAdjustmentCommand(MAKER_UID, BASE_CCY, 400_000, 1));
            try commands.append(allocator, makeBalanceAdjustmentCommand(TAKER_UID, QUOTE_CCY, 400_000, 1));

            var maker_anchor = cmd.OrderCommand.newOrder(.gtc, 9101, MAKER_UID, 200, 0, 20, .ask);
            maker_anchor.symbol = EXCHANGE_SYMBOL;
            try commands.append(allocator, maker_anchor);

            var taker_anchor = cmd.OrderCommand.newOrder(.gtc, 9201, TAKER_UID, 50, 50, 20, .bid);
            taker_anchor.symbol = EXCHANGE_SYMBOL;
            try commands.append(allocator, taker_anchor);

            const rounds: u32 = 18;
            var round: u32 = 0;
            while (round < rounds) : (round += 1) {
                const round_i64: i64 = @intCast(round);
                const ask_id: i64 = 9700 + round_i64;
                const ask_price: i64 = 220 + (2 * round_i64);
                const ask_size: i64 = 3 + @as(i64, @intCast(round % 3));
                const bid_id: i64 = 9800 + round_i64;
                const bid_price: i64 = 40 - @as(i64, @intCast(round % 5));
                const bid_size: i64 = 2 + @as(i64, @intCast(round % 2));

                var maker_ask = cmd.OrderCommand.newOrder(.gtc, ask_id, MAKER_UID, ask_price, 0, ask_size, .ask);
                maker_ask.symbol = EXCHANGE_SYMBOL;
                try commands.append(allocator, maker_ask);

                var taker_bid = cmd.OrderCommand.newOrder(.gtc, bid_id, TAKER_UID, bid_price, bid_price, bid_size, .bid);
                taker_bid.symbol = EXCHANGE_SYMBOL;
                try commands.append(allocator, taker_bid);
            }

            var maker_tail = cmd.OrderCommand.newOrder(.gtc, 9102, MAKER_UID, 300, 0, 10, .ask);
            maker_tail.symbol = EXCHANGE_SYMBOL;
            try commands.append(allocator, maker_tail);

            var taker_tail = cmd.OrderCommand.newOrder(.gtc, 9202, TAKER_UID, 35, 35, 9, .bid);
            taker_tail.symbol = EXCHANGE_SYMBOL;
            try commands.append(allocator, taker_tail);

            var maker_reduce_missing = cmd.OrderCommand.reduce(MISSING_UID, MAKER_UID, 1);
            maker_reduce_missing.symbol = EXCHANGE_SYMBOL;
            try commands.append(allocator, maker_reduce_missing);

            var maker_cancel_missing = cmd.OrderCommand.cancel(MISSING_UID, MAKER_UID);
            maker_cancel_missing.symbol = EXCHANGE_SYMBOL;
            try commands.append(allocator, maker_cancel_missing);
        },
        else => return error.UnsupportedScenario,
    }

    var restored: ?kernel_mod.ExchangeKernel = null;
    defer if (restored) |*kernel| kernel.deinit();

    var active_kernel: *kernel_mod.ExchangeKernel = &source;
    const checkpoint_index: usize = switch (fixture.scenario) {
        .exchange_trade_bootstrap => 8,
        .exchange_trade_bootstrap_long => 24,
        else => return error.UnsupportedScenario,
    };

    for (commands.items, 0..) |*command, idx| {
        var response = try journal.executeAndRecord(active_kernel, command);
        defer response.deinit(allocator);
        try result.command_results.append(allocator, response.result_code);
        if (simple_events_runtime) |runtime| {
            try runtime.processor.accept(command, response.sequence, &response, response.matcher_events_head);
        }

        if (idx + 1 != checkpoint_index) continue;
        if (restored != null) return error.UnexpectedResult;

        const checkpoint_seq = source.next_seq;
        var snapshot = try snapshot_mod.KernelSnapshot.captureFromKernel(
            allocator,
            &source,
            &.{EXCHANGE_SYMBOL},
            32,
        );
        defer snapshot.deinit();

        restored = try kernel_mod.ExchangeKernel.init(allocator, .{
            .orderbook_kind = orderbook_kind,
        });
        try snapshot.restoreIntoKernel(&restored.?);
        restored.?.next_seq = checkpoint_seq;

        const replay_window = try journal.replayIntoKernelFromSequenceUntilTimestamp(
            &restored.?,
            checkpoint_seq,
            std.math.maxInt(u64),
        );
        if (replay_window.replayed != 0) return error.UnexpectedResult;

        var source_hash = try source.queryReport(.risk_state_hash);
        defer source_hash.deinit(allocator);
        var restored_hash = try restored.?.queryReport(.risk_state_hash);
        defer restored_hash.deinit(allocator);
        const source_risk = switch (source_hash) {
            .risk_state_hash => |value| value,
            else => return error.UnexpectedResult,
        };
        const restored_risk = switch (restored_hash) {
            .risk_state_hash => |value| value,
            else => return error.UnexpectedResult,
        };
        if (source_risk != restored_risk) return error.UnexpectedResult;

        active_kernel = &restored.?;
    }

    if (active_kernel != &source and restored == null) return error.UnexpectedResult;

    if (simple_events_runtime) |runtime| {
        try runtime.copyLinesToResult(allocator, &result);
    }

    result.state_hash_u64 = try active_kernel.risk_engine.stateHash();
    result.state_hash_java_i32 = toJavaSignedIntFromLow32(result.state_hash_u64);
    var detailed_hash_report = try active_kernel.queryReport(.state_hash_detailed);
    defer detailed_hash_report.deinit(allocator);
    result.state_hash_detailed_u64 = switch (detailed_hash_report) {
        .state_hash_detailed => |detailed| detailed.aggregate_hash,
        else => return error.UnexpectedResult,
    };
    result.state_hash_detailed_java_i32 = toJavaSignedIntFromLow32(result.state_hash_detailed_u64);
    result.fee_15 = active_kernel.risk_engine.getFeeCollected(QUOTE_CCY);
    result.maker_balance_15 = accountBalance(active_kernel, MAKER_UID, QUOTE_CCY);
    result.maker_balance_11 = accountBalance(active_kernel, MAKER_UID, BASE_CCY);
    result.taker_balance_15 = accountBalance(active_kernel, TAKER_UID, QUOTE_CCY);
    result.taker_balance_11 = accountBalance(active_kernel, TAKER_UID, BASE_CCY);

    if (fixture.symbol != 0) {
        var report = try active_kernel.queryReport(.{
            .order_book = .{
                .symbol = fixture.symbol,
                .depth = std.math.maxInt(usize),
            },
        });
        defer report.deinit(allocator);

        switch (report) {
            .order_book => |*snapshot| {
                try appendLevelsFromSnapshot(allocator, snapshot, &result.asks, &result.bids);
            },
            else => return error.UnexpectedResult,
        }
    }

    return result;
}

fn runUserBalanceScenario(
    allocator: std.mem.Allocator,
    kernel: *kernel_mod.ExchangeKernel,
    result: *ScenarioResult,
) DiffError!void {
    var add_maker = makeAddUserCommand(MAKER_UID);
    var add_taker = makeAddUserCommand(TAKER_UID);
    var dep_maker = makeBalanceAdjustmentCommand(MAKER_UID, QUOTE_CCY, 100_000, 1);
    var dep_taker = makeBalanceAdjustmentCommand(TAKER_UID, BASE_CCY, 100_000, 1);

    try executeAndRecord(allocator, kernel, &add_maker, &result.command_results);
    try executeAndRecord(allocator, kernel, &add_taker, &result.command_results);
    try executeAndRecord(allocator, kernel, &dep_maker, &result.command_results);
    try executeAndRecord(allocator, kernel, &dep_taker, &result.command_results);
}

fn runExchangeControlScenario(
    allocator: std.mem.Allocator,
    kernel: *kernel_mod.ExchangeKernel,
    result: *ScenarioResult,
) DiffError!void {
    var api = kernel_mod.ExchangeApi.init(kernel);
    defer api.deinit();

    {
        var response = try api.addUser(MAKER_UID);
        defer response.deinit(allocator);
        try result.command_results.append(allocator, response.result_code);
    }
    {
        var response = try api.suspendUser(MISSING_UID);
        defer response.deinit(allocator);
        try result.command_results.append(allocator, response.result_code);
    }
    {
        var response = try api.suspendUser(MAKER_UID);
        defer response.deinit(allocator);
        try result.command_results.append(allocator, response.result_code);
    }
    {
        var response = try api.suspendUser(MAKER_UID);
        defer response.deinit(allocator);
        try result.command_results.append(allocator, response.result_code);
    }
    {
        var response = try api.resumeUser(MAKER_UID);
        defer response.deinit(allocator);
        try result.command_results.append(allocator, response.result_code);
    }
    {
        var response = try api.resumeUser(MAKER_UID);
        defer response.deinit(allocator);
        try result.command_results.append(allocator, response.result_code);
    }
    {
        var response = try api.adjustUserBalance(MAKER_UID, 1, QUOTE_CCY, 1234, .adjustment);
        defer response.deinit(allocator);
        try result.command_results.append(allocator, response.result_code);
    }
    {
        var grouping: cmd.OrderCommand = .{
            .command = .grouping_control,
            .order_id = 1,
            .timestamp = 444_555_666,
            .result_code = .new,
        };
        try executeAndRecord(allocator, kernel, &grouping, &result.command_results);
    }
    {
        var response = try api.nop();
        defer response.deinit(allocator);
        try result.command_results.append(allocator, response.result_code);
    }
    {
        const merged = try api.persistState(8001, 444_555_667);
        try result.command_results.append(allocator, merged);
    }
    {
        var response = try api.reset();
        defer response.deinit(allocator);
        try result.command_results.append(allocator, response.result_code);
    }

    {
        var response = try api.addUser(TAKER_UID);
        defer response.deinit(allocator);
        try result.command_results.append(allocator, response.result_code);
    }
    {
        var response = try api.adjustUserBalance(TAKER_UID, 1, BASE_CCY, 55, .adjustment);
        defer response.deinit(allocator);
        try result.command_results.append(allocator, response.result_code);
    }
}

fn runExchangeTradeScenario(
    allocator: std.mem.Allocator,
    kernel: *kernel_mod.ExchangeKernel,
    result: *ScenarioResult,
    simple_events_runtime: ?*SimpleEventsRuntime,
) DiffError!void {
    const spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = EXCHANGE_SYMBOL,
        .symbol_type = .currency_exchange_pair,
        .base_currency = BASE_CCY,
        .quote_currency = QUOTE_CCY,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
    });
    if (!(try kernel.addSymbol(spec))) return error.UnexpectedResult;

    var add_maker = makeAddUserCommand(MAKER_UID);
    var add_taker = makeAddUserCommand(TAKER_UID);
    var dep_maker = makeBalanceAdjustmentCommand(MAKER_UID, BASE_CCY, 100_000, 1);
    var dep_taker = makeBalanceAdjustmentCommand(TAKER_UID, QUOTE_CCY, 100_000, 1);

    var maker_ask = cmd.OrderCommand.newOrder(.gtc, 9001, MAKER_UID, 100, 0, 7, .ask);
    maker_ask.symbol = EXCHANGE_SYMBOL;
    var taker_bid = cmd.OrderCommand.newOrder(.ioc, 9002, TAKER_UID, 100, 100, 5, .bid);
    taker_bid.symbol = EXCHANGE_SYMBOL;

    try executeAndRecordWithSimpleEvents(allocator, kernel, &add_maker, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &add_taker, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &dep_maker, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &dep_taker, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &maker_ask, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &taker_bid, &result.command_results, simple_events_runtime);
}

fn runExchangeTradeExtendedScenario(
    allocator: std.mem.Allocator,
    kernel: *kernel_mod.ExchangeKernel,
    result: *ScenarioResult,
    simple_events_runtime: ?*SimpleEventsRuntime,
) DiffError!void {
    const spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = EXCHANGE_SYMBOL,
        .symbol_type = .currency_exchange_pair,
        .base_currency = BASE_CCY,
        .quote_currency = QUOTE_CCY,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
    });
    if (!(try kernel.addSymbol(spec))) return error.UnexpectedResult;

    var add_maker = makeAddUserCommand(MAKER_UID);
    var add_taker = makeAddUserCommand(TAKER_UID);
    var add_maker_dup = makeAddUserCommand(MAKER_UID);
    var dep_maker = makeBalanceAdjustmentCommand(MAKER_UID, BASE_CCY, 150_000, 1);
    var dep_taker = makeBalanceAdjustmentCommand(TAKER_UID, QUOTE_CCY, 200_000, 1);

    var maker_ask = cmd.OrderCommand.newOrder(.gtc, 9101, MAKER_UID, 101, 0, 10, .ask);
    maker_ask.symbol = EXCHANGE_SYMBOL;
    var maker_move = cmd.OrderCommand.update(9101, MAKER_UID, 102);
    maker_move.symbol = EXCHANGE_SYMBOL;

    var taker_bid_rest = cmd.OrderCommand.newOrder(.gtc, 9201, TAKER_UID, 99, 99, 4, .bid);
    taker_bid_rest.symbol = EXCHANGE_SYMBOL;
    var taker_bid_ioc = cmd.OrderCommand.newOrder(.ioc, 9202, TAKER_UID, 102, 102, 5, .bid);
    taker_bid_ioc.symbol = EXCHANGE_SYMBOL;

    var maker_ask_2 = cmd.OrderCommand.newOrder(.gtc, 9102, MAKER_UID, 103, 0, 4, .ask);
    maker_ask_2.symbol = EXCHANGE_SYMBOL;
    var taker_bid_cross = cmd.OrderCommand.newOrder(.gtc, 9203, TAKER_UID, 102, 102, 1, .bid);
    taker_bid_cross.symbol = EXCHANGE_SYMBOL;
    var taker_move_cross = cmd.OrderCommand.update(9203, TAKER_UID, 104);
    taker_move_cross.symbol = EXCHANGE_SYMBOL;
    var maker_reduce_missing = cmd.OrderCommand.reduce(9999, MAKER_UID, 1);
    maker_reduce_missing.symbol = EXCHANGE_SYMBOL;
    var maker_cancel_missing = cmd.OrderCommand.cancel(9999, MAKER_UID);
    maker_cancel_missing.symbol = EXCHANGE_SYMBOL;

    try executeAndRecordWithSimpleEvents(allocator, kernel, &add_maker, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &add_taker, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &add_maker_dup, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &dep_maker, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &dep_taker, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &maker_ask, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &maker_move, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &taker_bid_rest, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &taker_bid_ioc, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &maker_ask_2, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &taker_bid_cross, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &taker_move_cross, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &maker_reduce_missing, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &maker_cancel_missing, &result.command_results, simple_events_runtime);
}

fn runExchangeTradeMultiSymbolScenario(
    allocator: std.mem.Allocator,
    kernel: *kernel_mod.ExchangeKernel,
    result: *ScenarioResult,
    simple_events_runtime: ?*SimpleEventsRuntime,
) DiffError!void {
    const symbol_a: i32 = 241;
    const symbol_b: i32 = 242;

    const spec_a = try common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol_a,
        .symbol_type = .currency_exchange_pair,
        .base_currency = BASE_CCY,
        .quote_currency = QUOTE_CCY,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
    });
    const spec_b = try common.CoreSymbolSpecification.init(.{
        .symbol_id = symbol_b,
        .symbol_type = .currency_exchange_pair,
        .base_currency = BASE_CCY,
        .quote_currency = QUOTE_CCY,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
    });
    if (!(try kernel.addSymbol(spec_a))) return error.UnexpectedResult;
    if (!(try kernel.addSymbol(spec_b))) return error.UnexpectedResult;

    var add_maker = makeAddUserCommand(MAKER_UID);
    var add_taker = makeAddUserCommand(TAKER_UID);
    var add_maker_dup = makeAddUserCommand(MAKER_UID);
    var dep_maker = makeBalanceAdjustmentCommand(MAKER_UID, BASE_CCY, 220_000, 1);
    var dep_taker = makeBalanceAdjustmentCommand(TAKER_UID, QUOTE_CCY, 220_000, 1);

    var ask_a_1 = cmd.OrderCommand.newOrder(.gtc, 9301, MAKER_UID, 101, 0, 6, .ask);
    ask_a_1.symbol = symbol_a;
    var bid_a_1 = cmd.OrderCommand.newOrder(.ioc, 9302, TAKER_UID, 101, 101, 4, .bid);
    bid_a_1.symbol = symbol_a;

    var ask_b_1 = cmd.OrderCommand.newOrder(.gtc, 9401, MAKER_UID, 161, 0, 5, .ask);
    ask_b_1.symbol = symbol_b;
    var move_b_1 = cmd.OrderCommand.update(9401, MAKER_UID, 160);
    move_b_1.symbol = symbol_b;
    var bid_b_1 = cmd.OrderCommand.newOrder(.ioc, 9402, TAKER_UID, 160, 160, 3, .bid);
    bid_b_1.symbol = symbol_b;
    var bid_b_rest = cmd.OrderCommand.newOrder(.gtc, 9403, TAKER_UID, 150, 150, 2, .bid);
    bid_b_rest.symbol = symbol_b;

    var ask_a_2 = cmd.OrderCommand.newOrder(.gtc, 9303, MAKER_UID, 103, 0, 2, .ask);
    ask_a_2.symbol = symbol_a;

    var reduce_missing = cmd.OrderCommand.reduce(9999, MAKER_UID, 1);
    reduce_missing.symbol = symbol_a;
    var cancel_missing = cmd.OrderCommand.cancel(9999, TAKER_UID);
    cancel_missing.symbol = symbol_b;

    try executeAndRecordWithSimpleEvents(allocator, kernel, &add_maker, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &add_taker, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &add_maker_dup, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &dep_maker, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &dep_taker, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &ask_a_1, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &bid_a_1, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &ask_b_1, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &move_b_1, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &bid_b_1, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &bid_b_rest, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &ask_a_2, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &reduce_missing, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &cancel_missing, &result.command_results, simple_events_runtime);
}

fn runExchangeTradeReportsScenario(
    allocator: std.mem.Allocator,
    kernel: *kernel_mod.ExchangeKernel,
    result: *ScenarioResult,
    simple_events_runtime: ?*SimpleEventsRuntime,
) DiffError!void {
    const spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = EXCHANGE_SYMBOL,
        .symbol_type = .currency_exchange_pair,
        .base_currency = BASE_CCY,
        .quote_currency = QUOTE_CCY,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
    });
    if (!(try kernel.addSymbol(spec))) return error.UnexpectedResult;

    var add_maker = makeAddUserCommand(MAKER_UID);
    var add_taker = makeAddUserCommand(TAKER_UID);
    var add_maker_dup = makeAddUserCommand(MAKER_UID);
    var dep_maker = makeBalanceAdjustmentCommand(MAKER_UID, BASE_CCY, 150_000, 1);
    var dep_taker = makeBalanceAdjustmentCommand(TAKER_UID, QUOTE_CCY, 200_000, 1);

    var maker_ask = cmd.OrderCommand.newOrder(.gtc, 9101, MAKER_UID, 101, 0, 10, .ask);
    maker_ask.symbol = EXCHANGE_SYMBOL;
    var maker_move = cmd.OrderCommand.update(9101, MAKER_UID, 102);
    maker_move.symbol = EXCHANGE_SYMBOL;

    var taker_bid_rest = cmd.OrderCommand.newOrder(.gtc, 9201, TAKER_UID, 99, 99, 4, .bid);
    taker_bid_rest.symbol = EXCHANGE_SYMBOL;
    var taker_bid_ioc = cmd.OrderCommand.newOrder(.ioc, 9202, TAKER_UID, 102, 102, 5, .bid);
    taker_bid_ioc.symbol = EXCHANGE_SYMBOL;

    var maker_ask_2 = cmd.OrderCommand.newOrder(.gtc, 9102, MAKER_UID, 103, 0, 4, .ask);
    maker_ask_2.symbol = EXCHANGE_SYMBOL;
    var taker_bid_cross = cmd.OrderCommand.newOrder(.gtc, 9203, TAKER_UID, 102, 102, 1, .bid);
    taker_bid_cross.symbol = EXCHANGE_SYMBOL;
    var taker_move_cross = cmd.OrderCommand.update(9203, TAKER_UID, 104);
    taker_move_cross.symbol = EXCHANGE_SYMBOL;
    var maker_reduce_missing = cmd.OrderCommand.reduce(MISSING_UID, MAKER_UID, 1);
    maker_reduce_missing.symbol = EXCHANGE_SYMBOL;
    var maker_cancel_missing = cmd.OrderCommand.cancel(MISSING_UID, MAKER_UID);
    maker_cancel_missing.symbol = EXCHANGE_SYMBOL;

    try executeAndRecordWithSimpleEvents(allocator, kernel, &add_maker, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &add_taker, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &add_maker_dup, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &dep_maker, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &dep_taker, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &maker_ask, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &maker_move, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &taker_bid_rest, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &taker_bid_ioc, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &maker_ask_2, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &taker_bid_cross, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &taker_move_cross, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &maker_reduce_missing, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &maker_cancel_missing, &result.command_results, simple_events_runtime);

    try populateTradeReportFields(allocator, kernel, result);
}

fn runExchangeTradeReportsLongScenario(
    allocator: std.mem.Allocator,
    kernel: *kernel_mod.ExchangeKernel,
    result: *ScenarioResult,
    simple_events_runtime: ?*SimpleEventsRuntime,
) DiffError!void {
    const spec = try common.CoreSymbolSpecification.init(.{
        .symbol_id = EXCHANGE_SYMBOL,
        .symbol_type = .currency_exchange_pair,
        .base_currency = BASE_CCY,
        .quote_currency = QUOTE_CCY,
        .base_scale_k = 1,
        .quote_scale_k = 1,
        .taker_fee = 0,
        .maker_fee = 0,
    });
    if (!(try kernel.addSymbol(spec))) return error.UnexpectedResult;

    var add_maker = makeAddUserCommand(MAKER_UID);
    var add_taker = makeAddUserCommand(TAKER_UID);
    var add_maker_dup = makeAddUserCommand(MAKER_UID);
    var dep_maker = makeBalanceAdjustmentCommand(MAKER_UID, BASE_CCY, 400_000, 1);
    var dep_taker = makeBalanceAdjustmentCommand(TAKER_UID, QUOTE_CCY, 400_000, 1);

    var maker_anchor = cmd.OrderCommand.newOrder(.gtc, 9101, MAKER_UID, 101, 0, 20, .ask);
    maker_anchor.symbol = EXCHANGE_SYMBOL;
    var maker_anchor_move = cmd.OrderCommand.update(9101, MAKER_UID, 102);
    maker_anchor_move.symbol = EXCHANGE_SYMBOL;
    var taker_bid_rest = cmd.OrderCommand.newOrder(.gtc, 9201, TAKER_UID, 99, 99, 7, .bid);
    taker_bid_rest.symbol = EXCHANGE_SYMBOL;

    try executeAndRecordWithSimpleEvents(allocator, kernel, &add_maker, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &add_taker, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &add_maker_dup, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &dep_maker, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &dep_taker, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &maker_anchor, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &maker_anchor_move, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &taker_bid_rest, &result.command_results, simple_events_runtime);

    const rounds: u32 = 16;
    var round: u32 = 0;
    while (round < rounds) : (round += 1) {
        const round_i64: i64 = @intCast(round);
        const ask_id: i64 = 9500 + round_i64;
        const ask_price: i64 = 140 + round_i64;
        const ask_size: i64 = 4 + @as(i64, @intCast(round % 3));
        const bid_id: i64 = 9600 + round_i64;
        const bid_price: i64 = 60 - @as(i64, @intCast(round % 4));
        const bid_size: i64 = 2 + @as(i64, @intCast(round % 2));

        var maker_ask = cmd.OrderCommand.newOrder(.gtc, ask_id, MAKER_UID, ask_price, 0, ask_size, .ask);
        maker_ask.symbol = EXCHANGE_SYMBOL;
        try executeAndRecordWithSimpleEvents(allocator, kernel, &maker_ask, &result.command_results, simple_events_runtime);

        var taker_bid = cmd.OrderCommand.newOrder(.gtc, bid_id, TAKER_UID, bid_price, bid_price, bid_size, .bid);
        taker_bid.symbol = EXCHANGE_SYMBOL;
        try executeAndRecordWithSimpleEvents(allocator, kernel, &taker_bid, &result.command_results, simple_events_runtime);
    }

    var maker_tail = cmd.OrderCommand.newOrder(.gtc, 9102, MAKER_UID, 171, 0, 6, .ask);
    maker_tail.symbol = EXCHANGE_SYMBOL;
    var taker_tail_bid = cmd.OrderCommand.newOrder(.gtc, 9203, TAKER_UID, 58, 58, 3, .bid);
    taker_tail_bid.symbol = EXCHANGE_SYMBOL;
    var taker_tail_rest = cmd.OrderCommand.newOrder(.gtc, 9204, TAKER_UID, 57, 57, 5, .bid);
    taker_tail_rest.symbol = EXCHANGE_SYMBOL;
    var maker_reduce_missing = cmd.OrderCommand.reduce(MISSING_UID, MAKER_UID, 1);
    maker_reduce_missing.symbol = EXCHANGE_SYMBOL;
    var maker_cancel_missing = cmd.OrderCommand.cancel(MISSING_UID, MAKER_UID);
    maker_cancel_missing.symbol = EXCHANGE_SYMBOL;

    try executeAndRecordWithSimpleEvents(allocator, kernel, &maker_tail, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &taker_tail_bid, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &taker_tail_rest, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &maker_reduce_missing, &result.command_results, simple_events_runtime);
    try executeAndRecordWithSimpleEvents(allocator, kernel, &maker_cancel_missing, &result.command_results, simple_events_runtime);

    try populateTradeReportFields(allocator, kernel, result);
}

fn populateTradeReportFields(
    allocator: std.mem.Allocator,
    kernel: *kernel_mod.ExchangeKernel,
    result: *ScenarioResult,
) DiffError!void {
    var maker_report = try kernel.queryReport(.{ .single_user = MAKER_UID });
    defer maker_report.deinit(allocator);
    switch (maker_report) {
        .single_user => |*single| {
            result.report_maker_status = singleUserStatusCode(single.query_execution_status);
            result.report_maker_orders = @as(i64, @intCast(single.orders.items.len));
            result.report_maker_balance_15 = mapValueFromI32I64Map(&single.accounts, QUOTE_CCY);
            result.report_maker_balance_11 = mapValueFromI32I64Map(&single.accounts, BASE_CCY);
        },
        else => return error.UnexpectedResult,
    }

    var taker_report = try kernel.queryReport(.{ .single_user = TAKER_UID });
    defer taker_report.deinit(allocator);
    switch (taker_report) {
        .single_user => |*single| {
            result.report_taker_status = singleUserStatusCode(single.query_execution_status);
            result.report_taker_orders = @as(i64, @intCast(single.orders.items.len));
            result.report_taker_balance_15 = mapValueFromI32I64Map(&single.accounts, QUOTE_CCY);
            result.report_taker_balance_11 = mapValueFromI32I64Map(&single.accounts, BASE_CCY);
        },
        else => return error.UnexpectedResult,
    }

    var missing_report = try kernel.queryReport(.{ .single_user = MISSING_UID });
    defer missing_report.deinit(allocator);
    switch (missing_report) {
        .single_user => |*single| {
            result.report_missing_status = singleUserStatusCode(single.query_execution_status);
        },
        else => return error.UnexpectedResult,
    }

    var total_report = try kernel.queryReport(.total_currency_balance);
    defer total_report.deinit(allocator);
    switch (total_report) {
        .total_currency_balance => |*total| {
            result.report_total_accounts_15 = mapValueFromI32I64Map(&total.account_balances, QUOTE_CCY);
            result.report_total_accounts_11 = mapValueFromI32I64Map(&total.account_balances, BASE_CCY);
            result.report_total_orders_15 = mapValueFromI32I64Map(&total.orders_balances, QUOTE_CCY);
            result.report_total_orders_11 = mapValueFromI32I64Map(&total.orders_balances, BASE_CCY);
            result.report_total_fees_15 = mapValueFromI32I64Map(&total.fees, QUOTE_CCY);
            result.report_total_fees_11 = mapValueFromI32I64Map(&total.fees, BASE_CCY);
        },
        else => return error.UnexpectedResult,
    }
}

fn executeAndRecord(
    allocator: std.mem.Allocator,
    kernel: *kernel_mod.ExchangeKernel,
    command: *cmd.OrderCommand,
    out: *std.ArrayListUnmanaged(cmd.CommandResultCode),
) DiffError!void {
    try executeAndRecordWithSimpleEvents(allocator, kernel, command, out, null);
}

fn executeAndRecordWithSimpleEvents(
    allocator: std.mem.Allocator,
    kernel: *kernel_mod.ExchangeKernel,
    command: *cmd.OrderCommand,
    out: *std.ArrayListUnmanaged(cmd.CommandResultCode),
    simple_events_runtime: ?*SimpleEventsRuntime,
) DiffError!void {
    var response = try kernel.execute(command);
    defer response.deinit(allocator);
    try out.append(allocator, response.result_code);
    if (simple_events_runtime) |runtime| {
        try runtime.processor.accept(command, response.sequence, &response, response.matcher_events_head);
    }
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
    tx_id: i64,
) cmd.OrderCommand {
    return .{
        .command = .balance_adjustment,
        .uid = uid,
        .symbol = currency,
        .price = amount,
        .order_id = tx_id,
        .order_type = .gtc,
        .result_code = .new,
    };
}

fn accountBalance(kernel: *kernel_mod.ExchangeKernel, uid: i64, currency: i32) i64 {
    if (kernel.risk_engine.getUserProfile(uid)) |profile| {
        return profile.accountBalance(currency);
    }
    return 0;
}

fn mapValueFromI32I64Map(map: *const std.AutoHashMapUnmanaged(i32, i64), key: i32) i64 {
    return map.get(key) orelse 0;
}

fn singleUserStatusCode(status: kernel_mod.SingleUserQueryExecutionStatus) i64 {
    return switch (status) {
        .ok => 0,
        .user_not_found => 1,
    };
}

fn appendLevelsFromSnapshot(
    allocator: std.mem.Allocator,
    snapshot: *const naive.L2MarketData,
    asks: *std.ArrayListUnmanaged(Level),
    bids: *std.ArrayListUnmanaged(Level),
) DiffError!void {
    try asks.ensureTotalCapacityPrecise(allocator, snapshot.asks.items.len);
    try bids.ensureTotalCapacityPrecise(allocator, snapshot.bids.items.len);

    for (snapshot.asks.items, 0..) |level, idx| {
        asks.appendAssumeCapacity(.{
            .index = idx,
            .price = level.price,
            .volume = level.volume,
            .orders = level.orders,
        });
    }
    for (snapshot.bids.items, 0..) |level, idx| {
        bids.appendAssumeCapacity(.{
            .index = idx,
            .price = level.price,
            .volume = level.volume,
            .orders = level.orders,
        });
    }
}

fn toJavaSignedIntFromLow32(value: u64) i64 {
    const lower: u32 = @truncate(value);
    const signed: i32 = @bitCast(lower);
    return @as(i64, signed);
}

fn compareFixture(path: []const u8, expected: *const Fixture, actual: *const ScenarioResult) usize {
    var mismatches: usize = 0;

    if (expected.command_results.items.len != actual.command_results.items.len) {
        mismatches += 1;
        std.debug.print(
            "  {s}: command_results length expected={d} actual={d}\n",
            .{ path, expected.command_results.items.len, actual.command_results.items.len },
        );
    }

    const min_len = @min(expected.command_results.items.len, actual.command_results.items.len);
    for (0..min_len) |idx| {
        const exp = expected.command_results.items[idx];
        const act = actual.command_results.items[idx];
        if (exp != act) {
            mismatches += 1;
            var exp_buf: [64]u8 = undefined;
            var act_buf: [64]u8 = undefined;
            std.debug.print(
                "  {s}: command_results[{d}] expected={s} actual={s}\n",
                .{ path, idx, toJavaResultName(exp, &exp_buf), toJavaResultName(act, &act_buf) },
            );
        }
    }

    if (expected.has_simple_events) {
        if (expected.simple_events.items.len != actual.simple_events.items.len) {
            mismatches += 1;
            std.debug.print(
                "  {s}: simple_events length expected={d} actual={d}\n",
                .{ path, expected.simple_events.items.len, actual.simple_events.items.len },
            );
        }

        const min_simple_len = @min(expected.simple_events.items.len, actual.simple_events.items.len);
        for (0..min_simple_len) |idx| {
            const exp = expected.simple_events.items[idx].value;
            const act = actual.simple_events.items[idx];
            if (!std.mem.eql(u8, exp, act)) {
                mismatches += 1;
                std.debug.print(
                    "  {s}: simple_event_{d} expected={s} actual={s}\n",
                    .{ path, idx, exp, act },
                );
            }
        }
    }

    switch (expected.state_hash_kind) {
        .zig_risk_low32 => {
            mismatches += compareI64(path, "state_hash", expected.state_hash, actual.state_hash_java_i32);
            if (expected.state_hash != actual.state_hash_java_i32) {
                std.debug.print(
                    "  {s}: state_hash details expected(i32)={d} actual(i32)={d} actual_u64={x}\n",
                    .{ path, expected.state_hash, actual.state_hash_java_i32, actual.state_hash_u64 },
                );
            }
        },
        .java_report => {
            // Legacy fixture mode: compare against deterministic detailed-hash low32.
            mismatches += compareI64(path, "state_hash", expected.state_hash, actual.state_hash_detailed_java_i32);
            if (expected.state_hash != actual.state_hash_detailed_java_i32) {
                std.debug.print(
                    "  {s}: state_hash details expected(i32)={d} actual(i32)={d} actual_u64={x}\n",
                    .{ path, expected.state_hash, actual.state_hash_detailed_java_i32, actual.state_hash_detailed_u64 },
                );
            }
        },
        .zig_detailed_low32 => {
            mismatches += compareI64(path, "state_hash", expected.state_hash, actual.state_hash_detailed_java_i32);
            if (expected.state_hash != actual.state_hash_detailed_java_i32) {
                std.debug.print(
                    "  {s}: state_hash details expected(i32)={d} actual(i32)={d} actual_u64={x}\n",
                    .{ path, expected.state_hash, actual.state_hash_detailed_java_i32, actual.state_hash_detailed_u64 },
                );
            }
        },
    }
    mismatches += compareI64(path, "fee_15", expected.fee_15, actual.fee_15);
    const expected_maker_balance_15 = normalizedExpectedProjectedBalance(
        expected.settlement_mode,
        expected.scenario,
        expected.maker_balance_15,
        .maker_quote,
    );
    mismatches += compareI64(path, "maker_balance_15", expected_maker_balance_15, actual.maker_balance_15);
    mismatches += compareI64(path, "maker_balance_11", expected.maker_balance_11, actual.maker_balance_11);
    mismatches += compareI64(path, "taker_balance_15", expected.taker_balance_15, actual.taker_balance_15);
    const expected_taker_balance_11 = normalizedExpectedProjectedBalance(
        expected.settlement_mode,
        expected.scenario,
        expected.taker_balance_11,
        .taker_base,
    );
    mismatches += compareI64(path, "taker_balance_11", expected_taker_balance_11, actual.taker_balance_11);

    if (scenarioHasReportFields(expected.scenario)) {
        mismatches += compareI64(path, "report_maker_status", expected.report_maker_status, actual.report_maker_status);
        mismatches += compareI64(path, "report_missing_status", expected.report_missing_status, actual.report_missing_status);
        mismatches += compareI64(path, "report_maker_orders", expected.report_maker_orders, actual.report_maker_orders);
        const expected_report_maker_balance_15 = normalizedExpectedProjectedBalance(
            expected.settlement_mode,
            expected.scenario,
            expected.report_maker_balance_15,
            .maker_quote,
        );
        mismatches += compareI64(path, "report_maker_balance_15", expected_report_maker_balance_15, actual.report_maker_balance_15);
        mismatches += compareI64(path, "report_maker_balance_11", expected.report_maker_balance_11, actual.report_maker_balance_11);
        mismatches += compareI64(path, "report_taker_status", expected.report_taker_status, actual.report_taker_status);
        mismatches += compareI64(path, "report_taker_orders", expected.report_taker_orders, actual.report_taker_orders);
        mismatches += compareI64(path, "report_taker_balance_15", expected.report_taker_balance_15, actual.report_taker_balance_15);
        const expected_report_taker_balance_11 = normalizedExpectedProjectedBalance(
            expected.settlement_mode,
            expected.scenario,
            expected.report_taker_balance_11,
            .taker_base,
        );
        mismatches += compareI64(path, "report_taker_balance_11", expected_report_taker_balance_11, actual.report_taker_balance_11);
        const expected_report_total_accounts_15 = normalizedExpectedAggregateBalance(
            expected.settlement_mode,
            expected.scenario,
            expected.report_total_accounts_15,
            .maker_quote,
        );
        mismatches += compareI64(path, "report_total_accounts_15", expected_report_total_accounts_15, actual.report_total_accounts_15);
        const expected_report_total_accounts_11 = normalizedExpectedAggregateBalance(
            expected.settlement_mode,
            expected.scenario,
            expected.report_total_accounts_11,
            .taker_base,
        );
        mismatches += compareI64(path, "report_total_accounts_11", expected_report_total_accounts_11, actual.report_total_accounts_11);
        mismatches += compareI64(path, "report_total_orders_15", expected.report_total_orders_15, actual.report_total_orders_15);
        mismatches += compareI64(path, "report_total_orders_11", expected.report_total_orders_11, actual.report_total_orders_11);
        mismatches += compareI64(path, "report_total_fees_15", expected.report_total_fees_15, actual.report_total_fees_15);
        mismatches += compareI64(path, "report_total_fees_11", expected.report_total_fees_11, actual.report_total_fees_11);
    }

    mismatches += compareUsize(path, "orderbook_ask_size", expected.orderbook_ask_size, actual.asks.items.len);
    mismatches += compareUsize(path, "orderbook_bid_size", expected.orderbook_bid_size, actual.bids.items.len);

    mismatches += compareLevels(path, "ask", expected.asks.items, actual.asks.items);
    mismatches += compareLevels(path, "bid", expected.bids.items, actual.bids.items);

    return mismatches;
}

fn compareI64(path: []const u8, field: []const u8, expected: i64, actual: i64) usize {
    if (expected == actual) return 0;
    std.debug.print("  {s}: {s} expected={d} actual={d}\n", .{ path, field, expected, actual });
    return 1;
}

fn compareUsize(path: []const u8, field: []const u8, expected: usize, actual: usize) usize {
    if (expected == actual) return 0;
    std.debug.print("  {s}: {s} expected={d} actual={d}\n", .{ path, field, expected, actual });
    return 1;
}

const SettlementSensitiveBalance = enum {
    maker_quote,
    taker_base,
};

const PreReleaseSettlementProjection = struct {
    maker_quote_delta: i64,
    taker_base_delta: i64,

    fn deltaFor(self: PreReleaseSettlementProjection, field: SettlementSensitiveBalance) i64 {
        return switch (field) {
            .maker_quote => self.maker_quote_delta,
            .taker_base => self.taker_base_delta,
        };
    }
};

fn normalizedExpectedProjectedBalance(
    settlement_mode: SettlementMode,
    scenario: FixtureScenario,
    expected_balance: i64,
    field: SettlementSensitiveBalance,
) i64 {
    if (settlement_mode != .pre_release) return expected_balance;
    const projection = preReleaseSettlementProjection(scenario) orelse return expected_balance;
    return projection.deltaFor(field);
}

fn normalizedExpectedAggregateBalance(
    settlement_mode: SettlementMode,
    scenario: FixtureScenario,
    expected_balance: i64,
    field: SettlementSensitiveBalance,
) i64 {
    if (settlement_mode != .pre_release) return expected_balance;
    const projection = preReleaseSettlementProjection(scenario) orelse return expected_balance;
    return std.math.add(i64, expected_balance, projection.deltaFor(field)) catch unreachable;
}

fn preReleaseSettlementProjection(scenario: FixtureScenario) ?PreReleaseSettlementProjection {
    return switch (scenario) {
        .exchange_trade_basic => .{
            .maker_quote_delta = BASIC_EXECUTED_QUOTE,
            .taker_base_delta = BASIC_EXECUTED_BASE,
        },
        .exchange_trade_extended,
        .exchange_trade_bootstrap,
        .exchange_trade_reports,
        => .{
            .maker_quote_delta = EXTENDED_EXECUTED_QUOTE,
            .taker_base_delta = EXTENDED_EXECUTED_BASE,
        },
        .exchange_trade_multisymbol,
        => .{
            .maker_quote_delta = MULTISYMBOL_EXECUTED_QUOTE,
            .taker_base_delta = MULTISYMBOL_EXECUTED_BASE,
        },
        else => null,
    };
}

fn scenarioHasReportFields(scenario: FixtureScenario) bool {
    return switch (scenario) {
        .exchange_trade_reports, .exchange_trade_reports_long => true,
        else => false,
    };
}

fn compareLevels(path: []const u8, side: []const u8, expected: []const Level, actual: []const Level) usize {
    var mismatches: usize = 0;
    const min_len = @min(expected.len, actual.len);
    for (0..min_len) |idx| {
        const exp = expected[idx];
        const act = actual[idx];
        if (exp.price != act.price or exp.volume != act.volume or exp.orders != act.orders) {
            mismatches += 1;
            std.debug.print(
                "  {s}: {s}_{d} expected={d}:{d}:{d} actual={d}:{d}:{d}\n",
                .{ path, side, idx, exp.price, exp.volume, exp.orders, act.price, act.volume, act.orders },
            );
        }
    }
    return mismatches;
}

fn toJavaResultName(code: cmd.CommandResultCode, out: *[64]u8) []const u8 {
    const tag = @tagName(code);
    std.debug.assert(tag.len <= out.len);
    for (tag, 0..) |ch, idx| {
        out[idx] = std.ascii.toUpper(ch);
    }
    return out[0..tag.len];
}

test "parse command result code from java enum name" {
    try std.testing.expectEqual(cmd.CommandResultCode.success, try parseCommandResultCode("SUCCESS"));
    try std.testing.expectEqual(
        cmd.CommandResultCode.matching_invalid_order_book_id,
        try parseCommandResultCode("MATCHING_INVALID_ORDER_BOOK_ID"),
    );
    try std.testing.expectError(error.UnknownCommandResult, parseCommandResultCode("NOT_A_RESULT"));
}

test "parse fixture mode and scenario enums" {
    try std.testing.expectEqual(FixtureOrderBookMode.naive, try parseOrderBookMode("naive"));
    try std.testing.expectEqual(FixtureOrderBookMode.na, try parseOrderBookMode("na"));
    try std.testing.expectEqual(FixtureScenario.exchange_control, try parseScenario("exchange_control"));
    try std.testing.expectEqual(FixtureScenario.exchange_trade_basic, try parseScenario("exchange_trade_basic"));
    try std.testing.expectEqual(FixtureScenario.exchange_trade_extended, try parseScenario("exchange_trade_extended"));
    try std.testing.expectEqual(FixtureScenario.exchange_trade_bootstrap, try parseScenario("exchange_trade_bootstrap"));
    try std.testing.expectEqual(FixtureScenario.exchange_trade_bootstrap_long, try parseScenario("exchange_trade_bootstrap_long"));
    try std.testing.expectEqual(FixtureScenario.exchange_trade_multisymbol, try parseScenario("exchange_trade_multisymbol"));
    try std.testing.expectEqual(FixtureScenario.exchange_trade_reports, try parseScenario("exchange_trade_reports"));
    try std.testing.expectEqual(FixtureScenario.exchange_trade_reports_long, try parseScenario("exchange_trade_reports_long"));
    try std.testing.expectEqual(StateHashKind.zig_detailed_low32, try parseStateHashKind("zig_detailed_low32"));
    try std.testing.expectError(error.UnknownOrderBookMode, parseOrderBookMode("bad"));
    try std.testing.expectError(error.UnknownScenario, parseScenario("bad"));
    try std.testing.expectError(error.InvalidFieldValue, parseStateHashKind("bad"));
}

test "pre-release settlement projection mapping" {
    const basic = preReleaseSettlementProjection(.exchange_trade_basic) orelse return error.UnexpectedResult;
    try std.testing.expectEqual(BASIC_EXECUTED_QUOTE, basic.deltaFor(.maker_quote));
    try std.testing.expectEqual(BASIC_EXECUTED_BASE, basic.deltaFor(.taker_base));

    const extended = preReleaseSettlementProjection(.exchange_trade_extended) orelse return error.UnexpectedResult;
    try std.testing.expectEqual(EXTENDED_EXECUTED_QUOTE, extended.deltaFor(.maker_quote));
    try std.testing.expectEqual(EXTENDED_EXECUTED_BASE, extended.deltaFor(.taker_base));

    const bootstrap = preReleaseSettlementProjection(.exchange_trade_bootstrap) orelse return error.UnexpectedResult;
    try std.testing.expectEqual(EXTENDED_EXECUTED_QUOTE, bootstrap.deltaFor(.maker_quote));
    try std.testing.expectEqual(EXTENDED_EXECUTED_BASE, bootstrap.deltaFor(.taker_base));

    const reports = preReleaseSettlementProjection(.exchange_trade_reports) orelse return error.UnexpectedResult;
    try std.testing.expectEqual(EXTENDED_EXECUTED_QUOTE, reports.deltaFor(.maker_quote));
    try std.testing.expectEqual(EXTENDED_EXECUTED_BASE, reports.deltaFor(.taker_base));

    const multisymbol = preReleaseSettlementProjection(.exchange_trade_multisymbol) orelse return error.UnexpectedResult;
    try std.testing.expectEqual(MULTISYMBOL_EXECUTED_QUOTE, multisymbol.deltaFor(.maker_quote));
    try std.testing.expectEqual(MULTISYMBOL_EXECUTED_BASE, multisymbol.deltaFor(.taker_base));

    try std.testing.expect(preReleaseSettlementProjection(.exchange_trade_bootstrap_long) == null);
    try std.testing.expect(preReleaseSettlementProjection(.exchange_trade_reports_long) == null);
}

test "normalized projected balance uses canonical full-settlement targets" {
    try std.testing.expectEqual(
        BASIC_EXECUTED_QUOTE,
        normalizedExpectedProjectedBalance(.pre_release, .exchange_trade_basic, 100, .maker_quote),
    );
    try std.testing.expectEqual(
        BASIC_EXECUTED_BASE,
        normalizedExpectedProjectedBalance(.pre_release, .exchange_trade_basic, 200, .taker_base),
    );
    try std.testing.expectEqual(
        @as(i64, 300),
        normalizedExpectedProjectedBalance(.full, .exchange_trade_basic, 300, .maker_quote),
    );
    try std.testing.expectEqual(
        @as(i64, 400),
        normalizedExpectedProjectedBalance(.pre_release, .exchange_trade_bootstrap_long, 400, .taker_base),
    );
}

test "normalized aggregate balance adds projected deltas for pre-release fixtures" {
    try std.testing.expectEqual(
        @as(i64, 1000 + EXTENDED_EXECUTED_QUOTE),
        normalizedExpectedAggregateBalance(.pre_release, .exchange_trade_reports, 1000, .maker_quote),
    );
    try std.testing.expectEqual(
        @as(i64, 2000 + EXTENDED_EXECUTED_BASE),
        normalizedExpectedAggregateBalance(.pre_release, .exchange_trade_reports, 2000, .taker_base),
    );
    try std.testing.expectEqual(
        @as(i64, 3000),
        normalizedExpectedAggregateBalance(.full, .exchange_trade_reports, 3000, .maker_quote),
    );
    try std.testing.expectEqual(
        @as(i64, 4000),
        normalizedExpectedAggregateBalance(.pre_release, .exchange_trade_reports_long, 4000, .taker_base),
    );
}
