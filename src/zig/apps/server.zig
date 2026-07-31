const std = @import("std");
const exchange_core = @import("exchange_core");
const protocol = @import("exchange_protocol");

const core = exchange_core.core;
const log = std.log.scoped(.exchange_server);

pub const DefaultHost = "127.0.0.1";
pub const DefaultPort: u16 = 7379;
pub const DefaultSymbolId: i32 = 241;
pub const DefaultBaseCurrency: i32 = 11;
pub const DefaultQuoteCurrency: i32 = 15;

const MaxRequestLineBytes = 64 * 1024;
const BootstrapTransferId: i32 = 1;

const ArgumentError = error{
    InvalidArguments,
    MissingArgumentValue,
    InvalidPort,
};

const RequestMapError = error{
    NonCommandRequest,
};

pub const Options = struct {
    host: []const u8 = DefaultHost,
    port: u16 = DefaultPort,
    owned_host: ?[]u8 = null,

    fn deinit(self: *Options, allocator: std.mem.Allocator) void {
        if (self.owned_host) |owned| allocator.free(owned);
        self.* = undefined;
    }

    fn setHost(
        self: *Options,
        allocator: std.mem.Allocator,
        host: []const u8,
    ) (std.mem.Allocator.Error || ArgumentError)!void {
        if (host.len == 0) return error.InvalidArguments;
        const replacement = try allocator.dupe(u8, host);
        if (self.owned_host) |owned| allocator.free(owned);
        self.owned_host = replacement;
        self.host = replacement;
    }
};

const ParseOutcome = union(enum) {
    help,
    options: Options,
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var parsed = parseOptions(init.minimal.args, allocator) catch |err| {
        try writeArgumentError(init.io, err);
        return err;
    };

    switch (parsed) {
        .help => {
            var stdout_buffer: [2048]u8 = undefined;
            var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
            try writeUsage(&stdout_writer.interface);
            try stdout_writer.interface.flush();
            return;
        },
        .options => |*options| {
            defer options.deinit(allocator);
            try run(init.io, allocator, options.*);
        },
    }
}

fn run(io: std.Io, allocator: std.mem.Allocator, options: Options) !void {
    const engine = try allocator.create(core.exchange_core.ExchangeCore);
    defer allocator.destroy(engine);

    engine.* = try core.exchange_core.ExchangeCore.initWithIo(allocator, io, .{
        .matching = .{ .orderbook_kind = .direct },
    });
    defer engine.deinit();

    var api = core.exchange_api.ExchangeApi.init(allocator, engine);
    defer api.deinit();

    try engine.startup();
    defer engine.shutdown();

    try bootstrapDefaultSymbol(&api, allocator);

    const address = try std.Io.net.IpAddress.resolve(io, options.host, options.port);
    var server = try address.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);

    var stdout_buffer: [2048]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(io, &stdout_buffer);
    try stdout_writer.interface.print(
        "exchange-core server ready on {s}:{d} symbol={d} base={d} quote={d}\n",
        .{
            options.host,
            options.port,
            DefaultSymbolId,
            DefaultBaseCurrency,
            DefaultQuoteCurrency,
        },
    );
    try stdout_writer.interface.flush();

    while (true) {
        const stream = server.accept(io) catch |err| switch (err) {
            error.ConnectionAborted => continue,
            else => return err,
        };
        const shutdown_requested = serveConnection(io, allocator, stream, &api) catch |err| {
            log.warn("client connection failed: {s}", .{@errorName(err)});
            continue;
        };
        if (shutdown_requested) break;
    }
}

fn serveConnection(
    io: std.Io,
    allocator: std.mem.Allocator,
    stream: std.Io.net.Stream,
    api: *core.exchange_api.ExchangeApi,
) !bool {
    defer stream.close(io);

    var read_buffer: [MaxRequestLineBytes]u8 = undefined;
    var write_buffer: [4096]u8 = undefined;
    var stream_reader = stream.reader(io, &read_buffer);
    var stream_writer = stream.writer(io, &write_buffer);
    const reader = &stream_reader.interface;
    const writer = &stream_writer.interface;

    while (true) {
        const line_optional = reader.takeDelimiter('\n') catch |err| switch (err) {
            error.StreamTooLong => {
                const discarded: ?usize = reader.discardDelimiterInclusive('\n') catch |discard_err| switch (discard_err) {
                    error.EndOfStream => null,
                    error.ReadFailed => return error.ReadFailed,
                };
                try writeError(writer, "protocol", "RequestLineTooLong");
                try writer.flush();
                if (discarded == null) return false;
                continue;
            },
            error.ReadFailed => return error.ReadFailed,
        };
        const raw_line = line_optional orelse return false;
        const line = std.mem.trimEnd(u8, raw_line, "\r");
        const request = protocol.parseLine(line) catch |err| {
            try writeError(writer, "protocol", @errorName(err));
            try writer.flush();
            continue;
        };

        const shutdown_requested = executeRequest(allocator, api, request, writer) catch |err| {
            try writeError(writer, "execution", @errorName(err));
            try writer.flush();
            continue;
        };
        try writer.flush();
        if (shutdown_requested) return true;
    }
}

fn executeRequest(
    allocator: std.mem.Allocator,
    api: *core.exchange_api.ExchangeApi,
    request: protocol.Request,
    writer: *std.Io.Writer,
) !bool {
    switch (request) {
        .ping => {
            try writer.print(
                "OK ready={s}\n",
                .{if (api.isStarted() and !api.isStopped()) "true" else "false"},
            );
            return false;
        },
        .balance => |query| {
            try settleReadBarrier(api, allocator);
            const balance = try api.queryUserBalance(query.uid, query.currency);
            try writer.print(
                "OK balance uid={d} currency={d} value=",
                .{ query.uid, query.currency },
            );
            if (balance) |value| {
                try writer.print("{d}\n", .{value});
            } else {
                try writer.writeAll("null\n");
            }
            return false;
        },
        .book => |query| {
            try settleReadBarrier(api, allocator);
            var snapshot = try api.queryOrderBook(query.symbol, query.depth);
            defer snapshot.deinit(allocator);

            try writer.print(
                "OK book symbol={d} asks={d} bids={d} best_ask=",
                .{ query.symbol, snapshot.asks.items.len, snapshot.bids.items.len },
            );
            if (snapshot.asks.items.len == 0) {
                try writer.writeAll("null");
            } else {
                try writer.print("{d}", .{snapshot.asks.items[0].price});
            }
            try writer.writeAll(" best_bid=");
            if (snapshot.bids.items.len == 0) {
                try writer.writeAll("null\n");
            } else {
                try writer.print("{d}\n", .{snapshot.bids.items[0].price});
            }
            return false;
        },
        .shutdown => {
            try writer.writeAll("OK shutdown\n");
            return true;
        },
        else => {
            var result = try api.submitCommandSync(try apiCommandForRequest(request));
            defer result.deinit(allocator);
            try writeCommandResult(writer, result);
            return false;
        },
    }
}

/// A completed matching response can precede the risk-release stage that
/// publishes post-trade balances. A settlement-safe NOP places both balance
/// and order-book reads after every stage of the preceding command stream.
fn settleReadBarrier(
    api: *core.exchange_api.ExchangeApi,
    allocator: std.mem.Allocator,
) !void {
    var result = try api.submitCommandSync(.nop);
    defer result.deinit(allocator);
    const result_code = switch (result) {
        .response => |response| response.result_code,
        .persist_state => return error.UnexpectedReadBarrierResult,
    };
    if (result_code != .success) return error.ReadBarrierRejected;
}

fn apiCommandForRequest(request: protocol.Request) RequestMapError!core.api_command.ApiCommand {
    return switch (request) {
        .add_user => |command| .{ .add_user = command.uid },
        .deposit => |command| .{
            .adjust_user_balance = .{
                .uid = command.uid,
                .transaction_id = command.transaction_id,
                .currency = command.currency,
                .amount = command.amount,
                .adjustment_type = .adjustment,
            },
        },
        .place => |command| .{
            .place_order = .{
                .order_id = command.order_id,
                .uid = command.uid,
                .symbol = command.symbol,
                .price = command.price,
                .reserve_bid_price = command.price,
                .size = command.size,
                .action = switch (command.side) {
                    .bid => .bid,
                    .ask => .ask,
                },
                .order_type = switch (command.time_in_force) {
                    .gtc => .gtc,
                    .ioc => .ioc,
                },
                .user_cookie = 0,
            },
        },
        .move => |command| .{
            .move_order = .{
                .order_id = command.order_id,
                .uid = command.uid,
                .symbol = command.symbol,
                .new_price = command.new_price,
            },
        },
        .reduce => |command| .{
            .reduce_order = .{
                .order_id = command.order_id,
                .uid = command.uid,
                .symbol = command.symbol,
                .reduce_size = command.size,
            },
        },
        .cancel => |command| .{
            .cancel_order = .{
                .order_id = command.order_id,
                .uid = command.uid,
                .symbol = command.symbol,
            },
        },
        .ping, .balance, .book, .shutdown => error.NonCommandRequest,
    };
}

fn writeCommandResult(
    writer: *std.Io.Writer,
    result: core.api_command.ApiCommandResult,
) std.Io.Writer.Error!void {
    switch (result) {
        .response => |response| try writer.print(
            "OK tag=response code={d} sequence={d} events={d}\n",
            .{
                response.result_code.code(),
                response.sequence,
                core.events.listSize(response.matcher_events_head),
            },
        ),
        .persist_state => |result_code| try writer.print(
            "OK tag=persist_state code={d} sequence=0 events=0\n",
            .{result_code.code()},
        ),
    }
}

fn writeError(
    writer: *std.Io.Writer,
    kind: []const u8,
    message: []const u8,
) std.Io.Writer.Error!void {
    try writer.print("ERR kind={s} message={s}\n", .{ kind, message });
}

fn bootstrapDefaultSymbol(
    api: *core.exchange_api.ExchangeApi,
    allocator: std.mem.Allocator,
) !void {
    const symbol = try defaultSymbolSpecification();
    var result = try api.submitCommandSync(.{
        .binary_add_symbols = .{
            .transfer_id = BootstrapTransferId,
            .timestamp = 0,
            .symbols = &.{symbol},
        },
    });
    defer result.deinit(allocator);

    const result_code = switch (result) {
        .response => |response| response.result_code,
        .persist_state => return error.UnexpectedBootstrapResult,
    };
    if (result_code != .success) return error.DefaultSymbolBootstrapRejected;
}

fn defaultSymbolSpecification() core.common.CoreSymbolSpecification.ValidationError!core.common.CoreSymbolSpecification {
    return core.common.CoreSymbolSpecification.init(.{
        .symbol_id = DefaultSymbolId,
        .symbol_type = .currency_exchange_pair,
        .base_currency = DefaultBaseCurrency,
        .quote_currency = DefaultQuoteCurrency,
        .base_scale_k = 100,
        .quote_scale_k = 10,
        .taker_fee = 2,
        .maker_fee = 1,
    });
}

fn parseOptions(
    process_args: std.process.Args,
    allocator: std.mem.Allocator,
) (std.process.Args.Iterator.InitError || std.mem.Allocator.Error || ArgumentError)!ParseOutcome {
    var args = try std.process.Args.Iterator.initAllocator(process_args, allocator);
    defer args.deinit();
    _ = args.next();

    var options: Options = .{};
    errdefer options.deinit(allocator);

    while (args.next()) |argument| {
        if (std.mem.eql(u8, argument, "--help") or std.mem.eql(u8, argument, "-h")) {
            options.deinit(allocator);
            return .help;
        }
        if (std.mem.eql(u8, argument, "--host")) {
            try options.setHost(
                allocator,
                args.next() orelse return error.MissingArgumentValue,
            );
            continue;
        }
        if (std.mem.eql(u8, argument, "--port")) {
            options.port = parsePort(args.next() orelse return error.MissingArgumentValue) catch
                return error.InvalidPort;
            continue;
        }
        return error.InvalidArguments;
    }

    return .{ .options = options };
}

fn parsePort(raw: []const u8) ArgumentError!u16 {
    const port = std.fmt.parseInt(u16, raw, 10) catch return error.InvalidPort;
    if (port == 0) return error.InvalidPort;
    return port;
}

fn writeArgumentError(io: std.Io, err: anyerror) !void {
    var stderr_buffer: [2048]u8 = undefined;
    var stderr_writer = std.Io.File.stderr().writer(io, &stderr_buffer);
    try stderr_writer.interface.print("argument error: {s}\n\n", .{@errorName(err)});
    try writeUsage(&stderr_writer.interface);
    try stderr_writer.interface.flush();
}

fn writeUsage(writer: *std.Io.Writer) std.Io.Writer.Error!void {
    try writer.writeAll(
        \\Usage: exchange-core-server [--host HOST] [--port PORT]
        \\
        \\Starts a newline-delimited TCP exchange server.
        \\
        \\Options:
        \\  --host HOST  Bind host or IP address (default: 127.0.0.1)
        \\  --port PORT  Bind TCP port (default: 7379)
        \\  -h, --help   Show this help
        \\
    );
}

test "server defaults and bootstrap symbol are production-ready" {
    const options: Options = .{};
    try std.testing.expectEqualStrings(DefaultHost, options.host);
    try std.testing.expectEqual(DefaultPort, options.port);

    const symbol = try defaultSymbolSpecification();
    try symbol.validate();
    try std.testing.expectEqual(DefaultSymbolId, symbol.symbol_id);
    try std.testing.expectEqual(DefaultBaseCurrency, symbol.base_currency);
    try std.testing.expectEqual(DefaultQuoteCurrency, symbol.quote_currency);
    try std.testing.expectEqual(@as(i64, 100), symbol.base_scale_k);
    try std.testing.expectEqual(@as(i64, 10), symbol.quote_scale_k);
    try std.testing.expectEqual(@as(i64, 2), symbol.taker_fee);
    try std.testing.expectEqual(@as(i64, 1), symbol.maker_fee);
}

test "server port parser rejects zero, overflow, and non-numeric input" {
    try std.testing.expectEqual(@as(u16, 7379), try parsePort("7379"));
    try std.testing.expectError(error.InvalidPort, parsePort("0"));
    try std.testing.expectError(error.InvalidPort, parsePort("65536"));
    try std.testing.expectError(error.InvalidPort, parsePort("tcp"));
}

test "server maps wire place orders to settlement-safe API commands" {
    const mapped = try apiCommandForRequest(.{ .place = .{
        .order_id = 71,
        .uid = 81,
        .symbol = DefaultSymbolId,
        .price = 1_001,
        .size = 9,
        .side = .bid,
        .time_in_force = .ioc,
    } });
    switch (mapped) {
        .place_order => |command| {
            try std.testing.expectEqual(@as(i64, 71), command.order_id);
            try std.testing.expectEqual(@as(i64, 81), command.uid);
            try std.testing.expectEqual(DefaultSymbolId, command.symbol);
            try std.testing.expectEqual(@as(i64, 1_001), command.reserve_bid_price);
            try std.testing.expectEqual(core.common.OrderAction.bid, command.action);
            try std.testing.expectEqual(core.common.OrderType.ioc, command.order_type);
        },
        else => return error.UnexpectedApiCommand,
    }
}

test "running server core bootstraps symbol and exposes settled balances" {
    const allocator = std.testing.allocator;
    const engine = try allocator.create(core.exchange_core.ExchangeCore);
    defer allocator.destroy(engine);

    engine.* = try core.exchange_core.ExchangeCore.initWithIo(allocator, std.testing.io, .{
        .matching = .{ .orderbook_kind = .direct },
    });
    defer engine.deinit();

    var api = core.exchange_api.ExchangeApi.init(allocator, engine);
    defer api.deinit();
    try engine.startup();
    defer engine.shutdown();

    try bootstrapDefaultSymbol(&api, allocator);
    var snapshot = try api.queryOrderBook(DefaultSymbolId, 20);
    defer snapshot.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), snapshot.asks.items.len);
    try std.testing.expectEqual(@as(usize, 0), snapshot.bids.items.len);

    var add_user = try api.submitCommandSync(.{ .add_user = 1_001 });
    defer add_user.deinit(allocator);
    var deposit = try api.submitCommandSync(.{ .adjust_user_balance = .{
        .uid = 1_001,
        .transaction_id = 1,
        .currency = DefaultQuoteCurrency,
        .amount = 25_000,
        .adjustment_type = .adjustment,
    } });
    defer deposit.deinit(allocator);
    try settleReadBarrier(&api, allocator);
    try std.testing.expectEqual(
        @as(?i64, 25_000),
        try api.queryUserBalance(1_001, DefaultQuoteCurrency),
    );
}
