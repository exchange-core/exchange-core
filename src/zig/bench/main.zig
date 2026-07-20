const std = @import("std");
const core = @import("exchange_core").core;

const common = core.common;
const cmd = core.cmd;
const kernel_mod = core.exchange_kernel;

const BenchError = kernel_mod.InitError ||
    kernel_mod.ExecuteError ||
    kernel_mod.ReportError ||
    core.risk.SymbolSpecificationProvider.AddSymbolError ||
    common.CoreSymbolSpecification.ValidationError ||
    error{
        UnexpectedResult,
        InvalidArgs,
        TimerUnsupported,
    };

const BenchResult = struct {
    kind: kernel_mod.OrderBookKind,
    commands: usize,
    elapsed_ns: u64,
    throughput_cmd_per_s: f64,
    risk_hash: u64,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const rounds = try parseRoundsArg(allocator);

    const naive_result = try runBenchmark(allocator, .naive, rounds);
    const direct_result = try runBenchmark(allocator, .direct, rounds);

    const stdout = std.fs.File.stdout();
    var out_buffer: [4096]u8 = undefined;
    var out_writer = stdout.writer(&out_buffer);
    const out = &out_writer.interface;

    try out.print("exchange-core zig bench (rounds={d})\n", .{rounds});
    try out.writeAll("kind     commands    elapsed_ms    throughput_cmd_per_s    risk_hash\n");
    try printResult(out, naive_result);
    try printResult(out, direct_result);
    try out.flush();
}

fn parseRoundsArg(allocator: std.mem.Allocator) BenchError!usize {
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.next();
    const raw = args.next() orelse return 50_000;
    return std.fmt.parseInt(usize, raw, 10) catch return error.InvalidArgs;
}

fn printResult(writer: anytype, result: BenchResult) !void {
    const kind_name = switch (result.kind) {
        .naive => "naive ",
        .direct => "direct",
    };
    const elapsed_ms = @as(f64, @floatFromInt(result.elapsed_ns)) / 1_000_000.0;
    try writer.print(
        "{s}  {d:>8}    {d:>10.3}    {d:>18.2}    {x:0>16}\n",
        .{
            kind_name,
            result.commands,
            elapsed_ms,
            result.throughput_cmd_per_s,
            result.risk_hash,
        },
    );
}

fn runBenchmark(
    allocator: std.mem.Allocator,
    kind: kernel_mod.OrderBookKind,
    rounds: usize,
) BenchError!BenchResult {
    var kernel = try kernel_mod.ExchangeKernel.init(allocator, .{
        .orderbook_kind = kind,
    });
    defer kernel.deinit();
    if (!(try kernel.addSymbol(try makeExchangeSpec()))) {
        return error.UnexpectedResult;
    }

    try runSetupPhase(allocator, &kernel);

    var timer = try std.time.Timer.start();

    var commands: usize = 0;
    var order_id_counter: i64 = 1_000_000;
    var i: usize = 0;
    while (i < rounds) : (i += 1) {
        const base_price = 100 + @as(i64, @intCast(i % 4));
        const size = 1 + @as(i64, @intCast(i % 3));

        var bid = cmd.OrderCommand.newOrder(.gtc, order_id_counter, 50_001, base_price, base_price, size, .bid);
        bid.symbol = 241;
        order_id_counter += 1;
        var bid_resp = try kernel.execute(&bid);
        bid_resp.deinit(allocator);
        if (bid_resp.result_code != .success) return error.UnexpectedResult;
        commands += 1;

        var ask = cmd.OrderCommand.newOrder(.gtc, order_id_counter, 50_002, base_price, 0, size, .ask);
        ask.symbol = 241;
        order_id_counter += 1;
        var ask_resp = try kernel.execute(&ask);
        ask_resp.deinit(allocator);
        if (ask_resp.result_code != .success) return error.UnexpectedResult;
        commands += 1;

        if ((i & 1023) == 1023) {
            var request: cmd.OrderCommand = .{
                .command = .order_book_request,
                .symbol = 241,
                .size = 10,
                .result_code = .new,
            };
            var req_resp = try kernel.execute(&request);
            req_resp.deinit(allocator);
            if (req_resp.result_code != .success) return error.UnexpectedResult;
            commands += 1;
        }
    }

    const elapsed_ns = timer.read();
    const elapsed_sec_raw = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0;
    const elapsed_sec = if (elapsed_sec_raw <= 0.0) 0.000_000_001 else elapsed_sec_raw;

    var hash_report = try kernel.queryReport(.risk_state_hash);
    defer hash_report.deinit(allocator);

    return .{
        .kind = kind,
        .commands = commands,
        .elapsed_ns = elapsed_ns,
        .throughput_cmd_per_s = @as(f64, @floatFromInt(commands)) / elapsed_sec,
        .risk_hash = hash_report.risk_state_hash,
    };
}

fn runSetupPhase(
    allocator: std.mem.Allocator,
    kernel: *kernel_mod.ExchangeKernel,
) BenchError!void {
    var add_u1: cmd.OrderCommand = .{
        .command = .add_user,
        .uid = 50_001,
        .result_code = .new,
    };
    var add_u2: cmd.OrderCommand = .{
        .command = .add_user,
        .uid = 50_002,
        .result_code = .new,
    };
    var dep_quote: cmd.OrderCommand = .{
        .command = .balance_adjustment,
        .uid = 50_001,
        .symbol = 15,
        .price = 10_000_000_000,
        .order_id = 1,
        .order_type = .gtc,
        .result_code = .new,
    };
    var dep_base: cmd.OrderCommand = .{
        .command = .balance_adjustment,
        .uid = 50_002,
        .symbol = 11,
        .price = 10_000_000_000,
        .order_id = 1,
        .order_type = .gtc,
        .result_code = .new,
    };

    var r1 = try kernel.execute(&add_u1);
    defer r1.deinit(allocator);
    var r2 = try kernel.execute(&add_u2);
    defer r2.deinit(allocator);
    var r3 = try kernel.execute(&dep_quote);
    defer r3.deinit(allocator);
    var r4 = try kernel.execute(&dep_base);
    defer r4.deinit(allocator);

    if (r1.result_code != .success or
        r2.result_code != .success or
        r3.result_code != .success or
        r4.result_code != .success)
    {
        return error.UnexpectedResult;
    }
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
