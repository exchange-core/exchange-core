# Zig Port Usage Examples

This guide is the practical companion for the Java -> Zig migration under `src/zig`.
All snippets target Zig `0.16.0`.

## 1. Day-to-day commands

Run from repository root:

```sh
export ZIG_GLOBAL_CACHE_DIR=.zig-cache-global
export ZIG_LOCAL_CACHE_DIR=.zig-cache-local

# Unit tests
zig build test

# ReleaseSafe hardening
zig build test-release-safe

# Java fixture parity check
zig build parity-diff

# API parity + fixture parity migration gate
zig build gate

# ExchangeApi public-surface guard
zig build api-surface-check

# Java-vs-Zig ExchangeApi family parity audit artifacts
zig build java-api-parity-audit
```

## 2. Minimal embedded engine lifecycle (ExchangeCore + ExchangeApi)

```zig
const std = @import("std");
const exchange_core = @import("exchange_core");
const core = exchange_core.core;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var engine = try core.exchange_core.ExchangeCore.init(allocator, .{});
    defer engine.deinit();

    var api = core.exchange_api.ExchangeApi.init(allocator, &engine);
    defer api.deinit();

    try api.startup();
    defer api.shutdown() catch {};

    try api.submitCommand(.{ .add_user = 1001 });
    try api.submitCommand(.{
        .adjust_user_balance = .{
            .uid = 1001,
            .transaction_id = 1,
            .currency = 15,
            .amount = 250_000,
            .adjustment_type = .adjustment,
        },
    });

    const balance = try api.queryUserBalance(1001, 15);
    std.debug.assert(balance != null);
    std.debug.print("uid=1001 ccy=15 balance={d}\n", .{balance.?});
}
```

## 3. Exchange API usage: sync + async + order book

```zig
const std = @import("std");
const exchange_core = @import("exchange_core");
const core = exchange_core.core;

fn run(api: *core.exchange_api.ExchangeApi, allocator: std.mem.Allocator, io: std.Io) !void {
    const now_ns: i64 = std.math.lossyCast(i64, std.Io.Clock.real.now(io).nanoseconds);

    const spec = try core.common.CoreSymbolSpecification.init(.{
        .symbol_id = 241,
        .symbol_type = .currency_exchange_pair,
        .base_currency = 11,
        .quote_currency = 15,
        .base_scale_k = 1_000_000,
        .quote_scale_k = 10_000,
        .taker_fee = 1900,
        .maker_fee = 700,
    });
    try api.submitAddSymbolsBinary(5_001, now_ns, &.{spec});

    try api.submitCommandsSync(&.{
        .{ .add_user = 2_001 },
        .{ .add_user = 2_002 },
        .{
            .adjust_user_balance = .{
                .uid = 2_001,
                .transaction_id = 10_001,
                .currency = 15,
                .amount = 1_000_000,
                .adjustment_type = .adjustment,
            },
        },
        .{
            .adjust_user_balance = .{
                .uid = 2_002,
                .transaction_id = 10_002,
                .currency = 11,
                .amount = 1_000_000,
                .adjustment_type = .adjustment,
            },
        },
    });

    const async_handle = try api.submitCommandAsyncResultCode(.{
        .place_order = .{
            .order_id = 9_001,
            .uid = 2_001,
            .symbol = 241,
            .price = 10_500,
            .reserve_bid_price = 10_500,
            .size = 100,
            .action = .bid,
            .order_type = .gtc,
            .user_cookie = 0,
        },
    });
    _ = try api.drainPending();
    const place_result = api.takeResultCode(async_handle) orelse return error.AsyncResultMissing;
    if (place_result != .success) return error.PlaceOrderFailed;

    var snapshot = try api.processOrderBookRequest(241, 16);
    defer snapshot.deinit(allocator);
    std.debug.print("book asks={d} bids={d}\n", .{ snapshot.asks.items.len, snapshot.bids.items.len });
}
```

## 4. Persistence flow: write snapshot + restart from discovered default paths

This is the recommended day-to-day flow for parity work: persist with main-log metadata, then restart from discovered snapshot/journal files.

```zig
const std = @import("std");
const exchange_core = @import("exchange_core");
const core = exchange_core.core;

pub fn persistenceRoundTrip(allocator: std.mem.Allocator, io: std.Io) !void {
    var cfg = core.config.ExchangeCoreConfig{
        .initial_state = core.config.InitialStateConfig.cleanStart("DEMO_EXCHANGE"),
        .serialization = core.config.SerializationConfig.diskJournaling(),
    };
    try cfg.validate();

    var engine = try core.exchange_core.ExchangeCore.init(allocator, cfg);
    defer engine.deinit();
    var api = core.exchange_api.ExchangeApi.init(allocator, &engine);
    defer api.deinit();

    try api.startup();
    defer api.shutdown() catch {};

    try api.submitCommand(.{ .add_user = 3001 });
    const timestamp_ns: i64 = std.math.lossyCast(i64, std.Io.Clock.real.now(io).nanoseconds);
    const persist_result = try api.persistStateWithDefaultMainLog(
        std.Io.Dir.cwd(),
        77,
        timestamp_ns,
        .risk_engine,
        0,
    );
    if (persist_result != .success) return error.PersistStateRejected;

    var restore_cfg = core.config.ExchangeCoreConfig{
        .initial_state = core.config.InitialStateConfig.lastKnownStateFromJournal(
            "DEMO_EXCHANGE",
            1, // non-zero enables snapshot bootstrap policy
            0,
        ),
        .serialization = core.config.SerializationConfig.diskJournaling(),
    };
    try restore_cfg.validate();

    var restored_engine = try core.exchange_core.ExchangeCore.init(allocator, restore_cfg);
    defer restored_engine.deinit();
    var restored_api = core.exchange_api.ExchangeApi.init(allocator, &restored_engine);
    defer restored_api.deinit();

    const outcome = try restored_api.startupFromDiscoveredConfiguredPersistence(
        std.Io.Dir.cwd(),
        .risk_engine,
        0,
        0,
    );
    defer restored_api.shutdown() catch {};

    std.debug.print(
        "snapshot_found={} journal_replayed={d}\n",
        .{ outcome.snapshot_found, outcome.journal_replayed_commands },
    );
}
```

## 5. Java-shaped config + orchestrator (single vs sharded backend)

`ExchangeOrchestrator` chooses backend from Java-style config automatically:
- single backend for `matching_engines_num == 1` (default profile)
- sharded backend for throughput profile (`matching_engines_num > 1`)

```zig
const std = @import("std");
const exchange_core = @import("exchange_core");
const core = exchange_core.core;

pub fn runOrchestrator(allocator: std.mem.Allocator) !void {
    const java_cfg = core.config.JavaExchangeConfig{
        .performance_cfg = core.config.PerformanceConfig.throughputPreset(),
        .serialization_cfg = core.config.SerializationConfig.diskSnapshotOnly(),
    };

    var orchestrator = try core.exchange_orchestrator.ExchangeOrchestrator.initFromJavaConfig(
        allocator,
        java_cfg,
    );
    defer orchestrator.deinit();
    try orchestrator.startup();
    defer orchestrator.shutdown();

    const mode: core.exchange_orchestrator.ExecutionMode = .routed;
    var response = try orchestrator.addUser(mode, 9_800_001);
    defer response.deinit(allocator);

    if (response.result_code != .success) return error.AddUserRejected;
}
```

If you use `.split` mode, ensure backend is sharded (`orchestrator.backendKind() == .sharded`) or you will get `error.SplitModeRequiresShardedBackend`.

## 6. Porting workflow for new Java behavior

1. Generate/update Java parity fixtures in `src/zig/fixtures/` (Maven test).
2. Implement Zig behavior with typed command/report paths and explicit error handling.
3. Add deterministic tests (including seeded perturbation for async/batched paths when relevant).
4. Run `zig build gate`.
5. If parity fails, run `zig build parity-diff -- <fixture-path>` for a focused diff.

Fixture generation entrypoint:

```sh
mvn -Dtest=exchange.core2.tests.fixtures.GenerateJavaParityFixtureTest \
    -Dparity.fixture.write=true \
    -Dparity.fixture.path=src/zig/fixtures/java_user_balance_basic.fixture \
    -Dparity.fixture.control.path=src/zig/fixtures/java_exchange_control.fixture \
    -Dparity.fixture.trade.path=src/zig/fixtures/java_exchange_trade_basic.fixture \
    test
```

## 7. Style and safety references

- Tiger style baseline: `src/zig/TIGER_STYLE_CHECKLIST.md`
- Migration status: `src/zig/MIGRATION_COVERAGE.md`
- Persistence policy: `src/zig/PERSISTENCE_INTEROP_DECISION.md`
- Fixture format/details: `src/zig/fixtures/README.md`
