# Zig Port Usage Examples

This guide is the practical companion for the Java -> Zig migration under `src/zig`.
All snippets target Zig `0.16.0`.

## 1. Standalone TCP quickstart

The repository builds two runnable programs:

- `exchange-core-server` starts an in-memory exchange and listens for commands.
- `exchange-core-client` sends one command and prints the server's one-line response.

They speak a small newline-delimited line protocol. Without Parix settlement,
the server runs in an unauthenticated loopback-only development mode intended
for local exploration of the port; it is not a FIX session or REST API.

Build both binaries from the repository root. A plain build installs them in
`zig-out/bin`:

```sh
zig build
```

The server initializes symbol `241` as a currency-exchange pair with base
currency `11` and quote currency `15`. Start it in the first terminal:

```sh
./zig-out/bin/exchange-core-server
# equivalent: zig build run-server
```

It binds to `127.0.0.1:7379` by default, rejects non-loopback hosts, and remains
in the foreground. In a second terminal, run a complete development sequence:

```sh
CLIENT=./zig-out/bin/exchange-core-client

$CLIENT ping
$CLIENT add-user 1001
$CLIENT add-user 1002
$CLIENT deposit 1001 15 20000000 1
$CLIENT deposit 1002 11 1000000 2
$CLIENT place 9001 1001 241 10500 100 bid gtc
$CLIENT place 9002 1002 241 10500 100 ask gtc
$CLIENT balance 1001 15
$CLIENT balance 1002 11
$CLIENT book 241 20
```

Each request receives exactly one newline-delimited response. `ping`, `balance`,
`book`, and `shutdown` return an `OK` record with command-specific fields.
Exchange commands return this record shape:

```text
OK tag=response code=CODE sequence=SEQUENCE events=COUNT
```

Callers must inspect the numeric exchange result `code` (`100` is success),
because an engine rejection is still a successfully processed protocol request.
Malformed requests and execution failures return
`ERR kind=KIND message=ERROR_NAME`. Client-side argument, address, connection,
read, and write failures are printed to stderr. A bad request does not stop the
listening server.

The full command set is:

```text
ping
add-user UID
deposit UID CURRENCY AMOUNT TRANSACTION_ID
fund-deposit UID CURRENCY AMOUNT OPERATION_UUID
fund-withdraw UID CURRENCY AMOUNT OPERATION_UUID
funding-status OPERATION_UUID
clearing-status PAYMENT_ID
clearing-operations
place ORDER_ID UID SYMBOL PRICE SIZE bid|ask [gtc|ioc]
move ORDER_ID UID SYMBOL NEW_PRICE
reduce ORDER_ID UID SYMBOL SIZE
cancel ORDER_ID UID SYMBOL
balance UID CURRENCY
book SYMBOL [DEPTH]
shutdown
```

In authenticated Parix mode, traders may use `ping`, `book`, and the
UID-bearing `balance`, `place`, `move`, `reduce`, and `cancel` commands only for
their profile-bound UID. Operators may use `ping`, `book`, `shutdown`,
`fund-deposit`, `fund-withdraw`, and `funding-status`. The legacy `add-user`
and direct `deposit` commands are denied to every authenticated principal.
When payment clearing is enabled, operators may also use the read-only
`clearing-status PAYMENT_ID` and `clearing-operations`; traders may not.

`place` defaults to `gtc`; `book` defaults to depth `20` and accepts at most
`1000`. Use the same `--host HOST --port PORT` options on both programs when
changing the endpoint:

```sh
./zig-out/bin/exchange-core-server --host 127.0.0.1 --port 7400
./zig-out/bin/exchange-core-client --host 127.0.0.1 --port 7400 ping
```

For built-in help, run either binary with `--help`, or forward arguments through
the build steps:

```sh
zig build run-server -- --help
zig build run-client -- --help
```

Shut down gracefully from the client terminal. The server sends its response,
stops accepting connections, and then shuts down the exchange core:

```sh
$CLIENT shutdown
```

### Authenticated Parix ingress

Parix settlement requires `--parix-profile` and `--ingress-auth-profile`
together. The ingress profile is non-secret: it maps credential IDs to
trader/operator principals and names environment variables containing paths to
owner-only (`0600`) files containing 32-byte keys as 64 lowercase hexadecimal
characters, with an optional final LF. See
[`AUTHENTICATED_INGRESS_RUNBOOK.md`](AUTHENTICATED_INGRESS_RUNBOOK.md) for the
strict schema, key generation, TLS proxy topology, rotation, and incident
procedure.

Start the paired server:

```sh
zig build run-server -- \
  --parix-settlement \
  --parix-profile path/to/operator-settlement-profile.json \
  --ingress-auth-profile path/to/operator-ingress-profile.json \
  --parix-funding-initialize \
  --persistence-root zig-out/runtime/parix
```

`--parix-funding-initialize` is required only for the proven first creation of
the local `FSG1` store. It requires the file to be absent and rejects an
existing store. Remove it for every restart; never use it to replace missing or
rolled-back recovery evidence.

The client identifies the expected instance and credential on the command line,
but reads the key-file path only from `EXCHANGE_INGRESS_KEY_FILE`. Supply both
flags and a non-empty variable together; supplying only part of that triad is a
client configuration error. Omit all three only for loopback development mode:

```sh
EXCHANGE_INGRESS_KEY_FILE=/run/secrets/exchange-ingress-trader-1001.key \
  zig build run-client -- \
    --auth-instance 0e46f90e-c38b-487a-b9f9-630f7839cc3f \
    --auth-credential trader-1001-a \
    balance 1001 11
```

Each authenticated connection performs a challenge-response exchange, sends
one signed command with sequence `1`, verifies one signed response, and closes.
The client prints only the inner `OK` or `ERR` response, preserving one-line
shell usage. Handshake, frame, and MAC failures exit nonzero; a verified signed
inner `ERR` exits zero, so scripts must inspect the printed payload. One
five-second absolute deadline covers authenticated connection setup through
verification of the signed response; expiry prints no response and exits
nonzero.

Authentication and signed frames do not encrypt the command or response. The
native listener remains plaintext and loopback-only. Remote clients require an
operator-owned TLS/mTLS L4 proxy plus a TLS-capable tunnel or client; the bundled
`exchange-core-client` does not negotiate TLS itself. Never expose port `7379`
directly.

### Durable operator funding

The following is protocol syntax, not a recommendation to mutate the shared
development database:

```text
fund-deposit 1001 11 500 1d0ce5c2-91bb-4d3b-8e54-55e266fe44cb
fund-withdraw 1001 11 100 4d33b92b-b62d-4e51-9770-6c77f0141f60
funding-status 1d0ce5c2-91bb-4d3b-8e54-55e266fe44cb
```

Run mutation commands only as an authenticated operator against a dedicated
database and a retained persistence root whose identities have already passed
validate-only startup. The operation UUID is the idempotency key: an exact
duplicate returns the existing operation, while the same UUID with different
UID, currency, amount, or direction is rejected. Poll `funding-status` until
the durable state is `completed`, `failed`, or `quarantined`; `accepted` and
`pending` are non-terminal. A quarantined operation requires the reconciliation
procedure in [`PARIX_SETTLEMENT_RUNBOOK.md`](PARIX_SETTLEMENT_RUNBOOK.md), not a
new UUID or a direct local `deposit`.

Funding remains native and server-side. The browser/WASM desk has no funding
commands, network transport, Parix credential, or ingress key.

## 2. Browser WebAssembly quickstart

Build the browser bundle and run its funded crossing-order smoke test from the
repository root:

```sh
zig build -j1 wasm
zig build -j1 wasm-smoke
```

The first command stages `exchange-core.wasm`, its dependency-free JavaScript
wrapper, and the demo page in `zig-out/browser/`. Serve that directory instead
of opening the page as a `file:` URL:

```sh
python3 -m http.server 8000 --directory zig-out/browser
```

Open <http://127.0.0.1:8000/>. The page drives the real single-threaded
`ExchangeKernel`, including risk checks, matching, settlement, and read-only L2
queries.

The wrapper is an exclusive-owner synchronous API. Instantiate it in one Web
Worker for production applications so a large command stream cannot block the
UI, and never call or share one instance concurrently. Every Zig `i64` argument
and result is a JavaScript `bigint`; the wrapper rejects `number` values and
out-of-range BigInts rather than silently losing precision:

```js
import {
  BalanceAdjustmentType,
  EngineKind,
  Exchange,
  OrderAction,
  OrderType,
  SymbolType,
  assertCommandSuccess,
} from "./exchange-core.js";

const exchange = await Exchange.create("./exchange-core.wasm", {
  kind: EngineKind.direct,
});
try {
  assertCommandSuccess(exchange.addSymbol({
    symbolId: 241,
    symbolType: SymbolType.currencyExchangePair,
    baseCurrency: 11,
    quoteCurrency: 15,
    baseScaleK: 1_000_000n,
    quoteScaleK: 10_000n,
    takerFee: 1_900n,
    makerFee: 700n,
    marginBuy: 0n,
    marginSell: 0n,
  }), "add symbol");

  assertCommandSuccess(exchange.addUser(1_001n), "add user");
  assertCommandSuccess(exchange.adjustBalance({
    uid: 1_001n,
    transactionId: 1n,
    currency: 15,
    amount: 20_000_000n,
    adjustmentType: BalanceAdjustmentType.adjustment,
  }), "adjust balance");

  const placeResult = exchange.placeOrder({
    orderId: 9_001n,
    uid: 1_001n,
    symbol: 241,
    price: 10_500n,
    reserveBidPrice: 10_500n,
    size: 100n,
    action: OrderAction.bid,
    orderType: OrderType.gtc,
    userCookie: 0,
  });
  assertCommandSuccess(placeResult, "place order");

  const book = exchange.queryOrderBook(241, 20);
  console.log(book.bids);
} finally {
  exchange.close();
}
```

Mutating calls return a frozen `{ code, name, ok }` object; `result.code === 100`
and `result.ok === true` mean the native exchange result is `SUCCESS`. Other
command result codes are business outcomes rather than ABI failures. See
`docs/zig/BROWSER.md` for symbol creation, the full wrapper surface,
opaque-handle lifecycle, packed ABI status values, the versioned little-endian
`XCL2` order-book format, and target limitations.

The WebAssembly build intentionally excludes filesystem
persistence/journaling/snapshots and the worker-thread/sharded `ExchangeCore`
runtime. Persist browser state in a JavaScript-owned store only after a future
versioned byte import/export capability is added.

## 3. Day-to-day commands

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

## 4. Minimal embedded engine lifecycle (ExchangeCore + ExchangeApi)

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

## 5. Exchange API usage: sync + async + order book

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

## 6. Persistence flow: write snapshot + restart from discovered default paths

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

## 7. Java-shaped config + orchestrator (single vs sharded backend)

`ExchangeOrchestrator` chooses backend from Java-style config automatically:
- the default `persistent_grouped` pipeline uses the sharded backend, including
  the Java-default 1ME/1RE profile
- explicit `sharded_pipeline_mode = .immediate` retains the single backend for
  a 1ME/1RE compatibility profile
- multi-engine profiles use the sharded backend in either pipeline mode

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

The promoted default supports `.split` because it selects the sharded backend.
An explicit `.immediate` 1ME/1RE compatibility configuration selects the
single backend and returns `error.SplitModeRequiresShardedBackend` for `.split`.

## 8. Porting workflow for new Java behavior

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

## 9. Style and safety references

- Tiger style baseline: `docs/zig/TIGER_STYLE_CHECKLIST.md`
- Migration status: `docs/zig/MIGRATION_COVERAGE.md`
- Persistence policy: `docs/zig/PERSISTENCE_INTEROP_DECISION.md`
- Fixture format/details: `docs/zig/FIXTURES.md`
