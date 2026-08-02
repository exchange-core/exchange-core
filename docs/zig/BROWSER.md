# Browser WebAssembly exchange

This target runs the real in-memory Zig `ExchangeKernel` in a browser. The root
route is a manual trading desk where several logical clients submit orders to
one shared order book. The deterministic scenario explorer remains available
at `/scenarios.html`.

Commands still follow the production risk -> matching -> risk-release path and
use the same native `CommandResultCode` values as the rest of the Zig port. The
browser adapter is a narrow lifecycle and integer ABI around that kernel; it is
not a second matching-engine implementation.

## Build and verify

Run both commands from the repository root. The build is deliberately serial,
like the rest of the migration gates:

```sh
zig build -j1 wasm
zig build -j1 wasm-smoke
```

`wasm` stages a self-contained browser bundle under `zig-out/browser/`:

- `exchange-core.wasm` - the `wasm32-freestanding`, single-threaded engine
- `exchange-core.js` - the dependency-free JavaScript wrapper
- `index.html`, `manual-app.js`, and `manual-session.js` - the manual trading desk
- `scenarios.html`, `app.js`, and `scenarios.js` - the deterministic scenario explorer

`wasm-smoke` compiles the same adapter and runs every catalog scenario through
the exact emitted WebAssembly in Node. It also runs an independent multi-client
flow against the shared manual-session logic and keeps the ABI export,
no-import, strict `bigint`, and stale-handle checks around those market flows.
Node is used only as a portable WebAssembly host for this focused smoke test;
the main Zig migration gate does not depend on Node.

## Run in a browser

WebAssembly loading uses `fetch`, so serve the bundle over HTTP instead of
opening `index.html` as a `file:` URL. For example:

```sh
python3 -m http.server 8000 --directory zig-out/browser
```

Then open <http://127.0.0.1:8000/> for the manual trading desk or
<http://127.0.0.1:8000/scenarios.html> for the deterministic explorer.

## Manual multi-client trading desk

The root page owns one synchronous `Exchange` handle and registers multiple
logical client identities in it. All clients therefore trade against the same
browser-local order book. They are not network clients, and separate tabs,
browsers, devices, or processes do not share this state.

This page does not connect to the native TCP server or exercise authenticated
ingress. Browsers cannot speak that raw TCP protocol directly. A production
browser client needs a separate HTTPS/WebSocket gateway that keeps ingress key
files and Parix credentials on the server side. Never place those secrets in
JavaScript, WebAssembly, browser storage, or the staged browser bundle.

The desk starts with Alice, Bob, and Carol (UIDs 1001-1003) and gives every
client initial funding of 1,000 base units and 100,000 quote units. Use **Add
client** to register another logical identity; its form starts with those same
funding amounts but accepts custom non-negative values. The new identity is
immediately available in the order ticket. These figures are bootstrap inputs,
not live balance displays: the browser ABI does not currently expose post-trade
balance queries. Select a client, choose buy (bid) or sell (ask) and GTC or IOC,
enter integer price and quantity values, then submit the order. Bids reserve at
the entered price, while asks do not require a reserve bid price.

Every submission shows the native command result, matcher-event count, and the
refreshed shared L2 asks and bids. The activity log identifies which client sent
each command so crossing orders can be followed across client identities.
Business rejections remain visible result codes rather than JavaScript errors.
Resetting the desk clears the engine and deterministically recreates the symbol,
client roster, and prefunding.

This is a single-tab manual demonstration, not an order-book server. For
concurrent JavaScript callers, put the exchange behind one serialized worker
owner. For cross-device shared state, use an authoritative server and network
transport. Do not let several JavaScript agents call the same wrapper
concurrently.

`manual-session.js` keeps this bootstrap and order-submission logic independent
of the DOM so the browser desk and Node smoke exercise the same contract.
`ManualMarketSession` operates on one supplied exchange handle, registers
clients, assigns monotonic order and transaction IDs, and submits orders. Each
activity entry contains the native result and matcher-event count; snapshots
contain the clients, activity, next order ID, and refreshed L2 book. Prices,
quantities, balances, and IDs remain JavaScript `bigint` values at this
boundary.

## Deterministic scenarios

On `/scenarios.html`, choose and run any catalog scenario:

- **Partial fill** - a smaller IOC bid leaves part of a resting ask
- **Multi-level sweep** - an IOC bid consumes two ask levels in price order
- **Resting spread** - non-crossing GTC orders remain on both sides
- **Move, reduce, cancel** - one resting order completes its full lifecycle
- **Insufficient funds** - risk rejects an underfunded bid before matching

Selecting or resetting a scenario clears the explorer's engine. Running always
resets and bootstraps it again, so repeated runs start from identical state and
produce the same native result codes, matcher-event counts, and L2 snapshot.

## Shared scenario runner

`scenarios.js` has no host-specific or third-party dependencies. The browser UI
and Node smoke import the same frozen catalog and call
`runMarketScenario(exchange, scenarioId)`. Each run resets and bootstraps the
real `ExchangeKernel`, checks every command's exact native result code and
matcher-event count, validates intermediate lifecycle snapshots where useful,
and validates the final L2 book.

The returned frozen summary contains the selected scenario metadata, a status
message, cumulative matcher-event count, final book, and structured command-log
entries. An expected business rejection such as `riskNsf (-2001)` is recorded
as its frozen `{ code, name, ok }` result rather than converted into an ABI
failure.

## JavaScript wrapper

The public wrapper owns both the WebAssembly instance and one opaque engine
handle. Create it with the module loader, close it when finished, and treat it
as an exclusive-owner synchronous object:

```js
import {
  BalanceAdjustmentType,
  EngineKind,
  Exchange,
  OrderAction,
  OrderType,
  SymbolType,
  assertCommandSuccess,
  instantiate,
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
  console.log(book.asks, book.bids);
} finally {
  exchange.close();
}
```

The checked-in demo is also an executable reference for the complete public
surface. For callers that need to create several engine handles from one
WebAssembly instance, the lower-level API is
`const runtime = await instantiate(url)` followed by
`runtime.create(EngineKind.direct)`.

All values represented by Zig `i64` must be JavaScript `bigint`: user and order
IDs, transaction IDs, prices, reserve prices, quantities, scale factors, fees,
margins, and balance adjustments. The wrapper rejects a `number` in those
positions and rejects values outside the signed 64-bit range. Currency IDs,
symbol IDs, enum choices, depth, cookies, status values, event counts, and
native command result codes are ordinary JavaScript numbers.
Use the exported numeric enum objects (`SymbolType`, `BalanceAdjustmentType`,
`OrderAction`, and `OrderType`); string enum spellings are rejected.

A successful ABI call does not imply that an exchange command was accepted.
The wrapper throws for ABI misuse or a faulted engine, but mutating methods
return a frozen `{ code, name, ok }` object containing the native signed
`CommandResultCode`. `100` is `SUCCESS`; other codes are business outcomes such
as duplicate users, insufficient funds, or an unknown order. Call
`assertCommandSuccess(result, operation)` when a rejection should become an
`ExchangeCommandError`.

## Ownership and execution model

The low-level kernel is synchronous and unsynchronized. Do not call the same
wrapper concurrently or share it across JavaScript agents. A single browser
event loop already serializes calls, but matching a large command stream on the
main thread can still block rendering. For production browser applications,
instantiate and own the wrapper inside one dedicated Web Worker and communicate
with it using messages.

Each engine is heap-allocated at a stable address and exposed only through an
opaque, monotonically assigned handle. Handles are never reused. `close()`
destroys the kernel and invalidates the handle; further operations fail instead
of accessing another engine.

If an internal `ExchangeKernel` execution error escapes, the adapter marks that
engine faulted. Subsequent engine operations fail until `close()` because the
failed command may already have advanced sequence or partially mutated state.
Normal exchange rejections remain native command result codes and do not fault
the engine.

## Versioned integer ABI

The ABI exports only fixed-width integers and WebAssembly memory. It does not
expose Zig structs, slices, allocator pointers, enum layouts, or error ordinals.
ABI version 1 uses these exports:

```text
exchange_abi_version
exchange_create / exchange_destroy / exchange_reset
exchange_add_symbol
exchange_add_user / exchange_adjust_balance
exchange_place_order / exchange_move_order
exchange_cancel_order / exchange_reduce_order
exchange_query_order_book
exchange_last_event_count
exchange_order_book_ptr / exchange_order_book_len
```

Except for the version and destroy calls, results are packed into an unsigned
64-bit integer:

```text
bits 63..32  ABI status (u32)
bits 31..0   opaque handle, value, or native signed CommandResultCode
```

The stable ABI statuses are:

| Value | Status |
| ---: | --- |
| 0 | `ok` |
| 1 | `invalid_handle` |
| 2 | `out_of_memory` |
| 3 | `invalid_argument` |
| 4 | `invalid_enum` |
| 5 | `instance_limit` |
| 6 | `handle_exhausted` |
| 7 | `faulted` |
| 255 | `internal` |

ABI statuses and native exchange result codes are separate namespaces. The
JavaScript wrapper decodes both and sign-extends the low 32 bits for command
results.

## L2 order-book wire format

`exchange_query_order_book` uses the kernel's read-only report path; it does
not consume a command sequence. On success, `exchange_order_book_ptr` and
`exchange_order_book_len` identify an instance-owned little-endian byte buffer
in exported WebAssembly memory.

The 16-byte `XCL2` header is:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 4 | ASCII magic `XCL2` |
| 4 | 2 | schema version, currently `1` |
| 6 | 2 | level size, currently `24` |
| 8 | 4 | ask count |
| 12 | 4 | bid count |

Ask records come first, followed by bid records. Every 24-byte level is:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | signed `i64` price |
| 8 | 8 | signed `i64` volume |
| 16 | 4 | unsigned order count |
| 20 | 4 | reserved, must be zero |

The buffer is replaced by the next order-book query and released when the
engine is destroyed. JavaScript must recreate its `DataView` after any call
that can allocate because WebAssembly memory growth replaces the underlying
`ArrayBuffer`. The wrapper performs these checks and returns prices and volumes
as `bigint`.

## Scope and non-goals

The browser target deliberately includes the in-memory single-owner
`ExchangeKernel`: symbol admission, user and balance management, risk checks,
naive or direct order books, matching, settlement, reset, matcher-event count,
and read-only L2 queries.

It does not compile the full `ExchangeCore` runtime. In particular, this target
does not provide:

- filesystem persistence, journaling, snapshots, or Chronicle/LZ4 archive I/O
- worker-thread lifecycle, persistent-grouped processing, or shared-memory
  WebAssembly threads
- matching-engine/risk-engine sharding or `ExchangeOrchestrator`

A future browser persistence layer should export/import versioned bytes and let
JavaScript store them in IndexedDB. It should not emulate synchronous filesystem
access inside the matching kernel.
