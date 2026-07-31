# Browser WebAssembly exchange

This target runs the real in-memory Zig `ExchangeKernel` in a browser. Commands
still follow the production risk -> matching -> risk-release path and use the
same native `CommandResultCode` values as the rest of the Zig port. The browser
adapter is a narrow lifecycle and integer ABI around that kernel; it is not a
second matching-engine implementation.

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
- `index.html` and `app.js` - a small interactive crossing-order demo

`wasm-smoke` compiles the same adapter and runs a funded crossing-order flow in
Node. Node is used only as a portable WebAssembly host for this focused smoke
test; the main Zig migration gate does not depend on Node.

## Run in a browser

WebAssembly loading uses `fetch`, so serve the bundle over HTTP instead of
opening `index.html` as a `file:` URL. For example:

```sh
python3 -m http.server 8000 --directory zig-out/browser
```

Then open <http://127.0.0.1:8000/>. The demo creates an engine, registers a
spot symbol, funds a buyer and seller, crosses their orders, and displays the
resulting L2 book.

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
