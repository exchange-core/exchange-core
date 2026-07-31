import { readFile } from "node:fs/promises";

import {
  ABI_VERSION,
  CommandResultCode,
  EngineKind,
  ExchangeDestroyedError,
  OrderAction,
  OrderType,
  SymbolType,
  assertCommandSuccess,
  instantiate,
} from "./exchange-core.js";

const wasmPath = process.argv[2];
if (!wasmPath) {
  throw new Error("usage: node smoke.mjs <exchange-core.wasm>");
}

function invariant(condition, message) {
  if (!condition) {
    throw new Error(`smoke assertion failed: ${message}`);
  }
}

const bytes = await readFile(wasmPath);
invariant(WebAssembly.validate(bytes), "WebAssembly.validate rejected the module");
const compiledModule = new WebAssembly.Module(bytes);
invariant(
  WebAssembly.Module.imports(compiledModule).length === 0,
  "browser module must not require imports",
);
const exportNames = new Set(
  WebAssembly.Module.exports(compiledModule).map(({ name }) => name),
);
for (const requiredExport of [
  "memory",
  "exchange_abi_version",
  "exchange_create",
  "exchange_destroy",
  "exchange_reset",
  "exchange_add_symbol",
  "exchange_add_user",
  "exchange_adjust_balance",
  "exchange_place_order",
  "exchange_move_order",
  "exchange_cancel_order",
  "exchange_reduce_order",
  "exchange_query_order_book",
  "exchange_order_book_ptr",
  "exchange_order_book_len",
  "exchange_last_event_count",
]) {
  invariant(exportNames.has(requiredExport), `missing export ${requiredExport}`);
}

const core = await instantiate(bytes);
invariant(core.abiVersion === ABI_VERSION, "ABI version mismatch");

const exchange = core.create(EngineKind.direct);
let crossingResult;
let book;
let eventCount;

try {
  let rejectedNumberI64 = false;
  try {
    exchange.addUser(1001);
  } catch (error) {
    rejectedNumberI64 = error instanceof TypeError;
  }
  invariant(rejectedNumberI64, "numeric i64 argument was not rejected");

  let rejectedOutOfRangeI64 = false;
  try {
    exchange.addUser(1n << 63n);
  } catch (error) {
    rejectedOutOfRangeI64 = error instanceof RangeError;
  }
  invariant(rejectedOutOfRangeI64, "out-of-range i64 argument was not rejected");

  assertCommandSuccess(exchange.addSymbol({
    symbolId: 241,
    symbolType: SymbolType.currencyExchangePair,
    baseCurrency: 11,
    quoteCurrency: 15,
    baseScaleK: 1n,
    quoteScaleK: 1n,
  }), "add symbol");
  assertCommandSuccess(exchange.addUser(1001n), "add seller");
  assertCommandSuccess(exchange.addUser(1002n), "add buyer");
  assertCommandSuccess(exchange.adjustBalance({
    uid: 1001n,
    transactionId: 1n,
    currency: 11,
    amount: 1_000n,
  }), "deposit seller base");
  assertCommandSuccess(exchange.adjustBalance({
    uid: 1002n,
    transactionId: 2n,
    currency: 15,
    amount: 100_000n,
  }), "deposit buyer quote");

  const restingResult = exchange.placeOrder({
    orderId: 10_001n,
    uid: 1001n,
    symbol: 241,
    price: 100n,
    size: 10n,
    action: OrderAction.ask,
    orderType: OrderType.gtc,
  });
  assertCommandSuccess(restingResult, "place resting ask");

  crossingResult = exchange.placeOrder({
    orderId: 10_002n,
    uid: 1002n,
    symbol: 241,
    price: 105n,
    reserveBidPrice: 105n,
    size: 4n,
    action: OrderAction.bid,
    orderType: OrderType.ioc,
  });
  assertCommandSuccess(crossingResult, "place crossing bid");
  invariant(
    crossingResult.code === CommandResultCode.success,
    `crossing result was ${crossingResult.name}`,
  );

  eventCount = exchange.lastEventCount();
  invariant(eventCount > 0, "crossing order produced no matcher events");

  book = exchange.orderBook(241, 16);
  invariant(book.asks.length === 1, "expected one remaining ask level");
  invariant(book.bids.length === 0, "IOC bid should not remain in the book");
  invariant(book.asks[0].price === 100n, "remaining ask price mismatch");
  invariant(book.asks[0].volume === 6n, "remaining ask volume mismatch");
  invariant(book.asks[0].orders === 1, "remaining ask order count mismatch");

  assertCommandSuccess(exchange.clear(), "reset exchange");
} finally {
  exchange.destroy();
}

let rejectedStaleHandle = false;
try {
  exchange.addUser(1003n);
} catch (error) {
  rejectedStaleHandle = error instanceof ExchangeDestroyedError;
}
invariant(rejectedStaleHandle, "wrapper allowed use after destroy");

console.log(
  `PASS wasm-browser abi=${core.abiVersion} result=${crossingResult.name} events=${eventCount} asks=${book.asks.length} bids=${book.bids.length}`,
);
