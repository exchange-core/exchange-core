import { readFile } from "node:fs/promises";

import {
  ABI_VERSION,
  CommandResultCode,
  EngineKind,
  ExchangeDestroyedError,
  instantiate,
} from "./exchange-core.js";
import {
  runMarketScenario,
  scenarioCatalog,
} from "./scenarios.js";

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
invariant(Object.isFrozen(scenarioCatalog), "scenario catalog must be frozen");
invariant(scenarioCatalog.length === 5, "expected five market scenarios");
invariant(
  new Set(scenarioCatalog.map(({ id }) => id)).size === scenarioCatalog.length,
  "scenario ids must be unique",
);
for (const scenario of scenarioCatalog) {
  invariant(Object.isFrozen(scenario), `${scenario.id} metadata must be frozen`);
  invariant(scenario.metrics.length === 3, `${scenario.id} must expose three metrics`);
  invariant(Object.isFrozen(scenario.metrics), `${scenario.id} metrics must be frozen`);
}

const expectedScenarioSummaries = new Map([
  ["partial-fill", { eventCount: 1, book: "asks=100/6/1;bids=" }],
  ["multi-level-sweep", { eventCount: 2, book: "asks=101/1/1;bids=" }],
  ["resting-spread", { eventCount: 0, book: "asks=105/5/1;bids=100/4/1" }],
  ["order-lifecycle", { eventCount: 2, book: "asks=;bids=" }],
  ["insufficient-funds", { eventCount: 0, book: "asks=;bids=" }],
]);
invariant(
  expectedScenarioSummaries.size === scenarioCatalog.length,
  "smoke expectation matrix does not cover the catalog",
);

function bookSignature(book) {
  const sideSignature = (side) => side
    .map(({ price, volume, orders }) => `${price}/${volume}/${orders}`)
    .join(",");
  return `asks=${sideSignature(book.asks)};bids=${sideSignature(book.bids)}`;
}

const exchange = core.create(EngineKind.direct);
const summaries = [];

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

  for (const scenario of scenarioCatalog) {
    const summary = runMarketScenario(exchange, scenario.id);
    const expected = expectedScenarioSummaries.get(scenario.id);
    invariant(expected !== undefined, `${scenario.id} has no smoke expectation`);
    invariant(Object.isFrozen(summary), `${scenario.id} summary must be frozen`);
    invariant(summary.scenario === scenario, `${scenario.id} metadata identity changed`);
    invariant(typeof summary.status === "string", `${scenario.id} status is missing`);
    invariant(
      summary.eventCount === expected.eventCount,
      `${scenario.id} total matcher event count changed`,
    );
    invariant(
      bookSignature(summary.book) === expected.book,
      `${scenario.id} final L2 book changed`,
    );
    invariant(Object.isFrozen(summary.book), `${scenario.id} book must be frozen`);
    invariant(Object.isFrozen(summary.logEntries), `${scenario.id} log must be frozen`);
    invariant(summary.logEntries.length > 0, `${scenario.id} command log is empty`);
    for (const entry of summary.logEntries) {
      invariant(Object.isFrozen(entry), `${scenario.id} log entry must be frozen`);
      invariant(Object.isFrozen(entry.result), `${scenario.id} result must be frozen`);
      invariant(
        entry.result.code === entry.expectedCode,
        `${scenario.id} recorded an unexpected command result`,
      );
      invariant(
        entry.eventCount === entry.expectedEventCount,
        `${scenario.id} recorded an unexpected matcher event count`,
      );
    }
    summaries.push(summary);
    console.log(
      `PASS scenario=${scenario.id} events=${summary.eventCount} asks=${summary.book.asks.length} bids=${summary.book.bids.length}`,
    );
  }

  const rejectionSummary = summaries.find(
    ({ scenario }) => scenario.id === "insufficient-funds",
  );
  invariant(rejectionSummary !== undefined, "insufficient-funds scenario did not run");
  invariant(
    rejectionSummary.logEntries.some(
      ({ result }) => result.code === CommandResultCode.riskNsf && result.ok === false,
    ),
    "insufficient-funds scenario did not preserve riskNsf",
  );
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
  `PASS wasm-browser abi=${core.abiVersion} scenarios=${summaries.length} total-events=${summaries.reduce((total, summary) => total + summary.eventCount, 0)}`,
);
