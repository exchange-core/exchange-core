import {
  CommandResultCode,
  OrderAction,
  OrderType,
  SymbolType,
} from "./exchange-core.js";

export const MARKET_SYMBOL_ID = 241;

const BOOK_DEPTH = 16;
const BASE_CURRENCY = 11;
const QUOTE_CURRENCY = 15;
const SELLER_UID = 1_001n;
const BUYER_UID = 1_002n;

function level(price, volume, orders = 1) {
  return Object.freeze({ price, volume, orders });
}

function book(asks = [], bids = []) {
  return Object.freeze({
    asks: Object.freeze(asks),
    bids: Object.freeze(bids),
  });
}

const EMPTY_BOOK = book();

function operation(
  label,
  method,
  args,
  expectedCode = CommandResultCode.success,
  expectedEventCount = 0,
  expectedBook,
) {
  return Object.freeze({
    label,
    method,
    args: args === undefined ? undefined : Object.freeze(args),
    expectedCode,
    expectedEventCount,
    expectedBook,
  });
}

function addUser(label, uid) {
  return operation(label, "addUser", uid);
}

function adjustBalance(label, uid, transactionId, currency, amount) {
  return operation(label, "adjustBalance", {
    uid,
    transactionId,
    currency,
    amount,
  });
}

const RESET = operation("reset exchange", "clear");
const ADD_SYMBOL = operation("add symbol 241", "addSymbol", {
  symbolId: MARKET_SYMBOL_ID,
  symbolType: SymbolType.currencyExchangePair,
  baseCurrency: BASE_CURRENCY,
  quoteCurrency: QUOTE_CURRENCY,
  baseScaleK: 1n,
  quoteScaleK: 1n,
});

function metadata(id, title, description, metrics) {
  if (metrics.length !== 3) {
    throw new Error(`scenario ${id} must define exactly three metrics`);
  }
  return Object.freeze({
    id,
    title,
    description,
    metrics: Object.freeze(
      metrics.map(([labelText, value]) => Object.freeze({
        label: labelText,
        value,
      })),
    ),
  });
}

const scenarioDefinitions = Object.freeze([
  Object.freeze({
    metadata: metadata(
      "partial-fill",
      "Partial fill",
      "A smaller IOC bid crosses one resting ask, leaving the unfilled ask quantity on the book.",
      [
        ["Resting ask", "10 @ 100"],
        ["Crossing IOC bid", "4 @ 105"],
        ["Expected remainder", "6 @ 100"],
      ],
    ),
    status: "Partial fill validated: 6 units remain at 100.",
    expectedEventCount: 1,
    expectedBook: book([level(100n, 6n)]),
    operations: Object.freeze([
      RESET,
      ADD_SYMBOL,
      addUser("add seller 1001", SELLER_UID),
      addUser("add buyer 1002", BUYER_UID),
      adjustBalance("deposit seller base", SELLER_UID, 1n, BASE_CURRENCY, 10n),
      adjustBalance("deposit buyer quote", BUYER_UID, 2n, QUOTE_CURRENCY, 420n),
      operation(
        "resting ask 10 @ 100",
        "placeOrder",
        {
          orderId: 10_001n,
          uid: SELLER_UID,
          symbol: MARKET_SYMBOL_ID,
          price: 100n,
          size: 10n,
          action: OrderAction.ask,
          orderType: OrderType.gtc,
        },
        CommandResultCode.success,
        0,
        book([level(100n, 10n)]),
      ),
      operation(
        "crossing IOC bid 4 @ 105",
        "placeOrder",
        {
          orderId: 10_002n,
          uid: BUYER_UID,
          symbol: MARKET_SYMBOL_ID,
          price: 105n,
          reserveBidPrice: 105n,
          size: 4n,
          action: OrderAction.bid,
          orderType: OrderType.ioc,
        },
        CommandResultCode.success,
        1,
      ),
    ]),
  }),
  Object.freeze({
    metadata: metadata(
      "multi-level-sweep",
      "Multi-level sweep",
      "One IOC bid consumes the best ask and part of the next level in strict price order.",
      [
        ["Ask ladder", "3 @ 100 · 4 @ 101"],
        ["Sweeping IOC bid", "6 @ 101"],
        ["Expected remainder", "1 @ 101"],
      ],
    ),
    status: "Multi-level sweep validated: the second ask has 1 unit left.",
    expectedEventCount: 2,
    expectedBook: book([level(101n, 1n)]),
    operations: Object.freeze([
      RESET,
      ADD_SYMBOL,
      addUser("add seller 1001", SELLER_UID),
      addUser("add buyer 1002", BUYER_UID),
      adjustBalance("deposit seller base", SELLER_UID, 1n, BASE_CURRENCY, 7n),
      adjustBalance("deposit buyer quote", BUYER_UID, 2n, QUOTE_CURRENCY, 606n),
      operation(
        "resting ask 3 @ 100",
        "placeOrder",
        {
          orderId: 20_001n,
          uid: SELLER_UID,
          symbol: MARKET_SYMBOL_ID,
          price: 100n,
          size: 3n,
          action: OrderAction.ask,
          orderType: OrderType.gtc,
        },
        CommandResultCode.success,
        0,
        book([level(100n, 3n)]),
      ),
      operation(
        "resting ask 4 @ 101",
        "placeOrder",
        {
          orderId: 20_002n,
          uid: SELLER_UID,
          symbol: MARKET_SYMBOL_ID,
          price: 101n,
          size: 4n,
          action: OrderAction.ask,
          orderType: OrderType.gtc,
        },
        CommandResultCode.success,
        0,
        book([level(100n, 3n), level(101n, 4n)]),
      ),
      operation(
        "sweeping IOC bid 6 @ 101",
        "placeOrder",
        {
          orderId: 20_003n,
          uid: BUYER_UID,
          symbol: MARKET_SYMBOL_ID,
          price: 101n,
          reserveBidPrice: 101n,
          size: 6n,
          action: OrderAction.bid,
          orderType: OrderType.ioc,
        },
        CommandResultCode.success,
        2,
      ),
    ]),
  }),
  Object.freeze({
    metadata: metadata(
      "resting-spread",
      "Resting spread",
      "A bid below an ask rests without matching, producing a stable two-sided order book.",
      [
        ["Resting ask", "5 @ 105"],
        ["Resting bid", "4 @ 100"],
        ["Expected spread", "5"],
      ],
    ),
    status: "Resting spread validated: both sides remain five ticks apart.",
    expectedEventCount: 0,
    expectedBook: book([level(105n, 5n)], [level(100n, 4n)]),
    operations: Object.freeze([
      RESET,
      ADD_SYMBOL,
      addUser("add seller 1001", SELLER_UID),
      addUser("add buyer 1002", BUYER_UID),
      adjustBalance("deposit seller base", SELLER_UID, 1n, BASE_CURRENCY, 5n),
      adjustBalance("deposit buyer quote", BUYER_UID, 2n, QUOTE_CURRENCY, 400n),
      operation(
        "resting ask 5 @ 105",
        "placeOrder",
        {
          orderId: 30_001n,
          uid: SELLER_UID,
          symbol: MARKET_SYMBOL_ID,
          price: 105n,
          size: 5n,
          action: OrderAction.ask,
          orderType: OrderType.gtc,
        },
        CommandResultCode.success,
        0,
        book([level(105n, 5n)]),
      ),
      operation(
        "resting bid 4 @ 100",
        "placeOrder",
        {
          orderId: 30_002n,
          uid: BUYER_UID,
          symbol: MARKET_SYMBOL_ID,
          price: 100n,
          reserveBidPrice: 100n,
          size: 4n,
          action: OrderAction.bid,
          orderType: OrderType.gtc,
        },
        CommandResultCode.success,
        0,
      ),
    ]),
  }),
  Object.freeze({
    metadata: metadata(
      "order-lifecycle",
      "Move, reduce, cancel",
      "A resting bid moves within its reserve limit, shrinks in place, then cancels cleanly.",
      [
        ["Initial bid", "10 @ 100"],
        ["Move / reduce", "105 / −3"],
        ["Final state", "Canceled"],
      ],
    ),
    status: "Order lifecycle validated: move, reduce, and cancel all completed.",
    expectedEventCount: 2,
    expectedBook: EMPTY_BOOK,
    operations: Object.freeze([
      RESET,
      ADD_SYMBOL,
      addUser("add buyer 1002", BUYER_UID),
      adjustBalance("deposit buyer quote", BUYER_UID, 1n, QUOTE_CURRENCY, 1_100n),
      operation(
        "resting bid 10 @ 100",
        "placeOrder",
        {
          orderId: 40_001n,
          uid: BUYER_UID,
          symbol: MARKET_SYMBOL_ID,
          price: 100n,
          reserveBidPrice: 110n,
          size: 10n,
          action: OrderAction.bid,
          orderType: OrderType.gtc,
        },
        CommandResultCode.success,
        0,
        book([], [level(100n, 10n)]),
      ),
      operation(
        "move bid to 105",
        "moveOrder",
        {
          orderId: 40_001n,
          uid: BUYER_UID,
          symbol: MARKET_SYMBOL_ID,
          newPrice: 105n,
        },
        CommandResultCode.success,
        0,
        book([], [level(105n, 10n)]),
      ),
      operation(
        "reduce bid by 3",
        "reduceOrder",
        {
          orderId: 40_001n,
          uid: BUYER_UID,
          symbol: MARKET_SYMBOL_ID,
          reduceSize: 3n,
        },
        CommandResultCode.success,
        1,
        book([], [level(105n, 7n)]),
      ),
      operation(
        "cancel remaining bid",
        "cancelOrder",
        {
          orderId: 40_001n,
          uid: BUYER_UID,
          symbol: MARKET_SYMBOL_ID,
        },
        CommandResultCode.success,
        1,
      ),
    ]),
  }),
  Object.freeze({
    metadata: metadata(
      "insufficient-funds",
      "Insufficient funds",
      "A bid that needs 100 quote units is rejected before matching when the buyer owns only 99.",
      [
        ["Available quote", "99"],
        ["Required quote", "100"],
        ["Expected result", "riskNsf (−2001)"],
      ],
    ),
    status: "Insufficient funds rejection validated: the order book stayed empty.",
    expectedEventCount: 0,
    expectedBook: EMPTY_BOOK,
    operations: Object.freeze([
      RESET,
      ADD_SYMBOL,
      addUser("add buyer 1002", BUYER_UID),
      adjustBalance("deposit buyer quote", BUYER_UID, 1n, QUOTE_CURRENCY, 99n),
      operation(
        "unfunded bid 1 @ 100",
        "placeOrder",
        {
          orderId: 50_001n,
          uid: BUYER_UID,
          symbol: MARKET_SYMBOL_ID,
          price: 100n,
          reserveBidPrice: 100n,
          size: 1n,
          action: OrderAction.bid,
          orderType: OrderType.gtc,
        },
        CommandResultCode.riskNsf,
        0,
      ),
    ]),
  }),
]);

export const scenarioCatalog = Object.freeze(
  scenarioDefinitions.map(({ metadata: scenario }) => scenario),
);

const definitionsById = new Map(
  scenarioDefinitions.map((definition) => [definition.metadata.id, definition]),
);

export function getScenario(scenarioId) {
  const definition = definitionsById.get(scenarioId);
  if (!definition) {
    throw new RangeError(`unknown market scenario: ${scenarioId}`);
  }
  return definition.metadata;
}

function freezeResult(result) {
  return Object.freeze({
    code: result.code,
    name: result.name,
    ok: result.ok,
  });
}

function freezeBook(bookSnapshot) {
  const freezeLevels = (levels) => Object.freeze(
    levels.map(({ price, volume, orders }) => level(price, volume, orders)),
  );
  return Object.freeze({
    asks: freezeLevels(bookSnapshot.asks),
    bids: freezeLevels(bookSnapshot.bids),
  });
}

function assertBook(actual, expected, context) {
  for (const side of ["asks", "bids"]) {
    if (actual[side].length !== expected[side].length) {
      throw new Error(
        `${context}: expected ${expected[side].length} ${side}, got ${actual[side].length}`,
      );
    }
    for (let index = 0; index < expected[side].length; index += 1) {
      const actualLevel = actual[side][index];
      const expectedLevel = expected[side][index];
      for (const field of ["price", "volume", "orders"]) {
        if (actualLevel[field] !== expectedLevel[field]) {
          throw new Error(
            `${context}: ${side}[${index}].${field} expected ${expectedLevel[field]}, got ${actualLevel[field]}`,
          );
        }
      }
    }
  }
}

function runOperation(exchange, step, scenarioTitle) {
  const command = exchange[step.method];
  if (typeof command !== "function") {
    throw new TypeError(`exchange does not implement ${step.method}()`);
  }

  const rawResult = step.args === undefined
    ? command.call(exchange)
    : command.call(exchange, step.args);
  const result = freezeResult(rawResult);
  const eventCount = exchange.lastEventCount();
  const entry = Object.freeze({
    label: step.label,
    result,
    eventCount,
    expectedCode: step.expectedCode,
    expectedEventCount: step.expectedEventCount,
  });

  if (result.code !== step.expectedCode) {
    throw new Error(
      `${scenarioTitle} / ${step.label}: expected result ${step.expectedCode}, got ${result.name} (${result.code})`,
    );
  }
  if (eventCount !== step.expectedEventCount) {
    throw new Error(
      `${scenarioTitle} / ${step.label}: expected ${step.expectedEventCount} matcher events, got ${eventCount}`,
    );
  }
  if (step.expectedBook) {
    const checkpoint = freezeBook(exchange.orderBook(MARKET_SYMBOL_ID, BOOK_DEPTH));
    assertBook(checkpoint, step.expectedBook, `${scenarioTitle} / ${step.label}`);
  }

  return entry;
}

/**
 * Reset, bootstrap, execute, and validate one catalog scenario against a live
 * Exchange wrapper. The same synchronous runner is used by the browser and
 * the exact-artifact Node smoke test.
 */
export function runMarketScenario(exchange, scenarioId) {
  const definition = definitionsById.get(scenarioId);
  if (!definition) {
    throw new RangeError(`unknown market scenario: ${scenarioId}`);
  }

  const logEntries = [];
  let eventCount = 0;
  for (const step of definition.operations) {
    const entry = runOperation(exchange, step, definition.metadata.title);
    logEntries.push(entry);
    eventCount += entry.eventCount;
  }

  if (eventCount !== definition.expectedEventCount) {
    throw new Error(
      `${definition.metadata.title}: expected ${definition.expectedEventCount} total matcher events, got ${eventCount}`,
    );
  }

  const finalBook = freezeBook(exchange.orderBook(MARKET_SYMBOL_ID, BOOK_DEPTH));
  assertBook(finalBook, definition.expectedBook, `${definition.metadata.title} / final L2`);

  return Object.freeze({
    scenario: definition.metadata,
    status: definition.status,
    eventCount,
    book: finalBook,
    logEntries: Object.freeze(logEntries),
  });
}
