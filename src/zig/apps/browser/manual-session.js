import {
  OrderAction,
  OrderType,
  SymbolType,
  assertCommandSuccess,
} from "./exchange-core.js";

export const MANUAL_SYMBOL_ID = 241;
export const BASE_CURRENCY = 11;
export const QUOTE_CURRENCY = 15;

const BOOK_DEPTH = 20;
const FIRST_CLIENT_UID = 1_001n;
const FIRST_ORDER_ID = 10_001n;
const FIRST_TRANSACTION_ID = 1n;
const I64_MAX = (1n << 63n) - 1n;

function frozenClient(name, uid, baseBalance, quoteBalance) {
  return Object.freeze({ name, uid, baseBalance, quoteBalance });
}

export const DEFAULT_CLIENTS = Object.freeze([
  frozenClient("Alice", 1_001n, 1_000n, 100_000n),
  frozenClient("Bob", 1_002n, 1_000n, 100_000n),
  frozenClient("Carol", 1_003n, 1_000n, 100_000n),
]);

const SIDE_ACTIONS = Object.freeze({
  buy: OrderAction.bid,
  sell: OrderAction.ask,
});

const ORDER_TYPES = Object.freeze({
  gtc: OrderType.gtc,
  ioc: OrderType.ioc,
});

function requireI64(value, name, { allowZero = false } = {}) {
  if (typeof value !== "bigint") {
    throw new TypeError(`${name} must be a bigint`);
  }
  if (value > I64_MAX) {
    throw new RangeError(`${name} exceeds the signed 64-bit range`);
  }
  if (allowZero ? value < 0n : value <= 0n) {
    const qualifier = allowZero ? "zero or greater" : "greater than zero";
    throw new RangeError(`${name} must be ${qualifier}`);
  }
  return value;
}

function requireName(value) {
  if (typeof value !== "string") {
    throw new TypeError("client name must be a string");
  }
  const name = value.trim();
  if (name.length === 0 || name.length > 40) {
    throw new RangeError("client name must contain 1 to 40 characters");
  }
  return name;
}

function requireChoice(value, choices, name) {
  if (!Object.hasOwn(choices, value)) {
    throw new RangeError(`${name} must be one of: ${Object.keys(choices).join(", ")}`);
  }
  return value;
}

function requireExchange(exchange) {
  for (const method of [
    "clear",
    "addSymbol",
    "addUser",
    "adjustBalance",
    "placeOrder",
    "lastEventCount",
    "orderBook",
  ]) {
    if (typeof exchange?.[method] !== "function") {
      throw new TypeError(`exchange must implement ${method}()`);
    }
  }
  return exchange;
}

function freezeResult(result) {
  return Object.freeze({ code: result.code, name: result.name, ok: result.ok });
}

export class ManualMarketSession {
  #exchange;
  #clients = [];
  #activity = [];
  #nextClientUid = FIRST_CLIENT_UID;
  #nextOrderId = FIRST_ORDER_ID;
  #nextTransactionId = FIRST_TRANSACTION_ID;
  #ready = false;

  constructor(exchange) {
    this.#exchange = requireExchange(exchange);
  }

  #registerClient({ name, uid, baseBalance, quoteBalance }) {
    const normalizedName = requireName(name);
    requireI64(uid, "client uid");
    requireI64(baseBalance, "base balance", { allowZero: true });
    requireI64(quoteBalance, "quote balance", { allowZero: true });

    assertCommandSuccess(this.#exchange.addUser(uid), `add client ${normalizedName}`);
    if (baseBalance > 0n) {
      assertCommandSuccess(this.#exchange.adjustBalance({
        uid,
        transactionId: this.#nextTransactionId,
        currency: BASE_CURRENCY,
        amount: baseBalance,
      }), `fund ${normalizedName} base balance`);
      this.#nextTransactionId += 1n;
    }
    if (quoteBalance > 0n) {
      assertCommandSuccess(this.#exchange.adjustBalance({
        uid,
        transactionId: this.#nextTransactionId,
        currency: QUOTE_CURRENCY,
        amount: quoteBalance,
      }), `fund ${normalizedName} quote balance`);
      this.#nextTransactionId += 1n;
    }

    const client = frozenClient(normalizedName, uid, baseBalance, quoteBalance);
    this.#clients.push(client);
    if (uid >= this.#nextClientUid) this.#nextClientUid = uid + 1n;
    return client;
  }

  reset() {
    assertCommandSuccess(this.#exchange.clear(), "reset manual market");
    this.#clients = [];
    this.#activity = [];
    this.#nextClientUid = FIRST_CLIENT_UID;
    this.#nextOrderId = FIRST_ORDER_ID;
    this.#nextTransactionId = FIRST_TRANSACTION_ID;
    this.#ready = false;

    assertCommandSuccess(this.#exchange.addSymbol({
      symbolId: MANUAL_SYMBOL_ID,
      symbolType: SymbolType.currencyExchangePair,
      baseCurrency: BASE_CURRENCY,
      quoteCurrency: QUOTE_CURRENCY,
      baseScaleK: 1n,
      quoteScaleK: 1n,
    }), "add manual market symbol");

    for (const client of DEFAULT_CLIENTS) {
      this.#registerClient(client);
    }
    this.#ready = true;
    return this.snapshot();
  }

  addClient({ name, baseBalance = 1_000n, quoteBalance = 100_000n }) {
    this.#ensureReady();
    return this.#registerClient({
      name,
      uid: this.#nextClientUid,
      baseBalance,
      quoteBalance,
    });
  }

  submitOrder({ uid, side, orderType, price, size }) {
    this.#ensureReady();
    requireI64(uid, "client uid");
    requireChoice(side, SIDE_ACTIONS, "side");
    requireChoice(orderType, ORDER_TYPES, "orderType");
    requireI64(price, "price");
    requireI64(size, "size");

    const client = this.#clients.find((candidate) => candidate.uid === uid);
    if (!client) {
      throw new RangeError(`unknown client uid ${uid}`);
    }

    const orderId = this.#nextOrderId;
    const rawResult = this.#exchange.placeOrder({
      orderId,
      uid,
      symbol: MANUAL_SYMBOL_ID,
      price,
      reserveBidPrice: side === "buy" ? price : 0n,
      size,
      action: SIDE_ACTIONS[side],
      orderType: ORDER_TYPES[orderType],
    });
    const result = freezeResult(rawResult);
    const eventCount = this.#exchange.lastEventCount();
    this.#nextOrderId += 1n;

    const entry = Object.freeze({
      sequence: this.#activity.length + 1,
      orderId,
      uid,
      clientName: client.name,
      side,
      orderType,
      price,
      size,
      result,
      eventCount,
    });
    this.#activity.push(entry);
    return entry;
  }

  snapshot(depth = BOOK_DEPTH) {
    this.#ensureReady();
    return Object.freeze({
      clients: Object.freeze([...this.#clients]),
      activity: Object.freeze([...this.#activity]),
      book: this.#exchange.orderBook(MANUAL_SYMBOL_ID, depth),
      nextOrderId: this.#nextOrderId,
    });
  }

  #ensureReady() {
    if (!this.#ready) {
      throw new Error("manual market session must be reset before use");
    }
  }
}
