const EXPECTED_ABI_VERSION = 1;
const U32_MAX = 0xffff_ffff;
const I32_MIN = -0x8000_0000;
const I32_MAX = 0x7fff_ffff;
const I64_MIN = -(1n << 63n);
const I64_MAX = (1n << 63n) - 1n;
const L2_HEADER_SIZE = 16;
const L2_LEVEL_SIZE = 24;

export const ABI_VERSION = EXPECTED_ABI_VERSION;

export const AbiStatus = Object.freeze({
  ok: 0,
  invalidHandle: 1,
  outOfMemory: 2,
  invalidArgument: 3,
  invalidEnum: 4,
  instanceLimit: 5,
  handleExhausted: 6,
  faulted: 7,
  internal: 255,
});

export const EngineKind = Object.freeze({
  naive: 0,
  direct: 1,
});

export const SymbolType = Object.freeze({
  currencyExchangePair: 0,
  spot: 0,
  futuresContract: 1,
  option: 2,
});

export const BalanceAdjustmentType = Object.freeze({
  adjustment: 0,
  suspend: 1,
});

export const OrderAction = Object.freeze({
  ask: 0,
  bid: 1,
});

export const OrderType = Object.freeze({
  gtc: 0,
  ioc: 1,
  iocBudget: 2,
  fok: 3,
  fokBudget: 4,
});

export const CommandResultCode = Object.freeze({
  new: 0,
  validForMatchingEngine: 1,
  success: 100,
  accepted: 110,
  authInvalidUser: -1001,
  authTokenExpired: -1002,
  invalidSymbol: -1201,
  invalidPriceStep: -1202,
  unsupportedSymbolType: -1203,
  riskNsf: -2001,
  riskInvalidReserveBidPrice: -2002,
  riskAskPriceLowerThanFee: -2003,
  riskMarginTradingDisabled: -2004,
  matchingUnknownOrderId: -3002,
  matchingUnsupportedCommand: -3004,
  matchingInvalidOrderBookId: -3005,
  matchingMoveFailedPriceOverRiskLimit: -3041,
  matchingReduceFailedWrongSize: -3051,
  userAlreadyExists: -4001,
  balanceAdjustmentAlreadyAppliedSame: -4101,
  balanceAdjustmentAlreadyAppliedMany: -4102,
  balanceAdjustmentNsf: -4103,
  userNonZeroAccountBalance: -4104,
  userHasPositions: -4130,
  userHasNonEmptyAccounts: -4131,
  userNotSuspended: -4132,
  userAlreadySuspended: -4133,
  userNotFound: -4201,
  symbolAlreadyExists: -5001,
  binaryCommandFailed: -8001,
  reportQueryUnknownType: -8003,
  statePersistRiskEngineFailed: -8010,
  statePersistMatchingEngineFailed: -8020,
  drop: -9999,
});

const ABI_STATUS_NAMES = new Map(
  Object.entries(AbiStatus).map(([name, value]) => [value, name]),
);
const COMMAND_RESULT_NAMES = new Map(
  Object.entries(CommandResultCode).map(([name, value]) => [value, name]),
);

const REQUIRED_FUNCTIONS = Object.freeze([
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
]);

export class ExchangeAbiError extends Error {
  constructor(operation, status) {
    const statusName = ABI_STATUS_NAMES.get(status) ?? `unknown_${status}`;
    super(`${operation} failed: ${statusName} (${status})`);
    this.name = "ExchangeAbiError";
    this.operation = operation;
    this.status = status;
    this.statusName = statusName;
  }
}

export class ExchangeCommandError extends Error {
  constructor(operation, result) {
    super(`${operation} returned ${result.name} (${result.code})`);
    this.name = "ExchangeCommandError";
    this.operation = operation;
    this.result = result;
  }
}

export class ExchangeDestroyedError extends Error {
  constructor() {
    super("exchange instance has been destroyed");
    this.name = "ExchangeDestroyedError";
  }
}

function toI32(value, name) {
  if (!Number.isInteger(value) || value < I32_MIN || value > I32_MAX) {
    throw new RangeError(`${name} must be a signed 32-bit integer`);
  }
  return value;
}

function toU32(value, name) {
  if (!Number.isInteger(value) || value < 0 || value > U32_MAX) {
    throw new RangeError(`${name} must be an unsigned 32-bit integer`);
  }
  return value;
}

function toI64(value, name) {
  if (typeof value !== "bigint") {
    throw new TypeError(`${name} must be a bigint`);
  }

  if (value < I64_MIN || value > I64_MAX) {
    throw new RangeError(`${name} must be a signed 64-bit integer`);
  }
  return value;
}

function unpack(raw, operation) {
  if (typeof raw !== "bigint") {
    throw new TypeError(`${operation} returned a non-i64 value`);
  }
  const bits = BigInt.asUintN(64, raw);
  const status = Number((bits >> 32n) & 0xffff_ffffn);
  const payload = Number(bits & 0xffff_ffffn);
  if (status !== AbiStatus.ok) {
    throw new ExchangeAbiError(operation, status);
  }
  return payload;
}

function signedPayload(payload) {
  return payload > I32_MAX ? payload - 0x1_0000_0000 : payload;
}

function commandResult(code) {
  return Object.freeze({
    code,
    name: COMMAND_RESULT_NAMES.get(code) ?? `unknown_${code}`,
    ok: code === CommandResultCode.success || code === CommandResultCode.accepted,
  });
}

export function assertCommandSuccess(result, operation = "exchange command") {
  if (!result || result.ok !== true) {
    throw new ExchangeCommandError(
      operation,
      result ?? commandResult(CommandResultCode.drop),
    );
  }
  return result;
}

function validateResponse(response) {
  if (!response.ok) {
    throw new Error(
      `failed to fetch WebAssembly module: ${response.status} ${response.statusText}`,
    );
  }
  return response;
}

async function instantiateResponse(response) {
  validateResponse(response);
  if (response.bodyUsed) {
    throw new TypeError("WebAssembly response body has already been consumed");
  }

  const fallbackResponse = response.clone();
  if (typeof WebAssembly.instantiateStreaming === "function") {
    try {
      return await WebAssembly.instantiateStreaming(response, {});
    } catch (streamingError) {
      try {
        return await WebAssembly.instantiate(
          await fallbackResponse.arrayBuffer(),
          {},
        );
      } catch (fallbackError) {
        if (fallbackError.cause === undefined) {
          fallbackError.cause = streamingError;
        }
        throw fallbackError;
      }
    }
  }

  return WebAssembly.instantiate(await fallbackResponse.arrayBuffer(), {});
}

async function instantiateSource(source) {
  if (typeof source === "string" || source instanceof URL) {
    return instantiateResponse(await fetch(source));
  }

  if (typeof Response !== "undefined" && source instanceof Response) {
    return instantiateResponse(source);
  }

  if (source instanceof ArrayBuffer || ArrayBuffer.isView(source)) {
    return WebAssembly.instantiate(source, {});
  }

  throw new TypeError(
    "source must be a URL, Response, ArrayBuffer, or typed byte view",
  );
}

/**
 * Instantiate the browser ABI with no imports and return its instance manager.
 */
export async function instantiate(source) {
  const instantiated = await instantiateSource(source);
  const instance = instantiated instanceof WebAssembly.Instance
    ? instantiated
    : instantiated.instance;
  const module = instantiated instanceof WebAssembly.Instance
    ? undefined
    : instantiated.module;

  if (!(instance instanceof WebAssembly.Instance)) {
    throw new TypeError("source did not instantiate to a WebAssembly.Instance");
  }
  return new ExchangeCore(instance, module);
}

export const instantiateExchange = instantiate;

export class ExchangeCore {
  #exports;
  #view;

  constructor(instance, module = undefined) {
    const wasmExports = instance.exports;
    if (!(wasmExports.memory instanceof WebAssembly.Memory)) {
      throw new TypeError("WebAssembly module must export memory");
    }
    for (const name of REQUIRED_FUNCTIONS) {
      if (typeof wasmExports[name] !== "function") {
        throw new TypeError(`WebAssembly module is missing export ${name}`);
      }
    }

    this.instance = instance;
    this.module = module;
    this.#exports = wasmExports;
    this.#view = new DataView(wasmExports.memory.buffer);

    const abiVersion = this.#invoke("exchange_abi_version", []);
    if (abiVersion !== EXPECTED_ABI_VERSION) {
      throw new Error(
        `unsupported exchange ABI version ${abiVersion}; expected ${EXPECTED_ABI_VERSION}`,
      );
    }
    this.abiVersion = abiVersion;
  }

  get memory() {
    return this.#exports.memory;
  }

  create(kind = EngineKind.direct) {
    const payload = this.#packedValue(
      "exchange_create",
      [toI32(kind, "kind")],
    );
    return new Exchange(this, payload);
  }

  #refreshView() {
    this.#view = new DataView(this.#exports.memory.buffer);
  }

  #invoke(name, args) {
    try {
      return this.#exports[name](...args);
    } finally {
      // memory.grow replaces memory.buffer. Never retain a DataView across calls.
      this.#refreshView();
    }
  }

  #packedValue(name, args) {
    return unpack(this.#invoke(name, args), name);
  }

  _command(name, args) {
    return commandResult(signedPayload(this.#packedValue(name, args)));
  }

  _value(name, args) {
    return this.#packedValue(name, args);
  }

  _destroy(handle) {
    const rawStatus = this.#invoke("exchange_destroy", [
      toU32(handle, "handle"),
    ]);
    if (!Number.isInteger(rawStatus)) {
      throw new TypeError("exchange_destroy returned a non-i32 value");
    }
    const status = rawStatus >>> 0;
    if (status !== AbiStatus.ok) {
      throw new ExchangeAbiError("exchange_destroy", status);
    }
  }

  _decodeOrderBook(pointer, length) {
    const start = toU32(pointer, "order book pointer");
    const byteLength = toU32(length, "order book length");
    const end = start + byteLength;
    if (end > this.#view.byteLength || end < start) {
      throw new RangeError("order book wire lies outside WebAssembly memory");
    }
    if (byteLength < L2_HEADER_SIZE) {
      throw new Error("order book wire is shorter than its header");
    }

    const view = this.#view;
    if (
      view.getUint8(start) !== 0x58 ||
      view.getUint8(start + 1) !== 0x43 ||
      view.getUint8(start + 2) !== 0x4c ||
      view.getUint8(start + 3) !== 0x32
    ) {
      throw new Error("invalid order book wire magic; expected XCL2");
    }

    const version = view.getUint16(start + 4, true);
    const levelSize = view.getUint16(start + 6, true);
    if (version !== 1) {
      throw new Error(`unsupported order book wire version ${version}`);
    }
    if (levelSize !== L2_LEVEL_SIZE) {
      throw new Error(`unsupported order book level size ${levelSize}`);
    }

    const askCount = view.getUint32(start + 8, true);
    const bidCount = view.getUint32(start + 12, true);
    const totalLevels = askCount + bidCount;
    const expectedLength = L2_HEADER_SIZE + totalLevels * L2_LEVEL_SIZE;
    if (!Number.isSafeInteger(expectedLength) || expectedLength !== byteLength) {
      throw new Error(
        `invalid order book wire length ${byteLength}; expected ${expectedLength}`,
      );
    }

    let cursor = start + L2_HEADER_SIZE;
    const readLevels = (count, side) => {
      const levels = new Array(count);
      for (let index = 0; index < count; index += 1) {
        const price = view.getBigInt64(cursor, true);
        const volume = view.getBigInt64(cursor + 8, true);
        const orders = view.getUint32(cursor + 16, true);
        const reserved = view.getUint32(cursor + 20, true);
        if (reserved !== 0) {
          throw new Error(
            `${side} level ${index} has non-zero reserved field ${reserved}`,
          );
        }
        levels[index] = Object.freeze({ price, volume, orders });
        cursor += L2_LEVEL_SIZE;
      }
      return Object.freeze(levels);
    };

    const asks = readLevels(askCount, "ask");
    const bids = readLevels(bidCount, "bid");
    if (cursor !== end) {
      throw new Error("order book decoder did not consume the complete wire");
    }
    return Object.freeze({ asks, bids });
  }
}

export class Exchange {
  #core;
  #handle;
  #destroyed = false;

  constructor(core, handle) {
    if (!(core instanceof ExchangeCore)) {
      throw new TypeError("Exchange instances must be created by ExchangeCore");
    }
    this.#core = core;
    this.#handle = toU32(handle, "handle");
  }

  static async create(source, options = {}) {
    const core = source instanceof ExchangeCore ? source : await instantiate(source);
    return core.create(options.kind ?? EngineKind.direct);
  }

  get destroyed() {
    return this.#destroyed;
  }

  #ensureLive() {
    if (this.#destroyed) {
      throw new ExchangeDestroyedError();
    }
  }

  #command(name, args) {
    this.#ensureLive();
    return this.#core._command(name, [this.#handle, ...args]);
  }

  reset() {
    return this.#command("exchange_reset", []);
  }

  clear() {
    return this.reset();
  }

  addSymbol({
    symbolId,
    symbolType = SymbolType.currencyExchangePair,
    baseCurrency,
    quoteCurrency,
    baseScaleK = 1n,
    quoteScaleK = 1n,
    takerFee = 0n,
    makerFee = 0n,
    marginBuy = 0n,
    marginSell = 0n,
  }) {
    return this.#command("exchange_add_symbol", [
      toI32(symbolId, "symbolId"),
      toI32(symbolType, "symbolType"),
      toI32(baseCurrency, "baseCurrency"),
      toI32(quoteCurrency, "quoteCurrency"),
      toI64(baseScaleK, "baseScaleK"),
      toI64(quoteScaleK, "quoteScaleK"),
      toI64(takerFee, "takerFee"),
      toI64(makerFee, "makerFee"),
      toI64(marginBuy, "marginBuy"),
      toI64(marginSell, "marginSell"),
    ]);
  }

  addUser(uid) {
    return this.#command("exchange_add_user", [toI64(uid, "uid")]);
  }

  adjustBalance({
    uid,
    transactionId,
    currency,
    amount,
    adjustmentType = BalanceAdjustmentType.adjustment,
  }) {
    return this.#command("exchange_adjust_balance", [
      toI64(uid, "uid"),
      toI64(transactionId, "transactionId"),
      toI32(currency, "currency"),
      toI64(amount, "amount"),
      toI32(adjustmentType, "adjustmentType"),
    ]);
  }

  placeOrder({
    orderId,
    uid,
    symbol,
    price,
    reserveBidPrice = 0n,
    size,
    action,
    orderType = OrderType.gtc,
    userCookie = 0,
  }) {
    return this.#command("exchange_place_order", [
      toI64(orderId, "orderId"),
      toI64(uid, "uid"),
      toI32(symbol, "symbol"),
      toI64(price, "price"),
      toI64(reserveBidPrice, "reserveBidPrice"),
      toI64(size, "size"),
      toI32(action, "action"),
      toI32(orderType, "orderType"),
      toI32(userCookie, "userCookie"),
    ]);
  }

  moveOrder({ orderId, uid, symbol, newPrice }) {
    return this.#command("exchange_move_order", [
      toI64(orderId, "orderId"),
      toI64(uid, "uid"),
      toI32(symbol, "symbol"),
      toI64(newPrice, "newPrice"),
    ]);
  }

  cancelOrder({ orderId, uid, symbol }) {
    return this.#command("exchange_cancel_order", [
      toI64(orderId, "orderId"),
      toI64(uid, "uid"),
      toI32(symbol, "symbol"),
    ]);
  }

  reduceOrder({ orderId, uid, symbol, reduceSize }) {
    return this.#command("exchange_reduce_order", [
      toI64(orderId, "orderId"),
      toI64(uid, "uid"),
      toI32(symbol, "symbol"),
      toI64(reduceSize, "reduceSize"),
    ]);
  }

  lastEventCount() {
    this.#ensureLive();
    return this.#core._value("exchange_last_event_count", [this.#handle]);
  }

  orderBook(symbol, depth = U32_MAX) {
    this.#ensureLive();
    const queryPayload = this.#core._value("exchange_query_order_book", [
      this.#handle,
      toI32(symbol, "symbol"),
      toU32(depth, "depth"),
    ]);
    if (queryPayload !== 0) {
      throw new Error(`exchange_query_order_book returned reserved payload ${queryPayload}`);
    }
    const pointer = this.#core._value("exchange_order_book_ptr", [this.#handle]);
    const length = this.#core._value("exchange_order_book_len", [this.#handle]);
    return this.#core._decodeOrderBook(pointer, length);
  }

  queryOrderBook(symbol, depth = U32_MAX) {
    return this.orderBook(symbol, depth);
  }

  requestOrderBook(symbol, depth = U32_MAX) {
    return this.orderBook(symbol, depth);
  }

  destroy() {
    if (this.#destroyed) {
      return false;
    }
    this.#core._destroy(this.#handle);
    this.#destroyed = true;
    this.#handle = 0;
    return true;
  }

  close() {
    return this.destroy();
  }
}
