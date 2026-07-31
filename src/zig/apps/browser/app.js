import {
  EngineKind,
  OrderAction,
  OrderType,
  SymbolType,
  assertCommandSuccess,
  instantiate,
} from "./exchange-core.js";

const runButton = document.querySelector("#run");
const resetButton = document.querySelector("#reset");
const status = document.querySelector("#status");
const log = document.querySelector("#log");
const abi = document.querySelector("#abi");
const bookMeta = document.querySelector("#book-meta");
const asksBody = document.querySelector("#asks");
const bidsBody = document.querySelector("#bids");

let exchange;
let busy = false;
const logLines = [];

function setStatus(message, state = "loading") {
  status.textContent = message;
  status.dataset.state = state;
}

function appendLog(message) {
  logLines.push(message);
  log.textContent = logLines.join("\n");
  log.scrollTop = log.scrollHeight;
}

function logResult(label, result) {
  appendLog(`${label.padEnd(24)} ${result.name} (${result.code})`);
  return assertCommandSuccess(result, label);
}

function renderLevels(body, levels, emptyLabel) {
  body.replaceChildren();
  if (levels.length === 0) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.className = "empty";
    cell.colSpan = 3;
    cell.textContent = emptyLabel;
    row.append(cell);
    body.append(row);
    return;
  }

  for (const level of levels) {
    const row = document.createElement("tr");
    for (const value of [level.price, level.volume, level.orders]) {
      const cell = document.createElement("td");
      cell.textContent = value.toString();
      row.append(cell);
    }
    body.append(row);
  }
}

function renderBook(book) {
  renderLevels(asksBody, book.asks, "No asks");
  renderLevels(bidsBody, book.bids, "No bids");
  bookMeta.textContent = `${book.asks.length} ask · ${book.bids.length} bid`;
}

function setBusy(nextBusy) {
  busy = nextBusy;
  runButton.disabled = nextBusy || !exchange;
  resetButton.disabled = nextBusy || !exchange;
}

async function runScenario() {
  if (!exchange || busy) return;
  setBusy(true);
  setStatus("Running the market scenario…");
  logLines.length = 0;
  log.textContent = "";

  try {
    logResult("reset", exchange.clear());
    logResult("add symbol 241", exchange.addSymbol({
      symbolId: 241,
      symbolType: SymbolType.currencyExchangePair,
      baseCurrency: 11,
      quoteCurrency: 15,
      baseScaleK: 1n,
      quoteScaleK: 1n,
    }));
    logResult("add seller 1001", exchange.addUser(1001n));
    logResult("add buyer 1002", exchange.addUser(1002n));
    logResult("deposit seller base", exchange.adjustBalance({
      uid: 1001n,
      transactionId: 1n,
      currency: 11,
      amount: 1_000n,
    }));
    logResult("deposit buyer quote", exchange.adjustBalance({
      uid: 1002n,
      transactionId: 2n,
      currency: 15,
      amount: 100_000n,
    }));
    logResult("resting ask 10 @ 100", exchange.placeOrder({
      orderId: 10_001n,
      uid: 1001n,
      symbol: 241,
      price: 100n,
      size: 10n,
      action: OrderAction.ask,
      orderType: OrderType.gtc,
    }));
    logResult("crossing bid 4 @ 105", exchange.placeOrder({
      orderId: 10_002n,
      uid: 1002n,
      symbol: 241,
      price: 105n,
      reserveBidPrice: 105n,
      size: 4n,
      action: OrderAction.bid,
      orderType: OrderType.ioc,
    }));

    const eventCount = exchange.lastEventCount();
    appendLog(`matcher events           ${eventCount}`);
    if (eventCount === 0) {
      throw new Error("crossing order emitted no matcher events");
    }

    const book = exchange.orderBook(241, 16);
    if (
      book.asks.length !== 1 ||
      book.bids.length !== 0 ||
      book.asks[0].price !== 100n ||
      book.asks[0].volume !== 6n
    ) {
      throw new Error("unexpected L2 remainder after crossing trade");
    }
    renderBook(book);
    appendLog("L2 wire                  XCL2 validated");
    setStatus(
      `Trade complete · ${eventCount} matcher event${eventCount === 1 ? "" : "s"} · 6 units remain at 100`,
      "ready",
    );
  } catch (error) {
    console.error(error);
    appendLog(`ERROR                    ${error.message}`);
    setStatus(error.message, "error");
  } finally {
    setBusy(false);
  }
}

function resetScenario() {
  if (!exchange || busy) return;
  setBusy(true);
  try {
    const result = exchange.clear();
    assertCommandSuccess(result, "reset");
    logLines.length = 0;
    log.textContent = "Exchange reset. Ready to run again.";
    renderLevels(asksBody, [], "—");
    renderLevels(bidsBody, [], "—");
    bookMeta.textContent = "No snapshot";
    setStatus("Exchange reset and ready.", "ready");
  } catch (error) {
    console.error(error);
    setStatus(error.message, "error");
  } finally {
    setBusy(false);
  }
}

runButton.addEventListener("click", runScenario);
resetButton.addEventListener("click", resetScenario);

try {
  const core = await instantiate("./exchange-core.wasm");
  exchange = core.create(EngineKind.direct);
  abi.textContent = `ABI ${core.abiVersion}`;
  logLines.length = 0;
  log.textContent = "WASM loaded. Direct order book allocated.";
  setStatus("Exchange loaded. Run the deterministic trade.", "ready");
  setBusy(false);
} catch (error) {
  console.error(error);
  setStatus(
    `${error.message}. Serve this directory over HTTP if it was opened as a local file.`,
    "error",
  );
}

window.addEventListener("pagehide", () => {
  exchange?.destroy();
}, { once: true });
