import { EngineKind, instantiate } from "./exchange-core.js";
import { ManualMarketSession } from "./manual-session.js";

const I64_MAX = (1n << 63n) - 1n;

const status = document.querySelector("#status");
const abi = document.querySelector("#abi");
const resetButton = document.querySelector("#reset-market");
const orderForm = document.querySelector("#order-form");
const clientSelect = document.querySelector("#order-client");
const sideSelect = document.querySelector("#order-side");
const orderTypeSelect = document.querySelector("#order-type");
const priceInput = document.querySelector("#order-price");
const sizeInput = document.querySelector("#order-size");
const orderSubmit = document.querySelector("#order-submit");
const addClientForm = document.querySelector("#add-client-form");
const clientNameInput = document.querySelector("#client-name");
const baseBalanceInput = document.querySelector("#base-balance");
const quoteBalanceInput = document.querySelector("#quote-balance");
const addClientSubmit = document.querySelector("#add-client-submit");
const clientList = document.querySelector("#client-list");
const clientCount = document.querySelector("#client-count");
const nextOrderId = document.querySelector("#next-order-id");
const asksBody = document.querySelector("#asks");
const bidsBody = document.querySelector("#bids");
const bookMeta = document.querySelector("#book-meta");
const activityList = document.querySelector("#activity");

const controlledElements = [
  resetButton,
  clientSelect,
  sideSelect,
  orderTypeSelect,
  priceInput,
  sizeInput,
  orderSubmit,
  clientNameInput,
  baseBalanceInput,
  quoteBalanceInput,
  addClientSubmit,
];

const integerInputs = [
  priceInput,
  sizeInput,
  baseBalanceInput,
  quoteBalanceInput,
];

let exchange;
let session;
let ready = false;
let busy = false;

class InputValidationError extends Error {
  constructor(input, message) {
    super(message);
    this.name = "InputValidationError";
    this.input = input;
  }
}

function setStatus(message, state = "loading") {
  status.textContent = message;
  status.dataset.state = state;
}

function setBusy(nextBusy) {
  busy = nextBusy;
  for (const element of controlledElements) {
    element.disabled = !ready || busy;
  }
}

function clearInvalidInputs() {
  for (const input of integerInputs) input.removeAttribute("aria-invalid");
  clientNameInput.removeAttribute("aria-invalid");
}

function parseInteger(input, label, { allowZero = false } = {}) {
  const raw = input.value.trim();
  if (!/^(0|[1-9][0-9]*)$/.test(raw)) {
    throw new InputValidationError(input, `${label} must be a whole number.`);
  }

  let value;
  try {
    value = BigInt(raw);
  } catch {
    throw new InputValidationError(input, `${label} is outside the supported integer range.`);
  }

  if (allowZero ? value < 0n : value <= 0n) {
    const qualifier = allowZero ? "zero or greater" : "greater than zero";
    throw new InputValidationError(input, `${label} must be ${qualifier}.`);
  }
  if (value > I64_MAX) {
    throw new InputValidationError(input, `${label} exceeds the supported integer range.`);
  }
  return value;
}

function formatCount(count, singular, plural = `${singular}s`) {
  return `${count} ${count === 1 ? singular : plural}`;
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
  bookMeta.textContent = `${formatCount(book.asks.length, "ask")} · ${formatCount(book.bids.length, "bid")}`;
}

function renderClients(clients, preferredUid) {
  const currentUid = preferredUid?.toString() ?? clientSelect.value;
  clientSelect.replaceChildren();
  clientList.replaceChildren();

  for (const client of clients) {
    const option = document.createElement("option");
    option.value = client.uid.toString();
    option.textContent = `${client.name} · UID ${client.uid}`;
    clientSelect.append(option);

    const item = document.createElement("li");
    item.className = "client";

    const identity = document.createElement("div");
    const name = document.createElement("div");
    name.className = "client-name";
    name.textContent = client.name;
    const uid = document.createElement("div");
    uid.className = "client-uid";
    uid.textContent = `UID ${client.uid}`;
    identity.append(name, uid);

    const funds = document.createElement("div");
    funds.className = "client-funds";
    funds.textContent = `BTC ${client.baseBalance}\nUSD ${client.quoteBalance}`;
    item.append(identity, funds);
    clientList.append(item);
  }

  if (clients.some((client) => client.uid.toString() === currentUid)) {
    clientSelect.value = currentUid;
  }
  clientCount.textContent = formatCount(clients.length, "client");
}

function formatSide(side) {
  return side === "buy" || side === "bid" ? "BUY" : "SELL";
}

function renderActivity(entries) {
  activityList.replaceChildren();
  if (entries.length === 0) {
    const empty = document.createElement("li");
    empty.className = "activity-empty";
    empty.textContent = "Orders, business rejections, and matcher-event counts will appear here.";
    activityList.append(empty);
    return;
  }

  for (const entry of [...entries].reverse()) {
    const item = document.createElement("li");
    item.className = "activity-item";
    item.value = entry.sequence;

    const top = document.createElement("div");
    top.className = "activity-top";
    const client = document.createElement("span");
    client.className = "activity-client";
    client.textContent = entry.clientName;
    const sequence = document.createElement("span");
    sequence.className = "activity-sequence";
    sequence.textContent = `#${entry.sequence}`;
    top.append(client, sequence);

    const order = document.createElement("p");
    order.className = "activity-order";
    order.textContent = `${formatSide(entry.side)} ${entry.orderType.toUpperCase()} · ${entry.size} @ ${entry.price} · order ${entry.orderId}`;

    const result = document.createElement("p");
    result.className = `activity-result${entry.result.ok ? "" : " rejected"}`;
    result.textContent = `${entry.result.name} (${entry.result.code}) · ${formatCount(entry.eventCount, "matcher event")}`;
    item.append(top, order, result);
    activityList.append(item);
  }
}

function renderSnapshot(snapshot, preferredUid) {
  renderClients(snapshot.clients, preferredUid);
  renderBook(snapshot.book);
  renderActivity(snapshot.activity);
  nextOrderId.textContent = snapshot.nextOrderId === undefined
    ? "Next order ready"
    : `Next order ${snapshot.nextOrderId}`;
}

async function serialize(action) {
  if (!ready || busy || !session) return;
  setBusy(true);
  clearInvalidInputs();
  try {
    await Promise.resolve();
    action();
  } catch (error) {
    if (error instanceof InputValidationError) {
      error.input.setAttribute("aria-invalid", "true");
      error.input.focus();
      setStatus(error.message, "error");
    } else {
      console.error(error);
      setStatus(error.message, "error");
    }
  } finally {
    setBusy(false);
  }
}

orderForm.addEventListener("submit", (event) => {
  event.preventDefault();
  void serialize(() => {
    const uid = BigInt(clientSelect.value);
    if (uid <= 0n) {
      throw new Error("Select a valid client before sending an order.");
    }

    const entry = session.submitOrder({
      uid,
      side: sideSelect.value,
      orderType: orderTypeSelect.value,
      price: parseInteger(priceInput, "Price"),
      size: parseInteger(sizeInput, "Quantity"),
    });
    renderSnapshot(session.snapshot(), uid);

    if (entry.result.ok) {
      setStatus(
        `${entry.clientName}'s order ${entry.orderId} succeeded with ${formatCount(entry.eventCount, "matcher event")}.`,
        "ready",
      );
      sizeInput.value = "";
    } else {
      setStatus(
        `${entry.clientName}'s order ${entry.orderId} was rejected: ${entry.result.name} (${entry.result.code}).`,
        "warning",
      );
    }
  });
});

addClientForm.addEventListener("submit", (event) => {
  event.preventDefault();
  void serialize(() => {
    const name = clientNameInput.value.trim();
    if (name.length === 0) {
      throw new InputValidationError(clientNameInput, "Client name is required.");
    }

    const client = session.addClient({
      name,
      baseBalance: parseInteger(baseBalanceInput, "BTC balance", { allowZero: true }),
      quoteBalance: parseInteger(quoteBalanceInput, "USD balance", { allowZero: true }),
    });
    renderSnapshot(session.snapshot(), client.uid);
    clientNameInput.value = "";
    baseBalanceInput.value = "1000";
    quoteBalanceInput.value = "100000";
    setStatus(`${client.name} joined the shared market as UID ${client.uid}.`, "ready");
  });
});

resetButton.addEventListener("click", () => {
  void serialize(() => {
    renderSnapshot(session.reset());
    setStatus("Shared market reset with three prefunded clients.", "ready");
  });
});

try {
  const core = await instantiate("./exchange-core.wasm");
  exchange = core.create(EngineKind.direct);
  session = new ManualMarketSession(exchange);
  const snapshot = session.reset();
  abi.textContent = `ABI ${core.abiVersion} · direct engine`;
  renderSnapshot(snapshot);
  ready = true;
  setBusy(false);
  setStatus("Shared market ready. Choose a client and send an order.", "ready");
} catch (error) {
  console.error(error);
  setStatus(
    `${error.message}. Serve this directory over HTTP if it was opened as a local file.`,
    "error",
  );
}

window.addEventListener("pagehide", (event) => {
  if (!event.persisted) exchange?.destroy();
});
