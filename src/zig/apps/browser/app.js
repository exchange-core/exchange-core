import {
  EngineKind,
  assertCommandSuccess,
  instantiate,
} from "./exchange-core.js";
import {
  getScenario,
  runMarketScenario,
  scenarioCatalog,
} from "./scenarios.js";

const scenarioSelect = document.querySelector("#scenario-select");
const scenarioDescription = document.querySelector("#scenario-description");
const metricElements = [0, 1, 2].map((index) => ({
  label: document.querySelector(`#metric-${index}-label`),
  value: document.querySelector(`#metric-${index}-value`),
}));
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

function setStatus(message, state = "loading") {
  status.textContent = message;
  status.dataset.state = state;
}

function pluralize(count, singular, plural = `${singular}s`) {
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

function renderEmptyBook(emptyLabel = "—") {
  renderLevels(asksBody, [], emptyLabel);
  renderLevels(bidsBody, [], emptyLabel);
  bookMeta.textContent = "No snapshot";
}

function renderBook(book) {
  renderLevels(asksBody, book.asks, "No asks");
  renderLevels(bidsBody, book.bids, "No bids");
  bookMeta.textContent = `${pluralize(book.asks.length, "ask")} · ${pluralize(book.bids.length, "bid")}`;
}

function renderScenarioMetadata(scenario) {
  scenarioDescription.textContent = scenario.description;
  for (let index = 0; index < metricElements.length; index += 1) {
    const metric = scenario.metrics[index];
    metricElements[index].label.textContent = metric.label;
    metricElements[index].value.textContent = metric.value;
  }
}

function selectedScenario() {
  return getScenario(scenarioSelect.value);
}

function setBusy(nextBusy) {
  busy = nextBusy;
  scenarioSelect.disabled = nextBusy || !exchange;
  runButton.disabled = nextBusy || !exchange;
  resetButton.disabled = nextBusy || !exchange;
}

function formatLog(summary) {
  const commandLines = summary.logEntries.map((entry) => {
    const events = entry.eventCount === 0
      ? ""
      : ` · ${pluralize(entry.eventCount, "event")}`;
    const expectedRejection = entry.result.ok ? "" : " · expected rejection";
    return `${entry.label.padEnd(29)} ${entry.result.name} (${entry.result.code})${events}${expectedRejection}`;
  });
  commandLines.push(
    `matcher events                ${summary.eventCount}`,
    "L2 wire                       XCL2 validated",
  );
  return commandLines.join("\n");
}

function runSelectedScenario() {
  if (!exchange || busy) return;
  setBusy(true);
  const scenario = selectedScenario();
  setStatus(`Running ${scenario.title.toLowerCase()}…`);
  log.textContent = "Resetting and bootstrapping the exchange…";

  try {
    const summary = runMarketScenario(exchange, scenario.id);
    renderBook(summary.book);
    log.textContent = formatLog(summary);
    log.scrollTop = log.scrollHeight;
    setStatus(
      `${summary.status} ${pluralize(summary.eventCount, "matcher event")} observed.`,
      "ready",
    );
  } catch (error) {
    console.error(error);
    log.textContent += `\nERROR                         ${error.message}`;
    setStatus(error.message, "error");
  } finally {
    setBusy(false);
  }
}

function resetScenario() {
  if (!exchange || busy) return;
  setBusy(true);
  const scenario = selectedScenario();
  try {
    assertCommandSuccess(exchange.clear(), "reset exchange");
    renderEmptyBook();
    log.textContent = `${scenario.title} selected. Exchange reset; the runner will bootstrap fresh state.`;
    setStatus(`${scenario.title} is reset and ready to run.`, "ready");
  } catch (error) {
    console.error(error);
    setStatus(error.message, "error");
  } finally {
    setBusy(false);
  }
}

for (const scenario of scenarioCatalog) {
  const option = document.createElement("option");
  option.value = scenario.id;
  option.textContent = scenario.title;
  scenarioSelect.append(option);
}
renderScenarioMetadata(selectedScenario());

scenarioSelect.addEventListener("change", () => {
  renderScenarioMetadata(selectedScenario());
  resetScenario();
});
runButton.addEventListener("click", runSelectedScenario);
resetButton.addEventListener("click", resetScenario);

try {
  const core = await instantiate("./exchange-core.wasm");
  exchange = core.create(EngineKind.direct);
  abi.textContent = `ABI ${core.abiVersion}`;
  log.textContent = "WASM loaded. Choose a scenario or run the selected one.";
  setStatus(`${selectedScenario().title} is ready to run.`, "ready");
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
