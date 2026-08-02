# Matching and risk

Matching and accounting are deliberately separated. R1 reserves the maximum exposure before an order reaches a book, the matching engine produces an event chain describing what happened, and R2 converts that chain into final account or position changes.

## Integer units and symbol rules

[`CoreSymbolSpecification`](../../src/zig/core/common.zig) defines all arithmetic. No floating-point values enter the core.

| Field | Meaning |
|---|---|
| `base_scale_k` | Base-currency units in one order lot. |
| `quote_scale_k` | Quote-currency units in one price step per lot. |
| `price` | Integer number of quote price steps. |
| `size` | Integer number of lots. |
| `maker_fee`, `taker_fee` | Quote-currency units charged per lot. |
| `margin_buy`, `margin_sell` | Quote-currency margin units per futures lot. |

Central arithmetic (same as Java `CoreArithmeticUtils`):

```text
base amount                 = size * base_scale_k
quote amount                = size * price * quote_scale_k
bid hold with taker fee     = size * (reserve_price * quote_scale_k + taker_fee)
budget-bid hold             = budget_steps * quote_scale_k + size * taker_fee
```

Callers choose scales that keep all intermediate `i64` calculations within range.

## Order-book contract and implementations

Both books implement the same external behavior and state-hash contract:

- [`orderbook_naive.zig`](../../src/zig/core/orderbook_naive.zig) — ordered price buckets and per-bucket FIFO links. Default and easiest to read.
- [`orderbook_direct.zig`](../../src/zig/core/orderbook_direct.zig) — denser indexes, intrusive order links, and optional static order/bucket pools for latency presets.

They keep asks from lowest to highest price, bids from highest to lowest price, and FIFO order within a price level: price priority first, then insertion priority.

Symbol definitions are broadcast by binary command. Each matching shard has the symbol definition, but ordinary order commands mutate only the router selected by the symbol mask. Resting orders for a symbol exist only in that active shard’s book.

Kernel routing lives in [`ExchangeKernel`](../../src/zig/core/exchange_kernel.zig) (`Book` union, `processCommand` on the selected implementation). Multi-shard ownership is enforced by `MultiShardKernelSet` / `ExchangeApiSharded`.

## Supported order behavior

| Type | Current behavior |
|---|---|
| `gtc` | Match immediately at acceptable prices; if volume remains, rest it in the book. |
| `ioc` | Match immediately up to the limit; attach a `REJECT` event for any remainder; never rest. |
| `fok_budget` | Pre-scan available liquidity and aggregate price. Execute all only when full size is available within the total buy/sell budget; otherwise reject all. |
| `ioc_budget` | Present in the enum but unsupported → full reject event path. |
| `fok` | Present in the enum but unsupported → full reject event path. |

An unsupported or unfilled order can still finish with matching `success`; the outcome is represented by the attached `REJECT` event. Integrations must consume both `result_code` and matcher events.

Static capacity exhaustion (order/bucket pools) can surface as `matching_engine_capacity` — a Zig-native code, not historical Java.

## Price-time matching flow

```mermaid
flowchart TD
    Start["Valid place_order reaches symbol book"] --> Type{"Order type"}
    Type -- "gtc" --> Match["Walk best opposite price, FIFO within bucket"]
    Type -- "ioc" --> Match
    Type -- "fok_budget" --> Budget["Pre-calculate cost/proceeds for full size"]
    Type -- "other" --> RejectAll["Attach REJECT for full size"]
    Budget --> Fits{"Full liquidity and budget satisfied?"}
    Fits -- "no" --> RejectAll
    Fits -- "yes" --> Match
    Match --> Trade["For each maker: fill min(remaining, maker remaining)"]
    Trade --> Event["Append TRADE event at maker price"]
    Event --> More{"Taker remaining and next price acceptable?"}
    More -- "yes" --> Trade
    More -- "no" --> Remainder{"Unfilled remainder?"}
    Remainder -- "gtc" --> Rest["Insert remaining order at limit price"]
    Remainder -- "ioc" --> RejectPart["Prepend REJECT for remainder"]
    Remainder -- "none" --> Done["Return success"]
    Rest --> Done
    RejectPart --> Done
    RejectAll --> Done
```

Each trade executes at the resting maker’s price. A fully filled maker is removed from the order-ID index and its price bucket is deleted when empty. A partially filled maker retains its position and priority.

For a duplicate GTC order ID, the implementation first allows any immediately marketable portion to trade, then rejects the remainder instead of placing a second resting order with that ID.

## Matcher event chain

The matching engine communicates with risk and result consumers through a linked list of [`MatcherTradeEvent`](../../src/zig/core/events.zig) nodes attached to the response/command.

| Event | Meaning | Key fields |
|---|---|---|
| `trade` | Taker matched a resting maker | maker order/uid, maker price, size, maker/taker completion flags, bidder hold price |
| `reduce` | Cancel or reduce released resting volume | released size, original price and reserve, completion flag |
| `reject` | Incoming remainder did not enter the book | rejected size, command price/reserve |
| `binary_event` | Serialized report section, not a trade | section number plus payload words |

R2 ignores binary events. [`SimpleEventsProcessor`](../../src/zig/core/simple_events_processor.zig) converts trade/reduce/reject nodes into client-friendly callbacks and ignores non-simple kinds.

Event storage can be heap-per-node (default) or a static pool sized from ring capacity (`StaticCapacityConfig` / `matcher_event_pool_capacity`).

## Currency-exchange risk: R1 hold

For `currency_exchange_pair`, the owning risk shard looks up the user and symbol, calculates the maximum debit, and applies it speculatively:

```mermaid
flowchart TD
    P["place_order"] --> Side{"BID or ASK?"}
    Side -- "BID" --> Reserve{"Budget type?"}
    Reserve -- "no" --> Limit{"reserve_bid_price >= limit price?"}
    Limit -- "no" --> Bad["Reject risk"]
    Limit -- "yes" --> BidHold["Hold size * (reserve * quote_scale_k + taker_fee)"]
    Reserve -- "yes" --> Eq{"reserve == budget price?"}
    Eq -- "no" --> Bad
    Eq -- "yes" --> BudgetHold["Hold budget * quote_scale_k + size * taker_fee"]
    Side -- "ASK" --> Fee{"price * quote_scale_k >= taker_fee?"}
    Fee -- "no" --> Bad
    Fee -- "yes" --> AskHold["Hold size * base_scale_k"]
    BidHold --> Balance{"Account after hold + free futures margin >= 0?"}
    BudgetHold --> Balance
    AskHold --> Balance
    Balance -- "no" --> Revert["Revert speculative debit and reject"]
    Balance -- "yes" --> Valid["valid_for_matching_engine"]
```

If margin trading is enabled, unrealized futures P&L minus required futures margin in the same currency contributes to available funds. Many detailed placement failures collapse to `risk_nsf` for the caller; invalid user and invalid symbol retain specific codes.

Implementation: [`RiskEngine.placeOrderRiskCheck`](../../src/zig/core/risk.zig) → `placeOrder` → `placeExchangeOrder`.

## Currency-exchange settlement: R2

R1 has already removed the maximum held amount, so R2 mostly transfers proceeds and returns unused hold. Zig uses a **prepare-then-commit** path for exchange (and margin) releases so multi-user updates are atomic within a shard and settlement hooks can observe prepared fills.

### Incoming BID (taker buys base)

For every trade:

- the maker is an ASK and receives `trade quote amount - maker fee`;
- the taker accumulates bought base volume;
- the taker receives the difference between held reserve price and actual maker price;
- fees accumulate as taker fee plus maker fee per executed lot.

After the chain, the taker gains `executed size * base_scale_k`. A reject or reduce returns the quote amount held for the rejected size at its bidder hold price, including the taker-fee component.

### Incoming ASK (taker sells base)

For every trade:

- the maker is a BID and gains the bought base;
- the maker receives the unused difference between its reserved bid price and actual trade price, corrected from taker-fee hold to maker-fee charge;
- the taker accumulates actual quote proceeds.

After the chain, the taker receives `sum(size * maker_price * quote_scale_k) - taker fees`. A reject or reduce returns the unexecuted base hold.

```mermaid
sequenceDiagram
    participant R1 as R1 risk
    participant Book as Order book
    participant R2T as R2 owning taker
    participant R2M as R2 owning maker(s)

    R1->>R1: debit maximum taker hold
    R1->>Book: valid order
    Book->>Book: match at maker prices
    Book-->>R2T: TRADE / REJECT chain
    Book-->>R2M: same shared event chain
    R2T->>R2T: prepare then commit taker settlement
    R2M->>R2M: prepare then commit maker settlement
    R2T->>R2T: add taker fees to shard counter
    R2M->>R2M: add maker fees to shard counter
```

Every R2 shard scans the same event chain but updates only users selected by `uid & shard_mask`. One trade settles taker and makers on different risk shards without cross-shard locks.

Entry points: `prepareExchangeRiskRelease` / `commitPreparedExchangeRiskRelease`, or `handlerRiskRelease` when no symbol spec applies.

Optional [`settlement_lifecycle.Hooks`](../../src/zig/core/settlement_lifecycle.zig) can prepare external fill capture (for example TigerBeetle) between prepare and commit on the exchange path.

## Futures/margin flow

For `futures_contract`, account currency does not transfer between counterparties. Risk tracks exposure and realized/unrealized P&L in symbol position records (`user_profile.zig`).

R1:

1. finds or creates the user’s per-symbol position record;
2. calculates current required margin from open position plus pending buy/sell sizes;
3. permits an exposure-reducing order without additional margin;
4. otherwise compares new margin against cash plus P&L/free margin across positions sharing the quote currency;
5. increments pending buy/sell size when accepted.

R2 for each trade:

1. removes the executed size from pending exposure;
2. closes any opposite position and realizes P&L;
3. opens any remainder in the new direction;
4. charges maker/taker fee only on size that opens exposure;
5. removes an empty position and moves accumulated profit into the user’s cash account.

Reject and reduce events only release pending exposure. Periodic or requested L2 snapshots update the risk shard’s best ask/bid cache used for unrealized P&L estimates.

## Cancel, reduce, and move

### Cancel

The book verifies that both order ID and uid match, removes the entire remaining order, copies the original action into the command, and attaches a single `REDUCE` event. R2 uses the action, remaining size, and reserve price to release currency hold or pending margin.

### Reduce

A positive requested reduction is capped at the order’s remaining size. A full reduction removes the order; a partial reduction decreases the order and bucket volume in place. Either way one `REDUCE` event reports the effective amount.

### Move

Move removes the resting order from its old price, changes its price, and treats it as an active order against the opposite book. If it fully trades, it disappears; otherwise the remainder is inserted at the new price with new queue priority.

In currency-exchange mode, an existing BID cannot move above its original `reserve_bid_price`, because R1 does not reserve new funds for a move. That is why place carries a separate reserve price even when the initial limit is lower.

## Accounting invariants exposed by reports

The total currency balance report independently totals:

- free balances and position P&L from risk shards;
- held balances in open exchange orders from matching shards;
- collected fees;
- external adjustments and suspends;
- futures long and short open interest.

Tests expect the signed global balance sum to be zero and aggregated long open interest to equal aggregated short open interest. This report is the most useful end-to-end check that R1 holds, matching events, and R2 settlement stayed consistent.

Native report types live in `ExchangeKernel` (`TotalCurrencyBalanceReport`, `SingleUserReport`, `DetailedStateHashReport`). Java wire projections are produced separately via `compat/java/report_producer.zig` when exact-Java report lanes are requested.

[Previous: command processing](02-command-processing.md) · [Next: reports and persistence](04-reports-and-persistence.md)
