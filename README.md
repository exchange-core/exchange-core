# exchange-core

**Ultra-fast market exchange core matching engine** based on LMAX Disruptor and Eclipse Collections (ex. Goldman Sachs GS Collections).

Capable to process 5M commands per second on 7-years old hardware (Intel® Xeon® X5690) without significant latency degradation:

|    |50.0% |90.0% |99.0% |99.9% |99.99% |99.999%|
|----|------|------|------|------|-------|-------|
|125K|0.83µs|1.15µs|1.73µs|16.2µs|550.0µs|1.47ms |
|250K|0.83µs|1.09µs|1.92µs|22.1µs|572.0µs|1.35ms |
|500K|0.83µs|1.09µs|2.24µs|17.7µs|81.0µs |407.0µs|
|  1M|0.77µs|1.09µs|6.4µs |36.0µs|740.0µs|999.0µs|
|  2M|0.51µs|1.28µs|9.7µs |30.0µs|94.0µs |150.0µs|
|  3M|0.64µs|2.94µs|11.9µs|33.0µs|84.0µs |101.0µs|
|  4M|0.7µs |6.2µs |16.4µs|54.0µs|123.0µs|137.0µs|
|  5M|0.96µs|6.1µs |21.4µs|55.0µs|89.0µs |97.0µs |

Peak performance: 7M commands per second with awful latency.

Benchmark configuration:
- 1 order book
- 3,000,000 commands distributed as follows: 9% limit, 3% market orders, 6% cancel and 82% move operations. About 6% commands are triggering trades.
- 1,000 active accounts.
- 1,000 limit orders in order book (in average).
- Does not include network interface and IPC latency.
- Test data is not bursty, same interval between commands (very short though: 200ns-8µs).
- No coordinated omission effect. Any processing delay affects upcoming messages.
- GC triggered prior running benchmark cycle.

### Features
- HFT optimized. Priority is a limit-order-move command mean latency (0.5µs). Cancel takes about 0.7µs, new order - 1.0µs;
- In-memory working state.
- Lock-free and contention-free orders matching and risk control algorithms.
- Matching engine and risk control operations are atomic and deterministic.
- Pipeline processing design (LMAX Disruptor), each CPU core is responsible for different operation, accounts shard, or symbol order books set.
- Low GC pressure, often created objects are reused.
- Supports crossing Ask-Bid orders for market makers.
- Testing - unit-tests, integration tests, stress tests.
- Two implementations of matching engine: naive and optimized.

### TODOs
- Journaling support, event-sourcing - snapshot and replay operations support.
- Market data feeds (full order log, L2 market data, BBO, trades).
- Clearing and settlement.
- FIX and REST API gateways.
- More tests and benchmarks.
- NUMA-aware and thread affinity support.