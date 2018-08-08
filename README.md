# exchange-core

**Ultra-fast market exchange core matching engine** based on LMAX Disruptor and Eclipse Collections (ex. Goldman Sachs GS Collections).

Capable to process 6,000,000 TPS (transactions per second) on 7-years old hardware (Intel® Xeon® X5690) without significant latency degradation:

|TPS|50.0%|90.0%|99.0% |99.9%  |99.99% |99.999%|
|---|-----|-----|------|-------|-------|-------|
|1M |0.5µs|1.0µs|1.5µs,|45.0µs |244.0µs|428.0µs|
|2M |0.5µs|0.5µs|5.1µs,|42.0µs |129.0µs|229.0µs|
|3M |0.5µs|1.0µs|15.4µs|73.0µs |207.0µs|229.0µs|
|4M |0.5µs|1.0µs|11.8µs|63.0µs |183.0µs|223.0µs|
|5M |0.5µs|1.0µs|20.5µs|80.0µs |208.0µs|237.0µs|
|6M |0.5µs|2.0µs|25.6µs|120.0µs|205.0µs|223.0µs|

Peak performance: 10,000,000 TPS (but latency is awful - over 50ms).

Benchmark configuration:
- 1 order book
- 3,000,000 commands distributed as follows: 9% limit, 3% market orders, 6% cancel and 82% move operations. About 6% commands are triggering trades.
- 1,000 active accounts.
- 1,000 limit orders in order book (in average).
- Does not include network interface and IPC latency.
- Test data is not bursty, same interval between commands.
- Processing delay affects following commands latency (fair benchmarking).
- GC triggered each time before starting benchmark (3M commands).

### Features
- HFT optimized. Priority is a limit-order-move command latency.
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