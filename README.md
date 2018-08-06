# exchange-core

**Ultra-fast market exchange core matching engine** based on LMAX Disruptor and Eclipse Collections (ex. Goldman Sachs GS Collections).

Capable to process 6,000,000 TPS (transactions per second) on 7-years old hardware (Intel® Xeon® X5690) without significant latency degradation:
50.0%=0.5µs, 90.0%=2.0µs, 99.0%=25.6µs, 99.9%=120.0µs, 99.99%=205.0µs, 99.999%=223.0µs
Peak performance: 10,000,000 TPS (but latency is awful).
Benchmark configuration:
- 1 order book
- 3,000,000 commands distributed as follows: 9% limit, 3% market orders, 6% cancel and 82% move operations. About 6% commands are triggering trades.
- 1,000 active accounts.
- 1,000 limit orders in order book (in average).
- does not include network interface and IPC latency.

### Features
- HFT low-latency optimized. Fastest limit order move operation is a priority.
- In-memory working state
- Lock-free contention-free risk control and matching algorithms.
- Matching engine and risk control operations are atomic and deterministic.
- Pipeline processing design (LMAX Disruptor), each CPU core is responsible for different operation, accounts shard, or set of symbols.
- Low GC pressure, newly created objects are reused.
- Supports crossing Ask-Bid orders for market makers.
- Tests - unit-tests, integration tests, stress tests.
- Two implementations of matching engine: naive and optimized.

### Planned
- Journaling support, event-sourcing - snapshot and replay operations support.
- NUMA and thread affinity support.