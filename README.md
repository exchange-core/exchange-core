# exchange-core

**Ultra-fast market exchange core matching engine** based on LMAX Disruptor and Eclipse Collections (ex. Goldman Sachs GS Collections).

Capable to process 5M commands per second on 7-years old hardware (Intel® Xeon® X5690) without significant latency degradation:

|rate|50.0%|90.0%|95.0%|99.0%|99.9%|99.99%|worst|
|----|-----|-----|-----|-----|-----|------|-----|
|125K|1.2µs|1.6µs|1.8µs|8µs  |95µs |300µs |1ms  |
|250K|1.2µs|1.6µs|1.8µs|12µs |90µs |250µs |500µs|
|500K|1.2µs|1.6µs|1.9µs|17µs |130µs|250µs |500µs|
|  1M|1.0µs|1.4µs|2.5µs|20µs |145µs|300µs |400µs|
|  2M|0.8µs|4.5µs|8.7µs|30µs |130µs|250µs |330µs|
|  3M|0.7µs|4.0µs|7.0µs|28µs |140µs|500µs |400µs|
|  4M|0.8µs|6.0µs|9.0µs|30µs |60µs |160µs |200µs|
|  5M|2.8µs|20µs |30µs |70µs |300µs|350µs |380µs|
|  6M|5.7µs|34µs |260µs|600µs|680µs|710µs |740µs|

Peak performance: 6.7M commands per second with awful latency (5-100ms).

Benchmark configuration:
- Single order book.
- 3,000,000 inbound messages are distributed as follows: 9% limit + 3% market new orders, 6% cancel operations, 82% move operations. About 6% commands are causing trades.
- 1,000 active user accounts.
- In average ~1,000 limit orders in the order book.
- Latency results do not include network interface and IPC.
- Test data is not bursty, constant interval between commands (very short though: 200ns-8µs).
- BBO prices are not changing significantly thoghout the test, no avalanche orders.
- No coordinated omission effect - processing delay affects following measurements.
- GC is triggered prior running every benchmark cycle or 3,000,000 messages.

### Main features
- HFT optimized. Priority is a limit-order-move operation mean latency (0.5µs). Cancel operation takes about 0.7µs, placing new order - 1.0µs;
- In-memory working state.
- Lock-free and contention-free orders matching and risk control algorithms.
- Matching engine and risk control operations are atomic and deterministic.
- Pipelined processing (based on LMAX Disruptor), each CPU core is responsible for different processing stage, user accounts shard, or symbol order books set.
- Low GC pressure, objects pooling.
- Supports crossing Ask-Bid orders for market makers.
- Two implementations of matching engine: simple and optimized.
- Testing - unit-tests, integration tests, stress tests, integrity tests.

### TODOs
- Journaling support, event-sourcing - snapshot and replay operations support.
- Market data feeds (full order log, L2 market data, BBO, trades).
- Clearing and settlement.
- FIX and REST API gateways.
- More tests and benchmarks.
- NUMA-aware and thread affinity support.


### How to run tests
- Latency test: mvn -Dtest=ExchangeCorePerformance#latencyTest test
- Throughput test: mvn -Dtest=ExchangeCorePerformance#throughputTest test

