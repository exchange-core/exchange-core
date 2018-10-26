# exchange-core
[![Build Status](https://travis-ci.org/mzheravin/exchange-core.svg?branch=master)](https://travis-ci.org/mzheravin/exchange-core)

**Ultra-fast market exchange core matching engine** based on LMAX Disruptor and Eclipse Collections (ex. Goldman Sachs GS Collections).

Capable to process 5M order book operations per second on 7-years old hardware (Intel® Xeon® X5690) with moderate latency degradation:

|rate|50.0%|90.0%|95.0%|99.0%|99.9%|99.99%|worst|
|----|-----|-----|-----|-----|-----|------|-----|
|125K|0.7µs|0.9µs|1.0µs|1.3µs|9µs  |500µs |1.3ms|
|250K|0.7µs|0.9µs|1.0µs|1.3µs|10µs |60µs  |1.2ms|
|500K|0.7µs|0.9µs|1.0µs|1.4µs|14µs |30µs  |1.2ms|
|  1M|0.6µs|0.9µs|1.2µs|5µs  |22µs |900µs |1.2ms|
|  2M|0.7µs|1.3µs|4.5µs|11µs |30µs |1.1ms |1.2ms|
|  3M|0.8µs|4.3µs|7.0µs|16µs |35µs |45µs  | 70µs|
|  4M|1.1µs|7.0µs|9.5µs|25µs |43µs |55µs  | 80µs|
|  5M|1.7µs|9.6µs|14µs |35µs |50µs |65µs  | 90µs|
|  6M|6.0µs|22µs |40µs |380µs|500µs|550µs |600µs|

Peak throughput: 6.5M commands per second with awful latency (5-100ms).

Benchmark configuration:
- Single order book.
- 3,000,000 inbound messages are distributed as follows: 9% limit + 3% market new orders, 6% cancel operations, 82% move operations. About 6% commands are causing trades.
- 1,000 active user accounts.
- In average ~1,000 limit orders in the order book, placed in ~750 different price slots.
- Latency results are only for risk processing and matcing engine. Network interface latency, IPC, journslling are not included.
- Test data is not bursty, meaning constant interval between commands (0.2~8µs depending on target throughput).
- BBO prices are not changing significantly thoghout the test, no avalanche orders.
- No coordinated omission effect. Processing delay is always affecting latency measurements for following messages.
- GC is triggered prior running every benchmark cycle (of 3,000,000 messages).

### Main features
- HFT optimized. Priority is a limit-order-move operation mean latency (currently ~0.5µs). Cancel operation takes ~0.7µs, placing new order ~1.0µs;
- In-memory working state.
- Lock-free and contention-free orders matching and risk control algorithms.
- Matching engine and risk control operations are atomic and deterministic.
- Pipelined processing (based on LMAX Disruptor): each CPU core is responsible for different processing stage, user accounts shard, or symbol order books set.
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

