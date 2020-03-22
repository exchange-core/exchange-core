# exchange-core
[![Build Status](https://travis-ci.org/mzheravin/exchange-core.svg?branch=master)](https://travis-ci.org/mzheravin/exchange-core)
[![][license img]][license]

**Ultra-fast market exchange core matching engine** based on 
[LMAX Disruptor](https://github.com/LMAX-Exchange/disruptor), 
[Eclipse Collections](https://www.eclipse.org/collections/) (ex. Goldman Sachs GS Collections) 
and [Adaptive Radix Trees](https://db.in.tum.de/~leis/papers/ART.pdf).

Designed for high scalability and pauseless 24/7 operation under high-load conditions and providing low-latency responses:
- 1M users having 3M accounts int total;
- 100K symbols (order books);
- 1M+ orders book operations per second;
- less than 1ms worst wire-to-wire latency.

Single order book configuration is capable to process 5M operations per second on 10-years old hardware (Intel® Xeon® X5690) with moderate latency degradation:

|rate|50.0%|90.0%|95.0%|99.0%|99.9%|99.99%|worst|
|----|-----|-----|-----|-----|-----|------|-----|
|125K|0.6µs|0.9µs|1.0µs|1.4µs|4µs  |24µs  |41µs |
|250K|0.6µs|0.9µs|1.0µs|1.4µs|9µs  |27µs  |41µs |
|500K|0.6µs|0.9µs|1.0µs|1.6µs|14µs |29µs  |42µs |
|  1M|0.5µs|0.9µs|1.2µs|4µs  |22µs |31µs  |45µs |
|  2M|0.5µs|1.2µs|3.9µs|10µs |30µs |39µs  |60µs |
|  3M|0.7µs|3.6µs|6.2µs|15µs |36µs |45µs  |60µs |
|  4M|1.0µs|6.0µs|9µs  |25µs |45µs |55µs  |70µs |
|  5M|1.5µs|9.5µs|16µs |42µs |150µs|170µs |190µs|
|  6M|5µs  |30µs |45µs |300µs|500µs|520µs |540µs|
|  7M|60µs |1.3ms|1.5ms|1.8ms|1.9ms|1.9ms |1.9ms|

![Latencies HDR Histogram](hdr-histogram.png)

Benchmark configuration:
- Single symbol order book.
- 3,000,000 inbound messages are distributed as follows: 9% GTC orders, 3% IOC orders, 6% cancel commands, 82% move commands. About 6% of all messages are triggering one or more trades.
- 1,000 active user accounts.
- In average ~1,000 limit orders are active, placed in ~750 different price slots.
- Latency results are only for risk processing and orders matching. Other stuff like network interface latency, IPC, journaling is not included.
- Test data is not bursty, meaning constant interval between commands (0.2~8µs depending on target throughput).
- BBO prices are not changing significantly throughout the test. No avalanche orders.
- No coordinated omission effect for latency benchmark. Any processing delay affects measurements for next following messages.
- GC is triggered prior/after running every benchmark cycle (3,000,000 messages).
- RHEL 7.5, network-latency tuned profile, dual X5690 6 cores 3.47GHz, one socket isolated and tickless, spectre/meltdown protection disabled.
- Java version 8u192, newer Java 8 versions can have a [performance bug](https://bugs.java.com/bugdatabase/view_bug.do?bug_id=JDK-8221355)

### Main features
- HFT optimized. Priority is a limit-order-move operation mean latency (currently ~0.5µs). Cancel operation takes ~0.7µs, placing new order ~1.0µs;
- In-memory working state for accounting data and order books.
- Event-sourcing - disk journaling and journal replay support, state snapshots (serialization) and restore operations.
- Lock-free and contention-free orders matching and risk control algorithms.
- No floating-point arithmetic, no loss of significance is possible.
- Matching engine and risk control operations are atomic and deterministic.
- Pipelined multi-core processing (based on LMAX Disruptor): each CPU core is responsible for certain processing stage, user accounts shard, or symbol order books shard.
- Two different risk processing modes (specified per symbol): direct-exchange and margin-trade.
- Maker/taker fees (defined in quote currency units).
- 3 implementations of matching engine: reference simple implementation ("Naive"), small order books optimized ("Fast"), scalability optimized ("Direct").
- Testing - unit-tests, integration tests, stress tests, integrity/consistency tests.
- Low GC pressure, objects pooling, single ring-buffer.
- Threads affinity (requires JNA).
- User suspend/resume operation (reduces memory consumption).
- Core reports API (user balances, open interest).

### Usage examples
Create and start empty exchange core:
```
// simple async events handler
SimpleEventsProcessor eventsProcessor = new SimpleEventsProcessor(new IEventsHandler() {
    @Override
    public void tradeEvent(TradeEvent tradeEvent) {
        System.out.println("Trade event: " + tradeEvent);
    }

    @Override
    public void cancelEvent(CancelEvent cancelEvent) {
        System.out.println("Cancel event: " + cancelEvent);
    }

    @Override
    public void rejectEvent(RejectEvent rejectEvent) {
        System.out.println("Reject event: " + rejectEvent);
    }

    @Override
    public void commandResult(ApiCommandResult commandResult) {
        System.out.println("Command result: " + commandResult);
    }

    @Override
    public void orderBook(OrderBook orderBook) {
        System.out.println("OrderBook event: " + orderBook);
    }
});

// default exchange configuration
ExchangeConfiguration conf = ExchangeConfiguration.defaultBuilder().build();

// no serialization
Supplier<ISerializationProcessor> serializationProcessorFactory = () -> DummySerializationProcessor.INSTANCE;

// build exchange core
ExchangeCore exchangeCore = ExchangeCore.builder()
        .resultsConsumer(eventsProcessor)
        .serializationProcessorFactory(serializationProcessorFactory)
        .exchangeConfiguration(conf)
        .build();

// start up disruptor threads
exchangeCore.startup();

// get exchange API for publishing commands
ExchangeApi api = exchangeCore.getApi();
```

Create new symbol:
```
// currency code constants
final int currencyCodeXbt = 11;
final int currencyCodeLtc = 15;

// symbol constants
final int symbolXbtLtc = 241;

// create symbol specification and publish it
CoreSymbolSpecification symbolSpecXbtLtc = CoreSymbolSpecification.builder()
        .symbolId(symbolXbtLtc)         // symbol id
        .type(SymbolType.CURRENCY_EXCHANGE_PAIR)
        .baseCurrency(currencyCodeXbt)    // base = satoshi (1E-8)
        .quoteCurrency(currencyCodeLtc)   // quote = litoshi (1E-8)
        .baseScaleK(1_000_000L) // 1 lot = 1M satoshi (0.01 BTC)
        .quoteScaleK(10_000L)   // 1 price step = 10K litoshi
        .takerFee(1900L)        // taker fee 1900 litoshi per 1 lot
        .makerFee(700L)         // maker fee 700 litoshi per 1 lot
        .build();

future = api.submitBinaryDataAsync(new BatchAddSymbolsCommand(symbolSpecXbtLtc));
```

Create new users:
```
// create user uid=301
future = api.submitCommandAsync(ApiAddUser.builder()
        .uid(301L)
        .build());

// create user uid=302
future = api.submitCommandAsync(ApiAddUser.builder()
        .uid(302L)
        .build());
```

Perform deposits:
```
// first user deposits 20 LTC
future = api.submitCommandAsync(ApiAdjustUserBalance.builder()
        .uid(301L)
        .currency(currencyCodeLtc)
        .amount(2_000_000_000L)
        .transactionId(1L)
        .build());

// second user deposits 0.10 BTC
future = api.submitCommandAsync(ApiAdjustUserBalance.builder()
        .uid(302L)
        .currency(currencyCodeXbt)
        .amount(10_000_000L)
        .transactionId(2L)
        .build());
```

Place orders:
```
// first user places Good-till-Cancel Bid order
// he assumes BTCLTC exchange rate 154 LTC for 1 BTC
// bid price for 1 lot (0.01BTC) is 1.54 LTC => 1_5400_0000 litoshi => 10K * 15_400 (in price steps)
future = api.submitCommandAsync(ApiPlaceOrder.builder()
        .uid(301L)
        .orderId(5001L)
        .price(15_400L)
        .reservePrice(15_600L) // can move bid order up to the 1.56 LTC, without replacing it
        .size(12L) // order size is 12 lots
        .action(OrderAction.BID)
        .orderType(OrderType.GTC) // Good-till-Cancel
        .symbol(symbolXbtLtc)
        .build());

// second user places Immediate-or-Cancel Ask (Sell) order
// he assumes wost rate to sell 152.5 LTC for 1 BTC
future = api.submitCommandAsync(ApiPlaceOrder.builder()
        .uid(302L)
        .orderId(5002L)
        .price(15_250L)
        .size(10L) // order size is 10 lots
        .action(OrderAction.ASK)
        .orderType(OrderType.IOC) // Immediate-or-Cancel
        .symbol(symbolXbtLtc)
        .build());
```

Request order book:
```
future = api.submitCommandAsync(ApiOrderBookRequest.builder()
        .size(10)
        .symbol(symbolXbtLtc)
        .build());
```

GtC orders manipulations:
```
// first user moves remaining order to price 1.53 LTC
future = api.submitCommandAsync(ApiMoveOrder.builder()
        .uid(301L)
        .orderId(5001L)
        .newPrice(15_300L)
        .symbol(symbolXbtLtc)
        .build());
        
// first user cancel remaining order
future = api.submitCommandAsync(ApiCancelOrder.builder()
        .uid(301L)
        .orderId(5001L)
        .symbol(symbolXbtLtc)
        .build());
```

Check user balance and GtC orders:
```
Future<SingleUserReportResult> report = api.processReport(new SingleUserReportQuery(301), 0);
```

Check system balance:
```
// check fees collected
Future<TotalCurrencyBalanceReportResult> totalsReport = api.processReport(new TotalCurrencyBalanceReportQuery(), 0);
System.out.println("LTC fees collected: " + totalsReport.get().getFees().get(currencyCodeLtc));
```

### TODOs
- Market data feeds (full order log, L2 market data, BBO, trades).
- Clearing and settlement.
- FIX and REST API gateways.
- More tests and benchmarks.
- NUMA-aware.

### How to run performance tests
- Latency test: mvn -Dtest=PerfLatency#testLatencyMargin test
- Throughput test: mvn -Dtest=PerfThroughput#testThroughputMargin test
- Hiccups test: mvn -Dtest=PerfHiccups#testHiccups test
- Serialization test: mvn -Dtest=PerfPersistence#testPersistenceMargin test

[license]:LICENSE.txt
[license img]:https://img.shields.io/badge/License-Apache%202-blue.svg
