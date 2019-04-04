package org.openpredict.exchange.tests;

import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.junit.Before;
import org.junit.Test;
import org.openpredict.exchange.beans.L2MarketData;
import org.openpredict.exchange.beans.api.*;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.core.ExchangeApi;
import org.openpredict.exchange.core.ExchangeCore;
import org.openpredict.exchange.tests.util.TestOrdersGenerator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.openpredict.exchange.util.LatencyTools.createLatencyReportFast;

@Slf4j
public class ExchangeCorePerformance {

    private ExchangeCore exchangeCore;
    private ExchangeApi api;

    private TestOrdersGenerator generator = new TestOrdersGenerator();

    private static final int SYMBOL = 5991;

    public static final int LATENCY_PRECISION_BITS = 8; // Latency precision ~0.4%

    public static final boolean WRITE_HDR_HISTOGRAMS = false;

    public static final Consumer<OrderCommand> NO_CONSUMER = cmd -> {
    };

    private long startTimeNs = 0;
    private long nextHiccupAcceptTimestampNs = 0;

    private volatile Consumer<OrderCommand> consumer;

    @Before
    public void before() {

        consumer = cmd -> {
        };
        exchangeCore = new ExchangeCore(cmd -> consumer.accept(cmd));
        exchangeCore.startup();
        api = exchangeCore.getApi();

    }


    // TODO shutdown disruptor if test fails
    // TODO start new disruptor for every test iteration (re-usage is not possible in real use case anyway)
    @Test
    public void throughputTest() throws Exception {

        int numOrders = 3_000_000;
        int targetOrderBookOrders = 1_000;

        int numUsers = 1_000;

        try (AffinityLock cpuLock = AffinityLock.acquireCore()) {
            TestOrdersGenerator.GenResult genResult = generator.generateCommands(numOrders, targetOrderBookOrders, numUsers, SYMBOL, false);
            List<ApiCommand> apiCommands = generator.convertToApiCommand(genResult.getCommands());

            List<Float> perfResults = new ArrayList<>();
            for (int j = 0; j < 100; j++) {

                initSymbol();
                usersInit(numUsers);

                final CountDownLatch latch = new CountDownLatch(apiCommands.size());
                consumer = cmd -> latch.countDown();
                long t = System.currentTimeMillis();
                for (ApiCommand cmd : apiCommands) {
                    api.submitCommand(cmd);
                }
                latch.await();
                t = System.currentTimeMillis() - t;
                float perfMt = (float) apiCommands.size() / (float) t / 1000.0f;
                log.info("{}. {} MT/s", j, perfMt);
                perfResults.add(perfMt);

                // compare orderBook final state just to make sure all commands executed same way
                // TODO compare events, balances, portfolios
                assertEquals(genResult.getFinalOrderBookSnapshot(), requestCurrentOrderBook(-1));

                resetExchangeCore();

                System.gc();
                Thread.sleep(300);
            }

            float avg = (float) perfResults.stream().mapToDouble(x -> x).average().orElse(0);
            log.info("Average: {} MT/s", avg);
        }
    }


    @Test
    public void latencyTest() {

        final int numOrders = 3_000_000;
        final int targetOrderBookOrders = 1000;
        final int numUsers = 1000;

//        int targetTps = 1000000; // transactions per second
        final int targetTps = 100_000; // transactions per second
        final int targetTpsEnd = 8_000_000;
//        int targetTps = 4_000_000; // transactions per second

        try (AffinityLock cpuLock = AffinityLock.acquireCore()) {

            TestOrdersGenerator.GenResult genResult = generator.generateCommands(numOrders, targetOrderBookOrders, numUsers, SYMBOL, false);
            List<ApiCommand> apiCommands = generator.convertToApiCommand(genResult.getCommands());

            SingleWriterRecorder hdrRecorder = new SingleWriterRecorder(10_000_000_000L, 3);

            // TODO - first run should validate the output (orders are accepted and processed properly)

            IntConsumer testIteration = tps -> {
                try {

                    initSymbol();
                    usersInit(numUsers);

                    hdrRecorder.reset();

                    final CountDownLatch latch = new CountDownLatch(apiCommands.size());
                    consumer = cmd -> {
                        hdrRecorder.recordValue((System.nanoTime() - cmd.timestamp));
                        latch.countDown();
                        //receiveCounter.lazySet(cmd.timestamp);
                    };

                    final int nanosPerCmd = (1_000_000_000 / tps);
                    final long startTimeMs = System.currentTimeMillis();

                    long plannedTimestamp = System.nanoTime();

                    for (ApiCommand cmd : apiCommands) {
                        while (System.nanoTime() < plannedTimestamp) {
                            // spin while too early for sending next message
                        }
                        cmd.timestamp = plannedTimestamp;
                        api.submitCommand(cmd);
                        plannedTimestamp += nanosPerCmd;
                    }

                    latch.await();
                    final long processingTimeMs = System.currentTimeMillis() - startTimeMs;
                    float perfMt = (float) apiCommands.size() / (float) processingTimeMs / 1000.0f;
                    String tag = String.format("%03f MT/s", perfMt);
                    Histogram histogram = hdrRecorder.getIntervalHistogram();
                    log.info("{} {}", tag, createLatencyReportFast(histogram));

                    // compare orderBook final state just to make sure all commands executed same way
                    // TODO compare events, balances, portfolios
                    assertEquals(genResult.getFinalOrderBookSnapshot(), requestCurrentOrderBook(-1));

                    if (WRITE_HDR_HISTOGRAMS) {
                        PrintStream printStream = new PrintStream(new File(System.currentTimeMillis() + "-" + perfMt + ".perc"));
                        //log.info("HDR 50%:{}", hdr.getValueAtPercentile(50));
                        histogram.outputPercentileDistribution(printStream, 1000.0);
                    }

                    resetExchangeCore();

                    System.gc();
                    Thread.sleep(300);

                } catch (InterruptedException | FileNotFoundException e) {
                    //
                }
            };

            log.debug("Warming up 20 cycles...");
            IntStream.range(0, 20)
                    .forEach(i -> testIteration.accept(1_000_000));
            log.debug("Warmup done, starting tests");

            IntStream.range(0, 10000)
                    .map(i -> targetTps + 25000 * i)
                    .filter(tps -> tps <= targetTpsEnd)
                    .forEach(testIteration);
        }
    }


    @Test
    public void hiccupsTest() {

        final int numOrders = 3_000_000;
        final int targetOrderBookOrders = 1000;
        final int numUsers = 1000;

        // will print each occurrence if latency>0.2ms
        final long hiccupThresholdNs = 200_000;

        final int targetTps = 500_000; // transactions per second

        try (AffinityLock cpuLock = AffinityLock.acquireCore()) {

            TestOrdersGenerator.GenResult genResult = generator.generateCommands(numOrders, targetOrderBookOrders, numUsers, SYMBOL, false);
            List<ApiCommand> apiCommands = generator.convertToApiCommand(genResult.getCommands());

            IntFunction<TreeMap<Instant, Long>> testIteration = tps -> {
                try {

                    System.gc();
                    Thread.sleep(300);

                    initSymbol();
                    usersInit(numUsers);

                    LongLongHashMap hiccupTimestampsNs = new LongLongHashMap(10000);

                    final CountDownLatch latch = new CountDownLatch(apiCommands.size());
                    consumer = cmd -> {
                        long now = System.nanoTime();
                        // skip other messages in delayed group
                        if (now < nextHiccupAcceptTimestampNs) {
                            return;
                        }
                        long diffNs = now - cmd.timestamp;
                        // register hiccup timestamps
                        if (diffNs > hiccupThresholdNs) {
                            hiccupTimestampsNs.put(cmd.timestamp, diffNs);
                            nextHiccupAcceptTimestampNs = cmd.timestamp + diffNs;
                        }
                        latch.countDown();
                    };

                    nextHiccupAcceptTimestampNs = Long.MIN_VALUE;

                    final int nanosPerCmd = (1_000_000_000 / tps);
                    final long startTimeMs = System.currentTimeMillis();

                    startTimeNs = System.nanoTime();
                    long plannedTimestamp = startTimeNs;

                    for (ApiCommand cmd : apiCommands) {
                        long currentTimeNs;
                        while ((currentTimeNs = System.nanoTime()) < plannedTimestamp) {
                            // spin while too early for sending next message
                        }
                        // setting current timestamp (not planned) for catching original hiccups only (this is different from latency test)
                        cmd.timestamp = currentTimeNs;
                        api.submitCommand(cmd);
                        plannedTimestamp += nanosPerCmd;
                    }

                    TreeMap<Instant, Long> sorted = new TreeMap<>();
                    // convert nanosecond timestamp into Instant
                    // not very precise, but for 1ms resolution is ok (0,05% accuracy is required)...
                    // delay (nanoseconds) merging as max value
                    hiccupTimestampsNs.forEachKeyValue((eventTimestampNs, delay) -> sorted.compute(
                            Instant.ofEpochMilli(startTimeMs + (eventTimestampNs - startTimeNs) / 1_000_000),
                            (k, d) -> d == null ? delay : Math.max(d, delay)));

                    resetExchangeCore();

                    return sorted;

                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }

            };

            log.debug("warming up...");
            IntStream.range(0, 2)
                    .forEach(i -> testIteration.apply(targetTps));
            log.debug("warmup done");

            IntStream.range(0, 2000)
                    .mapToObj(i -> testIteration.apply(targetTps))
                    .forEach(res -> {
                        if (res.isEmpty()) {
                            log.debug("no hiccups");
                        }
                        res.forEach((timestamp, delay) -> log.debug("{}: {}µs", timestamp, delay / 1000));
                    });
        }
    }

        /*

    // TODO fix - fails with - int DEFAULT_HOT_WIDTH = 1024;
    @Test
    public void latencyTestFailing() throws Exception {

        int numOrders = 3_000_000;
        int targetOrderBookOrders = 1000;
        int numUsers = 1000;

//        int targetTps = 1000000; // transactions per second
        int targetTps = 500_000; // transactions per second
        int targetTpsEnd = 7_000_000;
//        int targetTps = 4_000_000; // transactions per second

        List<Long> uids = Stream.iterate(1L, i -> i + 1).limit(numUsers).collect(Collectors.toList());

        TestOrdersGenerator.GenResult genResult = generator.generateCommands(numOrders, targetOrderBookOrders, uids, SYMBOL, true);
        List<ApiCommand> apiCommands = generator.convertToApiCommand(genResult.getCommands());

        IntLongHashMap latencies = new IntLongHashMap(20000);

        Consumer<OrderCommand> resultsConsumer = cmd -> {
            int key = (int) (System.nanoTime() - cmd.timestamp);
            key |= ((Integer.highestOneBit(key) - 1) >> LATENCY_PRECISION_BITS);
            latencies.updateValue(key, 0, x -> x + 1);
        };

        // TODO - first run should validate the output (orders are accepted and processed properly)

        for (int j = 0; j < 10000; j++) {

            int nanosPerCmd = 1_000_000_000 / targetTps;
            targetTps += 50_000;

            Thread.sleep(20);
            userProfileService.reset();
            matchingEngineRouter.reset();
            exchangeCore.setResultsConsumer(NO_CONSUMER);
            apiCore.submitCommand(ApiNoOp.builder().build());
            Thread.sleep(20);
            uids.forEach(this::userInit);
            System.gc();
            Thread.sleep(200);
            exchangeCore.setResultsConsumer(resultsConsumer);

            long t = System.currentTimeMillis();
            long plannedTimestamp = System.nanoTime();

            long latenciesSum = latencies.sum();

            for (ApiCommand cmd : apiCommands) {
                // calculate planned time and spin if too early
                while (System.nanoTime() < plannedTimestamp) {
                }
                cmd.timestamp = plannedTimestamp;
                apiCore.submitCommand(cmd);
                plannedTimestamp += nanosPerCmd;
            }

            // wait until last response received
            long expectedSum = latenciesSum + apiCommands.size();
            while (latencies.sum() != expectedSum) {
                //log.debug("commands not processed yet: {}", expectedSum - sum);
            }
            t = System.currentTimeMillis() - t;

            float perfMt = (float) apiCommands.size() / (float) t / 1000.0f;
            Map<String, String> fmtPlace = createLatencyReportFast(latencies);
            log.info("{}. {} MT/s {}", j, perfMt, fmtPlace);

            // weak compare orderBook final state just to make sure all commands executed same way
            // TODO compare events
            assertThat(matchingEngineRouter.getOrderBook(SYMBOL).hashCode(), is(genResult.getFinalOrderbookHash()));

//            if (j == 5) {
//                log.info("Warmup completed, RESET latency stat");
            latencies.clear();
//            }

            if (targetTps > targetTpsEnd) {
                break;
            }
        }
    }
*/


    private void initSymbol() throws InterruptedException {
        submitCommandSync(ApiAddSymbol.builder().depositBuy(22000).depositSell(32100).symbolId(SYMBOL).build());
    }


    public void usersInit(int numUsers) throws InterruptedException {
        final CountDownLatch usersLatch = new CountDownLatch(numUsers * 2);
        consumer = cmd -> usersLatch.countDown();
        LongStream.rangeClosed(1, numUsers).forEach(uid -> {
            api.submitCommand(ApiAddUser.builder().uid(uid).build());
            api.submitCommand(ApiAdjustUserBalance.builder().uid(uid).amount(2_000_000_000L).build());
        });
        usersLatch.await();
    }

    public void resetExchangeCore() throws InterruptedException {
        submitCommandSync(ApiReset.builder().build());
    }

    private void submitCommandSync(ApiCommand apiCommand) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        consumer = cmd -> latch.countDown();
        api.submitCommand(apiCommand);
        latch.await();
    }


    private L2MarketData requestCurrentOrderBook(int orderBookSize) {
        BlockingQueue<OrderCommand> queue = attachNewConsumerQueue();
        api.submitCommand(ApiOrderBookRequest.builder().symbol(SYMBOL).size(orderBookSize).build());
        OrderCommand orderBookCmd = waitForOrderCommands(queue, 1).get(0);
        L2MarketData actualState = orderBookCmd.marketData;
        assertNotNull(actualState);
        return actualState;
    }

    private BlockingQueue<OrderCommand> attachNewConsumerQueue() {
        final BlockingQueue<OrderCommand> results = new LinkedBlockingQueue<>();
        consumer = cmd -> results.add(cmd.copy());
        return results;
    }

    private List<OrderCommand> waitForOrderCommands(BlockingQueue<OrderCommand> results, int c) {
        return Stream.generate(() -> {
            try {
                return results.poll(10000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ex) {
                throw new IllegalStateException();
            }
        })
                .limit(c)
                .collect(Collectors.toList());
    }
}