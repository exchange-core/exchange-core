package org.openpredict.exchange.tests;

import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;
import org.junit.Test;
import org.openpredict.exchange.beans.api.ApiCommand;
import org.openpredict.exchange.tests.util.TestOrdersGenerator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.openpredict.exchange.tests.util.LatencyTools.createLatencyReportFast;

@Slf4j
public final class PerfLatency extends IntegrationTestBase {

    private static final boolean WRITE_HDR_HISTOGRAMS = false;

    @Test
    public void latencyTest() {
        latencyTestImpl(
                3_000_000,
                1_000,
                1_000);
    }

    @Test
    public void latencyTestTripleMillion() throws Exception {
        latencyTestImpl(
                8_000_000,
                1_100_000,
                1_000_000);
    }

    private void latencyTestImpl(final int numOrders, final int targetOrderBookOrders, final int numUsers) {

//        int targetTps = 1000000; // transactions per second
        final int targetTps = 100_000; // transactions per second
        final int targetTpsEnd = 8_000_000;
//        int targetTps = 4_000_000; // transactions per second

        try (AffinityLock cpuLock = AffinityLock.acquireCore()) {

            TestOrdersGenerator.GenResult genResult = TestOrdersGenerator.generateCommands(numOrders, targetOrderBookOrders, numUsers, SYMBOL, false);
            List<ApiCommand> apiCommands = TestOrdersGenerator.convertToApiCommand(genResult.getCommands());

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
                    assertEquals(genResult.getFinalOrderBookSnapshot(), requestCurrentOrderBook());

                    if (WRITE_HDR_HISTOGRAMS) {
                        PrintStream printStream = new PrintStream(new File(System.currentTimeMillis() + "-" + perfMt + ".perc"));
                        //log.info("HDR 50%:{}", hdr.getValueAtPercentile(50));
                        histogram.outputPercentileDistribution(printStream, 1000.0);
                    }

                    resetExchangeCore();

                    System.gc();
                    Thread.sleep(300);

                } catch (InterruptedException | FileNotFoundException ex) {
                    ex.printStackTrace();
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


}