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
    public void latencyTestTripleMillion() {
        latencyTestImpl(
                8_000_000,
                1_075_000,
                1_000_000);
    }

    private void latencyTestImpl(final int numOrders, final int targetOrderBookOrders, final int numUsers) {

//        int targetTps = 1000000; // transactions per second
        final int targetTps = 100_000; // transactions per second
        final int targetTpsEnd = 8_000_000;
        final int targetTpsStep = 25_000;

        final int warmupTps = 1_000_000;
        final int warmupCycles = 20;
//        int targetTps = 4_000_000; // transactions per second

        try (AffinityLock cpuLock = AffinityLock.acquireCore()) {

            TestOrdersGenerator.GenResult genResult = TestOrdersGenerator.generateCommands(numOrders, targetOrderBookOrders, numUsers, SYMBOL_MARGIN, false);
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
                    String tag = String.format("%.3f MT/s", perfMt);
                    Histogram histogram = hdrRecorder.getIntervalHistogram();
                    log.info("{} {}", tag, createLatencyReportFast(histogram));

                    // compare orderBook final state just to make sure all commands executed same way
                    // TODO compare events, balances, portfolios
                    assertEquals(genResult.getFinalOrderBookSnapshot(), requestCurrentOrderBook(SYMBOL_MARGIN));

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

            log.debug("Warming up {} cycles...", warmupCycles);
            IntStream.range(0, warmupCycles)
                    .forEach(i -> testIteration.accept(warmupTps));
            log.debug("Warmup done, starting tests");

            IntStream.range(0, 10000)
                    .map(i -> targetTps + targetTpsStep * i)
                    .filter(tps -> tps <= targetTpsEnd)
                    .forEach(testIteration);
        }
    }
}