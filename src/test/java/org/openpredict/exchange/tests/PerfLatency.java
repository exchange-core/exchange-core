package org.openpredict.exchange.tests;

import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;
import org.junit.Test;
import org.openpredict.exchange.beans.CoreSymbolSpecification;
import org.openpredict.exchange.beans.api.ApiCommand;
import org.openpredict.exchange.tests.util.TestOrdersGenerator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.openpredict.exchange.tests.util.LatencyTools.createLatencyReportFast;

@Slf4j
public final class PerfLatency extends IntegrationTestBase {

    private static final boolean WRITE_HDR_HISTOGRAMS = false;

    /**
     * This is latency test for simplified conditions
     * - one symbol
     * - 1K active users (~2K currency accounts)
     * - 1K pending limit-orders (in one order book)
     * 6-threads processor can run this test
     */

    @Test
    public void latencyTest() {
        initExchange();
        latencyTestImpl(
                3_000_000,
                1_000,
                1_000,
                CURRENCIES_FUTURES,
                1,
                AllowedSymbolTypes.FUTURES_CONTRACT,
                7_000_000);
    }

    /**
     * This is high load latency test for verifying "triple million" capability:
     * - 1M active users (~5M currency accounts)
     * - 1M pending limit-orders (in 384 order books)
     * - at least 1M messages per second throughput
     * 12-threads processor is required for running this test in 4+4 configuration.
     */
    @Test
    public void latencyMultiSymbol() {
        initExchange(128 * 1024, 4, 4, 1024);
        latencyTestImpl(
                5_000_000,
                1_000_000,
                1_000_000,
                ALL_CURRENCIES,
                384,
                AllowedSymbolTypes.BOTH,
                4_000_000);
    }


    private void latencyTestImpl(final int totalTransactionsNumber,
                                 final int targetOrderBookOrdersTotal,
                                 final int numUsers,
                                 final Set<Integer> currenciesAllowed,
                                 final int numSymbols,
                                 final AllowedSymbolTypes allowedSymbolTypes,
                                 final int targetTpsEnd) {

//        int targetTps = 1000000; // transactions per second
        final int targetTps = 200_000; // transactions per second
        final int targetTpsStep = 50_000;

        final int warmupTps = 1_000_000;
        final int warmupCycles = 20;
//        int targetTps = 4_000_000; // transactions per second

        try (AffinityLock cpuLock = AffinityLock.acquireCore()) {

            final List<CoreSymbolSpecification> coreSymbolSpecifications = generateAndAddSymbols(numSymbols, currenciesAllowed, allowedSymbolTypes);

            TestOrdersGenerator.MultiSymbolGenResult genResult = TestOrdersGenerator.generateMultipleSymbols(coreSymbolSpecifications,
                    totalTransactionsNumber,
                    numUsers,
                    targetOrderBookOrdersTotal);


            final SingleWriterRecorder hdrRecorder = new SingleWriterRecorder(Integer.MAX_VALUE, 2);

            // TODO - first run should validate the output (orders are accepted and processed properly)

            IntConsumer testIteration = tps -> {
                try {

                    initBasicSymbols();
                    coreSymbolSpecifications.forEach(super::addSymbol);
                    usersInit(numUsers, currenciesAllowed);

                    hdrRecorder.reset();
                    final CountDownLatch latchFill = new CountDownLatch(genResult.getApiCommandsFill().size());
                    consumer = cmd -> latchFill.countDown();
                    genResult.getApiCommandsFill().forEach(api::submitCommand);
                    latchFill.await();

                    final CountDownLatch latchBenchmark = new CountDownLatch(genResult.getApiCommandsBenchmark().size());

                    consumer = cmd -> {
                        final long latency = System.nanoTime() - cmd.timestamp;
                        hdrRecorder.recordValue(Math.min(latency, Integer.MAX_VALUE));
                        latchBenchmark.countDown();
                    };

                    final int nanosPerCmd = 1_000_000_000 / tps;
                    final long startTimeMs = System.currentTimeMillis();

                    long plannedTimestamp = System.nanoTime();

                    for (ApiCommand cmd : genResult.getApiCommandsBenchmark()) {
                        while (System.nanoTime() < plannedTimestamp) {
                            // spin while too early for sending next message
                        }
                        cmd.timestamp = plannedTimestamp;
                        api.submitCommand(cmd);
                        plannedTimestamp += nanosPerCmd;
                    }

                    latchBenchmark.await();
                    final long processingTimeMs = System.currentTimeMillis() - startTimeMs;
                    final float perfMt = (float) genResult.getApiCommandsBenchmark().size() / (float) processingTimeMs / 1000.0f;
                    String tag = String.format("%.3f MT/s", perfMt);
                    final Histogram histogram = hdrRecorder.getIntervalHistogram();
                    log.info("{} {}", tag, createLatencyReportFast(histogram));

                    // compare orderBook final state just to make sure all commands executed same way
                    // TODO compare events, balances, portfolios
                    coreSymbolSpecifications.forEach(
                            symbol -> assertEquals(genResult.getGenResults().get(symbol.symbolId).getFinalOrderBookSnapshot(), requestCurrentOrderBook(symbol.symbolId)));

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