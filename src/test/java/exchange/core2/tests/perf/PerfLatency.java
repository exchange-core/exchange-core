/*
 * Copyright 2019 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package exchange.core2.tests.perf;

import exchange.core2.tests.util.*;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;
import org.junit.Test;
import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.common.api.ApiCommand;
import exchange.core2.core.ExchangeApi;
import exchange.core2.tests.util.ExchangeTestContainer;
import exchange.core2.tests.util.LatencyTools;
import exchange.core2.tests.util.TestOrdersGenerator;
import exchange.core2.tests.util.UserCurrencyAccountsGenerator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.BitSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

@Slf4j
public final class PerfLatency {

    private static final boolean WRITE_HDR_HISTOGRAMS = false;

    /**
     * This is latency test for simplified conditions
     * - one symbol (margin mode)
     * - ~1K active users (2K currency accounts)
     * - 1K pending limit-orders (in one order book)
     * 6-threads CPU can run this test
     */
    @Test
    public void testLatencyMargin() {
        latencyTestImpl(() -> new ExchangeTestContainer(2 * 1024, 1, 1, 256, null),
                3_000_000,
                1_000,
                2_000,
                TestConstants.CURRENCIES_FUTURES,
                1,
                ExchangeTestContainer.AllowedSymbolTypes.FUTURES_CONTRACT,
                20);
    }

    /**
     * This is latency test for simplified conditions
     * - one symbol (exchange mode)
     * - ~1K active users (2K currency accounts)
     * - 1K pending limit-orders (in one order book)
     * 6-threads CPU can run this test
     */
    @Test
    public void testLatencyExchange() {
        latencyTestImpl(() -> new ExchangeTestContainer(2 * 1024, 1, 1, 256, null),
                3_000_000,
                1_000,
                2_000,
                TestConstants.CURRENCIES_EXCHANGE,
                1,
                ExchangeTestContainer.AllowedSymbolTypes.CURRENCY_EXCHANGE_PAIR,
                20);
    }

    /**
     * This is medium load latency test for verifying "triple million" capability:
     * - 1M active users (3M currency accounts)
     * - 1M pending limit-orders
     * - 1M+ messages per second throughput
     * - 100K symbols
     * - less than 1 millisecond 99.99% latency
     * 12-threads CPU and 32GiB RAM is required for running this test in 2+4 configuration.
     */
    @Test
    public void testLatencyMultiSymbolMedium() {
        latencyTestImpl(() -> new ExchangeTestContainer(32 * 1024, 4, 2, 256, null),
                6_000_000,
                1_000_000,
                3_300_000,
                TestConstants.ALL_CURRENCIES,
                100_000,
                ExchangeTestContainer.AllowedSymbolTypes.BOTH,
                10);
    }

    /**
     * This is high load latency test for verifying exchange core scalability:
     * - 10M active users (33M currency accounts)
     * - 30M pending limit-orders
     * - 1M+ messages per second throughput
     * - 200K symbols
     * - less than 1 millisecond 99.99% latency
     * 12-threads CPU and 32GiB RAM is required for running this test in 2+4 configuration.
     */
    @Test
    public void testLatencyMultiSymbolLarge() {
        latencyTestImpl(
                () -> new ExchangeTestContainer(64 * 1024, 4, 2, 256, null),
                40_000_000,
                30_000_000,
                33_000_000,
                TestConstants.ALL_CURRENCIES,
                200_000,
                ExchangeTestContainer.AllowedSymbolTypes.BOTH,
                10);
    }


    private void latencyTestImpl(final Supplier<ExchangeTestContainer> containerFactory,
                                 final int totalCommandsNumber,
                                 final int targetOrderBookOrdersTotal,
                                 final int numAccounts,
                                 final Set<Integer> currenciesAllowed,
                                 final int numSymbols,
                                 final ExchangeTestContainer.AllowedSymbolTypes allowedSymbolTypes,
                                 final int warmupCycles) {

        final int targetTps = 200_000; // transactions per second
        final int targetTpsStep = 100_000;

        final int warmupTps = 1_000_000;


        final List<CoreSymbolSpecification> coreSymbolSpecifications = ExchangeTestContainer.generateRandomSymbols(numSymbols, currenciesAllowed, allowedSymbolTypes);

        final List<BitSet> usersAccounts = UserCurrencyAccountsGenerator.generateUsers(numAccounts, currenciesAllowed);

        final TestOrdersGenerator.MultiSymbolGenResult genResult = TestOrdersGenerator.generateMultipleSymbols(
                coreSymbolSpecifications,
                totalCommandsNumber,
                usersAccounts,
                targetOrderBookOrdersTotal,
                1);

        try (final AffinityLock cpuLock = AffinityLock.acquireLock()) {
            try (ExchangeTestContainer container = containerFactory.get()) {

                final ExchangeApi api = container.api;
                final SingleWriterRecorder hdrRecorder = new SingleWriterRecorder(Integer.MAX_VALUE, 2);

                // TODO - first run should validate the output (orders are accepted and processed properly)

                final BiFunction<Integer, Boolean, Boolean> testIteration = (tps, warmup) -> {
                    try {

                        container.initBasicSymbols();
                        container.addSymbols(coreSymbolSpecifications);
                        container.userAccountsInit(usersAccounts);

                        hdrRecorder.reset();
                        final CountDownLatch latchFill = new CountDownLatch(genResult.getApiCommandsFill().size());
                        container.setConsumer(cmd -> latchFill.countDown());
                        genResult.getApiCommandsFill().forEach(api::submitCommand);
                        latchFill.await();

                        final CountDownLatch latchBenchmark = new CountDownLatch(genResult.getApiCommandsBenchmark().size());

                        container.setConsumer(cmd -> {
                            final long latency = System.nanoTime() - cmd.timestamp;
                            hdrRecorder.recordValue(Math.min(latency, Integer.MAX_VALUE));
                            latchBenchmark.countDown();
                        });

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
                        log.info("{} {}", tag, LatencyTools.createLatencyReportFast(histogram));

                        // compare orderBook final state just to make sure all commands executed same way
                        // TODO compare events, balances, positions
                        coreSymbolSpecifications.forEach(
                                symbol -> assertEquals(genResult.getGenResults().get(symbol.symbolId).getFinalOrderBookSnapshot(), container.requestCurrentOrderBook(symbol.symbolId)));

                        if (WRITE_HDR_HISTOGRAMS) {
                            final PrintStream printStream = new PrintStream(new File(System.currentTimeMillis() + "-" + perfMt + ".perc"));
                            //log.info("HDR 50%:{}", hdr.getValueAtPercentile(50));
                            histogram.outputPercentileDistribution(printStream, 1000.0);
                        }

                        container.resetExchangeCore();

                        System.gc();
                        Thread.sleep(300);

                        // stop testing if median latency above 1 millisecond
                        return warmup || histogram.getValueAtPercentile(50.0) < 1_000_000;

                    } catch (InterruptedException | FileNotFoundException ex) {
                        throw new IllegalStateException(ex);
                    }
                };

                log.debug("Warming up {} cycles...", warmupCycles);
                IntStream.range(0, warmupCycles)
                        .forEach(i -> testIteration.apply(warmupTps, true));
                log.debug("Warmup done, starting tests");

                final boolean ignore = IntStream.range(0, 10000)
                        .map(i -> targetTps + targetTpsStep * i)
                        .mapToObj(tps -> testIteration.apply(tps, false))
                        .allMatch(x -> x);
            }
        }
    }
}