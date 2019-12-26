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
package exchange.core2.tests.util;

import exchange.core2.core.ExchangeApi;
import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.common.api.ApiCommand;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;

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
public class LatencyTestsModule {

    private static final boolean WRITE_HDR_HISTOGRAMS = false;

    public static void latencyTestImpl(final Supplier<ExchangeTestContainer> containerFactory,
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

        try (final AffinityLock cpuLock = AffinityLock.acquireLock();
             final ExchangeTestContainer container = containerFactory.get()) {

            final ExchangeApi api = container.getApi();
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
