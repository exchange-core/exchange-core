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
import exchange.core2.core.common.MatcherTradeEvent;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.api.ApiCommand;
import exchange.core2.core.common.api.ApiMoveOrder;
import exchange.core2.core.common.api.ApiPlaceOrder;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

@Slf4j
public class LatencyTestsModule {

    private static final boolean WRITE_HDR_HISTOGRAMS = false;

    private static int c = 0;

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

        final TestOrdersGeneratorConfig genConfig = TestOrdersGeneratorConfig.builder()
                .coreSymbolSpecifications(coreSymbolSpecifications)
                .totalTransactionsNumber(totalCommandsNumber)
                .usersAccounts(usersAccounts)
                .targetOrderBookOrdersTotal(targetOrderBookOrdersTotal)
                .seed(1)
                .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER)
                .build();

        final TestOrdersGenerator.MultiSymbolGenResult genResult = TestOrdersGenerator.generateMultipleSymbols(genConfig);

        try (final ExchangeTestContainer container = containerFactory.get()) {

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

            container.executeTestingThread(() -> {
                log.debug("Warming up {} cycles...", warmupCycles);
                IntStream.range(0, warmupCycles)
                        .forEach(i -> testIteration.apply(warmupTps, true));
                log.debug("Warmup done, starting tests");

                return IntStream.range(0, 10000)
                        .map(i -> targetTps + targetTpsStep * i)
                        .mapToObj(tps -> testIteration.apply(tps, false))
                        .allMatch(x -> x);
            });
        }
    }

    public static void individualLatencyTest(final Supplier<ExchangeTestContainer> containerFactory,
                                             final int totalCommandsNumber,
                                             final int targetOrderBookOrdersTotal,
                                             final int numAccounts,
                                             final Set<Integer> currenciesAllowed,
                                             final int numSymbols,
                                             final ExchangeTestContainer.AllowedSymbolTypes allowedSymbolTypes,
                                             final boolean hugeSizeIOC) {


        final List<CoreSymbolSpecification> coreSymbolSpecifications = ExchangeTestContainer.generateRandomSymbols(numSymbols, currenciesAllowed, allowedSymbolTypes);

        final List<BitSet> usersAccounts = UserCurrencyAccountsGenerator.generateUsers(numAccounts, currenciesAllowed);

        final TestOrdersGeneratorConfig genConfig = TestOrdersGeneratorConfig.builder()
                .coreSymbolSpecifications(coreSymbolSpecifications)
                .totalTransactionsNumber(totalCommandsNumber)
                .usersAccounts(usersAccounts)
                .targetOrderBookOrdersTotal(targetOrderBookOrdersTotal)
                .seed(1)
                .hugeSizeIOC(hugeSizeIOC)
                .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER)
                .build();

        final TestOrdersGenerator.MultiSymbolGenResult genResult = TestOrdersGenerator.generateMultipleSymbols(genConfig);

        final int[] minLatencies = new int[genResult.apiCommandsBenchmark.size()];
        Arrays.fill(minLatencies, Integer.MAX_VALUE);

        try (final ExchangeTestContainer container = containerFactory.get()) {

            // TODO - first run should validate the output (orders are accepted and processed properly)

            final Function<Integer, Boolean> testIteration = (step) -> {

                try {
                    final ExchangeApi api = container.getApi();


                    container.initBasicSymbols();
                    container.addSymbols(coreSymbolSpecifications);
                    container.userAccountsInit(usersAccounts);

                    final CountDownLatch latchFill = new CountDownLatch(genResult.getApiCommandsFill().size());
                    container.setConsumer(cmd -> latchFill.countDown());
                    genResult.getApiCommandsFill().forEach(api::submitCommand);
                    latchFill.await();

                    final List<ApiCommand> apiCommandsBenchmark = genResult.getApiCommandsBenchmark();
                    final int[] latencies = new int[apiCommandsBenchmark.size()];
                    final int[] matcherEvents = new int[apiCommandsBenchmark.size()];
                    c = 0;

                    final AtomicLong orderProgressCounter = new AtomicLong(0);
                    container.setConsumer(cmd -> {
                        final long latency = System.nanoTime() - cmd.timestamp;
                        long lat = Math.min(latency, Integer.MAX_VALUE);
                        int i = c++;

                        orderProgressCounter.set(cmd.timestamp);
                        latencies[i] = (int) lat;


                        MatcherTradeEvent matcherEvent = cmd.matcherEvent;
                        while (matcherEvent != null) {
                            matcherEvent = matcherEvent.nextEvent;
                            matcherEvents[i]++;
                        }

                    });


                    for (ApiCommand cmd : apiCommandsBenchmark) {
                        long t = System.nanoTime();
                        cmd.timestamp = t;
                        api.submitCommand(cmd);
                        while (orderProgressCounter.get() != t) {
                            // spin until command is processed
                        }
                    }


                    final Map<Class<? extends ApiCommand>, SingleWriterRecorder> commandsClassLatencies = new HashMap<>();

                    SingleWriterRecorder placeIocLatencies = new SingleWriterRecorder(Integer.MAX_VALUE, 2);
                    SingleWriterRecorder placeGtcLatencies = new SingleWriterRecorder(Integer.MAX_VALUE, 2);
                    SingleWriterRecorder moveOrderEvts0 = new SingleWriterRecorder(Integer.MAX_VALUE, 2);
                    SingleWriterRecorder moveOrderEvts1 = new SingleWriterRecorder(Integer.MAX_VALUE, 2);
                    SingleWriterRecorder moveOrderEvts2 = new SingleWriterRecorder(Integer.MAX_VALUE, 2);

                    SingleWriterRecorder minLatenciesHdr = new SingleWriterRecorder(Integer.MAX_VALUE, 2);

                    // TODO change to case based
                    List<SlowCommandRecord> slowCommands = new ArrayList<>(apiCommandsBenchmark.size());

                    for (int i = 0; i < apiCommandsBenchmark.size(); i++) {
                        final int latency = latencies[i];
                        final int minLatency = Math.min(minLatencies[i], latency);
                        minLatencies[i] = minLatency;
                        minLatenciesHdr.recordValue(minLatency);

                        final int matcherEventsNum = matcherEvents[i];
                        final ApiCommand apiCommand = apiCommandsBenchmark.get(i);
                        final Class<? extends ApiCommand> aClass = apiCommand.getClass();
                        final SingleWriterRecorder hdrSvr = commandsClassLatencies.compute(aClass, (k, v) -> v == null
                                ? (new SingleWriterRecorder(Integer.MAX_VALUE, 2))
                                : v);
                        hdrSvr.recordValue(minLatency);

                        slowCommands.add(new SlowCommandRecord(minLatency, i, apiCommand, matcherEventsNum));

                        if (apiCommand instanceof ApiPlaceOrder) {
                            if (((ApiPlaceOrder) apiCommand).orderType == OrderType.GTC) {
                                placeGtcLatencies.recordValue(minLatency);
                            } else {
                                placeIocLatencies.recordValue(minLatency);
                            }
                        } else if (apiCommand instanceof ApiMoveOrder) {
                            if (matcherEventsNum == 0) {
                                moveOrderEvts0.recordValue(minLatency);
                            } else if (matcherEventsNum == 1) {
                                moveOrderEvts1.recordValue(minLatency);
                            } else {
                                moveOrderEvts2.recordValue(minLatency);
                            }
                        }

                    }
                    log.info("command independent latencies:");
                    log.info("  Theoretical {}", LatencyTools.createLatencyReportFast(minLatenciesHdr.getIntervalHistogram()));
                    commandsClassLatencies.forEach((cls, hdr) -> {
                        log.info("  {} {}", cls.getSimpleName(), LatencyTools.createLatencyReportFast(hdr.getIntervalHistogram()));
                    });
                    log.info("  Place GTC   {}", LatencyTools.createLatencyReportFast(placeGtcLatencies.getIntervalHistogram()));
                    log.info("  Place IOC   {}", LatencyTools.createLatencyReportFast(placeIocLatencies.getIntervalHistogram()));
                    log.info("  Move 0  evt {}", LatencyTools.createLatencyReportFast(moveOrderEvts0.getIntervalHistogram()));
                    log.info("  Move 1  evt {}", LatencyTools.createLatencyReportFast(moveOrderEvts1.getIntervalHistogram()));
                    log.info("  Move 2+ evt {}", LatencyTools.createLatencyReportFast(moveOrderEvts2.getIntervalHistogram()));

                    slowCommands.sort(COMPARATOR_LATENCY_DESC);

                    log.info("Slowest commands (theoretical):");
                    slowCommands.stream().limit(100).forEach(
                            p -> log.info("{}. {} {} events:{} {}",
                                    String.format("%06X", p.seqNumber), LatencyTools.formatNanos(p.minLatency), p.apiCommand, p.eventsNum,
                                    p.eventsNum > 0 ? String.format("(%dns per matching)", p.minLatency / p.eventsNum) : ""));

                    // compare orderBook final state just to make sure all commands executed same way
                    // TODO compare events, balances, positions
                    coreSymbolSpecifications.forEach(
                            symbol -> assertEquals(genResult.getGenResults().get(symbol.symbolId).getFinalOrderBookSnapshot(), container.requestCurrentOrderBook(symbol.symbolId)));

                    container.resetExchangeCore();

                    System.gc();
                    Thread.sleep(300);

                    // stop testing if median latency above 1 millisecond
                    return true;

                } catch (InterruptedException ex) {
                    throw new IllegalStateException(ex);
                }
            };

            // running tests
            container.executeTestingThread(() -> IntStream.range(0, 32).mapToObj(testIteration::apply).allMatch(x -> x));
        }
    }

    @AllArgsConstructor
    private static class SlowCommandRecord {
        int minLatency;
        int seqNumber;
        ApiCommand apiCommand;
        int eventsNum;
    }

    static Comparator<SlowCommandRecord> COMPARATOR_LATENCY_DESC = Comparator.<SlowCommandRecord>comparingInt(c -> c.minLatency).reversed();
}


