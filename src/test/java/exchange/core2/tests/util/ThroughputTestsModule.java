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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import exchange.core2.core.common.config.InitialStateConfiguration;
import exchange.core2.core.common.config.PerformanceConfiguration;
import exchange.core2.core.common.config.SerializationConfiguration;
import lombok.extern.slf4j.Slf4j;

import java.util.stream.IntStream;


@Slf4j
public class ThroughputTestsModule {

    public static void throughputTestImpl(final PerformanceConfiguration performanceCfg,
                                          final TestDataParameters testDataParameters,
                                          final InitialStateConfiguration initialStateCfg,
                                          final SerializationConfiguration serializationCfg,
                                          final int iterations) {

        final ExchangeTestContainer.TestDataFutures testDataFutures = ExchangeTestContainer.prepareTestDataAsync(testDataParameters, 1);

        try (final ExchangeTestContainer container = ExchangeTestContainer.create(performanceCfg, initialStateCfg, serializationCfg)) {

            final float avgMt = container.executeTestingThread(
                    () -> (float) IntStream.range(0, iterations)
                            .mapToObj(j -> {
                                container.loadSymbolsUsersAndPrefillOrdersNoLog(testDataFutures);

                                final float perfMt = container.benchmarkMtps(testDataFutures.getGenResult().join().apiCommandsBenchmark.join());
                                log.info("{}. {} MT/s", j, String.format("%.3f", perfMt));

                                assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());

                                // compare orderBook final state just to make sure all commands executed same way
                                testDataFutures.coreSymbolSpecifications.join().forEach(
                                        symbol -> assertEquals(
                                                testDataFutures.getGenResult().join().getGenResults().get(symbol.symbolId).getFinalOrderBookSnapshot(),
                                                container.requestCurrentOrderBook(symbol.symbolId)));

                                // TODO compare events, balances, positions

                                container.resetExchangeCore();

                                System.gc();

                                return perfMt;
                            })
                            .mapToDouble(x -> x)
                            .average().orElse(0));

            log.info("Average: {} MT/s", avgMt);
        }
    }

}
