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
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

@Slf4j
public class ThroughputTestsModule {


    public static void throughputTestImpl(final ExchangeTestContainer container,
                                          final int totalTransactionsNumber,
                                          final int targetOrderBookOrdersTotal,
                                          final int numAccounts,
                                          final int iterations,
                                          final Set<Integer> currenciesAllowed,
                                          final int numSymbols,
                                          final ExchangeTestContainer.AllowedSymbolTypes allowedSymbolTypes) throws Exception {

        try (final AffinityLock cpuLock = AffinityLock.acquireLock()) {

            final ExchangeApi api = container.api;

            final List<CoreSymbolSpecification> coreSymbolSpecifications = ExchangeTestContainer.generateRandomSymbols(numSymbols, currenciesAllowed, allowedSymbolTypes);

            final List<BitSet> usersAccounts = UserCurrencyAccountsGenerator.generateUsers(numAccounts, currenciesAllowed);

            final TestOrdersGenerator.MultiSymbolGenResult genResult = TestOrdersGenerator.generateMultipleSymbols(
                    coreSymbolSpecifications,
                    totalTransactionsNumber,
                    usersAccounts,
                    targetOrderBookOrdersTotal);

            List<Float> perfResults = new ArrayList<>();
            for (int j = 0; j < iterations; j++) {

                container.addSymbols(coreSymbolSpecifications);
                container.userAccountsInit(usersAccounts);

                assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());

                final CountDownLatch latchFill = new CountDownLatch(genResult.getApiCommandsFill().size());
                container.setConsumer(cmd -> latchFill.countDown());
                genResult.getApiCommandsFill().forEach(api::submitCommand);
                latchFill.await();

                final CountDownLatch latchBenchmark = new CountDownLatch(genResult.getApiCommandsBenchmark().size());
                container.setConsumer(cmd -> latchBenchmark.countDown());
                long t = System.currentTimeMillis();
                genResult.getApiCommandsBenchmark().forEach(api::submitCommand);
                latchBenchmark.await();
                t = System.currentTimeMillis() - t;
                float perfMt = (float) genResult.getApiCommandsBenchmark().size() / (float) t / 1000.0f;
                log.info("{}. {} MT/s", j, String.format("%.3f", perfMt));
                perfResults.add(perfMt);

                assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());

                // compare orderBook final state just to make sure all commands executed same way
                // TODO compare events, balances, positions
                coreSymbolSpecifications.forEach(
                        symbol -> assertEquals(genResult.getGenResults().get(symbol.symbolId).getFinalOrderBookSnapshot(), container.requestCurrentOrderBook(symbol.symbolId)));

                container.resetExchangeCore();

                System.gc();
                Thread.sleep(300);
            }

            float avg = (float) perfResults.stream().mapToDouble(x -> x).average().orElse(0);
            log.info("Average: {} MT/s", avg);
        }
    }

}
