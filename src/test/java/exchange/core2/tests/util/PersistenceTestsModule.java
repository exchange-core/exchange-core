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
import exchange.core2.core.common.api.ApiPersistState;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommandType;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;

import java.util.BitSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@Slf4j
public class PersistenceTestsModule {

    public static void persistenceTestImpl(final Function<Long, ExchangeTestContainer> containerFactory,
                                           final int totalTransactionsNumber,
                                           final int targetOrderBookOrdersTotal,
                                           final int numAccounts,
                                           final int iterations,
                                           final Set<Integer> currenciesAllowed,
                                           final int numSymbols,
                                           final ExchangeTestContainer.AllowedSymbolTypes allowedSymbolTypes) throws InterruptedException, ExecutionException {

        for (int iteration = 0; iteration < iterations; iteration++) {

            final long stateId;
            final List<CoreSymbolSpecification> coreSymbolSpecifications = ExchangeTestContainer.generateRandomSymbols(numSymbols, currenciesAllowed, allowedSymbolTypes);
            final List<BitSet> usersAccounts = UserCurrencyAccountsGenerator.generateUsers(numAccounts, currenciesAllowed);

            final TestOrdersGeneratorConfig genConfig = TestOrdersGeneratorConfig.builder()
                    .coreSymbolSpecifications(coreSymbolSpecifications)
                    .totalTransactionsNumber(totalTransactionsNumber)
                    .usersAccounts(usersAccounts)
                    .targetOrderBookOrdersTotal(targetOrderBookOrdersTotal)
                    .seed(iteration)
                    .build();

            final TestOrdersGenerator.MultiSymbolGenResult genResult = TestOrdersGenerator.generateMultipleSymbols(genConfig);

            final long originalPrefillStateHash;
            final float originalPerfMt;

            try (AffinityLock cpuLock = AffinityLock.acquireLock()) {
                try (final ExchangeTestContainer container = containerFactory.apply(null)) {

                    final ExchangeApi api = container.getApi();

                    log.info("Init basic symbols...");
                    container.initBasicSymbols();

                    log.info("Loading {} symbols...", coreSymbolSpecifications.size());
                    container.addSymbols(coreSymbolSpecifications);

                    log.info("Loading {} users having {} accounts...", usersAccounts.size(), usersAccounts.stream().mapToInt(BitSet::cardinality).sum());
                    container.userAccountsInit(usersAccounts);

                    final List<ApiCommand> apiCommandsFill = genResult.getApiCommandsFill();
                    log.info("Order books pre-fill with {} orders...", apiCommandsFill.size());
                    final CountDownLatch latchFill = new CountDownLatch(apiCommandsFill.size());
                    container.setConsumer(cmd -> {
                        if (cmd.resultCode == CommandResultCode.SUCCESS
                                && (cmd.command == OrderCommandType.MOVE_ORDER || cmd.command == OrderCommandType.CANCEL_ORDER || cmd.command == OrderCommandType.PLACE_ORDER)) {
                            latchFill.countDown();
                        } else {
                            throw new IllegalStateException("Unexpected command");
                        }
                    });
                    apiCommandsFill.forEach(api::submitCommand);
                    latchFill.await();

                    container.setConsumer(cmd -> {
                    });

                    assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());

                    log.info("Persisting...");
                    final long tc = System.currentTimeMillis();
                    stateId = tc;
                    container.submitMultiCommandSync(ApiPersistState.builder().dumpId(stateId).build());
                    final float persistTimeSec = (float) (System.currentTimeMillis() - tc) / 1000.0f;
                    log.debug("Persisting time: {}s", String.format("%.3f", persistTimeSec));

                    originalPrefillStateHash = container.requestStateHash();

                    log.info("Benchmarking original state...");
                    List<ApiCommand> apiCommandsBenchmark = genResult.getApiCommandsBenchmark();
                    final CountDownLatch latchBenchmark = new CountDownLatch(apiCommandsBenchmark.size());
                    container.setConsumer(cmd -> latchBenchmark.countDown());
                    long t = System.currentTimeMillis();
                    apiCommandsBenchmark.forEach(api::submitCommand);
                    latchBenchmark.await();
                    t = System.currentTimeMillis() - t;

                    assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());

                    originalPerfMt = (float) apiCommandsBenchmark.size() / (float) t / 1000.0f;
                    log.info("{}. original speed: {} MT/s", iteration, String.format("%.3f", originalPerfMt));
                }

            }

            System.gc();
            Thread.sleep(200);

            log.debug("Creating new exchange from persisted state...");
            final long tLoad = System.currentTimeMillis();
            try (final ExchangeTestContainer recreatedContainer = containerFactory.apply(stateId)) {

                // simple sync query in order to wait until core is started to respond
                recreatedContainer.validateUserState(0, IGNORING_CONSUMER, IGNORING_CONSUMER);

                float loadTimeSec = (float) (System.currentTimeMillis() - tLoad) / 1000.0f;
                log.debug("Load+start time: {}s", String.format("%.3f", loadTimeSec));

                try (AffinityLock cpuLock = AffinityLock.acquireCore()) {

                    final long restoredPrefillStateHash = recreatedContainer.requestStateHash();
                    assertThat(restoredPrefillStateHash, is(originalPrefillStateHash));

                    assertTrue(recreatedContainer.totalBalanceReport().isGlobalBalancesAllZero());

                    log.info("Restored snapshot is valid, benchmarking original state...");
                    final ExchangeApi api = recreatedContainer.getApi();
                    List<ApiCommand> apiCommandsBenchmark = genResult.getApiCommandsBenchmark();
                    final CountDownLatch latchBenchmark = new CountDownLatch(apiCommandsBenchmark.size());
                    recreatedContainer.setConsumer(cmd -> latchBenchmark.countDown());
                    long t = System.currentTimeMillis();
                    apiCommandsBenchmark.forEach(api::submitCommand);
                    latchBenchmark.await();
                    t = System.currentTimeMillis() - t;

                    assertTrue(recreatedContainer.totalBalanceReport().isGlobalBalancesAllZero());

                    final float perfMt = (float) apiCommandsBenchmark.size() / (float) t / 1000.0f;
                    final float perfRatioPerc = perfMt / originalPerfMt * 100f;
                    log.info("{}. restored speed: {} MT/s ({}%)", iteration, String.format("%.3f", perfMt), String.format("%.1f", perfRatioPerc));
                }
            }

            System.gc();
            Thread.sleep(200);
        }

    }


    private static final Consumer<? super Object> IGNORING_CONSUMER = x -> {
    };

}
