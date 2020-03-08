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
import exchange.core2.core.common.config.InitialStateConfiguration;
import lombok.extern.slf4j.Slf4j;

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
public class JournalingTestsModule {


    public static void journalingTestImpl(final Function<InitialStateConfiguration, ExchangeTestContainer> containerFactory,
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
                    .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER_PLUS_QUARTER)
                    .build();

            final TestOrdersGenerator.MultiSymbolGenResult genResult = TestOrdersGenerator.generateMultipleSymbols(genConfig);

            final long originalFinalStateHash;
            final float originalPerfMt;

            final String exchangeId = ExchangeTestContainer.timeBasedExchangeId();

            try (final ExchangeTestContainer container = containerFactory.apply(InitialStateConfiguration.cleanStartJournaling(exchangeId))) {

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
                stateId = tc * 1000 + iteration;
                container.submitMultiCommandSync(ApiPersistState.builder().dumpId(stateId).build());


                log.info("Benchmarking original state...");
                final List<ApiCommand> apiCommandsBenchmark = genResult.getApiCommandsBenchmark();
                final int size = apiCommandsBenchmark.size();
                final CountDownLatch latchBenchmark = new CountDownLatch(size);
                container.setConsumer(cmd -> latchBenchmark.countDown());

                originalPerfMt = container.executeTestingThread(() -> {
                    final long tStart = System.currentTimeMillis();
                    apiCommandsBenchmark.forEach(api::submitCommand);
                    latchBenchmark.await();
                    final long tDuration = System.currentTimeMillis() - tStart;
                    return size / (float) tDuration / 1000.0f;
                });

                assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());
                originalFinalStateHash = container.requestStateHash();

                log.info("{}. original throughput: {} MT/s", iteration, String.format("%.3f", originalPerfMt));

                // TODO save hash?
            }

            System.gc();
            Thread.sleep(200);


            // TODO Discover snapshots and journals with DiskSerializationProcessor
            final long snapshotBaseSeq = 0L;

            log.debug("Creating new exchange from persisted state...");
            final long tLoad = System.currentTimeMillis();
            try (final ExchangeTestContainer recreatedContainer = containerFactory.apply(
                    InitialStateConfiguration.lastKnownStateFromJournal(exchangeId, stateId, snapshotBaseSeq))) {

                // simple sync query in order to wait until core is started to respond
                recreatedContainer.validateUserState(0, IGNORING_CONSUMER, IGNORING_CONSUMER);

                float loadTimeSec = (float) (System.currentTimeMillis() - tLoad) / 1000.0f;
                log.debug("Load+start+replay time: {}s", String.format("%.3f", loadTimeSec));

                final long restoredStateHash = recreatedContainer.requestStateHash();
                assertThat(restoredStateHash, is(originalFinalStateHash));

                assertTrue(recreatedContainer.totalBalanceReport().isGlobalBalancesAllZero());
                log.info("Restored snapshot+journal is valid");

//                final ExchangeApi api = recreatedContainer.getApi();
//                List<ApiCommand> apiCommandsBenchmark = genResult.getApiCommandsBenchmark();
//                final CountDownLatch latchBenchmark = new CountDownLatch(apiCommandsBenchmark.size());
//                recreatedContainer.setConsumer(cmd -> latchBenchmark.countDown());
//
//                final float perfMt = recreatedContainer.executeTestingThread(() -> {
//
//                    final long tStart = System.currentTimeMillis();
//                    apiCommandsBenchmark.forEach(api::submitCommand);
//                    latchBenchmark.await();
//                    final long tDuration = System.currentTimeMillis() - tStart;
//
//                    assertTrue(recreatedContainer.totalBalanceReport().isGlobalBalancesAllZero());
//
//                    return apiCommandsBenchmark.size() / (float) tDuration / 1000.0f;
//                });

//                final float perfRatioPerc = perfMt / originalPerfMt * 100f;
//                log.info("{}. restored throughput: {} MT/s ({}%)", iteration, String.format("%.3f", perfMt), String.format("%.1f", perfRatioPerc));
            }

            System.gc();
            Thread.sleep(200);
        }

    }


    private static final Consumer<? super Object> IGNORING_CONSUMER = x -> {
    };

}
