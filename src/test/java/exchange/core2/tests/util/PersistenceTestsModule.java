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
import exchange.core2.core.common.config.PerformanceConfiguration;
import lombok.extern.slf4j.Slf4j;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@Slf4j
public class PersistenceTestsModule {

    // TODO current persistence test does not cover positions serialization

    public static void persistenceTestImpl(final PerformanceConfiguration performanceConfiguration,
                                           final TestDataParameters testDataParameters,
                                           final int iterations) throws InterruptedException, ExecutionException {

        for (int iteration = 0; iteration < iterations; iteration++) {

            final long stateId;

//            long t = System.currentTimeMillis();

            final ExchangeTestContainer.TestDataFutures testDataFutures = ExchangeTestContainer.prepareTestDataAsync(testDataParameters, iteration);

            final String exchangeId = String.format("%012X", System.currentTimeMillis());
            final InitialStateConfiguration firstStartConfig = InitialStateConfiguration.cleanStart(exchangeId);

            final long originalPrefillStateHash;
            final float originalPerfMt;

            try (final ExchangeTestContainer container = new ExchangeTestContainer(performanceConfiguration, firstStartConfig)) {

                final ExchangeApi api = container.getApi();

                log.info("Init basic symbols...");
                container.initBasicSymbols();

                // start loading symbols as soon as all symbols are ready
                final List<CoreSymbolSpecification> coreSymbolSpecifications = testDataFutures.coreSymbolSpecifications.get();
                log.info("Loading {} symbols...", coreSymbolSpecifications.size());
                container.addSymbols(coreSymbolSpecifications);

                // start creating accounts and perform deposits
                final List<BitSet> userAccounts = testDataFutures.usersAccounts.get();
                log.info("Loading {} users having {} accounts...", userAccounts.size(), userAccounts.stream().mapToInt(BitSet::cardinality).sum());
                container.userAccountsInit(userAccounts);

                final TestOrdersGenerator.MultiSymbolGenResult genResult = testDataFutures.genResult.get();
                final List<ApiCommand> apiCommandsFill = genResult.getApiCommandsFill();
//                log.info(">>> READY in {}ms", System.currentTimeMillis() - t);
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
                final float persistTimeSec = (float) (System.currentTimeMillis() - tc) / 1000.0f;
                log.debug("Persisting time: {}s", String.format("%.3f", persistTimeSec));

                originalPrefillStateHash = container.requestStateHash();

                log.info("Benchmarking original state...");
                List<ApiCommand> apiCommandsBenchmark = genResult.getApiCommandsBenchmark();
                final CountDownLatch latchBenchmark = new CountDownLatch(apiCommandsBenchmark.size());
                container.setConsumer(cmd -> latchBenchmark.countDown());

                originalPerfMt = container.executeTestingThread(() -> {
                    final long tStart = System.currentTimeMillis();
                    apiCommandsBenchmark.forEach(api::submitCommand);
                    latchBenchmark.await();
                    final long tDuration = System.currentTimeMillis() - tStart;
                    return apiCommandsBenchmark.size() / (float) tDuration / 1000.0f;
                });

                assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());

                log.info("{}. original throughput: {} MT/s", iteration, String.format("%.3f", originalPerfMt));
            }


            System.gc();
            Thread.sleep(200);

            final InitialStateConfiguration fromSnapshotConfig = InitialStateConfiguration.fromSnapshotOnly(exchangeId, stateId, 0);

            log.debug("Creating new exchange from persisted state...");
            final long tLoad = System.currentTimeMillis();
            try (final ExchangeTestContainer recreatedContainer = new ExchangeTestContainer(performanceConfiguration, fromSnapshotConfig)) {

                // simple sync query in order to wait until core is started to respond
                recreatedContainer.totalBalanceReport();

                float loadTimeSec = (float) (System.currentTimeMillis() - tLoad) / 1000.0f;
                log.debug("Load+start time: {}s", String.format("%.3f", loadTimeSec));

                final long restoredPrefillStateHash = recreatedContainer.requestStateHash();
                assertThat(restoredPrefillStateHash, is(originalPrefillStateHash));

                assertTrue(recreatedContainer.totalBalanceReport().isGlobalBalancesAllZero());
                log.info("Restored snapshot is valid, benchmarking original state...");

                final ExchangeApi api = recreatedContainer.getApi();
                final TestOrdersGenerator.MultiSymbolGenResult genResult = testDataFutures.genResult.get();
                List<ApiCommand> apiCommandsBenchmark = genResult.getApiCommandsBenchmark();
                final CountDownLatch latchBenchmark = new CountDownLatch(apiCommandsBenchmark.size());
                recreatedContainer.setConsumer(cmd -> latchBenchmark.countDown());

                final float perfMt = recreatedContainer.executeTestingThread(() -> {

                    final long tStart = System.currentTimeMillis();
                    apiCommandsBenchmark.forEach(api::submitCommand);
                    latchBenchmark.await();
                    final long tDuration = System.currentTimeMillis() - tStart;

                    assertTrue(recreatedContainer.totalBalanceReport().isGlobalBalancesAllZero());

                    return apiCommandsBenchmark.size() / (float) tDuration / 1000.0f;
                });

                final float perfRatioPerc = perfMt / originalPerfMt * 100f;
                log.info("{}. restored throughput: {} MT/s ({}%)", iteration, String.format("%.3f", perfMt), String.format("%.1f", perfRatioPerc));
            }

            System.gc();
            Thread.sleep(200);
        }

    }


    private static final Consumer<? super Object> IGNORING_CONSUMER = x -> {
    };

}
