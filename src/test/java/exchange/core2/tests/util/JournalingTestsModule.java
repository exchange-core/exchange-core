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

import exchange.core2.core.common.api.ApiPersistState;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.config.InitialStateConfiguration;
import exchange.core2.core.common.config.PerformanceConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.hamcrest.core.Is;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@Slf4j
public class JournalingTestsModule {


    public static void journalingTestImpl(final PerformanceConfiguration performanceConfiguration,
                                          final TestDataParameters testDataParameters,
                                          final int iterations) throws InterruptedException, ExecutionException, TimeoutException {

        for (int iteration = 0; iteration < iterations; iteration++) {

            //long t = System.currentTimeMillis();

            final ExchangeTestContainer.TestDataFutures testDataFutures = ExchangeTestContainer.prepareTestDataAsync(testDataParameters, iteration);

            final long stateId;
            final long originalFinalStateHash;

            final String exchangeId = ExchangeTestContainer.timeBasedExchangeId();

            final InitialStateConfiguration firstStartConfig = InitialStateConfiguration.cleanStartJournaling(exchangeId);

            try (final ExchangeTestContainer container = new ExchangeTestContainer(performanceConfiguration, firstStartConfig)) {

                container.loadSymbolsUsersAndPrefillOrders(testDataFutures);

                log.info("Creating snapshot...");
                stateId = System.currentTimeMillis() * 1000 + iteration;
                final ApiPersistState apiPersistState = ApiPersistState.builder().dumpId(stateId).build();
                try (ExecutionTime ignore = new ExecutionTime(t -> log.debug("Snapshot {} created in {}", stateId, t))) {
                    final CommandResultCode resultCode = container.getApi().submitCommandAsync(apiPersistState).get();
                    assertThat(resultCode, Is.is(CommandResultCode.SUCCESS));
                }

                log.info("Running commands on original state...");
                final TestOrdersGenerator.MultiSymbolGenResult genResult = testDataFutures.genResult.get();
                container.getApi().submitCommandsSync(genResult.getApiCommandsBenchmark().join());
                assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());

                originalFinalStateHash = container.requestStateHash();
                log.info("Original state checks completed");
            }

            // TODO Discover snapshots and journals with DiskSerializationProcessor
            final long snapshotBaseSeq = 0L;

            final InitialStateConfiguration fromSnapshotConfig = InitialStateConfiguration.lastKnownStateFromJournal(exchangeId, stateId, snapshotBaseSeq);

            log.debug("Creating new exchange from persisted state...");
            final long tLoad = System.currentTimeMillis();
            try (final ExchangeTestContainer recreatedContainer = new ExchangeTestContainer(performanceConfiguration, fromSnapshotConfig)) {

                // simple sync query in order to wait until core is started to respond
                recreatedContainer.totalBalanceReport();

                float loadTimeSec = (float) (System.currentTimeMillis() - tLoad) / 1000.0f;
                log.debug("Load+start+replay time: {}s", String.format("%.3f", loadTimeSec));

                final long restoredStateHash = recreatedContainer.requestStateHash();
                assertThat(restoredStateHash, is(originalFinalStateHash));

                assertTrue(recreatedContainer.totalBalanceReport().isGlobalBalancesAllZero());
                log.info("Restored snapshot+journal is valid");
            }

        }

    }
}
