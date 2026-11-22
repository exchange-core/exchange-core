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

import exchange.core2.core.common.config.PerformanceConfiguration;
import exchange.core2.tests.util.ExchangeTestContainer;
import exchange.core2.tests.util.PersistenceTestsModule;
import exchange.core2.tests.util.TestDataParameters;
import exchange.core2.tests.util.TestOrdersGeneratorConfig;
import org.junit.jupiter.api.Test;

public final class PerfPersistence {


    /**
     * Single-symbol margin snapshot test.
     * - 1K users with 2K currency accounts
     * - 1K limit orders targeted for prefill
     * - 3M benchmark commands per pass, run before and after restore in each of 10 iterations
     * - 1 RE / 1 ME
     * Checks the restored state hash and balances.
     */
    @Test
    public void testPersistenceMargin() throws Exception {
        PersistenceTestsModule.persistenceTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder()
                        .ringBufferSize(32 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .msgsInGroupLimit(512)
                        .build(),
                TestDataParameters.singlePairMarginBuilder()
                        .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER_PLUS_QUARTER)
                        .build(),
                10);
    }

    @Test
    public void testPersistenceExchange() throws Exception {
        PersistenceTestsModule.persistenceTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder()
                        .ringBufferSize(32 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .msgsInGroupLimit(512)
                        .build(),
                TestDataParameters.singlePairExchangeBuilder()
                        .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER_PLUS_QUARTER)
                        .build(),
                10);
    }

    /**
     * Medium snapshot test.
     * - About 1M generated users with 3.3M currency accounts
     * - 1M limit orders targeted for prefill across 10K symbols
     * - 3M benchmark commands per pass, run before and after restore in each of 25 iterations
     * - 2 RE / 4 ME
     * Checks the restored state hash and balances. Each iteration writes a snapshot to disk.
     */
    @Test
    public void testPersistenceMultiSymbolMedium() throws Exception {
        PersistenceTestsModule.persistenceTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder()
                        .ringBufferSize(32 * 1024)
                        .matchingEnginesNum(4)
                        .riskEnginesNum(2)
                        .msgsInGroupLimit(1024)
                        .build(),
                TestDataParameters.mediumBuilder()
                        .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.BOTH)
                        .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER_PLUS_QUARTER)
                        .build(),
                25);
    }

    @Test
    public void testPersistenceMultiSymbolLarge() throws Exception {
        PersistenceTestsModule.persistenceTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder()
                        .ringBufferSize(32 * 1024)
                        .matchingEnginesNum(4)
                        .riskEnginesNum(4)
                        .msgsInGroupLimit(1024)
                        .build(),
                TestDataParameters.largeBuilder()
                        .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER_PLUS_QUARTER)
                        .build(),
                25);
    }

    @Test
    public void testPersistenceMultiSymbolHuge() throws Exception {
        PersistenceTestsModule.persistenceTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder()
                        .ringBufferSize(32 * 1024)
                        .matchingEnginesNum(4)
                        .riskEnginesNum(4)
                        .msgsInGroupLimit(1024)
                        .build(),
                TestDataParameters.hugeBuilder()
                        .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER_PLUS_QUARTER)
                        .build(),
                25);
    }


}
