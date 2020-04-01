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
import exchange.core2.tests.util.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public final class PerfPersistence {


    /**
     * This is serialization test for simplified conditions
     * - one symbol
     * - ~1K active users (2K currency accounts)
     * - 1K pending limit-orders (in one order book)
     * 6-threads CPU can run this test
     */
    @Test
    public void testPersistenceMargin() throws Exception {
        PersistenceTestsModule.persistenceTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder()
                        .ringBufferSize(2 * 1024)
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
                        .ringBufferSize(2 * 1024)
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
     * This is serialization test for verifying "triple million" capability.
     * This test requires 10+ GiB free disk space, 16+ GiB of RAM and 12-threads CPU
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