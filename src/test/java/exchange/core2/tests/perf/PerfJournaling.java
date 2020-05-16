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
public final class PerfJournaling {


    @Test
    public void testJournalingMargin() throws Exception {
        JournalingTestsModule.journalingTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder()
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .build(),
                TestDataParameters.singlePairMarginBuilder()
                        .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER_PLUS_QUARTER)
                        .build(),
                10);
    }

    @Test
    public void testJournalingExchange() throws Exception {
        JournalingTestsModule.journalingTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder()
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .build(),
                TestDataParameters.singlePairExchangeBuilder()
                        .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER_PLUS_QUARTER)
                        .build(),
                10);
    }

    @Test
    public void testJournalingMultiSymbolSmall() throws Exception {
        JournalingTestsModule.journalingTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder()
                        .matchingEnginesNum(2)
                        .riskEnginesNum(2)
                        .build(),
                TestDataParameters.builder()
                        .totalTransactionsNumber(3_000_000)
                        .targetOrderBookOrdersTotal(50_000)
                        .numAccounts(100_000)
                        .currenciesAllowed(TestConstants.ALL_CURRENCIES)
                        .numSymbols(1_000)
                        .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.BOTH)
                        .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER_PLUS_QUARTER)
                        .build(),
                25);
    }

    @Test
    public void testJournalingMultiSymbolMedium() throws Exception {
        JournalingTestsModule.journalingTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder()
                        .matchingEnginesNum(4)
                        .riskEnginesNum(2)
                        .build(),
                TestDataParameters.mediumBuilder()
                        .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER_PLUS_QUARTER)
                        .build(),
                25);
    }

    @Test
    public void testJournalingMultiSymbolLarge() throws Exception {
        JournalingTestsModule.journalingTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder()
                        .matchingEnginesNum(4)
                        .riskEnginesNum(4)
                        .build(),
                TestDataParameters.largeBuilder()
                        .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER_PLUS_QUARTER)
                        .build(),
                25);
    }


    @Test
    public void testJournalingMultiSymbolHuge() throws Exception {
        JournalingTestsModule.journalingTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder()
                        .ringBufferSize(128 * 1024)
                        .matchingEnginesNum(4)
                        .riskEnginesNum(4)
                        .msgsInGroupLimit(1024)
                        .build(),
                TestDataParameters.hugeBuilder()
                        .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER_PLUS_QUARTER)
                        .build(),
                10);
    }

}