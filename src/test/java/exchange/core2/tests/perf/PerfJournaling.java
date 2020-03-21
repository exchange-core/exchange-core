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
import exchange.core2.tests.util.JournalingTestsModule;
import exchange.core2.tests.util.TestConstants;
import exchange.core2.tests.util.TestDataParameters;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public final class PerfJournaling {


    @Test
    public void testJournalingMargin() throws Exception {
        JournalingTestsModule.journalingTestImpl(
                initStateCfg -> new ExchangeTestContainer(
                        PerformanceConfiguration.throughputPerformanceBuilder()
                                .ringBufferSize(64 * 1024)
                                .matchingEnginesNum(1)
                                .riskEnginesNum(1)
                                .msgsInGroupLimit(512)
                                .build(),
                        initStateCfg),
                TestDataParameters.builder()
                        .totalTransactionsNumber(3_000_000)
                        .targetOrderBookOrdersTotal(1000)
                        .numAccounts(2000)
                        .currenciesAllowed(TestConstants.CURRENCIES_FUTURES)
                        .numSymbols(1)
                        .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.FUTURES_CONTRACT)
                        .build(),
                10);
    }

    @Test
    public void testJournalingMultiSymbolSmall() throws Exception {
        JournalingTestsModule.journalingTestImpl(
                initStateCfg -> new ExchangeTestContainer(
                        PerformanceConfiguration.throughputPerformanceBuilder()
                                .ringBufferSize(64 * 1024)
                                .matchingEnginesNum(2)
                                .riskEnginesNum(2)
                                .msgsInGroupLimit(1024)
                                .build(),
                        initStateCfg),
                TestDataParameters.builder()
                        .totalTransactionsNumber(3_000_000)
                        .targetOrderBookOrdersTotal(50_000)
                        .numAccounts(100_000)
                        .currenciesAllowed(TestConstants.ALL_CURRENCIES)
                        .numSymbols(1_000)
                        .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.BOTH)
                        .build(),
                25);
    }

    @Test
    public void testJournalingMultiSymbolMedium() throws Exception {
        JournalingTestsModule.journalingTestImpl(
                initStateCfg -> new ExchangeTestContainer(
                        PerformanceConfiguration.throughputPerformanceBuilder()
                                .ringBufferSize(32 * 1024)
                                .matchingEnginesNum(4)
                                .riskEnginesNum(2)
                                .msgsInGroupLimit(1024)
                                .build(),
                        initStateCfg),
                TestDataParameters.builder()
                        .totalTransactionsNumber(7_500_000)
                        .targetOrderBookOrdersTotal(1_000_000)
                        .numAccounts(3_300_000)
                        .currenciesAllowed(TestConstants.ALL_CURRENCIES)
                        .numSymbols(10_000)
                        .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.BOTH)
                        .build(),
                25);
    }

    @Test
    public void testJournalingMultiSymbolLarge() throws Exception {
        JournalingTestsModule.journalingTestImpl(
                initStateCfg -> new ExchangeTestContainer(
                        PerformanceConfiguration.throughputPerformanceBuilder()
                                .ringBufferSize(64 * 1024)
                                .matchingEnginesNum(4)
                                .riskEnginesNum(4)
                                .msgsInGroupLimit(1024)
                                .build(),
                        initStateCfg),
                TestDataParameters.builder()
                        .totalTransactionsNumber(10_000_000)
                        .targetOrderBookOrdersTotal(4_000_000)
                        .numAccounts(10_000_000)
                        .currenciesAllowed(TestConstants.ALL_CURRENCIES)
                        .numSymbols(100_000)
                        .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.BOTH)
                        .build(),
                25);
    }


    @Test
    public void testJournalingMultiSymbolHuge() throws Exception {
        JournalingTestsModule.journalingTestImpl(
                initStateCfg -> new ExchangeTestContainer(
                        PerformanceConfiguration.throughputPerformanceBuilder()
                                .ringBufferSize(64 * 1024)
                                .matchingEnginesNum(4)
                                .riskEnginesNum(4)
                                .msgsInGroupLimit(1024)
                                .build(),
                        initStateCfg),
                TestDataParameters.builder()
                        .totalTransactionsNumber(30_000_000)
                        .targetOrderBookOrdersTotal(20_000_000)
                        .numAccounts(20_000_000)
                        .currenciesAllowed(TestConstants.ALL_CURRENCIES)
                        .numSymbols(200_000)
                        .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.BOTH)
                        .build(),
                200_000);
    }

}