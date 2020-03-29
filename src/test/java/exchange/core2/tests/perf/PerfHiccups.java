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

import exchange.core2.core.common.config.InitialStateConfiguration;
import exchange.core2.core.common.config.PerformanceConfiguration;
import exchange.core2.tests.util.ExchangeTestContainer;
import exchange.core2.tests.util.TestConstants;
import exchange.core2.tests.util.TestDataParameters;
import exchange.core2.tests.util.TestOrdersGeneratorConfig;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import static exchange.core2.tests.util.LatencyTestsModule.hiccupTestImpl;


@Slf4j
public final class PerfHiccups {


    @Test
    public void testHiccupMargin() {
        hiccupTestImpl(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(2 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.builder()
                        .totalTransactionsNumber(3_000_000)
                        .targetOrderBookOrdersTotal(1000)
                        .numAccounts(2000)
                        .currenciesAllowed(TestConstants.CURRENCIES_FUTURES)
                        .numSymbols(1)
                        .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.FUTURES_CONTRACT)
                        .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER)
                        .build(),
                InitialStateConfiguration.CLEAN_TEST,
                3);
    }


}