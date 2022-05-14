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
import exchange.core2.tests.util.TestDataParameters;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import static exchange.core2.tests.util.LatencyTestsModule.individualLatencyTest;

@Slf4j
public final class PerfLatencyCommandsAvalanche {


    /**
     * Avalanche IoC orders test to verify matching performance for big-size taker orders.
     */
    @Test
    public void testLatencyMarginAvalancheIoc() {
        individualLatencyTest(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(2 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.singlePairMarginBuilder()
                        .avalancheIOC(true)
                        .build(),
                InitialStateConfiguration.CLEAN_TEST);
    }


    /**
     * Avalanche IoC orders test to verify matching performance for big-size taker orders.
     */
    @Test
    public void testLatencyExchangeAvalancheIoc() {
        individualLatencyTest(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(2 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.singlePairExchangeBuilder()
                        .avalancheIOC(true)
                        .build(),
                InitialStateConfiguration.CLEAN_TEST);
    }


    /**
     * Avalanche IoC orders test to verify matching performance for big-size taker orders. </br>
     * Why 4R1M configuration:</br>
     * Less matching engines provides better individual command latency - due to less interference. </br>
     * More risk engines provide better individual command latency because of parallel processing R2 stage (0.5 + 0.5/N) </br>
     */
    @Test
    public void testLatencyMultiSymbolMediumAvalancheIOC() {
        individualLatencyTest(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(64 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(4)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.mediumBuilder()
                        .numSymbols(2_000)
                        .avalancheIOC(true)
                        .build(),
                InitialStateConfiguration.CLEAN_TEST);
    }


    @Test
    public void testLatencyMultiSymbolLargeAvalancheIOC() {
        individualLatencyTest(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(64 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(4)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.largeBuilder()
                        .numSymbols(5_000)
                        .avalancheIOC(true)
                        .build(),
                InitialStateConfiguration.CLEAN_TEST);
    }

}