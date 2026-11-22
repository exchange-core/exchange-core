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
import org.junit.jupiter.api.Test;

import static exchange.core2.tests.util.LatencyTestsModule.individualLatencyTest;

/**
 * Submits one command at a time and reports per-command latency minima across repeated runs.
 */
public final class PerfLatencyCommands {

    /**
     * Per-command latency for a single margin symbol.
     * - 1K users with 2K currency accounts
     * - 1K limit orders targeted for prefill
     * - 3M benchmark commands per iteration, one command in flight at a time
     * - 1 RE / 1 ME
     */
    @Test
    public void testLatencyMargin() {
        individualLatencyTest(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(2 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.singlePairMarginBuilder().build(),
                InitialStateConfiguration.CLEAN_TEST);
    }


    /**
     * Per-command latency for a single spot symbol.
     * - 1K users with 2K currency accounts
     * - 1K limit orders targeted for prefill
     * - 3M benchmark commands per iteration, one command in flight at a time
     * - 1 RE / 1 ME
     */
    @Test
    public void testLatencyExchange() {
        individualLatencyTest(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(2 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.singlePairExchangeBuilder().build(),
                InitialStateConfiguration.CLEAN_TEST);
    }


    /**
     * Medium per-command latency test.
     * - About 1M generated users with 3.3M currency accounts
     * - 1M limit orders targeted for prefill across 10K symbols
     * - 3M benchmark commands per iteration, one command in flight at a time
     * - 2 RE / 4 ME
     */
    @Test
    public void testLatencyMultiSymbolMedium() {
        individualLatencyTest(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(64 * 1024)
                        .matchingEnginesNum(4)
                        .riskEnginesNum(2)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.mediumBuilder().build(),
                InitialStateConfiguration.CLEAN_TEST);
    }


    @Test
    public void testLatencyMultiSymbolLarge() {
        individualLatencyTest(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(64 * 1024)
                        .matchingEnginesNum(4)
                        .riskEnginesNum(2)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.largeBuilder().build(),
                InitialStateConfiguration.CLEAN_TEST);
    }

    @Test
    public void testLatencyMultiSymbolHuge() {
        individualLatencyTest(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(64 * 1024)
                        .matchingEnginesNum(4)
                        .riskEnginesNum(2)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.hugeBuilder().build(),
                InitialStateConfiguration.CLEAN_TEST);
    }
}
