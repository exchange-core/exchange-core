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
import exchange.core2.core.common.config.SerializationConfiguration;
import exchange.core2.tests.util.TestDataParameters;
import org.junit.jupiter.api.Test;

import static exchange.core2.tests.util.LatencyTestsModule.latencyTestImpl;

/**
 * Samples latency from scheduled command submission to the result callback across increasing offered rates.
 */
public final class PerfLatency {

    /**
     * Single-symbol margin latency test.
     * - 1K users with 2K currency accounts
     * - 1K limit orders targeted for prefill
     * - 3M benchmark commands per offered rate, 16 warmup cycles
     * - 1 RE / 1 ME
     */
    @Test
    public void testLatencyMargin() {
        latencyTestImpl(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(2 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.singlePairMarginBuilder().build(),
                InitialStateConfiguration.CLEAN_TEST,
                SerializationConfiguration.DEFAULT,
                16);
    }

    /**
     * Single-symbol spot latency test.
     * - 1K users with 2K currency accounts
     * - 1K limit orders targeted for prefill
     * - 3M benchmark commands per offered rate, 16 warmup cycles
     * - 1 RE / 1 ME
     */
    @Test
    public void testLatencyExchange() {
        latencyTestImpl(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(2 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.singlePairExchangeBuilder().build(),
                InitialStateConfiguration.CLEAN_TEST,
                SerializationConfiguration.DEFAULT,
                16);
    }

    /**
     * Medium latency test.
     * - About 1M generated users with 3.3M currency accounts
     * - 1M limit orders targeted for prefill across 10K symbols
     * - 3M benchmark commands per offered rate, 8 warmup cycles
     * - Reference load of 1M commands/s with a p99.99 goal below 1 ms
     * - 2 RE / 4 ME
     * A reference run used 12 hardware threads and a 24 GiB JVM heap.
     */
    @Test
    public void testLatencyMultiSymbolMedium() {
        latencyTestImpl(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(32 * 1024)
                        .matchingEnginesNum(4)
                        .riskEnginesNum(2)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.mediumBuilder().build(),
                InitialStateConfiguration.CLEAN_TEST,
                SerializationConfiguration.DEFAULT,
                8);
    }

    /**
     * Large latency test.
     * - About 3M generated users with 10M currency accounts
     * - 3M limit orders targeted for prefill across 50K symbols
     * - 3M benchmark commands per offered rate, 4 warmup cycles
     * - Reference load of 1M commands/s with a p99.99 goal below 1 ms
     * - 2 RE / 4 ME
     */
    @Test
    public void testLatencyMultiSymbolLarge() {
        latencyTestImpl(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(32 * 1024)
                        .matchingEnginesNum(4)
                        .riskEnginesNum(2)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.largeBuilder().build(),
                InitialStateConfiguration.CLEAN_TEST,
                SerializationConfiguration.DEFAULT,
                4);
    }

    /**
     * Huge latency test.
     * - About 10M generated users with 33M currency accounts
     * - 30M limit orders targeted for prefill across 100K symbols
     * - 10M benchmark commands per offered rate, 2 warmup cycles
     * - Reference load of 1M commands/s
     * - 2 RE / 4 ME
     */
    @Test
    public void testLatencyMultiSymbolHuge() {
        latencyTestImpl(
                PerformanceConfiguration.latencyPerformanceBuilder()
                        .ringBufferSize(64 * 1024)
                        .matchingEnginesNum(4)
                        .riskEnginesNum(2)
                        .msgsInGroupLimit(256)
                        .build(),
                TestDataParameters.hugeBuilder().build(),
                InitialStateConfiguration.CLEAN_TEST,
                SerializationConfiguration.DEFAULT,
                2);
    }

}
