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
import exchange.core2.tests.util.ExchangeTestContainer;
import exchange.core2.tests.util.TestDataParameters;
import exchange.core2.tests.util.ThroughputTestsModule;
import org.junit.jupiter.api.Test;

/**
 * Measures command throughput with disk journaling enabled.
 */
public final class PerfThroughputJournaling {

    // TODO shutdown disruptor if test fails

    /**
     * Single-symbol margin throughput test with disk journaling.
     * - 1K users with 2K currency accounts
     * - 1K limit orders targeted for prefill
     * - 3M benchmark commands per iteration, 50 iterations
     * - 1 RE / 1 ME
     */
    @Test
    public void testThroughputMargin() {
        ThroughputTestsModule.throughputTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder()
                        .ringBufferSize(32 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .msgsInGroupLimit(1536)
                        .build(),
                TestDataParameters.singlePairMarginBuilder().build(),
                InitialStateConfiguration.cleanStartJournaling(ExchangeTestContainer.timeBasedExchangeId()),
                SerializationConfiguration.DISK_JOURNALING,
                50);
    }

    @Test
    public void testThroughputExchange() {
        ThroughputTestsModule.throughputTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder()
                        .ringBufferSize(32 * 1024)
                        .matchingEnginesNum(1)
                        .riskEnginesNum(1)
                        .msgsInGroupLimit(1536)
                        .build(),
                TestDataParameters.singlePairExchangeBuilder().build(),
                InitialStateConfiguration.cleanStartJournaling(ExchangeTestContainer.timeBasedExchangeId()),
                SerializationConfiguration.DISK_JOURNALING,
                50);
    }

    /**
     * Medium throughput test with disk journaling.
     * - About 1M generated users with 3.3M currency accounts
     * - 1M limit orders targeted for prefill across 10K symbols
     * - 3M benchmark commands per iteration, 25 iterations
     * - 1M+ commands/s reference throughput
     * - 2 RE / 4 ME
     * The Medium reference run with journaling disabled used 12 hardware threads and a 24 GiB JVM heap.
     */
    @Test
    public void testThroughputMultiSymbolMedium() {
        ThroughputTestsModule.throughputTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder().build(),
                TestDataParameters.mediumBuilder().build(),
                InitialStateConfiguration.cleanStartJournaling(ExchangeTestContainer.timeBasedExchangeId()),
                SerializationConfiguration.DISK_JOURNALING,
                25);
    }

    /**
     * Large throughput test with disk journaling.
     * - About 3M generated users with 10M currency accounts
     * - 3M limit orders targeted for prefill across 50K symbols
     * - 3M benchmark commands per iteration, 25 iterations
     * - 1M+ commands/s reference throughput
     * - 2 RE / 4 ME
     */
    @Test
    public void testThroughputMultiSymbolLarge() {
        ThroughputTestsModule.throughputTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder().build(),
                TestDataParameters.largeBuilder().build(),
                InitialStateConfiguration.cleanStartJournaling(ExchangeTestContainer.timeBasedExchangeId()),
                SerializationConfiguration.DISK_JOURNALING,
                25);
    }

    /**
     * Huge throughput test with disk journaling.
     * - About 10M generated users with 33M currency accounts
     * - 30M limit orders targeted for prefill across 100K symbols
     * - 10M benchmark commands per iteration, 25 iterations
     * - 1M+ commands/s reference throughput
     * - 2 RE / 4 ME
     */
    @Test
    public void testThroughputMultiSymbolHuge() {
        ThroughputTestsModule.throughputTestImpl(
                PerformanceConfiguration.throughputPerformanceBuilder().build(),
                TestDataParameters.hugeBuilder().build(),
                InitialStateConfiguration.cleanStartJournaling(ExchangeTestContainer.timeBasedExchangeId()),
                SerializationConfiguration.DISK_JOURNALING,
                25);
    }

}
