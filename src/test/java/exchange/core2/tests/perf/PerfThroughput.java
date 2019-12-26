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

import exchange.core2.tests.util.ExchangeTestContainer;
import exchange.core2.tests.util.TestConstants;
import exchange.core2.tests.util.ThroughputTestsModule;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public final class PerfThroughput {

    // TODO shutdown disruptor if test fails

    /**
     * This is throughput test for simplified conditions
     * - one symbol
     * - ~1K active users (2K currency accounts)
     * - 1K pending limit-orders (in one order book)
     * 6-threads CPU can run this test
     */
    @Test
    public void testThroughputMargin() throws Exception {
        ThroughputTestsModule.throughputTestImpl(
                () -> new ExchangeTestContainer(2 * 1024, 1, 1, 1536, null),
                3_000_000,
                1000,
                2000,
                50,
                TestConstants.CURRENCIES_FUTURES,
                1,
                ExchangeTestContainer.AllowedSymbolTypes.FUTURES_CONTRACT);
    }

    @Test
    public void testThroughputExchange() throws Exception {
        ThroughputTestsModule.throughputTestImpl(
                () -> new ExchangeTestContainer(2 * 1024, 1, 1, 1536, null),
                3_000_000,
                1000,
                2000,
                50,
                TestConstants.CURRENCIES_EXCHANGE,
                1,
                ExchangeTestContainer.AllowedSymbolTypes.CURRENCY_EXCHANGE_PAIR);
    }

    @Test
    public void testThroughputPeak() throws Exception {
        ThroughputTestsModule.throughputTestImpl(
                () -> new ExchangeTestContainer(64 * 1024, 4, 2, 2048, null),
                3_000_000,
                10_000,
                10_000,
                50,
                TestConstants.ALL_CURRENCIES,
                100,
                ExchangeTestContainer.AllowedSymbolTypes.BOTH);
    }

    /**
     * This is medium load throughput test for verifying "triple million" capability:
     * * - 1M active users (3M currency accounts)
     * * - 1M pending limit-orders
     * * - 100K symbols
     * * - 1M+ messages per second target throughput
     * 12-threads CPU and 32GiB RAM is required for running this test in 4+4 configuration.
     */
    @Test
    public void testThroughputMultiSymbolMedium() throws Exception {
        ThroughputTestsModule.throughputTestImpl(
                () -> new ExchangeTestContainer(64 * 1024, 4, 2, 2048, null),
                6_000_000,
                1_000_000,
                3_300_000,
                25,
                TestConstants.ALL_CURRENCIES,
                100_000,
                ExchangeTestContainer.AllowedSymbolTypes.BOTH);
    }


    /**
     * This is high load throughput test for verifying exchange core scalability:
     * - 10M active users (33M currency accounts)
     * - 30M pending limit-orders
     * - 1M+ messages per second throughput
     * - 200K symbols
     * - less than 1 millisecond 99.99% latency
     * 12-threads CPU and 32GiB RAM is required for running this test in 2+4 configuration.
     */
    @Test
    public void testThroughputMultiSymbolLarge() throws Exception {
        ThroughputTestsModule.throughputTestImpl(
                () -> new ExchangeTestContainer(64 * 1024, 4, 2, 2048, null),
                40_000_000,
                30_000_000,
                33_000_000,
                25,
                TestConstants.ALL_CURRENCIES,
                200_000,
                ExchangeTestContainer.AllowedSymbolTypes.BOTH);
    }
}