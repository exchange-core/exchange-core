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
        try (final ExchangeTestContainer container = new ExchangeTestContainer(2 * 1024, 1, 1, 1536, null)) {
            ThroughputTestsModule.throughputTestImpl(
                    container,
                    3_000_000,
                    1000,
                    2000,
                    50,
                    TestConstants.CURRENCIES_FUTURES,
                    1,
                    ExchangeTestContainer.AllowedSymbolTypes.FUTURES_CONTRACT);
        }
    }

    @Test
    public void testThroughputExchange() throws Exception {
        try (final ExchangeTestContainer container = new ExchangeTestContainer(2 * 1024, 1, 1, 1536, null)) {
            ThroughputTestsModule.throughputTestImpl(
                    container,
                    3_000_000,
                    1000,
                    2000,
                    50,
                    TestConstants.CURRENCIES_EXCHANGE,
                    1,
                    ExchangeTestContainer.AllowedSymbolTypes.CURRENCY_EXCHANGE_PAIR);
        }
    }

    /**
     * This is high load throughput test for verifying "triple million" capability:
     * - 10M currency accounts (~3M active users)
     * - 1M pending limit-orders (in 1K order books)
     * - 1K symbols
     * - at least 1M messages per second throughput
     * 12-threads CPU and 32GiB RAM is required for running this test in 4+4 configuration.
     */
    @Test
    public void testThroughputMultiSymbol() throws Exception {
        try (final ExchangeTestContainer container = new ExchangeTestContainer(64 * 1024, 4, 4, 2048, null)) {
            ThroughputTestsModule.throughputTestImpl(
                    container,
                    5_000_000,
                    1_000_000,
                    10_000_000,
                    25,
                    TestConstants.ALL_CURRENCIES,
                    1_000,
                    ExchangeTestContainer.AllowedSymbolTypes.BOTH);
        }
    }

}