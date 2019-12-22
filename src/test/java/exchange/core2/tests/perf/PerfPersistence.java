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
import exchange.core2.tests.util.PersistenceTestsModule;
import exchange.core2.tests.util.TestConstants;
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
                3_000_000,
                1000,
                2000,
                10,
                TestConstants.CURRENCIES_FUTURES,
                1,
                ExchangeTestContainer.AllowedSymbolTypes.FUTURES_CONTRACT,
                1,
                1,
                2 * 1024,
                1024);
    }

    @Test
    public void testPersistenceExchange() throws Exception {
        PersistenceTestsModule.persistenceTestImpl(
                3_000_000,
                1000,
                2000,
                10,
                TestConstants.CURRENCIES_EXCHANGE,
                1,
                ExchangeTestContainer.AllowedSymbolTypes.CURRENCY_EXCHANGE_PAIR,
                1,
                1,
                2 * 1024,
                1024);
    }

    /**
     * This is serialization test for verifying "triple million" capability.
     * This test requires 10+ GiB free disk space, 16+ GiB of RAM and 12-threads CPU
     */
    @Test
    public void testPersistenceMultiSymbol() throws Exception {
        PersistenceTestsModule.persistenceTestImpl(
                5_000_000, //16.5
                1_000_000, // 10
                5_000_000, // 10
                25,
                TestConstants.ALL_CURRENCIES,
                100_000,
                ExchangeTestContainer.AllowedSymbolTypes.BOTH,
                4,
                4,
                64 * 1024,
                1546);
    }

}