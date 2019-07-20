package org.openpredict.exchange.tests.perf;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.openpredict.exchange.tests.util.ExchangeTestContainer;
import org.openpredict.exchange.tests.util.PersistenceTestsModule;
import org.openpredict.exchange.tests.util.TestConstants;

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
                10_000_000, // 10
                25,
                TestConstants.ALL_CURRENCIES,
                1_000,
                ExchangeTestContainer.AllowedSymbolTypes.BOTH,
                4,
                4,
                64 * 1024,
                1546);
    }

}