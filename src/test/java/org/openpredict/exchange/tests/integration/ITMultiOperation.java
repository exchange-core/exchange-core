package org.openpredict.exchange.tests.integration;

import org.junit.Test;
import org.openpredict.exchange.tests.util.ExchangeTestContainer;
import org.openpredict.exchange.tests.util.ThroughputTestsModule;
import org.openpredict.exchange.tests.util.TestConstants;


public class ITMultiOperation {

    @Test(timeout = 60000L)
    public void shouldPerformMarginOperations() throws Exception {
        try (final ExchangeTestContainer container = new ExchangeTestContainer(2 * 1024, 1, 1, 1536, null)) {
            ThroughputTestsModule.throughputTestImpl(
                    container,
                    1_000_000,
                    1_000,
                    2_000,
                    2,
                    TestConstants.CURRENCIES_FUTURES,
                    1,
                    ExchangeTestContainer.AllowedSymbolTypes.FUTURES_CONTRACT);
        }
    }

    @Test(timeout = 60000L)
    public void shouldPerformExchangeOperations() throws Exception {
        try (final ExchangeTestContainer container = new ExchangeTestContainer(2 * 1024, 1, 1, 1536, null)) {
            ThroughputTestsModule.throughputTestImpl(
                    container,
                    1_000_000,
                    1_000,
                    2_000,
                    2,
                    TestConstants.CURRENCIES_EXCHANGE,
                    1,
                    ExchangeTestContainer.AllowedSymbolTypes.CURRENCY_EXCHANGE_PAIR);
        }
    }


    @Test(timeout = 60000L)
    public void shouldPerformSharded() throws Exception {
        try (final ExchangeTestContainer container = new ExchangeTestContainer(32 * 1024, 2, 2, 1536, null)) {
            ThroughputTestsModule.throughputTestImpl(
                    container,
                    1_000_000,
                    10_000,
                    50_000,
                    2,
                    TestConstants.ALL_CURRENCIES,
                    32,
                    ExchangeTestContainer.AllowedSymbolTypes.BOTH);
        }
    }


}
