package org.openpredict.exchange.tests.perf;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.openpredict.exchange.tests.util.ThroughputTestsModule;
import org.openpredict.exchange.tests.util.ExchangeTestContainer;

import static org.openpredict.exchange.tests.util.TestConstants.*;

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
                    CURRENCIES_FUTURES,
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
                    CURRENCIES_EXCHANGE,
                    1,
                    ExchangeTestContainer.AllowedSymbolTypes.CURRENCY_EXCHANGE_PAIR);
        }
    }

    /**
     * This is high load throughput test for verifying "triple million" capability:
     * - 10M currency accounts (~3M active users)
     * - 1M pending limit-orders (in 1K order books)
     * - at least 1M messages per second throughput
     * 12-threads CPU is required for running this test in 4+4 configuration.
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
                    ALL_CURRENCIES,
                    1_000,
                    ExchangeTestContainer.AllowedSymbolTypes.BOTH);
        }
    }

}