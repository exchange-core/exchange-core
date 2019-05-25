package org.openpredict.exchange.tests;

import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;
import org.junit.Test;
import org.openpredict.exchange.beans.CoreSymbolSpecification;
import org.openpredict.exchange.beans.api.ApiPersistState;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.tests.util.TestOrdersGenerator;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@Slf4j
public final class PerfPersistence extends IntegrationTestBase {


    @Test
    public void persistenceTest() throws Exception {
        initExchange(2 * 1024, 1, 1, 1536);
        persistenceTestImpl(
                3_000_000,
                1000,
                1000,
                50,
                CURRENCIES_FUTURES,
                1,
                AllowedSymbolTypes.FUTURES_CONTRACT);
    }

    @Test
    public void persistenceMultiSymbol() throws Exception {
        initExchange(64 * 1024, 4, 4, 2048);
        persistenceTestImpl(
                5_000_000, //12
                1_000_000, // 8
                1_000_000, // 8
                25,
                ALL_CURRENCIES,
                1_000,
                AllowedSymbolTypes.BOTH);
    }

    private void persistenceTestImpl(final int totalTransactionsNumber,
                                     final int targetOrderBookOrdersTotal,
                                     final int numUsers,
                                     final int iterations,
                                     final Set<Integer> currenciesAllowed,
                                     final int numSymbols,
                                     final AllowedSymbolTypes allowedSymbolTypes) throws InterruptedException {

        try (AffinityLock cpuLock = AffinityLock.acquireCore()) {

            final List<CoreSymbolSpecification> coreSymbolSpecifications = generateAndAddSymbols(numSymbols, currenciesAllowed, allowedSymbolTypes);

            TestOrdersGenerator.MultiSymbolGenResult genResult = TestOrdersGenerator.generateMultipleSymbols(coreSymbolSpecifications,
                    totalTransactionsNumber,
                    numUsers,
                    targetOrderBookOrdersTotal);

            for (int j = 0; j < iterations; j++) {

                initBasicSymbols();
                coreSymbolSpecifications.forEach(super::addSymbol);
                usersInit(numUsers, currenciesAllowed);

                final CountDownLatch latchFill = new CountDownLatch(genResult.getApiCommandsFill().size());
                consumer = cmd -> latchFill.countDown();
                genResult.getApiCommandsFill().forEach(api::submitCommand);
                latchFill.await();


                final CountDownLatch latchBenchmark = new CountDownLatch(genResult.getApiCommandsBenchmark().size());
                consumer = cmd -> latchBenchmark.countDown();
                long t = System.currentTimeMillis();
                genResult.getApiCommandsBenchmark().forEach(api::submitCommand);
                latchBenchmark.await();
                t = System.currentTimeMillis() - t;
                float perfMt = (float) genResult.getApiCommandsBenchmark().size() / (float) t / 1000.0f;
                log.info("{}. {} MT/s", j, String.format("%.3f", perfMt));

                // compare orderBook final state just to make sure all commands executed same way
                // TODO compare events, balances, portfolios
                coreSymbolSpecifications.forEach(
                        symbol -> assertEquals(genResult.getGenResults().get(symbol.symbolId).getFinalOrderBookSnapshot(), requestCurrentOrderBook(symbol.symbolId)));

                long tc = System.currentTimeMillis();
                final ApiPersistState dumpCommand = ApiPersistState.builder().dumpId(tc).build();
                submitCommandSync(dumpCommand, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

                float inSeconds = (float) (System.currentTimeMillis() - tc) / 1000.0f;
                log.debug("PERSISTING TIME: {}s", String.format("%.3f", inSeconds));

                resetExchangeCore();

                System.gc();
                Thread.sleep(2000);
            }
        }
    }
}