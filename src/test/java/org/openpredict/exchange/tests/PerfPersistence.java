package org.openpredict.exchange.tests;

import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;
import org.junit.Test;
import org.openpredict.exchange.beans.CoreSymbolSpecification;
import org.openpredict.exchange.beans.api.ApiPersistState;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.core.ExchangeCore;
import org.openpredict.exchange.core.journalling.DiskSerializationProcessor;
import org.openpredict.exchange.tests.util.TestOrdersGenerator;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.openpredict.exchange.core.ExchangeCore.DisruptorWaitStrategy.BUSY_SPIN;
import static org.openpredict.exchange.core.Utils.ThreadAffityMode.THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE;

@Slf4j
public final class PerfPersistence extends IntegrationTestBase {


    @Test
    public void persistenceTest() throws Exception {
        final int bufferSize = 2 * 1024;
        final int msgsInGroupLimit = 1536;
        initExchange(bufferSize, 1, 1, msgsInGroupLimit);
        persistenceTestImpl(
                3_000_000,
                1000,
                1000,
                50,
                CURRENCIES_FUTURES,
                1,
                AllowedSymbolTypes.FUTURES_CONTRACT,
                1,
                1);
    }

    @Test
    public void persistenceMultiSymbol() throws Exception {
        final int bufferSize = 2 * 1024;
        final int msgsInGroupLimit = 1536;
        initExchange(bufferSize, 4, 4, msgsInGroupLimit);
        persistenceTestImpl(
                5_000_000, //16.5
                1_000_000, // 10
                1_000_000, // 10
                25,
                ALL_CURRENCIES,
                1_000,
                AllowedSymbolTypes.BOTH,
                4,
                4);
    }

    private void persistenceTestImpl(final int totalTransactionsNumber,
                                     final int targetOrderBookOrdersTotal,
                                     final int numUsers,
                                     final int iterations,
                                     final Set<Integer> currenciesAllowed,
                                     final int numSymbols,
                                     final AllowedSymbolTypes allowedSymbolTypes,
                                     final int matchingEngines,
                                     final int riskEngines) throws InterruptedException {

        try (AffinityLock cpuLock = AffinityLock.acquireCore()) {

            final List<CoreSymbolSpecification> coreSymbolSpecifications = generateAndAddSymbols(numSymbols, currenciesAllowed, allowedSymbolTypes);

            TestOrdersGenerator.MultiSymbolGenResult genResult = TestOrdersGenerator.generateMultipleSymbols(coreSymbolSpecifications,
                    totalTransactionsNumber,
                    numUsers,
                    targetOrderBookOrdersTotal);

            for (int j = 0; j < iterations; j++) {

                final int bufferSize = 2 * 1024;
                final int msgsInGroupLimit = 1536;

                //initExchange(bufferSize, matchingEngines, riskEngines, msgsInGroupLimit);


                log.info("Load symbols...");
                initBasicSymbols();
                coreSymbolSpecifications.forEach(super::addSymbol);
                log.info("Load users...");
                usersInit(numUsers, currenciesAllowed);

                log.info("Pre-fill...");
                final CountDownLatch latchFill = new CountDownLatch(genResult.getApiCommandsFill().size());
                consumer = cmd -> latchFill.countDown();
                genResult.getApiCommandsFill().forEach(api::submitCommand);
                latchFill.await();

                log.info("Benchmarking...");
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
//                coreSymbolSpecifications.forEach(
//                        symbol -> assertEquals(genResult.getGenResults().get(symbol.symbolId).getFinalOrderBookSnapshot(), requestCurrentOrderBook(symbol.symbolId)));

                final long tc = System.currentTimeMillis();
                final long stateId = tc;
                submitMultiCommandSync(ApiPersistState.builder().dumpId(stateId).build());

                float persistTimeSec = (float) (System.currentTimeMillis() - tc) / 1000.0f;
                log.debug("PERSISTING TIME: {}s", String.format("%.3f", persistTimeSec));

//                shutdownExchange();
//
//                final long tLoad = System.currentTimeMillis();
//                log.debug("Creating new exchange...");
//                exchangeCore = ExchangeCore.builder()
//                        .resultsConsumer(consumer)
//                        .serializationProcessor(new DiskSerializationProcessor())
//                        .ringBufferSize(bufferSize)
//                        .matchingEnginesNum(matchingEngines)
//                        .riskEnginesNum(riskEngines)
//                        .msgsInGroupLimit(msgsInGroupLimit)
//                        .threadAffityMode(THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE)
//                        .waitStrategy(BUSY_SPIN)
//                        .loadStateId(stateId) // Loading from persisted state
//                        .build();
//
//                float loadTimeSec = (float) (System.currentTimeMillis() - tLoad) / 1000.0f;
//                log.debug("LOAD TIME: {}s", String.format("%.3f", loadTimeSec));
//
//                exchangeCore.startup();
//                api = exchangeCore.getApi();
//
//                // validate order books have restored correctly:
//                log.debug("Validate restored snapshot...");
//                coreSymbolSpecifications.forEach(
//                        symbol -> assertEquals(genResult.getGenResults().get(symbol.symbolId).getFinalOrderBookSnapshot(), requestCurrentOrderBook(symbol.symbolId)));
//                // TODO validate accounts
//
//                log.debug("Validated");

                resetExchangeCore();

                System.gc();
                Thread.sleep(200);
            }
        }
    }
}