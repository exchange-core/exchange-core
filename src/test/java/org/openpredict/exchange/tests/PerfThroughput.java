package org.openpredict.exchange.tests;

import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;
import org.junit.Test;
import org.openpredict.exchange.beans.CoreSymbolSpecification;
import org.openpredict.exchange.beans.api.ApiCommand;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.tests.util.TestOrdersGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@Slf4j
public final class PerfThroughput extends IntegrationTestBase {

    // TODO shutdown disruptor if test fails
    @Test
    public void throughputTest() throws Exception {
        throughputTestImpl(
                3_000_000,
                1_000,
                1_000,
                50,
                CURRENCIES_FUTURES,
                1,
                AllowedSymbolTypes.FUTURES_CONTRACT);
    }

    @Test
    public void throughputTestTripleMillion() throws Exception {
        throughputTestImpl(
                8_000_000,
                1_075_000,
                1_000_000,
                20,
                CURRENCIES_FUTURES,
                1,
                AllowedSymbolTypes.FUTURES_CONTRACT);
    }

    @Test
    public void multiSymbol() throws Exception {

        throughputTestImpl(
                10_000_000,
                50_000,
                100_000,
                50,
                ALL_CURRENCIES,
                23,
                AllowedSymbolTypes.BOTH);

    }


    private void throughputTestImpl(final int totalTransactionsNumber,
                                    final int targetOrderBookOrders,
                                    final int numUsers,
                                    final int iterations,
                                    final Set<Integer> currenciesAllowed,
                                    final int numSymbols,
                                    final AllowedSymbolTypes allowedSymbolTypes) throws InterruptedException {

        try (AffinityLock cpuLock = AffinityLock.acquireCore()) {

            final List<CoreSymbolSpecification> coreSymbolSpecifications = generateAndAddSymbols(numSymbols, currenciesAllowed, allowedSymbolTypes);

            final Set<Integer> symbols = coreSymbolSpecifications.stream().map(spec -> spec.symbolId).collect(Collectors.toSet());
            final Map<Integer, TestOrdersGenerator.GenResult> genResults = TestOrdersGenerator.generateMultipleSymbols(
                    totalTransactionsNumber,
                    targetOrderBookOrders,
                    numUsers,
                    symbols);

            final List<OrderCommand> commands = TestOrdersGenerator.mergeCommands(genResults.values());

            final List<ApiCommand> apiCommands = TestOrdersGenerator.convertToApiCommand(commands);

            List<Float> perfResults = new ArrayList<>();
            for (int j = 0; j < iterations; j++) {

                initSymbol();
                coreSymbolSpecifications.forEach(super::addSymbol);
                usersInit(numUsers, currenciesAllowed);

                final CountDownLatch latch = new CountDownLatch(apiCommands.size());
                consumer = cmd -> latch.countDown();
                long t = System.currentTimeMillis();
                for (ApiCommand cmd : apiCommands) {
                    api.submitCommand(cmd);
                }
                latch.await();
                t = System.currentTimeMillis() - t;
                float perfMt = (float) apiCommands.size() / (float) t / 1000.0f;
                log.info("{}. {} MT/s", j, String.format("%.3f", perfMt));
                perfResults.add(perfMt);

                // compare orderBook final state just to make sure all commands executed same way
                // TODO compare events, balances, portfolios
                symbols.forEach(symbol -> assertEquals(genResults.get(symbol).getFinalOrderBookSnapshot(), requestCurrentOrderBook(symbol)));

                resetExchangeCore();

                System.gc();
                Thread.sleep(300);
            }

            float avg = (float) perfResults.stream().mapToDouble(x -> x).average().orElse(0);
            log.info("Average: {} MT/s", avg);
        }
    }
}