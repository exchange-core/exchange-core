package org.openpredict.exchange.tests;

import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;
import org.junit.Test;
import org.openpredict.exchange.beans.api.ApiCommand;
import org.openpredict.exchange.tests.util.TestOrdersGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

@Slf4j
public final class PerfThroughput extends IntegrationTestBase {

    // TODO shutdown disruptor if test fails
    @Test
    public void throughputTest() throws Exception {
        throughputTestImpl(
                3_000_000,
                1_000,
                1_000);
    }

    @Test
    public void throughputTestTripleMillion() throws Exception {
        throughputTestImpl(
                8_000_000,
                1_075_000,
                1_000_000);
    }

    private void throughputTestImpl(final int numOrders, final int targetOrderBookOrders, final int numUsers) throws InterruptedException {

        try (AffinityLock cpuLock = AffinityLock.acquireCore()) {
            TestOrdersGenerator.GenResult genResult = TestOrdersGenerator.generateCommands(numOrders, targetOrderBookOrders, numUsers, SYMBOL, false);
            List<ApiCommand> apiCommands = TestOrdersGenerator.convertToApiCommand(genResult.getCommands());

            List<Float> perfResults = new ArrayList<>();
            for (int j = 0; j < 100; j++) {

                initSymbol();
                usersInit(numUsers);

                final CountDownLatch latch = new CountDownLatch(apiCommands.size());
                consumer = cmd -> latch.countDown();
                long t = System.currentTimeMillis();
                for (ApiCommand cmd : apiCommands) {
                    api.submitCommand(cmd);
                }
                latch.await();
                t = System.currentTimeMillis() - t;
                float perfMt = (float) apiCommands.size() / (float) t / 1000.0f;
                log.info("{}. {} MT/s", j, perfMt);
                perfResults.add(perfMt);

                // compare orderBook final state just to make sure all commands executed same way
                // TODO compare events, balances, portfolios
                assertEquals(genResult.getFinalOrderBookSnapshot(), requestCurrentOrderBook());

                resetExchangeCore();

                System.gc();
                Thread.sleep(300);
            }

            float avg = (float) perfResults.stream().mapToDouble(x -> x).average().orElse(0);
            log.info("Average: {} MT/s", avg);
        }
    }

}