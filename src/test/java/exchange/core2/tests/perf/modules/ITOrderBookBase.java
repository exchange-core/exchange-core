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
package exchange.core2.tests.perf.modules;

import com.lmax.disruptor.EventTranslator;
import exchange.core2.tests.util.TestOrdersGenerator;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import exchange.core2.core.common.L2MarketData;
import exchange.core2.core.common.MatcherEventType;
import exchange.core2.core.common.MatcherTradeEvent;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.orderbook.IOrderBook;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static exchange.core2.core.common.OrderAction.ASK;
import static exchange.core2.core.common.OrderAction.BID;
import static exchange.core2.core.common.OrderType.IOC;

/**
 * TODO add tests where orders for same UID ignored during matching
 */
@Slf4j
public abstract class ITOrderBookBase {

    private IOrderBook orderBook;

    protected abstract IOrderBook createNewOrderBook();

    @After
    public void after() {

        L2MarketData snapshot = orderBook.getL2MarketDataSnapshot(10000);

        final long maxPrice = Arrays.stream(snapshot.askPrices).max().orElse(1);

        // match all asks
        long askSum = Arrays.stream(snapshot.askVolumes).sum();
        IOrderBook.processCommand(orderBook, OrderCommand.newOrder(IOC, 100000000000L, -1, maxPrice, maxPrice, askSum, BID));
//        log.debug("{}", dumpOrderBook(orderBook.getL2MarketDataSnapshot(100000)));

        // match all bids
        long bidSum = Arrays.stream(snapshot.bidVolumes).sum();

//        log.debug("Matching {} bids", bidSum);
        IOrderBook.processCommand(orderBook, OrderCommand.newOrder(IOC, 100000000001L, -2, 1, 0, bidSum, ASK));

//        log.debug("{}", dumpOrderBook(orderBook.getL2MarketDataSnapshot(100000)));

        assertThat(orderBook.getL2MarketDataSnapshot(10000).askVolumes.length, is(0));
        assertThat(orderBook.getL2MarketDataSnapshot(10000).bidVolumes.length, is(0));

    }


    @Test
    public void performanceTest1K() {

        int numOrders = 3_000_000;
        int targetOrderBookOrders = 1000;

        performanceTest(numOrders, targetOrderBookOrders);

    }

    @Test
    public void performanceTest1M() {

        int numOrders = 5_000_000;
        int targetOrderBookOrders = 1_000_000;

        performanceTest(numOrders, targetOrderBookOrders);

    }

    private void performanceTest(int numOrders, int targetOrderBookOrders) {

        try (AffinityLock cpuLock = AffinityLock.acquireLock()) {

            TestOrdersGenerator.GenResult genResult = TestOrdersGenerator.generateCommands(
                    numOrders,
                    targetOrderBookOrders,
                    1000,
                    TestOrdersGenerator.UID_PLAIN_MAPPER,
                    0,
                    false,
                    TestOrdersGenerator.createAsyncProgressLogger(numOrders));
            List<OrderCommand> orderCommands = genResult.getCommands();
            log.debug("orderCommands size: {}", orderCommands.size());

            List<Float> perfResults = new ArrayList<>();
            for (int j = 0; j < 32; j++) {
                orderBook = createNewOrderBook();

                long t = System.currentTimeMillis();
                OrderCommand workCmd = new OrderCommand();
                for (OrderCommand cmd : orderCommands) {
                    cmd.writeTo(workCmd);
                    workCmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
                    IOrderBook.processCommand(orderBook, workCmd);
                }
                t = System.currentTimeMillis() - t;

                // weak compare orderBook final state just to make sure all commands executed same way
                // TODO compare events
                orderBook.validateInternalState();
                assertThat(orderBook.hashCode(), is(genResult.getFinalOrderbookHash()));

                float perfMt = (float) orderCommands.size() / (float) t / 1000.0f;
                perfResults.add(perfMt);
                float averageMt = (float) perfResults.stream().mapToDouble(x -> x).average().orElse(0);
                log.info("{}. {} MT/s ({} ms) average: {} MT/s", j, perfMt, t, averageMt);
            }

            double avg = (float) perfResults.stream().mapToDouble(x -> x).average().orElse(0);
            log.info("Average: {} MT/s", avg);
        }
    }


    // ------------------------------- UTILITY METHODS --------------------------

    public void checkTrade(EventTranslator<MatcherTradeEvent> translatorLambda, long activeId, long matchedId, long price, long size) {

        MatcherTradeEvent event = new MatcherTradeEvent();
        translatorLambda.translateTo(event, 0);

        assertThat(event.eventType, is(MatcherEventType.TRADE));

        assertThat(event.activeOrderId, is(activeId));
        assertThat(event.matchedOrderId, is(matchedId));
        assertThat(event.price, is(price));
        assertThat(event.size, is(size));
        // TODO add more checks for MatcherTradeEvent
    }

    public void checkRejection(EventTranslator<MatcherTradeEvent> translatorLambda, long activeId, long size) {

        MatcherTradeEvent event = new MatcherTradeEvent();
        translatorLambda.translateTo(event, 0);

        assertThat(event.eventType, is(MatcherEventType.REJECTION));

        assertThat(event.activeOrderId, is(activeId));
        assertThat(event.size, is(size));
        // TODO add more checks for MatcherTradeEvent
    }

    @Ignore
    @Test
    public void testNano() throws InterruptedException {
        long baseTime = System.currentTimeMillis() * 1_000_000 - System.nanoTime();
        long baseNano = System.nanoTime();
        long baseMillis = System.currentTimeMillis();
        int iterations = 5_000_000;
        long[] data = new long[iterations];
        for (int i = 0; i < iterations; i++) {
            //Thread.sleep(0, 1000);
            data[i] = System.nanoTime() + baseTime;

            log.debug("{}  {}ms {}ns {}", data[i], System.currentTimeMillis() - baseMillis, System.nanoTime() - baseNano, Instant.now());

        }
        log.debug("time: {}ns", System.nanoTime() + baseTime);

//        for (int i = iterations - 1; i >= 0; i--) {
//            log.debug("{}", data[i]);
//        }

    }


    @Ignore
    @Test
    public void testNano2() throws InterruptedException {
        long baseTime = System.currentTimeMillis() * 1_000_000 - System.nanoTime();
        long baseNano = System.nanoTime();
        long baseMillis = System.currentTimeMillis();
        int iterations = 5_000_000;
        long[] data = new long[iterations];
        for (int i = 0; i < iterations; i++) {
            //Thread.sleep(0, 1000);
            data[i] = System.nanoTime() + baseTime;

            //log.debug("{}  {}ms {}ns {}", data[i], System.currentTimeMillis() -  baseMillis, System.nanoTime() - baseNano, Instant.now() );

        }
        log.debug("time: {}ns", System.nanoTime() + baseTime);

        for (int i = iterations - 1; i >= iterations - 1000; i--) {
            log.debug("{}", data[i]);
        }

    }


}