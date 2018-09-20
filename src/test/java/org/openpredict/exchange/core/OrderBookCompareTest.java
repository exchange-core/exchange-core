package org.openpredict.exchange.core;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.tests.util.TestOrdersGenerator;

import java.util.ArrayList;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
@Slf4j
public class OrderBookCompareTest {

    @Test
    public void multipleCommandsTest() {

        TestOrdersGenerator generator = new TestOrdersGenerator();

        long nextUpdateTime = 0;

        int tranNum = 100_000;
        int targetOrderBookOrders = 10_000;
        int numUsers = 10_000;

        IOrderBook orderBook = new OrderBookFast(4096);
        //IOrderBook orderBook = new OrderBookSlow();
        IOrderBook orderBookRef = new OrderBookSlow();

        ArrayList<Long> uids = new ArrayList<>();
        for (long i = 1; i <= numUsers; i++) {
            uids.add(i);
        }

        TestOrdersGenerator.GenResult genResult = generator.generateCommands(tranNum, targetOrderBookOrders, uids);

        long i = 0;
        for (OrderCommand cmd : genResult.getCommands()) {
            i++;
            cmd.orderId += 100;

            //log.debug("{}. {}", i, cmd);

            cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
            orderBook.processCommand(cmd);

            cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
            orderBookRef.processCommand(cmd);

            assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));

//            if (!orderBook.equals(orderBookRef)) {
//
//                if (!orderBook.getAllAskBuckets().equals(orderBookRef.getAllAskBuckets())) {
//                    log.warn("ASK FAST: {}", orderBook.getAllAskBuckets());
//                    log.warn("ASK REF : {}", orderBookRef.getAllAskBuckets());
//                } else {
//                    log.info("ASK ok");
//                }
//
//                if (!orderBook.getAllBidBuckets().equals(orderBookRef.getAllBidBuckets())) {
//                    log.warn("BID FAST: {}", orderBook.getAllBidBuckets().stream().map(x -> x.getPrice() + " " + x.getTotalVolume()).toArray());
//                    log.warn("BID REF : {}", orderBookRef.getAllBidBuckets().stream().map(x -> x.getPrice() + " " + x.getTotalVolume()).toArray());
//                } else {
//                    log.info("BID ok");
//                }
//
//            }

//            assertEquals(orderBook.hashCode(), orderBookRef.hashCode());
            assertEquals(orderBook, orderBookRef);

            // TODO compare events!


            if (System.currentTimeMillis() > nextUpdateTime) {
                log.debug("{}% done ({})", (i * 10000 / genResult.getCommands().size()) / 100f, i);
                nextUpdateTime = System.currentTimeMillis() + 3000;
            }

        }

    }

}
