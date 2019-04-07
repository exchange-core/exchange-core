package org.openpredict.exchange.core.orderbook;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.tests.util.TestOrdersGenerator;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@Slf4j
public class OrderBookFastImplTest extends OrderBookBaseTest {

    @Override
    protected IOrderBook createNewOrderBook() {
        return new OrderBookFastImpl(IOrderBook.DEFAULT_HOT_WIDTH);
    }


    @Test
    public void multipleCommandsCompareTest() {

        long nextUpdateTime = 0;

        int tranNum = 100_000;
        int targetOrderBookOrders = 500;
        int numUsers = 500;

        IOrderBook orderBook = new OrderBookFastImpl(4096);
        //IOrderBook orderBook = new OrderBookNaiveImpl();
        IOrderBook orderBookRef = new OrderBookNaiveImpl();

        TestOrdersGenerator.GenResult genResult = TestOrdersGenerator.generateCommands(tranNum, targetOrderBookOrders, numUsers, 0, true);

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
            // TODO compare L2 marketdata


            if (System.currentTimeMillis() > nextUpdateTime) {
                log.debug("{}% done ({})", (i * 10000 / genResult.getCommands().size()) / 100f, i);
                nextUpdateTime = System.currentTimeMillis() + 3000;
            }

        }

    }
}