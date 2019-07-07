package org.openpredict.exchange.core.orderbook;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.openpredict.exchange.beans.L2MarketData;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.tests.util.TestOrdersGenerator;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.openpredict.exchange.beans.OrderAction.ASK;
import static org.openpredict.exchange.beans.OrderAction.BID;
import static org.openpredict.exchange.beans.OrderType.GTC;
import static org.openpredict.exchange.beans.cmd.CommandResultCode.SUCCESS;
import static org.openpredict.exchange.tests.util.TestConstants.SYMBOLSPEC_EUR_USD;

@Slf4j
public class OrderBookFastImplTest extends OrderBookBaseTest {

    private static int HOT_PRICES_RANGE = 1024;

    @Override
    protected IOrderBook createNewOrderBook() {

        return new OrderBookFastImpl(HOT_PRICES_RANGE, SYMBOLSPEC_EUR_USD);
    }


    @Test
    public void multipleCommandsCompareTest() {

        long nextUpdateTime = 0;

        int tranNum = 100_000;
        int targetOrderBookOrders = 500;
        int numUsers = 500;

        IOrderBook orderBook = new OrderBookFastImpl(4096, SYMBOLSPEC_EUR_USD);
        //IOrderBook orderBook = new OrderBookNaiveImpl();
        IOrderBook orderBookRef = new OrderBookNaiveImpl(SYMBOLSPEC_EUR_USD);

        TestOrdersGenerator.GenResult genResult = TestOrdersGenerator.generateCommands(tranNum, targetOrderBookOrders, numUsers, 0, true);

        long i = 0;
        for (OrderCommand cmd : genResult.getCommands()) {
            i++;
            cmd.orderId += 100;

            //log.debug("{}. {}", i, cmd);

            cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
            IOrderBook.processCommand(orderBook, cmd);

            cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
            CommandResultCode commandResultCode = IOrderBook.processCommand(orderBookRef, cmd);

            assertThat(commandResultCode, is(SUCCESS));

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


    @Test
    public void sequentialAsksTest() {

//        int hotPricesRange = 1024;
//        orderBook = new OrderBookFastImpl(hotPricesRange);
        //orderBook = new OrderBookNaiveImpl();


        // empty order book
        clearOrderBook();
        orderBook.validateInternalState();

        // ask prices start from here, overlap with far ask area
        final int topPrice = INITIAL_PRICE + HOT_PRICES_RANGE / 2 + 10;
        // ask prices stop from here, overlap with far bid area
        final int bottomPrice = INITIAL_PRICE - HOT_PRICES_RANGE / 2 - 14;

        int orderId = 100;

        // collecting expected limit order volumes for each price
        Map<Long, Long> results = new HashMap<>();

        // placing limit bid orders
        for (long price = bottomPrice; price < INITIAL_PRICE; price++) {
            OrderCommand cmd = OrderCommand.newOrder(GTC, orderId++, UID_1, price, price * 10, 1, BID);
//            log.debug("BID {}", price);
            processAndValidate(cmd, SUCCESS);
            results.put(price, -1L);
        }


        for (long price = topPrice; price >= bottomPrice; price--) {
            long size = price * price;
            OrderCommand cmd = OrderCommand.newOrder(GTC, orderId++, UID_2, price, 0, size, ASK);
//            log.debug("ASK {}", price);
            processAndValidate(cmd, SUCCESS);
            results.compute(price, (p, v) -> v == null ? size : v + size);

            //L2MarketData snapshot = orderBook.getL2MarketDataSnapshot(100000);
            //log.debug("A:{} B:{}", snapshot.askSize, snapshot.bidSize);
        }

        // collecting full order book
        L2MarketData snapshot = orderBook.getL2MarketDataSnapshot(-1);

        // check the number of records, should match to expected results
        assertThat(snapshot.askSize, is(results.size()));

        // verify expected size for each price
        for (int i = 0; i < snapshot.askSize; i++) {
            long price = snapshot.askPrices[i];
            Long expectedSize = results.get(price);
            assertThat(expectedSize, notNullValue());
//            if (snapshot.askVolumes[i] != expectedSize) {
//                log.error("volume mismatch for price {} : diff={}", price, snapshot.askVolumes[i] - expectedSize);
//            }
            assertThat("volume mismatch for price " + price, snapshot.askVolumes[i], is(expectedSize));
        }

        // obviously no bid records expected
        assertThat(snapshot.bidSize, is(0));
    }


    @Test
    public void sequentialBidsTest() {

        // empty order book
        clearOrderBook();
        orderBook.validateInternalState();

        // bid prices starts from here, overlap with far bid area
        final int bottomPrice = INITIAL_PRICE - HOT_PRICES_RANGE / 2 - 4;
        // bid prices stop here, overlap with far ask area
        final int topPrice = INITIAL_PRICE + HOT_PRICES_RANGE / 2 + 21;

        int orderId = 100;

        // collecting expected limit order volumes for each price
        Map<Long, Long> results = new HashMap<>();

        // placing limit ask orders
        for (long price = topPrice; price > INITIAL_PRICE; price--) {
            OrderCommand cmd = OrderCommand.newOrder(GTC, orderId++, UID_1, price, 0, 1, ASK);
//            log.debug("BID {}", price);
            processAndValidate(cmd, SUCCESS);
            results.put(price, -1L);
        }

        for (long price = bottomPrice; price <= topPrice; price++) {
            long size = price * price;
            OrderCommand cmd = OrderCommand.newOrder(GTC, orderId++, UID_2, price, price * 10, size, BID);
//            log.debug("ASK {}", price);
            processAndValidate(cmd, SUCCESS);
            results.compute(price, (p, v) -> v == null ? size : v + size);

            //L2MarketData snapshot = orderBook.getL2MarketDataSnapshot(100000);
            //log.debug("A:{} B:{}", snapshot.askSize, snapshot.bidSize);
        }

        // collecting full order book
        L2MarketData snapshot = orderBook.getL2MarketDataSnapshot(-1);

        // check the number of records, should match to expected results
        assertThat(snapshot.bidSize, is(results.size()));

        // verify expected size for each price
        for (int i = 0; i < snapshot.bidSize; i++) {
            long price = snapshot.bidPrices[i];
            Long expectedSize = results.get(price);
            assertThat(expectedSize, notNullValue());
//            if (snapshot.askVolumes[i] != expectedSize) {
//                log.error("volume mismatch for price {} : diff={}", price, snapshot.askVolumes[i] - expectedSize);
//            }
            assertThat("volume mismatch for price " + price, snapshot.bidVolumes[i], is(expectedSize));
        }

        // obviously no aks records expected (they all should be matched)
        assertThat(snapshot.askSize, is(0));
    }

}