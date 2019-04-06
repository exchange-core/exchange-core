package org.openpredict.exchange.tests;

import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openpredict.exchange.beans.*;
import org.openpredict.exchange.beans.api.*;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.core.ExchangeCore;
import org.openpredict.exchange.tests.util.L2MarketDataHelper;
import org.openpredict.exchange.tests.util.TestOrdersGenerator;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.stream.LongStream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

@Slf4j
public class ITExchangeCoreIntegration extends IntegrationTestBase {

    @Before
    public void before() {

        exchangeCore = new ExchangeCore(cmd -> consumer.accept(cmd));
        exchangeCore.startup();
        api = exchangeCore.getApi();

        ApiAddSymbol addSymbol = ApiAddSymbol.builder().depositBuy(2200).depositSell(4210).symbolId(SYMBOL).build();
        api.submitCommand(addSymbol);

        BlockingQueue<OrderCommand> results = attachNewConsumerQueue();

        api.submitCommand(ApiAddUser.builder().uid(UID_1).build());
        api.submitCommand(ApiAddUser.builder().uid(UID_2).build());

        api.submitCommand(ApiAdjustUserBalance.builder().uid(UID_1).amount(1_000_000L).build());
        api.submitCommand(ApiAdjustUserBalance.builder().uid(UID_2).amount(2_000_000L).build());

        waitForOrderCommands(results, 4).forEach(cmd -> {
            assertEquals(CommandResultCode.SUCCESS, cmd.resultCode);
        });
    }

    @After
    public void after() {
        BlockingQueue<OrderCommand> results = attachNewConsumerQueue();
        api.submitCommand(ApiReset.builder().build());
        List<OrderCommand> commands = waitForOrderCommands(results, 1);
        assertThat(commands.get(0).resultCode, is(CommandResultCode.SUCCESS));
        exchangeCore.shutdown();
    }


    @Test(timeout = 10_000)
    public void basicFullCycleTest() throws Exception {

        BlockingQueue<OrderCommand> results = attachNewConsumerQueue();

        // ### 1. first trader places limit orders
        ApiPlaceOrder order101 = ApiPlaceOrder.builder().uid(UID_1).id(101).price(1600).size(7).action(OrderAction.ASK).orderType(OrderType.LIMIT).symbol(SYMBOL).build();
        log.debug("PLACE: {}", order101);
        api.submitCommand(order101);

        List<OrderCommand> orderCommands = waitForOrderCommands(results, 1);

        assertThat(orderCommands.size(), is(1));
        OrderCommand cmd = orderCommands.get(0);
        assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));
        assertThat(cmd.orderId, is(101L));
        assertThat(cmd.uid, is((long) UID_1));
        assertThat(cmd.price, is(1600L));
        assertThat(cmd.size, is(7L));
        assertThat(cmd.action, is(OrderAction.ASK));
        assertThat(cmd.orderType, is(OrderType.LIMIT));
        assertThat(cmd.symbol, is(SYMBOL));
        assertNull(cmd.matcherEvent);

        results.clear();

        ApiPlaceOrder order102 = ApiPlaceOrder.builder().uid(UID_1).id(102).price(1550).size(4).action(OrderAction.BID).orderType(OrderType.LIMIT).symbol(SYMBOL).build();
        log.debug("PLACE: {}", order102);
        api.submitCommand(order102);

        orderCommands = waitForOrderCommands(results, 1);
        assertThat(orderCommands.size(), is(1));
        cmd = orderCommands.get(0);
        assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));
        assertNull(cmd.matcherEvent);


        final L2MarketDataHelper l2helper = new L2MarketDataHelper().addAsk(1600, 7).addBid(1550, 4);
        assertEquals(l2helper.build(), requestCurrentOrderBook(results));

        // ### 2. second trader sends market order, first order partially matched
        results.clear();

        ApiPlaceOrder order201 = ApiPlaceOrder.builder().uid(UID_2).id(201).size(2).action(OrderAction.BID).orderType(OrderType.MARKET).symbol(SYMBOL).build();
        log.debug("PLACE: {}", order201);
        api.submitCommand(order201);

        orderCommands = waitForOrderCommands(results, 1);
        cmd = orderCommands.get(0);
        assertThat(orderCommands.size(), is(1));
        assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));

        List<MatcherTradeEvent> matcherEvents = cmd.extractEvents();
        assertThat(matcherEvents.size(), is(1));

        MatcherTradeEvent evt = matcherEvents.get(0);
        assertThat(evt.activeOrderId, is(201L));
        assertThat(evt.activeOrderAction, is(OrderAction.BID));
        assertThat(evt.activeOrderUid, is((long) UID_2));
        assertThat(evt.activeOrderCompleted, is(true));
        assertThat(evt.matchedOrderId, is(101L));
        assertThat(evt.matchedOrderUid, is((long) UID_1));
        assertThat(evt.matchedOrderCompleted, is(false));
        assertThat(evt.eventType, is(MatcherEventType.TRADE));
        assertThat(evt.size, is(2L));
        assertThat(evt.price, is(1600L));

        // volume decreased to 5
        l2helper.setAskVolume(0, 5);
        assertEquals(l2helper.build(), requestCurrentOrderBook(results));

        // ### 3. second trader places limit order
        ApiPlaceOrder order202 = ApiPlaceOrder.builder().uid(UID_2).id(202).price(1583).size(4).action(OrderAction.BID).orderType(OrderType.LIMIT).symbol(SYMBOL).build();
        log.debug("PLACE: {}", order202);
        api.submitCommand(order202);

        orderCommands = waitForOrderCommands(results, 1);
        cmd = orderCommands.get(0);
        assertThat(orderCommands.size(), is(1));
        assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));

        matcherEvents = cmd.extractEvents();
        assertThat(matcherEvents.size(), is(0));

        l2helper.insertBid(0, 1583, 4);
        assertEquals(l2helper.build(), requestCurrentOrderBook(results));

        //log.debug("{}", dumpOrderBook(matchingEngineRouter.getMarketData(SYMBOL, 10)));


        // ### 4. first trader moves his order - it will match existing order (202) but not entirely
        ApiMoveOrder moveOrder = ApiMoveOrder.builder().symbol(SYMBOL).uid(UID_1).id(101).newPrice(1580).build();
        log.debug("MOVE: {}", moveOrder);
        api.submitCommand(moveOrder);

        orderCommands = waitForOrderCommands(results, 1);
        cmd = orderCommands.get(0);
        assertThat(orderCommands.size(), is(1));
        assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));

        matcherEvents = cmd.extractEvents();
        assertThat(matcherEvents.size(), is(1));

        evt = matcherEvents.get(0);
        assertThat(evt.activeOrderId, is(101L));
        assertThat(evt.activeOrderAction, is(OrderAction.ASK));
        assertThat(evt.activeOrderUid, is((long) UID_1));
        assertThat(evt.activeOrderCompleted, is(false));
        assertThat(evt.matchedOrderId, is(202L));
        assertThat(evt.matchedOrderUid, is((long) UID_2));
        assertThat(evt.matchedOrderCompleted, is(true));
        assertThat(evt.eventType, is(MatcherEventType.TRADE));
        assertThat(evt.size, is(4L));
        assertThat(evt.price, is(1583L));

        l2helper.setAskPriceVolume(0, 1580, 1).removeBid(0);
        assertEquals(l2helper.build(), requestCurrentOrderBook(results));
    }

    private L2MarketData requestCurrentOrderBook(BlockingQueue<OrderCommand> results) {
        api.submitCommand(ApiOrderBookRequest.builder().symbol(SYMBOL).size(-1).build());
        OrderCommand orderBookCmd = waitForOrderCommands(results, 1).get(0);
        L2MarketData actualState = orderBookCmd.marketData;
        assertNotNull(actualState);
        return actualState;
    }


    @Test(timeout = 30_000)
    public void manyOperations() throws Exception {

        int numOrders = 1_000_000;
        int targetOrderBookOrders = 1000;
        int numUsers = 1000;

        TestOrdersGenerator.GenResult genResult = TestOrdersGenerator.generateCommands(numOrders, targetOrderBookOrders, numUsers, SYMBOL, false);
        List<ApiCommand> apiCommands = TestOrdersGenerator.convertToApiCommand(genResult.getCommands());

        final CountDownLatch usersLatch = new CountDownLatch(numUsers * 2);
        consumer = cmd -> usersLatch.countDown();
        LongStream.rangeClosed(1, numUsers).forEach(uid -> {
            api.submitCommand(ApiAddUser.builder().uid(uid).build());
            api.submitCommand(ApiAdjustUserBalance.builder().uid(uid).amount(7_000_000L).build());
        });
        usersLatch.await();

        final CountDownLatch ordersLatch = new CountDownLatch(apiCommands.size());
        consumer = cmd -> ordersLatch.countDown();
        for (ApiCommand cmd : apiCommands) {
            cmd.timestamp = System.currentTimeMillis();
            api.submitCommand(cmd);
        }
        ordersLatch.await();

        // compare orderBook final state just to make sure all commands executed same way
        // TODO compare events, wait until finish
        L2MarketData l2MarketData = requestCurrentOrderBook(attachNewConsumerQueue());
        assertEquals(genResult.getFinalOrderBookSnapshot(), l2MarketData);
        assertTrue(l2MarketData.askSize > targetOrderBookOrders / 4);
        assertTrue(l2MarketData.bidSize > targetOrderBookOrders / 4);
    }

}