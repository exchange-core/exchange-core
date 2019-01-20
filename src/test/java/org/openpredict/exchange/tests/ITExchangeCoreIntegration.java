package org.openpredict.exchange.tests;

import lombok.extern.slf4j.Slf4j;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openpredict.exchange.beans.*;
import org.openpredict.exchange.beans.api.*;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.core.*;
import org.openpredict.exchange.tests.util.L2MarketDataHelper;
import org.openpredict.exchange.tests.util.TestOrdersGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@SpringBootTest
@ComponentScan(basePackages = {
        "org.openpredict.exchange",
})
@TestPropertySource(locations = "classpath:it.properties")
@Slf4j
public class ITExchangeCoreIntegration {

    @Autowired
    private ExchangeApi apiCore;

    @Autowired
    private ExchangeCore exchangeCore;

    @Autowired
    private PortfolioService portfolioService;

    @Autowired
    private UserProfileService userProfileService;

    @Autowired
    private MatchingEngineRouter matchingEngineRouter;

    @Autowired
    private RiskEngine riskEngine;

    @Autowired
    private SymbolSpecificationProvider symbolSpecificationProvider;

    @MockBean
    private Consumer<OrderCommand> resultsConsumerMock;

    private TestOrdersGenerator generator = new TestOrdersGenerator();

    private static final int SYMBOL = 5991;
    private static final int UID_1 = 1442412;
    private static final int UID_2 = 1442413;

    @Before
    public void before() {
        detatchConsumer();

        CoreSymbolSpecification spec = CoreSymbolSpecification.builder().depositBuy(2200).depositSell(4210).symbolId(SYMBOL).build();
        symbolSpecificationProvider.registerSymbol(SYMBOL, spec);
        matchingEngineRouter.addOrderBook(SYMBOL);

        BlockingQueue<OrderCommand> results = attachBufferedConsumer();

        apiCore.submitCommand(ApiAddUser.builder().uid(UID_1).build());
        apiCore.submitCommand(ApiAddUser.builder().uid(UID_2).build());

        apiCore.submitCommand(ApiAdjustUserBalance.builder().uid(UID_1).amount(1_000_000L).build());
        apiCore.submitCommand(ApiAdjustUserBalance.builder().uid(UID_2).amount(2_000_000L).build());

        waitForOrderCommands(results, 4).forEach(cmd -> assertEquals(CommandResultCode.SUCCESS, cmd.resultCode));
    }

    @After
    public void after() throws Exception{
        Thread.sleep(100);
        BlockingQueue<OrderCommand> results = attachBufferedConsumer();
        apiCore.submitCommand(ApiReset.builder().build());
        List<OrderCommand> commands = waitForOrderCommands(results, 1);
        assertThat(commands.get(0).resultCode, is(CommandResultCode.SUCCESS));
        detatchConsumer();
    }


    @Test
    public void contextStarts() {

    }

    @Test
    public void basicFullCycleTest() throws Exception {

        BlockingQueue<OrderCommand> results = attachBufferedConsumer();

        // ### 1. first trader places limit orders
        ApiPlaceOrder order101 = ApiPlaceOrder.builder().uid(UID_1).id(101).price(1600).size(7).action(OrderAction.ASK).orderType(OrderType.LIMIT).symbol(SYMBOL).build();
        log.debug("PLACE: {}", order101);
        apiCore.submitCommand(order101);

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
        apiCore.submitCommand(order102);

        orderCommands = waitForOrderCommands(results, 1);
        assertThat(orderCommands.size(), is(1));
        cmd = orderCommands.get(0);
        assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));
        assertNull(cmd.matcherEvent);

        L2MarketDataHelper l2helper = new L2MarketDataHelper().addAsk(1600, 7).addBid(1550, 4);
        L2MarketData expectedState = l2helper.build();
        assertEquals(expectedState, matchingEngineRouter.getMarketData(SYMBOL, 10));


        // ### 2. second trader sends market order, first order partially matched
        results.clear();

        ApiPlaceOrder order201 = ApiPlaceOrder.builder().uid(UID_2).id(201).size(2).action(OrderAction.BID).orderType(OrderType.MARKET).symbol(SYMBOL).build();
        log.debug("PLACE: {}", order201);
        apiCore.submitCommand(order201);

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
        expectedState = l2helper.setAskVolume(0, 5).build();
        Thread.sleep(100);
        assertEquals(expectedState, matchingEngineRouter.getMarketData(SYMBOL, 10));


        // ### 3. second trader places limit order
        ApiPlaceOrder order202 = ApiPlaceOrder.builder().uid(UID_2).id(202).price(1583).size(4).action(OrderAction.BID).orderType(OrderType.LIMIT).symbol(SYMBOL).build();
        log.debug("PLACE: {}", order202);
        apiCore.submitCommand(order202);

        orderCommands = waitForOrderCommands(results, 1);
        cmd = orderCommands.get(0);
        assertThat(orderCommands.size(), is(1));
        assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));

        matcherEvents = cmd.extractEvents();
        assertThat(matcherEvents.size(), is(0));

        expectedState = l2helper.insertBid(0, 1583, 4).build();
        Thread.sleep(100);
        assertEquals(expectedState, matchingEngineRouter.getMarketData(SYMBOL, 10));

        //log.debug("{}", dumpOrderBook(matchingEngineRouter.getMarketData(SYMBOL, 10)));


        // ### 4. first trader moves his order - it will match existing order (202) but not entirely
        ApiMoveOrder moveOrder = ApiMoveOrder.builder().symbol(SYMBOL).uid(UID_1).id(101).newPrice(1580).build();
        log.debug("MOVE: {}", moveOrder);
        apiCore.submitCommand(moveOrder);

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

        expectedState = l2helper.setAskPriceVolume(0, 1580, 1).removeBid(0).build();
        Thread.sleep(100);
        assertEquals(expectedState, matchingEngineRouter.getMarketData(SYMBOL, 10));

    }

    private BlockingQueue<OrderCommand> attachBufferedConsumer() {
        BlockingQueue<OrderCommand> results = new LinkedBlockingQueue<>();

        exchangeCore.setResultsConsumer(cmd -> {
            //log.debug(">>>>: {}", cmd);
            results.add(cmd.copy());
        });
        return results;
    }

    private List<OrderCommand> waitForOrderCommands(BlockingQueue<OrderCommand> results, int c) {
        return Stream.generate(() -> {
            try {
                return results.poll(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ex) {
                throw new IllegalStateException();
            }
        }).limit(c).collect(Collectors.toList());
    }

    @Test
    public void manyOperations() throws Exception {

        int numOrders = 200_000;
        int targetOrderBookOrders = 1000;
        int numUsers = 1000;

        List<Long> uids = Stream.iterate(1L, i -> i + 1).limit(numUsers).collect(Collectors.toList());
        TestOrdersGenerator.GenResult genResult = generator.generateCommands(numOrders, targetOrderBookOrders, uids, SYMBOL, false);
        List<ApiCommand> apiCommands = generator.convertToApiCommand(genResult.getCommands());

        detatchConsumer();

        Thread.sleep(20);
        userProfileService.reset();
        matchingEngineRouter.reset();
        Thread.sleep(20);
        uids.forEach(uid -> {
            apiCore.submitCommand(ApiAddUser.builder().uid(uid).build());
            apiCore.submitCommand(ApiAdjustUserBalance.builder().uid(uid).amount(7_000_000L).build());
        });

        System.gc();
        Thread.sleep(200);

        log.info("Start");
        for (ApiCommand cmd : apiCommands) {
            cmd.timestamp = System.currentTimeMillis();
            apiCore.submitCommand(cmd);
        }
        log.info("Done");
        Thread.sleep(200);

        // weak compare orderBook final state just to make sure all commands executed same way
        // TODO compare events, wait until finish
        assertThat(matchingEngineRouter.getOrderBook(SYMBOL).hashCode(), Matchers.is(genResult.getFinalOrderbookHash()));

    }

    private void detatchConsumer() {
        exchangeCore.setResultsConsumer(cmd -> {
            //log.debug("Result: {}", cmd);
        });
    }

}