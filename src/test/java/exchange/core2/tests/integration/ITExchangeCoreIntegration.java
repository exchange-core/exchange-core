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
package exchange.core2.tests.integration;

import exchange.core2.core.common.*;
import exchange.core2.core.common.api.ApiAdjustUserBalance;
import exchange.core2.core.common.api.ApiCancelOrder;
import exchange.core2.core.common.api.ApiMoveOrder;
import exchange.core2.core.common.api.ApiPlaceOrder;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.common.config.PerformanceConfiguration;
import exchange.core2.tests.util.ExchangeTestContainer;
import exchange.core2.tests.util.L2MarketDataHelper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.List;
import org.junit.jupiter.api.Timeout;

import static exchange.core2.core.common.OrderAction.ASK;
import static exchange.core2.core.common.OrderType.GTC;
import static exchange.core2.tests.util.ExchangeTestContainer.CHECK_SUCCESS;
import static exchange.core2.tests.util.TestConstants.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
//TODO test cases are moved to cucumber scenarios, remove this class
public abstract class ITExchangeCoreIntegration {

    // configuration provided by child class
    public abstract PerformanceConfiguration getPerformanceConfiguration();

    @Test
    @Timeout(5)
    public void basicFullCycleTestMargin() {
        basicFullCycleTest(SYMBOLSPEC_EUR_USD);
    }

    @Test
    @Timeout(5)
    public void basicFullCycleTestExchange() {

        basicFullCycleTest(SYMBOLSPEC_ETH_XBT);
    }

    @Test
    @Timeout(5)
    public void shouldInitSymbols() {
        try (final ExchangeTestContainer container = ExchangeTestContainer.create(getPerformanceConfiguration())) {
            container.initBasicSymbols();
        }
    }

    @Test
    @Timeout(5)
    public void shouldInitUsers() {
        try (final ExchangeTestContainer container = ExchangeTestContainer.create(getPerformanceConfiguration())) {
            container.initBasicUsers();
        }
    }


    // TODO count/verify number of commands and events
    private void basicFullCycleTest(final CoreSymbolSpecification symbolSpec) {

        try (final ExchangeTestContainer container = ExchangeTestContainer.create(getPerformanceConfiguration())) {
            container.initBasicSymbols();
            container.initBasicUsers();

            // ### 1. first user places limit orders
            final ApiPlaceOrder order101 = ApiPlaceOrder.builder().uid(UID_1).orderId(101).price(1600).size(7).action(ASK).orderType(GTC).symbol(symbolSpec.symbolId).build();

            log.debug("PLACE 101: {}", order101);
            container.submitCommandSync(order101, cmd -> {
                assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));
                assertThat(cmd.orderId, is(101L));
                assertThat(cmd.uid, is(UID_1));
                assertThat(cmd.price, is(1600L));
                assertThat(cmd.size, is(7L));
                assertThat(cmd.action, is(ASK));
                assertThat(cmd.orderType, is(GTC));
                assertThat(cmd.symbol, is(symbolSpec.symbolId));
                assertNull(cmd.matcherEvent);
            });

            final int reserve102 = symbolSpec.type == SymbolType.CURRENCY_EXCHANGE_PAIR ? 1561 : 0;
            final ApiPlaceOrder order102 = ApiPlaceOrder.builder().uid(UID_1).orderId(102).price(1550).reservePrice(reserve102).size(4)
                    .action(OrderAction.BID).orderType(GTC).symbol(symbolSpec.symbolId).build();
            log.debug("PLACE 102: {}", order102);
            container.submitCommandSync(order102, cmd -> {
                assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));
                assertNull(cmd.matcherEvent);
            });

            final L2MarketDataHelper l2helper = new L2MarketDataHelper().addAsk(1600, 7).addBid(1550, 4);
            assertEquals(l2helper.build(), container.requestCurrentOrderBook(symbolSpec.symbolId));

            // ### 2. second user sends market order, first order partially matched
            final int reserve201 = symbolSpec.type == SymbolType.CURRENCY_EXCHANGE_PAIR ? 1800 : 0;
            final ApiPlaceOrder order201 = ApiPlaceOrder.builder().uid(UID_2).orderId(201).price(1700).reservePrice(reserve201).size(2).action(OrderAction.BID).orderType(OrderType.IOC).symbol(symbolSpec.symbolId).build();
            log.debug("PLACE 201: {}", order201);
            container.submitCommandSync(order201, cmd -> {
                assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));

                List<MatcherTradeEvent> matcherEvents = cmd.extractEvents();
                assertThat(matcherEvents.size(), is(1));

                assertThat(cmd.action, is(OrderAction.BID));
                assertThat(cmd.orderId, is(201L));
                assertThat(cmd.uid, is(UID_2));

                MatcherTradeEvent evt = matcherEvents.get(0);
                assertThat(evt.activeOrderCompleted, is(true));
                assertThat(evt.matchedOrderId, is(101L));
                assertThat(evt.matchedOrderUid, is(UID_1));
                assertThat(evt.matchedOrderCompleted, is(false));
                assertThat(evt.eventType, is(MatcherEventType.TRADE));
                assertThat(evt.size, is(2L));
                assertThat(evt.price, is(1600L));
            });

            // volume is decreased to 5
            l2helper.setAskVolume(0, 5);
            assertEquals(l2helper.build(), container.requestCurrentOrderBook(symbolSpec.symbolId));


            // ### 3. second user places limit order
            final int reserve202 = symbolSpec.type == SymbolType.CURRENCY_EXCHANGE_PAIR ? 1583 : 0;
            final ApiPlaceOrder order202 = ApiPlaceOrder.builder().uid(UID_2).orderId(202).price(1583).reservePrice(reserve202)
                    .size(4).action(OrderAction.BID).orderType(GTC).symbol(symbolSpec.symbolId).build();
            log.debug("PLACE 202: {}", order202);
            container.submitCommandSync(order202, cmd -> {
                assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));
                assertNull(cmd.matcherEvent);
                List<MatcherTradeEvent> matcherEvents = cmd.extractEvents();
                assertThat(matcherEvents.size(), is(0));
            });

            l2helper.insertBid(0, 1583, 4);
            assertEquals(l2helper.build(), container.requestCurrentOrderBook(symbolSpec.symbolId));


            // ### 4. first trader moves his order - it will match existing order (202) but not entirely
            final ApiMoveOrder moveOrder = ApiMoveOrder.builder().symbol(symbolSpec.symbolId).uid(UID_1).orderId(101).newPrice(1580).build();
            log.debug("MOVE 101: {}", moveOrder);
            container.submitCommandSync(moveOrder, cmd -> {
                assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));

                List<MatcherTradeEvent> matcherEvents = cmd.extractEvents();
                assertThat(matcherEvents.size(), is(1));

                assertThat(cmd.action, is(ASK));
                assertThat(cmd.orderId, is(101L));
                assertThat(cmd.uid, is(UID_1));

                MatcherTradeEvent evt = matcherEvents.get(0);
                assertThat(evt.activeOrderCompleted, is(false));
                assertThat(evt.matchedOrderId, is(202L));
                assertThat(evt.matchedOrderUid, is(UID_2));
                assertThat(evt.matchedOrderCompleted, is(true));
                assertThat(evt.eventType, is(MatcherEventType.TRADE));
                assertThat(evt.size, is(4L));
                assertThat(evt.price, is(1583L));
            });

            l2helper.setAskPriceVolume(0, 1580, 1).removeBid(0);
            assertEquals(l2helper.build(), container.requestCurrentOrderBook(symbolSpec.symbolId));

            assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());
        }
    }


    @Test
    @Timeout(5)
    public void exchangeRiskBasicTest() throws Exception {

        try (final ExchangeTestContainer container = ExchangeTestContainer.create(getPerformanceConfiguration())) {
            container.initBasicSymbols();
            container.createUserWithMoney(UID_1, CURRENECY_XBT, 2_000_000); // 2M satoshi (0.02 BTC)

            // try submit an order - limit BUY 7 lots, price 300K satoshi (30K x10 step) for each lot 100K szabo
            // should be rejected
            final ApiPlaceOrder order101 = ApiPlaceOrder.builder().uid(UID_1).orderId(101).price(30_000).reservePrice(30_000)
                    .size(7).action(OrderAction.BID).orderType(GTC).symbol(SYMBOL_EXCHANGE).build();

            container.submitCommandSync(order101, cmd -> {
                assertThat(cmd.resultCode, is(CommandResultCode.RISK_NSF));
            });

            // verify
            container.validateUserState(UID_1, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(2_000_000L));
                assertTrue(profile.fetchIndexedOrders().isEmpty());
            });

            // add 100K more
            container.submitCommandSync(ApiAdjustUserBalance.builder().uid(UID_1).currency(CURRENECY_XBT).amount(100_000).transactionId(223948217349827L).build(), CHECK_SUCCESS);

            // submit order again - should be placed
            container.submitCommandSync(order101, cmd -> {
                assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));
                assertThat(cmd.orderId, is(101L));
                assertThat(cmd.uid, is(UID_1));
                assertThat(cmd.price, is(30_000L));
                assertThat(cmd.reserveBidPrice, is(30_000L));
                assertThat(cmd.size, is(7L));
                assertThat(cmd.action, is(OrderAction.BID));
                assertThat(cmd.orderType, is(GTC));
                assertThat(cmd.symbol, is(SYMBOL_EXCHANGE));
                assertNull(cmd.matcherEvent);
            });

            // verify order placed with correct reserve price and account balance is updated accordingly
            container.validateUserState(UID_1, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(0L));
                assertThat(profile.fetchIndexedOrders().get(101L).price, is(30_000L));
            });

            container.createUserWithMoney(UID_2, CURRENECY_ETH, 699_999); // 699'999 szabo (<~0.7 ETH)
            // try submit an order - sell 7 lots, price 300K satoshi (30K x10 step) for each lot 100K szabo
            // should be rejected
            final ApiPlaceOrder order102 = ApiPlaceOrder.builder().uid(UID_2).orderId(102).price(30_000).size(7).action(ASK).orderType(OrderType.IOC).symbol(SYMBOL_EXCHANGE).build();
            container.submitCommandSync(order102, cmd -> {
                assertThat(cmd.resultCode, is(CommandResultCode.RISK_NSF));
            });

            // verify order is rejected and account balance is not changed
            container.validateUserState(UID_2, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_ETH), is(699_999L));
                assertTrue(profile.fetchIndexedOrders().isEmpty());
            });

            // add 1 szabo more
            container.submitCommandSync(ApiAdjustUserBalance.builder().uid(UID_2).currency(CURRENECY_ETH).amount(1).transactionId(2193842938742L).build(), CHECK_SUCCESS);

            // submit order again - should be matched
            container.submitCommandSync(order102, cmd -> {
                assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));
                assertThat(cmd.orderId, is(102L));
                assertThat(cmd.uid, is(UID_2));
                assertThat(cmd.price, is(30_000L));
                assertThat(cmd.size, is(7L));
                assertThat(cmd.action, is(ASK));
                assertThat(cmd.orderType, is(OrderType.IOC));
                assertThat(cmd.symbol, is(SYMBOL_EXCHANGE));
                assertNotNull(cmd.matcherEvent);
            });

            container.validateUserState(UID_2, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(2_100_000L));
                assertThat(profile.getAccounts().get(CURRENECY_ETH), is(0L));
                assertTrue(profile.fetchIndexedOrders().isEmpty());
            });

            container.validateUserState(UID_1, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_ETH), is(700_000L));
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(0L));
                assertTrue(profile.fetchIndexedOrders().isEmpty());
            });

            assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());
        }
    }

    @Test
    @Timeout(5)
    public void exchangeRiskMoveTest() throws Exception {

        try (final ExchangeTestContainer container = ExchangeTestContainer.create(getPerformanceConfiguration())) {
            container.initBasicSymbols();
            container.createUserWithMoney(UID_1, CURRENECY_ETH, 100_000_000); // 100M szabo (100 ETH)

            // try submit an order - sell 1001 lots, price 300K satoshi (30K x10 step) for each lot 100K szabo
            // should be rejected
            container.submitCommandSync(ApiPlaceOrder.builder().uid(UID_1).orderId(202).price(30_000).size(1001).action(ASK).orderType(GTC).symbol(SYMBOL_EXCHANGE).build(),
                    cmd -> {
                        assertThat(cmd.resultCode, is(CommandResultCode.RISK_NSF));
                    });

            container.validateUserState(UID_1, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_ETH), is(100_000_000L));
                assertTrue(profile.fetchIndexedOrders().isEmpty());
            });

            // submit order again - should be placed
            container.submitCommandSync(
                    ApiPlaceOrder.builder().uid(UID_1).orderId(202).price(30_000).size(1000).action(ASK).orderType(GTC).symbol(SYMBOL_EXCHANGE).build(),
                    cmd -> {
                        assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));
                        assertThat(cmd.command, is(OrderCommandType.PLACE_ORDER));
                        assertThat(cmd.orderId, is(202L));
                        assertThat(cmd.uid, is(UID_1));
                        assertThat(cmd.price, is(30_000L));
                        assertThat(cmd.size, is(1000L));
                        assertThat(cmd.action, is(ASK));
                        assertThat(cmd.orderType, is(GTC));
                        assertThat(cmd.symbol, is(SYMBOL_EXCHANGE));
                        assertNull(cmd.matcherEvent);
                    });

            container.validateUserState(UID_1, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_ETH), is(0L));
                assertTrue(profile.fetchIndexedOrders().containsKey(202L));
            });

            // move order to higher price - shouldn't be a problem for ASK order
            container.submitCommandSync(
                    ApiMoveOrder.builder().uid(UID_1).orderId(202).newPrice(40_000).symbol(SYMBOL_EXCHANGE).build(),
                    cmd -> {
                        assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));
                        assertThat(cmd.command, is(OrderCommandType.MOVE_ORDER));
                        assertThat(cmd.orderId, is(202L));
                        assertThat(cmd.uid, is(UID_1));
                        assertThat(cmd.price, is(40_000L));
                        assertThat(cmd.symbol, is(SYMBOL_EXCHANGE));
                        assertNull(cmd.matcherEvent);
                    });

            container.validateUserState(UID_1, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_ETH), is(0L));
                assertTrue(profile.fetchIndexedOrders().containsKey(202L));
            });

            // move order to lower price - shouldn't be a problem as well for ASK order
            container.submitCommandSync(
                    ApiMoveOrder.builder().uid(UID_1).orderId(202).newPrice(20_000).symbol(SYMBOL_EXCHANGE).build(),
                    cmd -> {
                        assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));
                        assertThat(cmd.command, is(OrderCommandType.MOVE_ORDER));
                        assertThat(cmd.orderId, is(202L));
                        assertThat(cmd.uid, is(UID_1));
                        assertThat(cmd.price, is(20_000L));
                        assertThat(cmd.symbol, is(SYMBOL_EXCHANGE));
                        assertNull(cmd.matcherEvent);
                    });

            container.validateUserState(UID_1, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_ETH), is(0L));
                assertTrue(profile.fetchIndexedOrders().containsKey(202L));
            });

            // create user
            container.createUserWithMoney(UID_2, CURRENECY_XBT, 94_000_000); // 94M satoshi (0.94 BTC)

            // try submit order with reservePrice above funds limit - rejected
            container.submitCommandSync(
                    ApiPlaceOrder.builder().uid(UID_2).orderId(203).price(18_000).reservePrice(19_000).size(500).action(OrderAction.BID).orderType(GTC).symbol(SYMBOL_EXCHANGE).build(),
                    cmd -> {
                        assertThat(cmd.resultCode, is(CommandResultCode.RISK_NSF));
                    });

            container.validateUserState(UID_2, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(94_000_000L));
                assertTrue(profile.fetchIndexedOrders().isEmpty());
            });

            // submit order with reservePrice below funds limit - should be placed
            container.submitCommandSync(
                    ApiPlaceOrder.builder().uid(UID_2).orderId(203).price(18_000).reservePrice(18_500).size(500).action(OrderAction.BID).orderType(GTC).symbol(SYMBOL_EXCHANGE).build(),
                    cmd -> {
                        assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));
                        assertThat(cmd.command, is(OrderCommandType.PLACE_ORDER));
                        assertThat(cmd.orderId, is(203L));
                        assertThat(cmd.uid, is(UID_2));
                        assertThat(cmd.price, is(18_000L));
                        assertThat(cmd.reserveBidPrice, is(18_500L));
                        assertThat(cmd.size, is(500L));
                        assertThat(cmd.action, is(OrderAction.BID));
                        assertThat(cmd.orderType, is(GTC));
                        assertThat(cmd.symbol, is(SYMBOL_EXCHANGE));
                        assertNull(cmd.matcherEvent);
                    });


            // expected balance when 203 placed with reserve price 18_500
            final long ethUid2 = 94_000_000L - 18_500 * 500 * SYMBOLSPEC_ETH_XBT.getQuoteScaleK();

            container.validateUserState(UID_2, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(ethUid2));
                assertTrue(profile.fetchIndexedOrders().containsKey(203L));
            });

            // move order to lower price - shouldn't be a problem for BID order
            container.submitCommandSync(
                    ApiMoveOrder.builder().uid(UID_2).orderId(203).newPrice(15_000).symbol(SYMBOL_EXCHANGE).build(),
                    cmd -> {
                        assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));
                        assertThat(cmd.command, is(OrderCommandType.MOVE_ORDER));
                        assertThat(cmd.orderId, is(203L));
                        assertThat(cmd.uid, is(UID_2));
                        assertThat(cmd.price, is(15_000L));
                        assertThat(cmd.symbol, is(SYMBOL_EXCHANGE));
                        assertNull(cmd.matcherEvent);
                    });

            container.validateUserState(UID_2, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(ethUid2));
                assertThat(profile.fetchIndexedOrders().get(203L).price, is(15_000L));
            });

            // move order to higher price (above limit) - should be rejected
            container.submitCommandSync(
                    ApiMoveOrder.builder().uid(UID_2).orderId(203).newPrice(18_501).symbol(SYMBOL_EXCHANGE).build(),
                    cmd -> {
                        assertThat(cmd.resultCode, is(CommandResultCode.MATCHING_MOVE_FAILED_PRICE_OVER_RISK_LIMIT));
                    });

            container.validateUserState(UID_2, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(ethUid2));
                assertThat(profile.fetchIndexedOrders().get(203L).price, is(15_000L));
            });

            // move order to higher price (equals limit) - should be accepted
            container.submitCommandSync(
                    ApiMoveOrder.builder().uid(UID_2).orderId(203).newPrice(18_500).symbol(SYMBOL_EXCHANGE).build(),
                    cmd -> {
                        assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));
                        assertThat(cmd.command, is(OrderCommandType.MOVE_ORDER));
                        assertThat(cmd.orderId, is(203L));
                        assertThat(cmd.uid, is(UID_2));
                        assertThat(cmd.price, is(18_500L));
                        assertThat(cmd.symbol, is(SYMBOL_EXCHANGE));
                        assertNull(cmd.matcherEvent);
                    });

            container.validateUserState(UID_2, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(ethUid2));
                assertThat(profile.fetchIndexedOrders().get(203L).price, is(18_500L));
            });

            // set second order price to 17'500
            container.submitCommandSync(
                    ApiMoveOrder.builder().uid(UID_2).orderId(203).newPrice(17_500).symbol(SYMBOL_EXCHANGE).build(),
                    cmd -> {
                        assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));
                    });

            container.validateUserState(UID_2, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(ethUid2));
                assertThat(profile.fetchIndexedOrders().get(203L).price, is(17_500L));
            });

            // move ASK order to lower price 16'900 so it will trigger trades (by maker's price 17_500)
            container.submitCommandSync(
                    ApiMoveOrder.builder().uid(UID_1).orderId(202).newPrice(16_900).symbol(SYMBOL_EXCHANGE).build(),
                    cmd -> {
                        assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));
                        assertThat(cmd.command, is(OrderCommandType.MOVE_ORDER));
                        assertThat(cmd.orderId, is(202L));
                        assertThat(cmd.uid, is(UID_1));
                        assertThat(cmd.price, is(16_900L));
                        assertThat(cmd.symbol, is(SYMBOL_EXCHANGE));

                        assertThat(cmd.action, is(ASK));

                        final MatcherTradeEvent evt = cmd.matcherEvent;
                        assertNotNull(evt);
                        assertThat(evt.eventType, is(MatcherEventType.TRADE));
                        assertThat(evt.activeOrderCompleted, is(false));
                        assertThat(evt.matchedOrderId, is(203L));
                        assertThat(evt.matchedOrderUid, is(UID_2));
                        assertThat(evt.matchedOrderCompleted, is(true));
                        assertThat(evt.price, is(17_500L)); // user price from maker order
                        assertThat(evt.bidderHoldPrice, is(18_500L)); // user original reserve price from bidder order (203)
                        assertThat(evt.size, is(500L));
                    });

            // check UID_1 has 87.5M satoshi (17_500 * 10 * 500) and half-filled SELL order
            container.validateUserState(UID_1, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(87_500_000L));
                assertThat(profile.getAccounts().get(CURRENECY_ETH), is(0L));
                assertThat(profile.fetchIndexedOrders().get(202L).filled, is(500L));
            });

            // check UID_2 has 6.5M satoshi (after 94M), and 50M szabo (10_000 * 500)
            container.validateUserState(UID_2, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(6_500_000L));
                assertThat(profile.getAccounts().get(CURRENECY_ETH), is(50_000_000L));
                assertTrue(profile.fetchIndexedOrders().isEmpty());
            });

            // cancel remaining order
            container.submitCommandSync(
                    ApiCancelOrder.builder().orderId(202).uid(UID_1).symbol(SYMBOL_EXCHANGE).build(),
                    cmd -> {
                        assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));
                        assertThat(cmd.command, is(OrderCommandType.CANCEL_ORDER));
                        assertThat(cmd.orderId, is(202L));
                        assertThat(cmd.uid, is(UID_1));
                        assertThat(cmd.symbol, is(SYMBOL_EXCHANGE));

                        assertThat(cmd.action, is(ASK));

                        final MatcherTradeEvent evt = cmd.matcherEvent;
                        assertNotNull(evt);
                        assertThat(evt.eventType, is(MatcherEventType.REDUCE));
                        assertThat(evt.size, is(500L));
                    });

            // check UID_1 has 87.5M satoshi (17_500 * 10 * 500) and 50M szabo (after 100M)
            container.validateUserState(UID_1, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(87_500_000L));
                assertThat(profile.getAccounts().get(CURRENECY_ETH), is(50_000_000L));
                assertTrue(profile.fetchIndexedOrders().isEmpty());
            });

            assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());
        }
    }

    @Test
    @Timeout(5)
    public void exchangeCancelBid() throws Exception {

        try (final ExchangeTestContainer container = ExchangeTestContainer.create(getPerformanceConfiguration())) {
            container.initBasicSymbols();

            // create user
            container.createUserWithMoney(UID_2, CURRENECY_XBT, 94_000_000); // 94M satoshi (0.94 BTC)

            // submit order with reservePrice below funds limit - should be placed
            container.submitCommandSync(
                    ApiPlaceOrder.builder().uid(UID_2).orderId(203).price(18_000).reservePrice(18_500).size(500).action(OrderAction.BID).orderType(GTC).symbol(SYMBOL_EXCHANGE).build(),
                    cmd -> {
                        assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));
                    });

            // verify order placed with correct reserve price and account balance is updated accordingly
            container.validateUserState(UID_2, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(94_000_000L - 18_500 * 500 * SYMBOLSPEC_ETH_XBT.getQuoteScaleK()));
                assertThat(profile.fetchIndexedOrders().get(203L).reserveBidPrice, is(18_500L));
            });

            // cancel remaining order
            container.submitCommandSync(
                    ApiCancelOrder.builder().orderId(203).uid(UID_2).symbol(SYMBOL_EXCHANGE).build(),
                    cmd -> {
                        assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS));
                        assertThat(cmd.command, is(OrderCommandType.CANCEL_ORDER));
                        assertThat(cmd.orderId, is(203L));
                        assertThat(cmd.uid, is(UID_2));
                        assertThat(cmd.symbol, is(SYMBOL_EXCHANGE));

                        assertThat(cmd.action, is(OrderAction.BID));

                        final MatcherTradeEvent evt = cmd.matcherEvent;
                        assertNotNull(evt);
                        assertThat(evt.eventType, is(MatcherEventType.REDUCE));
                        assertThat(evt.bidderHoldPrice, is(18_500L));
                        assertThat(evt.size, is(500L));
                    });

            // verify that all 94M satoshi were returned back
            container.validateUserState(UID_2, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(94_000_000L));
                assertTrue(profile.fetchIndexedOrders().isEmpty());
            });

            assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());
        }
    }
}