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

import exchange.core2.core.ExchangeApi;
import exchange.core2.core.common.L2MarketData;
import exchange.core2.core.common.MatcherTradeEvent;
import exchange.core2.core.common.api.ApiNop;
import exchange.core2.core.common.api.ApiPlaceOrder;
import exchange.core2.core.common.api.reports.SingleUserReportQuery;
import exchange.core2.core.common.api.reports.StateHashReportQuery;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.config.PerformanceConfiguration;
import exchange.core2.core.orderbook.OrderBookDirectImpl;
import exchange.core2.core.orderbook.OrderBookEventsHelper;
import exchange.core2.tests.util.ExchangeTestContainer;
import exchange.core2.tests.util.TestConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static exchange.core2.core.common.MatcherEventType.TRADE;
import static exchange.core2.core.common.OrderAction.ASK;
import static exchange.core2.core.common.OrderAction.BID;
import static exchange.core2.core.common.OrderType.GTC;
import static exchange.core2.core.common.OrderType.IOC;
import static exchange.core2.core.common.cmd.CommandResultCode.SUCCESS;
import static exchange.core2.core.common.cmd.OrderCommandType.BINARY_DATA_QUERY;
import static exchange.core2.core.common.cmd.OrderCommandType.ORDER_BOOK_REQUEST;
import static exchange.core2.core.common.cmd.OrderCommandType.PLACE_ORDER;
import static exchange.core2.tests.util.TestConstants.CURRENECY_ETH;
import static exchange.core2.tests.util.TestConstants.CURRENECY_XBT;
import static exchange.core2.tests.util.TestConstants.SYMBOLSPEC_ETH_XBT;
import static exchange.core2.tests.util.TestConstants.SYMBOL_EXCHANGE;
import static exchange.core2.tests.util.TestConstants.UID_1;
import static exchange.core2.tests.util.TestConstants.UID_2;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FullAsyncResponseIntegrationTest {

    @Test
    @Timeout(10)
    void largeResponsesAreReleasedBeforeRingBufferReuse() throws Exception {
        try (ExchangeTestContainer container = ExchangeTestContainer.create(PerformanceConfiguration.DEFAULT)) {
            container.addSymbol(SYMBOLSPEC_ETH_XBT);
            container.createUserWithMoney(UID_1, CURRENECY_ETH, 100_000_000);
            final ExchangeApi api = container.getApi();
            assertEquals(SUCCESS, api.submitCommandAsync(ApiPlaceOrder.builder()
                    .symbol(SYMBOL_EXCHANGE).uid(UID_1).action(ASK).orderType(GTC)
                    .orderId(TestConstants.orderIdOf(101)).price(1600).size(1).build()).get(3, SECONDS));

            final List<OrderCommand> largeResponseSlots = new CopyOnWriteArrayList<>();
            container.setConsumer((cmd, seq) -> {
                if ((cmd.command == BINARY_DATA_QUERY && cmd.matcherEvent != null)
                        || (cmd.command == ORDER_BOOK_REQUEST && cmd.marketData != null)) {
                    largeResponseSlots.add(cmd);
                }
            });

            final var report = api.processReport(new StateHashReportQuery(), 1).get(3, SECONDS);
            final int customReportSections = api.submitQueryAsync(new SingleUserReportQuery(UID_1), 2,
                    cmd -> OrderBookEventsHelper.deserializeEvents(cmd).size()).get(3, SECONDS);
            final L2MarketData book = api.requestOrderBookAsync(SYMBOL_EXCHANGE, -1).get(3, SECONDS);
            // The results stage handles commands in order, so the earlier slots are released before this completes.
            assertEquals(SUCCESS, api.submitCommandAsync(ApiNop.builder().build()).get(3, SECONDS));

            assertFalse(report.getHashCodes().isEmpty());
            assertTrue(customReportSections > 0);
            assertArrayEquals(new long[]{1600}, book.getAskPricesCopy());
            assertEquals(3, largeResponseSlots.size(), "The results consumer must still see the responses");
            for (OrderCommand slot : largeResponseSlots) {
                assertNull(slot.matcherEvent, () -> slot.command + " must release its binary events");
                assertNull(slot.marketData, () -> slot.command + " must release its order book snapshot");
            }
        }
    }

    @Test
    @Timeout(10)
    void fullResponseSurvivesRingBufferReuse() throws Exception {
        final int ringSize = 32;
        final PerformanceConfiguration configuration = PerformanceConfiguration.baseBuilder()
                .ringBufferSize(ringSize)
                .msgsInGroupLimit(4)
                .orderBookFactory(OrderBookDirectImpl::new)
                .sendL2ForEveryCmd(true)
                .build();

        try (ExchangeTestContainer container = ExchangeTestContainer.create(configuration)) {
            container.addSymbol(SYMBOLSPEC_ETH_XBT);
            container.createUserWithMoney(UID_1, CURRENECY_ETH, 100_000_000);
            container.createUserWithMoney(UID_2, CURRENECY_XBT, 100_000_000);
            final ExchangeApi api = container.getApi();
            final ApiPlaceOrder.ApiPlaceOrderBuilder ask = ApiPlaceOrder.builder()
                    .symbol(SYMBOL_EXCHANGE).uid(UID_1).action(ASK).orderType(GTC);
            assertEquals(SUCCESS, api.submitCommandAsync(ask.orderId(TestConstants.orderIdOf(101)).price(1600).size(1).build()).get(3, SECONDS));
            assertEquals(SUCCESS, api.submitCommandAsync(ask.orderId(TestConstants.orderIdOf(102)).price(1650).size(3).build()).get(3, SECONDS));

            final ApiPlaceOrder taker = ApiPlaceOrder.builder()
                    .symbol(SYMBOL_EXCHANGE).uid(UID_2).action(BID).orderType(IOC)
                    .orderId(TestConstants.orderIdOf(201)).price(1700).reservePrice(1800).size(3).userCookie(42)
                    .build();
            taker.timestamp = 123_456;
            final AtomicReference<MatcherTradeEvent> engineTrades = new AtomicReference<>();
            final AtomicReference<L2MarketData> engineBook = new AtomicReference<>();
            container.setConsumer((cmd, seq) -> {
                if (cmd.command == PLACE_ORDER && cmd.getOrderId().equals(taker.orderId)) {
                    engineTrades.set(cmd.matcherEvent);
                    engineBook.set(cmd.marketData);
                }
            });
            final OrderCommand response = api.submitCommandAsyncFullResponse(taker).get(3, SECONDS);

            final ApiNop nop = ApiNop.builder().build();
            for (int i = 0; i < ringSize; i++) {
                assertEquals(SUCCESS, api.submitCommandAsync(nop).get(3, SECONDS));
            }

            assertEquals(PLACE_ORDER, response.command);
            assertEquals(SUCCESS, response.resultCode);
            assertEquals(taker.orderId, response.getOrderId());
            assertEquals(taker.timestamp, response.timestamp);
            assertEquals(taker.userCookie, response.userCookie);
            assertEquals(taker.reservePrice, response.reserveBidPrice);

            final List<MatcherTradeEvent> trades = response.extractEvents();
            assertEquals(List.of(TestConstants.orderIdOf(101L), TestConstants.orderIdOf(102L)), trades.stream().map(MatcherTradeEvent::getMatchedOrderId).toList());
            assertEquals(List.of(1L, 2L), trades.stream().map(event -> event.size).toList());
            assertEquals(List.of(TRADE, TRADE), trades.stream().map(event -> event.eventType).toList());
            assertNotSame(engineTrades.get(), trades.get(0));
            assertNotSame(engineTrades.get().nextEvent, trades.get(1));

            assertNotSame(engineBook.get(), response.marketData);
            assertNotSame(engineBook.get().askPrices, response.marketData.askPrices);
            assertArrayEquals(new long[]{1650}, response.marketData.getAskPricesCopy());
            assertArrayEquals(new long[]{1}, response.marketData.getAskVolumesCopy());
        }
    }
}
