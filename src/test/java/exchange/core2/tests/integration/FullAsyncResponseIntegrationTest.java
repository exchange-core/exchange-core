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
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.config.PerformanceConfiguration;
import exchange.core2.core.orderbook.OrderBookDirectImpl;
import exchange.core2.tests.util.ExchangeTestContainer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static exchange.core2.core.common.MatcherEventType.TRADE;
import static exchange.core2.core.common.OrderAction.ASK;
import static exchange.core2.core.common.OrderAction.BID;
import static exchange.core2.core.common.OrderType.GTC;
import static exchange.core2.core.common.OrderType.IOC;
import static exchange.core2.core.common.cmd.CommandResultCode.SUCCESS;
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
import static org.junit.jupiter.api.Assertions.assertNotSame;

class FullAsyncResponseIntegrationTest {

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
            assertEquals(SUCCESS, api.submitCommandAsync(ask.orderId(101).price(1600).size(1).build()).get(3, SECONDS));
            assertEquals(SUCCESS, api.submitCommandAsync(ask.orderId(102).price(1650).size(3).build()).get(3, SECONDS));

            final ApiPlaceOrder taker = ApiPlaceOrder.builder()
                    .symbol(SYMBOL_EXCHANGE).uid(UID_2).action(BID).orderType(IOC)
                    .orderId(201).price(1700).reservePrice(1800).size(3).userCookie(42)
                    .build();
            taker.timestamp = 123_456;
            final AtomicReference<MatcherTradeEvent> engineTrades = new AtomicReference<>();
            final AtomicReference<L2MarketData> engineBook = new AtomicReference<>();
            container.setConsumer((cmd, seq) -> {
                if (cmd.command == PLACE_ORDER && cmd.orderId == taker.orderId) {
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
            assertEquals(taker.orderId, response.orderId);
            assertEquals(taker.timestamp, response.timestamp);
            assertEquals(taker.userCookie, response.userCookie);
            assertEquals(taker.reservePrice, response.reserveBidPrice);

            final List<MatcherTradeEvent> trades = response.extractEvents();
            assertEquals(List.of(101L, 102L), trades.stream().map(event -> event.matchedOrderId).toList());
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
