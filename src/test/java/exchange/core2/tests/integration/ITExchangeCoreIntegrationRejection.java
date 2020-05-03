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

import exchange.core2.core.IEventsHandler;
import exchange.core2.core.SimpleEventsProcessor;
import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.api.ApiPlaceOrder;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.tests.util.ExchangeTestContainer;
import lombok.extern.slf4j.Slf4j;
import org.hamcrest.core.Is;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;

import static exchange.core2.core.common.OrderAction.ASK;
import static exchange.core2.core.common.OrderAction.BID;
import static exchange.core2.core.common.OrderType.GTC;
import static exchange.core2.core.common.OrderType.IOC;
import static exchange.core2.tests.util.TestConstants.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public final class ITExchangeCoreIntegrationRejection {

    private SimpleEventsProcessor processor;

    @Mock
    private IEventsHandler handler;

    @Captor
    private ArgumentCaptor<IEventsHandler.ApiCommandResult> commandResultCaptor;

    @Captor
    private ArgumentCaptor<IEventsHandler.TradeEvent> tradeEventCaptor;

    @Captor
    private ArgumentCaptor<IEventsHandler.RejectEvent> rejectEventCaptor;

    @Before
    public void before() {
        processor = new SimpleEventsProcessor(handler);
    }


    // -------------------------- buy no rejection tests -----------------------------

    @Test(timeout = 5_000)
    public void testMultiBuyNoRejectionMarginGtc() {
        testMultiBuy(SYMBOLSPEC_EUR_USD, GTC, false);
    }

    @Test(timeout = 5_000)
    public void testMultiBuyNoRejectionExchangeGtc() {
        testMultiBuy(SYMBOLSPEC_ETH_XBT, GTC, false);
    }

    @Test(timeout = 5_000)
    public void testMultiBuyNoRejectionExchangeIoc() {
        testMultiBuy(SYMBOLSPEC_ETH_XBT, IOC, false);
    }

    @Test(timeout = 5_000)
    public void testMultiBuyNoRejectionMarginIoc() {
        testMultiBuy(SYMBOLSPEC_EUR_USD, IOC, false);
    }

    // -------------------------- buy with rejection tests -----------------------------

    @Test(timeout = 5_000)
    public void testMultiBuyWithRejectionMarginGtc() {
        testMultiBuy(SYMBOLSPEC_EUR_USD, GTC, true);
    }

    @Test(timeout = 5_000)
    public void testMultiBuyWithRejectionExchangeGtc() {
        testMultiBuy(SYMBOLSPEC_ETH_XBT, GTC, true);
    }

    @Test(timeout = 5_000)
    public void testMultiBuyWithRejectionExchangeIoc() {
        testMultiBuy(SYMBOLSPEC_ETH_XBT, IOC, true);
    }

    @Test(timeout = 5_000)
    public void testMultiBuyWithRejectionMarginIoc() {
        testMultiBuy(SYMBOLSPEC_EUR_USD, IOC, true);
    }

    // -------------------------- sell no rejection tests -----------------------------

    @Test(timeout = 5_000)
    public void testMultiSellNoRejectionMarginGtc() {
        testMultiSell(SYMBOLSPEC_EUR_USD, GTC, false);
    }

    @Test(timeout = 5_000)
    public void testMultiSellNoRejectionExchangeGtc() {
        testMultiSell(SYMBOLSPEC_ETH_XBT, GTC, false);
    }

    @Test(timeout = 5_000)
    public void testMultiSellNoRejectionMarginIoc() {
        testMultiSell(SYMBOLSPEC_EUR_USD, IOC, false);
    }

    @Test(timeout = 5_000)
    public void testMultiSellNoRejectionExchangeIoc() {
        testMultiSell(SYMBOLSPEC_ETH_XBT, IOC, false);
    }

    // -------------------------- sell with rejection tests -----------------------------

    @Test(timeout = 5_000)
    public void testMultiSellWithRejectionMarginGtc() {
        testMultiSell(SYMBOLSPEC_EUR_USD, GTC, true);
    }

    @Test(timeout = 5_000)
    public void testMultiSellWithRejectionExchangeGtc() {
        testMultiSell(SYMBOLSPEC_ETH_XBT, GTC, true);
    }

    @Test(timeout = 5_000)
    public void testMultiSellWithRejectionMarginIoc() {
        testMultiSell(SYMBOLSPEC_EUR_USD, IOC, true);
    }

    @Test(timeout = 5_000)
    public void testMultiSellWithRejectionExchangeIoc() {
        testMultiSell(SYMBOLSPEC_ETH_XBT, IOC, true);
    }

    private ApiPlaceOrder.ApiPlaceOrderBuilder builderPlace(int symbolId, long uid, OrderAction action, OrderType type) {
        return ApiPlaceOrder.builder().uid(uid).action(action).orderType(type).symbol(symbolId);
    }

    // TODO count/verify number of commands and events
    private void testMultiBuy(final CoreSymbolSpecification symbolSpec, final OrderType orderType, boolean causeRejection) {

        final int symbolId = symbolSpec.symbolId;

        try (final ExchangeTestContainer container = new ExchangeTestContainer()) {
            container.initBasicSymbols();
            container.initBasicUsers();

            container.setConsumer(processor);

            container.submitCommandSync(builderPlace(symbolId, UID_1, ASK, GTC).orderId(101L).price(1600L).size(7L).build(), CommandResultCode.SUCCESS);
            container.submitCommandSync(builderPlace(symbolId, UID_2, ASK, GTC).orderId(202L).price(1599L).size(10L).build(), CommandResultCode.SUCCESS);
            container.submitCommandSync(builderPlace(symbolId, UID_3, ASK, GTC).orderId(303L).price(1600L).size(3L).build(), CommandResultCode.SUCCESS);
            container.submitCommandSync(builderPlace(symbolId, UID_3, ASK, GTC).orderId(304L).price(1605L).size(20L).build(), CommandResultCode.SUCCESS);

            final long size = 40L + (causeRejection ? 1 : 0);
            container.submitCommandSync(builderPlace(symbolId, UID_4, BID, orderType).orderId(405L).price(1605L).reservePrice(1605L).size(size).build(), CommandResultCode.SUCCESS);
        }

        verify(handler, times(5)).commandResult(commandResultCaptor.capture());
        verify(handler, never()).reduceEvent(any());
        verify(handler, times(1)).tradeEvent(tradeEventCaptor.capture());

        // validating first event
        final IEventsHandler.TradeEvent tradeEvent = tradeEventCaptor.getAllValues().get(0);
        assertThat(tradeEvent.getSymbol(), Is.is(symbolId));
        assertThat(tradeEvent.getTotalVolume(), Is.is(40L));
        assertThat(tradeEvent.getTakerOrderId(), Is.is(405L));
        assertThat(tradeEvent.getTakerUid(), Is.is(UID_4));
        assertThat(tradeEvent.getTakerAction(), Is.is(OrderAction.BID));
        assertThat(tradeEvent.isTakeOrderCompleted(), Is.is(!causeRejection)); // completed only if no rejection was happened

        final List<IEventsHandler.Trade> trades = tradeEvent.getTrades();
        assertThat(trades.size(), Is.is(4));

        assertThat(trades.get(0).getMakerOrderId(), Is.is(202L));
        assertThat(trades.get(0).getMakerUid(), Is.is(UID_2));
        assertTrue(trades.get(0).isMakerOrderCompleted());
        assertThat(trades.get(0).getPrice(), Is.is(1599L));
        assertThat(trades.get(0).getVolume(), Is.is(10L));

        assertThat(trades.get(1).getMakerOrderId(), Is.is(101L));
        assertThat(trades.get(1).getMakerUid(), Is.is(UID_1));
        assertTrue(trades.get(1).isMakerOrderCompleted());
        assertThat(trades.get(1).getPrice(), Is.is(1600L));
        assertThat(trades.get(1).getVolume(), Is.is(7L));

        assertThat(trades.get(2).getMakerOrderId(), Is.is(303L));
        assertThat(trades.get(2).getMakerUid(), Is.is(UID_3));
        assertTrue(trades.get(2).isMakerOrderCompleted());
        assertThat(trades.get(2).getPrice(), Is.is(1600L));
        assertThat(trades.get(2).getVolume(), Is.is(3L));

        assertThat(trades.get(3).getMakerOrderId(), Is.is(304L));
        assertThat(trades.get(3).getMakerUid(), Is.is(UID_3));
        assertTrue(trades.get(3).isMakerOrderCompleted());
        assertThat(trades.get(3).getPrice(), Is.is(1605L));
        assertThat(trades.get(3).getVolume(), Is.is(20L));

        if (causeRejection && orderType != GTC) { // rejection can not happen for GTC orders
            verify(handler, times(1)).rejectEvent(rejectEventCaptor.capture());
            final IEventsHandler.RejectEvent rejectEvent = rejectEventCaptor.getValue();
            assertThat(rejectEvent.getSymbol(), Is.is(symbolId));
            assertThat(rejectEvent.getRejectedVolume(), Is.is(1L));
            assertThat(rejectEvent.getOrderId(), Is.is(405L));
            assertThat(rejectEvent.getUid(), Is.is(UID_4));
        } else {
            verify(handler, never()).rejectEvent(any());
        }

    }

    private void testMultiSell(final CoreSymbolSpecification symbolSpec, final OrderType orderType, boolean causeRejection) {

        final int symbolId = symbolSpec.symbolId;

        try (final ExchangeTestContainer container = new ExchangeTestContainer()) {
            container.initBasicSymbols();
            container.initBasicUsers();

            container.setConsumer(processor);

            container.submitCommandSync(builderPlace(symbolId, UID_1, BID, GTC).orderId(101L).price(1600L).reservePrice(1660L).size(12L).build(), CommandResultCode.SUCCESS);
            container.submitCommandSync(builderPlace(symbolId, UID_2, BID, GTC).orderId(202L).price(1599L).reservePrice(1660L).size(1L).build(), CommandResultCode.SUCCESS);
            container.submitCommandSync(builderPlace(symbolId, UID_3, BID, GTC).orderId(303L).price(1600L).reservePrice(1660L).size(8L).build(), CommandResultCode.SUCCESS);
            container.submitCommandSync(builderPlace(symbolId, UID_3, BID, GTC).orderId(304L).price(1605L).reservePrice(1660L).size(1L).build(), CommandResultCode.SUCCESS);

            final long size = 22L + (causeRejection ? 1 : 0);
            container.submitCommandSync(builderPlace(symbolId, UID_4, ASK, orderType).orderId(405L).price(1599L).size(size).build(), CommandResultCode.SUCCESS);
        }

        verify(handler, times(5)).commandResult(commandResultCaptor.capture());
        verify(handler, never()).reduceEvent(any());
        verify(handler, times(1)).tradeEvent(tradeEventCaptor.capture());

        // validating first event
        final IEventsHandler.TradeEvent tradeEvent = tradeEventCaptor.getAllValues().get(0);
        assertThat(tradeEvent.getSymbol(), Is.is(symbolId));
        assertThat(tradeEvent.getTotalVolume(), Is.is(22L));
        assertThat(tradeEvent.getTakerOrderId(), Is.is(405L));
        assertThat(tradeEvent.getTakerUid(), Is.is(UID_4));
        assertThat(tradeEvent.getTakerAction(), Is.is(ASK));
        assertThat(tradeEvent.isTakeOrderCompleted(), Is.is(!causeRejection)); // completed only if no rejection was happened

        final List<IEventsHandler.Trade> trades = tradeEvent.getTrades();
        assertThat(trades.size(), Is.is(4));

        assertThat(trades.get(0).getMakerOrderId(), Is.is(304L));
        assertThat(trades.get(0).getMakerUid(), Is.is(UID_3));
        assertTrue(trades.get(0).isMakerOrderCompleted());
        assertThat(trades.get(0).getPrice(), Is.is(1605L));
        assertThat(trades.get(0).getVolume(), Is.is(1L));

        assertThat(trades.get(1).getMakerOrderId(), Is.is(101L));
        assertThat(trades.get(1).getMakerUid(), Is.is(UID_1));
        assertTrue(trades.get(1).isMakerOrderCompleted());
        assertThat(trades.get(1).getPrice(), Is.is(1600L));
        assertThat(trades.get(1).getVolume(), Is.is(12L));

        assertThat(trades.get(2).getMakerOrderId(), Is.is(303L));
        assertThat(trades.get(2).getMakerUid(), Is.is(UID_3));
        assertTrue(trades.get(2).isMakerOrderCompleted());
        assertThat(trades.get(2).getPrice(), Is.is(1600L));
        assertThat(trades.get(2).getVolume(), Is.is(8L));

        assertThat(trades.get(3).getMakerOrderId(), Is.is(202L));
        assertThat(trades.get(3).getMakerUid(), Is.is(UID_2));
        assertTrue(trades.get(3).isMakerOrderCompleted());
        assertThat(trades.get(3).getPrice(), Is.is(1599L));
        assertThat(trades.get(3).getVolume(), Is.is(1L));

        if (causeRejection && orderType != GTC) { // rejection can not happen for GTC orders
            verify(handler, times(1)).rejectEvent(rejectEventCaptor.capture());
            final IEventsHandler.RejectEvent rejectEvent = rejectEventCaptor.getValue();
            assertThat(rejectEvent.getSymbol(), Is.is(symbolId));
            assertThat(rejectEvent.getRejectedVolume(), Is.is(1L));
            assertThat(rejectEvent.getOrderId(), Is.is(405L));
            assertThat(rejectEvent.getUid(), Is.is(UID_4));
        } else {
            verify(handler, never()).rejectEvent(any());
        }

    }


}