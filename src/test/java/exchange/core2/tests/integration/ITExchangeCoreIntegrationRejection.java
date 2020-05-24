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
import exchange.core2.core.common.config.InitialStateConfiguration;
import exchange.core2.core.common.config.PerformanceConfiguration;
import exchange.core2.core.common.config.SerializationConfiguration;
import exchange.core2.tests.util.ExchangeTestContainer;
import junit.framework.TestCase;
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
import static exchange.core2.core.common.OrderType.*;
import static exchange.core2.tests.integration.ITExchangeCoreIntegrationRejection.RejectionCause.*;
import static exchange.core2.tests.util.TestConstants.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public abstract class ITExchangeCoreIntegrationRejection {

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
        testMultiBuy(SYMBOLSPECFEE_USD_JPY, GTC, NO_REJECTION);
    }

    @Test(timeout = 5_000)
    public void testMultiBuyNoRejectionExchangeGtc() {
        testMultiBuy(SYMBOLSPECFEE_XBT_LTC, GTC, NO_REJECTION);
    }

    @Test(timeout = 5_000)
    public void testMultiBuyNoRejectionExchangeIoc() {
        testMultiBuy(SYMBOLSPECFEE_XBT_LTC, IOC, NO_REJECTION);
    }

    @Test(timeout = 5_000)
    public void testMultiBuyNoRejectionMarginIoc() {
        testMultiBuy(SYMBOLSPECFEE_USD_JPY, IOC, NO_REJECTION);
    }

    @Test(timeout = 5_000)
    public void testMultiBuyNoRejectionExchangeFokB() {
        testMultiBuy(SYMBOLSPECFEE_XBT_LTC, FOK_BUDGET, NO_REJECTION);
    }

    @Test(timeout = 5_000)
    public void testMultiBuyNoRejectionMarginFokB() {
        testMultiBuy(SYMBOLSPECFEE_USD_JPY, FOK_BUDGET, NO_REJECTION);
    }

    // -------------------------- buy with rejection tests -----------------------------

    @Test(timeout = 5_000)
    public void testMultiBuyWithRejectionMarginGtc() {
        testMultiBuy(SYMBOLSPECFEE_USD_JPY, GTC, REJECTION_BY_SIZE);
    }

    @Test(timeout = 5_000)
    public void testMultiBuyWithRejectionExchangeGtc() {
        testMultiBuy(SYMBOLSPECFEE_XBT_LTC, GTC, REJECTION_BY_SIZE);
    }

    @Test(timeout = 5_000)
    public void testMultiBuyWithRejectionExchangeIoc() {
        testMultiBuy(SYMBOLSPECFEE_XBT_LTC, IOC, REJECTION_BY_SIZE);
    }

    @Test(timeout = 5_000)
    public void testMultiBuyWithRejectionMarginIoc() {
        testMultiBuy(SYMBOLSPECFEE_USD_JPY, IOC, REJECTION_BY_SIZE);
    }

    @Test(timeout = 5_000)
    public void testMultiBuyWithSizeRejectionExchangeFokB() {
        testMultiBuy(SYMBOLSPECFEE_XBT_LTC, FOK_BUDGET, REJECTION_BY_SIZE);
    }

    @Test(timeout = 5_000)
    public void testMultiBuyWithSizeRejectionMarginFokB() {
        testMultiBuy(SYMBOLSPECFEE_USD_JPY, FOK_BUDGET, REJECTION_BY_SIZE);
    }

    @Test(timeout = 5_000)
    public void testMultiBuyWithBudgetRejectionExchangeFokB() {
        testMultiBuy(SYMBOLSPECFEE_XBT_LTC, FOK_BUDGET, REJECTION_BY_BUDGET);
    }

    @Test(timeout = 5_000)
    public void testMultiBuyWithBudgetRejectionMarginFokB() {
        testMultiBuy(SYMBOLSPECFEE_USD_JPY, FOK_BUDGET, REJECTION_BY_BUDGET);
    }

    // -------------------------- sell no rejection tests -----------------------------

    @Test(timeout = 5_000)
    public void testMultiSellNoRejectionMarginGtc() {
        testMultiSell(SYMBOLSPECFEE_USD_JPY, GTC, NO_REJECTION);
    }

    @Test(timeout = 5_000)
    public void testMultiSellNoRejectionExchangeGtc() {
        testMultiSell(SYMBOLSPECFEE_XBT_LTC, GTC, NO_REJECTION);
    }

    @Test(timeout = 5_000)
    public void testMultiSellNoRejectionMarginIoc() {
        testMultiSell(SYMBOLSPECFEE_USD_JPY, IOC, NO_REJECTION);
    }

    @Test(timeout = 5_000)
    public void testMultiSellNoRejectionExchangeIoc() {
        testMultiSell(SYMBOLSPECFEE_XBT_LTC, IOC, NO_REJECTION);
    }

    @Test(timeout = 5_000)
    public void testMultiSellNoRejectionMarginFokB() {
        testMultiSell(SYMBOLSPECFEE_USD_JPY, FOK_BUDGET, NO_REJECTION);
    }

    @Test(timeout = 5_000)
    public void testMultiSellNoRejectionExchangeFokB() {
        testMultiSell(SYMBOLSPECFEE_XBT_LTC, FOK_BUDGET, NO_REJECTION);
    }

    // -------------------------- sell with rejection tests -----------------------------

    @Test(timeout = 5_000)
    public void testMultiSellWithRejectionMarginGtc() {
        testMultiSell(SYMBOLSPECFEE_USD_JPY, GTC, REJECTION_BY_SIZE);
    }

    @Test(timeout = 5_000)
    public void testMultiSellWithRejectionExchangeGtc() {
        testMultiSell(SYMBOLSPECFEE_XBT_LTC, GTC, REJECTION_BY_SIZE);
    }

    @Test(timeout = 5_000)
    public void testMultiSellWithRejectionMarginIoc() {
        testMultiSell(SYMBOLSPECFEE_USD_JPY, IOC, REJECTION_BY_SIZE);
    }

    @Test(timeout = 5_000)
    public void testMultiSellWithRejectionExchangeIoc() {
        testMultiSell(SYMBOLSPECFEE_XBT_LTC, IOC, REJECTION_BY_SIZE);
    }

    @Test(timeout = 5_000)
    public void testMultiSellWithSizeRejectionMarginFokB() {
        testMultiSell(SYMBOLSPECFEE_USD_JPY, FOK_BUDGET, REJECTION_BY_SIZE);
    }

    @Test(timeout = 5_000)
    public void testMultiSellWithSizeRejectionExchangeFokB() {
        testMultiSell(SYMBOLSPECFEE_XBT_LTC, FOK_BUDGET, REJECTION_BY_SIZE);
    }

    @Test(timeout = 5_000)
    public void testMultiSellWithExpectationRejectionMarginFokB() {
        testMultiSell(SYMBOLSPECFEE_USD_JPY, FOK_BUDGET, REJECTION_BY_BUDGET);
    }

    @Test(timeout = 5_000)
    public void testMultiSellWithExpectationRejectionExchangeFokB() {
        testMultiSell(SYMBOLSPECFEE_XBT_LTC, FOK_BUDGET, REJECTION_BY_BUDGET);
    }

    public abstract PerformanceConfiguration getPerfCfg();

    // ------------------------------------------------------------------------------

    private ApiPlaceOrder.ApiPlaceOrderBuilder builderPlace(int symbolId, long uid, OrderAction action, OrderType type) {
        return ApiPlaceOrder.builder().uid(uid).action(action).orderType(type).symbol(symbolId);
    }

    // TODO count/verify number of commands and events
    private void testMultiBuy(final CoreSymbolSpecification symbolSpec, final OrderType orderType, final RejectionCause rejectionCause) {

        final int symbolId = symbolSpec.symbolId;

        final long size = 40L + (rejectionCause == REJECTION_BY_SIZE ? 1 : 0);

        try (final ExchangeTestContainer container = new ExchangeTestContainer(getPerfCfg(), InitialStateConfiguration.CLEAN_TEST, SerializationConfiguration.DEFAULT)) {
            container.initFeeSymbols();
            container.initFeeUsers();

            container.setConsumer(processor);

            container.submitCommandSync(builderPlace(symbolId, UID_1, ASK, GTC).orderId(101L).price(160000L).size(7L).build(), CommandResultCode.SUCCESS);
            container.submitCommandSync(builderPlace(symbolId, UID_2, ASK, GTC).orderId(202L).price(159900L).size(10L).build(), CommandResultCode.SUCCESS);
            container.submitCommandSync(builderPlace(symbolId, UID_3, ASK, GTC).orderId(303L).price(160000L).size(3L).build(), CommandResultCode.SUCCESS);
            container.submitCommandSync(builderPlace(symbolId, UID_3, ASK, GTC).orderId(304L).price(160500L).size(20L).build(), CommandResultCode.SUCCESS);


            long price = 160500L;
            if (orderType == FOK_BUDGET) {
                price = 160000L * 7L + 159900L * 10L + 160000L * 3L + 160500L * 20L + (rejectionCause == REJECTION_BY_BUDGET ? -1 : 0);
            }

            container.submitCommandSync(builderPlace(symbolId, UID_4, BID, orderType).orderId(405L).price(price).reservePrice(price).size(size).build(), CommandResultCode.SUCCESS);

            TestCase.assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());
        }

        verify(handler, times(5)).commandResult(commandResultCaptor.capture());
        verify(handler, never()).reduceEvent(any());

        if (orderType == FOK_BUDGET && rejectionCause != NO_REJECTION) {
            // no trades for FoK
            verify(handler, never()).tradeEvent(any());

        } else {
            verify(handler, times(1)).tradeEvent(tradeEventCaptor.capture());

            // validating first event
            final IEventsHandler.TradeEvent tradeEvent = tradeEventCaptor.getAllValues().get(0);
            assertThat(tradeEvent.getSymbol(), Is.is(symbolId));
            assertThat(tradeEvent.getTotalVolume(), Is.is(40L));
            assertThat(tradeEvent.getTakerOrderId(), Is.is(405L));
            assertThat(tradeEvent.getTakerUid(), Is.is(UID_4));
            assertThat(tradeEvent.getTakerAction(), Is.is(OrderAction.BID));
            assertThat(tradeEvent.isTakeOrderCompleted(), Is.is(rejectionCause == NO_REJECTION)); // completed only if no rejection was happened

            final List<IEventsHandler.Trade> trades = tradeEvent.getTrades();
            assertThat(trades.size(), Is.is(4));

            assertThat(trades.get(0).getMakerOrderId(), Is.is(202L));
            assertThat(trades.get(0).getMakerUid(), Is.is(UID_2));
            assertTrue(trades.get(0).isMakerOrderCompleted());
            assertThat(trades.get(0).getPrice(), Is.is(159900L));
            assertThat(trades.get(0).getVolume(), Is.is(10L));

            assertThat(trades.get(1).getMakerOrderId(), Is.is(101L));
            assertThat(trades.get(1).getMakerUid(), Is.is(UID_1));
            assertTrue(trades.get(1).isMakerOrderCompleted());
            assertThat(trades.get(1).getPrice(), Is.is(160000L));
            assertThat(trades.get(1).getVolume(), Is.is(7L));

            assertThat(trades.get(2).getMakerOrderId(), Is.is(303L));
            assertThat(trades.get(2).getMakerUid(), Is.is(UID_3));
            assertTrue(trades.get(2).isMakerOrderCompleted());
            assertThat(trades.get(2).getPrice(), Is.is(160000L));
            assertThat(trades.get(2).getVolume(), Is.is(3L));

            assertThat(trades.get(3).getMakerOrderId(), Is.is(304L));
            assertThat(trades.get(3).getMakerUid(), Is.is(UID_3));
            assertTrue(trades.get(3).isMakerOrderCompleted());
            assertThat(trades.get(3).getPrice(), Is.is(160500L));
            assertThat(trades.get(3).getVolume(), Is.is(20L));
        }

        if (rejectionCause != NO_REJECTION && orderType != GTC) { // rejection can not happen for GTC orders
            verify(handler, times(1)).rejectEvent(rejectEventCaptor.capture());
            final IEventsHandler.RejectEvent rejectEvent = rejectEventCaptor.getValue();
            assertThat(rejectEvent.getSymbol(), Is.is(symbolId));
            assertThat(rejectEvent.getRejectedVolume(), Is.is((orderType == FOK_BUDGET) ? size : 1L));
            assertThat(rejectEvent.getOrderId(), Is.is(405L));
            assertThat(rejectEvent.getUid(), Is.is(UID_4));
        } else {
            verify(handler, never()).rejectEvent(any());
        }

    }

    private void testMultiSell(final CoreSymbolSpecification symbolSpec, final OrderType orderType, final RejectionCause rejectionCause) {

        final int symbolId = symbolSpec.symbolId;

        final long size = 22L + (rejectionCause == REJECTION_BY_SIZE ? 1 : 0);

        try (final ExchangeTestContainer container = new ExchangeTestContainer()) {
            container.initFeeSymbols();
            container.initFeeUsers();

            container.setConsumer(processor);

            long price = 159_900L;
            if (orderType == FOK_BUDGET) {
                price = 160_500L + 160_000L * 20L + 159_900L + (rejectionCause == REJECTION_BY_BUDGET ? 1 : 0);
            }

            container.submitCommandSync(builderPlace(symbolId, UID_1, BID, GTC).orderId(101L).price(160_000L).reservePrice(166_000L).size(12L).build(), CommandResultCode.SUCCESS);
            container.submitCommandSync(builderPlace(symbolId, UID_2, BID, GTC).orderId(202L).price(159_900L).reservePrice(166_000L).size(1L).build(), CommandResultCode.SUCCESS);
            container.submitCommandSync(builderPlace(symbolId, UID_3, BID, GTC).orderId(303L).price(160_000L).reservePrice(166_000L).size(8L).build(), CommandResultCode.SUCCESS);
            container.submitCommandSync(builderPlace(symbolId, UID_3, BID, GTC).orderId(304L).price(160_500L).reservePrice(166_000L).size(1L).build(), CommandResultCode.SUCCESS);

            container.submitCommandSync(builderPlace(symbolId, UID_4, ASK, orderType).orderId(405L).price(price).size(size).build(), CommandResultCode.SUCCESS);

            TestCase.assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());
        }

        verify(handler, times(5)).commandResult(commandResultCaptor.capture());
        verify(handler, never()).reduceEvent(any());

        if (orderType == FOK_BUDGET && rejectionCause != NO_REJECTION) {
            // no trades for FoK
            verify(handler, never()).tradeEvent(any());

        } else {
            verify(handler, times(1)).tradeEvent(tradeEventCaptor.capture());

            // validating first event
            final IEventsHandler.TradeEvent tradeEvent = tradeEventCaptor.getAllValues().get(0);
            assertThat(tradeEvent.getSymbol(), Is.is(symbolId));
            assertThat(tradeEvent.getTotalVolume(), Is.is(22L));
            assertThat(tradeEvent.getTakerOrderId(), Is.is(405L));
            assertThat(tradeEvent.getTakerUid(), Is.is(UID_4));
            assertThat(tradeEvent.getTakerAction(), Is.is(ASK));
            assertThat(tradeEvent.isTakeOrderCompleted(), Is.is(rejectionCause == NO_REJECTION)); // completed only if no rejection was happened

            final List<IEventsHandler.Trade> trades = tradeEvent.getTrades();
            assertThat(trades.size(), Is.is(4));

            assertThat(trades.get(0).getMakerOrderId(), Is.is(304L));
            assertThat(trades.get(0).getMakerUid(), Is.is(UID_3));
            assertTrue(trades.get(0).isMakerOrderCompleted());
            assertThat(trades.get(0).getPrice(), Is.is(160500L));
            assertThat(trades.get(0).getVolume(), Is.is(1L));

            assertThat(trades.get(1).getMakerOrderId(), Is.is(101L));
            assertThat(trades.get(1).getMakerUid(), Is.is(UID_1));
            assertTrue(trades.get(1).isMakerOrderCompleted());
            assertThat(trades.get(1).getPrice(), Is.is(160000L));
            assertThat(trades.get(1).getVolume(), Is.is(12L));

            assertThat(trades.get(2).getMakerOrderId(), Is.is(303L));
            assertThat(trades.get(2).getMakerUid(), Is.is(UID_3));
            assertTrue(trades.get(2).isMakerOrderCompleted());
            assertThat(trades.get(2).getPrice(), Is.is(160000L));
            assertThat(trades.get(2).getVolume(), Is.is(8L));

            assertThat(trades.get(3).getMakerOrderId(), Is.is(202L));
            assertThat(trades.get(3).getMakerUid(), Is.is(UID_2));
            assertTrue(trades.get(3).isMakerOrderCompleted());
            assertThat(trades.get(3).getPrice(), Is.is(159900L));
            assertThat(trades.get(3).getVolume(), Is.is(1L));
        }

        if (rejectionCause != NO_REJECTION && orderType != GTC) { // rejection can not happen for GTC orders
            verify(handler, times(1)).rejectEvent(rejectEventCaptor.capture());
            final IEventsHandler.RejectEvent rejectEvent = rejectEventCaptor.getValue();
            assertThat(rejectEvent.getSymbol(), Is.is(symbolId));
            assertThat(rejectEvent.getRejectedVolume(), Is.is((orderType == FOK_BUDGET) ? size : 1L));
            assertThat(rejectEvent.getOrderId(), Is.is(405L));
            assertThat(rejectEvent.getUid(), Is.is(UID_4));
        } else {
            verify(handler, never()).rejectEvent(any());
        }

    }

    enum RejectionCause {
        NO_REJECTION,
        REJECTION_BY_SIZE,
        REJECTION_BY_BUDGET
    }

}