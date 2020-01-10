/*
 * Copyright 2020 Maksim Zheravin
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

import exchange.core2.core.ExchangeCore;
import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.PositionDirection;
import exchange.core2.core.common.api.ApiPlaceOrder;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.tests.util.ExchangeTestContainer;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import static exchange.core2.core.common.OrderType.GTC;
import static exchange.core2.tests.util.TestConstants.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

@Slf4j
public final class ITDisableNsf {

    private final long marginMakerFee = SYMBOLSPECFEE_USD_JPY.makerFee;
    private final long marginTakerFee = SYMBOLSPECFEE_USD_JPY.takerFee;
    private final int marginSymbolId = SYMBOLSPECFEE_USD_JPY.symbolId;

    private final long exchangeStep = SYMBOLSPECFEE_XBT_LTC.quoteScaleK;
    private final long exchangeMakerFee = SYMBOLSPECFEE_XBT_LTC.makerFee;
    private final long exchangeTakerFee = SYMBOLSPECFEE_XBT_LTC.takerFee;

    // disable risk check
    private final ExchangeCore.ExchangeCoreBuilder cfgDisableNsf = ExchangeTestContainer.createBasicTestConfig()
            .riskDisableNfsReject(true);


    @Test(timeout = 10_000)
    public void shouldAcceptZeroBalance_Margin_BidIocTaker_AskGtcMaker() throws Exception {

        try (final ExchangeTestContainer container = new ExchangeTestContainer(cfgDisableNsf)) {
            container.addSymbol(SYMBOLSPECFEE_USD_JPY);

            final long jpyAmount1 = 0L; // zero balance
            container.createUserWithMoney(UID_1, CURRENECY_JPY, jpyAmount1);

            final ApiPlaceOrder order101 = ApiPlaceOrder.builder()
                    .uid(UID_1)
                    .id(101L)
                    .price(10770L)
                    .reservePrice(0L)
                    .size(40L)
                    .action(OrderAction.ASK)
                    .orderType(GTC)
                    .symbol(marginSymbolId)
                    .build();

            container.submitCommandSync(order101, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            // verify order placed
            container.validateUserState(
                    UID_1,
                    userProfile -> assertThat(userProfile.accounts.get(CURRENECY_XBT), is(0L)),
                    orders -> assertThat(orders.get(101L).price, is(order101.price)));

            // create second user
            final long jpyAmount2 = 0; // zero balance
            container.createUserWithMoney(UID_2, CURRENECY_JPY, jpyAmount2);

            // validate total balance
            assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());

            final ApiPlaceOrder order102 = ApiPlaceOrder.builder()
                    .uid(UID_2)
                    .id(102)
                    .price(10770L)
                    .reservePrice(10770L)
                    .size(30L)
                    .action(OrderAction.BID)
                    .orderType(OrderType.IOC)
                    .symbol(marginSymbolId)
                    .build();

            container.submitCommandSync(order102, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            // verify seller maker balance
            container.validateUserState(
                    UID_1,
                    userProfile -> {
                        assertThat(userProfile.accounts.get(CURRENECY_JPY), is(0L - marginMakerFee * 30));
                        assertThat(userProfile.accounts.get(CURRENECY_USD), is(0L));
                        assertThat(userProfile.positions.get(marginSymbolId).direction, is(PositionDirection.SHORT));
                        assertThat(userProfile.positions.get(marginSymbolId).openVolume, is(30L));
                        assertThat(userProfile.positions.get(marginSymbolId).pendingBuySize, is(0L));
                        assertThat(userProfile.positions.get(marginSymbolId).pendingSellSize, is(10L));
                    },
                    orders -> assertFalse(orders.isEmpty()));

            // verify buyer taker balance
            container.validateUserState(
                    UID_2,
                    userProfile -> {
                        assertThat(userProfile.accounts.get(CURRENECY_JPY), is(0L - marginTakerFee * 30));
                        assertThat(userProfile.accounts.get(CURRENECY_USD), is(0L));
                        assertThat(userProfile.positions.get(marginSymbolId).direction, is(PositionDirection.LONG));
                        assertThat(userProfile.positions.get(marginSymbolId).openVolume, is(30L));
                        assertThat(userProfile.positions.get(marginSymbolId).pendingBuySize, is(0L));
                        assertThat(userProfile.positions.get(marginSymbolId).pendingSellSize, is(0L));
                    },
                    orders -> assertTrue(orders.isEmpty()));

            // validate total balance
            assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());
        }
    }

    @Test(timeout = 10_000)
    public void shouldAcceptZeroBalance_Margin_BidGtcMakerPartial_AskIocTaker() throws Exception {

        try (final ExchangeTestContainer container = new ExchangeTestContainer(cfgDisableNsf)) {
            container.addSymbol(SYMBOLSPECFEE_USD_JPY);

            final long jpyAmount1 = 0L;
            container.createUserWithMoney(UID_1, CURRENECY_JPY, jpyAmount1);

            final ApiPlaceOrder order101 = ApiPlaceOrder.builder()
                    .uid(UID_1)
                    .id(101L)
                    .price(10770L)
                    .reservePrice(0L)
                    .size(50L)
                    .action(OrderAction.BID)
                    .orderType(GTC)
                    .symbol(marginSymbolId)
                    .build();

            container.submitCommandSync(order101, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            // verify order placed
            container.validateUserState(
                    UID_1,
                    userProfile -> assertThat(userProfile.accounts.get(CURRENECY_XBT), is(0L)),
                    orders -> assertThat(orders.get(101L).price, is(order101.price)));

            // create second user
            final long jpyAmount2 = 0L;
            container.createUserWithMoney(UID_2, CURRENECY_JPY, jpyAmount2);

            // validate total balance
            assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());


            final ApiPlaceOrder order102 = ApiPlaceOrder.builder()
                    .uid(UID_2)
                    .id(102)
                    .price(10770L)
                    .reservePrice(10770L)
                    .size(30L)
                    .action(OrderAction.ASK)
                    .orderType(OrderType.IOC)
                    .symbol(marginSymbolId)
                    .build();

            container.submitCommandSync(order102, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            // verify buyer maker balance
            container.validateUserState(
                    UID_1,
                    userProfile -> {
                        assertThat(userProfile.accounts.get(CURRENECY_JPY), is(0L - marginMakerFee * 30));
                        assertThat(userProfile.accounts.get(CURRENECY_USD), is(0L));
                        assertThat(userProfile.positions.get(marginSymbolId).direction, is(PositionDirection.LONG));
                        assertThat(userProfile.positions.get(marginSymbolId).openVolume, is(30L));
                        assertThat(userProfile.positions.get(marginSymbolId).pendingBuySize, is(20L));
                        assertThat(userProfile.positions.get(marginSymbolId).pendingSellSize, is(0L));
                    },
                    orders -> assertFalse(orders.isEmpty()));

            // verify seller taker balance
            container.validateUserState(
                    UID_2,
                    userProfile -> {
                        assertThat(userProfile.accounts.get(CURRENECY_JPY), is(0L - marginTakerFee * 30));
                        assertThat(userProfile.accounts.get(CURRENECY_USD), is(0L));
                        assertThat(userProfile.positions.get(marginSymbolId).direction, is(PositionDirection.SHORT));
                        assertThat(userProfile.positions.get(marginSymbolId).openVolume, is(30L));
                        assertThat(userProfile.positions.get(marginSymbolId).pendingBuySize, is(0L));
                        assertThat(userProfile.positions.get(marginSymbolId).pendingSellSize, is(0L));
                    },
                    orders -> assertTrue(orders.isEmpty()));

            // validate total balance
            assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());
        }
    }


    @Test(timeout = 10_000)
    public void shouldAcceptZeroBalance_Exchange_BidGtcMaker_AskIocTakerPartial() throws Exception {

        try (final ExchangeTestContainer container = new ExchangeTestContainer(cfgDisableNsf)) {
            container.initFeeSymbols();
            final long ltcAmount = 0L;
            container.createUserWithMoney(UID_1, CURRENECY_LTC, ltcAmount);

            // submit an GtC order
            final ApiPlaceOrder order101 = ApiPlaceOrder.builder()
                    .uid(UID_1)
                    .id(101L)
                    .price(11_500L)
                    .reservePrice(11_553L)
                    .size(1731L)
                    .action(OrderAction.BID)
                    .orderType(GTC)
                    .symbol(SYMBOL_EXCHANGE_FEE)
                    .build();

            container.submitCommandSync(order101, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            final long expectedFundsLtc = ltcAmount - (order101.reservePrice * exchangeStep + exchangeTakerFee) * order101.size;
            // verify order placed with correct reserve price and account balance is updated accordingly
            container.validateUserState(
                    UID_1,
                    userProfile -> assertThat(userProfile.accounts.get(CURRENECY_LTC), is(expectedFundsLtc)),
                    orders -> assertThat(orders.get(101L).price, is(order101.price)));

            // create second user
            final long btcAmount = 0L;
            container.createUserWithMoney(UID_2, CURRENECY_XBT, btcAmount);

            // validate total balance
            assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());

            // submit an IoC order - sell 2,000 lots, price 114,930K (11,493 x10,000 step)
            final ApiPlaceOrder order102 = ApiPlaceOrder.builder()
                    .uid(UID_2)
                    .id(102)
                    .price(11_493L)
                    .size(2000L)
                    .action(OrderAction.ASK)
                    .orderType(OrderType.IOC)
                    .symbol(SYMBOL_EXCHANGE_FEE)
                    .build();

            container.submitCommandSync(order102, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            // verify buyer maker balance
            container.validateUserState(
                    UID_1,
                    userProfile -> {
                        assertThat(userProfile.accounts.get(CURRENECY_LTC), is(ltcAmount - (order101.price * exchangeStep + exchangeMakerFee) * 1731L));
                        assertThat(userProfile.accounts.get(CURRENECY_XBT), is(1731L * SYMBOLSPECFEE_XBT_LTC.baseScaleK));
                    },
                    orders -> assertTrue(orders.isEmpty()));

            // verify seller taker balance
            container.validateUserState(
                    UID_2,
                    userProfile -> {
                        assertThat(userProfile.accounts.get(CURRENECY_LTC), is((order101.price * exchangeStep - exchangeTakerFee) * 1731L));
                        assertThat(userProfile.accounts.get(CURRENECY_XBT), is(btcAmount - 1731L * SYMBOLSPECFEE_XBT_LTC.baseScaleK));
                    },
                    orders -> assertTrue(orders.isEmpty()));

            // validate total balance
            assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());
        }

    }


    @Test(timeout = 10_000)
    public void shouldAcceptZeroBalance_Exchange_BidGtcMakerPartial_AskIocTaker() throws Exception {

        try (final ExchangeTestContainer container = new ExchangeTestContainer(cfgDisableNsf)) {
            container.initFeeSymbols();
            final long ltcAmount = 0L;
            container.createUserWithMoney(UID_1, CURRENECY_LTC, ltcAmount); // 200B litoshi (2,000 LTC)

            // submit an GtC order - limit BUY 1,731 lots, price 115M (11,500 x10,000 step) for each lot 1M satoshi
            final ApiPlaceOrder order101 = ApiPlaceOrder.builder()
                    .uid(UID_1)
                    .id(101L)
                    .price(11_500L)
                    .reservePrice(11_553L)
                    .size(1731L)
                    .action(OrderAction.BID)
                    .orderType(GTC)
                    .symbol(SYMBOL_EXCHANGE_FEE)
                    .build();

            container.submitCommandSync(order101, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            final long expectedFundsLtc = ltcAmount - (order101.reservePrice * exchangeStep + exchangeTakerFee) * order101.size;
            // verify order placed with correct reserve price and account balance is updated accordingly
            container.validateUserState(
                    UID_1,
                    userProfile -> assertThat(userProfile.accounts.get(CURRENECY_LTC), is(expectedFundsLtc)),
                    orders -> assertThat(orders.get(101L).price, is(order101.price)));

            // create second user
            final long btcAmount = 0L;
            container.createUserWithMoney(UID_2, CURRENECY_XBT, btcAmount);

            // validate total balance
            assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());

            // submit an IoC order - sell 1,000 lots, price 114,930K (11,493 x10,000 step)
            final ApiPlaceOrder order102 = ApiPlaceOrder.builder()
                    .uid(UID_2)
                    .id(102)
                    .price(11_493L)
                    .size(1000L)
                    .action(OrderAction.ASK)
                    .orderType(OrderType.IOC)
                    .symbol(SYMBOL_EXCHANGE_FEE)
                    .build();

            container.submitCommandSync(order102, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            // verify buyer maker balance
            container.validateUserState(
                    UID_1,
                    userProfile -> {
                        assertThat(userProfile.accounts.get(CURRENECY_LTC),
                                is(ltcAmount - (order101.price * exchangeStep + exchangeMakerFee) * 1000L - (order101.reservePrice * exchangeStep + exchangeTakerFee) * 731L));
                        assertThat(userProfile.accounts.get(CURRENECY_XBT), is(1000L * SYMBOLSPECFEE_XBT_LTC.baseScaleK));
                    },
                    orders -> assertFalse(orders.isEmpty()));

            // verify seller taker balance
            container.validateUserState(
                    UID_2,
                    userProfile -> {
                        assertThat(userProfile.accounts.get(CURRENECY_LTC), is((order101.price * exchangeStep - exchangeTakerFee) * 1000L));
                        assertThat(userProfile.accounts.get(CURRENECY_XBT), is(btcAmount - 1000L * SYMBOLSPECFEE_XBT_LTC.baseScaleK));
                    },
                    orders -> assertTrue(orders.isEmpty()));

            // validate total balance
            assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());
        }

    }


}
