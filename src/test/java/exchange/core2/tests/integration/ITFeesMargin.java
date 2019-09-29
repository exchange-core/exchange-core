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

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.PositionDirection;
import exchange.core2.core.common.api.ApiCancelOrder;
import exchange.core2.core.common.api.ApiPlaceOrder;
import exchange.core2.core.common.api.reports.TotalCurrencyBalanceReportResult;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.tests.util.ExchangeTestContainer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static exchange.core2.core.common.OrderType.GTC;
import static exchange.core2.tests.util.TestConstants.*;

/**
 * TODO more place scenarios, IOC reject tests, GTC move matching, Cancel tests
 */

@Slf4j
public final class ITFeesMargin {

    private final long makerFee = SYMBOLSPECFEE_USD_JPY.makerFee;
    private final long takerFee = SYMBOLSPECFEE_USD_JPY.takerFee;
    private final int symbolId = SYMBOLSPECFEE_USD_JPY.symbolId;

    @Test(timeout = 10_000)
    public void shouldProcessFees_AskGtcMakerPartial_BidIocTaker() throws Exception {

        try (final ExchangeTestContainer container = new ExchangeTestContainer()) {
            container.addSymbol(SYMBOLSPECFEE_USD_JPY);

            final long jpyAmount1 = 240_000L;
            container.createUserWithMoney(UID_1, CURRENECY_JPY, jpyAmount1);

            final ApiPlaceOrder order101 = ApiPlaceOrder.builder()
                    .uid(UID_1)
                    .id(101L)
                    .price(10770L)
                    .reservePrice(0L)
                    .size(40L)
                    .action(OrderAction.ASK)
                    .orderType(GTC)
                    .symbol(symbolId)
                    .build();

            container.submitCommandSync(order101, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            // verify order placed
            container.validateUserState(
                    UID_1,
                    userProfile -> assertThat(userProfile.accounts.get(CURRENECY_XBT), is(0L)),
                    orders -> assertThat(orders.get(101L).price, is(order101.price)));

            // create second user
            final long jpyAmount2 = 150_000L;
            container.createUserWithMoney(UID_2, CURRENECY_JPY, jpyAmount2);

            TotalCurrencyBalanceReportResult totalBal1 = container.totalBalanceReport();
            assertThat(totalBal1.getSum().get(CURRENECY_USD), is(0L));
            assertThat(totalBal1.getSum().get(CURRENECY_JPY), is(jpyAmount1 + jpyAmount2));
            assertThat(totalBal1.getFees().get(CURRENECY_USD), is(0L));
            assertThat(totalBal1.getFees().get(CURRENECY_JPY), is(0L));
            assertThat(totalBal1.getOpenInterestLong().get(symbolId), is(0L));

            final ApiPlaceOrder order102 = ApiPlaceOrder.builder()
                    .uid(UID_2)
                    .id(102)
                    .price(10770L)
                    .reservePrice(10770L)
                    .size(30L)
                    .action(OrderAction.BID)
                    .orderType(OrderType.IOC)
                    .symbol(symbolId)
                    .build();

            container.submitCommandSync(order102, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            // verify seller maker balance
            container.validateUserState(
                    UID_1,
                    userProfile -> {
                        assertThat(userProfile.accounts.get(CURRENECY_JPY), is(240_000L - makerFee * 30));
                        assertThat(userProfile.accounts.get(CURRENECY_USD), is(0L));
                        assertThat(userProfile.positions.get(symbolId).direction, is(PositionDirection.SHORT));
                        assertThat(userProfile.positions.get(symbolId).openVolume, is(30L));
                        assertThat(userProfile.positions.get(symbolId).pendingBuySize, is(0L));
                        assertThat(userProfile.positions.get(symbolId).pendingSellSize, is(10L));
                    },
                    orders -> assertFalse(orders.isEmpty()));

            // verify buyer taker balance
            container.validateUserState(
                    UID_2,
                    userProfile -> {
                        assertThat(userProfile.accounts.get(CURRENECY_JPY), is(150_000L - takerFee * 30));
                        assertThat(userProfile.accounts.get(CURRENECY_USD), is(0L));
                        assertThat(userProfile.positions.get(symbolId).direction, is(PositionDirection.LONG));
                        assertThat(userProfile.positions.get(symbolId).openVolume, is(30L));
                        assertThat(userProfile.positions.get(symbolId).pendingBuySize, is(0L));
                        assertThat(userProfile.positions.get(symbolId).pendingSellSize, is(0L));
                    },
                    orders -> assertTrue(orders.isEmpty()));

            // total balance remains the same
            final TotalCurrencyBalanceReportResult totalBal2 = container.totalBalanceReport();
            assertThat(totalBal2.getSum().get(CURRENECY_USD), is(0L));
            assertThat(totalBal2.getSum().get(CURRENECY_JPY), is(jpyAmount1 + jpyAmount2));
            assertThat(totalBal2.getFees().get(CURRENECY_USD), is(0L));
            assertThat(totalBal2.getFees().get(CURRENECY_JPY), is((makerFee + takerFee) * 30));
            assertThat(totalBal2.getOpenInterestLong().get(symbolId), is(30L));
        }
    }

    @Test(timeout = 10_000)
    public void shouldProcessFees_BidGtcMakerPartial_AskIocTaker() throws Exception {

        try (final ExchangeTestContainer container = new ExchangeTestContainer()) {
            container.addSymbol(SYMBOLSPECFEE_USD_JPY);

            final long jpyAmount1 = 250_000L;
            container.createUserWithMoney(UID_1, CURRENECY_JPY, jpyAmount1);

            final ApiPlaceOrder order101 = ApiPlaceOrder.builder()
                    .uid(UID_1)
                    .id(101L)
                    .price(10770L)
                    .reservePrice(0L)
                    .size(50L)
                    .action(OrderAction.BID)
                    .orderType(GTC)
                    .symbol(symbolId)
                    .build();

            container.submitCommandSync(order101, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            // verify order placed
            container.validateUserState(
                    UID_1,
                    userProfile -> assertThat(userProfile.accounts.get(CURRENECY_XBT), is(0L)),
                    orders -> assertThat(orders.get(101L).price, is(order101.price)));

            // create second user
            final long jpyAmount2 = 200_000L;
            container.createUserWithMoney(UID_2, CURRENECY_JPY, jpyAmount2);

            TotalCurrencyBalanceReportResult totalBal1 = container.totalBalanceReport();
            assertThat(totalBal1.getSum().get(CURRENECY_USD), is(0L));
            assertThat(totalBal1.getSum().get(CURRENECY_JPY), is(jpyAmount1 + jpyAmount2));
            assertThat(totalBal1.getFees().get(CURRENECY_USD), is(0L));
            assertThat(totalBal1.getFees().get(CURRENECY_JPY), is(0L));
            assertThat(totalBal1.getOpenInterestLong().get(symbolId), is(0L));

            final ApiPlaceOrder order102 = ApiPlaceOrder.builder()
                    .uid(UID_2)
                    .id(102)
                    .price(10770L)
                    .reservePrice(10770L)
                    .size(30L)
                    .action(OrderAction.ASK)
                    .orderType(OrderType.IOC)
                    .symbol(symbolId)
                    .build();

            container.submitCommandSync(order102, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            // verify buyer maker balance
            container.validateUserState(
                    UID_1,
                    userProfile -> {
                        assertThat(userProfile.accounts.get(CURRENECY_JPY), is(250_000L - makerFee * 30));
                        assertThat(userProfile.accounts.get(CURRENECY_USD), is(0L));
                        assertThat(userProfile.positions.get(symbolId).direction, is(PositionDirection.LONG));
                        assertThat(userProfile.positions.get(symbolId).openVolume, is(30L));
                        assertThat(userProfile.positions.get(symbolId).pendingBuySize, is(20L));
                        assertThat(userProfile.positions.get(symbolId).pendingSellSize, is(0L));
                    },
                    orders -> assertFalse(orders.isEmpty()));

            // verify seller taker balance
            container.validateUserState(
                    UID_2,
                    userProfile -> {
                        assertThat(userProfile.accounts.get(CURRENECY_JPY), is(200_000L - takerFee * 30));
                        assertThat(userProfile.accounts.get(CURRENECY_USD), is(0L));
                        assertThat(userProfile.positions.get(symbolId).direction, is(PositionDirection.SHORT));
                        assertThat(userProfile.positions.get(symbolId).openVolume, is(30L));
                        assertThat(userProfile.positions.get(symbolId).pendingBuySize, is(0L));
                        assertThat(userProfile.positions.get(symbolId).pendingSellSize, is(0L));
                    },
                    orders -> assertTrue(orders.isEmpty()));

            // total balance remains the same
            final TotalCurrencyBalanceReportResult totalBal2 = container.totalBalanceReport();
            assertThat(totalBal2.getSum().get(CURRENECY_USD), is(0L));
            assertThat(totalBal2.getSum().get(CURRENECY_JPY), is(jpyAmount1 + jpyAmount2));
            assertThat(totalBal2.getFees().get(CURRENECY_USD), is(0L));
            assertThat(totalBal2.getFees().get(CURRENECY_JPY), is((makerFee + takerFee) * 30));
            assertThat(totalBal2.getOpenInterestLong().get(symbolId), is(30L));
        }
    }


    @Test(timeout = 10_000)
    public void shouldNotTakeFeesForCancelAsk() throws Exception {

        try (final ExchangeTestContainer container = new ExchangeTestContainer()) {
            container.addSymbol(SYMBOLSPECFEE_USD_JPY);

            final long jpyAmount1 = 240_000L;
            container.createUserWithMoney(UID_1, CURRENECY_JPY, jpyAmount1);

            final ApiPlaceOrder order101 = ApiPlaceOrder.builder()
                    .uid(UID_1)
                    .id(101L)
                    .price(10770L)
                    .reservePrice(0L)
                    .size(40L)
                    .action(OrderAction.ASK)
                    .orderType(GTC)
                    .symbol(symbolId)
                    .build();

            container.submitCommandSync(order101, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            // verify order placed
            container.validateUserState(
                    UID_1,
                    userProfile -> assertThat(userProfile.accounts.get(CURRENECY_XBT), is(0L)),
                    orders -> assertThat(orders.get(101L).price, is(order101.price)));


            // verify balance
            container.validateUserState(
                    UID_1,
                    userProfile -> {
                        assertThat(userProfile.accounts.get(CURRENECY_JPY), is(240_000L));
                        assertThat(userProfile.accounts.get(CURRENECY_USD), is(0L));
                        assertThat(userProfile.positions.get(symbolId).direction, is(PositionDirection.EMPTY));
                        assertThat(userProfile.positions.get(symbolId).openVolume, is(0L));
                        assertThat(userProfile.positions.get(symbolId).pendingBuySize, is(0L));
                        assertThat(userProfile.positions.get(symbolId).pendingSellSize, is(40L));
                    },
                    orders -> assertFalse(orders.isEmpty()));


            // cancel
            container.submitCommandSync(
                    ApiCancelOrder.builder().id(101L).uid(UID_1).symbol(symbolId).build(),
                    CommandResultCode.SUCCESS);

            // verify balance
            container.validateUserState(
                    UID_1,
                    userProfile -> {
                        assertThat(userProfile.accounts.get(CURRENECY_JPY), is(240_000L));
                        assertThat(userProfile.accounts.get(CURRENECY_USD), is(0L));
                        assertTrue(userProfile.positions.isEmpty());
                    },
                    orders -> assertTrue(orders.isEmpty()));


            // total balance remains the same
            final TotalCurrencyBalanceReportResult totalBal2 = container.totalBalanceReport();
            assertThat(totalBal2.getSum().get(CURRENECY_USD), is(0L));
            assertThat(totalBal2.getSum().get(CURRENECY_JPY), is(240_000L));
            assertThat(totalBal2.getFees().get(CURRENECY_USD), is(0L));
            assertThat(totalBal2.getFees().get(CURRENECY_JPY), is(0L));
            assertThat(totalBal2.getOpenInterestLong().get(symbolId), is(0L));
        }
    }


}
