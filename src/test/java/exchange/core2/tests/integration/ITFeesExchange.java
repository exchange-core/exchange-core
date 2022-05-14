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

import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.api.ApiCancelOrder;
import exchange.core2.core.common.api.ApiPlaceOrder;
import exchange.core2.core.common.api.reports.TotalCurrencyBalanceReportResult;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.config.PerformanceConfiguration;
import exchange.core2.tests.util.ExchangeTestContainer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static exchange.core2.core.common.OrderType.GTC;
import static exchange.core2.tests.util.TestConstants.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TODO IOC reject tests
 * TODO GTC move matching
 * TODO
 */

@Slf4j
public abstract class ITFeesExchange {

    private final long step = SYMBOLSPECFEE_XBT_LTC.quoteScaleK;
    private final long makerFee = SYMBOLSPECFEE_XBT_LTC.makerFee;
    private final long takerFee = SYMBOLSPECFEE_XBT_LTC.takerFee;

    // configuration provided by child class
    public abstract PerformanceConfiguration getPerformanceConfiguration();


    @Test
    @Timeout(10)
    public void shouldRequireTakerFees_GtcCancel() throws Exception {

        try (final ExchangeTestContainer container = ExchangeTestContainer.create(getPerformanceConfiguration())) {
            container.initFeeSymbols();

            // ----------------- 1 test GTC BID cancel ------------------

            // create user - 3.42B litoshi (34.2 LTC)
            final long ltcAmount = 3_420_000_000L;
            container.createUserWithMoney(UID_2, CURRENECY_LTC, ltcAmount);

            // submit BID order for 1000 lots - should be rejected because of the fee
            final ApiPlaceOrder order203 = ApiPlaceOrder.builder().uid(UID_2).orderId(203).price(11_400).reservePrice(11_400).size(30).action(OrderAction.BID).orderType(GTC).symbol(SYMBOL_EXCHANGE_FEE).build();
            container.submitCommandSync(order203, CommandResultCode.RISK_NSF);

            // add fee-1 - NSF
            container.addMoneyToUser(UID_2, CURRENECY_LTC, takerFee * 30 - 1);
            container.submitCommandSync(order203, CommandResultCode.RISK_NSF);

            // add 1 extra - SUCCESS
            container.addMoneyToUser(UID_2, CURRENECY_LTC, 1);
            container.submitCommandSync(order203, CommandResultCode.SUCCESS);

            // cancel bid
            container.submitCommandSync(
                    ApiCancelOrder.builder().orderId(203).uid(UID_2).symbol(SYMBOL_EXCHANGE_FEE).build(),
                    CommandResultCode.SUCCESS);

            container.validateUserState(UID_2, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_LTC), is(ltcAmount + takerFee * 30));
                assertTrue(profile.fetchIndexedOrders().isEmpty());
            });

            TotalCurrencyBalanceReportResult totalBal1 = container.totalBalanceReport();
            assertTrue(totalBal1.isGlobalBalancesAllZero());
            assertThat(totalBal1.getClientsBalancesSum().get(CURRENECY_LTC), is(ltcAmount + takerFee * 30));
            assertThat(totalBal1.getFees().get(CURRENECY_LTC), is(0L));

            // ----------------- 2 test GTC ASK cancel ------------------

            // add 100M satoshi (1 BTC)
            final long btcAmount = 100_000_000L;
            container.addMoneyToUser(UID_2, CURRENECY_XBT, btcAmount);

            // can place ASK order, no extra is fee required for lock hold
            final ApiPlaceOrder order204 = ApiPlaceOrder.builder().uid(UID_2).orderId(204).price(11_400).reservePrice(11_400).size(100).action(OrderAction.ASK).orderType(GTC).symbol(SYMBOL_EXCHANGE_FEE).build();
            container.submitCommandSync(order204, CommandResultCode.SUCCESS);

            // cancel ask
            container.submitCommandSync(
                    ApiCancelOrder.builder().orderId(204).uid(UID_2).symbol(SYMBOL_EXCHANGE_FEE).build(),
                    CommandResultCode.SUCCESS);

            container.validateUserState(UID_2, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(btcAmount));
                assertTrue(profile.fetchIndexedOrders().isEmpty());
            });

            // no fees collected
            TotalCurrencyBalanceReportResult totalBal2 = container.totalBalanceReport();
            assertTrue(totalBal2.isGlobalBalancesAllZero());
            assertThat(totalBal2.getClientsBalancesSum().get(CURRENECY_LTC), is(ltcAmount + takerFee * 30));
            assertThat(totalBal2.getClientsBalancesSum().get(CURRENECY_XBT), is(btcAmount));
            assertThat(totalBal2.getFees().get(CURRENECY_LTC), is(0L));
            assertThat(totalBal2.getFees().get(CURRENECY_XBT), is(0L));
        }
    }


    @Test
    @Timeout(10)
    public void shouldProcessFees_BidGtcMaker_AskIocTakerPartial() throws Exception {

        try (final ExchangeTestContainer container = ExchangeTestContainer.create(getPerformanceConfiguration())) {
            container.initFeeSymbols();
            final long ltcAmount = 200_000_000_000L;
            container.createUserWithMoney(UID_1, CURRENECY_LTC, ltcAmount); // 200B litoshi (2,000 LTC)

            // submit an GtC order - limit BUY 1,731 lots, price 115M (11,500 x10,000 step) for each lot 1M satoshi
            final ApiPlaceOrder order101 = ApiPlaceOrder.builder()
                    .uid(UID_1)
                    .orderId(101L)
                    .price(11_500L)
                    .reservePrice(11_553L)
                    .size(1731L)
                    .action(OrderAction.BID)
                    .orderType(GTC)
                    .symbol(SYMBOL_EXCHANGE_FEE)
                    .build();

            container.submitCommandSync(order101, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            final long expectedFundsLtc = ltcAmount - (order101.reservePrice * step + takerFee) * order101.size;
            // verify order placed with correct reserve price and account balance is updated accordingly
            container.validateUserState(UID_1, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_LTC), is(expectedFundsLtc));
                assertThat(profile.fetchIndexedOrders().get(101L).price, is(order101.price));
            });

            // create second user
            final long btcAmount = 2_000_000_000L;
            container.createUserWithMoney(UID_2, CURRENECY_XBT, btcAmount);

            // no fees collected
            TotalCurrencyBalanceReportResult totalBal1 = container.totalBalanceReport();
            assertTrue(totalBal1.isGlobalBalancesAllZero());
            assertThat(totalBal1.getClientsBalancesSum().get(CURRENECY_LTC), is(ltcAmount));
            assertThat(totalBal1.getClientsBalancesSum().get(CURRENECY_XBT), is(btcAmount));
            assertThat(totalBal1.getFees().get(CURRENECY_LTC), is(0L));

            // submit an IoC order - sell 2,000 lots, price 114,930K (11,493 x10,000 step)
            final ApiPlaceOrder order102 = ApiPlaceOrder.builder()
                    .uid(UID_2)
                    .orderId(102)
                    .price(11_493L)
                    .size(2000L)
                    .action(OrderAction.ASK)
                    .orderType(OrderType.IOC)
                    .symbol(SYMBOL_EXCHANGE_FEE)
                    .build();

            container.submitCommandSync(order102, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            // verify buyer maker balance
            container.validateUserState(UID_1, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_LTC), is(ltcAmount - (order101.price * step + makerFee) * 1731L));
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(1731L * SYMBOLSPECFEE_XBT_LTC.baseScaleK));
                assertTrue(profile.fetchIndexedOrders().isEmpty());
            });

            // verify seller taker balance
            container.validateUserState(UID_2, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_LTC), is((order101.price * step - takerFee) * 1731L));
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(btcAmount - 1731L * SYMBOLSPECFEE_XBT_LTC.baseScaleK));
                assertTrue(profile.fetchIndexedOrders().isEmpty());
            });

            // total balance remains the same
            final TotalCurrencyBalanceReportResult totalBal2 = container.totalBalanceReport();
            final long ltcFees = (makerFee + takerFee) * 1731L;
            assertTrue(totalBal2.isGlobalBalancesAllZero());
            assertThat(totalBal2.getFees().get(CURRENECY_LTC), is(ltcFees));
            assertThat(totalBal2.getClientsBalancesSum().get(CURRENECY_LTC), is(ltcAmount - ltcFees));
            assertThat(totalBal2.getClientsBalancesSum().get(CURRENECY_XBT), is(btcAmount));
        }

    }


    @Test
    @Timeout(10)
    public void shouldProcessFees_BidGtcMakerPartial_AskIocTaker() throws Exception {

        try (final ExchangeTestContainer container = ExchangeTestContainer.create(getPerformanceConfiguration())) {
            container.initFeeSymbols();
            final long ltcAmount = 200_000_000_000L;
            container.createUserWithMoney(UID_1, CURRENECY_LTC, ltcAmount); // 200B litoshi (2,000 LTC)

            // submit an GtC order - limit BUY 1,731 lots, price 115M (11,500 x10,000 step) for each lot 1M satoshi
            final ApiPlaceOrder order101 = ApiPlaceOrder.builder()
                    .uid(UID_1)
                    .orderId(101L)
                    .price(11_500L)
                    .reservePrice(11_553L)
                    .size(1731L)
                    .action(OrderAction.BID)
                    .orderType(GTC)
                    .symbol(SYMBOL_EXCHANGE_FEE)
                    .build();

            container.submitCommandSync(order101, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            final long expectedFundsLtc = ltcAmount - (order101.reservePrice * step + takerFee) * order101.size;
            // verify order placed with correct reserve price and account balance is updated accordingly
            container.validateUserState(UID_1, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_LTC), is(expectedFundsLtc));
                assertThat(profile.fetchIndexedOrders().get(101L).price, is(order101.price));
            });

            // create second user
            final long btcAmount = 2_000_000_000L;
            container.createUserWithMoney(UID_2, CURRENECY_XBT, btcAmount);

            // no fees collected
            TotalCurrencyBalanceReportResult totalBal1 = container.totalBalanceReport();
            assertTrue(totalBal1.isGlobalBalancesAllZero());
            assertThat(totalBal1.getClientsBalancesSum().get(CURRENECY_LTC), is(ltcAmount));
            assertThat(totalBal1.getClientsBalancesSum().get(CURRENECY_XBT), is(btcAmount));
            assertThat(totalBal1.getFees().get(CURRENECY_LTC), is(0L));

            // submit an IoC order - sell 1,000 lots, price 114,930K (11,493 x10,000 step)
            final ApiPlaceOrder order102 = ApiPlaceOrder.builder()
                    .uid(UID_2)
                    .orderId(102)
                    .price(11_493L)
                    .size(1000L)
                    .action(OrderAction.ASK)
                    .orderType(OrderType.IOC)
                    .symbol(SYMBOL_EXCHANGE_FEE)
                    .build();

            container.submitCommandSync(order102, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            // verify buyer maker balance
            container.validateUserState(UID_1, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_LTC),
                        is(ltcAmount - (order101.price * step + makerFee) * 1000L - (order101.reservePrice * step + takerFee) * 731L));
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(1000L * SYMBOLSPECFEE_XBT_LTC.baseScaleK));
                assertFalse(profile.fetchIndexedOrders().isEmpty());
            });

            // verify seller taker balance
            container.validateUserState(UID_2, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_LTC), is((order101.price * step - takerFee) * 1000L));
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(btcAmount - 1000L * SYMBOLSPECFEE_XBT_LTC.baseScaleK));
                assertTrue(profile.fetchIndexedOrders().isEmpty());
            });

            // total balance remains the same
            final TotalCurrencyBalanceReportResult totalBal2 = container.totalBalanceReport();
            assertTrue(totalBal2.isGlobalBalancesAllZero());
            final long ltcFees = (makerFee + takerFee) * 1000L;
            assertThat(totalBal2.getFees().get(CURRENECY_LTC), is(ltcFees));
            assertThat(totalBal2.getClientsBalancesSum().get(CURRENECY_LTC), is(ltcAmount - ltcFees));
            assertThat(totalBal2.getClientsBalancesSum().get(CURRENECY_XBT), is(btcAmount));
        }

    }

    @Test
    @Timeout(10)
    public void shouldProcessFees_AskGtcMaker_BidIocTakerPartial() throws Exception {

        try (final ExchangeTestContainer container = ExchangeTestContainer.create(getPerformanceConfiguration())) {
            container.initFeeSymbols();

            final long btcAmount = 2_000_000_000L;
            container.createUserWithMoney(UID_1, CURRENECY_XBT, btcAmount);

            // submit an ASK GtC order, no fees, sell 2,000 lots, price 115,000K (11,500 x10,000 step)
            final ApiPlaceOrder order101 = ApiPlaceOrder.builder()
                    .uid(UID_1)
                    .orderId(101L)
                    .price(11_500L)
                    .reservePrice(11_500L)
                    .size(2000L)
                    .action(OrderAction.ASK)
                    .orderType(GTC)
                    .symbol(SYMBOL_EXCHANGE_FEE)
                    .build();

            container.submitCommandSync(order101, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            // verify order placed
            container.validateUserState(UID_1, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(0L));
                assertThat(profile.fetchIndexedOrders().get(101L).price, is(order101.price));
            });

            // create second user
            final long ltcAmount = 260_000_000_000L;// 260B litoshi (2,600 LTC)
            container.createUserWithMoney(UID_2, CURRENECY_LTC, ltcAmount);

            TotalCurrencyBalanceReportResult totalBal1 = container.totalBalanceReport();
            assertTrue(totalBal1.isGlobalBalancesAllZero());
            assertThat(totalBal1.getClientsBalancesSum().get(CURRENECY_LTC), is(ltcAmount));
            assertThat(totalBal1.getClientsBalancesSum().get(CURRENECY_XBT), is(btcAmount));
            assertThat(totalBal1.getFees().get(CURRENECY_LTC), is(0L));

            // submit an IoC order - ASK 2,197 lots, price 115,210K (11,521 x10,000 step) for each lot 1M satoshi
            final ApiPlaceOrder order102 = ApiPlaceOrder.builder()
                    .uid(UID_2)
                    .orderId(102)
                    .price(11_521L)
                    .reservePrice(11_659L)
                    .size(2197L)
                    .action(OrderAction.BID)
                    .orderType(OrderType.IOC)
                    .symbol(SYMBOL_EXCHANGE_FEE)
                    .build();

            container.submitCommandSync(order102, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            // verify seller maker balance
            container.validateUserState(UID_1, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(0L));
                assertThat(profile.getAccounts().get(CURRENECY_LTC), is((11_500L * step - makerFee) * 2000L));
                assertTrue(profile.fetchIndexedOrders().isEmpty());
            });

            // verify buyer taker balance
            container.validateUserState(UID_2, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(SYMBOLSPECFEE_XBT_LTC.baseScaleK * 2000L));
                assertThat(profile.getAccounts().get(CURRENECY_LTC), is(ltcAmount - (11_500L * step + takerFee) * 2000L));
                assertTrue(profile.fetchIndexedOrders().isEmpty());
            });

            // total balance remains the same
            final TotalCurrencyBalanceReportResult totalBal2 = container.totalBalanceReport();
            final long ltcFees = (makerFee + takerFee) * 2000L;
            assertTrue(totalBal2.isGlobalBalancesAllZero());
            assertThat(totalBal2.getFees().get(CURRENECY_LTC), is(ltcFees));
            assertThat(totalBal2.getClientsBalancesSum().get(CURRENECY_LTC), is(ltcAmount - ltcFees));
            assertThat(totalBal2.getClientsBalancesSum().get(CURRENECY_XBT), is(btcAmount));
        }

    }

    @Test
    @Timeout(10)
    public void shouldProcessFees_AskGtcMakerPartial_BidIocTaker() throws Exception {

        try (final ExchangeTestContainer container = ExchangeTestContainer.create(getPerformanceConfiguration())) {
            container.initFeeSymbols();

            final long btcAmount = 2_000_000_000L;
            container.createUserWithMoney(UID_1, CURRENECY_XBT, btcAmount);

            // submit an ASK GtC order, no fees, sell 2,000 lots, price 115,000K (11,500 x10,000 step)
            final ApiPlaceOrder order101 = ApiPlaceOrder.builder()
                    .uid(UID_1)
                    .orderId(101L)
                    .price(11_500L)
                    .reservePrice(11_500L)
                    .size(2000L)
                    .action(OrderAction.ASK)
                    .orderType(GTC)
                    .symbol(SYMBOL_EXCHANGE_FEE)
                    .build();

            container.submitCommandSync(order101, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            // verify order placed
            container.validateUserState(UID_1, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(0L));
                assertThat(profile.fetchIndexedOrders().get(101L).price, is(order101.price));
            });

            // create second user
            final long ltcAmount = 260_000_000_000L;// 260B litoshi (2,600 LTC)
            container.createUserWithMoney(UID_2, CURRENECY_LTC, ltcAmount);

            TotalCurrencyBalanceReportResult totalBal1 = container.totalBalanceReport();
            assertThat(totalBal1.getFees().get(CURRENECY_LTC), is(0L));
            assertTrue(totalBal1.isGlobalBalancesAllZero());
            assertThat(totalBal1.getClientsBalancesSum().get(CURRENECY_LTC), is(ltcAmount));
            assertThat(totalBal1.getClientsBalancesSum().get(CURRENECY_XBT), is(btcAmount));

            // submit an IoC order - ASK 1,997 lots, price 115,210K (11,521 x10,000 step) for each lot 1M satoshi
            final ApiPlaceOrder order102 = ApiPlaceOrder.builder()
                    .uid(UID_2)
                    .orderId(102)
                    .price(11_521L)
                    .reservePrice(11_659L)
                    .size(1997L)
                    .action(OrderAction.BID)
                    .orderType(OrderType.IOC)
                    .symbol(SYMBOL_EXCHANGE_FEE)
                    .build();

            container.submitCommandSync(order102, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            // verify seller maker balance
            container.validateUserState(UID_1, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(0L));
                assertThat(profile.getAccounts().get(CURRENECY_LTC), is((11_500L * step - makerFee) * 1997L));
                assertFalse(profile.fetchIndexedOrders().isEmpty());
            });

            // verify buyer taker balance
            container.validateUserState(UID_2, profile -> {
                assertThat(profile.getAccounts().get(CURRENECY_XBT), is(SYMBOLSPECFEE_XBT_LTC.baseScaleK * 1997L));
                assertThat(profile.getAccounts().get(CURRENECY_LTC), is(ltcAmount - (11_500L * step + takerFee) * 1997L));
                assertTrue(profile.fetchIndexedOrders().isEmpty());
            });

            // total balance remains the same
            final long ltcFees = (makerFee + takerFee) * 1997L;
            final TotalCurrencyBalanceReportResult totalBal2 = container.totalBalanceReport();
            assertTrue(totalBal2.isGlobalBalancesAllZero());
            assertThat(totalBal2.getClientsBalancesSum().get(CURRENECY_LTC), is(ltcAmount - ltcFees));
            assertThat(totalBal2.getClientsBalancesSum().get(CURRENECY_XBT), is(btcAmount));
            assertThat(totalBal2.getFees().get(CURRENECY_LTC), is(ltcFees));
        }
    }


}
