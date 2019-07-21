package org.openpredict.exchange.tests.integration;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.openpredict.exchange.beans.OrderAction;
import org.openpredict.exchange.beans.OrderType;
import org.openpredict.exchange.beans.api.ApiCancelOrder;
import org.openpredict.exchange.beans.api.ApiPlaceOrder;
import org.openpredict.exchange.beans.api.reports.TotalCurrencyBalanceReportResult;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.tests.util.ExchangeTestContainer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.openpredict.exchange.beans.OrderType.GTC;
import static org.openpredict.exchange.tests.util.TestConstants.*;

/**
 * TODO IOC reject tests
 * TODO GTC move matching
 * TODO
 */

@Slf4j
public final class ITFeesExchange {

    private final long step = SYMBOLSPECFEE_XBT_LTC.quoteScaleK;
    private final long makerFee = SYMBOLSPECFEE_XBT_LTC.makerFee;
    private final long takerFee = SYMBOLSPECFEE_XBT_LTC.takerFee;


    @Test(timeout = 10_000)
    public void shouldRequireTakerFees_GtcCancel() throws Exception {

        try (final ExchangeTestContainer container = new ExchangeTestContainer()) {
            container.initFeeSymbols();

            // ----------------- 1 test GTC BID cancel ------------------

            // create user - 3.42B litoshi (34.2 LTC)
            final long ltcAmount = 3_420_000_000L;
            container.createUserWithMoney(UID_2, CURRENECY_LTC, ltcAmount);

            // submit BID order for 1000 lots - should be rejected because of the fee
            final ApiPlaceOrder order203 = ApiPlaceOrder.builder().uid(UID_2).id(203).price(11_400).reservePrice(11_400).size(30).action(OrderAction.BID).orderType(GTC).symbol(SYMBOL_EXCHANGE_FEE).build();
            container.submitCommandSync(order203, CommandResultCode.RISK_NSF);

            // add fee-1 - NSF
            container.addMoneyToUser(UID_2, CURRENECY_LTC, takerFee * 30 - 1);
            container.submitCommandSync(order203, CommandResultCode.RISK_NSF);

            // add 1 extra - SUCCESS
            container.addMoneyToUser(UID_2, CURRENECY_LTC, 1);
            container.submitCommandSync(order203, CommandResultCode.SUCCESS);

            // cancel bid
            container.submitCommandSync(
                    ApiCancelOrder.builder().id(203).uid(UID_2).symbol(SYMBOL_EXCHANGE_FEE).build(),
                    CommandResultCode.SUCCESS);

            container.validateUserState(
                    UID_2,
                    userProfile -> assertThat(userProfile.accounts.get(CURRENECY_LTC), is(ltcAmount + takerFee * 30)),
                    orders -> assertTrue(orders.isEmpty()));

            TotalCurrencyBalanceReportResult totalBal1 = container.totalBalanceReport();
            assertThat(totalBal1.getSum().get(CURRENECY_LTC), is(ltcAmount + takerFee * 30));
            assertThat(totalBal1.getFees().get(CURRENECY_LTC), is(0L));

            // ----------------- 2 test GTC ASK cancel ------------------

            // add 100M satoshi (1 BTC)
            final long btcAmount = 100_000_000L;
            container.addMoneyToUser(UID_2, CURRENECY_XBT, btcAmount);

            // can place ASK order, no extra is fee required for lock hold
            final ApiPlaceOrder order204 = ApiPlaceOrder.builder().uid(UID_2).id(204).price(11_400).reservePrice(11_400).size(100).action(OrderAction.ASK).orderType(GTC).symbol(SYMBOL_EXCHANGE_FEE).build();
            container.submitCommandSync(order204, CommandResultCode.SUCCESS);

            // cancel ask
            container.submitCommandSync(
                    ApiCancelOrder.builder().id(204).uid(UID_2).symbol(SYMBOL_EXCHANGE_FEE).build(),
                    CommandResultCode.SUCCESS);

            container.validateUserState(
                    UID_2,
                    userProfile -> assertThat(userProfile.accounts.get(CURRENECY_XBT), is(btcAmount)),
                    orders -> assertTrue(orders.isEmpty()));

            // no fees collected
            TotalCurrencyBalanceReportResult totalBal2 = container.totalBalanceReport();
            assertThat(totalBal2.getSum().get(CURRENECY_LTC), is(ltcAmount + takerFee * 30));
            assertThat(totalBal2.getSum().get(CURRENECY_XBT), is(btcAmount));
            assertThat(totalBal2.getFees().get(CURRENECY_LTC), is(0L));
            assertThat(totalBal2.getFees().get(CURRENECY_XBT), is(0L));
        }
    }


    @Test(timeout = 10_000)
    public void shouldProcessFees_BidGtcMaker_AskIocTakerPartial() throws Exception {

        try (final ExchangeTestContainer container = new ExchangeTestContainer()) {
            container.initFeeSymbols();
            final long ltcAmount = 200_000_000_000L;
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

            final long expectedFundsLtc = ltcAmount - (order101.reservePrice * step + takerFee) * order101.size;
            // verify order placed with correct reserve price and account balance is updated accordingly
            container.validateUserState(
                    UID_1,
                    userProfile -> assertThat(userProfile.accounts.get(CURRENECY_LTC), is(expectedFundsLtc)),
                    orders -> assertThat(orders.get(101L).price, is(order101.price)));

            // create second user
            final long btcAmount = 2_000_000_000L;
            container.createUserWithMoney(UID_2, CURRENECY_XBT, btcAmount);

            // no fees collected
            TotalCurrencyBalanceReportResult totalBal1 = container.totalBalanceReport();
            assertThat(totalBal1.getSum().get(CURRENECY_LTC), is(ltcAmount));
            assertThat(totalBal1.getSum().get(CURRENECY_XBT), is(btcAmount));
            assertThat(totalBal1.getFees().get(CURRENECY_LTC), is(0L));

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
                        assertThat(userProfile.accounts.get(CURRENECY_LTC), is(ltcAmount - (order101.price * step + makerFee) * 1731L));
                        assertThat(userProfile.accounts.get(CURRENECY_XBT), is(1731L * SYMBOLSPECFEE_XBT_LTC.baseScaleK));
                    },
                    orders -> assertTrue(orders.isEmpty()));

            // verify seller taker balance
            container.validateUserState(
                    UID_2,
                    userProfile -> {
                        assertThat(userProfile.accounts.get(CURRENECY_LTC), is((order101.price * step - takerFee) * 1731L));
                        assertThat(userProfile.accounts.get(CURRENECY_XBT), is(btcAmount - 1731L * SYMBOLSPECFEE_XBT_LTC.baseScaleK));
                    },
                    orders -> assertTrue(orders.isEmpty()));

            // total balance remains the same
            final TotalCurrencyBalanceReportResult totalBal2 = container.totalBalanceReport();
            assertThat(totalBal2.getSum().get(CURRENECY_LTC), is(ltcAmount));
            assertThat(totalBal2.getSum().get(CURRENECY_XBT), is(btcAmount));
            assertThat(totalBal2.getFees().get(CURRENECY_LTC), is((makerFee + takerFee) * 1731L));
        }

    }


    @Test(timeout = 10_000)
    public void shouldProcessFees_BidGtcMakerPartial_AskIocTaker() throws Exception {

        try (final ExchangeTestContainer container = new ExchangeTestContainer()) {
            container.initFeeSymbols();
            final long ltcAmount = 200_000_000_000L;
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

            final long expectedFundsLtc = ltcAmount - (order101.reservePrice * step + takerFee) * order101.size;
            // verify order placed with correct reserve price and account balance is updated accordingly
            container.validateUserState(
                    UID_1,
                    userProfile -> assertThat(userProfile.accounts.get(CURRENECY_LTC), is(expectedFundsLtc)),
                    orders -> assertThat(orders.get(101L).price, is(order101.price)));

            // create second user
            final long btcAmount = 2_000_000_000L;
            container.createUserWithMoney(UID_2, CURRENECY_XBT, btcAmount);

            // no fees collected
            TotalCurrencyBalanceReportResult totalBal1 = container.totalBalanceReport();
            assertThat(totalBal1.getSum().get(CURRENECY_LTC), is(ltcAmount));
            assertThat(totalBal1.getSum().get(CURRENECY_XBT), is(btcAmount));
            assertThat(totalBal1.getFees().get(CURRENECY_LTC), is(0L));

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
                                is(ltcAmount - (order101.price * step + makerFee) * 1000L - (order101.reservePrice * step + takerFee) * 731L));
                        assertThat(userProfile.accounts.get(CURRENECY_XBT), is(1000L * SYMBOLSPECFEE_XBT_LTC.baseScaleK));
                    },
                    orders -> assertFalse(orders.isEmpty()));

            // verify seller taker balance
            container.validateUserState(
                    UID_2,
                    userProfile -> {
                        assertThat(userProfile.accounts.get(CURRENECY_LTC), is((order101.price * step - takerFee) * 1000L));
                        assertThat(userProfile.accounts.get(CURRENECY_XBT), is(btcAmount - 1000L * SYMBOLSPECFEE_XBT_LTC.baseScaleK));
                    },
                    orders -> assertTrue(orders.isEmpty()));

            // total balance remains the same
            final TotalCurrencyBalanceReportResult totalBal2 = container.totalBalanceReport();
            assertThat(totalBal2.getSum().get(CURRENECY_LTC), is(ltcAmount));
            assertThat(totalBal2.getSum().get(CURRENECY_XBT), is(btcAmount));
            assertThat(totalBal2.getFees().get(CURRENECY_LTC), is((makerFee + takerFee) * 1000L));
        }

    }

    @Test(timeout = 10_000)
    public void shouldProcessFees_AskGtcMaker_BidIocTakerPartial() throws Exception {

        try (final ExchangeTestContainer container = new ExchangeTestContainer()) {
            container.initFeeSymbols();

            final long btcAmount = 2_000_000_000L;
            container.createUserWithMoney(UID_1, CURRENECY_XBT, btcAmount);

            // submit an ASK GtC order, no fees, sell 2,000 lots, price 115,000K (11,500 x10,000 step)
            final ApiPlaceOrder order101 = ApiPlaceOrder.builder()
                    .uid(UID_1)
                    .id(101L)
                    .price(11_500L)
                    .reservePrice(11_500L)
                    .size(2000L)
                    .action(OrderAction.ASK)
                    .orderType(GTC)
                    .symbol(SYMBOL_EXCHANGE_FEE)
                    .build();

            container.submitCommandSync(order101, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            // verify order placed
            container.validateUserState(
                    UID_1,
                    userProfile -> assertThat(userProfile.accounts.get(CURRENECY_XBT), is(0L)),
                    orders -> assertThat(orders.get(101L).price, is(order101.price)));

            // create second user
            final long ltcAmount = 260_000_000_000L;// 260B litoshi (2,600 LTC)
            container.createUserWithMoney(UID_2, CURRENECY_LTC, ltcAmount);

            TotalCurrencyBalanceReportResult totalBal1 = container.totalBalanceReport();
            assertThat(totalBal1.getSum().get(CURRENECY_LTC), is(ltcAmount));
            assertThat(totalBal1.getSum().get(CURRENECY_XBT), is(btcAmount));
            assertThat(totalBal1.getFees().get(CURRENECY_LTC), is(0L));

            // submit an IoC order - ASK 2,197 lots, price 115,210K (11,521 x10,000 step) for each lot 1M satoshi
            final ApiPlaceOrder order102 = ApiPlaceOrder.builder()
                    .uid(UID_2)
                    .id(102)
                    .price(11_521L)
                    .reservePrice(11_659L)
                    .size(2197L)
                    .action(OrderAction.BID)
                    .orderType(OrderType.IOC)
                    .symbol(SYMBOL_EXCHANGE_FEE)
                    .build();

            container.submitCommandSync(order102, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            // verify seller maker balance
            container.validateUserState(
                    UID_1,
                    userProfile -> {
                        assertThat(userProfile.accounts.get(CURRENECY_XBT), is(0L));
                        assertThat(userProfile.accounts.get(CURRENECY_LTC), is((11_500L * step - makerFee) * 2000L));
                    },
                    orders -> assertTrue(orders.isEmpty()));

            // verify buyer taker balance
            container.validateUserState(
                    UID_2,
                    userProfile -> {
                        assertThat(userProfile.accounts.get(CURRENECY_XBT), is(SYMBOLSPECFEE_XBT_LTC.baseScaleK * 2000L));
                        assertThat(userProfile.accounts.get(CURRENECY_LTC), is(ltcAmount - (11_500L * step + takerFee) * 2000L));
                    },
                    orders -> assertTrue(orders.isEmpty()));

            // total balance remains the same
            final TotalCurrencyBalanceReportResult totalBal2 = container.totalBalanceReport();
            assertThat(totalBal2.getSum().get(CURRENECY_LTC), is(ltcAmount));
            assertThat(totalBal2.getSum().get(CURRENECY_XBT), is(btcAmount));
            assertThat(totalBal2.getFees().get(CURRENECY_LTC), is((makerFee + takerFee) * 2000L));
        }

    }

    @Test(timeout = 10_000)
    public void shouldProcessFees_AskGtcMakerPartial_BidIocTaker() throws Exception {

        try (final ExchangeTestContainer container = new ExchangeTestContainer()) {
            container.initFeeSymbols();

            final long btcAmount = 2_000_000_000L;
            container.createUserWithMoney(UID_1, CURRENECY_XBT, btcAmount);

            // submit an ASK GtC order, no fees, sell 2,000 lots, price 115,000K (11,500 x10,000 step)
            final ApiPlaceOrder order101 = ApiPlaceOrder.builder()
                    .uid(UID_1)
                    .id(101L)
                    .price(11_500L)
                    .reservePrice(11_500L)
                    .size(2000L)
                    .action(OrderAction.ASK)
                    .orderType(GTC)
                    .symbol(SYMBOL_EXCHANGE_FEE)
                    .build();

            container.submitCommandSync(order101, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            // verify order placed
            container.validateUserState(
                    UID_1,
                    userProfile -> assertThat(userProfile.accounts.get(CURRENECY_XBT), is(0L)),
                    orders -> assertThat(orders.get(101L).price, is(order101.price)));

            // create second user
            final long ltcAmount = 260_000_000_000L;// 260B litoshi (2,600 LTC)
            container.createUserWithMoney(UID_2, CURRENECY_LTC, ltcAmount);

            TotalCurrencyBalanceReportResult totalBal1 = container.totalBalanceReport();
            assertThat(totalBal1.getSum().get(CURRENECY_LTC), is(ltcAmount));
            assertThat(totalBal1.getSum().get(CURRENECY_XBT), is(btcAmount));
            assertThat(totalBal1.getFees().get(CURRENECY_LTC), is(0L));

            // submit an IoC order - ASK 1,997 lots, price 115,210K (11,521 x10,000 step) for each lot 1M satoshi
            final ApiPlaceOrder order102 = ApiPlaceOrder.builder()
                    .uid(UID_2)
                    .id(102)
                    .price(11_521L)
                    .reservePrice(11_659L)
                    .size(1997L)
                    .action(OrderAction.BID)
                    .orderType(OrderType.IOC)
                    .symbol(SYMBOL_EXCHANGE_FEE)
                    .build();

            container.submitCommandSync(order102, cmd -> assertThat(cmd.resultCode, is(CommandResultCode.SUCCESS)));

            // verify seller maker balance
            container.validateUserState(
                    UID_1,
                    userProfile -> {
                        assertThat(userProfile.accounts.get(CURRENECY_XBT), is(0L));
                        assertThat(userProfile.accounts.get(CURRENECY_LTC), is((11_500L * step - makerFee) * 1997L));
                    },
                    orders -> assertFalse(orders.isEmpty()));

            // verify buyer taker balance
            container.validateUserState(
                    UID_2,
                    userProfile -> {
                        assertThat(userProfile.accounts.get(CURRENECY_XBT), is(SYMBOLSPECFEE_XBT_LTC.baseScaleK * 1997L));
                        assertThat(userProfile.accounts.get(CURRENECY_LTC), is(ltcAmount - (11_500L * step + takerFee) * 1997L));
                    },
                    orders -> assertTrue(orders.isEmpty()));

            // total balance remains the same
            final TotalCurrencyBalanceReportResult totalBal2 = container.totalBalanceReport();
            assertThat(totalBal2.getSum().get(CURRENECY_LTC), is(ltcAmount));
            assertThat(totalBal2.getSum().get(CURRENECY_XBT), is(btcAmount));
            assertThat(totalBal2.getFees().get(CURRENECY_LTC), is((makerFee + takerFee) * 1997L));
        }
    }


}
