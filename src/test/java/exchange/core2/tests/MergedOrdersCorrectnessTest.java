/*
 * Copyright 2019-2026 Maksim Zheravin
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
package exchange.core2.tests;

import exchange.core2.core.ExchangeApi;
import exchange.core2.core.ExchangeCore;
import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.common.CoreWaitStrategy;
import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.SymbolType;
import exchange.core2.core.common.api.ApiAddUser;
import exchange.core2.core.common.api.ApiAdjustUserBalance;
import exchange.core2.core.common.api.ApiCancelOrder;
import exchange.core2.core.common.api.ApiCommand;
import exchange.core2.core.common.api.ApiMoveOrder;
import exchange.core2.core.common.api.ApiPlaceOrder;
import exchange.core2.core.common.api.ApiReduceOrder;
import exchange.core2.core.common.api.binary.BatchAddSymbolsCommand;
import exchange.core2.core.common.api.reports.SingleUserReportQuery;
import exchange.core2.core.common.api.reports.SingleUserReportResult;
import exchange.core2.core.common.api.reports.TotalCurrencyBalanceReportQuery;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.config.ExchangeConfiguration;
import exchange.core2.core.common.config.InitialStateConfiguration;
import exchange.core2.core.common.config.OrdersProcessingConfiguration;
import exchange.core2.core.common.config.PerformanceConfiguration;
import org.junit.jupiter.api.Test;

import static exchange.core2.core.common.OrderAction.ASK;
import static exchange.core2.core.common.OrderAction.BID;
import static exchange.core2.core.common.OrderType.GTC;
import static exchange.core2.core.common.OrderType.IOC;
import static exchange.core2.core.common.cmd.CommandResultCode.RISK_NSF;
import static exchange.core2.core.common.cmd.CommandResultCode.SUCCESS;
import static exchange.core2.core.common.cmd.CommandResultCode.USER_MGMT_ACCOUNT_BALANCE_ADJUSTMENT_ALREADY_APPLIED_SAME;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import exchange.core2.tests.util.TestConstants;

import java.util.UUID;

class MergedOrdersCorrectnessTest {
    private static final int BASE = 10;
    private static final int QUOTE = 20;
    private static final int SYMBOL = 100;

    @Test
    void shouldChargeEachMakerAndTakerOnlyOnItsOwnFilledLots() throws Exception {
        for (OrderAction side : OrderAction.values()) {
            final ExchangeCore core = start(PerformanceConfiguration.baseBuilder().riskEnginesNum(2).build(), false);
            try {
                final ExchangeApi api = core.getApi();
                addSymbol(api, SymbolType.CURRENCY_EXCHANGE_PAIR);
                final boolean takerBuys = side == BID;
                addUser(api, 1, takerBuys ? QUOTE : BASE, takerBuys ? 1000 : 10);
                addUser(api, 2, takerBuys ? BASE : QUOTE, takerBuys ? 10 : 1000);
                addUser(api, 3, takerBuys ? BASE : QUOTE, takerBuys ? 10 : 1000);

                submit(api, order(11, 2, side.opposite(), GTC, takerBuys ? 100 : 101, 2, 2, takerBuys ? 3 : 7), SUCCESS);
                submit(api, order(12, 3, side.opposite(), GTC, takerBuys ? 101 : 100, 3, 1, takerBuys ? 7 : 3), SUCCESS);
                submit(api, order(13, 1, side, IOC, takerBuys ? 101 : 100, 5, 11, -1), SUCCESS);

                assertEquals(takerBuys ? 442 : 447, balance(api, 1, QUOTE));
                assertEquals(5L, balance(api, 1, BASE));
                assertEquals(takerBuys ? 194 : 784, balance(api, 2, QUOTE));
                assertEquals(takerBuys ? 282 : 691, balance(api, 3, QUOTE));

                final var total = api.processReport(new TotalCurrencyBalanceReportQuery(), 0).get(5, SECONDS);
                assertEquals(takerBuys ? 82 : 78, total.getFees().get(QUOTE));
                assertTrue(total.isGlobalBalancesAllZero());
            } finally {
                core.shutdown(5, SECONDS);
            }
        }
    }

    @Test
    void shouldReturnOriginalBidReserveAfterReduceMoveStpAndCancel() throws Exception {
        final var direct = PerformanceConfiguration.latencyPerformanceBuilder()
                .threadFactory(Thread::new).waitStrategy(CoreWaitStrategy.BLOCKING).build();
        for (PerformanceConfiguration performance : new PerformanceConfiguration[]{
                PerformanceConfiguration.baseBuilder().build(), direct}) {
            final ExchangeCore core = start(performance, true);
            try {
                final ExchangeApi api = core.getApi();
                addSymbol(api, SymbolType.CURRENCY_EXCHANGE_PAIR);
                addUser(api, 1, QUOTE, 2000);
                submit(api, new ApiAdjustUserBalance(TestConstants.uidOf(1), BASE, 2, 2), SUCCESS);
                submit(api, order(11, 1, ASK, GTC, 105, 1, 0, 0), SUCCESS);
                submit(api, ApiPlaceOrder.builder().symbol(SYMBOL).orderId(TestConstants.orderIdOf(12)).uid(TestConstants.uidOf(1))
                        .action(BID).orderType(GTC).price(100).reservePrice(110).size(5)
                        .orderTakerFee(3).orderMakerFee(10).build(), SUCCESS);
                assertEquals(1400L, balance(api, 1, QUOTE));
                submit(api, ApiReduceOrder.builder().symbol(SYMBOL).uid(TestConstants.uidOf(1)).orderId(TestConstants.orderIdOf(12)).reduceSize(1).build(), SUCCESS);
                assertEquals(1520L, balance(api, 1, QUOTE));
                submit(api, ApiMoveOrder.builder().symbol(SYMBOL).uid(TestConstants.uidOf(1)).orderId(TestConstants.orderIdOf(12)).newPrice(105).build(), SUCCESS);
                assertEquals(2000L, balance(api, 1, QUOTE));
                final var orders = user(api, 1).fetchIndexedOrders();
                assertTrue(orders.containsKey(TestConstants.orderIdOf(11L)));
                assertFalse(orders.containsKey(TestConstants.orderIdOf(12L)));

                submit(api, order(13, 1, BID, GTC, 100, 2, 3, 10), SUCCESS);
                assertEquals(1780L, balance(api, 1, QUOTE));
                submit(api, ApiCancelOrder.builder().symbol(SYMBOL).uid(TestConstants.uidOf(1)).orderId(TestConstants.orderIdOf(13)).build(), SUCCESS);
                assertEquals(2000L, balance(api, 1, QUOTE));
            } finally {
                core.shutdown(5, SECONDS);
            }
        }
    }

    @Test
    void shouldKeepCollateralForPendingFuturesOrderAndRecognizeWithdrawalRetry() throws Exception {
        // Force the cancel and withdrawal to stay in one group unless a withdrawal creates a boundary.
        final ExchangeCore core = start(PerformanceConfiguration.baseBuilder()
                .maxGroupDurationNs(1_000_000_000).build(), false);
        try {
            final ExchangeApi api = core.getApi();
            addSymbol(api, SymbolType.FUTURES_CONTRACT);
            addUser(api, 1, QUOTE, 500);
            submit(api, order(11, 1, BID, GTC, 100, 3, -1, -1), SUCCESS);
            submit(api, new ApiAdjustUserBalance(TestConstants.uidOf(1), QUOTE, -201, 2), RISK_NSF);
            submit(api, new ApiAdjustUserBalance(TestConstants.uidOf(1), QUOTE, -200, 2), SUCCESS);
            submit(api, new ApiAdjustUserBalance(TestConstants.uidOf(1), QUOTE, -200, 2),
                    USER_MGMT_ACCOUNT_BALANCE_ADJUSTMENT_ALREADY_APPLIED_SAME);
            assertEquals(300L, balance(api, 1, QUOTE));
            submit(api, ApiCancelOrder.builder().symbol(SYMBOL).uid(TestConstants.uidOf(1)).orderId(TestConstants.orderIdOf(11)).build(), SUCCESS);
            submit(api, new ApiAdjustUserBalance(TestConstants.uidOf(1), QUOTE, -300, 3), SUCCESS);
            assertEquals(0L, balance(api, 1, QUOTE));
        } finally {
            core.shutdown(5, SECONDS);
        }
    }

    private static ExchangeCore start(PerformanceConfiguration performance, boolean stp) {
        final var processing = OrdersProcessingConfiguration.builder()
                .riskProcessingMode(OrdersProcessingConfiguration.RiskProcessingMode.FULL_PER_CURRENCY)
                .marginTradingMode(OrdersProcessingConfiguration.MarginTradingMode.MARGIN_TRADING_ENABLED);
        if (stp) {
            processing.selfTradeProtectionMode(OrdersProcessingConfiguration.SelfTradeProtectionMode.CANCEL_NEWEST);
        }
        final ExchangeCore core = ExchangeCore.builder()
                .resultsConsumer((cmd, seq) -> { })
                .exchangeConfiguration(ExchangeConfiguration.defaultBuilder()
                        .initStateCfg(InitialStateConfiguration.CLEAN_TEST)
                        .ordersProcessingCfg(processing.build()).performanceCfg(performance).build())
                .build();
        core.startup();
        return core;
    }

    private static void addSymbol(ExchangeApi api, SymbolType type) throws Exception {
        final var spec = CoreSymbolSpecification.builder().symbolId(SYMBOL).type(type)
                .baseCurrency(BASE).quoteCurrency(QUOTE).baseScaleK(1).quoteScaleK(1)
                .makerFee(1).takerFee(2).marginBuy(100).marginSell(100).build();
        assertEquals(SUCCESS, api.submitBinaryDataAsync(new BatchAddSymbolsCommand(spec)).get(5, SECONDS));
    }

    private static ApiPlaceOrder order(long id, long user, OrderAction side, OrderType type,
                                       long price, long size, long takerFee, long makerFee) {
        return ApiPlaceOrder.builder().orderId(TestConstants.orderIdOf(id)).uid(TestConstants.uidOf(user)).symbol(SYMBOL).action(side)
                .orderType(type).price(price).reservePrice(price).size(size)
                .orderTakerFee(takerFee).orderMakerFee(makerFee).build();
    }

    private static void addUser(ExchangeApi api, long user, int currency, long amount) throws Exception {
        submit(api, ApiAddUser.builder().uid(TestConstants.uidOf(user)).build(), SUCCESS);
        submit(api, new ApiAdjustUserBalance(TestConstants.uidOf(user), currency, amount, 1), SUCCESS);
    }

    private static void submit(ExchangeApi api, ApiCommand cmd, CommandResultCode expected) throws Exception {
        assertEquals(expected, api.submitCommandAsync(cmd).get(5, SECONDS), cmd::toString);
    }

    private static SingleUserReportResult user(ExchangeApi api, long user) throws Exception {
        return api.processReport(new SingleUserReportQuery(TestConstants.uidOf(user)), 0).get(5, SECONDS);
    }

    private static long balance(ExchangeApi api, long user, int currency) throws Exception {
        return user(api, user).getAccounts().get(currency);
    }
}
