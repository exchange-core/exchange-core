package org.openpredict.exchange.tests;

import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.hamcrest.core.Is;
import org.junit.Test;
import org.openpredict.exchange.beans.CoreSymbolSpecification;
import org.openpredict.exchange.beans.api.ApiCommand;
import org.openpredict.exchange.beans.api.ApiPersistState;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommandType;
import org.openpredict.exchange.core.ExchangeApi;
import org.openpredict.exchange.tests.util.ExchangeTestContainer;
import org.openpredict.exchange.tests.util.TestOrdersGenerator;
import org.openpredict.exchange.tests.util.UserCurrencyAccountsGenerator;

import java.util.BitSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.openpredict.exchange.tests.util.ExchangeTestContainer.AllowedSymbolTypes.*;
import static org.openpredict.exchange.tests.util.TestConstants.ALL_CURRENCIES;
import static org.openpredict.exchange.tests.util.TestConstants.CURRENCIES_EXCHANGE;
import static org.openpredict.exchange.tests.util.TestConstants.CURRENCIES_FUTURES;

@Slf4j
public final class PerfPersistence {


    /**
     * This is serialization test for simplified conditions
     * - one symbol
     * - ~1K active users (2K currency accounts)
     * - 1K pending limit-orders (in one order book)
     * 6-threads CPU can run this test
     */
    @Test
    public void persistenceTest() throws Exception {
        persistenceTestImpl(
                3_000_000,
                1000,
                2000,
                10,
                CURRENCIES_FUTURES,
                1,
                FUTURES_CONTRACT,
                1,
                1,
                2 * 1024,
                1024);
    }

    @Test
    public void persistenceExchangeTest() throws Exception {
        persistenceTestImpl(
                3_000_000,
                1000,
                2000,
                10,
                CURRENCIES_EXCHANGE,
                1,
                CURRENCY_EXCHANGE_PAIR,
                1,
                1,
                2 * 1024,
                1024);
    }

    /**
     * This is serialization test for verifying "triple million" capability.
     * This test requires 10+ GiB free disk space, 16+ GiB of RAM and 12-threads CPU
     */
    @Test
    public void persistenceMultiSymbol() throws Exception {
        persistenceTestImpl(
                5_000_000, //16.5
                1_000_000, // 10
                1_000_000, // 10
                25,
                ALL_CURRENCIES,
                1_000,
                BOTH,
                4,
                4,
                64 * 1024,
                1546);
    }

    private void persistenceTestImpl(final int totalTransactionsNumber,
                                     final int targetOrderBookOrdersTotal,
                                     final int numAccounts,
                                     final int iterations,
                                     final Set<Integer> currenciesAllowed,
                                     final int numSymbols,
                                     final ExchangeTestContainer.AllowedSymbolTypes allowedSymbolTypes,
                                     final int matchingEngines,
                                     final int riskEngines,
                                     final int bufferSize,
                                     final int msgsInGroupLimit) throws InterruptedException, ExecutionException {

        for (int iteration = 0; iteration < iterations; iteration++) {

            final long stateId;
            final List<CoreSymbolSpecification> coreSymbolSpecifications;
            final TestOrdersGenerator.MultiSymbolGenResult genResult;

            final long originalPrefillStateHash;

            final float originalPerfMt;

            // validate total balance as a sum of loaded funds
            final Consumer<IntLongHashMap> balancesValidator;

            try (final ExchangeTestContainer container = new ExchangeTestContainer(bufferSize, matchingEngines, riskEngines, msgsInGroupLimit, null)) {

                try (AffinityLock cpuLock = AffinityLock.acquireLock()) {

                    coreSymbolSpecifications = ExchangeTestContainer.generateRandomSymbols(numSymbols, currenciesAllowed, allowedSymbolTypes);

                    final List<BitSet> usersAccounts = UserCurrencyAccountsGenerator.generateUsers(numAccounts, currenciesAllowed);

                    final IntLongHashMap globalAmountPerCurrency = new IntLongHashMap();
                    usersAccounts.forEach(user -> user.stream().forEach(cur -> globalAmountPerCurrency.addToValue(cur, 10_0000_0000L)));
                    balancesValidator = balances -> currenciesAllowed.forEach(cur -> {
                        assertThat(balances.get(cur), Is.is(globalAmountPerCurrency.get(cur)));
                    });

                    genResult = TestOrdersGenerator.generateMultipleSymbols(coreSymbolSpecifications,
                            totalTransactionsNumber,
                            usersAccounts,
                            targetOrderBookOrdersTotal);

                    final ExchangeApi api = container.api;

                    log.info("Init symbols...");
                    container.initBasicSymbols();
                    log.info("Load symbols...");
                    container.addSymbols(coreSymbolSpecifications);
                    log.info("Load users...");
                    container.userAccountsInit(usersAccounts);

                    log.info("Pre-fill...");
                    final List<ApiCommand> apiCommandsFill = genResult.getApiCommandsFill();
                    final CountDownLatch latchFill = new CountDownLatch(apiCommandsFill.size());
                    container.setConsumer(cmd -> {
                        if (cmd.resultCode == CommandResultCode.SUCCESS
                                && (cmd.command == OrderCommandType.MOVE_ORDER || cmd.command == OrderCommandType.CANCEL_ORDER || cmd.command == OrderCommandType.PLACE_ORDER)) {
                            latchFill.countDown();
                        } else {
                            throw new IllegalStateException("Unexpected command");
                        }
                    });
                    apiCommandsFill.forEach(api::submitCommand);
                    latchFill.await();

                    container.setConsumer(cmd -> {
                    });

                    container.validateTotalBalance(balancesValidator);

                    log.info("Persisting...");
                    final long tc = System.currentTimeMillis();
                    stateId = tc;
                    container.submitMultiCommandSync(ApiPersistState.builder().dumpId(stateId).build());
                    final float persistTimeSec = (float) (System.currentTimeMillis() - tc) / 1000.0f;
                    log.debug("Persisting time: {}s", String.format("%.3f", persistTimeSec));


                    originalPrefillStateHash = container.requestStateHash();
//                    originalPrefillStateHash = container.submitCommandSync(ApiStateHashRequest.builder().build(), res -> {
//                        assertThat(res.command, is(OrderCommandType.STATE_HASH_REQUEST));
//                        assertThat(res.resultCode, is(CommandResultCode.SUCCESS));
//                        return res.orderId;
//                    });

                    log.info("Benchmarking original state...");
                    List<ApiCommand> apiCommandsBenchmark = genResult.getApiCommandsBenchmark();
                    final CountDownLatch latchBenchmark = new CountDownLatch(apiCommandsBenchmark.size());
                    container.setConsumer(cmd -> latchBenchmark.countDown());
                    long t = System.currentTimeMillis();
                    apiCommandsBenchmark.forEach(api::submitCommand);
                    latchBenchmark.await();
                    t = System.currentTimeMillis() - t;

                    container.validateTotalBalance(balancesValidator);

                    originalPerfMt = (float) apiCommandsBenchmark.size() / (float) t / 1000.0f;
                    log.info("{}. original speed: {} MT/s", iteration, String.format("%.3f", originalPerfMt));
                }

            }

            System.gc();
            Thread.sleep(200);

            log.debug("Creating new exchange from persisted state...");
            final long tLoad = System.currentTimeMillis();
            try (final ExchangeTestContainer recreatedContainer = new ExchangeTestContainer(bufferSize, matchingEngines, riskEngines, msgsInGroupLimit, stateId)) {
                float loadTimeSec = (float) (System.currentTimeMillis() - tLoad) / 1000.0f;
                log.debug("Load+start time: {}s", String.format("%.3f", loadTimeSec));

                try (AffinityLock cpuLock = AffinityLock.acquireCore()) {

//                    final long restoredPrefillStateHash = recreatedContainer.submitCommandSync(ApiStateHashRequest.builder().build(), res -> {
//                        assertThat(res.command, is(OrderCommandType.STATE_HASH_REQUEST));
//                        assertThat(res.resultCode, is(CommandResultCode.SUCCESS));
//                        return res.orderId;
//                    });
                    final long restoredPrefillStateHash = recreatedContainer.requestStateHash();
                    assertThat(restoredPrefillStateHash, is(originalPrefillStateHash));

                    recreatedContainer.validateTotalBalance(balancesValidator);

                    log.info("Restored snapshot is valid, benchmarking original state...");
                    final ExchangeApi api = recreatedContainer.api;
                    List<ApiCommand> apiCommandsBenchmark = genResult.getApiCommandsBenchmark();
                    final CountDownLatch latchBenchmark = new CountDownLatch(apiCommandsBenchmark.size());
                    recreatedContainer.setConsumer(cmd -> latchBenchmark.countDown());
                    long t = System.currentTimeMillis();
                    apiCommandsBenchmark.forEach(api::submitCommand);
                    latchBenchmark.await();
                    t = System.currentTimeMillis() - t;

                    recreatedContainer.validateTotalBalance(balancesValidator);

                    final float perfMt = (float) apiCommandsBenchmark.size() / (float) t / 1000.0f;
                    final float perfRatioPerc = perfMt / originalPerfMt * 100f;
                    log.info("{}. restored speed: {} MT/s ({}%)", iteration, String.format("%.3f", perfMt), String.format("%.1f", perfRatioPerc));
                }
            }

            System.gc();
            Thread.sleep(200);
        }

    }
}