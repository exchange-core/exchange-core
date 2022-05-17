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
package exchange.core2.tests.util;

import com.google.common.collect.Lists;
import exchange.core2.core.ExchangeApi;
import exchange.core2.core.ExchangeCore;
import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.common.L2MarketData;
import exchange.core2.core.common.SymbolType;
import exchange.core2.core.common.api.*;
import exchange.core2.core.common.api.binary.BatchAddSymbolsCommand;
import exchange.core2.core.common.api.binary.BinaryDataCommand;
import exchange.core2.core.common.api.reports.*;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.config.*;
import exchange.core2.core.utils.AffinityThreadFactory;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.hamcrest.core.Is;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.ObjLongConsumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public final class ExchangeTestContainer implements AutoCloseable {

    private final ExchangeCore exchangeCore;

    @Getter
    private final ExchangeApi api;

    @Getter
    private final AffinityThreadFactory threadFactory;

    private AtomicLong uniqueIdCounterLong = new AtomicLong();
    private AtomicInteger uniqueIdCounterInt = new AtomicInteger();

    @Setter
    private ObjLongConsumer<OrderCommand> consumer = (cmd, seq) -> {
    };

    public static final Consumer<OrderCommand> CHECK_SUCCESS = cmd -> assertEquals(CommandResultCode.SUCCESS, cmd.resultCode);

    public static String timeBasedExchangeId() {
        return String.format("%012X", System.currentTimeMillis());
    }

    public static ExchangeTestContainer create(final PerformanceConfiguration perfCfg) {
        return new ExchangeTestContainer(perfCfg,
                InitialStateConfiguration.CLEAN_TEST,
                SerializationConfiguration.DEFAULT);
    }

    public static ExchangeTestContainer create(final PerformanceConfiguration perfCfg,
                                               final InitialStateConfiguration initStateCfg,
                                               final SerializationConfiguration serializationCfg) {
        return new ExchangeTestContainer(perfCfg, initStateCfg, serializationCfg);
    }

    public static TestDataFutures prepareTestDataAsync(TestDataParameters parameters, int seed) {

        final CompletableFuture<List<CoreSymbolSpecification>> coreSymbolSpecificationsFuture = CompletableFuture.supplyAsync(
                () -> ExchangeTestContainer.generateRandomSymbols(parameters.numSymbols, parameters.currenciesAllowed, parameters.allowedSymbolTypes));

        final CompletableFuture<List<BitSet>> usersAccountsFuture = CompletableFuture.supplyAsync(
                () -> UserCurrencyAccountsGenerator.generateUsers(parameters.numAccounts, parameters.currenciesAllowed));

        final CompletableFuture<TestOrdersGenerator.MultiSymbolGenResult> genResultFuture = coreSymbolSpecificationsFuture.thenCombineAsync(
                usersAccountsFuture,
                (css, ua) -> TestOrdersGenerator.generateMultipleSymbols(
                        TestOrdersGeneratorConfig.builder()
                                .coreSymbolSpecifications(css)
                                .totalTransactionsNumber(parameters.totalTransactionsNumber)
                                .usersAccounts(ua)
                                .targetOrderBookOrdersTotal(parameters.targetOrderBookOrdersTotal)
                                .seed(seed)
                                .preFillMode(parameters.preFillMode)
                                .avalancheIOC(parameters.avalancheIOC)
                                .build()));

        return TestDataFutures.builder()
                .coreSymbolSpecifications(coreSymbolSpecificationsFuture)
                .usersAccounts(usersAccountsFuture)
                .genResult(genResultFuture)
                .build();
    }

    @Data
    @Builder
    public static class TestDataFutures {
        final CompletableFuture<List<CoreSymbolSpecification>> coreSymbolSpecifications;
        final CompletableFuture<List<BitSet>> usersAccounts;
        final CompletableFuture<TestOrdersGenerator.MultiSymbolGenResult> genResult;
    }

    private ExchangeTestContainer(final PerformanceConfiguration perfCfg,
                                  final InitialStateConfiguration initStateCfg,
                                  final SerializationConfiguration serializationCfg) {

        //log.debug("CREATING exchange container");

        this.threadFactory = new AffinityThreadFactory(AffinityThreadFactory.ThreadAffinityMode.THREAD_AFFINITY_ENABLE_PER_PHYSICAL_CORE);

        final ExchangeConfiguration exchangeConfiguration = ExchangeConfiguration.defaultBuilder()
                .initStateCfg(initStateCfg)
                .performanceCfg(perfCfg)
                .reportsQueriesCfg(ReportsQueriesConfiguration.createStandardConfig())
                .ordersProcessingCfg(OrdersProcessingConfiguration.DEFAULT)
                .loggingCfg(LoggingConfiguration.DEFAULT)
                .serializationCfg(serializationCfg)
                .build();

        this.exchangeCore = ExchangeCore.builder()
                .resultsConsumer((cmd, seq) -> consumer.accept(cmd, seq))
                .exchangeConfiguration(exchangeConfiguration)
                .build();

        //log.debug("STARTING exchange container");
        this.exchangeCore.startup();

        //log.debug("STARTED exchange container");
        this.api = this.exchangeCore.getApi();
    }

    public void initBasicSymbols() {

        addSymbol(TestConstants.SYMBOLSPEC_EUR_USD);
        addSymbol(TestConstants.SYMBOLSPEC_ETH_XBT);
    }

    public void initFeeSymbols() {
        addSymbol(TestConstants.SYMBOLSPECFEE_XBT_LTC);
        addSymbol(TestConstants.SYMBOLSPECFEE_USD_JPY);
    }

    public void initBasicUsers() {
        initBasicUser(TestConstants.UID_1);
        initBasicUser(TestConstants.UID_2);
        initBasicUser(TestConstants.UID_3);
        initBasicUser(TestConstants.UID_4);
    }

    public void initFeeUsers() {
        initFeeUser(TestConstants.UID_1);
        initFeeUser(TestConstants.UID_2);
        initFeeUser(TestConstants.UID_3);
        initFeeUser(TestConstants.UID_4);
    }

    public void initBasicUser(long uid) {
        assertThat(api.submitCommandAsync(ApiAddUser.builder().uid(uid).build()).join(), Is.is(CommandResultCode.SUCCESS));
        assertThat(api.submitCommandAsync(ApiAdjustUserBalance.builder().uid(uid).transactionId(1L).amount(10_000_00L).currency(TestConstants.CURRENECY_USD).build()).join(), Is.is(CommandResultCode.SUCCESS));
        assertThat(api.submitCommandAsync(ApiAdjustUserBalance.builder().uid(uid).transactionId(2L).amount(1_0000_0000L).currency(TestConstants.CURRENECY_XBT).build()).join(), Is.is(CommandResultCode.SUCCESS));
        assertThat(api.submitCommandAsync(ApiAdjustUserBalance.builder().uid(uid).transactionId(3L).amount(1_0000_0000L).currency(TestConstants.CURRENECY_ETH).build()).join(), Is.is(CommandResultCode.SUCCESS));
    }

    public void initFeeUser(long uid) {
        assertThat(api.submitCommandAsync(ApiAddUser.builder().uid(uid).build()).join(), Is.is(CommandResultCode.SUCCESS));
        assertThat(api.submitCommandAsync(ApiAdjustUserBalance.builder().uid(uid).transactionId(1L).amount(10_000_00L).currency(TestConstants.CURRENECY_USD).build()).join(), Is.is(CommandResultCode.SUCCESS));
        assertThat(api.submitCommandAsync(ApiAdjustUserBalance.builder().uid(uid).transactionId(2L).amount(10_000_000L).currency(TestConstants.CURRENECY_JPY).build()).join(), Is.is(CommandResultCode.SUCCESS));
        assertThat(api.submitCommandAsync(ApiAdjustUserBalance.builder().uid(uid).transactionId(3L).amount(1_0000_0000L).currency(TestConstants.CURRENECY_XBT).build()).join(), Is.is(CommandResultCode.SUCCESS));
        assertThat(api.submitCommandAsync(ApiAdjustUserBalance.builder().uid(uid).transactionId(4L).amount(1000_0000_0000L).currency(TestConstants.CURRENECY_LTC).build()).join(), Is.is(CommandResultCode.SUCCESS));
    }

    public void createUserWithMoney(long uid, int currency, long amount) {
        final List<ApiCommand> cmds = new ArrayList<>();
        cmds.add(ApiAddUser.builder().uid(uid).build());
        cmds.add(ApiAdjustUserBalance.builder().uid(uid).transactionId(getRandomTransactionId()).amount(amount).currency(currency).build());
        api.submitCommandsSync(cmds);
    }

    public void addMoneyToUser(long uid, int currency, long amount) {
        final List<ApiCommand> cmds = new ArrayList<>();
        cmds.add(ApiAdjustUserBalance.builder().uid(uid).transactionId(getRandomTransactionId()).amount(amount).currency(currency).build());
        api.submitCommandsSync(cmds);
    }


    public void addSymbol(final CoreSymbolSpecification symbol) {
        sendBinaryDataCommandSync(new BatchAddSymbolsCommand(symbol), 5000);
    }

    public void addSymbols(final List<CoreSymbolSpecification> symbols) {
        // split by chunks
        Lists.partition(symbols, 10000).forEach(partition -> sendBinaryDataCommandSync(new BatchAddSymbolsCommand(partition), 5000));
    }

    public void sendBinaryDataCommandSync(final BinaryDataCommand data, final int timeOutMs) {
        final Future<CommandResultCode> future = api.submitBinaryDataAsync(data);
        try {
            assertThat(future.get(timeOutMs, TimeUnit.MILLISECONDS), Is.is(CommandResultCode.SUCCESS));
        } catch (final InterruptedException | ExecutionException | TimeoutException ex) {
            log.error("Failed sending binary data command", ex);
            throw new RuntimeException(ex);
        }
    }

    private int getRandomTransferId() {
        return uniqueIdCounterInt.incrementAndGet();
    }

    private long getRandomTransactionId() {
        return uniqueIdCounterLong.incrementAndGet();
    }

    public final void userAccountsInit(List<BitSet> userCurrencies) {

        // calculate max amount can transfer to each account so that it is not possible to get long overflow
        final IntLongHashMap accountsNumPerCurrency = new IntLongHashMap();
        userCurrencies.forEach(accounts -> accounts.stream().forEach(currency -> accountsNumPerCurrency.addToValue(currency, 1)));
        final IntLongHashMap amountPerAccount = new IntLongHashMap();
        accountsNumPerCurrency.forEachKeyValue((currency, numAcc) -> amountPerAccount.put(currency, Long.MAX_VALUE / (numAcc + 1)));
        // amountPerAccount.forEachKeyValue((k, v) -> log.debug("{}={}", k, v));

        createUserAccountsRegular(userCurrencies, amountPerAccount);
    }


    private void createUserAccountsRegular(List<BitSet> userCurrencies, IntLongHashMap amountPerAccount) {
        final int numUsers = userCurrencies.size() - 1;

        IntStream.rangeClosed(1, numUsers).forEach(uid -> {
            api.submitCommand(ApiAddUser.builder().uid(uid).build());
            userCurrencies.get(uid).stream().forEach(currency ->
                    api.submitCommand(ApiAdjustUserBalance.builder()
                            .uid(uid)
                            .transactionId(getRandomTransactionId())
                            .amount(amountPerAccount.get(currency))
                            .currency(currency)
                            .build()));
        });

        api.submitCommandAsync(ApiNop.builder().build()).join();
    }

    public void usersInit(int numUsers, Set<Integer> currencies) {

        LongStream.rangeClosed(1, numUsers)
                .forEach(uid -> {
                    api.submitCommand(ApiAddUser.builder().uid(uid).build());
                    long transactionId = 1L;
                    for (int currency : currencies) {
                        api.submitCommand(ApiAdjustUserBalance.builder()
                                .uid(uid)
                                .transactionId(transactionId++)
                                .amount(10_0000_0000L)
                                .currency(currency).build());
                    }
                });

        api.submitCommandAsync(ApiNop.builder().build()).join();
    }

    public void resetExchangeCore() {
        final CommandResultCode res = api.submitCommandAsync(ApiReset.builder().build()).join();
        assertThat(res, Is.is(CommandResultCode.SUCCESS));
    }

    public void submitCommandSync(ApiCommand apiCommand, CommandResultCode expectedResultCode) {
        assertThat(api.submitCommandAsync(apiCommand).join(), Is.is(expectedResultCode));
    }

    public void submitCommandSync(ApiCommand apiCommand, Consumer<OrderCommand> validator) {
        validator.accept(api.submitCommandAsyncFullResponse(apiCommand).join());
    }

    public L2MarketData requestCurrentOrderBook(final int symbol) {
        return api.requestOrderBookAsync(symbol, -1).join();
    }

    // todo rename
    public void validateUserState(long uid, Consumer<SingleUserReportResult> resultValidator) throws InterruptedException, ExecutionException {
        resultValidator.accept(getUserProfile(uid));
    }

    public SingleUserReportResult getUserProfile(long clientId) throws InterruptedException, ExecutionException {
        return api.processReport(new SingleUserReportQuery(clientId), getRandomTransferId()).get();
    }

    public TotalCurrencyBalanceReportResult totalBalanceReport() {
        final TotalCurrencyBalanceReportResult res = api.processReport(new TotalCurrencyBalanceReportQuery(), getRandomTransferId()).join();
        final IntLongHashMap openInterestLong = res.getOpenInterestLong();
        final IntLongHashMap openInterestShort = res.getOpenInterestShort();
        final IntLongHashMap openInterestDiff = new IntLongHashMap(openInterestLong);
        openInterestShort.forEachKeyValue((k, v) -> openInterestDiff.addToValue(k, -v));
        if (openInterestDiff.anySatisfy(vol -> vol != 0)) {
            throw new IllegalStateException("Open Interest balance check failed");
        }

        return res;
    }

    public int requestStateHash() throws InterruptedException, ExecutionException {
        return api.processReport(new StateHashReportQuery(), getRandomTransferId()).get().getStateHash();
    }

    public static List<CoreSymbolSpecification> generateRandomSymbols(final int num,
                                                                      final Collection<Integer> currenciesAllowed,
                                                                      final AllowedSymbolTypes allowedSymbolTypes) {
        final Random random = new Random(1L);

        final Supplier<SymbolType> symbolTypeSupplier;

        switch (allowedSymbolTypes) {
            case FUTURES_CONTRACT:
                symbolTypeSupplier = () -> SymbolType.FUTURES_CONTRACT;
                break;

            case CURRENCY_EXCHANGE_PAIR:
                symbolTypeSupplier = () -> SymbolType.CURRENCY_EXCHANGE_PAIR;
                break;

            case BOTH:
            default:
                symbolTypeSupplier = () -> random.nextBoolean() ? SymbolType.FUTURES_CONTRACT : SymbolType.CURRENCY_EXCHANGE_PAIR;
                break;
        }

        final List<Integer> currencies = new ArrayList<>(currenciesAllowed);
        final List<CoreSymbolSpecification> result = new ArrayList<>();
        for (int i = 0; i < num; ) {
            int baseCurrency = currencies.get(random.nextInt(currencies.size()));
            int quoteCurrency = currencies.get(random.nextInt(currencies.size()));
            if (baseCurrency != quoteCurrency) {
                final SymbolType type = symbolTypeSupplier.get();
                final long makerFee = random.nextInt(1000);
                final long takerFee = makerFee + random.nextInt(500);
                final CoreSymbolSpecification symbol = CoreSymbolSpecification.builder()
                        .symbolId(TestConstants.SYMBOL_AUTOGENERATED_RANGE_START + i)
                        .type(type)
                        .baseCurrency(baseCurrency) // TODO for futures can be any value
                        .quoteCurrency(quoteCurrency)
                        .baseScaleK(100)
                        .quoteScaleK(10)
                        .takerFee(takerFee)
                        .makerFee(makerFee) // TODO margins for futures?
                        .build();

                result.add(symbol);

                //log.debug("{}", symbol);
                i++;
            }
        }
        return result;
    }

    public void loadSymbolsUsersAndPrefillOrders(TestDataFutures testDataFutures) {

        // load symbols
        final List<CoreSymbolSpecification> coreSymbolSpecifications = testDataFutures.coreSymbolSpecifications.join();
        log.info("Loading {} symbols...", coreSymbolSpecifications.size());
        try (ExecutionTime ignore = new ExecutionTime(t -> log.debug("Loaded all symbols in {}", t))) {
            addSymbols(coreSymbolSpecifications);
        }

        // create accounts and deposit initial funds
        final List<BitSet> userAccounts = testDataFutures.usersAccounts.join();
        log.info("Loading {} users having {} accounts...", userAccounts.size(), userAccounts.stream().mapToInt(BitSet::cardinality).sum());
        try (ExecutionTime ignore = new ExecutionTime(t -> log.debug("Loaded all users in {}", t))) {
            userAccountsInit(userAccounts);
        }

        final List<ApiCommand> apiCommandsFill = testDataFutures.genResult.join().getApiCommandsFill().join();
        log.info("Order books pre-fill with {} orders...", apiCommandsFill.size());
        try (ExecutionTime ignore = new ExecutionTime(t -> log.debug("Order books pre-fill completed in {}", t))) {
            getApi().submitCommandsSync(apiCommandsFill);
        }

        assertTrue(totalBalanceReport().isGlobalBalancesAllZero());
    }

    public void loadSymbolsUsersAndPrefillOrdersNoLog(TestDataFutures testDataFutures) {

        // load symbols
        addSymbols(testDataFutures.coreSymbolSpecifications.join());

        // create accounts and deposit initial funds
        userAccountsInit(testDataFutures.usersAccounts.join());

        getApi().submitCommandsSync(testDataFutures.genResult.join().getApiCommandsFill().join());
    }


    /**
     * Run test using threads factory.
     * This is needed for correct cpu pinning.
     *
     * @param test - test lambda
     * @param <V>  return parameter type
     * @return result from test lambda
     */
    public <V> V executeTestingThread(final Callable<V> test) {
        try {
            final ExecutorService executor = Executors.newSingleThreadExecutor(threadFactory);
            final V result = executor.submit(test).get();
            executor.shutdown();
            executor.awaitTermination(3000, TimeUnit.SECONDS);
            return result;
        } catch (ExecutionException | InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    public float executeTestingThreadPerfMtps(final Callable<Integer> test) {
        return executeTestingThread(() -> {
            final long tStart = System.currentTimeMillis();
            final int numMessages = test.call();
            final long tDuration = System.currentTimeMillis() - tStart;
            return numMessages / (float) tDuration / 1000.0f;
        });
    }

    public float benchmarkMtps(final List<ApiCommand> apiCommandsBenchmark) {
        final long tStart = System.currentTimeMillis();
        getApi().submitCommandsSync(apiCommandsBenchmark);
        final long tDuration = System.currentTimeMillis() - tStart;
        return apiCommandsBenchmark.size() / (float) tDuration / 1000.0f;
    }

    @Override
    public void close() {
        exchangeCore.shutdown(3000, TimeUnit.MILLISECONDS);
    }

    public enum AllowedSymbolTypes {
        FUTURES_CONTRACT,
        CURRENCY_EXCHANGE_PAIR,
        BOTH
    }
}
