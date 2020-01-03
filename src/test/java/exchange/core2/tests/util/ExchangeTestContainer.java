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
import exchange.core2.core.common.*;
import exchange.core2.core.common.api.*;
import exchange.core2.core.common.api.binary.BatchAddAccountsCommand;
import exchange.core2.core.common.api.binary.BatchAddSymbolsCommand;
import exchange.core2.core.common.api.reports.*;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.orderbook.OrderBookDirectImpl;
import exchange.core2.core.processors.journalling.DiskSerializationProcessor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.hamcrest.core.Is;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static exchange.core2.core.utils.UnsafeUtils.ThreadAffinityMode.THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Slf4j
public final class ExchangeTestContainer implements AutoCloseable {

    private static final int RING_BUFFER_SIZE_DEFAULT = 64 * 1024;
    private static final int RISK_ENGINES_ONE = 1;
    private static final int MATCHING_ENGINES_ONE = 1;
    private static final int MGS_IN_GROUP_LIMIT_DEFAULT = 128;


    private final ExchangeCore exchangeCore;

    @Getter
    private final ExchangeApi api;

    private AtomicLong uniqueIdCounterLong = new AtomicLong();
    private AtomicInteger uniqueIdCounterInt = new AtomicInteger();

    @Setter
    private Consumer<OrderCommand> consumer = cmd -> {
    };

    public static final Consumer<OrderCommand> CHECK_SUCCESS = cmd -> assertEquals(CommandResultCode.SUCCESS, cmd.resultCode);

    public ExchangeTestContainer() {
        this(RING_BUFFER_SIZE_DEFAULT, MATCHING_ENGINES_ONE, RISK_ENGINES_ONE, MGS_IN_GROUP_LIMIT_DEFAULT, null);
    }

    public ExchangeTestContainer(final int bufferSize,
                                 final int matchingEnginesNum,
                                 final int riskEnginesNum,
                                 final int msgsInGroupLimit,
                                 final Long stateId) {

        this.exchangeCore = ExchangeCore.builder()
                .resultsConsumer((cmd, seq) -> consumer.accept(cmd))
                .serializationProcessor(new DiskSerializationProcessor("./dumps"))
                .ringBufferSize(bufferSize)
                .matchingEnginesNum(matchingEnginesNum)
                .riskEnginesNum(riskEnginesNum)
                .msgsInGroupLimit(msgsInGroupLimit)
                .threadAffinityMode(THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE)
                .waitStrategy(CoreWaitStrategy.BUSY_SPIN)
//                .orderBookFactory(symbolType -> new OrderBookFastImpl(OrderBookFastImpl.DEFAULT_HOT_WIDTH, symbolType))
                .orderBookFactory(OrderBookDirectImpl::new)
//                .orderBookFactory(OrderBookNaiveImpl::new)
                .loadStateId(stateId) // Loading from persisted state
                .build();

        this.exchangeCore.startup();
        api = this.exchangeCore.getApi();
    }

//    public ExchangeTestContainer(final ExchangeCore exchangeCore) {
//
//        this.exchangeCore = exchangeCore;
//        this.exchangeCore.startup();
//        api = this.exchangeCore.getApi();
//    }


    public void initBasicSymbols() {

        addSymbol(TestConstants.SYMBOLSPEC_EUR_USD);
        addSymbol(TestConstants.SYMBOLSPEC_ETH_XBT);
    }

    public void initFeeSymbols() {

        addSymbol(TestConstants.SYMBOLSPECFEE_XBT_LTC);
    }

    public void initBasicUsers() throws InterruptedException {

        final List<ApiCommand> cmds = new ArrayList<>();

        cmds.add(ApiAddUser.builder().uid(TestConstants.UID_1).build());
        cmds.add(ApiAdjustUserBalance.builder().uid(TestConstants.UID_1).transactionId(1L).amount(10_000_00L).currency(TestConstants.CURRENECY_USD).build());
        cmds.add(ApiAdjustUserBalance.builder().uid(TestConstants.UID_1).transactionId(2L).amount(1_0000_0000L).currency(TestConstants.CURRENECY_XBT).build());
        cmds.add(ApiAdjustUserBalance.builder().uid(TestConstants.UID_1).transactionId(3L).amount(1_0000_0000L).currency(TestConstants.CURRENECY_ETH).build());

        cmds.add(ApiAddUser.builder().uid(TestConstants.UID_2).build());
        cmds.add(ApiAdjustUserBalance.builder().uid(TestConstants.UID_2).transactionId(1L).amount(20_000_00L).currency(TestConstants.CURRENECY_USD).build());
        cmds.add(ApiAdjustUserBalance.builder().uid(TestConstants.UID_2).transactionId(2L).amount(1_0000_0000L).currency(TestConstants.CURRENECY_XBT).build());
        cmds.add(ApiAdjustUserBalance.builder().uid(TestConstants.UID_2).transactionId(3L).amount(1_0000_0000L).currency(TestConstants.CURRENECY_ETH).build());

        submitCommandsSync(cmds);
    }

    public void createUserWithMoney(long uid, int currency, long amount) throws InterruptedException {
        final List<ApiCommand> cmds = new ArrayList<>();
        cmds.add(ApiAddUser.builder().uid(uid).build());
        cmds.add(ApiAdjustUserBalance.builder().uid(uid).transactionId(getRandomTransactionId()).amount(amount).currency(currency).build());
        submitCommandsSync(cmds);
    }

    public void addMoneyToUser(long uid, int currency, long amount) throws InterruptedException {
        final List<ApiCommand> cmds = new ArrayList<>();
        cmds.add(ApiAdjustUserBalance.builder().uid(uid).transactionId(getRandomTransactionId()).amount(amount).currency(currency).build());
        submitCommandsSync(cmds);
    }


    public void addSymbol(final CoreSymbolSpecification symbol) {
        addSymbols(new BatchAddSymbolsCommand(symbol));
    }

    public void addSymbols(final List<CoreSymbolSpecification> symbols) {
        Lists.partition(symbols, 1024).forEach(partition -> addSymbols(new BatchAddSymbolsCommand(partition)));
    }

    public void addSymbols(final BatchAddSymbolsCommand symbols) {
        submitMultiCommandSync(ApiBinaryDataCommand.builder().transferId(getRandomTransferId()).data(symbols).build());
    }

    private int getRandomTransferId() {
        return uniqueIdCounterInt.incrementAndGet();
    }

    private long getRandomTransactionId() {
        return uniqueIdCounterLong.incrementAndGet();
    }

    public final void userAccountsInit(List<BitSet> userCurrencies) throws InterruptedException {

        // calculate max amount can transfer to each account so that it is not possible to get long overflow
        final IntLongHashMap accountsNumPerCurrency = new IntLongHashMap();
        userCurrencies.forEach(accounts -> accounts.stream().forEach(currency -> accountsNumPerCurrency.addToValue(currency, 1)));
        final IntLongHashMap amountPerAccount = new IntLongHashMap();
        accountsNumPerCurrency.forEachKeyValue((currency, numAcc) -> amountPerAccount.put(currency, Long.MAX_VALUE / (numAcc + 1)));
        // amountPerAccount.forEachKeyValue((k, v) -> log.debug("{}={}", k, v));

        final int totalAccounts = userCurrencies.stream().skip(1).mapToInt(BitSet::cardinality).sum();
        final int numUsers = userCurrencies.size() - 1;
        final CountDownLatch usersLatch = new CountDownLatch(totalAccounts + numUsers);
        consumer = cmd -> {
            if (cmd.resultCode == CommandResultCode.SUCCESS
                    && (cmd.command == OrderCommandType.ADD_USER || cmd.command == OrderCommandType.BALANCE_ADJUSTMENT)) {
                usersLatch.countDown();
            } else {
                throw new IllegalStateException("Unexpected command" + cmd);
            }
        };

        IntStream.rangeClosed(1, numUsers).forEach(uid -> {
            api.submitCommand(ApiAddUser.builder().uid(uid).build());
            userCurrencies.get(uid).stream().forEach(currency ->
                    api.submitCommand(ApiAdjustUserBalance.builder()
                            .uid(uid)
                            .transactionId(getRandomTransactionId())
                            .amount(amountPerAccount.get(currency))
                            .currency(currency)
                            .build()));

//            if (uid > 1000000 && uid % 1000000 == 0) {
//                log.debug("uid: {} usersLatch: {}", uid, usersLatch.getCount());
//            }
        });
        usersLatch.await();

        consumer = cmd -> {
        };

    }

    public void usersInit(int numUsers, Set<Integer> currencies) throws InterruptedException {

        int totalCommands = numUsers * (1 + currencies.size());
        final CountDownLatch usersLatch = new CountDownLatch(totalCommands);
        consumer = cmd -> {
            if (cmd.resultCode == CommandResultCode.SUCCESS
                    && (cmd.command == OrderCommandType.ADD_USER || cmd.command == OrderCommandType.BALANCE_ADJUSTMENT)) {
                usersLatch.countDown();
            } else {
                throw new IllegalStateException("Unexpected command" + cmd);
            }
        };

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
                    if (uid > 1_000_000 && uid % 1_000_000 == 0) {
                        log.debug("uid: {} usersLatch: {}", uid, usersLatch.getCount());
                    }
                });
        usersLatch.await();

        consumer = cmd -> {
        };

    }

    // TODO slow (due allocations)
    public void usersInitBatch(int numUsers, Set<Integer> currencies) {
        int fromUid = 0;
        final int batchSize = 1024;
        while (usersInitBatch(fromUid, Math.min(fromUid + batchSize, numUsers + 1), currencies)) {
            fromUid += batchSize;
        }
    }

    public boolean usersInitBatch(int uidStartIncl, int uidStartExcl, Set<Integer> currencies) {

        if (uidStartIncl > uidStartExcl) {
            return false;
        }

        final LongObjectHashMap<IntLongHashMap> users = new LongObjectHashMap<>();
        for (int uid = uidStartIncl; uid < uidStartExcl; uid++) {
            final IntLongHashMap accounts = new IntLongHashMap();
            currencies.forEach(currency -> accounts.put(currency, 10_0000_0000L));
            users.put(uid, accounts);
            if (uid > 100000 && uid % 100000 == 0) {
                log.debug("uid: {}", uid);
            }
        }
        submitMultiCommandSync(ApiBinaryDataCommand.builder().transferId(getRandomTransferId()).data(new BatchAddAccountsCommand(users)).build());

        return true;
    }


    public void resetExchangeCore() throws InterruptedException {
        submitCommandSync(ApiReset.builder().build(), CHECK_SUCCESS);
    }

    public void submitCommandSync(ApiCommand apiCommand, CommandResultCode expectedResultCode) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        consumer = cmd -> {
            assertThat(cmd.resultCode, Is.is(expectedResultCode));
            latch.countDown();
        };
        api.submitCommand(apiCommand);
        latch.await();
        consumer = cmd -> {
        };
    }


    public void submitCommandSync(ApiCommand apiCommand, Consumer<OrderCommand> validator) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        consumer = cmd -> {
            validator.accept(cmd);
            latch.countDown();
        };
        api.submitCommand(apiCommand);
        latch.await();
        consumer = cmd -> {
        };
    }

    public void submitMultiCommandSync(ApiCommand dataCommand) {
        final CountDownLatch latch = new CountDownLatch(1);
        consumer = cmd -> {
            if (cmd.command != OrderCommandType.BINARY_DATA
                    && cmd.command != OrderCommandType.PERSIST_STATE_RISK
                    && cmd.command != OrderCommandType.PERSIST_STATE_MATCHING) {
                throw new IllegalStateException("Unexpected command");
            }
            if (cmd.resultCode == CommandResultCode.SUCCESS) {
                latch.countDown();
            } else if (cmd.resultCode != CommandResultCode.ACCEPTED) {
                throw new IllegalStateException("Unexpected result code");
            }
        };
        api.submitCommand(dataCommand);
        try {
            latch.await();
        } catch (InterruptedException ex) {
            throw new IllegalStateException(ex);
        }
        consumer = cmd -> {
        };
    }


    void submitCommandsSync(List<ApiCommand> apiCommand) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(apiCommand.size());
        consumer = cmd -> {
            assertEquals(CommandResultCode.SUCCESS, cmd.resultCode);
            latch.countDown();
        };
        apiCommand.forEach(api::submitCommand);
        latch.await();
        consumer = cmd -> {
        };
    }

    public L2MarketData requestCurrentOrderBook(final int symbol) {
        BlockingQueue<OrderCommand> queue = attachNewConsumerQueue();
        api.submitCommand(ApiOrderBookRequest.builder().symbol(symbol).size(-1).build());
        OrderCommand orderBookCmd = waitForOrderCommands(queue, 1).get(0);
        L2MarketData actualState = orderBookCmd.marketData;
        assertNotNull(actualState);
        return actualState;
    }

    BlockingQueue<OrderCommand> attachNewConsumerQueue() {
        final BlockingQueue<OrderCommand> results = new LinkedBlockingQueue<>();
        consumer = cmd -> results.add(cmd.copy());
        return results;
    }

    List<OrderCommand> waitForOrderCommands(BlockingQueue<OrderCommand> results, int c) {
        return Stream.generate(() -> {
            try {
                return results.poll(10000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ex) {
                throw new IllegalStateException();
            }
        })
                .limit(c)
                .collect(Collectors.toList());
    }


    public void validateUserState(
            long uid,
            Consumer<? super UserProfile> riskEngineStateConsumer,
            Consumer<? super Map<Long, Order>> matchingEngineStateConsumer) throws InterruptedException, ExecutionException {

        final SingleUserReportResult res = api.processReport(new SingleUserReportQuery(uid), getRandomTransferId()).get();
        riskEngineStateConsumer.accept(res.getUserProfile());
        matchingEngineStateConsumer.accept(res.getOrders().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(Order::getOrderId, ord -> ord)));
    }


    public TotalCurrencyBalanceReportResult totalBalanceReport() throws InterruptedException, ExecutionException {
        final TotalCurrencyBalanceReportResult res = api.processReport(new TotalCurrencyBalanceReportQuery(), getRandomTransferId()).get();
        final IntLongHashMap openInterestLong = res.getOpenInterestLong();
        final IntLongHashMap openInterestShort = res.getOpenInterestShort();
//        log.debug("accBal : {}", res.getAccountBalances());
//        log.debug("fees   : {}", res.getFees());
//        log.debug("ordBal : {}", res.getOrdersBalances());
//        log.debug("OpenIntLong: {}", openInterestLong);
//        log.debug("OpenIntShort: {}", openInterestShort);
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
                        .makerFee(makerFee)
                        .build();

                result.add(symbol);

                //log.debug("{}", symbol);
                i++;
            }
        }
        return result;
    }

    @Override
    public void close() {
        exchangeCore.shutdown();
    }

    public enum AllowedSymbolTypes {
        FUTURES_CONTRACT,
        CURRENCY_EXCHANGE_PAIR,
        BOTH
    }
}
