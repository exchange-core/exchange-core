package org.openpredict.exchange.tests.util;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.openpredict.exchange.beans.*;
import org.openpredict.exchange.beans.api.*;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.beans.cmd.OrderCommandType;
import org.openpredict.exchange.beans.reports.*;
import org.openpredict.exchange.core.ExchangeApi;
import org.openpredict.exchange.core.ExchangeCore;
import org.openpredict.exchange.core.Utils;
import org.openpredict.exchange.core.journalling.DiskSerializationProcessor;
import org.openpredict.exchange.core.orderbook.OrderBookFastImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.openpredict.exchange.core.ExchangeCore.DisruptorWaitStrategy.BUSY_SPIN;
import static org.openpredict.exchange.core.Utils.ThreadAffityMode.THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE;
import static org.openpredict.exchange.tests.util.TestConstants.*;

@Slf4j
public final class ExchangeTestContainer implements AutoCloseable {

    static final int RING_BUFFER_SIZE_DEFAULT = 64 * 1024;
    static final int RISK_ENGINES_ONE = 1;
    static final int MATCHING_ENGINES_ONE = 1;
    static final int MGS_IN_GROUP_LIMIT_DEFAULT = 128;


    public final ExchangeCore exchangeCore;
    public final ExchangeApi api;

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
                .resultsConsumer(cmd -> consumer.accept(cmd))
                .serializationProcessor(new DiskSerializationProcessor("./dumps"))
                .ringBufferSize(bufferSize)
                .matchingEnginesNum(matchingEnginesNum)
                .riskEnginesNum(riskEnginesNum)
                .msgsInGroupLimit(msgsInGroupLimit)
                .threadAffityMode(THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE)
                .waitStrategy(BUSY_SPIN)
                .orderBookFactory(symbolType -> new OrderBookFastImpl(OrderBookFastImpl.DEFAULT_HOT_WIDTH, symbolType))
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

        addSymbol(SYMBOLSPEC_EUR_USD);
        addSymbol(SYMBOLSPEC_ETH_XBT);
    }

    public void initBasicUsers() throws InterruptedException {

        final List<ApiCommand> cmds = new ArrayList<>();

        cmds.add(ApiAddUser.builder().uid(UID_1).build());
        cmds.add(ApiAdjustUserBalance.builder().uid(UID_1).transactionId(1L).amount(10_000_00L).currency(CURRENECY_USD).build());
        cmds.add(ApiAdjustUserBalance.builder().uid(UID_1).transactionId(2L).amount(1_0000_0000L).currency(CURRENECY_XBT).build());
        cmds.add(ApiAdjustUserBalance.builder().uid(UID_1).transactionId(3L).amount(1_0000_0000L).currency(CURRENECY_ETH).build());

        cmds.add(ApiAddUser.builder().uid(UID_2).build());
        cmds.add(ApiAdjustUserBalance.builder().uid(UID_2).transactionId(1L).amount(20_000_00L).currency(CURRENECY_USD).build());
        cmds.add(ApiAdjustUserBalance.builder().uid(UID_2).transactionId(2L).amount(1_0000_0000L).currency(CURRENECY_XBT).build());
        cmds.add(ApiAdjustUserBalance.builder().uid(UID_2).transactionId(3L).amount(1_0000_0000L).currency(CURRENECY_ETH).build());

        submitCommandsSync(cmds);
    }

    public void createUserWithMoney(long uid, int currency, long amount) throws InterruptedException {
        final List<ApiCommand> cmds = new ArrayList<>();
        cmds.add(ApiAddUser.builder().uid(uid).build());
        cmds.add(ApiAdjustUserBalance.builder().uid(uid).transactionId(1L).amount(amount).currency(currency).build());
        submitCommandsSync(cmds);
    }


    public void addSymbol(CoreSymbolSpecification symbol) {
        submitMultiCommandSync(ApiBinaryDataCommand.builder().transferId(System.nanoTime()).data(symbol).build());
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

        LongAdder c = new LongAdder();

        LongStream.rangeClosed(1, numUsers)
                .forEach(uid -> {
                    api.submitCommand(ApiAddUser.builder().uid(uid).build());
                    c.increment();
                    currencies.forEach(currency -> {
                        int transactionId = currency;
                        api.submitCommand(ApiAdjustUserBalance.builder().uid(uid).transactionId(transactionId).amount(10_0000_0000L).currency(currency).build());
                        c.increment();
                    });
                    if (uid > 1000000 && uid % 1000000 == 0) {
                        log.debug("uid: {} usersLatch: {}", uid, usersLatch.getCount());
                    }
                });
        usersLatch.await();

        consumer = cmd -> {
        };

//        log.debug("commands sent: {} totalCommands:{}", c, totalCommands);
    }

    public void resetExchangeCore() throws InterruptedException {
        submitCommandSync(ApiReset.builder().build(), CHECK_SUCCESS);
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

    public <T> T submitCommandSync(ApiCommand apiCommand, Function<OrderCommand, T> resultBuilder) throws InterruptedException {
        final CompletableFuture<T> future = new CompletableFuture<>();
        consumer = cmd -> future.complete(resultBuilder.apply(cmd));
        api.submitCommand(apiCommand);
        try {
            return future.get();
        } catch (ExecutionException ex) {
            throw new IllegalStateException(ex);
        } finally {
            consumer = cmd -> {
            };
        }
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


    public void validateUserState(long uid,
                                  Consumer<UserProfile> riskEngineStateConsumer,
                                  Consumer<LongObjectHashMap<Order>> matchingEngineStateConsumer) throws InterruptedException, ExecutionException {

        final SingleUserReportResult res = api.processReport(new SingleUserReportQuery(uid)).get();
        riskEngineStateConsumer.accept(res.getUserProfile());
        matchingEngineStateConsumer.accept(res.getOrders());
    }

    public void validateTotalBalance(Consumer<IntLongHashMap> balancesValidator) throws InterruptedException, ExecutionException {
        final TotalCurrencyBalanceReportResult res = api.processReport(new TotalCurrencyBalanceReportQuery()).get();
        log.debug("accBal : {}", res.getAccountBalances());
        log.debug("ordBal : {}", res.getOrdersBalances());
        final IntLongHashMap totalBalances = Utils.mergeSum(res.getAccountBalances(), res.getOrdersBalances());
        log.debug("totalBalances : {}", totalBalances);
        balancesValidator.accept(totalBalances);
    }

    public int requestStateHash() throws InterruptedException, ExecutionException {
        return api.processReport(new StateHashReportQuery()).get().getStateHash();
    }

    public List<CoreSymbolSpecification> generateAndAddSymbols(final int num,
                                                               final Set<Integer> currenciesAllowed,
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
                final CoreSymbolSpecification symbol = CoreSymbolSpecification.builder()
                        .symbolId(SYMBOL_AUTOGENERATED_RANGE_START + i)
                        .type(symbolTypeSupplier.get())
                        .baseCurrency(baseCurrency) // TODO for futures can be any value
                        .quoteCurrency(quoteCurrency)
                        .baseScaleK(1)
                        .quoteScaleK(1)
                        .takerFee(0)
                        .makerFee(0)
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
