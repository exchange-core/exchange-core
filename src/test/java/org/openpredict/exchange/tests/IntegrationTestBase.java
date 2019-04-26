package org.openpredict.exchange.tests;

import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.nustaq.serialization.FSTConfiguration;
import org.openpredict.exchange.beans.CoreSymbolSpecification;
import org.openpredict.exchange.beans.L2MarketData;
import org.openpredict.exchange.beans.SymbolType;
import org.openpredict.exchange.beans.api.*;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.core.ExchangeApi;
import org.openpredict.exchange.core.ExchangeCore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Slf4j
public abstract class IntegrationTestBase {

    static final int SYMBOL_MARGIN = 5991;
    static final int SYMBOL_EXCHANGE = 9269;

    static final int CURRENECY_USD = 840;
    static final int CURRENECY_EUR = 978;
    static final int CURRENECY_XBT = 3762;
    static final int CURRENECY_ETH = 3928;

    ExchangeCore exchangeCore;
    ExchangeApi api;

    volatile Consumer<OrderCommand> consumer = cmd -> {
    };

    static final Consumer<OrderCommand> CHECK_SUCCESS = cmd -> assertEquals(CommandResultCode.SUCCESS, cmd.resultCode);

    @Before
    public void initExchange() {
        exchangeCore = new ExchangeCore(cmd -> consumer.accept(cmd));
        exchangeCore.startup();
        api = exchangeCore.getApi();
    }

    @After
    public void shutdownExchange() {
        exchangeCore.shutdown();
    }

    void initSymbol() throws InterruptedException {

        final CoreSymbolSpecification symbol1 = CoreSymbolSpecification.builder()
                .symbolId(SYMBOL_MARGIN)
                .type(SymbolType.FUTURES_CONTRACT)
                .baseCurrency(CURRENECY_EUR)
                .quoteCurrency(CURRENECY_USD)
                .baseScaleK(1)
                .quoteScaleK(1)
                .depositBuy(22000)
                .depositSell(32100)
                .takerFee(0)
                .makerFee(0)
                .build();

        addSymbol(symbol1);

        final CoreSymbolSpecification symbol2 = CoreSymbolSpecification.builder()
                .symbolId(SYMBOL_EXCHANGE)
                .type(SymbolType.CURRENCY_EXCHANGE_PAIR)
                .baseCurrency(CURRENECY_ETH)
                .quoteCurrency(CURRENECY_XBT)
                .baseScaleK(1)
                .quoteScaleK(1)
                .takerFee(0)
                .makerFee(0)
                .build();

        addSymbol(symbol2);
    }

    private void addSymbol(CoreSymbolSpecification symbol) throws InterruptedException {
        final FSTConfiguration minBin = FSTConfiguration.createMinBinConfiguration();
        minBin.registerCrossPlatformClassMappingUseSimpleName(CoreSymbolSpecification.class);
        //new MBPrinter().printMessage(minBin.asByteArray(symbol));

        final ApiBinaryDataCommand binaryCmd = ApiBinaryDataCommand.builder().transferId(0).data(symbol).build();
        submitBinaryCommandSync(binaryCmd);
    }

    void usersInit(int numUsers) throws InterruptedException {
        List<ApiCommand> commands = LongStream.rangeClosed(1, numUsers)
                .mapToObj(uid -> {
                    List<ApiCommand> userCmds = new ArrayList<>();
                    userCmds.add(ApiAddUser.builder().uid(uid).build());
                    userCmds.add(ApiAdjustUserBalance.builder().uid(uid).transactionId(1243).amount(20_000_000_00L).currency(CURRENECY_USD).build());
                    userCmds.add(ApiAdjustUserBalance.builder().uid(uid).transactionId(1256).amount(10_0000_0000L).currency(CURRENECY_XBT).build());
                    userCmds.add(ApiAdjustUserBalance.builder().uid(uid).transactionId(1277).amount(10_0000_0000L).currency(CURRENECY_ETH).build());
                    return userCmds;
                })
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        final CountDownLatch usersLatch = new CountDownLatch(commands.size());
        consumer = cmd -> usersLatch.countDown();
        commands.forEach(api::submitCommand);
        usersLatch.await();
    }

    void resetExchangeCore() throws InterruptedException {
        submitCommandSync(ApiReset.builder().build(), CHECK_SUCCESS);
    }

    void submitCommandSync(ApiCommand apiCommand, Consumer<OrderCommand> validator) throws InterruptedException {
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

    void submitBinaryCommandSync(ApiBinaryDataCommand dataCommand) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        consumer = cmd -> {
            if (cmd.resultCode == CommandResultCode.ACCEPTED) {
                //
            } else if (cmd.resultCode == CommandResultCode.SUCCESS) {
                latch.countDown();
            } else {
                throw new IllegalStateException("Expected ACCEPTED or SUCCESS only, but received " + cmd.resultCode);
            }
        };
        api.submitCommand(dataCommand);
        latch.await();
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


    L2MarketData requestCurrentOrderBook(final int symbol) {
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

}
