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
public class IntegrationTestBase {

    static final int SYMBOL_MARGIN = 5991;
    static final int SYMBOL_EXCHANGE = 9269;

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

        final CoreSymbolSpecification symbol = CoreSymbolSpecification.builder()
                .symbolId(SYMBOL_MARGIN)
                .type(SymbolType.FUTURES_CONTRACT)
                .baseCurrency(978)
                .quoteCurrency(840)
                .baseScaleK(1)
                .quoteScaleK(1)
                .depositBuy(22000)
                .depositSell(32100)
                .takerFee(0)
                .makerFee(0)
                .stepSize(1)
                .build();

        final FSTConfiguration minBin = FSTConfiguration.createMinBinConfiguration();
        minBin.registerCrossPlatformClassMappingUseSimpleName(CoreSymbolSpecification.class);
        //new MBPrinter().printMessage(minBin.asByteArray(symbol));

        final ApiBinaryDataCommand binaryCmd = ApiBinaryDataCommand.builder().transferId(0).data(symbol).build();
        submitBinaryCommandSync(binaryCmd);
    }

    void usersInit(int numUsers) throws InterruptedException {
        final CountDownLatch usersLatch = new CountDownLatch(numUsers * 2);
        consumer = cmd -> usersLatch.countDown();
        LongStream.rangeClosed(1, numUsers).forEach(uid -> {
            api.submitCommand(ApiAddUser.builder().uid(uid).build());
            api.submitCommand(ApiAdjustUserBalance.builder().uid(uid).amount(2_000_000_000L).build());
        });
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


    L2MarketData requestCurrentOrderBook() {
        BlockingQueue<OrderCommand> queue = attachNewConsumerQueue();
        api.submitCommand(ApiOrderBookRequest.builder().symbol(SYMBOL_MARGIN).size(-1).build());
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
