package org.openpredict.exchange.tests;

import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.openpredict.exchange.beans.L2MarketData;
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

    static final int SYMBOL = 5991;

    ExchangeCore exchangeCore;
    ExchangeApi api;

    volatile Consumer<OrderCommand> consumer;

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
        submitCommandSync(ApiAddSymbol.builder().depositBuy(22000).depositSell(32100).symbolId(SYMBOL).build(), CHECK_SUCCESS);
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
        api.submitCommand(ApiOrderBookRequest.builder().symbol(SYMBOL).size(-1).build());
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
