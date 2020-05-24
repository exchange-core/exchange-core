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
package exchange.core2.core;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import exchange.core2.core.common.CoreWaitStrategy;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.common.config.ExchangeConfiguration;
import exchange.core2.core.common.config.PerformanceConfiguration;
import exchange.core2.core.common.config.SerializationConfiguration;
import exchange.core2.core.orderbook.IOrderBook;
import exchange.core2.core.processors.*;
import exchange.core2.core.processors.journaling.ISerializationProcessor;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.ObjLongConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Main exchange core class.
 * Builds configuration and starts disruptor.
 */
@Slf4j
public final class ExchangeCore {

    private final Disruptor<OrderCommand> disruptor;

    private final ExchangeApi api;

    private final ISerializationProcessor serializationProcessor;

    private final ExchangeConfiguration exchangeConfiguration;

    // core can be started and stopped only once
    private boolean started = false;
    private boolean stopped = false;

    // enable MatcherTradeEvent pooling
    public static final boolean EVENTS_POOLING = false;

    /**
     * Exchange core constructor.
     *
     * @param resultsConsumer       - custom consumer of processed commands
     * @param exchangeConfiguration - exchange configuration
     */
    @Builder
    public ExchangeCore(final ObjLongConsumer<OrderCommand> resultsConsumer,
                        final ExchangeConfiguration exchangeConfiguration) {

        log.debug("Building exchange core from configuration: {}", exchangeConfiguration);

        this.exchangeConfiguration = exchangeConfiguration;

        final PerformanceConfiguration perfCfg = exchangeConfiguration.getPerformanceCfg();

        final int ringBufferSize = perfCfg.getRingBufferSize();

        this.disruptor = new Disruptor<>(
                OrderCommand::new,
                ringBufferSize,
                perfCfg.getThreadFactory(),
                ProducerType.MULTI, // multiple gateway threads are writing
                perfCfg.getWaitStrategy().getDisruptorWaitStrategy());

        this.api = new ExchangeApi(disruptor.getRingBuffer(), perfCfg.getBinaryCommandsLz4CompressorFactory().get());

        final ThreadFactory threadFactory = perfCfg.getThreadFactory();
        final IOrderBook.OrderBookFactory orderBookFactory = perfCfg.getOrderBookFactory();
        final CoreWaitStrategy coreWaitStrategy = perfCfg.getWaitStrategy();

        final int matchingEnginesNum = perfCfg.getMatchingEnginesNum();
        final int riskEnginesNum = perfCfg.getRiskEnginesNum();

        final SerializationConfiguration serializationCfg = exchangeConfiguration.getSerializationCfg();

        // creating serialization processor
        serializationProcessor = serializationCfg.getSerializationProcessorFactory().apply(exchangeConfiguration);

        // creating shared objects pool
        final int poolInitialSize = (matchingEnginesNum + riskEnginesNum) * 8;
        final int chainLength = EVENTS_POOLING ? 1024 : 1;
        final SharedPool sharedPool = new SharedPool(poolInitialSize * 4, poolInitialSize, chainLength);

        // creating and attaching exceptions handler
        final DisruptorExceptionHandler<OrderCommand> exceptionHandler = new DisruptorExceptionHandler<>("main", (ex, seq) -> {
            log.error("Exception thrown on sequence={}", seq, ex);
            // TODO re-throw exception on publishing
            disruptor.getRingBuffer().publishEvent(SHUTDOWN_SIGNAL_TRANSLATOR);
            disruptor.shutdown();
        });

        disruptor.setDefaultExceptionHandler(exceptionHandler);

        // advice completable future to use the same CPU socket as disruptor
        final ExecutorService loaderExecutor = Executors.newFixedThreadPool(matchingEnginesNum + riskEnginesNum, threadFactory);

        // start creating matching engines
        final Map<Integer, CompletableFuture<MatchingEngineRouter>> matchingEngineFutures = IntStream.range(0, matchingEnginesNum)
                .boxed()
                .collect(Collectors.toMap(
                        shardId -> shardId,
                        shardId -> CompletableFuture.supplyAsync(
                                () -> new MatchingEngineRouter(shardId, matchingEnginesNum, serializationProcessor, orderBookFactory, sharedPool, exchangeConfiguration),
                                loaderExecutor)));

        // TODO create processors in same thread we will execute it??

        // start creating risk engines
        final Map<Integer, CompletableFuture<RiskEngine>> riskEngineFutures = IntStream.range(0, riskEnginesNum)
                .boxed()
                .collect(Collectors.toMap(
                        shardId -> shardId,
                        shardId -> CompletableFuture.supplyAsync(
                                () -> new RiskEngine(shardId, riskEnginesNum, serializationProcessor, sharedPool, exchangeConfiguration),
                                loaderExecutor)));

        final EventHandler<OrderCommand>[] matchingEngineHandlers = matchingEngineFutures.values().stream()
                .map(CompletableFuture::join)
                .map(mer -> (EventHandler<OrderCommand>) (cmd, seq, eob) -> mer.processOrder(seq, cmd))
                .toArray(ExchangeCore::newEventHandlersArray);

        final Map<Integer, RiskEngine> riskEngines = riskEngineFutures.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().join()));


        final List<TwoStepMasterProcessor> procR1 = new ArrayList<>(riskEnginesNum);
        final List<TwoStepSlaveProcessor> procR2 = new ArrayList<>(riskEnginesNum);

        // 1. grouping processor (G)
        final EventHandlerGroup<OrderCommand> afterGrouping =
                disruptor.handleEventsWith((rb, bs) -> new GroupingProcessor(rb, rb.newBarrier(bs), perfCfg, coreWaitStrategy, sharedPool));

        // 2. [journaling (J)] in parallel with risk hold (R1) + matching engine (ME)

        boolean enableJournaling = serializationCfg.isEnableJournaling();
        final EventHandler<OrderCommand> jh = enableJournaling ? serializationProcessor::writeToJournal : null;

        if (enableJournaling) {
            afterGrouping.handleEventsWith(jh);
        }

        riskEngines.forEach((idx, riskEngine) -> afterGrouping.handleEventsWith(
                (rb, bs) -> {
                    final TwoStepMasterProcessor r1 = new TwoStepMasterProcessor(rb, rb.newBarrier(bs), riskEngine::preProcessCommand, exceptionHandler, coreWaitStrategy, "R1_" + idx);
                    procR1.add(r1);
                    return r1;
                }));

        disruptor.after(procR1.toArray(new TwoStepMasterProcessor[0])).handleEventsWith(matchingEngineHandlers);

        // 3. risk release (R2) after matching engine (ME)
        final EventHandlerGroup<OrderCommand> afterMatchingEngine = disruptor.after(matchingEngineHandlers);

        riskEngines.forEach((idx, riskEngine) -> afterMatchingEngine.handleEventsWith(
                (rb, bs) -> {
                    final TwoStepSlaveProcessor r2 = new TwoStepSlaveProcessor(rb, rb.newBarrier(bs), riskEngine::handlerRiskRelease, exceptionHandler, "R2_" + idx);
                    procR2.add(r2);
                    return r2;
                }));


        // 4. results handler (E) after matching engine (ME) + [journaling (J)]
        final EventHandlerGroup<OrderCommand> mainHandlerGroup = enableJournaling
                ? disruptor.after(arraysAddHandler(matchingEngineHandlers, jh))
                : afterMatchingEngine;

        final ResultsHandler resultsHandler = new ResultsHandler(resultsConsumer);

        mainHandlerGroup.handleEventsWith((cmd, seq, eob) -> {
            resultsHandler.onEvent(cmd, seq, eob);
            api.processResult(seq, cmd); // TODO SLOW ?(volatile operations)
        });

        // attach slave processors to master processor
        IntStream.range(0, riskEnginesNum).forEach(i -> procR1.get(i).setSlaveProcessor(procR2.get(i)));

        try {
            loaderExecutor.shutdown();
            loaderExecutor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    public synchronized void startup() {
        if (!started) {
            log.debug("Starting disruptor...");
            disruptor.start();
            started = true;

            serializationProcessor.replayJournalFullAndThenEnableJouraling(exchangeConfiguration.getInitStateCfg(), api);
        }
    }

    /**
     * Provides ExchangeApi instance.
     *
     * @return ExchangeApi instance (always same object)
     */
    public ExchangeApi getApi() {
        return api;
    }

    private static final EventTranslator<OrderCommand> SHUTDOWN_SIGNAL_TRANSLATOR = (cmd, seq) -> {
        cmd.command = OrderCommandType.SHUTDOWN_SIGNAL;
        cmd.resultCode = CommandResultCode.NEW;
    };

    /**
     * shut down disruptor
     */
    public synchronized void shutdown() {
        shutdown(-1, TimeUnit.MILLISECONDS);
    }

    /**
     * Will throw IllegalStateException if an exchange core can not stop gracefully.
     *
     * @param timeout  the amount of time to wait for all events to be processed. <code>-1</code> will give an infinite timeout
     * @param timeUnit the unit the timeOut is specified in
     */
    public synchronized void shutdown(final long timeout, final TimeUnit timeUnit) {
        if (!stopped) {
            stopped = true;
            // TODO stop accepting new events first
            try {
                log.info("Shutdown disruptor...");
                disruptor.getRingBuffer().publishEvent(SHUTDOWN_SIGNAL_TRANSLATOR);
                disruptor.shutdown(timeout, timeUnit);
                log.info("Disruptor stopped");
            } catch (TimeoutException e) {
                throw new IllegalStateException("could not stop a disruptor gracefully. Not all events may be executed.");
            }
        }
    }

    private static EventHandler<OrderCommand>[] arraysAddHandler(EventHandler<OrderCommand>[] handlers, EventHandler<OrderCommand> extraHandler) {
        final EventHandler<OrderCommand>[] result = Arrays.copyOf(handlers, handlers.length + 1);
        result[handlers.length] = extraHandler;
        return result;
    }

    @SuppressWarnings(value = {"unchecked"})
    private static EventHandler<OrderCommand>[] newEventHandlersArray(int size) {
        return new EventHandler[size];
    }
}
