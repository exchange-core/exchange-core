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

import com.google.common.collect.Streams;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.common.CoreWaitStrategy;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.common.config.InitialStateConfiguration;
import exchange.core2.core.orderbook.IOrderBook;
import exchange.core2.core.processors.*;
import exchange.core2.core.processors.journaling.IJournalingProcessor;
import exchange.core2.core.processors.journaling.ISerializationProcessor;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.ObjLongConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public final class ExchangeCore {

    private final Disruptor<OrderCommand> disruptor;

    private final ExchangeApi api;

    // core can be started and stopped only once
    private boolean started = false;
    private boolean stopped = false;

    // enable MatcherTradeEvent pooling
    public static boolean EVENTS_POOLING = false;

    @Builder
    public ExchangeCore(final ObjLongConsumer<OrderCommand> resultsConsumer,
                        final IJournalingProcessor journalingHandler,
                        final ISerializationProcessor serializationProcessor,
                        final int ringBufferSize,
                        final int matchingEnginesNum,
                        final int riskEnginesNum,
                        final int msgsInGroupLimit,
                        final ThreadFactory threadFactory,
                        final CoreWaitStrategy waitStrategy,
                        final BiFunction<CoreSymbolSpecification, ObjectsPool, IOrderBook> orderBookFactory,
                        final InitialStateConfiguration initialStateConfiguration) {

        if (msgsInGroupLimit >= ringBufferSize) {
            throw new IllegalArgumentException("msgsInGroupLimit should be less than ringBufferSize");
        }

        this.disruptor = new Disruptor<>(
                OrderCommand::new,
                ringBufferSize,
                threadFactory,
                ProducerType.MULTI, // multiple gateway threads are writing
                waitStrategy.create());

        this.api = new ExchangeApi(disruptor.getRingBuffer());

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

        // advice completable future to use the same socket as disruptor
        final ExecutorService loaderExecutor = Executors.newFixedThreadPool(matchingEnginesNum + riskEnginesNum, threadFactory);

        // start creating matching engines
        final Map<Integer, CompletableFuture<MatchingEngineRouter>> matchingEngineFutures = IntStream.range(0, matchingEnginesNum)
                .boxed()
                .collect(Collectors.toMap(
                        shardId -> shardId,
                        shardId -> CompletableFuture.supplyAsync(
                                () -> new MatchingEngineRouter(shardId, matchingEnginesNum, serializationProcessor, orderBookFactory, sharedPool, initialStateConfiguration.getSnapshotConf()),
                                loaderExecutor)));


        // start creating risk engines
        final Map<Integer, CompletableFuture<RiskEngine>> riskEngineFutures = IntStream.range(0, riskEnginesNum)
                .boxed()
                .collect(Collectors.toMap(
                        shardId -> shardId,
                        shardId -> CompletableFuture.supplyAsync(
                                () -> new RiskEngine(shardId, riskEnginesNum, serializationProcessor, sharedPool, initialStateConfiguration.getSnapshotConf()),
                                loaderExecutor)));

        final EventHandler<OrderCommand>[] matchingEngineHandlers = matchingEngineFutures.values().stream()
                .map(merFuture -> {
                    try {
                        return merFuture.get();
                    } catch (InterruptedException | ExecutionException ex) {
                        throw new RuntimeException(ex);
                    }
                })
                .map(mer -> (EventHandler<OrderCommand>) (cmd, seq, eob) -> mer.processOrder(cmd))
                .toArray(ExchangeCore::newEventHandlersArray);

        final Map<Integer, RiskEngine> riskEngines = riskEngineFutures.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> {
                            try {
                                return entry.getValue().get();
                            } catch (InterruptedException | ExecutionException ex) {
                                throw new RuntimeException(ex);
                            }
                        }));


        final List<TwoStepMasterProcessor> procR1 = new ArrayList<>(riskEnginesNum);
        final List<TwoStepSlaveProcessor> procR2 = new ArrayList<>(riskEnginesNum);

        // 1. grouping processor (G)
        final EventHandlerGroup<OrderCommand> afterGrouping =
                disruptor.handleEventsWith((rb, bs) -> new GroupingProcessor(rb, rb.newBarrier(bs), msgsInGroupLimit, waitStrategy, sharedPool));

        // 2. [journaling (J)] in parallel with risk hold (R1) + matching engine (ME)

        final EventHandler<OrderCommand> jh =  journalingHandler != null ? journalingHandler::onEvent : null;
        if (jh != null) {
            afterGrouping.handleEventsWith(jh);
        }

        riskEngines.forEach((idx, riskEngine) -> afterGrouping.handleEventsWith(
                (rb, bs) -> {
                    final TwoStepMasterProcessor r1 = new TwoStepMasterProcessor(rb, rb.newBarrier(bs), riskEngine::preProcessCommand, exceptionHandler, waitStrategy, "R" + idx);
                    procR1.add(r1);
                    return r1;
                }));

        disruptor.after(procR1.toArray(new TwoStepMasterProcessor[0])).handleEventsWith(matchingEngineHandlers);

        // 3. risk release (R2) after matching engine (ME)
        final EventHandlerGroup<OrderCommand> afterMatchingEngine = disruptor.after(matchingEngineHandlers);

        riskEngines.forEach((idx, riskEngine) -> afterMatchingEngine.handleEventsWith(
                (rb, bs) -> {
                    final TwoStepSlaveProcessor r2 = new TwoStepSlaveProcessor(rb, rb.newBarrier(bs), riskEngine::handlerRiskRelease, exceptionHandler);
                    procR2.add(r2);
                    return r2;
                }));

        // 4. results handler (E) after matching engine (ME) + [journaling (J)]
        (jh != null ? disruptor.after(ArrayUtils.add(matchingEngineHandlers, jh)) : afterMatchingEngine)
                .handleEventsWith((cmd, seq, eob) -> {
                    resultsConsumer.accept(cmd, seq);
                    api.processResult(seq, cmd); // TODO SLOW ?(volatile operations)
                });

        // attach slave processors to master processor
        Streams.forEachPair(procR1.stream(), procR2.stream(), TwoStepMasterProcessor::setSlaveProcessor);

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
        }
    }

    public ExchangeApi getApi() {
        return api;
    }

    private static final EventTranslator<OrderCommand> SHUTDOWN_SIGNAL_TRANSLATOR = (cmd, seq) -> {
        cmd.command = OrderCommandType.SHUTDOWN_SIGNAL;
        cmd.resultCode = CommandResultCode.NEW;
    };

    public synchronized void shutdown() {
        if (!stopped) {
            stopped = true;
            // TODO stop accepting new events first
            log.info("Shutdown disruptor...");
            disruptor.getRingBuffer().publishEvent(SHUTDOWN_SIGNAL_TRANSLATOR);
            disruptor.shutdown();
            log.info("Disruptor stopped");
        }
    }

    @SuppressWarnings(value = {"unchecked"})
    private static EventHandler<OrderCommand>[] newEventHandlersArray(int size) {
        return new EventHandler[size];
    }
}
