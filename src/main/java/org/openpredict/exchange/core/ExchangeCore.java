package org.openpredict.exchange.core;

import com.google.common.collect.Streams;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.openpredict.exchange.beans.CoreSymbolSpecification;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.beans.cmd.OrderCommandType;
import org.openpredict.exchange.core.biprocessor.GroupingProcessor;
import org.openpredict.exchange.core.biprocessor.MasterProcessor;
import org.openpredict.exchange.core.biprocessor.SlaveProcessor;
import org.openpredict.exchange.core.journalling.ISerializationProcessor;
import org.openpredict.exchange.core.journalling.JournallingProcessor;
import org.openpredict.exchange.core.orderbook.IOrderBook;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public final class ExchangeCore {

    private final Disruptor<OrderCommand> disruptor;

    private final ExchangeApi api;

    // core can be started and stopped only once
    private boolean started = false;
    private boolean stopped = false;

    @Builder
    public ExchangeCore(final BiConsumer<Long, OrderCommand> resultsConsumer,
                        final JournallingProcessor journallingHandler,
                        final ISerializationProcessor serializationProcessor,
                        final int ringBufferSize,
                        final int matchingEnginesNum,
                        final int riskEnginesNum,
                        final int msgsInGroupLimit,
                        final Utils.ThreadAffityMode threadAffityMode,
                        final CoreWaitStrategy waitStrategy,
                        final Function<CoreSymbolSpecification, IOrderBook> orderBookFactory,
                        final Long loadStateId) {

        this.disruptor = new Disruptor<>(
                OrderCommand::new,
                ringBufferSize,
                Utils.affinedThreadFactory(threadAffityMode),
                ProducerType.MULTI, // multiple gateway threads are writing
                waitStrategy.create());

        this.api = new ExchangeApi(disruptor.getRingBuffer());

        // creating and attaching exceptions handler
        final DisruptorExceptionHandler<OrderCommand> exceptionHandler = new DisruptorExceptionHandler<>("main", (ex, seq) -> {
            log.error("Exception thrown on sequence={}", seq, ex);
            // TODO re-throw exception on publishing
            disruptor.getRingBuffer().publishEvent(SHUTDOWN_SIGNAL_TRANSLATOR);
            disruptor.shutdown();
        });

        disruptor.setDefaultExceptionHandler(exceptionHandler);

        // creating matching engine event handlers array // TODO parallel deserialization
        final EventHandler<OrderCommand>[] matchingEngineHandlers = IntStream.range(0, matchingEnginesNum)
                .mapToObj(shardId -> {
                    final MatchingEngineRouter router = new MatchingEngineRouter(shardId, matchingEnginesNum, serializationProcessor, orderBookFactory, loadStateId);
                    return (EventHandler<OrderCommand>) (cmd, seq, eob) -> router.processOrder(cmd);
                })
                .toArray(ExchangeCore::newEventHandlersArray);

        // creating risk engines array // TODO parallel deserialization
        final List<RiskEngine> riskEngines = IntStream.range(0, riskEnginesNum)
                .mapToObj(shardId -> new RiskEngine(shardId, riskEnginesNum, serializationProcessor, loadStateId))
                .collect(Collectors.toList());

        final List<MasterProcessor> procR1 = new ArrayList<>(riskEnginesNum);
        final List<SlaveProcessor> procR2 = new ArrayList<>(riskEnginesNum);

        // 1. grouping processor (G)
        final EventHandlerGroup<OrderCommand> afterGrouping =
                disruptor.handleEventsWith((rb, bs) -> new GroupingProcessor(rb, rb.newBarrier(bs), msgsInGroupLimit, waitStrategy));

        // 2. [journalling (J)] in parallel with risk hold (R1) + matching engine (ME)
        if (journallingHandler != null) {
            afterGrouping.handleEventsWith(journallingHandler::onEvent);
        }

        riskEngines.forEach(riskEngine -> afterGrouping.handleEventsWith(
                (rb, bs) -> {
                    final MasterProcessor r1 = new MasterProcessor(rb, rb.newBarrier(bs), riskEngine::preProcessCommand, exceptionHandler, waitStrategy);
                    procR1.add(r1);
                    return r1;
                }));

        disruptor.after(procR1.toArray(new MasterProcessor[0])).handleEventsWith(matchingEngineHandlers);

        // 3. risk release (R2) after matching engine (ME)
        final EventHandlerGroup<OrderCommand> afterMatchingEngine = disruptor.after(matchingEngineHandlers);

        riskEngines.forEach(riskEngine -> afterMatchingEngine.handleEventsWith(
                (rb, bs) -> {
                    final SlaveProcessor r2 = new SlaveProcessor(rb, rb.newBarrier(bs), riskEngine::handlerRiskRelease, exceptionHandler);
                    procR2.add(r2);
                    return r2;
                }));

        // 4. results handler (E) after matching engine (ME) + [journalling (J)]
        (journallingHandler != null ? disruptor.after(ArrayUtils.add(matchingEngineHandlers, journallingHandler::onEvent)) : afterMatchingEngine)
                .handleEventsWith((cmd, seq, eob) -> {
                    resultsConsumer.accept(seq, cmd);
                    api.processResult(seq, cmd); // TODO SLOW ?(volatile operations)
                });

        // attach slave processors to master processor
        Streams.forEachPair(procR1.stream(), procR2.stream(), MasterProcessor::setSlaveProcessor);

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
