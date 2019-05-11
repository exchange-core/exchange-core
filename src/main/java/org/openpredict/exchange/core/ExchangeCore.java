package org.openpredict.exchange.core;

import com.google.common.collect.Streams;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.beans.cmd.OrderCommandType;
import org.openpredict.exchange.core.biprocessor.GroupingProcessor;
import org.openpredict.exchange.core.biprocessor.MasterProcessor;
import org.openpredict.exchange.core.biprocessor.SlaveProcessor;
import org.openpredict.exchange.core.journalling.JournallingProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public final class ExchangeCore {

    private final Disruptor<OrderCommand> disruptor;

    private final RingBuffer<OrderCommand> cmdRingBuffer;

    @Builder
    public ExchangeCore(final Consumer<OrderCommand> resultsConsumer,
                        final JournallingProcessor journallingHandler,
                        final int ringBufferSize,
                        final int matchingEnginesNum,
                        final int riskEnginesNum,
                        final int msgsInGroupLimit,
                        final Utils.ThreadAffityMode threadAffityMode,
                        final DisruptorWaitStrategy waitStrategy) {

        this.disruptor = new Disruptor<>(
                OrderCommand::new,
                ringBufferSize,
                Utils.affinedThreadFactory(threadAffityMode),
                ProducerType.MULTI, // multiple gateway threads are writing
                waitStrategy.create());

        this.cmdRingBuffer = disruptor.getRingBuffer();

        // creating and attaching exceptions handler
        final DisruptorExceptionHandler<OrderCommand> exceptionHandler = new DisruptorExceptionHandler<>("main", (ex, seq) -> {
            log.error("Exception thrown on sequence={}", seq, ex);
            // TODO re-throw exception on publishing
            cmdRingBuffer.publishEvent(SHUTDOWN_SIGNAL_TRANSLATOR);
            disruptor.shutdown();
        });

        disruptor.setDefaultExceptionHandler(exceptionHandler);

        // creating matching engine event handlers array
        final EventHandler<OrderCommand>[] matchingEngineHandlers = IntStream.range(0, matchingEnginesNum)
                .mapToObj(shardId -> {
                    final MatchingEngineRouter router = new MatchingEngineRouter(shardId, matchingEnginesNum);
                    return (EventHandler<OrderCommand>) (cmd, seq, eob) -> router.processOrder(cmd);
                })
                .toArray(ExchangeCore::newEventHandlersArray);

        // creating risk engines array
        final List<RiskEngine> riskEngines = IntStream.range(0, riskEnginesNum)
                .mapToObj(shardId -> new RiskEngine(shardId, riskEnginesNum))
                .collect(Collectors.toList());

        final List<MasterProcessor> procR1 = new ArrayList<>(riskEnginesNum);
        final List<SlaveProcessor> procR2 = new ArrayList<>(riskEnginesNum);

        // 1. grouping processor (G)
        final EventHandlerGroup<OrderCommand> afterGrouping =
                disruptor.handleEventsWith((rb, bs) -> new GroupingProcessor(rb, rb.newBarrier(bs), msgsInGroupLimit));

        // 2. [journalling (J)] in parallel with risk hold (R1) + matching engine (ME)
        if (journallingHandler != null) {
            afterGrouping.handleEventsWith(journallingHandler);
        }

        riskEngines.forEach(riskEngine -> afterGrouping.handleEventsWith(
                (rb, bs) -> {
                    final MasterProcessor r1 = new MasterProcessor(rb, rb.newBarrier(bs), riskEngine::preProcessCommand, exceptionHandler);
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
        (journallingHandler != null ? disruptor.after(ArrayUtils.add(matchingEngineHandlers, journallingHandler)) : afterMatchingEngine)
                .handleEventsWith((cmd, seq, eob) -> resultsConsumer.accept(cmd));

        // attach slave processors to master processor
        Streams.forEachPair(procR1.stream(), procR2.stream(), MasterProcessor::setSlaveProcessor);

    }

    public void startup() {
        log.debug("Starting disruptor...");
        disruptor.start();
    }

    public ExchangeApi getApi() {
        return new ExchangeApi(this);
    }

    public RingBuffer<OrderCommand> getRingBuffer() {
        return cmdRingBuffer;
    }

    private static final EventTranslator<OrderCommand> SHUTDOWN_SIGNAL_TRANSLATOR = (cmd, seq) -> {
        cmd.command = OrderCommandType.SHUTDOWN_SIGNAL;
        cmd.resultCode = CommandResultCode.NEW;
    };

    public void shutdown() {
        // TODO stop accepting new events first
        log.info("Shutdown disruptor...");
        cmdRingBuffer.publishEvent(SHUTDOWN_SIGNAL_TRANSLATOR);
        disruptor.shutdown();
        log.info("Disruptor stopped");
    }

    @SuppressWarnings(value = {"unchecked"})
    private static EventHandler<OrderCommand>[] newEventHandlersArray(int size) {
        return new EventHandler[size];
    }

    @RequiredArgsConstructor
    public enum DisruptorWaitStrategy {
        BUSY_SPIN(BusySpinWaitStrategy::new),
        YIELDING(YieldingWaitStrategy::new),
        SLEEPING(SleepingWaitStrategy::new);

        private final Supplier<WaitStrategy> supplier;

        public WaitStrategy create() {
            return supplier.get();
        }
    }

}
