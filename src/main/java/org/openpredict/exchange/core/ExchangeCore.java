package org.openpredict.exchange.core;

import com.google.common.collect.Streams;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;
import org.openpredict.exchange.beans.CfgWaitStrategyType;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.beans.cmd.OrderCommandType;
import org.openpredict.exchange.core.biprocessor.GroupingProcessor;
import org.openpredict.exchange.core.biprocessor.MasterProcessor;
import org.openpredict.exchange.core.biprocessor.SlaveProcessor;
import org.openpredict.exchange.core.journalling.JournallingProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RequiredArgsConstructor
@Slf4j
public final class ExchangeCore {

    private static final boolean THREAD_AFFINITY_ENABLE = true;
    private static final boolean THREAD_AFFINITY_PER_CORE = false;

    private static final int RING_BUFFER_SIZE = 64 * 1024;
    private static final int RISK_ENGINES_NUM = 2;

    private final Disruptor<OrderCommand> disruptor;

    private final RingBuffer<OrderCommand> cmdRingBuffer;

    public ExchangeCore(Consumer<OrderCommand> resultsConsumer) {
        this(resultsConsumer, null);
    }

    public ExchangeCore(Consumer<OrderCommand> resultsConsumer, JournallingProcessor journallingHandler) {

        ThreadFactory threadFactory = eventProcessor -> new Thread(() -> {
            try (AffinityLock lock = THREAD_AFFINITY_PER_CORE ? AffinityLock.acquireCore() : AffinityLock.acquireLock()) {
                log.debug("{} pinned to {}", Thread.currentThread(), lock.cpuId());
                eventProcessor.run();
            }
        });

        this.disruptor = new Disruptor<>(
                OrderCommand::new,
                RING_BUFFER_SIZE,
                THREAD_AFFINITY_ENABLE ? threadFactory : Executors.defaultThreadFactory(),
                ProducerType.MULTI, // multiple gateway threads are writing
                CfgWaitStrategyType.BUSY_SPIN.create());

        this.cmdRingBuffer = disruptor.getRingBuffer();

        final DisruptorExceptionHandler<OrderCommand> exceptionHandler = new DisruptorExceptionHandler<>("main", (ex, seq) -> {
            log.error("Exception thrown on sequence={}", seq, ex);
            // TODO re-throw exception on publishing
            cmdRingBuffer.publishEvent(SHUTDOWN_SIGNAL_TRANSLATOR);
            disruptor.shutdown();
        });

        disruptor.setDefaultExceptionHandler(exceptionHandler);

        final MatchingEngineRouter matchingEngineRouter = new MatchingEngineRouter();

        final EventHandler<OrderCommand> matchingEngineHandler = (cmd, seq, eob) -> matchingEngineRouter.processOrder(cmd);

        final List<RiskEngine> riskEngines = IntStream.range(0, RISK_ENGINES_NUM)
                .mapToObj(i -> new RiskEngine(i, RISK_ENGINES_NUM - 1))
                .collect(Collectors.toList());

        final List<MasterProcessor> procR1 = new ArrayList<>();
        final List<SlaveProcessor> procR2 = new ArrayList<>();

        // 1. grouping processor (G)
        final EventHandlerGroup<OrderCommand> afterGrouping =
                disruptor.handleEventsWith((rb, bs) -> new GroupingProcessor(rb, rb.newBarrier(bs)));

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

        disruptor.after(procR1.toArray(new MasterProcessor[0])).handleEventsWith(matchingEngineHandler);

        // 3. results handler (E) after matching engine (ME) + [journalling (J)]
        // 4. risk release (R2) after matching engine (ME)
        EventHandlerGroup<OrderCommand> afterMatchingEngine = disruptor.after(matchingEngineHandler);

        riskEngines.forEach(riskEngine -> afterMatchingEngine.handleEventsWith(
                (rb, bs) -> {
                    final SlaveProcessor r2 = new SlaveProcessor(rb, rb.newBarrier(bs), riskEngine::handlerRiskRelease, exceptionHandler);
                    procR2.add(r2);
                    return r2;
                }));

        Streams.forEachPair(procR1.stream(), procR2.stream(), MasterProcessor::setSlaveProcessor);

        (journallingHandler != null ? disruptor.after(matchingEngineHandler, journallingHandler) : afterMatchingEngine)
                .handleEventsWith((cmd, seq, eob) -> resultsConsumer.accept(cmd));

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
}
