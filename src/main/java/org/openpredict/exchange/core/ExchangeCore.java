package org.openpredict.exchange.core;

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

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

@RequiredArgsConstructor
@Slf4j
public final class ExchangeCore {

    private static final boolean THREAD_AFFINITY_ENABLE = true;
    private static final boolean THREAD_AFFINITY_PER_CORE = false;

    private static final int RING_BUFFER_SIZE = 64 * 1024;

    private final MatchingEngineRouter matchingEngineRouter = new MatchingEngineRouter();

    private final RiskEngine riskEngine = new RiskEngine();

    private final Disruptor<OrderCommand> disruptor;

    private final RingBuffer<OrderCommand> cmdRingBuffer;

    private MasterProcessor procR1;

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

        final EventHandler<OrderCommand> matchingEngineHandler = (cmd, seq, eob) -> matchingEngineRouter.processOrder(cmd);

//         1. grouping processor (G)
        EventHandlerGroup<OrderCommand> afterGrouping =
                disruptor.handleEventsWith((rb, bs) -> new GroupingProcessor(rb, rb.newBarrier(bs)));

        // 2. journalling (J) in parallel with risk hold (R1) + matching engine (ME)
        if (journallingHandler != null) {
            afterGrouping.handleEventsWith(journallingHandler);
        }

        afterGrouping.handleEventsWith((rb, bs) -> {
            procR1 = new MasterProcessor(rb, rb.newBarrier(bs), this::riskPreProcess, exceptionHandler);
            return procR1;
        }).then(matchingEngineHandler);

        // 3. results handler (E) after matching engine (ME) + [journalling (J)]
        // 4. risk release (R2) after matching engine (ME)
        EventHandlerGroup<OrderCommand> afterMatchingEngine = disruptor.after(matchingEngineHandler);
        afterMatchingEngine.then((rb, bs) -> {
            final SlaveProcessor procR2 = new SlaveProcessor(rb, rb.newBarrier(bs), this::handlerRiskRelease, exceptionHandler);
            procR1.setSlaveProcessor(procR2);
            return procR2;
        });

        (journallingHandler != null ? disruptor.after(matchingEngineHandler, journallingHandler) : afterMatchingEngine)
                .then((cmd, seq, eob) -> resultsConsumer.accept(cmd));

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

    /**
     * Pre-process command handler
     * 1. MOVE/CANCEL commands ignored, for specific uid marked as valid for matching engine
     * 2. PLACE ORDER checked with risk ending for specific uid
     * 3. ADD USER, BALANCE_ADJUSTMENT processed for specific uid, not valid for matching engine
     * 4. BINARY_DATA commands processed for ANY uid and marked as valid for matching engine TODO which handler marks?
     * 5. RESET commands processed for any uid
     * @param cmd - command
     */
    private void riskPreProcess(OrderCommand cmd) {

        switch (cmd.command) {
            case MOVE_ORDER:
            case CANCEL_ORDER:
            case ORDER_BOOK_REQUEST:
                // NO checks for UPDATE or CANCEL
                break;

            case PLACE_ORDER:
                cmd.resultCode = riskEngine.placeOrderRiskCheck(cmd);
                break;

            case ADD_USER:
                cmd.resultCode = riskEngine.getUserProfileService().addEmptyUserProfile(cmd.uid);
                break;

            case BALANCE_ADJUSTMENT:
                cmd.resultCode = riskEngine.getUserProfileService().balanceAdjustment(cmd.uid, cmd.symbol, cmd.price, cmd.orderId);
                break;

            case BINARY_DATA:
                riskEngine.getBinaryCommandsProcessor().binaryData(cmd);
                break;

            case RESET:
                resetState();
                break;

            case NOP:
                // TODO set only by processor 0
                cmd.resultCode = CommandResultCode.SUCCESS;
                break;
        }
    }

    private void handlerRiskRelease(final OrderCommand cmd) {
        riskEngine.handlerRiskRelease(cmd.symbol, cmd.marketData, cmd.matcherEvent);
    }

    private void resetState() {
        riskEngine.reset();
    }
}
