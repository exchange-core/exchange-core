/*
 * Copyright 2011 LMAX Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package exchange.core2.core.biprocessor;

import com.lmax.disruptor.*;
import exchange.core2.core.common.CoreWaitStrategy;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public final class TwoStepMasterProcessor implements EventProcessor {
    private static final int IDLE = 0;
    private static final int HALTED = IDLE + 1;
    private static final int RUNNING = HALTED + 1;

    private static final int MASTER_SPIN_LIMIT = 5000;

    private final AtomicInteger running = new AtomicInteger(IDLE);
    private final DataProvider<OrderCommand> dataProvider;
    private final SequenceBarrier sequenceBarrier;
    private final WaitSpinningHelper waitSpinningHelper;
    private final SimpleEventHandler eventHandler;
    private final ExceptionHandler<OrderCommand> exceptionHandler;
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

    @Setter
    private TwoStepSlaveProcessor slaveProcessor;

    public TwoStepMasterProcessor(final RingBuffer<OrderCommand> ringBuffer,
                                  final SequenceBarrier sequenceBarrier,
                                  final SimpleEventHandler eventHandler,
                                  final ExceptionHandler<OrderCommand> exceptionHandler,
                                  final CoreWaitStrategy coreWaitStrategy) {
        this.dataProvider = ringBuffer;
        this.sequenceBarrier = sequenceBarrier;
        this.waitSpinningHelper = new WaitSpinningHelper(ringBuffer, sequenceBarrier, MASTER_SPIN_LIMIT, coreWaitStrategy);
        this.eventHandler = eventHandler;
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public Sequence getSequence() {
        return sequence;
    }

    @Override
    public void halt() {
        running.set(HALTED);
        sequenceBarrier.alert();
    }

    @Override
    public boolean isRunning() {
        return running.get() != IDLE;
    }


    /**
     * It is ok to have another thread rerun this method after a halt().
     *
     * @throws IllegalStateException if this object instance is already running in a thread
     */
    @Override
    public void run() {
        if (running.compareAndSet(IDLE, RUNNING)) {
            sequenceBarrier.clearAlert();

            try {
                if (running.get() == RUNNING) {
                    processEvents();
                }
            } finally {
                running.set(IDLE);
            }
        } else {
            // This is a little bit of guess work.  The running state could of changed to HALTED by
            // this point.  However, Java does not have compareAndExchange which is the only way
            // to get it exactly correct.
            if (running.get() == RUNNING) {
                throw new IllegalStateException("Thread is already running");
            }
        }
    }

    private void processEvents() {
        long nextSequence = sequence.get() + 1L;

        long currentSequenceGroup = 0;

        // wait until slave processor has instructed to run
        while (!slaveProcessor.isRunning()) {
            Thread.yield();
        }

        while (true) {
            OrderCommand cmd = null;
            try {

                // should spin and also check another barrier
                final long availableSequence = waitSpinningHelper.tryWaitFor(nextSequence);

                if (availableSequence >= nextSequence) {
                    while (nextSequence <= availableSequence) {
                        cmd = dataProvider.get(nextSequence);

                        // switch to next group - let slave processor to do a handling cycle
                        if (cmd.eventsGroup != currentSequenceGroup) {
                            sequence.set(nextSequence - 1);
                            slaveProcessor.handlingCycle(nextSequence);
                            currentSequenceGroup = cmd.eventsGroup;
                        }

                        boolean forcedPublish = eventHandler.onEvent(cmd);
                        nextSequence++;

                        if (forcedPublish) {
                            sequence.set(nextSequence - 1);
                        }

                        if (cmd.command == OrderCommandType.SHUTDOWN_SIGNAL) {
                            // having all sequences aligned with the ringbuffer cursor is a requirement for proper shutdown

                            // let following processors to catch up
                            sequence.set(nextSequence - 1);

                            // trigger slave processor
                            slaveProcessor.handlingCycle(nextSequence);
                        }
                    }
                    sequence.set(availableSequence);
                }
            } catch (final AlertException ex) {
                if (running.get() != RUNNING) {
                    break;
                }
            } catch (final Throwable ex) {
                exceptionHandler.handleEventException(ex, nextSequence, cmd);
                sequence.set(nextSequence);
                nextSequence++;
            }

        }
    }

}