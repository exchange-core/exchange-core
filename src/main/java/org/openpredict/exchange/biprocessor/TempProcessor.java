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
package org.openpredict.exchange.biprocessor;

import com.lmax.disruptor.*;
import lombok.Setter;

import java.util.concurrent.atomic.AtomicInteger;

public final class TempProcessor<T> implements EventProcessor {
    private static final int IDLE = 0;
    private static final int HALTED = IDLE + 1;
    private static final int RUNNING = HALTED + 1;

    private final AtomicInteger running = new AtomicInteger(IDLE);
    private final DataProvider<T> dataProvider;
    private final SequenceBarrier sequenceBarrier;
    private final RefusingEventHandler<? super T> eventHandler;
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

    @Setter
    private SlaveProcessor<?> slaveProcessor;

    public TempProcessor(final DataProvider<T> dataProvider, final SequenceBarrier sequenceBarrier,
                         final RefusingEventHandler<? super T> eventHandler) {
        this.dataProvider = dataProvider;
        this.sequenceBarrier = sequenceBarrier;
        this.eventHandler = eventHandler;
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

            //notifyStart();
            try {
                if (running.get() == RUNNING) {
                    processEvents();
                }
            } finally {
                //notifyShutdown();
                running.set(IDLE);
            }
        } else {
            // This is a little bit of guess work.  The running state could of changed to HALTED by
            // this point.  However, Java does not have compareAndExchange which is the only way
            // to get it exactly correct.
            if (running.get() == RUNNING) {
                throw new IllegalStateException("Thread is already running");
            } else {
                //earlyExit();
            }
        }
    }

    private void processEvents() {
        long nextSequence = sequence.get() + 1L;

        // wait until slave processor has instruced to run
        while (!slaveProcessor.isRunning()) {
            Thread.yield();
        }

        //T event;
        while (true) {
            try {

                // should spin and also check another barrier
                long availableSequence = sequenceBarrier.tryWaitFor(nextSequence, 5000);

                if (availableSequence >= nextSequence) {
                    while (nextSequence <= availableSequence) {
                        eventHandler.onEvent(dataProvider.get(nextSequence), nextSequence);
//                        event = dataProvider.get(nextSequence);
//                        if (!eventHandler.onEvent(event, nextSequence)) {
//                            availableSequence = nextSequence - 1;
//                            break;
//                        }

                        nextSequence++;
                    }
                    sequence.set(availableSequence);
                }
            } catch (final TimeoutException e) {
                //
            } catch (final AlertException ex) {
                if (running.get() != RUNNING) {
                    break;
                }
            } catch (final Throwable ex) {
                //exceptionHandler.handleEventException(ex, nextSequence, event);
                sequence.set(nextSequence);
                nextSequence++;
            }

            slaveProcessor.handlingCycle();
        }
    }


}