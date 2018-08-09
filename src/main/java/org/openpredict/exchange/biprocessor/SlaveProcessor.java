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
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public final class SlaveProcessor<T> implements EventProcessor {
    private static final int IDLE = 0;
    private static final int HALTED = IDLE + 1;
    private static final int RUNNING = HALTED + 1;

    private final AtomicInteger running = new AtomicInteger(IDLE);
    private final DataProvider<T> dataProvider;
    private final SequenceBarrier sequenceBarrier;
    private final SimpleEventHandler<? super T> eventHandler;
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

    private long nextSequence = -1;

    public SlaveProcessor(DataProvider<T> dataProvider, SequenceBarrier sequenceBarrier, SimpleEventHandler<? super T> eventHandler) {
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
        } else if (running.get() == RUNNING) {
            throw new IllegalStateException("Thread is already running");
        }

        nextSequence = sequence.get() + 1L;
    }

    public void handlingCycle(long processUpTo) {
//        log.debug(" SLAVE processUpTo: {}", processUpTo);
        while (true) {
            try {
//                log.debug(" SLAVE tryWaitFor: {}", nextSequence);
                long availableSequence = sequenceBarrier.tryWaitFor(nextSequence, 0);
//                log.debug(" SLAVE availableSequence: {}", availableSequence);

//                Thread.sleep(100);

                // process batch
                while (nextSequence <= availableSequence && nextSequence < processUpTo) {
                    eventHandler.onEvent(dataProvider.get(nextSequence));
                    nextSequence++;
                }

                // exit if processed all group
                if (nextSequence == processUpTo) {
                    sequence.set(nextSequence - 1);
                    return;
                }

                sequence.set(availableSequence);

            } catch (final TimeoutException e) {
                //
            } catch (final Throwable ex) {
                //exceptionHandler.handleEventException(ex, nextSequence, event);
                sequence.set(nextSequence);
                nextSequence++;
            }
        }
    }


}