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
import net.openhft.affinity.AffinityLock;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.beans.cmd.OrderCommandType;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public final class GroupingProcessor implements EventProcessor {
    private static final int IDLE = 0;
    private static final int HALTED = IDLE + 1;
    private static final int RUNNING = HALTED + 1;

    private final AtomicInteger running = new AtomicInteger(IDLE);
    private final RingBuffer<OrderCommand> ringBuffer;
    private final SequenceBarrier sequenceBarrier;
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

    public GroupingProcessor(final RingBuffer<OrderCommand> ringBuffer, final SequenceBarrier sequenceBarrier) {
        this.ringBuffer = ringBuffer;
        this.sequenceBarrier = sequenceBarrier;
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
                    try (AffinityLock cpuLock = AffinityLock.acquireLock()) {
                        processEvents();
                    }
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

        long groupCounter = 0;
        long msgsInGroup = 0;

        long groupLastNs = 0;

        long ec = 0;

        //T event;
        while (true) {
            try {

                // should spin and also check another barrier
                //log.debug("tryWait");
                long availableSequence = sequenceBarrier.tryWaitFor(nextSequence, 1000);
                //long availableSequence = sequenceBarrier.waitFor(nextSequence);
                //log.debug("availableSequence={}", availableSequence);

                if (nextSequence <= availableSequence) {
                    while (nextSequence <= availableSequence) {

//                        if ((nextSequence & (1024 * 128L - 1)) == 0) {
//                            log.debug(">> nextSequence={} ec={}", nextSequence, ec);
//                        }

                        OrderCommand cmd = ringBuffer.get(nextSequence);
                        nextSequence++;

                        cmd.eventsGroup = groupCounter;

                        if (cmd.command == OrderCommandType.NOP) {
                            // gust set next group and pass
                            continue;
                        }

                        msgsInGroup++;

                        // switch group after each 8000 messages
                        if (msgsInGroup >= 192) {
                            groupCounter++;
                            msgsInGroup = 0;
                        }

                    }
                    sequence.set(availableSequence);
                    groupLastNs = System.nanoTime() + 1000;

                } else if (msgsInGroup > 0 && System.nanoTime() > groupLastNs) {
                    // switch group after each 2us since first message in the group

                    msgsInGroup = 0;
                    groupCounter++;

                    ec++;

//                    // generate new NOP message to trigger R2 processing
//                    sendNextGroupCmd();
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
        }
    }

    private void sendNextGroupCmd() {

        // TODO tryPublishEvent - slow as creates an exception object ??
        ringBuffer.tryPublishEvent((event, seq) -> {
            event.timestamp = -1;
            //event.orderId = 0;
            event.resultCode = CommandResultCode.NEW;
            event.uid = -1;
            event.command = OrderCommandType.NOP;
        });
    }


}