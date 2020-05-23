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
package exchange.core2.core.processors;

import com.lmax.disruptor.*;
import exchange.core2.core.common.CoreWaitStrategy;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;

@Slf4j
public final class WaitSpinningHelper {

    private final SequenceBarrier sequenceBarrier;
    private final Sequencer sequencer;

    private final int spinLimit;
    private final int yieldLimit;
    private final boolean block;

    private final Object mutex;

    private final BlockingWaitStrategy blockingDiruptorWaitStrategy;

    public <T> WaitSpinningHelper(RingBuffer<T> ringBuffer, SequenceBarrier sequenceBarrier, int spinLimit, CoreWaitStrategy waitStrategy) {
        this.sequenceBarrier = sequenceBarrier;
        this.spinLimit = spinLimit;
        this.sequencer = extractSequencer(ringBuffer);
        this.yieldLimit = waitStrategy.isYield() ? spinLimit / 2 : 0;
        this.block = waitStrategy.isBlock();
        this.blockingDiruptorWaitStrategy = (BlockingWaitStrategy) CoreWaitStrategy.BLOCKING.getDisruptorWaitStrategy();
        this.mutex = extractBlockingMutex(blockingDiruptorWaitStrategy);
    }

    public long tryWaitFor(final long seq) throws AlertException, InterruptedException {
        sequenceBarrier.checkAlert();

        long spin = spinLimit;
        long availableSequence;
        while ((availableSequence = sequenceBarrier.getCursor()) < seq && spin > 0) {
            if (spin < yieldLimit && spin > 1) {
                Thread.yield();
            } else if (block) {
                synchronized (mutex) {
                    sequenceBarrier.checkAlert();
                    mutex.wait();
                }
            }

            spin--;
        }

        return (availableSequence < seq)
                ? availableSequence
                : sequencer.getHighestPublishedSequence(seq, availableSequence);
    }

    public void signalAllWhenBlocking() {
        if (block) {
            blockingDiruptorWaitStrategy.signalAllWhenBlocking();
        }
    }

    private static <T> Sequencer extractSequencer(RingBuffer<T> ringBuffer) {
        try {
            final Field f = getField(RingBuffer.class, "sequencer");
            f.setAccessible(true);
            return (Sequencer) f.get(ringBuffer);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException("Can not access Disruptor internals: ", e);
        }
    }

    private static Object extractBlockingMutex(BlockingWaitStrategy blockingWaitStrategy) {
        try {
            final Field f = getField(BlockingWaitStrategy.class, "mutex");
            f.setAccessible(true);
            return f.get(blockingWaitStrategy);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException("Can not access Disruptor internals: ", e);
        }
    }

    private static Field getField(Class clazz, String fieldName) throws NoSuchFieldException {
        try {
            return clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            Class superClass = clazz.getSuperclass();
            if (superClass == null) {
                throw e;
            } else {
                return getField(superClass, fieldName);
            }
        }
    }


}
