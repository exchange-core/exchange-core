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

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.Sequencer;
import exchange.core2.core.common.CoreWaitStrategy;

import java.lang.reflect.Field;
import java.util.concurrent.locks.LockSupport;

public final class WaitSpinningHelper {

    private static final long SLEEP_NS = 200_000; // 0.2ms

    private final SequenceBarrier sequenceBarrier;
    private final Sequencer sequencer;

    private final int spinLimit;
    private final int yieldLimit;
    private final boolean park;

    public <T> WaitSpinningHelper(RingBuffer<T> ringBuffer, SequenceBarrier sequenceBarrier, int spinLimit, CoreWaitStrategy waitStrategy) {
        this.sequenceBarrier = sequenceBarrier;
        this.spinLimit = spinLimit;
        this.sequencer = extractSequencer(ringBuffer);
        this.yieldLimit = waitStrategy.isYield() ? spinLimit / 2 : 0;
        this.park = waitStrategy.isPark();
    }

    public long tryWaitFor(final long seq) throws AlertException {
        sequenceBarrier.checkAlert();

        long spin = spinLimit;
        long availableSequence;
        while ((availableSequence = sequenceBarrier.getCursor()) < seq && spin > 0) {
            if (spin < yieldLimit && spin > 1) {
                Thread.yield();
            } else if (park) {
                LockSupport.parkNanos(SLEEP_NS);
            }

            spin--;
        }

        return (availableSequence < seq)
                ? availableSequence
                : sequencer.getHighestPublishedSequence(seq, availableSequence);
    }

    private static <T> Sequencer extractSequencer(RingBuffer<T> ringBuffer) {
        try {
            Field f = getField(RingBuffer.class, "sequencer");
            f.setAccessible(true);
            return (Sequencer) f.get(ringBuffer);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException("Can not access Disruptor internals: ", e);
        }
    }

    private static Field getField(Class clazz, String fieldName)
            throws NoSuchFieldException {
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
