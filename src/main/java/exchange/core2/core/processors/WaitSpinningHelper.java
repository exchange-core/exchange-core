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
import exchange.core2.core.utils.ReflectionUtils;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

@Slf4j
public final class WaitSpinningHelper {

    private final SequenceBarrier sequenceBarrier;
    private final Sequencer sequencer;

    private final int spinLimit;
    private final int yieldLimit;

    // blocking mode, using same locking objects that Disruptor operates with
    private final boolean block;
    private final BlockingWaitStrategy blockingDiruptorWaitStrategy;
    private final Lock lock;
    private final Condition processorNotifyCondition;
    // next Disruptor release will have mutex (to avoid allocations)
    // private final Object mutex;

    public <T> WaitSpinningHelper(RingBuffer<T> ringBuffer, SequenceBarrier sequenceBarrier, int spinLimit, CoreWaitStrategy waitStrategy) {
        this.sequenceBarrier = sequenceBarrier;
        this.spinLimit = spinLimit;
        this.sequencer = extractSequencer(ringBuffer);
        this.yieldLimit = waitStrategy.isYield() ? spinLimit / 2 : 0;

        this.block = waitStrategy.isBlock();
        this.blockingDiruptorWaitStrategy = (BlockingWaitStrategy) CoreWaitStrategy.BLOCKING.getDisruptorWaitStrategy();
        this.lock = ReflectionUtils.extractField(BlockingWaitStrategy.class, blockingDiruptorWaitStrategy, "lock");
        this.processorNotifyCondition = ReflectionUtils.extractField(BlockingWaitStrategy.class, blockingDiruptorWaitStrategy, "processorNotifyCondition");
        //this.mutex = extractBlockingMutex(blockingDiruptorWaitStrategy);
    }

    public long tryWaitFor(final long seq) throws AlertException, InterruptedException {
        sequenceBarrier.checkAlert();

        long spin = spinLimit;
        long availableSequence;
        while ((availableSequence = sequenceBarrier.getCursor()) < seq && spin > 0) {
            if (spin < yieldLimit && spin > 1) {
                Thread.yield();
            } else if (block) {
/*
                synchronized (mutex) {
                    sequenceBarrier.checkAlert();
                    mutex.wait();
                }
*/
                lock.lock();
                try {
                    sequenceBarrier.checkAlert();
                    processorNotifyCondition.await();
                } finally {
                    lock.unlock();
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
            final Field f = ReflectionUtils.getField(RingBuffer.class, "sequencer");
            f.setAccessible(true);
            return (Sequencer) f.get(ringBuffer);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException("Can not access Disruptor internals: ", e);
        }
    }
}
