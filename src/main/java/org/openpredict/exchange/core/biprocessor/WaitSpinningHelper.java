package org.openpredict.exchange.core.biprocessor;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.Sequencer;
import org.openpredict.exchange.core.CoreWaitStrategy;

import java.lang.reflect.Field;
import java.util.concurrent.locks.LockSupport;

public final class WaitSpinningHelper {

    private static final long SLEEP_NS = 200;

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


    public static <T> Sequencer extractSequencer(RingBuffer<T> ringBuffer) {
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
