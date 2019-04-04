package org.openpredict.exchange.core.biprocessor;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.Sequencer;

import java.lang.reflect.Field;

public final class WaitSpinningHelper {

    private final SequenceBarrier sequenceBarrier;
    private final Sequencer sequencer;

    private final int spinLimit;

    public <T> WaitSpinningHelper(RingBuffer<T> ringBuffer, SequenceBarrier sequenceBarrier, int spinLimit) {
        this.sequenceBarrier = sequenceBarrier;
        this.spinLimit = spinLimit;
        this.sequencer = extractSequencer(ringBuffer);
    }

    public long tryWaitFor(final long seq) throws AlertException {
        sequenceBarrier.checkAlert();

        // TODO optimize
        long spin = spinLimit;
        long availableSequence;
        do {
            availableSequence = sequenceBarrier.getCursor();
        } while (availableSequence < seq && spin-- > 0);

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
