package org.openpredict.exchange.biprocessor;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.Sequencer;

import java.lang.reflect.Field;

public class WaitSpinningHelper {

    private final SequenceBarrier sequenceBarrier;
    private final Sequence dependentSequence;
    private final Sequencer sequencer;

    private final int spinLimit;

    public WaitSpinningHelper(SequenceBarrier sequenceBarrier, int spinLimit) {

        this.spinLimit = spinLimit;
        this.sequenceBarrier = sequenceBarrier;
        try {
            Field f;

            f = sequenceBarrier.getClass().getDeclaredField("dependentSequence");
            f.setAccessible(true);
            dependentSequence = (Sequence) f.get(sequenceBarrier);

            f = sequenceBarrier.getClass().getDeclaredField("sequencer");
            f.setAccessible(true);
            sequencer = (Sequencer) f.get(sequenceBarrier);

        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException("Can not access Disruptor internals: ", e);
        }
    }

    public long tryWaitFor(final long sequence) throws AlertException {

        sequenceBarrier.checkAlert();

        int c = spinLimit;
        long availableSequence;
        while ((availableSequence = dependentSequence.get()) < sequence
                && c-- > 0) {
            sequenceBarrier.checkAlert();
        }

        return (availableSequence >= sequence)
                ? sequencer.getHighestPublishedSequence(sequence, availableSequence)
                : availableSequence;
    }


}
