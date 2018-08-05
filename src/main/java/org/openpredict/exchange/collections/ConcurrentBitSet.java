package org.openpredict.exchange.collections;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLongArray;

@Slf4j
public class ConcurrentBitSet {
    private final AtomicLongArray words;
    private final int length;

    private static final long WORD_MASK = 0xffffffffffffffffL;
    private static final int ADDRESS_BITS_PER_WORD = 6;
    private static final int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;

    public ConcurrentBitSet(int length) {
        words = new AtomicLongArray((length + 63) >>> ADDRESS_BITS_PER_WORD);
        this.length = length;
    }

    public void set(int n) {
        if (n >= length || n < 0) {
            throw new IndexOutOfBoundsException();
        }
        int wordIndex = wordIndex(n);
        long bitIndex = 1L << n; // assuming automatic '& 63';
        long expectedValue, newValue;
//        log.debug("set {}: wordIndex={} bitIndex={}", n, wordIndex, bitIndex);
        do {
            newValue = (expectedValue = words.get(wordIndex)) | bitIndex;
            // will leave if bit is already set, or if CAS operation successful
        } while (expectedValue != newValue && !words.compareAndSet(wordIndex, expectedValue, newValue));
    }


    public void clear(int n) {
        if (n >= length || n < 0) {
            throw new IndexOutOfBoundsException();
        }
        int wordIndex = wordIndex(n);
        long bitMask = ~(1L << n); // assuming automatic '& 63';
        long expectedValue, newValue;
        do {
            newValue = (expectedValue = words.get(wordIndex)) & bitMask;
            // will leave if bit is already cleared, or if CAS operation successful
        } while (expectedValue != newValue && !words.compareAndSet(wordIndex, expectedValue, newValue));
    }


    public boolean get(int n) {
        return (words.get(wordIndex(n)) & (1L << n)) != 0;
    }

    /**
     *
     */
    public int nextSetBit(int fromIndex) {

        if (fromIndex >= length || fromIndex < 0) {
            throw new IndexOutOfBoundsException();
        }

        int u = wordIndex(fromIndex);

        long word = words.get(u) & (WORD_MASK << fromIndex);

        while (true) {
            if (word != 0) {
                return (u * BITS_PER_WORD) + Long.numberOfTrailingZeros(word);
            }
            if (++u == words.length()) {
                return -1;
            }
            word = words.get(u);
        }
    }

    private static int wordIndex(int bitIndex) {
        return bitIndex >> ADDRESS_BITS_PER_WORD;
    }

}