package org.openpredict.exchange.collections;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.BitSet;
import java.util.Random;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

@Slf4j
public class ConcurrentBitSetTest {


    private final static int TEST_LENGTH = 20000;

    @Test
    public void shouldSetAndClearOnlySpecifiedBits() {

        ConcurrentBitSet cbs = new ConcurrentBitSet(TEST_LENGTH);
        BitSet bs = new BitSet();

        Random rand = new Random(1L);
        for (int i = 0; i < TEST_LENGTH; i++) {
            boolean b = rand.nextBoolean();
            if (b) {
                cbs.set(i);
                bs.set(i);
            }
        }

        for (int i = 0; i < TEST_LENGTH; i++) {
            //log.debug("{}. {} expected:{}", i, cbs.get(i), bs.get(i));
            assertThat(cbs.get(i), is(bs.get(i)));
        }


        for (int i = 0; i < TEST_LENGTH; i++) {
            boolean b = rand.nextBoolean();
            if (b) {
                cbs.clear(i);
                bs.clear(i);
            }
        }

        for (int i = 0; i < TEST_LENGTH; i++) {
            //log.debug("{}. {} expected:{}", i, cbs.get(i), bs.get(i));
            assertThat(cbs.get(i), is(bs.get(i)));
        }


    }

    @Test
    public void shouldFindSetBits() {

        Random rand = new Random(1L);

        for (int j = 2; j < TEST_LENGTH; j *= 2) {

            ConcurrentBitSet cbs = new ConcurrentBitSet(TEST_LENGTH);
            BitSet bs = new BitSet();
            for (int i = 0; i < TEST_LENGTH; i += j) {
                int idx = rand.nextInt(TEST_LENGTH);
                cbs.set(idx);
                bs.set(idx);
            }
//            log.info("{}: {}", j, bs);

            for (int i = 0; i < TEST_LENGTH; i++) {
                //log.debug("{}. {} expected:{}", i, cbs.get(i), bs.get(i));
                assertThat(cbs.get(i), is(bs.get(i)));
                assertThat(cbs.nextSetBit(i), is(bs.nextSetBit(i)));
            }

        }


    }

}