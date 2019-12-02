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
package exchange.core2.core.art;

import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class LongAdaptiveRadixTreeMapTest {

    private LongAdaptiveRadixTreeMap<String> map;
    private TreeMap<Long, String> origMap;

    @Before
    public void before() {
        map = new LongAdaptiveRadixTreeMap<>();
        origMap = new TreeMap<>();
    }

    @Test
    public void test() {

        map.validateInternalState();
        assertNull(map.get(0));
        map.put(2, "two");
        map.validateInternalState();
        //assertThat(map.get(2), is("two"));
        map.put(223, "dds");
        map.put(49, "fn");
        map.put(1, "fn");

        System.out.println(String.format("11239847219L = %016X", 11239847219L));
        System.out.println(String.format("1123909L = %016X", 1123909L));
        map.put(Long.MAX_VALUE, "fn");
        map.put(11239847219L, "11239847219L");
        map.put(1123909L, "1123909L");
        map.put(11239837212L, "11239837212L");
        map.put(13213, "13213");
        map.put(13423, "13423");

        System.out.println(map.printDiagram());

        assertThat(map.get(223), is("dds"));
        assertThat(map.get(Long.MAX_VALUE), is("fn"));
        assertThat(map.get(11239837212L), is("11239837212L"));


        System.out.println(map.printDiagram());

    }


    @Test
    public void shouldExtendTo16andReduceTo4() {
        put(2, "2");
        put(223, "223");
        put(49, "49");
        put(1, "1");
        // 4->16
        put(77, "77");
        put(4, "4");

        remove(223);
        remove(1);
        // 16->4
        remove(4);
        remove(49);

        // reduce intermediate

        put(65536 * 7, "65536*7");
        put(65536 * 3, "65536*3");
        put(65536 * 2, "65536*2");
        // 4->16
        put(65536 * 4, "65536*4");
        put(65536 * 3 + 3, "65536*3+3");

        remove(65536 * 2);
        // 16->4
        remove(65536 * 4);
        remove(65536 * 7);
//        System.out.println(map.printDiagram());
    }

    @Test
    public void shouldExtendTo48andReduceTo16() {
        // reduce ay end level

        for (int i = 0; i < 16; i++) {
            put(i, "" + i);
        }
        // 16->48
        put(177, "177");
        put(56, "56");
        System.out.println(map.printDiagram());
        //put(177, "177");

//        remove(223);
//        remove(1);
//        // 16->4
//        remove(4);
//        remove(49);


        // reduce intermediate
//
//        put(65536 * 7, "65536*7");
//        put(65536 * 3, "65536*3");
//        put(65536 * 2, "65536*2");
//        // 4->16
//        put(65536 * 4, "65536*4");
//        put(65536 * 3 + 3, "65536*3+3");
//
//        remove(65536 * 2);
//        // 16->4
//        remove(65536 * 4);
//        remove(65536 * 7);
    }


    private void put(long key, String value) {
        map.put(key, value);
        map.validateInternalState();
        origMap.put(key, value);

//        map.entriesList().forEach(entry -> System.out.println("k=" + entry.getKey() + " v=" + entry.getValue()));
//        origMap.forEach((key1, value1) -> System.out.println("k1=" + key1 + " v1=" + value1));

        checkStreamsEqual(map.entriesList().stream(), origMap.entrySet().stream());
    }

    private void remove(long key) {
        map.remove(key);
        map.validateInternalState();
        origMap.remove(key);

//        map.entriesList().forEach(entry -> System.out.println("k=" + entry.getKey() + " v=" + entry.getValue()));
//        origMap.forEach((key1, value1) -> System.out.println("k1=" + key1 + " v1=" + value1));

        checkStreamsEqual(map.entriesList().stream(), origMap.entrySet().stream());
    }


    private static <K, V> void checkStreamsEqual(final Stream<Map.Entry<K, V>> entry, final Stream<Map.Entry<K, V>> origEntry) {
        final Iterator<Map.Entry<K, V>> iter = entry.iterator();
        final Iterator<Map.Entry<K, V>> origIter = origEntry.iterator();
        while (iter.hasNext() && origIter.hasNext()) {
            final Map.Entry<K, V> next = iter.next();
            final Map.Entry<K, V> origNext = origIter.next();
            if (!next.getKey().equals(origNext.getKey())) {
                throw new IllegalStateException(String.format("unexpected key: %s  (expected %s)", next.getKey(), origNext.getKey()));
            }
            if (!next.getValue().equals(origNext.getValue())) {
                throw new IllegalStateException(String.format("unexpected value: %s  (expected %s)", next.getValue(), origNext.getValue()));
            }
        }
        if (iter.hasNext() || origIter.hasNext()) {
            throw new IllegalStateException("different size");
        }
    }


}