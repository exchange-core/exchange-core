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
package exchange.core2.core.utils;

import exchange.core2.core.common.StateHash;
import lombok.extern.slf4j.Slf4j;
import org.agrona.collections.MutableLong;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Stream;

@Slf4j
public final class HashingUtils {

    public static int stateHash(final BitSet bitSet) {
        return Arrays.hashCode(bitSet.toLongArray());
    }

    public static <T extends StateHash> int stateHash(final LongObjectHashMap<T> hashMap) {

        final MutableLong mutableLong = new MutableLong();
        hashMap.forEachKeyValue((k, v) -> mutableLong.addAndGet(Objects.hash(k, v.stateHash())));
        return Long.hashCode(mutableLong.value);
    }

    public static <T extends StateHash> int stateHash(final IntObjectHashMap<T> hashMap) {

        final MutableLong mutableLong = new MutableLong();
        hashMap.forEachKeyValue((k, v) -> mutableLong.addAndGet(Objects.hash(k, v.stateHash())));
        return Long.hashCode(mutableLong.value);
    }


    public static int stateHashStream(final Stream<? extends StateHash> stream) {
        int h = 0;
        final Iterator<? extends StateHash> iterator = stream.iterator();
        while (iterator.hasNext()) {
            h = h * 31 + iterator.next().stateHash();
        }
        return h;
    }

    /**
     * Checks if both streams contain same elements in same order
     *
     * @param s1 stream 1
     * @param s2 stream 2
     * @return true if streams contain same elements in same order
     */
    public static boolean checkStreamsEqual(final Stream<?> s1, final Stream<?> s2) {
        final Iterator<?> iter1 = s1.iterator(), iter2 = s2.iterator();
        while (iter1.hasNext() && iter2.hasNext()) {
            if (!iter1.next().equals(iter2.next())) {
                return false;
            }
        }
        return !iter1.hasNext() && !iter2.hasNext();
    }

}