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
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.util.*;

public class HashingUtils {

    public static int stateHash(final BitSet bitSet) {
        return Arrays.hashCode(bitSet.toLongArray());
    }


    public static <T extends StateHash> int stateHash(final LongObjectHashMap<T> hashMap) {
        final SortedMap<Long, T> sortedMap = new TreeMap<>();
        hashMap.forEachKeyValue(sortedMap::put);
        return Arrays.hashCode(sortedMap.entrySet().stream().mapToInt(ent -> Objects.hash(ent.getKey(), ent.getValue().stateHash())).toArray());
    }


    public static <T extends StateHash> int stateHash(final IntObjectHashMap<T> hashMap) {
        final SortedMap<Integer, T> sortedMap = new TreeMap<>();
        hashMap.forEachKeyValue(sortedMap::put);
        return Arrays.hashCode(sortedMap.entrySet().stream().mapToInt(ent -> Objects.hash(ent.getKey(), ent.getValue().stateHash())).toArray());
    }


    public static <T extends StateHash> int stateHash(final Map<Long, T> map) {
        final SortedMap<Long, T> sortedMap = new TreeMap<>();
        map.forEach(sortedMap::put);
        return Arrays.hashCode(sortedMap.entrySet().stream().mapToInt(ent -> Objects.hash(ent.getKey(), ent.getValue().stateHash())).toArray());
    }

}
