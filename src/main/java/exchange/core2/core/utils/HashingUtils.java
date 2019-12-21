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
import exchange.core2.core.orderbook.IOrderBook;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.util.*;
import java.util.stream.Stream;

@Slf4j
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

    public static int stateHash(IOrderBook orderBook) {

//        log.debug("State hash of {}", orderBook.getClass().getSimpleName());
//        log.debug("  Ask orders stream: {}", orderBook.askOrdersStream(true).collect(Collectors.toList()));
//        log.debug("  Ask orders hash: {}", stateHashStream(orderBook.askOrdersStream(true)));
//        log.debug("  Bid orders stream: {}", orderBook.bidOrdersStream(true).collect(Collectors.toList()));
//        log.debug("  Bid orders hash: {}", stateHashStream(orderBook.bidOrdersStream(true)));
//        log.debug("  getSymbolSpec: {}", orderBook.getSymbolSpec());
//        log.debug("  getSymbolSpec hash: {}", orderBook.getSymbolSpec().stateHash());

        return Objects.hash(
                stateHashStream(orderBook.askOrdersStream(true)),
                stateHashStream(orderBook.bidOrdersStream(true)),
                orderBook.getSymbolSpec().stateHash());
    }

    public static boolean checkSameOrders(IOrderBook ob1, IOrderBook ob2) {
        return ob1.getSymbolSpec().equals(ob2.getSymbolSpec())
                && checkStreamsEqual(ob1.askOrdersStream(true), ob2.askOrdersStream(true))
                && checkStreamsEqual(ob1.bidOrdersStream(true), ob2.bidOrdersStream(true));
    }


    private static int stateHashStream(Stream<? extends StateHash> stream) {
        int h = 0;
        final Iterator<? extends StateHash> iterator = stream.iterator();
        while (iterator.hasNext()) {
            h = h * 31 + iterator.next().stateHash();
        }
        return h;
    }

    public static boolean eq(IOrderBook me, Object o) {
        if (o == me) return true;
        if (o == null) return false;
        if (!(o instanceof IOrderBook)) return false;
        IOrderBook other = (IOrderBook) o;
        return me.getSymbolSpec().equals(((IOrderBook) o).getSymbolSpec())
                && checkStreamsEqual(me.askOrdersStream(true), other.askOrdersStream(true))
                && checkStreamsEqual(me.bidOrdersStream(true), other.bidOrdersStream(true));
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