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

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Adaptive Radix Tree (ART) Java implementation
 * <p>
 * based on original paper:
 * <p>
 * The Adaptive Radix Tree:
 * ARTful Indexing for Main-Memory Databases
 * <p>
 * Viktor Leis, Alfons Kemper, Thomas Neumann
 * Fakultat fur Informatik
 * Technische Universitat Munchen
 * Boltzmannstrae 3, D-85748 Garching
 * <p>
 * https://db.in.tum.de/~leis/papers/ART.pdf
 * <p>
 * Target operations:
 * - GET or (PUT + GET_LOWER) - placing/moving/bulkload order - often GET, more rare PUT ??cache
 * - REMOVE - cancel or move last order in a bucket
 * - TRAVERSE from LOWER - filling L2 market data, in hot area (Node256 or Node48).
 * - REMOVE price during matching - !! can use RANGE removal operation - rare, but latency critical
 * - GET or PUT if not exists - inserting back own orders, very rare
 */
@Slf4j
public final class LongAdaptiveRadixTreeMap<V> {

    private static final int INITIAL_LEVEL = 56;

    private IArtNode<V> root = null;

    public V get(final long key) {
        return root != null
                ? root.getValue(key, INITIAL_LEVEL)
                : null;
    }

    public void put(final long key, final V value) {
        if (root == null) {
            root = new ArtNode4<>(key, INITIAL_LEVEL, value);

        } else {

            final IArtNode<V> upSizedNode = root.put(key, INITIAL_LEVEL, value);
            if (upSizedNode != null) {
                // TODO put old into the pool
                root = upSizedNode;
            }
        }
    }

    public V getOrInsert(final long key, Supplier<V> supplier) {
        // TODO implement
        return null;
    }

    public void getOrInsertFromNode(final IArtNode<V> node, Supplier<V> supplier) {
        // TODO implement
    }

    public void remove(final long key) {
        if (root != null) {
            final IArtNode<V> downSizeNode = root.remove(key, INITIAL_LEVEL);
            // ignore null because can not remove root
            if (downSizeNode != null && downSizeNode != root) {
                // TODO put old into the pool
                root = downSizeNode;
            }
        }
    }

    /**
     * remove on matching
     */
    public void removeRange(final long keyFromInclusive, final long keyToExclusive) {

    }


    public void validateInternalState() {
        if (root != null) {
            // TODO initial level
            root.validateInternalState();
        }
    }

    List<Map.Entry<Long, V>> entriesList() {
        if (root != null) {
            return root.entries(0L, INITIAL_LEVEL);
        } else {
            return Collections.emptyList();
        }
    }

    String printDiagram() {
        if (root != null) {
            return root.printDiagram("", INITIAL_LEVEL);
        } else {
            return "";
        }
    }

    // TODO remove based on leaf  (having reference) ?

    static String printDiagram(String prefix, int level, short numChildren, Function<Short, Short> subKeys, Function<Short, Object> nodes) {
        StringBuilder sb = new StringBuilder();
        for (short i = 0; i < numChildren; i++) {
            Object node = nodes.apply(i);
            String key = String.format("%02X", subKeys.apply(i));
            String x = (i == 0 ? (numChildren == 1 ? "──" : "┬─") : (i + 1 == numChildren ? (prefix + "└─") : (prefix + "├─")));

            log.debug("level: {} numChildren={}", level, numChildren);

            if (level == 0) {
                sb.append(x + key + " = " + node);
            } else {
                sb.append(x + key + "" + (((IArtNode<?>) node).printDiagram(prefix + (i + 1 == numChildren ? "    " : "│   "), level - 8)));
            }
            if (i < numChildren - 1) {
                sb.append("\n");
            } else if (level == 0) {
                sb.append("\n" + prefix);
            }
        }
        return sb.toString();
    }

    @AllArgsConstructor
    public static final class Entry<V> implements Map.Entry<Long, V> {

        final long key;

        V value;

        @Override
        public Long getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            final V v = this.value;
            this.value = value;
            return v;
        }
    }

}
