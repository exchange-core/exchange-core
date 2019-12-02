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

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static exchange.core2.core.art.ArtNode4.toNodeIndex;

/**
 * As the number of entries in a node increases,
 * searching the key array becomes expensive. Therefore, nodes
 * with more than 16 pointers do not store the keys explicitly.
 * Instead, a 256-element array is used, which can be indexed
 * with key bytes directly. If a node has between 17 and 48 child
 * pointers, this array stores indexes into a second array which
 * contains up to 48 pointers. This indirection saves space in
 * comparison to 256 pointers of 8 bytes, because the indexes
 * only require 6 bits (we use 1 byte for simplicity).
 */
@Slf4j
public final class ArtNode48<V> implements IArtNode<V> {

    private static final int NODE16_SWITCH_THRESHOLD = 12;

    // just keep indexes
    final byte[] indexes;
    final Object[] nodes = new Object[48];

    byte numChildren;

    public ArtNode48(ArtNode16<V> node16, short subKey, Object newElement) {
        final byte sourceSize = 16;
        this.indexes = new byte[256];
        Arrays.fill(this.indexes, (byte) -1);
        this.numChildren = sourceSize + 1;

        for (byte i = 0; i < sourceSize; i++) {
            this.indexes[node16.keys[i]] = i;
            this.nodes[i] = node16.nodes[i];
        }

        this.indexes[subKey] = sourceSize;
        this.nodes[sourceSize] = newElement;
    }

    public ArtNode48(ArtNode256<V> node256) {
        this.indexes = new byte[256];
        Arrays.fill(this.indexes, (byte) -1);
        this.numChildren = (byte) node256.numChildren;

        byte idx = 0;
        for (int i = 0; i < 256; i++) {
            Object node = node256.nodes[i];
            if (node != null) {
                this.indexes[i] = idx;
                this.nodes[idx] = node;
                idx++;
                if (idx == numChildren) {
                    break;
                }
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public V getValue(final long key, final int level) {
        final short idx = toNodeIndex(key, level);
        final byte nodeIndex = indexes[idx];
        if (nodeIndex != -1) {
            final Object node = nodes[nodeIndex];
            return level == 0
                    ? (V) node
                    : ((IArtNode<V>) node).getValue(key, level - 8);
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public IArtNode<V> put(final long key, final int level, final V value) {
        final short idx = toNodeIndex(key, level);
        final byte nodeIndex = indexes[idx];
        if (nodeIndex != -1) {
            // found
            if (level == 0) {
                nodes[nodeIndex] = value;
            } else {
                final IArtNode<V> resizedNode = ((IArtNode<V>) nodes[nodeIndex]).put(key, level - 8, value);
                if (resizedNode != null) {
                    // update resized node if capacity has increased
                    // TODO put old into the pool
                    nodes[nodeIndex] = resizedNode;
                }
            }
            return null;
        }

        // not found, put new element

        if (numChildren != 48) {
            // capacity less than 48 - can simply insert node
            indexes[idx] = numChildren;

            if (level == 0) {
                nodes[numChildren] = value;
            } else {
                // TODO take from pool
                // TODO create compressed-path node
                final ArtNode4 newSubNode = new ArtNode4(key, level - 8, value);
                nodes[numChildren] = newSubNode;
            }
            numChildren++;
            return null;

        } else {
            // no space left, create a ArtNode256 containing a new item
            return new ArtNode256<>(this, nodeIndex, value);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public IArtNode<V> remove(long key, int level) {
        final short idx = toNodeIndex(key, level);
        final byte nodeIndex = indexes[idx];
        if (nodeIndex == -1) {
            return this;
        }

        if (level == 0) {
            nodes[nodeIndex] = null;
            indexes[idx] = -1;
            numChildren--;
        } else {
            final IArtNode<V> node = (IArtNode<V>) nodes[nodeIndex];
            final IArtNode<V> resizedNode = node.remove(key, level - 8);
            if (resizedNode != node) {
                // TODO put old into the pool
                // update resized node if capacity has decreased
                nodes[nodeIndex] = resizedNode;
                if (resizedNode == null) {
                    numChildren--;
                }
            }
        }

        return (numChildren == NODE16_SWITCH_THRESHOLD) ? new ArtNode16(this) : this;
    }


    @Override
    public void validateInternalState() {

        // TODO
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Map.Entry<Long, V>> entries(long keyPrefix, int level) {
        final long keyPrefixNext = keyPrefix << 8;
        final List<Map.Entry<Long, V>> list = new ArrayList<>();
        short[] keys = createKeysArray();
        for (int i = 0; i < numChildren; i++) {
            if (level == 0) {
                list.add(new LongAdaptiveRadixTreeMap.Entry<>(keyPrefixNext + keys[i], (V) nodes[i]));
            } else {
                list.addAll(((IArtNode<V>) nodes[i]).entries(keyPrefixNext + keys[i], level - 8));
            }
        }
        return list;
    }

    @Override
    public String printDiagram(String prefix, int level) {
        final short[] keys = createKeysArray();
        return LongAdaptiveRadixTreeMap.printDiagram(prefix, level, numChildren, idx -> keys[idx], idx -> nodes[idx]);
    }

    private short[] createKeysArray() {
        short[] keys = new short[numChildren];
        int j = 0;
        for (short i = 0; i < 256; i++) {
            if (indexes[i] != -1) {
                keys[j++] = i;
            }
        }
        return keys;
    }
}
