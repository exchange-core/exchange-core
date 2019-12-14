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
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
    final byte[] indexes = new byte[256];
    final Object[] nodes = new Object[48];

    long nodeKey;
    int nodeLevel;

    byte numChildren;

    public ArtNode48(ArtNode16<V> node16, short subKey, Object newElement) {
        final byte sourceSize = 16;
        Arrays.fill(this.indexes, (byte) -1);
        this.numChildren = sourceSize + 1;
        this.nodeLevel = node16.nodeLevel;
        this.nodeKey = node16.nodeKey;

        for (byte i = 0; i < sourceSize; i++) {
            this.indexes[node16.keys[i]] = i;
            this.nodes[i] = node16.nodes[i];
        }

        this.indexes[subKey] = sourceSize;
        this.nodes[sourceSize] = newElement;
    }

    public ArtNode48(ArtNode256<V> node256) {
        Arrays.fill(this.indexes, (byte) -1);
        this.numChildren = (byte) node256.numChildren;
        this.nodeLevel = node256.nodeLevel;
        this.nodeKey = node256.nodeKey;
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
        if (level != nodeLevel && ((key ^ nodeKey) & (-1L << (nodeLevel + 8))) != 0) {
            return null;
        }
        final short idx = (short) ((key >>> nodeLevel) & 0xFF);
        final byte nodeIndex = indexes[idx];
        if (nodeIndex != -1) {
            final Object node = nodes[nodeIndex];
            return nodeLevel == 0
                    ? (V) node
                    : ((IArtNode<V>) node).getValue(key, nodeLevel - 8);
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public IArtNode<V> put(final long key, final int level, final V value) {
        if (level != nodeLevel) {
            final IArtNode<V> branch = LongAdaptiveRadixTreeMap.branchIfRequired(key, value, nodeKey, nodeLevel, this);
            if (branch != null) {
                return branch;
            }
        }
        final short idx = (short) ((key >>> nodeLevel) & 0xFF);
        final byte nodeIndex = indexes[idx];
        if (nodeIndex != -1) {
            // found
            if (nodeLevel == 0) {
                nodes[nodeIndex] = value;
            } else {
                final IArtNode<V> resizedNode = ((IArtNode<V>) nodes[nodeIndex]).put(key, nodeLevel - 8, value);
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

            if (nodeLevel == 0) {
                nodes[numChildren] = value;
            } else {
                // TODO take from pool
                // TODO create compressed-path node
                final ArtNode4 newSubNode = new ArtNode4(key, value);
                nodes[numChildren] = newSubNode;
            }
            numChildren++;
            return null;

        } else {
            // no space left, create a ArtNode256 containing a new item
            return new ArtNode256<>(this, idx, nodeLevel == 0 ? value : new ArtNode4<>(key, value));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public IArtNode<V> remove(long key, int level) {
        if (level != nodeLevel && ((key ^ nodeKey) & (-1L << (nodeLevel + 8))) != 0) {
            return this;
        }
        final short idx = (short) ((key >>> nodeLevel) & 0xFF);
        final byte nodeIndex = indexes[idx];
        if (nodeIndex == -1) {
            return this;
        }

        if (nodeLevel == 0) {
            nodes[nodeIndex] = null;
            indexes[idx] = -1;
            numChildren--;
        } else {
            final IArtNode<V> node = (IArtNode<V>) nodes[nodeIndex];
            final IArtNode<V> resizedNode = node.remove(key, nodeLevel - 8);
            if (resizedNode != node) {
                // TODO put old into the pool
                // update resized node if capacity has decreased
                nodes[nodeIndex] = resizedNode;
                if (resizedNode == null) {
                    numChildren--;
                    indexes[idx] = -1;
                }
            }
        }

        return (numChildren == NODE16_SWITCH_THRESHOLD) ? new ArtNode16(this) : this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V getCeilingValue(long key, int level) {
//        log.debug("key = {}", String.format("%Xh", key));
        // special processing for compacted nodes
        if ((level != nodeLevel)) {
            // try first
            final long mask = -1L << (nodeLevel + 8);
//            log.debug("key & mask = {} > nodeKey & mask = {}", String.format("%Xh", key & mask), String.format("%Xh", nodeKey & mask));
            final long keyWithMask = key & mask;
            final long nodeKeyWithMask = nodeKey & mask;
            if (nodeKeyWithMask < keyWithMask) {
                // compacted part is lower - no need to search for ceiling entry here
                return null;
            } else if (keyWithMask != nodeKeyWithMask) {
                // can reset key, because compacted nodekey is higher
                key = 0;
            }
        }

        short idx = (short) ((key >>> nodeLevel) & 0xFF);

        byte index = indexes[idx];
        if (index != -1) {
            // if exact key found
            final V res = nodeLevel == 0 ? (V) nodes[index] : ((IArtNode<V>) nodes[index]).getCeilingValue(key, nodeLevel - 8);
            if (res != null) {
                // return if found ceiling, otherwise will try next one
                return res;
            }
        }

        // if exact key not found - searching for first higher key
        while (++idx < 256) {
            index = indexes[idx];
            if (index != -1) {
                if (nodeLevel == 0) {
                    // found
                    return (V) nodes[index];
                } else {
                    // find first lowest key
                    return ((IArtNode<V>) nodes[index]).getCeilingValue(0, nodeLevel - 8);
                }
            }
        }

        // no keys found
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V getFloorValue(long key, int level) {
        //        log.debug("key = {}", String.format("%Xh", key));
        // special processing for compacted nodes
        if ((level != nodeLevel)) {
            // try first
            final long mask = -1L << (nodeLevel + 8);
//            log.debug("key & mask = {} > nodeKey & mask = {}",
//                    String.format("%Xh", key & mask), String.format("%Xh", nodeKey & mask));
            final long keyWithMask = key & mask;
            final long nodeKeyWithMask = nodeKey & mask;
            if (nodeKeyWithMask > keyWithMask) {
                // compacted part is higher - no need to search for floor entry here
                return null;
            } else if (keyWithMask != nodeKeyWithMask) {
                // find highest value, because compacted nodekey is lower
                key = Long.MAX_VALUE;
            }
        }

        short idx = (short) ((key >>> nodeLevel) & 0xFF);

        byte index = indexes[idx];
        if (index != -1) {
            // if exact key found
            final V res = nodeLevel == 0 ? (V) nodes[index] : ((IArtNode<V>) nodes[index]).getFloorValue(key, nodeLevel - 8);
            if (res != null) {
                // return if found ceiling, otherwise will try prev one
                return res;
            }
        }

        // if exact key not found - searching for first lower key
        while (--idx >= 0) {
            index = indexes[idx];
            if (index != -1) {
                if (nodeLevel == 0) {
                    // found
                    return (V) nodes[index];
                } else {
                    // find first highest key
                    return ((IArtNode<V>) nodes[index]).getFloorValue(Long.MAX_VALUE, nodeLevel - 8);
                }
            }
        }

        // no keys found
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int forEach(LongObjConsumer<V> consumer, int limit) {
        if (nodeLevel == 0) {
            final long keyBase = (nodeKey >>> 8) << 8;
            int numFound = 0;
            for (short i = 0; i < 256; i++) {
                if (numFound == limit) {
                    return numFound;
                }
                final byte index = indexes[i];
                if (index != -1) {
                    consumer.accept(keyBase + i, (V) nodes[index]);
                    numFound++;
                }
            }
            return numFound;
        } else {
            int numLeft = limit;
            for (short i = 0; i < 256 && numLeft > 0; i++) {
                final byte index = indexes[i];
                if (index != -1) {
                    numLeft -= ((IArtNode<V>) nodes[index]).forEach(consumer, numLeft);
                }
            }
            return limit - numLeft;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public int forEachDesc(LongObjConsumer<V> consumer, int limit) {
        return 0;
    }

    @Override
    public void validateInternalState(int level) {
        if (nodeLevel > level) throw new IllegalStateException("unexpected nodeLevel");
        int found = 0;
        IntHashSet keysSet = new IntHashSet();
        for (int i = 0; i < 256; i++) {
            int idx = indexes[i];
            if (idx != -1) {
                if (idx > 47 || idx < -1) throw new IllegalStateException("wrong index");
                keysSet.add(idx);
                found++;
                if (nodes[idx] == null) throw new IllegalStateException("null node");
            }
        }
        if (found != numChildren) throw new IllegalStateException("wrong numChildren");
        if (keysSet.size() != numChildren) throw new IllegalStateException("duplicate keys");
        if (numChildren <= NODE16_SWITCH_THRESHOLD) throw new IllegalStateException("too small");

        for (int i = 0; i < 48; i++) {
            Object node = nodes[i];
            if (keysSet.contains(i)) {
                if (node == null) {
                    throw new IllegalStateException("null node");
                } else {
                    if (node instanceof IArtNode) {
                        if (nodeLevel == 0) throw new IllegalStateException("unexpected node type");
                        IArtNode artNode = (IArtNode) node;
                        artNode.validateInternalState(nodeLevel - 8);
                    } else {
                        if (nodeLevel != 0) throw new IllegalStateException("unexpected node type");
                    }
                }
            } else {
                if (node != null) throw new IllegalStateException("not released node");
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Map.Entry<Long, V>> entries() {
        final long keyPrefix = nodeKey & (-1L << 8);
        final List<Map.Entry<Long, V>> list = new ArrayList<>();
        short[] keys = createKeysArray();
        for (int i = 0; i < numChildren; i++) {
            if (nodeLevel == 0) {
                list.add(new LongAdaptiveRadixTreeMap.Entry<>(keyPrefix + keys[i], (V) nodes[indexes[keys[i]]]));
            } else {
                list.addAll(((IArtNode<V>) nodes[indexes[keys[i]]]).entries());
            }
        }
        return list;
    }

    @Override
    public String printDiagram(String prefix, int level) {
        final short[] keys = createKeysArray();
        return LongAdaptiveRadixTreeMap.printDiagram(prefix, level, nodeLevel, nodeKey, numChildren, idx -> keys[idx], idx -> nodes[indexes[keys[idx]]]);
    }

    @Override
    public String toString() {
        return "ArtNode48{" +
                "nodeKey=" + nodeKey +
                ", nodeLevel=" + nodeLevel +
                ", numChildren=" + numChildren +
                '}';
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
