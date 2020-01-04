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

import exchange.core2.core.processors.ObjectsPool;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * This node type is used for storing between 5 and
 * 16 child pointers. Like the Node4, the keys and pointers
 * are stored in separate arrays at corresponding positions, but
 * both arrays have space for 16 entries. A key can be found
 * efficiently with binary search or, on modern hardware, with
 * parallel comparisons using SIMD instructions.
 */
@Slf4j
@RequiredArgsConstructor
public final class ArtNode16<V> implements IArtNode<V> {

    private static final int NODE4_SWITCH_THRESHOLD = 3;

    // keys are ordered
    final short[] keys = new short[16];
    final Object[] nodes = new Object[16];

    long nodeKey;
    int nodeLevel;

    byte numChildren;

    @Getter
    final ObjectsPool objectsPool;

    void initFromNode4(ArtNode4<V> node4, short subKey, Object newElement) {

        final byte sourceSize = node4.numChildren;
        this.nodeLevel = node4.nodeLevel;
        this.nodeKey = node4.nodeKey;
        this.numChildren = (byte) (sourceSize + 1);
        int inserted = 0;
        for (int i = 0; i < sourceSize; i++) {
            final int key = node4.keys[i];
            if (inserted == 0 && key > subKey) {
                keys[i] = subKey;
                nodes[i] = newElement;
                inserted = 1;
            }
            keys[i + inserted] = node4.keys[i];
            nodes[i + inserted] = node4.nodes[i];
        }
        if (inserted == 0) {
            keys[sourceSize] = subKey;
            nodes[sourceSize] = newElement;
        }

        // put original node back into pool
        Arrays.fill(node4.nodes, null);
        objectsPool.put(ObjectsPool.ART_NODE_4, node4);
    }

    void initFromNode48(ArtNode48<V> node48) {
//        log.debug("48->16 nodeLevel={} (nodekey={})", node48.nodeLevel, node48.nodeKey);
        this.numChildren = node48.numChildren;
        this.nodeLevel = node48.nodeLevel;
        this.nodeKey = node48.nodeKey;
        byte idx = 0;
        for (short i = 0; i < 256; i++) {
            final byte j = node48.indexes[i];
            if (j != -1) {
                this.keys[idx] = i;
                this.nodes[idx] = node48.nodes[j];
                idx++;
            }
            if (idx == numChildren) {
                break;
            }
        }

        Arrays.fill(node48.nodes, null);
        Arrays.fill(node48.indexes, (byte) -1);
        objectsPool.put(ObjectsPool.ART_NODE_48, node48);
    }


    @Override
    @SuppressWarnings("unchecked")
    public V getValue(final long key, final int level) {
        if (level != nodeLevel && ((key ^ nodeKey) & (-1L << (nodeLevel + 8))) != 0) {
            return null;
        }
        final short nodeIndex = (short) ((key >>> nodeLevel) & 0xFF);
        for (int i = 0; i < numChildren; i++) {
            final short index = keys[i];
            if (index == nodeIndex) {
                final Object node = nodes[i];
                return nodeLevel == 0
                        ? (V) node
                        : ((IArtNode<V>) node).getValue(key, nodeLevel - 8);
            }
            if (nodeIndex < index) {
                // can give up searching because keys are in sorted order
                break;
            }
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
        final short nodeIndex = (short) ((key >>> nodeLevel) & 0xFF);
        int pos = 0;
        while (pos < numChildren) {
            if (nodeIndex == keys[pos]) {
                // just update
                if (nodeLevel == 0) {
                    nodes[pos] = value;
                } else {
                    final IArtNode<V> resizedNode = ((IArtNode<V>) nodes[pos]).put(key, nodeLevel - 8, value);
                    if (resizedNode != null) {
                        // TODO put old into the pool
                        // update resized node if capacity has increased
                        nodes[pos] = resizedNode;
                    }
                }
                return null;
            }
            if (nodeIndex < keys[pos]) {
                // can give up searching because keys are in sorted order
                break;
            }
            pos++;
        }

        // not found, put new element
        if (numChildren != 16) {
            // capacity less than 16 - can simply insert node
            final int copyLength = numChildren - pos;
            if (copyLength != 0) {
                System.arraycopy(keys, pos, keys, pos + 1, copyLength);
                System.arraycopy(nodes, pos, nodes, pos + 1, copyLength);
            }
            keys[pos] = nodeIndex;
            if (nodeLevel == 0) {
                nodes[pos] = value;
            } else {
                final ArtNode4<V> newSubNode = objectsPool.get(ObjectsPool.ART_NODE_4, ArtNode4::new);
                newSubNode.initFirstKey(key, value);
                nodes[pos] = newSubNode;
                newSubNode.put(key, nodeLevel - 8, value);
            }
            numChildren++;
            return null;
        } else {
            // no space left, create a Node48 with new element
            final Object newElement;
            if (nodeLevel == 0) {
                newElement = value;
            } else {
                final ArtNode4<V> newSubNode = objectsPool.get(ObjectsPool.ART_NODE_4, ArtNode4::new);
                newSubNode.initFirstKey(key, value);
                newElement = newSubNode;
            }

            ArtNode48<V> node48 = objectsPool.get(ObjectsPool.ART_NODE_48, ArtNode48::new);
            node48.initFromNode16(this, nodeIndex, newElement);

            return node48;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public IArtNode<V> remove(long key, int level) {
        if (level != nodeLevel && ((key ^ nodeKey) & (-1L << (nodeLevel + 8))) != 0) {
            return this;
        }
        final short nodeIndex = (short) ((key >>> nodeLevel) & 0xFF);
        Object node = null;
        int pos = 0;
        while (pos < numChildren) {
            if (nodeIndex == keys[pos]) {
                // found
                node = nodes[pos];
                break;
            }
            if (nodeIndex < keys[pos]) {
                // can give up searching because keys are in sorted order
                return this;
            }
            pos++;
        }

        if (node == null) {
            // not found
            return this;
        }

        // removing
        if (nodeLevel == 0) {
            removeElementAtPos(pos);
        } else {
            final IArtNode<V> resizedNode = ((IArtNode<V>) node).remove(key, nodeLevel - 8);
            if (resizedNode != node) {
                // update resized node if capacity has decreased
                nodes[pos] = resizedNode;
                if (resizedNode == null) {
                    removeElementAtPos(pos);
                }
            }
        }

        // switch to ArtNode4 if too small
        if (numChildren == NODE4_SWITCH_THRESHOLD) {
            final ArtNode4<V> newNode = objectsPool.get(ObjectsPool.ART_NODE_4, ArtNode4::new);
            newNode.initFromNode16(this);
            return newNode;
        } else {
            return this;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public V getCeilingValue(long key, int level) {
//        log.debug("key = {}", String.format("%Xh", key));
//        log.debug("level={} nodeLevel={} nodekey={} looking for key={} mask={}",
//                level, nodeLevel, String.format("%Xh", nodeKey), String.format("%Xh", key), String.format("%Xh", mask));
//
//        log.debug("key & mask = {} > nodeKey & mask = {}",
//                String.format("%Xh", key & mask), String.format("%Xh", nodeKey & mask));

        // special processing for compacted nodes
        if ((level != nodeLevel)) {
            // try first
            final long mask = -1L << (nodeLevel + 8);
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

        final short nodeIndex = (short) ((key >>> nodeLevel) & 0xFF);

        for (int i = 0; i < numChildren; i++) {
            final short index = keys[i];
//            log.debug("try index={} (looking for {}) key={}", String.format("%X", index), String.format("%X", nodeIndex), String.format("%X", key));
            // any equal or higher is ok
            if (index == nodeIndex) {
                final V res = nodeLevel == 0
                        ? (V) nodes[i]
                        : ((IArtNode<V>) nodes[i]).getCeilingValue(key, nodeLevel - 8);
                if (res != null) {
                    // return if found ceiling, otherwise will try next one
                    return res;
                }
            }
            if (index > nodeIndex) {
                // exploring first higher key
                return nodeLevel == 0
                        ? (V) nodes[i]
                        : ((IArtNode<V>) nodes[i]).getCeilingValue(0, nodeLevel - 8); // take lowest existing key
            }
        }
        return null;
    }


    @Override
    @SuppressWarnings("unchecked")
    public V getFloorValue(long key, int level) {
        //        log.debug("key = {}", String.format("%Xh", key));
//        log.debug("level={} nodeLevel={} nodekey={} looking for key={} mask={}",
//                level, nodeLevel, String.format("%Xh", nodeKey), String.format("%Xh", key), String.format("%Xh", mask));

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

        final short nodeIndex = (short) ((key >>> nodeLevel) & 0xFF);

        for (int i = numChildren - 1; i >= 0; i--) {
            final short index = keys[i];
            if (index == nodeIndex) {
                final V res = nodeLevel == 0
                        ? (V) nodes[i]
                        : ((IArtNode<V>) nodes[i]).getFloorValue(key, nodeLevel - 8);
                if (res != null) {
                    // return if found ceiling, otherwise will try next one
                    return res;
                }
            }
            if (index < nodeIndex) {
                // exploring first lower key
                return nodeLevel == 0
                        ? (V) nodes[i]
                        : ((IArtNode<V>) nodes[i]).getFloorValue(Long.MAX_VALUE, nodeLevel - 8); // take highest existing key
            }
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int forEach(LongObjConsumer<V> consumer, int limit) {
        if (nodeLevel == 0) {
            final long keyBase = (nodeKey >>> 8) << 8;
            final int n = Math.min(numChildren, limit);
            for (int i = 0; i < n; i++) {
                consumer.accept(keyBase + keys[i], (V) nodes[i]);
            }
            return n;
        } else {
            int numLeft = limit;
            for (int i = 0; i < numChildren && numLeft > 0; i++) {
                numLeft -= ((IArtNode<V>) nodes[i]).forEach(consumer, numLeft);
            }
            return limit - numLeft;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public int forEachDesc(LongObjConsumer<V> consumer, int limit) {
        if (nodeLevel == 0) {
            final long keyBase = (nodeKey >>> 8) << 8;
            int numFound = 0;
            for (int i = numChildren - 1; i >= 0 && numFound < limit; i--) {
                consumer.accept(keyBase + keys[i], (V) nodes[i]);
                numFound++;
            }
            return numFound;
        } else {
            int numLeft = limit;
            for (int i = numChildren - 1; i >= 0 && numLeft > 0; i--) {
                numLeft -= ((IArtNode<V>) nodes[i]).forEachDesc(consumer, numLeft);
            }
            return limit - numLeft;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public int size(int limit) {
        if (nodeLevel == 0) {
            return numChildren;
        } else {
            int numLeft = limit;
            for (int i = numChildren - 1; i >= 0 && numLeft > 0; i--) {
                numLeft -= ((IArtNode<V>) nodes[i]).size(numLeft);
            }
            return limit - numLeft;
        }
    }

    @Override
    public void validateInternalState(int level) {
        if (nodeLevel > level) throw new IllegalStateException("unexpected nodeLevel");
        if (numChildren > 16 || numChildren <= NODE4_SWITCH_THRESHOLD)
            throw new IllegalStateException("unexpected numChildren");
        short last = -1;
        for (int i = 0; i < 16; i++) {
            Object node = nodes[i];
            if (i < numChildren) {
                if (node == null) throw new IllegalStateException("null node");
                if (keys[i] < 0 || keys[i] >= 256) throw new IllegalStateException("key out of range");
                if (keys[i] == last) throw new IllegalStateException("duplicate key");
                if (keys[i] < last) throw new IllegalStateException("wrong key order");
                last = keys[i];
                if (node instanceof IArtNode) {
                    if (nodeLevel == 0) throw new IllegalStateException("unexpected node type");
                    IArtNode artNode = (IArtNode) node;
                    artNode.validateInternalState(nodeLevel - 8);
                } else {
                    if (nodeLevel != 0) throw new IllegalStateException("unexpected node type");
                }

            } else {
                if (node != null) throw new IllegalStateException("not released node");
            }
        }
    }

    @Override
    public String printDiagram(String prefix, int level) {
        return LongAdaptiveRadixTreeMap.printDiagram(prefix, level, nodeLevel, nodeKey, numChildren, idx -> keys[idx], idx -> nodes[idx]);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Map.Entry<Long, V>> entries() {
        final long keyPrefix = nodeKey & (-1L << 8);
        final List<Map.Entry<Long, V>> list = new ArrayList<>();
        for (int i = 0; i < numChildren; i++) {
            if (nodeLevel == 0) {
                list.add(new LongAdaptiveRadixTreeMap.Entry<>(keyPrefix + keys[i], (V) nodes[i]));
            } else {
                list.addAll(((IArtNode<V>) nodes[i]).entries());
            }
        }
        return list;
    }

    private void removeElementAtPos(final int pos) {
        final int ppos = pos + 1;
        final int copyLength = numChildren - ppos;
        if (copyLength != 0) {
            System.arraycopy(keys, ppos, keys, pos, copyLength);
            System.arraycopy(nodes, ppos, nodes, pos, copyLength);
        }
        numChildren--;
        nodes[numChildren] = null;
    }

    @Override
    public String toString() {
        return "ArtNode16{" +
                "nodeKey=" + nodeKey +
                ", nodeLevel=" + nodeLevel +
                ", numChildren=" + numChildren +
                '}';
    }
}
