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
import java.util.List;
import java.util.Map;

import static exchange.core2.core.art.ArtNode4.toNodeIndex;

/**
 * This node type is used for storing between 5 and
 * 16 child pointers. Like the Node4, the keys and pointers
 * are stored in separate arrays at corresponding positions, but
 * both arrays have space for 16 entries. A key can be found
 * efficiently with binary search or, on modern hardware, with
 * parallel comparisons using SIMD instructions.
 */
@Slf4j
public final class ArtNode16<V> implements IArtNode<V> {

    private static final int NODE4_SWITCH_THRESHOLD = 3;

    // keys are ordered
    final short[] keys = new short[16];
    final Object[] nodes = new Object[16];

    byte numChildren;

    public ArtNode16(ArtNode4<V> node4, short subKey, Object newElement) {
        final byte sourceSize = node4.numChildren;
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
    }

    public ArtNode16(ArtNode48<V> node48) {
        this.numChildren = node48.numChildren;
        byte idx = 0;
        for (short i = 0; i < 256; i++) {
            final byte j = node48.indexes[i];
            if (j != -1) {
                this.keys[idx] = i;
                this.nodes[idx] = node48.nodes[j];
                idx++;
            }
            if (idx == numChildren) {
                return;
            }
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public V getValue(final long key, final int level) {
        final short nodeIndex = toNodeIndex(key, level);
        for (int i = 0; i < numChildren; i++) {
            final short index = keys[i];
            if (index == nodeIndex) {
                final Object node = nodes[i];
                return level == 0
                        ? (V) node
                        : ((IArtNode<V>) node).getValue(key, level - 8);
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
        final short nodeIndex = toNodeIndex(key, level);
        int pos = 0;
        while (pos < numChildren) {
            if (nodeIndex == keys[pos]) {
                // just update
                if (level == 0) {
                    nodes[pos] = value;
                } else {
                    final IArtNode<V> resizedNode = ((IArtNode<V>) nodes[pos]).put(key, level - 8, value);
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
            if (level == 0) {
                nodes[pos] = value;
            } else {
                // TODO take from pool
                final ArtNode4 newSubNode = new ArtNode4(key, level - 8, value);
                nodes[pos] = newSubNode;
                // TODO create compressed-path node
                newSubNode.put(key, level - 8, value);
            }
            numChildren++;
            return null;
        } else {
            // no space left, create a Node48 with new item
            return new ArtNode48<>(this, nodeIndex, level == 0 ? value : new ArtNode4<>(key, level - 8, value));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public IArtNode<V> remove(long key, int level) {
        final short nodeIndex = toNodeIndex(key, level);
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
        if (level == 0) {
            removeElementAtPos(pos);
        } else {
            final IArtNode<V> resizedNode = ((IArtNode<V>) node).remove(key, level - 8);
            if (resizedNode != node) {
                // TODO put old into the pool
                // update resized node if capacity has decreased
                nodes[pos] = resizedNode;
                if (resizedNode == null) {
                    removeElementAtPos(pos);
                }
            }
        }

        // switch to ArtNode4 if too small
        return (numChildren == NODE4_SWITCH_THRESHOLD) ? new ArtNode4(this) : this;
    }

    @Override
    public void validateInternalState() {
        if (numChildren <= NODE4_SWITCH_THRESHOLD) throw new IllegalStateException("too small");
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
                    IArtNode artNode = (IArtNode) node;
                    artNode.validateInternalState();
                }

            } else {
                if (node != null) throw new IllegalStateException("not released node");
            }
        }
    }

    @Override
    public String printDiagram(String prefix, int level) {
        return LongAdaptiveRadixTreeMap.printDiagram(prefix, level, numChildren, idx -> keys[idx], idx -> nodes[idx]);
    }

    @Override
    public List<Map.Entry<Long, V>> entries(long keyPrefix, int level) {
        final long keyPrefixNext = keyPrefix << 8;
        final List<Map.Entry<Long, V>> list = new ArrayList<>();
        for (int i = 0; i < numChildren; i++) {
            if (level == 0) {
                list.add(new LongAdaptiveRadixTreeMap.Entry<>(keyPrefixNext + keys[i], (V) nodes[i]));
            } else {
                list.addAll(((IArtNode<V>) nodes[i]).entries(keyPrefixNext + keys[i], level - 8));
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
}
