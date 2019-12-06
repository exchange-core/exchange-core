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

/**
 * The smallest node type can store up to 4 child
 * pointers and uses an array of length 4 for keys and another
 * array of the same length for pointers. The keys and pointers
 * are stored at corresponding positions and the keys are sorted.
 */
@Slf4j
public final class ArtNode4<V> implements IArtNode<V> {

    // keys are ordered
    final short[] keys = new short[4];
    final Object[] nodes = new Object[4];

    byte numChildren;


    public ArtNode4(final long key, final int level, final V value) {
        // TODO create compact node
        this.numChildren = 1;
        this.keys[0] = toNodeIndex(key, level);
        this.nodes[0] = (level == 0) ? value : new ArtNode4<>(key, level - 8, value);
    }


    public ArtNode4(ArtNode16 artNode16) {
        this.numChildren = artNode16.numChildren;
        System.arraycopy(artNode16.keys, 0, this.keys, 0, numChildren);
        System.arraycopy(artNode16.nodes, 0, this.nodes, 0, numChildren);
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

//        log.debug("PUT key:{} level:{} value:{}", key, level, value);

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

//        log.debug("pos:{}", pos);

        // new element
        if (numChildren != 4) {
            // capacity less than 4 - can simply insert node
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
                // TODO create compressed-path node

                final ArtNode4 newSubNode = new ArtNode4(key, level - 8, value);
                nodes[pos] = newSubNode;
                // TODO create compressed-path node
                //newSubNode.put();
            }
            numChildren++;
            return null;
        } else {
            // no space left, create a Node16 with new item
            return new ArtNode16<>(this, nodeIndex, (level == 0) ? value : new ArtNode4<>(key, level - 8, value));
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

        // indicate if removed last one
        return (numChildren == 0) ? null : this;
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
    public void validateInternalState() {
        short last = -1;
        for (int i = 0; i < 4; i++) {
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

    static short toNodeIndex(long key, int level) {
        return (short) ((key >>> level) & 0xFF);
    }
}
