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

/**
 * The smallest node type can store up to 4 child
 * pointers and uses an array of length 4 for keys and another
 * array of the same length for pointers. The keys and pointers
 * are stored at corresponding positions and the keys are sorted.
 */
public final class ArtNode4<V> implements IArtNode<V> {

    // store keys in unsorted order
    final short[] keys = new short[4];
    final Object[] nodes = new Object[4];

    private byte numChildren;

    public ArtNode4() {
        this.numChildren = 0;
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
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public IArtNode<V> put(final long key, final int level, final V value) {
        final short nodeIndex = toNodeIndex(key, level);
        for (int i = 0; i < numChildren; i++) {
            if (keys[i] == nodeIndex) {
                if (level == 0) {
                    nodes[i] = value;
                } else {
                    final IArtNode<V> resizedNode = ((IArtNode<V>) nodes[i]).put(key, level - 8, value);
                    if (resizedNode != null) {
                        // TODO put old into the pool
                        // update resized node if capacity has increased
                        nodes[i] = resizedNode;
                    }
                }
                return null;
            }
        }

        // new element
        if (numChildren != 4) {
            // filled less than 4 - can simply insert node
            keys[numChildren] = nodeIndex;
            if (level == 0) {
                nodes[numChildren] = value;
            } else {
                // TODO take from pool
                final ArtNode4 newSubNode = new ArtNode4();
                nodes[numChildren] = newSubNode;
                // TODO create compressed-path node
                newSubNode.put(key, level - 8, value);
            }
            numChildren++;
            return null;
        } else {
            // no space left, create a Node16 with new item
            return new ArtNode16<>(this, nodeIndex, value);
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

        return (numChildren == 0) ? null : this;
    }

    private void removeElementAtPos(final int pos) {
        final int copyLength = numChildren - pos;
        if (copyLength != 0) {
            System.arraycopy(keys, pos + 1, keys, pos, copyLength);
            System.arraycopy(nodes, pos + 1, nodes, pos, copyLength);
        }
        numChildren--;
        nodes[numChildren] = null;
    }

    @Override
    public void validateInternalState() {
        short last = -1;
        for (int i = 0; i < 4; i++) {
            if (i < numChildren) {
                if (nodes[i] == null) throw new IllegalStateException("null node");
                if (keys[i] < 0 || keys[i] >= 256) throw new IllegalStateException("key out of range");
                if (keys[i] == last) throw new IllegalStateException("duplicate key");
                if (keys[i] < last) throw new IllegalStateException("wrong key order");
                last = keys[i];
            } else {
                if (nodes[i] != null) throw new IllegalStateException("not released node");
            }

        }

    }

    @Override
    public String printDiagram(String prefix, int level) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numChildren; i++) {
            Object node = nodes[i];
            String key = String.format("%02X", keys[i]);
            String x = (i == 0 ? (numChildren == 1 ? "──" : "┬─") : (i + 1 == numChildren ? (prefix + "└─") : (prefix + "├─")));

            if (level == 0) {
                sb.append(x + key + " = " + node);
            } else {
                sb.append(x + key + "" + (((IArtNode<V>) node).printDiagram(prefix + (i + 1 == numChildren ? "    " : "│   "), level - 8)));
            }
            if (i < numChildren - 1) {
                sb.append("\n");
            } else if (level == 0) {
                sb.append("\n" + prefix);
            }
        }
        return sb.toString();
    }

    static short toNodeIndex(long key, int level) {
        return (short) ((key >>> level) & 0xFF);
    }
}
