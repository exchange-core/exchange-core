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

    long nodeKey;
    int nodeLevel;

    byte numChildren;

    // default compact node constructor
    // terminal node has always nodeLevel=0
    ArtNode4(final long key, final V value) {
        // create compact node
        this.numChildren = 1;
        this.keys[0] = (short) (key & 0xFF);
        //this.nodes[0] = (level == 0) ? value : new ArtNode4<>(key, level - 8, value);
        this.nodes[0] = value;
        this.nodeKey = key;
        this.nodeLevel = 0;
    }

    // split-compact operation constructor
    // ! smallest key first
    ArtNode4(final long key1, final Object value1, final long key2, final Object value2, final int level) {
//        log.debug("new level={} key1={} key2={}", level, key1, key2);
        // create compact node
        this.numChildren = 2;
        this.keys[0] = (short) ((key1 >> level) & 0xFF);
        this.nodes[0] = value1;
        this.keys[1] = (short) ((key2 >> level) & 0xFF);
        this.nodes[1] = value2;

        this.nodeKey = key1; // leading part the same for both keys
        this.nodeLevel = level;
    }


    // downsize constructor 16->4
    ArtNode4(ArtNode16 artNode16) {
        this.numChildren = artNode16.numChildren;
        System.arraycopy(artNode16.keys, 0, this.keys, 0, numChildren);
        System.arraycopy(artNode16.nodes, 0, this.nodes, 0, numChildren);
        this.nodeLevel = artNode16.nodeLevel;
        this.nodeKey = artNode16.nodeKey;
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

//        log.debug(" ------ PUT {}", String.format("%X", key));
//        log.debug("level={} nodeLevel={}", level, nodeLevel);
//        log.debug("key={} nodeKey={}", key, nodeKey);

        if (level != nodeLevel) {
            final IArtNode<V> branch = LongAdaptiveRadixTreeMap.branchIfRequired(key, value, nodeKey, nodeLevel, this);
            if (branch != null) {
                return branch;
            }
        }

//        log.debug("PUT key:{} level:{} value:{}", key, level, value);

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
            if (nodeLevel == 0) {
                nodes[pos] = value;
            } else {
                // TODO take from pool
                // TODO create compressed-path node

                final ArtNode4 newSubNode = new ArtNode4(key, value);
                nodes[pos] = newSubNode;
                // TODO create compressed-path node
                //newSubNode.put();
            }
            numChildren++;
            return null;
        } else {
            // no space left, create a Node16 with new item
            return new ArtNode16<>(this, nodeIndex, (nodeLevel == 0) ? value : new ArtNode4<>(key, value));
        }
    }

    @Override
    public String toString() {
        return "ArtNode4{" +
                "nodeKey=" + nodeKey +
                ", nodeLevel=" + nodeLevel +
                ", numChildren=" + numChildren +
                '}';
    }

    @Override
    @SuppressWarnings("unchecked")
    public IArtNode<V> remove(long key, int level) {

//        String prefix = StringUtils.repeat(" ", (56 - level) / 4);
//        log.debug(prefix + " ------ REMOVE {}", String.format("%X", key));
        //          56 48 40 32 24 16  8  0
        // rem key  00 00 11 22 33 44 55 66

//        log.debug(prefix + "level={} nodeLevel={}", level, nodeLevel);
//        log.debug(prefix + "key={} nodeKey={}", key, nodeKey);


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
                // TODO put old into the pool
                // update resized node if capacity has decreased
                nodes[pos] = resizedNode;
                if (resizedNode == null) {
                    removeElementAtPos(pos);
                    if (numChildren == 1) {
//                        log.debug(prefix + "CAN MERGE! nodeLevel={} level={}", nodeLevel, level);
                        // todo put 'this' into pul
                        IArtNode<V> lastNode = (IArtNode<V>) nodes[0];
                        //   lastNode.setNodeLevel(nodeLevel);
                        return lastNode;
                    }
                }
            }
        }

        // indicate if removed last one
        return (numChildren == 0) ? null : this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V getCeilingValue(long key, int level) {
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
            if (nodeKeyWithMask < keyWithMask) {
                // compacted part is lower - no need to search for ceiling entry here
                return null;
            } else if (keyWithMask != nodeKeyWithMask) {
                // accept first existing entry, because compacted nodekey is higher
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
    public void validateInternalState(int level) {
        if (nodeLevel > level) throw new IllegalStateException("unexpected nodeLevel");
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
//        log.debug(">>>> {} level={} nodelevel={} nodekey={}", prefix, level, nodeLevel, nodeKey);


        return LongAdaptiveRadixTreeMap.printDiagram(prefix, level, nodeLevel, nodeKey, numChildren,
                idx -> keys[idx], idx -> nodes[idx]);
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

}
