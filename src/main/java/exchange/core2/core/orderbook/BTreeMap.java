/*
 * Copyright 2017 Justin Wetherell <phishman3579@gmail.com>
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
package exchange.core2.core.orderbook;


import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;


/**
 * B-tree is a tree data structure that keeps data sorted and allows searches,
 * sequential access, insertions, and deletions in logarithmic time. The B-tree
 * is a generalization of a binary search tree in that a node can have more than
 * two children. Unlike self-balancing binary search trees, the B-tree is
 * optimized for systems that read and write large blocks of data. It is
 * commonly used in databases and file-systems.
 * <p>
 *
 * @author Justin Wetherell <phishman3579@gmail.com>
 * @see <a href="https://en.wikipedia.org/wiki/B-tree">B-Tree (Wikipedia)</a>
 * <br>
 */
@Slf4j
public class BTreeMap<T> {

    private final int minKeySize;
    private final int minChildrenSize;
    private final int maxKeySize;
    private final int maxChildrenSize;

    private Node<T> root = null;

    /**
     * Constructor for B-Tree of ordered parameter. Order here means minimum
     * number of keys in a non-root node.
     * order=1 -> Default to 2-3 Tree
     *
     * @param order of the B-Tree.
     */
    public BTreeMap(int order) {
        this.minKeySize = order;
        this.minChildrenSize = order + 1;
        this.maxKeySize = 2 * order;
        this.maxChildrenSize = 2 * order + 1;
    }

    public void put(long key, T value) {

        if (root == null) {
            root = new Node<>(null, maxKeySize, maxChildrenSize);
            root.putEntry(key, value);
            return;
        }

        Node<T> node = root;
        while (node.childrenSize != 0) {
            // Navigate

            // Lesser or equal
            long lesser = node.keys[0];
            if (key <= lesser) {
                node = node.getChild(0);
                continue;
            }

            // Greater
            int numberOfKeys = node.keysSize;
            int last = numberOfKeys - 1;
            long greater = node.keys[last];
            if (key > greater) {
                node = node.getChild(numberOfKeys);
                continue;
            }

            // Search internal nodes
            for (int i = 1; i < node.keysSize; i++) {
                long prev = node.keys[i - 1];
                long next = node.keys[i];
                if (key > prev && key <= next) {
                    node = node.getChild(i);
                    break;
                }
            }
        }

        log.debug("add into node: " + node);
        if (node.putEntry(key, value) && node.keysSize > maxKeySize) {
            // Need to split up
            split(node);
        }
    }

    /**
     * The node's key size is greater than maxKeySize, split down the middle.
     *
     * @param node to split.
     */
    private void split(final Node<T> node) {
        final int numberOfKeys = node.keysSize;
        final int medianIndex = numberOfKeys >> 1;

        Node<T> left = new Node<>(null, maxKeySize, maxChildrenSize);
        for (int i = 0; i < medianIndex; i++) {
            left.putEntry(node.keys[i], node.values[i]);
        }
        if (node.childrenSize > 0) {
            for (int j = 0; j <= medianIndex; j++) {
                left.addChild(node.getChild(j));
            }
        }

        final Node<T> right = new Node<>(null, maxKeySize, maxChildrenSize);
        for (int i = medianIndex + 1; i < numberOfKeys; i++) {
            right.putEntry(node.keys[i], node.values[i]);
        }
        if (node.childrenSize > 0) {
            for (int j = medianIndex + 1; j < node.childrenSize; j++) {
                right.addChild(node.getChild(j));
            }
        }


        if (node.parent == null) {
            // new root, height of tree is increased
            root = new Node<>(null, maxKeySize, maxChildrenSize);
            root.putEntry(node.keys[medianIndex], node.values[medianIndex]);
            node.parent = root;
            root.addChild(left);
            root.addChild(right);
        } else {
            // Move the median value up to the parent
            final Node<T> parent = node.parent;
            parent.putEntry(node.keys[medianIndex], node.values[medianIndex]);
            parent.removeChild(node);
            parent.addChild(left);
            parent.addChild(right);

            if (parent.keysSize > maxKeySize) split(parent);
        }
    }

    public T remove(final long key) {
        Node<T> node = getNode(key);
        return remove(key, node);
    }

    /**
     * Remove the value from the Node and check invariants
     *
     * @param value T to remove from the tree
     * @param node  Node to remove value from
     */
    private T remove(final long value, final Node<T> node) {
        if (node == null) {
            return null;
        }

        final int index = node.indexOf(value);
        final T removed = node.removeEntry(value);
        if (node.childrenSize == 0) {
            // leaf node
            if (node.parent != null && node.keysSize < minKeySize) {
                this.combined(node);
            } else if (node.parent == null && node.keysSize == 0) {
                // Removing root node with no keys or children
                root = null;
            }
        } else {
            // internal node
            final Node<T> lesser = node.getChild(index);
            final Node<T> greatest = getGreatestNode(lesser);
            transferGreatestValue(greatest, node);
            if (greatest.parent != null && greatest.keysSize < minKeySize) {
                combined(greatest);
            }
            if (greatest.childrenSize > maxChildrenSize) {
                split(greatest);
            }
        }

        return removed;
    }

    /**
     * Remove greatest valued key from node.
     *
     * @param node to remove greatest value from.
     * @return value removed;
     */
    private void transferGreatestValue(Node<T> node, Node<T> toNode) {
        if (node.keysSize > 0) {
            node.transferEntry(node.keysSize - 1, toNode);
        }
    }

    public void clear() {
        root = null;
    }

    public T get(final long key) {
        Node<T> node = getNode(key);
        return node != null ? node.get(key) : null;
    }

    /**
     * Get the node with key.
     *
     * @param key to find in the tree.
     * @return Node<T> with key.
     */
    private Node<T> getNode(final long key) {
        Node<T> node = root;
        while (node != null) {

            if (key < node.keys[0]) {
                if (node.childrenSize > 0) {
                    node = node.getChild(0);
                } else {
                    node = null;
                }
                continue;
            }

            final int last = node.keysSize - 1;
            if (key > node.keys[last]) {
                if (node.childrenSize > node.keysSize) {
                    node = node.getChild(node.keysSize);
                } else {
                    node = null;
                }
                continue;
            }

            for (int i = 0; i < node.keysSize; i++) {
                final long currentKey = node.keys[i];
                if (currentKey == key) {
                    return node;
                }

                final int next = i + 1;
                if (next <= last) {
                    long nextValue = node.keys[next];
                    if (currentKey < key && nextValue > key) {
                        if (next < node.childrenSize) {
                            node = node.getChild(next);
                            break;
                        }
                        return null;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Get the greatest valued child from node.
     *
     * @param nodeToGet child with the greatest value.
     * @return Node<T> child with greatest value.
     */
    private Node<T> getGreatestNode(Node<T> nodeToGet) {
        Node<T> node = nodeToGet;
        while (node.childrenSize > 0) {
            node = node.getChild(node.childrenSize - 1);
        }
        return node;
    }

    /**
     * Combined children keys with parent when size is less than minKeySize.
     *
     * @param node with children to combined.
     */
    private void combined(Node<T> node) {
        Node<T> parent = node.parent;
        int index = parent.indexOf(node);
        int indexOfLeftNeighbor = index - 1;
        int indexOfRightNeighbor = index + 1;

        final Node<T> rightNeighbor;
        final int rightNeighborSize;
        if (indexOfRightNeighbor < parent.childrenSize) {
            rightNeighbor = parent.getChild(indexOfRightNeighbor);
            rightNeighborSize = rightNeighbor.keysSize;
        } else {
            rightNeighbor = null;
            rightNeighborSize = -1;
        }

        // Try to borrow neighbor
        if (rightNeighbor != null && rightNeighborSize > minKeySize) {
            // Try to borrow from right neighbor
            int prev = getIndexOfPreviousValue(parent, rightNeighbor.keys[0]);
            parent.transferEntry(prev, node);
            rightNeighbor.transferEntry(0, parent);
            if (rightNeighbor.childrenSize > 0) {
                node.addChild(rightNeighbor.removeChild(0));
            }
        } else {
            Node<T> leftNeighbor = null;
            int leftNeighborSize = -minChildrenSize;
            if (indexOfLeftNeighbor >= 0) {
                leftNeighbor = parent.getChild(indexOfLeftNeighbor);
                leftNeighborSize = leftNeighbor.keysSize;
            }

            if (leftNeighbor != null && leftNeighborSize > minKeySize) {
                // Try to borrow from left neighbor
                long removeValue = leftNeighbor.keys[leftNeighbor.keysSize - 1];
                int prev = getIndexOfNextValue(parent, removeValue);
                parent.transferEntry(prev, node);
                leftNeighbor.transferEntry(leftNeighbor.keysSize - 1, parent);
                if (leftNeighbor.childrenSize > 0) {
                    node.addChild(leftNeighbor.removeChild(leftNeighbor.childrenSize - 1));
                }
            } else if (rightNeighbor != null && parent.keysSize > 0) {
                // Can't borrow from neighbors, try to combined with right neighbor
                long removeValue = rightNeighbor.keys[0];
                int prev = getIndexOfPreviousValue(parent, removeValue);
                parent.transferEntry(prev, node);
                parent.removeChild(rightNeighbor);
                for (int i = 0; i < rightNeighbor.keysSize; i++) {
                    node.putEntry(rightNeighbor.keys[i], rightNeighbor.values[i]);
                }
                for (int i = 0; i < rightNeighbor.childrenSize; i++) {
                    Node<T> c = rightNeighbor.getChild(i);
                    node.addChild(c);
                }

                if (parent.parent != null && parent.keysSize < minKeySize) {
                    // removing key made parent too small, combined up tree
                    this.combined(parent);
                } else if (parent.keysSize == 0) {
                    // parent no longer has keys, make this node the new root
                    // which decreases the height of the tree
                    node.parent = null;
                    root = node;
                }

            } else if (leftNeighbor != null && parent.keysSize > 0) {
                // Can't borrow from neighbors, try to combined with left neighbor
                long removeValue = leftNeighbor.keys[leftNeighbor.keysSize - 1];
                int prev = getIndexOfNextValue(parent, removeValue);
                parent.transferEntry(prev, node);
                parent.removeChild(leftNeighbor);
                for (int i = 0; i < leftNeighbor.keysSize; i++) {
                    node.putEntry(leftNeighbor.keys[i], leftNeighbor.values[i]);
                }
                for (int i = 0; i < leftNeighbor.childrenSize; i++) {
                    Node<T> c = leftNeighbor.getChild(i);
                    node.addChild(c);
                }

                if (parent.parent != null && parent.keysSize < minKeySize) {
                    // removing key made parent too small, combined up tree
                    this.combined(parent);
                } else if (parent.keysSize == 0) {
                    // parent no longer has keys, make this node the new root
                    // which decreases the height of the tree
                    node.parent = null;
                    root = node;
                }
            }
        }
    }

    /**
     * Get the index of previous key in node.
     *
     * @param node  to find the previous key in.
     * @param value to find a previous value for.
     * @return index of previous key or -1 if not found.
     */
    private int getIndexOfPreviousValue(Node<T> node, long value) {
        for (int i = 1; i < node.keysSize; i++) {
            long t = node.keys[i];
            if (t >= value)
                return i - 1;
        }
        return node.keysSize - 1;
    }

    /**
     * Get the index of next key in node.
     *
     * @param node  to find the next key in.
     * @param value to find a next value for.
     * @return index of next key or -1 if not found.
     */
    private int getIndexOfNextValue(Node<T> node, long value) {
        for (int i = 0; i < node.keysSize; i++) {
            long t = node.keys[i];
            if (t >= value)
                return i;
        }
        return node.keysSize - 1;
    }

    public void validate() {
        if (root != null) {
            validateNode(root);
        }
    }

    /**
     * Validate the node according to the B-Tree invariants.
     *
     * @param node to validate.
     */
    private void validateNode(Node<T> node) {
        int keySize = node.keysSize;
        if (keySize > 1) {
            // Make sure the keys are sorted
            for (int i = 1; i < keySize; i++) {
                long p = node.keys[i - 1];
                long n = node.keys[i];
                if (p > n) {
                    throw new IllegalStateException("keys are not sorted " + p + " > " + n);
                }
            }
        }
        int childrenSize = node.childrenSize;
        if (node.parent == null) {
            // root
            if (keySize > maxKeySize) {
                // check max key size. root does not have a min key size
                throw new IllegalStateException("keySize > maxKeySize");
            } else if (childrenSize == 0) {
                // if root, no children, and keys are valid
                return;
            } else if (childrenSize < 2) {
                // root should have zero or at least two children
                throw new IllegalStateException("root should have at least two children");
            } else if (childrenSize > maxChildrenSize) {
                throw new IllegalStateException("childrenSize > maxChildrenSize");
            }
        } else {
            // non-root
            if (keySize < minKeySize) {
                throw new IllegalStateException("keySize < minKeySize");
            } else if (keySize > maxKeySize) {
                throw new IllegalStateException("keySize > maxKeySize");
            } else if (childrenSize == 0) {
                return;
            } else if (keySize != (childrenSize - 1)) {
                // If there are children, there should be one more child than keys
                throw new IllegalStateException("If there are children, there should be one more child than keys");
            } else if (childrenSize < minChildrenSize) {
                throw new IllegalStateException("childrenSize < minChildrenSize");
            } else if (childrenSize > maxChildrenSize) {
                throw new IllegalStateException("childrenSize > maxChildrenSize");
            }
        }

        Node<T> first = node.getChild(0);
        // The first child's last key should be less than the node's first key
        if (first.keys[first.keysSize - 1] > node.keys[0])
            throw new IllegalStateException("The first child's last key should be less than the node's first key");

        Node<T> last = node.getChild(node.childrenSize - 1);
        // The last child's first key should be greater than the node's last key
        if (last.keys[0] < node.keys[node.keysSize - 1])
            throw new IllegalStateException("The last child's first key should be greater than the node's last key");

        // Check that each node's first and last key holds it's invariance
        for (int i = 1; i < node.keysSize; i++) {
            long p = node.keys[i - 1];
            long n = node.keys[i];
            Node<T> c = node.getChild(i);
            if (p > c.keys[0])
                throw new IllegalStateException("Check that each node's first and last key holds it's invariance : p > c.getKey(0)");

            if (n < c.keys[c.keysSize - 1]) {
                throw new IllegalStateException("Check that each node's first and last key holds it's invariance :" +
                        " n < c.getKey(c.keysSize - 1) n=" + n + " getKey=" + c.keys[c.keysSize - 1]);
            }
        }

        for (int i = 0; i < node.childrenSize; i++) {
            Node<T> c = node.getChild(i);
            this.validateNode(c);
        }

    }

    @Override
    public String toString() {
        return TreePrinter.getString(this);
    }

    private static class Node<T> {

        private final long[] keys;
        private final T[] values;
        private final Node<T>[] children;

        private int childrenSize;
        private Node<T> parent;
        private int keysSize;

        private static final Comparator<Node<?>> comparator = Comparator.comparing(x -> x.keys[0]);

        private Node(Node<T> parent, int maxKeySize, int maxChildrenSize) {
            this.parent = parent;
            this.keys = new long[maxKeySize + 1];
            this.values = (T[]) new Object[maxKeySize + 1];
            this.keysSize = 0;
            this.children = new Node[maxChildrenSize + 1];
            this.childrenSize = 0;
        }

        private int indexOf(long key) {
            for (int i = 0; i < keysSize; i++) {
                if (keys[i] == key) return i;
            }
            return -1;
        }

        private T get(long key) {
            for (int i = 0; i < keysSize; i++) {
                if (keys[i] == key) return values[i];
            }
            return null;
        }


        private boolean putEntry(long key, T value) {
            int i = 0;
            while (i < keysSize && keys[i] <= key) {
                if (keys[i] == key) {
                    values[i] = value;
                    return false;
                }
                i++;
            }
            System.arraycopy(keys, i, keys, i + 1, keysSize - i);
            System.arraycopy(values, i, values, i + 1, keysSize - i);
            keys[i] = key;
            values[i] = value;
            keysSize++;
            return true;
        }

        private T removeEntry(final long key) {
            T value = null;
            if (keysSize == 0) {
                return null;
            }
            for (int i = 0; i < keysSize; i++) {
                if (keys[i] == key) {
                    value = values[i];
                } else if (value != null) {
                    // shift the rest of the keys down
                    keys[i - 1] = keys[i];
                }
            }
            if (value != null) {
                keysSize--;
                keys[keysSize] = 0;
                values[keysSize] = null;
            }
            return value;
        }


        private void transferEntry(final int index, final Node<T> target) {
            if (index >= keysSize) {
                return;
            }

            target.putEntry(keys[index], values[index]);

            for (int i = index + 1; i < keysSize; i++) {
                // shift the rest of the keys down
                keys[i - 1] = keys[i];
            }
            keysSize--;
            keys[keysSize] = 0;
            values[keysSize] = null;
        }


        private Node<T> getChild(int index) {
            if (index >= childrenSize) {
                return null;
            }
            return children[index];
        }

        private int indexOf(Node<T> child) {
            for (int i = 0; i < childrenSize; i++) {
                if (children[i].equals(child))
                    return i;
            }
            return -1;
        }

        private boolean addChild(final Node<T> child) {
            child.parent = this;
            children[childrenSize++] = child;

            Arrays.sort(children, 0, childrenSize, comparator);
            return true;
        }

        private boolean removeChild(final Node<T> child) {
            if (childrenSize == 0) {
                return false;
            }
            boolean found = false;
            for (int i = 0; i < childrenSize; i++) {
                if (children[i].equals(child)) {
                    found = true;
                } else if (found) {
                    // shift the rest of the keys down
                    children[i - 1] = children[i];
                }
            }
            if (found) {
                childrenSize--;
                children[childrenSize] = null;
            }
            return found;
        }

        private Node<T> removeChild(final int index) {
            if (index >= childrenSize)
                return null;
            Node<T> value = children[index];
            children[index] = null;
            for (int i = index + 1; i < childrenSize; i++) {
                // shift the rest of the keys down
                children[i - 1] = children[i];
            }
            childrenSize--;
            children[childrenSize] = null;
            return value;
        }


        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();

            builder.append("keys=[");
            for (int i = 0; i < keysSize; i++) {
                long value = keys[i];
                builder.append(value);
                if (i < keysSize - 1)
                    builder.append(", ");
            }
            builder.append("]\n");

            if (parent != null) {
                builder.append("parent=[");
                for (int i = 0; i < parent.keysSize; i++) {
                    long value = parent.keys[i];
                    builder.append(value);
                    if (i < parent.keysSize - 1)
                        builder.append(", ");
                }
                builder.append("]\n");
            }

            if (children != null) {
                builder.append("keySize=").append(keysSize).append(" children=").append(childrenSize).append("\n");
            }

            return builder.toString();
        }

    }

    private static class TreePrinter {

        public static String getString(BTreeMap<?> tree) {
            if (tree.root == null) return "Tree has no nodes.";
            return getString(tree.root, "", true);
        }

        private static String getString(Node<?> node, String prefix, boolean isTail) {
            StringBuilder builder = new StringBuilder();

            builder.append(prefix).append((isTail ? "`-- " : "|-- "));
            for (int i = 0; i < node.keysSize; i++) {
                long key = node.keys[i];
                builder.append(key).append("=").append(node.values[i]);
                if (i < node.keysSize - 1)
                    builder.append(", ");
            }
            builder.append("\n");

            if (node.children != null) {
                for (int i = 0; i < node.childrenSize - 1; i++) {
                    Node<?> obj = node.getChild(i);
                    builder.append(getString(obj, prefix + (isTail ? "    " : "|   "), false));
                }
                if (node.childrenSize >= 1) {
                    Node<?> obj = node.getChild(node.childrenSize - 1);
                    builder.append(getString(obj, prefix + (isTail ? "    " : "|   "), true));
                }
            }

            return builder.toString();
        }
    }


    public static void main(String[] args) {

        BTreeMap<Long> map = new BTreeMap<>(3);

        Random random = new Random(0);

        for (int i = 0; i < 1000; i++) {
            long x = 1 + random.nextInt(200);

            if(random.nextInt(3) == 0) {
                System.out.println("\n " + i + ". add " + x);
                map.put(x, x * 10);
            }else{
                System.out.println("\n " + i + ". remove " + x);
                map.remove(x);
            }

            System.out.println(map);

            map.validate();
        }

    }
}