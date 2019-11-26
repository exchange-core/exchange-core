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
 */
public final class LongAdaptiveRadixTreeMap<V> {

    private static final int INITIAL_LEVEL = 56;

    private IArtNode<V> root = new ArtNode4<>();

    public V get(final long key) {
        return root.getValue(key, INITIAL_LEVEL);
    }

    public void put(final long key, final V value) {
        final IArtNode<V> upSizedNode = root.put(key, INITIAL_LEVEL, value);
        if (upSizedNode != null) {
            // TODO put old into the pool
            root = upSizedNode;
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
        final IArtNode<V> downSizeNode = root.remove(key, INITIAL_LEVEL);
        // ignore null because can not remove root
        if (downSizeNode != null && downSizeNode != root) {
            // TODO put old into the pool
            root = downSizeNode;
        }
    }

    public void validateInternalState() {
        root.validateInternalState();
    }


    String printDiagram(){
        return root.printDiagram("", INITIAL_LEVEL);
    }

    // TODO remove based on leaf  (having reference) ?

}
