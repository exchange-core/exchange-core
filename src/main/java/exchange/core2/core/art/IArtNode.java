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

import java.util.List;
import java.util.Map;

public interface IArtNode<V> {

    V getValue(long key, int level);

    IArtNode<V> put(long key, int level, V value);

    IArtNode<V> remove(long key, int level);

    V getCeilingValue(long key, int level);

    V getFloorValue(long key, int level);

    int forEach(LongObjConsumer<V> consumer, int limit);

    int forEachDesc(LongObjConsumer<V> consumer, int limit);

    /**
     * Get number of elements
     * Slow operation - O(n) complexity
     *
     * @param limit - can provide value to operation increase performance
     * @return if returned value less than limit - it is precise size of the node
     */
    int size(int limit);

    /**
     * For testing only
     */
    void validateInternalState(int level);

    /**
     * For testing only
     */
    String printDiagram(String prefix, int level);

    /**
     * For testing only
     */
    List<Map.Entry<Long, V>> entries();

}
