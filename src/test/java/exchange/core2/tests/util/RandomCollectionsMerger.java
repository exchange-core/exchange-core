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
package exchange.core2.tests.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.util.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Spliterator;
import java.util.stream.Collectors;

@Slf4j
public class RandomCollectionsMerger {

    public static <T> ArrayList<T> mergeCollections(final Collection<? extends Collection<T>> chunks, final long seed) {

        final JDKRandomGenerator jdkRandomGenerator = new JDKRandomGenerator(Long.hashCode(seed));

        final ArrayList<T> mergedResult = new ArrayList<>();

        // create initial weight pairs
        List<Pair<Spliterator<T>, Double>> weightPairs = chunks.stream()
                .map(chunk -> Pair.create(chunk.spliterator(), (double) chunk.size()))
                .collect(Collectors.toList());

        while (!weightPairs.isEmpty()) {

            final EnumeratedDistribution<Spliterator<T>> ed = new EnumeratedDistribution<>(jdkRandomGenerator, weightPairs);

            // take random elements until face too many misses
            int missCounter = 0;
            while (missCounter++ < 3) {
                final Spliterator<T> sample = ed.sample();
                if (sample.tryAdvance(mergedResult::add)) {
                    missCounter = 0;
                }
            }

            // as empty queues leading to misses - rebuild wight pairs without them
            weightPairs = weightPairs.stream()
                    .filter(p -> p.getFirst().estimateSize() > 0)
                    .map(p -> Pair.create(p.getFirst(), (double) p.getFirst().estimateSize()))
                    .collect(Collectors.toList());

//            log.debug("rebuild size {}", weightPairs.size());
        }

        return mergedResult;
    }
}
