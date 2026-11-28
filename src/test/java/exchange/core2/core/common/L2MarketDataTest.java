/*
 * Copyright 2019-2026 Maksim Zheravin
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
package exchange.core2.core.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class L2MarketDataTest {

    @Test
    void equalSnapshotsHaveTheSameHashDespiteUnusedLevelsAndMetadata() {
        final var compact = new L2MarketData(
                new long[]{110}, new long[]{20}, new long[]{2},
                new long[]{100}, new long[]{30}, new long[]{3});
        final var withSpareCapacity = new L2MarketData(
                new long[]{110, 120}, new long[]{20, 99}, new long[]{2, 9},
                new long[]{100, 90, 80}, new long[]{30, 99, 99}, new long[]{3, 9, 9});
        withSpareCapacity.askSize = 1;
        withSpareCapacity.bidSize = 1;
        withSpareCapacity.timestamp = 123L;
        withSpareCapacity.referenceSeq = 456L;

        assertEquals(compact, withSpareCapacity);
        assertEquals(compact.hashCode(), withSpareCapacity.hashCode());
    }

    @Test
    void equalEmptySnapshotsHaveTheSameHashWithOrWithoutBackingArrays() {
        final var withBuffers = new L2MarketData(4, 4);
        final var withoutBuffers = new L2MarketData(null, null, null, null, null, null);

        assertEquals(withBuffers, withoutBuffers);
        assertEquals(withBuffers.hashCode(), withoutBuffers.hashCode());
    }
}
