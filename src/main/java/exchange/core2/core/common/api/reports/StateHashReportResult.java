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
package exchange.core2.core.common.api.reports;


import exchange.core2.core.utils.SerializationUtils;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Stream;

@Getter
@Slf4j
@EqualsAndHashCode
@RequiredArgsConstructor
@ToString
public final class StateHashReportResult implements ReportResult {

    public static final StateHashReportResult EMPTY = new StateHashReportResult(new TreeMap<>());
    private static final Comparator<SubmoduleKey> SUBMODULE_KEY_COMPARATOR =
            Comparator.<SubmoduleKey>comparingInt(k -> k.submodule.code)
                    .thenComparing(k -> k.moduleId);
    private final SortedMap<SubmoduleKey, Integer> hashCodes;

    private StateHashReportResult(final BytesIn bytesIn) {

        this.hashCodes = SerializationUtils.readGenericMap(bytesIn, TreeMap::new, SubmoduleKey::new, BytesIn::readInt);
    }

    public static StateHashReportResult merge(final Stream<BytesIn> pieces) {

        return pieces.map(StateHashReportResult::new)
                .reduce(EMPTY, (r1, r2) -> {
                    SortedMap<SubmoduleKey, Integer> hashcodes = new TreeMap<>(r1.hashCodes);
                    hashcodes.putAll(r2.hashCodes);
                    return new StateHashReportResult(hashcodes);
                });
    }

    public static SubmoduleKey createKey(int moduleId, SubmoduleType submoduleType) {
        return new SubmoduleKey(moduleId, submoduleType);
    }

    @Override
    public void writeMarshallable(final BytesOut bytes) {
        SerializationUtils.marshallGenericMap(hashCodes, bytes, (b, k) -> k.writeMarshallable(b), BytesOut::writeInt);
    }

    public int getStateHash() {

        final int[] hashes = hashCodes.entrySet().stream()
                .mapToInt(e -> Objects.hash(e.getKey(), e.getValue())).toArray();

        return Arrays.hashCode(hashes);
    }

    public enum ModuleType {
        RISK_ENGINE,
        MATCHING_ENGINE
    }

    @AllArgsConstructor
    public enum SubmoduleType {
        RISK_SYMBOL_SPEC_PROVIDER(0, ModuleType.RISK_ENGINE),
        RISK_USER_PROFILE_SERVICE(1, ModuleType.RISK_ENGINE),
        RISK_BINARY_CMD_PROCESSOR(2, ModuleType.MATCHING_ENGINE),
        RISK_LAST_PRICE_CACHE(3, ModuleType.RISK_ENGINE),
        RISK_FEES(4, ModuleType.RISK_ENGINE),
        RISK_ADJUSTMENTS(5, ModuleType.RISK_ENGINE),
        RISK_SUSPENDS(6, ModuleType.RISK_ENGINE),
        RISK_SHARD_MASK(7, ModuleType.RISK_ENGINE),

        MATCHING_BINARY_CMD_PROCESSOR(64, ModuleType.MATCHING_ENGINE),
        MATCHING_ORDER_BOOKS(65, ModuleType.MATCHING_ENGINE),
        MATCHING_SHARD_MASK(66, ModuleType.MATCHING_ENGINE);

        public final int code;
        public final ModuleType moduleType;

        public static SubmoduleType fromCode(int code) {
            return Arrays.stream(values()).filter(c -> c.code == code).findFirst().orElseThrow(() -> new IllegalArgumentException("Unknown SubmoduleType"));
        }
    }

    @RequiredArgsConstructor
    @EqualsAndHashCode
    public static final class SubmoduleKey implements WriteBytesMarshallable, Comparable<SubmoduleKey> {

        public final int moduleId;
        public final SubmoduleType submodule;

        private SubmoduleKey(final BytesIn bytesIn) {
            this.moduleId = bytesIn.readInt();
            this.submodule = SubmoduleType.fromCode(bytesIn.readInt());
        }

        @Override
        public void writeMarshallable(BytesOut bytes) {
            bytes.writeInt(moduleId);
            bytes.writeInt(submodule.code);
        }

        @Override
        public int compareTo(@NotNull SubmoduleKey o) {
            return SUBMODULE_KEY_COMPARATOR.compare(this, o);
        }
    }
}
