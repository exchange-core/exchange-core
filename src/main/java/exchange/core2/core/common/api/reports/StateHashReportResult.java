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


import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@AllArgsConstructor
@Getter
@Slf4j
@EqualsAndHashCode
@ToString
public final class StateHashReportResult implements ReportResult {

    private int stateHashRiskEngines;
    private int stateHashMatcherEngines;

    private StateHashReportResult(final BytesIn bytesIn) {
        this.stateHashRiskEngines = bytesIn.readInt();
        this.stateHashMatcherEngines = bytesIn.readInt();
    }

    @Override
    public void writeMarshallable(final BytesOut bytes) {
        bytes.writeInt(stateHashRiskEngines);
        bytes.writeInt(stateHashMatcherEngines);
    }

    public int getStateHash() {
        return Objects.hash(stateHashMatcherEngines, stateHashRiskEngines);
    }

    public static StateHashReportResult merge(final Stream<BytesIn> pieces) {

        final List<StateHashReportResult> results = pieces.map(StateHashReportResult::new).collect(Collectors.toList());
        final int[] riskHashes = results.stream().mapToInt(StateHashReportResult::getStateHashRiskEngines).sorted().toArray();
        final int[] matcherHashes = results.stream().mapToInt(StateHashReportResult::getStateHashMatcherEngines).sorted().toArray();

        return new StateHashReportResult(Arrays.hashCode(riskHashes), Arrays.hashCode(matcherHashes));
    }

}
