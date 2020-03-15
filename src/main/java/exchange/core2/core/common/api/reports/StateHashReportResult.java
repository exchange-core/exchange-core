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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;

import java.util.Arrays;
import java.util.stream.Stream;

@AllArgsConstructor
@Getter
@Slf4j
public final class StateHashReportResult implements ReportResult {

    // state hash
    private int stateHash;


    private StateHashReportResult(final BytesIn bytesIn) {
        this.stateHash = bytesIn.readInt();
    }

    @Override
    public void writeMarshallable(final BytesOut bytes) {
        bytes.writeInt(stateHash);
    }

    public static StateHashReportResult merge(final Stream<BytesIn> pieces) {

        final int[] pieceHashes = pieces
                .map(StateHashReportResult::new)
                .mapToInt(StateHashReportResult::getStateHash)
                .sorted() // make deterministic
                .toArray();

        final int stateHash = Arrays.hashCode(pieceHashes);
//        log.info("merged stateHash={} ",stateHash);
        return new StateHashReportResult(stateHash);
    }

}
