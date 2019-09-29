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

import exchange.core2.core.common.ReportType;
import lombok.NoArgsConstructor;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;

import java.util.function.Function;
import java.util.stream.Stream;

@NoArgsConstructor
public class StateHashReportQuery implements ReportQuery<StateHashReportResult> {

    public StateHashReportQuery(BytesIn bytesIn) {
        // do nothing
    }

    @Override
    public ReportType getReportType() {
        return ReportType.STATE_HASH;
    }

    @Override
    public Function<Stream<BytesIn>, StateHashReportResult> getResultBuilder() {
        return StateHashReportResult::merge;
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        // do nothing
    }
}
