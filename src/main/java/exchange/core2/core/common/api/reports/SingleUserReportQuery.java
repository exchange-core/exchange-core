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
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;

import java.util.function.Function;
import java.util.stream.Stream;

public class SingleUserReportQuery implements ReportQuery<SingleUserReportResult> {

    private final long uid;

    public SingleUserReportQuery(long uid) {
        this.uid = uid;
    }

    public SingleUserReportQuery(final BytesIn bytesIn) {
        this.uid = bytesIn.readLong();
    }

    public long getUid() {
        return uid;
    }

    @Override
    public ReportType getReportType() {
        return ReportType.SINGLE_USER_REPORT;
    }

    @Override
    public Function<Stream<BytesIn>, SingleUserReportResult> getResultBuilder() {
        return SingleUserReportResult::merge;
    }

    @Override
    public void writeMarshallable(final BytesOut bytes) {
        bytes.writeLong(uid);
    }
}
