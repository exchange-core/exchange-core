package org.openpredict.exchange.beans.reports;

import lombok.NoArgsConstructor;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.openpredict.exchange.beans.ReportType;

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
