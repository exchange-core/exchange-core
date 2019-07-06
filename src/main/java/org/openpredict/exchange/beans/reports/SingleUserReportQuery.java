package org.openpredict.exchange.beans.reports;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.openpredict.exchange.beans.ReportType;

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
