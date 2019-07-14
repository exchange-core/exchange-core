package org.openpredict.exchange.beans.api.reports;

import lombok.NoArgsConstructor;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.openpredict.exchange.beans.ReportType;

import java.util.function.Function;
import java.util.stream.Stream;

@NoArgsConstructor
public class TotalCurrencyBalanceReportQuery implements ReportQuery<TotalCurrencyBalanceReportResult> {

    public TotalCurrencyBalanceReportQuery(BytesIn bytesIn) {
        // do nothing
    }

    @Override
    public ReportType getReportType() {
        return ReportType.TOTAL_CURRENCY_BALANCE;
    }

    @Override
    public Function<Stream<BytesIn>, TotalCurrencyBalanceReportResult> getResultBuilder() {
        return TotalCurrencyBalanceReportResult::merge;
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        // do nothing
    }
}
