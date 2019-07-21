package org.openpredict.exchange.beans.api.reports;


import lombok.AllArgsConstructor;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.openpredict.exchange.core.Utils;

import java.util.stream.Stream;

@AllArgsConstructor
@Getter
public class TotalCurrencyBalanceReportResult implements ReportResult {

    // currency -> balance
    private IntLongHashMap accountBalances;
    private IntLongHashMap fees;
    private IntLongHashMap ordersBalances;

    // symbol -> volume
    // We have to keep shorts and longs separately because for multi-core processing different risk engine instances will give non-matching results.
    // They should match when aggregated though.
    private IntLongHashMap openInterestLong;
    private IntLongHashMap openInterestShort;

    private TotalCurrencyBalanceReportResult(final BytesIn bytesIn) {
        this.accountBalances = bytesIn.readBoolean() ? Utils.readIntLongHashMap(bytesIn) : null;
        this.fees = bytesIn.readBoolean() ? Utils.readIntLongHashMap(bytesIn) : null;
        this.ordersBalances = bytesIn.readBoolean() ? Utils.readIntLongHashMap(bytesIn) : null;
        this.openInterestLong = bytesIn.readBoolean() ? Utils.readIntLongHashMap(bytesIn) : null;
        this.openInterestShort = bytesIn.readBoolean() ? Utils.readIntLongHashMap(bytesIn) : null;
    }

    @Override
    public void writeMarshallable(final BytesOut bytes) {
        bytes.writeBoolean(accountBalances != null);
        if (accountBalances != null) {
            Utils.marshallIntLongHashMap(accountBalances, bytes);
        }

        bytes.writeBoolean(fees != null);
        if (fees != null) {
            Utils.marshallIntLongHashMap(fees, bytes);
        }

        bytes.writeBoolean(ordersBalances != null);
        if (ordersBalances != null) {
            Utils.marshallIntLongHashMap(ordersBalances, bytes);
        }

        bytes.writeBoolean(openInterestLong != null);
        if (openInterestLong != null) {
            Utils.marshallIntLongHashMap(openInterestLong, bytes);
        }

        bytes.writeBoolean(openInterestShort != null);
        if (openInterestShort != null) {
            Utils.marshallIntLongHashMap(openInterestShort, bytes);
        }
    }

    public IntLongHashMap getSum() {
        return Utils.mergeSum(Utils.mergeSum(accountBalances, ordersBalances), fees);
    }

    public static TotalCurrencyBalanceReportResult merge(final Stream<BytesIn> pieces) {
        return pieces
                .map(TotalCurrencyBalanceReportResult::new)
                .reduce(
                        new TotalCurrencyBalanceReportResult(null, null, null, null, null),
                        (a, b) -> new TotalCurrencyBalanceReportResult(
                                Utils.mergeSum(a.accountBalances, b.accountBalances),
                                Utils.mergeSum(a.fees, b.fees),
                                Utils.mergeSum(a.ordersBalances, b.ordersBalances),
                                Utils.mergeSum(a.openInterestLong, b.openInterestLong),
                                Utils.mergeSum(a.openInterestShort, b.openInterestShort)));
    }

}
