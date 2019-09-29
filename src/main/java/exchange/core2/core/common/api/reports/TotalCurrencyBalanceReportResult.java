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
import lombok.AllArgsConstructor;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;

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
        this.accountBalances = bytesIn.readBoolean() ? SerializationUtils.readIntLongHashMap(bytesIn) : null;
        this.fees = bytesIn.readBoolean() ? SerializationUtils.readIntLongHashMap(bytesIn) : null;
        this.ordersBalances = bytesIn.readBoolean() ? SerializationUtils.readIntLongHashMap(bytesIn) : null;
        this.openInterestLong = bytesIn.readBoolean() ? SerializationUtils.readIntLongHashMap(bytesIn) : null;
        this.openInterestShort = bytesIn.readBoolean() ? SerializationUtils.readIntLongHashMap(bytesIn) : null;
    }

    @Override
    public void writeMarshallable(final BytesOut bytes) {
        bytes.writeBoolean(accountBalances != null);
        if (accountBalances != null) {
            SerializationUtils.marshallIntLongHashMap(accountBalances, bytes);
        }

        bytes.writeBoolean(fees != null);
        if (fees != null) {
            SerializationUtils.marshallIntLongHashMap(fees, bytes);
        }

        bytes.writeBoolean(ordersBalances != null);
        if (ordersBalances != null) {
            SerializationUtils.marshallIntLongHashMap(ordersBalances, bytes);
        }

        bytes.writeBoolean(openInterestLong != null);
        if (openInterestLong != null) {
            SerializationUtils.marshallIntLongHashMap(openInterestLong, bytes);
        }

        bytes.writeBoolean(openInterestShort != null);
        if (openInterestShort != null) {
            SerializationUtils.marshallIntLongHashMap(openInterestShort, bytes);
        }
    }

    public IntLongHashMap getSum() {
        return SerializationUtils.mergeSum(SerializationUtils.mergeSum(accountBalances, ordersBalances), fees);
    }

    public static TotalCurrencyBalanceReportResult merge(final Stream<BytesIn> pieces) {
        return pieces
                .map(TotalCurrencyBalanceReportResult::new)
                .reduce(
                        new TotalCurrencyBalanceReportResult(null, null, null, null, null),
                        (a, b) -> new TotalCurrencyBalanceReportResult(
                                SerializationUtils.mergeSum(a.accountBalances, b.accountBalances),
                                SerializationUtils.mergeSum(a.fees, b.fees),
                                SerializationUtils.mergeSum(a.ordersBalances, b.ordersBalances),
                                SerializationUtils.mergeSum(a.openInterestLong, b.openInterestLong),
                                SerializationUtils.mergeSum(a.openInterestShort, b.openInterestShort)));
    }

}
