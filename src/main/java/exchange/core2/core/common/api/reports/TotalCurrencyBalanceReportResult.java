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
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;

import java.util.Objects;
import java.util.stream.Stream;

public final class TotalCurrencyBalanceReportResult implements ReportResult {

    // currency -> balance
    final private IntLongHashMap accountBalances;
    final private IntLongHashMap fees;
    final private IntLongHashMap adjustments;
    final private IntLongHashMap suspends;
    final private IntLongHashMap ordersBalances;

    // symbol -> volume
    // We have to keep shorts and longs separately because for multi-core processing different risk engine instances will give non-matching results.
    // They should match when aggregated though.
    final private IntLongHashMap openInterestLong;
    final private IntLongHashMap openInterestShort;

    public static TotalCurrencyBalanceReportResult createEmpty() {
        return new TotalCurrencyBalanceReportResult(
                null, null, null, null, null, null, null);
    }

    public static TotalCurrencyBalanceReportResult ofOrderBalances(final IntLongHashMap currencyBalance) {
        return new TotalCurrencyBalanceReportResult(
                null, null, null, null, currencyBalance, null, null);
    }

    private TotalCurrencyBalanceReportResult(final BytesIn bytesIn) {
        this.accountBalances = SerializationUtils.readNullable(bytesIn, SerializationUtils::readIntLongHashMap);
        this.fees = SerializationUtils.readNullable(bytesIn, SerializationUtils::readIntLongHashMap);
        this.adjustments = SerializationUtils.readNullable(bytesIn, SerializationUtils::readIntLongHashMap);
        this.suspends = SerializationUtils.readNullable(bytesIn, SerializationUtils::readIntLongHashMap);
        this.ordersBalances = SerializationUtils.readNullable(bytesIn, SerializationUtils::readIntLongHashMap);
        this.openInterestLong = SerializationUtils.readNullable(bytesIn, SerializationUtils::readIntLongHashMap);
        this.openInterestShort = SerializationUtils.readNullable(bytesIn, SerializationUtils::readIntLongHashMap);
    }

    @Override
    public void writeMarshallable(final BytesOut bytes) {
        SerializationUtils.marshallNullable(accountBalances, bytes, SerializationUtils::marshallIntLongHashMap);
        SerializationUtils.marshallNullable(fees, bytes, SerializationUtils::marshallIntLongHashMap);
        SerializationUtils.marshallNullable(adjustments, bytes, SerializationUtils::marshallIntLongHashMap);
        SerializationUtils.marshallNullable(suspends, bytes, SerializationUtils::marshallIntLongHashMap);
        SerializationUtils.marshallNullable(ordersBalances, bytes, SerializationUtils::marshallIntLongHashMap);
        SerializationUtils.marshallNullable(openInterestLong, bytes, SerializationUtils::marshallIntLongHashMap);
        SerializationUtils.marshallNullable(openInterestShort, bytes, SerializationUtils::marshallIntLongHashMap);
    }

    public IntLongHashMap getGlobalBalancesSum() {
        return SerializationUtils.mergeSum(accountBalances, ordersBalances, fees, adjustments, suspends);
    }

    public IntLongHashMap getClientsBalancesSum() {
        return SerializationUtils.mergeSum(accountBalances, ordersBalances, suspends);
    }

    public boolean isGlobalBalancesAllZero() {
        return getGlobalBalancesSum().allSatisfy(amount -> amount == 0L);
    }

    public static TotalCurrencyBalanceReportResult merge(final Stream<BytesIn> pieces) {
        return pieces
                .map(TotalCurrencyBalanceReportResult::new)
                .reduce(
                        TotalCurrencyBalanceReportResult.createEmpty(),
                        (a, b) -> new TotalCurrencyBalanceReportResult(
                                SerializationUtils.mergeSum(a.accountBalances, b.accountBalances),
                                SerializationUtils.mergeSum(a.fees, b.fees),
                                SerializationUtils.mergeSum(a.adjustments, b.adjustments),
                                SerializationUtils.mergeSum(a.suspends, b.suspends),
                                SerializationUtils.mergeSum(a.ordersBalances, b.ordersBalances),
                                SerializationUtils.mergeSum(a.openInterestLong, b.openInterestLong),
                                SerializationUtils.mergeSum(a.openInterestShort, b.openInterestShort)));
    }


    public TotalCurrencyBalanceReportResult(
            IntLongHashMap accountBalances,
            IntLongHashMap fees,
            IntLongHashMap adjustments,
            IntLongHashMap suspends,
            IntLongHashMap ordersBalances,
            IntLongHashMap openInterestLong,
            IntLongHashMap openInterestShort) {
        this.accountBalances = accountBalances;
        this.fees = fees;
        this.adjustments = adjustments;
        this.suspends = suspends;
        this.ordersBalances = ordersBalances;
        this.openInterestLong = openInterestLong;
        this.openInterestShort = openInterestShort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof TotalCurrencyBalanceReportResult other
                && Objects.equals(this.getAccountBalances(), other.getAccountBalances())
                && Objects.equals(this.getFees(), other.getFees())
                && Objects.equals(this.getAdjustments(), other.getAdjustments())
                && Objects.equals(this.getSuspends(), other.getSuspends())
                && Objects.equals(this.getOrdersBalances(), other.getOrdersBalances())
                && Objects.equals(this.getOpenInterestLong(), other.getOpenInterestLong())
                && Objects.equals(this.getOpenInterestShort(), other.getOpenInterestShort());
    }

    @Override
    public int hashCode() {
        final int prime = 59;
        int result = 1;
        var accountBalances = this.getAccountBalances();
        result = result * prime + (accountBalances == null ? 43 : accountBalances.hashCode());
        var fees = this.getFees();
        result = result * prime + (fees == null ? 43 : fees.hashCode());
        var adjustments = this.getAdjustments();
        result = result * prime + (adjustments == null ? 43 : adjustments.hashCode());
        var suspends = this.getSuspends();
        result = result * prime + (suspends == null ? 43 : suspends.hashCode());
        var ordersBalances = this.getOrdersBalances();
        result = result * prime + (ordersBalances == null ? 43 : ordersBalances.hashCode());
        var openInterestLong = this.getOpenInterestLong();
        result = result * prime + (openInterestLong == null ? 43 : openInterestLong.hashCode());
        var openInterestShort = this.getOpenInterestShort();
        result = result * prime + (openInterestShort == null ? 43 : openInterestShort.hashCode());
        return result;
    }

    public IntLongHashMap getAccountBalances() {
        return this.accountBalances;
    }

    public IntLongHashMap getFees() {
        return this.fees;
    }

    public IntLongHashMap getAdjustments() {
        return this.adjustments;
    }

    public IntLongHashMap getSuspends() {
        return this.suspends;
    }

    public IntLongHashMap getOrdersBalances() {
        return this.ordersBalances;
    }

    public IntLongHashMap getOpenInterestLong() {
        return this.openInterestLong;
    }

    public IntLongHashMap getOpenInterestShort() {
        return this.openInterestShort;
    }

    @Override
    public String toString() {
        return "TotalCurrencyBalanceReportResult(accountBalances=" + this.getAccountBalances()
                + ", fees=" + this.getFees()
                + ", adjustments=" + this.getAdjustments()
                + ", suspends=" + this.getSuspends()
                + ", ordersBalances=" + this.getOrdersBalances()
                + ", openInterestLong=" + this.getOpenInterestLong()
                + ", openInterestShort=" + this.getOpenInterestShort() + ")";
    }
}
