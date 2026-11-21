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
package exchange.core2.core.common.api;

import exchange.core2.core.common.BalanceAdjustmentType;

import java.util.Objects;

public final class ApiAdjustUserBalance extends ApiCommand {

    public final long uid;

    public final int currency;
    public final long amount;
    public final long transactionId;

    public final BalanceAdjustmentType adjustmentType;

    public ApiAdjustUserBalance(long uid, int currency, long amount, long transactionId) {
        this(uid, currency, amount, transactionId, BalanceAdjustmentType.ADJUSTMENT);
    }

    @Override
    public String toString() {
        String amountFmt = String.format("%s%d c%d", amount >= 0 ? "+" : "-", Math.abs(amount), currency);
        return "[ADJUST_BALANCE " + uid + " id:" + transactionId + " " + amountFmt + " " + adjustmentType + "]";

    }

    public ApiAdjustUserBalance(long uid, int currency, long amount, long transactionId,
                                BalanceAdjustmentType adjustmentType) {
        this.uid = uid;
        this.currency = currency;
        this.amount = amount;
        this.transactionId = transactionId;
        this.adjustmentType = adjustmentType;
    }

    public static ApiAdjustUserBalanceBuilder builder() {
        return new ApiAdjustUserBalanceBuilder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof ApiAdjustUserBalance other
                && other.canEqual(this)
                && this.uid == other.uid
                && this.currency == other.currency
                && this.amount == other.amount
                && this.transactionId == other.transactionId
                && Objects.equals(this.adjustmentType, other.adjustmentType);
    }

    protected boolean canEqual(Object other) {
        return other instanceof ApiAdjustUserBalance;
    }

    @Override
    public int hashCode() {
        final int prime = 59;
        int result = 1;
        result = result * prime + Long.hashCode(this.uid);
        result = result * prime + this.currency;
        result = result * prime + Long.hashCode(this.amount);
        result = result * prime + Long.hashCode(this.transactionId);
        var adjustmentType = this.adjustmentType;
        result = result * prime + (adjustmentType == null ? 43 : adjustmentType.hashCode());
        return result;
    }

    public static class ApiAdjustUserBalanceBuilder {
        private long uid;
        private int currency;
        private long amount;
        private long transactionId;
        private BalanceAdjustmentType adjustmentType = BalanceAdjustmentType.ADJUSTMENT;

        ApiAdjustUserBalanceBuilder() {
        }

        public ApiAdjustUserBalanceBuilder uid(long uid) {
            this.uid = uid;
            return this;
        }

        public ApiAdjustUserBalanceBuilder currency(int currency) {
            this.currency = currency;
            return this;
        }

        public ApiAdjustUserBalanceBuilder amount(long amount) {
            this.amount = amount;
            return this;
        }

        public ApiAdjustUserBalanceBuilder transactionId(long transactionId) {
            this.transactionId = transactionId;
            return this;
        }

        public ApiAdjustUserBalanceBuilder adjustmentType(BalanceAdjustmentType adjustmentType) {
            this.adjustmentType = adjustmentType;
            return this;
        }

        public ApiAdjustUserBalance build() {
            return new ApiAdjustUserBalance(uid, currency, amount, transactionId, adjustmentType);
        }

        @Override
        public String toString() {
            return "ApiAdjustUserBalance.ApiAdjustUserBalanceBuilder(uid=" + this.uid
                    + ", currency=" + this.currency
                    + ", amount=" + this.amount
                    + ", transactionId=" + this.transactionId
                    + ", adjustmentType=" + this.adjustmentType + ")";
        }
    }
}
