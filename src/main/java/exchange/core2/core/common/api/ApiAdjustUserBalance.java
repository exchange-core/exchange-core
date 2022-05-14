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
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;

@Builder
@EqualsAndHashCode(callSuper = false)
@AllArgsConstructor
public final class ApiAdjustUserBalance extends ApiCommand {

    public final long uid;

    public final int currency;
    public final long amount;
    public final long transactionId;

    public final BalanceAdjustmentType adjustmentType;

    @Override
    public String toString() {
        String amountFmt = String.format("%s%d c%d", amount >= 0 ? "+" : "-", Math.abs(amount), currency);
        return "[ADJUST_BALANCE " + uid + " id:" + transactionId + " " + amountFmt + " " + adjustmentType + "]";

    }
}
