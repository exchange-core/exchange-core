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
package exchange.core2.core.utils;

import exchange.core2.core.common.CoreSymbolSpecification;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class CoreArithmeticUtils {

    public static long calculateAmountAsk(long size, CoreSymbolSpecification spec) {
        return size * spec.baseScaleK;
    }

    public static long calculateAmountBid(long size, long price, CoreSymbolSpecification spec) {
        return size * (price * spec.quoteScaleK);
    }

    public static long resolveFee(long override, long symbolFee) {
        return override == -1 ? symbolFee : override;
    }

    public static long bidHoldFee(long takerFee, long makerFee, CoreSymbolSpecification spec) {
        return Math.max(resolveFee(takerFee, spec.takerFee), resolveFee(makerFee, spec.makerFee));
    }

    public static long calculateAmountBidHold(long size, long price, long holdFee, CoreSymbolSpecification spec) {
        return Math.multiplyExact(size, Math.addExact(Math.multiplyExact(price, spec.quoteScaleK), holdFee));
    }

    public static long calculateAmountBidTakerFee(long size, long price, CoreSymbolSpecification spec) {
        return calculateAmountBidTakerFee(size, price, -1, spec);
    }

    public static long calculateAmountBidTakerFee(long size, long price, long orderTakerFee, CoreSymbolSpecification spec) {
        return calculateAmountBidHold(size, price, resolveFee(orderTakerFee, spec.takerFee), spec);
    }

    public static long calculateAmountBidReleaseCorrMaker(long size, long priceDiff, CoreSymbolSpecification spec) {
        return calculateAmountBidReleaseCorrMaker(size, priceDiff, spec, -1, -1);
    }

    public static long calculateAmountBidReleaseCorrMaker(long size, long priceDiff, CoreSymbolSpecification spec,
                                                           long orderTakerFee, long orderMakerFee) {
        final long held = bidHoldFee(orderTakerFee, orderMakerFee, spec);
        final long charged = resolveFee(orderMakerFee, spec.makerFee);
        return size * (priceDiff * spec.quoteScaleK + held - charged);
    }

    public static long calculateAmountBidTakerFeeForBudget(long size, long budgetInSteps, CoreSymbolSpecification spec) {
        return calculateAmountBidTakerFeeForBudget(size, budgetInSteps, -1, spec);
    }

    public static long calculateAmountBidTakerFeeForBudget(long size, long budgetInSteps,
                                                           long orderTakerFee, CoreSymbolSpecification spec) {
        return Math.addExact(Math.multiplyExact(budgetInSteps, spec.quoteScaleK),
                Math.multiplyExact(size, resolveFee(orderTakerFee, spec.takerFee)));
    }
}
