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

    public static long calculateAmountBidTakerFee(long size, long price, int firstX, CoreSymbolSpecification spec) {
        return size * (price * spec.quoteScaleK + ( (firstX != -1) ? firstX : spec.takerFee));
    }

    public static long calculateAmountBidReleaseCorrMaker(long size, long priceDiff, CoreSymbolSpecification spec, int firstX, int firstY) {
        return size * (priceDiff * spec.quoteScaleK + (( (firstX != -1) ? firstX : spec.takerFee) - ( (firstY != -1) ? firstY : spec.makerFee)));
    }

    public static long calculateAmountBidTakerFeeForBudget(long size, long budgetInSteps, int firstX, CoreSymbolSpecification spec) {

        return budgetInSteps * spec.quoteScaleK + size * ( (firstX != -1) ? firstX : spec.takerFee);
    }

}
