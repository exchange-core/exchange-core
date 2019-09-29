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
import exchange.core2.core.common.OrderAction;

public class CoreArithmeticUtils {

    public static long calculateHoldAmount(OrderAction action, long size, long price, CoreSymbolSpecification spec) {
        return action == OrderAction.BID ? calculateAmountBidTakerFee(size, price, spec) : calculateAmountAsk(size, spec);
    }

    public static long calculateAmountAsk(long size, CoreSymbolSpecification spec) {
        return size * spec.baseScaleK;
    }

    public static long calculateAmountBid(long size, long price, CoreSymbolSpecification spec) {
        return size * (price * spec.quoteScaleK);
    }

    public static long calculateAmountBidTakerFee(long size, long price, CoreSymbolSpecification spec) {
        return size * (price * spec.quoteScaleK + spec.takerFee);
    }

    public static long calculateAmountBidReleaseCorr(long size, long priceDiff, CoreSymbolSpecification spec, boolean isTaker) {
        return size * (priceDiff * spec.quoteScaleK + (isTaker ? 0 : (spec.takerFee - spec.makerFee)));
    }

}
