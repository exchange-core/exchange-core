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


import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import lombok.Builder;
import lombok.RequiredArgsConstructor;

@Builder
@RequiredArgsConstructor
public final class ApiPlaceOrder extends ApiCommand {

    public final long price;
    public final long size;
    public final long orderId;
    public final OrderAction action;
    public final OrderType orderType;
    public final long uid;
    public final int symbol;
    public final int userCookie;
    public final long reservePrice;

    @Override
    public String toString() {
        return "[ADD o" + orderId + " s" + symbol + " u" + uid + " " + (action == OrderAction.ASK ? 'A' : 'B')
                + ":" + (orderType == OrderType.IOC ? "IOC" : "GTC")
                + ":" + price + ":" + size + "]";
        //(reservePrice != 0 ? ("(R" + reservePrice + ")") : "") +
    }
}
