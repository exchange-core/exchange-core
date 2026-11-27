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
package exchange.core2.core.common;

import java.util.UUID;

public interface IOrder extends StateHash {

    long getPrice();

    long getSize();

    long getFilled();

    // user UUID halves
    long getUidHi();

    long getUidLo();

    // allocates; for reports and logs
    default UUID getUid() {
        return new UUID(getUidHi(), getUidLo());
    }

    OrderAction getAction();

    long getOrderId();

    long getTimestamp();

    long getReserveBidPrice();

    default long getOrderTakerFee() {
        return -1;
    }

    default long getOrderMakerFee() {
        return -1;
    }

    /**
     * @return fee overrides, null when the order uses symbol fees
     */
    default OrderFees getOrderFees() {
        return OrderFees.of(getOrderTakerFee(), getOrderMakerFee());
    }

}
