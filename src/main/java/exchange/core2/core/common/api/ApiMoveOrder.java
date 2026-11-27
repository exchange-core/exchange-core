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

import java.util.Objects;
import java.util.UUID;

public final class ApiMoveOrder extends ApiCommand {

    public final UUID orderId;

    public final long newPrice;

    public final UUID uid;
    public final int symbol;

    @Override
    public String toString() {
        return "[MOVE " + orderId + " " + newPrice + " u" + uid + " s" + symbol + "]";
    }

    public ApiMoveOrder(UUID orderId, long newPrice, UUID uid, int symbol) {
        this.orderId = orderId;
        this.newPrice = newPrice;
        this.uid = uid;
        this.symbol = symbol;
    }

    public static ApiMoveOrderBuilder builder() {
        return new ApiMoveOrderBuilder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof ApiMoveOrder other
                && other.canEqual(this)
                && Objects.equals(this.orderId, other.orderId)
                && this.newPrice == other.newPrice
                && Objects.equals(this.uid, other.uid)
                && this.symbol == other.symbol;
    }

    protected boolean canEqual(Object other) {
        return other instanceof ApiMoveOrder;
    }

    @Override
    public int hashCode() {
        final int prime = 59;
        int result = 1;
        result = result * prime + Objects.hashCode(this.orderId);
        result = result * prime + Long.hashCode(this.newPrice);
        result = result * prime + Objects.hashCode(this.uid);
        result = result * prime + this.symbol;
        return result;
    }

    public static class ApiMoveOrderBuilder {
        private UUID orderId;
        private long newPrice;
        private UUID uid;
        private int symbol;

        ApiMoveOrderBuilder() {
        }

        public ApiMoveOrderBuilder orderId(UUID orderId) {
            this.orderId = orderId;
            return this;
        }

        public ApiMoveOrderBuilder newPrice(long newPrice) {
            this.newPrice = newPrice;
            return this;
        }

        public ApiMoveOrderBuilder uid(UUID uid) {
            this.uid = uid;
            return this;
        }

        public ApiMoveOrderBuilder symbol(int symbol) {
            this.symbol = symbol;
            return this;
        }

        public ApiMoveOrder build() {
            return new ApiMoveOrder(this.orderId, this.newPrice, this.uid, this.symbol);
        }

        @Override
        public String toString() {
            return "ApiMoveOrder.ApiMoveOrderBuilder(orderId=" + this.orderId
                    + ", newPrice=" + this.newPrice
                    + ", uid=" + this.uid
                    + ", symbol=" + this.symbol + ")";
        }
    }
}
