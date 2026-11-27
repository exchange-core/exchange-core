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

public final class ApiReduceOrder extends ApiCommand {

    public final UUID orderId;

    public final UUID uid;
    public final int symbol;
    public final long reduceSize;

    @Override
    public String toString() {
        return "[REDUCE " + orderId + " by " + reduceSize + "]";
    }

    public ApiReduceOrder(UUID orderId, UUID uid, int symbol, long reduceSize) {
        this.orderId = orderId;
        this.uid = uid;
        this.symbol = symbol;
        this.reduceSize = reduceSize;
    }

    public static ApiReduceOrderBuilder builder() {
        return new ApiReduceOrderBuilder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof ApiReduceOrder other
                && other.canEqual(this)
                && Objects.equals(this.orderId, other.orderId)
                && Objects.equals(this.uid, other.uid)
                && this.symbol == other.symbol
                && this.reduceSize == other.reduceSize;
    }

    protected boolean canEqual(Object other) {
        return other instanceof ApiReduceOrder;
    }

    @Override
    public int hashCode() {
        final int prime = 59;
        int result = 1;
        result = result * prime + Objects.hashCode(this.orderId);
        result = result * prime + Objects.hashCode(this.uid);
        result = result * prime + this.symbol;
        result = result * prime + Long.hashCode(this.reduceSize);
        return result;
    }

    public static class ApiReduceOrderBuilder {
        private UUID orderId;
        private UUID uid;
        private int symbol;
        private long reduceSize;

        ApiReduceOrderBuilder() {
        }

        public ApiReduceOrderBuilder orderId(UUID orderId) {
            this.orderId = orderId;
            return this;
        }

        public ApiReduceOrderBuilder uid(UUID uid) {
            this.uid = uid;
            return this;
        }

        public ApiReduceOrderBuilder symbol(int symbol) {
            this.symbol = symbol;
            return this;
        }

        public ApiReduceOrderBuilder reduceSize(long reduceSize) {
            this.reduceSize = reduceSize;
            return this;
        }

        public ApiReduceOrder build() {
            return new ApiReduceOrder(this.orderId, this.uid, this.symbol, this.reduceSize);
        }

        @Override
        public String toString() {
            return "ApiReduceOrder.ApiReduceOrderBuilder(orderId=" + this.orderId
                    + ", uid=" + this.uid
                    + ", symbol=" + this.symbol
                    + ", reduceSize=" + this.reduceSize + ")";
        }
    }
}
