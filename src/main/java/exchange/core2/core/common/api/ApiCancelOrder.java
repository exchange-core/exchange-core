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

public final class ApiCancelOrder extends ApiCommand {

    public final long orderId;

    public final long uid;
    public final int symbol;

    @Override
    public String toString() {
        return "[CANCEL " + orderId + " u" + uid + " s" + symbol + "]";
    }

    public ApiCancelOrder(long orderId, long uid, int symbol) {
        this.orderId = orderId;
        this.uid = uid;
        this.symbol = symbol;
    }

    public static ApiCancelOrderBuilder builder() {
        return new ApiCancelOrderBuilder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof ApiCancelOrder other
                && other.canEqual(this)
                && this.orderId == other.orderId
                && this.uid == other.uid
                && this.symbol == other.symbol;
    }

    protected boolean canEqual(Object other) {
        return other instanceof ApiCancelOrder;
    }

    @Override
    public int hashCode() {
        final int prime = 59;
        int result = 1;
        result = result * prime + Long.hashCode(this.orderId);
        result = result * prime + Long.hashCode(this.uid);
        result = result * prime + this.symbol;
        return result;
    }

    public static class ApiCancelOrderBuilder {
        private long orderId;
        private long uid;
        private int symbol;

        ApiCancelOrderBuilder() {
        }

        public ApiCancelOrderBuilder orderId(long orderId) {
            this.orderId = orderId;
            return this;
        }

        public ApiCancelOrderBuilder uid(long uid) {
            this.uid = uid;
            return this;
        }

        public ApiCancelOrderBuilder symbol(int symbol) {
            this.symbol = symbol;
            return this;
        }

        public ApiCancelOrder build() {
            return new ApiCancelOrder(this.orderId, this.uid, this.symbol);
        }

        @Override
        public String toString() {
            return "ApiCancelOrder.ApiCancelOrderBuilder(orderId=" + this.orderId
                    + ", uid=" + this.uid
                    + ", symbol=" + this.symbol + ")";
        }
    }
}
