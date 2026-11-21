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

import java.util.Objects;

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
    // Non-negative overrides are per lot. A value of -1 uses the symbol fee.
    public final long orderTakerFee;
    public final long orderMakerFee;

    @Override
    public String toString() {
        return "[ADD o" + orderId + " s" + symbol + " u" + uid + " " + (action == OrderAction.ASK ? 'A' : 'B')
                + ":" + (orderType == OrderType.IOC ? "IOC" : "GTC")
                + ":" + price + ":" + size + ":orderTakerFee:" + orderTakerFee + ":orderMakerFee:" + orderMakerFee + "]";
        //(reservePrice != 0 ? ("(R" + reservePrice + ")") : "") +
    }

    public ApiPlaceOrder(
            long price,
            long size,
            long orderId,
            OrderAction action,
            OrderType orderType,
            long uid,
            int symbol,
            int userCookie,
            long reservePrice,
            long orderTakerFee,
            long orderMakerFee) {
        this.price = price;
        this.size = size;
        this.orderId = orderId;
        this.action = action;
        this.orderType = orderType;
        this.uid = uid;
        this.symbol = symbol;
        this.userCookie = userCookie;
        this.reservePrice = reservePrice;
        this.orderTakerFee = orderTakerFee;
        this.orderMakerFee = orderMakerFee;
    }

    public static ApiPlaceOrderBuilder builder() {
        return new ApiPlaceOrderBuilder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof ApiPlaceOrder other
                && other.canEqual(this)
                && this.price == other.price
                && this.size == other.size
                && this.orderId == other.orderId
                && this.uid == other.uid
                && this.symbol == other.symbol
                && this.userCookie == other.userCookie
                && this.reservePrice == other.reservePrice
                && this.orderTakerFee == other.orderTakerFee
                && this.orderMakerFee == other.orderMakerFee
                && Objects.equals(this.action, other.action)
                && Objects.equals(this.orderType, other.orderType);
    }

    protected boolean canEqual(Object other) {
        return other instanceof ApiPlaceOrder;
    }

    @Override
    public int hashCode() {
        final int prime = 59;
        int result = 1;
        result = result * prime + Long.hashCode(this.price);
        result = result * prime + Long.hashCode(this.size);
        result = result * prime + Long.hashCode(this.orderId);
        result = result * prime + Long.hashCode(this.uid);
        result = result * prime + this.symbol;
        result = result * prime + this.userCookie;
        result = result * prime + Long.hashCode(this.reservePrice);
        result = result * prime + Long.hashCode(this.orderTakerFee);
        result = result * prime + Long.hashCode(this.orderMakerFee);
        var action = this.action;
        result = result * prime + (action == null ? 43 : action.hashCode());
        var orderType = this.orderType;
        result = result * prime + (orderType == null ? 43 : orderType.hashCode());
        return result;
    }

    public static class ApiPlaceOrderBuilder {
        private long price;
        private long size;
        private long orderId;
        private OrderAction action;
        private OrderType orderType;
        private long uid;
        private int symbol;
        private int userCookie;
        private long reservePrice;
        private long orderTakerFee = -1;
        private long orderMakerFee = -1;

        ApiPlaceOrderBuilder() {
        }

        public ApiPlaceOrderBuilder price(long price) {
            this.price = price;
            return this;
        }

        public ApiPlaceOrderBuilder size(long size) {
            this.size = size;
            return this;
        }

        public ApiPlaceOrderBuilder orderId(long orderId) {
            this.orderId = orderId;
            return this;
        }

        public ApiPlaceOrderBuilder action(OrderAction action) {
            this.action = action;
            return this;
        }

        public ApiPlaceOrderBuilder orderType(OrderType orderType) {
            this.orderType = orderType;
            return this;
        }

        public ApiPlaceOrderBuilder uid(long uid) {
            this.uid = uid;
            return this;
        }

        public ApiPlaceOrderBuilder symbol(int symbol) {
            this.symbol = symbol;
            return this;
        }

        public ApiPlaceOrderBuilder userCookie(int userCookie) {
            this.userCookie = userCookie;
            return this;
        }

        public ApiPlaceOrderBuilder reservePrice(long reservePrice) {
            this.reservePrice = reservePrice;
            return this;
        }

        public ApiPlaceOrderBuilder orderTakerFee(long orderTakerFee) {
            this.orderTakerFee = orderTakerFee;
            return this;
        }

        public ApiPlaceOrderBuilder orderMakerFee(long orderMakerFee) {
            this.orderMakerFee = orderMakerFee;
            return this;
        }

        public ApiPlaceOrder build() {
            return new ApiPlaceOrder(
                    this.price,
                    this.size,
                    this.orderId,
                    this.action,
                    this.orderType,
                    this.uid,
                    this.symbol,
                    this.userCookie,
                    this.reservePrice,
                    this.orderTakerFee,
                    this.orderMakerFee);
        }

        @Override
        public String toString() {
            return "ApiPlaceOrder.ApiPlaceOrderBuilder(price=" + this.price
                    + ", size=" + this.size
                    + ", orderId=" + this.orderId
                    + ", action=" + this.action
                    + ", orderType=" + this.orderType
                    + ", uid=" + this.uid
                    + ", symbol=" + this.symbol
                    + ", userCookie=" + this.userCookie
                    + ", reservePrice=" + this.reservePrice
                    + ", orderTakerFee=" + this.orderTakerFee
                    + ", orderMakerFee=" + this.orderMakerFee + ")";
        }
    }
}
