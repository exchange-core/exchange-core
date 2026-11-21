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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.Objects;

/**
 * Extending OrderCommand allows to avoid creating new objects
 * for instantly matching orders (MARKET or marketable LIMIT orders)
 * as well as use same code for matching moved orders
 * <p>
 * No external references allowed to such object - order objects only live inside OrderBook.
 */
@NoArgsConstructor
@AllArgsConstructor
@Builder
public final class Order implements WriteBytesMarshallable, IOrder {

    @Getter
    public long orderId;

    @Getter
    public long price;

    @Getter
    public long size;

    @Getter
    public long filled;

    // new orders - reserved price for fast moves of GTC bid orders in exchange mode
    @Getter
    public long reserveBidPrice;

    // required for PLACE_ORDER only;
    @Getter
    public OrderAction action;

    @Getter
    public long uid;

    @Getter
    public long orderTakerFee = -1;

    @Getter
    public long orderMakerFee = -1;

    @Getter
    public long timestamp;

//    public int userCookie;

    // The existing action is 0 or 1. Default-fee orders retain the original format.
    public static final int FEE_OVERRIDES_FLAG = 2;

    public Order(long orderId, long price, long size, long filled, long reserveBidPrice,
                 OrderAction action, long uid, long timestamp) {
        this(orderId, price, size, filled, reserveBidPrice, action, uid, -1, -1, timestamp);
    }

    public Order(BytesIn bytes) {


        this.orderId = bytes.readLong(); // orderId
        this.price = bytes.readLong();  // price
        this.size = bytes.readLong(); // size
        this.filled = bytes.readLong(); // filled
        this.reserveBidPrice = bytes.readLong(); // price2
        final byte actionAndFlags = bytes.readByte();
        this.action = OrderAction.of((byte) (actionAndFlags & ~FEE_OVERRIDES_FLAG));
        this.uid = bytes.readLong(); // uid
        final boolean hasFees = (actionAndFlags & FEE_OVERRIDES_FLAG) != 0;
        this.orderTakerFee = hasFees ? bytes.readLong() : -1;
        this.orderMakerFee = hasFees ? bytes.readLong() : -1;
        this.timestamp = bytes.readLong(); // timestamp
//        this.userCookie = bytes.readInt();  // userCookie

    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        writeOrder(this, bytes);
    }

    public static void writeOrder(IOrder order, BytesOut bytes) {
        final boolean hasFees = order.getOrderTakerFee() != -1 || order.getOrderMakerFee() != -1;
        bytes.writeLong(order.getOrderId());
        bytes.writeLong(order.getPrice());
        bytes.writeLong(order.getSize());
        bytes.writeLong(order.getFilled());
        bytes.writeLong(order.getReserveBidPrice());
        bytes.writeByte((byte) (order.getAction().getCode() | (hasFees ? FEE_OVERRIDES_FLAG : 0)));
        bytes.writeLong(order.getUid());
        if (hasFees) {
            bytes.writeLong(order.getOrderTakerFee());
            bytes.writeLong(order.getOrderMakerFee());
        }
        bytes.writeLong(order.getTimestamp());
    }

    @Override
    public String toString() {
        return "[" + orderId + " " + (action == OrderAction.ASK ? 'A' : 'B')
                + price + ":" + size + "F" + filled
                // + " C" + userCookie
                + " U" + uid + "]";
    }

    @Override
    public int hashCode() {
        return hashOrder(this);
    }


    public static int hashOrder(IOrder order) {
        final int base = Objects.hash(order.getOrderId(), order.getAction(), order.getPrice(),
                order.getSize(), order.getReserveBidPrice(), order.getFilled(), order.getUid());
        return order.getOrderTakerFee() == -1 && order.getOrderMakerFee() == -1
                ? base
                : Objects.hash(base, order.getOrderTakerFee(), order.getOrderMakerFee());
    }

    /**
     * timestamp is not included into hashCode() and equals() for repeatable results
     */
    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null) return false;
        if (!(o instanceof Order)) return false;

        Order other = (Order) o;

        // ignore timestamp and userCookie
        return orderId == other.orderId
                && action == other.action
                && price == other.price
                && size == other.size
                && reserveBidPrice == other.reserveBidPrice
                && filled == other.filled
                && uid == other.uid
                && orderTakerFee == other.orderTakerFee
                && orderMakerFee == other.orderMakerFee;
    }

    @Override
    public int stateHash() {
        return hashCode();
    }
}
