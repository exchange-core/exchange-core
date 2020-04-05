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
package exchange.core2.core.orderbook;

import exchange.core2.core.common.IOrder;
import exchange.core2.core.common.Order;
import exchange.core2.core.common.cmd.OrderCommand;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public interface IOrdersBucket extends Comparable<IOrdersBucket>, WriteBytesMarshallable {

    /**
     * Put a new order into bucket
     *
     * @param order - order
     */
    void put(Order order);

    /**
     * Remove order from the bucket
     *
     * @param orderId - order id
     * @param uid     - order uid
     * @return order if removed, or null if not found
     */
    Order remove(long orderId, long uid);

    /**
     * Match specified volume,
     * ignore orders from uid
     *
     * @param volumeToCollect     - volume to collect
     * @param activeOrder         -  (ignore orders same uid)
     * @param triggerCmd          - triggered command (taker data)
     * @param removeOrderCallback - called to remove order
     * @param helper              - events helper
     * @return - total matched volume
     */
    long match(long volumeToCollect, IOrder activeOrder, OrderCommand triggerCmd, Consumer<Order> removeOrderCallback, OrderBookEventsHelper helper);

    /**
     * Get number of orders in the bucket
     *
     * @return number of orders in the bucket
     */
    int getNumOrders();

    /**
     * Get bucket price
     *
     * @return bucket price
     */
    long getPrice();

    /**
     * Set new price if bucket is reused
     * TODO verify bucket is empty
     *
     * @param price - new price
     */
    void setPrice(long price);

    /**
     * Total size of all orders
     *
     * @return total size of all orders
     */
    long getTotalVolume();


    Order findOrder(long orderId);

    /**
     * Inefficient method - for testing only
     *
     * @return new array with references to orders, preserving execution queue order
     */
    List<Order> getAllOrders();


    /**
     * execute some action for each order (preserving execution queue order)
     *
     * @param consumer action consumer function
     */
    void forEachOrder(Consumer<Order> consumer);


    /**
     * @return actual implementation
     */
    OrderBucketImplType getImplementationType();

    static IOrdersBucket create(OrderBucketImplType type) {
        switch (type) {
            case NAIVE:
                return new OrdersBucketNaiveImpl();
            case FAST:
                return new OrdersBucketFastImpl();
            default:
                throw new IllegalArgumentException();
        }
    }

    static IOrdersBucket create(BytesIn bytes) {
        switch (OrderBucketImplType.of(bytes.readByte())) {
            case NAIVE:
                return new OrdersBucketNaiveImpl(bytes);
            case FAST:
                return new OrdersBucketFastImpl(bytes);
            default:
                throw new IllegalArgumentException();
        }
    }

    @Getter
    enum OrderBucketImplType {
        NAIVE(0),
        FAST(1);

        private byte code;

        OrderBucketImplType(int code) {
            this.code = (byte) code;
        }

        public static OrderBucketImplType of(byte code) {
            switch (code) {
                case 0:
                    return NAIVE;
                case 1:
                    return FAST;
                default:
                    throw new IllegalArgumentException("unknown OrderBucketImplType:" + code);
            }
        }
    }

    // testing only
    void validate();

    // TODO to default?
    static int hash(long price, Order[] orders) {
        return Objects.hash(price, Arrays.hashCode(orders));
    }

    default int compareTo(IOrdersBucket other) {
        return Long.compare(this.getPrice(), other.getPrice());
    }

    default String dumpToSingleLine() {
        String orders = getAllOrders().stream()
                .map(o -> String.format("id%d_L%d_F%d", o.orderId, o.size, o.filled))
                .collect(Collectors.joining(", "));

        return String.format("%d : vol:%d num:%d : %s", getPrice(), getTotalVolume(), getNumOrders(), orders);
    }

    // TODO default equals
}
