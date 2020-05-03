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
import exchange.core2.core.common.MatcherTradeEvent;
import exchange.core2.core.common.Order;
import lombok.AllArgsConstructor;
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
     * @param volumeToCollect - volume to collect
     * @param activeOrder     - for getReserveBidPrice
     * @param helper          - events helper
     * @return - total matched volume, events, completed orders to remove
     */
    MatcherResult match(long volumeToCollect, IOrder activeOrder, OrderBookEventsHelper helper);

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
     * Reduce size of the order
     *
     * @param reduceSize - size to reduce (difference)
     */
    void reduceSize(long reduceSize);

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

    @AllArgsConstructor
    final class MatcherResult {
        public MatcherTradeEvent eventsChainHead;
        public MatcherTradeEvent eventsChainTail;
        public long volume;
        public List<Long> ordersToRemove;
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
