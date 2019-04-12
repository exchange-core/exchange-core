package org.openpredict.exchange.core.orderbook;

import org.openpredict.exchange.beans.Order;
import org.openpredict.exchange.beans.cmd.OrderCommand;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public interface IOrdersBucket extends Comparable<IOrdersBucket> {

    /**
     * Add order into bucket
     *
     * @param order - order
     */
    void add(Order order);

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
     * @param activeOrder         (ignore orders same uid)
     * @param triggerCmd
     * @param removeOrderCallback
     * @return - total matched volume
     */
    long match(long volumeToCollect, OrderCommand activeOrder, OrderCommand triggerCmd, Consumer<Order> removeOrderCallback);

    /**
     * Try to reduce size of the order.
     * <p>
     * orderId - order Id
     * newSize - new size
     *
     * @param cmd - update order command
     */
    boolean tryReduceSize(OrderCommand cmd);

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

    List<Order> getAllOrders();

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
