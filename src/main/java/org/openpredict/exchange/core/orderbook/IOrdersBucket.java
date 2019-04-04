package org.openpredict.exchange.core.orderbook;

import org.openpredict.exchange.beans.Order;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.core.ReduceEventCallback;
import org.openpredict.exchange.core.TradeEventCallback;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
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
     * @return order if removed, or null if not found
     */
    Order remove(long orderId, long uid);

    /**
     * Match specified volume,
     * ignore orders from uid
     *
     * @param volumeToCollect - volume to collect
     * @param ignoreUid       - ignore orders from uid
     * @return - total matched volume
     */
    long match(long volumeToCollect, long ignoreUid, TradeEventCallback lambda);

    /**
     * Try to reduce size of the order.
     * <p>
     * orderId - order Id
     * newSize - new size
     *
     * @param lambda - call if reduced
     */
    boolean tryReduceSize(OrderCommand cmd, ReduceEventCallback lambda);

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

    void setPrice(long price);

    /**
     * Total size of all orders
     *
     * @return total size of all orders
     */
    long getTotalVolume();

    // testing only
    void validate();

    Order findOrder(long orderId);

    List<Order> getAllOrders();

    /**
     * Factory method to create new instance of the IOrdersBucket
     *
     * @return new instance of the IOrdersBucket
     */
    static IOrdersBucket newInstance() {
        return new OrdersBucketFast();
//        return new OrdersBucketSlow();
    }

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
