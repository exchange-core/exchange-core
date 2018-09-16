package org.openpredict.exchange.core;

import org.openpredict.exchange.beans.Order;
import org.openpredict.exchange.beans.cmd.OrderCommand;

import java.util.List;

public interface IOrdersBucket {

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
     *
     * orderId - order Id
     * newSize - new size
     * @param lambda  - call if reduced
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
}
