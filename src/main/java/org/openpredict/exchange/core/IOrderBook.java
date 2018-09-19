package org.openpredict.exchange.core;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.openpredict.exchange.beans.L2MarketData;
import org.openpredict.exchange.beans.Order;
import org.openpredict.exchange.beans.cmd.OrderCommand;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public interface IOrderBook {

    int DEFAULT_HOT_WIDTH = 1024; //32678

    void processCommand(OrderCommand cmd);

    // testing only - validateInternalState without changing state
    void validateInternalState();

    void clear();

    int getOrdersNum();

    Order getOrderById(long orderId);

    /**
     * Obtain current L2 Market Data snapshot
     *
     * @param size max size for each part (ask, bid), if negative - all records returned
     * @return L2 Market Data snapshot
     */
    L2MarketData getL2MarketDataSnapshot(int size);

    /**
     * Request to publish L2 market data into outgoing disruptor message
     *
     * @param data - pre-allocated object from ring buffer
     */
    void publishL2MarketDataSnapshot(L2MarketData data);


    List<IOrdersBucket> getAllAskBuckets();

    List<IOrdersBucket> getAllBidBuckets();

    /**
     * Obtain new instance of order book
     *
     * @return new instance
     */
    static IOrderBook newInstance() {
        return new OrderBookFast(DEFAULT_HOT_WIDTH);
        //return new OrderBookSlow();
    }

    // TODO to default?
    static int hash(IOrdersBucket[] askBuckets, IOrdersBucket[] bidBuckets) {
        int a = Arrays.hashCode(askBuckets);
        int b = Arrays.hashCode(bidBuckets);
        return Objects.hash(a, b);
    }

    // TODO to default?
    static boolean equals(IOrderBook me, Object o) {
        if (o == me) return true;
        if (o == null) return false;
        if (!(o instanceof IOrderBook)) return false;
        IOrderBook other = (IOrderBook) o;
        return new EqualsBuilder()
                // TODO compare symbol?
                .append(me.getAllAskBuckets(), other.getAllAskBuckets())
                .append(me.getAllBidBuckets(), other.getAllBidBuckets())
                .isEquals();

    }

}
