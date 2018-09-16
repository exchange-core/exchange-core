package org.openpredict.exchange.core;

import org.openpredict.exchange.beans.L2MarketData;
import org.openpredict.exchange.beans.Order;
import org.openpredict.exchange.beans.cmd.OrderCommand;

public interface IOrderBook {

    int DEFAULT_HOT_WIDTH = 1024;

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

    /**
     * Obtain new instance of order book
     *
     * @return new instance
     */
    static IOrderBook newInstance() {
        return new OrderBookFast(DEFAULT_HOT_WIDTH);
//        return new OrderBookSlow();
    }

}
