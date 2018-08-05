package org.openpredict.exchange.core;

import com.lmax.disruptor.EventSink;
import org.openpredict.exchange.beans.L2MarketData;
import org.openpredict.exchange.beans.Order;
import org.openpredict.exchange.beans.cmd.OrderCommand;

public interface IOrderBook {


    void processCommand(OrderCommand cmd);

    // testing only - validateInternalState without changing state
    void validateInternalState();

    void clear();

    int getOrdersNum();

    Order getOrderById(long orderId);

    /**
     * Obtain current L2 Market Data snapshot
     *
     * @param size max size for each part (ask, bid)
     * @return L2 Market Data snapshot
     */
    // TODO for fixed size L2 - can use Disruptor for publishing - triggered by incoming command - don't reuse MatcherTradeEvent
    L2MarketData getL2MarketDataSnapshot(int size);

    /**
     * Obtain new instance of order book
     *
     * @param marketDataBuffer - reference to outgoing messages com.lmax.disruptor
     * @return
     */
    static IOrderBook newInstance(EventSink<L2MarketData> marketDataBuffer) {
        return new OrderBookFast(marketDataBuffer);
//        return new OrderBookSlow(outBuffer, marketDataBuffer);
    }

}
