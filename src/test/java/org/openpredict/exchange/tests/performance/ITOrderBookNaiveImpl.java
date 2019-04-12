package org.openpredict.exchange.tests.performance;

import org.openpredict.exchange.core.orderbook.IOrderBook;
import org.openpredict.exchange.core.orderbook.OrderBookNaiveImpl;

public class ITOrderBookNaiveImpl extends ITOrderBookBase {

    @Override
    protected IOrderBook createNewOrderBook() {
        return new OrderBookNaiveImpl();
    }
}
