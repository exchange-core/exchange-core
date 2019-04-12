package org.openpredict.exchange.tests.performance;

import org.openpredict.exchange.core.orderbook.IOrderBook;
import org.openpredict.exchange.core.orderbook.OrderBookFastImpl;

public class ITOrderBookFastImpl extends ITOrderBookBase {

    @Override
    protected IOrderBook createNewOrderBook() {
        return new OrderBookFastImpl(OrderBookFastImpl.DEFAULT_HOT_WIDTH);
    }
}
