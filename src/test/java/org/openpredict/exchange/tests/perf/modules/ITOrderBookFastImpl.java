package org.openpredict.exchange.tests.perf.modules;

import org.openpredict.exchange.core.orderbook.IOrderBook;
import org.openpredict.exchange.core.orderbook.OrderBookFastImpl;

import static org.openpredict.exchange.tests.util.TestConstants.SYMBOLSPEC_EUR_USD;

public class ITOrderBookFastImpl extends ITOrderBookBase {

    @Override
    protected IOrderBook createNewOrderBook() {
        return new OrderBookFastImpl(OrderBookFastImpl.DEFAULT_HOT_WIDTH, SYMBOLSPEC_EUR_USD);
    }
}
