package org.openpredict.exchange.tests.perf.modules;

import org.openpredict.exchange.core.orderbook.IOrderBook;
import org.openpredict.exchange.core.orderbook.OrderBookNaiveImpl;

import static org.openpredict.exchange.tests.util.TestConstants.SYMBOLSPEC_EUR_USD;

public class ITOrderBookNaiveImpl extends ITOrderBookBase {

    @Override
    protected IOrderBook createNewOrderBook() {
        return new OrderBookNaiveImpl(SYMBOLSPEC_EUR_USD);
    }
}
