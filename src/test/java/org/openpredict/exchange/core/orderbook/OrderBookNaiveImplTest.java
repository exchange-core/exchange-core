package org.openpredict.exchange.core.orderbook;

public class OrderBookNaiveImplTest extends OrderBookBaseTest {

    @Override
    protected IOrderBook createNewOrderBook() {
        return new OrderBookNaiveImpl();
    }
}