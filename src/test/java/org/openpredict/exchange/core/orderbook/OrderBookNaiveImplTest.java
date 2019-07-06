package org.openpredict.exchange.core.orderbook;

import static org.openpredict.exchange.tests.util.TestConstants.SYMBOLSPEC_ETH_XBT;

public class OrderBookNaiveImplTest extends OrderBookBaseTest {

    @Override
    protected IOrderBook createNewOrderBook() {
        return new OrderBookNaiveImpl(SYMBOLSPEC_ETH_XBT);
    }
}