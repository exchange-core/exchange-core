package org.openpredict.exchange.core.orderbook;

import org.openpredict.exchange.beans.SymbolType;

public class OrderBookNaiveImplTest extends OrderBookBaseTest {

    @Override
    protected IOrderBook createNewOrderBook() {
        return new OrderBookNaiveImpl(SymbolType.FUTURES_CONTRACT);
    }
}