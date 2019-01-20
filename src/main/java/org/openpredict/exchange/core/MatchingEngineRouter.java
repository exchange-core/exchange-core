package org.openpredict.exchange.core;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.openpredict.exchange.beans.L2MarketData;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public final class MatchingEngineRouter {

    // symbol->OB
    private final IntObjectHashMap<IOrderBook> orderBooks = new IntObjectHashMap<>();

    public void processOrder(OrderCommand cmd) {
        if (cmd.resultCode != CommandResultCode.VALID_FOR_MATCHING_ENGINE) {
            cmd.matcherEvent = null; // remove and let garbage collected
            return;
        }

        final IOrderBook orderBook = orderBooks.get(cmd.symbol);
        if (orderBook == null) {
            cmd.matcherEvent = null; // remove and let garbage collected
            cmd.resultCode = CommandResultCode.MATCHING_INVALID_ORDER_ID;
            return;
        }

        orderBook.processCommand(cmd);
    }

    public void addOrderBook(int symbol) {
        IOrderBook orderBook = IOrderBook.newInstance();
        orderBooks.put(symbol, orderBook);
    }

    public IOrderBook getOrderBook(int symbol) {
        return orderBooks.get(symbol);
    }

    public L2MarketData getMarketData(int symbol, int size) {
        return orderBooks.get(symbol).getL2MarketDataSnapshot(size);
    }

    public void reset() {
        orderBooks.forEach(IOrderBook::clear);
    }

}
