package org.openpredict.exchange.core;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.beans.cmd.OrderCommandType;
import org.openpredict.exchange.core.orderbook.IOrderBook;
import org.openpredict.exchange.core.orderbook.OrderBookFastImpl;

@Slf4j
public final class MatchingEngineRouter {

    // symbol->OB
    private final IntObjectHashMap<IOrderBook> orderBooks = new IntObjectHashMap<>();

    public void processOrder(OrderCommand cmd) {
        if (cmd.resultCode != CommandResultCode.VALID_FOR_MATCHING_ENGINE) {
            cmd.matcherEvent = null; // remove and let garbage collected
            return;
        }

        if (cmd.command == OrderCommandType.RESET) {
            orderBooks.clear();
            cmd.resultCode = CommandResultCode.SUCCESS;
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
        IOrderBook orderBook = new OrderBookFastImpl(OrderBookFastImpl.DEFAULT_HOT_WIDTH);
        orderBooks.put(symbol, orderBook);
    }


}
