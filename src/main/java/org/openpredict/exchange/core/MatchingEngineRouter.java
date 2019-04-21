package org.openpredict.exchange.core;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.openpredict.exchange.beans.CoreSymbolSpecification;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.beans.cmd.OrderCommandType;
import org.openpredict.exchange.core.orderbook.IOrderBook;
import org.openpredict.exchange.core.orderbook.OrderBookFastImpl;

import static org.openpredict.exchange.beans.cmd.OrderCommandType.BINARY_DATA;

@Slf4j
public final class MatchingEngineRouter {

    // state
    private final BinaryCommandsProcessor binaryCommandsProcessor = new BinaryCommandsProcessor(this::addSymbol, CommandResultCode.ACCEPTED);

    // symbol->OB
    private final IntObjectHashMap<IOrderBook> orderBooks = new IntObjectHashMap<>();

    public void processOrder(OrderCommand cmd) {
        if (cmd.resultCode != CommandResultCode.VALID_FOR_MATCHING_ENGINE) {
            cmd.matcherEvent = null; // remove and let garbage collected

        } else if (cmd.command == OrderCommandType.RESET) {
            orderBooks.clear();
            binaryCommandsProcessor.reset();
            cmd.resultCode = CommandResultCode.SUCCESS;

        } else if (cmd.command == BINARY_DATA) {
            cmd.resultCode = binaryCommandsProcessor.binaryData(cmd);

        } else {
            processCommand(cmd);
        }
    }

    private CommandResultCode addSymbol(final CoreSymbolSpecification symbolSpecification) {

        //log.debug("symbolSpecification: {}", symbolSpecification);

        final int symbolId = symbolSpecification.symbolId;
        if (orderBooks.get(symbolId) != null) {
            return CommandResultCode.MATCHING_ORDER_BOOK_ALREADY_EXISTS;
        } else {
            IOrderBook orderBook = new OrderBookFastImpl(OrderBookFastImpl.DEFAULT_HOT_WIDTH);
            orderBooks.put(symbolId, orderBook);
            return CommandResultCode.SUCCESS;
        }
    }

    private void processCommand(final OrderCommand cmd) {

        final IOrderBook orderBook = orderBooks.get(cmd.symbol);
        if (orderBook == null) {
            cmd.matcherEvent = null; // remove and let garbage collected
            cmd.resultCode = CommandResultCode.MATCHING_INVALID_ORDER_BOOK_ID;
            return;
        }


        // TODO revoke
        cmd.marketData = null;
        cmd.matcherEvent = null;

        if (cmd.resultCode != CommandResultCode.VALID_FOR_MATCHING_ENGINE) {
            return;
        }

        // TODO check symbol
        IOrderBook.processCommand(orderBook, cmd);
    }

}
