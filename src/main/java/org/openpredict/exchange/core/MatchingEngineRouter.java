package org.openpredict.exchange.core;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.openpredict.exchange.beans.CoreSymbolSpecification;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.core.orderbook.IOrderBook;
import org.openpredict.exchange.core.orderbook.OrderBookFastImpl;

import static org.openpredict.exchange.beans.cmd.OrderCommandType.ORDER_BOOK_REQUEST;

@Slf4j
public final class MatchingEngineRouter {

    // state
    private final BinaryCommandsProcessor binaryCommandsProcessor = new BinaryCommandsProcessor(this::addSymbol, CommandResultCode.ACCEPTED);

    // symbol->OB
    private final IntObjectHashMap<IOrderBook> orderBooks = new IntObjectHashMap<>();

    public void processOrder(OrderCommand cmd) {

        // TODO check symbolId

        // TODO revoke with only symbol group specific processor
        cmd.matcherEvent = null;
        cmd.marketData = null;

        switch (cmd.command) {
            case MOVE_ORDER:
            case CANCEL_ORDER:
            case ORDER_BOOK_REQUEST:
            case PLACE_ORDER:
                // TODO process specific symbol group only
                processCommand(cmd);
                break;

            case BINARY_DATA:
                // TODO process all symbols groups, only processor 0 writes result
                cmd.resultCode = binaryCommandsProcessor.binaryData(cmd);
                break;

            case RESET:
                // TODO process all symbols groups, only processor 0 writes result
                orderBooks.clear();
                binaryCommandsProcessor.reset();
                cmd.resultCode = CommandResultCode.SUCCESS;
                break;
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
            cmd.resultCode = CommandResultCode.MATCHING_INVALID_ORDER_BOOK_ID;
        }else {
            cmd.resultCode = IOrderBook.processCommand(orderBook, cmd);

            // posting market data for risk processor makes sense only if command execution is successful, otherwise it will be ignored (possible garbage from previous cycle)
            if ((cmd.serviceFlags & 1) != 0 && cmd.command != ORDER_BOOK_REQUEST && cmd.resultCode == CommandResultCode.SUCCESS) {
                cmd.marketData = orderBook.getL2MarketDataSnapshot(8);
            }
        }
    }

}
