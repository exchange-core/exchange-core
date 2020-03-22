package exchange.core2.core;

import exchange.core2.core.common.L2MarketData;
import exchange.core2.core.common.MatcherEventType;
import exchange.core2.core.common.MatcherTradeEvent;
import exchange.core2.core.common.api.*;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.agrona.collections.MutableBoolean;
import org.agrona.collections.MutableLong;

import java.util.ArrayList;
import java.util.List;
import java.util.function.ObjLongConsumer;

@RequiredArgsConstructor
@Getter
@Slf4j
public class SimpleEventsProcessor implements ObjLongConsumer<OrderCommand> {

    private final IEventsHandler eventsHandler;

    @Override
    public void accept(OrderCommand cmd, long seq) {
        sendCommandResult(cmd, seq);
        sendTradeEvents(cmd);
        sendMarketData(cmd);
    }

    private void sendTradeEvents(OrderCommand cmd) {
        final MatcherTradeEvent firstEvent = cmd.matcherEvent;
        if (firstEvent == null) {
            return;
        }

        if (firstEvent.eventType == MatcherEventType.CANCEL) {

            final IEventsHandler.CancelEvent evt = new IEventsHandler.CancelEvent(
                    cmd.symbol,
                    firstEvent.size,
                    firstEvent.price,
                    cmd.orderId,
                    cmd.uid,
                    cmd.timestamp);

            eventsHandler.cancelEvent(evt);

            if (firstEvent.nextEvent != null) {
                throw new IllegalStateException("Only single CANCEL event is expected");
            }

        } else if (firstEvent.eventType == MatcherEventType.REJECTION) {

            final IEventsHandler.RejectEvent evt = new IEventsHandler.RejectEvent(
                    cmd.symbol,
                    firstEvent.size,
                    firstEvent.price,
                    cmd.orderId,
                    cmd.uid,
                    cmd.timestamp);

            eventsHandler.rejectEvent(evt);

            if (firstEvent.nextEvent != null) {
                throw new IllegalStateException("Only single REJECTION event is expected");
            }

        } else if (firstEvent.eventType == MatcherEventType.TRADE) {

            final MutableBoolean takerOrderCompleted = new MutableBoolean(false);
            final MutableLong mutableLong = new MutableLong(0L);
            final List<IEventsHandler.Deal> deals = new ArrayList<>();
            cmd.processMatcherEvents(evt -> {
                final IEventsHandler.Deal deal = new IEventsHandler.Deal(
                        evt.matchedOrderId,
                        evt.matchedOrderUid,
                        evt.matchedOrderCompleted,
                        evt.price,
                        evt.size);

                deals.add(deal);
                mutableLong.value += evt.size;

                if (evt.activeOrderCompleted) {
                    takerOrderCompleted.value = true;
                }
                if (evt.eventType != MatcherEventType.TRADE) {
                    throw new IllegalStateException("Only TRADE events are expected");
                }
            });

            final IEventsHandler.TradeEvent evt = new IEventsHandler.TradeEvent(
                    cmd.symbol,
                    mutableLong.value,
                    cmd.orderId,
                    cmd.uid,
                    cmd.action,
                    takerOrderCompleted.value,
                    cmd.timestamp,
                    deals);

            eventsHandler.tradeEvent(evt);
        }
    }

    private void sendMarketData(OrderCommand cmd) {
        final L2MarketData marketData = cmd.marketData;
        if (marketData != null) {
            final List<IEventsHandler.OrderBookRecord> asks = new ArrayList<>(marketData.askSize);
            for (int i = 0; i < marketData.askSize; i++) {
                asks.add(new IEventsHandler.OrderBookRecord(marketData.askPrices[i], marketData.askVolumes[i], (int) marketData.askOrders[i]));
            }

            final List<IEventsHandler.OrderBookRecord> bids = new ArrayList<>(marketData.bidSize);
            for (int i = 0; i < marketData.bidSize; i++) {
                bids.add(new IEventsHandler.OrderBookRecord(marketData.bidPrices[i], marketData.bidVolumes[i], (int) marketData.bidOrders[i]));
            }

            eventsHandler.orderBook(new IEventsHandler.OrderBook(cmd.symbol, asks, bids, cmd.timestamp));
        }
    }


    private void sendCommandResult(OrderCommand cmd, long seq) {

        switch (cmd.command) {
            case PLACE_ORDER:
                sendApiCommandResult(new ApiPlaceOrder(
                                cmd.price,
                                cmd.size,
                                cmd.orderId,
                                cmd.action,
                                cmd.orderType,
                                cmd.uid,
                                cmd.symbol,
                                cmd.userCookie,
                                cmd.reserveBidPrice),
                        cmd.resultCode,
                        cmd.timestamp,
                        seq);
                break;

            case MOVE_ORDER:
                sendApiCommandResult(new ApiMoveOrder(cmd.orderId, cmd.price, cmd.uid, cmd.symbol), cmd.resultCode, cmd.timestamp, seq);
                break;

            case CANCEL_ORDER:
                sendApiCommandResult(new ApiCancelOrder(cmd.orderId, cmd.uid, cmd.symbol), cmd.resultCode, cmd.timestamp, seq);
                break;

            case ADD_USER:
                sendApiCommandResult(new ApiAddUser(cmd.uid), cmd.resultCode, cmd.timestamp, seq);
                break;

            case BALANCE_ADJUSTMENT:
                sendApiCommandResult(new ApiAdjustUserBalance(cmd.uid, cmd.symbol, cmd.price, cmd.orderId), cmd.resultCode, cmd.timestamp, seq);
                break;

            case BINARY_DATA_COMMAND:
                if (cmd.resultCode != CommandResultCode.ACCEPTED) {
                    sendApiCommandResult(new ApiBinaryDataCommand(cmd.userCookie, null), cmd.resultCode, cmd.timestamp, seq);
                }
                break;

            case ORDER_BOOK_REQUEST:
                sendApiCommandResult(new ApiOrderBookRequest(cmd.symbol, (int) cmd.size), cmd.resultCode, cmd.timestamp, seq);
                break;

            // TODO add rest of commands

        }

    }

    private void sendApiCommandResult(ApiCommand cmd, CommandResultCode resultCode, long timestamp, long seq) {
        cmd.timestamp = timestamp;
        eventsHandler.commandResult(new IEventsHandler.ApiCommandResult(cmd, resultCode, seq));
    }
}
