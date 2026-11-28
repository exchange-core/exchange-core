/*
 * Copyright 2019-2026 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package exchange.core2.core;

import exchange.core2.core.common.BalanceAdjustmentType;
import exchange.core2.core.common.L2MarketData;
import exchange.core2.core.common.MatcherEventType;
import exchange.core2.core.common.MatcherTradeEvent;
import exchange.core2.core.common.api.*;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import org.agrona.collections.MutableBoolean;
import org.agrona.collections.MutableLong;
import org.agrona.collections.MutableReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.ObjLongConsumer;

public class SimpleEventsProcessor implements ObjLongConsumer<OrderCommand> {

    private static final Logger log = LoggerFactory.getLogger(SimpleEventsProcessor.class);

    private final IEventsHandler eventsHandler;

    @Override
    public void accept(OrderCommand cmd, long seq) {
        try {
            sendCommandResult(cmd, seq);
            sendTradeEvents(cmd);
            sendMarketData(cmd);
        } catch (Exception ex) {
            log.error("Exception when handling command result data", ex);
        }
    }

    private void sendTradeEvents(OrderCommand cmd) {
        final MatcherTradeEvent firstEvent = cmd.matcherEvent;
        if (firstEvent == null) {
            return;
        }

        if (firstEvent.eventType == MatcherEventType.REDUCE) {

            final IEventsHandler.ReduceEvent evt = new IEventsHandler.ReduceEvent(
                    cmd.symbol,
                    firstEvent.size,
                    firstEvent.activeOrderCompleted,
                    firstEvent.price,
                    cmd.getOrderId(),
                    cmd.getUid(),
                    cmd.timestamp);

            eventsHandler.reduceEvent(evt);

            if (firstEvent.nextEvent != null) {
                throw new IllegalStateException("Only single REDUCE event is expected");
            }

            return;
        }

        sendTradeEvent(cmd);
    }

    private void sendTradeEvent(OrderCommand cmd) {

        final MutableBoolean takerOrderCompleted = new MutableBoolean(false);
        final MutableLong mutableLong = new MutableLong(0L);
        final List<IEventsHandler.Trade> trades = new ArrayList<>();

        final MutableReference<IEventsHandler.RejectEvent> rejectEvent = new MutableReference<>(null);

        cmd.processMatcherEvents(evt -> {

            if (evt.eventType == MatcherEventType.TRADE) {

                final IEventsHandler.Trade trade = new IEventsHandler.Trade(
                        evt.getMatchedOrderId(),
                        new UUID(evt.matchedOrderUidHi, evt.matchedOrderUidLo),
                        evt.matchedOrderCompleted,
                        evt.price,
                        evt.size,
                        evt.getMatchedOrderMakerFee(),
                        evt.getMatchedOrderTakerFee());

                trades.add(trade);
                mutableLong.value += evt.size;

                if (evt.activeOrderCompleted) {
                    takerOrderCompleted.value = true;
                }

            } else if (evt.eventType == MatcherEventType.REJECT) {

                rejectEvent.set(new IEventsHandler.RejectEvent(
                        cmd.symbol,
                        evt.size,
                        evt.price,
                        cmd.getOrderId(),
                        cmd.getUid(),
                        cmd.timestamp));
            }
        });

        if (!trades.isEmpty()) {

            final IEventsHandler.TradeEvent evt = new IEventsHandler.TradeEvent(
                    cmd.symbol,
                    mutableLong.value,
                    cmd.getOrderId(),
                    cmd.getUid(),
                    cmd.action,
                    takerOrderCompleted.value,
                    cmd.timestamp,
                    cmd.orderTakerFee,
                    trades);

            eventsHandler.tradeEvent(evt);
        }

        if (rejectEvent.ref != null) {
            eventsHandler.rejectEvent(rejectEvent.ref);
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
        final ApiCommand apiCommand = switch (cmd.command) {
            case PLACE_ORDER -> new ApiPlaceOrder(
                    cmd.price, cmd.size, cmd.getOrderId(), cmd.action, cmd.orderType,
                    cmd.getUid(), cmd.symbol, cmd.userCookie, cmd.reserveBidPrice, cmd.orderTakerFee, cmd.orderMakerFee);
            case MOVE_ORDER -> new ApiMoveOrder(cmd.getOrderId(), cmd.price, cmd.getUid(), cmd.symbol);
            case CANCEL_ORDER -> new ApiCancelOrder(cmd.getOrderId(), cmd.getUid(), cmd.symbol);
            case REDUCE_ORDER -> new ApiReduceOrder(cmd.getOrderId(), cmd.getUid(), cmd.symbol, cmd.size);
            case ADD_USER -> new ApiAddUser(cmd.getUid());
            case BALANCE_ADJUSTMENT -> new ApiAdjustUserBalance(
                    cmd.getUid(), cmd.symbol, cmd.price, cmd.orderIdLo, BalanceAdjustmentType.of(cmd.orderType.getCode()));
            case BINARY_DATA_COMMAND -> cmd.resultCode == CommandResultCode.ACCEPTED
                    ? null : new ApiBinaryDataCommand(cmd.userCookie, null);
            case ORDER_BOOK_REQUEST -> new ApiOrderBookRequest(cmd.symbol, (int) cmd.size);
            // TODO add rest of commands
            default -> null;
        };
        if (apiCommand != null) {
            sendApiCommandResult(apiCommand, cmd.resultCode, cmd.timestamp, seq);
        }
    }

    private void sendApiCommandResult(ApiCommand cmd, CommandResultCode resultCode, long timestamp, long seq) {
        cmd.timestamp = timestamp;
        final IEventsHandler.ApiCommandResult commandResult = new IEventsHandler.ApiCommandResult(cmd, resultCode, seq);
        eventsHandler.commandResult(commandResult);
    }

    public SimpleEventsProcessor(IEventsHandler eventsHandler) {
        this.eventsHandler = eventsHandler;
    }

    public IEventsHandler getEventsHandler() {
        return this.eventsHandler;
    }
}
