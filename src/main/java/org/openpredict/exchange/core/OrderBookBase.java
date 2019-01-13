package org.openpredict.exchange.core;

import lombok.extern.slf4j.Slf4j;
import org.openpredict.exchange.beans.L2MarketData;
import org.openpredict.exchange.beans.MatcherEventType;
import org.openpredict.exchange.beans.MatcherTradeEvent;
import org.openpredict.exchange.beans.Order;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;

import java.util.ArrayDeque;

/**
 * TODO get rid of this class in favor of new interface
 */
@Slf4j
public abstract class OrderBookBase implements IOrderBook {

    private final ArrayDeque<MatcherTradeEvent> eventsPool = new ArrayDeque<>(1024);

    private OrderCommand currentCmd;

    @Override
    public void processCommand(OrderCommand cmd) {
        currentCmd = cmd;

        revokeEvents();

        if (cmd.resultCode != CommandResultCode.VALID_FOR_MATCHING_ENGINE) {
            return;
        }

        // TODO check symbol

        switch (cmd.command) {
            case MOVE_ORDER:
//                log.debug("Move {}", cmd.orderId);
                boolean isUpdated = updateOrder(cmd);
                cmd.resultCode = isUpdated ? CommandResultCode.SUCCESS : CommandResultCode.MATCHING_INVALID_ORDER_ID;
//                log.debug("Move {} = {}", cmd.orderId, isUpdated);
                break;

            case CANCEL_ORDER:
//                log.debug("Cancel {}", cmd.orderId);
                boolean isCancelled = cancelOrder(cmd);
                cmd.resultCode = isCancelled ? CommandResultCode.SUCCESS : CommandResultCode.MATCHING_INVALID_ORDER_ID;
//                log.debug("Cancel {} = {}", cmd.orderId, isCancelled);
                break;

            case PLACE_ORDER:
//                log.debug("Place {}", cmd.orderId);
                boolean isPlaced = addNewOrder(cmd);
                cmd.resultCode = isPlaced ? CommandResultCode.SUCCESS : CommandResultCode.MATCHING_INVALID_ORDER_ID;
//                log.debug("Place {} = {}", cmd.orderId, isPlaced);
                break;

            case ORDER_BOOK_REQUEST:
                //log.debug("ORDER_BOOK_REQUEST {}", cmd.size);
                cmd.marketData = getL2MarketDataSnapshot((int) cmd.size);
                cmd.resultCode = CommandResultCode.SUCCESS;
                break;
        }

    }

    /**
     * Add new order
     *
     * @param cmd - order command
     * @return - always true (in case of rejection - special event issued)
     */
    private boolean addNewOrder(OrderCommand cmd) {
        switch (cmd.orderType) {
            case LIMIT:
                placeNewLimitOrder(cmd);
                return true;
            case MARKET:
                matchMarketOrder(cmd);
                return true;
            default:
                return false;
        }
    }

    /**
     * Process new MARKET order
     * Such order matched to any existing LIMIT orders
     * Of there is not enough volume in order book - reject as partially filled
     *
     * @param order - market order to match
     */
    abstract protected void matchMarketOrder(OrderCommand order);

    /**
     * Place new LIMIT order
     * If order is marketable (there are matching limit orders) - match it first with existing liquidity
     *
     * @param cmd - limit order to place
     */
    abstract protected void placeNewLimitOrder(OrderCommand cmd);

    /**
     * Cancel order
     * <p>
     * orderId - order Id
     *
     * @return false if order was not found, otherwise always true
     */
    abstract protected boolean cancelOrder(OrderCommand cmd);

    /**
     * Reduce volume or/and move an order
     * <p>
     * orderId  - order Id
     * newPrice - new price (if 0 or same - order will not moved)
     * newSize  - new size (if higher than current size or 0 - order will not downsized)
     *
     * @return false if order was not found, otherwise always true
     */
    abstract protected boolean updateOrder(OrderCommand cmd);

    /**
     * @param size max size for each part (ask, bid)
     * @return
     */
    @Override
    public L2MarketData getL2MarketDataSnapshot(int size) {
        int asksSize = getTotalAskBuckets();
        int bidsSize = getTotalBidBuckets();
        if (size >= 0) {
            // limit size
            asksSize = Math.min(asksSize, size);
            bidsSize = Math.min(bidsSize, size);
        }
        L2MarketData data = new L2MarketData(asksSize, bidsSize);
        fillAsks(asksSize, data);
        fillBids(bidsSize, data);
        return data;
    }

    @Override
    public void publishL2MarketDataSnapshot(L2MarketData data) {
        int size = L2MarketData.L2_SIZE;
        fillAsks(size, data);
        fillBids(size, data);
    }

    abstract protected void fillAsks(final int size, L2MarketData data);

    abstract protected void fillBids(final int size, L2MarketData data);

    abstract protected int getTotalAskBuckets();

    abstract protected int getTotalBidBuckets();


    private void revokeEvents() {

        currentCmd.marketData = null;

        MatcherTradeEvent matcherEvent = currentCmd.matcherEvent;
        currentCmd.matcherEvent = null;
        //log.debug("  {}", cmd);
        while (matcherEvent != null) {
            eventsPool.addLast(matcherEvent);
            MatcherTradeEvent tmp = matcherEvent;
            matcherEvent = matcherEvent.nextEvent;
            tmp.nextEvent = null;
//            log.debug("  eventsPool: {}", eventsPool.size());
        }
    }

    private MatcherTradeEvent newMatcherEvent() {
        MatcherTradeEvent event = eventsPool.pollLast();
        return (event == null) ? new MatcherTradeEvent() : event;
    }


    protected void sendTradeEvent(OrderCommand activeOrder, Order matchingOrder, boolean fm, boolean fma, long price, long v) {

//        log.debug("** sendTradeEvent: active id:{} matched id:{}", activeOrder.orderId, matchingOrder.orderId);
//        log.debug("** sendTradeEvent: price:{} v:{}", price, v);

        final MatcherTradeEvent event = newMatcherEvent();

        event.eventType = MatcherEventType.TRADE;

        event.activeOrderId = activeOrder.orderId;
        event.activeOrderUid = activeOrder.uid;
        event.activeOrderCompleted = fma;
        event.activeOrderAction = activeOrder.action;
//        event.activeOrderSeq = activeOrder.seq;

        event.matchedOrderId = matchingOrder.orderId;
        event.matchedOrderUid = matchingOrder.uid;
        event.matchedOrderCompleted = fm;

        event.price = price;
        event.size = v;
        event.timestamp = activeOrder.timestamp;
        event.symbol = activeOrder.symbol;

        event.nextEvent = currentCmd.matcherEvent;
        currentCmd.matcherEvent = event;

//        log.debug(" currentCmd.matcherEvent={}", currentCmd.matcherEvent);
    }


    protected void sendReduceEvent(Order order, long reducedBy) {
//        log.debug("Reduce ");
        final MatcherTradeEvent event = newMatcherEvent();
        event.eventType = MatcherEventType.REDUCE;
        event.activeOrderId = order.orderId;
        event.activeOrderUid = order.uid;
        event.activeOrderCompleted = false;
        event.activeOrderAction = order.action;
//        event.activeOrderSeq = order.seq;
        event.matchedOrderId = 0;
        event.matchedOrderCompleted = false;
        event.price = 0;
        event.size = reducedBy;
        event.timestamp = order.timestamp; // TODO should be current timestamp
        event.symbol = order.symbol;

        event.nextEvent = currentCmd.matcherEvent;
        currentCmd.matcherEvent = event;
    }


    protected void sendRejectEvent(OrderCommand order, long filledSize) {

//        log.debug("Rejected {}", order.orderId);
//        log.debug("\n{}", getL2MarketDataSnapshot(10).dumpOrderBook());

        final MatcherTradeEvent event = newMatcherEvent();

        event.eventType = MatcherEventType.REJECTION;

        event.activeOrderId = order.orderId;
        event.activeOrderUid = order.uid;
        event.activeOrderCompleted = false;
        event.activeOrderAction = order.action;
//        event.activeOrderSeq = order.seq;

        event.matchedOrderId = 0;
        event.matchedOrderCompleted = false;

        event.price = 0;
        event.size = order.size - filledSize;
        event.timestamp = order.timestamp;
        event.symbol = order.symbol;

        event.nextEvent = currentCmd.matcherEvent;
        currentCmd.matcherEvent = event;
    }


}
