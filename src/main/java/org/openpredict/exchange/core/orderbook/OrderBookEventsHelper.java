package org.openpredict.exchange.core.orderbook;

import lombok.extern.slf4j.Slf4j;
import org.openpredict.exchange.beans.MatcherEventType;
import org.openpredict.exchange.beans.MatcherTradeEvent;
import org.openpredict.exchange.beans.Order;
import org.openpredict.exchange.beans.cmd.OrderCommand;

import java.util.concurrent.BlockingQueue;

@Slf4j
public final class OrderBookEventsHelper {

    private final BlockingQueue<MatcherTradeEvent[]> matcherTradeEventsPool;
    private MatcherTradeEvent[] eventsBucket = null;
    private int eventsBucketCounter = 0;

    public OrderBookEventsHelper(BlockingQueue<MatcherTradeEvent[]> matcherTradeEventsPool) {
        this.matcherTradeEventsPool = matcherTradeEventsPool;
    }

    public OrderBookEventsHelper() {
        this.matcherTradeEventsPool = null;
    }


    public void sendTradeEvent(OrderCommand cmd, OrderCommand activeOrder, Order matchingOrder, boolean fm, boolean fma, long price, long v) {

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

        event.nextEvent = cmd.matcherEvent;
        cmd.matcherEvent = event;

//        log.debug(" currentCmd.matcherEvent={}", currentCmd.matcherEvent);
    }


    public void sendReduceEvent(OrderCommand cmd, Order order, long reducedBy) {
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

        event.nextEvent = cmd.matcherEvent;
        cmd.matcherEvent = event;
    }


    public void attachRejectEvent(OrderCommand cmd, long filledSize) {

//        log.debug("Rejected {}", cmd.orderId);
//        log.debug("\n{}", getL2MarketDataSnapshot(10).dumpOrderBook());

        final MatcherTradeEvent event = newMatcherEvent();

        event.eventType = MatcherEventType.REJECTION;

        event.activeOrderId = cmd.orderId;
        event.activeOrderUid = cmd.uid;
        event.activeOrderCompleted = false;
        event.activeOrderAction = cmd.action;
//        event.activeOrderSeq = cmd.seq;

        event.matchedOrderId = 0;
        event.matchedOrderCompleted = false;

        event.price = 0;
        event.size = cmd.size - filledSize;
        event.timestamp = cmd.timestamp;
        event.symbol = cmd.symbol;

        // insert event
        event.nextEvent = cmd.matcherEvent;
        cmd.matcherEvent = event;
    }

    private MatcherTradeEvent newMatcherEvent() {

        if (matcherTradeEventsPool == null) {
            return new MatcherTradeEvent();
        }

        if (eventsBucket == null || eventsBucketCounter == eventsBucket.length) {
            eventsBucketCounter = 0;
            eventsBucket = matcherTradeEventsPool.poll();
            if (eventsBucket == null) {
                eventsBucket = new MatcherTradeEvent[64];
                for (int i = 0; i < 64; i++) {
                    eventsBucket[i] = new MatcherTradeEvent();
                }
            }
        }

        return eventsBucket[eventsBucketCounter++];
    }

}
