package org.openpredict.exchange.beans;


import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.builder.EqualsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@AllArgsConstructor
@NoArgsConstructor
public final class MatcherTradeEvent {

    public MatcherEventType eventType; // TRADE, CANCEL or REJECTION (rare)

    public int symbol;

    // taker (for TRADE)
    public long activeOrderId;
    public long activeOrderUid;
    public boolean activeOrderCompleted; // false, except when activeOrder is completely filled (should be ignored for CANCEL or REJECTION)
    public OrderAction activeOrderAction; // assume matched order has opposite action
//    public long activeOrderSeq;

    // maker (for TRADE)
    public long matchedOrderId;
    public long matchedOrderUid; // 0 for rejection
    public boolean matchedOrderCompleted; // false, except when matchedOrder is completely filled

    public long price; // actual price of the deal (from maker order), 0 for rejection
    public long size;  // trade size, or unmatched size for REJECTION or CANCEL
    public long timestamp; // same as activeOrder related event timestamp

    public long bidderHoldPrice; // frozen price from BID order owner (depends on activeOrderAction)

    // reference to next event in chain
    public MatcherTradeEvent nextEvent;


    // testing only
    public MatcherTradeEvent copy() {
        MatcherTradeEvent evt = new MatcherTradeEvent();
        evt.eventType = this.eventType;
        evt.activeOrderId = this.activeOrderId;
        evt.activeOrderUid = this.activeOrderUid;
        evt.activeOrderCompleted = this.activeOrderCompleted;
        evt.activeOrderAction = this.activeOrderAction;
        evt.matchedOrderId = this.matchedOrderId;
        evt.matchedOrderUid = this.matchedOrderUid;
        evt.matchedOrderCompleted = this.matchedOrderCompleted;
        evt.price = this.price;
        evt.size = this.size;
        evt.timestamp = this.timestamp;
        evt.bidderHoldPrice = this.bidderHoldPrice;
        return evt;
    }

    // testing only
    public MatcherTradeEvent findTail() {
        MatcherTradeEvent tail = this;
        while (tail.nextEvent != null) {
            tail = tail.nextEvent;
        }
        return tail;
    }

    // testing only
    public static List<MatcherTradeEvent> asList(MatcherTradeEvent next) {
        List<MatcherTradeEvent> list = new ArrayList<>();
        while (next != null) {
            list.add(next);
            next = next.nextEvent;
        }
        return list;
    }

    /**
     * Compare next events chain as well.
     */
    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null) return false;
        if (!(o instanceof MatcherTradeEvent)) return false;
        MatcherTradeEvent other = (MatcherTradeEvent) o;
        return new EqualsBuilder()
                .append(symbol, other.symbol)
                .append(activeOrderId, other.activeOrderId)
                .append(activeOrderUid, other.activeOrderUid)
                .append(activeOrderCompleted, other.activeOrderCompleted)
                .append(activeOrderAction, other.activeOrderAction)
                .append(matchedOrderId, other.matchedOrderId)
                .append(matchedOrderUid, other.matchedOrderUid)
                .append(matchedOrderCompleted, other.matchedOrderCompleted)
                .append(price, other.price)
                .append(size, other.size)
                .append(bidderHoldPrice, other.bidderHoldPrice)
                // ignore timestamp
                .append(nextEvent, other.nextEvent)
                .isEquals();
    }

    /**
     * Includes chaining events
     */
    @Override
    public int hashCode() {
        return Objects.hash(
                symbol,
                activeOrderId,
                activeOrderUid,
                activeOrderCompleted,
                activeOrderAction,
                matchedOrderId,
                matchedOrderUid,
                matchedOrderCompleted,
                price,
                size,
                bidderHoldPrice,
                nextEvent);
    }


    @Override
    public String toString() {
        return "MatcherTradeEvent{" +
                "eventType=" + eventType +
                ", symbol=" + symbol +
                ", activeOrderId=" + activeOrderId +
                ", activeOrderUid=" + activeOrderUid +
                ", activeOrderCompleted=" + activeOrderCompleted +
                ", activeOrderAction=" + activeOrderAction +
                ", matchedOrderId=" + matchedOrderId +
                ", matchedOrderUid=" + matchedOrderUid +
                ", matchedOrderCompleted=" + matchedOrderCompleted +
                ", price=" + price +
                ", size=" + size +
                ", timestamp=" + timestamp +
                ", bidderHoldPrice=" + bidderHoldPrice +
                ", nextEvent=" + (nextEvent != null) +
                '}';
    }
}
