package org.openpredict.exchange.beans;


import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

@AllArgsConstructor
@NoArgsConstructor
@ToString
public class MatcherTradeEvent {

    public MatcherEventType eventType; // TRADE, REDUCE or REJECTION (rare)

    public int symbol;

    public long activeOrderId;
    public long activeOrderUid;
    public boolean activeOrderCompleted; // false, except when activeOrder is completely filled
    public OrderAction activeOrderAction; // assume matched order has opposite action
//    public long activeOrderSeq;

    public long matchedOrderId;
    public long matchedOrderUid; // 0 for rejection
    public boolean matchedOrderCompleted; // false, except when matchedOrder is completely filled

    public long price; // 0 for rejection
    public long size;  // ? unmatched size for rejection
    public long timestamp; // same as activeOrder related event timestamp

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
        return evt;
    }

}
