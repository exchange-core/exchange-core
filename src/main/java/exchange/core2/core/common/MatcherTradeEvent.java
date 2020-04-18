/*
 * Copyright 2019 Maksim Zheravin
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
package exchange.core2.core.common;


import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@AllArgsConstructor
@NoArgsConstructor
public final class MatcherTradeEvent {
    /*
        CANCEL needs remaining size (can write into size), bidderHoldPrice - can write into price
        REJECT needs remaining size (can not write into size),
     */

    // TODO move to the order?
    public MatcherEventType eventType; // TRADE, CANCEL, REJECTION (rare) or BINARY_EVENT (reports data)

    // TODO join (requires 11+ bits)
    public int section;

    // taker (for TRADE)
    // TODO move to the order?
    public boolean activeOrderCompleted; // false, except when activeOrder is completely filled, removed or rejected

    // maker (for TRADE)
    public long matchedOrderId;
    public long matchedOrderUid; // 0 for rejection
    public boolean matchedOrderCompleted; // false, except when matchedOrder is completely filled

    public long price; // actual price of the deal (from maker order), 0 for rejection
    public long size;  // trade size, or unmatched size for REJECTION or CANCEL
    //public long timestamp; // same as activeOrder related event timestamp

    public long bidderHoldPrice; // frozen price from BID order owner (depends on activeOrderAction)

    // reference to next event in chain
    public MatcherTradeEvent nextEvent;


    // testing only
    public MatcherTradeEvent copy() {
        MatcherTradeEvent evt = new MatcherTradeEvent();
        evt.eventType = this.eventType;
        evt.section = this.section;
        evt.activeOrderCompleted = this.activeOrderCompleted;
        evt.matchedOrderId = this.matchedOrderId;
        evt.matchedOrderUid = this.matchedOrderUid;
        evt.matchedOrderCompleted = this.matchedOrderCompleted;
        evt.price = this.price;
        evt.size = this.size;
//        evt.timestamp = this.timestamp;
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

    public int getChainSize() {
        MatcherTradeEvent tail = this;
        int c = 1;
        while (tail.nextEvent != null) {
            tail = tail.nextEvent;
            c++;
        }
        return c;
    }

    @NotNull
    public static MatcherTradeEvent createEventChain(int chainLength) {
        final MatcherTradeEvent head = new MatcherTradeEvent();
        MatcherTradeEvent prev = head;
        for (int j = 1; j < chainLength; j++) {
            MatcherTradeEvent nextEvent = new MatcherTradeEvent();
            prev.nextEvent = nextEvent;
            prev = nextEvent;
        }
        return head;
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

        // ignore timestamp
        return section == other.section
                && activeOrderCompleted == other.activeOrderCompleted
                && matchedOrderId == other.matchedOrderId
                && matchedOrderUid == other.matchedOrderUid
                && matchedOrderCompleted == other.matchedOrderCompleted
                && price == other.price
                && size == other.size
                && bidderHoldPrice == other.bidderHoldPrice
                && ((nextEvent == null && other.nextEvent == null) || (nextEvent != null && nextEvent.equals(other.nextEvent)));
    }

    /**
     * Includes chaining events
     */
    @Override
    public int hashCode() {
        return Objects.hash(
                section,
                activeOrderCompleted,
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
                ", section=" + section +
                ", activeOrderCompleted=" + activeOrderCompleted +
                ", matchedOrderId=" + matchedOrderId +
                ", matchedOrderUid=" + matchedOrderUid +
                ", matchedOrderCompleted=" + matchedOrderCompleted +
                ", price=" + price +
                ", size=" + size +
//                ", timestamp=" + timestamp +
                ", bidderHoldPrice=" + bidderHoldPrice +
                ", nextEvent=" + (nextEvent != null) +
                '}';
    }
}
