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

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

// TODO move activeOrderCompleted, eventType, section into the order?
// TODO REDUCE needs remaining size (can write into size), bidderHoldPrice - can write into price
// TODO REJECT needs remaining size (can not write into size),

public final class MatcherTradeEvent {

    public MatcherEventType eventType; // TRADE, REDUCE, REJECT (rare) or BINARY_EVENT (reports data)

    public int section;

    // TODO join (requires 11+ bits)
    // false, except when activeOrder is completely filled, removed or rejected
    // it is always true for REJECT event
    // it is true for REDUCE event if reduce was triggered by COMMAND
    public boolean activeOrderCompleted;

    // maker (for TRADE event type only)
    public long matchedOrderId;
    public long matchedOrderUid; // 0 for rejection
    public boolean matchedOrderCompleted; // false, except when matchedOrder is completely filled

    // actual price of the deal (from maker order), 0 for rejection (price can be take from original order)
    public long price;

    // TRADE - trade size
    // REDUCE - effective reduce size of REDUCE command, or not filled size for CANCEL command
    // REJECT - unmatched size of rejected order
    public long size;

    //public long timestamp; // same as activeOrder related event timestamp

    // frozen price from BID order owner (depends on activeOrderAction)
    public long bidderHoldPrice;
    public long matchedOrderTakerFee = -1;
    public long matchedOrderMakerFee = -1;

    // reference to next event in chain
    public MatcherTradeEvent nextEvent;


    // Copies a single event.
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
        evt.matchedOrderTakerFee = this.matchedOrderTakerFee;
        evt.matchedOrderMakerFee = this.matchedOrderMakerFee;
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
                && ((nextEvent == null && other.nextEvent == null) || (nextEvent != null && nextEvent.equals(other.nextEvent)))
                && matchedOrderMakerFee == other.matchedOrderMakerFee
                && matchedOrderTakerFee == other.matchedOrderTakerFee;
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
                nextEvent,
                matchedOrderMakerFee,
                matchedOrderTakerFee);
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

    public MatcherTradeEvent(
            MatcherEventType eventType,
            int section,
            boolean activeOrderCompleted,
            long matchedOrderId,
            long matchedOrderUid,
            boolean matchedOrderCompleted,
            long price,
            long size,
            long bidderHoldPrice,
            long matchedOrderTakerFee,
            long matchedOrderMakerFee,
            MatcherTradeEvent nextEvent) {
        this.eventType = eventType;
        this.section = section;
        this.activeOrderCompleted = activeOrderCompleted;
        this.matchedOrderId = matchedOrderId;
        this.matchedOrderUid = matchedOrderUid;
        this.matchedOrderCompleted = matchedOrderCompleted;
        this.price = price;
        this.size = size;
        this.bidderHoldPrice = bidderHoldPrice;
        this.matchedOrderTakerFee = matchedOrderTakerFee;
        this.matchedOrderMakerFee = matchedOrderMakerFee;
        this.nextEvent = nextEvent;
    }

    public MatcherTradeEvent() {
    }

    public static MatcherTradeEventBuilder builder() {
        return new MatcherTradeEventBuilder();
    }

    public static class MatcherTradeEventBuilder {
        private MatcherEventType eventType;
        private int section;
        private boolean activeOrderCompleted;
        private long matchedOrderId;
        private long matchedOrderUid;
        private boolean matchedOrderCompleted;
        private long price;
        private long size;
        private long bidderHoldPrice;
        private long matchedOrderTakerFee;
        private long matchedOrderMakerFee;
        private MatcherTradeEvent nextEvent;

        MatcherTradeEventBuilder() {
        }

        public MatcherTradeEventBuilder eventType(MatcherEventType eventType) {
            this.eventType = eventType;
            return this;
        }

        public MatcherTradeEventBuilder section(int section) {
            this.section = section;
            return this;
        }

        public MatcherTradeEventBuilder activeOrderCompleted(boolean activeOrderCompleted) {
            this.activeOrderCompleted = activeOrderCompleted;
            return this;
        }

        public MatcherTradeEventBuilder matchedOrderId(long matchedOrderId) {
            this.matchedOrderId = matchedOrderId;
            return this;
        }

        public MatcherTradeEventBuilder matchedOrderUid(long matchedOrderUid) {
            this.matchedOrderUid = matchedOrderUid;
            return this;
        }

        public MatcherTradeEventBuilder matchedOrderCompleted(boolean matchedOrderCompleted) {
            this.matchedOrderCompleted = matchedOrderCompleted;
            return this;
        }

        public MatcherTradeEventBuilder price(long price) {
            this.price = price;
            return this;
        }

        public MatcherTradeEventBuilder size(long size) {
            this.size = size;
            return this;
        }

        public MatcherTradeEventBuilder bidderHoldPrice(long bidderHoldPrice) {
            this.bidderHoldPrice = bidderHoldPrice;
            return this;
        }

        public MatcherTradeEventBuilder matchedOrderTakerFee(long matchedOrderTakerFee) {
            this.matchedOrderTakerFee = matchedOrderTakerFee;
            return this;
        }

        public MatcherTradeEventBuilder matchedOrderMakerFee(long matchedOrderMakerFee) {
            this.matchedOrderMakerFee = matchedOrderMakerFee;
            return this;
        }

        public MatcherTradeEventBuilder nextEvent(MatcherTradeEvent nextEvent) {
            this.nextEvent = nextEvent;
            return this;
        }

        public MatcherTradeEvent build() {
            return new MatcherTradeEvent(
                    this.eventType,
                    this.section,
                    this.activeOrderCompleted,
                    this.matchedOrderId,
                    this.matchedOrderUid,
                    this.matchedOrderCompleted,
                    this.price,
                    this.size,
                    this.bidderHoldPrice,
                    this.matchedOrderTakerFee,
                    this.matchedOrderMakerFee,
                    this.nextEvent);
        }

        @Override
        public String toString() {
            return "MatcherTradeEvent.MatcherTradeEventBuilder(eventType=" + this.eventType
                    + ", section=" + this.section
                    + ", activeOrderCompleted=" + this.activeOrderCompleted
                    + ", matchedOrderId=" + this.matchedOrderId
                    + ", matchedOrderUid=" + this.matchedOrderUid
                    + ", matchedOrderCompleted=" + this.matchedOrderCompleted
                    + ", price=" + this.price
                    + ", size=" + this.size
                    + ", bidderHoldPrice=" + this.bidderHoldPrice
                    + ", matchedOrderTakerFee=" + this.matchedOrderTakerFee
                    + ", matchedOrderMakerFee=" + this.matchedOrderMakerFee
                    + ", nextEvent=" + this.nextEvent + ")";
        }
    }
}
