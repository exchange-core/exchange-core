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
package exchange.core2.core.orderbook;

import exchange.core2.core.common.*;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class OrderBookDirectImpl implements IOrderBook {

    // buckets
    private final NavigableMap<Long, Bucket> askPriceBuckets = new TreeMap<>();
    private final NavigableMap<Long, Bucket> bidPriceBuckets = new TreeMap<>(Collections.reverseOrder());

    // symbol specification
    private final CoreSymbolSpecification symbolSpec;

    // index: orderId -> order
    private final LongObjectHashMap<DirectOrder> orderIdIndex = new LongObjectHashMap<>();

    // updatable heads (nullable)
    private DirectOrder bestAskOrder = null;
    private DirectOrder bestBidOrder = null;

    // Object pools
    private final ArrayDeque<DirectOrder> ordersPool = new ArrayDeque<>(4096);
    // private final ArrayDeque<Bucket> bucketsPool = new ArrayDeque<>(4096);

    public OrderBookDirectImpl(final CoreSymbolSpecification symbolSpec) {
        this.symbolSpec = symbolSpec;
    }

    public OrderBookDirectImpl(final BytesIn bytes) {
        this.symbolSpec = new CoreSymbolSpecification(bytes);

        final int size = bytes.readInt();
        for (int i = 0; i < size; i++) {
            DirectOrder order = new DirectOrder(bytes);
            insertOrder(order);
            orderIdIndex.put(order.orderId, order);
        }

        //validateInternalState();
    }

    @Override
    public CommandResultCode newOrder(OrderCommand cmd) {
        final OrderType orderType = cmd.orderType;
        final long size = cmd.size;

        // check if order is marketable there are matching orders
        final long filledSize = tryMatchInstantly(cmd, cmd);
        if (filledSize == size) {
            // completed before being placed - can just return
            return CommandResultCode.SUCCESS;
        }

        if (orderType == OrderType.IOC) {
            // send reject for not-completed ImmediateOrCancel order
            OrderBookEventsHelper.attachRejectEvent(cmd, size - filledSize);
            return CommandResultCode.SUCCESS;
        }

        final long orderId = cmd.orderId;
        if (orderIdIndex.containsKey(orderId)) { // TODO eliminate double hashtable lookup?
            // duplicate order id - can match, but can not place
            OrderBookEventsHelper.attachRejectEvent(cmd, size - filledSize);
            return CommandResultCode.MATCHING_DUPLICATE_ORDER_ID;
        }

        final long price = cmd.price;

        // normally placing regular GTC order
        DirectOrder orderRecord = ordersPool.pollLast();
        if (orderRecord == null) {
            orderRecord = new DirectOrder();
        }

        orderRecord.orderId = orderId;
        orderRecord.price = price;
        orderRecord.size = size;
        orderRecord.reserveBidPrice = cmd.reserveBidPrice;
        orderRecord.action = cmd.action;
        orderRecord.uid = cmd.uid;
        orderRecord.timestamp = cmd.timestamp;
        orderRecord.filled = filledSize;


        orderIdIndex.put(orderId, orderRecord);
        insertOrder(orderRecord);

        return CommandResultCode.SUCCESS;
    }


    /**
     * Get bucket by order action
     *
     * @param action - action
     * @return bucket - navigable map
     */
    private NavigableMap<Long, Bucket> getBucketsByAction(OrderAction action) {
        return action == OrderAction.ASK ? askPriceBuckets : bidPriceBuckets;
    }


    private long tryMatchInstantly(
            final IOrder takerOrder,
            final OrderCommand triggerCmd) {

        long remainingSize = takerOrder.getSize() - takerOrder.getFilled();

        if (remainingSize == 0) {
            return takerOrder.getFilled();
        }

        final long limitPrice = takerOrder.getPrice();

        DirectOrder makerOrder;
        boolean isBidAction = takerOrder.getAction() == OrderAction.BID;
        if (isBidAction) {
            makerOrder = bestAskOrder;
            if (makerOrder == null || makerOrder.price > limitPrice) {
                return takerOrder.getFilled();
            }
        } else {
            makerOrder = bestBidOrder;
            if (makerOrder == null || makerOrder.price < limitPrice) {
                return takerOrder.getFilled();
            }
        }

        DirectOrder takerOwnOrders = null;
        DirectOrder priceBucketTail = makerOrder.parent.tail;

        // iterate through all orders
        do {

            if (makerOrder.uid != takerOrder.getUid()) {
                // calculate exact volume can fill for this order
                // log.debug("volumeToCollect={} order: s{} f{}", volumeToCollect, order.size, order.filled);
                final long tradeSize = Math.min(remainingSize, makerOrder.size - makerOrder.filled);
                // log.debug("totalMatchingVolume={} v={}", totalMatchingVolume, v);

                makerOrder.filled += tradeSize;
                makerOrder.parent.volume -= tradeSize;
                remainingSize -= tradeSize;

                // remove from order book filled orders
                boolean makerCompleted = makerOrder.size == makerOrder.filled;

                OrderBookEventsHelper.sendTradeEvent(triggerCmd, takerOrder, makerOrder, makerCompleted, remainingSize == 0, tradeSize);

                if (!makerCompleted) {
                    break;
                }

                // if completed can remove maker order
                orderIdIndex.remove(makerOrder.orderId);
                ordersPool.add(makerOrder);

            } else {
                // attach own orders to separate chain for later processing
                makerOrder.next = takerOwnOrders;
                takerOwnOrders = makerOrder;
                // TODO remove price
            }

            if (makerOrder == priceBucketTail) {
                // reached current price tail -> remove bucket reference
                final NavigableMap<Long, Bucket> buckets = isBidAction ? askPriceBuckets : bidPriceBuckets;
                buckets.remove(makerOrder.price);

                // set next price tail
                priceBucketTail = makerOrder.parent.tail;
            }

            // switch to next order
            makerOrder = makerOrder.prev; // can be null

        } while (makerOrder != null && remainingSize > 0);

        // break chain after last order
        if (makerOrder != null) {
            makerOrder.next = null;
        }

        // update best orders order
        if (isBidAction) {
            bestAskOrder = makerOrder;
        } else {
            bestBidOrder = makerOrder;
        }

        // process chain of own orders
        while (takerOwnOrders != null) {
            // TODO not always correct because will insert into the end of the queue
            insertOrder(takerOwnOrders);
            takerOwnOrders = takerOwnOrders.next;
        }

        // return filled amount
        return takerOrder.getSize() - remainingSize;
    }


    @Override
    public boolean cancelOrder(OrderCommand cmd) {

        final long orderId = cmd.orderId;

        final DirectOrder order = orderIdIndex.get(orderId);
        if (order == null || order.uid != cmd.uid) {
            return false;
        }

        removeOrder(order);

        OrderBookEventsHelper.sendCancelEvent(cmd, order);

        return true;
    }

    @Override
    public CommandResultCode moveOrder(OrderCommand cmd) {

        // order lookup
        final DirectOrder orderToMove = orderIdIndex.get(cmd.orderId);
        if (orderToMove == null || orderToMove.uid != cmd.uid) {
            return CommandResultCode.MATCHING_UNKNOWN_ORDER_ID;
        }

        // remove order
        removeOrder(orderToMove);

        // update price
        orderToMove.price = cmd.price;

        // try match with new price as a taker order
        final long filled = tryMatchInstantly(orderToMove, cmd);
        if (filled == orderToMove.size) {
            // order was fully matched - removing
            orderIdIndex.remove(cmd.orderId);
            // saving free object back to pool
            ordersPool.addLast(orderToMove);
            return CommandResultCode.SUCCESS;
        }

        // not filled completely, inserting into new position
        orderToMove.filled = filled;

        // insert into a new place
        insertOrder(orderToMove);

        return CommandResultCode.SUCCESS;
    }


    private void removeOrder(final DirectOrder order) {

        final Bucket bucket = order.parent;
        bucket.volume -= order.size - order.filled;

        if (bucket.tail == order) {
            // if we removing tail order -> change bucket tail reference
            if (order.next == null || order.next.parent != bucket) {
                // if no next or next order has different parent -> then it was the last bucket -> remove record
                final NavigableMap<Long, Bucket> buckets = order.action == OrderAction.ASK ? askPriceBuckets : bidPriceBuckets;
                buckets.remove(order.price);
            } else {
                // otherwise at least one order always having the same parent left -> update tail reference to it
                bucket.tail = order.next; // always not null
            }
        }

        // update neighbor orders
        if (order.next != null) {
            order.next.prev = order.prev; // can be null
        }
        if (order.prev != null) {
            order.prev.next = order.next; // can be null
        }

        // check if best ask/bid were referring to the order we just removed
        if (order == bestAskOrder) {
            bestAskOrder = order.prev; // can be null
        } else if (order == bestBidOrder) {
            bestBidOrder = order.prev; // can be null
        }
    }


    private void insertOrder(final DirectOrder order) {

        final boolean isAsk = order.action == OrderAction.ASK;
        final NavigableMap<Long, Bucket> buckets = isAsk ? askPriceBuckets : bidPriceBuckets;
        final Bucket toBucket = buckets.get(order.price);

        if (toBucket != null) {
            // update tail if bucket already exists
            toBucket.volume += order.size - order.filled;
            toBucket.tail = order;
            final DirectOrder oldTail = toBucket.tail; // always exists, not null
            final DirectOrder prevOrder = oldTail.prev; // can be null
            // update neighbors
            oldTail.prev = order;
            if (prevOrder != null) {
                prevOrder.next = order;
            }
            // update self
            order.next = oldTail;
            order.prev = prevOrder;
            order.parent = toBucket;

        } else {

            // insert a new bucket
            final Bucket newBucket = new Bucket(order);
            order.parent = newBucket;
            buckets.put(order.price, newBucket);
            final Map.Entry<Long, Bucket> floorEntry = buckets.floorEntry(order.price);
            if (floorEntry != null) {
                // update tail
                final Bucket floorBucket = floorEntry.getValue();
                DirectOrder floorTail = floorBucket.tail;
                final DirectOrder prevOrder = floorTail.prev; // can be null
                // update neighbors
                floorTail.prev = order;
                if (prevOrder != null) {
                    prevOrder.next = order;
                }
                // update self
                order.next = floorTail;
                order.prev = prevOrder;
            } else {

                // if no floor entry, then update best order
                final DirectOrder oldBestOrder = isAsk ? bestAskOrder : bestBidOrder; // can be null

                if (oldBestOrder != null) {
                    oldBestOrder.next = order;
                }

                if (isAsk) {
                    bestAskOrder = order;
                } else {
                    bestBidOrder = order;
                }

                // update self
                order.next = null;
                order.prev = oldBestOrder;
            }
        }
    }

    @Override
    public int getOrdersNum() {
        return orderIdIndex.size();
    }

    @Override
    public IOrder getOrderById(final long orderId) {
        return orderIdIndex.get(orderId);
    }

    @Override
    public void validateInternalState() {
        final LongHashSet ordersInChain = new LongHashSet(orderIdIndex.size());
        validateChain(this.bestAskOrder, true, ordersInChain);
        validateChain(this.bestBidOrder, false, ordersInChain);

        if (!orderIdIndex.keySet().equals(ordersInChain)) {
            thrw("orderIdIndex does not match to the chained orders");
        }
    }

    private void validateChain(DirectOrder order, boolean asksChain, LongHashSet ordersInChain) {

        if (order != null && order.next != null) {
            thrw("best order has not-null next reference");
        }

        long lastPrice = -1;
        long expectedBucketVolume = 0;
        DirectOrder lastOrder = null;

        while (order != null) {

            if (ordersInChain.contains(order.orderId)) {
                thrw("duplicate orderid in the chain");
            }
            ordersInChain.add(order.orderId);

            expectedBucketVolume += order.size - order.filled;

            if (lastOrder != null && order.next != lastOrder) {
                thrw("incorrect next reference");
            }
            if (order.parent.tail.price != order.price) {
                thrw("price of parent.tail differs");
            }
            if (lastPrice != -1 && order.price != lastPrice) {
                if (asksChain ^ order.price > lastPrice) {
                    thrw("unexpected price change direction");
                }
                if (order.next.parent == order.parent) {
                    thrw("unexpected price change within same bucket");
                }
            }

            if (order.parent.tail == order) {
                if (order.parent.volume != expectedBucketVolume) {
                    thrw("bucket volume does not match orders chain sizes");
                }
                if (order.prev != null && order.prev.price == order.price) {
                    thrw("previous bucket has the same price");
                }
                expectedBucketVolume = 0;
            }

            if (asksChain ^ order.action == OrderAction.ASK) {
                thrw("not expected order action");
            }

            lastPrice = order.price;
            lastOrder = order;
            order = order.prev;
        }

        // validate last order
        if (lastOrder != null && lastOrder.parent.tail != lastOrder) {
            thrw("last order is not a tail");
        }
    }

    public void thrw(final String msg) {
        throw new IllegalStateException(msg);
    }

    @Override
    public OrderBookImplType getImplementationType() {
        return OrderBookImplType.DIRECT;
    }

    @Override
    public List<Order> findUserOrders(long uid) {
        final List<Order> list = new ArrayList<>();
        orderIdIndex.forEachValue(order -> {
            if (order.uid == uid) {
                list.add(Order.builder()
                        .orderId(order.orderId)
                        .price(order.price)
                        .size(order.size)
                        .filled(order.filled)
                        .reserveBidPrice(order.reserveBidPrice)
                        .action(order.action)
                        .uid(order.uid)
                        .timestamp(order.timestamp)
                        .build());
            }
        });

        return list;
    }

    @Override
    public CoreSymbolSpecification getSymbolSpec() {
        return symbolSpec;
    }

    @Override
    public Stream<DirectOrder> askOrdersStream(boolean sortedIgnore) {
        return StreamSupport.stream(new OrdersSpliterator(bestAskOrder), false);
    }

    @Override
    public Stream<DirectOrder> bidOrdersStream(boolean sortedIgnore) {
        return StreamSupport.stream(new OrdersSpliterator(bestBidOrder), false);
    }

    @Override
    public void fillAsks(final int size, L2MarketData data) {
        if (size == 0) {
            data.askSize = 0;
            return;
        }

        int i = 0;
        for (Bucket bucket : askPriceBuckets.values()) {
            data.askPrices[i] = bucket.tail.price;
            data.askVolumes[i] = bucket.volume;
            if (++i == size) {
                break;
            }
        }
        data.askSize = i;
    }

    @Override
    public void fillBids(final int size, L2MarketData data) {
        if (size == 0) {
            data.bidSize = 0;
            return;
        }

        int i = 0;
        for (Bucket bucket : bidPriceBuckets.values()) {
            data.bidPrices[i] = bucket.tail.price;
            data.bidVolumes[i] = bucket.volume;
            if (++i == size) {
                break;
            }
        }
        data.bidSize = i;
    }

    @Override
    public int getTotalAskBuckets() {
        return askPriceBuckets.size();
    }

    @Override
    public int getTotalBidBuckets() {
        return bidPriceBuckets.size();
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeByte(getImplementationType().getCode());
        symbolSpec.writeMarshallable(bytes);
        bytes.writeInt(orderIdIndex.size());
        askOrdersStream(true).forEach(order -> order.writeMarshallable(bytes));
        bidOrdersStream(true).forEach(order -> order.writeMarshallable(bytes));
    }

    @Override
    public int hashCode() {

        // TODO wrong
        IOrdersBucket[] a = this.askPriceBuckets.values().toArray(new IOrdersBucket[0]);
        IOrdersBucket[] b = this.bidPriceBuckets.values().toArray(new IOrdersBucket[0]);
//        for(IOrdersBucket ord: a) log.debug("ask {}", ord);
//        for(IOrdersBucket ord: b) log.debug("bid {}", ord);
        return IOrderBook.hash(a, b, symbolSpec);
    }

    @Override
    public boolean equals(Object o) {
        return IOrderBook.equals(this, o);
    }


    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static final class DirectOrder implements WriteBytesMarshallable, IOrder {

        @Getter
        public long orderId;

        @Getter
        public long price;

        @Getter
        public long size;

        @Getter
        public long filled;

        // new orders - reserved price for fast moves of GTC bid orders in exchange mode
        @Getter
        public long reserveBidPrice;

        // required for PLACE_ORDER only;
        @Getter
        public OrderAction action;

        @Getter
        public long uid;

        @Getter
        public long timestamp;

        // fast orders structure

        Bucket parent;

        // next order (towards the matching direction, price grows for asks)
        DirectOrder next;

        // previous order (to the tail of the queue)
        DirectOrder prev;


        // public int userCookie;

        public DirectOrder(BytesIn bytes) {


            this.orderId = bytes.readLong(); // orderId
            this.price = bytes.readLong();  // price
            this.size = bytes.readLong(); // size
            this.filled = bytes.readLong(); // filled
            this.reserveBidPrice = bytes.readLong(); // price2
            this.action = OrderAction.of(bytes.readByte());
            this.uid = bytes.readLong(); // uid
            this.timestamp = bytes.readLong(); // timestamp
            // this.userCookie = bytes.readInt();  // userCookie

            // TODO
        }

        @Override
        public void writeMarshallable(BytesOut bytes) {
            bytes.writeLong(orderId);
            bytes.writeLong(price);
            bytes.writeLong(size);
            bytes.writeLong(filled);
            bytes.writeLong(reserveBidPrice);
            bytes.writeByte(action.getCode());
            bytes.writeLong(uid);
            bytes.writeLong(timestamp);
            // bytes.writeInt(userCookie);
            // TODO
        }

        @Override
        public String toString() {
            return "[" + orderId + " " + (action == OrderAction.ASK ? 'A' : 'B')
                    + price + ":" + size + "F" + filled
                    // + " C" + userCookie
                    + " U" + uid + "]";
        }

        @Override
        public int hashCode() {
            return Objects.hash(orderId, action, price, size, reserveBidPrice, filled,
                    //userCookie,
                    uid);
        }


        /**
         * timestamp is not included into hashCode() and equals() for repeatable results
         */
        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (o == null) return false;
            if (!(o instanceof DirectOrder)) return false;

            DirectOrder other = (DirectOrder) o;
            return new EqualsBuilder()
                    .append(orderId, other.orderId)
                    .append(action, other.action)
                    .append(price, other.price)
                    .append(size, other.size)
                    .append(reserveBidPrice, other.reserveBidPrice)
                    .append(filled, other.filled)
//                .append(userCookie, other.userCookie)
                    .append(uid, other.uid)
                    //.append(timestamp, other.timestamp)
                    .isEquals();
        }

    }

    private static class Bucket {
        long volume;
        DirectOrder tail;

        Bucket(DirectOrder order) {
            this.tail = order;
            this.volume = order.size - order.filled;
        }
    }
}
