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

import com.google.common.collect.ObjectArrays;
import exchange.core2.core.common.*;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.utils.SerializationUtils;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public final class OrderBookFastImpl implements IOrderBook {

    public static final int DEFAULT_HOT_WIDTH = 32768;

    private final CoreSymbolSpecification symbolSpec;

    private final int hotPricesRange;

    private BitSet hotAskBitSet;
    private BitSet hotBidBitSet;
    private final LongObjectHashMap<IOrdersBucket> hotAskBuckets;
    private final LongObjectHashMap<IOrdersBucket> hotBidBuckets;
    private long minAskPrice = Long.MAX_VALUE;
    private long maxBidPrice = 0;

    // Bucket within FAR section: (price < basePrice) OR (price >= basePrice + hotPricesRange)
    // Bucket within HOT section: (price >= basePrice) AND (price < basePrice + hotPricesRange)
    private long basePrice = -1;
    private long rebalanceThresholdLow = -1;
    private long rebalanceThresholdHigh = -1;

    // TODO garbage-free navigable map implementation
    private final NavigableMap<Long, IOrdersBucket> farAskBuckets;
    private final NavigableMap<Long, IOrdersBucket> farBidBuckets;

    // Hashtable for fast (cached) resolving OrderId -> Bucket
    private final LongObjectHashMap<IOrdersBucket> idMapToBucket = new LongObjectHashMap<>();

    // Object pools
    private final ArrayDeque<Order> ordersPool = new ArrayDeque<>(16384);
    private final ArrayDeque<IOrdersBucket> bucketsPool = new ArrayDeque<>(16384);

    public OrderBookFastImpl(final int hotPricesRange, final CoreSymbolSpecification symbolSpec) {
        // must be aligned by 64 bit, can not be lower than 1024
        if ((hotPricesRange & 63) != 0 || hotPricesRange < 1024) {
            throw new IllegalArgumentException("invalid hotPricesRange=" + hotPricesRange);
        }
        this.symbolSpec = symbolSpec;
        this.hotPricesRange = hotPricesRange;
        this.hotAskBitSet = new BitSet(hotPricesRange);
        this.hotBidBitSet = new BitSet(hotPricesRange);
        this.hotAskBuckets = new LongObjectHashMap<>();
        this.hotBidBuckets = new LongObjectHashMap<>();
        this.farAskBuckets = new TreeMap<>();
        this.farBidBuckets = new TreeMap<>(Collections.reverseOrder());
    }

    public OrderBookFastImpl(final BytesIn bytes) {

        this.symbolSpec = new CoreSymbolSpecification(bytes);

        this.hotPricesRange = bytes.readInt();

        this.hotAskBitSet = SerializationUtils.readBitSet(bytes);
        this.hotBidBitSet = SerializationUtils.readBitSet(bytes);

        this.hotAskBuckets = SerializationUtils.readLongHashMap(bytes, IOrdersBucket::create);
        this.hotBidBuckets = SerializationUtils.readLongHashMap(bytes, IOrdersBucket::create);

        this.minAskPrice = bytes.readLong();
        this.maxBidPrice = bytes.readLong();

        this.basePrice = bytes.readLong();
        this.rebalanceThresholdLow = bytes.readLong();
        this.rebalanceThresholdHigh = bytes.readLong();

        this.farAskBuckets = SerializationUtils.readLongMap(bytes, TreeMap::new, IOrdersBucket::create);
        this.farBidBuckets = SerializationUtils.readLongMap(bytes, () -> new TreeMap<>(Collections.reverseOrder()), IOrdersBucket::create);

        // reconstruct ordersId-> Bucket cache
        // TODO check resulting performance
        hotAskBuckets.forEach(bucket -> bucket.forEachOrder(order -> idMapToBucket.put(order.orderId, bucket)));
        hotBidBuckets.forEach(bucket -> bucket.forEachOrder(order -> idMapToBucket.put(order.orderId, bucket)));
        farAskBuckets.values().forEach(bucket -> bucket.forEachOrder(order -> idMapToBucket.put(order.orderId, bucket)));
        farBidBuckets.values().forEach(bucket -> bucket.forEachOrder(order -> idMapToBucket.put(order.orderId, bucket)));

        //validateInternalState();
    }

    private int priceToIndex(long price) {
        long idx = price - basePrice;
        if (idx < Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        } else if (idx > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) idx;
    }

    private long indexToPrice(int idx) {
        return idx + basePrice;
    }

    @Override
    public CommandResultCode newOrder(OrderCommand cmd) {

        final OrderType orderType = cmd.orderType;
        final long size = cmd.size;

        // check if order is marketable there are matching orders
        final long filledSize = tryMatchInstantly(cmd, 0, cmd);
        if (filledSize == size) {
            // fully matched as marketable before actually place - can just return
            return CommandResultCode.SUCCESS;
        }

        if (orderType == OrderType.IOC) {
            OrderBookEventsHelper.attachRejectEvent(cmd, size - filledSize);
            return CommandResultCode.SUCCESS;
        }

        final long orderId = cmd.orderId;
        if (idMapToBucket.containsKey(orderId)) {
            // duplicate order id - can match, but can not place
            OrderBookEventsHelper.attachRejectEvent(cmd, size - filledSize);
            return CommandResultCode.MATCHING_DUPLICATE_ORDER_ID;
        }

        final long price = cmd.price;
        if (basePrice == -1) {
            // first GTC limit order will define a base price (middle of the hotPricesRange range)
            setBasePrice(calculateBasePrice(price));
        }

        // normally placing regular GTC order

        Order orderRecord = ordersPool.pollLast();
        if (orderRecord == null) {
            orderRecord = new Order();
        }

        orderRecord.orderId = orderId;
        orderRecord.price = price;
        orderRecord.size = size;
        orderRecord.reserveBidPrice = cmd.reserveBidPrice;
        orderRecord.action = cmd.action;
        orderRecord.uid = cmd.uid;
        orderRecord.timestamp = cmd.timestamp;
        orderRecord.filled = filledSize;

        final IOrdersBucket bucket = cmd.action == OrderAction.ASK ? getOrCreateNewBucketAck(price) : getOrCreateNewBucketBid(price);
        bucket.put(orderRecord);
        idMapToBucket.put(orderId, bucket);

        return CommandResultCode.SUCCESS;
    }

    /**
     * Calculate base price so the given price would be in the center of the hotPricesRange range.<br/>
     * Will also do the 'long' alignment for faster bitset shift operations.
     *
     * @param price - central price
     * @return new base price
     */
    private long calculateBasePrice(long price) {
        long newBasePrice = (price - (hotPricesRange / 2) + 31) & (~63L);
        return Math.max(0, newBasePrice);
    }

    /**
     * Set new base price and update re-balance thresholds (1/4 and 3/4 of the hotPricesRange range)
     *
     * @param newBasePrice - new base price
     */
    private void setBasePrice(long newBasePrice) {
        basePrice = newBasePrice;
        rebalanceThresholdLow = newBasePrice + hotPricesRange / 4;
        rebalanceThresholdHigh = newBasePrice + hotPricesRange / 4 * 3;
    }

    private IOrdersBucket getOrCreateNewBucketAck(long price) {

        // if price is too low - time to re-balance
        if (price <= rebalanceThresholdLow) {
            // decreasing base price
            decreaseBasePrice(calculateBasePrice(price));
        }

        int idx = priceToIndex(price);
        boolean far = (idx >= hotPricesRange);

//        log.debug("{} idx={} FAR={}", price, idx, far);
        IOrdersBucket ordersBucket = far ? farAskBuckets.get(price) : hotAskBuckets.get(price);

        if (ordersBucket != null) {
            // bucket exists
            return ordersBucket;
        }

        ordersBucket = bucketsPool.pollLast();
        if (ordersBucket == null) {
            ordersBucket = new OrdersBucketFastImpl();
//            ordersBucket = new OrdersBucketNaiveImpl();
        }

        ordersBucket.setPrice(price);
        minAskPrice = Math.min(minAskPrice, price);

        if (far) {
            farAskBuckets.put(price, ordersBucket);
        } else {
            hotAskBuckets.put(price, ordersBucket);
            hotAskBitSet.set(idx);
        }

        return ordersBucket;
    }

    private IOrdersBucket getOrCreateNewBucketBid(long price) {
        //log.debug("getOrCreateNewBucketBid: p={} rebalanceThresholdHigh={}", price, rebalanceThresholdHigh);
        if (price >= rebalanceThresholdHigh) {
            // increasing base price
            increaseBasePrice(calculateBasePrice(price));
        }

        int idx = priceToIndex(price);
        boolean far = idx < 0;

//        log.debug("{} idx={} FAR={}", price, idx, far);
        IOrdersBucket ordersBucket = far ? farBidBuckets.get(price) : hotBidBuckets.get(price);
        if (ordersBucket != null) {
            // bucket exists
            return ordersBucket;
        }

        ordersBucket = bucketsPool.pollLast();
        if (ordersBucket == null) {
            ordersBucket = new OrdersBucketFastImpl();
//            ordersBucket = new OrdersBucketNaiveImpl();
        }

        ordersBucket.setPrice(price);
        maxBidPrice = Math.max(maxBidPrice, price);

        if (far) {
            farBidBuckets.put(price, ordersBucket);
        } else {
            hotBidBuckets.put(price, ordersBucket);
            hotBidBitSet.set(idx);
        }
        return ordersBucket;
    }

    /**
     * Match the activeOrder instantly to specified sorted buckets map
     * Fully matching orders are removed from orderId index
     * Should any trades occur - they sent to tradesConsumer
     *
     * @param activeOrder - GTC or IOC activeOrder to match
     * @param filled      - current filled value of the activeOrder
     * @param triggerCmd  -
     * @return matched size (filled - if nothing is matching to the activeOrder)
     */
    private long tryMatchInstantly(
            final IOrder activeOrder,
            long filled,
            final OrderCommand triggerCmd) {

        long nextPrice;

        final long limitPrice = activeOrder.getPrice();
        final OrderAction action = activeOrder.getAction();

        if (action == OrderAction.BID) {
            if (minAskPrice > limitPrice) {
                // no orders to match
                return filled;
            }
            nextPrice = minAskPrice;
        } else {
            if (maxBidPrice < limitPrice) {
                // no orders to match
                return filled;
            }
            nextPrice = maxBidPrice;
        }

        while (filled < activeOrder.getSize()) {

            // search for next available bucket
            final IOrdersBucket bucket = (action == OrderAction.BID) ? nextAvailableBucketAsk(nextPrice, limitPrice) : nextAvailableBucketBid(nextPrice, limitPrice);
            if (bucket == null) {
                break;
            }

            final long tradePrice = bucket.getPrice();
            // next iteration price
            nextPrice = (action == OrderAction.BID) ? tradePrice + 1 : tradePrice - 1;

            // matching orders within bucket
            final long sizeLeft = activeOrder.getSize() - filled;
            // log.debug("bucket {} match size: {}", bucket.getPrice(), sizeLeft);
            filled += bucket.match(sizeLeft, activeOrder, triggerCmd, this::removeFullyMatchedOrder);

            // remove bucket if its empty
            if (bucket.getTotalVolume() == 0) {
                removeBucket(action.opposite(), tradePrice);
            }
        }
        return filled;
    }

    private void removeFullyMatchedOrder(Order mOrder) {
        // forget if fully matched
        idMapToBucket.remove(mOrder.orderId);
        // saving free object back to pool
        ordersPool.addLast(mOrder);
    }

    /**
     * Searches for next available bucket for matching starting from currentPrice inclusive and till lastPrice inclusive.
     *
     * @param currentPrice - price to start with
     * @param lastPrice    - limit price
     * @return bucket or null if not found
     */
    private IOrdersBucket nextAvailableBucketAsk(long currentPrice, long lastPrice) {
        int idx = priceToIndex(currentPrice);
        // normally searching within hot buckets
        if (idx < hotPricesRange) {
            int nextIdx = hotAskBitSet.nextSetBit(idx);
            // log.debug("A next {} for currentPrice={} lastPrice={}", next, currentPrice, lastPrice);
            if (nextIdx != -1) {
                // found a bucket, but if limit is reached - no need to check far orders, just return null
                long nextPrice = nextIdx + basePrice;
                return nextPrice <= lastPrice ? hotAskBuckets.get(nextPrice) : null;
            }
        }

        // TODO independent searching can be slower comparing to processing a subtree (NLogN vs N) for superorders, though it's easier to remove buckets
        // nothing yet found and limit also not reached yet, therefore trying to search far buckets
        Map.Entry<Long, IOrdersBucket> entry = farAskBuckets.ceilingEntry(currentPrice);
        return (entry != null && entry.getKey() <= lastPrice) ? entry.getValue() : null;
    }

    /**
     * Searches for next available bucket for matching starting from currentPrice inclusive and till lastPrice inclusive.
     *
     * @param currentPrice - price to start with
     * @param lastPrice    - limit price
     * @return bucket or null if not found
     */
    private IOrdersBucket nextAvailableBucketBid(long currentPrice, long lastPrice) {
        int idx = priceToIndex(currentPrice);
        // normally searching within hot buckets
        if (idx >= 0) {
            int nextIdx = hotBidBitSet.previousSetBit(idx);
            // log.debug("B next {} for currentPrice={} lastPrice={}", next, currentPrice, lastPrice);
            if (nextIdx >= 0) {
                // found a bucket, but if limit is reached - no need to check far orders, just return null
                long nextPrice = nextIdx + basePrice;
                return (nextPrice >= lastPrice) ? hotBidBuckets.get(nextPrice) : null;
            }
        }

        // TODO independent searching can be slower comparing to processing a subtree (NLogN vs N) for superorders
        // nothing yet found and limit also not reached yet, therefore trying to search far buckets
        // note: bid far buckets tree order is reversed, so searching ceiling key like for asks
        Map.Entry<Long, IOrdersBucket> entry = farBidBuckets.ceilingEntry(currentPrice);
        return (entry != null && entry.getKey() >= lastPrice) ? entry.getValue() : null;
    }


    /**
     * Cancel an order.
     * h* <p>
     * orderId - order to cancel
     *
     * @return true if order removed, false if not found (can be removed/matched earlier)
     */
    @Override
    public boolean cancelOrder(OrderCommand cmd) {

        // can not remove because uid is not verified yet
        IOrdersBucket ordersBucket = idMapToBucket.get(cmd.orderId);
        if (ordersBucket == null) {
            // order already matched and removed from order book previously
            return false;
        }

        // remove order and whole bucket if bucket is empty
        Order removedOrder = ordersBucket.remove(cmd.orderId, cmd.uid);
        if (removedOrder == null) {
            // uid is different
            return false;
        }

        // remove from map
        idMapToBucket.remove(cmd.orderId);

        // remove bucket if cancelled order was the last one in the bucket
        if (ordersBucket.getTotalVolume() == 0) {
            removeBucket(removedOrder.action, ordersBucket.getPrice());
        }

        // send cancel event
        OrderBookEventsHelper.sendCancelEvent(cmd, removedOrder);

        // saving free object back to the pool
        ordersPool.addLast(removedOrder);

        return true;
    }


    /**
     * Move an order to different price
     * <p>
     * Normally requires 4 hash table lookup operations.
     * 1. Find bucket by orderId
     * (optional reduce, validate price)
     * 2. Find in remove order in the bucket (remove from internal queue and hash table)
     * (optional remove bucket)
     * (set new price and try match instantly)
     * 3. Find bucket for new price
     * 4. Insert order in the bucket (internal hash table and queue)
     * <p>
     * orderId  - order id
     * newPrice - new price
     *
     * @return - false if order not found (can be matched or removed), true otherwise
     */
    @Override
    public CommandResultCode moveOrder(OrderCommand cmd) {

        final long orderId = cmd.orderId;

        final IOrdersBucket bucket = idMapToBucket.get(orderId);
        if (bucket == null) {
            return CommandResultCode.MATCHING_UNKNOWN_ORDER_ID;
        }

        // take order out of the original bucket
        final Order order = bucket.remove(orderId, cmd.uid);
        if (order == null) {
            // uid is checked in the bucket.remove method
            return CommandResultCode.MATCHING_UNKNOWN_ORDER_ID;
        }

//        log.debug("{} {} {}>{}", symbolType, order.action, cmd.price, order.price2);

        // optimistic risk check mode for exchange bids
        if (symbolSpec.type == SymbolType.CURRENCY_EXCHANGE_PAIR && order.action == OrderAction.BID && cmd.price > order.reserveBidPrice) {
            // put order back (yes it will be in the end of queue)
            bucket.put(order);
            return CommandResultCode.MATCHING_MOVE_FAILED_PRICE_OVER_RISK_LIMIT;
        }

        // remove bucket if moved order was the last one in the bucket
        if (bucket.getTotalVolume() == 0) {
            removeBucket(order.action, bucket.getPrice());
        }

        final long newPrice = cmd.price;
        order.price = newPrice;

        // try match with new price
        long filled = tryMatchInstantly(order, order.filled, cmd);
        if (filled == order.size) {
            // order was fully matched (100% marketable) - removing from order book
            idMapToBucket.remove(orderId);
            // saving free object back to pool
            ordersPool.addLast(order);
        } else {
            order.filled = filled;

            // if not filled completely - put it into corresponding bucket
            final IOrdersBucket otherBucket = (order.action == OrderAction.ASK) ? getOrCreateNewBucketAck(newPrice) : getOrCreateNewBucketBid(newPrice);
            otherBucket.put(order);
            // override cache record
            idMapToBucket.put(orderId, otherBucket);
        }
        return CommandResultCode.SUCCESS;
    }


    /**
     * Remove bucket for specific action and price
     *
     * @param action
     * @param price
     */
    private void removeBucket(OrderAction action, long price) {
        if (action == OrderAction.ASK) {
            removeAskBucket(price);
        } else {
            removeBidBucket(price);
        }
    }

    private void removeAskBucket(long price) {
        int idx = priceToIndex(price);

        if (idx < hotPricesRange) {
            // in hot area
            hotAskBitSet.clear(idx);
            bucketsPool.addLast(hotAskBuckets.remove(price));
        } else {
            // in far area
            bucketsPool.addLast(farAskBuckets.remove(price));
        }

        if (minAskPrice != price) {
            // no need to update minAskPrice
            return;
        }

        if (idx >= hotPricesRange || updateMinAskPriceHot(idx)) {
            updateMinAskPriceFar(price);
        }
    }

    private boolean updateMinAskPriceHot(int idx) {
        int nextIdx = hotAskBitSet.nextSetBit(idx);
        if (nextIdx == -1) {
            // not found, have to also check far area
            return true;
        }

        // found new minAskPrice in hot bitset
        minAskPrice = nextIdx + basePrice;
        return false;
    }

    private void updateMinAskPriceFar(long price) {
        Long p = farAskBuckets.higherKey(price);
        minAskPrice = (p != null) ? p : Long.MAX_VALUE;
    }

    private void removeBidBucket(long price) {
        int idx = priceToIndex(price);

        if (idx >= 0) {
            // in hot area
            hotBidBitSet.clear(idx);
            bucketsPool.addLast(hotBidBuckets.remove(price));
        } else {
            // in far area
            bucketsPool.addLast(farBidBuckets.remove(price));
        }

        if (maxBidPrice != price) {
            // no need to update maxBidPrice
            return;
        }

        // need to update maxBidPrice
        // if makes sense - first check hot area
        if (idx < 0 || updateMaxBidPriceHot(idx)) {
            updateMaxBidPriceFar(price);
        }

    }

    private boolean updateMaxBidPriceHot(int idx) {
        int nextIdx = hotBidBitSet.previousSetBit(idx);
        if (nextIdx == -1) {
            // not found, have to also check far area
            return true;
        }

        // found new maxBidPrice in hot bitset
        maxBidPrice = nextIdx + basePrice;
        return false;
    }

    private void updateMaxBidPriceFar(long price) {
        Long p = farBidBuckets.higherKey(price); // higherKey() instead of lowerKey() - because of opposite sort order
        maxBidPrice = (p != null) ? p : 0;
    }

    /**
     * Re-balances HOT/FAR parts (bitsets and hashtables)
     * Triggered when BBO price moved significantly from the center price
     * <p>
     * price going up: asks FAR -> HOT, bids HOT -> FAR
     *
     * @param newBasePrice new base price
     */
    private void decreaseBasePrice(long newBasePrice) {
        int shift = (int) (basePrice - newBasePrice);
        //log.debug("decrease base price {}->{}", basePrice, newBasePrice);
        if (maxBidPrice != 0) {
            // shift hot bids bitset (base price lower -> index is higher for the same price)
            hotBidBitSet = shiftBitSetUp(hotBidBitSet, shift);

            // BID buckets from the FAR section need to be moved to the HOT section where price >= newBasePrice
            NavigableMap<Long, IOrdersBucket> toHotBuckets = farBidBuckets.headMap(newBasePrice, true);

            //log.debug("toHotBuckets={}", toHotBuckets);
            moveBucketsToHot(toHotBuckets, hotBidBuckets, hotBidBitSet, newBasePrice);
        }

        if (minAskPrice != Long.MAX_VALUE) {
            // evicting ASK buckets from the HOT section into the FAR section where price >= newBasePrice + hotPricesRange
            int next = Math.max(hotPricesRange - shift, 0);
            while ((next = hotAskBitSet.nextSetBit(next)) != -1) {
                IOrdersBucket bucket = hotAskBuckets.remove(indexToPrice(next));
                farAskBuckets.put(bucket.getPrice(), bucket);
                next++;
            }

            // shift up and clear asks BitSet
            hotAskBitSet = shiftBitSetUp(hotAskBitSet, shift);
        }

        setBasePrice(newBasePrice);
    }

    /**
     * Re-balances HOT/FAR parts (bitsets and hashtables)
     * Triggered when BBO price moved significantly from the center price
     * <p>
     * price going down: asks HOT -> FAR, bids FAR -> HOT
     *
     * @param newBasePrice new base price
     */
    private void increaseBasePrice(long newBasePrice) {
        int shift = (int) (newBasePrice - basePrice);
//        log.debug("increase base price {}->{}", basePrice, newBasePrice);
        if (minAskPrice != Long.MAX_VALUE) {
            // shift hot asks bitset
            hotAskBitSet = shiftBitSetDown(hotAskBitSet, shift);

            // moving ASK buckets from the FAR section to the HOT section where price < newBasePrice + hotPricesRange
            NavigableMap<Long, IOrdersBucket> toHotBuckets = farAskBuckets.headMap(newBasePrice + hotPricesRange, false);
            moveBucketsToHot(toHotBuckets, hotAskBuckets, hotAskBitSet, newBasePrice);
        }

        if (maxBidPrice != 0) {
            // evicting BID buckets from the HOT section into the FAR section where price < newBasePrice
            int next = priceToIndex(newBasePrice - 1); // newBasePrice is left in the hot (will have index 0), therefore -1
            while ((next = hotBidBitSet.previousSetBit(next)) != -1) {
                IOrdersBucket bucket = hotBidBuckets.remove(indexToPrice(next));
//                log.debug("Evicting bucket to FAR: {}", bucket.getPrice());
                farBidBuckets.put(bucket.getPrice(), bucket);
                next--;
            }

            // shift down and clear bids BitSet
            hotBidBitSet = shiftBitSetDown(hotBidBitSet, shift);
        }

        setBasePrice(newBasePrice);
    }

    // TODO slow - implement rolling bitset (extending BitSet)
    private BitSet shiftBitSetDown(BitSet bitSet, int shift) {
        int shiftLongs = shift >> 6;
        long[] src = bitSet.toLongArray();
        long[] dst = new long[hotPricesRange >> 6];
        int lengthCopy = src.length - shiftLongs;
        if (lengthCopy > 0) {
            System.arraycopy(src, shiftLongs, dst, 0, lengthCopy);
        }
        return BitSet.valueOf(dst);
    }

    private BitSet shiftBitSetUp(BitSet bitSet, int shift) {
        int shiftLongs = shift >> 6;
        long[] src = bitSet.toLongArray();
        long[] dst = new long[hotPricesRange >> 6];
        int lengthCopy = Math.min(src.length, dst.length - shiftLongs);
        if (lengthCopy > 0) {
            System.arraycopy(src, 0, dst, shiftLongs, lengthCopy);
        }
        return BitSet.valueOf(dst);
    }

    /**
     * Moves buckets from FAR subtree into HOT hashmap and update bitset according the new base price
     *
     * @param fromFar
     * @param toHot
     * @param newBitSet
     * @param newBasePrice
     */
    private void moveBucketsToHot(SortedMap<Long, IOrdersBucket> fromFar, LongObjectHashMap<IOrdersBucket> toHot, BitSet newBitSet, long newBasePrice) {
        Iterator<IOrdersBucket> iterator = fromFar.values().iterator();
        while (iterator.hasNext()) {
            IOrdersBucket next = iterator.next();
            iterator.remove();
            long price = next.getPrice();
            toHot.put(price, next);
            int idx = (int) (price - newBasePrice);
            //log.debug("move to hot:{}  idx-set:{}", price, idx);
            newBitSet.set(idx);
        }
    }

    /**
     * Get order from internal map
     * Testing only
     *
     * @param orderId -
     * @return - order
     */
    @Override
    public IOrder getOrderById(long orderId) {
        IOrdersBucket bucket = idMapToBucket.get(orderId);
        return (bucket != null) ? bucket.findOrder(orderId) : null;
    }

    @Override
    public void fillAsks(final int size, L2MarketData data) {
        if (minAskPrice == Long.MAX_VALUE || size == 0) {
            data.askSize = 0;
            return;
        }

        int i = 0;
        // scan hot section only if there are buckets in it
        if (minAskPrice < basePrice + hotPricesRange) {
            int next = priceToIndex(minAskPrice);
            while ((next = hotAskBitSet.nextSetBit(next)) != -1) {
                IOrdersBucket bucket = hotAskBuckets.get(indexToPrice(next));
                data.askPrices[i] = bucket.getPrice();
                data.askVolumes[i] = bucket.getTotalVolume();
                if (++i == size) {
                    data.askSize = size;
                    return;
                }
                next++;
            }
        }

        // extracting buckets from far trees
        for (IOrdersBucket bucket : farAskBuckets.values()) {
            data.askPrices[i] = bucket.getPrice();
            data.askVolumes[i] = bucket.getTotalVolume();
            if (++i == size) {
                data.askSize = size;
                return;
            }
        }

        // not filled completely
        data.askSize = i;
    }

    @Override
    public void fillBids(final int size, L2MarketData data) {

        if (maxBidPrice == 0 || size == 0) {
            data.bidSize = 0;
            return;
        }

        int i = 0;

        // scan hot section only if there are buckets in it
        if (maxBidPrice >= basePrice) {
            int next = priceToIndex(maxBidPrice);
            while ((next = hotBidBitSet.previousSetBit(next)) != -1) {
                IOrdersBucket bucket = hotBidBuckets.get(indexToPrice(next));
                data.bidPrices[i] = bucket.getPrice();
                data.bidVolumes[i] = bucket.getTotalVolume();
                if (++i == size) {
                    data.bidSize = size;
                    return;
                }
                next--;
            }
        }

        // extracting buckets from far trees
        // note: farBidBuckets is in reversed order
        for (IOrdersBucket bucket : farBidBuckets.values()) {
            data.bidPrices[i] = bucket.getPrice();
            data.bidVolumes[i] = bucket.getTotalVolume();
            if (++i == size) {
                data.bidSize = size;
                return;
            }
        }

        // not filled completely
        data.bidSize = i;
    }

    @Override
    public int getTotalAskBuckets() {
        return hotAskBuckets.size() + farAskBuckets.size();
    }

    @Override
    public int getTotalBidBuckets() {
        return hotBidBuckets.size() + farBidBuckets.size();
    }

    @Override
    public void validateInternalState() {

        // check price in the bucket is the same as map key
        hotAskBuckets.forEachKeyValue(this::checkBucketPriceIsTheSame);
        hotBidBuckets.forEachKeyValue(this::checkBucketPriceIsTheSame);
        farAskBuckets.forEach(this::checkBucketPriceIsTheSame);
        farBidBuckets.forEach(this::checkBucketPriceIsTheSame);

        // check there are not same orders in the hot and far areas
        Set<Long> ordersIdsBH = dumpAllOrdersIds(hotBidBuckets.values());
        Set<Long> ordersIdsBF = dumpAllOrdersIds(farBidBuckets.values());
        checkNoSameOrdersInHotAndFar(ordersIdsBH, ordersIdsBF);

        Set<Long> ordersIdsAH = dumpAllOrdersIds(hotAskBuckets.values());
        Set<Long> ordersIdsAF = dumpAllOrdersIds(farAskBuckets.values());
        checkNoSameOrdersInHotAndFar(ordersIdsAH, ordersIdsAF);

        // check that hot bit sets are matching to corresponding hot bucket keys
        if (!Arrays.equals(hotAskBitSet.stream().mapToLong(x -> x + basePrice).toArray(), hotAskBuckets.keySet().toSortedArray())) {
            throw new IllegalStateException("ASK HOT buckets bit set differs from map!");
        }
        if (!Arrays.equals(hotBidBitSet.stream().mapToLong(x -> x + basePrice).toArray(), hotBidBuckets.keySet().toSortedArray())) {
            throw new IllegalStateException("BID HOT buckets bit set differs from map!");
        }

        // check buckets within ranges
        if (!hotAskBuckets.isEmpty()) {
            if (hotAskBuckets.keySet().max() >= basePrice + hotPricesRange) {
                throw new IllegalStateException("Hot ask bucket price exceeds the hotPricesRange range!");
            }
            if (hotAskBuckets.keySet().min() != minAskPrice) {
                throw new IllegalStateException("incorrect minAskPrice! (expected inside hot area)");
            }
        }

        if (!hotBidBuckets.isEmpty()) {
            if (hotBidBuckets.keySet().min() < basePrice) {
                throw new IllegalStateException("Hot bid bucket price is below the basePrice range!");
            }
            if (hotBidBuckets.keySet().max() != maxBidPrice) {
                throw new IllegalStateException("incorrect maxBidPrice! (expected inside hot area)");
            }
        }

        if (!farAskBuckets.isEmpty()) {
            if (farAskBuckets.firstKey() < basePrice + hotPricesRange) {
                throw new IllegalStateException("Far ask bucket price is inside hot area!");
            }
            if (hotAskBuckets.isEmpty() && farAskBuckets.firstKey() != minAskPrice) {
                throw new IllegalStateException("incorrect minAskPrice (expected inside far area)!");
            }
        }

        if (!farBidBuckets.isEmpty()) {
            if (farBidBuckets.firstKey() >= basePrice) {
                throw new IllegalStateException("Far bid bucket price is inside hot area!");
            }
            if (hotBidBuckets.isEmpty() && farBidBuckets.firstKey() != maxBidPrice) {
                throw new IllegalStateException("incorrect maxBidPrice (expected inside far area)!");
            }
        }

        if (farAskBuckets.isEmpty() && hotAskBuckets.isEmpty() && minAskPrice != Long.MAX_VALUE) {
            throw new IllegalStateException("incorrect minAskPrice! (no buckets)");
        }
        if (farBidBuckets.isEmpty() && hotBidBuckets.isEmpty() && maxBidPrice != 0) {
            throw new IllegalStateException("incorrect maxBidPrice! (no buckets)");
        }

        // check known orders number is the same as total orders in all buckets TODO compare explicitly
        int ah = hotAskBuckets.stream().mapToInt(IOrdersBucket::getNumOrders).sum();
        int bh = hotBidBuckets.stream().mapToInt(IOrdersBucket::getNumOrders).sum();
        int af = farAskBuckets.values().stream().mapToInt(IOrdersBucket::getNumOrders).sum();
        int bf = farBidBuckets.values().stream().mapToInt(IOrdersBucket::getNumOrders).sum();
        if (idMapToBucket.size() != af + ah + bf + bh) {
//            log.debug("bh {}: {}", bh, dumpAllOrders(hotBidBuckets.values()));
//            log.debug("bf {}: {}", bf, dumpAllOrders(farBidBuckets.values()));
//            log.debug("ID {}: {}", idMapToBucket.size(), idMapToBucket.keySet());
            throw new IllegalStateException(String.format("AH:%d + AF:%d + BH:%d + BF:%d != knownOrders %d ", ah, af, bh, bf, idMapToBucket.size()));
        }

        // validateInternalState each bucket
        hotAskBuckets.stream().forEach(IOrdersBucket::validate);
        hotBidBuckets.stream().forEach(IOrdersBucket::validate);

        // TODO validateInternalState - orderid maps
    }

    @Override
    public OrderBookImplType getImplementationType() {
        return OrderBookImplType.FAST;
    }

    @Override
    public List<Order> findUserOrders(final long uid) {
        List<Order> list = new ArrayList<>();
        Consumer<IOrdersBucket> bucketConsumer = bucket -> bucket.forEachOrder(order -> {
            if (order.uid == uid) {
                list.add(order);
            }
        });
        hotAskBuckets.stream().forEach(bucketConsumer);
        hotBidBuckets.stream().forEach(bucketConsumer);
        farAskBuckets.values().forEach(bucketConsumer);
        farBidBuckets.values().forEach(bucketConsumer);
        return list;
    }

    private void checkNoSameOrdersInHotAndFar(Set<Long> hot, Set<Long> far) {
        Set<Long> intersection = new HashSet<>(hot);
        intersection.retainAll(far);
        if (!intersection.isEmpty()) {
            //log.debug("intersection: {}", intersection);
            throw new IllegalStateException("same order found in HOT and FAR sections!");
        }
    }

    private void checkBucketPriceIsTheSame(long p, IOrdersBucket b) {
        if (p != b.getPrice()) {
            throw new IllegalStateException(String.format("Bucket price %d not the same as map key %d", b.getPrice(), p));
        }
    }

    private String dumpAllOrders(Collection<IOrdersBucket> buckets) {
        return buckets.stream().map(bucket -> {
            String ordersInBucket = bucket.getAllOrders().stream().map(order -> String.valueOf(order.orderId)).collect(Collectors.joining());
            return String.format("[BUCKET %d: %s]", bucket.getPrice(), ordersInBucket);
        }).collect(Collectors.joining());
    }

    private Set<Long> dumpAllOrdersIds(Collection<IOrdersBucket> buckets) {
        List<Long> asList = buckets.stream()
                .flatMap(bucket -> bucket.getAllOrders().stream())
                .map(ord -> ord.orderId)
                .collect(Collectors.toList());
        Set<Long> asSet = new HashSet<>(asList);
        if (asSet.size() != asList.size()) {
            throw new IllegalStateException("Duplicate orders found!");
        }
        return asSet;
    }


    @Override
    public CoreSymbolSpecification getSymbolSpec() {
        return symbolSpec;
    }

    @Override
    public Stream<IOrder> askOrdersStream(final boolean sorted) {
        // TODO sorted version is slow
        final Stream<IOrdersBucket> hotStream = sorted ? hotAskBuckets.toSortedList().stream() : hotAskBuckets.stream();
        return Stream.concat(hotStream, farAskBuckets.values().stream())
                .flatMap(bucket -> bucket.getAllOrders().stream());
    }

    @Override
    public Stream<IOrder> bidOrdersStream(final boolean sorted) {
        // TODO sorted version is slow
        final Stream<IOrdersBucket> hotStream = sorted ? hotBidBuckets.toSortedList().reverseThis().stream() : hotBidBuckets.stream();
        return Stream.concat(hotStream, farBidBuckets.values().stream())
                .flatMap(bucket -> bucket.getAllOrders().stream());
    }


    // for testing only
    @Override
    public int getOrdersNum() {
        //validateInternalState();

        // TODO add trees
        int ah = hotAskBuckets.values().stream().mapToInt(IOrdersBucket::getNumOrders).sum();
        int bh = hotBidBuckets.values().stream().mapToInt(IOrdersBucket::getNumOrders).sum();
        int af = farAskBuckets.values().stream().mapToInt(IOrdersBucket::getNumOrders).sum();
        int bf = farBidBuckets.values().stream().mapToInt(IOrdersBucket::getNumOrders).sum();

//        log.debug("idMap:{} askOrders:{} bidOrders:{}", idMap.size(), askOrders, bidOrders);
        int knownOrders = idMapToBucket.size();

        assert knownOrders == ah + af + bh + bf : "inconsistent known orders";

        return idMapToBucket.size();
    }

    private IOrdersBucket[] getBidsAsArray() {
        final IOrdersBucket[] farBids = farBidBuckets.values().toArray(new IOrdersBucket[0]);
        final IOrdersBucket[] hotBids = hotBidBuckets.toSortedMap(k -> k, v -> v).values().toArray(new IOrdersBucket[hotBidBuckets.size()]);
        ArrayUtils.reverse(hotBids);
        return ObjectArrays.concat(hotBids, farBids, IOrdersBucket.class);
    }

    private IOrdersBucket[] getAsksAsArray() {
        final IOrdersBucket[] farAsks = farAskBuckets.values().toArray(new IOrdersBucket[0]);
        final IOrdersBucket[] hotAsks = hotAskBuckets.toSortedMap(k -> k, v -> v).values().toArray(new IOrdersBucket[hotAskBuckets.size()]);
        return ObjectArrays.concat(hotAsks, farAsks, IOrdersBucket.class);
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeByte(getImplementationType().getCode());
        symbolSpec.writeMarshallable(bytes);
        bytes.writeInt(hotPricesRange);

        SerializationUtils.marshallBitSet(hotAskBitSet, bytes);
        SerializationUtils.marshallBitSet(hotBidBitSet, bytes);

        SerializationUtils.marshallLongHashMap(hotAskBuckets, bytes);
        SerializationUtils.marshallLongHashMap(hotBidBuckets, bytes);

        bytes.writeLong(minAskPrice);
        bytes.writeLong(maxBidPrice);

        bytes.writeLong(basePrice);
        bytes.writeLong(rebalanceThresholdLow);
        bytes.writeLong(rebalanceThresholdHigh);

        SerializationUtils.marshallLongMap(farAskBuckets, bytes);
        SerializationUtils.marshallLongMap(farBidBuckets, bytes);
    }

}
