package org.openpredict.exchange.core;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.openpredict.exchange.beans.L2MarketData;
import org.openpredict.exchange.beans.Order;
import org.openpredict.exchange.beans.OrderAction;
import org.openpredict.exchange.beans.OrderType;
import org.openpredict.exchange.beans.cmd.OrderCommand;

import java.util.*;

import static org.openpredict.exchange.beans.OrderAction.ASK;
import static org.openpredict.exchange.beans.OrderAction.BID;

@Slf4j
@RequiredArgsConstructor
public class OrderBookFast extends OrderBookBase {

    public static final int HOT_PRICES_RANGE = 65535; // must be aligned by 64 bit, can not be lower than 1024

    private BitSet hotAskBitSet = new BitSet(HOT_PRICES_RANGE);
    private BitSet hotBidBitSet = new BitSet(HOT_PRICES_RANGE);
    private LongObjectHashMap<IOrdersBucket> hotAskBuckets = new LongObjectHashMap<>();
    private LongObjectHashMap<IOrdersBucket> hotBidBuckets = new LongObjectHashMap<>();
    private long minAskPrice = Long.MAX_VALUE;
    private long maxBidPrice = 0;

    /**
     * Bucket within FAR section: (price < basePrice) OR (price >= basePrice + HOT_PRICES_RANGE)
     * Bucket within HOT section: (price >= basePrice) AND (price < basePrice + HOT_PRICES_RANGE)
     */
    private long basePrice = -1;
    private long rebalanceThresholdLow = -1;
    private long rebalanceThresholdHigh = -1;

    private NavigableMap<Long, IOrdersBucket> farAskBuckets = new TreeMap<>();
    private NavigableMap<Long, IOrdersBucket> farBidBuckets = new TreeMap<>(Collections.reverseOrder());


    //    private LongObjectHashMap<Order> idMap = new LongObjectHashMap<>();
    /**
     * Hashtable for fast resolving OrderId -> Bucket
     */
    private LongObjectHashMap<IOrdersBucket> idMapToBucket = new LongObjectHashMap<>();

    /**
     * Object pools
     */
    private final ArrayDeque<Order> ordersPool = new ArrayDeque<>(65536);
    private final ArrayDeque<IOrdersBucket> bucketsPool = new ArrayDeque<>(65536);

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

    /**
     * Process new MARKET order
     * Such order matched to any existing LIMIT orders
     * Of there is not enough volume in order book - reject as partially filled
     *
     * @param order - market order to match
     */
    protected void matchMarketOrder(OrderCommand order) {
        long filledSize = tryMatchInstantly(order, 0);

        // rare case - partially filled due no liquidity - should report PARTIAL order execution
        if (filledSize < order.size) {
            sendRejectEvent(order, filledSize);
        }
    }


    /**
     * Place new LIMIT order
     * If order is marketable (there are matching limit orders) - match it first with existing liquidity
     *
     * @param cmd - limit order to place
     */
    @Override
    protected void placeNewLimitOrder(OrderCommand cmd) {

        long orderId = cmd.orderId;
        if (idMapToBucket.containsKey(orderId)) {
            throw new IllegalArgumentException("duplicate orderId: " + orderId);
        }

        if (basePrice == -1) {
            // first limit order will define a base price (middle of the HOT_PRICES_RANGE range)
            setBasePrice(calculateBasePrice(cmd.price));
        }

        // check if order is marketable there are matching orders
        long filled = tryMatchInstantly(cmd, 0);
        if (filled == cmd.size) {
            // fully matched as marketable before actually place - can just return
            return;
        }

        OrderAction action = cmd.action;
        long price = cmd.price;

        // normally placing regular limit order

        Order orderRecord = ordersPool.pollLast();
        if (orderRecord == null) {
            orderRecord = new Order();
        } else {
            // log.debug(" <<< from pool pool ex:{}->{} {}", orderRecord.orderId, orderId, orderRecord);
        }

        orderRecord.command = cmd.command;
        orderRecord.orderId = orderId;
        orderRecord.symbol = cmd.symbol;
        orderRecord.price = price;
        orderRecord.size = cmd.size;
        orderRecord.action = action;
        orderRecord.orderType = cmd.orderType;
        orderRecord.uid = cmd.uid;
        orderRecord.timestamp = cmd.timestamp;
        orderRecord.filled = filled;

//        log.debug(" New object: {}", orderRecord);

        IOrdersBucket bucket = action == ASK ? getOrCreateNewBucketAck(price) : getOrCreateNewBucketBid(price);
        bucket.add(orderRecord);

        idMapToBucket.put(orderId, bucket);

    }

    /**
     * Calculate base price so the given price would be in the center of the HOT_PRICES_RANGE range.<br/>
     * Will also do the 'long' alignment for faster bitset shift operations.
     *
     * @param price - central price
     * @return new base price
     */
    private long calculateBasePrice(long price) {
        long newBasePrice = (price - (HOT_PRICES_RANGE / 2) + 31) & (~63L);
        return Math.max(0, newBasePrice);
    }

    /**
     * Set new base price and update re-balance thresholds (1/4 and 3/4 of the HOT_PRICES_RANGE range)
     *
     * @param newBasePrice - new base price
     */
    private void setBasePrice(long newBasePrice) {
        basePrice = newBasePrice;
        rebalanceThresholdLow = newBasePrice + HOT_PRICES_RANGE / 4;
        rebalanceThresholdHigh = newBasePrice + HOT_PRICES_RANGE / 4 * 3;
    }

    private IOrdersBucket getOrCreateNewBucketAck(long price) {

        // if price is too low - time to re-balance
        if (price <= rebalanceThresholdLow) {
            // decreasing base price
            decreaseBasePrice(calculateBasePrice(price));
        }

        int idx = priceToIndex(price);
        boolean far = (idx >= HOT_PRICES_RANGE);
        IOrdersBucket ordersBucket = far ? farAskBuckets.get(price) : hotAskBuckets.get(price);

        if (ordersBucket != null) {
            // bucket exists
            return ordersBucket;
        }

        ordersBucket = bucketsPool.pollLast();
        if (ordersBucket == null) {
            ordersBucket = IOrdersBucket.newInstance();
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
        if (price >= rebalanceThresholdHigh) {
            // increasing base price
            increaseBasePrice(calculateBasePrice(price));
        }

        int idx = priceToIndex(price);
        boolean far = idx < 0;

        IOrdersBucket ordersBucket = far ? farBidBuckets.get(price) : hotBidBuckets.get(price);
        if (ordersBucket != null) {
            // bucket exists
            return ordersBucket;
        }

        ordersBucket = bucketsPool.pollLast();
        if (ordersBucket == null) {
            ordersBucket = IOrdersBucket.newInstance();
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

    private void throwPriceOutOfFastRangeException(long price) {
        throw new IllegalArgumentException(String.format("Price %d out of bounds [%d,%d)", price, basePrice, basePrice + HOT_PRICES_RANGE));
    }

    /**
     * Match the order instantly to specified sorted buckets map
     * Fully matching orders are removed from orderId index
     * Should any trades occur - they sent to tradesConsumer
     *
     * @param order - LIMIT or MARKET order to match
     * @return matched size (0 if nothing is matching to the order)
     */
    private long tryMatchInstantly(OrderCommand order, long filled) {
        OrderAction action = order.action;

//      log.info("-------- matchInstantly: {}", order);
//      log.info("filled {} to match {}", filled, order.size);

        long nextPrice;
        long limitPrice;
        if (action == BID) {
            if (minAskPrice == Long.MAX_VALUE) {
                // no orders to match
                return filled;
            }
            nextPrice = minAskPrice;
            limitPrice = (order.orderType == OrderType.LIMIT) ? order.price : Long.MAX_VALUE;
        } else {
            if (maxBidPrice == 0) {
                // no orders to match
                return filled;
            }
            nextPrice = maxBidPrice;
            limitPrice = (order.orderType == OrderType.LIMIT) ? order.price : 0;
        }

        long orderSize = order.size;

        while (filled < orderSize) {

            // search for next available bucket
            IOrdersBucket bucket = (action == BID) ? nextAvailableBucketAsk(nextPrice, limitPrice) : nextAvailableBucketBid(nextPrice, limitPrice);
            if (bucket == null) {
                break;
            }

            final long tradePrice = bucket.getPrice();
            // next iteration price
            nextPrice = (action == BID) ? tradePrice + 1 : tradePrice - 1;

            TradeEventCallback tradeEventCallback = (mOrder, v, fm, fma) -> {
                sendTradeEvent(order, mOrder, fm, fma, tradePrice, v);
                if (fm) {
                    // forget if fully matched
                    idMapToBucket.remove(mOrder.orderId);
                    // saving free object back to pool
                    ordersPool.addLast(mOrder);
                }
            };

            // matching orders within bucket
            long sizeLeft = orderSize - filled;
            filled += bucket.match(sizeLeft, order.uid, tradeEventCallback);

            // remove bucket if its empty
            if (bucket.getTotalVolume() == 0) {
                removeBucket(action.opposite(), tradePrice);
            }
        }
        return filled;
    }

    /**
     * Searches for next available bucket for matching starting from currentPrice inclusive and till lastPrice inclusive.
     *
     * @param currentPrice - price to start with
     * @param lastPrice    - limit price, can also be 0 or LONG_MAX for market orders.
     * @return bucket or null if not found
     */
    private IOrdersBucket nextAvailableBucketAsk(long currentPrice, long lastPrice) {
        int idx = priceToIndex(currentPrice);
        // normally searching within hot buckets
        if (idx < HOT_PRICES_RANGE) {
            int nextIdx = hotAskBitSet.nextSetBit(idx);
            // log.debug("A next {} for currentPrice={} lastPrice={}", next, currentPrice, lastPrice);
            if (nextIdx >= 0) {
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
     * @param lastPrice    - limit price, can also be 0 or INT_MAX for market orders.
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

        // send reduce event
        long reducedBy = removedOrder.size - removedOrder.filled;
        sendReduceEvent(removedOrder, reducedBy);

        // saving free object back to the pool
        ordersPool.addLast(removedOrder);

        return true;
    }


    /**
     * Reduce volume or/and move an order
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
     * newPrice - new price (0 - don't move the order)
     * newSize  - new size (0 - don't reduce size of the order)
     *
     * @return - false if order not found (can be matched or removed), true otherwise
     */
    @Override
    public boolean updateOrder(OrderCommand cmd) {

        long orderId = cmd.orderId;
        long newSize = cmd.size;
        long newPrice = cmd.price;

        IOrdersBucket bucket = idMapToBucket.get(orderId);
        if (bucket == null) {
            return false;
        }


        // if change volume operation - use bucket implementation
        // NOTE: not very efficient if moving and reducing volume
        if (newSize > 0) {
            if (!bucket.tryReduceSize(cmd, super::sendReduceEvent)) {
                return false;
            }
        }

        // return if there is no move operation (downsize only)
        if (newPrice <= 0 || newPrice == bucket.getPrice()) {
            return true;
        }

        // take order out of the original bucket
        Order order = bucket.remove(orderId, cmd.uid);
        if (order == null) {
            return false;
        }

        // remove bucket if moved order was the last one in the bucket
        if (bucket.getTotalVolume() == 0) {
            removeBucket(order.action, bucket.getPrice());
        }

        order.price = newPrice;
        //if ((action == BID && newPrice >= minAskPrice) || (action == ASK && newPrice <= maxBidPrice)) {
        // try match with new price
        long filled = tryMatchInstantly(order, order.filled);
        if (filled == order.size) {
            // order was fully matched (100% marketable) - removing from order book
            idMapToBucket.remove(orderId);
            // saving free object back to pool
            ordersPool.addLast(order);
            return true;
        }
        order.filled = filled;

        // if not filled completely - put it into corresponding bucket
        bucket = (order.action == ASK) ? getOrCreateNewBucketAck(newPrice) : getOrCreateNewBucketBid(newPrice);
        bucket.add(order);
        idMapToBucket.put(orderId, bucket);
        return true;
    }


    private void removeBucket(OrderAction action, long price) {
        if (action == ASK) {
            removeAskBucket(price);
        } else {
            removeBidBucket(price);
        }
    }

    private void removeAskBucket(long price) {
        int idx = priceToIndex(price);

        if (idx < HOT_PRICES_RANGE) {
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

        if (idx >= HOT_PRICES_RANGE || updateMinAskPriceHot(idx)) {
            updateMinAskPriceFar(price);
        }
    }

    private boolean updateMinAskPriceHot(int idx) {
        int nextIdx = hotAskBitSet.nextSetBit(idx);
        if (nextIdx < 0) {
            return false;
        }

        // found new minAskPrice in hot bitset
        minAskPrice = nextIdx + basePrice;
        return true;
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

        if (maxBidPrice == price) {
            // need to update maxBidPrice
            // if makes sence - first check hot area
            if (idx < 0 || !updateMaxBidPriceHot(idx)) {
                updateMaxBidPriceFar(price);
            }
        }
    }

    private boolean updateMaxBidPriceHot(int idx) {
        int nextIdx = hotBidBitSet.previousSetBit(idx);
        if (nextIdx < 0) {
            return false;
        }

        // found new maxBidPrice in hot bitset
        maxBidPrice = nextIdx + basePrice;
        return true;
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

        if (maxBidPrice != 0) {
            // shift hot bids bitset (base price lower -> index is higher for the same price)
            hotBidBitSet = shiftBitSetUp(hotBidBitSet, shift);

            // BID buckets from the FAR section need to be moved to the HOT section where price >= newBasePrice
            NavigableMap<Long, IOrdersBucket> toHotBuckets = farBidBuckets.headMap(newBasePrice, true);
            moveBucketsToHot(toHotBuckets, hotBidBuckets, hotBidBitSet, newBasePrice);
        }

        if (minAskPrice != Long.MAX_VALUE) {
            // evicting ASK buckets from the HOT section into the FAR section where price >= newBasePrice + HOT_PRICES_RANGE
            int next = priceToIndex(newBasePrice - basePrice + HOT_PRICES_RANGE);
            while ((next = hotAskBitSet.nextSetBit(next)) >= 0) {
                IOrdersBucket bucket = hotAskBuckets.get(indexToPrice(next));
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

        if (minAskPrice != Long.MAX_VALUE) {
            // shift hot asks bitset
            hotAskBitSet = shiftBitSetDown(hotAskBitSet, shift);

            // moving ASK buckets from the FAR section to the HOT section where price < newBasePrice + HOT_PRICES_RANGE
            NavigableMap<Long, IOrdersBucket> toHotBuckets = farAskBuckets.headMap(newBasePrice + HOT_PRICES_RANGE, false);
            moveBucketsToHot(toHotBuckets, hotAskBuckets, hotAskBitSet, newBasePrice);
        }

        if (maxBidPrice != 0) {
            // evicting BID buckets from the HOT section into the FAR section where price < newBasePrice
            int next = priceToIndex(newBasePrice - basePrice - 1);
            while ((next = hotBidBitSet.previousSetBit(next)) >= 0) {
                IOrdersBucket bucket = hotBidBuckets.get(indexToPrice(next));
                farBidBuckets.put(bucket.getPrice(), bucket);
                next--;
            }

            // shift down and clear bids BitSet
            hotBidBitSet = shiftBitSetDown(hotBidBitSet, shift);
        }

        setBasePrice(newBasePrice);
    }

    // TODO slow - implement rolling bitset
    private BitSet shiftBitSetDown(BitSet bitSet, int shift) {
        int shiftLongs = shift >> 6;
        long[] src = bitSet.toLongArray();
        long[] dst = new long[src.length];
        System.arraycopy(src, shiftLongs, dst, 0, src.length - shiftLongs);
        return BitSet.valueOf(dst);
    }

    private BitSet shiftBitSetUp(BitSet bitSet, int shift) {
        int shiftLongs = shift >> 6;
        long[] src = bitSet.toLongArray();
        long[] dst = new long[src.length];
        System.arraycopy(src, 0, dst, shiftLongs, src.length - shiftLongs);
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
            newBitSet.set((int) (price - newBasePrice));
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
    public Order getOrderById(long orderId) {
        IOrdersBucket bucket = idMapToBucket.get(orderId);
        return (bucket != null) ? bucket.findOrder(orderId) : null;
    }

    @Override
    protected void fillAsks(final int size, L2MarketData data) {
        if (minAskPrice == Long.MAX_VALUE || size == 0) {
            data.askSize = 0;
            return;
        }

        int next = priceToIndex(minAskPrice);
        int i = 0;
        while ((next = hotAskBitSet.nextSetBit(next)) >= 0) {
            IOrdersBucket bucket = hotAskBuckets.get(indexToPrice(next));
            data.askPrices[i] = bucket.getPrice();
            data.askVolumes[i] = bucket.getTotalVolume();
            if (++i == size) {
                data.askSize = size;
                return;
            }
            next++;
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
    protected void fillBids(final int size, L2MarketData data) {

        if (maxBidPrice == 0 || size == 0) {
            data.bidSize = 0;
            return;
        }


        int next = priceToIndex(maxBidPrice);
        int i = 0;
        while ((next = hotBidBitSet.previousSetBit(next)) >= 0) {
            IOrdersBucket bucket = hotBidBuckets.get(indexToPrice(next));
            data.bidPrices[i] = bucket.getPrice();
            data.bidVolumes[i] = bucket.getTotalVolume();
            if (++i == size) {
                data.bidSize = size;
                return;
            }
            next--;
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
    protected int getTotalAskBuckets() {
        return hotAskBuckets.size() + farAskBuckets.size();
    }

    @Override
    protected int getTotalBidBuckets() {
        return hotBidBuckets.size() + farBidBuckets.size();
    }

    @Override
    public void validateInternalState() {
        // validateInternalState each bucket
        hotAskBuckets.stream().forEach(IOrdersBucket::validate);
        hotBidBuckets.stream().forEach(IOrdersBucket::validate);
        // TODO validateInternalState bitset, BBO, orderid maps
    }

    // for testing only
    @Override
    public void clear() {
        hotAskBuckets.clear();
        hotBidBuckets.clear();
        hotAskBitSet.clear();
        hotBidBitSet.clear();
        farAskBuckets.clear();
        farBidBuckets.clear();
        minAskPrice = Long.MAX_VALUE;
        maxBidPrice = 0;
        idMapToBucket.clear();
//        ordersPool.clear(); // ?
    }


    // for testing only
    @Override
    public int getOrdersNum() {

        //validateInternalState();

        // TODO add trees
        int askOrders = hotAskBuckets.values().stream().mapToInt(IOrdersBucket::getNumOrders).sum();
        int bidOrders = hotBidBuckets.values().stream().mapToInt(IOrdersBucket::getNumOrders).sum();

//        log.debug("idMap:{} askOrders:{} bidOrders:{}", idMap.size(), askOrders, bidOrders);
        int knownOrders = idMapToBucket.size();

        assert knownOrders == askOrders + bidOrders : "inconsistent known orders";

        return idMapToBucket.size();
    }

}
