package org.openpredict.exchange.core;

import com.lmax.disruptor.EventSink;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.openpredict.exchange.beans.L2MarketData;
import org.openpredict.exchange.beans.Order;
import org.openpredict.exchange.beans.OrderAction;
import org.openpredict.exchange.beans.OrderType;
import org.openpredict.exchange.beans.cmd.OrderCommand;

import java.util.ArrayDeque;
import java.util.BitSet;

import static org.openpredict.exchange.beans.OrderAction.ASK;
import static org.openpredict.exchange.beans.OrderAction.BID;

@Slf4j
@RequiredArgsConstructor
public class OrderBookFast extends OrderBookBase {

    // TODO add far prices support

    private BitSet askBitSet = new BitSet();
    private BitSet bidBitSet = new BitSet();
    private MutableIntObjectMap<IOrdersBucket> askBuckets = new IntObjectHashMap<>();
    private MutableIntObjectMap<IOrdersBucket> bidBuckets = new IntObjectHashMap<>();
    private int minAskPrice = Integer.MAX_VALUE;
    private int maxBidPrice = 0;

    // private long baseCenterPrice = 1000;

    //    private LongObjectHashMap<Order> idMap = new LongObjectHashMap<>();
    private LongObjectHashMap<IOrdersBucket> idMapToBucket = new LongObjectHashMap<>();

    private final EventSink<L2MarketData> marketDataBuffer;

    private final ArrayDeque<Order> ordersPool = new ArrayDeque<>(65536);
    private final ArrayDeque<IOrdersBucket> bucketsPool = new ArrayDeque<>(65536);


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
    protected void placeNewLimitOrder(OrderCommand cmd) {

        long orderId = cmd.orderId;
        if (idMapToBucket.containsKey(orderId)) {
            throw new IllegalArgumentException("duplicate orderId: " + orderId);
        }

        // check if order is marketable there are matching orders
        long filled = tryMatchInstantly(cmd, 0);
        if (filled == cmd.size) {
            // fully matched as marketable before actually place - can just return
            return;
        }

        OrderAction action = cmd.action;
        int price = cmd.price;

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

    private IOrdersBucket getOrCreateNewBucketAck(int price) {
        IOrdersBucket ordersBucket = askBuckets.get(price);
        if (ordersBucket != null) {
            return ordersBucket;
        }

        ordersBucket = bucketsPool.pollLast();
        if (ordersBucket == null) {
            ordersBucket = IOrdersBucket.newInstance();
        }

        ordersBucket.setPrice(price);

        askBuckets.put(price, ordersBucket);
        if (minAskPrice > price) {
            minAskPrice = price;
        }
        askBitSet.set(price);
        return ordersBucket;
    }


    private IOrdersBucket getOrCreateNewBucketBid(int price) {
        IOrdersBucket ordersBucket = bidBuckets.get(price);
        if (ordersBucket != null) {
            return ordersBucket;
        }

        ordersBucket = bucketsPool.pollLast();
        if (ordersBucket == null) {
            ordersBucket = IOrdersBucket.newInstance();
        }

        ordersBucket.setPrice(price);

        bidBuckets.put(price, ordersBucket);
        if (maxBidPrice < price) {
            maxBidPrice = price;
        }
        bidBitSet.set(price);
        return ordersBucket;
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
//        log.info("-------- matchInstantly: {}", order);
//        log.info("filled {} to match {}", filled, order.size);

        int nextPrice;
        int limitPrice;
        if (action == BID) {
            if (minAskPrice == Integer.MAX_VALUE) {
                return filled;
            }
            nextPrice = minAskPrice;
            limitPrice = (order.orderType == OrderType.LIMIT) ? order.price : Integer.MAX_VALUE;
        } else {
            if (maxBidPrice == 0) {
                return filled;
            }
            nextPrice = maxBidPrice;
            limitPrice = (order.orderType == OrderType.LIMIT) ? order.price : 0;
        }

        long orderSize = order.size;

        while (filled < orderSize) {
            IOrdersBucket bucket = nextBucket(action, nextPrice, limitPrice);
            if (bucket == null) {
                break;
            }

            final int tradePrice = bucket.getPrice();

//            log.debug("Price: {}, matching bucket: {} ...", tradePrice, bucket);
//            log.debug("... with order: {}", order);

            TradeEventCallback tradeEventCallback = (mOrder, v, fm, fma) -> {
                sendTradeEvent(order, mOrder, fm, fma, tradePrice, v);
                if (fm) {
                    // forget if fully matched
                    idMapToBucket.remove(mOrder.orderId);
                    // saving free object back to pool
                    ordersPool.addLast(mOrder);
//                            log.debug(" >>> back to pool {}", mOrder.orderId);
                }
            };

            filled += bucket.match(orderSize - filled, order.uid, tradeEventCallback);

            //log.debug("Matching orders: {}", matchingOrders);
//            log.debug("order.filled: {}", filled);

            // remove empty buckets
            if (bucket.getTotalVolume() == 0) {
                removeBucket(tradePrice, action.opposite());
            }

            nextPrice = (action == BID) ? tradePrice + 1 : tradePrice - 1;
        }

        return filled;
    }

    private IOrdersBucket nextBucket(OrderAction action, int currentPrice, int lastPrice) {
        if (action == BID) {
            int next = askBitSet.nextSetBit(currentPrice);
//            log.debug("A next {} for currentPrice={} lastPrice={}", next, currentPrice, lastPrice);
            return (next >= 0 && next <= lastPrice) ? askBuckets.get(next) : null;
        } else {
            int next = bidBitSet.previousSetBit(currentPrice);
//            log.debug("B next {} for currentPrice={} lastPrice={}", next, currentPrice, lastPrice);
            return (next >= 0 && next >= lastPrice) ? bidBuckets.get(next) : null;
        }
    }


    /**
     * Cancel an order.
     * <p>
     * orderId - order to cancel
     *
     * @return true if order removed, false if not found (can be removed/matched earlier)
     */
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

        if (ordersBucket.getTotalVolume() == 0) {
            removeBucket(ordersBucket.getPrice(), removedOrder.action);
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
        int newPrice = cmd.price;

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

        // remove bucket if its empty
        if (bucket.getTotalVolume() == 0) {
            removeBucket(bucket.getPrice(), order.action);
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
        bucket = (order.action == ASK)
                ? getOrCreateNewBucketAck(newPrice)
                : getOrCreateNewBucketBid(newPrice);
        bucket.add(order);
        idMapToBucket.put(orderId, bucket);

        return true;
    }


    private void removeBucket(int price, OrderAction action) {

        if (action == ASK) {
            askBitSet.clear(price);
            bucketsPool.addLast(askBuckets.remove(price));
            if (minAskPrice == price) {
                minAskPrice = askBitSet.nextSetBit(minAskPrice);
                if (minAskPrice == -1) {
                    minAskPrice = Integer.MAX_VALUE;
                }
            }
        } else {
            bidBitSet.clear(price);
            bucketsPool.addLast(bidBuckets.remove(price));
            if (maxBidPrice == price) {
                maxBidPrice = bidBitSet.previousSetBit(maxBidPrice);
                if (maxBidPrice == -1) {
                    maxBidPrice = 0;
                }
            }
        }
    }

    /**
     * Get bucket by order action
     *
     * @param action - action
     * @return bucket - navigable map
     */
    private MutableIntObjectMap<IOrdersBucket> getBucketsByAction(OrderAction action) {
        return action == ASK ? askBuckets : bidBuckets;
    }


    /**
     * Get order from internal map
     * Testing only
     *
     * @param orderId -
     * @return - order
     */
    public Order getOrderById(long orderId) {
        IOrdersBucket bucket = idMapToBucket.get(orderId);
        return (bucket != null) ? bucket.findOrder(orderId) : null;
    }

    public L2MarketData getL2MarketDataSnapshot(int size) {
        int sizeAsk = Math.min(size, askBuckets.size());
        int askPrices[] = new int[sizeAsk];
        long askVolumes[] = new long[sizeAsk];
        long totalVolumeAsk = 0;
        if (sizeAsk > 0) {
            int nextPrice = minAskPrice;
            for (int i = 0; i < sizeAsk; i++) {
                int next = askBitSet.nextSetBit(nextPrice);
                if (next < 0) {
                    break;
                }
                IOrdersBucket bucket = askBuckets.get(next);
                nextPrice = bucket.getPrice() + 1;
                askPrices[i] = bucket.getPrice();
                askVolumes[i] = bucket.getTotalVolume();
                totalVolumeAsk += bucket.getTotalVolume();
            }
        }

        int sizeBid = Math.min(size, bidBuckets.size());
        int bidPrices[] = new int[sizeBid];
        long bidVolumes[] = new long[sizeBid];
        long totalVolumeBid = 0;
        if (sizeBid > 0) {
            int nextPrice = maxBidPrice;
            for (int i = 0; i < sizeBid; i++) {
                int next = bidBitSet.previousSetBit(nextPrice);
                if (next < 0) {
                    break;
                }
                IOrdersBucket bucket = bidBuckets.get(next);
                nextPrice = bucket.getPrice() - 1;
                bidPrices[i] = bucket.getPrice();
                bidVolumes[i] = bucket.getTotalVolume();
                totalVolumeBid += bucket.getTotalVolume();
            }
        }
        L2MarketData l2MarketData = new L2MarketData(askPrices, askVolumes, bidPrices, bidVolumes);
        l2MarketData.totalVolumeAsk = totalVolumeAsk;
        l2MarketData.totalVolumeBid = totalVolumeBid;
        return l2MarketData;
    }

    @Override
    public void publishL2MarketDataSnapshot(int size) {
        throw new IllegalStateException();
    }

    @Override
    public void validateInternalState() {
        // validateInternalState each bucket
        askBuckets.stream().forEach(IOrdersBucket::validate);
        bidBuckets.stream().forEach(IOrdersBucket::validate);
        // TODO validateInternalState bitset, BBO, orderid maps
    }

    // for testing only
    public void clear() {
        askBuckets.clear();
        bidBuckets.clear();
        askBitSet.clear();
        bidBitSet.clear();
        minAskPrice = Integer.MAX_VALUE;
        maxBidPrice = 0;
        idMapToBucket.clear();
//        ordersPool.clear(); // ?
    }


    // for testing only
    public int getOrdersNum() {

        //validateInternalState();

        int askOrders = askBuckets.values().stream().mapToInt(IOrdersBucket::getNumOrders).sum();
        int bidOrders = bidBuckets.values().stream().mapToInt(IOrdersBucket::getNumOrders).sum();

//        log.debug("idMap:{} askOrders:{} bidOrders:{}", idMap.size(), askOrders, bidOrders);
        int knownOrders = idMapToBucket.size();

        assert knownOrders == askOrders + bidOrders : "inconsistent known orders";

        return idMapToBucket.size();
    }

}
