package org.openpredict.exchange.core;

import com.lmax.disruptor.EventSink;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.openpredict.exchange.beans.L2MarketData;
import org.openpredict.exchange.beans.Order;
import org.openpredict.exchange.beans.OrderAction;
import org.openpredict.exchange.beans.cmd.OrderCommand;

import java.util.*;

@Slf4j
@RequiredArgsConstructor
public class OrderBookSlow extends OrderBookBase {

    private NavigableMap<Integer, IOrdersBucket> askBuckets = new TreeMap<>();
    private NavigableMap<Integer, IOrdersBucket> bidBuckets = new TreeMap<>(Collections.reverseOrder());

//    private Long2ObjectAVLTreeMap<IOrdersBucket> askBuckets = new Long2ObjectAVLTreeMap<>();
//    private Long2ObjectAVLTreeMap<IOrdersBucket> bidBuckets = new Long2ObjectAVLTreeMap<>(Collections.reverseOrder());

    private LongObjectHashMap<Order> idMap = new LongObjectHashMap<>();
//    private MutableLongObjectMap<Order> idMap = new LongObjectHashMap<>();

    private final EventSink<L2MarketData> marketDataBuffer;

    private final MatcherEventsCallback eventsCallback;


    protected void matchMarketOrder(OrderCommand order) {
        long filledSize = tryMatchInstantly(order, order.action == OrderAction.ASK ? bidBuckets : askBuckets, 0);

        // rare case - partially filled due no liquidity - should report PARTIAL order execution
        if (filledSize < order.size) {
            eventsCallback.sendRejectEvent(order, filledSize);
        }
    }

    protected void placeNewLimitOrder(OrderCommand cmd) {

        if (idMap.containsKey(cmd.orderId)) {
            throw new IllegalArgumentException("duplicate orderId: " + cmd.orderId);
        }

        // check if order is arketable there are matching orders
        long filled = tryMatchInstantly(cmd, subtreeForMatching(cmd.action, cmd.price), 0);
        if (filled == cmd.size) {
            // fully matched as marketable before actually place - can just return
            return;
        }

        // normally placing regular limit order
        Order orderRecord = new Order(
                cmd.command,
                cmd.orderId,
                cmd.symbol,
                cmd.price,
                cmd.size,
                cmd.action,
                cmd.orderType,
                cmd.uid,
                cmd.timestamp,
                filled);

        IOrdersBucket bucket = getBucketsByAction(cmd.action)
                .computeIfAbsent(cmd.price, price -> {
                    IOrdersBucket b = IOrdersBucket.newInstance();
                    b.setPrice(price);
                    return b;
                });
        bucket.add(orderRecord);

        idMap.put(cmd.orderId, orderRecord);
    }

    private SortedMap<Integer, IOrdersBucket> subtreeForMatching(OrderAction action, int price) {
        return (action == OrderAction.ASK ? bidBuckets : askBuckets)
                .headMap(price, true);
    }

    /**
     * Match the order instantly to specified sorted buckets map
     * Fully matching orders are removed from orderId index
     * Should any trades occur - they sent to tradesConsumer
     *
     * @param order           - LIMIT or MARKET order to match
     * @param matchingBuckets - sorted buckets map
     * @return matched size (0 if nothing matched)
     */
    private long tryMatchInstantly(OrderCommand order, SortedMap<Integer, IOrdersBucket> matchingBuckets, long filled) {

//        log.info("matchInstantly: {} {}", order, matchingBuckets);

        if (matchingBuckets.size() == 0) {
            return 0;
        }

        long orderSize = order.size;

        List<Integer> emptyBuckets = new ArrayList<>();
        for (IOrdersBucket bucket : matchingBuckets.values()) {

//            log.debug("Matching bucket: {} ...", bucket);
//            log.debug("... with order: {}", order);

            //OrderMatchingResult matchingOrders = bucket.match(order.size - filled, order.uid);

            int tradePrice = bucket.getPrice();

            filled += bucket.match(orderSize - filled, order.uid,
                    (mOrder, v, fm, fma) -> {
                        eventsCallback.sendTradeEvent(order, mOrder, fm, fma, tradePrice, v);
                        if (fm) {
                            idMap.remove(mOrder.orderId);
                        }
                    });

//            log.debug("Matching orders: {}", matchingOrders);
//            log.debug("order.filled: {}", order.filled);

            int price = bucket.getPrice();

            // remove empty buckets
            if (bucket.getTotalVolume() == 0) {
                emptyBuckets.add(price);
            }

            if (filled == order.size) {
                // enough matched
                break;
            }
        }

        // remove empty buckets (is it necessary?)
        emptyBuckets.forEach(matchingBuckets::remove);

//        log.debug("emptyBuckets: {}", emptyBuckets);
//        log.debug("matchingRecords: {}", matchingRecords);

        return filled;
    }


    /**
     * Remove an order
     * <p>
     * orderId - order to remove
     *
     * @return true if order removed, false if not found (can be removed/matched earlier)
     */
    public boolean cancelOrder(OrderCommand cmd) {
        long orderId = cmd.orderId;

        Order order = idMap.get(orderId);
        if (order == null || order.uid != cmd.uid) {
            // order already matched and removed from orderbook previously
            return false;
        }

        // now can remove it
        idMap.remove(orderId);

        NavigableMap<Integer, IOrdersBucket> buckets = getBucketsByAction(order.action);
        int price = order.price;
        IOrdersBucket ordersBucket = buckets.get(price);
        if (ordersBucket == null) {
            // not possible state
            throw new IllegalStateException("Can not find bucket for order price=" + price + " for order " + order);
        }

        // remove order and whole bucket if its empty
        ordersBucket.remove(orderId, cmd.uid);
        if (ordersBucket.getTotalVolume() == 0) {
            buckets.remove(price);
        }

        eventsCallback.sendReduceEvent(order, order.size - order.filled);

        return true;
    }


    /**
     * Reduce volume or/and move an order
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

        Order order = idMap.get(orderId);
        if (order == null) {
            // already matched, moved or cancelled
            return false;
        }

        if (order.uid != cmd.uid) {
            return false;
        }

        int price = order.price;
        NavigableMap<Integer, IOrdersBucket> buckets = getBucketsByAction(order.action);
        IOrdersBucket ordersBucket = buckets.get(price);

        // if change volume operation - use bucket implementation
        // not very efficient if moving and reducing volume
        if (newSize > 0) {
            if (!ordersBucket.tryReduceSize(cmd, eventsCallback::sendReduceEvent)) {
                return false;
            }
        }

        // return if there is no move operation (downsize only)
        if (newPrice <= 0 || newPrice == price) {
            return true;
        }

        // take order out of the original bucket and clean bucket if its empty
        if (ordersBucket.remove(orderId, cmd.uid) == null) {
            return false;
        }

        if (ordersBucket.getTotalVolume() == 0) {
            buckets.remove(price);
        }

        order.price = newPrice;

        // try match with new price
        SortedMap<Integer, IOrdersBucket> matchingArea = subtreeForMatching(order.action, newPrice);
        long filled = tryMatchInstantly(order, matchingArea, order.filled);
        if (filled == order.size) {
            // order was fully matched (100% marketable) - removing from order book
            idMap.remove(orderId);
            return true;
        }
        order.filled = filled;

        // if not filled completely - put it into corresponding bucket
        ordersBucket = buckets.computeIfAbsent(newPrice, p -> {
            IOrdersBucket b = IOrdersBucket.newInstance();
            b.setPrice(p);
            return b;
        });
        ordersBucket.add(order);

        return true;
    }

    /**
     * Get bucket by order action
     *
     * @param action - action
     * @return bucket - navigable map
     */
    private NavigableMap<Integer, IOrdersBucket> getBucketsByAction(OrderAction action) {
        return action == OrderAction.ASK ? askBuckets : bidBuckets;
    }


    /**
     * Get order from internal map
     *
     * @param orderId -
     * @return order
     */
    public Order getOrderById(long orderId) {
        return idMap.get(orderId);
    }

    public L2MarketData getL2MarketDataSnapshot(int size) {
        int askPrices[] = askBuckets.values().stream().limit(size).mapToInt(IOrdersBucket::getPrice).toArray();
        long askVolumes[] = askBuckets.values().stream().limit(size).mapToLong(IOrdersBucket::getTotalVolume).toArray();
        int bidPrices[] = bidBuckets.values().stream().limit(size).mapToInt(IOrdersBucket::getPrice).toArray();
        long bidVolumes[] = bidBuckets.values().stream().limit(size).mapToLong(IOrdersBucket::getTotalVolume).toArray();
        L2MarketData l2MarketData = new L2MarketData(askPrices, askVolumes, bidPrices, bidVolumes);
        l2MarketData.totalVolumeAsk = Arrays.stream(askVolumes).sum();
        l2MarketData.totalVolumeBid = Arrays.stream(bidVolumes).sum();

        return l2MarketData;
    }

    @Override
    public void publishL2MarketDataSnapshot(int size) {
        marketDataBuffer.publishEvent((rec, seq) -> {
            int i = 0;
            for (IOrdersBucket bucket : askBuckets.values()) {
                rec.askPrices[i] = bucket.getPrice();
                rec.askVolumes[i] = bucket.getTotalVolume();
                if (++i == size) {
                    break;
                }
            }
            rec.askSize = i;

            i = 0;
            for (IOrdersBucket bucket : bidBuckets.values()) {
                rec.bidPrices[i] = bucket.getPrice();
                rec.bidVolumes[i] = bucket.getTotalVolume();
                if (i++ == size) {
                    break;
                }
            }
            rec.bidSize = i;
        });
    }

    @Override
    public void validateInternalState() {
        askBuckets.values().forEach(IOrdersBucket::validate);
        bidBuckets.values().forEach(IOrdersBucket::validate);
    }

    // for testing only
    public void clear() {
        askBuckets.clear();
        bidBuckets.clear();
        idMap.clear();
    }

    // for testing only
    public int getOrdersNum() {

        int askOrders = askBuckets.values().stream().mapToInt(IOrdersBucket::getNumOrders).sum();
        int bidOrders = bidBuckets.values().stream().mapToInt(IOrdersBucket::getNumOrders).sum();
        //log.debug("idMap:{} askOrders:{} bidOrders:{}", idMap.size(), askOrders, bidOrders);
        int knownOrders = idMap.size();

        assert knownOrders == askOrders + bidOrders : "inconsistent known orders";

        return knownOrders;
    }

}
