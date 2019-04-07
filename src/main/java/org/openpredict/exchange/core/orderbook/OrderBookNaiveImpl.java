package org.openpredict.exchange.core.orderbook;

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
public final class OrderBookNaiveImpl extends OrderBookBase {

    private NavigableMap<Long, IOrdersBucket> askBuckets = new TreeMap<>();
    private NavigableMap<Long, IOrdersBucket> bidBuckets = new TreeMap<>(Collections.reverseOrder());

//    private Long2ObjectAVLTreeMap<IOrdersBucket> askBuckets = new Long2ObjectAVLTreeMap<>();
//    private Long2ObjectAVLTreeMap<IOrdersBucket> bidBuckets = new Long2ObjectAVLTreeMap<>(Collections.reverseOrder());

    private LongObjectHashMap<Order> idMap = new LongObjectHashMap<>();
//    private MutableLongObjectMap<Order> idMap = new LongObjectHashMap<>();


    protected void matchMarketOrder(OrderCommand order) {
        long filledSize = tryMatchInstantly(order, order.action == OrderAction.ASK ? bidBuckets : askBuckets, 0);

        // rare case - partially filled due no liquidity - should report PARTIAL order execution
        if (filledSize < order.size) {
            sendRejectEvent(order, filledSize);
        }
    }

    @Override
    protected void placeNewLimitOrder(OrderCommand cmd) {

        if (idMap.containsKey(cmd.orderId)) {
            throw new IllegalArgumentException("duplicate orderId: " + cmd.orderId);
        }

        // check if order is marketable (if there are opposite matching orders)
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
                cmd.userCookie,
                filled);

        IOrdersBucket bucket = getBucketsByAction(cmd.action)
                .computeIfAbsent(cmd.price, price -> {
                    IOrdersBucket b = new OrdersBucketNaiveImpl();
                    b.setPrice(price);
                    return b;
                });
        bucket.add(orderRecord);

        idMap.put(cmd.orderId, orderRecord);
    }

    private SortedMap<Long, IOrdersBucket> subtreeForMatching(OrderAction action, long price) {
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
     * @param filled          - current 'filled' value for the order
     * @return new filled size
     */
    private long tryMatchInstantly(final OrderCommand order, final SortedMap<Long, IOrdersBucket> matchingBuckets, long filled) {

//        log.info("matchInstantly: {} {}", order, matchingBuckets);

        if (matchingBuckets.size() == 0) {
            return filled;
        }

        long orderSize = order.size;

        List<Long> emptyBuckets = new ArrayList<>();
        for (IOrdersBucket bucket : matchingBuckets.values()) {

//            log.debug("Matching bucket: {} ...", bucket);
//            log.debug("... with order: {}", order);

            //OrderMatchingResult matchingOrders = bucket.match(order.size - filled, order.uid);

            long tradePrice = bucket.getPrice();

            filled += bucket.match(orderSize - filled, order.uid,
                    (mOrder, v, fm, fma) -> {
                        sendTradeEvent(order, mOrder, fm, fma, tradePrice, v);
                        if (fm) {
                            idMap.remove(mOrder.orderId);
                        }
                    });

//            log.debug("Matching orders: {}", matchingOrders);
//            log.debug("order.filled: {}", order.filled);

            long price = bucket.getPrice();

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
        // TODO can remove through iterator ??
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

        NavigableMap<Long, IOrdersBucket> buckets = getBucketsByAction(order.action);
        long price = order.price;
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

        sendReduceEvent(order, order.size - order.filled);

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
        long newPrice = cmd.price;

        Order order = idMap.get(orderId);
        if (order == null) {
            // already matched, moved or cancelled
            return false;
        }

        if (order.uid != cmd.uid) {
            return false;
        }

        long price = order.price;
        NavigableMap<Long, IOrdersBucket> buckets = getBucketsByAction(order.action);
        IOrdersBucket ordersBucket = buckets.get(price);

        // if change volume operation - use bucket implementation
        // not very efficient if moving and reducing volume
        if (newSize > 0) {
            if (!ordersBucket.tryReduceSize(cmd, super::sendReduceEvent)) {
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
        SortedMap<Long, IOrdersBucket> matchingArea = subtreeForMatching(order.action, newPrice);
        long filled = tryMatchInstantly(order, matchingArea, order.filled);
        if (filled == order.size) {
            // order was fully matched (100% marketable) - removing from order book
            idMap.remove(orderId);
            return true;
        }
        order.filled = filled;

        // if not filled completely - put it into corresponding bucket
        ordersBucket = buckets.computeIfAbsent(newPrice, p -> {
            IOrdersBucket b = new OrdersBucketNaiveImpl();
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
    private NavigableMap<Long, IOrdersBucket> getBucketsByAction(OrderAction action) {
        return action == OrderAction.ASK ? askBuckets : bidBuckets;
    }


    /**
     * Get order from internal map
     *
     * @param orderId -
     * @return order
     */
    @Override
    public Order getOrderById(long orderId) {
        return idMap.get(orderId);
    }

    @Override
    protected void fillAsks(final int size, L2MarketData data) {
        int i = 0;
        for (IOrdersBucket bucket : askBuckets.values()) {
            data.askPrices[i] = bucket.getPrice();
            data.askVolumes[i] = bucket.getTotalVolume();
            if (++i == size) {
                break;
            }
        }
        data.askSize = i;
    }

    @Override
    protected void fillBids(final int size, L2MarketData data) {
        int i = 0;
        for (IOrdersBucket bucket : bidBuckets.values()) {
            data.bidPrices[i] = bucket.getPrice();
            data.bidVolumes[i] = bucket.getTotalVolume();
            if (i++ == size) {
                break;
            }
        }
        data.bidSize = i;
    }

    @Override
    protected int getTotalAskBuckets() {
        return askBuckets.size();
    }

    @Override
    protected int getTotalBidBuckets() {
        return bidBuckets.size();
    }


    @Override
    public List<IOrdersBucket> getAllAskBuckets() {
        return new ArrayList<>(askBuckets.values());
    }

    @Override
    public List<IOrdersBucket> getAllBidBuckets() {
        return new ArrayList<>(bidBuckets.values());
    }

    @Override
    public long getBestAsk() {
        Long price = askBuckets.firstKey();
        return price != null ? price : Long.MAX_VALUE;
    }

    @Override
    public long getBestBid() {
        Long price = bidBuckets.firstKey();
        return price != null ? price : 0;
    }

    @Override
    public void validateInternalState() {
        askBuckets.values().forEach(IOrdersBucket::validate);
        bidBuckets.values().forEach(IOrdersBucket::validate);
    }

    // for testing only
    @Override
    public void clear() {
        askBuckets.clear();
        bidBuckets.clear();
        idMap.clear();
    }

    // for testing only
    @Override
    public int getOrdersNum() {

        int askOrders = askBuckets.values().stream().mapToInt(IOrdersBucket::getNumOrders).sum();
        int bidOrders = bidBuckets.values().stream().mapToInt(IOrdersBucket::getNumOrders).sum();
        //log.debug("idMap:{} askOrders:{} bidOrders:{}", idMap.size(), askOrders, bidOrders);
        int knownOrders = idMap.size();

        assert knownOrders == askOrders + bidOrders : "inconsistent known orders";

        return knownOrders;
    }

    @Override
    public int hashCode() {
        IOrdersBucket[] a = this.askBuckets.values().toArray(new IOrdersBucket[this.askBuckets.size()]);
        IOrdersBucket[] b = this.bidBuckets.values().toArray(new IOrdersBucket[this.bidBuckets.size()]);

        //log.debug("SLOW A:{} B:{}", a, b);

        return IOrderBook.hash(a, b);
    }

    @Override
    public boolean equals(Object o) {
        return IOrderBook.equals(this, o);
    }

}
