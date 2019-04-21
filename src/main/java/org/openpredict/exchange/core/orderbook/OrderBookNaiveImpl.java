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
public final class OrderBookNaiveImpl implements IOrderBook {

    private final NavigableMap<Long, IOrdersBucket> askBuckets = new TreeMap<>();
    private final NavigableMap<Long, IOrdersBucket> bidBuckets = new TreeMap<>(Collections.reverseOrder());

    private final LongObjectHashMap<Order> idMap = new LongObjectHashMap<>();

    public void matchMarketOrder(OrderCommand cmd) {
        final NavigableMap<Long, IOrdersBucket> matchingBuckets = cmd.action == OrderAction.ASK ? bidBuckets : askBuckets;
        long filledSize = tryMatchInstantly(cmd, matchingBuckets, 0, cmd);

        // partially filled due no liquidity - should report PARTIAL order execution
        if (filledSize < cmd.size) {
            OrderBookEventsHelper.attachRejectEvent(cmd, filledSize);
        }
    }

    @Override
    public boolean placeNewLimitOrder(OrderCommand cmd) {

        if (idMap.containsKey(cmd.orderId)) {
            // duplicate
            return false;
        }

        // check if order is marketable (if there are opposite matching orders)
        long filled = tryMatchInstantly(cmd, subtreeForMatching(cmd.action, cmd.price), 0, cmd);
        if (filled == cmd.size) {
            // fully matched as marketable before actually place - can just return
            return true;
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

        return true;
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
     * @param activeOrder     - LIMIT or MARKET order to match
     * @param matchingBuckets - sorted buckets map
     * @param filled          - current 'filled' value for the order
     * @param triggerCmd      -
     * @return new filled size
     */
    private long tryMatchInstantly(
            final OrderCommand activeOrder,
            final SortedMap<Long, IOrdersBucket> matchingBuckets,
            long filled,
            final OrderCommand triggerCmd) {

//        log.info("matchInstantly: {} {}", order, matchingBuckets);

        if (matchingBuckets.size() == 0) {
            return filled;
        }

        long orderSize = activeOrder.size;

        List<Long> emptyBuckets = new ArrayList<>();
        for (IOrdersBucket bucket : matchingBuckets.values()) {

//            log.debug("Matching bucket: {} ...", bucket);
//            log.debug("... with order: {}", activeOrder);

            //OrderMatchingResult matchingOrders = bucket.match(activeOrder.size - filled, activeOrder.uid);

            final long sizeLeft = orderSize - filled;

            filled += bucket.match(sizeLeft, activeOrder, triggerCmd, this::removeFullyMatchedOrder);

//            log.debug("Matching orders: {}", matchingOrders);
//            log.debug("order.filled: {}", activeOrder.filled);

            long price = bucket.getPrice();

            // remove empty buckets
            if (bucket.getTotalVolume() == 0) {
                emptyBuckets.add(price);
            }

            if (filled == activeOrder.size) {
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

    private void removeFullyMatchedOrder(Order mOrder) {
        idMap.remove(mOrder.orderId);
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

        OrderBookEventsHelper.sendReduceEvent(cmd, order, order.size - order.filled);

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
            if (!ordersBucket.tryReduceSize(cmd)) {
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
        long filled = tryMatchInstantly(order, matchingArea, order.filled, cmd);
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
    public void fillAsks(final int size, L2MarketData data) {
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
    public void fillBids(final int size, L2MarketData data) {
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
    public int getTotalAskBuckets() {
        return askBuckets.size();
    }

    @Override
    public int getTotalBidBuckets() {
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
