package org.openpredict.exchange.core.orderbook;

import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.openpredict.exchange.beans.*;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.beans.cmd.OrderCommandType;
import org.openpredict.exchange.core.Utils;

import java.util.*;

import static org.openpredict.exchange.beans.OrderAction.BID;

@Slf4j
public final class OrderBookNaiveImpl implements IOrderBook {

    private final NavigableMap<Long, IOrdersBucket> askBuckets;
    private final NavigableMap<Long, IOrdersBucket> bidBuckets;

    private final SymbolType symbolType = SymbolType.FUTURES_CONTRACT;

    private final LongObjectHashMap<Order> idMap = new LongObjectHashMap<>();

    public OrderBookNaiveImpl() {

        this.askBuckets = new TreeMap<>();
        this.bidBuckets = new TreeMap<>(Collections.reverseOrder());
    }

    public OrderBookNaiveImpl(BytesIn bytes) {

        this.askBuckets = Utils.readLongMap(bytes, TreeMap::new, IOrdersBucket::create);
        this.bidBuckets = Utils.readLongMap(bytes, () -> new TreeMap<>(Collections.reverseOrder()), IOrdersBucket::create);

        // reconstruct ordersId-> Order cache
        // TODO check resulting performance
        askBuckets.values().forEach(bucket -> bucket.forEachOrder(order -> idMap.put(order.orderId, order)));
        bidBuckets.values().forEach(bucket -> bucket.forEachOrder(order -> idMap.put(order.orderId, order)));

        //validateInternalState();
    }

    @Override
    public CommandResultCode newOrder(OrderCommand cmd) {

        final OrderType orderType = cmd.orderType;
        final OrderAction action = cmd.action;
        final long price = cmd.price;
        final long size = cmd.size;

        // check if order is marketable (if there are opposite matching orders)
        long filledSize = tryMatchInstantly(cmd, subtreeForMatching(action, price), 0, cmd);
        if (filledSize == size) {
            // order is fully matched - can just return
            return CommandResultCode.SUCCESS;

        }

        if (orderType == OrderType.IOC) {
            OrderBookEventsHelper.attachRejectEvent(cmd, cmd.size - filledSize);
            return CommandResultCode.SUCCESS;
        }

        long newOrderId = cmd.orderId;
        if (idMap.containsKey(newOrderId)) {

            // duplicate order id - can match, but can not place
            OrderBookEventsHelper.attachRejectEvent(cmd, cmd.size - filledSize);
            return CommandResultCode.MATCHING_DUPLICATE_ORDER_ID;
        }

        // normally placing regular GTC limit order

        final Order orderRecord = new Order(
                OrderCommandType.PLACE_ORDER,
                newOrderId,
                cmd.symbol,
                price,
                size,
                cmd.price2,
                action,
                orderType,
                cmd.uid,
                cmd.timestamp,
                cmd.userCookie,
                filledSize);

        final IOrdersBucket bucket = getBucketsByAction(action)
                .computeIfAbsent(price, p -> {
                    final IOrdersBucket b = new OrdersBucketNaiveImpl();
                    b.setPrice(p);
                    return b;
                });
        bucket.put(orderRecord);

        idMap.put(newOrderId, orderRecord);

        return CommandResultCode.SUCCESS;
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
     * @param activeOrder     - GTC or IOC order to match
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

    @Override
    public CommandResultCode moveOrder(OrderCommand cmd) {

        final long orderId = cmd.orderId;
        final long newPrice = cmd.price;

        final Order order = idMap.get(orderId);
        if (order == null) {
            // already matched, moved or cancelled
            return CommandResultCode.MATCHING_UNKNOWN_ORDER_ID;
        }

//         log.debug("{}. {}->{}", orderId, order.price, newPrice);

        if (order.uid != cmd.uid) {
            return CommandResultCode.MATCHING_UNKNOWN_ORDER_ID;
        }

        final long price = order.price;
        final NavigableMap<Long, IOrdersBucket> buckets = getBucketsByAction(order.action);
        final IOrdersBucket bucket = buckets.get(price);

        // optimistic risk check mode for exchange bids
        if (symbolType == SymbolType.CURRENCY_EXCHANGE_PAIR && order.action == BID && cmd.price > order.price2) {
            // put order back (yes it will be in the end of queue)
            bucket.put(order);
            return CommandResultCode.MATCHING_MOVE_FAILED_PRICE_ABOVE_RISK_LIMIT;
        }

        // take order out of the original bucket and clean bucket if its empty
        bucket.remove(orderId, cmd.uid);
        if (bucket.getTotalVolume() == 0) {
            buckets.remove(price);
        }

        order.price = newPrice;

        // try match with new price
        final SortedMap<Long, IOrdersBucket> matchingArea = subtreeForMatching(order.action, newPrice);
        long filled = tryMatchInstantly(order, matchingArea, order.filled, cmd);
        if (filled == order.size) {
            // order was fully matched (100% marketable) - removing from order book
            idMap.remove(orderId);
            return CommandResultCode.SUCCESS;
        }
        order.filled = filled;

        // if not filled completely - put it into corresponding bucket
        final IOrdersBucket anotherBucket = buckets.computeIfAbsent(newPrice, p -> {
            IOrdersBucket b = new OrdersBucketNaiveImpl();
            b.setPrice(p);
            return b;
        });
        anotherBucket.put(order);

        return CommandResultCode.SUCCESS;
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
        if (size == 0) {
            data.askSize = 0;
            return;
        }

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
        if (size == 0) {
            data.bidSize = 0;
            return;
        }

        int i = 0;
        for (IOrdersBucket bucket : bidBuckets.values()) {
            data.bidPrices[i] = bucket.getPrice();
            data.bidVolumes[i] = bucket.getTotalVolume();
            if (++i == size) {
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

    @Override
    public OrderBookImplType getImplementationType() {
        return OrderBookImplType.NAIVE;
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
    public void writeMarshallable(BytesOut bytes) {
        Utils.marshallLongMap(askBuckets, bytes);
        Utils.marshallLongMap(bidBuckets, bytes);
    }

    @Override
    public int hashCode() {
        IOrdersBucket[] a = this.askBuckets.values().toArray(new IOrdersBucket[0]);
        IOrdersBucket[] b = this.bidBuckets.values().toArray(new IOrdersBucket[0]);
//        for(IOrdersBucket ord: a) log.debug("ask {}", ord);
//        for(IOrdersBucket ord: b) log.debug("bid {}", ord);
        return IOrderBook.hash(a, b);
    }

    @Override
    public boolean equals(Object o) {
        return IOrderBook.equals(this, o);
    }

}
