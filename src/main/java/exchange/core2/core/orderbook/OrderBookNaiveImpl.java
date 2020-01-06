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
import exchange.core2.core.utils.SerializationUtils;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

@Slf4j
public final class OrderBookNaiveImpl implements IOrderBook {

    private final NavigableMap<Long, IOrdersBucket> askBuckets;
    private final NavigableMap<Long, IOrdersBucket> bidBuckets;

    private final CoreSymbolSpecification symbolSpec;

    private final LongObjectHashMap<Order> idMap = new LongObjectHashMap<>();

    public OrderBookNaiveImpl(final CoreSymbolSpecification symbolSpec) {
        this.symbolSpec = symbolSpec;
        this.askBuckets = new TreeMap<>();
        this.bidBuckets = new TreeMap<>(Collections.reverseOrder());
    }

    public OrderBookNaiveImpl(final BytesIn bytes) {
        this.symbolSpec = new CoreSymbolSpecification(bytes);
        this.askBuckets = SerializationUtils.readLongMap(bytes, TreeMap::new, IOrdersBucket::create);
        this.bidBuckets = SerializationUtils.readLongMap(bytes, () -> new TreeMap<>(Collections.reverseOrder()), IOrdersBucket::create);

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
            OrderBookEventsHelper.NO_POOL_HELPER.attachRejectEvent(cmd, cmd.size - filledSize);
            return CommandResultCode.SUCCESS;
        }

        long newOrderId = cmd.orderId;
        if (idMap.containsKey(newOrderId)) {

            // duplicate order id - can match, but can not place
            OrderBookEventsHelper.NO_POOL_HELPER.attachRejectEvent(cmd, cmd.size - filledSize);
            return CommandResultCode.MATCHING_DUPLICATE_ORDER_ID;
        }

        // normally placing regular GTC limit order

        final Order orderRecord = new Order(
                newOrderId,
                price,
                size,
                filledSize,
                cmd.reserveBidPrice,
                action,
                cmd.uid,
                cmd.timestamp);
//                cmd.userCookie);

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
            final IOrder activeOrder,
            final SortedMap<Long, IOrdersBucket> matchingBuckets,
            long filled,
            final OrderCommand triggerCmd) {

//        log.info("matchInstantly: {} {}", order, matchingBuckets);

        if (matchingBuckets.size() == 0) {
            return filled;
        }

        final long orderSize = activeOrder.getSize();

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

            if (filled == orderSize) {
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
    public CommandResultCode cancelOrder(OrderCommand cmd) {
        long orderId = cmd.orderId;

        Order order = idMap.get(orderId);
        if (order == null || order.uid != cmd.uid) {
            // order already matched and removed from order book previously
            return CommandResultCode.MATCHING_UNKNOWN_ORDER_ID;
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

        // send cancel event
        OrderBookEventsHelper.NO_POOL_HELPER.sendCancelEvent(cmd, order);

        // fill action fields (for events handling)
        cmd.action = order.getAction();

        return CommandResultCode.SUCCESS;
    }

    @Override
    public CommandResultCode moveOrder(OrderCommand cmd) {

        final long orderId = cmd.orderId;
        final long newPrice = cmd.price;

        final Order order = idMap.get(orderId);
        if (order == null || order.uid != cmd.uid) {
            // already matched, moved or cancelled
            return CommandResultCode.MATCHING_UNKNOWN_ORDER_ID;
        }

        final long price = order.price;
        final NavigableMap<Long, IOrdersBucket> buckets = getBucketsByAction(order.action);
        final IOrdersBucket bucket = buckets.get(price);

        // optimistic risk check mode for exchange bids
        if (symbolSpec.type == SymbolType.CURRENCY_EXCHANGE_PAIR && order.action == OrderAction.BID && cmd.price > order.reserveBidPrice) {
            // put order back (yes it will be in the end of queue)
            bucket.put(order);
            return CommandResultCode.MATCHING_MOVE_FAILED_PRICE_OVER_RISK_LIMIT;
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
    public IOrder getOrderById(long orderId) {
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
            data.askOrders[i] = bucket.getNumOrders();
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
            data.bidOrders[i] = bucket.getNumOrders();
            if (++i == size) {
                break;
            }
        }
        data.bidSize = i;
    }

    @Override
    public int getTotalAskBuckets(final int limit) {
        return Math.min(limit, askBuckets.size());
    }

    @Override
    public int getTotalBidBuckets(final int limit) {
        return Math.min(limit, bidBuckets.size());
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

    @Override
    public List<Order> findUserOrders(final long uid) {
        List<Order> list = new ArrayList<>();
        Consumer<IOrdersBucket> bucketConsumer = bucket -> bucket.forEachOrder(order -> {
            if (order.uid == uid) {
                list.add(order);
            }
        });
        askBuckets.values().forEach(bucketConsumer);
        bidBuckets.values().forEach(bucketConsumer);
        return list;
    }

    @Override
    public CoreSymbolSpecification getSymbolSpec() {
        return symbolSpec;
    }

    @Override
    public Stream<IOrder> askOrdersStream(final boolean sorted) {
        return askBuckets.values().stream().flatMap(bucket -> bucket.getAllOrders().stream());
    }

    @Override
    public Stream<IOrder> bidOrdersStream(final boolean sorted) {
        return bidBuckets.values().stream().flatMap(bucket -> bucket.getAllOrders().stream());
    }

    // for testing only
    @Override
    public int getOrdersNum(OrderAction action) {
        final NavigableMap<Long, IOrdersBucket> buckets = action == OrderAction.ASK ? askBuckets : bidBuckets;
        return buckets.values().stream().mapToInt(IOrdersBucket::getNumOrders).sum();
//        int askOrders = askBuckets.values().stream().mapToInt(IOrdersBucket::getNumOrders).sum();
//        int bidOrders = bidBuckets.values().stream().mapToInt(IOrdersBucket::getNumOrders).sum();
        //log.debug("idMap:{} askOrders:{} bidOrders:{}", idMap.size(), askOrders, bidOrders);
//        int knownOrders = idMap.size();
//        assert knownOrders == askOrders + bidOrders : "inconsistent known orders";
    }

    @Override
    public long getTotalOrdersVolume(OrderAction action) {
        final NavigableMap<Long, IOrdersBucket> buckets = action == OrderAction.ASK ? askBuckets : bidBuckets;
        return buckets.values().stream().mapToLong(IOrdersBucket::getTotalVolume).sum();
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeByte(getImplementationType().getCode());
        symbolSpec.writeMarshallable(bytes);
        SerializationUtils.marshallLongMap(askBuckets, bytes);
        SerializationUtils.marshallLongMap(bidBuckets, bytes);
    }
}
