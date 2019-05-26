package org.openpredict.exchange.core.orderbook;

import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.openpredict.exchange.beans.L2MarketData;
import org.openpredict.exchange.beans.Order;
import org.openpredict.exchange.beans.OrderType;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public interface IOrderBook extends WriteBytesMarshallable {

    /**
     * Process new MARKET order
     * Such order matched to any existing LIMIT orders
     * Of there is not enough volume in order book - reject as partially filled
     *
     * @param order - market order to match
     */
    void matchMarketOrder(OrderCommand order);

    /**
     * Place new LIMIT order
     * If order is marketable (there are matching limit orders) - match it first with existing liquidity
     * <p>
     * // todo return reject reason ?
     *
     * @param cmd - limit order to place
     */
    boolean placeNewLimitOrder(OrderCommand cmd);

    /**
     * Cancel order
     * <p>
     * orderId - order Id
     *
     * @return false if order was not found, otherwise always true
     */
    boolean cancelOrder(OrderCommand cmd);

    /**
     * Reduce volume or/and move an order
     * <p>
     * orderId  - order Id
     * newPrice - new price (if 0 or same - order will not moved)
     * newSize  - new size (if higher than current size or 0 - order will not downsized)
     *
     * @return false if order was not found, otherwise always true
     */
    boolean updateOrder(OrderCommand cmd);


    int getOrdersNum();

    Order getOrderById(long orderId);

    List<IOrdersBucket> getAllAskBuckets();

    List<IOrdersBucket> getAllBidBuckets();

    /**
     * Request best ask price.
     *
     * @return best ask price, or Long.MAX_VALUE if there are no asks
     */
    long getBestAsk();

    /**
     * Request best bid price.
     *
     * @return best ask price, or 0 if there are no bids
     */
    long getBestBid();

    // testing only - validateInternalState without changing state
    void validateInternalState();

    /**
     * @return actual implementation
     */
    OrderBookImplType getImplementationType();

    // TODO to default?
    static int hash(IOrdersBucket[] askBuckets, IOrdersBucket[] bidBuckets) {
        int a = Arrays.hashCode(askBuckets);
        int b = Arrays.hashCode(bidBuckets);
        return Objects.hash(a, b);
    }

    // TODO to default?
    static boolean equals(IOrderBook me, Object o) {
        if (o == me) return true;
        if (o == null) return false;
        if (!(o instanceof IOrderBook)) return false;
        IOrderBook other = (IOrderBook) o;
        return new EqualsBuilder()
                // TODO compare symbol?
                .append(me.getAllAskBuckets(), other.getAllAskBuckets())
                .append(me.getAllBidBuckets(), other.getAllBidBuckets())
                .isEquals();

    }

    default void printFullOrderBook() {
        getAllAskBuckets().forEach(a -> System.out.println(String.format("ASK %s", a.dumpToSingleLine())));
        getAllBidBuckets().forEach(b -> System.out.println(String.format("BID %s", b.dumpToSingleLine())));
    }


    /**
     * @param size max size for each part (ask, bid)
     * @return
     */

    /**
     * Obtain current L2 Market Data snapshot
     *
     * @param size max size for each part (ask, bid), if negative - all records returned
     * @return L2 Market Data snapshot
     */
    default L2MarketData getL2MarketDataSnapshot(int size) {
        int asksSize = getTotalAskBuckets();
        int bidsSize = getTotalBidBuckets();
        if (size >= 0) {
            // limit size
            asksSize = Math.min(asksSize, size);
            bidsSize = Math.min(bidsSize, size);
        }
        L2MarketData data = new L2MarketData(asksSize, bidsSize);
        fillAsks(asksSize, data);
        fillBids(bidsSize, data);
        return data;
    }

    /**
     * Request to publish L2 market data into outgoing disruptor message
     *
     * @param data - pre-allocated object from ring buffer
     */
    default void publishL2MarketDataSnapshot(L2MarketData data) {
        int size = L2MarketData.L2_SIZE;
        fillAsks(size, data);
        fillBids(size, data);
    }

    void fillAsks(final int size, L2MarketData data);

    void fillBids(final int size, L2MarketData data);

    int getTotalAskBuckets();

    int getTotalBidBuckets();


    static CommandResultCode processCommand(final IOrderBook orderBook, final OrderCommand cmd) {

        switch (cmd.command) {
            case MOVE_ORDER:
                boolean isUpdated = orderBook.updateOrder(cmd);
                return isUpdated ? CommandResultCode.SUCCESS : CommandResultCode.MATCHING_INVALID_ORDER_ID;

            case CANCEL_ORDER:
                boolean isCancelled = orderBook.cancelOrder(cmd);
                return isCancelled ? CommandResultCode.SUCCESS : CommandResultCode.MATCHING_INVALID_ORDER_ID;

            case PLACE_ORDER:
//                log.debug("Place {}", cmd.orderId);
                if (cmd.resultCode == CommandResultCode.VALID_FOR_MATCHING_ENGINE) {

                    if (cmd.orderType == OrderType.LIMIT) {

                        // todo validate price step (also for MOVE_ORDER)
//                    if (spec.stepSize != 1 && cmd.orderType == OrderType.LIMIT && cmd.price % spec.stepSize != 0) {
//                        cmd.resultCode = CommandResultCode.INVALID_PRICE_STEP;
//                        log.warn("Price {} does not match step {} of symbol {}", cmd.price, spec.stepSize, cmd.symbol);
//                        return;
//                    }

                        return orderBook.placeNewLimitOrder(cmd)
                                ? CommandResultCode.SUCCESS
                                : CommandResultCode.MATCHING_DUPLICATE_ORDER_ID;
                    } else {
                        orderBook.matchMarketOrder(cmd);
                        return CommandResultCode.SUCCESS;
                    }
                } else {
                    // no change
                    return cmd.resultCode;
                }


            case ORDER_BOOK_REQUEST:
                //log.debug("ORDER_BOOK_REQUEST {}", cmd.size);
                cmd.marketData = orderBook.getL2MarketDataSnapshot((int) cmd.size);
                return CommandResultCode.SUCCESS;

            default:
                //log.warn("unsupported command {}", cmd.command);
                return CommandResultCode.MATCHING_UNSUPPORTED_COMMAND;
        }

    }


    static IOrderBook create(OrderBookImplType type) {
        switch (type) {
            case NAIVE:
                return new OrderBookNaiveImpl();
            case FAST:
                return new OrderBookFastImpl(OrderBookFastImpl.DEFAULT_HOT_WIDTH);
            default:
                throw new IllegalArgumentException();
        }
    }

    static IOrderBook create(BytesIn bytes) {
        switch (OrderBookImplType.of(bytes.readByte())) {
            case NAIVE:
                return new OrderBookNaiveImpl(bytes);
            case FAST:
                return new OrderBookFastImpl(bytes);
            default:
                throw new IllegalArgumentException();
        }
    }


    @Getter
    enum OrderBookImplType {
        NAIVE(0),
        FAST(1);

        private byte code;

        OrderBookImplType(int code) {
            this.code = (byte) code;
        }

        public static OrderBookImplType of(byte code) {
            switch (code) {
                case 0:
                    return NAIVE;
                case 1:
                    return FAST;
                default:
                    throw new IllegalArgumentException("unknown OrderBookImplType:" + code);
            }
        }
    }


}
