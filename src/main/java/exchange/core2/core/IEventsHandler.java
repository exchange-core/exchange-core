package exchange.core2.core;

import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.api.ApiCommand;
import exchange.core2.core.common.cmd.CommandResultCode;

import java.util.List;
import java.util.Objects;

/**
 * Convenient events handler interface for non latency-critical applications.<br>
 * Custom handler implementation should be attached to SimpleEventProcessor.<br>
 * Handler method are invoked from single thread in following order:
 * <table>
 * <caption>Execution order</caption>
 * <tr><td>1. </td><td> commandResult</td></tr>
 * <tr><td>2A.  </td><td> optional reduceEvent <td> optional tradeEvent</td></tr>
 * <tr><td>2B. </td><td> <td>optional rejectEvent</td></tr>
 * <tr><td>3. </td><td> orderBook - mandatory for ApiOrderBookRequest, optional for other commands</td></tr>
 * </table>
 * Events processing will stop immediately if any handler throws an exception - you should consider wrapping logic into try-catch block if necessary.
 */
public interface IEventsHandler {

    /**
     * Method is called after each commands execution.
     *
     * @param commandResult - immutable object describing original command, result code, and assigned sequence number.
     */
    void commandResult(ApiCommandResult commandResult);

    /**
     * Method is called if order execution was resulted to one or more trades.
     *
     * @param tradeEvent - immutable object describing event details
     */
    void tradeEvent(TradeEvent tradeEvent);

    /**
     * Method is called if IoC order was not possible to match with provided price limit.
     *
     * @param rejectEvent - immutable object describing event details
     */
    void rejectEvent(RejectEvent rejectEvent);

    /**
     * Method is called if Cancel or Reduce command was successfully executed.
     *
     * @param reduceEvent - immutable object describing event details
     */
    void reduceEvent(ReduceEvent reduceEvent);

    /**
     * Method is called when order book snapshot (L2MarketData) was attached to commands by matching engine.
     * That always happens for ApiOrderBookRequest, sometimes for other commands.
     *
     * @param orderBook - immutable object containing L2 OrderBook snapshot
     */
    void orderBook(OrderBook orderBook);

    class ApiCommandResult {
        public final ApiCommand command;
        public final CommandResultCode resultCode;
        public final long seq;

        public ApiCommandResult(ApiCommand command, CommandResultCode resultCode, long seq) {
            this.command = command;
            this.resultCode = resultCode;
            this.seq = seq;
        }

        public ApiCommand getCommand() {
            return this.command;
        }

        public CommandResultCode getResultCode() {
            return this.resultCode;
        }

        public long getSeq() {
            return this.seq;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o instanceof ApiCommandResult other
                    && other.canEqual(this)
                    && this.getSeq() == other.getSeq()
                    && Objects.equals(this.getCommand(), other.getCommand())
                    && Objects.equals(this.getResultCode(), other.getResultCode());
        }

        protected boolean canEqual(Object other) {
            return other instanceof IEventsHandler.ApiCommandResult;
        }

        @Override
        public int hashCode() {
            final int prime = 59;
            int result = 1;
            result = result * prime + Long.hashCode(this.getSeq());
            var command = this.getCommand();
            result = result * prime + (command == null ? 43 : command.hashCode());
            var resultCode = this.getResultCode();
            result = result * prime + (resultCode == null ? 43 : resultCode.hashCode());
            return result;
        }

        @Override
        public String toString() {
            return "IEventsHandler.ApiCommandResult(command=" + this.getCommand()
                    + ", resultCode=" + this.getResultCode()
                    + ", seq=" + this.getSeq() + ")";
        }
    }

    class TradeEvent {
        public final int symbol;
        public final long totalVolume;
        public final long takerOrderId;
        public final long takerUid;
        public final OrderAction takerAction;
        public final boolean takeOrderCompleted;
        public final long timestamp;
        public final long takerFee;
        public final List<Trade> trades;

        public TradeEvent(
                int symbol,
                long totalVolume,
                long takerOrderId,
                long takerUid,
                OrderAction takerAction,
                boolean takeOrderCompleted,
                long timestamp,
                long takerFee,
                List<Trade> trades) {
            this.symbol = symbol;
            this.totalVolume = totalVolume;
            this.takerOrderId = takerOrderId;
            this.takerUid = takerUid;
            this.takerAction = takerAction;
            this.takeOrderCompleted = takeOrderCompleted;
            this.timestamp = timestamp;
            this.takerFee = takerFee;
            this.trades = trades;
        }

        public int getSymbol() {
            return this.symbol;
        }

        public long getTotalVolume() {
            return this.totalVolume;
        }

        public long getTakerOrderId() {
            return this.takerOrderId;
        }

        public long getTakerUid() {
            return this.takerUid;
        }

        public OrderAction getTakerAction() {
            return this.takerAction;
        }

        public boolean isTakeOrderCompleted() {
            return this.takeOrderCompleted;
        }

        public long getTimestamp() {
            return this.timestamp;
        }

        public long getTakerFee() {
            return this.takerFee;
        }

        public List<Trade> getTrades() {
            return this.trades;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o instanceof TradeEvent other
                    && other.canEqual(this)
                    && this.getSymbol() == other.getSymbol()
                    && this.getTotalVolume() == other.getTotalVolume()
                    && this.getTakerOrderId() == other.getTakerOrderId()
                    && this.getTakerUid() == other.getTakerUid()
                    && this.isTakeOrderCompleted() == other.isTakeOrderCompleted()
                    && this.getTimestamp() == other.getTimestamp()
                    && this.getTakerFee() == other.getTakerFee()
                    && Objects.equals(this.getTakerAction(), other.getTakerAction())
                    && Objects.equals(this.getTrades(), other.getTrades());
        }

        protected boolean canEqual(Object other) {
            return other instanceof IEventsHandler.TradeEvent;
        }

        @Override
        public int hashCode() {
            final int prime = 59;
            int result = 1;
            result = result * prime + this.getSymbol();
            result = result * prime + Long.hashCode(this.getTotalVolume());
            result = result * prime + Long.hashCode(this.getTakerOrderId());
            result = result * prime + Long.hashCode(this.getTakerUid());
            result = result * prime + (this.isTakeOrderCompleted() ? 79 : 97);
            result = result * prime + Long.hashCode(this.getTimestamp());
            result = result * prime + Long.hashCode(this.getTakerFee());
            var takerAction = this.getTakerAction();
            result = result * prime + (takerAction == null ? 43 : takerAction.hashCode());
            var trades = this.getTrades();
            result = result * prime + (trades == null ? 43 : trades.hashCode());
            return result;
        }

        @Override
        public String toString() {
            return "IEventsHandler.TradeEvent(symbol=" + this.getSymbol()
                    + ", totalVolume=" + this.getTotalVolume()
                    + ", takerOrderId=" + this.getTakerOrderId()
                    + ", takerUid=" + this.getTakerUid()
                    + ", takerAction=" + this.getTakerAction()
                    + ", takeOrderCompleted=" + this.isTakeOrderCompleted()
                    + ", timestamp=" + this.getTimestamp()
                    + ", takerFee=" + this.getTakerFee()
                    + ", trades=" + this.getTrades() + ")";
        }
    }

    class Trade {
        public final long makerOrderId;
        public final long makerUid;
        public final boolean makerOrderCompleted;
        public final long price;
        public final long volume;
        public final long makerFee;
        public final long takerFee;

        public Trade(long makerOrderId, long makerUid, boolean makerOrderCompleted,
                     long price, long volume, long makerFee, long takerFee) {
            this.makerOrderId = makerOrderId;
            this.makerUid = makerUid;
            this.makerOrderCompleted = makerOrderCompleted;
            this.price = price;
            this.volume = volume;
            this.makerFee = makerFee;
            this.takerFee = takerFee;
        }

        public long getMakerOrderId() {
            return this.makerOrderId;
        }

        public long getMakerUid() {
            return this.makerUid;
        }

        public boolean isMakerOrderCompleted() {
            return this.makerOrderCompleted;
        }

        public long getPrice() {
            return this.price;
        }

        public long getVolume() {
            return this.volume;
        }

        public long getMakerFee() {
            return this.makerFee;
        }

        public long getTakerFee() {
            return this.takerFee;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o instanceof Trade other
                    && other.canEqual(this)
                    && this.getMakerOrderId() == other.getMakerOrderId()
                    && this.getMakerUid() == other.getMakerUid()
                    && this.isMakerOrderCompleted() == other.isMakerOrderCompleted()
                    && this.getPrice() == other.getPrice()
                    && this.getVolume() == other.getVolume()
                    && this.getMakerFee() == other.getMakerFee()
                    && this.getTakerFee() == other.getTakerFee();
        }

        protected boolean canEqual(Object other) {
            return other instanceof IEventsHandler.Trade;
        }

        @Override
        public int hashCode() {
            final int prime = 59;
            int result = 1;
            result = result * prime + Long.hashCode(this.getMakerOrderId());
            result = result * prime + Long.hashCode(this.getMakerUid());
            result = result * prime + (this.isMakerOrderCompleted() ? 79 : 97);
            result = result * prime + Long.hashCode(this.getPrice());
            result = result * prime + Long.hashCode(this.getVolume());
            result = result * prime + Long.hashCode(this.getMakerFee());
            result = result * prime + Long.hashCode(this.getTakerFee());
            return result;
        }

        @Override
        public String toString() {
            return "IEventsHandler.Trade(makerOrderId=" + this.getMakerOrderId()
                    + ", makerUid=" + this.getMakerUid()
                    + ", makerOrderCompleted=" + this.isMakerOrderCompleted()
                    + ", price=" + this.getPrice()
                    + ", volume=" + this.getVolume()
                    + ", makerFee=" + this.getMakerFee()
                    + ", takerFee=" + this.getTakerFee() + ")";
        }
    }

    class ReduceEvent {
        public final int symbol;
        public final long reducedVolume;
        public final boolean orderCompleted;
        public final long price;
        public final long orderId;
        public final long uid;
        public final long timestamp;

        public ReduceEvent(
                int symbol,
                long reducedVolume,
                boolean orderCompleted,
                long price,
                long orderId,
                long uid,
                long timestamp) {
            this.symbol = symbol;
            this.reducedVolume = reducedVolume;
            this.orderCompleted = orderCompleted;
            this.price = price;
            this.orderId = orderId;
            this.uid = uid;
            this.timestamp = timestamp;
        }

        public int getSymbol() {
            return this.symbol;
        }

        public long getReducedVolume() {
            return this.reducedVolume;
        }

        public boolean isOrderCompleted() {
            return this.orderCompleted;
        }

        public long getPrice() {
            return this.price;
        }

        public long getOrderId() {
            return this.orderId;
        }

        public long getUid() {
            return this.uid;
        }

        public long getTimestamp() {
            return this.timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o instanceof ReduceEvent other
                    && other.canEqual(this)
                    && this.getSymbol() == other.getSymbol()
                    && this.getReducedVolume() == other.getReducedVolume()
                    && this.isOrderCompleted() == other.isOrderCompleted()
                    && this.getPrice() == other.getPrice()
                    && this.getOrderId() == other.getOrderId()
                    && this.getUid() == other.getUid()
                    && this.getTimestamp() == other.getTimestamp();
        }

        protected boolean canEqual(Object other) {
            return other instanceof IEventsHandler.ReduceEvent;
        }

        @Override
        public int hashCode() {
            final int prime = 59;
            int result = 1;
            result = result * prime + this.getSymbol();
            result = result * prime + Long.hashCode(this.getReducedVolume());
            result = result * prime + (this.isOrderCompleted() ? 79 : 97);
            result = result * prime + Long.hashCode(this.getPrice());
            result = result * prime + Long.hashCode(this.getOrderId());
            result = result * prime + Long.hashCode(this.getUid());
            result = result * prime + Long.hashCode(this.getTimestamp());
            return result;
        }

        @Override
        public String toString() {
            return "IEventsHandler.ReduceEvent(symbol=" + this.getSymbol()
                    + ", reducedVolume=" + this.getReducedVolume()
                    + ", orderCompleted=" + this.isOrderCompleted()
                    + ", price=" + this.getPrice()
                    + ", orderId=" + this.getOrderId()
                    + ", uid=" + this.getUid()
                    + ", timestamp=" + this.getTimestamp() + ")";
        }
    }

    class RejectEvent {
        public final int symbol;
        public final long rejectedVolume;
        public final long price;
        public final long orderId;
        public final long uid;
        public final long timestamp;

        public RejectEvent(int symbol, long rejectedVolume, long price, long orderId, long uid, long timestamp) {
            this.symbol = symbol;
            this.rejectedVolume = rejectedVolume;
            this.price = price;
            this.orderId = orderId;
            this.uid = uid;
            this.timestamp = timestamp;
        }

        public int getSymbol() {
            return this.symbol;
        }

        public long getRejectedVolume() {
            return this.rejectedVolume;
        }

        public long getPrice() {
            return this.price;
        }

        public long getOrderId() {
            return this.orderId;
        }

        public long getUid() {
            return this.uid;
        }

        public long getTimestamp() {
            return this.timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o instanceof RejectEvent other
                    && other.canEqual(this)
                    && this.getSymbol() == other.getSymbol()
                    && this.getRejectedVolume() == other.getRejectedVolume()
                    && this.getPrice() == other.getPrice()
                    && this.getOrderId() == other.getOrderId()
                    && this.getUid() == other.getUid()
                    && this.getTimestamp() == other.getTimestamp();
        }

        protected boolean canEqual(Object other) {
            return other instanceof IEventsHandler.RejectEvent;
        }

        @Override
        public int hashCode() {
            final int prime = 59;
            int result = 1;
            result = result * prime + this.getSymbol();
            result = result * prime + Long.hashCode(this.getRejectedVolume());
            result = result * prime + Long.hashCode(this.getPrice());
            result = result * prime + Long.hashCode(this.getOrderId());
            result = result * prime + Long.hashCode(this.getUid());
            result = result * prime + Long.hashCode(this.getTimestamp());
            return result;
        }

        @Override
        public String toString() {
            return "IEventsHandler.RejectEvent(symbol=" + this.getSymbol()
                    + ", rejectedVolume=" + this.getRejectedVolume()
                    + ", price=" + this.getPrice()
                    + ", orderId=" + this.getOrderId()
                    + ", uid=" + this.getUid()
                    + ", timestamp=" + this.getTimestamp() + ")";
        }
    }

    class CommandExecutionResult {
        public final int symbol;
        public final long volume;
        public final long price;
        public final long orderId;
        public final long uid;
        public final long timestamp;

        public CommandExecutionResult(int symbol, long volume, long price, long orderId, long uid, long timestamp) {
            this.symbol = symbol;
            this.volume = volume;
            this.price = price;
            this.orderId = orderId;
            this.uid = uid;
            this.timestamp = timestamp;
        }

        public int getSymbol() {
            return this.symbol;
        }

        public long getVolume() {
            return this.volume;
        }

        public long getPrice() {
            return this.price;
        }

        public long getOrderId() {
            return this.orderId;
        }

        public long getUid() {
            return this.uid;
        }

        public long getTimestamp() {
            return this.timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o instanceof CommandExecutionResult other
                    && other.canEqual(this)
                    && this.getSymbol() == other.getSymbol()
                    && this.getVolume() == other.getVolume()
                    && this.getPrice() == other.getPrice()
                    && this.getOrderId() == other.getOrderId()
                    && this.getUid() == other.getUid()
                    && this.getTimestamp() == other.getTimestamp();
        }

        protected boolean canEqual(Object other) {
            return other instanceof IEventsHandler.CommandExecutionResult;
        }

        @Override
        public int hashCode() {
            final int prime = 59;
            int result = 1;
            result = result * prime + this.getSymbol();
            result = result * prime + Long.hashCode(this.getVolume());
            result = result * prime + Long.hashCode(this.getPrice());
            result = result * prime + Long.hashCode(this.getOrderId());
            result = result * prime + Long.hashCode(this.getUid());
            result = result * prime + Long.hashCode(this.getTimestamp());
            return result;
        }

        @Override
        public String toString() {
            return "IEventsHandler.CommandExecutionResult(symbol=" + this.getSymbol()
                    + ", volume=" + this.getVolume()
                    + ", price=" + this.getPrice()
                    + ", orderId=" + this.getOrderId()
                    + ", uid=" + this.getUid()
                    + ", timestamp=" + this.getTimestamp() + ")";
        }
    }

    class OrderBook {
        public final int symbol;
        public final List<OrderBookRecord> asks;
        public final List<OrderBookRecord> bids;
        public final long timestamp;

        public OrderBook(int symbol, List<OrderBookRecord> asks, List<OrderBookRecord> bids, long timestamp) {
            this.symbol = symbol;
            this.asks = asks;
            this.bids = bids;
            this.timestamp = timestamp;
        }

        public int getSymbol() {
            return this.symbol;
        }

        public List<OrderBookRecord> getAsks() {
            return this.asks;
        }

        public List<OrderBookRecord> getBids() {
            return this.bids;
        }

        public long getTimestamp() {
            return this.timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o instanceof OrderBook other
                    && other.canEqual(this)
                    && this.getSymbol() == other.getSymbol()
                    && this.getTimestamp() == other.getTimestamp()
                    && Objects.equals(this.getAsks(), other.getAsks())
                    && Objects.equals(this.getBids(), other.getBids());
        }

        protected boolean canEqual(Object other) {
            return other instanceof IEventsHandler.OrderBook;
        }

        @Override
        public int hashCode() {
            final int prime = 59;
            int result = 1;
            result = result * prime + this.getSymbol();
            result = result * prime + Long.hashCode(this.getTimestamp());
            var asks = this.getAsks();
            result = result * prime + (asks == null ? 43 : asks.hashCode());
            var bids = this.getBids();
            result = result * prime + (bids == null ? 43 : bids.hashCode());
            return result;
        }

        @Override
        public String toString() {
            return "IEventsHandler.OrderBook(symbol=" + this.getSymbol()
                    + ", asks=" + this.getAsks()
                    + ", bids=" + this.getBids()
                    + ", timestamp=" + this.getTimestamp() + ")";
        }
    }

    class OrderBookRecord {
        public final long price;
        public final long volume;
        public final int orders;

        public OrderBookRecord(long price, long volume, int orders) {
            this.price = price;
            this.volume = volume;
            this.orders = orders;
        }

        public long getPrice() {
            return this.price;
        }

        public long getVolume() {
            return this.volume;
        }

        public int getOrders() {
            return this.orders;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o instanceof OrderBookRecord other
                    && other.canEqual(this)
                    && this.getPrice() == other.getPrice()
                    && this.getVolume() == other.getVolume()
                    && this.getOrders() == other.getOrders();
        }

        protected boolean canEqual(Object other) {
            return other instanceof IEventsHandler.OrderBookRecord;
        }

        @Override
        public int hashCode() {
            final int prime = 59;
            int result = 1;
            result = result * prime + Long.hashCode(this.getPrice());
            result = result * prime + Long.hashCode(this.getVolume());
            result = result * prime + this.getOrders();
            return result;
        }

        @Override
        public String toString() {
            return "IEventsHandler.OrderBookRecord(price=" + this.getPrice()
                    + ", volume=" + this.getVolume()
                    + ", orders=" + this.getOrders() + ")";
        }
    }
}


