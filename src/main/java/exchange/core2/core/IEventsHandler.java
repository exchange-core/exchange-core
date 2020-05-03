package exchange.core2.core;

import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.api.ApiCommand;
import exchange.core2.core.common.cmd.CommandResultCode;
import lombok.Data;

import java.util.List;

/**
 * Convenient events handler interface for non latency-critical applications.<p>
 * Custom handler implementation should be attached to SimpleEventProcessor.<p>
 * Handler method are invoked from single thread in following order:<p>
 * <table>
 * <tr><td>1. <td> commandResult
 * <tr><td>2A.  <td> optional reduceEvent <td> optional tradeEvent
 * <tr><td>2B. <td> <td>optional rejectEvent
 * <tr><td>3. <td> orderBook - mandatory for ApiOrderBookRequest, optional for other commands
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

    @Data
    class ApiCommandResult {
        public final ApiCommand command;
        public final CommandResultCode resultCode;
        public final long seq;
    }

    @Data
    class TradeEvent {
        public final int symbol;
        public final long totalVolume;
        public final long takerOrderId;
        public final long takerUid;
        public final OrderAction takerAction;
        public final boolean takeOrderCompleted;
        public final long timestamp;
        public final List<Trade> trades;
    }

    @Data
    class Trade {
        public final long makerOrderId;
        public final long makerUid;
        public final boolean makerOrderCompleted;
        public final long price;
        public final long volume;
    }

    @Data
    class ReduceEvent {
        public final int symbol;
        public final long reducedVolume;
        public final boolean orderCompleted;
        public final long price;
        public final long orderId;
        public final long uid;
        public final long timestamp;
    }

    @Data
    class RejectEvent {
        public final int symbol;
        public final long rejectedVolume;
        public final long price;
        public final long orderId;
        public final long uid;
        public final long timestamp;
    }

    @Data
    class CommandExecutionResult {
        public final int symbol;
        public final long volume;
        public final long price;
        public final long orderId;
        public final long uid;
        public final long timestamp;
    }

    @Data
    class OrderBook {
        public final int symbol;
        public final List<OrderBookRecord> asks;
        public final List<OrderBookRecord> bids;
        public final long timestamp;
    }

    @Data
    class OrderBookRecord {
        public final long price;
        public final long volume;
        public final int orders;
    }
}


