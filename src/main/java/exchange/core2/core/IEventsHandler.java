package exchange.core2.core;

import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.api.ApiCommand;
import exchange.core2.core.common.cmd.CommandResultCode;
import lombok.Data;

import java.util.List;

public interface IEventsHandler {

    void tradeEvent(TradeEvent event);

    void cancelEvent(CancelEvent cancelEvent);

    void rejectEvent(RejectEvent rejectEvent);

    void commandResult(ApiCommandResult commandResult);

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
        public final List<Deal> deals;
    }

    @Data
    class Deal {
        public final long makerOrderId;
        public final long makerUid;
        public final boolean makerOrderCompleted;
        public final long price;
        public final long volume;
    }

    @Data
    class CancelEvent {
        public final int symbol;
        public final long volume;
        public final long price;
        public final long orderId;
        public final long uid;
        public final long timestamp;
    }

    @Data
    class RejectEvent {
        public final int symbol;
        public final long volume;
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


