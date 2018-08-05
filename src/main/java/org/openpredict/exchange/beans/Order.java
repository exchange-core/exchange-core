package org.openpredict.exchange.beans;


import lombok.Builder;
import lombok.NoArgsConstructor;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.beans.cmd.OrderCommandType;

/**
 * Extending OrderCommand allows to avoid creating new objects
 * for instantly matching orders (MARKET or marketable LIMIT orders)
 * as well as use same code for matching moved orders
 * <p>
 * Objects only lives in OrderBook, no external references allowed
 */
@NoArgsConstructor
public class Order extends OrderCommand {

    public long filled;

    @Builder(builderMethodName = "orderBuilder", builderClassName = "OrderBuilder")
    public Order(OrderCommandType command, long orderId, int symbol, int price, long size, OrderAction action, OrderType orderType,
                 long uid, long timestamp, long filled) {
        //super(command, orderId, symbol, price, size, action, orderType, uid, timestamp, 0, null, null);
        super(command, orderId, symbol, price, size, action, orderType, uid, timestamp, null, null);
        this.filled = filled;
    }

    @Override
    public String toString() {
        return "[" + orderId + " " + (action == OrderAction.ASK ? 'A' : 'B') + (orderType == OrderType.MARKET ? 'M' : 'L')
                + price + ":" + size + "F" + filled + "]";
    }
}
