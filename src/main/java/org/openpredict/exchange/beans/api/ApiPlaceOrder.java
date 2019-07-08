package org.openpredict.exchange.beans.api;


import lombok.Builder;
import org.openpredict.exchange.beans.OrderAction;
import org.openpredict.exchange.beans.OrderType;

@Builder
public final class ApiPlaceOrder extends ApiCommand {

    final public long price;
    final public long size;
    final public long id;
    final public OrderAction action;
    final public OrderType orderType;

    final public long uid;
    final public int symbol;

    final public long reservePrice;

    // options

    @Override
    public String toString() {
        return "[ADD " + id + " u" + uid + " " + (action == OrderAction.ASK ? 'A' : 'B')
                + ":" + (orderType == OrderType.IOC ? "IOC" : "GTC")
                + ":" + price + ":" + size + "]";
        //(reservePrice != 0 ? ("(R" + reservePrice + ")") : "") +
    }
}
