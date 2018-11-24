package org.openpredict.exchange.beans.api;


import lombok.Builder;
import org.openpredict.exchange.beans.OrderAction;
import org.openpredict.exchange.beans.OrderType;

@Builder
public class ApiPlaceOrder extends ApiCommand {

    public final long price;
    public final long size;
    public final long id;
    public final OrderAction action;
    public final OrderType orderType;

    public final long uid;
    public final int symbol;

    // options


    @Override
    public String toString() {
        return "[ADD " + id + " " + (action == OrderAction.ASK ? 'A' : 'B') + (orderType == OrderType.MARKET ? 'M' : 'L')
                + price + ":" + size + "]";
    }
}
