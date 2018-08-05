package org.openpredict.exchange.beans.api;


import lombok.Builder;
import org.openpredict.exchange.beans.OrderAction;
import org.openpredict.exchange.beans.OrderType;

@Builder
public class ApiPlaceOrder extends ApiCommand {

    public int price;
    public long size;
    public long id;
    public OrderAction action;
    public OrderType orderType;

    public long uid;
    public int symbol;

    // options


    @Override
    public String toString() {
        return "[ADD " + id + " " + (action == OrderAction.ASK ? 'A' : 'B') + (orderType == OrderType.MARKET ? 'M' : 'L')
                + price + ":" + size + "]";
    }
}
