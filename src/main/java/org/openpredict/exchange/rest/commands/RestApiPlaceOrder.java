package org.openpredict.exchange.rest.commands;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import org.openpredict.exchange.beans.OrderAction;
import org.openpredict.exchange.beans.OrderType;

@Getter
public final class RestApiPlaceOrder {

    private final String price;
    private final String size;

    private final long cookieId;
    private final OrderAction action;
    private final OrderType orderType;

    private final String symbol;

    // TODO remove
    private final long uid;

    @JsonCreator
    public RestApiPlaceOrder(
            @JsonProperty("price") String price,
            @JsonProperty("size") String size,
            @JsonProperty("cookieId") long cookieId,
            @JsonProperty("action") OrderAction action,
            @JsonProperty("orderType") OrderType orderType,
            @JsonProperty("uid") long uid,
            @JsonProperty("symbol") String symbol) {

        this.price = price;
        this.size = size;
        this.cookieId = cookieId;
        this.action = action;
        this.orderType = orderType;
        this.uid = uid;
        this.symbol = symbol;
    }

    @Override
    public String toString() {
        return "[ADD " + cookieId + " " + (action == OrderAction.ASK ? 'A' : 'B') + (orderType == OrderType.MARKET ? 'M' : 'L')
                + price + ":" + size + "]";
    }
}
