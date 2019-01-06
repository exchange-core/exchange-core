package org.openpredict.exchange.rest.commands;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public final class RestApiMoveOrder {

    private final String price;
    private final String size;

    private final long orderId;

    private final String symbol;

    // TODO remove
    private final long uid;

    @JsonCreator
    public RestApiMoveOrder(
            @JsonProperty("price") String price,
            @JsonProperty("size") String size,
            @JsonProperty("orderId") long orderId,
            @JsonProperty("uid") long uid,
            @JsonProperty("symbol") String symbol) {

        this.price = price;
        this.size = size;
        this.orderId = orderId;
        this.uid = uid;
        this.symbol = symbol;
    }

    @Override
    public String toString() {
        return "[MOVE " + orderId + " " + price + ":" + size + "]";
    }
}
