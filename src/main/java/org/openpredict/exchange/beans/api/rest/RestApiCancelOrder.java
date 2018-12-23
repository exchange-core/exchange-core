package org.openpredict.exchange.beans.api.rest;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public final class RestApiCancelOrder {

    private final long orderId;

    private final String symbol;

    // TODO remove
    private final long uid;

    @JsonCreator
    public RestApiCancelOrder(
            @JsonProperty("orderId") long orderId,
            @JsonProperty("symbol") String symbol,
            @JsonProperty("uid") long uid) {

        this.orderId = orderId;
        this.symbol = symbol;
        this.uid = uid;
    }

    @Override
    public String toString() {
        return "[CANCEL " + orderId + "]";
    }
}
