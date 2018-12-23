package org.openpredict.exchange.beans.api.rest.admin;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public final class RestApiAccountBalanceAdjustment {

    private final long uid;
    private final String amount;

    @JsonCreator
    public RestApiAccountBalanceAdjustment(
            @JsonProperty("uid") long uid,
            @JsonProperty("amount") String amount) {

        this.uid = uid;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "[BALANCE_ADJ " + uid + " for " + amount + "]";
    }
}
