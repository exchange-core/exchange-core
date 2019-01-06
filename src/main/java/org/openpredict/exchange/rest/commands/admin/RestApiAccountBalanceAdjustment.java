package org.openpredict.exchange.rest.commands.admin;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public final class RestApiAccountBalanceAdjustment {

    private final long uid;
    private final long transactionId;
    private final String amount;

    @JsonCreator
    public RestApiAccountBalanceAdjustment(
            @JsonProperty("uid") long uid,
            @JsonProperty("transactionId") long transactionId,
            @JsonProperty("amount") String amount) {

        this.uid = uid;
        this.transactionId = transactionId;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "[BALANCE_ADJ " + uid + " for " + amount + " transactionId:" + transactionId + "]";
    }
}
