package org.openpredict.exchange.rest.commands.admin;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public final class RestApiRequestMarketData {

    private final String symbolName;

    @JsonCreator
    public RestApiRequestMarketData(@JsonProperty("symbolName") String symbolName) {

        this.symbolName = symbolName;
    }

    @Override
    public String toString() {
        return "[REQ_MARKET_DATA " + symbolName + "]";
    }
}
