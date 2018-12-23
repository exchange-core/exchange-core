package org.openpredict.exchange.beans.api.rest.admin;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public final class RestApiAddSymbol {

    // unmodifiable properties
    private final int symbolId;
    private final String symbolName;

    // unmodifiable properties (gateway level)
    private final int priceStep; // price % priceStep == 0
    private final int priceScale; // decimal point position
    private final int lotSize;


    // modifiable properties (core level)
    // deposit settings
    private final long depositBuy;
    private final long depositSell;

    // order book limits
    private final long priceHighLimit;
    private final long priceLowLimit;


    @JsonCreator
    public RestApiAddSymbol(
            @JsonProperty("symbolName") String symbolName,
            @JsonProperty("symbolId") int symbolId,
            @JsonProperty("priceStep") int priceStep,
            @JsonProperty("priceScale") int priceScale,
            @JsonProperty("lotSize") int lotSize,
            @JsonProperty("depositBuy") long depositBuy,
            @JsonProperty("depositSell") long depositSell,
            @JsonProperty("priceHighLimit") long priceHighLimit,
            @JsonProperty("priceLowLimit") long priceLowLimit) {

        this.symbolName = symbolName;
        this.symbolId = symbolId;
        this.priceStep = priceStep;
        this.priceScale = priceScale;
        this.lotSize = lotSize;
        this.depositBuy = depositBuy;
        this.depositSell = depositSell;
        this.priceHighLimit = priceHighLimit;
        this.priceLowLimit = priceLowLimit;
    }

    @Override
    public String toString() {
        return "[ADDSYMBOL " + symbolName + " " + symbolId + "]";
    }
}
