package org.openpredict.exchange.beans;


import lombok.AllArgsConstructor;
import lombok.Builder;

@Builder
@AllArgsConstructor
public class GatewaySymbolSpecification {

    public final int symbolId;

    public final String symbolName;

    public final int priceStep; // price % priceStep == 0
    public final int priceScale; // decimal point position
    public final int lotSize;

    // TODO lifecycle - new, active, ceased

}
