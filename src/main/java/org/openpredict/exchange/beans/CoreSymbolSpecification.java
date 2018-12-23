package org.openpredict.exchange.beans;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CoreSymbolSpecification {

    public int symbolId;

    // deposit settings
    public long depositBuy;
    public long depositSell;

    // order book limits
    public long highLimit;
    public long lowLimit;

}
