package org.openpredict.exchange.beans;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SymbolSpecification {

    public int symbolId;
    public String symbolName;

    public long depositBuy;
    public long depositSell;

    public long highLimit;
    public long lowLimit;

    public int priceStep;
    public int priceScale;
    public int lotSize;

}
