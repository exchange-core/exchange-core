package org.openpredict.exchange.beans;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

@Builder
@NoArgsConstructor
@AllArgsConstructor
public final class CoreSymbolSpecification {

    // TODO make immutable (copy on write)??

    public int symbolId;

    // deposit settings
    public long depositBuy;
    public long depositSell;

    // order book limits
    public long highLimit;
    public long lowLimit;

    // commissions
    public long takerCommission = 0;
    public long makerCommission = 0;

    // swaps
    public long longSwap = 0;
    public long shortSwap = 0;


    // TODO collect ask/bin from L2, user deposit when unknown
    public long lastPrice;
}
