package org.openpredict.exchange.beans;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
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
    public long takerCommission; // = 0;
    public long makerCommission; // = 0;

    // swaps
    public long longSwap;// = 0;
    public long shortSwap;// = 0;

    public long lastAskPrice; // = Long.MAX_VALUE
    public long lastBidPrice; // = 0

}
