package org.openpredict.exchange.beans;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.ToString;

import java.io.Serializable;

@Builder
@AllArgsConstructor
@ToString
public final class CoreSymbolSpecification implements Serializable {

    public final int symbolId;

    @NonNull
    public final SymbolType type;

    // currency pair specification
    public final int baseCurrency;  // base currency
    public final int quoteCurrency; // quote/counter currency (OR futures contract currency)

    public final long baseScaleK;   // base currency amount multiplier
    public final long quoteScaleK;  // quote currency amount multiplier

    // deposit settings
    public final long depositBuy;
    public final long depositSell;

    // commissions (in base currency)
    public final long takerCommission; // = 0;
    public final long makerCommission; // = 0;

    // lot size
//    public final long lotSize;
    public final int stepSize; // TODO validate in OrderBook (place limit / move)

    // order book limits
//    public final long highLimit;
//    public final long lowLimit;

    // swaps
//    public final long longSwap;// = 0;
//    public final long shortSwap;// = 0;

}
