package org.openpredict.exchange.beans;


import lombok.*;

import java.io.Serializable;

@Builder
@AllArgsConstructor
@Getter
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

    // deposit settings (for type=FUTURES_CONTRACT only)
    public final long depositBuy;   // buy margin (quote currency)
    public final long depositSell;  // sell margin (quote currency)

    // fees (per lot)
    public final long takerFee;
    public final long makerFee;
    // TODO public final int feeCurrency; //  if type=CURRENCY_EXCHANGE_PAIR - should be the same as quoteCurrency


/* NOT SUPPORTED YET:

    // lot size
//    public final long lotSize;
//    public final int stepSize;

    // order book limits
//    public final long highLimit;
//    public final long lowLimit;

    // swaps
//    public final long longSwap;
//    public final long shortSwap;

// activity (inactive, active, expired)

  */


}
