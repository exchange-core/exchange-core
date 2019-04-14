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

    public SymbolType type =  SymbolType.FUTURES_CONTRACT;

    // currency pair specification
    public int baseCurrency;    // base currency
    public int counterCurrency; // quote/counter currency (OR futures contract currency)

    // lot size
    public long lotSize = 1; // = 1;

    public long counterLotScaleK = 1;
    public long baseLotScaleK = 1;
    // TODO price step?

    // deposit settings
    public long depositBuy;
    public long depositSell;

    // order book limits
    public long highLimit;
    public long lowLimit;

    // commissions (in base currency)
    public long takerCommission; // = 0;
    public long makerCommission; // = 0;

    // swaps
    public long longSwap;// = 0;
    public long shortSwap;// = 0;

    public long lastAskPrice; // = Long.MAX_VALUE
    public long lastBidPrice; // = 0

}
