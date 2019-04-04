package org.openpredict.exchange.beans.api;


import lombok.Builder;

@Builder
public class ApiAddSymbol extends ApiCommand {

    public int symbolId;
    public long depositBuy;
    public long depositSell;
    public long priceLowLimit;
    public long priceHighLimit;

    @Override
    public String toString() {
        return "[ADDSYMBOL " + symbolId + "]";
    }
}
