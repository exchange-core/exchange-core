package org.openpredict.exchange.beans;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Getter
public final class SymbolSpecification {

    private int symbolId;

    private long depositBuy = Long.MAX_VALUE;
    private long depositSell = Long.MAX_VALUE;

    private long highLimit = Long.MAX_VALUE;
    private long lowLimit = 0;

    private int priceStep = 1;
    private int priceScale = 1;
    private int lotSize = 1;

    private SymbolStatus status = SymbolStatus.INACTIVE;

}
