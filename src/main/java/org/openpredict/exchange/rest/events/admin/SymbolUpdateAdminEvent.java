package org.openpredict.exchange.rest.events.admin;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;


@Getter
@Setter
@Builder
@AllArgsConstructor
public final class SymbolUpdateAdminEvent {
    private final String msgType = "adm_symbol_update";

    private final int symbolId;
    private final String symbolName;

    // unmodifiable properties (gateway level)
    private final int priceStep; // price % priceStep == 0
    private final int priceScale; // decimal point position
    private final int lotSize;


    // modifiable properties (core level)
    // deposit settings
    private final long depositBuy;
    private final long depositSell;

    // order book limits
    private final long priceHighLimit;
    private final long priceLowLimit;
}

