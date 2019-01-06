package org.openpredict.exchange.rest.events;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;


@Getter
@Setter
@AllArgsConstructor
public final class OrderBookEvent {
    private final String msgType = "orderbook";
    private String symbol;
    private long timestamp;
    private long[] askPrices;
    private long[] askVolumes;
    private long[] bidPrices;
    private long[] bidVolumes;
}

