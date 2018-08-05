package org.openpredict.exchange.beans;

public enum MatcherEventType {
    TRADE, // regular type of the event

    // Can happen only when MARKET order has to be rejected by Matcher Engine due lack of liquidity
    // That basically means no ASK (or BID) orders left in the order book for any price.
    // Before being rejected active order can partially filled though.
    REJECTION,

    // After cancel order or reduced its size - risk engine has to unlock deposit accordingly
    REDUCE
}
