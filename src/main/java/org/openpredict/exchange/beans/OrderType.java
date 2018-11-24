package org.openpredict.exchange.beans;

import lombok.Getter;

@Getter
public enum OrderType {
    LIMIT(0),
    MARKET(1);

    private final byte code;

    OrderType(int code) {
        this.code = (byte) code;
    }

    public static OrderType valueOf(long v) {
        return v == 0 ? LIMIT : MARKET;
    }

}
