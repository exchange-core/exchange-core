package org.openpredict.exchange.beans;

import lombok.Getter;

@Getter
public enum OrderType {
    LIMIT(0),
    MARKET(1); // TODO deprecate

    private byte code;

    OrderType(int code) {
        this.code = (byte) code;
    }

    public static OrderType of(byte code) {
        switch (code) {
            case 0:
                return LIMIT;
            case 1:
                return MARKET;
            default:
                throw new IllegalArgumentException("unknown OrderType:" + code);
        }
    }

}
