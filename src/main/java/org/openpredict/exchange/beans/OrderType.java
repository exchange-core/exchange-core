package org.openpredict.exchange.beans;

import lombok.Getter;

@Getter
public enum OrderType {
    GTC(0), // Good till Cancel - equivalent to regular limit order
    IOC(1); // Immediate or Cancel - equivalent to strict-risk market order

    private byte code;

    OrderType(int code) {
        this.code = (byte) code;
    }

    public static OrderType of(byte code) {
        switch (code) {
            case 0:
                return GTC;
            case 1:
                return IOC;
            default:
                throw new IllegalArgumentException("unknown OrderType:" + code);
        }
    }

}
