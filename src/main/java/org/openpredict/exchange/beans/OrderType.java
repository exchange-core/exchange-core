package org.openpredict.exchange.beans;

import lombok.Getter;

@Getter
public enum OrderType {
    LIMIT(0),
    MARKET(1);

    private byte code;

    OrderType(int code) {
        this.code = (byte) code;
    }
}
