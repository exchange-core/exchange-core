package org.openpredict.exchange.beans;

import lombok.Getter;

@Getter
public enum OrderAction {
    ASK(0),
    BID(1);

    private final byte code;

    OrderAction(int code) {
        this.code = (byte) code;
    }

    public OrderAction opposite() {
        return this == ASK ? BID : ASK;
    }

    public static OrderAction valueOf(long v) {
        return v == 0 ? ASK : BID;
    }

}
