package org.openpredict.exchange.beans;

import lombok.Getter;

@Getter
public enum OrderAction {
    ASK(0),
    BID(1);

    private byte code;

    OrderAction(int code) {
        this.code = (byte) code;
    }

    public OrderAction opposite() {
        return this == ASK ? BID : ASK;
    }

}
