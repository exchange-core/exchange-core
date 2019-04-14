package org.openpredict.exchange.beans;

import lombok.Getter;

@Getter
public enum SymbolType {
    CURRENCY_EXCHANGE_PAIR(0),
    FUTURES_CONTRACT(1),
    OPTION(2);

    private byte code;

    SymbolType(int code) {
        this.code = (byte) code;
    }

}
