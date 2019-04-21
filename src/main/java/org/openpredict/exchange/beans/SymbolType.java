package org.openpredict.exchange.beans;

import lombok.Getter;

import java.util.Arrays;

@Getter
public enum SymbolType {
    CURRENCY_EXCHANGE_PAIR(0),
    FUTURES_CONTRACT(1),
    OPTION(2);

    private byte code;

    SymbolType(int code) {
        this.code = (byte) code;
    }

    public static SymbolType of(int code) {
        return Arrays.stream(values())
                .filter(c -> c.code == (byte) code)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("unknown SymbolType code: " + code));
    }
}
