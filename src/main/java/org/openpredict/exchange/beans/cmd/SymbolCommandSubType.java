package org.openpredict.exchange.beans.cmd;

import lombok.Getter;

import java.util.Arrays;

@Getter
public enum SymbolCommandSubType {
    ADD_SYMBOL(1),
    UPDATE_SYMBOL_STATUS(2),
    UPDATE_SYMBOL_LIMITS(3),
    REMOVE_SYMBOL(4);

    private final byte code;

    SymbolCommandSubType(int code) {
        this.code = (byte) code;
    }

    public static SymbolCommandSubType valueOf(byte code) {
        return Arrays.stream(values())
                .filter(c -> c.code == code)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No such command subtype: " + code));
    }
}
