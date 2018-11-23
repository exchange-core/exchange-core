package org.openpredict.exchange.beans;

import lombok.Getter;

@Getter
public enum SymbolStatus {
    INACTIVE(0),
    ACTIVE(1),
    EVICTING(2);

    private byte code;

    SymbolStatus(int code) {
        this.code = (byte) code;
    }
}
