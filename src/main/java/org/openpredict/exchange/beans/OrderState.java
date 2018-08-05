package org.openpredict.exchange.beans;

public enum OrderState {
    PENDING,
    OPEN,
    EXPIRED,
    CANCEL,
    FILLED,
    PARTIAL,
    CONTRAGENT_CANCEL,
    SYSTEM_REJECT,
    SYSTEM_CANCEL;
}
