package org.openpredict.exchange.beans;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum ExchangeErrorCode {

    NO_ERROR(0),

    REJECTED_UNKNOWN_SYMBOL(4023),
    REJECTED_RE_NSF(4120),


    CANCEL_FAILED_UNKNOWN_ORDER(5030);

    @Getter
    private int code;


}
