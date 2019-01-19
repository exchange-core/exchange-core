package org.openpredict.exchange.beans.cmd;

import lombok.Getter;

@Getter
public enum OrderCommandType {
    PLACE_ORDER(1),
    CANCEL_ORDER(2),
    MOVE_ORDER(3),

    ORDER_BOOK_REQUEST(6),

    ADD_USER(10),
    BALANCE_ADJUSTMENT(11),

    CLEARING_OPERATION(30),

    ADD_SYMBOL(50),
    UPDATE_SYMBOL(51),

    NOP(120),
    RESET(124),
    SHUTDOWN_SIGNAL(127);

    private byte code;

    OrderCommandType(int code) {
        this.code = (byte) code;
    }

}
