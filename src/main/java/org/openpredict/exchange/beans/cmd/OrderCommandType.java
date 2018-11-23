package org.openpredict.exchange.beans.cmd;

import lombok.Getter;

import java.util.Arrays;

@Getter
public enum OrderCommandType {
    PLACE_ORDER(1),
    CANCEL_ORDER(2),
    MOVE_ORDER(3),

    ORDER_BOOK_REQUEST(6),

    ADD_USER(10), // TODO rename to account
    BALANCE_ADJUSTMENT(11),

    CLEARING_OPERATION(30),

    ADD_SYMBOL(50),

    NOP(127);

    private byte code;

    OrderCommandType(int code) {
        this.code = (byte) code;
    }

    public static OrderCommandType valueOf(byte code) {
        if (code == 1) {
            return PLACE_ORDER;
        } else if (code == 2) {
            return CANCEL_ORDER;
        } else if (code == 3) {
            return MOVE_ORDER;
        } else {
            return Arrays.stream(values())
                    .filter(c -> c.code == code)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("No such command" + code));
        }
    }
}
