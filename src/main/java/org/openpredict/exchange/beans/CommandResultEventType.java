package org.openpredict.exchange.beans;

import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommandType;

public enum CommandResultEventType {

    PLACE_SUCCEEDED,
    PLACE_FAILED,

    UPDATE_SUCCEEDED,
    UPDATE_FAILED,

    CANCEL_SUCCEEDED,
    CANCEL_FAILED,

    UNKNOWN;

    public static CommandResultEventType of(OrderCommandType commandType, CommandResultCode resultCode) {
        switch (commandType) {
            case PLACE_ORDER:
                return resultCode == CommandResultCode.SUCCESS ? PLACE_SUCCEEDED : PLACE_FAILED;

            case MOVE_ORDER:
                return resultCode == CommandResultCode.SUCCESS ? UPDATE_SUCCEEDED : UPDATE_FAILED;
            case CANCEL_ORDER:
                return resultCode == CommandResultCode.SUCCESS ? CANCEL_SUCCEEDED : CANCEL_FAILED;
        }

        return UNKNOWN;

    }

}
