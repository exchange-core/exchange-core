package org.openpredict.exchange.beans;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum PositionDirection {
    LONG(1),
    SHORT(-1),
    EMPTY(0);

    @Getter
    private int multiplier;

    public static PositionDirection of(OrderAction action) {
        return action == OrderAction.BID ? LONG : SHORT;
    }

    public static PositionDirection of(byte code) {
        switch (code) {
            case 1:
                return LONG;
            case -1:
                return SHORT;
            case 0:
                return EMPTY;
            default:
                throw new IllegalArgumentException("unknown PositionDirection:" + code);
        }
    }


    public boolean isOppositeToAction(OrderAction action) {
        return (this == PositionDirection.LONG && action == OrderAction.ASK) || (this == PositionDirection.SHORT && action == OrderAction.BID);
    }

    public boolean isSameAsAction(OrderAction action) {
        return (this == PositionDirection.LONG && action == OrderAction.BID) || (this == PositionDirection.SHORT && action == OrderAction.ASK);
    }

}
