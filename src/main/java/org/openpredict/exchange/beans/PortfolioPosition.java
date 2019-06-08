package org.openpredict.exchange.beans;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum PortfolioPosition {
    LONG(1),
    SHORT(-1),
    EMPTY(0);

    @Getter
    private int multiplier;

    public static PortfolioPosition of(OrderAction action) {
        return action == OrderAction.BID ? LONG : SHORT;
    }

    public static PortfolioPosition of(byte code) {
        switch (code) {
            case 1:
                return LONG;
            case -1:
                return SHORT;
            case 0:
                return EMPTY;
            default:
                throw new IllegalArgumentException("unknown PortfolioPosition:" + code);
        }
    }


    public boolean isOppositeToAction(OrderAction action) {
        return (this == PortfolioPosition.LONG && action == OrderAction.ASK) || (this == PortfolioPosition.SHORT && action == OrderAction.BID);
    }

    public boolean isSameAsAction(OrderAction action) {
        return (this == PortfolioPosition.LONG && action == OrderAction.BID) || (this == PortfolioPosition.SHORT && action == OrderAction.ASK);
    }

}
