package org.openpredict.exchange.beans;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum PortfolioPosition {
    EMPTY(0),
    LONG(1),
    SHORT(-1);

    @Getter
    private int multiplier;

    public static PortfolioPosition of(OrderAction action) {
        return action == OrderAction.BID ? LONG : SHORT;
    }

    public boolean isOppositeToAction(OrderAction action) {
        return (this == PortfolioPosition.LONG && action == OrderAction.ASK) || (this == PortfolioPosition.SHORT && action == OrderAction.BID);
    }

}
