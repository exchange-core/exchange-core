package org.openpredict.exchange.core;

import org.openpredict.exchange.beans.Order;

@FunctionalInterface
public interface TradeEventCallback {

    void submit(Order order, long volume, boolean fullMatch, boolean fullMatchForActiveOrder);


    static void empty(Order order, long volume, boolean fullMatch, boolean fullMatchForActiveOrder) {
    }
}
