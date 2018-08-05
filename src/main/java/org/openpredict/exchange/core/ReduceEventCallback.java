package org.openpredict.exchange.core;

import org.openpredict.exchange.beans.Order;

@FunctionalInterface
public interface ReduceEventCallback {

    void submit(Order order, long reducedBy);

    static void empty(Order order, long reducedBy) {
    }
}
