package org.openpredict.exchange.core;

import org.openpredict.exchange.beans.Order;
import org.openpredict.exchange.beans.cmd.OrderCommand;


public interface MatcherEventsCallback {


    void sendTradeEvent(OrderCommand activeOrder, Order matchingOrder, boolean fm, boolean fma, int price, long v);

    void sendReduceEvent(Order order, long reducedBy);

    void sendRejectEvent(OrderCommand order, long filledSize);

}
