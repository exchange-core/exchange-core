package org.openpredict.exchange.beans;

public interface IOrder {

    long getPrice();

    long getSize();

    long getUid();

    OrderAction getAction();

    long getOrderId();

    long getTimestamp();

    long getReserveBidPrice();

}
