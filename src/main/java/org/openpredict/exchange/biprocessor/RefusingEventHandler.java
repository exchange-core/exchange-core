package org.openpredict.exchange.biprocessor;

public interface RefusingEventHandler<T> {

    void onEvent(T event, long seq);
}
