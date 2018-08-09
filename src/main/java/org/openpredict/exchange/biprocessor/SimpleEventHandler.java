package org.openpredict.exchange.biprocessor;

public interface SimpleEventHandler<T> {

    void onEvent(T event);

}
