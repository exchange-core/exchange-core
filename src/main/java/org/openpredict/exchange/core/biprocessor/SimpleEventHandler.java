package org.openpredict.exchange.core.biprocessor;

public interface SimpleEventHandler<T> {

    void onEvent(T event);

}
