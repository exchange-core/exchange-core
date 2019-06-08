package org.openpredict.exchange.core.biprocessor;

public interface SimpleEventHandler<T> {

    /**
     * @param event - event
     * @return true to forcibly publish sequence (batches)
     */
    boolean onEvent(T event);

}
