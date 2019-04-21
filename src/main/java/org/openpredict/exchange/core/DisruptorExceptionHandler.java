package org.openpredict.exchange.core;

import com.lmax.disruptor.ExceptionHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.function.BiConsumer;

@Slf4j
@RequiredArgsConstructor
public final class DisruptorExceptionHandler<T> implements ExceptionHandler<T> {

    public final String name;
    public final BiConsumer<Throwable, Long> onException;

    @Override
    public void handleEventException(Throwable ex, long sequence, T event) {
        log.debug("Disruptor '{}' seq={} caught exception: {}", name, sequence, event, ex);
        onException.accept(ex, sequence);
    }

    @Override
    public void handleOnStartException(Throwable ex) {
        log.debug("Disruptor '{}' startup exception: {}", name, ex);
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
        log.debug("Disruptor '{}' shutdown exception: {}", name, ex);
    }
}
