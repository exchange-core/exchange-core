package org.openpredict.exchange.core;

import com.lmax.disruptor.ExceptionHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.function.BiConsumer;

@Slf4j
@RequiredArgsConstructor
public class DisruptorExceptionHandler<T> implements ExceptionHandler<T> {

    public final String name;
    public final BiConsumer<Throwable, Long> onException;

    @Override
    public void handleEventException(Throwable ex, long sequence, T event) {
        log.debug("Disruptor '{}' caught exception: {}", name, event, ex);
        ex.printStackTrace();
        onException.accept(ex, sequence);
    }

    @Override
    public void handleOnStartException(Throwable ex) {
        log.debug("Disruptor '{}' startup exception: {}", name, ex);
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {

    }
}
