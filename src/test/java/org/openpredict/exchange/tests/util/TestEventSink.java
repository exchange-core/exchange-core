package org.openpredict.exchange.tests.util;

import com.lmax.disruptor.*;


/**
 * Queue implementation of com.lmax.disruptor
 * For testing
 * <p>
 * For single thread (contended)
 */
public class TestEventSink<E> implements EventSink<E> {

    private final E singleEventContainer;

    public TestEventSink(E container) {
        singleEventContainer = container;
    }

    @Override
    public void publishEvent(EventTranslator<E> translator) {
        translator.translateTo(singleEventContainer, 0L);
    }

    @Override
    public boolean tryPublishEvent(EventTranslator<E> translator) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A> void publishEvent(EventTranslatorOneArg<E, A> translator, A arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A> boolean tryPublishEvent(EventTranslatorOneArg<E, A> translator, A arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A, B> void publishEvent(EventTranslatorTwoArg<E, A, B> translator, A arg0, B arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A, B> boolean tryPublishEvent(EventTranslatorTwoArg<E, A, B> translator, A arg0, B arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A, B, C> void publishEvent(EventTranslatorThreeArg<E, A, B, C> translator, A arg0, B arg1, C arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A, B, C> boolean tryPublishEvent(EventTranslatorThreeArg<E, A, B, C> translator, A arg0, B arg1, C arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void publishEvent(EventTranslatorVararg<E> translator, Object... args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryPublishEvent(EventTranslatorVararg<E> translator, Object... args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void publishEvents(EventTranslator<E>[] translators) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void publishEvents(EventTranslator<E>[] translators, int batchStartsAt, int batchSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryPublishEvents(EventTranslator<E>[] translators) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryPublishEvents(EventTranslator<E>[] translators, int batchStartsAt, int batchSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A> void publishEvents(EventTranslatorOneArg<E, A> translator, A[] arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A> void publishEvents(EventTranslatorOneArg<E, A> translator, int batchStartsAt, int batchSize, A[] arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A> boolean tryPublishEvents(EventTranslatorOneArg<E, A> translator, A[] arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A> boolean tryPublishEvents(EventTranslatorOneArg<E, A> translator, int batchStartsAt, int batchSize, A[] arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A, B> void publishEvents(EventTranslatorTwoArg<E, A, B> translator, A[] arg0, B[] arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A, B> void publishEvents(EventTranslatorTwoArg<E, A, B> translator, int batchStartsAt, int batchSize, A[] arg0, B[] arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A, B> boolean tryPublishEvents(EventTranslatorTwoArg<E, A, B> translator, A[] arg0, B[] arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A, B> boolean tryPublishEvents(EventTranslatorTwoArg<E, A, B> translator, int batchStartsAt, int batchSize, A[] arg0, B[] arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A, B, C> void publishEvents(EventTranslatorThreeArg<E, A, B, C> translator, A[] arg0, B[] arg1, C[] arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A, B, C> void publishEvents(EventTranslatorThreeArg<E, A, B, C> translator, int batchStartsAt, int batchSize, A[] arg0, B[] arg1, C[] arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A, B, C> boolean tryPublishEvents(EventTranslatorThreeArg<E, A, B, C> translator, A[] arg0, B[] arg1, C[] arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A, B, C> boolean tryPublishEvents(EventTranslatorThreeArg<E, A, B, C> translator, int batchStartsAt, int batchSize, A[] arg0, B[] arg1, C[] arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void publishEvents(EventTranslatorVararg<E> translator, Object[]... args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void publishEvents(EventTranslatorVararg<E> translator, int batchStartsAt, int batchSize, Object[]... args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryPublishEvents(EventTranslatorVararg<E> translator, Object[]... args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryPublishEvents(EventTranslatorVararg<E> translator, int batchStartsAt, int batchSize, Object[]... args) {
        throw new UnsupportedOperationException();
    }
}
