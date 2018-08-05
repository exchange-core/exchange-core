package org.openpredict.exchange.tests.util;

import com.lmax.disruptor.EventTranslator;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Supplier;

/**
 * For testing - multi threaded (ABQ backed)
 *
 * @param <E>
 */
public class QueuedEventSink<E> extends TestEventSink<E> {

    private final Queue<E> queue;

    private final Supplier<E> supplier;

    public QueuedEventSink(Supplier<E> supplier, int capacity) {
        super(null);
        this.supplier = supplier;
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    public E pollElement() {
        return queue.poll();
    }

    /**
     * @param translator
     * @throws IllegalStateException if no space left (capacity exceeded)
     */
    @Override
    public void publishEvent(EventTranslator<E> translator) {
        E event = supplier.get();
        translator.translateTo(event, 0L);
        queue.add(event);
    }

    public List<E> pollAllElements() {
        List<E> res = new ArrayList<>(queue.size());
        res.addAll(queue);
        queue.clear();
        return res;
    }

}
