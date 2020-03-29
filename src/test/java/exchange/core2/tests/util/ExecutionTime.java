package exchange.core2.tests.util;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Consumer;

@Slf4j
@RequiredArgsConstructor
public class ExecutionTime implements AutoCloseable {

    private final Consumer<String> executionTimeConsumer;
    private final long startNs = System.nanoTime();

    @Override
    public void close() {
        executionTimeConsumer.accept(
                LatencyTools.formatNanos(
                        System.nanoTime() - startNs));
    }
}
