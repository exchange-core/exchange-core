package exchange.core2.tests.util;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Consumer;

@Slf4j
@RequiredArgsConstructor
public class ExecutionTime implements AutoCloseable {

    private final Consumer<String> executionTimeConsumer;
    private final long startNs = System.nanoTime();

    public ExecutionTime() {
        this.executionTimeConsumer = s -> {
        };
    }

    @Override
    public void close() {
        executionTimeConsumer.accept(getTimeFormatted());
    }

    public String getTimeFormatted() {
        return LatencyTools.formatNanos(System.nanoTime() - startNs);
    }

}
