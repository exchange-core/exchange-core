package exchange.core2.tests.util;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class ExecutionTime implements AutoCloseable {

    private final Consumer<String> executionTimeConsumer;
    private final long startNs = System.nanoTime();

    private final CompletableFuture<Long> resultNs = new CompletableFuture<>();

    public ExecutionTime() {
        this.executionTimeConsumer = s -> {
        };
    }

    @Override
    public void close() {
        executionTimeConsumer.accept(getTimeFormatted());
    }

    public String getTimeFormatted() {
        if (!resultNs.isDone()) {
            resultNs.complete(System.nanoTime() - startNs);
        }
        return LatencyTools.formatNanos(resultNs.join());
    }

    public ExecutionTime(Consumer<String> executionTimeConsumer) {
        this.executionTimeConsumer = executionTimeConsumer;
    }

    public CompletableFuture<Long> getResultNs() {
        return this.resultNs;
    }
}
