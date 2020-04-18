package exchange.core2.tests.util;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

@Slf4j
@RequiredArgsConstructor
public class ExecutionTime implements AutoCloseable {

    private final Consumer<String> executionTimeConsumer;
    private final long startNs = System.nanoTime();

    @Getter
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
}
