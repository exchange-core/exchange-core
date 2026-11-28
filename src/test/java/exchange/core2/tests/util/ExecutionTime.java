/*
 * Copyright 2019-2026 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
