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
package exchange.core2.core.utils;

import exchange.core2.collections.affinity.CpuAffinity;
import exchange.core2.core.processors.TwoStepSlaveProcessor;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.IntStream;

public final class AffinityThreadFactory implements ThreadFactory {

    private static final Logger log = LoggerFactory.getLogger(AffinityThreadFactory.class);

    private final ThreadAffinityMode threadAffinityMode;
    private final int[] logicalCpus;
    private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

    public AffinityThreadFactory(ThreadAffinityMode threadAffinityMode) {
        this.threadAffinityMode = threadAffinityMode;
        logicalCpus = threadAffinityMode == ThreadAffinityMode.THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE
                ? IntStream.concat(highestFirst(CpuAffinity.reservedCpus()), highestFirst(CpuAffinity.spareCpus())).toArray()
                : new int[0];
    }

    @Override
    public Thread newThread(@NotNull Runnable runnable) {
        // Risk release runs on the corresponding risk master after marking the slave ready.
        if (threadAffinityMode == ThreadAffinityMode.THREAD_AFFINITY_DISABLE
                || runnable instanceof TwoStepSlaveProcessor) {
            return defaultFactory.newThread(runnable);
        }

        final CompletableFuture<Void> affinityReady = new CompletableFuture<>();
        return new Thread(() -> executePinned(runnable, affinityReady)) {
            @Override
            public void start() {
                super.start();
                // Disruptor starts processors in registration order. Wait for each reservation
                // before it starts the next processor so OS scheduling cannot reorder the pins.
                affinityReady.join();
            }
        };
    }

    private void executePinned(Runnable runnable, CompletableFuture<Void> affinityReady) {
        try (CpuAffinity affinity = acquireAffinity()) {
            if (affinity != null && affinity.isPinned()) {
                final Thread thread = Thread.currentThread();
                thread.setName(thread.getName() + "-cpu" + affinity.cpu());
                log.debug("{} will be running on thread={} pinned to cpu {}",
                        runnable, thread.getName(), affinity.cpu());
            }
            affinityReady.complete(null);
            runnable.run();
        } catch (RuntimeException | Error failure) {
            affinityReady.completeExceptionally(failure);
            throw failure;
        }
    }

    private CpuAffinity acquireAffinity() {
        if (threadAffinityMode == ThreadAffinityMode.THREAD_AFFINITY_ENABLE_PER_PHYSICAL_CORE) {
            return CpuAffinity.acquireCore();
        }
        for (int cpu : logicalCpus) {
            final CpuAffinity affinity;
            try {
                affinity = CpuAffinity.acquireCpu(cpu);
            } catch (IllegalStateException occupied) {
                continue;
            }
            if (affinity.isPinned()) {
                return affinity;
            }
            affinity.close();
        }
        if (logicalCpus.length > 0) {
            log.warn("No configured logical CPU available for {}", Thread.currentThread().getName());
        }
        return null;
    }

    private static IntStream highestFirst(BitSet cpus) {
        return IntStream.iterate(cpus.length() - 1, cpu -> cpu >= 0, cpu -> cpus.previousSetBit(cpu - 1));
    }

    public enum ThreadAffinityMode {
        THREAD_AFFINITY_ENABLE_PER_PHYSICAL_CORE,
        THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE,
        THREAD_AFFINITY_DISABLE
    }
}
