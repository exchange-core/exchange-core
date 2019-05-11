package org.openpredict.exchange.core;

import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

@Slf4j
public final class Utils {

    public static int requiredLongArraySize(final int bytesLength) {
        return ((bytesLength - 1) >> 3) + 1;
    }


    public enum ThreadAffityMode {
        THREAD_AFFINITY_ENABLE_PER_PHYSICAL_CORE,
        THREAD_AFFINITY_ENABLE_PER_LOGICAL_CODE,
        THREAD_AFFINITY_DISABLE
    }

    public static ThreadFactory affinedThreadFactory(final ThreadAffityMode threadAffityMode) {

        if (threadAffityMode == ThreadAffityMode.THREAD_AFFINITY_DISABLE) {
            return Executors.defaultThreadFactory();

        } else {
            final Supplier<AffinityLock> lockSupplier = threadAffityMode == ThreadAffityMode.THREAD_AFFINITY_ENABLE_PER_PHYSICAL_CORE
                    ? AffinityLock::acquireCore
                    : AffinityLock::acquireLock;

            return eventProcessor -> new Thread(() -> {
                try (AffinityLock lock = lockSupplier.get()) {
                    log.debug("{} pinned to {}", Thread.currentThread(), lock.cpuId());
                    eventProcessor.run();
                }
            });
        }
    }


}
