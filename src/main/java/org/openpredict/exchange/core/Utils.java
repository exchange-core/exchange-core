package org.openpredict.exchange.core;

import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.eclipse.collections.api.map.primitive.MutableLongIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.util.BitSet;
import java.util.Map;
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

    public static void marshallBitSet(final BitSet bitSet, final BytesOut bytes) {

        long[] longs = bitSet.toLongArray();

        bytes.writeInt(longs.length);

        for (long word : longs) {
            bytes.writeLong(word);
        }
    }


    public static void marshallMutableLongIntMap(final MutableLongIntMap hashMap, final BytesOut bytes) {

        bytes.writeInt(hashMap.size());

        hashMap.forEachKeyValue((k, v) -> {
            bytes.writeLong(k);
            bytes.writeInt(v);
        });
    }

    public static <T extends WriteBytesMarshallable> void marshallLongHashMap(final LongObjectHashMap<T> hashMap, final BytesOut bytes) {

        bytes.writeInt(hashMap.size());

        hashMap.forEachKeyValue((k, v) -> {
            bytes.writeLong(k);
            v.writeMarshallable(bytes);
        });

    }

    public static <T extends WriteBytesMarshallable> void marshallIntHashMap(final IntObjectHashMap<T> hashMap, final BytesOut bytes) {

        bytes.writeInt(hashMap.size());

        hashMap.forEachKeyValue((k, v) -> {
            bytes.writeInt(k);
            v.writeMarshallable(bytes);
        });
    }

    public static <T extends WriteBytesMarshallable> void marshallLongMap(final Map<Long, T> map, final BytesOut bytes) {
        bytes.writeInt(map.size());

        map.forEach((k, v) -> {
            bytes.writeLong(k);
            v.writeMarshallable(bytes);
        });
    }

}
