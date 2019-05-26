package org.openpredict.exchange.core;

import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.eclipse.collections.api.map.primitive.MutableIntLongMap;
import org.eclipse.collections.api.map.primitive.MutableLongIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongIntHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.util.BitSet;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
public final class Utils {

    public static int requiredLongArraySize(final int bytesLength) {
        return ((bytesLength - 1) >> 3) + 1;
    }


    public enum ThreadAffityMode {
        THREAD_AFFINITY_ENABLE_PER_PHYSICAL_CORE,
        THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE,
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
        marshallLongArray(bitSet.toLongArray(), bytes);
    }

    public static BitSet readBitSet(final BytesIn bytes) {
        // TODO use LongBuffer
        return BitSet.valueOf(readLongArray(bytes));
    }


    public static void marshallLongArray(final long[] longs, final BytesOut bytes) {
        bytes.writeInt(longs.length);
        for (long word : longs) {
            bytes.writeLong(word);
        }
    }

    public static long[] readLongArray(final BytesIn bytes) {
        final int length = bytes.readInt();
        final long[] array = new long[length];
        // TODO read byte[], then convert into long[]
        for (int i = 0; i < length; i++) {
            array[i] = bytes.readLong();
        }
        return array;
    }

    public static void marshallLongIntHashMap(final MutableLongIntMap hashMap, final BytesOut bytes) {

        bytes.writeInt(hashMap.size());
        hashMap.forEachKeyValue((k, v) -> {
            bytes.writeLong(k);
            bytes.writeInt(v);
        });
    }

    public static LongIntHashMap readLongIntHashMap(final BytesIn bytes) {
        int length = bytes.readInt();
        final LongIntHashMap hashMap = new LongIntHashMap(length);
        // TODO shuffle (? performance can be reduced if populating linearly)
        for (int i = 0; i < length; i++) {
            long k = bytes.readLong();
            int v = bytes.readInt();
            hashMap.put(k, v);
        }
        return hashMap;
    }

    public static void marshallIntLongHashMap(final MutableIntLongMap hashMap, final BytesOut bytes) {

        bytes.writeInt(hashMap.size());

        hashMap.forEachKeyValue((k, v) -> {
            bytes.writeInt(k);
            bytes.writeLong(v);
        });
    }

    public static IntLongHashMap readIntLongHashMap(final BytesIn bytes) {
        int length = bytes.readInt();
        final IntLongHashMap hashMap = new IntLongHashMap(length);
        // TODO shuffle (? performance can be reduced if populating linearly)
        for (int i = 0; i < length; i++) {
            int k = bytes.readInt();
            long v = bytes.readLong();
            hashMap.put(k, v);
        }
        return hashMap;
    }


    public static void marshallLongHashSet(final LongHashSet set, final BytesOut bytes) {
        bytes.writeInt(set.size());
        set.forEach(bytes::writeLong);
    }

    public static LongHashSet readLongHashSet(final BytesIn bytes) {
        int length = bytes.readInt();
        final LongHashSet set = new LongHashSet(length);
        // TODO shuffle (? performance can be reduced if populating linearly)
        for (int i = 0; i < length; i++) {
            set.add(bytes.readLong());
        }
        return set;
    }


    public static <T extends WriteBytesMarshallable> void marshallLongHashMap(final LongObjectHashMap<T> hashMap, final BytesOut bytes) {

        bytes.writeInt(hashMap.size());

        hashMap.forEachKeyValue((k, v) -> {
            bytes.writeLong(k);
            v.writeMarshallable(bytes);
        });

    }

    public static <T> LongObjectHashMap<T> readLongHashMap(final BytesIn bytes, Function<BytesIn, T> creator) {
        int length = bytes.readInt();
        final LongObjectHashMap<T> hashMap = new LongObjectHashMap<>(length);
        for (int i = 0; i < length; i++) {
            hashMap.put(bytes.readLong(), creator.apply(bytes));
        }
        return hashMap;
    }

    public static <T extends WriteBytesMarshallable> void marshallIntHashMap(final IntObjectHashMap<T> hashMap, final BytesOut bytes) {

        bytes.writeInt(hashMap.size());

        hashMap.forEachKeyValue((k, v) -> {
            bytes.writeInt(k);
            v.writeMarshallable(bytes);
        });
    }

    public static <T> IntObjectHashMap<T> readIntHashMap(final BytesIn bytes, Function<BytesIn, T> creator) {
        int length = bytes.readInt();
        final IntObjectHashMap<T> hashMap = new IntObjectHashMap<>(length);
        for (int i = 0; i < length; i++) {
            hashMap.put(bytes.readInt(), creator.apply(bytes));
        }
        return hashMap;
    }


    public static <T extends WriteBytesMarshallable> void marshallLongMap(final Map<Long, T> map, final BytesOut bytes) {
        bytes.writeInt(map.size());

        map.forEach((k, v) -> {
            bytes.writeLong(k);
            v.writeMarshallable(bytes);
        });
    }

    public static <T, M extends Map<Long, T>> M readLongMap(final BytesIn bytes, Supplier<M> mapSupplier, Function<BytesIn, T> creator) {
        int length = bytes.readInt();
        final M map = mapSupplier.get();
        for (int i = 0; i < length; i++) {
            map.put(bytes.readLong(), creator.apply(bytes));
        }
        return map;
    }

}
