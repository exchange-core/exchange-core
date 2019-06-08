package org.openpredict.exchange.core.journalling;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.function.Function;

public interface ISerializationProcessor {

    boolean storeData(long snapshotId, SerializedModuleType type, int instanceId, WriteBytesMarshallable obj);

    <T> T loadData(long snapshotId, SerializedModuleType type, int instanceId, Function<BytesIn, T> initFunc);

    enum SerializedModuleType {
        RISK_ENGINE,
        MATCHING_ENGINE_ROUTER
    }

}
