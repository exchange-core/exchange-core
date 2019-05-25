package org.openpredict.exchange.core.journalling;

import net.openhft.chronicle.bytes.WriteBytesMarshallable;

public interface ISerializationProcessor {

    void storeData(long dumpId, SerializedModuleType type, int instanceNum, WriteBytesMarshallable obj);

    enum SerializedModuleType {
        RISK_ENGINE,
        MATCHING_ENGINE_ROUTER
    }

}
