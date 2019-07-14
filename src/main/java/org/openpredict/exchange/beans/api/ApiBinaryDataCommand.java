package org.openpredict.exchange.beans.api;


import lombok.Builder;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;


@Builder
public final class ApiBinaryDataCommand extends ApiCommand {

    // transfer unique id
    // can be constant unless going to push data concurrently
    public final int transferId;

    // serializable object
    public final WriteBytesMarshallable data;

    @Override
    public String toString() {
        return "[BINARY_DATA tid=" + transferId + " data=" + data + "]";
    }
}
