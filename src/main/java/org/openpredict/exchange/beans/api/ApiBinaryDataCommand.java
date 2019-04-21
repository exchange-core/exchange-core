package org.openpredict.exchange.beans.api;


import lombok.Builder;

import java.io.Serializable;

@Builder
public final class ApiBinaryDataCommand extends ApiCommand {

    // transfer unique id
    // can be constant unless going to push data concurrently
    public final long transferId;

    // serializable object
    public final Serializable data;

    @Override
    public String toString() {
        return "[BINARY_DATA tid=" + transferId + " data=" + data + "]";
    }
}
