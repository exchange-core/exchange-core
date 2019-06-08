package org.openpredict.exchange.beans.api;


import lombok.Builder;

@Builder
public final class ApiPersistState extends ApiCommand {

    public long dumpId;
    public boolean seal;

    @Override
    public String toString() {
        return "[PERSIST]-" + dumpId + " seal=" + seal;
    }
}
