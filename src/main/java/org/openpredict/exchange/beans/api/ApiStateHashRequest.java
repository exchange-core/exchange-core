package org.openpredict.exchange.beans.api;


import lombok.Builder;

@Builder
public final class ApiStateHashRequest extends ApiCommand {


    @Override
    public String toString() {
        return "[HASH]";
    }
}
