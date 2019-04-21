package org.openpredict.exchange.beans.api;


import lombok.Builder;

@Builder
public final class ApiReset extends ApiCommand {
    @Override
    public String toString() {
        return "[RESET]";
    }
}
