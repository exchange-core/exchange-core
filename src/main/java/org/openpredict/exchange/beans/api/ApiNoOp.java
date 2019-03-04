package org.openpredict.exchange.beans.api;


import lombok.Builder;

@Builder
public class ApiNoOp extends ApiCommand {
    @Override
    public String toString() {
        return "[RESET]";
    }
}
