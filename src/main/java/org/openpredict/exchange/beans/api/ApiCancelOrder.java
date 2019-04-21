package org.openpredict.exchange.beans.api;


import lombok.Builder;

@Builder
public final class ApiCancelOrder extends ApiCommand {

    public final long id;

    public final long uid;
    public final int symbol;

    @Override
    public String toString() {
        return "[CANCEL " + id + "]";
    }
}
