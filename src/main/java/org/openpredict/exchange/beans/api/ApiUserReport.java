package org.openpredict.exchange.beans.api;


import lombok.Builder;

@Builder
public final class ApiUserReport extends ApiCommand {

    public final long uid;

    @Override
    public String toString() {
        return "[USER_REPORT " + uid + "]";
    }
}
