package org.openpredict.exchange.beans.api;


import lombok.Builder;

@Builder
public class ApiCancelOrder extends ApiCommand {

    public long id;

    public long uid;
    public int symbol;

    @Override
    public String toString() {
        return "[CANCEL " + id + "]";
    }
}
