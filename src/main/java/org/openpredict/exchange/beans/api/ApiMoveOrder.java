package org.openpredict.exchange.beans.api;


import lombok.Builder;

@Builder
public class ApiMoveOrder extends ApiCommand {

    public final long id;

    public final long newPrice;
    public final long newSize;

    public final long uid;
    public final int symbol;

    @Override
    public String toString() {
        return "[MOVE " + id + " " + newPrice + ":" + newSize + "]";
    }
}
