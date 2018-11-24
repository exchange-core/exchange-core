package org.openpredict.exchange.beans.api;


import lombok.Builder;

@Builder
public class ApiOrderBookRequest extends ApiCommand {

    public final int symbol;

    public final int size;

    @Override
    public String toString() {
        return "[OB " + symbol + " " + size + "]";
    }
}
