package org.openpredict.exchange.beans.api;


import lombok.Builder;

@Builder
public class ApiOrderBookRequest extends ApiCommand {

    public int symbol;

    public int size;

    @Override
    public String toString() {
        return "[OB " + symbol + " " + size + "]";
    }
}
