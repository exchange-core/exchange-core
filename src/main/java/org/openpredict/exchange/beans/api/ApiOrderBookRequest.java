package org.openpredict.exchange.beans.api;


import lombok.Builder;

@Builder
public final class ApiOrderBookRequest extends ApiCommand {

    final public int symbol;

    final public int size;

    @Override
    public String toString() {
        return "[OB " + symbol + " " + size + "]";
    }
}
