package org.openpredict.exchange.beans.api;


import lombok.Builder;

@Builder
public class ApiAdjustUserBalance extends ApiCommand {

    public final long uid;

    public final long amount;

    @Override
    public String toString() {
        String amountFmt = String.format("%s%d", amount >= 0 ? "+" : "-", Math.abs(amount));
        return "[ADJUST_BALANCE " + uid + " " + amountFmt + "]";

    }
}
