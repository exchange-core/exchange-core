package org.openpredict.exchange.rest.events;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * Rejected (no liquidity)
 */
@Getter
@Builder
@AllArgsConstructor
public final class RejectionRecord implements OrderSizeChangeRecord {
    private final String type = "reject";
    private final long rejectedSize;

    @Override
    public long getAffectedSize() {
        return rejectedSize;
    }
}
