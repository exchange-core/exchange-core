package org.openpredict.exchange.rest.events;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * Reduced (update or cancel)
 */
@Getter
@Builder
@AllArgsConstructor
public final class ReduceRecord implements OrderSizeChangeRecord {
    private final String type = "reduce";
    private final long reducedSize;

    @Override
    public long getAffectedSize() {
        return reducedSize;
    }
}
