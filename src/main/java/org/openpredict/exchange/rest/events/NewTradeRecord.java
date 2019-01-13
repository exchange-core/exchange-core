package org.openpredict.exchange.rest.events;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.openpredict.exchange.beans.cmd.MatchingRole;

/**
 * Trade fill
 */

@Getter
@Builder
@AllArgsConstructor
public final class NewTradeRecord implements OrderSizeChangeRecord {
    private final String type = "trade";
    private final long filledSize;
    private final long fillPrice;
    private final MatchingRole matchingRole;

    @Override
    public long getAffectedSize() {
        return filledSize;
    }
}
