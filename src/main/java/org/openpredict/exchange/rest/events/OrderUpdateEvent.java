package org.openpredict.exchange.rest.events;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Getter
@Builder
@AllArgsConstructor
public final class OrderUpdateEvent {

    private final long orderId;
    private final long activeSize;
    private final long price;
    // todo add status?

    private final List<? extends OrderSizeChangeRecord> trades;
}
