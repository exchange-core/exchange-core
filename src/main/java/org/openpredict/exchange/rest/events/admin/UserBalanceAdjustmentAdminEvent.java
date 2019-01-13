package org.openpredict.exchange.rest.events.admin;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;


@Getter
@Setter
@Builder
@AllArgsConstructor
public final class UserBalanceAdjustmentAdminEvent {
    private final String msgType = "user_balance_updated";

    private final long uid;
    private final long transactionId;
    private final long amount;
    private final long balance;
}

