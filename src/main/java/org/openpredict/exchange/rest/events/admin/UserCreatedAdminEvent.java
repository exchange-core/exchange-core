package org.openpredict.exchange.rest.events.admin;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;


@Getter
@Setter
@Builder
@AllArgsConstructor
public final class UserCreatedAdminEvent {
    private final String msgType = "adm_user_created";

    private final long uid;
}

