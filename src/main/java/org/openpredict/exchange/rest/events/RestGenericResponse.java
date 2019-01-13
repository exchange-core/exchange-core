package org.openpredict.exchange.rest.events;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.openpredict.exchange.beans.cmd.CommandResultCode;

@Getter
@AllArgsConstructor
@Builder
public final class RestGenericResponse {

    private final long ticket;
    private final int responseCode;
    private final CommandResultCode response;
    private final Object data;

    @Override
    public String toString() {
        return "[RESPONSE T:" + ticket + " RES:" + responseCode + " " + response + "]";
    }
}
