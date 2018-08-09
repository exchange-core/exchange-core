package org.openpredict.exchange.beans.cmd;

import lombok.Getter;

@Getter
public enum CommandResultCode {
    NEW(0),
    VALID_FOR_MATCHING_ENGINE(1),

    SUCCESS(100),

    AUTH_INVALID_USER(-1001),
    AUTH_TOKEN_EXPIRED(-1002),

    RISK_NSF(-2001),

    // MATCHING_NO_LIQUIDITY(-3001), // ?? order can be applied partially
    MATCHING_INVALID_ORDER_ID(-3002),

    USER_MGMT_USER_ALREADY_EXISTS(-4001),

    DROP(-9999);


    private int code;

    CommandResultCode(int code) {
        this.code = code;
    }

}
