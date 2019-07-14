package org.openpredict.exchange.beans;

import lombok.Getter;

@Getter
public enum ReportType {

    STATE_HASH(101),

    SINGLE_USER_REPORT(201),

    TOTAL_CURRENCY_BALANCE(601);

    private final int code;

    ReportType(int code) {
        this.code = code;
    }

    public static ReportType of(int code) {

        switch (code) {
            case 101:
                return STATE_HASH;
            case 201:
                return SINGLE_USER_REPORT;
            case 601:
                return TOTAL_CURRENCY_BALANCE;
            default:
                throw new IllegalArgumentException("unknown ReportType:" + code);
        }

    }

}
