/*
 * Copyright 2019 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package exchange.core2.core.common;

import lombok.Getter;

@Getter
public enum ReportType {

    STATE_HASH(10001),

    SINGLE_USER_REPORT(10002),

    TOTAL_CURRENCY_BALANCE(10003);

    private final int code;

    ReportType(int code) {
        this.code = code;
    }

    public static ReportType of(int code) {

        switch (code) {
            case 10001:
                return STATE_HASH;
            case 10002:
                return SINGLE_USER_REPORT;
            case 10003:
                return TOTAL_CURRENCY_BALANCE;
            default:
                throw new IllegalArgumentException("unknown ReportType:" + code);
        }

    }

}
