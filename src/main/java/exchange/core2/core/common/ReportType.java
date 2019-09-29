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
