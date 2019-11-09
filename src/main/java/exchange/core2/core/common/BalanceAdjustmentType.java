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
public enum BalanceAdjustmentType {
    ADJUSTMENT(0),
    SUSPEND(1);

    private byte code;

    BalanceAdjustmentType(int code) {
        this.code = (byte) code;
    }

    public static BalanceAdjustmentType of(byte code) {
        switch (code) {
            case 0:
                return ADJUSTMENT;
            case 1:
                return SUSPEND;
            default:
                throw new IllegalArgumentException("unknown BalanceAdjustmentType:" + code);
        }
    }
}
