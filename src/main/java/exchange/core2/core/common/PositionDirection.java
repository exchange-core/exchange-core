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

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum PositionDirection {
    LONG(1),
    SHORT(-1),
    EMPTY(0);

    @Getter
    private int multiplier;

    public static PositionDirection of(OrderAction action) {
        return action == OrderAction.BID ? LONG : SHORT;
    }

    public static PositionDirection of(byte code) {
        switch (code) {
            case 1:
                return LONG;
            case -1:
                return SHORT;
            case 0:
                return EMPTY;
            default:
                throw new IllegalArgumentException("unknown PositionDirection:" + code);
        }
    }


    public boolean isOppositeToAction(OrderAction action) {
        return (this == PositionDirection.LONG && action == OrderAction.ASK) || (this == PositionDirection.SHORT && action == OrderAction.BID);
    }

    public boolean isSameAsAction(OrderAction action) {
        return (this == PositionDirection.LONG && action == OrderAction.BID) || (this == PositionDirection.SHORT && action == OrderAction.ASK);
    }

}
