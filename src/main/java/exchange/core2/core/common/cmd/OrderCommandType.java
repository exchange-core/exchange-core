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
package exchange.core2.core.common.cmd;

import lombok.Getter;

@Getter
public enum OrderCommandType {
    PLACE_ORDER(1),
    CANCEL_ORDER(2),
    MOVE_ORDER(3),

    ORDER_BOOK_REQUEST(6),

    ADD_USER(10),
    BALANCE_ADJUSTMENT(11),

    CLEARING_OPERATION(30),

    BINARY_DATA(90),

    PERSIST_STATE_MATCHING(110),
    PERSIST_STATE_RISK(111),

    NOP(120),
    RESET(124),
    SHUTDOWN_SIGNAL(127);

    private byte code;

    OrderCommandType(int code) {
        this.code = (byte) code;
    }

}
