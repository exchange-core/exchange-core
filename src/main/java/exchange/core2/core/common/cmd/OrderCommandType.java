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

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum OrderCommandType {
    PLACE_ORDER((byte) 1, true),
    CANCEL_ORDER((byte) 2, true),
    MOVE_ORDER((byte) 3, true),

    ORDER_BOOK_REQUEST((byte) 6, false),

    ADD_USER((byte) 10, true),
    BALANCE_ADJUSTMENT((byte) 11, true),
    SUSPEND_USER((byte) 12, true),
    RESUME_USER((byte) 13, true),

    BINARY_DATA_QUERY((byte) 90, false),
    BINARY_DATA_COMMAND((byte) 91, true),

    PERSIST_STATE_MATCHING((byte) 110, true),
    PERSIST_STATE_RISK((byte) 111, true),

    RESET((byte) 124, true),
    SHUTDOWN_SIGNAL((byte) 127, false);

    private byte code;
    private boolean mutate;


}
