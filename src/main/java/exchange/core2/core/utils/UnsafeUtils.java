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
package exchange.core2.core.utils;

import exchange.core2.core.common.MatcherTradeEvent;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import lombok.extern.slf4j.Slf4j;

import static net.openhft.chronicle.core.UnsafeMemory.UNSAFE;

@Slf4j
public class UnsafeUtils {

    final static long OFFSET_ORDER_ID;
    final static long OFFSET_RESULT_CODE;
    final static long OFFSET_PRICE;
    final static long OFFSET_UID;
    final static long OFFSET_EVENT;

    static {
        try {
            OFFSET_ORDER_ID = UNSAFE.objectFieldOffset(OrderCommand.class.getDeclaredField("orderId"));
            OFFSET_PRICE = UNSAFE.objectFieldOffset(OrderCommand.class.getDeclaredField("price"));
            OFFSET_UID = UNSAFE.objectFieldOffset(OrderCommand.class.getDeclaredField("uid"));
            OFFSET_RESULT_CODE = UNSAFE.objectFieldOffset(OrderCommand.class.getDeclaredField("resultCode"));
            OFFSET_EVENT = UNSAFE.objectFieldOffset(OrderCommand.class.getDeclaredField("matcherEvent"));
        } catch (NoSuchFieldException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public static void setResultVolatile(final OrderCommand cmd,
                                         final boolean result,
                                         final CommandResultCode successCode,
                                         final CommandResultCode failureCode) {

        final CommandResultCode codeToSet = result ? successCode : failureCode;

        CommandResultCode currentCode;
        do {
            // read current code
            currentCode = (CommandResultCode) UNSAFE.getObjectVolatile(cmd, OFFSET_RESULT_CODE);

            // finish if desired code was already set
            // or if someone has set failure
            if (currentCode == codeToSet || currentCode == failureCode) {
                break;
            }

            // do a CAS operation
        } while (!UNSAFE.compareAndSwapObject(cmd, OFFSET_RESULT_CODE, currentCode, codeToSet));
    }

    public static void appendEventsVolatile(final OrderCommand cmd,
                                            final MatcherTradeEvent eventHead) {

        final MatcherTradeEvent tail = eventHead.findTail();

        //MatcherTradeEvent.asList(eventHead).forEach(a -> log.info("in {}", a));

        do {
            // read current head and attach to the tail of new
            tail.nextEvent = (MatcherTradeEvent) UNSAFE.getObjectVolatile(cmd, OFFSET_EVENT);

            // do a CAS operation
        } while (!UNSAFE.compareAndSwapObject(cmd, OFFSET_EVENT, tail.nextEvent, eventHead));
    }

}
