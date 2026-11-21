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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public final class UnsafeUtils {

    private static final VarHandle RESULT_CODE;
    private static final VarHandle MATCHER_EVENT;

    static {
        try {
            final MethodHandles.Lookup lookup = MethodHandles.lookup();
            RESULT_CODE = lookup.findVarHandle(OrderCommand.class, "resultCode", CommandResultCode.class);
            MATCHER_EVENT = lookup.findVarHandle(OrderCommand.class, "matcherEvent", MatcherTradeEvent.class);
        } catch (ReflectiveOperationException ex) {
            throw new ExceptionInInitializerError(ex);
        }
    }

    public static void setResultVolatile(final OrderCommand cmd,
                                         final boolean result,
                                         final CommandResultCode successCode,
                                         final CommandResultCode failureCode) {

        final CommandResultCode codeToSet = result ? successCode : failureCode;

        CommandResultCode currentCode;
        do {
            currentCode = (CommandResultCode) RESULT_CODE.getVolatile(cmd);

            // finish if desired code was already set
            // or if someone has set failure
            if (currentCode == codeToSet || currentCode == failureCode) {
                break;
            }

        } while (!RESULT_CODE.compareAndSet(cmd, currentCode, codeToSet));
    }

    public static void appendEventsVolatile(final OrderCommand cmd,
                                            final MatcherTradeEvent eventHead) {

        final MatcherTradeEvent tail = eventHead.findTail();

        do {
            tail.nextEvent = (MatcherTradeEvent) MATCHER_EVENT.getVolatile(cmd);
        } while (!MATCHER_EVENT.compareAndSet(cmd, tail.nextEvent, eventHead));
    }

}
