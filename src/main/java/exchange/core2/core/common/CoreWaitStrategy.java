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

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.function.Supplier;

@RequiredArgsConstructor
public enum CoreWaitStrategy {
    BUSY_SPIN(BusySpinWaitStrategy::new, false, false),
    YIELDING(YieldingWaitStrategy::new, true, false),
    SLEEPING(SleepingWaitStrategy::new, true, true),

    // special case
    NO_WAIT(() -> null, false, false);

    private final Supplier<WaitStrategy> supplier;

    @Getter
    private final boolean yield;

    @Getter
    private final boolean park;

    public WaitStrategy create() {
        return supplier.get();
    }
}
