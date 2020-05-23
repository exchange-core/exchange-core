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

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum CoreWaitStrategy {

    BUSY_SPIN(new BusySpinWaitStrategy(), false, false),

    YIELDING(new YieldingWaitStrategy(), true, false),

    BLOCKING(new BlockingWaitStrategy(), false, true),

    // special case
    SECOND_STEP_NO_WAIT(null, false, false);

    @Getter
    private final WaitStrategy disruptorWaitStrategy;

    @Getter
    private final boolean yield;

    @Getter
    private final boolean block;
}
