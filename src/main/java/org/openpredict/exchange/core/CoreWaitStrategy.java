package org.openpredict.exchange.core;

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
