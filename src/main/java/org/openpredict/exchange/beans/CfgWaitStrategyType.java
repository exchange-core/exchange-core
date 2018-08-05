package org.openpredict.exchange.beans;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;

public enum CfgWaitStrategyType {
    BUSY_SPIN,
    YIELDING,
    SLEEPING;

    public WaitStrategy create() {
        switch (this) {

            case SLEEPING:
                return new SleepingWaitStrategy();
            case BUSY_SPIN:
                return new BusySpinWaitStrategy();
            case YIELDING:
            default:
                return new YieldingWaitStrategy();
        }
    }

}
