package org.openpredict.exchange.core;

@FunctionalInterface
public interface PortfolioFundsAdjustmentCallback {

    void submit(long profitAmount, long acquiredVolume);

}
