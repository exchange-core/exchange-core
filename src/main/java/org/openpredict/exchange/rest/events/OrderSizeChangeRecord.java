package org.openpredict.exchange.rest.events;

/**
 * Size of the order has been reduced due one of the following reasons:
 * - trade matching,
 * - reduce (cancel or update) or
 * - rejection (no liquidity for matching order)
 * <p>
 * Size of the order can never increase.
 */

public interface OrderSizeChangeRecord {

    long getAffectedSize();
}
