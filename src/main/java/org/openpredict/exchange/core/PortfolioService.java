package org.openpredict.exchange.core;

import lombok.extern.slf4j.Slf4j;
import org.openpredict.exchange.beans.OrderAction;
import org.openpredict.exchange.beans.PortfolioPosition;
import org.openpredict.exchange.beans.SymbolPortfolio;
import org.openpredict.exchange.beans.UserProfile;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.springframework.stereotype.Service;

/**
 * Stateless portfolio service
 */
@Service
@Slf4j
public class PortfolioService {

    /**
     * Hold deposit
     *
     * @param order
     * @param userProfile
     */
    public void holdDepositForNewOrder(OrderCommand order, UserProfile userProfile) {
        SymbolPortfolio portfolio = userProfile.getOrCreatePortfolio(order.symbol);

        portfolio.pendingHold(order.action, order.size);
    }

    /**
     * Update portfolio for one user
     * 1. Un-hold pending size
     * 2. Reduce opposite position accordingly (if exists)
     * 3. Increase forward position accordingly (if size left in the trading event)
     */
    public void updatePortfolioForTrade(OrderAction action, long size, long price, SymbolPortfolio portfolio) {

        // 1. un-hold pending size
        portfolio.pendingRelease(action, size);

        if (portfolio.pendingSellSize < 0 || portfolio.pendingBuySize < 0) {
            log.error("uid {} : pendingSellSize:{} pendingBuySize:{}", portfolio.uid, portfolio.pendingSellSize, portfolio.pendingBuySize);
        }

        long remainingVolume = size;

        // 2. Reduce opposite position accordingly (if exists)
        remainingVolume = portfolio.reducePositionIfOppositeAction(remainingVolume, action);

        if (remainingVolume < 0) {
            log.error("uid {} : remainingVolume:{}", portfolio.uid, remainingVolume);
        }

        // 3. Increase forward position accordingly
        if (remainingVolume > 0) {
            portfolio.totalSize += remainingVolume;
            portfolio.acquireAmountSum += price * remainingVolume;
            portfolio.position = portfolio.totalSize != 0 ? PortfolioPosition.of(action) : PortfolioPosition.EMPTY;

            if (portfolio.portfolioHasElement() && portfolio.tailPrice() == price) {
                // just an optimization
                // if trading big amount for the same price - smaller parts can be glued together into a bigger one
                portfolio.incTailVolume(remainingVolume);
            } else {
                portfolio.portfolioAdd(price, remainingVolume);
            }
        }
    }

    /**
     * Release pending size for Reduce or Rejection event
     *
     * @param action
     * @param size
     * @param portfolio
     */
    public void updatePortfolioForReduce(OrderAction action, long size, SymbolPortfolio portfolio) {
        // un-hold pending size
        portfolio.pendingRelease(action, size);
    }


}
