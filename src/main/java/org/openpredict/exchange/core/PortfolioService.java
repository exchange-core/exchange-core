package org.openpredict.exchange.core;

import lombok.extern.slf4j.Slf4j;
import org.openpredict.exchange.beans.OrderAction;
import org.openpredict.exchange.beans.SymbolPortfolioRecord;
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
        SymbolPortfolioRecord portfolio = userProfile.getOrCreatePortfolio(order.symbol);

        portfolio.pendingHold(order.action, order.size);
    }


    /**
     * Release pending size for Reduce or Rejection event
     *
     * @param action
     * @param size
     * @param portfolio
     */
    public void updatePortfolioForReduce(OrderAction action, long size, SymbolPortfolioRecord portfolio) {
        // un-hold pending size
        portfolio.pendingRelease(action, size);
    }


}
