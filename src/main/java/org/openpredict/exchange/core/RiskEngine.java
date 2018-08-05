package org.openpredict.exchange.core;

import lombok.extern.slf4j.Slf4j;
import org.openpredict.exchange.beans.OrderAction;
import org.openpredict.exchange.beans.SymbolPortfolio;
import org.openpredict.exchange.beans.SymbolSpecification;
import org.openpredict.exchange.beans.UserProfile;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class RiskEngine {

    @Autowired
    private UserProfileService userProfileService;

    @Autowired
    private PortfolioService portfolioService;

    @Autowired
    private SymbolSpecificationProvider symbolSpecificationProvider;

    /**
     * 1. Users account balance
     * 2. Margin
     * 3. Current limit orders
     */

    public boolean checkIfCanPlaceOrder(OrderCommand cmd, UserProfile userProfile) {
        SymbolPortfolio portfolio = userProfile.portfolio.get(cmd.symbol);

        long currentRiskBuySize = 0;
        long currentRiskSellSize = 0;
        if (portfolio != null) {
            long signedPosition = portfolio.totalSize * portfolio.position.getMultiplier();
            currentRiskBuySize = portfolio.pendingBuySize + signedPosition;
            currentRiskSellSize = portfolio.pendingSellSize - signedPosition;
        }

        SymbolSpecification spec = symbolSpecificationProvider.getSymbolSpecification(cmd.symbol);

        long depositBuy = spec.depositBuy * currentRiskBuySize;
        long depositSell = spec.depositSell * currentRiskSellSize;
        // depositBuy or depositSell can be negative, but not both
        long originalDeposit = Math.max(depositBuy, depositSell);

        if (cmd.action == OrderAction.BID) {
            depositBuy += spec.depositBuy * cmd.size;
        } else {
            depositSell += spec.depositSell * cmd.size;
        }
        // depositBuy or depositSell can be negative, but not both
        long newDeposit = Math.max(depositBuy, depositSell);

//        log.debug("originalDeposit={} newDeposit={}", originalDeposit, newDeposit);

        if (newDeposit <= originalDeposit) {
            // always allow place order which does not increase trader's risk
            return true;

        } else {
            // extra deposit is required - check if current balance and margin can cover it
//            log.debug("fastBalance={} fastMargin={}", userProfile.fastBalance, userProfile.fastMargin);

            return newDeposit <= userProfile.fastBalance + userProfile.fastMargin;
        }
    }

}
