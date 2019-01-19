package org.openpredict.exchange.core;

import lombok.extern.slf4j.Slf4j;
import org.openpredict.exchange.beans.CoreSymbolSpecification;
import org.openpredict.exchange.beans.OrderAction;
import org.openpredict.exchange.beans.SymbolPortfolio;
import org.openpredict.exchange.beans.UserProfile;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


/**
 * Stateless risk engine
 *
 */
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

        CoreSymbolSpecification spec = symbolSpecificationProvider.getSymbolSpecification(cmd.symbol);

        long depositBuy = spec.depositBuy * currentRiskBuySize;
        long depositSell = spec.depositSell * currentRiskSellSize;
        // depositBuy or depositSell can be negative, but not both of them
        long originalDeposit = Math.max(depositBuy, depositSell);

        if (cmd.action == OrderAction.BID) {
            depositBuy += spec.depositBuy * cmd.size;
        } else {
            depositSell += spec.depositSell * cmd.size;
        }

        // depositBuy or depositSell can be negative, but not both of them
        long newDeposit = Math.max(depositBuy, depositSell);

        // always allow to place an order that would not increase trader's risk
        if (newDeposit <= originalDeposit) {
            return true;
        }

        // extra deposit is required
        // check if current balance and margin can cover new deposit
        return newDeposit <= userProfile.fastBalance + userProfile.fastMargin;

    }

}
