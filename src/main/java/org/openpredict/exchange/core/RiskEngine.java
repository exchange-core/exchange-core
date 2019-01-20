package org.openpredict.exchange.core;

import lombok.extern.slf4j.Slf4j;
import org.openpredict.exchange.beans.CoreSymbolSpecification;
import org.openpredict.exchange.beans.OrderAction;
import org.openpredict.exchange.beans.SymbolPortfolioRecord;
import org.openpredict.exchange.beans.UserProfile;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.function.IntFunction;


/**
 * Stateless risk engine
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
        final int symbol = cmd.symbol;
        SymbolPortfolioRecord portfolio = userProfile.getOrCreatePortfolio(symbol);

        final long signedPosition = portfolio.openVolume * portfolio.position.getMultiplier();
        final long currentRiskBuySize = portfolio.pendingBuySize + signedPosition;
        final long currentRiskSellSize = portfolio.pendingSellSize - signedPosition;

        CoreSymbolSpecification spec = symbolSpecificationProvider.getSymbolSpecification(symbol);

        long depositBuy = spec.depositBuy * currentRiskBuySize;
        long depositSell = spec.depositSell * currentRiskSellSize;
        // depositBuy or depositSell can be negative, but not both of them
        final long originalDeposit = Math.max(depositBuy, depositSell);

        if (cmd.action == OrderAction.BID) {
            depositBuy += spec.depositBuy * cmd.size;
        } else {
            depositSell += spec.depositSell * cmd.size;
        }

        // depositBuy or depositSell can be negative, but not both of them
        final long newDeposit = Math.max(depositBuy, depositSell);

        // always allow to place an order that would not increase trader's risk
        if (newDeposit <= originalDeposit) {
            return true;
        }

        // extra deposit is required
        // check if current balance and margin can cover new deposit
        final IntFunction<CoreSymbolSpecification> specSupplier = s -> symbolSpecificationProvider.getSymbolSpecification(s);
        final long availableFunds = userProfile.getAvailableFunds(specSupplier);
        if (newDeposit <= availableFunds) {
            return true;
        }

        // try to cleanup portfolio if refusing to place
        userProfile.removeRecordIfEmpty(portfolio);
        return false;
    }

}
