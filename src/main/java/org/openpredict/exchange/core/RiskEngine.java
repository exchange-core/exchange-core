package org.openpredict.exchange.core;

import lombok.extern.slf4j.Slf4j;
import org.openpredict.exchange.beans.CoreSymbolSpecification;
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
public final class RiskEngine {

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

        final CoreSymbolSpecification spec = symbolSpecificationProvider.getSymbolSpecification(symbol);
        final long newDeposit = portfolio.calculateRequiredDepositForOrder(spec, cmd.action, cmd.size);
        if (newDeposit == -1) {
            // always allow to place an order that would not increase trader's risk
            return true;
        }

        // extra deposit is required
        // check if current balance and margin can cover new deposit
        final IntFunction<CoreSymbolSpecification> specSupplier = s -> symbolSpecificationProvider.getSymbolSpecification(s);
        final long availableFunds = userProfile.balance + userProfile.getMarginMinusDeposit(specSupplier, symbol);  //final long availableFunds = userProfile.balance;

        if (newDeposit <= availableFunds) {
            return true;
        }

        // try to cleanup portfolio if refusing to place
        userProfile.removeRecordIfEmpty(portfolio);
        return false;
    }

}
