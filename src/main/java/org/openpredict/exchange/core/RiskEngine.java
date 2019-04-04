package org.openpredict.exchange.core;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.openpredict.exchange.beans.CoreSymbolSpecification;
import org.openpredict.exchange.beans.OrderAction;
import org.openpredict.exchange.beans.SymbolPortfolioRecord;
import org.openpredict.exchange.beans.UserProfile;
import org.openpredict.exchange.beans.cmd.OrderCommand;


/**
 * Stateless risk engine
 */
@Slf4j
@AllArgsConstructor
public final class RiskEngine {

    final private SymbolSpecificationProvider symbolSpecificationProvider;

    /**
     * 1. Users account balance
     * 2. Margin
     * 3. Current limit orders
     */
    public boolean placeOrder(OrderCommand cmd, UserProfile userProfile) {
        final int symbol = cmd.symbol;
        final OrderAction action = cmd.action;
        final long size = cmd.size;
        final SymbolPortfolioRecord portfolio = userProfile.getOrCreatePortfolioRecord(symbol);
        final CoreSymbolSpecification spec = symbolSpecificationProvider.getSymbolSpecification(symbol);

        final long newRequiredDeposit = portfolio.calculateRequiredDepositForOrder(spec, action, size);
        if (newRequiredDeposit == -1) {
            // always allow to place an order that would not increase trader's risk
            portfolio.pendingHold(action, size);
            return true;
        }

        long estimatedSymbolProfit = portfolio.estimateProfit(spec);

        long profitMinusDepositOtherSymbols = 0L;
        for (SymbolPortfolioRecord portfolioRecord : userProfile.portfolio) {
            if (portfolioRecord.symbol != symbol) {
                final CoreSymbolSpecification spec2 = symbolSpecificationProvider.getSymbolSpecification(portfolioRecord.symbol);
                profitMinusDepositOtherSymbols += portfolioRecord.estimateProfit(spec2);
                profitMinusDepositOtherSymbols -= portfolioRecord.calculateRequiredDeposit(spec2);
            }
        }

        // extra deposit is required
        // check if current balance and margin can cover new deposit
        final long availableFunds = userProfile.balance
                + estimatedSymbolProfit
                + profitMinusDepositOtherSymbols;

        if (newRequiredDeposit <= availableFunds) {
            portfolio.pendingHold(action, size);
            return true;
        }

        // try to cleanup portfolio if refusing to place
        userProfile.removeRecordIfEmpty(portfolio);
        return false;
    }


}
