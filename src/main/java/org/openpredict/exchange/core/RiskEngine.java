package org.openpredict.exchange.core;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.openpredict.exchange.beans.*;
import org.openpredict.exchange.beans.cmd.OrderCommand;


/**
 * Stateless risk engine
 */
@Slf4j
@AllArgsConstructor
public final class RiskEngine {

    final private SymbolSpecificationProvider symbolSpecificationProvider;

    public boolean placeOrder(OrderCommand cmd, UserProfile userProfile) {

        final CoreSymbolSpecification spec = symbolSpecificationProvider.getSymbolSpecification(cmd.symbol);

        final SymbolPortfolioRecord portfolio = userProfile.getOrCreatePortfolioRecord(cmd.symbol);

        final boolean canPlaceOrder;
        if (spec.type == SymbolType.CURRENCY_EXCHANGE_PAIR) {
            canPlaceOrder = placeExchangeOrder(cmd, userProfile, spec);
        } else if (spec.type == SymbolType.FUTURES_CONTRACT) {
            canPlaceOrder = placeMarginTradeOrder(cmd, userProfile, spec, portfolio);
        } else {
            log.error("Symbol {} - unsupported type: {}", cmd.symbol, spec.type);
            canPlaceOrder = false;
        }

        if (canPlaceOrder) {
            portfolio.pendingHold(cmd.action, cmd.size);
        } else {
            // try to cleanup portfolio if refusing to place
            userProfile.removeRecordIfEmpty(portfolio);
        }

        return canPlaceOrder;
    }

    private boolean placeExchangeOrder(OrderCommand cmd, UserProfile userProfile, CoreSymbolSpecification spec) {

        final int currency = (cmd.action == OrderAction.BID) ? spec.counterCurrency : spec.baseCurrency;

        final long currencyAccountBalance = userProfile.accounts.get(currency);

        // go through all positions and check reserved in this currency
        long pendingAmount = 0L;
        for (SymbolPortfolioRecord portfolioRecord : userProfile.portfolio) {
            final CoreSymbolSpecification prSpec = symbolSpecificationProvider.getSymbolSpecification(portfolioRecord.symbol);
            // TODO support futures positions checks (margin, pnl, etc)
            // TODO split futures and exchange positions hashtables?
            if (prSpec.type == SymbolType.CURRENCY_EXCHANGE_PAIR) {
                if (prSpec.baseCurrency == currency) {
                    pendingAmount += portfolioRecord.pendingSellSize;
                } else if (prSpec.counterCurrency == currency) {
                    pendingAmount += portfolioRecord.pendingBuySize;
                }
            }
        }

        final long size = cmd.size;

        // TODO fees
        return currencyAccountBalance - pendingAmount >= size;
    }

    /**
     * 1. Users account balance
     * 2. Margin
     * 3. Current limit orders
     */
    private boolean placeMarginTradeOrder(OrderCommand cmd, UserProfile userProfile, CoreSymbolSpecification spec, final SymbolPortfolioRecord portfolio) {

        final long newRequiredDeposit = portfolio.calculateRequiredDepositForOrder(spec, cmd.action, cmd.size);
        if (newRequiredDeposit == -1) {
            // always allow to place an order that would not increase trader's risk
            return true;
        }

        long estimatedSymbolProfit = portfolio.estimateProfit(spec);

        final int symbol = cmd.symbol;

        long profitMinusDepositOtherSymbols = 0L;
        for (SymbolPortfolioRecord portfolioRecord : userProfile.portfolio) {
            if (portfolioRecord.symbol != symbol) {
                final CoreSymbolSpecification spec2 = symbolSpecificationProvider.getSymbolSpecification(portfolioRecord.symbol);
                profitMinusDepositOtherSymbols += portfolioRecord.estimateProfit(spec2);
                profitMinusDepositOtherSymbols -= portfolioRecord.calculateRequiredDepositForFutures(spec2);
            }
        }

        // extra deposit is required
        // check if current balance and margin can cover new deposit
        final long availableFunds = userProfile.futuresBalance
                + estimatedSymbolProfit
                + profitMinusDepositOtherSymbols;

        return newRequiredDeposit <= availableFunds;
    }


}
