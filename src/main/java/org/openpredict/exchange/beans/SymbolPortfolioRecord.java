package org.openpredict.exchange.beans;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Builder
@ToString
@AllArgsConstructor
@Slf4j
public final class SymbolPortfolioRecord {

    public final int symbol;
    public final long uid;

    // open positions state (for margin trades only)
    public PortfolioPosition position = PortfolioPosition.EMPTY;
    public long openVolume = 0;
    public long openPriceSum = 0; //
    public long profit = 0;

    // pending orders total size
    // increment before sending order to matching engine
    // decrement after receiving trade confirmation from matching engine
    public long pendingSellSize = 0;
    public long pendingBuySize = 0;

    public SymbolPortfolioRecord(int symbol, long uid) {
        this.symbol = symbol;
        this.uid = uid;
    }

    /**
     * Check if portfolio is empty (no pending orders, no open trades) - can remove it from hashmap
     *
     * @return true if portfolio is empty (no pending orders, no open trades)
     */
    public boolean isEmpty() {
        return position == PortfolioPosition.EMPTY
                && pendingSellSize == 0
                && pendingBuySize == 0;
    }

    public void pendingHold(OrderAction orderAction, long size) {
        if (orderAction == OrderAction.ASK) {
            pendingSellSize += size;
        } else {
            pendingBuySize += size;
        }
    }

    public void pendingRelease(OrderAction orderAction, long size) {
        if (orderAction == OrderAction.ASK) {
            pendingSellSize -= size;
        } else {
            pendingBuySize -= size;
        }

//        if (pendingSellSize < 0 || pendingBuySize < 0) {
//            log.error("uid {} : pendingSellSize:{} pendingBuySize:{}", uid, pendingSellSize, pendingBuySize);
//        }
    }

    public long estimateProfit(CoreSymbolSpecification spec) {

        final long varProfit;
        if (position == PortfolioPosition.LONG) {
            varProfit = spec.lastBidPrice != 0
                    ? (openVolume * spec.lastBidPrice - openPriceSum)
                    : spec.depositBuy * openVolume; // unknown price - no liquidity - use extra deposit

//            if(Math.random()<0.001) log.debug("LONG  {} spec.lastBidPrice={} openVolume={} openPriceSum={}", varProfit, spec.lastBidPrice, openVolume, openPriceSum);

        } else {
            varProfit = spec.lastAskPrice != Long.MAX_VALUE
                    ? (openPriceSum - openVolume * spec.lastAskPrice)
                    : spec.depositSell * openVolume; // unknown price - no liquidity - use extra deposit

//            if(Math.random()<0.001) log.debug("SHORT {} spec.lastBidPrice={} openVolume={} openPriceSum={}", varProfit, spec.lastAskPrice, openVolume, openPriceSum);

        }


        return profit + varProfit;
    }

    /**
     * Calculate required deposit based on specification and current position/orders
     *
     * @param spec
     * @return
     */
    public long calculateRequiredDepositForFutures(CoreSymbolSpecification spec) {
        final long specDepositBuy = spec.depositBuy;
        final long specDepositSell = spec.depositSell;

        final long signedPosition = openVolume * position.getMultiplier();
        final long currentRiskBuySize = pendingBuySize + signedPosition;
        final long currentRiskSellSize = pendingSellSize - signedPosition;

        final long depositBuy = specDepositBuy * currentRiskBuySize;
        final long depositSell = specDepositSell * currentRiskSellSize;
        // depositBuy or depositSell can be negative, but not both of them
        return Math.max(depositBuy, depositSell);
    }

    /**
     * Calculate required deposit based on specification and current position/orders
     * considering extra size added to current position (or outstanding orders)
     *
     * @param spec
     * @return -1 if no extra deposit will be required (order will reduce current exposure)
     */
    public long calculateRequiredDepositForOrder(final CoreSymbolSpecification spec, final OrderAction action, final long size) {
        final long specDepositBuy = spec.depositBuy;
        final long specDepositSell = spec.depositSell;

        final long signedPosition = openVolume * position.getMultiplier();
        final long currentRiskBuySize = pendingBuySize + signedPosition;
        final long currentRiskSellSize = pendingSellSize - signedPosition;

        long depositBuy = specDepositBuy * currentRiskBuySize;
        long depositSell = specDepositSell * currentRiskSellSize;
        // either depositBuy or depositSell can be negative (because of signedPosition), but not both of them
        final long currentDeposit = Math.max(depositBuy, depositSell);

        if (action == OrderAction.BID) {
            depositBuy += spec.depositBuy * size;
        } else {
            depositSell += spec.depositSell * size;
        }

        // depositBuy or depositSell can be negative, but not both of them
        final long newDeposit = Math.max(depositBuy, depositSell);

        return (newDeposit <= currentDeposit) ? -1 : newDeposit;
    }


    /**
     * Update portfolio for one user
     * 1. Un-hold pending size
     * 2. Reduce opposite position accordingly (if exists)
     * 3. Increase forward position accordingly (if size left in the trading event)
     */
    public void updatePortfolioForMarginTrade(OrderAction action, long size, long price, final long commission) {

        // 1. Un-hold pending size
        pendingRelease(action, size);

        // 2. Reduce opposite position accordingly (if exists)
        final long sizeToOpen = closeCurrentPositionFutures(action, size, price);

        // 3. Increase forward position accordingly (if size left in the trading event)
        if (sizeToOpen > 0) {
            openPositionFutures(action, sizeToOpen, price, commission);
        }
    }

    private long closeCurrentPositionFutures(final OrderAction action, final long tradeSize, final long tradePrice) {

        // log.debug("{} {} {} {} cur:{}-{} profit={}", uid, action, tradeSize, tradePrice, position, totalSize, profit);

        if (position == PortfolioPosition.EMPTY || position == PortfolioPosition.of(action)) {
            // nothing to close
            return tradeSize;
        }

        if (openVolume > tradeSize) {
            // current position is bigger than trade size - just reduce position accordingly
            openVolume -= tradeSize;
            openPriceSum -= tradeSize * tradePrice;
            return 0;
        }

        // current position smaller than trade size, can close completely and calculate profit
        profit = (openPriceSum - openVolume * tradePrice) * position.getMultiplier();
        openPriceSum = 0;

        position = PortfolioPosition.EMPTY;

        final long sizeToOpen = tradeSize - openVolume;
        openVolume = 0;

        // validateInternalState();

        return sizeToOpen;
    }

    private void openPositionFutures(OrderAction action, long sizeToOpen, long tradePrice, long commission) {
        openVolume += sizeToOpen;
        openPriceSum += tradePrice * sizeToOpen;
        position = PortfolioPosition.of(action);
        profit -= commission * sizeToOpen;

        // validateInternalState();
    }

    public void reset() {

        // log.debug("records: {}, Pending B{} S{} total size: {}", records.size(), pendingBuySize, pendingSellSize, totalSize);

        pendingBuySize = 0;
        pendingSellSize = 0;

        openVolume = 0;
        openPriceSum = 0;
        position = PortfolioPosition.EMPTY;
    }

    public void validateInternalState() {
        if (position == PortfolioPosition.EMPTY && (openVolume != 0 || openPriceSum != 0)) {
            log.error("uid {} : position:{} totalSize:{} openPriceSum:{}", uid, position, openVolume, openPriceSum);
            throw new IllegalStateException();
        }
        if (position != PortfolioPosition.EMPTY && (openVolume <= 0 || openPriceSum <= 0)) {
            log.error("uid {} : position:{} totalSize:{} openPriceSum:{}", uid, position, openVolume, openPriceSum);
            throw new IllegalStateException();
        }

        if (pendingSellSize < 0 || pendingBuySize < 0) {
            log.error("uid {} : pendingSellSize:{} pendingBuySize:{}", uid, pendingSellSize, pendingBuySize);
            throw new IllegalStateException();
        }
    }

}
