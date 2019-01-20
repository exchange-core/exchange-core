package org.openpredict.exchange.beans;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Builder
@ToString
@AllArgsConstructor
@Slf4j
public class SymbolPortfolioRecord {

    public final int symbol;
    public final long uid;

    // open positions state
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

        if (pendingSellSize < 0 || pendingBuySize < 0) {
            log.error("uid {} : pendingSellSize:{} pendingBuySize:{}", uid, pendingSellSize, pendingBuySize);
        }
    }

    public long estimateProfit(CoreSymbolSpecification spec) {
        if (spec.lastPrice == 0) {
            // unknown price - no liquidity - use extra deposit
            return profit + (position == PortfolioPosition.LONG ? spec.depositBuy : spec.depositSell) * openVolume;

        } else {
            return profit + (openPriceSum - openVolume * spec.lastPrice) * position.getMultiplier();
        }
    }

    /**
     * Update portfolio for one user
     * 1. Un-hold pending size
     * 2. Reduce opposite position accordingly (if exists)
     * 3. Increase forward position accordingly (if size left in the trading event)
     */
    public void updatePortfolioForTrade(OrderAction action, long size, long price, final long commission) {

        // 1. Un-hold pending size
        pendingRelease(action, size);

        // 2. Reduce opposite position accordingly (if exists)
        final long sizeToOpen = closeCurrentPosition(action, size, price);

        // 3. Increase forward position accordingly (if size left in the trading event)
        openPosition(action, sizeToOpen, price, commission);
    }


    private long closeCurrentPosition(final OrderAction action, final long tradeSize, final long tradePrice) {
//        if(uid == 196) {
//            log.debug("{} {} {} {} cur:{}-{} profit={}", uid, action, tradeSize, tradePrice, position, totalSize, profit);
//        }

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

    private void openPosition(OrderAction action, long sizeToOpen, long tradePrice, long commission) {
        if (sizeToOpen > 0) {
            openVolume += sizeToOpen;
            openPriceSum += tradePrice * sizeToOpen;
            position = PortfolioPosition.of(action);
            profit -= commission * sizeToOpen;
        }

//        validateInternalState();
    }

    public void reset() {

//        log.debug("records: {}, Pending B{} S{} total size: {}", records.size(), pendingBuySize, pendingSellSize, totalSize);

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
