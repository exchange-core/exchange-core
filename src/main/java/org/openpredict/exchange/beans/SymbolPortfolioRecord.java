package org.openpredict.exchange.beans;


import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.openpredict.exchange.core.RiskEngine;

import java.util.Objects;

@Slf4j
public final class SymbolPortfolioRecord implements WriteBytesMarshallable, StateHash {

    public final long uid;

    public final int symbol;
    public final int currency;

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

    public SymbolPortfolioRecord(long uid, int symbol, int currency) {
        this.uid = uid;

        this.symbol = symbol;
        this.currency = currency;
    }

    public SymbolPortfolioRecord(long uid, BytesIn bytes) {
        this.uid = uid;

        this.symbol = bytes.readInt();
        this.currency = bytes.readInt();
        this.position = PortfolioPosition.of(bytes.readByte());
        this.openVolume = bytes.readLong();
        this.openPriceSum = bytes.readLong();
        this.profit = bytes.readLong();
        this.pendingSellSize = bytes.readLong();
        this.pendingBuySize = bytes.readLong();
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

    public long estimateProfit(final CoreSymbolSpecification spec, final RiskEngine.LastPriceCacheRecord lastPriceCacheRecord) {
        switch (position) {
            case EMPTY:
                return profit;
            case LONG:
                return profit + ((lastPriceCacheRecord != null && lastPriceCacheRecord.bidPrice != 0)
                        ? (openVolume * lastPriceCacheRecord.bidPrice - openPriceSum)
                        : spec.marginBuy * openVolume); // unknown price - no liquidity - require extra margin
            case SHORT:
                return profit + ((lastPriceCacheRecord != null && lastPriceCacheRecord.askPrice != Long.MAX_VALUE)
                        ? (openPriceSum - openVolume * lastPriceCacheRecord.askPrice)
                        : spec.marginSell * openVolume); // unknown price - no liquidity - require extra margin
            default:
                throw new IllegalStateException();
        }
    }

    /**
     * Calculate required margin based on specification and current position/orders
     *
     * @param spec
     * @return
     */
    public long calculateRequiredMarginForFutures(CoreSymbolSpecification spec) {
        final long specMarginBuy = spec.marginBuy;
        final long specMarginSell = spec.marginSell;

        final long signedPosition = openVolume * position.getMultiplier();
        final long currentRiskBuySize = pendingBuySize + signedPosition;
        final long currentRiskSellSize = pendingSellSize - signedPosition;

        final long marginBuy = specMarginBuy * currentRiskBuySize;
        final long marginSell = specMarginSell * currentRiskSellSize;
        // marginBuy or marginSell can be negative, but not both of them
        return Math.max(marginBuy, marginSell);
    }

    /**
     * Calculate required margin based on specification and current position/orders
     * considering extra size added to current position (or outstanding orders)
     *
     * @param spec   symbols specification
     * @param action order action
     * @param size   order size
     * @return -1 if order will reduce current exposure (no additional margin required), otherwise full margin for symbol position if order placed/executed
     */
    public long calculateRequiredMarginForOrder(final CoreSymbolSpecification spec, final OrderAction action, final long size) {
        final long specMarginBuy = spec.marginBuy;
        final long specMarginSell = spec.marginSell;

        final long signedPosition = openVolume * position.getMultiplier();
        final long currentRiskBuySize = pendingBuySize + signedPosition;
        final long currentRiskSellSize = pendingSellSize - signedPosition;

        long marginBuy = specMarginBuy * currentRiskBuySize;
        long marginSell = specMarginSell * currentRiskSellSize;
        // either marginBuy or marginSell can be negative (because of signedPosition), but not both of them
        final long currentMargin = Math.max(marginBuy, marginSell);

        if (action == OrderAction.BID) {
            marginBuy += spec.marginBuy * size;
        } else {
            marginSell += spec.marginSell * size;
        }

        // marginBuy or marginSell can be negative, but not both of them
        final long newMargin = Math.max(marginBuy, marginSell);

        return (newMargin <= currentMargin) ? -1 : newMargin;
    }


    /**
     * Update portfolio for one user
     * 1. Un-hold pending size
     * 2. Reduce opposite position accordingly (if exists)
     * 3. Increase forward position accordingly (if size left in the trading event)
     */
    public long updatePortfolioForMarginTrade(OrderAction action, long size, long price) {

        // 1. Un-hold pending size
        pendingRelease(action, size);

        // 2. Reduce opposite position accordingly (if exists)
        final long sizeToOpen = closeCurrentPositionFutures(action, size, price);

        // 3. Increase forward position accordingly (if size left in the trading event)
        if (sizeToOpen > 0) {
            openPositionFutures(action, sizeToOpen, price);
        }
        return sizeToOpen;
    }

    private long closeCurrentPositionFutures(final OrderAction action, final long tradeSize, final long tradePrice) {

        // log.debug("{} {} {} {} cur:{}-{} profit={}", uid, action, tradeSize, tradePrice, position, totalSize, profit);

        if (position == PortfolioPosition.EMPTY || position == PortfolioPosition.of(action)) {
            // nothing to close
            return tradeSize;
        }

        if (openVolume > tradeSize) {
            // current position is bigger than trade size - just reduce position accordingly, don't fix profit
            openVolume -= tradeSize;
            openPriceSum -= tradeSize * tradePrice;
            return 0;
        }

        // current position smaller than trade size, can close completely and calculate profit
        profit += (openVolume * tradePrice - openPriceSum) * position.getMultiplier();
        openPriceSum = 0;
        position = PortfolioPosition.EMPTY;
        final long sizeToOpen = tradeSize - openVolume;
        openVolume = 0;

        // validateInternalState();

        return sizeToOpen;
    }

    private void openPositionFutures(OrderAction action, long sizeToOpen, long tradePrice) {
        openVolume += sizeToOpen;
        openPriceSum += tradePrice * sizeToOpen;
        position = PortfolioPosition.of(action);

        // validateInternalState();
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeInt(symbol);
        bytes.writeInt(currency);
        bytes.writeByte((byte) position.getMultiplier());
        bytes.writeLong(openVolume);
        bytes.writeLong(openPriceSum);
        bytes.writeLong(profit);
        bytes.writeLong(pendingSellSize);
        bytes.writeLong(pendingBuySize);
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

    @Override
    public int stateHash() {
        return Objects.hash(symbol, currency, position.getMultiplier(), openVolume, openPriceSum, profit, pendingSellSize, pendingBuySize);
    }

    @Override
    public String toString() {
        return "SPR{" +
                "u" + uid +
                " sym" + symbol +
                " cur" + currency +
                " pos" + position +
                " Σv=" + openVolume +
                " Σp=" + openPriceSum +
                " pnl=" + profit +
                " pendingS=" + pendingSellSize +
                " pendingB=" + pendingBuySize +
                '}';
    }
}
