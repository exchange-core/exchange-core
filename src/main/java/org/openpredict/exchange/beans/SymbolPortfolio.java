package org.openpredict.exchange.beans;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;

@Builder
@ToString
@AllArgsConstructor
@Slf4j
public class SymbolPortfolio {

    public final int symbol;
    public final long uid;

    // open positions state
    public PortfolioPosition position = PortfolioPosition.EMPTY;
    public long totalSize = 0;
    public long acquireAmountSum = 0; //

    // pending orders total size
    // increment before sending order to matching engine
    // decrement after receiving trade confirmation from matching engine
    public long pendingSellSize = 0;
    public long pendingBuySize = 0;

    // TODO use external implementation (Eclipse?)
    // portfolio records array queue (processed as FIFO)
    public long[] portfolioVolumes = new long[64];
    public long[] portfolioPrices = new long[64];
    public int portfolioTail = 0;
    public int portfolioHead = 0;
    public int portfolioSize = 0;

    public SymbolPortfolio(int symbol, long uid) {
        this.symbol = symbol;
        this.uid = uid;
    }

    /**
     * Check if portfolio is empty - can remove it from hashmap
     *
     * @return true if portfolio is empty (no pending orders, no open trades)
     */
    public boolean isEmpty() {
        return position == PortfolioPosition.EMPTY
                && pendingSellSize == 0
                && pendingBuySize == 0;
    }

    public void validateInternalState() {
        if (position == PortfolioPosition.EMPTY && (totalSize != 0 || acquireAmountSum != 0)) {
            log.error("uid {} : position:{} totalSize:{} acquireAmountSum:{}", uid, position, totalSize, acquireAmountSum);
            throw new IllegalStateException();
        }
        if (position != PortfolioPosition.EMPTY && (totalSize == 0 || acquireAmountSum == 0)) {
            log.error("uid {} : position:{} totalSize:{} acquireAmountSum:{}", uid, position, totalSize, acquireAmountSum);
            throw new IllegalStateException();
        }
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

    public long reducePositionIfOppositeAction(long remainingVolume, OrderAction action) {

        if (position.isOppositeToAction(action)) {
            // remove position until position is empty or no trade size left (whichever comes first)
            while (remainingVolume != 0 && portfolioHasElement()) {
                final long portfRecordVol = headVolume();
                if (portfRecordVol <= remainingVolume) {
                    // portfolio record is smaller than size left, can remove completely
                    totalSize -= portfRecordVol;
                    acquireAmountSum -= headPrice() * portfRecordVol;
                    remainingVolume -= portfRecordVol;
                    portfolioRemove();
                } else {
                    // portfolio record has bigger size than we need - reduce size partially
                    reduceHeadVolume(remainingVolume);
                    totalSize -= remainingVolume;
                    acquireAmountSum -= headPrice() * remainingVolume;
                    remainingVolume = 0;
                }
            }
        }

        if (totalSize == 0) {
            position = PortfolioPosition.EMPTY;
        }

        return remainingVolume;
    }


    public void portfolioAdd(long price, long volume) {
        checkSize();
        portfolioVolumes[portfolioTail] = volume;
        portfolioPrices[portfolioTail] = price;
        portfolioSize++;
        portfolioTail++;
        if (portfolioTail == portfolioVolumes.length) {
            portfolioTail = 0;
        }
    }

    public long headVolume() {
        return portfolioVolumes[portfolioHead];
    }

    public void reduceHeadVolume(long size) {
        portfolioVolumes[portfolioHead] -= size;
    }

    public void incTailVolume(long size) {
        portfolioPrices[portfolioTail] += size;
    }

    public long headPrice() {
        return portfolioPrices[portfolioHead];
    }

    public long tailPrice() {
        return portfolioPrices[portfolioTail];
    }

    public boolean portfolioHasElement() {
        return portfolioSize > 0;
    }

    public void portfolioRemove() {
        portfolioSize--;
        portfolioHead++;
        if (portfolioHead == portfolioVolumes.length) {
            portfolioHead = 0;
        }
    }

    private void checkSize() {
        // check if no space left
        if (portfolioSize == portfolioVolumes.length) {
            portfolioVolumes = upsizeBuffer(portfolioVolumes, portfolioTail);
            portfolioPrices = upsizeBuffer(portfolioPrices, portfolioTail);
            portfolioTail += portfolioSize;
        }
    }


    // TODO test

    /*
     *  HT -- empty
     * [?][?][?][?][?][?][?][?]
     *
     *  H  T  --- entry in 0
     * [0][?][?][?][?][?][?][?]
     *
     *
     *           HT
     * [0][1][2][3][4][5][6][7]
     * ~~~~~~~~
     *           H                       T
     * [?][?][?][3][4][5][6][7][0][1][2][?][?][?][?][?]
     *                          ~~~~~~~

     * HT
     * [0][1][2][3][4][5][6][7]
     *
     *  H                       T
     * [0][1][2][3][4][5][6][7][?][?][?][?][?][?][?][?]
     *

     *                       TH
     * [0][1][2][3][4][5][6][7]
     *
     *                       H                       T
     * [0][1][2][3][4][5][6][7][0][1][2][3][4][5][6][7]
     * TODO is HT is in second half of array - its smarter to move HEAD, not TAIL
     *
     */
    private long[] upsizeBuffer(long[] array, int head) {
        long[] array2 = Arrays.copyOf(array, array.length * 2);
        System.arraycopy(array, 0, array2, array.length, head);
        return array2;
    }

    public void reset() {

//        log.debug("records: {}, Pending B{} S{} total size: {}", records.size(), pendingBuySize, pendingSellSize, totalSize);

        pendingBuySize = 0;
        pendingSellSize = 0;

        portfolioTail = 0;
        portfolioHead = 0;
        portfolioSize = 0;
        portfolioVolumes = new long[16];
        portfolioPrices = new long[16];

        totalSize = 0;
        acquireAmountSum = 0;
        position = PortfolioPosition.EMPTY;
    }

}
