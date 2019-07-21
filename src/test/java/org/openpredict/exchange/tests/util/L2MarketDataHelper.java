package org.openpredict.exchange.tests.util;

import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;
import org.openpredict.exchange.beans.L2MarketData;

import java.util.Arrays;

@NoArgsConstructor
public class L2MarketDataHelper {

    private long[] askPrices;
    private long[] askVolumes;
    private long[] bidPrices;
    private long[] bidVolumes;

    public L2MarketDataHelper(L2MarketData l2) {
        askPrices = Arrays.copyOf(l2.askPrices, l2.askPrices.length);
        askVolumes = Arrays.copyOf(l2.askVolumes, l2.askVolumes.length);
        bidPrices = Arrays.copyOf(l2.bidPrices, l2.bidPrices.length);
        bidVolumes = Arrays.copyOf(l2.bidVolumes, l2.bidVolumes.length);
    }

    public L2MarketData build() {
        return new L2MarketData(
                askPrices,
                askVolumes,
                bidPrices,
                bidVolumes
        );
    }


    public L2MarketDataHelper setAskPrice(int pos, int askPrice) {
        askPrices[pos] = askPrice;
        return this;
    }

    public L2MarketDataHelper setBidPrice(int pos, int bidPrice) {
        bidPrices[pos] = bidPrice;
        return this;
    }

    public L2MarketDataHelper setAskVolume(int pos, long askVolume) {
        askVolumes[pos] = askVolume;
        return this;
    }

    public L2MarketDataHelper setBidVolume(int pos, long bidVolume) {
        bidVolumes[pos] = bidVolume;
        return this;
    }

    public L2MarketDataHelper setAskPriceVolume(int pos, int askPrice, long askVolume) {
        askVolumes[pos] = askVolume;
        askPrices[pos] = askPrice;
        return this;
    }

    public L2MarketDataHelper setBidPriceVolume(int pos, int bidPrice, long bidVolume) {
        bidVolumes[pos] = bidVolume;
        bidPrices[pos] = bidPrice;
        return this;
    }


    public L2MarketDataHelper removeAsk(int pos) {
        askPrices = ArrayUtils.remove(askPrices, pos);
        askVolumes = ArrayUtils.remove(askVolumes, pos);
        return this;
    }

    public L2MarketDataHelper removeAllAsks() {
        askPrices = new long[0];
        askVolumes = new long[0];
        return this;
    }

    public L2MarketDataHelper removeBid(int pos) {
        bidPrices = ArrayUtils.remove(bidPrices, pos);
        bidVolumes = ArrayUtils.remove(bidVolumes, pos);
        return this;
    }

    public L2MarketDataHelper removeAllBids() {
        bidPrices = new long[0];
        bidVolumes = new long[0];
        return this;
    }

    public L2MarketDataHelper insertAsk(int pos, int price, long volume) {
        askPrices = ArrayUtils.insert(pos, askPrices, price);
        askVolumes = ArrayUtils.insert(pos, askVolumes, volume);
        return this;
    }

    public L2MarketDataHelper insertBid(int pos, int price, long volume) {
        bidPrices = ArrayUtils.insert(pos, bidPrices, price);
        bidVolumes = ArrayUtils.insert(pos, bidVolumes, volume);
        return this;
    }

    public L2MarketDataHelper addAsk(int price, long volume) {
        askPrices = ArrayUtils.add(askPrices, price);
        askVolumes = ArrayUtils.add(askVolumes, volume);
        return this;
    }

    public L2MarketDataHelper addBid(int price, long volume) {
        bidPrices = ArrayUtils.add(bidPrices, price);
        bidVolumes = ArrayUtils.add(bidVolumes, volume);
        return this;
    }

}
