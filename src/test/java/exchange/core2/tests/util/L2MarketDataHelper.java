/*
 * Copyright 2019 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package exchange.core2.tests.util;

import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;
import exchange.core2.core.common.L2MarketData;

import java.util.Arrays;

@NoArgsConstructor
public class L2MarketDataHelper {

    private long[] askPrices;
    private long[] askVolumes;
    private long[] askOrders;
    private long[] bidPrices;
    private long[] bidVolumes;
    private long[] bidOrders;

    public L2MarketDataHelper(L2MarketData l2) {
        askPrices = Arrays.copyOf(l2.askPrices, l2.askPrices.length);
        askVolumes = Arrays.copyOf(l2.askVolumes, l2.askVolumes.length);
        askOrders = Arrays.copyOf(l2.askOrders, l2.askOrders.length);
        bidPrices = Arrays.copyOf(l2.bidPrices, l2.bidPrices.length);
        bidVolumes = Arrays.copyOf(l2.bidVolumes, l2.bidVolumes.length);
        bidOrders = Arrays.copyOf(l2.bidOrders, l2.bidOrders.length);
    }

    public L2MarketData build() {
        return new L2MarketData(
                askPrices,
                askVolumes,
                askOrders,
                bidPrices,
                bidVolumes,
                bidOrders
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

    public L2MarketDataHelper rollAskOrder(int pos, boolean inc) {
        askOrders[pos] += (inc ? 1 : -1);
        return this;
    }

    public L2MarketDataHelper rollBidOrder(int pos, boolean inc) {
        bidOrders[pos] += (inc ? 1 : -1);
        return this;
    }

    public L2MarketDataHelper removeAsk(int pos) {
        askPrices = ArrayUtils.remove(askPrices, pos);
        askVolumes = ArrayUtils.remove(askVolumes, pos);
        askOrders = ArrayUtils.remove(askOrders, pos);
        return this;
    }

    public L2MarketDataHelper removeAllAsks() {
        askPrices = new long[0];
        askVolumes = new long[0];
        askOrders = new long[0];
        return this;
    }

    public L2MarketDataHelper removeBid(int pos) {
        bidPrices = ArrayUtils.remove(bidPrices, pos);
        bidVolumes = ArrayUtils.remove(bidVolumes, pos);
        bidOrders = ArrayUtils.remove(bidOrders, pos);
        return this;
    }

    public L2MarketDataHelper removeAllBids() {
        bidPrices = new long[0];
        bidVolumes = new long[0];
        bidOrders = new long[0];
        return this;
    }

    public L2MarketDataHelper insertAsk(int pos, int price, long volume) {
        askPrices = ArrayUtils.insert(pos, askPrices, price);
        askVolumes = ArrayUtils.insert(pos, askVolumes, volume);
        askOrders = ArrayUtils.insert(pos, askOrders, 1);
        return this;
    }

    public L2MarketDataHelper insertBid(int pos, int price, long volume) {
        bidPrices = ArrayUtils.insert(pos, bidPrices, price);
        bidVolumes = ArrayUtils.insert(pos, bidVolumes, volume);
        bidOrders = ArrayUtils.insert(pos, bidOrders, 1);
        return this;
    }

    public L2MarketDataHelper addAsk(int price, long volume) {
        askPrices = ArrayUtils.add(askPrices, price);
        askVolumes = ArrayUtils.add(askVolumes, volume);
        askOrders = ArrayUtils.add(askOrders, 1);
        return this;
    }

    public L2MarketDataHelper addBid(int price, long volume) {
        bidPrices = ArrayUtils.add(bidPrices, price);
        bidVolumes = ArrayUtils.add(bidVolumes, volume);
        bidOrders = ArrayUtils.add(bidOrders, 1);
        return this;
    }

}
