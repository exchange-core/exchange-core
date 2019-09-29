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
package exchange.core2.core.common;

import com.google.common.base.Strings;
import lombok.ToString;

import java.util.Arrays;

/**
 * L2 Market Data carrier object
 * <p>
 * NOTE: Can have dirty data, askSize and bidSize are important!
 */
@ToString
public final class L2MarketData {

    public static final int L2_SIZE = 32;

    public int askSize;
    public int bidSize;

    public long[] askPrices;
    public long[] askVolumes;
    public long[] bidPrices;
    public long[] bidVolumes;

    // when published
    public long timestamp;
    public long referenceSeq;

//    public long totalVolumeAsk;
//    public long totalVolumeBid;

    public L2MarketData(long[] askPrices, long[] askVolumes, long[] bidPrices, long[] bidVolumes) {
        this.askPrices = askPrices;
        this.askVolumes = askVolumes;
        this.bidPrices = bidPrices;
        this.bidVolumes = bidVolumes;

        this.askSize = askPrices.length;
        this.bidSize = bidPrices.length;
    }

    public L2MarketData(int askSize, int bidSize) {
        this.askPrices = new long[askSize];
        this.bidPrices = new long[bidSize];
        this.askVolumes = new long[askSize];
        this.bidVolumes = new long[bidSize];
    }

    public long[] getAskPricesCopy() {
        return Arrays.copyOf(askPrices, askSize);
    }

    public long[] getAskVolumesCopy() {
        return Arrays.copyOf(askVolumes, askSize);
    }

    public long[] getBidPricesCopy() {
        return Arrays.copyOf(bidPrices, bidSize);
    }

    public long[] getBidVolumesCopy() {
        return Arrays.copyOf(bidVolumes, bidSize);
    }

    public String dumpOrderBook() {
        int priceWith = maxWidth(2, Arrays.copyOf(askPrices, askSize), Arrays.copyOf(bidPrices, bidSize));
        int volWith = maxWidth(2, Arrays.copyOf(askVolumes, askSize), Arrays.copyOf(bidVolumes, bidSize));

        StringBuilder s = new StringBuilder("Order book:\n");
        s.append(".").append(Strings.repeat("-", priceWith - 2)).append("ASKS").append(Strings.repeat("-", volWith - 1)).append(".\n");
        for (int i = askSize - 1; i >= 0; i--) {
            String price = Strings.padStart(String.valueOf(this.askPrices[i]), priceWith, ' ');
            String volume = Strings.padStart(String.valueOf(this.askVolumes[i]), volWith, ' ');
            s.append(String.format("|%s|%s|\n", price, volume));
        }
        s.append("|").append(Strings.repeat("-", priceWith)).append("+").append(Strings.repeat("-", volWith)).append("|\n");
        for (int i = 0; i < bidSize; i++) {
            String price = Strings.padStart(String.valueOf(this.bidPrices[i]), priceWith, ' ');
            String volume = Strings.padStart(String.valueOf(this.bidVolumes[i]), volWith, ' ');
            s.append(String.format("|%s|%s|\n", price, volume));
        }
        s.append("'").append(Strings.repeat("-", priceWith - 2)).append("BIDS").append(Strings.repeat("-", volWith - 1)).append("'\n");
        return s.toString();
    }

    private static int maxWidth(int minWidth, long[]... arrays) {
        return Arrays.stream(arrays)
                .flatMapToLong(Arrays::stream)
                .mapToInt(p -> String.valueOf(p).length())
                .max()
                .orElse(minWidth);
    }

    private static int maxWidth(int minWidth, int[]... arrays) {
        return Arrays.stream(arrays)
                .flatMapToInt(Arrays::stream)
                .map(p -> String.valueOf(p).length())
                .max()
                .orElse(minWidth);
    }

    public L2MarketData copy() {
        return new L2MarketData(
                getAskPricesCopy(),
                getAskVolumesCopy(),
                getBidPricesCopy(),
                getBidVolumesCopy());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof L2MarketData)) {
            return false;
        }
        L2MarketData o = (L2MarketData) obj;

        if (askSize != o.askSize || bidSize != o.bidSize) {
            return false;
        }

        for (int i = 0; i < askSize; i++) {
            if (askPrices[i] != o.askPrices[i] || askVolumes[i] != o.askVolumes[i]) {
                return false;
            }
        }
        for (int i = 0; i < bidSize; i++) {
            if (bidPrices[i] != o.bidPrices[i] || bidVolumes[i] != o.bidVolumes[i]) {
                return false;
            }
        }
        return true;

    }

    // TODO hashcode
}
