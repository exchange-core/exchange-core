package org.openpredict.exchange.beans;

import com.google.common.base.Strings;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Arrays;

@ToString
@EqualsAndHashCode
public class L2MarketData {

    public int askSize;
    public int bidSize;

    public long totalVolumeAsk;
    public long totalVolumeBid;

    public int askPrices[];
    public long askVolumes[];
    public int bidPrices[];
    public long bidVolumes[];

    public L2MarketData(int[] askPrices, long[] askVolumes, int[] bidPrices, long[] bidVolumes) {
        this.askPrices = askPrices;
        this.askVolumes = askVolumes;
        this.bidPrices = bidPrices;
        this.bidVolumes = bidVolumes;

        this.askSize = askPrices.length;
        this.bidSize = bidPrices.length;
    }

    public L2MarketData(int preAllocatedSize) {
        this.askPrices = new int[preAllocatedSize];
        this.bidPrices = new int[preAllocatedSize];
        this.askVolumes = new long[preAllocatedSize];
        this.bidVolumes = new long[preAllocatedSize];
    }

    public String dumpOrderBook() {
        L2MarketData l2MarketData = this;
        int priceWith = maxWidth(2, l2MarketData.askPrices, l2MarketData.bidPrices);
        int volWith = maxWidth(2, l2MarketData.askVolumes, l2MarketData.bidVolumes);

        StringBuilder s = new StringBuilder("Order book:\n");
        s.append(".").append(Strings.repeat("-", priceWith - 2)).append("ASKS").append(Strings.repeat("-", volWith - 1)).append(".\n");
        for (int i = l2MarketData.askPrices.length - 1; i >= 0; i--) {
            String price = Strings.padStart(String.valueOf(l2MarketData.askPrices[i]), priceWith, ' ');
            String volume = Strings.padStart(String.valueOf(l2MarketData.askVolumes[i]), volWith, ' ');
            s.append(String.format("|%s|%s|\n", price, volume));
        }
        s.append("|").append(Strings.repeat("-", priceWith)).append("+").append(Strings.repeat("-", volWith)).append("|\n");
        for (int i = 0; i < l2MarketData.bidPrices.length; i++) {
            String price = Strings.padStart(String.valueOf(l2MarketData.bidPrices[i]), priceWith, ' ');
            String volume = Strings.padStart(String.valueOf(l2MarketData.bidVolumes[i]), volWith, ' ');
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


}
