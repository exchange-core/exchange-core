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

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.Objects;

public final class CoreSymbolSpecification implements WriteBytesMarshallable, StateHash {

    public final int symbolId;

    public final SymbolType type;

    // currency pair specification
    public final int baseCurrency;  // base currency
    public final int quoteCurrency; // quote/counter currency (OR futures contract currency)
    public final long baseScaleK;   // base currency amount multiplier (lot size in base currency units)
    public final long quoteScaleK;  // quote currency amount multiplier (step size in quote currency units)

    // fees per lot in quote? currency units
    public final long takerFee; // TODO check invariant: taker fee is not less than maker fee
    public final long makerFee;

    // margin settings (for type=FUTURES_CONTRACT only)
    public final long marginBuy;   // buy margin (quote currency)
    public final long marginSell;  // sell margin (quote currency)

    public CoreSymbolSpecification(BytesIn bytes) {
        this.symbolId = bytes.readInt();
        this.type = SymbolType.of(bytes.readByte());
        this.baseCurrency = bytes.readInt();
        this.quoteCurrency = bytes.readInt();
        this.baseScaleK = bytes.readLong();
        this.quoteScaleK = bytes.readLong();
        this.takerFee = bytes.readLong();
        this.makerFee = bytes.readLong();
        this.marginBuy = bytes.readLong();
        this.marginSell = bytes.readLong();
    }

/* NOT SUPPORTED YET:

//    order book limits -- for FUTURES only
//    public final long highLimit;
//    public final long lowLimit;

//    swaps -- not by
//    public final long longSwap;
//    public final long shortSwap;

// activity (inactive, active, expired)

  */

    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeInt(symbolId);
        bytes.writeByte(type.getCode());
        bytes.writeInt(baseCurrency);
        bytes.writeInt(quoteCurrency);
        bytes.writeLong(baseScaleK);
        bytes.writeLong(quoteScaleK);
        bytes.writeLong(takerFee);
        bytes.writeLong(makerFee);
        bytes.writeLong(marginBuy);
        bytes.writeLong(marginSell);
    }

    @Override
    public int stateHash() {
        return Objects.hash(
                symbolId,
                type.getCode(),
                baseCurrency,
                quoteCurrency,
                baseScaleK,
                quoteScaleK,
                takerFee,
                makerFee,
                marginBuy,
                marginSell);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CoreSymbolSpecification that = (CoreSymbolSpecification) o;
        return symbolId == that.symbolId &&
                baseCurrency == that.baseCurrency &&
                quoteCurrency == that.quoteCurrency &&
                baseScaleK == that.baseScaleK &&
                quoteScaleK == that.quoteScaleK &&
                takerFee == that.takerFee &&
                makerFee == that.makerFee &&
                marginBuy == that.marginBuy &&
                marginSell == that.marginSell &&
                type == that.type;
    }

    public CoreSymbolSpecification(
            int symbolId,
            SymbolType type,
            int baseCurrency,
            int quoteCurrency,
            long baseScaleK,
            long quoteScaleK,
            long takerFee,
            long makerFee,
            long marginBuy,
            long marginSell) {
        Objects.requireNonNull(type, "type is marked non-null but is null");
        this.symbolId = symbolId;
        this.type = type;
        this.baseCurrency = baseCurrency;
        this.quoteCurrency = quoteCurrency;
        this.baseScaleK = baseScaleK;
        this.quoteScaleK = quoteScaleK;
        this.takerFee = takerFee;
        this.makerFee = makerFee;
        this.marginBuy = marginBuy;
        this.marginSell = marginSell;
    }

    public static CoreSymbolSpecificationBuilder builder() {
        return new CoreSymbolSpecificationBuilder();
    }

    public int getSymbolId() {
        return this.symbolId;
    }

    public SymbolType getType() {
        return this.type;
    }

    public int getBaseCurrency() {
        return this.baseCurrency;
    }

    public int getQuoteCurrency() {
        return this.quoteCurrency;
    }

    public long getBaseScaleK() {
        return this.baseScaleK;
    }

    public long getQuoteScaleK() {
        return this.quoteScaleK;
    }

    public long getTakerFee() {
        return this.takerFee;
    }

    public long getMakerFee() {
        return this.makerFee;
    }

    public long getMarginBuy() {
        return this.marginBuy;
    }

    public long getMarginSell() {
        return this.marginSell;
    }

    @Override
    public String toString() {
        return "CoreSymbolSpecification(symbolId=" + this.getSymbolId()
                + ", type=" + this.getType()
                + ", baseCurrency=" + this.getBaseCurrency()
                + ", quoteCurrency=" + this.getQuoteCurrency()
                + ", baseScaleK=" + this.getBaseScaleK()
                + ", quoteScaleK=" + this.getQuoteScaleK()
                + ", takerFee=" + this.getTakerFee()
                + ", makerFee=" + this.getMakerFee()
                + ", marginBuy=" + this.getMarginBuy()
                + ", marginSell=" + this.getMarginSell() + ")";
    }

    public static class CoreSymbolSpecificationBuilder {
        private int symbolId;
        private SymbolType type;
        private int baseCurrency;
        private int quoteCurrency;
        private long baseScaleK;
        private long quoteScaleK;
        private long takerFee;
        private long makerFee;
        private long marginBuy;
        private long marginSell;

        CoreSymbolSpecificationBuilder() {
        }

        public CoreSymbolSpecificationBuilder symbolId(int symbolId) {
            this.symbolId = symbolId;
            return this;
        }

        public CoreSymbolSpecificationBuilder type(SymbolType type) {
            Objects.requireNonNull(type, "type is marked non-null but is null");
            this.type = type;
            return this;
        }

        public CoreSymbolSpecificationBuilder baseCurrency(int baseCurrency) {
            this.baseCurrency = baseCurrency;
            return this;
        }

        public CoreSymbolSpecificationBuilder quoteCurrency(int quoteCurrency) {
            this.quoteCurrency = quoteCurrency;
            return this;
        }

        public CoreSymbolSpecificationBuilder baseScaleK(long baseScaleK) {
            this.baseScaleK = baseScaleK;
            return this;
        }

        public CoreSymbolSpecificationBuilder quoteScaleK(long quoteScaleK) {
            this.quoteScaleK = quoteScaleK;
            return this;
        }

        public CoreSymbolSpecificationBuilder takerFee(long takerFee) {
            this.takerFee = takerFee;
            return this;
        }

        public CoreSymbolSpecificationBuilder makerFee(long makerFee) {
            this.makerFee = makerFee;
            return this;
        }

        public CoreSymbolSpecificationBuilder marginBuy(long marginBuy) {
            this.marginBuy = marginBuy;
            return this;
        }

        public CoreSymbolSpecificationBuilder marginSell(long marginSell) {
            this.marginSell = marginSell;
            return this;
        }

        public CoreSymbolSpecification build() {
            return new CoreSymbolSpecification(
                    this.symbolId,
                    this.type,
                    this.baseCurrency,
                    this.quoteCurrency,
                    this.baseScaleK,
                    this.quoteScaleK,
                    this.takerFee,
                    this.makerFee,
                    this.marginBuy,
                    this.marginSell);
        }

        @Override
        public String toString() {
            return "CoreSymbolSpecification.CoreSymbolSpecificationBuilder(symbolId=" + this.symbolId
                    + ", type=" + this.type
                    + ", baseCurrency=" + this.baseCurrency
                    + ", quoteCurrency=" + this.quoteCurrency
                    + ", baseScaleK=" + this.baseScaleK
                    + ", quoteScaleK=" + this.quoteScaleK
                    + ", takerFee=" + this.takerFee
                    + ", makerFee=" + this.makerFee
                    + ", marginBuy=" + this.marginBuy
                    + ", marginSell=" + this.marginSell + ")";
        }
    }
}
