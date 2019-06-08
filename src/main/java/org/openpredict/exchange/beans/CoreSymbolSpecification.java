package org.openpredict.exchange.beans;


import lombok.*;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.io.Serializable;
import java.util.Objects;

@Builder
@AllArgsConstructor
@Getter
@ToString
public final class CoreSymbolSpecification implements Serializable, WriteBytesMarshallable, StateHash {

    public final int symbolId;

    @NonNull
    public final SymbolType type;

    // currency pair specification
    public final int baseCurrency;  // base currency
    public final int quoteCurrency; // quote/counter currency (OR futures contract currency)
    public final long baseScaleK;   // base currency amount multiplier
    public final long quoteScaleK;  // quote currency amount multiplier

    // deposit settings (for type=FUTURES_CONTRACT only)
    public final long depositBuy;   // buy margin (quote currency)
    public final long depositSell;  // sell margin (quote currency)

    // fees (per lot)
    public final long takerFee;
    public final long makerFee;
    // TODO public final int feeCurrency; //  if type=CURRENCY_EXCHANGE_PAIR - should be the same as quoteCurrency

    public CoreSymbolSpecification(BytesIn bytes) {
        this.symbolId = bytes.readInt();
        this.type = SymbolType.of(bytes.readByte());
        this.baseCurrency = bytes.readInt();
        this.quoteCurrency = bytes.readInt();
        this.baseScaleK = bytes.readLong();
        this.quoteScaleK = bytes.readLong();
        this.depositBuy = bytes.readLong();
        this.depositSell = bytes.readLong();
        this.takerFee = bytes.readLong();
        this.makerFee = bytes.readLong();
    }

/* NOT SUPPORTED YET:

    // lot size
//    public final long lotSize;
//    public final int stepSize;

    // order book limits
//    public final long highLimit;
//    public final long lowLimit;

    // swaps
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
        bytes.writeLong(depositBuy);
        bytes.writeLong(depositSell);
        bytes.writeLong(takerFee);
        bytes.writeLong(makerFee);
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
                depositBuy,
                depositSell,
                takerFee,
                makerFee);
    }
}
