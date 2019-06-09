package org.openpredict.exchange.beans;


import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.Objects;

/**
 * Does not contain position-related fields, because exchange trades lead to account transfers.
 */
@ToString
@Slf4j
public final class SymbolPortfolioRecordExchange implements WriteBytesMarshallable, StateHash {

    public final long uid;

    public final int symbol;
    public final int currency;

    // pending orders total size
    // increment before sending order to matching engine
    // decrement after receiving trade confirmation from matching engine
    public long pendingSellSize = 0;
    public long pendingBuyAmount = 0;

    public SymbolPortfolioRecordExchange(long uid, int symbol, int currency) {
        this.uid = uid;

        this.symbol = symbol;
        this.currency = currency;
    }

    public SymbolPortfolioRecordExchange(long uid, BytesIn bytes) {
        this.uid = uid;

        this.symbol = bytes.readInt();
        this.currency = bytes.readInt();
        this.pendingSellSize = bytes.readLong();
        this.pendingBuyAmount = bytes.readLong();
    }

    public void pendingHold(OrderAction orderAction, long size) {
        if (orderAction == OrderAction.ASK) {
            pendingSellSize += size;
        } else {
            pendingBuyAmount += size;
        }
    }

    public void pendingSellRelease(long size) {
        pendingSellSize -= size;
    }

    public void pendingBuyRelease(long amount) {
        pendingBuyAmount -= amount;
    }

    /**
     * Check if portfolio record is effectively empty (no pending orders) - can remove it from hash table
     *
     * @return true if portfolio is empty (no pending orders)
     */
    public boolean isEmpty() {
        return pendingSellSize == 0 && pendingBuyAmount == 0;
    }


    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeInt(symbol);
        bytes.writeInt(currency);

        bytes.writeLong(pendingSellSize);
        bytes.writeLong(pendingBuyAmount);
    }

    public void reset() {

        // log.debug("records: {}, Pending B{} S{} total size: {}", records.size(), pendingBuySize, pendingSellSize, totalSize);
        pendingBuyAmount = 0;
        pendingSellSize = 0;
    }

    public void validateInternalState() {
        if (pendingSellSize < 0 || pendingBuyAmount < 0) {
            log.error("uid {} : pendingSellSize:{} pendingBuyAmount:{}", uid, pendingSellSize, pendingBuyAmount);
            throw new IllegalStateException();
        }
    }

    @Override
    public int stateHash() {
        return Objects.hash(symbol, currency, pendingSellSize, pendingBuyAmount);
    }
}
