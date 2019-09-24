package org.openpredict.exchange.beans;

import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.openpredict.exchange.core.Utils;

import java.util.Objects;

@Slf4j
public final class UserProfile implements WriteBytesMarshallable, StateHash {

    public final long uid;

    // symbol -> margin position records
    public final IntObjectHashMap<SymbolPositionRecord> positions;

    // set of applied transactionId
    public final LongHashSet externalTransactions;

    // collected from accounts

    // currency accounts
    // currency -> balance
    public final IntLongHashMap accounts;

    public long commandsCounter = 0L;

    public UserProfile(long uid) {
        //log.debug("New {}", uid);
        this.uid = uid;
        this.positions = new IntObjectHashMap<>();
        this.externalTransactions = new LongHashSet();
        this.accounts = new IntLongHashMap();
    }

    public UserProfile(BytesIn bytesIn) {

        this.uid = bytesIn.readLong();

        // positions
        this.positions = Utils.readIntHashMap(bytesIn, b -> new SymbolPositionRecord(uid, b));

        // externalTransactions
        this.externalTransactions = Utils.readLongHashSet(bytesIn);

        // account balances
        this.accounts = Utils.readIntLongHashMap(bytesIn);
    }

    public SymbolPositionRecord getOrCreatePositionRecord(CoreSymbolSpecification spec) {
        final int symbol = spec.symbolId;
        SymbolPositionRecord record = positions.get(symbol);
        if (record == null) {
            record = new SymbolPositionRecord(uid, symbol, spec.quoteCurrency);
            positions.put(symbol, record);
        }
        return record;
    }

    public SymbolPositionRecord getPositionRecordOrThrowEx(int symbol) {
        final SymbolPositionRecord record = positions.get(symbol);
        if (record == null) {
            throw new IllegalStateException("not found position for symbol " + symbol);
        }
        return record;
    }

    public void removeRecordIfEmpty(SymbolPositionRecord record) {
        if (record.isEmpty()) {
            accounts.addToValue(record.currency, record.profit);
            positions.removeKey(record.symbol);
        }
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {

        bytes.writeLong(uid);

        // positions
        Utils.marshallIntHashMap(positions, bytes);

        // externalTransactions
        Utils.marshallLongHashSet(externalTransactions, bytes);

        // account balances
        Utils.marshallIntLongHashMap(accounts, bytes);
    }


    @Override
    public String toString() {
        return "UserProfile{" +
                "uid=" + uid +
                ", positions=" + positions.size() +
                ", accounts=" + accounts +
                ", commandsCounter=" + commandsCounter +
                '}';
    }

    @Override
    public int stateHash() {
        return Objects.hash(
                uid,
                Utils.stateHash(positions),
                externalTransactions.hashCode(),
                accounts.hashCode());
    }
}
