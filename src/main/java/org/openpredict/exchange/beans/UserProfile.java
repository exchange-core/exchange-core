package org.openpredict.exchange.beans;

import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

@Slf4j
public final class UserProfile implements WriteBytesMarshallable {

    public final long uid;

    // symbol -> portfolio records
    public IntObjectHashMap<SymbolPortfolioRecord> portfolio = new IntObjectHashMap<>();

    // set of applied transactionId
    public LongHashSet externalTransactions = new LongHashSet();

    // collected from accounts

    // currency accounts
    // currency -> balance
    public IntLongHashMap accounts = new IntLongHashMap();


    // collected from portfolio
    // TODO change to cached guaranteed available funds based on current position?
    // public long fastMargin = 0L;

    public long commandsCounter = 0L;

    public UserProfile(long uid) {
        //log.debug("New {}", uid);
        this.uid = uid;
    }

    public SymbolPortfolioRecord getOrCreatePortfolioRecord(CoreSymbolSpecification spec) {
        final int symbol = spec.symbolId;
        SymbolPortfolioRecord record = portfolio.get(symbol);
        if (record == null) {
            record = new SymbolPortfolioRecord(symbol, uid, spec.quoteCurrency);
            portfolio.put(symbol, record);
        }
        return record;
    }

    public SymbolPortfolioRecord getPortfolioRecordOrThrowEx(int symbol) {
        final SymbolPortfolioRecord record = portfolio.get(symbol);
        if (record == null) {
            throw new IllegalStateException("not found portfolio for symbol " + symbol);
        }
        return record;
    }

    public void removeRecordIfEmpty(SymbolPortfolioRecord record) {
        if (record.isEmpty()) {
            accounts.addToValue(record.currency, record.profit);
            portfolio.removeKey(record.symbol);
        }
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {

        bytes.writeLong(uid);

        // positions
        bytes.writeInt(portfolio.size());
        portfolio.forEachKeyValue((k, v) -> {
            bytes.writeInt(k);
            v.writeMarshallable(bytes);
        });

        // externalTransactions
        bytes.writeInt(externalTransactions.size());
        externalTransactions.forEach(bytes::writeLong);

        bytes.writeInt(accounts.size());
        accounts.forEachKeyValue((currency, balance) -> {
            bytes.writeInt(currency);
            bytes.writeLong(balance);
        });

    }


    @Override
    public String toString() {
        return "UserProfile{" +
                "uid=" + uid +
                ", portfolios=" + portfolio.size() +
                ", accounts=" + accounts +
                ", commandsCounter=" + commandsCounter +
                '}';
    }
}
