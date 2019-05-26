package org.openpredict.exchange.beans;

import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.openpredict.exchange.core.Utils;

@Slf4j
public final class UserProfile implements WriteBytesMarshallable {

    public final long uid;

    // symbol -> portfolio records
    public final IntObjectHashMap<SymbolPortfolioRecord> portfolio;

    // set of applied transactionId
    public final LongHashSet externalTransactions;

    // collected from accounts

    // currency accounts
    // currency -> balance
    public final IntLongHashMap accounts;


    // collected from portfolio
    // TODO change to cached guaranteed available funds based on current position?
    // public long fastMargin = 0L;

    public long commandsCounter = 0L;

    public UserProfile(long uid) {
        //log.debug("New {}", uid);
        this.uid = uid;
        this.portfolio = new IntObjectHashMap<>();
        this.externalTransactions = new LongHashSet();
        this.accounts = new IntLongHashMap();
    }

    public UserProfile(BytesIn bytesIn) {

        this.uid = bytesIn.readLong();

        // positions
        this.portfolio = Utils.readIntHashMap(bytesIn, b -> new SymbolPortfolioRecord(uid, b));

        // externalTransactions
        this.externalTransactions = Utils.readLongHashSet(bytesIn);

        // account balances
        this.accounts = Utils.readIntLongHashMap(bytesIn);
    }

    public SymbolPortfolioRecord getOrCreatePortfolioRecord(CoreSymbolSpecification spec) {
        final int symbol = spec.symbolId;
        SymbolPortfolioRecord record = portfolio.get(symbol);
        if (record == null) {
            record = new SymbolPortfolioRecord(uid, symbol, spec.quoteCurrency);
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
        Utils.marshallIntHashMap(portfolio, bytes);

        // externalTransactions
        Utils.marshallLongHashSet(externalTransactions, bytes);

        // account balances
        Utils.marshallIntLongHashMap(accounts, bytes);
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
