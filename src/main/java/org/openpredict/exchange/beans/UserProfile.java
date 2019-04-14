package org.openpredict.exchange.beans;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

@Slf4j
public final class UserProfile {

    public final long uid;

    // symbol -> portfolio records
    public IntObjectHashMap<SymbolPortfolioRecord> portfolio = new IntObjectHashMap<>();

    // set of applied transactionId
    public LongHashSet externalTransactions = new LongHashSet();

    // collected from accounts
    public long futuresBalance = 0L;

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

    public SymbolPortfolioRecord getOrCreatePortfolioRecord(int symbol) {
        SymbolPortfolioRecord record = portfolio.get(symbol);
        if (record == null) {
            record = new SymbolPortfolioRecord(symbol, uid);
            portfolio.put(symbol, record);
        }
        return record;
    }


    public void removeRecordIfEmpty(SymbolPortfolioRecord record) {
        if (record.isEmpty()) {
            futuresBalance += record.profit;
            portfolio.removeKey(record.symbol);
        }
    }

    @Override
    public String toString() {
        return "UserProfile{" +
                "uid=" + uid +
                ", portfolios=" + portfolio.size() +
                ", futuresBalance=" + futuresBalance +
                ", accounts=" + accounts +
                ", commandsCounter=" + commandsCounter +
                '}';
    }
}
