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

    // symbol -> portfolio records
    public final IntObjectHashMap<SymbolPortfolioRecord> portfolio;
    public final IntObjectHashMap<SymbolPortfolioRecordExchange> exchangePortfolio;

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
        this.exchangePortfolio = new IntObjectHashMap<>();
        this.externalTransactions = new LongHashSet();
        this.accounts = new IntLongHashMap();
    }

    public UserProfile(BytesIn bytesIn) {

        this.uid = bytesIn.readLong();

        // positions
        this.portfolio = Utils.readIntHashMap(bytesIn, b -> new SymbolPortfolioRecord(uid, b));
        this.exchangePortfolio = Utils.readIntHashMap(bytesIn, b -> new SymbolPortfolioRecordExchange(uid, b));

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

    public SymbolPortfolioRecordExchange getOrCreatePortfolioRecordExch(CoreSymbolSpecification spec) {
        final int symbol = spec.symbolId;
        SymbolPortfolioRecordExchange record = exchangePortfolio.get(symbol);
        if (record == null) {
            record = new SymbolPortfolioRecordExchange(uid, symbol, spec.quoteCurrency);
            exchangePortfolio.put(symbol, record);
        }
        return record;
    }

    public SymbolPortfolioRecordExchange getPortfolioExchRecordOrThrowEx(int symbol) {
        final SymbolPortfolioRecordExchange record = exchangePortfolio.get(symbol);
        if (record == null) {
            throw new IllegalStateException("not found portfolio for symbol " + symbol);
        }
        return record;
    }

    public void removeRecordIfEmpty(SymbolPortfolioRecordExchange record) {
        if (record.isEmpty()) {
            portfolio.removeKey(record.symbol);
        }
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {

        bytes.writeLong(uid);

        // positions
        Utils.marshallIntHashMap(portfolio, bytes);
        Utils.marshallIntHashMap(exchangePortfolio, bytes);

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
                ", exchangePortfolios=" + exchangePortfolio.size() +
                ", accounts=" + accounts +
                ", commandsCounter=" + commandsCounter +
                '}';
    }

    @Override
    public int stateHash() {
        return Objects.hash(
                uid,
                Utils.stateHash(portfolio),
                Utils.stateHash(exchangePortfolio),
                externalTransactions.hashCode(),
                accounts.hashCode());
    }
}
