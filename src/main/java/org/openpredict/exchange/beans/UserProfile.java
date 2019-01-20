package org.openpredict.exchange.beans;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;

@Slf4j
public class UserProfile {

    public final long uid;

    // symbol -> portfolio records
    private IntObjectHashMap<SymbolPortfolio> portfolios = new IntObjectHashMap<>();

    // transactionId -> amount
    public LongLongHashMap externalTransactions = new LongLongHashMap();

    // collected from accounts
    public long fastBalance = 0L;

    // collected from portfolio
    // TODO change to cached guaranteed available funds based on current position?
    public long fastMargin = 0L;

    public long commandsCounter = 0L;

    public UserProfile(long uid) {
        //log.debug("New {}", uid);
        this.uid = uid;
    }


    public SymbolPortfolio getOrCreatePortfolio(int symbol) {
        SymbolPortfolio portfolio = portfolios.get(symbol);
        if (portfolio == null) {
            portfolio = new SymbolPortfolio(symbol, uid);
            portfolios.put(symbol, portfolio);
        }
        return portfolio;
    }

    public void removePortfolioIfEmpty(SymbolPortfolio portfolio) {
        if (portfolio.isEmpty()) {
            portfolios.removeKey(portfolio.symbol);
        }
    }

    public void clear() {
//        log.debug("{} Portfolio size: {}, commands {}, fastBalance: {}", uid, portfolio.size(), commandsCounter.longValue(), fastBalance);
        portfolios.forEach(SymbolPortfolio::reset);
        commandsCounter = 0;
        // TODO clear margin?
    }


    @Override
    public String toString() {
        return "UserProfile{" +
                "uid=" + uid +
                ", portfolios=" + portfolios.size() +
                ", fastBalance=" + fastBalance +
                ", commandsCounter=" + commandsCounter +
                '}';
    }
}
