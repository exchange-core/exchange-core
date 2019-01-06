package org.openpredict.exchange.beans;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;

@Slf4j
public class UserProfile {

    public final long uid;

    // symbol -> portfolio records
    public IntObjectHashMap<SymbolPortfolio> portfolio = new IntObjectHashMap<>();

    // transactionId -> amount
    public LongLongHashMap externalTransactions = new LongLongHashMap();

    // collected from accounts
    public long fastBalance = 0L;

    // collected from portfolio
    public long fastMargin = 0L;

    public long commandsCounter = 0L;

    public UserProfile(long uid) {
        //log.debug("New {}", uid);
        this.uid = uid;
    }

    public void clear() {
//        log.debug("{} Portfolio size: {}, commands {}, fastBalance: {}", uid, portfolio.size(), commandsCounter.longValue(), fastBalance);
        portfolio.forEach(SymbolPortfolio::reset);
        commandsCounter = 0;
        // TODO clear margin?
    }


    @Override
    public String toString() {
        return "UserProfile{" +
                "uid=" + uid +
                ", portfolio=" + portfolio +
                ", fastBalance=" + fastBalance +
                ", commandsCounter=" + commandsCounter +
                '}';
    }
}
