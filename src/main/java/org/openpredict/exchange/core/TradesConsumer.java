package org.openpredict.exchange.core;

import org.openpredict.exchange.beans.MatcherTradeEvent;

public interface TradesConsumer {

    void registerMatcherEvent(MatcherTradeEvent trade);

}
