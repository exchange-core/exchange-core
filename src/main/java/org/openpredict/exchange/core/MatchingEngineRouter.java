package org.openpredict.exchange.core;

import com.lmax.disruptor.dsl.Disruptor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.openpredict.exchange.beans.L2MarketData;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;

@Service
@Slf4j
public class MatchingEngineRouter {

    public static final int L2_MARKET_DATA_SIZE = 20;

    @Autowired // TODO better service name?
    private ExchangeCore apiCore;

    //private Disruptor<MatcherTradeEvent> tradesDisruptor;

    @Value("${matchingEngine.orderCommands.bufferSize}")
    private int commandsBufferSize;

    @Value("${matchingEngine.symbolCodes}")
    private List<Integer> symbolCodes;

    private Disruptor<L2MarketData> marketDataDisruptor;

    // symbol->OB
    private IntObjectHashMap<IOrderBook> orderBooks = new IntObjectHashMap<>();

    public void processOrder(OrderCommand cmd) {
        IOrderBook orderBook = orderBooks.get(cmd.symbol);
        if (orderBook != null) {
            orderBook.processCommand(cmd);
        } else {
            cmd.resultCode = CommandResultCode.MATCHING_INVALID_ORDER_ID;
        }

    }

    public void addOrderBook(int symbol) {
        IOrderBook orderBook = IOrderBook.newInstance();
        orderBooks.put(symbol, orderBook);
    }

    public IOrderBook getOrderBook(int symbol){
        return orderBooks.get(symbol);
    }

    public L2MarketData getMarketData(int symbol, int size) {
        return orderBooks.get(symbol).getL2MarketDataSnapshot(size);
    }

    @PostConstruct
    public void start() {
        symbolCodes.forEach(this::addOrderBook);
    }

    @PreDestroy
    public void stop() {
        log.info("Stopping Matching Engine disruptors...");
        //marketDataDisruptor.shutdown();
        //matchingEngines.values().forEach(MatchingEngine::stop);
        log.info("Matching Engine disruptors stopped");
    }

    public void reset() {
        orderBooks.forEach(IOrderBook::clear);
    }

}
