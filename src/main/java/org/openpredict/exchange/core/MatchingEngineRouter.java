package org.openpredict.exchange.core;

import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.extern.slf4j.Slf4j;
import org.openpredict.exchange.beans.CfgWaitStrategyType;
import org.openpredict.exchange.beans.L2MarketData;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.Executors;

@Service
@Slf4j
public class MatchingEngineRouter {

    public static final int L2_MARKET_DATA_SIZE = 20;

    @Autowired // TODO better service name?
    private ExchangeCore apiCore;

    //private Disruptor<MatcherTradeEvent> tradesDisruptor;

    @Value("${matchingEngine.orderCommands.bufferSize}")
    private int commandsBufferSize;

    private Disruptor<L2MarketData> marketDataDisruptor;

    // symbol->OB
    //private Map<Integer, MatchingEngine> matchingEngines = new HashMap<>();
    private IOrderBook orderBook;

    //private IntObjectHashMap<RingBuffer<OrderCommand>> buffers = new IntObjectHashMap<>();

    public void processOrder(OrderCommand cmd) {

            // TODO fix
            orderBook.processCommand(cmd);
//        RingBuffer<OrderCommand> ringBuffer = buffers.get(cmd.symbol);
//        ringBuffer.publishEvent((to, seq, from) -> from.copyTo(to), cmd);

    }

    public void addOrderBook(int symbol) {

        orderBook = IOrderBook.newInstance();

    }

    public L2MarketData getMarketData(int symbol, int size) {
        return orderBook.getL2MarketDataSnapshot(size);
    }

    @PostConstruct
    public void start() {

//        // init Disruptor
//        marketDataDisruptor = new Disruptor<>(
//                () -> new L2MarketData(L2_MARKET_DATA_SIZE),
//                32768,
//                Executors.defaultThreadFactory(),
//                ProducerType.MULTI, // multiple order books can write
//                CfgWaitStrategyType.YIELDING.create());
//
//        marketDataDisruptor.handleEventsWith((evt, seq, eob) -> {
//        });
//        marketDataDisruptor.setDefaultExceptionHandler(new DisruptorExceptionHandler<>("l2 market data"));
//        marketDataDisruptor.start();
    }

    @PreDestroy
    public void stop() {
        log.info("Stopping Matching Engine disruptors...");
        marketDataDisruptor.shutdown();
        //matchingEngines.values().forEach(MatchingEngine::stop);
        log.info("Matching Engine disruptors stopped");
    }

    // TESTING ONLY
//    public void reset() {
//        matchingEngines.values().forEach(MatchingEngine::reset);
//    }
    public void reset() {
        orderBook.clear();
    }

}
