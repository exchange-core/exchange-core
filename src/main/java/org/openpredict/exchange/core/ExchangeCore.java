package org.openpredict.exchange.core;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.openpredict.exchange.beans.CfgWaitStrategyType;
import org.openpredict.exchange.beans.MatcherTradeEvent;
import org.openpredict.exchange.beans.SymbolPortfolio;
import org.openpredict.exchange.beans.UserProfile;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.biprocessor.RefusingEventHandler;
import org.openpredict.exchange.biprocessor.SlaveProcessor;
import org.openpredict.exchange.biprocessor.TempProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

@Service
@Slf4j
public class ExchangeCore {

    @Autowired
    private PortfolioService portfolioService;

    @Autowired
    private UserProfileService userProfileService;

    @Autowired
    private MatchingEngineRouter matchingEngineRouter;

    @Autowired
    private RiskEngine riskEngine;

    @Autowired
    private JournallingProcessor journallingHandler;

    @Autowired
    @Setter
    private Consumer<OrderCommand> resultsConsumer;


    @Value("${riskEngine.orderCommands.bufferSize}")
    private int commandsBufferSize;
    @Value("${matchingEngine.tradingEvents.bufferSize}")
    private int tradeEventsBufferSize;

    private Disruptor<OrderCommand> disruptor;
    private RingBuffer<OrderCommand> cmdRingBuffer;

    //private long lastReleaseSeqProcessed = -1;

//    private SubProcessor<OrderCommand> r1;
//    private SubProcessor<OrderCommand> r2;

    TempProcessor<OrderCommand> procR1;
    SlaveProcessor<OrderCommand> procR2;

//    /**
//     * This handler logs values of third sequence barrier
//     */
//    class LoggingBarrierHandler implements BatchStartAware, EventHandler<OrderCommand> {
//
//        private long lastLoggedBarrierSeq;
//
//        @Setter
//        private SequenceBarrier loggedBarrier;
//
//        @Override
//        public void onBatchStart(long batchSize) {
//            if (batchSize < 1) {
//                // TODO submit fix for the com.lmax.disruptor so this is not required
//                return;
//            }
//            lastLoggedBarrierSeq = loggedBarrier.getCursor();
////            log.debug("onBatchStart: lastLoggedBarrierSeq={} batchSize={}", lastLoggedBarrierSeq, batchSize);
//        }
//
//        @Override
//        public void onEvent(OrderCommand cmd, long seq, boolean eob) {
//            cmd.availableEventSeq = lastLoggedBarrierSeq;
//            //log.debug("set {}.availableEventSeq={}", seq, lastLoggedBarrierSeq);
//        }
//    }


    @PostConstruct
    public void start() {
        disruptor = new Disruptor<>(
                OrderCommand::new,
                32 * 1024,
                Executors.defaultThreadFactory(),
                ProducerType.MULTI, // multiple gateway threads are writing
                CfgWaitStrategyType.YIELDING.create());

        disruptor.setDefaultExceptionHandler(new DisruptorExceptionHandler<>("main"));

//        LoggingBarrierHandler loggingBarrierHandler = new LoggingBarrierHandler();

        EventHandler<OrderCommand> matchingEngineHandler = (cmd, seq, eob) -> {
//            log.debug("ME processOrder {}", seq);
            matchingEngineRouter.processOrder(cmd);
        };

        EventHandler<OrderCommand> resultsHandler = (cmd, seq, eob) -> {
            //log.debug("RES HANDLER: seq:{} cmd:{}", seq, cmd);
            resultsConsumer.accept(cmd);
        };

        RefusingEventHandler<OrderCommand> handlerR1 = this::handleRiskHold;
        RefusingEventHandler<OrderCommand> handlerR2 = this::handlerRiskRelease;

        // 1. barierMarker
        //disruptor.handleEventsWith(loggingBarrierHandler);

        // 2. journalling (J) in parallel with risk hold (R1) + matching engine (ME)
        disruptor
                //.after(loggingBarrierHandler)
                .handleEventsWith(journallingHandler);


        disruptor
                //.after(loggingBarrierHandler)
                .handleEventsWith((ringBuffer, barrierSequences) -> {
                    procR1 = new TempProcessor<>(ringBuffer, ringBuffer.newBarrier(barrierSequences), handlerR1);
                    return procR1;
                });
        disruptor.after(procR1).handleEventsWith(matchingEngineHandler);


//        com.lmax.disruptor.after(journallingHandler).handleEventsWith(matchingEngineHandler);

        // 3. results handler after matching engine (ME) + journalling (J)
        disruptor.after(matchingEngineHandler, journallingHandler).then(resultsHandler);

        disruptor.after(matchingEngineHandler).then((ringBuffer, barrierSequences) -> {
            SequenceBarrier sequenceBarrier = ringBuffer.newBarrier(barrierSequences);
            procR2 = new SlaveProcessor<>(ringBuffer, sequenceBarrier, handlerR2);
            procR1.setSlaveProcessor(procR2);
            // R2 barrier is set as logged barrier
            //loggingBarrierHandler.setLoggedBarrier(sequenceBarrier);
            return procR2;
        });


        // TODO add second journalling handler?

        // create new master processor before starting disruptors
//        new MasterProcessor(r1, r2);

        log.debug("STARTING Disruptor");

        // starting disruptors
        disruptor.start();

        cmdRingBuffer = disruptor.getRingBuffer();


//        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(() -> {
//
//            long lg = disruptor.getBarrierFor(loggingBarrierHandler).getCursor();
//            long me = disruptor.getBarrierFor(matchingEngineHandler).getCursor();
//            long r1 = procR1.getSequence().get();
//            long r2 = procR2.getSequence().get();
//
////            log.debug("lg:{} r1:{} me:{} r2:{}", lg, r1.getSequence().get(), me, r2.getSequence().get());
////            log.debug("lg:{} r1:{} me:{} ", lg, r1.getSequence().get(), me);
//            log.debug("lg:{} r1:{} me:{} r2:{} D={}", lg, r1, me, r2, r1-r2);
//        }, 1001, 1000, TimeUnit.MILLISECONDS);

    }

    public RingBuffer<OrderCommand> getRingBuffer() {
        return cmdRingBuffer;
    }

    @PreDestroy
    public void stop() {
        disruptor.shutdown();
    }

    private void handleRiskHold(OrderCommand cmd, long seq) {

//        log.debug("R1 Hold {}", cmd);

        //log.debug("uid={} n={}", cmd.uid, Long.hashCode(cmd.uid) % USER_CHECK_PARALLELISM);

//        if ((cmd.uid & 1) != mod) {
//            return;
//        }


//        if (cmd.availableEventSeq > lastReleaseSeqProcessed) {
//            log.debug("refused R1: availableEventSeq {} > lastReleaseSeqProcessed {}", cmd.availableEventSeq, lastReleaseSeqProcessed);
//            return false;
//        }else{
//            //log.debug("accepted R1: availableEventSeq {} <= lastReleaseSeqProcessed {}", cmd.availableEventSeq, lastReleaseSeqProcessed);
//        }

        switch (cmd.command) {
            case MOVE_ORDER:
            case CANCEL_ORDER:
            case ORDER_BOOK_REQUEST:
                // NO checks for UPDATE or CANCEL
                cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
                break;

            case PLACE_ORDER:
                placeOrder(cmd);
                break;

            case ADD_USER:
                addUser(cmd);
                break;

            case BALANCE_ADJUSTMENT:
                balanceAdjustment(cmd);
                break;
        }

        //return true;
    }

    private void placeOrder(OrderCommand cmd) {
        UserProfile userProfile;
        userProfile = userProfileService.getUserProfile(cmd.uid);
        if (userProfile == null) {
            log.warn("User profile {} not found", cmd.uid);
            return;
        }

        if (!riskEngine.checkIfCanPlaceOrder(cmd, userProfile)) {
            cmd.resultCode = CommandResultCode.RISK_NSF;

            // TODO SEND rejected by risk engine
            log.debug("Can not place {}", cmd);
            log.debug("         user {}", userProfile);
            return;
        }

        // send event accepted
        // TODO SEND pending
        // lock funds
        portfolioService.holdDepositForNewOrder(cmd, userProfile);
        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        userProfile.commandsCounter++; // TODO should also set for MOVE
    }

    private void balanceAdjustment(OrderCommand cmd) {
        UserProfile userProfile;
        userProfile = userProfileService.getUserProfile(cmd.uid);
        if (userProfile == null) {
            log.warn("User profile {} not found", cmd.uid);
            cmd.resultCode = CommandResultCode.AUTH_INVALID_USER;
            return;
        }
        //log.warn("Adjust balance: {}", cmd);
        userProfile.fastBalance += cmd.price;
        cmd.resultCode = CommandResultCode.SUCCESS;
    }

    private void addUser(OrderCommand cmd) {
        if (!userProfileService.addEmptyUserProfile(cmd.uid)) {
            cmd.resultCode = CommandResultCode.USER_MGMT_USER_ALREADY_EXISTS;
            return;
        }
        cmd.resultCode = CommandResultCode.SUCCESS;
    }

    private void handlerRiskRelease(OrderCommand cmd, long seq) {

//        log.debug("R2 Release {}", seq);

//        if (seq > cmd.availableEventSeq) {
//            log.debug("refused R2: sequence {} > avail {}", seq, cmd.availableEventSeq);
//            return false;
//        }else{
//            log.debug("accepted R2: sequence {} <= avail {}", seq, cmd.availableEventSeq);
//        }

        // TODO ?? check if order is not reverse
        cmd.processMatherEvents(this::handleMatcherEvent);

//        lastReleaseSeqProcessed = seq;
//        log.debug("lastReleaseSeqProcessed set to {}", lastReleaseSeqProcessed);
        //return true;
    }

    private void handleMatcherEvent(MatcherTradeEvent ev) {

        switch (ev.eventType) {
            case TRADE:
                // TODO group by user profile ??
                SymbolPortfolio portfolio = userProfileService.getUserProfile(ev.activeOrderUid).portfolio.get(ev.symbol);
                portfolioService.updatePortfolioForTrade(ev.activeOrderAction, ev.size, ev.price, portfolio);
                portfolio = userProfileService.getUserProfile(ev.matchedOrderUid).portfolio.get(ev.symbol);
                portfolioService.updatePortfolioForTrade(ev.activeOrderAction.opposite(), ev.size, ev.price, portfolio);
                return;

            case REJECTION:
            case REDUCE:
                portfolio = userProfileService.getUserProfile(ev.activeOrderUid).portfolio.get(ev.symbol);
                portfolioService.updatePortfolioForReduce(ev.activeOrderAction, ev.size, portfolio);
        }


    }


}
