package org.openpredict.exchange.core;

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.openpredict.exchange.beans.CfgWaitStrategyType;
import org.openpredict.exchange.beans.CoreSymbolSpecification;
import org.openpredict.exchange.beans.MatcherTradeEvent;
import org.openpredict.exchange.beans.UserProfile;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.beans.cmd.OrderCommandType;
import org.openpredict.exchange.biprocessor.GroupingProcessor;
import org.openpredict.exchange.biprocessor.MasterProcessor;
import org.openpredict.exchange.biprocessor.SimpleEventHandler;
import org.openpredict.exchange.biprocessor.SlaveProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static org.openpredict.exchange.beans.MatcherEventType.*;

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
    private SymbolSpecificationProvider symbolSpecificationProvider;

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

    private MasterProcessor procR1;
    private SlaveProcessor<OrderCommand> procR2;

    private GroupingProcessor procG;

    private BatchEventProcessor<OrderCommand> procTest;

    @PostConstruct
    public void start() {
        disruptor = new Disruptor<>(
                OrderCommand::new,
                64 * 1024,
                Executors.defaultThreadFactory(),
                ProducerType.MULTI, // multiple gateway threads are writing
                CfgWaitStrategyType.BUSY_SPIN.create());

        DisruptorExceptionHandler<OrderCommand> exceptionHandler = new DisruptorExceptionHandler<>("main", (ex, seq) -> {
            cmdRingBuffer.publishEvent(SHUTDOWN_SIGNAL_TRANSLATOR);
            disruptor.shutdown();
        });

        disruptor.setDefaultExceptionHandler(exceptionHandler);

//        LoggingBarrierHandler loggingBarrierHandler = new LoggingBarrierHandler();

        EventHandler<OrderCommand> matchingEngineHandler = (cmd, seq, eob) -> {
//            log.debug("ME processOrder {}", seq);
            matchingEngineRouter.processOrder(cmd);
        };

        EventHandler<OrderCommand> resultsHandler = (cmd, seq, eob) -> {
            //log.debug("RES HANDLER: seq:{} cmd:{}", seq, cmd);
            resultsConsumer.accept(cmd);
        };

        SimpleEventHandler<OrderCommand> handlerR1 = this::handleRiskHold;
        SimpleEventHandler<OrderCommand> handlerR2 = this::handlerRiskRelease;

//         1. grouping processor (G)
        disruptor.handleEventsWith((rb, bs) -> {
            procG = new GroupingProcessor(rb, rb.newBarrier(bs));
            return procG;
        });

//        disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
//            if((sequence & 32767) == 0){
//                log.debug("sequence={}", sequence);
//            }
//        });


//        disruptor.handleEventsWith((rb, bs) -> {
//            procTest = new BatchEventProcessor<>(rb, rb.newBarrier(bs),
//                    (event, sequence, endOfBatch) -> {
//                        if ((sequence & 32767) == 0) {
//                            log.debug("2. sequence={}", sequence);
//                        }
//                    });
//
//            return procTest;
//        });
//

        // 2. journalling (J) in parallel with risk hold (R1) + matching engine (ME)
        disruptor
                .after(procG)
                .handleEventsWith(journallingHandler);

        disruptor
                .after(procG)
                .handleEventsWith((rb, bs) -> {
                    procR1 = new MasterProcessor(rb, rb.newBarrier(bs), handlerR1, exceptionHandler);
                    return procR1;
                });
        disruptor.after(procR1).handleEventsWith(matchingEngineHandler);


        // 3. results handler (E) and risk release (R2) after matching engine (ME) + journalling (J)
        disruptor.after(matchingEngineHandler, journallingHandler)
                .then(resultsHandler);

        disruptor.after(matchingEngineHandler)
                .then((rb, bs) -> {
                    procR2 = new SlaveProcessor<>(rb, rb.newBarrier(bs), handlerR2, exceptionHandler);
                    procR1.setSlaveProcessor(procR2);
                    return procR2;
                });


        // TODO add second journalling handler?

        log.debug("STARTING Disruptor");

        // starting disruptor
        disruptor.start();

        cmdRingBuffer = disruptor.getRingBuffer();


//        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(() -> {
////            long lg = disruptor.getBarrierFor(loggingBarrierHandler).getCursor();
////            long me = disruptor.getBarrierFor(matchingEngineHandler).getCursor();
//            long g = procG.getSequence().get();
//            long r1 = procR1.getSequence().get();
//            long r2 = procR2.getSequence().get();
//
////            log.debug("lg:{} r1:{} me:{} r2:{}", lg, r1.getSequence().get(), me, r2.getSequence().get());
////            log.debug("lg:{} r1:{} me:{} ", lg, r1.getSequence().get(), me);
//            log.debug("g:{} r1:{} me:{} r2:{} D={}", g, r1, -4, r2, r1-r2);
//        }, 1001, 1000, TimeUnit.MILLISECONDS);

    }

    public RingBuffer<OrderCommand> getRingBuffer() {
        return cmdRingBuffer;
    }

    private static final EventTranslator<OrderCommand> SHUTDOWN_SIGNAL_TRANSLATOR = (cmd, seq) -> {
        cmd.command = OrderCommandType.SHUTDOWN_SIGNAL;
        cmd.resultCode = CommandResultCode.NEW;
    };

    @PreDestroy
    public void stop() {

        // TODO stop accepting new events first
        cmdRingBuffer.publishEvent(SHUTDOWN_SIGNAL_TRANSLATOR);

        log.info("Shutdown disruptor...");
        disruptor.shutdown();
        log.info("Disruptor stopped");
    }

    private void handleRiskHold(OrderCommand cmd) {

//        log.debug("R1 Hold {}", cmd);

        //log.debug("uid={} n={}", cmd.uid, Long.hashCode(cmd.uid) % USER_CHECK_PARALLELISM);

//        if ((cmd.uid & 1) != mod) {
//            return;
//        }

        switch (cmd.command) {
            case MOVE_ORDER:
            case CANCEL_ORDER:
            case ORDER_BOOK_REQUEST:
                // NO checks for UPDATE or CANCEL
                cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
                break;

            case PLACE_ORDER:
                placeOrderRiskCheck(cmd);
                break;

            case ADD_USER:
                addUser(cmd);
                break;

            case BALANCE_ADJUSTMENT:
                balanceAdjustment(cmd);
                break;

            case ADD_SYMBOL:
                addSymbol(cmd);
                break;

            case RESET:
                reset();
                cmd.resultCode = CommandResultCode.SUCCESS;
                break;

            case NOP:
                cmd.resultCode = CommandResultCode.SUCCESS;
                break;
        }

        //return true;
    }

    private void placeOrderRiskCheck(OrderCommand cmd) {

//        boolean debug = cmd.uid == 124;
//        if (debug) log.debug(">>> {}", cmd);


        final UserProfile userProfile = userProfileService.getUserProfile(cmd.uid);
        if (userProfile == null) {
            cmd.resultCode = CommandResultCode.AUTH_INVALID_USER;
            log.warn("User profile {} not found", cmd.uid);
            return;
        }

        // check if enough funds
        if (!riskEngine.checkIfCanPlaceOrder(cmd, userProfile)) {
            cmd.resultCode = CommandResultCode.RISK_NSF;
            log.warn("NSF uid={}: Can not place {}", userProfile.uid, cmd);
            return;
        }

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

        final long amount = cmd.price;
        if (amount == 0) {
            cmd.resultCode = CommandResultCode.USER_MGMT_ACCOUNT_BALANCE_ADJUSTMENT_ZERO;
            return;
        }

        if (userProfile.externalTransactions.containsKey(cmd.orderId)) {
            cmd.resultCode = CommandResultCode.USER_MGMT_ACCOUNT_BALANCE_ADJUSTMENT_ALREADY_APPLIED;
            return;
        }

        //log.info("Adjust balance: {}", cmd);

        userProfile.externalTransactions.put(cmd.orderId, amount);
        userProfile.balance += amount;
        cmd.size = userProfile.balance;
        cmd.resultCode = CommandResultCode.SUCCESS;
    }

    private void addUser(OrderCommand cmd) {
        if (!userProfileService.addEmptyUserProfile(cmd.uid)) {
            cmd.resultCode = CommandResultCode.USER_MGMT_USER_ALREADY_EXISTS;
            return;
        }
        cmd.resultCode = CommandResultCode.SUCCESS;
    }

    private void addSymbol(OrderCommand cmd) {

        if (symbolSpecificationProvider.getSymbolSpecification(cmd.symbol) != null) {
            cmd.resultCode = CommandResultCode.SYMBOL_MGMT_SYMBOL_ALREADY_EXISTS;
            return;
        }

        // use order-specific fields for symbol spec
        CoreSymbolSpecification spec = CoreSymbolSpecification.builder().symbolId(cmd.symbol)
                .depositBuy(cmd.price)
                .depositSell(cmd.uid)
                .lowLimit(cmd.orderId)
                .highLimit(cmd.size)
                .build();

        symbolSpecificationProvider.registerSymbol(cmd.symbol, spec);
        matchingEngineRouter.addOrderBook(cmd.symbol);

        cmd.resultCode = CommandResultCode.SUCCESS;
    }


    private void handlerRiskRelease(OrderCommand cmd) {

//        log.debug("R2 Release {}", seq);

//        if (seq > cmd.availableEventSeq) {
//            log.debug("refused R2: sequence {} > avail {}", seq, cmd.availableEventSeq);
//            return false;
//        }else{
//            log.debug("accepted R2: sequence {} <= avail {}", seq, cmd.availableEventSeq);
//        }

        final CoreSymbolSpecification spec = (cmd.matcherEvent == null) ? null : symbolSpecificationProvider.getSymbolSpecification(cmd.symbol);

        // TODO ?? check if processing order is not reversed
        cmd.processMatherEvents(ev -> handleMatcherEvent(ev, spec));

//        lastReleaseSeqProcessed = seq;
//        log.debug("lastReleaseSeqProcessed set to {}", lastReleaseSeqProcessed);
        //return true;
    }

    // TODO process uid only for designated shard
    private void handleMatcherEvent(MatcherTradeEvent ev, CoreSymbolSpecification spec) {

        if (ev.eventType == TRADE) {
            // TODO group by user profile ??

            // update taker's portfolio
            userProfileService.processSymbolPortfolioRecord(ev.activeOrderUid, ev.symbol, (userProfile, symbolPortfolioRecord) -> {
                symbolPortfolioRecord.updatePortfolioForTrade(ev.activeOrderAction, ev.size, ev.price, spec.takerCommission);
                userProfile.removeRecordIfEmpty(symbolPortfolioRecord);
            });

            // update maker's portfolio
            userProfileService.processSymbolPortfolioRecord(ev.matchedOrderUid, ev.symbol, (userProfile, symbolPortfolioRecord) -> {
                symbolPortfolioRecord.updatePortfolioForTrade(ev.activeOrderAction.opposite(), ev.size, ev.price, spec.makerCommission);
                userProfile.removeRecordIfEmpty(symbolPortfolioRecord);
            });

        } else if (ev.eventType == REJECTION || ev.eventType == REDUCE) {

            // for reduce/rejection only one party is involved
            userProfileService.processSymbolPortfolioRecord(ev.activeOrderUid, ev.symbol, (up, spr) -> {
                portfolioService.updatePortfolioForReduce(ev.activeOrderAction, ev.size, spr);
                up.removeRecordIfEmpty(spr);
            });

        } else {
            log.error("unsupported eventType: {}", ev.eventType);
        }

    }

    private void reset() {
        log.info("doing RESET");
        userProfileService.reset();
        symbolSpecificationProvider.reset();
    }

}
