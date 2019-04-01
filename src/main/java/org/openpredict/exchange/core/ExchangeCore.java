package org.openpredict.exchange.core;

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;
import org.openpredict.exchange.beans.CfgWaitStrategyType;
import org.openpredict.exchange.beans.MatcherTradeEvent;
import org.openpredict.exchange.beans.UserProfile;
import org.openpredict.exchange.beans.*;
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
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

import static org.openpredict.exchange.beans.MatcherEventType.*;

@Service
@Slf4j
public final class ExchangeCore {

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

    private static final boolean THREAD_AFFINITY_PER_CORE = false;

    private static final boolean JOURNALLING_ENABLED = false;


    @PostConstruct
    public void start() {

        ThreadFactory threadFactory = eventProcessor -> new Thread(() -> {
            try (AffinityLock lock = THREAD_AFFINITY_PER_CORE ? AffinityLock.acquireCore() : AffinityLock.acquireLock()) {
                log.debug("{} pinned to {}", Thread.currentThread(), lock.cpuId());
                eventProcessor.run();
            }
        });

        disruptor = new Disruptor<>(
                OrderCommand::new,
                64 * 1024,
                threadFactory,
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
        EventHandlerGroup<OrderCommand> afterGrouping = disruptor.handleEventsWith((rb, bs) -> {
            procG = new GroupingProcessor(rb, rb.newBarrier(bs));
            return procG;
        });

        // 2. journalling (J) in parallel with risk hold (R1) + matching engine (ME)
        if (JOURNALLING_ENABLED) {
            afterGrouping.handleEventsWith(journallingHandler);
        }

        afterGrouping
                .handleEventsWith((rb, bs) -> {
                    procR1 = new MasterProcessor(rb, rb.newBarrier(bs), handlerR1, exceptionHandler);
                    return procR1;
                });
        disruptor.after(procR1).handleEventsWith(matchingEngineHandler);


        // 3. results handler (E) after matching engine (ME) + [journalling (J)]
        // 4. risk release (R2) after matching engine (ME)
        EventHandlerGroup<OrderCommand> afterMatchingEngine = disruptor.after(matchingEngineHandler);
        afterMatchingEngine
                .then((rb, bs) -> {
                    procR2 = new SlaveProcessor<>(rb, rb.newBarrier(bs), handlerR2, exceptionHandler);
                    procR1.setSlaveProcessor(procR2);
                    return procR2;
                });

        (JOURNALLING_ENABLED ? disruptor.after(matchingEngineHandler, journallingHandler) : afterMatchingEngine)
                .then(resultsHandler);

        // 5. cleaning handler
//        final EventHandler<OrderCommand> cleaningHandler = (cmd, seq, eob) -> {
//            //            log.debug("cleaning {}", seq);
//            cmd.marketData = null;
//            cmd.matcherEvent = null;
//        };
//        disruptor.after(procR2, resultsHandler).handleEventsWith(cleaningHandler);

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

        // check if account has enough funds
        if (!riskEngine.placeOrder(cmd, userProfile)) {
            cmd.resultCode = CommandResultCode.RISK_NSF;
            log.warn("NSF uid={}: Can not place {}", userProfile.uid, cmd);
            return;
        }

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
                .lastAskPrice(Long.MAX_VALUE)
                .lastBidPrice(0)
                .build();

        symbolSpecificationProvider.registerSymbol(cmd.symbol, spec);
        matchingEngineRouter.addOrderBook(cmd.symbol);

        cmd.resultCode = CommandResultCode.SUCCESS;
    }


    private void handlerRiskRelease(final OrderCommand cmd) {

//        log.debug("R2 Release {}", seq);

//        if (seq > cmd.availableEventSeq) {
//            log.debug("refused R2: sequence {} > avail {}", seq, cmd.availableEventSeq);
//            return false;
//        }else{
//            log.debug("accepted R2: sequence {} <= avail {}", seq, cmd.availableEventSeq);
//        }

        CoreSymbolSpecification spec = null;

        if (cmd.matcherEvent != null) {
            spec = symbolSpecificationProvider.getSymbolSpecification(cmd.symbol);
            if (spec == null) {
                return;
            }

            // TODO ?? check if processing order is not reversed
            MatcherTradeEvent mte = cmd.matcherEvent;
            do {
                handleMatcherEvent(mte, spec);
                mte = mte.nextEvent;
            } while (mte != null);
        }

        L2MarketData marketData = cmd.marketData;
        if (marketData != null) {
            if (spec == null) {
                spec = symbolSpecificationProvider.getSymbolSpecification(cmd.symbol);
            }
            if (spec != null) {
                if (marketData.askSize > 0) {
                    spec.lastAskPrice = marketData.askPrices[0];
                }
                if (marketData.bidSize > 0) {
                    spec.lastBidPrice = marketData.bidPrices[0];
                }

                //log.debug("MARKED DATA: {}+{} ask={} bid={}", marketData.askSize, marketData.bidSize, spec.lastAskPrice, spec.lastBidPrice);
            }
        }

//        lastReleaseSeqProcessed = seq;
//        log.debug("lastReleaseSeqProcessed set to {}", lastReleaseSeqProcessed);
        //return true;
    }

    // TODO process uid only for designated shard
    private void handleMatcherEvent(MatcherTradeEvent ev, CoreSymbolSpecification spec) {

        if (ev.eventType == TRADE) {
            // TODO group by user profile ??

            // update taker's portfolio
            final UserProfile taker = userProfileService.getUserProfile(ev.activeOrderUid);
            if (taker != null) {
                final SymbolPortfolioRecord spr = taker.getOrCreatePortfolioRecord(ev.symbol);
                spr.updatePortfolioForTrade(ev.activeOrderAction, ev.size, ev.price, spec.takerCommission);
                taker.removeRecordIfEmpty(spr);
            } else {
                log.warn("User TAKER profile {} not found", ev.activeOrderUid);
            }

            // update maker's portfolio
            final UserProfile maker = userProfileService.getUserProfile(ev.matchedOrderUid);
            if (maker != null) {
                final SymbolPortfolioRecord spr = maker.getOrCreatePortfolioRecord(ev.symbol);
                spr.updatePortfolioForTrade(ev.activeOrderAction.opposite(), ev.size, ev.price, spec.makerCommission);
                maker.removeRecordIfEmpty(spr);
            } else {
                log.warn("User MAKER profile {} not found", ev.activeOrderUid);
            }

        } else if (ev.eventType == REJECTION || ev.eventType == REDUCE) {

            // for reduce/rejection only one party is involved
            final UserProfile up = userProfileService.getUserProfile(ev.activeOrderUid);
            if (up != null) {
                final SymbolPortfolioRecord spr = up.getOrCreatePortfolioRecord(ev.symbol);
                spr.pendingRelease(ev.activeOrderAction, ev.size);
                up.removeRecordIfEmpty(spr);
            } else {
                log.warn("{} User profile {} not found", ev.eventType, ev.activeOrderUid);
            }

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
