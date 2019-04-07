package org.openpredict.exchange.core;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;
import org.openpredict.exchange.beans.*;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.beans.cmd.OrderCommandType;
import org.openpredict.exchange.core.biprocessor.GroupingProcessor;
import org.openpredict.exchange.core.biprocessor.MasterProcessor;
import org.openpredict.exchange.core.biprocessor.SimpleEventHandler;
import org.openpredict.exchange.core.biprocessor.SlaveProcessor;
import org.openpredict.exchange.core.journalling.JournallingProcessor;

import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

import static org.openpredict.exchange.beans.MatcherEventType.*;

@RequiredArgsConstructor
@Slf4j
public final class ExchangeCore {

    private static final boolean THREAD_AFFINITY_PER_CORE = false;
    private static final boolean JOURNALLING_ENABLED = false;


    private final UserProfileService userProfileService = new UserProfileService();
    private final MatchingEngineRouter matchingEngineRouter = new MatchingEngineRouter();
    private final SymbolSpecificationProvider symbolSpecificationProvider = new SymbolSpecificationProvider();
    private final RiskEngine riskEngine = new RiskEngine(symbolSpecificationProvider);
    private final JournallingProcessor journallingHandler = new JournallingProcessor();

    private final Disruptor<OrderCommand> disruptor;
    private final RingBuffer<OrderCommand> cmdRingBuffer;

    private MasterProcessor procR1;

    public ExchangeCore(Consumer<OrderCommand> resultsConsumer) {

        ThreadFactory threadFactory = eventProcessor -> new Thread(() -> {
            try (AffinityLock lock = THREAD_AFFINITY_PER_CORE ? AffinityLock.acquireCore() : AffinityLock.acquireLock()) {
                log.debug("{} pinned to {}", Thread.currentThread(), lock.cpuId());
                eventProcessor.run();
            }
        });

        this.disruptor = new Disruptor<>(
                OrderCommand::new,
                64 * 1024,
                threadFactory,
                ProducerType.MULTI, // multiple gateway threads are writing
                CfgWaitStrategyType.BUSY_SPIN.create());

        this.cmdRingBuffer = disruptor.getRingBuffer();

        DisruptorExceptionHandler<OrderCommand> exceptionHandler = new DisruptorExceptionHandler<>("main", (ex, seq) -> {
            cmdRingBuffer.publishEvent(SHUTDOWN_SIGNAL_TRANSLATOR);
            disruptor.shutdown();
        });

        disruptor.setDefaultExceptionHandler(exceptionHandler);

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
        EventHandlerGroup<OrderCommand> afterGrouping =
                disruptor.handleEventsWith((rb, bs) -> new GroupingProcessor(rb, rb.newBarrier(bs)));

        // 2. journalling (J) in parallel with risk hold (R1) + matching engine (ME)
        if (JOURNALLING_ENABLED) {
            afterGrouping.handleEventsWith(journallingHandler);
        }

        afterGrouping.handleEventsWith((rb, bs) -> {
            procR1 = new MasterProcessor(rb, rb.newBarrier(bs), handlerR1, exceptionHandler);
            return procR1;
        });
        disruptor.after(procR1).handleEventsWith(matchingEngineHandler);


        // 3. results handler (E) after matching engine (ME) + [journalling (J)]
        // 4. risk release (R2) after matching engine (ME)
        EventHandlerGroup<OrderCommand> afterMatchingEngine = disruptor.after(matchingEngineHandler);
        afterMatchingEngine.then((rb, bs) -> {
            final SlaveProcessor procR2 = new SlaveProcessor(rb, rb.newBarrier(bs), handlerR2, exceptionHandler);
            procR1.setSlaveProcessor(procR2);
            return procR2;
        });

        (JOURNALLING_ENABLED ? disruptor.after(matchingEngineHandler, journallingHandler) : afterMatchingEngine)
                .then(resultsHandler);

    }

    public void startup() {
        log.debug("STARTING Disruptor");

        // starting disruptor
        disruptor.start();
    }

    public ExchangeApi getApi() {
        return new ExchangeApi(this);
    }

    public RingBuffer<OrderCommand> getRingBuffer() {
        return cmdRingBuffer;
    }

    private static final EventTranslator<OrderCommand> SHUTDOWN_SIGNAL_TRANSLATOR = (cmd, seq) -> {
        cmd.command = OrderCommandType.SHUTDOWN_SIGNAL;
        cmd.resultCode = CommandResultCode.NEW;
    };

    public void shutdown() {

        // TODO stop accepting new events first
        cmdRingBuffer.publishEvent(SHUTDOWN_SIGNAL_TRANSLATOR);

        log.info("Shutdown disruptor...");
        disruptor.shutdown();
        log.info("Disruptor stopped");
    }

    private void handleRiskHold(OrderCommand cmd) {

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
                resetState();
                cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
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
            }
        }
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

    private void resetState() {
        userProfileService.reset();
        symbolSpecificationProvider.reset();
    }
}
