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
import org.openpredict.exchange.core.biprocessor.SlaveProcessor;
import org.openpredict.exchange.core.journalling.JournallingProcessor;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

import static org.openpredict.exchange.beans.MatcherEventType.*;

@RequiredArgsConstructor
@Slf4j
public final class ExchangeCore {

    private static final boolean THREAD_AFFINITY_ENABLE = true;
    private static final boolean THREAD_AFFINITY_PER_CORE = false;

    private static final int RING_BUFFER_SIZE = 64 * 1024;

    private final UserProfileService userProfileService = new UserProfileService();
    private final MatchingEngineRouter matchingEngineRouter = new MatchingEngineRouter();
    private final SymbolSpecificationProvider symbolSpecificationProvider = new SymbolSpecificationProvider();
    private final RiskEngine riskEngine = new RiskEngine(symbolSpecificationProvider);
    private final BinaryCommandsProcessor binaryCommandsProcessor = new BinaryCommandsProcessor(this::addSymbol, CommandResultCode.VALID_FOR_MATCHING_ENGINE);

    private final Disruptor<OrderCommand> disruptor;
    private final RingBuffer<OrderCommand> cmdRingBuffer;

    private MasterProcessor procR1;

    public ExchangeCore(Consumer<OrderCommand> resultsConsumer) {
        this(resultsConsumer, null);
    }

    public ExchangeCore(Consumer<OrderCommand> resultsConsumer, JournallingProcessor journallingHandler) {

        ThreadFactory threadFactory = eventProcessor -> new Thread(() -> {
            try (AffinityLock lock = THREAD_AFFINITY_PER_CORE ? AffinityLock.acquireCore() : AffinityLock.acquireLock()) {
                log.debug("{} pinned to {}", Thread.currentThread(), lock.cpuId());
                eventProcessor.run();
            }
        });

        this.disruptor = new Disruptor<>(
                OrderCommand::new,
                RING_BUFFER_SIZE,
                THREAD_AFFINITY_ENABLE ? threadFactory : Executors.defaultThreadFactory(),
                ProducerType.MULTI, // multiple gateway threads are writing
                CfgWaitStrategyType.BUSY_SPIN.create());

        this.cmdRingBuffer = disruptor.getRingBuffer();

        final DisruptorExceptionHandler<OrderCommand> exceptionHandler = new DisruptorExceptionHandler<>("main", (ex, seq) -> {
            log.error("Exception thrown on sequence={}", seq, ex);
            // TODO re-throw exception on publishing
            cmdRingBuffer.publishEvent(SHUTDOWN_SIGNAL_TRANSLATOR);
            disruptor.shutdown();
        });

        disruptor.setDefaultExceptionHandler(exceptionHandler);

        final EventHandler<OrderCommand> matchingEngineHandler = (cmd, seq, eob) -> matchingEngineRouter.processOrder(cmd);

//         1. grouping processor (G)
        EventHandlerGroup<OrderCommand> afterGrouping =
                disruptor.handleEventsWith((rb, bs) -> new GroupingProcessor(rb, rb.newBarrier(bs)));

        // 2. journalling (J) in parallel with risk hold (R1) + matching engine (ME)
        if (journallingHandler != null) {
            afterGrouping.handleEventsWith(journallingHandler);
        }

        afterGrouping.handleEventsWith((rb, bs) -> {
            procR1 = new MasterProcessor(rb, rb.newBarrier(bs), this::handleRiskHold, exceptionHandler);
            return procR1;
        }).then(matchingEngineHandler);

        // 3. results handler (E) after matching engine (ME) + [journalling (J)]
        // 4. risk release (R2) after matching engine (ME)
        EventHandlerGroup<OrderCommand> afterMatchingEngine = disruptor.after(matchingEngineHandler);
        afterMatchingEngine.then((rb, bs) -> {
            final SlaveProcessor procR2 = new SlaveProcessor(rb, rb.newBarrier(bs), this::handlerRiskRelease, exceptionHandler);
            procR1.setSlaveProcessor(procR2);
            return procR2;
        });

        (journallingHandler != null ? disruptor.after(matchingEngineHandler, journallingHandler) : afterMatchingEngine)
                .then((cmd, seq, eob) -> resultsConsumer.accept(cmd));

    }

    public void startup() {
        log.debug("Starting disruptor...");
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
        log.info("Shutdown disruptor...");
        cmdRingBuffer.publishEvent(SHUTDOWN_SIGNAL_TRANSLATOR);
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

            case BINARY_DATA:
                binaryCommandsProcessor.binaryData(cmd);
                break;

            case RESET:
                resetState();
                cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
                break;

            case NOP:
                cmd.resultCode = CommandResultCode.SUCCESS;
                break;
        }
    }

    private void placeOrderRiskCheck(OrderCommand cmd) {

        final UserProfile userProfile = userProfileService.getUserProfile(cmd.uid);
        if (userProfile == null) {
            cmd.resultCode = CommandResultCode.AUTH_INVALID_USER;
            log.warn("User profile {} not found", cmd.uid);
            return;
        }

        final CoreSymbolSpecification spec = symbolSpecificationProvider.getSymbolSpecification(cmd.symbol);
        if (spec == null) {
            cmd.resultCode = CommandResultCode.INVALID_SYMBOL;
            log.warn("Symbol {} not found", cmd.symbol);
            return;
        }

        // check if account has enough funds
        if (!riskEngine.placeOrder(cmd, userProfile, spec)) {
            cmd.resultCode = CommandResultCode.RISK_NSF;
            log.warn("NSF uid={}: Can not place {}", userProfile.uid, cmd);
            return;
        }

        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        userProfile.commandsCounter++; // TODO should also set for MOVE
    }

    private void balanceAdjustment(OrderCommand cmd) {
        final UserProfile userProfile = userProfileService.getUserProfile(cmd.uid);
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

        // double settlement protection
        if (userProfile.externalTransactions.contains(cmd.orderId)) {
            cmd.resultCode = CommandResultCode.USER_MGMT_ACCOUNT_BALANCE_ADJUSTMENT_ALREADY_APPLIED;
            return;
        }

        final int currency = cmd.symbol;
        if (currency == 0) {
            if (userProfile.futuresBalance + amount < 0) {
                cmd.resultCode = CommandResultCode.USER_MGMT_ACCOUNT_BALANCE_ADJUSTMENT_NSF;
                return;
            }

            userProfile.externalTransactions.add(cmd.orderId);
            userProfile.futuresBalance += amount;
            cmd.size = userProfile.futuresBalance;
            cmd.resultCode = CommandResultCode.SUCCESS;

        } else {
            if (amount < 0 && userProfile.accounts.get(currency) + amount < 0) {
                cmd.resultCode = CommandResultCode.USER_MGMT_ACCOUNT_BALANCE_ADJUSTMENT_NSF;
                return;
            }

            userProfile.externalTransactions.add(cmd.orderId);
            cmd.size = userProfile.accounts.addToValue(currency, amount);
            cmd.resultCode = CommandResultCode.SUCCESS;
        }
    }

    private void addUser(OrderCommand cmd) {
        if (!userProfileService.addEmptyUserProfile(cmd.uid)) {
            cmd.resultCode = CommandResultCode.USER_MGMT_USER_ALREADY_EXISTS;
            return;
        }
        cmd.resultCode = CommandResultCode.SUCCESS;
    }

    private CommandResultCode addSymbol(final CoreSymbolSpecification symbolSpecification) {
        if (symbolSpecificationProvider.getSymbolSpecification(symbolSpecification.symbolId) != null) {
            return CommandResultCode.SYMBOL_MGMT_SYMBOL_ALREADY_EXISTS;
        } else {
            symbolSpecificationProvider.registerSymbol(symbolSpecification.symbolId, symbolSpecification);
            return CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        }
    }

    private void handlerRiskRelease(final OrderCommand cmd) {


        final L2MarketData marketData = cmd.marketData;
        MatcherTradeEvent mte = cmd.matcherEvent;

        if (marketData == null && mte == null) {
            return;
        }

        final CoreSymbolSpecification spec = symbolSpecificationProvider.getSymbolSpecification(cmd.symbol);
        if (spec == null) {
            log.error("Symbol not found: {}", cmd.symbol);
            return;
        }

        if (mte != null) {
            // TODO ?? check if processing order is not reversed
            do {
                if (spec.type == SymbolType.CURRENCY_EXCHANGE_PAIR) {
                    handleMatcherEventExchange(mte, spec);
                } else {
                    handleMatcherEventMargin(mte, spec);
                }
                mte = mte.nextEvent;
            } while (mte != null);
        }

        // Process marked data
        if (marketData != null) {
            riskEngine.updateLastPrice(marketData, cmd.symbol);
        }
    }

    // TODO process uid only for designated shard
    private void handleMatcherEventMargin(final MatcherTradeEvent ev, final CoreSymbolSpecification spec) {

        final long size = ev.size;

        if (ev.eventType == TRADE) {
            // TODO group by user profile ??
            // update taker's portfolio
            final UserProfile taker = userProfileService.getUserProfileOrThrowEx(ev.activeOrderUid);
            final SymbolPortfolioRecord takerSpr = taker.getOrCreatePortfolioRecord(ev.symbol);
            takerSpr.updatePortfolioForMarginTrade(ev.activeOrderAction, size, ev.price, spec.takerCommission);
            taker.removeRecordIfEmpty(takerSpr);

            // update maker's portfolio
            final UserProfile maker = userProfileService.getUserProfileOrThrowEx(ev.matchedOrderUid);
            final SymbolPortfolioRecord makerSpr = maker.getOrCreatePortfolioRecord(ev.symbol);
            makerSpr.updatePortfolioForMarginTrade(ev.activeOrderAction.opposite(), size, ev.price, spec.makerCommission);
            maker.removeRecordIfEmpty(makerSpr);

        } else if (ev.eventType == REJECTION || ev.eventType == REDUCE) {
            // for reduce/rejection only one party is involved
            final UserProfile up = userProfileService.getUserProfileOrThrowEx(ev.activeOrderUid);
            final SymbolPortfolioRecord spr = up.getOrCreatePortfolioRecord(ev.symbol);
            spr.pendingRelease(ev.activeOrderAction, size);
            up.removeRecordIfEmpty(spr);

        } else {
            log.error("unsupported eventType: {}", ev.eventType);
        }
    }


    private void handleMatcherEventExchange(final MatcherTradeEvent ev, final CoreSymbolSpecification spec) {

        final long size = ev.size;

        if (ev.eventType == TRADE) {
            // TODO group by user profile ??

            // release taker's portfolio
            final UserProfile taker = userProfileService.getUserProfileOrThrowEx(ev.activeOrderUid);
            final SymbolPortfolioRecord takerSpr = taker.getOrCreatePortfolioRecord(ev.symbol);
            takerSpr.pendingRelease(ev.activeOrderAction, size);

            // release maker's portfolio
            final UserProfile maker = userProfileService.getUserProfileOrThrowEx(ev.matchedOrderUid);
            final SymbolPortfolioRecord makerSpr = maker.getOrCreatePortfolioRecord(ev.symbol);
            makerSpr.pendingRelease(ev.activeOrderAction.opposite(), size);

            // perform account-to-account transfers
            // TODO multiplier ???
            final long amountInCounterCurrency = ev.price * size * spec.quoteScaleK;
            final long amountInBaseCurrency = size * spec.baseScaleK;
            // TODO add commission

            final long takerFee = spec.takerCommission * size;
            final long makerFee = spec.makerCommission * size;

            if (ev.activeOrderAction == OrderAction.ASK) {
                // taker is selling, maker is buying
                taker.accounts.addToValue(spec.quoteCurrency, amountInCounterCurrency);
                taker.accounts.addToValue(spec.baseCurrency, -amountInBaseCurrency - takerFee);
                maker.accounts.addToValue(spec.baseCurrency, amountInBaseCurrency - makerFee);
                maker.accounts.addToValue(spec.quoteCurrency, -amountInCounterCurrency);
            } else {
                // taker is buying, maker is selling
                taker.accounts.addToValue(spec.quoteCurrency, -amountInCounterCurrency);
                taker.accounts.addToValue(spec.baseCurrency, amountInBaseCurrency - takerFee);
                maker.accounts.addToValue(spec.baseCurrency, -amountInBaseCurrency - makerFee);
                maker.accounts.addToValue(spec.quoteCurrency, amountInCounterCurrency);
            }

            taker.removeRecordIfEmpty(takerSpr);
            maker.removeRecordIfEmpty(makerSpr);

        } else if (ev.eventType == REJECTION || ev.eventType == REDUCE) {
            // for reduce/rejection only one party is involved
            final UserProfile up = userProfileService.getUserProfileOrThrowEx(ev.activeOrderUid);
            final SymbolPortfolioRecord spr = up.getOrCreatePortfolioRecord(ev.symbol);
            spr.pendingRelease(ev.activeOrderAction, size);
            up.removeRecordIfEmpty(spr);

        } else {
            log.error("unsupported eventType: {}", ev.eventType);
        }
    }


    private void resetState() {
        userProfileService.reset();
        symbolSpecificationProvider.reset();
    }
}
