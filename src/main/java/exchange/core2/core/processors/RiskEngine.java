/*
 * Copyright 2019 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package exchange.core2.core.processors;

import exchange.core2.collections.objpool.ObjectsPool;
import exchange.core2.core.common.*;
import exchange.core2.core.common.api.binary.BatchAddAccountsCommand;
import exchange.core2.core.common.api.binary.BatchAddSymbolsCommand;
import exchange.core2.core.common.api.binary.BinaryDataCommand;
import exchange.core2.core.common.api.reports.ReportQuery;
import exchange.core2.core.common.api.reports.ReportResult;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.common.config.ExchangeConfiguration;
import exchange.core2.core.common.config.LoggingConfiguration;
import exchange.core2.core.common.config.OrdersProcessingConfiguration;
import exchange.core2.core.processors.journaling.ISerializationProcessor;
import exchange.core2.core.utils.CoreArithmeticUtils;
import exchange.core2.core.utils.SerializationUtils;
import exchange.core2.core.utils.UnsafeUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;

/**
 * Stateful risk engine
 */
@Slf4j
@Getter
public final class RiskEngine implements WriteBytesMarshallable {

    // state
    private final SymbolSpecificationProvider symbolSpecificationProvider;
    private final UserProfileService userProfileService;
    private final BinaryCommandsProcessor binaryCommandsProcessor;
    private final IntObjectHashMap<LastPriceCacheRecord> lastPriceCache;
    private final IntLongHashMap fees;
    private final IntLongHashMap adjustments;
    private final IntLongHashMap suspends;
    private final ObjectsPool objectsPool;

    // sharding by symbolId
    private final int shardId;
    private final long shardMask;

    private final boolean cfgIgnoreRiskProcessing;
    private final boolean cfgMarginTradingEnabled;

    private final ISerializationProcessor serializationProcessor;

    private final boolean logDebug;

    public RiskEngine(final int shardId,
                      final long numShards,
                      final ISerializationProcessor serializationProcessor,
                      final SharedPool sharedPool,
                      final ExchangeConfiguration exchangeConfiguration) {
        if (Long.bitCount(numShards) != 1) {
            throw new IllegalArgumentException("Invalid number of shards " + numShards + " - must be power of 2");
        }
        this.shardId = shardId;
        this.shardMask = numShards - 1;
        this.serializationProcessor = serializationProcessor;

        // initialize object pools // TODO move to perf config
        final HashMap<Integer, Integer> objectsPoolConfig = new HashMap<>();
        objectsPoolConfig.put(ObjectsPool.SYMBOL_POSITION_RECORD, 1024 * 256);
        this.objectsPool = new ObjectsPool(objectsPoolConfig);

        this.logDebug = exchangeConfiguration.getLoggingCfg().getLoggingLevels().contains(LoggingConfiguration.LoggingLevel.LOGGING_RISK_DEBUG);

        if (exchangeConfiguration.getInitStateCfg().fromSnapshot()) {

            // TODO refactor, change to creator (simpler init)`
            final State state = serializationProcessor.loadData(
                    exchangeConfiguration.getInitStateCfg().getSnapshotId(),
                    ISerializationProcessor.SerializedModuleType.RISK_ENGINE,
                    shardId,
                    bytesIn -> {
                        if (shardId != bytesIn.readInt()) {
                            throw new IllegalStateException("wrong shardId");
                        }
                        if (shardMask != bytesIn.readLong()) {
                            throw new IllegalStateException("wrong shardMask");
                        }
                        final SymbolSpecificationProvider symbolSpecificationProvider = new SymbolSpecificationProvider(bytesIn);
                        final UserProfileService userProfileService = new UserProfileService(bytesIn);
                        final BinaryCommandsProcessor binaryCommandsProcessor = new BinaryCommandsProcessor(
                                this::handleBinaryMessage,
                                this::handleReportQuery,
                                sharedPool,
                                exchangeConfiguration.getReportsQueriesCfg(),
                                bytesIn,
                                shardId);
                        final IntObjectHashMap<LastPriceCacheRecord> lastPriceCache = SerializationUtils.readIntHashMap(bytesIn, LastPriceCacheRecord::new);
                        final IntLongHashMap fees = SerializationUtils.readIntLongHashMap(bytesIn);
                        final IntLongHashMap adjustments = SerializationUtils.readIntLongHashMap(bytesIn);
                        final IntLongHashMap suspends = SerializationUtils.readIntLongHashMap(bytesIn);

                        return new State(
                                symbolSpecificationProvider,
                                userProfileService,
                                binaryCommandsProcessor,
                                lastPriceCache,
                                fees,
                                adjustments,
                                suspends);
                    });

            this.symbolSpecificationProvider = state.symbolSpecificationProvider;
            this.userProfileService = state.userProfileService;
            this.binaryCommandsProcessor = state.binaryCommandsProcessor;
            this.lastPriceCache = state.lastPriceCache;
            this.fees = state.fees;
            this.adjustments = state.adjustments;
            this.suspends = state.suspends;

        } else {
            this.symbolSpecificationProvider = new SymbolSpecificationProvider();
            this.userProfileService = new UserProfileService();
            this.binaryCommandsProcessor = new BinaryCommandsProcessor(
                    this::handleBinaryMessage,
                    this::handleReportQuery,
                    sharedPool,
                    exchangeConfiguration.getReportsQueriesCfg(),
                    shardId);
            this.lastPriceCache = new IntObjectHashMap<>();
            this.fees = new IntLongHashMap();
            this.adjustments = new IntLongHashMap();
            this.suspends = new IntLongHashMap();
        }

        final OrdersProcessingConfiguration ordersProcCfg = exchangeConfiguration.getOrdersProcessingCfg();
        this.cfgIgnoreRiskProcessing = ordersProcCfg.getRiskProcessingMode() == OrdersProcessingConfiguration.RiskProcessingMode.NO_RISK_PROCESSING;
        this.cfgMarginTradingEnabled = ordersProcCfg.getMarginTradingMode() == OrdersProcessingConfiguration.MarginTradingMode.MARGIN_TRADING_ENABLED;
    }

    @ToString
    public static class LastPriceCacheRecord implements BytesMarshallable, StateHash {
        public long askPrice = Long.MAX_VALUE;
        public long bidPrice = 0L;

        public LastPriceCacheRecord() {
        }

        public LastPriceCacheRecord(long askPrice, long bidPrice) {
            this.askPrice = askPrice;
            this.bidPrice = bidPrice;
        }

        public LastPriceCacheRecord(BytesIn bytes) {
            this.askPrice = bytes.readLong();
            this.bidPrice = bytes.readLong();
        }

        @Override
        public void writeMarshallable(BytesOut bytes) {
            bytes.writeLong(askPrice);
            bytes.writeLong(bidPrice);
        }

        public LastPriceCacheRecord averagingRecord() {
            LastPriceCacheRecord average = new LastPriceCacheRecord();
            average.askPrice = (this.askPrice + this.bidPrice) >> 1;
            average.bidPrice = average.askPrice;
            return average;
        }

        public static LastPriceCacheRecord dummy = new LastPriceCacheRecord(42, 42);

        @Override
        public int stateHash() {
            return Objects.hash(askPrice, bidPrice);
        }
    }


    /**
     * Pre-process command handler
     * 1. MOVE/CANCEL commands ignored, for specific uid marked as valid for matching engine
     * 2. PLACE ORDER checked with risk ending for specific uid
     * 3. ADD USER, BALANCE_ADJUSTMENT processed for specific uid, not valid for matching engine
     * 4. BINARY_DATA commands processed for ANY uid and marked as valid for matching engine TODO which handler marks?
     * 5. RESET commands processed for any uid
     *
     * @param cmd - command
     * @param seq - command sequence
     * @return true if caller should publish sequence even if batch was not processed yet
     */
    public boolean preProcessCommand(final long seq, final OrderCommand cmd) {
        switch (cmd.command) {
            case MOVE_ORDER:
            case CANCEL_ORDER:
            case REDUCE_ORDER:
            case ORDER_BOOK_REQUEST:
                return false;

            case PLACE_ORDER:
                if (uidForThisHandler(cmd.uid)) {
                    cmd.resultCode = placeOrderRiskCheck(cmd);
                }
                return false;

            case ADD_USER:
                if (uidForThisHandler(cmd.uid)) {
                    cmd.resultCode = userProfileService.addEmptyUserProfile(cmd.uid)
                            ? CommandResultCode.SUCCESS
                            : CommandResultCode.USER_MGMT_USER_ALREADY_EXISTS;
                }
                return false;

            case BALANCE_ADJUSTMENT:
                if (uidForThisHandler(cmd.uid)) {
                    cmd.resultCode = adjustBalance(
                            cmd.uid, cmd.symbol, cmd.price, cmd.orderId, BalanceAdjustmentType.of(cmd.orderType.getCode()));
                }
                return false;

            case SUSPEND_USER:
                if (uidForThisHandler(cmd.uid)) {
                    cmd.resultCode = userProfileService.suspendUserProfile(cmd.uid);
                }
                return false;
            case RESUME_USER:
                if (uidForThisHandler(cmd.uid)) {
                    cmd.resultCode = userProfileService.resumeUserProfile(cmd.uid);
                }
                return false;

            case BINARY_DATA_COMMAND:
            case BINARY_DATA_QUERY:
                binaryCommandsProcessor.acceptBinaryFrame(cmd); // ignore return result, because it should be set by MatchingEngineRouter
                if (shardId == 0) {
                    cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
                }
                return false;

            case RESET:
                reset();
                if (shardId == 0) {
                    cmd.resultCode = CommandResultCode.SUCCESS;
                }
                return false;

            case PERSIST_STATE_MATCHING:
                if (shardId == 0) {
                    cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
                }
                return true;// true = publish sequence before finishing processing whole batch

            case PERSIST_STATE_RISK:
                final boolean isSuccess = serializationProcessor.storeData(
                        cmd.orderId,
                        seq,
                        cmd.timestamp,
                        ISerializationProcessor.SerializedModuleType.RISK_ENGINE,
                        shardId,
                        this);
                UnsafeUtils.setResultVolatile(cmd, isSuccess, CommandResultCode.SUCCESS, CommandResultCode.STATE_PERSIST_RISK_ENGINE_FAILED);
                return false;
        }
        return false;
    }


    private CommandResultCode adjustBalance(long uid, int currency, long amountDiff, long fundingTransactionId, BalanceAdjustmentType adjustmentType) {
        final CommandResultCode res = userProfileService.balanceAdjustment(uid, currency, amountDiff, fundingTransactionId);
        if (res == CommandResultCode.SUCCESS) {
            switch (adjustmentType) {
                case ADJUSTMENT: // adjust total adjustments amount
                    adjustments.addToValue(currency, -amountDiff);
                    break;

                case SUSPEND: // adjust total suspends amount
                    suspends.addToValue(currency, -amountDiff);
                    break;
            }
        }
        return res;
    }

    private void handleBinaryMessage(BinaryDataCommand message) {

        if (message instanceof BatchAddSymbolsCommand) {

            final IntObjectHashMap<CoreSymbolSpecification> symbols = ((BatchAddSymbolsCommand) message).getSymbols();
            symbols.forEach(spec -> {
                if (spec.type == SymbolType.CURRENCY_EXCHANGE_PAIR || cfgMarginTradingEnabled) {
                    symbolSpecificationProvider.addSymbol(spec);
                } else {
                    log.warn("Margin symbols are not allowed: {}", spec);
                }
            });

        } else if (message instanceof BatchAddAccountsCommand) {

            ((BatchAddAccountsCommand) message).getUsers().forEachKeyValue((uid, accounts) -> {
                if (userProfileService.addEmptyUserProfile(uid)) {
                    accounts.forEachKeyValue((cur, bal) ->
                            adjustBalance(uid, cur, bal, 1_000_000_000 + cur, BalanceAdjustmentType.ADJUSTMENT));
                } else {
                    log.debug("User already exist: {}", uid);
                }
            });
        }
    }

    private <R extends ReportResult> Optional<R> handleReportQuery(ReportQuery<R> reportQuery) {
        return reportQuery.process(this);
    }

    public boolean uidForThisHandler(final long uid) {
        return (shardMask == 0) || ((uid & shardMask) == shardId);
    }

    private CommandResultCode placeOrderRiskCheck(final OrderCommand cmd) {

        final UserProfile userProfile = userProfileService.getUserProfile(cmd.uid);
        if (userProfile == null) {
            cmd.resultCode = CommandResultCode.AUTH_INVALID_USER;
            log.warn("User profile {} not found", cmd.uid);
            return CommandResultCode.AUTH_INVALID_USER;
        }

        final CoreSymbolSpecification spec = symbolSpecificationProvider.getSymbolSpecification(cmd.symbol);
        if (spec == null) {
            log.warn("Symbol {} not found", cmd.symbol);
            return CommandResultCode.INVALID_SYMBOL;
        }

        if (cfgIgnoreRiskProcessing) {
            // skip processing
            return CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        }

        // check if account has enough funds
        final CommandResultCode resultCode = placeOrder(cmd, userProfile, spec);

        if (resultCode != CommandResultCode.VALID_FOR_MATCHING_ENGINE) {
            log.warn("{} risk result={} uid={}: Can not place {}", cmd.orderId, resultCode, userProfile.uid, cmd);
            log.warn("{} accounts:{}", cmd.orderId, userProfile.accounts);
            return CommandResultCode.RISK_NSF;
        }

        return resultCode;
    }


    private CommandResultCode placeOrder(final OrderCommand cmd,
                                         final UserProfile userProfile,
                                         final CoreSymbolSpecification spec) {


        if (spec.type == SymbolType.CURRENCY_EXCHANGE_PAIR) {

            return placeExchangeOrder(cmd, userProfile, spec);

        } else if (spec.type == SymbolType.FUTURES_CONTRACT) {

            if (!cfgMarginTradingEnabled) {
                return CommandResultCode.RISK_MARGIN_TRADING_DISABLED;
            }

            SymbolPositionRecord position = userProfile.positions.get(spec.symbolId); // TODO getIfAbsentPut?
            if (position == null) {
                position = objectsPool.get(ObjectsPool.SYMBOL_POSITION_RECORD, SymbolPositionRecord::new);
                position.initialize(userProfile.uid, spec.symbolId, spec.quoteCurrency);
                userProfile.positions.put(spec.symbolId, position);
            }

            final boolean canPlaceOrder = canPlaceMarginOrder(cmd, userProfile, spec, position);
            if (canPlaceOrder) {
                position.pendingHold(cmd.action, cmd.size);
                return CommandResultCode.VALID_FOR_MATCHING_ENGINE;
            } else {
                // try to cleanup position if refusing to place
                if (position.isEmpty()) {
                    removePositionRecord(position, userProfile);
                }
                return CommandResultCode.RISK_NSF;
            }

        } else {
            return CommandResultCode.UNSUPPORTED_SYMBOL_TYPE;
        }
    }

    private CommandResultCode placeExchangeOrder(final OrderCommand cmd,
                                                 final UserProfile userProfile,
                                                 final CoreSymbolSpecification spec) {

        final int currency = (cmd.action == OrderAction.BID) ? spec.quoteCurrency : spec.baseCurrency;

        // futures positions check for this currency
        long freeFuturesMargin = 0L;
        if (cfgMarginTradingEnabled) {
            for (final SymbolPositionRecord position : userProfile.positions) {
                if (position.currency == currency) {
                    final int recSymbol = position.symbol;
                    final CoreSymbolSpecification spec2 = symbolSpecificationProvider.getSymbolSpecification(recSymbol);
                    // add P&L subtract margin
                    freeFuturesMargin +=
                            (position.estimateProfit(spec2, lastPriceCache.get(recSymbol)) - position.calculateRequiredMarginForFutures(spec2));
                }
            }
        }

        final long size = cmd.size;
        final long orderHoldAmount;
        if (cmd.action == OrderAction.BID) {

            if (cmd.orderType == OrderType.FOK_BUDGET || cmd.orderType == OrderType.IOC_BUDGET) {

                if (cmd.reserveBidPrice != cmd.price) {
                    //log.warn("reserveBidPrice={} less than price={}", cmd.reserveBidPrice, cmd.price);
                    return CommandResultCode.RISK_INVALID_RESERVE_BID_PRICE;
                }

                orderHoldAmount = CoreArithmeticUtils.calculateAmountBidTakerFeeForBudget(size, cmd.price, spec);
                if (logDebug) log.debug("hold amount budget buy {} = {} * {} + {} * {}", cmd.price, size, spec.quoteScaleK, size, spec.takerFee);

            } else {

                if (cmd.reserveBidPrice < cmd.price) {
                    //log.warn("reserveBidPrice={} less than price={}", cmd.reserveBidPrice, cmd.price);
                    return CommandResultCode.RISK_INVALID_RESERVE_BID_PRICE;
                }
                orderHoldAmount = CoreArithmeticUtils.calculateAmountBidTakerFee(size, cmd.reserveBidPrice, spec);
                if (logDebug) log.debug("hold amount buy {} = {} * ( {} * {} + {} )", orderHoldAmount, size, cmd.reserveBidPrice, spec.quoteScaleK, spec.takerFee);
            }

        } else {

            if (cmd.price * spec.quoteScaleK < spec.takerFee) {
                // log.debug("cmd.price {} * spec.quoteScaleK {} < {} spec.takerFee", cmd.price, spec.quoteScaleK, spec.takerFee);
                // todo also check for move command
                return CommandResultCode.RISK_ASK_PRICE_LOWER_THAN_FEE;
            }

            orderHoldAmount = CoreArithmeticUtils.calculateAmountAsk(size, spec);
            if (logDebug) log.debug("hold sell {} = {} * {} ", orderHoldAmount, size, spec.baseScaleK);
        }

        if (logDebug) {
            log.debug("R1 uid={} : orderHoldAmount={} vs serProfile.accounts.get({})={} + freeFuturesMargin={}",
                    userProfile.uid, orderHoldAmount, currency, userProfile.accounts.get(currency), freeFuturesMargin);
        }

        // speculative change balance
        long newBalance = userProfile.accounts.addToValue(currency, -orderHoldAmount);

        final boolean canPlace = newBalance + freeFuturesMargin >= 0;

        if (!canPlace) {
            // revert balance change
            userProfile.accounts.addToValue(currency, orderHoldAmount);
//            log.warn("orderAmount={} > userProfile.accounts.get({})={}", orderAmount, currency, userProfile.accounts.get(currency));
            return CommandResultCode.RISK_NSF;
        } else {
            return CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        }
    }


    /**
     * Checks:
     * 1. Users account balance
     * 2. Margin
     * 3. Current limit orders
     * <p>
     * NOTE: Current implementation does not care about accounts and positions quoted in different currencies
     *
     * @param cmd         - order command
     * @param userProfile - user profile
     * @param spec        - symbol specification
     * @param position    - users position
     * @return true if placing is allowed
     */
    private boolean canPlaceMarginOrder(final OrderCommand cmd,
                                        final UserProfile userProfile,
                                        final CoreSymbolSpecification spec,
                                        final SymbolPositionRecord position) {

        final long newRequiredMarginForSymbol = position.calculateRequiredMarginForOrder(spec, cmd.action, cmd.size);
        if (newRequiredMarginForSymbol == -1) {
            // always allow placing a new order if it would not increase exposure
            return true;
        }

        // extra margin is required

        final int symbol = cmd.symbol;
        // calculate free margin for all positions same currency
        long freeMargin = 0L;
        for (final SymbolPositionRecord positionRecord : userProfile.positions) {
            final int recSymbol = positionRecord.symbol;
            if (recSymbol != symbol) {
                if (positionRecord.currency == spec.quoteCurrency) {
                    final CoreSymbolSpecification spec2 = symbolSpecificationProvider.getSymbolSpecification(recSymbol);
                    // add P&L subtract margin
                    freeMargin += positionRecord.estimateProfit(spec2, lastPriceCache.get(recSymbol));
                    freeMargin -= positionRecord.calculateRequiredMarginForFutures(spec2);
                }
            } else {
                freeMargin = position.estimateProfit(spec, lastPriceCache.get(spec.symbolId));
            }
        }

//        log.debug("newMargin={} <= account({})={} + free {}",
//                newRequiredMarginForSymbol, position.currency, userProfile.accounts.get(position.currency), freeMargin);

        // check if current balance and margin can cover new required margin for symbol position
        return newRequiredMarginForSymbol <= userProfile.accounts.get(position.currency) + freeMargin;
    }

    public boolean handlerRiskRelease(final long seq, final OrderCommand cmd) {

        final int symbol = cmd.symbol;

        final L2MarketData marketData = cmd.marketData;
        MatcherTradeEvent mte = cmd.matcherEvent;

        // skip events processing if no events (or if contains BINARY EVENT)
        if (marketData == null && (mte == null || mte.eventType == MatcherEventType.BINARY_EVENT)) {
            return false;
        }

        final CoreSymbolSpecification spec = symbolSpecificationProvider.getSymbolSpecification(symbol);
        if (spec == null) {
            throw new IllegalStateException("Symbol not found: " + symbol);
        }

        final boolean takerSell = cmd.action == OrderAction.ASK;

        if (mte != null && mte.eventType != MatcherEventType.BINARY_EVENT) {
            // at least one event to process, resolving primary/taker user profile
            // TODO processing order is reversed
            if (spec.type == SymbolType.CURRENCY_EXCHANGE_PAIR) {

                final UserProfile takerUp = uidForThisHandler(cmd.uid)
                        ? userProfileService.getUserProfileOrAddSuspended(cmd.uid)
                        : null;

                // REJECT always comes first; REDUCE is always single event
                if (mte.eventType == MatcherEventType.REDUCE || mte.eventType == MatcherEventType.REJECT) {
                    if (takerUp != null) {
                        handleMatcherRejectReduceEventExchange(cmd, mte, spec, takerSell, takerUp);
                    }
                    mte = mte.nextEvent;
                }

                if (mte != null) {
                    if (takerSell) {
                        handleMatcherEventsExchangeSell(mte, spec, takerUp);
                    } else {
                        handleMatcherEventsExchangeBuy(mte, spec, takerUp, cmd);
                    }
                }
            } else {

                final UserProfile takerUp = uidForThisHandler(cmd.uid) ? userProfileService.getUserProfileOrAddSuspended(cmd.uid) : null;

                // for margin-mode symbols also resolve position record
                final SymbolPositionRecord takerSpr = (takerUp != null) ? takerUp.getPositionRecordOrThrowEx(symbol) : null;
                do {
                    handleMatcherEventMargin(mte, spec, cmd.action, takerUp, takerSpr);
                    mte = mte.nextEvent;
                } while (mte != null);
            }
        }

        // Process marked data
        if (marketData != null && cfgMarginTradingEnabled) {
            final RiskEngine.LastPriceCacheRecord record = lastPriceCache.getIfAbsentPut(symbol, RiskEngine.LastPriceCacheRecord::new);
            record.askPrice = (marketData.askSize != 0) ? marketData.askPrices[0] : Long.MAX_VALUE;
            record.bidPrice = (marketData.bidSize != 0) ? marketData.bidPrices[0] : 0;
        }

        return false;
    }

    private void handleMatcherEventMargin(final MatcherTradeEvent ev,
                                          final CoreSymbolSpecification spec,
                                          final OrderAction takerAction,
                                          final UserProfile takerUp,
                                          final SymbolPositionRecord takerSpr) {
        if (takerUp != null) {
            if (ev.eventType == MatcherEventType.TRADE) {
                // update taker's position
                final long sizeOpen = takerSpr.updatePositionForMarginTrade(takerAction, ev.size, ev.price);
                final long fee = spec.takerFee * sizeOpen;
                takerUp.accounts.addToValue(spec.quoteCurrency, -fee);
                fees.addToValue(spec.quoteCurrency, fee);
            } else if (ev.eventType == MatcherEventType.REJECT || ev.eventType == MatcherEventType.REDUCE) {
                // for cancel/rejection only one party is involved
                takerSpr.pendingRelease(takerAction, ev.size);
            }

            if (takerSpr.isEmpty()) {
                removePositionRecord(takerSpr, takerUp);
            }
        }

        if (ev.eventType == MatcherEventType.TRADE && uidForThisHandler(ev.matchedOrderUid)) {
            // update maker's position
            final UserProfile maker = userProfileService.getUserProfileOrAddSuspended(ev.matchedOrderUid);
            final SymbolPositionRecord makerSpr = maker.getPositionRecordOrThrowEx(spec.symbolId);
            long sizeOpen = makerSpr.updatePositionForMarginTrade(takerAction.opposite(), ev.size, ev.price);
            final long fee = spec.makerFee * sizeOpen;
            maker.accounts.addToValue(spec.quoteCurrency, -fee);
            fees.addToValue(spec.quoteCurrency, fee);
            if (makerSpr.isEmpty()) {
                removePositionRecord(makerSpr, maker);
            }
        }

    }

    private void handleMatcherRejectReduceEventExchange(final OrderCommand cmd,
                                                        final MatcherTradeEvent ev,
                                                        final CoreSymbolSpecification spec,
                                                        final boolean takerSell,
                                                        final UserProfile taker) {

        //log.debug("REDUCE/REJECT {} {}", cmd, ev);

        // for cancel/rejection only one party is involved
        if (takerSell) {

            taker.accounts.addToValue(spec.baseCurrency, CoreArithmeticUtils.calculateAmountAsk(ev.size, spec));

        } else {

            if (cmd.command == OrderCommandType.PLACE_ORDER && cmd.orderType == OrderType.FOK_BUDGET) {
                taker.accounts.addToValue(spec.quoteCurrency, CoreArithmeticUtils.calculateAmountBidTakerFeeForBudget(ev.size, ev.price, spec));
            } else {
                taker.accounts.addToValue(spec.quoteCurrency, CoreArithmeticUtils.calculateAmountBidTakerFee(ev.size, ev.bidderHoldPrice, spec));
            }
            // TODO for OrderType.IOC_BUDGET - for REJECT should release leftover deposit after all trades calculated
        }

    }


    private void handleMatcherEventsExchangeSell(MatcherTradeEvent ev,
                                                 final CoreSymbolSpecification spec,
                                                 final UserProfile taker) {

        //log.debug("TRADE EXCH SELL {}", ev);

        long takerSizeForThisHandler = 0L;
        long makerSizeForThisHandler = 0L;

        long takerSizePriceForThisHandler = 0L;

        final int quoteCurrency = spec.quoteCurrency;

        while (ev != null) {
            assert ev.eventType == MatcherEventType.TRADE;

            // aggregate transfers for selling taker
            if (taker != null) {
                takerSizePriceForThisHandler += ev.size * ev.price;
                takerSizeForThisHandler += ev.size;
            }

            // process transfers for buying maker
            if (uidForThisHandler(ev.matchedOrderUid)) {
                final long size = ev.size;
                final UserProfile maker = userProfileService.getUserProfileOrAddSuspended(ev.matchedOrderUid);

                // buying, use bidderHoldPrice to calculate released amount based on price difference
                final long priceDiff = ev.bidderHoldPrice - ev.price;
                final long amountDiffToReleaseInQuoteCurrency = CoreArithmeticUtils.calculateAmountBidReleaseCorrMaker(size, priceDiff, spec);
                maker.accounts.addToValue(quoteCurrency, amountDiffToReleaseInQuoteCurrency);

                final long gainedAmountInBaseCurrency = CoreArithmeticUtils.calculateAmountAsk(size, spec);
                maker.accounts.addToValue(spec.baseCurrency, gainedAmountInBaseCurrency);

                makerSizeForThisHandler += size;
            }

            ev = ev.nextEvent;
        }

        if (taker != null) {
            taker.accounts.addToValue(quoteCurrency, takerSizePriceForThisHandler * spec.quoteScaleK - spec.takerFee * takerSizeForThisHandler);
        }

        if (takerSizeForThisHandler != 0 || makerSizeForThisHandler != 0) {

            fees.addToValue(quoteCurrency, spec.takerFee * takerSizeForThisHandler + spec.makerFee * makerSizeForThisHandler);
        }
    }

    private void handleMatcherEventsExchangeBuy(MatcherTradeEvent ev,
                                                final CoreSymbolSpecification spec,
                                                final UserProfile taker,
                                                final OrderCommand cmd) {
        //log.debug("TRADE EXCH BUY {}", ev);

        long takerSizeForThisHandler = 0L;
        long makerSizeForThisHandler = 0L;

        long takerSizePriceSum = 0L;
        long takerSizePriceHeldSum = 0L;

        final int quoteCurrency = spec.quoteCurrency;

        while (ev != null) {
            assert ev.eventType == MatcherEventType.TRADE;

            // perform transfers for taker
            if (taker != null) {

                takerSizePriceSum += ev.size * ev.price;
                takerSizePriceHeldSum += ev.size * ev.bidderHoldPrice;

                takerSizeForThisHandler += ev.size;
            }

            // process transfers for maker
            if (uidForThisHandler(ev.matchedOrderUid)) {
                final long size = ev.size;
                final UserProfile maker = userProfileService.getUserProfileOrAddSuspended(ev.matchedOrderUid);
                final long gainedAmountInQuoteCurrency = CoreArithmeticUtils.calculateAmountBid(size, ev.price, spec);
                maker.accounts.addToValue(quoteCurrency, gainedAmountInQuoteCurrency - spec.makerFee * size);
                makerSizeForThisHandler += size;
            }

            ev = ev.nextEvent;
        }

        if (taker != null) {

            if (cmd.command == OrderCommandType.PLACE_ORDER && cmd.orderType == OrderType.FOK_BUDGET) {
                // for FOK budget held sum calculated differently
                takerSizePriceHeldSum = cmd.price;
            }
            // TODO IOC_BUDGET - order can be partially rejected - need held taker fee correction

            taker.accounts.addToValue(quoteCurrency, (takerSizePriceHeldSum - takerSizePriceSum) * spec.quoteScaleK);
            taker.accounts.addToValue(spec.baseCurrency, takerSizeForThisHandler * spec.baseScaleK);
        }

        if (takerSizeForThisHandler != 0 || makerSizeForThisHandler != 0) {
            fees.addToValue(quoteCurrency, spec.takerFee * takerSizeForThisHandler + spec.makerFee * makerSizeForThisHandler);
        }
    }

    private void removePositionRecord(SymbolPositionRecord record, UserProfile userProfile) {
        userProfile.accounts.addToValue(record.currency, record.profit);
        userProfile.positions.removeKey(record.symbol);
        objectsPool.put(ObjectsPool.SYMBOL_POSITION_RECORD, record);
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {

        bytes.writeInt(shardId).writeLong(shardMask);

        symbolSpecificationProvider.writeMarshallable(bytes);
        userProfileService.writeMarshallable(bytes);
        binaryCommandsProcessor.writeMarshallable(bytes);
        SerializationUtils.marshallIntHashMap(lastPriceCache, bytes);
        SerializationUtils.marshallIntLongHashMap(fees, bytes);
        SerializationUtils.marshallIntLongHashMap(adjustments, bytes);
        SerializationUtils.marshallIntLongHashMap(suspends, bytes);
    }

    public void reset() {
        userProfileService.reset();
        symbolSpecificationProvider.reset();
        binaryCommandsProcessor.reset();
        lastPriceCache.clear();
        fees.clear();
        adjustments.clear();
        suspends.clear();
    }

    @AllArgsConstructor
    @Getter
    private static class State {
        private final SymbolSpecificationProvider symbolSpecificationProvider;
        private final UserProfileService userProfileService;
        private final BinaryCommandsProcessor binaryCommandsProcessor;
        private final IntObjectHashMap<LastPriceCacheRecord> lastPriceCache;
        private final IntLongHashMap fees;
        private final IntLongHashMap adjustments;
        private final IntLongHashMap suspends;
    }
}
