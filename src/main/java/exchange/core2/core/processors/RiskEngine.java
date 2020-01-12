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

import exchange.core2.core.common.*;
import exchange.core2.core.common.api.binary.BatchAddAccountsCommand;
import exchange.core2.core.common.api.binary.BatchAddSymbolsCommand;
import exchange.core2.core.common.api.reports.*;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.processors.journalling.ISerializationProcessor;
import exchange.core2.core.utils.CoreArithmeticUtils;
import exchange.core2.core.utils.HashingUtils;
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
public final class RiskEngine implements WriteBytesMarshallable, StateHash {

    // state
    private final SymbolSpecificationProvider symbolSpecificationProvider;
    private final UserProfileService userProfileService;
    private final BinaryCommandsProcessor binaryCommandsProcessor;
    private final IntObjectHashMap<LastPriceCacheRecord> lastPriceCache;
    private final IntLongHashMap fees;
    private final IntLongHashMap adjustments;
    private final IntLongHashMap suspends;
    private final ObjectsPool objectsPool;

    // configuration
    private final int shardId;
    private final long shardMask;

    private final ISerializationProcessor serializationProcessor;

    public RiskEngine(final int shardId,
                      final long numShards,
                      final ISerializationProcessor serializationProcessor,
                      final SharedPool sharedPool,
                      final Long loadStateId) {
        if (Long.bitCount(numShards) != 1) {
            throw new IllegalArgumentException("Invalid number of shards " + numShards + " - must be power of 2");
        }
        this.shardId = shardId;
        this.shardMask = numShards - 1;
        this.serializationProcessor = serializationProcessor;

        // initialize object pools
        final HashMap<Integer, Integer> objectsPoolConfig = new HashMap<>();
        objectsPoolConfig.put(ObjectsPool.SYMBOL_POSITION_RECORD, 1024 * 256);
        this.objectsPool = new ObjectsPool(objectsPoolConfig, sharedPool);

        if (loadStateId == null) {
            this.symbolSpecificationProvider = new SymbolSpecificationProvider();
            this.userProfileService = new UserProfileService();
            this.binaryCommandsProcessor = new BinaryCommandsProcessor(this::handleBinaryMessage, sharedPool, shardId);
            this.lastPriceCache = new IntObjectHashMap<>();
            this.fees = new IntLongHashMap();
            this.adjustments = new IntLongHashMap();
            this.suspends = new IntLongHashMap();

        } else {
            // TODO refactor, change to creator (simpler init)
            final State state = serializationProcessor.loadData(
                    loadStateId,
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
                        final BinaryCommandsProcessor binaryCommandsProcessor = new BinaryCommandsProcessor(this::handleBinaryMessage, sharedPool, bytesIn, shardId);
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
        }
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
     */
    public boolean preProcessCommand(final OrderCommand cmd) {
        switch (cmd.command) {
            case MOVE_ORDER:
            case CANCEL_ORDER:
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

            case BINARY_DATA:
                binaryCommandsProcessor.acceptBinaryFrame(cmd);
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

            case NOP:
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
                final boolean isSuccess = serializationProcessor.storeData(cmd.orderId, ISerializationProcessor.SerializedModuleType.RISK_ENGINE, shardId, this);
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

    private Optional<? extends WriteBytesMarshallable> handleBinaryMessage(Object message) {

        if (message instanceof BatchAddSymbolsCommand) {
            // TODO return status object
            final IntObjectHashMap<CoreSymbolSpecification> symbols = ((BatchAddSymbolsCommand) message).getSymbols();
            symbols.forEach(symbolSpecificationProvider::addSymbol);
            return Optional.empty();
        } else if (message instanceof BatchAddAccountsCommand) {
            // TODO return status object
            ((BatchAddAccountsCommand) message).getUsers().forEachKeyValue((uid, accounts) -> {
                if (userProfileService.addEmptyUserProfile(uid)) {
                    accounts.forEachKeyValue((cur, bal) ->
                            adjustBalance(uid, cur, bal, 1_000_000_000 + cur, BalanceAdjustmentType.ADJUSTMENT));
                } else {
                    log.debug("User already exist: {}", uid);
                }
            });
            return Optional.empty();
        } else if (message instanceof ReportQuery) {
            return processReport((ReportQuery) message);
        } else {
            return Optional.empty();
        }
    }


    // TODO use with common module accepting implementations?
    private Optional<? extends WriteBytesMarshallable> processReport(ReportQuery reportQuery) {

        switch (reportQuery.getReportType()) {

            case STATE_HASH:
                return reportStateHash();

            case SINGLE_USER_REPORT:
                return reportSingleUser((SingleUserReportQuery) reportQuery);

            case TOTAL_CURRENCY_BALANCE:
                return reportGlobalBalance();

            default:
                throw new IllegalStateException("Report not implemented");
        }
    }

    private Optional<StateHashReportResult> reportStateHash() {
        return Optional.of(new StateHashReportResult(stateHash()));
    }

    private Optional<SingleUserReportResult> reportSingleUser(final SingleUserReportQuery query) {
        if (uidForThisHandler(query.getUid())) {
            final UserProfile userProfile = userProfileService.getUserProfile(query.getUid());
            return Optional.of(new SingleUserReportResult(
                    userProfile,
                    null,
                    userProfile == null ? SingleUserReportResult.ExecutionStatus.USER_NOT_FOUND : SingleUserReportResult.ExecutionStatus.OK));
        } else {
            return Optional.empty();
        }
    }

    private Optional<TotalCurrencyBalanceReportResult> reportGlobalBalance() {

        // prepare fast price cache for profit estimation with some price (exact value is not important, except ask==bid condition)
        final IntObjectHashMap<LastPriceCacheRecord> dummyLastPriceCache = new IntObjectHashMap<>();
        lastPriceCache.forEachKeyValue((s, r) -> dummyLastPriceCache.put(s, r.averagingRecord()));

        final IntLongHashMap currencyBalance = new IntLongHashMap();

        final IntLongHashMap symbolOpenInterestLong = new IntLongHashMap();
        final IntLongHashMap symbolOpenInterestShort = new IntLongHashMap();

        userProfileService.getUserProfiles().forEach(userProfile -> {
            userProfile.accounts.forEachKeyValue(currencyBalance::addToValue);
            userProfile.positions.forEachKeyValue((symbolId, positionRecord) -> {
                final CoreSymbolSpecification spec = symbolSpecificationProvider.getSymbolSpecification(symbolId);
                final LastPriceCacheRecord avgPrice = dummyLastPriceCache.getIfAbsentPut(symbolId, LastPriceCacheRecord.dummy);
                currencyBalance.addToValue(positionRecord.currency, positionRecord.estimateProfit(spec, avgPrice));

                if (positionRecord.direction == PositionDirection.LONG) {
                    symbolOpenInterestLong.addToValue(symbolId, positionRecord.openVolume);
                } else if (positionRecord.direction == PositionDirection.SHORT) {
                    symbolOpenInterestShort.addToValue(symbolId, positionRecord.openVolume);
                }
            });
        });

        return Optional.of(
                new TotalCurrencyBalanceReportResult(
                        currencyBalance,
                        new IntLongHashMap(fees),
                        new IntLongHashMap(adjustments),
                        new IntLongHashMap(suspends),
                        null,
                        symbolOpenInterestLong,
                        symbolOpenInterestShort));
    }

    private boolean uidForThisHandler(final long uid) {
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

        // check if account has enough funds
        if (!placeOrder(cmd, userProfile, spec)) {
            log.warn("{} NSF uid={}: Can not place {}", cmd.orderId, userProfile.uid, cmd);
            log.warn("{} accounts:{}", cmd.orderId, userProfile.accounts);
            return CommandResultCode.RISK_NSF;
        }

        return CommandResultCode.VALID_FOR_MATCHING_ENGINE;
    }


    private boolean placeOrder(final OrderCommand cmd,
                               final UserProfile userProfile,
                               final CoreSymbolSpecification spec) {


        if (spec.type == SymbolType.CURRENCY_EXCHANGE_PAIR) {

            return placeExchangeOrder(cmd, userProfile, spec);

        } else if (spec.type == SymbolType.FUTURES_CONTRACT) {

            SymbolPositionRecord position = userProfile.positions.get(spec.symbolId); // TODO getIfAbsentPut?
            if (position == null) {
                position = objectsPool.get(ObjectsPool.SYMBOL_POSITION_RECORD, SymbolPositionRecord::new);
                position.initialize(userProfile.uid, spec.symbolId, spec.quoteCurrency);
                userProfile.positions.put(spec.symbolId, position);
            }

            final boolean canPlaceOrder = canPlaceMarginOrder(cmd, userProfile, spec, position);
            if (canPlaceOrder) {
                position.pendingHold(cmd.action, cmd.size);
                return true;
            } else {
                // try to cleanup position if refusing to place
                if (position.isEmpty()) {
                    removePositionRecord(position, userProfile);
                }
                return false;
            }

        } else {
            log.error("Symbol {} - unsupported type: {}", cmd.symbol, spec.type);
            return false;
        }
    }

    private boolean placeExchangeOrder(final OrderCommand cmd,
                                       final UserProfile userProfile,
                                       final CoreSymbolSpecification spec) {

        final int currency = (cmd.action == OrderAction.BID) ? spec.quoteCurrency : spec.baseCurrency;

        // futures positions check for this currency
        long freeFuturesMargin = 0L;
        for (final SymbolPositionRecord position : userProfile.positions) {
            if (position.currency == currency) {
                final int recSymbol = position.symbol;
                final CoreSymbolSpecification spec2 = symbolSpecificationProvider.getSymbolSpecification(recSymbol);
                // add P&L subtract margin
                freeFuturesMargin += position.estimateProfit(spec2, lastPriceCache.get(recSymbol));
                freeFuturesMargin -= position.calculateRequiredMarginForFutures(spec2);
            }
        }

        if (cmd.action == OrderAction.BID && cmd.reserveBidPrice < cmd.price) {
            // TODO refactor
            log.warn("reserveBidPrice={} less than price={}", cmd.reserveBidPrice, cmd.price);
            return false;
        }

        final long orderAmount = CoreArithmeticUtils.calculateHoldAmount(cmd.action, cmd.size, cmd.action == OrderAction.BID ? cmd.reserveBidPrice : cmd.price, spec);

//        log.debug("--------- {} -----------", cmd.orderId);
//        log.debug("serProfile.accounts.get(currency)={}", userProfile.accounts.get(currency));
//        log.debug("freeFuturesMargin={}", freeFuturesMargin);
//        log.debug("orderAmount={}", orderAmount);

        // speculative change balance
        long newBalance = userProfile.accounts.addToValue(currency, -orderAmount);

        final boolean canPlace = newBalance + freeFuturesMargin >= 0;

        if (!canPlace) {
            // revert balance change
            userProfile.accounts.addToValue(currency, orderAmount);
//            log.warn("orderAmount={} > userProfile.accounts.get({})={}", orderAmount, currency, userProfile.accounts.get(currency));
        }

        return canPlace;
    }


    /**
     * Checks:
     * 1. Users account balance
     * 2. Margin
     * 3. Current limit orders
     * <p>
     * NOTE: Current implementation does not care about accounts and positions quoted in different currencies
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

    public boolean handlerRiskRelease(final OrderCommand cmd) {

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

        if (mte != null && mte.eventType != MatcherEventType.BINARY_EVENT) {
            // at least one event to process, resolving primary/taker user profile
            final UserProfile takerUp = uidForThisHandler(cmd.uid) ? userProfileService.getUserProfileOrAddSuspended(cmd.uid) : null;
            // TODO processing order is reversed
            if (spec.type == SymbolType.CURRENCY_EXCHANGE_PAIR) {
                do {
                    handleMatcherEventExchange(mte, spec, cmd.action, takerUp);
                    mte = mte.nextEvent;
                } while (mte != null);
            } else {
                // for margin-mode symbols also resolve position record
                final SymbolPositionRecord takerSpr = (takerUp != null) ? takerUp.getPositionRecordOrThrowEx(symbol) : null;
                do {
                    handleMatcherEventMargin(mte, spec, cmd.action, takerUp, takerSpr);
                    mte = mte.nextEvent;
                } while (mte != null);
            }
        }

        // Process marked data
        if (marketData != null) {
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
            } else if (ev.eventType == MatcherEventType.REJECTION || ev.eventType == MatcherEventType.CANCEL) {
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


    private void handleMatcherEventExchange(final MatcherTradeEvent ev,
                                            final CoreSymbolSpecification spec,
                                            final OrderAction takerAction,
                                            final UserProfile takerUp) {
        if (takerUp != null) {
            if (ev.eventType == MatcherEventType.TRADE) {
                // TODO group by user profile ??

                // perform account-to-account transfers

//                log.debug("Processing release for taker");
                processExchangeHoldRelease(ev, spec, true, takerAction, takerUp);


            } else if (ev.eventType == MatcherEventType.REJECTION || ev.eventType == MatcherEventType.CANCEL) {

//                log.debug("CANCEL/REJ uid: {}", ev.activeOrderUid);

                // for cancel/rejection only one party is involved
                final int currency = (takerAction == OrderAction.ASK) ? spec.baseCurrency : spec.quoteCurrency;
                final long amountForRelease = CoreArithmeticUtils.calculateHoldAmount(takerAction, ev.size, ev.bidderHoldPrice, spec);

                takerUp.accounts.addToValue(currency, amountForRelease);

//                log.debug("REJ/CAN ASK: uid={} amountToRelease = {}  ACC:{}",
//                        ev.activeOrderUid, amountForRelease, userProfileService.getUserProfile(ev.activeOrderUid).accounts);

            }
        }

        if (ev.eventType == MatcherEventType.TRADE && uidForThisHandler(ev.matchedOrderUid)) {
            //                log.debug("Processing release for maker");
            processExchangeHoldRelease(ev, spec, false, takerAction, takerUp);
        }
    }

    private void processExchangeHoldRelease(MatcherTradeEvent ev,
                                            CoreSymbolSpecification spec,
                                            boolean isTaker,
                                            final OrderAction takerAction,
                                            final UserProfile takerUp) {
        final long size = ev.size;
        final UserProfile up = isTaker ? takerUp : userProfileService.getUserProfileOrAddSuspended(ev.matchedOrderUid);

        final long feeForSize = (isTaker ? spec.takerFee : spec.makerFee) * size;
        fees.addToValue(spec.quoteCurrency, feeForSize);

        final boolean isSelling = takerAction == OrderAction.BID ^ isTaker;
        if (isSelling) {

            // selling
            final long obtainedAmountInQuoteCurrency = CoreArithmeticUtils.calculateAmountBid(size, ev.price, spec);
            up.accounts.addToValue(spec.quoteCurrency, obtainedAmountInQuoteCurrency - feeForSize);
//            log.debug("{} sells - getting {} -fee:{} (in quote cur={}) size={} ACCOUNTS:{}", up.uid, obtainedAmountInQuoteCurrency, feeForSize, spec.quoteCurrency, size, userProfileService.getUserProfile(uid).accounts);
        } else {

            //final long makerFeeCorrection = isTaker ? 0 : (spec.takerFee - spec.makerFee) * size;
            //log.debug("makerFeeCorrection={} ....(   - makerFee {} ) * {}", makerFeeCorrection, spec.makerFee, size);

            // buying, use bidderHoldPrice to calculate released amount based on price difference
            final long amountDiffToReleaseInQuoteCurrency = CoreArithmeticUtils.calculateAmountBidReleaseCorr(size, ev.bidderHoldPrice - ev.price, spec, isTaker);
            up.accounts.addToValue(spec.quoteCurrency, amountDiffToReleaseInQuoteCurrency);

            final long obtainedAmountInBaseCurrency = CoreArithmeticUtils.calculateAmountAsk(size, spec);
            up.accounts.addToValue(spec.baseCurrency, obtainedAmountInBaseCurrency);

//            log.debug("{} buys - amountDiffToReleaseInQuoteCurrency={} ({}-{}) (in quote cur={})",
//                    up.uid, amountDiffToReleaseInQuoteCurrency, ev.bidderHoldPrice, ev.price, spec.quoteCurrency);
//            log.debug("{} buys - getting {} (in base cur={}) size={} ACCOUNTS:{}",
//                    up.uid, obtainedAmountInBaseCurrency, spec.baseCurrency, size, userProfileService.getUserProfile(uid).accounts);
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

    @Override
    public int stateHash() {

        return Objects.hash(
                shardId,
                shardMask,
                symbolSpecificationProvider.stateHash(),
                userProfileService.stateHash(),
                binaryCommandsProcessor.stateHash(),
                HashingUtils.stateHash(lastPriceCache),
                fees.hashCode(),
                adjustments.hashCode(),
                suspends.hashCode());

        //log.debug("HASH RE{}/{} hash={} -- ssp={} ups={} bcp={} lpc={}", shardId, shardMask, hash, symbolSpecificationProvider.stateHash(), userProfileService.stateHash(), binaryCommandsProcessor.stateHash(), lastPriceCache.hashCode());
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
