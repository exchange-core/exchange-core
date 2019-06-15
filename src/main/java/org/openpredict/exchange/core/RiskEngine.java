package org.openpredict.exchange.core;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.openpredict.exchange.beans.*;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.beans.cmd.OrderCommandType;
import org.openpredict.exchange.core.journalling.ISerializationProcessor;

import java.util.Objects;

import static net.openhft.chronicle.core.UnsafeMemory.UNSAFE;
import static org.openpredict.exchange.beans.MatcherEventType.*;
import static org.openpredict.exchange.beans.cmd.OrderCommandType.*;
import static org.openpredict.exchange.core.Utils.OFFSET_ORDER_ID;

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

    // configuration
    private final int shardId;
    private final long shardMask;

    private final ISerializationProcessor serializationProcessor;

    public RiskEngine(final int shardId, final long numShards, final ISerializationProcessor serializationProcessor, final Long loadStateId) {
        if (Long.bitCount(numShards) != 1) {
            throw new IllegalArgumentException("Invalid number of shards " + numShards + " - must be power of 2");
        }
        this.shardId = shardId;
        this.shardMask = numShards - 1;
        this.serializationProcessor = serializationProcessor;

        if (loadStateId == null) {
            this.symbolSpecificationProvider = new SymbolSpecificationProvider();
            this.userProfileService = new UserProfileService();
            this.binaryCommandsProcessor = new BinaryCommandsProcessor(symbolSpecificationProvider::addSymbol, CommandResultCode.VALID_FOR_MATCHING_ENGINE);
            this.lastPriceCache = new IntObjectHashMap<>();

        } else {
            // TODO change to creator (simpler init)
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
                        final BinaryCommandsProcessor binaryCommandsProcessor = new BinaryCommandsProcessor(symbolSpecificationProvider::addSymbol, CommandResultCode.VALID_FOR_MATCHING_ENGINE, bytesIn);
                        final IntObjectHashMap<LastPriceCacheRecord> lastPriceCache = Utils.readIntHashMap(bytesIn, LastPriceCacheRecord::new);
                        return new State(symbolSpecificationProvider, userProfileService, binaryCommandsProcessor, lastPriceCache);
                    });

            this.symbolSpecificationProvider = state.symbolSpecificationProvider;
            this.userProfileService = state.userProfileService;
            this.binaryCommandsProcessor = state.binaryCommandsProcessor;
            this.lastPriceCache = state.lastPriceCache;
        }
    }

    public static class LastPriceCacheRecord implements BytesMarshallable, StateHash {
        public long askPrice = Long.MAX_VALUE;
        public long bidPrice = 0L;

        public LastPriceCacheRecord() {
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

        final OrderCommandType command = cmd.command;

        if (command == MOVE_ORDER || command == CANCEL_ORDER || command == ORDER_BOOK_REQUEST) {
            return false;

        } else if (command == PLACE_ORDER) {
            if (uidForThisHandler(cmd.uid)) {
                cmd.resultCode = placeOrderRiskCheck(cmd);
            }
        } else if (command == ADD_USER) {
            if (uidForThisHandler(cmd.uid)) {
                cmd.resultCode = userProfileService.addEmptyUserProfile(cmd.uid);
            }
        } else if (command == BALANCE_ADJUSTMENT) {
            if (uidForThisHandler(cmd.uid)) {
                cmd.resultCode = userProfileService.balanceAdjustment(cmd.uid, cmd.symbol, cmd.price, cmd.orderId);
            }
        } else if (command == BINARY_DATA) {
            binaryCommandsProcessor.binaryData(cmd);
        } else if (command == RESET) {
            reset();
            if (shardId == 0) {
                cmd.resultCode = CommandResultCode.SUCCESS;
            }
        } else if (command == NOP) {
            if (shardId == 0) {
                cmd.resultCode = CommandResultCode.SUCCESS;
            }
        } else if (command == PERSIST_STATE_MATCHING) {
            if (shardId == 0) {
                cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
            }
            return true; // true = publish sequence before finishing processing whole batch
        } else if (command == PERSIST_STATE_RISK) {
            final boolean isSuccess = serializationProcessor.storeData(cmd.orderId, ISerializationProcessor.SerializedModuleType.RISK_ENGINE, shardId, this);
            Utils.setResultVolatile(cmd, isSuccess, CommandResultCode.SUCCESS, CommandResultCode.STATE_PERSIST_RISK_ENGINE_FAILED);

        } else if (command == STATE_HASH_REQUEST) {
            // common hash as sum of each module hash (for simplicity)
            UNSAFE.getAndAddLong(cmd, OFFSET_ORDER_ID, stateHash());

            if (shardId == 0) {
                cmd.resultCode = CommandResultCode.ACCEPTED;
            }
        }

        return false;
    }

    /**
     * Post process command
     *
     * @param cmd
     */
    public boolean handlerRiskRelease(final OrderCommand cmd) {
        handlerRiskRelease(cmd.symbol, cmd.marketData, cmd.matcherEvent);
        return false;
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

        userProfile.commandsCounter++; // TODO should also set for MOVE
        return CommandResultCode.VALID_FOR_MATCHING_ENGINE;
    }


    private boolean placeOrder(final OrderCommand cmd,
                               final UserProfile userProfile,
                               final CoreSymbolSpecification spec) {


        if (spec.type == SymbolType.CURRENCY_EXCHANGE_PAIR) {

            if (canPlaceExchangeOrder(cmd, userProfile, spec)) {
                userProfile.getOrCreatePortfolioRecordExch(spec).pendingHold(cmd.action, cmd.size);
                return true;
            } else {
                return false;
            }

        } else if (spec.type == SymbolType.FUTURES_CONTRACT) {

            final SymbolPortfolioRecord portfolio = userProfile.getOrCreatePortfolioRecord(spec);
            final boolean canPlaceOrder = canPlaceFuturesOrder(cmd, userProfile, spec, portfolio);
            if (canPlaceOrder) {
                portfolio.pendingHold(cmd.action, cmd.size);
                return true;
            } else {
                // try to cleanup portfolio if refusing to place
                userProfile.removeRecordIfEmpty(portfolio);
                return false;
            }

        } else {
            log.error("Symbol {} - unsupported type: {}", cmd.symbol, spec.type);
            return false;
        }
    }

    private boolean canPlaceExchangeOrder(final OrderCommand cmd,
                                          final UserProfile userProfile,
                                          final CoreSymbolSpecification spec) {

        final int currency = (cmd.action == OrderAction.BID) ? spec.quoteCurrency : spec.baseCurrency;

        // go through all positions and check reserved in this currency
        long pendingAmount = 0L;
        for (final SymbolPortfolioRecordExchange portfolioRecord : userProfile.exchangePortfolio) {
            final CoreSymbolSpecification prSpec = symbolSpecificationProvider.getSymbolSpecification(portfolioRecord.symbol);
            if (prSpec.type == SymbolType.CURRENCY_EXCHANGE_PAIR) {
                if (prSpec.baseCurrency == currency) {
                    // for asks - adding pending sell volume TODO multiplier
                    pendingAmount += portfolioRecord.pendingSellSize;
                } else if (prSpec.quoteCurrency == currency) {
                    // for bids - adding pending buy amount (based on prices)
                    pendingAmount += portfolioRecord.pendingBuyAmount;
                }
            }
        }

        // futures positions check for this currency
        long freeFuturesMargin = 0L;
        for (final SymbolPortfolioRecord portfolioRecord : userProfile.portfolio) {
            if (portfolioRecord.currency == currency) {
                final int recSymbol = portfolioRecord.symbol;
                final CoreSymbolSpecification spec2 = symbolSpecificationProvider.getSymbolSpecification(recSymbol);
                // add P&L subtract margin
                freeFuturesMargin += portfolioRecord.estimateProfit(spec2, lastPriceCache.get(recSymbol));
                freeFuturesMargin -= portfolioRecord.calculateRequiredDepositForFutures(spec2);
            }
        }

        long orderAmount =  (cmd.action == OrderAction.BID)
                ? cmd.size * Math.max(cmd.price, cmd.price2) * spec.quoteScaleK
                : cmd.size * spec.baseScaleK;

//        log.debug("userProfile.accounts.get(currency)={}", userProfile.accounts.get(currency));
//        log.debug("freeFuturesMargin={}", freeFuturesMargin);
//        log.debug("orderAmount={}", orderAmount);
//        log.debug("pendingAmount={}", pendingAmount);

        // TODO fees

        return userProfile.accounts.get(currency) + freeFuturesMargin >= orderAmount + pendingAmount;
    }

    /**
     * Checks:
     * 1. Users account balance
     * 2. Margin
     * 3. Current limit orders
     * <p>
     * NOTE: Current implementation does not care about accounts and positions quoted in different currencies
     */
    private boolean canPlaceFuturesOrder(final OrderCommand cmd,
                                         final UserProfile userProfile,
                                         final CoreSymbolSpecification spec,
                                         final SymbolPortfolioRecord portfolio) {

        final long newRequiredDepositForSymbol = portfolio.calculateRequiredDepositForOrder(spec, cmd.action, cmd.size);
        if (newRequiredDepositForSymbol == -1) {
            // always allow placing a new order if it would not increase exposure
            return true;
        }

        // extra deposit is required

        final int symbol = cmd.symbol;
        // calculate free margin for all positions same currency
        long freeMargin = 0L;
        for (final SymbolPortfolioRecord portfolioRecord : userProfile.portfolio) {
            final int recSymbol = portfolioRecord.symbol;
            if (recSymbol != symbol) {
                if (portfolioRecord.currency == spec.quoteCurrency) {
                    final CoreSymbolSpecification spec2 = symbolSpecificationProvider.getSymbolSpecification(recSymbol);
                    // add P&L subtract margin
                    freeMargin += portfolioRecord.estimateProfit(spec2, lastPriceCache.get(recSymbol));
                    freeMargin -= portfolioRecord.calculateRequiredDepositForFutures(spec2);
                }
            } else {
                freeMargin = portfolio.estimateProfit(spec, lastPriceCache.get(spec.symbolId));
            }
        }

        // check if current balance and margin can cover new required margin for symbol position
        return newRequiredDepositForSymbol <= userProfile.accounts.get(portfolio.currency) + freeMargin;
    }

    public void handlerRiskRelease(final int symbol,
                                   final L2MarketData marketData,
                                   MatcherTradeEvent mte) {

        if (marketData == null && mte == null) {
            return;
        }

        final CoreSymbolSpecification spec = symbolSpecificationProvider.getSymbolSpecification(symbol);
        if (spec == null) {
            throw new IllegalStateException("Symbol not found: " + symbol);
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
            final RiskEngine.LastPriceCacheRecord record = lastPriceCache.getIfAbsentPut(symbol, RiskEngine.LastPriceCacheRecord::new);
            record.askPrice = (marketData.askSize != 0) ? marketData.askPrices[0] : Long.MAX_VALUE;
            record.bidPrice = (marketData.bidSize != 0) ? marketData.bidPrices[0] : 0;
        }
    }

    private void handleMatcherEventMargin(final MatcherTradeEvent ev, final CoreSymbolSpecification spec) {

        final long size = ev.size;

        if (ev.eventType == TRADE) {
            // TODO group by user profile ??
            if (uidForThisHandler(ev.activeOrderUid)) {
                // update taker's portfolio
                final UserProfile taker = userProfileService.getUserProfileOrThrowEx(ev.activeOrderUid);
                final SymbolPortfolioRecord takerSpr = taker.getPortfolioRecordOrThrowEx(ev.symbol);
                takerSpr.updatePortfolioForMarginTrade(ev.activeOrderAction, size, ev.price, spec.takerFee);
                taker.removeRecordIfEmpty(takerSpr);
            }

            if (uidForThisHandler(ev.matchedOrderUid)) {
                // update maker's portfolio
                final UserProfile maker = userProfileService.getUserProfileOrThrowEx(ev.matchedOrderUid);
                final SymbolPortfolioRecord makerSpr = maker.getPortfolioRecordOrThrowEx(ev.symbol);
                makerSpr.updatePortfolioForMarginTrade(ev.activeOrderAction.opposite(), size, ev.price, spec.makerFee);
                maker.removeRecordIfEmpty(makerSpr);
            }

        } else if (ev.eventType == REJECTION || ev.eventType == REDUCE) {

            if (uidForThisHandler(ev.activeOrderUid)) {
                // for reduce/rejection only one party is involved
                final UserProfile up = userProfileService.getUserProfileOrThrowEx(ev.activeOrderUid);
                final SymbolPortfolioRecord spr = up.getPortfolioRecordOrThrowEx(ev.symbol);
                spr.pendingRelease(ev.activeOrderAction, size);
                up.removeRecordIfEmpty(spr);
            }

        } else {
            log.error("unsupported eventType: {}", ev.eventType);
        }
    }


    private void handleMatcherEventExchange(final MatcherTradeEvent ev, final CoreSymbolSpecification spec) {

        final long size = ev.size;

        if (ev.eventType == TRADE) {
            // TODO group by user profile ??

            boolean processTaker = uidForThisHandler(ev.activeOrderUid);
            boolean processMaker = uidForThisHandler(ev.matchedOrderUid);

            if (processTaker || processMaker) {
                // perform account-to-account transfers
                // TODO multiplier ???
                final long amount = ev.price * size;
                final long amountInCounterCurrency = amount * spec.quoteScaleK;
                final long amountInBaseCurrency = size * spec.baseScaleK;

                // TODO add commission
                final long takerFee = spec.takerFee * size;
                final long makerFee = spec.makerFee * size;

                if (processTaker) {
                    // release taker's portfolio
                    final UserProfile taker = userProfileService.getUserProfileOrThrowEx(ev.activeOrderUid);
                    final SymbolPortfolioRecordExchange takerSpr = taker.getPortfolioExchRecordOrThrowEx(ev.symbol);

                    if (ev.activeOrderAction == OrderAction.ASK) {
                        // taker is selling
                        takerSpr.pendingSellRelease(size);
                        taker.accounts.addToValue(spec.quoteCurrency, amountInCounterCurrency);
                        taker.accounts.addToValue(spec.baseCurrency, -amountInBaseCurrency - takerFee);
                    } else {
                        // taker is buying, use takerHoldPrice price to calculate released amount
                        takerSpr.pendingBuyRelease(ev.holdPrice2 * size);
                        taker.accounts.addToValue(spec.quoteCurrency, -amountInCounterCurrency);
                        taker.accounts.addToValue(spec.baseCurrency, amountInBaseCurrency - takerFee);
                    }

                    taker.removeRecordIfEmpty(takerSpr);
                }

                if (processMaker) {
                    // release maker's portfolio
                    final UserProfile maker = userProfileService.getUserProfileOrThrowEx(ev.matchedOrderUid);
                    final SymbolPortfolioRecordExchange makerSpr = maker.getPortfolioExchRecordOrThrowEx(ev.symbol);

                    if (ev.activeOrderAction == OrderAction.ASK) {
                        // maker is buying, use fixed price to calculate released amount
                        makerSpr.pendingBuyRelease(amount);
                        maker.accounts.addToValue(spec.baseCurrency, amountInBaseCurrency - makerFee);
                        maker.accounts.addToValue(spec.quoteCurrency, -amountInCounterCurrency);
                    } else {
                        // maker is selling
                        makerSpr.pendingSellRelease(size);
                        maker.accounts.addToValue(spec.baseCurrency, -amountInBaseCurrency - makerFee);
                        maker.accounts.addToValue(spec.quoteCurrency, amountInCounterCurrency);
                    }

                    maker.removeRecordIfEmpty(makerSpr);
                }
            }

        } else if (ev.eventType == REJECTION || ev.eventType == REDUCE) {
            if (uidForThisHandler(ev.activeOrderUid)) {
                // for reduce/rejection only one party is involved
                final UserProfile up = userProfileService.getUserProfileOrThrowEx(ev.activeOrderUid);
                final SymbolPortfolioRecordExchange spr = up.getPortfolioExchRecordOrThrowEx(ev.symbol);
                if (ev.activeOrderAction == OrderAction.ASK) {
                    spr.pendingSellRelease(size);
                } else {
                    spr.pendingBuyRelease(size * ev.holdPrice2);
                }
                up.removeRecordIfEmpty(spr);
            }
        } else {
            log.error("unsupported eventType: {}", ev.eventType);
        }
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {

        bytes.writeInt(shardId).writeLong(shardMask);

        symbolSpecificationProvider.writeMarshallable(bytes);
        userProfileService.writeMarshallable(bytes);
        binaryCommandsProcessor.writeMarshallable(bytes);
        Utils.marshallIntHashMap(lastPriceCache, bytes);
    }

    public void reset() {
        userProfileService.reset();
        symbolSpecificationProvider.reset();
        binaryCommandsProcessor.reset();
        lastPriceCache.clear();
    }

    @Override
    public int stateHash() {

        return Objects.hash(
                shardId,
                shardMask,
                symbolSpecificationProvider.stateHash(),
                userProfileService.stateHash(),
                binaryCommandsProcessor.stateHash(),
                Utils.stateHash(lastPriceCache));

        //log.debug("HASH RE{}/{} hash={} -- ssp={} ups={} bcp={} lpc={}", shardId, shardMask, hash, symbolSpecificationProvider.stateHash(), userProfileService.stateHash(), binaryCommandsProcessor.stateHash(), lastPriceCache.hashCode());
    }

    @AllArgsConstructor
    @Getter
    public class State {
        private final SymbolSpecificationProvider symbolSpecificationProvider;
        private final UserProfileService userProfileService;
        private final BinaryCommandsProcessor binaryCommandsProcessor;
        private final IntObjectHashMap<LastPriceCacheRecord> lastPriceCache;
    }
}
