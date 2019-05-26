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
import org.openpredict.exchange.core.journalling.ISerializationProcessor;

import static org.openpredict.exchange.beans.MatcherEventType.*;
import static org.openpredict.exchange.beans.cmd.OrderCommandType.*;

/**
 * Stateful risk engine
 */
@Slf4j
public final class RiskEngine implements WriteBytesMarshallable {

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

    public static class LastPriceCacheRecord implements BytesMarshallable {
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
    public void preProcessCommand(OrderCommand cmd) {

        if (cmd.command == MOVE_ORDER || cmd.command == CANCEL_ORDER || cmd.command == ORDER_BOOK_REQUEST) {
            return;
        }

        if (cmd.command == PLACE_ORDER) {
            if (uidForThisHandler(cmd.uid)) {
                cmd.resultCode = placeOrderRiskCheck(cmd);
            }
        } else if (cmd.command == ADD_USER) {
            if (uidForThisHandler(cmd.uid)) {
                cmd.resultCode = userProfileService.addEmptyUserProfile(cmd.uid);
            }
        } else if (cmd.command == BALANCE_ADJUSTMENT) {
            if (uidForThisHandler(cmd.uid)) {
                cmd.resultCode = userProfileService.balanceAdjustment(cmd.uid, cmd.symbol, cmd.price, cmd.orderId);
            }
        } else if (cmd.command == BINARY_DATA) {
            binaryCommandsProcessor.binaryData(cmd);
        } else if (cmd.command == RESET) {
            reset();
            if (shardId == 0) {
                cmd.resultCode = CommandResultCode.SUCCESS;
            }
        } else if (cmd.command == NOP) {
            if (shardId == 0) {
                cmd.resultCode = CommandResultCode.SUCCESS;
            }
        } else if (cmd.command == PERSIST_STATE) {
            serializationProcessor.storeData(cmd.orderId,
                    ISerializationProcessor.SerializedModuleType.RISK_ENGINE,
                    shardId,
                    this);

            if (shardId == 0) {
                cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
            }
        }
    }

    /**
     * Post process command
     *
     * @param cmd
     */
    public void handlerRiskRelease(final OrderCommand cmd) {
        handlerRiskRelease(cmd.symbol, cmd.marketData, cmd.matcherEvent);
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
            log.warn("NSF uid={}: Can not place {}", userProfile.uid, cmd);
            log.warn("accounts:{}", userProfile.accounts);
            return CommandResultCode.RISK_NSF;
        }

        userProfile.commandsCounter++; // TODO should also set for MOVE
        return CommandResultCode.VALID_FOR_MATCHING_ENGINE;
    }


    private boolean placeOrder(final OrderCommand cmd,
                               final UserProfile userProfile,
                               final CoreSymbolSpecification spec) {

        final SymbolPortfolioRecord portfolio = userProfile.getOrCreatePortfolioRecord(spec);

        final boolean canPlaceOrder;
        if (spec.type == SymbolType.CURRENCY_EXCHANGE_PAIR) {
            canPlaceOrder = placeExchangeOrder(cmd, userProfile, spec);
        } else if (spec.type == SymbolType.FUTURES_CONTRACT) {
            canPlaceOrder = placeMarginTradeOrder(cmd, userProfile, spec, portfolio);
        } else {
            log.error("Symbol {} - unsupported type: {}", cmd.symbol, spec.type);
            canPlaceOrder = false;
        }

        if (canPlaceOrder) {
            portfolio.pendingHold(cmd.action, cmd.size);
        } else {
            // try to cleanup portfolio if refusing to place
            userProfile.removeRecordIfEmpty(portfolio);
        }

        return canPlaceOrder;
    }

    private boolean placeExchangeOrder(final OrderCommand cmd,
                                       final UserProfile userProfile,
                                       final CoreSymbolSpecification spec) {

        final int currency = (cmd.action == OrderAction.BID) ? spec.quoteCurrency : spec.baseCurrency;

        final long currencyAccountBalance = userProfile.accounts.get(currency);

        // go through all positions and check reserved in this currency
        long pendingAmount = 0L;
        for (SymbolPortfolioRecord portfolioRecord : userProfile.portfolio) {
            final CoreSymbolSpecification prSpec = symbolSpecificationProvider.getSymbolSpecification(portfolioRecord.symbol);
            // TODO support futures positions checks (margin, pnl, etc)
            // TODO split futures and exchange positions hashtables?
            if (prSpec.type == SymbolType.CURRENCY_EXCHANGE_PAIR) {
                if (prSpec.baseCurrency == currency) {
                    pendingAmount += portfolioRecord.pendingSellSize;
                } else if (prSpec.quoteCurrency == currency) {
                    pendingAmount += portfolioRecord.pendingBuySize;
                }
            }
        }

        final long size = cmd.size;

        // TODO fees
        return currencyAccountBalance - pendingAmount >= size;
    }

    /**
     * Checks:
     * 1. Users account balance
     * 2. Margin
     * 3. Current limit orders
     * <p>
     * NOTE: Current implementation does not care about accounts and positions quoted in different currencies
     */
    private boolean placeMarginTradeOrder(final OrderCommand cmd,
                                          final UserProfile userProfile,
                                          final CoreSymbolSpecification spec,
                                          final SymbolPortfolioRecord portfolio) {

        final long newRequiredDepositForSymbol = portfolio.calculateRequiredDepositForOrder(spec, cmd.action, cmd.size);
        if (newRequiredDepositForSymbol == -1) {
            // always allow placing a new order if it would not increase exposure
            return true;
        }

        long estimatedSymbolProfit = portfolio.estimateProfit(spec, lastPriceCache.get(spec.symbolId));

        final int symbol = cmd.symbol;

        // if there are other positions same currency - calculating free margin for them
        long freeMarginOtherSymbols = 0L;
        for (final SymbolPortfolioRecord portfolioRecord : userProfile.portfolio) {
            if (portfolioRecord.symbol != symbol) {
                final CoreSymbolSpecification spec2 = symbolSpecificationProvider.getSymbolSpecification(portfolioRecord.symbol);

                if (spec2.quoteCurrency == spec.quoteCurrency) {
                    // add P&L subtract margin
                    freeMarginOtherSymbols += portfolioRecord.estimateProfit(spec2, lastPriceCache.get(spec2.symbolId));
                    freeMarginOtherSymbols -= portfolioRecord.calculateRequiredDepositForFutures(spec2);
                }
            }
        }

        // extra deposit is required
        // check if current balance and margin can cover new deposit
        final long availableFunds = userProfile.accounts.get(portfolio.currency)
                + estimatedSymbolProfit
                + freeMarginOtherSymbols;

        return newRequiredDepositForSymbol <= availableFunds;
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
                final long amountInCounterCurrency = ev.price * size * spec.quoteScaleK;
                final long amountInBaseCurrency = size * spec.baseScaleK;

                // TODO add commission
                final long takerFee = spec.takerFee * size;
                final long makerFee = spec.makerFee * size;

                if (processTaker) {
                    // release taker's portfolio
                    final UserProfile taker = userProfileService.getUserProfileOrThrowEx(ev.activeOrderUid);
                    final SymbolPortfolioRecord takerSpr = taker.getPortfolioRecordOrThrowEx(ev.symbol);
                    takerSpr.pendingRelease(ev.activeOrderAction, size);

                    if (ev.activeOrderAction == OrderAction.ASK) {
                        // taker is selling
                        taker.accounts.addToValue(spec.quoteCurrency, amountInCounterCurrency);
                        taker.accounts.addToValue(spec.baseCurrency, -amountInBaseCurrency - takerFee);
                    } else {
                        // taker is buying
                        taker.accounts.addToValue(spec.quoteCurrency, -amountInCounterCurrency);
                        taker.accounts.addToValue(spec.baseCurrency, amountInBaseCurrency - takerFee);
                    }

                    taker.removeRecordIfEmpty(takerSpr);
                }

                if (processMaker) {
                    // release maker's portfolio
                    final UserProfile maker = userProfileService.getUserProfileOrThrowEx(ev.matchedOrderUid);
                    final SymbolPortfolioRecord makerSpr = maker.getPortfolioRecordOrThrowEx(ev.symbol);
                    makerSpr.pendingRelease(ev.activeOrderAction.opposite(), size);

                    if (ev.activeOrderAction == OrderAction.ASK) {
                        // maker is buying
                        maker.accounts.addToValue(spec.baseCurrency, amountInBaseCurrency - makerFee);
                        maker.accounts.addToValue(spec.quoteCurrency, -amountInCounterCurrency);
                    } else {
                        // maker is selling
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
                final SymbolPortfolioRecord spr = up.getPortfolioRecordOrThrowEx(ev.symbol);
                spr.pendingRelease(ev.activeOrderAction, size);
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

    @AllArgsConstructor
    @Getter
    public class State {
        private final SymbolSpecificationProvider symbolSpecificationProvider;
        private final UserProfileService userProfileService;
        private final BinaryCommandsProcessor binaryCommandsProcessor;
        private final IntObjectHashMap<LastPriceCacheRecord> lastPriceCache;
    }
}
