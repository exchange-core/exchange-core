package exchange.core2.core.processors;

import exchange.core2.core.ExchangeApi;
import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.common.MatcherEventType;
import exchange.core2.core.common.MatcherTradeEvent;
import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.PositionDirection;
import exchange.core2.core.common.SymbolPositionRecord;
import exchange.core2.core.common.SymbolType;
import exchange.core2.core.common.UserProfile;
import exchange.core2.core.common.api.ApiLiquidationOrder;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.processors.RiskEngine.LastPriceCacheRecord;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Liquidation scanner for checking user profiles and triggering liquidations.
 */
@Slf4j
public final class LiquidationScanner {

    private final ExchangeApi api;
    private final Collection<RiskEngine> riskEngines;

    private final ScheduledExecutorService scheduler;

    public LiquidationScanner(ExchangeApi api, Collection<RiskEngine> riskEngines) {
        this.api = api;
        this.riskEngines = riskEngines;
        this.scheduler = Executors.newScheduledThreadPool(riskEngines.size(), r -> new Thread(r, "LiquidationScanner"));
    }

    public void start() {
        for (RiskEngine riskEngine : riskEngines) {
            scheduler.scheduleWithFixedDelay(() -> {
                log.info("Checking liquidation for shard {}", riskEngine.getShardId());
                try {
                    checkLiquidations(riskEngine);
                } catch (Throwable e) {
                    log.error("Error during liquidation check for shard {}", riskEngine.getShardId(), e);
                }
            }, 2, 2, TimeUnit.SECONDS);
        }
    }

    public void stop(long timeout, TimeUnit timeUnit) {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(timeout, timeUnit)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
    }

    public void triggerOnce() {
        for (RiskEngine riskEngine : riskEngines) {
            try {
                log.info("Manual trigger: Checking liquidation for shard {}", riskEngine.getShardId());
                checkLiquidations(riskEngine);
            } catch (Throwable e) {
                log.error("Manual trigger failed for shard {}", riskEngine.getShardId(), e);
            }
        }
    }

    private void checkLiquidations(RiskEngine riskEngine) {
        SymbolSpecificationProvider symbolSpecificationProvider = riskEngine.getSymbolSpecificationProvider();
        MutableLongObjectMap<UserProfile> userProfiles = riskEngine.getUserProfileService().getUserProfiles().asUnmodifiable();
        MutableIntObjectMap<LastPriceCacheRecord> lastPriceCache = riskEngine.getLastPriceCache().asUnmodifiable();
        userProfiles.forEachValue(userProfile -> {
            MutableIntObjectMap<SymbolPositionRecord> positions = userProfile.positions.asUnmodifiable();
            positions.forEachValue(position -> {
                int symbol = position.symbol;
                CoreSymbolSpecification spec = symbolSpecificationProvider.getSymbolSpecification(symbol);
                if (spec.type != SymbolType.FUTURES_CONTRACT) {
                    return;
                }
                LastPriceCacheRecord priceRecord = lastPriceCache.get(symbol);
                if (priceRecord == null) {
                    log.warn("No price record for symbol={}", symbol);
                    return;
                }
                evaluateForLiquidation(userProfile, spec, priceRecord, position);
            });
        });
    }

    private void evaluateForLiquidation(UserProfile userProfile,
                                        CoreSymbolSpecification spec, LastPriceCacheRecord priceRecord,
                                        SymbolPositionRecord position) {
        if (position != null && position.direction != PositionDirection.EMPTY) {
            long balance = userProfile.accounts.get(spec.quoteCurrency);
            long profit = position.liquidateEstimateProfit(spec, priceRecord);
            long equity = balance + profit;
            long maintenanceMargin = position.calculateMaintenanceMargin(spec);
            long warningThreshold = (long) (maintenanceMargin * 1.2);
            if (equity < maintenanceMargin) {
                long deficit = maintenanceMargin - equity;
                long price = position.direction == PositionDirection.LONG ? priceRecord.bidPrice : priceRecord.askPrice;
                if (price == 0 || price == Long.MAX_VALUE) {
                    price = position.openVolume > 0 ? position.openPriceSum / position.openVolume : priceRecord.markPrice;
                    if (price == 0) price = 1;
                    log.debug("Fallback to average open price={} for symbol={}", price, position.symbol);
                }
                /**
                 * calc sizeToLiquidate(x)
                 *
                 * find an x, where x × price ≥ deficit + x × taker_fee
                 * x ≥ deficit / (price - taker_fee)
                 */
                long x = (long) Math.ceil((double) deficit / (price - spec.takerFee));
                long sizeToLiquidate = Math.min(position.openVolume, x);
                if (sizeToLiquidate > 0) {
                    OrderAction action = position.direction == PositionDirection.LONG ? OrderAction.ASK : OrderAction.BID;
                    CompletableFuture<OrderCommand> liquidationFuture = api.submitCommandAsyncFullResponse(ApiLiquidationOrder.builder()
                            .orderType(OrderType.IOC)
                            .orderId(generateLiquidationOrderId(position.symbol, position.uid))
                            .uid(position.uid)
                            .symbol(position.symbol)
                            .price(price)
                            .size(sizeToLiquidate)
                            .action(action).build());
                    liquidationFuture.whenCompleteAsync((cmd, err) -> {
                        MatcherTradeEvent firstEvent = cmd.matcherEvent;
                        if (firstEvent.eventType == MatcherEventType.REJECT) {
                            long remainSize = firstEvent.size;
                            // todo need downgrade to IFC, ADL, etc.
                        }
                    });
                    log.debug("Liquidated: uid={} symbol={} size={} price={}", userProfile.uid, position.symbol, sizeToLiquidate, price);
                }
            }
            else if (equity < warningThreshold) {
                log.debug("Margin call: uid={} symbol={} equity={} threshold={}", userProfile.uid, position.symbol, equity, warningThreshold);
            }
        }
    }

    // IOC order, order will not insert in order book, so orderId doesn't matter that much.
    private long generateLiquidationOrderId(int symbolId, long uid) {
        long uidHash = (uid * 31 + 17) & 0xFFFFF;
        long tsPart = (System.currentTimeMillis() / 1000) & 0xFFF;
        return  ((long) symbolId << 32) | (uidHash << 12) | tsPart;
    }
}