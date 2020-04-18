package exchange.core2.tests.nasdaq;

import com.paritytrading.foundation.ASCII;
import com.paritytrading.juncture.nasdaq.itch50.ITCH50;
import com.paritytrading.juncture.nasdaq.itch50.ITCH50Listener;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.map.mutable.primitive.ByteIntHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.HashMap;
import java.util.Map;


/*
VTI      8491 ls=100 mc=P etpF=Y etpL=0 SUCCESS
VTI     :
 AddOrder = 584748
 AddOrderMPID = 118
 OrderExecuted = 3265
 Trade = 244
 NOII = 451
 OrderCancel = 5589
 StockTradingAction = 1
 OrderDelete = 582044
 OrderExecutedWithPrice = 75
 CrossTrade = 2
 OrderReplace = 57000

WFC      8582 ls=100 mc=N etpF=N etpL=0 SUCCESS
WFC     :
 AddOrder = 390040
 AddOrderMPID = 147
 OrderExecuted = 10723
 Trade = 1341
 NOII = 451
 OrderCancel = 3684
 StockTradingAction = 1
 OrderDelete = 380922
 OrderExecutedWithPrice = 507
 CrossTrade = 2
 OrderReplace = 46049
 */

@Slf4j
@Getter
public class ITCH50StatListener implements ITCH50Listener {

    private final Map<Integer, StockStat> symbolStat = new HashMap<>();
    private final IntObjectHashMap<StockDescr> symbolDescr = new IntObjectHashMap<>();

    public void printStat() {
        symbolStat.forEach((k, v) -> {
            if (v.counter > 500000) {
                log.info("{} {}", symbolDescr.get(k), v);
            }
        });
    }

    @AllArgsConstructor
    public static class StockDescr {
        final String name;
        final byte etpFlag;
        final long etpLeverageFactor;

        @Override
        public String toString() {
            return name + " ETP=" + (char) etpFlag + " LF" + etpLeverageFactor;
        }
    }

    @RequiredArgsConstructor
    public static class StockStat {
        final int stockLocate;

        int minPrice = Integer.MAX_VALUE;
        int maxPrice = Integer.MIN_VALUE;
        int priceStep = 1_000_000_000;
        float priceAvg = Float.NaN;
        int counter = 0;
        boolean priceSample = false;
        ByteIntHashMap counters = new ByteIntHashMap();

        @Override
        public String toString() {

            final StringBuilder sb = new StringBuilder();
            counters.forEachKeyValue((k, v) -> sb.append((char) k).append(v).append(' '));

            return stockLocate + " p:" + minPrice + "-" + (int) priceAvg + "-" + maxPrice + " s:" + priceStep + " c:" + counter + " ca:" + sb;
        }
    }

    @Override
    public void systemEvent(ITCH50.SystemEvent message) {
        log.debug("{}", message);
    }

    private void updateStat(byte msgType, int stockLocate) {
        updateStat(msgType, stockLocate, Long.MIN_VALUE);
    }

    private void updateStat(byte msgType, int stockLocate, long longPrice) {

        final StockStat stockStat = symbolStat.computeIfAbsent(stockLocate, StockStat::new);
        stockStat.counter++;
        stockStat.counters.addToValue(msgType, 1);

        if (longPrice != Long.MIN_VALUE) {
            stockStat.priceSample = true;

            if (longPrice > 2_000_000_000) {
                throw new IllegalStateException("PRICE is above 2_000_000_000");
            }
            int msgPrice = (int) longPrice;


            while (stockStat.priceStep != 1 && msgPrice % stockStat.priceStep != 0) {
                stockStat.priceStep /= 10;
            }

            stockStat.minPrice = Math.min(stockStat.minPrice, msgPrice);
            stockStat.maxPrice = Math.max(stockStat.maxPrice, msgPrice);

            stockStat.priceAvg = Float.isNaN(stockStat.priceAvg)
                    ? msgPrice
                    : (stockStat.priceAvg * stockStat.counter + msgPrice) / (float) (stockStat.counter + 1);

        }
    }


    //----------------- init? ----------
    @Override
    public void stockDirectory(ITCH50.StockDirectory itchMsg) {
//                log.debug("{}", itchMsg);

        symbolDescr.put(itchMsg.stockLocate, new StockDescr(ASCII.unpackLong(itchMsg.stock), itchMsg.etpFlag, itchMsg.etpLeverageFactor));

//        CoreSymbolSpecification symbolSpecification = CoreSymbolSpecification.builder()
//                .symbolId(itchMsg.stockLocate)
//                .type(SymbolType.FUTURES_CONTRACT) // allow margin trade TODO calculate based on ETP
//                .baseCurrency(itchMsg.stockLocate + 10_000)
//                .quoteCurrency(TestConstants.CURRENECY_USD)
//                .takerFee(3)
//                .makerFee(1)
//                .quoteScaleK(2)
//                .baseScaleK(itchMsg.roundLotSize)
//                .marginBuy(itchMsg.etpLeverageFactor + 100L) // TODO price
//                .marginSell(itchMsg.etpLeverageFactor + 100L) // TODO price
//                .build();
//
//        final CompletableFuture<OrderCommand> response = api.submitBinaryCommandAsync(new BatchAddSymbolsCommand(symbolSpecification), itchMsg.stockLocate, Function.identity());
//
//        response.thenAccept(cmdRes -> {
//            if (cmdRes.resultCode != CommandResultCode.SUCCESS) {
//                log.error("symbol add failed: {}", response);
//            }
//            if (ASCII.unpackLong(itchMsg.stock).trim().equals("INTL")) {
//                log.debug("{} {} ls={} mc={} etpF={} etpL={} rlo={}",
//                        ASCII.unpackLong(itchMsg.stock),
//                        itchMsg.stockLocate,
//                        itchMsg.roundLotSize,
//                        (char) itchMsg.marketCategory,
//                        (char) itchMsg.etpFlag,
//                        itchMsg.etpLeverageFactor,
//                        (char) itchMsg.roundLotsOnly);
//            }
//        });
    }

    @Override
    public void stockTradingAction(ITCH50.StockTradingAction message) {
//                log.debug("{}", message);
        updateStat(ITCH50.MESSAGE_TYPE_STOCK_TRADING_ACTION, message.stockLocate);
    }

    @Override
    public void regSHORestriction(ITCH50.RegSHORestriction message) {
//                log.debug("{}", message);
        updateStat(ITCH50.MESSAGE_TYPE_REG_SHO_RESTRICTION, message.locateCode);
    }

    @Override
    public void marketParticipantPosition(ITCH50.MarketParticipantPosition message) {
//                log.debug("{}", message);
        updateStat(ITCH50.MESSAGE_TYPE_MARKET_PARTICIPANT_POSITION, message.stockLocate);
    }
    //-----------------------------------

    @Override
    public void mwcbDeclineLevel(ITCH50.MWCBDeclineLevel message) {
        log.debug("{}", message);
        updateStat(ITCH50.MESSAGE_TYPE_MWCB_DECLINE_LEVEL, message.stockLocate);
    }

    @Override
    public void mwcbStatus(ITCH50.MWCBStatus message) {
        log.debug("{}", message);
        updateStat(ITCH50.MESSAGE_TYPE_MWCB_STATUS, message.stockLocate);
        ;
    }

    @Override
    public void ipoQuotingPeriodUpdate(ITCH50.IPOQuotingPeriodUpdate message) {
        log.debug("{}", message);
        updateStat(ITCH50.MESSAGE_TYPE_IPO_QUOTING_PERIOD_UPDATE, message.stockLocate);
    }

    @Override
    public void luldAuctionCollar(ITCH50.LULDAuctionCollar message) {
        log.debug("{}", message);
        updateStat(ITCH50.MESSAGE_TYPE_LULD_AUCTION_COLLAR, message.stockLocate);
    }

    @Override
    public void operationalHalt(ITCH50.OperationalHalt message) {
        log.debug("{}", message);
        updateStat(ITCH50.MESSAGE_TYPE_OPERATIONAL_HALT, message.stockLocate);
    }

    @Override
    public void addOrder(ITCH50.AddOrder message) {


//                    validatePrice(msgPrice, message.stockLocate);
//
//        final int uid = hashToUid(message.orderReferenceNumber, numUsersMask);
//        long time = convertTime(message.timestampHigh, message.timestampLow);
//        //                log.debug("{}", message);
////                    if (message.stockLocate == 4193) { // INTL
////                        log.debug("ADD_ORDER p={} orderId={} uid={} shares={}", message.price, message.orderReferenceNumber, uid, message.shares);
////                        log.debug("ADD ORDER h={} l={} -> {} ", message.timestampHigh, message.timestampLow, convertTime(message.timestampHigh, message.timestampLow));
////                    }
//
//        if (message.stockLocate == 4193) { // INTL
//            api.placeNewOrder(
//                    0,
//                    0,
//                    time,
//                    message.orderReferenceNumber,
//                    message.trackingNumber,
//                    msgPrice,
//                    msgPrice + 100000,
//                    message.shares,
//                    message.buySellIndicator == ITCH50.SELL ? OrderAction.ASK : OrderAction.BID,
//                    OrderType.GTC,
//                    message.stockLocate,
//                    uid);
//        }


        if (message.stockLocate == 8893) {
            log.debug("ADD_ORDER      p={} orderId={} shares={} action={}", message.price, message.orderReferenceNumber, message.shares, (char) message.buySellIndicator);
        }

        updateStat(ITCH50.MESSAGE_TYPE_ADD_ORDER, message.stockLocate, message.price);
    }

    @Override
    public void addOrderMPID(ITCH50.AddOrderMPID message) {
        if (message.stockLocate == 8893) {
            log.debug("ADD_ORDER MPID p={} orderId={} shares={} action={}", message.price, message.orderReferenceNumber, message.shares, (char) message.buySellIndicator);
        }


//                log.debug("{}", message);
//        updateStat(ITCH50.MESSAGE_TYPE_ADD_ORDER_MPID, message.stockLocate, message.price);
        updateStat(ITCH50.MESSAGE_TYPE_ADD_ORDER_MPID, message.stockLocate);
    }

    @Override
    public void orderExecuted(ITCH50.OrderExecuted message) {
//                log.debug("{}", message);
        updateStat(ITCH50.MESSAGE_TYPE_ORDER_EXECUTED, message.stockLocate);
    }

    @Override
    public void orderExecutedWithPrice(ITCH50.OrderExecutedWithPrice message) {
//                if (Math.random() < 0.01) {
//                    log.debug("{} {} {}", message, message.timestampHigh, message.timestampLow);
//                }
//        updateStat(ITCH50.MESSAGE_TYPE_,message.stockLocate);
        if (message.stockLocate == 8893) {
            log.debug("ORDER_EXE_PRIC p={} orderId={} shares={}", message.executionPrice, message.orderReferenceNumber, message.executedShares);
        }
        updateStat(ITCH50.MESSAGE_TYPE_ORDER_EXECUTED_WITH_PRICE, message.stockLocate, message.executionPrice);
    }

    @Override
    public void orderCancel(ITCH50.OrderCancel message) {
//                log.debug("{}", message);
//                if (Math.random() < 0.0001) {
//                    log.debug("{} {} {}", message, message.timestampHigh, message.timestampLow);
//                }
        updateStat(ITCH50.MESSAGE_TYPE_ORDER_CANCEL, message.stockLocate);
    }

    @Override
    public void orderDelete(ITCH50.OrderDelete message) {
//                log.debug("{}", message);
        updateStat(ITCH50.MESSAGE_TYPE_ORDER_DELETE, message.stockLocate);
    }

    @Override
    public void orderReplace(ITCH50.OrderReplace message) {
//                log.debug("{}", message);
        updateStat(ITCH50.MESSAGE_TYPE_ORDER_REPLACE, message.stockLocate, message.price);
    }

    @Override
    public void trade(ITCH50.Trade message) {
//                log.debug("{}", message);
//        updateStat(ITCH50.MESSAGE_TYPE_TRADE, message.stockLocate, message.price);
        updateStat(ITCH50.MESSAGE_TYPE_TRADE, message.stockLocate);
    }

    @Override
    public void crossTrade(ITCH50.CrossTrade message) {
//                if (Math.random() < 0.01) {
//                    log.debug("{} {} {}", message, message.timestampHigh, message.timestampLow);
//                }
        updateStat(ITCH50.MESSAGE_TYPE_CROSS_TRADE, message.stockLocate);
//        updateStat(ITCH50.MESSAGE_TYPE_CROSS_TRADE, message.stockLocate, message.crossPrice);
    }

    @Override
    public void brokenTrade(ITCH50.BrokenTrade message) {
        log.debug("{}", message);
        updateStat(ITCH50.MESSAGE_TYPE_BROKEN_TRADE, message.stockLocate);
    }

    @Override
    public void noii(ITCH50.NOII message) {
//                if (Math.random() < 0.001) {
//                    log.debug("{} {} {}", message, message.timestampHigh, message.timestampLow);
//                }
        updateStat(ITCH50.MESSAGE_TYPE_NOII, message.stockLocate);
//        updateStat(ITCH50.MESSAGE_TYPE_,message.stockLocate, message.currentReferencePrice);
    }

    @Override
    public void rpii(ITCH50.RPII message) {
//                log.debug("{}", message);
        updateStat(ITCH50.MESSAGE_TYPE_RPII, message.stockLocate);
    }
}
