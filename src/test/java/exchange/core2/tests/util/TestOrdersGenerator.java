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
package exchange.core2.tests.util;

import exchange.core2.core.common.*;
import exchange.core2.core.common.api.ApiCancelOrder;
import exchange.core2.core.common.api.ApiCommand;
import exchange.core2.core.common.api.ApiMoveOrder;
import exchange.core2.core.common.api.ApiPlaceOrder;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.orderbook.IOrderBook;
import exchange.core2.core.orderbook.OrderBookNaiveImpl;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.distribution.ParetoDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongConsumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import static exchange.core2.tests.util.TestConstants.SYMBOLSPEC_EUR_USD;

@Slf4j
public final class TestOrdersGenerator {


    // TODO randomize scales
    public static final int CENTRAL_PRICE = 100_000;
    public static final int MIN_PRICE = 50_000;
    public static final int MAX_PRICE = 150_000;
    public static final int PRICE_DEVIATION_DEFAULT = 5_000;

    public static final double CENTRAL_MOVE_ALPHA = 0.01;

    public static final int CHECK_ORDERBOOK_STAT_EVERY_NTH_COMMAND = 256;

    public static final UnaryOperator<Integer> UID_PLAIN_MAPPER = i -> i + 1;

    // TODO allow limiting max volume

    // TODO allow limiting number of opened positions (currently it just grows)

    public static MultiSymbolGenResult generateMultipleSymbols(final TestOrdersGeneratorConfig config) {

        final List<CoreSymbolSpecification> coreSymbolSpecifications = config.coreSymbolSpecifications;
        final int totalTransactionsNumber = config.totalTransactionsNumber;
        final List<BitSet> usersAccounts = config.usersAccounts;
        final int targetOrderBookOrdersTotal = config.targetOrderBookOrdersTotal;
        final int seed = config.seed;

        final long timeGenStart = System.currentTimeMillis();

        final double[] distribution = createWeightedDistribution(coreSymbolSpecifications.size(), seed);
        int quotaLeft = totalTransactionsNumber;
        final Map<Integer, CompletableFuture<GenResult>> futures = new HashMap<>();

        final LongConsumer sharedProgressLogger = createAsyncProgressLogger(totalTransactionsNumber);

        for (int i = 0; i < coreSymbolSpecifications.size(); i++) {
            final CoreSymbolSpecification spec = coreSymbolSpecifications.get(i);
            final int numOrdersTarget = (int) (targetOrderBookOrdersTotal * distribution[i]);
            final int commandsNum = (i != coreSymbolSpecifications.size() - 1) ? (int) (totalTransactionsNumber * distribution[i]) : quotaLeft;
            quotaLeft -= commandsNum;
            //log.debug("{}. Generating symbol {} : commands={} numOrdersTarget={}", i, symbolId, commandsNum, numOrdersTarget);
            futures.put(spec.symbolId, CompletableFuture.supplyAsync(() -> {
                final int[] uidsAvailableForSymbol = UserCurrencyAccountsGenerator.createUserListForSymbol(usersAccounts, spec, commandsNum);
                final int numUsers = uidsAvailableForSymbol.length;
                final UnaryOperator<Integer> uidMapper = idx -> uidsAvailableForSymbol[idx];
                return generateCommands(commandsNum, numOrdersTarget, numUsers, uidMapper, spec.symbolId, false, config.hugeSizeIOC, sharedProgressLogger, seed);
            }));
        }

        final Map<Integer, GenResult> genResults = new HashMap<>();
        futures.forEach((symbol, future) -> {
            try {
                genResults.put(symbol, future.get());
            } catch (InterruptedException | ExecutionException ex) {
                throw new IllegalStateException("Exception while generating commands for symbol " + symbol, ex);
            }
        });

        log.debug("All test commands generated in {}s", String.format("%.3f", (System.currentTimeMillis() - timeGenStart) / 1000.0));

        final List<List<OrderCommand>> commandsLists = genResults.values().stream()
                .map(genResult -> genResult.commands)
                .collect(Collectors.toList());

        log.debug("Merging {} commands for {} symbols ...",
                commandsLists.stream().mapToInt(Collection::size).sum(), genResults.size());

        final List<OrderCommand> allCommands = RandomCollectionsMerger.mergeCollections(commandsLists, 1L);

        final int readyAtSeq = config.preFillMode.calculateReadySeqFunc.apply(config);

        printStatistics(readyAtSeq, allCommands);

        final List<ApiCommand> apiCommandsFill = TestOrdersGenerator.convertToApiCommand(allCommands, 0, readyAtSeq);
        final List<ApiCommand> apiCommandsBenchmark = TestOrdersGenerator.convertToApiCommand(allCommands, readyAtSeq, allCommands.size());

        return MultiSymbolGenResult.builder()
                .genResults(genResults)
                .apiCommandsBenchmark(apiCommandsBenchmark)
                .apiCommandsFill(apiCommandsFill)
                .build();
    }

    public static double[] createWeightedDistribution(int size, int seed) {
        final RealDistribution paretoDistribution = new ParetoDistribution(new JDKRandomGenerator(seed), 0.001, 1.0);
        final double[] paretoRaw = DoubleStream.generate(paretoDistribution::sample).limit(size).toArray();

        // normalize
        final double sum = Arrays.stream(paretoRaw).sum();
        double[] doubles = Arrays.stream(paretoRaw).map(x -> x / sum).toArray();
//        Arrays.stream(doubles).sorted().forEach(d -> log.debug("{}", d));
        return doubles;
    }

    @NotNull
    public static LongConsumer createAsyncProgressLogger(int totalTransactionsNumber) {
        final long progressLogInterval = 5_000_000_000L; // 5 sec
        final AtomicLong nextUpdateTime = new AtomicLong(System.nanoTime() + progressLogInterval);
        final LongAdder progress = new LongAdder();
        return transactions -> {
            progress.add(transactions);
            final long whenLogNext = nextUpdateTime.get();
            final long timeNow = System.nanoTime();
            if (timeNow > whenLogNext) {
                if (nextUpdateTime.compareAndSet(whenLogNext, timeNow + progressLogInterval)) {
                    // whichever thread won - it should print progress
                    final long done = progress.sum();
                    log.debug(String.format("Generating commands: %d of %d (%.01f%% done)...",
                            done, totalTransactionsNumber, done * 100.0 / totalTransactionsNumber));
                }
            }
        };
    }

    public static GenResult generateCommands(
            final int transactionsNumber,
            final int targetOrderBookOrders,
            final int numUsers,
            final UnaryOperator<Integer> uidMapper,
            final int symbol,
            final boolean enableSlidingPrice,
            final boolean hugeSizeIOC,
            final LongConsumer asyncProgressConsumer,
            final int seed) {

        // TODO specify symbol type (for testing exchange-bid-move rejects)
        final IOrderBook orderBook = new OrderBookNaiveImpl(SYMBOLSPEC_EUR_USD);

        final TestOrdersGeneratorSession session = new TestOrdersGeneratorSession(
                orderBook,
                targetOrderBookOrders,
                PRICE_DEVIATION_DEFAULT,
                hugeSizeIOC,
                numUsers,
                uidMapper,
                symbol,
                CENTRAL_PRICE,
                enableSlidingPrice,
                seed);

        final List<OrderCommand> commands = new ArrayList<>();

        int successfulCommands = 0;

        int nextSizeCheck = CHECK_ORDERBOOK_STAT_EVERY_NTH_COMMAND;

        int lastProgress = 0;

        for (int i = 0; i < transactionsNumber; i++) {
            OrderCommand cmd = generateRandomOrder(session);
            if (cmd == null) {
                continue;
            }

            cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
            cmd.symbol = session.symbol;
            //log.debug("{}. {}",i, cmd);

            if (IOrderBook.processCommand(orderBook, cmd) == CommandResultCode.SUCCESS) {
                successfulCommands++;
            }

            cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
            commands.add(cmd);

            // process and cleanup matcher events
            cmd.processMatcherEvents(ev -> matcherTradeEventEventHandler(session, ev, (int) cmd.orderId));
            cmd.matcherEvent = null;

            if (i >= nextSizeCheck) {
                nextSizeCheck += CHECK_ORDERBOOK_STAT_EVERY_NTH_COMMAND;

                updateOrderBookSizeStat(session);

                // TODO report less often
                asyncProgressConsumer.accept(i - lastProgress);
                lastProgress = i;

            }
        }

        asyncProgressConsumer.accept(transactionsNumber - lastProgress);

        updateOrderBookSizeStat(session);

//        assertThat("Orderbook was not filled for target rate " + session.targetOrderBookOrders, session.orderbooksFilledAtSequence, greaterThan(0L)); // check targetOrdersFilled
//        final int commandsListSize = commands.size() - (int) session.orderbooksFilledAtSequence;
//        log.debug("total commands: {}, post-fill commands: {}", commands.size(), commandsListSize);

//        log.debug("completed:{} rejected:{} cancel:{}", session.numCompleted, session.numRejected, session.numCancelled);
//
//        log.debug("place limit: {} ({}%)", session.counterPlaceLimit, (float) session.counterPlaceLimit / (float) commandsListSize * 100.0f);
//        log.debug("place market: {} ({}%)", session.counterPlaceMarket, (float) session.counterPlaceMarket / (float) commandsListSize * 100.0f);
//        log.debug("cancel: {} ({}%)", session.counterCancel, (float) session.counterCancel / (float) commandsListSize * 100.0f);
//        log.debug("move: {} ({}%)", session.counterMove, (float) session.counterMove / (float) commandsListSize * 100.0f);


//        final float succPerc = (float) successfulCommands / (float) commands.size() * 100.0f;
//        final float avgOrderBookSizeAsk = (float) session.orderBookSizeAskStat.stream().mapToInt(x -> x).average().orElse(0);
//        final float avgOrderBookSizeBid = (float) session.orderBookSizeBidStat.stream().mapToInt(x -> x).average().orElse(0);
//        final float avgOrdersNumInOrderBook = (float) session.orderBookNumOrdersStat.stream().mapToInt(x -> x).average().orElse(0);

//        assertThat(succPerc, greaterThan(85.0f));
//        assertThat(avgOrderBookSizeAsk, greaterThan(Math.min(100, session.targetOrderBookOrders / 20f - 1)));
//        assertThat(avgOrderBookSizeBid, greaterThan(Math.min(100, session.targetOrderBookOrders / 20f - 1)));
//        assertThat(avgOrdersNumInOrderBook, greaterThan(session.targetOrderBookOrders / 2f - 1));

//        log.debug("Average order book size: ASK={} BID={} ({} samples)", avgOrderBookSizeAsk, avgOrderBookSizeBid, session.orderBookSizeBidStat.size());
//        log.debug("Average limit orders number in the order book:{} (target:{})", avgOrdersNumInOrderBook, targetOrderBookOrders);
//        log.debug("Commands success={}%", succPerc);

        final L2MarketData l2MarketData = orderBook.getL2MarketDataSnapshot(Integer.MAX_VALUE);

        //log.info("gen: {}", LatencyTools.createLatencyReportFast(session.hdrRecorder.getIntervalHistogram()));

        return GenResult.builder().commands(commands)
                .finalOrderbookHash(orderBook.stateHash())
                .finalOrderBookSnapshot(l2MarketData)
                .build();
    }

    private static void updateOrderBookSizeStat(TestOrdersGeneratorSession session) {

        final int ordersNumAsk = session.orderBook.getOrdersNum(OrderAction.ASK);
        final int ordersNumBid = session.orderBook.getOrdersNum(OrderAction.BID);
        // regulating OB size
        session.lastOrderBookOrdersSizeAsk = ordersNumAsk;
        session.lastOrderBookOrdersSizeBid = ordersNumBid;
//        log.debug("ordersNum:{}", ordersNum);

        if (session.initialOrdersPlaced || session.hugeSizeIOC) {
            final L2MarketData l2MarketDataSnapshot = session.orderBook.getL2MarketDataSnapshot(Integer.MAX_VALUE);
//                log.debug("{}", dumpOrderBook(l2MarketDataSnapshot));

            if (session.hugeSizeIOC) {
                session.lastTotalVolumeAsk = l2MarketDataSnapshot.totalOrderBookVolumeAsk();
                session.lastTotalVolumeBid = l2MarketDataSnapshot.totalOrderBookVolumeBid();
            }

            if (session.initialOrdersPlaced) {
                session.orderBookSizeAskStat.add(l2MarketDataSnapshot.askSize);
                session.orderBookSizeBidStat.add(l2MarketDataSnapshot.bidSize);
                session.orderBookNumOrdersAskStat.add(ordersNumAsk);
                session.orderBookNumOrdersBidStat.add(ordersNumBid);
            }
        }
    }

    private static void matcherTradeEventEventHandler(TestOrdersGeneratorSession session, MatcherTradeEvent ev, int activeOrderId) {
        if (ev.eventType == MatcherEventType.TRADE) {
            if (ev.activeOrderCompleted) {
                session.orderUids.remove(activeOrderId);
                session.numCompleted++;
            }
            if (ev.matchedOrderCompleted) {
                session.orderUids.remove((int) ev.matchedOrderId);
                session.numCompleted++;
            }

            session.lastTradePrice = Math.min(MAX_PRICE, Math.max(MIN_PRICE, ev.price));

            if (ev.price <= MIN_PRICE) {
                session.priceDirection = 1;
            } else if (ev.price >= MAX_PRICE) {
                session.priceDirection = -1;
            }

        } else if (ev.eventType == MatcherEventType.REJECTION) {
            session.orderUids.remove(activeOrderId);
            session.numRejected++;

            // update order book stat if order get rejected
            // that will trigger generator to issue more limit orders
            updateOrderBookSizeStat(session);

        } else if (ev.eventType == MatcherEventType.CANCEL) {
            // full cancel - forget about this order
            session.orderUids.remove(activeOrderId);
            session.numCancelled++;
        }
    }


    private static OrderCommand generateRandomOrder(TestOrdersGeneratorSession session) {

        final Random rand = session.rand;

        // TODO move to lastOrderBookOrdersSize writer method
        final int lackOfOrdersAsk = session.targetOrderBookOrders - session.lastOrderBookOrdersSizeAsk;
        final int lackOfOrdersBid = session.targetOrderBookOrders - session.lastOrderBookOrdersSizeBid;
        if (!session.initialOrdersPlaced && lackOfOrdersAsk <= 0 && lackOfOrdersBid <= 0) {
            session.initialOrdersPlaced = true;

            session.counterPlaceMarket = 0;
            session.counterPlaceLimit = 0;
            session.counterCancel = 0;
            session.counterMove = 0;
        }

        final OrderAction action = (rand.nextInt(4) + session.priceDirection >= 2)
                ? OrderAction.BID
                : OrderAction.ASK;

        final int lackOfOrders = (action == OrderAction.ASK) ? lackOfOrdersAsk : lackOfOrdersBid;

        final boolean requireFastFill = lackOfOrders > (CHECK_ORDERBOOK_STAT_EVERY_NTH_COMMAND / 2);

        final boolean growOrders = lackOfOrders > 0;

        final int cmd = rand.nextInt(growOrders
                ? (requireFastFill ? 2 : 10)
                : 40);

        if (cmd < 2) {

            final int uid = session.uidMapper.apply(rand.nextInt(session.numUsers));

            final OrderCommand placeCmd = OrderCommand.builder()
                    .command(OrderCommandType.PLACE_ORDER)
                    .uid(uid)
                    .orderId(session.seq)
                    .action(action)
                    .build();

            if (growOrders) {
                placeCmd.orderType = OrderType.GTC;
                placeCmd.size = 1 + rand.nextInt(6) * rand.nextInt(6) * rand.nextInt(6);

                final int dev = 1 + (int) (Math.pow(rand.nextDouble(), 2) * session.priceDeviation);

                long p = 0;
                final int x = 4;
                for (int i = 0; i < x; i++) {
                    p += rand.nextInt(dev);
                }
                p = p / x * 2 - dev;
                if (p > 0 ^ action == OrderAction.ASK) {
                    p = -p;
                }

                //log.debug("p={} action={}", p, action);
                final int price = (int) session.lastTradePrice + (int) p;

                session.orderPrices.put(session.seq, price);
                session.orderUids.put(session.seq, uid);
                placeCmd.price = price;
                placeCmd.reserveBidPrice = action == OrderAction.BID ? MAX_PRICE : 0; // set limit price
                session.counterPlaceLimit++;
            } else {

                placeCmd.orderType = OrderType.IOC;

                if (session.hugeSizeIOC) {

                    final long availableVolume = action == OrderAction.ASK ? session.lastTotalVolumeAsk : session.lastTotalVolumeBid;

                    long bigRand = rand.nextLong();
                    bigRand = bigRand < 0 ? -1 - bigRand : bigRand;
                    placeCmd.size = 1 + bigRand % (availableVolume + 1);

                    if (action == OrderAction.ASK) {
                        session.lastTotalVolumeAsk = Math.max(session.lastTotalVolumeAsk - placeCmd.size, 0);
                    } else {
                        session.lastTotalVolumeBid = Math.max(session.lastTotalVolumeAsk - placeCmd.size, 0);
                    }
//                    log.debug("huge size={} at {}", placeCmd.size, session.seq);

                } else {
                    placeCmd.size = 1 + rand.nextInt(6) * rand.nextInt(6) * rand.nextInt(6);
                }
                placeCmd.price = action == OrderAction.BID ? MAX_PRICE : MIN_PRICE;
                placeCmd.reserveBidPrice = action == OrderAction.BID ? placeCmd.price : 0; // set limit price
                session.counterPlaceMarket++;
            }

            session.seq++;

            return placeCmd;
        }

        // TODO improve random picking performance (custom hashset implementation?)
//        long t = System.nanoTime();
        int size = Math.min(session.orderUids.size(), 512);
        if (size == 0) {
            return null;
        }

        int randPos = rand.nextInt(size);
        Iterator<Map.Entry<Integer, Integer>> iterator = session.orderUids.entrySet().iterator();

        Map.Entry<Integer, Integer> rec = iterator.next();
        for (int i = 0; i < randPos; i++) {
            rec = iterator.next();
        }
//        session.hdrRecorder.recordValue(Math.min(System.nanoTime() - t, Integer.MAX_VALUE));
        int orderId = rec.getKey();

        long uid = rec.getValue();
        if (uid == 0) {
            return null;
        }

        if (cmd == 2) {
            session.orderUids.remove(orderId);
            session.counterCancel++;
            return OrderCommand.cancel(orderId, (int) (long) uid);

        } else {

            int prevPrice = session.orderPrices.get(orderId);
            if (prevPrice == 0) {
                return null;
            }

            double priceMove = (session.lastTradePrice - prevPrice) * CENTRAL_MOVE_ALPHA;
            int priceMoveRounded;
            if (prevPrice > session.lastTradePrice) {
                priceMoveRounded = (int) Math.floor(priceMove);
            } else if (prevPrice < session.lastTradePrice) {
                priceMoveRounded = (int) Math.ceil(priceMove);
            } else {
                priceMoveRounded = rand.nextInt(2) * 2 - 1;
            }

            int newPrice = prevPrice + priceMoveRounded;

            // log.debug("session.seq={} orderId={} size={} p={}", session.seq, orderId, session.actualOrders.size(), priceMoveRounded);

            session.counterMove++;

            session.orderPrices.put(orderId, newPrice);

            return OrderCommand.update(orderId, (int) (long) uid, newPrice);
        }
    }


    public static List<ApiCommand> convertToApiCommand(List<OrderCommand> commands) {
        return convertToApiCommand(commands, 0, commands.size());
    }

    public static List<ApiCommand> convertToApiCommand(List<OrderCommand> commands, int from, int to) {
        return commands.stream()
                .skip(from)
                .limit(to - from)
                .map(cmd -> {
                    switch (cmd.command) {
                        case PLACE_ORDER:
                            return ApiPlaceOrder.builder().symbol(cmd.symbol).uid(cmd.uid).orderId(cmd.orderId).price(cmd.price).size(cmd.size).action(cmd.action).orderType(cmd.orderType)
                                    .reservePrice(cmd.reserveBidPrice)
                                    .build();
                        case MOVE_ORDER:
                            return ApiMoveOrder.builder().symbol(cmd.symbol).uid(cmd.uid).orderId(cmd.orderId).newPrice(cmd.price).build();
                        case CANCEL_ORDER:
                            return ApiCancelOrder.builder().symbol(cmd.symbol).uid(cmd.uid).orderId(cmd.orderId).build();
                    }
                    throw new IllegalStateException("unsupported type: " + cmd.command);
                })
                .collect(Collectors.toCollection(() -> new ArrayList<>(to - from)));
    }

    private static void printStatistics(final int readyAtSequenceApproximate, final List<OrderCommand> allCommands) {
        final int commandsListSize = allCommands.size() - readyAtSequenceApproximate;
        final Map<OrderCommandType, List<OrderCommand>> commandsByType = allCommands.stream().skip(readyAtSequenceApproximate).collect(Collectors.groupingBy(r -> r.command));
        final Map<OrderType, List<OrderCommand>> ordersByType = commandsByType.get(OrderCommandType.PLACE_ORDER).stream().collect(Collectors.groupingBy(cmd -> cmd.orderType));
        final int counterPlaceGTC = ordersByType.get(OrderType.GTC).size();
        final int counterPlaceIOC = ordersByType.get(OrderType.IOC).size();
        final int counterCancel = commandsByType.get(OrderCommandType.CANCEL_ORDER).size();
        final int counterMove = commandsByType.get(OrderCommandType.MOVE_ORDER).size();

        final String commandsGtc = String.format("%d (%.2f%%)", counterPlaceGTC, (float) counterPlaceGTC / (float) commandsListSize * 100.0f);
        final String commandsIoc = String.format("%d (%.2f%%)", counterPlaceIOC, (float) counterPlaceIOC / (float) commandsListSize * 100.0f);
        final String commandsCancel = String.format("%d (%.2f%%)", counterCancel, (float) counterCancel / (float) commandsListSize * 100.0f);
        final String commandsMove = String.format("%d (%.2f%%)", counterMove, (float) counterMove / (float) commandsListSize * 100.0f);

        log.info("new GTC: {}; new IOC: {}; cancel: {}; move: {}", commandsGtc, commandsIoc, commandsCancel, commandsMove);

        final Map<Integer, Long> perSymbols = allCommands.stream().skip(readyAtSequenceApproximate).collect(Collectors.groupingBy(cmd -> cmd.symbol, Collectors.counting()));
        final LongSummaryStatistics symbolStat = perSymbols.values().stream().collect(Collectors.summarizingLong(n -> n));
        final String cpsMax = String.format("%d (%.2f%%)", symbolStat.getMax(), symbolStat.getMax() * 100.0f / commandsListSize);
        final String cpsAvg = String.format("%d (%.2f%%)", (int) symbolStat.getAverage(), symbolStat.getAverage() * 100.0f / commandsListSize);
        final String cpsMin = String.format("%d (%.2f%%)", symbolStat.getMin(), symbolStat.getMin() * 100.0f / commandsListSize);
        log.info("commands per symbol: max:{}; avg:{}; min:{}", cpsMax, cpsAvg, cpsMin);
    }

    @Builder
    @Getter
    public static class GenResult {
        final private L2MarketData finalOrderBookSnapshot;
        final private int finalOrderbookHash;
        final private List<OrderCommand> commands;
    }

    @Builder
    @Getter
    public static class MultiSymbolGenResult {
        final Map<Integer, TestOrdersGenerator.GenResult> genResults;
        final List<ApiCommand> apiCommandsFill;
        final List<ApiCommand> apiCommandsBenchmark;
    }
}
