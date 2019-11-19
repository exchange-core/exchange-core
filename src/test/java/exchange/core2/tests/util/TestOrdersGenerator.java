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
import exchange.core2.core.utils.UnsafeUtils;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongConsumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import static exchange.core2.tests.util.TestConstants.SYMBOLSPEC_EUR_USD;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

@Slf4j
public final class TestOrdersGenerator {


    // TODO randomize scales
    public static final int CENTRAL_PRICE = 100_000;
    public static final int MIN_PRICE = 50_000;
    public static final int MAX_PRICE = 150_000;
    public static final int PRICE_DEVIATION_DEFAULT = 5_000;

    public static final double CENTRAL_MOVE_ALPHA = 0.01;

    public static final int CHECK_ORDERBOOK_STAT_EVERY_NTH_COMMAND = 256;

    public static final int GENERATION_THREADS = 6;

    public static final UnaryOperator<Integer> UID_PLAIN_MAPPER = i -> i + 1;

    // TODO allow limiting max volume


    public static MultiSymbolGenResult generateMultipleSymbols(final List<CoreSymbolSpecification> coreSymbolSpecifications,
                                                               final int totalTransactionsNumber,
                                                               final List<BitSet> usersAccounts,
                                                               final int targetOrderBookOrdersTotal) {

        final ExecutorService executor = Executors.newFixedThreadPool(
                GENERATION_THREADS,
                UnsafeUtils.affinedThreadFactory(UnsafeUtils.ThreadAffinityMode.THREAD_AFFINITY_DISABLE));

        final double[] distribution = createWeightedDistribution(coreSymbolSpecifications.size());
        int quotaLeft = totalTransactionsNumber;
        final Map<Integer, CompletableFuture<GenResult>> futures = new HashMap<>();

        final LongConsumer sharedProgressLogger = createAsyncProgressLogger(totalTransactionsNumber);

        //int[] stat = new int[4];
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
                return generateCommands(commandsNum, numOrdersTarget, numUsers, uidMapper, spec.symbolId, false, sharedProgressLogger);
            }, executor));

            //stat[symbolId%4] += numOrdersTarget;
        }

        //log.debug("stat: {}", stat);

        final Map<Integer, GenResult> genResults = new HashMap<>();
        futures.forEach((symbol, future) -> {
            try {
                genResults.put(symbol, future.get());
            } catch (InterruptedException | ExecutionException ex) {
                throw new IllegalStateException("Exception while generating commands for symbol " + symbol, ex);
            }
        });

        executor.shutdown();

        final int readyAtSequenceApproximate = genResults.values().stream().mapToInt(TestOrdersGenerator.GenResult::getOrderbooksFilledAtSequence).sum();
        log.debug("readyAtSequenceApproximate={}", readyAtSequenceApproximate);

        final List<OrderCommand> allCommands = TestOrdersGenerator.mergeCommands(genResults.values());

        printStatistics(readyAtSequenceApproximate, allCommands);

        final List<ApiCommand> apiCommandsFill = TestOrdersGenerator.convertToApiCommand(allCommands, 0, readyAtSequenceApproximate);
        final List<ApiCommand> apiCommandsBenchmark = TestOrdersGenerator.convertToApiCommand(allCommands, readyAtSequenceApproximate, allCommands.size());

        return MultiSymbolGenResult.builder()
                .genResults(genResults)
                .apiCommandsBenchmark(apiCommandsBenchmark)
                .apiCommandsFill(apiCommandsFill)
                .build();
    }

    public static double[] createWeightedDistribution(int size) {
        final RealDistribution paretoDistribution = new ParetoDistribution(new JDKRandomGenerator(0), 0.001, 1.6);
        final double[] paretoRaw = DoubleStream.generate(paretoDistribution::sample).limit(size).toArray();
        //Arrays.sort(paretoRaw);
        //ArrayUtils.reverse(paretoRaw);
        final double sum = Arrays.stream(paretoRaw).sum();
        return Arrays.stream(paretoRaw).map(x -> x / sum).toArray();
    }

    @NotNull
    public static LongConsumer createAsyncProgressLogger(int totalTransactionsNumber) {
        final long progressLogInterval = 3_000_000_000L; // 3 sec
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
                    log.debug("{} ({}% done)", done, (done * 100L / totalTransactionsNumber));
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
            final LongConsumer asyncProgressConsumer) {

        // TODO specify symbol type
        final IOrderBook orderBook = new OrderBookNaiveImpl(SYMBOLSPEC_EUR_USD);

        final TestOrdersGeneratorSession session = new TestOrdersGeneratorSession(
                orderBook,
                targetOrderBookOrders,
                PRICE_DEVIATION_DEFAULT,
                numUsers,
                uidMapper,
                symbol,
                CENTRAL_PRICE,
                enableSlidingPrice);

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
            cmd.processMatcherEvents(ev -> matcherTradeEventEventHandler(session, ev));
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

        // if transactionsNumber is too small - assume order books filled
        if (session.orderbooksFilledAtSequence == 0 && transactionsNumber < 10000) {
            session.orderbooksFilledAtSequence = 1;
        }

        updateOrderBookSizeStat(session);

        assertThat("Orderbook was not filled for target rate " + session.targetOrderBookOrders, session.orderbooksFilledAtSequence, greaterThan(0L)); // check targetOrdersFilled
        final int commandsListSize = commands.size() - (int) session.orderbooksFilledAtSequence;
//        log.debug("total commands: {}, post-fill commands: {}", commands.size(), commandsListSize);

//        log.debug("completed:{} rejected:{} cancel:{}", session.numCompleted, session.numRejected, session.numCancelled);
//
//        log.debug("place limit: {} ({}%)", session.counterPlaceLimit, (float) session.counterPlaceLimit / (float) commandsListSize * 100.0f);
//        log.debug("place market: {} ({}%)", session.counterPlaceMarket, (float) session.counterPlaceMarket / (float) commandsListSize * 100.0f);
//        log.debug("cancel: {} ({}%)", session.counterCancel, (float) session.counterCancel / (float) commandsListSize * 100.0f);
//        log.debug("move: {} ({}%)", session.counterMove, (float) session.counterMove / (float) commandsListSize * 100.0f);


        final float succPerc = (float) successfulCommands / (float) commands.size() * 100.0f;
        final float avgOrderBookSizeAsk = (float) session.orderBookSizeAskStat.stream().mapToInt(x -> x).average().orElse(0);
        final float avgOrderBookSizeBid = (float) session.orderBookSizeBidStat.stream().mapToInt(x -> x).average().orElse(0);
        final float avgOrdersNumInOrderBook = (float) session.orderBookNumOrdersStat.stream().mapToInt(x -> x).average().orElse(0);

//        assertThat(succPerc, greaterThan(85.0f));
//        assertThat(avgOrderBookSizeAsk, greaterThan(Math.min(100, session.targetOrderBookOrders / 20f - 1)));
//        assertThat(avgOrderBookSizeBid, greaterThan(Math.min(100, session.targetOrderBookOrders / 20f - 1)));
//        assertThat(avgOrdersNumInOrderBook, greaterThan(session.targetOrderBookOrders / 2f - 1));

//        log.debug("Average order book size: ASK={} BID={} ({} samples)", avgOrderBookSizeAsk, avgOrderBookSizeBid, session.orderBookSizeBidStat.size());
//        log.debug("Average limit orders number in the order book:{} (target:{})", avgOrdersNumInOrderBook, targetOrderBookOrders);
//        log.debug("Commands success={}%", succPerc);

        final L2MarketData l2MarketData = orderBook.getL2MarketDataSnapshot(-1);

        //log.info("gen: {}", LatencyTools.createLatencyReportFast(session.hdrRecorder.getIntervalHistogram()));

        return GenResult.builder().commands(commands)
                .finalOrderbookHash(orderBook.stateHash())
                .finalOrderBookSnapshot(l2MarketData)
                .orderbooksFilledAtSequence((int) session.orderbooksFilledAtSequence)
                .build();
    }

    private static void updateOrderBookSizeStat(TestOrdersGeneratorSession session) {
        L2MarketData l2MarketDataSnapshot = session.orderBook.getL2MarketDataSnapshot(-1);
//                log.debug("{}", dumpOrderBook(l2MarketDataSnapshot));

        int ordersNum = session.orderBook.getOrdersNum();
        // regulating OB size
        session.lastOrderBookOrdersSize = ordersNum;

//        log.debug("ordersNum:{}", ordersNum);

        if (session.orderbooksFilledAtSequence > 0) {
            session.orderBookSizeAskStat.add(l2MarketDataSnapshot.askSize);
            session.orderBookSizeBidStat.add(l2MarketDataSnapshot.bidSize);
            session.orderBookNumOrdersStat.add(ordersNum);
        }
    }

    private static void matcherTradeEventEventHandler(TestOrdersGeneratorSession session, MatcherTradeEvent ev) {
        if (ev.eventType == MatcherEventType.TRADE) {
            if (ev.activeOrderCompleted) {
                session.orderUids.remove((int) ev.activeOrderId);
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
            session.orderUids.remove((int) ev.activeOrderId);
            session.numRejected++;

            // update order book stat if order get rejected
            // that will trigger generator to issue more limit orders
            updateOrderBookSizeStat(session);

        } else if (ev.eventType == MatcherEventType.CANCEL) {
            // full cancel - forget about this order
            session.orderUids.remove((int) ev.activeOrderId);
            session.numCancelled++;
        }
    }


    private static OrderCommand generateRandomOrder(TestOrdersGeneratorSession session) {

        Random rand = session.rand;

        // TODO move to lastOrderBookOrdersSize writer method
        int lackOfOrders = session.targetOrderBookOrders - session.lastOrderBookOrdersSize;
        boolean growOrders = lackOfOrders > 0;
        if (session.orderbooksFilledAtSequence == 0 && lackOfOrders <= 0) {
            session.orderbooksFilledAtSequence = session.seq;

            session.counterPlaceMarket = 0;
            session.counterPlaceLimit = 0;
            session.counterCancel = 0;
            session.counterMove = 0;
        }

        int cmd = rand.nextInt(growOrders ? (lackOfOrders > (CHECK_ORDERBOOK_STAT_EVERY_NTH_COMMAND / 2) ? 2 : 10) : 40);

        if (cmd < 2) {

            OrderAction action = (rand.nextInt(4) + session.priceDirection >= 2)
                    ? OrderAction.BID
                    : OrderAction.ASK;

            long size = 1 + rand.nextInt(6) * rand.nextInt(6) * rand.nextInt(6);

            OrderType orderType = growOrders ? OrderType.GTC : OrderType.IOC;
            final int uid = session.uidMapper.apply(rand.nextInt(session.numUsers));

            OrderCommand placeCmd = OrderCommand.builder().command(OrderCommandType.PLACE_ORDER).uid(uid).orderId(session.seq).size(size)
                    .action(action).orderType(orderType).build();

            if (orderType == OrderType.GTC) {

                int dev = 1 + (int) (Math.pow(rand.nextDouble(), 2) * session.priceDeviation);

                long p = 0;
                int x = 4;
                for (int i = 0; i < x; i++) {
                    p += rand.nextInt(dev);
                }
                p = p / x * 2 - dev;
                if (p > 0 ^ action == OrderAction.ASK) {
                    p = -p;
                }

                //log.debug("p={} action={}", p, action);
                int price = (int) session.lastTradePrice + (int) p;

                session.orderPrices.put(session.seq, price);
                session.orderUids.put(session.seq, uid);
                placeCmd.price = price;
                placeCmd.reserveBidPrice = action == OrderAction.BID ? MAX_PRICE : 0; // set limit price
                session.counterPlaceLimit++;
            } else {
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
                            return ApiPlaceOrder.builder().symbol(cmd.symbol).uid(cmd.uid).id(cmd.orderId).price(cmd.price).size(cmd.size).action(cmd.action).orderType(cmd.orderType)
                                    .reservePrice(cmd.reserveBidPrice)
                                    .build();
                        case MOVE_ORDER:
                            return ApiMoveOrder.builder().symbol(cmd.symbol).uid(cmd.uid).id(cmd.orderId).newPrice(cmd.price).build();
                        case CANCEL_ORDER:
                            return ApiCancelOrder.builder().symbol(cmd.symbol).uid(cmd.uid).id(cmd.orderId).build();
                    }
                    throw new IllegalStateException("unsupported type: " + cmd.command);
                })
                .collect(Collectors.toList());
    }

    private static void printStatistics(final int readyAtSequenceApproximate, final List<OrderCommand> allCommands) {
        final int commandsListSize = allCommands.size() - readyAtSequenceApproximate;
        final Map<OrderCommandType, List<OrderCommand>> commandsByType = allCommands.stream().skip(readyAtSequenceApproximate).collect(Collectors.groupingBy(r -> r.command));
        final Map<OrderType, List<OrderCommand>> ordersByType = commandsByType.get(OrderCommandType.PLACE_ORDER).stream().collect(Collectors.groupingBy(cmd -> cmd.orderType));
        final int counterPlaceGTC = ordersByType.get(OrderType.GTC).size();
        final int counterPlaceIOC = ordersByType.get(OrderType.IOC).size();
        final int counterCancel = commandsByType.get(OrderCommandType.CANCEL_ORDER).size();
        final int counterMove = commandsByType.get(OrderCommandType.MOVE_ORDER).size();

        log.debug("new GTC: {} ({}%)", counterPlaceGTC, (float) counterPlaceGTC / (float) commandsListSize * 100.0f);
        log.debug("new IOC: {} ({}%)", counterPlaceIOC, (float) counterPlaceIOC / (float) commandsListSize * 100.0f);
        log.debug("cancel: {} ({}%)", counterCancel, (float) counterCancel / (float) commandsListSize * 100.0f);
        log.debug("move: {} ({}%)", counterMove, (float) counterMove / (float) commandsListSize * 100.0f);

        final Map<Integer, Long> perSymbols = allCommands.stream().skip(readyAtSequenceApproximate).collect(Collectors.groupingBy(cmd -> cmd.symbol, Collectors.counting()));
        final LongSummaryStatistics symbolStat = perSymbols.values().stream().collect(Collectors.summarizingLong(n -> n));
        log.debug("max commands per symbol: {} ({}%)", symbolStat.getMax(), (float) symbolStat.getMax() / (float) commandsListSize * 100.0f);
        log.debug("avg commands per symbol: {} ({}%)", (int) symbolStat.getAverage(), (float) symbolStat.getAverage() / (float) commandsListSize * 100.0f);
        log.debug("min commands per symbol: {} ({}%)", symbolStat.getMin(), (float) symbolStat.getMin() / (float) commandsListSize * 100.0f);
    }

    @Builder
    @Getter
    public static class GenResult {
        final private L2MarketData finalOrderBookSnapshot;
        final private int finalOrderbookHash;
        final private List<OrderCommand> commands;
        final private int orderbooksFilledAtSequence;
    }

    @Builder
    @Getter
    public static class MultiSymbolGenResult {
        final Map<Integer, TestOrdersGenerator.GenResult> genResults;
        final List<ApiCommand> apiCommandsFill;
        final List<ApiCommand> apiCommandsBenchmark;
    }


    public static List<OrderCommand> mergeCommands(final Collection<GenResult> genResultsCollection) {

        if (genResultsCollection.size() == 1) {
            return genResultsCollection.stream().findFirst().get().commands;
        }

        Random rand = new Random(1L);

        List<GenResult> genResults = new ArrayList<>(genResultsCollection);

        List<Integer> probabilityRanges = new ArrayList<>();
        List<Integer> leftCounters = new ArrayList<>();
        int totalCommands = 0;
        for (final GenResult genResult : genResults) {
            final int size = genResult.getCommands().size();
            leftCounters.add(size);
            totalCommands += size;
            probabilityRanges.add(totalCommands);
        }

        //log.debug("Merging {} commands for {} different symbols: probabilityRanges: {}", totalCommands, genResults.size(), probabilityRanges);
        log.debug("Merging {} commands for {} different symbols...", totalCommands, genResults.size());

        List<OrderCommand> res = new ArrayList<>(totalCommands);

        while (true) {

            int r = rand.nextInt(totalCommands);

            int pos = Collections.binarySearch(probabilityRanges, r);
            if (pos < 0) {
                pos = -1 - pos;
            }

            int left = leftCounters.get(pos);

            if (left > 0) {
                List<OrderCommand> commands = genResults.get(pos).getCommands();
                res.add(commands.get(commands.size() - left));
                leftCounters.set(pos, left - 1);
            } else {
                // todo remove/optimize
                if (res.size() == totalCommands) {
                    log.debug("Done merging");
                    return res;
                }
            }

        }

    }

}
