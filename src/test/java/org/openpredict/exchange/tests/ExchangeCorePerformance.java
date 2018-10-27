package org.openpredict.exchange.tests;

import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openpredict.exchange.beans.SymbolSpecification;
import org.openpredict.exchange.beans.api.ApiAddUser;
import org.openpredict.exchange.beans.api.ApiAdjustUserBalance;
import org.openpredict.exchange.beans.api.ApiCommand;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.core.*;
import org.openpredict.exchange.tests.util.TestOrdersGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.openpredict.exchange.util.LatencyTools.LATENCY_RESOLUTION;
import static org.openpredict.exchange.util.LatencyTools.createLatencyReportFast;

@RunWith(SpringRunner.class)
@SpringBootTest
@ComponentScan(basePackages = {
        "org.openpredict.exchange",
})
@TestPropertySource(locations = "classpath:it.properties")
@Slf4j
public class ExchangeCorePerformance {

    @Autowired
    private ExchangeApi apiCore;

    @Autowired
    private ExchangeCore exchangeCore;

    @Autowired
    private PortfolioService portfolioService;

    @Autowired
    private UserProfileService userProfileService;

    @Autowired
    private MatchingEngineRouter matchingEngineRouter;

    @Autowired
    private RiskEngine riskEngine;

    @Autowired
    private SymbolSpecificationProvider symbolSpecificationProvider;

    @MockBean
    private Consumer<OrderCommand> resultsConsumerMock;


    private TestOrdersGenerator generator = new TestOrdersGenerator();

    private static final int SYMBOL = 5991;

    private int latencyMessagesCounter = 0;

    @Before
    public void before() {
        SymbolSpecification spec = SymbolSpecification.builder().depositBuy(22000).depositSell(32100).symbolId(SYMBOL).symbolName("XBTC").build();
        symbolSpecificationProvider.registerSymbol(SYMBOL, spec);
        matchingEngineRouter.addOrderBook(SYMBOL);
    }

    // TODO shutdown disruptor if test fails
    @Test
    public void throughputTest() throws Exception {

        int numOrders = 3_000_000;
        int targetOrderBookOrders = 1000;

        int numUsers = 1000;

        try (AffinityLock cpuLock = AffinityLock.acquireCore()) {

            List<Long> uids = Stream.iterate(1L, i -> i + 1).limit(numUsers).collect(Collectors.toList());
            uids.forEach(uid -> {
                apiCore.submitCommand(ApiAddUser.builder().uid(uid).build());
                apiCore.submitCommand(ApiAdjustUserBalance.builder().uid(uid).amount(2_000_000_000L).build());
            });

            TestOrdersGenerator.GenResult genResult = generator.generateCommands(numOrders, targetOrderBookOrders, uids, SYMBOL, false);
            List<ApiCommand> apiCommands = generator.convertToApiCommand(genResult.getCommands());

            AtomicInteger counter = new AtomicInteger();

            exchangeCore.setResultsConsumer(resultEvent -> {
//            log.debug("RESULT {}", resultEvent);
                counter.decrementAndGet();
            });

            List<Float> perfResults = new ArrayList<>();
            for (int j = 0; j < 100; j++) {
                Thread.sleep(20);
                userProfileService.reset();
                matchingEngineRouter.reset();
                System.gc();
                Thread.sleep(200);
                counter.set(apiCommands.size());

                long t = System.currentTimeMillis();
                for (ApiCommand cmd : apiCommands) {
                    apiCore.submitCommand(cmd);
                }

                while (counter.get() > 0) {
                    // spin until all commands have processed
                }

                t = System.currentTimeMillis() - t;
                float perfMt = (float) apiCommands.size() / (float) t / 1000.0f;
                log.info("{}. {} MT/s", j, perfMt);
                perfResults.add(perfMt);

                //matchingEngineRouter.getOrderBook().printFullOrderBook();

                // weak compare orderBook final state just to make sure all commands executed same way
                // TODO compare events
                assertThat(matchingEngineRouter.getOrderBook().hashCode(), is(genResult.getFinalOrderbookHash()));

            }

            double avg = (float) perfResults.stream().mapToDouble(x -> x).average().orElse(0);
            log.info("Average: {} MT/s", avg);
        }
    }


    @Test
    public void latencyTest() throws Exception {

        final int numOrders = 3_000_000;
        final int targetOrderBookOrders = 1000;
        final int numUsers = 1000;

        final int ignoreLatencyForFirstNMessages = numOrders / 100;

//        int targetTps = 1000000; // transactions per second
        final int targetTps = 100_000; // transactions per second
        final int targetTpsEnd = 8_000_000;
//        int targetTps = 4_000_000; // transactions per second

        try (AffinityLock cpuLock = AffinityLock.acquireCore()) {

            List<Long> uids = Stream.iterate(1L, i -> i + 1).limit(numUsers).collect(Collectors.toList());
            uids.forEach(uid -> {
                apiCore.submitCommand(ApiAddUser.builder().uid(uid).build());
                apiCore.submitCommand(ApiAdjustUserBalance.builder().uid(uid).amount(2_000_000_000L).build());
            });

            TestOrdersGenerator.GenResult genResult = generator.generateCommands(
                    numOrders,
                    targetOrderBookOrders,
                    uids,
                    SYMBOL,
                    false);
            List<ApiCommand> apiCommands = generator.convertToApiCommand(genResult.getCommands());

            IntLongHashMap latencies = new IntLongHashMap(20000);

            exchangeCore.setResultsConsumer(cmd -> {
                int key = (int) ((System.nanoTime() - cmd.timestamp) >> LATENCY_RESOLUTION);
                latencies.updateValue(key, 0, x -> x + 1);
                // reset latency map after first 1% messages
                if (latencyMessagesCounter++ == ignoreLatencyForFirstNMessages) {
                    latencies.clear();
                }
            });

            // TODO - first run should validate the output (orders are accepted and processed properly)


            IntConsumer testIteration = tps -> {
                try {
                    Thread.sleep(20);
                    userProfileService.reset();
                    matchingEngineRouter.reset();
                    System.gc();
                    Thread.sleep(200);

                    latencies.clear();
                    latencyMessagesCounter = 0;

                    final int nanosPerCmd = (1_000_000_000 / tps);
                    final long startTimeMs = System.currentTimeMillis();

                    long plannedTimestamp = System.nanoTime();

                    for (ApiCommand cmd : apiCommands) {
                        while (System.nanoTime() < plannedTimestamp) {
                            // spin while too early for sending next message
                        }
                        cmd.timestamp = plannedTimestamp;
                        apiCore.submitCommand(cmd);
                        plannedTimestamp += nanosPerCmd;
                    }

                    // wait until last response received
                    final long expectedSum = apiCommands.size() - ignoreLatencyForFirstNMessages - 1;
                    while (latencies.sum() != expectedSum) {
                        //log.debug("commands not processed yet: {}", expectedSum - sum);
                    }
                    final long processingTimeMs = System.currentTimeMillis() - startTimeMs;

                    float perfMt = (float) apiCommands.size() / (float) processingTimeMs / 1000.0f;
                    log.info("{} MT/s {}", perfMt, createLatencyReportFast(latencies));

                    // weak compare orderBook final state just to make sure all commands executed same way
                    // TODO compare events
                    assertThat(matchingEngineRouter.getOrderBook().hashCode(), is(genResult.getFinalOrderbookHash()));

                } catch (InterruptedException e) {
                    //
                }

            };


            // warm-up
            IntStream.range(0, 20)
                    .forEach(i -> testIteration.accept(1_000_000));

            IntStream.range(0, 10000)
                    .map(i -> targetTps + 25000 * i)
                    .filter(tps -> tps <= targetTpsEnd)
                    .forEach(testIteration);
        }
    }


    // TODO fix - fails with - int DEFAULT_HOT_WIDTH = 1024;
    @Test
    public void latencyTestFailing() throws Exception {

        int numOrders = 3_000_000;
        int targetOrderBookOrders = 1000;
        int numUsers = 1000;

//        int targetTps = 1000000; // transactions per second
        int targetTps = 500_000; // transactions per second
        int targetTpsEnd = 7_000_000;
//        int targetTps = 4_000_000; // transactions per second

        List<Long> uids = Stream.iterate(1L, i -> i + 1).limit(numUsers).collect(Collectors.toList());
        uids.forEach(uid -> {
            apiCore.submitCommand(ApiAddUser.builder().uid(uid).build());
            apiCore.submitCommand(ApiAdjustUserBalance.builder().uid(uid).amount(2_000_000_000L).build());
        });

        TestOrdersGenerator.GenResult genResult = generator.generateCommands(numOrders, targetOrderBookOrders, uids, SYMBOL, true);
        List<ApiCommand> apiCommands = generator.convertToApiCommand(genResult.getCommands());

        IntLongHashMap latencies = new IntLongHashMap(20000);

        exchangeCore.setResultsConsumer(cmd -> {
            int key = (int) ((System.nanoTime() - cmd.timestamp) >> LATENCY_RESOLUTION);
            latencies.updateValue(key, 0, x -> x + 1);
        });

        // TODO - first run should validate the output (orders are accepted and processed properly)

        for (int j = 0; j < 10000; j++) {

            int nanosPerCmd = 1_000_000_000 / targetTps;
            targetTps += 50_000;

            Thread.sleep(20);
            userProfileService.reset();
            matchingEngineRouter.reset();
            System.gc();
            Thread.sleep(200);

            long t = System.currentTimeMillis();
            long plannedTimestamp = System.nanoTime();

            long latenciesSum = latencies.sum();

            for (ApiCommand cmd : apiCommands) {
                // calculate planned time and spin if too early
                while (System.nanoTime() < plannedTimestamp) {
                }
                cmd.timestamp = plannedTimestamp;
                apiCore.submitCommand(cmd);
                plannedTimestamp += nanosPerCmd;
            }

            // wait until last response received
            long expectedSum = latenciesSum + apiCommands.size();
            while (latencies.sum() != expectedSum) {
                //log.debug("commands not processed yet: {}", expectedSum - sum);
            }
            t = System.currentTimeMillis() - t;

            float perfMt = (float) apiCommands.size() / (float) t / 1000.0f;
            Map<String, String> fmtPlace = createLatencyReportFast(latencies);
            log.info("{}. {} MT/s {}", j, perfMt, fmtPlace);

            // weak compare orderBook final state just to make sure all commands executed same way
            // TODO compare events
            assertThat(matchingEngineRouter.getOrderBook().hashCode(), is(genResult.getFinalOrderbookHash()));

//            if (j == 5) {
//                log.info("Warmup completed, RESET latency stat");
            latencies.clear();
//            }

            if (targetTps > targetTpsEnd) {
                break;
            }
        }
    }


}