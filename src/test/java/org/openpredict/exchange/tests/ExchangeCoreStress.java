package org.openpredict.exchange.tests;

import lombok.extern.slf4j.Slf4j;
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
import java.util.function.Consumer;

import static org.openpredict.exchange.util.LatencyTools.LATENCY_RESOLUTION;
import static org.openpredict.exchange.util.LatencyTools.createLatencyReportFast;

@RunWith(SpringRunner.class)
@SpringBootTest
@ComponentScan(basePackages = {
        "org.openpredict.exchange",
})
@TestPropertySource(locations = "classpath:it.properties")
@Slf4j
public class ExchangeCoreStress {

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

    @Before
    public void before() {
        SymbolSpecification spec = SymbolSpecification.builder().depositBuy(22000).depositSell(32100).symbolId(SYMBOL).symbolName("XBTC").build();
        symbolSpecificationProvider.registerSymbol(SYMBOL, spec);
        matchingEngineRouter.addOrderBook(SYMBOL);
    }

    @Test
    public void throughputTest() throws Exception {

        int numOrders = 3_000_000;
        int targetOrderBookOrders = 1000;

        int numUsers = 1000;
        ArrayList<Long> uid = new ArrayList<>();
        for (long i = 1; i <= numUsers; i++) {
            apiCore.submitCommand(ApiAddUser.builder().uid(i).build());
            apiCore.submitCommand(ApiAdjustUserBalance.builder().uid(i).amount(2_000_000_000L).build());
            uid.add(i);
        }

        exchangeCore.setResultsConsumer(resultEvent -> {
//            log.debug("RESULT {}", resultEvent);
        });

        List<OrderCommand> orderCommands = generator.generateCommands(numOrders, targetOrderBookOrders, uid);
        List<ApiCommand> apiCommands = generator.convertToApiCommand(orderCommands, SYMBOL);

        List<Float> perfResults = new ArrayList<>();
        for (int j = 0; j < 100; j++) {
            Thread.sleep(20);
            userProfileService.reset();
            matchingEngineRouter.reset();
            System.gc();
            Thread.sleep(200);

            long t = System.currentTimeMillis();
            for (ApiCommand cmd : apiCommands) {
                apiCore.submitCommand(cmd);
            }
            t = System.currentTimeMillis() - t;

            float perfMt = (float) apiCommands.size() / (float) t / 1000.0f;
            log.info("{}. {} MT/s", j, perfMt);
            perfResults.add(perfMt);
        }

        double avg = (float) perfResults.stream().mapToDouble(x -> x).average().orElse(0);
        log.info("Average: {} MT/s", avg);

    }


    @Test
    public void latencyTest() throws Exception {

        int numOrders = 3_000_000;
        int targetOrderBookOrders = 1000;
        int numUsers = 1000;

//        int targetTps = 1000000; // transactions per second
        int targetTps = 75_000; // transactions per second
        int targetTpsEnd = 7_000_000;
//        int targetTps = 4_000_000; // transactions per second

        ArrayList<Long> uids = new ArrayList<>();
        for (long i = 1; i <= numUsers; i++) {
            apiCore.submitCommand(ApiAddUser.builder().uid(i).build());
            apiCore.submitCommand(ApiAdjustUserBalance.builder().uid(i).amount(2_000_000_000L).build());
            uids.add(i);
        }

        List<OrderCommand> orderCommands = generator.generateCommands(numOrders, targetOrderBookOrders, uids);
        List<ApiCommand> apiCommands = generator.convertToApiCommand(orderCommands, SYMBOL);

        IntLongHashMap latencies = new IntLongHashMap(20000);

        exchangeCore.setResultsConsumer(cmd -> {
            int key = (int) ((System.nanoTime() - cmd.timestamp) >> LATENCY_RESOLUTION);
            latencies.updateValue(key, 0, x -> x + 1);
        });

        // TODO - first run should validate the output (orders are accepted and processed properly)

        for (int j = 0; j < 10000; j++) {

            int nanosPerCmd = 1_000_000_000 / targetTps;
            targetTps += 25_000;

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