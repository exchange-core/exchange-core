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
package exchange.core2.tests.perf;

import exchange.core2.tests.util.ExchangeTestContainer;
import exchange.core2.tests.util.TestConstants;
import exchange.core2.tests.util.TestOrdersGenerator;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.junit.Test;
import exchange.core2.core.common.api.ApiCommand;

import java.time.Instant;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.IntFunction;
import java.util.stream.IntStream;


@Slf4j
public final class PerfHiccups {

    private long nextHiccupAcceptTimestampNs = 0;

    @Test
    public void testHiccups() {

        final int numOrders = 3_000_000;
        final int targetOrderBookOrders = 1000;
        final int numUsers = 2000;

        // will print each occurrence if latency>0.2ms
        final long hiccupThresholdNs = 200_000;

        final int targetTps = 500_000; // transactions per second

        try (final ExchangeTestContainer container = new ExchangeTestContainer(16 * 1024, 1, 1, 512, null);
             final AffinityLock cpuLock = AffinityLock.acquireLock()) {

            final TestOrdersGenerator.GenResult genResult = TestOrdersGenerator.generateCommands(
                    numOrders,
                    targetOrderBookOrders,
                    numUsers,
                    TestOrdersGenerator.UID_PLAIN_MAPPER,
                    TestConstants.SYMBOL_MARGIN,
                    false,
                    TestOrdersGenerator.createAsyncProgressLogger(numOrders));

            final List<ApiCommand> apiCommands = TestOrdersGenerator.convertToApiCommand(genResult.getCommands());

            IntFunction<TreeMap<Instant, Long>> testIteration = tps -> {
                try {

                    System.gc();
                    Thread.sleep(300);

                    container.initBasicSymbols();
                    container.usersInit(numUsers, TestConstants.CURRENCIES_FUTURES);

                    final LongLongHashMap hiccupTimestampsNs = new LongLongHashMap(10000);

                    final CountDownLatch latch = new CountDownLatch(apiCommands.size());
                    container.setConsumer(cmd -> {
                        long now = System.nanoTime();
                        // skip other messages in delayed group
                        if (now < nextHiccupAcceptTimestampNs) {
                            return;
                        }
                        long diffNs = now - cmd.timestamp;
                        // register hiccup timestamps
                        if (diffNs > hiccupThresholdNs) {
                            hiccupTimestampsNs.put(cmd.timestamp, diffNs);
                            nextHiccupAcceptTimestampNs = cmd.timestamp + diffNs;
                        }
                        latch.countDown();
                    });

                    nextHiccupAcceptTimestampNs = Long.MIN_VALUE;

                    final int nanosPerCmd = (1_000_000_000 / tps);
                    final long startTimeMs = System.currentTimeMillis();

                    final long startTimeNs = System.nanoTime();
                    long plannedTimestamp = startTimeNs;

                    for (ApiCommand cmd : apiCommands) {
                        long currentTimeNs;
                        while ((currentTimeNs = System.nanoTime()) < plannedTimestamp) {
                            // spin while too early for sending next message
                        }
                        // setting current timestamp (not planned) for catching original hiccups only (this is different from latency test)
                        cmd.timestamp = currentTimeNs;
                        container.api.submitCommand(cmd);
                        plannedTimestamp += nanosPerCmd;
                    }

                    final TreeMap<Instant, Long> sorted = new TreeMap<>();
                    // convert nanosecond timestamp into Instant
                    // not very precise, but for 1ms resolution is ok (0,05% accuracy is required)...
                    // delay (nanoseconds) merging as max value
                    hiccupTimestampsNs.forEachKeyValue((eventTimestampNs, delay) -> sorted.compute(
                            Instant.ofEpochMilli(startTimeMs + (eventTimestampNs - startTimeNs) / 1_000_000),
                            (k, d) -> d == null ? delay : Math.max(d, delay)));

                    container.resetExchangeCore();

                    return sorted;

                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }

            };

            log.debug("warming up...");
            IntStream.range(0, 2)
                    .forEach(i -> testIteration.apply(targetTps));
            log.debug("warmup done");

            IntStream.range(0, 2000)
                    .mapToObj(i -> testIteration.apply(targetTps))
                    .forEach(res -> {
                        if (res.isEmpty()) {
                            log.debug("no hiccups");
                        }
                        res.forEach((timestamp, delay) -> log.debug("{}: {}µs", timestamp, delay / 1000));
                    });
        }
    }


}