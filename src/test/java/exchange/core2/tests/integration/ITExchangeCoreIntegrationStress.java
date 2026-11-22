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
package exchange.core2.tests.integration;

import exchange.core2.core.ExchangeApi;
import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.common.L2MarketData;
import exchange.core2.core.common.api.ApiCommand;
import exchange.core2.core.common.config.PerformanceConfiguration;
import exchange.core2.tests.util.ExchangeTestContainer;
import exchange.core2.tests.util.TestOrdersGenerator;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static exchange.core2.tests.util.TestConstants.SYMBOLSPEC_ETH_XBT;
import static exchange.core2.tests.util.TestConstants.SYMBOLSPEC_EUR_USD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Runs a single-symbol stress scenario with 1,000 users, 1,000 prefill orders and 1 million
 * subsequent commands. Checks the final L2 book, book depth and total client balances by currency.
 */
public abstract class ITExchangeCoreIntegrationStress {

    private static final Logger log = LoggerFactory.getLogger(ITExchangeCoreIntegrationStress.class);

    // configuration provided by child class
    public abstract PerformanceConfiguration getPerformanceConfiguration();

    @Test
    @Timeout(60)
    public void manyOperationsMargin() throws Exception {

        manyOperations(SYMBOLSPEC_EUR_USD);
    }

    @Test
    @Timeout(60)
    public void manyOperationsExchange() throws Exception {

        manyOperations(SYMBOLSPEC_ETH_XBT);
    }

    public void manyOperations(final CoreSymbolSpecification symbolSpec) throws Exception {
        try (final ExchangeTestContainer container = ExchangeTestContainer.create(getPerformanceConfiguration())) {
            container.initBasicSymbols();
            //container.initBasicUsers();
            final ExchangeApi api = container.getApi();

            int numOrders = 1_000_000;
            int targetOrderBookOrders = 1000;
            int numUsers = 1000;

            log.debug("Generating commands...");
            final TestOrdersGenerator.GenResult genResult = TestOrdersGenerator.generateCommands(
                    numOrders,
                    targetOrderBookOrders,
                    numUsers,
                    TestOrdersGenerator.UID_PLAIN_MAPPER,
                    symbolSpec.getSymbolId(),
                    false,
                    false,
                    TestOrdersGenerator.createAsyncProgressLogger(numOrders),
                    288379917);

            final List<ApiCommand> apiCommands = TestOrdersGenerator.convertToApiCommand(genResult);

            final Set<Integer> allowedCurrencies = Stream.of(symbolSpec.quoteCurrency, symbolSpec.baseCurrency).collect(Collectors.toSet());

            log.debug("Users init ...");
            container.usersInit(numUsers, allowedCurrencies);

            // validate total balance as a sum of loaded funds
            final Consumer<IntLongHashMap> balancesValidator = balances -> allowedCurrencies.forEach(
                    cur -> assertThat(balances.get(cur), is(10_0000_0000L * numUsers)));


            log.debug("Verifying balances...");
            balancesValidator.accept(container.totalBalanceReport().getClientsBalancesSum());

            log.debug("Running benchmark...");
            final CountDownLatch ordersLatch = new CountDownLatch(apiCommands.size());
            container.setConsumer((cmd, seq) -> ordersLatch.countDown());
            for (ApiCommand cmd : apiCommands) {
                cmd.timestamp = System.currentTimeMillis();
                api.submitCommand(cmd);
            }
            ordersLatch.await();

            // Check the final L2 snapshot and depth after all command results arrive.
            // TODO compare matcher events.
            final L2MarketData l2MarketData = container.requestCurrentOrderBook(symbolSpec.getSymbolId());
            assertEquals(genResult.getFinalOrderBookSnapshot(), l2MarketData);
            assertThat(l2MarketData.askSize, greaterThan(10));
            assertThat(l2MarketData.bidSize, greaterThan(10));

            // verify that total balance was not changed
            balancesValidator.accept(container.totalBalanceReport().getClientsBalancesSum());
        }
    }

}
