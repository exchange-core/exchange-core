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
import exchange.core2.core.common.*;
import exchange.core2.core.common.api.*;
import exchange.core2.tests.util.ExchangeTestContainer;
import exchange.core2.tests.util.TestOrdersGenerator;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static exchange.core2.core.common.OrderAction.ASK;
import static exchange.core2.core.common.OrderType.GTC;
import static exchange.core2.tests.util.ExchangeTestContainer.CHECK_SUCCESS;
import static exchange.core2.tests.util.TestConstants.*;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.Assert.*;

@Slf4j
public final class ITExchangeCoreIntegrationStress {

    @Test(timeout = 60_000)
    public void manyOperationsMargin() throws Exception {

        manyOperations(SYMBOLSPEC_EUR_USD);
    }

    @Test(timeout = 60_000)
    public void manyOperationsExchange() throws Exception {

        manyOperations(SYMBOLSPEC_ETH_XBT);
    }

    public void manyOperations(final CoreSymbolSpecification symbolSpec) throws Exception {
        try (final ExchangeTestContainer container = new ExchangeTestContainer()) {
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
                    TestOrdersGenerator.createAsyncProgressLogger(numOrders),
                    288379917);

            final List<ApiCommand> apiCommands = TestOrdersGenerator.convertToApiCommand(genResult.getCommands());

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
            container.setConsumer(cmd -> ordersLatch.countDown());
            for (ApiCommand cmd : apiCommands) {
                cmd.timestamp = System.currentTimeMillis();
                api.submitCommand(cmd);
            }
            ordersLatch.await();

            // compare orderBook final state just to make sure all commands executed same way
            // TODO compare events, wait until finish
            final L2MarketData l2MarketData = container.requestCurrentOrderBook(symbolSpec.getSymbolId());
            assertEquals(genResult.getFinalOrderBookSnapshot(), l2MarketData);
            assertThat(l2MarketData.askSize, greaterThan(10));
            assertThat(l2MarketData.bidSize, greaterThan(10));

            // verify that total balance was not changed
            balancesValidator.accept(container.totalBalanceReport().getClientsBalancesSum());
        }
    }

}