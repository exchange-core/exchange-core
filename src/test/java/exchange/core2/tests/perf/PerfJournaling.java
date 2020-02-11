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
import exchange.core2.tests.util.JournalingTestsModule;
import exchange.core2.tests.util.TestConstants;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public final class PerfJournaling {


    @Test
    public void testJournalingMargin() throws Exception {
        JournalingTestsModule.journalingTestImpl(
                initStateCfg -> new ExchangeTestContainer(64 * 1024, 1, 1, 512, initStateCfg, true),
                3_000_000,
                1000,
                2000,
                10,
                TestConstants.CURRENCIES_FUTURES,
                1,
                ExchangeTestContainer.AllowedSymbolTypes.FUTURES_CONTRACT);
    }


    @Test
    public void testJournalingMultiSymbolLarge() throws Exception {
        JournalingTestsModule.journalingTestImpl(
                initStateCfg -> new ExchangeTestContainer(32 * 1024, 4, 4, 1024, initStateCfg, true),
                10_000_000,
                4_000_000,
                10_000_000,
                25,
                TestConstants.ALL_CURRENCIES,
                100_000,
                ExchangeTestContainer.AllowedSymbolTypes.BOTH);
    }


    @Test
    public void testJournalingMultiSymbolHuge() throws Exception {
        JournalingTestsModule.journalingTestImpl(
                initStateCfg -> new ExchangeTestContainer(64 * 1024, 4, 4, 1024, initStateCfg, true),
                30_000_000,
                20_000_000,
                20_000_000,
                25,
                TestConstants.ALL_CURRENCIES,
                200_000,
                ExchangeTestContainer.AllowedSymbolTypes.BOTH);
    }

}