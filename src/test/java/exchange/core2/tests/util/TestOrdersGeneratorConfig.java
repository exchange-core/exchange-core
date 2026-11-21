/*
 * Copyright 2020 Maksim Zheravin
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

import exchange.core2.core.common.CoreSymbolSpecification;

import java.util.BitSet;
import java.util.List;
import java.util.function.Function;

public class TestOrdersGeneratorConfig {

    final List<CoreSymbolSpecification> coreSymbolSpecifications;
    final int totalTransactionsNumber;
    final List<BitSet> usersAccounts;
    final int targetOrderBookOrdersTotal;
    final int seed;
    final boolean avalancheIOC;
    final PreFillMode preFillMode;

    public enum PreFillMode {

        ORDERS_NUMBER(TestOrdersGeneratorConfig::getTargetOrderBookOrdersTotal),
        ORDERS_NUMBER_PLUS_QUARTER(config -> config.targetOrderBookOrdersTotal * 5 / 4); // used for snapshot tests to let some margin positions open

        final Function<TestOrdersGeneratorConfig, Integer> calculateReadySeqFunc;

        private PreFillMode(Function<TestOrdersGeneratorConfig, Integer> calculateReadySeqFunc) {
            this.calculateReadySeqFunc = calculateReadySeqFunc;
        }
    }

    public TestOrdersGeneratorConfig(
            List<CoreSymbolSpecification> coreSymbolSpecifications,
            int totalTransactionsNumber,
            List<BitSet> usersAccounts,
            int targetOrderBookOrdersTotal,
            int seed,
            boolean avalancheIOC,
            PreFillMode preFillMode) {
        this.coreSymbolSpecifications = coreSymbolSpecifications;
        this.totalTransactionsNumber = totalTransactionsNumber;
        this.usersAccounts = usersAccounts;
        this.targetOrderBookOrdersTotal = targetOrderBookOrdersTotal;
        this.seed = seed;
        this.avalancheIOC = avalancheIOC;
        this.preFillMode = preFillMode;
    }

    public static TestOrdersGeneratorConfigBuilder builder() {
        return new TestOrdersGeneratorConfigBuilder();
    }

    public List<CoreSymbolSpecification> getCoreSymbolSpecifications() {
        return this.coreSymbolSpecifications;
    }

    public int getTotalTransactionsNumber() {
        return this.totalTransactionsNumber;
    }

    public List<BitSet> getUsersAccounts() {
        return this.usersAccounts;
    }

    public int getTargetOrderBookOrdersTotal() {
        return this.targetOrderBookOrdersTotal;
    }

    public int getSeed() {
        return this.seed;
    }

    public boolean isAvalancheIOC() {
        return this.avalancheIOC;
    }

    public PreFillMode getPreFillMode() {
        return this.preFillMode;
    }

    public static class TestOrdersGeneratorConfigBuilder {
        private List<CoreSymbolSpecification> coreSymbolSpecifications;
        private int totalTransactionsNumber;
        private List<BitSet> usersAccounts;
        private int targetOrderBookOrdersTotal;
        private int seed;
        private boolean avalancheIOC;
        private PreFillMode preFillMode;

        TestOrdersGeneratorConfigBuilder() {
        }

        public TestOrdersGeneratorConfigBuilder coreSymbolSpecifications(List<CoreSymbolSpecification> coreSymbolSpecifications) {
            this.coreSymbolSpecifications = coreSymbolSpecifications;
            return this;
        }

        public TestOrdersGeneratorConfigBuilder totalTransactionsNumber(int totalTransactionsNumber) {
            this.totalTransactionsNumber = totalTransactionsNumber;
            return this;
        }

        public TestOrdersGeneratorConfigBuilder usersAccounts(List<BitSet> usersAccounts) {
            this.usersAccounts = usersAccounts;
            return this;
        }

        public TestOrdersGeneratorConfigBuilder targetOrderBookOrdersTotal(int targetOrderBookOrdersTotal) {
            this.targetOrderBookOrdersTotal = targetOrderBookOrdersTotal;
            return this;
        }

        public TestOrdersGeneratorConfigBuilder seed(int seed) {
            this.seed = seed;
            return this;
        }

        public TestOrdersGeneratorConfigBuilder avalancheIOC(boolean avalancheIOC) {
            this.avalancheIOC = avalancheIOC;
            return this;
        }

        public TestOrdersGeneratorConfigBuilder preFillMode(PreFillMode preFillMode) {
            this.preFillMode = preFillMode;
            return this;
        }

        public TestOrdersGeneratorConfig build() {
            return new TestOrdersGeneratorConfig(
                    this.coreSymbolSpecifications,
                    this.totalTransactionsNumber,
                    this.usersAccounts,
                    this.targetOrderBookOrdersTotal,
                    this.seed,
                    this.avalancheIOC,
                    this.preFillMode);
        }

        @Override
        public String toString() {
            return "TestOrdersGeneratorConfig.TestOrdersGeneratorConfigBuilder(coreSymbolSpecifications=" + this.coreSymbolSpecifications
                    + ", totalTransactionsNumber=" + this.totalTransactionsNumber
                    + ", usersAccounts=" + this.usersAccounts
                    + ", targetOrderBookOrdersTotal=" + this.targetOrderBookOrdersTotal
                    + ", seed=" + this.seed
                    + ", avalancheIOC=" + this.avalancheIOC
                    + ", preFillMode=" + this.preFillMode + ")";
        }
    }
}
