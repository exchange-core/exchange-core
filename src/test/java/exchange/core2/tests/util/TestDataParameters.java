package exchange.core2.tests.util;

import java.util.Objects;
import java.util.Set;

public class TestDataParameters {
    public final int totalTransactionsNumber;
    public final int targetOrderBookOrdersTotal;
    public final int numAccounts;
    public final Set<Integer> currenciesAllowed;
    public final int numSymbols;
    public final ExchangeTestContainer.AllowedSymbolTypes allowedSymbolTypes;
    public final TestOrdersGeneratorConfig.PreFillMode preFillMode;
    public final boolean avalancheIOC;

    public static TestDataParametersBuilder singlePairMarginBuilder() {
        return TestDataParameters.builder()
                .totalTransactionsNumber(3_000_000)
                .targetOrderBookOrdersTotal(1000)
                .numAccounts(2000)
                .currenciesAllowed(TestConstants.CURRENCIES_FUTURES)
                .numSymbols(1)
                .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.FUTURES_CONTRACT)
                .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER);
    }

    public static TestDataParametersBuilder singlePairExchangeBuilder() {
        return TestDataParameters.builder()
                .totalTransactionsNumber(3_000_000)
                .targetOrderBookOrdersTotal(1000)
                .numAccounts(2000)
                .currenciesAllowed(TestConstants.CURRENCIES_EXCHANGE)
                .numSymbols(1)
                .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.CURRENCY_EXCHANGE_PAIR)
                .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER);
    }

    /**
     * Mixed spot and futures workload with 10,000 symbols, about 1 million generated users
     * with 3.3 million currency accounts, a prefill target of 1 million orders and 3 million benchmark commands.
     *
     * @return medium exchange test data configuration
     */
    public static TestDataParametersBuilder mediumBuilder() {
        return TestDataParameters.builder()
                .totalTransactionsNumber(3_000_000)
                .targetOrderBookOrdersTotal(1_000_000)
                .numAccounts(3_300_000)
                .currenciesAllowed(TestConstants.ALL_CURRENCIES)
                .numSymbols(10_000)
                .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.BOTH)
                .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER);
    }

    /**
     * Mixed spot and futures workload with 50,000 symbols, about 3 million generated users
     * with 10 million currency accounts, a prefill target of 3 million orders and 3 million benchmark commands.
     *
     * @return large exchange test data configuration
     */
    public static TestDataParametersBuilder largeBuilder() {
        return TestDataParameters.builder()
                .totalTransactionsNumber(3_000_000)
                .targetOrderBookOrdersTotal(3_000_000)
                .numAccounts(10_000_000)
                .currenciesAllowed(TestConstants.ALL_CURRENCIES)
                .numSymbols(50_000)
                .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.BOTH)
                .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER);
    }

    /**
     * Mixed spot and futures workload with 100,000 symbols, about 10 million generated users
     * with 33 million currency accounts, a prefill target of 30 million orders and 10 million benchmark commands.
     *
     * @return huge exchange test data configuration
     */
    public static TestDataParametersBuilder hugeBuilder() {
        return TestDataParameters.builder()
                .totalTransactionsNumber(10_000_000)
                .targetOrderBookOrdersTotal(30_000_000)
                .numAccounts(33_000_000)
                .currenciesAllowed(TestConstants.ALL_CURRENCIES)
                .numSymbols(100_000)
                .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.BOTH)
                .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER);
    }

    TestDataParameters(
            int totalTransactionsNumber,
            int targetOrderBookOrdersTotal,
            int numAccounts,
            Set<Integer> currenciesAllowed,
            int numSymbols,
            ExchangeTestContainer.AllowedSymbolTypes allowedSymbolTypes,
            TestOrdersGeneratorConfig.PreFillMode preFillMode,
            boolean avalancheIOC) {
        this.totalTransactionsNumber = totalTransactionsNumber;
        this.targetOrderBookOrdersTotal = targetOrderBookOrdersTotal;
        this.numAccounts = numAccounts;
        this.currenciesAllowed = currenciesAllowed;
        this.numSymbols = numSymbols;
        this.allowedSymbolTypes = allowedSymbolTypes;
        this.preFillMode = preFillMode;
        this.avalancheIOC = avalancheIOC;
    }

    public static TestDataParametersBuilder builder() {
        return new TestDataParametersBuilder();
    }

    public int getTotalTransactionsNumber() {
        return this.totalTransactionsNumber;
    }

    public int getTargetOrderBookOrdersTotal() {
        return this.targetOrderBookOrdersTotal;
    }

    public int getNumAccounts() {
        return this.numAccounts;
    }

    public Set<Integer> getCurrenciesAllowed() {
        return this.currenciesAllowed;
    }

    public int getNumSymbols() {
        return this.numSymbols;
    }

    public ExchangeTestContainer.AllowedSymbolTypes getAllowedSymbolTypes() {
        return this.allowedSymbolTypes;
    }

    public TestOrdersGeneratorConfig.PreFillMode getPreFillMode() {
        return this.preFillMode;
    }

    public boolean isAvalancheIOC() {
        return this.avalancheIOC;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof TestDataParameters other
                && other.canEqual(this)
                && this.getTotalTransactionsNumber() == other.getTotalTransactionsNumber()
                && this.getTargetOrderBookOrdersTotal() == other.getTargetOrderBookOrdersTotal()
                && this.getNumAccounts() == other.getNumAccounts()
                && this.getNumSymbols() == other.getNumSymbols()
                && this.isAvalancheIOC() == other.isAvalancheIOC()
                && Objects.equals(this.getCurrenciesAllowed(), other.getCurrenciesAllowed())
                && Objects.equals(this.getAllowedSymbolTypes(), other.getAllowedSymbolTypes())
                && Objects.equals(this.getPreFillMode(), other.getPreFillMode());
    }

    protected boolean canEqual(Object other) {
        return other instanceof TestDataParameters;
    }

    @Override
    public int hashCode() {
        final int prime = 59;
        int result = 1;
        result = result * prime + this.getTotalTransactionsNumber();
        result = result * prime + this.getTargetOrderBookOrdersTotal();
        result = result * prime + this.getNumAccounts();
        result = result * prime + this.getNumSymbols();
        result = result * prime + (this.isAvalancheIOC() ? 79 : 97);
        var currenciesAllowed = this.getCurrenciesAllowed();
        result = result * prime + (currenciesAllowed == null ? 43 : currenciesAllowed.hashCode());
        var allowedSymbolTypes = this.getAllowedSymbolTypes();
        result = result * prime + (allowedSymbolTypes == null ? 43 : allowedSymbolTypes.hashCode());
        var preFillMode = this.getPreFillMode();
        result = result * prime + (preFillMode == null ? 43 : preFillMode.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return "TestDataParameters(totalTransactionsNumber=" + this.getTotalTransactionsNumber()
                + ", targetOrderBookOrdersTotal=" + this.getTargetOrderBookOrdersTotal()
                + ", numAccounts=" + this.getNumAccounts()
                + ", currenciesAllowed=" + this.getCurrenciesAllowed()
                + ", numSymbols=" + this.getNumSymbols()
                + ", allowedSymbolTypes=" + this.getAllowedSymbolTypes()
                + ", preFillMode=" + this.getPreFillMode()
                + ", avalancheIOC=" + this.isAvalancheIOC() + ")";
    }

    public static class TestDataParametersBuilder {
        private int totalTransactionsNumber;
        private int targetOrderBookOrdersTotal;
        private int numAccounts;
        private Set<Integer> currenciesAllowed;
        private int numSymbols;
        private ExchangeTestContainer.AllowedSymbolTypes allowedSymbolTypes;
        private TestOrdersGeneratorConfig.PreFillMode preFillMode;
        private boolean avalancheIOC;

        TestDataParametersBuilder() {
        }

        public TestDataParametersBuilder totalTransactionsNumber(int totalTransactionsNumber) {
            this.totalTransactionsNumber = totalTransactionsNumber;
            return this;
        }

        public TestDataParametersBuilder targetOrderBookOrdersTotal(int targetOrderBookOrdersTotal) {
            this.targetOrderBookOrdersTotal = targetOrderBookOrdersTotal;
            return this;
        }

        public TestDataParametersBuilder numAccounts(int numAccounts) {
            this.numAccounts = numAccounts;
            return this;
        }

        public TestDataParametersBuilder currenciesAllowed(Set<Integer> currenciesAllowed) {
            this.currenciesAllowed = currenciesAllowed;
            return this;
        }

        public TestDataParametersBuilder numSymbols(int numSymbols) {
            this.numSymbols = numSymbols;
            return this;
        }

        public TestDataParametersBuilder allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes allowedSymbolTypes) {
            this.allowedSymbolTypes = allowedSymbolTypes;
            return this;
        }

        public TestDataParametersBuilder preFillMode(TestOrdersGeneratorConfig.PreFillMode preFillMode) {
            this.preFillMode = preFillMode;
            return this;
        }

        public TestDataParametersBuilder avalancheIOC(boolean avalancheIOC) {
            this.avalancheIOC = avalancheIOC;
            return this;
        }

        public TestDataParameters build() {
            return new TestDataParameters(
                    this.totalTransactionsNumber,
                    this.targetOrderBookOrdersTotal,
                    this.numAccounts,
                    this.currenciesAllowed,
                    this.numSymbols,
                    this.allowedSymbolTypes,
                    this.preFillMode,
                    this.avalancheIOC);
        }

        @Override
        public String toString() {
            return "TestDataParameters.TestDataParametersBuilder(totalTransactionsNumber=" + this.totalTransactionsNumber
                    + ", targetOrderBookOrdersTotal=" + this.targetOrderBookOrdersTotal
                    + ", numAccounts=" + this.numAccounts
                    + ", currenciesAllowed=" + this.currenciesAllowed
                    + ", numSymbols=" + this.numSymbols
                    + ", allowedSymbolTypes=" + this.allowedSymbolTypes
                    + ", preFillMode=" + this.preFillMode
                    + ", avalancheIOC=" + this.avalancheIOC + ")";
        }
    }
}
