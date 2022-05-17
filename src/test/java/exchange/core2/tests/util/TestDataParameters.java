package exchange.core2.tests.util;

import lombok.Builder;
import lombok.Data;

import java.util.Set;

@Data
@Builder
public class TestDataParameters {
    public final int totalTransactionsNumber;
    public final int targetOrderBookOrdersTotal;
    public final int numAccounts;
    public final Set<Integer> currenciesAllowed;
    public final int numSymbols;
    public final ExchangeTestContainer.AllowedSymbolTypes allowedSymbolTypes;
    public final TestOrdersGeneratorConfig.PreFillMode preFillMode;
    public final boolean avalancheIOC;

    public static TestDataParameters.TestDataParametersBuilder singlePairMarginBuilder() {
        return TestDataParameters.builder()
                .totalTransactionsNumber(3_000_000)
                .targetOrderBookOrdersTotal(1000)
                .numAccounts(2000)
                .currenciesAllowed(TestConstants.CURRENCIES_FUTURES)
                .numSymbols(1)
                .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.FUTURES_CONTRACT)
                .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER);
    }

    public static TestDataParameters.TestDataParametersBuilder singlePairExchangeBuilder() {
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
     * - 1M active users (3M currency accounts)
     * - 1M pending limit-orders
     * - 10K symbols
     *
     * @return medium exchange test data configuration
     */
    public static TestDataParameters.TestDataParametersBuilder mediumBuilder() {
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
     * - 3M active users (10M currency accounts)
     * - 3M pending limit-orders
     * - 50K symbols
     *
     * @return large exchange test data configuration
     */
    public static TestDataParameters.TestDataParametersBuilder largeBuilder() {
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
     * - 10M active users (33M currency accounts)
     * - 30M pending limit-orders
     * - 100K symbols
     *
     * @return huge exchange test data configuration
     */
    public static TestDataParameters.TestDataParametersBuilder hugeBuilder() {
        return TestDataParameters.builder()
                .totalTransactionsNumber(10_000_000)
                .targetOrderBookOrdersTotal(30_000_000)
                .numAccounts(33_000_000)
                .currenciesAllowed(TestConstants.ALL_CURRENCIES)
                .numSymbols(100_000)
                .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.BOTH)
                .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER);
    }
}
