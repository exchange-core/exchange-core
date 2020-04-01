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

    public static TestDataParameters.TestDataParametersBuilder mediumBuilder() {
        return TestDataParameters.builder()
                .totalTransactionsNumber(6_000_000)
                .targetOrderBookOrdersTotal(1_000_000)
                .numAccounts(3_300_000)
                .currenciesAllowed(TestConstants.ALL_CURRENCIES)
                .numSymbols(10_000)
                .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.BOTH)
                .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER);
    }

    public static TestDataParameters.TestDataParametersBuilder largeBuilder() {
        return TestDataParameters.builder()
                .totalTransactionsNumber(10_000_000)
                .targetOrderBookOrdersTotal(4_000_000)
                .numAccounts(10_000_000)
                .currenciesAllowed(TestConstants.ALL_CURRENCIES)
                .numSymbols(100_000)
                .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.BOTH)
                .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER);
    }

    public static TestDataParameters.TestDataParametersBuilder hugeBuilder() {
        return TestDataParameters.builder()
                .totalTransactionsNumber(33_000_000)
                .targetOrderBookOrdersTotal(25_000_000)
                .numAccounts(25_000_000)
                .currenciesAllowed(TestConstants.ALL_CURRENCIES)
                .numSymbols(200_000)
                .allowedSymbolTypes(ExchangeTestContainer.AllowedSymbolTypes.BOTH)
                .preFillMode(TestOrdersGeneratorConfig.PreFillMode.ORDERS_NUMBER);
    }
}
