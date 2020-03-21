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
}
