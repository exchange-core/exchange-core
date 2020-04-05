package exchange.core2.tests.steps;

import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.tests.util.L2MarketDataHelper;
import io.cucumber.core.api.TypeRegistry;
import io.cucumber.core.api.TypeRegistryConfigurer;
import io.cucumber.cucumberexpressions.ParameterType;
import io.cucumber.cucumberexpressions.Transformer;
import io.cucumber.datatable.DataTableType;
import io.cucumber.datatable.TableTransformer;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static exchange.core2.tests.util.TestConstants.SYMBOLSPEC_ETH_XBT;
import static exchange.core2.tests.util.TestConstants.SYMBOLSPEC_EUR_USD;

// TODO use annotation based configuration
public class TypeRegistryConfiguration implements TypeRegistryConfigurer {

    @Override
    public void configureTypeRegistry(TypeRegistry typeRegistry) {
        final Map<String, CoreSymbolSpecification> symbolSpecificationMap = new HashMap<>();
        symbolSpecificationMap.put("EUR_USD", SYMBOLSPEC_EUR_USD);
        symbolSpecificationMap.put("ETH_XBT", SYMBOLSPEC_ETH_XBT);

        typeRegistry.defineParameterType(
                new ParameterType<>("symbol",
                        "(EUR_USD)?(ETH_XBT)?",
                        CoreSymbolSpecification.class,
                        (Transformer<CoreSymbolSpecification>) symbolSpecificationMap::get
                ));

        final Map<String, Long> users = new HashMap<>();
        users.put("A", 1440001L);
        users.put("B", 1440002L);
        users.put("C", 1440003L);

        typeRegistry.defineParameterType(
                new ParameterType<>("user",
                        "([ABC]{1})",
                        Long.class,
                        (Transformer<Long>) users::get
                ));

        typeRegistry.defineDataTableType(new DataTableType(
                L2MarketDataHelper.class,
                (TableTransformer<L2MarketDataHelper>) table -> {
                    List<List<String>> list = table.asLists();

                    //skip a header if it presents
                    if (list.get(0).get(0) != null && list.get(0).get(0).trim().equals("bid")) {
                        list = list.subList(1, list.size());
                    }

                    //format | bid | price | ask |
                    final L2MarketDataHelper l2helper = new L2MarketDataHelper();
                    for (List<String> row : list) {
                        int price = Integer.parseInt(row.get(1));

                        String bid = row.get(0);
                        if (bid != null && bid.length() > 0) {
                            l2helper.addBid(price, Integer.parseInt(bid));
                        } else {
                            l2helper.addAsk(price, Integer.parseInt(row.get(2)));
                        }
                    }
                    return l2helper;
                }
        ));
    }

    @Override
    public Locale locale() {
        return Locale.getDefault();
    }
}
