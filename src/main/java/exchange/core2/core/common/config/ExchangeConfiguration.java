package exchange.core2.core.common.config;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;


/**
 * Exchange configuration
 */
@AllArgsConstructor
@Getter
@Builder
public final class ExchangeConfiguration {

    /*
     * Orders processing configuration
     */
    private final OrdersProcessingConfiguration ordersProcessingCfg;

    /*
     * Performance configuration
     */
    private final PerformanceConfiguration perfCfg;


    /*
     * Exchange initialization configuration
     */
    private final InitialStateConfiguration initStateCfg;

    /*
     * Exchange configuration
     */
    private final ReportsQueriesConfiguration reportsQueriesCfg;

    /**
     * Sample configuration builder having predefined default settings.
     *
     * @return configuration builder
     */
    public static ExchangeConfiguration.ExchangeConfigurationBuilder defaultBuilder() {
        return ExchangeConfiguration.builder()
                .ordersProcessingCfg(OrdersProcessingConfiguration.DEFAULT)
                .initStateCfg(InitialStateConfiguration.cleanStart("MY_EXCHANGE"))
                .perfCfg(PerformanceConfiguration.baseBuilder().build())
                .reportsQueriesCfg(ReportsQueriesConfiguration.createStandardConfig());
    }
}
