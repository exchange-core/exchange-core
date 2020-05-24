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
    private final PerformanceConfiguration performanceCfg;


    /*
     * Exchange initialization configuration
     */
    private final InitialStateConfiguration initStateCfg;

    /*
     * Exchange configuration
     */
    private final ReportsQueriesConfiguration reportsQueriesCfg;

    /*
     * Logging configuration
     */
    private final LoggingConfiguration loggingCfg;

    /*
     * Serialization (snapshots and journaling) configuration
     */
    private final SerializationConfiguration serializationCfg;

    @Override
    public String toString() {
        return "ExchangeConfiguration{" +
                "\n  ordersProcessingCfg=" + ordersProcessingCfg +
                "\n  performanceCfg=" + performanceCfg +
                "\n  initStateCfg=" + initStateCfg +
                "\n  reportsQueriesCfg=" + reportsQueriesCfg +
                "\n  loggingCfg=" + loggingCfg +
                "\n  serializationCfg=" + serializationCfg +
                '}';
    }

    /**
     * Sample configuration builder having predefined default settings.
     *
     * @return configuration builder
     */
    public static ExchangeConfiguration.ExchangeConfigurationBuilder defaultBuilder() {
        return ExchangeConfiguration.builder()
                .ordersProcessingCfg(OrdersProcessingConfiguration.DEFAULT)
                .initStateCfg(InitialStateConfiguration.cleanStart("MY_EXCHANGE"))
                .performanceCfg(PerformanceConfiguration.baseBuilder().build())
                .reportsQueriesCfg(ReportsQueriesConfiguration.createStandardConfig())
                .loggingCfg(LoggingConfiguration.DEFAULT)
                .serializationCfg(SerializationConfiguration.DEFAULT);
    }
}
