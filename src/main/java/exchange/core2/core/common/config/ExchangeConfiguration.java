package exchange.core2.core.common.config;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@AllArgsConstructor
@Getter
@Builder
public final class ExchangeConfiguration {

    private final PerformanceConfiguration perfCfg;
    private final InitialStateConfiguration initStateCfg;
    private final ReportsQueriesConfiguration reportsQueriesCfg;


    public static ExchangeConfiguration.ExchangeConfigurationBuilder defaultBuilder() {
        return ExchangeConfiguration.builder()
                .initStateCfg(InitialStateConfiguration.cleanStart("MY_EXCHANGE"))
                .perfCfg(PerformanceConfiguration.baseBuilder().build())
                .reportsQueriesCfg(ReportsQueriesConfiguration.createStandardConfig());
    }
}
