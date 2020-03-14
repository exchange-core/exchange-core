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

}
