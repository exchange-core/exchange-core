package exchange.core2.tests.integration;

import exchange.core2.core.common.config.PerformanceConfiguration;

public class ITExchangeCoreIntegrationStressLatency extends ITExchangeCoreIntegrationStress {

    @Override
    public PerformanceConfiguration getPerformanceConfiguration() {
        return PerformanceConfiguration.DEFAULT;
    }
}
