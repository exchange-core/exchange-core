package exchange.core2.tests.integration;

import exchange.core2.core.common.config.PerformanceConfiguration;

public final class ITExchangeCoreIntegrationLatency extends ITExchangeCoreIntegration {

    @Override
    public PerformanceConfiguration getPerformanceConfiguration() {
        return PerformanceConfiguration.latencyPerformanceBuilder().build();
    }
}
