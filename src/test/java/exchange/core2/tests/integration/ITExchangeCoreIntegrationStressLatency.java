package exchange.core2.tests.integration;

import exchange.core2.core.common.config.PerformanceConfiguration;

/**
 * Runs the shared stress scenarios with the default blocking configuration.
 */
public class ITExchangeCoreIntegrationStressLatency extends ITExchangeCoreIntegrationStress {

    @Override
    public PerformanceConfiguration getPerformanceConfiguration() {
        return PerformanceConfiguration.DEFAULT;
    }
}
