package exchange.core2.tests.integration;

import exchange.core2.core.common.config.PerformanceConfiguration;
import exchange.core2.tests.util.JournalingTestsModule;
import exchange.core2.tests.util.TestDataParameters;
import org.junit.jupiter.api.Test;

final class ITJournalRecovery {

    @Test
    void restoresStateAndBalancesFromSnapshotAndJournal() throws Exception {
        JournalingTestsModule.journalingTestImpl(
                PerformanceConfiguration.DEFAULT,
                TestDataParameters.singlePairExchangeBuilder()
                        .totalTransactionsNumber(10_000)
                        .targetOrderBookOrdersTotal(100)
                        .numAccounts(100)
                        .build(),
                1);
    }
}
