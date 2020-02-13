package exchange.core2.core.common.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@AllArgsConstructor
@Getter
@Builder
public final class InitialStateConfiguration {

    public static InitialStateConfiguration TEST_CONFIG = InitialStateConfiguration.cleanStart("EC0");

    private final String exchangeId;

    private final boolean enableJournaling;

    private final long snapshotId;

    private final long timestampNs; // exclusive timestamp,
    // Long.MAX_VALUE to read full available journal (or until reading error)
    // 0 to ignore the journal

    // TODO ignore journal

    public boolean fromSnapshot() {
        return snapshotId != 0;
    }

    public static InitialStateConfiguration cleanStart(String exchangeId) {

        return InitialStateConfiguration.builder()
                .exchangeId(exchangeId)
                .enableJournaling(false)
                .snapshotId(0)
                .build();
    }

    public static InitialStateConfiguration cleanStartJournalling(String exchangeId) {

        return InitialStateConfiguration.builder()
                .exchangeId(exchangeId)
                .enableJournaling(true)
                .snapshotId(0)
                .build();
    }


    public static InitialStateConfiguration fromSnapshotOnly(String exchangeId, long snapshotId) {

        return InitialStateConfiguration.builder()
                .exchangeId(exchangeId)
                .enableJournaling(false)
                .snapshotId(snapshotId)
                .build();
    }
}
