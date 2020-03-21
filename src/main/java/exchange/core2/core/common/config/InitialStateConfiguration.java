package exchange.core2.core.common.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@AllArgsConstructor
@Getter
@Builder
public final class InitialStateConfiguration {

    public static InitialStateConfiguration CLEAN_TEST = InitialStateConfiguration.cleanStart("EC0");

    private final String exchangeId;

    private final boolean enableJournaling;

    private final long snapshotId;

    private final long snapshotBaseSeq;

    private final long journalTimestampNs; // exclusive timestamp (for reports),
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

    public static InitialStateConfiguration cleanStartJournaling(String exchangeId) {

        return InitialStateConfiguration.builder()
                .exchangeId(exchangeId)
                .enableJournaling(true)
                .snapshotId(0)
                .snapshotBaseSeq(0)
                .build();
    }


    public static InitialStateConfiguration fromSnapshotOnly(String exchangeId, long snapshotId, long baseSeq) {

        return InitialStateConfiguration.builder()
                .exchangeId(exchangeId)
                .enableJournaling(false)
                .snapshotId(snapshotId)
                .snapshotBaseSeq(baseSeq)
                .build();
    }


    /**
     * Load exchange from last known state including journal
     * Journal ON
     *
     * @param exchangeId
     * @param snapshotId
     * @param baseSeq
     * @return
     */
    public static InitialStateConfiguration lastKnownStateFromJournal(String exchangeId, long snapshotId, long baseSeq) {

        return InitialStateConfiguration.builder()
                .exchangeId(exchangeId)
                .enableJournaling(true)
                .snapshotId(snapshotId)
                .snapshotBaseSeq(baseSeq)
                .journalTimestampNs(Long.MAX_VALUE)
                .build();
    }
}
