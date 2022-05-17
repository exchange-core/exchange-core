package exchange.core2.core.common.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * Exchange initialization configuration
 * <p>
 * TODO use interface with different implementations for Clean, Snapshot, Journal
 */
@AllArgsConstructor
@Getter
@Builder
@ToString
public final class InitialStateConfiguration {

    public static InitialStateConfiguration DEFAULT = InitialStateConfiguration.cleanStart("MY_EXCHANGE");

    public static InitialStateConfiguration CLEAN_TEST = InitialStateConfiguration.cleanStart("EC0");
    /*
     * Exchange ID string.
     * Should not have special characters because it is used for file names.
     */
    private final String exchangeId;

    /*
     * SnapshotID to load.
     * Set to 0 fot clean start.
     */
    private final long snapshotId;

    private final long snapshotBaseSeq;

    /*
     * When loading from journal, it will stop replaying commands as soon as this timestamp reached.
     * Set to 0 to ignore the journal, or Long.MAX_VALUE to read full available journal (or until reading error).
     */
    private final long journalTimestampNs;

    /*
     * When requested loading from snapshot, and snapshot was not found - throw an exception,
     * if throwIfSnapshotNotFound true. Otherwise, will initialize empty exchange.
     */
    private final boolean throwIfSnapshotNotFound;

    // TODO ignore journal

    public boolean fromSnapshot() {
        return snapshotId != 0;
    }

    /**
     * Clean start configuration
     *
     * @param exchangeId Exchange ID
     * @return clean start configuration without journaling.
     */
    public static InitialStateConfiguration cleanStart(String exchangeId) {

        return InitialStateConfiguration.builder()
                .exchangeId(exchangeId)
                .snapshotId(0)
                .build();
    }

    /**
     * Clean start configuration with journaling on.
     *
     * @param exchangeId Exchange ID
     * @return clean start configuration with journaling on.
     */
    public static InitialStateConfiguration cleanStartJournaling(String exchangeId) {

        return InitialStateConfiguration.builder()
                .exchangeId(exchangeId)
                .snapshotId(0)
                .snapshotBaseSeq(0)
                .throwIfSnapshotNotFound(true)
                .build();
    }

    /**
     * Configuration that loads from snapshot, without journal replay with journaling off.
     *
     * @param exchangeId Exchange ID
     * @param snapshotId snapshot ID
     * @param baseSeq    bas seq
     * @return configuration that loads from snapshot, without journal replay with journaling off.
     */
    public static InitialStateConfiguration fromSnapshotOnly(String exchangeId, long snapshotId, long baseSeq) {

        return InitialStateConfiguration.builder()
                .exchangeId(exchangeId)
                .snapshotId(snapshotId)
                .snapshotBaseSeq(baseSeq)
                .throwIfSnapshotNotFound(true)
                .build();
    }


    /**
     * Configuration that load exchange from last known state including journal replay till last known start. Journal is enabled.
     *
     * @param exchangeId Exchange ID
     * @param snapshotId snapshot ID
     * @param baseSeq    bas seq
     * @return configuration that load exchange from last known state including journal replay till last known start. Journal is enabled.
     * TODO how to recreate from the next journal section recorded after the first recovery?
     */
    public static InitialStateConfiguration lastKnownStateFromJournal(String exchangeId, long snapshotId, long baseSeq) {

        return InitialStateConfiguration.builder()
                .exchangeId(exchangeId)
                .snapshotId(snapshotId)
                .snapshotBaseSeq(baseSeq)
                .throwIfSnapshotNotFound(true)
                .journalTimestampNs(Long.MAX_VALUE)
                .build();
    }
}
