package exchange.core2.core.common.config;

/**
 * Exchange initialization configuration
 * <p>
 * TODO use interface with different implementations for Clean, Snapshot, Journal
 */
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

    public InitialStateConfiguration(
            String exchangeId,
            long snapshotId,
            long snapshotBaseSeq,
            long journalTimestampNs,
            boolean throwIfSnapshotNotFound) {
        this.exchangeId = exchangeId;
        this.snapshotId = snapshotId;
        this.snapshotBaseSeq = snapshotBaseSeq;
        this.journalTimestampNs = journalTimestampNs;
        this.throwIfSnapshotNotFound = throwIfSnapshotNotFound;
    }

    public static InitialStateConfigurationBuilder builder() {
        return new InitialStateConfigurationBuilder();
    }

    public String getExchangeId() {
        return this.exchangeId;
    }

    public long getSnapshotId() {
        return this.snapshotId;
    }

    public long getSnapshotBaseSeq() {
        return this.snapshotBaseSeq;
    }

    public long getJournalTimestampNs() {
        return this.journalTimestampNs;
    }

    public boolean isThrowIfSnapshotNotFound() {
        return this.throwIfSnapshotNotFound;
    }

    @Override
    public String toString() {
        return "InitialStateConfiguration(exchangeId=" + this.getExchangeId()
                + ", snapshotId=" + this.getSnapshotId()
                + ", snapshotBaseSeq=" + this.getSnapshotBaseSeq()
                + ", journalTimestampNs=" + this.getJournalTimestampNs()
                + ", throwIfSnapshotNotFound=" + this.isThrowIfSnapshotNotFound() + ")";
    }

    public static class InitialStateConfigurationBuilder {
        private String exchangeId;
        private long snapshotId;
        private long snapshotBaseSeq;
        private long journalTimestampNs;
        private boolean throwIfSnapshotNotFound;

        InitialStateConfigurationBuilder() {
        }

        public InitialStateConfigurationBuilder exchangeId(String exchangeId) {
            this.exchangeId = exchangeId;
            return this;
        }

        public InitialStateConfigurationBuilder snapshotId(long snapshotId) {
            this.snapshotId = snapshotId;
            return this;
        }

        public InitialStateConfigurationBuilder snapshotBaseSeq(long snapshotBaseSeq) {
            this.snapshotBaseSeq = snapshotBaseSeq;
            return this;
        }

        public InitialStateConfigurationBuilder journalTimestampNs(long journalTimestampNs) {
            this.journalTimestampNs = journalTimestampNs;
            return this;
        }

        public InitialStateConfigurationBuilder throwIfSnapshotNotFound(boolean throwIfSnapshotNotFound) {
            this.throwIfSnapshotNotFound = throwIfSnapshotNotFound;
            return this;
        }

        public InitialStateConfiguration build() {
            return new InitialStateConfiguration(
                    this.exchangeId,
                    this.snapshotId,
                    this.snapshotBaseSeq,
                    this.journalTimestampNs,
                    this.throwIfSnapshotNotFound);
        }

        @Override
        public String toString() {
            return "InitialStateConfiguration.InitialStateConfigurationBuilder(exchangeId=" + this.exchangeId
                    + ", snapshotId=" + this.snapshotId
                    + ", snapshotBaseSeq=" + this.snapshotBaseSeq
                    + ", journalTimestampNs=" + this.journalTimestampNs
                    + ", throwIfSnapshotNotFound=" + this.throwIfSnapshotNotFound + ")";
        }
    }
}
