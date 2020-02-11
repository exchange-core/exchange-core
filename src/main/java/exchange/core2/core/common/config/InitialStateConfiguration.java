package exchange.core2.core.common.config;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public final class InitialStateConfiguration {

    public static final InitialStateConfiguration CLEAN_START = new InitialStateConfiguration(
            InitSnapshotConfiguration.CLEAN_START, InitJournalConfiguration.IGNORE_JOURNAL);

    final InitSnapshotConfiguration snapshotConf;

    final InitJournalConfiguration journalConf;

    public static InitialStateConfiguration fromSnapshotOnly(final long snapshotId) {

        return new InitialStateConfiguration(
                InitSnapshotConfiguration.fromSnapshotId(snapshotId),
                InitJournalConfiguration.IGNORE_JOURNAL);
    }
}
