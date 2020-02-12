package exchange.core2.core.common.config;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public final class InitialStateConfiguration {

    final String exchangeId;

    final InitSnapshotConfiguration snapshotConf;

    final InitJournalConfiguration journalConf;

    public static InitialStateConfiguration cleanStart(String exchangeId) {

        return new InitialStateConfiguration(
                exchangeId,
                InitSnapshotConfiguration.CLEAN_START,
                InitJournalConfiguration.IGNORE_JOURNAL);
    }

    public static InitialStateConfiguration fromSnapshotOnly(String exchangeId, long snapshotId) {

        return new InitialStateConfiguration(
                exchangeId,
                InitSnapshotConfiguration.fromSnapshotId(snapshotId),
                InitJournalConfiguration.IGNORE_JOURNAL);
    }
}
