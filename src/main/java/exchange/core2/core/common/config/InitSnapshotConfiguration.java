package exchange.core2.core.common.config;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class InitSnapshotConfiguration {

    public static InitSnapshotConfiguration CLEAN_START = new InitSnapshotConfiguration(0);

    @Getter
    private final long snapshotId;

    public boolean noSnapshot() {
        return this == CLEAN_START;
    }

    public static InitSnapshotConfiguration fromSnapshotId(long snapshotId) {
        return new InitSnapshotConfiguration(snapshotId);
    }
}
