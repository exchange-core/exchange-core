package exchange.core2.core.processors.journaling;

import lombok.Data;
import org.jetbrains.annotations.NotNull;

import java.util.NavigableMap;
import java.util.TreeMap;

@Data
public class SnapshotDescriptor implements Comparable<SnapshotDescriptor> {

    private final long snapshotId; // 0 means empty snapshot (clean start)

    // sequence when snapshot was made
    private final long seq;
    private final long timestampNs;

    // next and previous snapshots
    private final SnapshotDescriptor prev;
    private SnapshotDescriptor next = null; // TODO can be a list

    private final int numMatchingEngines;
    private final int numRiskEngines;

    // all journals based on this snapshot
    // mapping: startingSeq -> JournalDescriptor
    private final NavigableMap<Long, JournalDescriptor> journals = new TreeMap<>();

    /**
     * Create initial empty snapshot descriptor
     *
     * @param initialNumME - number of matching engine instances
     * @param initialNumRE - number of risk engine instances
     * @return new instance
     */
    public static SnapshotDescriptor createEmpty(int initialNumME, int initialNumRE) {
        return new SnapshotDescriptor(0, 0, 0, null, initialNumME, initialNumRE);
    }

    public SnapshotDescriptor createNext(long snapshotId, long seq, long timestampNs) {
        return new SnapshotDescriptor(snapshotId, seq, timestampNs, this, numMatchingEngines, numRiskEngines);
    }

    @Override
    public int compareTo(@NotNull SnapshotDescriptor o) {
        return Long.compare(this.seq, o.seq);
    }
}
