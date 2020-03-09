package exchange.core2.core.processors.journaling;

import lombok.Data;

@Data
public class JournalDescriptor {

    private final long timestampNs;
    private final long seqFirst;
    private long seqLast = -1; // -1 if not finished yet

    private final SnapshotDescriptor baseSnapshot;

    private final JournalDescriptor prev; // can be null
    private JournalDescriptor next = null; // can be null

}
