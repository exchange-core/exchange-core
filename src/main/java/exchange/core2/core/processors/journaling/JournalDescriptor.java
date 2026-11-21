package exchange.core2.core.processors.journaling;

import java.util.Objects;

public class JournalDescriptor {

    private final long timestampNs;
    private final long seqFirst;
    private long seqLast = -1; // -1 if not finished yet

    private final SnapshotDescriptor baseSnapshot;

    private final JournalDescriptor prev; // can be null
    private JournalDescriptor next = null; // can be null


    public JournalDescriptor(long timestampNs, long seqFirst, SnapshotDescriptor baseSnapshot, JournalDescriptor prev) {
        this.timestampNs = timestampNs;
        this.seqFirst = seqFirst;
        this.baseSnapshot = baseSnapshot;
        this.prev = prev;
    }

    public long getTimestampNs() {
        return this.timestampNs;
    }

    public long getSeqFirst() {
        return this.seqFirst;
    }

    public long getSeqLast() {
        return this.seqLast;
    }

    public SnapshotDescriptor getBaseSnapshot() {
        return this.baseSnapshot;
    }

    public JournalDescriptor getPrev() {
        return this.prev;
    }

    public JournalDescriptor getNext() {
        return this.next;
    }

    public void setSeqLast(long seqLast) {
        this.seqLast = seqLast;
    }

    public void setNext(JournalDescriptor next) {
        this.next = next;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof JournalDescriptor other
                && other.canEqual(this)
                && this.getTimestampNs() == other.getTimestampNs()
                && this.getSeqFirst() == other.getSeqFirst()
                && this.getSeqLast() == other.getSeqLast()
                && Objects.equals(this.getBaseSnapshot(), other.getBaseSnapshot())
                && Objects.equals(this.getPrev(), other.getPrev())
                && Objects.equals(this.getNext(), other.getNext());
    }

    protected boolean canEqual(Object other) {
        return other instanceof JournalDescriptor;
    }

    @Override
    public int hashCode() {
        final int prime = 59;
        int result = 1;
        result = result * prime + Long.hashCode(this.getTimestampNs());
        result = result * prime + Long.hashCode(this.getSeqFirst());
        result = result * prime + Long.hashCode(this.getSeqLast());
        var baseSnapshot = this.getBaseSnapshot();
        result = result * prime + (baseSnapshot == null ? 43 : baseSnapshot.hashCode());
        var prev = this.getPrev();
        result = result * prime + (prev == null ? 43 : prev.hashCode());
        var next = this.getNext();
        result = result * prime + (next == null ? 43 : next.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return "JournalDescriptor(timestampNs=" + this.getTimestampNs()
                + ", seqFirst=" + this.getSeqFirst()
                + ", seqLast=" + this.getSeqLast()
                + ", baseSnapshot=" + this.getBaseSnapshot()
                + ", prev=" + this.getPrev()
                + ", next=" + this.getNext() + ")";
    }
}
