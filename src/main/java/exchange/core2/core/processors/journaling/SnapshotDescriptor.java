/*
 * Copyright 2019-2026 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package exchange.core2.core.processors.journaling;

import org.jetbrains.annotations.NotNull;

import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

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

    public SnapshotDescriptor(
            long snapshotId,
            long seq,
            long timestampNs,
            SnapshotDescriptor prev,
            int numMatchingEngines,
            int numRiskEngines) {
        this.snapshotId = snapshotId;
        this.seq = seq;
        this.timestampNs = timestampNs;
        this.prev = prev;
        this.numMatchingEngines = numMatchingEngines;
        this.numRiskEngines = numRiskEngines;
    }

    public long getSnapshotId() {
        return this.snapshotId;
    }

    public long getSeq() {
        return this.seq;
    }

    public long getTimestampNs() {
        return this.timestampNs;
    }

    public SnapshotDescriptor getPrev() {
        return this.prev;
    }

    public SnapshotDescriptor getNext() {
        return this.next;
    }

    public int getNumMatchingEngines() {
        return this.numMatchingEngines;
    }

    public int getNumRiskEngines() {
        return this.numRiskEngines;
    }

    public NavigableMap<Long, JournalDescriptor> getJournals() {
        return this.journals;
    }

    public void setNext(SnapshotDescriptor next) {
        this.next = next;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof SnapshotDescriptor other
                && other.canEqual(this)
                && this.getSnapshotId() == other.getSnapshotId()
                && this.getSeq() == other.getSeq()
                && this.getTimestampNs() == other.getTimestampNs()
                && this.getNumMatchingEngines() == other.getNumMatchingEngines()
                && this.getNumRiskEngines() == other.getNumRiskEngines()
                && Objects.equals(this.getPrev(), other.getPrev())
                && Objects.equals(this.getNext(), other.getNext())
                && Objects.equals(this.getJournals(), other.getJournals());
    }

    protected boolean canEqual(Object other) {
        return other instanceof SnapshotDescriptor;
    }

    @Override
    public int hashCode() {
        final int prime = 59;
        int result = 1;
        result = result * prime + Long.hashCode(this.getSnapshotId());
        result = result * prime + Long.hashCode(this.getSeq());
        result = result * prime + Long.hashCode(this.getTimestampNs());
        result = result * prime + this.getNumMatchingEngines();
        result = result * prime + this.getNumRiskEngines();
        var prev = this.getPrev();
        result = result * prime + (prev == null ? 43 : prev.hashCode());
        var next = this.getNext();
        result = result * prime + (next == null ? 43 : next.hashCode());
        var journals = this.getJournals();
        result = result * prime + (journals == null ? 43 : journals.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return "SnapshotDescriptor(snapshotId=" + this.getSnapshotId()
                + ", seq=" + this.getSeq()
                + ", timestampNs=" + this.getTimestampNs()
                + ", prev=" + this.getPrev()
                + ", next=" + this.getNext()
                + ", numMatchingEngines=" + this.getNumMatchingEngines()
                + ", numRiskEngines=" + this.getNumRiskEngines()
                + ", journals=" + this.getJournals() + ")";
    }
}
