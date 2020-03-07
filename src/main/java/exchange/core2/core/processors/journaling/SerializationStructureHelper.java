package exchange.core2.core.processors.journaling;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

@Getter
@Slf4j
@Deprecated
public class SerializationStructureHelper {

    private final long exchangeId;

    // TODO make threadsafe if going to consume from other processors?
    private SnapshotDescriptor lastSnapshotDescriptor;
    private JournalDescriptor lastJournalDescriptor;

    private ConcurrentSkipListMap<Long, SnapshotDescriptor> snapshotsIndex;

    private final int numMatchingEngines;
    private final int numRiskEngines;

//    private final ObjectHashSet<ModuleSnapshots> preparingModules = new ObjectHashSet<>();

    public SerializationStructureHelper(long exchangeId, int numMatchingEngines, int numRiskEngines) {
        this.exchangeId = exchangeId;
        this.lastJournalDescriptor = null; // no journal
        this.lastSnapshotDescriptor = SnapshotDescriptor.createEmpty(numMatchingEngines, numRiskEngines);
        this.numMatchingEngines = numMatchingEngines;
        this.numRiskEngines = numRiskEngines;
    }


//    public synchronized boolean registerNextSnapshotPart(long snapshotId,
//                                                      long seq,
//                                                      long timestampNs,
//                                                      ISerializationProcessor.SerializedModuleType moduleType,
//                                                      int moduleId) {
//
//        final ModuleSnapshots ms = new ModuleSnapshots(snapshotId, moduleId, moduleType);
//        final boolean changed = preparingModules.add(ms);
//        if (!changed) {
//            log.error("duplicate snapshot call for {}", ms);
//            return false;
//        }
//
//        if (preparingModules.size() < numMatchingEngines + numRiskEngines) {
//            return false;
//        }
//
//        // TODO validations (all modules complete)
//
//        final SnapshotDescriptor sdNext = lastSnapshotDescriptor.createNext(snapshotId, seq, timestampNs);
//        final JournalDescriptor jdNext = new JournalDescriptor(timestampNs, seq, sdNext, lastJournalDescriptor);
//
//        lastSnapshotDescriptor = sdNext;
//        lastJournalDescriptor = jdNext;
//
//        return true;
//    }

    /**
     * call only from journal thread
     *
     * @param seq
     * @param timestampNs
     */
    public void registerNextJournal(long seq, long timestampNs) {

        lastJournalDescriptor = new JournalDescriptor(timestampNs, seq, lastSnapshotDescriptor, lastJournalDescriptor);
    }


    public NavigableMap<Long, SnapshotDescriptor> getAllSnapshots() {

        // TODO can use synchronized view on treemap or skiplistmap?

        return snapshotsIndex;

    }

//    @EqualsAndHashCode
//    @AllArgsConstructor
//    @Getter
//    @ToString
//    private static class ModuleSnapshots {
//        private final long snapshotId;
//        private final int moduleId;
//        private final ISerializationProcessor.SerializedModuleType moduleType;
//    }
}
