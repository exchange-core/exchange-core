/*
 * Copyright 2019 Maksim Zheravin
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

import exchange.core2.core.ExchangeApi;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.config.InitialStateConfiguration;
import lombok.AllArgsConstructor;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.io.IOException;
import java.nio.file.Path;
import java.util.NavigableMap;
import java.util.function.Function;

public interface ISerializationProcessor {

    /**
     * Serialize state into a storage (disk, NAS, etc).<p>
     * Method is threadsafe - called from each module's thread upon receiving serialization command.<p>
     * Method is synchronous - returning true value only when the data was safely stored into independent storage.<p>
     *
     * @param snapshotId  - unique snapshot id
     * @param seq         - sequence of serialization
     * @param timestampNs - timestamp
     * @param type        - module (risk engine or matching engine)
     * @param instanceId  - module instance number (starting from 0 for each module type)
     * @param obj         - serialized data
     * @return true if serialization succeeded, false otherwise
     */
    boolean storeData(long snapshotId,
                      long seq,
                      long timestampNs,
                      SerializedModuleType type,
                      int instanceId,
                      WriteBytesMarshallable obj);

    /**
     * Deserialize state from a storage (disk, NAS, etc).<p>
     * Method is threadsafe - called from each module's thread on creation.<p>
     *
     * @param snapshotId - unique snapshot id
     * @param type       - module (risk engine or matching engine)
     * @param instanceId - module instance number (starting from 0)
     * @param initFunc   - creator lambda function
     * @param <T>        - module implementation class
     * @return constructed object, or throws exception
     */
    <T> T loadData(long snapshotId,
                   SerializedModuleType type,
                   int instanceId,
                   Function<BytesIn, T> initFunc);


    /**
     * Write command into journal
     *
     * @param cmd  - command to write
     * @param dSeq - disruptor sequence
     * @param eob  - if true, journal should commit all previous data synchronously
     * @throws IOException - can throw in case of writing issue (will stop exchange core from responding)
     */
    void writeToJournal(OrderCommand cmd, long dSeq, boolean eob) throws IOException;


    /**
     * Activate journal
     *
     * @param afterSeq - enable only after specified sequence, for lower sequences no writes to journal
     * @param api      - API reference
     */
    void enableJournaling(long afterSeq, ExchangeApi api);

    /**
     * get all available snapshots
     *
     * @return sequential map of snapshots (TODO can be a tree)
     */
    NavigableMap<Long, SnapshotDescriptor> findAllSnapshotPoints();

    /**
     * Replay journal
     *
     * @param snapshotId - snapshot id (important for tree history)
     * @param seqFrom    - starting command sequence (exclusive)
     * @param seqTo      - ending command sequence (inclusive)
     * @param api        - API reference
     */
    void replayJournalStep(long snapshotId, long seqFrom, long seqTo, ExchangeApi api);

    long replayJournalFull(InitialStateConfiguration initialStateConfiguration, ExchangeApi api);

    void replayJournalFullAndThenEnableJouraling(InitialStateConfiguration initialStateConfiguration, ExchangeApi exchangeApi);

    /**
     * Resolve the snapshot path
     *
     * @param snapshotId  - unique snapshot id
     * @param type        - module (risk engine or matching engine)
     * @param instanceId  - module instance number (starting from 0 for each module type)
     * @return Path the resolved snapshot path
     */
    Path resolveSnapshotPath(long snapshotId, SerializedModuleType type, int instanceId);

    @AllArgsConstructor
    enum SerializedModuleType {
        RISK_ENGINE("RE"),
        MATCHING_ENGINE_ROUTER("ME");

        final String code;
    }

}
