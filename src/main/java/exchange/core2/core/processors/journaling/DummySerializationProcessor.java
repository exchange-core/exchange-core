/*
 * Copyright 2019-2022 Maksim Zheravin
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
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.NavigableMap;
import java.util.function.Function;

public class DummySerializationProcessor implements ISerializationProcessor {

    public static final DummySerializationProcessor INSTANCE = new DummySerializationProcessor();

    @Override
    public boolean storeData(long snapshotId, long seq, long timestampNs, SerializedModuleType type, int instanceId, WriteBytesMarshallable obj) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T loadData(long snapshotId, SerializedModuleType type, int instanceId, Function<BytesIn, T> initFunc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeToJournal(OrderCommand cmd, long dSeq, boolean eob) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void enableJournaling(long afterSeq, ExchangeApi api) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableMap<Long, SnapshotDescriptor> findAllSnapshotPoints() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replayJournalStep(long snapshotId, long seqFrom, long seqTo, ExchangeApi exchangeApi) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long replayJournalFull(InitialStateConfiguration initialStateConfiguration, ExchangeApi exchangeApi) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replayJournalFullAndThenEnableJouraling(InitialStateConfiguration initialStateConfiguration, ExchangeApi exchangeApi) {
        // do nothing
    }

    @Override
    public boolean checkSnapshotExists(long snapshotId, SerializedModuleType type, int instanceId) {
        return false;
    }
}
