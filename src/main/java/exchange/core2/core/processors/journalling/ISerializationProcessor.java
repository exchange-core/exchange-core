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
package exchange.core2.core.processors.journalling;

import lombok.AllArgsConstructor;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.function.Function;

public interface ISerializationProcessor {

    boolean storeData(long snapshotId, SerializedModuleType type, int instanceId, WriteBytesMarshallable obj);

    <T> T loadData(long snapshotId, SerializedModuleType type, int instanceId, Function<BytesIn, T> initFunc);

    @AllArgsConstructor
    enum SerializedModuleType {
        RISK_ENGINE("RE"),
        MATCHING_ENGINE_ROUTER("ME");

        final String code;
    }

}
