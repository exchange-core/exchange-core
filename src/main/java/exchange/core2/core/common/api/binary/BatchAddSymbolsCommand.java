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
package exchange.core2.core.common.api.binary;

import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.utils.SerializationUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.Collection;

@AllArgsConstructor
@Getter
public final class BatchAddSymbolsCommand implements BinaryDataCommand {

    private final IntObjectHashMap<CoreSymbolSpecification> symbols;

    public BatchAddSymbolsCommand(final CoreSymbolSpecification symbol) {
        symbols = IntObjectHashMap.newWithKeysValues(symbol.symbolId, symbol);
    }

    public BatchAddSymbolsCommand(final Collection<CoreSymbolSpecification> collection) {
        symbols = new IntObjectHashMap<>(collection.size());
        collection.forEach(s -> symbols.put(s.symbolId, s));
    }


    public BatchAddSymbolsCommand(final BytesIn bytes) {
        symbols = SerializationUtils.readIntHashMap(bytes, CoreSymbolSpecification::new);
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        SerializationUtils.marshallIntHashMap(symbols, bytes);
    }

    @Override
    public int getBinaryCommandTypeCode() {
        return BinaryCommandType.ADD_SYMBOLS.getCode();
    }
}
