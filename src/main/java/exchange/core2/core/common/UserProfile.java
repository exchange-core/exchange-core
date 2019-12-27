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
package exchange.core2.core.common;

import com.koloboke.collect.map.IntLongMap;
import com.koloboke.collect.map.IntObjMap;
import com.koloboke.collect.map.hash.HashIntLongMaps;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import exchange.core2.core.utils.HashingUtils;
import exchange.core2.core.utils.SerializationUtils;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.Objects;

@Slf4j
public final class UserProfile implements WriteBytesMarshallable, StateHash {

    public final long uid;

    // symbol -> margin position records
    // TODO initialize lazily (only needed if margin trading allowed)
    public final IntObjMap<SymbolPositionRecord> positions;

    // protects from double adjustment
    public long adjustmentsCounter;

    // currency accounts
    // currency -> balance
    public final IntLongMap accounts;

    public boolean suspended;

    public UserProfile(long uid, boolean suspended) {
        //log.debug("New {}", uid);
        this.uid = uid;
        this.positions = HashIntObjMaps.newMutableMap();
        this.adjustmentsCounter = 0L;
        this.accounts = HashIntLongMaps.newUpdatableMap(4);
        this.suspended = suspended;
    }

    public UserProfile(BytesIn bytesIn) {

        this.uid = bytesIn.readLong();

        // positions
        this.positions = SerializationUtils.readIntObjMap(bytesIn, b -> new SymbolPositionRecord(uid, b), HashIntObjMaps::newMutableMap);

        // adjustmentsCounter
        this.adjustmentsCounter = bytesIn.readLong();

        // account balances
        this.accounts = SerializationUtils.readIntLongMap(bytesIn, HashIntLongMaps::newUpdatableMap);

        // suspended
        this.suspended = bytesIn.readBoolean();
    }

    public SymbolPositionRecord getOrCreatePositionRecord(CoreSymbolSpecification spec) {
        final int symbol = spec.symbolId;
        SymbolPositionRecord record = positions.get(symbol);
        if (record == null) {
            record = new SymbolPositionRecord(uid, symbol, spec.quoteCurrency);
            positions.put(symbol, record);
        }
        return record;
    }

    public SymbolPositionRecord getPositionRecordOrThrowEx(int symbol) {
        final SymbolPositionRecord record = positions.get(symbol);
        if (record == null) {
            throw new IllegalStateException("not found position for symbol " + symbol);
        }
        return record;
    }

    public void removeRecordIfEmpty(SymbolPositionRecord record) {
        if (record.isEmpty()) {
            accounts.addValue(record.currency, record.profit);
            positions.remove(record.symbol);
        }
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {

        bytes.writeLong(uid);

        // positions
        SerializationUtils.marshallIntHashMap(positions, bytes);

        // adjustmentsCounter
        bytes.writeLong(adjustmentsCounter);

        // account balances
        SerializationUtils.marshallIntLongHashMap(accounts, bytes);

        // suspended
        bytes.writeBoolean(suspended);
    }


    @Override
    public String toString() {
        return "UserProfile{" +
                "uid=" + uid +
                ", positions=" + positions.size() +
                ", accounts=" + accounts +
                ", adjustmentsCounter=" + adjustmentsCounter +
                ", suspended=" + suspended +
                '}';
    }

    @Override
    public int stateHash() {
        return Objects.hash(
                uid,
                HashingUtils.stateHash(positions),
                adjustmentsCounter,
                accounts.hashCode(),
                Boolean.hashCode(suspended));
    }
}
