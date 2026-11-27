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

import exchange.core2.collections.hashtable.IntLongHashtable;
import exchange.core2.core.utils.HashingUtils;
import exchange.core2.core.utils.SerializationUtils;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;

public final class UserProfile implements WriteBytesMarshallable, StateHash {

    // user UUID: the most and the least significant bits
    public final long uidHi;
    public final long uidLo;

    // Margin positions by symbol. Most users hold none or one, so one position is kept in a field and the
    // map (~200 bytes even when empty) is created for the second one. At most one of the two is not null;
    // the map stays until its last position is removed.
    private SymbolPositionRecord singlePosition;
    private IntObjectHashMap<SymbolPositionRecord> positionsMap;

    // protects from double adjustment
    public long adjustmentsCounter;

    // currency accounts
    // currency -> balance
    public final IntLongHashtable accounts;

    public UserStatus userStatus;

    public UserProfile(long uidHi, long uidLo, UserStatus userStatus) {
        this.uidHi = uidHi;
        this.uidLo = uidLo;
        this.adjustmentsCounter = 0L;
        this.accounts = new IntLongHashtable(2);
        this.userStatus = userStatus;
    }

    public UserProfile(BytesIn bytesIn) {

        this.uidHi = bytesIn.readLong();
        this.uidLo = bytesIn.readLong();

        // positions
        final int positionsCount = bytesIn.readInt();
        if (positionsCount == 1) {
            bytesIn.readInt(); // symbol, also stored in the record
            this.singlePosition = new SymbolPositionRecord(uidHi, uidLo, bytesIn);
        } else if (positionsCount > 1) {
            this.positionsMap = new IntObjectHashMap<>(positionsCount);
            for (int i = 0; i < positionsCount; i++) {
                this.positionsMap.put(bytesIn.readInt(), new SymbolPositionRecord(uidHi, uidLo, bytesIn));
            }
        }

        // adjustmentsCounter
        this.adjustmentsCounter = bytesIn.readLong();

        // account balances
        this.accounts = SerializationUtils.readIntLongHashtable(bytesIn);

        // suspended
        this.userStatus = UserStatus.of(bytesIn.readByte());
    }

    public IntLongHashMap copyAccounts() {
        final IntLongHashMap snapshot = new IntLongHashMap(Math.toIntExact(accounts.size()));
        accounts.forEach(snapshot::put);
        return snapshot;
    }

    public SymbolPositionRecord getPositionRecordOrThrowEx(int symbol) {
        final SymbolPositionRecord record = getPosition(symbol);
        if (record == null) {
            throw new IllegalStateException("not found position for symbol " + symbol);
        }
        return record;
    }

    /**
     * @return the position for the symbol, or null
     */
    public SymbolPositionRecord getPosition(final int symbol) {
        final SymbolPositionRecord single = singlePosition;
        if (single != null) {
            return single.symbol == symbol ? single : null;
        }
        final IntObjectHashMap<SymbolPositionRecord> map = positionsMap;
        return map != null ? map.get(symbol) : null;
    }

    /**
     * Adds the position under its symbol, replacing the one held for that symbol.
     */
    public void putPosition(final SymbolPositionRecord position) {
        final IntObjectHashMap<SymbolPositionRecord> map = positionsMap;
        if (map != null) {
            map.put(position.symbol, position);
        } else if (singlePosition == null || singlePosition.symbol == position.symbol) {
            singlePosition = position;
        } else {
            final IntObjectHashMap<SymbolPositionRecord> created = new IntObjectHashMap<>();
            created.put(singlePosition.symbol, singlePosition);
            created.put(position.symbol, position);
            singlePosition = null;
            positionsMap = created;
        }
    }

    /**
     * Removes the position for the symbol, if any.
     */
    public void removePosition(final int symbol) {
        final SymbolPositionRecord single = singlePosition;
        if (single != null) {
            if (single.symbol == symbol) {
                singlePosition = null;
            }
            return;
        }
        final IntObjectHashMap<SymbolPositionRecord> map = positionsMap;
        if (map != null) {
            map.remove(symbol);
            if (map.isEmpty()) {
                positionsMap = null;
            }
        }
    }

    public int positionsCount() {
        if (singlePosition != null) {
            return 1;
        }
        return positionsMap != null ? positionsMap.size() : 0;
    }

    /**
     * Hot loops use this and {@link #getPositionsMap()} directly to avoid lambdas: one of them is null.
     *
     * @return the position held outside the map, or null
     */
    public SymbolPositionRecord getSinglePosition() {
        return singlePosition;
    }

    /**
     * @return the positions map (two or more positions, or one left after removals), or null
     */
    public IntObjectHashMap<SymbolPositionRecord> getPositionsMap() {
        return positionsMap;
    }

    public void forEachPosition(final Consumer<SymbolPositionRecord> consumer) {
        if (singlePosition != null) {
            consumer.accept(singlePosition);
        } else if (positionsMap != null) {
            positionsMap.forEachValue(consumer::accept);
        }
    }

    public boolean anyPosition(final Predicate<SymbolPositionRecord> predicate) {
        if (singlePosition != null) {
            return predicate.test(singlePosition);
        }
        return positionsMap != null && positionsMap.anySatisfy(predicate::test);
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {

        bytes.writeLong(uidHi);
        bytes.writeLong(uidLo);

        // positions, same format as SerializationUtils.marshallIntHashMap
        if (singlePosition != null) {
            bytes.writeInt(1);
            bytes.writeInt(singlePosition.symbol);
            singlePosition.writeMarshallable(bytes);
        } else if (positionsMap != null) {
            SerializationUtils.marshallIntHashMap(positionsMap, bytes);
        } else {
            bytes.writeInt(0);
        }

        // adjustmentsCounter
        bytes.writeLong(adjustmentsCounter);

        // account balances
        SerializationUtils.marshallIntLongHashtable(accounts, bytes);

        // suspended
        bytes.writeByte(userStatus.getCode());
    }


    // allocates; for reports and logs
    public UUID uid() {
        return new UUID(uidHi, uidLo);
    }

    @Override
    public String toString() {
        return "UserProfile{" +
                "uid=" + uid() +
                ", positions=" + positionsCount() +
                ", accounts=" + accounts +
                ", adjustmentsCounter=" + adjustmentsCounter +
                ", userStatus=" + userStatus +
                '}';
    }

    @Override
    public int stateHash() {
        return Objects.hash(
                uidHi,
                uidLo,
                positionsStateHash(),
                adjustmentsCounter,
                copyAccounts().hashCode(),
                userStatus.hashCode());
    }

    // same value as HashingUtils.stateHash(IntObjectHashMap) over all positions
    private int positionsStateHash() {
        if (singlePosition != null) {
            return Long.hashCode(Objects.hash(singlePosition.symbol, singlePosition.stateHash()));
        }
        return positionsMap != null ? HashingUtils.stateHash(positionsMap) : Long.hashCode(0L);
    }
}
