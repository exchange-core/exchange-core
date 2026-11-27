package exchange.core2.core.common;

import exchange.core2.core.utils.HashingUtils;
import exchange.core2.core.utils.SerializationUtils;
import net.openhft.chronicle.bytes.Bytes;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

class UserProfilePositionsTest {

    private static final long UID = 77L;

    private static SymbolPositionRecord position(int symbol, int currency, long openVolume) {
        final SymbolPositionRecord record = new SymbolPositionRecord();
        record.initialize(UID, symbol, currency);
        record.openVolume = openVolume;
        record.direction = PositionDirection.LONG;
        return record;
    }

    private static List<Integer> symbols(UserProfile profile) {
        final List<Integer> symbols = new ArrayList<>();
        profile.forEachPosition(p -> symbols.add(p.symbol));
        symbols.sort(Integer::compare);
        return symbols;
    }

    @Test
    void newProfileHasNoPositionsAndNoMap() {
        final UserProfile profile = new UserProfile(UID, UserStatus.ACTIVE);
        assertEquals(0, profile.positionsCount());
        assertNull(profile.getSinglePosition());
        assertNull(profile.getPositionsMap());
        assertNull(profile.getPosition(1));
        assertFalse(profile.anyPosition(p -> true));
        assertThrows(IllegalStateException.class, () -> profile.getPositionRecordOrThrowEx(1));
    }

    @Test
    void firstPositionStaysOutsideTheMap() {
        final UserProfile profile = new UserProfile(UID, UserStatus.ACTIVE);
        final SymbolPositionRecord a = position(10, 1, 5);
        profile.putPosition(a);

        assertSame(a, profile.getSinglePosition());
        assertNull(profile.getPositionsMap());
        assertSame(a, profile.getPosition(10));
        assertSame(a, profile.getPositionRecordOrThrowEx(10));
        assertNull(profile.getPosition(11));
        assertEquals(1, profile.positionsCount());

        // same symbol replaces, no map
        final SymbolPositionRecord a2 = position(10, 1, 6);
        profile.putPosition(a2);
        assertSame(a2, profile.getSinglePosition());
        assertNull(profile.getPositionsMap());

        profile.removePosition(11);
        assertEquals(1, profile.positionsCount());
        profile.removePosition(10);
        assertEquals(0, profile.positionsCount());
        assertNull(profile.getSinglePosition());
    }

    @Test
    void secondPositionMovesBothIntoTheMapUntilItIsEmpty() {
        final UserProfile profile = new UserProfile(UID, UserStatus.ACTIVE);
        final SymbolPositionRecord a = position(10, 1, 5);
        final SymbolPositionRecord b = position(20, 2, 7);
        profile.putPosition(a);
        profile.putPosition(b);

        assertNull(profile.getSinglePosition());
        assertNotNull(profile.getPositionsMap());
        assertEquals(2, profile.positionsCount());
        assertSame(a, profile.getPosition(10));
        assertSame(b, profile.getPosition(20));
        assertEquals(List.of(10, 20), symbols(profile));
        assertTrue(profile.anyPosition(p -> p.symbol == 20));
        assertFalse(profile.anyPosition(p -> p.symbol == 30));

        // one left: the map stays
        profile.removePosition(10);
        assertEquals(1, profile.positionsCount());
        assertNull(profile.getSinglePosition());
        assertSame(b, profile.getPosition(20));
        assertNull(profile.getPosition(10));

        // empty: the map goes, the next position is single again
        profile.removePosition(20);
        assertEquals(0, profile.positionsCount());
        assertNull(profile.getPositionsMap());
        profile.putPosition(a);
        assertSame(a, profile.getSinglePosition());
        assertNull(profile.getPositionsMap());
    }

    @Test
    void stateHashMatchesTheMapBasedHash() {
        for (int count = 0; count <= 3; count++) {
            final UserProfile profile = new UserProfile(UID, UserStatus.ACTIVE);
            final IntObjectHashMap<SymbolPositionRecord> reference = new IntObjectHashMap<>();
            for (int i = 0; i < count; i++) {
                final SymbolPositionRecord p = position(100 + i, 3, 10 + i);
                profile.putPosition(p);
                reference.put(p.symbol, p);
            }
            final int expected = Objects.hash(
                    UID,
                    HashingUtils.stateHash(reference),
                    0L,
                    new org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap().hashCode(),
                    UserStatus.ACTIVE.hashCode());
            assertEquals(expected, profile.stateHash(), "positions: " + count);
        }
    }

    @Test
    void serializationKeepsTheMapFormat() {
        for (int count = 0; count <= 3; count++) {
            final UserProfile profile = new UserProfile(UID, UserStatus.ACTIVE);
            final IntObjectHashMap<SymbolPositionRecord> reference = new IntObjectHashMap<>();
            for (int i = 0; i < count; i++) {
                final SymbolPositionRecord p = position(200 + i, 4, 20 + i);
                profile.putPosition(p);
                reference.put(p.symbol, p);
            }
            profile.accounts.put(4, 1000L + count);

            final Bytes<?> bytes = Bytes.allocateElasticOnHeap(256);
            final Bytes<?> referenceBytes = Bytes.allocateElasticOnHeap(256);
            try {
                profile.writeMarshallable(bytes);

                // the format written by the map-based version
                referenceBytes.writeLong(UID);
                SerializationUtils.marshallIntHashMap(reference, referenceBytes);
                referenceBytes.writeLong(profile.adjustmentsCounter);
                SerializationUtils.marshallIntLongHashMap(org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap.newWithKeysValues(4, 1000L + count), referenceBytes);
                referenceBytes.writeByte(profile.userStatus.getCode());
                if (count <= 1) {
                    // one entry or none: the same bytes in any order
                    assertEquals(referenceBytes.toHexString(), bytes.toHexString(), "positions: " + count);
                } else {
                    assertEquals(referenceBytes.readRemaining(), bytes.readRemaining(), "positions: " + count);
                }

                final UserProfile restored = new UserProfile(bytes);
                assertEquals(profile.stateHash(), restored.stateHash(), "positions: " + count);
                assertEquals(count, restored.positionsCount());
                assertEquals(count == 1, restored.getSinglePosition() != null);
                assertEquals(symbols(profile), symbols(restored));
                assertEquals(0, bytes.readRemaining());
            } finally {
                bytes.releaseLast();
                referenceBytes.releaseLast();
            }
        }
    }
}
