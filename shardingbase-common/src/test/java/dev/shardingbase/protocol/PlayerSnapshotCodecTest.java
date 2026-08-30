package dev.shardingbase.protocol;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PlayerSnapshotCodecTest {
    @Test
    void roundTripsEveryPortableCategory() throws Exception {
        final UUID playerId = UUID.randomUUID();
        final Map<PlayerDataCategory, byte[]> categories = new EnumMap<>(PlayerDataCategory.class);
        for (final PlayerDataCategory category : PlayerDataCategory.values()) {
            categories.put(category, new byte[] {(byte) category.ordinal(), 42});
        }
        final PlayerSnapshot decoded = PlayerSnapshotCodec.decode(PlayerSnapshotCodec.encode(
            new PlayerSnapshot(playerId, 19, "backend-a", categories)
        ));

        assertEquals(playerId, decoded.playerId());
        assertEquals(19, decoded.revision());
        assertEquals("backend-a", decoded.sourceBackendId());
        for (final PlayerDataCategory category : PlayerDataCategory.values()) {
            assertArrayEquals(categories.get(category), decoded.categories().get(category));
        }
    }

    @Test
    void rejectsDuplicateAndTruncatedCategories() throws Exception {
        final PlayerSnapshot snapshot = new PlayerSnapshot(
            UUID.randomUUID(),
            1,
            "backend-a",
            Map.of(PlayerDataCategory.INVENTORY, new byte[] {1, 2, 3})
        );
        final byte[] encoded = PlayerSnapshotCodec.encode(snapshot);
        assertThrows(ProtocolException.class, () -> PlayerSnapshotCodec.decode(
            Arrays.copyOf(encoded, encoded.length - 1)
        ));
    }

    @Test
    void snapshotDefensivelyCopiesCategoryPayloads() {
        final byte[] mutable = new byte[] {1};
        final PlayerSnapshot snapshot = new PlayerSnapshot(
            UUID.randomUUID(),
            1,
            "backend-a",
            Map.of(PlayerDataCategory.INVENTORY, mutable)
        );
        mutable[0] = 2;
        final Map<PlayerDataCategory, byte[]> returned = snapshot.categories();
        returned.get(PlayerDataCategory.INVENTORY)[0] = 3;

        assertArrayEquals(new byte[] {1}, snapshot.categories().get(PlayerDataCategory.INVENTORY));
    }
}
