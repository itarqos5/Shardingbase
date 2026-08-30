package dev.shardingbase.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class MapPlannerCodecTest {
    @Test
    void roundTripsIncrementalMapSession() throws Exception {
        final UUID id = UUID.randomUUID();
        final MapPlannerCodec.Create create = new MapPlannerCodec.Create(
            id, "backend-a", "minecraft:overworld", "world", UUID.randomUUID(), 42L, 4671,
            -33, 40, -2, 8, 201, 40_000L
        );
        assertEquals(create, MapPlannerCodec.decodeCreate(MapPlannerCodec.encodeCreate(create)));
        final MapPlannerCodec.Tile tile = new MapPlannerCodec.Tile(id, -1, 2, new byte[] {1, 2, 3});
        final MapPlannerCodec.Tile decoded = MapPlannerCodec.decodeTile(MapPlannerCodec.encodeTile(tile));
        assertEquals(tile.sessionId(), decoded.sessionId());
        assertEquals(tile.tileX(), decoded.tileX());
        assertEquals(tile.tileZ(), decoded.tileZ());
        assertArrayEquals(tile.png(), decoded.png());
        assertEquals(id, MapPlannerCodec.decodeSessionId(MapPlannerCodec.encodeSessionId(id)));
    }
}
