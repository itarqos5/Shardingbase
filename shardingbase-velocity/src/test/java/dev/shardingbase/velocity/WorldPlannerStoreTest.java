package dev.shardingbase.velocity;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.shardingbase.protocol.MapPlannerCodec;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WorldPlannerStoreTest {
    @Test
    void redeemsLinkOnceAndPersistsOneImmutableActivePlan(@TempDir final Path directory) throws Exception {
        final WorldPlannerStore store = new WorldPlannerStore(directory.resolve("shardingbase.db"));
        final UUID sessionId = UUID.randomUUID();
        store.create(new MapPlannerCodec.Create(
            sessionId, "backend-a", "minecraft:overworld", -10, 20, -5, 30, 50, 80_000
        ));
        store.putTile(new MapPlannerCodec.Tile(sessionId, -1, 0, new byte[] {1, 2, 3}));
        assertArrayEquals(new byte[] {1, 2, 3}, store.tile(sessionId, -1, 0).orElseThrow());

        final String linkToken = store.complete(sessionId);
        final WorldPlannerStore.Redeemed redeemed = store.redeem(linkToken).orElseThrow();
        assertTrue(store.redeem(linkToken).isEmpty());
        assertEquals(sessionId, store.authenticate(sessionId, redeemed.browserToken()).orElseThrow().sessionId());

        final UUID transactionId = store.confirm(
            redeemed.session(), "X", 4, "backend-a", "backend-b"
        );
        assertTrue(transactionId.toString().length() > 30);
        assertThrows(Exception.class, () -> store.confirm(
            redeemed.session(), "Z", 2, "backend-b", "backend-a"
        ));
    }
}
