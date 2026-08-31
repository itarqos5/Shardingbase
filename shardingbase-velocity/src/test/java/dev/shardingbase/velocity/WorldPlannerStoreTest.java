package dev.shardingbase.velocity;

import dev.shardingbase.protocol.MapPlannerCodec;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorldPlannerStoreTest {
    @Test
    void redeemsLinkOnceAndPersistsOneImmutableActivePlan(@TempDir final Path directory) throws Exception {
        final WorldPlannerStore store = new WorldPlannerStore(directory.resolve("shardingbase.db"));
        final UUID sessionId = UUID.randomUUID();
        store.create(new MapPlannerCodec.Create(
            sessionId, "backend-a", "minecraft:overworld", "world", UUID.randomUUID(), 42L, 4671,
            -10, 20, -5, 30, 50, 80_000
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
        assertEquals("PLANNED", store.transaction(transactionId).orElseThrow().state());
        store.transition(transactionId, "PLANNED", "PREFLIGHTING", "checking nodes");
        assertEquals("PREFLIGHTING", store.transaction(transactionId).orElseThrow().state());
        assertEquals(1, store.transactionsIn("PREFLIGHTING").size());
        assertThrows(Exception.class, () -> store.confirm(
            redeemed.session(), "Z", 2, "backend-b", "backend-a"
        ));
    }

    @Test
    void migratesExistingMapSessionsWithAuthoritativeWorldIdentity(@TempDir final Path directory)
        throws Exception {
        final Path database = directory.resolve("shardingbase.db");
        try (
            var connection = DriverManager.getConnection("jdbc:sqlite:" + database);
            var statement = connection.createStatement()
        ) {
            statement.executeUpdate("""
                CREATE TABLE map_sessions (
                    session_id TEXT PRIMARY KEY NOT NULL,
                    backend_id TEXT NOT NULL,
                    world_key TEXT NOT NULL,
                    min_chunk_x INTEGER NOT NULL,
                    max_chunk_x INTEGER NOT NULL,
                    min_chunk_z INTEGER NOT NULL,
                    max_chunk_z INTEGER NOT NULL,
                    generated_chunks INTEGER NOT NULL,
                    estimated_bytes INTEGER NOT NULL,
                    link_token_hash BLOB,
                    browser_token_hash BLOB,
                    state TEXT NOT NULL,
                    created_epoch_ms INTEGER NOT NULL
                )
                """);
        }

        final WorldPlannerStore migrated = new WorldPlannerStore(database);
        final UUID sessionId = UUID.randomUUID();
        final UUID worldId = UUID.randomUUID();
        migrated.create(new MapPlannerCodec.Create(
            sessionId, "backend-a", "minecraft:overworld", "world", worldId, 987L, 4671,
            -2, 2, -2, 2, 12, 1_024L
        ));
        final WorldPlannerStore.Redeemed redeemed =
            migrated.redeem(migrated.complete(sessionId)).orElseThrow();

        assertEquals("world", redeemed.session().worldDirectory());
        assertEquals(worldId, redeemed.session().worldId());
        assertEquals(987L, redeemed.session().worldSeed());
        assertEquals(4671, redeemed.session().dataVersion());
    }
}
