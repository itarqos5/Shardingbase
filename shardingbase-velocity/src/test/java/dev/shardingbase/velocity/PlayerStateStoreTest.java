package dev.shardingbase.velocity;

import dev.shardingbase.protocol.PlayerDataCategory;
import dev.shardingbase.protocol.PlayerHandoffCodec;
import dev.shardingbase.protocol.PlayerSnapshot;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PlayerStateStoreTest {
    @Test
    void assignsMonotonicRevisionsAndPersistsThem(@TempDir final Path directory) throws Exception {
        final Path database = directory.resolve("shardingbase.db");
        final PlayerStateStore store = new PlayerStateStore(database);
        final UUID playerId = UUID.randomUUID();

        assertEquals(1, store.storeNext(playerId, "backend-a", new byte[] {1}).revision());
        assertEquals(2, store.storeNext(playerId, "backend-b", new byte[] {2}).revision());

        final PlayerStateStore.StoredSnapshot reloaded = new PlayerStateStore(database).load(playerId).orElseThrow();
        assertEquals(2, reloaded.revision());
        assertEquals("backend-b", reloaded.sourceBackendId());
        assertArrayEquals(new byte[] {2}, reloaded.snapshot());
    }

    @Test
    void ignoresStaleAndDuplicateRetriesAndRejectsRevisionConflicts(@TempDir final Path directory) throws Exception {
        final PlayerStateStore store = new PlayerStateStore(directory.resolve("shardingbase.db"));
        final UUID playerId = UUID.randomUUID();

        assertEquals(PlayerStateStore.StageResult.STORED,
            store.acceptRevision(playerId, 5, "backend-a", new byte[] {5}));
        assertEquals(PlayerStateStore.StageResult.DUPLICATE,
            store.acceptRevision(playerId, 5, "backend-a", new byte[] {5}));
        assertEquals(PlayerStateStore.StageResult.CONFLICT,
            store.acceptRevision(playerId, 5, "backend-a", new byte[] {9}));
        assertEquals(PlayerStateStore.StageResult.STALE,
            store.acceptRevision(playerId, 4, "backend-a", new byte[] {4}));
        assertEquals(5, store.load(playerId).orElseThrow().revision());
    }

    @Test
    void reservesRevisionsAcrossRetriesAndStoredSnapshots(@TempDir final Path directory) throws Exception {
        final PlayerStateStore store = new PlayerStateStore(directory.resolve("shardingbase.db"));
        final UUID playerId = UUID.randomUUID();
        assertEquals(1, store.reserveRevision(playerId));
        assertEquals(2, store.reserveRevision(playerId));
        assertEquals(PlayerStateStore.StageResult.STORED,
            store.acceptRevision(playerId, 2, "backend-a", new byte[] {2}));
        assertEquals(3, new PlayerStateStore(directory.resolve("shardingbase.db")).reserveRevision(playerId));
    }

    @Test
    void awaitsTheExactTargetRevision(@TempDir final Path directory) throws Exception {
        final PlayerStateStore store = new PlayerStateStore(directory.resolve("shardingbase.db"));
        final UUID playerId = UUID.randomUUID();
        final Thread writer = Thread.ofPlatform().start(() -> {
            try {
                Thread.sleep(50);
                final PlayerHandoffCodec.Stage stage = new PlayerHandoffCodec.Stage(
                    "backend-b",
                    new PlayerSnapshot(
                        playerId,
                        8,
                        "backend-a",
                        Map.of(PlayerDataCategory.INVENTORY, new byte[] {1})
                    )
                );
                store.acceptRevision(
                    playerId,
                    8,
                    "backend-a",
                    PlayerHandoffCodec.encodeStage(stage)
                );
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        });

        assertTrue(store.awaitStage(playerId, 8, "backend-b", 2_000));
        writer.join();
    }

    @Test
    void persistsNetworkWideCategorySelection(@TempDir final Path directory) throws Exception {
        final Path database = directory.resolve("shardingbase.db");
        final PlayerStateStore store = new PlayerStateStore(database);
        assertEquals(EnumSet.allOf(PlayerDataCategory.class), store.categories());

        final var selected = EnumSet.of(PlayerDataCategory.INVENTORY, PlayerDataCategory.HEALTH);
        store.categories(selected);

        assertEquals(selected, new PlayerStateStore(database).categories());
    }
}
