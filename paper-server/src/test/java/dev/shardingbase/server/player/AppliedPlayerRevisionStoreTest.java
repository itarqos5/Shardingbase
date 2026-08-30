package dev.shardingbase.server.player;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AppliedPlayerRevisionStoreTest {
    @Test
    void persistsMonotonicAppliedRevisions(@TempDir final Path directory) throws Exception {
        final UUID playerId = UUID.randomUUID();
        final AppliedPlayerRevisionStore store = new AppliedPlayerRevisionStore(directory);
        assertTrue(store.shouldApply(playerId, 4));

        store.markApplied(playerId, 4);

        final AppliedPlayerRevisionStore reopened = new AppliedPlayerRevisionStore(directory);
        assertFalse(reopened.shouldApply(playerId, 3));
        assertFalse(reopened.shouldApply(playerId, 4));
        assertTrue(reopened.shouldApply(playerId, 5));
    }
}
