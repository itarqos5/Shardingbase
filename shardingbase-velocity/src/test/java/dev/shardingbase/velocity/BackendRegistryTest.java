package dev.shardingbase.velocity;

import dev.shardingbase.protocol.ValidationPayloadCodec.ValidationRequest;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BackendRegistryTest {
    @Test
    void registersExactlyTwoMatchingBackendsAndReturnsPeer(@TempDir final Path directory) throws Exception {
        final BackendRegistry registry = new BackendRegistry(directory.resolve("shardingbase.db"));

        final var first = registry.register("node-a", request("id-a", "backend-a", "26.2", "build-a"));
        final var second = registry.register("node-b", request("id-b", "backend-b", "26.2", "build-a"));

        assertFalse(first.accepted());
        assertEquals("", first.peerId());
        assertTrue(second.accepted());
        assertEquals("id-a", second.peerId());
        assertEquals("backend-a", second.peerName());
        assertTrue(registry.register("node-a", request("id-a", "backend-a", "26.2", "build-a")).accepted());
        assertFalse(registry.register("node-c", request("id-c", "backend-c", "26.2", "build-a")).accepted());
        assertEquals("node-a", registry.nodeIdForTarget("id-a").orElseThrow());
        assertEquals("node-a", registry.nodeIdForTarget("backend-a").orElseThrow());
        assertEquals("node-b", registry.nodeIdForTarget("node-b").orElseThrow());
        assertEquals("id-b", registry.peerForName("backend-a").orElseThrow().serverId());
        registry.setPairStatus(java.util.List.of("id-a", "id-b"), "MAINTENANCE", "world cut");
        assertTrue(registry.statusForName("backend-a").orElseThrow().maintenance());
        assertEquals("world cut", registry.statusForName("backend-b").orElseThrow().detail());
        assertTrue(registry.register("node-a", request("id-a", "backend-a", "26.2", "build-a")).accepted());
        assertTrue(registry.statusForName("backend-a").orElseThrow().maintenance());
        assertTrue(registry.lastSeen("id-a") > 0L);
        assertTrue(registry.lastSeen("id-b") > 0L);
        registry.clearHealth("id-a");
        assertEquals(0L, registry.lastSeen("id-a"));
        assertTrue(registry.lastSeen("id-b") > 0L);
        assertTrue(registry.register("node-a", request("id-a", "backend-a", "26.2", "build-a")).accepted());
        assertTrue(registry.lastSeen("id-a") > 0L);
        registry.setPairStatus(java.util.List.of("id-a", "id-b"), "ONLINE", "transaction complete");
        assertFalse(registry.statusForName("backend-a").orElseThrow().maintenance());
        assertTrue(registry.nodeIdForTarget("missing").isEmpty());
    }

    @Test
    void rejectsIdentityAndVersionConflicts(@TempDir final Path directory) throws Exception {
        final BackendRegistry registry = new BackendRegistry(directory.resolve("shardingbase.db"));
        assertFalse(registry.register("node-a", request("id-a", "backend-a", "26.2", "build-a")).accepted());

        assertFalse(registry.register("node-b", request("id-a", "backend-b", "26.2", "build-a")).accepted());
        assertFalse(registry.register("node-b", request("id-b", "backend-a", "26.2", "build-a")).accepted());
        assertFalse(registry.register("node-b", request("id-b", "backend-b", "26.3", "build-a")).accepted());
        assertFalse(registry.register("node-b", request("id-b", "backend-b", "26.2", "build-b")).accepted());
    }

    private static ValidationRequest request(
        final String id,
        final String name,
        final String minecraftVersion,
        final String shardingbaseVersion
    ) {
        return new ValidationRequest("credential", id, name, minecraftVersion, shardingbaseVersion);
    }
}
