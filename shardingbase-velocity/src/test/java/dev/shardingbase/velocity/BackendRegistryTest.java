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
