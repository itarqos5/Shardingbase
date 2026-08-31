package dev.shardingbase.server;

import dev.shardingbase.api.Ownership;
import dev.shardingbase.api.WorldPosition;
import dev.shardingbase.server.config.ShardingbaseConfigurationException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.lang.reflect.Proxy;
import java.util.UUID;
import org.bukkit.Location;
import org.bukkit.World;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardManifestRegistryTest {
    @Test
    void resolvesPositiveNegativeAndUnshardedOwnership(@TempDir final Path directory) throws Exception {
        writeManifest(directory.resolve("world"), "minecraft:overworld", "X", "-2", "NEGATIVE");
        final ShardManifestRegistry registry = ShardManifestRegistry.load(directory);

        assertTrue(registry.isSharded());
        assertEquals(Ownership.LOCAL, registry.ownership(position("minecraft:overworld", -33, 0)));
        assertEquals(Ownership.REMOTE, registry.ownership(position("minecraft:overworld", -32, 0)));
        assertEquals(Ownership.UNSHARDED, registry.ownership(position("minecraft:the_nether", -33, 0)));
    }

    @Test
    void resolvesZAxisAndRejectsAmbiguousManifests(@TempDir final Path directory) throws Exception {
        writeManifest(directory.resolve("world"), "minecraft:overworld", "Z", "1", "POSITIVE");
        final ShardManifestRegistry registry = ShardManifestRegistry.load(directory);
        assertEquals(Ownership.REMOTE, registry.ownership(position("minecraft:overworld", 0, 15)));
        assertEquals(Ownership.LOCAL, registry.ownership(position("minecraft:overworld", 0, 16)));

        writeManifest(directory.resolve("world-copy"), "minecraft:overworld", "Z", "1", "NEGATIVE");
        assertThrows(ShardingbaseConfigurationException.class, () -> ShardManifestRegistry.load(directory));
    }

    @Test
    void exposesExactRuntimeBoundary(@TempDir final Path directory) throws Exception {
        writeManifest(directory.resolve("world"), "minecraft:overworld", "X", "-2", "POSITIVE");
        final ShardManifestRegistry.Boundary boundary = ShardManifestRegistry.load(directory)
            .boundary("minecraft:overworld")
            .orElseThrow();
        assertEquals(-32L, boundary.cutBlock());
        assertFalse(boundary.owns(-33, 0));
        assertTrue(boundary.owns(-32, 0));
    }

    @Test
    void discoversManifestInModernPaperDimensionLayout(@TempDir final Path directory) throws Exception {
        writeManifest(
            directory.resolve("world/dimensions/minecraft/overworld"),
            "minecraft:overworld",
            "X",
            "1562",
            "NEGATIVE"
        );

        final ShardManifestRegistry.Boundary boundary = ShardManifestRegistry.load(directory)
            .boundary("minecraft:overworld")
            .orElseThrow();

        assertEquals(24_992L, boundary.cutBlock());
    }

    @Test
    void preservesGlobalCoordinatesAcrossAPositiveCut(@TempDir final Path directory) throws Exception {
        writeManifest(directory.resolve("world"), "minecraft:overworld", "X", "1562", "NEGATIVE");
        final ShardManifestRegistry.Boundary boundary = ShardManifestRegistry.load(directory)
            .boundary("minecraft:overworld")
            .orElseThrow();

        assertEquals(24_992L, boundary.cutBlock());
        assertTrue(boundary.owns(24_991, 0));
        assertFalse(boundary.owns(24_992, 0));
        assertFalse(boundary.owns(25_000, 0));
    }

    @Test
    void detectsWalkingTowardTheCutWithoutTreatingTeleportsAsCrossings() {
        final World world = emptyWorld();
        final ShardManifestRegistry.Boundary boundary = new ShardManifestRegistry.Boundary(
            "minecraft:overworld",
            UUID.randomUUID(),
            UUID.randomUUID(),
            ShardManifestRegistry.Axis.X,
            1_562,
            ShardManifestRegistry.Side.NEGATIVE,
            "peer-a"
        );

        assertTrue(ShardBoundaryCoordinator.approachingBoundary(
            boundary,
            new Location(world, 24_991.4, 64.0, 0.0),
            new Location(world, 24_991.0, 64.0, 0.0)
        ));
        assertFalse(ShardBoundaryCoordinator.approachingBoundary(
            boundary,
            new Location(world, 24_991.4, 64.0, 0.0),
            new Location(world, 100.0, 64.0, 0.0)
        ));
    }

    private static WorldPosition position(final String world, final int x, final int z) {
        return new WorldPosition(world, x, 64, z);
    }

    private static World emptyWorld() {
        return (World) Proxy.newProxyInstance(
            World.class.getClassLoader(),
            new Class<?>[] {World.class},
            (proxy, method, arguments) -> method.getReturnType().isPrimitive() ? 0 : null
        );
    }

    private static void writeManifest(
        final Path world,
        final String worldKey,
        final String axis,
        final String cutChunk,
        final String side
    ) throws Exception {
        Files.createDirectories(world);
        Files.writeString(world.resolve(ShardManifestRegistry.FILE_NAME), """
            format-version=1
            world-key=%s
            world-id=080d7407-075f-4d7a-888b-e4c30d12fbc4
            transaction-id=8ef6e718-b766-465f-9df2-7827f2577682
            axis=%s
            cut-chunk=%s
            owned-side=%s
            peer-id=peer-a
            """.formatted(worldKey, axis, cutChunk, side), StandardCharsets.UTF_8);
    }
}
