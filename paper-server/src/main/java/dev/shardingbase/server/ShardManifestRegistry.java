package dev.shardingbase.server;

import dev.shardingbase.api.Ownership;
import dev.shardingbase.api.WorldPosition;
import dev.shardingbase.server.config.ShardingbaseConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/** Strict ownership manifests discovered in direct world directories. */
final class ShardManifestRegistry {
    static final String FILE_NAME = "shardingbase-shard.properties";
    private static final Set<String> FIELDS = Set.of(
        "format-version",
        "world-key",
        "world-id",
        "transaction-id",
        "axis",
        "cut-chunk",
        "owned-side",
        "peer-id"
    );

    private final Map<String, Manifest> manifests;

    private ShardManifestRegistry(final Map<String, Manifest> manifests) {
        this.manifests = Map.copyOf(manifests);
    }

    static ShardManifestRegistry empty() {
        return new ShardManifestRegistry(Map.of());
    }

    static ShardManifestRegistry load(final Path serverDirectory) throws ShardingbaseConfigurationException {
        final Map<String, Manifest> manifests = new HashMap<>();
        try (var children = Files.list(serverDirectory.toAbsolutePath().normalize())) {
            for (final Path child : children.filter(Files::isDirectory).toList()) {
                final Path path = child.resolve(FILE_NAME);
                if (Files.notExists(path)) {
                    continue;
                }
                final Manifest manifest = read(path);
                if (manifests.put(manifest.worldKey(), manifest) != null) {
                    throw new ShardingbaseConfigurationException(
                        "Duplicate Shardingbase shard manifest for world " + manifest.worldKey()
                    );
                }
            }
            return new ShardManifestRegistry(manifests);
        } catch (final IOException exception) {
            throw new ShardingbaseConfigurationException("Unable to discover Shardingbase shard manifests", exception);
        }
    }

    boolean isSharded() {
        return !this.manifests.isEmpty();
    }

    Ownership ownership(final WorldPosition position) {
        final Manifest manifest = this.manifests.get(position.worldKey());
        if (manifest == null) {
            return Ownership.UNSHARDED;
        }
        final int coordinate = manifest.axis() == Axis.X
            ? Math.floorDiv(position.x(), 16)
            : Math.floorDiv(position.z(), 16);
        final boolean negative = coordinate < manifest.cutChunk();
        return negative == (manifest.ownedSide() == Side.NEGATIVE) ? Ownership.LOCAL : Ownership.REMOTE;
    }

    Optional<Boundary> boundary(final String worldKey) {
        final Manifest manifest = this.manifests.get(worldKey);
        return manifest == null ? Optional.empty() : Optional.of(new Boundary(
            manifest.worldKey(),
            manifest.worldId(),
            manifest.transactionId(),
            manifest.axis(),
            manifest.cutChunk(),
            manifest.ownedSide(),
            manifest.peerId()
        ));
    }

    private static Manifest read(final Path path) throws ShardingbaseConfigurationException {
        final Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(path)) {
            properties.load(input);
        } catch (final IOException exception) {
            throw new ShardingbaseConfigurationException("Unable to read shard manifest " + path, exception);
        }
        if (!properties.stringPropertyNames().equals(FIELDS)) {
            throw new ShardingbaseConfigurationException("Shard manifest has missing or unknown fields: " + path);
        }
        try {
            if (!"1".equals(properties.getProperty("format-version"))) {
                throw new IllegalArgumentException("unsupported format-version");
            }
            final String worldKey = nonBlank(properties, "world-key");
            final UUID worldId = UUID.fromString(properties.getProperty("world-id"));
            final UUID transactionId = UUID.fromString(properties.getProperty("transaction-id"));
            final Axis axis = Axis.valueOf(properties.getProperty("axis"));
            final int cutChunk = Integer.parseInt(properties.getProperty("cut-chunk"));
            final Side side = Side.valueOf(properties.getProperty("owned-side"));
            final String peerId = nonBlank(properties, "peer-id");
            return new Manifest(worldKey, worldId, transactionId, axis, cutChunk, side, peerId);
        } catch (final IllegalArgumentException | NullPointerException exception) {
            throw new ShardingbaseConfigurationException("Invalid shard manifest " + path + ": " + exception.getMessage(), exception);
        }
    }

    private static String nonBlank(final Properties properties, final String key) {
        final String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(key + " must not be blank");
        }
        return value;
    }

    enum Axis {
        X,
        Z
    }

    enum Side {
        NEGATIVE,
        POSITIVE
    }

    private record Manifest(
        String worldKey,
        UUID worldId,
        UUID transactionId,
        Axis axis,
        int cutChunk,
        Side ownedSide,
        String peerId
    ) {
    }

    record Boundary(
        String worldKey,
        UUID worldId,
        UUID transactionId,
        Axis axis,
        int cutChunk,
        Side ownedSide,
        String peerId
    ) {
        long cutBlock() {
            return (long) this.cutChunk * 16L;
        }

        boolean owns(final int blockX, final int blockZ) {
            final int chunk = Math.floorDiv(this.axis == Axis.X ? blockX : blockZ, 16);
            final boolean negative = chunk < this.cutChunk;
            return negative == (this.ownedSide == Side.NEGATIVE);
        }
    }
}
