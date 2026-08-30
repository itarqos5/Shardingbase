package dev.shardingbase.node.world;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Properties;
import java.util.UUID;

/** Atomically writes the ownership manifest consumed before backend plugin loading. */
public final class ShardManifestWriter {
    public static final String FILE_NAME = "shardingbase-shard.properties";

    private ShardManifestWriter() {
    }

    public static Path write(final Path worldRoot, final Manifest manifest) throws IOException {
        final Path root = worldRoot.toAbsolutePath().normalize();
        if (!Files.isDirectory(root)) {
            throw new IOException("Shard manifest world root is not a directory: " + root);
        }
        final Path target = root.resolve(FILE_NAME);
        final Properties properties = new Properties();
        properties.setProperty("format-version", "1");
        properties.setProperty("world-key", manifest.worldKey());
        properties.setProperty("transaction-id", manifest.transactionId().toString());
        properties.setProperty("axis", manifest.axis().name());
        properties.setProperty("cut-chunk", Integer.toString(manifest.cutChunk()));
        properties.setProperty("owned-side", manifest.ownedSide().name());
        properties.setProperty("peer-id", manifest.peerId());
        final Path temporary = Files.createTempFile(root, ".shardingbase-manifest-", ".tmp");
        try {
            try (OutputStream output = Files.newOutputStream(
                temporary,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
            )) {
                properties.store(output, "Shardingbase immutable shard ownership");
            }
            try {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (final AtomicMoveNotSupportedException exception) {
                throw new IOException("Atomic shard manifest replacement is not supported", exception);
            }
            return target;
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    public record Manifest(
        String worldKey,
        UUID transactionId,
        ShardAxis axis,
        int cutChunk,
        ShardSide ownedSide,
        String peerId
    ) {
        public Manifest {
            if (worldKey == null || worldKey.isBlank() || transactionId == null || axis == null || ownedSide == null
                || peerId == null || peerId.isBlank()) {
                throw new IllegalArgumentException("Shard manifest fields are required");
            }
        }
    }
}
