package dev.shardingbase.node.world;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.UUID;

/** Creates a complete hash-manifested backup before any world mutation is allowed. */
public final class WorldBackupEngine {
    private static final String MANIFEST_NAME = "manifest.sha256";

    private WorldBackupEngine() {
    }

    public static BackupResult backup(final Path worldRoot, final Path backupRoot, final UUID transactionId)
        throws IOException {
        final Path source = worldRoot.toAbsolutePath().normalize();
        final Path root = backupRoot.toAbsolutePath().normalize();
        if (!Files.isDirectory(source) || source.equals(root) || root.startsWith(source) || source.startsWith(root)) {
            throw new IOException("World and backup roots must be existing, distinct, non-nested directories");
        }
        Files.createDirectories(root);
        final long sourceBytes = sizeAndRejectLinks(source);
        final long safetyMargin = Math.addExact(sourceBytes, Math.floorDiv(sourceBytes, 5));
        if (Files.getFileStore(root).getUsableSpace() < safetyMargin) {
            throw new IOException("Backup root does not have the required 20% free-space margin");
        }

        final Path destination = root.resolve(transactionId.toString()).normalize();
        final Path staging = root.resolve("." + transactionId + ".staging").normalize();
        if (!destination.startsWith(root) || !staging.startsWith(root) || Files.exists(destination) || Files.exists(staging)) {
            throw new IOException("Backup destination already exists or escapes the configured backup root");
        }

        final StringBuilder manifest = new StringBuilder();
        try {
            Files.walkFileTree(source, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(final Path directory, final BasicFileAttributes attributes)
                    throws IOException {
                    rejectLink(directory);
                    Files.createDirectories(staging.resolve(source.relativize(directory)));
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(final Path file, final BasicFileAttributes attributes) throws IOException {
                    rejectLink(file);
                    if (!attributes.isRegularFile()) {
                        throw new IOException("Unsupported non-regular world entry: " + file);
                    }
                    final Path relative = source.relativize(file);
                    final Path target = staging.resolve(relative);
                    Files.copy(file, target, StandardCopyOption.COPY_ATTRIBUTES);
                    manifest.append(sha256(target)).append("  ")
                        .append(relative.toString().replace('\\', '/')).append('\n');
                    return FileVisitResult.CONTINUE;
                }
            });
            Files.writeString(staging.resolve(MANIFEST_NAME), manifest.toString(), StandardCharsets.UTF_8);
            try {
                Files.move(staging, destination, StandardCopyOption.ATOMIC_MOVE);
            } catch (final AtomicMoveNotSupportedException exception) {
                throw new IOException("Atomic backup publication is not supported", exception);
            }
            return new BackupResult(destination, sourceBytes, manifest.toString().lines().count());
        } catch (final IOException | RuntimeException exception) {
            deleteStaging(staging, root);
            throw exception;
        }
    }

    private static long sizeAndRejectLinks(final Path source) throws IOException {
        final long[] size = {0};
        Files.walkFileTree(source, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(final Path directory, final BasicFileAttributes attributes)
                throws IOException {
                rejectLink(directory);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attributes) throws IOException {
                rejectLink(file);
                if (!attributes.isRegularFile()) {
                    throw new IOException("Unsupported non-regular world entry: " + file);
                }
                size[0] = Math.addExact(size[0], attributes.size());
                return FileVisitResult.CONTINUE;
            }
        });
        return size[0];
    }

    private static void rejectLink(final Path path) throws IOException {
        if (Files.isSymbolicLink(path)) {
            throw new IOException("Symbolic links are not allowed in a world transaction: " + path);
        }
    }

    private static String sha256(final Path file) throws IOException {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (var input = Files.newInputStream(file)) {
                final byte[] buffer = new byte[64 * 1024];
                int read;
                while ((read = input.read(buffer)) >= 0) {
                    digest.update(buffer, 0, read);
                }
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (final NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is required", exception);
        }
    }

    private static void deleteStaging(final Path staging, final Path root) throws IOException {
        if (!staging.startsWith(root) || staging.equals(root) || Files.notExists(staging)) {
            return;
        }
        Files.walkFileTree(staging, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attributes) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(final Path directory, final IOException exception) throws IOException {
                if (exception != null) {
                    throw exception;
                }
                Files.delete(directory);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public record BackupResult(Path path, long bytes, long files) {
    }
}
