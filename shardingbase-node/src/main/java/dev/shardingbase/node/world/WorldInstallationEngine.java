package dev.shardingbase.node.world;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;

/** Copies, verifies, and atomically swaps one prepared shard tree into its world path. */
public final class WorldInstallationEngine {
    private WorldInstallationEngine() {
    }

    public static InstalledWorld install(
        final Path worldRoot,
        final Path preparedTree,
        final Path transactionDirectory,
        final String role,
        final ShardManifestWriter.Manifest shardManifest
    ) throws IOException {
        if (!role.matches("[a-z-]{2,32}")) {
            throw new IllegalArgumentException("Invalid world installation role");
        }
        final Path world = worldRoot.toAbsolutePath().normalize();
        final Path prepared = preparedTree.toAbsolutePath().normalize();
        final Path transaction = transactionDirectory.toAbsolutePath().normalize();
        if (!Files.isDirectory(prepared) || !Files.isDirectory(transaction)
            || world.equals(prepared) || world.equals(transaction)
            || prepared.startsWith(world) || world.startsWith(prepared)) {
            throw new IOException("World installation paths are invalid or nested");
        }
        final Path parent = world.getParent();
        if (parent == null || Files.notExists(parent)) {
            throw new IOException("World installation parent does not exist");
        }
        if (!Files.getFileStore(parent).equals(Files.getFileStore(transaction))) {
            throw new IOException("Transaction and world roots must use the same file store for atomic commit");
        }
        TransferTreeManifest.verify(prepared);
        final Path install = transaction.resolve(role + "-install");
        final Path retired = transaction.resolve(role + "-original");
        if (Files.exists(install) || Files.exists(retired)) {
            throw new IOException("World installation staging or retired path already exists");
        }
        copyTree(prepared, install);
        TransferTreeManifest.verify(install);
        ShardManifestWriter.write(install, shardManifest);
        final boolean existed = Files.exists(world);
        try {
            if (existed) {
                atomicMove(world, retired);
            }
            try {
                atomicMove(install, world);
            } catch (final IOException exception) {
                if (existed && Files.exists(retired) && Files.notExists(world)) {
                    atomicMove(retired, world);
                }
                throw exception;
            }
            return new InstalledWorld(world, existed ? retired : null);
        } catch (final IOException exception) {
            deleteTree(install);
            throw exception;
        }
    }

    public static void rollback(
        final Path worldRoot,
        final Path retiredWorld,
        final boolean worldInitiallyAbsent,
        final Path failedRoot
    ) throws IOException {
        final Path world = worldRoot.toAbsolutePath().normalize();
        final Path failed = failedRoot.toAbsolutePath().normalize();
        if (world.equals(failed) || world.startsWith(failed) || failed.startsWith(world)) {
            throw new IOException("Rollback paths are invalid or nested");
        }
        if (Files.exists(world)) {
            if (Files.exists(failed)) {
                throw new IOException("Failed-world diagnostic path already exists");
            }
            atomicMove(world, failed);
        }
        if (worldInitiallyAbsent) {
            return;
        }
        if (retiredWorld == null || Files.notExists(retiredWorld)) {
            throw new IOException("Retired original world is missing during rollback");
        }
        atomicMove(retiredWorld, world);
    }

    private static void copyTree(final Path source, final Path target) throws IOException {
        Files.walkFileTree(source, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(final Path directory, final BasicFileAttributes attributes)
                throws IOException {
                if (Files.isSymbolicLink(directory)) {
                    throw new IOException("Symbolic links are not allowed in a shard installation");
                }
                Files.createDirectories(target.resolve(source.relativize(directory)));
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attributes)
                throws IOException {
                if (Files.isSymbolicLink(file) || !attributes.isRegularFile()) {
                    throw new IOException("Unsupported shard installation entry: " + file);
                }
                Files.copy(
                    file,
                    target.resolve(source.relativize(file)),
                    StandardCopyOption.COPY_ATTRIBUTES
                );
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static void atomicMove(final Path source, final Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (final AtomicMoveNotSupportedException exception) {
            throw new IOException("Atomic world directory moves are required", exception);
        }
    }

    private static void deleteTree(final Path root) throws IOException {
        if (Files.notExists(root)) {
            return;
        }
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attributes) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(final Path directory, final IOException exception)
                throws IOException {
                if (exception != null) {
                    throw exception;
                }
                Files.delete(directory);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public record InstalledWorld(Path path, Path retiredOriginal) {
    }
}
