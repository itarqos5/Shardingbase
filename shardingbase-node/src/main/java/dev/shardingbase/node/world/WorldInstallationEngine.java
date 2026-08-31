package dev.shardingbase.node.world;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Base64;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

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
        writeIntent(world, transaction, role, existed);
        final InstalledWorld result = commit(world, install, retired, existed);
        ContainerMetadataEngine.install(world, transaction, role);
        writeIntent(world, transaction, role, existed, InstallPhase.COMMITTED);
        return result;
    }

    /**
     * Recovers an install whose durable intent may have been interrupted between atomic moves.
     *
     * @param worldRoot            destination world root
     * @param transactionDirectory durable transaction directory
     * @param role                 installation role
     * @return recovered committed installation, or empty when no intent exists or the original was restored
     * @throws IOException when the durable state is invalid or cannot be recovered
     */
    public static Optional<InstalledWorld> recover(
        final Path worldRoot,
        final Path transactionDirectory,
        final String role
    ) throws IOException {
        final Path world = worldRoot.toAbsolutePath().normalize();
        final Path transaction = transactionDirectory.toAbsolutePath().normalize();
        final InstallIntent intent = readIntent(transaction, role);
        if (intent == null) {
            return Optional.empty();
        }
        if (!world.toString().equals(intent.world())) {
            throw new IOException("World installation intent targets a different world path");
        }
        if (intent.phase() == InstallPhase.ROLLED_BACK) {
            return Optional.empty();
        }
        final Path install = transaction.resolve(role + "-install");
        final Path retired = transaction.resolve(role + "-original");
        if (intent.phase() == InstallPhase.ROLLING_BACK) {
            final Path failed = transaction.resolve(role + "-failed");
            rollbackIntent(world, transaction, role, intent, install, retired, failed);
            writeIntent(world, transaction, role, intent.initiallyExisted(), InstallPhase.ROLLED_BACK);
            return Optional.empty();
        }
        if (Files.exists(install)) {
            TransferTreeManifest.verify(install, Set.of(ShardManifestWriter.FILE_NAME));
        }
        if (Files.notExists(world) && Files.notExists(install)) {
            if (intent.initiallyExisted() && Files.exists(retired)) {
                atomicMove(retired, world);
            }
            return Optional.empty();
        }
        final InstalledWorld result = commit(world, install, retired, intent.initiallyExisted());
        ContainerMetadataEngine.install(world, transaction, role);
        writeIntent(world, transaction, role, intent.initiallyExisted(), InstallPhase.COMMITTED);
        return Optional.of(result);
    }

    private static InstalledWorld commit(
        final Path world,
        final Path install,
        final Path retired,
        final boolean initiallyExisted
    ) throws IOException {
        if (Files.exists(install)) {
            if (Files.exists(world)) {
                if (!initiallyExisted || Files.exists(retired)) {
                    throw new IOException("Ambiguous world installation recovery state");
                }
                atomicMove(world, retired);
            } else if (initiallyExisted && Files.notExists(retired)) {
                throw new IOException("Original world is missing during installation recovery");
            }
            atomicMove(install, world);
        } else if (Files.notExists(world)) {
            throw new IOException("Installed world and prepared tree are both missing");
        }
        return new InstalledWorld(world, initiallyExisted && Files.exists(retired) ? retired : null);
    }

    static void writeIntent(
        final Path world,
        final Path transaction,
        final String role,
        final boolean initiallyExisted
    ) throws IOException {
        writeIntent(world, transaction, role, initiallyExisted, InstallPhase.INSTALLING);
    }

    private static void writeIntent(
        final Path world,
        final Path transaction,
        final String role,
        final boolean initiallyExisted,
        final InstallPhase phase
    ) throws IOException {
        final Path target = transaction.resolve(role + "-install.intent");
        final Path temporary = Files.createTempFile(transaction, "." + role + "-intent-", ".tmp");
        final String encodedWorld = Base64.getUrlEncoder().withoutPadding().encodeToString(
            world.toAbsolutePath().normalize().toString().getBytes(StandardCharsets.UTF_8)
        );
        final String content = "format-version=1\n"
            + "role=" + role + '\n'
            + "world-base64=" + encodedWorld + '\n'
            + "initially-existed=" + initiallyExisted + '\n'
            + "phase=" + phase.name() + '\n';
        try (FileChannel channel = FileChannel.open(
            temporary,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        )) {
            final var buffer = StandardCharsets.UTF_8.encode(content);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }
        try {
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static InstallIntent readIntent(final Path transaction, final String role) throws IOException {
        final Path path = transaction.resolve(role + "-install.intent");
        if (Files.notExists(path)) {
            return null;
        }
        if (Files.isSymbolicLink(path) || !Files.isRegularFile(path)) {
            throw new IOException("World installation intent is not a regular file");
        }
        final Properties properties = new Properties();
        try (var input = Files.newInputStream(path)) {
            properties.load(input);
        }
        if (!properties.stringPropertyNames().equals(Set.of(
            "format-version", "role", "world-base64", "initially-existed", "phase"
        )) || !"1".equals(properties.getProperty("format-version"))
            || !role.equals(properties.getProperty("role"))
            || !("true".equals(properties.getProperty("initially-existed"))
                || "false".equals(properties.getProperty("initially-existed")))) {
            throw new IOException("World installation intent is invalid");
        }
        final String world;
        final InstallPhase phase;
        try {
            world = new String(
                Base64.getUrlDecoder().decode(properties.getProperty("world-base64")),
                StandardCharsets.UTF_8
            );
            phase = InstallPhase.valueOf(properties.getProperty("phase"));
        } catch (final IllegalArgumentException | NullPointerException exception) {
            throw new IOException("World installation intent path or phase is invalid", exception);
        }
        return new InstallIntent(
            world,
            Boolean.parseBoolean(properties.getProperty("initially-existed")),
            phase
        );
    }

    public static void rollback(
        final Path worldRoot,
        final Path retiredWorld,
        final boolean worldInitiallyAbsent,
        final Path failedRoot,
        final Path transactionDirectory,
        final String role
    ) throws IOException {
        final Path world = worldRoot.toAbsolutePath().normalize();
        final Path failed = failedRoot.toAbsolutePath().normalize();
        final Path transaction = transactionDirectory.toAbsolutePath().normalize();
        if (world.equals(failed) || world.startsWith(failed) || failed.startsWith(world)) {
            throw new IOException("Rollback paths are invalid or nested");
        }
        final Path expectedRetired = transaction.resolve(role + "-original");
        if (retiredWorld != null && !retiredWorld.toAbsolutePath().normalize().equals(expectedRetired)) {
            throw new IOException("Retired world does not match its durable installation intent");
        }
        if (!rollbackPending(world, transaction, role, failed)) {
            rollbackMoves(world, expectedRetired, !worldInitiallyAbsent, failed);
        }
    }

    /**
     * Reverses a durable install directly, including one interrupted during metadata installation.
     *
     * @param worldRoot            installed dimension root
     * @param transactionDirectory durable transaction directory
     * @param role                 installation role
     * @param failedRoot           diagnostic root for the rejected dimension
     * @return true when an install intent existed
     * @throws IOException when the intent cannot be rolled back atomically
     */
    public static boolean rollbackPending(
        final Path worldRoot,
        final Path transactionDirectory,
        final String role,
        final Path failedRoot
    ) throws IOException {
        final Path world = worldRoot.toAbsolutePath().normalize();
        final Path transaction = transactionDirectory.toAbsolutePath().normalize();
        final Path failed = failedRoot.toAbsolutePath().normalize();
        final InstallIntent intent = readIntent(transaction, role);
        if (intent == null) {
            return false;
        }
        if (!world.toString().equals(intent.world())) {
            throw new IOException("World rollback intent targets a different world path");
        }
        if (intent.phase() == InstallPhase.ROLLED_BACK) {
            return true;
        }
        writeIntent(world, transaction, role, intent.initiallyExisted(), InstallPhase.ROLLING_BACK);
        rollbackIntent(
            world,
            transaction,
            role,
            intent,
            transaction.resolve(role + "-install"),
            transaction.resolve(role + "-original"),
            failed
        );
        writeIntent(world, transaction, role, intent.initiallyExisted(), InstallPhase.ROLLED_BACK);
        return true;
    }

    private static void rollbackIntent(
        final Path world,
        final Path transaction,
        final String role,
        final InstallIntent intent,
        final Path install,
        final Path retired,
        final Path failed
    ) throws IOException {
        final Path abortedInstall = transaction.resolve(role + "-aborted-install");
        if (Files.exists(install)) {
            if (Files.exists(abortedInstall)) {
                throw new IOException("Prepared and aborted world installations are both present");
            }
            atomicMove(install, abortedInstall);
            if (Files.exists(retired)) {
                if (Files.exists(world)) {
                    throw new IOException("Original and retired worlds are both present during pre-commit rollback");
                }
                atomicMove(retired, world);
            } else if (intent.initiallyExisted() && Files.notExists(world)) {
                throw new IOException("Original world is missing during pre-commit rollback");
            }
            return;
        }
        if (Files.exists(abortedInstall)) {
            if (Files.exists(retired) && Files.notExists(world)) {
                atomicMove(retired, world);
            }
            return;
        }
        final Path containerFailed = transaction.resolve(role + "-container-failed");
        ContainerMetadataEngine.rollback(world, transaction, role, containerFailed);
        rollbackMoves(world, retired, intent.initiallyExisted(), failed);
    }

    private static void rollbackMoves(
        final Path world,
        final Path retired,
        final boolean initiallyExisted,
        final Path failed
    ) throws IOException {
        if (Files.notExists(failed) && Files.exists(world)) {
            atomicMove(world, failed);
        }
        if (!initiallyExisted) {
            if (Files.exists(world)) {
                throw new IOException("World reappeared while rolling back an initially absent installation");
            }
            return;
        }
        if (Files.exists(retired)) {
            if (Files.exists(world)) {
                throw new IOException("Original and installed worlds are both present during rollback");
            }
            atomicMove(retired, world);
        } else if (Files.notExists(world)) {
            throw new IOException("Retired original world is missing during rollback");
        }
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

    private enum InstallPhase {
        INSTALLING,
        COMMITTED,
        ROLLING_BACK,
        ROLLED_BACK
    }

    private record InstallIntent(String world, boolean initiallyExisted, InstallPhase phase) {
    }
}
