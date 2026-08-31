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
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/** Carries shared 26.2 world-container metadata alongside a nested dimension shard. */
public final class ContainerMetadataEngine {
    static final String BUNDLE_DIRECTORY = ".shardingbase-container";
    private static final List<String> SHARED_ENTRIES = List.of(
        "data", "datapacks", "generated", "icon.png", "level.dat", "level.dat_old"
    );

    private ContainerMetadataEngine() {
    }

    static void seed(final Path worldRoot, final Path negativeRoot, final Path positiveRoot) throws IOException {
        final Path container = container(worldRoot);
        if (container == null) {
            return;
        }
        for (final String name : SHARED_ENTRIES) {
            final Path source = container.resolve(name);
            if (Files.notExists(source)) {
                continue;
            }
            copyEntry(source, negativeRoot.resolve(BUNDLE_DIRECTORY).resolve(name));
            copyEntry(source, positiveRoot.resolve(BUNDLE_DIRECTORY).resolve(name));
        }
    }

    /**
     * Installs or recovers the shared container metadata bundled with an installed nested dimension.
     *
     * @param worldRoot            installed dimension root
     * @param transactionDirectory durable transaction directory
     * @param role                 source or target installation role
     * @throws IOException when metadata cannot be installed atomically
     */
    public static void install(final Path worldRoot, final Path transactionDirectory, final String role)
        throws IOException {
        final Path world = worldRoot.toAbsolutePath().normalize();
        final Path bundle = world.resolve(BUNDLE_DIRECTORY);
        final Path transaction = transactionDirectory.toAbsolutePath().normalize();
        final Path intent = transaction.resolve(role + "-container.intent");
        if ("source".equals(role)) {
            deleteTree(bundle);
            return;
        }
        if (!"target".equals(role)) {
            throw new IOException("Unsupported container metadata installation role");
        }
        final Path container = container(world);
        if (container == null) {
            if (Files.exists(bundle) || Files.exists(intent)) {
                throw new IOException("Container metadata exists for a non-nested world path");
            }
            return;
        }
        final InstallIntent state;
        if (Files.exists(intent)) {
            state = readIntent(intent, container);
        } else if (Files.isDirectory(bundle)) {
            final List<String> entries = bundleEntries(bundle);
            state = new InstallIntent(container, entries);
            writeIntent(intent, state);
        } else {
            return;
        }
        final Path retiredRoot = transaction.resolve(role + "-container-original");
        for (final String name : state.entries()) {
            final Path staged = bundle.resolve(name);
            final Path target = container.resolve(name);
            final Path retired = retiredRoot.resolve(name);
            if (Files.exists(staged)) {
                if (Files.exists(target)) {
                    if (Files.exists(retired)) {
                        throw new IOException("Ambiguous container metadata recovery state for " + name);
                    }
                    Files.createDirectories(retired.getParent());
                    atomicMove(target, retired);
                }
                atomicMove(staged, target);
            } else if (Files.notExists(target)) {
                throw new IOException("Installed and staged container metadata are both missing for " + name);
            }
        }
        deleteTree(bundle);
    }

    /**
     * Restores target container metadata after a rolled-back shard installation.
     *
     * @param worldRoot            installed dimension root
     * @param transactionDirectory durable transaction directory
     * @param role                 installation role
     * @param failedRoot           diagnostic root for rejected metadata
     * @throws IOException when metadata cannot be restored atomically
     */
    public static void rollback(
        final Path worldRoot,
        final Path transactionDirectory,
        final String role,
        final Path failedRoot
    ) throws IOException {
        if (!"target".equals(role)) {
            return;
        }
        final Path transaction = transactionDirectory.toAbsolutePath().normalize();
        final Path intent = transaction.resolve(role + "-container.intent");
        if (Files.notExists(intent)) {
            return;
        }
        final Path container = container(worldRoot);
        if (container == null) {
            throw new IOException("Cannot roll back nested container metadata for a direct world path");
        }
        final InstallIntent state = readIntent(intent, container);
        final Path retiredRoot = transaction.resolve(role + "-container-original");
        final Path failed = failedRoot.toAbsolutePath().normalize();
        final Path bundle = worldRoot.toAbsolutePath().normalize().resolve(BUNDLE_DIRECTORY);
        for (final String name : state.entries()) {
            final Path target = container.resolve(name);
            final Path retired = retiredRoot.resolve(name);
            final Path diagnostic = failed.resolve(name);
            final Path staged = bundle.resolve(name);
            if (Files.exists(staged)) {
                if (Files.exists(retired)) {
                    if (Files.exists(target)) {
                        throw new IOException("Ambiguous uncommitted container metadata state for " + name);
                    }
                    atomicMove(retired, target);
                } else if (Files.notExists(target)) {
                    throw new IOException("Original container metadata is missing before installation for " + name);
                }
                continue;
            }
            if (Files.exists(retired) && Files.exists(target)) {
                if (Files.exists(diagnostic)) {
                    throw new IOException("Ambiguous container metadata rollback state for " + name);
                }
                Files.createDirectories(diagnostic.getParent());
                atomicMove(target, diagnostic);
            } else if (Files.notExists(retired) && Files.notExists(diagnostic) && Files.exists(target)) {
                Files.createDirectories(diagnostic.getParent());
                atomicMove(target, diagnostic);
            }
            if (Files.exists(retired)) {
                atomicMove(retired, target);
            }
        }
    }

    /**
     * Counts shared metadata bytes beside a nested dimension.
     *
     * @param worldRoot dimension root
     * @return shared metadata byte count
     * @throws IOException when metadata cannot be inspected safely
     */
    public static long sourceBytes(final Path worldRoot) throws IOException {
        final Path container = container(worldRoot);
        long total = 0L;
        if (container != null) {
            for (final String name : SHARED_ENTRIES) {
                final Path entry = container.resolve(name);
                if (Files.exists(entry)) {
                    total = Math.addExact(total, entryBytes(entry));
                }
            }
        }
        return total;
    }

    private static Path container(final Path worldRoot) {
        final Path world = worldRoot.toAbsolutePath().normalize();
        Path current = world;
        while (current != null) {
            if (current.getFileName() != null && "dimensions".equals(current.getFileName().toString())) {
                return current.getParent();
            }
            current = current.getParent();
        }
        return null;
    }

    private static List<String> bundleEntries(final Path bundle) throws IOException {
        final List<String> entries = new ArrayList<>();
        try (var paths = Files.list(bundle)) {
            for (final Path entry : paths.sorted().toList()) {
                final String name = entry.getFileName().toString();
                if (!SHARED_ENTRIES.contains(name) || Files.isSymbolicLink(entry)) {
                    throw new IOException("Unsupported container metadata bundle entry: " + entry);
                }
                entries.add(name);
            }
        }
        return List.copyOf(entries);
    }

    private static void writeIntent(final Path path, final InstallIntent intent) throws IOException {
        final Path temporary = Files.createTempFile(path.getParent(), ".container-intent-", ".tmp");
        final String container = Base64.getUrlEncoder().withoutPadding().encodeToString(
            intent.container().toString().getBytes(StandardCharsets.UTF_8)
        );
        final String content = "format-version=1\ncontainer-base64=" + container
            + "\nentries=" + String.join(",", intent.entries()) + '\n';
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
            Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE);
        } catch (final AtomicMoveNotSupportedException exception) {
            throw new IOException("Atomic container metadata intent publication is required", exception);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static InstallIntent readIntent(final Path path, final Path expectedContainer) throws IOException {
        final Properties properties = new Properties();
        try (var input = Files.newInputStream(path)) {
            properties.load(input);
        }
        if (!properties.stringPropertyNames().equals(Set.of(
            "format-version", "container-base64", "entries"
        )) || !"1".equals(properties.getProperty("format-version"))) {
            throw new IOException("Container metadata installation intent is invalid");
        }
        final Path container;
        try {
            container = Path.of(new String(
                Base64.getUrlDecoder().decode(properties.getProperty("container-base64")),
                StandardCharsets.UTF_8
            )).toAbsolutePath().normalize();
        } catch (final IllegalArgumentException exception) {
            throw new IOException("Container metadata installation path is invalid", exception);
        }
        final List<String> entries = properties.getProperty("entries").isEmpty()
            ? List.of()
            : List.of(properties.getProperty("entries").split(",", -1));
        if (!container.equals(expectedContainer.toAbsolutePath().normalize())
            || entries.stream().anyMatch(entry -> !SHARED_ENTRIES.contains(entry))
            || entries.size() != entries.stream().distinct().count()) {
            throw new IOException("Container metadata installation intent does not match this world");
        }
        return new InstallIntent(container, entries);
    }

    private static void copyEntry(final Path source, final Path target) throws IOException {
        if (Files.isSymbolicLink(source)) {
            throw new IOException("Symbolic links are not allowed in container metadata: " + source);
        }
        if (Files.isRegularFile(source)) {
            Files.createDirectories(target.getParent());
            Files.copy(source, target, StandardCopyOption.COPY_ATTRIBUTES);
        } else if (Files.isDirectory(source)) {
            copyTree(source, target);
        } else {
            throw new IOException("Unsupported container metadata entry: " + source);
        }
    }

    private static void copyTree(final Path source, final Path target) throws IOException {
        Files.walkFileTree(source, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(final Path directory, final BasicFileAttributes attributes)
                throws IOException {
                if (Files.isSymbolicLink(directory)) {
                    throw new IOException("Symbolic links are not allowed in container metadata: " + directory);
                }
                Files.createDirectories(target.resolve(source.relativize(directory)));
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attributes)
                throws IOException {
                if (Files.isSymbolicLink(file) || !attributes.isRegularFile()) {
                    throw new IOException("Unsupported container metadata file: " + file);
                }
                Files.copy(file, target.resolve(source.relativize(file)), StandardCopyOption.COPY_ATTRIBUTES);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static long entryBytes(final Path entry) throws IOException {
        if (Files.isRegularFile(entry)) {
            return Files.size(entry);
        }
        long total = 0L;
        try (var paths = Files.walk(entry)) {
            for (final Path path : paths.toList()) {
                if (Files.isSymbolicLink(path)) {
                    throw new IOException("Symbolic links are not allowed in container metadata: " + path);
                }
                if (Files.isRegularFile(path)) {
                    total = Math.addExact(total, Files.size(path));
                }
            }
        }
        return total;
    }

    private static void atomicMove(final Path source, final Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (final AtomicMoveNotSupportedException exception) {
            throw new IOException("Atomic container metadata moves are required", exception);
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

    private record InstallIntent(Path container, List<String> entries) {
    }
}
