package dev.shardingbase.node.world;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Offline terrain, entity, and POI region splitter used by node transactions. */
public final class WorldSplitEngine {
    private static final List<String> DATA_DIRECTORIES = List.of("region", "entities", "poi");
    private static final Set<String> EXCLUDED_SHARED_ENTRIES = Set.of(
        "region", "entities", "poi", "session.lock", ShardManifestWriter.FILE_NAME
    );

    private WorldSplitEngine() {
    }

    public static SplitSummary split(
        final Path worldRoot,
        final Path negativeRoot,
        final Path positiveRoot,
        final ShardAxis axis,
        final int cutChunk
    ) throws IOException {
        final Path source = worldRoot.toAbsolutePath().normalize();
        final Path negative = negativeRoot.toAbsolutePath().normalize();
        final Path positive = positiveRoot.toAbsolutePath().normalize();
        if (source.equals(negative) || source.equals(positive) || negative.equals(positive)) {
            throw new IOException("Source and split output roots must be distinct");
        }
        int negativeChunks = 0;
        int positiveChunks = 0;
        int files = 0;
        for (final String directoryName : DATA_DIRECTORIES) {
            final Path dataDirectory = source.resolve(directoryName);
            if (Files.notExists(dataDirectory)) {
                continue;
            }
            try (var paths = Files.list(dataDirectory)) {
                final List<Path> regionFiles = new ArrayList<>();
                final Set<String> sidecars = new HashSet<>();
                for (final Path path : paths.toList()) {
                    final String name = path.getFileName().toString();
                    if (Files.isSymbolicLink(path) || !Files.isRegularFile(path)) {
                        throw new IOException("Unsupported entry in world region directory: " + path);
                    }
                    if (name.matches("r\\.-?\\d+\\.-?\\d+\\.mca")) {
                        regionFiles.add(path);
                    } else if (name.matches("c\\.-?\\d+\\.-?\\d+\\.mcc")) {
                        sidecars.add(name);
                    } else {
                        throw new IOException("Unsupported entry in world region directory: " + path);
                    }
                }
                final Set<String> referencedSidecars = new HashSet<>();
                for (final Path regionFile : regionFiles) {
                    final Path relative = source.relativize(regionFile);
                    final RegionFileSplitter.SplitResult result = RegionFileSplitter.split(
                        regionFile,
                        negative.resolve(relative),
                        positive.resolve(relative),
                        axis,
                        cutChunk
                    );
                    negativeChunks += result.negativeChunks();
                    positiveChunks += result.positiveChunks();
                    referencedSidecars.addAll(result.externalSidecars());
                    files++;
                }
                if (!referencedSidecars.equals(sidecars)) {
                    final Set<String> orphaned = new HashSet<>(sidecars);
                    orphaned.removeAll(referencedSidecars);
                    final Set<String> missing = new HashSet<>(referencedSidecars);
                    missing.removeAll(sidecars);
                    throw new IOException("External Anvil sidecar mismatch in " + dataDirectory
                        + "; orphaned=" + orphaned + ", missing=" + missing);
                }
            }
        }
        copySharedEntries(source, negative);
        copySharedEntries(source, positive);
        ContainerMetadataEngine.seed(source, negative, positive);
        return new SplitSummary(files, negativeChunks, positiveChunks);
    }

    private static void copySharedEntries(final Path source, final Path target) throws IOException {
        final List<Path> entries;
        try (var paths = Files.list(source)) {
            entries = paths
                .filter(entry -> !EXCLUDED_SHARED_ENTRIES.contains(entry.getFileName().toString()))
                .sorted()
                .toList();
        }
        for (final Path entry : entries) {
            final String entryName = entry.getFileName().toString();
            if (Files.isSymbolicLink(entry)) {
                throw new IOException("Symbolic links are not allowed in shared world metadata: " + entry);
            }
            if (Files.isRegularFile(entry)) {
                Files.createDirectories(target);
                Files.copy(entry, target.resolve(entryName), StandardCopyOption.COPY_ATTRIBUTES);
                continue;
            }
            if (!Files.isDirectory(entry)) {
                throw new IOException("Unsupported shared world metadata entry: " + entry);
            }
            Files.walkFileTree(entry, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(
                    final Path directory,
                    final BasicFileAttributes attributes
                ) throws IOException {
                    if (Files.isSymbolicLink(directory)) {
                        throw new IOException("Symbolic links are not allowed in shared world metadata: " + directory);
                    }
                    Files.createDirectories(target.resolve(source.relativize(directory)));
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(final Path file, final BasicFileAttributes attributes)
                    throws IOException {
                    if (Files.isSymbolicLink(file) || !attributes.isRegularFile()) {
                        throw new IOException("Unsupported shared world metadata entry: " + file);
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
    }

    public record SplitSummary(int regionFiles, int negativeChunkEntries, int positiveChunkEntries) {
    }
}
