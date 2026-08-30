package dev.shardingbase.node.world;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Offline terrain, entity, and POI region splitter used by node transactions. */
public final class WorldSplitEngine {
    private static final List<String> DATA_DIRECTORIES = List.of("region", "entities", "poi");

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
                final List<Path> regionFiles = paths
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().matches("r\\.-?\\d+\\.-?\\d+\\.mca"))
                    .toList();
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
                    files++;
                }
            }
        }
        return new SplitSummary(files, negativeChunks, positiveChunks);
    }

    public record SplitSummary(int regionFiles, int negativeChunkEntries, int positiveChunkEntries) {
    }
}
