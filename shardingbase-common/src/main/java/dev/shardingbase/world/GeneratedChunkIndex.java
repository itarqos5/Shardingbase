package dev.shardingbase.world;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Reads Anvil location tables to enumerate generated chunks without loading or generating terrain. */
public final class GeneratedChunkIndex {
    private static final int SECTOR_BYTES = 4_096;
    private static final int LOCATION_TABLE_BYTES = 4_096;
    private static final Pattern REGION_NAME = Pattern.compile("r\\.(-?\\d+)\\.(-?\\d+)\\.mca");

    private GeneratedChunkIndex() {
    }

    public static Scan scan(final Path regionDirectory) throws IOException {
        final Path directory = regionDirectory.toAbsolutePath().normalize();
        if (!Files.isDirectory(directory)) {
            throw new IOException("World region directory does not exist: " + directory);
        }
        final List<Path> regions;
        try (var paths = Files.list(directory)) {
            regions = paths.filter(Files::isRegularFile)
                .filter(path -> REGION_NAME.matcher(path.getFileName().toString()).matches())
                .sorted()
                .toList();
        }
        final List<Chunk> chunks = new ArrayList<>();
        long estimatedBytes = 0;
        for (final Path region : regions) {
            final Matcher matcher = REGION_NAME.matcher(region.getFileName().toString());
            if (!matcher.matches()) {
                continue;
            }
            final int regionX;
            final int regionZ;
            try {
                regionX = Integer.parseInt(matcher.group(1));
                regionZ = Integer.parseInt(matcher.group(2));
            } catch (final NumberFormatException exception) {
                throw new IOException("Region coordinates are outside the supported range: " + region, exception);
            }
            final ByteBuffer locations = ByteBuffer.allocate(LOCATION_TABLE_BYTES).order(ByteOrder.BIG_ENDIAN);
            try (FileChannel channel = FileChannel.open(region, StandardOpenOption.READ)) {
                while (locations.hasRemaining() && channel.read(locations) >= 0) {
                    // Read exactly the location table; timestamps and payloads are not needed.
                }
            }
            if (locations.position() != LOCATION_TABLE_BYTES) {
                throw new IOException("Truncated Anvil location table: " + region);
            }
            locations.flip();
            for (int index = 0; index < 1_024; index++) {
                final int location = locations.getInt();
                final int sectorOffset = location >>> 8;
                final int sectorCount = location & 0xFF;
                if (sectorOffset == 0 && sectorCount == 0) {
                    continue;
                }
                if (sectorOffset < 2 || sectorCount < 1) {
                    throw new IOException("Invalid Anvil chunk location in " + region + " at index " + index);
                }
                chunks.add(new Chunk(regionX * 32 + (index & 31), regionZ * 32 + (index >>> 5)));
                estimatedBytes = Math.addExact(estimatedBytes, (long) sectorCount * SECTOR_BYTES);
            }
        }
        if (chunks.isEmpty()) {
            throw new IOException("No generated chunks were found in " + directory);
        }
        chunks.sort(Comparator.comparingInt(Chunk::z).thenComparingInt(Chunk::x));
        int minX = Integer.MAX_VALUE;
        int maxX = Integer.MIN_VALUE;
        int minZ = Integer.MAX_VALUE;
        int maxZ = Integer.MIN_VALUE;
        for (final Chunk chunk : chunks) {
            minX = Math.min(minX, chunk.x());
            maxX = Math.max(maxX, chunk.x());
            minZ = Math.min(minZ, chunk.z());
            maxZ = Math.max(maxZ, chunk.z());
        }
        return new Scan(List.copyOf(chunks), minX, maxX, minZ, maxZ, estimatedBytes);
    }

    public record Chunk(int x, int z) {
    }

    public record Scan(
        List<Chunk> chunks,
        int minChunkX,
        int maxChunkX,
        int minChunkZ,
        int maxChunkZ,
        long estimatedBytes
    ) {
        public Scan {
            chunks = List.copyOf(chunks);
            if (chunks.isEmpty() || minChunkX > maxChunkX || minChunkZ > maxChunkZ || estimatedBytes < 0) {
                throw new IllegalArgumentException("Invalid generated chunk scan");
            }
        }
    }
}
