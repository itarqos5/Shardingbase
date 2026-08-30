package dev.shardingbase.node.world;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RegionFileSplitterTest {
    @Test
    void splitsPositiveAndNegativeChunksAcrossBothAxes(@TempDir final Path directory) throws Exception {
        final Path world = directory.resolve("world");
        final Path region = world.resolve("region").resolve("r.-1.0.mca");
        writeFixture(region, new int[] {31, 0}, new int[] {0, 0}); // chunks -1 and -32

        final Path negative = directory.resolve("negative");
        final Path positive = directory.resolve("positive");
        final WorldSplitEngine.SplitSummary xSummary = WorldSplitEngine.split(world, negative, positive, ShardAxis.X, -16);

        assertEquals(1, xSummary.regionFiles());
        assertEquals(1, xSummary.negativeChunkEntries());
        assertEquals(1, xSummary.positiveChunkEntries());
        assertEquals(1, entryCount(negative.resolve("region/r.-1.0.mca")));
        assertEquals(1, entryCount(positive.resolve("region/r.-1.0.mca")));

        final Path zNegative = directory.resolve("z-negative");
        final Path zPositive = directory.resolve("z-positive");
        final WorldSplitEngine.SplitSummary zSummary = WorldSplitEngine.split(world, zNegative, zPositive, ShardAxis.Z, 1);
        assertEquals(2, zSummary.negativeChunkEntries());
        assertEquals(0, zSummary.positiveChunkEntries());
        assertTrue(Files.isRegularFile(zNegative.resolve("region/r.-1.0.mca")));
        assertFalse(Files.exists(zPositive.resolve("region/r.-1.0.mca")));
    }

    @Test
    void processesTerrainEntitiesAndPoi(@TempDir final Path directory) throws Exception {
        final Path world = directory.resolve("world");
        for (final String data : new String[] {"region", "entities", "poi"}) {
            writeFixture(world.resolve(data).resolve("r.0.0.mca"), new int[] {0}, new int[] {0});
        }

        final WorldSplitEngine.SplitSummary summary = WorldSplitEngine.split(
            world,
            directory.resolve("negative"),
            directory.resolve("positive"),
            ShardAxis.X,
            0
        );

        assertEquals(3, summary.regionFiles());
        assertEquals(0, summary.negativeChunkEntries());
        assertEquals(3, summary.positiveChunkEntries());
    }

    @Test
    void rejectsMalformedLocations(@TempDir final Path directory) throws Exception {
        final Path source = directory.resolve("r.0.0.mca");
        final ByteBuffer invalid = ByteBuffer.allocate(RegionFileSplitter.HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
        invalid.putInt(1 << 8 | 1);
        Files.write(source, invalid.array());

        boolean rejected = false;
        try {
            RegionFileSplitter.split(
                source,
                directory.resolve("negative.mca"),
                directory.resolve("positive.mca"),
                ShardAxis.X,
                0
            );
        } catch (IOException _) {
            rejected = true;
        }
        assertTrue(rejected);
    }

    private static void writeFixture(final Path path, final int[] localX, final int[] localZ) throws IOException {
        Files.createDirectories(path.getParent());
        final int sectors = localX.length;
        final ByteBuffer file = ByteBuffer.allocate(RegionFileSplitter.HEADER_BYTES + sectors * RegionFileSplitter.SECTOR_BYTES)
            .order(ByteOrder.BIG_ENDIAN);
        for (int entry = 0; entry < sectors; entry++) {
            final int index = localX[entry] + localZ[entry] * 32;
            file.putInt(index * Integer.BYTES, (2 + entry) << 8 | 1);
            file.putInt(RegionFileSplitter.SECTOR_BYTES + index * Integer.BYTES, 100 + entry);
            file.putInt(RegionFileSplitter.HEADER_BYTES + entry * RegionFileSplitter.SECTOR_BYTES, 2);
            file.put(RegionFileSplitter.HEADER_BYTES + entry * RegionFileSplitter.SECTOR_BYTES + 4, (byte) 3);
            file.put(RegionFileSplitter.HEADER_BYTES + entry * RegionFileSplitter.SECTOR_BYTES + 5, (byte) entry);
        }
        Files.write(path, file.array());
    }

    private static int entryCount(final Path region) throws IOException {
        final ByteBuffer header = ByteBuffer.wrap(Files.readAllBytes(region)).order(ByteOrder.BIG_ENDIAN);
        int count = 0;
        for (int index = 0; index < 1024; index++) {
            if (header.getInt(index * Integer.BYTES) != 0) {
                count++;
            }
        }
        return count;
    }
}
