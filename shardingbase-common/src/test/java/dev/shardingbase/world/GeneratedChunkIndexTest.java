package dev.shardingbase.world;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GeneratedChunkIndexTest {
    @Test
    void indexesGeneratedChunksAcrossNegativeRegions(@TempDir final Path directory) throws Exception {
        writeRegion(directory.resolve("r.-1.2.mca"), new Entry(0, 2, 1), new Entry(1_023, 3, 2));
        writeRegion(directory.resolve("r.0.-1.mca"), new Entry(31, 2, 1));

        final GeneratedChunkIndex.Scan scan = GeneratedChunkIndex.scan(directory);

        assertEquals(3, scan.chunks().size());
        assertEquals(-32, scan.minChunkX());
        assertEquals(31, scan.maxChunkX());
        assertEquals(-32, scan.minChunkZ());
        assertEquals(95, scan.maxChunkZ());
        assertEquals(4L * 4_096L, scan.estimatedBytes());
    }

    private static void writeRegion(final Path path, final Entry... entries) throws Exception {
        final ByteBuffer bytes = ByteBuffer.allocate(5 * 4_096).order(ByteOrder.BIG_ENDIAN);
        for (final Entry entry : entries) {
            bytes.putInt(entry.index() * Integer.BYTES, entry.sectorOffset() << 8 | entry.sectorCount());
        }
        Files.write(path, bytes.array());
    }

    private record Entry(int index, int sectorOffset, int sectorCount) {
    }
}
