package dev.shardingbase.node.world;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Rewrites Anvil region files without decoding their compressed chunk payloads. */
final class RegionFileSplitter {
    static final int SECTOR_BYTES = 4096;
    static final int HEADER_BYTES = SECTOR_BYTES * 2;
    private static final Pattern REGION_NAME = Pattern.compile("r\\.(-?\\d+)\\.(-?\\d+)\\.mca");

    private RegionFileSplitter() {
    }

    static SplitResult split(
        final Path source,
        final Path negativeOutput,
        final Path positiveOutput,
        final ShardAxis axis,
        final int cutChunk
    ) throws IOException {
        final RegionCoordinates region = coordinates(source);
        final byte[] sourceBytes = Files.readAllBytes(source);
        if (sourceBytes.length < HEADER_BYTES || sourceBytes.length % SECTOR_BYTES != 0) {
            throw new IOException("Invalid Anvil region length: " + source);
        }
        final ByteBuffer header = ByteBuffer.wrap(sourceBytes).order(ByteOrder.BIG_ENDIAN);
        final List<ChunkRecord> negative = new ArrayList<>();
        final List<ChunkRecord> positive = new ArrayList<>();
        for (int index = 0; index < 1024; index++) {
            final int location = header.getInt(index * Integer.BYTES);
            final int sectorOffset = location >>> 8;
            final int sectorCount = location & 0xFF;
            if (sectorOffset == 0 && sectorCount == 0) {
                continue;
            }
            if (sectorOffset < 2 || sectorCount < 1 || (sectorOffset + sectorCount) * SECTOR_BYTES > sourceBytes.length) {
                throw new IOException("Invalid chunk location at index " + index + " in " + source);
            }
            final int timestamp = header.getInt(SECTOR_BYTES + index * Integer.BYTES);
            final byte[] sectors = new byte[sectorCount * SECTOR_BYTES];
            System.arraycopy(sourceBytes, sectorOffset * SECTOR_BYTES, sectors, 0, sectors.length);
            validateRecordLength(sectors, source, index);
            final int localX = index & 31;
            final int localZ = index >>> 5;
            final int chunkX = region.x() * 32 + localX;
            final int chunkZ = region.z() * 32 + localZ;
            final ChunkRecord record = new ChunkRecord(index, timestamp, sectors);
            if (ownsNegative(axis, cutChunk).test(chunkX, chunkZ)) {
                negative.add(record);
            } else {
                positive.add(record);
            }
        }

        write(negativeOutput, negative);
        write(positiveOutput, positive);
        return new SplitResult(negative.size(), positive.size());
    }

    private static BiPredicate<Integer, Integer> ownsNegative(final ShardAxis axis, final int cutChunk) {
        return axis == ShardAxis.X
            ? (chunkX, chunkZ) -> chunkX < cutChunk
            : (chunkX, chunkZ) -> chunkZ < cutChunk;
    }

    private static void validateRecordLength(final byte[] sectors, final Path source, final int index) throws IOException {
        final int encodedLength = ByteBuffer.wrap(sectors, 0, Integer.BYTES).order(ByteOrder.BIG_ENDIAN).getInt();
        if (encodedLength < 1 || encodedLength > sectors.length - Integer.BYTES) {
            throw new IOException("Invalid chunk payload length at index " + index + " in " + source);
        }
    }

    private static void write(final Path output, final List<ChunkRecord> chunks) throws IOException {
        if (chunks.isEmpty()) {
            Files.deleteIfExists(output);
            return;
        }
        Files.createDirectories(output.getParent());
        final Path temporary = Files.createTempFile(output.getParent(), ".shardingbase-region-", ".tmp");
        try {
            final ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
            int nextSector = 2;
            for (final ChunkRecord chunk : chunks) {
                final int sectorCount = chunk.sectors().length / SECTOR_BYTES;
                if (sectorCount > 255 || nextSector > 0xFFFFFF) {
                    throw new IOException("Region output exceeds the Anvil location table limits: " + output);
                }
                header.putInt(chunk.index() * Integer.BYTES, nextSector << 8 | sectorCount);
                header.putInt(SECTOR_BYTES + chunk.index() * Integer.BYTES, chunk.timestamp());
                nextSector += sectorCount;
            }
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                while (header.hasRemaining()) {
                    channel.write(header);
                }
                for (final ChunkRecord chunk : chunks) {
                    final ByteBuffer sectors = ByteBuffer.wrap(chunk.sectors());
                    while (sectors.hasRemaining()) {
                        channel.write(sectors);
                    }
                }
                channel.force(true);
            }
            Files.move(
                temporary,
                output,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                java.nio.file.StandardCopyOption.REPLACE_EXISTING
            );
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static RegionCoordinates coordinates(final Path source) throws IOException {
        final Matcher matcher = REGION_NAME.matcher(source.getFileName().toString());
        if (!matcher.matches()) {
            throw new IOException("Not an Anvil region filename: " + source);
        }
        try {
            return new RegionCoordinates(Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2)));
        } catch (final NumberFormatException exception) {
            throw new IOException("Region coordinates are outside the supported integer range: " + source, exception);
        }
    }

    record SplitResult(int negativeChunks, int positiveChunks) {
    }

    private record RegionCoordinates(int x, int z) {
    }

    private record ChunkRecord(int index, int timestamp, byte[] sectors) {
    }
}
