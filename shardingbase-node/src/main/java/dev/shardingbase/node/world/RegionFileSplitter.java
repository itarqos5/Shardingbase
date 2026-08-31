package dev.shardingbase.node.world;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Rewrites Anvil region files without decoding or retaining their compressed chunk payloads. */
final class RegionFileSplitter {
    static final int SECTOR_BYTES = 4096;
    static final int HEADER_BYTES = SECTOR_BYTES * 2;
    private static final int COPY_BUFFER_BYTES = 64 * 1024;
    private static final int EXTERNAL_STREAM_FLAG = 0x80;
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
        final long sourceBytes = Files.size(source);
        if (sourceBytes < HEADER_BYTES || sourceBytes % SECTOR_BYTES != 0) {
            throw new IOException("Invalid Anvil region length: " + source);
        }
        final List<ChunkRecord> negative = new ArrayList<>();
        final List<ChunkRecord> positive = new ArrayList<>();
        final Set<String> sidecars = new HashSet<>();
        try (FileChannel input = FileChannel.open(source, StandardOpenOption.READ)) {
            final ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
            readFully(input, header, 0L, source);
            header.flip();
            for (int index = 0; index < 1024; index++) {
                final int location = header.getInt(index * Integer.BYTES);
                final int sectorOffset = location >>> 8;
                final int sectorCount = location & 0xFF;
                if (sectorOffset == 0 && sectorCount == 0) {
                    continue;
                }
                if (sectorOffset < 2 || sectorCount < 1
                    || ((long) sectorOffset + sectorCount) * SECTOR_BYTES > sourceBytes) {
                    throw new IOException("Invalid chunk location at index " + index + " in " + source);
                }
                final int timestamp = header.getInt(SECTOR_BYTES + index * Integer.BYTES);
                final int localX = index & 31;
                final int localZ = index >>> 5;
                final int chunkX = Math.addExact(Math.multiplyExact(region.x(), 32), localX);
                final int chunkZ = Math.addExact(Math.multiplyExact(region.z(), 32), localZ);
                final boolean external = validateRecord(input, source, index, sectorOffset, sectorCount);
                final String sidecar = external ? sidecarName(chunkX, chunkZ) : null;
                if (sidecar != null) {
                    requireSidecar(source.resolveSibling(sidecar));
                    sidecars.add(sidecar);
                }
                final ChunkRecord record = new ChunkRecord(index, timestamp, sectorOffset, sectorCount, sidecar);
                if (ownsNegative(axis, cutChunk).test(chunkX, chunkZ)) {
                    negative.add(record);
                } else {
                    positive.add(record);
                }
            }

            write(input, source, negativeOutput, negative);
            write(input, source, positiveOutput, positive);
        } catch (final ArithmeticException exception) {
            throw new IOException("Region coordinates overflow the supported chunk range: " + source, exception);
        }
        copySidecars(source, negativeOutput, negative);
        copySidecars(source, positiveOutput, positive);
        return new SplitResult(negative.size(), positive.size(), Set.copyOf(sidecars));
    }

    private static boolean validateRecord(
        final FileChannel input,
        final Path source,
        final int index,
        final int sectorOffset,
        final int sectorCount
    ) throws IOException {
        final ByteBuffer prefix = ByteBuffer.allocate(Integer.BYTES + 1).order(ByteOrder.BIG_ENDIAN);
        readFully(input, prefix, (long) sectorOffset * SECTOR_BYTES, source);
        prefix.flip();
        final int encodedLength = prefix.getInt();
        if (encodedLength < 1 || encodedLength > sectorCount * SECTOR_BYTES - Integer.BYTES) {
            throw new IOException("Invalid chunk payload length at index " + index + " in " + source);
        }
        return (prefix.get() & EXTERNAL_STREAM_FLAG) != 0;
    }

    private static void requireSidecar(final Path sidecar) throws IOException {
        if (Files.isSymbolicLink(sidecar) || !Files.isRegularFile(sidecar, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("External Anvil chunk sidecar is missing or unsafe: " + sidecar);
        }
    }

    private static void copySidecars(
        final Path sourceRegion,
        final Path outputRegion,
        final List<ChunkRecord> chunks
    ) throws IOException {
        final Path outputDirectory = outputRegion.getParent();
        for (final ChunkRecord chunk : chunks) {
            if (chunk.sidecar() == null) {
                continue;
            }
            Files.createDirectories(outputDirectory);
            Files.copy(
                sourceRegion.resolveSibling(chunk.sidecar()),
                outputDirectory.resolve(chunk.sidecar()),
                StandardCopyOption.COPY_ATTRIBUTES
            );
        }
    }

    private static BiPredicate<Integer, Integer> ownsNegative(final ShardAxis axis, final int cutChunk) {
        return axis == ShardAxis.X
            ? (chunkX, chunkZ) -> chunkX < cutChunk
            : (chunkX, chunkZ) -> chunkZ < cutChunk;
    }

    private static void write(
        final FileChannel input,
        final Path source,
        final Path output,
        final List<ChunkRecord> chunks
    ) throws IOException {
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
                if (chunk.sectorCount() > 255 || nextSector > 0xFFFFFF - chunk.sectorCount()) {
                    throw new IOException("Region output exceeds the Anvil location table limits: " + output);
                }
                header.putInt(chunk.index() * Integer.BYTES, nextSector << 8 | chunk.sectorCount());
                header.putInt(SECTOR_BYTES + chunk.index() * Integer.BYTES, chunk.timestamp());
                nextSector += chunk.sectorCount();
            }
            try (FileChannel target = FileChannel.open(
                temporary,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
            )) {
                header.clear();
                writeFully(target, header);
                final ByteBuffer buffer = ByteBuffer.allocate(COPY_BUFFER_BYTES);
                for (final ChunkRecord chunk : chunks) {
                    copy(
                        input,
                        (long) chunk.sectorOffset() * SECTOR_BYTES,
                        (long) chunk.sectorCount() * SECTOR_BYTES,
                        target,
                        buffer,
                        source
                    );
                }
                target.force(true);
            }
            Files.move(temporary, output, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static void copy(
        final FileChannel input,
        final long inputOffset,
        final long length,
        final FileChannel output,
        final ByteBuffer buffer,
        final Path source
    ) throws IOException {
        long position = inputOffset;
        long remaining = length;
        while (remaining > 0) {
            buffer.clear();
            buffer.limit((int) Math.min(buffer.capacity(), remaining));
            final int read = input.read(buffer, position);
            if (read < 1) {
                throw new IOException("Unexpected end of Anvil region: " + source);
            }
            buffer.flip();
            writeFully(output, buffer);
            position += read;
            remaining -= read;
        }
    }

    private static void readFully(
        final FileChannel channel,
        final ByteBuffer target,
        final long offset,
        final Path source
    ) throws IOException {
        long position = offset;
        while (target.hasRemaining()) {
            final int read = channel.read(target, position);
            if (read < 1) {
                throw new IOException("Unexpected end of Anvil region: " + source);
            }
            position += read;
        }
    }

    private static void writeFully(final FileChannel channel, final ByteBuffer source) throws IOException {
        while (source.hasRemaining()) {
            channel.write(source);
        }
    }

    private static String sidecarName(final int chunkX, final int chunkZ) {
        return "c." + chunkX + '.' + chunkZ + ".mcc";
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

    record SplitResult(int negativeChunks, int positiveChunks, Set<String> externalSidecars) {
    }

    private record RegionCoordinates(int x, int z) {
    }

    private record ChunkRecord(int index, int timestamp, int sectorOffset, int sectorCount, String sidecar) {
    }
}
