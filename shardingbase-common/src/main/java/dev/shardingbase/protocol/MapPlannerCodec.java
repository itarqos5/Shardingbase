package dev.shardingbase.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/** Bounded protocol for incrementally publishing generated-chunk map tiles to Velocity. */
public final class MapPlannerCodec {
    public static final int TILE_CHUNKS = 16;
    public static final int TILE_BLOCKS = TILE_CHUNKS * 16;
    private static final int MAX_STRING_BYTES = 32_768;
    private static final int MAX_TILE_BYTES = 2 * 1_024 * 1_024;

    private MapPlannerCodec() {
    }

    public static byte[] encodeCreate(final Create request) throws IOException {
        return encode(output -> {
            writeUuid(output, request.sessionId());
            writeString(output, request.backendId());
            writeString(output, request.worldKey());
            writeString(output, request.worldDirectory());
            writeUuid(output, request.worldId());
            output.writeLong(request.worldSeed());
            output.writeInt(request.dataVersion());
            output.writeInt(request.minChunkX());
            output.writeInt(request.maxChunkX());
            output.writeInt(request.minChunkZ());
            output.writeInt(request.maxChunkZ());
            output.writeLong(request.generatedChunks());
            output.writeLong(request.estimatedBytes());
        });
    }

    public static Create decodeCreate(final byte[] payload) throws IOException {
        return decode(payload, input -> new Create(
            readUuid(input), readString(input), readString(input), readString(input), readUuid(input),
            input.readLong(), input.readInt(), input.readInt(), input.readInt(), input.readInt(), input.readInt(),
            input.readLong(), input.readLong()
        ));
    }

    public static byte[] encodeCreated(final Created response) throws IOException {
        return encode(output -> {
            writeUuid(output, response.sessionId());
            output.writeBoolean(response.accepted());
            writeString(output, response.detail());
        });
    }

    public static Created decodeCreated(final byte[] payload) throws IOException {
        return decode(payload, input -> new Created(readUuid(input), input.readBoolean(), readString(input)));
    }

    public static byte[] encodeTile(final Tile tile) throws IOException {
        if (tile.png().length > MAX_TILE_BYTES) {
            throw new ProtocolException("Map tile exceeds two MiB");
        }
        return encode(output -> {
            writeUuid(output, tile.sessionId());
            output.writeInt(tile.tileX());
            output.writeInt(tile.tileZ());
            output.writeInt(tile.png().length);
            output.write(tile.png());
        });
    }

    public static Tile decodeTile(final byte[] payload) throws IOException {
        return decode(payload, input -> {
            final UUID sessionId = readUuid(input);
            final int tileX = input.readInt();
            final int tileZ = input.readInt();
            final int length = input.readInt();
            if (length < 1 || length > MAX_TILE_BYTES || length > input.available()) {
                throw new ProtocolException("Invalid map tile length");
            }
            return new Tile(sessionId, tileX, tileZ, input.readNBytes(length));
        });
    }

    public static byte[] encodeSessionId(final UUID sessionId) throws IOException {
        return encode(output -> writeUuid(output, sessionId));
    }

    public static UUID decodeSessionId(final byte[] payload) throws IOException {
        return decode(payload, MapPlannerCodec::readUuid);
    }

    public static byte[] encodeLink(final Link link) throws IOException {
        return encode(output -> {
            writeUuid(output, link.sessionId());
            writeString(output, link.url());
        });
    }

    public static Link decodeLink(final byte[] payload) throws IOException {
        return decode(payload, input -> new Link(readUuid(input), readString(input)));
    }

    private static byte[] encode(final Writer writer) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            writer.write(output);
        }
        final byte[] payload = bytes.toByteArray();
        if (payload.length > FrameCodec.MAX_PAYLOAD_BYTES) {
            throw new ProtocolException("Map planner payload exceeds the frame limit");
        }
        return payload;
    }

    private static <T> T decode(final byte[] payload, final Reader<T> reader) throws IOException {
        if (payload.length > FrameCodec.MAX_PAYLOAD_BYTES) {
            throw new ProtocolException("Map planner payload exceeds the frame limit");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload))) {
            final T result = reader.read(input);
            if (input.available() != 0) {
                throw new ProtocolException("Trailing map planner payload data");
            }
            return result;
        } catch (final IllegalArgumentException exception) {
            throw new ProtocolException("Invalid map planner payload", exception);
        }
    }

    private static void writeUuid(final DataOutputStream output, final UUID value) throws IOException {
        output.writeLong(value.getMostSignificantBits());
        output.writeLong(value.getLeastSignificantBits());
    }

    private static UUID readUuid(final DataInputStream input) throws IOException {
        return new UUID(input.readLong(), input.readLong());
    }

    private static void writeString(final DataOutputStream output, final String value) throws IOException {
        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length < 1 || bytes.length > MAX_STRING_BYTES) {
            throw new ProtocolException("Invalid map planner string");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readString(final DataInputStream input) throws IOException {
        final int length = input.readInt();
        if (length < 1 || length > MAX_STRING_BYTES || length > input.available()) {
            throw new ProtocolException("Invalid map planner string length");
        }
        return new String(input.readNBytes(length), StandardCharsets.UTF_8);
    }

    public record Create(
        UUID sessionId,
        String backendId,
        String worldKey,
        String worldDirectory,
        UUID worldId,
        long worldSeed,
        int dataVersion,
        int minChunkX,
        int maxChunkX,
        int minChunkZ,
        int maxChunkZ,
        long generatedChunks,
        long estimatedBytes
    ) {
        public Create {
            if (sessionId == null || backendId == null || backendId.isBlank() || worldKey == null || worldKey.isBlank()
                || worldDirectory == null || !worldDirectory.matches("[A-Za-z0-9._-]{1,255}")
                || worldId == null || dataVersion < 1
                || minChunkX > maxChunkX || minChunkZ > maxChunkZ || generatedChunks < 1 || estimatedBytes < 0) {
                throw new IllegalArgumentException("Invalid map planner session");
            }
        }
    }

    public record Created(UUID sessionId, boolean accepted, String detail) {
        public Created {
            if (sessionId == null || detail == null || detail.isBlank()) {
                throw new IllegalArgumentException("Invalid map planner acknowledgement");
            }
        }
    }

    public record Tile(UUID sessionId, int tileX, int tileZ, byte[] png) {
        public Tile {
            if (sessionId == null || png == null || png.length < 1 || png.length > MAX_TILE_BYTES) {
                throw new IllegalArgumentException("Invalid map planner tile");
            }
            png = png.clone();
        }

        @Override
        public byte[] png() {
            return this.png.clone();
        }
    }

    public record Link(UUID sessionId, String url) {
        public Link {
            if (sessionId == null || url == null || url.isBlank()) {
                throw new IllegalArgumentException("Invalid map planner link");
            }
        }
    }

    @FunctionalInterface
    private interface Writer {
        void write(DataOutputStream output) throws IOException;
    }

    @FunctionalInterface
    private interface Reader<T> {
        T read(DataInputStream input) throws IOException;
    }
}
