package dev.shardingbase.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/** Bounded detached payloads for the asynchronous shard-aware public API. */
public final class RemoteOperationCodec {
    private static final int MAX_STRING_BYTES = 32_768;
    private static final int MAX_PROPERTIES = 128;

    private RemoteOperationCodec() {
    }

    public static byte[] encodeRequest(final Request request) throws IOException {
        return encode(output -> {
            writeUuid(output, request.operationId());
            writeString(output, request.originBackendId());
            output.writeByte(request.operation().ordinal());
            writeString(output, request.worldKey());
            output.writeInt(request.x());
            output.writeInt(request.y());
            output.writeInt(request.z());
            writeString(output, request.argument());
            writeMap(output, request.properties());
        });
    }

    public static Request decodeRequest(final byte[] payload) throws IOException {
        return decode(payload, input -> new Request(
            readUuid(input),
            readString(input),
            enumValue(Operation.values(), input.readUnsignedByte(), "remote operation"),
            readString(input),
            input.readInt(),
            input.readInt(),
            input.readInt(),
            readString(input),
            readMap(input)
        ));
    }

    public static byte[] encodeResponse(final Response response) throws IOException {
        return encode(output -> {
            writeUuid(output, response.operationId());
            output.writeByte(response.outcome().ordinal());
            writeString(output, response.detail());
            writeString(output, response.value());
            writeMap(output, response.properties());
        });
    }

    public static Response decodeResponse(final byte[] payload) throws IOException {
        return decode(payload, input -> new Response(
            readUuid(input),
            enumValue(Outcome.values(), input.readUnsignedByte(), "remote outcome"),
            readString(input),
            readString(input),
            readMap(input)
        ));
    }

    private static byte[] encode(final Writer writer) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            writer.write(output);
        }
        final byte[] payload = bytes.toByteArray();
        if (payload.length > FrameCodec.MAX_PAYLOAD_BYTES) {
            throw new ProtocolException("Remote operation payload exceeds the frame limit");
        }
        return payload;
    }

    private static <T> T decode(final byte[] payload, final Reader<T> reader) throws IOException {
        if (payload.length > FrameCodec.MAX_PAYLOAD_BYTES) {
            throw new ProtocolException("Remote operation payload exceeds the frame limit");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload))) {
            final T value = reader.read(input);
            if (input.available() != 0) {
                throw new ProtocolException("Trailing remote operation payload data");
            }
            return value;
        } catch (final IllegalArgumentException exception) {
            throw new ProtocolException("Invalid remote operation payload", exception);
        }
    }

    private static void writeMap(final DataOutputStream output, final Map<String, String> values) throws IOException {
        if (values.size() > MAX_PROPERTIES) {
            throw new ProtocolException("Too many remote operation properties");
        }
        output.writeInt(values.size());
        for (final Map.Entry<String, String> entry : values.entrySet()) {
            writeString(output, entry.getKey());
            writeString(output, entry.getValue());
        }
    }

    private static Map<String, String> readMap(final DataInputStream input) throws IOException {
        final int count = input.readInt();
        if (count < 0 || count > MAX_PROPERTIES) {
            throw new ProtocolException("Invalid remote operation property count");
        }
        final Map<String, String> values = new LinkedHashMap<>();
        for (int index = 0; index < count; index++) {
            if (values.put(readString(input), readString(input)) != null) {
                throw new ProtocolException("Duplicate remote operation property");
            }
        }
        return Map.copyOf(values);
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
        if (bytes.length > MAX_STRING_BYTES) {
            throw new ProtocolException("Remote operation string exceeds its limit");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readString(final DataInputStream input) throws IOException {
        final int length = input.readInt();
        if (length < 0 || length > MAX_STRING_BYTES || length > input.available()) {
            throw new ProtocolException("Invalid remote operation string length");
        }
        return new String(input.readNBytes(length), StandardCharsets.UTF_8);
    }

    private static <T> T enumValue(final T[] values, final int ordinal, final String field) throws ProtocolException {
        if (ordinal < 0 || ordinal >= values.length) {
            throw new ProtocolException("Invalid " + field + " ordinal");
        }
        return values[ordinal];
    }

    public enum Operation {
        READ_BLOCK,
        SET_BLOCK_DATA,
        BREAK_BLOCK,
        SPAWN_ENTITY
    }

    public enum Outcome {
        SUCCESS,
        VALIDATION_FAILURE,
        REMOTE_FAILURE
    }

    public record Request(
        UUID operationId,
        String originBackendId,
        Operation operation,
        String worldKey,
        int x,
        int y,
        int z,
        String argument,
        Map<String, String> properties
    ) {
        public Request {
            if (operationId == null || originBackendId == null || originBackendId.isBlank() || operation == null
                || worldKey == null || worldKey.isBlank() || argument == null || properties == null) {
                throw new IllegalArgumentException("Remote operation request fields are required");
            }
            properties = Map.copyOf(properties);
        }
    }

    public record Response(UUID operationId, Outcome outcome, String detail, String value, Map<String, String> properties) {
        public Response {
            if (operationId == null || outcome == null || detail == null || value == null || properties == null) {
                throw new IllegalArgumentException("Remote operation response fields are required");
            }
            properties = Map.copyOf(properties);
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
