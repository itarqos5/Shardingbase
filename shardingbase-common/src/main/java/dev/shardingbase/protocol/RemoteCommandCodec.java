package dev.shardingbase.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/** Bounded catalogs, execution requests, suggestions, and captured command output. */
public final class RemoteCommandCodec {
    private static final int MAX_STRING_BYTES = 32_768;
    private static final int MAX_ENTRIES = 4_096;

    private RemoteCommandCodec() {
    }

    public static byte[] encodeCatalog(final Catalog catalog) throws IOException {
        return encode(output -> {
            writeString(output, catalog.backendId());
            writeStrings(output, List.copyOf(catalog.labels()));
        });
    }

    public static Catalog decodeCatalog(final byte[] payload) throws IOException {
        return decode(payload, input -> new Catalog(readString(input), Set.copyOf(readStrings(input))));
    }

    public static byte[] encodeRequest(final Request request) throws IOException {
        return encode(output -> {
            writeUuid(output, request.requestId());
            writeString(output, request.originBackendId());
            output.writeByte(request.operation().ordinal());
            writeString(output, request.commandLine());
        });
    }

    public static Request decodeRequest(final byte[] payload) throws IOException {
        return decode(payload, input -> new Request(
            readUuid(input),
            readString(input),
            enumValue(Operation.values(), input.readUnsignedByte()),
            readString(input)
        ));
    }

    public static byte[] encodeResponse(final Response response) throws IOException {
        return encode(output -> {
            writeUuid(output, response.requestId());
            output.writeByte(response.outcome().ordinal());
            writeString(output, response.detail());
            writeStrings(output, response.lines());
        });
    }

    public static Response decodeResponse(final byte[] payload) throws IOException {
        return decode(payload, input -> new Response(
            readUuid(input),
            enumValue(Outcome.values(), input.readUnsignedByte()),
            readString(input),
            List.copyOf(readStrings(input))
        ));
    }

    private static byte[] encode(final Writer writer) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            writer.write(output);
        }
        final byte[] payload = bytes.toByteArray();
        if (payload.length > FrameCodec.MAX_PAYLOAD_BYTES) {
            throw new ProtocolException("Remote command payload exceeds the frame limit");
        }
        return payload;
    }

    private static <T> T decode(final byte[] payload, final Reader<T> reader) throws IOException {
        if (payload.length > FrameCodec.MAX_PAYLOAD_BYTES) {
            throw new ProtocolException("Remote command payload exceeds the frame limit");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload))) {
            final T value = reader.read(input);
            if (input.available() != 0) {
                throw new ProtocolException("Trailing remote command payload data");
            }
            return value;
        } catch (final IllegalArgumentException exception) {
            throw new ProtocolException("Invalid remote command payload", exception);
        }
    }

    private static void writeStrings(final DataOutputStream output, final List<String> values) throws IOException {
        if (values.size() > MAX_ENTRIES) {
            throw new ProtocolException("Too many remote command entries");
        }
        output.writeInt(values.size());
        for (final String value : values) {
            writeString(output, value);
        }
    }

    private static List<String> readStrings(final DataInputStream input) throws IOException {
        final int count = input.readInt();
        if (count < 0 || count > MAX_ENTRIES) {
            throw new ProtocolException("Invalid remote command entry count");
        }
        final List<String> values = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            values.add(readString(input));
        }
        return values;
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
            throw new ProtocolException("Remote command string exceeds its limit");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readString(final DataInputStream input) throws IOException {
        final int length = input.readInt();
        if (length < 0 || length > MAX_STRING_BYTES || length > input.available()) {
            throw new ProtocolException("Invalid remote command string length");
        }
        return new String(input.readNBytes(length), StandardCharsets.UTF_8);
    }

    private static <T> T enumValue(final T[] values, final int ordinal) throws ProtocolException {
        if (ordinal < 0 || ordinal >= values.length) {
            throw new ProtocolException("Invalid remote command enum ordinal");
        }
        return values[ordinal];
    }

    public enum Operation {
        EXECUTE,
        SUGGEST
    }

    public enum Outcome {
        SUCCESS,
        REJECTED,
        FAILURE
    }

    public record Catalog(String backendId, Set<String> labels) {
        public Catalog {
            if (backendId == null || backendId.isBlank() || labels == null) {
                throw new IllegalArgumentException("Remote command catalog fields are required");
            }
            final LinkedHashSet<String> normalized = new LinkedHashSet<>();
            for (final String label : labels) {
                if (label == null) {
                    throw new IllegalArgumentException("Invalid remote command label");
                }
                final String candidate = label.toLowerCase(java.util.Locale.ROOT);
                if (!candidate.matches("[a-z0-9_.:-]+")) {
                    throw new IllegalArgumentException("Invalid remote command label");
                }
                normalized.add(candidate);
            }
            labels = Set.copyOf(normalized);
        }
    }

    public record Request(UUID requestId, String originBackendId, Operation operation, String commandLine) {
        public Request {
            if (requestId == null || originBackendId == null || originBackendId.isBlank() || operation == null
                || commandLine == null || commandLine.isBlank()) {
                throw new IllegalArgumentException("Remote command request fields are required");
            }
        }
    }

    public record Response(UUID requestId, Outcome outcome, String detail, List<String> lines) {
        public Response {
            if (requestId == null || outcome == null || detail == null || lines == null) {
                throw new IllegalArgumentException("Remote command response fields are required");
            }
            lines = List.copyOf(lines);
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
