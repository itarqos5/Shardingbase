package dev.shardingbase.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Bounded node-session authentication payloads. */
public final class NodeAuthenticationCodec {
    private NodeAuthenticationCodec() {
    }

    public static byte[] encodeRequest(final String credential) throws IOException {
        return encodeFields(credential);
    }

    public static String decodeRequest(final byte[] payload) throws IOException {
        final String[] fields = decodeFields(payload, 1);
        return fields[0];
    }

    public static byte[] encodeResponse(final boolean accepted, final String detail) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeBoolean(accepted);
            writeField(output, detail);
        }
        return bytes.toByteArray();
    }

    public static AuthenticationResponse decodeResponse(final byte[] payload) throws IOException {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload))) {
            final boolean accepted = input.readBoolean();
            final String detail = readField(input);
            if (input.available() != 0) {
                throw new ProtocolException("Trailing node authentication payload");
            }
            return new AuthenticationResponse(accepted, detail);
        }
    }

    private static byte[] encodeFields(final String... fields) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            for (final String field : fields) {
                writeField(output, field);
            }
        }
        return bytes.toByteArray();
    }

    private static String[] decodeFields(final byte[] payload, final int count) throws IOException {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload))) {
            final String[] fields = new String[count];
            for (int index = 0; index < count; index++) {
                fields[index] = readField(input);
            }
            if (input.available() != 0) {
                throw new ProtocolException("Trailing node authentication payload");
            }
            return fields;
        }
    }

    private static void writeField(final DataOutputStream output, final String value) throws IOException {
        if (value == null) {
            throw new ProtocolException("Node authentication field is required");
        }
        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > ShardingbaseProtocol.MAX_CONTROL_FIELD_BYTES) {
            throw new ProtocolException("Node authentication field exceeds the control limit");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readField(final DataInputStream input) throws IOException {
        final int length = input.readInt();
        if (length < 0 || length > ShardingbaseProtocol.MAX_CONTROL_FIELD_BYTES || length > input.available()) {
            throw new ProtocolException("Invalid node authentication field length");
        }
        return new String(input.readNBytes(length), StandardCharsets.UTF_8);
    }

    public record AuthenticationResponse(boolean accepted, String detail) {
    }
}
