package dev.shardingbase.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Bounded structured payloads used during backend validation. */
public final class ValidationPayloadCodec {
    private ValidationPayloadCodec() {
    }

    public static byte[] encodeRequest(final ValidationRequest request) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            writeString(output, request.credential());
            writeString(output, request.serverId());
            writeString(output, request.serverName());
            writeString(output, request.minecraftVersion());
            writeString(output, request.shardingbaseVersion());
        }
        return bytes.toByteArray();
    }

    public static ValidationRequest decodeRequest(final byte[] payload) throws IOException {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload))) {
            final ValidationRequest request = new ValidationRequest(
                readString(input),
                readString(input),
                readString(input),
                readString(input),
                readString(input)
            );
            requireFinished(input);
            return request;
        }
    }

    public static byte[] encodeResponse(final ValidationResponse response) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeBoolean(response.accepted());
            writeString(output, response.detail());
            writeString(output, response.peerId());
            writeString(output, response.peerName());
        }
        return bytes.toByteArray();
    }

    public static ValidationResponse decodeResponse(final byte[] payload) throws IOException {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload))) {
            final ValidationResponse response = new ValidationResponse(
                input.readBoolean(),
                readString(input),
                readString(input),
                readString(input)
            );
            requireFinished(input);
            return response;
        }
    }

    private static void writeString(final DataOutputStream output, final String value) throws IOException {
        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > ShardingbaseProtocol.MAX_CONTROL_FIELD_BYTES) {
            throw new ProtocolException("Validation field exceeds the control-field limit");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readString(final DataInputStream input) throws IOException {
        final int length = input.readInt();
        if (length < 0 || length > ShardingbaseProtocol.MAX_CONTROL_FIELD_BYTES || length > input.available()) {
            throw new ProtocolException("Invalid validation field length: " + length);
        }
        return new String(input.readNBytes(length), StandardCharsets.UTF_8);
    }

    private static void requireFinished(final DataInputStream input) throws IOException {
        if (input.available() != 0) {
            throw new ProtocolException("Trailing bytes in validation payload");
        }
    }

    public record ValidationRequest(
        String credential,
        String serverId,
        String serverName,
        String minecraftVersion,
        String shardingbaseVersion
    ) {
    }

    public record ValidationResponse(boolean accepted, String detail, String peerId, String peerName) {
    }
}
