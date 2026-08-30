package dev.shardingbase.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

/** Length-prefixed binary frame codec with SHA-256 integrity verification. */
public final class FrameCodec {
    /** Maximum opaque payload accepted by one frame. */
    public static final int MAX_PAYLOAD_BYTES = 16 * 1024 * 1024;
    private static final int CHECKSUM_BYTES = 32;
    private static final int MAX_FRAME_BYTES = MAX_PAYLOAD_BYTES + 32_768;

    private FrameCodec() {
    }

    public static void write(final OutputStream stream, final ProtocolFrame frame) throws IOException {
        final ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
        try (DataOutputStream body = new DataOutputStream(bodyBytes)) {
            body.writeInt(ShardingbaseProtocol.MAGIC);
            body.writeInt(frame.version());
            body.writeByte(frame.channel().ordinal());
            body.writeByte(frame.messageType().ordinal());
            body.writeLong(frame.correlationId().getMostSignificantBits());
            body.writeLong(frame.correlationId().getLeastSignificantBits());
            writeString(body, frame.sourceId());
            writeString(body, frame.targetId());
            final byte[] payload = frame.payload();
            if (payload.length > MAX_PAYLOAD_BYTES) {
                throw new ProtocolException("Payload exceeds " + MAX_PAYLOAD_BYTES + " bytes");
            }
            body.writeInt(payload.length);
            body.write(payload);
        }

        final byte[] body = bodyBytes.toByteArray();
        final byte[] checksum = sha256(body);
        final int frameLength = body.length + checksum.length;
        if (frameLength > MAX_FRAME_BYTES) {
            throw new ProtocolException("Frame exceeds " + MAX_FRAME_BYTES + " bytes");
        }
        final DataOutputStream output = new DataOutputStream(stream);
        output.writeInt(frameLength);
        output.write(body);
        output.write(checksum);
        output.flush();
    }

    public static ProtocolFrame read(final InputStream stream) throws IOException {
        final DataInputStream input = new DataInputStream(stream);
        final int frameLength;
        try {
            frameLength = input.readInt();
        } catch (final EOFException exception) {
            throw new ProtocolException("Connection ended before a frame length", exception);
        }
        if (frameLength < CHECKSUM_BYTES || frameLength > MAX_FRAME_BYTES) {
            throw new ProtocolException("Invalid frame length: " + frameLength);
        }
        final byte[] encoded = input.readNBytes(frameLength);
        if (encoded.length != frameLength) {
            throw new ProtocolException("Connection ended inside a frame");
        }
        final int bodyLength = frameLength - CHECKSUM_BYTES;
        final byte[] expected = sha256(encoded, 0, bodyLength);
        if (!MessageDigest.isEqual(expected, copyOfRange(encoded, bodyLength, frameLength))) {
            throw new ProtocolException("Frame checksum mismatch");
        }

        try (DataInputStream body = new DataInputStream(new ByteArrayInputStream(encoded, 0, bodyLength))) {
            if (body.readInt() != ShardingbaseProtocol.MAGIC) {
                throw new ProtocolException("Invalid frame marker");
            }
            final int version = body.readInt();
            final ProtocolChannel channel = enumValue(ProtocolChannel.values(), body.readUnsignedByte(), "channel");
            final MessageType messageType = enumValue(MessageType.values(), body.readUnsignedByte(), "message type");
            final UUID correlationId = new UUID(body.readLong(), body.readLong());
            final String source = readString(body);
            final String target = readString(body);
            final int payloadLength = body.readInt();
            if (payloadLength < 0 || payloadLength > MAX_PAYLOAD_BYTES || payloadLength != body.available()) {
                throw new ProtocolException("Invalid payload length: " + payloadLength);
            }
            final byte[] payload = body.readNBytes(payloadLength);
            return new ProtocolFrame(version, channel, messageType, correlationId, source, target, payload);
        }
    }

    private static void writeString(final DataOutputStream output, final String value) throws IOException {
        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > ShardingbaseProtocol.MAX_CONTROL_FIELD_BYTES) {
            throw new ProtocolException("Control field exceeds " + ShardingbaseProtocol.MAX_CONTROL_FIELD_BYTES + " bytes");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readString(final DataInputStream input) throws IOException {
        final int length = input.readInt();
        if (length < 0 || length > ShardingbaseProtocol.MAX_CONTROL_FIELD_BYTES || length > input.available()) {
            throw new ProtocolException("Invalid control field length: " + length);
        }
        return new String(input.readNBytes(length), StandardCharsets.UTF_8);
    }

    private static <T> T enumValue(final T[] values, final int ordinal, final String field) throws ProtocolException {
        if (ordinal < 0 || ordinal >= values.length) {
            throw new ProtocolException("Invalid " + field + " ordinal: " + ordinal);
        }
        return values[ordinal];
    }

    private static byte[] sha256(final byte[] bytes) {
        return sha256(bytes, 0, bytes.length);
    }

    private static byte[] sha256(final byte[] bytes, final int offset, final int length) {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(bytes, offset, length);
            return digest.digest();
        } catch (final NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is required by the Java runtime", exception);
        }
    }

    private static byte[] copyOfRange(final byte[] source, final int start, final int end) {
        final byte[] result = new byte[end - start];
        System.arraycopy(source, start, result, 0, result.length);
        return result;
    }
}
