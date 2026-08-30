package dev.shardingbase.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

/** Resumable file-transfer control and data payloads. */
public final class FileTransferCodec {
    public static final int MAX_CHUNK_BYTES = 1024 * 1024;
    private static final int SHA_256_BYTES = 32;

    private FileTransferCodec() {
    }

    public static byte[] encodeBegin(final Begin begin) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            writeUuid(output, begin.transferId());
            writeString(output, begin.relativePath());
            output.writeLong(begin.totalBytes());
            output.write(begin.sha256());
        }
        return bytes.toByteArray();
    }

    public static Begin decodeBegin(final byte[] payload) throws IOException {
        try (DataInputStream input = input(payload)) {
            final Begin begin = new Begin(readUuid(input), readString(input), input.readLong(), input.readNBytes(SHA_256_BYTES));
            requireEnd(input);
            return begin;
        } catch (final IllegalArgumentException exception) {
            throw new ProtocolException("Invalid file begin payload", exception);
        }
    }

    public static byte[] encodeChunk(final Chunk chunk) throws IOException {
        final byte[] data = chunk.data();
        if (data.length > MAX_CHUNK_BYTES) {
            throw new ProtocolException("File chunk exceeds " + MAX_CHUNK_BYTES + " bytes");
        }
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            writeUuid(output, chunk.transferId());
            output.writeLong(chunk.offset());
            output.writeInt(data.length);
            output.write(data);
            output.write(sha256(data));
        }
        return bytes.toByteArray();
    }

    public static Chunk decodeChunk(final byte[] payload) throws IOException {
        try (DataInputStream input = input(payload)) {
            final UUID transferId = readUuid(input);
            final long offset = input.readLong();
            final int length = input.readInt();
            if (length < 0 || length > MAX_CHUNK_BYTES || length > input.available() - SHA_256_BYTES) {
                throw new ProtocolException("Invalid file chunk length");
            }
            final byte[] data = input.readNBytes(length);
            final byte[] expected = input.readNBytes(SHA_256_BYTES);
            requireEnd(input);
            if (!MessageDigest.isEqual(expected, sha256(data))) {
                throw new ProtocolException("File chunk checksum mismatch");
            }
            return new Chunk(transferId, offset, data);
        }
    }

    public static byte[] encodeTransferId(final UUID transferId) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            writeUuid(output, transferId);
        }
        return bytes.toByteArray();
    }

    public static UUID decodeTransferId(final byte[] payload) throws IOException {
        try (DataInputStream input = input(payload)) {
            final UUID transferId = readUuid(input);
            requireEnd(input);
            return transferId;
        }
    }

    public static byte[] encodeAck(final Ack ack) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            writeUuid(output, ack.transferId());
            output.writeLong(ack.nextOffset());
            output.writeBoolean(ack.complete());
            writeString(output, ack.detail());
        }
        return bytes.toByteArray();
    }

    public static Ack decodeAck(final byte[] payload) throws IOException {
        try (DataInputStream input = input(payload)) {
            final Ack ack = new Ack(readUuid(input), input.readLong(), input.readBoolean(), readString(input));
            requireEnd(input);
            return ack;
        }
    }

    private static DataInputStream input(final byte[] payload) throws ProtocolException {
        if (payload.length > FrameCodec.MAX_PAYLOAD_BYTES) {
            throw new ProtocolException("File transfer payload exceeds the frame limit");
        }
        return new DataInputStream(new ByteArrayInputStream(payload));
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
        if (bytes.length > ShardingbaseProtocol.MAX_CONTROL_FIELD_BYTES) {
            throw new ProtocolException("File transfer field exceeds the control limit");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readString(final DataInputStream input) throws IOException {
        final int length = input.readInt();
        if (length < 0 || length > ShardingbaseProtocol.MAX_CONTROL_FIELD_BYTES || length > input.available()) {
            throw new ProtocolException("Invalid file transfer field length");
        }
        return new String(input.readNBytes(length), StandardCharsets.UTF_8);
    }

    private static void requireEnd(final DataInputStream input) throws IOException {
        if (input.available() != 0) {
            throw new ProtocolException("Trailing file transfer payload");
        }
    }

    private static byte[] sha256(final byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        } catch (final NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is required", exception);
        }
    }

    public record Begin(UUID transferId, String relativePath, long totalBytes, byte[] sha256) {
        public Begin {
            if (transferId == null || relativePath == null || relativePath.isBlank() || totalBytes < 0
                || sha256 == null || sha256.length != SHA_256_BYTES) {
                throw new IllegalArgumentException("Invalid file transfer begin fields");
            }
            sha256 = sha256.clone();
        }

        @Override
        public byte[] sha256() {
            return this.sha256.clone();
        }
    }

    public record Chunk(UUID transferId, long offset, byte[] data) {
        public Chunk {
            if (transferId == null || offset < 0 || data == null || data.length > MAX_CHUNK_BYTES) {
                throw new IllegalArgumentException("Invalid file transfer chunk fields");
            }
            data = data.clone();
        }

        @Override
        public byte[] data() {
            return this.data.clone();
        }
    }

    public record Ack(UUID transferId, long nextOffset, boolean complete, String detail) {
        public Ack {
            if (transferId == null || nextOffset < 0 || detail == null) {
                throw new IllegalArgumentException("Invalid file transfer acknowledgement fields");
            }
        }
    }
}
