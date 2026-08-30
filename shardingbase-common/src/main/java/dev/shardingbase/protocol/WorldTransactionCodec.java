package dev.shardingbase.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Objects;
import java.util.UUID;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** Canonical, HMAC-authenticated control payloads for one offline world transaction. */
public final class WorldTransactionCodec {
    public static final int SIGNATURE_BYTES = 32;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_STRING_BYTES = 4_096;
    private static final int MAX_SIGNATURE_BYTES = 1_024;
    private static final String HMAC_ALGORITHM = "HmacSHA256";

    private WorldTransactionCodec() {
    }

    public static SignedManifest sign(final Manifest manifest, final byte[] key) throws IOException {
        requireKey(key);
        try {
            final Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(new SecretKeySpec(key, HMAC_ALGORITHM));
            return new SignedManifest(manifest, mac.doFinal(canonicalManifest(manifest)));
        } catch (final GeneralSecurityException exception) {
            throw new IOException("Unable to sign the world transaction manifest", exception);
        }
    }

    public static boolean verify(final SignedManifest signed, final byte[] key) throws IOException {
        final SignedManifest expected = sign(signed.manifest(), key);
        return MessageDigest.isEqual(expected.signature(), signed.signature());
    }

    public static byte[] digest(final Manifest manifest) throws IOException {
        try {
            return MessageDigest.getInstance("SHA-256").digest(canonicalManifest(manifest));
        } catch (final GeneralSecurityException exception) {
            throw new IOException("Unable to digest the world transaction manifest", exception);
        }
    }

    public static byte[] encodeRequest(final Request request) throws IOException {
        return encode(output -> {
            output.writeInt(request.operation().ordinal());
            writeManifest(output, request.signedManifest().manifest());
            writeBytes(output, request.signedManifest().signature(), MAX_SIGNATURE_BYTES, "manifest signature");
        });
    }

    public static Request decodeRequest(final byte[] payload) throws IOException {
        return decode(payload, input -> {
            final Operation operation = readEnum(input, Operation.values(), "world transaction operation");
            final Manifest manifest = readManifest(input);
            final byte[] signature = readBytes(input, MAX_SIGNATURE_BYTES, "manifest signature");
            return new Request(operation, new SignedManifest(manifest, signature));
        });
    }

    public static byte[] encodeResponse(final Response response) throws IOException {
        return encode(output -> {
            writeUuid(output, response.transactionId());
            output.writeInt(response.operation().ordinal());
            output.writeInt(response.outcome().ordinal());
            writeString(output, response.detail());
            output.writeBoolean(response.backendRunning());
            output.writeLong(response.processId());
            output.writeInt(response.lastExitCode());
            output.writeLong(response.usableBytes());
            writeBytes(output, response.manifestDigest(), DIGEST_BYTES, "manifest digest");
        });
    }

    public static Response decodeResponse(final byte[] payload) throws IOException {
        return decode(payload, input -> new Response(
            readUuid(input),
            readEnum(input, Operation.values(), "world transaction operation"),
            readEnum(input, Outcome.values(), "world transaction outcome"),
            readString(input),
            input.readBoolean(),
            input.readLong(),
            input.readInt(),
            input.readLong(),
            readBytes(input, DIGEST_BYTES, "manifest digest")
        ));
    }

    private static byte[] canonicalManifest(final Manifest manifest) throws IOException {
        return encode(output -> writeManifest(output, manifest));
    }

    private static void writeManifest(final DataOutputStream output, final Manifest manifest) throws IOException {
        writeUuid(output, manifest.transactionId());
        writeString(output, manifest.sourceNodeId());
        writeString(output, manifest.targetNodeId());
        writeString(output, manifest.sourceBackendId());
        writeString(output, manifest.targetBackendId());
        writeString(output, manifest.worldKey());
        writeString(output, manifest.worldDirectory());
        writeUuid(output, manifest.worldId());
        output.writeLong(manifest.worldSeed());
        output.writeInt(manifest.dataVersion());
        output.writeInt(manifest.axis().ordinal());
        output.writeInt(manifest.cutChunk());
        writeString(output, manifest.negativeNodeId());
        writeString(output, manifest.positiveNodeId());
        output.writeLong(manifest.estimatedBytes());
    }

    private static Manifest readManifest(final DataInputStream input) throws IOException {
        return new Manifest(
            readUuid(input),
            readString(input),
            readString(input),
            readString(input),
            readString(input),
            readString(input),
            readString(input),
            readUuid(input),
            input.readLong(),
            input.readInt(),
            readEnum(input, Axis.values(), "shard axis"),
            input.readInt(),
            readString(input),
            readString(input),
            input.readLong()
        );
    }

    private static byte[] encode(final Writer writer) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            writer.write(output);
        }
        final byte[] payload = bytes.toByteArray();
        if (payload.length > FrameCodec.MAX_PAYLOAD_BYTES) {
            throw new ProtocolException("World transaction payload exceeds the frame limit");
        }
        return payload;
    }

    private static <T> T decode(final byte[] payload, final Reader<T> reader) throws IOException {
        if (payload.length > FrameCodec.MAX_PAYLOAD_BYTES) {
            throw new ProtocolException("World transaction payload exceeds the frame limit");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload))) {
            final T result = reader.read(input);
            if (input.available() != 0) {
                throw new ProtocolException("Trailing world transaction payload data");
            }
            return result;
        } catch (final IllegalArgumentException exception) {
            throw new ProtocolException("Invalid world transaction payload", exception);
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
            throw new ProtocolException("Invalid world transaction string");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readString(final DataInputStream input) throws IOException {
        final int length = input.readInt();
        if (length < 1 || length > MAX_STRING_BYTES || length > input.available()) {
            throw new ProtocolException("Invalid world transaction string length");
        }
        return new String(input.readNBytes(length), StandardCharsets.UTF_8);
    }

    private static void writeBytes(
        final DataOutputStream output,
        final byte[] value,
        final int maximum,
        final String field
    ) throws IOException {
        if (value.length < 1 || value.length > maximum) {
            throw new ProtocolException("Invalid " + field);
        }
        output.writeInt(value.length);
        output.write(value);
    }

    private static byte[] readBytes(final DataInputStream input, final int maximum, final String field)
        throws IOException {
        final int length = input.readInt();
        if (length < 1 || length > maximum || length > input.available()) {
            throw new ProtocolException("Invalid " + field + " length");
        }
        return input.readNBytes(length);
    }

    private static <E extends Enum<E>> E readEnum(
        final DataInputStream input,
        final E[] values,
        final String field
    ) throws IOException {
        final int ordinal = input.readInt();
        if (ordinal < 0 || ordinal >= values.length) {
            throw new ProtocolException("Invalid " + field);
        }
        return values[ordinal];
    }

    private static void requireKey(final byte[] key) {
        if (key == null || key.length < 32) {
            throw new IllegalArgumentException("World transaction signing key must contain at least 32 bytes");
        }
    }

    private static boolean identifier(final String value) {
        return value != null && value.matches("[A-Za-z0-9_.:-]{1,128}");
    }

    private static boolean relativeWorldDirectory(final String value) {
        if (value == null || value.isBlank() || value.startsWith("/") || value.contains("\\")
            || !value.matches("[A-Za-z0-9._/-]{1,512}")) {
            return false;
        }
        for (final String segment : value.split("/")) {
            if (segment.isBlank() || ".".equals(segment) || "..".equals(segment)) {
                return false;
            }
        }
        return true;
    }

    public enum Operation {
        AUTHORIZE_AND_SAVE,
        STOP_BACKEND,
        PREPARE_SOURCE,
        RESTART_BACKEND,
        STATUS,
        PREPARE_TARGET,
        RELAY_TARGET,
        INSTALL_TARGET,
        COMMIT_SOURCE,
        ROLLBACK
    }

    public enum Outcome {
        READY,
        SUCCESS,
        REJECTED,
        FAILED
    }

    public enum Axis {
        X,
        Z
    }

    public record Manifest(
        UUID transactionId,
        String sourceNodeId,
        String targetNodeId,
        String sourceBackendId,
        String targetBackendId,
        String worldKey,
        String worldDirectory,
        UUID worldId,
        long worldSeed,
        int dataVersion,
        Axis axis,
        int cutChunk,
        String negativeNodeId,
        String positiveNodeId,
        long estimatedBytes
    ) {
        public Manifest {
            if (transactionId == null || worldId == null || axis == null
                || !identifier(sourceNodeId) || !identifier(targetNodeId)
                || sourceNodeId.equals(targetNodeId)
                || !identifier(sourceBackendId) || !identifier(targetBackendId)
                || sourceBackendId.equals(targetBackendId)
                || !identifier(negativeNodeId) || !identifier(positiveNodeId)
                || negativeNodeId.equals(positiveNodeId)
                || worldKey == null || !worldKey.matches("[a-z0-9_.-]+:[a-z0-9_./-]+")
                || !relativeWorldDirectory(worldDirectory)
                || dataVersion < 1 || estimatedBytes < 0) {
                throw new IllegalArgumentException("Invalid world transaction manifest");
            }
            if (!(negativeNodeId.equals(sourceNodeId) || negativeNodeId.equals(targetNodeId))
                || !(positiveNodeId.equals(sourceNodeId) || positiveNodeId.equals(targetNodeId))) {
                throw new IllegalArgumentException("Shard owners must be the source and target nodes");
            }
        }
    }

    public record SignedManifest(Manifest manifest, byte[] signature) {
        public SignedManifest {
            Objects.requireNonNull(manifest, "manifest");
            if (signature == null || signature.length != SIGNATURE_BYTES) {
                throw new IllegalArgumentException("World transaction signature must be HMAC-SHA-256");
            }
            signature = signature.clone();
        }

        @Override
        public byte[] signature() {
            return this.signature.clone();
        }
    }

    public record Request(Operation operation, SignedManifest signedManifest) {
        public Request {
            Objects.requireNonNull(operation, "operation");
            Objects.requireNonNull(signedManifest, "signedManifest");
        }
    }

    public record Response(
        UUID transactionId,
        Operation operation,
        Outcome outcome,
        String detail,
        boolean backendRunning,
        long processId,
        int lastExitCode,
        long usableBytes,
        byte[] manifestDigest
    ) {
        public Response {
            if (transactionId == null || operation == null || outcome == null
                || detail == null || detail.isBlank() || processId < -1 || lastExitCode < -1 || usableBytes < -1
                || manifestDigest == null || manifestDigest.length != DIGEST_BYTES) {
                throw new IllegalArgumentException("Invalid world transaction response");
            }
            manifestDigest = manifestDigest.clone();
        }

        @Override
        public byte[] manifestDigest() {
            return this.manifestDigest.clone();
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
