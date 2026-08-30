package dev.shardingbase.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;

/** Control payloads for revision allocation and staged player handoff. */
public final class PlayerHandoffCodec {
    private PlayerHandoffCodec() {
    }

    public static byte[] encodePrepare(final Prepare prepare) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            writeUuid(output, prepare.playerId());
            writeString(output, prepare.targetBackendId());
            long categoryBits = 0;
            for (final PlayerDataCategory category : prepare.categories()) {
                categoryBits |= 1L << category.ordinal();
            }
            output.writeLong(categoryBits);
        }
        return bytes.toByteArray();
    }

    public static Prepare decodePrepare(final byte[] payload) throws IOException {
        try (DataInputStream input = input(payload)) {
            final UUID playerId = readUuid(input);
            final String target = readString(input);
            final long bits = input.readLong();
            final EnumSet<PlayerDataCategory> categories = EnumSet.noneOf(PlayerDataCategory.class);
            long knownBits = 0;
            for (final PlayerDataCategory category : PlayerDataCategory.values()) {
                final long bit = 1L << category.ordinal();
                knownBits |= bit;
                if ((bits & bit) != 0) {
                    categories.add(category);
                }
            }
            if ((bits & ~knownBits) != 0 || categories.isEmpty() || input.available() != 0) {
                throw new ProtocolException("Invalid player handoff category mask");
            }
            return new Prepare(playerId, target, categories);
        }
    }

    public static byte[] encodeAcknowledgement(final Acknowledgement acknowledgement) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            writeUuid(output, acknowledgement.playerId());
            output.writeLong(acknowledgement.revision());
            output.writeBoolean(acknowledgement.accepted());
            writeString(output, acknowledgement.detail());
        }
        return bytes.toByteArray();
    }

    public static Acknowledgement decodeAcknowledgement(final byte[] payload) throws IOException {
        try (DataInputStream input = input(payload)) {
            final Acknowledgement acknowledgement = new Acknowledgement(
                readUuid(input),
                input.readLong(),
                input.readBoolean(),
                readString(input)
            );
            if (input.available() != 0) {
                throw new ProtocolException("Trailing player handoff acknowledgement");
            }
            return acknowledgement;
        } catch (final IllegalArgumentException exception) {
            throw new ProtocolException("Invalid player handoff acknowledgement", exception);
        }
    }

    public static byte[] encodeStage(final Stage stage) throws IOException {
        final byte[] snapshot = PlayerSnapshotCodec.encode(stage.snapshot());
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            writeString(output, stage.targetBackendId());
            output.writeInt(snapshot.length);
            output.write(snapshot);
        }
        return bytes.toByteArray();
    }

    public static Stage decodeStage(final byte[] payload) throws IOException {
        try (DataInputStream input = input(payload)) {
            final String target = readString(input);
            final int length = input.readInt();
            if (length < 0 || length > input.available()) {
                throw new ProtocolException("Invalid staged player snapshot length");
            }
            final PlayerSnapshot snapshot = PlayerSnapshotCodec.decode(input.readNBytes(length));
            if (input.available() != 0) {
                throw new ProtocolException("Trailing staged player snapshot payload");
            }
            return new Stage(target, snapshot);
        }
    }

    public static byte[] encodeFetch(final Fetch fetch) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            writeUuid(output, fetch.playerId());
            writeString(output, fetch.targetBackendId());
        }
        return bytes.toByteArray();
    }

    public static Fetch decodeFetch(final byte[] payload) throws IOException {
        try (DataInputStream input = input(payload)) {
            final Fetch fetch = new Fetch(readUuid(input), readString(input));
            if (input.available() != 0) {
                throw new ProtocolException("Trailing player snapshot fetch payload");
            }
            return fetch;
        }
    }

    public static byte[] encodeFetchResponse(final FetchResponse response) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeBoolean(response.stage() != null);
            if (response.stage() != null) {
                final byte[] stage = encodeStage(response.stage());
                output.writeInt(stage.length);
                output.write(stage);
            }
        }
        return bytes.toByteArray();
    }

    public static FetchResponse decodeFetchResponse(final byte[] payload) throws IOException {
        try (DataInputStream input = input(payload)) {
            final boolean present = input.readBoolean();
            final Stage stage;
            if (present) {
                final int length = input.readInt();
                if (length < 0 || length > input.available()) {
                    throw new ProtocolException("Invalid fetched player snapshot length");
                }
                stage = decodeStage(input.readNBytes(length));
            } else {
                stage = null;
            }
            if (input.available() != 0) {
                throw new ProtocolException("Trailing player snapshot fetch response");
            }
            return new FetchResponse(stage);
        }
    }

    private static DataInputStream input(final byte[] payload) throws ProtocolException {
        if (payload.length > FrameCodec.MAX_PAYLOAD_BYTES) {
            throw new ProtocolException("Player handoff payload exceeds the frame limit");
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
        if (bytes.length == 0 || bytes.length > ShardingbaseProtocol.MAX_CONTROL_FIELD_BYTES) {
            throw new ProtocolException("Invalid player handoff field");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readString(final DataInputStream input) throws IOException {
        final int length = input.readInt();
        if (length < 1 || length > ShardingbaseProtocol.MAX_CONTROL_FIELD_BYTES || length > input.available()) {
            throw new ProtocolException("Invalid player handoff field length");
        }
        return new String(input.readNBytes(length), StandardCharsets.UTF_8);
    }

    public record Prepare(UUID playerId, String targetBackendId, Set<PlayerDataCategory> categories) {
        public Prepare {
            if (playerId == null || targetBackendId == null || targetBackendId.isBlank()
                || categories == null || categories.isEmpty()) {
                throw new IllegalArgumentException("Player handoff preparation fields are required");
            }
            categories = Set.copyOf(categories);
        }
    }

    public record Acknowledgement(UUID playerId, long revision, boolean accepted, String detail) {
        public Acknowledgement {
            if (playerId == null || revision < 1 || detail == null || detail.isBlank()) {
                throw new IllegalArgumentException("Invalid player handoff acknowledgement fields");
            }
        }
    }

    public record Stage(String targetBackendId, PlayerSnapshot snapshot) {
        public Stage {
            if (targetBackendId == null || targetBackendId.isBlank() || snapshot == null) {
                throw new IllegalArgumentException("Staged player snapshot fields are required");
            }
        }
    }

    public record Fetch(UUID playerId, String targetBackendId) {
        public Fetch {
            if (playerId == null || targetBackendId == null || targetBackendId.isBlank()) {
                throw new IllegalArgumentException("Player snapshot fetch fields are required");
            }
        }
    }

    public record FetchResponse(Stage stage) {
    }
}
