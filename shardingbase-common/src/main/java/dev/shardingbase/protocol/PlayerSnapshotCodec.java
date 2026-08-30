package dev.shardingbase.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;

/** Bounded binary codec for portable player snapshots. */
public final class PlayerSnapshotCodec {
    private static final int FORMAT_VERSION = 1;

    private PlayerSnapshotCodec() {
    }

    public static byte[] encode(final PlayerSnapshot snapshot) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeByte(FORMAT_VERSION);
            output.writeLong(snapshot.playerId().getMostSignificantBits());
            output.writeLong(snapshot.playerId().getLeastSignificantBits());
            output.writeLong(snapshot.revision());
            writeString(output, snapshot.sourceBackendId());
            final Map<PlayerDataCategory, byte[]> categories = snapshot.categories();
            output.writeByte(categories.size());
            for (final Map.Entry<PlayerDataCategory, byte[]> entry : categories.entrySet()) {
                output.writeByte(entry.getKey().ordinal());
                output.writeInt(entry.getValue().length);
                output.write(entry.getValue());
                if (bytes.size() > FrameCodec.MAX_PAYLOAD_BYTES) {
                    throw new ProtocolException("Player snapshot exceeds the frame payload limit");
                }
            }
        }
        return bytes.toByteArray();
    }

    public static PlayerSnapshot decode(final byte[] encoded) throws IOException {
        if (encoded.length > FrameCodec.MAX_PAYLOAD_BYTES) {
            throw new ProtocolException("Player snapshot exceeds the frame payload limit");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded))) {
            if (input.readUnsignedByte() != FORMAT_VERSION) {
                throw new ProtocolException("Unsupported player snapshot format");
            }
            final UUID playerId = new UUID(input.readLong(), input.readLong());
            final long revision = input.readLong();
            if (revision < 1) {
                throw new ProtocolException("Player snapshot revision must be positive");
            }
            final String source = readString(input);
            final int count = input.readUnsignedByte();
            if (count > PlayerDataCategory.values().length) {
                throw new ProtocolException("Invalid player snapshot category count");
            }
            final EnumMap<PlayerDataCategory, byte[]> categories = new EnumMap<>(PlayerDataCategory.class);
            for (int index = 0; index < count; index++) {
                final int ordinal = input.readUnsignedByte();
                if (ordinal >= PlayerDataCategory.values().length) {
                    throw new ProtocolException("Invalid player snapshot category");
                }
                final int length = input.readInt();
                if (length < 0 || length > input.available()) {
                    throw new ProtocolException("Invalid player snapshot category length");
                }
                final PlayerDataCategory category = PlayerDataCategory.values()[ordinal];
                if (categories.put(category, input.readNBytes(length)) != null) {
                    throw new ProtocolException("Duplicate player snapshot category");
                }
            }
            if (input.available() != 0) {
                throw new ProtocolException("Trailing player snapshot data");
            }
            return new PlayerSnapshot(playerId, revision, source, categories);
        } catch (final IllegalArgumentException exception) {
            throw new ProtocolException("Invalid player snapshot", exception);
        }
    }

    private static void writeString(final DataOutputStream output, final String value) throws IOException {
        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0 || bytes.length > ShardingbaseProtocol.MAX_CONTROL_FIELD_BYTES) {
            throw new ProtocolException("Invalid player snapshot source backend ID");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readString(final DataInputStream input) throws IOException {
        final int length = input.readInt();
        if (length < 1 || length > ShardingbaseProtocol.MAX_CONTROL_FIELD_BYTES || length > input.available()) {
            throw new ProtocolException("Invalid player snapshot source backend ID length");
        }
        return new String(input.readNBytes(length), StandardCharsets.UTF_8);
    }
}
