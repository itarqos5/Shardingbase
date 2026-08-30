package dev.shardingbase.protocol;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Set;

/** Compact category selection shared by both backends and the Velocity authority. */
public final class PlayerSettingsCodec {
    private PlayerSettingsCodec() {
    }

    public static byte[] encode(final Set<PlayerDataCategory> categories) throws IOException {
        if (categories == null || categories.isEmpty()) {
            throw new ProtocolException("At least one portable player category must remain enabled");
        }
        long bits = 0;
        for (final PlayerDataCategory category : categories) {
            bits |= 1L << category.ordinal();
        }
        return ByteBuffer.allocate(Long.BYTES).putLong(bits).array();
    }

    public static Set<PlayerDataCategory> decode(final byte[] payload) throws IOException {
        if (payload.length != Long.BYTES) {
            throw new ProtocolException("Invalid player settings payload length");
        }
        final long bits = ByteBuffer.wrap(payload).getLong();
        long knownBits = 0;
        final EnumSet<PlayerDataCategory> categories = EnumSet.noneOf(PlayerDataCategory.class);
        for (final PlayerDataCategory category : PlayerDataCategory.values()) {
            final long bit = 1L << category.ordinal();
            knownBits |= bit;
            if ((bits & bit) != 0) {
                categories.add(category);
            }
        }
        if (categories.isEmpty() || (bits & ~knownBits) != 0) {
            throw new ProtocolException("Invalid player settings category mask");
        }
        return Set.copyOf(categories);
    }
}
