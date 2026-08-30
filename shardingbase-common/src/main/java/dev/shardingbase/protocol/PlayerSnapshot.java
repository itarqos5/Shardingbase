package dev.shardingbase.protocol;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Immutable, revisioned portable player data with opaque category payloads. */
public final class PlayerSnapshot {
    private final UUID playerId;
    private final long revision;
    private final String sourceBackendId;
    private final Map<PlayerDataCategory, byte[]> categories;

    public PlayerSnapshot(
        final UUID playerId,
        final long revision,
        final String sourceBackendId,
        final Map<PlayerDataCategory, byte[]> categories
    ) {
        this.playerId = Objects.requireNonNull(playerId, "playerId");
        if (revision < 1) {
            throw new IllegalArgumentException("revision must be positive");
        }
        this.revision = revision;
        this.sourceBackendId = Objects.requireNonNull(sourceBackendId, "sourceBackendId");
        if (sourceBackendId.isBlank()) {
            throw new IllegalArgumentException("sourceBackendId must not be blank");
        }
        final EnumMap<PlayerDataCategory, byte[]> copied = new EnumMap<>(PlayerDataCategory.class);
        categories.forEach((category, payload) -> copied.put(
            Objects.requireNonNull(category, "category"),
            Objects.requireNonNull(payload, "payload").clone()
        ));
        this.categories = Collections.unmodifiableMap(copied);
    }

    public UUID playerId() {
        return this.playerId;
    }

    public long revision() {
        return this.revision;
    }

    public String sourceBackendId() {
        return this.sourceBackendId;
    }

    public Map<PlayerDataCategory, byte[]> categories() {
        final EnumMap<PlayerDataCategory, byte[]> copied = new EnumMap<>(PlayerDataCategory.class);
        this.categories.forEach((category, payload) -> copied.put(category, payload.clone()));
        return Collections.unmodifiableMap(copied);
    }
}
