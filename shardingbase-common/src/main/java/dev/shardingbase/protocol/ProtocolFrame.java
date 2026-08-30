package dev.shardingbase.protocol;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/** Immutable transport frame. */
public final class ProtocolFrame {
    private final int version;
    private final ProtocolChannel channel;
    private final MessageType messageType;
    private final UUID correlationId;
    private final String sourceId;
    private final String targetId;
    private final byte[] payload;

    public ProtocolFrame(
        final int version,
        final ProtocolChannel channel,
        final MessageType messageType,
        final UUID correlationId,
        final String sourceId,
        final String targetId,
        final byte[] payload
    ) {
        this.version = version;
        this.channel = Objects.requireNonNull(channel, "channel");
        this.messageType = Objects.requireNonNull(messageType, "messageType");
        this.correlationId = Objects.requireNonNull(correlationId, "correlationId");
        this.sourceId = Objects.requireNonNull(sourceId, "sourceId");
        this.targetId = Objects.requireNonNull(targetId, "targetId");
        this.payload = Objects.requireNonNull(payload, "payload").clone();
    }

    public int version() {
        return this.version;
    }

    public ProtocolChannel channel() {
        return this.channel;
    }

    public MessageType messageType() {
        return this.messageType;
    }

    public UUID correlationId() {
        return this.correlationId;
    }

    public String sourceId() {
        return this.sourceId;
    }

    public String targetId() {
        return this.targetId;
    }

    public byte[] payload() {
        return this.payload.clone();
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof final ProtocolFrame frame)) {
            return false;
        }
        return this.version == frame.version
            && this.channel == frame.channel
            && this.messageType == frame.messageType
            && this.correlationId.equals(frame.correlationId)
            && this.sourceId.equals(frame.sourceId)
            && this.targetId.equals(frame.targetId)
            && Arrays.equals(this.payload, frame.payload);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
            this.version,
            this.channel,
            this.messageType,
            this.correlationId,
            this.sourceId,
            this.targetId
        );
        result = 31 * result + Arrays.hashCode(this.payload);
        return result;
    }
}
