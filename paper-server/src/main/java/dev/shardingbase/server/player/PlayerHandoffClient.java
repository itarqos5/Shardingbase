package dev.shardingbase.server.player;

import dev.shardingbase.protocol.MessageType;
import dev.shardingbase.protocol.PlayerDataCategory;
import dev.shardingbase.protocol.PlayerHandoffCodec;
import dev.shardingbase.protocol.PlayerSnapshot;
import dev.shardingbase.protocol.ProtocolChannel;
import dev.shardingbase.protocol.ProtocolFrame;
import dev.shardingbase.server.validation.LocalNodeClient;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/** Backend-side player handoff operations routed through the supervising node. */
public final class PlayerHandoffClient {
    private final String backendId;
    private final NodeTransport node;

    public PlayerHandoffClient(final String backendId) {
        this(backendId, new LocalNodeClient()::request);
    }

    PlayerHandoffClient(final String backendId, final NodeTransport node) {
        if (backendId == null || backendId.isBlank()) {
            throw new IllegalArgumentException("backendId must not be blank");
        }
        this.backendId = backendId;
        this.node = node;
    }

    /** Reserves the next authoritative revision before source capture. */
    public long prepare(
        final UUID playerId,
        final String targetBackendId,
        final Set<PlayerDataCategory> categories
    ) throws IOException {
        final ProtocolFrame response = this.node.request(
            this.backendId,
            ProtocolChannel.PLAYER_SYNC,
            MessageType.PLAYER_SNAPSHOT_PREPARE,
            "velocity",
            PlayerHandoffCodec.encodePrepare(new PlayerHandoffCodec.Prepare(
                playerId,
                targetBackendId,
                categories
            ))
        );
        requireType(response, MessageType.PLAYER_SNAPSHOT_ACK);
        final PlayerHandoffCodec.Acknowledgement acknowledgement = PlayerHandoffCodec.decodeAcknowledgement(
            response.payload()
        );
        if (!acknowledgement.accepted()) {
            throw new IOException("Player handoff preparation rejected: " + acknowledgement.detail());
        }
        return acknowledgement.revision();
    }

    /** Persists a captured revision at Velocity for one target backend. */
    public void stage(final String targetBackendId, final PlayerSnapshot snapshot) throws IOException {
        final ProtocolFrame response = this.node.request(
            this.backendId,
            ProtocolChannel.PLAYER_SYNC,
            MessageType.PLAYER_SNAPSHOT_STAGE,
            "velocity",
            PlayerHandoffCodec.encodeStage(new PlayerHandoffCodec.Stage(targetBackendId, snapshot))
        );
        requireType(response, MessageType.PLAYER_SNAPSHOT_ACK);
        final PlayerHandoffCodec.Acknowledgement acknowledgement = PlayerHandoffCodec.decodeAcknowledgement(
            response.payload()
        );
        if (!acknowledgement.accepted() || acknowledgement.revision() != snapshot.revision()) {
            throw new IOException("Player snapshot staging rejected: " + acknowledgement.detail());
        }
    }

    /** Fetches the latest snapshot staged specifically for this backend. */
    public Optional<PlayerHandoffCodec.Stage> fetch(final UUID playerId) throws IOException {
        final ProtocolFrame response = this.node.request(
            this.backendId,
            ProtocolChannel.PLAYER_SYNC,
            MessageType.PLAYER_SNAPSHOT_FETCH,
            "velocity",
            PlayerHandoffCodec.encodeFetch(new PlayerHandoffCodec.Fetch(playerId, this.backendId))
        );
        requireType(response, MessageType.PLAYER_SNAPSHOT_FETCH_RESPONSE);
        return Optional.ofNullable(PlayerHandoffCodec.decodeFetchResponse(response.payload()).stage());
    }

    private static void requireType(final ProtocolFrame response, final MessageType expected) throws IOException {
        if (response.messageType() != expected) {
            throw new IOException("Unexpected player handoff response: " + response.messageType());
        }
    }

    @FunctionalInterface
    interface NodeTransport {
        ProtocolFrame request(
            String sourceId,
            ProtocolChannel channel,
            MessageType messageType,
            String targetId,
            byte[] payload
        ) throws IOException;
    }
}
