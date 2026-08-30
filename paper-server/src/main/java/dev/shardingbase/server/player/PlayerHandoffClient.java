package dev.shardingbase.server.player;

import dev.shardingbase.protocol.MessageType;
import dev.shardingbase.protocol.PlayerDataCategory;
import dev.shardingbase.protocol.PlayerHandoffCodec;
import dev.shardingbase.protocol.PlayerSnapshot;
import dev.shardingbase.protocol.PlayerSettingsCodec;
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
        this.stage(targetBackendId, snapshot, null);
    }

    /** Persists a captured revision and its optional exact transfer destination. */
    public void stage(
        final String targetBackendId,
        final PlayerSnapshot snapshot,
        final PlayerHandoffCodec.TransferDestination destination
    ) throws IOException {
        final ProtocolFrame response = this.node.request(
            this.backendId,
            ProtocolChannel.PLAYER_SYNC,
            MessageType.PLAYER_SNAPSHOT_STAGE,
            "velocity",
            PlayerHandoffCodec.encodeStage(new PlayerHandoffCodec.Stage(targetBackendId, snapshot, destination))
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

    /** Requests an authenticated managed transfer after a player crosses a shard boundary. */
    public PlayerHandoffCodec.BoundaryResponse boundary(
        final String targetBackendId,
        final UUID playerId,
        final PlayerHandoffCodec.TransferDestination destination
    ) throws IOException {
        final ProtocolFrame response = this.node.request(
            this.backendId,
            ProtocolChannel.PLAYER_SYNC,
            MessageType.PLAYER_BOUNDARY_REQUEST,
            "velocity",
            PlayerHandoffCodec.encodeBoundaryRequest(new PlayerHandoffCodec.BoundaryRequest(
                playerId, this.backendId, targetBackendId, destination
            ))
        );
        requireType(response, MessageType.PLAYER_BOUNDARY_RESPONSE);
        final PlayerHandoffCodec.BoundaryResponse boundary = PlayerHandoffCodec.decodeBoundaryResponse(
            response.payload()
        );
        if (!playerId.equals(boundary.playerId())) {
            throw new IOException("Velocity returned a boundary response for another player");
        }
        return boundary;
    }

    /** Loads the network-authoritative portable category selection. */
    public Set<PlayerDataCategory> settings() throws IOException {
        final ProtocolFrame response = this.node.request(
            this.backendId,
            ProtocolChannel.PLAYER_SYNC,
            MessageType.PLAYER_SETTINGS_GET,
            "velocity",
            new byte[0]
        );
        requireType(response, MessageType.PLAYER_SETTINGS_RESPONSE);
        return PlayerSettingsCodec.decode(response.payload());
    }

    /** Atomically replaces and returns the network-authoritative portable category selection. */
    public Set<PlayerDataCategory> settings(final Set<PlayerDataCategory> categories) throws IOException {
        final ProtocolFrame response = this.node.request(
            this.backendId,
            ProtocolChannel.PLAYER_SYNC,
            MessageType.PLAYER_SETTINGS_SET,
            "velocity",
            PlayerSettingsCodec.encode(categories)
        );
        requireType(response, MessageType.PLAYER_SETTINGS_RESPONSE);
        return PlayerSettingsCodec.decode(response.payload());
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
