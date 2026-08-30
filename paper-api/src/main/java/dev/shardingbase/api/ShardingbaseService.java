package dev.shardingbase.api;

import java.util.concurrent.CompletionStage;

/**
 * Asynchronous entry point for Shardingbase state and operations.
 *
 * <p>Implementations never return synthetic remote Bukkit objects. Operations which may cross a
 * process boundary return a {@link CompletionStage}.</p>
 */
public interface ShardingbaseService {
    /**
     * Gets the currently published backend identity.
     *
     * @return immutable identity
     */
    ServerIdentity identity();

    /**
     * Gets the current distributed-feature state.
     *
     * @return current state
     */
    FeatureState featureState();

    /**
     * Gets a human-readable validation or connectivity detail.
     *
     * @return current status detail
     */
    String statusDetail();

    /**
     * Gets detached peer status.
     *
     * @return current peer status
     */
    PeerStatus peerStatus();

    /**
     * Resolves ownership without loading or generating a chunk.
     *
     * @param position detached position
     * @return current ownership result
     */
    Ownership ownership(WorldPosition position);

    /**
     * Gets the explicit asynchronous remote-operation surface.
     *
     * @return remote operations
     */
    RemoteOperations remoteOperations();

    /**
     * Reloads only Shardingbase configuration and repeats validation.
     *
     * @return eventual reload result
     */
    CompletionStage<ReloadResult> reload();

    /**
     * Result of an attempted Shardingbase-only reload.
     *
     * @param successful whether the candidate configuration was accepted
     * @param message    operator-facing result detail
     */
    record ReloadResult(boolean successful, String message) {
    }
}
