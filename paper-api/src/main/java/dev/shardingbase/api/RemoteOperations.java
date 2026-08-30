package dev.shardingbase.api;

import java.util.UUID;
import java.util.concurrent.CompletionStage;

/** Explicit asynchronous operations which may be routed to the peer shard. */
public interface RemoteOperations {
    /**
     * Reads a detached block snapshot.
     *
     * @param position target position
     * @return eventual typed result
     */
    CompletionStage<RemoteResult<BlockSnapshot>> readBlock(WorldPosition position);

    /**
     * Sets serialized Bukkit block data.
     *
     * @param position  target position
     * @param blockData serialized Bukkit block data
     * @return eventual typed result
     */
    CompletionStage<RemoteResult<Void>> setBlockData(WorldPosition position, String blockData);

    /**
     * Breaks a block using the peer's validated operation rules.
     *
     * @param position target position
     * @return eventual typed result
     */
    CompletionStage<RemoteResult<Boolean>> breakBlock(WorldPosition position);

    /**
     * Spawns an entity and returns its UUID.
     *
     * @param spawn validated detached spawn request
     * @return eventual typed result
     */
    CompletionStage<RemoteResult<UUID>> spawnEntity(EntitySpawn spawn);
}
