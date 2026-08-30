package dev.shardingbase.protocol;

/** Logical channels multiplexed over a Shardingbase control connection. */
public enum ProtocolChannel {
    CONTROL,
    PLAYER_SYNC,
    COMMAND,
    REMOTE_OPERATION,
    MAP,
    WORLD_TRANSACTION,
    FILE_TRANSFER
}
