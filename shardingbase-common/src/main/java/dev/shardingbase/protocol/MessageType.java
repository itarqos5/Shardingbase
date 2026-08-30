package dev.shardingbase.protocol;

/** Message families understood by the prototype transport. */
public enum MessageType {
    AUTHENTICATE_NODE_REQUEST,
    AUTHENTICATE_NODE_RESPONSE,
    VALIDATE_BACKEND_REQUEST,
    VALIDATE_BACKEND_RESPONSE,
    HEARTBEAT,
    HEARTBEAT_ACK,
    PLAYER_SNAPSHOT_PREPARE,
    PLAYER_SNAPSHOT_STAGE,
    PLAYER_SNAPSHOT_ACK,
    ERROR,
    FILE_BEGIN,
    FILE_CHUNK,
    FILE_ACK,
    FILE_COMPLETE,
    FILE_ABORT
}
