package dev.shardingbase.protocol;

/** Message families understood by the prototype transport. */
public enum MessageType {
    VALIDATE_BACKEND_REQUEST,
    VALIDATE_BACKEND_RESPONSE,
    HEARTBEAT,
    HEARTBEAT_ACK,
    ERROR,
    FILE_CHUNK,
    FILE_ACK
}
