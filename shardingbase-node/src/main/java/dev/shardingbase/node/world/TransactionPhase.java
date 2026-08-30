package dev.shardingbase.node.world;

/** Durable phases of an offline world transaction. */
public enum TransactionPhase {
    PLANNED,
    AUTHORIZED,
    BACKEND_STOPPED,
    BACKUP_COMPLETE,
    SPLIT_COMPLETE,
    TARGET_PREPARED,
    SOURCE_COMMITTED,
    STARTING_TARGET,
    STARTING_SOURCE,
    COMPLETE,
    ROLLED_BACK,
    FAILED
}
