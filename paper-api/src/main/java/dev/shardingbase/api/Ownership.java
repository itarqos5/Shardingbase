package dev.shardingbase.api;

/** Ownership of a world position in the current shard topology. */
public enum Ownership {
    /** The current backend owns the position. */
    LOCAL,
    /** The peer backend owns the position. */
    REMOTE,
    /** The world has no active shard manifest. */
    UNSHARDED,
    /** Ownership exists but is locked by a transaction or recovery. */
    MAINTENANCE
}
