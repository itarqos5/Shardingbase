package dev.shardingbase.api;

/**
 * Describes whether Shardingbase's distributed features are safe to use.
 */
public enum FeatureState {
    /** The backend is waiting for validation from its node and proxy. */
    PENDING,
    /** The backend identity and peer topology have been validated. */
    ENABLED,
    /** Distributed features are unavailable; ordinary Paper behavior remains available. */
    DISABLED,
    /** A world transaction owns this backend and normal player access is locked. */
    MAINTENANCE
}
