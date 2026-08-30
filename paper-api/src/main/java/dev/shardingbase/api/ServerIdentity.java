package dev.shardingbase.api;

/**
 * Stable identity configured for one Shardingbase backend.
 *
 * @param serverId   globally unique backend identifier
 * @param serverName matching Velocity server name
 */
public record ServerIdentity(String serverId, String serverName) {
}
