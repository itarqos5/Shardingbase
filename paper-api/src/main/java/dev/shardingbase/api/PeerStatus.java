package dev.shardingbase.api;

/**
 * Detached status for the peer backend.
 *
 * @param available  whether the peer is validated and healthy
 * @param serverId   peer server ID, or an empty string when unknown
 * @param serverName peer Velocity name, or an empty string when unknown
 * @param detail     operator-facing status detail
 */
public record PeerStatus(boolean available, String serverId, String serverName, String detail) {
}
