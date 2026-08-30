package dev.shardingbase.server.config;

import dev.shardingbase.api.ServerIdentity;
/** Published, immutable backend configuration. */
public record ShardingbaseConfiguration(ServerIdentity identity) {
}
