package dev.shardingbase.api;

import java.util.Map;

/**
 * Validated detached entity-spawn request.
 *
 * @param entityType namespaced entity type key
 * @param position   target position
 * @param properties allowlisted portable properties
 */
public record EntitySpawn(String entityType, WorldPosition position, Map<String, String> properties) {
    /** Creates a request with an immutable copy of its properties. */
    public EntitySpawn {
        properties = Map.copyOf(properties);
    }
}
