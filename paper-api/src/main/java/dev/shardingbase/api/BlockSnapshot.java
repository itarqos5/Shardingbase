package dev.shardingbase.api;

import java.util.Map;

/**
 * Detached remote block snapshot.
 *
 * @param blockData serialized Bukkit block-data string
 * @param stateData portable state fields, empty for stateless blocks
 */
public record BlockSnapshot(String blockData, Map<String, String> stateData) {
    /** Creates a snapshot with an immutable copy of its state data. */
    public BlockSnapshot {
        stateData = Map.copyOf(stateData);
    }
}
