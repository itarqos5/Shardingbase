package dev.shardingbase.server.menu;

import java.util.Map;

/** Immutable configuration for one inventory menu. */
public record MenuDefinition(String id, String title, int rows, Map<String, MenuButton> buttons) {
    public MenuDefinition {
        buttons = Map.copyOf(buttons);
    }
}
