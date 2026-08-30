package dev.shardingbase.server.menu;

import java.util.List;
import org.bukkit.Material;

/** Immutable configuration for one menu button. */
public record MenuButton(String id, boolean enabled, Material material, int slot, String name, List<String> lore) {
    public MenuButton {
        lore = List.copyOf(lore);
    }
}
