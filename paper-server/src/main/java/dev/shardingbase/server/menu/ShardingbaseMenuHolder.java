package dev.shardingbase.server.menu;

import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.InventoryHolder;
import org.jetbrains.annotations.NotNull;

/** Stable holder identity for Shardingbase-owned inventory views. */
final class ShardingbaseMenuHolder implements InventoryHolder {
    private final ShardingbaseMenuManager manager;
    private final String menuId;
    private Inventory inventory;

    ShardingbaseMenuHolder(final ShardingbaseMenuManager manager, final String menuId) {
        this.manager = manager;
        this.menuId = menuId;
    }

    ShardingbaseMenuManager manager() {
        return this.manager;
    }

    String menuId() {
        return this.menuId;
    }

    void inventory(final Inventory inventory) {
        this.inventory = inventory;
    }

    @Override
    public @NotNull Inventory getInventory() {
        if (this.inventory == null) {
            throw new IllegalStateException("Menu inventory has not been created yet");
        }
        return this.inventory;
    }
}
